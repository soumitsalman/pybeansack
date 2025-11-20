from cmath import log
import os
from pydantic import Field
import lancedb
from lancedb.rerankers import Reranker
from lancedb.pydantic import LanceModel, Vector
from datetime import datetime, timedelta
import pyarrow as pa
import pandas as pd
from .models import *
import logging
from icecream import ic

log = logging.getLogger(__name__)

NOT_SUPPORTED = NotImplementedError("Querying trend data is not yet supported")

_PRIMARY_KEYS = {
    BEANS: K_URL,
    PUBLISHERS: K_SOURCE
}

class _Bean(Bean, LanceModel):
    embedding: Vector(VECTOR_LEN, nullable=True) = Field(None)

class _Publisher(Publisher, LanceModel):
    pass

class _Chatter(Chatter, LanceModel):
    pass

class _Sip(Sip, LanceModel):    
    embedding: Vector(VECTOR_LEN, nullable=True) = Field(None, description="This is the embedding vector of title+content")
    
class _Mug(Mug, LanceModel):    
    embedding: Vector(VECTOR_LEN, nullable=True) = Field(None, description="This is the embedding vector of title+content")

class _Cluster(LanceModel):
    url: str
    related: list[str]

class _ScalarReranker(Reranker):
    column: str
    direction: str

    def __init__(self, column: str, desc: bool = False):
        super().__init__("relevance")
        self.column = column
        self.direction = "descending" if desc else "ascending"       

    def _add_relevance_score(self, table: pa.Table):
        """Add _relevance_score column based on the scalar column value"""
        scores = table[self.column].to_pylist()
        # Normalize scores to 0-1 range (higher is better)
        min_score = min(scores) if scores else 0
        max_score = max(scores) if scores else 1
        range_score = max_score - min_score if max_score != min_score else 1
        normalized_scores = [(s - min_score) / range_score for s in scores]
        return table.append_column("_relevance_score", pa.array(normalized_scores, type=pa.float32()))

    def _rerank(self, table: pa.Table):
        table = self._add_relevance_score(table)
        return table.sort_by([("_relevance_score", "descending")])

    def rerank_hybrid(self, query: str, vector_results: pa.Table, fts_results: pa.Table):
        table = self._merge_and_keep_scores(vector_results, fts_results)
        return self._rerank(table)

    def rerank_vector(self, query: str, vector_results: pa.Table):
        return self._rerank(vector_results)

    def rerank_fts(self, query: str, fts_results: pa.Table):
        return self._rerank(fts_results)

ORDER_BY_LATEST = _ScalarReranker(column="created", desc=True)

class Beansack: 
    db: lancedb.DBConnection
    tables: dict[str, lancedb.Table]
    allmugs: lancedb.Table
    allsips: lancedb.Table
    allbeans: lancedb.Table
    allpublishers: lancedb.Table
    allchatters: lancedb.Table
    allclusters: lancedb.Table

    def __init__(self, storage_path: str):
        self.db = _connect(storage_path)
        self.tables = {}
        self.tables["beans"] = self.allbeans = self.db.open_table("beans")        
        self.tables["publishers"] = self.allpublishers = self.db.open_table("publishers")
        self.tables["chatters"] = self.allchatters = self.db.open_table("chatters")
        self.tables["mugs"] = self.allmugs = self.db.open_table("mugs")
        self.tables["sips"] = self.allsips = self.db.open_table("sips")
        self.tables["fixed_categories"] = self.fixed_categories = self.db.open_table("fixed_categories")
        self.tables["fixed_sentiments"] = self.fixed_sentiments = self.db.open_table("fixed_sentiments")
        self.tables["clusters"] = self.allclusters = self.db.open_table("clusters")

    # INGESTION functions
    def store_beans(self, beans: list[Bean]):
        if not beans: return 0

        to_store = prepare_beans_for_store(beans)
        to_store = distinct(to_store, "url")        
        result = self.allbeans.merge_insert("url") \
            .when_not_matched_insert_all() \
            .execute([_Bean(**bean.model_dump(exclude_none=True)) for bean in to_store])
        return result.num_inserted_rows
    
    def update_beans(self, beans: list[Bean], columns: list[str] = None):
        if not beans: return 0

        if columns:
            fields = list(set(columns + [K_URL]))
            updates = [bean.model_dump(include=fields) for bean in beans]
        else:
            updates = [bean.model_dump(exclude_none=True) for bean in beans]
            fields = non_null_fields(updates)

        get_field_values = lambda field: [update.get(field) for update in updates]
        result = self.allbeans.merge_insert("url") \
            .when_matched_update_all() \
            .execute(
                pa.table(
                    data={ field: get_field_values(field) for field in fields },
                    schema=pa.schema(list(map(self.allbeans.schema.field, fields)))
                )
            )
        return result.num_updated_rows
    
    # this assuming that the embedding field is already set
    # this is a specialized update that also updates categories, sentiments and clusters
    def update_embeddings(self, beans: list[Bean]):
        if not beans: return 0

        urls = [bean.url for bean in beans]
        vecs = [bean.embedding for bean in beans]

        # inserting along with classification
        categories = self.fixed_categories.search(query=vecs, query_type="vector", vector_column_name=K_EMBEDDING).distance_type("cosine").limit(2).select(["category", "_distance"]).to_pandas()
        sentiments = self.fixed_sentiments.search(query=vecs, query_type="vector", vector_column_name=K_EMBEDDING).distance_type("cosine").limit(2).select(["sentiment", "_distance"]).to_pandas()
        updates = {
            K_URL: urls,
            K_EMBEDDING: vecs,
            K_CATEGORIES: categories.groupby('query_index')['category'].apply(list).sort_index().tolist(),
            K_SENTIMENTS: sentiments.groupby('query_index')['sentiment'].apply(list).sort_index().tolist()
        } 
        result = self.allbeans.merge_insert("url") \
            .when_matched_update_all() \
            .execute(
                pa.table(
                    data=updates,
                    schema=pa.schema(list(map(self.allbeans.schema.field, [K_URL, K_EMBEDDING, K_CATEGORIES, K_SENTIMENTS])))
                )
            )

        # compute clusters with existing items
        clusters = self.allbeans.search(query=vecs, query_type="vector", vector_column_name=K_EMBEDDING).distance_type("l2").distance_range(upper_bound=CLUSTER_EPS).select(["url", "_distance"]).to_pandas()
        self.allclusters.add([
            _Cluster(url=url, related=related) for url, related
                in zip(urls, clusters.groupby('query_index')['url'].apply(list).sort_index().tolist())
        ])   

        return result.num_updated_rows

    def store_publishers(self, publishers: list[Publisher]):
        if not publishers: return 0

        to_store = prepare_publishers_for_store(publishers)  
        to_store = distinct(to_store, K_SOURCE)
        result = self.allpublishers.merge_insert(K_SOURCE) \
            .when_not_matched_insert_all() \
            .execute([_Publisher(**publisher.model_dump(exclude_none=True)) for publisher in to_store])
        return result.num_inserted_rows

    def update_publishers(self, publishers: list[Publisher]):
        if not publishers: return 0

        updates = [publisher.model_dump(exclude_none=True, exclude=[K_BASE_URL]) for publisher in publishers]
        fields = non_null_fields(updates)

        get_field_values = lambda field: [update.get(field) for update in updates]
        result = self.allpublishers.merge_insert(K_SOURCE) \
            .when_matched_update_all() \
            .execute(
                pa.table(
                    data={field: get_field_values(field) for field in fields},
                    schema=pa.schema(list(map(self.allpublishers.schema.field, fields)))
                )
            )
        return result.num_updated_rows

    def store_chatters(self, chatters: list[Chatter]):
        if not chatters: return 0

        to_store = prepare_chatters_for_store(chatters)
        self.allchatters.add([_Chatter(**chatter.model_dump(exclude_none=True, exclude=[K_SHARES, K_UPDATED])) for chatter in to_store])
        return len(to_store)

    def store_mugs(self, mugs: list[Mug]):
        if not mugs: return 0

        to_store = distinct(mugs, "id")
        result = self.allmugs.merge_insert("id") \
            .when_not_matched_insert_all() \
            .execute( [_Mug(**mug.model_dump(exclude_none=True)) for mug in to_store])
        return result.num_inserted_rows
    
    def store_sips(self, sips: list[Sip]):
        if not sips: return 0

        to_store = distinct(sips, "id") 
        result = self.allsips.merge_insert("id") \
            .when_not_matched_insert_all() \
            .execute([_Sip(**sip.model_dump(exclude_none=True)) for sip in to_store])
        return result.num_inserted_rows

    # QUERY functions
    def deduplicate(self, table: str, items: list) -> list:
        if not items: return items    
        idkey = _PRIMARY_KEYS[table]    
        ids = [getattr(item, idkey) for item in items]
        existing_ids = self.tables[table].search().where(f"{idkey} IN ({list_expr(ids)})").select([idkey]).to_list()
        existing_ids = [item[idkey] for item in existing_ids]
        return list(filter(lambda item: getattr(item, idkey) not in existing_ids, items))

    def count_rows(self, table):
        return self.tables[table].count_rows()

    def _query_beans(self,
        kind: str = None, 
        created: datetime = None, collected: datetime = None, updated: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        order = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[Bean]:
        query = self.allbeans.search() if not embedding else self.allbeans.search(query=embedding, query_type="vector", vector_column_name=K_EMBEDDING)      
        where_expr = _where(urls=None, kind=kind, created=created, collected=collected, updated=updated, categories=categories, regions=regions, entities=entities, sources=sources, conditions=conditions)
        if where_expr: query = query.where(where_expr)
        if embedding: query = query.distance_type("cosine")
        if distance: query = query.distance_range(upper_bound = distance)
        if order and embedding: query = query.rerank(order, query_string="default")
        if limit: query = query.limit(limit)
        if offset: query = query.offset(offset)
        if columns: query = query.select(columns+(["_distance"] if embedding else []))
        return query.to_pydantic(_Bean)
    
    def query_latest_beans(self,
        kind: str = None, 
        created: datetime = None, 
        collected: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[Bean]:
        return self._query_beans(
            kind=kind,
            created=created,
            collected=collected,
            categories=categories,
            regions=regions,
            entities=entities,
            sources=sources,
            embedding=embedding,
            distance=distance,
            conditions=conditions,
            order=ORDER_BY_LATEST,
            limit=limit,
            offset=offset,
            columns=columns
        )
    
    def query_trending_beans(self,
        kind: str = None, 
        updated: datetime = None, 
        collected: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[Bean]:
        raise NOT_SUPPORTED
    
    def query_aggregated_beans(self,
        kind: str = None, 
        created: datetime = None, 
        collected: datetime = None,
        updated: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0
    ) -> list[AggregatedBean]:
        beans = self._query_beans(
            kind=kind,
            created=created,
            collected=collected,
            updated=updated,
            categories=categories,
            regions=regions,
            entities=entities,
            sources=sources,
            embedding=embedding,
            distance=distance,
            conditions=conditions,
            order=ORDER_BY_LATEST,
            limit=limit,
            offset=offset
        )
        publishers = self.allpublishers.search().where(_where(sources=[bean.source for bean in beans])).to_pydantic(_Publisher)
        clusters = self.allclusters.search().where(_where(urls=[bean.url for bean in beans])).to_pydantic(_Cluster)
        get_publisher = lambda source: next((pub.model_dump(exclude_none=True, exclude=[K_SOURCE]) for pub in publishers if pub.source == source), {})
        get_cluster = lambda url: next(({K_RELATED: cluster.related, K_CLUSTER_SIZE: len(cluster.related)} for cluster in clusters if cluster.url == url), {})
        beans = [AggregatedBean(**bean.model_dump(exclude_none=True), **get_publisher(bean.source), **get_cluster(bean.url)) for bean in beans]
        # TODO: add cluster_id -- related with the highest cluster_size
        # TODO: add aggregated chatter stats when ready
        # Additional aggregation logic can be added here
        return beans

    def query_aggregated_chatters(self, updated: datetime, limit: int = None):        
        raise NOT_SUPPORTED
    
    def query_publishers(self, conditions: list[str] = None, limit: int = None):  
        query = self.allpublishers.search()
        if conditions: query = query.where(_where(conditions=conditions))
        if limit: query = query.limit(limit)
        return query.to_pydantic(_Publisher)
    
    # MAINTENANCE functions
    def refresh_aggregated_chatters(self):
        log.info("refreshing aggregated chatters - not yet implemented")
        pass

    def refresh(self):
        [table.optimize() for table in self.tables.values()]

    def close(self):
        del self.db
        del self.tables


def create_db(storage_path: str, factory_dir: str):
    db = _connect(storage_path)
    beans = db.create_table("beans", schema=_Bean, exist_ok=True)
    publishers = db.create_table("publishers", schema=_Publisher, exist_ok=True)
    chatters = db.create_table("chatters", schema=_Chatter, exist_ok=True)
    mugs = db.create_table("mugs", schema=_Mug, exist_ok=True)
    sip = db.create_table("sips", schema=_Sip, exist_ok=True)
    categories = db.create_table(
        "fixed_categories", 
        pd.read_parquet(f"{factory_dir}/categories.parquet"),
        mode="overwrite"
    )
    sentiments = db.create_table(
        "fixed_sentiments", 
        pd.read_parquet(f"{factory_dir}/sentiments.parquet"),
        mode="overwrite"
    )
    db.create_table(
        "fixed_sentiments", 
        pd.read_parquet(f"{factory_dir}/sentiments.parquet"),
        mode="overwrite"
    )
    clusters = db.create_table("clusters", schema = _Cluster, exist_ok=True)

    beans.create_scalar_index(K_URL, index_type="BTREE")
    beans.create_scalar_index(K_KIND, index_type="BITMAP")
    beans.create_scalar_index(K_CREATED, index_type="BTREE")
    beans.create_index(vector_column_name=K_EMBEDDING, metric="cosine", index_type="IVF_HNSW_SQ")
    beans.create_scalar_index(K_CATEGORIES, index_type="LABEL_LIST")
    beans.create_scalar_index(K_REGIONS, index_type="LABEL_LIST")
    beans.create_scalar_index(K_ENTITIES, index_type="LABEL_LIST")
    publishers.create_scalar_index(K_SOURCE, index_type="BTREE")
    chatters.create_scalar_index(K_URL, index_type="BTREE")
    clusters.create_scalar_index(K_URL, index_type="BTREE")

    return Beansack(storage_path)

def _connect(storage_path: str):
    storage_options = None
    if storage_path.startswith("s3://"):
        storage_options = {
            "access_key_id": os.getenv("S3_ACCESS_KEY_ID"),
            "secret_access_key": os.getenv("S3_SECRET_ACCESS_KEY"),
            "endpoint": os.getenv("S3_ENDPOINT"),
            "region": os.getenv("S3_REGION"),
            "timeout": "60s"
        }
    return lancedb.connect(
        uri=storage_path, 
        read_consistency_interval = timedelta(hours=1),
        storage_options=storage_options
    )

list_expr = lambda items: ", ".join(f"'{item}'" for item in items)
date_expr = lambda date_val: f"date '{date_val.strftime('%Y-%m-%d')}'"

def _where(
    urls: list[str] = None,
    kind: str = None,
    created: datetime = None,
    collected: datetime = None,
    updated: datetime = None,
    categories: list[str] = None,
    regions: list[str] = None,
    entities: list[str] = None,
    sources: list[str] = None,  
    conditions: list[str] = None
):
    exprs = []
    if urls: exprs.append(f"url IN ({list_expr(urls)})")
    if kind: exprs.append(f"kind = '{kind}'")
    if created: exprs.append(f"created >= {date_expr(created)}")
    if collected: exprs.append(f"collected >= {date_expr(collected)}")
    if updated: raise NOT_SUPPORTED
    if categories: exprs.append(f"ARRAY_HAS_ANY(categories, [{list_expr(categories)}])")
    if regions: exprs.append(f"ARRAY_HAS_ANY(regions, [{list_expr(regions)}])")
    if entities: exprs.append(f"ARRAY_HAS_ANY(entities, [{list_expr(entities)}])")
    if sources: exprs.append(f"source IN ({list_expr(sources)})")
    if conditions: exprs.extend([c for c in conditions if c])

    if exprs: return " AND ".join(exprs)