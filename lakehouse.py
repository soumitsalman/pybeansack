from dataclasses import fields
import os
import random
import logging
import duckdb
from duckdb import TransactionException
import pandas as pd
from datetime import datetime
from retry import retry
from .models import *
from .utils import *
from .bases import BeansackBase

from icecream import ic

RETRY_COUNT = 10
RETRY_DELAY = (1,5)  # seconds

DISTINCT_SOURCES = "DISTINCT source"
DISTINCT_CLUSTERS = "DISTINCT cluster_id"
MISSING_EMBEDDING = "embedding IS NULL"
MISSING_DIGEST = "gist IS NULL"
MISSING_SOURCE_METADATA = """
source IS NOT NULL AND
NOT EXISTS (SELECT 1 FROM publishers p WHERE p.source = source)
"""
ORDER_BY_LATEST = "created DESC"
ORDER_BY_TRENDING = "updated DESC, comments DESC, likes DESC"
ORDER_BY_DISTANCE = "distance ASC"

_TYPES = {
    BEANS: Bean,
    PUBLISHERS: Publisher,
    CHATTERS: Chatter,
    "_materialized_chatter_aggregates": AggregatedBean,
    "latest_beans_view": Bean,
    "aggregated_beans_view": AggregatedBean,
    "trending_beans_view": AggregatedBean   
}

_PRIMARY_KEYS = {
    "beans": K_URL,
    "publishers": K_SOURCE
}

log = logging.getLogger(__name__)

def _select(table: str, columns: list[str] = None, embedding: list[float] = None):
    if columns: fields = columns.copy()
    else: fields = ["*"]
    if embedding: fields.append(f"array_cosine_distance(embedding::FLOAT[{VECTOR_LEN}], ?::FLOAT[{VECTOR_LEN}]) AS distance")
    return f"SELECT {', '.join(fields)} FROM warehouse.{table}", [embedding] if embedding else None

def _where(
    kind: str = None,
    created: datetime = None,
    collected: datetime = None,
    updated: datetime = None,
    categories: list[str] = None,
    regions: list[str] = None,
    entities: list[str] = None,
    sources: list[str] = None,  
    distance: float = 0,
    conditions: list[str] = None
):
    exprs = []
    params = []
    if kind: exprs.append("kind = ?"), params.append(kind)
    if created: exprs.append("created >= ?"), params.append(created)
    if collected: exprs.append("collected >= ?"), params.append(collected)
    if updated: exprs.append("updated >= ?"), params.append(updated)
    if categories: exprs.append("ARRAY_HAS_ANY(categories, ?)"), params.append(categories)
    if regions: exprs.append("ARRAY_HAS_ANY(regions, ?)"), params.append(regions)
    if entities: exprs.append("ARRAY_HAS_ANY(entities, ?)"), params.append(entities)
    if sources: exprs.append(f"source IN ({', '.join('?' for _ in sources)})"), params.extend(sources)
    if distance: exprs.append("distance <= ?"), params.append(distance)
    if conditions: exprs.extend(conditions)

    if exprs: return " WHERE "+ (" AND ".join(exprs)), params
    return None, None

_EXCLUDE_COLUMNS = ["tags", "chatter", "publisher", "trend_score", "updated", "distance"]

##### Store methods
def _beans_to_df(beans: list[Bean], columns):
    if not beans: return

    beans = distinct(beans, K_URL)
    if columns: beans = [bean.model_dump(include=columns) for bean in beans] 
    else: beans = [bean.model_dump(exclude_none=True, exclude=_EXCLUDE_COLUMNS) for bean in beans] 
    
    df = pd.DataFrame(beans)
    fields = columns or [col for col in df.columns if df[col].notnull().any()]
    dtype_specs = {field:mapping for field, mapping in Bean.Config.dtype_specs.items() if field in [fields]}
    return df.astype(dtype_specs)

def _publishers_to_df(publishers: list[Publisher], filter_func = lambda x: True):
    if not publishers: return
    publishers = prepare_publishers_for_store(publishers)        
    publishers = distinct(publishers, K_SOURCE)
    if not publishers: return    
    return pd.DataFrame([pub.model_dump(exclude_none=True) for pub in publishers])

class Beansack(BeansackBase):
    db: duckdb.DuckDBPyConnection

    def __init__(self, catalogdb: str, storagedb: str):
        config = {
            'threads': max(os.cpu_count() >> 1, 1),
            'enable_http_metadata_cache': True,
            'ducklake_max_retry_count': 100,
            's3_access_key_id': os.getenv('S3_ACCESS_KEY_ID'),
            's3_secret_access_key': os.getenv('S3_SECRET_ACCESS_KEY'),
            's3_endpoint': os.getenv('S3_ENDPOINT'),
            's3_region': os.getenv('S3_REGION'),
        }
        catalogdb_type = "postgres" if "postgresql://" in catalogdb else "sqlite"

        SQL_CONNECT = f"""
        INSTALL ducklake;
        LOAD ducklake;
        INSTALL {catalogdb_type};
        LOAD {catalogdb_type};
        ATTACH 'ducklake:{catalogdb}' AS warehouse (DATA_PATH '{storagedb}');
        USE warehouse;
        """
        self.db = duckdb.connect(config=config) 
        self.execute(SQL_CONNECT)
        log.debug("Data warehouse connected.")
    
    def _exists(self, table: str, field: str, ids: list) -> list[str]:
        if not ids: return
        SQL_EXISTS = f"SELECT {field} FROM warehouse.{table} WHERE {field} IN ({','.join('?' for _ in ids)})"
        return self.query(SQL_EXISTS, params=ids)

    @retry(exceptions=TransactionException, tries=RETRY_COUNT, jitter=RETRY_DELAY)
    def _execute_df(self, sql, df):
        cursor = self.db.cursor()
        cursor.execute(sql)        
        cursor.close()
        return True

    def store_beans(self, beans: list[Bean]) -> list[Bean]:                   
        if not beans: return
       
        df = _beans_to_df(prepare_beans_for_store(beans), None)
        fields=', '.join(df.columns.to_list())
        if not fields: return

        SQL_INSERT = f"""
        INSERT INTO warehouse.beans ({fields})
        SELECT {fields} FROM df
        WHERE NOT EXISTS (
            SELECT 1 FROM warehouse.beans b
            WHERE b.url = df.url
        );
        """
        current_count = self.count_rows(BEANS)
        self._execute_df(SQL_INSERT, df)
        return self.count_rows(BEANS) - current_count
    
    def store_publishers(self, publishers: list[Publisher]): 
        df = _publishers_to_df(publishers, publisher_filter)
        if df is None: return
        fields=', '.join(df.columns.to_list())
        if not fields: return

        SQL_INSERT = f"""
        INSERT INTO warehouse.publishers ({fields})
        SELECT {fields} FROM df
        WHERE source NOT IN (
            SELECT source FROM warehouse.publishers p
        );
        """
        current_count = self.count_rows(PUBLISHERS)
        self._execute_df(SQL_INSERT, df)
        return self.count_rows(PUBLISHERS) - current_count
    
    def store_chatters(self, chatters: list[Chatter]):       
        if not chatters: return

        df = pd.DataFrame([chatter.model_dump(exclude=[K_SHARES, K_UPDATED]) for chatter in prepare_chatters_for_store(chatters)])        
        fields=', '.join(df.columns.to_list())
        if not fields: return
        SQL_INSERT = f"""
        INSERT INTO warehouse.chatters ({fields})
        SELECT {fields} FROM df;
        """
        return self._execute_df(SQL_INSERT, df)   
    
    # this is a special function that updates embeddings, associated classifications and clustering
    def update_embeddings(self, beans: list[Bean]):
        if not beans: return None

        df = _beans_to_df(beans, [K_URL, K_EMBEDDING])
        if df is None or df.empty: return

        SQL_UPDATE = f"""
        WITH update_pack AS (
            SELECT 
                url,
                ANY_VALUE(embedding) AS embedding,
                LIST(DISTINCT fc.category) as categories,
                LIST(DISTINCT fs.sentiment) as sentiments
            FROM df
            LEFT JOIN LATERAL (
                SELECT category FROM warehouse.fixed_categories
                ORDER BY array_cosine_distance(df.embedding::FLOAT[{VECTOR_LEN}], embedding::FLOAT[{VECTOR_LEN}])
                LIMIT 3
            ) fc ON TRUE
            LEFT JOIN LATERAL (
                SELECT sentiment FROM warehouse.fixed_sentiments
                ORDER BY array_cosine_distance(df.embedding::FLOAT[{VECTOR_LEN}], embedding::FLOAT[{VECTOR_LEN}])
                LIMIT 3
            ) fs ON TRUE
            GROUP BY df.url
        )
        MERGE INTO warehouse.beans
        USING (SELECT * FROM update_pack) AS pack
        USING (url)
        WHEN MATCHED THEN UPDATE SET embedding = pack.embedding, categories = pack.categories, sentiments = pack.sentiments;
        """
        return self._execute_df(SQL_UPDATE, df)
        # return self.refresh_clusters()
    
    def update_beans(self, beans: list[Bean], columns: list[str] = None):
        if not beans: return None

        df = _beans_to_df(beans, list(set(columns+[K_URL])) if columns else None)
        fields = columns or df.columns.to_list()
        if K_URL in fields: fields.remove(K_URL)
        if not fields: return

        updates = [f"{f} = pack.{f}" for f in fields]

        SQL_UPDATE = f"""
        MERGE INTO warehouse.beans
        USING (SELECT url, {', '.join(fields)} FROM df) AS pack
        USING (url)
        WHEN MATCHED THEN UPDATE SET {', '.join(updates)};
        """
        return self._execute_df(SQL_UPDATE, df)    

    def update_publishers(self, publishers: list[Publisher]):
        df = _publishers_to_df(publishers)
        if df is None: return
        fields = df.columns.to_list()
        fields.remove(K_SOURCE)
        if not fields: return
        
        updates = [f"{f} = pack.{f}" for f in fields]
        SQL_UPDATE = f"""
        MERGE INTO warehouse.publishers
        USING (SELECT source, {', '.join(fields)} FROM df) AS pack
        USING (source)
        WHEN MATCHED THEN UPDATE SET {', '.join(updates)};
        """
        return self._execute_df(SQL_UPDATE, df)    

    ###### Query methods
    def deduplicate(self, table: str, items: list) -> list:
        if not items: return items
        idkey = _PRIMARY_KEYS[table]
        ids = [getattr(item, idkey) for item in items]
        existing_ids = self._exists(table, idkey, ids) or []
        return [item for id, item in zip(ids, items) if id not in existing_ids]
    
    def exists(self, urls: list[str]) -> list[str]:
        return self._exists("bean", "url", urls)
    
    def count_rows(self, table, conditions: list[str] = None) -> int:
        where_exprs, _ = _where(conditions=conditions)
        SQL_COUNT = f"SELECT count(*) FROM warehouse.{table} {where_exprs or ''};"
        return self.query_one(SQL_COUNT)

    def _fetch_all(self, 
        table: str = BEANS,
        kind: str = None, 
        created: datetime = None, collected: datetime = None, updated: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        order: str = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[Bean]:
        select_expr, select_params = _select(table, columns, embedding)
        where_expr, where_params = _where(kind, created, collected, updated, categories, regions, entities, sources, distance, conditions)
        if where_expr: select_expr += where_expr
        params = []
        if select_params: params.extend(select_params)
        if where_params: params.extend(where_params)

        cursor = self.db.cursor()
        rel = cursor.query(select_expr, params=params)
        if order: rel = rel.order(order)
        if distance: rel = rel.order(self.ORDER_BY_DISTANCE)
        if offset or limit: rel = rel.limit(limit, offset=offset)
        items = [dict(zip(rel.columns, row)) for row in rel.fetchall()]
        if table in _TYPES: items = [_TYPES[table](**item) for item in items]
        cursor.close()

        return items
    
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
        if columns: fields = list(set(columns + [K_CREATED]))
        else: fields = None
        return self._fetch_all(
            table="latest_beans_view",
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
            columns=fields
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
    ) -> list[AggregatedBean]:
        if columns: fields = list(set(columns + [K_UPDATED, K_COMMENTS, K_LIKES]))
        else: fields = None
        return self._fetch_all(
            table="trending_beans_view",
            kind=kind,
            updated=updated,
            collected=collected,
            categories=categories,
            regions=regions,
            entities=entities,
            sources=sources,
            embedding=embedding,
            distance=distance,
            conditions=conditions,
            order=ORDER_BY_TRENDING,
            limit=limit,
            offset=offset,
            columns=fields
        )
    
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
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ) -> list[AggregatedBean]:
        if columns: fields = list(set(columns + [K_UPDATED, K_COMMENTS, K_LIKES]))
        else: fields = None
        return self._fetch_all(
            table="aggregated_beans_view",
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
            offset=offset,
            columns=fields
        )

    def query_aggregated_chatters(self, urls: list[str] = None, updated: datetime = None, limit: int = 0, offset: int = 0):    
        return self._fetch_all(
            table="_materialized_aggregated_chatters",
            urls=urls,  
            updated=updated,
            limit=limit,
            offset=offset
        ) 
    
    def query_chatters(self, collected: datetime = None, sources: list[str] = None, conditions: list[str] = None, limit: int = 0, offset: int = 0):        
        return self._fetch_all(
            table=CHATTERS,
            collected=collected,
            sources=sources,
            conditions=conditions,
            limit=limit,
            offset=offset
        )
    
    def query_publishers(self, collected: datetime = None, sources: list[str] = None, conditions: list[str] = None, limit: int = 0, offset: int = 0):        
        return self._fetch_all(
            table=PUBLISHERS,
            collected=collected,
            sources=sources,
            conditions=conditions,
            limit=limit,
            offset=offset
        )
    
    ##### Maintenance methods
    def query(self, query_expr: str, params: list = None) -> list[dict]:
        cursor = self.db.cursor()
        rel = cursor.query(query_expr, params=params)
        return [dict(zip(rel.columns, row)) for row in rel.fetchall()]
    
    def query_one(self, query_expr: str, params: list = None):
        cursor = self.db.cursor()
        rel = cursor.query(query_expr, params=params)
        count = rel.fetchone()[0]
        cursor.close()
        return count

    @retry(exceptions=TransactionException, tries=RETRY_COUNT, jitter=RETRY_DELAY)
    def execute(self, expr: str, params: list = None):
        cursor = self.db.cursor()
        cursor.execute(expr, params)
        cursor.close()

    def refresh_classifications(self):
        SQL_INSERT_CLASSIFICATION = f"""
        WITH needs_classification AS (
            SELECT url, embedding FROM warehouse.beans b
            WHERE 
                embedding IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM warehouse._materialized_bean_classifications c
                    WHERE c.url = b.url
                )                
        )
        INSERT INTO warehouse._materialized_bean_classifications
        SELECT 
            nc.url,
            LIST(DISTINCT fc.category) as categories,
            LIST(DISTINCT fs.sentiment) as sentiments
        FROM needs_classification nc
        LEFT JOIN LATERAL (
            SELECT category FROM warehouse.fixed_categories
            ORDER BY array_cosine_distance(nc.embedding::FLOAT[{VECTOR_LEN}], embedding::FLOAT[{VECTOR_LEN}])
            LIMIT 3
        ) fc ON TRUE
        LEFT JOIN LATERAL (
            SELECT sentiment FROM warehouse.fixed_sentiments
            ORDER BY array_cosine_distance(nc.embedding::FLOAT[{VECTOR_LEN}], embedding::FLOAT[{VECTOR_LEN}])
            LIMIT 3
        ) fs ON TRUE
        GROUP BY nc.url
        """
        return self.execute(SQL_INSERT_CLASSIFICATION)
    
    def refresh_clusters(self):
        # calculate and update for a batch of 512, otherwise it dies
        # first insert into related beans
        SQL_INSERT_CLUSTER = f"""
        WITH 
            scope AS (
                SELECT url, embedding FROM warehouse.beans                 
                WHERE 
                    embedding IS NOT NULL
                    AND collected >= CURRENT_TIMESTAMP - INTERVAL '30 days'
            ),
            needs_relating AS (
                SELECT s.* FROM scope s
                LEFT JOIN warehouse._materialized_bean_clusters r ON s.url = r.url
                WHERE r.related IS NULL         
            )
        INSERT INTO warehouse._materialized_bean_clusters        
        SELECT nr.url as url, s.url as related, abs(array_distance(nr.embedding::FLOAT[{VECTOR_LEN}], s.embedding::FLOAT[{VECTOR_LEN}])) as distance 
        FROM needs_relating nr
        CROSS JOIN scope s
        WHERE distance <= {CLUSTER_EPS};

        WITH 
            needs_clustering AS (
                SELECT cl.* FROM warehouse._materialized_bean_clusters cl
                WHERE NOT EXISTS (
                    SELECT 1 FROM warehouse._materialized_bean_cluster_stats stats
                    WHERE stats.url = cl.url 
                )
            ),
            cluster_sizes AS (
                SELECT related, count(*) AS cluster_size 
                FROM warehouse._materialized_bean_clusters 
                GROUP BY related
            )
        INSERT INTO warehouse._materialized_bean_cluster_stats
        SELECT 
            url, 
            FIRST(cl.related ORDER BY cluster_size DESC) AS cluster_id,
            COUNT(*) AS cluster_size
        FROM needs_clustering cl
        INNER JOIN cluster_sizes clsz ON cl.related = clsz.related
        GROUP BY url;
        """
        return self.execute(SQL_INSERT_CLUSTER)

    def refresh_aggregated_chatters(self):  
        SQL_INSERT_AGGREGATES = f"""
        INSERT INTO warehouse._materialized_aggregated_chatters        
        SELECT *, CURRENT_TIMESTAMP as refresh_ts 
        FROM warehouse._internal_aggregated_chatters_view;        

        DELETE FROM warehouse._materialized_aggregated_chatters    
        WHERE refresh_ts < CURRENT_TIMESTAMP - INTERVAL '30 minutes';
        """
        return self.execute(SQL_INSERT_AGGREGATES)  

    def refresh(self):    
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.submit(self.refresh_classifications)
            executor.submit(self.refresh_clusters)
            executor.submit(self.refresh_aggregated_chatters)

    def cleanup(self):
        SQL_CLEANUP = """
        CALL ducklake_expire_snapshots('warehouse', older_than => now() - INTERVAL '1 day');
        CALL ducklake_merge_adjacent_files('warehouse');
        CALL ducklake_cleanup_old_files('warehouse', cleanup_all => true);
        CALL ducklake_delete_orphaned_files('warehouse', cleanup_all => true);
        """     
        self.execute(SQL_CLEANUP)
        log.debug("Compacted data files.")

    def snapshot(self):
        SQL_CURRENT_SNAPSHOT = "SELECT * FROM warehouse.current_snapshot();"
        return self.query_one(SQL_CURRENT_SNAPSHOT)
    
    def close(self):
        if not self.db: return        
        self.db.close()        
        log.debug("Database connection closed.")

def create_db(catalogdb: str, storagedb: str, factory_dir: str = "factory"):
    config = {
        'threads': max(os.cpu_count() >> 1, 1),
        'enable_http_metadata_cache': True,
        'ducklake_max_retry_count': 100
    }

    s3_endpoint = os.getenv('S3_ENDPOINT', '')
    s3_region = os.getenv('S3_REGION', '')
    s3_access_key_id = os.getenv('S3_ACCESS_KEY_ID', '')
    s3_secret_access_key = os.getenv('S3_SECRET_ACCESS_KEY', '')
    
    with open(os.path.join(os.path.dirname(__file__), 'lakehouse.sql'), 'r') as sql_file:
        init_sql = sql_file.read().format(
            # loading prefixed categories and sentiments
            factory=os.path.expanduser(factory_dir),
            catalog_path=catalogdb,
            data_path=os.path.expanduser(storagedb),
            # s3 storage configurations
            s3_access_key_id=s3_access_key_id,
            s3_secret_access_key=s3_secret_access_key,
            s3_endpoint=s3_endpoint,
            s3_region=s3_region,
        )

    db = duckdb.connect(config=config) 
    db.execute(init_sql)
    db.close()
    log.debug("Data warehouse initialized.")
    return Beansack(catalogdb, storagedb)