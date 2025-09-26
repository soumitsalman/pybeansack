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

# SQL_INSERT_CATEGORIES = """INSERT INTO warehouse.bean_categories
# SELECT e.url, LIST(m.category) AS categories
# FROM warehouse.bean_embeddings e 
# LEFT JOIN LATERAL (
#     SELECT category FROM warehouse.categories c
#     ORDER BY array_cosine_distance(e.embedding::FLOAT[384], c.embedding::FLOAT[384]) 
#     LIMIT 3
# ) AS m ON TRUE
# WHERE e.url IN ({urls})
# GROUP BY e.url;"""

# SQL_INSERT_SENTIMENTS = """INSERT INTO warehouse.bean_sentiments
# SELECT e.url, LIST(m.sentiment) AS sentiments
# FROM warehouse.bean_embeddings e
# LEFT JOIN LATERAL (
#     SELECT sentiment FROM warehouse.sentiments s
#     ORDER BY array_cosine_distance(e.embedding::FLOAT[384], s.embedding::FLOAT[384]) 
#     LIMIT 3
# ) AS m ON TRUE
# WHERE e.url IN ({urls})
# GROUP BY e.url;"""

# SQL_INSERT_CLUSTERS = """INSERT INTO warehouse.bean_clusters
# SELECT e1.url, m.url as related, m.distance
# FROM warehouse.bean_embeddings e1
# LEFT JOIN LATERAL (
#     SELECT e2.url, array_distance(e1.embedding::FLOAT[384], e2.embedding::FLOAT[384]) as distance
#     FROM warehouse.bean_embeddings e2
#     WHERE distance <= 0.43 AND e1.url <> e2.url
# ) as m ON TRUE
# WHERE e1.url IN ({urls}) AND m.url IS NOT NULL;"""

# related beans identification option 1 - LEFT JOIN LATERAL
# INSERT INTO warehouse.computed_bean_clusters
# SELECT url, related, distance
# FROM missing_clusters_view mcl
# LEFT JOIN LATERAL (
#     SELECT e.url as related, array_distance(mcl.embedding::FLOAT[384], e.embedding::FLOAT[384]) as distance 
#     FROM bean_embeddings e
#     WHERE e.url <> mcl.url AND distance <= 0.43
# ) ON url <> related
# WHERE related IS NOT NULL;

RETRY_COUNT = 10
RETRY_DELAY = (1,5)  # seconds

SQL_INSERT_PARQUET = "CALL ducklake_add_data_files('warehouse', ?, ?, ignore_extra_columns => true);"
SQL_INSERT_DF = "INSERT INTO warehouse.{table} SELECT * FROM df;"
SQL_INSERT_CHATTERS_DF = "INSERT INTO warehouse.chatters SELECT * EXCLUDE(shares) FROM df;"

SQL_EXISTS = "SELECT {field} FROM warehouse.{table} WHERE {field} IN ({placeholders})"
SQL_SCALAR_SEARCH_BEANS = "SELECT * EXCLUDE(title_length, summary_length, content_length) FROM warehouse.processed_beans_view"
SQL_VECTOR_SEARCH_BEANS = f"SELECT * EXCLUDE(title_length, summary_length, content_length), array_cosine_distance(embedding::FLOAT[{VECTOR_LEN}], ?::FLOAT[{VECTOR_LEN}]) AS distance FROM warehouse.processed_beans_view"
SQL_LATEST_CHATTERS = "SELECT * FROM warehouse.bean_chatters_view"

SQL_REFRESH_CATEGORIES = f"""
INSERT INTO warehouse.computed_bean_categories
SELECT url, LIST(category) as categories FROM warehouse.missing_categories_view mc
LEFT JOIN LATERAL (
    SELECT category FROM warehouse.fixed_categories
    ORDER BY array_cosine_distance(mc.embedding::FLOAT[{VECTOR_LEN}], embedding::FLOAT[{VECTOR_LEN}])
    LIMIT 3
) ON TRUE
GROUP BY url;
"""
SQL_REFRESH_SENTIMENTS = f"""
INSERT INTO warehouse.computed_bean_sentiments
SELECT url, LIST(sentiment) as sentiments FROM warehouse.missing_sentiments_view ms
LEFT JOIN LATERAL (
    SELECT sentiment FROM warehouse.fixed_sentiments
    ORDER BY array_cosine_distance(ms.embedding::FLOAT[{VECTOR_LEN}], embedding::FLOAT[{VECTOR_LEN}])
    LIMIT 3
) ON TRUE
GROUP BY url;
"""
SQL_REFRESH_CLUSTERS = f"""
INSERT INTO warehouse.computed_bean_clusters
WITH last_n_days AS (
    SELECT url FROM warehouse.bean_cores WHERE created >= CURRENT_TIMESTAMP - INTERVAL '21 days'
)
SELECT mcl.url as url, e.url as related, array_distance(mcl.embedding::FLOAT[{VECTOR_LEN}], e.embedding::FLOAT[{VECTOR_LEN}]) as distance 
FROM (
    SELECT * FROM warehouse.missing_clusters_view
    WHERE url IN (SELECT * FROM last_n_days)
) mcl
CROSS JOIN (
    SELECT * FROM warehouse.bean_embeddings
    WHERE url IN (SELECT * FROM last_n_days)
) e
WHERE distance <= {CLUSTER_EPS};
"""
SQL_COMPACT = """
-- CALL ducklake_merge_adjacent_files('warehouse');
CALL ducklake_cleanup_old_files('warehouse', cleanup_all => true);
CALL ducklake_delete_orphaned_files('warehouse', cleanup_all => true);
"""

log = logging.getLogger(__name__)

def _create_where_exprs(
    kind: str = None,
    created: datetime = None,
    collected: datetime = None,
    categories: list[str] = None,
    regions: list[str] = None,
    entities: list[str] = None,
    sources: list[str] = None,  
    distance: float = 0,
    condition_exprs: list[str] = None
):
    conditions = []
    params = []
    if kind: conditions.append("kind = ?"), params.append(kind)
    if created: conditions.append("created >= ?"), params.append(created)
    if collected: conditions.append("collected >= ?"), params.append(collected)
    if categories: conditions.append("ARRAY_HAS_ANY(categories, ?)"), params.append(categories)
    if regions: conditions.append("ARRAY_HAS_ANY(regions, ?)"), params.append(regions)
    if entities: conditions.append("ARRAY_HAS_ANY(entities, ?)"), params.append(entities)
    if sources: conditions.append(f"source IN ({', '.join('?' for _ in sources)})"), params.extend(sources)
    if distance: conditions.append("distance <= ?"), params.append(distance)
    if condition_exprs: conditions.extend(condition_exprs)
    return conditions, params

class Beansack:
    def __init__(self, catalogdb: str, storagedb: str, factory_dir: str = "factory"):
        config = {
            'threads': max(os.cpu_count() >> 1, 1),
            # 'preserve_insertion_order': False,
            'enable_http_metadata_cache': True,
            'ducklake_max_retry_count': 100
        }        

        if catalogdb.startswith("postgresql://"): catalogdb = f"postgres:{catalogdb}"
        s3_endpoint, s3_region, s3_access_key_id, s3_secret_access_key = "", "", "", ""
        if storagedb.startswith("s3://"):
            s3_endpoint = os.getenv('S3_ENDPOINT', '')
            s3_region = os.getenv('S3_REGION', '')
            s3_access_key_id = os.getenv('S3_ACCESS_KEY_ID', '')
            s3_secret_access_key = os.getenv('S3_SECRET_ACCESS_KEY', '')
        else: storagedb = os.path.expanduser(storagedb)

        with open(os.path.join(os.path.dirname(__file__), 'warehouse.sql'), 'r') as sql_file:
            init_sql = sql_file.read().format(
                # loading prefixed categories and sentiments
                factory=os.path.expanduser(factory_dir),
                catalog_path=catalogdb,
                data_path=storagedb,
                # s3 storage configurations
                s3_access_key_id=s3_access_key_id,
                s3_secret_access_key=s3_secret_access_key,
                s3_endpoint=s3_endpoint,
                s3_region=s3_region,
            )

        self.db = duckdb.connect(config=config) 
        self.execute(init_sql)
        log.debug("Data warehouse initialized.")
            
    def deduplicate(self, table: str, field: str, items: list) -> list:
        if not items: return items
        ids = [getattr(item, field) for item in items]
        existing_ids = self._exists(table, field, ids) or []
        return [item for id, item in zip(ids, items) if id not in existing_ids]

    def _exists(self, table: str, field: str, ids: list) -> list[str]:
        if not ids: return
        cursor = self.db.cursor()
        rel = cursor.query(
            SQL_EXISTS.format(
                table=table, 
                field=field, 
                placeholders=','.join('?' for _ in ids)
            ), 
            params=ids
        )
        return [row[0] for row in rel.fetchall()]
    
    def to_dataframe(self, items: list, dtype_specs=None):
        if not items: return
        df = pd.DataFrame([item.model_dump() for item in items])
        if dtype_specs: df = df.astype(dtype_specs)
        return df    
    
    def _bulk_insert_as_dataframe(self, table: str, items: list, dtype_specs):
        @retry(exceptions=TransactionException, tries=RETRY_COUNT, jitter=RETRY_DELAY)
        def _insert_dataframe(df):
            if table == "chatters": expr = SQL_INSERT_CHATTERS_DF
            else: expr = SQL_INSERT_DF.format(table=table)
            cursor = self.db.cursor()
            cursor.execute(expr)
            cursor.close()

        if items: _insert_dataframe(self.to_dataframe(items, dtype_specs))
        log.debug(f"inserted", {"source": table, "count": len(items)})
        return items

    def _bulk_insert_as_parquet(self, table: str, items: list, dtype_specs):
        @retry(exceptions=TransactionException, tries=RETRY_COUNT, jitter=RETRY_DELAY)
        def _insert_parquet(df):
            filename = f".beansack/{table}/{int(datetime.now().timestamp())}_{random.randint(1000, 9999)}.parquet"
            df.to_parquet(filename)
            cursor = self.db.cursor()
            cursor.execute(SQL_INSERT_PARQUET, (table, filename,))
            cursor.close()

        if items: _insert_parquet(self.to_dataframe(items, dtype_specs))
        log.debug(f"inserted", {"source": table, "count": len(items)})
        return items    

    ##### Store methods
    def store_cores(self, items: list[BeanCore]):                   
        items = self.deduplicate("bean_cores", "url", items)  
        items = rectify_bean_fields(items)
        return self._bulk_insert_as_dataframe("bean_cores", items, BeanCore.Config.dtype_specs)
    
    def store_embeddings(self, items: list[BeanEmbedding]):
        # items = list(filter(lambda x: len(x.embedding) == VECTOR_LEN, items))
        items = self.deduplicate("bean_embeddings", "url", items)
        return self._bulk_insert_as_dataframe("bean_embeddings", items, None)        

    def store_gists(self, items: list[BeanGist]):        
        # items = list(filter(lambda x: x.gist, items))
        items = self.deduplicate("bean_gists", "url", items)
        return self._bulk_insert_as_dataframe("bean_gists", items, BeanGist.Config.dtype_specs)
    
    def store_chatters(self, items: list[Chatter]):       
        # there is no primary key here
        items = list(filter(lambda x: x.likes or x.comments or x.subscribers, items))         
        return self._bulk_insert_as_dataframe("chatters", items, Chatter.Config.dtype_specs)

    def store_publishers(self, items: list[Publisher]):      
        # items = list(filter(lambda x: x.source and x.base_url, items))
        items = self.deduplicate("publishers", "source", items)
        return self._bulk_insert_as_dataframe("publishers", items, Publisher.Config.dtype_specs)

    ###### Query methods
    def exists(self, urls: list[str]) -> list[str]:
        return self._exists("bean_cores", "url", urls)

    def _query_cores(self, query_expr, conditions, offset: int, limit: int) -> list[BeanCore]:
        conditions, _ = _create_where_exprs(condition_exprs=conditions)
        if conditions: query_expr += " WHERE " + " AND ".join(conditions)

        cursor = self.db.cursor()
        rel = cursor.query(query_expr)
        if offset or limit: rel = rel.limit(limit, offset=offset)
        return [BeanCore(**dict(zip(rel.columns, row))) for row in rel.fetchall()]

    def query_unprocessed_beans(self, conditions: list[str] = None, limit: int = 0) -> list[BeanCore]:
        query_expr = "SELECT * FROM warehouse.unprocessed_beans_view"
        return self._query_cores(query_expr, conditions, 0, limit)

    def query_contents_with_missing_embeddings(self, conditions: list[str] = None, limit: int = 0) -> list[BeanCore]:        
        query_expr = "SELECT * FROM warehouse.missing_embeddings_view"
        return self._query_cores(query_expr, conditions, 0, limit)

    def query_contents_with_missing_gists(self, conditions: list[str] = None, limit: int = 0) -> list[BeanCore]:        
        query_expr = "SELECT * FROM warehouse.missing_gists_view"
        return self._query_cores(query_expr, conditions, 0, limit)

    def query_processed_beans(self, kind: str = None, created: datetime = None, categories: list[str] = None, regions: list[str] = None, entities: list[str] = None, sources: list[str] = None, embedding: list[float] = None, distance: float = 0, offset: int = 0, limit: int = 0):
        query_expr = SQL_VECTOR_SEARCH_BEANS if embedding else SQL_SCALAR_SEARCH_BEANS
        conditions, params = _create_where_exprs(
            kind=kind, created=created, categories=categories, regions=regions, entities=entities, sources=sources, distance=distance)
        if conditions: query_expr += " WHERE " + " AND ".join(conditions)
        if embedding: params = [embedding] + params

        cursor = self.db.cursor()
        rel = cursor.query(query_expr, params=params)
        # rel = rel.order("created DESC")
        if distance: rel = rel.order("distance ASC")
        if offset or limit: rel = rel.limit(limit, offset=offset)
        return [Bean(**dict(zip(rel.columns, row))) for row in rel.fetchall()]

    def query_bean_chatters(self, collected: datetime, limit: int):        
        query_expr = SQL_LATEST_CHATTERS
        conditions, params = _create_where_exprs(collected=collected)
        if conditions: query_expr = query_expr + " WHERE " + " AND ".join(conditions)
        cursor = self.db.cursor()
        rel = cursor.query(query_expr, params=params)
        if limit: rel = rel.limit(limit)
        return [Chatter(**dict(zip(rel.columns, row))) for row in rel.fetchall()]
    
    ##### Maintenance methods
    def query(self, query_expr: str, params: list = None) -> list[dict]:
        cursor = self.db.cursor()
        rel = cursor.query(query_expr, params=params)
        return [dict(zip(rel.columns, row)) for row in rel.fetchall()]

    @retry(exceptions=TransactionException, tries=RETRY_COUNT, jitter=RETRY_DELAY)
    def execute(self, sql_expr: str, params: list = None):
        cursor = self.db.cursor()
        cursor.execute(sql_expr, params)
        cursor.close()

    # TODO: this dies with more than 10000
    def recompute(self):       
        self.execute(SQL_REFRESH_CATEGORIES)
        log.debug("Refreshed computed categories.")
        self.execute(SQL_REFRESH_SENTIMENTS)
        log.debug("Refreshed computed sentiments.")   
        self.execute(SQL_REFRESH_CLUSTERS)
        log.debug("Refreshed computed clusters.")

    def cleanup(self):     
        self.db.execute(SQL_COMPACT)
        log.debug("Compacted data files.")
    
    def close(self):
        if not self.db: return        
        self.db.close()        
        log.debug("Database connection closed.")

    # TODO: backup catalog somewhere
    def backup(self):
        raise NotImplementedError("Backup not implemented")

 