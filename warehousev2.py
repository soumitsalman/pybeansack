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
from icecream import ic

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


# SQL_INSERT_PARQUET = "CALL ducklake_add_data_files('warehouse', ?, ?, ignore_extra_columns => true);"
# SQL_INSERT_DF = "INSERT INTO warehouse.{table} SELECT * FROM df;"
# SQL_INSERT_CHATTERS_DF = "INSERT INTO warehouse.chatters SELECT * EXCLUDE(shares) FROM df;"
SQL_INSERT_BEANS = """
INSERT INTO warehouse.beans ({fields})
SELECT {fields} FROM df
WHERE NOT EXISTS (
    SELECT 1 FROM warehouse.beans b
    WHERE b.url = df.url
);
"""

SQL_COUNT_ROWS = "SELECT count(*) FROM warehouse.{table};"

SQL_EXISTS = "SELECT {field} FROM warehouse.{table} WHERE {field} IN ({placeholders})"
SQL_QUERY_BEANS = "SELECT {select_fields} FROM warehouse.processed_beans_view"
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
# This is an expensive operation. So working in batches of 512
SQL_REFRESH_CLUSTERS = f"""
INSERT INTO warehouse.computed_bean_clusters
WITH 
    bean_embeddings_query AS (
        SELECT e.* FROM warehouse.bean_embeddings e
        INNER JOIN ( 
           SELECT * FROM warehouse.bean_cores 
           WHERE collected >= CURRENT_TIMESTAMP - INTERVAL '28 days'
        ) b ON b.url = e.url 
    ),
    missing_clusters_query AS (
        SELECT * FROM warehouse.missing_clusters_view LIMIT 512
    ),
    combined_embeddings AS (
        SELECT * FROM bean_embeddings_query
        UNION
        SELECT * FROM missing_clusters_query
    )
SELECT mcl.url as url, e.url as related, array_distance(mcl.embedding::FLOAT[{VECTOR_LEN}], e.embedding::FLOAT[{VECTOR_LEN}]) as distance 
FROM missing_clusters_query mcl
CROSS JOIN combined_embeddings e
WHERE distance <= {CLUSTER_EPS};
"""
SQL_COUNT_MISSING_CLUSTERS = "SELECT count(*) FROM warehouse.missing_clusters_view;"

SQL_CLEANUP = """
-- CALL ducklake_merge_adjacent_files('warehouse');
CALL ducklake_expire_snapshots('warehouse', older_than => now() - INTERVAL '1 week');
CALL ducklake_cleanup_old_files('warehouse', older_than => now() - INTERVAL '1 week');
CALL ducklake_delete_orphaned_files('warehouse', cleanup_all => true);
"""
SQL_CURRENT_SNAPSHOT = "SELECT * FROM warehouse.current_snapshot();"

_EXCLUDE_BEAN_FIELDS = ["tags", "chatter", "publisher", "trend_score", "updated", "distance"]

log = logging.getLogger(__name__)

def _select(table: str, columns: list[str] = None, embedding: list[float] = None):
    fields = columns.copy() or ["*"]
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
    exprs: list[str] = None
):
    conditions = []
    params = []
    if kind: conditions.append("kind = ?"), params.append(kind)
    if created: conditions.append("created >= ?"), params.append(created)
    if collected: conditions.append("collected >= ?"), params.append(collected)
    if updated: conditions.append("updated >= ?"), params.append(updated)
    if categories: conditions.append("ARRAY_HAS_ANY(categories, ?)"), params.append(categories)
    if regions: conditions.append("ARRAY_HAS_ANY(regions, ?)"), params.append(regions)
    if entities: conditions.append("ARRAY_HAS_ANY(entities, ?)"), params.append(entities)
    if sources: conditions.append(f"source IN ({', '.join('?' for _ in sources)})"), params.extend(sources)
    if distance: conditions.append("distance <= ?"), params.append(distance)
    if exprs: conditions.extend(exprs)

    if conditions: return " WHERE "+ (" AND ".join(conditions)), params
    return None, None

class Beansack:
    def __init__(self, catalogdb: str, storagedb: str, factory_dir: str = "factory"):
        config = {
            'threads': max(os.cpu_count() >> 1, 1),
            'enable_http_metadata_cache': True,
            'ducklake_max_retry_count': 100
        }       
        
        with open(os.path.join(os.path.dirname(__file__), 'warehousev2.sql'), 'r') as sql_file:
            init_sql = sql_file.read().format(
                # loading prefixed categories and sentiments
                factory=os.path.expanduser(factory_dir),
                catalog_path=catalogdb,
                data_path=storagedb,
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
    
    # def to_dataframe(self, items: list, dtype_specs=None):
    #     if not items: return
    #     df = pd.DataFrame([item.model_dump() for item in items])
    #     if dtype_specs: df = df.astype(dtype_specs)
    #     return df    
    
    # def _bulk_insert_as_dataframe(self, table: str, items: list, dtype_specs):
    #     @retry(exceptions=TransactionException, tries=RETRY_COUNT, jitter=RETRY_DELAY)
    #     def _insert_dataframe(df):
    #         if table == "chatters": expr = SQL_INSERT_CHATTERS_DF
    #         else: expr = SQL_INSERT_DF.format(table=table)
    #         cursor = self.db.cursor()
    #         cursor.execute(expr)
    #         cursor.close()

    #     if items: _insert_dataframe(self.to_dataframe(items, dtype_specs))
    #     log.debug(f"inserted", {"source": table, "count": len(items)})
    #     return items

    # def _bulk_insert_as_parquet(self, table: str, items: list, dtype_specs):
    #     @retry(exceptions=TransactionException, tries=RETRY_COUNT, jitter=RETRY_DELAY)
    #     def _insert_parquet(df):
    #         filename = f".beansack/{table}/{int(datetime.now().timestamp())}_{random.randint(1000, 9999)}.parquet"
    #         df.to_parquet(filename)
    #         cursor = self.db.cursor()
    #         cursor.execute(SQL_INSERT_PARQUET, (table, filename,))
    #         cursor.close()

    #     if items: _insert_parquet(self.to_dataframe(items, dtype_specs))
    #     log.debug(f"inserted", {"source": table, "count": len(items)})
    #     return items    

    ##### Store methods
    def _beans_to_df(self, beans: list[Bean], columns):
        if columns: beans = [bean.model_dump(include=columns) for bean in beans] 
        else: beans = [bean.model_dump(exclude_none = True, exclude=_EXCLUDE_BEAN_FIELDS) for bean in beans] 
        
        df = pd.DataFrame(beans)
        fields = columns or [col for col in df.columns if df[col].notnull().any()]
        dtype_specs = {field:mapping for field, mapping in Bean.Config.dtype_specs.items() if field in [fields]}
        return df.astype(dtype_specs)

    @retry(exceptions=TransactionException, tries=RETRY_COUNT, jitter=RETRY_DELAY)
    def _execute_df(self, sql, df):
        cursor = self.db.cursor()
        cursor.execute(sql)        
        cursor.close()
        return True

    def store_beans(self, beans: list[Bean]) -> list[Bean]:                   
        if not beans: return
        bean_filter = lambda bean: bool(bean.title and bean.collected and bean.created and bean.source and bean.kind)
        df = self._beans_to_df(list(filter(bean_filter, rectify_bean_fields(beans))), None)
        # insert the non null columns
        fields=', '.join(df.columns)
        SQL_INSERT_BEANS = f"""
        INSERT INTO warehouse.beans ({fields})
        SELECT {fields} FROM df
        WHERE NOT EXISTS (
            SELECT 1 FROM warehouse.beans b
            WHERE b.url = df.url
        );
        """
        return self._execute_df(SQL_INSERT_BEANS, df)
    
    def update_beans(self, beans: list[Bean], columns: list[str] = None):
        if not beans: return None
        
        df = self._beans_to_df(beans, list(set(columns+[K_URL])) if columns else None)
        fields = columns or df.columns
        updates = [f"{f} = pack.{f}" for f in fields]
        SQL_UPDATE = f"""
        MERGE INTO warehouse.beans
        USING (SELECT url, {', '.join(fields)} FROM df) AS pack
        USING (url)
        WHEN MATCHED THEN UPDATE SET {', '.join(updates)};
        """
        return self._execute_df(SQL_UPDATE, df)    
    
    # def update_embeddings(self, beans: list[Bean]):
    #     if not beans: return None
    #     df = self._beans_to_df(beans, lambda bean: bean.embedding)
    #     SQL_UPDATE_EMBEDDINGS = """
    #     MERGE INTO warehouse.beans
    #     USING (SELECT url, embedding FROM df) AS pack
    #     USING (url)
    #     WHEN MATCHED THEN UPDATE SET embedding = pack.embedding;
    #     """
    #     return self._execute_df(SQL_UPDATE_EMBEDDINGS, df)       

    # def update_gists(self, beans: list[BeanGist]):   
    #     if not beans: return     
    #     df = self._beans_to_df(beans, lambda bean: bean.gist)
    #     SQL_UPDATE_GISTS = """
    #     MERGE INTO warehouse.beans
    #     USING (SELECT url, gist, regions, entities FROM df) AS pack
    #     USING (url)
    #     WHEN MATCHED THEN UPDATE SET gist = pack.gist, regions=pack.regions, entities=pack.entities;
    #     """
    #     return self._execute_df(SQL_UPDATE_GISTS, df)
    

    def update_classifications(self):
        pass

    def store_chatters(self, items: list[Chatter]):       
        # there is no primary key here
        items = list(filter(lambda x: x.likes or x.comments or x.subscribers, items))    
        if not beans: return
        df = self._beans_to_df(
            rectify_bean_fields(beans), 
            lambda bean: bool(bean.title and bean.collected and bean.created and bean.source and bean.kind)
        )
        # insert the non null columns
        fields=', '.join(col for col in df.columns if df[col].notnull().any())
        SQL_INSERT_BEANS = f"""
        INSERT INTO warehouse.beans ({fields})
        SELECT {fields} FROM df
        WHERE NOT EXISTS (
            SELECT 1 FROM warehouse.beans b
            WHERE b.url = df.url
        );
        """
        return self._execute_df(SQL_INSERT_BEANS, df)     
        # return self._bulk_insert_as_dataframe("chatters", items, Chatter.Config.dtype_specs)

    def store_publishers(self, items: list[Publisher]):      
        items = list(filter(lambda x: x.source and x.base_url, items))
        # items = self.deduplicate("publishers", "source", items)
        if not beans: return
        df = self._beans_to_df(
            rectify_bean_fields(beans), 
            lambda bean: bool(bean.title and bean.collected and bean.created and bean.source and bean.kind)
        )
        # insert the non null columns
        fields=', '.join(col for col in df.columns if df[col].notnull().any())
        SQL_INSERT_BEANS = f"""
        INSERT INTO warehouse.beans ({fields})
        SELECT {fields} FROM df
        WHERE NOT EXISTS (
            SELECT 1 FROM warehouse.beans b
            WHERE b.url = df.url
        );
        """
        return self._execute_df(SQL_INSERT_BEANS, df)
        # return self._bulk_insert_as_dataframe("publishers", items, Publisher.Config.dtype_specs)

    ###### Query methods
    def exists(self, urls: list[str]) -> list[str]:
        return self._exists("bean_", "url", urls)
    
    def count_items(self, table):
        cursor = self.db.cursor()
        count = cursor.query(SQL_COUNT_ROWS.format(table=table)).fetchone()[0]
        cursor.close()
        return count

    # def _query_cores(self, query_expr, conditions, offset: int, limit: int) -> list[BeanCore]:
    #     conditions, _ = _where(exprs=conditions)
    #     if conditions: query_expr += " WHERE " + " AND ".join(conditions)

    #     cursor = self.db.cursor()
    #     rel = cursor.query(query_expr)
    #     if offset or limit: rel = rel.limit(limit, offset=offset)
    #     return [BeanCore(**dict(zip(rel.columns, row))) for row in rel.fetchall()]

    def query_latest_beans(self, 
        kind: str = None, 
        created: datetime = None, collected: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        exprs: list[str] = None,
        limit: int = 0, offset: int = 0, 
        select: list[str] = None
    ) -> list[Bean]:
        # create the query expr
        columns = select + ([K_URL] if K_URL not in select else []) + ([K_CREATED] if K_CREATED not in select else [])     
        expr, select_params = _select("beans", columns, embedding)
        where_expr, where_params = _where(kind, created, collected, None, categories, regions, entities, sources, distance, exprs)
        if where_expr: expr += where_expr
        params = []
        if select_params: params.extend(select_params)
        if where_params: params.extend(where_params)

        cursor = self.db.cursor()
        rel = cursor.query(expr, params=params)
        rel = rel.order("created DESC")
        if distance: rel = rel.order("distance ASC")
        if offset or limit: rel = rel.limit(limit, offset=offset)
        beans = [Bean(**dict(zip(rel.columns, row))) for row in rel.fetchall()]
        cursor.close()

        return beans

    # def query_contents_with_missing_embeddings(self, conditions: list[str] = None, limit: int = 0) -> list[BeanCore]:        
    #     query_expr = "SELECT * FROM warehouse.missing_embeddings_view"
    #     return self._query_cores(query_expr, conditions, 0, limit)

    # def query_contents_with_missing_gists(self, conditions: list[str] = None, limit: int = 0) -> list[BeanCore]:        
    #     query_expr = "SELECT * FROM warehouse.missing_gists_view"
    #     return self._query_cores(query_expr, conditions, 0, limit)
    
    # def query_processed_beans(self, kind: str = None, created: datetime = None, categories: list[str] = None, regions: list[str] = None, entities: list[str] = None, sources: list[str] = None, embedding: list[float] = None, distance: float = 0, offset: int = 0, limit: int = 0, select: list[str] = None):        
    #     query_expr = _select("processed_beans_view", select or ["* EXCLUDE(title_length, summary_length, content_length)"], embedding)
    #     conditions, params = _where(
    #         kind=kind, created=created, categories=categories, regions=regions, entities=entities, sources=sources, distance=distance)
    #     if conditions: query_expr += " WHERE " + " AND ".join(conditions)
    #     if embedding: params = [embedding] + params

    #     cursor = self.db.cursor()
    #     rel = cursor.query(query_expr, params=params)
    #     rel = rel.order("created DESC")
    #     if distance: rel = rel.order("distance ASC")
    #     if offset or limit: rel = rel.limit(limit, offset=offset)
    #     return [Bean(**dict(zip(rel.columns, row))) for row in rel.fetchall()]

    def query_bean_chatters(self, collected: datetime, limit: int):        
        query_expr = SQL_LATEST_CHATTERS
        conditions, params = _where(collected=collected)
        if conditions: query_expr = query_expr + " WHERE " + " AND ".join(conditions)
        cursor = self.db.cursor()
        rel = cursor.query(query_expr, params=params)
        if limit: rel = rel.limit(limit)
        return [Chatter(**dict(zip(rel.columns, row))) for row in rel.fetchall()]
    
    def query_publishers(self, conditions: list[str] = None, limit: int = None):        
        query_expr = "SELECT * FROM warehouse.publishers"
        if conditions: query_expr = query_expr + " WHERE " + " AND ".join(conditions)
        cursor = self.db.cursor()
        rel = cursor.query(query_expr)
        if limit: rel = rel.limit(limit)
        return [Publisher(**dict(zip(rel.columns, row))) for row in rel.fetchall()]
    
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
    def execute(self, sql_expr: str, params: list = None):
        cursor = self.db.cursor()
        cursor.execute(sql_expr, params)
        cursor.close()

    def recompute(self):       
        self.execute(SQL_REFRESH_CATEGORIES)
        log.debug("Refreshed computed categories.")
        self.execute(SQL_REFRESH_SENTIMENTS)
        log.debug("Refreshed computed sentiments.")
        # NOTE: clustering needs to be done in smaller batches or else it dies
        # from tqdm import tqdm
        # start_count = self.query_one(SQL_COUNT_MISSING_CLUSTERS)
        # with tqdm(total=start_count, desc="Clustering", unit="beans") as pbar:
        #     while start_count:
        #         self.execute(SQL_REFRESH_CLUSTERS)
        #         current_count = self.query_one(SQL_COUNT_MISSING_CLUSTERS)
        #         pbar.update(start_count - current_count)
        #         start_count = current_count
        while self.query_one(SQL_COUNT_MISSING_CLUSTERS):
            self.execute(SQL_REFRESH_CLUSTERS)
        log.debug("Refreshed computed clusters.")

    def cleanup(self):     
        self.db.execute(SQL_CLEANUP)
        log.debug("Compacted data files.")

    def snapshot(self):
        return self.query_one(SQL_CURRENT_SNAPSHOT)
    
    def close(self):
        if not self.db: return        
        self.db.close()        
        log.debug("Database connection closed.")

    # TODO: backup catalog somewhere
    def backup(self):
        raise NotImplementedError("Backup not implemented")

 