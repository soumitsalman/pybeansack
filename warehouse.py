import os
import random
import logging
import duckdb
import pandas as pd
from datetime import datetime
from models import *
from icecream import ic

VECTOR_LEN = 384

SQL_INSERT_PARQUET = "CALL ducklake_add_data_files('warehouse', ?, ?, ignore_extra_columns => true);"
SQL_INSERT_DF = """INSERT INTO warehouse.{table} 
SELECT * FROM df;"""

SQL_INSERT_CATEGORIES = """INSERT INTO warehouse.bean_categories
SELECT e.url, LIST(m.category) AS categories
FROM warehouse.bean_embeddings e 
LEFT JOIN LATERAL (
    SELECT category FROM warehouse.categories c
    ORDER BY array_cosine_distance(e.embedding::FLOAT[384], c.embedding::FLOAT[384]) 
    LIMIT 3
) AS m ON TRUE
WHERE e.url IN ({urls})
GROUP BY e.url;"""

SQL_INSERT_SENTIMENTS = """INSERT INTO warehouse.bean_sentiments
SELECT e.url, LIST(m.sentiment) AS sentiments
FROM warehouse.bean_embeddings e
LEFT JOIN LATERAL (
    SELECT sentiment FROM warehouse.sentiments s
    ORDER BY array_cosine_distance(e.embedding::FLOAT[384], s.embedding::FLOAT[384]) 
    LIMIT 3
) AS m ON TRUE
WHERE e.url IN ({urls})
GROUP BY e.url;"""

SQL_INSERT_CLUSTERS = """INSERT INTO warehouse.bean_clusters
SELECT e1.url, m.url as related, m.distance
FROM warehouse.bean_embeddings e1
LEFT JOIN LATERAL (
    SELECT e2.url, array_distance(e1.embedding::FLOAT[384], e2.embedding::FLOAT[384]) as distance
    FROM warehouse.bean_embeddings e2
    WHERE distance <= 0.43 AND e1.url <> e2.url
) as m ON TRUE
WHERE e1.url IN ({urls}) AND m.url IS NOT NULL;"""

SQL_EXISTS = "SELECT {field} FROM warehouse.{table} WHERE {field} IN ({placeholders})"
SQL_UNPROCESSED_BEANS = "SELECT url, content, created FROM warehouse.unprocessed_beans_view"
SQL_SCALAR_SEARCH_BEANS = "SELECT * EXCLUDE(title_length, summary_length, content_length) FROM warehouse.processed_beans_view"
SQL_VECTOR_SEARCH_BEANS = f"SELECT * EXCLUDE(title_length, summary_length, content_length), array_cosine_distance(embedding::FLOAT[{VECTOR_LEN}], ?::FLOAT[{VECTOR_LEN}]) AS distance FROM warehouse.processed_beans_view"
SQL_LATEST_CHATTERS = "SELECT * FROM warehouse.bean_chatters_view"
SQL_REFRESH = """
TRUNCATE TABLE warehouse.bean_clusters;
INSERT INTO warehouse.bean_clusters 
SELECT * FROM warehouse.bean_clusters_view;

TRUNCATE TABLE warehouse.bean_categories;
INSERT INTO warehouse.bean_categories 
SELECT * FROM warehouse.bean_categories_view;

TRUNCATE TABLE warehouse.bean_sentiments;
INSERT INTO warehouse.bean_sentiments 
SELECT * FROM warehouse.bean_sentiments_view;
"""
SQL_COMPACT = """
CALL warehouse.merge_adjacent_files();
CALL ducklake_cleanup_old_files('warehouse', older_than => now() - INTERVAL '1 day');
"""

log = logging.getLogger(__name__)

def _prepare_cores_for_storage(items: list[BeanCore]) -> list[BeanCore]:
    for item in items:
        item.title_length = len(item.title.split()) if item.title else 0
        item.summary_length = len(item.summary.split()) if item.summary else 0
        item.content_length = len(item.content.split()) if item.content else 0
        item.created = item.created or datetime.now()
        item.collected = item.collected or datetime.now()
    return items

def _create_where_exprs(
    kind: str = None,
    created: datetime = None,
    updated: datetime = None,
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
    if updated: conditions.append("updated >= ?"), params.append(updated)
    if categories: conditions.append("ARRAY_HAS_ANY(categories, ?)"), params.append(categories)
    if regions: conditions.append("ARRAY_HAS_ANY(regions, ?)"), params.append(regions)
    if entities: conditions.append("ARRAY_HAS_ANY(entities, ?)"), params.append(entities)
    if sources: conditions.append(f"source IN ({', '.join('?' for _ in sources)})"), params.extend(sources)
    if distance: conditions.append("distance <= ?"), params.append(distance)
    if condition_exprs: conditions.extend(condition_exprs)
    return conditions, params

class Beansack:
    def __init__(self, init_sql: str = "", storage_config: dict = None):
        config = {'threads': max(os.cpu_count() >> 1, 1)}        
        if storage_config: config.update(storage_config)
            
        self.db = duckdb.connect(config=config)
        if not init_sql: return
        with open(init_sql, 'r') as sql_file:
            self.db.execute(sql_file.read())
        log.debug("Data warehouse initialized.")
            
    def _deduplicate(self, table: str, field: str, items: list) -> list:
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

    def _bulk_insert_as_parquet(self, table: str, items: list, dtype_specs):
        if not items: return
        df = pd.DataFrame([item.model_dump() for item in items])
        if dtype_specs: df = df.astype(dtype_specs)
        
        filename = f".data/{table}_{int(datetime.now().timestamp())}_{random.randint(1000, 9999)}.parquet"
        df.to_parquet(filename)
        cursor = self.db.cursor()
        cursor.execute(SQL_INSERT_PARQUET, (table, filename,))
        log.debug(f"Inserted {len(items)} records into {table}.")
        return items

    def _bulk_insert_as_df(self, table: str, items: list, dtype_specs):
        if not items: return
        df = pd.DataFrame([item.model_dump() for item in items])
        # if dtype_specs: df = df.astype(dtype_specs)
        cursor = self.db.cursor()       
        cursor.execute(SQL_INSERT_DF.format(table=table))
        log.debug(f"Inserted {len(items)} records into {table}.")
        return items

    ##### Store methods
    def store_cores(self, items: list[BeanCore]):              
        items = self._deduplicate("bean_cores", "url", items)  
        items = _prepare_cores_for_storage(items)
        return self._bulk_insert_as_df("bean_cores", items, BeanCore.Config.dtype_specs)

    def store_embeddings(self, items: list[BeanEmbedding]):
        items = self._deduplicate("bean_embeddings", "url", list(filter(lambda x: len(x.embedding) == VECTOR_LEN, items)))
        items = self._bulk_insert_as_df("bean_embeddings", items, None)
        self._refresh_classifications(items)
        return items

    def store_gists(self, items: list[BeanGist]):        
        items = self._deduplicate("bean_gists", "url", list(filter(lambda x: x.gist, items)))
        return self._bulk_insert_as_df("bean_gists", items, BeanGist.Config.dtype_specs)

    def store_chatters(self, items: list[Chatter]):       
        # there is no primary key here
        items = list(filter(lambda x: x.likes or x.comments or x.subscribers, items))         
        return self._bulk_insert_as_df("chatters", items, Chatter.Config.dtype_specs)

    def store_sources(self, items: list[Source]):      
        items = self._deduplicate("sources", "source", list(filter(lambda x: x.source and x.base_url, items)))
        return self._bulk_insert_as_df("sources", items, Source.Config.dtype_specs)

    def _refresh_classifications(self, items: list[BeanEmbedding]):
        if not items: return
        placeholders = ','.join('?' for _ in items)
        urls = [item.url for item in items]
        cursor = self.db.cursor()
        cursor.execute(SQL_INSERT_CATEGORIES.format(urls=placeholders), urls)
        cursor.execute(SQL_INSERT_SENTIMENTS.format(urls=placeholders), urls)
        # cursor.execute(SQL_INSERT_CLUSTERS.format(urls=placeholders), urls)
        log.debug(f"Inserted classifications for {len(items)} beans")

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

    def query_contents_with_missing_embeddings(self, conditions: list[str] = None, limit: int = 0) -> list[BeanCore]:        
        query_expr = "SELECT url, title, content FROM warehouse.missing_embeddings_view"
        return self._query_cores(query_expr, conditions, 0, limit)

    def query_contents_with_missing_gists(self, conditions: list[str] = None, limit: int = 0) -> list[BeanCore]:        
        query_expr = "SELECT url, title, content FROM warehouse.missing_gists_view"
        return self._query_cores(query_expr, conditions, 0, limit)

    def query_processed_beans(self, kind: str = None, created: datetime = None, categories: list[str] = None, regions: list[str] = None, entities: list[str] = None, sources: list[str] = None, embedding: list[float] = None, distance: float = 0, offset: int = 0, limit: int = 0) -> list:
        query_expr = SQL_VECTOR_SEARCH_BEANS if embedding else SQL_SCALAR_SEARCH_BEANS
        conditions, params = _create_where_exprs(
            kind=kind, created=created, categories=categories, regions=regions, entities=entities, sources=sources, distance=distance)
        if conditions: query_expr += " WHERE " + " AND ".join(conditions)
        if embedding: params = [embedding] + params

        cursor = self.db.cursor()
        rel = cursor.query(query_expr, params=params)
        rel = rel.order("created DESC")
        if distance: rel = rel.order("distance ASC")
        if offset or limit: rel = rel.limit(limit, offset=offset)
        return [dict(zip(rel.columns, row)) for row in rel.fetchall()]

    def query_latest_chatters(self, updated: datetime, limit: int) -> list:        
        query_expr = SQL_LATEST_CHATTERS
        conditions, params = _create_where_exprs(updated=updated)
        if conditions: query_expr = query_expr + " WHERE " + " AND ".join(conditions)
        cursor = self.db.cursor()
        rel = cursor.sql(query_expr, params=params)
        rel = rel.order("updated DESC")
        if limit: rel = rel.limit(limit)
        return [dict(zip(rel.columns, row)) for row in rel.fetchall()]
    
    ##### Maintenance methods
    def query(self, query_expr: str, params: list = None) -> list[dict]:
        cursor = self.db.cursor()
        rel = cursor.query(query_expr, params=params)
        return [dict(zip(rel.columns, row)) for row in rel.fetchall()]

    def execute(self, sql_expr: str, params: list = None):
        cursor = self.db.cursor()
        cursor.execute(sql_expr, params)

    def refresh_computed_tables(self):
        cursor = self.db.cursor()
        cursor.execute(SQL_REFRESH)
        cursor.close()
        log.debug("Refreshed computed tables.")

    def register_datafile(self, table: str, filename: str):
        cursor = self.db.cursor()
        cursor.execute(SQL_INSERT_PARQUET, (table, filename,))
        log.debug(f"Registered data file {filename} into {table}.")

    def compact(self):
        cursor = self.db.cursor()
        cursor.execute(SQL_COMPACT)
        log.debug("Data compaction completed.")
    
    def close(self):
        if not self.db: return        
        self.db.close()        
        log.debug("Database connection closed.")

    # TODO: backup catalog somewhere
    def backup(self):
        raise NotImplementedError("Backup not implemented")

 