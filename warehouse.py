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
SQL_INSERT_DF = "INSERT INTO warehouse.{table} SELECT * FROM df;"
SQL_EXISTS = "SELECT {field} FROM warehouse.{table} WHERE {field} IN ({placeholders})"
SQL_UNPROCESSED_BEANS = "SELECT url, content, created FROM warehouse.unprocessed_beans_view"
SQL_SCALAR_SEARCH_BEANS = "SELECT * EXCLUDE(title_length, summary_length, content_length) FROM warehouse.processed_beans_view"
SQL_VECTOR_SEARCH_BEANS = f"SELECT * EXCLUDE(title_length, summary_length, content_length), array_cosine_distance(embedding::FLOAT[{VECTOR_LEN}], ?::FLOAT[{VECTOR_LEN}]) AS distance FROM warehouse.processed_beans_view"
SQL_LATEST_CHATTERS = "SELECT * FROM warehouse.bean_chatters_view"
SQL_COMPACT = """
CALL warehouse.merge_adjacent_files();
CALL ducklake_cleanup_old_files('warehouse', older_than => now() - INTERVAL '1 day');
"""

log = logging.getLogger(__name__)

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

class BeanWarehouse:
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
        return self._bulk_insert_as_df("bean_cores", items, BeanCore.Config.dtype_specs)

    def store_embeddings(self, items: list[BeanEmbedding]):
        items = self._deduplicate("bean_embeddings", "url", list(filter(lambda x: len(x.embedding) == VECTOR_LEN, items)))
        return self._bulk_insert_as_df("bean_embeddings", items, None)

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

    ##### Query methods
    def exists(self, urls: list[str]) -> list[str]:
        return self._exists("bean_cores", "url", urls)

    def query_unprocessed_beans(self, conditions: list[str], limit: int) -> list[BeanCore]:        
        query_expr = SQL_UNPROCESSED_BEANS
        conditions, _ = _create_where_exprs(condition_exprs=conditions)
        if conditions: query_expr += " WHERE " + " AND ".join(conditions)

        cursor = self.db.cursor()
        rel = cursor.query(query_expr)
        rel = rel.order("created DESC")
        if limit: rel = rel.limit(limit) 
        return [BeanCore(**dict(zip(rel.columns, row))) for row in rel.fetchall()]
    
    def query_processed_beans(self, kind: str, created: datetime, categories: list[str], regions: list[str], entities: list[str], sources: list[str], embedding: list[float], distance: float, limit: int) -> list:
        query_expr = SQL_VECTOR_SEARCH_BEANS if embedding else SQL_SCALAR_SEARCH_BEANS
        conditions, params = _create_where_exprs(
            kind=kind, created=created, categories=categories, regions=regions, entities=entities, sources=sources, distance=distance)
        if conditions: query_expr += " WHERE " + " AND ".join(conditions)
        if embedding: params = [embedding] + params

        cursor = self.db.cursor()
        rel = cursor.query(query_expr, params=params)
        rel = rel.order("created DESC")
        if distance: rel = rel.order("distance ASC")
        if limit: rel = rel.limit(limit)
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

 