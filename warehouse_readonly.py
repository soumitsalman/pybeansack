from dataclasses import fields
import os
import random
import logging
import duckdb
from duckdb import TransactionException
from dbcache.api import kvstore
import pandas as pd
from datetime import datetime
from retry import retry
from .models import *
from .utils import *

SQL_INIT = """
INSTALL ducklake;
LOAD ducklake;
INSTALL httpfs;
LOAD httpfs;
INSTALL postgres;
LOAD postgres;

CREATE OR REPLACE SECRET s3secret (
    TYPE s3,
    PROVIDER config,
    ENDPOINT '{s3_endpoint}',
    REGION '{s3_region}',
    KEY_ID '{s3_access_key_id}',
    SECRET '{s3_secret_access_key}'
);

ATTACH 'ducklake:{catalog_path}' AS warehouse 
(METADATA_SCHEMA 'beansack', DATA_PATH '{data_path}');
USE warehouse;
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
    db = None
    current_snapshot = None

    def __init__(self, catalogdb: str, storagedb: str):
        # self.current_snapshot = kvstore(catalogdb).get("current_snapshot")
        config = {
            'threads': max(os.cpu_count() >> 1, 1),
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

        init_sql = SQL_INIT.format(                
            catalog_path=catalogdb,
            data_path=storagedb,
            # s3 storage configurations
            s3_access_key_id=s3_access_key_id,
            s3_secret_access_key=s3_secret_access_key,
            s3_endpoint=s3_endpoint,
            s3_region=s3_region,
        )

        self.db = duckdb.connect(config=config) 
        self.db.execute(init_sql)
        log.debug("Data warehouse initialized.")        

    def _select(self, table: str, columns: list[str] = None, embedding: list[float] = None):
        columns = columns or ["*"]
        if embedding: columns.append(f"array_cosine_distance(embedding::FLOAT[{VECTOR_LEN}], ?::FLOAT[{VECTOR_LEN}]) AS distance")
        expr = f"SELECT {', '.join(columns)} FROM warehouse.{table}"
        if self.current_snapshot: expr += f" AT (VERSION => {self.current_snapshot})"
        return expr
    
    def query_trending_beans(self, kind: str = None, created: datetime = None, categories: list[str] = None, regions: list[str] = None, entities: list[str] = None, sources: list[str] = None, embedding: list[float] = None, distance: float = 0, offset: int = 0, limit: int = 0, columns: list[str] = None):        
        columns = columns or ["*"]
        query_expr = self._select("trending_beans_view", columns, embedding)
        conditions, params = _create_where_exprs(
            kind=kind, created=created, categories=categories, regions=regions, entities=entities, sources=sources, distance=distance)
        if conditions: query_expr += " WHERE " + " AND ".join(conditions)
        if embedding: params = [embedding] + params

        cursor = self.db.cursor()
        rel = cursor.query(query_expr, params=params)
        rel = rel.order("created DESC")
        if distance: rel = rel.order("distance ASC")
        if offset or limit: rel = rel.limit(limit, offset=offset)
        return [Bean(**dict(zip(rel.columns, row))) for row in rel.fetchall()]


    def query_processed_beans(self, kind: str = None, created: datetime = None, categories: list[str] = None, regions: list[str] = None, entities: list[str] = None, sources: list[str] = None, embedding: list[float] = None, distance: float = 0, offset: int = 0, limit: int = 0, columns: list[str] = None):        
        columns = columns or ["* EXCLUDE(title_length, summary_length, content_length)"]
        query_expr = self._select("processed_beans_view", columns, embedding)
        conditions, params = _create_where_exprs(
            kind=kind, created=created, categories=categories, regions=regions, entities=entities, sources=sources, distance=distance)
        if conditions: query_expr += " WHERE " + " AND ".join(conditions)
        if embedding: params = [embedding] + params

        cursor = self.db.cursor()
        rel = cursor.query(query_expr, params=params)
        rel = rel.order("created DESC")
        if distance: rel = rel.order("distance ASC")
        if offset or limit: rel = rel.limit(limit, offset=offset)
        return [Bean(**dict(zip(rel.columns, row))) for row in rel.fetchall()]

    def query_bean_chatters(self, collected: datetime, limit: int):        
        query_expr = self._select("bean_chatters_view")
        conditions, params = _create_where_exprs(collected=collected)
        if conditions: query_expr = query_expr + " WHERE " + " AND ".join(conditions)
        cursor = self.db.cursor()
        rel = cursor.query(query_expr, params=params)
        if limit: rel = rel.limit(limit)
        return [Chatter(**dict(zip(rel.columns, row))) for row in rel.fetchall()]
    
    def query_publishers(self, conditions: list[str] = None, limit: int = None):        
        query_expr = self._select("publishers")
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
    
    def close(self):
        if not self.db: return        
        self.db.close()        
        log.debug("Database connection closed.")


 
