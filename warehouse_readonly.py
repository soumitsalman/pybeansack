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
from icecream import ic

DIGEST_COLUMNS = [K_URL, K_CREATED, K_GIST, K_CATEGORIES, K_SENTIMENTS]

SQL_INIT = """
INSTALL ducklake;
LOAD ducklake;
INSTALL httpfs;
LOAD httpfs;
INSTALL postgres;
LOAD postgres;

ATTACH 'ducklake:{catalog_path}' AS warehouse 
(METADATA_SCHEMA 'beansack', DATA_PATH '{data_path}');
USE warehouse;
"""

log = logging.getLogger(__name__)

def _select(table: str, columns: list[str] = None, embedding: list[float] = None):
    select_columns = columns.copy() if columns else ["*"]
    if embedding: select_columns.append(f"array_cosine_distance(embedding::FLOAT[{len(embedding)}], ?::FLOAT[{len(embedding)}]) AS distance")
    expr = f"SELECT {', '.join(select_columns)} FROM warehouse.{table}"
    # if self.current_snapshot: expr += f" AT (VERSION => {self.current_snapshot})"
    return expr

def _where(
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
        storagedb = os.path.expanduser(storagedb)

        init_sql = SQL_INIT.format(                
            catalog_path=catalogdb,
            data_path=storagedb,
        )

        self.db = duckdb.connect(config=config) 
        self.db.execute(init_sql)
        log.debug("Data warehouse initialized.")       
    
    def query_trending_beans(self, kind: str = None, last_ndays: datetime = None, categories: list[str] = None, regions: list[str] = None, entities: list[str] = None, sources: list[str] = None, embedding: list[float] = None, distance: float = 0, offset: int = 0, limit: int = 0, columns: list[str] = None):        
        query_expr = _select("trending_beans_view", columns, embedding)
        conditions, params = _where(kind=kind, updated=last_ndays, categories=categories, regions=regions, entities=entities, sources=sources, distance=distance)
        if conditions: query_expr += " WHERE " + " AND ".join(conditions)
        if embedding: params = [embedding] + params

        cursor = self.db.cursor()        
        rel = cursor.query(query_expr, params=params)
        rel = rel.order("created DESC")
        if distance: rel = rel.order("distance ASC")
        if offset or limit: rel = rel.limit(limit, offset=offset)
        return [Bean(**dict(zip(rel.columns, row))) for row in rel.fetchall()]

    def query_processed_beans(self, kind: str = None, last_ndays: datetime = None, categories: list[str] = None, regions: list[str] = None, entities: list[str] = None, sources: list[str] = None, embedding: list[float] = None, distance: float = 0, offset: int = 0, limit: int = 0, columns: list[str] = None):        
        query_expr = _select("processed_beans_view", columns, embedding)
        conditions, params = _where(
            kind=kind, created=last_ndays, categories=categories, regions=regions, entities=entities, sources=sources, distance=distance)
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
        conditions, params = _where(updated=collected)
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


 
