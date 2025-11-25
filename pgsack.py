import os
import logging
from pathlib import Path
from typing import Any
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, execute_batch
from pgvector.psycopg2 import register_vector

from .models import *
from .utils import *
from icecream import ic

_TYPES = {
    BEANS: Bean,
    PUBLISHERS: Publisher,
    CHATTERS: Chatter,
    "_materialized_chatter_aggregates": AggregatedBean,
    "aggregated_beans_view": AggregatedBean,
    "trending_beans_view": AggregatedBean   
}

_PRIMARY_KEYS = {
    "beans": K_URL,
    "publishers": K_SOURCE
}

ORDER_BY_LATEST = "created DESC"
ORDER_BY_TRENDING = "updated DESC, comments DESC, likes DESC"
ORDER_BY_DISTANCE = "distance ASC"

UPDATE_CLASSIFICATIONS = """
WITH pack AS (
    SELECT 
        b.url,
        ARRAY(
            SELECT category FROM fixed_categories fc
            ORDER BY b.embedding <=> fc.embedding LIMIT 2
        )  AS categories,
        ARRAY(
            SELECT sentiment FROM fixed_sentiments fs
            ORDER BY b.embedding <=> fs.embedding LIMIT 2
        )  AS sentiments
    FROM beans b
    WHERE b.embedding is NOT NULL AND b.categories IS NULL
)
UPDATE beans b
SET 
    categories = pack.categories,
    sentiments = pack.sentiments
FROM pack
WHERE b.url = pack.url;
"""
REFRESH_VIEWS = """
REFRESH MATERIALIZED VIEW CONCURRENTLY _materialized_chatter_aggregates;
REFRESH MATERIALIZED VIEW _materialized_clusters;
REFRESH MATERIALIZED VIEW _materialized_cluster_aggregates;
"""

_PAGE_SIZE = 4096

log = logging.getLogger(__name__)

class Beansack:
    db: psycopg2.extensions.connection

    def __init__(self, conn_str: str):
        """Initialize the Beansack with a PostgreSQL connection string."""
        self.db = psycopg2.connect(conn_str)
        register_vector(self.db, globally=True, arrays=True)
    
    # STORE METHODS

    def deduplicate(self, table: str, items: list) -> list:
        if not items: return items
        get_id = lambda item: getattr(item, _PRIMARY_KEYS[table])
        ids = [get_id(item) for item in items]
        if table == BEANS: existing_ids = {bean.url for bean in self._fetch_all(table, urls=ids)}
        elif table == PUBLISHERS: existing_ids = {pub.source for pub in self._fetch_all(table, sources=ids)}
        else: raise ValueError(f"Deduplication not supported for table: {table}")
        return list(filter(lambda item: get_id(item) not in existing_ids, items))

    def _store(self, table: str, items: list[dict | BaseModel], override: bool = False) -> int:
        if not items: return 0

        if isinstance(items[0], BaseModel): data = [item.model_dump() for item in items]
        elif isinstance(items[0], dict): data = items
        else: raise ValueError("Items must be a list of dicts or BaseModel instances.")

        columns = non_null_fields(data)
        if not columns: return 0

        fields = ', '.join(columns)
        placeholders = ', '.join([f'%({c})s' for c in columns])
        on_conflict = f"ON CONFLICT ({_PRIMARY_KEYS[table]}) DO NOTHING" if table in _PRIMARY_KEYS else ""
        sql = f"INSERT INTO {table} ({fields}) VALUES %s {on_conflict};"

        with self.db.cursor() as cursor:
            if override: cursor.execute(f"TRUNCATE TABLE {table};")
            execute_values(cursor, sql, data, template=f"({placeholders})")
            count = cursor.rowcount
        self.db.commit()
        log.debug("stored", extra={"source": table, "num_items": count})
        return count

    def store_beans(self, beans: list[Bean]):
        """Store a list of Beans in the database."""
        return self._store(BEANS, prepare_beans_for_store(beans))
    
    def store_publishers(self, publishers: list[Publisher]):
        """Store a list of Publishers in the database."""
        return self._store(PUBLISHERS, prepare_publishers_for_store(publishers))
    
    def store_chatters(self, chatters: list[Chatter]):
        """Store a list of Chatters in the database."""
        return self._store(CHATTERS, prepare_chatters_for_store(chatters))
    
    def _update(self, table: str, items: list, columns: list[str] = None):
        if not items: return 0

        pk = _PRIMARY_KEYS[table]
        if columns: data = [bean.model_dump(include=set(columns) | ({pk} if pk else {})) for bean in items]
        else: data = [bean.model_dump() for bean in items]
        
        fields = ', '.join([f"{col} = %({col})s" for col in data[0].keys() if col != pk])
        sql = f"UPDATE {table} SET {fields} WHERE {pk} = %({pk})s;"
        with self.db.cursor() as cursor:
            execute_batch(cursor, sql, data, page_size=_PAGE_SIZE)
            count = cursor.rowcount
        self.db.commit()
        log.debug("updated", extra={"source": table, "num_items": count})
        return count
    
    def update_beans(self, beans: list[Bean], columns: list[str] = None):
        """Partially update a list of Beans in the database."""
        if not beans: return 0
        return self._update(BEANS, distinct(beans, K_URL), columns)
    
    def update_embeddings(self, beans: list[Bean]):
        """Update embeddings for a list of Beans and the computed categories + sentiments during the process."""
        if not beans: return 0
        data = distinct(beans, K_URL)
        data = [(bean.url, bean.embedding) for bean in data if bean.embedding and len(bean.embedding) == VECTOR_LEN]
        sql = """
        WITH 
            data(url, embedding) AS (VALUES %s),
            updates AS (
                SELECT 
                d.url,
                d.embedding,
                ARRAY(
                    SELECT fc.category FROM fixed_categories fc
                    ORDER BY d.embedding <=> fc.embedding LIMIT 2
                )  as categories,
                ARRAY(
                    SELECT fs.sentiment FROM fixed_sentiments fs
                    ORDER BY d.embedding <=> fs.embedding LIMIT 2
                )  as sentiments
                FROM data d
            )
        UPDATE beans b
        SET
            embedding = u.embedding,
            categories = u.categories,
            sentiments = u.sentiments
        FROM updates u
        WHERE b.url = u.url;
        """
        with self.db.cursor() as cursor:
            execute_values(cursor, sql, data, template=f"(%s, %s::vector({VECTOR_LEN}))", page_size=_PAGE_SIZE)
            count = cursor.rowcount
        self.db.commit()
        return count
    
    def update_publishers(self, publishers: list[Publisher]):
        """Store a list of Publishers in the database."""
        if not publishers: return 0
        return self._update(PUBLISHERS, distinct(publishers, K_SOURCE))       

    def _fetch_all(self, 
        table: str,
        urls: list[str] = None,
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
    ):        
        fields = ", ".join(columns) if columns else  "*"
        sql = f"SELECT {fields} FROM {table} "
        where_exprs, params = _where( 
            urls=urls,
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
            conditions=conditions
        )
        if where_exprs: sql += f"{where_exprs} "
        
        # TODO: add order by distance if embedding is provide and no distance is given
        if order: sql += f"ORDER BY {order} "
        if limit: 
            sql += "LIMIT %(limit)s "
            params['limit'] = limit
        if offset: 
            sql += "OFFSET %(offset)s "
            params['offset'] = offset
        
        with self.db.cursor() as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            items = [dict(zip(cols, row)) for row in rows]

        if table in _TYPES: items = [_TYPES[table](**item) for item in items]
        log.debug("queried", extra={"source": table, "num_items": len(items)})
        return items
    
    def query_latest_beans(self,
        kind: str = None, 
        created: datetime = None, collected: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ):
        return self._fetch_all(
            table=BEANS, 
            urls=None,
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
        updated: datetime = None, collected: datetime = None,
        categories: list[str] = None, 
        regions: list[str] = None, entities: list[str] = None, 
        sources: list[str] = None, 
        embedding: list[float] = None, distance: float = 0, 
        conditions: list[str] = None,
        limit: int = 0, offset: int = 0, 
        columns: list[str] = None
    ):
        return self._fetch_all(
            table="trending_beans_view", 
            urls=None,
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
            limit=limit,
            offset=offset,
            columns=columns
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
    ) -> list[Bean]:
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
            columns=columns
        )

    def query_aggregated_chatters(self, urls: list[str] = None, updated: datetime = None, limit: int = 0, offset: int = 0):        
        return self._fetch_all(
            table="_materialized_chatter_aggregates",
            urls=urls,
            updated=updated,            
            order=ORDER_BY_TRENDING,
            limit=limit,
            offset=offset
        )

    def query_publishers(self, sources: list[str] = None, conditions: list[str] = None, limit: int = 0):
        return self._fetch_all(
            table=PUBLISHERS,
            sources=sources,
            conditions=conditions,
            limit=limit
        )

    def count_rows(self, table: str) -> int:
        SQL_COUNT = f"SELECT count(*) FROM {table};"
        with self.db.cursor() as cursor:
            cursor.execute(SQL_COUNT)
            result = cursor.fetchone()
            return result[0]
    
    # MAINTENANCE METHODS
    def execute(self, sql: str):
        """Execute arbitrary SQL commands."""
        with self.db.cursor() as cursor:
            cursor.execute(sql)
        self.db.commit()

    def _refresh_classifications(self):        
        self.execute(UPDATE_CLASSIFICATIONS)
        
    def refresh(self):
        self.execute(REFRESH_VIEWS)
    
    def close(self):
        self.db.close()
        
def create_db(conn_str: str, factory_dir: str) -> Beansack:
    """Create the new tables, views, indexes etc."""
    db = Beansack(conn_str)  # Just to ensure the DB is reachable
    with open(os.path.join(os.path.dirname(__file__), 'pgsack.sql'), 'r') as sql_file:
        init_sql = sql_file.read().format(vector_len = VECTOR_LEN, cluster_eps=CLUSTER_EPS)
    db.execute(init_sql)

    factory_path = Path(factory_dir) 
    _store_parquet(db, factory_path / "categories.parquet", "fixed_categories", True)
    _store_parquet(db, factory_path / "sentiments.parquet", "fixed_sentiments", True)
    return db

def _store_parquet(db, file_path: Path, table_name: str, override: bool = False):
    """Load a parquet file into a database table, converting embedding columns to lists."""
    df = pd.read_parquet(file_path)    
    # Convert embedding column to list if it exists
    if K_EMBEDDING in df.columns: 
        df[K_EMBEDDING] = df[K_EMBEDDING].apply(lambda x: x.tolist() if hasattr(x, 'tolist') else x)
    return db._store(table_name, df.to_dict('records'), override=override)

def _where(
    urls: list[str] = None,
    kind: str = None, 
    created: datetime = None, collected: datetime = None, updated: datetime = None,
    categories: list[str] = None, regions: list[str] = None, entities: list[str] = None, 
    sources: list[str] = None, 
    embedding: list[float] = None, distance: float = 0, 
    conditions: list[str] = None,
):
    exprs = []
    params = {}
    if urls: 
        exprs.append("url = ANY(%(urls)s)")
        params['urls'] = urls
    if kind: 
        exprs.append("kind = %(kind)s")
        params['kind'] = kind
    if created: 
        exprs.append("created >= %(created)s")
        params['created'] = created
    if collected: 
        exprs.append("collected >= %(collected)s")
        params['collected'] = collected
    if updated: 
        exprs.append("updated >= %(updated)s")
        params['updated'] = updated
    # array overlap operator: &&
    if categories: 
        exprs.append("categories && %(categories)s::varchar[]")
        params['categories'] = categories
    if regions: 
        exprs.append("regions && %(regions)s::varchar[]")
        params['regions'] = regions
    if entities: 
        exprs.append("entities && %(entities)s::varchar[]")
        params['entities'] = entities
    if sources: 
        exprs.append("source = ANY(%(sources)s)")
        params['sources'] = sources
    # cosine distance operator: <=>
    if embedding and distance: 
        exprs.append("(embedding <=> %(embedding)s::vector) <= %(distance)s")
        params['embedding'] = embedding
        params['distance'] = distance
    if conditions: exprs.extend(conditions)
    if exprs: return ("WHERE " + " AND ".join(exprs), {k: v for k, v in params.items() if v})
    else: return ("", {})
