from contextlib import contextmanager
import os
import logging
from pathlib import Path
from typing import Any
import pandas as pd
from psycopg import sql
from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector, Vector
from .models import *
from .utils import *
from .database import *
from retry import retry

TIMEOUT = 270  # seconds
RETRY_COUNT = 3
RETRY_DELAY = (10,120)  # seconds

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

log = logging.getLogger(__name__)

class Postgres(Beansack):
    pool: ConnectionPool

    def __init__(self, conn_str: str):
        """Initialize the Beansack with a PostgreSQL connection string."""
        self.pool = ConnectionPool(
            conn_str, 
            min_size=0,
            max_size=32,
            timeout=TIMEOUT,
            max_idle=TIMEOUT,
            num_workers=os.cpu_count(),
            configure=register_vector
        )
        self.pool.open()

    @contextmanager
    def cursor(self):
        """Get a new transaction context manager."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                yield cur
                conn.commit()
    
    # STORE METHODS
    def deduplicate(self, table: str, items: list) -> list:
        if not items: return items
        get_id = lambda item: getattr(item, _PRIMARY_KEYS[table])
        ids = [get_id(item) for item in items]

        SQL_DEDUP = sql.SQL("""
        SELECT unnest(%(ids)s::varchar[]) AS id
        EXCEPT
        SELECT {pk_col} FROM {table};
        """).format(
            table=sql.Identifier(table),
            pk_col=sql.Identifier(_PRIMARY_KEYS[table])
        )
        non_existing_ids = self._query_scalars(SQL_DEDUP, {"ids": ids})
        return [item for item in items if get_id(item) in non_existing_ids]

    @retry(tries=RETRY_COUNT, jitter=RETRY_DELAY)
    def _store(self, table: str, items: list[dict | BaseModel], override: bool = False) -> int:
        if not items: return 0

        if isinstance(items[0], BaseModel): data = [item.model_dump() for item in items]
        elif isinstance(items[0], dict): data = items
        else: raise ValueError("Items must be a list of dicts or BaseModel instances.")

        columns = non_null_fields(data)
        if not columns: return 0

        fields = sql.SQL(', ').join(map(sql.Identifier, columns))
        placeholders = sql.SQL(', ').join([sql.Placeholder(name=c) for c in columns])
        on_conflict = sql.SQL("ON CONFLICT ({}) DO NOTHING").format(sql.Identifier(_PRIMARY_KEYS[table])) if table in _PRIMARY_KEYS else sql.SQL("")
        SQL_INSERT = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({placeholders}) {on_conflict}").format(
            table=sql.Identifier(table),
            fields=fields,
            placeholders=placeholders,
            on_conflict=on_conflict
        )
        
        with self.cursor() as cur:
            if override: cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table)))
            cur.executemany(SQL_INSERT, data)
            count = cur.rowcount
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
        
        setters = sql.SQL(', ').join(
            sql.Composed([
                sql.Identifier(col),
                sql.SQL(" = "),
                sql.Placeholder(col)
            ]) for col in data[0].keys() if col != pk
        )
        SQL_UPDATE = sql.SQL("UPDATE {table} SET {setters} WHERE {pk_field} = {pk_placeholder};").format(
            table=sql.Identifier(table),
            setters=setters,
            pk_field=sql.Identifier(pk),
            pk_placeholder=sql.Placeholder(pk)
        )
        with self.cursor() as cur:
            cur.executemany(SQL_UPDATE, data)
            count = cur.rowcount
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
        urls = [bean.url for bean in data]
        embeddings = [Vector(bean.embedding) for bean in data]
        SQL_UPDATE = """
        UPDATE beans AS b
        SET
            embedding = d.embedding::vector,
            categories = (
                SELECT ARRAY(
                    SELECT category
                    FROM fixed_categories fc
                    ORDER BY d.embedding <=> fc.embedding LIMIT 2
                )
            ),
            sentiments = (
                SELECT ARRAY(
                    SELECT sentiment
                    FROM fixed_sentiments fs
                    ORDER BY d.embedding <=> fs.embedding LIMIT 2
                )
            )
        FROM (
            SELECT 
                u.url,
                u.embedding
            FROM unnest(%s::varchar[], %s) AS u(url, embedding)
        ) AS d
        WHERE b.url = d.url;
        """
        
        with self.cursor() as cur:
            cur.execute(SQL_UPDATE, (urls, embeddings))
            count = cur.rowcount
        return count
    
    def update_publishers(self, publishers: list[Publisher]):
        """Store a list of Publishers in the database."""
        if not publishers: return 0
        return self._update(PUBLISHERS, distinct(publishers, K_SOURCE))    

    @retry(tries=RETRY_COUNT, jitter=RETRY_DELAY)
    def _query_composites(self, expr: str, params: dict = None) -> list[Any]:
        with self.pool.connection() as conn:
            with conn.execute(expr, params=params, binary=True) as cur:
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
                items = [dict(zip(cols, row)) for row in rows]
        return items    

    @retry(tries=RETRY_COUNT, jitter=RETRY_DELAY)
    def _query_scalars(self, expr: str, params: dict = None) -> list[str]:
        with self.pool.connection() as conn:
            with conn.execute(expr, params=params, binary=True) as cur: 
                rows = cur.fetchall()         
                items = [row[0] for row in rows]
        return items
    
    @retry(tries=RETRY_COUNT, jitter=RETRY_DELAY)
    def _query_one(self, expr: str, params: dict = None):
        with self.pool.connection() as conn:
            with conn.execute(expr, params=params, binary=True) as cur: 
                result = cur.fetchone()         
        return result[0]

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
        select_expr, params = f"SELECT {fields} FROM {table} ", {}
        where_exprs, where_params = _where( 
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
        if where_exprs: 
            select_expr += f"{where_exprs} "
            params.update(where_params)
        
        if embedding and not distance: order = f"{order}, {ORDER_BY_DISTANCE}" if order else ORDER_BY_DISTANCE
        if order: select_expr += f"ORDER BY {order} "

        if limit or offset:
            limit_expr, limit_params = _limit(limit=limit, offset=offset)
            select_expr += limit_expr
            params.update(limit_params)
        
        items = self._query_composites(select_expr, params)
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
    ) -> list[Bean]:
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
    ) -> list[AggregatedBean]:
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
    ) -> list[AggregatedBean]:
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

    def query_aggregated_chatters(self, urls: list[str] = None, updated: datetime = None, limit: int = 0, offset: int = 0, columns: list[str] = None) -> list[AggregatedBean]:        
        return self._fetch_all(
            table="_materialized_chatter_aggregates",
            urls=urls,
            updated=updated,            
            order=ORDER_BY_TRENDING,
            limit=limit,
            offset=offset,
            columns=columns
        )

    def query_publishers(self, collected: datetime = None, sources: list[str] = None, conditions: list[str] = None, limit: int = 0, offset: int = 0, columns: list[str] = None) -> list[Publisher]:
        return self._fetch_all(
            table=PUBLISHERS,
            collected=collected,
            sources=sources,
            conditions=conditions,
            limit=limit,
            offset=offset,
            columns=columns
        )
    
    def query_chatters(self, collected: datetime = None, sources: list[str] = None, conditions: list[str] = None, limit: int = 0, offset: int = 0, columns: list[str] = None) -> list[Chatter]:
        return self._fetch_all(
            table=CHATTERS,
            collected=collected,
            sources=sources,
            conditions=conditions,
            limit=limit,
            offset=offset,
            columns=columns
        )
    
    def distinct_categories(self, limit: int = 0, offset: int = 0) -> list[str]:
        expr = "SELECT category FROM fixed_categories ORDER BY category "
        limit_expr, limit_params = _limit(limit=limit, offset=offset)
        expr += limit_expr
        return self._query_scalars(expr, limit_params)
    
    def distinct_sentiments(self, limit: int = 0, offset: int = 0) -> list[str]:
        expr = "SELECT sentiment FROM fixed_sentiments ORDER BY sentiment "
        limit_expr, limit_params = _limit(limit=limit, offset=offset)
        expr += limit_expr
        return self._query_scalars(expr, limit_params)
    
    def distinct_entities(self, limit: int = 0, offset: int = 0) -> list[str]:
        expr = "SELECT DISTINCT unnest(entities) as entity FROM beans WHERE entities IS NOT NULL ORDER BY entity "
        limit_expr, limit_params = _limit(limit=limit, offset=offset)
        expr += limit_expr
        return self._query_scalars(expr, limit_params)
    
    def distinct_regions(self, limit: int = 0, offset: int = 0) -> list[str]:
        expr = "SELECT DISTINCT unnest(regions) as region FROM beans WHERE regions IS NOT NULL ORDER BY region "
        limit_expr, limit_params = _limit(limit=limit, offset=offset)
        expr += limit_expr
        return self._query_scalars(expr, limit_params)
    
    def distinct_publishers(self, limit: int = 0, offset: int = 0) -> list[str]:
        expr = "SELECT source FROM publishers ORDER BY source "
        limit_expr, limit_params = _limit(limit=limit, offset=offset)
        expr += limit_expr
        return self._query_scalars(expr, limit_params)

    def count_rows(self, table: str, conditions: list[str] = None) -> int:
        expr = f"SELECT count(*) FROM {table} "
        where_exprs, _ = _where(conditions=conditions)
        if where_exprs: expr += where_exprs        
        return self._query_one(expr)
    
    # MAINTENANCE METHODS
    def execute(self, sql: str):
        """Execute arbitrary SQL commands."""
        with self.cursor() as cursor:
            cursor.execute(sql)

    def refresh_classifications(self):  
        SQL_UPDATE_CLASSIFICATIONS = """
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
        self.execute(SQL_UPDATE_CLASSIFICATIONS)

    def refresh_clusters(self):
        SQL_REFRESH_CLUSTERS = """
        REFRESH MATERIALIZED VIEW _materialized_clusters;
        REFRESH MATERIALIZED VIEW _materialized_cluster_aggregates;
        """
        self.execute(SQL_REFRESH_CLUSTERS)

    def refresh_chatters(self):
        self.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY _materialized_chatter_aggregates;")
        
    def optimize(self):
        SQL_REFRESH_VIEWS = """
        REFRESH MATERIALIZED VIEW CONCURRENTLY _materialized_chatter_aggregates;
        REFRESH MATERIALIZED VIEW _materialized_clusters;
        REFRESH MATERIALIZED VIEW _materialized_cluster_aggregates;
        """
        self.execute(SQL_REFRESH_VIEWS)
    
    def close(self):
        self.pool.close()
        
def create_db(conn_str: str, factory_dir: str) -> Postgres:
    """Create the new tables, views, indexes etc."""
    db = Postgres(conn_str)  # Just to ensure the DB is reachable
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

def _limit(limit: int = 0, offset: int = 0) -> tuple[str, dict]:
    expr, params = "", {}
    if limit: 
        expr += "LIMIT %(limit)s "
        params['limit'] = limit
    if offset: 
        expr += "OFFSET %(offset)s "
        params['offset'] = offset
    return expr, params
        