import os
from typing import Callable
import duckdb
import logging
import pandas as pd
from .models import *
from .database import *

SQL_INSERT_BEANS = """
INSERT INTO beans (url, kind, source, created, collected, updated, title, embedding, digest) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING;
"""
SQL_CHECKPOINT = "CHECKPOINT;"

SQL_INSERT_CHATTERS = """
INSERT INTO chatters (url, chatter_url, source, group, collected, likes, comments, shares, subscribers) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (url, chatter_url, likes, comments, shares) DO NOTHING
"""

SQL_WHERE_URLS = lambda urls: "url IN (" + ', '.join(f"'{url}'" for url in urls) + ")"
SQL_NOT_WHERE_URLS = lambda urls: "url NOT IN (" + ', '.join(f"'{url}'" for url in urls) + ")"

SQL_SEARCH_BEANS = lambda embedding: f"""
SELECT 
    url, 
    kind,
    source,

    created, 
    collected,
    updated,

    title, 
    array_cosine_distance(
        embedding, 
        {embedding}::FLOAT[384]
    ) as distance
FROM beans
ORDER BY distance DESC
"""
SQL_SEARCH_BEAN_CLUSTER = lambda url: f"""
SELECT 
    url, 
    title,
    array_distance(
        embedding, 
        (SELECT embedding FROM beans WHERE url = '{url}')::FLOAT[384]
    ) as distance            
FROM beans
ORDER BY distance
"""
SQL_TOTAL_CHATTERS = """
SELECT url, 
    SUM(likes) as likes, 
    SUM(comments) as comments, 
    MAX(collected) as collected,
    COUNT(chatter_url) as shares,
    ARRAY_AGG(DISTINCT source) FILTER (WHERE source IS NOT NULL) || ARRAY_AGG(DISTINCT group) FILTER (WHERE group IS NOT NULL) as shared_in

FROM(
    SELECT url, 
        chatter_url, 
        MAX(collected) as collected, 
        MAX(likes) as likes, 
        MAX(comments) as comments, 
        FIRST(source) as source, 
        FIRST(group) as group
    FROM chatters 
    GROUP BY url, chatter_url
) 
GROUP BY url
"""
sql_total_chatters_ndays_ago = lambda last_ndays: f"""
SELECT url, 
    SUM(likes) as likes, 
    SUM(comments) as comments, 
    MAX(collected) as collected,
    COUNT(chatter_url) as shares,
    ARRAY_AGG(DISTINCT source) FILTER (WHERE source IS NOT NULL) || ARRAY_AGG(DISTINCT group) FILTER (WHERE group IS NOT NULL) as shared_in
FROM(
    SELECT url, 
        chatter_url, 
        MAX(collected) as collected, 
        MAX(likes) as likes, 
        MAX(comments) as comments, 
        FIRST(source) as source, 
        FIRST(group) as group
    FROM chatters 
    WHERE collected < CURRENT_TIMESTAMP - INTERVAL '{last_ndays} days'
    GROUP BY url, chatter_url
)
GROUP BY url
"""
sql_search_categories = lambda embedding, min_score: f"""
SELECT text, array_cosine_similarity(embedding, {embedding}::FLOAT[384]) as search_score 
FROM categories 
WHERE search_score >= {min_score}
ORDER BY search_score DESC
"""

ORDER_BY_LATEST = "created DESC"
ORDER_BY_TRENDING = "updated DESC, comments DESC, likes DESC"
ORDER_BY_DISTANCE = "distance ASC"

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
log = logging.getLogger(__name__)

class DuckDB(Beansack):
    storage_path: str
    db: duckdb.DuckDBPyConnection

    def __init__(self, storage_path: str): 
        self.storage_path = os.path.expanduser(storage_path)
        self.db = duckdb.connect(self.storage_path, read_only=False)

    def _exists(self, table: str, field: str, ids: list) -> list[str]:
        if not ids: return
        SQL_EXISTS = f"SELECT {field} FROM warehouse.{table} WHERE {field} IN ({','.join('?' for _ in ids)})"
        return self.query(SQL_EXISTS, params=ids)
    
    def deduplicate(self, table, items):
        if not items: return []
        
        field = _PRIMARY_KEYS.get(table)
        if not field: return items

        ids = [getattr(item, field) for item in items if getattr(item, field)]
        existing_ids = set(row[0] for row in self._exists(table, field, ids))
        deduped = [item for item in items if getattr(item, field) not in existing_ids]
        return deduped

    def _execute_df(self, sql, df):
        with self.db.cursor() as cursor:
            cursor.execute(sql)        
        return True

    def store_beans(self, beans: list[Bean]) -> list[Bean]:                   
        if not beans: return
       
        df = _beans_to_df(prepare_beans_for_store(beans), None)
        fields=', '.join(df.columns.to_list())
        if not fields: return

        SQL_INSERT = f"""
        INSERT INTO beans ({fields})
        SELECT {fields} FROM df
        WHERE NOT EXISTS (
            SELECT 1 FROM beans b
            WHERE b.url = df.url
        );
        """
        return self._execute_df(SQL_INSERT, df)
    
    def store_chatters(self, chatters: list[Chatter]):       
        if not chatters: return

        df = pd.DataFrame([chatter.model_dump(exclude=[K_SHARES, K_UPDATED]) for chatter in prepare_chatters_for_store(chatters)])        
        fields=', '.join(col for col in df.columns if df[col].notnull().any())
        SQL_INSERT = f"""
        INSERT INTO chatters ({fields})
        SELECT {fields} FROM df;
        """
        return self._execute_df(SQL_INSERT, df)     

    def store_publishers(self, publishers: list[Publisher]): 
        df = _publishers_to_df(publishers, publisher_filter)
        if df is None: return
        fields=', '.join(df.columns.to_list())
        if not fields: return

        SQL_INSERT = f"""
        INSERT INTO publishers ({fields})
        SELECT {fields} FROM df
        WHERE source NOT IN (
            SELECT source FROM publishers p
        );
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
                SELECT category FROM fixed_categories
                ORDER BY array_cosine_distance(df.embedding::FLOAT[{VECTOR_LEN}], embedding::FLOAT[{VECTOR_LEN}])
                LIMIT 3
            ) fc ON TRUE
            LEFT JOIN LATERAL (
                SELECT sentiment FROM fixed_sentiments
                ORDER BY array_cosine_distance(df.embedding::FLOAT[{VECTOR_LEN}], embedding::FLOAT[{VECTOR_LEN}])
                LIMIT 3
            ) fs ON TRUE
            GROUP BY df.url
        )
        MERGE INTO beans
        USING (SELECT * FROM update_pack) AS pack
        USING (url)
        WHEN MATCHED THEN UPDATE SET embedding = pack.embedding, categories = pack.categories, sentiments = pack.sentiments;
        """
        return self._execute_df(SQL_UPDATE, df)
    
    def update_beans(self, beans: list[Bean], columns: list[str] = None):
        if not beans: return None

        df = _beans_to_df(beans, list(set(columns+[K_URL])) if columns else None)
        fields = columns or df.columns.to_list()
        if K_URL in fields: fields.remove(K_URL)
        if not fields: return

        updates = [f"{f} = pack.{f}" for f in fields]

        SQL_UPDATE = f"""
        MERGE INTO beans
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
        MERGE INTO publishers
        USING (SELECT source, {', '.join(fields)} FROM df) AS pack
        USING (source)
        WHEN MATCHED THEN UPDATE SET {', '.join(updates)};
        """
        return self._execute_df(SQL_UPDATE, df)    
    
    def _fetch_all(self, 
        table: str = "beans",
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
        columns: list[str] = None,
    ):
        select_expr, select_params = _select(table, columns, embedding)
        where_expr, where_params = _where(urls, kind, created, collected, updated, categories, regions, entities, sources, distance, conditions)
        if where_expr: select_expr += where_expr
        params = []
        if select_params: params.extend(select_params)
        if where_params: params.extend(where_params)

        with self.db.cursor() as cursor:
            rel = cursor.query(select_expr, params=params)
            if order: rel = rel.order(order)
            if distance: rel = rel.order(ORDER_BY_DISTANCE)
            if offset or limit: rel = rel.limit(limit, offset=offset)
            items = [dict(zip(rel.columns, row)) for row in rel.fetchall()]
            if table in _TYPES: items = [_TYPES[table](**item) for item in items]
        return items
    
    def count_rows(self, table, conditions: list[str] = None) -> int:
        where_exprs, _ = _where(conditions=conditions)
        SQL_COUNT = f"SELECT count(*) FROM warehouse.{table} {where_exprs or ''};"
        return self.query_one(SQL_COUNT)

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
    
    def query_aggregated_chatters(self, urls: list[str] = None, updated: datetime = None, limit: int = 0, offset: int = 0, columns: list[str] = None) -> list[AggregatedBean]:
        return self._fetch_all(
            table="_materialized_chatter_aggregates",
            urls=urls,
            updated=updated,
            limit=limit,
            offset=offset,
            columns=columns
        )
    
    def query_chatters(self, collected: datetime = None, sources: list[str] = None, conditions: list[str] = None, limit: int = 0, offset: int = 0, columns: list[str] = None): 
        return self._fetch_all(
            table=CHATTERS,
            collected=collected,
            sources=sources,
            conditions=conditions,
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
    
    def _fetch_all_scalar(self, expr: str, order_by: str = None, limit: int = 0, offset: int = 0, ) -> list[str]:
        with self.db.cursor() as cur:
            rel = cur.query(expr)
            if order_by: rel.order(order_by)
            if limit or offset: rel.limit(limit, offset=offset)
            items = [row[0] for row in rel.fetchall()]
        return items
    
    def distinct_categories(self, limit: int = 0, offset: int = 0) -> list[str]:
        SQL_CATEGORIES = "SELECT category FROM fixed_categories"
        return self._fetch_all_scalar(SQL_CATEGORIES, order_by="category", limit=limit, offset=offset)
    
    def distinct_sentiments(self, limit: int = 0, offset: int = 0) -> list[str]:
        SQL_SENTIMENTS = "SELECT sentiment FROM fixed_sentiments"
        return self._fetch_all_scalar(SQL_SENTIMENTS, order_by="sentiment", limit=limit, offset=offset)
    
    def distinct_entities(self, limit: int = 0, offset: int = 0) -> list[str]:
        SQL_ENTITIES = "SELECT DISTINCT unnest(entities) as entity FROM beans WHERE entities IS NOT NULL"
        return self._fetch_all_scalar(SQL_ENTITIES, order_by="entity", limit=limit, offset=offset)
    
    def distinct_regions(self, limit: int = 0, offset: int = 0) -> list[str]:
        SQL_REGIONS = "SELECT DISTINCT unnest(regions) as region FROM beans WHERE regions IS NOT NULL"
        return self._fetch_all_scalar(SQL_REGIONS, order_by="region", limit=limit, offset=offset)
    
    def distinct_publishers(self, limit: int = 0, offset: int = 0) -> list[str]:
        SQL_SOURCES = "SELECT source FROM publishers"
        return self._fetch_all_scalar(SQL_SOURCES, order_by="source", limit=limit, offset=offset)
    
    # MAINTENANCE FUNCTIONS

    def query(self, query_expr: str, params: list = None) -> list[dict]:
        with self.db.cursor() as cursor:
            rel = cursor.query(query_expr, params=params)
            items = [dict(zip(rel.columns, row)) for row in rel.fetchall()]
        return items    
    
    def query_one(self, query_expr: str, params: list = None):
        with self.db.cursor() as cursor:
            rel = cursor.query(query_expr, params=params)
            count = rel.fetchone()[0]
        return count

    def execute(self, expr: str, params: list = None):
        with self.db.cursor() as cursor:
            cursor.execute(expr, params)

    def refresh_classifications(self):
        pass

    def refresh_clusters(self):
        pass

    def refresh_chatters(self):
        pass
    
    def optimize(self):
        pass

    def close(self):
        self.db.close()

    def backup(self, store_func: Callable):
        with open(self.storage_path, "rb") as data:
            store_func(data)   

def _select(table: str, columns: list[str] = None, embedding: list[float] = None):
    if columns: fields = columns.copy()
    else: fields = ["*"]
    if embedding: fields.append(f"array_cosine_distance(embedding, ?::FLOAT[{VECTOR_LEN}]) AS distance")
    return f"SELECT {', '.join(fields)} FROM {table}", [embedding] if embedding else None

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
    distance: float = 0,
    exprs: list[str] = None
):
    conditions = []
    params = []
    if urls: conditions.append(f"url IN ({', '.join('?' for _ in urls)})"), params.extend(urls)
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

def _limit(limit: int = 0, offset: int = 0) -> tuple[str, list]:
    expr, params = "", []
    if limit: 
        expr += "LIMIT ? "
        params.append(limit)
    if offset: 
        expr += "OFFSET ? "
        params.append(offset)
    return expr, params

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

def create_db(storage_path: str, factory_dir: str) -> Beansack:
    os.makedirs(os.path.dirname(storage_path), exist_ok=True)
    db = DuckDB(storage_path)
    with open(os.path.join(os.path.dirname(__file__), 'ducksack.sql'), 'r') as sql_file:
        init_sql = sql_file.read().format(vector_len=VECTOR_LEN, factory=os.path.expanduser(factory_dir))
    db.db.execute(init_sql)
    return db