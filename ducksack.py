import os
from typing import Callable
import duckdb
from .models import *

SQL_DB_INIT = """
INSTALL vss;
LOAD vss;
"""

SQL_CREATE_BEANS = """
CREATE TABLE IF NOT EXISTS beans (
    url VARCHAR PRIMARY KEY,
    kind VARCHAR,
    source VARCHAR,

    created TIMESTAMP,    
    collected TIMESTAMP,
    updated TIMESTAMP,
      
    title VARCHAR,    
    embedding FLOAT[384],
    digest TEXT
);
"""
SQL_CREATE_BEANS_VECTOR_INDEX = """
CREATE INDEX IF NOT EXISTS beans_embedding 
ON beans 
USING HNSW (embedding)
WITH (metric = 'cosine');
"""
SQL_INSERT_BEANS = """
INSERT INTO beans (url, kind, source, created, collected, updated, title, embedding, digest) 
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING;
"""
SQL_CHECKPOINT = "CHECKPOINT;"

SQL_CREATE_CHATTERS = """
CREATE TABLE IF NOT EXISTS chatters (
    url VARCHAR,
    chatter_url VARCHAR,
    collected TIMESTAMP,
    source VARCHAR,
    group VARCHAR,
    likes INTEGER DEFAULT 0,
    comments INTEGER DEFAULT 0,
    shares INTEGER DEFAULT 0,
    subscribers INTEGER DEFAULT 0,
    UNIQUE (url, chatter_url, likes, comments, shares)
)
"""
SQL_INSERT_CHATTERS = """
INSERT INTO chatters (url, chatter_url, source, group, collected, likes, comments, shares, subscribers) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (url, chatter_url, likes, comments, shares) DO NOTHING
"""

SQL_CREATE_BARISTAS = """
CREATE TABLE IF NOT EXISTS categories (
    id VARCHAR PRIMARY KEY,
    title VARCHAR,
    description TEXT,

    query_kinds VARCHAR[],
    query_sources VARCHAR[],    
    query_tags VARCHAR[],
    query_text VARCHAR,
    query_embedding FLOAT[384],

    owner VARCHAR
);
"""
SQL_CREATE_BARISTA_VECTOR_INDEX = """
CREATE INDEX IF NOT EXISTS categories_embedding 
ON categories 
USING HNSW (embedding)
WITH (metric = 'cosine');
"""
SQL_INSERT_BARISTA = """
INSERT INTO categories (id, title, description, query_kinds, query_sources, query_tags, query_text, query_embedding, owner) VALUES (?,?,?,  ?,?,?,?,?, ?)
ON CONFLICT DO NOTHING
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

class Beansack:
    db_filepath: str
    db_name: str
    db: duckdb.DuckDBPyConnection

    def __init__(self, 
        db_path: str = os.getenv("LOCAL_DB_PATH", ".db"), 
        db_name: str = os.getenv("DB_NAME", "beansack")
    ):
        if not os.path.exists(db_path): os.makedirs(db_path)

        self.db_name = db_name+".db"
        self.db_filepath = os.path.join(db_path, self.db_name)
        self.db = duckdb.connect(self.db_filepath, read_only=False) \
            .execute(SQL_DB_INIT) \
            .execute(SQL_CREATE_BEANS) \
            .execute(SQL_CREATE_CHATTERS) \
            .execute(SQL_CREATE_BARISTAS) \
            .commit()

    def store_beans(self, beans: list[Bean]):
        local_conn = self.db.cursor()
        beans_data = [
            (
                bean.url,
                bean.kind,
                bean.source,

                bean.created,                
                bean.collected,
                bean.updated,

                bean.title,
                bean.embedding,
                bean.digest()
            ) for bean in beans
        ]
        local_conn.executemany(SQL_INSERT_BEANS, beans_data).commit()

    def exists(self, beans: list[Bean]) -> list[str]:
        if not beans: return None

        local_conn = self.db.cursor()
        query = local_conn.sql("SELECT url FROM beans").filter(SQL_WHERE_URLS([bean.url for bean in beans]))
        return {item[0] for item in query.fetchall()}

    def get_beans(self, filter=None, offset=0, limit=0) -> list[Bean]:
        query = "SELECT * FROM beans"
        if filter: query += f" WHERE {filter}"
        if limit: query += f" LIMIT {limit}"
        if offset: query += f" OFFSET {offset}"

        local_conn = self.db.cursor()
        return [Bean(
            url=bean[0],
            kind=bean[1],
            source=bean[2],

            created=bean[3],
            collected=bean[4],
            updated=bean[5],

            title=bean[6],
            embedding=bean[7],
            slots=True
        ) for bean in local_conn.sql(query).fetchall()]

    def search_beans(self, embedding: list[float], max_distance: float = 0.0, limit: int = 0) -> list[Bean]:
        local_conn = self.db.cursor()
        query = local_conn.sql(SQL_SEARCH_BEANS(embedding))
        if max_distance:
            query = query.filter(f"distance <= {max_distance}")
        if limit:
            query = query.limit(limit)
        # result.show()
        return [Bean(
            url=bean[0],
            kind=bean[1],
            source=bean[2],

            created=bean[3],
            collected=bean[4],
            updated=bean[5],

            title=bean[6],
            search_score=bean[7],
            slots=True
        ) for bean in query.fetchall()]
    
    def search_bean_cluster(self, url: str, max_distance: float = 0.0, limit: int = 0) -> list[str]:        
        local_conn = self.db.cursor()
        query = local_conn.query(SQL_SEARCH_BEAN_CLUSTER(url))
        if max_distance:
            query = query.filter(f"distance <= {max_distance}")
        if limit:
            query = query.limit(limit)
        # query.show()
        return [bean[0] for bean in query.fetchall()]

    def store_chatters(self, chatters: list[Chatter]):
        chatters_data = [
            (
                chatter.url,
                chatter.chatter_url,                
                chatter.source,
                chatter.group,
                chatter.collected,
                chatter.likes,
                chatter.comments,
                chatter.shares,
                chatter.subscribers
            ) for chatter in chatters
        ]
        local_conn = self.db.cursor()
        local_conn.executemany(SQL_INSERT_CHATTERS, chatters_data).commit()

    def get_chatters(self, filter = None, offset = 0, limit = 0):
        query = "SELECT * FROM chatters"
        if filter: query += f" WHERE {filter}"
        if limit: query += f" LIMIT {limit}"
        if offset: query += f" OFFSET {offset}"

        local_conn = self.db.cursor()
        return [Chatter(
            url=chatter[0],
            chatter_url=chatter[1],
            collected=chatter[2],
            source=chatter[3],
            group=chatter[4],
            likes=chatter[5],
            comments=chatter[6],
            shares=chatter[7],
            subscribers=chatter[8],
            slots=True
        ) for chatter in local_conn.sql(query).fetchall()]

    # TODO: switch to chatters and deprcate chatter analysis
    def get_latest_chatters(self, last_ndays: int, urls: list[str] = None) -> list[ChatterAnalysis]:
        local_conn = self.db.cursor()
        total = local_conn.query(SQL_TOTAL_CHATTERS)
        ndays_ago = local_conn.query(sql_total_chatters_ndays_ago(last_ndays))
        if urls:
            total = total.filter(SQL_WHERE_URLS(urls))
            ndays_ago = ndays_ago.filter(SQL_WHERE_URLS(urls))
        result = local_conn.query("""
            SELECT 
                total.url as url, 
                total.likes as likes, 
                total.comments as comments, 
                total.collected as collected,
                total.shares as shares,
                total.shared_in as shared_in,                            
                total.likes - COALESCE(ndays_ago.likes, 0) as likes_change, 
                total.comments - COALESCE(ndays_ago.comments, 0) as comments_change, 
                total.shares - COALESCE(ndays_ago.shares, 0) as shares_change, 
                ndays_ago.shared_in as shared_in_change,
            FROM total
            LEFT JOIN ndays_ago ON total.url = ndays_ago.url
            WHERE likes_change <> 0 OR comments_change <> 0 OR shares_change <> 0
        """)
        return [ChatterAnalysis(
            url=chatter[0],
            likes=chatter[1],
            comments=chatter[2],
            collected=chatter[3],
            shares=chatter[4],
            shared_in=chatter[5],
            likes_change=chatter[6],
            comments_change=chatter[7],
            shares_change=chatter[8],
            shared_in_change=chatter[9],
            slots=True
        ) for chatter in result.fetchall()]
    
    # def get_total_chatters(self) -> list[Chatter]:
    #     result = self.db.query(SQL_TOTAL_CHATTERS)        
    #     result.show()
    #     return [Chatter(
    #         url=chatter[0],
    #         likes=chatter[1],
    #         comments=chatter[2],
    #         shares=chatter[3],
    #         slots=True
    #     ) for chatter in result.fetchall()]

    def close(self):
        self.db.close()

    def backup(self, store_func: Callable):
        with open(self.db_filepath, "rb") as data:
            store_func(data)   

  