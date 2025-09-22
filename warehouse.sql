INSTALL ducklake;
LOAD ducklake;
INSTALL httpfs;
LOAD httpfs;
INSTALL postgres;
LOAD postgres;
-- INSTALL cache_httpfs FROM community;
-- LOAD cache_httpfs;

-- SET cache_httpfs_type = 'on_disk';
-- SET cache_httpfs_cache_directory = '.cache';

CREATE OR REPLACE SECRET secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID '{s3_access_key_id}',
    SECRET '{s3_secret_access_key}',
    ENDPOINT '{s3_endpoint}',
    REGION '{s3_region}'
);


CREATE OR REPLACE SECRET pgsecrets (
    TYPE postgres,
    PROVIDER config,
    HOST '{pg_host}',
    PORT '{pg_port}',
    USER '{pg_user}',
    PASSWORD '{pg_password}'    
);

ATTACH 'ducklake:postgres:dbname=beansackcatalogdb' AS warehouse (DATA_PATH 's3://{s3_tenant_id}/beansackstoragedb');
USE warehouse;
CALL warehouse.set_option('per_thread_output', true);

CREATE TABLE IF NOT EXISTS bean_cores (
    url VARCHAR NOT NULL,
    kind VARCHAR NOT NULL,
    title VARCHAR,
    title_length UINT16,
    summary TEXT,
    summary_length UINT16,
    content TEXT,
    content_length UINT16,
    restricted_content BOOLEAN,  -- 0 for False, 1 for True
    author VARCHAR,
    source VARCHAR NOT NULL,  
    image_url VARCHAR,
    created TIMESTAMP NOT NULL,  -- ISO format datetime string
    collected TIMESTAMP NOT NULL  -- ISO format datetime string
);

CREATE TABLE IF NOT EXISTS bean_embeddings (
    url VARCHAR NOT NULL,  -- Foreign key to Bean.url
    embedding FLOAT[] NOT NULL
);

CREATE TABLE IF NOT EXISTS bean_gists (
    url VARCHAR NOT NULL,  -- Foreign key to Bean.url
    gist TEXT NOT NULL,
    regions VARCHAR[],
    entities VARCHAR[]
);

CREATE TABLE IF NOT EXISTS chatters (
    chatter_url VARCHAR NOT NULL,
    url VARCHAR NOT NULL,  -- Foreign key to Bean.url
    source VARCHAR,
    forum VARCHAR,
    collected TIMESTAMP, 
    likes UINT32,
    comments UINT32,
    subscribers UINT32
);

CREATE TABLE IF NOT EXISTS sources (
    source VARCHAR NOT NULL,
    base_url VARCHAR NOT NULL,
    title VARCHAR DEFAULT NULL,
    summary TEXT DEFAULT NULL,    
    favicon VARCHAR DEFAULT NULL,
    rss_feed VARCHAR DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS exported_beans (
    url VARCHAR NOT NULL,
    exported TIMESTAMP NOT NULL
);

-- THESE 2 ARE STATIC TABLES. ONCE INITIALIZED OR REGISTERED, THEY DO NOT CHANGE

CREATE TABLE IF NOT EXISTS fixed_categories AS
SELECT * FROM read_parquet('{factory}/categories.parquet');

CREATE TABLE IF NOT EXISTS fixed_sentiments AS
SELECT * FROM read_parquet('{factory}/sentiments.parquet');

CREATE TABLE IF NOT EXISTS fixed_sentiments (
    sentiment VARCHAR NOT NULL,
    embedding FLOAT[] NOT NULL
);

-- THERE ARE COMPUTED TABLES/MATERIALIZED VIEWS THAT ARE REFRESHED PERIODICALLY

CREATE TABLE IF NOT EXISTS computed_bean_clusters (
    url VARCHAR NOT NULL,
    related VARCHAR NOT NULL,
    distance FLOAT DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS computed_bean_categories (
    url VARCHAR NOT NULL,
    categories VARCHAR[] NOT NULL
);

CREATE TABLE IF NOT EXISTS computed_bean_sentiments (
    url VARCHAR NOT NULL,
    sentiments VARCHAR[] NOT NULL
);

-- THERE ARE VIEWS/DYNAMIC QUERIES THAT ARE USED TO SIMPLIFY APP LEVEL QUERIES

CREATE VIEW IF NOT EXISTS missing_embeddings_view AS
SELECT * FROM bean_cores b
WHERE b.url NOT IN (SELECT url FROM bean_embeddings)
ORDER BY created DESC;

CREATE VIEW IF NOT EXISTS missing_gists_view AS
SELECT * FROM bean_cores b
WHERE b.url NOT IN (SELECT url FROM bean_gists)
ORDER BY created DESC;

CREATE VIEW IF NOT EXISTS missing_clusters_view AS
SELECT * FROM bean_embeddings e
WHERE e.url NOT IN (SELECT url FROM computed_bean_clusters);

CREATE VIEW IF NOT EXISTS missing_categories_view AS
SELECT * FROM bean_embeddings e
WHERE e.url NOT IN (SELECT url FROM computed_bean_categories);

CREATE VIEW IF NOT EXISTS missing_sentiments_view AS
SELECT * FROM bean_embeddings e
WHERE e.url NOT IN (SELECT url FROM computed_bean_sentiments);

-- TODO: create a clusters id view


CREATE VIEW IF NOT EXISTS processed_beans_view AS
SELECT * EXCLUDE(e.url, g.url, c.url, s.url) FROM bean_cores b
INNER JOIN bean_embeddings e ON b.url = e.url
INNER JOIN bean_gists g ON b.url = g.url
INNER JOIN computed_bean_categories c ON b.url = c.url
INNER JOIN computed_bean_sentiments s ON b.url = s.url
-- INNER JOIN (
--     SELECT url, FIRST(related ORDER BY distance) as cluster_id
--     FROM bean_clusters
--     GROUP BY url
-- ) cl ON b.url = cl.url;
;

CREATE VIEW IF NOT EXISTS unexported_beans_view AS
SELECT * FROM processed_beans_view pb
WHERE pb.url NOT IN (SELECT url FROM exported_beans);

-- TODO: look to see if it can be replaced with FIRST(collected ORDER BY likes DESC)
CREATE VIEW IF NOT EXISTS bean_chatters_view AS
WITH 
    max_stats AS (
        SELECT 
            chatter_url,
            MAX(likes) as likes, 
            MAX(comments) as comments
        FROM chatters 
        GROUP BY chatter_url
    ),
    first_seen AS (
        SELECT 
            fs.chatter_url,
            MIN(fs.collected) as collected
        FROM chatters fs 
        LEFT JOIN max_stats mx ON fs.chatter_url = mx.chatter_url
        WHERE fs.likes = mx.likes AND fs.comments = mx.comments
        GROUP BY fs.chatter_url
    )
SELECT 
    url,
    MAX(collected) as updated,
    SUM(likes) as likes, 
    SUM(comments) as comments, 
    SUM(subscribers) as subscribers,
    COUNT(chatter_url) as shares
FROM(
    SELECT ch.* FROM chatters ch 
    LEFT JOIN first_seen fs ON fs.chatter_url = ch.chatter_url  
    WHERE fs.collected = ch.collected
) 
GROUP BY url;