INSTALL ducklake;
LOAD ducklake;
INSTALL httpfs;
LOAD httpfs;
INSTALL sqlite;
LOAD sqlite;
INSTALL postgres;
LOAD postgres;

-- postgres:user=coffeemaker password=npg_iez5lWT3PxBC host=ep-weathered-river-aa5mc59f-pooler.westus3.azure.neon.tech dbname=beans_catalogdb sslmode=require

ATTACH 'ducklake:postgres:dbname=beans_catalogdb sslmode=require' AS warehouse (
    DATA_PATH 's3://test-cafecito-cdn'
);
USE warehouse;

CREATE TABLE IF NOT EXISTS bean_cores (
    url VARCHAR NOT NULL,
    kind VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    title_length SMALLINT DEFAULT 0,
    summary TEXT DEFAULT NULL,
    summary_length SMALLINT DEFAULT 0,
    content TEXT,
    content_length SMALLINT DEFAULT 0,
    restricted_content BOOLEAN DEFAULT 0,  -- 0 for False, 1 for True
    author VARCHAR DEFAULT NULL,
    source VARCHAR NOT NULL,  
    image_url VARCHAR DEFAULT NULL,
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
    source VARCHAR DEFAULT NULL,
    forum VARCHAR DEFAULT NULL,
    collected TIMESTAMP, 
    likes INTEGER DEFAULT 0,
    comments INTEGER DEFAULT 0,
    subscribers INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sources (
    source VARCHAR NOT NULL,
    base_url VARCHAR NOT NULL,
    title VARCHAR DEFAULT NULL,
    summary TEXT DEFAULT NULL,    
    favicon VARCHAR DEFAULT NULL,
    rss_feed VARCHAR DEFAULT NULL
);

CREATE TABLE IF NOT EXISTS categories (
    category VARCHAR NOT NULL,
    embedding FLOAT[] NOT NULL
);

CREATE TABLE IF NOT EXISTS sentiments (
    sentiment VARCHAR NOT NULL,
    embedding FLOAT[] NOT NULL
);

CREATE TABLE IF NOT EXISTS exported_beans (
    url VARCHAR NOT NULL,
    exported TIMESTAMP NOT NULL
);

CREATE VIEW IF NOT EXISTS bean_clusters_view AS 
SELECT 
    be1.url as url, 
    be2.url as related, 
    array_distance(be1.embedding::FLOAT[384], be2.embedding::FLOAT[384]) as distance
FROM bean_embeddings be1 
CROSS JOIN bean_embeddings be2
WHERE be1.url <> be2.url AND distance <= 0.43
ORDER BY distance;

CREATE VIEW IF NOT EXISTS bean_categories_view AS
SELECT e.url, c.category FROM bean_embeddings e
CROSS JOIN categories c
ORDER BY array_cosine_distance(e.embedding::FLOAT[384], c.embedding::FLOAT[384])
LIMIT 3;

CREATE VIEW IF NOT EXISTS bean_sentiments_view AS
SELECT e.url, s.sentiment FROM bean_embeddings e
CROSS JOIN sentiments s
ORDER BY array_cosine_distance(e.embedding::FLOAT[384], s.embedding::FLOAT[384])
LIMIT 3;

CREATE VIEW IF NOT EXISTS unprocessed_beans_view AS
SELECT * EXCLUDE (e.url, g.url) FROM bean_cores b
LEFT JOIN bean_embeddings e ON b.url = e.url
LEFT JOIN bean_gists g ON b.url = g.url
WHERE embedding IS NULL OR gist IS NULL;

CREATE VIEW IF NOT EXISTS processed_beans_view AS
SELECT * EXCLUDE(e.url, g.url, c.url, s.url) FROM bean_cores b
INNER JOIN bean_embeddings e ON b.url = e.url
INNER JOIN bean_gists g ON b.url = g.url
LEFT JOIN (
    SELECT url, LIST(DISTINCT category) as categories FROM bean_categories_view GROUP BY url
) as c ON b.url = c.url
LEFT JOIN (
    SELECT url, LIST(DISTINCT sentiment) as sentiments FROM bean_sentiments_view GROUP BY url
) as s ON b.url = s.url;

CREATE VIEW IF NOT EXISTS unexported_beans_view AS
SELECT * FROM processed_beans_view pb
WHERE pb.url NOT IN (SELECT url FROM exported_beans);

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