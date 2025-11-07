INSTALL ducklake;
LOAD ducklake;
INSTALL sqlite;
LOAD sqlite;
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

ATTACH 'ducklake:{catalog_path}' AS warehouse (DATA_PATH '{data_path}');
USE warehouse;

CREATE TABLE IF NOT EXISTS beans (
    -- CORE FIELDS
    url VARCHAR NOT NULL,
    kind VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    title_length UINT16,    
    author VARCHAR,
    source VARCHAR NOT NULL,  
    image_url VARCHAR,
    created TIMESTAMP NOT NULL,
    collected TIMESTAMP NOT NULL,

    -- TEXT HEAVY FIELDS
    summary TEXT,
    summary_length UINT16,
    content TEXT,
    content_length UINT16,
    restricted_content BOOLEAN,

    -- CLASSIFICATION FIELDS
    embedding FLOAT[],

    -- TODO: DEPRECATE THESE FIELDS IN FAVOR OF MATERIALIZED VIEWS
    categories VARCHAR[],
    sentiments VARCHAR[],
    cluster_id VARCHAR,
    cluster_size UINT32,

    -- COMPRESSED EXTRACTION FIELDS
    gist TEXT,
    regions VARCHAR[],
    entities VARCHAR[],

    -- EXTRACTION FIELDS FOR FUTURE EXTENSION
    regions_v2 VARCHAR[],
    entities_v2 VARCHAR[]
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

CREATE TABLE IF NOT EXISTS publishers (
    source VARCHAR NOT NULL,
    base_url VARCHAR NOT NULL,
    site_name VARCHAR DEFAULT NULL,
    description TEXT DEFAULT NULL,    
    favicon VARCHAR DEFAULT NULL,
    rss_feed VARCHAR DEFAULT NULL
);


CREATE TABLE IF NOT EXISTS fixed_categories AS
SELECT * FROM read_parquet('{factory}/categories.parquet');

CREATE TABLE IF NOT EXISTS fixed_sentiments AS
SELECT * FROM read_parquet('{factory}/sentiments.parquet');

-- THERE ARE COMPUTED TABLES/MATERIALIZED VIEWS THAT ARE REFRESHED PERIODICALLY
-- THIS IS COMPUTED FROM BEAN EMBEDDINGS BEING COMPARED TO CATEGORIES AND SENTIMENTS
CREATE TABLE IF NOT EXISTS _materialized_bean_classifications (
    url VARCHAR NOT NULL,
    categories VARCHAR[] NOT NULL,
    sentiments VARCHAR[] NOT NULL
);

-- THIS IS COMPUTED FROM BEANS EMBEDDINGS BEING COMPARED TO OTHER BEANS EMBEDDINGS
CREATE TABLE IF NOT EXISTS _materialized_bean_clusters (
    url VARCHAR NOT NULL,
    related VARCHAR NOT NULL,
    distance FLOAT DEFAULT 0.0
);

-- THIS IS COMPUTED FROM TAKING DATA FROM BEAN CLUSTERS AND AGGREGATING THE STATS FOR FASTER QUERY
CREATE TABLE IF NOT EXISTS _materialized_bean_cluster_stats (
    url VARCHAR NOT NULL,
    cluster_id VARCHAR NOT NULL,
    cluster_size UINT32 NOT NULL
);

CREATE VIEW IF NOT EXISTS _internal_aggregated_chatters_view AS
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
    DATE(MAX(collected)) as updated,
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

CREATE TABLE IF NOT EXISTS _materialized_aggregated_chatters (
    url VARCHAR NOT NULL,  -- Foreign key to Bean.url
    updated DATE NOT NULL,
    likes UINT32,
    comments UINT32,
    subscribers UINT32,
    shares UINT32,
    refresh_ts TIMESTAMP NOT NULL
);

DROP VIEW IF EXISTS latest_beans_view;
CREATE VIEW IF NOT EXISTS latest_beans_view AS
SELECT * EXCLUDE(b.categories, b.sentiments, c.url) FROM beans b
LEFT JOIN _materialized_bean_classifications c ON b.url = c.url;

DROP VIEW IF EXISTS trending_beans_view;
CREATE VIEW IF NOT EXISTS trending_beans_view AS
SELECT * EXCLUDE(b.categories, b.sentiments, c.url, ch.url) FROM beans b
LEFT JOIN _materialized_bean_classifications c ON b.url = c.url
INNER JOIN _materialized_aggregated_chatters ch ON b.url = ch.url;

DROP VIEW IF EXISTS aggregated_beans_view;
CREATE VIEW IF NOT EXISTS aggregated_beans_view AS
SELECT * EXCLUDE(b.categories, b.sentiments, b.cluster_id, b.cluster_size, c.url, cl.url, ch.url, p.source) FROM beans b
LEFT JOIN _materialized_bean_classifications c ON b.url = c.url
LEFT JOIN _materialized_bean_cluster_stats cl ON b.url = cl.url
LEFT JOIN _materialized_aggregated_chatters ch ON b.url = ch.url
LEFT JOIN (
    SELECT DISTINCT * FROM publishers
) p ON b.source = p.source;