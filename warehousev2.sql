INSTALL ducklake;
LOAD ducklake;
INSTALL sqlite;
LOAD sqlite;
INSTALL postgres;
LOAD postgres;

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

CREATE TABLE IF NOT EXISTS computed_bean_clusters (
    url VARCHAR NOT NULL,
    related VARCHAR NOT NULL,
    distance FLOAT DEFAULT 0.0
);


CREATE VIEW IF NOT EXISTS missing_clusters_view AS
SELECT url, embedding FROM beans e
WHERE NOT EXISTS (
    SELECT 1 FROM computed_bean_clusters cl 
    WHERE cl.url = e.url
);

CREATE VIEW IF NOT EXISTS bean_clusters_view AS
SELECT 
    url, 
    FIRST(cl.related ORDER BY cluster_size DESC) AS cluster_id, -- this is the larger cluster url is part of
    COUNT(*) AS cluster_size -- this is the number of related items
FROM computed_bean_clusters cl
INNER JOIN (
    SELECT related, count(*) AS cluster_size 
    FROM computed_bean_clusters GROUP BY related
) clsz ON cl.related = clsz.related
GROUP BY url;

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