CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- CONTENT TABLES

CREATE TABLE IF NOT EXISTS beans (
    -- CORE FIELDS
    url VARCHAR NOT NULL PRIMARY KEY,
    kind VARCHAR,
    title VARCHAR,
    title_length SMALLINT,
    author VARCHAR,
    source VARCHAR,
    image_url VARCHAR,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    collected TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- TEXT HEAVY FIELDS
    summary TEXT,
    summary_length SMALLINT,
    content TEXT,
    content_length SMALLINT,
    restricted_content BOOLEAN,
    
    -- CLASSIFICATION FIELDS
    embedding vector({vector_len}),
    categories VARCHAR[],
    sentiments VARCHAR[],
    
    -- COMPRESSED EXTRACTION FIELDS
    gist TEXT,
    regions VARCHAR[],
    entities VARCHAR[]
);

CREATE TABLE IF NOT EXISTS publishers (
    source VARCHAR NOT NULL PRIMARY KEY,
    base_url VARCHAR NOT NULL,
    site_name VARCHAR,
    description TEXT,
    favicon VARCHAR,
    rss_feed VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS chatters (
    chatter_url VARCHAR NOT NULL,
    -- this is a foreign key to beans.url but not enforced due to insertion sequence
    url VARCHAR NOT NULL, 
    source VARCHAR,
    forum VARCHAR,
    collected TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    likes INTEGER DEFAULT 0,
    comments INTEGER DEFAULT 0,
    subscribers INTEGER DEFAULT 0,
    shares INTEGER DEFAULT 0
);

-- FIXED TABLES THAT DO NOT CHANGE

CREATE TABLE IF NOT EXISTS fixed_categories (
    category VARCHAR NOT NULL PRIMARY KEY,
    embedding vector({vector_len}) NOT NULL
);

CREATE TABLE IF NOT EXISTS fixed_sentiments (
    sentiment VARCHAR NOT NULL PRIMARY KEY,
    embedding vector({vector_len}) NOT NULL
);

-- VIEWS & MATERIALIZED VIEWS --

CREATE MATERIALIZED VIEW IF NOT EXISTS _materialized_clusters AS
WITH scope AS (
    SELECT url, embedding FROM beans                 
    WHERE embedding IS NOT NULL
)
SELECT s1.url as url, s2.url as related
FROM scope s1
CROSS JOIN scope s2
WHERE s1.url <> s2.url AND (s1.embedding <-> s2.embedding) <= {cluster_eps}; 

CREATE MATERIALIZED VIEW IF NOT EXISTS _materialized_cluster_aggregates AS
WITH cluster_groups AS (
    SELECT url, ARRAY_AGG(related) AS related, count(*) AS cluster_size 
    FROM _materialized_clusters 
    GROUP BY url
)
SELECT cg.*, cl.related AS cluster_id
FROM cluster_groups cg
INNER JOIN _materialized_clusters cl ON cg.url = cl.url
WHERE cl.related = (SELECT related FROM _materialized_clusters WHERE url = cg.url ORDER BY cluster_size DESC LIMIT 1);

CREATE MATERIALIZED VIEW IF NOT EXISTS _materialized_chatter_aggregates AS
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

CREATE OR REPLACE VIEW trending_beans_view AS
SELECT 
    b.*,  
    ch.updated, ch.likes, ch.comments, ch.subscribers, ch.shares
FROM beans b
INNER JOIN _materialized_chatter_aggregates ch ON b.url = ch.url
ORDER BY updated DESC, comments DESC, likes DESC;

CREATE OR REPLACE VIEW aggregated_beans_view AS
SELECT 
    b.*,  
    ch.updated, ch.likes, ch.comments, ch.subscribers, ch.shares,
    cl.related, cl.cluster_id, cl.cluster_size,
    p.base_url, p.site_name, p.description, p.favicon, p.rss_feed
FROM beans b
LEFT JOIN _materialized_chatter_aggregates ch ON b.url = ch.url
LEFT JOIN _materialized_cluster_aggregates cl ON b.url = cl.url
LEFT JOIN publishers p ON b.source = p.source;

-- INDEXES --
-- beans
CREATE INDEX IF NOT EXISTS idx_beans_kind ON beans(kind);
CREATE INDEX IF NOT EXISTS idx_beans_created ON beans(created DESC);
CREATE INDEX IF NOT EXISTS idx_beans_source ON beans(source);
CREATE INDEX IF NOT EXISTS idx_beans_categories ON beans USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_beans_entities ON beans USING gin(entities);
CREATE INDEX IF NOT EXISTS idx_beans_regions ON beans USING gin(regions);
-- Full text search index on title and content
-- CREATE INDEX IF NOT EXISTS idx_beans_title_fts ON beans USING gin(to_tsvector('english', title));
-- CREATE INDEX IF NOT EXISTS idx_beans_content_fts ON beans USING gin(to_tsvector('english', content));
CREATE INDEX IF NOT EXISTS idx_beans_embedding_hnsw_cosine ON beans USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);
CREATE INDEX IF NOT EXISTS idx_beans_embedding_hnsw_l2 ON beans USING hnsw (embedding vector_l2_ops)
    WITH (m = 16, ef_construction = 64);

-- publishers
CREATE INDEX IF NOT EXISTS idx_publishers_source ON publishers(source);

-- chatters
CREATE INDEX IF NOT EXISTS idx_chatters_url ON chatters(url);
CREATE INDEX IF NOT EXISTS idx_chatters_collected ON chatters(collected DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mat_agg_chatters_url ON _materialized_chatter_aggregates(url);
