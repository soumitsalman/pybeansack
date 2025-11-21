-- PostgreSQL Backend for Beansack
-- Initialize database with required extensions and tables

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Beans table: core content repository
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
    entities VARCHAR[],
    
    -- TIMESTAMPS FOR TRACKING
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Publishers table: source metadata
CREATE TABLE IF NOT EXISTS publishers (
    source VARCHAR NOT NULL PRIMARY KEY,
    base_url VARCHAR NOT NULL,
    site_name VARCHAR,
    description TEXT,
    favicon VARCHAR,
    rss_feed VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Chatters table: social media mentions and engagement
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



-- Fixed categories reference table
CREATE TABLE IF NOT EXISTS fixed_categories (
    category VARCHAR NOT NULL PRIMARY KEY,
    embedding vector({vector_len})
);

-- Fixed sentiments reference table
CREATE TABLE IF NOT EXISTS fixed_sentiments (
    sentiment VARCHAR NOT NULL PRIMARY KEY,
    embedding vector({vector_len})
);

-- CREATING INDEXES --
-- beans
CREATE INDEX IF NOT EXISTS idx_beans_created ON beans(kind);
CREATE INDEX IF NOT EXISTS idx_beans_created ON beans(created DESC);
CREATE INDEX IF NOT EXISTS idx_beans_source ON beans(source);
CREATE INDEX IF NOT EXISTS idx_beans_categories ON beans USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_beans_entities ON beans USING gin(entities);
CREATE INDEX IF NOT EXISTS idx_beans_regions ON beans USING gin(regions);
-- Full text search index on title and content
-- CREATE INDEX IF NOT EXISTS idx_beans_title_fts ON beans USING gin(to_tsvector('english', title));
-- CREATE INDEX IF NOT EXISTS idx_beans_content_fts ON beans USING gin(to_tsvector('english', content));
CREATE INDEX IF NOT EXISTS idx_beans_embedding_hnsw ON beans USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);

-- publishers
CREATE INDEX IF NOT EXISTS idx_publishers_source ON publishers(source);

-- chatters
CREATE INDEX IF NOT EXISTS idx_chatters_url ON chatters(url);
CREATE INDEX IF NOT EXISTS idx_chatters_collected ON chatters(collected DESC);
