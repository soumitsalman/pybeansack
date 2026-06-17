CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE OR REPLACE FUNCTION immutable_tags_to_text(
    a varchar[],
    b varchar[],
    c varchar[]
)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT array_to_string(
        (
            SELECT array_agg(elem)
            FROM unnest(
                COALESCE(a, '{}') ||
                COALESCE(b, '{}') ||
                COALESCE(c, '{}')
            ) AS elem
            WHERE elem IS NOT NULL
        ),
        ' '
    );
$$;

-- CONTENT TABLES
CREATE TABLE IF NOT EXISTS beans (
    -- CORE FIELDS
    url VARCHAR NOT NULL PRIMARY KEY,
    kind VARCHAR,
    title VARCHAR,
    author VARCHAR,
    source VARCHAR,
    image_url VARCHAR,
    created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    collected TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- TEXT HEAVY FIELDS
    summary TEXT,
    content TEXT,
    restricted_content BOOLEAN,

    -- CLASSIFICATION FIELDS
    embedding vector(384), -- vector length is not easily mutable once set, so hardcoding it for now
    categories VARCHAR[],
    sentiments VARCHAR[],

    -- COMPRESSED EXTRACTION FIELDS
    regions VARCHAR[],
    entities VARCHAR[],

    -- TEXT SEARCH FIELD
    tags TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('simple', immutable_tags_to_text(regions, entities, categories))
    ) STORED
);

CREATE TABLE IF NOT EXISTS publishers (
    source VARCHAR NOT NULL PRIMARY KEY,
    base_url VARCHAR NOT NULL,
    site_name VARCHAR,
    description TEXT,
    favicon VARCHAR,
    rss_feed VARCHAR,
    collected TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

CREATE TABLE IF NOT EXISTS related_beans (
    url VARCHAR NOT NULL,
    related_url VARCHAR NOT NULL,
    UNIQUE (url, related_url)
);

DROP MATERIALIZED VIEW IF EXISTS trend_aggregates CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS trend_aggregates AS
WITH
    max_chatters AS (
        SELECT
            chatter_url,
            MAX(likes) as likes,
            MAX(comments) as comments
        FROM chatters
        GROUP BY chatter_url
    ),
    first_seen_max_chatters AS (
        SELECT
            fs.chatter_url,
            MIN(fs.collected) as collected
        FROM chatters fs
        LEFT JOIN max_chatters mx ON fs.chatter_url = mx.chatter_url
        WHERE fs.likes = mx.likes AND fs.comments = mx.comments
        GROUP BY fs.chatter_url
    ),
    chatter_stats AS (
        SELECT
            url,
            DATE(MAX(collected)) as updated,
            SUM(likes) as likes,
            SUM(comments) as comments,
            SUM(subscribers) as subscribers,
            COUNT(chatter_url) as shares
        FROM(
            SELECT ch.* FROM chatters ch
            LEFT JOIN first_seen_max_chatters fs ON fs.chatter_url = ch.chatter_url
            WHERE fs.collected = ch.collected
        )
        GROUP BY url
    ),
    related_stats AS (
        SELECT url, COUNT(*) AS related
        FROM related_beans
        GROUP BY url
    ),
    related_hub_sizes AS (
        SELECT related_url, COUNT(*) AS hub_size
        FROM related_beans
        GROUP BY related_url
    ),
    url_cluster AS (
        SELECT DISTINCT ON (rb.url)
            rb.url,
            rb.related_url AS cluster_id
        FROM related_beans rb
        INNER JOIN related_hub_sizes rhs ON rb.related_url = rhs.related_url
        ORDER BY rb.url, rhs.hub_size DESC, rb.related_url
    ),
    trend_stats AS (
        SELECT
            b.url,
            COALESCE(likes, 0) as likes,
            COALESCE(comments, 0) as comments,
            COALESCE(subscribers, 0) as subscribers,
            COALESCE(shares, 0) as shares,
            COALESCE(related, 0) as related,
            uc.cluster_id,
            GREATEST(DATE(b.created), COALESCE(cg.updated, DATE(b.created))) as updated
        FROM beans b
        LEFT JOIN related_stats rg ON b.url = rg.url
        LEFT JOIN chatter_stats cg ON b.url = cg.url
        LEFT JOIN url_cluster uc ON b.url = uc.url
    )
SELECT
    *,
    ((100*related + 50*comments + 10*shares + likes) / (CURRENT_DATE + 2 - updated))::float AS trend_score
FROM trend_stats
WHERE GREATEST(likes, comments, shares, related) > 0;

DROP VIEW IF EXISTS trending_beans_view CASCADE;
CREATE VIEW trending_beans_view AS
SELECT
    b.*,
    tr.updated, tr.comments, tr.shares, tr.likes, tr.subscribers, tr.related, tr.cluster_id, tr.trend_score
FROM beans b
INNER JOIN trend_aggregates tr ON b.url = tr.url;

DROP VIEW IF EXISTS aggregated_beans_view CASCADE;
CREATE VIEW aggregated_beans_view AS
WITH related_groups AS (
    SELECT url, ARRAY_AGG(related_url) AS related_urls
    FROM related_beans
    GROUP BY url
)
SELECT
    b.*,
    tr.updated, tr.comments, tr.shares, tr.likes, tr.subscribers, tr.related, tr.cluster_id, tr.trend_score,
    rel.related_urls,
    p.base_url, p.site_name, p.description, p.favicon, p.rss_feed
FROM beans b
LEFT JOIN trend_aggregates tr ON b.url = tr.url
LEFT JOIN related_groups rel ON b.url = rel.url
LEFT JOIN publishers p ON b.source = p.source;

-- INDEXES --
-- beans
CREATE INDEX IF NOT EXISTS idx_beans_kind ON beans(kind);
CREATE INDEX IF NOT EXISTS idx_beans_created ON beans(created DESC);
CREATE INDEX IF NOT EXISTS idx_beans_source ON beans(source);
CREATE INDEX IF NOT EXISTS idx_beans_categories ON beans USING gin(categories);
CREATE INDEX IF NOT EXISTS idx_beans_entities ON beans USING gin(entities);
CREATE INDEX IF NOT EXISTS idx_beans_regions ON beans USING gin(regions);

-- tags search
CREATE INDEX IF NOT EXISTS idx_beans_tags ON beans USING gin(tags);
-- vector search
CREATE INDEX IF NOT EXISTS idx_beans_embedding_hnsw_cosine ON beans USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);
CREATE INDEX IF NOT EXISTS idx_beans_embedding_hnsw_l2 ON beans USING hnsw (embedding vector_l2_ops)
    WITH (m = 16, ef_construction = 64);

-- publishers
CREATE INDEX IF NOT EXISTS idx_publishers_source ON publishers(source);

-- chatters
CREATE INDEX IF NOT EXISTS idx_chatters_url ON chatters(url);
CREATE INDEX IF NOT EXISTS idx_chatters_collected ON chatters(collected DESC);

-- related_beans
CREATE INDEX IF NOT EXISTS idx_related_beans_related_url ON related_beans(related_url);

CREATE UNIQUE INDEX IF NOT EXISTS idx_trend_agg_url ON trend_aggregates(url);