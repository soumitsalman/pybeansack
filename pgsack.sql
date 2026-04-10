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
    embedding vector(384), -- vector length is not easily mutable once set, so hardcoding it for now
    categories VARCHAR[],
    sentiments VARCHAR[],

    -- COMPRESSED EXTRACTION FIELDS
    gist TEXT,
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

-- FIXED TABLES THAT DO NOT CHANGE

-- CREATE TABLE IF NOT EXISTS fixed_categories (
--     category VARCHAR NOT NULL PRIMARY KEY,
--     embedding vector({vector_len}) NOT NULL
-- );

-- CREATE TABLE IF NOT EXISTS fixed_sentiments (
--     sentiment VARCHAR NOT NULL PRIMARY KEY,
--     embedding vector({vector_len}) NOT NULL
-- );

CREATE TABLE IF NOT EXISTS related_beans (
    url VARCHAR NOT NULL,
    related_url VARCHAR NOT NULL,
    UNIQUE (url, related_url)
);

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
    trend_stats AS (
        SELECT
            b.url,
            COALESCE(likes, 0) as likes,
            COALESCE(comments, 0) as comments,
            COALESCE(subscribers, 0) as subscribers,
            COALESCE(shares, 0) as shares,
            COALESCE(related, 0) as related,
            GREATEST(DATE(b.created), COALESCE(cg.updated, DATE(b.created))) as updated
        FROM beans b
        LEFT JOIN related_stats rg ON b.url = rg.url
        LEFT JOIN chatter_stats cg ON b.url = cg.url
    )
SELECT
    *,
    ((100*related + 50*comments + 10*shares + likes) / (CURRENT_DATE + 2 - updated))::float AS trend_score
FROM trend_stats
WHERE GREATEST(likes, comments, shares, related) > 0;

DROP VIEW trending_beans_view CASCADE;
CREATE VIEW trending_beans_view AS
SELECT
    b.*,
    tr.updated, tr.comments, tr.shares, tr.likes, tr.subscribers, tr.related, tr.trend_score
FROM beans b
INNER JOIN trend_aggregates tr ON b.url = tr.url;

DROP VIEW aggregated_beans_view CASCADE;
CREATE VIEW aggregated_beans_view AS
WITH related_groups AS (
    SELECT url, ARRAY_AGG(related_url) AS related_urls
    FROM related_beans
    GROUP BY url
)
SELECT
    b.*,
    tr.updated, tr.comments, tr.shares, tr.likes, tr.subscribers, tr.related, tr.trend_score,
    rel.related_urls,
    p.base_url, p.site_name, p.description, p.favicon, p.rss_feed
FROM beans b
LEFT JOIN trend_aggregates tr ON b.url = tr.url
LEFT JOIN related_groups rel ON b.url = rel.url
LEFT JOIN publishers p ON b.source = p.source;


-- VIEWS & MATERIALIZED VIEWS --
-- materialized view for this is a bad idea. recomputes everything
-- CREATE TABLE IF NOT EXISTS _internal_clusters (
--     url VARCHAR NOT NULL,
--     related VARCHAR NOT NULL,
--     UNIQUE(url, related)
-- );

-- CREATE MATERIALIZED VIEW IF NOT EXISTS _materialized_cluster_aggregates AS
-- WITH cluster_groups AS (
--     SELECT url,
--         ARRAY_AGG(related) AS related,
--         COUNT(*)           AS cluster_size
--     FROM _internal_clusters
--     GROUP BY url
-- ),
-- best_related AS (
--     SELECT DISTINCT ON (ic.url)
--         ic.url,
--         ic.related AS cluster_id
--     FROM _internal_clusters ic
--     JOIN cluster_groups cg ON cg.url = ic.related
--     ORDER BY ic.url, cg.cluster_size DESC
-- )
-- SELECT cg.url, cg.related, cg.cluster_size, br.cluster_id
-- FROM cluster_groups cg
-- JOIN best_related br ON br.url = cg.url;

-- CREATE MATERIALIZED VIEW IF NOT EXISTS _materialized_chatter_aggregates AS
-- WITH
--     max_stats AS (
--         SELECT
--             chatter_url,
--             MAX(likes) as likes,
--             MAX(comments) as comments
--         FROM chatters
--         GROUP BY chatter_url
--     ),
--     first_seen AS (
--         SELECT
--             fs.chatter_url,
--             MIN(fs.collected) as collected
--         FROM chatters fs
--         LEFT JOIN max_stats mx ON fs.chatter_url = mx.chatter_url
--         WHERE fs.likes = mx.likes AND fs.comments = mx.comments
--         GROUP BY fs.chatter_url
--     )
-- SELECT
--     url,
--     DATE(MAX(collected)) as updated,
--     SUM(likes) as likes,
--     SUM(comments) as comments,
--     SUM(subscribers) as subscribers,
--     COUNT(chatter_url) as shares
-- FROM(
--     SELECT ch.* FROM chatters ch
--     LEFT JOIN first_seen fs ON fs.chatter_url = ch.chatter_url
--     WHERE fs.collected = ch.collected
-- )
-- GROUP BY url;

-- CREATE OR REPLACE VIEW trending_beans_view AS
-- SELECT
--     b.*,
--     ch.updated, ch.comments, ch.shares, ch.likes, ch.subscribers,
--     cl.cluster_size, cl.related, cl.cluster_id,
--     (100 * COALESCE(cl.cluster_size - 1, 0)
--      + 50 * COALESCE(ch.comments, 0)
--      + 10 * COALESCE(ch.shares, 0)
--      +      COALESCE(ch.likes, 0))
--     / ((CURRENT_DATE + 2) - GREATEST(DATE(b.created), COALESCE(ch.updated, DATE(b.created))))::float
--     AS trend_score
-- FROM beans b
-- LEFT JOIN _materialized_chatter_aggregates ch ON b.url = ch.url
-- LEFT JOIN _materialized_cluster_aggregates cl ON b.url = cl.url;

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

-- CREATE UNIQUE INDEX IF NOT EXISTS idx_mat_agg_chatters_url ON _materialized_chatter_aggregates(url);
-- CREATE UNIQUE INDEX IF NOT EXISTS idx_mat_agg_clusters_url ON _materialized_cluster_aggregates(url);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trend_agg_url ON trend_aggregates(url);