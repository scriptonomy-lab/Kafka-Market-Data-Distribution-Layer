-- =============================================================================
-- 002_storage_batches.sql: one row per Storage write (transaction).
--
-- Lets the experiments prove with SQL, not log scraping, how many duplicates
-- were suppressed, which instance wrote which partitions, and how stale the
-- data was during catch-up. Written in the SAME transaction as the batch's
-- ticks, so a batch and its record are durable together or not at all.
-- A plain table, not a hypertable: it is small and queried by id and time.
-- =============================================================================

CREATE TABLE storage_batches (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    written_at   timestamptz NOT NULL,
    instance_id  text        NOT NULL,  -- Storage container hostname
    messages     integer     NOT NULL,  -- messages in the batch
    inserted     integer     NOT NULL,  -- rows actually inserted (RETURNING)
    duplicates   integer     NOT NULL,  -- messages - inserted: redeliveries ignored
    max_age_ms   bigint      NOT NULL,  -- age of the OLDEST message when written
    min_age_ms   bigint      NOT NULL,  -- age of the NEWEST message when written
    delay_ms     integer     NOT NULL,  -- STORAGE_DELAY_MS in force (experiment C)
    -- The topic-partitions in this batch, as 'topic:partition' strings
    partitions   text[]      NOT NULL
);

CREATE INDEX ix_storage_batches_written_at ON storage_batches (written_at);
