-- =============================================================================
-- 001_ticks.sql: one row per Kafka message that Storage has persisted.
--
-- Applied once by scripts/db_init.py (the one-shot `db-init` service), never
-- by Storage itself: with several Storage instances starting together, DDL in
-- the service would race. db_init records each file in schema_migrations.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE ticks (
    -- The message's address in Kafka. Together with kafka_ts this is the
    -- idempotency key (see the primary key below).
    topic         text             NOT NULL,
    partition     integer          NOT NULL,
    kafka_offset  bigint           NOT NULL,
    kafka_ts      timestamptz      NOT NULL,  -- Kafka CreateTime (producer clock)
    -- The event
    symbol        text             NOT NULL,  -- internal symbol (topology.toml)
    asset_class   text             NOT NULL,  -- fx | crypto | us_stocks
    bid           double precision NOT NULL,  -- floats, as in v1 (a stated limitation)
    ask           double precision NOT NULL,
    mid           double precision NOT NULL,
    provider_ts   timestamptz      NOT NULL,  -- TraderMade "ts"
    ingest_ts     timestamptz      NOT NULL,  -- producer receive time
    seq           bigint           NOT NULL,  -- producer-stamped, per symbol and session
    session_id    text             NOT NULL,
    event_type    text             NOT NULL,  -- quote | snapshot
    source        text             NOT NULL,  -- tradermade | simulated
    -- Who wrote it and when
    instance_id   text             NOT NULL,  -- Storage container hostname
    stored_at     timestamptz      NOT NULL,  -- when THIS row was written

    -- Idempotency key. On a TimescaleDB hypertable every UNIQUE / PRIMARY
    -- KEY must include the partitioning (time) column, so the v1 key
    -- (topic, partition, kafka_offset) gains kafka_ts. That does not weaken
    -- duplicate detection: a redelivered message is the SAME record in the
    -- Kafka log, with the identical CreateTime the broker stored, so it
    -- carries the identical (topic, partition, offset, kafka_ts) and
    -- ON CONFLICT DO NOTHING drops it. Two different messages can never
    -- share (topic, partition, offset), so the extra column cannot merge them.
    PRIMARY KEY (topic, partition, kafka_offset, kafka_ts)
);

-- Chunk by Kafka time, one chunk per day: queries in the experiments look at
-- minutes to hours of data, and a day-sized chunk keeps the chunk count small.
SELECT create_hypertable('ticks', by_range('kafka_ts', INTERVAL '1 day'));

-- The no-loss checks count rows by OFFSET range per partition, not by time,
-- so they need their own index (the primary key also leads with these three
-- columns, but this keeps the plan obvious and cheap).
CREATE INDEX ix_ticks_tp_offset ON ticks (topic, partition, kafka_offset);
-- Seq contiguity checks group by producer session and symbol.
CREATE INDEX ix_ticks_session_symbol_seq ON ticks (session_id, symbol, seq);
