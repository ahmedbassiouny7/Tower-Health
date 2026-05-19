-- Raw event landing table for the realtime path:
-- Kafka -> Spark Structured Streaming -> this table -> Streamlit dashboard.
CREATE TABLE IF NOT EXISTS kafka_events (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    message_key TEXT,
    -- Keep the producer JSON as text so RAN and weather messages can share
    -- one table even though their schemas are different.
    message_value TEXT,
    event_time TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Kafka offsets are unique within a topic partition, so this prevents
    -- duplicate inserts if Spark retries a micro-batch.
    UNIQUE (topic, kafka_partition, kafka_offset)
);

-- Dashboard queries sort by topic and newest event time.
CREATE INDEX IF NOT EXISTS idx_kafka_events_topic_time
    ON kafka_events (topic, event_time DESC);

-- Small operational table reserved for future stream status/heartbeat records.
CREATE TABLE IF NOT EXISTS stream_health (
    name TEXT PRIMARY KEY,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);
