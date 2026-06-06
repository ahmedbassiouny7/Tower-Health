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

-- Spark RAN job: one row per cell after explode.
CREATE TABLE IF NOT EXISTS processed_ran_metrics (
    timestamp TEXT,
    message_id TEXT,
    site_id TEXT,
    site_name TEXT,
    vendor TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    alert_severity TEXT,
    avg_ru_temp DOUBLE PRECISION,
    battery_charge DOUBLE PRECISION,
    battery_status TEXT,
    cell_id TEXT,
    tech TEXT,
    cell_status TEXT,
    users INTEGER,
    downlink_mbps DOUBLE PRECISION,
    rsrp DOUBLE PRECISION,
    rsrq DOUBLE PRECISION,
    sinr DOUBLE PRECISION,
    cqi DOUBLE PRECISION,
    ho_success_rate DOUBLE PRECISION,
    signal_quality TEXT,
    ingested_at TIMESTAMPTZ
);

-- Spark RAN job: one row per transport/backhaul link.
CREATE TABLE IF NOT EXISTS transport_metrics (
    timestamp TEXT,
    message_id TEXT,
    site_id TEXT,
    site_name TEXT,
    link_id TEXT,
    link_type TEXT,
    link_status TEXT,
    latency_ms DOUBLE PRECISION,
    jitter_ms DOUBLE PRECISION,
    packet_loss_percent DOUBLE PRECISION,
    throughput_mbps DOUBLE PRECISION,
    utilization_percent DOUBLE PRECISION,
    severity TEXT,
    link_quality_score DOUBLE PRECISION,
    ingested_at TIMESTAMPTZ
);

-- Spark weather job: one row per tower location poll.
CREATE TABLE IF NOT EXISTS weather_metrics (
    event_timestamp TEXT,
    source_system TEXT,
    location_query TEXT,
    ran_site_id TEXT,
    ran_site_name TEXT,
    ran_region TEXT,
    weather_temperature_c DOUBLE PRECISION,
    weather_humidity_pct INTEGER,
    weather_rainfall_mm DOUBLE PRECISION,
    weather_wind_speed_kmh DOUBLE PRECISION,
    weather_condition TEXT,
    weather_observed_at TEXT,
    weather_fetched_at TEXT,
    weather_location_name TEXT,
    weather_region TEXT,
    weather_country TEXT,
    weather_latitude DOUBLE PRECISION,
    weather_longitude DOUBLE PRECISION,
    is_raining BOOLEAN,
    rain_intensity TEXT,
    ingested_at TIMESTAMPTZ
);
