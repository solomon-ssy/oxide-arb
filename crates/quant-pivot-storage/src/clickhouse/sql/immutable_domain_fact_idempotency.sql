CREATE TABLE IF NOT EXISTS quant_domain_observation_idempotent_stage
(
    family LowCardinality(String),
    source_id LowCardinality(String),
    instrument_key LowCardinality(String),
    metric LowCardinality(String),
    value Decimal64(8),
    event_time DateTime64(3, 'UTC'),
    publish_time DateTime64(3, 'UTC'),
    ingestion_time DateTime64(3, 'UTC'),
    schema_version UInt32,
    event_date Date MATERIALIZED toDate(event_time)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (instrument_key, metric, event_time)
SETTINGS non_replicated_deduplication_window = 10000;

CREATE TABLE IF NOT EXISTS quant_crypto_price_report_idempotent_stage
(
    source_id LowCardinality(String),
    instrument_key String,
    source_sequence UInt64,
    price Decimal64(8),
    quantity Nullable(Decimal64(8)),
    event_time DateTime64(3, 'UTC'),
    published_at DateTime64(3, 'UTC'),
    available_at DateTime64(3, 'UTC'),
    valid_from Nullable(DateTime64(3, 'UTC')),
    observations_timestamp Nullable(DateTime64(3, 'UTC')),
    expires_at Nullable(DateTime64(3, 'UTC')),
    report_hash String,
    raw_report String,
    schema_version UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (source_id, instrument_key, source_sequence, event_time, report_hash)
SETTINGS non_replicated_deduplication_window = 10000;

CREATE TABLE IF NOT EXISTS quant_weather_observation_fact_idempotent_stage
(
    source_id LowCardinality(String),
    instrument_key String,
    subject_key LowCardinality(String),
    local_date Int32,
    report_kind LowCardinality(String),
    variable LowCardinality(String),
    value Decimal64(8),
    unit LowCardinality(String),
    precision Decimal64(8),
    observed_at Int64,
    valid_from Nullable(Int64),
    valid_to Nullable(Int64),
    published_at DateTime64(3, 'UTC'),
    available_at DateTime64(3, 'UTC'),
    revision UInt32,
    report_hash String,
    supersedes_report_hash Nullable(String),
    raw_report String,
    schema_version UInt32
)
ENGINE = MergeTree
PARTITION BY intDiv(local_date, 3660)
ORDER BY (source_id, instrument_key, variable, observed_at, revision, report_hash)
SETTINGS non_replicated_deduplication_window = 10000;

CREATE TABLE IF NOT EXISTS quant_weather_forecast_fact_idempotent_stage
(
    source_id LowCardinality(String),
    instrument_key String,
    subject_key LowCardinality(String),
    variable LowCardinality(String),
    value Decimal64(8),
    unit LowCardinality(String),
    precision Decimal64(8),
    reference_time DateTime64(3, 'UTC'),
    valid_time DateTime64(3, 'UTC'),
    published_at DateTime64(3, 'UTC'),
    available_at DateTime64(3, 'UTC'),
    lead_hours UInt16,
    member Nullable(UInt16),
    revision UInt32,
    grid_binding_hash String,
    run_manifest_hash String,
    report_hash String,
    schema_version UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(reference_time)
ORDER BY (source_id, instrument_key, variable, reference_time, valid_time, ifNull(member, 65535), revision, report_hash)
SETTINGS non_replicated_deduplication_window = 10000;
