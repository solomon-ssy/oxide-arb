CREATE TABLE IF NOT EXISTS quant_weather_observation_fact_epoch_stage
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
ENGINE = ReplacingMergeTree(available_at)
PARTITION BY intDiv(local_date, 3660)
ORDER BY (source_id, instrument_key, variable, observed_at, revision, report_hash);
