DROP TABLE IF EXISTS quant_weather_observation_report;

DROP TABLE IF EXISTS quant_weather_forecast_point;

CREATE TABLE IF NOT EXISTS quant_weather_observation_fact
(
    source_id LowCardinality(String),
    instrument_key String,
    subject_key LowCardinality(String),
    local_date Date,
    report_kind LowCardinality(String),
    variable LowCardinality(String),
    value Decimal64(8),
    unit LowCardinality(String),
    precision Decimal64(8),
    observed_at DateTime64(3, 'UTC'),
    valid_from Nullable(DateTime64(3, 'UTC')),
    valid_to Nullable(DateTime64(3, 'UTC')),
    published_at DateTime64(3, 'UTC'),
    available_at DateTime64(3, 'UTC'),
    revision UInt32,
    report_hash String,
    supersedes_report_hash Nullable(String),
    raw_report String,
    schema_version UInt32
)
ENGINE = ReplacingMergeTree(available_at)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (source_id, instrument_key, variable, observed_at, revision, report_hash);

CREATE TABLE IF NOT EXISTS quant_weather_forecast_fact
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
ENGINE = ReplacingMergeTree(available_at)
PARTITION BY toYYYYMM(reference_time)
ORDER BY (source_id, instrument_key, variable, reference_time, valid_time, ifNull(member, 65535), revision, report_hash);
