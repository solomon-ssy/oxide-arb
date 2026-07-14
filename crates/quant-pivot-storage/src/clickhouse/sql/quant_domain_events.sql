CREATE TABLE IF NOT EXISTS quant_crypto_price_report
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
    schema_version UInt16
)
ENGINE = ReplacingMergeTree(available_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (source_id, instrument_key, source_sequence, event_time, report_hash);

CREATE TABLE IF NOT EXISTS quant_weather_observation_report
(
    source_id LowCardinality(String),
    station LowCardinality(String),
    local_date Date,
    report_kind LowCardinality(String),
    temperature_celsius Decimal64(4),
    precision_celsius Decimal64(4),
    observation_time DateTime64(3, 'UTC'),
    published_at DateTime64(3, 'UTC'),
    available_at DateTime64(3, 'UTC'),
    revision UInt32,
    report_hash String,
    supersedes_report_hash Nullable(String),
    raw_report String,
    schema_version UInt16
)
ENGINE = ReplacingMergeTree(available_at)
PARTITION BY toYYYYMM(local_date)
ORDER BY (station, local_date, observation_time, revision, report_hash);

CREATE TABLE IF NOT EXISTS quant_weather_forecast_point
(
    source_id LowCardinality(String),
    station LowCardinality(String),
    reference_time DateTime64(3, 'UTC'),
    valid_time DateTime64(3, 'UTC'),
    available_at DateTime64(3, 'UTC'),
    lead_hours UInt16,
    member UInt8,
    tmax_celsius Decimal64(4),
    grid_binding_hash String,
    run_manifest_hash String,
    schema_version UInt16
)
ENGINE = ReplacingMergeTree(available_at)
PARTITION BY toYYYYMM(reference_time)
ORDER BY (station, reference_time, valid_time, member, run_manifest_hash);

CREATE TABLE IF NOT EXISTS quant_domain_event
(
    event_id UUID,
    source String,
    event_type LowCardinality(String),
    subject String,
    event_time DateTime64(3, 'UTC'),
    published_at DateTime64(3, 'UTC'),
    available_at DateTime64(3, 'UTC'),
    schema_version UInt16,
    revision UInt32,
    supersedes_event_id Nullable(UUID),
    payload_hash String,
    source_checkpoint_hash String,
    payload_json String
)
ENGINE = ReplacingMergeTree(available_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (subject, event_type, event_time, revision, event_id);

CREATE TABLE IF NOT EXISTS quant_entry_condition_evaluation_event
(
    condition_instance_id UUID,
    revision Int64,
    evaluator_version UInt32,
    evaluated_at DateTime64(3, 'UTC'),
    state LowCardinality(String),
    truth LowCardinality(String),
    evaluation_hash String,
    input_fingerprint String,
    tree_json String,
    schema_version UInt16
)
ENGINE = ReplacingMergeTree(evaluated_at)
PARTITION BY toYYYYMM(evaluated_at)
ORDER BY (condition_instance_id, revision, evaluation_hash);
