CREATE TABLE IF NOT EXISTS quant_domain_observation (
    family                LowCardinality(String),
    source_id             LowCardinality(String),
    instrument_key        LowCardinality(String),
    metric                LowCardinality(String),
    value                 Decimal64(8),
    event_time            DateTime64(3, 'UTC'),
    publish_time          DateTime64(3, 'UTC'),
    ingestion_time        DateTime64(3, 'UTC'),
    schema_version        UInt32,
    event_date            Date MATERIALIZED toDate(event_time)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (instrument_key, metric, event_time)
SETTINGS index_granularity = 8192
;
