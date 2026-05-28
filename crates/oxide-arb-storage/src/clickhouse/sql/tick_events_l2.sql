CREATE TABLE IF NOT EXISTS tick_events_l2 (
    token_id        String,
    event_type      UInt8,
    bid_prices      Array(Float64),
    bid_sizes       Array(Float64),
    ask_prices      Array(Float64),
    ask_sizes       Array(Float64),
    changed_levels  Nullable(String) CODEC(ZSTD(3)),
    received_at     DateTime64(3, 'UTC'),
    event_date      Date MATERIALIZED toDate(received_at)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (token_id, received_at)
TTL event_date + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192
