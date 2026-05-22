CREATE TABLE IF NOT EXISTS tick_events_l2 (
    token_id        String,
    event_type      Enum8('snapshot' = 1, 'delta' = 2),
    bid_prices      Array(Decimal64(8)),
    bid_sizes       Array(Decimal64(8)),
    ask_prices      Array(Decimal64(8)),
    ask_sizes       Array(Decimal64(8)),
    changed_levels  Nullable(String) CODEC(ZSTD(3)),
    received_at     DateTime64(3, 'UTC'),
    event_date      Date MATERIALIZED toDate(received_at)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (token_id, received_at)
TTL event_date + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192
