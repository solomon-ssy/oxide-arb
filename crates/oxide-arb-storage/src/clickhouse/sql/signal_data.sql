CREATE TABLE IF NOT EXISTS signal_data (
    market_id       String,
    signal_name     String,
    signal_value    Float64,
    metadata        String CODEC(ZSTD(1)),
    recorded_at     DateTime64(3, 'UTC'),
    signal_date     Date MATERIALIZED toDate(recorded_at)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(signal_date)
ORDER BY (market_id, signal_name, recorded_at)
TTL signal_date + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192
