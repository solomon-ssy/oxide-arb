CREATE TABLE IF NOT EXISTS tick_events (
    token_id        String,
    event_type      UInt8,
    best_bid        Float64,
    best_ask        Float64,
    bid_depth_usd   Float64,
    ask_depth_usd   Float64,
    spread_bps      UInt32,
    raw_payload     String CODEC(ZSTD(3)),
    received_at     DateTime64(3, 'UTC'),
    event_date      Date MATERIALIZED toDate(received_at)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (token_id, received_at)
TTL event_date + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192
