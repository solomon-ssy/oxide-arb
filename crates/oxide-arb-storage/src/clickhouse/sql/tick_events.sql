CREATE TABLE IF NOT EXISTS tick_events (
    token_id        String,
    event_type      Enum8('book_snapshot' = 1, 'price_change' = 2, 'best_bid_ask' = 3),
    best_bid        Decimal64(8),
    best_ask        Decimal64(8),
    bid_depth_usd   Decimal64(8),
    ask_depth_usd   Decimal64(8),
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
