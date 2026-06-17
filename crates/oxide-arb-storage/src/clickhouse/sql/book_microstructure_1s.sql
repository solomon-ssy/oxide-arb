CREATE TABLE IF NOT EXISTS book_microstructure_1s (
    token_id              String,
    market_id             Nullable(String),
    bucket_time           DateTime64(3, 'UTC'),
    best_bid_open         Nullable(Decimal64(8)),
    best_bid_high         Nullable(Decimal64(8)),
    best_bid_low          Nullable(Decimal64(8)),
    best_bid_close        Nullable(Decimal64(8)),
    best_ask_open         Nullable(Decimal64(8)),
    best_ask_high         Nullable(Decimal64(8)),
    best_ask_low          Nullable(Decimal64(8)),
    best_ask_close        Nullable(Decimal64(8)),
    spread_bps_min        Nullable(Decimal64(4)),
    spread_bps_avg        Nullable(Decimal64(4)),
    spread_bps_max        Nullable(Decimal64(4)),
    mid_price_open        Nullable(Decimal64(8)),
    mid_price_close       Nullable(Decimal64(8)),
    top1_depth_usd_avg    Nullable(Decimal128(18)),
    top5_depth_usd_avg    Nullable(Decimal128(18)),
    top20_depth_usd_avg   Nullable(Decimal128(18)),
    imbalance_avg         Nullable(Decimal64(8)),
    update_count          UInt64,
    snapshot_count        UInt64,
    delta_count           UInt64,
    delete_count          UInt64,
    crossed_count         UInt64,
    invalid_level_count   UInt64,
    gap_count             UInt64,
    last_trade_count      UInt64,
    max_book_age_ms       UInt64,
    schema_version        UInt32,
    bucket_date           Date MATERIALIZED toDate(bucket_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(bucket_date)
ORDER BY (token_id, bucket_time)
TTL bucket_date + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 4096
