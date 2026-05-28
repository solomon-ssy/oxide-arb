CREATE TABLE IF NOT EXISTS book_snapshots (
    token_id        String,
    snapshot_time   DateTime64(3, 'UTC'),
    bids            String CODEC(ZSTD(3)),
    asks            String CODEC(ZSTD(3)),
    bid_depth_usd   Float64,
    ask_depth_usd   Float64,
    mid_price       Float64,
    spread_bps      UInt32,
    levels_count    UInt16,
    snapshot_date   Date MATERIALIZED toDate(snapshot_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (token_id, snapshot_time)
TTL snapshot_date + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 4096
