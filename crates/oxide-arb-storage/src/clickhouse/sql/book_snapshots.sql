CREATE TABLE IF NOT EXISTS book_snapshots (
    token_id        String,
    market_id       Nullable(String),
    snapshot_reason Enum8('Startup' = 1, 'Reconnect' = 2, 'Gap' = 3, 'Periodic' = 4, 'Manual' = 5, 'WsSnapshot' = 6),
    top_n           UInt16,
    bids_json       String CODEC(ZSTD(3)),
    asks_json       String CODEC(ZSTD(3)),
    bid_depth_usd   Nullable(Decimal128(18)),
    ask_depth_usd   Nullable(Decimal128(18)),
    mid_price       Nullable(Decimal64(8)),
    spread_bps      Nullable(Decimal64(4)),
    book_version    UInt64,
    levels_count    UInt16,
    event_time      DateTime64(3, 'UTC'),
    ingestion_time  DateTime64(3, 'UTC'),
    sequence        UInt64,
    source          Enum8('WsSnapshot' = 1, 'WsDelta' = 2, 'WsBbo' = 3, 'WsTickSize' = 4, 'WsLastTrade' = 5, 'WsMarketResolved' = 6, 'Scanner' = 7, 'Execution' = 8, 'Settlement' = 9, 'CalibrationUpdater' = 10, 'WsShardStatus' = 11),
    schema_version  UInt32,
    snapshot_date   Date MATERIALIZED toDate(event_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (token_id, event_time, ingestion_time, sequence)
TTL snapshot_date + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 4096
