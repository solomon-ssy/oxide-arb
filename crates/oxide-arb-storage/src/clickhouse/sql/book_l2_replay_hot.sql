CREATE TABLE IF NOT EXISTS book_l2_replay_hot (
    token_id        String,
    market_id       Nullable(String),
    event_type      Enum8('Snapshot' = 1, 'Delta' = 2, 'Bbo' = 3, 'TickSizeChange' = 4, 'LastTrade' = 5, 'MarketResolved' = 6, 'ShardStatus' = 7),
    bid_prices      Array(Decimal64(8)),
    bid_sizes       Array(Decimal128(18)),
    ask_prices      Array(Decimal64(8)),
    ask_sizes       Array(Decimal128(18)),
    book_version    UInt64,
    levels_count    UInt16,
    is_full_snapshot Bool,
    event_time      DateTime64(3, 'UTC'),
    ingestion_time  DateTime64(3, 'UTC'),
    sequence        UInt64,
    source          Enum8('WsSnapshot' = 1, 'WsDelta' = 2, 'WsBbo' = 3, 'WsTickSize' = 4, 'WsLastTrade' = 5, 'WsMarketResolved' = 6, 'Scanner' = 7, 'Execution' = 8, 'Settlement' = 9, 'CalibrationUpdater' = 10, 'WsShardStatus' = 11),
    feed_event_hash Nullable(String),
    schema_version  UInt32,
    event_date      Date MATERIALIZED toDate(event_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_date)
ORDER BY (token_id, event_time, ingestion_time, sequence)
TTL event_time + INTERVAL 72 HOUR DELETE
SETTINGS index_granularity = 8192
