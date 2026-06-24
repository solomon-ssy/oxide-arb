CREATE TABLE IF NOT EXISTS tick_events (
    token_id        String,
    market_id       Nullable(String),
    event_type      Enum8('Snapshot' = 1, 'Delta' = 2, 'Bbo' = 3, 'TickSizeChange' = 4, 'LastTrade' = 5, 'ShardStatus' = 7),
    best_bid        Nullable(Decimal64(8)),
    best_ask        Nullable(Decimal64(8)),
    last_trade_price Nullable(Decimal64(8)),
    bid_depth_usd   Nullable(Decimal128(18)),
    ask_depth_usd   Nullable(Decimal128(18)),
    spread_bps      Nullable(Decimal64(4)),
    book_version    UInt64,
    raw_payload_json Nullable(String) CODEC(ZSTD(3)),
    event_time      DateTime64(3, 'UTC'),
    ingestion_time  DateTime64(3, 'UTC'),
    sequence        UInt64,
    source          Enum8('WsSnapshot' = 1, 'WsDelta' = 2, 'WsBbo' = 3, 'WsTickSize' = 4, 'WsLastTrade' = 5, 'WsMarketResolved' = 6, 'QuantPipeline' = 7, 'Execution' = 8, 'WsShardStatus' = 9),
    schema_version  UInt32,
    event_date      Date MATERIALIZED toDate(event_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (token_id, event_time, ingestion_time, sequence)
TTL event_date + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192
