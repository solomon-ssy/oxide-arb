CREATE TABLE IF NOT EXISTS market_resolution_event (
    market_id        String,
    winning_token_id String,
    winning_outcome  String,
    asset_token_ids  Array(String),
    resolved_at      DateTime64(3, 'UTC'),
    observed_at      DateTime64(3, 'UTC'),
    source           Enum8('WsSnapshot' = 1, 'WsDelta' = 2, 'WsBbo' = 3, 'WsTickSize' = 4, 'WsLastTrade' = 5, 'WsMarketResolved' = 6, 'QuantPipeline' = 7, 'Execution' = 8, 'WsShardStatus' = 9),
    sequence         UInt64,
    schema_version   UInt32,
    resolved_date    Date MATERIALIZED toDate(resolved_at)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(resolved_date)
ORDER BY (market_id, resolved_at, observed_at, sequence)
SETTINGS index_granularity = 8192
