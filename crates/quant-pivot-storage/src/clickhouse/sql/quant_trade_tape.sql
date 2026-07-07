CREATE TABLE IF NOT EXISTS quant_trade_tape (
    market_id             String,
    token_id              String,
    event_time            DateTime64(3, 'UTC'),
    ingestion_time        DateTime64(3, 'UTC'),
    participant_address   String,
    participant_role      Enum8('Maker' = 1, 'Taker' = 2, 'Unknown' = 3),
    side                  Enum8('Buy' = 1, 'Sell' = 2, 'Unknown' = 3),
    price                 Decimal64(8),
    size_shares           Decimal128(18),
    notional_usd          Decimal128(18),
    tx_hash               Nullable(String),
    trade_id              String,
    source                Enum8('OnChain' = 1),
    coverage_flags        UInt16,
    raw_payload_json      Nullable(String) CODEC(ZSTD(3)),
    schema_version        UInt32,
    event_date            Date MATERIALIZED toDate(event_time)
)
ENGINE = ReplacingMergeTree(ingestion_time)
PARTITION BY toYYYYMM(event_date)
ORDER BY (market_id, token_id, participant_role, event_time, trade_id, participant_address)
TTL event_date + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192
