CREATE TABLE IF NOT EXISTS quant_trade_tape (
    market_id             String,
    token_id              String,
    event_time            DateTime64(3, 'UTC'),
    ingestion_time        DateTime64(3, 'UTC'),
    stream_session_id     Nullable(UUID),
    token_sequence        Nullable(UInt64),
    participant_address   String,
    participant_role      Enum8('Maker' = 1, 'Taker' = 2, 'Unknown' = 3),
    side                  Enum8('Buy' = 1, 'Sell' = 2, 'Unknown' = 3),
    price                 Decimal64(8),
    size_shares           Decimal128(18),
    notional_usd          Decimal128(18),
    tx_hash               Nullable(String),
    source_event_id       String,
    source                Enum8('MarketWs' = 1, 'OnChainOrderFilled' = 2),
    observed_field_flags  UInt16,
    fee_rate_bps          Nullable(Decimal64(4)),
    reconciliation_status Enum8('Pending' = 1, 'Matched' = 2, 'Unavailable' = 3, 'Ambiguous' = 4, 'OnChainOnly' = 5),
    matched_source_event_id Nullable(String),
    revision              UInt32,
    reconciled_at         Nullable(DateTime64(3, 'UTC')),
    raw_payload_json      Nullable(String) CODEC(ZSTD(3)),
    schema_version        UInt32,
    event_date            Date MATERIALIZED toDate(event_time)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (market_id, token_id, event_time, source_event_id, participant_address)
SETTINGS index_granularity = 8192
