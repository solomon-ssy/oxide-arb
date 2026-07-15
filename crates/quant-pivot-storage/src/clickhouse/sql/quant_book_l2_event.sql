CREATE TABLE IF NOT EXISTS quant_book_l2_event (
    stream_session_id UUID,
    shard_id          UInt32,
    token_id          String,
    market_id         Nullable(String),
    token_sequence    UInt64,
    event_type        Enum8('Snapshot' = 1, 'Delta' = 2, 'TickSizeChange' = 3, 'Gap' = 4),
    bid_prices        Array(Decimal64(8)),
    bid_sizes         Array(Decimal128(18)),
    ask_prices        Array(Decimal64(8)),
    ask_sizes         Array(Decimal128(18)),
    old_tick_size     Nullable(Decimal64(8)),
    new_tick_size     Nullable(Decimal64(8)),
    book_version      UInt64,
    venue_event_time  DateTime64(3, 'UTC'),
    ingress_time      DateTime64(3, 'UTC'),
    persisted_time    DateTime64(3, 'UTC'),
    payload_hash      String,
    schema_version    UInt32,
    event_date        Date MATERIALIZED toDate(venue_event_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (token_id, stream_session_id, token_sequence)
SETTINGS index_granularity = 8192
