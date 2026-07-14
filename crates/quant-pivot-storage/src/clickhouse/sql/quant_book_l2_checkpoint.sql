CREATE TABLE IF NOT EXISTS quant_book_l2_checkpoint (
    token_id          String,
    market_id         Nullable(String),
    stream_session_id UUID,
    token_sequence    UInt64,
    bids_json         String CODEC(ZSTD(3)),
    asks_json         String CODEC(ZSTD(3)),
    book_version      UInt64,
    source_event_hash String,
    checkpoint_hash   String,
    event_time        DateTime64(3, 'UTC'),
    created_at        DateTime64(3, 'UTC'),
    schema_version    UInt32,
    checkpoint_date   Date MATERIALIZED toDate(event_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(checkpoint_date)
ORDER BY (token_id, event_time, stream_session_id, token_sequence)
SETTINGS index_granularity = 4096
