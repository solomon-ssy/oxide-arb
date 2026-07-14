CREATE TABLE IF NOT EXISTS quant_book_stream_session (
    stream_session_id       UUID,
    shard_id                UInt32,
    ledger_sequence         UInt32,
    state                   Enum8('Open' = 1, 'Sealed' = 2, 'Invalidated' = 3),
    end_reason              Enum8('None' = 0, 'Normal' = 1, 'Resubscribe' = 2, 'Overflow' = 3, 'Disconnect' = 4, 'Shutdown' = 5, 'CrashRecovery' = 6),
    subscription_token_hash String,
    subscription_token_count UInt32,
    received_sequence_json  String CODEC(ZSTD(3)),
    persisted_sequence_json String CODEC(ZSTD(3)),
    opened_at               DateTime64(3, 'UTC'),
    recorded_at             DateTime64(3, 'UTC'),
    schema_version          UInt32,
    session_date            Date MATERIALIZED toDate(opened_at)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(session_date)
ORDER BY (stream_session_id, ledger_sequence)
SETTINGS index_granularity = 4096
