CREATE TABLE IF NOT EXISTS calibration_snapshots (
    category        Enum8('Geopolitics' = 1, 'Sports' = 2, 'Politics' = 3, 'Finance' = 4, 'Tech' = 5, 'Culture' = 6, 'Weather' = 7, 'Economics' = 8, 'Crypto' = 9, 'Other' = 10),
    price_zone      Enum8('Z95' = 1, 'Z96' = 2, 'Z97' = 3, 'Z98' = 4, 'Z99' = 5),
    duration_bucket Enum8('Short' = 1, 'Medium' = 2, 'Long' = 3, 'VeryLong' = 4),
    total_count     UInt32,
    correct_count   UInt32,
    alpha_prior     Decimal64(8),
    beta_prior      Decimal64(8),
    posterior_mean  Nullable(Decimal64(8)),
    fallback_tier   UInt8,
    config_hash     String,
    snapshot_hash   String,
    event_time      DateTime64(3, 'UTC'),
    ingestion_time  DateTime64(3, 'UTC'),
    sequence        UInt64,
    source          Enum8('WsSnapshot' = 1, 'WsDelta' = 2, 'WsBbo' = 3, 'WsTickSize' = 4, 'WsLastTrade' = 5, 'WsMarketResolved' = 6, 'Scanner' = 7, 'Execution' = 8, 'Settlement' = 9, 'CalibrationUpdater' = 10, 'WsShardStatus' = 11),
    schema_version  UInt32,
    snapshot_date   Date MATERIALIZED toDate(event_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (category, price_zone, duration_bucket, event_time, ingestion_time, sequence)
TTL snapshot_date + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 4096
