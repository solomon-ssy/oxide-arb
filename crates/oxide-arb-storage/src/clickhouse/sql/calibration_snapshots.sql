CREATE TABLE IF NOT EXISTS calibration_snapshots (
    category        String,
    price_zone      String,
    duration_bucket String,
    total_count     UInt32,
    correct_count   UInt32,
    alpha_prior     Decimal64(8),
    beta_prior      Decimal64(8),
    posterior_mean  Decimal64(8),
    snapshot_time   DateTime64(3, 'UTC'),
    snapshot_date   Date MATERIALIZED toDate(snapshot_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (category, price_zone, duration_bucket, snapshot_time)
TTL snapshot_date + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 4096
