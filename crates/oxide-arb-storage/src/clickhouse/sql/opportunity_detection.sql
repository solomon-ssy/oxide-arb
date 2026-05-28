CREATE TABLE IF NOT EXISTS opportunity_detection (
    opportunity_id   String,
    market_id        String,
    event_id         String,
    token_id         String,
    side             String,
    entry_price      Float64,
    edge_bps         UInt32,
    net_profit       Float64,
    resolution_prob  Float64,
    confidence       Float64,
    category         String,
    price_zone       String,
    duration_bucket  String,
    detected_at      DateTime64(3, 'UTC'),
    detection_date   Date MATERIALIZED toDate(detected_at)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(detection_date)
ORDER BY (market_id, detected_at)
TTL detection_date + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192
