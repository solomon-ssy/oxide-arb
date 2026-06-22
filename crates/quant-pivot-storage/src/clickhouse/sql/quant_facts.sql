CREATE TABLE IF NOT EXISTS quant_feature_event
(
    event_time DateTime64(3, 'UTC'),
    as_of DateTime64(3, 'UTC'),
    market_id String,
    token_id String,
    feature_schema_version UInt32,
    feature_name LowCardinality(String),
    feature_value Decimal64(8),
    value_kind Int8,
    source_kind LowCardinality(String),
    staleness_ms UInt64,
    ingestion_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (market_id, feature_name, as_of, ingestion_time);

CREATE TABLE IF NOT EXISTS quant_factor_event
(
    event_time DateTime64(3, 'UTC'),
    as_of DateTime64(3, 'UTC'),
    market_id String,
    factor_name LowCardinality(String),
    factor_family LowCardinality(String),
    raw_value Decimal64(8),
    normalized_score Decimal64(8),
    confidence Decimal64(8),
    direction Int8,
    model_run_id String,
    ingestion_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (model_run_id, market_id, factor_name, as_of);

CREATE TABLE IF NOT EXISTS quant_signal_candidate_event
(
    event_time DateTime64(3, 'UTC'),
    signal_candidate_id String,
    model_run_id String,
    market_id String,
    token_id String,
    side Int8,
    score Decimal64(8),
    confidence Decimal64(8),
    entry_price Decimal64(8),
    target_price Decimal64(8),
    stop_price Decimal64(8),
    rank_before_portfolio UInt32,
    rejection_reason LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (model_run_id, rank_before_portfolio, market_id);

CREATE TABLE IF NOT EXISTS quant_recommendation_event
(
    event_time DateTime64(3, 'UTC'),
    recommendation_report_id String,
    recommendation_id String,
    rank UInt32,
    market_id String,
    token_id String,
    side Int8,
    score Decimal64(8),
    risk_adjusted_score Decimal64(8),
    suggested_usd Decimal128(18),
    valid_until DateTime64(3, 'UTC'),
    status LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (recommendation_report_id, rank, market_id);

CREATE TABLE IF NOT EXISTS quant_execution_event
(
    event_time DateTime64(3, 'UTC'),
    order_intent_id String,
    execution_order_id String,
    recommendation_id String,
    event_kind LowCardinality(String),
    market_id String,
    token_id String,
    side Int8,
    price Decimal64(8),
    shares Decimal128(18),
    cost_usd Decimal128(18),
    venue_order_id String,
    ingestion_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (order_intent_id, event_time, ingestion_time);
