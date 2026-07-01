CREATE TABLE IF NOT EXISTS quant_feature_event
(
    event_time DateTime64(3, 'UTC'),
    as_of DateTime64(3, 'UTC'),
    market_id String,
    token_id String,
    feature_schema_version UInt32,
    feature_name LowCardinality(String),
    feature_value Decimal64(8),
    value_kind Enum8('decimal' = 0, 'probability' = 1, 'bps' = 2, 'usd' = 3, 'count' = 4, 'bool' = 5, 'category' = 6),
    source_kind Enum8('book' = 1, 'gamma_metadata' = 2, 'clickhouse_fact' = 3, 'domain_external' = 4, 'derived' = 5),
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
    direction Enum8('negative' = -1, 'neutral' = 0, 'positive' = 1),
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
    side Enum8('yes' = 1, 'no' = 2),
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
    side Enum8('yes' = 1, 'no' = 2),
    score Decimal64(8),
    risk_adjusted_score Decimal64(8),
    suggested_usd Decimal128(18),
    valid_until DateTime64(3, 'UTC'),
    status Enum8('published' = 1, 'revoked' = 2, 'expired' = 3, 'intent_created' = 4, 'executed' = 5, 'attributed' = 6)
)
ENGINE = MergeTree
ORDER BY (recommendation_report_id, rank, market_id);

CREATE TABLE IF NOT EXISTS quant_execution_event
(
    event_time DateTime64(3, 'UTC'),
    order_intent_id String,
    execution_order_id String,
    recommendation_id String,
    event_kind Enum8('submitted' = 1, 'submission_result' = 2, 'exit_submitted' = 3, 'exit_submission_result' = 4, 'reconciled' = 5, 'operator_resolved' = 6, 'unresolvable' = 7, 'settlement_redeem_confirmed' = 8, 'opened' = 9),
    market_id String,
    token_id String,
    side Enum8('buy' = 1, 'sell' = 2),
    price Decimal64(8),
    shares Decimal128(18),
    cost_usd Decimal128(18),
    venue_order_id Nullable(String),
    ingestion_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (order_intent_id, event_time, ingestion_time);

CREATE TABLE IF NOT EXISTS quant_capital_allocation_event
(
    event_time DateTime64(3, 'UTC'),
    capital_allocation_id String,
    order_intent_id String,
    recommendation_id String,
    event_kind Enum8('submitted' = 1, 'submission_result' = 2, 'exit_submitted' = 3, 'exit_submission_result' = 4, 'reconciled' = 5, 'operator_resolved' = 6, 'unresolvable' = 7, 'settlement_redeem_confirmed' = 8, 'opened' = 9),
    state Enum8('planned' = 1, 'allocated' = 2, 'locked' = 3, 'spent' = 4, 'released' = 5, 'impaired' = 6),
    allocated_usd Decimal128(18),
    locked_usd Decimal128(18),
    spent_usd Decimal128(18),
    released_usd Decimal128(18),
    ingestion_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (order_intent_id, event_time, ingestion_time);

CREATE TABLE IF NOT EXISTS quant_position_event
(
    event_time DateTime64(3, 'UTC'),
    position_id String,
    order_intent_id String,
    market_id String,
    token_id String,
    event_kind Enum8('submitted' = 1, 'submission_result' = 2, 'exit_submitted' = 3, 'exit_submission_result' = 4, 'reconciled' = 5, 'operator_resolved' = 6, 'unresolvable' = 7, 'settlement_redeem_confirmed' = 8, 'opened' = 9),
    state Enum8('open' = 1, 'closing' = 2, 'closed' = 3, 'settled' = 4),
    side Enum8('yes' = 1, 'no' = 2),
    shares Decimal128(18),
    avg_price Decimal64(8),
    cost_usd Decimal128(18),
    realized_pnl_usd Decimal128(18),
    ingestion_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (market_id, token_id, event_time, ingestion_time);

CREATE TABLE IF NOT EXISTS quant_recommendation_attribution_event
(
    event_time DateTime64(3, 'UTC'),
    recommendation_id String,
    outcome Enum8('filled_exited' = 1, 'filled_settled' = 2, 'expired_unfilled' = 3, 'cancelled_unfilled' = 4, 'failed_unfilled' = 5),
    realized_pnl_usd Decimal128(18),
    max_adverse_excursion_bps Nullable(Decimal64(8)),
    max_favorable_excursion_bps Decimal64(8),
    label_available_at DateTime64(3, 'UTC'),
    ingestion_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (recommendation_id, event_time, ingestion_time);

CREATE TABLE IF NOT EXISTS quant_exit_signal_evaluation_event
(
    event_time DateTime64(3, 'UTC'),
    order_intent_id String,
    position_id String,
    market_id String,
    token_id String,
    evaluator_kind Enum8('reinference' = 1, 'opportunistic' = 2),
    verdict Enum8('thesis_invalidated' = 1, 'opportunistic_sell' = 2, 'holds' = 3, 'indeterminate' = 4),
    model_version_id Nullable(String),
    mark_price Nullable(Decimal64(8)),
    entry_composite_score Decimal64(8),
    fresh_composite_score Nullable(Decimal64(8)),
    exit_alpha_bps Nullable(Decimal64(8)),
    confidence Nullable(Decimal64(8)),
    target_cumulative_exit_pct Nullable(Decimal64(8)),
    shadow UInt8,
    detail String,
    ingestion_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (order_intent_id, event_time, ingestion_time);
