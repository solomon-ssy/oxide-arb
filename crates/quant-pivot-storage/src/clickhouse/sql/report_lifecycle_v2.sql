DROP TABLE IF EXISTS quant_recommendation_event;

DROP TABLE IF EXISTS quant_recommendation_attribution_event;

CREATE TABLE IF NOT EXISTS quant_report_recommendation_fact
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
    trade_plan_available Bool,
    suggested_usd Nullable(Decimal128(18)),
    valid_until DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(event_time)
ORDER BY (recommendation_report_id, recommendation_id);

CREATE TABLE IF NOT EXISTS quant_recommendation_attribution_event
(
    event_time DateTime64(3, 'UTC'),
    recommendation_id String,
    outcome Enum8('filled_exited' = 1, 'filled_settled' = 2, 'expired_unfilled' = 3, 'cancelled_unfilled' = 4, 'failed_unfilled' = 5, 'superseded_unfilled' = 6),
    realized_pnl_usd Decimal128(18),
    max_adverse_excursion_bps Nullable(Decimal64(8)),
    max_favorable_excursion_bps Decimal64(8),
    label_available_at DateTime64(3, 'UTC'),
    ingestion_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (recommendation_id, event_time, ingestion_time);
