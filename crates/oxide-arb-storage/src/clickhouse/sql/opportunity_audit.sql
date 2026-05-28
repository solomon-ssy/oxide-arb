CREATE TABLE IF NOT EXISTS opportunity_audit (
    opportunity_id  String,
    execution_id    String,
    trade_id        String,
    market_id       String,
    event_id        String,
    side            String,
    entry_price     Float64,
    shares          Float64,
    total_cost_usd  Float64,
    total_fees_usd  Float64,
    net_profit_usd  Float64,
    expected_profit Float64,
    edge_bps        UInt32,
    resolution_prob Float64,
    confidence      Float64,
    convergence_secs UInt32,
    price_zone      String,
    duration_bucket String,
    depth_used_pct  Float64,
    staleness       String,
    category        String,
    stage           String,
    stage_order     UInt8,
    stage_at        DateTime64(3, 'UTC'),
    payout_usd      Float64,
    realized_pnl_usd Float64,
    settlement_status Nullable(String),
    settlement_trigger Nullable(String),
    winning_token_id Nullable(String),
    accounting_status Nullable(String),
    fee_source      Nullable(String),
    outcome         Nullable(String),
    rejection_stage Nullable(String),
    rejection_reason Nullable(String),
    detected_at     DateTime64(3, 'UTC'),
    updated_at      DateTime64(3, 'UTC'),
    audit_date      Date MATERIALIZED toDate(detected_at)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(audit_date)
ORDER BY (opportunity_id, stage_order, execution_id, updated_at)
TTL audit_date + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192
