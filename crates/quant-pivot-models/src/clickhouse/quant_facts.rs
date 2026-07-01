//! Quant-pivot `ClickHouse` fact rows.

use crate::{
    clickhouse::{ChDecimal64, ChPrice, ChProbability, ChShares, ChUsd},
    enums::clickhouse::{
        ChCapitalAllocationState, ChExecutionSide, ChExitSignalEvaluatorKind, ChExitSignalVerdict,
        ChFactorDirection, ChFeatureSourceKind, ChFeatureValueKind, ChOutcomeSide,
        ChPositionLedgerState, ChQuantLedgerEventKind, ChRecommendationAttributionOutcome,
        ChRecommendationStatus,
    },
    types::{
        CapitalAllocationId, ExecutionOrderId, MarketId, ModelRunId, ModelVersionId, OrderId,
        OrderIntentId, PositionId, RecommendationId, RecommendationReportId, SignalCandidateId,
        TokenId,
    },
};
use serde::{Deserialize, Serialize};

/// Feature value fact emitted by PIT feature builders.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantFeatureEventRow {
    pub event_time: i64,
    pub as_of: i64,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub feature_schema_version: u32,
    pub feature_name: String,
    pub feature_value: ChDecimal64,
    pub value_kind: ChFeatureValueKind,
    pub source_kind: ChFeatureSourceKind,
    pub staleness_ms: u64,
    pub ingestion_time: i64,
}

/// Factor value fact emitted after feature normalization.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantFactorEventRow {
    pub event_time: i64,
    pub as_of: i64,
    pub market_id: MarketId,
    pub factor_name: String,
    pub factor_family: String,
    pub raw_value: ChDecimal64,
    pub normalized_score: ChProbability,
    pub confidence: ChProbability,
    pub direction: ChFactorDirection,
    pub model_run_id: ModelRunId,
    pub ingestion_time: i64,
}

/// Candidate signal fact before portfolio pruning.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantSignalCandidateEventRow {
    pub event_time: i64,
    pub signal_candidate_id: SignalCandidateId,
    pub model_run_id: ModelRunId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: ChOutcomeSide,
    pub score: ChProbability,
    pub confidence: ChProbability,
    pub entry_price: ChPrice,
    pub target_price: ChPrice,
    pub stop_price: ChPrice,
    pub rank_before_portfolio: u32,
    pub rejection_reason: String,
}

/// Published recommendation fact.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantRecommendationEventRow {
    pub event_time: i64,
    pub recommendation_report_id: RecommendationReportId,
    pub recommendation_id: RecommendationId,
    pub rank: u32,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: ChOutcomeSide,
    pub score: ChProbability,
    pub risk_adjusted_score: ChProbability,
    pub suggested_usd: ChUsd,
    pub valid_until: i64,
    pub status: ChRecommendationStatus,
}

/// Execution lifecycle fact.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantExecutionEventRow {
    pub event_time: i64,
    pub order_intent_id: OrderIntentId,
    pub execution_order_id: ExecutionOrderId,
    pub recommendation_id: RecommendationId,
    pub event_kind: ChQuantLedgerEventKind,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: ChExecutionSide,
    pub price: ChPrice,
    pub shares: ChShares,
    pub cost_usd: ChUsd,
    pub venue_order_id: Option<OrderId>,
    pub ingestion_time: i64,
}

/// Capital allocation ledger fact.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantCapitalAllocationEventRow {
    pub event_time: i64,
    pub capital_allocation_id: CapitalAllocationId,
    pub order_intent_id: OrderIntentId,
    pub recommendation_id: RecommendationId,
    pub event_kind: ChQuantLedgerEventKind,
    pub state: ChCapitalAllocationState,
    pub allocated_usd: ChUsd,
    pub locked_usd: ChUsd,
    pub spent_usd: ChUsd,
    pub released_usd: ChUsd,
    pub ingestion_time: i64,
}

/// Position lot ledger fact.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantPositionEventRow {
    pub event_time: i64,
    pub position_id: PositionId,
    pub order_intent_id: OrderIntentId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub event_kind: ChQuantLedgerEventKind,
    pub state: ChPositionLedgerState,
    pub side: ChOutcomeSide,
    pub shares: ChShares,
    pub avg_price: ChPrice,
    pub cost_usd: ChUsd,
    pub realized_pnl_usd: ChUsd,
    pub ingestion_time: i64,
}

/// Exit-signal evaluation audit fact (Phase 06.0 re-inference + 06.1 opportunistic).
///
/// One row per model-driven exit-signal evaluation of an open lot, whether it
/// forced a hold, invalidated the thesis, or proposed an opportunistic exit —
/// including shadow evaluations that never submitted. Analytics-only mirror
/// (Postgres carries the authoritative exit ledger); enables ex-post shadow
/// analysis comparing "would-exit" verdicts against realized hold outcomes.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantExitSignalEvaluationEventRow {
    pub event_time: i64,
    pub order_intent_id: OrderIntentId,
    pub position_id: PositionId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub evaluator_kind: ChExitSignalEvaluatorKind,
    pub verdict: ChExitSignalVerdict,
    /// Model version the evaluator scored against (`None` when not model-backed).
    pub model_version_id: Option<ModelVersionId>,
    /// Sell-side mark (best bid) at evaluation time, when the book was readable.
    pub mark_price: Option<ChPrice>,
    /// Frozen entry composite score baseline.
    pub entry_composite_score: ChProbability,
    /// Re-inference fresh composite score (thesis-invalidation path only).
    pub fresh_composite_score: Option<ChProbability>,
    /// Opportunistic expected exit alpha over holding, in bps.
    pub exit_alpha_bps: Option<ChDecimal64>,
    /// Model confidence in the verdict.
    pub confidence: Option<ChProbability>,
    /// Opportunistic target cumulative exit fraction of entry-filled shares.
    pub target_cumulative_exit_pct: Option<ChDecimal64>,
    /// `1` when the evaluation was shadow-only (audited, never submitted).
    pub shadow: u8,
    /// Human-readable verdict detail / reason.
    pub detail: String,
    pub ingestion_time: i64,
}

/// Final recommendation attribution fact.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantRecommendationAttributionEventRow {
    pub event_time: i64,
    pub recommendation_id: RecommendationId,
    pub outcome: ChRecommendationAttributionOutcome,
    pub realized_pnl_usd: ChUsd,
    /// `None` when PG stores `NULL` (filled-path MAE deferred to 06.6 book replay).
    pub max_adverse_excursion_bps: Option<ChDecimal64>,
    pub max_favorable_excursion_bps: ChDecimal64,
    pub label_available_at: i64,
    pub ingestion_time: i64,
}
