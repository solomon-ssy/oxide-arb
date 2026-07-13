//! Quant-pivot `ClickHouse` fact rows.

use crate::{
    clickhouse::{ChDecimal64, ChPrice, ChProbability, ChShares, ChUsd},
    enums::clickhouse::{
        ChCapitalAllocationState, ChExecutionSide, ChExitSignalEvaluatorKind, ChExitSignalVerdict,
        ChFactorDirection, ChFactorValueState, ChFeatureCellState, ChFeatureSourceKind,
        ChFeatureValueKind, ChNormalizationSource, ChOutcomeSide, ChPositionLedgerState,
        ChQuantLedgerEventKind, ChRecommendationAttributionOutcome, ChRecommendationStatus,
    },
    types::{
        CapitalAllocationId, ExecutionOrderId, FeatureParityEventId, FeatureParityRunId,
        FeatureVectorId, MarketId, ModelRunId, ModelVersionId, OrderId, OrderIntentId, PositionId,
        RecommendationId, RecommendationReportId, RuntimeConfigVersionId, SignalCandidateId,
        TokenId, TrainingDatasetId,
    },
};
use serde::{Deserialize, Serialize};

/// Complete stateful feature-cell evidence emitted by PIT feature builders.
///
/// One row exists for every cell of every persisted feature vector, including
/// vectors rejected by model-required input checks. Missing and structurally
/// not-applicable cells remain explicit. `raw_value` is the exact typed value
/// text and is `None` only for states that carry no value.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantFeatureEventRow {
    pub event_time: i64,
    pub feature_vector_id: FeatureVectorId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub decision_at: i64,
    pub knowledge_cutoff: i64,
    pub per_source_cutoffs_json: String,
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub feature_schema_version: u32,
    pub feature_schema_hash: String,
    pub feature_hash: String,
    /// Hash of the exact persisted decision capture bound to this vector.
    pub decision_capture_hash: String,
    pub feature_name: String,
    pub cell_state: ChFeatureCellState,
    pub raw_value: Option<String>,
    pub value_kind: ChFeatureValueKind,
    /// Source required by the governed feature specification.
    pub source_kind: ChFeatureSourceKind,
    /// Actual evidence source, when the cell carries a source reference.
    pub evidence_source_kind: Option<ChFeatureSourceKind>,
    pub evidence_reference: Option<String>,
    pub evidence_effective_at: Option<i64>,
    /// Source availability time. `None` means the resolver did not provide it;
    /// decision/persistence timestamps must never be substituted here.
    pub evidence_available_at: Option<i64>,
    pub reason: Option<String>,
    pub staleness_ms: Option<u64>,
    pub data_quality: String,
    /// Canonical hash of every audit field above (excluding transport-only
    /// `ingestion_time` and the fingerprint itself).
    pub audit_fingerprint: String,
    pub ingestion_time: i64,
}

/// Exact model-input evidence persisted for every serving decision.
///
/// `encoded_value_bits` stores the IEEE-754 payload without decimal/text
/// round-tripping, making byte-level training/serving comparisons possible. It
/// is `None` only for a weighted factor whose explicit state has no normalized
/// score; no numeric sentinel is written.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantModelInputEventRow {
    pub event_time: i64,
    pub decision_at: i64,
    pub knowledge_cutoff: i64,
    pub model_run_id: ModelRunId,
    pub model_version_id: ModelVersionId,
    pub recommendation_report_id: Option<RecommendationReportId>,
    pub market_id: MarketId,
    pub feature_vector_id: FeatureVectorId,
    pub model_family: String,
    pub raw_input_name: String,
    pub raw_state: String,
    pub raw_value: Option<String>,
    pub encoded_column: String,
    pub encoded_value_bits: Option<u64>,
    pub input_contract_hash: String,
    pub transform_hash: String,
    pub training_input_hash: String,
    pub audit_fingerprint: String,
    pub ingestion_time: i64,
}

/// Durable completion barrier for one serving model run.
///
/// A row is written only after the exact feature-cell and model-input batches
/// named by the commitments have been acknowledged by `ClickHouse`. The model
/// run may transition to `Succeeded` only after this marker is acknowledged.
/// Replay therefore reasons about this run-scoped marker instead of an
/// unrelated global ingestion watermark.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantServingEvidenceCompletionRow {
    pub event_time: i64,
    pub format_version: u32,
    pub model_run_id: ModelRunId,
    pub decision_at: i64,
    pub knowledge_cutoff: i64,
    pub feature_vector_ids_json: String,
    pub expected_feature_row_count: u64,
    pub feature_rows_hash: String,
    pub expected_model_input_row_count: u64,
    pub model_input_rows_hash: String,
    pub completion_hash: String,
    pub ingestion_time: i64,
}

/// Row-level deterministic online/replay comparison evidence.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantFeatureParityEventRow {
    pub event_time: i64,
    pub parity_event_id: FeatureParityEventId,
    pub parity_run_id: FeatureParityRunId,
    pub decision_at: i64,
    pub stage: String,
    pub status: String,
    pub report_id: Option<RecommendationReportId>,
    pub model_run_id: Option<ModelRunId>,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub market_id: Option<MarketId>,
    pub feature_name: Option<String>,
    pub reason: Option<String>,
    pub online_state: Option<String>,
    pub replay_state: Option<String>,
    pub online_value: Option<String>,
    pub replay_value: Option<String>,
    pub online_effective_at: Option<i64>,
    pub online_available_at: Option<i64>,
    pub online_cutoff: Option<i64>,
    pub replay_effective_at: Option<i64>,
    pub replay_available_at: Option<i64>,
    pub replay_cutoff: Option<i64>,
    pub feature_contract_hash: String,
    pub transform_hash: String,
    pub online_fingerprint: String,
    pub replay_fingerprint: String,
    pub detail_json: String,
    pub ingestion_time: i64,
}

/// Factor value fact emitted after feature normalization.
///
/// Scored factors of eligible markets carry present raw/normalized values.
/// Structurally **not-applicable** factors (e.g. neg-risk on a binary market)
/// also emit a row tagged with `value_state = not_applicable` so analytics can
/// distinguish structural absence from missing data — the authoritative record
/// with full detail lives in Postgres `quant_factor_value`.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantFactorEventRow {
    pub event_time: i64,
    pub decision_at: i64,
    pub market_id: MarketId,
    pub factor_name: String,
    pub factor_family: String,
    pub value_state: ChFactorValueState,
    pub raw_value: Option<ChDecimal64>,
    pub normalized_score: Option<ChProbability>,
    /// How the score was derived (absent when not scored).
    pub normalization_source: Option<ChNormalizationSource>,
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
