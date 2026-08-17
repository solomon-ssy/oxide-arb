//! Quant-pivot `ClickHouse` fact rows.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChDecimal64, ChPrice, ChProbability, ChShares, ChUsd},
    enums::clickhouse::{
        ChCapitalAllocationState, ChExecutionSide, ChExitSignalEvaluatorKind, ChExitSignalVerdict,
        ChFactorDirection, ChFactorValueState, ChFeatureCellState, ChFeatureSourceKind,
        ChFeatureValueKind, ChNormalizationSource, ChOutcomeSide, ChPositionLedgerState,
        ChQuantLedgerEventKind,
    },
    hashing::CanonicalDigest,
    types::{
        CapitalAllocationId, ContentHash, DecisionPolicySnapshotId, EconomicTierId, EventId,
        ExecutionOrderId, FeatureParityEventId, FeatureParityRunId, FeatureVectorId, MarketId,
        MarketSelectionId, ModelRunId, ModelVersionId, OrderId, OrderIntentId, PositionId,
        RecommendationId, RecommendationReportId, ReportFunnelDiagnostics, ReportFunnelReason,
        ReportFunnelStage, ReportRouteRunId, SignalCandidateId, TokenId, TrainingDatasetId,
    },
};

/// Complete stateful feature-cell evidence emitted by PIT feature builders.
///
/// One row exists for every cell of every persisted feature vector, including
/// vectors rejected by model-required input checks. Missing and structurally
/// not-applicable cells remain explicit. `raw_value` is the exact typed value
/// text and is `None` only for states that carry no value.
#[derive(Debug, Clone, PartialEq, Eq, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantFeatureEventRow {
    pub event_time: i64,
    pub feature_vector_id: FeatureVectorId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
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
#[derive(Debug, Clone, PartialEq, Eq, clickhouse::Row, Serialize, Deserialize)]
pub struct QuantModelInputEventRow {
    pub event_time: i64,
    /// Version of the run-scoped serving-evidence wire contract.
    pub format_version: u32,
    pub decision_at: i64,
    pub knowledge_cutoff: i64,
    pub model_run_id: ModelRunId,
    pub model_version_id: ModelVersionId,
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
#[derive(Debug, Clone, PartialEq, Eq, clickhouse::Row, Serialize, Deserialize)]
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
    pub route_rank: u32,
    pub rejection_reason: String,
}

/// Immutable report-scoped recommendation decision fact.
///
/// Live recommendation/report/delivery lifecycle belongs exclusively to
/// Postgres and must never be copied into this prepare-time snapshot.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuantReportRecommendationFactRow {
    pub event_time: i64,
    pub recommendation_report_id: RecommendationReportId,
    pub recommendation_id: RecommendationId,
    pub report_route_run_id: ReportRouteRunId,
    pub economic_tier_id: EconomicTierId,
    pub route: String,
    pub rank: u32,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: ChOutcomeSide,
    pub profit_probability_bps: i64,
    pub nominal_expected_net_usd: ChUsd,
    pub robust_expected_net_usd: ChUsd,
    pub max_loss_usd: ChUsd,
    pub cvar_contribution_usd: ChUsd,
    pub capital_occupancy_usd_hours: ChUsd,
    pub marginal_portfolio_value_usd: ChUsd,
    pub hard_reserved_cash_usd: ChUsd,
    pub valid_until: i64,
}

/// Conserved, report-scoped decision for one catalog-visible market.
///
/// `(recommendation_report_id, market_id)` is the logical key. Every report
/// must durably acknowledge the complete batch before its Postgres header can
/// become visible.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct ReportMarketFunnelRow {
    pub event_time: i64,
    pub recommendation_report_id: RecommendationReportId,
    pub market_selection_id: MarketSelectionId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub report_route_run_id: Option<ReportRouteRunId>,
    pub route: Option<String>,
    pub model_version_id: Option<ModelVersionId>,
    pub model_run_id: Option<ModelRunId>,
    pub market_id: MarketId,
    pub event_id: EventId,
    /// Primary selection token used by feature construction and model input.
    /// A published recommendation may target the complementary outcome token;
    /// that action token lives in `quant_report_recommendation_fact`.
    pub primary_token_id: TokenId,
    pub terminal_stage: String,
    pub primary_reason: String,
    pub secondary_diagnostics_json: String,
    pub feature_vector_id: Option<FeatureVectorId>,
    pub signal_candidate_id: Option<SignalCandidateId>,
    pub recommendation_id: Option<RecommendationId>,
    pub row_hash: String,
    pub ingestion_time: i64,
}

/// Stable contract failure for a conserved report-market funnel row.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid report-market funnel row: {detail}")]
pub struct ReportMarketFunnelRowError {
    detail: String,
}

#[derive(Serialize)]
struct ReportMarketFunnelHashInput<'a> {
    event_time: i64,
    recommendation_report_id: &'a RecommendationReportId,
    market_selection_id: &'a MarketSelectionId,
    decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    report_route_run_id: &'a Option<ReportRouteRunId>,
    route: &'a Option<String>,
    model_version_id: &'a Option<ModelVersionId>,
    model_run_id: &'a Option<ModelRunId>,
    market_id: &'a MarketId,
    event_id: &'a EventId,
    primary_token_id: &'a TokenId,
    terminal_stage: ReportFunnelStage,
    primary_reason: ReportFunnelReason,
    secondary_diagnostics: &'a ReportFunnelDiagnostics,
    feature_vector_id: &'a Option<FeatureVectorId>,
    signal_candidate_id: &'a Option<SignalCandidateId>,
    recommendation_id: &'a Option<RecommendationId>,
}

impl ReportMarketFunnelRow {
    /// Recompute the semantic hash of every immutable business field.
    pub fn expected_row_hash(&self) -> Result<ContentHash, ReportMarketFunnelRowError> {
        if self.ingestion_time < self.event_time {
            return Err(Self::invalid("ingestion time precedes event time"));
        }
        if self.report_route_run_id.is_some() != self.route.is_some() {
            return Err(Self::invalid(
                "report Route run and Route label must be present together",
            ));
        }
        let terminal_stage = ReportFunnelStage::from_str(&self.terminal_stage)
            .map_err(|error| Self::invalid(format!("terminal stage: {error}")))?;
        let primary_reason = ReportFunnelReason::from_str(&self.primary_reason)
            .map_err(|error| Self::invalid(format!("primary reason: {error}")))?;
        let secondary_diagnostics =
            serde_json::from_str::<ReportFunnelDiagnostics>(&self.secondary_diagnostics_json)
                .map_err(|error| Self::invalid(format!("secondary diagnostics: {error}")))?;
        secondary_diagnostics
            .validate_for(primary_reason)
            .map_err(Self::invalid)?;
        let canonical_diagnostics = serde_json::to_string(&secondary_diagnostics)
            .map_err(|error| Self::invalid(format!("secondary diagnostics: {error}")))?;
        if canonical_diagnostics != self.secondary_diagnostics_json {
            return Err(Self::invalid("secondary diagnostics JSON is not canonical"));
        }
        CanonicalDigest::content_hash_json(&ReportMarketFunnelHashInput {
            event_time: self.event_time,
            recommendation_report_id: &self.recommendation_report_id,
            market_selection_id: &self.market_selection_id,
            decision_policy_snapshot_id: &self.decision_policy_snapshot_id,
            report_route_run_id: &self.report_route_run_id,
            route: &self.route,
            model_version_id: &self.model_version_id,
            model_run_id: &self.model_run_id,
            market_id: &self.market_id,
            event_id: &self.event_id,
            primary_token_id: &self.primary_token_id,
            terminal_stage,
            primary_reason,
            secondary_diagnostics: &secondary_diagnostics,
            feature_vector_id: &self.feature_vector_id,
            signal_candidate_id: &self.signal_candidate_id,
            recommendation_id: &self.recommendation_id,
        })
        .map_err(|error| Self::invalid(format!("canonical hash: {error}")))
    }

    /// Seal a newly constructed row. Existing hash text is rejected.
    pub fn seal_hash(&mut self) -> Result<(), ReportMarketFunnelRowError> {
        if !self.row_hash.is_empty() {
            return Err(Self::invalid("row is already sealed"));
        }
        self.row_hash = self.expected_row_hash()?.to_string();
        Ok(())
    }

    /// Verify stored hash text against the complete row contract.
    pub fn verify_hash(&self) -> Result<(), ReportMarketFunnelRowError> {
        let stored = ContentHash::from_str(&self.row_hash)
            .map_err(|error| Self::invalid(format!("stored row hash: {error}")))?;
        if stored.to_string() != self.row_hash || stored != self.expected_row_hash()? {
            return Err(Self::invalid("stored row hash does not match row content"));
        }
        Ok(())
    }

    fn invalid(detail: impl Into<String>) -> ReportMarketFunnelRowError {
        ReportMarketFunnelRowError {
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct ReportMarketFunnelCountRow {
    pub terminal_stage: String,
    pub row_count: u64,
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

/// Exit-signal evaluation audit fact for re-inference and opportunistic exits.
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

#[cfg(test)]
mod report_market_funnel_tests {
    use std::error::Error;

    use super::ReportMarketFunnelRow;
    use crate::types::{
        DecisionPolicySnapshotId, EventId, FeatureVectorId, MarketId, MarketSelectionId,
        ModelRunId, ModelVersionId, RecommendationReportId, ReportRouteRunId, TokenId,
    };

    fn funnel_row(event_time: i64) -> ReportMarketFunnelRow {
        ReportMarketFunnelRow {
            event_time,
            recommendation_report_id: RecommendationReportId::from_v7(),
            market_selection_id: MarketSelectionId::from_v7(),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            report_route_run_id: Some(ReportRouteRunId::from_v7()),
            route: Some("pooled".to_owned()),
            model_version_id: Some(ModelVersionId::from_v7()),
            model_run_id: Some(ModelRunId::from_v7()),
            market_id: MarketId::new("market-1"),
            event_id: EventId::new("event-1"),
            primary_token_id: TokenId::new("101"),
            terminal_stage: "model_scored".to_owned(),
            primary_reason: "no_positive_signal".to_owned(),
            secondary_diagnostics_json: r#"{"kind":"none"}"#.to_owned(),
            feature_vector_id: Some(FeatureVectorId::from_v7()),
            signal_candidate_id: None,
            recommendation_id: None,
            row_hash: String::new(),
            ingestion_time: event_time,
        }
    }

    #[test]
    fn semantic_hash_detects_tampering() -> Result<(), Box<dyn Error>> {
        let mut row = funnel_row(1_700_000_000_000);
        row.seal_hash()?;
        row.verify_hash()?;

        let mut tampered = row.clone();
        tampered.market_id = MarketId::new("market-2");
        assert!(tampered.verify_hash().is_err());
        assert!(row.seal_hash().is_err());
        Ok(())
    }

    #[test]
    fn seal_rejects_noncanonical_diagnostics() {
        let mut row = funnel_row(1_700_000_000_000);
        row.secondary_diagnostics_json = r#"{ "kind": "none" }"#.to_owned();
        assert!(row.seal_hash().is_err());
    }
}
