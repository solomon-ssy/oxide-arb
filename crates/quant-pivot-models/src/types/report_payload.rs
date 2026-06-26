//! Strong-typed recommendation/report payload value types.
//!
//! These types are the **content contract** for the JSONB columns on
//! `quant_recommendation` (`entry_plan` / `sizing_plan` / `exit_plan` /
//! `risk_envelope` / `factor_breakdown` / `evidence_refs` / `execution_eligibility`)
//! and `quant_recommendation_report` (`summary_json`). They live in `types/`
//! (below the entity layer) so the entity `Model` can use them directly as column
//! types, keeping `info_from_model!` / `DeriveIntoActiveModel` 1:1 with no
//! hand-rolled `serde_json::Value` round-tripping.
//!
//! Every monetary / price / probability field uses a project newtype; basis
//! points use [`Bps`]. Schema evolution happens through additive fields guarded
//! by `#[serde(default)]`, never through a bare `serde_json::Value`.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{
        common::MarketCategory,
        factor::FactorFamily,
        quant::{
            BindingConstraint, EmptyReason, EntryTriggerKind, ExitTriggerKind, FactorDirection,
            IneligibilityReason, QuantRuntimeMode, SettlementPolicy, SizingModelKind,
        },
    },
    jsonb_active,
    types::{
        Bps, ContentHash, EventId, FactorDefinitionId, FeatureVectorId, MarketSelectionId,
        ModelRunId, ModelVersionId, Price, Probability, RuntimeConfigVersionId, Shares,
        SignalCandidateId, Usd,
    },
};

// ── Entry plan (parent §8 "when to buy") ─────────────────────────────────────

/// When and how a recommendation becomes executable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct EntryPlan {
    /// How the entry is triggered.
    pub trigger_kind: EntryTriggerKind,
    /// Trigger price for breakout / pullback / limit triggers.
    pub trigger_price: Option<Price>,
    /// Hard limit price for the entry order.
    pub limit_price: Option<Price>,
    /// Maximum acceptable slippage from the reference price.
    pub max_slippage_bps: Bps,
    /// Earliest time the entry is valid.
    pub valid_from: DateTime<Utc>,
    /// Latest time the entry is valid.
    pub valid_until: DateTime<Utc>,
    /// Minimum visible depth (USD) required at entry.
    pub min_depth_usd: Usd,
    /// Maximum tolerated book age at entry.
    pub max_book_age_ms: u64,
    /// Confirmation window before a triggered entry fires.
    pub confirmation_window_secs: u64,
    /// Whether to cancel the entry if it never triggers within the window.
    pub cancel_if_not_triggered: bool,
    /// Human explanation of the entry decision.
    pub entry_reason: String,
}

// ── Sizing plan (parent §9 "how much to buy") ────────────────────────────────

/// How much capital a recommendation should deploy and the binding cap.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct SizingPlan {
    /// Suggested allocation in USD.
    pub suggested_usd: Usd,
    /// Suggested allocation in shares at the reference price.
    pub suggested_shares: Shares,
    /// Upper bound permitted by the caps.
    pub max_usd: Usd,
    /// Lower useful bound (below this the recommendation is dropped).
    pub min_usd: Usd,
    /// Suggested allocation as a fraction of the capital base.
    pub portfolio_weight_pct: Decimal,
    /// Projected market exposure after this allocation.
    pub market_exposure_after_usd: Usd,
    /// Projected event exposure after this allocation.
    pub event_exposure_after_usd: Usd,
    /// Projected category exposure after this allocation.
    pub category_exposure_after_usd: Usd,
    /// The cap that bound the final size.
    pub binding_constraint: BindingConstraint,
    /// Human explanation of the sizing decision.
    pub sizing_reason: String,
    /// Which sizing model produced this size.
    pub sizing_model: SizingModelKind,
    /// Estimated edge over the entry price (Kelly provenance).
    pub edge_bps: Option<Bps>,
    /// The fractional-Kelly multiplier actually applied.
    pub kelly_fraction_applied: Option<Decimal>,
}

// ── Exit plan (parent §10 "when / how much to sell") ─────────────────────────

/// When and how a recommendation should be exited.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct ExitPlan {
    /// Take-profit price target.
    pub take_profit_price: Option<Price>,
    /// Take-profit as a percentage move.
    pub take_profit_pct: Option<Decimal>,
    /// Stop-loss price target.
    pub stop_loss_price: Option<Price>,
    /// Stop-loss as a percentage move.
    pub stop_loss_pct: Option<Decimal>,
    /// Absolute time-based exit.
    pub time_exit_at: Option<DateTime<Utc>>,
    /// Maximum holding period in seconds.
    pub max_hold_secs: Option<u64>,
    /// Scaled partial-exit nodes.
    pub partial_exit_nodes: Vec<PartialExitNode>,
    /// Optional trailing-stop policy.
    pub trailing_stop: Option<TrailingStop>,
    /// Conditions that invalidate the thesis and force an exit.
    pub signal_invalidation_rules: Vec<SignalInvalidationRule>,
    /// How the position settles at resolution.
    pub settlement_policy: SettlementPolicy,
    /// Optional manual-review checkpoint time.
    pub manual_review_at: Option<DateTime<Utc>>,
    /// Human explanation of the exit decision.
    pub exit_reason: String,
}

/// One scaled partial-exit node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialExitNode {
    /// Stable node identifier.
    pub node_id: String,
    /// What triggers this partial exit.
    pub trigger_kind: ExitTriggerKind,
    /// Numeric trigger value (price / pct / seconds depending on `trigger_kind`).
    pub trigger_value: Decimal,
    /// Fraction of the remaining position to sell at this node.
    pub sell_pct: Decimal,
    /// Minimum acceptable sell price.
    pub min_price: Option<Price>,
    /// Earliest time this node is active.
    pub valid_after: Option<DateTime<Utc>>,
    /// Latest time this node is active.
    pub valid_until: Option<DateTime<Utc>>,
    /// Human explanation for this node.
    pub reason: String,
}

/// A trailing-stop policy relative to the position's peak mark.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailingStop {
    /// Trailing distance in basis points below the peak mark.
    pub trail_bps: Bps,
    /// Price that must be reached before the trailing stop arms.
    pub activation_price: Option<Price>,
}

/// A condition that invalidates the recommendation thesis.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalInvalidationRule {
    /// Stable rule identifier.
    pub rule_id: String,
    /// Human description of the invalidating condition.
    pub description: String,
    /// Optional numeric threshold associated with the rule.
    pub threshold: Option<Decimal>,
}

// ── Risk envelope (parent §11 — admission inputs) ────────────────────────────

/// Hard risk bounds consumed by execution admission (not natural language).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct RiskEnvelope {
    /// Maximum tolerated loss in USD.
    pub max_loss_usd: Usd,
    /// Maximum tolerated slippage.
    pub max_slippage_bps: Bps,
    /// Maximum position size in USD.
    pub max_position_usd: Usd,
    /// Maximum market exposure in USD.
    pub max_market_exposure_usd: Usd,
    /// Maximum event exposure in USD.
    pub max_event_exposure_usd: Usd,
    /// Maximum category exposure in USD.
    pub max_category_exposure_usd: Usd,
    /// Whether human approval is required before execution.
    pub requires_approval: bool,
    /// Whether auto-execution is permitted by this envelope.
    pub auto_execution_allowed: bool,
    /// Free-form risk notes for the report.
    pub risk_notes: Vec<String>,
    /// Canonical hash of the envelope (admission verification).
    pub envelope_hash: ContentHash,
}

// ── Factor breakdown (parent §12) ────────────────────────────────────────────

/// One factor's signed contribution to a recommendation's composite score.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorBreakdownEntry {
    /// Factor name.
    pub factor_name: String,
    /// Factor family.
    pub family: FactorFamily,
    /// Raw factor value before normalization.
    pub raw_value: Option<Decimal>,
    /// Normalized factor score in `[0, 1]`.
    pub normalized_score: Probability,
    /// Weight applied by the model.
    pub weight: Decimal,
    /// Signed contribution to the composite score.
    pub contribution: Decimal,
    /// Confidence attached to the factor.
    pub confidence: Probability,
    /// Direction the factor pushed the score.
    pub direction: FactorDirection,
    /// Human explanation.
    pub explanation: String,
    /// References to the evidence behind this factor.
    pub source_refs: Vec<String>,
}

/// JSONB column wrapper for a recommendation's full factor breakdown.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct RecommendationFactorBreakdown(pub Vec<FactorBreakdownEntry>);

// ── Evidence refs (parent §13 — replay) ──────────────────────────────────────

/// Replay handles for one recommendation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct EvidenceRefs {
    /// Signal candidate this recommendation was promoted from (replay handle into
    /// the `quant_signal_candidate_event` fact).
    pub signal_candidate_id: SignalCandidateId,
    /// Feature vector that fed inference.
    pub feature_vector_id: FeatureVectorId,
    /// Model run that emitted the candidate.
    pub model_run_id: ModelRunId,
    /// Market selection snapshot.
    pub market_selection_id: MarketSelectionId,
    /// Optional book-snapshot reference.
    pub book_snapshot_ref: Option<String>,
    /// Runtime-config version frozen for the run.
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Model version used.
    pub model_version_id: ModelVersionId,
    /// Factor definition versions used.
    pub factor_definition_versions: Vec<FactorDefinitionId>,
    /// Optional data-quality report reference.
    pub data_quality_report_ref: Option<String>,
}

// ── Execution eligibility (parent §14 — computed, mode-orthogonal) ────────────

/// Per-recommendation execution eligibility across runtime modes.
///
/// Computed and persisted in Phase 4; the actual create-intent / admission flow
/// lands in Phase 5. `eligible_modes` always contains
/// [`QuantRuntimeMode::ReportOnly`] (a report is the report-only artifact).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct ExecutionEligibility {
    /// Runtime modes in which this recommendation is eligible for execution.
    pub eligible_modes: Vec<QuantRuntimeMode>,
    /// Reasons the recommendation is ineligible (empty when fully eligible).
    pub ineligibility_reasons: Vec<IneligibilityReason>,
    /// Whether human approval is required.
    pub approval_required: bool,
    /// Approval role required, when applicable.
    pub approval_role: Option<String>,
    /// Auto-execution policy id, when applicable.
    pub auto_policy_id: Option<String>,
}

impl ExecutionEligibility {
    /// Whether the recommendation supports order execution in `mode`.
    ///
    /// [`QuantRuntimeMode::ReportOnly`] and [`QuantRuntimeMode::SemiAuto`] are
    /// eligible when listed in [`Self::eligible_modes`]. Auto-execution additionally
    /// requires an empty [`Self::ineligibility_reasons`] (reasons document score /
    /// confidence denials when auto mode is withheld).
    #[must_use]
    pub fn is_eligible(&self, mode: QuantRuntimeMode) -> bool {
        if !self.eligible_modes.contains(&mode) {
            return false;
        }
        mode != QuantRuntimeMode::AutoExecution || self.ineligibility_reasons.is_empty()
    }
}

// ── Report summary (parent §3 — report-level) ────────────────────────────────

/// Report-level summary persisted to `quant_recommendation_report.summary_json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct ReportSummary {
    /// Number of markets in the selection snapshot.
    pub market_selection_count: u32,
    /// Number of scored candidates considered.
    pub candidate_count: u32,
    /// Number of candidates rejected before publication.
    pub rejected_count: u32,
    /// Number of published recommendations.
    pub published_recommendation_count: u32,
    /// Total suggested USD across published recommendations.
    pub total_suggested_usd: Usd,
    /// Largest single suggested USD.
    pub max_single_recommendation_usd: Usd,
    /// Suggested USD allocated per category.
    pub category_allocation: BTreeMap<MarketCategory, Usd>,
    /// Suggested USD allocated per event.
    pub event_allocation: BTreeMap<EventId, Usd>,
    /// Mean composite score across published recommendations.
    pub average_score: Probability,
    /// Minimum composite score across published recommendations.
    pub min_score: Probability,
    /// Model confidence summary.
    pub model_confidence_summary: ConfidenceSummary,
    /// Data-quality summary.
    pub data_quality_summary: DataQualitySummary,
    /// Most common rejection reasons.
    pub top_rejection_reasons: Vec<RejectionReasonCount>,
    /// Execution-eligibility roll-up.
    pub execution_eligibility_summary: EligibilitySummary,
    /// Reason the report is empty, when `published_recommendation_count == 0`.
    pub empty_reason: Option<EmptyReason>,
    /// Report-level warnings.
    pub warnings: Vec<String>,
}

/// Confidence distribution across published recommendations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfidenceSummary {
    /// Mean confidence.
    pub mean_confidence: Probability,
    /// Minimum confidence.
    pub min_confidence: Probability,
    /// Maximum confidence.
    pub max_confidence: Probability,
}

/// Counts of candidates by data-quality classification.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataQualitySummary {
    /// Fresh inputs.
    pub fresh_count: u32,
    /// Acceptable inputs.
    pub acceptable_count: u32,
    /// Degraded inputs.
    pub degraded_count: u32,
    /// Stale inputs.
    pub stale_count: u32,
    /// Insufficient inputs.
    pub insufficient_count: u32,
}

/// One rejection-reason tally.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RejectionReasonCount {
    /// Rejection reason label.
    pub reason: String,
    /// Number of candidates rejected for this reason.
    pub count: u32,
}

/// Execution-eligibility roll-up across published recommendations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EligibilitySummary {
    /// Eligible under report-only.
    pub eligible_report_only: u32,
    /// Eligible under semi-auto.
    pub eligible_semi_auto: u32,
    /// Eligible under auto-execution.
    pub eligible_auto_execution: u32,
}

jsonb_active!(
    EntryPlan,
    SizingPlan,
    ExitPlan,
    RiskEnvelope,
    RecommendationFactorBreakdown,
    EvidenceRefs,
    ExecutionEligibility,
    ReportSummary,
);
