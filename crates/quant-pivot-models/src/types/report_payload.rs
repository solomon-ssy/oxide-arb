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

use std::{
    collections::BTreeMap,
    iter::Sum,
    ops::{Add, AddAssign},
};

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use rust_decimal::Decimal;
use schemars::JsonSchema;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{
        common::MarketCategory,
        factor::{FactorFamily, FactorIndeterminateReason, FactorValueState, NormalizationSource},
        quant::{
            EmptyReportReason, ExecutionAuthorityCeiling, ExitSettlementMode, FactorDirection,
            FillRequirement, IneligibilityReason, RedeemPolicy,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::BuyModelRoute,
    types::{
        BookSnapshotRef, Bps, ContentHash, DecisionPolicySnapshotId, EconomicTierId,
        EntryConditionPlan, EntryMakerRebateTerms, EventId, FactorDefinitionId, FeatureVectorId,
        MakerRebateDelayBasis, MakerRebateObjectiveStatus, MarketSelectionId, ModelRunId,
        ModelVersionId, Price, Probability, ReportDataQualitySnapshotId, ResearchFeatureContract,
        ResearchProfileRef, Shares, SignalCandidateId, TradePolicyArtifactId, TradePolicyCohortKey,
        Usd, UsdHours,
    },
};

// ── Entry plan: when to buy ───────────────────────────────────────────────

/// When and how a recommendation becomes executable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
pub struct EntryPlan {
    /// Immutable condition artifact reference evaluated at recommendation scope.
    pub condition: EntryConditionPlan,
    /// Venue execution policy, orthogonal to the entry condition.
    pub order_policy: EntryOrderPolicy,
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
    /// Whether to cancel the entry if it never triggers within the window.
    pub cancel_if_not_triggered: bool,
    /// Human explanation of the entry decision.
    pub entry_reason: String,
}

/// How an armed entry is submitted to the venue.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum EntryOrderPolicy {
    /// Rest a bounded, post-only limit order until the recommendation expires.
    Passive { limit_price: Price, post_only: bool },
    /// Cross the spread with a worst-price cap and explicit fill semantics.
    Aggressive {
        worst_price: Price,
        fill_requirement: FillRequirement,
    },
}

impl EntryOrderPolicy {
    /// The hard maximum buy price admitted for this plan.
    #[must_use]
    pub const fn limit_price(&self) -> Price {
        match self {
            Self::Passive { limit_price, .. } => *limit_price,
            Self::Aggressive { worst_price, .. } => *worst_price,
        }
    }
}

// ── Sizing plan: how much to buy ──────────────────────────────────────────

/// How much capital a recommendation should deploy and the binding cap.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
pub struct SizingPlan {
    /// Exact immutable tier selected by the global MILP.
    pub economic_tier_id: EconomicTierId,
    /// Shares submitted to the selected route.
    pub requested_shares: Shares,
    /// OOS expected fill quantity; equals requested shares for aggressive entry.
    pub expected_filled_shares: Shares,
    /// Cash reserved independently of expected passive fills or incentives.
    pub hard_reserved_cash_usd: Usd,
    /// Immediate full-fill venue and builder fee.
    pub immediate_fee_usd: Usd,
    /// Delayed maker incentive expectation; never spendable cash.
    pub expected_maker_rebate_accrual_usd: Usd,
    /// Threshold-aware discounted amount admitted to the expected objective.
    pub objective_maker_rebate_usd: Usd,
    pub maker_rebate_objective_status: MakerRebateObjectiveStatus,
    pub rebate_delay_basis: Option<MakerRebateDelayBasis>,
    pub rebate_valuation_hash: Option<ContentHash>,
    /// Required route applicability and independent Gamma terms.
    pub maker_rebate_terms: EntryMakerRebateTerms,
    /// Aggressive executable VWAP or passive post-only limit price.
    pub reference_entry_price: Price,
    /// Suggested allocation as a fraction of the capital base.
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub portfolio_weight_pct: Decimal,
    /// Projected market exposure after this allocation.
    pub market_exposure_after_usd: Usd,
    /// Projected event exposure after this allocation.
    pub event_exposure_after_usd: Usd,
    /// Projected category exposure after this allocation.
    pub category_exposure_after_usd: Usd,
    /// Projected Route exposure after this allocation.
    pub route_exposure_after_usd: Usd,
    /// Discounted capital occupancy contributed by this tier.
    pub capital_occupancy_usd_hours: UsdHours,
    /// Human explanation of the sizing decision.
    pub sizing_reason: String,
}

// ── Exit plan: when and how much to sell ─────────────────────────────────

/// Policy-fitted opportunistic exit rule frozen into recommendation and intent.
///
/// Runtime configuration may disable or shadow it, but never supplies or
/// tightens these decision thresholds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OpportunisticExitPolicy {
    pub min_confidence: Probability,
    pub min_expected_alpha_bps: Bps,
    pub min_p_exit_better: Probability,
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub max_cumulative_exit_pct: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub min_incremental_exit_pct: Decimal,
}

/// When and how a recommendation should be exited.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
pub struct ExitPlan {
    /// Take-profit price target.
    pub take_profit_price: Option<Price>,
    /// Take-profit as a percentage move.
    #[serde(with = "crate::types::decimal_string_option")]
    #[schemars(with = "Option<String>")]
    pub take_profit_pct: Option<Decimal>,
    /// Stop-loss price target.
    pub stop_loss_price: Option<Price>,
    /// Stop-loss as a percentage move.
    #[serde(with = "crate::types::decimal_string_option")]
    #[schemars(with = "Option<String>")]
    pub stop_loss_pct: Option<Decimal>,
    /// Absolute time-based exit.
    pub time_exit_at: Option<DateTime<Utc>>,
    /// Maximum holding period in seconds.
    pub max_hold_secs: Option<u64>,
    /// Monotone cumulative scale-out targets.
    pub scale_out_targets: Vec<ScaleOutTarget>,
    /// Optional trailing-stop policy.
    pub trailing_stop: Option<TrailingStopPolicy>,
    /// Frozen, machine-evaluable thesis invalidation policy.
    pub thesis_invalidation: ThesisInvalidationPolicy,
    /// Policy-fitted advisory exit thresholds.
    pub opportunistic_exit: OpportunisticExitPolicy,
    /// Whether the lot exits before resolution or holds through resolution.
    pub settlement_mode: ExitSettlementMode,
    /// Whether a resolved hold-to-resolution lot is redeemed automatically.
    pub redeem_policy: RedeemPolicy,
    /// Optional manual-review checkpoint time.
    pub manual_review_at: Option<DateTime<Utc>>,
    /// Human explanation of the exit decision.
    pub exit_reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TradePolicyCohortProvenance {
    pub artifact_id: TradePolicyArtifactId,
    pub artifact_hash: ContentHash,
    pub cohort_index: u32,
    pub cohort_key: TradePolicyCohortKey,
}

/// Exact decision-policy lineage behind one published recommendation.
///
/// Bootstrap recommendations bind their immutable L2-free profile instead of
/// pretending that a historical execution-policy fit exists.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum RecommendationPolicyProvenance {
    TradePolicy {
        artifact_id: TradePolicyArtifactId,
        artifact_hash: ContentHash,
        cohort_index: u32,
        cohort_key: Box<TradePolicyCohortKey>,
    },
    BootstrapProfile {
        profile_ref: ResearchProfileRef,
        feature_contract: ResearchFeatureContract,
        recommendation_contract_hash: ContentHash,
    },
}

impl From<TradePolicyCohortProvenance> for RecommendationPolicyProvenance {
    fn from(value: TradePolicyCohortProvenance) -> Self {
        Self::TradePolicy {
            artifact_id: value.artifact_id,
            artifact_hash: value.artifact_hash,
            cohort_index: value.cohort_index,
            cohort_key: Box::new(value.cohort_key),
        }
    }
}

/// Honest report-only exit guidance for an L2-free bootstrap model.
///
/// This intentionally contains no synthetic take-profit, stop-loss, trailing,
/// or opportunistic-exit thresholds. It is not accepted by execution paths.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BootstrapExitGuidance {
    pub reference_horizon_secs: u64,
    pub manual_review_at: DateTime<Utc>,
    pub settlement_value_is_terminal: bool,
    pub guidance: String,
}

/// Exit authority carried by a recommendation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum RecommendationExitPlan {
    Executable { plan: Box<ExitPlan> },
    BootstrapAdvisory { guidance: BootstrapExitGuidance },
}

impl From<ExitPlan> for RecommendationExitPlan {
    fn from(value: ExitPlan) -> Self {
        Self::Executable {
            plan: Box::new(value),
        }
    }
}

/// The single authoritative recommendation plan contract.
///
/// Full-L2 recommendations own an executable exit plan. Bootstrap
/// recommendations instead own explicit, non-executable manual guidance. Both
/// regimes require calibration, an exact scenario binding, and live L2 entry
/// and sizing evidence before publication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct RecommendationTradePlan {
    pub policy: Box<RecommendationPolicyProvenance>,
    pub entry: EntryPlan,
    pub sizing: Box<SizingPlan>,
    pub exit: Box<RecommendationExitPlan>,
    pub risk_envelope: Box<RiskEnvelope>,
}

/// One deterministic cumulative scale-out target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ScaleOutTarget {
    /// Stable target identifier.
    pub target_id: String,
    /// Executable mark that activates this target.
    pub trigger_price: Price,
    /// Target fraction exited relative to frozen entry-filled shares.
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub target_cumulative_exit_pct: Decimal,
    /// Minimum acceptable sell price.
    pub min_price: Option<Price>,
    /// Earliest time this node is active.
    pub valid_after: Option<DateTime<Utc>>,
    /// Latest time this node is active.
    pub valid_until: Option<DateTime<Utc>>,
    /// Human explanation for this target.
    pub reason: String,
}

/// A trailing-stop policy relative to the position's peak mark.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TrailingStopPolicy {
    /// Trailing distance in basis points below the peak mark.
    pub trail_bps: Bps,
    /// Price that must be reached before the trailing stop arms.
    pub activation_price: Option<Price>,
}

/// Frozen conditions that invalidate the entry thesis.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ThesisInvalidationPolicy {
    /// Minimum fresh-score / entry-score ratio. Must be in `[0, 1]`.
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub min_score_retention: Decimal,
    /// Minimum executable expected return required to keep holding.
    pub min_expected_return_bps: Bps,
    /// Whether the frozen Route-local model gate must still pass to keep holding.
    pub require_route_gate_eligibility: bool,
}

// ── Risk envelope: admission inputs ──────────────────────────────────────

/// Hard risk bounds consumed by execution admission (not natural language).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
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
    /// Maximum model-Route exposure after admitting this recommendation.
    pub max_route_exposure_usd: Usd,
    /// This recommendation's exact contribution to portfolio `CVaR`.
    pub cvar_contribution_usd: Usd,
    /// Governed portfolio `CVaR` cap used by the exact solve.
    pub portfolio_cvar_cap_usd: Usd,
    /// Governed maximum loss over every promoted joint scenario.
    pub maximum_scenario_loss_cap_usd: Usd,
    /// Free-form risk notes for the report.
    pub risk_notes: Vec<String>,
    /// Canonical hash of the envelope (admission verification).
    pub envelope_hash: ContentHash,
}

/// Canonical numeric subset hashed into [`RiskEnvelope::envelope_hash`].
///
/// Free-form notes are intentionally excluded from the admission anchor. Field
/// order and names are part of the hash contract.
#[derive(Serialize)]
pub struct RiskEnvelopeHashInput {
    pub loss_usd: Usd,
    pub slippage_bps: Bps,
    pub position_usd: Usd,
    pub market_exposure_usd: Usd,
    pub event_exposure_usd: Usd,
    pub category_exposure_usd: Usd,
    pub route_exposure_usd: Usd,
    pub cvar_contribution_usd: Usd,
    pub portfolio_cvar_cap_usd: Usd,
    pub maximum_scenario_loss_cap_usd: Usd,
}

impl RiskEnvelope {
    /// Recompute the canonical anchor hash from this envelope's numeric bounds.
    ///
    /// Equal to [`Self::envelope_hash`] for an untampered envelope; admission
    /// compares the recomputation against the hash frozen on the order intent.
    pub fn canonical_hash(&self) -> Result<ContentHash, CanonicalDigestError> {
        CanonicalDigest::content_hash_json(&RiskEnvelopeHashInput {
            loss_usd: self.max_loss_usd,
            slippage_bps: self.max_slippage_bps,
            position_usd: self.max_position_usd,
            market_exposure_usd: self.max_market_exposure_usd,
            event_exposure_usd: self.max_event_exposure_usd,
            category_exposure_usd: self.max_category_exposure_usd,
            route_exposure_usd: self.max_route_exposure_usd,
            cvar_contribution_usd: self.cvar_contribution_usd,
            portfolio_cvar_cap_usd: self.portfolio_cvar_cap_usd,
            maximum_scenario_loss_cap_usd: self.maximum_scenario_loss_cap_usd,
        })
    }
}

// ── Factor breakdown ─────────────────────────────────────────────────────

/// One factor's signed contribution to a recommendation's composite score.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FactorBreakdownEntry {
    /// Factor name.
    pub factor_name: String,
    /// Factor family.
    pub family: FactorFamily,
    /// Authoritative value state (scored / missing-input / not-applicable /
    /// indeterminate) — drives the report's distinct "—" rendering.
    pub value_state: FactorValueState,
    /// Raw factor value before normalization.
    #[serde(with = "crate::types::decimal_string_option")]
    #[schemars(with = "Option<String>")]
    pub raw_value: Option<Decimal>,
    /// Normalized factor score in `[0, 1]`; `None` when the factor was missing
    /// or indeterminate (never a fabricated neutral).
    #[serde(default)]
    pub normalized_score: Option<Probability>,
    /// How the score was derived; `None` when missing / indeterminate.
    #[serde(default)]
    pub normalization_source: Option<NormalizationSource>,
    /// Why the factor was indeterminate; `None` when scored / missing.
    #[serde(default)]
    pub indeterminate_reason: Option<FactorIndeterminateReason>,
    /// Weight applied by the model.
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub weight: Decimal,
    /// Signed contribution to the composite score.
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(transparent)]
pub struct RecommendationFactorBreakdown(pub Vec<FactorBreakdownEntry>);

// ── Evidence refs: replay anchors ────────────────────────────────────────

/// Replay handles for one recommendation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct EvidenceRefs {
    pub signal_candidate_id: SignalCandidateId,
    pub feature_vector_id: FeatureVectorId,
    pub model_run_id: ModelRunId,
    pub market_selection_id: MarketSelectionId,
    pub book_snapshot_ref: BookSnapshotRef,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_version_id: ModelVersionId,
    pub factor_definition_versions: Vec<FactorDefinitionId>,
    pub data_quality_snapshot_ref: ReportDataQualitySnapshotId,
}

/// Inputs required to build [`EvidenceRefs`] from a frozen decision capture.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvidenceRefsInput {
    pub signal_candidate_id: SignalCandidateId,
    pub feature_vector_id: FeatureVectorId,
    pub model_run_id: ModelRunId,
    pub market_selection_id: MarketSelectionId,
    pub book_snapshot_ref: BookSnapshotRef,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_version_id: ModelVersionId,
    pub factor_definition_versions: Vec<FactorDefinitionId>,
    pub data_quality_snapshot_ref: ReportDataQualitySnapshotId,
}

impl EvidenceRefs {
    /// Project capture + report ids into the persisted evidence block.
    #[must_use]
    pub fn from_input(input: EvidenceRefsInput) -> Self {
        Self {
            signal_candidate_id: input.signal_candidate_id,
            feature_vector_id: input.feature_vector_id,
            model_run_id: input.model_run_id,
            market_selection_id: input.market_selection_id,
            book_snapshot_ref: input.book_snapshot_ref,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            model_version_id: input.model_version_id,
            factor_definition_versions: input.factor_definition_versions,
            data_quality_snapshot_ref: input.data_quality_snapshot_ref,
        }
    }
}

// ── Execution eligibility (computed, mode-orthogonal) ─────────────────────

/// Per-recommendation execution authority ceiling and immutable blockers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ExecutionEligibility {
    pub ceiling: ExecutionAuthorityCeiling,
    pub blockers: Vec<IneligibilityReason>,
    pub policy_binding: Option<String>,
}

impl ExecutionEligibility {
    #[must_use]
    pub fn allows_operator(&self) -> bool {
        self.ceiling.allows_operator()
    }

    #[must_use]
    pub fn allows_policy(&self) -> bool {
        self.blockers.is_empty() && self.ceiling.allows_policy() && self.policy_binding.is_some()
    }
}

// ── Report summary ───────────────────────────────────────────────────────

/// Report-level summary persisted to `quant_recommendation_report.summary_json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ReportSummary {
    /// Number of markets in the selection snapshot.
    pub market_selection_count: u32,
    /// Number of atomically ready Routes represented by this report.
    pub represented_route_count: u32,
    /// Number of scored candidates considered.
    pub candidate_count: u32,
    /// Number of executable tiers rejected before publication.
    pub rejected_tier_count: u32,
    /// Number of published recommendations.
    pub published_recommendation_count: u32,
    /// Total hard cash reservation across published recommendations.
    pub total_hard_reserved_cash_usd: Usd,
    /// Largest single-recommendation hard cash reservation.
    pub max_single_recommendation_usd: Usd,
    /// Robust worst-distribution discounted expected net USD for the selected portfolio.
    pub robust_expected_net_usd: Usd,
    /// Nominal discounted expected net USD for the selected portfolio.
    pub nominal_expected_net_usd: Usd,
    /// Exact portfolio `CVaR` under the governed tail mass.
    pub cvar_usd: Usd,
    /// Maximum loss over every promoted scenario.
    pub maximum_scenario_loss_usd: Usd,
    /// Discounted capital occupation across the selected portfolio.
    pub capital_occupancy_usd_hours: UsdHours,
    /// Hard-reserved cash allocated per category.
    pub category_allocation: BTreeMap<MarketCategory, Usd>,
    /// Hard-reserved cash allocated per event.
    pub event_allocation: BTreeMap<EventId, Usd>,
    /// Hard-reserved cash allocated per model Route.
    pub route_allocation: BTreeMap<BuyModelRoute, Usd>,
    /// Data-quality summary.
    pub data_quality_summary: DataQualitySummary,
    /// Most common rejection reasons.
    pub top_rejection_reasons: Vec<RejectionReasonCount>,
    /// Execution-eligibility roll-up.
    pub execution_eligibility_summary: EligibilitySummary,
    /// Reason the report is empty, when `published_recommendation_count == 0`.
    pub empty_reason: Option<EmptyReportReason>,
    /// Report-level warnings.
    pub warnings: Vec<String>,
}

/// Confidence distribution across published recommendations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ConfidenceSummary {
    /// Mean confidence.
    pub mean_confidence: Probability,
    /// Minimum confidence.
    pub min_confidence: Probability,
    /// Maximum confidence.
    pub max_confidence: Probability,
}

/// Counts of candidates by data-quality classification.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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

impl Add for DataQualitySummary {
    type Output = Self;

    #[inline]
    fn add(mut self, rhs: Self) -> Self {
        self += rhs;
        self
    }
}

impl AddAssign for DataQualitySummary {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.fresh_count += rhs.fresh_count;
        self.acceptable_count += rhs.acceptable_count;
        self.degraded_count += rhs.degraded_count;
        self.stale_count += rhs.stale_count;
        self.insufficient_count += rhs.insufficient_count;
    }
}

impl Sum for DataQualitySummary {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), Add::add)
    }
}

/// One rejection-reason tally.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RejectionReasonCount {
    /// Rejection reason label.
    pub reason: PortfolioRejectionReason,
    /// Number of candidates rejected for this reason.
    pub count: u32,
}

/// Stable economic admission or global-optimum exclusion reason.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum PortfolioRejectionReason {
    ScenarioExitCapacity,
    NominalExpectedNetFloor,
    RobustExpectedNetFloor,
    ProfitProbabilityFloor,
    ProbabilityIntervalWidth,
    LiquidityBuffer,
    SingleRecommendationExposure,
    ExistingStructuralConflict,
    NotSelectedByGlobalOptimum,
}

/// Execution-eligibility roll-up across published recommendations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EligibilitySummary {
    pub analysis_only: u32,
    pub operator_approval: u32,
    pub policy_automatic: u32,
}

impl Add for EligibilitySummary {
    type Output = Self;

    #[inline]
    fn add(mut self, rhs: Self) -> Self {
        self += rhs;
        self
    }
}

impl AddAssign for EligibilitySummary {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        self.analysis_only += rhs.analysis_only;
        self.operator_approval += rhs.operator_approval;
        self.policy_automatic += rhs.policy_automatic;
    }
}

impl Sum for EligibilitySummary {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::default(), Add::add)
    }
}
