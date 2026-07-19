//! Governed trade-policy artifact contracts.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{
        common::MarketCategory,
        domain::DomainFamily,
        execution::ExitReason,
        quant::{
            ExitSettlementMode, FillRequirement, PriceComparison, RedeemPolicy, TradePolicyStatus,
        },
    },
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, Bps, ClockAnchor, ContentHash, DecisionPolicySnapshotId,
        ENTRY_CONDITION_MAX_DEPTH, ENTRY_CONDITION_MAX_GROUP_CHILDREN, ENTRY_CONDITION_MAX_NODES,
        ENTRY_CONDITION_MIN_GROUP_CHILDREN, FactorDefinitionId, FactorMeasure, ModelVersionId,
        OpportunisticExitPolicy, Price, ResearchEvaluationTrack, ResearchJobId, ResearchProfileRef,
        StructuralVolatilityOosEvidence, TradePolicyArtifactId, TrainingDatasetId, Usd,
        resolve_builtin_research_profile,
    },
};

/// Breaking wire version for the standalone policy artifact family.
pub const TRADE_POLICY_ARTIFACT_FORMAT_VERSION: u32 = 1;
pub const TRADE_POLICY_EVIDENCE_BUNDLE_FORMAT_VERSION: u32 = 1;
pub const TRADE_POLICY_MAX_CANDIDATES: usize = 32;

/// Immutable statistical and execution-quality thresholds used for publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyQualityGate {
    pub min_effective_sample_size: u64,
    pub min_full_l2_coverage: Decimal,
    pub min_common_candidate_support: Decimal,
    pub min_passive_reconciled_trade_coverage: Decimal,
    pub min_fee_catalog_coverage: Decimal,
    pub min_eligible_market_coverage: Decimal,
    pub min_cpcv_paths: u32,
    pub min_deflated_sharpe_ratio: Decimal,
    pub max_probability_of_backtest_overfitting: Decimal,
    pub max_ambiguous_touch_rate: Decimal,
    pub max_depth_failure_rate: Decimal,
    pub min_lower_confidence_utility_bps: Bps,
}

impl TradePolicyQualityGate {
    pub fn validate(&self) -> Result<(), String> {
        if self.min_effective_sample_size < 100 {
            return Err("min_effective_sample_size cannot be lower than 100".to_owned());
        }
        if self.min_cpcv_paths != 21 {
            return Err("min_cpcv_paths must equal the 21 complete N=8,k=3 paths".to_owned());
        }
        for (name, value, allow_zero) in [
            ("min_full_l2_coverage", self.min_full_l2_coverage, false),
            (
                "min_common_candidate_support",
                self.min_common_candidate_support,
                false,
            ),
            (
                "min_passive_reconciled_trade_coverage",
                self.min_passive_reconciled_trade_coverage,
                false,
            ),
            (
                "min_fee_catalog_coverage",
                self.min_fee_catalog_coverage,
                false,
            ),
            (
                "min_eligible_market_coverage",
                self.min_eligible_market_coverage,
                false,
            ),
            (
                "max_probability_of_backtest_overfitting",
                self.max_probability_of_backtest_overfitting,
                true,
            ),
            (
                "max_ambiguous_touch_rate",
                self.max_ambiguous_touch_rate,
                true,
            ),
            ("max_depth_failure_rate", self.max_depth_failure_rate, true),
        ] {
            let lower_ok = allow_zero && value == Decimal::ZERO || value > Decimal::ZERO;
            if !lower_ok || value > Decimal::ONE {
                return Err(format!(
                    "{name} must be in {}",
                    if allow_zero { "[0, 1]" } else { "(0, 1]" }
                ));
            }
        }
        for (name, value) in [
            ("min_full_l2_coverage", self.min_full_l2_coverage),
            (
                "min_common_candidate_support",
                self.min_common_candidate_support,
            ),
            (
                "min_passive_reconciled_trade_coverage",
                self.min_passive_reconciled_trade_coverage,
            ),
            (
                "min_eligible_market_coverage",
                self.min_eligible_market_coverage,
            ),
        ] {
            if value < Decimal::new(95, 2) {
                return Err(format!("{name} cannot be lower than 0.95"));
            }
        }
        if self.min_fee_catalog_coverage != Decimal::ONE {
            return Err("min_fee_catalog_coverage must equal 1".to_owned());
        }
        if self.min_deflated_sharpe_ratio < Decimal::new(95, 2) {
            return Err("min_deflated_sharpe_ratio cannot be lower than 0.95".to_owned());
        }
        if self.max_probability_of_backtest_overfitting > Decimal::new(5, 1) {
            return Err("max_probability_of_backtest_overfitting cannot exceed 0.5".to_owned());
        }
        if self.max_ambiguous_touch_rate > Decimal::new(5, 2)
            || self.max_depth_failure_rate > Decimal::new(5, 2)
        {
            return Err("ambiguity and depth-failure gates cannot exceed 0.05".to_owned());
        }
        if self.min_lower_confidence_utility_bps < Bps::ZERO {
            return Err("min_lower_confidence_utility_bps cannot be negative".to_owned());
        }
        Ok(())
    }
}

/// Fully specified, reproducible policy-fit request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyFitContract {
    pub profile_ref: ResearchProfileRef,
    pub evaluation_track: ResearchEvaluationTrack,
    pub research_program_hash: ContentHash,
    pub source_dataset_id: TrainingDatasetId,
    pub model_version_id: ModelVersionId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    /// Latest point-in-time instant from which any fit evidence may be consumed.
    pub pit_cutoff: DateTime<Utc>,
    pub target_horizon_secs: u64,
    pub cash_budget_tiers: Vec<Usd>,
    pub methodology_hash: ContentHash,
    pub latency_evidence_id: uuid::Uuid,
    pub latency_profile_hash: ContentHash,
    pub quality_gate: TradePolicyQualityGate,
}

impl TradePolicyFitContract {
    pub fn validate(&self) -> Result<(), String> {
        if self.fit_window_start >= self.fit_window_end {
            return Err("trade-policy fit window must be half-open and non-empty".to_owned());
        }
        if self.fit_window_end > self.pit_cutoff {
            return Err("trade-policy fit window must end at or before pit_cutoff".to_owned());
        }
        if self.latency_evidence_id.is_nil() {
            return Err("trade-policy latency evidence identity cannot be nil".to_owned());
        }
        let profile = resolve_builtin_research_profile(&self.profile_ref)?;
        if !profile.spec.permits(self.evaluation_track) {
            return Err(
                "research profile does not permit the requested evaluation track".to_owned(),
            );
        }
        let expected_span = chrono::Duration::days(i64::from(profile.spec.fit_span_days));
        if self.fit_window_end - self.fit_window_start != expected_span {
            return Err("trade-policy fit window does not match the immutable profile".to_owned());
        }
        if self.target_horizon_secs != profile.spec.target_horizon_secs {
            return Err("trade-policy horizon does not match the immutable profile".to_owned());
        }
        if self.cash_budget_tiers != profile.spec.allowed_cash_budget_tiers {
            return Err(
                "trade-policy cash-budget tiers do not match the immutable profile".to_owned(),
            );
        }
        if self.quality_gate != profile.spec.quality_gate {
            return Err("trade-policy quality gate must equal the immutable profile".to_owned());
        }
        self.quality_gate.validate()
    }
}

/// Versioned provenance for one fitted cohort dimension.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyCohortDimension {
    pub methodology_id: String,
    pub methodology_hash: ContentHash,
    pub bucket_id: String,
}

/// Deterministic cohort selector for prediction-market trajectory policies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyCohortKey {
    pub profile_ref: ResearchProfileRef,
    pub category: MarketCategory,
    pub horizon_secs: u64,
    pub entry_price_min: Price,
    pub entry_price_max: Price,
    pub cash_budget_tier: Usd,
    pub liquidity: TradePolicyCohortDimension,
    pub volatility: TradePolicyCohortDimension,
}

/// Entry-condition template expressed relative to the executable entry basis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum EntryConditionTemplate {
    Immediate,
    /// Canonical AST template selected by the research trial ledger. Leaf
    /// bindings are materialized against each recommendation at report time.
    Conditional {
        root: EntryConditionTemplateV1,
        confirmation_ms: u64,
        max_observation_gap_ms: u64,
    },
}

/// Recommendation-relative condition tree used by research.
///
/// It deliberately omits recommendation/token/model/linkage/source clocks;
/// report composition materializes those bindings from the frozen decision capture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum EntryConditionTemplateV1 {
    Price {
        comparison: PriceComparison,
        threshold: Price,
        max_input_age_ms: u64,
    },
    Clock {
        anchor: ClockAnchor,
        offset_ms: i64,
    },
    Factor {
        definition_id: FactorDefinitionId,
        definition_hash: ContentHash,
        measure: FactorMeasure,
        comparison: PriceComparison,
        threshold: Decimal,
        minimum_confidence: Decimal,
        max_input_age_ms: u64,
    },
    MarketEvent {
        event: MarketEventTemplate,
    },
    All {
        children: Vec<Self>,
    },
    Any {
        children: Vec<Self>,
    },
}

/// Closed market-event template set. Exact source and subject fields are
/// always copied from the recommendation's frozen linkage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum MarketEventTemplate {
    CryptoSubjectPredicateEntered { max_input_age_ms: u64 },
    WeatherDailyTemperaturePredicate { max_input_age_ms: u64 },
}

impl EntryConditionTemplateV1 {
    pub fn canonicalized(self) -> Result<Self, String> {
        let value = self.canonicalize_node(1)?;
        let nodes = value.node_count();
        if nodes > ENTRY_CONDITION_MAX_NODES {
            return Err(format!(
                "entry-condition template contains {nodes} nodes; maximum is {ENTRY_CONDITION_MAX_NODES}"
            ));
        }
        Ok(value)
    }

    fn canonicalize_node(self, depth: usize) -> Result<Self, String> {
        if depth > ENTRY_CONDITION_MAX_DEPTH {
            return Err(format!(
                "entry-condition template depth {depth} exceeds {ENTRY_CONDITION_MAX_DEPTH}"
            ));
        }
        match self {
            Self::All { children } => canonicalize_template_group(children, depth, true),
            Self::Any { children } => canonicalize_template_group(children, depth, false),
            Self::Price {
                max_input_age_ms: 0,
                ..
            }
            | Self::MarketEvent {
                event:
                    MarketEventTemplate::CryptoSubjectPredicateEntered {
                        max_input_age_ms: 0,
                    }
                    | MarketEventTemplate::WeatherDailyTemperaturePredicate {
                        max_input_age_ms: 0,
                    },
            } => Err("entry-condition template freshness must be positive".to_owned()),
            Self::Factor {
                definition_id,
                definition_hash,
                measure,
                comparison,
                threshold,
                minimum_confidence,
                max_input_age_ms,
            } => {
                if FactorDefinitionId::from_definition_hash(&definition_hash) != definition_id {
                    return Err("factor template definition id/hash mismatch".to_owned());
                }
                if !(Decimal::ZERO..=Decimal::ONE).contains(&minimum_confidence) {
                    return Err("factor template confidence must be in [0, 1]".to_owned());
                }
                if max_input_age_ms == 0 {
                    return Err("entry-condition template freshness must be positive".to_owned());
                }
                Ok(Self::Factor {
                    definition_id,
                    definition_hash,
                    measure,
                    comparison,
                    threshold,
                    minimum_confidence,
                    max_input_age_ms,
                })
            }
            leaf => Ok(leaf),
        }
    }

    fn node_count(&self) -> usize {
        match self {
            Self::All { children } | Self::Any { children } => {
                1 + children.iter().map(Self::node_count).sum::<usize>()
            }
            Self::Price { .. }
            | Self::Clock { .. }
            | Self::Factor { .. }
            | Self::MarketEvent { .. } => 1,
        }
    }
}

fn canonicalize_template_group(
    children: Vec<EntryConditionTemplateV1>,
    depth: usize,
    all: bool,
) -> Result<EntryConditionTemplateV1, String> {
    let mut flattened = Vec::new();
    for child in children {
        let child = child.canonicalize_node(depth + 1)?;
        match (all, child) {
            (true, EntryConditionTemplateV1::All { children })
            | (false, EntryConditionTemplateV1::Any { children }) => {
                flattened.extend(children);
            }
            (_, child) => flattened.push(child),
        }
    }
    if !(ENTRY_CONDITION_MIN_GROUP_CHILDREN..=ENTRY_CONDITION_MAX_GROUP_CHILDREN)
        .contains(&flattened.len())
    {
        return Err(format!(
            "entry-condition group must contain \
             {ENTRY_CONDITION_MIN_GROUP_CHILDREN}..={ENTRY_CONDITION_MAX_GROUP_CHILDREN} children \
             after flattening"
        ));
    }
    let mut hashed = flattened
        .into_iter()
        .map(|child| {
            CanonicalDigest::content_hash_json(&child)
                .map(|hash| (hash, child))
                .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    hashed.sort_by(|left, right| left.0.as_str().cmp(right.0.as_str()));
    if hashed.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err("entry-condition group contains a duplicate subtree".to_owned());
    }
    let children = hashed.into_iter().map(|(_, child)| child).collect();
    Ok(if all {
        EntryConditionTemplateV1::All { children }
    } else {
        EntryConditionTemplateV1::Any { children }
    })
}

/// Venue execution template selected out of sample for a cohort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum EntryOrderTemplate {
    PassivePostOnly {
        placement: PassivePlacement,
        good_til_secs: u64,
        max_book_age_ms: u64,
    },
    Aggressive {
        fill_requirement: FillRequirement,
        max_slippage_bps: Bps,
        max_book_age_ms: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum PassivePlacement {
    JoinBestBid,
    ImproveBestBidByTicks { ticks: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScaleOutTemplate {
    pub target_id: String,
    pub trigger_return_bps: Bps,
    pub target_cumulative_exit_pct: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrailingStopTemplate {
    pub trail_bps: Bps,
    pub activation_return_bps: Bps,
}

/// One reason-specific FAK liquidation rule. Protective reasons always target
/// all remaining shares; scale-out is governed by its cumulative target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitExecutionTemplate {
    pub reason: ExitReason,
    pub fill_requirement: FillRequirement,
    pub max_attempts: u32,
    pub retry_cadence_ms: u64,
    pub max_slippage_bps: Bps,
    pub residual_share_policy: ResidualSharePolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResidualSharePolicy {
    RetryUntilVertical,
    HoldToSettlement,
    RedeemAfterResolution,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyExitTemplate {
    pub upper_barrier_bps: Bps,
    pub lower_barrier_bps: Bps,
    pub vertical_barrier_secs: u64,
    pub scale_out_targets: Vec<ScaleOutTemplate>,
    pub trailing_stop: Option<TrailingStopTemplate>,
    pub min_score_retention: Decimal,
    pub min_expected_return_bps: Bps,
    pub require_execution_eligibility: bool,
    pub opportunistic_exit: OpportunisticExitPolicy,
    pub settlement_mode: ExitSettlementMode,
    pub redeem_policy: RedeemPolicy,
    pub reason_execution: Vec<ExitExecutionTemplate>,
}

/// One complete candidate. Research never creates an implicit Cartesian
/// product from partial UI parameters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyCandidateSpec {
    pub candidate_id: String,
    pub entry_condition: EntryConditionTemplate,
    pub entry_execution: EntryOrderTemplate,
    pub exit: TradePolicyExitTemplate,
}

/// Inline provenance for parameters borrowed by a sparse serving leaf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyParameterSource {
    pub relaxed_dimensions: Vec<TradePolicyShrinkDimension>,
    pub source_sample_count: u64,
    pub source_effective_sample_size: Decimal,
    pub source_selector_hash: ContentHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyShrinkDimension {
    Category,
    Volatility,
    Liquidity,
    EntryPrice,
}

/// One fitted entry/exit policy cohort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyCohort {
    pub key: TradePolicyCohortKey,
    pub selected_candidate_id: String,
    pub entry_condition: EntryConditionTemplate,
    pub entry_order: EntryOrderTemplate,
    pub max_slippage_bps: Bps,
    pub max_book_age_ms: u64,
    pub upper_barrier_bps: Bps,
    pub lower_barrier_bps: Bps,
    pub vertical_barrier_secs: u64,
    pub scale_out_targets: Vec<ScaleOutTemplate>,
    pub trailing_stop: Option<TrailingStopTemplate>,
    pub min_score_retention: Decimal,
    pub min_expected_return_bps: Bps,
    pub require_execution_eligibility: bool,
    pub opportunistic_exit: OpportunisticExitPolicy,
    pub settlement_mode: ExitSettlementMode,
    pub redeem_policy: RedeemPolicy,
    pub sample_count: u64,
    pub effective_sample_size: Decimal,
    pub executable_sample_count: u64,
    pub executable_coverage: Decimal,
    pub full_l2_coverage: Decimal,
    pub common_candidate_support: Decimal,
    pub passive_reconciled_trade_coverage: Option<Decimal>,
    pub fee_catalog_coverage: Decimal,
    pub cpcv_path_count: u32,
    pub trial_count: u32,
    pub deflated_sharpe_ratio: Decimal,
    pub probability_of_backtest_overfitting: Decimal,
    pub ambiguous_touch_rate: Decimal,
    pub depth_failure_rate: Decimal,
    pub lower_confidence_utility_bps: Option<Bps>,
    pub parameter_source: TradePolicyParameterSource,
}

/// Validation evidence. Missing evidence is represented by `None`, never a fake zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyValidationEvidence {
    pub trial_ledger_cutoff: Option<DateTime<Utc>>,
    pub trial_ledger_hash: Option<ContentHash>,
    pub attempted_candidate_count: Option<u32>,
    pub cpcv_path_count: Option<u32>,
    pub deflated_sharpe_ratio: Option<Decimal>,
    pub probability_of_backtest_overfitting: Option<Decimal>,
    pub effective_sample_size: Option<Decimal>,
    pub ambiguous_touch_rate: Option<Decimal>,
    pub depth_failure_rate: Option<Decimal>,
    pub common_candidate_support: Option<Decimal>,
    pub fee_catalog_coverage: Option<Decimal>,
    pub eligible_market_coverage: Option<Decimal>,
}

/// Typed metrics carried by one successful trial-ledger attempt. Percentages
/// use exact decimals in `[0, 1]`; no JSON number or floating-point money enters
/// the immutable experiment record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyTrialMetrics {
    pub sample_count: u64,
    pub effective_sample_size: Decimal,
    pub net_return_bps: Decimal,
    pub sharpe_ratio: Option<Decimal>,
    pub executable_coverage: Decimal,
    pub full_l2_coverage: Decimal,
    pub fee_catalog_coverage: Decimal,
    pub ambiguous_touch_rate: Decimal,
    pub depth_failure_rate: Decimal,
    pub latency_stress_multiplier: Decimal,
}

impl TradePolicyTrialMetrics {
    pub fn validate(&self) -> Result<(), String> {
        if self.sample_count == 0
            || self.effective_sample_size <= Decimal::ZERO
            || self.effective_sample_size > Decimal::from(self.sample_count)
            || self.latency_stress_multiplier < Decimal::ONE
        {
            return Err("trade-policy trial sample/ESS/latency metrics are invalid".to_owned());
        }
        if [
            self.executable_coverage,
            self.full_l2_coverage,
            self.fee_catalog_coverage,
            self.ambiguous_touch_rate,
            self.depth_failure_rate,
        ]
        .into_iter()
        .any(|value| !unit_interval(value))
        {
            return Err(
                "trade-policy trial coverage/rate metrics must be within [0, 1]".to_owned(),
            );
        }
        Ok(())
    }
}

/// Durable row-level evidence referenced by a compact policy artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyEvidenceBundleRef {
    pub manifest_uri: ArtifactUri,
    pub manifest_hash: ContentHash,
    pub simulator_hash: ContentHash,
    pub replay_kernel_hash: ContentHash,
    pub methodology_hash: ContentHash,
    pub latency_evidence_id: uuid::Uuid,
    pub latency_profile_hash: ContentHash,
    pub catalog_ledger_hash: ContentHash,
    pub source_slice_manifest_hash: ContentHash,
    pub fit_job_id: ResearchJobId,
    pub trial_ledger_hash: ContentHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyEvidenceObjectKind {
    ObservationEligibility,
    Fills,
    CandidateTrials,
    CohortTrials,
    CpcvPaths,
    CoverageGaps,
    StatisticalSummaries,
    VerticalGates,
    StructuralVolatilityOos,
}

impl TradePolicyEvidenceObjectKind {
    pub const REQUIRED: [Self; 9] = [
        Self::ObservationEligibility,
        Self::Fills,
        Self::CandidateTrials,
        Self::CohortTrials,
        Self::CpcvPaths,
        Self::CoverageGaps,
        Self::StatisticalSummaries,
        Self::VerticalGates,
        Self::StructuralVolatilityOos,
    ];
}

/// One immutable Parquet object in a policy evidence bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyEvidenceObjectRef {
    pub kind: TradePolicyEvidenceObjectKind,
    pub uri: ArtifactUri,
    pub byte_hash: ContentHash,
    pub row_chain_hash: ContentHash,
    pub row_count: u64,
}

/// Small canonical JSON manifest whose referenced row-level objects are
/// verified again at Validate and Publish boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyEvidenceBundleManifest {
    pub format_version: u32,
    pub source_dataset_hash: ContentHash,
    pub candidate_set_hash: ContentHash,
    pub simulator_hash: ContentHash,
    pub replay_kernel_hash: ContentHash,
    pub methodology_hash: ContentHash,
    pub latency_evidence_id: uuid::Uuid,
    pub latency_profile_hash: ContentHash,
    pub catalog_ledger_hash: ContentHash,
    pub source_slice_manifest_hash: ContentHash,
    pub fit_job_id: ResearchJobId,
    pub trial_ledger_cutoff: DateTime<Utc>,
    pub trial_ledger_hash: ContentHash,
    pub objects: Vec<TradePolicyEvidenceObjectRef>,
}

impl TradePolicyEvidenceBundleManifest {
    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != TRADE_POLICY_EVIDENCE_BUNDLE_FORMAT_VERSION {
            return Err(format!(
                "unsupported policy evidence bundle format {}",
                self.format_version
            ));
        }
        if self.objects.len() != TradePolicyEvidenceObjectKind::REQUIRED.len() {
            return Err(
                "policy evidence bundle must contain every required object once".to_owned(),
            );
        }
        if self.latency_evidence_id.is_nil() {
            return Err(
                "policy evidence manifest has no signed latency evidence identity".to_owned(),
            );
        }
        let mut prior = None;
        let mut uris = BTreeSet::new();
        for object in &self.objects {
            if prior.is_some_and(|kind| kind >= object.kind) {
                return Err(
                    "policy evidence objects must be unique and sorted by canonical kind"
                        .to_owned(),
                );
            }
            if !uris.insert(object.uri.as_str()) || !object.uri.as_str().ends_with(".parquet") {
                return Err(
                    "policy evidence objects must use unique immutable Parquet URIs".to_owned(),
                );
            }
            if object.kind != TradePolicyEvidenceObjectKind::CoverageGaps && object.row_count == 0 {
                return Err(format!("policy evidence object {:?} is empty", object.kind));
            }
            prior = Some(object.kind);
        }
        if self
            .objects
            .iter()
            .map(|object| object.kind)
            .ne(TradePolicyEvidenceObjectKind::REQUIRED)
        {
            return Err("policy evidence bundle object set is incomplete".to_owned());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutablePriceBasis {
    FullL2Vwap,
}

/// Typed reason why an execution-fidelity input is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyEvidenceGap {
    FullL2EntryUnavailable,
    FullL2ExitUnavailable,
    PitFeeModelUnavailable,
    TrialLedgerUnavailable,
    CpcvUnavailable,
    AmbiguousTouchEvidenceUnavailable,
    DepthFailureEvidenceUnavailable,
}

/// Executability fidelity used while fitting the artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyExecutionEvidence {
    pub entry_basis: Option<ExecutablePriceBasis>,
    pub exit_basis: Option<ExecutablePriceBasis>,
    pub full_l2_sample_count: u64,
    pub full_l2_coverage: Option<Decimal>,
    pub fee_model_hash: Option<ContentHash>,
    pub gaps: Vec<TradePolicyEvidenceGap>,
}

/// Canonical proof of the exact label projection visible at the PIT cutoff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyPitCutoffEvidence {
    pub filtered_sample_count: u64,
    pub labels_matured_by_cutoff: u64,
    pub labels_excluded_after_cutoff: u64,
    pub filtered_sample_hash: ContentHash,
}

/// Authoritative publication denial derived from immutable payload evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TradePolicyPublicationBlocker {
    UnsupportedFormat { actual: u32 },
    ResearchOnlyEvaluationTrack,
    InvalidFitContract { detail: String },
    InvalidConditionCandidates { detail: String },
    ConditionCandidateSetHashMismatch,
    InvalidVerticalGateEvidence { detail: String },
    MissingVerticalGateEvidence { gate: VerticalGateKind },
    VerticalGateFailed { gate: VerticalGateKind },
    MissingPitCutoffEvidence,
    EmptyCohorts,
    MissingFullL2EntryBasis,
    MissingFullL2ExitBasis,
    MissingFullL2Coverage,
    InsufficientFullL2Coverage,
    MissingFeeModel,
    MissingEvidenceBundle,
    EvidenceBundleIdentityMismatch,
    InvalidStructuralVolatilityOos,
    MissingTrialLedger,
    MissingCpcvPathCount,
    InsufficientCpcvPaths,
    MissingDeflatedSharpeRatio,
    DeflatedSharpeRatioBelowGate,
    MissingProbabilityOfBacktestOverfitting,
    ProbabilityOfBacktestOverfittingAboveGate,
    MissingAmbiguousTouchRate,
    AmbiguousTouchRateAboveGate,
    MissingDepthFailureRate,
    DepthFailureRateAboveGate,
    MissingCommonCandidateSupport,
    InsufficientCommonCandidateSupport,
    MissingFeeCatalogCoverage,
    InsufficientFeeCatalogCoverage,
    MissingEligibleMarketCoverage,
    InsufficientEligibleMarketCoverage,
    InsufficientCohortEffectiveSampleSize { cohort_index: u32 },
    InsufficientCohortCoverage { cohort_index: u32 },
    InsufficientCohortFullL2Coverage { cohort_index: u32 },
    InsufficientCohortCommonSupport { cohort_index: u32 },
    InsufficientPassiveTradeCoverage { cohort_index: u32 },
    InsufficientCohortFeeCoverage { cohort_index: u32 },
    InsufficientCohortCpcvPaths { cohort_index: u32 },
    CohortDeflatedSharpeRatioBelowGate { cohort_index: u32 },
    CohortPboAboveGate { cohort_index: u32 },
    CohortAmbiguityAboveGate { cohort_index: u32 },
    CohortDepthFailureAboveGate { cohort_index: u32 },
    MissingUtilityLowerBound { cohort_index: u32 },
    UtilityLowerBoundBelowGate { cohort_index: u32 },
    InvalidParameterSource { cohort_index: u32 },
}

/// Canonicalize and validate one fit candidate set. Candidate order is not a
/// semantic dimension; ids are unique and exactly one Immediate baseline is
/// mandatory.
pub fn canonicalize_policy_candidates(
    mut candidates: Vec<TradePolicyCandidateSpec>,
) -> Result<Vec<TradePolicyCandidateSpec>, String> {
    if candidates.is_empty() || candidates.len() > TRADE_POLICY_MAX_CANDIDATES {
        return Err(format!(
            "policy candidate count must be in 1..={TRADE_POLICY_MAX_CANDIDATES}"
        ));
    }
    let mut ids = BTreeSet::new();
    let mut immediate_count = 0_usize;
    for candidate in &mut candidates {
        let id = candidate.candidate_id.trim();
        if id.is_empty() || id.len() > 128 {
            return Err("condition candidate ids must contain 1..=128 characters".to_owned());
        }
        if !ids.insert(id.to_owned()) {
            return Err(format!("duplicate condition candidate id `{id}`"));
        }
        match &mut candidate.entry_condition {
            EntryConditionTemplate::Immediate => immediate_count += 1,
            EntryConditionTemplate::Conditional {
                root,
                confirmation_ms,
                max_observation_gap_ms,
            } => {
                if *confirmation_ms > 0
                    && (*max_observation_gap_ms == 0 || *max_observation_gap_ms > *confirmation_ms)
                {
                    return Err(
                        "conditional confirmation gap must be within 1..=confirmation_ms"
                            .to_owned(),
                    );
                }
                *root = root.clone().canonicalized()?;
            }
        }
        validate_entry_execution(&candidate.entry_execution)?;
        validate_exit_template(&candidate.exit)?;
    }
    if immediate_count != 1 {
        return Err("policy candidates must contain exactly one Immediate baseline".to_owned());
    }
    candidates.sort_by(|left, right| left.candidate_id.cmp(&right.candidate_id));
    Ok(candidates)
}

fn validate_entry_execution(entry: &EntryOrderTemplate) -> Result<(), String> {
    match entry {
        EntryOrderTemplate::PassivePostOnly {
            placement,
            good_til_secs,
            max_book_age_ms,
        } => {
            if *good_til_secs == 0 || *max_book_age_ms == 0 {
                return Err("passive GTD and book freshness must be positive".to_owned());
            }
            if matches!(
                placement,
                PassivePlacement::ImproveBestBidByTicks { ticks: 0 }
            ) {
                return Err("passive improve ticks must be positive".to_owned());
            }
        }
        EntryOrderTemplate::Aggressive {
            max_slippage_bps,
            max_book_age_ms,
            ..
        } => {
            if *max_slippage_bps < Bps::ZERO || *max_book_age_ms == 0 {
                return Err(
                    "aggressive slippage must be non-negative and freshness positive".to_owned(),
                );
            }
        }
    }
    Ok(())
}

fn validate_exit_template(exit: &TradePolicyExitTemplate) -> Result<(), String> {
    if exit.vertical_barrier_secs == 0
        || exit.scale_out_targets.len() > 3
        || !(Decimal::ZERO..=Decimal::ONE).contains(&exit.min_score_retention)
    {
        return Err("exit vertical/scale-out/retention contract is invalid".to_owned());
    }
    let opportunistic = &exit.opportunistic_exit;
    if opportunistic.min_expected_alpha_bps < Bps::ZERO
        || opportunistic.max_cumulative_exit_pct <= Decimal::ZERO
        || opportunistic.max_cumulative_exit_pct > Decimal::ONE
        || opportunistic.min_incremental_exit_pct <= Decimal::ZERO
        || opportunistic.min_incremental_exit_pct > opportunistic.max_cumulative_exit_pct
    {
        return Err("opportunistic exit policy bounds are invalid".to_owned());
    }
    let mut prior = Decimal::ZERO;
    let mut target_ids = BTreeSet::new();
    for target in &exit.scale_out_targets {
        if target.target_id.trim().is_empty()
            || !target_ids.insert(target.target_id.as_str())
            || target.target_cumulative_exit_pct <= prior
            || target.target_cumulative_exit_pct > Decimal::ONE
        {
            return Err(
                "scale-out targets must be unique and strictly cumulative in (0,1]".to_owned(),
            );
        }
        prior = target.target_cumulative_exit_pct;
    }
    let mut reasons = BTreeSet::new();
    for execution in &exit.reason_execution {
        if !reasons.insert(execution.reason.as_str())
            || execution.fill_requirement != FillRequirement::AllowPartial
            || execution.max_attempts == 0
            || execution.retry_cadence_ms == 0
            || execution.max_slippage_bps < Bps::ZERO
        {
            return Err(
                "exit reason execution must be unique, FAK, bounded and non-negative".to_owned(),
            );
        }
    }
    if reasons.len() != ExitReason::ALL.len()
        || ExitReason::ALL
            .iter()
            .any(|reason| !reasons.contains(reason.as_str()))
    {
        return Err("exit policy must freeze exactly one rule for every exit reason".to_owned());
    }
    Ok(())
}

/// Publication target governed by source-specific shadow evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerticalActivationTarget {
    ResearchOnly,
    SemiAuto,
    AutoExecution,
}

/// Closed set of source-specific activation gates implemented by the two
/// production verticals. The Crypto gate deliberately separates settlement
/// fidelity from the Binance feature/cross-check plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerticalGateKind {
    CryptoChainlinkResolution,
    CryptoBinanceContinuity,
    WeatherNoaaProxy,
}

impl VerticalGateKind {
    #[must_use]
    pub const fn family(self) -> DomainFamily {
        match self {
            Self::CryptoChainlinkResolution | Self::CryptoBinanceContinuity => DomainFamily::Crypto,
            Self::WeatherNoaaProxy => DomainFamily::Weather,
        }
    }
}

/// Immutable source-specific activation evidence. The payload stores facts;
/// threshold evaluation is always recomputed by the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerticalGateEvidence {
    pub gate: VerticalGateKind,
    pub target: VerticalActivationTarget,
    pub methodology_hash: ContentHash,
    pub evidence_window_start: DateTime<Utc>,
    pub evidence_window_end: DateTime<Utc>,
    pub sample_count: u64,
    pub distinct_subject_count: u32,
    pub distinct_local_dates: u32,
    pub availability: Decimal,
    pub agreement_wilson_lower_bound: Decimal,
    pub target_subject_sample_count: Option<u64>,
    pub target_subject_wilson_lower_bound: Option<Decimal>,
    pub unresolved_mismatch_count: u64,
    pub gaps_recovered: bool,
}

impl VerticalGateEvidence {
    #[must_use]
    pub fn passes(&self, target: VerticalActivationTarget) -> bool {
        if self.target != target
            || self.evidence_window_start >= self.evidence_window_end
            || !unit_interval(self.availability)
            || !unit_interval(self.agreement_wilson_lower_bound)
            || self
                .target_subject_wilson_lower_bound
                .is_some_and(|value| !unit_interval(value))
        {
            return false;
        }
        let live_days = (self.evidence_window_end - self.evidence_window_start).num_days();
        match (self.gate, target) {
            (_, VerticalActivationTarget::ResearchOnly) => false,
            (VerticalGateKind::CryptoChainlinkResolution, VerticalActivationTarget::SemiAuto) => {
                live_days >= 14
                    && self.sample_count >= 2_000
                    && self.availability >= Decimal::new(999, 3)
                    && self.unresolved_mismatch_count == 0
                    && self.gaps_recovered
            }
            (
                VerticalGateKind::CryptoChainlinkResolution,
                VerticalActivationTarget::AutoExecution,
            ) => {
                live_days >= 30
                    && self.sample_count >= 10_000
                    && self.availability >= Decimal::new(9_995, 4)
                    && self.unresolved_mismatch_count == 0
                    && self.gaps_recovered
            }
            (VerticalGateKind::CryptoBinanceContinuity, VerticalActivationTarget::SemiAuto) => {
                live_days >= 30
                    && self.sample_count >= 100_000
                    && self.unresolved_mismatch_count == 0
                    && self.gaps_recovered
            }
            (
                VerticalGateKind::CryptoBinanceContinuity,
                VerticalActivationTarget::AutoExecution,
            ) => {
                live_days >= 60
                    && self.sample_count >= 250_000
                    && self.unresolved_mismatch_count == 0
                    && self.gaps_recovered
            }
            (VerticalGateKind::WeatherNoaaProxy, VerticalActivationTarget::SemiAuto) => {
                self.sample_count >= 500
                    && self.distinct_subject_count >= 20
                    && self.distinct_local_dates >= 30
                    && self.agreement_wilson_lower_bound >= Decimal::new(95, 2)
                    && self
                        .target_subject_sample_count
                        .is_some_and(|count| count >= 20)
                    && self
                        .target_subject_wilson_lower_bound
                        .is_some_and(|bound| bound >= Decimal::new(9, 1))
                    && self.availability >= Decimal::new(99, 2)
                    && self.gaps_recovered
            }
            (VerticalGateKind::WeatherNoaaProxy, VerticalActivationTarget::AutoExecution) => {
                self.sample_count >= 2_000
                    && self.distinct_subject_count >= 30
                    && self.distinct_local_dates >= 90
                    && self.agreement_wilson_lower_bound >= Decimal::new(97, 2)
                    && self
                        .target_subject_sample_count
                        .is_some_and(|count| count >= 30)
                    && self
                        .target_subject_wilson_lower_bound
                        .is_some_and(|bound| bound >= Decimal::new(95, 2))
                    && self.availability >= Decimal::new(995, 3)
                    && self.gaps_recovered
            }
        }
    }
}

fn unit_interval(value: Decimal) -> bool {
    value >= Decimal::ZERO && value <= Decimal::ONE
}

/// Immutable content hashed and stored for a governed entry/exit policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct TradePolicyArtifactPayload {
    pub format_version: u32,
    pub activation_target: VerticalActivationTarget,
    pub fit_contract: TradePolicyFitContract,
    pub source_dataset_hash: ContentHash,
    pub feature_schema_hash: ContentHash,
    pub label_schema_hash: ContentHash,
    pub fill_simulator_version: String,
    /// Server-derived `max(2% fit span, maximum feature lookback)`.
    pub embargo_secs: u64,
    pub pit_cutoff_evidence: Option<TradePolicyPitCutoffEvidence>,
    pub execution_evidence: TradePolicyExecutionEvidence,
    pub candidate_set_hash: ContentHash,
    pub candidates: Vec<TradePolicyCandidateSpec>,
    pub evidence_bundle: Option<TradePolicyEvidenceBundleRef>,
    pub vertical_gate_evidence: Vec<VerticalGateEvidence>,
    pub structural_volatility_oos: StructuralVolatilityOosEvidence,
    pub cohorts: Vec<TradePolicyCohort>,
    pub validation: TradePolicyValidationEvidence,
}

impl TradePolicyArtifactPayload {
    /// Derive every hard publication blocker from immutable evidence and gates.
    #[must_use]
    pub fn publication_blockers(&self) -> Vec<TradePolicyPublicationBlocker> {
        let mut blockers = self.contract_and_execution_blockers();
        blockers.extend(self.validation_blockers());
        blockers.extend(self.cohort_blockers());
        blockers
    }

    fn contract_and_execution_blockers(&self) -> Vec<TradePolicyPublicationBlocker> {
        let mut blockers = Vec::new();
        if self.format_version != TRADE_POLICY_ARTIFACT_FORMAT_VERSION {
            blockers.push(TradePolicyPublicationBlocker::UnsupportedFormat {
                actual: self.format_version,
            });
        }
        if let Err(detail) = self.fit_contract.validate() {
            blockers.push(TradePolicyPublicationBlocker::InvalidFitContract { detail });
        }
        if self.fit_contract.evaluation_track == ResearchEvaluationTrack::ResearchOnly {
            blockers.push(TradePolicyPublicationBlocker::ResearchOnlyEvaluationTrack);
        }
        if let Err(detail) = self.validate_candidates() {
            blockers.push(TradePolicyPublicationBlocker::InvalidConditionCandidates { detail });
        } else {
            match CanonicalDigest::content_hash_json(&self.candidates) {
                Ok(hash) if hash == self.candidate_set_hash => {}
                Ok(_) | Err(_) => {
                    blockers.push(TradePolicyPublicationBlocker::ConditionCandidateSetHashMismatch);
                }
            }
        }
        let required_gates = self.required_vertical_gates();
        for evidence in &self.vertical_gate_evidence {
            if evidence.target != self.activation_target || !required_gates.contains(&evidence.gate)
            {
                blockers.push(TradePolicyPublicationBlocker::InvalidVerticalGateEvidence {
                    detail: format!(
                        "unexpected {:?} evidence for {:?} publication",
                        evidence.gate, self.activation_target
                    ),
                });
            }
        }
        for gate in required_gates {
            let mut evidence = self.vertical_gate_evidence.iter().filter(|evidence| {
                evidence.gate == gate && evidence.target == self.activation_target
            });
            match (evidence.next(), evidence.next()) {
                (None, _) => {
                    blockers
                        .push(TradePolicyPublicationBlocker::MissingVerticalGateEvidence { gate });
                }
                (Some(_), Some(_)) => {
                    blockers.push(TradePolicyPublicationBlocker::InvalidVerticalGateEvidence {
                        detail: format!("duplicate {gate:?} evidence"),
                    });
                }
                (Some(evidence), None) if !evidence.passes(self.activation_target) => {
                    blockers.push(TradePolicyPublicationBlocker::VerticalGateFailed { gate });
                }
                (Some(_), None) => {}
            }
        }
        if self.pit_cutoff_evidence.is_none() {
            blockers.push(TradePolicyPublicationBlocker::MissingPitCutoffEvidence);
        }
        match &self.evidence_bundle {
            None => blockers.push(TradePolicyPublicationBlocker::MissingEvidenceBundle),
            Some(bundle)
                if bundle.methodology_hash != self.fit_contract.methodology_hash
                    || bundle.latency_evidence_id != self.fit_contract.latency_evidence_id
                    || bundle.latency_profile_hash != self.fit_contract.latency_profile_hash
                    || self.validation.trial_ledger_hash.as_ref()
                        != Some(&bundle.trial_ledger_hash) =>
            {
                blockers.push(TradePolicyPublicationBlocker::EvidenceBundleIdentityMismatch);
            }
            Some(_) => {}
        }
        if self.cohorts.is_empty() {
            blockers.push(TradePolicyPublicationBlocker::EmptyCohorts);
        }
        if self.execution_evidence.entry_basis != Some(ExecutablePriceBasis::FullL2Vwap) {
            blockers.push(TradePolicyPublicationBlocker::MissingFullL2EntryBasis);
        }
        if self.execution_evidence.exit_basis != Some(ExecutablePriceBasis::FullL2Vwap) {
            blockers.push(TradePolicyPublicationBlocker::MissingFullL2ExitBasis);
        }
        match self.execution_evidence.full_l2_coverage {
            None => blockers.push(TradePolicyPublicationBlocker::MissingFullL2Coverage),
            Some(value) if value < self.fit_contract.quality_gate.min_full_l2_coverage => {
                blockers.push(TradePolicyPublicationBlocker::InsufficientFullL2Coverage);
            }
            Some(_) => {}
        }
        if self.execution_evidence.fee_model_hash.is_none() {
            blockers.push(TradePolicyPublicationBlocker::MissingFeeModel);
        }
        if !self.structural_volatility_oos.valid {
            blockers.push(TradePolicyPublicationBlocker::InvalidStructuralVolatilityOos);
        }
        blockers
    }

    fn validate_candidates(&self) -> Result<(), String> {
        let canonical = canonicalize_policy_candidates(self.candidates.clone())?;
        if canonical != self.candidates {
            return Err("policy candidates are not canonical".to_owned());
        }
        Ok(())
    }

    fn required_vertical_gates(&self) -> Vec<VerticalGateKind> {
        if self.activation_target == VerticalActivationTarget::ResearchOnly {
            return Vec::new();
        }
        let mut families = self
            .cohorts
            .iter()
            .filter_map(|cohort| DomainFamily::for_category(cohort.key.category))
            .collect::<Vec<_>>();
        families.sort();
        families.dedup();
        let mut gates = Vec::new();
        for family in families {
            match family {
                DomainFamily::Crypto => gates.extend([
                    VerticalGateKind::CryptoChainlinkResolution,
                    VerticalGateKind::CryptoBinanceContinuity,
                ]),
                DomainFamily::Weather => gates.push(VerticalGateKind::WeatherNoaaProxy),
            }
        }
        gates
    }

    fn validation_blockers(&self) -> Vec<TradePolicyPublicationBlocker> {
        let mut blockers = Vec::new();
        let validation = &self.validation;
        let gate = &self.fit_contract.quality_gate;
        if validation.trial_ledger_hash.is_none() {
            blockers.push(TradePolicyPublicationBlocker::MissingTrialLedger);
        }
        match validation.cpcv_path_count {
            None => blockers.push(TradePolicyPublicationBlocker::MissingCpcvPathCount),
            Some(value) if value < gate.min_cpcv_paths => {
                blockers.push(TradePolicyPublicationBlocker::InsufficientCpcvPaths);
            }
            Some(_) => {}
        }
        match validation.deflated_sharpe_ratio {
            None => blockers.push(TradePolicyPublicationBlocker::MissingDeflatedSharpeRatio),
            Some(value) if value < gate.min_deflated_sharpe_ratio => {
                blockers.push(TradePolicyPublicationBlocker::DeflatedSharpeRatioBelowGate);
            }
            Some(_) => {}
        }
        match validation.probability_of_backtest_overfitting {
            None => blockers
                .push(TradePolicyPublicationBlocker::MissingProbabilityOfBacktestOverfitting),
            Some(value) if value > gate.max_probability_of_backtest_overfitting => {
                blockers
                    .push(TradePolicyPublicationBlocker::ProbabilityOfBacktestOverfittingAboveGate);
            }
            Some(_) => {}
        }
        match validation.ambiguous_touch_rate {
            None => blockers.push(TradePolicyPublicationBlocker::MissingAmbiguousTouchRate),
            Some(value) if value > gate.max_ambiguous_touch_rate => {
                blockers.push(TradePolicyPublicationBlocker::AmbiguousTouchRateAboveGate);
            }
            Some(_) => {}
        }
        match validation.depth_failure_rate {
            None => blockers.push(TradePolicyPublicationBlocker::MissingDepthFailureRate),
            Some(value) if value > gate.max_depth_failure_rate => {
                blockers.push(TradePolicyPublicationBlocker::DepthFailureRateAboveGate);
            }
            Some(_) => {}
        }
        match validation.common_candidate_support {
            None => blockers.push(TradePolicyPublicationBlocker::MissingCommonCandidateSupport),
            Some(value) if value < gate.min_common_candidate_support => {
                blockers.push(TradePolicyPublicationBlocker::InsufficientCommonCandidateSupport);
            }
            Some(_) => {}
        }
        match validation.fee_catalog_coverage {
            None => blockers.push(TradePolicyPublicationBlocker::MissingFeeCatalogCoverage),
            Some(value) if value < gate.min_fee_catalog_coverage => {
                blockers.push(TradePolicyPublicationBlocker::InsufficientFeeCatalogCoverage);
            }
            Some(_) => {}
        }
        match validation.eligible_market_coverage {
            None => blockers.push(TradePolicyPublicationBlocker::MissingEligibleMarketCoverage),
            Some(value) if value < gate.min_eligible_market_coverage => {
                blockers.push(TradePolicyPublicationBlocker::InsufficientEligibleMarketCoverage);
            }
            Some(_) => {}
        }
        blockers
    }

    fn cohort_blockers(&self) -> Vec<TradePolicyPublicationBlocker> {
        let mut blockers = Vec::new();
        let gate = &self.fit_contract.quality_gate;
        for (index, cohort) in self.cohorts.iter().enumerate() {
            let Ok(cohort_index) = u32::try_from(index) else {
                continue;
            };
            if cohort.effective_sample_size < Decimal::from(gate.min_effective_sample_size) {
                blockers.push(
                    TradePolicyPublicationBlocker::InsufficientCohortEffectiveSampleSize {
                        cohort_index,
                    },
                );
            }
            if cohort.executable_coverage < gate.min_common_candidate_support {
                blockers.push(TradePolicyPublicationBlocker::InsufficientCohortCoverage {
                    cohort_index,
                });
            }
            if cohort.full_l2_coverage < gate.min_full_l2_coverage {
                blockers.push(
                    TradePolicyPublicationBlocker::InsufficientCohortFullL2Coverage {
                        cohort_index,
                    },
                );
            }
            if cohort.common_candidate_support < gate.min_common_candidate_support {
                blockers.push(
                    TradePolicyPublicationBlocker::InsufficientCohortCommonSupport { cohort_index },
                );
            }
            if matches!(
                cohort.entry_order,
                EntryOrderTemplate::PassivePostOnly { .. }
            ) && cohort
                .passive_reconciled_trade_coverage
                .is_none_or(|value| value < gate.min_passive_reconciled_trade_coverage)
            {
                blockers.push(
                    TradePolicyPublicationBlocker::InsufficientPassiveTradeCoverage {
                        cohort_index,
                    },
                );
            }
            if cohort.fee_catalog_coverage < gate.min_fee_catalog_coverage {
                blockers.push(
                    TradePolicyPublicationBlocker::InsufficientCohortFeeCoverage { cohort_index },
                );
            }
            if cohort.cpcv_path_count < gate.min_cpcv_paths {
                blockers.push(TradePolicyPublicationBlocker::InsufficientCohortCpcvPaths {
                    cohort_index,
                });
            }
            if cohort.deflated_sharpe_ratio < gate.min_deflated_sharpe_ratio {
                blockers.push(
                    TradePolicyPublicationBlocker::CohortDeflatedSharpeRatioBelowGate {
                        cohort_index,
                    },
                );
            }
            if cohort.probability_of_backtest_overfitting
                > gate.max_probability_of_backtest_overfitting
            {
                blockers.push(TradePolicyPublicationBlocker::CohortPboAboveGate { cohort_index });
            }
            if cohort.ambiguous_touch_rate > gate.max_ambiguous_touch_rate {
                blockers
                    .push(TradePolicyPublicationBlocker::CohortAmbiguityAboveGate { cohort_index });
            }
            if cohort.depth_failure_rate > gate.max_depth_failure_rate {
                blockers.push(TradePolicyPublicationBlocker::CohortDepthFailureAboveGate {
                    cohort_index,
                });
            }
            match cohort.lower_confidence_utility_bps {
                None => {
                    blockers.push(TradePolicyPublicationBlocker::MissingUtilityLowerBound {
                        cohort_index,
                    });
                }
                Some(value) if value <= gate.min_lower_confidence_utility_bps => {
                    blockers.push(TradePolicyPublicationBlocker::UtilityLowerBoundBelowGate {
                        cohort_index,
                    });
                }
                Some(_) => {}
            }
            if cohort
                .parameter_source
                .relaxed_dimensions
                .windows(2)
                .any(|pair| shrink_rank(pair[0]) >= shrink_rank(pair[1]))
                || cohort.parameter_source.source_effective_sample_size <= Decimal::ZERO
            {
                blockers
                    .push(TradePolicyPublicationBlocker::InvalidParameterSource { cohort_index });
            }
        }
        blockers
    }

    #[must_use]
    pub fn is_publishable(&self) -> bool {
        self.publication_blockers().is_empty()
    }
}

const fn shrink_rank(dimension: TradePolicyShrinkDimension) -> u8 {
    match dimension {
        TradePolicyShrinkDimension::Category => 0,
        TradePolicyShrinkDimension::Volatility => 1,
        TradePolicyShrinkDimension::Liquidity => 2,
        TradePolicyShrinkDimension::EntryPrice => 3,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyArtifact {
    pub artifact_id: TradePolicyArtifactId,
    pub content_hash: ContentHash,
    pub status: TradePolicyStatus,
    pub payload: TradePolicyArtifactPayload,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;

    use super::{
        TRADE_POLICY_ARTIFACT_FORMAT_VERSION, TradePolicyArtifactPayload,
        TradePolicyExecutionEvidence, TradePolicyFitContract, TradePolicyPublicationBlocker,
        TradePolicyValidationEvidence, VerticalActivationTarget,
    };
    use crate::types::{
        ContentHash, DecisionPolicySnapshotId, ModelVersionId, ResearchEvaluationTrack,
        StructuralVolatilityOosEvidence, TrainingDatasetId, builtin_research_profiles,
    };

    fn hash(digit: char) -> ContentHash {
        ContentHash::parse(format!("blake3:{}", digit.to_string().repeat(64)))
            .expect("canonical hash")
    }

    fn contract() -> TradePolicyFitContract {
        let profile = builtin_research_profiles()
            .expect("profiles")
            .into_iter()
            .next()
            .expect("pooled profile");
        let fit_window_end = Utc.timestamp_opt(1_700_086_400, 0).single().expect("time");
        TradePolicyFitContract {
            profile_ref: profile.profile_ref,
            evaluation_track: ResearchEvaluationTrack::ResearchOnly,
            research_program_hash: hash('7'),
            source_dataset_id: TrainingDatasetId::from_v7(),
            model_version_id: ModelVersionId::from_v7(),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            fit_window_start: fit_window_end - chrono::Duration::days(30),
            fit_window_end,
            pit_cutoff: fit_window_end,
            target_horizon_secs: profile.spec.target_horizon_secs,
            cash_budget_tiers: profile.spec.allowed_cash_budget_tiers,
            methodology_hash: hash('8'),
            latency_evidence_id: uuid::Uuid::now_v7(),
            latency_profile_hash: hash('9'),
            quality_gate: profile.spec.quality_gate,
        }
    }

    #[test]
    fn fit_contract_rejects_labels_beyond_pit_cutoff() {
        let mut value = contract();
        value.pit_cutoff = value.fit_window_end - chrono::Duration::seconds(1);
        assert!(value.validate().is_err());
    }

    #[test]
    fn fit_contract_rejects_a_second_quality_gate_truth() {
        let mut value = contract();
        value.quality_gate.min_effective_sample_size += 1;
        assert_eq!(
            value.validate(),
            Err("trade-policy quality gate must equal the immutable profile".to_owned())
        );
    }

    #[test]
    fn absent_evidence_and_empty_cohorts_cannot_self_attest_publication() {
        let payload = TradePolicyArtifactPayload {
            format_version: TRADE_POLICY_ARTIFACT_FORMAT_VERSION,
            activation_target: VerticalActivationTarget::SemiAuto,
            fit_contract: contract(),
            source_dataset_hash: hash('1'),
            feature_schema_hash: hash('2'),
            label_schema_hash: hash('3'),
            fill_simulator_version: "unavailable-v1".to_owned(),
            embargo_secs: 3_600,
            pit_cutoff_evidence: None,
            execution_evidence: TradePolicyExecutionEvidence {
                entry_basis: None,
                exit_basis: None,
                full_l2_sample_count: 0,
                full_l2_coverage: None,
                fee_model_hash: None,
                gaps: Vec::new(),
            },
            candidate_set_hash: hash('4'),
            candidates: Vec::new(),
            evidence_bundle: None,
            vertical_gate_evidence: Vec::new(),
            structural_volatility_oos: StructuralVolatilityOosEvidence {
                methodology_hash: hash('5'),
                active_update_only: true,
                activity_proxy: "unavailable".to_owned(),
                minimum_contract_observations: 48,
                fold_count: 0,
                forecast_count: 0,
                deadline_vw_interval_score: Decimal::ZERO,
                dr_as_vw_interval_score: Decimal::ZERO,
                deadline_volume_weighted_coverage: Decimal::ZERO,
                dr_as_volume_weighted_coverage: Decimal::ZERO,
                valid: false,
            },
            cohorts: Vec::new(),
            validation: TradePolicyValidationEvidence {
                trial_ledger_cutoff: None,
                trial_ledger_hash: None,
                attempted_candidate_count: None,
                cpcv_path_count: None,
                deflated_sharpe_ratio: None,
                probability_of_backtest_overfitting: None,
                effective_sample_size: None,
                ambiguous_touch_rate: None,
                depth_failure_rate: None,
                common_candidate_support: None,
                fee_catalog_coverage: None,
                eligible_market_coverage: None,
            },
        };

        let blockers = payload.publication_blockers();
        assert!(blockers.contains(&TradePolicyPublicationBlocker::EmptyCohorts));
        assert!(blockers.contains(&TradePolicyPublicationBlocker::MissingPitCutoffEvidence));
        assert!(blockers.contains(&TradePolicyPublicationBlocker::MissingFeeModel));
        assert!(blockers.contains(&TradePolicyPublicationBlocker::MissingTrialLedger));
        assert!(!payload.is_publishable());
    }
}
