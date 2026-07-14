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
        quant::{
            ExitSettlementMode, FillRequirement, PriceComparison, RedeemPolicy, TradePolicyStatus,
        },
    },
    hashing::CanonicalDigest,
    jsonb_active,
    types::{
        Bps, ClockAnchor, ContentHash, ENTRY_CONDITION_MAX_CANDIDATES, ENTRY_CONDITION_MAX_DEPTH,
        ENTRY_CONDITION_MAX_GROUP_CHILDREN, ENTRY_CONDITION_MAX_NODES,
        ENTRY_CONDITION_MIN_GROUP_CHILDREN, FactorDefinitionId, FactorMeasure, Price,
        RuntimeConfigVersionId, TradePolicyArtifactId, TrainingDatasetId, Usd,
    },
};

/// Breaking wire version for the standalone policy artifact family.
pub const TRADE_POLICY_ARTIFACT_FORMAT_VERSION: u32 = 2;

/// Immutable statistical and execution-quality thresholds used for publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyQualityGate {
    pub min_cohort_samples: u64,
    pub min_executable_coverage: Decimal,
    pub min_full_l2_coverage: Decimal,
    pub min_cpcv_paths: u32,
    pub min_deflated_sharpe_ratio: Decimal,
    pub max_probability_of_backtest_overfitting: Decimal,
    pub max_ambiguous_touch_rate: Decimal,
    pub max_depth_failure_rate: Decimal,
    pub min_lower_confidence_utility_bps: Bps,
}

impl TradePolicyQualityGate {
    fn validate(&self) -> Result<(), String> {
        if self.min_cohort_samples == 0 || self.min_cpcv_paths == 0 {
            return Err("trade-policy sample and CPCV path gates must be positive".to_owned());
        }
        for (name, value, allow_zero) in [
            (
                "min_executable_coverage",
                self.min_executable_coverage,
                false,
            ),
            ("min_full_l2_coverage", self.min_full_l2_coverage, false),
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
        if self.min_lower_confidence_utility_bps <= Bps::ZERO {
            return Err("min_lower_confidence_utility_bps must be positive".to_owned());
        }
        Ok(())
    }
}

/// Fully specified, reproducible policy-fit request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyFitContract {
    pub source_dataset_id: TrainingDatasetId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    /// Latest point-in-time instant from which any fit evidence may be consumed.
    pub pit_cutoff: DateTime<Utc>,
    pub embargo_secs: u64,
    pub notional_tiers: Vec<Usd>,
    pub maximum_scale_out_targets: u8,
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
        if self.notional_tiers.is_empty()
            || self.notional_tiers.iter().any(|tier| !tier.is_positive())
        {
            return Err("trade-policy notional tiers must be non-empty and positive".to_owned());
        }
        if self
            .notional_tiers
            .windows(2)
            .any(|tiers| tiers[0] >= tiers[1])
        {
            return Err("trade-policy notional tiers must be strictly increasing".to_owned());
        }
        if self.maximum_scale_out_targets > 3 {
            return Err("trade-policy supports at most three scale-out targets".to_owned());
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
    pub category: MarketCategory,
    pub horizon_secs: u64,
    pub entry_price_min: Price,
    pub entry_price_max: Price,
    pub notional_tier: Usd,
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
    WeatherDailyHighPredicate { max_input_age_ms: u64 },
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
                    | MarketEventTemplate::WeatherDailyHighPredicate {
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
    Passive { post_only: bool },
    Aggressive { fill_requirement: FillRequirement },
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

/// One deterministic parent-cohort step used to shrink a sparse leaf.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyShrinkStep {
    pub parent_cohort_index: u32,
    pub relaxed_dimension: TradePolicyShrinkDimension,
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
    pub settlement_mode: ExitSettlementMode,
    pub redeem_policy: RedeemPolicy,
    pub sample_count: u64,
    pub effective_sample_size: Decimal,
    pub executable_sample_count: u64,
    pub executable_coverage: Decimal,
    pub lower_confidence_utility_bps: Option<Bps>,
    pub shrink_path: Vec<TradePolicyShrinkStep>,
}

/// Validation evidence. Missing evidence is represented by `None`, never a fake zero.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyValidationEvidence {
    pub trial_ledger_hash: Option<ContentHash>,
    pub cpcv_path_count: Option<u32>,
    pub deflated_sharpe_ratio: Option<Decimal>,
    pub probability_of_backtest_overfitting: Option<Decimal>,
    pub effective_sample_size: Option<Decimal>,
    pub ambiguous_touch_rate: Option<Decimal>,
    pub depth_failure_rate: Option<Decimal>,
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
    pub degraded_top_of_book_sample_count: u64,
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
    InsufficientCohortSamples { cohort_index: u32 },
    InsufficientCohortCoverage { cohort_index: u32 },
    MissingUtilityLowerBound { cohort_index: u32 },
    UtilityLowerBoundBelowGate { cohort_index: u32 },
    InvalidParentProvenance { cohort_index: u32 },
}

/// One fit candidate. `Immediate` must exist exactly once and cannot be removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradePolicyConditionCandidate {
    pub candidate_id: String,
    pub condition: EntryConditionTemplate,
}

/// Canonicalize and validate one fit candidate set. Candidate order is not a
/// semantic dimension; ids are unique and exactly one Immediate baseline is
/// mandatory.
pub fn canonicalize_condition_candidates(
    mut candidates: Vec<TradePolicyConditionCandidate>,
) -> Result<Vec<TradePolicyConditionCandidate>, String> {
    if candidates.is_empty() || candidates.len() > ENTRY_CONDITION_MAX_CANDIDATES {
        return Err(format!(
            "condition candidate count must be in 1..={ENTRY_CONDITION_MAX_CANDIDATES}"
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
        match &mut candidate.condition {
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
    }
    if immediate_count != 1 {
        return Err("condition candidates must contain exactly one Immediate baseline".to_owned());
    }
    candidates.sort_by(|left, right| left.candidate_id.cmp(&right.candidate_id));
    Ok(candidates)
}

/// Publication target governed by source-specific shadow evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerticalActivationTarget {
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
    pub pit_cutoff_evidence: Option<TradePolicyPitCutoffEvidence>,
    pub execution_evidence: TradePolicyExecutionEvidence,
    pub condition_candidate_set_hash: ContentHash,
    pub condition_candidates: Vec<TradePolicyConditionCandidate>,
    pub vertical_gate_evidence: Vec<VerticalGateEvidence>,
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
        if let Err(detail) = self.validate_condition_candidates() {
            blockers.push(TradePolicyPublicationBlocker::InvalidConditionCandidates { detail });
        } else {
            match CanonicalDigest::content_hash_json(&self.condition_candidates) {
                Ok(hash) if hash == self.condition_candidate_set_hash => {}
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
        blockers
    }

    fn validate_condition_candidates(&self) -> Result<(), String> {
        let canonical = canonicalize_condition_candidates(self.condition_candidates.clone())?;
        if canonical != self.condition_candidates {
            return Err("condition candidates are not canonical".to_owned());
        }
        Ok(())
    }

    fn required_vertical_gates(&self) -> Vec<VerticalGateKind> {
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
        blockers
    }

    fn cohort_blockers(&self) -> Vec<TradePolicyPublicationBlocker> {
        let mut blockers = Vec::new();
        let gate = &self.fit_contract.quality_gate;
        for (index, cohort) in self.cohorts.iter().enumerate() {
            let Ok(cohort_index) = u32::try_from(index) else {
                continue;
            };
            if cohort.sample_count < gate.min_cohort_samples {
                blockers.push(TradePolicyPublicationBlocker::InsufficientCohortSamples {
                    cohort_index,
                });
            }
            if cohort.executable_coverage < gate.min_executable_coverage {
                blockers.push(TradePolicyPublicationBlocker::InsufficientCohortCoverage {
                    cohort_index,
                });
            }
            match cohort.lower_confidence_utility_bps {
                None => {
                    blockers.push(TradePolicyPublicationBlocker::MissingUtilityLowerBound {
                        cohort_index,
                    });
                }
                Some(value) if value < gate.min_lower_confidence_utility_bps => {
                    blockers.push(TradePolicyPublicationBlocker::UtilityLowerBoundBelowGate {
                        cohort_index,
                    });
                }
                Some(_) => {}
            }
            if cohort
                .shrink_path
                .iter()
                .any(|step| step.parent_cohort_index >= cohort_index)
            {
                blockers
                    .push(TradePolicyPublicationBlocker::InvalidParentProvenance { cohort_index });
            }
        }
        blockers
    }

    #[must_use]
    pub fn is_publishable(&self) -> bool {
        self.publication_blockers().is_empty()
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

jsonb_active!(TradePolicyFitContract, TradePolicyArtifactPayload);

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    use super::{
        TRADE_POLICY_ARTIFACT_FORMAT_VERSION, TradePolicyArtifactPayload,
        TradePolicyExecutionEvidence, TradePolicyFitContract, TradePolicyPublicationBlocker,
        TradePolicyQualityGate, TradePolicyValidationEvidence, VerticalActivationTarget,
    };
    use crate::types::{Bps, ContentHash, RuntimeConfigVersionId, TrainingDatasetId, Usd};

    fn hash(digit: char) -> ContentHash {
        ContentHash::parse(format!("blake3:{}", digit.to_string().repeat(64)))
            .expect("canonical hash")
    }

    fn contract() -> TradePolicyFitContract {
        let fit_window_end = Utc.timestamp_opt(1_700_086_400, 0).single().expect("time");
        TradePolicyFitContract {
            source_dataset_id: TrainingDatasetId::from_v7(),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            fit_window_start: Utc.timestamp_opt(1_700_000_000, 0).single().expect("time"),
            fit_window_end,
            pit_cutoff: fit_window_end,
            embargo_secs: 3_600,
            notional_tiers: vec![Usd::new(dec!(25)), Usd::new(dec!(100))],
            maximum_scale_out_targets: 3,
            quality_gate: TradePolicyQualityGate {
                min_cohort_samples: 100,
                min_executable_coverage: dec!(0.8),
                min_full_l2_coverage: dec!(0.8),
                min_cpcv_paths: 16,
                min_deflated_sharpe_ratio: dec!(0.5),
                max_probability_of_backtest_overfitting: dec!(0.2),
                max_ambiguous_touch_rate: dec!(0.01),
                max_depth_failure_rate: dec!(0.05),
                min_lower_confidence_utility_bps: Bps::new(dec!(1)),
            },
        }
    }

    #[test]
    fn fit_contract_rejects_labels_beyond_pit_cutoff() {
        let mut value = contract();
        value.pit_cutoff = value.fit_window_end - chrono::Duration::seconds(1);
        assert!(value.validate().is_err());
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
            pit_cutoff_evidence: None,
            execution_evidence: TradePolicyExecutionEvidence {
                entry_basis: None,
                exit_basis: None,
                full_l2_sample_count: 0,
                degraded_top_of_book_sample_count: 100,
                full_l2_coverage: None,
                fee_model_hash: None,
                gaps: Vec::new(),
            },
            condition_candidate_set_hash: hash('4'),
            condition_candidates: Vec::new(),
            vertical_gate_evidence: Vec::new(),
            cohorts: Vec::new(),
            validation: TradePolicyValidationEvidence {
                trial_ledger_hash: None,
                cpcv_path_count: None,
                deflated_sharpe_ratio: None,
                probability_of_backtest_overfitting: None,
                effective_sample_size: None,
                ambiguous_touch_rate: None,
                depth_failure_rate: None,
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
