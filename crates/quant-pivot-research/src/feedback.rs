//! Versioned, deterministic feedback coverage and drift methodology.
//!
//! The functions in this module are pure. They consume already-frozen cohort
//! counts or immutable artifact rows and return the exact statistics persisted
//! by the feedback stage. Deterministic feature parity is deliberately absent:
//! parity is a serving-contract latch, not a statistical drift input.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::quant::{FeedbackCohortWindow, FeedbackCycleKey},
    enums::{
        feature::FeatureValueKind,
        quant::{DatasetPurpose, FeedbackDriftAssessment, FeedbackDriftKind, FeedbackDriftMetric},
    },
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, CapabilityRegistryHashes, ContentHash, DatasetCohortCounts, FeatureValue,
        FeedbackCoverageArtifactId, FeedbackCycleId, FeedbackDriftArtifactId,
        ModelLearningCohortRow, ModelVersionId, RecommendationId, ResearchFeedbackPolicy,
        ResearchProfileRef, TrainingDatasetId, stable_name::FeatureName,
    },
};
use rust_decimal::{Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};

use crate::{
    precision::RESEARCH_DECIMAL_SCALE,
    stats::{spearman, variance},
    training::{TOKEN_PAYOUT_RATIO, TrainingExample},
};

pub const FEEDBACK_COVERAGE_ARTIFACT_FORMAT_VERSION: u32 = 2;
pub const FEEDBACK_DRIFT_ARTIFACT_FORMAT_VERSION: u32 = 1;

const POPULATION_BIN_COUNT: usize = 10;
const POPULATION_SMOOTHING: Decimal = Decimal::from_parts(5, 0, 0, false, 1);
const KS_SERIES_TERMS: u32 = 100;
const SERIES_EPSILON: Decimal = Decimal::from_parts(1, 0, 0, false, 18);
// exp(-42) is below SERIES_EPSILON. Later terms have strictly more-negative
// exponents, so truncating here is the governed series tolerance, not an error.
const KS_EXP_CUTOFF: Decimal = Decimal::from_parts(42, 0, 0, true, 0);
const COVERAGE_SCHEMA_DOMAIN: &str = "quant-pivot/feedback-coverage-schema";
const DRIFT_SCHEMA_DOMAIN: &str = "quant-pivot/feedback-drift-schema";

/// Stable business reason for stopping after frozen coverage evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageNoActionReason {
    NoModelLearningCandidates,
    InsufficientMatureLabels,
    InsufficientNewMatureLabels,
    InsufficientCoverage,
}

impl CoverageNoActionReason {
    /// Stable terminal reason code persisted by the coordinator.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoModelLearningCandidates => "feedback_no_model_learning_candidates",
            Self::InsufficientMatureLabels => "feedback_insufficient_mature_labels",
            Self::InsufficientNewMatureLabels => "feedback_insufficient_new_mature_labels",
            Self::InsufficientCoverage => "feedback_insufficient_coverage",
        }
    }
}

/// Complete count and threshold preimage for the coverage gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoverageGateInput {
    /// Every recommendation classified by the frozen `ModelLearning` cohort.
    /// Policy-evaluation-only economic outcomes never enter this denominator.
    pub model_learning_candidate_count: u64,
    pub mature_label_count: u64,
    pub new_mature_label_count: u64,
    pub minimum_mature_labels: u64,
    pub minimum_new_mature_labels: u64,
    pub minimum_coverage: Decimal,
}

/// Deterministic coverage transition, including the exact observed ratio.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum CoverageGateOutcome {
    Advance {
        coverage: Decimal,
    },
    NoAction {
        reason: CoverageNoActionReason,
        coverage: Decimal,
    },
}

impl CoverageGateInput {
    /// Reconcile the frozen counts and evaluate thresholds in stable order.
    pub fn evaluate(self) -> QuantResult<CoverageGateOutcome> {
        if self.minimum_mature_labels == 0
            || self.minimum_new_mature_labels == 0
            || self.minimum_new_mature_labels > self.minimum_mature_labels
            || self.minimum_coverage <= Decimal::ZERO
            || self.minimum_coverage > Decimal::ONE
            || self.mature_label_count > self.model_learning_candidate_count
            || self.new_mature_label_count > self.mature_label_count
        {
            return Err(methodology(
                "feedback coverage counts or thresholds do not reconcile",
            ));
        }
        let coverage = if self.model_learning_candidate_count == 0 {
            Decimal::ZERO
        } else {
            (Decimal::from(self.mature_label_count)
                / Decimal::from(self.model_learning_candidate_count))
            .round_dp(RESEARCH_DECIMAL_SCALE)
        };
        let reason = if self.model_learning_candidate_count == 0 {
            Some(CoverageNoActionReason::NoModelLearningCandidates)
        } else if self.mature_label_count < self.minimum_mature_labels {
            Some(CoverageNoActionReason::InsufficientMatureLabels)
        } else if self.new_mature_label_count < self.minimum_new_mature_labels {
            Some(CoverageNoActionReason::InsufficientNewMatureLabels)
        } else if coverage < self.minimum_coverage {
            Some(CoverageNoActionReason::InsufficientCoverage)
        } else {
            None
        };
        Ok(
            reason.map_or(CoverageGateOutcome::Advance { coverage }, |reason| {
                CoverageGateOutcome::NoAction { reason, coverage }
            }),
        )
    }
}

/// One baseline-quantile population bin used by the PSI detail artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PopulationBinKind {
    Missing,
    Continuous { upper_bound: Option<Decimal> },
    Discrete { value: String },
}

/// One canonical population bin used by the PSI detail artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PopulationBin {
    pub kind: PopulationBinKind,
    pub baseline_count: u64,
    pub evaluation_count: u64,
    pub baseline_share: Decimal,
    pub evaluation_share: Decimal,
}

/// Exact summary produced for one numeric feature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NumericDriftSummary {
    pub baseline_count: u64,
    pub evaluation_count: u64,
    pub population_stability_index: Decimal,
    pub kolmogorov_smirnov_statistic: Decimal,
    pub kolmogorov_smirnov_p_value: Decimal,
    pub population_bins: Vec<PopulationBin>,
}

/// Exact concept-drift summary for one champion runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetRankIcDriftSummary {
    pub baseline_target_rank_ic: Decimal,
    pub evaluation_target_rank_ic: Decimal,
    /// Positive deterioration, capped at one to match the governed metric
    /// domain. The uncapped endpoints remain present above.
    pub observed_drop: Decimal,
}

/// Immutable identity and byte commitments of the champion's verified
/// training Dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChampionBaselineRef {
    pub training_dataset_id: TrainingDatasetId,
    pub purpose: DatasetPurpose,
    pub dataset_hash: ContentHash,
    pub manifest_hash: ContentHash,
    pub artifact_bytes_hash: ContentHash,
    pub parquet_uri: ArtifactUri,
    pub feature_schema_hash: ContentHash,
    pub label_schema_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub pit_cutoff: DateTime<Utc>,
    pub sample_count: u64,
}

impl ChampionBaselineRef {
    fn validate(&self) -> QuantResult<()> {
        if self.purpose != DatasetPurpose::Training
            || self.window_start >= self.window_end
            || self.window_end > self.pit_cutoff
            || self.sample_count == 0
        {
            return Err(methodology(
                "champion baseline must be a non-empty Training Dataset with an ordered PIT window",
            ));
        }
        Ok(())
    }
}

/// Minimal immutable mature-label evidence retained for exact coverage
/// reconciliation, including non-champion rows which are not replayed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackMatureLabel {
    pub recommendation_id: RecommendationId,
    pub model_version_id: ModelVersionId,
    pub decision_at: DateTime<Utc>,
    pub candidate_available_at: DateTime<Utc>,
    pub label_available_at: DateTime<Utc>,
    pub outcome_hash: ContentHash,
}

/// Reconciled counts for all three orthogonal feedback cohorts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackCoverageCohorts {
    pub model_learning: DatasetCohortCounts,
    pub execution_learning: DatasetCohortCounts,
    pub policy_evaluation: DatasetCohortCounts,
}

impl FeedbackCoverageCohorts {
    fn validate(&self) -> QuantResult<()> {
        for counts in [
            &self.model_learning,
            &self.execution_learning,
            &self.policy_evaluation,
        ] {
            counts.validate().map_err(|error| {
                methodology(format!("feedback cohort counts do not reconcile: {error}"))
            })?;
        }
        if self.execution_learning.included_count() != self.execution_learning.eligible_count()
            || self.policy_evaluation.included_count() != self.policy_evaluation.eligible_count()
        {
            return Err(methodology(
                "count-only feedback cohorts must include every eligible observation",
            ));
        }
        Ok(())
    }
}

/// Complete immutable coverage artifact. All mature labels are retained for
/// threshold reconciliation; only rows exactly bound to the champion model
/// are materialized for the following drift stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackCoverageArtifact {
    pub format_version: u32,
    pub artifact_id: FeedbackCoverageArtifactId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub cycle_key: FeedbackCycleKey,
    pub profile_ref: ResearchProfileRef,
    pub feedback_policy: ResearchFeedbackPolicy,
    pub feedback_policy_hash: ContentHash,
    pub capability_registry_hashes: CapabilityRegistryHashes,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub evaluation_window: FeedbackCohortWindow,
    pub champion_baseline: ChampionBaselineRef,
    pub cohorts: FeedbackCoverageCohorts,
    pub mature_labels: Vec<FeedbackMatureLabel>,
    pub new_mature_label_count: u64,
    pub gate_input: CoverageGateInput,
    pub gate_outcome: CoverageGateOutcome,
    pub champion_rows: Vec<ModelLearningCohortRow>,
    pub champion_examples: Vec<TrainingExample>,
}

impl FeedbackCoverageArtifact {
    /// Recompute all content-addressed identities, counts, and the coverage
    /// transition. A decoded artifact is never trusted by shape alone.
    pub fn validate(&self) -> QuantResult<()> {
        self.validate_identity()?;
        self.validate_counts()?;
        self.validate_examples()
    }

    fn validate_identity(&self) -> QuantResult<()> {
        self.cycle_key.validate()?;
        self.feedback_policy
            .validate()
            .map_err(|error| methodology(format!("feedback policy is invalid: {error}")))?;
        self.champion_baseline.validate()?;
        let idempotency_hash = self.cycle_key.idempotency_hash()?;
        let policy_hash = self
            .feedback_policy
            .content_hash()
            .map_err(|error| methodology(format!("feedback policy hash failed: {error}")))?;
        let valid = self.format_version == FEEDBACK_COVERAGE_ARTIFACT_FORMAT_VERSION
            && self.cycle_idempotency_hash == idempotency_hash
            && self.feedback_cycle_id == FeedbackCycleId::from_idempotency_hash(&idempotency_hash)
            && self.artifact_id
                == FeedbackCoverageArtifactId::from_cycle_id(self.feedback_cycle_id)
            && self.profile_ref == *self.cycle_key.profile_ref()
            && self.profile_ref == *self.evaluation_window.profile_ref()
            && self.feedback_policy_hash == policy_hash
            && self.feedback_policy_hash == self.cycle_key.feedback_policy_hash()
            && self.champion_model_version_id == self.cycle_key.champion_model_version_id()
            && self.champion_serving_contract_hash
                == self.cycle_key.champion_serving_contract_hash()
            && self.champion_baseline.pit_cutoff <= self.cycle_key.label_cutoff()
            && self.evaluation_window.cutoff() <= self.cycle_key.label_cutoff();
        if !valid {
            return Err(methodology(
                "feedback coverage artifact identity differs from its frozen cycle",
            ));
        }
        Ok(())
    }

    fn validate_counts(&self) -> QuantResult<()> {
        self.cohorts.validate()?;
        let mature_count = exact_count("mature feedback labels", self.mature_labels.len())?;
        let champion_count = exact_count("champion feedback rows", self.champion_rows.len())?;
        if mature_count != self.cohorts.model_learning.eligible_count()
            || champion_count != self.cohorts.model_learning.included_count()
            || self.champion_examples.len() != self.champion_rows.len()
            || self.mature_labels.windows(2).any(|pair| {
                pair[0].recommendation_id == pair[1].recommendation_id
                    || (
                        pair[0].recommendation_id.as_uuid(),
                        pair[0].candidate_available_at,
                        pair[0].label_available_at,
                    ) >= (
                        pair[1].recommendation_id.as_uuid(),
                        pair[1].candidate_available_at,
                        pair[1].label_available_at,
                    )
            })
            || self.mature_labels.iter().any(|label| {
                label.decision_at < self.evaluation_window.window_start()
                    || label.decision_at > self.evaluation_window.cutoff()
                    || label.decision_at > label.candidate_available_at
                    || label.candidate_available_at > label.label_available_at
                    || label.label_available_at > self.cycle_key.label_cutoff()
            })
        {
            return Err(methodology(
                "feedback coverage label or champion-row counts do not reconcile",
            ));
        }
        let new_count = self
            .mature_labels
            .iter()
            .filter(|label| label.label_available_at > self.champion_baseline.pit_cutoff)
            .count();
        if exact_count("new mature feedback labels", new_count)? != self.new_mature_label_count {
            return Err(methodology(
                "new mature-label count does not match the frozen champion cutoff",
            ));
        }
        let expected_input = CoverageGateInput {
            model_learning_candidate_count: self.cohorts.model_learning.candidate_count(),
            mature_label_count: mature_count,
            new_mature_label_count: self.new_mature_label_count,
            minimum_mature_labels: self.feedback_policy.minimum_mature_labels,
            minimum_new_mature_labels: self.feedback_policy.minimum_new_mature_labels,
            minimum_coverage: self.feedback_policy.minimum_coverage,
        };
        if self.gate_input != expected_input || self.gate_outcome != expected_input.evaluate()? {
            return Err(methodology(
                "feedback coverage gate differs from its exact count preimage",
            ));
        }
        Ok(())
    }

    fn validate_examples(&self) -> QuantResult<()> {
        if self
            .champion_rows
            .windows(2)
            .any(|pair| pair[0].example_id.as_uuid() >= pair[1].example_id.as_uuid())
            || self
                .champion_examples
                .windows(2)
                .any(|pair| pair[0].example_id.as_uuid() >= pair[1].example_id.as_uuid())
        {
            return Err(methodology(
                "champion feedback rows must be in unique example-id order",
            ));
        }
        for (row, example) in self.champion_rows.iter().zip(&self.champion_examples) {
            row.validate().map_err(|error| {
                methodology(format!("champion feedback row is invalid: {error}"))
            })?;
            let labels = example
                .labels
                .iter()
                .filter(|label| label.label_name == TOKEN_PAYOUT_RATIO && label.horizon_secs == 0)
                .collect::<Vec<_>>();
            let label = labels
                .first()
                .ok_or_else(|| methodology("champion feedback example has no payout label"))?;
            let mature = self.mature_labels.binary_search_by(|candidate| {
                candidate
                    .recommendation_id
                    .as_uuid()
                    .cmp(&row.recommendation_id.as_uuid())
            });
            let mature = mature.ok().and_then(|index| self.mature_labels.get(index));
            let model_token_matches = row.model_token_id == example.token_id;
            let valid = row.example_id == example.example_id
                && row.market_id == example.market_id
                && model_token_matches
                && row.model_version_id == self.champion_model_version_id
                && row.decision_at >= self.evaluation_window.window_start()
                && row.decision_at <= self.evaluation_window.cutoff()
                && row.candidate_available_at <= self.cycle_key.label_cutoff()
                && row.resolution.available_at <= self.cycle_key.label_cutoff()
                && labels.len() == 1
                && label.is_resolved
                && label.value == row.model_token_payout_ratio.inner()
                && label.matured_at == row.resolution.resolved_at
                && mature.is_some_and(|mature| {
                    mature.model_version_id == row.model_version_id
                        && mature.decision_at == row.decision_at
                        && mature.candidate_available_at == row.candidate_available_at
                        && mature.label_available_at == row.resolution.available_at
                        && mature.outcome_hash == row.resolution.outcome_hash
                });
            if !valid {
                return Err(methodology(
                    "champion feedback example differs from its sealed cohort row",
                ));
            }
        }
        Ok(())
    }
}

/// Canonical JSON codec for one frozen coverage artifact.
pub struct FeedbackCoverageCodec;

impl FeedbackCoverageCodec {
    pub fn encode(artifact: &FeedbackCoverageArtifact) -> QuantResult<Vec<u8>> {
        artifact.validate()?;
        CanonicalDigest::canonical_json_bytes(artifact).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> QuantResult<FeedbackCoverageArtifact> {
        let artifact =
            serde_json::from_slice::<FeedbackCoverageArtifact>(bytes).map_err(|error| {
                ResearchError::Serialization {
                    detail: format!("decode feedback coverage artifact: {error}"),
                }
            })?;
        artifact.validate()?;
        if Self::encode(&artifact)? != bytes {
            return Err(ResearchError::Serialization {
                detail: "feedback coverage artifact is not canonical JSON".to_owned(),
            }
            .into());
        }
        Ok(artifact)
    }

    #[must_use]
    pub fn bytes_hash(bytes: &[u8]) -> ContentHash {
        CanonicalDigest::content_hash_bytes(bytes)
    }

    pub fn schema_hash() -> QuantResult<ContentHash> {
        CanonicalDigest::content_hash_typed(
            COVERAGE_SCHEMA_DOMAIN,
            FEEDBACK_COVERAGE_ARTIFACT_FORMAT_VERSION,
            &[
                "cycle_key",
                "feedback_policy",
                "evaluation_window",
                "champion_baseline",
                "cohorts",
                "mature_labels",
                "gate",
                "champion_rows",
                "champion_examples",
            ],
        )
        .map_err(Into::into)
    }
}

/// One per-feature data-drift detail retained outside `PostgreSQL`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureDriftDetail {
    pub feature_name: FeatureName,
    pub value_kind: Option<FeatureValueKind>,
    pub baseline_total: u64,
    pub baseline_observed: u64,
    pub evaluation_total: u64,
    pub evaluation_observed: u64,
    pub population_stability_index: Option<Decimal>,
    pub kolmogorov_smirnov_statistic: Option<Decimal>,
    pub kolmogorov_smirnov_p_value: Option<Decimal>,
    pub population_bins: Vec<PopulationBin>,
}

impl FeatureDriftDetail {
    /// Compute one feature's population drift over the complete baseline and
    /// evaluation populations. Missing cells are an explicit PSI bin; KS is
    /// emitted only for continuous observed values.
    pub fn compute(
        feature_name: FeatureName,
        baseline: &[Option<FeatureValue>],
        evaluation: &[Option<FeatureValue>],
    ) -> QuantResult<Self> {
        if baseline.is_empty() || evaluation.is_empty() {
            return Err(methodology(
                "feature drift populations must both be non-empty",
            ));
        }
        let value_kind = one_value_kind(baseline, evaluation)?;
        let baseline_observed = baseline.iter().filter(|value| value.is_some()).count();
        let evaluation_observed = evaluation.iter().filter(|value| value.is_some()).count();
        let bins = match value_kind {
            Some(kind) if is_continuous(kind) => {
                continuous_feature_bins(baseline, evaluation, kind)?
            }
            Some(kind) => discrete_feature_bins(baseline, evaluation, kind)?,
            None => Vec::new(),
        };
        let population_stability_index = if bins.is_empty() {
            None
        } else {
            Some(population_stability(&bins)?)
        };
        let (kolmogorov_smirnov_statistic, kolmogorov_smirnov_p_value) =
            if value_kind.is_some_and(is_continuous) {
                let baseline = continuous_values(baseline)?;
                let evaluation = continuous_values(evaluation)?;
                if baseline.len() < 2 || evaluation.len() < 2 {
                    (None, None)
                } else {
                    let statistic = ks_statistic(&baseline, &evaluation)?;
                    let p_value = ks_asymptotic_p(statistic, baseline.len(), evaluation.len())?;
                    (Some(statistic), Some(p_value))
                }
            } else {
                (None, None)
            };
        Ok(Self {
            feature_name,
            value_kind,
            baseline_total: exact_count("feature baseline", baseline.len())?,
            baseline_observed: exact_count("observed feature baseline", baseline_observed)?,
            evaluation_total: exact_count("feature evaluation", evaluation.len())?,
            evaluation_observed: exact_count("observed feature evaluation", evaluation_observed)?,
            population_stability_index,
            kolmogorov_smirnov_statistic,
            kolmogorov_smirnov_p_value,
            population_bins: bins,
        })
    }

    fn validate(&self) -> QuantResult<()> {
        if self.baseline_total == 0
            || self.evaluation_total == 0
            || self.baseline_observed > self.baseline_total
            || self.evaluation_observed > self.evaluation_total
        {
            return Err(methodology(
                "per-feature drift populations are empty or do not reconcile",
            ));
        }
        let Some(kind) = self.value_kind else {
            if self.baseline_observed != 0
                || self.evaluation_observed != 0
                || !self.population_bins.is_empty()
                || self.population_stability_index.is_some()
                || self.kolmogorov_smirnov_statistic.is_some()
                || self.kolmogorov_smirnov_p_value.is_some()
            {
                return Err(methodology(
                    "all-missing feature drift must retain only typed insufficient evidence",
                ));
            }
            return Ok(());
        };
        self.validate_population(kind)?;
        self.validate_ks(kind)
    }

    fn validate_population(&self, kind: FeatureValueKind) -> QuantResult<()> {
        if self.population_bins.is_empty() {
            return Err(methodology(
                "observed feature drift must retain population bins",
            ));
        }
        let baseline_count = population_count(&self.population_bins, true)?;
        let evaluation_count = population_count(&self.population_bins, false)?;
        if baseline_count != self.baseline_total || evaluation_count != self.evaluation_total {
            return Err(methodology(
                "feature population-bin counts do not match their totals",
            ));
        }
        validate_bin_kinds(
            &self.population_bins,
            kind,
            self.baseline_total - self.baseline_observed,
            self.evaluation_total - self.evaluation_observed,
        )?;
        validate_bin_shares(
            &self.population_bins,
            self.baseline_total,
            self.evaluation_total,
        )?;
        if self.population_stability_index != Some(population_stability(&self.population_bins)?) {
            return Err(methodology(
                "feature PSI does not reproduce from its population bins",
            ));
        }
        Ok(())
    }

    fn validate_ks(&self, kind: FeatureValueKind) -> QuantResult<()> {
        let statistic = self.kolmogorov_smirnov_statistic;
        let p_value = self.kolmogorov_smirnov_p_value;
        let has_enough = self.baseline_observed >= 2 && self.evaluation_observed >= 2;
        if !is_continuous(kind) {
            if statistic.is_some() || p_value.is_some() {
                return Err(methodology(
                    "discrete feature drift cannot carry a KS observation",
                ));
            }
            return Ok(());
        }
        if statistic.is_some() != p_value.is_some()
            || has_enough != statistic.is_some()
            || statistic.is_some_and(|value| !(Decimal::ZERO..=Decimal::ONE).contains(&value))
            || p_value.is_some_and(|value| !(Decimal::ZERO..=Decimal::ONE).contains(&value))
        {
            return Err(methodology(
                "continuous feature KS evidence does not reconcile",
            ));
        }
        Ok(())
    }
}

/// Champion prediction/label cardinalities and rank-IC endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConceptDriftDetail {
    pub baseline_scored_count: u64,
    pub evaluation_scored_count: u64,
    pub summary: Option<TargetRankIcDriftSummary>,
}

impl ConceptDriftDetail {
    fn validate(&self) -> QuantResult<()> {
        let Some(summary) = self.summary else {
            return Ok(());
        };
        let valid_range = -Decimal::ONE..=Decimal::ONE;
        let expected_drop = (summary.baseline_target_rank_ic - summary.evaluation_target_rank_ic)
            .clamp(Decimal::ZERO, Decimal::ONE)
            .round_dp(RESEARCH_DECIMAL_SCALE);
        if self.baseline_scored_count < 2
            || self.evaluation_scored_count < 2
            || !valid_range.contains(&summary.baseline_target_rank_ic)
            || !valid_range.contains(&summary.evaluation_target_rank_ic)
            || summary.observed_drop != expected_drop
        {
            return Err(methodology("champion rank-IC detail does not reconcile"));
        }
        Ok(())
    }
}

/// Fixed 0.1-wide payout histogram and its base-2 JS divergence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelDriftDetail {
    pub baseline_counts: Vec<u64>,
    pub evaluation_counts: Vec<u64>,
    pub divergence: Option<Decimal>,
}

impl LabelDriftDetail {
    fn validate(&self) -> QuantResult<()> {
        if self.baseline_counts.len() != 11
            || self.evaluation_counts.len() != 11
            || self.divergence != jensen_shannon(&self.baseline_counts, &self.evaluation_counts)?
        {
            return Err(methodology(
                "label histogram or Jensen-Shannon detail does not reproduce",
            ));
        }
        Ok(())
    }
}

/// One `PostgreSQL` drift-header preimage. The object-store artifact owns full
/// detail; exactly four of these aggregate observations are sealed as headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriftObservation {
    pub kind: FeedbackDriftKind,
    pub metric: FeedbackDriftMetric,
    pub assessment: FeedbackDriftAssessment,
    pub observed_value: Option<Decimal>,
    pub threshold: Decimal,
    pub sample_count: u64,
}

impl DriftObservation {
    /// Seal one aggregate metric against its governed threshold.
    pub fn try_new(
        metric: FeedbackDriftMetric,
        observed_value: Option<Decimal>,
        threshold: Decimal,
        sample_count: u64,
    ) -> QuantResult<Self> {
        if threshold <= Decimal::ZERO
            || metric.is_unit_interval() && threshold > Decimal::ONE
            || observed_value.is_some() && sample_count == 0
        {
            return Err(methodology(
                "feedback drift threshold or sample count is invalid",
            ));
        }
        Ok(Self {
            kind: metric.kind(),
            metric,
            assessment: drift_assessment(metric, observed_value, threshold)?,
            observed_value,
            threshold,
            sample_count,
        })
    }
}

/// Stable business reason for stopping after drift evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftNoActionReason {
    InsufficientEvidence,
    NoThresholdExceeded,
}

impl DriftNoActionReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InsufficientEvidence => "feedback_drift_insufficient_evidence",
            Self::NoThresholdExceeded => "feedback_drift_not_detected",
        }
    }
}

/// Deterministic transition produced from the four aggregate drift metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum DriftGateOutcome {
    Advance {
        exceeded_metrics: Vec<FeedbackDriftMetric>,
    },
    NoAction {
        reason: DriftNoActionReason,
    },
}

/// Complete immutable drift artifact, linked to one accepted coverage object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackDriftArtifact {
    pub format_version: u32,
    pub artifact_id: FeedbackDriftArtifactId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub coverage_artifact_id: FeedbackCoverageArtifactId,
    pub coverage_artifact_uri: ArtifactUri,
    pub coverage_artifact_hash: ContentHash,
    pub profile_ref: ResearchProfileRef,
    pub feedback_policy: ResearchFeedbackPolicy,
    pub feedback_policy_hash: ContentHash,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub champion_baseline: ChampionBaselineRef,
    pub evaluation_window: FeedbackCohortWindow,
    /// Present only when the champion baseline ends no later than the frozen
    /// evaluation window begins. An overlap is typed insufficient evidence,
    /// never silently shortened into a different comparison window.
    pub comparison_window_start: Option<DateTime<Utc>>,
    pub data_details: Vec<FeatureDriftDetail>,
    pub concept_detail: ConceptDriftDetail,
    pub label_detail: LabelDriftDetail,
    pub observations: Vec<DriftObservation>,
    pub gate_outcome: DriftGateOutcome,
    pub observed_at: DateTime<Utc>,
}

impl FeedbackDriftArtifact {
    /// Recompute identity, metric ordering, threshold assessments, and the
    /// resulting drift transition.
    pub fn validate(&self) -> QuantResult<()> {
        self.feedback_policy
            .validate()
            .map_err(|error| methodology(format!("feedback policy is invalid: {error}")))?;
        self.champion_baseline.validate()?;
        let policy_hash = self
            .feedback_policy
            .content_hash()
            .map_err(|error| methodology(format!("feedback policy hash failed: {error}")))?;
        let valid = self.format_version == FEEDBACK_DRIFT_ARTIFACT_FORMAT_VERSION
            && self.artifact_id == FeedbackDriftArtifactId::from_cycle_id(self.feedback_cycle_id)
            && self.coverage_artifact_id
                == FeedbackCoverageArtifactId::from_cycle_id(self.feedback_cycle_id)
            && self.profile_ref == *self.evaluation_window.profile_ref()
            && self.feedback_policy_hash == policy_hash
            && self.evaluation_window.cutoff() <= self.observed_at
            && self.comparison_window_start.is_none_or(|start| {
                start == self.evaluation_window.window_start()
                    && self.champion_baseline.window_end <= start
            });
        if !valid {
            return Err(methodology(
                "feedback drift artifact identity, policy, or windows are invalid",
            ));
        }
        self.validate_details()?;
        self.validate_observations()?;
        if self.comparison_window_start.is_none()
            && (!self.data_details.is_empty()
                || self.concept_detail.baseline_scored_count != 0
                || self.concept_detail.evaluation_scored_count != 0
                || self.concept_detail.summary.is_some()
                || self
                    .label_detail
                    .baseline_counts
                    .iter()
                    .any(|count| *count != 0)
                || self
                    .label_detail
                    .evaluation_counts
                    .iter()
                    .any(|count| *count != 0)
                || self.label_detail.divergence.is_some()
                || self.observations.iter().any(|observation| {
                    observation.assessment != FeedbackDriftAssessment::InsufficientEvidence
                }))
        {
            return Err(methodology(
                "overlapping drift windows must retain only typed insufficient evidence",
            ));
        }
        Ok(())
    }

    fn validate_details(&self) -> QuantResult<()> {
        if self
            .data_details
            .windows(2)
            .any(|pair| pair[0].feature_name.as_str() >= pair[1].feature_name.as_str())
        {
            return Err(methodology(
                "feedback drift feature detail is not in canonical order",
            ));
        }
        for detail in &self.data_details {
            detail.validate()?;
        }
        self.concept_detail.validate()?;
        self.label_detail.validate()?;
        Ok(())
    }

    fn validate_observations(&self) -> QuantResult<()> {
        let expected = drift_observations(
            &self.feedback_policy,
            &self.data_details,
            &self.concept_detail,
            &self.label_detail,
        )?;
        if self.observations != expected {
            return Err(methodology(
                "feedback drift observations differ from their detail preimage",
            ));
        }
        if self.gate_outcome != drift_gate(&self.observations) {
            return Err(methodology(
                "feedback drift transition differs from its observations",
            ));
        }
        Ok(())
    }
}

fn drift_assessment(
    metric: FeedbackDriftMetric,
    value: Option<Decimal>,
    threshold: Decimal,
) -> QuantResult<FeedbackDriftAssessment> {
    let Some(value) = value else {
        return Ok(FeedbackDriftAssessment::InsufficientEvidence);
    };
    if value < Decimal::ZERO || metric.is_unit_interval() && value > Decimal::ONE {
        return Err(methodology(
            "feedback drift observation is outside its metric range",
        ));
    }
    let exceeded = match metric {
        FeedbackDriftMetric::KolmogorovSmirnovPValue => value <= threshold,
        FeedbackDriftMetric::PopulationStabilityIndex
        | FeedbackDriftMetric::TargetRankIcDrop
        | FeedbackDriftMetric::JensenShannonDivergence => value >= threshold,
    };
    Ok(if exceeded {
        FeedbackDriftAssessment::ThresholdExceeded
    } else {
        FeedbackDriftAssessment::WithinThreshold
    })
}

/// Derive the fail-closed stage transition from all four aggregate metrics.
#[must_use]
pub fn drift_gate(observations: &[DriftObservation]) -> DriftGateOutcome {
    let exceeded_metrics = observations
        .iter()
        .filter(|observation| observation.assessment == FeedbackDriftAssessment::ThresholdExceeded)
        .map(|observation| observation.metric)
        .collect::<Vec<_>>();
    if !exceeded_metrics.is_empty() {
        DriftGateOutcome::Advance { exceeded_metrics }
    } else if observations
        .iter()
        .any(|observation| observation.assessment == FeedbackDriftAssessment::InsufficientEvidence)
    {
        DriftGateOutcome::NoAction {
            reason: DriftNoActionReason::InsufficientEvidence,
        }
    } else {
        DriftGateOutcome::NoAction {
            reason: DriftNoActionReason::NoThresholdExceeded,
        }
    }
}

/// Reconstruct the exact four aggregate headers from immutable drift detail.
pub fn drift_observations(
    policy: &ResearchFeedbackPolicy,
    data: &[FeatureDriftDetail],
    concept: &ConceptDriftDetail,
    label: &LabelDriftDetail,
) -> QuantResult<Vec<DriftObservation>> {
    let (psi, psi_count) = data
        .iter()
        .filter_map(|detail| {
            detail
                .population_stability_index
                .map(|value| (value, detail.baseline_total.min(detail.evaluation_total)))
        })
        .max_by_key(|(value, _)| *value)
        .map_or((None, 0), |(value, count)| (Some(value), count));
    let (ks, ks_count) = data
        .iter()
        .filter_map(|detail| {
            detail.kolmogorov_smirnov_p_value.map(|value| {
                (
                    value,
                    detail.baseline_observed.min(detail.evaluation_observed),
                )
            })
        })
        .min_by_key(|(value, _)| *value)
        .map_or((None, 0), |(value, count)| (Some(value), count));
    let target_rank_ic = concept.summary.map(|summary| summary.observed_drop);
    let rank_count = concept
        .baseline_scored_count
        .min(concept.evaluation_scored_count);
    let label_count =
        histogram_total(&label.baseline_counts)?.min(histogram_total(&label.evaluation_counts)?);
    Ok(vec![
        DriftObservation::try_new(
            FeedbackDriftMetric::PopulationStabilityIndex,
            psi,
            policy.data_drift_psi_threshold,
            psi_count,
        )?,
        DriftObservation::try_new(
            FeedbackDriftMetric::KolmogorovSmirnovPValue,
            ks,
            policy.data_drift_ks_p_value,
            ks_count,
        )?,
        DriftObservation::try_new(
            FeedbackDriftMetric::TargetRankIcDrop,
            target_rank_ic,
            policy.concept_target_rank_ic_drop,
            rank_count,
        )?,
        DriftObservation::try_new(
            FeedbackDriftMetric::JensenShannonDivergence,
            label.divergence,
            policy.label_js_divergence,
            label_count,
        )?,
    ])
}

/// Canonical JSON codec for one frozen drift artifact.
pub struct FeedbackDriftCodec;

impl FeedbackDriftCodec {
    pub fn encode(artifact: &FeedbackDriftArtifact) -> QuantResult<Vec<u8>> {
        artifact.validate()?;
        CanonicalDigest::canonical_json_bytes(artifact).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> QuantResult<FeedbackDriftArtifact> {
        let artifact = serde_json::from_slice::<FeedbackDriftArtifact>(bytes).map_err(|error| {
            ResearchError::Serialization {
                detail: format!("decode feedback drift artifact: {error}"),
            }
        })?;
        artifact.validate()?;
        if Self::encode(&artifact)? != bytes {
            return Err(ResearchError::Serialization {
                detail: "feedback drift artifact is not canonical JSON".to_owned(),
            }
            .into());
        }
        Ok(artifact)
    }

    #[must_use]
    pub fn bytes_hash(bytes: &[u8]) -> ContentHash {
        CanonicalDigest::content_hash_bytes(bytes)
    }

    pub fn schema_hash() -> QuantResult<ContentHash> {
        CanonicalDigest::content_hash_typed(
            DRIFT_SCHEMA_DOMAIN,
            FEEDBACK_DRIFT_ARTIFACT_FORMAT_VERSION,
            &[
                "coverage_artifact",
                "feedback_policy",
                "champion_baseline",
                "evaluation_window",
                "comparison_window_start",
                "data_details",
                "concept_detail",
                "label_detail",
                "observations",
                "gate_outcome",
            ],
        )
        .map_err(Into::into)
    }
}

/// Compute PSI and the two-sided asymptotic two-sample KS result.
///
/// PSI uses at most ten baseline-quantile bins and a fixed Jeffreys-style
/// `0.5` pseudocount per bin. This smoothing is part of the method contract:
/// it prevents an empty bin from turning a finite population change into an
/// undefined logarithm. KS uses the exact ECDF statistic and a deterministic
/// Kolmogorov asymptotic series. Fewer than two observations in either sample
/// produce typed insufficient evidence (`None`), never a numeric zero.
pub fn numeric_drift(
    baseline: &[Decimal],
    evaluation: &[Decimal],
) -> QuantResult<Option<NumericDriftSummary>> {
    if baseline.len() < 2 || evaluation.len() < 2 {
        return Ok(None);
    }
    let bins = population_bins(baseline, evaluation)?;
    let population_stability_index = population_stability(&bins)?;
    let kolmogorov_smirnov_statistic = ks_statistic(baseline, evaluation)?;
    let kolmogorov_smirnov_p_value = ks_asymptotic_p(
        kolmogorov_smirnov_statistic,
        baseline.len(),
        evaluation.len(),
    )?;
    Ok(Some(NumericDriftSummary {
        baseline_count: exact_count("baseline numeric drift", baseline.len())?,
        evaluation_count: exact_count("evaluation numeric drift", evaluation.len())?,
        population_stability_index,
        kolmogorov_smirnov_statistic,
        kolmogorov_smirnov_p_value,
        population_bins: bins,
    }))
}

/// Compute champion rank-IC deterioration across non-overlapping windows.
///
/// Degenerate score/label support is undefined correlation and therefore
/// returns `None`. It is never represented as a zero drift observation.
pub fn target_rank_ic_drift(
    baseline_scores: &[Decimal],
    baseline_labels: &[Decimal],
    evaluation_scores: &[Decimal],
    evaluation_labels: &[Decimal],
) -> QuantResult<Option<TargetRankIcDriftSummary>> {
    if baseline_scores.len() != baseline_labels.len()
        || evaluation_scores.len() != evaluation_labels.len()
    {
        return Err(methodology(
            "rank-IC score and label cardinalities do not match",
        ));
    }
    if baseline_scores.len() < 2
        || evaluation_scores.len() < 2
        || variance(baseline_scores).is_zero()
        || variance(baseline_labels).is_zero()
        || variance(evaluation_scores).is_zero()
        || variance(evaluation_labels).is_zero()
    {
        return Ok(None);
    }
    let baseline_target_rank_ic =
        spearman(baseline_scores, baseline_labels).round_dp(RESEARCH_DECIMAL_SCALE);
    let evaluation_target_rank_ic =
        spearman(evaluation_scores, evaluation_labels).round_dp(RESEARCH_DECIMAL_SCALE);
    let observed_drop = (baseline_target_rank_ic - evaluation_target_rank_ic)
        .clamp(Decimal::ZERO, Decimal::ONE)
        .round_dp(RESEARCH_DECIMAL_SCALE);
    Ok(Some(TargetRankIcDriftSummary {
        baseline_target_rank_ic,
        evaluation_target_rank_ic,
        observed_drop,
    }))
}

/// Base-2 Jensen–Shannon divergence over aligned histogram counts.
///
/// The base-2 form is bounded by one. Zero-probability terms contribute zero,
/// so no smoothing or synthetic observations are introduced.
pub fn jensen_shannon(
    baseline_counts: &[u64],
    evaluation_counts: &[u64],
) -> QuantResult<Option<Decimal>> {
    if baseline_counts.len() != evaluation_counts.len() || baseline_counts.is_empty() {
        return Err(methodology(
            "Jensen-Shannon histograms must be aligned and non-empty",
        ));
    }
    let baseline_total = histogram_total(baseline_counts)?;
    let evaluation_total = histogram_total(evaluation_counts)?;
    if baseline_total == 0 || evaluation_total == 0 {
        return Ok(None);
    }
    let baseline_total = Decimal::from(baseline_total);
    let evaluation_total = Decimal::from(evaluation_total);
    let two = Decimal::TWO;
    let ln_two = two
        .checked_ln()
        .ok_or_else(|| methodology("base-2 logarithm constant is undefined"))?;
    let mut divergence = Decimal::ZERO;
    for (baseline_count, evaluation_count) in baseline_counts.iter().zip(evaluation_counts) {
        let baseline = Decimal::from(*baseline_count) / baseline_total;
        let evaluation = Decimal::from(*evaluation_count) / evaluation_total;
        let midpoint = (baseline + evaluation) / two;
        if !baseline.is_zero() {
            let ratio = baseline / midpoint;
            divergence += baseline
                * ratio
                    .checked_ln()
                    .ok_or_else(|| methodology("baseline JS ratio is non-positive"))?;
        }
        if !evaluation.is_zero() {
            let ratio = evaluation / midpoint;
            divergence += evaluation
                * ratio
                    .checked_ln()
                    .ok_or_else(|| methodology("evaluation JS ratio is non-positive"))?;
        }
    }
    Ok(Some(
        (divergence / (two * ln_two))
            .clamp(Decimal::ZERO, Decimal::ONE)
            .round_dp(RESEARCH_DECIMAL_SCALE),
    ))
}

fn histogram_total(counts: &[u64]) -> QuantResult<u64> {
    counts.iter().try_fold(0_u64, |total, count| {
        total
            .checked_add(*count)
            .ok_or_else(|| methodology("label histogram count overflowed"))
    })
}

fn population_bins(
    baseline: &[Decimal],
    evaluation: &[Decimal],
) -> QuantResult<Vec<PopulationBin>> {
    let cutpoints = quantile_cutpoints(baseline)?;
    let actual_bins = cutpoints
        .len()
        .checked_add(1)
        .ok_or_else(|| methodology("population bin count overflowed"))?;
    let mut baseline_counts = vec![0_u64; actual_bins];
    let mut evaluation_counts = vec![0_u64; actual_bins];
    count_bins(baseline, &cutpoints, &mut baseline_counts)?;
    count_bins(evaluation, &cutpoints, &mut evaluation_counts)?;
    let raw = baseline_counts
        .into_iter()
        .zip(evaluation_counts)
        .enumerate()
        .map(|(index, (baseline_count, evaluation_count))| {
            (
                PopulationBinKind::Continuous {
                    upper_bound: cutpoints.get(index).copied(),
                },
                baseline_count,
                evaluation_count,
            )
        })
        .collect::<Vec<_>>();
    seal_population_bins(raw, baseline.len(), evaluation.len())
}

fn quantile_cutpoints(baseline: &[Decimal]) -> QuantResult<Vec<Decimal>> {
    let mut sorted = baseline.to_vec();
    sorted.sort_unstable();
    let bin_count = POPULATION_BIN_COUNT.min(sorted.len());
    let mut cutpoints = Vec::with_capacity(bin_count.saturating_sub(1));
    for ordinal in 1..bin_count {
        let rank = ordinal
            .checked_mul(sorted.len())
            .ok_or_else(|| methodology("population quantile rank overflowed"))?
            .div_ceil(bin_count);
        let index = rank.saturating_sub(1);
        let cutpoint = sorted[index];
        if cutpoints.last().is_none_or(|last| *last != cutpoint) {
            cutpoints.push(cutpoint);
        }
    }
    Ok(cutpoints)
}

fn seal_population_bins(
    raw: Vec<(PopulationBinKind, u64, u64)>,
    baseline_total: usize,
    evaluation_total: usize,
) -> QuantResult<Vec<PopulationBin>> {
    if raw.is_empty() {
        return Err(methodology("population drift produced no bins"));
    }
    let bin_scale = Decimal::from(exact_count("population bins", raw.len())?);
    let baseline_denominator = Decimal::from(exact_count("baseline population", baseline_total)?)
        + POPULATION_SMOOTHING * bin_scale;
    let evaluation_denominator =
        Decimal::from(exact_count("evaluation population", evaluation_total)?)
            + POPULATION_SMOOTHING * bin_scale;
    Ok(raw
        .into_iter()
        .map(|(kind, baseline_count, evaluation_count)| PopulationBin {
            kind,
            baseline_count,
            evaluation_count,
            baseline_share: ((Decimal::from(baseline_count) + POPULATION_SMOOTHING)
                / baseline_denominator)
                .round_dp(RESEARCH_DECIMAL_SCALE),
            evaluation_share: ((Decimal::from(evaluation_count) + POPULATION_SMOOTHING)
                / evaluation_denominator)
                .round_dp(RESEARCH_DECIMAL_SCALE),
        })
        .collect())
}

fn population_count(bins: &[PopulationBin], baseline: bool) -> QuantResult<u64> {
    bins.iter().try_fold(0_u64, |total, bin| {
        let count = if baseline {
            bin.baseline_count
        } else {
            bin.evaluation_count
        };
        total
            .checked_add(count)
            .ok_or_else(|| methodology("population-bin count overflowed"))
    })
}

fn validate_bin_kinds(
    bins: &[PopulationBin],
    kind: FeatureValueKind,
    baseline_missing: u64,
    evaluation_missing: u64,
) -> QuantResult<()> {
    let expects_missing = baseline_missing > 0 || evaluation_missing > 0;
    let first_is_missing = bins
        .first()
        .is_some_and(|bin| bin.kind == PopulationBinKind::Missing);
    if expects_missing != first_is_missing
        || first_is_missing
            && bins.first().is_some_and(|bin| {
                bin.baseline_count != baseline_missing || bin.evaluation_count != evaluation_missing
            })
    {
        return Err(methodology(
            "feature population bins lost their exact missing-value bucket",
        ));
    }
    let observed = &bins[usize::from(expects_missing)..];
    if observed.is_empty() {
        return Err(methodology(
            "typed feature population bins contain no observed-value bucket",
        ));
    }
    if is_continuous(kind) {
        let mut previous = None;
        for (index, bin) in observed.iter().enumerate() {
            let PopulationBinKind::Continuous { upper_bound } = &bin.kind else {
                return Err(methodology(
                    "continuous feature population contains a non-continuous bin",
                ));
            };
            let upper_bound = *upper_bound;
            let is_last = index + 1 == observed.len();
            if is_last != upper_bound.is_none()
                || upper_bound
                    .is_some_and(|bound| previous.is_some_and(|previous| previous >= bound))
            {
                return Err(methodology(
                    "continuous feature population cutpoints are not canonical",
                ));
            }
            if upper_bound.is_some() {
                previous = upper_bound;
            }
        }
        return Ok(());
    }
    let mut previous = None;
    for bin in observed {
        let PopulationBinKind::Discrete { value } = &bin.kind else {
            return Err(methodology(
                "discrete feature population contains a non-discrete bin",
            ));
        };
        let decoded = serde_json::from_str::<FeatureValue>(value)
            .map_err(|error| methodology(format!("decode discrete drift bin: {error}")))?;
        let canonical = String::from_utf8(CanonicalDigest::canonical_json_bytes(&decoded)?)
            .map_err(|error| methodology(format!("encode discrete drift bin: {error}")))?;
        if decoded.kind() != kind
            || canonical != *value
            || previous.is_some_and(|previous: &String| previous >= value)
        {
            return Err(methodology(
                "discrete feature population keys are not canonical",
            ));
        }
        previous = Some(value);
    }
    Ok(())
}

fn validate_bin_shares(
    bins: &[PopulationBin],
    baseline_total: u64,
    evaluation_total: u64,
) -> QuantResult<()> {
    let bin_scale = Decimal::from(exact_count("population bins", bins.len())?);
    let baseline_denominator = Decimal::from(baseline_total) + POPULATION_SMOOTHING * bin_scale;
    let evaluation_denominator = Decimal::from(evaluation_total) + POPULATION_SMOOTHING * bin_scale;
    if bins.iter().any(|bin| {
        let baseline_share = ((Decimal::from(bin.baseline_count) + POPULATION_SMOOTHING)
            / baseline_denominator)
            .round_dp(RESEARCH_DECIMAL_SCALE);
        let evaluation_share = ((Decimal::from(bin.evaluation_count) + POPULATION_SMOOTHING)
            / evaluation_denominator)
            .round_dp(RESEARCH_DECIMAL_SCALE);
        bin.baseline_share != baseline_share || bin.evaluation_share != evaluation_share
    }) {
        return Err(methodology(
            "feature population-bin shares do not reproduce",
        ));
    }
    Ok(())
}

fn population_stability(bins: &[PopulationBin]) -> QuantResult<Decimal> {
    Ok(bins
        .iter()
        .map(|bin| {
            let ratio = bin.evaluation_share / bin.baseline_share;
            ratio
                .checked_ln()
                .map(|log_ratio| (bin.evaluation_share - bin.baseline_share) * log_ratio)
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| methodology("PSI smoothing produced a non-positive share"))?
        .into_iter()
        .sum::<Decimal>()
        .max(Decimal::ZERO)
        .round_dp(RESEARCH_DECIMAL_SCALE))
}

fn one_value_kind(
    baseline: &[Option<FeatureValue>],
    evaluation: &[Option<FeatureValue>],
) -> QuantResult<Option<FeatureValueKind>> {
    let mut expected = None;
    for value in baseline.iter().chain(evaluation).flatten() {
        let kind = value.kind();
        if expected.is_some_and(|current| current != kind) {
            return Err(methodology(
                "one feature drift comparison cannot mix value kinds",
            ));
        }
        expected = Some(kind);
    }
    Ok(expected)
}

const fn is_continuous(kind: FeatureValueKind) -> bool {
    matches!(
        kind,
        FeatureValueKind::Decimal
            | FeatureValueKind::Probability
            | FeatureValueKind::Bps
            | FeatureValueKind::Usd
    )
}

fn continuous_values(values: &[Option<FeatureValue>]) -> QuantResult<Vec<Decimal>> {
    values
        .iter()
        .filter_map(Option::as_ref)
        .map(|value| {
            if !is_continuous(value.kind()) {
                return Err(methodology(
                    "continuous drift received a discrete feature value",
                ));
            }
            value.to_fact_decimal()
        })
        .collect()
}

fn continuous_feature_bins(
    baseline: &[Option<FeatureValue>],
    evaluation: &[Option<FeatureValue>],
    kind: FeatureValueKind,
) -> QuantResult<Vec<PopulationBin>> {
    if !is_continuous(kind) {
        return Err(methodology(
            "continuous feature bins require a continuous value kind",
        ));
    }
    let baseline_values = continuous_values(baseline)?;
    let evaluation_values = continuous_values(evaluation)?;
    let cutpoints = quantile_cutpoints(&baseline_values)?;
    let numeric_bins = cutpoints
        .len()
        .checked_add(1)
        .ok_or_else(|| methodology("continuous feature bin count overflowed"))?;
    let mut baseline_counts = vec![0_u64; numeric_bins];
    let mut evaluation_counts = vec![0_u64; numeric_bins];
    count_bins(&baseline_values, &cutpoints, &mut baseline_counts)?;
    count_bins(&evaluation_values, &cutpoints, &mut evaluation_counts)?;
    let baseline_missing = baseline.len().saturating_sub(baseline_values.len());
    let evaluation_missing = evaluation.len().saturating_sub(evaluation_values.len());
    let mut raw = Vec::with_capacity(numeric_bins.saturating_add(1));
    if baseline_missing > 0 || evaluation_missing > 0 {
        raw.push((
            PopulationBinKind::Missing,
            exact_count("missing feature baseline", baseline_missing)?,
            exact_count("missing feature evaluation", evaluation_missing)?,
        ));
    }
    raw.extend(
        baseline_counts
            .into_iter()
            .zip(evaluation_counts)
            .enumerate()
            .map(|(index, (baseline_count, evaluation_count))| {
                (
                    PopulationBinKind::Continuous {
                        upper_bound: cutpoints.get(index).copied(),
                    },
                    baseline_count,
                    evaluation_count,
                )
            }),
    );
    seal_population_bins(raw, baseline.len(), evaluation.len())
}

fn discrete_feature_bins(
    baseline: &[Option<FeatureValue>],
    evaluation: &[Option<FeatureValue>],
    kind: FeatureValueKind,
) -> QuantResult<Vec<PopulationBin>> {
    if is_continuous(kind) {
        return Err(methodology(
            "discrete feature bins cannot receive a continuous value kind",
        ));
    }
    let mut counts = BTreeMap::<String, (u64, u64)>::new();
    for value in baseline.iter().flatten() {
        let key = discrete_key(value, kind)?;
        increment_population_count(&mut counts, key, true)?;
    }
    for value in evaluation.iter().flatten() {
        let key = discrete_key(value, kind)?;
        increment_population_count(&mut counts, key, false)?;
    }
    let baseline_observed = baseline.iter().filter(|value| value.is_some()).count();
    let evaluation_observed = evaluation.iter().filter(|value| value.is_some()).count();
    let baseline_missing = baseline.len().saturating_sub(baseline_observed);
    let evaluation_missing = evaluation.len().saturating_sub(evaluation_observed);
    let mut raw = Vec::with_capacity(counts.len().saturating_add(1));
    if baseline_missing > 0 || evaluation_missing > 0 {
        raw.push((
            PopulationBinKind::Missing,
            exact_count("missing feature baseline", baseline_missing)?,
            exact_count("missing feature evaluation", evaluation_missing)?,
        ));
    }
    raw.extend(
        counts
            .into_iter()
            .map(|(value, (baseline_count, evaluation_count))| {
                (
                    PopulationBinKind::Discrete { value },
                    baseline_count,
                    evaluation_count,
                )
            }),
    );
    seal_population_bins(raw, baseline.len(), evaluation.len())
}

fn discrete_key(value: &FeatureValue, expected: FeatureValueKind) -> QuantResult<String> {
    if value.kind() != expected || is_continuous(expected) {
        return Err(methodology(
            "discrete feature value differs from its frozen kind",
        ));
    }
    String::from_utf8(CanonicalDigest::canonical_json_bytes(value)?).map_err(|error| {
        methodology(format!(
            "canonical discrete feature key is not UTF-8: {error}"
        ))
    })
}

fn increment_population_count(
    counts: &mut BTreeMap<String, (u64, u64)>,
    key: String,
    baseline: bool,
) -> QuantResult<()> {
    let entry = counts.entry(key).or_default();
    let count = if baseline { &mut entry.0 } else { &mut entry.1 };
    *count = count
        .checked_add(1)
        .ok_or_else(|| methodology("discrete feature count overflowed"))?;
    Ok(())
}

fn count_bins(values: &[Decimal], cutpoints: &[Decimal], counts: &mut [u64]) -> QuantResult<()> {
    for value in values {
        let bin = cutpoints.partition_point(|cutpoint| value > cutpoint);
        let count = counts
            .get_mut(bin)
            .ok_or_else(|| methodology("population value resolved outside its bins"))?;
        *count = count
            .checked_add(1)
            .ok_or_else(|| methodology("population bin count overflowed"))?;
    }
    Ok(())
}

fn ks_statistic(baseline: &[Decimal], evaluation: &[Decimal]) -> QuantResult<Decimal> {
    let mut baseline = baseline.to_vec();
    let mut evaluation = evaluation.to_vec();
    baseline.sort_unstable();
    evaluation.sort_unstable();
    let baseline_denominator = Decimal::from(exact_count("KS baseline", baseline.len())?);
    let evaluation_denominator = Decimal::from(exact_count("KS evaluation", evaluation.len())?);
    let mut baseline_index = 0;
    let mut evaluation_index = 0;
    let mut statistic = Decimal::ZERO;
    while baseline_index < baseline.len() || evaluation_index < evaluation.len() {
        let next = match (
            baseline.get(baseline_index),
            evaluation.get(evaluation_index),
        ) {
            (Some(left), Some(right)) => (*left).min(*right),
            (Some(left), None) => *left,
            (None, Some(right)) => *right,
            (None, None) => break,
        };
        while baseline
            .get(baseline_index)
            .is_some_and(|value| *value <= next)
        {
            baseline_index += 1;
        }
        while evaluation
            .get(evaluation_index)
            .is_some_and(|value| *value <= next)
        {
            evaluation_index += 1;
        }
        let baseline_cdf =
            Decimal::from(exact_count("KS baseline rank", baseline_index)?) / baseline_denominator;
        let evaluation_cdf = Decimal::from(exact_count("KS evaluation rank", evaluation_index)?)
            / evaluation_denominator;
        statistic = statistic.max((baseline_cdf - evaluation_cdf).abs());
    }
    Ok(statistic.round_dp(RESEARCH_DECIMAL_SCALE))
}

fn ks_asymptotic_p(statistic: Decimal, left: usize, right: usize) -> QuantResult<Decimal> {
    if statistic.is_zero() {
        return Ok(Decimal::ONE);
    }
    let left = Decimal::from(exact_count("KS left sample", left)?);
    let right = Decimal::from(exact_count("KS right sample", right)?);
    let effective = left * right / (left + right);
    let scale = effective
        .sqrt()
        .ok_or_else(|| methodology("KS effective sample size has no square root"))?;
    let lambda_sq = (scale * statistic) * (scale * statistic);
    let mut series = Decimal::ZERO;
    for ordinal in 1..=KS_SERIES_TERMS {
        let ordinal_decimal = Decimal::from(ordinal);
        let exponent = -Decimal::TWO * ordinal_decimal * ordinal_decimal * lambda_sq;
        if exponent <= KS_EXP_CUTOFF {
            break;
        }
        let term = exponent
            .checked_exp()
            .ok_or_else(|| methodology("KS exponential failed within the supported range"))?;
        if ordinal % 2 == 0 {
            series -= term;
        } else {
            series += term;
        }
        if term < SERIES_EPSILON {
            break;
        }
    }
    Ok((Decimal::TWO * series)
        .clamp(Decimal::ZERO, Decimal::ONE)
        .round_dp(RESEARCH_DECIMAL_SCALE))
}

fn exact_count(field: &'static str, value: usize) -> QuantResult<u64> {
    u64::try_from(value).map_err(|error| {
        methodology(format!(
            "{field} cardinality cannot be represented exactly: {error}"
        ))
    })
}

fn methodology(detail: impl Into<String>) -> QuantError {
    ResearchError::ValidationMethodology {
        detail: detail.into(),
    }
    .into()
}
