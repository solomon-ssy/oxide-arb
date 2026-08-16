//! Typed execution boundaries for feedback coverage, drift, and learning jobs.

use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, feedback::FeedbackError};
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    domain::{
        api::{
            CpcvBacktestJobParams, FeedbackCoverageJobParams, FeedbackDriftJobParams,
            ModelTrainJobParams,
        },
        quant::{
            FeedbackCohortWindow, FeedbackEvaluationUseInfo, JobProgressSink,
            ResearchJobArtifactRef,
        },
    },
    enums::{
        common::MarketCategory,
        quant::{CalibrationMethod, DatasetPurpose, DownsideSource, FeedbackStage},
    },
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, BacktestPathSetId, BacktestReportId, Bps, ContentHash, DatasetSourceLineage,
        DecisionPolicySnapshotId, FeedbackComparisonArtifactId, FeedbackCoverageArtifactId,
        FeedbackCycleId, FeedbackDecisionArtifactId, FeedbackDriftArtifactId,
        FeedbackEvaluationUseId, FeedbackLearningStageArtifactId, FeedbackShadowArtifactId,
        ModelRunId, ModelSpecId, ModelVersionId, PolicyBundleGeneration, Probability,
        ResearchFeedbackPolicy, ResearchJobId, ResearchProfileRef, TrainingDatasetId,
    },
};

use super::{
    calibration_artifact::ModelCalibrationFitJobParams,
    feedback_governance::FeedbackValidationArtifactRef,
    feedback_recipe::{FeedbackRecipeCpcvSpec, FeedbackRecipeResourceBudget},
    feedback_shadow_binding::ShadowBindingArtifactRef,
};

const LEARNING_INPUT_VERSION: u32 = 1;
const LEARNING_INPUT_DOMAIN: &str = "quant-pivot/feedback-learning-stage-input";
const CANDIDATE_RECIPE_VERSION: u32 = 3;
const CANDIDATE_RECIPE_DOMAIN: &str = "quant-pivot/feedback-candidate-recipe";
const CANDIDATE_FAMILY_VERSION: u32 = 2;
const CANDIDATE_FAMILY_DOMAIN: &str = "quant-pivot/feedback-candidate-family";
const COMPARISON_CONTRACT_VERSION: u32 = 1;
const COMPARISON_CONTRACT_DOMAIN: &str = "quant-pivot/feedback-comparison-contract";
const COMPARISON_INPUT_VERSION: u32 = 1;
const COMPARISON_INPUT_DOMAIN: &str = "quant-pivot/feedback-comparison-stage-input";
const COMPARISON_EFFECT_PRECISION_DP: u32 = 12;
const SHADOW_CONTRACT_VERSION: u32 = 1;
const SHADOW_CONTRACT_DOMAIN: &str = "quant-pivot/feedback-shadow-contract";
const SHADOW_INPUT_VERSION: u32 = 1;
const SHADOW_INPUT_DOMAIN: &str = "quant-pivot/feedback-shadow-stage-input";
const DRIFT_INPUT_VERSION: u32 = 1;
const DRIFT_INPUT_DOMAIN: &str = "quant-pivot/feedback-drift-stage-input";
const DECISION_INPUT_VERSION: u32 = 1;
const DECISION_INPUT_DOMAIN: &str = "quant-pivot/feedback-decision-stage-input";
/// Structural upper bound for one feedback cycle's frozen candidate family.
///
/// F08 owns the exact family contents; this hard ceiling protects every
/// persisted job/decode boundary before scheduler-level budgets from F12 run.
pub const FEEDBACK_LEARNING_MAX_CANDIDATES: usize = 32;
const FEEDBACK_DATASET_MAX_COMMANDS: usize = FEEDBACK_LEARNING_MAX_CANDIDATES * 2 + 1;

/// Verified object-store result of one coverage job.
pub struct FeedbackCoverageExecutionResult {
    pub artifact_id: FeedbackCoverageArtifactId,
    pub artifact: ResearchJobArtifactRef,
}

/// Verified object-store result of one drift job.
pub struct FeedbackDriftExecutionResult {
    pub artifact_id: FeedbackDriftArtifactId,
    pub artifact: ResearchJobArtifactRef,
}

/// Exact owner of one Dataset emitted by the `DatasetSeal` stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "owner", rename_all = "snake_case")]
pub enum FeedbackDatasetRole {
    CandidateTraining { candidate_recipe_hash: ContentHash },
    CandidateCalibration { candidate_recipe_hash: ContentHash },
    SharedEvaluation,
}

impl FeedbackDatasetRole {
    #[must_use]
    pub const fn purpose(self) -> DatasetPurpose {
        match self {
            Self::CandidateTraining { .. } => DatasetPurpose::Training,
            Self::CandidateCalibration { .. } => DatasetPurpose::Calibration,
            Self::SharedEvaluation => DatasetPurpose::Evaluation,
        }
    }

    #[must_use]
    pub const fn candidate_recipe_hash(self) -> Option<ContentHash> {
        match self {
            Self::CandidateTraining {
                candidate_recipe_hash,
            }
            | Self::CandidateCalibration {
                candidate_recipe_hash,
            } => Some(candidate_recipe_hash),
            Self::SharedEvaluation => None,
        }
    }
}

/// Server-frozen input for one feedback-owned Dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct FeedbackDatasetBuildRequest {
    pub training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub model_spec_definition_hash: ContentHash,
    pub source_lineage: DatasetSourceLineage,
    pub window: FeedbackCohortWindow,
    pub purpose: DatasetPurpose,
}

impl FeedbackDatasetBuildRequest {
    /// Verify that feedback owns the purpose, profile, and PIT cutoff.
    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.source_lineage
            .validate()
            .map_err(|error| FeedbackError::InvalidJobContract {
                detail: format!("invalid feedback Dataset source lineage: {error}"),
            })?;
        if !matches!(
            self.purpose,
            DatasetPurpose::Training | DatasetPurpose::Calibration | DatasetPurpose::Evaluation
        ) || self.source_lineage.pit_cutoff < self.window.cutoff()
            || self.source_lineage.research_profile_artifact_id
                != self.window.profile_ref().artifact_id()
        {
            return Err(FeedbackError::InvalidJobContract {
                detail: "feedback Dataset must bind Training/Calibration/Evaluation purpose, the exact profile, and a source cutoff no earlier than its decision window"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

/// One predeclared candidate recipe frozen before any evaluation metric opens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "FeedbackCandidateRecipeDocument")]
pub struct FeedbackCandidateRecipe {
    format_version: u32,
    candidate_recipe_hash: ContentHash,
    recipe_template_hash: ContentHash,
    planner_evidence_hash: ContentHash,
    resource_budget: FeedbackRecipeResourceBudget,
    training: FeedbackDatasetBuildRequest,
    calibration: FeedbackDatasetBuildRequest,
    calibration_method: CalibrationMethod,
    cpcv_spec: FeedbackRecipeCpcvSpec,
    downside_source: DownsideSource,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
}

/// Complete owned input used to seal one immutable candidate recipe.
pub struct FeedbackCandidateRecipeInput {
    pub recipe_template_hash: ContentHash,
    pub planner_evidence_hash: ContentHash,
    pub resource_budget: FeedbackRecipeResourceBudget,
    pub training: FeedbackDatasetBuildRequest,
    pub calibration: FeedbackDatasetBuildRequest,
    pub calibration_method: CalibrationMethod,
    pub cpcv_spec: FeedbackRecipeCpcvSpec,
    pub downside_source: DownsideSource,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedbackCandidateRecipeDocument {
    format_version: u32,
    candidate_recipe_hash: ContentHash,
    recipe_template_hash: ContentHash,
    planner_evidence_hash: ContentHash,
    resource_budget: FeedbackRecipeResourceBudget,
    training: FeedbackDatasetBuildRequest,
    calibration: FeedbackDatasetBuildRequest,
    calibration_method: CalibrationMethod,
    cpcv_spec: FeedbackRecipeCpcvSpec,
    downside_source: DownsideSource,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
}

#[derive(Serialize)]
struct FeedbackCandidateRecipePreimage<'a> {
    format_version: u32,
    recipe_template_hash: ContentHash,
    planner_evidence_hash: ContentHash,
    resource_budget: FeedbackRecipeResourceBudget,
    training: &'a FeedbackDatasetBuildRequest,
    calibration: &'a FeedbackDatasetBuildRequest,
    calibration_method: CalibrationMethod,
    cpcv_spec: &'a FeedbackRecipeCpcvSpec,
    downside_source: DownsideSource,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
}

impl FeedbackCandidateRecipe {
    pub fn try_seal(input: FeedbackCandidateRecipeInput) -> Result<Self, FeedbackError> {
        let candidate_recipe_hash = Self::derive_hash(&FeedbackCandidateRecipePreimage {
            format_version: CANDIDATE_RECIPE_VERSION,
            recipe_template_hash: input.recipe_template_hash,
            planner_evidence_hash: input.planner_evidence_hash,
            resource_budget: input.resource_budget,
            training: &input.training,
            calibration: &input.calibration,
            calibration_method: input.calibration_method,
            cpcv_spec: &input.cpcv_spec,
            downside_source: input.downside_source,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
        })?;
        let recipe = Self {
            format_version: CANDIDATE_RECIPE_VERSION,
            candidate_recipe_hash,
            recipe_template_hash: input.recipe_template_hash,
            planner_evidence_hash: input.planner_evidence_hash,
            resource_budget: input.resource_budget,
            training: input.training,
            calibration: input.calibration,
            calibration_method: input.calibration_method,
            cpcv_spec: input.cpcv_spec,
            downside_source: input.downside_source,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
        };
        recipe.validate()?;
        Ok(recipe)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.resource_budget.validate()?;
        self.training.validate()?;
        self.calibration.validate()?;
        self.cpcv_spec.validate()?;
        let exact_hash = Self::derive_hash(&FeedbackCandidateRecipePreimage {
            format_version: CANDIDATE_RECIPE_VERSION,
            recipe_template_hash: self.recipe_template_hash,
            planner_evidence_hash: self.planner_evidence_hash,
            resource_budget: self.resource_budget,
            training: &self.training,
            calibration: &self.calibration,
            calibration_method: self.calibration_method,
            cpcv_spec: &self.cpcv_spec,
            downside_source: self.downside_source,
            decision_policy_snapshot_id: self.decision_policy_snapshot_id,
        })?;
        let training_lineage = &self.training.source_lineage;
        let calibration_lineage = &self.calibration.source_lineage;
        if self.format_version != CANDIDATE_RECIPE_VERSION
            || self.candidate_recipe_hash != exact_hash
            || self.training.purpose != DatasetPurpose::Training
            || self.calibration.purpose != DatasetPurpose::Calibration
            || self.training.training_dataset_id == self.calibration.training_dataset_id
            || self.training.model_spec_id != self.calibration.model_spec_id
            || self.training.model_spec_definition_hash
                != self.calibration.model_spec_definition_hash
            || self.training.window.profile_ref() != self.calibration.window.profile_ref()
            || self.training.window.cutoff() >= self.calibration.window.window_start()
            || training_lineage.decision_policy_snapshot_id != self.decision_policy_snapshot_id
            || calibration_lineage.decision_policy_snapshot_id != self.decision_policy_snapshot_id
            || training_lineage.capability_registry_hashes
                != calibration_lineage.capability_registry_hashes
        {
            return Err(invalid_batch(
                "candidate recipe version, hash, Dataset plan, profile, split, or policy is invalid",
            ));
        }
        Ok(())
    }

    fn derive_hash(
        preimage: &FeedbackCandidateRecipePreimage<'_>,
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            CANDIDATE_RECIPE_DOMAIN,
            CANDIDATE_RECIPE_VERSION,
            preimage,
        )
        .map_err(Into::into)
    }

    #[must_use]
    pub const fn candidate_recipe_hash(&self) -> ContentHash {
        self.candidate_recipe_hash
    }

    #[must_use]
    pub const fn recipe_template_hash(&self) -> ContentHash {
        self.recipe_template_hash
    }

    #[must_use]
    pub const fn planner_evidence_hash(&self) -> ContentHash {
        self.planner_evidence_hash
    }

    #[must_use]
    pub const fn resource_budget(&self) -> FeedbackRecipeResourceBudget {
        self.resource_budget
    }

    #[must_use]
    pub const fn training(&self) -> &FeedbackDatasetBuildRequest {
        &self.training
    }

    #[must_use]
    pub const fn calibration(&self) -> &FeedbackDatasetBuildRequest {
        &self.calibration
    }

    #[must_use]
    pub const fn calibration_method(&self) -> CalibrationMethod {
        self.calibration_method
    }

    #[must_use]
    pub const fn cpcv_spec(&self) -> &FeedbackRecipeCpcvSpec {
        &self.cpcv_spec
    }

    #[must_use]
    pub const fn downside_source(&self) -> DownsideSource {
        self.downside_source
    }

    #[must_use]
    pub const fn decision_policy_snapshot_id(&self) -> DecisionPolicySnapshotId {
        self.decision_policy_snapshot_id
    }
}

impl TryFrom<FeedbackCandidateRecipeDocument> for FeedbackCandidateRecipe {
    type Error = FeedbackError;

    fn try_from(document: FeedbackCandidateRecipeDocument) -> Result<Self, Self::Error> {
        let recipe = Self {
            format_version: document.format_version,
            candidate_recipe_hash: document.candidate_recipe_hash,
            recipe_template_hash: document.recipe_template_hash,
            planner_evidence_hash: document.planner_evidence_hash,
            resource_budget: document.resource_budget,
            training: document.training,
            calibration: document.calibration,
            calibration_method: document.calibration_method,
            cpcv_spec: document.cpcv_spec,
            downside_source: document.downside_source,
            decision_policy_snapshot_id: document.decision_policy_snapshot_id,
        };
        recipe.validate()?;
        Ok(recipe)
    }
}

/// Same-window statistic frozen before the Evaluation holdout opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackComparisonStatistic {
    MeanDecisionTickNetReturnBps,
}

/// Direction of every hypothesis in one candidate family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackComparisonAlternative {
    CandidateGreaterThanChampion,
}

/// Dependence-preserving bootstrap scheme for the ordered decision-tick series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackComparisonResampling {
    CircularFixedBlock,
}

/// Family-wise multiple-testing procedure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackComparisonStepdown {
    RomanoWolfBasic,
}

/// Finite-resample p-value convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackComparisonPValue {
    PlusOneGreaterOrEqual,
}

/// Explicit permutation-invariant handling of equal observed effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackComparisonTies {
    EqualStatisticGroup,
}

/// Reproducible, dependency-free bootstrap-index generator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackComparisonGenerator {
    Blake3CounterRejectionV1,
}

/// Full immutable same-window methodology.
///
/// Fixed enum values deliberately remain part of the hash preimage: changing
/// the metric, alternative, resampler, finite-sample convention, or tie rule
/// requires a new contract rather than an operational default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "FeedbackComparisonContractDocument")]
pub struct FeedbackComparisonContract {
    format_version: u32,
    comparison_contract_hash: ContentHash,
    statistic: FeedbackComparisonStatistic,
    alternative: FeedbackComparisonAlternative,
    resampling: FeedbackComparisonResampling,
    stepdown: FeedbackComparisonStepdown,
    p_value: FeedbackComparisonPValue,
    ties: FeedbackComparisonTies,
    generator: FeedbackComparisonGenerator,
    minimum_observations: u64,
    bootstrap_repetitions: u32,
    block_length: u32,
    bootstrap_seed: u64,
    minimum_effect_bps: Bps,
    confidence: Decimal,
    effect_precision_dp: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedbackComparisonContractDocument {
    format_version: u32,
    comparison_contract_hash: ContentHash,
    statistic: FeedbackComparisonStatistic,
    alternative: FeedbackComparisonAlternative,
    resampling: FeedbackComparisonResampling,
    stepdown: FeedbackComparisonStepdown,
    p_value: FeedbackComparisonPValue,
    ties: FeedbackComparisonTies,
    generator: FeedbackComparisonGenerator,
    minimum_observations: u64,
    bootstrap_repetitions: u32,
    block_length: u32,
    bootstrap_seed: u64,
    minimum_effect_bps: Bps,
    confidence: Decimal,
    effect_precision_dp: u32,
}

#[derive(Serialize)]
struct FeedbackComparisonContractPreimage {
    format_version: u32,
    statistic: FeedbackComparisonStatistic,
    alternative: FeedbackComparisonAlternative,
    resampling: FeedbackComparisonResampling,
    stepdown: FeedbackComparisonStepdown,
    p_value: FeedbackComparisonPValue,
    ties: FeedbackComparisonTies,
    generator: FeedbackComparisonGenerator,
    minimum_observations: u64,
    bootstrap_repetitions: u32,
    block_length: u32,
    bootstrap_seed: u64,
    minimum_effect_bps: Bps,
    confidence: Decimal,
    effect_precision_dp: u32,
}

impl FeedbackComparisonContract {
    /// Freeze the only supported F09 method from the immutable profile policy.
    pub fn try_from_policy(policy: &ResearchFeedbackPolicy) -> Result<Self, FeedbackError> {
        policy
            .validate()
            .map_err(|error| FeedbackError::InvalidComparisonContract {
                detail: error.to_string(),
            })?;
        let preimage = FeedbackComparisonContractPreimage {
            format_version: COMPARISON_CONTRACT_VERSION,
            statistic: FeedbackComparisonStatistic::MeanDecisionTickNetReturnBps,
            alternative: FeedbackComparisonAlternative::CandidateGreaterThanChampion,
            resampling: FeedbackComparisonResampling::CircularFixedBlock,
            stepdown: FeedbackComparisonStepdown::RomanoWolfBasic,
            p_value: FeedbackComparisonPValue::PlusOneGreaterOrEqual,
            ties: FeedbackComparisonTies::EqualStatisticGroup,
            generator: FeedbackComparisonGenerator::Blake3CounterRejectionV1,
            minimum_observations: policy.comparison_minimum_observations,
            bootstrap_repetitions: policy.comparison_bootstrap_repetitions,
            block_length: policy.comparison_block_length,
            bootstrap_seed: policy.comparison_bootstrap_seed,
            minimum_effect_bps: policy.minimum_effect_bps,
            confidence: policy.effect_confidence,
            effect_precision_dp: COMPARISON_EFFECT_PRECISION_DP,
        };
        let comparison_contract_hash = Self::derive_hash(&preimage)?;
        let contract = Self {
            format_version: preimage.format_version,
            comparison_contract_hash,
            statistic: preimage.statistic,
            alternative: preimage.alternative,
            resampling: preimage.resampling,
            stepdown: preimage.stepdown,
            p_value: preimage.p_value,
            ties: preimage.ties,
            generator: preimage.generator,
            minimum_observations: preimage.minimum_observations,
            bootstrap_repetitions: preimage.bootstrap_repetitions,
            block_length: preimage.block_length,
            bootstrap_seed: preimage.bootstrap_seed,
            minimum_effect_bps: preimage.minimum_effect_bps,
            confidence: preimage.confidence,
            effect_precision_dp: preimage.effect_precision_dp,
        };
        contract.validate()?;
        Ok(contract)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        let preimage = self.preimage();
        let expected_hash = Self::derive_hash(&preimage)?;
        if self.format_version != COMPARISON_CONTRACT_VERSION
            || self.statistic != FeedbackComparisonStatistic::MeanDecisionTickNetReturnBps
            || self.alternative != FeedbackComparisonAlternative::CandidateGreaterThanChampion
            || self.resampling != FeedbackComparisonResampling::CircularFixedBlock
            || self.stepdown != FeedbackComparisonStepdown::RomanoWolfBasic
            || self.p_value != FeedbackComparisonPValue::PlusOneGreaterOrEqual
            || self.ties != FeedbackComparisonTies::EqualStatisticGroup
            || self.generator != FeedbackComparisonGenerator::Blake3CounterRejectionV1
            || self.minimum_observations == 0
            || self.bootstrap_repetitions < 1_000
            || self.block_length == 0
            || u64::from(self.block_length) > self.minimum_observations
            || self.minimum_effect_bps.inner() <= Decimal::ZERO
            || self.confidence <= Decimal::ZERO
            || self.confidence >= Decimal::ONE
            || self.effect_precision_dp != COMPARISON_EFFECT_PRECISION_DP
            || self.comparison_contract_hash != expected_hash
        {
            return Err(FeedbackError::InvalidComparisonContract {
                detail: "comparison version, method, bounds, precision, or content hash is invalid"
                    .to_owned(),
            });
        }
        Ok(())
    }

    const fn preimage(&self) -> FeedbackComparisonContractPreimage {
        FeedbackComparisonContractPreimage {
            format_version: self.format_version,
            statistic: self.statistic,
            alternative: self.alternative,
            resampling: self.resampling,
            stepdown: self.stepdown,
            p_value: self.p_value,
            ties: self.ties,
            generator: self.generator,
            minimum_observations: self.minimum_observations,
            bootstrap_repetitions: self.bootstrap_repetitions,
            block_length: self.block_length,
            bootstrap_seed: self.bootstrap_seed,
            minimum_effect_bps: self.minimum_effect_bps,
            confidence: self.confidence,
            effect_precision_dp: self.effect_precision_dp,
        }
    }

    fn derive_hash(
        preimage: &FeedbackComparisonContractPreimage,
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            COMPARISON_CONTRACT_DOMAIN,
            COMPARISON_CONTRACT_VERSION,
            preimage,
        )
        .map_err(Into::into)
    }

    #[must_use]
    pub const fn comparison_contract_hash(&self) -> ContentHash {
        self.comparison_contract_hash
    }

    #[must_use]
    pub const fn minimum_observations(&self) -> u64 {
        self.minimum_observations
    }

    #[must_use]
    pub const fn bootstrap_repetitions(&self) -> u32 {
        self.bootstrap_repetitions
    }

    #[must_use]
    pub const fn block_length(&self) -> u32 {
        self.block_length
    }

    #[must_use]
    pub const fn bootstrap_seed(&self) -> u64 {
        self.bootstrap_seed
    }

    #[must_use]
    pub const fn minimum_effect_bps(&self) -> Bps {
        self.minimum_effect_bps
    }

    #[must_use]
    pub const fn confidence(&self) -> Decimal {
        self.confidence
    }

    #[must_use]
    pub const fn effect_precision_dp(&self) -> u32 {
        self.effect_precision_dp
    }
}

impl TryFrom<FeedbackComparisonContractDocument> for FeedbackComparisonContract {
    type Error = FeedbackError;

    fn try_from(document: FeedbackComparisonContractDocument) -> Result<Self, Self::Error> {
        let contract = Self {
            format_version: document.format_version,
            comparison_contract_hash: document.comparison_contract_hash,
            statistic: document.statistic,
            alternative: document.alternative,
            resampling: document.resampling,
            stepdown: document.stepdown,
            p_value: document.p_value,
            ties: document.ties,
            generator: document.generator,
            minimum_observations: document.minimum_observations,
            bootstrap_repetitions: document.bootstrap_repetitions,
            block_length: document.block_length,
            bootstrap_seed: document.bootstrap_seed,
            minimum_effect_bps: document.minimum_effect_bps,
            confidence: document.confidence,
            effect_precision_dp: document.effect_precision_dp,
        };
        contract.validate()?;
        Ok(contract)
    }
}

/// Inputs sealed into one canonical candidate family.
pub struct FeedbackCandidateFamilyInput {
    pub shared_evaluation: FeedbackDatasetBuildRequest,
    pub comparison_contract: FeedbackComparisonContract,
    pub candidates: Vec<FeedbackCandidateRecipe>,
}

/// Complete candidate family persisted in the cycle before evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields, try_from = "FeedbackCandidateFamilyDocument")]
pub struct FeedbackCandidateFamily {
    format_version: u32,
    candidate_family_hash: ContentHash,
    shared_evaluation: FeedbackDatasetBuildRequest,
    comparison_contract: FeedbackComparisonContract,
    candidates: Vec<FeedbackCandidateRecipe>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedbackCandidateFamilyDocument {
    format_version: u32,
    candidate_family_hash: ContentHash,
    shared_evaluation: FeedbackDatasetBuildRequest,
    comparison_contract: FeedbackComparisonContract,
    candidates: Vec<FeedbackCandidateRecipe>,
}

#[derive(Serialize)]
struct FeedbackCandidateFamilyPreimage<'a> {
    format_version: u32,
    shared_evaluation: &'a FeedbackDatasetBuildRequest,
    comparison_contract: &'a FeedbackComparisonContract,
    candidates: &'a [FeedbackCandidateRecipe],
}

impl FeedbackCandidateFamily {
    pub fn try_seal(mut input: FeedbackCandidateFamilyInput) -> Result<Self, FeedbackError> {
        input
            .candidates
            .sort_by_key(FeedbackCandidateRecipe::candidate_recipe_hash);
        let candidate_family_hash = Self::derive_hash(
            &input.shared_evaluation,
            &input.comparison_contract,
            &input.candidates,
        )?;
        let family = Self {
            format_version: CANDIDATE_FAMILY_VERSION,
            candidate_family_hash,
            shared_evaluation: input.shared_evaluation,
            comparison_contract: input.comparison_contract,
            candidates: input.candidates,
        };
        family.validate()?;
        Ok(family)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.shared_evaluation.validate()?;
        self.comparison_contract.validate()?;
        if self.format_version != CANDIDATE_FAMILY_VERSION
            || self.shared_evaluation.purpose != DatasetPurpose::Evaluation
            || self.candidates.is_empty()
            || self.candidates.len() > FEEDBACK_LEARNING_MAX_CANDIDATES
        {
            return Err(invalid_batch(
                "candidate family version, Evaluation Dataset, or cardinality is invalid",
            ));
        }
        let evaluation_lineage = &self.shared_evaluation.source_lineage;
        let evaluation_profile = self.shared_evaluation.window.profile_ref();
        let evaluation_start = self.shared_evaluation.window.window_start();
        let mut previous = None;
        let mut dataset_ids = HashSet::from([self.shared_evaluation.training_dataset_id]);
        for candidate in &self.candidates {
            candidate.validate()?;
            let recipe_hash = candidate.candidate_recipe_hash();
            if previous.is_some_and(|previous| previous >= recipe_hash)
                || !dataset_ids.insert(candidate.training().training_dataset_id)
                || !dataset_ids.insert(candidate.calibration().training_dataset_id)
                || candidate.training().window.profile_ref() != evaluation_profile
                || candidate.calibration().window.profile_ref() != evaluation_profile
                || candidate.calibration().window.cutoff() >= evaluation_start
                || candidate.training().model_spec_id != self.shared_evaluation.model_spec_id
                || candidate.training().model_spec_definition_hash
                    != self.shared_evaluation.model_spec_definition_hash
                || candidate.calibration().model_spec_id != self.shared_evaluation.model_spec_id
                || candidate.calibration().model_spec_definition_hash
                    != self.shared_evaluation.model_spec_definition_hash
                || candidate.decision_policy_snapshot_id()
                    != evaluation_lineage.decision_policy_snapshot_id
                || candidate
                    .training()
                    .source_lineage
                    .capability_registry_hashes
                    != evaluation_lineage.capability_registry_hashes
                || candidate
                    .calibration()
                    .source_lineage
                    .capability_registry_hashes
                    != evaluation_lineage.capability_registry_hashes
            {
                return Err(invalid_batch(
                    "candidate family recipes are not canonical or do not share exact profile, split, policy, capability, and Dataset identities",
                ));
            }
            previous = Some(recipe_hash);
        }
        let exact_hash = Self::derive_hash(
            &self.shared_evaluation,
            &self.comparison_contract,
            &self.candidates,
        )?;
        if self.candidate_family_hash != exact_hash {
            return Err(invalid_batch(
                "candidate family hash differs from its canonical preimage",
            ));
        }
        Ok(())
    }

    fn derive_hash(
        shared_evaluation: &FeedbackDatasetBuildRequest,
        comparison_contract: &FeedbackComparisonContract,
        candidates: &[FeedbackCandidateRecipe],
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            CANDIDATE_FAMILY_DOMAIN,
            CANDIDATE_FAMILY_VERSION,
            &FeedbackCandidateFamilyPreimage {
                format_version: CANDIDATE_FAMILY_VERSION,
                shared_evaluation,
                comparison_contract,
                candidates,
            },
        )
        .map_err(Into::into)
    }

    #[must_use]
    pub const fn candidate_family_hash(&self) -> ContentHash {
        self.candidate_family_hash
    }

    #[must_use]
    pub const fn shared_evaluation(&self) -> &FeedbackDatasetBuildRequest {
        &self.shared_evaluation
    }

    #[must_use]
    pub const fn comparison_contract_hash(&self) -> ContentHash {
        self.comparison_contract.comparison_contract_hash()
    }

    #[must_use]
    pub const fn comparison_contract(&self) -> &FeedbackComparisonContract {
        &self.comparison_contract
    }

    #[must_use]
    pub fn candidates(&self) -> &[FeedbackCandidateRecipe] {
        &self.candidates
    }

    #[must_use]
    pub fn candidate(&self, recipe_hash: ContentHash) -> Option<&FeedbackCandidateRecipe> {
        self.candidates
            .binary_search_by_key(&recipe_hash, FeedbackCandidateRecipe::candidate_recipe_hash)
            .ok()
            .map(|index| &self.candidates[index])
    }
}

impl TryFrom<FeedbackCandidateFamilyDocument> for FeedbackCandidateFamily {
    type Error = FeedbackError;

    fn try_from(document: FeedbackCandidateFamilyDocument) -> Result<Self, Self::Error> {
        let family = Self {
            format_version: document.format_version,
            candidate_family_hash: document.candidate_family_hash,
            shared_evaluation: document.shared_evaluation,
            comparison_contract: document.comparison_contract,
            candidates: document.candidates,
        };
        family.validate()?;
        Ok(family)
    }
}

/// One `DatasetSeal` batch member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackDatasetBuildCommand {
    pub role: FeedbackDatasetRole,
    pub resource_budget: FeedbackRecipeResourceBudget,
    pub request: FeedbackDatasetBuildRequest,
}

/// One Training batch member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackTrainingCommand {
    pub candidate_recipe_hash: ContentHash,
    pub resource_budget: FeedbackRecipeResourceBudget,
    pub params: ModelTrainJobParams,
}

/// One Calibration batch member. The fit parameters own the immutable
/// derived-model semantics and actor provenance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackCalibrationCommand {
    pub candidate_recipe_hash: ContentHash,
    pub resource_budget: FeedbackRecipeResourceBudget,
    pub params: ModelCalibrationFitJobParams,
}

/// One CPCV batch member carrying its preassigned immutable path-set identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackCpcvCommand {
    pub candidate_recipe_hash: ContentHash,
    pub resource_budget: FeedbackRecipeResourceBudget,
    pub cpcv_spec: FeedbackRecipeCpcvSpec,
    pub params: CpcvBacktestJobParams,
}

/// Exact terminal artifact of the immediately preceding feedback stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackLearningStageArtifactRef {
    pub feedback_cycle_id: FeedbackCycleId,
    pub stage: FeedbackStage,
    pub job_id: ResearchJobId,
    pub artifact_id: FeedbackLearningStageArtifactId,
    pub input_hash: ContentHash,
    pub artifact: ResearchJobArtifactRef,
}

impl FeedbackLearningStageArtifactRef {
    /// Verify this reference is the exact predecessor for a cycle and stage.
    pub fn validate_for(
        &self,
        feedback_cycle_id: FeedbackCycleId,
        stage: FeedbackStage,
    ) -> Result<(), FeedbackError> {
        if self.feedback_cycle_id != feedback_cycle_id
            || self.stage != stage
            || FeedbackLearningStageArtifactId::from_cycle_stage(feedback_cycle_id, stage)
                != Some(self.artifact_id)
        {
            return Err(FeedbackError::InvalidJobContract {
                detail: format!(
                    "learning-stage predecessor must bind cycle {feedback_cycle_id} stage {stage}"
                ),
            });
        }
        Ok(())
    }
}

/// Frozen `DatasetSeal` batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackDatasetSealJobParams {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub artifact_id: FeedbackLearningStageArtifactId,
    pub commands: Vec<FeedbackDatasetBuildCommand>,
}

impl FeedbackDatasetSealJobParams {
    pub fn try_new(
        feedback_cycle_id: FeedbackCycleId,
        cycle_idempotency_hash: ContentHash,
        candidate_family_hash: ContentHash,
        commands: Vec<FeedbackDatasetBuildCommand>,
    ) -> Result<Self, FeedbackError> {
        let artifact_id = learning_artifact_id(feedback_cycle_id, FeedbackStage::DatasetSeal)?;
        let params = Self {
            feedback_cycle_id,
            cycle_idempotency_hash,
            candidate_family_hash,
            artifact_id,
            commands,
        };
        params.validate()?;
        Ok(params)
    }

    pub fn input_hash(&self) -> Result<ContentHash, FeedbackError> {
        self.validate()?;
        learning_input_hash(FeedbackStage::DatasetSeal, self)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        validate_stage_identity(
            self.feedback_cycle_id,
            self.cycle_idempotency_hash,
            FeedbackStage::DatasetSeal,
            self.artifact_id,
        )?;
        validate_dataset_commands(&self.commands)
    }
}

/// Frozen Training batch and exact `DatasetSeal` predecessor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackTrainingJobParams {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub artifact_id: FeedbackLearningStageArtifactId,
    pub previous: FeedbackLearningStageArtifactRef,
    pub commands: Vec<FeedbackTrainingCommand>,
}

impl FeedbackTrainingJobParams {
    pub fn try_new(
        feedback_cycle_id: FeedbackCycleId,
        cycle_idempotency_hash: ContentHash,
        candidate_family_hash: ContentHash,
        previous: FeedbackLearningStageArtifactRef,
        commands: Vec<FeedbackTrainingCommand>,
    ) -> Result<Self, FeedbackError> {
        let artifact_id = learning_artifact_id(feedback_cycle_id, FeedbackStage::Training)?;
        let params = Self {
            feedback_cycle_id,
            cycle_idempotency_hash,
            candidate_family_hash,
            artifact_id,
            previous,
            commands,
        };
        params.validate()?;
        Ok(params)
    }

    pub fn input_hash(&self) -> Result<ContentHash, FeedbackError> {
        self.validate()?;
        learning_input_hash(FeedbackStage::Training, self)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        validate_stage_identity(
            self.feedback_cycle_id,
            self.cycle_idempotency_hash,
            FeedbackStage::Training,
            self.artifact_id,
        )?;
        self.previous
            .validate_for(self.feedback_cycle_id, FeedbackStage::DatasetSeal)?;
        validate_training_commands(&self.commands)
    }
}

/// Frozen Calibration batch and exact Training predecessor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackCalibrationJobParams {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub artifact_id: FeedbackLearningStageArtifactId,
    pub previous: FeedbackLearningStageArtifactRef,
    pub commands: Vec<FeedbackCalibrationCommand>,
}

impl FeedbackCalibrationJobParams {
    pub fn try_new(
        feedback_cycle_id: FeedbackCycleId,
        cycle_idempotency_hash: ContentHash,
        candidate_family_hash: ContentHash,
        previous: FeedbackLearningStageArtifactRef,
        commands: Vec<FeedbackCalibrationCommand>,
    ) -> Result<Self, FeedbackError> {
        let artifact_id = learning_artifact_id(feedback_cycle_id, FeedbackStage::Calibration)?;
        let params = Self {
            feedback_cycle_id,
            cycle_idempotency_hash,
            candidate_family_hash,
            artifact_id,
            previous,
            commands,
        };
        params.validate()?;
        Ok(params)
    }

    pub fn input_hash(&self) -> Result<ContentHash, FeedbackError> {
        self.validate()?;
        learning_input_hash(FeedbackStage::Calibration, self)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        validate_stage_identity(
            self.feedback_cycle_id,
            self.cycle_idempotency_hash,
            FeedbackStage::Calibration,
            self.artifact_id,
        )?;
        self.previous
            .validate_for(self.feedback_cycle_id, FeedbackStage::Training)?;
        validate_calibration_commands(&self.commands)
    }
}

/// Frozen CPCV batch and exact Calibration predecessor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackCpcvJobParams {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub artifact_id: FeedbackLearningStageArtifactId,
    pub previous: FeedbackLearningStageArtifactRef,
    pub commands: Vec<FeedbackCpcvCommand>,
}

impl FeedbackCpcvJobParams {
    pub fn try_new(
        feedback_cycle_id: FeedbackCycleId,
        cycle_idempotency_hash: ContentHash,
        candidate_family_hash: ContentHash,
        previous: FeedbackLearningStageArtifactRef,
        commands: Vec<FeedbackCpcvCommand>,
    ) -> Result<Self, FeedbackError> {
        let artifact_id = learning_artifact_id(feedback_cycle_id, FeedbackStage::Cpcv)?;
        let params = Self {
            feedback_cycle_id,
            cycle_idempotency_hash,
            candidate_family_hash,
            artifact_id,
            previous,
            commands,
        };
        params.validate()?;
        Ok(params)
    }

    pub fn input_hash(&self) -> Result<ContentHash, FeedbackError> {
        self.validate()?;
        learning_input_hash(FeedbackStage::Cpcv, self)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        validate_stage_identity(
            self.feedback_cycle_id,
            self.cycle_idempotency_hash,
            FeedbackStage::Cpcv,
            self.artifact_id,
        )?;
        self.previous
            .validate_for(self.feedback_cycle_id, FeedbackStage::Calibration)?;
        validate_cpcv_commands(&self.commands)
    }
}

/// Immutable projection of the durable one-time Evaluation reservation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackEvaluationUseRef {
    pub feedback_evaluation_use_id: FeedbackEvaluationUseId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub profile_ref: ResearchProfileRef,
    pub evaluation_dataset_id: TrainingDatasetId,
    pub evaluation_dataset_hash: ContentHash,
    pub evaluation_artifact_bytes_hash: ContentHash,
    pub cohort_manifest_hash: ContentHash,
    pub evaluation_window_start: DateTime<Utc>,
    pub evaluation_window_end: DateTime<Utc>,
    pub label_cutoff: DateTime<Utc>,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub comparison_contract_hash: ContentHash,
    pub semantic_use_hash: ContentHash,
    pub cpcv_artifact_uri: ArtifactUri,
    pub cpcv_artifact_hash: ContentHash,
    pub evaluation_use_hash: ContentHash,
}

impl From<&FeedbackEvaluationUseInfo> for FeedbackEvaluationUseRef {
    fn from(info: &FeedbackEvaluationUseInfo) -> Self {
        Self {
            feedback_evaluation_use_id: info.feedback_evaluation_use_id,
            feedback_cycle_id: info.feedback_cycle_id,
            profile_ref: info.profile_ref.clone(),
            evaluation_dataset_id: info.evaluation_dataset_id,
            evaluation_dataset_hash: info.evaluation_dataset_hash,
            evaluation_artifact_bytes_hash: info.evaluation_artifact_bytes_hash,
            cohort_manifest_hash: info.cohort_manifest_hash,
            evaluation_window_start: info.evaluation_window_start,
            evaluation_window_end: info.evaluation_window_end,
            label_cutoff: info.label_cutoff,
            champion_model_version_id: info.champion_model_version_id,
            champion_serving_contract_hash: info.champion_serving_contract_hash,
            candidate_family_hash: info.candidate_family_hash,
            comparison_contract_hash: info.comparison_contract_hash,
            semantic_use_hash: info.semantic_use_hash,
            cpcv_artifact_uri: info.cpcv_artifact_uri.clone(),
            cpcv_artifact_hash: info.cpcv_artifact_hash,
            evaluation_use_hash: info.evaluation_use_hash,
        }
    }
}

impl FeedbackEvaluationUseRef {
    pub fn validate_for(
        &self,
        feedback_cycle_id: FeedbackCycleId,
        candidate_family_hash: ContentHash,
        contract: &FeedbackComparisonContract,
        previous: &FeedbackLearningStageArtifactRef,
    ) -> Result<(), FeedbackError> {
        if self.feedback_cycle_id != feedback_cycle_id
            || self.evaluation_window_start >= self.evaluation_window_end
            || self.evaluation_window_end > self.label_cutoff
            || self.candidate_family_hash != candidate_family_hash
            || self.comparison_contract_hash != contract.comparison_contract_hash()
            || self.cpcv_artifact_uri != previous.artifact.uri
            || self.cpcv_artifact_hash != previous.artifact.content_hash
        {
            return Err(FeedbackError::InvalidComparisonEvidence {
                detail: "evaluation reservation differs from cycle, family, method, window, or CPCV predecessor"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

/// One CPCV-eligible challenger frozen into the Comparison job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackComparisonCandidateRef {
    pub candidate_recipe_hash: ContentHash,
    pub model_version_id: ModelVersionId,
    pub serving_contract_hash: ContentHash,
    pub path_set_id: BacktestPathSetId,
    pub path_set_hash: ContentHash,
    pub model_run_id: ModelRunId,
    pub backtest_report_id: BacktestReportId,
}

/// Frozen Comparison stage input and exact Validation/Evaluation lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackComparisonJobParams {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub artifact_id: FeedbackComparisonArtifactId,
    pub validation: FeedbackValidationArtifactRef,
    pub evaluation_use: FeedbackEvaluationUseRef,
    pub comparison_contract: FeedbackComparisonContract,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub champion_model_run_id: ModelRunId,
    pub champion_backtest_report_id: BacktestReportId,
    pub candidates: Vec<FeedbackComparisonCandidateRef>,
}

/// Inputs sealed into one Comparison stage job.
pub struct FeedbackComparisonJobInput {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub validation: FeedbackValidationArtifactRef,
    pub evaluation_use: FeedbackEvaluationUseRef,
    pub comparison_contract: FeedbackComparisonContract,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidates: Vec<FeedbackComparisonCandidateRef>,
}

impl FeedbackComparisonJobParams {
    pub fn try_new(input: FeedbackComparisonJobInput) -> Result<Self, FeedbackError> {
        let params = Self {
            feedback_cycle_id: input.feedback_cycle_id,
            cycle_idempotency_hash: input.cycle_idempotency_hash,
            candidate_family_hash: input.candidate_family_hash,
            artifact_id: FeedbackComparisonArtifactId::from_cycle_id(input.feedback_cycle_id),
            validation: input.validation,
            evaluation_use: input.evaluation_use,
            comparison_contract: input.comparison_contract,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            champion_model_version_id: input.champion_model_version_id,
            champion_serving_contract_hash: input.champion_serving_contract_hash,
            champion_model_run_id: ModelRunId::from_feedback_comparison(
                FeedbackComparisonArtifactId::from_cycle_id(input.feedback_cycle_id),
                input.champion_model_version_id,
            ),
            champion_backtest_report_id: BacktestReportId::from_feedback_comparison(
                FeedbackComparisonArtifactId::from_cycle_id(input.feedback_cycle_id),
                input.champion_model_version_id,
            ),
            candidates: input.candidates,
        };
        params.validate()?;
        Ok(params)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.comparison_contract.validate()?;
        self.validation.validate_for(self.feedback_cycle_id)?;
        self.evaluation_use.validate_for(
            self.feedback_cycle_id,
            self.candidate_family_hash,
            &self.comparison_contract,
            &self.validation.cpcv,
        )?;
        if FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
            != self.feedback_cycle_id
            || self.artifact_id
                != FeedbackComparisonArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.champion_model_version_id != self.evaluation_use.champion_model_version_id
            || self.champion_serving_contract_hash
                != self.evaluation_use.champion_serving_contract_hash
            || self.champion_model_run_id
                != ModelRunId::from_feedback_comparison(
                    self.artifact_id,
                    self.champion_model_version_id,
                )
            || self.champion_backtest_report_id
                != BacktestReportId::from_feedback_comparison(
                    self.artifact_id,
                    self.champion_model_version_id,
                )
            || self.candidates.is_empty()
            || self.candidates.len() > FEEDBACK_LEARNING_MAX_CANDIDATES
        {
            return Err(FeedbackError::InvalidComparisonEvidence {
                detail: "comparison job identity, champion, or candidate cardinality is invalid"
                    .to_owned(),
            });
        }
        let mut previous_hash = None;
        let mut models = HashSet::new();
        let mut path_sets = HashSet::new();
        for candidate in &self.candidates {
            if previous_hash.is_some_and(|previous| previous >= candidate.candidate_recipe_hash)
                || candidate.model_version_id == self.champion_model_version_id
                || !models.insert(candidate.model_version_id)
                || !path_sets.insert(candidate.path_set_id)
                || candidate.model_run_id
                    != ModelRunId::from_feedback_comparison(
                        self.artifact_id,
                        candidate.model_version_id,
                    )
                || candidate.backtest_report_id
                    != BacktestReportId::from_feedback_comparison(
                        self.artifact_id,
                        candidate.model_version_id,
                    )
            {
                return Err(FeedbackError::InvalidComparisonEvidence {
                    detail: "comparison candidates must be recipe-sorted and have unique non-champion model/path identities"
                        .to_owned(),
                });
            }
            previous_hash = Some(candidate.candidate_recipe_hash);
        }
        Ok(())
    }

    pub fn input_hash(&self) -> Result<ContentHash, FeedbackError> {
        self.validate()?;
        CanonicalDigest::content_hash_typed(COMPARISON_INPUT_DOMAIN, COMPARISON_INPUT_VERSION, self)
            .map_err(Into::into)
    }
}

/// Verified object-store result of one Comparison job.
pub struct FeedbackComparisonExecutionResult {
    pub artifact_id: FeedbackComparisonArtifactId,
    pub artifact: ResearchJobArtifactRef,
}

/// Executes the reserved same-window comparison without promotion authority.
#[async_trait]
pub trait FeedbackComparisonExecutionPort: Send + Sync {
    async fn execute(
        &self,
        params: FeedbackComparisonJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackComparisonExecutionResult>;
}

/// Immutable pointer to the verified F09 predecessor object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackComparisonArtifactRef {
    pub feedback_cycle_id: FeedbackCycleId,
    pub job_id: ResearchJobId,
    pub artifact_id: FeedbackComparisonArtifactId,
    pub input_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub artifact: ResearchJobArtifactRef,
}

impl FeedbackComparisonArtifactRef {
    pub fn validate_for(&self, feedback_cycle_id: FeedbackCycleId) -> Result<(), FeedbackError> {
        if self.feedback_cycle_id != feedback_cycle_id
            || self.artifact_id != FeedbackComparisonArtifactId::from_cycle_id(feedback_cycle_id)
        {
            return Err(FeedbackError::InvalidComparisonEvidence {
                detail: "shadow predecessor does not bind the exact feedback cycle".to_owned(),
            });
        }
        Ok(())
    }
}

/// Only production generation observations may satisfy the F10 gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackShadowObservationSource {
    PublishedGeneration,
}

/// Complete identity and thresholds for one exact production shadow window.
pub struct FeedbackShadowContractInput {
    pub profile_ref: ResearchProfileRef,
    pub feedback_policy_hash: ContentHash,
    pub category_scope: Option<MarketCategory>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub decision_policy_snapshot_hash: ContentHash,
    pub policy_bundle_generation: PolicyBundleGeneration,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_model_version_id: ModelVersionId,
    pub candidate_serving_contract_hash: ContentHash,
    pub observation_window_start: DateTime<Utc>,
    pub observation_window_end: DateTime<Utc>,
    pub minimum_observations: u64,
    pub required_window_secs: u64,
    pub minimum_topn_decision_overlap: Probability,
}

/// Versioned F10 contract. It cannot represent replay as production evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackShadowContract {
    format_version: u32,
    contract_hash: ContentHash,
    observation_source: FeedbackShadowObservationSource,
    profile_ref: ResearchProfileRef,
    feedback_policy_hash: ContentHash,
    category_scope: Option<MarketCategory>,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    decision_policy_snapshot_hash: ContentHash,
    policy_bundle_generation: PolicyBundleGeneration,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    candidate_model_version_id: ModelVersionId,
    candidate_serving_contract_hash: ContentHash,
    observation_window_start: DateTime<Utc>,
    observation_window_end: DateTime<Utc>,
    minimum_observations: u64,
    required_window_secs: u64,
    minimum_topn_decision_overlap: Probability,
}

#[derive(Serialize)]
struct FeedbackShadowContractPreimage<'a> {
    format_version: u32,
    observation_source: FeedbackShadowObservationSource,
    profile_ref: &'a ResearchProfileRef,
    feedback_policy_hash: ContentHash,
    category_scope: Option<MarketCategory>,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    decision_policy_snapshot_hash: ContentHash,
    policy_bundle_generation: PolicyBundleGeneration,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    candidate_model_version_id: ModelVersionId,
    candidate_serving_contract_hash: ContentHash,
    observation_window_start: DateTime<Utc>,
    observation_window_end: DateTime<Utc>,
    minimum_observations: u64,
    required_window_secs: u64,
    minimum_topn_decision_overlap: Probability,
}

impl FeedbackShadowContract {
    pub fn try_seal(input: FeedbackShadowContractInput) -> Result<Self, FeedbackError> {
        let contract_hash = Self::derive_hash(&FeedbackShadowContractPreimage {
            format_version: SHADOW_CONTRACT_VERSION,
            observation_source: FeedbackShadowObservationSource::PublishedGeneration,
            profile_ref: &input.profile_ref,
            feedback_policy_hash: input.feedback_policy_hash,
            category_scope: input.category_scope,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: input.decision_policy_snapshot_hash,
            policy_bundle_generation: input.policy_bundle_generation,
            champion_model_version_id: input.champion_model_version_id,
            champion_serving_contract_hash: input.champion_serving_contract_hash,
            candidate_model_version_id: input.candidate_model_version_id,
            candidate_serving_contract_hash: input.candidate_serving_contract_hash,
            observation_window_start: input.observation_window_start,
            observation_window_end: input.observation_window_end,
            minimum_observations: input.minimum_observations,
            required_window_secs: input.required_window_secs,
            minimum_topn_decision_overlap: input.minimum_topn_decision_overlap,
        })?;
        let contract = Self {
            format_version: SHADOW_CONTRACT_VERSION,
            contract_hash,
            observation_source: FeedbackShadowObservationSource::PublishedGeneration,
            profile_ref: input.profile_ref,
            feedback_policy_hash: input.feedback_policy_hash,
            category_scope: input.category_scope,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: input.decision_policy_snapshot_hash,
            policy_bundle_generation: input.policy_bundle_generation,
            champion_model_version_id: input.champion_model_version_id,
            champion_serving_contract_hash: input.champion_serving_contract_hash,
            candidate_model_version_id: input.candidate_model_version_id,
            candidate_serving_contract_hash: input.candidate_serving_contract_hash,
            observation_window_start: input.observation_window_start,
            observation_window_end: input.observation_window_end,
            minimum_observations: input.minimum_observations,
            required_window_secs: input.required_window_secs,
            minimum_topn_decision_overlap: input.minimum_topn_decision_overlap,
        };
        contract.validate()?;
        Ok(contract)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.profile_ref
            .validate()
            .map_err(|error| FeedbackError::InvalidComparisonEvidence {
                detail: format!("shadow contract profile is invalid: {error}"),
            })?;
        let profile = self
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(|error| FeedbackError::InvalidComparisonEvidence { detail: error })?;
        let expected_policy_hash =
            profile
                .spec
                .feedback_policy
                .content_hash()
                .map_err(|error| FeedbackError::InvalidComparisonEvidence {
                    detail: format!("shadow contract feedback policy is invalid: {error}"),
                })?;
        let duration_secs = self
            .observation_window_end
            .signed_duration_since(self.observation_window_start)
            .num_seconds();
        let duration_covers_requirement = u64::try_from(duration_secs)
            .is_ok_and(|duration| duration >= self.required_window_secs);
        if self.format_version != SHADOW_CONTRACT_VERSION
            || self.observation_source != FeedbackShadowObservationSource::PublishedGeneration
            || profile.spec.category != self.category_scope
            || expected_policy_hash != self.feedback_policy_hash
            || profile.spec.feedback_policy.shadow_minimum_observations != self.minimum_observations
            || self.champion_model_version_id == self.candidate_model_version_id
            || self.champion_serving_contract_hash == self.candidate_serving_contract_hash
            || self.minimum_observations == 0
            || self.required_window_secs == 0
            || !duration_covers_requirement
            || self.minimum_topn_decision_overlap.inner() <= Decimal::ZERO
            || self.minimum_topn_decision_overlap.inner() > Decimal::ONE
            || self.contract_hash != Self::derive_hash(&self.preimage())?
        {
            return Err(FeedbackError::InvalidComparisonEvidence {
                detail:
                    "shadow contract identity, policy, window, subjects, or thresholds are invalid"
                        .to_owned(),
            });
        }
        Ok(())
    }

    const fn preimage(&self) -> FeedbackShadowContractPreimage<'_> {
        FeedbackShadowContractPreimage {
            format_version: self.format_version,
            observation_source: self.observation_source,
            profile_ref: &self.profile_ref,
            feedback_policy_hash: self.feedback_policy_hash,
            category_scope: self.category_scope,
            decision_policy_snapshot_id: self.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: self.decision_policy_snapshot_hash,
            policy_bundle_generation: self.policy_bundle_generation,
            champion_model_version_id: self.champion_model_version_id,
            champion_serving_contract_hash: self.champion_serving_contract_hash,
            candidate_model_version_id: self.candidate_model_version_id,
            candidate_serving_contract_hash: self.candidate_serving_contract_hash,
            observation_window_start: self.observation_window_start,
            observation_window_end: self.observation_window_end,
            minimum_observations: self.minimum_observations,
            required_window_secs: self.required_window_secs,
            minimum_topn_decision_overlap: self.minimum_topn_decision_overlap,
        }
    }

    fn derive_hash(
        preimage: &FeedbackShadowContractPreimage<'_>,
    ) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            SHADOW_CONTRACT_DOMAIN,
            SHADOW_CONTRACT_VERSION,
            preimage,
        )
        .map_err(Into::into)
    }

    #[must_use]
    pub const fn contract_hash(&self) -> ContentHash {
        self.contract_hash
    }

    #[must_use]
    pub const fn profile_ref(&self) -> &ResearchProfileRef {
        &self.profile_ref
    }

    #[must_use]
    pub const fn feedback_policy_hash(&self) -> ContentHash {
        self.feedback_policy_hash
    }

    #[must_use]
    pub const fn category_scope(&self) -> Option<MarketCategory> {
        self.category_scope
    }

    #[must_use]
    pub const fn decision_policy_snapshot_id(&self) -> DecisionPolicySnapshotId {
        self.decision_policy_snapshot_id
    }

    #[must_use]
    pub const fn decision_policy_snapshot_hash(&self) -> ContentHash {
        self.decision_policy_snapshot_hash
    }

    #[must_use]
    pub const fn policy_bundle_generation(&self) -> PolicyBundleGeneration {
        self.policy_bundle_generation
    }

    #[must_use]
    pub const fn champion_model_version_id(&self) -> ModelVersionId {
        self.champion_model_version_id
    }

    #[must_use]
    pub const fn champion_serving_contract_hash(&self) -> ContentHash {
        self.champion_serving_contract_hash
    }

    #[must_use]
    pub const fn candidate_model_version_id(&self) -> ModelVersionId {
        self.candidate_model_version_id
    }

    #[must_use]
    pub const fn candidate_serving_contract_hash(&self) -> ContentHash {
        self.candidate_serving_contract_hash
    }

    #[must_use]
    pub const fn observation_window_start(&self) -> DateTime<Utc> {
        self.observation_window_start
    }

    #[must_use]
    pub const fn observation_window_end(&self) -> DateTime<Utc> {
        self.observation_window_end
    }

    #[must_use]
    pub const fn minimum_observations(&self) -> u64 {
        self.minimum_observations
    }

    #[must_use]
    pub const fn required_window_secs(&self) -> u64 {
        self.required_window_secs
    }

    #[must_use]
    pub const fn minimum_topn_decision_overlap(&self) -> Probability {
        self.minimum_topn_decision_overlap
    }
}

/// Why F09 produced no challenger that may enter production-shadow evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackShadowUnavailableReason {
    ComparisonInsufficientObservations,
    AllCandidatesRejected,
}

/// F10 evaluates either no eligible challenger or one exact pinned challenger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "subject", rename_all = "snake_case", deny_unknown_fields)]
pub enum FeedbackShadowSubject {
    NoEligibleCandidate {
        reason: FeedbackShadowUnavailableReason,
    },
    Candidate {
        candidate_recipe_hash: ContentHash,
        contract: Box<FeedbackShadowContract>,
    },
}

/// Frozen `Shadow` stage input and exact `ShadowBind` lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackShadowJobParams {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub artifact_id: FeedbackShadowArtifactId,
    pub binding: ShadowBindingArtifactRef,
    pub profile_ref: ResearchProfileRef,
    pub feedback_policy_hash: ContentHash,
    pub subject: FeedbackShadowSubject,
}

impl FeedbackShadowJobParams {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.binding.validate_for(self.feedback_cycle_id)?;
        self.profile_ref
            .validate()
            .map_err(|error| FeedbackError::InvalidComparisonEvidence {
                detail: format!("shadow job profile is invalid: {error}"),
            })?;
        if FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
            != self.feedback_cycle_id
            || self.artifact_id != FeedbackShadowArtifactId::from_cycle_id(self.feedback_cycle_id)
        {
            return Err(FeedbackError::InvalidComparisonEvidence {
                detail: "shadow job identity or comparison lineage is invalid".to_owned(),
            });
        }
        if let FeedbackShadowSubject::Candidate { contract, .. } = &self.subject {
            contract.validate()?;
            if contract.profile_ref() != &self.profile_ref
                || contract.feedback_policy_hash() != self.feedback_policy_hash
                || contract.decision_policy_snapshot_id() != self.binding.committed_snapshot_id
                || contract.decision_policy_snapshot_hash() != self.binding.committed_snapshot_hash
                || contract.policy_bundle_generation() != self.binding.committed_policy_generation
                || contract.champion_model_version_id() != self.binding.champion_model_version_id
                || contract.champion_serving_contract_hash()
                    != self.binding.champion_serving_contract_hash
                || contract.candidate_model_version_id() != self.binding.candidate_model_version_id
                || contract.candidate_serving_contract_hash()
                    != self.binding.candidate_serving_contract_hash
                || contract.observation_window_start() != self.binding.bound_at
            {
                return Err(FeedbackError::InvalidComparisonEvidence {
                    detail: "shadow candidate contract differs from cycle profile, policy, or comparison predecessor"
                        .to_owned(),
                });
            }
        }
        Ok(())
    }

    pub fn input_hash(&self) -> Result<ContentHash, FeedbackError> {
        self.validate()?;
        CanonicalDigest::content_hash_typed(SHADOW_INPUT_DOMAIN, SHADOW_INPUT_VERSION, self)
            .map_err(Into::into)
    }
}

/// Verified object-store result of one `Shadow` job.
pub struct FeedbackShadowExecutionResult {
    pub artifact_id: FeedbackShadowArtifactId,
    pub artifact: ResearchJobArtifactRef,
}

/// Evaluates exact production-generation observations without route authority.
#[async_trait]
pub trait FeedbackShadowExecutionPort: Send + Sync {
    async fn execute(
        &self,
        params: FeedbackShadowJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackShadowExecutionResult>;
}

impl FeedbackDriftJobParams {
    /// Verify and hash the complete F06 job input for downstream lineage.
    pub fn input_hash(&self) -> Result<ContentHash, FeedbackError> {
        if FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
            != self.feedback_cycle_id
            || self.artifact_id != FeedbackDriftArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.coverage_artifact_id
                != FeedbackCoverageArtifactId::from_cycle_id(self.feedback_cycle_id)
        {
            return Err(FeedbackError::InvalidJobContract {
                detail: "feedback drift job does not bind its cycle or coverage identity"
                    .to_owned(),
            });
        }
        CanonicalDigest::content_hash_typed(DRIFT_INPUT_DOMAIN, DRIFT_INPUT_VERSION, self)
            .map_err(Into::into)
    }
}

/// Immutable pointer to the verified F06 drift predecessor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackDriftArtifactRef {
    pub feedback_cycle_id: FeedbackCycleId,
    pub job_id: ResearchJobId,
    pub artifact_id: FeedbackDriftArtifactId,
    pub input_hash: ContentHash,
    pub artifact: ResearchJobArtifactRef,
}

impl FeedbackDriftArtifactRef {
    pub fn validate_for(&self, feedback_cycle_id: FeedbackCycleId) -> Result<(), FeedbackError> {
        if self.feedback_cycle_id != feedback_cycle_id
            || self.artifact_id != FeedbackDriftArtifactId::from_cycle_id(feedback_cycle_id)
        {
            return Err(FeedbackError::InvalidJobContract {
                detail: "decision drift predecessor does not bind the exact feedback cycle"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

/// Immutable pointer to the verified F10 shadow/replay predecessor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackShadowArtifactRef {
    pub feedback_cycle_id: FeedbackCycleId,
    pub job_id: ResearchJobId,
    pub artifact_id: FeedbackShadowArtifactId,
    pub input_hash: ContentHash,
    pub binding: ShadowBindingArtifactRef,
    pub artifact: ResearchJobArtifactRef,
}

impl FeedbackShadowArtifactRef {
    pub fn validate_for(
        &self,
        feedback_cycle_id: FeedbackCycleId,
        binding: &ShadowBindingArtifactRef,
    ) -> Result<(), FeedbackError> {
        if self.feedback_cycle_id != feedback_cycle_id
            || self.artifact_id != FeedbackShadowArtifactId::from_cycle_id(feedback_cycle_id)
            || &self.binding != binding
        {
            return Err(FeedbackError::InvalidJobContract {
                detail: "decision shadow predecessor does not bind the exact cycle and ShadowBind"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

/// Inputs jointly sealing the evidence-only terminal Decision job.
pub struct FeedbackDecisionJobInput {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub profile_ref: ResearchProfileRef,
    pub feedback_policy_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub drift: FeedbackDriftArtifactRef,
    pub comparison: FeedbackComparisonArtifactRef,
    pub shadow: FeedbackShadowArtifactRef,
}

/// Frozen F11 input with exact F06/F09/F10 content-addressed lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackDecisionJobParams {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub artifact_id: FeedbackDecisionArtifactId,
    pub profile_ref: ResearchProfileRef,
    pub feedback_policy_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub drift: FeedbackDriftArtifactRef,
    pub comparison: FeedbackComparisonArtifactRef,
    pub shadow: FeedbackShadowArtifactRef,
}

impl FeedbackDecisionJobParams {
    pub fn try_new(input: FeedbackDecisionJobInput) -> Result<Self, FeedbackError> {
        let params = Self {
            feedback_cycle_id: input.feedback_cycle_id,
            cycle_idempotency_hash: input.cycle_idempotency_hash,
            artifact_id: FeedbackDecisionArtifactId::from_cycle_id(input.feedback_cycle_id),
            profile_ref: input.profile_ref,
            feedback_policy_hash: input.feedback_policy_hash,
            candidate_family_hash: input.candidate_family_hash,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            champion_model_version_id: input.champion_model_version_id,
            champion_serving_contract_hash: input.champion_serving_contract_hash,
            drift: input.drift,
            comparison: input.comparison,
            shadow: input.shadow,
        };
        params.validate()?;
        Ok(params)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.profile_ref
            .validate()
            .map_err(|error| FeedbackError::InvalidJobContract {
                detail: format!("decision profile is invalid: {error}"),
            })?;
        self.drift.validate_for(self.feedback_cycle_id)?;
        self.comparison.validate_for(self.feedback_cycle_id)?;
        self.shadow
            .validate_for(self.feedback_cycle_id, &self.shadow.binding)?;
        if FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
            != self.feedback_cycle_id
            || self.artifact_id != FeedbackDecisionArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.candidate_family_hash != self.comparison.candidate_family_hash
            || self.decision_policy_snapshot_id != self.comparison.decision_policy_snapshot_id
            || self.shadow.binding.comparison != self.comparison
        {
            return Err(FeedbackError::InvalidJobContract {
                detail: "decision identity, family, policy, or predecessor lineage is invalid"
                    .to_owned(),
            });
        }
        Ok(())
    }

    pub fn input_hash(&self) -> Result<ContentHash, FeedbackError> {
        self.validate()?;
        CanonicalDigest::content_hash_typed(DECISION_INPUT_DOMAIN, DECISION_INPUT_VERSION, self)
            .map_err(Into::into)
    }
}

/// Verified object-store result of one evidence-only Decision job.
pub struct FeedbackDecisionExecutionResult {
    pub artifact_id: FeedbackDecisionArtifactId,
    pub artifact: ResearchJobArtifactRef,
}

/// Seals an evidence-only terminal decision without route authority.
#[async_trait]
pub trait FeedbackDecisionExecutionPort: Send + Sync {
    async fn execute(
        &self,
        params: FeedbackDecisionJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackDecisionExecutionResult>;
}

fn validate_stage_identity(
    feedback_cycle_id: FeedbackCycleId,
    cycle_idempotency_hash: ContentHash,
    stage: FeedbackStage,
    artifact_id: FeedbackLearningStageArtifactId,
) -> Result<(), FeedbackError> {
    if FeedbackCycleId::from_idempotency_hash(&cycle_idempotency_hash) != feedback_cycle_id
        || FeedbackLearningStageArtifactId::from_cycle_stage(feedback_cycle_id, stage)
            != Some(artifact_id)
    {
        return Err(FeedbackError::InvalidJobContract {
            detail: format!("{stage} params do not bind their cycle hash or artifact identity"),
        });
    }
    Ok(())
}

fn validate_dataset_commands(
    commands: &[FeedbackDatasetBuildCommand],
) -> Result<(), FeedbackError> {
    if commands.len() > FEEDBACK_DATASET_MAX_COMMANDS {
        return Err(invalid_batch(format!(
            "DatasetSeal command count must not exceed {FEEDBACK_DATASET_MAX_COMMANDS}"
        )));
    }
    let mut previous = None;
    let mut dataset_ids = HashSet::new();
    let mut training = BTreeSet::new();
    let mut calibration = BTreeSet::new();
    let mut evaluation_count = 0_usize;
    for command in commands {
        command.resource_budget.validate()?;
        command.request.validate()?;
        if command.role.purpose() != command.request.purpose
            || !dataset_ids.insert(command.request.training_dataset_id)
        {
            return Err(invalid_batch(
                "DatasetSeal command has a mismatched purpose or duplicate Dataset id",
            ));
        }
        let key = command.role;
        if previous.as_ref().is_some_and(|previous| previous >= &key) {
            return Err(invalid_batch(
                "DatasetSeal commands must be canonical and unique",
            ));
        }
        previous = Some(key);
        match command.role {
            FeedbackDatasetRole::CandidateTraining {
                candidate_recipe_hash,
            } => {
                training.insert(candidate_recipe_hash);
            }
            FeedbackDatasetRole::CandidateCalibration {
                candidate_recipe_hash,
            } => {
                calibration.insert(candidate_recipe_hash);
            }
            FeedbackDatasetRole::SharedEvaluation => {
                evaluation_count = evaluation_count.saturating_add(1);
            }
        }
    }
    if evaluation_count != 1 || training.is_empty() || training != calibration {
        return Err(invalid_batch(
            "DatasetSeal requires paired candidate Training/Calibration commands and one shared Evaluation command",
        ));
    }
    Ok(())
}

fn validate_training_commands(commands: &[FeedbackTrainingCommand]) -> Result<(), FeedbackError> {
    let mut recipes = BTreeSet::new();
    let mut model_versions = HashSet::new();
    let mut model_runs = HashSet::new();
    let mut datasets = HashSet::new();
    for command in commands {
        command.resource_budget.validate()?;
        validate_reason(&command.params.request.reason, 512, "Training")?;
        if !recipes.insert(command.candidate_recipe_hash)
            || !model_versions.insert(command.params.model_version_id)
            || !model_runs.insert(command.params.model_run_id)
            || !datasets.insert(command.params.request.training_dataset_id)
        {
            return Err(invalid_batch(
                "Training commands must have unique recipe, model-version, model-run, and Dataset identities",
            ));
        }
    }
    validate_recipe_order(commands.iter().map(|command| command.candidate_recipe_hash))
}

fn validate_calibration_commands(
    commands: &[FeedbackCalibrationCommand],
) -> Result<(), FeedbackError> {
    let mut recipes = BTreeSet::new();
    let mut models = HashSet::new();
    let mut model_runs = HashSet::new();
    let mut datasets = HashSet::new();
    for command in commands {
        command.resource_budget.validate()?;
        validate_reason(
            &command.params.request.reason,
            512,
            "Calibration derivation",
        )?;
        if !recipes.insert(command.candidate_recipe_hash)
            || !models.insert(command.params.request.model_version_id)
            || !model_runs.insert(command.params.model_run_id)
            || !datasets.insert(command.params.request.calibration_dataset_id)
        {
            return Err(invalid_batch(
                "Calibration commands must have unique recipe, model, model-run, and Dataset identities",
            ));
        }
    }
    validate_recipe_order(commands.iter().map(|command| command.candidate_recipe_hash))
}

fn validate_cpcv_commands(commands: &[FeedbackCpcvCommand]) -> Result<(), FeedbackError> {
    if commands.is_empty() {
        return Ok(());
    }
    let mut recipes = BTreeSet::new();
    let mut models = HashSet::new();
    let mut model_runs = HashSet::new();
    let mut datasets = HashSet::new();
    let mut path_sets = HashSet::new();
    for command in commands {
        command.resource_budget.validate()?;
        command.cpcv_spec.validate()?;
        validate_reason(&command.params.request.reason, 512, "CPCV")?;
        let path_set_id = command.params.request.path_set_id.ok_or_else(|| {
            invalid_batch("feedback CPCV command requires a preassigned path-set id")
        })?;
        if !recipes.insert(command.candidate_recipe_hash)
            || !models.insert(command.params.model_version_id)
            || !model_runs.insert(command.params.model_run_id)
            || !datasets.insert(command.params.request.training_dataset_id)
            || !path_sets.insert(path_set_id)
        {
            return Err(invalid_batch(
                "CPCV commands must have unique recipe, model, model-run, Dataset, and path-set identities",
            ));
        }
    }
    validate_recipe_order(commands.iter().map(|command| command.candidate_recipe_hash))
}

fn validate_recipe_order(hashes: impl Iterator<Item = ContentHash>) -> Result<(), FeedbackError> {
    let mut previous = None;
    let mut count = 0_usize;
    for hash in hashes {
        if previous.is_some_and(|previous| previous >= hash) {
            return Err(invalid_batch(
                "learning-stage commands must use strictly increasing recipe hashes",
            ));
        }
        previous = Some(hash);
        count = count
            .checked_add(1)
            .ok_or_else(|| invalid_batch("learning-stage command count overflowed"))?;
        if count > FEEDBACK_LEARNING_MAX_CANDIDATES {
            return Err(invalid_batch(format!(
                "learning-stage candidate count must be in 1..={FEEDBACK_LEARNING_MAX_CANDIDATES}"
            )));
        }
    }
    if count == 0 {
        return Err(invalid_batch("learning-stage command batch is empty"));
    }
    Ok(())
}

fn validate_reason(reason: &str, maximum: usize, operation: &str) -> Result<(), FeedbackError> {
    if reason.trim().is_empty() || reason.len() > maximum {
        return Err(invalid_batch(format!(
            "{operation} reason must contain 1..={maximum} bytes"
        )));
    }
    Ok(())
}

fn invalid_batch(detail: impl Into<String>) -> FeedbackError {
    FeedbackError::InvalidJobContract {
        detail: detail.into(),
    }
}

fn learning_artifact_id(
    feedback_cycle_id: FeedbackCycleId,
    stage: FeedbackStage,
) -> Result<FeedbackLearningStageArtifactId, FeedbackError> {
    FeedbackLearningStageArtifactId::from_cycle_stage(feedback_cycle_id, stage).ok_or_else(|| {
        FeedbackError::InvalidJobIdentity {
            detail: format!("{stage} is not a learning-stage artifact owner"),
        }
    })
}

fn learning_input_hash<T: Serialize>(
    stage: FeedbackStage,
    input: &T,
) -> Result<ContentHash, FeedbackError> {
    CanonicalDigest::content_hash_typed(
        LEARNING_INPUT_DOMAIN,
        LEARNING_INPUT_VERSION,
        &(stage, input),
    )
    .map_err(Into::into)
}

/// Verified object-store result of one learning-stage batch.
pub struct FeedbackLearningExecutionResult {
    pub artifact_id: FeedbackLearningStageArtifactId,
    pub artifact: ResearchJobArtifactRef,
}

/// Executes all-cohort coverage qualification for one exact feedback cycle.
#[async_trait]
pub trait FeedbackCoverageExecutionPort: Send + Sync {
    async fn execute(
        &self,
        params: FeedbackCoverageJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackCoverageExecutionResult>;
}

/// Executes champion-relative data, concept, and label drift qualification.
#[async_trait]
pub trait FeedbackDriftExecutionPort: Send + Sync {
    async fn execute(
        &self,
        params: FeedbackDriftJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackDriftExecutionResult>;
}

/// Executes the four bounded batch jobs between Drift and Comparison.
#[async_trait]
pub trait FeedbackLearningExecutionPort: Send + Sync {
    async fn seal_datasets(
        &self,
        params: FeedbackDatasetSealJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackLearningExecutionResult>;

    async fn train(
        &self,
        params: FeedbackTrainingJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackLearningExecutionResult>;

    async fn calibrate(
        &self,
        params: FeedbackCalibrationJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackLearningExecutionResult>;

    async fn validate_cpcv(
        &self,
        params: FeedbackCpcvJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackLearningExecutionResult>;
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_error::feedback::FeedbackError;
    use serde_json::{from_value, json, to_value};
    use uuid::Uuid;

    use super::{
        FEEDBACK_LEARNING_MAX_CANDIDATES, FeedbackCalibrationCommand, FeedbackCalibrationJobParams,
        FeedbackCandidateFamily, FeedbackCandidateFamilyInput, FeedbackCandidateRecipe,
        FeedbackCandidateRecipeInput, FeedbackComparisonContract, FeedbackCpcvCommand,
        FeedbackCpcvJobParams, FeedbackDatasetBuildCommand, FeedbackDatasetBuildRequest,
        FeedbackDatasetRole, FeedbackDatasetSealJobParams, FeedbackLearningStageArtifactRef,
        FeedbackTrainingCommand, FeedbackTrainingJobParams,
    };
    use crate::{
        domain::{
            api::{
                CpcvBacktestJobParams, FitModelCalibratorRequest, ModelTrainJobParams,
                RunCpcvBacktestRequest, TrainModelRequest,
            },
            ports::{
                FeedbackRecipeCpcvSpec, FeedbackRecipeResourceBudget, GovernanceActor,
                ModelCalibrationFitJobParams,
            },
            quant::{FeedbackCohortWindow, ResearchJobArtifactRef},
        },
        enums::quant::{CalibrationMethod, DatasetPurpose, DownsideSource, FeedbackStage},
        runtime_config::ResearchValidationConfig,
        types::{
            ArtifactUri, BacktestPathSetId, CapabilityRegistryHashes, ContentHash,
            DATASET_SOURCE_LINEAGE_FORMAT_VERSION, DatasetSourceLineage, DecisionPolicySnapshotId,
            FeedbackCycleId, FeedbackLearningStageArtifactId, ModelRunId, ModelSpecId,
            ModelVersionId, ReaderContractVersion, ResearchJobId, ResearchProfileArtifactId,
            ResearchProfileId, ResearchProfileRef, SchemaContractVersion, SourceSliceId,
            SourceSliceManifestRef, TrainingDatasetId, builtin_research_profiles,
        },
    };

    fn hash(seed: u8) -> ContentHash {
        ContentHash::from_bytes([seed; 32])
    }

    fn instant(hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 1, hour, 0, 0)
            .single()
            .expect("valid fixture instant")
    }

    impl FeedbackRecipeResourceBudget {
        const fn test_fixture() -> Self {
            Self {
                max_concurrency: 1,
                max_working_set_bytes: 10 * 1024 * 1024 * 1024,
                max_resident_model_bytes: 128 * 1024 * 1024,
                deadline_secs: 300,
            }
        }
    }

    struct FeedbackExecutionFixture {
        profile: ResearchProfileRef,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        model_spec_id: ModelSpecId,
        model_spec_definition_hash: ContentHash,
    }

    impl FeedbackExecutionFixture {
        fn new() -> Self {
            Self {
                profile: ResearchProfileRef {
                    id: ResearchProfileId::new("crypto_1h"),
                    version: 1,
                    content_hash: hash(1),
                },
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                model_spec_id: ModelSpecId::from_v7(),
                model_spec_definition_hash: hash(90),
            }
        }

        fn window(&self) -> FeedbackCohortWindow {
            FeedbackCohortWindow::try_new(self.profile.clone(), instant(1), instant(5))
                .expect("valid feedback window")
        }

        fn lineage(&self) -> DatasetSourceLineage {
            self.lineage_for(instant(0), instant(5))
        }

        fn lineage_for(
            &self,
            source_window_start: DateTime<Utc>,
            cutoff: DateTime<Utc>,
        ) -> DatasetSourceLineage {
            DatasetSourceLineage {
                format_version: DATASET_SOURCE_LINEAGE_FORMAT_VERSION,
                fit_seal_id: Uuid::from_u128(16).into(),
                fit_seal_hash: hash(16),
                source_slice_id: SourceSliceId::from_v7(),
                source_slice_identity_hash: hash(2),
                research_profile_artifact_id: ResearchProfileArtifactId::from_profile_ref(
                    &self.profile,
                ),
                research_program_hash: hash(3),
                source_slice: SourceSliceManifestRef {
                    manifest_uri: ArtifactUri::parse("s3://worm/source/manifest.json")
                        .expect("valid source URI"),
                    manifest_hash: hash(4),
                },
                source_window_start,
                source_window_end: cutoff,
                pit_cutoff: cutoff,
                decision_policy_snapshot_id: self.decision_policy_snapshot_id,
                runtime_config_hash: hash(5),
                reader_contract_version: ReaderContractVersion::v1(),
                schema_contract_version: SchemaContractVersion::v1(),
                source_schema_hash: hash(6),
                capability_registry_hashes: CapabilityRegistryHashes::try_new(vec![hash(7)])
                    .expect("valid capability set"),
            }
        }

        fn dataset(
            &self,
            purpose: DatasetPurpose,
            window_start: DateTime<Utc>,
            cutoff: DateTime<Utc>,
            model_spec_id: ModelSpecId,
            model_spec_definition_hash: ContentHash,
        ) -> FeedbackDatasetBuildRequest {
            FeedbackDatasetBuildRequest {
                training_dataset_id: TrainingDatasetId::from_v7(),
                model_spec_id,
                model_spec_definition_hash,
                source_lineage: self.lineage_for(window_start, cutoff),
                window: FeedbackCohortWindow::try_new(self.profile.clone(), window_start, cutoff)
                    .expect("valid candidate Dataset window"),
                purpose,
            }
        }

        fn recipe(&self, _seed: u8) -> FeedbackCandidateRecipe {
            FeedbackCandidateRecipe::try_seal(FeedbackCandidateRecipeInput {
                recipe_template_hash: hash(91),
                planner_evidence_hash: hash(92),
                resource_budget: FeedbackRecipeResourceBudget::test_fixture(),
                training: self.dataset(
                    DatasetPurpose::Training,
                    instant(0),
                    instant(2),
                    self.model_spec_id,
                    self.model_spec_definition_hash,
                ),
                calibration: self.dataset(
                    DatasetPurpose::Calibration,
                    instant(3),
                    instant(5),
                    self.model_spec_id,
                    self.model_spec_definition_hash,
                ),
                calibration_method: CalibrationMethod::Platt,
                cpcv_spec: FeedbackRecipeCpcvSpec::try_new(
                    ResearchValidationConfig::default(),
                    3_600,
                    300,
                )
                .expect("valid CPCV recipe spec"),
                downside_source: DownsideSource::MfeMae,
                decision_policy_snapshot_id: self.decision_policy_snapshot_id,
            })
            .expect("valid candidate recipe")
        }

        fn family(
            &self,
            candidates: Vec<FeedbackCandidateRecipe>,
        ) -> Result<FeedbackCandidateFamily, FeedbackError> {
            FeedbackCandidateFamily::try_seal(FeedbackCandidateFamilyInput {
                shared_evaluation: self.dataset(
                    DatasetPurpose::Evaluation,
                    instant(6),
                    instant(8),
                    self.model_spec_id,
                    self.model_spec_definition_hash,
                ),
                comparison_contract: FeedbackComparisonContract::try_from_policy(
                    &builtin_research_profiles()
                        .map_err(|error| FeedbackError::InvalidComparisonContract {
                            detail: error,
                        })?
                        .into_iter()
                        .next()
                        .ok_or_else(|| FeedbackError::InvalidComparisonContract {
                            detail: "built-in profile registry is empty".to_owned(),
                        })?
                        .spec
                        .feedback_policy,
                )?,
                candidates,
            })
        }
    }

    fn dataset_command(
        role: FeedbackDatasetRole,
        purpose: DatasetPurpose,
    ) -> FeedbackDatasetBuildCommand {
        let fixture = FeedbackExecutionFixture::new();
        FeedbackDatasetBuildCommand {
            role,
            resource_budget: FeedbackRecipeResourceBudget::test_fixture(),
            request: FeedbackDatasetBuildRequest {
                training_dataset_id: TrainingDatasetId::from_v7(),
                model_spec_id: ModelSpecId::from_v7(),
                model_spec_definition_hash: hash(8),
                source_lineage: fixture.lineage(),
                window: fixture.window(),
                purpose,
            },
        }
    }

    fn dataset_commands() -> Vec<FeedbackDatasetBuildCommand> {
        let first = hash(10);
        let second = hash(11);
        vec![
            dataset_command(
                FeedbackDatasetRole::CandidateTraining {
                    candidate_recipe_hash: first,
                },
                DatasetPurpose::Training,
            ),
            dataset_command(
                FeedbackDatasetRole::CandidateTraining {
                    candidate_recipe_hash: second,
                },
                DatasetPurpose::Training,
            ),
            dataset_command(
                FeedbackDatasetRole::CandidateCalibration {
                    candidate_recipe_hash: first,
                },
                DatasetPurpose::Calibration,
            ),
            dataset_command(
                FeedbackDatasetRole::CandidateCalibration {
                    candidate_recipe_hash: second,
                },
                DatasetPurpose::Calibration,
            ),
            dataset_command(
                FeedbackDatasetRole::SharedEvaluation,
                DatasetPurpose::Evaluation,
            ),
        ]
    }

    fn stage_ref(
        cycle_id: FeedbackCycleId,
        stage: FeedbackStage,
    ) -> FeedbackLearningStageArtifactRef {
        FeedbackLearningStageArtifactRef {
            feedback_cycle_id: cycle_id,
            stage,
            job_id: ResearchJobId::from_v7(),
            artifact_id: FeedbackLearningStageArtifactId::from_cycle_stage(cycle_id, stage)
                .expect("learning stage must own an artifact"),
            input_hash: hash(20),
            artifact: ResearchJobArtifactRef {
                uri: ArtifactUri::parse(format!("s3://worm/feedback/{}.json", stage.as_str()))
                    .expect("valid feedback URI"),
                content_hash: hash(21),
            },
        }
    }

    fn training_command(seed: u8) -> FeedbackTrainingCommand {
        FeedbackTrainingCommand {
            candidate_recipe_hash: hash(seed),
            resource_budget: FeedbackRecipeResourceBudget::test_fixture(),
            params: ModelTrainJobParams {
                model_version_id: ModelVersionId::from_v7(),
                model_run_id: ModelRunId::from_v7(),
                request: TrainModelRequest {
                    training_dataset_id: TrainingDatasetId::from_v7(),
                    reason: "feedback training".to_owned(),
                },
            },
        }
    }

    fn calibration_command(seed: u8) -> FeedbackCalibrationCommand {
        FeedbackCalibrationCommand {
            candidate_recipe_hash: hash(seed),
            resource_budget: FeedbackRecipeResourceBudget::test_fixture(),
            params: ModelCalibrationFitJobParams {
                model_run_id: ModelRunId::from_v7(),
                request: FitModelCalibratorRequest {
                    model_version_id: ModelVersionId::from_v7(),
                    calibration_dataset_id: TrainingDatasetId::from_v7(),
                    method: CalibrationMethod::Platt,
                    reason: "feedback calibration".to_owned(),
                },
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                downside_source: DownsideSource::MfeMae,
                actor: GovernanceActor::system(),
            },
        }
    }

    fn cpcv_command(seed: u8) -> FeedbackCpcvCommand {
        FeedbackCpcvCommand {
            candidate_recipe_hash: hash(seed),
            resource_budget: FeedbackRecipeResourceBudget::test_fixture(),
            cpcv_spec: FeedbackRecipeCpcvSpec::try_new(
                ResearchValidationConfig::default(),
                3_600,
                300,
            )
            .expect("valid CPCV command spec"),
            params: CpcvBacktestJobParams {
                model_version_id: ModelVersionId::from_v7(),
                model_run_id: ModelRunId::from_v7(),
                request: RunCpcvBacktestRequest {
                    training_dataset_id: TrainingDatasetId::from_v7(),
                    decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                    reason: "feedback CPCV".to_owned(),
                    path_set_id: Some(BacktestPathSetId::from_v7()),
                },
            },
        }
    }

    #[test]
    fn stage_params_bind_inputs() {
        let cycle_hash = hash(30);
        let cycle_id = FeedbackCycleId::from_idempotency_hash(&cycle_hash);
        let family_hash = hash(31);
        let dataset = FeedbackDatasetSealJobParams::try_new(
            cycle_id,
            cycle_hash,
            family_hash,
            dataset_commands(),
        )
        .expect("canonical DatasetSeal params");
        assert_eq!(
            dataset.input_hash().expect("DatasetSeal input hash"),
            dataset.input_hash().expect("stable DatasetSeal input hash")
        );

        let training = FeedbackTrainingJobParams::try_new(
            cycle_id,
            cycle_hash,
            family_hash,
            stage_ref(cycle_id, FeedbackStage::DatasetSeal),
            vec![training_command(10), training_command(11)],
        )
        .expect("canonical Training params");
        let calibration = FeedbackCalibrationJobParams::try_new(
            cycle_id,
            cycle_hash,
            family_hash,
            stage_ref(cycle_id, FeedbackStage::Training),
            vec![calibration_command(10), calibration_command(11)],
        )
        .expect("canonical Calibration params");
        let cpcv = FeedbackCpcvJobParams::try_new(
            cycle_id,
            cycle_hash,
            family_hash,
            stage_ref(cycle_id, FeedbackStage::Calibration),
            vec![cpcv_command(10), cpcv_command(11)],
        )
        .expect("canonical CPCV params");

        assert_ne!(
            training.input_hash().expect("Training input hash"),
            calibration.input_hash().expect("Calibration input hash")
        );
        assert_ne!(
            calibration.input_hash().expect("Calibration input hash"),
            cpcv.input_hash().expect("CPCV input hash")
        );
        let bytes = serde_json::to_vec(&cpcv).expect("serialize CPCV params");
        let decoded =
            serde_json::from_slice::<FeedbackCpcvJobParams>(&bytes).expect("decode CPCV params");
        assert_eq!(decoded, cpcv);
    }

    #[test]
    fn candidate_family_freezes_recipes() {
        let fixture = FeedbackExecutionFixture::new();
        let family = fixture
            .family(vec![fixture.recipe(11), fixture.recipe(10)])
            .expect("seal canonical family");
        family.validate().expect("validate candidate family");
        assert_eq!(family.candidates().len(), 2);
        assert!(
            family.candidates()[0].candidate_recipe_hash()
                < family.candidates()[1].candidate_recipe_hash()
        );
        assert_eq!(
            family
                .candidate(family.candidates()[1].candidate_recipe_hash())
                .expect("lookup recipe"),
            &family.candidates()[1]
        );
        let value = to_value(&family).expect("serialize candidate family");
        let decoded = from_value::<FeedbackCandidateFamily>(value).expect("decode exact family");
        assert_eq!(decoded, family);
        assert_eq!(
            decoded.candidate_family_hash(),
            family.candidate_family_hash()
        );
    }

    #[test]
    fn candidate_family_rejects_tamper() {
        let fixture = FeedbackExecutionFixture::new();
        let family = fixture
            .family(vec![fixture.recipe(10)])
            .expect("seal candidate family");
        let mut value = to_value(family).expect("serialize candidate family");
        value["candidates"][0]["calibration_method"] = json!("isotonic");
        assert!(from_value::<FeedbackCandidateFamily>(value).is_err());

        let family = fixture
            .family(vec![fixture.recipe(10)])
            .expect("seal budget-bound candidate family");
        let mut value = to_value(family).expect("serialize budget-bound family");
        value["candidates"][0]["resource_budget"]["deadline_secs"] = json!(301);
        assert!(from_value::<FeedbackCandidateFamily>(value).is_err());
    }

    #[test]
    fn candidate_family_rejects_bounds() {
        let fixture = FeedbackExecutionFixture::new();
        assert!(fixture.family(Vec::new()).is_err());
        let oversize = u8::try_from(FEEDBACK_LEARNING_MAX_CANDIDATES + 1)
            .expect("feedback candidate bound fits fixture seed");
        assert!(
            fixture
                .family((1_u8..=oversize).map(|seed| fixture.recipe(seed)).collect())
                .is_err()
        );
    }

    #[test]
    fn batches_reject_drift() {
        let cycle_hash = hash(40);
        let cycle_id = FeedbackCycleId::from_idempotency_hash(&cycle_hash);
        let family_hash = hash(41);

        let mut reordered = dataset_commands();
        reordered.swap(0, 1);
        assert!(
            FeedbackDatasetSealJobParams::try_new(cycle_id, cycle_hash, family_hash, reordered,)
                .is_err()
        );

        let mut wrong_purpose = dataset_commands();
        wrong_purpose[0].request.purpose = DatasetPurpose::Calibration;
        assert!(
            FeedbackDatasetSealJobParams::try_new(
                cycle_id,
                cycle_hash,
                family_hash,
                wrong_purpose,
            )
            .is_err()
        );

        let first = training_command(10);
        let mut duplicate = training_command(11);
        duplicate.params.model_version_id = first.params.model_version_id;
        assert!(
            FeedbackTrainingJobParams::try_new(
                cycle_id,
                cycle_hash,
                family_hash,
                stage_ref(cycle_id, FeedbackStage::DatasetSeal),
                vec![first, duplicate],
            )
            .is_err()
        );

        let wrong_previous = stage_ref(cycle_id, FeedbackStage::DatasetSeal);
        assert!(
            FeedbackCalibrationJobParams::try_new(
                cycle_id,
                cycle_hash,
                family_hash,
                wrong_previous,
                vec![calibration_command(10)],
            )
            .is_err()
        );

        let mut missing_path = cpcv_command(10);
        missing_path.params.request.path_set_id = None;
        assert!(
            FeedbackCpcvJobParams::try_new(
                cycle_id,
                cycle_hash,
                family_hash,
                stage_ref(cycle_id, FeedbackStage::Calibration),
                vec![missing_path],
            )
            .is_err()
        );

        let mut seven_paths = cpcv_command(10);
        seven_paths.cpcv_spec.validation.cpcv.k_test = 2;
        assert!(
            FeedbackCpcvJobParams::try_new(
                cycle_id,
                cycle_hash,
                family_hash,
                stage_ref(cycle_id, FeedbackStage::Calibration),
                vec![seven_paths],
            )
            .is_err()
        );
    }

    #[test]
    fn batches_reject_oversize() {
        let cycle_hash = hash(50);
        let cycle_id = FeedbackCycleId::from_idempotency_hash(&cycle_hash);
        let family_hash = hash(51);
        let oversize = u8::try_from(FEEDBACK_LEARNING_MAX_CANDIDATES + 1)
            .expect("feedback candidate bound fits fixture seed");
        let recipes = (1_u8..=oversize).map(hash).collect::<Vec<_>>();
        let mut datasets = recipes
            .iter()
            .copied()
            .map(|candidate_recipe_hash| {
                dataset_command(
                    FeedbackDatasetRole::CandidateTraining {
                        candidate_recipe_hash,
                    },
                    DatasetPurpose::Training,
                )
            })
            .collect::<Vec<_>>();
        datasets.extend(recipes.iter().copied().map(|candidate_recipe_hash| {
            dataset_command(
                FeedbackDatasetRole::CandidateCalibration {
                    candidate_recipe_hash,
                },
                DatasetPurpose::Calibration,
            )
        }));
        datasets.push(dataset_command(
            FeedbackDatasetRole::SharedEvaluation,
            DatasetPurpose::Evaluation,
        ));
        assert!(
            FeedbackDatasetSealJobParams::try_new(cycle_id, cycle_hash, family_hash, datasets,)
                .is_err()
        );
        assert!(
            FeedbackTrainingJobParams::try_new(
                cycle_id,
                cycle_hash,
                family_hash,
                stage_ref(cycle_id, FeedbackStage::DatasetSeal),
                (1_u8..=oversize).map(training_command).collect(),
            )
            .is_err()
        );
        assert!(
            FeedbackCalibrationJobParams::try_new(
                cycle_id,
                cycle_hash,
                family_hash,
                stage_ref(cycle_id, FeedbackStage::Training),
                (1_u8..=oversize).map(calibration_command).collect(),
            )
            .is_err()
        );
        assert!(
            FeedbackCpcvJobParams::try_new(
                cycle_id,
                cycle_hash,
                family_hash,
                stage_ref(cycle_id, FeedbackStage::Calibration),
                (1_u8..=oversize).map(cpcv_command).collect(),
            )
            .is_err()
        );
    }
}
