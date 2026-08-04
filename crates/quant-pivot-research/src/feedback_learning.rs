//! Versioned content-addressed evidence for the four feedback learning stages.

use std::collections::{BTreeSet, HashSet};

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        ports::{FeedbackDatasetRole, FeedbackLearningStageArtifactRef},
        quant::{ResearchJobArtifactRef, ResearchJobInfo},
    },
    enums::quant::{CalibrationMethod, DatasetPurpose, FeedbackStage},
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, BacktestPathSetId, CalibrationArtifactId, ContentHash, FeedbackCycleId,
        FeedbackLearningStageArtifactId, ModelRunId, ModelVersionId, ResearchJobId,
        TrainingDatasetId,
    },
};
use serde::{Deserialize, Serialize};

/// Breaking schema version for learning-stage evidence.
pub const FEEDBACK_LEARNING_ARTIFACT_FORMAT_VERSION: u32 = 1;
const LEARNING_SCHEMA_DOMAIN: &str = "quant-pivot/feedback-learning-stage-schema";

/// One Dataset sealed for a candidate recipe or the shared evaluation holdout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackDatasetStageResult {
    pub role: FeedbackDatasetRole,
    pub training_dataset_id: TrainingDatasetId,
    pub purpose: DatasetPurpose,
    pub dataset_hash: ContentHash,
    pub manifest_hash: ContentHash,
    pub artifact_bytes_hash: ContentHash,
    pub parquet_uri: ArtifactUri,
    pub cohort_manifest_hash: ContentHash,
    pub sample_count: u64,
}

/// One trained candidate and the exact transform input commitment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackTrainingStageResult {
    pub candidate_recipe_hash: ContentHash,
    pub model_version_id: ModelVersionId,
    pub model_run_id: ModelRunId,
    pub training_dataset_id: TrainingDatasetId,
    pub model_artifact_hash: ContentHash,
    pub serving_contract_hash: ContentHash,
    pub training_input_hash: ContentHash,
}

/// Terminal candidate outcome of the calibration stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum FeedbackCalibrationStageResult {
    Calibrated {
        candidate_recipe_hash: ContentHash,
        source_model_version_id: ModelVersionId,
        model_run_id: ModelRunId,
        calibration_dataset_id: TrainingDatasetId,
        method: CalibrationMethod,
        calibration_artifact_id: CalibrationArtifactId,
        calibration_artifact_hash: ContentHash,
        calibrated_model_version_id: ModelVersionId,
        calibrated_model_artifact_hash: ContentHash,
        calibrated_serving_contract_hash: ContentHash,
        training_input_hash: ContentHash,
        sample_count: u64,
    },
    Insufficient {
        candidate_recipe_hash: ContentHash,
        source_model_version_id: ModelVersionId,
        model_run_id: ModelRunId,
        calibration_dataset_id: TrainingDatasetId,
        method: CalibrationMethod,
        sample_count: u64,
        total_sample_count: u64,
        minimum_sample_count: u64,
        outcome_hash: ContentHash,
    },
}

impl FeedbackCalibrationStageResult {
    #[must_use]
    pub const fn candidate_recipe_hash(&self) -> ContentHash {
        match self {
            Self::Calibrated {
                candidate_recipe_hash,
                ..
            }
            | Self::Insufficient {
                candidate_recipe_hash,
                ..
            } => *candidate_recipe_hash,
        }
    }
}

/// Terminal CPCV outcome for every recipe admitted to the feedback trial universe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum FeedbackCpcvStageResult {
    Evaluated {
        candidate_recipe_hash: ContentHash,
        model_version_id: ModelVersionId,
        training_dataset_id: TrainingDatasetId,
        path_set_id: BacktestPathSetId,
        model_run_id: ModelRunId,
        path_set_hash: ContentHash,
    },
    CalibrationInsufficient {
        candidate_recipe_hash: ContentHash,
        source_model_version_id: ModelVersionId,
        model_run_id: ModelRunId,
        calibration_dataset_id: TrainingDatasetId,
        method: CalibrationMethod,
        sample_count: u64,
        total_sample_count: u64,
        minimum_sample_count: u64,
        outcome_hash: ContentHash,
    },
}

impl FeedbackCpcvStageResult {
    #[must_use]
    pub const fn candidate_recipe_hash(&self) -> ContentHash {
        match self {
            Self::Evaluated {
                candidate_recipe_hash,
                ..
            }
            | Self::CalibrationInsufficient {
                candidate_recipe_hash,
                ..
            } => *candidate_recipe_hash,
        }
    }

    #[must_use]
    pub const fn model_version_id(&self) -> ModelVersionId {
        match self {
            Self::Evaluated {
                model_version_id, ..
            } => *model_version_id,
            Self::CalibrationInsufficient {
                source_model_version_id,
                ..
            } => *source_model_version_id,
        }
    }
}

/// Stage-specific result body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "stage", content = "results", rename_all = "snake_case")]
pub enum FeedbackLearningStageResults {
    DatasetSeal(Vec<FeedbackDatasetStageResult>),
    Training(Vec<FeedbackTrainingStageResult>),
    Calibration(Vec<FeedbackCalibrationStageResult>),
    Cpcv(Vec<FeedbackCpcvStageResult>),
}

impl FeedbackLearningStageResults {
    #[must_use]
    pub const fn stage(&self) -> FeedbackStage {
        match self {
            Self::DatasetSeal(_) => FeedbackStage::DatasetSeal,
            Self::Training(_) => FeedbackStage::Training,
            Self::Calibration(_) => FeedbackStage::Calibration,
            Self::Cpcv(_) => FeedbackStage::Cpcv,
        }
    }

    const fn is_empty(&self) -> bool {
        match self {
            Self::DatasetSeal(results) => results.is_empty(),
            Self::Training(results) => results.is_empty(),
            Self::Calibration(results) => results.is_empty(),
            Self::Cpcv(results) => results.is_empty(),
        }
    }
}

/// Complete immutable output of one feedback learning stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedbackLearningStageArtifact {
    pub format_version: u32,
    pub artifact_id: FeedbackLearningStageArtifactId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub input_hash: ContentHash,
    pub previous: Option<FeedbackLearningStageArtifactRef>,
    pub results: FeedbackLearningStageResults,
}

impl FeedbackLearningStageArtifact {
    pub fn try_new(
        feedback_cycle_id: FeedbackCycleId,
        cycle_idempotency_hash: ContentHash,
        candidate_family_hash: ContentHash,
        input_hash: ContentHash,
        previous: Option<FeedbackLearningStageArtifactRef>,
        results: FeedbackLearningStageResults,
    ) -> QuantResult<Self> {
        let stage = results.stage();
        let artifact_id =
            FeedbackLearningStageArtifactId::from_cycle_stage(feedback_cycle_id, stage)
                .ok_or_else(|| contract("invalid learning-stage artifact owner"))?;
        let artifact = Self {
            format_version: FEEDBACK_LEARNING_ARTIFACT_FORMAT_VERSION,
            artifact_id,
            feedback_cycle_id,
            cycle_idempotency_hash,
            candidate_family_hash,
            input_hash,
            previous,
            results,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> QuantResult<()> {
        let stage = self.results.stage();
        if self.format_version != FEEDBACK_LEARNING_ARTIFACT_FORMAT_VERSION
            || FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
                != self.feedback_cycle_id
            || FeedbackLearningStageArtifactId::from_cycle_stage(self.feedback_cycle_id, stage)
                != Some(self.artifact_id)
            || self.results.is_empty()
        {
            return Err(contract(
                "learning-stage artifact version, identity, or result cardinality is invalid",
            ));
        }
        self.validate_previous(stage)?;
        match &self.results {
            FeedbackLearningStageResults::DatasetSeal(results) => validate_dataset_results(results),
            FeedbackLearningStageResults::Training(results) => validate_training_results(results),
            FeedbackLearningStageResults::Calibration(results) => {
                validate_calibration_results(results)
            }
            FeedbackLearningStageResults::Cpcv(results) => validate_cpcv_results(results),
        }
    }

    fn validate_previous(&self, stage: FeedbackStage) -> QuantResult<()> {
        let expected = match stage {
            FeedbackStage::DatasetSeal => None,
            FeedbackStage::Training => Some(FeedbackStage::DatasetSeal),
            FeedbackStage::Calibration => Some(FeedbackStage::Training),
            FeedbackStage::Cpcv => Some(FeedbackStage::Calibration),
            _ => return Err(contract("artifact carries a non-learning feedback stage")),
        };
        match (expected, self.previous.as_ref()) {
            (None, None) => Ok(()),
            (Some(expected), Some(previous)) => previous
                .validate_for(self.feedback_cycle_id, expected)
                .map_err(Into::into),
            _ => Err(contract(
                "learning-stage artifact has no exact predecessor binding",
            )),
        }
    }

    pub fn reference(
        &self,
        job_id: ResearchJobId,
        artifact: ResearchJobArtifactRef,
    ) -> QuantResult<FeedbackLearningStageArtifactRef> {
        self.validate()?;
        Ok(FeedbackLearningStageArtifactRef {
            feedback_cycle_id: self.feedback_cycle_id,
            stage: self.results.stage(),
            job_id,
            artifact_id: self.artifact_id,
            input_hash: self.input_hash,
            artifact,
        })
    }

    pub fn reference_from_job(
        &self,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackLearningStageArtifactRef> {
        let artifact = job.result_artifact().ok_or_else(|| {
            contract("succeeded learning-stage job has no terminal artifact reference")
        })?;
        self.reference(job.job_id, artifact)
    }
}

/// Canonical JSON codec for all four learning-stage artifacts.
pub struct FeedbackLearningStageCodec;

impl FeedbackLearningStageCodec {
    pub fn encode(artifact: &FeedbackLearningStageArtifact) -> QuantResult<Vec<u8>> {
        artifact.validate()?;
        CanonicalDigest::canonical_json_bytes(artifact).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> QuantResult<FeedbackLearningStageArtifact> {
        let artifact =
            serde_json::from_slice::<FeedbackLearningStageArtifact>(bytes).map_err(|error| {
                ResearchError::Serialization {
                    detail: format!("decode feedback learning-stage artifact: {error}"),
                }
            })?;
        artifact.validate()?;
        if Self::encode(&artifact)? != bytes {
            return Err(ResearchError::Serialization {
                detail: "feedback learning-stage artifact is not canonical JSON".to_owned(),
            }
            .into());
        }
        Ok(artifact)
    }

    pub fn schema_hash() -> QuantResult<ContentHash> {
        CanonicalDigest::content_hash_typed(
            LEARNING_SCHEMA_DOMAIN,
            FEEDBACK_LEARNING_ARTIFACT_FORMAT_VERSION,
            &[
                "identity",
                "candidate_family",
                "input_hash",
                "previous_artifact",
                "dataset_results",
                "training_results",
                "calibration_results",
                "cpcv_results",
            ],
        )
        .map_err(Into::into)
    }
}

fn validate_dataset_results(results: &[FeedbackDatasetStageResult]) -> QuantResult<()> {
    let mut evaluation = 0_usize;
    let mut training = BTreeSet::new();
    let mut calibration = BTreeSet::new();
    let mut dataset_ids = HashSet::new();
    let mut previous = None;
    for result in results {
        if result.purpose != result.role.purpose()
            || result.sample_count == 0
            || !dataset_ids.insert(result.training_dataset_id)
        {
            return Err(contract(
                "DatasetSeal result has a mismatched role, purpose, duplicate identity, or empty artifact",
            ));
        }
        let key = result.role;
        if previous.as_ref().is_some_and(|previous| previous >= &key) {
            return Err(contract("DatasetSeal results are not canonical and unique"));
        }
        previous = Some(key);
        match result.role {
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
                evaluation = evaluation.saturating_add(1);
            }
        }
    }
    if evaluation != 1 || training.is_empty() || training != calibration {
        return Err(contract(
            "DatasetSeal requires paired candidate Training/Calibration Datasets and one shared Evaluation Dataset",
        ));
    }
    Ok(())
}

fn validate_training_results(results: &[FeedbackTrainingStageResult]) -> QuantResult<()> {
    validate_recipe_order(results.iter().map(|result| result.candidate_recipe_hash))?;
    let mut models = HashSet::new();
    let mut runs = HashSet::new();
    let mut datasets = HashSet::new();
    if results.iter().any(|result| {
        !models.insert(result.model_version_id)
            || !runs.insert(result.model_run_id)
            || !datasets.insert(result.training_dataset_id)
    }) {
        return Err(contract(
            "Training results reuse a model-version, model-run, or Dataset identity",
        ));
    }
    Ok(())
}

fn validate_calibration_results(results: &[FeedbackCalibrationStageResult]) -> QuantResult<()> {
    validate_recipe_order(
        results
            .iter()
            .map(FeedbackCalibrationStageResult::candidate_recipe_hash),
    )?;
    let mut source_models = HashSet::new();
    let mut runs = HashSet::new();
    let mut datasets = HashSet::new();
    let mut artifacts = HashSet::new();
    let mut calibrated_models = HashSet::new();
    let mut outcome_hashes = HashSet::new();
    for result in results {
        let (source_model, run, dataset) = match result {
            FeedbackCalibrationStageResult::Calibrated {
                source_model_version_id,
                model_run_id,
                calibration_dataset_id,
                calibration_artifact_id,
                calibrated_model_version_id,
                calibration_artifact_hash,
                sample_count,
                ..
            } => {
                if *sample_count == 0
                    || !artifacts.insert(*calibration_artifact_id)
                    || !calibrated_models.insert(*calibrated_model_version_id)
                    || !outcome_hashes.insert(*calibration_artifact_hash)
                {
                    return Err(contract(
                        "Calibrated result has no samples or reuses a derived artifact identity",
                    ));
                }
                (
                    *source_model_version_id,
                    *model_run_id,
                    *calibration_dataset_id,
                )
            }
            FeedbackCalibrationStageResult::Insufficient {
                source_model_version_id,
                model_run_id,
                calibration_dataset_id,
                sample_count,
                total_sample_count,
                minimum_sample_count,
                outcome_hash,
                ..
            } => {
                if *minimum_sample_count == 0
                    || sample_count >= minimum_sample_count
                    || sample_count > total_sample_count
                    || !outcome_hashes.insert(*outcome_hash)
                {
                    return Err(contract(
                        "Insufficient result does not prove a unique unmet sample floor",
                    ));
                }
                (
                    *source_model_version_id,
                    *model_run_id,
                    *calibration_dataset_id,
                )
            }
        };
        if !source_models.insert(source_model) || !runs.insert(run) || !datasets.insert(dataset) {
            return Err(contract(
                "Calibration results reuse a source-model, model-run, or Dataset identity",
            ));
        }
    }
    Ok(())
}

fn validate_cpcv_results(results: &[FeedbackCpcvStageResult]) -> QuantResult<()> {
    validate_recipe_order(
        results
            .iter()
            .map(FeedbackCpcvStageResult::candidate_recipe_hash),
    )?;
    let mut models = HashSet::new();
    let mut runs = HashSet::new();
    let mut datasets = HashSet::new();
    let mut path_sets = HashSet::new();
    let mut outcome_hashes = HashSet::new();
    for result in results {
        let (model, run, dataset) = match result {
            FeedbackCpcvStageResult::Evaluated {
                model_version_id,
                training_dataset_id,
                path_set_id,
                model_run_id,
                ..
            } => {
                if !path_sets.insert(*path_set_id) {
                    return Err(contract("CPCV results reuse a path-set identity"));
                }
                (*model_version_id, *model_run_id, *training_dataset_id)
            }
            FeedbackCpcvStageResult::CalibrationInsufficient {
                source_model_version_id,
                model_run_id,
                calibration_dataset_id,
                sample_count,
                total_sample_count,
                minimum_sample_count,
                outcome_hash,
                ..
            } => {
                if *minimum_sample_count == 0
                    || sample_count >= minimum_sample_count
                    || sample_count > total_sample_count
                    || !outcome_hashes.insert(*outcome_hash)
                {
                    return Err(contract(
                        "CPCV ineligible result does not prove a unique unmet calibration sample floor",
                    ));
                }
                (
                    *source_model_version_id,
                    *model_run_id,
                    *calibration_dataset_id,
                )
            }
        };
        if !models.insert(model) || !runs.insert(run) || !datasets.insert(dataset) {
            return Err(contract(
                "CPCV results reuse a model, model-run, or Dataset identity",
            ));
        }
    }
    Ok(())
}

fn validate_recipe_order(hashes: impl Iterator<Item = ContentHash>) -> QuantResult<()> {
    let mut previous = None;
    let mut count = 0_usize;
    for hash in hashes {
        if previous.is_some_and(|previous| previous >= hash) {
            return Err(contract(
                "learning-stage candidate results are not canonical and unique",
            ));
        }
        previous = Some(hash);
        count = count.saturating_add(1);
    }
    if count == 0 {
        return Err(contract("learning-stage candidate result batch is empty"));
    }
    Ok(())
}

fn contract(detail: impl Into<String>) -> QuantError {
    ResearchError::DatasetPlan {
        detail: detail.into(),
    }
    .into()
}
