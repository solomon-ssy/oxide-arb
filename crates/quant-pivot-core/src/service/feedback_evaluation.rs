//! Pre-open reservation of one reusable Evaluation holdout.

use std::sync::Arc;

use quant_pivot_error::{QuantError, QuantResult, feedback::FeedbackError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        ports::FeedbackLearningStageArtifactRef,
        quant::{FeedbackEvaluationUseInput, NewFeedbackEvaluationUse},
    },
    enums::quant::{DatasetPurpose, FeedbackStage, TrainingDatasetStatus},
    hashing::CanonicalDigest,
};
use quant_pivot_repository::traits::{
    FeedbackCycleLeaseGuard, FeedbackCycleRepository, FeedbackEvaluationWriteOutcome,
    TrainingDatasetRepository,
};
use quant_pivot_research::feedback_learning::FeedbackLearningStageResults;

use crate::service::feedback_learning_stage::FeedbackLearningStageAdapter;

/// Dependencies for [`FeedbackEvaluationReservationService`].
pub struct FeedbackEvaluationReservationDeps {
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub datasets: Arc<dyn TrainingDatasetRepository>,
    pub learning_stages: Arc<FeedbackLearningStageAdapter>,
}

/// Single authority that consumes an unseen holdout before its Parquet bytes open.
pub struct FeedbackEvaluationReservationService {
    cycles: Arc<dyn FeedbackCycleRepository>,
    datasets: Arc<dyn TrainingDatasetRepository>,
    learning_stages: Arc<FeedbackLearningStageAdapter>,
}

impl FeedbackEvaluationReservationService {
    #[must_use]
    pub fn new(deps: FeedbackEvaluationReservationDeps) -> Self {
        Self {
            cycles: deps.cycles,
            datasets: deps.datasets,
            learning_stages: deps.learning_stages,
        }
    }

    /// Verify the exact CPCV chain and atomically reserve its frozen Evaluation Dataset.
    ///
    /// This method reads Dataset metadata and the CPCV object only. Evaluation
    /// Parquet bytes must not be opened until this call returns a durable row.
    pub async fn reserve(
        &self,
        lease: FeedbackCycleLeaseGuard,
        cpcv: FeedbackLearningStageArtifactRef,
    ) -> QuantResult<FeedbackEvaluationWriteOutcome> {
        cpcv.validate_for(lease.feedback_cycle_id, FeedbackStage::Cpcv)?;
        let cycle = self
            .cycles
            .find_cycle(&lease.feedback_cycle_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found("quant_feedback_cycle", lease.feedback_cycle_id)
            })?;
        cycle.validate()?;
        let artifact = self.learning_stages.verify_reference(&cycle, &cpcv).await?;
        if !matches!(artifact.results, FeedbackLearningStageResults::Cpcv(_)) {
            return Err(Self::invalid(
                "evaluation reservation requires the exact CPCV terminal artifact",
            ));
        }

        let family = self.learning_stages.family(&cycle).await?;
        let plan = family.shared_evaluation();
        let dataset = self
            .datasets
            .find_by_id(&plan.training_dataset_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found("quant_training_dataset", plan.training_dataset_id)
            })?;
        let materialization = dataset.materialization().ok_or_else(|| {
            Self::invalid("Evaluation Dataset has no complete validated materialization")
        })?;
        let cohort_manifest = dataset.cohort_manifest.as_ref().ok_or_else(|| {
            Self::invalid("Evaluation Dataset has no frozen feedback cohort manifest")
        })?;
        cohort_manifest
            .validate()
            .map_err(|error| Self::invalid(format!("invalid Evaluation cohort: {error}")))?;
        if dataset.status != TrainingDatasetStatus::Ready
            || dataset.purpose != DatasetPurpose::Evaluation
            || dataset.training_dataset_id != plan.training_dataset_id
            || dataset.model_spec_id != plan.model_spec_id
            || dataset.model_spec_definition_hash != plan.model_spec_definition_hash
            || dataset.source_lineage != plan.source_lineage
            || dataset.window_start != plan.window.window_start()
            || dataset.window_end != plan.window.cutoff()
            || cohort_manifest.window != plan.window
            || cohort_manifest.capability_registry_hashes
                != plan.source_lineage.capability_registry_hashes
        {
            return Err(Self::invalid(
                "Evaluation Dataset differs from the cycle-frozen shared holdout plan",
            ));
        }
        let cohort_manifest_hash = CanonicalDigest::content_hash_json(cohort_manifest)?;
        let reservation = NewFeedbackEvaluationUse::try_seal(FeedbackEvaluationUseInput {
            feedback_cycle_id: cycle.feedback_cycle_id,
            profile_ref: cycle.profile_ref,
            evaluation_dataset_id: dataset.training_dataset_id,
            evaluation_dataset_hash: *materialization.dataset_hash,
            evaluation_artifact_bytes_hash: *materialization.artifact_bytes_hash,
            cohort_manifest_hash,
            evaluation_window_start: dataset.window_start,
            evaluation_window_end: dataset.window_end,
            label_cutoff: cycle.label_cutoff,
            champion_model_version_id: cycle.champion_model_version_id,
            champion_serving_contract_hash: cycle.champion_serving_contract_hash,
            candidate_family_hash: family.candidate_family_hash(),
            comparison_contract_hash: family.comparison_contract_hash(),
            cpcv_artifact_uri: cpcv.artifact.uri,
            cpcv_artifact_hash: cpcv.artifact.content_hash,
        })?;
        self.cycles
            .append_evaluation(lease, reservation)
            .await
            .map_err(Into::into)
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidEvaluationUse {
            detail: detail.into(),
        }
        .into()
    }
}
