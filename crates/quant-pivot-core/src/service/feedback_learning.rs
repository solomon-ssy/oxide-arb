//! Durable execution for feedback-owned Dataset, training, calibration, and CPCV batches.

use std::{collections::BTreeMap, future::Future, sync::Arc, time::Duration};

use async_trait::async_trait;
use quant_pivot_compute::{ComputeExecutor, OFFLINE_MEMORY_BYTES, OfflineMemory};
use quant_pivot_error::{
    QuantError, QuantResult, feedback::FeedbackError, research::ResearchError,
    storage::StorageError,
};
use quant_pivot_models::{
    domain::{
        ports::{
            CalibratedModelSealCommand, CalibrationArtifactFitPort, CpcvBacktestPort,
            FeedbackCalibrationCommand, FeedbackCalibrationJobParams, FeedbackCpcvJobParams,
            FeedbackDatasetBuildCommand, FeedbackDatasetRole, FeedbackDatasetSealJobParams,
            FeedbackLearningExecutionPort, FeedbackLearningExecutionResult,
            FeedbackLearningStageArtifactRef, FeedbackRecipeResourceBudget,
            FeedbackTrainingJobParams, ModelCalibrationFitOutcome, ModelCalibrationFitPort,
            ModelGovernancePort, ModelTrainingPort, TrainingDatasetPort,
        },
        quant::{JobProgressSink, ModelVersionInfo, ResearchJobArtifactRef, TrainingDatasetInfo},
    },
    enums::quant::{CalibrationMethod, DatasetPurpose, FeedbackStage, TrainingDatasetStatus},
    hashing::CanonicalDigest,
    types::{
        ContentHash, ModelRunId, ModelVersionId, ResearchJobProgress, TrainingDatasetId,
        calibration::MonotoneMapping, model_lineage::ModelVersionDerivation,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    feedback_learning::{
        FeedbackCalibrationStageResult, FeedbackCpcvStageResult, FeedbackDatasetStageResult,
        FeedbackLearningStageArtifact, FeedbackLearningStageCodec, FeedbackLearningStageResults,
        FeedbackTrainingStageResult,
    },
};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

const COHORT_MANIFEST_DOMAIN: &str = "quant-pivot/feedback-learning-cohort-manifest";
const COHORT_MANIFEST_VERSION: u32 = 1;

/// Dependencies for [`FeedbackLearningExecutionService`].
pub struct FeedbackLearningExecutionDeps {
    pub datasets: Arc<dyn TrainingDatasetPort>,
    pub training: Arc<dyn ModelTrainingPort>,
    pub calibration_fit: Arc<dyn ModelCalibrationFitPort>,
    pub calibration_artifacts: Arc<dyn CalibrationArtifactFitPort>,
    pub cpcv: Arc<dyn CpcvBacktestPort>,
    pub governance: Arc<dyn ModelGovernancePort>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub compute: Arc<ComputeExecutor>,
}

/// Executes one canonical, bounded batch for each feedback learning stage.
pub struct FeedbackLearningExecutionService {
    datasets: Arc<dyn TrainingDatasetPort>,
    training: Arc<dyn ModelTrainingPort>,
    calibration_fit: Arc<dyn ModelCalibrationFitPort>,
    calibration_artifacts: Arc<dyn CalibrationArtifactFitPort>,
    cpcv: Arc<dyn CpcvBacktestPort>,
    governance: Arc<dyn ModelGovernancePort>,
    artifacts: Arc<dyn ArtifactStore>,
    compute: FeedbackLearningCompute,
}

struct FeedbackLearningCompute {
    executor: Arc<ComputeExecutor>,
    memory: OfflineMemory,
}

impl FeedbackLearningCompute {
    fn try_new(executor: Arc<ComputeExecutor>) -> QuantResult<Self> {
        Ok(Self {
            executor,
            memory: OfflineMemory::try_bytes(OFFLINE_MEMORY_BYTES)?,
        })
    }

    async fn run<T, F>(&self, cancel: &CancellationToken, work: F) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        self.executor
            .run_offline_cancellable(self.memory, cancel, work)
            .await
    }

    async fn finalize<T, F>(&self, work: F) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        self.executor.run_offline(self.memory, work).await
    }
}

struct CpcvBatchPlan {
    calibrated: BTreeMap<ContentHash, ModelVersionId>,
    results: Vec<FeedbackCpcvStageResult>,
    total: Option<u64>,
}

impl FeedbackLearningExecutionService {
    pub fn try_new(deps: FeedbackLearningExecutionDeps) -> QuantResult<Self> {
        Ok(Self {
            datasets: deps.datasets,
            training: deps.training,
            calibration_fit: deps.calibration_fit,
            calibration_artifacts: deps.calibration_artifacts,
            cpcv: deps.cpcv,
            governance: deps.governance,
            artifacts: deps.artifacts,
            compute: FeedbackLearningCompute::try_new(deps.compute)?,
        })
    }

    async fn execute_dataset_batch(
        &self,
        params: FeedbackDatasetSealJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackLearningExecutionResult> {
        let validation = params.clone();
        self.compute
            .run(&cancel, move || {
                validation.validate().map_err(QuantError::from)
            })
            .await?;
        let total = Self::batch_total(params.commands.len())?;
        let mut results = Vec::with_capacity(params.commands.len());
        for (index, command) in params.commands.iter().cloned().enumerate() {
            Self::require_active(&cancel, FeedbackStage::DatasetSeal)?;
            let info = Self::execute_bounded(
                FeedbackStage::DatasetSeal,
                command.resource_budget,
                &cancel,
                |command_cancel| {
                    self.datasets.build_feedback(
                        command.request.clone(),
                        command.resource_budget.max_working_set_bytes,
                        Arc::clone(&progress),
                        command_cancel,
                    )
                },
            )
            .await?;
            let result_cancel = cancel.clone();
            results.push(
                self.compute
                    .run(&cancel, move || {
                        Self::require_active(&result_cancel, FeedbackStage::DatasetSeal)?;
                        Self::dataset_result(&command, &info)
                    })
                    .await?,
            );
            progress.report(ResearchJobProgress::with_total(
                "feedback-dataset-seal",
                Self::batch_progress(index)?,
                total,
            ));
        }
        let artifact_cancel = cancel.clone();
        let artifact = self
            .compute
            .run(&cancel, move || {
                Self::require_active(&artifact_cancel, FeedbackStage::DatasetSeal)?;
                FeedbackLearningStageArtifact::try_new(
                    params.feedback_cycle_id,
                    params.cycle_idempotency_hash,
                    params.candidate_family_hash,
                    params.input_hash()?,
                    None,
                    FeedbackLearningStageResults::DatasetSeal(results),
                )
            })
            .await?;
        self.persist(artifact, &cancel).await
    }

    async fn execute_training_batch(
        &self,
        params: FeedbackTrainingJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackLearningExecutionResult> {
        let validation = params.clone();
        self.compute
            .run(&cancel, move || {
                validation.validate().map_err(QuantError::from)
            })
            .await?;
        let previous = self
            .load_previous(
                &params.previous,
                params.candidate_family_hash,
                FeedbackStage::DatasetSeal,
                &cancel,
            )
            .await?;
        let FeedbackLearningStageResults::DatasetSeal(dataset_results) = &previous.results else {
            return Err(Self::contract(
                "Training predecessor is not a DatasetSeal result",
            ));
        };
        let datasets = Self::candidate_datasets(dataset_results, DatasetPurpose::Training);
        let total = Self::batch_total(params.commands.len())?;
        let mut results = Vec::with_capacity(params.commands.len());
        for (index, command) in params.commands.iter().cloned().enumerate() {
            Self::require_active(&cancel, FeedbackStage::Training)?;
            let expected = datasets
                .get(&command.candidate_recipe_hash)
                .ok_or_else(|| {
                    Self::contract("Training command has no exact DatasetSeal recipe")
                })?;
            if command.params.request.training_dataset_id != expected.training_dataset_id {
                return Err(Self::contract(
                    "Training command Dataset differs from its frozen recipe",
                ));
            }
            let dataset = self
                .require_dataset(&expected.training_dataset_id, DatasetPurpose::Training)
                .await?;
            let verify_dataset = dataset.clone();
            let verify_expected = expected.clone();
            self.compute
                .run(&cancel, move || {
                    Self::verify_dataset_evidence(&verify_dataset, &verify_expected)
                })
                .await?;
            let view = Self::execute_bounded(
                FeedbackStage::Training,
                command.resource_budget,
                &cancel,
                |command_cancel| {
                    self.training.train(
                        command.params.clone(),
                        Arc::clone(&progress),
                        command_cancel,
                    )
                },
            )
            .await?;
            if view.model_version_id != command.params.model_version_id
                || view.model_run_id != Some(command.params.model_run_id)
            {
                return Err(Self::contract(
                    "Training port returned another preassigned model version or run",
                ));
            }
            let model = self.require_model(&command.params.model_version_id).await?;
            let candidate_recipe_hash = command.candidate_recipe_hash;
            let model_run_id = command.params.model_run_id;
            results.push(
                Box::pin(self.compute.run(&cancel, move || {
                    Self::training_result(candidate_recipe_hash, model_run_id, &dataset, &model)
                }))
                .await?,
            );
            progress.report(ResearchJobProgress::with_total(
                "feedback-training",
                Self::batch_progress(index)?,
                total,
            ));
        }
        let artifact = self
            .compute
            .run(&cancel, move || {
                FeedbackLearningStageArtifact::try_new(
                    params.feedback_cycle_id,
                    params.cycle_idempotency_hash,
                    params.candidate_family_hash,
                    params.input_hash()?,
                    Some(params.previous),
                    FeedbackLearningStageResults::Training(results),
                )
            })
            .await?;
        self.persist(artifact, &cancel).await
    }

    fn dataset_result(
        command: &FeedbackDatasetBuildCommand,
        info: &TrainingDatasetInfo,
    ) -> QuantResult<FeedbackDatasetStageResult> {
        if info.training_dataset_id != command.request.training_dataset_id {
            return Err(Self::contract(format!(
                "feedback Dataset identity mismatch: expected {}, read {}",
                command.request.training_dataset_id, info.training_dataset_id
            )));
        }
        if info.model_spec_id != command.request.model_spec_id {
            return Err(Self::contract(format!(
                "feedback Dataset ModelSpec mismatch: expected {}, read {}",
                command.request.model_spec_id, info.model_spec_id
            )));
        }
        if info.purpose != command.request.purpose {
            return Err(Self::contract(format!(
                "feedback Dataset purpose mismatch: expected {}, read {}",
                command.request.purpose, info.purpose
            )));
        }
        if info.source_lineage != command.request.source_lineage {
            return Err(Self::contract(
                "feedback Dataset source-lineage preimage differs from its frozen command",
            ));
        }
        Self::ensure_ready_status(info.status, info.failure_detail.as_deref())?;
        let manifest = info
            .manifest
            .as_ref()
            .ok_or_else(|| Self::contract("Ready feedback Dataset has no immutable manifest"))?;
        manifest.validate().map_err(|error| {
            Self::contract(format!("invalid feedback Dataset manifest: {error}"))
        })?;
        let cohort = info
            .cohort_manifest
            .as_ref()
            .ok_or_else(|| Self::contract("Ready feedback Dataset has no cohort manifest"))?;
        cohort.validate().map_err(|error| {
            Self::contract(format!("invalid feedback cohort manifest: {error}"))
        })?;
        if manifest.cohort_manifest.as_ref() != Some(cohort)
            || cohort.window != command.request.window
            || manifest.purpose != command.request.purpose
            || manifest.model_spec_id != command.request.model_spec_id
            || manifest.source_lineage != command.request.source_lineage
        {
            return Err(Self::contract(
                "feedback Dataset manifests differ from their frozen command",
            ));
        }
        let sample_count = info
            .sample_count
            .and_then(|count| u64::try_from(count).ok())
            .filter(|count| *count > 0)
            .ok_or_else(|| Self::contract("Ready feedback Dataset has no positive sample count"))?;
        let dataset_hash = info
            .dataset_hash
            .ok_or_else(|| Self::contract("Ready feedback Dataset has no dataset hash"))?;
        let manifest_hash = info
            .manifest_hash
            .ok_or_else(|| Self::contract("Ready feedback Dataset has no manifest hash"))?;
        let artifact_bytes_hash = info
            .artifact_bytes_hash
            .ok_or_else(|| Self::contract("Ready feedback Dataset has no artifact byte hash"))?;
        let parquet_uri = info
            .parquet_uri
            .clone()
            .ok_or_else(|| Self::contract("Ready feedback Dataset has no artifact URI"))?;
        let cohort_manifest_hash = CanonicalDigest::content_hash_typed(
            COHORT_MANIFEST_DOMAIN,
            COHORT_MANIFEST_VERSION,
            cohort,
        )?;
        Ok(FeedbackDatasetStageResult {
            role: command.role,
            training_dataset_id: info.training_dataset_id,
            purpose: info.purpose,
            dataset_hash,
            manifest_hash,
            artifact_bytes_hash,
            parquet_uri,
            cohort_manifest_hash,
            sample_count,
        })
    }

    fn ensure_ready_status(
        status: TrainingDatasetStatus,
        failure_detail: Option<&str>,
    ) -> QuantResult<()> {
        if status == TrainingDatasetStatus::Ready {
            return Ok(());
        }
        let detail = failure_detail.unwrap_or("dataset ledger omitted terminal diagnostics");
        Err(Self::contract(format!(
            "feedback Dataset is not Ready: status={status}, detail={detail}"
        )))
    }

    fn training_result(
        candidate_recipe_hash: ContentHash,
        model_run_id: ModelRunId,
        dataset: &TrainingDatasetInfo,
        model: &ModelVersionInfo,
    ) -> QuantResult<FeedbackTrainingStageResult> {
        let contract = model.verified_serving_contract().map_err(|error| {
            Self::contract(format!("invalid trained serving contract: {error}"))
        })?;
        let bindings = contract.bindings();
        if model.training_dataset_id != Some(dataset.training_dataset_id)
            || model.model_spec_id != dataset.model_spec_id
            || model.model_family != dataset.model_family
            || model.model_spec_definition_hash != dataset.model_spec_definition_hash
            || model.profile_ref.artifact_id() != dataset.research_profile_artifact_id
            || bindings.dataset.manifest.training_dataset_id != dataset.training_dataset_id
        {
            return Err(Self::contract(
                "trained model does not exactly bind its frozen feedback Dataset",
            ));
        }
        Ok(FeedbackTrainingStageResult {
            candidate_recipe_hash,
            model_version_id: model.model_version_id,
            model_run_id,
            training_dataset_id: dataset.training_dataset_id,
            model_artifact_hash: model.artifact_hash,
            serving_contract_hash: model.serving_contract_hash,
            training_input_hash: bindings.transform.training_input_hash,
        })
    }

    async fn require_dataset(
        &self,
        training_dataset_id: &TrainingDatasetId,
        purpose: DatasetPurpose,
    ) -> QuantResult<TrainingDatasetInfo> {
        let info = self
            .datasets
            .find_by_id(training_dataset_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found("quant_training_dataset", training_dataset_id)
            })?;
        if info.status != TrainingDatasetStatus::Ready || info.purpose != purpose {
            return Err(Self::contract(format!(
                "Dataset {training_dataset_id} must be Ready/{purpose}, found {}/{}",
                info.status, info.purpose
            )));
        }
        Ok(info)
    }

    async fn require_model(
        &self,
        model_version_id: &ModelVersionId,
    ) -> QuantResult<ModelVersionInfo> {
        self.training
            .find_version(model_version_id)
            .await?
            .ok_or_else(|| {
                QuantError::from(StorageError::not_found(
                    "quant_model_version",
                    model_version_id,
                ))
            })
    }

    fn verify_dataset_evidence(
        info: &TrainingDatasetInfo,
        expected: &FeedbackDatasetStageResult,
    ) -> QuantResult<()> {
        let exact = info.training_dataset_id == expected.training_dataset_id
            && info.purpose == expected.purpose
            && info.dataset_hash == Some(expected.dataset_hash)
            && info.manifest_hash == Some(expected.manifest_hash)
            && info.artifact_bytes_hash == Some(expected.artifact_bytes_hash)
            && info.parquet_uri.as_ref() == Some(&expected.parquet_uri)
            && info
                .sample_count
                .and_then(|count| u64::try_from(count).ok())
                == Some(expected.sample_count);
        if !exact {
            return Err(Self::contract(
                "persisted Dataset differs from DatasetSeal stage evidence",
            ));
        }
        Ok(())
    }

    fn candidate_datasets(
        results: &[FeedbackDatasetStageResult],
        purpose: DatasetPurpose,
    ) -> BTreeMap<ContentHash, FeedbackDatasetStageResult> {
        results
            .iter()
            .filter_map(|result| {
                let ((
                    DatasetPurpose::Training,
                    FeedbackDatasetRole::CandidateTraining {
                        candidate_recipe_hash: recipe,
                    },
                )
                | (
                    DatasetPurpose::Calibration,
                    FeedbackDatasetRole::CandidateCalibration {
                        candidate_recipe_hash: recipe,
                    },
                )) = (purpose, result.role)
                else {
                    return None;
                };
                Some((recipe, result.clone()))
            })
            .collect::<BTreeMap<_, _>>()
    }

    async fn execute_calibration_batch(
        &self,
        params: FeedbackCalibrationJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackLearningExecutionResult> {
        let validation = params.clone();
        self.compute
            .run(&cancel, move || {
                validation.validate().map_err(QuantError::from)
            })
            .await?;
        let training_artifact = self
            .load_previous(
                &params.previous,
                params.candidate_family_hash,
                FeedbackStage::Training,
                &cancel,
            )
            .await?;
        let FeedbackLearningStageResults::Training(training_results) = &training_artifact.results
        else {
            return Err(Self::contract(
                "Calibration predecessor is not a Training result",
            ));
        };
        let dataset_reference = training_artifact
            .previous
            .as_ref()
            .ok_or_else(|| Self::contract("Training artifact lost its DatasetSeal predecessor"))?;
        let dataset_artifact = self
            .load_previous(
                dataset_reference,
                params.candidate_family_hash,
                FeedbackStage::DatasetSeal,
                &cancel,
            )
            .await?;
        let FeedbackLearningStageResults::DatasetSeal(dataset_results) = &dataset_artifact.results
        else {
            return Err(Self::contract(
                "Calibration ancestry is not a DatasetSeal result",
            ));
        };
        let training_models = training_results
            .iter()
            .map(|result| (result.candidate_recipe_hash, result))
            .collect::<BTreeMap<_, _>>();
        let calibration_datasets =
            Self::candidate_datasets(dataset_results, DatasetPurpose::Calibration);
        let total = Self::batch_total(params.commands.len())?;
        let mut results = Vec::with_capacity(params.commands.len());
        for (index, command) in params.commands.iter().cloned().enumerate() {
            Self::require_active(&cancel, FeedbackStage::Calibration)?;
            let trained = training_models
                .get(&command.candidate_recipe_hash)
                .ok_or_else(|| Self::contract("Calibration command has no exact trained recipe"))?;
            let dataset = calibration_datasets
                .get(&command.candidate_recipe_hash)
                .ok_or_else(|| {
                    Self::contract("Calibration command has no exact held-out Dataset")
                })?;
            if command.params.request.model_version_id != trained.model_version_id
                || command.params.request.calibration_dataset_id != dataset.training_dataset_id
            {
                return Err(Self::contract(
                    "Calibration command differs from its Training/DatasetSeal evidence",
                ));
            }
            let source_model = self.require_model(&trained.model_version_id).await?;
            let calibration_dataset = self
                .require_dataset(&dataset.training_dataset_id, DatasetPurpose::Calibration)
                .await?;
            let verify_model = source_model.clone();
            let verify_training = (*trained).clone();
            let verify_dataset = calibration_dataset.clone();
            let verify_dataset_result = dataset.clone();
            Box::pin(self.compute.run(&cancel, move || {
                Self::verify_training_evidence(&verify_model, &verify_training)?;
                Self::verify_dataset_evidence(&verify_dataset, &verify_dataset_result)
            }))
            .await?;
            let outcome = Self::execute_bounded(
                FeedbackStage::Calibration,
                command.resource_budget,
                &cancel,
                |command_cancel| {
                    self.calibration_fit.fit(
                        command.params.clone(),
                        Arc::clone(&progress),
                        command_cancel,
                    )
                },
            )
            .await?;
            results.push(
                Box::pin(self.calibration_result(
                    command,
                    source_model,
                    calibration_dataset,
                    outcome,
                    &cancel,
                ))
                .await?,
            );
            progress.report(ResearchJobProgress::with_total(
                "feedback-calibration",
                Self::batch_progress(index)?,
                total,
            ));
        }
        let artifact = self
            .compute
            .run(&cancel, move || {
                FeedbackLearningStageArtifact::try_new(
                    params.feedback_cycle_id,
                    params.cycle_idempotency_hash,
                    params.candidate_family_hash,
                    params.input_hash()?,
                    Some(params.previous),
                    FeedbackLearningStageResults::Calibration(results),
                )
            })
            .await?;
        self.persist(artifact, &cancel).await
    }

    async fn calibration_result(
        &self,
        command: FeedbackCalibrationCommand,
        source_model: ModelVersionInfo,
        calibration_dataset: TrainingDatasetInfo,
        outcome: ModelCalibrationFitOutcome,
        cancel: &CancellationToken,
    ) -> QuantResult<FeedbackCalibrationStageResult> {
        let method = command.params.request.method;
        let (calibration_artifact_id, sample_count) = match outcome {
            ModelCalibrationFitOutcome::Calibrated {
                artifact_id,
                sample_count,
            } => (artifact_id, sample_count),
            ModelCalibrationFitOutcome::Insufficient {
                sample_count,
                total_sample_count,
                minimum_sample_count,
                outcome_hash,
            } => {
                return Ok(FeedbackCalibrationStageResult::Insufficient {
                    candidate_recipe_hash: command.candidate_recipe_hash,
                    source_model_version_id: source_model.model_version_id,
                    model_run_id: command.params.model_run_id,
                    calibration_dataset_id: calibration_dataset.training_dataset_id,
                    method,
                    sample_count,
                    total_sample_count,
                    minimum_sample_count,
                    outcome_hash,
                });
            }
        };
        let info = self
            .calibration_artifacts
            .find(&calibration_artifact_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found("quant_calibration_artifact", calibration_artifact_id)
            })?;
        let verify_source = source_model.clone();
        let verify_dataset = calibration_dataset.clone();
        let decision_policy_snapshot_id = command.params.decision_policy_snapshot_id;
        let calibration_artifact_hash = self
            .compute
            .run(cancel, move || {
                let payload = info.verify_model_score().map_err(|error| {
                    Self::contract(format!("invalid calibration artifact: {error}"))
                })?;
                let fit = &payload.fit_contract;
                let mapping_matches = matches!(
                    (method, &payload.mapping),
                    (
                        CalibrationMethod::Isotonic,
                        MonotoneMapping::Isotonic { .. }
                    ) | (CalibrationMethod::Platt, MonotoneMapping::Platt { .. })
                );
                if !mapping_matches
                    || fit.model.model_version_id != verify_source.model_version_id
                    || fit.model.artifact_hash != verify_source.artifact_hash
                    || fit.model.serving_contract_hash != verify_source.serving_contract_hash
                    || fit.calibration_dataset.calibration_dataset_id
                        != verify_dataset.training_dataset_id
                    || fit.calibration_dataset.dataset_hash != Self::dataset_hash(&verify_dataset)?
                    || fit.policy_snapshot.decision_policy_snapshot_id
                        != decision_policy_snapshot_id
                    || sample_count != payload.reliability.n_samples
                    || i64::try_from(sample_count).ok() != Some(info.sample_count)
                {
                    return Err(Self::contract(
                        "calibration artifact read-back differs from its frozen fit command",
                    ));
                }
                Ok(info.content_hash)
            })
            .await?;
        let calibrated = self
            .governance
            .seal_calibrated_model(
                &source_model.model_version_id,
                CalibratedModelSealCommand {
                    calibrator_ref: calibration_artifact_id,
                    downside_source: command.params.downside_source,
                    reason: command.params.request.reason.clone(),
                },
                command.params.actor.clone(),
            )
            .await?;
        Box::pin(self.compute.finalize(move || {
            let derivation = calibrated.verified_derivation().map_err(|error| {
                Self::contract(format!("invalid calibrated model derivation: {error}"))
            })?;
            if derivation
                != (ModelVersionDerivation::ReturnCalibration {
                    parent_model_version_id: source_model.model_version_id,
                    calibration_artifact_id,
                })
            {
                return Err(Self::contract(
                    "calibrated model does not bind its exact source and calibrator",
                ));
            }
            let contract = calibrated.verified_serving_contract().map_err(|error| {
                Self::contract(format!("invalid calibrated serving contract: {error}"))
            })?;
            let source_contract = source_model.verified_serving_contract().map_err(|error| {
                Self::contract(format!("invalid source serving contract: {error}"))
            })?;
            let bindings = contract.bindings();
            if calibrated.training_dataset_id != source_model.training_dataset_id
                || bindings.transform.training_input_hash
                    != source_contract.bindings().transform.training_input_hash
            {
                return Err(Self::contract(
                    "calibrated model changed its source Dataset or training input",
                ));
            }
            Ok(FeedbackCalibrationStageResult::Calibrated {
                candidate_recipe_hash: command.candidate_recipe_hash,
                source_model_version_id: source_model.model_version_id,
                model_run_id: command.params.model_run_id,
                calibration_dataset_id: calibration_dataset.training_dataset_id,
                method,
                calibration_artifact_id,
                calibration_artifact_hash,
                calibrated_model_version_id: calibrated.model_version_id,
                calibrated_model_artifact_hash: calibrated.artifact_hash,
                calibrated_serving_contract_hash: calibrated.serving_contract_hash,
                training_input_hash: bindings.transform.training_input_hash,
                sample_count,
            })
        }))
        .await
    }

    async fn execute_cpcv_batch(
        &self,
        params: FeedbackCpcvJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackLearningExecutionResult> {
        let validation = params.clone();
        self.compute
            .run(&cancel, move || {
                validation.validate().map_err(QuantError::from)
            })
            .await?;
        let CpcvBatchPlan {
            calibrated,
            mut results,
            total,
        } = self.prepare_cpcv_batch(&params, &cancel).await?;
        results.reserve(params.commands.len());
        for (index, command) in params.commands.iter().cloned().enumerate() {
            Self::require_active(&cancel, FeedbackStage::Cpcv)?;
            let expected_model = calibrated
                .get(&command.candidate_recipe_hash)
                .ok_or_else(|| Self::contract("CPCV command has no calibrated candidate"))?;
            if command.params.model_version_id != *expected_model {
                return Err(Self::contract(
                    "CPCV command model differs from Calibration evidence",
                ));
            }
            let path_set_id =
                command.params.request.path_set_id.ok_or_else(|| {
                    Self::contract("CPCV command lost its preassigned path-set id")
                })?;
            let expected_path_count = i64::try_from(command.cpcv_spec.expected_path_count()?)
                .map_err(|error| {
                    Self::contract(format!("CPCV expected path count exceeds i64: {error}"))
                })?;
            let expected_combination_count = i64::try_from(
                command.cpcv_spec.expected_combination_count()?,
            )
            .map_err(|error| {
                Self::contract(format!(
                    "CPCV expected combination count exceeds i64: {error}"
                ))
            })?;
            let view = Self::execute_bounded(
                FeedbackStage::Cpcv,
                command.resource_budget,
                &cancel,
                |command_cancel| {
                    self.cpcv.run(
                        command.params.clone(),
                        Arc::clone(&progress),
                        command_cancel,
                    )
                },
            )
            .await?;
            if view.path_set_id != path_set_id {
                return Err(Self::contract(
                    "CPCV port returned another preassigned path-set id",
                ));
            }
            let stored = self
                .cpcv
                .find_path_set(&path_set_id)
                .await?
                .ok_or_else(|| StorageError::not_found("quant_backtest_path_set", path_set_id))?;
            if stored.path_set_hash != view.path_set_hash
                || stored.model_version_id != command.params.model_version_id
                || stored.model_run_id != command.params.model_run_id
                || stored.training_dataset_id != command.params.request.training_dataset_id
                || stored.decision_policy_snapshot_id
                    != command.params.request.decision_policy_snapshot_id
                || stored.path_count != expected_path_count
                || stored.combination_count != expected_combination_count
            {
                return Err(Self::contract(
                    "CPCV path-set identity, methodology counts, or policy differs from its frozen command",
                ));
            }
            results.push(FeedbackCpcvStageResult::Evaluated {
                candidate_recipe_hash: command.candidate_recipe_hash,
                model_version_id: stored.model_version_id,
                training_dataset_id: stored.training_dataset_id,
                path_set_id: stored.path_set_id,
                model_run_id: stored.model_run_id,
                path_set_hash: stored.path_set_hash,
            });
            if let Some(total) = total {
                progress.report(ResearchJobProgress::with_total(
                    "feedback-cpcv",
                    Self::batch_progress(index)?,
                    total,
                ));
            }
        }
        let artifact = self
            .compute
            .run(&cancel, move || {
                results.sort_unstable_by_key(FeedbackCpcvStageResult::candidate_recipe_hash);
                FeedbackLearningStageArtifact::try_new(
                    params.feedback_cycle_id,
                    params.cycle_idempotency_hash,
                    params.candidate_family_hash,
                    params.input_hash()?,
                    Some(params.previous),
                    FeedbackLearningStageResults::Cpcv(results),
                )
            })
            .await?;
        self.persist(artifact, &cancel).await
    }

    async fn prepare_cpcv_batch(
        &self,
        params: &FeedbackCpcvJobParams,
        cancel: &CancellationToken,
    ) -> QuantResult<CpcvBatchPlan> {
        let calibration_artifact = self
            .load_previous(
                &params.previous,
                params.candidate_family_hash,
                FeedbackStage::Calibration,
                cancel,
            )
            .await?;
        let FeedbackLearningStageResults::Calibration(calibration_results) =
            &calibration_artifact.results
        else {
            return Err(Self::contract(
                "CPCV predecessor is not a Calibration result",
            ));
        };
        let calibrated = calibration_results
            .iter()
            .filter_map(|result| match result {
                FeedbackCalibrationStageResult::Calibrated {
                    candidate_recipe_hash,
                    calibrated_model_version_id,
                    ..
                } => Some((*candidate_recipe_hash, *calibrated_model_version_id)),
                FeedbackCalibrationStageResult::Insufficient { .. } => None,
            })
            .collect::<BTreeMap<_, _>>();
        if params.commands.len() != calibrated.len()
            || params.commands.iter().zip(&calibrated).any(
                |(command, (recipe_hash, model_version_id))| {
                    command.candidate_recipe_hash != *recipe_hash
                        || command.params.model_version_id != *model_version_id
                },
            )
        {
            return Err(Self::contract(
                "CPCV commands differ from the complete calibrated candidate set",
            ));
        }
        let total = if params.commands.is_empty() {
            None
        } else {
            Some(Self::batch_total(params.commands.len())?)
        };
        let results = calibration_results
            .iter()
            .filter_map(|result| match result {
                FeedbackCalibrationStageResult::Calibrated { .. } => None,
                FeedbackCalibrationStageResult::Insufficient {
                    candidate_recipe_hash,
                    source_model_version_id,
                    model_run_id,
                    calibration_dataset_id,
                    method,
                    sample_count,
                    total_sample_count,
                    minimum_sample_count,
                    outcome_hash,
                } => Some(FeedbackCpcvStageResult::CalibrationInsufficient {
                    candidate_recipe_hash: *candidate_recipe_hash,
                    source_model_version_id: *source_model_version_id,
                    model_run_id: *model_run_id,
                    calibration_dataset_id: *calibration_dataset_id,
                    method: *method,
                    sample_count: *sample_count,
                    total_sample_count: *total_sample_count,
                    minimum_sample_count: *minimum_sample_count,
                    outcome_hash: *outcome_hash,
                }),
            })
            .collect::<Vec<_>>();
        Ok(CpcvBatchPlan {
            calibrated,
            results,
            total,
        })
    }

    async fn load_previous(
        &self,
        reference: &FeedbackLearningStageArtifactRef,
        candidate_family_hash: ContentHash,
        stage: FeedbackStage,
        cancel: &CancellationToken,
    ) -> QuantResult<FeedbackLearningStageArtifact> {
        reference.validate_for(reference.feedback_cycle_id, stage)?;
        Self::require_active(cancel, stage)?;
        let bytes = self.artifacts.get(&reference.artifact.uri).await?;
        Self::require_active(cancel, stage)?;
        let reference = reference.clone();
        self.compute
            .run(cancel, move || {
                if CanonicalDigest::content_hash_bytes(&bytes) != reference.artifact.content_hash {
                    return Err(Self::contract(
                        "learning-stage predecessor bytes differ from their terminal hash",
                    ));
                }
                let artifact = FeedbackLearningStageCodec::decode(&bytes)?;
                if artifact.feedback_cycle_id != reference.feedback_cycle_id
                    || artifact.results.stage() != reference.stage
                    || artifact.artifact_id != reference.artifact_id
                    || artifact.input_hash != reference.input_hash
                    || artifact.candidate_family_hash != candidate_family_hash
                {
                    return Err(Self::contract(
                        "learning-stage predecessor differs from its frozen reference",
                    ));
                }
                Ok(artifact)
            })
            .await
    }

    async fn persist(
        &self,
        artifact: FeedbackLearningStageArtifact,
        cancel: &CancellationToken,
    ) -> QuantResult<FeedbackLearningExecutionResult> {
        let (artifact, bytes, content_hash, key) = self
            .compute
            .run(cancel, move || {
                let bytes = FeedbackLearningStageCodec::encode(&artifact)?;
                let content_hash = CanonicalDigest::content_hash_bytes(&bytes);
                let key = ArtifactKey::new(
                    ArtifactNamespace::FeedbackLearning,
                    content_hash.hex(),
                    "json",
                )?;
                Ok((artifact, bytes, content_hash, key))
            })
            .await?;
        let artifact_id = artifact.artifact_id;
        Self::require_active(cancel, artifact.results.stage())?;
        let uri = self.artifacts.put(key, &bytes).await?;
        let persisted = self.artifacts.get(&uri).await?;
        self.compute
            .finalize(move || {
                let readback = Self::decode_readback(&persisted, content_hash)?;
                if readback != artifact {
                    return Err(ResearchError::ArtifactHashMismatch {
                        expected: content_hash.to_string(),
                        actual: CanonicalDigest::content_hash_bytes(&persisted).to_string(),
                    }
                    .into());
                }
                Ok(FeedbackLearningExecutionResult {
                    artifact_id,
                    artifact: ResearchJobArtifactRef { uri, content_hash },
                })
            })
            .await
    }

    fn decode_readback(
        bytes: &[u8],
        expected: ContentHash,
    ) -> QuantResult<FeedbackLearningStageArtifact> {
        let actual = CanonicalDigest::content_hash_bytes(bytes);
        if actual != expected {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: expected.to_string(),
                actual: actual.to_string(),
            }
            .into());
        }
        FeedbackLearningStageCodec::decode(bytes)
    }

    fn verify_training_evidence(
        model: &ModelVersionInfo,
        expected: &FeedbackTrainingStageResult,
    ) -> QuantResult<()> {
        let contract = model
            .verified_serving_contract()
            .map_err(|error| Self::contract(format!("invalid trained model contract: {error}")))?;
        let exact = model.model_version_id == expected.model_version_id
            && model.training_dataset_id == Some(expected.training_dataset_id)
            && model.artifact_hash == expected.model_artifact_hash
            && model.serving_contract_hash == expected.serving_contract_hash
            && contract.bindings().transform.training_input_hash == expected.training_input_hash;
        if !exact {
            return Err(Self::contract(
                "persisted model differs from Training stage evidence",
            ));
        }
        Ok(())
    }

    fn dataset_hash(dataset: &TrainingDatasetInfo) -> QuantResult<ContentHash> {
        dataset
            .dataset_hash
            .ok_or_else(|| Self::contract("Ready Dataset has no immutable dataset hash"))
    }

    fn batch_total(len: usize) -> QuantResult<u64> {
        u64::try_from(len).map_err(|error| {
            Self::contract(format!("learning-stage batch length exceeds u64: {error}"))
        })
    }

    fn batch_progress(index: usize) -> QuantResult<u64> {
        index
            .checked_add(1)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| Self::contract("learning-stage progress counter overflowed"))
    }

    fn require_active(cancel: &CancellationToken, stage: FeedbackStage) -> QuantResult<()> {
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: format!("{stage} batch cancelled between candidates"),
            }
            .into());
        }
        Ok(())
    }

    async fn execute_bounded<T, F, Fut>(
        stage: FeedbackStage,
        budget: FeedbackRecipeResourceBudget,
        cancel: &CancellationToken,
        operation: F,
    ) -> QuantResult<T>
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = QuantResult<T>>,
    {
        budget.validate()?;
        let command_cancel = cancel.child_token();
        let operation = operation(command_cancel.clone());
        tokio::pin!(operation);
        let deadline = sleep(Duration::from_secs(budget.deadline_secs));
        tokio::pin!(deadline);
        tokio::select! {
            biased;
            result = &mut operation => result,
            () = &mut deadline => {
                command_cancel.cancel();
                match operation.await {
                    // An idempotent owner may cross its durable commit boundary
                    // concurrently with the deadline. Drain it and preserve the
                    // committed result instead of manufacturing a failed job.
                    Ok(result) => Ok(result),
                    Err(QuantError::Research(ResearchError::Cancelled { .. })) => {
                        Err(ResearchError::ComputeDeadlineExceeded {
                            operation: match stage {
                                FeedbackStage::DatasetSeal => "feedback_dataset_seal",
                                FeedbackStage::Training => "feedback_training",
                                FeedbackStage::Calibration => "feedback_calibration",
                                FeedbackStage::Cpcv => "feedback_cpcv",
                                _ => "feedback_learning",
                            },
                            deadline_secs: budget.deadline_secs,
                        }
                        .into())
                    }
                    Err(error) => Err(error),
                }
            }
        }
    }

    fn contract(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidJobContract {
            detail: detail.into(),
        }
        .into()
    }
}

#[async_trait]
impl FeedbackLearningExecutionPort for FeedbackLearningExecutionService {
    async fn seal_datasets(
        &self,
        params: FeedbackDatasetSealJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackLearningExecutionResult> {
        self.execute_dataset_batch(params, progress, cancel).await
    }

    async fn train(
        &self,
        params: FeedbackTrainingJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackLearningExecutionResult> {
        Box::pin(self.execute_training_batch(params, progress, cancel)).await
    }

    async fn calibrate(
        &self,
        params: FeedbackCalibrationJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackLearningExecutionResult> {
        Box::pin(self.execute_calibration_batch(params, progress, cancel)).await
    }

    async fn validate_cpcv(
        &self,
        params: FeedbackCpcvJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackLearningExecutionResult> {
        self.execute_cpcv_batch(params, progress, cancel).await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, OnceLock},
        thread,
    };

    use quant_pivot_compute::ComputeExecutor;
    use quant_pivot_error::{QuantError, research::ResearchError};
    use quant_pivot_models::{
        domain::ports::FeedbackRecipeResourceBudget,
        enums::quant::{FeedbackStage, TrainingDatasetStatus},
        hashing::CanonicalDigest,
    };
    use tokio_util::sync::CancellationToken;

    use super::{FeedbackLearningCompute, FeedbackLearningExecutionService};

    #[test]
    fn dataset_status_preserves_diagnostic() {
        let result = FeedbackLearningExecutionService::ensure_ready_status(
            TrainingDatasetStatus::InsufficientLabels,
            Some("target=token_payout_ratio/0, label_rows=0"),
        );
        let error = result.expect_err("insufficient labels must stop DatasetSeal");
        let rendered = error.to_string();
        assert!(rendered.contains("status=insufficient_labels"));
        assert!(rendered.contains("target=token_payout_ratio/0, label_rows=0"));
    }

    #[test]
    fn malformed_readback_is_rejected() {
        let bytes = br#"{"not":"a learning artifact"}"#;
        let expected = CanonicalDigest::content_hash_bytes(bytes);
        let result = FeedbackLearningExecutionService::decode_readback(bytes, expected);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn compute_uses_offline_pool() {
        let compute = FeedbackLearningCompute::try_new(Arc::new(
            ComputeExecutor::new().expect("construct governed compute fixture"),
        ))
        .expect("construct feedback compute policy");
        let cancel = CancellationToken::new();
        let thread_name = compute
            .run(&cancel, || {
                Ok::<_, QuantError>(thread::current().name().unwrap_or_default().to_owned())
            })
            .await
            .expect("run feedback verification on governed pool");
        assert!(thread_name.starts_with("quant-offline-"));
        let finalize_thread = compute
            .finalize(|| {
                Ok::<_, QuantError>(thread::current().name().unwrap_or_default().to_owned())
            })
            .await
            .expect("run committed feedback verification on governed pool");
        assert!(finalize_thread.starts_with("quant-offline-"));
    }

    #[tokio::test]
    async fn deadline_commit_wins() {
        let observed = Arc::new(OnceLock::new());
        let captured = Arc::clone(&observed);
        let parent = CancellationToken::new();
        let result = FeedbackLearningExecutionService::execute_bounded(
            FeedbackStage::Training,
            FeedbackRecipeResourceBudget {
                max_concurrency: 1,
                max_working_set_bytes: 1,
                max_resident_model_bytes: 1,
                deadline_secs: 1,
            },
            &parent,
            move |cancel| {
                let _ = captured.set(cancel.clone());
                async move {
                    cancel.cancelled().await;
                    Ok::<_, QuantError>("committed")
                }
            },
        )
        .await;
        assert_eq!(
            result.expect("committed result must win deadline"),
            "committed"
        );
        assert!(observed.get().is_some_and(CancellationToken::is_cancelled));
        assert!(!parent.is_cancelled());
    }

    #[tokio::test]
    async fn deadline_cancels_work() {
        let observed = Arc::new(OnceLock::new());
        let captured = Arc::clone(&observed);
        let parent = CancellationToken::new();
        let result = FeedbackLearningExecutionService::execute_bounded(
            FeedbackStage::Training,
            FeedbackRecipeResourceBudget {
                max_concurrency: 1,
                max_working_set_bytes: 1,
                max_resident_model_bytes: 1,
                deadline_secs: 1,
            },
            &parent,
            move |cancel| {
                let _ = captured.set(cancel.clone());
                async move {
                    cancel.cancelled().await;
                    Err::<(), QuantError>(
                        ResearchError::Cancelled {
                            detail: "deadline fixture observed cancellation".to_owned(),
                        }
                        .into(),
                    )
                }
            },
        )
        .await;
        assert!(matches!(
            result,
            Err(QuantError::Research(
                ResearchError::ComputeDeadlineExceeded {
                    operation: "feedback_training",
                    deadline_secs: 1,
                }
            ))
        ));
        assert!(observed.get().is_some_and(CancellationToken::is_cancelled));
        assert!(!parent.is_cancelled());
    }
}
