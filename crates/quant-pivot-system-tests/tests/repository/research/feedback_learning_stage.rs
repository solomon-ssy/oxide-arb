//! Learning-stage adapter contracts against real cycle and job ledgers.

use std::{env, fs, path::PathBuf, sync::Arc};

use chrono::{Duration, Utc};
use quant_pivot_core::service::{
    feedback_evaluation::{
        FeedbackEvaluationReservationDeps, FeedbackEvaluationReservationService,
    },
    feedback_learning_stage::{FeedbackLearningStageAdapter, FeedbackLearningStageDeps},
    feedback_recipe_stage::{FeedbackRecipeStageAdapter, FeedbackRecipeStageDeps},
};
use quant_pivot_models::{
    domain::{
        api::{
            CpcvBacktestJobParams, FitModelCalibratorRequest, ModelTrainJobParams,
            RunCpcvBacktestRequest, TrainModelRequest,
        },
        ports::{
            FeedbackCalibrationCommand, FeedbackCalibrationJobParams, FeedbackCandidateFamily,
            FeedbackCpcvCommand, FeedbackCpcvJobParams, FeedbackDatasetBuildCommand,
            FeedbackDatasetRole, FeedbackDatasetSealJobParams, FeedbackLearningStageArtifactRef,
            FeedbackTrainingCommand, FeedbackTrainingJobParams, GovernanceActor,
            ModelCalibrationFitJobParams,
        },
        quant::{
            FeedbackCycleInfo, FeedbackStageJobIdentity, NewResearchJob, ResearchJobArtifactRef,
            ResearchJobFinalization, ResearchJobInfo, ResearchJobResultRef,
        },
    },
    enums::quant::{
        CalibrationMethod, DatasetPurpose, DownsideSource, FeedbackCycleStatus, FeedbackStage,
        ResearchJobResultKind,
    },
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, BacktestPathSetId, CalibrationArtifactId, ContentHash,
        DecisionPolicySnapshotId, FeedbackLearningStageArtifactId, ModelRunId, ModelVersionId,
        ResearchJobParams, TrainingDatasetId, WorkerId,
    },
};
use quant_pivot_repository::{
    postgres::{PgFeedbackCycleRepository, PgResearchJobRepository, PgTrainingDatasetRepository},
    traits::{
        FeedbackCycleClaim, FeedbackCycleLeaseGuard, FeedbackCycleRepository,
        FeedbackEvaluationWriteOutcome, ResearchJobEnqueueOutcome, ResearchJobRepository,
        TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
    feedback_learning::{
        FeedbackCalibrationStageResult, FeedbackCpcvStageResult, FeedbackDatasetStageResult,
        FeedbackLearningStageArtifact, FeedbackLearningStageCodec, FeedbackLearningStageResults,
        FeedbackTrainingStageResult,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg, support::artifact_store::ReadTamperArtifactStoreFixture,
};
use sea_orm::DatabaseConnection;

use super::feedback_boot_schema::{
    FeedbackSchemaFixture, content_hash, persist_recipe_plan_fixture, prepare_fixture,
};

struct ArtifactRoot {
    path: PathBuf,
}

impl ArtifactRoot {
    fn create() -> Self {
        let path = env::temp_dir().join(format!(
            "quant-pivot-w2-f07-{}",
            FeedbackLearningStageArtifactId::from_v7()
        ));
        fs::create_dir_all(&path).expect("create F07 artifact root");
        Self { path }
    }
}

impl Drop for ArtifactRoot {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).expect("remove F07 artifact root");
    }
}

async fn record_cycle(
    cycles: &PgFeedbackCycleRepository,
    fixture: &FeedbackSchemaFixture,
    second: bool,
) -> FeedbackCycleClaim {
    let cycle = if second {
        fixture.second_cycle.clone()
    } else {
        fixture.cycle.clone()
    };
    let cycle_id = cycle.feedback_cycle_id();
    cycles
        .record_trigger(
            cycle,
            fixture.stage_event(cycle_id, "feedback-learning-stage"),
        )
        .await
        .expect("record feedback cycle");
    let claim = cycles
        .claim_cycle(WorkerId::from_v7(), 90)
        .await
        .expect("claim feedback cycle")
        .expect("queued feedback cycle");
    assert_eq!(claim.cycle.feedback_cycle_id, cycle_id);
    assert_eq!(claim.cycle.status, FeedbackCycleStatus::Running);
    claim
}

pub fn dataset_params(
    cycle: &FeedbackCycleInfo,
    family: &FeedbackCandidateFamily,
    candidate_family_hash: ContentHash,
) -> FeedbackDatasetSealJobParams {
    let mut commands = Vec::with_capacity(family.candidates().len() * 2 + 1);
    commands.extend(
        family
            .candidates()
            .iter()
            .map(|candidate| FeedbackDatasetBuildCommand {
                role: FeedbackDatasetRole::CandidateTraining {
                    candidate_recipe_hash: candidate.candidate_recipe_hash(),
                },
                resource_budget: candidate.resource_budget(),
                request: candidate.training().clone(),
            }),
    );
    commands.extend(
        family
            .candidates()
            .iter()
            .map(|candidate| FeedbackDatasetBuildCommand {
                role: FeedbackDatasetRole::CandidateCalibration {
                    candidate_recipe_hash: candidate.candidate_recipe_hash(),
                },
                resource_budget: candidate.resource_budget(),
                request: candidate.calibration().clone(),
            }),
    );
    commands.push(FeedbackDatasetBuildCommand {
        role: FeedbackDatasetRole::SharedEvaluation,
        resource_budget: family.candidates()[0].resource_budget(),
        request: family.shared_evaluation().clone(),
    });
    FeedbackDatasetSealJobParams::try_new(
        cycle.feedback_cycle_id,
        cycle.idempotency_hash,
        candidate_family_hash,
        commands,
    )
    .expect("valid DatasetSeal params")
}

pub trait FeedbackDatasetParamsExt {
    fn dataset_results(&self) -> Vec<FeedbackDatasetStageResult>;
}

impl FeedbackDatasetParamsExt for FeedbackDatasetSealJobParams {
    fn dataset_results(&self) -> Vec<FeedbackDatasetStageResult> {
        self.commands
            .iter()
            .enumerate()
            .map(|(index, command)| {
                let seed = char::from_digit(
                    u32::try_from(index.saturating_add(2)).expect("small Dataset result index"),
                    16,
                )
                .expect("hex Dataset result seed");
                FeedbackDatasetStageResult {
                    role: command.role,
                    training_dataset_id: command.request.training_dataset_id,
                    purpose: command.request.purpose,
                    dataset_hash: content_hash(seed),
                    manifest_hash: content_hash('a'),
                    artifact_bytes_hash: content_hash('b'),
                    parquet_uri: ArtifactUri::parse(format!(
                        "s3://feedback-learning/{}.parquet",
                        command.request.training_dataset_id
                    ))
                    .expect("valid Dataset result URI"),
                    cohort_manifest_hash: content_hash('d'),
                    sample_count: 100,
                }
            })
            .collect()
    }
}

pub async fn persist_artifact(
    store: &Arc<dyn ArtifactStore>,
    artifact: &FeedbackLearningStageArtifact,
) -> ResearchJobArtifactRef {
    let bytes = FeedbackLearningStageCodec::encode(artifact)
        .expect("encode feedback learning-stage artifact");
    let content_hash = CanonicalDigest::content_hash_bytes(&bytes);
    let key = ArtifactKey::new(
        ArtifactNamespace::FeedbackLearning,
        content_hash.hex(),
        "json",
    )
    .expect("feedback learning-stage artifact key");
    let uri = store
        .put(key, &bytes)
        .await
        .expect("persist feedback learning-stage artifact");
    let read_back = store
        .get(&uri)
        .await
        .expect("read back feedback learning-stage artifact");
    assert_eq!(read_back, bytes);
    ResearchJobArtifactRef { uri, content_hash }
}

async fn finalize_job(
    jobs: &PgResearchJobRepository,
    job: NewResearchJob,
    artifact: ResearchJobArtifactRef,
) -> ResearchJobInfo {
    let job_id = job.job_id;
    let kind = job.kind;
    let artifact_id = match &job.params_json {
        ResearchJobParams::FeedbackDatasetSeal(params) => params.artifact_id,
        ResearchJobParams::FeedbackTraining(params) => params.artifact_id,
        ResearchJobParams::FeedbackCalibration(params) => params.artifact_id,
        ResearchJobParams::FeedbackCpcv(params) => params.artifact_id,
        _ => panic!("learning-stage fixture emitted another job kind"),
    };
    let queued = match jobs.enqueue(job).await.expect("enqueue learning-stage job") {
        ResearchJobEnqueueOutcome::Inserted(info)
        | ResearchJobEnqueueOutcome::AlreadyPresent(info) => info,
    };
    assert_eq!(queued.job_id, job_id);
    let worker = WorkerId::from_v7();
    let leased = jobs
        .lease_next(&[kind], &worker, Utc::now() + Duration::seconds(90))
        .await
        .expect("lease learning-stage job")
        .expect("queued learning-stage job");
    assert_eq!(leased.job_id, job_id);
    jobs.finalize(
        &job_id,
        &worker,
        ResearchJobFinalization::succeeded(
            Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackLearningStageArtifact,
                id: artifact_id.as_uuid(),
            }),
            Some(artifact),
            None,
        ),
    )
    .await
    .expect("finalize learning-stage job")
}

pub fn candidate_dataset(
    params: &FeedbackDatasetSealJobParams,
    purpose: DatasetPurpose,
) -> TrainingDatasetId {
    params
        .commands
        .iter()
        .find_map(|command| {
            (command.request.purpose == purpose && command.role.candidate_recipe_hash().is_some())
                .then_some(command.request.training_dataset_id)
        })
        .expect("candidate Dataset for purpose")
}

fn stage_adapter(
    cycles: &Arc<PgFeedbackCycleRepository>,
    jobs: &Arc<PgResearchJobRepository>,
    store: Arc<dyn ArtifactStore>,
) -> FeedbackLearningStageAdapter {
    let recipes = Arc::new(
        FeedbackRecipeStageAdapter::try_new(FeedbackRecipeStageDeps {
            cycles: Arc::clone(cycles) as Arc<dyn FeedbackCycleRepository>,
            jobs: Arc::clone(jobs) as Arc<dyn ResearchJobRepository>,
            artifacts: Arc::clone(&store),
            max_recovery_attempts: 3,
        })
        .expect("RecipePlan-stage adapter"),
    );
    FeedbackLearningStageAdapter::try_new(FeedbackLearningStageDeps {
        jobs: Arc::clone(jobs) as Arc<dyn ResearchJobRepository>,
        artifacts: store,
        recipes,
        max_recovery_attempts: 3,
    })
    .expect("learning-stage adapter")
}

pub async fn cycle_drift_rejected() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let cycles = Arc::new(PgFeedbackCycleRepository::new(db.clone()));
    let cycle = record_cycle(cycles.as_ref(), &fixture, false).await.cycle;
    let jobs = Arc::new(PgResearchJobRepository::new(db.clone()));
    let artifact_root = ArtifactRoot::create();
    let store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(artifact_root.path.clone()));
    let adapter = stage_adapter(&cycles, &jobs, store);
    let params = dataset_params(&cycle, &fixture.candidate_family, content_hash('f'));
    let identity =
        FeedbackStageJobIdentity::try_root(cycle.feedback_cycle_id, FeedbackStage::DatasetSeal)
            .expect("DatasetSeal root identity");
    adapter
        .prepare_dataset_seal(&cycle, identity, params, &fixture.candidate_family)
        .expect_err("DatasetSeal adapter must reject candidate-family drift");
    let mut plan_drift = dataset_params(
        &cycle,
        &fixture.candidate_family,
        fixture.candidate_family_hash,
    );
    plan_drift.commands[0].request.training_dataset_id = TrainingDatasetId::from_v7();
    let exact_identity =
        FeedbackStageJobIdentity::try_root(cycle.feedback_cycle_id, FeedbackStage::DatasetSeal)
            .expect("DatasetSeal exact-family identity");
    adapter
        .prepare_dataset_seal(
            &cycle,
            exact_identity,
            plan_drift,
            &fixture.candidate_family,
        )
        .expect_err("DatasetSeal adapter must reject a recipe Dataset-plan drift");
}

pub async fn result_drift_rejected() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let cycles = Arc::new(PgFeedbackCycleRepository::new(db.clone()));
    let cycle = record_cycle(cycles.as_ref(), &fixture, true).await.cycle;
    let jobs = Arc::new(PgResearchJobRepository::new(db.clone()));
    let artifact_root = ArtifactRoot::create();
    let store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(artifact_root.path.clone()));
    let adapter = stage_adapter(&cycles, &jobs, Arc::clone(&store));
    let params = dataset_params(
        &cycle,
        &fixture.second_candidate_family,
        fixture.second_candidate_family_hash,
    );
    let identity =
        FeedbackStageJobIdentity::try_root(cycle.feedback_cycle_id, FeedbackStage::DatasetSeal)
            .expect("DatasetSeal root identity");
    let job = adapter
        .prepare_dataset_seal(
            &cycle,
            identity,
            params.clone(),
            &fixture.second_candidate_family,
        )
        .expect("prepare exact DatasetSeal job");
    let mut results = params.dataset_results();
    results[0].training_dataset_id = TrainingDatasetId::from_v7();
    let artifact = FeedbackLearningStageArtifact::try_new(
        cycle.feedback_cycle_id,
        cycle.idempotency_hash,
        fixture.second_candidate_family_hash,
        params.input_hash().expect("DatasetSeal input hash"),
        None,
        FeedbackLearningStageResults::DatasetSeal(results),
    )
    .expect("self-consistent DatasetSeal artifact with drifted output identity");
    let artifact_ref = persist_artifact(&store, &artifact).await;
    let info = finalize_job(&jobs, job, artifact_ref).await;
    adapter
        .succeeded_dataset_seal(&cycle, &info)
        .await
        .expect_err("terminal artifact must match every frozen Dataset command");
}

struct DatasetChainEvidence {
    previous: FeedbackLearningStageArtifactRef,
    training_dataset_id: TrainingDatasetId,
    calibration_dataset_id: TrainingDatasetId,
}

struct TrainingChainEvidence {
    previous: FeedbackLearningStageArtifactRef,
    model_version_id: ModelVersionId,
}

struct CalibrationChainEvidence {
    previous: FeedbackLearningStageArtifactRef,
    calibrated_model_version_id: ModelVersionId,
    policy_id: DecisionPolicySnapshotId,
}

struct ExactChainFixture {
    _artifact_root: ArtifactRoot,
    cycle: FeedbackCycleInfo,
    lease: FeedbackCycleLeaseGuard,
    cycles: Arc<PgFeedbackCycleRepository>,
    jobs: Arc<PgResearchJobRepository>,
    store: Arc<dyn ArtifactStore>,
    adapter: FeedbackLearningStageAdapter,
    family: FeedbackCandidateFamily,
    family_hash: ContentHash,
    dataset_params: FeedbackDatasetSealJobParams,
    recipe: ContentHash,
}

impl ExactChainFixture {
    async fn new(db: &DatabaseConnection, schema: &FeedbackSchemaFixture, second: bool) -> Self {
        let cycles = Arc::new(PgFeedbackCycleRepository::new(db.clone()));
        let claim = record_cycle(cycles.as_ref(), schema, second).await;
        let cycle = claim.cycle;
        let (family, family_hash) = if second {
            (
                schema.second_candidate_family.clone(),
                schema.second_candidate_family_hash,
            )
        } else {
            (
                schema.candidate_family.clone(),
                schema.candidate_family_hash,
            )
        };
        let jobs = Arc::new(PgResearchJobRepository::new(db.clone()));
        let artifact_root = ArtifactRoot::create();
        let store: Arc<dyn ArtifactStore> =
            Arc::new(LocalArtifactStore::new(artifact_root.path.clone()));
        persist_recipe_plan_fixture(
            cycles.as_ref(),
            jobs.as_ref(),
            &store,
            claim.lease,
            &cycle,
            &family,
            2,
        )
        .await;
        let adapter = stage_adapter(&cycles, &jobs, Arc::clone(&store));
        let dataset_params = dataset_params(&cycle, &family, family_hash);
        let recipe = family.candidates()[0].candidate_recipe_hash();
        Self {
            _artifact_root: artifact_root,
            cycle,
            lease: claim.lease,
            cycles,
            jobs,
            store,
            adapter,
            family,
            family_hash,
            dataset_params,
            recipe,
        }
    }

    async fn seal_dataset(&self) -> DatasetChainEvidence {
        let identity = FeedbackStageJobIdentity::try_root(
            self.cycle.feedback_cycle_id,
            FeedbackStage::DatasetSeal,
        )
        .expect("DatasetSeal root identity");
        let job = self
            .adapter
            .prepare_dataset_seal(
                &self.cycle,
                identity,
                self.dataset_params.clone(),
                &self.family,
            )
            .expect("prepare DatasetSeal");
        let artifact = FeedbackLearningStageArtifact::try_new(
            self.cycle.feedback_cycle_id,
            self.cycle.idempotency_hash,
            self.family_hash,
            self.dataset_params
                .input_hash()
                .expect("DatasetSeal input hash"),
            None,
            FeedbackLearningStageResults::DatasetSeal(self.dataset_params.dataset_results()),
        )
        .expect("DatasetSeal artifact");
        let artifact_ref = persist_artifact(&self.store, &artifact).await;
        let info = finalize_job(&self.jobs, job, artifact_ref).await;
        let first = self
            .adapter
            .succeeded_dataset_seal(&self.cycle, &info)
            .await
            .expect("DatasetSeal terminal read-back");
        let second = self
            .adapter
            .succeeded_dataset_seal(&self.cycle, &info)
            .await
            .expect("DatasetSeal restart read-back");
        assert_eq!(first, second);
        DatasetChainEvidence {
            previous: artifact
                .reference_from_job(&info)
                .expect("DatasetSeal predecessor reference"),
            training_dataset_id: candidate_dataset(&self.dataset_params, DatasetPurpose::Training),
            calibration_dataset_id: candidate_dataset(
                &self.dataset_params,
                DatasetPurpose::Calibration,
            ),
        }
    }

    async fn train(&self, dataset: &DatasetChainEvidence) -> TrainingChainEvidence {
        let model_version_id = ModelVersionId::from_v7();
        let model_run_id = ModelRunId::from_v7();
        let params = FeedbackTrainingJobParams::try_new(
            self.cycle.feedback_cycle_id,
            self.cycle.idempotency_hash,
            self.family_hash,
            dataset.previous.clone(),
            vec![FeedbackTrainingCommand {
                candidate_recipe_hash: self.recipe,
                resource_budget: self.family.candidates()[0].resource_budget(),
                params: ModelTrainJobParams {
                    model_version_id,
                    model_run_id,
                    request: TrainModelRequest {
                        training_dataset_id: dataset.training_dataset_id,
                        reason: "feedback learning-stage contract".to_owned(),
                    },
                },
            }],
        )
        .expect("Training params");
        let identity = FeedbackStageJobIdentity::try_root(
            self.cycle.feedback_cycle_id,
            FeedbackStage::Training,
        )
        .expect("Training root identity");
        let job = self
            .adapter
            .prepare_training(&self.cycle, identity, params.clone())
            .await
            .expect("prepare Training after exact DatasetSeal read-back");
        let artifact = FeedbackLearningStageArtifact::try_new(
            self.cycle.feedback_cycle_id,
            self.cycle.idempotency_hash,
            self.family_hash,
            params.input_hash().expect("Training input hash"),
            Some(params.previous.clone()),
            FeedbackLearningStageResults::Training(vec![FeedbackTrainingStageResult {
                candidate_recipe_hash: self.recipe,
                model_version_id,
                model_run_id,
                training_dataset_id: dataset.training_dataset_id,
                model_artifact_hash: content_hash('6'),
                serving_contract_hash: content_hash('7'),
                training_input_hash: content_hash('8'),
            }]),
        )
        .expect("Training artifact");
        let artifact_ref = persist_artifact(&self.store, &artifact).await;
        let info = finalize_job(&self.jobs, job, artifact_ref).await;
        let first = self
            .adapter
            .succeeded_training(&self.cycle, &info)
            .await
            .expect("Training terminal read-back");
        let second = self
            .adapter
            .succeeded_training(&self.cycle, &info)
            .await
            .expect("Training restart read-back");
        assert_eq!(first, second);
        TrainingChainEvidence {
            previous: artifact
                .reference_from_job(&info)
                .expect("Training predecessor reference"),
            model_version_id,
        }
    }

    async fn calibrate(
        &self,
        dataset: &DatasetChainEvidence,
        training: &TrainingChainEvidence,
    ) -> CalibrationChainEvidence {
        let model_run_id = ModelRunId::from_v7();
        let policy_id = self
            .family
            .candidate(self.recipe)
            .expect("frozen candidate recipe")
            .decision_policy_snapshot_id();
        let params = FeedbackCalibrationJobParams::try_new(
            self.cycle.feedback_cycle_id,
            self.cycle.idempotency_hash,
            self.family_hash,
            training.previous.clone(),
            vec![FeedbackCalibrationCommand {
                candidate_recipe_hash: self.recipe,
                resource_budget: self.family.candidates()[0].resource_budget(),
                params: ModelCalibrationFitJobParams {
                    model_run_id,
                    request: FitModelCalibratorRequest {
                        model_version_id: training.model_version_id,
                        calibration_dataset_id: dataset.calibration_dataset_id,
                        method: CalibrationMethod::Platt,
                        reason: "feedback calibration contract".to_owned(),
                    },
                    decision_policy_snapshot_id: policy_id,
                    downside_source: DownsideSource::MfeMae,
                    actor: GovernanceActor::system(),
                },
            }],
        )
        .expect("Calibration params");
        let identity = FeedbackStageJobIdentity::try_root(
            self.cycle.feedback_cycle_id,
            FeedbackStage::Calibration,
        )
        .expect("Calibration root identity");
        let job = self
            .adapter
            .prepare_calibration(&self.cycle, identity, params.clone())
            .await
            .expect("prepare Calibration after exact Training chain read-back");
        let calibrated_model_version_id = ModelVersionId::from_v7();
        let artifact = FeedbackLearningStageArtifact::try_new(
            self.cycle.feedback_cycle_id,
            self.cycle.idempotency_hash,
            self.family_hash,
            params.input_hash().expect("Calibration input hash"),
            Some(params.previous.clone()),
            FeedbackLearningStageResults::Calibration(vec![
                FeedbackCalibrationStageResult::Calibrated {
                    candidate_recipe_hash: self.recipe,
                    source_model_version_id: training.model_version_id,
                    model_run_id,
                    calibration_dataset_id: dataset.calibration_dataset_id,
                    method: CalibrationMethod::Platt,
                    calibration_artifact_id: CalibrationArtifactId::from_v7(),
                    calibration_artifact_hash: content_hash('9'),
                    calibrated_model_version_id,
                    calibrated_model_artifact_hash: content_hash('a'),
                    calibrated_serving_contract_hash: content_hash('b'),
                    training_input_hash: content_hash('8'),
                    sample_count: 100,
                },
            ]),
        )
        .expect("Calibration artifact");
        let artifact_ref = persist_artifact(&self.store, &artifact).await;
        let info = finalize_job(&self.jobs, job, artifact_ref).await;
        let first = self
            .adapter
            .succeeded_calibration(&self.cycle, &info)
            .await
            .expect("Calibration terminal read-back");
        let second = self
            .adapter
            .succeeded_calibration(&self.cycle, &info)
            .await
            .expect("Calibration restart read-back");
        assert_eq!(first, second);
        CalibrationChainEvidence {
            previous: artifact
                .reference_from_job(&info)
                .expect("Calibration predecessor reference"),
            calibrated_model_version_id,
            policy_id,
        }
    }

    async fn run_cpcv(
        &self,
        dataset: &DatasetChainEvidence,
        calibration: &CalibrationChainEvidence,
    ) -> FeedbackLearningStageArtifactRef {
        let model_run_id = ModelRunId::from_v7();
        let path_set_id = BacktestPathSetId::from_v7();
        let params = FeedbackCpcvJobParams::try_new(
            self.cycle.feedback_cycle_id,
            self.cycle.idempotency_hash,
            self.family_hash,
            calibration.previous.clone(),
            vec![FeedbackCpcvCommand {
                candidate_recipe_hash: self.recipe,
                resource_budget: self.family.candidates()[0].resource_budget(),
                cpcv_spec: self.family.candidates()[0].cpcv_spec().clone(),
                params: CpcvBacktestJobParams {
                    model_version_id: calibration.calibrated_model_version_id,
                    model_run_id,
                    request: RunCpcvBacktestRequest {
                        training_dataset_id: dataset.training_dataset_id,
                        decision_policy_snapshot_id: calibration.policy_id,
                        reason: "feedback CPCV contract".to_owned(),
                        path_set_id: Some(path_set_id),
                    },
                },
            }],
        )
        .expect("CPCV params");
        let identity =
            FeedbackStageJobIdentity::try_root(self.cycle.feedback_cycle_id, FeedbackStage::Cpcv)
                .expect("CPCV root identity");
        let job = self
            .adapter
            .prepare_cpcv(&self.cycle, identity, params.clone())
            .await
            .expect("prepare CPCV after exact Calibration chain read-back");
        let artifact = FeedbackLearningStageArtifact::try_new(
            self.cycle.feedback_cycle_id,
            self.cycle.idempotency_hash,
            self.family_hash,
            params.input_hash().expect("CPCV input hash"),
            Some(params.previous.clone()),
            FeedbackLearningStageResults::Cpcv(vec![FeedbackCpcvStageResult::Evaluated {
                candidate_recipe_hash: self.recipe,
                model_version_id: calibration.calibrated_model_version_id,
                training_dataset_id: dataset.training_dataset_id,
                path_set_id,
                model_run_id,
                path_set_hash: content_hash('e'),
            }]),
        )
        .expect("CPCV artifact");
        let artifact_ref = persist_artifact(&self.store, &artifact).await;
        let info = finalize_job(&self.jobs, job, artifact_ref).await;
        let first = self
            .adapter
            .succeeded_cpcv(&self.cycle, &info)
            .await
            .expect("CPCV terminal read-back");
        let second = self
            .adapter
            .succeeded_cpcv(&self.cycle, &info)
            .await
            .expect("CPCV restart read-back");
        assert_eq!(first, second);

        let terminal = info
            .result_artifact()
            .expect("CPCV terminal artifact reference");
        let tampered_store: Arc<dyn ArtifactStore> = Arc::new(ReadTamperArtifactStoreFixture::new(
            Arc::clone(&self.store),
            terminal.uri,
            b"{}".to_vec(),
        ));
        stage_adapter(&self.cycles, &self.jobs, tampered_store)
            .succeeded_cpcv(&self.cycle, &info)
            .await
            .expect_err("CPCV restart must reject tampered object bytes");
        artifact
            .reference_from_job(&info)
            .expect("CPCV reservation predecessor reference")
    }
}

pub async fn exact_chain_replays() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let schema = Box::pin(prepare_fixture(&db)).await;
    let first = ExactChainFixture::new(&db, &schema, false).await;
    let dataset = first.seal_dataset().await;
    let training = first.train(&dataset).await;
    let calibration = first.calibrate(&dataset, &training).await;
    let cpcv = first.run_cpcv(&dataset, &calibration).await;
    let reservations =
        FeedbackEvaluationReservationService::new(FeedbackEvaluationReservationDeps {
            cycles: Arc::new(PgFeedbackCycleRepository::new(db.clone()))
                as Arc<dyn FeedbackCycleRepository>,
            datasets: Arc::new(PgTrainingDatasetRepository::new(db.clone()))
                as Arc<dyn TrainingDatasetRepository>,
            learning_stages: Arc::new(stage_adapter(
                &first.cycles,
                &first.jobs,
                Arc::clone(&first.store),
            )),
        });
    let inserted = reservations
        .reserve(first.lease, cpcv.clone())
        .await
        .expect("reserve Evaluation Dataset before comparison");
    let inserted = match inserted {
        FeedbackEvaluationWriteOutcome::Inserted(info) => info,
        FeedbackEvaluationWriteOutcome::AlreadyPresent(_) => {
            panic!("first Evaluation reservation was not inserted")
        }
    };
    inserted
        .validate()
        .expect("validate Evaluation reservation");
    assert_eq!(inserted.reserved_at, inserted.created_at);
    let replayed = reservations
        .reserve(first.lease, cpcv)
        .await
        .expect("confirm exact reservation after response loss");
    let replayed = match replayed {
        FeedbackEvaluationWriteOutcome::AlreadyPresent(info) => info,
        FeedbackEvaluationWriteOutcome::Inserted(_) => {
            panic!("exact Evaluation reservation retry inserted twice")
        }
    };
    assert_eq!(
        replayed.feedback_evaluation_use_id,
        inserted.feedback_evaluation_use_id
    );

    let second = ExactChainFixture::new(&db, &schema, true).await;
    let second_dataset = second.seal_dataset().await;
    let second_training = second.train(&second_dataset).await;
    let second_calibration = second.calibrate(&second_dataset, &second_training).await;
    let second_cpcv = second.run_cpcv(&second_dataset, &second_calibration).await;
    let second_reservation =
        FeedbackEvaluationReservationService::new(FeedbackEvaluationReservationDeps {
            cycles: Arc::new(PgFeedbackCycleRepository::new(db.clone()))
                as Arc<dyn FeedbackCycleRepository>,
            datasets: Arc::new(PgTrainingDatasetRepository::new(db))
                as Arc<dyn TrainingDatasetRepository>,
            learning_stages: Arc::new(stage_adapter(
                &second.cycles,
                &second.jobs,
                Arc::clone(&second.store),
            )),
        })
        .reserve(second.lease, second_cpcv)
        .await
        .expect("another cadence window owns a distinct Evaluation Dataset");
    let FeedbackEvaluationWriteOutcome::Inserted(second_use) = second_reservation else {
        panic!("first reservation for the prior cadence window must be inserted");
    };
    assert_ne!(
        second_use.evaluation_dataset_id,
        inserted.evaluation_dataset_id
    );
}
