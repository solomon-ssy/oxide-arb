//! Coverage/drift stage contracts against real job, cycle, and parity ledgers.

use std::{env, fs, path::PathBuf, slice, sync::Arc};

use chrono::{Duration, Utc};
use quant_pivot_core::service::{
    feedback_coordinator::{FeedbackStageDirective, FeedbackStageSuccess},
    feedback_signal_stage::{FeedbackSignalStageAdapter, FeedbackSignalStageDeps},
};
use quant_pivot_models::{
    domain::quant::{
        CompleteFeatureParityRun, FeatureParityStateInfo, FeedbackCohortWindow, FeedbackCycleInfo,
        FeedbackStageJobIdentity, NewDriftReport, NewFeatureParityRun, NewFeedbackCycle,
        NewResearchJob, ResearchJobArtifactRef, ResearchJobFinalization, ResearchJobInfo,
        ResearchJobResultRef,
    },
    enums::quant::{
        DatasetPurpose, FeatureParityRunKind, FeatureParityRunStatus, FeedbackCycleStatus,
        FeedbackDecision, FeedbackDriftMetric, FeedbackStage, ResearchJobResultKind,
    },
    types::{
        ArtifactUri, CapabilityRegistryHashes, DatasetCohortCounts, FeatureParityRunId,
        FeatureValue, FeedbackCoverageArtifactId, FeedbackCycleId, FeedbackDriftArtifactId,
        ModelVersionId, RecommendationId, ResearchJobParams, RoleCode, TrainingDatasetId, WorkerId,
        stable_name::FeatureName,
    },
};
use quant_pivot_repository::{
    postgres::{PgFeatureParityRepository, PgFeedbackCycleRepository, PgResearchJobRepository},
    traits::{
        FeatureParityRepository, FeedbackCycleLeaseGuard, FeedbackCycleRepository,
        ResearchJobEnqueueOutcome, ResearchJobRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
    feedback::{
        ChampionBaselineRef, ConceptDriftDetail, CoverageGateInput, CoverageGateOutcome,
        CoverageNoActionReason, FEEDBACK_COVERAGE_ARTIFACT_FORMAT_VERSION,
        FEEDBACK_DRIFT_ARTIFACT_FORMAT_VERSION, FeatureDriftDetail, FeedbackCoverageArtifact,
        FeedbackCoverageCodec, FeedbackCoverageCohorts, FeedbackDriftArtifact, FeedbackDriftCodec,
        FeedbackMatureLabel, LabelDriftDetail, drift_gate, drift_observations,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg, support::artifact_store::ReadTamperArtifactStoreFixture,
};
use sea_orm::DatabaseConnection;

use super::feedback_boot_schema::{FeedbackSchemaFixture, content_hash, prepare_fixture};

const JOB_LEASE_SECS: i64 = 90;

struct ArtifactRoot {
    path: PathBuf,
}

impl ArtifactRoot {
    fn create() -> Self {
        let path = env::temp_dir().join(format!(
            "quant-pivot-w2-f06-{}",
            FeedbackDriftArtifactId::from_cycle_id(FeedbackCycleId::from_v7())
        ));
        fs::create_dir_all(&path).expect("create F06 artifact root");
        Self { path }
    }
}

impl Drop for ArtifactRoot {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).expect("remove F06 artifact root");
    }
}

fn empty_counts() -> DatasetCohortCounts {
    DatasetCohortCounts::try_new(0, 0, 0, Vec::new(), Vec::new()).expect("empty cohort counts")
}

#[derive(Clone, Copy)]
pub enum CoverageScenario {
    NoLabels,
    LowCoverage,
    Advance,
}

fn baseline_ref(evaluation_window: &FeedbackCohortWindow) -> ChampionBaselineRef {
    ChampionBaselineRef {
        training_dataset_id: TrainingDatasetId::from_v7(),
        purpose: DatasetPurpose::Training,
        dataset_hash: content_hash('1'),
        manifest_hash: content_hash('2'),
        artifact_bytes_hash: content_hash('3'),
        parquet_uri: ArtifactUri::parse("s3://feedback-stage/champion-baseline.parquet")
            .expect("champion baseline URI"),
        feature_schema_hash: content_hash('4'),
        label_schema_hash: content_hash('5'),
        window_start: evaluation_window.window_start() - Duration::days(2),
        window_end: evaluation_window.window_start() - Duration::days(1),
        pit_cutoff: evaluation_window.window_start(),
        sample_count: 1,
    }
}

pub fn coverage_artifact(
    cycle: &FeedbackCycleInfo,
    capability_registry_hashes: CapabilityRegistryHashes,
    scenario: CoverageScenario,
) -> FeedbackCoverageArtifact {
    let profile = cycle
        .profile_ref
        .resolve_builtin_research_profile()
        .expect("resolve cycle ResearchProfile");
    let policy = profile.spec.feedback_policy;
    let evaluation_window = FeedbackCohortWindow::try_new(
        cycle.profile_ref.clone(),
        cycle.label_cutoff - Duration::days(i64::from(policy.evaluation_window_days)),
        cycle.label_cutoff,
    )
    .expect("feedback evaluation window");
    let champion_baseline = baseline_ref(&evaluation_window);
    let (policy_count, mature_count, new_mature_count) = match scenario {
        CoverageScenario::NoLabels => (0, 0, 0),
        CoverageScenario::LowCoverage => (
            policy
                .minimum_mature_labels
                .checked_mul(1_000)
                .expect("low-coverage denominator fits u64"),
            policy.minimum_mature_labels,
            policy.minimum_mature_labels,
        ),
        CoverageScenario::Advance => (
            policy.minimum_mature_labels,
            policy.minimum_mature_labels,
            policy.minimum_mature_labels,
        ),
    };
    let other_model = ModelVersionId::from_v7();
    assert_ne!(other_model, cycle.champion_model_version_id);
    let mut mature_labels = (0..mature_count)
        .map(|_| FeedbackMatureLabel {
            recommendation_id: RecommendationId::from_v7(),
            model_version_id: other_model,
            decision_at: evaluation_window.window_start() + Duration::seconds(30),
            candidate_available_at: evaluation_window.window_start() + Duration::minutes(1),
            label_available_at: evaluation_window.window_start() + Duration::minutes(2),
            outcome_hash: content_hash('6'),
        })
        .collect::<Vec<_>>();
    mature_labels.sort_by_key(|label| {
        (
            label.recommendation_id.as_uuid(),
            label.candidate_available_at,
            label.label_available_at,
        )
    });
    let model_learning =
        DatasetCohortCounts::try_new(mature_count, mature_count, 0, Vec::new(), Vec::new())
            .expect("model-learning coverage counts");
    let policy_evaluation = DatasetCohortCounts::try_new(
        policy_count,
        policy_count,
        policy_count,
        Vec::new(),
        Vec::new(),
    )
    .expect("policy-evaluation coverage counts");
    let gate_input = CoverageGateInput {
        policy_evaluation_count: policy_count,
        mature_label_count: mature_count,
        new_mature_label_count: new_mature_count,
        minimum_mature_labels: policy.minimum_mature_labels,
        minimum_new_mature_labels: policy.minimum_new_mature_labels,
        minimum_coverage: policy.minimum_coverage,
    };
    let artifact = FeedbackCoverageArtifact {
        format_version: FEEDBACK_COVERAGE_ARTIFACT_FORMAT_VERSION,
        artifact_id: FeedbackCoverageArtifactId::from_cycle_id(cycle.feedback_cycle_id),
        feedback_cycle_id: cycle.feedback_cycle_id,
        cycle_idempotency_hash: cycle.idempotency_hash,
        cycle_key: cycle.idempotency_key.clone(),
        profile_ref: cycle.profile_ref.clone(),
        feedback_policy: policy,
        feedback_policy_hash: cycle.feedback_policy_hash,
        capability_registry_hashes,
        champion_model_version_id: cycle.champion_model_version_id,
        champion_serving_contract_hash: cycle.champion_serving_contract_hash,
        evaluation_window,
        champion_baseline,
        cohorts: FeedbackCoverageCohorts {
            model_learning,
            execution_learning: empty_counts(),
            policy_evaluation,
        },
        mature_labels,
        new_mature_label_count: new_mature_count,
        gate_input,
        gate_outcome: gate_input
            .evaluate()
            .expect("evaluate coverage fixture gate"),
        champion_rows: Vec::new(),
        champion_examples: Vec::new(),
    };
    artifact.validate().expect("validate coverage artifact");
    artifact
}

fn drift_artifact(
    cycle: &FeedbackCycleInfo,
    coverage: &FeedbackCoverageArtifact,
    coverage_ref: &ResearchJobArtifactRef,
) -> FeedbackDriftArtifact {
    let data_detail = FeatureDriftDetail::compute(
        FeatureName::from_static("test.parity_isolated"),
        &[
            Some(FeatureValue::Bool(false)),
            Some(FeatureValue::Bool(false)),
        ],
        &[
            Some(FeatureValue::Bool(false)),
            Some(FeatureValue::Bool(false)),
        ],
    )
    .expect("stable discrete feature drift");
    let concept_detail = ConceptDriftDetail {
        baseline_scored_count: 0,
        evaluation_scored_count: 0,
        summary: None,
    };
    let label_detail = LabelDriftDetail {
        baseline_counts: vec![0; 11],
        evaluation_counts: vec![0; 11],
        divergence: None,
    };
    let observations = drift_observations(
        &coverage.feedback_policy,
        slice::from_ref(&data_detail),
        &concept_detail,
        &label_detail,
    )
    .expect("aggregate drift headers");
    let artifact = FeedbackDriftArtifact {
        format_version: FEEDBACK_DRIFT_ARTIFACT_FORMAT_VERSION,
        artifact_id: FeedbackDriftArtifactId::from_cycle_id(cycle.feedback_cycle_id),
        feedback_cycle_id: cycle.feedback_cycle_id,
        cycle_idempotency_hash: cycle.idempotency_hash,
        coverage_artifact_id: coverage.artifact_id,
        coverage_artifact_uri: coverage_ref.uri.clone(),
        coverage_artifact_hash: coverage_ref.content_hash,
        profile_ref: coverage.profile_ref.clone(),
        feedback_policy: coverage.feedback_policy.clone(),
        feedback_policy_hash: coverage.feedback_policy_hash,
        champion_model_version_id: coverage.champion_model_version_id,
        champion_serving_contract_hash: coverage.champion_serving_contract_hash,
        champion_baseline: coverage.champion_baseline.clone(),
        evaluation_window: coverage.evaluation_window.clone(),
        comparison_window_start: Some(coverage.evaluation_window.window_start()),
        data_details: vec![data_detail],
        concept_detail,
        label_detail,
        gate_outcome: drift_gate(&observations),
        observations,
        observed_at: coverage.evaluation_window.cutoff(),
    };
    artifact.validate().expect("validate drift artifact");
    artifact
}

pub async fn persist_coverage(
    store: &Arc<dyn ArtifactStore>,
    artifact: &FeedbackCoverageArtifact,
) -> ResearchJobArtifactRef {
    let bytes = FeedbackCoverageCodec::encode(artifact).expect("encode coverage artifact");
    let content_hash = FeedbackCoverageCodec::bytes_hash(&bytes);
    let key = ArtifactKey::new(
        ArtifactNamespace::FeedbackCoverage,
        artifact.artifact_id.to_string(),
        "json",
    )
    .expect("coverage artifact key");
    let uri = store
        .put(key, &bytes)
        .await
        .expect("persist coverage artifact");
    ResearchJobArtifactRef { uri, content_hash }
}

async fn persist_drift(
    store: &Arc<dyn ArtifactStore>,
    artifact: &FeedbackDriftArtifact,
) -> ResearchJobArtifactRef {
    let bytes = FeedbackDriftCodec::encode(artifact).expect("encode drift artifact");
    let content_hash = FeedbackDriftCodec::bytes_hash(&bytes);
    let key = ArtifactKey::new(
        ArtifactNamespace::FeedbackDrift,
        artifact.artifact_id.to_string(),
        "json",
    )
    .expect("drift artifact key");
    let uri = store
        .put(key, &bytes)
        .await
        .expect("persist drift artifact");
    ResearchJobArtifactRef { uri, content_hash }
}

async fn finalize_job(
    jobs: &PgResearchJobRepository,
    job: NewResearchJob,
    result: ResearchJobResultRef,
    artifact: ResearchJobArtifactRef,
) -> ResearchJobInfo {
    let job_id = job.job_id;
    let kind = job.kind;
    let queued = match jobs.enqueue(job).await.expect("enqueue feedback stage job") {
        ResearchJobEnqueueOutcome::Inserted(info)
        | ResearchJobEnqueueOutcome::AlreadyPresent(info) => info,
    };
    assert_eq!(queued.job_id, job_id);
    let worker = WorkerId::from_v7();
    let leased = jobs
        .lease_next(
            &[kind],
            &worker,
            Utc::now() + Duration::seconds(JOB_LEASE_SECS),
        )
        .await
        .expect("lease feedback stage job")
        .expect("queued feedback stage job");
    assert_eq!(leased.job_id, job_id);
    jobs.finalize(
        &job_id,
        &worker,
        ResearchJobFinalization::succeeded(Some(result), Some(artifact), None),
    )
    .await
    .expect("finalize feedback stage job")
}

async fn open_parity(parity: &PgFeatureParityRepository) -> FeatureParityStateInfo {
    let window_end = Utc::now();
    let run = parity
        .create_run(NewFeatureParityRun {
            run_id: FeatureParityRunId::from_v7(),
            kind: FeatureParityRunKind::Full,
            status: FeatureParityRunStatus::Queued,
            window_start: window_end - Duration::hours(1),
            window_end,
            report_id: None,
            model_version_id: None,
            training_dataset_id: None,
            triggered_by: "feedback-drift-isolation".to_owned(),
            requested_by: Some("system-test".to_owned()),
            acting_role: RoleCode::new("system"),
            reason: "prove statistical drift cannot mutate parity".to_owned(),
            total_count: 0,
            compared_count: 0,
            matched_count: 0,
            mismatched_count: 0,
            pending_materialization_count: 0,
            feature_contract_hash: Some(content_hash('7')),
            transform_hash: None,
            failure_code: None,
            failure_detail: None,
            started_at: None,
            pending_since: None,
            containment_completed_at: None,
            finished_at: None,
        })
        .await
        .expect("create parity mismatch run");
    parity
        .mark_running(&run.run_id)
        .await
        .expect("start parity mismatch run");
    parity
        .complete_run(
            &run.run_id,
            CompleteFeatureParityRun {
                status: FeatureParityRunStatus::Mismatched,
                total_count: 1,
                compared_count: 1,
                matched_count: 0,
                mismatched_count: 1,
                pending_materialization_count: 0,
                feature_contract_hash: Some(content_hash('7')),
                transform_hash: Some(content_hash('8')),
                failure_code: None,
                failure_detail: None,
            },
        )
        .await
        .expect("complete parity mismatch run");
    parity
        .mark_containment_complete(&run.run_id)
        .await
        .expect("complete parity containment");
    parity
        .current_state()
        .await
        .expect("read open parity latch")
        .expect("parity latch initialized")
}

async fn record_cycle(
    cycles: &PgFeedbackCycleRepository,
    fixture: &FeedbackSchemaFixture,
    cycle: &NewFeedbackCycle,
    worker: WorkerId,
) -> (FeedbackCycleInfo, FeedbackCycleLeaseGuard) {
    let cycle_id = cycle.feedback_cycle_id();
    cycles
        .record_trigger(
            cycle.clone(),
            fixture.stage_event(cycle_id, "feedback-signal-stage"),
        )
        .await
        .expect("record feedback cycle");
    let claim = cycles
        .claim_cycle(worker, 90)
        .await
        .expect("claim feedback cycle")
        .expect("queued feedback cycle");
    assert_eq!(claim.cycle.feedback_cycle_id, cycle_id);
    assert_eq!(claim.cycle.status, FeedbackCycleStatus::Running);
    (claim.cycle, claim.lease)
}

fn assert_no_action(success: &FeedbackStageSuccess) {
    assert!(matches!(
        success.directive(),
        FeedbackStageDirective::Complete(terminal)
            if terminal.decision() == Some(FeedbackDecision::NoAction)
    ));
}

struct SignalStageHarness {
    cycles: Arc<PgFeedbackCycleRepository>,
    jobs: Arc<PgResearchJobRepository>,
    store: Arc<dyn ArtifactStore>,
    adapter: FeedbackSignalStageAdapter,
}

fn signal_harness(
    db: &DatabaseConnection,
    artifact_root: &ArtifactRoot,
) -> (SignalStageHarness, PgFeatureParityRepository) {
    let cycles = Arc::new(PgFeedbackCycleRepository::new(db.clone()));
    let jobs = Arc::new(PgResearchJobRepository::new(db.clone()));
    let parity = PgFeatureParityRepository::new(db.clone());
    let store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(artifact_root.path.clone()));
    let adapter = FeedbackSignalStageAdapter::try_new(FeedbackSignalStageDeps {
        jobs: Arc::clone(&jobs) as Arc<dyn ResearchJobRepository>,
        artifacts: Arc::clone(&store),
        max_recovery_attempts: 3,
    })
    .expect("feedback signal stage adapter");
    (
        SignalStageHarness {
            cycles,
            jobs,
            store,
            adapter,
        },
        parity,
    )
}

fn assert_parity_unchanged(before: &FeatureParityStateInfo, after: &FeatureParityStateInfo) {
    assert_eq!(after.state_id, before.state_id);
    assert_eq!(after.state, before.state);
    assert_eq!(after.transition, before.transition);
    assert_eq!(after.cause_run_id, before.cause_run_id);
    assert_eq!(after.recovery_run_id, before.recovery_run_id);
    assert_eq!(after.previous_state_id, before.previous_state_id);
}

impl SignalStageHarness {
    async fn verify_coverage_no_action(
        &self,
        fixture: &FeedbackSchemaFixture,
        new_cycle: &NewFeedbackCycle,
        scenario: CoverageScenario,
        expected_reason: CoverageNoActionReason,
    ) {
        let (cycle, lease) = record_cycle(
            self.cycles.as_ref(),
            fixture,
            new_cycle,
            WorkerId::from_v7(),
        )
        .await;
        let family = if new_cycle.feedback_cycle_id() == fixture.cycle_id {
            &fixture.candidate_family
        } else {
            &fixture.second_candidate_family
        };
        let artifact = coverage_artifact(
            &cycle,
            family
                .shared_evaluation()
                .source_lineage
                .capability_registry_hashes
                .clone(),
            scenario,
        );
        assert!(matches!(
            artifact.gate_outcome,
            CoverageGateOutcome::NoAction { reason, .. } if reason == expected_reason
        ));
        let artifact_ref = persist_coverage(&self.store, &artifact).await;
        let identity =
            FeedbackStageJobIdentity::try_root(cycle.feedback_cycle_id, FeedbackStage::Coverage)
                .expect("coverage root identity");
        let job = self
            .adapter
            .prepare_coverage(&cycle, identity)
            .expect("prepare coverage job");
        let info = finalize_job(
            self.jobs.as_ref(),
            job,
            ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackCoverageArtifact,
                id: artifact.artifact_id.as_uuid(),
            },
            artifact_ref,
        )
        .await;
        let result = info
            .result_artifact()
            .expect("terminal coverage artifact reference");
        let tampered_store: Arc<dyn ArtifactStore> = Arc::new(ReadTamperArtifactStoreFixture::new(
            Arc::clone(&self.store),
            result.uri,
            b"{}".to_vec(),
        ));
        let tampered_adapter = FeedbackSignalStageAdapter::try_new(FeedbackSignalStageDeps {
            jobs: Arc::clone(&self.jobs) as Arc<dyn ResearchJobRepository>,
            artifacts: tampered_store,
            max_recovery_attempts: 3,
        })
        .expect("tampered feedback stage adapter");
        tampered_adapter
            .succeeded_coverage(&cycle, &info)
            .await
            .expect_err("stage adapter must reject tampered coverage bytes");
        let success = self
            .adapter
            .succeeded_coverage(&cycle, &info)
            .await
            .expect("validate terminal coverage NoAction");
        assert_no_action(&success);
        let FeedbackStageDirective::Complete(terminal) = success.directive() else {
            panic!("coverage NoAction must complete the cycle");
        };
        assert_eq!(terminal.reason_code(), expected_reason.as_str());
        let drift_identity =
            FeedbackStageJobIdentity::try_root(cycle.feedback_cycle_id, FeedbackStage::Drift)
                .expect("drift root identity");
        self.adapter
            .prepare_drift(&cycle, drift_identity)
            .await
            .expect_err("coverage NoAction cannot advance to drift");
        self.cycles
            .finalize_cycle(lease, terminal.clone())
            .await
            .expect("finalize coverage NoAction cycle");
    }

    async fn verify_drift(&self, fixture: &FeedbackSchemaFixture) {
        let (cycle, lease) = record_cycle(
            self.cycles.as_ref(),
            fixture,
            &fixture.second_cycle,
            WorkerId::from_v7(),
        )
        .await;
        let coverage = coverage_artifact(
            &cycle,
            fixture
                .second_candidate_family
                .shared_evaluation()
                .source_lineage
                .capability_registry_hashes
                .clone(),
            CoverageScenario::Advance,
        );
        assert!(matches!(
            coverage.gate_outcome,
            CoverageGateOutcome::Advance { .. }
        ));
        let coverage_ref = persist_coverage(&self.store, &coverage).await;
        let coverage_identity =
            FeedbackStageJobIdentity::try_root(cycle.feedback_cycle_id, FeedbackStage::Coverage)
                .expect("advance coverage identity");
        let coverage_job = self
            .adapter
            .prepare_coverage(&cycle, coverage_identity)
            .expect("prepare advancing coverage");
        finalize_job(
            self.jobs.as_ref(),
            coverage_job,
            ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackCoverageArtifact,
                id: coverage.artifact_id.as_uuid(),
            },
            coverage_ref.clone(),
        )
        .await;
        let drift_identity =
            FeedbackStageJobIdentity::try_root(cycle.feedback_cycle_id, FeedbackStage::Drift)
                .expect("drift identity");
        let drift_job = self
            .adapter
            .prepare_drift(&cycle, drift_identity)
            .await
            .expect("prepare exact drift job");
        let ResearchJobParams::FeedbackDrift(drift_params) = &drift_job.params_json else {
            panic!("drift stage emitted another job kind");
        };
        assert_eq!(
            drift_params.coverage_artifact_hash,
            coverage_ref.content_hash
        );
        let artifact = drift_artifact(&cycle, &coverage, &coverage_ref);
        let artifact_ref = persist_drift(&self.store, &artifact).await;
        let info = finalize_job(
            self.jobs.as_ref(),
            drift_job,
            ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackDriftArtifact,
                id: artifact.artifact_id.as_uuid(),
            },
            artifact_ref,
        )
        .await;
        let success = self
            .adapter
            .succeeded_drift(&cycle, &info)
            .await
            .expect("validate terminal drift artifact");
        assert_no_action(&success);
        assert_eq!(success.drift_reports().len(), 4);
        let metrics = success
            .drift_reports()
            .iter()
            .map(NewDriftReport::metric)
            .collect::<Vec<_>>();
        assert_eq!(
            metrics,
            vec![
                FeedbackDriftMetric::PopulationStabilityIndex,
                FeedbackDriftMetric::KolmogorovSmirnovPValue,
                FeedbackDriftMetric::RankIcDrop,
                FeedbackDriftMetric::JensenShannonDivergence,
            ]
        );
        for report in success.drift_reports() {
            self.cycles
                .append_drift(lease, report.clone())
                .await
                .expect("append drift header");
        }
        assert_eq!(
            self.cycles
                .list_drift_reports(&cycle.feedback_cycle_id)
                .await
                .expect("list persisted drift headers")
                .len(),
            4
        );
    }
}

pub async fn signal_stages_isolate_parity() {
    let (pool, container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let artifact_root = ArtifactRoot::create();
    let (harness, parity) = signal_harness(&db, &artifact_root);
    let parity_before = open_parity(&parity).await;
    harness
        .verify_coverage_no_action(
            &fixture,
            &fixture.cycle,
            CoverageScenario::NoLabels,
            CoverageNoActionReason::NoPolicyObservations,
        )
        .await;
    harness
        .verify_coverage_no_action(
            &fixture,
            &fixture.second_cycle,
            CoverageScenario::LowCoverage,
            CoverageNoActionReason::InsufficientCoverage,
        )
        .await;
    let parity_after = parity
        .current_state()
        .await
        .expect("read parity latch after drift")
        .expect("parity latch remains initialized");
    assert_parity_unchanged(&parity_before, &parity_after);
    drop(harness);
    drop(parity);
    drop(artifact_root);
    drop(fixture);
    drop(db);
    drop(pool);
    drop(container);

    let (drift_pool, _drift_container) = setup_pg().await;
    let drift_db = drift_pool.connection().clone();
    let drift_fixture = Box::pin(prepare_fixture(&drift_db)).await;
    let drift_artifact_root = ArtifactRoot::create();
    let (drift_harness, drift_parity) = signal_harness(&drift_db, &drift_artifact_root);
    let drift_parity_before = open_parity(&drift_parity).await;
    drift_harness.verify_drift(&drift_fixture).await;
    let drift_parity_after = drift_parity
        .current_state()
        .await
        .expect("read parity latch after statistical drift")
        .expect("statistical-drift parity latch remains initialized");
    assert_parity_unchanged(&drift_parity_before, &drift_parity_after);
}
