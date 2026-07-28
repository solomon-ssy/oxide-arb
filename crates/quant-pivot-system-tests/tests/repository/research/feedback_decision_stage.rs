//! F06/F09/F10-to-F11 contracts against real `PostgreSQL` and object storage.

use std::{slice, sync::Arc};

use chrono::{Duration, Utc};
use quant_pivot_core::service::{
    feedback_coordinator::{FeedbackStageDirective, FeedbackStageSuccess},
    feedback_decision::{FeedbackDecisionExecutionDeps, FeedbackDecisionExecutionService},
    feedback_decision_stage::{FeedbackDecisionStageAdapter, FeedbackDecisionStageDeps},
    feedback_shadow::{FeedbackShadowExecutionDeps, FeedbackShadowExecutionService},
    feedback_shadow_stage::{FeedbackShadowStageAdapter, FeedbackShadowStageDeps},
    model_serving_generation::ModelServingGenerationStore,
};
use quant_pivot_models::{
    domain::{
        api::FeedbackDriftJobParams,
        ports::{FeedbackDecisionExecutionPort, FeedbackShadowExecutionPort},
        quant::{
            FeedbackCohortWindow, FeedbackCycleInfo, FeedbackStageEventInput,
            FeedbackStageJobIdentity, NewFeedbackStageEvent, NewResearchJob, NoopProgressSink,
            ResearchJobArtifactRef, ResearchJobFinalization, ResearchJobInfo, ResearchJobResultRef,
        },
    },
    enums::quant::{
        DatasetPurpose, FeedbackCycleStatus, FeedbackDecision, FeedbackStage,
        FeedbackStageEventKind, PublicationStatus, ResearchJobKind, ResearchJobResultKind,
        ResearchJobStatus,
    },
    runtime_config::BuyModelRoute,
    types::{
        ArtifactUri, FeatureValue, FeedbackCoverageArtifactId, FeedbackDriftArtifactId,
        ResearchJobId, ResearchJobParams, RoleCode, TrainingDatasetId, WorkerId,
        stable_name::FeatureName,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgFeedbackCycleRepository, PgModelRegistryRepository, PgResearchJobRepository,
        PgShadowComparisonRepository,
    },
    traits::{
        FeedbackCycleClaim, FeedbackCycleLeaseGuard, FeedbackCycleRepository,
        ModelRegistryRepository, ResearchJobEnqueueOutcome, ResearchJobRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
    feedback::{
        ChampionBaselineRef, ConceptDriftDetail, DriftGateOutcome,
        FEEDBACK_DRIFT_ARTIFACT_FORMAT_VERSION, FeatureDriftDetail, FeedbackDriftArtifact,
        FeedbackDriftCodec, LabelDriftDetail, drift_gate, drift_observations,
    },
    feedback_decision::{FeedbackDecisionCodec, FeedbackDecisionOutcome},
};
use quant_pivot_system_tests::{
    postgres::setup_pg, support::artifact_store::ReadTamperArtifactStoreFixture,
};
use sea_orm::DatabaseConnection;
use tokio_util::sync::CancellationToken;

use super::{
    feedback_boot_schema::content_hash,
    feedback_shadow_stage::{
        ArtifactRoot, activate_generation, build_models, comparison_params, insert_observation,
        record_comparison, record_cycle,
    },
};

const JOB_LEASE_SECS: i64 = 90;

struct AdvancingDriftSeed<'a> {
    cycle: &'a FeedbackCycleInfo,
}

impl AdvancingDriftSeed<'_> {
    fn seal(self) -> FeedbackDriftArtifact {
        let profile = self
            .cycle
            .profile_ref
            .resolve_builtin_research_profile()
            .expect("resolve F11 ResearchProfile");
        let policy = profile.spec.feedback_policy;
        let evaluation_window = FeedbackCohortWindow::try_new(
            self.cycle.profile_ref.clone(),
            self.cycle.label_cutoff - Duration::days(i64::from(policy.evaluation_window_days)),
            self.cycle.label_cutoff,
        )
        .expect("F11 evaluation window");
        let champion_baseline = ChampionBaselineRef {
            training_dataset_id: TrainingDatasetId::from_v7(),
            purpose: DatasetPurpose::Training,
            dataset_hash: content_hash('a'),
            manifest_hash: content_hash('b'),
            artifact_bytes_hash: content_hash('c'),
            parquet_uri: ArtifactUri::parse("s3://f11-decision/champion-baseline.parquet")
                .expect("F11 baseline URI"),
            feature_schema_hash: content_hash('d'),
            label_schema_hash: content_hash('e'),
            window_start: evaluation_window.window_start() - Duration::days(2),
            window_end: evaluation_window.window_start() - Duration::days(1),
            pit_cutoff: evaluation_window.window_start(),
            sample_count: 2,
        };
        let data_detail = FeatureDriftDetail::compute(
            FeatureName::from_static("test.f11_regime"),
            &[
                Some(FeatureValue::Bool(false)),
                Some(FeatureValue::Bool(false)),
            ],
            &[
                Some(FeatureValue::Bool(true)),
                Some(FeatureValue::Bool(true)),
            ],
        )
        .expect("F11 discrete drift");
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
            &policy,
            slice::from_ref(&data_detail),
            &concept_detail,
            &label_detail,
        )
        .expect("F11 drift observations");
        let gate_outcome = drift_gate(&observations);
        assert!(matches!(gate_outcome, DriftGateOutcome::Advance { .. }));
        let artifact = FeedbackDriftArtifact {
            format_version: FEEDBACK_DRIFT_ARTIFACT_FORMAT_VERSION,
            artifact_id: FeedbackDriftArtifactId::from_cycle_id(self.cycle.feedback_cycle_id),
            feedback_cycle_id: self.cycle.feedback_cycle_id,
            cycle_idempotency_hash: self.cycle.idempotency_hash,
            coverage_artifact_id: FeedbackCoverageArtifactId::from_cycle_id(
                self.cycle.feedback_cycle_id,
            ),
            coverage_artifact_uri: ArtifactUri::parse("s3://f11-decision/coverage.json")
                .expect("F11 coverage URI"),
            coverage_artifact_hash: content_hash('f'),
            profile_ref: self.cycle.profile_ref.clone(),
            feedback_policy: policy,
            feedback_policy_hash: self.cycle.feedback_policy_hash,
            champion_model_version_id: self.cycle.champion_model_version_id,
            champion_serving_contract_hash: self.cycle.champion_serving_contract_hash,
            champion_baseline,
            evaluation_window: evaluation_window.clone(),
            comparison_window_start: Some(evaluation_window.window_start()),
            data_details: vec![data_detail],
            concept_detail,
            label_detail,
            observations,
            gate_outcome,
            observed_at: evaluation_window.cutoff(),
        };
        artifact.validate().expect("validate F11 drift artifact");
        artifact
    }
}

async fn persist_drift(
    store: &Arc<dyn ArtifactStore>,
    artifact: &FeedbackDriftArtifact,
) -> ResearchJobArtifactRef {
    let bytes = FeedbackDriftCodec::encode(artifact).expect("encode F11 drift");
    let content_hash = FeedbackDriftCodec::bytes_hash(&bytes);
    let uri = store
        .put(
            ArtifactKey::new(ArtifactNamespace::FeedbackDrift, content_hash.hex(), "json")
                .expect("F11 drift key"),
            &bytes,
        )
        .await
        .expect("persist F11 drift");
    assert_eq!(store.get(&uri).await.expect("read F11 drift"), bytes);
    ResearchJobArtifactRef { uri, content_hash }
}

async fn record_drift(
    cycles: &PgFeedbackCycleRepository,
    jobs: &PgResearchJobRepository,
    store: &Arc<dyn ArtifactStore>,
    cycle: &FeedbackCycleInfo,
    lease: FeedbackCycleLeaseGuard,
) {
    let artifact = AdvancingDriftSeed { cycle }.seal();
    let artifact_ref = persist_drift(store, &artifact).await;
    let identity =
        FeedbackStageJobIdentity::try_root(cycle.feedback_cycle_id, FeedbackStage::Drift)
            .expect("F11 drift identity");
    let coverage_job_id = ResearchJobId::from_feedback_identity_hash(&content_hash('7'));
    let params = FeedbackDriftJobParams {
        feedback_cycle_id: cycle.feedback_cycle_id,
        cycle_idempotency_hash: cycle.idempotency_hash,
        artifact_id: artifact.artifact_id,
        coverage_job_id,
        coverage_artifact_id: artifact.coverage_artifact_id,
        coverage_artifact_uri: artifact.coverage_artifact_uri.clone(),
        coverage_artifact_hash: artifact.coverage_artifact_hash,
    };
    params.input_hash().expect("F11 drift input hash");
    let job = NewResearchJob {
        job_id: identity.job_id(),
        feedback_cycle_id: None,
        feedback_stage: None,
        kind: ResearchJobKind::FeedbackDrift,
        status: ResearchJobStatus::Queued,
        model_spec_id: Some(cycle.candidate_family.shared_evaluation().model_spec_id),
        decision_policy_snapshot_id: Some(
            cycle
                .candidate_family
                .shared_evaluation()
                .source_lineage
                .decision_policy_snapshot_id,
        ),
        params_json: ResearchJobParams::FeedbackDrift(params),
        requested_by: None,
        acting_role: RoleCode::new("system"),
        parent_job_id: None,
        recovery_attempt: 0,
        max_recovery_attempts: 3,
    }
    .try_bind_feedback(identity)
    .expect("bind F11 drift job");
    match jobs.enqueue(job).await.expect("enqueue F11 drift job") {
        ResearchJobEnqueueOutcome::Inserted(_) | ResearchJobEnqueueOutcome::AlreadyPresent(_) => {}
    }
    let worker = WorkerId::from_v7();
    jobs.lease_next(
        &[ResearchJobKind::FeedbackDrift],
        &worker,
        Utc::now() + Duration::seconds(JOB_LEASE_SECS),
    )
    .await
    .expect("lease F11 drift")
    .expect("queued F11 drift");
    let info = jobs
        .finalize(
            &identity.job_id(),
            &worker,
            ResearchJobFinalization::succeeded(
                Some(ResearchJobResultRef {
                    kind: ResearchJobResultKind::FeedbackDriftArtifact,
                    id: artifact.artifact_id.as_uuid(),
                }),
                Some(artifact_ref.clone()),
                None,
            ),
        )
        .await
        .expect("finalize F11 drift");
    cycles
        .append_stage(
            lease,
            NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                feedback_cycle_id: cycle.feedback_cycle_id,
                event_sequence: 2,
                stage: FeedbackStage::Drift,
                event_kind: FeedbackStageEventKind::Succeeded,
                research_job_id: Some(identity.job_id()),
                actor: None,
                reason_code: None,
                evidence_uri: Some(artifact_ref.uri),
                evidence_hash: Some(artifact_ref.content_hash),
                occurred_at: info.finished_at.expect("F11 drift terminal time"),
            })
            .expect("seal F11 drift event"),
        )
        .await
        .expect("append F11 drift event");
}

async fn record_shadow(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    generations: &Arc<ModelServingGenerationStore>,
    cycles: &Arc<PgFeedbackCycleRepository>,
    jobs: &Arc<PgResearchJobRepository>,
    claim: &FeedbackCycleClaim,
) {
    let shadow_stage = FeedbackShadowStageAdapter::try_new(FeedbackShadowStageDeps {
        cycles: Arc::clone(cycles) as Arc<dyn FeedbackCycleRepository>,
        jobs: Arc::clone(jobs) as Arc<dyn ResearchJobRepository>,
        artifacts: Arc::clone(store),
        serving_generations: Arc::clone(generations),
        max_recovery_attempts: 3,
    })
    .expect("build F11 shadow stage");
    let shadow_identity = FeedbackStageJobIdentity::try_root(
        claim.cycle.feedback_cycle_id,
        FeedbackStage::ShadowReplay,
    )
    .expect("F11 shadow identity");
    let shadow_job = shadow_stage
        .prepare_shadow(&claim.cycle, claim.lease, shadow_identity)
        .await
        .expect("prepare F11 shadow");
    let shadow_params = match &shadow_job.params_json {
        ResearchJobParams::FeedbackShadowReplay(params) => params.as_ref().clone(),
        _ => panic!("F11 shadow stage emitted another kind"),
    };
    match jobs.enqueue(shadow_job).await.expect("enqueue F11 shadow") {
        ResearchJobEnqueueOutcome::Inserted(_) | ResearchJobEnqueueOutcome::AlreadyPresent(_) => {}
    }
    let shadow_worker = WorkerId::from_v7();
    jobs.lease_next(
        &[ResearchJobKind::FeedbackShadowReplay],
        &shadow_worker,
        Utc::now() + Duration::seconds(JOB_LEASE_SECS),
    )
    .await
    .expect("lease F11 shadow")
    .expect("queued F11 shadow");
    let shadow_result = FeedbackShadowExecutionService::new(FeedbackShadowExecutionDeps {
        observations: Arc::new(PgShadowComparisonRepository::new(db.clone())),
        artifacts: Arc::clone(store),
    })
    .execute(
        shadow_params,
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    )
    .await
    .expect("execute F11 shadow");
    let shadow_info = jobs
        .finalize(
            &shadow_identity.job_id(),
            &shadow_worker,
            ResearchJobFinalization::succeeded(
                Some(ResearchJobResultRef {
                    kind: ResearchJobResultKind::FeedbackShadowReplayArtifact,
                    id: shadow_result.artifact_id.as_uuid(),
                }),
                Some(shadow_result.artifact.clone()),
                None,
            ),
        )
        .await
        .expect("finalize F11 shadow");
    shadow_stage
        .succeeded_shadow(&claim.cycle, &shadow_info)
        .await
        .expect("verify F11 shadow");
    cycles
        .append_stage(
            claim.lease,
            NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                feedback_cycle_id: claim.cycle.feedback_cycle_id,
                event_sequence: 4,
                stage: FeedbackStage::ShadowReplay,
                event_kind: FeedbackStageEventKind::Succeeded,
                research_job_id: Some(shadow_identity.job_id()),
                actor: None,
                reason_code: None,
                evidence_uri: Some(shadow_result.artifact.uri.clone()),
                evidence_hash: Some(shadow_result.artifact.content_hash),
                occurred_at: shadow_info.finished_at.expect("F11 shadow terminal time"),
            })
            .expect("seal F11 shadow event"),
        )
        .await
        .expect("append F11 shadow event");
}

struct DecisionCompletion {
    identity: FeedbackStageJobIdentity,
    artifact: ResearchJobArtifactRef,
    info: ResearchJobInfo,
    success: FeedbackStageSuccess,
}

async fn execute_decision(
    store: &Arc<dyn ArtifactStore>,
    cycles: &Arc<PgFeedbackCycleRepository>,
    jobs: &Arc<PgResearchJobRepository>,
    claim: &FeedbackCycleClaim,
) -> DecisionCompletion {
    let decision_stage = FeedbackDecisionStageAdapter::try_new(FeedbackDecisionStageDeps {
        cycles: Arc::clone(cycles) as Arc<dyn FeedbackCycleRepository>,
        jobs: Arc::clone(jobs) as Arc<dyn ResearchJobRepository>,
        artifacts: Arc::clone(store),
        max_recovery_attempts: 3,
    })
    .expect("build F11 decision stage");
    let decision_identity =
        FeedbackStageJobIdentity::try_root(claim.cycle.feedback_cycle_id, FeedbackStage::Decision)
            .expect("F11 Decision identity");
    let decision_job = decision_stage
        .prepare_decision(&claim.cycle, claim.lease, decision_identity)
        .await
        .expect("prepare exact F11 Decision");
    let decision_params = match &decision_job.params_json {
        ResearchJobParams::FeedbackDecision(params) => params.as_ref().clone(),
        _ => panic!("F11 Decision stage emitted another kind"),
    };
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancelled_result = FeedbackDecisionExecutionService::new(FeedbackDecisionExecutionDeps {
        artifacts: Arc::clone(store),
    })
    .execute(
        decision_params.clone(),
        Arc::new(NoopProgressSink),
        cancelled,
    )
    .await;
    assert!(cancelled_result.is_err());
    match jobs
        .enqueue(decision_job)
        .await
        .expect("enqueue F11 Decision")
    {
        ResearchJobEnqueueOutcome::Inserted(_) | ResearchJobEnqueueOutcome::AlreadyPresent(_) => {}
    }
    let decision_worker = WorkerId::from_v7();
    jobs.lease_next(
        &[ResearchJobKind::FeedbackDecision],
        &decision_worker,
        Utc::now() + Duration::seconds(JOB_LEASE_SECS),
    )
    .await
    .expect("lease F11 Decision")
    .expect("queued F11 Decision");
    let decision_result = FeedbackDecisionExecutionService::new(FeedbackDecisionExecutionDeps {
        artifacts: Arc::clone(store),
    })
    .execute(
        decision_params,
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    )
    .await
    .expect("execute exact F11 Decision");
    let decision_bytes = store
        .get(&decision_result.artifact.uri)
        .await
        .expect("read F11 Decision artifact");
    let decision_artifact =
        FeedbackDecisionCodec::decode(&decision_bytes).expect("decode F11 Decision");
    assert!(matches!(
        decision_artifact.outcome(),
        FeedbackDecisionOutcome::NoAction { .. }
    ));
    assert_ne!(
        decision_artifact.outcome().decision(),
        FeedbackDecision::Promoted
    );
    let decision_info = jobs
        .finalize(
            &decision_identity.job_id(),
            &decision_worker,
            ResearchJobFinalization::succeeded(
                Some(ResearchJobResultRef {
                    kind: ResearchJobResultKind::FeedbackDecisionArtifact,
                    id: decision_result.artifact_id.as_uuid(),
                }),
                Some(decision_result.artifact.clone()),
                None,
            ),
        )
        .await
        .expect("finalize F11 Decision");
    let first = decision_stage
        .succeeded_decision(&claim.cycle, &decision_info)
        .await
        .expect("verify F11 Decision");
    let restarted = decision_stage
        .succeeded_decision(&claim.cycle, &decision_info)
        .await
        .expect("verify F11 Decision after restart");
    assert_eq!(first, restarted);
    let FeedbackStageDirective::Complete(terminal) = first.directive() else {
        panic!("F11 Decision must complete the cycle");
    };
    assert_eq!(terminal.decision(), Some(FeedbackDecision::NoAction));
    assert_eq!(
        terminal.reason_code(),
        "feedback_shadow_insufficient_observations"
    );
    DecisionCompletion {
        identity: decision_identity,
        artifact: decision_result.artifact,
        info: decision_info,
        success: first,
    }
}

async fn assert_tamper(
    store: &Arc<dyn ArtifactStore>,
    cycles: &Arc<PgFeedbackCycleRepository>,
    jobs: &Arc<PgResearchJobRepository>,
    cycle: &FeedbackCycleInfo,
    completion: &DecisionCompletion,
) {
    let tampered_store: Arc<dyn ArtifactStore> = Arc::new(ReadTamperArtifactStoreFixture::new(
        Arc::clone(store),
        completion.artifact.uri.clone(),
        b"{}".to_vec(),
    ));
    FeedbackDecisionStageAdapter::try_new(FeedbackDecisionStageDeps {
        cycles: Arc::clone(cycles) as Arc<dyn FeedbackCycleRepository>,
        jobs: Arc::clone(jobs) as Arc<dyn ResearchJobRepository>,
        artifacts: tampered_store,
        max_recovery_attempts: 3,
    })
    .expect("build tampered F11 stage")
    .succeeded_decision(cycle, &completion.info)
    .await
    .expect_err("F11 restart must reject tampered Decision bytes");
}

async fn finalize_decision(
    cycles: &PgFeedbackCycleRepository,
    claim: &FeedbackCycleClaim,
    completion: &DecisionCompletion,
) {
    cycles
        .append_stage(
            claim.lease,
            NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                feedback_cycle_id: claim.cycle.feedback_cycle_id,
                event_sequence: 5,
                stage: FeedbackStage::Decision,
                event_kind: FeedbackStageEventKind::Succeeded,
                research_job_id: Some(completion.identity.job_id()),
                actor: None,
                reason_code: None,
                evidence_uri: Some(completion.artifact.uri.clone()),
                evidence_hash: Some(completion.artifact.content_hash),
                occurred_at: completion
                    .info
                    .finished_at
                    .expect("F11 Decision terminal time"),
            })
            .expect("seal F11 Decision event"),
        )
        .await
        .expect("append F11 Decision event");
    let FeedbackStageDirective::Complete(terminal) = completion.success.directive() else {
        panic!("F11 must complete");
    };
    cycles
        .finalize_cycle(claim.lease, terminal.clone())
        .await
        .expect("finalize F11 cycle");
    let completed = cycles
        .find_cycle(&claim.cycle.feedback_cycle_id)
        .await
        .expect("read F11 cycle")
        .expect("F11 cycle exists");
    assert_eq!(completed.status, FeedbackCycleStatus::Succeeded);
    assert_eq!(completed.decision, Some(FeedbackDecision::NoAction));
}

pub async fn terminal_restart_tamper() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let artifact_root = ArtifactRoot::create();
    let store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(artifact_root.path.clone()));
    let models = Box::pin(build_models(&db, &store)).await;
    let generations = activate_generation(&db, &store, &models).await;
    let route_before = generations
        .current_route(BuyModelRoute::Pooled)
        .expect("F11 route before")
        .published_shadow_identity()
        .expect("F11 published identity before");
    let (schema, claim) = record_cycle(&db, &models).await;
    let cycles = Arc::new(PgFeedbackCycleRepository::new(db.clone()));
    let jobs = Arc::new(PgResearchJobRepository::new(db.clone()));
    record_drift(
        cycles.as_ref(),
        jobs.as_ref(),
        &store,
        &claim.cycle,
        claim.lease,
    )
    .await;
    let comparison = comparison_params(&db, &schema, &claim, &models).await;
    record_comparison(&db, &store, &claim, comparison, 3).await;
    insert_observation(&db, cycles.as_ref(), &generations).await;
    Box::pin(record_shadow(
        &db,
        &store,
        &generations,
        &cycles,
        &jobs,
        &claim,
    ))
    .await;
    let completion = Box::pin(execute_decision(&store, &cycles, &jobs, &claim)).await;
    assert_tamper(&store, &cycles, &jobs, &claim.cycle, &completion).await;
    finalize_decision(cycles.as_ref(), &claim, &completion).await;
    assert_eq!(
        generations
            .current_route(BuyModelRoute::Pooled)
            .expect("F11 route after")
            .published_shadow_identity()
            .expect("F11 published identity after"),
        route_before
    );
    assert_eq!(
        PgModelRegistryRepository::new(db)
            .find_model_version(&models.candidate.model_version_id)
            .await
            .expect("read F11 candidate")
            .expect("F11 candidate exists")
            .publication_status,
        PublicationStatus::Candidate
    );
}
