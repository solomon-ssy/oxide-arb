//! F06/F09/F10-to-F11 contracts against real `PostgreSQL` and object storage.

use std::{
    slice,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::{
    governance::{PromotionPermitService, RuntimeControlsHandle},
    runtime_config::{CommittedPolicyApplicator, DecisionPolicyStore},
    service::{
        feedback_coordinator::{FeedbackStageDirective, FeedbackStageSuccess},
        feedback_decision::{FeedbackDecisionExecutionDeps, FeedbackDecisionExecutionService},
        feedback_decision_stage::{FeedbackDecisionStageAdapter, FeedbackDecisionStageDeps},
        feedback_shadow::{FeedbackShadowExecutionDeps, FeedbackShadowExecutionService},
        feedback_shadow_stage::{FeedbackShadowStageAdapter, FeedbackShadowStageDeps},
        model_route_promotion::{ModelRoutePromotionService, ModelRoutePromotionServiceDeps},
        model_serving_generation::{ModelServingGenerationStore, PublishedShadowRouteIdentity},
        promotion_preflight::{
            PromotionPreflightDraft, PromotionPreflightPlan, PromotionPreflightService,
            PromotionPreflightServiceDeps,
        },
    },
};
use quant_pivot_error::{
    QuantError,
    control::{ControlError, RuntimeApplyStage},
    feedback::{FeedbackError, PromotionCommitError},
    storage::StorageError,
};
use quant_pivot_models::{
    domain::{
        api::FeedbackDriftJobParams,
        governance::{RuntimeControlSnapshot, RuntimeControlUpdate},
        ports::{
            CommittedPolicyApplyPort, FeedbackDecisionExecutionPort, FeedbackShadowExecutionPort,
            PolicySnapshotPort, PreparedPolicySnapshot,
        },
        quant::{
            CommitModelRoutePromotion, FeedbackCohortWindow, FeedbackCycleInfo,
            FeedbackStageEventInput, FeedbackStageJobIdentity, IssuePromotionPermit,
            NewFeedbackStageEvent, NewResearchJob, NoopProgressSink, PromoteModelRoute,
            PromotionPermitActor, PromotionPermitInfo, PromotionPolicyProjection,
            PromotionPreflight, ResearchJobArtifactRef, ResearchJobFinalization, ResearchJobInfo,
            ResearchJobResultRef, RevokePromotionPermit,
        },
    },
    entities::{
        decision_policy_snapshot::Entity as SnapshotEntity,
        policy_activation::{
            Column as ActivationColumn, Entity as ActivationEntity, Model as ActivationModel,
        },
        policy_activation_audit::Entity as ActivationAuditEntity,
        policy_activation_event_outbox::Entity as ActivationOutboxEntity,
        policy_activation_guard::Entity as ActivationGuardEntity,
        policy_approval::Entity as ApprovalEntity,
        policy_revision::Entity as RevisionEntity,
        quant_feedback_cycle::{Entity as CycleEntity, Model as CycleModel},
        quant_feedback_promotion_permit::Entity as PermitEntity,
        quant_model_governance_audit::Entity as ModelAuditEntity,
        quant_model_version::{Entity as ModelVersionEntity, Model as ModelVersionModel},
        user::{Column as UserColumn, Entity as UserEntity},
    },
    enums::{
        common::MarketCategory,
        quant::{
            DatasetPurpose, FeedbackCycleStatus, FeedbackDecision, FeedbackStage,
            FeedbackStageEventKind, PublicationStatus, QuantRuntimeMode, ResearchJobKind,
            ResearchJobResultKind, ResearchJobStatus,
        },
        runtime_config::PolicyActivationKind,
    },
    runtime_config::{
        ActivePolicyBundle, BuyModelRoute, DecisionPolicySnapshot, PolicyApplyDegradedCause,
        PolicyApplyReadiness, PolicyBundleIdentity,
    },
    types::{
        ArtifactUri, ContentHash, FeatureValue, FeedbackCoverageArtifactId, FeedbackCycleId,
        FeedbackDriftArtifactId, PolicyIdempotencyKey, PromotionPermitId, ResearchJobId,
        ResearchJobParams, RoleCode, TrainingDatasetId, WorkerId, stable_name::FeatureName,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgFeatureParityRepository, PgFeedbackCycleRepository, PgModelRegistryRepository,
        PgModelRoutePromotionRepository, PgPolicyRepository, PgPromotionPermitRepository,
        PgResearchJobRepository, PgRuntimeControlRepository, PgShadowComparisonRepository,
    },
    traits::{
        FeatureParityRepository, FeedbackCycleClaim, FeedbackCycleLeaseGuard,
        FeedbackCycleRepository, ModelRegistryRepository, ModelRoutePromotionCommit,
        ModelRoutePromotionOutcome, ModelRoutePromotionRepository, PolicyRepository,
        PromotionPermitIssueOutcome, PromotionPermitRepository, PromotionPermitRevokeOutcome,
        ResearchJobEnqueueOutcome, ResearchJobRepository, RuntimeControlRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
    feedback::{
        ChampionBaselineRef, ConceptDriftDetail, DriftGateOutcome,
        FEEDBACK_DRIFT_ARTIFACT_FORMAT_VERSION, FeatureDriftDetail, FeedbackDriftArtifact,
        FeedbackDriftCodec, LabelDriftDetail, drift_gate, drift_observations,
    },
    feedback_decision::FeedbackDecisionCodec,
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        artifact_store::ReadTamperArtifactStoreFixture, model_serving_fixtures::ModelVersionFixture,
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter,
    QuerySelect, TransactionTrait,
};
use tokio_util::sync::CancellationToken;

use super::{
    feedback_boot_schema::content_hash,
    feedback_shadow_stage::{
        ActivatedServing, ArtifactRoot, ShadowModels, activate_crypto_generation,
        activate_generation, build_crypto_models, build_models, comparison_params,
        insert_observation, insert_stable_observations, record_comparison, record_crypto_cycle,
        record_cycle,
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
    expected_decision: FeedbackDecision,
    expected_reason: &str,
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
    assert_eq!(decision_artifact.outcome().decision(), expected_decision);
    assert_eq!(decision_artifact.outcome().reason(), expected_reason);
    assert_ne!(expected_decision, FeedbackDecision::Promoted);
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
    assert_eq!(terminal.decision(), Some(expected_decision));
    assert_eq!(terminal.reason_code(), expected_reason);
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
    let expected_decision = terminal.decision();
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
    assert_eq!(completed.decision, expected_decision);
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
    let completion = Box::pin(execute_decision(
        &store,
        &cycles,
        &jobs,
        &claim,
        FeedbackDecision::NoAction,
        "feedback_shadow_insufficient_observations",
    ))
    .await;
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

struct PromotionPreflightCase {
    db: DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
    models: ShadowModels,
    model_repository: Arc<PgModelRegistryRepository>,
    serving: ActivatedServing,
    route_before: PublishedShadowRouteIdentity,
    claim: FeedbackCycleClaim,
    cycles: Arc<PgFeedbackCycleRepository>,
    jobs: Arc<PgResearchJobRepository>,
    completion: DecisionCompletion,
}

impl PromotionPreflightCase {
    async fn prepare(db: DatabaseConnection, store: Arc<dyn ArtifactStore>) -> Self {
        let mut models = Box::pin(build_crypto_models(&db, &store)).await;
        let model_repository = Arc::new(PgModelRegistryRepository::new(db.clone()));
        models.candidate = model_repository
            .promote_model_to_shadow(&models.candidate.model_version_id)
            .await
            .expect("promote P03 candidate into durable Shadow");
        assert_eq!(
            models.candidate.publication_status,
            PublicationStatus::Shadow
        );
        ModelVersionFixture::persist_parity_proof(&db, &models.candidate)
            .await
            .expect("persist P03 candidate full-parity proof");
        let serving = activate_crypto_generation(&db, &store, &models).await;
        let route_before = serving
            .generations
            .current_route(BuyModelRoute::Crypto)
            .expect("P03 Crypto route before preflight")
            .published_shadow_identity()
            .expect("P03 exact published shadow before preflight");
        let (schema, claim) = record_crypto_cycle(&db, &models).await;
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
        insert_stable_observations(&db, &serving.generations, claim.cycle.created_at).await;
        Box::pin(record_shadow(
            &db,
            &store,
            &serving.generations,
            &cycles,
            &jobs,
            &claim,
        ))
        .await;
        let completion = Box::pin(execute_decision(
            &store,
            &cycles,
            &jobs,
            &claim,
            FeedbackDecision::CandidateReady,
            "feedback_candidate_ready_governance_required",
        ))
        .await;
        finalize_decision(cycles.as_ref(), &claim, &completion).await;
        Self {
            db,
            store,
            models,
            model_repository,
            serving,
            route_before,
            claim,
            cycles,
            jobs,
            completion,
        }
    }

    fn decision_stage(&self) -> Arc<FeedbackDecisionStageAdapter> {
        Arc::new(
            FeedbackDecisionStageAdapter::try_new(FeedbackDecisionStageDeps {
                cycles: Arc::clone(&self.cycles) as Arc<dyn FeedbackCycleRepository>,
                jobs: Arc::clone(&self.jobs) as Arc<dyn ResearchJobRepository>,
                artifacts: Arc::clone(&self.store),
                max_recovery_attempts: 3,
            })
            .expect("build promotion decision evidence reader"),
        )
    }

    async fn verify_decision_evidence(&self) -> Arc<FeedbackDecisionStageAdapter> {
        let decision_stage = self.decision_stage();
        let evidence = decision_stage
            .promotion_evidence(&self.claim.cycle.feedback_cycle_id)
            .await
            .expect("re-read exact CandidateReady evidence");
        assert_eq!(
            evidence.shadow_contract.category_scope(),
            Some(MarketCategory::Crypto)
        );
        assert_eq!(
            evidence.shadow_contract.candidate_model_version_id(),
            self.models.candidate.model_version_id
        );
        let tampered_store: Arc<dyn ArtifactStore> = Arc::new(ReadTamperArtifactStoreFixture::new(
            Arc::clone(&self.store),
            self.completion.artifact.uri.clone(),
            b"{}".to_vec(),
        ));
        FeedbackDecisionStageAdapter::try_new(FeedbackDecisionStageDeps {
            cycles: Arc::clone(&self.cycles) as Arc<dyn FeedbackCycleRepository>,
            jobs: Arc::clone(&self.jobs) as Arc<dyn ResearchJobRepository>,
            artifacts: tampered_store,
            max_recovery_attempts: 3,
        })
        .expect("build P03 tampered decision reader")
        .promotion_evidence(&self.claim.cycle.feedback_cycle_id)
        .await
        .expect_err("promotion evidence must reject tampered F11 bytes");
        decision_stage
    }
}

pub async fn promotion_preflight_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let artifact_root = ArtifactRoot::create();
    let store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(artifact_root.path.clone()));
    let case = Box::pin(PromotionPreflightCase::prepare(db, store)).await;
    let decision_stage = case.verify_decision_evidence().await;
    let PromotionPreflightCase {
        db,
        models,
        model_repository,
        serving,
        route_before,
        claim,
        cycles,
        ..
    } = case;

    let policy_repository = Arc::new(PgPolicyRepository::new(db.clone()));
    let policy_before = policy_repository
        .load_current_bundle()
        .await
        .expect("load P03 policy before preflight")
        .expect("P03 policy exists");
    let runtime_repository = Arc::new(PgRuntimeControlRepository::new(db.clone()));
    let runtime = runtime_repository
        .load()
        .await
        .expect("load P03 durable runtime control");
    let runtime_controls = RuntimeControlsHandle::new(RuntimeControlSnapshot::from(runtime));
    let permit_repository = Arc::new(PgPromotionPermitRepository::new(db.clone()));
    let preflight = PromotionPreflightService::new(PromotionPreflightServiceDeps {
        permits: Arc::clone(&permit_repository) as Arc<dyn PromotionPermitRepository>,
        cycles: Arc::clone(&cycles) as Arc<dyn FeedbackCycleRepository>,
        decisions: decision_stage,
        policies: Arc::clone(&policy_repository) as Arc<dyn PolicyRepository>,
        durable_runtime: Arc::clone(&runtime_repository) as Arc<dyn RuntimeControlRepository>,
        runtime_controls,
        policy_store: Arc::new(DecisionPolicyStore::new_active(policy_before.clone())),
        models: Arc::clone(&model_repository) as Arc<dyn ModelRegistryRepository>,
        feature_parity: Arc::new(PgFeatureParityRepository::new(db.clone()))
            as Arc<dyn FeatureParityRepository>,
        runtime_registry: Arc::clone(&serving.runtime_registry),
        serving_generations: Arc::clone(&serving.generations),
    });
    let database_now = cycles.database_time().await.expect("P03 database clock");
    let plan = preflight
        .prepare_issue(PromotionPreflightDraft {
            feedback_cycle_id: claim.cycle.feedback_cycle_id,
            allowed_runtime_modes: vec![QuantRuntimeMode::ReportOnly],
            expires_at: database_now + Duration::minutes(10),
        })
        .await
        .expect("derive P03 server-side preflight");
    assert_eq!(plan.preflight().scope().category(), MarketCategory::Crypto);
    assert_eq!(
        plan.projection().champion_model_version_id(),
        models.champion.model_version_id
    );
    assert_eq!(
        plan.projection().candidate_model_version_id(),
        models.candidate.model_version_id
    );
    plan.projection()
        .validate_candidate(plan.projection().prospective_snapshot())
        .expect("P03 projection changes only the exact route and consumed shadow");

    let actor_row = UserEntity::find()
        .filter(UserColumn::Username.eq("admin"))
        .one(&db)
        .await
        .expect("load P03 admin actor")
        .expect("P03 admin actor exists");
    let permit_service = PromotionPermitService::new(
        Arc::clone(&permit_repository) as Arc<dyn PromotionPermitRepository>
    );
    let issue = IssuePromotionPermit {
        actor: PromotionPermitActor {
            user_id: actor_row.id,
            acting_role: RoleCode::new("super_admin"),
        },
        idempotency_key: "p03-preflight-issue-0001"
            .parse::<PolicyIdempotencyKey>()
            .expect("valid P03 permit idempotency key"),
        scope: plan.preflight().scope().clone(),
        preflight_hash: plan.preflight().preflight_hash(),
        reason: "authorize exact CandidateReady Crypto preflight".to_owned(),
    };
    let permit = match permit_service.issue(issue).await.expect("issue P03 permit") {
        PromotionPermitIssueOutcome::Issued(permit) => permit,
        PromotionPermitIssueOutcome::ExactReplay(_) => {
            panic!("first P03 permit issue cannot be a replay")
        }
    };
    let verified = preflight
        .verify_permit(&permit.promotion_permit_id, claim.cycle.feedback_cycle_id)
        .await
        .expect("verify exact persisted P03 permit");
    assert_eq!(verified.permit(), &permit);
    assert_eq!(
        verified.preflight().preflight_hash(),
        plan.preflight().preflight_hash()
    );
    assert_eq!(
        verified.projection().non_route_policy_hash(),
        plan.projection().non_route_policy_hash()
    );
    assert_eq!(
        verified.projection().prospective_snapshot(),
        plan.projection().prospective_snapshot()
    );

    let policy_after = policy_repository
        .load_current_bundle()
        .await
        .expect("load P03 policy after preflight")
        .expect("P03 policy still exists");
    let route_after = serving
        .generations
        .current_route(BuyModelRoute::Crypto)
        .expect("P03 Crypto route after preflight")
        .published_shadow_identity()
        .expect("P03 exact published shadow after preflight");
    assert_eq!(policy_after, policy_before);
    assert_eq!(route_after, route_before);
    assert_eq!(
        model_repository
            .find_model_version(&models.champion.model_version_id)
            .await
            .expect("read P03 champion")
            .expect("P03 champion exists")
            .publication_status,
        PublicationStatus::Published
    );
    assert_eq!(
        model_repository
            .find_model_version(&models.candidate.model_version_id)
            .await
            .expect("read P03 candidate")
            .expect("P03 candidate exists")
            .publication_status,
        PublicationStatus::Shadow
    );
}

struct PromotionPolicyPort {
    fail_prepare: AtomicBool,
    applied: Arc<Mutex<PolicyBundleIdentity>>,
}

impl PromotionPolicyPort {
    fn new(initial: PolicyBundleIdentity) -> Self {
        Self {
            fail_prepare: AtomicBool::new(false),
            applied: Arc::new(Mutex::new(initial)),
        }
    }

    fn reject_prepare(&self, reject: bool) {
        self.fail_prepare.store(reject, Ordering::SeqCst);
    }

    fn applied(&self) -> PolicyBundleIdentity {
        *self.applied.lock().expect("lock test applied policy")
    }
}

#[async_trait::async_trait]
impl PolicySnapshotPort for PromotionPolicyPort {
    fn current(&self) -> Arc<DecisionPolicySnapshot> {
        Arc::new(DecisionPolicySnapshot::default())
    }

    async fn prepare(
        &self,
        config: DecisionPolicySnapshot,
    ) -> Result<PreparedPolicySnapshot, ControlError> {
        if self.fail_prepare.load(Ordering::SeqCst) {
            return Err(ControlError::Precondition(
                "injected P05 committed-generation prepare failure".to_owned(),
            ));
        }
        let applied = Arc::clone(&self.applied);
        Ok(PreparedPolicySnapshot::new_governed(
            Arc::new(config),
            move |bundle| {
                let bundle = bundle.ok_or_else(|| {
                    ControlError::Precondition(
                        "P05 test publication requires a committed bundle".to_owned(),
                    )
                })?;
                *applied.lock().map_err(|_| {
                    ControlError::Engine("P05 applied-policy mutex is poisoned".to_owned())
                })? = PolicyBundleIdentity::from(&bundle);
                Ok(())
            },
        ))
    }
}

struct PromotionHarness {
    policies: Arc<PgPolicyRepository>,
    promotions: Arc<PgModelRoutePromotionRepository>,
    runtime_repository: Arc<PgRuntimeControlRepository>,
    runtime_controls: RuntimeControlsHandle,
    preflight: Arc<PromotionPreflightService>,
    permit_service: Arc<PromotionPermitService>,
    service: Arc<ModelRoutePromotionService>,
    raw_apply: Arc<PromotionPolicyPort>,
    policy_apply: Arc<CommittedPolicyApplicator>,
    actor: PromotionPermitActor,
    permit: PromotionPermitInfo,
    initial_preflight: PromotionPreflight,
    policy_before: ActivePolicyBundle,
}

impl PromotionHarness {
    async fn wire(case: &PromotionPreflightCase) -> Self {
        let policies = Arc::new(PgPolicyRepository::new(case.db.clone()));
        let policy_before = policies
            .load_current_bundle()
            .await
            .expect("load P04 policy before promotion")
            .expect("P04 policy exists");
        let runtime_repository = Arc::new(PgRuntimeControlRepository::new(case.db.clone()));
        let runtime = runtime_repository
            .load()
            .await
            .expect("load P04 durable runtime control");
        let runtime_controls = RuntimeControlsHandle::new(RuntimeControlSnapshot::from(runtime));
        let permits = Arc::new(PgPromotionPermitRepository::new(case.db.clone()));
        let preflight = Arc::new(PromotionPreflightService::new(
            PromotionPreflightServiceDeps {
                permits: Arc::clone(&permits) as Arc<dyn PromotionPermitRepository>,
                cycles: Arc::clone(&case.cycles) as Arc<dyn FeedbackCycleRepository>,
                decisions: case.decision_stage(),
                policies: Arc::clone(&policies) as Arc<dyn PolicyRepository>,
                durable_runtime: Arc::clone(&runtime_repository)
                    as Arc<dyn RuntimeControlRepository>,
                runtime_controls: runtime_controls.clone(),
                policy_store: Arc::new(DecisionPolicyStore::new_active(policy_before.clone())),
                models: Arc::clone(&case.model_repository) as Arc<dyn ModelRegistryRepository>,
                feature_parity: Arc::new(PgFeatureParityRepository::new(case.db.clone()))
                    as Arc<dyn FeatureParityRepository>,
                runtime_registry: Arc::clone(&case.serving.runtime_registry),
                serving_generations: Arc::clone(&case.serving.generations),
            },
        ));
        let database_now = case
            .cycles
            .database_time()
            .await
            .expect("P04 database clock");
        let plan = preflight
            .prepare_issue(PromotionPreflightDraft {
                feedback_cycle_id: case.claim.cycle.feedback_cycle_id,
                allowed_runtime_modes: vec![QuantRuntimeMode::ReportOnly],
                expires_at: database_now + Duration::minutes(10),
            })
            .await
            .expect("derive P04 server-side preflight");
        let actor = UserEntity::find()
            .filter(UserColumn::Username.eq("admin"))
            .one(&case.db)
            .await
            .expect("load P04 admin actor")
            .expect("P04 admin actor exists");
        let permit_service = Arc::new(PromotionPermitService::new(
            Arc::clone(&permits) as Arc<dyn PromotionPermitRepository>
        ));
        let permit_actor = PromotionPermitActor {
            user_id: actor.id,
            acting_role: RoleCode::new("super_admin"),
        };
        let permit = match permit_service
            .issue(IssuePromotionPermit {
                actor: permit_actor.clone(),
                idempotency_key: "p04-atomic-promotion-0001"
                    .parse::<PolicyIdempotencyKey>()
                    .expect("valid P04 permit idempotency key"),
                scope: plan.preflight().scope().clone(),
                preflight_hash: plan.preflight().preflight_hash(),
                reason: "authorize exact atomic Crypto route promotion".to_owned(),
            })
            .await
            .expect("issue P04 permit")
        {
            PromotionPermitIssueOutcome::Issued(permit) => permit,
            PromotionPermitIssueOutcome::ExactReplay(_) => {
                panic!("first P04 permit issue cannot be a replay")
            }
        };
        let verified = preflight
            .verify_permit(
                &permit.promotion_permit_id,
                case.claim.cycle.feedback_cycle_id,
            )
            .await
            .expect("verify exact P04 permit");
        let promotions = Arc::new(PgModelRoutePromotionRepository::new(case.db.clone()));
        let raw_apply = Arc::new(PromotionPolicyPort::new(PolicyBundleIdentity::from(
            &policy_before,
        )));
        let policy_apply = Arc::new(CommittedPolicyApplicator::new(
            Arc::clone(&raw_apply) as Arc<dyn PolicySnapshotPort>,
            PolicyBundleIdentity::from(&policy_before),
        ));
        let service = Arc::new(ModelRoutePromotionService::new(
            ModelRoutePromotionServiceDeps {
                preflight: Arc::clone(&preflight),
                repository: Arc::clone(&promotions) as Arc<dyn ModelRoutePromotionRepository>,
                policies: Arc::clone(&policies) as Arc<dyn PolicyRepository>,
                policy_apply: Arc::clone(&policy_apply) as Arc<dyn CommittedPolicyApplyPort>,
            },
        ));
        Self {
            policies,
            promotions,
            runtime_repository,
            runtime_controls,
            preflight,
            permit_service,
            service,
            raw_apply,
            policy_apply,
            actor: permit_actor,
            permit,
            initial_preflight: verified.preflight().clone(),
            policy_before,
        }
    }

    async fn prepare_plan(
        &self,
        cycle_id: FeedbackCycleId,
        allowed_runtime_modes: Vec<QuantRuntimeMode>,
        expires_at: DateTime<Utc>,
    ) -> PromotionPreflightPlan {
        self.preflight
            .prepare_issue(PromotionPreflightDraft {
                feedback_cycle_id: cycle_id,
                allowed_runtime_modes,
                expires_at,
            })
            .await
            .expect("derive P06 promotion preflight")
    }

    async fn issue_plan(
        &self,
        plan: &PromotionPreflightPlan,
        idempotency_key: &str,
        preflight_hash: ContentHash,
        reason: &str,
    ) -> PromotionPermitInfo {
        match self
            .permit_service
            .issue(IssuePromotionPermit {
                actor: self.actor.clone(),
                idempotency_key: idempotency_key
                    .parse::<PolicyIdempotencyKey>()
                    .expect("valid P06 permit idempotency key"),
                scope: plan.preflight().scope().clone(),
                preflight_hash,
                reason: reason.to_owned(),
            })
            .await
            .expect("issue P06 promotion permit")
        {
            PromotionPermitIssueOutcome::Issued(permit) => permit,
            PromotionPermitIssueOutcome::ExactReplay(_) => {
                panic!("first P06 permit issue cannot be a replay")
            }
        }
    }

    fn initial_command(&self) -> CommitModelRoutePromotion {
        CommitModelRoutePromotion::try_new(
            self.permit.promotion_permit_id,
            self.initial_preflight.clone(),
        )
        .expect("build exact P06 initial promotion command")
    }

    async fn change_mode(&self, mode: QuantRuntimeMode, reason: &str) {
        let current = self
            .runtime_repository
            .load()
            .await
            .expect("load P06 runtime before mode change");
        let changed = self
            .runtime_repository
            .compare_and_set(RuntimeControlUpdate {
                expected_revision: current.revision,
                quant_runtime_mode: Some(mode),
                settlement_write_policy: None,
                kill_switch_state: None,
                kill_switch_requires_ack: None,
                actor: "p06-fault-matrix".to_owned(),
                reason: reason.to_owned(),
            })
            .await
            .expect("apply P06 runtime mode change");
        self.runtime_controls
            .publish_local(RuntimeControlSnapshot::from(changed));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PromotionRowCounts {
    revisions: u64,
    approvals: u64,
    snapshots: u64,
    model_audits: u64,
    activations: u64,
    activation_audits: u64,
    outbox: u64,
}

impl PromotionRowCounts {
    async fn load(db: &DatabaseConnection) -> Self {
        Self {
            revisions: RevisionEntity::find()
                .count(db)
                .await
                .expect("count P04 revisions"),
            approvals: ApprovalEntity::find()
                .count(db)
                .await
                .expect("count P04 approvals"),
            snapshots: SnapshotEntity::find()
                .count(db)
                .await
                .expect("count P04 snapshots"),
            model_audits: ModelAuditEntity::find()
                .count(db)
                .await
                .expect("count P04 model audits"),
            activations: ActivationEntity::find()
                .count(db)
                .await
                .expect("count P04 activations"),
            activation_audits: ActivationAuditEntity::find()
                .count(db)
                .await
                .expect("count P04 activation audits"),
            outbox: ActivationOutboxEntity::find()
                .count(db)
                .await
                .expect("count P04 outbox"),
        }
    }

    const fn incremented(self) -> Self {
        Self {
            revisions: self.revisions + 1,
            approvals: self.approvals + 1,
            snapshots: self.snapshots + 1,
            model_audits: self.model_audits + 1,
            activations: self.activations + 1,
            activation_audits: self.activation_audits + 1,
            outbox: self.outbox + 1,
        }
    }
}

async fn install_rollback_fault(db: &DatabaseConnection) {
    db.execute_unprepared(
        "CREATE FUNCTION qp_test_reject_model_promotion()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             IF NEW.activation_kind = 'model_promotion' THEN
                 RAISE EXCEPTION 'injected model-promotion rollback';
             END IF;
             RETURN NEW;
         END;
         $$",
    )
    .await
    .expect("create P04 rollback function");
    db.execute_unprepared(
        "CREATE TRIGGER trg_qp_test_reject_model_promotion
         BEFORE INSERT ON policy_activation
         FOR EACH ROW
         EXECUTE FUNCTION qp_test_reject_model_promotion()",
    )
    .await
    .expect("create P04 rollback trigger");
}

async fn remove_rollback_fault(db: &DatabaseConnection) {
    db.execute_unprepared("DROP TRIGGER trg_qp_test_reject_model_promotion ON policy_activation")
        .await
        .expect("drop P04 rollback trigger");
    db.execute_unprepared("DROP FUNCTION qp_test_reject_model_promotion()")
        .await
        .expect("drop P04 rollback function");
}

struct AtomicPromotionCase {
    db: DatabaseConnection,
    case: PromotionPreflightCase,
    harness: PromotionHarness,
    request: PromoteModelRoute,
    counts_before: PromotionRowCounts,
    champion_before: ModelVersionModel,
    candidate_before: ModelVersionModel,
    cycle_before: CycleModel,
}

impl AtomicPromotionCase {
    async fn prepare(db: DatabaseConnection, store: Arc<dyn ArtifactStore>) -> Self {
        let case = Box::pin(PromotionPreflightCase::prepare(db.clone(), store)).await;
        let harness = Box::pin(PromotionHarness::wire(&case)).await;
        let request = PromoteModelRoute {
            promotion_permit_id: harness.permit.promotion_permit_id,
            feedback_cycle_id: case.claim.cycle.feedback_cycle_id,
        };
        let counts_before = PromotionRowCounts::load(&db).await;
        let champion_before = ModelVersionEntity::find_by_id(case.models.champion.model_version_id)
            .one(&db)
            .await
            .expect("read P04 champion before")
            .expect("P04 champion exists before");
        let candidate_before =
            ModelVersionEntity::find_by_id(case.models.candidate.model_version_id)
                .one(&db)
                .await
                .expect("read P04 candidate before")
                .expect("P04 candidate exists before");
        let cycle_before = CycleEntity::find_by_id(case.claim.cycle.feedback_cycle_id)
            .one(&db)
            .await
            .expect("read P04 cycle before")
            .expect("P04 cycle exists before");
        Self {
            db,
            case,
            harness,
            request,
            counts_before,
            champion_before,
            candidate_before,
            cycle_before,
        }
    }

    async fn assert_unchanged(&self) {
        assert_eq!(PromotionRowCounts::load(&self.db).await, self.counts_before);
        assert_eq!(
            self.harness
                .policies
                .load_current_bundle()
                .await
                .expect("load P04 policy after rollback")
                .expect("P04 policy exists after rollback"),
            self.harness.policy_before
        );
        assert_eq!(
            ModelVersionEntity::find_by_id(self.case.models.champion.model_version_id)
                .one(&self.db)
                .await
                .expect("read P04 champion after rollback")
                .expect("P04 champion exists after rollback"),
            self.champion_before
        );
        assert_eq!(
            ModelVersionEntity::find_by_id(self.case.models.candidate.model_version_id)
                .one(&self.db)
                .await
                .expect("read P04 candidate after rollback")
                .expect("P04 candidate exists after rollback"),
            self.candidate_before
        );
        assert_eq!(
            CycleEntity::find_by_id(self.case.claim.cycle.feedback_cycle_id)
                .one(&self.db)
                .await
                .expect("read P04 cycle after rollback")
                .expect("P04 cycle exists after rollback"),
            self.cycle_before
        );
        assert_eq!(
            self.case
                .serving
                .generations
                .current_route(BuyModelRoute::Crypto)
                .expect("P06 runtime route remains available")
                .published_shadow_identity()
                .expect("P06 runtime generation remains complete"),
            self.case.route_before
        );
        assert_eq!(
            self.harness.policy_apply.readiness(),
            PolicyApplyReadiness::Ready {
                applied: PolicyBundleIdentity::from(&self.harness.policy_before),
            }
        );
    }

    async fn assert_rollback(&self) {
        install_rollback_fault(&self.db).await;
        Box::pin(self.harness.service.promote(self.request))
            .await
            .expect_err("injected activation failure must roll back the whole promotion");
        remove_rollback_fault(&self.db).await;
        Box::pin(self.assert_unchanged()).await;
    }

    async fn wait_for_expiry(&self, expires_at: DateTime<Utc>) {
        for _ in 0..200 {
            let database_now = self
                .case
                .cycles
                .database_time()
                .await
                .expect("read P06 database clock");
            if database_now >= expires_at {
                return;
            }
            tokio::time::sleep(StdDuration::from_millis(25)).await;
        }
        panic!("P06 database clock did not reach permit expiry");
    }

    async fn assert_expiry_race(&self) {
        let database_now = self
            .case
            .cycles
            .database_time()
            .await
            .expect("read P06 expiry-race start clock");
        let expires_at = database_now + Duration::seconds(3);
        let plan = self
            .harness
            .prepare_plan(
                self.case.claim.cycle.feedback_cycle_id,
                vec![QuantRuntimeMode::ReportOnly, QuantRuntimeMode::SemiAuto],
                expires_at,
            )
            .await;
        let permit = self
            .harness
            .issue_plan(
                &plan,
                "p06-expiry-lock-race-0001",
                plan.preflight().preflight_hash(),
                "prove expiry after permit row lock wait",
            )
            .await;
        let command = CommitModelRoutePromotion::try_new(
            permit.promotion_permit_id,
            plan.preflight().clone(),
        )
        .expect("build P06 expiry-race command");

        let blocker = self.db.begin().await.expect("begin P06 permit blocker");
        PermitEntity::find_by_id(permit.promotion_permit_id)
            .lock_exclusive()
            .one(&blocker)
            .await
            .expect("lock P06 expiry-race permit")
            .expect("P06 expiry-race permit exists");
        let promotions = Arc::clone(&self.harness.promotions);
        let commit = tokio::spawn(async move { Box::pin(promotions.commit(command)).await });
        tokio::time::sleep(StdDuration::from_millis(250)).await;
        self.wait_for_expiry(expires_at).await;
        blocker
            .commit()
            .await
            .expect("release P06 permit blocker after expiry");

        let error = commit
            .await
            .expect("join P06 expiry-race promotion")
            .expect_err("promotion waiting across permit expiry must fail closed");
        assert!(matches!(
            error,
            PromotionCommitError::Contract(FeedbackError::PromotionTransactionConflict { .. })
        ));
        assert!(
            self.harness
                .promotions
                .find_committed(
                    &permit.promotion_permit_id,
                    &self.case.claim.cycle.feedback_cycle_id,
                )
                .await
                .expect("check P06 expiry-race commit absence")
                .is_none()
        );
        Box::pin(self.assert_unchanged()).await;
    }

    async fn assert_uncommitted(&self, permit_id: PromotionPermitId) {
        assert!(
            self.harness
                .promotions
                .find_committed(&permit_id, &self.case.claim.cycle.feedback_cycle_id)
                .await
                .expect("check P06 promotion commit absence")
                .is_none()
        );
    }

    async fn assert_absent(&self) {
        let permit_id = PromotionPermitId::from_v7();
        let error = Box::pin(self.harness.service.promote(PromoteModelRoute {
            promotion_permit_id: permit_id,
            feedback_cycle_id: self.case.claim.cycle.feedback_cycle_id,
        }))
        .await
        .expect_err("absent P06 permit must fail closed");
        assert!(matches!(
            error,
            QuantError::Storage(StorageError::NotFound {
                entity: "quant_feedback_promotion_permit",
                ..
            })
        ));
        self.assert_uncommitted(permit_id).await;
        Box::pin(self.assert_unchanged()).await;
    }

    async fn assert_expired(&self) {
        let database_now = self
            .case
            .cycles
            .database_time()
            .await
            .expect("read P06 expiry fault clock");
        let expires_at = database_now + Duration::seconds(3);
        let plan = self
            .harness
            .prepare_plan(
                self.case.claim.cycle.feedback_cycle_id,
                vec![QuantRuntimeMode::ReportOnly],
                expires_at,
            )
            .await;
        let permit = self
            .harness
            .issue_plan(
                &plan,
                "p06-expired-permit-0001",
                plan.preflight().preflight_hash(),
                "prove expired authority cannot promote",
            )
            .await;
        self.wait_for_expiry(expires_at).await;
        let error = Box::pin(self.harness.service.promote(PromoteModelRoute {
            promotion_permit_id: permit.promotion_permit_id,
            feedback_cycle_id: self.case.claim.cycle.feedback_cycle_id,
        }))
        .await
        .expect_err("expired P06 permit must fail closed");
        assert!(matches!(
            error,
            QuantError::Feedback(FeedbackError::InvalidPromotionPreflight { .. })
        ));
        self.assert_uncommitted(permit.promotion_permit_id).await;
        Box::pin(self.assert_unchanged()).await;
    }

    async fn assert_revoked(&self) {
        let database_now = self
            .case
            .cycles
            .database_time()
            .await
            .expect("read P06 revoke fault clock");
        let plan = self
            .harness
            .prepare_plan(
                self.case.claim.cycle.feedback_cycle_id,
                vec![QuantRuntimeMode::ReportOnly],
                database_now + Duration::minutes(10),
            )
            .await;
        let permit = self
            .harness
            .issue_plan(
                &plan,
                "p06-revoked-permit-0001",
                plan.preflight().preflight_hash(),
                "authorize then revoke before promotion",
            )
            .await;
        let revoked = self
            .harness
            .permit_service
            .revoke(RevokePromotionPermit {
                promotion_permit_id: permit.promotion_permit_id,
                expected_revision: 0,
                actor: self.harness.actor.clone(),
                reason: "withdraw P06 promotion authority".to_owned(),
            })
            .await
            .expect("revoke P06 permit");
        assert!(matches!(
            revoked,
            PromotionPermitRevokeOutcome::Revoked(ref stored)
                if stored.revision == 1 && stored.revoked_at.is_some()
        ));
        let error = Box::pin(self.harness.service.promote(PromoteModelRoute {
            promotion_permit_id: permit.promotion_permit_id,
            feedback_cycle_id: self.case.claim.cycle.feedback_cycle_id,
        }))
        .await
        .expect_err("revoked P06 permit must fail closed");
        assert!(matches!(
            error,
            QuantError::Feedback(FeedbackError::InvalidPromotionPreflight { .. })
        ));
        self.assert_uncommitted(permit.promotion_permit_id).await;
        Box::pin(self.assert_unchanged()).await;
    }

    async fn assert_hash_drift(&self) {
        let database_now = self
            .case
            .cycles
            .database_time()
            .await
            .expect("read P06 hash fault clock");
        let plan = self
            .harness
            .prepare_plan(
                self.case.claim.cycle.feedback_cycle_id,
                vec![QuantRuntimeMode::ReportOnly],
                database_now + Duration::minutes(10),
            )
            .await;
        let drifted_hash = ContentHash::from_bytes([0xa5; 32]);
        assert_ne!(drifted_hash, plan.preflight().preflight_hash());
        let permit = self
            .harness
            .issue_plan(
                &plan,
                "p06-hash-drift-0001",
                drifted_hash,
                "prove preflight hash drift cannot promote",
            )
            .await;
        let error = Box::pin(self.harness.service.promote(PromoteModelRoute {
            promotion_permit_id: permit.promotion_permit_id,
            feedback_cycle_id: self.case.claim.cycle.feedback_cycle_id,
        }))
        .await
        .expect_err("drifted P06 preflight hash must fail closed");
        assert!(matches!(
            error,
            QuantError::Feedback(FeedbackError::InvalidPromotionPreflight { .. })
        ));
        self.assert_uncommitted(permit.promotion_permit_id).await;
        Box::pin(self.assert_unchanged()).await;
    }

    async fn assert_mode_drift(&self) {
        self.harness
            .change_mode(
                QuantRuntimeMode::SemiAuto,
                "move outside P06 permit mode scope",
            )
            .await;
        assert_eq!(
            self.harness.runtime_controls.quant_runtime_mode(),
            QuantRuntimeMode::SemiAuto
        );
        let error = Box::pin(self.harness.service.promote(self.request))
            .await
            .expect_err("runtime mode outside P06 permit scope must fail closed");
        assert!(matches!(
            error,
            QuantError::Feedback(FeedbackError::InvalidPromotionPreflight { .. })
        ));
        self.assert_uncommitted(self.request.promotion_permit_id)
            .await;
        Box::pin(self.assert_unchanged()).await;
    }

    async fn assert_revision_cas(&self) {
        self.harness
            .change_mode(
                QuantRuntimeMode::ReportOnly,
                "restore mode while advancing P06 runtime revision",
            )
            .await;
        let current = self
            .harness
            .runtime_repository
            .load()
            .await
            .expect("load P06 runtime after revision advance");
        assert_eq!(current.quant_runtime_mode, QuantRuntimeMode::ReportOnly);
        assert!(current.revision > self.harness.initial_preflight.runtime_control_revision());
        let error = Box::pin(
            self.harness
                .promotions
                .commit(self.harness.initial_command()),
        )
        .await
        .expect_err("stale P06 runtime revision must lose transaction CAS");
        assert!(matches!(
            error,
            PromotionCommitError::Contract(FeedbackError::PromotionTransactionConflict { .. })
        ));
        self.assert_uncommitted(self.request.promotion_permit_id)
            .await;
        Box::pin(self.assert_unchanged()).await;
    }

    async fn assert_promotion_first(&self) {
        let blocker = self.db.begin().await.expect("begin P06 permit blocker");
        PermitEntity::find_by_id(self.harness.permit.promotion_permit_id)
            .lock_exclusive()
            .one(&blocker)
            .await
            .expect("lock P06 promotion-first permit")
            .expect("P06 promotion-first permit exists");

        let promotions = Arc::clone(&self.harness.promotions);
        let command = self.harness.initial_command();
        let commit = tokio::spawn(async move { Box::pin(promotions.commit(command)).await });
        tokio::time::sleep(StdDuration::from_millis(250)).await;

        let permit_service = Arc::clone(&self.harness.permit_service);
        let revoke_command = RevokePromotionPermit {
            promotion_permit_id: self.harness.permit.promotion_permit_id,
            expected_revision: 0,
            actor: self.harness.actor.clone(),
            reason: "revoke immediately after P06 promotion commit".to_owned(),
        };
        let revoke =
            tokio::spawn(async move { Box::pin(permit_service.revoke(revoke_command)).await });
        tokio::time::sleep(StdDuration::from_millis(250)).await;
        blocker
            .commit()
            .await
            .expect("release P06 promotion-first permit blocker");

        let committed = commit
            .await
            .expect("join P06 promotion-first commit")
            .expect("queued promotion must commit first");
        assert_eq!(committed.outcome, ModelRoutePromotionOutcome::Committed);
        let revoked = revoke
            .await
            .expect("join P06 promotion-first revoke")
            .expect("revoke after promotion commit");
        assert!(matches!(
            revoked,
            PromotionPermitRevokeOutcome::Revoked(ref stored)
                if stored.revision == 1 && stored.revoked_at.is_some()
        ));

        let replay = Box::pin(self.harness.service.promote(self.request))
            .await
            .expect("revoked permit retains exact committed replay");
        assert_eq!(replay.outcome, ModelRoutePromotionOutcome::ExactReplay);
        assert_eq!(replay.transaction_hash, committed.transaction_hash);
        Box::pin(self.assert_commit(&replay)).await;
    }

    async fn assert_revocation_first(&self) {
        let blocker = self
            .db
            .begin()
            .await
            .expect("begin P06 activation-guard blocker");
        ActivationGuardEntity::find_by_id(1_i16)
            .lock_exclusive()
            .one(&blocker)
            .await
            .expect("lock P06 activation guard")
            .expect("P06 activation guard exists");

        let promotions = Arc::clone(&self.harness.promotions);
        let command = self.harness.initial_command();
        let commit = tokio::spawn(async move { Box::pin(promotions.commit(command)).await });
        tokio::time::sleep(StdDuration::from_millis(250)).await;
        let revoked = self
            .harness
            .permit_service
            .revoke(RevokePromotionPermit {
                promotion_permit_id: self.harness.permit.promotion_permit_id,
                expected_revision: 0,
                actor: self.harness.actor.clone(),
                reason: "revoke before P06 promotion obtains serialization lock".to_owned(),
            })
            .await
            .expect("revoke P06 permit before promotion");
        assert!(matches!(
            revoked,
            PromotionPermitRevokeOutcome::Revoked(ref stored)
                if stored.revision == 1 && stored.revoked_at.is_some()
        ));
        blocker
            .commit()
            .await
            .expect("release P06 activation-guard blocker");

        let error = commit
            .await
            .expect("join P06 revocation-first commit")
            .expect_err("revocation-first promotion must fail closed");
        assert!(matches!(
            error,
            PromotionCommitError::Contract(FeedbackError::PromotionTransactionConflict { .. })
        ));
        self.assert_uncommitted(self.request.promotion_permit_id)
            .await;
        Box::pin(self.assert_unchanged()).await;
    }

    async fn commit_race(&self) -> ModelRoutePromotionCommit {
        let left_service = Arc::clone(&self.harness.service);
        let right_service = Arc::clone(&self.harness.service);
        let request = self.request;
        let (left, right) = tokio::join!(
            async move { Box::pin(left_service.promote(request)).await },
            async move { Box::pin(right_service.promote(request)).await },
        );
        let left = left.expect("left concurrent P04 promotion");
        let right = right.expect("right concurrent P04 promotion");
        let committed_count = [left.outcome, right.outcome]
            .into_iter()
            .filter(|outcome| *outcome == ModelRoutePromotionOutcome::Committed)
            .count();
        let replay_count = [left.outcome, right.outcome]
            .into_iter()
            .filter(|outcome| *outcome == ModelRoutePromotionOutcome::ExactReplay)
            .count();
        assert_eq!(committed_count, 1);
        assert_eq!(replay_count, 1);
        assert_eq!(left.transaction_hash, right.transaction_hash);
        assert_eq!(
            left.activation.policy_activation_id,
            right.activation.policy_activation_id
        );
        assert_eq!(
            PromotionRowCounts::load(&self.db).await,
            self.counts_before.incremented()
        );
        if left.outcome == ModelRoutePromotionOutcome::Committed {
            left
        } else {
            right
        }
    }

    async fn assert_commit(&self, committed: &ModelRoutePromotionCommit) {
        let policy_after = self
            .harness
            .policies
            .load_current_bundle()
            .await
            .expect("load P04 committed policy")
            .expect("P04 committed policy exists");
        assert_eq!(policy_after, committed.bundle);
        assert_eq!(
            policy_after.generation,
            self.harness
                .policy_before
                .generation
                .checked_next()
                .expect("P04 policy generation increment")
        );
        assert_eq!(
            self.harness.policy_apply.readiness(),
            PolicyApplyReadiness::Ready {
                applied: PolicyBundleIdentity::from(&policy_after),
            }
        );
        PromotionPolicyProjection::try_new(
            &self.harness.policy_before,
            MarketCategory::Crypto,
            self.case.models.candidate.model_version_id,
        )
        .expect("rebuild P04 route projection")
        .validate_candidate(&policy_after.snapshot)
        .expect("P04 changed only the exact category route and consumed shadow");

        let champion_after =
            ModelVersionEntity::find_by_id(self.case.models.champion.model_version_id)
                .one(&self.db)
                .await
                .expect("read P04 champion after commit")
                .expect("P04 champion exists after commit");
        assert_eq!(champion_after, self.champion_before);
        let mut candidate_after =
            ModelVersionEntity::find_by_id(self.case.models.candidate.model_version_id)
                .one(&self.db)
                .await
                .expect("read P04 candidate after commit")
                .expect("P04 candidate exists after commit");
        assert_eq!(
            candidate_after.publication_status,
            PublicationStatus::Published
        );
        assert!(candidate_after.published_at.is_some());
        assert!(candidate_after.retired_at.is_none());
        candidate_after.publication_status = PublicationStatus::Shadow;
        candidate_after.published_at = None;
        assert_eq!(candidate_after, self.candidate_before);

        let cycle_after = CycleEntity::find_by_id(self.case.claim.cycle.feedback_cycle_id)
            .one(&self.db)
            .await
            .expect("read P04 cycle after commit")
            .expect("P04 cycle exists after commit");
        assert_eq!(cycle_after.status, FeedbackCycleStatus::Succeeded);
        assert_eq!(cycle_after.decision, Some(FeedbackDecision::Promoted));
        assert_eq!(cycle_after.generation, self.cycle_before.generation + 1);
        assert_eq!(
            self.case
                .serving
                .generations
                .current_route(BuyModelRoute::Crypto)
                .expect("P04 runtime route remains available")
                .published_shadow_identity()
                .expect("P04 old runtime generation remains intact"),
            self.case.route_before
        );
        Box::pin(self.assert_ledgers(committed)).await;
    }

    async fn assert_apply_recovery(&self) {
        self.harness.raw_apply.reject_prepare(true);
        let error = Box::pin(self.harness.service.promote(self.request))
            .await
            .expect_err("P05 injected runtime prepare failure");
        assert!(matches!(
            error,
            QuantError::Control(ControlError::CommittedGenerationApply {
                stage: RuntimeApplyStage::Prepare,
                ..
            })
        ));
        let committed = self
            .harness
            .promotions
            .find_committed(
                &self.request.promotion_permit_id,
                &self.request.feedback_cycle_id,
            )
            .await
            .expect("load P05 durable promotion")
            .expect("P05 promotion committed before apply failure");
        let desired = PolicyBundleIdentity::from(&committed.bundle);
        let applied = PolicyBundleIdentity::from(&self.harness.policy_before);
        assert_eq!(
            self.harness.policy_apply.readiness(),
            PolicyApplyReadiness::Degraded {
                desired,
                applied,
                cause: PolicyApplyDegradedCause::PrepareFailed,
            }
        );
        assert_eq!(self.harness.raw_apply.applied(), applied);
        assert_eq!(
            PromotionRowCounts::load(&self.db).await,
            self.counts_before.incremented()
        );
        assert_eq!(
            self.harness
                .policies
                .load_current_bundle()
                .await
                .expect("load P05 durable bundle")
                .expect("P05 durable bundle exists"),
            committed.bundle
        );
        assert_eq!(
            self.case
                .serving
                .generations
                .current_route(BuyModelRoute::Crypto)
                .expect("P05 old route remains available")
                .published_shadow_identity()
                .expect("P05 old generation remains complete"),
            self.case.route_before
        );

        self.harness.raw_apply.reject_prepare(false);
        let replay = Box::pin(self.harness.service.promote(self.request))
            .await
            .expect("P05 exact retry converges committed generation");
        assert_eq!(replay.outcome, ModelRoutePromotionOutcome::ExactReplay);
        assert_eq!(replay.transaction_hash, committed.transaction_hash);
        assert_eq!(
            self.harness.policy_apply.readiness(),
            PolicyApplyReadiness::Ready { applied: desired }
        );
        assert_eq!(self.harness.raw_apply.applied(), desired);
        assert_eq!(
            PromotionRowCounts::load(&self.db).await,
            self.counts_before.incremented()
        );
    }

    async fn assert_ledgers(&self, committed: &ModelRoutePromotionCommit) {
        assert_eq!(
            committed.activation.activation_kind,
            PolicyActivationKind::ModelPromotion
        );
        assert_eq!(
            committed.activation.promotion_permit_id,
            Some(self.harness.permit.promotion_permit_id)
        );
        assert_eq!(
            committed.activation.promotion_transaction_hash,
            Some(committed.transaction_hash)
        );
        assert_eq!(
            committed.activation.model_governance_audit_id,
            Some(committed.audit.audit_id)
        );
        let activation: ActivationModel = ActivationEntity::find()
            .filter(ActivationColumn::PromotionPermitId.eq(self.harness.permit.promotion_permit_id))
            .one(&self.db)
            .await
            .expect("read P04 activation")
            .expect("P04 activation exists");
        assert_eq!(
            activation.promotion_transaction_hash,
            Some(committed.transaction_hash)
        );
        assert!(
            ActivationAuditEntity::find_by_id(activation.audit_event_id)
                .one(&self.db)
                .await
                .expect("read P04 policy audit")
                .is_some()
        );
        assert!(
            ActivationOutboxEntity::find_by_id(activation.audit_event_id)
                .one(&self.db)
                .await
                .expect("read P04 durable outbox")
                .is_some()
        );
        assert!(
            ModelAuditEntity::find_by_id(committed.audit.audit_id)
                .one(&self.db)
                .await
                .expect("read P04 model audit")
                .is_some()
        );
    }

    async fn assert_replay(&self, committed: &ModelRoutePromotionCommit) {
        let replay_counts = PromotionRowCounts::load(&self.db).await;
        let replay = Box::pin(self.harness.service.promote(self.request))
            .await
            .expect("replay exact P04 promotion");
        assert_eq!(replay.outcome, ModelRoutePromotionOutcome::ExactReplay);
        assert_eq!(replay.transaction_hash, committed.transaction_hash);
        assert_eq!(
            replay.activation.policy_activation_id,
            committed.activation.policy_activation_id
        );
        assert_eq!(PromotionRowCounts::load(&self.db).await, replay_counts);
        let drift = Box::pin(self.harness.service.promote(PromoteModelRoute {
            promotion_permit_id: self.harness.permit.promotion_permit_id,
            feedback_cycle_id: FeedbackCycleId::from_v7(),
        }))
        .await
        .expect_err("P04 replay with a different cycle must conflict");
        assert!(matches!(
            drift,
            QuantError::Feedback(FeedbackError::PromotionTransactionConflict { .. })
        ));
        assert_eq!(PromotionRowCounts::load(&self.db).await, replay_counts);
    }
}

pub async fn model_route_promotion_contracts() {
    let (pool, _container) = setup_pg().await;
    let artifact_root = ArtifactRoot::create();
    let store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(artifact_root.path.clone()));
    let case = Box::pin(AtomicPromotionCase::prepare(
        pool.connection().clone(),
        store,
    ))
    .await;
    Box::pin(case.assert_rollback()).await;
    let committed = Box::pin(case.commit_race()).await;
    Box::pin(case.assert_commit(&committed)).await;
    Box::pin(case.assert_replay(&committed)).await;
}

pub async fn promotion_runtime_apply_contracts() {
    let (pool, _container) = setup_pg().await;
    let artifact_root = ArtifactRoot::create();
    let store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(artifact_root.path.clone()));
    let case = Box::pin(AtomicPromotionCase::prepare(
        pool.connection().clone(),
        store,
    ))
    .await;
    Box::pin(case.assert_apply_recovery()).await;
}

async fn rejection_fault_matrix() {
    let (pool, _container) = setup_pg().await;
    let artifact_root = ArtifactRoot::create();
    let store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(artifact_root.path.clone()));
    let case = Box::pin(AtomicPromotionCase::prepare(
        pool.connection().clone(),
        store,
    ))
    .await;
    Box::pin(case.assert_absent()).await;
    Box::pin(case.assert_expired()).await;
    Box::pin(case.assert_revoked()).await;
    Box::pin(case.assert_hash_drift()).await;
    Box::pin(case.assert_expiry_race()).await;
    Box::pin(case.assert_mode_drift()).await;
    Box::pin(case.assert_revision_cas()).await;
}

async fn promotion_first_race() {
    let (pool, _container) = setup_pg().await;
    let artifact_root = ArtifactRoot::create();
    let store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(artifact_root.path.clone()));
    let case = Box::pin(AtomicPromotionCase::prepare(
        pool.connection().clone(),
        store,
    ))
    .await;
    Box::pin(case.assert_promotion_first()).await;
}

async fn revocation_first_race() {
    let (pool, _container) = setup_pg().await;
    let artifact_root = ArtifactRoot::create();
    let store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(artifact_root.path.clone()));
    let case = Box::pin(AtomicPromotionCase::prepare(
        pool.connection().clone(),
        store,
    ))
    .await;
    Box::pin(case.assert_revocation_first()).await;
}

pub async fn promotion_fault_matrix_contracts() {
    Box::pin(rejection_fault_matrix()).await;
    Box::pin(promotion_first_race()).await;
    Box::pin(revocation_first_race()).await;
}
