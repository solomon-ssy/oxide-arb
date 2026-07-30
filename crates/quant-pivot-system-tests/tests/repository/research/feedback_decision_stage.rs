//! F06/F09/F10-to-F11 contracts against real `PostgreSQL` and object storage.

use std::{
    collections::{BTreeMap, BTreeSet},
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
            FeedbackStageEventInfo, FeedbackStageEventInput, FeedbackStageJobIdentity,
            IssuePromotionPermit, NewFeedbackStageEvent, NewResearchJob, NoopProgressSink,
            PromoteModelRoute, PromotionPermitActor, PromotionPermitInfo, PromotionPermitStatus,
            PromotionPolicyProjection, PromotionPreflight, ResearchJobArtifactRef,
            ResearchJobFinalization, ResearchJobInfo, ResearchJobResultRef, RevokePromotionPermit,
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
        quant_capital_allocation::{
            Column as CapitalAllocationColumn, Entity as CapitalAllocationEntity,
        },
        quant_feature_parity_run::Entity as ParityRunEntity,
        quant_feature_parity_state::Entity as ParityStateEntity,
        quant_feedback_cycle::{Entity as CycleEntity, Model as CycleModel},
        quant_feedback_event_outbox::Entity as FeedbackOutboxEntity,
        quant_feedback_promotion_permit::Entity as PermitEntity,
        quant_feedback_stage_event::Entity as StageEventEntity,
        quant_model_governance_audit::Entity as ModelAuditEntity,
        quant_model_version::{Entity as ModelVersionEntity, Model as ModelVersionModel},
        quant_research_job::Entity as ResearchJobEntity,
        quant_shadow_comparison::Entity as ShadowComparisonEntity,
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
    hashing::CanonicalDigest,
    runtime_config::{
        ActivePolicyBundle, BuyModelRoute, DecisionPolicySnapshot, PolicyApplyDegradedCause,
        PolicyApplyReadiness, PolicyBundleIdentity,
    },
    types::{
        ArtifactUri, ContentHash, FeatureValue, FeedbackCoverageArtifactId, FeedbackCycleId,
        FeedbackDecisionArtifactId, FeedbackDriftArtifactId, ModelVersionId, PolicyIdempotencyKey,
        PromotionPermitId, ResearchJobId, ResearchJobParams, RoleCode, TrainingDatasetId, WorkerId,
        stable_name::FeatureName,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgFeatureParityRepository, PgFeedbackCycleRepository, PgModelGovernanceAuditRepository,
        PgModelRegistryRepository, PgModelRoutePromotionRepository, PgPolicyRepository,
        PgPromotionPermitRepository, PgResearchJobRepository, PgRuntimeControlRepository,
        PgShadowComparisonRepository,
    },
    traits::{
        FeatureParityRepository, FeedbackCycleClaim, FeedbackCycleLeaseGuard,
        FeedbackCycleRepository, ModelGovernanceAuditRepository, ModelRegistryRepository,
        ModelRoutePromotionCommit, ModelRoutePromotionOutcome, ModelRoutePromotionRepository,
        PolicyRepository, PromotionPermitIssueOutcome, PromotionPermitRepository,
        PromotionPermitRevokeOutcome, ResearchJobEnqueueOutcome, ResearchJobRepository,
        RuntimeControlRepository,
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
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect, TransactionTrait,
};
use serde::Serialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::{
    feedback_boot_schema::content_hash,
    feedback_decision_evidence::{
        DecisionArtifactEvidence, DecisionPath, DecisionPathEvidence, DecisionPathEvidenceManifest,
        DeploymentAuthorityBoundary, ExactDecisionIdentifiers, InvariantDiff, InvariantSnapshot,
        PermitBindingEvidence, PermitEvidence, PermitLifecycleEvidence, ReplayEvidence,
        RestartReadBackEvidence, RowCountSnapshot, TimelineEventEvidence,
    },
    feedback_shadow_stage::{
        ActivatedServing, ArtifactRoot, ShadowModels, activate_crypto_generation,
        activate_generation, build_crypto_models, build_models, comparison_params,
        insert_observation, insert_stable_observations, record_comparison, record_crypto_cycle,
        record_cycle,
    },
};

const JOB_LEASE_SECS: i64 = 90;

const PROMOTION_REPOSITORY_SOURCE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../quant-pivot-repository/src/postgres/quant/model_route_promotion.rs"
));
const PROMOTION_SERVICE_SOURCE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../quant-pivot-core/src/service/model_route_promotion.rs"
));

fn wire_value<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("serialize W4-E04 evidence value")
}

fn wire_name<T: Serialize>(value: &T) -> String {
    wire_value(value)
        .as_str()
        .expect("W4-E04 enum serializes as a string")
        .to_owned()
}

const fn route_name(route: BuyModelRoute) -> &'static str {
    match route {
        BuyModelRoute::Pooled => "pooled",
        BuyModelRoute::Crypto => "crypto",
        BuyModelRoute::Weather => "weather",
    }
}

fn policy_identity_value(identity: PolicyBundleIdentity) -> Value {
    json!({
        "generation": identity.generation,
        "decision_policy_snapshot_id": identity.decision_policy_snapshot_id,
        "snapshot_hash": identity.snapshot_hash,
    })
}

fn readiness_value(readiness: PolicyApplyReadiness) -> Value {
    match readiness {
        PolicyApplyReadiness::Ready { applied } => json!({
            "status": "ready",
            "applied": policy_identity_value(applied),
        }),
        PolicyApplyReadiness::Degraded {
            desired,
            applied,
            cause,
        } => json!({
            "status": "degraded",
            "desired": policy_identity_value(desired),
            "applied": policy_identity_value(applied),
            "cause": cause.to_string(),
        }),
    }
}

fn serving_route_value(identity: &PublishedShadowRouteIdentity) -> Value {
    json!({
        "route": route_name(identity.route),
        "category_scope": identity.category_scope,
        "research_profile_artifact_id": identity.research_profile_artifact_id,
        "decision_policy_snapshot_id": identity.decision_policy_snapshot_id,
        "decision_policy_snapshot_hash": identity.decision_policy_snapshot_hash,
        "policy_bundle_generation": identity.policy_bundle_generation,
        "active_model_version_id": identity.active_model_version_id,
        "active_serving_contract_hash": identity.active_serving_contract_hash,
        "shadow_model_version_id": identity.shadow_model_version_id,
        "shadow_serving_contract_hash": identity.shadow_serving_contract_hash,
        "minimum_topn_overlap": identity.minimum_topn_overlap,
        "required_shadow_window_secs": identity.required_shadow_window_secs,
    })
}

fn assert_authority_boundary() {
    for forbidden in [
        "DeployConfig",
        "KeysConfig",
        "private_key",
        "funder",
        "order_client",
        "signer",
        "capital_allocation",
    ] {
        assert!(
            !PROMOTION_REPOSITORY_SOURCE.contains(forbidden),
            "promotion repository unexpectedly reaches deployment authority token {forbidden}"
        );
        assert!(
            !PROMOTION_SERVICE_SOURCE.contains(forbidden),
            "promotion service unexpectedly reaches deployment authority token {forbidden}"
        );
    }
}

#[derive(Clone)]
struct InvariantProbe {
    db: DatabaseConnection,
    cycle_id: FeedbackCycleId,
    champion_model_version_id: ModelVersionId,
    candidate_model_version_id: ModelVersionId,
    route: BuyModelRoute,
    serving_generations: Arc<ModelServingGenerationStore>,
    policy_apply: Option<Arc<CommittedPolicyApplicator>>,
}

impl InvariantProbe {
    async fn load(&self) -> InvariantSnapshot {
        let cycles = PgFeedbackCycleRepository::new(self.db.clone());
        let policies = PgPolicyRepository::new(self.db.clone());
        let runtime = PgRuntimeControlRepository::new(self.db.clone());
        let models = PgModelRegistryRepository::new(self.db.clone());
        let parity = PgFeatureParityRepository::new(self.db.clone());
        let cycle = cycles
            .find_cycle(&self.cycle_id)
            .await
            .expect("load W4-E04 invariant cycle")
            .expect("W4-E04 invariant cycle exists");
        let policy_bundle = policies
            .load_current_bundle()
            .await
            .expect("load W4-E04 invariant policy")
            .expect("W4-E04 invariant policy exists");
        let runtime_control = runtime.load().await.expect("load W4-E04 invariant runtime");
        let champion_model = models
            .find_model_version(&self.champion_model_version_id)
            .await
            .expect("load W4-E04 invariant champion")
            .expect("W4-E04 invariant champion exists");
        let candidate_model = models
            .find_model_version(&self.candidate_model_version_id)
            .await
            .expect("load W4-E04 invariant candidate")
            .expect("W4-E04 invariant candidate exists");
        let parity_latch = parity
            .current_state()
            .await
            .expect("load W4-E04 invariant parity latch");
        let capital_allocations = CapitalAllocationEntity::find()
            .order_by_asc(CapitalAllocationColumn::CapitalAllocationId)
            .all(&self.db)
            .await
            .expect("load W4-E04 invariant capital allocations")
            .into_iter()
            .map(|allocation| {
                json!({
                    "capital_allocation_id": allocation.capital_allocation_id,
                    "order_intent_id": allocation.order_intent_id,
                    "recommendation_id": allocation.recommendation_id,
                    "state": allocation.state,
                    "planned_usd": allocation.planned_usd,
                    "allocated_usd": allocation.allocated_usd,
                    "locked_usd": allocation.locked_usd,
                    "spent_usd": allocation.spent_usd,
                    "released_usd": allocation.released_usd,
                    "reason": allocation.reason,
                    "created_at": allocation.created_at,
                    "updated_at": allocation.updated_at,
                })
            })
            .collect::<Vec<_>>();
        let route = self
            .serving_generations
            .current_route(self.route)
            .expect("load W4-E04 in-memory serving route")
            .published_shadow_identity()
            .expect("W4-E04 in-memory route is complete");
        let policy_apply_readiness = self
            .policy_apply
            .as_ref()
            .map_or(Value::Null, |apply| readiness_value(apply.readiness()));
        InvariantSnapshot {
            cycle: wire_value(&cycle),
            policy_bundle: wire_value(&policy_bundle),
            runtime_control: wire_value(&runtime_control),
            model_routes: wire_value(&policy_bundle.snapshot.model_routing),
            capital_allocations: Value::Array(capital_allocations),
            champion_model: wire_value(&champion_model),
            candidate_model: wire_value(&candidate_model),
            parity_latch: wire_value(&parity_latch),
            in_memory_serving_route: serving_route_value(&route),
            policy_apply_readiness,
            deployment_authority: DeploymentAuthorityBoundary::default(),
        }
    }
}

async fn load_evidence_counts(db: &DatabaseConnection) -> RowCountSnapshot {
    let rows = BTreeMap::from([
        (
            "decision_policy_snapshot".to_owned(),
            SnapshotEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 policy snapshots"),
        ),
        (
            "policy_activation".to_owned(),
            ActivationEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 policy activations"),
        ),
        (
            "policy_activation_audit".to_owned(),
            ActivationAuditEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 policy activation audits"),
        ),
        (
            "policy_activation_event_outbox".to_owned(),
            ActivationOutboxEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 policy activation outbox"),
        ),
        (
            "policy_approval".to_owned(),
            ApprovalEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 policy approvals"),
        ),
        (
            "policy_revision".to_owned(),
            RevisionEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 policy revisions"),
        ),
        (
            "quant_capital_allocation".to_owned(),
            CapitalAllocationEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 capital allocations"),
        ),
        (
            "quant_feature_parity_run".to_owned(),
            ParityRunEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 parity runs"),
        ),
        (
            "quant_feature_parity_state".to_owned(),
            ParityStateEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 parity states"),
        ),
        (
            "quant_feedback_cycle".to_owned(),
            CycleEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 cycles"),
        ),
        (
            "quant_feedback_event_outbox".to_owned(),
            FeedbackOutboxEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 feedback outbox"),
        ),
        (
            "quant_feedback_promotion_permit".to_owned(),
            PermitEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 permits"),
        ),
        (
            "quant_feedback_stage_event".to_owned(),
            StageEventEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 stage events"),
        ),
        (
            "quant_model_governance_audit".to_owned(),
            ModelAuditEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 model audits"),
        ),
        (
            "quant_model_version".to_owned(),
            ModelVersionEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 model versions"),
        ),
        (
            "quant_research_job".to_owned(),
            ResearchJobEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 research jobs"),
        ),
        (
            "quant_shadow_comparison".to_owned(),
            ShadowComparisonEntity::find()
                .count(db)
                .await
                .expect("count W4-E04 shadow comparisons"),
        ),
    ]);
    RowCountSnapshot { rows }
}

fn timeline_evidence(events: Vec<FeedbackStageEventInfo>) -> Vec<TimelineEventEvidence> {
    events
        .into_iter()
        .map(|event| TimelineEventEvidence {
            sequence: event.event_sequence,
            stage: wire_name(&event.stage),
            event_kind: wire_name(&event.event_kind),
            research_job_id: event.research_job_id.map(|id| id.to_string()),
            actor: event.actor,
            reason_code: event.reason_code,
            evidence_uri: event.evidence_uri.map(|uri| uri.to_string()),
            evidence_hash: event.evidence_hash.map(|hash| hash.to_string()),
            event_hash: event.event_hash.to_string(),
        })
        .collect()
}

fn assert_protected_unchanged(
    invariant_diff: &InvariantDiff,
    path_name: &str,
    protected_paths: &[&str],
) {
    for protected_path in protected_paths {
        assert!(
            !invariant_diff.any_below(protected_path),
            "{path_name} path changed protected invariant {protected_path}"
        );
    }
}

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
    artifact_id: FeedbackDecisionArtifactId,
    artifact: ResearchJobArtifactRef,
    semantic_hash: ContentHash,
    job_input_hash: ContentHash,
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
    let job_input_hash = decision_params
        .input_hash()
        .expect("hash exact F11 Decision input");
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
        artifact_id: decision_artifact.artifact_id(),
        artifact: decision_result.artifact,
        semantic_hash: decision_artifact.artifact_hash(),
        job_input_hash,
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

fn decision_artifact_evidence(
    completion: &DecisionCompletion,
    models: &ShadowModels,
    decision_job_input_hash: Option<ContentHash>,
) -> DecisionArtifactEvidence {
    DecisionArtifactEvidence {
        artifact_id: completion.artifact_id.to_string(),
        uri: completion.artifact.uri.to_string(),
        bytes_hash: completion.artifact.content_hash.to_string(),
        semantic_hash: completion.semantic_hash.to_string(),
        decision_job_input_hash: decision_job_input_hash.map(|hash| hash.to_string()),
        champion_serving_contract_hash: models.champion.serving_contract_hash.to_string(),
        candidate_serving_contract_hash: models.candidate.serving_contract_hash.to_string(),
    }
}

struct ExactIdInput<'a> {
    cycle: &'a FeedbackCycleInfo,
    models: &'a ShadowModels,
    policy_before: &'a ActivePolicyBundle,
    policy_after: &'a ActivePolicyBundle,
    permit: Option<&'a PromotionPermitInfo>,
    policy_activation_id: Option<String>,
    promotion_transaction_hash: Option<ContentHash>,
    route: BuyModelRoute,
}

fn exact_identifiers(input: ExactIdInput<'_>) -> ExactDecisionIdentifiers {
    let before_model = &input.policy_before.snapshot.model_routing.model;
    let after_model = &input.policy_after.snapshot.model_routing.model;
    ExactDecisionIdentifiers {
        feedback_cycle_id: input.cycle.feedback_cycle_id.to_string(),
        research_profile_artifact_id: input.cycle.research_profile_artifact_id.to_string(),
        profile_hash: input.cycle.profile_hash.to_string(),
        champion_model_version_id: input.models.champion.model_version_id.to_string(),
        champion_model_spec_id: input.models.champion.model_spec_id.to_string(),
        champion_training_dataset_id: input
            .models
            .champion
            .training_dataset_id
            .map(|id| id.to_string()),
        candidate_model_version_id: input.models.candidate.model_version_id.to_string(),
        candidate_model_spec_id: input.models.candidate.model_spec_id.to_string(),
        candidate_training_dataset_id: input
            .models
            .candidate
            .training_dataset_id
            .map(|id| id.to_string()),
        policy_generation_before: input.policy_before.generation.to_string(),
        policy_generation_after: input.policy_after.generation.to_string(),
        policy_snapshot_id_before: input.policy_before.decision_policy_snapshot_id.to_string(),
        policy_snapshot_id_after: input.policy_after.decision_policy_snapshot_id.to_string(),
        model_routing_revision_before: input
            .policy_before
            .revision_vector
            .model_routing
            .map(|id| id.to_string()),
        model_routing_revision_after: input
            .policy_after
            .revision_vector
            .model_routing
            .map(|id| id.to_string()),
        promotion_permit_id: input
            .permit
            .map(|permit| permit.promotion_permit_id.to_string()),
        policy_activation_id: input.policy_activation_id,
        promotion_transaction_hash: input
            .promotion_transaction_hash
            .map(|hash| hash.to_string()),
        target_category: input.route.category().map(|category| wire_name(&category)),
        route_pointer_before: before_model
            .active_pointer(input.route)
            .ok()
            .map(|pointer| pointer.id().to_string()),
        route_pointer_after: after_model
            .active_pointer(input.route)
            .ok()
            .map(|pointer| pointer.id().to_string()),
        global_shadow_before: before_model
            .shadow_model_version_id
            .as_ref()
            .map(|pointer| pointer.id().to_string()),
        global_shadow_after: after_model
            .shadow_model_version_id
            .as_ref()
            .map(|pointer| pointer.id().to_string()),
    }
}

struct RestartProbe<'a> {
    invariant: InvariantProbe,
    artifacts: Arc<dyn ArtifactStore>,
    completion: &'a DecisionCompletion,
    expected_after: &'a InvariantSnapshot,
    expected_timeline: &'a [TimelineEventEvidence],
    expected_permit: Option<&'a PromotionPermitInfo>,
    expected_transaction_hash: Option<ContentHash>,
}

impl RestartProbe<'_> {
    async fn verify(self) -> RestartReadBackEvidence {
        let cycles = Arc::new(PgFeedbackCycleRepository::new(self.invariant.db.clone()));
        let jobs = Arc::new(PgResearchJobRepository::new(self.invariant.db.clone()));
        let cycle = cycles
            .find_cycle(&self.invariant.cycle_id)
            .await
            .expect("restart read W4-E04 cycle")
            .expect("restart W4-E04 cycle exists");
        let job = jobs
            .find_by_id(&self.completion.identity.job_id())
            .await
            .expect("restart read W4-E04 Decision job")
            .expect("restart W4-E04 Decision job exists");
        let stage = FeedbackDecisionStageAdapter::try_new(FeedbackDecisionStageDeps {
            cycles: Arc::clone(&cycles) as Arc<dyn FeedbackCycleRepository>,
            jobs: Arc::clone(&jobs) as Arc<dyn ResearchJobRepository>,
            artifacts: Arc::clone(&self.artifacts),
            max_recovery_attempts: 3,
        })
        .expect("build fresh W4-E04 Decision stage adapter");
        let success = stage
            .succeeded_decision(&cycle, &job)
            .await
            .expect("restart verify W4-E04 Decision artifact");
        assert_eq!(success, self.completion.success);

        let timeline = timeline_evidence(
            cycles
                .list_stage_events(&self.invariant.cycle_id)
                .await
                .expect("restart read W4-E04 WORM timeline"),
        );
        let after = self.invariant.load().await;
        let permit = if let Some(expected) = self.expected_permit {
            let loaded = PgPromotionPermitRepository::new(self.invariant.db.clone())
                .load(&expected.promotion_permit_id)
                .await
                .expect("restart read W4-E04 permit");
            assert_eq!(&loaded, expected);
            Some(loaded)
        } else {
            None
        };
        let committed = if let Some(expected_permit) = self.expected_permit {
            PgModelRoutePromotionRepository::new(self.invariant.db.clone())
                .find_committed(
                    &expected_permit.promotion_permit_id,
                    &self.invariant.cycle_id,
                )
                .await
                .expect("restart read W4-E04 promotion")
        } else {
            None
        };
        let committed_hash = committed.as_ref().map(|commit| commit.transaction_hash);
        assert_eq!(committed_hash, self.expected_transaction_hash);

        let after_value = wire_value(&after);
        let expected_after_value = wire_value(self.expected_after);
        let exact_match = after_value == expected_after_value
            && timeline == self.expected_timeline
            && wire_value(&job) == wire_value(&self.completion.info);
        assert!(
            exact_match,
            "fresh W4-E04 owners read a different durable graph"
        );
        let read_back = json!({
            "invariant": after_value,
            "timeline": timeline,
            "decision_job": job,
            "permit": permit,
            "promotion_transaction_hash": committed_hash,
        });
        let canonical_read_back_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot:w4-e04:restart-read-back",
            1,
            &read_back,
        )
        .expect("hash W4-E04 restart read-back");
        RestartReadBackEvidence {
            fresh_repository_owners: true,
            fresh_stage_adapter: true,
            exact_match,
            canonical_read_back_hash: canonical_read_back_hash.to_string(),
        }
    }
}

#[derive(Clone, Copy)]
enum TerminalDecisionCase {
    InsufficientShadow,
    RejectedComparison,
}

struct TerminalEvidenceInput<'a> {
    claim: &'a FeedbackCycleClaim,
    models: &'a ShadowModels,
    policy_before: &'a ActivePolicyBundle,
    policy_after: &'a ActivePolicyBundle,
    completion: &'a DecisionCompletion,
    timeline: Vec<TimelineEventEvidence>,
    before: InvariantSnapshot,
    after: InvariantSnapshot,
    counts_before: RowCountSnapshot,
    counts_after_first: RowCountSnapshot,
    counts_after_exact_replay: RowCountSnapshot,
    restart_read_back: RestartReadBackEvidence,
}

impl TerminalDecisionCase {
    const fn contract(self) -> (FeedbackDecision, &'static str, DecisionPath) {
        match self {
            Self::InsufficientShadow => (
                FeedbackDecision::NoAction,
                "feedback_shadow_insufficient_observations",
                DecisionPath::NoAction,
            ),
            Self::RejectedComparison => (
                FeedbackDecision::ChallengerRejected,
                "feedback_all_candidates_rejected",
                DecisionPath::ChallengerRejected,
            ),
        }
    }

    fn evidence(self, input: TerminalEvidenceInput<'_>) -> DecisionPathEvidence {
        let (expected_decision, expected_reason, path) = self.contract();
        let invariant_diff = InvariantDiff::between(&input.before, &input.after);
        assert_protected_unchanged(
            &invariant_diff,
            "terminal",
            &[
                "$.policy_bundle",
                "$.runtime_control",
                "$.model_routes",
                "$.capital_allocations",
                "$.champion_model",
                "$.candidate_model",
                "$.parity_latch",
                "$.in_memory_serving_route",
                "$.policy_apply_readiness",
                "$.deployment_authority",
            ],
        );
        assert!(invariant_diff.any_below("$.cycle"));
        let evidence = DecisionPathEvidence {
            path,
            decision: wire_name(&expected_decision),
            decision_reason: expected_reason.to_owned(),
            exact_ids: exact_identifiers(ExactIdInput {
                cycle: &input.claim.cycle,
                models: input.models,
                policy_before: input.policy_before,
                policy_after: input.policy_after,
                permit: None,
                policy_activation_id: None,
                promotion_transaction_hash: None,
                route: BuyModelRoute::Pooled,
            }),
            decision_artifact: decision_artifact_evidence(
                input.completion,
                input.models,
                Some(input.completion.job_input_hash),
            ),
            permit: None,
            worm_timeline: input.timeline,
            before: input.before,
            after: input.after,
            invariant_diff,
            replay: ReplayEvidence::new(
                format!("committed:{}", wire_name(&expected_decision)),
                "exact_artifact_read_replay",
                input.counts_before,
                input.counts_after_first,
                input.counts_after_exact_replay,
            ),
            restart_read_back: input.restart_read_back,
            fault_injection_rollback_verified: false,
        };
        evidence.validate();
        evidence
    }

    async fn run(self) -> DecisionPathEvidence {
        assert_authority_boundary();
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
        let candidate_return_bps = match self {
            Self::InsufficientShadow => dec!(100),
            Self::RejectedComparison => Decimal::ZERO,
        };
        record_comparison(&db, &store, &claim, comparison, candidate_return_bps, 3).await;
        if matches!(self, Self::InsufficientShadow) {
            insert_observation(&db, cycles.as_ref(), &generations).await;
        }
        Box::pin(record_shadow(
            &db,
            &store,
            &generations,
            &cycles,
            &jobs,
            &claim,
        ))
        .await;
        let policy_repository = PgPolicyRepository::new(db.clone());
        let policy_before = policy_repository
            .load_current_bundle()
            .await
            .expect("load W4-E04 terminal policy before")
            .expect("W4-E04 terminal policy exists");
        let probe = InvariantProbe {
            db: db.clone(),
            cycle_id: claim.cycle.feedback_cycle_id,
            champion_model_version_id: models.champion.model_version_id,
            candidate_model_version_id: models.candidate.model_version_id,
            route: BuyModelRoute::Pooled,
            serving_generations: Arc::clone(&generations),
            policy_apply: None,
        };
        let before = probe.load().await;
        let counts_before = load_evidence_counts(&db).await;
        let (expected_decision, expected_reason, _) = self.contract();
        let completion = Box::pin(execute_decision(
            &store,
            &cycles,
            &jobs,
            &claim,
            expected_decision,
            expected_reason,
        ))
        .await;
        assert_tamper(&store, &cycles, &jobs, &claim.cycle, &completion).await;
        finalize_decision(cycles.as_ref(), &claim, &completion).await;
        let policy_after = policy_repository
            .load_current_bundle()
            .await
            .expect("load W4-E04 terminal policy after")
            .expect("W4-E04 terminal policy remains");
        let after = probe.load().await;
        let timeline = timeline_evidence(
            cycles
                .list_stage_events(&claim.cycle.feedback_cycle_id)
                .await
                .expect("load W4-E04 terminal WORM timeline"),
        );
        let counts_after_first = load_evidence_counts(&db).await;
        assert_eq!(
            generations
                .current_route(BuyModelRoute::Pooled)
                .expect("F11 route after")
                .published_shadow_identity()
                .expect("F11 published identity after"),
            route_before
        );
        assert_eq!(
            PgModelRegistryRepository::new(db.clone())
                .find_model_version(&models.candidate.model_version_id)
                .await
                .expect("read F11 candidate")
                .expect("F11 candidate exists")
                .publication_status,
            PublicationStatus::Candidate
        );
        let restart_read_back = Box::pin(
            RestartProbe {
                invariant: probe,
                artifacts: Arc::clone(&store),
                completion: &completion,
                expected_after: &after,
                expected_timeline: &timeline,
                expected_permit: None,
                expected_transaction_hash: None,
            }
            .verify(),
        )
        .await;
        let counts_after_exact_replay = load_evidence_counts(&db).await;
        self.evidence(TerminalEvidenceInput {
            claim: &claim,
            models: &models,
            policy_before: &policy_before,
            policy_after: &policy_after,
            completion: &completion,
            timeline,
            before,
            after,
            counts_before,
            counts_after_first,
            counts_after_exact_replay,
            restart_read_back,
        })
    }
}

pub async fn terminal_decision_contracts() {
    let no_action = Box::pin(TerminalDecisionCase::InsufficientShadow.run()).await;
    let rejected = Box::pin(TerminalDecisionCase::RejectedComparison.run()).await;
    no_action.validate();
    rejected.validate();
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
    decision_before: InvariantSnapshot,
    decision_counts_before: RowCountSnapshot,
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
        record_comparison(&db, &store, &claim, comparison, dec!(100), 3).await;
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
        let decision_probe = InvariantProbe {
            db: db.clone(),
            cycle_id: claim.cycle.feedback_cycle_id,
            champion_model_version_id: models.champion.model_version_id,
            candidate_model_version_id: models.candidate.model_version_id,
            route: BuyModelRoute::Crypto,
            serving_generations: Arc::clone(&serving.generations),
            policy_apply: None,
        };
        let decision_before = decision_probe.load().await;
        let decision_counts_before = load_evidence_counts(&db).await;
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
            decision_before,
            decision_counts_before,
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

    fn invariant_probe(
        &self,
        policy_apply: Option<Arc<CommittedPolicyApplicator>>,
    ) -> InvariantProbe {
        InvariantProbe {
            db: self.db.clone(),
            cycle_id: self.claim.cycle.feedback_cycle_id,
            champion_model_version_id: self.models.champion.model_version_id,
            candidate_model_version_id: self.models.candidate.model_version_id,
            route: BuyModelRoute::Crypto,
            serving_generations: Arc::clone(&self.serving.generations),
            policy_apply,
        }
    }
}

struct PermitEvidenceProbe<'a> {
    cycles: &'a PgFeedbackCycleRepository,
    runtime_repository: &'a PgRuntimeControlRepository,
    runtime_controls: &'a RuntimeControlsHandle,
    permit: &'a PromotionPermitInfo,
    preflight: &'a PromotionPreflight,
}

impl PermitEvidenceProbe<'_> {
    async fn load(&self) -> PermitEvidence {
        let runtime = self
            .runtime_repository
            .load()
            .await
            .expect("read promotion evidence runtime");
        assert_eq!(
            self.runtime_controls.quant_runtime_mode(),
            runtime.quant_runtime_mode
        );
        let action_time = self
            .cycles
            .database_time()
            .await
            .expect("read promotion evidence permit clock");
        let permit_status = self
            .permit
            .status_at(action_time)
            .expect("derive promotion evidence permit status");
        assert_eq!(permit_status, PromotionPermitStatus::Active);
        let permit_scope = self
            .permit
            .scope()
            .expect("rebuild promotion evidence permit scope");
        PermitEvidence {
            persisted_permit: wire_value(self.permit),
            status_at_action: wire_name(&permit_status),
            lifecycle: PermitLifecycleEvidence {
                active: permit_status == PromotionPermitStatus::Active,
                not_expired: action_time < self.permit.expires_at,
                not_revoked: self.permit.revoked_at.is_none(),
            },
            bindings: PermitBindingEvidence {
                scope_exact: permit_scope == *self.preflight.scope(),
                preflight_hash_exact: self.permit.preflight_hash == self.preflight.preflight_hash(),
                runtime_mode_exact: runtime.quant_runtime_mode
                    == self.preflight.current_runtime_mode()
                    && self
                        .permit
                        .allowed_runtime_modes
                        .contains(&runtime.quant_runtime_mode),
            },
        }
    }
}

struct PromotionPermitFixtureSpec {
    idempotency_key: &'static str,
    reason: &'static str,
}

struct PromotionPermitFixture {
    policies: Arc<PgPolicyRepository>,
    runtime_repository: Arc<PgRuntimeControlRepository>,
    runtime_controls: RuntimeControlsHandle,
    preflight: Arc<PromotionPreflightService>,
    permit_service: Arc<PromotionPermitService>,
    actor: PromotionPermitActor,
    permit: PromotionPermitInfo,
    initial_preflight: PromotionPreflight,
    initial_projection: PromotionPolicyProjection,
    policy_before: ActivePolicyBundle,
}

impl PromotionPermitFixture {
    async fn issue(
        case: &PromotionPreflightCase,
        decisions: Arc<FeedbackDecisionStageAdapter>,
        spec: PromotionPermitFixtureSpec,
    ) -> Self {
        let policies = Arc::new(PgPolicyRepository::new(case.db.clone()));
        let policy_before = policies
            .load_current_bundle()
            .await
            .expect("load promotion fixture policy")
            .expect("promotion fixture policy exists");
        let runtime_repository = Arc::new(PgRuntimeControlRepository::new(case.db.clone()));
        let runtime = runtime_repository
            .load()
            .await
            .expect("load promotion fixture runtime");
        let runtime_controls =
            RuntimeControlsHandle::new(RuntimeControlSnapshot::from(runtime.clone()));
        let permits = Arc::new(PgPromotionPermitRepository::new(case.db.clone()));
        let preflight = Arc::new(PromotionPreflightService::new(
            PromotionPreflightServiceDeps {
                permits: Arc::clone(&permits) as Arc<dyn PromotionPermitRepository>,
                cycles: Arc::clone(&case.cycles) as Arc<dyn FeedbackCycleRepository>,
                decisions,
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
            .expect("read promotion fixture database clock");
        let plan = preflight
            .prepare_issue(PromotionPreflightDraft {
                feedback_cycle_id: case.claim.cycle.feedback_cycle_id,
                allowed_runtime_modes: vec![QuantRuntimeMode::ReportOnly],
                expires_at: database_now + Duration::minutes(10),
            })
            .await
            .expect("derive server-owned promotion preflight");
        assert_eq!(plan.preflight().scope().category(), MarketCategory::Crypto);
        assert_eq!(
            plan.projection().champion_model_version_id(),
            case.models.champion.model_version_id
        );
        assert_eq!(
            plan.projection().candidate_model_version_id(),
            case.models.candidate.model_version_id
        );
        plan.projection()
            .validate_candidate(plan.projection().prospective_snapshot())
            .expect("promotion projection changes only its exact route and global shadow");
        let actor = UserEntity::find()
            .filter(UserColumn::Username.eq("admin"))
            .one(&case.db)
            .await
            .expect("load promotion fixture admin actor")
            .expect("promotion fixture admin actor exists");
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
                idempotency_key: spec
                    .idempotency_key
                    .parse::<PolicyIdempotencyKey>()
                    .expect("valid promotion fixture idempotency key"),
                scope: plan.preflight().scope().clone(),
                preflight_hash: plan.preflight().preflight_hash(),
                reason: spec.reason.to_owned(),
            })
            .await
            .expect("issue promotion fixture permit")
        {
            PromotionPermitIssueOutcome::Issued(permit) => permit,
            PromotionPermitIssueOutcome::ExactReplay(_) => {
                panic!("first promotion fixture permit issue cannot be a replay")
            }
        };
        let verified = preflight
            .verify_permit(
                &permit.promotion_permit_id,
                case.claim.cycle.feedback_cycle_id,
            )
            .await
            .expect("verify exact promotion fixture permit");
        Self {
            policies,
            runtime_repository,
            runtime_controls,
            preflight,
            permit_service,
            actor: permit_actor,
            permit,
            initial_preflight: verified.preflight().clone(),
            initial_projection: verified.projection().clone(),
            policy_before,
        }
    }

    async fn assert_exact_replay(&self, cycle_id: FeedbackCycleId) {
        let replay = self
            .permit_service
            .issue(IssuePromotionPermit {
                actor: self.actor.clone(),
                idempotency_key: self.permit.idempotency_key.clone(),
                scope: self
                    .permit
                    .scope()
                    .expect("rebuild exact promotion fixture permit scope"),
                preflight_hash: self.permit.preflight_hash,
                reason: self.permit.issuance_reason.clone(),
            })
            .await
            .expect("replay exact promotion fixture permit");
        let PromotionPermitIssueOutcome::ExactReplay(replayed) = replay else {
            panic!("second promotion fixture permit issue must be an exact replay");
        };
        assert_eq!(replayed, self.permit);
        let verified = self
            .preflight
            .verify_permit(&self.permit.promotion_permit_id, cycle_id)
            .await
            .expect("verify exact replayed promotion fixture permit");
        assert_eq!(verified.permit(), &self.permit);
        assert_eq!(
            verified.preflight().preflight_hash(),
            self.initial_preflight.preflight_hash()
        );
        assert_eq!(
            verified.projection().non_route_policy_hash(),
            self.initial_projection.non_route_policy_hash()
        );
        assert_eq!(
            verified.projection().prospective_snapshot(),
            self.initial_projection.prospective_snapshot()
        );
    }
}

struct CandidateReadyState {
    policy_after: ActivePolicyBundle,
    after: InvariantSnapshot,
    invariant_diff: InvariantDiff,
}

impl PromotionPreflightCase {
    async fn candidate_ready_state(
        &self,
        permit_fixture: &PromotionPermitFixture,
    ) -> CandidateReadyState {
        let policy_after = permit_fixture
            .policies
            .load_current_bundle()
            .await
            .expect("load CandidateReady policy after preflight")
            .expect("CandidateReady policy still exists");
        let route_after = self
            .serving
            .generations
            .current_route(BuyModelRoute::Crypto)
            .expect("CandidateReady Crypto route after preflight")
            .published_shadow_identity()
            .expect("CandidateReady published shadow remains complete");
        assert_eq!(policy_after, permit_fixture.policy_before);
        assert_eq!(route_after, self.route_before);
        assert_eq!(
            self.model_repository
                .find_model_version(&self.models.champion.model_version_id)
                .await
                .expect("read CandidateReady champion")
                .expect("CandidateReady champion exists")
                .publication_status,
            PublicationStatus::Published
        );
        assert_eq!(
            self.model_repository
                .find_model_version(&self.models.candidate.model_version_id)
                .await
                .expect("read CandidateReady candidate")
                .expect("CandidateReady candidate exists")
                .publication_status,
            PublicationStatus::Shadow
        );
        let after = self.invariant_probe(None).load().await;
        let invariant_diff = InvariantDiff::between(&self.decision_before, &after);
        assert_protected_unchanged(
            &invariant_diff,
            "CandidateReady",
            &[
                "$.policy_bundle",
                "$.runtime_control",
                "$.model_routes",
                "$.capital_allocations",
                "$.champion_model",
                "$.candidate_model",
                "$.parity_latch",
                "$.in_memory_serving_route",
                "$.policy_apply_readiness",
                "$.deployment_authority",
            ],
        );
        assert!(invariant_diff.any_below("$.cycle"));
        CandidateReadyState {
            policy_after,
            after,
            invariant_diff,
        }
    }
}

impl DecisionPathEvidence {
    async fn candidate_ready() -> Self {
        assert_authority_boundary();
        let (pool, _container) = setup_pg().await;
        let db = pool.connection().clone();
        let artifact_root = ArtifactRoot::create();
        let store: Arc<dyn ArtifactStore> =
            Arc::new(LocalArtifactStore::new(artifact_root.path.clone()));
        let case = Box::pin(PromotionPreflightCase::prepare(db, store)).await;
        let decisions = case.verify_decision_evidence().await;
        let permit_fixture = Box::pin(PromotionPermitFixture::issue(
            &case,
            decisions,
            PromotionPermitFixtureSpec {
                idempotency_key: "p03-preflight-issue-0001",
                reason: "authorize exact CandidateReady Crypto preflight",
            },
        ))
        .await;
        let counts_after_first = load_evidence_counts(&case.db).await;
        Box::pin(permit_fixture.assert_exact_replay(case.claim.cycle.feedback_cycle_id)).await;
        let counts_after_exact_replay = load_evidence_counts(&case.db).await;
        let state = Box::pin(case.candidate_ready_state(&permit_fixture)).await;
        let timeline = timeline_evidence(
            case.cycles
                .list_stage_events(&case.claim.cycle.feedback_cycle_id)
                .await
                .expect("load W4-E04 CandidateReady timeline"),
        );
        let permit_evidence = Box::pin(
            PermitEvidenceProbe {
                cycles: &case.cycles,
                runtime_repository: &permit_fixture.runtime_repository,
                runtime_controls: &permit_fixture.runtime_controls,
                permit: &permit_fixture.permit,
                preflight: &permit_fixture.initial_preflight,
            }
            .load(),
        )
        .await;
        let restart_read_back = Box::pin(
            RestartProbe {
                invariant: case.invariant_probe(None),
                artifacts: Arc::clone(&case.store),
                completion: &case.completion,
                expected_after: &state.after,
                expected_timeline: &timeline,
                expected_permit: Some(&permit_fixture.permit),
                expected_transaction_hash: None,
            }
            .verify(),
        )
        .await;
        let evidence = Self {
            path: DecisionPath::CandidateReady,
            decision: wire_name(&FeedbackDecision::CandidateReady),
            decision_reason: "feedback_candidate_ready_governance_required".to_owned(),
            exact_ids: exact_identifiers(ExactIdInput {
                cycle: &case.claim.cycle,
                models: &case.models,
                policy_before: &permit_fixture.policy_before,
                policy_after: &state.policy_after,
                permit: Some(&permit_fixture.permit),
                policy_activation_id: None,
                promotion_transaction_hash: None,
                route: BuyModelRoute::Crypto,
            }),
            decision_artifact: decision_artifact_evidence(
                &case.completion,
                &case.models,
                Some(permit_fixture.initial_preflight.decision_job_input_hash()),
            ),
            permit: Some(permit_evidence),
            worm_timeline: timeline,
            before: case.decision_before.clone(),
            after: state.after,
            invariant_diff: state.invariant_diff,
            replay: ReplayEvidence::new(
                "committed:candidate_ready_and_permit_issued",
                "exact_permit_and_artifact_replay",
                case.decision_counts_before.clone(),
                counts_after_first,
                counts_after_exact_replay,
            ),
            restart_read_back,
            fault_injection_rollback_verified: false,
        };
        evidence.validate();
        evidence
    }
}

pub async fn promotion_preflight_contracts() {
    Box::pin(DecisionPathEvidence::candidate_ready())
        .await
        .validate();
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
        let permit_fixture = Box::pin(PromotionPermitFixture::issue(
            case,
            case.decision_stage(),
            PromotionPermitFixtureSpec {
                idempotency_key: "p04-atomic-promotion-0001",
                reason: "authorize exact atomic Crypto route promotion",
            },
        ))
        .await;
        let promotions = Arc::new(PgModelRoutePromotionRepository::new(case.db.clone()));
        let raw_apply = Arc::new(PromotionPolicyPort::new(PolicyBundleIdentity::from(
            &permit_fixture.policy_before,
        )));
        let policy_apply = Arc::new(CommittedPolicyApplicator::new(
            Arc::clone(&raw_apply) as Arc<dyn PolicySnapshotPort>,
            PolicyBundleIdentity::from(&permit_fixture.policy_before),
        ));
        let service = Arc::new(ModelRoutePromotionService::new(
            ModelRoutePromotionServiceDeps {
                preflight: Arc::clone(&permit_fixture.preflight),
                repository: Arc::clone(&promotions) as Arc<dyn ModelRoutePromotionRepository>,
                policies: Arc::clone(&permit_fixture.policies) as Arc<dyn PolicyRepository>,
                policy_apply: Arc::clone(&policy_apply) as Arc<dyn CommittedPolicyApplyPort>,
            },
        ));
        let PromotionPermitFixture {
            policies,
            runtime_repository,
            runtime_controls,
            preflight,
            permit_service,
            actor,
            permit,
            initial_preflight,
            initial_projection: _,
            policy_before,
        } = permit_fixture;
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
            actor,
            permit,
            initial_preflight,
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
    invariant_before: InvariantSnapshot,
    evidence_counts_before: RowCountSnapshot,
}

struct PromotedState {
    policy_after: ActivePolicyBundle,
    after: InvariantSnapshot,
    invariant_diff: InvariantDiff,
    timeline: Vec<TimelineEventEvidence>,
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
        let invariant_before = case
            .invariant_probe(Some(Arc::clone(&harness.policy_apply)))
            .load()
            .await;
        let evidence_counts_before = load_evidence_counts(&db).await;
        Self {
            db,
            case,
            harness,
            request,
            counts_before,
            champion_before,
            candidate_before,
            cycle_before,
            invariant_before,
            evidence_counts_before,
        }
    }

    fn invariant_probe(&self) -> InvariantProbe {
        self.case
            .invariant_probe(Some(Arc::clone(&self.harness.policy_apply)))
    }

    fn assert_promoted_delta(
        &self,
        policy_after: &ActivePolicyBundle,
        invariant_diff: &InvariantDiff,
    ) {
        assert_protected_unchanged(
            invariant_diff,
            "Promoted",
            &[
                "$.runtime_control",
                "$.capital_allocations",
                "$.champion_model",
                "$.parity_latch",
                "$.in_memory_serving_route",
                "$.deployment_authority",
            ],
        );
        for changed in [
            "$.policy_bundle",
            "$.model_routes",
            "$.candidate_model",
            "$.cycle",
            "$.policy_apply_readiness",
        ] {
            assert!(
                invariant_diff.any_below(changed),
                "Promoted path failed to change required invariant {changed}"
            );
        }
        let candidate_changes = invariant_diff
            .changes
            .iter()
            .filter(|change| change.path.starts_with("$.candidate_model."))
            .map(|change| change.path.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            candidate_changes,
            BTreeSet::from([
                "$.candidate_model.publication_status",
                "$.candidate_model.published_at",
            ]),
            "candidate promotion changed fields beyond Shadow-to-Published"
        );
        let policy_before = &self.harness.policy_before;
        assert_eq!(
            policy_after.snapshot.recommendation,
            policy_before.snapshot.recommendation
        );
        assert_eq!(
            policy_after.snapshot.execution_risk,
            policy_before.snapshot.execution_risk
        );
        assert_eq!(
            policy_after.snapshot.report_schedule,
            policy_before.snapshot.report_schedule
        );
        assert_eq!(
            policy_after.snapshot.operational_control,
            policy_before.snapshot.operational_control
        );
        assert_eq!(
            policy_after.snapshot.execution_authorization,
            policy_before.snapshot.execution_authorization
        );
        assert_eq!(
            policy_after.snapshot.profile_artifacts,
            policy_before.snapshot.profile_artifacts
        );
        let before_routes = &policy_before.snapshot.model_routing.model;
        let after_routes = &policy_after.snapshot.model_routing.model;
        assert_eq!(
            after_routes.active_model_version_id,
            before_routes.active_model_version_id
        );
        assert_eq!(
            after_routes.active_exit_model_version_id,
            before_routes.active_exit_model_version_id
        );
        assert_eq!(
            after_routes
                .category_model_pointers
                .get(&MarketCategory::Weather),
            before_routes
                .category_model_pointers
                .get(&MarketCategory::Weather)
        );
        assert_eq!(
            after_routes
                .category_model_pointers
                .get(&MarketCategory::Crypto)
                .map(|pointer| *pointer.id()),
            Some(self.case.models.candidate.model_version_id)
        );
        assert_eq!(
            before_routes
                .category_model_pointers
                .get(&MarketCategory::Crypto)
                .map(|pointer| *pointer.id()),
            Some(self.case.models.champion.model_version_id)
        );
        assert_eq!(
            before_routes
                .shadow_model_version_id
                .as_ref()
                .map(|pointer| *pointer.id()),
            Some(self.case.models.candidate.model_version_id)
        );
        assert!(after_routes.shadow_model_version_id.is_none());
    }

    async fn promoted_state(&self) -> PromotedState {
        let policy_after = self
            .harness
            .policies
            .load_current_bundle()
            .await
            .expect("load W4-E04 promoted policy")
            .expect("W4-E04 promoted policy exists");
        let after = self.invariant_probe().load().await;
        let timeline = timeline_evidence(
            self.case
                .cycles
                .list_stage_events(&self.case.claim.cycle.feedback_cycle_id)
                .await
                .expect("load W4-E04 Promoted timeline"),
        );
        let invariant_diff = InvariantDiff::between(&self.invariant_before, &after);
        self.assert_promoted_delta(&policy_after, &invariant_diff);
        PromotedState {
            policy_after,
            after,
            invariant_diff,
            timeline,
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
        assert_eq!(
            load_evidence_counts(&self.db).await,
            self.evidence_counts_before,
            "injected promotion rollback changed the complete W4-E04 row-count witness"
        );
        assert_eq!(
            wire_value(&self.invariant_probe().load().await),
            wire_value(&self.invariant_before),
            "injected promotion rollback changed a protected W4-E04 invariant"
        );
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
        let audits = PgModelGovernanceAuditRepository::new(self.db.clone());
        for model_version_id in [
            self.case.models.champion.model_version_id,
            self.case.models.candidate.model_version_id,
        ] {
            let lineage = audits
                .list_promotions_by_version(&model_version_id)
                .await
                .expect("read exact P04 model promotion lineage");
            assert_eq!(lineage.len(), 1);
            assert_eq!(lineage[0].audit_id, committed.audit.audit_id);
            assert_eq!(lineage[0].audit_event_id, committed.audit.audit_event_id);
            assert_eq!(lineage[0].detail, committed.audit.detail);
        }
        assert!(
            audits
                .list_promotions_by_version(&ModelVersionId::from_v7())
                .await
                .expect("filter unrelated P04 model promotion lineage")
                .is_empty()
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

impl DecisionPathEvidence {
    async fn promoted() -> Self {
        assert_authority_boundary();
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
        let counts_after_first = load_evidence_counts(&case.db).await;
        Box::pin(case.assert_replay(&committed)).await;
        let counts_after_exact_replay = load_evidence_counts(&case.db).await;
        let state = Box::pin(case.promoted_state()).await;
        let permit_evidence = Box::pin(
            PermitEvidenceProbe {
                cycles: &case.case.cycles,
                runtime_repository: &case.harness.runtime_repository,
                runtime_controls: &case.harness.runtime_controls,
                permit: &case.harness.permit,
                preflight: &case.harness.initial_preflight,
            }
            .load(),
        )
        .await;
        let restart_read_back = Box::pin(
            RestartProbe {
                invariant: case.invariant_probe(),
                artifacts: Arc::clone(&case.case.store),
                completion: &case.case.completion,
                expected_after: &state.after,
                expected_timeline: &state.timeline,
                expected_permit: Some(&case.harness.permit),
                expected_transaction_hash: Some(committed.transaction_hash),
            }
            .verify(),
        )
        .await;
        let evidence = Self {
            path: DecisionPath::Promoted,
            decision: wire_name(&FeedbackDecision::Promoted),
            decision_reason: committed.audit.reason.clone(),
            exact_ids: exact_identifiers(ExactIdInput {
                cycle: &case.case.claim.cycle,
                models: &case.case.models,
                policy_before: &case.harness.policy_before,
                policy_after: &state.policy_after,
                permit: Some(&case.harness.permit),
                policy_activation_id: Some(committed.activation.policy_activation_id.to_string()),
                promotion_transaction_hash: Some(committed.transaction_hash),
                route: BuyModelRoute::Crypto,
            }),
            decision_artifact: decision_artifact_evidence(
                &case.case.completion,
                &case.case.models,
                Some(case.harness.initial_preflight.decision_job_input_hash()),
            ),
            permit: Some(permit_evidence),
            worm_timeline: state.timeline,
            before: case.invariant_before.clone(),
            after: state.after,
            invariant_diff: state.invariant_diff,
            replay: ReplayEvidence::new(
                "committed:promoted",
                "exact_concurrent_and_explicit_promotion_replay",
                case.evidence_counts_before.clone(),
                counts_after_first,
                counts_after_exact_replay,
            ),
            restart_read_back,
            fault_injection_rollback_verified: true,
        };
        evidence.validate();
        evidence
    }
}

pub async fn model_route_promotion_contracts() {
    Box::pin(DecisionPathEvidence::promoted()).await.validate();
}

pub async fn decision_path_evidence_contracts() {
    let paths = vec![
        Box::pin(TerminalDecisionCase::InsufficientShadow.run()).await,
        Box::pin(TerminalDecisionCase::RejectedComparison.run()).await,
        Box::pin(DecisionPathEvidence::candidate_ready()).await,
        Box::pin(DecisionPathEvidence::promoted()).await,
    ];
    let artifact = DecisionPathEvidenceManifest::new(paths).write();
    assert!(artifact.path.is_file());
    ContentHash::parse(&artifact.content_hash).expect("evidence hash uses canonical BLAKE3 text");
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
