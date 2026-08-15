//! F06/F09/F10-to-F11 contracts against real `PostgreSQL` and object storage.

use std::{
    collections::BTreeMap,
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
    observability::metrics_hub::MetricsHub,
    runtime_config::{CommittedPolicyApplicator, DecisionPolicyStore},
    service::{
        feedback_comparison_stage::{FeedbackComparisonStageAdapter, FeedbackComparisonStageDeps},
        feedback_coordinator::{
            FeedbackShadowCancellationPort, FeedbackStageDirective, FeedbackStagePreparation,
            FeedbackStageSuccess,
        },
        feedback_decision::{FeedbackDecisionExecutionDeps, FeedbackDecisionExecutionService},
        feedback_decision_stage::{FeedbackDecisionStageAdapter, FeedbackDecisionStageDeps},
        feedback_evaluation::{
            FeedbackEvaluationReservationDeps, FeedbackEvaluationReservationService,
        },
        feedback_governance_stage::{FeedbackGovernanceStageAdapter, FeedbackGovernanceStageDeps},
        feedback_learning_stage::{FeedbackLearningStageAdapter, FeedbackLearningStageDeps},
        feedback_recipe_stage::{FeedbackRecipeStageAdapter, FeedbackRecipeStageDeps},
        feedback_shadow::{FeedbackShadowExecutionDeps, FeedbackShadowExecutionService},
        feedback_shadow_binding::{
            ShadowBindingCancellationDeps, ShadowBindingCancellationService,
            ShadowBindingExecutionDeps, ShadowBindingExecutionService,
        },
        feedback_shadow_binding_stage::{
            FeedbackShadowBindingStageAdapter, FeedbackShadowBindingStageDeps,
        },
        feedback_shadow_stage::{FeedbackShadowStageAdapter, FeedbackShadowStageDeps},
        model_route_bootstrap::{ModelRouteBootstrapService, ModelRouteBootstrapServiceDeps},
        model_route_evidence::{ModelRouteEvidenceDeps, ModelRouteEvidenceService},
        model_route_governance::{ModelRouteGovernanceService, ModelRouteGovernanceServiceDeps},
        model_serving_generation::{
            ModelServingGenerationStore, PublishedChampionRouteIdentity,
            PublishedShadowRouteIdentity,
        },
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
        api::{
            CpcvBacktestJobParams, FeedbackCoverageJobParams, FeedbackDriftJobParams,
            FitModelCalibratorRequest, GatePreviewIntent, ModelTrainJobParams,
            QualityGateReportView, RunCpcvBacktestRequest, TrainModelRequest,
        },
        governance::{RuntimeControlSnapshot, RuntimeControlUpdate},
        ports::{
            BootstrapQualityGateEvidence, BootstrapQualityGateInput, CalibratedModelSealCommand,
            CandidateQualityGateEvidence, CommittedPolicyApplyPort, FeedbackAttributionJobParams,
            FeedbackAttributionManifest, FeedbackCalibrationCommand, FeedbackCalibrationJobParams,
            FeedbackCandidateFamily, FeedbackCandidateValidation, FeedbackComparisonArtifactRef,
            FeedbackCpcvCommand, FeedbackCpcvJobParams, FeedbackDecisionExecutionPort,
            FeedbackLearningStageArtifactRef, FeedbackShadowExecutionPort, FeedbackTrainingCommand,
            FeedbackTrainingJobParams, FeedbackTruthFreezeArtifact, FeedbackTruthFreezeJobParams,
            FeedbackValidationArtifact, FeedbackValidationArtifactRef, FeedbackValidationJobParams,
            FeedbackValidationTrialOutcome, GovernanceActor, ModelCalibrationFitJobParams,
            ModelGovernancePort, PolicySnapshotPort, PreparedPolicySnapshot,
            ShadowBindingExecutionPort, ShadowBindingLifecycle,
        },
        quant::{
            CommitModelRoutePromotion, ExecutionAttemptBarrier, ExecutionRollupBarrier,
            FeedbackCycleInfo, FeedbackCycleTerminal, FeedbackStageEventInfo,
            FeedbackStageEventInput, FeedbackStageJobIdentity, IssuePromotionPermit,
            ModelVersionInfo, NewBacktestPathSet, NewBacktestPathSetInput, NewFeedbackStageEvent,
            NewModelRun, NewResearchJob, NoopProgressSink, PromoteModelRoute, PromotionPermitActor,
            PromotionPermitInfo, PromotionPermitScope, PromotionPermitScopeInput,
            PromotionPermitStatus, PromotionPolicyProjection, PromotionPreflight,
            PromotionPreflightInput, ResearchJobArtifactRef, ResearchJobFinalization,
            ResearchJobInfo, ResearchJobResultRef, ResolutionProjectionBarrier,
            RevokePromotionPermit,
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
            CalibrationMethod, DatasetPurpose, DownsideSource, FeedbackCycleStatus,
            FeedbackDecision, FeedbackStage, FeedbackStageEventKind, ModelRunKind,
            QuantRuntimeMode, ResearchJobKind, ResearchJobResultKind, ResearchJobStatus,
            ShadowBindingStatus,
        },
        runtime_config::PolicyActivationKind,
    },
    hashing::CanonicalDigest,
    runtime_config::{
        ActivePolicyBundle, BuyModelRoute, DecisionPolicySnapshot, PolicyApplyDegradedCause,
        PolicyApplyReadiness, PolicyBundleIdentity,
    },
    types::{
        BacktestPathSetId, BacktestReportId, CalibrationArtifactId, ContentHash, FeatureValue,
        FeedbackCycleId, FeedbackDecisionArtifactId, FeedbackDriftArtifactId, ModelRunId,
        ModelVersionId, PolicyIdempotencyKey, PromotionPermitId, ResearchJobParams, RoleCode,
        ShadowBindingArtifactId, TrainingDatasetId, WorkerId,
        backtest::{
            BacktestPath, CpcvEstimatorIdentity, CpcvFoldArtifact, CpcvFoldArtifacts,
            CpcvFoldCalibrationPolicy, CpcvFoldValidationRegime, CpcvMethodologyBinding,
            CpcvPathSetSubject, CpcvTrialPathBinding, SharpeDistribution,
        },
        model_lineage::ModelVersionDerivation,
        model_quality::{
            GateClass, GateId, GateIntent, GateOutcome, GateStatus, GateSubject, QualityGateReport,
            QualityGateReportInput,
        },
        stable_name::FeatureName,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestPathSetRepository, PgBacktestReportRepository, PgCalibrationArtifactRepository,
        PgFeatureParityRepository, PgFeedbackCycleRepository, PgModelCandidateManifestRepository,
        PgModelGovernanceAuditRepository, PgModelRegistryRepository,
        PgModelRouteBootstrapRepository, PgModelRoutePromotionRepository,
        PgModelRouteShadowBindingRepository, PgModelRunRepository, PgPolicyRepository,
        PgPromotionPermitRepository, PgResearchJobRepository, PgRuntimeControlRepository,
        PgShadowComparisonRepository, PgTrainingDatasetRepository,
    },
    traits::{
        BacktestPathSetRepository, BacktestReportRepository, CalibrationArtifactRepository,
        CpcvPathSetCommit, FeatureParityLatchActor, FeatureParityRepository, FeedbackCycleClaim,
        FeedbackCycleGeneration, FeedbackCycleLeaseGuard, FeedbackCycleRepository,
        ModelCandidateManifestRepository, ModelGovernanceAuditRepository, ModelRegistryRepository,
        ModelRouteBootstrapRepository, ModelRoutePromotionCommit, ModelRoutePromotionOutcome,
        ModelRoutePromotionRepository, ModelRouteShadowBindingRepository, ModelRunRepository,
        PolicyRepository, PromotionPermitIssueOutcome, PromotionPermitRepository,
        PromotionPermitRevokeOutcome, ResearchJobEnqueueOutcome, ResearchJobRepository,
        RuntimeControlRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    feedback::{
        ConceptDriftDetail, DriftGateOutcome, FEEDBACK_DRIFT_ARTIFACT_FORMAT_VERSION,
        FeatureDriftDetail, FeedbackCoverageArtifact, FeedbackCoverageCodec, FeedbackDriftArtifact,
        FeedbackDriftCodec, LabelDriftDetail, drift_gate, drift_observations,
    },
    feedback_comparison::FeedbackComparisonCodec,
    feedback_decision::FeedbackDecisionCodec,
    feedback_governance::FeedbackGovernanceCodec,
    feedback_learning::{
        FeedbackCalibrationStageResult, FeedbackCpcvStageResult, FeedbackLearningStageArtifact,
        FeedbackLearningStageCodec, FeedbackLearningStageResults, FeedbackTrainingStageResult,
    },
    feedback_shadow_binding::ShadowBindingCodec,
};
use quant_pivot_system_tests::{
    postgres::{run_suite_large_stack, setup_pg},
    support::{
        artifact_store::ReadTamperArtifactStoreFixture,
        model_serving_fixtures::ModelVersionFixture, research_fixtures::cscv_selection_fixture,
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
    feedback_boot_schema::{content_hash, persist_recipe_plan_fixture},
    feedback_decision_evidence::{
        DecisionArtifactEvidence, DecisionPath, DecisionPathEvidence, DecisionPathEvidenceManifest,
        DeploymentAuthorityBoundary, ExactDecisionIdentifiers, InvariantDiff, InvariantSnapshot,
        PermitBindingEvidence, PermitEvidence, PermitLifecycleEvidence, ReplayEvidence,
        RestartReadBackEvidence, RowCountSnapshot, TimelineEventEvidence,
    },
    feedback_learning_stage::{
        FeedbackDatasetParamsExt, candidate_dataset, dataset_params, persist_artifact,
    },
    feedback_shadow_stage::{
        ActivatedServing, ArtifactRoot, ShadowModels, activate_crypto_generation,
        build_crypto_models, build_serving, comparison_params_with_validation, insert_observation,
        insert_stable_observations, record_comparison, record_crypto_cycle,
    },
    feedback_signal_stage::{CoverageScenario, coverage_artifact, persist_coverage},
};

const JOB_LEASE_SECS: i64 = 90;
const SHADOW_MODEL_BUDGET_BYTES: u64 = 1 << 30;

fn recipe_stage(
    cycles: &Arc<PgFeedbackCycleRepository>,
    jobs: &Arc<PgResearchJobRepository>,
    store: Arc<dyn ArtifactStore>,
) -> Arc<FeedbackRecipeStageAdapter> {
    Arc::new(
        FeedbackRecipeStageAdapter::try_new(FeedbackRecipeStageDeps {
            cycles: Arc::clone(cycles) as Arc<dyn FeedbackCycleRepository>,
            jobs: Arc::clone(jobs) as Arc<dyn ResearchJobRepository>,
            artifacts: store,
            max_recovery_attempts: 3,
        })
        .expect("build F11 RecipePlan stage"),
    )
}

fn comparison_stage(
    db: &DatabaseConnection,
    cycles: &Arc<PgFeedbackCycleRepository>,
    jobs: &Arc<PgResearchJobRepository>,
    store: Arc<dyn ArtifactStore>,
) -> Arc<FeedbackComparisonStageAdapter> {
    let recipes = recipe_stage(cycles, jobs, Arc::clone(&store));
    let learning = Arc::new(
        FeedbackLearningStageAdapter::try_new(FeedbackLearningStageDeps {
            jobs: Arc::clone(jobs) as Arc<dyn ResearchJobRepository>,
            artifacts: Arc::clone(&store),
            recipes,
            max_recovery_attempts: 3,
        })
        .expect("build terminal Comparison learning stage"),
    );
    let governance = Arc::new(
        FeedbackGovernanceStageAdapter::try_new(FeedbackGovernanceStageDeps {
            cycles: Arc::clone(cycles) as Arc<dyn FeedbackCycleRepository>,
            jobs: Arc::clone(jobs) as Arc<dyn ResearchJobRepository>,
            artifacts: Arc::clone(&store),
            learning: Arc::clone(&learning),
            max_recovery_attempts: 3,
        })
        .expect("build terminal Comparison governance stage"),
    );
    let reservations = Arc::new(FeedbackEvaluationReservationService::new(
        FeedbackEvaluationReservationDeps {
            cycles: Arc::clone(cycles) as Arc<dyn FeedbackCycleRepository>,
            datasets: Arc::new(PgTrainingDatasetRepository::new(db.clone()))
                as Arc<dyn TrainingDatasetRepository>,
            learning_stages: Arc::clone(&learning),
        },
    ));
    Arc::new(
        FeedbackComparisonStageAdapter::try_new(FeedbackComparisonStageDeps {
            cycles: Arc::clone(cycles) as Arc<dyn FeedbackCycleRepository>,
            jobs: Arc::clone(jobs) as Arc<dyn ResearchJobRepository>,
            models: Arc::new(PgModelRegistryRepository::new(db.clone()))
                as Arc<dyn ModelRegistryRepository>,
            path_sets: Arc::new(PgBacktestPathSetRepository::new(db.clone()))
                as Arc<dyn BacktestPathSetRepository>,
            artifacts: store,
            learning_stages: learning,
            governance_stages: governance,
            evaluation_reservations: reservations,
            max_recovery_attempts: 3,
        })
        .expect("build terminal Comparison stage"),
    )
}

fn shadow_binding_stage(
    db: &DatabaseConnection,
    cycles: &Arc<PgFeedbackCycleRepository>,
    jobs: &Arc<PgResearchJobRepository>,
    store: Arc<dyn ArtifactStore>,
    recipes: Arc<FeedbackRecipeStageAdapter>,
) -> Arc<FeedbackShadowBindingStageAdapter> {
    Arc::new(
        FeedbackShadowBindingStageAdapter::try_new(FeedbackShadowBindingStageDeps {
            cycles: Arc::clone(cycles) as Arc<dyn FeedbackCycleRepository>,
            jobs: Arc::clone(jobs) as Arc<dyn ResearchJobRepository>,
            models: Arc::new(PgModelRegistryRepository::new(db.clone())),
            path_sets: Arc::new(PgBacktestPathSetRepository::new(db.clone())),
            calibrations: Arc::new(PgCalibrationArtifactRepository::new(db.clone())),
            policies: Arc::new(PgPolicyRepository::new(db.clone())),
            manifests: Arc::new(PgModelCandidateManifestRepository::new(db.clone())),
            artifacts: store,
            recipes,
            total_shadow_model_budget_bytes: SHADOW_MODEL_BUDGET_BYTES,
            max_recovery_attempts: 3,
        })
        .expect("build F11 ShadowBind stage"),
    )
}

struct ShadowBindingApplyProbe {
    readiness: Mutex<PolicyApplyReadiness>,
}

impl ShadowBindingApplyProbe {
    const fn new(initial: PolicyBundleIdentity) -> Self {
        Self {
            readiness: Mutex::new(PolicyApplyReadiness::Ready { applied: initial }),
        }
    }
}

#[async_trait::async_trait]
impl CommittedPolicyApplyPort for ShadowBindingApplyProbe {
    async fn apply_committed(
        &self,
        bundle: ActivePolicyBundle,
    ) -> Result<PolicyApplyReadiness, ControlError> {
        let readiness = PolicyApplyReadiness::Ready {
            applied: PolicyBundleIdentity::from(&bundle),
        };
        *self
            .readiness
            .lock()
            .map_err(|_| ControlError::Engine("ShadowBind apply probe is poisoned".to_owned()))? =
            readiness;
        Ok(readiness)
    }

    fn publish_prepared(
        &self,
        prepared: PreparedPolicySnapshot,
        bundle: ActivePolicyBundle,
    ) -> Result<PolicyApplyReadiness, ControlError> {
        prepared.publish_bundle(bundle.clone())?;
        let readiness = PolicyApplyReadiness::Ready {
            applied: PolicyBundleIdentity::from(&bundle),
        };
        *self
            .readiness
            .lock()
            .map_err(|_| ControlError::Engine("ShadowBind apply probe is poisoned".to_owned()))? =
            readiness;
        Ok(readiness)
    }

    fn readiness(&self) -> PolicyApplyReadiness {
        *self
            .readiness
            .lock()
            .expect("lock ShadowBind apply readiness")
    }
}

async fn record_shadow_binding(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    serving: &ActivatedServing,
    cycles: &Arc<PgFeedbackCycleRepository>,
    jobs: &Arc<PgResearchJobRepository>,
    claim: &FeedbackCycleClaim,
    event_sequence: i64,
) -> ActivatedServing {
    let recipes = recipe_stage(cycles, jobs, Arc::clone(store));
    let stage = shadow_binding_stage(db, cycles, jobs, Arc::clone(store), recipes);
    let identity = FeedbackStageJobIdentity::try_root(
        claim.cycle.feedback_cycle_id,
        FeedbackStage::ShadowBind,
    )
    .expect("ShadowBind fixture identity");
    let job = stage
        .prepare(&claim.cycle, claim.lease, identity)
        .await
        .expect("prepare exact ShadowBind job");
    let params = match &job.params_json {
        ResearchJobParams::FeedbackShadowBind(params) => params.as_ref().clone(),
        _ => panic!("ShadowBind stage emitted another job kind"),
    };
    match jobs.enqueue(job).await.expect("enqueue ShadowBind job") {
        ResearchJobEnqueueOutcome::Inserted(_) | ResearchJobEnqueueOutcome::AlreadyPresent(_) => {}
    }
    let worker = WorkerId::from_v7();
    let leased = jobs
        .lease_next(
            &[ResearchJobKind::FeedbackShadowBind],
            &worker,
            Utc::now() + Duration::seconds(JOB_LEASE_SECS),
        )
        .await
        .expect("lease ShadowBind job")
        .expect("queued ShadowBind job");
    assert_eq!(leased.job_id, identity.job_id());

    let policies = Arc::new(PgPolicyRepository::new(db.clone()));
    let active = policies
        .load_current_bundle()
        .await
        .expect("load pre-ShadowBind policy")
        .expect("pre-ShadowBind policy exists");
    let runtime_repository = Arc::new(PgRuntimeControlRepository::new(db.clone()));
    let runtime = runtime_repository
        .load()
        .await
        .expect("load ShadowBind runtime control");
    let runtime_controls = RuntimeControlsHandle::new(RuntimeControlSnapshot::from(runtime));
    let policy_store = Arc::new(DecisionPolicyStore::new_active(active.clone()));
    let route_evidence = Arc::new(ModelRouteEvidenceService::new(ModelRouteEvidenceDeps {
        policies: Arc::clone(&policies) as Arc<dyn PolicyRepository>,
        durable_runtime: Arc::clone(&runtime_repository) as Arc<dyn RuntimeControlRepository>,
        runtime_controls,
        policy_store: Arc::clone(&policy_store),
        models: Arc::new(PgModelRegistryRepository::new(db.clone()))
            as Arc<dyn ModelRegistryRepository>,
        feature_parity: Arc::new(PgFeatureParityRepository::new(db.clone()))
            as Arc<dyn FeatureParityRepository>,
        runtime_registry: Arc::clone(&serving.runtime_registry),
        serving_generations: Arc::clone(&serving.generations),
    }));
    let policy_apply = Arc::new(ShadowBindingApplyProbe::new(PolicyBundleIdentity::from(
        &active,
    )));
    let result = ShadowBindingExecutionService::new(ShadowBindingExecutionDeps {
        bindings: Arc::new(PgModelRouteShadowBindingRepository::new(db.clone()))
            as Arc<dyn ModelRouteShadowBindingRepository>,
        policy_apply: Arc::clone(&policy_apply) as Arc<dyn CommittedPolicyApplyPort>,
        route_evidence,
        artifacts: Arc::clone(store),
    })
    .bind_shadow(
        params.clone(),
        Arc::new(NoopProgressSink),
        CancellationToken::new(),
    )
    .await
    .expect("commit and converge exact ShadowBind");
    let binding_bytes = store
        .get(&result.artifact.uri)
        .await
        .expect("read committed ShadowBind artifact");
    let binding =
        ShadowBindingCodec::decode(&binding_bytes).expect("decode committed ShadowBind artifact");
    let info = jobs
        .finalize(
            &identity.job_id(),
            &worker,
            ResearchJobFinalization::succeeded(
                Some(ResearchJobResultRef {
                    kind: ResearchJobResultKind::ShadowBindingArtifact,
                    id: result.artifact_id.as_uuid(),
                }),
                Some(result.artifact.clone()),
                None,
            ),
        )
        .await
        .expect("finalize ShadowBind job");
    stage
        .succeeded(&claim.cycle, &info)
        .await
        .expect("verify exact ShadowBind terminal artifact");
    cycles
        .append_stage(
            claim.lease,
            NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                feedback_cycle_id: claim.cycle.feedback_cycle_id,
                event_sequence,
                stage: FeedbackStage::ShadowBind,
                event_kind: FeedbackStageEventKind::Succeeded,
                trigger_family: None,
                research_job_id: Some(identity.job_id()),
                actor: None,
                reason_code: None,
                evidence_uri: Some(result.artifact.uri),
                evidence_hash: Some(result.artifact.content_hash),
                occurred_at: info.finished_at.expect("ShadowBind terminal time"),
            })
            .expect("seal ShadowBind success event"),
        )
        .await
        .expect("append ShadowBind success event");
    let converged = build_serving(db, store).await;
    let committed = PgPolicyRepository::new(db.clone())
        .load_current_bundle()
        .await
        .expect("reload committed ShadowBind policy")
        .expect("committed ShadowBind policy exists");
    let published = converged
        .generations
        .current_route(claim.cycle.route)
        .expect("committed ShadowBind route exists")
        .published_shadow_identity()
        .expect("committed ShadowBind route has a shadow");
    assert_eq!(
        policy_apply.readiness(),
        PolicyApplyReadiness::Ready {
            applied: PolicyBundleIdentity::from(&committed),
        }
    );
    assert_eq!(
        published.candidate_model_version_id,
        params.candidate_model_version_id
    );
    assert_eq!(published.shadow_bound_at, binding.receipt.bound_at);
    assert_eq!(
        published.route_generation,
        binding.receipt.binding_generation
    );
    converged
}

const PROMOTION_REPOSITORY_SOURCE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../quant-pivot-repository/src/postgres/quant/model_route_promotion.rs"
));
const PROMOTION_SERVICE_SOURCE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../quant-pivot-core/src/service/model_route_governance.rs"
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

fn expiring_preflight(
    preflight: &PromotionPreflight,
    expires_at: DateTime<Utc>,
) -> PromotionPreflight {
    let original = preflight.scope();
    let scope = PromotionPermitScope::try_new(PromotionPermitScopeInput {
        feedback_cycle_id: original.feedback_cycle_id(),
        profile_ref: original.profile_ref().clone(),
        category: original.category(),
        expected_policy_generation: original.expected_policy_generation(),
        expected_runtime_control_revision: original.expected_runtime_control_revision(),
        expected_decision_policy_snapshot_id: original.expected_snapshot_id(),
        expected_snapshot_hash: original.expected_snapshot_hash(),
        expected_route_generation: original.expected_route_generation(),
        champion_model_version_id: original.champion_model_version_id(),
        champion_serving_contract_hash: original.champion_serving_contract_hash(),
        candidate_model_version_id: original.candidate_model_version_id(),
        candidate_manifest_id: original.candidate_manifest_id(),
        candidate_manifest_hash: original.candidate_manifest_hash(),
        promotion_gate_hash: original.promotion_gate_hash(),
        allowed_runtime_modes: original.allowed_runtime_modes().to_vec(),
        non_route_policy_hash: original.non_route_policy_hash(),
        serving_constraints_hash: original.serving_constraints_hash(),
        expires_at,
    })
    .expect("build short-lived P06 permit scope");
    PromotionPreflight::try_seal(PromotionPreflightInput {
        scope,
        feedback_cycle_id: preflight.feedback_cycle_id(),
        cycle_idempotency_hash: preflight.cycle_idempotency_hash(),
        decision_artifact_id: preflight.decision_artifact_id(),
        decision_artifact_hash: preflight.decision_artifact_hash(),
        decision_object_hash: preflight.decision_object_hash(),
        decision_job_input_hash: preflight.decision_job_input_hash(),
        shadow_artifact_id: preflight.shadow_artifact_id(),
        shadow_artifact_hash: preflight.shadow_artifact_hash(),
        shadow_object_hash: preflight.shadow_object_hash(),
        shadow_contract_hash: preflight.shadow_contract_hash(),
        candidate_recipe_hash: preflight.candidate_recipe_hash(),
        serving_constraints: preflight.serving_constraints().clone(),
        current_runtime_mode: preflight.current_runtime_mode(),
        runtime_control_revision: preflight.runtime_control_revision(),
    })
    .expect("seal short-lived P06 promotion preflight")
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
        "champion_model_version_id": identity.champion_model_version_id,
        "champion_serving_contract_hash": identity.champion_serving_contract_hash,
        "candidate_model_version_id": identity.candidate_model_version_id,
        "candidate_serving_contract_hash": identity.candidate_serving_contract_hash,
        "minimum_topn_decision_overlap": identity.minimum_topn_decision_overlap,
        "required_shadow_window_secs": identity.required_shadow_window_secs,
    })
}

fn champion_route_value(identity: &PublishedChampionRouteIdentity) -> Value {
    json!({
        "route": route_name(identity.route),
        "category_scope": identity.category_scope,
        "research_profile_artifact_id": identity.research_profile_artifact_id,
        "decision_policy_snapshot_id": identity.decision_policy_snapshot_id,
        "decision_policy_snapshot_hash": identity.decision_policy_snapshot_hash,
        "policy_bundle_generation": identity.policy_bundle_generation,
        "champion_model_version_id": identity.champion_model_version_id,
        "champion_serving_contract_hash": identity.champion_serving_contract_hash,
        "candidate_model_version_id": Value::Null,
        "candidate_serving_contract_hash": Value::Null,
        "champion_bound_at": identity.champion_bound_at,
        "route_generation": identity.route_generation,
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
            .expect("load W4-E04 in-memory serving route");
        let in_memory_serving_route = route.published_shadow_identity().map_or_else(
            |_| {
                champion_route_value(
                    &route
                        .published_champion_identity()
                        .expect("W4-E04 in-memory champion route is complete"),
                )
            },
            |identity| serving_route_value(&identity),
        );
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
            in_memory_serving_route,
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
    coverage: &'a FeedbackCoverageArtifact,
    coverage_ref: &'a ResearchJobArtifactRef,
}

impl AdvancingDriftSeed<'_> {
    fn seal(self) -> FeedbackDriftArtifact {
        let profile = self
            .cycle
            .profile_ref
            .resolve_builtin_research_profile()
            .expect("resolve F11 ResearchProfile");
        let policy = profile.spec.feedback_policy;
        let evaluation_window = self.coverage.evaluation_window.clone();
        let champion_baseline = self.coverage.champion_baseline.clone();
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
            coverage_artifact_id: self.coverage.artifact_id,
            coverage_artifact_uri: self.coverage_ref.uri.clone(),
            coverage_artifact_hash: self.coverage_ref.content_hash,
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

async fn persist_stage_artifact(
    store: &Arc<dyn ArtifactStore>,
    namespace: ArtifactNamespace,
    bytes: &[u8],
) -> ResearchJobArtifactRef {
    let content_hash = CanonicalDigest::content_hash_bytes(bytes);
    let uri = store
        .put(
            ArtifactKey::new(namespace, content_hash.hex(), "json")
                .expect("governance stage artifact key"),
            bytes,
        )
        .await
        .expect("persist governance stage artifact");
    assert_eq!(
        store
            .get(&uri)
            .await
            .expect("read governance stage artifact"),
        bytes
    );
    ResearchJobArtifactRef { uri, content_hash }
}

struct StageFixtureInput {
    stage: FeedbackStage,
    params: ResearchJobParams,
    result: ResearchJobResultRef,
    artifact: ResearchJobArtifactRef,
    event_sequence: i64,
}

impl StageFixtureInput {
    async fn record(
        self,
        cycles: &PgFeedbackCycleRepository,
        jobs: &PgResearchJobRepository,
        claim: &FeedbackCycleClaim,
    ) -> ResearchJobInfo {
        let identity =
            FeedbackStageJobIdentity::try_root(claim.cycle.feedback_cycle_id, self.stage)
                .expect("governance stage identity");
        let kind = self.params.kind();
        let job = NewResearchJob {
            job_id: identity.job_id(),
            feedback_cycle_id: None,
            feedback_stage: None,
            kind,
            status: ResearchJobStatus::Queued,
            model_spec_id: None,
            decision_policy_snapshot_id: None,
            params_json: self.params,
            requested_by: None,
            acting_role: RoleCode::new("system"),
            parent_job_id: None,
            recovery_attempt: 0,
            max_recovery_attempts: 3,
        }
        .try_bind_feedback(identity)
        .expect("bind governance stage job");
        match jobs
            .enqueue(job)
            .await
            .expect("enqueue governance stage job")
        {
            ResearchJobEnqueueOutcome::Inserted(_)
            | ResearchJobEnqueueOutcome::AlreadyPresent(_) => {}
        }
        let worker = WorkerId::from_v7();
        jobs.lease_next(
            &[kind],
            &worker,
            Utc::now() + Duration::seconds(JOB_LEASE_SECS),
        )
        .await
        .expect("lease governance stage job")
        .expect("queued governance stage job");
        let info = jobs
            .finalize(
                &identity.job_id(),
                &worker,
                ResearchJobFinalization::succeeded(
                    Some(self.result),
                    Some(self.artifact.clone()),
                    None,
                ),
            )
            .await
            .expect("finalize governance stage job");
        cycles
            .append_stage(
                claim.lease,
                NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                    feedback_cycle_id: claim.cycle.feedback_cycle_id,
                    event_sequence: self.event_sequence,
                    stage: self.stage,
                    event_kind: FeedbackStageEventKind::Succeeded,
                    trigger_family: None,
                    research_job_id: Some(identity.job_id()),
                    actor: None,
                    reason_code: None,
                    evidence_uri: Some(self.artifact.uri),
                    evidence_hash: Some(self.artifact.content_hash),
                    occurred_at: info.finished_at.expect("governance stage terminal time"),
                })
                .expect("seal governance stage event"),
            )
            .await
            .expect("append governance stage event");
        info
    }
}

async fn record_truth_attribution(
    store: &Arc<dyn ArtifactStore>,
    cycles: &PgFeedbackCycleRepository,
    jobs: &PgResearchJobRepository,
    claim: &FeedbackCycleClaim,
    family: &FeedbackCandidateFamily,
) {
    let cycle = &claim.cycle;
    let cutoff = cycle.label_cutoff;
    let truth_params = FeedbackTruthFreezeJobParams::try_new(
        cycle.feedback_cycle_id,
        cycle.idempotency_hash,
        cutoff,
    )
    .expect("freeze TruthFreeze params");
    let truth = FeedbackTruthFreezeArtifact::try_new(
        &truth_params,
        ResolutionProjectionBarrier {
            cutoff,
            unresolved_count: 0,
            mapping_blocked_count: 0,
            quarantined_count: 0,
            excluded_count: 0,
            oldest_unresolved_at: None,
            terminal_through: cutoff,
        },
        ExecutionAttemptBarrier {
            cutoff,
            eligible_unsealed_count: 0,
            sealed_through: cutoff,
        },
        ExecutionRollupBarrier {
            cutoff,
            eligible_unsealed_count: 0,
            sealed_through: cutoff,
        },
    )
    .expect("seal complete TruthFreeze artifact");
    let truth_bytes = FeedbackGovernanceCodec::encode_truth(&truth)
        .expect("encode complete TruthFreeze artifact");
    let truth_ref =
        persist_stage_artifact(store, ArtifactNamespace::FeedbackTruth, &truth_bytes).await;
    StageFixtureInput {
        stage: FeedbackStage::TruthFreeze,
        params: ResearchJobParams::FeedbackTruthFreeze(truth_params.clone()),
        result: ResearchJobResultRef {
            kind: ResearchJobResultKind::FeedbackTruthFreezeArtifact,
            id: truth_params.artifact_id.as_uuid(),
        },
        artifact: truth_ref.clone(),
        event_sequence: 2,
    }
    .record(cycles, jobs, claim)
    .await;

    let coverage = coverage_artifact(
        cycle,
        family
            .shared_evaluation()
            .source_lineage
            .capability_registry_hashes
            .clone(),
        CoverageScenario::Advance,
    );
    let coverage_ref = persist_coverage(store, &coverage).await;
    let coverage_params = FeedbackCoverageJobParams {
        feedback_cycle_id: cycle.feedback_cycle_id,
        cycle_idempotency_hash: cycle.idempotency_hash,
        artifact_id: coverage.artifact_id,
    };
    StageFixtureInput {
        stage: FeedbackStage::Coverage,
        params: ResearchJobParams::FeedbackCoverage(coverage_params.clone()),
        result: ResearchJobResultRef {
            kind: ResearchJobResultKind::FeedbackCoverageArtifact,
            id: coverage_params.artifact_id.as_uuid(),
        },
        artifact: coverage_ref,
        event_sequence: 3,
    }
    .record(cycles, jobs, claim)
    .await;

    let attribution_params = FeedbackAttributionJobParams::try_new(
        cycle.feedback_cycle_id,
        cycle.idempotency_hash,
        cutoff,
        cycles
            .database_time()
            .await
            .expect("load attribution-manifest database time"),
        coverage.evaluation_window,
        truth_ref,
    )
    .expect("freeze AttributionManifest params");
    let attribution =
        FeedbackAttributionManifest::try_new(&attribution_params, Vec::new(), Vec::new())
            .expect("seal PIT-safe AttributionManifest artifact");
    let attribution_bytes = FeedbackGovernanceCodec::encode_attribution(&attribution)
        .expect("encode AttributionManifest artifact");
    let attribution_ref = persist_stage_artifact(
        store,
        ArtifactNamespace::FeedbackAttribution,
        &attribution_bytes,
    )
    .await;
    StageFixtureInput {
        stage: FeedbackStage::Attribution,
        params: ResearchJobParams::FeedbackAttribution(attribution_params.clone()),
        result: ResearchJobResultRef {
            kind: ResearchJobResultKind::FeedbackAttributionManifest,
            id: attribution_params.artifact_id.as_uuid(),
        },
        artifact: attribution_ref,
        event_sequence: 4,
    }
    .record(cycles, jobs, claim)
    .await;
}

fn candidate_gate_report(
    candidate_model_version_id: ModelVersionId,
    evaluated_at: DateTime<Utc>,
) -> QualityGateReport {
    let pass = |gate, class| GateOutcome {
        gate,
        class,
        status: GateStatus::Pass,
        observed: "fixture_pass".to_owned(),
        threshold: "governed".to_owned(),
        detail: "promotion fixture carries a complete passing scorecard".to_owned(),
    };
    let not_applicable = |gate, class| GateOutcome {
        gate,
        class,
        status: GateStatus::NotApplicable,
        observed: "n/a".to_owned(),
        threshold: "n/a".to_owned(),
        detail: "gate does not apply to Candidate intent".to_owned(),
    };
    QualityGateReport::try_new(QualityGateReportInput {
        subject: GateSubject::ModelVersion(candidate_model_version_id),
        intent: GateIntent::Candidate,
        evaluated_at,
        gates: vec![
            pass(GateId::SampleCount, GateClass::Hard),
            pass(GateId::LabelCoverage, GateClass::Hard),
            pass(GateId::MaterializationCoverage, GateClass::Hard),
            pass(GateId::NoPitLeakage, GateClass::Hard),
            pass(GateId::ValidationEvidenceRequired, GateClass::Hard),
            pass(GateId::MaxDrawdown, GateClass::Hard),
            pass(GateId::TurnoverBudget, GateClass::Hard),
            pass(GateId::TailLossBudget, GateClass::Hard),
            pass(GateId::HitRate, GateClass::Soft),
            pass(GateId::CategoryConcentration, GateClass::Soft),
            pass(GateId::CpcvRequired, GateClass::Hard),
            pass(GateId::CpcvPathCount, GateClass::Hard),
            pass(GateId::RankIc, GateClass::Hard),
            pass(GateId::DeflatedSharpe, GateClass::Hard),
            pass(GateId::Pbo, GateClass::Hard),
            pass(GateId::MinTrackRecordLength, GateClass::Soft),
            not_applicable(GateId::LiquidityExitFeasible, GateClass::Hard),
            not_applicable(GateId::ShadowDecisionOverlap, GateClass::Hard),
            not_applicable(GateId::CalibrationRequired, GateClass::Hard),
            pass(GateId::ExplainabilityRequired, GateClass::Hard),
        ],
    })
    .expect("seal complete Candidate quality-gate report")
}

struct ValidationGateFixture {
    reference: FeedbackValidationArtifactRef,
    path_set_id: BacktestPathSetId,
    path_set_hash: ContentHash,
}

struct ValidationGateRecorder<'a> {
    db: &'a DatabaseConnection,
    store: &'a Arc<dyn ArtifactStore>,
    cycles: &'a PgFeedbackCycleRepository,
    jobs: &'a PgResearchJobRepository,
    claim: &'a FeedbackCycleClaim,
    models: &'a ShadowModels,
    family: &'a FeedbackCandidateFamily,
    cpcv_event_sequence: i64,
    validation_event_sequence: i64,
}

struct DatasetGateFixture {
    reference: FeedbackLearningStageArtifactRef,
    training_dataset_id: TrainingDatasetId,
    calibration_dataset_id: TrainingDatasetId,
}

struct TrainingGateFixture {
    reference: FeedbackLearningStageArtifactRef,
    source_model_version_id: ModelVersionId,
    training_input_hash: ContentHash,
}

impl ValidationGateRecorder<'_> {
    fn fold_artifacts() -> CpcvFoldArtifacts {
        CpcvFoldArtifacts::try_new(vec![
            CpcvFoldArtifact {
                validation_regime: CpcvFoldValidationRegime::PortfolioEconomics,
                identity: CpcvEstimatorIdentity::Validation {
                    combination_index: 0,
                    test_partitions_hash: content_hash('b'),
                    test_partition_count: 1,
                    test_groups_hash: content_hash('c'),
                    test_group_count: 1,
                },
                training_groups_hash: content_hash('b'),
                training_group_count: 2,
                calibration_fit_groups_hash: content_hash('4'),
                calibration_fit_group_count: 1,
                scenario_fit_groups_hash: content_hash('0'),
                scenario_fit_group_count: 1,
                model_artifact_hash: content_hash('c'),
                serving_contract_hash: content_hash('d'),
                model_payload_hash: content_hash('e'),
                calibration_function_hash: content_hash('7'),
                scenario_economic_function_hash: content_hash('8'),
                calibration_artifact_hash: content_hash('5'),
                scenario_model_hash: content_hash('6'),
            },
            CpcvFoldArtifact {
                validation_regime: CpcvFoldValidationRegime::PortfolioEconomics,
                identity: CpcvEstimatorIdentity::TrialPathValidation {
                    trial_id: 0,
                    path_index: 0,
                    combination_index: 0,
                    test_partitions_hash: content_hash('b'),
                    test_partition_count: 1,
                    test_groups_hash: content_hash('c'),
                    test_group_count: 1,
                },
                training_groups_hash: content_hash('f'),
                training_group_count: 3,
                calibration_fit_groups_hash: content_hash('4'),
                calibration_fit_group_count: 1,
                scenario_fit_groups_hash: content_hash('0'),
                scenario_fit_group_count: 1,
                model_artifact_hash: content_hash('1'),
                serving_contract_hash: content_hash('2'),
                model_payload_hash: content_hash('3'),
                calibration_function_hash: content_hash('7'),
                scenario_economic_function_hash: content_hash('8'),
                calibration_artifact_hash: content_hash('5'),
                scenario_model_hash: content_hash('6'),
            },
            CpcvFoldArtifact {
                validation_regime: CpcvFoldValidationRegime::PortfolioEconomics,
                identity: CpcvEstimatorIdentity::TrialPathValidation {
                    trial_id: 1,
                    path_index: 0,
                    combination_index: 0,
                    test_partitions_hash: content_hash('b'),
                    test_partition_count: 1,
                    test_groups_hash: content_hash('c'),
                    test_group_count: 1,
                },
                training_groups_hash: content_hash('e'),
                training_group_count: 3,
                calibration_fit_groups_hash: content_hash('4'),
                calibration_fit_group_count: 1,
                scenario_fit_groups_hash: content_hash('0'),
                scenario_fit_group_count: 1,
                model_artifact_hash: content_hash('2'),
                serving_contract_hash: content_hash('3'),
                model_payload_hash: content_hash('4'),
                calibration_function_hash: content_hash('9'),
                scenario_economic_function_hash: content_hash('a'),
                calibration_artifact_hash: content_hash('5'),
                scenario_model_hash: content_hash('6'),
            },
        ])
        .expect("build promotion CPCV fold artifacts")
    }

    fn path_series(
        window_start: DateTime<Utc>,
        bucket_secs: i64,
        observation_count: i64,
    ) -> (Vec<DateTime<Utc>>, Vec<Decimal>) {
        let decision_times = (0..observation_count)
            .map(|bucket| {
                window_start
                    + Duration::seconds(
                        bucket_secs
                            .checked_mul(bucket)
                            .expect("promotion CPCV decision offset fits i64"),
                    )
                    + Duration::seconds(1)
            })
            .collect::<Vec<_>>();
        let group_returns = (0..observation_count)
            .map(|ordinal| match ordinal.rem_euclid(8) {
                0 | 3 => dec!(0.012),
                1 | 6 => dec!(-0.005),
                _ => dec!(0.004),
            })
            .collect::<Vec<_>>();
        (decision_times, group_returns)
    }

    async fn persist_path_set(
        &self,
        path_set_id: BacktestPathSetId,
        model_run_id: ModelRunId,
    ) -> ContentHash {
        let model = &self.models.candidate;
        let bindings = model
            .verified_serving_contract()
            .expect("verify promotion CPCV serving contract")
            .bindings();
        let training_dataset_id = model
            .training_dataset_id
            .expect("promotion candidate has a training dataset");
        let observation_count = 96_i64;
        let bucket_secs = model.model_spec_prediction_horizon_secs;
        assert!(bucket_secs > 0, "promotion candidate horizon is positive");
        let window_end = self.claim.cycle.label_cutoff - Duration::seconds(bucket_secs);
        let window_start = window_end
            - Duration::seconds(
                bucket_secs
                    .checked_mul(observation_count)
                    .expect("promotion CPCV window fits i64"),
            );
        let (decision_times, group_returns) =
            Self::path_series(window_start, bucket_secs, observation_count);
        let challenger_returns = group_returns
            .iter()
            .map(|value| *value - dec!(0.001))
            .collect::<Vec<_>>();
        let (trial_grid, cscv_selection_evidence) = cscv_selection_fixture(
            "feedback-decision-path-set",
            &decision_times,
            &[group_returns.clone(), challenger_returns],
            4,
        );
        let dsr_conservative_independent_trial_count = i64::from(
            cscv_selection_evidence
                .trial_dependence
                .conservative_independent_trial_count(),
        );
        let input_hash = content_hash('b');
        let ModelVersionDerivation::ReturnCalibration {
            parent_model_version_id,
            calibration_artifact_id,
        } = model
            .verified_derivation()
            .expect("verify promotion candidate derivation")
        else {
            panic!("promotion candidate must be a calibrated child")
        };
        let calibration = bindings
            .model
            .calibration
            .as_ref()
            .expect("promotion candidate has a calibration binding");
        assert_eq!(calibration.artifact_id, calibration_artifact_id);
        let parent = PgModelRegistryRepository::new(self.db.clone())
            .find_model_version(&parent_model_version_id)
            .await
            .expect("load promotion candidate parent")
            .expect("promotion candidate parent exists");
        PgModelRunRepository::new(self.db.clone())
            .start_exact(NewModelRun {
                model_run_id,
                run_kind: ModelRunKind::Cpcv,
                model_version_id: Some(model.model_version_id),
                decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
                market_selection_id: None,
                window_start,
                window_end,
                input_hash,
            })
            .await
            .expect("start exact promotion CPCV model run");
        let path_set = NewBacktestPathSet::try_seal(NewBacktestPathSetInput {
            path_set_id,
            model_version_id: model.model_version_id,
            model_run_id,
            training_dataset_id,
            decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
            window_start,
            window_end,
            subject: CpcvPathSetSubject::new(
                model.artifact_hash,
                model.serving_contract_hash,
                bindings.transform.training_dataset_hash,
                bindings.dataset.manifest_hash,
                bindings.dataset.artifact_bytes_hash,
                bindings.policy_snapshot.snapshot_hash,
            ),
            methodology: CpcvMethodologyBinding::new(
                content_hash('7'),
                content_hash('8'),
                content_hash('9'),
                CpcvFoldCalibrationPolicy::CalibratedSubjectParentHeuristic {
                    calibration_artifact_id,
                    calibration_hash: calibration.content_hash,
                    parent_model_version_id,
                    parent_artifact_hash: parent.artifact_hash,
                    parent_serving_contract_hash: parent.serving_contract_hash,
                    parent_return_model_hash: content_hash('a'),
                },
                CpcvTrialPathBinding::try_new(0, vec![0]).expect("build promotion CPCV trial path"),
                trial_grid,
            ),
            fold_artifacts: Self::fold_artifacts(),
            path_count: 1,
            combination_count: 1,
            median_rank_ic: dec!(0.12),
            sharpe_distribution: SharpeDistribution {
                min: dec!(0.1),
                p25: dec!(0.4),
                median: dec!(0.8),
                p75: dec!(1.1),
                max: dec!(1.5),
                median_max_drawdown: None,
                median_tail_loss: None,
                median_turnover: None,
                baseline_uplift: None,
            },
            paths: vec![BacktestPath {
                path_index: 0,
                decision_times,
                scenario_residuals: group_returns.iter().copied().map(Some).collect(),
                group_returns,
                sharpe: dec!(0.8),
                rank_ic: dec!(0.12),
                max_drawdown: dec!(0.005),
                tail_loss: dec!(-0.005),
                turnover: None,
            }]
            .into(),
            deflated_sharpe: dec!(0.96),
            dsr_benchmark_sharpe: dec!(0.4),
            pbo: cscv_selection_evidence.pbo,
            cscv_selection_evidence,
            min_track_record_length_secs: Some(observation_count * bucket_secs),
            dsr_conservative_independent_trial_count,
            trial_grid_count: 2,
            coord_search_effective_n: 2,
        })
        .expect("seal promotion CPCV path set");
        PgBacktestPathSetRepository::new(self.db.clone())
            .commit_cpcv(CpcvPathSetCommit {
                path_set,
                input_hash,
            })
            .await
            .expect("persist exact promotion CPCV path set")
            .path_set_hash
    }

    async fn record_dataset(&self, family_hash: ContentHash) -> DatasetGateFixture {
        let params = dataset_params(&self.claim.cycle, self.family, family_hash);
        let artifact = FeedbackLearningStageArtifact::try_new(
            self.claim.cycle.feedback_cycle_id,
            self.claim.cycle.idempotency_hash,
            family_hash,
            params
                .input_hash()
                .expect("promotion DatasetSeal input hash"),
            None,
            FeedbackLearningStageResults::DatasetSeal(params.dataset_results()),
        )
        .expect("seal promotion DatasetSeal artifact");
        let stored = persist_artifact(self.store, &artifact).await;
        let info = StageFixtureInput {
            stage: FeedbackStage::DatasetSeal,
            params: ResearchJobParams::FeedbackDatasetSeal(params.clone()),
            result: ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackLearningStageArtifact,
                id: params.artifact_id.as_uuid(),
            },
            artifact: stored.clone(),
            event_sequence: self
                .cpcv_event_sequence
                .checked_sub(3)
                .expect("CPCV sequence leaves room for DatasetSeal"),
        }
        .record(self.cycles, self.jobs, self.claim)
        .await;
        DatasetGateFixture {
            reference: artifact
                .reference(info.job_id, stored)
                .expect("freeze promotion DatasetSeal reference"),
            training_dataset_id: candidate_dataset(&params, DatasetPurpose::Training),
            calibration_dataset_id: candidate_dataset(&params, DatasetPurpose::Calibration),
        }
    }

    async fn record_training(
        &self,
        recipe_hash: ContentHash,
        family_hash: ContentHash,
        dataset: &DatasetGateFixture,
    ) -> TrainingGateFixture {
        let source_model_version_id = ModelVersionId::from_v7();
        let model_run_id = ModelRunId::from_feedback_stage(
            self.claim.cycle.feedback_cycle_id,
            FeedbackStage::Training,
            recipe_hash,
        );
        let params = FeedbackTrainingJobParams::try_new(
            self.claim.cycle.feedback_cycle_id,
            self.claim.cycle.idempotency_hash,
            family_hash,
            dataset.reference.clone(),
            vec![FeedbackTrainingCommand {
                candidate_recipe_hash: recipe_hash,
                resource_budget: self
                    .family
                    .candidate(recipe_hash)
                    .expect("promotion recipe exists")
                    .resource_budget(),
                params: ModelTrainJobParams {
                    model_version_id: source_model_version_id,
                    model_run_id,
                    request: TrainModelRequest {
                        training_dataset_id: dataset.training_dataset_id,
                        reason: "promotion fixture exact Training predecessor".to_owned(),
                    },
                },
            }],
        )
        .expect("freeze promotion Training params");
        let training_input_hash = content_hash('8');
        let artifact = FeedbackLearningStageArtifact::try_new(
            self.claim.cycle.feedback_cycle_id,
            self.claim.cycle.idempotency_hash,
            family_hash,
            params.input_hash().expect("promotion Training input hash"),
            Some(params.previous.clone()),
            FeedbackLearningStageResults::Training(vec![FeedbackTrainingStageResult {
                candidate_recipe_hash: recipe_hash,
                model_version_id: source_model_version_id,
                model_run_id,
                training_dataset_id: dataset.training_dataset_id,
                model_artifact_hash: content_hash('6'),
                serving_contract_hash: content_hash('7'),
                training_input_hash,
            }]),
        )
        .expect("seal promotion Training artifact");
        let stored = persist_artifact(self.store, &artifact).await;
        let info = StageFixtureInput {
            stage: FeedbackStage::Training,
            params: ResearchJobParams::FeedbackTraining(params.clone()),
            result: ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackLearningStageArtifact,
                id: params.artifact_id.as_uuid(),
            },
            artifact: stored.clone(),
            event_sequence: self
                .cpcv_event_sequence
                .checked_sub(2)
                .expect("CPCV sequence leaves room for Training"),
        }
        .record(self.cycles, self.jobs, self.claim)
        .await;
        TrainingGateFixture {
            reference: artifact
                .reference(info.job_id, stored)
                .expect("freeze promotion Training reference"),
            source_model_version_id,
            training_input_hash,
        }
    }

    async fn record_calibration(
        &self,
        recipe_hash: ContentHash,
        family_hash: ContentHash,
        dataset: &DatasetGateFixture,
        training: &TrainingGateFixture,
    ) -> FeedbackLearningStageArtifactRef {
        let model_run_id = ModelRunId::from_feedback_stage(
            self.claim.cycle.feedback_cycle_id,
            FeedbackStage::Calibration,
            recipe_hash,
        );
        let decision_policy_snapshot_id = self
            .family
            .candidate(recipe_hash)
            .expect("promotion recipe exists")
            .decision_policy_snapshot_id();
        let params = FeedbackCalibrationJobParams::try_new(
            self.claim.cycle.feedback_cycle_id,
            self.claim.cycle.idempotency_hash,
            family_hash,
            training.reference.clone(),
            vec![FeedbackCalibrationCommand {
                candidate_recipe_hash: recipe_hash,
                resource_budget: self
                    .family
                    .candidate(recipe_hash)
                    .expect("promotion recipe exists")
                    .resource_budget(),
                params: ModelCalibrationFitJobParams {
                    model_run_id,
                    request: FitModelCalibratorRequest {
                        model_version_id: training.source_model_version_id,
                        calibration_dataset_id: dataset.calibration_dataset_id,
                        method: CalibrationMethod::Platt,
                        reason: "promotion fixture exact Calibration predecessor".to_owned(),
                    },
                    decision_policy_snapshot_id,
                    downside_source: DownsideSource::MfeMae,
                    actor: GovernanceActor::system(),
                },
            }],
        )
        .expect("freeze promotion Calibration params");
        let artifact = FeedbackLearningStageArtifact::try_new(
            self.claim.cycle.feedback_cycle_id,
            self.claim.cycle.idempotency_hash,
            family_hash,
            params
                .input_hash()
                .expect("promotion Calibration input hash"),
            Some(params.previous.clone()),
            FeedbackLearningStageResults::Calibration(vec![
                FeedbackCalibrationStageResult::Calibrated {
                    candidate_recipe_hash: recipe_hash,
                    source_model_version_id: training.source_model_version_id,
                    model_run_id,
                    calibration_dataset_id: dataset.calibration_dataset_id,
                    method: CalibrationMethod::Platt,
                    calibration_artifact_id: CalibrationArtifactId::from_v7(),
                    calibration_artifact_hash: content_hash('9'),
                    calibrated_model_version_id: self.models.candidate.model_version_id,
                    calibrated_model_artifact_hash: self.models.candidate.artifact_hash,
                    calibrated_serving_contract_hash: self.models.candidate.serving_contract_hash,
                    training_input_hash: training.training_input_hash,
                    sample_count: 100,
                },
            ]),
        )
        .expect("seal promotion Calibration artifact");
        let stored = persist_artifact(self.store, &artifact).await;
        let info = StageFixtureInput {
            stage: FeedbackStage::Calibration,
            params: ResearchJobParams::FeedbackCalibration(params.clone()),
            result: ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackLearningStageArtifact,
                id: params.artifact_id.as_uuid(),
            },
            artifact: stored.clone(),
            event_sequence: self
                .cpcv_event_sequence
                .checked_sub(1)
                .expect("CPCV sequence leaves room for Calibration"),
        }
        .record(self.cycles, self.jobs, self.claim)
        .await;
        artifact
            .reference(info.job_id, stored)
            .expect("freeze promotion Calibration reference")
    }

    async fn record(self) -> ValidationGateFixture {
        let recipe_hash = self.family.candidates()[0].candidate_recipe_hash();
        let resource_budget = self.family.candidates()[0].resource_budget();
        let cpcv_spec = self.family.candidates()[0].cpcv_spec().clone();
        let family_hash = self.family.candidate_family_hash();
        let dataset = self.record_dataset(family_hash).await;
        let training = self
            .record_training(recipe_hash, family_hash, &dataset)
            .await;
        let calibration_ref = self
            .record_calibration(recipe_hash, family_hash, &dataset, &training)
            .await;
        let path_set_id = BacktestPathSetId::from_v7();
        let model_run_id = ModelRunId::from_feedback_stage(
            self.claim.cycle.feedback_cycle_id,
            FeedbackStage::Cpcv,
            recipe_hash,
        );
        let path_set_hash = self.persist_path_set(path_set_id, model_run_id).await;
        let Self {
            db: _,
            store,
            cycles,
            jobs,
            claim,
            models,
            family: _,
            cpcv_event_sequence,
            validation_event_sequence,
        } = self;
        let cycle = &claim.cycle;
        let candidate_training_dataset_id = dataset.training_dataset_id;

        let cpcv_params = FeedbackCpcvJobParams::try_new(
            cycle.feedback_cycle_id,
            cycle.idempotency_hash,
            family_hash,
            calibration_ref,
            vec![FeedbackCpcvCommand {
                candidate_recipe_hash: recipe_hash,
                resource_budget,
                cpcv_spec,
                params: CpcvBacktestJobParams {
                    model_version_id: models.candidate.model_version_id,
                    model_run_id,
                    request: RunCpcvBacktestRequest {
                        training_dataset_id: candidate_training_dataset_id,
                        decision_policy_snapshot_id: cycle.decision_policy_snapshot_id,
                        reason: "promotion fixture exact CPCV predecessor".to_owned(),
                        path_set_id: Some(path_set_id),
                    },
                },
            }],
        )
        .expect("freeze promotion CPCV params");
        let cpcv = FeedbackLearningStageArtifact::try_new(
            cycle.feedback_cycle_id,
            cycle.idempotency_hash,
            family_hash,
            cpcv_params.input_hash().expect("promotion CPCV input hash"),
            Some(cpcv_params.previous.clone()),
            FeedbackLearningStageResults::Cpcv(vec![FeedbackCpcvStageResult::Evaluated {
                candidate_recipe_hash: recipe_hash,
                model_version_id: models.candidate.model_version_id,
                training_dataset_id: candidate_training_dataset_id,
                path_set_id,
                model_run_id: cpcv_params.commands[0].params.model_run_id,
                path_set_hash,
            }]),
        )
        .expect("seal promotion CPCV artifact");
        let cpcv_bytes =
            FeedbackLearningStageCodec::encode(&cpcv).expect("encode promotion CPCV artifact");
        let cpcv_artifact =
            persist_stage_artifact(store, ArtifactNamespace::FeedbackLearning, &cpcv_bytes).await;
        let cpcv_info = StageFixtureInput {
            stage: FeedbackStage::Cpcv,
            params: ResearchJobParams::FeedbackCpcv(cpcv_params.clone()),
            result: ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackLearningStageArtifact,
                id: cpcv_params.artifact_id.as_uuid(),
            },
            artifact: cpcv_artifact.clone(),
            event_sequence: cpcv_event_sequence,
        }
        .record(cycles, jobs, claim)
        .await;
        let cpcv_ref = cpcv
            .reference(cpcv_info.job_id, cpcv_artifact)
            .expect("freeze promotion CPCV reference");
        let evaluated_at = cycles
            .database_time()
            .await
            .expect("load Validation database time");
        let params = FeedbackValidationJobParams::try_new(
            cycle.feedback_cycle_id,
            cycle.idempotency_hash,
            evaluated_at,
            cpcv_ref.clone(),
        )
        .expect("freeze Validation params");
        let validation = FeedbackValidationArtifact::try_new(
            &params,
            vec![FeedbackCandidateValidation {
                candidate_recipe_hash: recipe_hash,
                model_version_id: models.candidate.model_version_id,
                trial_outcome: FeedbackValidationTrialOutcome::CpcvEvaluated,
                quality_gate_report: candidate_gate_report(
                    models.candidate.model_version_id,
                    evaluated_at,
                ),
            }],
        )
        .expect("seal promotion Validation artifact");
        let validation_bytes = FeedbackGovernanceCodec::encode_validation(&validation)
            .expect("encode promotion Validation artifact");
        let validation_ref = persist_stage_artifact(
            store,
            ArtifactNamespace::FeedbackValidation,
            &validation_bytes,
        )
        .await;
        let info = StageFixtureInput {
            stage: FeedbackStage::Validation,
            params: ResearchJobParams::FeedbackValidation(params.clone()),
            result: ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackValidationArtifact,
                id: params.artifact_id.as_uuid(),
            },
            artifact: validation_ref.clone(),
            event_sequence: validation_event_sequence,
        }
        .record(cycles, jobs, claim)
        .await;
        ValidationGateFixture {
            reference: FeedbackValidationArtifactRef {
                feedback_cycle_id: cycle.feedback_cycle_id,
                job_id: info.job_id,
                artifact_id: params.artifact_id,
                input_hash: params.input_hash().expect("Validation input hash"),
                cpcv: cpcv_ref,
                artifact: validation_ref,
            },
            path_set_id,
            path_set_hash,
        }
    }
}

async fn record_drift(
    cycles: &PgFeedbackCycleRepository,
    jobs: &PgResearchJobRepository,
    store: &Arc<dyn ArtifactStore>,
    cycle: &FeedbackCycleInfo,
    lease: FeedbackCycleLeaseGuard,
    event_sequence: i64,
) {
    let coverage_identity =
        FeedbackStageJobIdentity::try_root(cycle.feedback_cycle_id, FeedbackStage::Coverage)
            .expect("F11 Coverage identity");
    let coverage_job = jobs
        .find_by_id(&coverage_identity.job_id())
        .await
        .expect("read F11 Coverage job")
        .expect("F11 Coverage job exists");
    let coverage_ref = coverage_job
        .result_artifact()
        .expect("F11 Coverage job has a terminal artifact");
    let coverage_bytes = store
        .get(&coverage_ref.uri)
        .await
        .expect("read F11 Coverage artifact");
    let coverage =
        FeedbackCoverageCodec::decode(&coverage_bytes).expect("decode F11 Coverage artifact");
    let artifact = AdvancingDriftSeed {
        cycle,
        coverage: &coverage,
        coverage_ref: &coverage_ref,
    }
    .seal();
    let artifact_ref = persist_drift(store, &artifact).await;
    let identity =
        FeedbackStageJobIdentity::try_root(cycle.feedback_cycle_id, FeedbackStage::Drift)
            .expect("F11 drift identity");
    let params = FeedbackDriftJobParams {
        feedback_cycle_id: cycle.feedback_cycle_id,
        cycle_idempotency_hash: cycle.idempotency_hash,
        artifact_id: artifact.artifact_id,
        coverage_job_id: coverage_job.job_id,
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
        model_spec_id: Some(cycle.champion_model_spec_id),
        decision_policy_snapshot_id: Some(cycle.decision_policy_snapshot_id),
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
                event_sequence,
                stage: FeedbackStage::Drift,
                event_kind: FeedbackStageEventKind::Succeeded,
                trigger_family: None,
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
    event_sequence: i64,
) {
    let recipes = recipe_stage(cycles, jobs, Arc::clone(store));
    let shadow_bindings =
        shadow_binding_stage(db, cycles, jobs, Arc::clone(store), Arc::clone(&recipes));
    let shadow_stage = FeedbackShadowStageAdapter::try_new(FeedbackShadowStageDeps {
        cycles: Arc::clone(cycles) as Arc<dyn FeedbackCycleRepository>,
        jobs: Arc::clone(jobs) as Arc<dyn ResearchJobRepository>,
        artifacts: Arc::clone(store),
        serving_generations: Arc::clone(generations),
        recipes,
        shadow_bindings,
        max_recovery_attempts: 3,
    })
    .expect("build F11 shadow stage");
    let shadow_identity =
        FeedbackStageJobIdentity::try_root(claim.cycle.feedback_cycle_id, FeedbackStage::Shadow)
            .expect("F11 shadow identity");
    let shadow_preparation = shadow_stage
        .prepare_shadow(&claim.cycle, claim.lease, shadow_identity)
        .await
        .expect("prepare F11 shadow");
    let shadow_job = match shadow_preparation {
        FeedbackStagePreparation::Ready(job) => job,
        FeedbackStagePreparation::Deferred {
            resume_after,
            reason_code,
        } => {
            assert_eq!(reason_code, "feedback_shadow_window_pending");
            let database_time = cycles
                .database_time()
                .await
                .expect("read F11 shadow-window database time");
            let remaining = resume_after
                .signed_duration_since(database_time)
                .to_std()
                .unwrap_or(StdDuration::ZERO);
            tokio::time::sleep(remaining + StdDuration::from_millis(10)).await;
            let preparation = shadow_stage
                .prepare_shadow(&claim.cycle, claim.lease, shadow_identity)
                .await
                .expect("prepare F11 shadow at its exact mature boundary");
            let FeedbackStagePreparation::Ready(job) = preparation else {
                panic!("F11 shadow remained deferred after its governed resume boundary");
            };
            job
        }
    };
    let shadow_params = match &shadow_job.params_json {
        ResearchJobParams::FeedbackShadow(params) => params.as_ref().clone(),
        _ => panic!("F11 shadow stage emitted another kind"),
    };
    match jobs.enqueue(*shadow_job).await.expect("enqueue F11 shadow") {
        ResearchJobEnqueueOutcome::Inserted(_) | ResearchJobEnqueueOutcome::AlreadyPresent(_) => {}
    }
    let shadow_worker = WorkerId::from_v7();
    jobs.lease_next(
        &[ResearchJobKind::FeedbackShadow],
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
                    kind: ResearchJobResultKind::FeedbackShadowArtifact,
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
                event_sequence,
                stage: FeedbackStage::Shadow,
                event_kind: FeedbackStageEventKind::Succeeded,
                trigger_family: None,
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
        recipes: recipe_stage(cycles, jobs, Arc::clone(store)),
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
    assert_eq!(
        decision_artifact.outcome().decision(),
        expected_decision,
        "unexpected decision evidence: {:?}",
        decision_artifact.outcome()
    );
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
        artifacts: Arc::clone(&tampered_store),
        recipes: recipe_stage(cycles, jobs, tampered_store),
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
    event_sequence: i64,
) {
    cycles
        .append_stage(
            claim.lease,
            NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                feedback_cycle_id: claim.cycle.feedback_cycle_id,
                event_sequence,
                stage: FeedbackStage::Decision,
                event_kind: FeedbackStageEventKind::Succeeded,
                trigger_family: None,
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
        route_champion_before: before_model
            .champion(input.route)
            .ok()
            .map(|binding| binding.model_version_id.to_string()),
        route_champion_after: after_model
            .champion(input.route)
            .ok()
            .map(|binding| binding.model_version_id.to_string()),
        route_shadow_before: before_model
            .route_binding(input.route)
            .ok()
            .and_then(|binding| binding.shadow.as_ref())
            .map(|binding| binding.model_version_id.to_string()),
        route_shadow_after: after_model
            .route_binding(input.route)
            .ok()
            .and_then(|binding| binding.shadow.as_ref())
            .map(|binding| binding.model_version_id.to_string()),
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
            recipes: recipe_stage(&cycles, &jobs, Arc::clone(&self.artifacts)),
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

struct TerminalPipeline {
    models: ShadowModels,
    serving: ActivatedServing,
    claim: FeedbackCycleClaim,
    cycles: Arc<PgFeedbackCycleRepository>,
    jobs: Arc<PgResearchJobRepository>,
    comparison: FeedbackComparisonArtifactRef,
}

impl TerminalPipeline {
    async fn prepare(
        db: &DatabaseConnection,
        store: &Arc<dyn ArtifactStore>,
        comparison_uplift: Decimal,
    ) -> Self {
        let models = Box::pin(build_crypto_models(db, store)).await;
        let serving = activate_crypto_generation(db, store, &models).await;
        let (schema, claim) = record_crypto_cycle(db, &models).await;
        let cycles = Arc::new(PgFeedbackCycleRepository::new(db.clone()));
        let jobs = Arc::new(PgResearchJobRepository::new(db.clone()));
        record_truth_attribution(
            store,
            cycles.as_ref(),
            jobs.as_ref(),
            &claim,
            &schema.candidate_family,
        )
        .await;
        record_drift(
            cycles.as_ref(),
            jobs.as_ref(),
            store,
            &claim.cycle,
            claim.lease,
            5,
        )
        .await;
        persist_recipe_plan_fixture(
            cycles.as_ref(),
            jobs.as_ref(),
            store,
            claim.lease,
            &claim.cycle,
            &schema.candidate_family,
            6,
        )
        .await;
        let validation = ValidationGateRecorder {
            db,
            store,
            cycles: cycles.as_ref(),
            jobs: jobs.as_ref(),
            claim: &claim,
            models: &models,
            family: &schema.candidate_family,
            cpcv_event_sequence: 10,
            validation_event_sequence: 11,
        }
        .record()
        .await;
        let params = comparison_params_with_validation(
            db,
            &schema,
            &claim,
            &models,
            validation.reference,
            validation.path_set_id,
            validation.path_set_hash,
        )
        .await;
        let comparison = record_comparison(db, store, &claim, params, comparison_uplift, 12).await;
        Self {
            models,
            serving,
            claim,
            cycles,
            jobs,
            comparison,
        }
    }
}

struct RejectedReplayEvidence {
    artifact: DecisionArtifactEvidence,
    restart: RestartReadBackEvidence,
    counts_after_exact_replay: RowCountSnapshot,
}

struct RejectedReplayProbe<'a> {
    db: &'a DatabaseConnection,
    store: &'a Arc<dyn ArtifactStore>,
    cycles: &'a Arc<PgFeedbackCycleRepository>,
    jobs: &'a Arc<PgResearchJobRepository>,
    claim: &'a FeedbackCycleClaim,
    comparison: &'a FeedbackComparisonArtifactRef,
    job: &'a ResearchJobInfo,
    success: &'a FeedbackStageSuccess,
    invariant: &'a InvariantProbe,
    expected_after: &'a InvariantSnapshot,
    expected_timeline: &'a [TimelineEventEvidence],
    models: &'a ShadowModels,
}

impl RejectedReplayProbe<'_> {
    async fn verify(self) -> RejectedReplayEvidence {
        let bytes = self
            .store
            .get(&self.comparison.artifact.uri)
            .await
            .expect("read terminal Comparison artifact");
        let artifact =
            FeedbackComparisonCodec::decode(&bytes).expect("decode terminal Comparison artifact");
        let artifact_evidence = DecisionArtifactEvidence {
            artifact_id: artifact.artifact_id().to_string(),
            uri: self.comparison.artifact.uri.to_string(),
            bytes_hash: self.comparison.artifact.content_hash.to_string(),
            semantic_hash: artifact.artifact_hash().to_string(),
            decision_job_input_hash: Some(artifact.job_input_hash().to_string()),
            champion_serving_contract_hash: self.models.champion.serving_contract_hash.to_string(),
            candidate_serving_contract_hash: self
                .models
                .candidate
                .serving_contract_hash
                .to_string(),
        };
        let tampered_store: Arc<dyn ArtifactStore> = Arc::new(ReadTamperArtifactStoreFixture::new(
            Arc::clone(self.store),
            self.comparison.artifact.uri.clone(),
            b"{}".to_vec(),
        ));
        comparison_stage(self.db, self.cycles, self.jobs, tampered_store)
            .succeeded_comparison(&self.claim.cycle, self.job)
            .await
            .expect_err("terminal Comparison replay must reject tampered bytes");
        let fresh_cycles = Arc::new(PgFeedbackCycleRepository::new(self.db.clone()));
        let fresh_jobs = Arc::new(PgResearchJobRepository::new(self.db.clone()));
        let fresh_job = fresh_jobs
            .find_by_id(&self.comparison.job_id)
            .await
            .expect("restart read terminal Comparison job")
            .expect("restart terminal Comparison job exists");
        let fresh_success =
            comparison_stage(self.db, &fresh_cycles, &fresh_jobs, Arc::clone(self.store))
                .succeeded_comparison(&self.claim.cycle, &fresh_job)
                .await
                .expect("restart verify terminal Comparison");
        assert_eq!(&fresh_success, self.success);
        let restart_after = self.invariant.load().await;
        let restart_timeline = timeline_evidence(
            fresh_cycles
                .list_stage_events(&self.claim.cycle.feedback_cycle_id)
                .await
                .expect("restart read terminal Comparison timeline"),
        );
        let exact_match = wire_value(&restart_after) == wire_value(self.expected_after)
            && restart_timeline == self.expected_timeline
            && wire_value(&fresh_job) == wire_value(self.job);
        assert!(exact_match, "fresh owners read a different terminal graph");
        let restart_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot:w4-e04:comparison-restart-read-back",
            1,
            &json!({
                "invariant": restart_after,
                "timeline": restart_timeline,
                "comparison_job": fresh_job,
            }),
        )
        .expect("hash terminal Comparison restart read-back");
        RejectedReplayEvidence {
            artifact: artifact_evidence,
            restart: RestartReadBackEvidence {
                fresh_repository_owners: true,
                fresh_stage_adapter: true,
                exact_match,
                canonical_read_back_hash: restart_hash.to_string(),
            },
            counts_after_exact_replay: load_evidence_counts(self.db).await,
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
                route: BuyModelRoute::Crypto,
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
        if matches!(self, Self::RejectedComparison) {
            return Box::pin(self.run_rejected()).await;
        }
        assert_authority_boundary();
        let (pool, _container) = setup_pg().await;
        let db = pool.connection().clone();
        let artifact_root = ArtifactRoot::create();
        let store = artifact_root.store();
        let TerminalPipeline {
            models,
            serving,
            claim,
            cycles,
            jobs,
            comparison: _,
        } = Box::pin(TerminalPipeline::prepare(&db, &store, dec!(100))).await;
        let serving = Box::pin(record_shadow_binding(
            &db, &store, &serving, &cycles, &jobs, &claim, 13,
        ))
        .await;
        let route_before = serving
            .generations
            .current_route(BuyModelRoute::Crypto)
            .expect("F11 Crypto route before Decision")
            .published_shadow_identity()
            .expect("F11 Crypto shadow is bound before Decision");
        insert_observation(
            &db,
            cycles.as_ref(),
            &serving.generations,
            BuyModelRoute::Crypto,
        )
        .await;
        tokio::time::sleep(StdDuration::from_millis(1_100)).await;
        Box::pin(record_shadow(
            &db,
            &store,
            &serving.generations,
            &cycles,
            &jobs,
            &claim,
            14,
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
            route: BuyModelRoute::Crypto,
            serving_generations: Arc::clone(&serving.generations),
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
        finalize_decision(cycles.as_ref(), &claim, &completion, 15).await;
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
            serving
                .generations
                .current_route(BuyModelRoute::Crypto)
                .expect("F11 route after")
                .published_shadow_identity()
                .expect("F11 published identity after"),
            route_before
        );
        let candidate_after = PgModelRegistryRepository::new(db.clone())
            .find_model_version(&models.candidate.model_version_id)
            .await
            .expect("read F11 candidate")
            .expect("F11 candidate exists");
        assert_eq!(
            candidate_after.artifact_hash,
            models.candidate.artifact_hash
        );
        assert_eq!(
            candidate_after.serving_contract_hash,
            models.candidate.serving_contract_hash
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

    async fn run_rejected(self) -> DecisionPathEvidence {
        assert!(matches!(self, Self::RejectedComparison));
        assert_authority_boundary();
        let (pool, _container) = setup_pg().await;
        let db = pool.connection().clone();
        let artifact_root = ArtifactRoot::create();
        let store = artifact_root.store();
        let TerminalPipeline {
            models,
            serving,
            claim,
            cycles,
            jobs,
            comparison,
        } = Box::pin(TerminalPipeline::prepare(&db, &store, Decimal::ZERO)).await;
        let policy_repository = PgPolicyRepository::new(db.clone());
        let policy_before = policy_repository
            .load_current_bundle()
            .await
            .expect("load terminal Comparison policy before")
            .expect("terminal Comparison policy exists");
        let probe = InvariantProbe {
            db: db.clone(),
            cycle_id: claim.cycle.feedback_cycle_id,
            champion_model_version_id: models.champion.model_version_id,
            candidate_model_version_id: models.candidate.model_version_id,
            route: BuyModelRoute::Crypto,
            serving_generations: Arc::clone(&serving.generations),
            policy_apply: None,
        };
        let before = probe.load().await;
        let counts_before = load_evidence_counts(&db).await;
        let stage = comparison_stage(&db, &cycles, &jobs, Arc::clone(&store));
        let job = jobs
            .find_by_id(&comparison.job_id)
            .await
            .expect("read terminal Comparison job")
            .expect("terminal Comparison job exists");
        let success = stage
            .succeeded_comparison(&claim.cycle, &job)
            .await
            .expect("verify terminal Comparison rejection");
        let replay = stage
            .succeeded_comparison(&claim.cycle, &job)
            .await
            .expect("replay terminal Comparison rejection");
        assert_eq!(success, replay);
        let FeedbackStageDirective::Complete(terminal) = success.directive() else {
            panic!("rejected Comparison must complete the cycle");
        };
        assert_eq!(
            terminal.decision(),
            Some(FeedbackDecision::ChallengerRejected)
        );
        assert_eq!(
            terminal.reason_code(),
            "comparison_all_challengers_rejected"
        );
        cycles
            .finalize_cycle(claim.lease, terminal.clone())
            .await
            .expect("finalize terminal Comparison cycle");
        let after = probe.load().await;
        let policy_after = policy_repository
            .load_current_bundle()
            .await
            .expect("load terminal Comparison policy after")
            .expect("terminal Comparison policy remains");
        let timeline = timeline_evidence(
            cycles
                .list_stage_events(&claim.cycle.feedback_cycle_id)
                .await
                .expect("load terminal Comparison timeline"),
        );
        let counts_after_first = load_evidence_counts(&db).await;

        let replay_evidence = Box::pin(
            RejectedReplayProbe {
                db: &db,
                store: &store,
                cycles: &cycles,
                jobs: &jobs,
                claim: &claim,
                comparison: &comparison,
                job: &job,
                success: &success,
                invariant: &probe,
                expected_after: &after,
                expected_timeline: &timeline,
                models: &models,
            }
            .verify(),
        )
        .await;
        let invariant_diff = InvariantDiff::between(&before, &after);
        assert_protected_unchanged(
            &invariant_diff,
            "terminal Comparison",
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
            path: DecisionPath::ChallengerRejected,
            decision: wire_name(&FeedbackDecision::ChallengerRejected),
            decision_reason: "comparison_all_challengers_rejected".to_owned(),
            exact_ids: exact_identifiers(ExactIdInput {
                cycle: &claim.cycle,
                models: &models,
                policy_before: &policy_before,
                policy_after: &policy_after,
                permit: None,
                policy_activation_id: None,
                promotion_transaction_hash: None,
                route: BuyModelRoute::Crypto,
            }),
            decision_artifact: replay_evidence.artifact,
            permit: None,
            worm_timeline: timeline,
            before,
            after,
            invariant_diff,
            replay: ReplayEvidence::new(
                "committed:challenger_rejected_at_comparison",
                "exact_comparison_artifact_read_replay",
                counts_before,
                counts_after_first,
                replay_evidence.counts_after_exact_replay,
            ),
            restart_read_back: replay_evidence.restart,
            fault_injection_rollback_verified: false,
        };
        evidence.validate();
        evidence
    }
}

pub async fn terminal_decision_contracts() {
    let no_action = Box::pin(run_suite_large_stack(
        TerminalDecisionCase::InsufficientShadow.run(),
    ))
    .await
    .expect("run isolated insufficient-shadow decision path");
    let rejected = Box::pin(run_suite_large_stack(
        TerminalDecisionCase::RejectedComparison.run(),
    ))
    .await
    .expect("run isolated rejected-comparison decision path");
    no_action.validate();
    rejected.validate();
}

struct ShadowBoundCase {
    db: DatabaseConnection,
    store: Arc<dyn ArtifactStore>,
    models: ShadowModels,
    model_repository: Arc<PgModelRegistryRepository>,
    serving: ActivatedServing,
    route_before: PublishedShadowRouteIdentity,
    claim: FeedbackCycleClaim,
    cycles: Arc<PgFeedbackCycleRepository>,
    jobs: Arc<PgResearchJobRepository>,
}

impl ShadowBoundCase {
    async fn prepare(db: DatabaseConnection, store: Arc<dyn ArtifactStore>) -> Self {
        let models = Box::pin(build_crypto_models(&db, &store)).await;
        let model_repository = Arc::new(PgModelRegistryRepository::new(db.clone()));
        let parity_run_id = ModelVersionFixture::persist_parity_proof(&db, &models.candidate)
            .await
            .expect("persist P03 candidate full-parity proof");
        PgFeatureParityRepository::new(db.clone())
            .acknowledge_latch(
                &parity_run_id,
                FeatureParityLatchActor {
                    actor: Some("promotion-preflight-fixture".to_owned()),
                    acting_role: "risk_owner".to_owned(),
                    reason: "acknowledge exact candidate-bound full parity proof".to_owned(),
                },
            )
            .await
            .expect("initialize P03 feature-parity latch from exact proof");
        let serving = activate_crypto_generation(&db, &store, &models).await;
        let (schema, claim) = record_crypto_cycle(&db, &models).await;
        let cycles = Arc::new(PgFeedbackCycleRepository::new(db.clone()));
        let jobs = Arc::new(PgResearchJobRepository::new(db.clone()));
        record_truth_attribution(
            &store,
            cycles.as_ref(),
            jobs.as_ref(),
            &claim,
            &schema.candidate_family,
        )
        .await;
        record_drift(
            cycles.as_ref(),
            jobs.as_ref(),
            &store,
            &claim.cycle,
            claim.lease,
            5,
        )
        .await;
        persist_recipe_plan_fixture(
            cycles.as_ref(),
            jobs.as_ref(),
            &store,
            claim.lease,
            &claim.cycle,
            &schema.candidate_family,
            6,
        )
        .await;
        let validation = ValidationGateRecorder {
            db: &db,
            store: &store,
            cycles: cycles.as_ref(),
            jobs: jobs.as_ref(),
            claim: &claim,
            models: &models,
            family: &schema.candidate_family,
            cpcv_event_sequence: 10,
            validation_event_sequence: 11,
        }
        .record()
        .await;
        let comparison = comparison_params_with_validation(
            &db,
            &schema,
            &claim,
            &models,
            validation.reference,
            validation.path_set_id,
            validation.path_set_hash,
        )
        .await;
        record_comparison(&db, &store, &claim, comparison, dec!(100), 12).await;
        let serving = Box::pin(record_shadow_binding(
            &db, &store, &serving, &cycles, &jobs, &claim, 13,
        ))
        .await;
        let route_before = serving
            .generations
            .current_route(BuyModelRoute::Crypto)
            .expect("P03 Crypto route after ShadowBind")
            .published_shadow_identity()
            .expect("P03 exact published shadow after ShadowBind");
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
        }
    }
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
        let bound = Box::pin(ShadowBoundCase::prepare(db, store)).await;
        let ShadowBoundCase {
            db,
            store,
            models,
            model_repository,
            serving,
            route_before,
            claim,
            cycles,
            jobs,
        } = bound;
        insert_stable_observations(&db, &serving.generations).await;
        Box::pin(record_shadow(
            &db,
            &store,
            &serving.generations,
            &cycles,
            &jobs,
            &claim,
            14,
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
        finalize_decision(cycles.as_ref(), &claim, &completion, 15).await;
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
                recipes: recipe_stage(&self.cycles, &self.jobs, Arc::clone(&self.store)),
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
            artifacts: Arc::clone(&tampered_store),
            recipes: recipe_stage(&self.cycles, &self.jobs, tampered_store),
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
    route_evidence: Arc<ModelRouteEvidenceService>,
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
        let route_evidence = Arc::new(ModelRouteEvidenceService::new(ModelRouteEvidenceDeps {
            policies: Arc::clone(&policies) as Arc<dyn PolicyRepository>,
            durable_runtime: Arc::clone(&runtime_repository) as Arc<dyn RuntimeControlRepository>,
            runtime_controls: runtime_controls.clone(),
            policy_store: Arc::new(DecisionPolicyStore::new_active(policy_before.clone())),
            models: Arc::clone(&case.model_repository) as Arc<dyn ModelRegistryRepository>,
            feature_parity: Arc::new(PgFeatureParityRepository::new(case.db.clone()))
                as Arc<dyn FeatureParityRepository>,
            runtime_registry: Arc::clone(&case.serving.runtime_registry),
            serving_generations: Arc::clone(&case.serving.generations),
        }));
        let preflight = Arc::new(PromotionPreflightService::new(
            PromotionPreflightServiceDeps {
                permits: Arc::clone(&permits) as Arc<dyn PromotionPermitRepository>,
                cycles: Arc::clone(&case.cycles) as Arc<dyn FeedbackCycleRepository>,
                decisions,
                manifests: Arc::new(PgModelCandidateManifestRepository::new(case.db.clone()))
                    as Arc<dyn ModelCandidateManifestRepository>,
                route_evidence: Arc::clone(&route_evidence),
                metrics: Arc::new(MetricsHub::new()),
            },
        ));
        let plan = preflight
            .prepare_issue(PromotionPreflightDraft {
                feedback_cycle_id: case.claim.cycle.feedback_cycle_id,
                ttl_secs: 600,
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
        let projected_at = case
            .cycles
            .database_time()
            .await
            .expect("load promotion projection clock");
        let projected = plan.projection().candidate_snapshot(projected_at);
        plan.projection()
            .validate_candidate(&projected, projected_at)
            .expect("promotion projection changes only its exact route-owned binding");
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
            route_evidence,
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
        assert_eq!(verified.projection(), &self.initial_projection);
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
        let champion = self
            .model_repository
            .find_model_version(&self.models.champion.model_version_id)
            .await
            .expect("read CandidateReady champion")
            .expect("CandidateReady champion exists");
        let candidate = self
            .model_repository
            .find_model_version(&self.models.candidate.model_version_id)
            .await
            .expect("read CandidateReady candidate")
            .expect("CandidateReady candidate exists");
        assert_eq!(champion.artifact_hash, self.models.champion.artifact_hash);
        assert_eq!(candidate.artifact_hash, self.models.candidate.artifact_hash);
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
        let store = artifact_root.store();
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
    service: Arc<ModelRouteGovernanceService>,
    raw_apply: Arc<PromotionPolicyPort>,
    policy_apply: Arc<CommittedPolicyApplicator>,
    actor: PromotionPermitActor,
    permit: PromotionPermitInfo,
    initial_preflight: PromotionPreflight,
    policy_before: ActivePolicyBundle,
}

struct PromotionOnlyModelGovernance;

#[async_trait::async_trait]
impl ModelGovernancePort for PromotionOnlyModelGovernance {
    async fn preview_gate(
        &self,
        _model_version_id: &ModelVersionId,
        _intent: GatePreviewIntent,
        _backtest_report_id: Option<&BacktestReportId>,
    ) -> Result<QualityGateReportView, QuantError> {
        Err(FeedbackError::InvalidBootstrapPreflight {
            detail: "promotion fixture does not execute the bootstrap quality gate".to_owned(),
        }
        .into())
    }

    async fn evaluate_candidate(
        &self,
        _model_version_id: &ModelVersionId,
        _evidence: CandidateQualityGateEvidence,
        _evaluated_at: DateTime<Utc>,
    ) -> Result<QualityGateReport, QuantError> {
        Err(FeedbackError::InvalidBootstrapPreflight {
            detail: "promotion fixture does not execute the candidate quality gate".to_owned(),
        }
        .into())
    }

    async fn evaluate_bootstrap(
        &self,
        _model_version_id: &ModelVersionId,
        _input: BootstrapQualityGateInput,
        _evaluated_at: DateTime<Utc>,
    ) -> Result<BootstrapQualityGateEvidence, QuantError> {
        Err(FeedbackError::InvalidBootstrapPreflight {
            detail: "promotion fixture does not execute the bootstrap quality gate".to_owned(),
        }
        .into())
    }

    async fn seal_calibrated_model(
        &self,
        _model_version_id: &ModelVersionId,
        _command: CalibratedModelSealCommand,
        _actor: GovernanceActor,
    ) -> Result<ModelVersionInfo, QuantError> {
        Err(FeedbackError::InvalidBootstrapPreflight {
            detail: "promotion fixture does not seal calibrated models".to_owned(),
        }
        .into())
    }
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
        let bootstrap_preflight = Arc::new(ModelRouteBootstrapService::new(
            ModelRouteBootstrapServiceDeps {
                route_evidence: Arc::clone(&permit_fixture.route_evidence),
                path_sets: Arc::new(PgBacktestPathSetRepository::new(case.db.clone()))
                    as Arc<dyn BacktestPathSetRepository>,
                backtests: Arc::new(PgBacktestReportRepository::new(case.db.clone()))
                    as Arc<dyn BacktestReportRepository>,
                cycles: Arc::clone(&case.cycles) as Arc<dyn FeedbackCycleRepository>,
                model_governance: Arc::new(PromotionOnlyModelGovernance)
                    as Arc<dyn ModelGovernancePort>,
                calibrations: Arc::new(PgCalibrationArtifactRepository::new(case.db.clone()))
                    as Arc<dyn CalibrationArtifactRepository>,
                datasets: Arc::new(PgTrainingDatasetRepository::new(case.db.clone()))
                    as Arc<dyn TrainingDatasetRepository>,
                artifacts: Arc::clone(&case.store),
            },
        ));
        let service = Arc::new(ModelRouteGovernanceService::new(
            ModelRouteGovernanceServiceDeps {
                bootstrap_preflight,
                bootstrap_repository: Arc::new(PgModelRouteBootstrapRepository::new(
                    case.db.clone(),
                )) as Arc<dyn ModelRouteBootstrapRepository>,
                preflight: Arc::clone(&permit_fixture.preflight),
                repository: Arc::clone(&promotions) as Arc<dyn ModelRoutePromotionRepository>,
                shadow_bindings: Arc::new(PgModelRouteShadowBindingRepository::new(case.db.clone()))
                    as Arc<dyn ModelRouteShadowBindingRepository>,
                policies: Arc::clone(&permit_fixture.policies) as Arc<dyn PolicyRepository>,
                policy_apply: Arc::clone(&policy_apply) as Arc<dyn CommittedPolicyApplyPort>,
            },
        ));
        let PromotionPermitFixture {
            policies,
            runtime_repository,
            runtime_controls,
            route_evidence: _,
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
        ttl_secs: u32,
    ) -> PromotionPreflightPlan {
        self.preflight
            .prepare_issue(PromotionPreflightDraft {
                feedback_cycle_id: cycle_id,
                ttl_secs,
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
        self.issue_preflight(plan.preflight(), idempotency_key, preflight_hash, reason)
            .await
    }

    async fn issue_preflight(
        &self,
        preflight: &PromotionPreflight,
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
                scope: preflight.scope().clone(),
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
        CommitModelRoutePromotion::try_new(self.initial_request(), self.initial_preflight.clone())
            .expect("build exact P06 initial promotion command")
    }

    fn initial_request(&self) -> PromoteModelRoute {
        self.activation_request(
            self.permit.promotion_permit_id,
            self.initial_preflight.feedback_cycle_id(),
            &self.initial_preflight,
            "p04-route-activation-0001",
        )
    }

    fn activation_request(
        &self,
        permit_id: PromotionPermitId,
        cycle_id: FeedbackCycleId,
        preflight: &PromotionPreflight,
        idempotency_key: &str,
    ) -> PromoteModelRoute {
        PromoteModelRoute {
            promotion_permit_id: permit_id,
            feedback_cycle_id: cycle_id,
            expected_policy_generation: preflight.scope().expected_policy_generation(),
            expected_runtime_control_revision: preflight.runtime_control_revision(),
            idempotency_key: idempotency_key
                .parse::<PolicyIdempotencyKey>()
                .expect("valid route-activation idempotency key"),
            actor: self.actor.clone(),
            reason_code: "candidate_approved".to_owned(),
            note: "activate exact permit-bound model route".to_owned(),
        }
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
        let request = harness.initial_request();
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
                "$.candidate_model",
                "$.parity_latch",
                "$.in_memory_serving_route",
                "$.deployment_authority",
            ],
        );
        for changed in [
            "$.policy_bundle",
            "$.model_routes",
            "$.cycle",
            "$.policy_apply_readiness",
        ] {
            assert!(
                invariant_diff.any_below(changed),
                "Promoted path failed to change required invariant {changed}"
            );
        }
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
            policy_after.snapshot.operations_policy,
            policy_before.snapshot.operations_policy
        );
        assert_eq!(
            policy_after.snapshot.execution_automation_policy,
            policy_before.snapshot.execution_automation_policy
        );
        assert_eq!(
            policy_after.snapshot.profile_artifacts,
            policy_before.snapshot.profile_artifacts
        );
        let before_routes = &policy_before.snapshot.model_routing.model;
        let after_routes = &policy_after.snapshot.model_routing.model;
        assert_eq!(
            after_routes.active_exit_model_version_id,
            before_routes.active_exit_model_version_id
        );
        assert_eq!(
            after_routes.buy_routes.get(&BuyModelRoute::Pooled),
            before_routes.buy_routes.get(&BuyModelRoute::Pooled)
        );
        assert_eq!(
            after_routes.buy_routes.get(&BuyModelRoute::Weather),
            before_routes.buy_routes.get(&BuyModelRoute::Weather)
        );
        assert_eq!(
            after_routes
                .champion(BuyModelRoute::Crypto)
                .map(|binding| binding.model_version_id)
                .ok(),
            Some(self.case.models.candidate.model_version_id)
        );
        assert_eq!(
            before_routes
                .champion(BuyModelRoute::Crypto)
                .map(|binding| binding.model_version_id)
                .ok(),
            Some(self.case.models.champion.model_version_id)
        );
        assert_eq!(
            before_routes
                .route_binding(BuyModelRoute::Crypto)
                .ok()
                .and_then(|binding| binding.shadow.as_ref())
                .map(|binding| binding.model_version_id),
            Some(self.case.models.candidate.model_version_id)
        );
        assert!(
            after_routes
                .route_binding(BuyModelRoute::Crypto)
                .expect("promoted Crypto route")
                .shadow
                .is_none()
        );
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
        Box::pin(self.harness.service.activate(self.request.clone()))
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
        let plan = self
            .harness
            .prepare_plan(self.case.claim.cycle.feedback_cycle_id, 300)
            .await;
        let database_now = self
            .case
            .cycles
            .database_time()
            .await
            .expect("read P06 expiry-race start clock");
        let preflight = expiring_preflight(plan.preflight(), database_now + Duration::seconds(3));
        let permit = self
            .harness
            .issue_preflight(
                &preflight,
                "p06-expiry-lock-race-0001",
                preflight.preflight_hash(),
                "prove expiry after permit row lock wait",
            )
            .await;
        let request = self.harness.activation_request(
            permit.promotion_permit_id,
            self.case.claim.cycle.feedback_cycle_id,
            &preflight,
            "p06-expiry-route-activation-0001",
        );
        let command = CommitModelRoutePromotion::try_new(request, preflight)
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
        self.wait_for_expiry(permit.expires_at).await;
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
        let request = self.harness.activation_request(
            permit_id,
            self.case.claim.cycle.feedback_cycle_id,
            &self.harness.initial_preflight,
            "p06-absent-route-activation-0001",
        );
        let error = Box::pin(self.harness.service.activate(request))
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
        let plan = self
            .harness
            .prepare_plan(self.case.claim.cycle.feedback_cycle_id, 300)
            .await;
        let database_now = self
            .case
            .cycles
            .database_time()
            .await
            .expect("read P06 expiry fault clock");
        let preflight = expiring_preflight(plan.preflight(), database_now + Duration::seconds(3));
        let permit = self
            .harness
            .issue_preflight(
                &preflight,
                "p06-expired-permit-0001",
                preflight.preflight_hash(),
                "prove expired authority cannot promote",
            )
            .await;
        self.wait_for_expiry(permit.expires_at).await;
        let request = self.harness.activation_request(
            permit.promotion_permit_id,
            self.case.claim.cycle.feedback_cycle_id,
            &preflight,
            "p06-expired-route-activation-0001",
        );
        let error = Box::pin(self.harness.service.activate(request))
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
        let plan = self
            .harness
            .prepare_plan(self.case.claim.cycle.feedback_cycle_id, 600)
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
        let request = self.harness.activation_request(
            permit.promotion_permit_id,
            self.case.claim.cycle.feedback_cycle_id,
            plan.preflight(),
            "p06-revoked-route-activation-0001",
        );
        let error = Box::pin(self.harness.service.activate(request))
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
        let plan = self
            .harness
            .prepare_plan(self.case.claim.cycle.feedback_cycle_id, 600)
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
        let request = self.harness.activation_request(
            permit.promotion_permit_id,
            self.case.claim.cycle.feedback_cycle_id,
            plan.preflight(),
            "p06-hash-route-activation-0001",
        );
        let error = Box::pin(self.harness.service.activate(request))
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
        let error = Box::pin(self.harness.service.activate(self.request.clone()))
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

        let replay = Box::pin(self.harness.service.activate(self.request.clone()))
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
        let left_request = self.request.clone();
        let right_request = self.request.clone();
        let (left, right) = tokio::join!(
            async move { Box::pin(left_service.activate(left_request)).await },
            async move { Box::pin(right_service.activate(right_request)).await },
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
            self.harness
                .initial_preflight
                .serving_constraints()
                .scenario_model_bindings()
                .to_vec(),
        )
        .expect("rebuild P04 route projection")
        .validate_candidate(&policy_after.snapshot, committed.activation.activated_at)
        .expect("P04 changed only the exact category route and consumed shadow");

        let champion_after =
            ModelVersionEntity::find_by_id(self.case.models.champion.model_version_id)
                .one(&self.db)
                .await
                .expect("read P04 champion after commit")
                .expect("P04 champion exists after commit");
        assert_eq!(champion_after, self.champion_before);
        let candidate_after =
            ModelVersionEntity::find_by_id(self.case.models.candidate.model_version_id)
                .one(&self.db)
                .await
                .expect("read P04 candidate after commit")
                .expect("P04 candidate exists after commit");
        assert_eq!(candidate_after, self.candidate_before);

        let cycle_after = CycleEntity::find_by_id(self.case.claim.cycle.feedback_cycle_id)
            .one(&self.db)
            .await
            .expect("read P04 cycle after commit")
            .expect("P04 cycle exists after commit");
        assert_eq!(cycle_after.status, FeedbackCycleStatus::Succeeded);
        assert_eq!(cycle_after.decision, Some(FeedbackDecision::Promoted));
        assert_eq!(cycle_after.generation, self.cycle_before.generation + 1);
        let decision_stage = self.case.decision_stage();
        let historical = decision_stage
            .candidate_evidence(&self.case.claim.cycle.feedback_cycle_id)
            .await
            .expect("Promoted cycle retains immutable CandidateReady audit evidence");
        assert_eq!(historical.cycle.decision, Some(FeedbackDecision::Promoted));
        decision_stage
            .promotion_evidence(&self.case.claim.cycle.feedback_cycle_id)
            .await
            .expect_err("Promoted cycle cannot be reused as actionable promotion preflight");
        let cycle_activation = self
            .harness
            .promotions
            .find_cycle_activation(&self.case.claim.cycle.feedback_cycle_id)
            .await
            .expect("resolve promotion by exact feedback cycle")
            .expect("Promoted cycle retains its immutable activation graph");
        assert_eq!(
            cycle_activation.activation.policy_activation_id,
            committed.activation.policy_activation_id
        );
        assert_eq!(
            cycle_activation.transaction_hash,
            committed.transaction_hash
        );
        let shadow_lifecycle = PgModelRouteShadowBindingRepository::new(self.db.clone())
            .find_lifecycle(&ShadowBindingArtifactId::from_cycle_id(
                self.case.claim.cycle.feedback_cycle_id,
            ))
            .await
            .expect("read promoted route-owned shadow lifecycle")
            .expect("promoted route-owned shadow lifecycle exists");
        assert_eq!(shadow_lifecycle.status, ShadowBindingStatus::Promoted);
        assert_eq!(
            shadow_lifecycle.termination_policy_activation_id,
            Some(committed.activation.policy_activation_id)
        );
        assert_eq!(
            shadow_lifecycle.terminated_at,
            Some(committed.activation.activated_at)
        );
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
        let error = Box::pin(self.harness.service.activate(self.request.clone()))
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
        let replay = Box::pin(self.harness.service.activate(self.request.clone()))
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
        let replay = Box::pin(self.harness.service.activate(self.request.clone()))
            .await
            .expect("replay exact P04 promotion");
        assert_eq!(replay.outcome, ModelRoutePromotionOutcome::ExactReplay);
        assert_eq!(replay.transaction_hash, committed.transaction_hash);
        assert_eq!(
            replay.activation.policy_activation_id,
            committed.activation.policy_activation_id
        );
        assert_eq!(PromotionRowCounts::load(&self.db).await, replay_counts);
        let mut drift_request = self.request.clone();
        drift_request.feedback_cycle_id = FeedbackCycleId::from_v7();
        let drift = Box::pin(self.harness.service.activate(drift_request))
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
        let store = artifact_root.store();
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
    let no_action = Box::pin(run_suite_large_stack(
        TerminalDecisionCase::InsufficientShadow.run(),
    ))
    .await
    .expect("run isolated insufficient-shadow evidence path");
    let rejected = Box::pin(run_suite_large_stack(
        TerminalDecisionCase::RejectedComparison.run(),
    ))
    .await
    .expect("run isolated rejected-comparison evidence path");
    let candidate_ready = Box::pin(run_suite_large_stack(
        DecisionPathEvidence::candidate_ready(),
    ))
    .await
    .expect("run isolated CandidateReady evidence path");
    let promoted = Box::pin(run_suite_large_stack(DecisionPathEvidence::promoted()))
        .await
        .expect("run isolated promoted evidence path");
    let paths = vec![no_action, rejected, candidate_ready, promoted];
    let artifact = DecisionPathEvidenceManifest::new(paths).write();
    assert!(artifact.path.is_file());
    ContentHash::parse(&artifact.content_hash).expect("evidence hash uses canonical BLAKE3 text");
}

pub async fn promotion_runtime_apply_contracts() {
    let (pool, _container) = setup_pg().await;
    let artifact_root = ArtifactRoot::create();
    let store = artifact_root.store();
    let case = Box::pin(AtomicPromotionCase::prepare(
        pool.connection().clone(),
        store,
    ))
    .await;
    Box::pin(case.assert_apply_recovery()).await;
}

const SHADOW_CANCEL_REASON: &str = "operator_cancelled_after_shadow_bind";

impl ShadowBoundCase {
    async fn request_cancel(&self) -> FeedbackCycleInfo {
        let cycle = self
            .cycles
            .find_cycle(&self.claim.cycle.feedback_cycle_id)
            .await
            .expect("load shadow-bound cycle")
            .expect("shadow-bound cycle exists");
        let events = self
            .cycles
            .list_stage_events(&cycle.feedback_cycle_id)
            .await
            .expect("load shadow-bound timeline");
        let event_sequence = events
            .iter()
            .map(|event| event.event_sequence)
            .max()
            .expect("shadow-bound timeline is non-empty")
            + 1;
        let occurred_at = self
            .cycles
            .database_time()
            .await
            .expect("load cancellation database time");
        let cancellation = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
            feedback_cycle_id: cycle.feedback_cycle_id,
            event_sequence,
            stage: FeedbackStage::Shadow,
            event_kind: FeedbackStageEventKind::CancellationRequested,
            trigger_family: None,
            research_job_id: None,
            actor: Some("operator@risk_owner".to_owned()),
            reason_code: Some(SHADOW_CANCEL_REASON.to_owned()),
            evidence_uri: None,
            evidence_hash: None,
            occurred_at,
        })
        .expect("seal post-ShadowBind cancellation request");
        self.cycles
            .request_cancel(FeedbackCycleGeneration::from(&cycle), cancellation)
            .await
            .expect("persist post-ShadowBind cancellation request");
        let cancelled_cycle = self
            .cycles
            .find_cycle(&cycle.feedback_cycle_id)
            .await
            .expect("reload cancellation-pending cycle")
            .expect("cancellation-pending cycle exists");
        assert_eq!(cancelled_cycle.status, FeedbackCycleStatus::Running);
        assert_eq!(cancelled_cycle.cancel_requested_at, Some(occurred_at));
        cancelled_cycle
    }
}

async fn assert_shadow_cancelled(
    case: &ShadowBoundCase,
    db: &DatabaseConnection,
    bindings: &Arc<PgModelRouteShadowBindingRepository>,
    before: &ShadowBindingLifecycle,
    cancelled_cycle: &FeedbackCycleInfo,
) {
    let policies = Arc::new(PgPolicyRepository::new(db.clone()));
    let policy_before = policies
        .load_current_bundle()
        .await
        .expect("load pre-cancellation policy")
        .expect("pre-cancellation policy exists");
    let policy_apply = Arc::new(ShadowBindingApplyProbe::new(PolicyBundleIdentity::from(
        &policy_before,
    )));
    let service = ShadowBindingCancellationService::new(ShadowBindingCancellationDeps {
        bindings: Arc::clone(bindings) as Arc<dyn ModelRouteShadowBindingRepository>,
        policies: Arc::clone(&policies) as Arc<dyn PolicyRepository>,
        policy_apply: Arc::clone(&policy_apply) as Arc<dyn CommittedPolicyApplyPort>,
    });
    service
        .release_cycle(cancelled_cycle, SHADOW_CANCEL_REASON)
        .await
        .expect("release exact cancelled-cycle route shadow");

    let after = bindings
        .find_lifecycle(&before.binding_id)
        .await
        .expect("load cancelled binding lifecycle")
        .expect("cancelled binding exists");
    assert_eq!(after.status, ShadowBindingStatus::Cancelled);
    assert_eq!(after.lifecycle_generation, before.lifecycle_generation + 1);
    assert_eq!(after.binding_generation, before.binding_generation);
    assert_eq!(
        after.termination_reason_code.as_deref(),
        Some(SHADOW_CANCEL_REASON)
    );
    let activation_id = after
        .termination_policy_activation_id
        .expect("cancelled binding records its policy activation");
    let activation = ActivationEntity::find_by_id(activation_id)
        .one(db)
        .await
        .expect("load shadow cancellation activation")
        .expect("shadow cancellation activation exists");
    assert_eq!(
        activation.activation_kind,
        PolicyActivationKind::ModelShadowCancellation
    );
    assert!(activation.activated_by_user_id.is_none());
    assert_eq!(activation.activated_by_label, "feedback-coordinator");
    assert!(
        ActivationAuditEntity::find_by_id(activation.audit_event_id)
            .one(db)
            .await
            .expect("load cancellation audit")
            .is_some()
    );
    assert!(
        ActivationOutboxEntity::find_by_id(activation.audit_event_id)
            .one(db)
            .await
            .expect("load cancellation outbox")
            .is_some()
    );
    let policy_after = policies
        .load_current_bundle()
        .await
        .expect("load post-cancellation policy")
        .expect("post-cancellation policy exists");
    let route = policy_after
        .snapshot
        .model_routing
        .model
        .route_binding(BuyModelRoute::Crypto)
        .expect("load cancelled Crypto route");
    assert_eq!(
        route.champion.model_version_id,
        before.champion_model_version_id
    );
    assert_eq!(route.champion.generation, before.binding_generation + 1);
    assert_eq!(route.champion.config_revision, policy_after.generation);
    assert!(route.shadow.is_none());
    assert_eq!(
        policy_apply.readiness(),
        PolicyApplyReadiness::Ready {
            applied: PolicyBundleIdentity::from(&policy_after),
        }
    );

    let counts = load_evidence_counts(db).await;
    service
        .release_cycle(cancelled_cycle, SHADOW_CANCEL_REASON)
        .await
        .expect("replay cancelled shadow convergence without another write");
    assert_eq!(load_evidence_counts(db).await, counts);

    case.cycles
        .finalize_cycle(
            case.claim.lease.with_generation(cancelled_cycle.generation),
            FeedbackCycleTerminal::try_cancelled(SHADOW_CANCEL_REASON.to_owned())
                .expect("seal cancelled feedback terminal"),
        )
        .await
        .expect("finalize cycle only after shadow release convergence");
    let terminal = case
        .cycles
        .find_cycle(&cancelled_cycle.feedback_cycle_id)
        .await
        .expect("load terminal cancelled cycle")
        .expect("terminal cancelled cycle exists");
    assert_eq!(terminal.status, FeedbackCycleStatus::Cancelled);
    assert_eq!(
        terminal.terminal_reason_code.as_deref(),
        Some(SHADOW_CANCEL_REASON)
    );
}

pub async fn shadow_cancellation_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let artifact_root = ArtifactRoot::create();
    let store = artifact_root.store();
    let case = Box::pin(ShadowBoundCase::prepare(db.clone(), store)).await;
    let bindings = Arc::new(PgModelRouteShadowBindingRepository::new(db.clone()));
    let binding_id = ShadowBindingArtifactId::from_cycle_id(case.claim.cycle.feedback_cycle_id);
    let before = bindings
        .find_lifecycle(&binding_id)
        .await
        .expect("load active cancellation fixture binding")
        .expect("cancellation fixture binding exists");
    assert_eq!(before.status, ShadowBindingStatus::Active);

    let cancelled_cycle = case.request_cancel().await;
    assert_shadow_cancelled(&case, &db, &bindings, &before, &cancelled_cycle).await;
}

async fn rejection_fault_matrix() {
    let (pool, _container) = setup_pg().await;
    let artifact_root = ArtifactRoot::create();
    let store = artifact_root.store();
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
    let store = artifact_root.store();
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
    let store = artifact_root.store();
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
