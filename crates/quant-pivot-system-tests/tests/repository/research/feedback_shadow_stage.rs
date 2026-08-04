//! F09-to-F10 contracts against real `PostgreSQL` and object-store adapters.

use std::{env, fs, path::PathBuf, sync::Arc, time::Duration as StdDuration};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::service::{
    feedback_coordinator::FeedbackStagePreparation,
    feedback_recipe_stage::{FeedbackRecipeStageAdapter, FeedbackRecipeStageDeps},
    feedback_shadow::{FeedbackShadowExecutionDeps, FeedbackShadowExecutionService},
    feedback_shadow_binding_stage::{
        FeedbackShadowBindingStageAdapter, FeedbackShadowBindingStageDeps,
    },
    feedback_shadow_stage::{FeedbackShadowStageAdapter, FeedbackShadowStageDeps},
    model_serving_generation::{ModelServingGenerationStore, PublishedShadowRouteIdentity},
    model_serving_registry::ModelServingRuntimeRegistry,
    research_readiness::EvidenceScopeIdentity,
};
use quant_pivot_models::{
    config::{ArtifactStoreDeployConfig, ClickHouseConfig},
    domain::{
        ports::{
            FeedbackComparisonArtifactRef, FeedbackComparisonCandidateRef,
            FeedbackComparisonJobInput, FeedbackComparisonJobParams, FeedbackEvaluationUseRef,
            FeedbackLearningStageArtifactRef, FeedbackShadowExecutionPort, FeedbackShadowSubject,
            FeedbackValidationArtifactRef, ShadowBindingArtifact, ShadowBindingJobInput,
            ShadowBindingJobParams, ShadowBindingReceipt, ShadowBindingReceiptInput,
        },
        quant::{
            FeedbackCycleKey, FeedbackCycleKeyInput, FeedbackEvaluationUseInput,
            FeedbackStageEventInput, FeedbackStageJobIdentity, ModelVersionInfo, NewFeedbackCycle,
            NewFeedbackEvaluationUse, NewFeedbackStageEvent, NewModelVersion, NewResearchJob,
            NewShadowComparison, NoopProgressSink, ResearchJobArtifactRef, ResearchJobFinalization,
            ResearchJobResultRef,
        },
    },
    entities::quant_shadow_comparison::Entity as ShadowComparisonEntity,
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{
            DatasetPurpose, FeedbackEvaluationMode, FeedbackStage, FeedbackStageEventKind,
            FeedbackTriggerFamily, ModelWeightSource, ResearchJobKind, ResearchJobResultKind,
            ResearchJobStatus,
        },
        runtime_config::ConfigResourceKind,
    },
    hashing::CanonicalDigest,
    runtime_config::{
        BuyModelRoute, BuyRouteBinding, DecisionPolicySnapshot, FactorCrossSectionConfig,
        FactorHeadConfig, ModelBinding, ModelBindingSource,
    },
    types::{
        ArtifactUri, AuditEventId, BacktestPathSetId, BacktestReportId, Bps, ContentHash,
        DecisionPolicySnapshotId, FeedbackComparisonArtifactId, FeedbackCycleId,
        FeedbackLearningStageArtifactId, FeedbackValidationArtifactId, ModelCandidateManifestId,
        ModelInputContract, ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId,
        PolicyActivationId, PolicyBundleGeneration, PolicyRevisionId, Probability, ResearchJobId,
        ResearchJobParams, ResearchProfileRef, RoleCode, SchemaVersion, ShadowComparisonId, Usd,
        WorkerId,
        factor::FactorServingPlane,
        model_metrics::ModelVersionMetrics,
        model_training::ModelTrainingObjective,
        shadow::{ShadowRankDelta, ShadowScoreDelta},
    },
};
use quant_pivot_repository::{
    postgres::{
        PgFeedbackCycleRepository, PgModelCandidateManifestRepository, PgModelRegistryRepository,
        PgPolicyRepository, PgResearchJobRepository, PgShadowComparisonRepository,
    },
    traits::{
        FeedbackCycleClaim, FeedbackCycleRepository, FeedbackEvaluationWriteOutcome,
        ModelRegistryRepository, PolicyRepository, ResearchJobEnqueueOutcome,
        ResearchJobRepository, ShadowComparisonRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
    backtest::PortfolioReturnObservation,
    factors::FactorEngine,
    features::FeatureSchema,
    feedback_comparison::{
        FeedbackComparisonArtifact, FeedbackComparisonArtifactInput, FeedbackComparisonCodec,
        FeedbackComparisonReplayRef, RomanoWolfCandidateInput, RomanoWolfOutcome,
        RomanoWolfStepdown,
    },
    feedback_shadow::{FeedbackShadowCodec, FeedbackShadowOutcome},
    feedback_shadow_binding::ShadowBindingCodec,
    hashing::ResearchHasher,
    model::ReturnModelSpec,
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        artifact_store::ReadTamperArtifactStoreFixture,
        model_serving_fixtures::{
            ModelArtifactFixtureSeed, ModelDatasetLedgerFixture, ModelDatasetLedgerSeed,
            ModelPayloadFixture, ModelVersionFixture, SealedModelFixture,
        },
        model_serving_runtime::ModelServingRegistryFixture,
        model_spec_fixtures,
        policy_fixtures::{activate_policy_bundle, bootstrap_policy_bundle},
    },
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{DatabaseConnection, EntityTrait, IntoActiveModel};
use tokio_util::sync::CancellationToken;

use super::feedback_boot_schema::{
    FeedbackSchemaFixture, content_hash, persist_recipe_plan_fixture, prepare_fixture,
    prepare_profile_fixture,
};

const JOB_LEASE_SECS: i64 = 90;
const OBSERVATION_COUNT: usize = 500;
const SHADOW_OBSERVATION_COUNT: usize = 1_000;
const SHADOW_MODEL_BUDGET_BYTES: u64 = 1 << 30;

fn shadow_stage(
    db: &DatabaseConnection,
    cycles: &Arc<PgFeedbackCycleRepository>,
    jobs: &Arc<PgResearchJobRepository>,
    store: Arc<dyn ArtifactStore>,
    generations: Arc<ModelServingGenerationStore>,
) -> FeedbackShadowStageAdapter {
    let recipes = Arc::new(
        FeedbackRecipeStageAdapter::try_new(FeedbackRecipeStageDeps {
            cycles: Arc::clone(cycles) as Arc<dyn FeedbackCycleRepository>,
            jobs: Arc::clone(jobs) as Arc<dyn ResearchJobRepository>,
            artifacts: Arc::clone(&store),
            max_recovery_attempts: 3,
        })
        .expect("build F10 RecipePlan stage"),
    );
    let shadow_bindings = Arc::new(
        FeedbackShadowBindingStageAdapter::try_new(FeedbackShadowBindingStageDeps {
            cycles: Arc::clone(cycles) as Arc<dyn FeedbackCycleRepository>,
            jobs: Arc::clone(jobs) as Arc<dyn ResearchJobRepository>,
            models: Arc::new(PgModelRegistryRepository::new(db.clone())),
            policies: Arc::new(PgPolicyRepository::new(db.clone())),
            manifests: Arc::new(PgModelCandidateManifestRepository::new(db.clone())),
            artifacts: Arc::clone(&store),
            recipes: Arc::clone(&recipes),
            total_shadow_model_budget_bytes: SHADOW_MODEL_BUDGET_BYTES,
            max_recovery_attempts: 3,
        })
        .expect("build F10 ShadowBind stage"),
    );
    FeedbackShadowStageAdapter::try_new(FeedbackShadowStageDeps {
        cycles: Arc::clone(cycles) as Arc<dyn FeedbackCycleRepository>,
        jobs: Arc::clone(jobs) as Arc<dyn ResearchJobRepository>,
        artifacts: store,
        serving_generations: generations,
        recipes,
        shadow_bindings,
        max_recovery_attempts: 3,
    })
    .expect("build F10 Shadow stage")
}

pub struct ArtifactRoot {
    pub path: PathBuf,
}

impl ArtifactRoot {
    pub fn create() -> Self {
        let path =
            env::temp_dir().join(format!("quant-pivot-w2-f10-{}", ModelVersionId::from_v7()));
        fs::create_dir_all(&path).expect("create F10 artifact root");
        Self { path }
    }
}

impl Drop for ArtifactRoot {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).expect("remove F10 artifact root");
    }
}

pub struct ShadowModels {
    pub champion: ModelVersionInfo,
    pub candidate: ModelVersionInfo,
}

fn new_model_version(
    fixture: &SealedModelFixture,
    model_spec_id: ModelSpecId,
    version: i32,
) -> NewModelVersion {
    let serving_contract = fixture.serving_contract().clone();
    let bindings = serving_contract.bindings();
    let model_version_id = bindings.model.model_version_id;
    let category_scope = bindings.model.category_scope;
    let profile_ref = bindings.model.profile_ref.clone();
    let training_dataset_id = bindings.dataset.manifest.training_dataset_id;
    NewModelVersion {
        model_version_id,
        model_spec_id,
        version,
        artifact_hash: fixture.artifact_hash(),
        serving_contract,
        category_scope,
        profile_ref,
        training_dataset_id: Some(training_dataset_id),
        trade_policy_artifact_id: None,
        trade_policy_hash: None,
        derivation: NewModelVersion::training_derivation(),
        metrics: ModelVersionMetrics::not_measured("F10 production-shadow fixture"),
        training_objective: ModelTrainingObjective::hand_authored("F10 production-shadow fixture"),
    }
}

struct ModelSealContext<'a> {
    db: &'a DatabaseConnection,
    store: &'a Arc<dyn ArtifactStore>,
    model_spec_id: ModelSpecId,
    model_spec_definition_hash: ContentHash,
    policy_id: DecisionPolicySnapshotId,
    factor_plane: &'a FactorServingPlane,
    feature_schema_hash: ContentHash,
    factor_head: &'a FactorHeadConfig,
    cross_section: &'a FactorCrossSectionConfig,
    profile_ref: ResearchProfileRef,
    category_scope: Option<MarketCategory>,
    prediction_horizon_secs: u64,
}

impl ModelSealContext<'_> {
    async fn seal(&self, scope: &str, version: i32) -> (NewModelVersion, ModelVersionId) {
        let model_version_id = ModelVersionId::from_v7();
        let window_end = Utc::now() - Duration::days(2);
        let dataset = ModelDatasetLedgerFixture::persist(
            self.db,
            self.store,
            ModelDatasetLedgerSeed {
                scope: format!("f10-shadow-{scope}"),
                model_spec_id: self.model_spec_id,
                model_family: ModelFamily::WeightedFactor,
                model_spec_definition_hash: self.model_spec_definition_hash,
                factor_serving_plane: self.factor_plane.clone(),
                feature_schema_version: SchemaVersion::FIRST,
                feature_schema_hash: self.feature_schema_hash,
                decision_policy_snapshot_id: self.policy_id,
                profile_ref: self.profile_ref.clone(),
                prediction_horizon_secs: self.prediction_horizon_secs,
                purpose: DatasetPurpose::Training,
                window_start: window_end - Duration::days(1),
                window_end,
                research_program_hash: ResearchHasher::canonical(&(
                    "f10-production-shadow-program-v1",
                    scope,
                    self.model_spec_id,
                ))
                .expect("F10 research-program hash"),
                sample_count: 32,
                decision_interval_secs: 1,
                trade_policy: None,
            },
        )
        .await
        .expect("persist F10 model Dataset");
        let payload = ModelPayloadFixture::weighted(
            self.factor_plane,
            self.factor_head,
            ModelInputContract::single_required("book.mid"),
            ReturnModelSpec::heuristic_default(),
            self.cross_section.clone(),
        )
        .expect("F10 weighted model payload");
        let fixture = SealedModelFixture::seal(
            self.db,
            ModelArtifactFixtureSeed {
                model_version_id,
                training_dataset_id: dataset.training_dataset_id,
                payload,
                training_input_hash: ResearchHasher::canonical(&(
                    "f10-production-shadow-training-input-v1",
                    scope,
                    model_version_id,
                ))
                .expect("F10 training-input hash"),
                category_scope: self.category_scope,
                calibration: None,
                bias_table: None,
            },
        )
        .await
        .expect("seal F10 model artifact");
        fixture
            .store(self.store)
            .await
            .expect("store F10 model artifact");
        (
            new_model_version(&fixture, self.model_spec_id, version),
            model_version_id,
        )
    }
}

async fn build_scoped_models(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    policy_id: DecisionPolicySnapshotId,
    profile_ref: ResearchProfileRef,
    category_scope: Option<MarketCategory>,
    prediction_horizon_secs: i64,
) -> ShadowModels {
    let policy_repo = PgPolicyRepository::new(db.clone());
    let policy = policy_repo
        .load_snapshot(&policy_id)
        .await
        .expect("load F10 policy")
        .expect("F10 policy exists");
    let profiles = &policy.snapshot.profile_artifacts;
    let factor_plane = FactorEngine::for_model_scope(
        &profiles.scoring.definition,
        &profiles.features.definition,
        &profiles.domain.definition,
        category_scope,
        None,
    )
    .serving_plane()
    .expect("build F10 factor plane")
    .clone();
    let feature_schema_hash = ResearchHasher::feature_schema(
        &FeatureSchema::build(&profiles.features.definition).expect("build F10 feature schema"),
    )
    .expect("F10 feature-schema hash");
    let model_spec_id = ModelSpecId::from_v7();
    let model_spec = model_spec_fixtures::new_model_spec_fixture(
        model_spec_id,
        "f10-shadow-generation",
        ModelFamily::WeightedFactor,
        prediction_horizon_secs,
        ModelInputContract::single_required("book.mid"),
        ModelTrainingContract::settlement_default(),
    );
    let definition_hash = model_spec.definition_hash;
    let registry = PgModelRegistryRepository::new(db.clone());
    registry
        .create_model_spec(model_spec)
        .await
        .expect("persist F10 model spec");
    let seal = ModelSealContext {
        db,
        store,
        model_spec_id,
        model_spec_definition_hash: definition_hash,
        policy_id,
        factor_plane: &factor_plane,
        feature_schema_hash,
        factor_head: &profiles.scoring.definition.factor_head,
        cross_section: &profiles.scoring.definition.cross_section,
        profile_ref,
        category_scope,
        prediction_horizon_secs: u64::try_from(prediction_horizon_secs)
            .expect("positive F10 prediction horizon"),
    };
    let (champion, champion_id) = seal.seal("champion", 1).await;
    let champion = ModelVersionFixture::persist_route_candidate(db, champion)
        .await
        .expect("persist F10 champion route candidate");
    assert_eq!(champion.model_version_id, champion_id);

    let (candidate, candidate_id) = seal.seal("candidate", 2).await;
    let candidate = registry
        .create_model_version(candidate)
        .await
        .expect("persist F10 shadow candidate");
    assert_eq!(candidate.model_version_id, candidate_id);
    assert_ne!(
        champion.serving_contract_hash, candidate.serving_contract_hash,
        "F10 serving subjects must have distinct contracts"
    );
    ShadowModels {
        champion,
        candidate,
    }
}

pub async fn build_models(db: &DatabaseConnection, store: &Arc<dyn ArtifactStore>) -> ShadowModels {
    let mut policy = DecisionPolicySnapshot::default();
    policy
        .profile_artifacts
        .research_method
        .model_promotion
        .required_shadow_window_secs = 10;
    let policy_id = bootstrap_policy_bundle(
        &PgPolicyRepository::new(db.clone()),
        &policy,
        "f10-shadow-stage",
        "bootstrap bounded F10 shadow-window fixture",
    )
    .await;
    Box::pin(build_scoped_models(
        db,
        store,
        policy_id,
        model_spec_fixtures::pooled_profile_ref(),
        None,
        model_spec_fixtures::pooled_horizon_secs(),
    ))
    .await
}

pub async fn build_crypto_models(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
) -> ShadowModels {
    let mut policy = DecisionPolicySnapshot::default();
    policy
        .profile_artifacts
        .research_method
        .model_promotion
        .required_shadow_window_secs = 2;
    let policy_id = bootstrap_policy_bundle(
        &PgPolicyRepository::new(db.clone()),
        &policy,
        "p03-promotion-preflight",
        "bootstrap exact category shadow window",
    )
    .await;
    Box::pin(build_scoped_models(
        db,
        store,
        policy_id,
        model_spec_fixtures::crypto_profile_ref(),
        Some(MarketCategory::Crypto),
        model_spec_fixtures::crypto_horizon_secs(),
    ))
    .await
}

pub struct ActivatedServing {
    pub runtime_registry: Arc<ModelServingRuntimeRegistry>,
    pub generations: Arc<ModelServingGenerationStore>,
}

pub async fn build_serving(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
) -> ActivatedServing {
    let evidence_scope = EvidenceScopeIdentity::from_config(
        &ClickHouseConfig::default(),
        &ArtifactStoreDeployConfig::default(),
    )
    .expect("F10 serving evidence scope");
    let runtime_registry = ModelServingRegistryFixture {
        db: db.clone(),
        artifact_store: Arc::clone(store),
        evidence_scope,
        evidence_attestor: None,
    }
    .build();
    let bundle = PgPolicyRepository::new(db.clone())
        .load_current_bundle()
        .await
        .expect("load activated F10 bundle")
        .expect("activated F10 bundle");
    let generations = Arc::new(
        ModelServingGenerationStore::bootstrap(
            Arc::new(PgModelRegistryRepository::new(db.clone()))
                as Arc<dyn ModelRegistryRepository>,
            Arc::clone(&runtime_registry),
            bundle,
        )
        .await
        .expect("bootstrap published F10 serving generation"),
    );
    ActivatedServing {
        runtime_registry,
        generations,
    }
}

pub async fn activate_generation(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    models: &ShadowModels,
) -> Arc<ModelServingGenerationStore> {
    let policy_repo = PgPolicyRepository::new(db.clone());
    let champion_id = models.champion.model_version_id;
    let candidate_id = models.candidate.model_version_id;
    let snapshot_id = activate_policy_bundle(
        &policy_repo,
        ConfigResourceKind::ModelRouting,
        "f10-shadow-stage",
        "publish exact active and shadow routes",
        move |snapshot| {
            snapshot.model_routing.model.buy_routes.insert(
                BuyModelRoute::Pooled,
                BuyRouteBinding {
                    champion: ModelBinding::new(
                        champion_id,
                        ModelBindingSource::Bootstrap,
                        Utc::now(),
                        PolicyBundleGeneration::FIRST,
                        1,
                    ),
                    shadow: Some(ModelBinding::new(
                        candidate_id,
                        ModelBindingSource::Feedback {
                            feedback_cycle_id: FeedbackCycleId::from_v7(),
                        },
                        Utc::now(),
                        PolicyBundleGeneration::FIRST,
                        2,
                    )),
                },
            );
        },
    )
    .await;
    let bundle = policy_repo
        .load_current_bundle()
        .await
        .expect("load activated F10 bundle")
        .expect("activated F10 bundle");
    assert_eq!(bundle.decision_policy_snapshot_id, snapshot_id);
    build_serving(db, store).await.generations
}

pub async fn activate_crypto_generation(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    models: &ShadowModels,
) -> ActivatedServing {
    let policy_repo = PgPolicyRepository::new(db.clone());
    let champion_id = models.champion.model_version_id;
    let snapshot_id = activate_policy_bundle(
        &policy_repo,
        ConfigResourceKind::ModelRouting,
        "p03-promotion-preflight",
        "publish exact Crypto champion before coordinator ShadowBind",
        move |snapshot| {
            snapshot.model_routing.model.buy_routes.insert(
                BuyModelRoute::Crypto,
                BuyRouteBinding {
                    champion: ModelBinding::new(
                        champion_id,
                        ModelBindingSource::Bootstrap,
                        Utc::now(),
                        PolicyBundleGeneration::FIRST,
                        1,
                    ),
                    shadow: None,
                },
            );
        },
    )
    .await;
    let bundle = policy_repo
        .load_current_bundle()
        .await
        .expect("load activated P03 bundle")
        .expect("activated P03 bundle");
    assert_eq!(bundle.decision_policy_snapshot_id, snapshot_id);
    build_serving(db, store).await
}

async fn record_prepared_cycle(
    db: &DatabaseConnection,
    models: &ShadowModels,
    schema: FeedbackSchemaFixture,
) -> (FeedbackSchemaFixture, FeedbackCycleClaim) {
    assert_eq!(schema.profile_ref, models.champion.profile_ref);
    let profile = schema
        .profile_ref
        .resolve_builtin_research_profile()
        .expect("resolve F10 profile");
    let source = &schema.candidate_family.shared_evaluation().source_lineage;
    let policy = PgPolicyRepository::new(db.clone())
        .load_current_bundle()
        .await
        .expect("load F10 policy bundle")
        .expect("F10 active policy bundle");
    let route =
        BuyModelRoute::try_from(models.champion.category_scope).expect("derive F10 champion route");
    let binding = policy
        .snapshot
        .model_routing
        .model
        .route_binding(route)
        .expect("load F10 route binding");
    let cycle = NewFeedbackCycle::try_seal(
        FeedbackCycleKey::try_new(FeedbackCycleKeyInput {
            profile_ref: schema.profile_ref.clone(),
            feedback_policy_hash: profile
                .spec
                .feedback_policy
                .content_hash()
                .expect("F10 feedback-policy hash"),
            label_cutoff: source.pit_cutoff,
            champion_model_version_id: models.champion.model_version_id,
            champion_serving_contract_hash: models.champion.serving_contract_hash,
            champion_model_spec_id: models.champion.model_spec_id,
            champion_model_spec_definition_hash: models.champion.model_spec_definition_hash,
            champion_model_family: models.champion.model_family,
            route,
            decision_policy_snapshot_id: policy.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: policy.snapshot_hash,
            policy_bundle_generation: policy.generation,
            route_generation: binding.champion.generation,
            evaluation_mode: FeedbackEvaluationMode::Conditional,
            parent_cycle_id: None,
            forced_idempotency_key: None,
        })
        .expect("freeze F10 cycle key"),
    )
    .expect("seal F10 cycle");
    let cycle_id = cycle.feedback_cycle_id();
    let cycles = PgFeedbackCycleRepository::new(db.clone());
    cycles
        .record_trigger(
            cycle,
            NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                feedback_cycle_id: cycle_id,
                event_sequence: 1,
                stage: FeedbackStage::Trigger,
                event_kind: FeedbackStageEventKind::Triggered,
                trigger_family: Some(FeedbackTriggerFamily::Manual),
                research_job_id: None,
                actor: Some("f10-shadow-stage".to_owned()),
                reason_code: Some("f10_contract".to_owned()),
                evidence_uri: None,
                evidence_hash: None,
                occurred_at: schema.observed_at,
            })
            .expect("seal F10 trigger event"),
        )
        .await
        .expect("record F10 trigger");
    let claim = cycles
        .claim_cycle(
            WorkerId::from_v7(),
            u64::try_from(JOB_LEASE_SECS).expect("lease seconds"),
        )
        .await
        .expect("claim F10 cycle")
        .expect("queued F10 cycle");
    assert_eq!(claim.cycle.feedback_cycle_id, cycle_id);
    (schema, claim)
}

pub async fn record_cycle(
    db: &DatabaseConnection,
    models: &ShadowModels,
) -> (FeedbackSchemaFixture, FeedbackCycleClaim) {
    let schema = Box::pin(prepare_fixture(db)).await;
    record_prepared_cycle(db, models, schema).await
}

pub async fn record_crypto_cycle(
    db: &DatabaseConnection,
    models: &ShadowModels,
) -> (FeedbackSchemaFixture, FeedbackCycleClaim) {
    let schema = Box::pin(prepare_profile_fixture(
        db,
        model_spec_fixtures::crypto_profile_ref(),
        model_spec_fixtures::crypto_horizon_secs(),
    ))
    .await;
    record_prepared_cycle(db, models, schema).await
}

pub async fn comparison_params(
    db: &DatabaseConnection,
    schema: &FeedbackSchemaFixture,
    claim: &FeedbackCycleClaim,
    models: &ShadowModels,
) -> FeedbackComparisonJobParams {
    let cycle = &claim.cycle;
    let previous = FeedbackLearningStageArtifactRef {
        feedback_cycle_id: cycle.feedback_cycle_id,
        stage: FeedbackStage::Cpcv,
        job_id: ResearchJobId::from_v7(),
        artifact_id: FeedbackLearningStageArtifactId::from_cycle_stage(
            cycle.feedback_cycle_id,
            FeedbackStage::Cpcv,
        )
        .expect("F10 CPCV artifact identity"),
        input_hash: content_hash('2'),
        artifact: ResearchJobArtifactRef {
            uri: ArtifactUri::parse("s3://f10-shadow-stage/cpcv.json").expect("F10 CPCV URI"),
            content_hash: content_hash('1'),
        },
    };
    let validation = FeedbackValidationArtifactRef {
        feedback_cycle_id: cycle.feedback_cycle_id,
        job_id: ResearchJobId::from_v7(),
        artifact_id: FeedbackValidationArtifactId::from_cycle_id(cycle.feedback_cycle_id),
        input_hash: content_hash('4'),
        cpcv: previous,
        artifact: ResearchJobArtifactRef {
            uri: ArtifactUri::parse("s3://f10-shadow-stage/validation.json")
                .expect("F10 Validation URI"),
            content_hash: content_hash('5'),
        },
    };
    comparison_params_with_validation(
        db,
        schema,
        claim,
        models,
        validation,
        BacktestPathSetId::from_v7(),
        content_hash('3'),
    )
    .await
}

pub async fn comparison_params_with_validation(
    db: &DatabaseConnection,
    schema: &FeedbackSchemaFixture,
    claim: &FeedbackCycleClaim,
    models: &ShadowModels,
    validation: FeedbackValidationArtifactRef,
    path_set_id: BacktestPathSetId,
    path_set_hash: ContentHash,
) -> FeedbackComparisonJobParams {
    let cycle = &claim.cycle;
    let previous = validation.cpcv.clone();
    let evaluation = NewFeedbackEvaluationUse::try_seal(FeedbackEvaluationUseInput {
        feedback_cycle_id: cycle.feedback_cycle_id,
        profile_ref: cycle.profile_ref.clone(),
        evaluation_dataset_id: schema.evaluation_dataset_id,
        evaluation_dataset_hash: schema.evaluation_dataset_hash,
        evaluation_artifact_bytes_hash: schema.evaluation_artifact_bytes_hash,
        cohort_manifest_hash: schema.cohort_manifest_hash,
        evaluation_window_start: schema.evaluation_window_start,
        evaluation_window_end: schema.evaluation_window_end,
        label_cutoff: schema.label_cutoff,
        champion_model_version_id: models.champion.model_version_id,
        champion_serving_contract_hash: models.champion.serving_contract_hash,
        candidate_family_hash: schema.candidate_family_hash,
        comparison_contract_hash: schema
            .candidate_family
            .comparison_contract()
            .comparison_contract_hash(),
        cpcv_artifact_uri: previous.artifact.uri.clone(),
        cpcv_artifact_hash: previous.artifact.content_hash,
    })
    .expect("seal F10 Evaluation reservation");
    let evaluation = match PgFeedbackCycleRepository::new(db.clone())
        .append_evaluation(claim.lease, evaluation)
        .await
        .expect("append F10 Evaluation reservation")
    {
        FeedbackEvaluationWriteOutcome::Inserted(info)
        | FeedbackEvaluationWriteOutcome::AlreadyPresent(info) => info,
    };
    let candidate_recipe_hash = schema.candidate_family.candidates()[0].candidate_recipe_hash();
    let artifact_id = FeedbackComparisonArtifactId::from_cycle_id(cycle.feedback_cycle_id);
    FeedbackComparisonJobParams::try_new(FeedbackComparisonJobInput {
        feedback_cycle_id: cycle.feedback_cycle_id,
        cycle_idempotency_hash: cycle.idempotency_hash,
        candidate_family_hash: schema.candidate_family_hash,
        validation,
        evaluation_use: FeedbackEvaluationUseRef::from(&evaluation),
        comparison_contract: schema.candidate_family.comparison_contract().clone(),
        decision_policy_snapshot_id: cycle.decision_policy_snapshot_id,
        champion_model_version_id: models.champion.model_version_id,
        champion_serving_contract_hash: models.champion.serving_contract_hash,
        candidates: vec![FeedbackComparisonCandidateRef {
            candidate_recipe_hash,
            model_version_id: models.candidate.model_version_id,
            serving_contract_hash: models.candidate.serving_contract_hash,
            path_set_id,
            path_set_hash,
            model_run_id: ModelRunId::from_feedback_comparison(
                artifact_id,
                models.candidate.model_version_id,
            ),
            backtest_report_id: BacktestReportId::from_feedback_comparison(
                artifact_id,
                models.candidate.model_version_id,
            ),
        }],
    })
    .expect("freeze F10 Comparison predecessor")
}

fn observations(
    window_start: DateTime<Utc>,
    value_bps: Decimal,
) -> Vec<PortfolioReturnObservation> {
    let capital = dec!(10000);
    (0..OBSERVATION_COUNT)
        .map(|index| PortfolioReturnObservation {
            decision_at: window_start
                + Duration::seconds(i64::try_from(index).expect("observation index")),
            realized_pnl_usd: Usd::new(
                value_bps
                    .checked_mul(capital)
                    .and_then(|amount| amount.checked_div(dec!(10000)))
                    .expect("F10 observation PnL"),
            ),
            capital_base_usd: Usd::new(capital),
            net_return_bps: Bps::new(value_bps),
        })
        .collect()
}

async fn persist_comparison(
    store: &Arc<dyn ArtifactStore>,
    params: &FeedbackComparisonJobParams,
    candidate_return_bps: Decimal,
) -> ResearchJobArtifactRef {
    let champion = observations(params.evaluation_use.evaluation_window_start, Decimal::ZERO);
    let candidate = observations(
        params.evaluation_use.evaluation_window_start,
        candidate_return_bps,
    );
    let outcome = RomanoWolfStepdown::evaluate(
        &params.comparison_contract,
        &champion,
        &[RomanoWolfCandidateInput {
            candidate_recipe_hash: params.candidates[0].candidate_recipe_hash,
            observations: &candidate,
        }],
    )
    .expect("evaluate F10 predecessor comparison");
    let RomanoWolfOutcome::Compared { evidence } = &outcome else {
        panic!("F10 predecessor must meet the governed observation floor");
    };
    assert_eq!(
        evidence.candidates[0].is_eligible(),
        candidate_return_bps > Decimal::ZERO
    );
    let artifact = FeedbackComparisonArtifact::try_seal(FeedbackComparisonArtifactInput {
        artifact_id: params.artifact_id,
        feedback_cycle_id: params.feedback_cycle_id,
        job_input_hash: params.input_hash().expect("F09 input hash"),
        candidate_family_hash: params.candidate_family_hash,
        comparison_contract: params.comparison_contract.clone(),
        evaluation_use: params.evaluation_use.clone(),
        champion_model_version_id: params.champion_model_version_id,
        champion_serving_contract_hash: params.champion_serving_contract_hash,
        champion_model_run_id: params.champion_model_run_id,
        champion_backtest_report_id: params.champion_backtest_report_id,
        champion_backtest_report_hash: content_hash('4'),
        champion_observation_hash: evidence.champion_observation_hash,
        candidate_replays: vec![FeedbackComparisonReplayRef {
            candidate_recipe_hash: params.candidates[0].candidate_recipe_hash,
            model_version_id: params.candidates[0].model_version_id,
            serving_contract_hash: params.candidates[0].serving_contract_hash,
            path_set_id: params.candidates[0].path_set_id,
            path_set_hash: params.candidates[0].path_set_hash,
            model_run_id: params.candidates[0].model_run_id,
            backtest_report_id: params.candidates[0].backtest_report_id,
            backtest_report_hash: content_hash('5'),
            observation_hash: evidence.candidates[0].observation_hash,
        }],
        outcome,
    })
    .expect("seal eligible F09 artifact");
    artifact
        .validate_for(params)
        .expect("validate eligible F09 artifact");
    let bytes = FeedbackComparisonCodec::encode(&artifact).expect("encode F09 artifact");
    let content_hash = FeedbackComparisonCodec::bytes_hash(&bytes);
    let uri = store
        .put(
            ArtifactKey::new(
                ArtifactNamespace::FeedbackComparison,
                content_hash.hex(),
                "json",
            )
            .expect("F09 artifact key"),
            &bytes,
        )
        .await
        .expect("persist F09 artifact");
    assert_eq!(store.get(&uri).await.expect("read F09 artifact"), bytes);
    ResearchJobArtifactRef { uri, content_hash }
}

pub async fn record_comparison(
    db: &DatabaseConnection,
    store: &Arc<dyn ArtifactStore>,
    claim: &FeedbackCycleClaim,
    params: FeedbackComparisonJobParams,
    candidate_return_bps: Decimal,
    event_sequence: i64,
) -> FeedbackComparisonArtifactRef {
    let identity = FeedbackStageJobIdentity::try_root(
        claim.cycle.feedback_cycle_id,
        FeedbackStage::Comparison,
    )
    .expect("F09 job identity");
    let job = NewResearchJob {
        job_id: identity.job_id(),
        feedback_cycle_id: None,
        feedback_stage: None,
        kind: ResearchJobKind::FeedbackComparison,
        status: ResearchJobStatus::Queued,
        model_spec_id: Some(claim.cycle.champion_model_spec_id),
        decision_policy_snapshot_id: Some(params.decision_policy_snapshot_id),
        params_json: ResearchJobParams::FeedbackComparison(Box::new(params.clone())),
        requested_by: None,
        acting_role: RoleCode::new("system"),
        parent_job_id: None,
        recovery_attempt: 0,
        max_recovery_attempts: 3,
    }
    .try_bind_feedback(identity)
    .expect("bind F09 job");
    let jobs = PgResearchJobRepository::new(db.clone());
    match jobs.enqueue(job).await.expect("enqueue F09 job") {
        ResearchJobEnqueueOutcome::Inserted(_) | ResearchJobEnqueueOutcome::AlreadyPresent(_) => {}
    }
    let worker = WorkerId::from_v7();
    jobs.lease_next(
        &[ResearchJobKind::FeedbackComparison],
        &worker,
        Utc::now() + Duration::seconds(JOB_LEASE_SECS),
    )
    .await
    .expect("lease F09 job")
    .expect("queued F09 job");
    let artifact = persist_comparison(store, &params, candidate_return_bps).await;
    let info = jobs
        .finalize(
            &identity.job_id(),
            &worker,
            ResearchJobFinalization::succeeded(
                Some(ResearchJobResultRef {
                    kind: ResearchJobResultKind::FeedbackComparisonArtifact,
                    id: params.artifact_id.as_uuid(),
                }),
                Some(artifact.clone()),
                None,
            ),
        )
        .await
        .expect("finalize F09 job");
    let reference = FeedbackComparisonArtifactRef {
        feedback_cycle_id: claim.cycle.feedback_cycle_id,
        job_id: identity.job_id(),
        artifact_id: params.artifact_id,
        input_hash: params.input_hash().expect("F09 input hash"),
        candidate_family_hash: params.candidate_family_hash,
        decision_policy_snapshot_id: params.decision_policy_snapshot_id,
        artifact: artifact.clone(),
    };
    PgFeedbackCycleRepository::new(db.clone())
        .append_stage(
            claim.lease,
            NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                feedback_cycle_id: claim.cycle.feedback_cycle_id,
                event_sequence,
                stage: FeedbackStage::Comparison,
                event_kind: FeedbackStageEventKind::Succeeded,
                trigger_family: None,
                research_job_id: Some(identity.job_id()),
                actor: None,
                reason_code: None,
                evidence_uri: Some(artifact.uri),
                evidence_hash: Some(artifact.content_hash),
                occurred_at: info.finished_at.expect("F09 terminal time"),
            })
            .expect("seal F09 success event"),
        )
        .await
        .expect("append F09 success event");
    reference
}

struct BindingFixtureInput<'a> {
    db: &'a DatabaseConnection,
    store: &'a Arc<dyn ArtifactStore>,
    cycles: &'a PgFeedbackCycleRepository,
    jobs: &'a PgResearchJobRepository,
    claim: &'a FeedbackCycleClaim,
    comparison: &'a FeedbackComparisonArtifactRef,
    candidate_recipe_hash: ContentHash,
    models: &'a ShadowModels,
    generations: &'a ModelServingGenerationStore,
    event_sequence: i64,
}

struct BindingArtifactFixture {
    params: ShadowBindingJobParams,
    artifact: ShadowBindingArtifact,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
}

impl BindingFixtureInput<'_> {
    async fn prepare(&self) -> BindingArtifactFixture {
        let published = self
            .generations
            .current_route(self.claim.cycle.route)
            .expect("ShadowBind fixture route exists")
            .published_shadow_identity()
            .expect("ShadowBind fixture route has a shadow");
        let bundle = PgPolicyRepository::new(self.db.clone())
            .load_current_bundle()
            .await
            .expect("load ShadowBind fixture policy")
            .expect("ShadowBind fixture policy exists");
        let committed_model_routing_revision_id = bundle
            .revision_vector
            .model_routing
            .expect("ShadowBind fixture ModelRouting revision");
        let expected_policy_generation = PolicyBundleGeneration::try_new(
            published
                .policy_bundle_generation
                .get()
                .checked_sub(1)
                .expect("ShadowBind fixture has a predecessor policy generation"),
        )
        .expect("valid ShadowBind predecessor policy generation");
        let expected_snapshot_hash = content_hash('f');
        let expected_model_routing_revision_id = PolicyRevisionId::from_v7();
        assert_ne!(
            expected_model_routing_revision_id,
            committed_model_routing_revision_id
        );
        let params = ShadowBindingJobParams::try_new(ShadowBindingJobInput {
            feedback_cycle_id: self.claim.cycle.feedback_cycle_id,
            cycle_idempotency_hash: self.claim.cycle.idempotency_hash,
            prepared_at: published.shadow_bound_at - Duration::milliseconds(1),
            profile_ref: self.claim.cycle.profile_ref.clone(),
            route: self.claim.cycle.route,
            comparison: self.comparison.clone(),
            candidate_recipe_hash: self.candidate_recipe_hash,
            champion_model_version_id: self.models.champion.model_version_id,
            champion_serving_contract_hash: self.models.champion.serving_contract_hash,
            candidate_model_version_id: self.models.candidate.model_version_id,
            candidate_artifact_hash: self.models.candidate.artifact_hash,
            candidate_serving_contract_hash: self.models.candidate.serving_contract_hash,
            candidate_manifest_id: ModelCandidateManifestId::from_v7(),
            candidate_manifest_hash: content_hash('c'),
            candidate_training_dataset_id: self
                .models
                .candidate
                .training_dataset_id
                .expect("ShadowBind candidate Training Dataset"),
            expected_policy_generation,
            expected_snapshot_id: DecisionPolicySnapshotId::from_content_hash(
                &expected_snapshot_hash,
            ),
            expected_snapshot_hash,
            expected_model_routing_revision_id,
            expected_route_generation: published
                .route_generation
                .checked_sub(1)
                .expect("ShadowBind fixture has a predecessor route generation"),
            reserved_model_bytes: 16 * 1024 * 1024,
            total_shadow_model_budget_bytes: SHADOW_MODEL_BUDGET_BYTES,
        })
        .expect("seal ShadowBind fixture params");
        let receipt = ShadowBindingReceipt::try_seal(ShadowBindingReceiptInput {
            params: params.clone(),
            bound_at: published.shadow_bound_at,
            binding_generation: published.route_generation,
            committed_policy_generation: published.policy_bundle_generation,
            committed_snapshot_id: published.decision_policy_snapshot_id,
            committed_snapshot_hash: published.decision_policy_snapshot_hash,
            committed_model_routing_revision_id,
            policy_activation_id: PolicyActivationId::from_v7(),
            audit_event_id: AuditEventId::from_v7(),
        })
        .expect("seal ShadowBind fixture receipt");
        let artifact = ShadowBindingArtifact::try_seal(&params, receipt)
            .expect("seal ShadowBind fixture artifact");
        BindingArtifactFixture {
            params,
            artifact,
            decision_policy_snapshot_id: published.decision_policy_snapshot_id,
        }
    }

    async fn persist(&self, fixture: &BindingArtifactFixture) -> ResearchJobArtifactRef {
        let bytes =
            ShadowBindingCodec::encode(&fixture.artifact).expect("encode ShadowBind fixture");
        let artifact_hash = CanonicalDigest::content_hash_bytes(&bytes);
        let uri = self
            .store
            .put(
                ArtifactKey::new(
                    ArtifactNamespace::FeedbackShadowBinding,
                    fixture.artifact.artifact_id.to_string(),
                    "json",
                )
                .expect("ShadowBind fixture key"),
                &bytes,
            )
            .await
            .expect("persist ShadowBind fixture");
        ResearchJobArtifactRef {
            uri,
            content_hash: artifact_hash,
        }
    }

    async fn record(&self, fixture: BindingArtifactFixture, artifact_ref: ResearchJobArtifactRef) {
        let identity = FeedbackStageJobIdentity::try_root(
            self.claim.cycle.feedback_cycle_id,
            FeedbackStage::ShadowBind,
        )
        .expect("ShadowBind fixture identity");
        let job = NewResearchJob {
            job_id: identity.job_id(),
            feedback_cycle_id: None,
            feedback_stage: None,
            kind: ResearchJobKind::FeedbackShadowBind,
            status: ResearchJobStatus::Queued,
            model_spec_id: Some(self.claim.cycle.champion_model_spec_id),
            decision_policy_snapshot_id: Some(fixture.decision_policy_snapshot_id),
            params_json: ResearchJobParams::FeedbackShadowBind(Box::new(fixture.params)),
            requested_by: None,
            acting_role: RoleCode::new("system"),
            parent_job_id: None,
            recovery_attempt: 0,
            max_recovery_attempts: 3,
        }
        .try_bind_feedback(identity)
        .expect("bind ShadowBind fixture job");
        match self
            .jobs
            .enqueue(job)
            .await
            .expect("enqueue ShadowBind fixture")
        {
            ResearchJobEnqueueOutcome::Inserted(_)
            | ResearchJobEnqueueOutcome::AlreadyPresent(_) => {}
        }
        let worker = WorkerId::from_v7();
        let leased = self
            .jobs
            .lease_next(
                &[ResearchJobKind::FeedbackShadowBind],
                &worker,
                Utc::now() + Duration::seconds(JOB_LEASE_SECS),
            )
            .await
            .expect("lease ShadowBind fixture")
            .expect("queued ShadowBind fixture");
        assert_eq!(leased.job_id, identity.job_id());
        let info = self
            .jobs
            .finalize(
                &identity.job_id(),
                &worker,
                ResearchJobFinalization::succeeded(
                    Some(ResearchJobResultRef {
                        kind: ResearchJobResultKind::ShadowBindingArtifact,
                        id: fixture.artifact.artifact_id.as_uuid(),
                    }),
                    Some(artifact_ref.clone()),
                    None,
                ),
            )
            .await
            .expect("finalize ShadowBind fixture");
        self.cycles
            .append_stage(
                self.claim.lease,
                NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                    feedback_cycle_id: self.claim.cycle.feedback_cycle_id,
                    event_sequence: self.event_sequence,
                    stage: FeedbackStage::ShadowBind,
                    event_kind: FeedbackStageEventKind::Succeeded,
                    trigger_family: None,
                    research_job_id: Some(identity.job_id()),
                    actor: None,
                    reason_code: None,
                    evidence_uri: Some(artifact_ref.uri),
                    evidence_hash: Some(artifact_ref.content_hash),
                    occurred_at: info.finished_at.expect("ShadowBind fixture terminal time"),
                })
                .expect("seal ShadowBind fixture event"),
            )
            .await
            .expect("append ShadowBind fixture event");
    }

    async fn execute(&self) {
        let fixture = self.prepare().await;
        let artifact_ref = self.persist(&fixture).await;
        self.record(fixture, artifact_ref).await;
    }
}

pub async fn insert_observation(
    db: &DatabaseConnection,
    cycles: &PgFeedbackCycleRepository,
    generations: &Arc<ModelServingGenerationStore>,
    route: BuyModelRoute,
) {
    let identity = generations
        .current_route(route)
        .expect("current route")
        .published_shadow_identity()
        .expect("published F10 shadow identity");
    let decision_at = cycles.database_time().await.expect("F10 decision clock");
    PgShadowComparisonRepository::new(db.clone())
        .create(shadow_observation(
            &identity,
            decision_at,
            content_hash('6'),
        ))
        .await
        .expect("persist exact production shadow observation");
}

fn shadow_observation(
    identity: &PublishedShadowRouteIdentity,
    decision_at: DateTime<Utc>,
    comparison_hash: ContentHash,
) -> NewShadowComparison {
    NewShadowComparison {
        shadow_comparison_id: ShadowComparisonId::from_v7(),
        champion_model_version_id: identity.champion_model_version_id,
        candidate_model_version_id: identity.candidate_model_version_id,
        champion_serving_contract_hash: identity.champion_serving_contract_hash,
        candidate_serving_contract_hash: identity.candidate_serving_contract_hash,
        research_profile_artifact_id: identity.research_profile_artifact_id.clone(),
        category_scope: identity.category_scope,
        decision_policy_snapshot_id: identity.decision_policy_snapshot_id,
        decision_policy_snapshot_hash: identity.decision_policy_snapshot_hash,
        policy_bundle_generation: identity.policy_bundle_generation,
        weight_source: ModelWeightSource::Artifact,
        decision_at,
        topn_decision_overlap: Probability::new(dec!(0.90)),
        rank_delta_json: ShadowRankDelta {
            mean_abs_rank_delta: dec!(0.1),
            max_rank_delta: 1,
            spearman: dec!(0.95),
            common_markets: 10,
        },
        score_delta_json: ShadowScoreDelta {
            mean_abs_score_delta: dec!(0.01),
            max_score_delta: dec!(0.02),
            side_disagreement_rate: Decimal::ZERO,
        },
        matured_outcome_json: None,
        hard_divergence: false,
        comparison_hash,
    }
}

pub async fn insert_stable_observations(
    db: &DatabaseConnection,
    generations: &Arc<ModelServingGenerationStore>,
) {
    let identity = generations
        .current_route(BuyModelRoute::Crypto)
        .expect("current Crypto route")
        .published_shadow_identity()
        .expect("published P03 shadow identity");
    let profile = identity
        .research_profile_artifact_id
        .profile_ref()
        .resolve_builtin_research_profile()
        .expect("resolve P03 shadow profile");
    assert_eq!(
        profile.spec.feedback_policy.shadow_minimum_observations,
        u64::try_from(SHADOW_OBSERVATION_COUNT).expect("shadow observation count")
    );
    assert_eq!(identity.required_shadow_window_secs, 2);
    let window_start = identity.shadow_bound_at;
    tokio::time::sleep(StdDuration::from_millis(1_100)).await;
    let rows = (0..SHADOW_OBSERVATION_COUNT).map(|index| {
        let offset_micros = i64::try_from(index).expect("shadow index") * 1_000_000
            / i64::try_from(SHADOW_OBSERVATION_COUNT - 1).expect("shadow observation denominator");
        let decision_at = window_start + Duration::microseconds(offset_micros);
        let comparison_hash = ResearchHasher::canonical(&(
            "p03-stable-published-shadow-v1",
            identity.policy_bundle_generation,
            index,
        ))
        .expect("P03 stable comparison hash");
        shadow_observation(&identity, decision_at, comparison_hash).into_active_model()
    });
    ShadowComparisonEntity::insert_many(rows)
        .exec(db)
        .await
        .expect("persist complete P03 production shadow window");
    tokio::time::sleep(StdDuration::from_millis(1_100)).await;
}

pub async fn terminal_restart_tamper() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let artifact_root = ArtifactRoot::create();
    let store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(artifact_root.path.clone()));
    let models = Box::pin(build_models(&db, &store)).await;
    let generations = activate_generation(&db, &store, &models).await;
    let (schema, claim) = record_cycle(&db, &models).await;
    let cycles = Arc::new(PgFeedbackCycleRepository::new(db.clone()));
    let jobs = Arc::new(PgResearchJobRepository::new(db.clone()));
    persist_recipe_plan_fixture(
        cycles.as_ref(),
        jobs.as_ref(),
        &store,
        claim.lease,
        &claim.cycle,
        &schema.candidate_family,
        2,
    )
    .await;
    let comparison = comparison_params(&db, &schema, &claim, &models).await;
    let comparison = record_comparison(&db, &store, &claim, comparison, dec!(100), 3).await;
    BindingFixtureInput {
        db: &db,
        store: &store,
        cycles: cycles.as_ref(),
        jobs: jobs.as_ref(),
        claim: &claim,
        comparison: &comparison,
        candidate_recipe_hash: schema.candidate_family.candidates()[0].candidate_recipe_hash(),
        models: &models,
        generations: generations.as_ref(),
        event_sequence: 4,
    }
    .execute()
    .await;

    insert_observation(&db, cycles.as_ref(), &generations, BuyModelRoute::Pooled).await;
    let stage = shadow_stage(
        &db,
        &cycles,
        &jobs,
        Arc::clone(&store),
        Arc::clone(&generations),
    );
    let identity =
        FeedbackStageJobIdentity::try_root(claim.cycle.feedback_cycle_id, FeedbackStage::Shadow)
            .expect("F10 job identity");
    let first_preparation = stage
        .prepare_shadow(&claim.cycle, claim.lease, identity)
        .await
        .expect("defer immature F10 window");
    let FeedbackStagePreparation::Deferred {
        resume_after,
        reason_code,
    } = first_preparation
    else {
        panic!("immature F10 fixture must defer its shadow window");
    };
    assert_eq!(reason_code, "feedback_shadow_window_pending");
    let database_time = cycles.database_time().await.expect("F10 database clock");
    let remaining = resume_after
        .signed_duration_since(database_time)
        .to_std()
        .expect("F10 resume boundary remains in the future");
    tokio::time::sleep(remaining + StdDuration::from_millis(10)).await;
    let preparation = stage
        .prepare_shadow(&claim.cycle, claim.lease, identity)
        .await
        .expect("prepare mature F10 job");
    let FeedbackStagePreparation::Ready(job) = preparation else {
        panic!("mature F10 fixture must not defer its shadow window");
    };
    let params = match &job.params_json {
        ResearchJobParams::FeedbackShadow(params) => params.as_ref().clone(),
        _ => panic!("F10 stage emitted another job kind"),
    };
    let FeedbackShadowSubject::Candidate { contract, .. } = &params.subject else {
        panic!("eligible F09 result must produce a candidate shadow subject");
    };
    assert_eq!(
        contract.candidate_model_version_id(),
        models.candidate.model_version_id
    );
    match jobs.enqueue(*job).await.expect("enqueue F10 job") {
        ResearchJobEnqueueOutcome::Inserted(_) | ResearchJobEnqueueOutcome::AlreadyPresent(_) => {}
    }
    let worker = WorkerId::from_v7();
    jobs.lease_next(
        &[ResearchJobKind::FeedbackShadow],
        &worker,
        Utc::now() + Duration::seconds(JOB_LEASE_SECS),
    )
    .await
    .expect("lease F10 job")
    .expect("queued F10 job");
    let executor = FeedbackShadowExecutionService::new(FeedbackShadowExecutionDeps {
        observations: Arc::new(PgShadowComparisonRepository::new(db.clone())),
        artifacts: Arc::clone(&store),
    });
    let result = executor
        .execute(params, Arc::new(NoopProgressSink), CancellationToken::new())
        .await
        .expect("execute F10 against exact PostgreSQL observations");
    let bytes = store
        .get(&result.artifact.uri)
        .await
        .expect("read F10 terminal artifact");
    let artifact = FeedbackShadowCodec::decode(&bytes).expect("decode F10 artifact");
    assert!(matches!(
        artifact.outcome(),
        FeedbackShadowOutcome::InsufficientObservations { observed: 1, .. }
    ));
    let info = jobs
        .finalize(
            &identity.job_id(),
            &worker,
            ResearchJobFinalization::succeeded(
                Some(ResearchJobResultRef {
                    kind: ResearchJobResultKind::FeedbackShadowArtifact,
                    id: result.artifact_id.as_uuid(),
                }),
                Some(result.artifact.clone()),
                None,
            ),
        )
        .await
        .expect("finalize F10 job");
    let first = stage
        .succeeded_shadow(&claim.cycle, &info)
        .await
        .expect("verify F10 terminal artifact");
    let restarted = stage
        .succeeded_shadow(&claim.cycle, &info)
        .await
        .expect("verify F10 artifact after restart");
    assert_eq!(first, restarted);

    let tampered_store: Arc<dyn ArtifactStore> = Arc::new(ReadTamperArtifactStoreFixture::new(
        Arc::clone(&store),
        result.artifact.uri,
        b"{}".to_vec(),
    ));
    shadow_stage(&db, &cycles, &jobs, tampered_store, generations)
        .succeeded_shadow(&claim.cycle, &info)
        .await
        .expect_err("F10 restart must reject tampered object bytes");
}
