//! Model-governance lifecycle system contracts.
//!
//! Exercises the publish / rollback / dataset-promotion orchestration end to end
//! against real repositories + the default quality gate: gate-pass and
//! shadow-stability enforcement, published-version immutability, rollback
//! restoration, runtime-config pointer sync, and the `InsufficientLabels`
//! promotion block.

use std::{collections::BTreeMap, env, future::Future, pin::Pin, sync::Arc};

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use quant_pivot_core::{
    governance::{CoreCalibrationArtifactLoader, ModelGovernanceDeps, ModelGovernanceService},
    runtime_config::DecisionPolicyStore,
    service::{
        feature_integrity::RepositoryFeatureParityGate,
        frozen_model_parity::{FrozenModelParityDeps, FrozenModelParityService},
        model_serving_preimage::{ModelServingPreimageDeps, ModelServingPreimageService},
        research_readiness::{EvidenceScopeIdentity, ResearchReadinessEvidenceService},
        trade_policy_evidence::{TradePolicyEvidenceVerifier, TradePolicyEvidenceVerifierDeps},
        trade_policy_preimage::{TradePolicyPreimageVerifier, TradePolicyPreimageVerifierDeps},
        training_dataset,
    },
};
use quant_pivot_error::{QuantError, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::QuantFeatureParityEventRow,
    config::{ArtifactStoreDeployConfig, ClickHouseConfig},
    domain::{
        api::BindCalibrationRequest,
        data_plane::DecisionClock,
        ports::{GovernanceActor, ModelGovernancePort, PublishModelCommand, RetireModelCommand},
        quant::{
            ModelGovernanceAuditDetail, NewBacktestPathSet, NewBacktestPathSetInput,
            NewBacktestReport, NewModelRun, NewModelVersion, NewShadowComparison,
        },
    },
    entities::quant_model_version::Entity as ModelVersionEntity,
    enums::{
        common::MarketCategory,
        factor::FactorFamily,
        model::ModelFamily,
        quant::{
            DataQualityStatus, DatasetPurpose, DownsideSource, FactorDirection, ModelRunKind,
            ModelWeightSource, PublicationStatus, TrainingDatasetStatus,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::{DecisionPolicySnapshot, ModelVersionRef},
    types::{
        ArtifactUri, BacktestPathSetId, BacktestReportId, ContentHash, DatasetCoverage,
        DecisionPolicySnapshotId, EventId, FactorDefinitionId, MarketId, ModelInputContract,
        ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId, Probability,
        ResearchEvaluationTrack, SchemaVersion, ShadowComparisonId, TokenId, TrainingDatasetId,
        TrainingExampleId, TrainingSampleSource, TrainingSampleSources,
        backtest::{
            BacktestPath, CategoryMetrics, CpcvFoldArtifact, CpcvFoldArtifacts,
            CpcvFoldCalibrationPolicy, CpcvFoldRole, CpcvMethodologyBinding, CpcvPathSetSubject,
            ExpectedVsRealized, PnlSimulation, SharpeDistribution,
        },
        factor::{FactorExplanation, FactorServingPlane},
        model_lineage::ModelVersionDerivation,
        model_metrics::{
            GovernedSellEstimatorMetrics, ModelArtifactTrainingLineage, ModelVersionMetrics,
        },
        model_quality::GateId,
        model_training::{GovernedSellFitStatus, ModelTrainingObjective},
        shadow::{ShadowRankDelta, ShadowScoreDelta},
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestPathSetRepository, PgBacktestReportRepository, PgCalibrationArtifactRepository,
        PgFeatureParityRepository, PgModelGovernanceAuditRepository, PgModelRegistryRepository,
        PgModelRunRepository, PgPolicyRepository, PgResearchReadinessEvidenceRepository,
        PgShadowComparisonRepository, PgSourceSliceRepository, PgTradePolicyRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        BacktestPathSetRepository, BacktestReportRepository, CalibrationArtifactRepository,
        FeatureParityRepository, ModelGovernanceAuditRepository, ModelRegistryRepository,
        ModelRunRepository, PolicyRepository, ShadowComparisonRepository, TradePolicyRepository,
        TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
    factors::{
        FactorEngine, FactorValue, FrozenReferenceQuantiles, NormalizedFactor,
        names::LIQUIDITY_DEPTH,
    },
    features::{FeatureSchema, FeatureVector},
    gates::{DefaultModelQualityGate, ModelQualityGate},
    hashing::ResearchHasher,
    model::{
        CalibratedReturnModel, CalibrationArtifactLoader, LabelSelector, ModelArtifact,
        ReturnModelSpec, artifact::ModelPayload, weighted_training_input_hash,
    },
    selection::SelectedMarket,
    training::{
        DatasetHashContract, DatasetParquetCodec, LabelName, TrainingDatasetArtifact,
        TrainingExample, TrainingLabel, dataset_source_fingerprint, label_names_for_sources,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        execution_pg_seed::seed_score_calibration,
        fact_sink::DiscardFactWriter,
        model_serving_fixtures::{
            ModelArtifactFixtureSeed, ModelBindingFixture, ModelPayloadFixture, SealedModelFixture,
        },
        model_spec_fixtures,
        policy_fixtures::{bootstrap_default_policy_bundle, bootstrap_policy_bundle},
        research_fixtures::{
            DatasetLedgerFixture, DatasetLedgerSeed, EvaluationDatasetSeed,
            ReplayableSourceSliceFixture, bind_fixture_decision_capture,
            persist_replayable_source_slice, seed_evaluation_dataset, seed_source_manifest,
        },
    },
};
use rust_decimal_macros::dec;
use sea_orm::{ActiveModelTrait, ActiveValue::Set, DatabaseConnection, EntityTrait};

/// Test harness wiring governance against a real store + config repo.
struct GovernanceHarness {
    service: ModelGovernanceService,
    store: Arc<DecisionPolicyStore>,
    artifact_store: Arc<dyn ArtifactStore>,
}

fn content_hash(seed: u32) -> ContentHash {
    let pair = format!("{seed:02x}");
    let hex: String = pair.chars().cycle().take(64).collect();
    ContentHash::parse(&format!("blake3:{hex}")).expect("hash")
}

async fn harness(db: &DatabaseConnection, config: DecisionPolicySnapshot) -> GovernanceHarness {
    let store = Arc::new(DecisionPolicyStore::new(config.clone()));
    bootstrap_policy_bundle(
        &PgPolicyRepository::new(db.clone()),
        &config,
        "governance-it",
        "governance integration test bootstrap",
    )
    .await;

    let artifact_store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(env::temp_dir().join(format!(
            "qp_governance_e2e_{}_{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ))));

    let gate: Arc<dyn ModelQualityGate> = Arc::new(DefaultModelQualityGate::new());
    let calibration_repo: Arc<dyn CalibrationArtifactRepository> =
        Arc::new(PgCalibrationArtifactRepository::new(db.clone()));
    let calibration_loader: Arc<dyn CalibrationArtifactLoader> = Arc::new(
        CoreCalibrationArtifactLoader::new(Arc::clone(&calibration_repo)),
    );
    let model_registry_repo: Arc<dyn ModelRegistryRepository> =
        Arc::new(PgModelRegistryRepository::new(db.clone()));
    let dataset_repo: Arc<dyn TrainingDatasetRepository> =
        Arc::new(PgTrainingDatasetRepository::new(db.clone()));
    let parity_repo: Arc<dyn FeatureParityRepository> =
        Arc::new(PgFeatureParityRepository::new(db.clone()));
    let frozen_model_parity = Arc::new(FrozenModelParityService::new(FrozenModelParityDeps {
        dataset_repo: Arc::clone(&dataset_repo),
        model_registry_repo: Arc::clone(&model_registry_repo),
        parity_repo: Arc::clone(&parity_repo),
        artifact_store: Arc::clone(&artifact_store),
        evidence_writer: Arc::new(DiscardFactWriter::<QuantFeatureParityEventRow>::new()),
    }));
    let evidence_scope = EvidenceScopeIdentity::from_config(
        &ClickHouseConfig::default(),
        &ArtifactStoreDeployConfig::default(),
    )
    .expect("governance evidence scope");
    let readiness = Arc::new(
        ResearchReadinessEvidenceService::new(
            Arc::new(PgResearchReadinessEvidenceRepository::new(db.clone())),
            Arc::clone(&artifact_store),
            None,
            &evidence_scope,
        )
        .expect("governance readiness verifier"),
    );
    let trade_policy_repo: Arc<dyn TradePolicyRepository> =
        Arc::new(PgTradePolicyRepository::new(db.clone()));
    let trade_policy_evidence = Arc::new(TradePolicyEvidenceVerifier::new(
        TradePolicyEvidenceVerifierDeps {
            artifacts: Arc::clone(&artifact_store),
            policies: Arc::clone(&trade_policy_repo),
            readiness,
        },
    ));
    let trade_policy_preimages = Arc::new(TradePolicyPreimageVerifier::new(
        TradePolicyPreimageVerifierDeps {
            trade_policy_repo,
            dataset_repo: Arc::clone(&dataset_repo),
            model_registry_repo: Arc::clone(&model_registry_repo),
            evidence: trade_policy_evidence,
        },
    ));
    let serving_preimages = Arc::new(ModelServingPreimageService::new(ModelServingPreimageDeps {
        model_registry_repo: Arc::clone(&model_registry_repo),
        dataset_repo: Arc::clone(&dataset_repo),
        source_slice_repo: Arc::new(PgSourceSliceRepository::new(db.clone())),
        policy_repo: Arc::new(PgPolicyRepository::new(db.clone())),
        calibration_repo: Arc::clone(&calibration_repo),
        trade_policy_preimages,
        artifact_store: Arc::clone(&artifact_store),
    }));
    let service = ModelGovernanceService::new(ModelGovernanceDeps {
        model_registry_repo,
        backtest_report_repo: Arc::new(PgBacktestReportRepository::new(db.clone())),
        backtest_path_set_repo: Arc::new(PgBacktestPathSetRepository::new(db.clone())),
        shadow_comparison_repo: Arc::new(PgShadowComparisonRepository::new(db.clone())),
        governance_audit_repo: Arc::new(PgModelGovernanceAuditRepository::new(db.clone())),
        dataset_repo,
        serving_preimages,
        artifact_store: Arc::clone(&artifact_store),
        calibration_repo,
        calibration_loader,
        gate,
        runtime_config: Arc::clone(&store),
        feature_parity_gate: Arc::new(RepositoryFeatureParityGate::new(parity_repo)),
        frozen_model_parity,
    });
    GovernanceHarness {
        service,
        store,
        artifact_store,
    }
}

async fn seed_runtime_config(db: &DatabaseConnection) -> DecisionPolicySnapshotId {
    bootstrap_default_policy_bundle(db, "governance-it", "integration test").await
}

async fn seed_spec(db: &DatabaseConnection) -> ModelSpecId {
    let id = ModelSpecId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            id,
            "governance-it",
            ModelFamily::WeightedFactor,
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        ))
        .await
        .expect("model spec");
    id
}

/// A dataset coverage that clears the gate with exact materialized-row accounting.
fn healthy_coverage() -> DatasetCoverage {
    DatasetCoverage {
        planned_samples: 505,
        built_examples: 500,
        markets: 50,
        labels_available: 500,
        labels_not_mature: 0,
        labels_unavailable: 0,
        samples_dropped_insufficient: 5,
        book_decode_failures: 0,
        matrix_probe: None,
        ..DatasetCoverage::default()
    }
}

/// A dataset coverage that clears the Sell-side gates:
/// `exit_decision_built` covers `min_sample_count`, and the L2/fallback row
/// split clears `SellL2BookFidelity`/`SellFallbackRatio`.
fn healthy_sell_coverage() -> DatasetCoverage {
    DatasetCoverage {
        exit_decision_built: 500,
        exit_fill_l2_rows: 450,
        exit_fill_fallback_rows: 50,
        ..healthy_coverage()
    }
}

fn healthy_training_examples(as_of: DateTime<Utc>) -> Vec<TrainingExample> {
    let as_of = DateTime::from_timestamp_millis(as_of.timestamp_millis())
        .expect("governance fixture timestamp is millisecond-aligned");
    (0..500)
        .map(|index| {
            let score = if index % 2 == 0 {
                dec!(0.25)
            } else {
                dec!(0.75)
            };
            let market_id = MarketId::new(format!("0xhealthy{index}"));
            let token_id = TokenId::new((index * 2 + 1).to_string());
            let secondary_token_id = TokenId::new((index * 2 + 2).to_string());
            let mut example = leaking_training_example();
            example.example_id = TrainingExampleId::from_v7();
            example.market_id = market_id.clone();
            example.token_id = token_id.clone();
            example.selected_market.market_id = market_id.clone();
            example.selected_market.primary_token_id = token_id.clone();
            example.selected_market.secondary_token_id = Some(secondary_token_id);
            example.feature_vector.market_id = market_id;
            example.feature_vector.token_id = Some(token_id);
            example.feature_vector.decision_at = as_of;
            example.decision_boundary = DecisionClock::new(10)
                .boundary(as_of)
                .expect("healthy decision boundary");
            example.factor_values = vec![FactorValue {
                definition_id: FactorDefinitionId::from_v7(),
                name: LIQUIDITY_DEPTH,
                family: FactorFamily::Liquidity,
                raw_value: Some(score),
                normalization: NormalizedFactor::cross_section(Probability::new(score)),
                direction: FactorDirection::Positive,
                confidence: Probability::new(dec!(1)),
                explanation: FactorExplanation {
                    headline: "healthy fixture".to_owned(),
                    drivers: Vec::new(),
                },
                input_feature_refs: Vec::new(),
            }];
            example.labels[0].value = score;
            example.labels[0].matured_at = as_of + ChronoDuration::seconds(1);
            example.source_refs.clear();
            bind_fixture_decision_capture(&mut example);
            example
        })
        .collect()
}

fn bind_factor_plane(examples: &mut [TrainingExample], plane: &FactorServingPlane) {
    for example in examples {
        let strength = example
            .factor_values
            .first()
            .and_then(|factor| factor.raw_value)
            .or_else(|| example.labels.first().map(|label| label.value))
            .unwrap_or(dec!(0.5));
        example.factor_values = plane
            .definitions()
            .iter()
            .map(|revision| {
                let definition = revision.definition();
                FactorValue {
                    definition_id: revision.factor_definition_id(),
                    name: definition.name.clone(),
                    family: definition.family,
                    raw_value: Some(strength),
                    normalization: NormalizedFactor::cross_section(Probability::new(strength)),
                    direction: definition
                        .contribution_direction(strength)
                        .expect("governance fixture strength projects a contribution direction"),
                    confidence: Probability::ONE,
                    explanation: FactorExplanation {
                        headline: format!("{} governed fixture score", definition.name),
                        drivers: Vec::new(),
                    },
                    input_feature_refs: definition.input_features.clone(),
                }
            })
            .collect();
    }
}

struct FrozenDatasetSeed {
    model_spec_id: ModelSpecId,
    model_family: ModelFamily,
    model_spec_definition_hash: ContentHash,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    examples: Vec<TrainingExample>,
    knowledge_lag_secs: u64,
}

struct FrozenDatasetFixture {
    id: TrainingDatasetId,
    ledger: DatasetLedgerFixture,
    artifact_bytes_hash: ContentHash,
    parquet_uri: ArtifactUri,
}

impl FrozenDatasetFixture {
    async fn persist(
        self,
        repository: &PgTrainingDatasetRepository,
        coverage: DatasetCoverage,
    ) -> TrainingDatasetId {
        repository
            .create_plan(self.ledger.plan.clone())
            .await
            .expect("dataset plan");
        repository
            .start_build(&self.id)
            .await
            .expect("start dataset build");
        let completion = self
            .ledger
            .completion(
                TrainingDatasetStatus::Ready,
                self.artifact_bytes_hash,
                self.parquet_uri,
                coverage,
                None,
            )
            .expect("dataset completion");
        repository
            .complete_build(&self.id, completion)
            .await
            .expect("complete dataset");
        self.id
    }
}

async fn build_frozen_dataset_fixture(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    seed: FrozenDatasetSeed,
) -> FrozenDatasetFixture {
    let FrozenDatasetSeed {
        model_spec_id,
        model_family,
        model_spec_definition_hash,
        decision_policy_snapshot_id,
        mut examples,
        knowledge_lag_secs,
    } = seed;
    let id = TrainingDatasetId::from_v7();
    let window_start = examples
        .iter()
        .map(TrainingExample::decision_at)
        .min()
        .expect("dataset examples")
        - ChronoDuration::minutes(1);
    let window_end = examples
        .iter()
        .map(TrainingExample::decision_at)
        .max()
        .expect("dataset examples")
        + ChronoDuration::hours(1);
    let policy = PgPolicyRepository::new(db.clone())
        .load_snapshot(&decision_policy_snapshot_id)
        .await
        .expect("dataset policy lookup")
        .expect("dataset policy");
    let features = &policy.snapshot.profile_artifacts.features.definition;
    let scoring = &policy.snapshot.profile_artifacts.scoring.definition;
    let domain = &policy.snapshot.profile_artifacts.domain.definition;
    let feature_schema_hash = ResearchHasher::feature_schema(
        &FeatureSchema::build(features).expect("governance feature schema"),
    )
    .expect("governance feature schema hash");
    let factor_serving_plane = FactorEngine::for_model_scope(scoring, features, domain, None, None)
        .serving_plane()
        .expect("governance factor plane")
        .clone();
    bind_factor_plane(&mut examples, &factor_serving_plane);
    let sample_sources = TrainingSampleSources::default();
    let label_schema_hash =
        ResearchHasher::label_schema(&label_names_for_sources(sample_sources.as_slice(), false))
            .expect("governance label schema hash");
    let dataset_hash = TrainingDatasetArtifact::compute_dataset_hash(
        DatasetHashContract {
            model_spec_id: &model_spec_id,
            model_family,
            window_start,
            window_end,
            purpose: DatasetPurpose::Training,
            feature_schema_hash: &feature_schema_hash,
            factor_serving_plane: &factor_serving_plane,
            label_schema_hash: &label_schema_hash,
        },
        &examples,
    )
    .expect("semantic dataset hash");
    let profile_ref = model_spec_fixtures::pooled_profile_ref();
    let runtime_config_hash = policy.snapshot_hash;
    let research_program_hash = CanonicalDigest::content_hash_json(&(
        "governance-dataset-program-v1",
        model_spec_id,
        model_spec_definition_hash,
        factor_serving_plane.factor_schema_hash(),
    ))
    .expect("dataset research program hash");
    let stored_source = persist_replayable_source_slice(
        artifact_store,
        &examples,
        ReplayableSourceSliceFixture {
            profile_ref,
            evaluation_track: ResearchEvaluationTrack::ResearchOnly,
            research_program_hash,
            decision_policy_snapshot_id,
            runtime_config_hash,
            window_start,
            window_end: window_end + ChronoDuration::hours(1),
        },
    )
    .await
    .expect("persist replayable Source Slice");
    let source_lineage = seed_source_manifest(db, &stored_source)
        .await
        .expect("dataset source lineage");
    let sample_count = u64::try_from(examples.len()).expect("sample count");
    let ledger = DatasetLedgerFixture::try_new(DatasetLedgerSeed {
        training_dataset_id: id,
        model_spec_id,
        model_family,
        model_spec_definition_hash,
        factor_serving_plane,
        source_lineage,
        cohort_manifest: None,
        window_start,
        window_end,
        purpose: DatasetPurpose::Training,
        knowledge_lag_secs,
        sample_interval_secs: 3_600,
        horizons_secs: vec![
            0,
            u64::try_from(model_spec_fixtures::pooled_horizon_secs()).expect("pooled horizon"),
        ],
        feature_schema_version: SchemaVersion::FIRST,
        sample_sources: Some(sample_sources),
        feature_schema_hash,
        label_schema_hash,
        semantic_dataset_hash: dataset_hash,
        source_fingerprint: dataset_source_fingerprint(&examples).expect("source fingerprint"),
        sample_count,
    })
    .expect("dataset ledger fixture");
    // Real Parquet so publish-time leakage rescan (#9) can decode bytes.
    let bytes = DatasetParquetCodec::encode(&examples, &ledger.manifest).expect("encode parquet");
    let artifact_bytes_hash = CanonicalDigest::content_hash_bytes(&bytes);
    let key = ArtifactKey::new(
        ArtifactNamespace::Dataset,
        id.as_uuid().simple().to_string(),
        "parquet",
    )
    .expect("key");
    let parquet_uri = artifact_store
        .put(key, &bytes)
        .await
        .expect("store parquet");

    FrozenDatasetFixture {
        id,
        ledger,
        artifact_bytes_hash,
        parquet_uri,
    }
}

async fn seed_dataset(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    spec: &ModelSpecId,
    rc_id: &DecisionPolicySnapshotId,
    coverage: DatasetCoverage,
) -> TrainingDatasetId {
    let model_spec = PgModelRegistryRepository::new(db.clone())
        .find_model_spec(spec)
        .await
        .expect("model spec lookup")
        .expect("model spec");
    let fixture = build_frozen_dataset_fixture(
        db,
        artifact_store,
        FrozenDatasetSeed {
            model_spec_id: *spec,
            model_family: model_spec.model_family,
            model_spec_definition_hash: model_spec.definition_hash,
            decision_policy_snapshot_id: *rc_id,
            examples: healthy_training_examples(Utc::now() - ChronoDuration::days(4)),
            knowledge_lag_secs: 10,
        },
    )
    .await;
    let dataset_repo = PgTrainingDatasetRepository::new(db.clone());
    fixture.persist(&dataset_repo, coverage).await
}

fn seed_version<'a>(
    db: &'a DatabaseConnection,
    artifact_store: &'a Arc<dyn ArtifactStore>,
    spec: &'a ModelSpecId,
    version: i32,
    dataset: TrainingDatasetId,
    calibrated: bool,
    dataset_projection: DatasetProjection,
) -> Pin<Box<dyn Future<Output = ModelVersionId> + Send + 'a>> {
    Box::pin(seed_version_inner(
        db,
        artifact_store,
        spec,
        version,
        dataset,
        calibrated,
        dataset_projection,
    ))
}

async fn seed_version_inner(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    spec: &ModelSpecId,
    version: i32,
    dataset: TrainingDatasetId,
    calibrated: bool,
    dataset_projection: DatasetProjection,
) -> ModelVersionId {
    let source_id = ModelVersionId::from_v7();
    let dataset_info = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&dataset)
        .await
        .expect("dataset lookup")
        .expect("dataset row");
    let materialization = dataset_info
        .materialization()
        .expect("dataset materialization");
    let bytes = artifact_store
        .get(materialization.parquet_uri)
        .await
        .expect("dataset bytes");
    let examples = training_dataset::verify_frozen_dataset_artifact(&dataset_info, &bytes)
        .expect("frozen dataset integrity");
    let policy = PgPolicyRepository::new(db.clone())
        .load_snapshot(&dataset_info.decision_policy_snapshot_id)
        .await
        .expect("policy lookup")
        .expect("policy row");
    let input_contract = PgModelRegistryRepository::new(db.clone())
        .find_model_spec(spec)
        .await
        .expect("model spec lookup")
        .expect("model spec")
        .input_contract;
    let scoring = &policy.snapshot.profile_artifacts.scoring.definition;
    let factor_names = materialization
        .factor_serving_plane
        .definitions()
        .iter()
        .filter(|revision| !revision.definition().is_diagnostic())
        .map(|revision| revision.factor_name().clone())
        .collect::<Vec<_>>();
    let training_input_hash = weighted_training_input_hash(
        &examples,
        &LabelSelector {
            name: LabelName::new("token_payout_ratio"),
            horizon_secs: 0,
        },
        &factor_names,
        &FrozenReferenceQuantiles::empty(),
        Some(&scoring.cross_section),
    )
    .expect("weighted training input hash");
    let source_payload = ModelPayloadFixture::weighted(
        materialization.factor_serving_plane,
        &scoring.factor_head,
        input_contract.clone(),
        ReturnModelSpec::heuristic_default(),
        scoring.cross_section.clone(),
    )
    .expect("weighted model payload");
    let source_fixture = SealedModelFixture::seal(
        db,
        ModelArtifactFixtureSeed {
            model_version_id: source_id,
            training_dataset_id: dataset,
            payload: source_payload,
            training_input_hash,
            category_scope: None,
            calibration: None,
            bias_table: None,
        },
    )
    .await
    .expect("seal weighted model fixture");
    source_fixture
        .store(artifact_store)
        .await
        .expect("store weighted model fixture");
    let evidence = CandidateVersionInput {
        metrics: ModelVersionMetrics::not_measured("test fixture"),
        objective: ModelTrainingObjective::hand_authored("test fixture"),
    };
    let source_version = candidate_version(
        &source_fixture,
        *spec,
        version,
        CandidateVersionInput {
            metrics: evidence.metrics.clone(),
            objective: evidence.objective.clone(),
        },
    );
    persist_dataset_projection(db, source_version, dataset_projection).await;
    if !calibrated {
        return source_id;
    }

    let calibrator_ref = Box::pin(seed_score_calibration(db, artifact_store, &source_id)).await;
    let calibration_repo = PgCalibrationArtifactRepository::new(db.clone());
    calibration_repo
        .mark_active(&calibrator_ref)
        .await
        .expect("activate calibrated fixture");
    let calibration = calibration_repo
        .find_by_id(&calibrator_ref)
        .await
        .expect("calibration lookup")
        .expect("calibration row");
    let child_id = ModelVersionId::from_v7();
    let child_payload = ModelPayloadFixture::weighted(
        materialization.factor_serving_plane,
        &scoring.factor_head,
        input_contract,
        ReturnModelSpec::Calibrated(CalibratedReturnModel {
            calibrator_ref,
            downside_source: DownsideSource::MfeMae,
        }),
        scoring.cross_section.clone(),
    )
    .expect("calibrated weighted model payload");
    let child_fixture = SealedModelFixture::seal(
        db,
        ModelArtifactFixtureSeed {
            model_version_id: child_id,
            training_dataset_id: dataset,
            payload: child_payload,
            training_input_hash,
            category_scope: None,
            calibration: Some(ModelBindingFixture::score_calibration(
                calibrator_ref,
                calibration.content_hash,
            )),
            bias_table: None,
        },
    )
    .await
    .expect("seal calibrated model fixture");
    child_fixture
        .store(artifact_store)
        .await
        .expect("store calibrated model fixture");
    let mut child_version = candidate_version(
        &child_fixture,
        *spec,
        version.checked_add(1).expect("derived model version"),
        evidence,
    );
    child_version.derivation = ModelVersionDerivation::ReturnCalibration {
        parent_model_version_id: source_id,
        calibration_artifact_id: calibrator_ref,
    };
    PgModelRegistryRepository::new(db.clone())
        .create_model_version(child_version)
        .await
        .expect("calibrated model version");
    child_id
}

async fn persist_dataset_projection(
    db: &DatabaseConnection,
    source_version: NewModelVersion,
    projection: DatasetProjection,
) {
    match projection {
        DatasetProjection::Exact => {
            PgModelRegistryRepository::new(db.clone())
                .create_model_version(source_version)
                .await
                .expect("model version");
        }
        DatasetProjection::Missing => {
            let mut active = source_version
                .try_into_active_model()
                .expect("exact model version fixture");
            active.training_dataset_id = Set(None);
            active
                .insert(db)
                .await
                .expect("insert deliberately incomplete dataset projection");
        }
    }
}

#[derive(Clone, Copy)]
enum DatasetProjection {
    Exact,
    Missing,
}

struct CandidateVersionInput {
    metrics: ModelVersionMetrics,
    objective: ModelTrainingObjective,
}

fn candidate_version(
    fixture: &SealedModelFixture,
    model_spec_id: ModelSpecId,
    version: i32,
    input: CandidateVersionInput,
) -> NewModelVersion {
    let serving_contract = fixture.serving_contract().clone();
    let bindings = serving_contract.bindings();
    let model_version_id = bindings.model.model_version_id;
    let category_scope = bindings.model.category_scope;
    let profile_ref = bindings.model.profile_ref.clone();
    let training_dataset_id = bindings.dataset.manifest.training_dataset_id;
    let trade_policy = bindings
        .trade_policy
        .as_ref()
        .map(|binding| (binding.artifact_id, binding.content_hash));
    NewModelVersion {
        model_version_id,
        model_spec_id,
        version,
        artifact_hash: fixture.artifact_hash(),
        serving_contract,
        category_scope,
        profile_ref,
        training_dataset_id: Some(training_dataset_id),
        trade_policy_artifact_id: trade_policy.map(|binding| binding.0),
        trade_policy_hash: trade_policy.map(|binding| binding.1),
        publish_path_set_id: None,
        derivation: NewModelVersion::training_derivation(),
        metrics: input.metrics,
        training_objective: input.objective,
        quality_gate_report: None,
        publication_status: PublicationStatus::Candidate,
        published_at: None,
        retired_at: None,
    }
}

/// A `HoldVsExitWeighted` model spec for sell-side governance tests.
async fn seed_sell_spec(db: &DatabaseConnection) -> ModelSpecId {
    let id = ModelSpecId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            id,
            "governance-it-sell",
            ModelFamily::HoldVsExitWeighted,
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        ))
        .await
        .expect("sell model spec");
    id
}

/// A `HoldVsExitWeighted` candidate version with a validated sealed Sell
/// payload. Sell scorers never carry a return model, so unlike
/// [`seed_version`] there is no calibrator parameter.
async fn seed_sell_version(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    spec: &ModelSpecId,
    version: i32,
    dataset: TrainingDatasetId,
) -> ModelVersionId {
    let id = ModelVersionId::from_v7();
    let dataset_info = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&dataset)
        .await
        .expect("dataset lookup")
        .expect("dataset row");
    let materialization = dataset_info
        .materialization()
        .expect("dataset materialization");
    let bytes = artifact_store
        .get(materialization.parquet_uri)
        .await
        .expect("dataset bytes");
    let examples = training_dataset::verify_frozen_dataset_artifact(&dataset_info, &bytes)
        .expect("frozen dataset integrity");
    let policy = PgPolicyRepository::new(db.clone())
        .load_snapshot(&dataset_info.decision_policy_snapshot_id)
        .await
        .expect("policy lookup")
        .expect("policy row");
    let input_contract = PgModelRegistryRepository::new(db.clone())
        .find_model_spec(spec)
        .await
        .expect("sell model spec lookup")
        .expect("sell model spec")
        .input_contract;
    let scoring = &policy.snapshot.profile_artifacts.scoring.definition;
    let factor_inputs = materialization
        .factor_serving_plane
        .definitions()
        .iter()
        .filter(|revision| !revision.definition().is_diagnostic())
        .map(|revision| revision.factor_name().clone())
        .collect::<Vec<_>>();
    let training_input_hash = weighted_training_input_hash(
        &examples,
        &LabelSelector {
            name: LabelName::new("token_payout_ratio"),
            horizon_secs: 0,
        },
        &factor_inputs,
        &FrozenReferenceQuantiles::empty(),
        Some(&scoring.cross_section),
    )
    .expect("sell training input hash");
    let payload = ModelPayloadFixture::sell(
        materialization.factor_serving_plane,
        &scoring.factor_head,
        &scoring.sell_scorer,
        input_contract,
        scoring.cross_section.clone(),
    )
    .expect("sell model payload");
    let fixture = SealedModelFixture::seal(
        db,
        ModelArtifactFixtureSeed {
            model_version_id: id,
            training_dataset_id: dataset,
            payload,
            training_input_hash,
            category_scope: None,
            calibration: None,
            bias_table: None,
        },
    )
    .await
    .expect("seal sell model fixture");
    fixture
        .store(artifact_store)
        .await
        .expect("store sell model fixture");
    let bindings = fixture.serving_contract().bindings();
    let artifact_lineage = ModelArtifactTrainingLineage::FactorNative {
        training_dataset_hash: bindings.transform.training_dataset_hash,
        training_input_hash: bindings.transform.training_input_hash,
        input_contract_hash: bindings.transform.input_contract_hash,
        input_transform_hash: bindings.transform.input_transform_hash,
        factor_inputs,
    };
    let fit_status = GovernedSellFitStatus::OofPredictionsRequired;
    PgModelRegistryRepository::new(db.clone())
        .create_model_version(candidate_version(
            &fixture,
            *spec,
            version,
            CandidateVersionInput {
                metrics: ModelVersionMetrics::governed_sell(
                    GovernedSellEstimatorMetrics {
                        resolved_label_rows: u64::try_from(examples.len())
                            .expect("sell label rows"),
                        position_state_rows: u64::try_from(examples.len())
                            .expect("sell state rows"),
                        fit_status,
                    },
                    artifact_lineage,
                ),
                objective: ModelTrainingObjective::governed_sell(fit_status),
            },
        ))
        .await
        .expect("sell model version");
    id
}

async fn seed_backtest(
    db: &DatabaseConnection,
    version: &ModelVersionId,
    rc_id: &DecisionPolicySnapshotId,
    hash_seed: char,
) {
    let registry = PgModelRegistryRepository::new(db.clone());
    let version_info = registry
        .find_model_version(version)
        .await
        .expect("model version lookup")
        .expect("model version");
    let model_spec = registry
        .find_model_spec(&version_info.model_spec_id)
        .await
        .expect("model spec lookup")
        .expect("model spec");
    let model_run_id = ModelRunId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);
    let window_end = window_start + ChronoDuration::hours(1);
    let evaluation_dataset_id = seed_evaluation_dataset(
        db,
        EvaluationDatasetSeed {
            scope: format!("governance-backtest-{version}-{hash_seed}"),
            source_training_dataset_id: version_info
                .training_dataset_id
                .expect("governance model version training Dataset"),
            model_spec_id: version_info.model_spec_id,
            model_spec_definition_hash: model_spec.definition_hash,
            profile_ref: version_info.profile_ref,
            decision_policy_snapshot_id: *rc_id,
            window_start,
            window_end,
            sample_count: 1_000,
        },
    )
    .await
    .expect("evaluation dataset");
    let evaluation_dataset = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&evaluation_dataset_id)
        .await
        .expect("evaluation dataset lookup")
        .expect("evaluation dataset row");
    let evaluation_hash = *evaluation_dataset
        .materialization()
        .expect("evaluation dataset materialization")
        .dataset_hash;
    let model_run_repo = PgModelRunRepository::new(db.clone());
    model_run_repo
        .create(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::Backtest,
            model_version_id: Some(*version),
            decision_policy_snapshot_id: *rc_id,
            market_selection_id: None,
            window_start,
            window_end,
            input_hash: evaluation_hash,
        })
        .await
        .expect("model run");

    let expected_vs_realized = ExpectedVsRealized {
        mean_expected_bps: dec!(120),
        mean_realized_bps: dec!(110),
        correlation: dec!(0.4),
        bias_bps: dec!(10),
    };
    let pnl_simulation = PnlSimulation {
        total_allocated_usd: dec!(10000),
        realized_pnl_usd: dec!(500),
        gross_return: dec!(0.05),
        pnl_curve: Vec::new(),
    };

    let mut report = NewBacktestReport {
        backtest_report_id: BacktestReportId::from_v7(),
        model_version_id: *version,
        evaluation_dataset_id,
        model_run_id,
        decision_policy_snapshot_id: *rc_id,
        window_start,
        window_end,
        coverage: dec!(0.99),
        sample_count: 1_000,
        missing_feature_count: 0,
        rank_ic: dec!(0.15),
        sharpe: dec!(1.1),
        hit_rate: Probability::new(dec!(0.62)),
        expected_vs_realized,
        max_drawdown: dec!(0.10),
        turnover: dec!(0.2),
        liquidity_feasibility: Probability::new(dec!(0.95)),
        category_breakdown: CategoryMetrics::default(),
        tail_loss: dec!(-50),
        report_pnl_simulation: pnl_simulation,
        report_hash: content_hash(u32::from(hash_seed) + 2),
        parquet_uri: None,
    };
    report.report_hash = report
        .recomputed_hash()
        .expect("canonical governance backtest report hash");
    let report_hash = report.report_hash;
    PgBacktestReportRepository::new(db.clone())
        .create(report)
        .await
        .expect("backtest report");
    model_run_repo
        .succeed(&model_run_id, report_hash, Some(*version))
        .await
        .expect("finalize backtest model run");
}

/// Seed a stable shadow history for `version` (as shadow) spanning > 24h.
async fn seed_shadow_window(
    db: &DatabaseConnection,
    active: &ModelVersionId,
    shadow: &ModelVersionId,
    seed_base: char,
) {
    let now = Utc::now();
    let repo = PgShadowComparisonRepository::new(db.clone());
    for (overlap, hours_ago, offset) in [(dec!(0.82), 25_i64, 0_u32), (dec!(0.80), 1, 1)] {
        repo.create(NewShadowComparison {
            shadow_comparison_id: ShadowComparisonId::from_v7(),
            active_model_version_id: *active,
            shadow_model_version_id: *shadow,
            weight_source: ModelWeightSource::Artifact,
            decision_at: now - ChronoDuration::hours(hours_ago),
            topn_overlap: Probability::new(overlap),
            rank_delta_json: ShadowRankDelta {
                mean_abs_rank_delta: dec!(0.1),
                max_rank_delta: 1,
                spearman: dec!(0.95),
                common_markets: 10,
            },
            score_delta_json: ShadowScoreDelta {
                mean_abs_score_delta: dec!(0.02),
                max_score_delta: dec!(0.04),
                side_disagreement_rate: dec!(0),
            },
            matured_outcome_json: None,
            hard_divergence: false,
            comparison_hash: content_hash((u32::from(seed_base) << 8) | offset),
        })
        .await
        .expect("shadow comparison");
    }
}

pub async fn publish_requires_quality_pass() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db, DecisionPolicySnapshot::default()).await;
    // Frozen dataset present so leakage rescan can run; no backtest → gate
    // evaluates and fails (BacktestRequired / risk gates), then persists evidence.
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        healthy_coverage(),
    )
    .await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        1,
        dataset,
        false,
        DatasetProjection::Exact,
    )
    .await;

    let result = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate,
                reason: "attempt".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await;
    assert!(result.is_err(), "publish must fail without a passing gate");

    let registry = PgModelRegistryRepository::new(db.clone());
    let row = registry
        .find_model_version(&candidate)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(
        row.publication_status,
        PublicationStatus::Candidate,
        "a blocked publish leaves the version a candidate"
    );
    // The gate evaluation is persisted as durable evidence even on failure.
    assert!(
        row.quality_gate_report
            .as_ref()
            .is_some_and(|report| !report.passed),
        "failed publish must persist quality_gate_report.passed=false"
    );
}

pub async fn publish_without_training_transition() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db, DecisionPolicySnapshot::default()).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        healthy_coverage(),
    )
    .await;
    // Bypass the typed insert only to persist a deliberately incomplete scalar
    // projection. The sealed contract and artifact remain exact; the
    // repository read boundary must reject the corrupt row before governance
    // gate evaluation.
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        1,
        dataset,
        false,
        DatasetProjection::Missing,
    )
    .await;

    let result = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate,
                reason: "attempt".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await;
    let err = result.expect_err("publish must refuse versions without a training dataset");
    assert!(
        matches!(
            err,
            QuantError::Storage(StorageError::InvariantViolation {
                entity: Some("quant_model_version"),
                ref detail,
            }) if detail.contains("training_dataset_id")
        ),
        "expected a model-version invariant violation naming the missing training_dataset_id"
    );

    let row = ModelVersionEntity::find_by_id(candidate)
        .one(&db)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(row.publication_status, PublicationStatus::Candidate);
    assert!(
        row.quality_gate_report.is_none(),
        "pre-gate repository rejection must not invent a quality_gate_report"
    );
}

pub async fn publish_requires_backtest_report() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db, DecisionPolicySnapshot::default()).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        healthy_coverage(),
    )
    .await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        1,
        dataset,
        false,
        DatasetProjection::Exact,
    )
    .await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;
    // Deliberately no backtest report.

    let result = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate,
                reason: "attempt".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await;
    assert!(
        result.is_err(),
        "publish must fail without a backtest report"
    );
    let err = result.expect_err("publish error");
    assert!(
        err.to_string().contains("BacktestRequired") || err.to_string().contains("backtest"),
        "expected backtest-required failure, got: {err}"
    );
}

pub async fn publish_requires_shadow_stability() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db, DecisionPolicySnapshot::default()).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        healthy_coverage(),
    )
    .await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        1,
        dataset,
        false,
        DatasetProjection::Exact,
    )
    .await;
    Box::pin(seed_backtest(&db, &candidate, &rc_id, '1')).await;
    // No shadow comparisons → shadow stability not established.

    let result = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate,
                reason: "attempt".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await;
    assert!(
        result.is_err(),
        "publish must fail without shadow stability"
    );
}

pub async fn publish_succeeds_without_immutable() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db, DecisionPolicySnapshot::default()).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        healthy_coverage(),
    )
    .await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        1,
        dataset,
        true,
        DatasetProjection::Exact,
    )
    .await;
    Box::pin(seed_backtest(&db, &candidate, &rc_id, '1')).await;
    seed_path_set(&db, &candidate, &rc_id, '1').await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;

    let published = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate,
                reason: "first publish".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await
        .expect("publish should pass every gate");
    assert_eq!(published.publication_status, PublicationStatus::Published);

    // An audit row was written.
    let audits = PgModelGovernanceAuditRepository::new(db.clone())
        .list_by_version(&candidate)
        .await
        .expect("audits");
    assert_eq!(audits.len(), 1);
    assert!(matches!(
        audits[0].detail,
        ModelGovernanceAuditDetail::Publish { .. }
    ));

    assert!(
        harness
            .store
            .current()
            .model_routing
            .model
            .active_model_version_id
            .is_none(),
        "artifact publication must not bypass ModelRouting governance"
    );
    assert!(
        harness
            .store
            .current()
            .model_routing
            .model
            .shadow_model_version_id
            .is_none(),
        "artifact publication must not mutate the shadow slot"
    );

    // Re-publishing a published version is rejected (immutability).
    let again = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate,
                reason: "again".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await;
    assert!(again.is_err(), "a published version cannot be re-published");
}

/// Inputs to [`publish_ready_version`].
struct PublishReadyParams<'a> {
    governance: &'a ModelGovernanceService,
    db: &'a DatabaseConnection,
    rc_id: &'a DecisionPolicySnapshotId,
    version: &'a ModelVersionId,
    active_for_shadow: &'a ModelVersionId,
    backtest_seed: char,
    shadow_seed: char,
    reason: &'a str,
}

/// Publish a fully gated candidate (backtest + CPCV path set + shadow + publish).
async fn publish_ready_version(params: PublishReadyParams<'_>) {
    Box::pin(seed_backtest(
        params.db,
        params.version,
        params.rc_id,
        params.backtest_seed,
    ))
    .await;
    seed_path_set(
        params.db,
        params.version,
        params.rc_id,
        params.backtest_seed,
    )
    .await;
    seed_shadow_window(
        params.db,
        params.active_for_shadow,
        params.version,
        params.shadow_seed,
    )
    .await;
    params
        .governance
        .publish(
            PublishModelCommand {
                model_version_id: *params.version,
                reason: params.reason.to_owned(),
            },
            GovernanceActor::system(),
        )
        .await
        .expect("publish ready version");
}

/// Seed a CPCV path set that clears the alpha hard gates.
async fn seed_path_set(
    db: &DatabaseConnection,
    version: &ModelVersionId,
    rc_id: &DecisionPolicySnapshotId,
    hash_seed: char,
) {
    let model_run_id = ModelRunId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);

    // Dataset id is required by the ledger FK; reuse any Ready dataset linked
    // to this version when present, otherwise mint a synthetic id that the
    // path-set row alone references (no FK to training_dataset in schema).
    let training_dataset_id = PgModelRegistryRepository::new(db.clone())
        .find_model_version(version)
        .await
        .expect("version")
        .and_then(|v| v.training_dataset_id)
        .unwrap_or_else(TrainingDatasetId::from_v7);

    let path_set_id = BacktestPathSetId::from_v7();
    let new_path_set = NewBacktestPathSet::try_seal(NewBacktestPathSetInput {
        path_set_id,
        model_version_id: *version,
        model_run_id,
        training_dataset_id,
        decision_policy_snapshot_id: *rc_id,
        window_start,
        window_end: window_start + ChronoDuration::hours(1),
        subject: CpcvPathSetSubject::new(
            content_hash(u32::from(hash_seed) + 20),
            content_hash(u32::from(hash_seed) + 21),
            content_hash(u32::from(hash_seed) + 22),
            content_hash(u32::from(hash_seed) + 23),
            content_hash(u32::from(hash_seed) + 24),
            content_hash(u32::from(hash_seed) + 25),
        ),
        methodology: CpcvMethodologyBinding::new(
            content_hash(u32::from(hash_seed) + 26),
            content_hash(u32::from(hash_seed) + 27),
            content_hash(u32::from(hash_seed) + 28),
            CpcvFoldCalibrationPolicy::SubjectHeuristic {
                return_model_hash: content_hash(u32::from(hash_seed) + 29),
            },
        ),
        fold_artifacts: CpcvFoldArtifacts::try_new(vec![
            CpcvFoldArtifact {
                role: CpcvFoldRole::Validation,
                training_groups_hash: content_hash(u32::from(hash_seed) + 30),
                training_group_count: 2,
                model_artifact_hash: content_hash(u32::from(hash_seed) + 31),
                serving_contract_hash: content_hash(u32::from(hash_seed) + 32),
                model_payload_hash: content_hash(u32::from(hash_seed) + 33),
            },
            CpcvFoldArtifact {
                role: CpcvFoldRole::Trial { trial_id: 0 },
                training_groups_hash: content_hash(u32::from(hash_seed) + 34),
                training_group_count: 3,
                model_artifact_hash: content_hash(u32::from(hash_seed) + 35),
                serving_contract_hash: content_hash(u32::from(hash_seed) + 36),
                model_payload_hash: content_hash(u32::from(hash_seed) + 37),
            },
        ])
        .expect("fold artifacts"),
        path_count: 1,
        combination_count: 1,
        median_rank_ic: dec!(0.15),
        sharpe_distribution: SharpeDistribution {
            min: dec!(0.5),
            p25: dec!(0.8),
            median: dec!(1.0),
            p75: dec!(1.2),
            max: dec!(1.5),
            median_max_drawdown: Some(dec!(0.10)),
            median_tail_loss: Some(dec!(-0.005)),
            baseline_uplift: Some(dec!(0.001)),
        },
        paths: vec![BacktestPath {
            path_index: 0,
            group_returns: vec![dec!(0.01), dec!(0.02)],
            sharpe: dec!(1.0),
            rank_ic: dec!(0.15),
            max_drawdown: dec!(0.10),
            tail_loss: dec!(-0.005),
        }]
        .into(),
        deflated_sharpe: dec!(0.97),
        dsr_benchmark_sharpe: dec!(0.5),
        pbo: dec!(0.20),
        min_track_record_length_secs: Some(86_400),
        trial_count: 1,
        trial_grid_count: 1,
        coord_search_effective_n: 2,
    })
    .expect("seal path set");
    let runs = PgModelRunRepository::new(db.clone());
    runs.create(NewModelRun {
        model_run_id,
        run_kind: ModelRunKind::Cpcv,
        model_version_id: Some(*version),
        decision_policy_snapshot_id: *rc_id,
        market_selection_id: None,
        window_start,
        window_end: window_start + ChronoDuration::hours(1),
        input_hash: content_hash(u32::from(hash_seed) + 10),
    })
    .await
    .expect("create cpcv model run");
    let path_set_hash = new_path_set.path_set_hash();
    PgBacktestPathSetRepository::new(db.clone())
        .create(new_path_set)
        .await
        .expect("path set");
    runs.succeed(&model_run_id, path_set_hash, None)
        .await
        .expect("finish cpcv model run");
    // Publish gates require an explicit bind — never implicit "latest".
    PgModelRegistryRepository::new(db.clone())
        .set_publish_path(version, Some(path_set_id))
        .await
        .expect("bind publish path set");
}

pub async fn retire_unrouted_without_routing() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db, DecisionPolicySnapshot::default()).await;

    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        healthy_coverage(),
    )
    .await;
    let version = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        1,
        dataset,
        true,
        DatasetProjection::Exact,
    )
    .await;
    Box::pin(publish_ready_version(PublishReadyParams {
        governance: &harness.service,
        db: &db,
        rc_id: &rc_id,
        version: &version,
        active_for_shadow: &version,
        backtest_seed: '1',
        shadow_seed: 'a',
        reason: "publish for retire",
    }))
    .await;

    let retired = harness
        .service
        .retire(
            RetireModelCommand {
                model_version_id: version,
                reason: "decommission".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await
        .expect("retire");
    assert_eq!(retired.publication_status, PublicationStatus::Retired);
    assert!(
        harness
            .store
            .current()
            .model_routing
            .model
            .active_model_version_id
            .is_none(),
        "retirement must not mutate ModelRouting"
    );

    let audits = PgModelGovernanceAuditRepository::new(db.clone())
        .list_by_version(&version)
        .await
        .expect("audits");
    assert!(
        audits.iter().any(|audit| audit.reason == "decommission"),
        "the retire reason is audited"
    );
}

pub async fn rejects_routed_model_retirement() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db, DecisionPolicySnapshot::default()).await;

    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        healthy_coverage(),
    )
    .await;
    let version = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        1,
        dataset,
        true,
        DatasetProjection::Exact,
    )
    .await;
    Box::pin(publish_ready_version(PublishReadyParams {
        governance: &harness.service,
        db: &db,
        rc_id: &rc_id,
        version: &version,
        active_for_shadow: &version,
        backtest_seed: '1',
        shadow_seed: 'a',
        reason: "publish for category-pointer retire",
    }))
    .await;

    // Simulate an operator having pinned this same version to a category
    // route (independent of the generic active pointer publish already set).
    let mut config = (*harness.store.current()).clone();
    config
        .model_routing
        .model
        .category_model_pointers
        .insert(MarketCategory::Crypto, ModelVersionRef::new(version));
    let routed_harness = self::harness(&db, config).await;
    assert!(
        routed_harness
            .store
            .current()
            .model_routing
            .model
            .category_model_pointers
            .contains_key(&MarketCategory::Crypto),
        "precondition: category pointer is armed before retire"
    );

    let error = routed_harness
        .service
        .retire(
            RetireModelCommand {
                model_version_id: version,
                reason: "decommission with category pointer".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await
        .expect_err("routed model retirement must fail closed");
    assert!(
        error
            .to_string()
            .contains("activate a ModelRouting revision")
    );

    assert!(
        routed_harness
            .store
            .current()
            .model_routing
            .model
            .active_model_version_id
            .is_none(),
        "failed retirement must not invent a generic route"
    );
    assert!(
        routed_harness
            .store
            .current()
            .model_routing
            .model
            .category_model_pointers
            .contains_key(&MarketCategory::Crypto),
        "failed retirement must preserve the governed category route"
    );
    let row = PgModelRegistryRepository::new(db)
        .find_model_version(&version)
        .await
        .expect("model lookup")
        .expect("model version");
    assert_eq!(row.publication_status, PublicationStatus::Published);
}

pub async fn uncalibrated_return_cannot_publish() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db, DecisionPolicySnapshot::default()).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        healthy_coverage(),
    )
    .await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        1,
        dataset,
        false,
        DatasetProjection::Exact,
    )
    .await;
    Box::pin(seed_backtest(&db, &candidate, &rc_id, '1')).await;
    seed_path_set(&db, &candidate, &rc_id, '1').await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;

    let result = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate,
                reason: "uncalibrated attempt".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await;
    assert!(result.is_err(), "heuristic return model must block publish");
    let registry = PgModelRegistryRepository::new(db.clone());
    let row = registry
        .find_model_version(&candidate)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(row.publication_status, PublicationStatus::Candidate);
    let gate = row
        .quality_gate_report
        .expect("failed publish persists gate report");
    assert!(
        gate.hard_failures
            .iter()
            .any(|failure| failure.gate == GateId::CalibrationRequired),
        "publish gate must record CalibrationRequired failure"
    );
}

pub async fn bind_calibration_creates_model() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db, DecisionPolicySnapshot::default()).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        healthy_coverage(),
    )
    .await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        1,
        dataset,
        false,
        DatasetProjection::Exact,
    )
    .await;
    let calibrator = Box::pin(seed_score_calibration(
        &db,
        &harness.artifact_store,
        &candidate,
    ))
    .await;

    let bound = harness
        .service
        .bind_calibration(
            &candidate,
            BindCalibrationRequest {
                calibrator_ref: calibrator,
                downside_source: DownsideSource::MfeMae,
                reason: "bind test".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await
        .expect("bind calibration");

    assert_ne!(bound.model_version_id, candidate);
    assert_eq!(bound.publication_status, PublicationStatus::Candidate);
    let bytes = harness
        .artifact_store
        .get_by_key(&ModelArtifact::artifact_key(&bound.artifact_hash).expect("artifact key"))
        .await
        .expect("artifact bytes");
    let artifact = ModelArtifact::from_bytes(&bytes).expect("decode");
    let ModelPayload::WeightedFactor(weighted) = artifact.payload() else {
        panic!("expected weighted factor artifact");
    };
    let ReturnModelSpec::Calibrated(calibrated) = &weighted.return_model else {
        panic!("bound version must carry Calibrated return model");
    };
    assert_eq!(calibrated.calibrator_ref, calibrator);
}

pub async fn publish_rescans_not_findings() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db, DecisionPolicySnapshot::default()).await;

    let dataset_id = seed_leaking_dataset(&db, &harness.artifact_store, &spec, &rc_id).await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        1,
        dataset_id,
        true,
        DatasetProjection::Exact,
    )
    .await;
    Box::pin(seed_backtest(&db, &candidate, &rc_id, '1')).await;
    seed_path_set(&db, &candidate, &rc_id, '1').await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;

    let result = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate,
                reason: "leakage rescan".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await;
    assert!(
        result.is_err(),
        "publish must fail when rescan finds PIT leakage"
    );

    let row = PgModelRegistryRepository::new(db.clone())
        .find_model_version(&candidate)
        .await
        .expect("find")
        .expect("row");
    let gate = row
        .quality_gate_report
        .expect("failed publish persists gate report");
    assert!(
        gate.hard_failures
            .iter()
            .any(|failure| failure.gate == GateId::NoPitLeakage),
        "gate report must record NoPitLeakage from rescan, got: {gate:?}"
    );
}

/// One training example whose label is incorrectly mature before `as_of`.
///
/// The Parquet codec rejects future-dated feature evidence at creation time,
/// so this fixture exercises the independent publish-time label leakage scan
/// without weakening artifact encoding or introducing an unchecked decoder.
fn leaking_training_example() -> TrainingExample {
    let as_of = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    let mut example = TrainingExample {
        example_id: TrainingExampleId::from_v7(),
        market_id: MarketId::new("0xleak"),
        token_id: TokenId::new("1"),
        selected_market: SelectedMarket {
            market_id: MarketId::new("0xleak"),
            event_id: EventId::new("event:leak"),
            category: MarketCategory::Sports,
            primary_token_id: TokenId::new("1"),
            secondary_token_id: Some(TokenId::new("2")),
            liquidity_usd: None,
            volume_24h_usd: None,
            source_refs: Vec::new(),
        },
        decision_boundary: DecisionClock::new(0).boundary(as_of).expect("boundary"),
        sample_source: TrainingSampleSource::HistoricalPit,
        feature_vector: FeatureVector {
            market_id: MarketId::new("0xleak"),
            token_id: Some(TokenId::new("1")),
            decision_at: as_of,
            generic_schema_version: SchemaVersion::FIRST,
            generic: BTreeMap::new(),
            domain: None,
            data_quality: DataQualityStatus::Fresh,
        },
        factor_values: Vec::new(),
        labels: vec![TrainingLabel {
            label_name: LabelName::new("token_payout_ratio"),
            horizon_secs: 0,
            value: dec!(1),
            is_resolved: true,
            matured_at: as_of - ChronoDuration::seconds(60),
        }],
        source_refs: Vec::new(),
        decision_capture: None,
        lot_context: None,
        position_state: None,
        book_fidelity: None,
    };
    bind_fixture_decision_capture(&mut example);
    example
}

/// Persist a Ready dataset whose Parquet fails publish-time PIT leakage rescan.
async fn seed_leaking_dataset(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    spec: &ModelSpecId,
    rc_id: &DecisionPolicySnapshotId,
) -> TrainingDatasetId {
    let model_spec = PgModelRegistryRepository::new(db.clone())
        .find_model_spec(spec)
        .await
        .expect("model spec lookup")
        .expect("model spec");
    let examples = (0..500)
        .map(|index| {
            let mut example = leaking_training_example();
            example.example_id = TrainingExampleId::from_v7();
            example.market_id = MarketId::new(format!("0xleak{index}"));
            example.token_id = TokenId::new((10_000 + index * 2).to_string());
            example.selected_market.market_id = example.market_id.clone();
            example.selected_market.primary_token_id = example.token_id.clone();
            example.selected_market.secondary_token_id =
                Some(TokenId::new((10_001 + index * 2).to_string()));
            example.feature_vector.market_id = example.market_id.clone();
            example.feature_vector.token_id = Some(example.token_id.clone());
            bind_fixture_decision_capture(&mut example);
            example
        })
        .collect();
    let fixture = build_frozen_dataset_fixture(
        db,
        artifact_store,
        FrozenDatasetSeed {
            model_spec_id: *spec,
            model_family: model_spec.model_family,
            model_spec_definition_hash: model_spec.definition_hash,
            decision_policy_snapshot_id: *rc_id,
            examples,
            knowledge_lag_secs: 0,
        },
    )
    .await;
    let dataset_repo = PgTrainingDatasetRepository::new(db.clone());
    fixture.persist(&dataset_repo, healthy_coverage()).await
}

/// A prepared Sell payload cannot publish until leakage-safe OOF estimator
/// evidence exists, even before the CPCV path-set gate is considered.
pub async fn sell_publish_requires_oof() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_sell_spec(&db).await;
    let harness = harness(&db, DecisionPolicySnapshot::default()).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        healthy_sell_coverage(),
    )
    .await;
    let candidate = seed_sell_version(&db, &harness.artifact_store, &spec, 1, dataset).await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;
    let result = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate,
                reason: "attempt".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await;
    let err = result.expect_err("publish error");
    assert!(
        matches!(
            err,
            QuantError::Research(ResearchError::SellOofEstimatorRequired)
        ),
        "Sell preparation without OOF predictions must return the typed OOF error"
    );

    let row = PgModelRegistryRepository::new(db.clone())
        .find_model_version(&candidate)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(
        row.publication_status,
        PublicationStatus::Candidate,
        "a blocked sell publish leaves the version a candidate"
    );
}

/// A bound CPCV path set cannot bypass the earlier Sell OOF-estimator
/// precondition.
pub async fn sell_cpcv_requires_oof() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_sell_spec(&db).await;
    let harness = harness(&db, DecisionPolicySnapshot::default()).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        healthy_sell_coverage(),
    )
    .await;
    let candidate = seed_sell_version(&db, &harness.artifact_store, &spec, 1, dataset).await;
    // `seed_path_set` clears the alpha-significance hard gates
    // (median_rank_ic=0.15 >= 0.02, deflated_sharpe=0.97 >= 1-0.05, pbo=0.20
    // <= 0.5) with the exact same defaults for Buy and Sell; the Sell settings
    // mirror `research.validation.gates.*` under `quality_gate.sell.*`.
    seed_path_set(&db, &candidate, &rc_id, '1').await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;

    let error = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate,
                reason: "sell publish".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await
        .expect_err("CPCV evidence cannot replace Sell OOF predictions");
    assert!(matches!(
        error,
        QuantError::Research(ResearchError::SellOofEstimatorRequired)
    ));

    let row = PgModelRegistryRepository::new(db.clone())
        .find_model_version(&candidate)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(row.publication_status, PublicationStatus::Candidate);
    assert!(row.published_at.is_none());
    assert!(
        row.publish_path_set_id.is_some(),
        "the bound path set stays recorded on the published version"
    );
}
