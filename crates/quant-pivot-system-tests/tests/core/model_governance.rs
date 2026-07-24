//! Model-governance lifecycle system contracts.
//!
//! Exercises the publish / rollback / dataset-promotion orchestration end to end
//! against real repositories + the default quality gate: gate-pass and
//! shadow-stability enforcement, published-version immutability, rollback
//! restoration, runtime-config pointer sync, and the `InsufficientLabels`
//! promotion block.

use std::{collections::BTreeMap, env, sync::Arc};

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use quant_pivot_core::{
    governance::{CoreCalibrationArtifactLoader, ModelGovernanceDeps, ModelGovernanceService},
    runtime_config::DecisionPolicyStore,
    service::{
        feature_integrity::RepositoryFeatureParityGate,
        frozen_model_parity::{FrozenModelParityDeps, FrozenModelParityService},
        training_dataset,
    },
};
use quant_pivot_models::{
    clickhouse::QuantFeatureParityEventRow,
    domain::{
        api::BindCalibrationRequest,
        data_plane::DecisionClock,
        ports::{GovernanceActor, ModelGovernancePort, PublishModelCommand, RetireModelCommand},
        quant::{
            CompleteTrainingDatasetBuild, ModelGovernanceAuditDetail, NewBacktestPathSet,
            NewBacktestReport, NewModelRun, NewModelVersion, NewShadowComparison,
            NewTrainingDatasetPlan,
        },
    },
    enums::{
        common::MarketCategory,
        factor::FactorFamily,
        model::ModelFamily,
        quant::{
            DataQualityStatus, DatasetPurpose, DownsideSource, FactorDirection, ModelRunKind,
            ModelRunStatus, ModelWeightSource, PublicationStatus, TrainingDatasetStatus,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::{DecisionPolicySnapshot, FactorCrossSectionConfig, ModelVersionRef},
    types::{
        ArtifactUri, BacktestPathSetId, BacktestReportId, ContentHash,
        DATASET_ARTIFACT_FORMAT_VERSION, DatasetCoverage, DatasetManifest,
        DecisionPolicySnapshotId, EventId, FactorDefinitionId, MarketId, ModelInputContract,
        ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId, Probability, SchemaVersion,
        ShadowComparisonId, TokenId, TrainingDatasetId, TrainingExampleId, TrainingHorizonsSecs,
        TrainingSampleSource, TrainingSampleSources,
        backtest::{
            BacktestPaths, CategoryMetrics, ExpectedVsRealized, PnlSimulation, SharpeDistribution,
        },
        default_sample_sources,
        factor::FactorExplanation,
        model_metrics::ModelVersionMetrics,
        model_quality::GateId,
        model_training::ModelTrainingObjective,
        shadow::{ShadowRankDelta, ShadowScoreDelta},
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestPathSetRepository, PgBacktestReportRepository, PgCalibrationArtifactRepository,
        PgFeatureParityRepository, PgModelGovernanceAuditRepository, PgModelRegistryRepository,
        PgModelRunRepository, PgPolicyRepository, PgShadowComparisonRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        BacktestPathSetRepository, BacktestReportRepository, CalibrationArtifactRepository,
        FeatureParityRepository, ModelGovernanceAuditRepository, ModelRegistryRepository,
        ModelRunRepository, ShadowComparisonRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
    factors::{FactorValue, FrozenReferenceQuantiles, NormalizedFactor, names::LIQUIDITY_DEPTH},
    features::FeatureVector,
    gates::{DefaultModelQualityGate, ModelQualityGate},
    model::{
        CalibratedReturnModel, CalibrationArtifactLoader, FactorWeight, LabelSelector,
        ModelArtifact, ModelArtifactHeader, ReturnModelSpec, ScoreMultiplierSpec,
        SellScorerArtifact, SellScorerOutputSpec, SubstitutionConfidenceRules,
        WeightedFactorModelArtifact, model_input_contract_hash, weighted_training_input_hash,
    },
    selection::SelectedMarket,
    training::{
        DatasetHashContract, DatasetParquetCodec, LabelName, TrainingDatasetArtifact,
        TrainingExample, TrainingLabel, dataset_manifest_hash, dataset_source_fingerprint,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        execution_pg_seed,
        execution_pg_seed::{fixture_profile_ref, seed_model_score_calibration_fixture},
        fact_sink::DiscardFactWriter,
        model_spec_fixtures,
        policy_fixtures::{bootstrap_default_policy_bundle, bootstrap_policy_bundle},
        research_fixtures::bind_fixture_decision_capture,
    },
};
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

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

async fn harness(db: &DatabaseConnection) -> GovernanceHarness {
    let config = DecisionPolicySnapshot::default();
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
    let service = ModelGovernanceService::new(ModelGovernanceDeps {
        model_registry_repo,
        backtest_report_repo: Arc::new(PgBacktestReportRepository::new(db.clone())),
        backtest_path_set_repo: Arc::new(PgBacktestPathSetRepository::new(db.clone())),
        shadow_comparison_repo: Arc::new(PgShadowComparisonRepository::new(db.clone())),
        governance_audit_repo: Arc::new(PgModelGovernanceAuditRepository::new(db.clone())),
        dataset_repo,
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
            86_400,
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        ))
        .await
        .expect("model spec");
    id
}

/// A dataset coverage that clears the gate (label 0.90, build 0.99).
fn healthy_coverage() -> DatasetCoverage {
    DatasetCoverage {
        planned_samples: 1_000,
        built_examples: 990,
        markets: 50,
        labels_available: 900,
        labels_not_mature: 50,
        labels_unavailable: 50,
        samples_dropped_insufficient: 10,
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
        exit_decision_built: 990,
        exit_fill_l2_rows: 900,
        exit_fill_fallback_rows: 90,
        ..healthy_coverage()
    }
}

fn healthy_training_examples(as_of: DateTime<Utc>) -> Vec<TrainingExample> {
    [dec!(0.25), dec!(0.75)]
        .into_iter()
        .enumerate()
        .map(|(index, score)| {
            let market_id = MarketId::new(format!("0xhealthy{index}"));
            let token_id = TokenId::new("yes");
            let mut example = leaking_training_example();
            example.example_id = TrainingExampleId::from_v7();
            example.market_id = market_id.clone();
            example.token_id = token_id.clone();
            example.selected_market.market_id = market_id.clone();
            example.selected_market.primary_token_id = token_id.clone();
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

struct FrozenDatasetFixture {
    id: TrainingDatasetId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    feature_schema_hash: ContentHash,
    factor_schema_hash: ContentHash,
    label_schema_hash: ContentHash,
    dataset_hash: ContentHash,
    manifest_hash: ContentHash,
    manifest: DatasetManifest,
    artifact_bytes_hash: ContentHash,
    parquet_uri: ArtifactUri,
    sample_count: i64,
}

async fn build_frozen_dataset_fixture(
    artifact_store: &Arc<dyn ArtifactStore>,
    spec: &ModelSpecId,
    model_spec_definition_hash: &ContentHash,
    rc_id: &DecisionPolicySnapshotId,
) -> FrozenDatasetFixture {
    let id = TrainingDatasetId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);
    let window_end = window_start + ChronoDuration::hours(1);
    let examples = healthy_training_examples(window_start + ChronoDuration::minutes(1));
    let sample_count = i64::try_from(examples.len()).expect("sample count");
    let feature_schema_hash = content_hash(u32::from('f'));
    let factor_schema_hash = content_hash(u32::from('g'));
    let label_schema_hash = content_hash(u32::from('l'));
    let dataset_hash = TrainingDatasetArtifact::compute_dataset_hash(
        DatasetHashContract {
            model_spec_id: spec,
            window_start,
            window_end,
            purpose: DatasetPurpose::Training,
            feature_schema_hash: &feature_schema_hash,
            factor_schema_hash: &factor_schema_hash,
            label_schema_hash: &label_schema_hash,
        },
        &examples,
    )
    .expect("semantic dataset hash");
    let manifest = DatasetManifest {
        format_version: DATASET_ARTIFACT_FORMAT_VERSION,
        training_dataset_id: id,
        profile_ref: fixture_profile_ref(),
        research_program_hash: content_hash(u32::from('p')),
        source_slice: execution_pg_seed::source_slice_ref('s'),
        model_spec_id: *spec,
        model_spec_definition_hash: *model_spec_definition_hash,
        trade_policy_artifact_id: None,
        trade_policy_hash: None,
        decision_policy_snapshot_id: *rc_id,
        window_start,
        window_end,
        purpose: DatasetPurpose::Training,
        knowledge_lag_secs: 10,
        sample_interval_secs: 3_600,
        horizons_secs: vec![3_600],
        feature_schema_hash,
        factor_schema_hash,
        label_schema_hash,
        semantic_dataset_hash: dataset_hash,
        source_fingerprint: dataset_source_fingerprint(&examples).expect("source fingerprint"),
        sample_count: u64::try_from(sample_count).expect("manifest sample count"),
    };
    // Real Parquet so publish-time leakage rescan (#9) can decode bytes.
    let bytes = DatasetParquetCodec::encode(&examples, &manifest).expect("encode parquet");
    let manifest_hash = dataset_manifest_hash(&manifest).expect("manifest hash");
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
        window_start,
        window_end,
        feature_schema_hash,
        factor_schema_hash,
        label_schema_hash,
        dataset_hash,
        manifest_hash,
        manifest,
        artifact_bytes_hash,
        parquet_uri,
        sample_count,
    }
}

async fn complete_seeded_dataset(
    dataset_repo: &PgTrainingDatasetRepository,
    fixture: FrozenDatasetFixture,
    status: TrainingDatasetStatus,
    coverage_json: DatasetCoverage,
) -> TrainingDatasetId {
    let terminal_status = if status == TrainingDatasetStatus::Expired {
        TrainingDatasetStatus::Ready
    } else {
        status
    };
    dataset_repo
        .complete_build(
            &fixture.id,
            CompleteTrainingDatasetBuild {
                status: terminal_status,
                feature_schema_hash: fixture.feature_schema_hash,
                factor_schema_hash: fixture.factor_schema_hash,
                label_schema_hash: fixture.label_schema_hash,
                dataset_hash: fixture.dataset_hash,
                manifest_hash: fixture.manifest_hash,
                manifest_json: fixture.manifest,
                artifact_bytes_hash: fixture.artifact_bytes_hash,
                parquet_uri: fixture.parquet_uri,
                sample_count: fixture.sample_count,
                coverage_json,
                failure_detail: None,
            },
        )
        .await
        .expect("complete dataset");
    if status == TrainingDatasetStatus::Expired {
        dataset_repo
            .expire(&fixture.id)
            .await
            .expect("expire dataset");
    }
    fixture.id
}

async fn seed_dataset(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    spec: &ModelSpecId,
    rc_id: &DecisionPolicySnapshotId,
    status: TrainingDatasetStatus,
    coverage_json: DatasetCoverage,
    dataset_hash_seed: char,
) -> TrainingDatasetId {
    let _ = dataset_hash_seed;
    let model_spec_definition_hash = PgModelRegistryRepository::new(db.clone())
        .find_model_spec_by_id(spec)
        .await
        .expect("model spec lookup")
        .expect("model spec")
        .definition_hash;
    let fixture =
        build_frozen_dataset_fixture(artifact_store, spec, &model_spec_definition_hash, rc_id)
            .await;
    let dataset_repo = PgTrainingDatasetRepository::new(db.clone());
    dataset_repo
        .create_plan(NewTrainingDatasetPlan {
            training_dataset_id: fixture.id,
            model_spec_id: *spec,
            model_spec_definition_hash,
            window_start: fixture.window_start,
            window_end: fixture.window_end,
            purpose: DatasetPurpose::Training,
            knowledge_lag_secs: 10,
            sample_interval_secs: 3_600,
            horizons_secs: TrainingHorizonsSecs(vec![3600]),
            feature_schema_version: Some(SchemaVersion::FIRST),
            sample_sources: Some(TrainingSampleSources(default_sample_sources())),
            decision_policy_snapshot_id: *rc_id,
        })
        .await
        .expect("dataset plan");
    if status != TrainingDatasetStatus::Planned {
        dataset_repo
            .start_build(&fixture.id)
            .await
            .expect("start dataset build");
    }
    match status {
        TrainingDatasetStatus::Ready
        | TrainingDatasetStatus::InsufficientLabels
        | TrainingDatasetStatus::Expired => {
            return complete_seeded_dataset(&dataset_repo, fixture, status, coverage_json).await;
        }
        TrainingDatasetStatus::Failed => {
            dataset_repo
                .fail_build(&fixture.id, "seeded dataset failure".to_owned())
                .await
                .expect("fail dataset");
        }
        TrainingDatasetStatus::Planned | TrainingDatasetStatus::Building => {}
    }
    fixture.id
}

async fn seed_version(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    spec: &ModelSpecId,
    seed: char,
    version: i32,
    dataset: Option<TrainingDatasetId>,
    calibrated: bool,
) -> ModelVersionId {
    let id = ModelVersionId::from_v7();
    let calibrator_ref = if calibrated {
        Some(seed_model_score_calibration_fixture(db, &id).await)
    } else {
        None
    };
    let return_model =
        calibrator_ref
            .as_ref()
            .map_or_else(ReturnModelSpec::heuristic_default, |calibrator| {
                ReturnModelSpec::Calibrated(CalibratedReturnModel {
                    calibrator_ref: *calibrator,
                    downside_source: DownsideSource::MfeMae,
                })
            });
    let (training_dataset_hash, training_input_hash) = match dataset.as_ref() {
        Some(dataset_id) => {
            let dataset_info = PgTrainingDatasetRepository::new(db.clone())
                .find_by_id(dataset_id)
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
            let cross_section = FactorCrossSectionConfig::default();
            let references = FrozenReferenceQuantiles::empty();
            let input_hash = weighted_training_input_hash(
                &examples,
                &LabelSelector {
                    name: LabelName::new("settlement_outcome"),
                    horizon_secs: 0,
                },
                &[LIQUIDITY_DEPTH],
                &references,
                Some(&cross_section),
            )
            .expect("weighted training input hash");
            (*materialization.dataset_hash, input_hash)
        }
        None => (content_hash(u32::from('d')), content_hash(u32::from('i'))),
    };
    let artifact_hash = store_weighted_artifact(
        artifact_store,
        &id,
        &PgModelRegistryRepository::new(db.clone())
            .find_model_spec_by_id(spec)
            .await
            .expect("model spec lookup")
            .expect("model spec")
            .definition_hash,
        seed,
        return_model,
        training_dataset_hash,
        training_input_hash,
    )
    .await;
    PgModelRegistryRepository::new(db.clone())
        .create_model_version(NewModelVersion {
            model_version_id: id,
            model_spec_id: *spec,
            version,
            artifact_hash,
            category_scope: None,
            profile_ref: fixture_profile_ref(),
            training_dataset_id: dataset,
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            publish_path_set_id: None,
            derivation: NewModelVersion::training_derivation(),
            metrics: ModelVersionMetrics::not_measured("test fixture"),
            training_objective: ModelTrainingObjective::hand_authored("test fixture"),
            quality_gate_report: None,
            publication_status: PublicationStatus::Candidate,
            published_at: None,
            retired_at: None,
        })
        .await
        .expect("model version");
    id
}

async fn store_weighted_artifact(
    store: &Arc<dyn ArtifactStore>,
    model_version_id: &ModelVersionId,
    model_spec_definition_hash: &ContentHash,
    seed: char,
    return_model: ReturnModelSpec,
    training_dataset_hash: ContentHash,
    training_input_hash: ContentHash,
) -> ContentHash {
    let input_contract = ModelInputContract::single_required("book.mid");
    let input_contract_hash =
        model_input_contract_hash(&input_contract).expect("input contract hash");
    let artifact = ModelArtifact::WeightedFactor(Box::new(WeightedFactorModelArtifact {
        header: ModelArtifactHeader {
            model_version_id: *model_version_id,
            model_spec_definition_hash: *model_spec_definition_hash,
            profile_ref: fixture_profile_ref(),
            model_family: ModelFamily::WeightedFactor,
            feature_schema_hash: content_hash(u32::from('f')),
            factor_schema_hash: content_hash(u32::from('g')),
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
        },
        training_dataset_hash,
        training_input_hash,
        input_contract,
        input_contract_hash,
        weights: vec![FactorWeight {
            factor: LIQUIDITY_DEPTH,
            weight: dec!(1),
        }],
        prediction_horizon_secs: 86_400,
        multipliers: ScoreMultiplierSpec::conservative(),
        substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
        return_model,
        factor_cross_section: FactorCrossSectionConfig::default(),
        frozen_reference_quantiles: FrozenReferenceQuantiles::empty(),
        objective_report: None,
        category_scope: None,
    }));
    artifact.validate().expect("artifact valid");
    let artifact_hash = artifact.content_hash().expect("hash");
    let key = ModelArtifact::artifact_key(&artifact_hash).expect("key");
    store
        .put(key, &artifact.to_bytes().expect("bytes"))
        .await
        .expect("store artifact");
    let _ = seed;
    artifact_hash
}

/// A `HoldVsExitWeighted` model spec for sell-side governance tests.
async fn seed_sell_spec(db: &DatabaseConnection) -> ModelSpecId {
    let id = ModelSpecId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            id,
            "governance-it-sell",
            ModelFamily::HoldVsExitWeighted,
            86_400,
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        ))
        .await
        .expect("sell model spec");
    id
}

/// A `HoldVsExitWeighted` candidate version with a validated
/// [`SellScorerArtifact`]. Sell scorers never carry a return
/// model, so unlike [`seed_version`] there is no calibrator parameter.
async fn seed_sell_version(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    spec: &ModelSpecId,
    version: i32,
    dataset: Option<TrainingDatasetId>,
) -> ModelVersionId {
    let id = ModelVersionId::from_v7();
    let (training_dataset_hash, training_input_hash) = match dataset.as_ref() {
        Some(dataset_id) => {
            let dataset_info = PgTrainingDatasetRepository::new(db.clone())
                .find_by_id(dataset_id)
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
            let input_hash = weighted_training_input_hash(
                &examples,
                &LabelSelector {
                    name: LabelName::new("settlement_outcome"),
                    horizon_secs: 0,
                },
                &[LIQUIDITY_DEPTH],
                &FrozenReferenceQuantiles::empty(),
                None,
            )
            .expect("sell training input hash");
            (*materialization.dataset_hash, input_hash)
        }
        None => (content_hash(u32::from('d')), content_hash(u32::from('i'))),
    };
    let input_contract = ModelInputContract::single_required("book.mid");
    let input_contract_hash =
        model_input_contract_hash(&input_contract).expect("input contract hash");
    let model_spec_definition_hash = PgModelRegistryRepository::new(db.clone())
        .find_model_spec_by_id(spec)
        .await
        .expect("sell model spec lookup")
        .expect("sell model spec")
        .definition_hash;
    let artifact = ModelArtifact::SellScorer(Box::new(SellScorerArtifact {
        header: ModelArtifactHeader {
            model_version_id: id,
            model_spec_definition_hash,
            profile_ref: fixture_profile_ref(),
            model_family: ModelFamily::HoldVsExitWeighted,
            feature_schema_hash: content_hash(u32::from('f')),
            factor_schema_hash: content_hash(u32::from('g')),
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
        },
        weights: vec![FactorWeight {
            factor: LIQUIDITY_DEPTH,
            weight: dec!(1),
        }],
        prediction_horizon_secs: 86_400,
        output_spec: SellScorerOutputSpec::conservative(),
        label_schema_hash: content_hash(u32::from('l')),
        training_dataset_hash,
        training_input_hash,
        input_contract,
        input_contract_hash,
        objective_report: None,
    }));
    artifact.validate().expect("sell artifact valid");
    let artifact_hash = artifact.content_hash().expect("hash");
    let key = ModelArtifact::artifact_key(&artifact_hash).expect("key");
    artifact_store
        .put(key, &artifact.to_bytes().expect("bytes"))
        .await
        .expect("store sell artifact");
    PgModelRegistryRepository::new(db.clone())
        .create_model_version(NewModelVersion {
            model_version_id: id,
            model_spec_id: *spec,
            version,
            artifact_hash,
            category_scope: None,
            profile_ref: fixture_profile_ref(),
            training_dataset_id: dataset,
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            publish_path_set_id: None,
            derivation: NewModelVersion::training_derivation(),
            metrics: ModelVersionMetrics::not_measured("test fixture"),
            training_objective: ModelTrainingObjective::hand_authored("test fixture"),
            quality_gate_report: None,
            publication_status: PublicationStatus::Candidate,
            published_at: None,
            retired_at: None,
        })
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
    let model_run_id = ModelRunId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::Backtest,
            model_version_id: Some(*version),
            decision_policy_snapshot_id: *rc_id,
            market_selection_id: None,
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            status: ModelRunStatus::Succeeded,
            input_hash: content_hash(u32::from(hash_seed)),
            output_hash: Some(content_hash(u32::from(hash_seed) + 1)),
            error_code: None,
            error_message: None,
            started_at: window_start,
            finished_at: Some(Utc::now()),
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

    PgBacktestReportRepository::new(db.clone())
        .create(NewBacktestReport {
            backtest_report_id: BacktestReportId::from_v7(),
            model_version_id: *version,
            model_run_id,
            decision_policy_snapshot_id: *rc_id,
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
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
        })
        .await
        .expect("backtest report");
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

pub async fn publish_requires_quality_gate_pass() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;
    // Frozen dataset present so leakage rescan can run; no backtest → gate
    // evaluates and fails (BacktestRequired / risk gates), then persists evidence.
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        '1',
    )
    .await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        'a',
        1,
        Some(dataset),
        false,
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
        .find_model_version_by_id(&candidate)
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

pub async fn publish_without_training_dataset_is_illegal_transition() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let _rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;
    // Missing training_dataset_id fails closed before gate evaluation — no
    // durable gate report is written (distinct from QualityGateFailed).
    let candidate = seed_version(&db, &harness.artifact_store, &spec, 'a', 1, None, false).await;

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
    let err_text = err.to_string();
    assert!(
        err_text.contains("illegal governance transition")
            && err_text.contains("training_dataset_id"),
        "expected IllegalTransition naming the missing training_dataset_id, got: {err_text}"
    );

    let registry = PgModelRegistryRepository::new(db.clone());
    let row = registry
        .find_model_version_by_id(&candidate)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(row.publication_status, PublicationStatus::Candidate);
    assert!(
        row.quality_gate_report.is_none(),
        "pre-gate IllegalTransition must not invent a quality_gate_report"
    );
}

pub async fn publish_requires_backtest_report() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        '1',
    )
    .await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        'a',
        1,
        Some(dataset),
        false,
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
    let harness = harness(&db).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        '1',
    )
    .await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        'a',
        1,
        Some(dataset),
        false,
    )
    .await;
    seed_backtest(&db, &candidate, &rc_id, '1').await;
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

pub async fn publish_succeeds_without_mutating_routing_then_version_is_immutable() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        '1',
    )
    .await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        'a',
        1,
        Some(dataset),
        true,
    )
    .await;
    seed_backtest(&db, &candidate, &rc_id, '1').await;
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
    seed_backtest(
        params.db,
        params.version,
        params.rc_id,
        params.backtest_seed,
    )
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
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::Cpcv,
            model_version_id: Some(*version),
            decision_policy_snapshot_id: *rc_id,
            market_selection_id: None,
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            status: ModelRunStatus::Succeeded,
            input_hash: content_hash(u32::from(hash_seed) + 10),
            output_hash: Some(content_hash(u32::from(hash_seed) + 11)),
            error_code: None,
            error_message: None,
            started_at: window_start,
            finished_at: Some(Utc::now()),
        })
        .await
        .expect("cpcv model run");

    // Dataset id is required by the ledger FK; reuse any Ready dataset linked
    // to this version when present, otherwise mint a synthetic id that the
    // path-set row alone references (no FK to training_dataset in schema).
    let training_dataset_id = PgModelRegistryRepository::new(db.clone())
        .find_model_version_by_id(version)
        .await
        .expect("version")
        .and_then(|v| v.training_dataset_id)
        .unwrap_or_else(TrainingDatasetId::from_v7);

    let path_set_id = BacktestPathSetId::from_v7();
    PgBacktestPathSetRepository::new(db.clone())
        .create(NewBacktestPathSet {
            path_set_id,
            model_version_id: *version,
            model_run_id,
            training_dataset_id,
            decision_policy_snapshot_id: *rc_id,
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            path_count: 7,
            combination_count: 28,
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
            paths: BacktestPaths::default(),
            deflated_sharpe: dec!(0.97),
            dsr_benchmark_sharpe: dec!(0.5),
            pbo: dec!(0.20),
            min_track_record_length_secs: Some(86_400),
            trial_count: 10,
            trial_grid_count: 10,
            coord_search_effective_n: 2,
            path_set_hash: ContentHash::parse(
                "blake3:0000000000000000000000000000000000000000000000000000000000000001",
            )
            .expect("hash"),
        })
        .await
        .expect("path set");
    // Publish gates require an explicit bind — never implicit "latest".
    PgModelRegistryRepository::new(db.clone())
        .set_publish_path_set_id(version, Some(path_set_id))
        .await
        .expect("bind publish path set");
}

pub async fn retire_unrouted_published_version_audits_without_mutating_routing() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;

    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        '1',
    )
    .await;
    let version = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        'a',
        1,
        Some(dataset),
        true,
    )
    .await;
    publish_ready_version(PublishReadyParams {
        governance: &harness.service,
        db: &db,
        rc_id: &rc_id,
        version: &version,
        active_for_shadow: &version,
        backtest_seed: '1',
        shadow_seed: 'a',
        reason: "publish for retire",
    })
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

pub async fn retire_routed_published_version_is_rejected_fail_closed() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;

    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        '1',
    )
    .await;
    let version = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        'a',
        1,
        Some(dataset),
        true,
    )
    .await;
    publish_ready_version(PublishReadyParams {
        governance: &harness.service,
        db: &db,
        rc_id: &rc_id,
        version: &version,
        active_for_shadow: &version,
        backtest_seed: '1',
        shadow_seed: 'a',
        reason: "publish for category-pointer retire",
    })
    .await;

    // Simulate an operator having pinned this same version to a category
    // route (independent of the generic active pointer publish already set).
    let mut config = (*harness.store.current()).clone();
    config
        .model_routing
        .model
        .category_model_pointers
        .insert(MarketCategory::Crypto, ModelVersionRef::new(version));
    harness.store.replace(config);
    assert!(
        harness
            .store
            .current()
            .model_routing
            .model
            .category_model_pointers
            .contains_key(&MarketCategory::Crypto),
        "precondition: category pointer is armed before retire"
    );

    let error = harness
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
        harness
            .store
            .current()
            .model_routing
            .model
            .active_model_version_id
            .is_none(),
        "failed retirement must not invent a generic route"
    );
    assert!(
        harness
            .store
            .current()
            .model_routing
            .model
            .category_model_pointers
            .contains_key(&MarketCategory::Crypto),
        "failed retirement must preserve the governed category route"
    );
    let row = PgModelRegistryRepository::new(db)
        .find_model_version_by_id(&version)
        .await
        .expect("model lookup")
        .expect("model version");
    assert_eq!(row.publication_status, PublicationStatus::Published);
}

pub async fn uncalibrated_return_model_cannot_publish() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        '1',
    )
    .await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        'a',
        1,
        Some(dataset),
        false,
    )
    .await;
    seed_backtest(&db, &candidate, &rc_id, '1').await;
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
        .find_model_version_by_id(&candidate)
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

pub async fn bind_calibration_creates_candidate_version_with_calibrated_return_model() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        '1',
    )
    .await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        'a',
        1,
        Some(dataset),
        false,
    )
    .await;
    let calibrator = seed_model_score_calibration_fixture(&db, &candidate).await;

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
    let ModelArtifact::WeightedFactor(weighted) = artifact else {
        panic!("expected weighted factor artifact");
    };
    let ReturnModelSpec::Calibrated(calibrated) = weighted.return_model else {
        panic!("bound version must carry Calibrated return model");
    };
    assert_eq!(calibrated.calibrator_ref, calibrator);
}

pub async fn publish_rescans_leakage_not_default_findings() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;

    let dataset_id = seed_leaking_dataset(&db, &harness.artifact_store, &spec, &rc_id).await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        'L',
        1,
        Some(dataset_id),
        true,
    )
    .await;
    seed_backtest(&db, &candidate, &rc_id, '1').await;
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
        .find_model_version_by_id(&candidate)
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
        token_id: TokenId::new("yes"),
        selected_market: SelectedMarket {
            market_id: MarketId::new("0xleak"),
            event_id: EventId::new("event:leak"),
            category: MarketCategory::Sports,
            primary_token_id: TokenId::new("yes"),
            secondary_token_id: None,
            liquidity_usd: None,
            volume_24h_usd: None,
            source_refs: Vec::new(),
        },
        decision_boundary: DecisionClock::new(0).boundary(as_of).expect("boundary"),
        sample_source: TrainingSampleSource::HistoricalPit,
        feature_vector: FeatureVector {
            market_id: MarketId::new("0xleak"),
            token_id: Some(TokenId::new("yes")),
            decision_at: as_of,
            generic_schema_version: SchemaVersion::FIRST,
            generic: BTreeMap::new(),
            domain: None,
            data_quality: DataQualityStatus::Fresh,
        },
        factor_values: Vec::new(),
        labels: vec![TrainingLabel {
            label_name: LabelName::new("settlement_outcome"),
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
    let dataset_id = TrainingDatasetId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);
    let window_end = window_start + ChronoDuration::hours(1);
    let first = leaking_training_example();
    let mut second = first.clone();
    second.example_id = TrainingExampleId::from_v7();
    second.market_id = MarketId::new("0xleak2");
    second.selected_market.market_id = second.market_id.clone();
    second.feature_vector.market_id = second.market_id.clone();
    bind_fixture_decision_capture(&mut second);
    let examples = vec![first, second];
    let sample_count = i64::try_from(examples.len()).expect("leaking sample count");
    let feature_schema_hash = content_hash(u32::from('f'));
    let factor_schema_hash = content_hash(u32::from('g'));
    let label_schema_hash = content_hash(u32::from('l'));
    let dataset_hash = TrainingDatasetArtifact::compute_dataset_hash(
        DatasetHashContract {
            model_spec_id: spec,
            window_start,
            window_end,
            purpose: DatasetPurpose::Training,
            feature_schema_hash: &feature_schema_hash,
            factor_schema_hash: &factor_schema_hash,
            label_schema_hash: &label_schema_hash,
        },
        &examples,
    )
    .expect("semantic dataset hash");
    let model_spec_definition_hash = PgModelRegistryRepository::new(db.clone())
        .find_model_spec_by_id(spec)
        .await
        .expect("model spec lookup")
        .expect("model spec")
        .definition_hash;
    let manifest = DatasetManifest {
        format_version: DATASET_ARTIFACT_FORMAT_VERSION,
        training_dataset_id: dataset_id,
        profile_ref: fixture_profile_ref(),
        research_program_hash: content_hash(u32::from('p')),
        source_slice: execution_pg_seed::source_slice_ref('s'),
        model_spec_id: *spec,
        model_spec_definition_hash,
        trade_policy_artifact_id: None,
        trade_policy_hash: None,
        decision_policy_snapshot_id: *rc_id,
        window_start,
        window_end,
        purpose: DatasetPurpose::Training,
        knowledge_lag_secs: 0,
        sample_interval_secs: 3_600,
        horizons_secs: vec![0],
        feature_schema_hash,
        factor_schema_hash,
        label_schema_hash,
        semantic_dataset_hash: dataset_hash,
        source_fingerprint: dataset_source_fingerprint(&examples).expect("source fingerprint"),
        sample_count: u64::try_from(sample_count).expect("manifest sample count"),
    };
    let bytes = DatasetParquetCodec::encode(&examples, &manifest).expect("encode");
    let manifest_hash = dataset_manifest_hash(&manifest).expect("manifest hash");
    let artifact_bytes_hash = CanonicalDigest::content_hash_bytes(&bytes);
    let hex = dataset_id.as_uuid().simple().to_string();
    let key = ArtifactKey::new(ArtifactNamespace::Dataset, hex, "parquet").expect("key");
    let parquet_uri = artifact_store.put(key, &bytes).await.expect("store");
    let dataset_repo = PgTrainingDatasetRepository::new(db.clone());
    dataset_repo
        .create_plan(NewTrainingDatasetPlan {
            training_dataset_id: dataset_id,
            model_spec_id: *spec,
            model_spec_definition_hash,
            window_start,
            window_end,
            purpose: DatasetPurpose::Training,
            knowledge_lag_secs: 0,
            sample_interval_secs: 3_600,
            horizons_secs: TrainingHorizonsSecs(vec![0]),
            feature_schema_version: Some(SchemaVersion::FIRST),
            sample_sources: Some(TrainingSampleSources(default_sample_sources())),
            decision_policy_snapshot_id: *rc_id,
        })
        .await
        .expect("dataset plan");
    dataset_repo
        .start_build(&dataset_id)
        .await
        .expect("start dataset");
    dataset_repo
        .complete_build(
            &dataset_id,
            CompleteTrainingDatasetBuild {
                status: TrainingDatasetStatus::Ready,
                feature_schema_hash,
                factor_schema_hash,
                label_schema_hash,
                dataset_hash,
                manifest_hash,
                manifest_json: manifest,
                artifact_bytes_hash,
                parquet_uri,
                sample_count,
                coverage_json: healthy_coverage(),
                failure_detail: None,
            },
        )
        .await
        .expect("dataset");
    dataset_id
}

/// Sell (`HoldVsExitWeighted`) publish requires a bound CPCV
/// path set exactly like Buy — `bound_path_set` is family-agnostic, and
/// `evaluate_gate` no longer excludes exit scorers from it.
pub async fn sell_publish_requires_bound_cpcv_path_set() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_sell_spec(&db).await;
    let harness = harness(&db).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_sell_coverage(),
        '1',
    )
    .await;
    let candidate = seed_sell_version(&db, &harness.artifact_store, &spec, 1, Some(dataset)).await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;
    // Deliberately no bound CPCV path set.

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
        "sell publish must fail without a bound CPCV path set"
    );
    let err = result.expect_err("publish error");
    assert!(
        err.to_string().contains("CpcvRequired") || err.to_string().contains("cpcv"),
        "expected CpcvRequired failure, got: {err}"
    );

    let row = PgModelRegistryRepository::new(db.clone())
        .find_model_version_by_id(&candidate)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(
        row.publication_status,
        PublicationStatus::Candidate,
        "a blocked sell publish leaves the version a candidate"
    );
}

/// Once a lot-level CPCV path set clearing the alpha-
/// significance hard gates is explicitly bound, Sell publish succeeds
/// through the same governance closure Buy uses (no calibrator needed —
/// `CalibrationRequired` is `NotApplicable` for exit scorers).
pub async fn sell_publish_succeeds_with_bound_cpcv_path_set() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_sell_spec(&db).await;
    let harness = harness(&db).await;
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_sell_coverage(),
        '1',
    )
    .await;
    let candidate = seed_sell_version(&db, &harness.artifact_store, &spec, 1, Some(dataset)).await;
    // `seed_path_set` clears the alpha-significance hard gates
    // (median_rank_ic=0.15 >= 0.02, deflated_sharpe=0.97 >= 1-0.05, pbo=0.20
    // <= 0.5) with the exact same defaults for Buy and Sell; the Sell settings
    // mirror `research.validation.gates.*` under `quality_gate.sell.*`.
    seed_path_set(&db, &candidate, &rc_id, '1').await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;

    harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate,
                reason: "sell publish".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await
        .expect("sell publish with bound path set should succeed");

    let row = PgModelRegistryRepository::new(db.clone())
        .find_model_version_by_id(&candidate)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(row.publication_status, PublicationStatus::Published);
    assert!(row.published_at.is_some());
    assert!(
        row.publish_path_set_id.is_some(),
        "the bound path set stays recorded on the published version"
    );
}
