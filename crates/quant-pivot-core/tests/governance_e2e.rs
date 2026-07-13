//! Model-governance closure integration tests (Postgres + testcontainers).
//!
//! Exercises the publish / rollback / dataset-promotion orchestration end to end
//! against real repositories + the default quality gate: gate-pass and
//! shadow-stability enforcement, published-version immutability, rollback
//! restoration, runtime-config pointer sync, and the `InsufficientLabels`
//! promotion block.

use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_core::governance::{
    CoreCalibrationArtifactLoader, ModelGovernanceDeps, ModelGovernanceService,
    ModelScoreCalibrationPayload, model_score_content_hash,
};
use quant_pivot_core::runtime_config::RuntimeConfigStore;
use quant_pivot_core::service::{
    feature_integrity::FeatureParityGatePort,
    frozen_model_parity::{FrozenModelParityDeps, FrozenModelParityService},
};
use quant_pivot_error::control::ControlError;
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::domain::{DecisionClock, TimeWindow};
use quant_pivot_models::enums::quant::DatasetPurpose;
use quant_pivot_models::runtime_config::FactorCrossSectionConfig;
use quant_pivot_models::types::{
    DATASET_ARTIFACT_FORMAT_VERSION, DatasetManifest, EventId, MarketId, ModelInputContract,
    ModelTrainingContract, TrainingExampleId,
};
use quant_pivot_models::{
    clickhouse::QuantFeatureParityEventRow,
    domain::{
        BindCalibrationRequest, CompleteTrainingDatasetBuild, GovernanceActor, ModelGovernancePort,
        NewBacktestPathSet, NewBacktestReport, NewCalibrationArtifact, NewModelRun, NewModelSpec,
        NewModelVersion, NewRuntimeConfigActivation, NewRuntimeConfigVersion, NewShadowComparison,
        NewTrainingDatasetPlan, PublishModelCommand, RetireModelCommand, RollbackModelCommand,
        RuntimeConfigPort,
    },
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{
            CalibrationKind, DownsideSource, FeatureParityLatchState, FeatureParityRunKind,
            ModelRunKind, ModelRunStatus, PublicationStatus, TrainingDatasetStatus,
        },
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    hashing::CanonicalDigest,
    runtime_config::{ModelVersionRef, RUNTIME_CONFIG_SCHEMA_VERSION, RuntimeConfig},
    types::{
        ArtifactUri, BacktestPathSetId, BacktestReportId, CalibrationArtifactId, ContentHash,
        DatasetCoverage, FeatureParityStateId, ModelRunId, ModelSpecId, ModelVersionId,
        Probability, RuntimeConfigActivationId, RuntimeConfigVersionId, SchemaVersion,
        ShadowComparisonId, TrainingDatasetId, TrainingHorizonsSecs, TrainingSampleSources,
        default_sample_sources,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestPathSetRepository, PgBacktestReportRepository, PgCalibrationArtifactRepository,
        PgFeatureParityRepository, PgModelGovernanceAuditRepository, PgModelRegistryRepository,
        PgModelRunRepository, PgRuntimeConfigVersionRepository, PgShadowComparisonRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        BacktestPathSetRepository, BacktestReportRepository, CalibrationArtifactRepository,
        FeatureParityLatchActor, FeatureParityRepository, ModelGovernanceAuditRepository,
        ModelRegistryRepository, ModelRunRepository, RuntimeConfigVersionRepository,
        ShadowComparisonRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::factors::FrozenReferenceQuantiles;
use quant_pivot_research::selection::SelectedMarket;
use quant_pivot_research::training::{LabelName, TrainingExample};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
    backtest::{ExpectedVsRealized, PnlSimulation},
    factors::names::LIQUIDITY_DEPTH,
    gates::{DefaultModelQualityGate, ModelQualityGate},
    model::{
        CalibratedReturnModel, CalibrationArtifactLoader, FactorWeight, LabelSelector,
        ModelArtifact, ModelArtifactHeader, ReturnModelSpec, ScoreMultiplierSpec,
        SellScorerArtifact, SellScorerOutputSpec, SubstitutionConfidenceRules,
        WeightedFactorModelArtifact, model_input_contract_hash, weighted_training_input_hash,
    },
    model::{MonotoneMapping, ReliabilityReport},
    training::{
        DatasetHashContract, DatasetParquetCodec, TrainingDatasetArtifact, dataset_manifest_hash,
        dataset_source_fingerprint,
    },
};
use quant_pivot_test_support::{fact_sink::DiscardFactWriter, pg::setup_pg};
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

/// Test harness wiring governance against a real store + config repo.
struct GovernanceHarness {
    service: ModelGovernanceService,
    store: Arc<RuntimeConfigStore>,
    runtime_apply: Arc<TestRuntimeConfigApply>,
    artifact_store: Arc<dyn ArtifactStore>,
}

/// Minimal [`RuntimeConfigPort`] for integration tests (store swap only).
struct TestRuntimeConfigApply {
    store: Arc<RuntimeConfigStore>,
    fault_mode: AtomicU8,
}

impl TestRuntimeConfigApply {
    fn fail_next_apply(&self) {
        self.fault_mode.store(1, Ordering::SeqCst);
    }

    fn partially_apply_target_then_fail_recovery(&self) {
        self.fault_mode.store(2, Ordering::SeqCst);
    }
}

struct ClearFeatureParityGate {
    parity: Arc<dyn FeatureParityRepository>,
}

#[async_trait]
impl FeatureParityGatePort for ClearFeatureParityGate {
    async fn ensure_clear(&self, _action: &'static str) -> QuantResult<()> {
        if self
            .parity
            .current_state()
            .await?
            .is_some_and(|state| state.state == FeatureParityLatchState::Clear)
        {
            return Ok(());
        }
        let recovery = self
            .parity
            .latest_run(FeatureParityRunKind::Full)
            .await?
            .ok_or_else(|| {
                QuantError::from(ResearchError::Determinism {
                    detail: "test publish gate has no full parity recovery run".to_owned(),
                })
            })?;
        self.parity
            .acknowledge_latch(
                &recovery.run_id,
                FeatureParityLatchActor {
                    actor: Some("governance-it".to_owned()),
                    acting_role: "risk_owner".to_owned(),
                    reason: "integration-test governed bootstrap".to_owned(),
                },
            )
            .await?;
        Ok(())
    }

    async fn commit_state_id(&self, action: &'static str) -> QuantResult<FeatureParityStateId> {
        self.ensure_clear(action).await?;
        self.parity
            .current_state()
            .await?
            .map(|state| state.state_id)
            .ok_or_else(|| {
                ResearchError::Determinism {
                    detail: "test parity gate has no durable clear generation".to_owned(),
                }
                .into()
            })
    }

    async fn trip_integrity_failure(
        &self,
        source_run_id: &quant_pivot_models::types::FeatureParityRunId,
        _action: &'static str,
        reason: String,
    ) -> QuantResult<quant_pivot_models::types::FeatureParityRunId> {
        self.parity
            .record_integrity_failure_and_open_latch(source_run_id, reason)
            .await
            .map(|(run, _state)| run.run_id)
            .map_err(QuantError::from)
    }
}

#[async_trait]
impl RuntimeConfigPort for TestRuntimeConfigApply {
    fn current(&self) -> Arc<RuntimeConfig> {
        self.store.current()
    }

    fn preflight(&self, _candidate: &RuntimeConfig) -> Result<(), ControlError> {
        Ok(())
    }

    async fn apply(&self, config: RuntimeConfig) -> Result<(), ControlError> {
        match self.fault_mode.load(Ordering::SeqCst) {
            1 => {
                self.fault_mode.store(0, Ordering::SeqCst);
                return Err(ControlError::Precondition(
                    "injected runtime pointer apply failure".to_owned(),
                ));
            }
            2 => {
                self.store.replace(config);
                self.fault_mode.store(3, Ordering::SeqCst);
                return Err(ControlError::Precondition(
                    "injected partial runtime pointer apply failure".to_owned(),
                ));
            }
            3 => {
                self.fault_mode.store(0, Ordering::SeqCst);
                return Err(ControlError::Precondition(
                    "injected rollback recovery apply failure".to_owned(),
                ));
            }
            _ => {}
        }
        self.store.replace(config);
        Ok(())
    }
}

fn content_hash(seed: u32) -> ContentHash {
    let pair = format!("{seed:02x}");
    let hex: String = pair.chars().cycle().take(64).collect();
    ContentHash::parse(format!("blake3:{hex}")).expect("hash")
}

async fn harness(db: &DatabaseConnection) -> GovernanceHarness {
    let config = RuntimeConfig::default();
    let store = Arc::new(RuntimeConfigStore::new(config.clone()));
    let runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository> =
        Arc::new(PgRuntimeConfigVersionRepository::new(db.clone()));
    bootstrap_runtime_config_activation(runtime_config_repo.as_ref(), &config).await;

    let artifact_store: Arc<dyn ArtifactStore> =
        Arc::new(LocalArtifactStore::new(std::env::temp_dir().join(format!(
            "qp_governance_e2e_{}_{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ))));

    let gate: Arc<dyn ModelQualityGate> = Arc::new(DefaultModelQualityGate::new());
    let runtime_apply = Arc::new(TestRuntimeConfigApply {
        store: Arc::clone(&store),
        fault_mode: AtomicU8::new(0),
    });
    let apply: Arc<dyn RuntimeConfigPort> = runtime_apply.clone();
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
        runtime_config_apply: apply,
        runtime_config_repo,
        feature_parity_gate: Arc::new(ClearFeatureParityGate {
            parity: parity_repo,
        }),
        frozen_model_parity,
    });
    GovernanceHarness {
        service,
        store,
        runtime_apply,
        artifact_store,
    }
}

async fn bootstrap_runtime_config_activation(
    repo: &dyn RuntimeConfigVersionRepository,
    config: &RuntimeConfig,
) {
    if repo
        .load_current_activation()
        .await
        .expect("activation")
        .is_some()
    {
        return;
    }
    let config_json = config.to_json();
    let config_hash = CanonicalDigest::content_hash_json(&config_json).expect("hash");
    let version = repo
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            config_hash,
            schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
            config_json,
            source: RuntimeConfigVersionSource::Bootstrap,
            created_by: "governance-it".to_owned(),
            reason: "governance integration test bootstrap".to_owned(),
        })
        .await
        .expect("runtime config version");
    repo.activate_version(NewRuntimeConfigActivation {
        runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
        runtime_config_version_id: version.runtime_config_version_id,
        activated_at: Utc::now(),
        activated_by: "governance-it".to_owned(),
        reason: "governance integration test bootstrap".to_owned(),
        activation_kind: RuntimeConfigActivationKind::Initial,
        previous_runtime_config_version_id: None,
        rollback_target_version_id: None,
        audit_event_id: None,
    })
    .await
    .expect("runtime config activation");
}

async fn seed_runtime_config(db: &DatabaseConnection) -> RuntimeConfigVersionId {
    let id = RuntimeConfigVersionId::from_v7();
    PgRuntimeConfigVersionRepository::new(db.clone())
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: id.clone(),
            config_hash: content_hash(u32::from('c')),
            schema_version: SchemaVersion::FIRST,
            config_json: serde_json::json!({}),
            source: RuntimeConfigVersionSource::Bootstrap,
            created_by: "governance-it".to_owned(),
            reason: "integration test".to_owned(),
        })
        .await
        .expect("runtime config");
    id
}

async fn seed_spec(db: &DatabaseConnection) -> ModelSpecId {
    let id = ModelSpecId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_spec(NewModelSpec {
            model_spec_id: id.clone(),
            name: "governance-it".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            prediction_horizon_secs: 86_400,
            feature_schema_version: SchemaVersion::FIRST,
            label_schema_version: SchemaVersion::FIRST,
            spec_json: serde_json::json!({}),
            input_contract: ModelInputContract::single_required("book.mid"),
            training_contract: ModelTrainingContract::settlement_default(),
            status: PublicationStatus::Published,
        })
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
        live_attribution_candidates: 0,
        live_attribution_dropped_missing_evidence: 0,
        matrix_probe: None,
        ..DatasetCoverage::default()
    }
}

/// A dataset coverage that clears the Sell-side gates (Phase 11.5.1):
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

fn healthy_training_examples(as_of: chrono::DateTime<Utc>) -> Vec<TrainingExample> {
    use quant_pivot_models::{
        enums::{factor::FactorFamily, quant::FactorDirection},
        types::{FactorDefinitionId, MarketId, TokenId, TrainingExampleId},
    };
    use quant_pivot_research::factors::{FactorExplanation, FactorValue, NormalizedFactor};

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
            example.decision_boundary = DecisionClock::new(0)
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
            example
        })
        .collect()
}

struct FrozenDatasetFixture {
    id: TrainingDatasetId,
    window_start: chrono::DateTime<Utc>,
    window_end: chrono::DateTime<Utc>,
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
    rc_id: &RuntimeConfigVersionId,
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
        training_dataset_id: id.clone(),
        model_spec_id: spec.clone(),
        runtime_config_version_id: rc_id.clone(),
        window_start,
        window_end,
        purpose: DatasetPurpose::Training,
        knowledge_lag_secs: 10,
        sample_interval_secs: 3_600,
        horizons_secs: vec![3_600],
        feature_schema_hash: feature_schema_hash.clone(),
        factor_schema_hash: factor_schema_hash.clone(),
        label_schema_hash: label_schema_hash.clone(),
        semantic_dataset_hash: dataset_hash.clone(),
        source_fingerprint: dataset_source_fingerprint(&examples).expect("source fingerprint"),
        sample_count: u64::try_from(sample_count).expect("manifest sample count"),
    };
    // Real Parquet so publish-time leakage rescan (#9) can decode bytes.
    let bytes = DatasetParquetCodec::encode(&examples, &manifest).expect("encode parquet");
    let manifest_hash = dataset_manifest_hash(&manifest).expect("manifest hash");
    let artifact_bytes_hash =
        ContentHash::parse(CanonicalDigest::prefixed_bytes(&bytes)).expect("bytes hash");
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
    rc_id: &RuntimeConfigVersionId,
    status: TrainingDatasetStatus,
    coverage_json: DatasetCoverage,
    dataset_hash_seed: char,
) -> TrainingDatasetId {
    let _ = dataset_hash_seed;
    let fixture = build_frozen_dataset_fixture(artifact_store, spec, rc_id).await;
    let dataset_repo = PgTrainingDatasetRepository::new(db.clone());
    dataset_repo
        .create_plan(NewTrainingDatasetPlan {
            training_dataset_id: fixture.id.clone(),
            model_spec_id: spec.clone(),
            window_start: fixture.window_start,
            window_end: fixture.window_end,
            purpose: DatasetPurpose::Training,
            knowledge_lag_secs: 10,
            sample_interval_secs: 3_600,
            horizons_secs: TrainingHorizonsSecs(vec![3600]),
            feature_schema_version: Some(SchemaVersion::FIRST),
            sample_sources: Some(TrainingSampleSources(default_sample_sources())),
            runtime_config_version_id: rc_id.clone(),
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
    calibrator_ref: Option<CalibrationArtifactId>,
) -> ModelVersionId {
    let id = ModelVersionId::from_v7();
    let return_model =
        calibrator_ref
            .as_ref()
            .map_or_else(ReturnModelSpec::heuristic_default, |calibrator| {
                ReturnModelSpec::Calibrated(CalibratedReturnModel {
                    calibrator_ref: calibrator.clone(),
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
            let examples =
                quant_pivot_core::service::training_dataset::verify_frozen_dataset_artifact(
                    &dataset_info,
                    &bytes,
                )
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
            (materialization.dataset_hash.clone(), input_hash)
        }
        None => (content_hash(u32::from('d')), content_hash(u32::from('i'))),
    };
    let artifact_hash = store_weighted_artifact(
        artifact_store,
        &id,
        seed,
        return_model,
        training_dataset_hash,
        training_input_hash,
    )
    .await;
    PgModelRegistryRepository::new(db.clone())
        .create_model_version(NewModelVersion {
            model_version_id: id.clone(),
            model_spec_id: spec.clone(),
            version,
            artifact_hash,
            training_dataset_id: dataset,
            publish_path_set_id: None,
            metrics_json: serde_json::json!({}),
            training_objective_json: serde_json::json!({"kind": "not_trained"}),
            quality_gate_report: serde_json::json!({}),
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
            model_version_id: model_version_id.clone(),
            model_family: ModelFamily::WeightedFactor,
            feature_schema_hash: content_hash(u32::from('f')),
            factor_schema_hash: content_hash(u32::from('g')),
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

/// A `HoldVsExitWeighted` model spec (Phase 11.5.1 sell-side governance tests).
async fn seed_sell_spec(db: &DatabaseConnection) -> ModelSpecId {
    let id = ModelSpecId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_spec(NewModelSpec {
            model_spec_id: id.clone(),
            name: "governance-it-sell".to_owned(),
            model_family: ModelFamily::HoldVsExitWeighted,
            prediction_horizon_secs: 86_400,
            feature_schema_version: SchemaVersion::FIRST,
            label_schema_version: SchemaVersion::FIRST,
            spec_json: serde_json::json!({}),
            input_contract: ModelInputContract::single_required("book.mid"),
            training_contract: ModelTrainingContract::settlement_default(),
            status: PublicationStatus::Published,
        })
        .await
        .expect("sell model spec");
    id
}

/// A `HoldVsExitWeighted` candidate version with a validated
/// [`SellScorerArtifact`] (Phase 11.5.1). Sell scorers never carry a return
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
            let examples =
                quant_pivot_core::service::training_dataset::verify_frozen_dataset_artifact(
                    &dataset_info,
                    &bytes,
                )
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
            (materialization.dataset_hash.clone(), input_hash)
        }
        None => (content_hash(u32::from('d')), content_hash(u32::from('i'))),
    };
    let input_contract = ModelInputContract::single_required("book.mid");
    let input_contract_hash =
        model_input_contract_hash(&input_contract).expect("input contract hash");
    let artifact = ModelArtifact::SellScorer(Box::new(SellScorerArtifact {
        header: ModelArtifactHeader {
            model_version_id: id.clone(),
            model_family: ModelFamily::HoldVsExitWeighted,
            feature_schema_hash: content_hash(u32::from('f')),
            factor_schema_hash: content_hash(u32::from('g')),
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
            model_version_id: id.clone(),
            model_spec_id: spec.clone(),
            version,
            artifact_hash,
            training_dataset_id: dataset,
            publish_path_set_id: None,
            metrics_json: serde_json::json!({}),
            training_objective_json: serde_json::json!({"kind": "not_trained"}),
            quality_gate_report: serde_json::json!({}),
            publication_status: PublicationStatus::Candidate,
            published_at: None,
            retired_at: None,
        })
        .await
        .expect("sell model version");
    id
}

async fn seed_model_score_calibrator(db: &DatabaseConnection) -> CalibrationArtifactId {
    let artifact_id = CalibrationArtifactId::from_v7();
    let window_start = Utc::now() - ChronoDuration::days(90);
    let window_end = Utc::now() - ChronoDuration::days(1);
    let fit_window = TimeWindow::new(window_start, window_end);
    let split_hash = content_hash(u32::from('s'));
    let payload = ModelScoreCalibrationPayload {
        model_version_id: ModelVersionId::from_v7(),
        calibration_dataset_id: TrainingDatasetId::from_v7(),
        mapping: MonotoneMapping::Isotonic { knots: vec![] },
        reliability: ReliabilityReport {
            bins: vec![],
            brier_score: dec!(0.1),
            log_loss: dec!(0.4),
            ece: dec!(0.05),
            n_samples: 100,
        },
    };
    // Content hash + `active: true` mirror what `bind_calibration` produces
    // in production (self-contained hash, activated on bind) — a seeded
    // fixture that would pass `CoreCalibrationArtifactLoader`'s fail-closed
    // hash/active verification exactly like a real one, not a shortcut that
    // only happens to work because these tests never load it.
    let content_hash = model_score_content_hash(&fit_window, &split_hash, &payload)
        .expect("model-score content hash");
    PgCalibrationArtifactRepository::new(db.clone())
        .create(NewCalibrationArtifact {
            artifact_id: artifact_id.clone(),
            kind: CalibrationKind::ModelScore,
            content_hash,
            fit_window_start: window_start,
            fit_window_end: window_end,
            calibration_split_hash: split_hash,
            sample_count: 100,
            payload_json: serde_json::to_value(payload).expect("payload"),
            active: true,
        })
        .await
        .expect("calibration artifact");
    artifact_id
}

async fn seed_backtest(
    db: &DatabaseConnection,
    version: &ModelVersionId,
    rc_id: &RuntimeConfigVersionId,
    hash_seed: char,
) {
    let model_run_id = ModelRunId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::Backtest,
            model_version_id: Some(version.clone()),
            runtime_config_version_id: rc_id.clone(),
            market_selection_id: None,
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            status: ModelRunStatus::Succeeded,
            input_hash: content_hash(u32::from(hash_seed)),
            output_hash: Some(content_hash(u32::from(hash_seed) + 1)),
            metrics_json: serde_json::json!({}),
            error_code: None,
            error_message: None,
            started_at: window_start,
            finished_at: Some(Utc::now()),
        })
        .await
        .expect("model run");

    let evr = serde_json::to_value(ExpectedVsRealized {
        mean_expected_bps: dec!(120),
        mean_realized_bps: dec!(110),
        correlation: dec!(0.4),
        bias_bps: dec!(10),
    })
    .expect("evr");
    let pnl = serde_json::to_value(PnlSimulation {
        total_allocated_usd: dec!(10000),
        realized_pnl_usd: dec!(500),
        gross_return: dec!(0.05),
        pnl_curve: Vec::new(),
    })
    .expect("pnl");

    PgBacktestReportRepository::new(db.clone())
        .create(NewBacktestReport {
            backtest_report_id: BacktestReportId::from_v7(),
            model_version_id: version.clone(),
            model_run_id,
            runtime_config_version_id: rc_id.clone(),
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            coverage: dec!(0.99),
            sample_count: 1_000,
            missing_feature_count: 0,
            rank_ic: dec!(0.15),
            sharpe: dec!(1.1),
            hit_rate: Probability::new(dec!(0.62)),
            expected_vs_realized: evr,
            max_drawdown: dec!(0.10),
            turnover: dec!(0.2),
            liquidity_feasibility: Probability::new(dec!(0.95)),
            category_breakdown: serde_json::json!([]),
            tail_loss: dec!(-50),
            report_pnl_simulation: pnl,
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
            active_model_version_id: active.clone(),
            shadow_model_version_id: shadow.clone(),
            decision_at: now - ChronoDuration::hours(hours_ago),
            topn_overlap: Probability::new(overlap),
            rank_delta_json: serde_json::json!({}),
            score_delta_json: serde_json::json!({}),
            matured_outcome_json: None,
            hard_divergence: false,
            comparison_hash: content_hash(u32::from(seed_base) + offset),
        })
        .await
        .expect("shadow comparison");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn publish_requires_quality_gate_pass() {
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
        None,
    )
    .await;

    let result = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate.clone(),
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
    assert_eq!(
        row.quality_gate_report.get("passed"),
        Some(&serde_json::json!(false)),
        "failed publish must persist quality_gate_report.passed=false"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn publish_without_training_dataset_is_illegal_transition() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let _rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;
    // Missing training_dataset_id fails closed before gate evaluation — no
    // durable gate report is written (distinct from QualityGateFailed).
    let candidate = seed_version(&db, &harness.artifact_store, &spec, 'a', 1, None, None).await;

    let result = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate.clone(),
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
        row.quality_gate_report.get("passed").is_none(),
        "pre-gate IllegalTransition must not invent a quality_gate_report"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn publish_requires_backtest_report() {
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
        None,
    )
    .await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;
    // Deliberately no backtest report.

    let result = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate.clone(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn publish_requires_shadow_stability() {
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
        None,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn publish_succeeds_then_published_version_is_immutable() {
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
    let calibrator = seed_model_score_calibrator(&db).await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        'a',
        1,
        Some(dataset),
        Some(calibrator),
    )
    .await;
    seed_backtest(&db, &candidate, &rc_id, '1').await;
    seed_path_set(&db, &candidate, &rc_id, '1').await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;

    let published = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate.clone(),
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
    assert!(audits[0].quality_gate_passed);

    assert_eq!(
        harness
            .store
            .current()
            .model
            .active_model_version_id
            .as_ref()
            .map(|reference| reference.id.as_str()),
        Some(candidate.to_string().as_str()),
        "publish must wire the active runtime-config pointer"
    );
    assert!(
        harness
            .store
            .current()
            .model
            .shadow_model_version_id
            .is_none(),
        "publish must clear the shadow slot"
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
    rc_id: &'a RuntimeConfigVersionId,
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
                model_version_id: params.version.clone(),
                reason: params.reason.to_owned(),
            },
            GovernanceActor::system(),
        )
        .await
        .expect("publish ready version");
}

/// Seed a CPCV path set that clears the Phase 11.5 alpha hard gates.
async fn seed_path_set(
    db: &DatabaseConnection,
    version: &ModelVersionId,
    rc_id: &RuntimeConfigVersionId,
    hash_seed: char,
) {
    let model_run_id = ModelRunId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::Cpcv,
            model_version_id: Some(version.clone()),
            runtime_config_version_id: rc_id.clone(),
            market_selection_id: None,
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            status: ModelRunStatus::Succeeded,
            input_hash: content_hash(u32::from(hash_seed) + 10),
            output_hash: Some(content_hash(u32::from(hash_seed) + 11)),
            metrics_json: serde_json::json!({}),
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
            path_set_id: path_set_id.clone(),
            model_version_id: version.clone(),
            model_run_id,
            training_dataset_id,
            runtime_config_version_id: rc_id.clone(),
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            path_count: 7,
            combination_count: 28,
            median_rank_ic: dec!(0.15),
            sharpe_distribution: serde_json::json!({
                "min": "0.5",
                "p25": "0.8",
                "median": "1.0",
                "p75": "1.2",
                "max": "1.5",
                "median_max_drawdown": "0.10",
                "median_tail_loss": "-0.005",
                "baseline_uplift": "0.001"
            }),
            paths: serde_json::json!([]),
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

/// Seeds for [`seed_and_publish_version`].
struct PublishableVersionSeeds {
    dataset_seed: char,
    version_seed: char,
    version_number: i32,
    backtest_seed: char,
    shadow_seed: char,
    publish_reason: &'static str,
}

/// Build a Ready dataset, train a candidate version, and publish it through every gate.
async fn seed_and_publish_version(
    harness: &GovernanceHarness,
    db: &DatabaseConnection,
    spec: &ModelSpecId,
    rc_id: &RuntimeConfigVersionId,
    seeds: PublishableVersionSeeds,
) -> ModelVersionId {
    let dataset = seed_dataset(
        db,
        &harness.artifact_store,
        spec,
        rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        seeds.dataset_seed,
    )
    .await;
    let calibrator = seed_model_score_calibrator(db).await;
    let version = seed_version(
        db,
        &harness.artifact_store,
        spec,
        seeds.version_seed,
        seeds.version_number,
        Some(dataset),
        Some(calibrator),
    )
    .await;
    publish_ready_version(PublishReadyParams {
        governance: &harness.service,
        db,
        rc_id,
        version: &version,
        active_for_shadow: &version,
        backtest_seed: seeds.backtest_seed,
        shadow_seed: seeds.shadow_seed,
        reason: seeds.publish_reason,
    })
    .await;
    version
}

async fn assert_rollback_restored_predecessor(
    db: &DatabaseConnection,
    store: &RuntimeConfigStore,
    rolled_back: &ModelVersionId,
    predecessor: &ModelVersionId,
    rollback_reason: &str,
) {
    let registry = PgModelRegistryRepository::new(db.clone());
    let rolled_back_row = registry
        .find_model_version_by_id(rolled_back)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(
        rolled_back_row.publication_status,
        PublicationStatus::Retired
    );
    let predecessor_row = registry
        .find_model_version_by_id(predecessor)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(
        predecessor_row.publication_status,
        PublicationStatus::Published,
        "rollback must re-publish the retired predecessor"
    );
    assert_eq!(
        store
            .current()
            .model
            .active_model_version_id
            .as_ref()
            .map(|reference| reference.id.as_str()),
        Some(predecessor.to_string().as_str()),
        "rollback must wire runtime config back to the restored version"
    );
    let audits = PgModelGovernanceAuditRepository::new(db.clone())
        .list_by_version(rolled_back)
        .await
        .expect("audits");
    let rollback_audit = audits
        .iter()
        .find(|audit| audit.reason == rollback_reason)
        .expect("the rollback reason is audited");
    assert!(
        rollback_audit.quality_gate_passed,
        "rollback must audit a freshly passed publish gate"
    );
    assert_eq!(
        rollback_audit.rollback_target_version_id.as_ref(),
        Some(predecessor)
    );
    assert!(
        rollback_audit
            .detail_json
            .get("feature_parity_run_id")
            .and_then(serde_json::Value::as_str)
            .is_some(),
        "rollback audit must bind the exact subject full-parity permit"
    );
    assert!(
        rollback_audit
            .detail_json
            .get("feature_parity_state_id")
            .and_then(serde_json::Value::as_str)
            .is_some(),
        "rollback audit must bind the exact clear-latch generation"
    );
    assert_eq!(
        predecessor_row
            .quality_gate_report
            .get("passed")
            .and_then(serde_json::Value::as_bool),
        Some(true),
        "rollback target must persist its current publish gate report"
    );
}

async fn assert_single_active_published(
    registry: &PgModelRegistryRepository,
    spec: &ModelSpecId,
    retired: &ModelVersionId,
    active: &ModelVersionId,
    store: &RuntimeConfigStore,
) {
    let retired_row = registry
        .find_model_version_by_id(retired)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(
        retired_row.publication_status,
        PublicationStatus::Retired,
        "publish must retire the predecessor"
    );
    assert_eq!(
        registry
            .list_published_for_spec(spec)
            .await
            .expect("published")
            .len(),
        1,
        "only one published version may exist per spec"
    );
    assert_eq!(
        store
            .current()
            .model
            .active_model_version_id
            .as_ref()
            .map(|reference| reference.id.as_str()),
        Some(active.to_string().as_str()),
        "runtime config must point at the active published version"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn rollback_retains_previous_and_writes_reason() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;

    let v1 = seed_and_publish_version(
        &harness,
        &db,
        &spec,
        &rc_id,
        PublishableVersionSeeds {
            dataset_seed: '1',
            version_seed: 'a',
            version_number: 1,
            backtest_seed: '1',
            shadow_seed: 'a',
            publish_reason: "publish v1",
        },
    )
    .await;
    let v2 = seed_and_publish_version(
        &harness,
        &db,
        &spec,
        &rc_id,
        PublishableVersionSeeds {
            dataset_seed: '2',
            version_seed: 'b',
            version_number: 2,
            backtest_seed: '2',
            shadow_seed: 'm',
            publish_reason: "publish v2",
        },
    )
    .await;

    let registry = PgModelRegistryRepository::new(db.clone());
    assert_single_active_published(&registry, &spec, &v1, &v2, &harness.store).await;

    let restored = harness
        .service
        .rollback(
            RollbackModelCommand {
                model_version_id: v2.clone(),
                reason: "v2 regressed".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await
        .expect("rollback");
    assert_eq!(
        restored.model_version_id, v1,
        "rollback restores the predecessor"
    );
    assert_rollback_restored_predecessor(&db, &harness.store, &v2, &v1, "v2 regressed").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn rollback_broken_pointer_preflight_leaves_registry_untouched() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;

    let predecessor = seed_and_publish_version(
        &harness,
        &db,
        &spec,
        &rc_id,
        PublishableVersionSeeds {
            dataset_seed: 'b',
            version_seed: 'b',
            version_number: 1,
            backtest_seed: 'b',
            shadow_seed: 'b',
            publish_reason: "publish preflight target",
        },
    )
    .await;
    let current = seed_and_publish_version(
        &harness,
        &db,
        &spec,
        &rc_id,
        PublishableVersionSeeds {
            dataset_seed: 'c',
            version_seed: 'c',
            version_number: 2,
            backtest_seed: 'c',
            shadow_seed: 'c',
            publish_reason: "publish preflight current",
        },
    )
    .await;
    let mut broken_live = (*harness.store.current()).clone();
    broken_live.model.active_model_version_id = Some(ModelVersionRef {
        id: predecessor.to_string(),
    });
    harness.store.replace(broken_live);

    let error = harness
        .service
        .rollback(
            RollbackModelCommand {
                model_version_id: current.clone(),
                reason: "must reject broken pointer preflight".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await
        .expect_err("broken live/durable ownership must fail before status CAS");
    assert!(
        error
            .to_string()
            .contains("live runtime config does not point")
    );

    let registry = PgModelRegistryRepository::new(db.clone());
    assert_eq!(
        registry
            .find_model_version_by_id(&current)
            .await
            .expect("current lookup")
            .expect("current row")
            .publication_status,
        PublicationStatus::Published
    );
    assert_eq!(
        registry
            .find_model_version_by_id(&predecessor)
            .await
            .expect("target lookup")
            .expect("target row")
            .publication_status,
        PublicationStatus::Retired
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn rollback_pointer_apply_failure_compensates_config_and_registry_atomically() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;

    let predecessor = seed_and_publish_version(
        &harness,
        &db,
        &spec,
        &rc_id,
        PublishableVersionSeeds {
            dataset_seed: '7',
            version_seed: '7',
            version_number: 1,
            backtest_seed: '7',
            shadow_seed: '7',
            publish_reason: "publish pointer-failure target",
        },
    )
    .await;
    let current = seed_and_publish_version(
        &harness,
        &db,
        &spec,
        &rc_id,
        PublishableVersionSeeds {
            dataset_seed: '8',
            version_seed: '8',
            version_number: 2,
            backtest_seed: '8',
            shadow_seed: '8',
            publish_reason: "publish pointer-failure current",
        },
    )
    .await;
    harness.runtime_apply.fail_next_apply();

    let error = harness
        .service
        .rollback(
            RollbackModelCommand {
                model_version_id: current.clone(),
                reason: "inject pointer apply failure".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await
        .expect_err("failed live pointer apply must surface to the operator");
    assert!(error.to_string().contains("outcome=compensated"));

    let registry = PgModelRegistryRepository::new(db.clone());
    assert_single_active_published(&registry, &spec, &predecessor, &current, &harness.store).await;
    let durable = PgRuntimeConfigVersionRepository::new(db.clone())
        .load_current()
        .await
        .expect("durable runtime config")
        .expect("active runtime config");
    let durable_config = RuntimeConfig::from_json(&durable.config_json).expect("typed config");
    assert_eq!(
        durable_config
            .model
            .active_model_version_id
            .as_ref()
            .map(|reference| reference.id.as_str()),
        Some(current.to_string().as_str()),
        "restart must load the same compensated current model as the live store"
    );
    let audits = PgModelGovernanceAuditRepository::new(db.clone())
        .list_by_version(&current)
        .await
        .expect("rollback audit");
    let compensated = audits
        .iter()
        .find(|audit| audit.reason == "inject pointer apply failure")
        .expect("compensated rollback audit");
    assert!(compensated.quality_gate_passed);
    assert_eq!(
        compensated
            .detail_json
            .get("outcome")
            .and_then(serde_json::Value::as_str),
        Some("compensated")
    );
    assert_eq!(
        compensated
            .detail_json
            .get("live_reverted")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn rollback_partial_live_apply_failure_opens_global_safety_latch() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;

    let predecessor = seed_and_publish_version(
        &harness,
        &db,
        &spec,
        &rc_id,
        PublishableVersionSeeds {
            dataset_seed: '9',
            version_seed: '9',
            version_number: 1,
            backtest_seed: '9',
            shadow_seed: '9',
            publish_reason: "publish partial-failure target",
        },
    )
    .await;
    let current = seed_and_publish_version(
        &harness,
        &db,
        &spec,
        &rc_id,
        PublishableVersionSeeds {
            dataset_seed: 'a',
            version_seed: 'a',
            version_number: 2,
            backtest_seed: 'a',
            shadow_seed: 'a',
            publish_reason: "publish partial-failure current",
        },
    )
    .await;
    harness
        .runtime_apply
        .partially_apply_target_then_fail_recovery();

    let error = harness
        .service
        .rollback(
            RollbackModelCommand {
                model_version_id: current.clone(),
                reason: "inject partial pointer failure".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await
        .expect_err("partial apply plus failed recovery must fail closed");
    assert!(
        error
            .to_string()
            .contains("outcome=compensated_durable_live_fail_closed")
    );

    assert_partial_rollback_failure_state(&db, &harness, &predecessor, &current).await;
}

async fn assert_partial_rollback_failure_state(
    db: &DatabaseConnection,
    harness: &GovernanceHarness,
    predecessor: &ModelVersionId,
    current: &ModelVersionId,
) {
    let registry = PgModelRegistryRepository::new(db.clone());
    assert_eq!(
        registry
            .find_model_version_by_id(predecessor)
            .await
            .expect("predecessor lookup")
            .expect("predecessor row")
            .publication_status,
        PublicationStatus::Retired
    );
    assert_eq!(
        registry
            .find_model_version_by_id(current)
            .await
            .expect("current lookup")
            .expect("current row")
            .publication_status,
        PublicationStatus::Published
    );
    assert_eq!(
        harness
            .store
            .current()
            .model
            .active_model_version_id
            .as_ref()
            .map(|reference| reference.id.as_str()),
        Some(predecessor.to_string().as_str()),
        "the injected partial live state remains observable for the fail-closed assertion"
    );
    let durable = PgRuntimeConfigVersionRepository::new(db.clone())
        .load_current()
        .await
        .expect("durable runtime config")
        .expect("active runtime config");
    let durable_config = RuntimeConfig::from_json(&durable.config_json).expect("typed config");
    assert_eq!(
        durable_config
            .model
            .active_model_version_id
            .as_ref()
            .map(|reference| reference.id.as_str()),
        Some(current.to_string().as_str()),
        "durable config and model registry are compensated together"
    );
    let latch = PgFeatureParityRepository::new(db.clone())
        .current_state()
        .await
        .expect("latch read")
        .expect("safety latch state");
    assert_eq!(latch.state, FeatureParityLatchState::Open);
    assert!(latch.cause_run_id.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn concurrent_rollbacks_commit_exactly_one_complete_switch() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;

    let predecessor = seed_and_publish_version(
        &harness,
        &db,
        &spec,
        &rc_id,
        PublishableVersionSeeds {
            dataset_seed: '5',
            version_seed: 'e',
            version_number: 1,
            backtest_seed: '5',
            shadow_seed: 'e',
            publish_reason: "publish concurrent rollback target",
        },
    )
    .await;
    let current = seed_and_publish_version(
        &harness,
        &db,
        &spec,
        &rc_id,
        PublishableVersionSeeds {
            dataset_seed: '6',
            version_seed: 'f',
            version_number: 2,
            backtest_seed: '6',
            shadow_seed: 'f',
            publish_reason: "publish concurrent rollback current",
        },
    )
    .await;

    let left = harness.service.rollback(
        RollbackModelCommand {
            model_version_id: current.clone(),
            reason: "concurrent rollback left".to_owned(),
        },
        GovernanceActor::system(),
    );
    let right = harness.service.rollback(
        RollbackModelCommand {
            model_version_id: current.clone(),
            reason: "concurrent rollback right".to_owned(),
        },
        GovernanceActor::system(),
    );
    let (left_result, right_result) = tokio::join!(left, right);
    assert_eq!(
        usize::from(left_result.is_ok()) + usize::from(right_result.is_ok()),
        1,
        "the spec/current compare-and-swap must admit exactly one rollback"
    );

    let registry = PgModelRegistryRepository::new(db.clone());
    assert_single_active_published(&registry, &spec, &current, &predecessor, &harness.store).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn rollback_quality_gate_failure_persists_report_without_half_switching() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;

    let v1 = seed_and_publish_version(
        &harness,
        &db,
        &spec,
        &rc_id,
        PublishableVersionSeeds {
            dataset_seed: '3',
            version_seed: 'c',
            version_number: 1,
            backtest_seed: '3',
            shadow_seed: 'c',
            publish_reason: "publish rollback target",
        },
    )
    .await;
    let v2 = seed_and_publish_version(
        &harness,
        &db,
        &spec,
        &rc_id,
        PublishableVersionSeeds {
            dataset_seed: '4',
            version_seed: 'd',
            version_number: 2,
            backtest_seed: '4',
            shadow_seed: 'd',
            publish_reason: "publish current",
        },
    )
    .await;
    let registry = PgModelRegistryRepository::new(db.clone());
    registry
        .set_publish_path_set_id(&v1, None)
        .await
        .expect("invalidate retired target's current CPCV binding");

    let error = harness
        .service
        .rollback(
            RollbackModelCommand {
                model_version_id: v2.clone(),
                reason: "must not bypass current quality".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await
        .expect_err("rollback must run today's publish gate");
    assert!(matches!(
        error,
        QuantError::Governance(
            quant_pivot_error::governance::GovernanceError::QualityGateFailed { .. }
        )
    ));

    let current = registry
        .find_model_version_by_id(&v2)
        .await
        .expect("current lookup")
        .expect("current row");
    let target = registry
        .find_model_version_by_id(&v1)
        .await
        .expect("target lookup")
        .expect("target row");
    assert_eq!(current.publication_status, PublicationStatus::Published);
    assert_eq!(target.publication_status, PublicationStatus::Retired);
    assert_eq!(
        target
            .quality_gate_report
            .get("passed")
            .and_then(serde_json::Value::as_bool),
        Some(false),
        "failed rollback gate report remains durable for operator diagnosis"
    );
    assert_eq!(
        harness
            .store
            .current()
            .model
            .active_model_version_id
            .as_ref()
            .map(|reference| reference.id.as_str()),
        Some(v2.to_string().as_str()),
        "runtime pointer must remain on the exact published current"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn retire_published_version_clears_runtime_pointer_and_audits() {
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
    let calibrator = seed_model_score_calibrator(&db).await;
    let version = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        'a',
        1,
        Some(dataset),
        Some(calibrator),
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
                model_version_id: version.clone(),
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
            .model
            .active_model_version_id
            .is_none(),
        "retire must clear the active runtime pointer"
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn retire_published_version_clears_dangling_category_pointer() {
    // 11.2.2 remediation R7: a category-specific pointer left dangling after
    // its target retires must never survive silently — the retire-sync must
    // clear it in the same activation as the generic active pointer, so the
    // online runner never routes another round onto a dead version.
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
    let calibrator = seed_model_score_calibrator(&db).await;
    let version = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        'a',
        1,
        Some(dataset),
        Some(calibrator),
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
    config.model.category_model_pointers.insert(
        MarketCategory::Crypto,
        ModelVersionRef {
            id: version.to_string(),
        },
    );
    harness.store.replace(config);
    assert!(
        harness
            .store
            .current()
            .model
            .category_model_pointers
            .contains_key(&MarketCategory::Crypto),
        "precondition: category pointer is armed before retire"
    );

    harness
        .service
        .retire(
            RetireModelCommand {
                model_version_id: version.clone(),
                reason: "decommission with category pointer".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await
        .expect("retire");

    assert!(
        harness
            .store
            .current()
            .model
            .active_model_version_id
            .is_none(),
        "retire must clear the active runtime pointer"
    );
    assert!(
        !harness
            .store
            .current()
            .model
            .category_model_pointers
            .contains_key(&MarketCategory::Crypto),
        "retire must also clear a dangling category_model_pointers entry"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn uncalibrated_return_model_cannot_publish() {
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
        None,
    )
    .await;
    seed_backtest(&db, &candidate, &rc_id, '1').await;
    seed_path_set(&db, &candidate, &rc_id, '1').await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;

    let result = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate.clone(),
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
    let gate = row.quality_gate_report;
    assert!(
        gate.get("hard_failures")
            .and_then(|v| v.as_array())
            .is_some_and(|failures| {
                failures.iter().any(|failure| {
                    failure.get("gate").and_then(|g| g.as_str()) == Some("calibration_required")
                })
            }),
        "publish gate must record CalibrationRequired failure"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn bind_calibration_creates_candidate_version_with_calibrated_return_model() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;
    let candidate = seed_version(&db, &harness.artifact_store, &spec, 'a', 1, None, None).await;
    let calibrator = seed_model_score_calibrator(&db).await;

    let bound = harness
        .service
        .bind_calibration(
            &candidate,
            BindCalibrationRequest {
                calibrator_ref: calibrator.clone(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn publish_rescans_leakage_not_default_findings() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;

    let dataset_id = seed_leaking_dataset(&db, &harness.artifact_store, &spec, &rc_id).await;
    let calibrator = seed_model_score_calibrator(&db).await;
    let candidate = seed_version(
        &db,
        &harness.artifact_store,
        &spec,
        'L',
        1,
        Some(dataset_id),
        Some(calibrator),
    )
    .await;
    seed_backtest(&db, &candidate, &rc_id, '1').await;
    seed_path_set(&db, &candidate, &rc_id, '1').await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;

    let result = harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate.clone(),
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
    let gate = &row.quality_gate_report;
    let hard = gate
        .get("hard_failures")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        hard.iter().any(|failure| {
            failure.get("gate").and_then(|g| g.as_str()) == Some("no_pit_leakage")
        }),
        "gate report must record NoPitLeakage from rescan, got: {gate}"
    );
}

/// One training example whose feature evidence is observed after `as_of`.
fn leaking_training_example() -> TrainingExample {
    use chrono::TimeZone;
    use quant_pivot_models::{
        enums::quant::DataQualityStatus,
        types::{MarketId, SchemaVersion, TokenId, TrainingExampleId, TrainingSampleSource},
    };
    use quant_pivot_research::features::{EvidenceSourceKind, EvidenceSourceRef, FeatureVector};
    use quant_pivot_research::training::{LabelName, TrainingExample, TrainingLabel};
    use std::collections::BTreeMap;

    let as_of = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    TrainingExample {
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
            matured_at: as_of,
        }],
        source_refs: vec![EvidenceSourceRef {
            source_kind: EvidenceSourceKind::Derived,
            reference: "future-leak".to_owned(),
            effective_at: as_of + ChronoDuration::seconds(60),
            available_at: Some(as_of),
        }],
        decision_capture: None,
        lot_context: None,
        position_state: None,
        book_fidelity: None,
    }
}

/// Persist a Ready dataset whose Parquet fails publish-time PIT leakage rescan.
async fn seed_leaking_dataset(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    spec: &ModelSpecId,
    rc_id: &RuntimeConfigVersionId,
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
    let manifest = DatasetManifest {
        format_version: DATASET_ARTIFACT_FORMAT_VERSION,
        training_dataset_id: dataset_id.clone(),
        model_spec_id: spec.clone(),
        runtime_config_version_id: rc_id.clone(),
        window_start,
        window_end,
        purpose: DatasetPurpose::Training,
        knowledge_lag_secs: 0,
        sample_interval_secs: 3_600,
        horizons_secs: vec![0],
        feature_schema_hash: feature_schema_hash.clone(),
        factor_schema_hash: factor_schema_hash.clone(),
        label_schema_hash: label_schema_hash.clone(),
        semantic_dataset_hash: dataset_hash.clone(),
        source_fingerprint: dataset_source_fingerprint(&examples).expect("source fingerprint"),
        sample_count: u64::try_from(sample_count).expect("manifest sample count"),
    };
    let bytes = DatasetParquetCodec::encode(&examples, &manifest).expect("encode");
    let manifest_hash = dataset_manifest_hash(&manifest).expect("manifest hash");
    let artifact_bytes_hash =
        ContentHash::parse(CanonicalDigest::prefixed_bytes(&bytes)).expect("bytes hash");
    let hex = dataset_id.as_uuid().simple().to_string();
    let key = ArtifactKey::new(ArtifactNamespace::Dataset, hex, "parquet").expect("key");
    let parquet_uri = artifact_store.put(key, &bytes).await.expect("store");
    let dataset_repo = PgTrainingDatasetRepository::new(db.clone());
    dataset_repo
        .create_plan(NewTrainingDatasetPlan {
            training_dataset_id: dataset_id.clone(),
            model_spec_id: spec.clone(),
            window_start,
            window_end,
            purpose: DatasetPurpose::Training,
            knowledge_lag_secs: 0,
            sample_interval_secs: 3_600,
            horizons_secs: TrainingHorizonsSecs(vec![0]),
            feature_schema_version: Some(SchemaVersion::FIRST),
            sample_sources: Some(TrainingSampleSources(default_sample_sources())),
            runtime_config_version_id: rc_id.clone(),
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

/// Phase 11.5.1: Sell (`HoldVsExitWeighted`) publish requires a bound CPCV
/// path set exactly like Buy — `bound_path_set` is family-agnostic, and
/// `evaluate_gate` no longer excludes exit scorers from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn sell_publish_requires_bound_cpcv_path_set() {
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
                model_version_id: candidate.clone(),
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

/// Phase 11.5.1: once a lot-level CPCV path set clearing the alpha-
/// significance hard gates is explicitly bound, Sell publish succeeds
/// through the same governance closure Buy uses (no calibrator needed —
/// `CalibrationRequired` is `NotApplicable` for exit scorers).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn sell_publish_succeeds_with_bound_cpcv_path_set() {
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
    // <= 0.5) with the exact same defaults for Buy and Sell (Phase 11.5.1
    // §4 mirrors `research.validation.gates.*` under `quality_gate.sell.*`).
    seed_path_set(&db, &candidate, &rc_id, '1').await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;

    harness
        .service
        .publish(
            PublishModelCommand {
                model_version_id: candidate.clone(),
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
