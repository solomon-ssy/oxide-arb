//! Model-governance closure integration tests (Postgres + testcontainers).
//!
//! Exercises the publish / rollback / dataset-promotion orchestration end to end
//! against real repositories + the default quality gate: gate-pass and
//! shadow-stability enforcement, published-version immutability, rollback
//! restoration, runtime-config pointer sync, and the `InsufficientLabels`
//! promotion block.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_core::governance::{
    CoreCalibrationArtifactLoader, ModelGovernanceDeps, ModelGovernanceService,
    ModelScoreCalibrationPayload, model_score_content_hash,
};
use quant_pivot_core::runtime_config::RuntimeConfigStore;
use quant_pivot_error::control::ControlError;
use quant_pivot_models::domain::TimeWindow;
use quant_pivot_models::enums::quant::DatasetPurpose;
use quant_pivot_models::{
    domain::{
        BindCalibrationRequest, GovernanceActor, ModelGovernancePort, NewBacktestPathSet,
        NewBacktestReport, NewCalibrationArtifact, NewModelRun, NewModelSpec, NewModelVersion,
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, NewShadowComparison,
        NewTrainingDataset, PromoteDatasetRequest, PublishModelCommand, RetireModelCommand,
        RollbackModelCommand, RuntimeConfigPort,
    },
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{
            CalibrationKind, DownsideSource, ModelRunKind, ModelRunStatus, PublicationStatus,
            TrainingDatasetStatus,
        },
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    hashing::CanonicalDigest,
    runtime_config::{ModelVersionRef, RUNTIME_CONFIG_SCHEMA_VERSION, RuntimeConfig},
    types::{
        BacktestPathSetId, BacktestReportId, CalibrationArtifactId, ContentHash, DatasetCoverage,
        ModelRunId, ModelSpecId, ModelVersionId, Probability, RuntimeConfigActivationId,
        RuntimeConfigVersionId, SchemaVersion, ShadowComparisonId, TrainingDatasetId,
        TrainingHorizonsSecs,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestPathSetRepository, PgBacktestReportRepository, PgCalibrationArtifactRepository,
        PgModelGovernanceAuditRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgRuntimeConfigVersionRepository, PgShadowComparisonRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        BacktestPathSetRepository, BacktestReportRepository, CalibrationArtifactRepository,
        ModelGovernanceAuditRepository, ModelRegistryRepository, ModelRunRepository,
        RuntimeConfigVersionRepository, ShadowComparisonRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::training::TrainingExample;
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
    backtest::{ExpectedVsRealized, PnlSimulation},
    factors::names::LIQUIDITY_DEPTH,
    gates::{DefaultModelQualityGate, ModelQualityGate},
    model::{
        CalibratedReturnModel, CalibrationArtifactLoader, FactorWeight, ModelArtifact,
        ModelArtifactHeader, ReturnModelSpec, ScoreMultiplierSpec, SellScorerArtifact,
        SellScorerOutputSpec, SubstitutionConfidenceRules, WeightedFactorModelArtifact,
    },
    model::{MonotoneMapping, ReliabilityReport},
    training::DatasetParquetCodec,
};
use quant_pivot_test_support::pg::setup_pg;
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

/// Test harness wiring governance against a real store + config repo.
struct GovernanceHarness {
    service: ModelGovernanceService,
    store: Arc<RuntimeConfigStore>,
    artifact_store: Arc<dyn ArtifactStore>,
}

/// Minimal [`RuntimeConfigPort`] for integration tests (store swap only).
struct TestRuntimeConfigApply {
    store: Arc<RuntimeConfigStore>,
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
    let apply: Arc<dyn RuntimeConfigPort> = Arc::new(TestRuntimeConfigApply {
        store: Arc::clone(&store),
    });
    let calibration_repo: Arc<dyn CalibrationArtifactRepository> =
        Arc::new(PgCalibrationArtifactRepository::new(db.clone()));
    let calibration_loader: Arc<dyn CalibrationArtifactLoader> = Arc::new(
        CoreCalibrationArtifactLoader::new(Arc::clone(&calibration_repo)),
    );
    let service = ModelGovernanceService::new(ModelGovernanceDeps {
        model_registry_repo: Arc::new(PgModelRegistryRepository::new(db.clone())),
        backtest_report_repo: Arc::new(PgBacktestReportRepository::new(db.clone())),
        backtest_path_set_repo: Arc::new(PgBacktestPathSetRepository::new(db.clone())),
        shadow_comparison_repo: Arc::new(PgShadowComparisonRepository::new(db.clone())),
        governance_audit_repo: Arc::new(PgModelGovernanceAuditRepository::new(db.clone())),
        dataset_repo: Arc::new(PgTrainingDatasetRepository::new(db.clone())),
        artifact_store: Arc::clone(&artifact_store),
        calibration_repo,
        calibration_loader,
        gate,
        runtime_config: Arc::clone(&store),
        runtime_config_apply: apply,
        runtime_config_repo,
    });
    GovernanceHarness {
        service,
        store,
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
            feature_requirements: serde_json::json!({}),
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

async fn seed_dataset(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    spec: &ModelSpecId,
    rc_id: &RuntimeConfigVersionId,
    status: TrainingDatasetStatus,
    coverage_json: DatasetCoverage,
    dataset_hash_seed: char,
) -> TrainingDatasetId {
    let id = TrainingDatasetId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);
    // Real Parquet so publish-time leakage rescan (#9) can decode bytes.
    let bytes = DatasetParquetCodec::encode(&[]).expect("encode empty parquet");
    let hex = id.as_uuid().simple().to_string();
    let key = ArtifactKey::new(ArtifactNamespace::Dataset, hex, "parquet").expect("key");
    let parquet_uri = artifact_store
        .put(key, &bytes)
        .await
        .expect("store parquet");
    PgTrainingDatasetRepository::new(db.clone())
        .create(NewTrainingDataset {
            training_dataset_id: id.clone(),
            model_spec_id: spec.clone(),
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            status,
            purpose: DatasetPurpose::Training,
            feature_schema_hash: content_hash(u32::from('f')),
            factor_schema_hash: content_hash(u32::from('g')),
            label_schema_hash: content_hash(u32::from('l')),
            dataset_hash: content_hash(u32::from(dataset_hash_seed)),
            parquet_uri,
            sample_count: 990,
            source_delay_secs: 10,
            sample_interval_secs: 3_600,
            horizons_secs: TrainingHorizonsSecs(vec![3600]),
            coverage_json,
            runtime_config_version_id: rc_id.clone(),
        })
        .await
        .expect("dataset");
    id
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
    let artifact_hash = store_weighted_artifact(artifact_store, &id, seed, return_model).await;
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
) -> ContentHash {
    let artifact = ModelArtifact::WeightedFactor(Box::new(WeightedFactorModelArtifact {
        header: ModelArtifactHeader {
            model_version_id: model_version_id.clone(),
            model_family: ModelFamily::WeightedFactor,
            feature_schema_hash: content_hash(u32::from('f')),
            factor_schema_hash: content_hash(u32::from('g')),
        },
        weights: vec![FactorWeight {
            factor: LIQUIDITY_DEPTH,
            weight: dec!(1),
        }],
        prediction_horizon_secs: 86_400,
        multipliers: ScoreMultiplierSpec::conservative(),
        substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
        return_model,
        required_features: Vec::new(),
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
            feature_requirements: serde_json::json!({}),
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
        required_features: Vec::new(),
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
            as_of: now - ChronoDuration::hours(hours_ago),
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
    assert!(
        audits.iter().any(|audit| audit.reason == rollback_reason),
        "the rollback reason is audited"
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
async fn dataset_insufficient_labels_cannot_promote_ready() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;
    // Terminal InsufficientLabels dataset (even with healthy coverage on paper).
    let dataset = seed_dataset(
        &db,
        &harness.artifact_store,
        &spec,
        &rc_id,
        TrainingDatasetStatus::InsufficientLabels,
        healthy_coverage(),
        '1',
    )
    .await;

    let result = harness
        .service
        .promote_dataset_ready(
            PromoteDatasetRequest {
                training_dataset_id: dataset,
                reason: "promote".to_owned(),
            },
            GovernanceActor::system(),
        )
        .await;
    assert!(
        result.is_err(),
        "an InsufficientLabels dataset can never be promoted to Ready"
    );
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
        as_of,
        sample_source: TrainingSampleSource::HistoricalPit,
        feature_vector: FeatureVector {
            market_id: MarketId::new("0xleak"),
            token_id: Some(TokenId::new("yes")),
            as_of,
            generic_schema_version: SchemaVersion::FIRST,
            generic: BTreeMap::new(),
            domain: None,
            substitutions: Vec::new(),
            data_quality: DataQualityStatus::Fresh,
            staleness_ms: 0,
            source_refs: Vec::new(),
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
            observed_at: as_of + ChronoDuration::seconds(60),
        }],
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
    let bytes = DatasetParquetCodec::encode(&[leaking_training_example()]).expect("encode");
    let dataset_id = TrainingDatasetId::from_v7();
    let hex = dataset_id.as_uuid().simple().to_string();
    let key = ArtifactKey::new(ArtifactNamespace::Dataset, hex, "parquet").expect("key");
    let parquet_uri = artifact_store.put(key, &bytes).await.expect("store");
    let window_start = Utc::now() - ChronoDuration::hours(2);
    PgTrainingDatasetRepository::new(db.clone())
        .create(NewTrainingDataset {
            training_dataset_id: dataset_id.clone(),
            model_spec_id: spec.clone(),
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            status: TrainingDatasetStatus::Ready,
            purpose: DatasetPurpose::Training,
            feature_schema_hash: content_hash(u32::from('f')),
            factor_schema_hash: content_hash(u32::from('g')),
            label_schema_hash: content_hash(u32::from('l')),
            dataset_hash: content_hash(u32::from('z')),
            parquet_uri,
            sample_count: 1,
            source_delay_secs: 0,
            sample_interval_secs: 3_600,
            horizons_secs: TrainingHorizonsSecs(vec![0]),
            coverage_json: healthy_coverage(),
            runtime_config_version_id: rc_id.clone(),
        })
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
