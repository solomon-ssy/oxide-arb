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
use quant_pivot_core::governance::{ModelGovernanceDeps, ModelGovernanceService};
use quant_pivot_core::runtime_config::RuntimeConfigStore;
use quant_pivot_error::control::ControlError;
use quant_pivot_models::{
    domain::{
        GovernanceActor, ModelGovernancePort, NewBacktestReport, NewModelRun, NewModelSpec,
        NewModelVersion, NewRuntimeConfigActivation, NewRuntimeConfigVersion, NewShadowComparison,
        NewTrainingDataset, PromoteDatasetRequest, PublishModelCommand, RetireModelCommand,
        RollbackModelCommand, RuntimeConfigPort,
    },
    enums::{
        model::ModelFamily,
        quant::{ModelRunKind, ModelRunStatus, PublicationStatus, TrainingDatasetStatus},
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    hashing::CanonicalDigest,
    runtime_config::{RUNTIME_CONFIG_SCHEMA_VERSION, RuntimeConfig},
    types::{
        ArtifactUri, BacktestReportId, ContentHash, ModelRunId, ModelSpecId, ModelVersionId,
        Probability, RuntimeConfigActivationId, RuntimeConfigVersionId, SchemaVersion,
        ShadowComparisonId, TrainingDatasetId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestReportRepository, PgModelGovernanceAuditRepository, PgModelRegistryRepository,
        PgModelRunRepository, PgRuntimeConfigVersionRepository, PgShadowComparisonRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        BacktestReportRepository, ModelGovernanceAuditRepository, ModelRegistryRepository,
        ModelRunRepository, RuntimeConfigVersionRepository, ShadowComparisonRepository,
        TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    backtest::{ExpectedVsRealized, PnlSimulation},
    gates::{DefaultModelQualityGate, ModelQualityGate},
    training::DatasetCoverage,
};
use quant_pivot_test_support::pg::setup_pg;
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

/// Test harness wiring governance against a real store + config repo.
struct GovernanceHarness {
    service: ModelGovernanceService,
    store: Arc<RuntimeConfigStore>,
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

    let gate: Arc<dyn ModelQualityGate> = Arc::new(DefaultModelQualityGate::new());
    let apply: Arc<dyn RuntimeConfigPort> = Arc::new(TestRuntimeConfigApply {
        store: Arc::clone(&store),
    });
    let service = ModelGovernanceService::new(ModelGovernanceDeps {
        model_registry_repo: Arc::new(PgModelRegistryRepository::new(db.clone())),
        backtest_report_repo: Arc::new(PgBacktestReportRepository::new(db.clone())),
        shadow_comparison_repo: Arc::new(PgShadowComparisonRepository::new(db.clone())),
        governance_audit_repo: Arc::new(PgModelGovernanceAuditRepository::new(db.clone())),
        dataset_repo: Arc::new(PgTrainingDatasetRepository::new(db.clone())),
        gate,
        runtime_config: Arc::clone(&store),
        runtime_config_apply: apply,
        runtime_config_repo,
    });
    GovernanceHarness { service, store }
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
            status: PublicationStatus::Published,
        })
        .await
        .expect("model spec");
    id
}

/// A dataset coverage that clears the gate (label 0.90, build 0.99).
fn healthy_coverage() -> serde_json::Value {
    serde_json::to_value(DatasetCoverage {
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
    })
    .expect("coverage json")
}

async fn seed_dataset(
    db: &DatabaseConnection,
    spec: &ModelSpecId,
    rc_id: &RuntimeConfigVersionId,
    status: TrainingDatasetStatus,
    coverage_json: serde_json::Value,
    dataset_hash_seed: char,
) -> TrainingDatasetId {
    let id = TrainingDatasetId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);
    PgTrainingDatasetRepository::new(db.clone())
        .create(NewTrainingDataset {
            training_dataset_id: id.clone(),
            model_spec_id: spec.clone(),
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            status,
            feature_schema_hash: content_hash(u32::from('f')),
            factor_schema_hash: content_hash(u32::from('g')),
            label_schema_hash: content_hash(u32::from('l')),
            dataset_hash: content_hash(u32::from(dataset_hash_seed)),
            parquet_uri: ArtifactUri::parse("file:///tmp/governance-it.parquet").expect("uri"),
            sample_count: 990,
            source_delay_secs: 10,
            sample_interval_secs: 3_600,
            horizons_secs: serde_json::json!([3600]),
            coverage_json,
            runtime_config_version_id: rc_id.clone(),
        })
        .await
        .expect("dataset");
    id
}

async fn seed_version(
    db: &DatabaseConnection,
    spec: &ModelSpecId,
    seed: char,
    version: i32,
    dataset: Option<TrainingDatasetId>,
) -> ModelVersionId {
    let id = ModelVersionId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_version(NewModelVersion {
            model_version_id: id.clone(),
            model_spec_id: spec.clone(),
            version,
            artifact_hash: content_hash(u32::from(seed)),
            training_dataset_id: dataset,
            metrics_json: serde_json::json!({}),
            quality_gate_report: serde_json::json!({}),
            publication_status: PublicationStatus::Candidate,
            published_at: None,
            retired_at: None,
        })
        .await
        .expect("model version");
    id
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
        equity_curve: Vec::new(),
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn publish_requires_quality_gate_pass() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    // No dataset, no backtest → coverage + sample gates fail.
    let candidate = seed_version(&db, &spec, 'a', 1, None).await;
    let _ = rc_id;

    let result = harness(&db)
        .await
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
    assert!(row.quality_gate_report.get("passed").is_some());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn publish_requires_backtest_report() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let dataset = seed_dataset(
        &db,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        '1',
    )
    .await;
    let candidate = seed_version(&db, &spec, 'a', 1, Some(dataset)).await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;
    // Deliberately no backtest report.

    let result = harness(&db)
        .await
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn publish_requires_shadow_stability() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let dataset = seed_dataset(
        &db,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        '1',
    )
    .await;
    let candidate = seed_version(&db, &spec, 'a', 1, Some(dataset)).await;
    seed_backtest(&db, &candidate, &rc_id, '1').await;
    // No shadow comparisons → shadow stability not established.

    let result = harness(&db)
        .await
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn publish_succeeds_then_published_version_is_immutable() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let dataset = seed_dataset(
        &db,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        '1',
    )
    .await;
    let candidate = seed_version(&db, &spec, 'a', 1, Some(dataset)).await;
    seed_backtest(&db, &candidate, &rc_id, '1').await;
    seed_shadow_window(&db, &candidate, &candidate, 'p').await;

    let harness = harness(&db).await;
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

/// Publish a fully gated candidate (backtest + shadow window + publish).
async fn publish_ready_version(params: PublishReadyParams<'_>) {
    seed_backtest(
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn rollback_retains_previous_and_writes_reason() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;

    let dataset_v1 = seed_dataset(
        &db,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        '1',
    )
    .await;
    let v1 = seed_version(&db, &spec, 'a', 1, Some(dataset_v1)).await;
    publish_ready_version(PublishReadyParams {
        governance: &harness.service,
        db: &db,
        rc_id: &rc_id,
        version: &v1,
        active_for_shadow: &v1,
        backtest_seed: '1',
        shadow_seed: 'a',
        reason: "publish v1",
    })
    .await;

    let dataset_v2 = seed_dataset(
        &db,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        '2',
    )
    .await;
    let v2 = seed_version(&db, &spec, 'b', 2, Some(dataset_v2)).await;
    publish_ready_version(PublishReadyParams {
        governance: &harness.service,
        db: &db,
        rc_id: &rc_id,
        version: &v2,
        active_for_shadow: &v2,
        backtest_seed: '2',
        shadow_seed: 'm',
        reason: "publish v2",
    })
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

    let registry = PgModelRegistryRepository::new(db.clone());
    let v2_row = registry
        .find_model_version_by_id(&v2)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(v2_row.publication_status, PublicationStatus::Retired);
    let v1_row = registry
        .find_model_version_by_id(&v1)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(
        v1_row.publication_status,
        PublicationStatus::Published,
        "rollback must re-publish the retired predecessor"
    );
    assert_eq!(
        harness
            .store
            .current()
            .model
            .active_model_version_id
            .as_ref()
            .map(|reference| reference.id.as_str()),
        Some(v1.to_string().as_str()),
        "rollback must wire runtime config back to the restored version"
    );

    let audits = PgModelGovernanceAuditRepository::new(db.clone())
        .list_by_version(&v2)
        .await
        .expect("audits");
    assert!(
        audits.iter().any(|audit| audit.reason == "v2 regressed"),
        "the rollback reason is audited"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn retire_published_version_clears_runtime_pointer_and_audits() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    let harness = harness(&db).await;

    let dataset = seed_dataset(
        &db,
        &spec,
        &rc_id,
        TrainingDatasetStatus::Ready,
        healthy_coverage(),
        '1',
    )
    .await;
    let version = seed_version(&db, &spec, 'a', 1, Some(dataset)).await;
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

#[tokio::test]
#[ignore = "requires Docker"]
async fn dataset_insufficient_labels_cannot_promote_ready() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let spec = seed_spec(&db).await;
    // Terminal InsufficientLabels dataset (even with healthy coverage on paper).
    let dataset = seed_dataset(
        &db,
        &spec,
        &rc_id,
        TrainingDatasetStatus::InsufficientLabels,
        healthy_coverage(),
        '1',
    )
    .await;

    let result = harness(&db)
        .await
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
