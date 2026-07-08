//! Backtest-report ledger integration tests (Postgres + testcontainers).

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::{
        NewBacktestReport, NewModelRun, NewModelSpec, NewModelVersion, NewRuntimeConfigVersion,
    },
    enums::{
        model::ModelFamily,
        quant::{ModelRunKind, ModelRunStatus, PublicationStatus},
        runtime_config::RuntimeConfigVersionSource,
    },
    types::{
        BacktestReportId, ContentHash, ModelRunId, ModelSpecId, ModelVersionId, Probability,
        RuntimeConfigVersionId, SchemaVersion,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestReportRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgRuntimeConfigVersionRepository,
    },
    traits::{
        BacktestReportRepository, ModelRegistryRepository, ModelRunRepository,
        RuntimeConfigVersionRepository,
    },
};
use quant_pivot_test_support::pg::setup_pg;
use rust_decimal_macros::dec;

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

async fn seed_runtime_config(db: &sea_orm::DatabaseConnection) -> RuntimeConfigVersionId {
    let id = RuntimeConfigVersionId::from_v7();
    PgRuntimeConfigVersionRepository::new(db.clone())
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: id.clone(),
            config_hash: content_hash('c'),
            schema_version: SchemaVersion::FIRST,
            config_json: serde_json::json!({}),
            source: RuntimeConfigVersionSource::Bootstrap,
            created_by: "pg-backtest-it".to_owned(),
            reason: "integration test".to_owned(),
        })
        .await
        .expect("runtime config");
    id
}

async fn seed_model_version(
    db: &sea_orm::DatabaseConnection,
    rc_id: &RuntimeConfigVersionId,
) -> (ModelVersionId, ModelRunId) {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(NewModelSpec {
            model_spec_id: model_spec_id.clone(),
            name: "pg-backtest-it".to_owned(),
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

    let model_version_id = ModelVersionId::from_v7();
    registry
        .create_model_version(NewModelVersion {
            model_version_id: model_version_id.clone(),
            model_spec_id,
            version: 1,
            artifact_hash: content_hash('a'),
            training_dataset_id: None,
            metrics_json: serde_json::json!({}),
            quality_gate_report: serde_json::json!({}),
            publication_status: PublicationStatus::Candidate,
            published_at: None,
            retired_at: None,
        })
        .await
        .expect("model version");

    let model_run_id = ModelRunId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::Backtest,
            model_version_id: Some(model_version_id.clone()),
            runtime_config_version_id: rc_id.clone(),
            market_selection_id: None,
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            status: ModelRunStatus::Running,
            input_hash: content_hash('d'),
            output_hash: None,
            metrics_json: serde_json::json!({}),
            error_code: None,
            error_message: None,
            started_at: Utc::now(),
            finished_at: None,
        })
        .await
        .expect("model run");

    (model_version_id, model_run_id)
}

fn new_report(
    backtest_report_id: BacktestReportId,
    model_version_id: ModelVersionId,
    model_run_id: ModelRunId,
    rc_id: RuntimeConfigVersionId,
) -> NewBacktestReport {
    let window_start = Utc::now() - ChronoDuration::hours(2);
    NewBacktestReport {
        backtest_report_id,
        model_version_id,
        model_run_id,
        runtime_config_version_id: rc_id,
        window_start,
        window_end: window_start + ChronoDuration::hours(1),
        coverage: dec!(1),
        sample_count: 10,
        missing_feature_count: 0,
        rank_ic: dec!(0.42),
        hit_rate: Probability::new(dec!(0.6)),
        expected_vs_realized: serde_json::json!({ "bias_bps": "5" }),
        max_drawdown: dec!(0.1),
        turnover: dec!(0.2),
        liquidity_feasibility: Probability::new(dec!(1)),
        category_breakdown: serde_json::json!([]),
        tail_loss: dec!(-50),
        report_pnl_simulation: serde_json::json!({ "realized_pnl_usd": "12.5" }),
        report_hash: content_hash('e'),
        parquet_uri: None,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn quant_backtest_report_migration_and_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (model_version_id, model_run_id) = seed_model_version(&db, &rc_id).await;
    let repo = PgBacktestReportRepository::new(db.clone());
    let report_id = BacktestReportId::from_v7();

    let created = repo
        .create(new_report(
            report_id.clone(),
            model_version_id.clone(),
            model_run_id,
            rc_id,
        ))
        .await
        .expect("create");
    assert_eq!(created.backtest_report_id, report_id);
    assert_eq!(created.rank_ic, dec!(0.42));
    assert_eq!(created.sample_count, 10);

    let found = repo
        .find_by_id(&report_id)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(found.report_hash, created.report_hash);
    assert_eq!(found.hit_rate, Probability::new(dec!(0.6)));

    let listed = repo
        .list_by_model_version(&model_version_id)
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].backtest_report_id, report_id);
}
