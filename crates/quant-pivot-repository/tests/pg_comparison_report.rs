//! Pairwise model-comparison report ledger integration tests (Postgres + testcontainers).

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::{
        NewBacktestReport, NewModelComparisonReport, NewModelRun, NewModelSpec, NewModelVersion,
        NewRuntimeConfigVersion,
    },
    enums::{
        model::ModelFamily,
        quant::{ModelRunKind, ModelRunStatus, PublicationStatus},
        runtime_config::RuntimeConfigVersionSource,
    },
    types::{
        BacktestReportId, ContentHash, ModelComparisonReportId, ModelRunId, ModelSpecId,
        ModelVersionId, Probability, RuntimeConfigVersionId, SchemaVersion,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestReportRepository, PgModelComparisonReportRepository, PgModelRegistryRepository,
        PgModelRunRepository, PgRuntimeConfigVersionRepository,
    },
    traits::{
        BacktestReportRepository, ModelComparisonReportRepository, ModelRegistryRepository,
        ModelRunRepository, RuntimeConfigVersionRepository,
    },
};
use quant_pivot_test_support::pg::setup_pg;
use rust_decimal_macros::dec;

fn content_hash(seed: &str) -> ContentHash {
    ContentHash::parse(format!("blake3:{seed:0>64}")).expect("hash")
}

async fn seed_runtime_config(db: &sea_orm::DatabaseConnection) -> RuntimeConfigVersionId {
    let id = RuntimeConfigVersionId::from_v7();
    PgRuntimeConfigVersionRepository::new(db.clone())
        .create_version(NewRuntimeConfigVersion {
            runtime_config_version_id: id.clone(),
            config_hash: content_hash("c"),
            schema_version: SchemaVersion::FIRST,
            config_json: serde_json::json!({}),
            source: RuntimeConfigVersionSource::Bootstrap,
            created_by: "pg-comparison-it".to_owned(),
            reason: "integration test".to_owned(),
        })
        .await
        .expect("runtime config");
    id
}

async fn seed_model_version(
    db: &sea_orm::DatabaseConnection,
    model_spec_id: &ModelSpecId,
    version: i32,
    artifact_seed: &str,
) -> ModelVersionId {
    let model_version_id = ModelVersionId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_version(NewModelVersion {
            model_version_id: model_version_id.clone(),
            model_spec_id: model_spec_id.clone(),
            version,
            artifact_hash: content_hash(artifact_seed),
            training_dataset_id: None,
            metrics_json: serde_json::json!({}),
            quality_gate_report: serde_json::json!({}),
            publication_status: PublicationStatus::Candidate,
            published_at: None,
            retired_at: None,
        })
        .await
        .expect("model version");
    model_version_id
}

async fn seed_backtest_report(
    db: &sea_orm::DatabaseConnection,
    model_version_id: &ModelVersionId,
    model_run_id: &ModelRunId,
    rc_id: &RuntimeConfigVersionId,
    report_seed: &str,
) -> BacktestReportId {
    let id = BacktestReportId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);
    PgBacktestReportRepository::new(db.clone())
        .create(NewBacktestReport {
            backtest_report_id: id.clone(),
            model_version_id: model_version_id.clone(),
            model_run_id: model_run_id.clone(),
            runtime_config_version_id: rc_id.clone(),
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            coverage: dec!(1),
            sample_count: 10,
            missing_feature_count: 0,
            rank_ic: dec!(0.3),
            hit_rate: Probability::new(dec!(0.6)),
            expected_vs_realized: serde_json::json!({}),
            max_drawdown: dec!(0.1),
            turnover: dec!(0.2),
            liquidity_feasibility: Probability::new(dec!(1)),
            category_breakdown: serde_json::json!([]),
            tail_loss: dec!(-50),
            report_pnl_simulation: serde_json::json!({}),
            report_hash: content_hash(report_seed),
            parquet_uri: None,
        })
        .await
        .expect("backtest report");
    id
}

async fn seed_run(
    db: &sea_orm::DatabaseConnection,
    model_version_id: &ModelVersionId,
    rc_id: &RuntimeConfigVersionId,
) -> ModelRunId {
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
            input_hash: content_hash("d"),
            output_hash: None,
            metrics_json: serde_json::json!({}),
            error_code: None,
            error_message: None,
            started_at: Utc::now(),
            finished_at: None,
        })
        .await
        .expect("model run");
    model_run_id
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn quant_model_comparison_report_migration_and_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;

    let model_spec_id = ModelSpecId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_spec(NewModelSpec {
            model_spec_id: model_spec_id.clone(),
            name: "pg-comparison-it".to_owned(),
            model_family: ModelFamily::WeightedFactor,
            prediction_horizon_secs: 86_400,
            feature_schema_version: SchemaVersion::FIRST,
            label_schema_version: SchemaVersion::FIRST,
            spec_json: serde_json::json!({}),
            status: PublicationStatus::Published,
        })
        .await
        .expect("model spec");

    let baseline_version = seed_model_version(&db, &model_spec_id, 1, "a").await;
    let candidate_version = seed_model_version(&db, &model_spec_id, 2, "b").await;
    let model_run_id = seed_run(&db, &candidate_version, &rc_id).await;
    let baseline_report =
        seed_backtest_report(&db, &baseline_version, &model_run_id, &rc_id, "e1").await;
    let candidate_report =
        seed_backtest_report(&db, &candidate_version, &model_run_id, &rc_id, "e2").await;

    let repo = PgModelComparisonReportRepository::new(db.clone());
    let comparison_report_id = ModelComparisonReportId::from_v7();
    let created = repo
        .create(NewModelComparisonReport {
            comparison_report_id: comparison_report_id.clone(),
            baseline_model_version_id: baseline_version.clone(),
            candidate_model_version_id: candidate_version.clone(),
            baseline_report_id: baseline_report.clone(),
            candidate_report_id: candidate_report.clone(),
            model_run_id,
            rank_ic_delta: dec!(0.15),
            hit_rate_delta: dec!(0.05),
            realized_pnl_delta: dec!(80),
            score_correlation: dec!(0.95),
            side_disagreement_rate: dec!(0.5),
            common_samples: 2,
            category_breakdown_diff: serde_json::json!([]),
            comparison_hash: content_hash("f"),
        })
        .await
        .expect("create comparison");
    assert_eq!(created.comparison_report_id, comparison_report_id);
    assert_eq!(created.rank_ic_delta, dec!(0.15));
    assert_eq!(created.common_samples, 2);

    let found = repo
        .find_by_id(&comparison_report_id)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(found.comparison_hash, created.comparison_hash);
    assert_eq!(found.realized_pnl_delta, dec!(80));

    let listed = repo
        .list_by_candidate_version(&candidate_version)
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].comparison_report_id, comparison_report_id);

    let by_candidate = repo
        .find_by_backtest_report_id(&candidate_report)
        .await
        .expect("find by candidate report")
        .expect("row");
    assert_eq!(by_candidate.comparison_report_id, comparison_report_id);

    let by_baseline = repo
        .find_by_backtest_report_id(&baseline_report)
        .await
        .expect("find by baseline report")
        .expect("row");
    assert_eq!(by_baseline.comparison_report_id, comparison_report_id);

    let id_map = repo
        .comparison_ids_for_backtest_reports(&[candidate_report.clone(), baseline_report.clone()])
        .await
        .expect("batch lookup");
    assert_eq!(id_map.get(&candidate_report), Some(&comparison_report_id));
    assert_eq!(id_map.get(&baseline_report), Some(&comparison_report_id));
}
