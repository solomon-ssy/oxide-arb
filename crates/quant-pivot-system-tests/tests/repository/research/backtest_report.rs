//! Backtest-report ledger persistence system contract.

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::quant::{NewBacktestReport, NewModelRun, NewModelVersion},
    enums::{
        model::ModelFamily,
        quant::{ModelRunKind, ModelRunStatus, PublicationStatus},
    },
    types::{
        BacktestReportId, ContentHash, DecisionPolicySnapshotId, ModelInputContract, ModelRunId,
        ModelSpecId, ModelTrainingContract, ModelVersionId, Probability,
        backtest::{CategoryMetrics, ExpectedVsRealized, PnlSimulation},
        model_metrics::ModelVersionMetrics,
        model_training::ModelTrainingObjective,
    },
};
use quant_pivot_repository::{
    postgres::{PgBacktestReportRepository, PgModelRegistryRepository, PgModelRunRepository},
    traits::{BacktestReportRepository, ModelRegistryRepository, ModelRunRepository},
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        execution_pg_seed, model_spec_fixtures, policy_fixtures::bootstrap_default_policy_bundle,
    },
};
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

async fn seed_runtime_config(db: &DatabaseConnection) -> DecisionPolicySnapshotId {
    bootstrap_default_policy_bundle(db, "pg-backtest-it", "integration test").await
}

async fn seed_model_version(
    db: &DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
) -> (ModelVersionId, ModelRunId) {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            model_spec_id,
            "pg-backtest-it",
            ModelFamily::WeightedFactor,
            86_400,
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        ))
        .await
        .expect("model spec");

    let model_version_id = ModelVersionId::from_v7();
    registry
        .create_model_version(NewModelVersion {
            model_version_id,
            model_spec_id,
            version: 1,
            profile_ref: execution_pg_seed::fixture_profile_ref(),
            artifact_hash: content_hash('a'),
            category_scope: None,
            training_dataset_id: None,
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

    let model_run_id = ModelRunId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::Backtest,
            model_version_id: Some(model_version_id),
            decision_policy_snapshot_id: *rc_id,
            market_selection_id: None,
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            status: ModelRunStatus::Running,
            input_hash: content_hash('d'),
            output_hash: None,
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
    rc_id: DecisionPolicySnapshotId,
) -> NewBacktestReport {
    let window_start = Utc::now() - ChronoDuration::hours(2);
    NewBacktestReport {
        backtest_report_id,
        model_version_id,
        model_run_id,
        decision_policy_snapshot_id: rc_id,
        window_start,
        window_end: window_start + ChronoDuration::hours(1),
        coverage: dec!(1),
        sample_count: 10,
        missing_feature_count: 0,
        rank_ic: dec!(0.42),
        sharpe: dec!(0.9),
        hit_rate: Probability::new(dec!(0.6)),
        expected_vs_realized: ExpectedVsRealized {
            mean_expected_bps: dec!(50),
            mean_realized_bps: dec!(45),
            correlation: dec!(0.8),
            bias_bps: dec!(5),
        },
        max_drawdown: dec!(0.1),
        turnover: dec!(0.2),
        liquidity_feasibility: Probability::new(dec!(1)),
        category_breakdown: CategoryMetrics::default(),
        tail_loss: dec!(-50),
        report_pnl_simulation: PnlSimulation {
            total_allocated_usd: dec!(100),
            realized_pnl_usd: dec!(12.5),
            gross_return: dec!(0.125),
            pnl_curve: Vec::new(),
        },
        report_hash: content_hash('e'),
        parquet_uri: None,
    }
}

pub async fn quant_backtest_report_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let (model_version_id, model_run_id) = seed_model_version(&db, &rc_id).await;
    let repo = PgBacktestReportRepository::new(db.clone());
    let report_id = BacktestReportId::from_v7();

    let created = repo
        .create(new_report(report_id, model_version_id, model_run_id, rc_id))
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
