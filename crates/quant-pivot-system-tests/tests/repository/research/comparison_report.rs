//! Pairwise model-comparison report ledger persistence system contract.

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_models::{
    domain::quant::{NewBacktestReport, NewModelComparisonReport, NewModelRun, NewModelVersion},
    enums::{
        model::ModelFamily,
        quant::{ModelRunKind, ModelRunStatus, PublicationStatus},
    },
    types::{
        BacktestReportId, ContentHash, DecisionPolicySnapshotId, ModelComparisonReportId,
        ModelInputContract, ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId,
        Probability,
        backtest::{CategoryMetrics, CategoryRankIcDeltas, ExpectedVsRealized, PnlSimulation},
        model_metrics::ModelVersionMetrics,
        model_training::ModelTrainingObjective,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestReportRepository, PgModelComparisonReportRepository, PgModelRegistryRepository,
        PgModelRunRepository,
    },
    traits::{
        BacktestReportRepository, ModelComparisonReportRepository, ModelRegistryRepository,
        ModelRunRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        execution_pg_seed, model_spec_fixtures, policy_fixtures::bootstrap_default_policy_bundle,
    },
};
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

fn content_hash(seed: &str) -> ContentHash {
    ContentHash::parse(&format!("blake3:{seed:0>64}")).expect("hash")
}

async fn seed_runtime_config(db: &DatabaseConnection) -> DecisionPolicySnapshotId {
    bootstrap_default_policy_bundle(db, "pg-comparison-it", "integration test").await
}

async fn seed_model_version(
    db: &DatabaseConnection,
    model_spec_id: &ModelSpecId,
    version: i32,
    artifact_seed: &str,
) -> ModelVersionId {
    let model_version_id = ModelVersionId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_version(NewModelVersion {
            model_version_id,
            model_spec_id: *model_spec_id,
            version,
            artifact_hash: content_hash(artifact_seed),
            category_scope: None,
            profile_ref: execution_pg_seed::fixture_profile_ref(),
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
    model_version_id
}

async fn seed_backtest_report(
    db: &DatabaseConnection,
    model_version_id: &ModelVersionId,
    model_run_id: &ModelRunId,
    rc_id: &DecisionPolicySnapshotId,
    report_seed: &str,
) -> BacktestReportId {
    let id = BacktestReportId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);
    PgBacktestReportRepository::new(db.clone())
        .create(NewBacktestReport {
            backtest_report_id: id,
            model_version_id: *model_version_id,
            model_run_id: *model_run_id,
            decision_policy_snapshot_id: *rc_id,
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            coverage: dec!(1),
            sample_count: 10,
            missing_feature_count: 0,
            rank_ic: dec!(0.3),
            sharpe: dec!(0.8),
            hit_rate: Probability::new(dec!(0.6)),
            expected_vs_realized: ExpectedVsRealized {
                mean_expected_bps: dec!(30),
                mean_realized_bps: dec!(28),
                correlation: dec!(0.9),
                bias_bps: dec!(2),
            },
            max_drawdown: dec!(0.1),
            turnover: dec!(0.2),
            liquidity_feasibility: Probability::new(dec!(1)),
            category_breakdown: CategoryMetrics::default(),
            tail_loss: dec!(-50),
            report_pnl_simulation: PnlSimulation {
                total_allocated_usd: dec!(100),
                realized_pnl_usd: dec!(8),
                gross_return: dec!(0.08),
                pnl_curve: Vec::new(),
            },
            report_hash: content_hash(report_seed),
            parquet_uri: None,
        })
        .await
        .expect("backtest report");
    id
}

async fn seed_run(
    db: &DatabaseConnection,
    model_version_id: &ModelVersionId,
    rc_id: &DecisionPolicySnapshotId,
) -> ModelRunId {
    let model_run_id = ModelRunId::from_v7();
    let window_start = Utc::now() - ChronoDuration::hours(2);
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::Backtest,
            model_version_id: Some(*model_version_id),
            decision_policy_snapshot_id: *rc_id,
            market_selection_id: None,
            window_start,
            window_end: window_start + ChronoDuration::hours(1),
            status: ModelRunStatus::Running,
            input_hash: content_hash("d"),
            output_hash: None,
            error_code: None,
            error_message: None,
            started_at: Utc::now(),
            finished_at: None,
        })
        .await
        .expect("model run");
    model_run_id
}

pub async fn quant_model_comparison_report_migration_and_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;

    let model_spec_id = ModelSpecId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            model_spec_id,
            "pg-comparison-it",
            ModelFamily::WeightedFactor,
            86_400,
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        ))
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
            comparison_report_id,
            baseline_model_version_id: baseline_version,
            candidate_model_version_id: candidate_version,
            baseline_report_id: baseline_report,
            candidate_report_id: candidate_report,
            model_run_id,
            rank_ic_delta: dec!(0.15),
            hit_rate_delta: dec!(0.05),
            realized_pnl_delta: dec!(80),
            score_correlation: dec!(0.95),
            side_disagreement_rate: dec!(0.5),
            common_samples: 2,
            category_breakdown_diff: CategoryRankIcDeltas::default(),
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
        .comparison_ids_for_backtest_reports(&[candidate_report, baseline_report])
        .await
        .expect("batch lookup");
    assert_eq!(id_map.get(&candidate_report), Some(&comparison_report_id));
    assert_eq!(id_map.get(&baseline_report), Some(&comparison_report_id));
}
