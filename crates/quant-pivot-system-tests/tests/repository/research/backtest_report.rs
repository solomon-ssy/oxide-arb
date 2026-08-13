//! Backtest-report ledger persistence system contracts.

use std::fmt::Debug;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_BACKTEST_REPORT, QUANT_TRAINING_DATASET},
};
use quant_pivot_models::{
    domain::quant::{NewBacktestReport, NewModelRun},
    enums::{
        model::ModelFamily,
        quant::{DatasetPurpose, ModelRunKind},
    },
    types::{
        BacktestReportId, ContentHash, DecisionPolicySnapshotId, ModelInputContract, ModelRunId,
        ModelSpecId, ModelTrainingContract, ModelVersionId, Probability, TrainingDatasetId,
        backtest::{CategoryMetrics, ExpectedVsRealized, PnlSimulation},
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestReportRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgTrainingDatasetRepository,
    },
    traits::{
        BacktestReportRepository, ModelRegistryRepository, ModelRunRepository,
        TrainingDatasetRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        model_serving_fixtures::{
            ModelDatasetLedgerFixture, ModelDatasetLedgerSeed, ModelVersionFixture,
            ModelVersionFixtureSeed,
        },
        model_spec_fixtures,
        policy_fixtures::bootstrap_default_policy_bundle,
        research_fixtures::fully_resolved_backtest_funnel,
    },
};
use rust_decimal_macros::dec;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};

struct BacktestLineage {
    model_version_id: ModelVersionId,
    model_run_id: ModelRunId,
    evaluation_dataset_id: TrainingDatasetId,
    evaluation_dataset_hash: ContentHash,
    training_dataset_id: TrainingDatasetId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
}

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("fixture hash")
}

async fn seed_runtime_config(db: &DatabaseConnection) {
    bootstrap_default_policy_bundle(db, "pg-backtest-it", "integration test").await;
}

async fn seed_model_version(
    db: &DatabaseConnection,
    scope: &str,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> BacktestLineage {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            model_spec_id,
            scope,
            ModelFamily::WeightedFactor,
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::outcome_default(),
        ))
        .await
        .expect("model spec");

    let model_version_id = ModelVersionId::from_v7();
    let model_version = registry
        .create_model_version(
            ModelVersionFixture::prepare(
                db,
                ModelVersionFixtureSeed::training(
                    format!("{scope}:{model_version_id}"),
                    model_version_id,
                    model_spec_id,
                    content_hash('a'),
                ),
            )
            .await
            .expect("prepare exact model version"),
        )
        .await
        .expect("model version");
    let training_dataset_id = model_version
        .training_dataset_id
        .expect("fixture model has Training Dataset");
    let training_dataset = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&training_dataset_id)
        .await
        .expect("load Training Dataset")
        .expect("Training Dataset exists");
    let training = training_dataset
        .materialization()
        .expect("Training Dataset materialization");
    let bindings = model_version
        .verified_serving_contract()
        .expect("verified serving contract")
        .bindings();
    let decision_policy_snapshot_id = bindings.policy_snapshot.decision_policy_snapshot_id;
    let prediction_horizon_secs = bindings.model.prediction_horizon_secs;
    let evaluation_dataset = ModelDatasetLedgerFixture::persist(
        db,
        &ModelDatasetLedgerFixture::local_store(),
        ModelDatasetLedgerSeed {
            scope: format!("{scope}:evaluation"),
            model_spec_id,
            model_family: model_version.model_family,
            model_spec_definition_hash: model_version.model_spec_definition_hash,
            factor_serving_plane: training.factor_serving_plane.clone(),
            feature_schema_version: training.manifest.feature_schema_version,
            feature_schema_hash: *training.feature_schema_hash,
            decision_policy_snapshot_id,
            profile_ref: model_version.profile_ref.clone(),
            prediction_horizon_secs,
            purpose: DatasetPurpose::Evaluation,
            window_start,
            window_end,
            research_program_hash: training_dataset.source_lineage.research_program_hash,
            sample_count: 10,
            decision_interval_secs: 1,
            trade_policy: bindings.trade_policy.clone(),
        },
    )
    .await
    .expect("Evaluation Dataset");
    let evaluation_dataset_hash = *evaluation_dataset
        .materialization()
        .expect("Evaluation Dataset materialization")
        .dataset_hash;

    let model_run_id = ModelRunId::from_v7();
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::Backtest,
            model_version_id: Some(model_version_id),
            decision_policy_snapshot_id,
            market_selection_id: None,
            window_start,
            window_end,
            input_hash: evaluation_dataset_hash,
        })
        .await
        .expect("model run");

    BacktestLineage {
        model_version_id,
        model_run_id,
        evaluation_dataset_id: evaluation_dataset.training_dataset_id,
        evaluation_dataset_hash,
        training_dataset_id,
        decision_policy_snapshot_id,
        window_start,
        window_end,
    }
}

fn assert_invariant(
    context: &str,
    expected_entity: &'static str,
    result: Result<impl Debug, StorageError>,
) {
    let error = match result {
        Ok(value) => {
            panic!("{context}: backtest lineage mismatch must fail closed, got {value:?}")
        }
        Err(error) => error,
    };
    assert!(
        matches!(
            error,
            StorageError::InvariantViolation {
                entity: Some(actual_entity),
                ..
            } if actual_entity == expected_entity
        ),
        "{context}: expected typed {expected_entity} lineage rejection, got {error:?}"
    );
}

fn seal_report(report: &mut NewBacktestReport) {
    report.report_hash = report.recomputed_hash().expect("canonical report hash");
}

fn new_report(
    backtest_report_id: BacktestReportId,
    lineage: &BacktestLineage,
) -> NewBacktestReport {
    let mut report = NewBacktestReport {
        backtest_report_id,
        model_version_id: lineage.model_version_id,
        evaluation_dataset_id: lineage.evaluation_dataset_id,
        model_run_id: lineage.model_run_id,
        decision_policy_snapshot_id: lineage.decision_policy_snapshot_id,
        window_start: lineage.window_start,
        window_end: lineage.window_end,
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
        portfolio_funnel: fully_resolved_backtest_funnel(1, 10),
        report_hash: content_hash('0'),
        parquet_uri: None,
    };
    seal_report(&mut report);
    report
}

pub async fn quant_backtest_report_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_runtime_config(&db).await;
    let window_start = Utc::now() - Duration::hours(2);
    let window_end = window_start + Duration::hours(1);
    let lineage = Box::pin(seed_model_version(
        &db,
        "pg-backtest-crud",
        window_start,
        window_end,
    ))
    .await;
    let repo = PgBacktestReportRepository::new(db.clone());
    let report_id = BacktestReportId::from_v7();

    let created = repo
        .create(new_report(report_id, &lineage))
        .await
        .expect("create");
    assert_eq!(created.backtest_report_id, report_id);
    assert_eq!(created.rank_ic, dec!(0.42));
    assert_eq!(created.sample_count, 10);
    created.verify_hash().expect("persisted canonical hash");

    let found = repo
        .find_by_id(&report_id)
        .await
        .expect("find")
        .expect("row");
    assert_eq!(found.report_hash, created.report_hash);
    assert_eq!(found.hit_rate, Probability::new(dec!(0.6)));

    let listed = repo
        .list_by_model_version(&lineage.model_version_id)
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].backtest_report_id, report_id);

    let duplicate = repo
        .create(new_report(BacktestReportId::from_v7(), &lineage))
        .await
        .expect_err("one model run must own exactly one backtest report");
    assert!(
        matches!(
            duplicate,
            StorageError::Duplicate {
                entity: QUANT_BACKTEST_REPORT,
                ..
            }
        ),
        "same-run conflict must be a typed duplicate, got {duplicate:?}"
    );
}

pub async fn backtest_report_is_worm() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_runtime_config(&db).await;
    let window_start = Utc::now() - Duration::hours(2);
    let window_end = window_start + Duration::hours(1);
    let lineage = Box::pin(seed_model_version(
        &db,
        "pg-backtest-worm",
        window_start,
        window_end,
    ))
    .await;
    let report_id = BacktestReportId::from_v7();
    PgBacktestReportRepository::new(db.clone())
        .create(new_report(report_id, &lineage))
        .await
        .expect("create WORM report");

    let update = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_backtest_report SET report_hash = report_hash \
             WHERE backtest_report_id = $1",
            [report_id.as_uuid().into()],
        ))
        .await;
    assert!(
        update.is_err(),
        "backtest report UPDATE must be rejected by the WORM trigger"
    );

    let delete = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM quant_backtest_report WHERE backtest_report_id = $1",
            [report_id.as_uuid().into()],
        ))
        .await;
    assert!(
        delete.is_err(),
        "backtest report DELETE must be rejected by the WORM trigger"
    );
}

pub async fn backtest_report_rejects_lineage() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    seed_runtime_config(&db).await;
    let window_start = Utc::now() - Duration::hours(2);
    let window_end = window_start + Duration::hours(1);
    let lineage = Box::pin(seed_model_version(
        &db,
        "pg-backtest-lineage",
        window_start,
        window_end,
    ))
    .await;
    let repo = PgBacktestReportRepository::new(db.clone());

    let mut report = new_report(BacktestReportId::from_v7(), &lineage);
    report.report_hash = content_hash('f');
    assert_invariant(
        "forged report hash",
        QUANT_BACKTEST_REPORT,
        repo.create(report).await,
    );

    let mut report = new_report(BacktestReportId::from_v7(), &lineage);
    report.window_end -= Duration::minutes(1);
    seal_report(&mut report);
    assert_invariant(
        "window drift",
        QUANT_BACKTEST_REPORT,
        repo.create(report).await,
    );

    let mut report = new_report(BacktestReportId::from_v7(), &lineage);
    report.sample_count = 11;
    seal_report(&mut report);
    assert_invariant(
        "sample-count overflow",
        QUANT_BACKTEST_REPORT,
        repo.create(report).await,
    );

    let calibration_run_id = ModelRunId::from_v7();
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id: calibration_run_id,
            run_kind: ModelRunKind::Calibration,
            model_version_id: Some(lineage.model_version_id),
            decision_policy_snapshot_id: lineage.decision_policy_snapshot_id,
            market_selection_id: None,
            window_start: lineage.window_start,
            window_end: lineage.window_end,
            input_hash: lineage.evaluation_dataset_hash,
        })
        .await
        .expect("calibration run");
    let mut report = new_report(BacktestReportId::from_v7(), &lineage);
    report.model_run_id = calibration_run_id;
    seal_report(&mut report);
    assert_invariant(
        "non-backtest run",
        QUANT_BACKTEST_REPORT,
        repo.create(report).await,
    );

    let mut report = new_report(BacktestReportId::from_v7(), &lineage);
    report.evaluation_dataset_id = lineage.training_dataset_id;
    seal_report(&mut report);
    assert_invariant(
        "non-evaluation dataset",
        QUANT_TRAINING_DATASET,
        repo.create(report).await,
    );

    let other_lineage = Box::pin(seed_model_version(
        &db,
        "pg-backtest-other-spec",
        window_start,
        window_end,
    ))
    .await;
    let mut report = new_report(BacktestReportId::from_v7(), &lineage);
    report.evaluation_dataset_id = other_lineage.evaluation_dataset_id;
    seal_report(&mut report);
    assert_invariant(
        "model-spec drift",
        QUANT_TRAINING_DATASET,
        repo.create(report).await,
    );

    let mut report = new_report(BacktestReportId::from_v7(), &lineage);
    report.decision_policy_snapshot_id = DecisionPolicySnapshotId::from_v7();
    seal_report(&mut report);
    assert_invariant(
        "policy drift",
        QUANT_BACKTEST_REPORT,
        repo.create(report).await,
    );
}
