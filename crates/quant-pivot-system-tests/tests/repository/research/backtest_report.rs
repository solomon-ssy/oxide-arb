//! Backtest-report ledger persistence system contract.

use std::fmt::Debug;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::quant::{NewBacktestReport, NewModelRun, NewModelVersion},
    enums::{
        model::ModelFamily,
        quant::{
            DatasetPurpose, ModelRunKind, ModelRunStatus, PublicationStatus, TrainingDatasetStatus,
        },
    },
    types::{
        ArtifactUri, BacktestReportId, ContentHash, DecisionPolicySnapshotId, ModelInputContract,
        ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId, Probability,
        ResearchProfileRef, SchemaVersion, TrainingDatasetId, TrainingSampleSources,
        backtest::{CategoryMetrics, ExpectedVsRealized, PnlSimulation},
        default_sample_sources,
        model_metrics::ModelVersionMetrics,
        model_training::ModelTrainingObjective,
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
        execution_pg_seed, model_spec_fixtures,
        policy_fixtures::bootstrap_default_policy_bundle,
        research_fixtures::{
            DatasetLedgerFixture, DatasetLedgerSeed, DatasetSourceSeed, EvaluationDatasetSeed,
            seed_dataset_source, seed_evaluation_dataset,
        },
    },
};
use rust_decimal_macros::dec;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};

struct BacktestLineage {
    model_version_id: ModelVersionId,
    model_run_id: ModelRunId,
    evaluation_dataset_id: TrainingDatasetId,
    model_spec_id: ModelSpecId,
    model_spec_definition_hash: ContentHash,
    profile_ref: ResearchProfileRef,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
}

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

async fn seed_runtime_config(db: &DatabaseConnection) -> DecisionPolicySnapshotId {
    bootstrap_default_policy_bundle(db, "pg-backtest-it", "integration test").await
}

async fn seed_model_version(
    db: &DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
    scope: &str,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> BacktestLineage {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    let model_spec = model_spec_fixtures::new_model_spec_fixture(
        model_spec_id,
        scope,
        ModelFamily::WeightedFactor,
        86_400,
        ModelInputContract::single_required("book.mid"),
        ModelTrainingContract::settlement_default(),
    );
    let model_spec_definition_hash = model_spec.definition_hash;
    registry
        .create_model_spec(model_spec)
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
    let evaluation_dataset_id = seed_evaluation_dataset(
        db,
        EvaluationDatasetSeed {
            scope: format!("{scope}:{model_run_id}"),
            model_spec_id,
            model_spec_definition_hash,
            profile_ref: execution_pg_seed::fixture_profile_ref(),
            decision_policy_snapshot_id: *rc_id,
            window_start,
            window_end,
            sample_count: 10,
        },
    )
    .await
    .expect("evaluation dataset");
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::Backtest,
            model_version_id: Some(model_version_id),
            decision_policy_snapshot_id: *rc_id,
            market_selection_id: None,
            window_start,
            window_end,
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

    BacktestLineage {
        model_version_id,
        model_run_id,
        evaluation_dataset_id,
        model_spec_id,
        model_spec_definition_hash,
        profile_ref: execution_pg_seed::fixture_profile_ref(),
        decision_policy_snapshot_id: *rc_id,
        window_start,
        window_end,
    }
}

async fn seed_training_dataset(
    db: &DatabaseConnection,
    lineage: &BacktestLineage,
    scope: &str,
) -> TrainingDatasetId {
    let training_dataset_id = TrainingDatasetId::from_v7();
    let source_lineage = seed_dataset_source(
        db,
        DatasetSourceSeed {
            scope: scope.to_owned(),
            profile_ref: lineage.profile_ref.clone(),
            decision_policy_snapshot_id: lineage.decision_policy_snapshot_id,
            window_start: lineage.window_start,
            window_end: lineage.window_end,
            pit_cutoff: lineage.window_end + ChronoDuration::hours(1),
        },
    )
    .await
    .expect("training source lineage");
    let fixture = DatasetLedgerFixture::try_new(DatasetLedgerSeed {
        training_dataset_id,
        model_spec_id: lineage.model_spec_id,
        model_spec_definition_hash: lineage.model_spec_definition_hash,
        source_lineage,
        cohort_manifest: None,
        window_start: lineage.window_start,
        window_end: lineage.window_end,
        purpose: DatasetPurpose::Training,
        knowledge_lag_secs: 0,
        sample_interval_secs: 3_600,
        horizons_secs: vec![0],
        feature_schema_version: Some(SchemaVersion::FIRST),
        sample_sources: Some(TrainingSampleSources(default_sample_sources())),
        feature_schema_hash: content_hash('1'),
        factor_schema_hash: content_hash('2'),
        label_schema_hash: content_hash('3'),
        semantic_dataset_hash: content_hash('4'),
        source_fingerprint: content_hash('5'),
        sample_count: 10,
    })
    .expect("training dataset fixture");
    let repository = PgTrainingDatasetRepository::new(db.clone());
    repository
        .create_plan(fixture.plan.clone())
        .await
        .expect("training dataset plan");
    repository
        .start_build(&training_dataset_id)
        .await
        .expect("training dataset build");
    repository
        .complete_build(
            &training_dataset_id,
            fixture
                .completion(
                    TrainingDatasetStatus::Ready,
                    content_hash('6'),
                    ArtifactUri::parse(format!("s3://fixture/backtest/{scope}/training.parquet"))
                        .expect("training artifact URI"),
                    fixture.coverage(),
                    None,
                )
                .expect("training dataset completion"),
        )
        .await
        .expect("complete training dataset");
    training_dataset_id
}

fn assert_invariant(context: &str, result: Result<impl Debug, StorageError>) {
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
                entity: Some(entity::QUANT_BACKTEST_REPORT),
                ..
            }
        ),
        "{context}: expected typed backtest lineage rejection, got {error:?}"
    );
}

fn new_report(
    backtest_report_id: BacktestReportId,
    lineage: &BacktestLineage,
) -> NewBacktestReport {
    NewBacktestReport {
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
        report_hash: content_hash('e'),
        parquet_uri: None,
    }
}

pub async fn quant_backtest_report_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let window_start = Utc::now() - ChronoDuration::hours(2);
    let window_end = window_start + ChronoDuration::hours(1);
    let lineage =
        seed_model_version(&db, &rc_id, "pg-backtest-crud", window_start, window_end).await;
    let repo = PgBacktestReportRepository::new(db.clone());
    let report_id = BacktestReportId::from_v7();

    let created = repo
        .create(new_report(report_id, &lineage))
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
                entity: entity::QUANT_BACKTEST_REPORT,
                ..
            }
        ),
        "same-run conflict must be a typed duplicate, got {duplicate:?}"
    );
}

pub async fn backtest_report_is_worm() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let window_start = Utc::now() - ChronoDuration::hours(2);
    let window_end = window_start + ChronoDuration::hours(1);
    let lineage =
        seed_model_version(&db, &rc_id, "pg-backtest-worm", window_start, window_end).await;
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
    let rc_id = seed_runtime_config(&db).await;
    let window_start = Utc::now() - ChronoDuration::hours(2);
    let window_end = window_start + ChronoDuration::hours(1);
    let lineage =
        seed_model_version(&db, &rc_id, "pg-backtest-lineage", window_start, window_end).await;
    let repo = PgBacktestReportRepository::new(db.clone());

    let mut report = new_report(BacktestReportId::from_v7(), &lineage);
    report.window_end -= ChronoDuration::minutes(1);
    assert_invariant("window drift", repo.create(report).await);

    let mut report = new_report(BacktestReportId::from_v7(), &lineage);
    report.sample_count = 11;
    assert_invariant("sample-count overflow", repo.create(report).await);

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
            status: ModelRunStatus::Running,
            input_hash: content_hash('7'),
            output_hash: None,
            error_code: None,
            error_message: None,
            started_at: Utc::now(),
            finished_at: None,
        })
        .await
        .expect("calibration run");
    let mut report = new_report(BacktestReportId::from_v7(), &lineage);
    report.model_run_id = calibration_run_id;
    assert_invariant("non-backtest run", repo.create(report).await);

    let training_dataset_id =
        seed_training_dataset(&db, &lineage, "pg-backtest-wrong-purpose").await;
    let mut report = new_report(BacktestReportId::from_v7(), &lineage);
    report.evaluation_dataset_id = training_dataset_id;
    assert_invariant("non-evaluation dataset", repo.create(report).await);

    let other_lineage = seed_model_version(
        &db,
        &rc_id,
        "pg-backtest-other-spec",
        window_start,
        window_end,
    )
    .await;
    let mut report = new_report(BacktestReportId::from_v7(), &lineage);
    report.evaluation_dataset_id = other_lineage.evaluation_dataset_id;
    assert_invariant("model-spec drift", repo.create(report).await);

    let mut report = new_report(BacktestReportId::from_v7(), &lineage);
    report.decision_policy_snapshot_id = DecisionPolicySnapshotId::from_v7();
    assert_invariant("policy drift", repo.create(report).await);
}
