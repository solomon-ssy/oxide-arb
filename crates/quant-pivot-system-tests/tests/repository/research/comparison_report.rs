//! Pairwise model-comparison report ledger persistence system contracts.

use std::{fmt::Debug, future::Future, pin::Pin};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        BacktestReportInfo, ModelVersionInfo, NewBacktestReport, NewModelComparisonReport,
        NewModelRun,
    },
    enums::{
        model::ModelFamily,
        quant::{DatasetPurpose, ModelRunKind},
    },
    types::{
        BacktestReportId, ContentHash, DecisionPolicySnapshotId, ModelComparisonReportId,
        ModelInputContract, ModelRunId, ModelSpecId, ModelTrainingContract, ModelVersionId,
        Probability, TrainingDatasetId,
        backtest::{CategoryMetrics, CategoryRankIcDeltas, ExpectedVsRealized, PnlSimulation},
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestReportRepository, PgModelComparisonReportRepository, PgModelRegistryRepository,
        PgModelRunRepository, PgTrainingDatasetRepository,
    },
    traits::{
        BacktestReportRepository, ModelComparisonReportRepository, ModelRegistryRepository,
        ModelRunRepository, TrainingDatasetRepository,
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
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement};

struct ComparisonFixture {
    baseline: BacktestReportInfo,
    candidate: BacktestReportInfo,
    candidate_run_id: ModelRunId,
}

impl ComparisonFixture {
    fn comparison(&self) -> NewModelComparisonReport {
        let mut report = NewModelComparisonReport {
            comparison_report_id: ModelComparisonReportId::from_v7(),
            baseline_model_version_id: self.baseline.model_version_id,
            candidate_model_version_id: self.candidate.model_version_id,
            baseline_report_id: self.baseline.backtest_report_id,
            candidate_report_id: self.candidate.backtest_report_id,
            model_run_id: self.candidate_run_id,
            rank_ic_delta: dec!(0.15),
            hit_rate_delta: dec!(0.05),
            realized_pnl_delta: dec!(80),
            score_correlation: dec!(0.95),
            side_disagreement_rate: dec!(0.5),
            common_samples: 2,
            category_breakdown_diff: CategoryRankIcDeltas::default(),
            comparison_hash: content_hash('0'),
        };
        report.comparison_hash = report
            .recomputed_hash(&self.baseline.report_hash, &self.candidate.report_hash)
            .expect("canonical comparison hash");
        report
    }
}

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("fixture hash")
}

async fn seed_runtime_config(db: &DatabaseConnection) {
    bootstrap_default_policy_bundle(db, "pg-comparison-it", "integration test").await;
}

async fn seed_model_version(
    db: &DatabaseConnection,
    model_spec_id: ModelSpecId,
    scope: &str,
    artifact_seed: char,
) -> ModelVersionInfo {
    let model_version_id = ModelVersionId::from_v7();
    let version = ModelVersionFixture::prepare(
        db,
        ModelVersionFixtureSeed::training(
            format!("{scope}:{model_version_id}"),
            model_version_id,
            model_spec_id,
            content_hash(artifact_seed),
        ),
    )
    .await
    .expect("prepare exact model version");
    PgModelRegistryRepository::new(db.clone())
        .create_model_version(version)
        .await
        .expect("model version")
}

async fn seed_run(
    db: &DatabaseConnection,
    model_version_id: ModelVersionId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    evaluation_dataset_hash: ContentHash,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
) -> ModelRunId {
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
    model_run_id
}

struct BacktestMetrics {
    rank_ic: Decimal,
    hit_rate: Probability,
    realized_pnl_usd: Decimal,
}

struct BacktestReportSeed {
    model_version_id: ModelVersionId,
    evaluation_dataset_id: TrainingDatasetId,
    model_run_id: ModelRunId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    metrics: BacktestMetrics,
}

async fn persist_report(db: &DatabaseConnection, seed: BacktestReportSeed) -> BacktestReportInfo {
    let mut report = NewBacktestReport {
        backtest_report_id: BacktestReportId::from_v7(),
        model_version_id: seed.model_version_id,
        evaluation_dataset_id: seed.evaluation_dataset_id,
        model_run_id: seed.model_run_id,
        decision_policy_snapshot_id: seed.decision_policy_snapshot_id,
        window_start: seed.window_start,
        window_end: seed.window_end,
        coverage: dec!(1),
        sample_count: 10,
        missing_feature_count: 0,
        rank_ic: seed.metrics.rank_ic,
        sharpe: dec!(0.8),
        hit_rate: seed.metrics.hit_rate,
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
            realized_pnl_usd: seed.metrics.realized_pnl_usd,
            gross_return: seed.metrics.realized_pnl_usd / dec!(100),
            pnl_curve: Vec::new(),
        },
        portfolio_funnel: fully_resolved_backtest_funnel(1, 10),
        report_hash: content_hash('0'),
        parquet_uri: None,
    };
    report.report_hash = report.recomputed_hash().expect("canonical report hash");
    PgBacktestReportRepository::new(db.clone())
        .create(report)
        .await
        .expect("backtest report")
}

fn prepare_fixture(
    db: &DatabaseConnection,
) -> Pin<Box<dyn Future<Output = ComparisonFixture> + Send + '_>> {
    Box::pin(prepare_fixture_inner(db))
}

async fn prepare_fixture_inner(db: &DatabaseConnection) -> ComparisonFixture {
    seed_runtime_config(db).await;
    let model_spec_id = ModelSpecId::from_v7();
    PgModelRegistryRepository::new(db.clone())
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            model_spec_id,
            "pg-comparison-it",
            ModelFamily::WeightedFactor,
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::outcome_default(),
        ))
        .await
        .expect("model spec");
    let baseline_version = seed_model_version(db, model_spec_id, "comparison-baseline", 'a').await;
    let candidate_version =
        seed_model_version(db, model_spec_id, "comparison-candidate", 'b').await;
    let baseline_bindings = baseline_version
        .verified_serving_contract()
        .expect("baseline serving contract")
        .bindings();
    let candidate_bindings = candidate_version
        .verified_serving_contract()
        .expect("candidate serving contract")
        .bindings();
    assert_eq!(
        baseline_bindings.policy_snapshot, candidate_bindings.policy_snapshot,
        "comparison fixture models require one exact policy snapshot"
    );
    assert_eq!(
        baseline_bindings.factors.plane, candidate_bindings.factors.plane,
        "comparison fixture models require one exact factor plane"
    );
    let training_dataset_id = baseline_version
        .training_dataset_id
        .expect("baseline Training Dataset");
    let training_dataset = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&training_dataset_id)
        .await
        .expect("load baseline Training Dataset")
        .expect("baseline Training Dataset exists");
    let training = training_dataset
        .materialization()
        .expect("baseline Training Dataset materialization");
    let window_start = Utc::now() - Duration::hours(2);
    let window_end = window_start + Duration::hours(1);
    let decision_policy_snapshot_id = baseline_bindings
        .policy_snapshot
        .decision_policy_snapshot_id;
    let evaluation_dataset = Box::pin(ModelDatasetLedgerFixture::persist(
        db,
        &ModelDatasetLedgerFixture::local_store(),
        ModelDatasetLedgerSeed {
            scope: "pg-comparison:evaluation".to_owned(),
            model_spec_id,
            model_family: baseline_version.model_family,
            model_spec_definition_hash: baseline_version.model_spec_definition_hash,
            factor_serving_plane: training.factor_serving_plane.clone(),
            feature_schema_version: training.manifest.feature_schema_version,
            feature_schema_hash: *training.feature_schema_hash,
            decision_policy_snapshot_id,
            profile_ref: baseline_version.profile_ref.clone(),
            prediction_horizon_secs: baseline_bindings.model.prediction_horizon_secs,
            purpose: DatasetPurpose::Evaluation,
            window_start,
            window_end,
            research_program_hash: training_dataset.source_lineage.research_program_hash,
            sample_count: 10,
            decision_interval_secs: 1,
            trade_policy: baseline_bindings.trade_policy.clone(),
        },
    ))
    .await
    .expect("shared Evaluation Dataset");
    let evaluation_dataset_hash = *evaluation_dataset
        .materialization()
        .expect("Evaluation Dataset materialization")
        .dataset_hash;
    let baseline_run_id = seed_run(
        db,
        baseline_version.model_version_id,
        decision_policy_snapshot_id,
        evaluation_dataset_hash,
        window_start,
        window_end,
    )
    .await;
    let candidate_run_id = seed_run(
        db,
        candidate_version.model_version_id,
        decision_policy_snapshot_id,
        evaluation_dataset_hash,
        window_start,
        window_end,
    )
    .await;
    let baseline = persist_report(
        db,
        BacktestReportSeed {
            model_version_id: baseline_version.model_version_id,
            evaluation_dataset_id: evaluation_dataset.training_dataset_id,
            model_run_id: baseline_run_id,
            decision_policy_snapshot_id,
            window_start,
            window_end,
            metrics: BacktestMetrics {
                rank_ic: dec!(0.30),
                hit_rate: Probability::new(dec!(0.60)),
                realized_pnl_usd: dec!(8),
            },
        },
    )
    .await;
    let candidate = persist_report(
        db,
        BacktestReportSeed {
            model_version_id: candidate_version.model_version_id,
            evaluation_dataset_id: evaluation_dataset.training_dataset_id,
            model_run_id: candidate_run_id,
            decision_policy_snapshot_id,
            window_start,
            window_end,
            metrics: BacktestMetrics {
                rank_ic: dec!(0.45),
                hit_rate: Probability::new(dec!(0.65)),
                realized_pnl_usd: dec!(88),
            },
        },
    )
    .await;
    PgModelRunRepository::new(db.clone())
        .succeed(
            &baseline_run_id,
            baseline.report_hash,
            Some(baseline.model_version_id),
        )
        .await
        .expect("finish baseline Backtest run");
    ComparisonFixture {
        baseline,
        candidate,
        candidate_run_id,
    }
}

fn assert_invariant(context: &str, result: Result<impl Debug, StorageError>) {
    let error = result.expect_err(context);
    assert!(
        matches!(
            error,
            StorageError::InvariantViolation {
                entity: Some("quant_model_comparison_report"),
                ..
            }
        ),
        "{context}: expected typed comparison invariant, got {error:?}"
    );
}

pub async fn quant_model_comparison_crud() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = prepare_fixture(&db).await;
    let model_run_repo = PgModelRunRepository::new(db.clone());
    let repo = PgModelComparisonReportRepository::new(db.clone());

    let while_running = repo.create(fixture.comparison()).await;
    assert_invariant(
        "comparison must reject a Running candidate Backtest run",
        while_running,
    );
    model_run_repo
        .succeed(
            &fixture.candidate_run_id,
            fixture.candidate.report_hash,
            Some(fixture.candidate.model_version_id),
        )
        .await
        .expect("finish candidate Backtest run");

    let comparison = fixture.comparison();
    let comparison_report_id = comparison.comparison_report_id;
    let created = repo.create(comparison).await.expect("create comparison");
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
        .list_by_candidate_version(&fixture.candidate.model_version_id)
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].comparison_report_id, comparison_report_id);

    let candidate_report_id = fixture.candidate.backtest_report_id;
    let baseline_report_id = fixture.baseline.backtest_report_id;
    let by_candidate = repo
        .find_by_backtest_report(&candidate_report_id)
        .await
        .expect("find by candidate report")
        .expect("row");
    assert_eq!(by_candidate.comparison_report_id, comparison_report_id);
    let by_baseline = repo
        .find_by_backtest_report(&baseline_report_id)
        .await
        .expect("find by baseline report")
        .expect("row");
    assert_eq!(by_baseline.comparison_report_id, comparison_report_id);

    let candidate_only = repo
        .backtest_comparison_ids(&[candidate_report_id])
        .await
        .expect("candidate-only batch lookup");
    assert_eq!(candidate_only.len(), 1);
    assert_eq!(
        candidate_only.get(&candidate_report_id),
        Some(&comparison_report_id)
    );
    assert!(
        !candidate_only.contains_key(&baseline_report_id),
        "batch lookup must return only the requested report-id intersection"
    );
}

pub async fn comparison_rejects_tampering() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = prepare_fixture(&db).await;
    PgModelRunRepository::new(db.clone())
        .succeed(
            &fixture.candidate_run_id,
            fixture.candidate.report_hash,
            Some(fixture.candidate.model_version_id),
        )
        .await
        .expect("finish candidate Backtest run");
    let repo = PgModelComparisonReportRepository::new(db.clone());

    let mut forged_hash = fixture.comparison();
    forged_hash.comparison_hash = content_hash('f');
    assert_invariant("forged comparison hash", repo.create(forged_hash).await);

    let mut forged_delta = fixture.comparison();
    forged_delta.rank_ic_delta = dec!(0.14);
    forged_delta.comparison_hash = forged_delta
        .recomputed_hash(
            &fixture.baseline.report_hash,
            &fixture.candidate.report_hash,
        )
        .expect("reseal forged delta");
    assert_invariant(
        "forged report-derived delta",
        repo.create(forged_delta).await,
    );

    let mut forged_run = fixture.comparison();
    forged_run.model_run_id = fixture.baseline.model_run_id;
    forged_run.comparison_hash = forged_run
        .recomputed_hash(
            &fixture.baseline.report_hash,
            &fixture.candidate.report_hash,
        )
        .expect("reseal forged candidate run");
    assert_invariant("wrong candidate run", repo.create(forged_run).await);
}

pub async fn comparison_report_is_worm() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = prepare_fixture(&db).await;
    PgModelRunRepository::new(db.clone())
        .succeed(
            &fixture.candidate_run_id,
            fixture.candidate.report_hash,
            Some(fixture.candidate.model_version_id),
        )
        .await
        .expect("finish candidate Backtest run");
    let comparison = PgModelComparisonReportRepository::new(db.clone())
        .create(fixture.comparison())
        .await
        .expect("create WORM comparison");

    let update = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_model_comparison_report SET comparison_hash = comparison_hash \
             WHERE comparison_report_id = $1",
            [comparison.comparison_report_id.as_uuid().into()],
        ))
        .await;
    assert!(
        update.is_err(),
        "comparison report UPDATE must be rejected by the WORM trigger"
    );
    let delete = db
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM quant_model_comparison_report WHERE comparison_report_id = $1",
            [comparison.comparison_report_id.as_uuid().into()],
        ))
        .await;
    assert!(
        delete.is_err(),
        "comparison report DELETE must be rejected by the WORM trigger"
    );
}
