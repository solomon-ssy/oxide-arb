//! Portfolio-plan optimizer metadata persistence (Postgres + testcontainers).

use chrono::Utc;
use quant_pivot_models::{
    domain::{NewMarketSelection, NewModelRun, NewModelVersion, NewPortfolioPlan},
    entities::quant_portfolio_plan,
    enums::{
        model::ModelFamily,
        quant::{
            CorrelationSource, ModelRunKind, ModelRunStatus, OptimizerSolverStatus,
            PortfolioSolveMode, PortfolioSolverKind, PublicationStatus,
        },
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, MarketSelectionId, ModelInputContract, ModelRunId,
        ModelSpecId, ModelTrainingContract, ModelVersionId, PortfolioConstraintsSnapshot,
        PortfolioOptimizerMeta, PortfolioPlanId, PortfolioRejectedSummary, PortfolioRiskBudget,
        SelectionExclusionSummary, Usd, model_metrics::ModelVersionMetrics,
        model_training::ModelTrainingObjective,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgMarketSelectionRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgPortfolioPlanRepository,
    },
    traits::{
        MarketSelectionRepository, ModelRegistryRepository, ModelRunRepository,
        PortfolioPlanRepository,
    },
};
use quant_pivot_test_support::{pg::setup_pg, policy_fixtures::bootstrap_default_policy_bundle};
use rust_decimal_macros::dec;
use sea_orm::{EntityTrait, IntoActiveModel};

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

async fn seed_runtime_config(db: &sea_orm::DatabaseConnection) -> DecisionPolicySnapshotId {
    bootstrap_default_policy_bundle(db, "portfolio-optimizer-meta-it", "integration test").await
}

async fn seed_model_run(
    db: &sea_orm::DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
) -> ModelRunId {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(
            quant_pivot_test_support::model_spec_fixtures::new_model_spec_fixture(
                model_spec_id.clone(),
                "portfolio-optimizer-meta-it",
                ModelFamily::WeightedFactor,
                86_400,
                ModelInputContract::single_required("book.mid"),
                ModelTrainingContract::settlement_default(),
            ),
        )
        .await
        .expect("model spec");

    let model_version_id = ModelVersionId::from_v7();
    registry
        .create_model_version(NewModelVersion {
            model_version_id: model_version_id.clone(),
            model_spec_id,
            version: 1,
            profile_ref: quant_pivot_test_support::execution_pg_seed::fixture_profile_ref(),
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
    PgModelRunRepository::new(db.clone())
        .create(NewModelRun {
            model_run_id: model_run_id.clone(),
            run_kind: ModelRunKind::LiveInference,
            model_version_id: Some(model_version_id),
            decision_policy_snapshot_id: rc_id.clone(),
            market_selection_id: None,
            window_start: Utc::now(),
            window_end: Utc::now(),
            status: ModelRunStatus::Succeeded,
            input_hash: content_hash('d'),
            output_hash: None,
            error_code: None,
            error_message: None,
            started_at: Utc::now(),
            finished_at: Some(Utc::now()),
        })
        .await
        .expect("model run");
    model_run_id
}

async fn seed_market_selection(
    db: &sea_orm::DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
) -> MarketSelectionId {
    let id = MarketSelectionId::from_v7();
    PgMarketSelectionRepository::new(db.clone())
        .create_snapshot(
            NewMarketSelection {
                market_selection_id: id.clone(),
                decision_at: Utc::now(),
                decision_policy_snapshot_id: rc_id.clone(),
                selector_hash: content_hash('b'),
                market_count: 1,
                exclusion_summary: SelectionExclusionSummary::default(),
            },
            Vec::new(),
        )
        .await
        .expect("market selection");
    id
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn optimizer_meta_persisted_in_plan_row() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let rc_id = seed_runtime_config(&db).await;
    let model_run_id = seed_model_run(&db, &rc_id).await;
    let market_selection_id = seed_market_selection(&db, &rc_id).await;

    let meta = PortfolioOptimizerMeta {
        solver: PortfolioSolverKind::Microlp,
        solve_mode: PortfolioSolveMode::ContinuousRelaxation,
        status: OptimizerSolverStatus::FellBackRelaxation,
        fell_back_to_relaxation: true,
        objective_value: Some(dec!(123.45)),
        elapsed_ms: 7,
        correlation_source: CorrelationSource::Historical,
        constraint_conflicts: vec!["test forced milp failure".to_owned()],
    };

    let plan_id = PortfolioPlanId::from_v7();
    let plan = NewPortfolioPlan {
        portfolio_plan_id: plan_id.clone(),
        model_run_id: Some(model_run_id),
        market_selection_id,
        decision_at: Utc::now(),
        budget_usd: Usd::new(dec!(10000)),
        allocated_usd: Usd::ZERO,
        risk_budget_json: PortfolioRiskBudget::default(),
        constraints_json: PortfolioConstraintsSnapshot::default(),
        rejected_summary: PortfolioRejectedSummary::default(),
        optimizer_meta_json: meta.clone(),
    };

    quant_portfolio_plan::Entity::insert(plan.into_active_model())
        .exec(&db)
        .await
        .expect("insert portfolio plan");

    let loaded = PgPortfolioPlanRepository::new(db)
        .find_by_id(&plan_id)
        .await
        .expect("read portfolio plan")
        .expect("plan row exists");

    assert_eq!(loaded.optimizer_meta_json, meta);
    assert_eq!(
        loaded.optimizer_meta_json.objective_value,
        Some(dec!(123.45))
    );
}
