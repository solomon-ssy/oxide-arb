//! Portfolio-plan optimizer metadata persistence system contract.

use chrono::Utc;
use quant_pivot_models::{
    domain::quant::{NewMarketSelection, NewModelRun, NewPortfolioPlan},
    entities::quant_portfolio_plan::Entity,
    enums::{
        model::ModelFamily,
        quant::{
            CorrelationSource, ModelRunKind, OptimizerSolverStatus, PortfolioSolveMode,
            PortfolioSolverKind,
        },
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, MarketSelectionId, ModelInputContract, ModelRunId,
        ModelSpecId, ModelTrainingContract, ModelVersionId, PortfolioConstraintsSnapshot,
        PortfolioOptimizerMeta, PortfolioPlanId, PortfolioRejectedSummary, PortfolioRiskBudget,
        SelectionExclusionSummary, Usd,
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
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        model_serving_fixtures::{ModelVersionFixture, ModelVersionFixtureSeed},
        model_spec_fixtures,
        policy_fixtures::bootstrap_default_policy_bundle,
    },
};
use rust_decimal_macros::dec;
use sea_orm::{DatabaseConnection, EntityTrait, IntoActiveModel};

fn content_hash(seed: char) -> ContentHash {
    ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
}

async fn seed_runtime_config(db: &DatabaseConnection) -> DecisionPolicySnapshotId {
    bootstrap_default_policy_bundle(db, "portfolio-optimizer-meta-it", "integration test").await
}

async fn seed_model_run(db: &DatabaseConnection, rc_id: &DecisionPolicySnapshotId) -> ModelRunId {
    let registry = PgModelRegistryRepository::new(db.clone());
    let model_spec_id = ModelSpecId::from_v7();
    registry
        .create_model_spec(model_spec_fixtures::new_model_spec_fixture(
            model_spec_id,
            "portfolio-optimizer-meta-it",
            ModelFamily::WeightedFactor,
            model_spec_fixtures::pooled_horizon_secs(),
            ModelInputContract::single_required("book.mid"),
            ModelTrainingContract::settlement_default(),
        ))
        .await
        .expect("model spec");

    let model_version_id = ModelVersionId::from_v7();
    let version = ModelVersionFixture::prepare(
        db,
        ModelVersionFixtureSeed::training(
            format!("portfolio-optimizer:{model_version_id}"),
            model_version_id,
            model_spec_id,
            content_hash('a'),
        ),
    )
    .await
    .expect("prepare exact model version");
    registry
        .create_model_version(version)
        .await
        .expect("model version");

    let model_run_id = ModelRunId::from_v7();
    let runs = PgModelRunRepository::new(db.clone());
    let window_at = Utc::now();
    runs.create(NewModelRun {
        model_run_id,
        run_kind: ModelRunKind::LiveInference,
        model_version_id: Some(model_version_id),
        decision_policy_snapshot_id: *rc_id,
        market_selection_id: None,
        window_start: window_at,
        window_end: window_at,
        input_hash: content_hash('d'),
    })
    .await
    .expect("create model run");
    runs.succeed(&model_run_id, content_hash('e'), None)
        .await
        .expect("finish model run");
    model_run_id
}

async fn seed_market_selection(
    db: &DatabaseConnection,
    rc_id: &DecisionPolicySnapshotId,
) -> MarketSelectionId {
    let id = MarketSelectionId::from_v7();
    PgMarketSelectionRepository::new(db.clone())
        .create_snapshot(
            NewMarketSelection {
                market_selection_id: id,
                decision_at: Utc::now(),
                decision_policy_snapshot_id: *rc_id,
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

pub async fn optimizer_meta_persisted_row() {
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
        portfolio_plan_id: plan_id,
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

    Entity::insert(plan.into_active_model())
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
