//! Global portfolio-plan evidence persistence system contract.

use chrono::Utc;
use quant_pivot_models::{
    domain::quant::PortfolioDecisionResult, runtime_config::BuyModelRoute, types::Usd,
};
use quant_pivot_repository::{
    postgres::PgPortfolioPlanRepository, traits::PortfolioPlanRepository,
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::{
        execution_pg_seed::{
            ReportBuildOptions, ReportSeedConfig, build_custom_report_transaction,
            prepare_report_on_infra, seed_shared_demo_infra,
        },
        report_lifecycle_seed::persist_and_publish_report,
    },
};
use rust_decimal_macros::dec;

pub async fn optimizer_meta_persisted_row() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let infra = seed_shared_demo_infra(&db).await;
    let config = ReportSeedConfig {
        event_id: "portfolio-plan-evidence-event".to_owned(),
        market_id: "portfolio-plan-evidence-market".to_owned(),
        market_question: "Will exact global portfolio evidence persist?".to_owned(),
        market_slug: "portfolio-plan-evidence".to_owned(),
        token_id: "portfolio-plan-evidence-token".to_owned(),
        trigger_key: "portfolio-plan-evidence-trigger".to_owned(),
    };
    let ids = prepare_report_on_infra(&db, &infra, &config, Utc::now()).await;
    ids.complete_model_run(&db).await;
    let mut options = ReportBuildOptions::published_single(&ids);
    options.account_capital_usd = Some(Usd::new(dec!(555.56)));
    let transaction = build_custom_report_transaction(&ids, options);
    assert_eq!(
        transaction.account_snapshot.capital_base_usd,
        Usd::new(dec!(555.56))
    );
    assert_eq!(
        transaction.account_snapshot.available_usd,
        Usd::new(dec!(495.56))
    );
    assert_eq!(
        transaction.equity_snapshot.high_water_mark_usd,
        Usd::new(dec!(555.56))
    );
    assert_eq!(transaction.report.capital_base_usd, Usd::new(dec!(555.56)));
    persist_and_publish_report(&db, transaction, &config.trigger_key, 10).await;

    let loaded = PgPortfolioPlanRepository::new(db)
        .find_by_id(&ids.portfolio_plan)
        .await
        .expect("read global portfolio plan")
        .expect("global portfolio plan row exists");

    assert_eq!(
        loaded.represented_routes_json.routes,
        [BuyModelRoute::Weather]
    );
    assert!(loaded.scenario_artifact_id.is_some());
    assert!(loaded.scenario_artifact_hash.is_some());
    let PortfolioDecisionResult::Optimized { plan } = loaded.decision_json else {
        panic!("non-empty report must persist an optimized global portfolio plan");
    };
    assert_eq!(plan.solver.backend, "highs");
    assert!(plan.solver.optimal);
    assert!(plan.exact_verification.passed);
    assert_eq!(plan.selected_tier_ids.len(), 1);
}
