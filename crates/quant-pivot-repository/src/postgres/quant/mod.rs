//! Quant-pivot Postgres repository implementations.

mod account_snapshot;
mod attribution;
mod backtest_report;
mod capital_allocation;
mod comparison_report;
mod dataset;
mod equity_snapshot;
mod execution_order;
mod execution_submission;
mod factor;
mod favorite_longshot;
mod feature;
mod governance_audit;
mod market_selection;
mod model_registry;
mod model_run;
mod order_intent;
mod portfolio_plan;
mod position;
mod recommendation;
mod recommendation_report;
mod reconciliation;
mod research_job;
mod reserved_capital;
mod settlement_redeem;
mod shadow_comparison;

pub use {
    account_snapshot::PgAccountSnapshotRepository, attribution::PgAttributionRepository,
    backtest_report::PgBacktestReportRepository, capital_allocation::PgCapitalAllocationRepository,
    comparison_report::PgModelComparisonReportRepository, dataset::PgTrainingDatasetRepository,
    equity_snapshot::PgEquitySnapshotRepository, execution_order::PgExecutionOrderRepository,
    execution_submission::PgExecutionSubmissionRepository, factor::PgFactorRepository,
    favorite_longshot::PgFavoriteLongshotBiasTableRepository, feature::PgFeatureRepository,
    governance_audit::PgModelGovernanceAuditRepository,
    market_selection::PgMarketSelectionRepository, model_registry::PgModelRegistryRepository,
    model_run::PgModelRunRepository, order_intent::PgOrderIntentRepository,
    portfolio_plan::PgPortfolioPlanRepository, position::PgPositionRepository,
    recommendation::PgRecommendationRepository,
    recommendation_report::PgRecommendationReportRepository,
    reconciliation::PgReconciliationRepository, research_job::PgResearchJobRepository,
    reserved_capital::PgReservedCapitalRepository, settlement_redeem::PgSettlementRedeemRepository,
    shadow_comparison::PgShadowComparisonRepository,
};
