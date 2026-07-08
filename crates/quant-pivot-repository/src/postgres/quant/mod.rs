//! Quant-pivot Postgres repository implementations.
mod account_snapshot;
mod attribution;
mod backtest_report;
mod basis_alert;
mod calibration_artifact;
mod capital_allocation;
mod comparison_report;
mod dataset;
mod domain_source_cursor;
mod equity_snapshot;
mod execution_order;
mod execution_submission;
mod factor;
mod feature;
mod governance_audit;
mod market_linkage;
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
mod trade_tape_block_cursor;

pub use {
    account_snapshot::PgAccountSnapshotRepository, attribution::PgAttributionRepository,
    backtest_report::PgBacktestReportRepository, basis_alert::PgBasisAlertRepository,
    calibration_artifact::PgCalibrationArtifactRepository,
    capital_allocation::PgCapitalAllocationRepository,
    comparison_report::PgModelComparisonReportRepository, dataset::PgTrainingDatasetRepository,
    domain_source_cursor::PgDomainSourceCursorRepository,
    equity_snapshot::PgEquitySnapshotRepository, execution_order::PgExecutionOrderRepository,
    execution_submission::PgExecutionSubmissionRepository, factor::PgFactorRepository,
    feature::PgFeatureRepository, governance_audit::PgModelGovernanceAuditRepository,
    market_linkage::PgMarketLinkageRepository, market_selection::PgMarketSelectionRepository,
    model_registry::PgModelRegistryRepository, model_run::PgModelRunRepository,
    order_intent::PgOrderIntentRepository, portfolio_plan::PgPortfolioPlanRepository,
    position::PgPositionRepository, recommendation::PgRecommendationRepository,
    recommendation_report::PgRecommendationReportRepository,
    reconciliation::PgReconciliationRepository, research_job::PgResearchJobRepository,
    reserved_capital::PgReservedCapitalRepository, settlement_redeem::PgSettlementRedeemRepository,
    shadow_comparison::PgShadowComparisonRepository,
    trade_tape_block_cursor::PgTradeTapeBlockCursorRepository,
};
