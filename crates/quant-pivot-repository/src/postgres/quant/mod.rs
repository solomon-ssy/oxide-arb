//! Quant-pivot Postgres repository implementations.
mod account_snapshot;
mod attribution;
mod backtest_path_set;
mod backtest_report;
mod basis_alert;
mod calibration_artifact;
mod capital_allocation;
mod comparison_report;
pub(crate) mod condition_wake;
mod dataset;
mod domain_projection;
mod domain_source_cursor;
mod domain_source_expectation;
mod entry_condition;
mod equity_snapshot;
mod execution_order;
mod execution_submission;
mod factor;
mod feature;
mod feature_parity;
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
mod report_run;
mod report_scope;
mod research_job;
mod research_readiness;
mod reserved_capital;
mod settlement_redeem;
mod shadow_comparison;
mod source_slice;
mod trade_policy;
mod trade_tape_block_cursor;

pub use {
    account_snapshot::PgAccountSnapshotRepository, attribution::PgAttributionRepository,
    backtest_path_set::PgBacktestPathSetRepository, backtest_report::PgBacktestReportRepository,
    basis_alert::PgBasisAlertRepository, calibration_artifact::PgCalibrationArtifactRepository,
    capital_allocation::PgCapitalAllocationRepository,
    comparison_report::PgModelComparisonReportRepository, dataset::PgTrainingDatasetRepository,
    domain_projection::PgDomainProjectionRepository,
    domain_source_cursor::PgDomainSourceCursorRepository,
    domain_source_expectation::PgDomainSourceExpectationRepository,
    entry_condition::PgEntryConditionRepository, equity_snapshot::PgEquitySnapshotRepository,
    execution_order::PgExecutionOrderRepository,
    execution_submission::PgExecutionSubmissionRepository, factor::PgFactorRepository,
    feature::PgFeatureRepository, feature_parity::PgFeatureParityRepository,
    governance_audit::PgModelGovernanceAuditRepository, market_linkage::PgMarketLinkageRepository,
    market_selection::PgMarketSelectionRepository, model_registry::PgModelRegistryRepository,
    model_run::PgModelRunRepository, order_intent::PgOrderIntentRepository,
    portfolio_plan::PgPortfolioPlanRepository, position::PgPositionRepository,
    recommendation::PgRecommendationRepository,
    recommendation_report::PgRecommendationReportRepository,
    reconciliation::PgReconciliationRepository, report_run::PgReportRunRepository,
    research_job::PgResearchJobRepository,
    research_readiness::PgResearchReadinessEvidenceRepository,
    reserved_capital::PgReservedCapitalRepository, settlement_redeem::PgSettlementRedeemRepository,
    shadow_comparison::PgShadowComparisonRepository, source_slice::PgSourceSliceRepository,
    trade_policy::PgTradePolicyRepository,
    trade_tape_block_cursor::PgTradeTapeBlockCursorRepository,
};
