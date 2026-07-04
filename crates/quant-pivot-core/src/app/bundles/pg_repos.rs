//! Shared Postgres repositories wired once at boot from a single pool handle.
//!
//! Every `Pg*Repository` owns a [`DatabaseConnection`] (pool handle clone).
//! [`PgRepositories::wire`] constructs each repository exactly once; downstream
//! bundles and services [`Arc::clone`] the shared instances.

use quant_pivot_repository::postgres::{
    PgAccountSnapshotRepository, PgAttributionRepository, PgBacktestReportRepository,
    PgCapitalAllocationRepository, PgEquitySnapshotRepository, PgEventRepository,
    PgExecutionOrderRepository, PgExecutionSubmissionRepository, PgFactorRepository,
    PgFeatureRepository, PgKillSwitchStateRepository, PgMarketRepository,
    PgMarketSelectionRepository, PgMenuRepository, PgModelComparisonReportRepository,
    PgModelGovernanceAuditRepository, PgModelRegistryRepository, PgModelRunRepository,
    PgOperationLogRepository, PgOrderIntentRepository, PgPositionRepository,
    PgRecommendationReportRepository, PgRecommendationRepository, PgReconciliationRepository,
    PgResearchJobRepository, PgReservedCapitalRepository, PgRoleMenuRepository,
    PgRolePermissionRepository, PgRoleRepository, PgRuntimeConfigVersionRepository,
    PgSettlementRedeemRepository, PgShadowComparisonRepository, PgSystemRuntimeStateRepository,
    PgTrainingDatasetRepository, PgUserRepository, PgUserRoleRepository, arc_repo,
};
use quant_pivot_storage::postgres::PostgresPool;
use std::sync::Arc;

/// All Postgres OLTP repositories shared across runtime bundles.
pub struct PgRepositories {
    pub runtime_config: Arc<PgRuntimeConfigVersionRepository>,
    pub system_runtime_state: Arc<PgSystemRuntimeStateRepository>,
    pub kill_switch_state: Arc<PgKillSwitchStateRepository>,
    pub operation_log: Arc<PgOperationLogRepository>,
    pub market: Arc<PgMarketRepository>,
    pub event: Arc<PgEventRepository>,
    pub order_intent: Arc<PgOrderIntentRepository>,
    pub execution_submission: Arc<PgExecutionSubmissionRepository>,
    pub execution_order: Arc<PgExecutionOrderRepository>,
    pub reconciliation: Arc<PgReconciliationRepository>,
    pub position: Arc<PgPositionRepository>,
    pub capital_allocation: Arc<PgCapitalAllocationRepository>,
    pub settlement_redeem: Arc<PgSettlementRedeemRepository>,
    pub recommendation: Arc<PgRecommendationRepository>,
    pub recommendation_report: Arc<PgRecommendationReportRepository>,
    pub equity_snapshot: Arc<PgEquitySnapshotRepository>,
    pub attribution: Arc<PgAttributionRepository>,
    pub market_selection: Arc<PgMarketSelectionRepository>,
    pub feature: Arc<PgFeatureRepository>,
    pub factor: Arc<PgFactorRepository>,
    pub model_run: Arc<PgModelRunRepository>,
    pub model_registry: Arc<PgModelRegistryRepository>,
    pub shadow_comparison: Arc<PgShadowComparisonRepository>,
    pub training_dataset: Arc<PgTrainingDatasetRepository>,
    pub backtest_report: Arc<PgBacktestReportRepository>,
    pub comparison_report: Arc<PgModelComparisonReportRepository>,
    pub governance_audit: Arc<PgModelGovernanceAuditRepository>,
    pub research_job: Arc<PgResearchJobRepository>,
    pub reserved_capital: Arc<PgReservedCapitalRepository>,
    pub user: Arc<PgUserRepository>,
    pub role: Arc<PgRoleRepository>,
    pub menu: Arc<PgMenuRepository>,
    pub user_role: Arc<PgUserRoleRepository>,
    pub role_menu: Arc<PgRoleMenuRepository>,
    pub role_permission: Arc<PgRolePermissionRepository>,
    pub account_snapshot: Arc<PgAccountSnapshotRepository>,
}

impl PgRepositories {
    /// Construct every shared repository from the connected pool (boot-only).
    #[must_use]
    pub fn wire(pg: &PostgresPool) -> Self {
        let db = pg.connection().clone();
        Self {
            runtime_config: arc_repo(&db, PgRuntimeConfigVersionRepository::new),
            system_runtime_state: arc_repo(&db, PgSystemRuntimeStateRepository::new),
            kill_switch_state: arc_repo(&db, PgKillSwitchStateRepository::new),
            operation_log: arc_repo(&db, PgOperationLogRepository::new),
            market: arc_repo(&db, PgMarketRepository::new),
            event: arc_repo(&db, PgEventRepository::new),
            order_intent: arc_repo(&db, PgOrderIntentRepository::new),
            execution_submission: arc_repo(&db, PgExecutionSubmissionRepository::new),
            execution_order: arc_repo(&db, PgExecutionOrderRepository::new),
            reconciliation: arc_repo(&db, PgReconciliationRepository::new),
            position: arc_repo(&db, PgPositionRepository::new),
            capital_allocation: arc_repo(&db, PgCapitalAllocationRepository::new),
            settlement_redeem: arc_repo(&db, PgSettlementRedeemRepository::new),
            recommendation: arc_repo(&db, PgRecommendationRepository::new),
            recommendation_report: arc_repo(&db, PgRecommendationReportRepository::new),
            equity_snapshot: arc_repo(&db, PgEquitySnapshotRepository::new),
            attribution: arc_repo(&db, PgAttributionRepository::new),
            market_selection: arc_repo(&db, PgMarketSelectionRepository::new),
            feature: arc_repo(&db, PgFeatureRepository::new),
            factor: arc_repo(&db, PgFactorRepository::new),
            model_run: arc_repo(&db, PgModelRunRepository::new),
            model_registry: arc_repo(&db, PgModelRegistryRepository::new),
            shadow_comparison: arc_repo(&db, PgShadowComparisonRepository::new),
            training_dataset: arc_repo(&db, PgTrainingDatasetRepository::new),
            backtest_report: arc_repo(&db, PgBacktestReportRepository::new),
            comparison_report: arc_repo(&db, PgModelComparisonReportRepository::new),
            governance_audit: arc_repo(&db, PgModelGovernanceAuditRepository::new),
            research_job: arc_repo(&db, PgResearchJobRepository::new),
            reserved_capital: arc_repo(&db, PgReservedCapitalRepository::new),
            user: arc_repo(&db, PgUserRepository::new),
            role: arc_repo(&db, PgRoleRepository::new),
            menu: arc_repo(&db, PgMenuRepository::new),
            user_role: arc_repo(&db, PgUserRoleRepository::new),
            role_menu: arc_repo(&db, PgRoleMenuRepository::new),
            role_permission: arc_repo(&db, PgRolePermissionRepository::new),
            account_snapshot: arc_repo(&db, PgAccountSnapshotRepository::new),
        }
    }
}
