//! `PostgreSQL` repository implementations, grouped by bounded context.
//!
//! Every concrete `Pg*Repository` is re-exported flat under `postgres::` so
//! wiring code can name it without threading the context path.

// Crate-internal helpers.
pub mod arc_repo;
pub(crate) mod error;
pub(crate) mod primitives;
pub(crate) mod query;
pub(crate) mod state_hash;
pub(crate) mod write;

pub use arc_repo::arc_repo;

pub mod catalog;
pub mod governance;
pub mod quant;
pub mod rbac;

// Flattened facade.
pub use catalog::{
    PgCatalogLedgerRepository, PgClobMarketInfoRepository, PgEventRepository, PgEventRepositoryTxn,
    PgMarketRepository, PgMarketRepositoryTxn, clob_market_info, event, ingest, ledger, market,
};
pub use governance::{
    PgKillSwitchStateRepository, PgOperationLogRepository, PgPolicyRepository,
    PgSystemRuntimeStateRepository, SYSTEM_KILL_SWITCH_ID, kill_switch, operation_log,
    policy_bootstrap, runtime_config, system_runtime_state,
};
pub use quant::{
    PgAccountSnapshotRepository, PgAttributionRepository, PgBacktestPathSetRepository,
    PgBacktestReportRepository, PgBasisAlertRepository, PgCalibrationArtifactRepository,
    PgCapitalAllocationRepository, PgDomainProjectionRepository, PgDomainSourceCursorRepository,
    PgDomainSourceExpectationRepository, PgEntryConditionRepository, PgEquitySnapshotRepository,
    PgExecutionOrderRepository, PgExecutionSubmissionRepository, PgFactorRepository,
    PgFeatureParityRepository, PgFeatureRepository, PgMarketLinkageRepository,
    PgMarketSelectionRepository, PgModelComparisonReportRepository,
    PgModelGovernanceAuditRepository, PgModelRegistryRepository, PgModelRunRepository,
    PgOrderIntentRepository, PgPortfolioPlanRepository, PgPositionRepository,
    PgRecommendationReportRepository, PgRecommendationRepository, PgReconciliationRepository,
    PgReportRunRepository, PgResearchJobRepository, PgResearchReadinessEvidenceRepository,
    PgReservedCapitalRepository, PgSettlementRedeemRepository, PgShadowComparisonRepository,
    PgSourceSliceRepository, PgTradePolicyRepository, PgTradeTapeBlockCursorRepository,
    PgTrainingDatasetRepository,
};
pub use rbac::{
    PgCasbinAdapter, PgMenuRepository, PgRoleMenuRepository, PgRolePermissionRepository,
    PgRoleRepository, PgUserRepository, PgUserRoleRepository, casbin, junction, menu, role,
    role_menu, role_permission, user, user_role,
};
