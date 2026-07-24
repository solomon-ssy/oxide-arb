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
    PgOperationLogRepository, PgPolicyRepository, PgRuntimeControlRepository,
    RUNTIME_CONTROL_NOTIFY_CHANNEL, SYSTEM_RUNTIME_CONTROL_ID, operation_log, policy_bootstrap,
    runtime_config, runtime_control,
};
pub use quant::{
    PgAccountSnapshotRepository, PgBacktestPathSetRepository, PgBacktestReportRepository,
    PgBasisAlertRepository, PgCalibrationArtifactRepository, PgCapitalAllocationRepository,
    PgDomainProjectionRepository, PgDomainSourceCursorRepository,
    PgDomainSourceExpectationRepository, PgEntryConditionRepository, PgEquitySnapshotRepository,
    PgExecutionAccountRepository, PgExecutionOrderRepository, PgExecutionSubmissionRepository,
    PgFactorRepository, PgFeatureParityRepository, PgFeatureRepository, PgFeedbackCohortRepository,
    PgMarketLinkageRepository, PgMarketSelectionRepository, PgModelComparisonReportRepository,
    PgModelGovernanceAuditRepository, PgModelRegistryRepository, PgModelRunRepository,
    PgOrderIntentRepository, PgPortfolioPlanRepository, PgPositionRepository,
    PgRecommendationExecutionOutcomeRepository, PgRecommendationReportRepository,
    PgRecommendationRepository, PgRecommendationResolutionOutcomeRepository,
    PgReconciliationRepository, PgReportRunRepository, PgResearchJobRepository,
    PgResearchReadinessEvidenceRepository, PgReservedCapitalRepository,
    PgShadowComparisonRepository, PgSourceSliceRepository, PgTradePolicyRepository,
    PgTradeTapeBlockCursorRepository, PgTrainingDatasetRepository,
};
pub use rbac::{
    PgCasbinAdapter, PgMenuRepository, PgRoleMenuRepository, PgRolePermissionRepository,
    PgRoleRepository, PgUserRepository, PgUserRoleRepository, casbin, junction, menu, role,
    role_menu, role_permission, user, user_role,
};
