//! `PostgreSQL` repository implementations, grouped by bounded context.
//!
//! Every concrete `Pg*Repository` is re-exported flat under `postgres::` so
//! wiring code can name it without threading the context path.

// Crate-internal helpers.
pub(crate) mod authorization;
pub(crate) mod connection;
pub(crate) mod error;
pub(crate) mod primitives;
pub(crate) mod query;
pub(crate) mod state_hash;
pub(crate) mod write;

pub mod catalog;
pub mod governance;
pub mod quant;
pub mod rbac;

// Flattened facade.
pub use catalog::{
    PgCatalogLedgerRepository, PgClobMarketInfoRepository, PgEventRepository, PgMarketRepository,
    clob_market_info, event, ingest, ledger, market,
};
pub use governance::{
    PgOperationLogRepository, PgPolicyRepository, PgRuntimeControlRepository,
    RUNTIME_CONTROL_NOTIFY_CHANNEL, SYSTEM_RUNTIME_CONTROL_ID, operation_log, policy_bootstrap,
    runtime_config, runtime_control,
};
pub use quant::{
    PgAccountSnapshotRepository, PgAttributionArtifactRepository, PgBacktestPathSetRepository,
    PgBacktestReportRepository, PgBasisAlertRepository, PgCalibrationArtifactRepository,
    PgCapitalAllocationRepository, PgDomainProjectionRepository, PgDomainSourceCursorRepository,
    PgDomainSourceExpectationRepository, PgEntryConditionRepository, PgEquitySnapshotRepository,
    PgExchangeHistoryRepository, PgExecutionAccountRepository, PgExecutionAttemptOutcomeRepository,
    PgExecutionOrderRepository, PgExecutionSubmissionRepository, PgFactorRepository,
    PgFeatureParityRepository, PgFeatureRepository, PgFeedbackCohortRepository,
    PgFeedbackCycleRepository, PgFeedbackRecipeTemplateRepository, PgFeedbackSchedulerRepository,
    PgFreshBootRepository, PgMarketLinkageRepository, PgMarketSelectionRepository,
    PgModelCandidateManifestRepository, PgModelComparisonReportRepository,
    PgModelGovernanceAuditRepository, PgModelRegistryRepository, PgModelRouteBootstrapRepository,
    PgModelRoutePromotionRepository, PgModelRouteShadowBindingRepository, PgModelRunRepository,
    PgOrderIntentRepository, PgPortfolioPlanRepository, PgPositionRepository,
    PgPromotionPermitRepository, PgRecommendationExecutionRollupRepository,
    PgRecommendationReportRepository, PgRecommendationRepository,
    PgRecommendationResolutionOutcomeRepository, PgReconciliationRepository, PgReportRunRepository,
    PgResearchJobRepository, PgResearchReadinessEvidenceRepository, PgReservedCapitalRepository,
    PgResolutionObservationRepository, PgRuntimeActivityRepository, PgShadowComparisonRepository,
    PgSourceSliceRepository, PgTradePolicyRepository, PgTrainingDatasetRepository,
    PgVenueIncentiveRepository,
};
pub use rbac::{
    PgCasbinAdapter, PgMenuRepository, PgRoleMenuRepository, PgRolePermissionRepository,
    PgRoleRepository, PgUserRepository, PgUserRoleRepository, casbin, junction, menu, role,
    role_menu, role_permission, user, user_role,
};
