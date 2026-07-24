//! Repository trait definitions, grouped by bounded context.
//!
//! Every trait is also re-exported flat under `traits::` so callers can depend
//! on `traits::MarketRepository` without threading the context path.

pub mod governance;
pub mod quant;
pub mod rbac;

// Single-trait contexts kept flat.
pub mod market;

// Flattened facade.
pub use governance::{
    EventRepository, OperationLogRepository, PolicyRepository, RuntimeControlRepository, event,
    operation_log, runtime_config, runtime_control,
};
pub use market::{CatalogLedgerRepository, ClobMarketInfoRepository, MarketRepository};
pub use quant::{
    AccountSnapshotRepository, BacktestPathSetRepository, BacktestReportRepository,
    BasisAlertRepository, CalibrationArtifactRepository, CapitalAllocationRepository,
    DomainProjectionRepository, DomainSourceCursorRepository, DomainSourceExpectationRepository,
    EnqueueFrozenFeatureParityOutcome, EntryConditionRepository, EquitySnapshotRepository,
    ExecutionAccountRepository, ExecutionOrderRepository, ExecutionSubmissionRepository,
    FactWriter, FactorRepository, FeatureParityEventRepository, FeatureParityLatchActor,
    FeatureParityRepository, FeatureRepository, FeedbackCohortRepository, KindRunningCount,
    MarketLinkageRepository, MarketSelectionRepository, ModelComparisonReportRepository,
    ModelGovernanceAuditRepository, ModelRegistryRepository, ModelRunRepository,
    OrderIntentRepository, PortfolioPlanRepository, PositionRepository, PublishFeatureParityPermit,
    PublishModelVersionCommit, PublishModelVersionResult, QuantFactReadRepository,
    QuantFactRepository, ReclaimOutcome, RecommendationExecutionOutcomeRepository,
    RecommendationReportRepository, RecommendationRepository,
    RecommendationResolutionOutcomeRepository, ReconciliationRepository, ReportRunRepository,
    ResearchJobRepository, ResearchReadinessEvidenceRepository, ReservedCapitalRepository,
    ServingEvidenceRepository, ShadowComparisonRepository, ShadowLatencyObservation,
    SourceSliceRepository, TradePolicyRepository, TradeTapeBlockCursorRepository,
    TrainingDatasetRepository,
};
pub use rbac::{
    MenuRepository, RoleMenuRepository, RolePermissionRepository, RoleRepository, UserRepository,
    UserRoleRepository, menu, role, role_menu, role_permission, user, user_role,
};
