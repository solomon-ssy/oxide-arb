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
    AccountSnapshotRepository, AttributionArtifactRepository, AttributionArtifactWriteOutcome,
    BacktestPathSetRepository, BacktestReportRepository, BasisAlertRepository,
    CalibrationArtifactRepository, CapitalAllocationRepository, CpcvPathSetCommit,
    DomainProjectionRepository, DomainSourceCursorRepository, DomainSourceExpectationRepository,
    DriftReportWriteOutcome, EnqueueFrozenFeatureParityOutcome, EntryConditionRepository,
    EquitySnapshotRepository, ExchangeHistoryRepository, ExecutionAccountRepository,
    ExecutionAttemptOutcomeRepository, ExecutionOrderRepository, ExecutionSubmissionRepository,
    FactWriter, FactorRepository, FeatureParityEventRepository, FeatureParityLatchActor,
    FeatureParityRepository, FeatureRepository, FeedbackCohortRepository,
    FeedbackCoordinatorFaultWriteOutcome, FeedbackCoordinatorQuarantine, FeedbackCycleCasOutcome,
    FeedbackCycleClaim, FeedbackCycleClaimMode, FeedbackCycleGeneration, FeedbackCycleLeaseGuard,
    FeedbackCycleRepository, FeedbackCycleWriteOutcome, FeedbackEvaluationWriteOutcome,
    FeedbackOutboxRepository, FeedbackRecipeTemplateRepository, FeedbackRecipeTemplateWriteOutcome,
    FeedbackSchedulerRepository, FeedbackStageWriteOutcome, FeedbackTriggerCommit,
    FeedbackTriggerWriteOutcome, FreshBootRepository, KindRunningCount, MarketLinkageRepository,
    MarketSelectionRepository, ModelCandidateManifestRepository,
    ModelCandidateManifestWriteOutcome, ModelComparisonReportRepository,
    ModelGovernanceAuditRepository, ModelRegistryRepository, ModelRouteBootstrapCommit,
    ModelRouteBootstrapOutcome, ModelRouteBootstrapRepository, ModelRoutePromotionCommit,
    ModelRoutePromotionOutcome, ModelRoutePromotionRepository, ModelRouteShadowBindingRepository,
    ModelRunRepository, OrderIntentRepository, PortfolioPlanRepository, PositionRepository,
    PromotionPermitIssueOutcome, PromotionPermitPage, PromotionPermitRepository,
    PromotionPermitRevokeOutcome, QuantFactReadRepository, QuantFactRepository, ReclaimOutcome,
    RecommendationExecutionRollupRepository, RecommendationReportRepository,
    RecommendationRepository, RecommendationResolutionOutcomeRepository, ReconciliationRepository,
    ReportRunRepository, ResearchJobEnqueueOutcome, ResearchJobRepository, ResearchJobRetryOutcome,
    ResearchReadinessEvidenceRepository, ReservedCapitalRepository,
    ResolutionObservationRepository, RuntimeActivityRepository, ServingEvidenceRepository,
    ShadowBindingCancelCommit, ShadowBindingCancelOutcome, ShadowBindingCommit,
    ShadowBindingCommitOutcome, ShadowBindingRejectCommit, ShadowBindingRejectOutcome,
    ShadowComparisonRepository, ShadowComparisonWriteOutcome, ShadowLatencyObservation,
    SourceSliceRepository, TradePolicyRepository, TrainingDatasetRepository,
    VenueIncentiveRepository,
};
pub use rbac::{
    MenuRepository, RoleMenuRepository, RolePermissionRepository, RoleRepository, UserRepository,
    UserRoleRepository, menu, role, role_menu, role_permission, user, user_role,
};
