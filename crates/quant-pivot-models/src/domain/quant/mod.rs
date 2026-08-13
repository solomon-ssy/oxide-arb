//! Quant-pivot persistence DTOs for schema-first repositories.

mod account;
#[allow(clippy::needless_update)] // Insert DTO omits DB-managed availability timestamps.
mod attribution;
#[allow(clippy::needless_update)] // NewBacktestReport omits DB-managed created_at
mod backtest;
#[allow(clippy::needless_update)] // NewBacktestPathSet omits DB-managed created_at
mod backtest_path_set;
#[allow(clippy::needless_update)] // NewBasisAlert omits DB-managed created_at
mod basis_alert;
#[allow(clippy::needless_update)] // NewCalibrationArtifact omits DB-managed created_at
mod calibration_artifact;
mod candidate;
mod capital;
#[allow(clippy::needless_update)] // NewModelComparisonReport omits DB-managed created_at
mod comparison;
#[allow(clippy::needless_update)] // NewTrainingDatasetPlan omits materialization/timestamps
mod dataset;
mod economic_tier;
#[allow(clippy::needless_update)] // Insert DTOs omit DB-managed timestamps.
mod entry_condition;
mod execution;
mod execution_account;
mod execution_attempt_outcome;
mod exit_training;
mod factor;
mod feature;
#[allow(clippy::needless_update)] // Insert DTOs omit database-managed timestamps.
mod feature_parity;
mod feedback_cohort;
#[allow(clippy::needless_update)] // Insert DTO omits DB-managed created_at.
mod feedback_coordinator_fault;
#[allow(clippy::needless_update)] // New feedback DTOs omit DB-managed lifecycle/timestamps.
mod feedback_cycle;
#[allow(clippy::needless_update)] // Scheduler sync payload omits DB-managed lifecycle/timestamps.
mod feedback_scheduler;
#[allow(clippy::needless_update)] // Trigger inserts omit DB-managed timestamps.
mod feedback_trigger;
mod global_portfolio_plan;
#[allow(clippy::needless_update)] // NewModelGovernanceAudit omits DB-managed created_at
mod governance_audit;
#[allow(clippy::needless_update)] // NewMarketLinkage omits DB-managed created_at
mod linkage;
#[allow(clippy::needless_update)] // NewModelRun omits DB-managed lifecycle columns
mod model;
#[allow(clippy::needless_update)] // Insert DTO omits DB-managed created_at.
mod model_candidate_manifest;
mod model_route_bootstrap;
mod outcome_reconciliation;
mod portfolio;
mod portfolio_scenario;
mod position;
mod promotion_permit;
mod recommendation;
mod recommendation_execution_rollup;
#[allow(clippy::needless_update)] // Insert DTO omits DB-managed created_at.
mod recommendation_resolution_outcome;
mod reconciliation;
mod report_data_quality;
mod report_diff;
#[allow(clippy::needless_update)] // Insert DTO omits delivery-managed timestamps and lease fields.
mod report_fact_delivery;
mod report_route_run;
#[allow(clippy::needless_update)] // Insert DTO intentionally contains queued-run fields only.
mod report_run;
mod report_txn;
mod represented_route;
#[allow(clippy::needless_update)] // NewResearchJob omits DB-managed timestamps
mod research_job;
#[allow(clippy::needless_update)] // Insert DTO omits DB-managed created_at.
mod research_readiness;
mod resolution_observation;
#[allow(clippy::needless_update)] // NewMarketSelectionMember covers all ActiveModel columns
mod selection;
#[allow(clippy::needless_update)] // Child settlement inserts omit DB-managed timestamps.
pub mod settlement;
pub mod settlement_governance;
#[allow(clippy::needless_update)] // NewSettlementInventoryLot omits DB-managed created_at
pub mod settlement_inventory;
pub mod settlement_readiness;
#[allow(clippy::needless_update)] // NewShadowComparison omits DB-managed created_at
mod shadow;
#[allow(clippy::needless_update)] // NewSourceSlice omits DB-managed timestamps
mod source_slice;
#[allow(clippy::needless_update)] // Insert DTOs omit DB-managed timestamps.
mod trade_policy;
mod trade_policy_trial;

pub use account::{
    AccountSnapshotInfo, EquitySnapshotInfo, EquitySnapshotQuery, LiveAccountSnapshot,
    NewAccountSnapshot, NewEquitySnapshot,
};
pub use attribution::{
    AttributionArtifactContractError, AttributionArtifactInfo, AttributionSubject,
    NewAttributionArtifact,
};
pub use backtest::{BacktestReportInfo, NewBacktestReport};
pub use backtest_path_set::{
    BacktestPathSetError, BacktestPathSetInfo, NewBacktestPathSet, NewBacktestPathSetInput,
};
pub use basis_alert::{BasisAlertInfo, NewBasisAlert};
pub use calibration_artifact::{
    CalibrationArtifactInfo, CalibrationArtifactPayload, ModelScoreCalibrationCommit,
    ModelScoreCalibrationCommitOutcome, NewCalibrationArtifact,
};
pub use candidate::{DomainAvailability, MarketCandidate, MarketDataHealth};
pub use capital::{CapitalAllocationInfo, CapitalAllocationPatch, NewCapitalAllocation};
pub use comparison::{ModelComparisonReportInfo, NewModelComparisonReport};
pub use dataset::{
    CompleteTrainingDatasetBuild, NewTrainingDatasetPlan, TrainingDatasetInfo,
    TrainingDatasetMaterialization,
};
pub use economic_tier::{
    CapitalOccupancyBucket, EntryEconomics, ExecutableEconomicTier, RecommendationEconomics,
    ScenarioCashflow,
};
pub use entry_condition::{
    ApplyEntryConditionEvaluation, ApplyEntryConditionEvaluationOutcome, CryptoPriceProjectionInfo,
    EntryConditionArtifactInfo, EntryConditionAuditInfo, EntryConditionClaim,
    EntryConditionInstanceInfo, NewEntryConditionArtifact, NewEntryConditionAudit,
    NewEntryConditionInstance, WeatherDailyTemperatureProjectionInfo,
};
pub use execution::{
    ApproveOrderIntent, ApproveOrderIntentOutcome, CapitalSettlement, ExecutionIdentityEnrichment,
    ExecutionIdentityRefs, ExecutionOrderIdentityRefs, ExecutionOrderInfo, ExecutionOrderPatch,
    ExecutionTradeObservation, ExecutionTradeRef, ExecutionTransactionRef, ExitLedgerWrite,
    NewExecutionOrder, NewExecutionTradeRef, NewExecutionTransactionRef, NewOrderIntent,
    OrderIntentInfo, SubmissionLedgerWrite,
};
pub use execution_account::{ExecutionAccountInfo, NewExecutionAccount};
pub use execution_attempt_outcome::{
    ExecutionAttemptOutcomeContractError, ExecutionAttemptOutcomeInfo, NewExecutionAttemptOutcome,
};
pub use exit_training::{ExitTrainingLotRow, LotExitEventRow};
pub use factor::{
    FactorDefinitionInfo, FactorDefinitionProjectionError, FactorRegistrationOutcome,
    FactorValueInfo, FactorValueProjectionError, LatestFactorSnapshotBundleInfo,
    LatestFactorSnapshotInfo, LatestFactorSnapshotValueInfo, NewFactorDefinition, NewFactorValue,
};
pub use feature::{FeatureVectorInfo, FeatureVectorModel, NewFeatureVector};
pub use feature_parity::{
    CompleteFeatureParityRun, FeatureParityRunInfo, FeatureParityStateInfo,
    FrozenFeatureParityCandidate, FrozenFeatureParitySubject, FrozenFeatureParitySubjectId,
    ModelRunParityEvidence, ModelVersionParityEvidence, NewFeatureParityRun, NewFeatureParityState,
    NewFrozenModelParitySubject, parity_candidate_membership_hash, parity_selection_hash,
    report_parity_evidence_hash, report_parity_generation_hash,
};
pub use feedback_cohort::{
    FEEDBACK_COHORT_PAGE_LIMIT, FeedbackCohortCandidate, FeedbackCohortContractError,
    FeedbackCohortCursor, FeedbackCohortDecision, FeedbackCohortEvidence, FeedbackCohortPage,
    FeedbackCohortPageQuery, FeedbackCohortSnapshot, FeedbackCohortWindow,
    FeedbackExecutionEvidence, FeedbackExecutionState, FeedbackRecommendationContext,
    FeedbackResolutionEvidence,
};
pub use feedback_coordinator_fault::{
    FeedbackCoordinatorFaultInfo, FeedbackCoordinatorFaultInput, FeedbackCoordinatorFaultReason,
    FeedbackCoordinatorTimelineHead, NewFeedbackCoordinatorFault,
};
pub use feedback_cycle::{
    DriftReportInfo, DriftReportInput, FeedbackCycleActor, FeedbackCycleInfo, FeedbackCycleKey,
    FeedbackCycleKeyInput, FeedbackCycleTerminal, FeedbackEvaluationUseInfo,
    FeedbackEvaluationUseInput, FeedbackEvaluationUseKey, FeedbackOutboxEntry,
    FeedbackOutboxSource, FeedbackQueueSnapshot, FeedbackStageEventInfo, FeedbackStageEventInput,
    GovernedFeedbackCancellation, GovernedFeedbackTrigger, NewDriftReport, NewFeedbackCycle,
    NewFeedbackEvaluationUse, NewFeedbackStageEvent,
};
pub use feedback_scheduler::{
    FeedbackSchedulerClaim, FeedbackSchedulerControl, FeedbackSchedulerLease,
    FeedbackSchedulerRetry, FeedbackSchedulerStateInfo, FeedbackSchedulerSuccess,
    NewFeedbackSchedulerState, cadence_cutoff, next_cadence_after,
};
pub use feedback_trigger::{
    FeedbackTriggerEventInfo, FeedbackTriggerEventInput, NewFeedbackTriggerEvent,
};
pub use global_portfolio_plan::{
    ExactVerificationEvidence, ExistingPortfolioState, GlobalPortfolioPlan,
    PortfolioConstraintEvidence, PortfolioDecisionResult, PortfolioObjectiveEvidence,
    SolverEvidence,
};
pub use governance_audit::{
    ModelGovernanceAuditDetail, ModelGovernanceAuditInfo, NewModelGovernanceAudit,
    NewRoutePromotionAudit,
};
pub use linkage::{
    CryptoSubject, GlobalTemperatureOutcome, GlobalTemperatureRank, GroundingField, GroundingKind,
    GroundingProof, GroundingSpan, LinkageOutcome, LinkageSourceMetadata, LinkageUnresolvedReason,
    LinkageValidationFailure, ManualEvidenceInput, MarketLinkage, MarketLinkageDerivation,
    MarketLinkageInfo, MarketSubject, NewMarketLinkage, OverrideContext, PriceBoundaryInclusion,
    PriceComparator, ResolutionOracle, ResolvedBinding, ResolvedSourceBinding, SeaIceAggregation,
    SeaIceHemisphere, SeaIceProduct, TropicalCycloneOutcome, WeatherAqiAggregation,
    WeatherAqiPollutant, WeatherAqiSubject, WeatherContractWindow, WeatherDecisionGroupKey,
    WeatherGlobalTemperatureSubject, WeatherPrecipitationSubject, WeatherRoundingRule,
    WeatherSeaIceSubject, WeatherSubject, WeatherTornadoFinalization, WeatherTornadoSubject,
    WeatherTropicalCycloneSubject, WeatherTruthPolicy, WeatherValueComparator,
    WeatherWindExtremeSubject, WeatherWindStatistic,
};
pub use model::{
    ModelCatalogInfo, ModelRunInfo, ModelSpecInfo, ModelVersionInfo, ModelVersionPersistenceError,
    NewModelRun, NewModelSpec, NewModelVersion, QuantModelRunModel,
};
pub use model_candidate_manifest::{
    CandidateExplanationMethod, CandidateExplanationValidation, CandidateExplanationVerification,
    ModelCandidateManifestDocument, ModelCandidateManifestError, ModelCandidateManifestInfo,
    ModelCandidateManifestInput, NewModelCandidateManifest, PromotionGateArtifact,
    PromotionGateArtifactInput, scenario_model_bindings_hash,
};
pub use model_route_bootstrap::{
    BootstrapModelRoute, CommitModelRouteBootstrap, ModelBootstrapManifest,
    ModelBootstrapManifestInput, ModelBootstrapPolicyProjection, ModelRouteBootstrapPolicy,
    ModelRouteBootstrapPreflight, ModelRouteBootstrapPreflightInput, ModelRouteBootstrapRecord,
    ModelRouteBootstrapRecordInput, ModelRouteBootstrapRoute,
};
pub use outcome_reconciliation::{
    ExecutionAttemptBarrier, ExecutionAttemptDeferredReason, ExecutionAttemptDerivation,
    ExecutionAttemptReconciliationCandidate, ExecutionAttemptReconciliationError,
    ExecutionAttemptReconciliationResult, ExecutionAttemptSourceGraph, ExecutionAttemptTaskClaim,
    ExecutionRollupTaskClaim, OutcomeTaskSettlement,
    RecommendationResolutionReconciliationCandidate, ResolutionOutcomeDeferredReason,
    ResolutionOutcomeReconciliationResult, ResolutionOutcomeTaskClaim,
};
pub use portfolio::{NewPortfolioPlan, PortfolioPlanInfo};
pub use portfolio_scenario::{
    DiscountCurvePoint, PortfolioScenario, PortfolioScenarioArtifact, PortfolioScenarioFitEvidence,
    PortfolioScenarioKind, PortfolioScenarioModelArtifact, PortfolioScenarioModelState,
    PortfolioScenarioResamplingMethod, PortfolioScenarioRouteFactor,
    PortfolioScenarioRouteFitLineage, PortfolioScenarioRouteModelLineage,
    PortfolioScenarioVisibility, ScenarioDistribution, ScenarioMarketOutcome, ScenarioPayoutState,
    ScenarioWeight, StructuralExclusivityGroup, StructuralOutcomeRef,
};
pub use position::{NewPosition, PositionExit, PositionFill, PositionInfo};
pub use promotion_permit::{
    CommitModelRoutePromotion, IssuePromotionPermit, ModelRoutePromotionPolicy,
    ModelRoutePromotionRecord, ModelRoutePromotionRecordInput, ModelRoutePromotionRoute,
    NewPromotionPermit, PromoteModelRoute, PromotionPermitActor, PromotionPermitInfo,
    PromotionPermitIssueInput, PromotionPermitRevocation, PromotionPermitRevocationCheck,
    PromotionPermitScope, PromotionPermitScopeInput, PromotionPermitStatus,
    PromotionPolicyProjection, PromotionPreflight, PromotionPreflightInput,
    PromotionServingConstraints, PromotionServingConstraintsInput, RevokePromotionPermit,
};
pub use recommendation::{
    NewRecommendation, NewRecommendationReport, RecommendationInfo, RecommendationReportInfo,
};
pub use recommendation_execution_rollup::{
    ExecutionRollupBarrier, ExecutionRollupDeferredReason, ExecutionRollupReconciliationResult,
    NewRecommendationExecutionRollup, NewRecommendationExecutionRollupAttempt,
    RecommendationExecutionRollupAttemptInfo, RecommendationExecutionRollupContractError,
    RecommendationExecutionRollupInfo, RecommendationExecutionRollupSeal,
};
pub use recommendation_resolution_outcome::{
    InsertResolutionOutcomeResult, NewRecommendationResolutionOutcome,
    RECOMMENDATION_RESOLUTION_OUTCOME_PAGE_LIMIT, RecommendationResolutionOutcomeContractError,
    RecommendationResolutionOutcomeCursor, RecommendationResolutionOutcomeInfo,
    RecommendationResolutionOutcomePage, RecommendationResolutionOutcomePageQuery,
    RecommendationResolutionOutcomePageQueryError,
};
pub use reconciliation::{
    AppendReconciliationEvidence, CapitalReconcileSettlement, NewReconciliation,
    ReconciliationInfo, ReconciliationLedgerWrite, ReconciliationPatch,
};
pub use report_data_quality::{NewReportDataQualitySnapshot, ReportDataQualitySnapshotInfo};
pub use report_diff::{
    EligibilityShift, RecommendationChangedField, RecommendationDelta, RecommendationDiffSnapshot,
    ReportDiff,
};
pub use report_fact_delivery::{NewReportFactDelivery, ReportFactDeliveryInfo};
pub use report_route_run::{
    NewReportRouteRun, ReportRouteRun, ReportRouteRunInfo, RouteCandidateFunnel, RouteLineageView,
    RouteModelLineage, RouteRunOutcome,
};
pub use report_run::{
    ClaimReportSchedule, EnqueueReportRunOutcome, MaterializeReportSchedule,
    MaterializeReportScheduleOutcome, NewReportRun, ReconcileReportSchedule,
    ReconcileReportSchedulesOutcome, ReportCurrentHealthInfo, ReportRunClaim, ReportRunClaimConfig,
    ReportRunInfo, ReportScheduleGapInfo, ReportScheduleHealthInfo, ReportScheduleStateInfo,
};
pub use report_txn::{
    CreatePreparedReport, FactDeliverySettlement, NewReportFeatureParity, NewReportTransaction,
    PreparedReportOutcome, PublishReportOutcome,
};
pub use represented_route::{
    RepresentedRouteSet, RouteCompatibilityDigests, RouteCompatibilityError, RouteContractHash,
};
pub use research_job::{
    FeedbackStageJobIdentity, JobProgressSink, NewResearchJob, NoopProgressSink,
    ResearchJobArtifactRef, ResearchJobFinalization, ResearchJobInfo, ResearchJobResultRef,
};
pub use research_readiness::{NewResearchReadinessEvidence, ResearchReadinessEvidenceInfo};
pub use resolution_observation::{
    NewResolutionObservationInbox, RemediateResolutionProjection,
    ResolutionObservationContractError, ResolutionObservationInboxInfo,
    ResolutionObservationProjectionInfo, ResolutionProjectionAttentionItem,
    ResolutionProjectionBarrier, ResolutionProjectionClaim, ResolutionProjectionRemediationInfo,
    ResolutionProjectionSettlement, ResolutionRemediationCommit, ResolutionScanCommitOutcome,
};
pub use selection::{
    MarketSelectionInfo, MarketSelectionMemberInfo, MarketSelectionModel, NewMarketSelection,
    NewMarketSelectionMember,
};
pub use shadow::{
    NewShadowComparison, ShadowComparisonInfo, ShadowObservationQuery, ShadowObservationWindow,
    ShadowStabilitySummary,
};
pub use source_slice::{
    BeginSourceSliceOutcome, CompleteSourceSlice, NewSourceSlice, SourceSliceIdentity,
    SourceSliceIdentityInput, SourceSliceInfo,
};
pub use trade_policy::{
    CompleteTradePolicyValidation, FailTradePolicyValidation, NewTradePolicyArtifact,
    NewTradePolicyGovernanceAudit, NewTradePolicyValidationRow, NewTradePolicyValidationRun,
    TradePolicyArtifactInfo, TradePolicyGovernanceAuditInfo, TradePolicyValidationRowInfo,
    TradePolicyValidationRunInfo,
};
pub use trade_policy_trial::{NewTradePolicyTrialAttempt, TradePolicyTrialAttemptInfo};
