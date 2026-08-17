//! HTTP API contract types — control plane subset.

mod auth;
mod backtest_path_set;
mod backtest_report;
mod calibration_artifact;
mod comparison_report;
pub mod dashboard;
mod decision_evidence;
mod execution_recovery;
mod factor_catalog;
mod feature_contract;
mod feature_integrity;
mod feedback;
mod health;
mod market;
mod market_linkage;
mod menu;
mod model_training;
mod operation_log;
pub mod operator_contract;
mod permission;
mod quality_gate;
mod quant_account;
mod quant_execution;
pub mod quant_incentive;
mod quant_model;
mod quant_recommendation;
mod quant_report;
mod reconciliation;
mod research_job;
mod research_model_contract;
mod role;
mod runtime_activity;
mod runtime_config;
pub mod settlement_redeem;
mod structural_monitor;
pub mod system;
mod trade_policy;
mod training_dataset;
mod user;
mod validation;
mod window;

pub use auth::{LoginRequest, MeResponse, RoleView, TokenResponse, UserView};
pub use backtest_path_set::{
    BacktestPathSetListQuery, BacktestPathSetView, RunCpcvBacktestRequest,
};
pub use backtest_report::{BacktestReportListQuery, BacktestReportView, RunBacktestRequest};
pub use calibration_artifact::{
    ActivateCalibrationArtifactRequest, CalibrationArtifactDetailView,
    CalibrationArtifactListQuery, CalibrationArtifactSummaryView, FitBiasTableRequest,
    FitModelCalibratorRequest, ModelCalibrationFitPreflightQuery, ModelCalibrationFitPreflightView,
};
pub use comparison_report::{ComparisonReportListQuery, ModelComparisonReportView};
pub use decision_evidence::{
    DecisionBoundaryEvidenceView, FeatureCellEvidenceView, ModelInputEvidenceView,
    ModelRouteEvidenceView,
};
pub use execution_recovery::{
    ExecutionRecoveryStep, ExecutionRecoverySummary, ExecutionRecoveryView,
};
pub use factor_catalog::{
    CollinearPairView, FactorCollinearityQuery, FactorCollinearitySource, FactorCollinearityView,
    FactorDefinitionDetailQuery, FactorDefinitionDetailView, FactorDefinitionListQuery,
    FactorDefinitionView, FactorServingUsageView,
};
pub use feature_contract::{FeatureContractEntryView, FeatureContractView, FeatureNullPolicyView};
pub use feature_integrity::{
    AcknowledgeFeatureParityLatchRequest, FeatureIntegrityCounts, FeatureIntegrityLatchView,
    FeatureIntegritySummaryView, FeatureParityEventListQuery, FeatureParityEventView,
    FeatureParityEvidenceView, FeatureParityRunListQuery, FeatureParityRunView,
    RunFullFeatureParityRequest,
};
pub use feedback::{
    ActivateModelRouteRequest, BootstrapModelRouteRequest, CancelFeedbackCycleRequest,
    DriftReportListQuery, DriftReportView, FeedbackAttributionSummaryView,
    FeedbackCandidateComparisonView, FeedbackCandidateReadyView, FeedbackCandidateShadowView,
    FeedbackCohortCountsView, FeedbackCoverageDecision, FeedbackCoverageView,
    FeedbackCycleDetailView, FeedbackCycleListQuery, FeedbackCycleMutationView,
    FeedbackCycleTriggerRequest, FeedbackCycleTriggerView, FeedbackCycleView,
    FeedbackEvaluationUseView, FeedbackOverviewView, FeedbackProfileOverviewView,
    FeedbackQueueView, FeedbackReadinessView, FeedbackRouteDiffView,
    FeedbackSchedulerControlRequest, FeedbackSchedulerListView, FeedbackSchedulerMutationView,
    FeedbackSchedulerStateView, FeedbackStageEventView, FeedbackTriggerEventView,
    FeedbackTruthOperationsView, IssuePromotionPermitRequest, ModelRouteActivationMutationView,
    ModelRouteActivationReceiptView, ModelRouteBootstrapReceiptView, ModelRouteRollbackTargetView,
    PromotionPermitListQuery, PromotionPermitMutationView, PromotionPermitView,
    RejectShadowBindingRequest, RemediateResolutionProjectionRequest,
    ResolutionProjectionRemediationView, RevokePromotionPermitRequest,
    ShadowBindingRejectionReceiptView,
};
pub use health::{DependencyCheck, HealthStatus, ReadinessReport, ReadinessStatus};
pub use market::{
    BlockMarketRequest, BookLevelView, MarketBookSideView, MarketBookSummaryView, MarketBookView,
    MarketMicrostructureQuery, MarketMicrostructureView, MarketPageQuery, MarketTradeTick,
    MarketView, MicrostructureBucket, MicrostructureResolution, ResolvedMicrostructureWindow,
    UnblockMarketRequest,
};
pub use market_linkage::{
    AcknowledgeBasisAlertRequest, BasisAlertListQuery, BasisAlertView, DomainSourceExpectationView,
    DomainSourceFamilySummary, DomainSourceSnapshotStatus, DomainSourcesSnapshot,
    LinkageResolveSummaryView, MarketLinkageDetailView, MarketLinkageHistoryEntryView,
    MarketLinkageListQuery, MarketLinkageSummaryView, OverrideLinkageRequest,
    OverrideSourceBindingInput, ResolveLinkagesRequest,
};
pub use menu::{CreateMenuRequest, UpdateMenuRequest};
pub use model_training::{
    ModelDetailQuery, ModelDetailView, ModelPromotionLineageView, ModelPromotionRole,
    ModelVersionListQuery, TrainModelRequest, TrainedModelView,
};
pub use operation_log::{OperationLogQuery, OperationLogView};
pub use permission::PermissionCatalogEntry;
pub use quality_gate::{
    GateOutcomeView, GatePreviewIntent, QualityGatePreviewQuery, QualityGateReportView,
};
pub use quant_account::{
    AccountSnapshotView, EquitySnapshotView, LiveAccountView, VenuePositionSnapshotView,
};
pub use quant_execution::{
    ApproveIntentRequest, CancelIntentRequest, CreateIntentRequest, EntryConditionArtifactView,
    EntryConditionAuditView, EntryConditionDetailView, EntryConditionEvaluationView,
    EntryConditionInstanceSummaryView, EntryConditionLeafEvidenceView,
    EntryConditionSourceCheckpointView, ExecutionOrderListQuery, ExecutionOrderView,
    ExitMonitorObservationView, OrderIntentListQuery, OrderIntentView, PositionDetailView,
    PositionListQuery, PositionSummary, PositionView, RejectIntentRequest,
};
pub use quant_model::{
    CreateModelSpecRequest, ModelPickerSide, ModelRouteCandidateQuery, ModelRouteCandidateView,
    ModelSpecListQuery, QuantModelSpecView,
};
pub use quant_recommendation::{
    QuantEvidenceView, QuantRecommendationView, RecommendationViewContext,
};
pub use quant_report::{
    CurrentReportQuery, QuantReportDetailView, QuantReportDiagnosticsView, QuantReportFunnelView,
    QuantReportListQuery, QuantReportView, RecommendationChangedFieldView, RecommendationDeltaView,
    RecommendationDiffSnapshotView, ReportCurrentHealthView, ReportDiffView,
    ReportEvidenceDiagnosticsView, ReportFactDeliveryView, ReportFunnelMarketListQuery,
    ReportFunnelMarketView, ReportFunnelStageView, ReportRouteDiagnosticsView, ReportRunListQuery,
    ReportRunView, ReportScheduleGapListQuery, ReportScheduleGapView, ReportScheduleHealthView,
    ReportScheduleStateView, ReportTimelineQuery, RetryReportRequest, RevokeReportRequest,
    RunReportRequest,
};
pub use reconciliation::{
    ReconciliationListQuery, ReconciliationView, ResolveReconciliationCommand,
    ResolveReconciliationOutcome, ResolveReconciliationRequest, ResolveReconciliationResponse,
};
pub use research_job::{
    BacktestJobParams, CancelResearchJobRequest, CpcvBacktestJobParams, FeatureParityJobParams,
    FeedbackCoverageJobParams, FeedbackDriftJobParams, ModelTrainJobParams, ResearchJobListQuery,
    ResearchJobView, RetryResearchJobRequest, TradePolicyFitJobParams,
    TradePolicyValidationJobParams,
};
pub use research_model_contract::ResearchModelApiContractSchema;
pub use role::{
    AssignMenusRequest, AssignPermissionsRequest, ChangeRoleStatusRequest, CreateRoleRequest,
    UpdateRoleRequest,
};
pub use runtime_activity::{
    RuntimeActivityActionView, RuntimeActivityCursor, RuntimeActivityDomainCountView,
    RuntimeActivityEntityView, RuntimeActivityIndicatorView, RuntimeActivityListQuery,
    RuntimeActivityPageView, RuntimeActivityReadQuery, RuntimeActivitySummaryView,
    RuntimeActivityView,
};
pub use runtime_config::{
    ActivatePolicyDraftRequest, ApprovePolicyDraftRequest, ConfigActivityQuery, ConfigActivityView,
    ConfigApiContractSchema, ConfigResourceSummaryView, ConfigResourcesView,
    ConfigSnapshotOptionsQuery, CreatePolicyDraftRequest, CurrentPolicyResourceView,
    DecisionPolicySnapshotOptionView, DeploymentConfigView, PolicyActivationResultView,
    PolicyActivationView, PolicyActorView, PolicyApprovalView, PolicyResourceSchemaView,
    PolicyRevisionListQuery, PolicyRevisionView, PolicyValidationView, SchedulePreviewRequest,
    SchedulePreviewView, ValidatePolicyDraftRequest,
};
pub use structural_monitor::{
    ExchangeHistorySourceView, ExecutionHistoryCoverageView, MissingReasonCountView,
    NegRiskEventDriftView, NegRiskLegView, ParticipantConcentrationDetailView,
    ParticipantConcentrationMarketView, ParticipantConcentrationParticipantView,
    ParticipantConcentrationSummaryView,
};
pub use system::{
    ActionEligibilityDecision, ActionEligibilityView, CapabilityView, FreshBootBlockerCode,
    FreshBootBlockerScope, FreshBootBlockerView, FreshBootProfileProgressView,
    FreshBootProgressView, FreshBootRecommendedAction, FreshBootRunDetailView,
    FreshBootRunEventView, FreshBootRunProgressView, RetryFreshBootRunRequest,
    SetKillSwitchRequest, SupersedeFreshBootRunRequest, SwitchQuantModeRequest,
    SwitchSettlementWritePolicyRequest, SystemCapabilities, SystemStatusView,
};
pub use trade_policy::{
    FitTradePolicyRequest, TradePolicyAuditListQuery, TradePolicyDetailView,
    TradePolicyEvidenceDownloadView, TradePolicyEvidenceRowListQuery, TradePolicyEvidenceRowView,
    TradePolicyFitPreflightRequest, TradePolicyFitPreflightView, TradePolicyFitReadiness,
    TradePolicyFitSelection, TradePolicyGovernanceAuditView, TradePolicyGovernanceRequest,
    TradePolicyListQuery, TradePolicyOperationalEvidenceView, TradePolicyPreflightBlockerDetail,
    TradePolicyPreflightBlockerView, TradePolicyPreflightCheckStatus,
    TradePolicySourceSliceObjectListQuery, TradePolicySourceSliceObjectView,
    TradePolicySourceSliceView, TradePolicySummaryView, TradePolicyTrialAttemptView,
    TradePolicyTrialListQuery, TradePolicyValidationListQuery, TradePolicyValidationRowListQuery,
    TradePolicyValidationRowView, TradePolicyValidationRunView,
};
pub use training_dataset::{
    BuildTrainingDatasetRequest, DatasetManifestView, TrainingDatasetListQuery,
    TrainingDatasetPlanView, TrainingDatasetView,
};
pub use user::{
    AssignRolesRequest, ChangePasswordRequest, ChangeUserStatusRequest, CreateUserRequest,
    UpdateUserRequest, UserPageQuery,
};
pub use validation::{validate_half_open_window, validate_optional_inclusive_range};
pub use window::TimeWindowQuery;
