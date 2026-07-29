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
mod model_governance;
mod model_training;
mod operation_log;
mod permission;
mod quality_gate;
mod quant_account;
mod quant_execution;
mod quant_model;
mod quant_recommendation;
mod quant_report;
mod reconciliation;
mod research_job;
mod research_model_contract;
mod role;
mod runtime_config;
pub mod settlement_redeem;
mod structural_monitor;
mod system;
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
    ActivateCalibrationArtifactRequest, BindCalibrationRequest, CalibrationArtifactDetailView,
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
    CancelFeedbackCycleRequest, DriftReportListQuery, DriftReportView, FeedbackCohortCountsView,
    FeedbackCoverageDecision, FeedbackCoverageView, FeedbackCycleDetailView,
    FeedbackCycleListQuery, FeedbackCycleMutationView, FeedbackCycleView,
    FeedbackEvaluationUseView, FeedbackOverviewView, FeedbackProfileOverviewView,
    FeedbackQueueView, FeedbackReadinessView, FeedbackStageEventView, IssuePromotionPermitRequest,
    PromotionPermitListQuery, PromotionPermitMutationView, PromotionPermitView,
    RevokePromotionPermitRequest, TriggerFeedbackCycleRequest,
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
    LinkageResolveSummaryView, MarketLinkageDetailView, MarketLinkageHistoryEntryView,
    MarketLinkageListQuery, MarketLinkageSummaryView, OverrideLinkageRequest,
    OverrideSourceBindingInput, ResolveLinkagesRequest,
};
pub use menu::{CreateMenuRequest, UpdateMenuRequest};
pub use model_governance::{BindPublishPathSetRequest, PublishModelRequest, RetireModelRequest};
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
    CreateModelSpecRequest, ModelPickerSide, ModelPublishedCatalogQuery, ModelSpecListQuery,
    PublishedModelOptionView, QuantModelSpecView,
};
pub use quant_recommendation::{
    QuantEvidenceView, QuantRecommendationView, RecommendationViewContext,
};
pub use quant_report::{
    CurrentReportQuery, QuantReportDetailView, QuantReportDiagnosticsView, QuantReportFunnelView,
    QuantReportListQuery, QuantReportView, RecommendationChangedFieldView, RecommendationDeltaView,
    RecommendationDiffSnapshotView, ReportCurrentHealthView, ReportDiagnosticsSubject,
    ReportDiffView, ReportFactDeliveryView, ReportFunnelMarketListQuery, ReportFunnelMarketView,
    ReportFunnelStageView, ReportRunListQuery, ReportRunView, ReportScheduleGapListQuery,
    ReportScheduleGapView, ReportScheduleHealthView, ReportScheduleStateView, ReportTimelineQuery,
    RetryReportRequest, RevokeReportRequest, RunReportRequest,
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
pub use runtime_config::{
    ActivatePolicyDraftRequest, ApprovePolicyDraftRequest, ConfigActivityQuery, ConfigActivityView,
    ConfigApiContractSchema, ConfigResourceSummaryView, ConfigResourcesView,
    ConfigSnapshotOptionsQuery, CreatePolicyDraftRequest, CredentialHealthView,
    CurrentPolicyResourceView, DecisionPolicySnapshotOptionView, DeploymentConfigSnapshotView,
    DeploymentConfigView, DeploymentEndpointView, DeploymentIdentityView,
    DeploymentResourceBudgetView, DeploymentResourceLimitView, PolicyActivationResultView,
    PolicyActivationView, PolicyActorView, PolicyApprovalView, PolicyResourceSchemaView,
    PolicyRevisionListQuery, PolicyRevisionView, PolicyValidationView, SchedulePreviewRequest,
    SchedulePreviewView, ValidatePolicyDraftRequest,
};
pub use structural_monitor::{
    MissingReasonCountView, NegRiskEventDriftView, NegRiskLegView,
    ParticipantConcentrationDetailView, ParticipantConcentrationMarketView,
    ParticipantConcentrationParticipantView, ParticipantConcentrationSummaryView,
    TradeTapeCoverageView, TradeTapeSourceHealthView,
};
pub use system::{
    ActionEligibilityDecision, ActionEligibilityView, CapabilityView, SetKillSwitchRequest,
    SwitchQuantModeRequest, SwitchSettlementWritePolicyRequest, SystemCapabilities,
    SystemStatusView,
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
