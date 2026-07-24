//! Quant-pivot persistence DTOs for schema-first repositories.

mod account;
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
#[allow(clippy::needless_update)] // Insert DTOs omit DB-managed timestamps.
mod entry_condition;
mod execution;
mod execution_account;
mod exit_training;
mod factor;
mod feature;
#[allow(clippy::needless_update)] // Insert DTOs omit database-managed timestamps.
mod feature_parity;
mod feedback_cohort;
#[allow(clippy::needless_update)] // NewModelGovernanceAudit omits DB-managed created_at
mod governance_audit;
#[allow(clippy::needless_update)] // NewMarketLinkage omits DB-managed created_at
mod linkage;
#[allow(clippy::needless_update)] // NewModelRun covers all ActiveModel columns
mod model;
mod outcome_reconciliation;
mod portfolio;
mod position;
mod recommendation;
mod recommendation_execution_outcome;
#[allow(clippy::needless_update)] // Insert DTO omits DB-managed created_at.
mod recommendation_resolution_outcome;
mod reconciliation;
mod report_data_quality;
mod report_diff;
#[allow(clippy::needless_update)] // Insert DTO omits delivery-managed timestamps and lease fields.
mod report_fact_delivery;
#[allow(clippy::needless_update)] // Insert DTO intentionally contains queued-run fields only.
mod report_run;
mod report_txn;
#[allow(clippy::needless_update)] // NewResearchJob omits DB-managed timestamps
mod research_job;
#[allow(clippy::needless_update)] // Insert DTO omits DB-managed created_at.
mod research_readiness;
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
pub use backtest::{BacktestReportInfo, NewBacktestReport};
pub use backtest_path_set::{BacktestPathSetInfo, NewBacktestPathSet};
pub use basis_alert::{BasisAlertInfo, NewBasisAlert};
pub use calibration_artifact::{
    CalibrationArtifactInfo, CalibrationArtifactPayload, NewCalibrationArtifact,
};
pub use candidate::{DomainAvailability, MarketCandidate, MarketDataHealth};
pub use capital::{CapitalAllocationInfo, CapitalAllocationPatch, NewCapitalAllocation};
pub use comparison::{ModelComparisonReportInfo, NewModelComparisonReport};
pub use dataset::{
    CompleteTrainingDatasetBuild, NewTrainingDatasetPlan, TrainingDatasetInfo,
    TrainingDatasetMaterialization,
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
pub use exit_training::{ExitTrainingLotRow, LotExitEventRow};
pub use factor::{
    FactorDefinitionInfo, FactorValueInfo, FactorValueModel, LatestFactorSnapshotBundleInfo,
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
    FeedbackCohortPageQuery, FeedbackCohortWindow, FeedbackExecutionAttempt,
    FeedbackExecutionEvidence, FeedbackRecommendationContext, FeedbackResolutionEvidence,
};
pub use governance_audit::{
    ModelGovernanceAuditDetail, ModelGovernanceAuditInfo, NewModelGovernanceAudit,
};
pub use linkage::{
    CryptoSubject, GroundingField, GroundingKind, GroundingProof, GroundingSpan, LinkageOutcome,
    LinkageSourceMetadata, LinkageUnresolvedReason, LinkageValidationFailure, ManualEvidenceInput,
    MarketLinkage, MarketLinkageDerivation, MarketLinkageInfo, MarketSubject, NewMarketLinkage,
    OverrideContext, PriceBoundaryInclusion, PriceComparator, ResolutionOracle, ResolvedBinding,
    ResolvedSourceBinding, WeatherDecisionGroupKey, WeatherSubject,
};
pub use model::{
    ModelRunInfo, ModelSpecInfo, ModelVersionInfo, NewModelRun, NewModelSpec, NewModelVersion,
    PublishedModelCatalogInfo, QuantModelRunModel,
};
pub use outcome_reconciliation::{
    ExecutionOutcomeDeferredReason, ExecutionOutcomeDerivation,
    ExecutionOutcomeReconciliationError, ExecutionOutcomeReconciliationResult,
    ExecutionOutcomeSourceGraph, RecommendationExecutionReconciliationCandidate,
    RecommendationResolutionReconciliationCandidate,
};
pub use portfolio::{NewPortfolioPlan, PortfolioPlanInfo};
pub use position::{NewPosition, PositionExit, PositionFill, PositionInfo};
pub use recommendation::{
    NewRecommendation, NewRecommendationReport, RecommendationInfo, RecommendationReportInfo,
};
pub use recommendation_execution_outcome::{
    NewRecommendationExecutionOutcome, RecommendationExecutionOutcomeContractError,
    RecommendationExecutionOutcomeInfo,
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
pub use research_job::{
    JobProgressSink, NewResearchJob, NoopProgressSink, ResearchJobInfo, ResearchJobResultRef,
};
pub use research_readiness::{NewResearchReadinessEvidence, ResearchReadinessEvidenceInfo};
pub use selection::{
    MarketSelectionInfo, MarketSelectionMemberInfo, MarketSelectionModel, NewMarketSelection,
    NewMarketSelectionMember,
};
pub use shadow::{NewShadowComparison, ShadowComparisonInfo, ShadowStabilitySummary};
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
