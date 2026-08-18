//! Web-facing dependency-inversion ports.

pub mod account_read;
pub mod backtest;
pub mod backtest_path_set;
pub mod calibration_artifact;
pub mod exchange_history_progress;
pub mod execution_read;
pub mod execution_recovery;
pub mod feature_integrity;
pub mod feature_parity_execution;
pub mod feedback_execution;
pub mod feedback_governance;
pub mod feedback_mutation;
pub mod feedback_read;
pub mod feedback_recipe;
pub mod feedback_shadow_binding;
pub mod market_linkage;
pub mod model_governance;
pub mod model_spec;
pub mod model_training;
pub mod order_intent;
pub mod password_crypto;
pub mod quant_report;
pub mod reconciliation;
pub mod research_catalog;
pub mod research_job;
pub mod research_readiness;
pub mod runtime_control;
pub mod settlement_control;
pub mod structural_monitor;
pub mod trade_policy;
pub mod training_dataset;

pub use account_read::{AccountReadPort, LiveAccountInfo};
pub use backtest::BacktestPort;
pub use backtest_path_set::CpcvBacktestPort;
pub use calibration_artifact::{
    BiasTableFitJobParams, BiasTableFitOutcome, CalibrationArtifactFitPort,
    ModelCalibrationFitJobParams, ModelCalibrationFitOutcome, ModelCalibrationFitPort,
};
pub use exchange_history_progress::ExchangeHistoryProgressPort;
pub use execution_read::ExecutionReadPort;
pub use execution_recovery::ExecutionRecoveryPort;
pub use feature_integrity::{FeatureIntegrityActionContext, FeatureIntegrityPort};
pub use feature_parity_execution::{FeatureParityExecutionOutcome, FeatureParityExecutionPort};
pub use feedback_execution::{
    FEEDBACK_LEARNING_MAX_CANDIDATES, FeedbackCalibrationCommand, FeedbackCalibrationJobParams,
    FeedbackCandidateFamily, FeedbackCandidateFamilyInput, FeedbackCandidateRecipe,
    FeedbackCandidateRecipeInput, FeedbackComparisonAlternative, FeedbackComparisonArtifactRef,
    FeedbackComparisonCandidateRef, FeedbackComparisonContract, FeedbackComparisonExecutionPort,
    FeedbackComparisonExecutionResult, FeedbackComparisonGenerator, FeedbackComparisonJobInput,
    FeedbackComparisonJobParams, FeedbackComparisonPValue, FeedbackComparisonResampling,
    FeedbackComparisonStatistic, FeedbackComparisonStepdown, FeedbackComparisonTies,
    FeedbackCoverageExecutionPort, FeedbackCoverageExecutionResult, FeedbackCpcvCommand,
    FeedbackCpcvJobParams, FeedbackDatasetBuildCommand, FeedbackDatasetBuildRequest,
    FeedbackDatasetRole, FeedbackDatasetSealJobParams, FeedbackDecisionExecutionPort,
    FeedbackDecisionExecutionResult, FeedbackDecisionJobInput, FeedbackDecisionJobParams,
    FeedbackDriftArtifactRef, FeedbackDriftExecutionPort, FeedbackDriftExecutionResult,
    FeedbackEvaluationUseRef, FeedbackLearningExecutionPort, FeedbackLearningExecutionResult,
    FeedbackLearningStageArtifactRef, FeedbackShadowArtifactRef, FeedbackShadowContract,
    FeedbackShadowContractInput, FeedbackShadowExecutionPort, FeedbackShadowExecutionResult,
    FeedbackShadowJobParams, FeedbackShadowObservationSource, FeedbackShadowSubject,
    FeedbackShadowUnavailableReason, FeedbackTrainingCommand, FeedbackTrainingJobParams,
};
pub use feedback_governance::{
    FeedbackAttributionJobParams, FeedbackAttributionManifest, FeedbackAttributionProduced,
    FeedbackAttributionUse, FeedbackCandidateValidation, FeedbackGovernanceExecutionPort,
    FeedbackGovernanceExecutionResult, FeedbackTruthBlocker, FeedbackTruthFreezeArtifact,
    FeedbackTruthFreezeJobParams, FeedbackValidationArtifact, FeedbackValidationArtifactRef,
    FeedbackValidationJobParams, FeedbackValidationTrialOutcome,
};
pub use feedback_mutation::{FeedbackActivationReadPort, FeedbackMutationPort};
pub use feedback_read::FeedbackReadPort;
pub use feedback_recipe::{
    CandidateRecipePlanArtifact, CandidateRecipePlanExecutionPort,
    CandidateRecipePlanExecutionResult, CandidateRecipePlanInput, CandidateRecipePlanJobParams,
    CandidateRecipePlanOutcome, CandidateRecipeReadinessBlocker, CandidateRecipeSelection,
    FeedbackAttributionManifestRef, FeedbackRecipeCalibrationSpec, FeedbackRecipeCpcvSpec,
    FeedbackRecipeDiagnosticEvidence, FeedbackRecipeDiagnosticSpec, FeedbackRecipeDownsideSpec,
    FeedbackRecipeDriftManifest, FeedbackRecipeOosAggregation, FeedbackRecipeOosEvidence,
    FeedbackRecipeOosSummary, FeedbackRecipeResourceBudget, FeedbackRecipeTemplate,
    FeedbackRecipeTemplateInput, FeedbackRecipeTrainingSpec,
};
pub use feedback_shadow_binding::{
    CancelShadowBinding, RejectShadowBinding, ShadowBindingArtifact, ShadowBindingArtifactRef,
    ShadowBindingCancellationReceipt, ShadowBindingExecutionPort, ShadowBindingExecutionResult,
    ShadowBindingJobInput, ShadowBindingJobParams, ShadowBindingLifecycle, ShadowBindingReceipt,
    ShadowBindingReceiptInput, ShadowBindingRejectionReceipt,
};
pub use market_linkage::MarketLinkageGovernancePort;
pub use model_governance::{
    BootstrapQualityGateEvidence, BootstrapQualityGateInput, CalibratedModelSealCommand,
    CandidateQualityGateEvidence, GovernanceActor, ModelGovernancePort,
};
pub use model_spec::{CreateModelSpecCommand, ModelSpecPort};
pub use model_training::ModelTrainingPort;
pub use order_intent::{
    ApproveIntentCommand, CancelIntentCommand, CreateIntentCommand, ExecutionSubmitPort,
    OrderIntentPort, RejectIntentCommand,
};
pub use password_crypto::PasswordCryptoPort;
pub use quant_report::{AdHocReportCommand, QuantReportPort};
pub use reconciliation::ReconciliationPort;
pub use research_catalog::ResearchCatalogPort;
pub use research_job::{JobSubmitContext, ResearchJobPort};
pub use research_readiness::{ResearchReadinessPort, ResearchReadinessSnapshot};
pub use runtime_control::{
    CatalogState, CatalogStatusPort, CommittedPolicyApplyPort, DataQualityPort,
    EntryAuthorizationTransitionReport, KillSwitchPort, MarketDataPort, MetricsScrapePort,
    PolicySnapshotPort, PreparedPolicySnapshot, ReadinessPort, RuntimeControlPort,
    SetKillSwitchCommand, SystemCapabilityPort,
};
pub use structural_monitor::StructuralMonitorPort;
pub use trade_policy::TradePolicyPort;
pub use training_dataset::{PolicyFitDatasetBuildRequest, TrainingDatasetPort};
