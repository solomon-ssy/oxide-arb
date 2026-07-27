//! Web-facing dependency-inversion ports.

pub mod account_read;
pub mod backtest;
pub mod backtest_path_set;
pub mod calibration_artifact;
pub mod execution_read;
pub mod execution_recovery;
pub mod feature_integrity;
pub mod feature_parity_execution;
pub mod market_linkage;
pub mod model_governance;
pub mod model_spec;
pub mod model_training;
pub mod order_intent;
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
pub use execution_read::ExecutionReadPort;
pub use execution_recovery::ExecutionRecoveryPort;
pub use feature_integrity::{FeatureIntegrityActionContext, FeatureIntegrityPort};
pub use feature_parity_execution::FeatureParityExecutionPort;
pub use market_linkage::MarketLinkageGovernancePort;
pub use model_governance::{
    GovernanceActor, ModelGovernancePort, PublishModelCommand, RetireModelCommand,
};
pub use model_spec::{CreateModelSpecCommand, ModelSpecPort};
pub use model_training::ModelTrainingPort;
pub use order_intent::{
    ApproveIntentCommand, CancelIntentCommand, CreateIntentCommand, ExecutionSubmitPort,
    OrderIntentPort, RejectIntentCommand,
};
pub use quant_report::{AdHocReportCommand, QuantReportPort};
pub use reconciliation::ReconciliationPort;
pub use research_catalog::ResearchCatalogPort;
pub use research_job::{JobSubmitContext, ResearchJobPort};
pub use research_readiness::{ResearchReadinessPort, ResearchReadinessSnapshot};
pub use runtime_control::{
    CatalogState, CatalogStatusPort, DataQualityPort, KillSwitchPort, MarketDataPort,
    MetricsScrapePort, PolicySnapshotPort, PreparedPolicySnapshot, QuantModeTransitionReport,
    ReadinessPort, RuntimeControlPort, SetKillSwitchCommand, SystemCapabilityPort,
};
pub use structural_monitor::StructuralMonitorPort;
pub use trade_policy::TradePolicyPort;
pub use training_dataset::{PolicyFitDatasetBuildRequest, TrainingDatasetPort};
