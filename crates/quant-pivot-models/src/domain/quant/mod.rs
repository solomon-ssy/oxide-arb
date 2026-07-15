//! Quant-pivot persistence DTOs for Phase 1 schema-first repositories.

mod account;
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
#[allow(clippy::needless_update)] // Insert DTOs omit DB-managed timestamps.
mod entry_condition;
mod execution;
mod exit_training;
mod factor;
mod feature;
#[allow(clippy::needless_update)] // Insert DTOs omit database-managed timestamps.
mod feature_parity;
#[allow(clippy::needless_update)] // NewModelGovernanceAudit omits DB-managed created_at
mod governance_audit;
#[allow(clippy::needless_update)] // NewMarketLinkage omits DB-managed created_at
mod linkage;
#[allow(clippy::needless_update)] // NewModelRun covers all ActiveModel columns
mod model;
mod portfolio;
mod position;
pub mod prelude;
mod recommendation;
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
#[allow(clippy::needless_update)] // NewSettlementRedeem* omit DB-managed timestamps
mod settlement;
#[allow(clippy::needless_update)] // NewShadowComparison omits DB-managed created_at
mod shadow;
#[allow(clippy::needless_update)] // NewSourceSlice omits DB-managed timestamps
mod source_slice;
#[allow(clippy::needless_update)] // Insert DTOs omit DB-managed timestamps.
mod trade_policy;
mod trade_policy_trial;

pub use account::*;
pub use attribution::*;
pub use backtest::*;
pub use backtest_path_set::*;
pub use basis_alert::*;
pub use calibration_artifact::*;
pub use candidate::*;
pub use capital::*;
pub use comparison::*;
pub use dataset::*;
pub use entry_condition::*;
pub use execution::*;
pub use exit_training::*;
pub use factor::*;
pub use feature::*;
pub use feature_parity::*;
pub use governance_audit::*;
pub use linkage::*;
pub use model::*;
pub use portfolio::*;
pub use position::*;
pub use recommendation::*;
pub use reconciliation::*;
pub use report_data_quality::*;
pub use report_diff::*;
pub use report_fact_delivery::*;
pub use report_run::*;
pub use report_txn::*;
pub use research_job::*;
pub use research_readiness::*;
pub use selection::*;
pub use settlement::*;
pub use shadow::*;
pub use source_slice::*;
pub use trade_policy::*;
pub use trade_policy_trial::*;
