//! Quant-pivot persistence DTOs for Phase 1 schema-first repositories.

mod account;
mod attribution;
#[allow(clippy::needless_update)] // NewBacktestReport omits DB-managed created_at
mod backtest;
mod candidate;
mod capital;
#[allow(clippy::needless_update)] // NewModelComparisonReport omits DB-managed created_at
mod comparison;
#[allow(clippy::needless_update)] // NewTrainingDataset omits DB-managed created_at
mod dataset;
mod execution;
mod exit_training;
mod factor;
#[allow(clippy::needless_update)] // NewFavoriteLongshotBiasTable omits DB-managed created_at
mod favorite_longshot;
mod feature;
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
mod report_txn;
#[allow(clippy::needless_update)] // NewResearchJob omits DB-managed timestamps
mod research_job;
#[allow(clippy::needless_update)] // NewMarketSelectionMember covers all ActiveModel columns
mod selection;
#[allow(clippy::needless_update)] // NewSettlementRedeem* omit DB-managed timestamps
mod settlement;
#[allow(clippy::needless_update)] // NewShadowComparison omits DB-managed created_at
mod shadow;

pub use account::*;
pub use attribution::*;
pub use backtest::*;
pub use candidate::*;
pub use capital::*;
pub use comparison::*;
pub use dataset::*;
pub use execution::*;
pub use exit_training::*;
pub use factor::*;
pub use favorite_longshot::*;
pub use feature::*;
pub use governance_audit::*;
pub use linkage::*;
pub use model::*;
pub use portfolio::*;
pub use position::*;
pub use recommendation::*;
pub use reconciliation::*;
pub use report_data_quality::*;
pub use report_diff::*;
pub use report_txn::*;
pub use research_job::*;
pub use selection::*;
pub use settlement::*;
pub use shadow::*;
