//! Quant-pivot persistence DTOs for Phase 1 schema-first repositories.

mod account;
mod attribution;
#[allow(clippy::needless_update)] // NewBacktestReport omits DB-managed created_at
mod backtest;
mod candidate;
#[allow(clippy::needless_update)] // NewModelComparisonReport omits DB-managed created_at
mod comparison;
#[allow(clippy::needless_update)] // NewTrainingDataset omits DB-managed created_at
mod dataset;
mod execution;
mod factor;
mod feature;
#[allow(clippy::needless_update)] // NewModelGovernanceAudit omits DB-managed created_at
mod governance_audit;
#[allow(clippy::needless_update)] // NewModelRun covers all ActiveModel columns
mod model;
mod portfolio;
pub mod prelude;
mod recommendation;
mod report_diff;
mod report_txn;
#[allow(clippy::needless_update)] // NewMarketSelectionMember covers all ActiveModel columns
mod selection;
#[allow(clippy::needless_update)] // NewShadowComparison omits DB-managed created_at
mod shadow;

pub use account::*;
pub use attribution::*;
pub use backtest::*;
pub use candidate::*;
pub use comparison::*;
pub use dataset::*;
pub use execution::*;
pub use factor::*;
pub use feature::*;
pub use governance_audit::*;
pub use model::*;
pub use portfolio::*;
pub use recommendation::*;
pub use report_diff::*;
pub use report_txn::*;
pub use selection::*;
pub use shadow::*;
