//! Quant-pivot persistence DTOs for Phase 1 schema-first repositories.

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
#[allow(clippy::needless_update)] // NewModelRun covers all ActiveModel columns
mod model;
mod portfolio;
pub mod prelude;
mod recommendation;
#[allow(clippy::needless_update)] // NewMarketSelectionMember covers all ActiveModel columns
mod selection;

pub use attribution::*;
pub use backtest::*;
pub use candidate::*;
pub use comparison::*;
pub use dataset::*;
pub use execution::*;
pub use factor::*;
pub use feature::*;
pub use model::*;
pub use portfolio::*;
pub use recommendation::*;
pub use selection::*;
