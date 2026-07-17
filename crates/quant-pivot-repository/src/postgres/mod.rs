//! `PostgreSQL` repository implementations, grouped by bounded context.
//!
//! Every concrete `Pg*Repository` is re-exported flat under `postgres::` so
//! wiring code can name it without threading the context path.

// Crate-internal helpers.
pub mod arc_repo;
pub(crate) mod error;
pub(crate) mod primitives;
pub(crate) mod query;
pub(crate) mod state_hash;
pub(crate) mod write;

pub use arc_repo::arc_repo;

pub mod catalog;
pub mod governance;
pub mod quant;
pub mod rbac;

// Flattened facade.
pub use catalog::*;
pub use governance::*;
pub use quant::*;
pub use rbac::*;
