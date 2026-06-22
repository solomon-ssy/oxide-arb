//! `PostgreSQL` repository implementations, grouped by bounded context.
//!
//! Every concrete `Pg*Repository` is re-exported flat under `postgres::` so
//! wiring code can name it without threading the context path.

// Crate-internal helpers.
pub mod arc_repo;
pub(crate) mod bind_limit;

pub use arc_repo::arc_repo;

pub mod governance;
pub mod quant;
pub mod rbac;

// Single-repository contexts kept flat.
pub mod market;

// Flattened facade.
pub use governance::*;
pub use market::*;
pub use quant::*;
pub use rbac::*;
