//! `PostgreSQL` repository implementations, grouped by bounded context.
//!
//! Every concrete `Pg*Repository` is re-exported flat under `postgres::` so
//! wiring code can name it without threading the context path.

// Bounded-context groups.
pub mod accounting;
pub mod evidence;
pub mod governance;
pub mod rbac;
pub mod risk;
pub mod trading;

// Single-repository contexts kept flat.
pub mod control_factor;
pub mod market;

// Flattened facade.
pub use accounting::*;
pub use control_factor::*;
pub use evidence::*;
pub use governance::*;
pub use market::*;
pub use rbac::*;
pub use risk::*;
pub use trading::*;
