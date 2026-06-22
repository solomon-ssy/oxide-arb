//! Repository trait definitions, grouped by bounded context.
//!
//! Every trait is also re-exported flat under `traits::` so callers can depend
//! on `traits::MarketRepository` without threading the context path.

pub mod governance;
pub mod quant;
pub mod rbac;

// Single-trait contexts kept flat.
pub mod market;

// Flattened facade.
pub use governance::*;
pub use market::*;
pub use quant::*;
pub use rbac::*;
