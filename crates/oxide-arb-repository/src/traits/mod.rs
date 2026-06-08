//! Repository trait definitions, grouped by bounded context.
//!
//! Every trait is also re-exported flat under `traits::` so callers can depend
//! on `traits::MarketRepository` without threading the context path.

// Bounded-context groups.
pub mod accounting;
pub mod evidence;
pub mod governance;
pub mod rbac;
pub mod risk;
pub mod trading;

// Single-trait contexts kept flat.
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
