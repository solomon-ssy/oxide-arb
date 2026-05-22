//! Domain models and DTOs.
//!
//! This module contains two categories of types:
//! 1. **Read models** (`TradeRecord`, `PositionInfo`, etc.) — rich domain objects
//!    used across the business layer for display and calculation.
//! 2. **Write DTOs** (`NewTrade`, `UpdateTradeOutcome`, `NewPosition`, etc.) —
//!    typed boundary objects between application and persistence. Application code
//!    constructs these, and the repository maps them to `ActiveModel` internally.
//!    This decouples domain logic from ORM details and enforces which fields are
//!    settable at creation vs. update time.

pub mod blacklist;
pub mod book;
pub mod calibration;
pub mod exposure;
pub mod market;
pub mod opportunity;
pub mod order;
pub mod pnl;
pub mod position;
pub mod potential_loss;
pub mod report;
pub mod risk;
pub mod system;
pub mod trade;

pub use blacklist::*;
pub use book::*;
pub use calibration::*;
pub use exposure::*;
pub use market::*;
pub use opportunity::*;
pub use order::*;
pub use pnl::*;
pub use position::*;
pub use potential_loss::*;
pub use report::*;
pub use risk::*;
pub use system::*;
pub use trade::*;
