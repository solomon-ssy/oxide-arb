//! Typed control-factor artifacts, lifecycle rules, and persistence DTOs.

mod evidence;
mod lifecycle;
mod materialization;
mod payload;
mod persistence;
mod publication;
mod safety;
mod value;

pub use evidence::*;
pub use lifecycle::FactorLifecycle;
pub use materialization::*;
pub use payload::*;
pub use persistence::*;
pub use publication::*;
pub use value::{ControlFactorValue, FactorDimensions};
