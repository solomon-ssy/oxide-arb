//! HTTP API contract types — the canonical JSON shapes for the control plane.
//!
//! These types are the wire contract between clients and the web layer. They
//! deliberately sit *above* repository write DTOs (`New*`, `Patch*`): request
//! bodies carry plaintext credentials and serde three-way null semantics, while
//! handlers translate into domain write DTOs before touching persistence.
//!
//! Response views (`*View`) strip sensitive columns (e.g. `password_hash`) that
//! must never cross the wire.

mod auth;
mod control_factor;
mod health;
mod menu;
mod operation_log;
mod permission;
mod role;
mod runtime_config;
pub mod serde;
mod user;

pub use auth::*;
pub use control_factor::*;
pub use health::*;
pub use menu::*;
pub use operation_log::*;
pub use permission::*;
pub use role::*;
pub use runtime_config::*;
pub use user::*;
