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
mod health;
mod menu;
mod permission;
mod role;
pub mod serde;
mod user;

pub use auth::*;
pub use health::*;
pub use menu::*;
pub use permission::*;
pub use role::*;
pub use user::*;
