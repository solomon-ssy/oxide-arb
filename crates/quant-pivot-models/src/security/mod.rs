//! Security primitives shared across the platform.
//!
//! Lives in `quant-pivot-models` (not the web crate) so that database seeds — which
//! run during migration, before any web layer exists — can hash the bootstrap
//! admin password with the exact same implementation the login path verifies
//! against. Single implementation, no duplication, no drift.

pub mod password;

pub use password::{hash_password, verify_password};
