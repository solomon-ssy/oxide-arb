//! Casbin persistence for RBAC.
//!
//! The policy-row encoding, the [`Adapter`] the live enforcer loads
//! from, and the transactional policy-sync helpers ([`sync`]) that the
//! assignment repositories write through.
//!
//! [`Adapter`]: adapter::PgCasbinAdapter

pub mod adapter;
pub mod sync;

pub(crate) mod row;

pub use adapter::PgCasbinAdapter;
