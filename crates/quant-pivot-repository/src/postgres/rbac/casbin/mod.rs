//! Casbin persistence for RBAC.
//!
//! The policy-row encoding, the [`Adapter`] the live enforcer loads
//! from, and the transaction-scoped [`CasbinPolicyStore`] that assignment
//! repositories write through.
//!
//! [`Adapter`]: adapter::PgCasbinAdapter

pub mod adapter;
mod policy_store;

pub(crate) mod row;

pub use adapter::PgCasbinAdapter;
pub(crate) use policy_store::CasbinPolicyStore;
