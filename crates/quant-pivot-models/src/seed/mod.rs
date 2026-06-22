//! Seed framework — type-safe, idempotent bootstrap data management.

pub mod context;
pub mod policy;
pub mod rbac;
pub mod system_runtime_state;

pub use context::SeedContext;
pub use policy::SeedConflictPolicy;
