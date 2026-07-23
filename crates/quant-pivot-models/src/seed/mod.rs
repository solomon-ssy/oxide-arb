//! Seed framework — type-safe, idempotent bootstrap data management.

pub mod context;
pub mod policy;
pub mod rbac;
pub mod spec;
pub mod system_runtime_control;

pub use context::SeedContext;
pub use policy::SeedConflictPolicy;
pub use spec::{SeedArtifact, SeedArtifactKey, SeedDependency, SeedSpec};
