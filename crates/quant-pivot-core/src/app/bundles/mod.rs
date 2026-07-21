//! Runtime subsystem bundles owned by [`super::AppContext`].
//!
//! Each bundle exposes an `assemble` entry point; [`super::build`] only
//! orchestrates bootstrap order and cross-bundle wiring.

mod account;
mod data;
mod execution;
mod governance;
mod infra;
mod pg_repos;
mod report;
mod research;

pub use account::{AccountBundle, AccountBundleDeps};
pub use data::{DataBundle, DataBundleDeps};
pub use execution::{ExecutionBundle, ExecutionBundleDeps};
pub use governance::{GovernanceBundle, GovernanceBundleDeps, RuntimeSnapshot};
pub use infra::InfraBundle;
pub use pg_repos::PgRepositories;
pub use report::{ReportBundle, ReportBundleDeps};
pub use research::{ResearchBundle, ResearchBundleDeps};
