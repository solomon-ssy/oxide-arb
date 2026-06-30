//! Runtime subsystem bundles owned by [`super::AppContext`].
//!
//! Each bundle exposes an `assemble` entry point; [`super::build`] only
//! orchestrates bootstrap order and cross-bundle wiring.

mod account;
mod data;
mod execution;
mod future;
mod governance;
mod infra;
mod pg_repos;
mod research;

pub use account::{AccountBundle, AccountBundleDeps};
pub use data::{DataBundle, DataBundleDeps};
pub use execution::{ExecutionBundle, ExecutionBundleDeps};
pub use future::{PortfolioBundle, ReportBundle, ReportBundleDeps, RuntimeChannels};
pub use governance::{GovernanceBundle, GovernanceBundleDeps, RuntimeSnapshot};
pub use infra::InfraBundle;
pub use pg_repos::PgRepositories;
pub use research::{ResearchBundle, ResearchBundleDeps};
