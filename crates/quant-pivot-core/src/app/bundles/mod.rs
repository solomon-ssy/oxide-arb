//! Runtime subsystem bundles owned by [`super::AppContext`].
//!
//! Each bundle exposes an `assemble` entry point; [`super::build`] only
//! orchestrates bootstrap order and cross-bundle wiring.

mod data;
mod future;
mod governance;
mod infra;
mod research;

pub use data::{DataBundle, DataBundleDeps};
pub use future::{ExecutionIntentBundle, PortfolioBundle, ReportBundle, RuntimeChannels};
pub use governance::{GovernanceBundle, GovernanceBundleDeps, RuntimeSnapshot};
pub use infra::InfraBundle;
pub use research::ResearchBundle;
