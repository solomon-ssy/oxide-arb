//! Live control-factor consumption.
//!
//! Provides snapshot storage, the refresher that keeps it current, and the
//! shadow evaluator. The trading hot path reads only the published `ArcSwap`
//! snapshot through [`FactorSnapshotStore`]; all Postgres I/O lives in the
//! refresher.

pub mod factor_refresher;
pub mod factor_shadow;
pub mod factor_snapshot;
pub mod mode_transition;
pub mod replay;

pub use factor_refresher::{FactorRefreshConfig, FactorRefresher};
pub use factor_shadow::{
    NewShadowDecision, ShadowDecisionWriter, ShadowEvaluator, ShadowWriterTask,
};
pub use factor_snapshot::FactorSnapshotStore;
pub use oxide_arb_control::governance::{ControlFactorRegistry, PublicationRequest};
