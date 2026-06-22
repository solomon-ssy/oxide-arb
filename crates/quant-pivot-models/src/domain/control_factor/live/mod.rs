//! Live consumption layer: compiled snapshot, typed indexes, pure consumption
//! math, dimension classifiers, the hot-path provider trait, and the auditable
//! application trace. These types are read on the trading hot path and never
//! touch `ClickHouse` or Postgres.

mod applied;
mod apply;
mod classify;
mod index;
mod provider;
mod snapshot;

pub use applied::{
    AppliedControlFactor, FactorDecisionContext, MarketAnomalyDecision, PortfolioRiskDecision,
    ReconciliationHealthDecision,
};
pub use apply::{
    bucket_resolution_trace, clamp_unit, effective_fill_probability, effective_min_edge_bps,
    effective_resolution_prob, effective_slippage_limit_bps, execution_quality_fill_trace,
    expected_net_profit, size_cap, size_cap_trace,
};
pub use classify::{book_age_bucket, depth_bucket, execution_quality_dimensions, spread_bucket};
pub use index::{
    BucketRiskIndex, ExecutionQualityIndex, IndexedFactor, MarketAnomalyIndex, PortfolioRiskState,
    ReconciliationHealthState,
};
pub use provider::ControlFactorProvider;
pub use snapshot::{ControlFactorSnapshot, LIVE_SNAPSHOT_SCHEMA_VERSION};
