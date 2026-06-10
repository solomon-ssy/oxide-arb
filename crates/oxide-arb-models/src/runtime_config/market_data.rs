//! Market-data staleness runtime configuration (`market_data` section).
//!
//! Staleness thresholds gate every detection and validation decision, so they
//! are hot-reloadable operator tunables. Connection-level WebSocket / Gamma
//! parameters are deploy configuration (`config::MarketDataDeployConfig`).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Book staleness ladder: Fresh < Acceptable < Stale < Expired.
///
/// `Fresh`/`Acceptable` books are tradeable; `Stale` books are scanned but
/// discounted; `Expired` books are ignored entirely.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct MarketDataStalenessConfig {
    /// Book age (ms) at or below which data is `Fresh`. Default: `2000`.
    pub staleness_fresh_ms: u64,
    /// Book age (ms) at or below which data is `Acceptable` (still tradeable).
    /// Default: `5000`.
    pub staleness_acceptable_ms: u64,
    /// Book age (ms) at or below which data is `Stale` (scored with discount,
    /// never traded). Default: `15000`.
    pub staleness_stale_ms: u64,
    /// Book age (ms) above `staleness_stale_ms` is `Expired` and ignored. This
    /// field documents the ladder's outer bound. Default: `30000`.
    pub staleness_expired_ms: u64,
}

impl Default for MarketDataStalenessConfig {
    fn default() -> Self {
        Self {
            staleness_fresh_ms: default_staleness_fresh(),
            staleness_acceptable_ms: default_staleness_acceptable(),
            staleness_stale_ms: default_staleness_stale(),
            staleness_expired_ms: default_staleness_expired(),
        }
    }
}

const fn default_staleness_fresh() -> u64 {
    2_000
}
const fn default_staleness_acceptable() -> u64 {
    5_000
}
const fn default_staleness_stale() -> u64 {
    15_000
}
const fn default_staleness_expired() -> u64 {
    30_000
}
