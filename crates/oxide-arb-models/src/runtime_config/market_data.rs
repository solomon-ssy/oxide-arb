//! Market-data runtime configuration (`market_data` section).
//!
//! Staleness thresholds gate every detection and validation decision, and the
//! category universe filter bounds the tradeable market set, so both are
//! hot-reloadable operator tunables. Connection-level WebSocket / Gamma
//! parameters are deploy configuration (`config::MarketDataDeployConfig`).

use crate::enums::common::MarketCategory;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Staleness ladder and tradeable-universe filter.
///
/// `Fresh`/`Acceptable` books are tradeable; `Stale` books are scanned but
/// discounted; `Expired` books are ignored entirely.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct MarketDataRuntimeConfig {
    /// Book age (ms) at or below which data is `Fresh`. Default: `2000`.
    #[schemars(extend("x-format" = "duration_ms"))]
    pub staleness_fresh_ms: u64,
    /// Book age (ms) at or below which data is `Acceptable` (still tradeable).
    /// Default: `5000`.
    #[schemars(extend("x-format" = "duration_ms"))]
    pub staleness_acceptable_ms: u64,
    /// Book age (ms) at or below which data is `Stale` (scored with discount,
    /// never traded). Default: `15000`.
    #[schemars(extend("x-format" = "duration_ms"))]
    pub staleness_stale_ms: u64,
    /// Book age (ms) above `staleness_stale_ms` is `Expired` and ignored. This
    /// field documents the ladder's outer bound. Default: `30000`.
    #[schemars(extend("x-format" = "duration_ms"))]
    pub staleness_expired_ms: u64,
    /// Categories admitted into the tradeable universe (WS subscriptions +
    /// scanner sweep). An event matches when any of its tag-derived categories
    /// is enabled. Empty list = every category. The full catalog is always
    /// ingested and persisted regardless of this filter — it only bounds the
    /// hot trading set, so narrowing it never loses settlement or evidence
    /// data. Default: empty (all categories).
    #[schemars(extend("x-enum-id" = "market_category"))]
    pub enabled_categories: Vec<MarketCategory>,
}

impl Default for MarketDataRuntimeConfig {
    fn default() -> Self {
        Self {
            staleness_fresh_ms: default_staleness_fresh(),
            staleness_acceptable_ms: default_staleness_acceptable(),
            staleness_stale_ms: default_staleness_stale(),
            staleness_expired_ms: default_staleness_expired(),
            enabled_categories: Vec::new(),
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
