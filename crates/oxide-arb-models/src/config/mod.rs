//! Application configuration tree.
//!
//! Each sub-module maps 1:1 to a `[section]` in `oxide-arb.toml`.
//! All fields carry `#[serde(default)]` so partial configs are always valid.
//!
//! Loading precedence (high → low):
//! 1. Environment variables (`OXIDE_ARB__*`)
//! 2. `config/oxide-arb.toml`
//! 3. Hard-coded defaults

mod analytics;
mod cache;
mod db;
mod detection;
mod execution;
mod keys;
mod market_data;
mod notification;
mod observability;
mod polymarket;
mod risk;
mod sizing;
mod treasury;

pub use analytics::*;
pub use cache::*;
pub use db::*;
pub use detection::*;
pub use execution::*;
pub use keys::*;
pub use market_data::*;
pub use notification::*;
pub use observability::*;
pub use polymarket::*;
pub use risk::*;
pub use sizing::*;
pub use treasury::*;

use serde::Deserialize;
use validator::Validate;

/// Top-level application configuration.
///
/// Single-strategy (endgame) + single-platform (polymarket) design.
/// See ADR-001 for rationale.
#[derive(Debug, Clone, Default, Deserialize, Validate)]
pub struct Settings {
    #[serde(default)]
    #[validate(nested)]
    pub polymarket: PolymarketConfig,
    #[serde(default)]
    #[validate(nested)]
    pub detection: DetectionConfig,
    #[serde(default)]
    #[validate(nested)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    #[validate(nested)]
    pub risk: RiskConfig,
    #[serde(default)]
    #[validate(nested)]
    pub sizing: PositionSizingConfig,
    #[serde(default)]
    #[validate(nested)]
    pub market_data: MarketDataConfig,
    #[serde(default)]
    #[validate(nested)]
    pub observability: ObservabilityConfig,
    #[serde(default)]
    #[validate(nested)]
    pub db: DatabaseConfig,
    #[serde(default)]
    #[validate(nested)]
    pub analytics: AnalyticsConfig,
    #[serde(default)]
    #[validate(nested)]
    pub cache: CacheConfig,
    #[serde(default)]
    #[validate(nested)]
    pub treasury: TreasuryConfig,
    #[serde(default)]
    #[validate(nested)]
    pub keys: KeysConfig,
    #[serde(default)]
    #[validate(nested)]
    pub notification: NotificationConfig,
}
