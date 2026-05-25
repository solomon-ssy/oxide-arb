//! Type-safe cache key builder with embedded TTL and domain labels.
//!
//! Each variant encodes a unique namespace prefix, a per-domain TTL for L2
//! (Redis), and a domain label for metrics partitioning. L1 (Moka) TTL is
//! derived as `ttl / 4` by the [`TieredCache`] layer.

use oxide_arb_models::{
    enums::calibration::{DurationBucket, PriceZone},
    enums::common::MarketCategory,
    enums::runtime_config::RuntimeConfigKey,
    types::{EventId, MarketId},
};
use std::time::Duration;

/// Strongly-typed cache key encompassing all cacheable domains.
///
/// Key format: `"{domain_prefix}:{discriminator}"`.
/// TTL semantics: represents the L2 (Redis) expiry; L1 uses `ttl / 4`.
pub enum CacheKey {
    // ── Market data ─────────────────────────────────────────────────────
    /// Cached market metadata (category, tokens, settlement info).
    MarketInfo { market_id: MarketId },

    /// Cached event entry (event-level metadata aggregate).
    EventInfo { event_id: EventId },

    /// Market metadata used by detection and scoring (category, deadline).
    MarketMetadata { market_id: MarketId },

    /// Cached active market list used by scanner startup and periodic refresh.
    ActiveMarkets,

    // ── Calibration ─────────────────────────────────────────────────────
    /// Single calibration bucket entry.
    CalibrationBucket {
        category: MarketCategory,
        price_zone: PriceZone,
        duration_bucket: DurationBucket,
    },

    /// Bulk cache of all calibration buckets (startup / periodic reload).
    AllCalibrationBuckets,

    // ── Risk & Position ─────────────────────────────────────────────────
    /// Per-market position summary (shares, avg cost, unrealized `PnL`).
    PositionSummary { market_id: MarketId },

    /// Singleton risk engine state snapshot (breaker level, counters).
    RiskState,

    /// Singleton available balance snapshot for the sole Polymarket venue.
    Balance,

    // ── Configuration ───────────────────────────────────────────────────
    /// Runtime configuration key-value pair.
    RuntimeConfig { key: RuntimeConfigKey },

    /// Bulk cache of all runtime configuration entries.
    AllRuntimeConfig,

    /// Fee parameters per category (maker/taker rates).
    FeeParams { category: MarketCategory },
}

impl CacheKey {
    /// Render the cache key as a string suitable for both L1 and L2 backends.
    pub fn as_str(&self) -> String {
        match self {
            Self::MarketInfo { market_id } => format!("mkt:{market_id}"),
            Self::EventInfo { event_id } => format!("evt:{event_id}"),
            Self::MarketMetadata { market_id } => format!("mkt_meta:{market_id}"),
            Self::ActiveMarkets => "mkt:__active__".to_owned(),
            Self::CalibrationBucket {
                category,
                price_zone,
                duration_bucket,
            } => format!("cal:{}:{price_zone}:{duration_bucket}", category.as_str()),
            Self::AllCalibrationBuckets => "cal:__all__".to_owned(),
            Self::PositionSummary { market_id } => format!("pos:{market_id}"),
            Self::RiskState => "risk:state".to_owned(),
            Self::Balance => "bal:polymarket".to_owned(),
            Self::RuntimeConfig { key } => format!("cfg:{}", key.as_str()),
            Self::AllRuntimeConfig => "cfg:__all__".to_owned(),
            Self::FeeParams { category } => format!("fee:{}", category.as_str()),
        }
    }

    /// L2 (Redis) TTL for this key variant.
    ///
    /// L1 (Moka) TTL is derived as `ttl / 4` by the tiered cache layer.
    pub const fn ttl(&self) -> Duration {
        match self {
            Self::MarketInfo { .. } | Self::EventInfo { .. } | Self::ActiveMarkets => {
                Duration::from_secs(300)
            }
            Self::MarketMetadata { .. } => Duration::from_secs(1800),
            Self::CalibrationBucket { .. } | Self::AllCalibrationBuckets => {
                Duration::from_secs(3600)
            }
            Self::PositionSummary { .. } => Duration::from_secs(30),
            Self::RiskState | Self::RuntimeConfig { .. } | Self::AllRuntimeConfig => {
                Duration::from_secs(60)
            }
            Self::Balance => Duration::from_secs(15),
            Self::FeeParams { .. } => Duration::from_secs(600),
        }
    }

    /// Domain label used for metrics partitioning (cache hit/miss counters).
    pub const fn domain(&self) -> &'static str {
        match self {
            Self::MarketInfo { .. } | Self::ActiveMarkets => "market",
            Self::EventInfo { .. } => "event",
            Self::MarketMetadata { .. } => "market_metadata",
            Self::CalibrationBucket { .. } | Self::AllCalibrationBuckets => "calibration",
            Self::PositionSummary { .. } => "position",
            Self::RiskState => "risk",
            Self::Balance => "balance",
            Self::RuntimeConfig { .. } | Self::AllRuntimeConfig => "config",
            Self::FeeParams { .. } => "fee",
        }
    }
}
