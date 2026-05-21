//! Type-safe cache key builder with embedded TTL and domain labels.

use std::time::Duration;

pub enum CacheKey {
    MarketEntry {
        market_id: String,
    },
    EventEntry {
        event_id: String,
    },
    CalibrationBucket {
        category: String,
        price_zone: String,
        duration_bucket: String,
    },
    RuntimeConfig {
        key: String,
    },
    FeeParams {
        category: String,
    },
}

impl CacheKey {
    pub fn as_str(&self) -> String {
        match self {
            Self::MarketEntry { market_id } => format!("mkt:{market_id}"),
            Self::EventEntry { event_id } => format!("evt:{event_id}"),
            Self::CalibrationBucket {
                category,
                price_zone,
                duration_bucket,
            } => {
                format!("cal:{category}:{price_zone}:{duration_bucket}")
            }
            Self::RuntimeConfig { key } => format!("cfg:{key}"),
            Self::FeeParams { category } => format!("fee:{category}"),
        }
    }

    pub const fn ttl(&self) -> Duration {
        match self {
            Self::MarketEntry { .. } | Self::EventEntry { .. } => Duration::from_secs(300),
            Self::CalibrationBucket { .. } => Duration::from_secs(3600),
            Self::RuntimeConfig { .. } => Duration::from_secs(60),
            Self::FeeParams { .. } => Duration::from_secs(600),
        }
    }

    pub const fn domain(&self) -> &'static str {
        match self {
            Self::MarketEntry { .. } => "market",
            Self::EventEntry { .. } => "event",
            Self::CalibrationBucket { .. } => "calibration",
            Self::RuntimeConfig { .. } => "config",
            Self::FeeParams { .. } => "fee",
        }
    }
}
