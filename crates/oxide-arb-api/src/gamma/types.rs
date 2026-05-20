//! Raw Gamma API response DTOs.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Resolution status from Gamma API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GammaResolution {
    pub resolved: bool,
    pub outcome: Option<String>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub winning_outcome: Option<String>,
}

/// Raw event response from Gamma API.
#[derive(Debug, Clone, Deserialize)]
pub struct RawGammaEvent {
    pub id: String,
    pub title: String,
    pub slug: String,
    pub neg_risk: Option<bool>,
    pub markets: Option<Vec<RawGammaMarket>>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

/// Raw market response from Gamma API.
#[derive(Debug, Clone, Deserialize)]
pub struct RawGammaMarket {
    pub condition_id: String,
    pub question: String,
    pub slug: Option<String>,
    pub tokens: Option<Vec<RawGammaToken>>,
    pub neg_risk: Option<bool>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub category: Option<String>,
    /// Whether taker fees apply for this market's tokens.
    pub fees_enabled: Option<bool>,
    pub minimum_order_size: Option<Decimal>,
    pub minimum_tick_size: Option<String>,
    #[allow(dead_code)]
    pub volume: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

/// Raw token response from Gamma API.
#[derive(Debug, Clone, Deserialize)]
pub struct RawGammaToken {
    pub token_id: String,
    pub outcome: String,
}
