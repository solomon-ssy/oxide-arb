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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawGammaEvent {
    pub id: String,
    pub title: String,
    pub slug: String,
    #[serde(alias = "end_date", alias = "endDateIso")]
    pub end_date: Option<String>,
    #[serde(alias = "neg_risk")]
    pub neg_risk: Option<bool>,
    pub markets: Option<Vec<RawGammaMarket>>,
    #[serde(alias = "created_at")]
    pub created_at: Option<String>,
    #[serde(alias = "updated_at")]
    pub updated_at: Option<String>,
}

/// Raw market response from Gamma API.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawGammaMarket {
    #[serde(alias = "condition_id")]
    pub condition_id: String,
    pub question: String,
    pub slug: Option<String>,
    pub tokens: Option<Vec<RawGammaToken>>,
    #[serde(alias = "neg_risk")]
    pub neg_risk: Option<bool>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub outcome: Option<String>,
    #[serde(alias = "winning_outcome")]
    pub winning_outcome: Option<String>,
    #[serde(alias = "resolved_at")]
    pub resolved_at: Option<String>,
    pub category: Option<String>,
    #[serde(alias = "end_date", alias = "endDateIso", alias = "end_date_iso")]
    pub end_date: Option<String>,
    /// Whether taker fees apply for this market's tokens.
    #[serde(alias = "fees_enabled")]
    pub fees_enabled: Option<bool>,
    #[serde(alias = "fee_schedule")]
    pub fee_schedule: Option<RawFeeSchedule>,
    #[serde(alias = "minimum_order_size")]
    pub minimum_order_size: Option<Decimal>,
    #[serde(alias = "minimum_tick_size")]
    pub minimum_tick_size: Option<String>,
    #[serde(alias = "created_at")]
    pub created_at: Option<String>,
    #[serde(alias = "updated_at")]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawFeeSchedule {
    pub exponent: Option<Decimal>,
    pub rate: Option<Decimal>,
    #[serde(alias = "taker_only")]
    pub taker_only: Option<bool>,
    #[serde(alias = "rebate_rate")]
    pub rebate_rate: Option<Decimal>,
}

/// Raw token response from Gamma API.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawGammaToken {
    #[serde(alias = "token_id")]
    pub token_id: String,
    pub outcome: String,
}
