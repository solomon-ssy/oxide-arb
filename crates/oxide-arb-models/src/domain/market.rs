//! Market and event registry domain models.
//!
//! These models represent the market data as ingested from Polymarket's
//! Gamma API and enriched by the data pipeline.

use crate::{
    domain::fee::MarketFeeSchedule,
    enums::{
        common::{MarketCategory, TickSize},
        fee::FeeSource,
        market::{EventStatus, MarketStatus},
    },
    types::{EventId, MarketId, Price, TokenId, Usd},
};
use chrono::{DateTime, Utc};
use oxide_arb_error::market::MarketError;
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

// ── Market read models ──────────────────────────────────────────────

/// DB row projection matching `entities::market::Model` columns exactly.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::market::Entity")]
pub struct MarketInfo {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub question: String,
    pub slug: String,
    pub category: MarketCategory,
    pub status: MarketStatus,
    pub outcome: Option<String>,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub tick_size: TickSize,
    pub neg_risk: bool,
    pub end_date: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub fees_enabled: bool,
    pub fee_rate: Option<Decimal>,
    pub fee_exponent: Option<Decimal>,
    pub fee_taker_only: Option<bool>,
    pub fee_rebate_rate: Option<Decimal>,
    pub fee_source: Option<String>,
    pub fee_observed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(MarketInfo, crate::entities::market::Model, {
    market_id, event_id, question, slug, category, status, outcome,
    yes_token_id, no_token_id, tick_size, neg_risk, end_date, resolved_at,
    fees_enabled, fee_rate, fee_exponent, fee_taker_only, fee_rebate_rate,
    fee_source, fee_observed_at, created_at, updated_at,
});

impl From<MarketInfo> for MarketRegistryInfo {
    fn from(info: MarketInfo) -> Self {
        let tokens = vec![
            TokenInfo {
                token_id: info.yes_token_id.clone(),
                outcome: "Yes".to_owned(),
                neg_risk: info.neg_risk,
            },
            TokenInfo {
                token_id: info.no_token_id.clone(),
                outcome: "No".to_owned(),
                neg_risk: info.neg_risk,
            },
        ];
        let fee_schedule = match (info.fee_rate, info.fee_exponent) {
            (Some(fee_rate), Some(exponent)) => Some(MarketFeeSchedule {
                market_id: info.market_id.clone(),
                fees_enabled: info.fees_enabled,
                fee_rate,
                exponent,
                taker_only: info.fee_taker_only.unwrap_or(true),
                rebate_rate: info.fee_rebate_rate,
                source: FeeSource::GammaFeeSchedule,
                observed_at: info.fee_observed_at.unwrap_or(info.updated_at),
            }),
            _ => None,
        };

        Self {
            market_id: info.market_id,
            event_id: info.event_id,
            token_yes: info.yes_token_id.clone(),
            token_no: info.no_token_id.clone(),
            question: info.question,
            slug: info.slug,
            category: info.category,
            status: info.status,
            neg_risk: info.neg_risk,
            tick_size: info.tick_size,
            tokens,
            best_bid: None,
            best_ask: None,
            depth_usd: None,
            min_order_size: Decimal::ZERO,
            volume_24h: Usd::ZERO,
            fee_schedule,
            end_date: info.end_date,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

/// A single conditional token within a market.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    pub token_id: TokenId,
    pub outcome: String,
    pub neg_risk: bool,
}

/// In-memory enriched market view with runtime orderbook fields.
///
/// Replaces the old `MarketEntry`. Not persisted directly; convert to
/// `UpsertMarket` for persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketRegistryInfo {
    pub market_id: MarketId,
    pub event_id: EventId,
    /// Cached at registration — hot-path lookups avoid scanning `tokens`.
    pub token_yes: TokenId,
    pub token_no: TokenId,
    pub question: String,
    pub slug: String,
    pub category: MarketCategory,
    pub status: MarketStatus,
    pub neg_risk: bool,
    pub tick_size: TickSize,
    pub tokens: Vec<TokenInfo>,
    pub best_bid: Option<Price>,
    pub best_ask: Option<Price>,
    pub depth_usd: Option<Usd>,
    pub min_order_size: Decimal,
    pub volume_24h: Usd,
    pub fee_schedule: Option<MarketFeeSchedule>,
    pub end_date: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MarketRegistryInfo {
    /// Populate cached [`Self::token_yes`] / [`Self::token_no`] from `tokens`.
    pub fn resolve_token_pair(&mut self) -> Result<(), MarketError> {
        let (yes, no) = self.yes_no_tokens()?;
        self.token_yes = yes;
        self.token_no = no;
        Ok(())
    }

    /// Resolve YES/NO token IDs using outcome labels (case-insensitive).
    ///
    /// Fails closed when either leg is missing — never guesses by token order.
    pub fn yes_no_tokens(&self) -> Result<(TokenId, TokenId), MarketError> {
        let yes = self
            .tokens
            .iter()
            .find(|t| t.outcome.eq_ignore_ascii_case("yes"))
            .map(|t| t.token_id.clone());
        let no = self
            .tokens
            .iter()
            .find(|t| t.outcome.eq_ignore_ascii_case("no"))
            .map(|t| t.token_id.clone());

        match (yes, no) {
            (Some(y), Some(n)) => Ok((y, n)),
            _ => Err(MarketError::InvalidTokenPair {
                market_id: self.market_id.to_string(),
            }),
        }
    }
}

// ── Market write DTOs ───────────────────────────────────────────────

/// Upsert payload for the `market` table. Derives `DeriveIntoActiveModel`.
///
/// Insert defaults may prepare `created_at`; DB defaults and triggers own
/// database-managed write timestamps.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::market::ActiveModel")]
pub struct UpsertMarket {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub question: String,
    pub slug: String,
    pub category: MarketCategory,
    pub status: MarketStatus,
    pub outcome: Option<String>,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub tick_size: TickSize,
    pub neg_risk: bool,
    pub end_date: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub fees_enabled: bool,
    pub fee_rate: Option<Decimal>,
    pub fee_exponent: Option<Decimal>,
    pub fee_taker_only: Option<bool>,
    pub fee_rebate_rate: Option<Decimal>,
    pub fee_source: Option<String>,
    pub fee_observed_at: Option<DateTime<Utc>>,
}

impl TryFrom<&MarketRegistryInfo> for UpsertMarket {
    type Error = MarketError;

    fn try_from(info: &MarketRegistryInfo) -> Result<Self, Self::Error> {
        let (yes_token_id, no_token_id) = info.yes_no_tokens()?;
        Ok(Self {
            market_id: info.market_id.clone(),
            event_id: info.event_id.clone(),
            question: info.question.clone(),
            slug: info.slug.clone(),
            category: info.category,
            status: info.status,
            outcome: None,
            yes_token_id,
            no_token_id,
            tick_size: info.tick_size,
            neg_risk: info.neg_risk,
            end_date: info.end_date,
            resolved_at: None,
            fees_enabled: info
                .fee_schedule
                .as_ref()
                .is_none_or(|schedule| schedule.fees_enabled),
            fee_rate: info.fee_schedule.as_ref().map(|schedule| schedule.fee_rate),
            fee_exponent: info.fee_schedule.as_ref().map(|schedule| schedule.exponent),
            fee_taker_only: info
                .fee_schedule
                .as_ref()
                .map(|schedule| schedule.taker_only),
            fee_rebate_rate: info
                .fee_schedule
                .as_ref()
                .and_then(|schedule| schedule.rebate_rate),
            fee_source: info
                .fee_schedule
                .as_ref()
                .map(|schedule| schedule.source.as_str().to_owned()),
            fee_observed_at: info
                .fee_schedule
                .as_ref()
                .map(|schedule| schedule.observed_at),
        })
    }
}

/// Market fee schedules cached in registry entries.
pub fn collect_fee_data(entries: &[MarketRegistryInfo]) -> Vec<MarketFeeSchedule> {
    entries
        .iter()
        .filter_map(|entry| entry.fee_schedule.clone())
        .collect()
}

// ── Event read models ───────────────────────────────────────────────

/// DB row projection matching `entities::event::Model` columns exactly.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::event::Entity")]
pub struct EventInfo {
    pub event_id: EventId,
    pub title: String,
    pub slug: String,
    pub category: MarketCategory,
    pub status: EventStatus,
    pub neg_risk: bool,
    pub end_date: Option<DateTime<Utc>>,
    pub raw_gamma: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(EventInfo, crate::entities::event::Model, {
    event_id, title, slug, category, status, neg_risk, end_date,
    raw_gamma, created_at, updated_at,
});

/// In-memory enriched event view with associated market IDs.
///
/// Replaces the old `EventEntry`. Not persisted directly; convert to
/// `UpsertEvent` for persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRegistryInfo {
    pub event_id: EventId,
    pub title: String,
    pub slug: String,
    pub market_ids: Vec<MarketId>,
    pub neg_risk: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ── Event write DTOs ────────────────────────────────────────────────

/// Upsert payload for the `event` table. Derives `DeriveIntoActiveModel`.
///
/// Insert defaults may prepare `created_at`; DB defaults and triggers own
/// database-managed write timestamps.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::event::ActiveModel")]
pub struct UpsertEvent {
    pub event_id: EventId,
    pub title: String,
    pub slug: String,
    pub category: MarketCategory,
    pub status: EventStatus,
    pub neg_risk: bool,
    pub end_date: Option<DateTime<Utc>>,
    pub raw_gamma: Option<serde_json::Value>,
}
