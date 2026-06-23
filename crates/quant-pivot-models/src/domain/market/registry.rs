//! Polymarket catalog persistence DTOs.

use crate::{
    domain::{MarketFeeColumns, MarketFeeSchedule},
    enums::{
        common::{CategorySet, MarketCategory, TickSize},
        market::MarketStatus,
    },
    types::{EventId, MarketId, TokenId, Usd},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Market registry conversion errors.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum MarketRegistryError {
    #[error("YES and NO token ids must differ for market {market_id}")]
    DuplicateTokenPair { market_id: MarketId },
    #[error("market {market_id} has no tokens")]
    EmptyTokenSet { market_id: MarketId },
    #[error("market {market_id} is missing a NO token")]
    MissingNoToken { market_id: MarketId },
}

/// DB row projection matching `entities::market::Model` columns exactly.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::market::Entity")]
pub struct MarketInfo {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub question: String,
    pub slug: String,
    /// Persisted category memberships (`text[]`).
    pub categories: Vec<MarketCategory>,
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
    market_id, event_id, question, slug, categories, status, outcome,
    yes_token_id, no_token_id, tick_size, neg_risk, end_date, resolved_at,
    fees_enabled, fee_rate, fee_exponent, fee_taker_only, fee_rebate_rate,
    fee_source, fee_observed_at, created_at, updated_at,
});

impl MarketInfo {
    /// Set view of the persisted category memberships.
    #[must_use]
    pub fn category_set(&self) -> CategorySet {
        CategorySet::from(self.categories.as_slice())
    }

    /// Deterministic single category for fee estimation.
    #[must_use]
    pub fn fee_category(&self) -> MarketCategory {
        self.category_set().fee_category()
    }
}

/// Upsert payload for a Polymarket market catalog row.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::market::ActiveModel")]
pub struct UpsertMarket {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub question: String,
    pub slug: String,
    pub categories: CategorySet,
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

/// Outcome token metadata held in the in-memory market registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    pub token_id: TokenId,
    pub outcome: String,
    pub neg_risk: bool,
}

/// In-memory event registry projection produced by Gamma sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRegistryInfo {
    pub event_id: EventId,
    pub title: String,
    pub slug: String,
    pub market_ids: Vec<MarketId>,
    pub categories: CategorySet,
    pub tags: Vec<String>,
    pub neg_risk: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// In-memory market registry projection produced by Gamma sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketRegistryInfo {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_yes: TokenId,
    pub token_no: TokenId,
    pub question: String,
    pub slug: String,
    pub categories: CategorySet,
    pub status: MarketStatus,
    pub outcome: Option<String>,
    pub neg_risk: bool,
    pub tick_size: TickSize,
    pub tokens: Vec<TokenInfo>,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub depth_usd: Option<Usd>,
    pub min_order_size: Decimal,
    /// Gamma-reported liquidity, when published by the upstream source.
    pub liquidity_usd: Option<Usd>,
    /// Gamma-reported trailing 24h volume when published by the upstream source.
    pub volume_24h: Option<Usd>,
    pub fee_schedule: Option<MarketFeeSchedule>,
    pub end_date: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MarketRegistryInfo {
    /// Deterministic fee category derived from the market category set.
    #[must_use]
    pub fn fee_category(&self) -> MarketCategory {
        self.categories.fee_category()
    }

    /// Resolve the YES/NO token pair represented by this registry row.
    pub fn resolve_token_pair(&self) -> Result<(TokenId, TokenId), MarketRegistryError> {
        if self.token_yes == self.token_no {
            return Err(MarketRegistryError::DuplicateTokenPair {
                market_id: self.market_id.clone(),
            });
        }
        Ok((self.token_yes.clone(), self.token_no.clone()))
    }
}

impl TryFrom<&MarketRegistryInfo> for UpsertMarket {
    type Error = MarketRegistryError;

    fn try_from(value: &MarketRegistryInfo) -> Result<Self, Self::Error> {
        Self::from_registry(value)
    }
}

impl UpsertMarket {
    /// Build a persistable upsert DTO from an in-memory Gamma registry row.
    pub fn from_registry(value: &MarketRegistryInfo) -> Result<Self, MarketRegistryError> {
        value.resolve_token_pair()?;
        let fee_columns = value.fee_schedule.as_ref().map_or_else(
            MarketFeeColumns::disabled,
            MarketFeeSchedule::to_market_fee_columns,
        );
        Ok(Self {
            market_id: value.market_id.clone(),
            event_id: value.event_id.clone(),
            question: value.question.clone(),
            slug: value.slug.clone(),
            categories: value.categories,
            status: value.status,
            outcome: value.outcome.clone(),
            yes_token_id: value.token_yes.clone(),
            no_token_id: value.token_no.clone(),
            tick_size: value.tick_size,
            neg_risk: value.neg_risk,
            end_date: value.end_date,
            resolved_at: value.resolved_at,
            fees_enabled: fee_columns.fees_enabled,
            fee_rate: fee_columns.fee_rate,
            fee_exponent: fee_columns.fee_exponent,
            fee_taker_only: fee_columns.fee_taker_only,
            fee_rebate_rate: fee_columns.fee_rebate_rate,
            fee_source: fee_columns.fee_source,
            fee_observed_at: fee_columns.fee_observed_at,
        })
    }
}

/// Resolve the YES/NO token pair from a normalized binary Polymarket market.
pub fn resolve_binary_pair_exact(
    market_id: &MarketId,
    tokens: &[TokenInfo],
) -> Result<(TokenId, TokenId), MarketRegistryError> {
    if tokens.is_empty() {
        return Err(MarketRegistryError::EmptyTokenSet {
            market_id: market_id.clone(),
        });
    }
    let Some(yes) = tokens
        .iter()
        .find(|token| token.outcome.eq_ignore_ascii_case("yes"))
        .or_else(|| tokens.first())
        .map(|token| token.token_id.clone())
    else {
        return Err(MarketRegistryError::EmptyTokenSet {
            market_id: market_id.clone(),
        });
    };
    let Some(no) = tokens
        .iter()
        .find(|token| token.outcome.eq_ignore_ascii_case("no"))
        .or_else(|| tokens.get(1))
        .map(|token| token.token_id.clone())
    else {
        return Err(MarketRegistryError::MissingNoToken {
            market_id: market_id.clone(),
        });
    };
    if yes == no {
        return Err(MarketRegistryError::DuplicateTokenPair {
            market_id: market_id.clone(),
        });
    }
    Ok((yes, no))
}

/// Collect fee schedules from in-memory market registry rows.
#[must_use]
pub fn collect_fee_data(markets: &[MarketRegistryInfo]) -> Vec<MarketFeeSchedule> {
    markets
        .iter()
        .filter_map(|market| market.fee_schedule.clone())
        .collect()
}
