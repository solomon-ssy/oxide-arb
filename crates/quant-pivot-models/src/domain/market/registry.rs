//! Polymarket catalog persistence DTOs.

use std::sync::Arc;

use crate::{
    domain::{MarketFeeColumns, MarketFeeSchedule},
    enums::{
        common::{CategorySet, MarketCategory, TickSize},
        fee::FeeSource,
        market::MarketStatus,
    },
    types::{EventId, MarketId, TokenId, Usd},
};
use chrono::{DateTime, Utc};
use quant_pivot_error::market::MarketError;
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

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
    pub fee_source: Option<FeeSource>,
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
    pub fee_source: Option<FeeSource>,
    pub fee_observed_at: Option<DateTime<Utc>>,
}

/// Outcome token metadata held in the in-memory market registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    pub token_id: TokenId,
    pub outcome: String,
    pub neg_risk: bool,
}

/// One YES leg of a neg-risk event (Phase 11.2.1).
///
/// Deterministically ordered by `(market_id, yes_token_id)` when enumerated, so
/// full-leg structural aggregates hash and replay identically online and offline.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NegRiskLeg {
    /// The sibling market that owns this YES leg.
    pub market_id: MarketId,
    /// The YES outcome token of the sibling leg.
    pub yes_token_id: TokenId,
}

/// Expected vs resolved neg-risk YES legs for an event (Phase 11.2.1 PIT).
///
/// `expected_legs` is the count of **neg-risk outcome markets** in the event
/// (not `event.market_ids.len()` — binary / non-neg-risk members are excluded).
/// `legs` holds the registry-resolvable subset with YES tokens. When
/// `legs.len() < expected_legs`, structural features fail closed with
/// [`NullReason::LegBookMissing`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NegRiskLegSet {
    /// Expected neg-risk YES-leg count for this event.
    pub expected_legs: usize,
    /// Resolved YES legs present in the registry at enumeration time.
    pub legs: Vec<NegRiskLeg>,
}

impl NegRiskLegSet {
    /// Empty set for binary / unknown events.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            expected_legs: 0,
            legs: Vec::new(),
        }
    }

    /// Build from the catalog markets of one neg-risk event (offline PG parity).
    ///
    /// `expected_legs` equals the number of `neg_risk` rows returned by
    /// `find_by_event`; non-neg-risk event members are not structural legs.
    #[must_use]
    pub fn from_event_catalog(markets: &[Arc<MarketInfo>]) -> Self {
        let mut legs: Vec<NegRiskLeg> = markets
            .iter()
            .filter(|market| market.neg_risk)
            .map(|market| NegRiskLeg {
                market_id: market.market_id.clone(),
                yes_token_id: market.yes_token_id.clone(),
            })
            .collect();
        legs.sort();
        Self {
            expected_legs: legs.len(),
            legs,
        }
    }
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
    pub fn resolve_token_pair(&self) -> Result<(TokenId, TokenId), MarketError> {
        if self.token_yes == self.token_no {
            return Err(MarketError::DuplicateTokenPair {
                market_id: self.market_id.to_string(),
            });
        }
        Ok((self.token_yes.clone(), self.token_no.clone()))
    }
}

impl TryFrom<&MarketRegistryInfo> for UpsertMarket {
    type Error = MarketError;

    fn try_from(value: &MarketRegistryInfo) -> Result<Self, Self::Error> {
        Self::from_registry(value)
    }
}

impl UpsertMarket {
    /// Build a persistable upsert DTO from an in-memory Gamma registry row.
    pub fn from_registry(value: &MarketRegistryInfo) -> Result<Self, MarketError> {
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
) -> Result<(TokenId, TokenId), MarketError> {
    let market_id = market_id.to_string();
    let empty_tokens = || MarketError::EmptyTokenSet {
        market_id: market_id.clone(),
    };
    if tokens.is_empty() {
        return Err(empty_tokens());
    }
    let Some(yes) = tokens
        .iter()
        .find(|token| token.outcome.eq_ignore_ascii_case("yes"))
        .or_else(|| tokens.first())
        .map(|token| token.token_id.clone())
    else {
        return Err(empty_tokens());
    };
    let Some(no) = tokens
        .iter()
        .find(|token| token.outcome.eq_ignore_ascii_case("no"))
        .or_else(|| tokens.get(1))
        .map(|token| token.token_id.clone())
    else {
        return Err(MarketError::MissingNoToken {
            market_id: market_id.clone(),
        });
    };
    if yes == no {
        return Err(MarketError::DuplicateTokenPair { market_id });
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

#[cfg(test)]
mod neg_risk_leg_set_tests {
    use std::sync::Arc;

    use chrono::Utc;

    use super::{MarketInfo, NegRiskLegSet};
    use crate::{
        enums::{
            common::{MarketCategory, TickSize},
            market::MarketStatus,
        },
        types::{EventId, MarketId, TokenId},
    };

    fn catalog_market(id: &str, neg_risk: bool) -> Arc<MarketInfo> {
        let now = Utc::now();
        Arc::new(MarketInfo {
            market_id: MarketId::new(id),
            event_id: EventId::new("evt-1"),
            question: "Q?".into(),
            slug: id.into(),
            categories: vec![MarketCategory::Crypto],
            status: MarketStatus::Active,
            outcome: None,
            yes_token_id: TokenId::new(format!("{id}-yes")),
            no_token_id: TokenId::new(format!("{id}-no")),
            tick_size: TickSize::Hundredth,
            neg_risk,
            end_date: None,
            resolved_at: None,
            fees_enabled: false,
            fee_rate: None,
            fee_exponent: None,
            fee_taker_only: None,
            fee_rebate_rate: None,
            fee_source: None,
            fee_observed_at: None,
            created_at: now,
            updated_at: now,
        })
    }

    #[test]
    fn from_event_catalog_counts_only_neg_risk_markets() {
        let set = NegRiskLegSet::from_event_catalog(&[
            catalog_market("m-yes-1", true),
            catalog_market("m-yes-2", true),
            catalog_market("m-binary", false),
        ]);
        assert_eq!(set.expected_legs, 2);
        assert_eq!(set.legs.len(), 2);
        assert!(
            set.legs
                .iter()
                .all(|leg| leg.market_id.as_str().starts_with("m-yes"))
        );
    }
}
