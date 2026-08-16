//! Polymarket catalog persistence DTOs.

use chrono::{DateTime, Utc};
use quant_pivot_error::market::MarketError;
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    domain::market::{fee::MarketMakerRebateSchedule, version::CATALOG_OBJECT_HASH_VERSION},
    enums::{
        catalog::{CatalogFilterReason, CatalogFilterReasonSet},
        common::{CategorySet, MarketCategory, TickSize},
        market::{EventStatus, MarketStatus},
    },
    hashing::CanonicalDigest,
    types::{ContentHash, EventId, MarketId, TokenId, Usd},
};

/// DB row projection matching `entities::market::Model` columns exactly.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::market::Entity")]
pub struct MarketInfo {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub question: String,
    pub slug: String,
    /// Market rules text used as a resolution-source grounding anchor.
    pub description: Option<String>,
    /// Persisted category memberships (`text[]`).
    pub categories: Vec<MarketCategory>,
    pub status: MarketStatus,
    pub filter_reasons: Vec<CatalogFilterReason>,
    pub outcome: Option<String>,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub tick_size: TickSize,
    pub neg_risk: bool,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub content_hash: ContentHash,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(MarketInfo, crate::entities::market::Model, {
    market_id, event_id, question, slug, description, categories, status, filter_reasons, outcome,
    yes_token_id, no_token_id, tick_size, neg_risk, start_date, end_date, resolved_at,
    content_hash, created_at, updated_at,
});

impl MarketInfo {
    /// Set view of the persisted category memberships.
    #[must_use]
    pub fn category_set(&self) -> CategorySet {
        CategorySet::from(self.categories.as_slice())
    }

    /// Deterministic primary category for single-cohort consumers.
    #[must_use]
    pub fn primary_category(&self) -> MarketCategory {
        self.category_set().primary_category()
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
    pub description: Option<String>,
    pub categories: CategorySet,
    pub status: MarketStatus,
    pub filter_reasons: CatalogFilterReasonSet,
    pub outcome: Option<String>,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub tick_size: TickSize,
    pub neg_risk: bool,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub content_hash: ContentHash,
}

/// Outcome token metadata held in the in-memory market registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    pub token_id: TokenId,
    pub outcome: String,
    pub neg_risk: bool,
}

/// One YES leg of a neg-risk event.
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

/// Expected versus resolved neg-risk YES legs at a point-in-time boundary.
///
/// `expected_legs` is the count of **neg-risk outcome markets** listed in the
/// event's Gamma catalog snapshot (`EventRegistryInfo.market_ids` online,
/// `EventInfo.catalog_market_ids` offline). Non-neg-risk catalog members are
/// excluded. `legs` holds the materialized subset with YES tokens. When
/// `legs.len < expected_legs`, structural features fail closed with
/// `NullReason::LegBookMissing`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NegRiskLegSet {
    /// Expected neg-risk YES-leg count for this event.
    pub expected_legs: usize,
    /// Resolved YES legs present in the registry at enumeration time.
    pub legs: Vec<NegRiskLeg>,
}

/// Materialized market metadata needed to resolve one catalog member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogMarketLeg {
    /// A neg-risk market with its YES token.
    NegRisk { yes_token_id: TokenId },
    /// A catalog member that is not a structural neg-risk leg (e.g. binary).
    NonNegRisk,
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

    /// Build from the event's Gamma catalog snapshot and a market resolver.
    ///
    /// `catalog_market_ids` preserves Gamma sync order. `resolve` returns:
    /// - `Some(CatalogMarketLeg::NegRisk {.. })` when the market is materialized
    ///   and `neg_risk`;
    /// - `Some(CatalogMarketLeg::NonNegRisk)` when materialized but not neg-risk;
    /// - `None` when the catalog member is not yet materialized (still expected).
    #[must_use]
    pub fn from_catalog<F>(catalog_market_ids: &[MarketId], mut resolve: F) -> Self
    where
        F: FnMut(&MarketId) -> Option<CatalogMarketLeg>,
    {
        let mut expected_legs = 0_usize;
        let mut legs = Vec::new();
        for market_id in catalog_market_ids {
            match resolve(market_id) {
                Some(CatalogMarketLeg::NegRisk { yes_token_id }) => {
                    expected_legs += 1;
                    legs.push(NegRiskLeg {
                        market_id: market_id.clone(),
                        yes_token_id,
                    });
                }
                Some(CatalogMarketLeg::NonNegRisk) => {}
                None => {
                    // Catalogued outcome not yet materialized — still expected so
                    // missing PIT books fail closed (online/offline identical).
                    expected_legs += 1;
                }
            }
        }
        legs.sort();
        Self {
            expected_legs,
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
    /// Recurring-series slug (Tier-0 linkage anchor), when present.
    pub series_slug: Option<String>,
    pub status: EventStatus,
    pub market_ids: Vec<MarketId>,
    pub categories: CategorySet,
    pub tags: Vec<String>,
    pub neg_risk: bool,
    pub end_date: Option<DateTime<Utc>>,
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
    /// Market rules text used as a resolution-source grounding anchor.
    pub description: Option<String>,
    pub categories: CategorySet,
    pub status: MarketStatus,
    /// Explicit upstream facts that make this otherwise valid market non-tradeable.
    pub filter_reasons: CatalogFilterReasonSet,
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
    /// Gamma-sourced delayed maker-incentive fact. Missing means unavailable,
    /// never a synthesized zero-rate schedule.
    pub maker_rebate_schedule: Option<MarketMakerRebateSchedule>,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    /// Upstream creation time. Missing remains `None`; age features must not
    /// substitute local ingestion time.
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl MarketRegistryInfo {
    /// Deterministic primary category derived from the category set.
    #[must_use]
    pub fn primary_category(&self) -> MarketCategory {
        self.categories.primary_category()
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
        Ok(Self {
            market_id: value.market_id.clone(),
            event_id: value.event_id.clone(),
            question: value.question.clone(),
            slug: value.slug.clone(),
            description: value.description.clone(),
            categories: value.categories,
            status: value.status,
            filter_reasons: value.filter_reasons,
            outcome: value.outcome.clone(),
            yes_token_id: value.token_yes.clone(),
            no_token_id: value.token_no.clone(),
            tick_size: value.tick_size,
            neg_risk: value.neg_risk,
            start_date: value.start_date,
            end_date: value.end_date,
            resolved_at: value.resolved_at,
            content_hash: CanonicalDigest::content_hash_typed(
                "quant-pivot/catalog-market-object",
                CATALOG_OBJECT_HASH_VERSION,
                value,
            )
            .map_err(|error| MarketError::CatalogSerialization {
                entity: "market",
                id: value.market_id.to_string(),
                reason: error.to_string(),
            })?,
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
