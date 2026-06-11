//! Market and event registry domain models.
//!
//! These models represent the market data as ingested from Polymarket's
//! Gamma API and enriched by the data pipeline.

use crate::{
    domain::fee::MarketFeeSchedule,
    enums::{
        common::{CategorySet, MarketCategory, TickSize},
        fee::FeeSource,
        market::{EventStatus, MarketStatus},
    },
    types::{EventId, MarketId, MarketPitSnapshotId, Price, TokenId, Usd},
};
use chrono::{DateTime, Utc};
use oxide_arb_error::market::MarketError;
use rust_decimal::Decimal;
use sea_orm::{
    ActiveValue, DeriveIntoActiveModel, DerivePartialModel, FromQueryResult, IntoActiveValue,
};
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

    /// Deterministic single category for fee estimation (conservative).
    #[must_use]
    pub fn fee_category(&self) -> MarketCategory {
        self.category_set().fee_category()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::market_pit_snapshot::Entity")]
pub struct MarketPitSnapshotInfo {
    pub market_pit_snapshot_id: MarketPitSnapshotId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub question: String,
    pub slug: String,
    /// Persisted category memberships (`text[]`) captured at snapshot time.
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
    pub payload_hash: String,
    pub observed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    MarketPitSnapshotInfo,
    crate::entities::market_pit_snapshot::Model,
    {
        market_pit_snapshot_id, market_id, event_id, question, slug, categories, status, outcome,
        yes_token_id, no_token_id, tick_size, neg_risk, end_date, resolved_at, fees_enabled,
        fee_rate, fee_exponent, fee_taker_only, fee_rebate_rate, fee_source, fee_observed_at,
        payload_hash, observed_at, created_at,
    }
);

impl MarketPitSnapshotInfo {
    /// Set view of the snapshotted category memberships.
    #[must_use]
    pub fn category_set(&self) -> CategorySet {
        CategorySet::from(self.categories.as_slice())
    }
}

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::market_pit_snapshot::ActiveModel")]
pub struct NewMarketPitSnapshot {
    pub market_pit_snapshot_id: MarketPitSnapshotId,
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
    pub payload_hash: String,
    pub observed_at: DateTime<Utc>,
}

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
            categories: CategorySet::from(info.categories.as_slice()),
            status: info.status,
            outcome: info.outcome,
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
            resolved_at: info.resolved_at,
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

/// Resolve the canonical (YES, NO) token pair of a binary market.
///
/// This is the single source of truth shared by Gamma catalog mapping,
/// persistence DTO conversion, and registry registration — every consumer
/// observes the same pair by construction.
///
/// Rules (fail-closed, never guesses beyond them):
/// 1. Exactly two tokens are required — Polymarket CLOB markets are
///    structurally binary; anything else is rejected.
/// 2. When both "Yes" and "No" outcome labels are present
///    (case-insensitive), they win.
/// 3. Otherwise the pair is positional: index 0 is the YES leg, index 1 the
///    NO leg. Binary complement (`P0 + P1 = $1`) holds regardless of labels
///    (team names, Over/Under), and settlement keys on `winning_token_id`,
///    never on outcome labels, so positional assignment is safe.
pub fn resolve_binary_pair(
    market_id: &MarketId,
    tokens: &[TokenInfo],
) -> Result<(TokenId, TokenId), MarketError> {
    let pair: &[TokenInfo; 2] = tokens
        .try_into()
        .map_err(|_| MarketError::NotBinaryMarket {
            market_id: market_id.to_string(),
            token_count: tokens.len(),
        })?;
    Ok(resolve_binary_pair_exact(pair))
}

/// Infallible core of [`resolve_binary_pair`] for inputs whose binary
/// invariant is already enforced by the type system.
#[must_use]
pub fn resolve_binary_pair_exact(tokens: &[TokenInfo; 2]) -> (TokenId, TokenId) {
    let labeled_yes = tokens
        .iter()
        .find(|token| token.outcome.eq_ignore_ascii_case("yes"));
    let labeled_no = tokens
        .iter()
        .find(|token| token.outcome.eq_ignore_ascii_case("no"));

    match (labeled_yes, labeled_no) {
        (Some(yes), Some(no)) => (yes.token_id.clone(), no.token_id.clone()),
        _ => (tokens[0].token_id.clone(), tokens[1].token_id.clone()),
    }
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
    /// Every category membership derived from the parent event's Gamma tags.
    /// Single source of truth: universe filtering matches the whole set, and
    /// fee estimation collapses it via [`Self::fee_category`].
    pub categories: CategorySet,
    pub status: MarketStatus,
    pub outcome: Option<String>,
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
    pub resolved_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MarketRegistryInfo {
    /// Populate cached [`Self::token_yes`] / [`Self::token_no`] from `tokens`
    /// via the canonical [`resolve_binary_pair`] rules.
    pub fn resolve_token_pair(&mut self) -> Result<(), MarketError> {
        let (yes, no) = resolve_binary_pair(&self.market_id, &self.tokens)?;
        self.token_yes = yes;
        self.token_no = no;
        Ok(())
    }

    /// Deterministic single category for fee estimation, derived from
    /// [`Self::categories`] with the fee-conservative tie-break.
    #[must_use]
    pub fn fee_category(&self) -> MarketCategory {
        self.categories.fee_category()
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

impl TryFrom<&MarketRegistryInfo> for UpsertMarket {
    type Error = MarketError;

    fn try_from(info: &MarketRegistryInfo) -> Result<Self, Self::Error> {
        let (yes_token_id, no_token_id) = resolve_binary_pair(&info.market_id, &info.tokens)?;
        Ok(Self {
            market_id: info.market_id.clone(),
            event_id: info.event_id.clone(),
            question: info.question.clone(),
            slug: info.slug.clone(),
            categories: info.categories,
            status: info.status,
            outcome: info.outcome.clone(),
            yes_token_id,
            no_token_id,
            tick_size: info.tick_size,
            neg_risk: info.neg_risk,
            end_date: info.end_date,
            resolved_at: info.resolved_at,
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
    pub status: EventStatus,
    pub tags: Vec<String>,
    pub neg_risk: bool,
    pub end_date: Option<DateTime<Utc>>,
    pub raw_gamma: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(EventInfo, crate::entities::event::Model, {
    event_id, title, slug, status, tags, neg_risk, end_date,
    raw_gamma, created_at, updated_at,
});

impl EventInfo {
    /// Typed category memberships derived from the persisted Gamma tags.
    #[must_use]
    pub fn category_set(&self) -> CategorySet {
        CategorySet::from_slugs(self.tags.iter().map(String::as_str))
    }
}

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
    /// Every category membership derived from the Gamma tags.
    pub categories: CategorySet,
    /// Raw Gamma tag slugs (audit / rebuild source for `categories`).
    pub tags: Vec<String>,
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
    pub status: EventStatus,
    /// Raw Gamma tag slugs — the official categorization source.
    pub tags: EventTags,
    pub neg_risk: bool,
    pub end_date: Option<DateTime<Utc>>,
    pub raw_gamma: Option<serde_json::Value>,
}

/// Gamma tag slugs bound for the `event.tags` `text[]` column.
///
/// Newtype exists solely because `SeaORM` provides no
/// `IntoActiveValue<Vec<String>>` blanket impl for array columns; the orphan
/// rule forbids adding one, so the write DTO wraps the vector.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventTags(pub Vec<String>);

impl IntoActiveValue<Vec<String>> for EventTags {
    fn into_active_value(self) -> ActiveValue<Vec<String>> {
        ActiveValue::Set(self.0)
    }
}

impl From<Vec<String>> for EventTags {
    fn from(slugs: Vec<String>) -> Self {
        Self(slugs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(id: &str, outcome: &str) -> TokenInfo {
        TokenInfo {
            token_id: TokenId::new(id),
            outcome: outcome.into(),
            neg_risk: false,
        }
    }

    #[test]
    fn binary_pair_prefers_yes_no_labels_regardless_of_order() {
        let market_id = MarketId::new("0xm");
        let tokens = [token("no-leg", "No"), token("yes-leg", "YES")];
        let (yes, no) = resolve_binary_pair(&market_id, &tokens).expect("labeled pair");
        assert_eq!(yes.as_str(), "yes-leg");
        assert_eq!(no.as_str(), "no-leg");
    }

    #[test]
    fn binary_pair_falls_back_to_positional_for_custom_outcomes() {
        let market_id = MarketId::new("0xm");
        let tokens = [token("t1", "Team A"), token("t2", "Team B")];
        let (yes, no) = resolve_binary_pair(&market_id, &tokens).expect("positional pair");
        assert_eq!(yes.as_str(), "t1");
        assert_eq!(no.as_str(), "t2");
    }

    #[test]
    fn binary_pair_is_positional_when_only_one_leg_is_labeled() {
        let market_id = MarketId::new("0xm");
        let tokens = [token("t1", "Over"), token("t2", "No")];
        let (yes, no) = resolve_binary_pair(&market_id, &tokens).expect("positional pair");
        assert_eq!(yes.as_str(), "t1");
        assert_eq!(no.as_str(), "t2");
    }

    #[test]
    fn non_binary_token_counts_are_rejected() {
        let market_id = MarketId::new("0xm");
        for tokens in [
            Vec::new(),
            vec![token("t1", "Yes")],
            vec![token("t1", "A"), token("t2", "B"), token("t3", "C")],
        ] {
            let error = resolve_binary_pair(&market_id, &tokens).expect_err("must reject");
            assert!(matches!(
                error,
                MarketError::NotBinaryMarket { token_count, .. } if token_count == tokens.len()
            ));
        }
    }
}
