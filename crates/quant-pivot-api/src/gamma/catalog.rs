//! Normalized Gamma catalog rows — validated wire → mapper input.

use super::wire::{WireEvent, WireFeeSchedule, WireMarket};
use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::{
        common::{CategorySet, TickSize},
        market::{EventStatus, MarketStatus},
    },
    types::Usd,
};
use rust_decimal::Decimal;
use serde_json::Value;
use thiserror::Error;

/// Tradeable token row after Gamma wire normalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogToken {
    pub token_id: String,
    pub outcome: String,
}

/// Settlement conclusion derived from `outcomePrices` once UMA resolves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSettlement {
    /// The winning leg's CLOB token id.
    pub winning_token_id: String,
    /// The winning leg's outcome label (team name, Over/Under, Yes/No, …).
    pub winning_outcome: String,
}

/// Market row ready for domain mapping and persistence.
///
/// Construction via [`TryFrom<WireMarket>`] enforces every catalog invariant
/// (non-empty `condition_id`, exactly two tokens), so downstream mapping is
/// infallible by design.
#[derive(Debug, Clone)]
pub struct CatalogMarket {
    pub condition_id: String,
    pub question: String,
    pub slug: Option<String>,
    /// Exactly two legs — the binary invariant is encoded in the type.
    pub tokens: [CatalogToken; 2],
    pub status: MarketStatus,
    pub neg_risk: bool,
    /// Settlement conclusion; present only when `closed` and
    /// `umaResolutionStatus == "resolved"` with an unambiguous `"1"` price.
    pub settlement: Option<CatalogSettlement>,
    /// Settlement close time (`closedTime`); only set on settled markets.
    pub resolved_at: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    pub fees_enabled: bool,
    pub fee_schedule: Option<WireFeeSchedule>,
    pub min_order_size: Decimal,
    /// Gamma-reported liquidity in USD (`liquidityNum`), when published.
    pub liquidity_usd: Option<Usd>,
    /// Gamma-reported trailing 24h volume in USD (`volume24hr`); absent when upstream omits the field.
    pub volume_24h_usd: Option<Usd>,
    pub tick_size: TickSize,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

/// Event row with embeddable markets that survived normalization.
#[derive(Debug, Clone)]
pub struct CatalogEvent {
    pub id: String,
    pub title: String,
    pub slug: String,
    pub status: EventStatus,
    /// Raw Gamma tag slugs — the official categorization source.
    pub tags: Vec<String>,
    /// Category memberships derived from `tags`.
    pub categories: CategorySet,
    pub neg_risk: bool,
    pub end_date: Option<DateTime<Utc>>,
    pub markets: Vec<CatalogMarket>,
    /// Embedded markets dropped during normalization (for sync summaries).
    pub rejected_markets: Vec<RejectedMarket>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    /// Original wire payload for `event.raw_gamma` audit storage.
    pub raw_wire: Value,
}

/// A market dropped during catalog normalization, kept for sync summaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedMarket {
    /// `condition_id` as received (may be empty for `EmptyConditionId`).
    pub condition_id: String,
    pub reject: CatalogMarketReject,
}

/// Why a single embedded market was dropped during catalog normalization.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CatalogMarketReject {
    #[error("empty condition_id")]
    EmptyConditionId,
    #[error("missing clob_token_ids")]
    MissingClobTokenIds,
    #[error("{token_count} tokens — Polymarket CLOB markets must be binary")]
    NotBinary { token_count: usize },
    #[error("invalid binary token pair: {reason}")]
    InvalidTokenPair { reason: String },
}

impl CatalogMarketReject {
    /// Stable label for metrics partitioning.
    #[must_use]
    pub const fn reason_label(&self) -> &'static str {
        match self {
            Self::EmptyConditionId => "empty_condition_id",
            Self::MissingClobTokenIds => "missing_clob_token_ids",
            Self::NotBinary { .. } => "not_binary",
            Self::InvalidTokenPair { .. } => "invalid_token_pair",
        }
    }
}

impl From<WireEvent> for CatalogEvent {
    fn from(wire: WireEvent) -> Self {
        let raw_wire = serde_json::to_value(&wire).unwrap_or(Value::Null);
        let end_date = wire.end_date();
        let tags = wire.tag_slugs();
        let categories = CategorySet::from_slugs(tags.iter().map(String::as_str));
        let status = if wire.closed.unwrap_or(false) {
            EventStatus::Closed
        } else {
            EventStatus::Active
        };
        let id = wire.id;
        let title = wire.title.unwrap_or_default();
        let slug = wire.slug.unwrap_or_default();
        let neg_risk = wire.neg_risk.unwrap_or(false);
        let created_at = parse_gamma_timestamp(wire.created_at.as_deref());
        let updated_at = parse_gamma_timestamp(wire.updated_at.as_deref());
        let wire_markets = wire.markets.unwrap_or_default();
        let mut markets = Vec::with_capacity(wire_markets.len());
        let mut rejected_markets = Vec::new();
        for market in wire_markets {
            let condition_id = market.condition_id.clone();
            match CatalogMarket::try_from(market) {
                Ok(row) => markets.push(row),
                Err(reject) => {
                    tracing::debug!(
                        market_id = %condition_id,
                        error = %reject,
                        "skipping non-catalog gamma market"
                    );
                    rejected_markets.push(RejectedMarket {
                        condition_id,
                        reject,
                    });
                }
            }
        }

        Self {
            id,
            title,
            slug,
            status,
            tags,
            categories,
            neg_risk,
            end_date,
            markets,
            rejected_markets,
            created_at,
            updated_at,
            raw_wire,
        }
    }
}

impl TryFrom<WireMarket> for CatalogMarket {
    type Error = CatalogMarketReject;

    fn try_from(wire: WireMarket) -> Result<Self, Self::Error> {
        if wire.condition_id.trim().is_empty() {
            return Err(CatalogMarketReject::EmptyConditionId);
        }
        if wire.clob_token_ids.is_empty() {
            return Err(CatalogMarketReject::MissingClobTokenIds);
        }
        let [yes_id, no_id] = wire.clob_token_ids.as_slice() else {
            return Err(CatalogMarketReject::NotBinary {
                token_count: wire.clob_token_ids.as_slice().len(),
            });
        };

        let tokens = zip_tokens([yes_id, no_id], wire.outcomes.as_slice());
        let tick_size = wire
            .order_price_min_tick_size
            .map_or(TickSize::Hundredth, |decimal| {
                TickSize::try_from(decimal).unwrap_or(TickSize::Hundredth)
            });
        let end_date = wire.end_date();
        let closed = wire.closed.unwrap_or(false);
        let status = market_status_from_wire(closed, wire.active.unwrap_or(true));
        let settlement = derive_settlement(&wire, &tokens);
        let resolved_at = if status == MarketStatus::Settled {
            parse_gamma_timestamp(wire.closed_time.as_deref())
        } else {
            None
        };

        Ok(Self {
            condition_id: wire.condition_id,
            question: wire.question,
            slug: wire.slug,
            tokens,
            status,
            neg_risk: wire.neg_risk.unwrap_or(false),
            settlement,
            resolved_at,
            end_date,
            fees_enabled: wire.fees_enabled.unwrap_or(true),
            fee_schedule: wire.fee_schedule,
            min_order_size: wire.order_min_size.unwrap_or(Decimal::ONE),
            liquidity_usd: wire.liquidity_num.map(Usd::new),
            volume_24h_usd: wire.volume_24hr.map(Usd::new),
            tick_size,
            created_at: parse_gamma_timestamp(wire.created_at.as_deref()),
            updated_at: parse_gamma_timestamp(wire.updated_at.as_deref()),
        })
    }
}

/// Derive the settlement conclusion from `outcomePrices` (fail-closed).
///
/// Gamma payloads carry no `outcome` / `winningOutcome` field — the
/// conclusion is encoded as a `"1"` settlement price on the winning leg.
/// The derivation only fires when the market is `closed`, UMA reports
/// `resolved`, prices align one-to-one with the tokens, and exactly one
/// price equals 1. Anything ambiguous yields `None` — never guess money.
fn derive_settlement(wire: &WireMarket, tokens: &[CatalogToken; 2]) -> Option<CatalogSettlement> {
    if !wire.closed.unwrap_or(false) {
        return None;
    }
    let uma_resolved = wire
        .uma_resolution_status
        .as_deref()
        .is_some_and(|status| status.trim().eq_ignore_ascii_case("resolved"));
    if !uma_resolved {
        return None;
    }

    let prices = wire.outcome_prices.as_slice();
    if prices.len() != tokens.len() {
        return None;
    }
    let mut winners = prices.iter().enumerate().filter_map(|(index, raw)| {
        let price = raw.trim().parse::<Decimal>().ok()?;
        (price == Decimal::ONE).then_some(index)
    });
    let winner = winners.next()?;
    if winners.next().is_some() {
        return None;
    }

    let token = &tokens[winner];
    Some(CatalogSettlement {
        winning_token_id: token.token_id.clone(),
        winning_outcome: token.outcome.clone(),
    })
}

fn zip_tokens(token_ids: [&String; 2], outcomes: &[String]) -> [CatalogToken; 2] {
    let leg = |index: usize, fallback: &str| CatalogToken {
        token_id: token_ids[index].clone(),
        outcome: outcomes
            .get(index)
            .cloned()
            .unwrap_or_else(|| fallback.to_owned()),
    };
    [leg(0, "Yes"), leg(1, "No")]
}

const fn market_status_from_wire(closed: bool, active: bool) -> MarketStatus {
    if closed {
        MarketStatus::Settled
    } else if active {
        MarketStatus::Active
    } else {
        MarketStatus::Paused
    }
}

/// Parse Gamma timestamps leniently.
///
/// Gamma mixes RFC3339 (`2026-06-11T04:05:01Z`) and a space-separated
/// Postgres-style format (`2026-06-11 04:05:01+00`, used by `closedTime`).
fn parse_gamma_timestamp(raw: Option<&str>) -> Option<DateTime<Utc>> {
    let trimmed = raw.map(str::trim).filter(|value| !value.is_empty())?;
    if let Ok(parsed) = trimmed.parse::<DateTime<Utc>>() {
        return Some(parsed);
    }
    DateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S%#z")
        .ok()
        .map(|parsed| parsed.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::{CatalogEvent, CatalogMarket, CatalogMarketReject};
    use crate::gamma::wire::{WireEvent, WireMarket};
    use quant_pivot_models::{
        enums::{
            common::MarketCategory,
            market::{EventStatus, MarketStatus},
        },
        types::Usd,
    };

    #[test]
    fn skips_market_with_empty_condition_id() {
        let wire: WireEvent = serde_json::from_str(
            r#"{
                "id": "1",
                "title": "E",
                "slug": "e",
                "markets": [{
                    "conditionId": "",
                    "question": "pending?",
                    "clobTokenIds": ["1"],
                    "outcomes": ["Yes"]
                }, {
                    "conditionId": "0xabc",
                    "question": "live?",
                    "clobTokenIds": ["2", "3"],
                    "outcomes": ["Yes", "No"]
                }]
            }"#,
        )
        .expect("wire event");
        let event = CatalogEvent::from(wire);
        assert_eq!(event.markets.len(), 1);
        assert_eq!(event.markets[0].condition_id, "0xabc");
        assert_eq!(event.rejected_markets.len(), 1);
    }

    #[test]
    fn rejects_market_without_clob_token_ids() {
        let wire: WireEvent = serde_json::from_str(
            r#"{
                "id": "1",
                "markets": [{
                    "conditionId": "0xabc",
                    "question": "Q?"
                }]
            }"#,
        )
        .expect("wire");
        let event = CatalogEvent::from(wire);
        assert!(event.markets.is_empty());
        assert!(matches!(
            event.rejected_markets[0].reject,
            CatalogMarketReject::MissingClobTokenIds
        ));
    }

    #[test]
    fn empty_condition_id_maps_to_reject_reason() {
        let wire: WireMarket = serde_json::from_str(
            r#"{"conditionId": "", "question": "?", "clobTokenIds": ["1"], "outcomes": ["Yes"]}"#,
        )
        .expect("wire market");
        let err = CatalogMarket::try_from(wire).expect_err("reject");
        assert!(matches!(err, CatalogMarketReject::EmptyConditionId));
    }

    #[test]
    fn non_binary_token_count_is_rejected() {
        let wire: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0xabc",
                "question": "?",
                "clobTokenIds": ["1", "2", "3"],
                "outcomes": ["A", "B", "C"]
            }"#,
        )
        .expect("wire market");
        let err = CatalogMarket::try_from(wire).expect_err("reject");
        assert!(matches!(
            err,
            CatalogMarketReject::NotBinary { token_count: 3 }
        ));
    }

    #[test]
    fn custom_outcome_binary_market_is_accepted() {
        let wire: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0xabc",
                "question": "Team A vs Team B?",
                "clobTokenIds": ["11", "22"],
                "outcomes": ["Team A", "Team B"]
            }"#,
        )
        .expect("wire market");
        let market = CatalogMarket::try_from(wire).expect("binary market accepted");
        assert_eq!(market.tokens[0].outcome, "Team A");
        assert_eq!(market.tokens[1].outcome, "Team B");
    }

    #[test]
    fn settlement_derived_from_outcome_prices_when_resolved() {
        let wire: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0xdone",
                "question": "Over/Under?",
                "closed": true,
                "active": false,
                "umaResolutionStatus": "resolved",
                "closedTime": "2026-06-11 04:05:01+00",
                "clobTokenIds": ["111", "222"],
                "outcomes": ["Over", "Under"],
                "outcomePrices": ["0", "1"]
            }"#,
        )
        .expect("wire market");
        let market = CatalogMarket::try_from(wire).expect("settled market");
        let settlement = market.settlement.expect("settlement derived");
        assert_eq!(settlement.winning_token_id, "222");
        assert_eq!(settlement.winning_outcome, "Under");
        assert!(market.resolved_at.is_some(), "closedTime must parse");
        assert_eq!(market.status, MarketStatus::Settled);
    }

    #[test]
    fn settlement_fails_closed_on_ambiguity() {
        // Not UMA-resolved.
        let unresolved: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0x1", "question": "?", "closed": true,
                "clobTokenIds": ["1", "2"], "outcomes": ["Yes", "No"],
                "outcomePrices": ["0", "1"]
            }"#,
        )
        .expect("wire");
        assert!(
            CatalogMarket::try_from(unresolved)
                .expect("market")
                .settlement
                .is_none()
        );

        // Two winning prices.
        let ambiguous: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0x2", "question": "?", "closed": true,
                "umaResolutionStatus": "resolved",
                "clobTokenIds": ["1", "2"], "outcomes": ["Yes", "No"],
                "outcomePrices": ["1", "1"]
            }"#,
        )
        .expect("wire");
        assert!(
            CatalogMarket::try_from(ambiguous)
                .expect("market")
                .settlement
                .is_none()
        );

        // Price vector shorter than the token pair.
        let mismatched: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0x3", "question": "?", "closed": true,
                "umaResolutionStatus": "resolved",
                "clobTokenIds": ["1", "2"], "outcomes": ["Yes", "No"],
                "outcomePrices": ["1"]
            }"#,
        )
        .expect("wire");
        assert!(
            CatalogMarket::try_from(mismatched)
                .expect("market")
                .settlement
                .is_none()
        );
    }

    #[test]
    fn volume_24h_preserves_missing_vs_zero_semantics() {
        let absent: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0xabsent",
                "question": "Q?",
                "clobTokenIds": ["1", "2"],
                "outcomes": ["Yes", "No"]
            }"#,
        )
        .expect("wire market");
        let absent_market = CatalogMarket::try_from(absent).expect("market");
        assert!(absent_market.volume_24h_usd.is_none());

        let zero: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0xzero",
                "question": "Q?",
                "clobTokenIds": ["1", "2"],
                "outcomes": ["Yes", "No"],
                "volume24hr": 0
            }"#,
        )
        .expect("wire market");
        let zero_market = CatalogMarket::try_from(zero).expect("market");
        assert_eq!(zero_market.volume_24h_usd, Some(Usd::ZERO));
    }

    #[test]
    fn event_category_and_status_derive_from_tags_and_closed() {
        let wire: WireEvent = serde_json::from_str(
            r#"{
                "id": "evt-1",
                "title": "E",
                "slug": "e",
                "closed": true,
                "tags": [
                    { "label": "Politics", "slug": "politics" },
                    { "label": "Geopolitics", "slug": "geopolitics" },
                    { "label": "Trump", "slug": "trump" }
                ],
                "markets": []
            }"#,
        )
        .expect("wire event");
        let event = CatalogEvent::from(wire);
        assert_eq!(event.status, EventStatus::Closed);
        assert_eq!(event.tags, vec!["politics", "geopolitics", "trump"]);
        assert_eq!(event.categories.iter().count(), 2);
        assert_eq!(event.categories.fee_category(), MarketCategory::Politics);
    }
}
