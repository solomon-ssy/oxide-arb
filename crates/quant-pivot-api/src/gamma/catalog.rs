//! Normalized Gamma catalog rows — validated wire → mapper input.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::{
        catalog::{CatalogFilterReason, CatalogFilterReasonSet, CatalogPrelistingFilterReason},
        common::{CategorySet, TickSize},
        market::{EventStatus, MarketStatus},
    },
    hashing::CanonicalDigest,
    types::{ContentHash, Usd},
};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

use super::wire::{WireEvent, WireFeeSchedule, WireMarket};

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

/// Complete, validated Gamma fee/rebate subdocument.
///
/// Incomplete upstream documents are retained as unavailable rather than
/// defaulted. A present but out-of-range value rejects the market revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogMakerRebateSchedule {
    pub fees_enabled: bool,
    pub platform_rate: Decimal,
    pub exponent: Decimal,
    pub taker_only: bool,
    pub rebate_rate: Decimal,
    pub effective_at: DateTime<Utc>,
    pub catalog_change_hash: ContentHash,
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
    /// Market rules text (resolution-source grounding anchor).
    pub description: Option<String>,
    /// Exactly two legs — the binary invariant is encoded in the type.
    pub tokens: [CatalogToken; 2],
    pub status: MarketStatus,
    pub filter_reasons: CatalogFilterReasonSet,
    pub neg_risk: bool,
    /// Settlement conclusion; present only when `closed` and
    /// `umaResolutionStatus == "resolved"` with an unambiguous `"1"` price.
    pub settlement: Option<CatalogSettlement>,
    /// Settlement close time (`closedTime`); only set on settled markets.
    pub resolved_at: Option<DateTime<Utc>>,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    pub min_order_size: Decimal,
    /// Gamma-reported liquidity in USD (`liquidityNum`), when published.
    pub liquidity_usd: Option<Usd>,
    /// Gamma-reported trailing 24h volume in USD (`volume24hr`); absent when upstream omits the field.
    pub volume_24h_usd: Option<Usd>,
    pub maker_rebate_schedule: Option<CatalogMakerRebateSchedule>,
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
    /// Recurring-series slug, when the event belongs to a series.
    pub series_slug: Option<String>,
    pub status: EventStatus,
    /// Raw Gamma tag slugs — the official categorization source.
    pub tags: Vec<String>,
    /// Category memberships derived from `tags`.
    pub categories: CategorySet,
    pub neg_risk: bool,
    pub end_date: Option<DateTime<Utc>>,
    pub markets: Vec<CatalogMarket>,
    /// Legitimate pre-listing objects excluded until Gamma assigns a CTF id.
    pub filtered_markets: Vec<FilteredPrelistingMarket>,
    /// Structurally invalid embedded markets rejected during normalization.
    pub rejected_markets: Vec<RejectedMarket>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
}

/// A legitimate Gamma market object observed before venue listing completes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilteredPrelistingMarket {
    pub source_id: String,
    pub reason: CatalogPrelistingFilterReason,
    pub raw_payload: Value,
}

/// A structurally invalid market dropped during catalog normalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedMarket {
    /// `condition_id` as received (may be empty for `EmptyConditionId`).
    pub condition_id: String,
    pub reject: CatalogMarketReject,
    pub raw_payload: Option<Value>,
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
    #[error("unsupported tick size: {value}")]
    UnsupportedTickSize { value: String },
    #[error("invalid Gamma fee schedule: {reason}")]
    InvalidFeeSchedule { reason: String },
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
            Self::UnsupportedTickSize { .. } => "unsupported_tick_size",
            Self::InvalidFeeSchedule { .. } => "invalid_fee_schedule",
        }
    }
}

impl From<WireEvent> for CatalogEvent {
    fn from(wire: WireEvent) -> Self {
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
        let series_slug = wire
            .series_slug
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty());
        let neg_risk = wire.neg_risk.unwrap_or(false);
        let created_at = parse_gamma_timestamp(wire.created_at.as_deref());
        let updated_at = parse_gamma_timestamp(wire.updated_at.as_deref());
        let wire_markets = wire.markets.unwrap_or_default();
        let mut markets = Vec::with_capacity(wire_markets.len());
        let mut filtered_markets = Vec::new();
        let mut rejected_markets = Vec::new();
        for market in wire_markets {
            let condition_id = market.condition_id.clone();
            let raw_payload = serde_json::to_value(&market).ok();
            if condition_id.trim().is_empty()
                && let Some(source_id) = market
                    .id
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
            {
                filtered_markets.push(FilteredPrelistingMarket {
                    source_id: source_id.to_owned(),
                    reason: CatalogPrelistingFilterReason::MissingConditionId,
                    raw_payload: raw_payload.unwrap_or(Value::Null),
                });
                continue;
            }
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
                        raw_payload,
                    });
                }
            }
        }

        Self {
            id,
            title,
            slug,
            series_slug,
            status,
            tags,
            categories,
            neg_risk,
            end_date,
            markets,
            filtered_markets,
            rejected_markets,
            created_at,
            updated_at,
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
        let tick_size = match wire.order_price_min_tick_size {
            None => TickSize::Hundredth,
            Some(decimal) => TickSize::try_from(decimal).map_err(|_| {
                CatalogMarketReject::UnsupportedTickSize {
                    value: decimal.normalize().to_string(),
                }
            })?,
        };
        let start_date = wire.start_date();
        let end_date = wire.end_date();
        let created_at = parse_gamma_timestamp(wire.created_at.as_deref());
        let updated_at = parse_gamma_timestamp(wire.updated_at.as_deref());
        let maker_rebate_schedule = normalize_maker_rebate_schedule(
            wire.fees_enabled,
            wire.fee_schedule.as_ref(),
            updated_at.or(created_at),
        )?;
        let closed = wire.closed.unwrap_or(false);
        let filter_reasons = wire.filter_reasons_from_wire();
        let status = market_status_from_wire(closed, filter_reasons);
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
            description: wire
                .description
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty()),
            tokens,
            status,
            filter_reasons,
            neg_risk: wire.neg_risk.unwrap_or(false),
            settlement,
            resolved_at,
            start_date,
            end_date,
            min_order_size: wire.order_min_size.unwrap_or(Decimal::ONE),
            liquidity_usd: wire.liquidity_num.map(Usd::new),
            volume_24h_usd: wire.volume_24hr.map(Usd::new),
            maker_rebate_schedule,
            tick_size,
            created_at,
            updated_at,
        })
    }
}

fn normalize_maker_rebate_schedule(
    fees_enabled: Option<bool>,
    schedule: Option<&WireFeeSchedule>,
    effective_at: Option<DateTime<Utc>>,
) -> Result<Option<CatalogMakerRebateSchedule>, CatalogMarketReject> {
    let Some(schedule) = schedule else {
        return Ok(None);
    };
    for (name, value) in [
        ("rate", schedule.rate),
        ("rebateRate", schedule.rebate_rate),
    ] {
        if value.is_some_and(|value| !(Decimal::ZERO..=Decimal::ONE).contains(&value)) {
            return Err(CatalogMarketReject::InvalidFeeSchedule {
                reason: format!("{name} must be within [0, 1]"),
            });
        }
    }
    if schedule
        .exponent
        .is_some_and(|value| value <= Decimal::ZERO || value > Decimal::from(8))
    {
        return Err(CatalogMarketReject::InvalidFeeSchedule {
            reason: "exponent must be within (0, 8]".to_owned(),
        });
    }
    let (
        Some(fees_enabled),
        Some(platform_rate),
        Some(exponent),
        Some(taker_only),
        Some(rebate_rate),
        Some(effective_at),
    ) = (
        fees_enabled,
        schedule.rate,
        schedule.exponent,
        schedule.taker_only,
        schedule.rebate_rate,
        effective_at,
    )
    else {
        return Ok(None);
    };
    let catalog_change_hash = CanonicalDigest::content_hash_typed(
        "quant-pivot/gamma-maker-rebate-schedule",
        1,
        &(
            fees_enabled,
            platform_rate,
            exponent,
            taker_only,
            rebate_rate,
            effective_at,
        ),
    )
    .map_err(|error| CatalogMarketReject::InvalidFeeSchedule {
        reason: format!("canonical schedule hash failed: {error}"),
    })?;
    Ok(Some(CatalogMakerRebateSchedule {
        fees_enabled,
        platform_rate,
        exponent,
        taker_only,
        rebate_rate,
        effective_at,
        catalog_change_hash,
    }))
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

const fn market_status_from_wire(
    closed: bool,
    filter_reasons: CatalogFilterReasonSet,
) -> MarketStatus {
    if closed {
        MarketStatus::Settled
    } else if filter_reasons.is_empty() {
        MarketStatus::Active
    } else {
        MarketStatus::Filtered
    }
}

impl WireMarket {
    fn filter_reasons_from_wire(&self) -> CatalogFilterReasonSet {
        let mut reasons = CatalogFilterReasonSet::EMPTY;
        if self.closed == Some(true) {
            reasons.insert(CatalogFilterReason::Closed);
        }
        if self.active == Some(false) {
            reasons.insert(CatalogFilterReason::Inactive);
        }
        if self.enable_order_book == Some(false) {
            reasons.insert(CatalogFilterReason::ClobDisabled);
        }
        if self.accepting_orders == Some(false) {
            reasons.insert(CatalogFilterReason::OrdersNotAccepted);
        }
        reasons
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
    use quant_pivot_models::{
        enums::{
            catalog::{CatalogFilterReason, CatalogPrelistingFilterReason},
            common::{MarketCategory, TickSize},
            market::{EventStatus, MarketStatus},
        },
        types::Usd,
    };
    use rust_decimal_macros::dec;

    use super::{CatalogEvent, CatalogMarket, CatalogMarketReject};
    use crate::gamma::wire::{WireEvent, WireMarket};

    #[test]
    fn filters_prelisting_empty_id() {
        let wire: WireEvent = serde_json::from_str(
            r#"{
                "id": "1",
                "title": "E",
                "slug": "e",
                "markets": [{
                    "id": "pending-1",
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
        assert!(event.rejected_markets.is_empty());
        assert_eq!(event.filtered_markets.len(), 1);
        assert_eq!(event.filtered_markets[0].source_id, "pending-1");
        assert_eq!(
            event.filtered_markets[0].reason,
            CatalogPrelistingFilterReason::MissingConditionId
        );
    }

    #[test]
    fn rejects_empty_without_identity() {
        let wire: WireEvent = serde_json::from_str(
            r#"{
                "id": "1",
                "markets": [{
                    "conditionId": "",
                    "question": "unknown source",
                    "clobTokenIds": []
                }]
            }"#,
        )
        .expect("wire event");
        let event = CatalogEvent::from(wire);
        assert!(event.filtered_markets.is_empty());
        assert!(matches!(
            event.rejected_markets[0].reject,
            CatalogMarketReject::EmptyConditionId
        ));
    }

    #[test]
    fn rejects_market_without_ids() {
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
    fn empty_condition_maps_reason() {
        let wire: WireMarket = serde_json::from_str(
            r#"{"conditionId": "", "question": "?", "clobTokenIds": ["1"], "outcomes": ["Yes"]}"#,
        )
        .expect("wire market");
        let err = CatalogMarket::try_from(wire).expect_err("reject");
        assert!(matches!(err, CatalogMarketReject::EmptyConditionId));
    }

    #[test]
    fn non_binary_token_rejected() {
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
    fn half_quarter_cent_accepted() {
        let half: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0xhalf",
                "question": "?",
                "clobTokenIds": ["1", "2"],
                "outcomes": ["Yes", "No"],
                "orderPriceMinTickSize": "0.005"
            }"#,
        )
        .expect("wire market");
        let market = CatalogMarket::try_from(half).expect("half cent");
        assert_eq!(market.tick_size, TickSize::HalfCent);

        let quarter: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0xquarter",
                "question": "?",
                "clobTokenIds": ["1", "2"],
                "outcomes": ["Yes", "No"],
                "orderPriceMinTickSize": "0.0025"
            }"#,
        )
        .expect("wire market");
        let market = CatalogMarket::try_from(quarter).expect("quarter cent");
        assert_eq!(market.tick_size, TickSize::QuarterCent);
    }

    #[test]
    fn unsupported_rejected_without_fallback() {
        let wire: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0xabc",
                "question": "?",
                "clobTokenIds": ["1", "2"],
                "outcomes": ["Yes", "No"],
                "orderPriceMinTickSize": "0.00001"
            }"#,
        )
        .expect("wire market");
        let err = CatalogMarket::try_from(wire).expect_err("reject");
        assert!(matches!(
            err,
            CatalogMarketReject::UnsupportedTickSize { .. }
        ));
    }

    #[test]
    fn custom_outcome_binary_accepted() {
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
    fn explicit_trading_flags_reasons() {
        let wire: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0xfiltered",
                "question": "Q?",
                "active": false,
                "enableOrderBook": false,
                "acceptingOrders": false,
                "clobTokenIds": ["1", "2"],
                "outcomes": ["Yes", "No"]
            }"#,
        )
        .expect("wire market");
        let market = CatalogMarket::try_from(wire).expect("valid filtered market");

        assert_eq!(market.status, MarketStatus::Filtered);
        assert!(
            market
                .filter_reasons
                .contains(CatalogFilterReason::Inactive)
        );
        assert!(
            market
                .filter_reasons
                .contains(CatalogFilterReason::ClobDisabled)
        );
        assert!(
            market
                .filter_reasons
                .contains(CatalogFilterReason::OrdersNotAccepted)
        );
    }

    #[test]
    fn absent_flags_never_fabricate() {
        let wire: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0xactive",
                "question": "Q?",
                "clobTokenIds": ["1", "2"],
                "outcomes": ["Yes", "No"]
            }"#,
        )
        .expect("wire market");
        let market = CatalogMarket::try_from(wire).expect("valid active market");

        assert_eq!(market.status, MarketStatus::Active);
        assert!(market.filter_reasons.is_empty());
    }

    #[test]
    fn settlement_derived_outcome_resolved() {
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
    fn settlement_rejects_ambiguity() {
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
    fn volume_preserves_missing_zero() {
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
    fn rebate_requires_complete_evidence() {
        let complete: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0xrebate",
                "question": "Q?",
                "clobTokenIds": ["1", "2"],
                "outcomes": ["Yes", "No"],
                "feesEnabled": true,
                "updatedAt": "2026-08-01T00:00:00Z",
                "feeSchedule": {
                    "rate": "0.07",
                    "exponent": "1",
                    "takerOnly": true,
                    "rebateRate": "0.20"
                }
            }"#,
        )
        .expect("wire market");
        let market = CatalogMarket::try_from(complete).expect("complete schedule");
        let schedule = market.maker_rebate_schedule.expect("rebate schedule");
        assert_eq!(schedule.platform_rate, dec!(0.07));
        assert_eq!(schedule.rebate_rate, dec!(0.20));

        let incomplete: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0xincomplete",
                "question": "Q?",
                "clobTokenIds": ["1", "2"],
                "outcomes": ["Yes", "No"],
                "feesEnabled": true,
                "updatedAt": "2026-08-01T00:00:00Z",
                "feeSchedule": { "rate": "0.07", "exponent": "1" }
            }"#,
        )
        .expect("wire market");
        assert!(
            CatalogMarket::try_from(incomplete)
                .expect("incomplete schedule is unavailable")
                .maker_rebate_schedule
                .is_none()
        );
    }

    #[test]
    fn rebate_rejects_invalid_rate() {
        let wire: WireMarket = serde_json::from_str(
            r#"{
                "conditionId": "0xinvalid",
                "question": "Q?",
                "clobTokenIds": ["1", "2"],
                "outcomes": ["Yes", "No"],
                "feesEnabled": true,
                "updatedAt": "2026-08-01T00:00:00Z",
                "feeSchedule": {
                    "rate": "0.07",
                    "exponent": "1",
                    "takerOnly": true,
                    "rebateRate": "1.01"
                }
            }"#,
        )
        .expect("wire market");
        assert!(matches!(
            CatalogMarket::try_from(wire),
            Err(CatalogMarketReject::InvalidFeeSchedule { .. })
        ));
    }

    #[test]
    fn rebate_rule_changes_hash() {
        let wire = |rebate_rate: &str| {
            serde_json::from_value::<WireMarket>(serde_json::json!({
                "conditionId": "0xhash",
                "question": "Q?",
                "clobTokenIds": ["1", "2"],
                "outcomes": ["Yes", "No"],
                "feesEnabled": true,
                "updatedAt": "2026-08-01T00:00:00Z",
                "feeSchedule": {
                    "rate": "0.07",
                    "exponent": "1",
                    "takerOnly": true,
                    "rebateRate": rebate_rate
                }
            }))
            .expect("wire market")
        };
        let first = CatalogMarket::try_from(wire("0.20"))
            .expect("first")
            .maker_rebate_schedule
            .expect("schedule");
        let second = CatalogMarket::try_from(wire("0.25"))
            .expect("second")
            .maker_rebate_schedule
            .expect("schedule");
        assert_ne!(first.catalog_change_hash, second.catalog_change_hash);
    }

    #[test]
    fn event_category_status_closed() {
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
        assert_eq!(
            event.categories.primary_category(),
            MarketCategory::Politics
        );
    }
}
