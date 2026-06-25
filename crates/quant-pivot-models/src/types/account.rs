//! Venue account snapshot value types: positions and exposure aggregates.
//!
//! These are the content contract for `quant_account_snapshot.positions_json` /
//! `exposures_json` and are shared by the research-plane `AccountSnapshot`
//! aggregate (the sizing capital base). They live in `types/` so the entity
//! `Model` can use them directly as JSONB column types.

use std::collections::BTreeMap;

use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::common::MarketCategory,
    jsonb_active,
    types::{EventId, MarketId, Price, Shares, TokenId, Usd},
};

/// One held outcome position at decision time, marked to the venue's price.
///
/// `current_value` and `cur_price` are recorded verbatim from the venue (Data
/// API) so the capital base is replayable. `event_id` is `None` and `category`
/// is [`MarketCategory::Other`] for positions held in markets not tracked by the
/// local registry — such positions still count toward equity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionSnapshot {
    /// CLOB outcome token id.
    pub token_id: TokenId,
    /// Owning market (Polymarket `condition_id`).
    pub market_id: MarketId,
    /// Owning event, when the market is in the registry.
    pub event_id: Option<EventId>,
    /// Market category (`Other` when unmapped).
    pub category: MarketCategory,
    /// Outcome label (e.g. `Yes` / `No`) as reported by the venue.
    pub outcome: String,
    /// Shares held.
    pub size: Shares,
    /// Average entry price (cost basis).
    pub avg_price: Price,
    /// Current venue mark price.
    pub cur_price: Price,
    /// Current marked value in USD (`size × cur_price`, venue-reported).
    pub current_value: Usd,
    /// Whether the position is redeemable (market resolved).
    pub redeemable: bool,
}

/// Net USD exposure aggregated by market, event, and category.
///
/// The planner uses this as the starting point for `exposure_after` projections
/// and cap-room checks. Built from [`PositionSnapshot`]s via
/// [`ExposureBreakdown::from_positions`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct ExposureBreakdown {
    /// Net USD exposure per market.
    pub per_market: BTreeMap<MarketId, Usd>,
    /// Net USD exposure per event.
    pub per_event: BTreeMap<EventId, Usd>,
    /// Net USD exposure per category.
    pub per_category: BTreeMap<MarketCategory, Usd>,
}

impl ExposureBreakdown {
    /// Aggregate position values into per-market / per-event / per-category nets.
    ///
    /// Every position contributes to `per_market` and `per_category`; only
    /// positions with a mapped `event_id` contribute to `per_event`.
    #[must_use]
    pub fn from_positions(positions: &[PositionSnapshot]) -> Self {
        let mut per_market: BTreeMap<MarketId, Usd> = BTreeMap::new();
        let mut per_event: BTreeMap<EventId, Usd> = BTreeMap::new();
        let mut per_category: BTreeMap<MarketCategory, Usd> = BTreeMap::new();
        for position in positions {
            *per_market.entry(position.market_id.clone()).or_default() += position.current_value;
            if let Some(event_id) = &position.event_id {
                *per_event.entry(event_id.clone()).or_default() += position.current_value;
            }
            *per_category.entry(position.category).or_default() += position.current_value;
        }
        Self {
            per_market,
            per_event,
            per_category,
        }
    }
}

/// JSONB column wrapper for the held positions of an account snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct AccountPositions(pub Vec<PositionSnapshot>);

jsonb_active!(ExposureBreakdown, AccountPositions);
