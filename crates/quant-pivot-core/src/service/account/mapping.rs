//! Map venue positions into registry-enriched [`PositionSnapshot`]s.

use quant_pivot_api::data_api::VenuePosition;
use quant_pivot_models::{
    enums::common::MarketCategory,
    types::{MarketId, PositionSnapshot, Price, Shares, TokenId, Usd},
};

use crate::ingest::market_registry::MarketRegistry;

/// Map one venue position to a [`PositionSnapshot`], enriching market metadata
/// from the registry.
///
/// Positions in markets the registry does not track get
/// [`MarketCategory::Other`] and no event — they still count toward equity.
#[must_use]
pub fn map_position(venue: &VenuePosition, registry: &MarketRegistry) -> PositionSnapshot {
    let market_id = MarketId::new(&venue.condition_id);
    let (event_id, category) =
        registry
            .get_market(&market_id)
            .map_or((None, MarketCategory::Other), |info| {
                (
                    Some(info.event_id.clone()),
                    info.categories.primary_category(),
                )
            });
    PositionSnapshot {
        token_id: TokenId::new(&venue.asset),
        market_id,
        event_id,
        category,
        outcome: venue.outcome.clone(),
        size: Shares::new(venue.size),
        avg_price: Price::new(venue.avg_price),
        cur_price: Price::new(venue.cur_price),
        current_value: Usd::new(venue.current_value),
        redeemable: venue.redeemable,
    }
}
