//! Decision-time market readability block (parent doc §7).

use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{common::TickSize, market::MarketStatus},
    types::{Bps, Price, Usd},
};

/// Frozen top-of-book and metadata at recommendation decision time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct MarketContext {
    pub best_bid: Option<Price>,
    pub best_ask: Option<Price>,
    pub mid_price: Option<Price>,
    pub spread_bps: Option<Bps>,
    pub depth_usd: Usd,
    pub volume_24h_usd: Option<Usd>,
    pub book_age_ms: u64,
    pub time_to_resolution_secs: Option<u64>,
    pub market_status: MarketStatus,
    pub neg_risk: bool,
    pub tick_size: TickSize,
    pub fee_rate: Option<Decimal>,
}
