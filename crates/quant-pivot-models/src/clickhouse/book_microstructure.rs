use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChBps, ChDecimal64, ChPrice, ChSchemaVersion, ChUsd},
    types::{MarketId, TokenId},
};

/// Microstructure observation row for one time bucket.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct BookMicrostructureRow {
    pub token_id: TokenId,
    pub market_id: Option<MarketId>,
    pub bucket_time: i64,
    pub best_bid_open: Option<ChPrice>,
    pub best_bid_high: Option<ChPrice>,
    pub best_bid_low: Option<ChPrice>,
    pub best_bid_close: Option<ChPrice>,
    pub best_ask_open: Option<ChPrice>,
    pub best_ask_high: Option<ChPrice>,
    pub best_ask_low: Option<ChPrice>,
    pub best_ask_close: Option<ChPrice>,
    pub spread_bps_min: Option<ChBps>,
    pub spread_bps_avg: Option<ChBps>,
    pub spread_bps_max: Option<ChBps>,
    pub mid_price_open: Option<ChPrice>,
    pub mid_price_close: Option<ChPrice>,
    pub top1_depth_usd_avg: Option<ChUsd>,
    pub top5_depth_usd_avg: Option<ChUsd>,
    pub top20_depth_usd_avg: Option<ChUsd>,
    pub imbalance_avg: Option<ChDecimal64>,
    pub update_count: u64,
    pub snapshot_count: u64,
    pub delta_count: u64,
    pub delete_count: u64,
    pub crossed_count: u64,
    pub invalid_level_count: u64,
    pub gap_count: u64,
    pub last_trade_count: u64,
    pub max_book_age_ms: u64,
    pub schema_version: ChSchemaVersion,
}
