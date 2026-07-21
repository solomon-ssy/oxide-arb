//! Canonical point-in-time Polymarket execution-fee schedule.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::types::{Bps, ClobMarketInfoVersionId, ContentHash, MarketId};

/// Exact order-route attribution used when calculating builder fees.
///
/// deliberately supports only unattributed venue orders. Builder
/// rates remain part of the venue fact so a later builder-enabled artifact must
/// introduce a new route contract instead of silently changing historical `PnL`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuilderFeeAttribution {
    NoBuilderCode,
}

/// Complete append-only fee fact resolved from one CLOB market-info revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketFeeSchedule {
    pub market_id: MarketId,
    pub market_info_version_id: ClobMarketInfoVersionId,
    pub market_info_payload_hash: ContentHash,
    pub platform_rate: Decimal,
    pub exponent: Decimal,
    pub taker_only: bool,
    pub builder_maker_fee_bps: Bps,
    pub builder_taker_fee_bps: Bps,
    pub builder_attribution: BuilderFeeAttribution,
    pub effective_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
}
