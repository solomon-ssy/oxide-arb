//! Venue-executable discrete economic tiers on a unified USD cash-flow scale.

use schemars::JsonSchema;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    enums::{common::MarketCategory, quant::OutcomeSide},
    runtime_config::BuyModelRoute,
    types::{
        Bps, ContentHash, EconomicTierId, EventId, MarketId, Price, ReportRouteRunId, Shares,
        SignalCandidateId, TokenId, Usd, UsdHours,
    },
};

/// Exact entry execution economics after a real L2 walk and venue rounding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EntryEconomics {
    pub notional_usd: Usd,
    pub entry_vwap: Price,
    pub fee_usd: Usd,
    pub slippage_usd: Usd,
    pub visible_liquidity_usd: Usd,
}

/// Discounted net cash flow of one tier in one promoted joint scenario.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult,
)]
#[serde(deny_unknown_fields)]
pub struct ScenarioCashflow {
    pub scenario_index: u32,
    pub discounted_net_usd: Usd,
}

/// Capital locked by this tier through one artifact-owned elapsed-time bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CapitalOccupancyBucket {
    pub end_secs: u64,
    pub locked_usd: Usd,
}

/// Economic values displayed and ranked after global optimization.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult,
)]
#[serde(deny_unknown_fields)]
pub struct RecommendationEconomics {
    pub profit_probability_bps: Bps,
    pub nominal_expected_net_usd: Usd,
    pub robust_expected_net_usd: Usd,
    pub max_loss_usd: Usd,
    pub cvar_contribution_usd: Usd,
    pub capital_occupancy_usd_hours: UsdHours,
    pub marginal_portfolio_value_usd: Usd,
}

/// One complete tier offered to the MILP. The optimizer may select the identity or reject it;
/// it never changes shares or money values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ExecutableEconomicTier {
    pub economic_tier_id: EconomicTierId,
    pub report_route_run_id: ReportRouteRunId,
    pub candidate_id: SignalCandidateId,
    pub tier_ordinal: u32,
    pub route: BuyModelRoute,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub category: MarketCategory,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub shares: Shares,
    pub entry: EntryEconomics,
    pub profit_probability_lower_bps: u32,
    pub probability_interval_width_bps: u32,
    pub scenario_cashflows: Vec<ScenarioCashflow>,
    pub capital_occupancy: Vec<CapitalOccupancyBucket>,
    pub economics: RecommendationEconomics,
    pub lineage_hash: ContentHash,
}
