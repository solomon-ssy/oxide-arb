//! Venue-executable discrete economic tiers on a unified USD cash-flow scale.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{
    domain::market::fee::{
        DeferredVenueIncentive, FrozenMakerRebateSchedule, ImmediateExecutionCost,
    },
    enums::{common::MarketCategory, quant::OutcomeSide},
    runtime_config::BuyModelRoute,
    types::{
        Bps, ContentHash, EconomicTierId, EventId, MarketId, Price, ReportRouteRunId, Shares,
        SignalCandidateId, TokenId, Usd, UsdHours, trade_policy::PassiveFillDistribution,
    },
};

/// Deterministic aggressive entry after a real L2 walk and venue rounding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AggressiveEntryEconomics {
    pub requested_shares: Shares,
    pub filled_shares: Shares,
    pub limit_price: Price,
    pub entry_vwap: Price,
    pub immediate_cost: ImmediateExecutionCost,
    pub slippage_usd: Usd,
    pub visible_liquidity_usd: Usd,
}

/// OOS-distributed passive post-only entry with a full hard reservation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PassiveEntryEconomics {
    pub requested_shares: Shares,
    pub limit_price: Price,
    pub decision_at: DateTime<Utc>,
    pub good_til_secs: u64,
    pub hard_reserved_cash_usd: Usd,
    pub expected_filled_shares: Shares,
    pub full_fill_cost: ImmediateExecutionCost,
    pub fill_distribution: PassiveFillDistribution,
    /// Gamma terms frozen at the decision boundary. `None` means rebate was
    /// unavailable and the route was valued with a strict zero incentive.
    pub maker_rebate_schedule: Option<FrozenMakerRebateSchedule>,
    pub full_fill_maker_rebate: Option<DeferredVenueIncentive>,
    pub expected_maker_rebate_usd: Usd,
    pub visible_liquidity_usd: Usd,
}

/// Route-specific entry contract offered to the optimizer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum EntryExecutionEconomics {
    Aggressive(AggressiveEntryEconomics),
    Passive(Box<PassiveEntryEconomics>),
}

/// Entry state selected jointly with one promoted market-outcome scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum ScenarioEntryExecution {
    AggressiveFill,
    PassiveNoFill {
        good_til_secs: u64,
    },
    PassivePartialFill {
        fill_latency_ms: u64,
        post_fill_markout_bps: Bps,
    },
    PassiveFullFill {
        fill_latency_ms: u64,
        post_fill_markout_bps: Bps,
    },
}

/// One contiguous interval during which scenario cash is unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScenarioCapitalOccupancySlice {
    pub locked_cash_usd: Usd,
    pub duration_secs: u64,
}

impl EntryExecutionEconomics {
    #[must_use]
    pub const fn requested_shares(&self) -> Shares {
        match self {
            Self::Aggressive(entry) => entry.requested_shares,
            Self::Passive(entry) => entry.requested_shares,
        }
    }

    #[must_use]
    pub const fn expected_filled_shares(&self) -> Shares {
        match self {
            Self::Aggressive(entry) => entry.filled_shares,
            Self::Passive(entry) => entry.expected_filled_shares,
        }
    }

    #[must_use]
    pub const fn hard_reserved_cash_usd(&self) -> Usd {
        match self {
            Self::Aggressive(entry) => entry.immediate_cost.cash_outlay_usd,
            Self::Passive(entry) => entry.hard_reserved_cash_usd,
        }
    }

    #[must_use]
    pub const fn limit_price(&self) -> Price {
        match self {
            Self::Aggressive(entry) => entry.limit_price,
            Self::Passive(entry) => entry.limit_price,
        }
    }

    #[must_use]
    pub fn immediate_fee_usd(&self) -> Usd {
        match self {
            Self::Aggressive(entry) => entry.immediate_cost.total_fee_usd(),
            Self::Passive(entry) => entry.full_fill_cost.total_fee_usd(),
        }
    }

    #[must_use]
    pub const fn expected_maker_rebate_usd(&self) -> Usd {
        match self {
            Self::Aggressive(_) => Usd::ZERO,
            Self::Passive(entry) => entry.expected_maker_rebate_usd,
        }
    }

    #[must_use]
    pub const fn visible_liquidity_usd(&self) -> Usd {
        match self {
            Self::Aggressive(entry) => entry.visible_liquidity_usd,
            Self::Passive(entry) => entry.visible_liquidity_usd,
        }
    }
}

/// Scenario-specific execution and discounted cash flow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ScenarioExecutionCashflow {
    pub scenario_index: u32,
    pub entry_execution: ScenarioEntryExecution,
    pub filled_shares: Shares,
    pub immediate_cash_outlay_usd: Usd,
    pub discounted_exit_cash_usd: Usd,
    pub delayed_maker_rebate_usd: Usd,
    pub discounted_maker_rebate_usd: Usd,
    /// Pre-fill reservation opportunity cost. Post-fill capital cost is already
    /// represented by the scenario artifact's discounted exit cash.
    pub capital_cost_usd: Usd,
    pub capital_occupancy: Vec<ScenarioCapitalOccupancySlice>,
    pub discounted_net_usd: Usd,
    /// Tail-risk cash flow with all unreceived venue incentives forced to zero.
    pub risk_net_usd: Usd,
}

/// Hard cash reservation held independently of expected passive fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HardReservationBucket {
    pub end_secs: u64,
    pub reserved_cash_usd: Usd,
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
    pub entry_execution: EntryExecutionEconomics,
    pub profit_probability_lower_bps: u32,
    pub probability_interval_width_bps: u32,
    pub scenario_cashflows: Vec<ScenarioExecutionCashflow>,
    pub hard_reservation_envelope: Vec<HardReservationBucket>,
    pub economics: RecommendationEconomics,
    pub lineage_hash: ContentHash,
}
