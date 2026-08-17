//! Exact conversion from executable entry tiers to unified scenario USD cash flows.

use std::collections::HashSet;

use chrono::{DateTime, Utc};

use quant_pivot_error::{QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::market::{book::BookLevel, fee::ImmediateExecutionCost},
    domain::quant::{
        AggressiveEntryEconomics, CapitalOccupancyBucket, EntryExecutionEconomics,
        ExecutableEconomicTier, ExistingPortfolioState, HardReservationBucket,
        PassiveEntryEconomics, PortfolioScenarioArtifact, RecommendationEconomics,
        ScenarioCapitalOccupancySlice, ScenarioCashflow, ScenarioEntryExecution,
        ScenarioExecutionCashflow, ScenarioMarketOutcome,
    },
    enums::{
        common::MarketCategory,
        quant::{FillRequirement, OutcomeSide},
    },
    hashing::CanonicalDigest,
    runtime_config::BuyModelRoute,
    types::{
        Bps, ContentHash, EconomicTierId, EventId, MarketId, Price, ReportRouteRunId, Shares,
        SignalCandidateId, TokenId, Usd, UsdHours,
        trade_policy::{PassiveFillDistribution, PassiveFillState, PassiveFillStateKind},
    },
};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::Serialize;

use crate::{
    execution_semantics::{
        BookWalkOutcome, LiquidityRole, PitFeeSchedule, PitMakerRebateSchedule,
        PitMarketExecutionEconomics, walk_buy_cash_budget, walk_buy_exact_shares,
    },
    precision::quantize_venue_amount,
};

use super::{AccountSnapshot, SealedPortfolioScenarioArtifact};

const DISTRIBUTION_MASS_BPS: u32 = 10_000;
const SECONDS_PER_HOUR: u64 = 3_600;
const SECONDS_PER_YEAR: u64 = 31_536_000;

#[derive(Serialize)]
struct TierLineagePreimage<'a> {
    seed: &'a ExecutableTierSeed,
    scenario_artifact_hash: ContentHash,
    outcome_hashes: &'a [ContentHash],
    scenario_cashflows: &'a [ScenarioExecutionCashflow],
    hard_reservation_envelope: &'a [HardReservationBucket],
}

/// Frozen venue-executable tier before promoted joint scenarios are applied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutableTierSeed {
    pub report_route_run_id: ReportRouteRunId,
    pub candidate_id: SignalCandidateId,
    pub tier_ordinal: u32,
    pub route: BuyModelRoute,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub category: MarketCategory,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    /// Frozen candidate-side bid depth available to a sell-to-close at the decision boundary.
    /// Scenario factors stress this exogenous venue capacity; it must never be derived from the
    /// requested tier size.
    pub observed_exit_capacity_shares: Shares,
    pub entry_execution: EntryExecutionEconomics,
    /// Hash of the model/calibration/trade-policy/L2 input preimage for this tier.
    pub source_lineage_hash: ContentHash,
}

/// Complete frozen input for constructing every executable tier exposed by one candidate.
#[derive(Debug, Clone)]
pub struct ExecutableTierLadderSeedInput<'a> {
    pub report_route_run_id: ReportRouteRunId,
    pub candidate_id: SignalCandidateId,
    pub route: BuyModelRoute,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub category: MarketCategory,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub bids: &'a [BookLevel],
    pub asks: &'a [BookLevel],
    pub fee_schedule: &'a PitFeeSchedule,
    pub fill_at: DateTime<Utc>,
    pub limit_price: Price,
    pub max_notional_usd: Usd,
    pub source_lineage_hash: ContentHash,
}

/// Constructs the canonical discrete L2 ladder used by live reports and PIT replay.
pub struct ExecutableTierLadderSeedFactory;

impl ExecutableTierLadderSeedFactory {
    /// Walk the true ask ladder, preserve every economically distinct cumulative depth point, and
    /// convert each point through the promoted scenario artifact. No synthetic equal-dollar grid
    /// or continuous amount is introduced.
    pub fn build(
        input: &ExecutableTierLadderSeedInput<'_>,
    ) -> QuantResult<Vec<ExecutableTierSeed>> {
        validate_ladder_input(input)?;
        let maximum = walk_buy_cash_budget(
            input.asks,
            input.max_notional_usd,
            input.limit_price,
            FillRequirement::AllowPartial,
            input.fee_schedule,
            LiquidityRole::Taker,
            input.fill_at,
        )
        .map_err(|error| ReportError::InvariantViolation {
            stage: "economic_tier_ladder",
            detail: format!("maximum executable L2 walk failed: {error:?}"),
        })?;
        if maximum.outcome == BookWalkOutcome::Unfilled || !maximum.filled_shares.is_positive() {
            return Ok(Vec::new());
        }

        let best_ask = input
            .asks
            .first()
            .map(|level| level.price_decimal())
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "economic_tier_ladder",
                detail: "non-empty canonical ask ladder lost its best level".to_owned(),
            })?;
        let visible_liquidity = visible_liquidity(input.asks, input.limit_price)?;
        let observed_exit_capacity_shares = visible_shares(input.bids)?;
        let mut targets = cumulative_targets(input.asks, input.limit_price, maximum.filled_shares)?;
        if targets.last().copied() != Some(maximum.filled_shares) {
            targets.push(maximum.filled_shares);
        }

        let mut tiers = Vec::with_capacity(targets.len());
        for (index, shares) in targets.into_iter().enumerate() {
            let fill = walk_buy_exact_shares(
                input.asks,
                shares,
                input.limit_price,
                FillRequirement::AllOrNothing,
                input.fee_schedule,
                LiquidityRole::Taker,
                input.fill_at,
            )
            .map_err(|error| ReportError::InvariantViolation {
                stage: "economic_tier_ladder",
                detail: format!("exact-share L2 walk failed: {error:?}"),
            })?;
            if fill.outcome != BookWalkOutcome::Filled || fill.filled_shares != shares {
                return Err(ReportError::InvariantViolation {
                    stage: "economic_tier_ladder",
                    detail: "a cumulative depth point was not exactly executable".to_owned(),
                }
                .into());
            }
            let vwap = fill.vwap.ok_or_else(|| ReportError::InvariantViolation {
                stage: "economic_tier_ladder",
                detail: "filled L2 tier has no VWAP".to_owned(),
            })?;
            let cash_outlay = fill.immediate_cost.cash_outlay_usd.inner();
            if cash_outlay > input.max_notional_usd.inner() {
                return Err(ReportError::InvariantViolation {
                    stage: "economic_tier_ladder",
                    detail: "exact tier exceeds the governed recommendation cap".to_owned(),
                }
                .into());
            }
            let best_price_principal =
                best_ask
                    .inner()
                    .checked_mul(shares.inner())
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "economic_tier.entry.slippage_usd",
                        detail: "best ask multiplied by shares overflowed Decimal".to_owned(),
                    })?;
            let slippage = quantize_venue_amount(
                fill.immediate_cost
                    .principal_usd
                    .inner()
                    .checked_sub(best_price_principal)
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "economic_tier.entry.slippage_usd",
                        detail: "walked principal minus best-ask principal overflowed Decimal"
                            .to_owned(),
                    })?,
            );
            let tier_ordinal =
                u32::try_from(index + 1).map_err(|error| ReportError::NumericOverflow {
                    field: "economic_tier.tier_ordinal",
                    detail: error.to_string(),
                })?;
            tiers.push(ExecutableTierSeed {
                report_route_run_id: input.report_route_run_id,
                candidate_id: input.candidate_id,
                tier_ordinal,
                route: input.route,
                market_id: input.market_id.clone(),
                event_id: input.event_id.clone(),
                category: input.category,
                token_id: input.token_id.clone(),
                outcome_side: input.outcome_side,
                observed_exit_capacity_shares,
                entry_execution: EntryExecutionEconomics::Aggressive(AggressiveEntryEconomics {
                    requested_shares: shares,
                    filled_shares: shares,
                    limit_price: input.limit_price,
                    entry_vwap: vwap,
                    immediate_cost: fill.immediate_cost,
                    slippage_usd: Usd::new(slippage),
                    visible_liquidity_usd: visible_liquidity,
                }),
                source_lineage_hash: input.source_lineage_hash,
            });
        }
        Ok(tiers)
    }
}

/// One exact policy-owned cash-budget tier evaluated against the frozen ask book.
#[derive(Debug, Clone)]
pub struct ExecutableCashTierSeedInput<'a> {
    pub report_route_run_id: ReportRouteRunId,
    pub candidate_id: SignalCandidateId,
    pub tier_ordinal: u32,
    pub route: BuyModelRoute,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub category: MarketCategory,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub bids: &'a [BookLevel],
    pub asks: &'a [BookLevel],
    pub fee_schedule: &'a PitFeeSchedule,
    pub fill_at: DateTime<Utc>,
    pub limit_price: Price,
    pub cash_budget: Usd,
    pub fill_requirement: FillRequirement,
    pub source_lineage_hash: ContentHash,
}

/// Converts one published Trade Policy cash tier into one executable entry seed.
pub struct ExecutableCashTierSeedFactory;

impl ExecutableCashTierSeedFactory {
    /// Walk the full ask ladder once. An unfilled policy tier is a normal candidate-level
    /// rejection; malformed books, fees, or economics remain report-fatal errors.
    pub fn build(
        input: ExecutableCashTierSeedInput<'_>,
    ) -> QuantResult<Option<ExecutableTierSeed>> {
        validate_cash_tier_input(&input)?;
        let fill = walk_buy_cash_budget(
            input.asks,
            input.cash_budget,
            input.limit_price,
            input.fill_requirement,
            input.fee_schedule,
            LiquidityRole::Taker,
            input.fill_at,
        )
        .map_err(|error| ReportError::InvariantViolation {
            stage: "economic_cash_tier",
            detail: format!("policy cash-budget L2 walk failed: {error:?}"),
        })?;
        if fill.outcome == BookWalkOutcome::Unfilled || !fill.filled_shares.is_positive() {
            return Ok(None);
        }
        let vwap = fill.vwap.ok_or_else(|| ReportError::InvariantViolation {
            stage: "economic_cash_tier",
            detail: "filled policy cash tier has no VWAP".to_owned(),
        })?;
        let best_ask = input
            .asks
            .first()
            .map(|level| level.price_decimal())
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "economic_cash_tier",
                detail: "non-empty ask ladder lost its best level".to_owned(),
            })?;
        let cash_outlay = fill.immediate_cost.cash_outlay_usd.inner();
        if cash_outlay > input.cash_budget.inner() {
            return Err(ReportError::InvariantViolation {
                stage: "economic_cash_tier",
                detail: "walked cash outlay exceeds its published policy tier".to_owned(),
            }
            .into());
        }
        let best_price_principal = best_ask
            .inner()
            .checked_mul(fill.filled_shares.inner())
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "economic_cash_tier.slippage_usd",
                detail: "best ask multiplied by shares overflowed Decimal".to_owned(),
            })?;
        let slippage = fill
            .immediate_cost
            .principal_usd
            .inner()
            .checked_sub(best_price_principal)
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "economic_cash_tier.slippage_usd",
                detail: "walked principal minus best-ask principal overflowed Decimal".to_owned(),
            })?;
        Ok(Some(ExecutableTierSeed {
            report_route_run_id: input.report_route_run_id,
            candidate_id: input.candidate_id,
            tier_ordinal: input.tier_ordinal,
            route: input.route,
            market_id: input.market_id,
            event_id: input.event_id,
            category: input.category,
            token_id: input.token_id,
            outcome_side: input.outcome_side,
            observed_exit_capacity_shares: visible_shares(input.bids)?,
            entry_execution: EntryExecutionEconomics::Aggressive(AggressiveEntryEconomics {
                requested_shares: fill.filled_shares,
                filled_shares: fill.filled_shares,
                limit_price: input.limit_price,
                entry_vwap: vwap,
                immediate_cost: fill.immediate_cost,
                slippage_usd: Usd::new(quantize_venue_amount(slippage)),
                visible_liquidity_usd: visible_liquidity(input.asks, input.limit_price)?,
            }),
            source_lineage_hash: input.source_lineage_hash,
        }))
    }
}

/// One passive post-only cash tier backed by a published OOS fill distribution.
#[derive(Debug, Clone)]
pub struct ExecutablePassiveTierSeedInput<'a> {
    pub report_route_run_id: ReportRouteRunId,
    pub candidate_id: SignalCandidateId,
    pub tier_ordinal: u32,
    pub route: BuyModelRoute,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub category: MarketCategory,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub bids: &'a [BookLevel],
    pub execution_economics: &'a PitMarketExecutionEconomics,
    pub decision_at: DateTime<Utc>,
    pub limit_price: Price,
    pub requested_shares: Shares,
    pub cash_budget: Usd,
    pub good_til_secs: u64,
    pub fill_distribution: PassiveFillDistribution,
    pub source_lineage_hash: ContentHash,
}

/// Constructs a fully reserved passive entry seed without pretending it has filled.
pub struct ExecutablePassiveTierSeedFactory;

impl ExecutablePassiveTierSeedFactory {
    pub fn build(input: ExecutablePassiveTierSeedInput<'_>) -> QuantResult<ExecutableTierSeed> {
        if input.tier_ordinal == 0
            || input.bids.is_empty()
            || !input.limit_price.is_positive()
            || input.limit_price > Price::ONE
            || !input.requested_shares.is_positive()
            || input.good_til_secs == 0
            || input.bids.windows(2).any(|levels| {
                levels[0].price_decimal() < levels[1].price_decimal()
                    || !levels[0].size_decimal().is_positive()
            })
            || input
                .bids
                .last()
                .is_some_and(|level| !level.size_decimal().is_positive())
        {
            return Err(ReportError::InvariantViolation {
                stage: "economic_passive_tier",
                detail: "passive identity, bid depth, limit, size, or GTD is invalid".to_owned(),
            }
            .into());
        }
        input
            .fill_distribution
            .validate()
            .map_err(|detail| ReportError::InvariantViolation {
                stage: "economic_passive_tier",
                detail,
            })?;
        if !input.execution_economics.fee_schedule.taker_only {
            return Err(ReportError::InvariantViolation {
                stage: "economic_passive_tier",
                detail: "production passive entry requires the CLOB V2 taker-only fee contract"
                    .to_owned(),
            }
            .into());
        }
        let principal = Usd::new(quantize_venue_amount(
            input.limit_price.inner() * input.requested_shares.inner(),
        ));
        let venue_fee = input
            .execution_economics
            .fee_schedule
            .fee(
                LiquidityRole::Maker,
                input.limit_price,
                input.requested_shares,
                input.decision_at,
            )
            .map_err(|error| ReportError::InvariantViolation {
                stage: "economic_passive_tier",
                detail: format!("passive full-fill fee calculation failed: {error:?}"),
            })?;
        let full_fill_cost =
            ImmediateExecutionCost::new(principal, venue_fee, Usd::ZERO).map_err(|detail| {
                ReportError::InvariantViolation {
                    stage: "economic_passive_tier",
                    detail: detail.to_owned(),
                }
            })?;
        if full_fill_cost.cash_outlay_usd > input.cash_budget {
            return Err(ReportError::InvariantViolation {
                stage: "economic_passive_tier",
                detail: "passive full-fill reservation exceeds its governed cash tier".to_owned(),
            }
            .into());
        }
        let expected_filled_shares =
            expected_passive_shares(input.requested_shares, &input.fill_distribution)?;
        let full_fill_maker_rebate = input
            .execution_economics
            .maker_rebate_schedule
            .as_ref()
            .map(|schedule| {
                schedule.expected_incentive(
                    &input.execution_economics.fee_schedule,
                    LiquidityRole::Maker,
                    input.limit_price,
                    input.requested_shares,
                    input.decision_at,
                )
            })
            .transpose()
            .map_err(|error| ReportError::InvariantViolation {
                stage: "economic_passive_tier",
                detail: format!("passive rebate calculation failed: {error:?}"),
            })?
            .flatten();
        let expected_maker_rebate_usd = Usd::new(quantize_venue_amount(
            full_fill_maker_rebate.map_or(Decimal::ZERO, |incentive| {
                incentive.expected_rebate_usd.inner()
            }) * expected_filled_shares.inner()
                / input.requested_shares.inner(),
        ));
        let source_lineage_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/passive-economic-tier-seed",
            1,
            &(
                input.source_lineage_hash,
                input.execution_economics.composite_hash,
                input.fill_distribution.source_evidence_hash,
            ),
        )?;
        Ok(ExecutableTierSeed {
            report_route_run_id: input.report_route_run_id,
            candidate_id: input.candidate_id,
            tier_ordinal: input.tier_ordinal,
            route: input.route,
            market_id: input.market_id,
            event_id: input.event_id,
            category: input.category,
            token_id: input.token_id,
            outcome_side: input.outcome_side,
            observed_exit_capacity_shares: visible_shares(input.bids)?,
            entry_execution: EntryExecutionEconomics::Passive(Box::new(PassiveEntryEconomics {
                requested_shares: input.requested_shares,
                limit_price: input.limit_price,
                decision_at: input.decision_at,
                good_til_secs: input.good_til_secs,
                hard_reserved_cash_usd: full_fill_cost.cash_outlay_usd,
                expected_filled_shares,
                full_fill_cost,
                fill_distribution: input.fill_distribution,
                maker_rebate_schedule: input
                    .execution_economics
                    .maker_rebate_schedule
                    .as_ref()
                    .map(PitMakerRebateSchedule::frozen),
                full_fill_maker_rebate,
                expected_maker_rebate_usd,
                visible_liquidity_usd: Usd::new(quantize_venue_amount(
                    input.limit_price.inner() * visible_shares(input.bids)?.inner(),
                )),
            })),
            source_lineage_hash,
        })
    }
}

fn expected_passive_shares(
    requested_shares: Shares,
    distribution: &PassiveFillDistribution,
) -> QuantResult<Shares> {
    let weighted_ratio = distribution
        .states
        .iter()
        .try_fold(Decimal::ZERO, |total, state| {
            let weighted = Decimal::from(state.probability_bps)
                .checked_mul(Decimal::from(state.fill_ratio_bps))
                .ok_or_else(|| ReportError::NumericOverflow {
                    field: "economic_passive_tier.expected_fill_ratio",
                    detail: "passive probability multiplied by fill ratio overflowed Decimal"
                        .to_owned(),
                })?;
            total
                .checked_add(weighted)
                .ok_or_else(|| ReportError::NumericOverflow {
                    field: "economic_passive_tier.expected_fill_ratio",
                    detail: "passive expected fill ratio overflowed Decimal".to_owned(),
                })
        })?
        / Decimal::from(100_000_000_u64);
    Ok(Shares::new(
        (requested_shares.inner() * weighted_ratio)
            .round_dp_with_strategy(6, RoundingStrategy::ToZero),
    ))
}

fn validate_cash_tier_input(input: &ExecutableCashTierSeedInput<'_>) -> QuantResult<()> {
    if input.tier_ordinal == 0
        || input.bids.is_empty()
        || input.asks.is_empty()
        || !input.cash_budget.is_positive()
        || !input.limit_price.is_positive()
        || input.limit_price > Price::ONE
        || input.asks.windows(2).any(|levels| {
            levels[0].price_decimal() > levels[1].price_decimal()
                || !levels[0].size_decimal().is_positive()
        })
        || input
            .asks
            .last()
            .is_some_and(|level| !level.size_decimal().is_positive())
        || input.bids.windows(2).any(|levels| {
            levels[0].price_decimal() < levels[1].price_decimal()
                || !levels[0].size_decimal().is_positive()
        })
        || input
            .bids
            .last()
            .is_some_and(|level| !level.size_decimal().is_positive())
    {
        return Err(ReportError::InvariantViolation {
            stage: "economic_cash_tier",
            detail: "policy tier identity, ask order, cash budget, or price limit is invalid"
                .to_owned(),
        }
        .into());
    }
    Ok(())
}

/// Builds exact existing-position scenario P&L and capital occupancy at the same frozen boundary.
pub struct ExistingPortfolioFactory;

impl ExistingPortfolioFactory {
    /// Every positive venue position must be represented by exactly one outcome in every joint
    /// scenario. Missing coverage is a scenario-contract failure, never an independence fallback.
    pub fn build(
        account: &AccountSnapshot,
        current_drawdown_usd: Usd,
        sealed_artifact: &SealedPortfolioScenarioArtifact,
    ) -> QuantResult<ExistingPortfolioState> {
        let artifact = sealed_artifact.artifact();
        let mut scenario_totals = vec![Decimal::ZERO; artifact.scenarios.len()];
        let mut position_release_secs = Vec::new();
        let mut open_capital = Decimal::ZERO;
        let mut open_count = 0_u32;

        for position in account
            .positions
            .iter()
            .filter(|position| position.size.is_positive())
        {
            open_count = open_count
                .checked_add(1)
                .ok_or_else(|| ReportError::NumericOverflow {
                    field: "existing_portfolio.open_recommendations",
                    detail: "open position count overflowed u32".to_owned(),
                })?;
            open_capital = open_capital
                .checked_add(position.current_value.inner())
                .ok_or_else(|| ReportError::NumericOverflow {
                    field: "existing_portfolio.open_capital_usd",
                    detail: "open position capital overflowed Decimal".to_owned(),
                })?;
            let route = BuyModelRoute::from(position.category);
            let mut maximum_release_secs = 0_u64;
            for (scenario_offset, scenario) in artifact.scenarios.iter().enumerate() {
                let mut matches = scenario.market_outcomes.iter().filter(|outcome| {
                    outcome.route == route
                        && outcome.market_id == position.market_id
                        && outcome.token_id == position.token_id
                });
                let outcome = matches.next().ok_or_else(|| ReportError::ScenarioArtifact {
                    detail: format!(
                        "scenario {} has no existing-position outcome for route {:?}, market {}, token {}",
                        scenario.scenario_index, route, position.market_id, position.token_id
                    ),
                })?;
                if matches.next().is_some() {
                    return Err(ReportError::ScenarioArtifact {
                        detail: format!(
                            "scenario {} repeats existing-position outcome for market {}, token {}",
                            scenario.scenario_index, position.market_id, position.token_id
                        ),
                    }
                    .into());
                }
                if position.size > outcome.max_executable_exit_shares
                    || outcome.discounted_exit_cash_per_share_usd.is_negative()
                    || outcome.capital_release_secs == 0
                {
                    return Err(ReportError::ScenarioArtifact {
                        detail: format!(
                            "scenario {} cannot safely liquidate existing token {}",
                            scenario.scenario_index, position.token_id
                        ),
                    }
                    .into());
                }
                let exit_cash = position
                    .size
                    .inner()
                    .checked_mul(outcome.discounted_exit_cash_per_share_usd.inner())
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "existing_portfolio.discounted_exit_cash",
                        detail: "existing shares multiplied by exit cash overflowed Decimal"
                            .to_owned(),
                    })?;
                let net = exit_cash
                    .checked_sub(position.current_value.inner())
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "existing_portfolio.scenario_net_usd",
                        detail: "existing exit cash minus current value overflowed Decimal"
                            .to_owned(),
                    })?;
                scenario_totals[scenario_offset] = scenario_totals[scenario_offset]
                    .checked_add(net)
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "existing_portfolio.scenario_net_usd",
                        detail: "existing scenario P&L overflowed Decimal".to_owned(),
                    })?;
                maximum_release_secs = maximum_release_secs.max(outcome.capital_release_secs);
            }
            position_release_secs.push((position.current_value, maximum_release_secs));
        }

        let scenario_cashflows = artifact
            .scenarios
            .iter()
            .zip(scenario_totals)
            .map(|(scenario, total)| ScenarioCashflow {
                scenario_index: scenario.scenario_index,
                discounted_net_usd: Usd::new(quantize_venue_amount(total)),
            })
            .collect();
        let mut prior = 0_u64;
        let mut capital_occupancy = Vec::with_capacity(artifact.discount_curve.len());
        for point in &artifact.discount_curve {
            let mut locked = Decimal::ZERO;
            for (capital, release_secs) in &position_release_secs {
                if prior < *release_secs {
                    locked = locked.checked_add(capital.inner()).ok_or_else(|| {
                        ReportError::NumericOverflow {
                            field: "existing_portfolio.capital_occupancy",
                            detail: "existing locked capital overflowed Decimal".to_owned(),
                        }
                    })?;
                }
            }
            capital_occupancy.push(CapitalOccupancyBucket {
                end_secs: point.end_secs,
                locked_usd: Usd::new(quantize_venue_amount(locked)),
            });
            prior = point.end_secs;
        }

        Ok(ExistingPortfolioState {
            existing_open_capital_usd: Usd::new(quantize_venue_amount(open_capital)),
            existing_open_recommendations: open_count,
            current_drawdown_usd,
            scenario_cashflows,
            capital_occupancy,
        })
    }
}

fn validate_ladder_input(input: &ExecutableTierLadderSeedInput<'_>) -> QuantResult<()> {
    if input.bids.is_empty()
        || input.asks.is_empty()
        || !input.limit_price.is_positive()
        || input.limit_price > Price::ONE
        || !input.max_notional_usd.is_positive()
        || input.asks.windows(2).any(|levels| {
            levels[0].price_decimal() > levels[1].price_decimal()
                || !levels[0].size_decimal().is_positive()
        })
        || input
            .asks
            .last()
            .is_some_and(|level| !level.size_decimal().is_positive())
        || input.bids.windows(2).any(|levels| {
            levels[0].price_decimal() < levels[1].price_decimal()
                || !levels[0].size_decimal().is_positive()
        })
        || input
            .bids
            .last()
            .is_some_and(|level| !level.size_decimal().is_positive())
    {
        return Err(ReportError::InvariantViolation {
            stage: "economic_tier_ladder",
            detail: "ask order, depth, price limit, or governed notional cap is invalid".to_owned(),
        }
        .into());
    }
    Ok(())
}

fn cumulative_targets(
    asks: &[BookLevel],
    limit_price: Price,
    maximum_shares: Shares,
) -> QuantResult<Vec<Shares>> {
    let mut cumulative = Decimal::ZERO;
    let mut targets = Vec::new();
    for level in asks
        .iter()
        .take_while(|level| level.price_decimal() <= limit_price)
    {
        cumulative = cumulative
            .checked_add(level.size_decimal().inner())
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "economic_tier.cumulative_shares",
                detail: "ask depth overflowed Decimal".to_owned(),
            })?
            .min(maximum_shares.inner());
        if cumulative > Decimal::ZERO {
            let target = Shares::new(cumulative);
            if targets.last().copied() != Some(target) {
                targets.push(target);
            }
        }
        if cumulative == maximum_shares.inner() {
            break;
        }
    }
    Ok(targets)
}

fn visible_liquidity(asks: &[BookLevel], limit_price: Price) -> QuantResult<Usd> {
    let mut total = Decimal::ZERO;
    for level in asks
        .iter()
        .take_while(|level| level.price_decimal() <= limit_price)
    {
        let notional = level
            .price_decimal()
            .inner()
            .checked_mul(level.size_decimal().inner())
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "economic_tier.visible_liquidity_usd",
                detail: "visible ask notional overflowed Decimal".to_owned(),
            })?;
        total = total
            .checked_add(notional)
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "economic_tier.visible_liquidity_usd",
                detail: "visible ask liquidity overflowed Decimal".to_owned(),
            })?;
    }
    Ok(Usd::new(quantize_venue_amount(total)))
}

fn visible_shares(levels: &[BookLevel]) -> QuantResult<Shares> {
    let total = levels.iter().try_fold(Decimal::ZERO, |total, level| {
        total
            .checked_add(level.size_decimal().inner())
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "economic_tier.observed_exit_capacity_shares",
                detail: "visible bid depth overflowed Decimal".to_owned(),
            })
    })?;
    if total <= Decimal::ZERO {
        return Err(ReportError::InvariantViolation {
            stage: "economic_tier",
            detail: "candidate-side bid depth has no positive exit capacity".to_owned(),
        }
        .into());
    }
    Ok(Shares::new(total))
}

/// Converts an entry tier through every promoted joint scenario without a fallback.
pub struct EconomicTierFactory;

impl EconomicTierFactory {
    /// Build one immutable tier on the common discounted net-USD scale.
    pub fn build(
        seed: ExecutableTierSeed,
        sealed_artifact: &SealedPortfolioScenarioArtifact,
    ) -> QuantResult<ExecutableEconomicTier> {
        let artifact = sealed_artifact.artifact();
        seed.validate()?;
        let outcome_offset = exact_outcome_offset(&seed, artifact)?;
        let mut scenario_cashflows = Vec::with_capacity(artifact.scenarios.len());
        let mut maximum_reservation_secs = 0_u64;
        let mut outcome_hashes = Vec::with_capacity(artifact.scenarios.len());
        for scenario in &artifact.scenarios {
            let outcome = scenario
                .market_outcomes
                .get(outcome_offset)
                .filter(|outcome| outcome_matches_seed(outcome, &seed))
                .ok_or_else(|| ReportError::ScenarioArtifact {
                    detail: format!(
                        "scenario {} differs from the sealed market-outcome layout at offset {}",
                        scenario.scenario_index, outcome_offset
                    ),
                })?;
            if outcome.discounted_exit_cash_per_share_usd.is_negative()
                || outcome.capital_release_secs == 0
            {
                return Err(ReportError::ScenarioArtifact {
                    detail: format!(
                        "scenario {} contains negative exit cash or zero capital-release time",
                        scenario.scenario_index
                    ),
                }
                .into());
            }
            let cashflow = scenario_cashflow(
                &seed,
                scenario.scenario_index,
                scenario.scenario_state_hash,
                outcome,
                artifact,
            )?;
            maximum_reservation_secs = maximum_reservation_secs.max(
                cashflow
                    .capital_occupancy
                    .iter()
                    .map(|slice| slice.duration_secs)
                    .sum(),
            );
            scenario_cashflows.push(cashflow);
            outcome_hashes.push(outcome.outcome_lineage_hash);
        }

        let (nominal_expected, robust_expected, nominal_profit_bps, lower_profit_bps, width_bps) =
            distribution_economics(&scenario_cashflows, artifact)?;
        let max_loss = scenario_cashflows
            .iter()
            .filter_map(|cashflow| {
                cashflow
                    .risk_net_usd
                    .is_negative()
                    .then_some(-cashflow.risk_net_usd)
            })
            .max()
            .unwrap_or(Usd::ZERO);
        let hard_reservation_envelope = hard_reservation_envelope(
            seed.entry_execution.hard_reserved_cash_usd(),
            hard_reservation_secs(&seed.entry_execution, maximum_reservation_secs),
            artifact,
        )?;
        let capital_hours = nominal_capital_hours(&scenario_cashflows, artifact)?;

        let lineage_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/executable-economic-tier-lineage",
            2,
            &TierLineagePreimage {
                seed: &seed,
                scenario_artifact_hash: artifact.content_hash,
                outcome_hashes: &outcome_hashes,
                scenario_cashflows: &scenario_cashflows,
                hard_reservation_envelope: &hard_reservation_envelope,
            },
        )?;
        let economic_tier_id = EconomicTierId::from_content_hash(&lineage_hash);

        Ok(ExecutableEconomicTier {
            economic_tier_id,
            report_route_run_id: seed.report_route_run_id,
            candidate_id: seed.candidate_id,
            tier_ordinal: seed.tier_ordinal,
            route: seed.route,
            market_id: seed.market_id,
            event_id: seed.event_id,
            category: seed.category,
            token_id: seed.token_id,
            outcome_side: seed.outcome_side,
            entry_execution: seed.entry_execution,
            profit_probability_lower_bps: lower_profit_bps,
            probability_interval_width_bps: width_bps,
            scenario_cashflows,
            hard_reservation_envelope,
            economics: RecommendationEconomics {
                profit_probability_bps: Bps::new(Decimal::from(nominal_profit_bps)),
                nominal_expected_net_usd: nominal_expected,
                robust_expected_net_usd: robust_expected,
                max_loss_usd: max_loss,
                cvar_contribution_usd: Usd::ZERO,
                capital_occupancy_usd_hours: capital_hours,
                marginal_portfolio_value_usd: Usd::ZERO,
            },
            lineage_hash,
        })
    }
}

impl ExecutableTierSeed {
    fn validate(&self) -> QuantResult<()> {
        let entry_valid = match &self.entry_execution {
            EntryExecutionEconomics::Aggressive(entry) => {
                entry.requested_shares.is_positive()
                    && entry.filled_shares == entry.requested_shares
                    && entry.limit_price.is_positive()
                    && entry.limit_price <= Price::ONE
                    && entry.entry_vwap.is_positive()
                    && entry.entry_vwap <= entry.limit_price
                    && entry.immediate_cost.cash_outlay_usd.is_positive()
                    && !entry.slippage_usd.is_negative()
                    && entry.visible_liquidity_usd.is_positive()
            }
            EntryExecutionEconomics::Passive(entry) => {
                entry.requested_shares.is_positive()
                    && entry.limit_price.is_positive()
                    && entry.limit_price <= Price::ONE
                    && entry.good_til_secs > 0
                    && entry.hard_reserved_cash_usd == entry.full_fill_cost.cash_outlay_usd
                    && entry.hard_reserved_cash_usd.is_positive()
                    && entry.expected_filled_shares <= entry.requested_shares
                    && entry.visible_liquidity_usd.is_positive()
                    && entry.fill_distribution.validate().is_ok()
            }
        };
        if self.tier_ordinal == 0
            || !self.observed_exit_capacity_shares.is_positive()
            || !entry_valid
        {
            return Err(ReportError::InvariantViolation {
                stage: "economic_tier",
                detail: "route-specific entry tier violates execution or reservation invariants"
                    .to_owned(),
            }
            .into());
        }
        Ok(())
    }
}

fn exact_outcome_offset(
    seed: &ExecutableTierSeed,
    artifact: &PortfolioScenarioArtifact,
) -> QuantResult<usize> {
    let outcomes = &artifact
        .scenarios
        .first()
        .ok_or_else(|| ReportError::ScenarioArtifact {
            detail: "sealed scenario artifact is empty".to_owned(),
        })?
        .market_outcomes;
    outcomes
        .binary_search_by(|outcome| {
            (
                outcome.route,
                outcome.market_id.as_str(),
                outcome.token_id.as_str(),
                outcome.outcome_side.as_str(),
            )
                .cmp(&(
                    seed.route,
                    seed.market_id.as_str(),
                    seed.token_id.as_str(),
                    seed.outcome_side.as_str(),
                ))
        })
        .map_err(|_| {
            ReportError::ScenarioArtifact {
                detail: format!(
                    "scenario has no outcome for route {:?}, market {}, token {}, side {}",
                    seed.route, seed.market_id, seed.token_id, seed.outcome_side
                ),
            }
            .into()
        })
}

fn outcome_matches_seed(outcome: &ScenarioMarketOutcome, seed: &ExecutableTierSeed) -> bool {
    outcome.route == seed.route
        && outcome.market_id == seed.market_id
        && outcome.token_id == seed.token_id
        && outcome.outcome_side == seed.outcome_side
}

fn scenario_cashflow(
    seed: &ExecutableTierSeed,
    scenario_index: u32,
    scenario_state_hash: ContentHash,
    outcome: &ScenarioMarketOutcome,
    artifact: &PortfolioScenarioArtifact,
) -> QuantResult<ScenarioExecutionCashflow> {
    match &seed.entry_execution {
        EntryExecutionEconomics::Aggressive(entry) => {
            let discounted_exit_cash = scaled_exit_cash(
                entry.filled_shares,
                outcome.discounted_exit_cash_per_share_usd,
                Bps::ZERO,
                entry.immediate_cost.principal_usd,
            )?;
            let discounted_net = checked_net(
                discounted_exit_cash,
                entry.immediate_cost.cash_outlay_usd,
                Usd::ZERO,
                Usd::ZERO,
            )?;
            Ok(ScenarioExecutionCashflow {
                scenario_index,
                entry_execution: ScenarioEntryExecution::AggressiveFill,
                filled_shares: entry.filled_shares,
                immediate_cash_outlay_usd: entry.immediate_cost.cash_outlay_usd,
                discounted_exit_cash_usd: discounted_exit_cash,
                delayed_maker_rebate_usd: Usd::ZERO,
                discounted_maker_rebate_usd: Usd::ZERO,
                capital_cost_usd: Usd::ZERO,
                capital_occupancy: vec![ScenarioCapitalOccupancySlice {
                    locked_cash_usd: entry.immediate_cost.cash_outlay_usd,
                    duration_secs: outcome.capital_release_secs,
                }],
                discounted_net_usd: discounted_net,
                risk_net_usd: discounted_net,
            })
        }
        EntryExecutionEconomics::Passive(entry) => {
            let state = passive_state(entry, scenario_state_hash, outcome.outcome_lineage_hash)?;
            passive_cashflow(entry, state, scenario_index, outcome, artifact)
        }
    }
}

fn passive_state(
    entry: &PassiveEntryEconomics,
    scenario_state_hash: ContentHash,
    outcome_hash: ContentHash,
) -> QuantResult<PassiveFillState> {
    let draw_hash = CanonicalDigest::content_hash_typed(
        "quant-pivot/scenario-passive-entry-draw",
        1,
        &(
            entry.fill_distribution.source_evidence_hash,
            scenario_state_hash,
            outcome_hash,
        ),
    )?;
    let bytes = draw_hash.as_bytes();
    let draw = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) % 10_000;
    let mut cumulative = 0_u32;
    for state in &entry.fill_distribution.states {
        cumulative = cumulative
            .checked_add(state.probability_bps)
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "scenario.passive_probability_mass",
                detail: "passive distribution probability overflowed u32".to_owned(),
            })?;
        if draw < cumulative {
            return Ok(*state);
        }
    }
    Err(ReportError::InvariantViolation {
        stage: "economic_passive_scenario",
        detail: "passive distribution did not cover the complete probability draw".to_owned(),
    }
    .into())
}

fn passive_cashflow(
    entry: &PassiveEntryEconomics,
    state: PassiveFillState,
    scenario_index: u32,
    outcome: &ScenarioMarketOutcome,
    artifact: &PortfolioScenarioArtifact,
) -> QuantResult<ScenarioExecutionCashflow> {
    if state.kind == PassiveFillStateKind::NoFill {
        let capital_cost =
            capital_cost(entry.hard_reserved_cash_usd, entry.good_til_secs, artifact)?;
        return Ok(ScenarioExecutionCashflow {
            scenario_index,
            entry_execution: ScenarioEntryExecution::PassiveNoFill {
                good_til_secs: entry.good_til_secs,
            },
            filled_shares: Shares::ZERO,
            immediate_cash_outlay_usd: Usd::ZERO,
            discounted_exit_cash_usd: Usd::ZERO,
            delayed_maker_rebate_usd: Usd::ZERO,
            discounted_maker_rebate_usd: Usd::ZERO,
            capital_cost_usd: capital_cost,
            capital_occupancy: vec![ScenarioCapitalOccupancySlice {
                locked_cash_usd: entry.hard_reserved_cash_usd,
                duration_secs: entry.good_til_secs,
            }],
            discounted_net_usd: -capital_cost,
            risk_net_usd: -capital_cost,
        });
    }

    let ratio = Decimal::from(state.fill_ratio_bps) / Decimal::from(DISTRIBUTION_MASS_BPS);
    let filled_shares = Shares::new(
        (entry.requested_shares.inner() * ratio)
            .round_dp_with_strategy(6, RoundingStrategy::ToZero),
    );
    if !filled_shares.is_positive() {
        return Err(ReportError::InvariantViolation {
            stage: "economic_passive_scenario",
            detail: "positive passive fill state rounded to zero executable shares".to_owned(),
        }
        .into());
    }
    let immediate_cost = passive_fill_cost(entry, filled_shares)?;
    let fill_latency_secs = state.fill_latency_ms.div_ceil(1_000);
    let reservation_cost = capital_cost(entry.hard_reserved_cash_usd, fill_latency_secs, artifact)?;
    let delayed_rebate = Usd::new(quantize_venue_amount(
        entry
            .full_fill_maker_rebate
            .map_or(Decimal::ZERO, |incentive| {
                incentive.expected_rebate_usd.inner()
            })
            * ratio,
    ));
    let rebate_delay_secs = incentive_delay_secs(entry)?;
    let discounted_rebate = discount_cash(delayed_rebate, rebate_delay_secs, artifact)?;
    let discounted_exit_cash = scaled_exit_cash(
        filled_shares,
        outcome.discounted_exit_cash_per_share_usd,
        state.post_fill_markout_bps,
        immediate_cost.principal_usd,
    )?;
    let discounted_net = checked_net(
        discounted_exit_cash,
        immediate_cost.cash_outlay_usd,
        discounted_rebate,
        reservation_cost,
    )?;
    let risk_net = checked_net(
        discounted_exit_cash,
        immediate_cost.cash_outlay_usd,
        Usd::ZERO,
        reservation_cost,
    )?;
    let mut occupancy = vec![ScenarioCapitalOccupancySlice {
        locked_cash_usd: entry.hard_reserved_cash_usd,
        duration_secs: fill_latency_secs,
    }];
    let invested_secs = outcome
        .capital_release_secs
        .saturating_sub(fill_latency_secs);
    if invested_secs > 0 {
        occupancy.push(ScenarioCapitalOccupancySlice {
            locked_cash_usd: immediate_cost.cash_outlay_usd,
            duration_secs: invested_secs,
        });
    }
    let entry_execution = match state.kind {
        PassiveFillStateKind::PartialFill => ScenarioEntryExecution::PassivePartialFill {
            fill_latency_ms: state.fill_latency_ms,
            post_fill_markout_bps: state.post_fill_markout_bps,
        },
        PassiveFillStateKind::FullFill => ScenarioEntryExecution::PassiveFullFill {
            fill_latency_ms: state.fill_latency_ms,
            post_fill_markout_bps: state.post_fill_markout_bps,
        },
        PassiveFillStateKind::NoFill => {
            return Err(ReportError::InvariantViolation {
                stage: "economic_passive_scenario",
                detail: "no-fill passive state escaped the zero-fill branch".to_owned(),
            }
            .into());
        }
    };
    Ok(ScenarioExecutionCashflow {
        scenario_index,
        entry_execution,
        filled_shares,
        immediate_cash_outlay_usd: immediate_cost.cash_outlay_usd,
        discounted_exit_cash_usd: discounted_exit_cash,
        delayed_maker_rebate_usd: delayed_rebate,
        discounted_maker_rebate_usd: discounted_rebate,
        capital_cost_usd: reservation_cost,
        capital_occupancy: occupancy,
        discounted_net_usd: discounted_net,
        risk_net_usd: risk_net,
    })
}

fn passive_fill_cost(
    entry: &PassiveEntryEconomics,
    filled_shares: Shares,
) -> QuantResult<ImmediateExecutionCost> {
    if !entry.full_fill_cost.venue_fee_usd.is_zero()
        || !entry.full_fill_cost.builder_fee_usd.is_zero()
    {
        return Err(ReportError::InvariantViolation {
            stage: "economic_passive_scenario",
            detail: "CLOB V2 passive entry unexpectedly contains an immediate maker fee".to_owned(),
        }
        .into());
    }
    ImmediateExecutionCost::new(
        Usd::new(quantize_venue_amount(
            entry.limit_price.inner() * filled_shares.inner(),
        )),
        Usd::ZERO,
        Usd::ZERO,
    )
    .map_err(|detail| {
        ReportError::InvariantViolation {
            stage: "economic_passive_scenario",
            detail: detail.to_owned(),
        }
        .into()
    })
}

fn scaled_exit_cash(
    shares: Shares,
    discounted_cash_per_share: Usd,
    markout_bps: Bps,
    principal: Usd,
) -> QuantResult<Usd> {
    let exit_cash = shares
        .inner()
        .checked_mul(discounted_cash_per_share.inner())
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario.discounted_exit_cash",
            detail: "shares multiplied by discounted per-share cash overflowed Decimal".to_owned(),
        })?;
    let markout = principal
        .inner()
        .checked_mul(markout_bps.to_fraction())
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario.passive_markout_cash",
            detail: "passive principal multiplied by markout overflowed Decimal".to_owned(),
        })?;
    Ok(Usd::new(quantize_venue_amount(
        exit_cash
            .checked_add(markout)
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "scenario.discounted_exit_cash",
                detail: "discounted exit cash plus markout overflowed Decimal".to_owned(),
            })?
            .max(Decimal::ZERO),
    )))
}

fn checked_net(
    exit_cash: Usd,
    immediate_outlay: Usd,
    discounted_rebate: Usd,
    capital_cost: Usd,
) -> QuantResult<Usd> {
    let value = exit_cash
        .inner()
        .checked_sub(immediate_outlay.inner())
        .and_then(|value| value.checked_add(discounted_rebate.inner()))
        .and_then(|value| value.checked_sub(capital_cost.inner()))
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario.discounted_net_usd",
            detail: "scenario entry, exit, incentive, and capital-cost sum overflowed Decimal"
                .to_owned(),
        })?;
    Ok(Usd::new(quantize_venue_amount(value)))
}

fn incentive_delay_secs(entry: &PassiveEntryEconomics) -> QuantResult<u64> {
    let Some(incentive) = entry.full_fill_maker_rebate else {
        return Ok(0);
    };
    u64::try_from(
        incentive
            .expected_credit_at
            .signed_duration_since(entry.decision_at)
            .num_seconds()
            .max(0),
    )
    .map_err(|error| {
        ReportError::NumericOverflow {
            field: "scenario.maker_rebate_delay_secs",
            detail: error.to_string(),
        }
        .into()
    })
}

fn capital_cost(
    locked: Usd,
    duration_secs: u64,
    artifact: &PortfolioScenarioArtifact,
) -> QuantResult<Usd> {
    let point = artifact
        .discount_curve
        .iter()
        .find(|point| duration_secs <= point.end_secs)
        .ok_or_else(|| ReportError::ScenarioArtifact {
            detail: format!("discount curve does not cover {duration_secs}s capital reservation"),
        })?;
    let value = locked
        .inner()
        .checked_mul(Decimal::from(point.annualized_cost_bps))
        .and_then(|value| value.checked_mul(Decimal::from(duration_secs)))
        .and_then(|value| value.checked_div(Decimal::from(DISTRIBUTION_MASS_BPS)))
        .and_then(|value| value.checked_div(Decimal::from(SECONDS_PER_YEAR)))
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario.capital_cost_usd",
            detail: "capital reservation cost overflowed Decimal".to_owned(),
        })?;
    Ok(Usd::new(quantize_venue_amount(value)))
}

fn discount_cash(
    cash: Usd,
    delay_secs: u64,
    artifact: &PortfolioScenarioArtifact,
) -> QuantResult<Usd> {
    if cash.is_zero() || delay_secs == 0 {
        return Ok(cash);
    }
    let point = artifact
        .discount_curve
        .iter()
        .find(|point| delay_secs <= point.end_secs)
        .ok_or_else(|| ReportError::ScenarioArtifact {
            detail: format!("discount curve does not cover {delay_secs}s incentive delay"),
        })?;
    let carrying_rate = Decimal::from(point.annualized_cost_bps)
        .checked_mul(Decimal::from(delay_secs))
        .and_then(|value| value.checked_div(Decimal::from(DISTRIBUTION_MASS_BPS)))
        .and_then(|value| value.checked_div(Decimal::from(SECONDS_PER_YEAR)))
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario.maker_rebate_discount",
            detail: "maker rebate discount factor overflowed Decimal".to_owned(),
        })?;
    let discounted = cash
        .inner()
        .checked_div(Decimal::ONE + carrying_rate)
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "scenario.discounted_maker_rebate_usd",
            detail: "maker rebate discount division failed".to_owned(),
        })?;
    Ok(Usd::new(quantize_venue_amount(discounted)))
}

fn distribution_economics(
    cashflows: &[ScenarioExecutionCashflow],
    artifact: &PortfolioScenarioArtifact,
) -> QuantResult<(Usd, Usd, u32, u32, u32)> {
    let mut expected = Vec::with_capacity(artifact.distributions.len());
    let mut profit_masses = Vec::with_capacity(artifact.distributions.len());
    let mut nominal = None;
    for distribution in &artifact.distributions {
        let mut numerator = Decimal::ZERO;
        let mut profit_mass = 0_u32;
        let mut seen = HashSet::new();
        for weight in &distribution.weights {
            if !seen.insert(weight.scenario_index) {
                return Err(ReportError::ScenarioArtifact {
                    detail: format!(
                        "distribution {} repeats scenario {}",
                        distribution.distribution_id, weight.scenario_index
                    ),
                }
                .into());
            }
            let cashflow = cashflows
                .get(usize::try_from(weight.scenario_index).map_err(|error| {
                    ReportError::NumericOverflow {
                        field: "scenario_index",
                        detail: error.to_string(),
                    }
                })?)
                .filter(|cashflow| cashflow.scenario_index == weight.scenario_index)
                .ok_or_else(|| ReportError::ScenarioArtifact {
                    detail: format!(
                        "distribution {} references absent scenario {}",
                        distribution.distribution_id, weight.scenario_index
                    ),
                })?;
            let weighted = cashflow
                .discounted_net_usd
                .inner()
                .checked_mul(Decimal::from(weight.probability_bps))
                .ok_or_else(|| ReportError::NumericOverflow {
                    field: "scenario.expected_net_usd",
                    detail: "weighted scenario cash flow overflowed Decimal".to_owned(),
                })?;
            numerator =
                numerator
                    .checked_add(weighted)
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "scenario.expected_net_usd",
                        detail: "scenario expectation sum overflowed Decimal".to_owned(),
                    })?;
            if cashflow.discounted_net_usd.is_positive() {
                profit_mass = profit_mass
                    .checked_add(weight.probability_bps)
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "scenario.profit_probability_bps",
                        detail: "profit probability mass overflowed u32".to_owned(),
                    })?;
            }
        }
        let value = Usd::new(numerator / Decimal::from(DISTRIBUTION_MASS_BPS));
        if distribution.nominal && nominal.replace((value, profit_mass)).is_some() {
            return Err(ReportError::ScenarioArtifact {
                detail: "more than one nominal distribution exists".to_owned(),
            }
            .into());
        }
        expected.push(value);
        profit_masses.push(profit_mass);
    }
    let (nominal_expected, nominal_profit) =
        nominal.ok_or_else(|| ReportError::ScenarioArtifact {
            detail: "nominal distribution is absent".to_owned(),
        })?;
    let robust_expected =
        expected
            .into_iter()
            .min()
            .ok_or_else(|| ReportError::ScenarioArtifact {
                detail: "allowed distribution set is empty".to_owned(),
            })?;
    let lower =
        profit_masses
            .iter()
            .copied()
            .min()
            .ok_or_else(|| ReportError::ScenarioArtifact {
                detail: "allowed distribution set is empty".to_owned(),
            })?;
    let upper =
        profit_masses
            .iter()
            .copied()
            .max()
            .ok_or_else(|| ReportError::ScenarioArtifact {
                detail: "allowed distribution set is empty".to_owned(),
            })?;
    Ok((
        nominal_expected,
        robust_expected,
        nominal_profit,
        lower,
        upper.saturating_sub(lower),
    ))
}

fn hard_reservation_envelope(
    reserved: Usd,
    reservation_secs: u64,
    artifact: &PortfolioScenarioArtifact,
) -> QuantResult<Vec<HardReservationBucket>> {
    let final_bucket =
        artifact
            .discount_curve
            .last()
            .ok_or_else(|| ReportError::ScenarioArtifact {
                detail: "discount curve is empty".to_owned(),
            })?;
    if reservation_secs > final_bucket.end_secs {
        return Err(ReportError::ScenarioArtifact {
            detail: format!(
                "hard cash reservation at {reservation_secs}s exceeds final governed bucket {}s",
                final_bucket.end_secs
            ),
        }
        .into());
    }
    let mut prior = 0_u64;
    let mut envelope = Vec::with_capacity(artifact.discount_curve.len());
    for point in &artifact.discount_curve {
        point
            .end_secs
            .checked_sub(prior)
            .ok_or_else(|| ReportError::ScenarioArtifact {
                detail: "discount curve bucket boundaries are not increasing".to_owned(),
            })?;
        envelope.push(HardReservationBucket {
            end_secs: point.end_secs,
            reserved_cash_usd: if prior < reservation_secs {
                reserved
            } else {
                Usd::ZERO
            },
        });
        prior = point.end_secs;
    }
    Ok(envelope)
}

fn hard_reservation_secs(entry: &EntryExecutionEconomics, maximum_scenario_secs: u64) -> u64 {
    match entry {
        EntryExecutionEconomics::Aggressive(_) => maximum_scenario_secs,
        EntryExecutionEconomics::Passive(entry) => entry.good_til_secs,
    }
}

fn nominal_capital_hours(
    cashflows: &[ScenarioExecutionCashflow],
    artifact: &PortfolioScenarioArtifact,
) -> QuantResult<UsdHours> {
    let nominal = artifact
        .distributions
        .iter()
        .find(|distribution| distribution.nominal)
        .ok_or_else(|| ReportError::ScenarioArtifact {
            detail: "nominal distribution is absent".to_owned(),
        })?;
    let mut weighted_hours = Decimal::ZERO;
    for weight in &nominal.weights {
        let cashflow = cashflows
            .get(usize::try_from(weight.scenario_index).map_err(|error| {
                ReportError::NumericOverflow {
                    field: "scenario_index",
                    detail: error.to_string(),
                }
            })?)
            .filter(|cashflow| cashflow.scenario_index == weight.scenario_index)
            .ok_or_else(|| ReportError::ScenarioArtifact {
                detail: format!(
                    "nominal distribution references absent scenario {}",
                    weight.scenario_index
                ),
            })?;
        let scenario_hours = quantize_venue_amount(cashflow.capital_occupancy.iter().try_fold(
            Decimal::ZERO,
            |total, slice| {
                let value = slice
                    .locked_cash_usd
                    .inner()
                    .checked_mul(Decimal::from(slice.duration_secs))
                    .and_then(|value| value.checked_div(Decimal::from(SECONDS_PER_HOUR)))
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "scenario.capital_occupancy_usd_hours",
                        detail: "scenario capital occupancy overflowed Decimal".to_owned(),
                    })?;
                total
                    .checked_add(value)
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "scenario.capital_occupancy_usd_hours",
                        detail: "scenario capital occupancy sum overflowed Decimal".to_owned(),
                    })
            },
        )?);
        weighted_hours = weighted_hours
            .checked_add(scenario_hours * Decimal::from(weight.probability_bps))
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "scenario.nominal_capital_occupancy_usd_hours",
                detail: "nominal capital occupancy overflowed Decimal".to_owned(),
            })?;
    }
    Ok(UsdHours::new(quantize_venue_amount(
        weighted_hours / Decimal::from(DISTRIBUTION_MASS_BPS),
    )))
}
