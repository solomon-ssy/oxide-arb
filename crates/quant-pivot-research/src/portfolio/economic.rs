//! Exact conversion from executable entry tiers to unified scenario USD cash flows.

use std::collections::HashSet;

use chrono::{DateTime, Utc};

use quant_pivot_error::{QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::market::book::BookLevel,
    domain::quant::{
        CapitalOccupancyBucket, EntryEconomics, ExecutableEconomicTier, ExistingPortfolioState,
        PortfolioScenarioArtifact, RecommendationEconomics, ScenarioCashflow,
        ScenarioMarketOutcome,
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
    },
};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    execution_semantics::{
        BookWalkOutcome, LiquidityRole, PitFeeSchedule, walk_buy_cash_budget, walk_buy_exact_shares,
    },
    precision::quantize_venue_amount,
};

use super::{AccountSnapshot, SealedPortfolioScenarioArtifact};

const DISTRIBUTION_MASS_BPS: u32 = 10_000;
const SECONDS_PER_HOUR: u64 = 3_600;

#[derive(Serialize)]
struct TierLineagePreimage<'a> {
    seed: &'a ExecutableTierSeed,
    scenario_artifact_hash: ContentHash,
    outcome_hashes: &'a [ContentHash],
    scenario_cashflows: &'a [ScenarioCashflow],
    capital_occupancy: &'a [CapitalOccupancyBucket],
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
    pub shares: Shares,
    /// Frozen candidate-side bid depth available to a sell-to-close at the decision boundary.
    /// Scenario factors stress this exogenous venue capacity; it must never be derived from the
    /// requested tier size.
    pub observed_exit_capacity_shares: Shares,
    pub entry: EntryEconomics,
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
            let cash_outlay = quantize_venue_amount(
                fill.gross_order_amount
                    .inner()
                    .checked_add(fill.expected_fee.inner())
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "economic_tier.entry.notional_usd",
                        detail: "principal plus fee overflowed Decimal".to_owned(),
                    })?,
            );
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
                fill.gross_order_amount
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
                shares,
                observed_exit_capacity_shares,
                entry: EntryEconomics {
                    notional_usd: Usd::new(cash_outlay),
                    entry_vwap: vwap,
                    fee_usd: Usd::new(quantize_venue_amount(fill.expected_fee.inner())),
                    slippage_usd: Usd::new(slippage),
                    visible_liquidity_usd: visible_liquidity,
                },
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
        let cash_outlay = quantize_venue_amount(
            fill.gross_order_amount
                .inner()
                .checked_add(fill.expected_fee.inner())
                .ok_or_else(|| ReportError::NumericOverflow {
                    field: "economic_cash_tier.notional_usd",
                    detail: "principal plus fee overflowed Decimal".to_owned(),
                })?,
        );
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
            .gross_order_amount
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
            shares: fill.filled_shares,
            observed_exit_capacity_shares: visible_shares(input.bids)?,
            entry: EntryEconomics {
                notional_usd: Usd::new(cash_outlay),
                entry_vwap: vwap,
                fee_usd: Usd::new(quantize_venue_amount(fill.expected_fee.inner())),
                slippage_usd: Usd::new(quantize_venue_amount(slippage)),
                visible_liquidity_usd: visible_liquidity(input.asks, input.limit_price)?,
            },
            source_lineage_hash: input.source_lineage_hash,
        }))
    }
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
        let mut release_secs = 0_u64;
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
            let exit_cash = quantize_venue_amount(
                seed.shares
                    .inner()
                    .checked_mul(outcome.discounted_exit_cash_per_share_usd.inner())
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "scenario.discounted_exit_cash",
                        detail: "shares multiplied by discounted per-share cash overflowed Decimal"
                            .to_owned(),
                    })?,
            );
            let net = quantize_venue_amount(
                exit_cash
                    .checked_sub(seed.entry.notional_usd.inner())
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "scenario.discounted_net_usd",
                        detail: "discounted exit cash minus entry outflow overflowed Decimal"
                            .to_owned(),
                    })?,
            );
            scenario_cashflows.push(ScenarioCashflow {
                scenario_index: scenario.scenario_index,
                discounted_net_usd: Usd::new(net),
            });
            release_secs = release_secs.max(outcome.capital_release_secs);
            outcome_hashes.push(outcome.outcome_lineage_hash);
        }

        let (nominal_expected, robust_expected, nominal_profit_bps, lower_profit_bps, width_bps) =
            distribution_economics(&scenario_cashflows, artifact)?;
        let max_loss = scenario_cashflows
            .iter()
            .filter_map(|cashflow| {
                cashflow
                    .discounted_net_usd
                    .is_negative()
                    .then_some(-cashflow.discounted_net_usd)
            })
            .max()
            .unwrap_or(Usd::ZERO);
        let (capital_occupancy, capital_hours) =
            capital_occupancy(seed.entry.notional_usd, release_secs, artifact)?;

        let lineage_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/executable-economic-tier-lineage",
            1,
            &TierLineagePreimage {
                seed: &seed,
                scenario_artifact_hash: artifact.content_hash,
                outcome_hashes: &outcome_hashes,
                scenario_cashflows: &scenario_cashflows,
                capital_occupancy: &capital_occupancy,
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
            shares: seed.shares,
            entry: seed.entry,
            profit_probability_lower_bps: lower_profit_bps,
            probability_interval_width_bps: width_bps,
            scenario_cashflows,
            capital_occupancy,
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
        if self.tier_ordinal == 0
            || !self.shares.is_positive()
            || !self.observed_exit_capacity_shares.is_positive()
            || !self.entry.notional_usd.is_positive()
            || !self.entry.entry_vwap.is_positive()
            || self.entry.entry_vwap > Price::ONE
            || self.entry.fee_usd.is_negative()
            || self.entry.slippage_usd.is_negative()
            || !self.entry.visible_liquidity_usd.is_positive()
        {
            return Err(ReportError::InvariantViolation {
                stage: "economic_tier",
                detail:
                    "entry tier identity, shares, price, fees, slippage, or liquidity is invalid"
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

fn distribution_economics(
    cashflows: &[ScenarioCashflow],
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

fn capital_occupancy(
    locked: Usd,
    release_secs: u64,
    artifact: &PortfolioScenarioArtifact,
) -> QuantResult<(Vec<CapitalOccupancyBucket>, UsdHours)> {
    let final_bucket =
        artifact
            .discount_curve
            .last()
            .ok_or_else(|| ReportError::ScenarioArtifact {
                detail: "discount curve is empty".to_owned(),
            })?;
    if release_secs > final_bucket.end_secs {
        return Err(ReportError::ScenarioArtifact {
            detail: format!(
                "capital release at {release_secs}s exceeds final governed bucket {}s",
                final_bucket.end_secs
            ),
        }
        .into());
    }
    let mut prior = 0_u64;
    let mut locked_secs = 0_u64;
    let mut occupancy = Vec::with_capacity(artifact.discount_curve.len());
    for point in &artifact.discount_curve {
        let active = prior < release_secs;
        let duration =
            point
                .end_secs
                .checked_sub(prior)
                .ok_or_else(|| ReportError::ScenarioArtifact {
                    detail: "discount curve bucket boundaries are not increasing".to_owned(),
                })?;
        if active {
            locked_secs =
                locked_secs
                    .checked_add(duration)
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "capital_occupancy_secs",
                        detail: "conservative bucket duration overflowed u64".to_owned(),
                    })?;
        }
        occupancy.push(CapitalOccupancyBucket {
            end_secs: point.end_secs,
            locked_usd: if active { locked } else { Usd::ZERO },
        });
        prior = point.end_secs;
    }
    let hours = locked
        .inner()
        .checked_mul(Decimal::from(locked_secs))
        .and_then(|value| value.checked_div(Decimal::from(SECONDS_PER_HOUR)))
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "capital_occupancy_usd_hours",
            detail: "time-weighted capital overflowed Decimal".to_owned(),
        })?;
    Ok((occupancy, UsdHours::new(quantize_venue_amount(hours))))
}
