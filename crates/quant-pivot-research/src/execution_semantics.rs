//! Pure venue execution semantics shared by research and serving.

use chrono::{DateTime, Days, Utc};
use quant_pivot_models::{
    domain::market::{
        book::BookLevel,
        fee::{
            BuilderFeeAttribution, DeferredVenueIncentive, FrozenMakerRebateSchedule,
            ImmediateExecutionCost, MakerRebateEligibility, MarketFeeSchedule,
            MarketMakerRebateSchedule,
        },
    },
    enums::{
        common::{Side, TickSize},
        quant::FillRequirement,
    },
    hashing::CanonicalDigest,
    types::{Bps, ContentHash, PassivePlacement, PayoutRatio, Price, Shares, Usd},
};
use rust_decimal::{Decimal, MathematicalOps, RoundingStrategy};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::precision::quantize_venue_amount;

/// Versioned identity of the shared book-walk, queue, and fee semantics.
pub const EXECUTION_SEMANTICS_VERSION: &str = "polymarket_execution_semantics_v2";

/// Full-depth book evidence is the only publishable fidelity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BookFidelity {
    FullL2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiquidityRole {
    Maker,
    Taker,
}

/// Point-in-time fee curve resolved from the market catalog/CLOB metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PitFeeSchedule {
    pub schedule_hash: ContentHash,
    pub effective_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub platform_rate: Decimal,
    pub exponent: Decimal,
    pub taker_only: bool,
    pub builder_maker_fee_bps: Bps,
    pub builder_taker_fee_bps: Bps,
    pub builder_attribution: BuilderFeeAttribution,
}

/// Point-in-time Gamma maker-rebate schedule, independent of immediate fees.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PitMakerRebateSchedule {
    pub schedule_hash: ContentHash,
    pub catalog_change_hash: ContentHash,
    pub effective_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub fees_enabled: bool,
    pub platform_rate: Decimal,
    pub exponent: Decimal,
    pub taker_only: bool,
    pub rebate_rate: Decimal,
}

/// Composite PIT identity used by candidate admission and economic scenarios.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PitMarketExecutionEconomics {
    pub fee_schedule: PitFeeSchedule,
    pub maker_rebate_schedule: Option<PitMakerRebateSchedule>,
    pub composite_hash: ContentHash,
}

impl PitFeeSchedule {
    /// Project the canonical market-info fee fact into executable semantics.
    /// The schedule hash includes both venue rates and the frozen route
    /// attribution, so changing order attribution is an artifact-breaking
    /// methodology change rather than an implicit cost-model change.
    pub fn from_market_fee_schedule(schedule: &MarketFeeSchedule) -> Result<Self, FeeError> {
        let schedule_hash =
            CanonicalDigest::content_hash_json(schedule).map_err(|_| FeeError::InvalidSchedule)?;
        Ok(Self {
            schedule_hash,
            effective_at: schedule.effective_at,
            available_at: schedule.available_at,
            platform_rate: schedule.platform_rate,
            exponent: schedule.exponent,
            taker_only: schedule.taker_only,
            builder_maker_fee_bps: schedule.builder_maker_fee_bps,
            builder_taker_fee_bps: schedule.builder_taker_fee_bps,
            builder_attribution: schedule.builder_attribution,
        })
    }

    pub fn validate_at(&self, fill_at: DateTime<Utc>) -> Result<(), FeeError> {
        if self.effective_at > fill_at || self.available_at > fill_at {
            return Err(FeeError::NotPointInTime);
        }
        if self.platform_rate < Decimal::ZERO
            || self.platform_rate > Decimal::ONE
            || self.exponent < Decimal::ZERO
            || self.exponent > Decimal::from(8)
            || self.builder_maker_fee_bps < Bps::ZERO
            || self.builder_taker_fee_bps < Bps::ZERO
        {
            return Err(FeeError::InvalidSchedule);
        }
        Ok(())
    }

    pub fn fee(
        &self,
        role: LiquidityRole,
        price: Price,
        shares: Shares,
        fill_at: DateTime<Utc>,
    ) -> Result<Usd, FeeError> {
        self.validate_at(fill_at)?;
        if !price.is_positive() || price > Price::ONE || !shares.is_positive() {
            return Err(FeeError::InvalidFill);
        }
        let curve = self.fee_curve(price);
        let platform_rate = match role {
            LiquidityRole::Maker if self.taker_only => Decimal::ZERO,
            LiquidityRole::Maker | LiquidityRole::Taker => self.platform_rate,
        };
        let builder_bps = match (self.builder_attribution, role) {
            (BuilderFeeAttribution::NoBuilderCode, _) => Bps::ZERO,
        };
        let platform = shares.inner() * platform_rate * curve;
        let notional = shares.inner() * price.inner();
        let builder = notional * builder_bps.to_fraction();
        let fee =
            (platform + builder).round_dp_with_strategy(5, RoundingStrategy::MidpointAwayFromZero);
        if fee < Decimal::new(1, 5) {
            Ok(Usd::ZERO)
        } else {
            Ok(Usd::new(fee))
        }
    }

    /// Fee-equivalent used by the maker-rebate program. It deliberately
    /// ignores `taker_only` and builder fees and applies no minimum-fee floor.
    pub fn fee_equivalent(
        &self,
        price: Price,
        shares: Shares,
        fill_at: DateTime<Utc>,
    ) -> Result<Usd, FeeError> {
        self.validate_at(fill_at)?;
        if !price.is_positive() || price > Price::ONE || !shares.is_positive() {
            return Err(FeeError::InvalidFill);
        }
        Ok(Usd::new(quantize_venue_amount(
            shares.inner() * self.platform_rate * self.fee_curve(price),
        )))
    }

    fn fee_curve(&self, price: Price) -> Decimal {
        (price.inner() * (Decimal::ONE - price.inner())).powd(self.exponent)
    }
}

impl PitMakerRebateSchedule {
    pub const fn from_market_schedule(
        schedule: &MarketMakerRebateSchedule,
    ) -> Result<Self, FeeError> {
        Ok(Self {
            schedule_hash: schedule.schedule_hash,
            catalog_change_hash: schedule.catalog_change_hash,
            effective_at: schedule.effective_at,
            available_at: schedule.available_at,
            fees_enabled: schedule.fees_enabled,
            platform_rate: schedule.platform_rate,
            exponent: schedule.exponent,
            taker_only: schedule.taker_only,
            rebate_rate: schedule.rebate_rate,
        })
    }

    pub fn validate_at(&self, decision_at: DateTime<Utc>) -> Result<(), FeeError> {
        if self.effective_at > decision_at || self.available_at > decision_at {
            return Err(FeeError::NotPointInTime);
        }
        if self.platform_rate < Decimal::ZERO
            || self.platform_rate > Decimal::ONE
            || self.exponent <= Decimal::ZERO
            || self.exponent > Decimal::from(8)
            || self.rebate_rate < Decimal::ZERO
            || self.rebate_rate > Decimal::ONE
        {
            return Err(FeeError::InvalidSchedule);
        }
        Ok(())
    }

    /// Freeze the exact Gamma terms into an executable recommendation.
    #[must_use]
    pub const fn frozen(&self) -> FrozenMakerRebateSchedule {
        FrozenMakerRebateSchedule {
            schedule_hash: self.schedule_hash,
            catalog_change_hash: self.catalog_change_hash,
            effective_at: self.effective_at,
            available_at: self.available_at,
            fees_enabled: self.fees_enabled,
            platform_rate: self.platform_rate,
            exponent: self.exponent,
            taker_only: self.taker_only,
            rebate_rate: self.rebate_rate,
        }
    }

    /// Estimate the delayed maker incentive for a confirmed or simulated
    /// maker fill. No fill, taker liquidity, disabled fees, or a zero program
    /// rate produces no accrual.
    pub fn expected_incentive(
        &self,
        fee_schedule: &PitFeeSchedule,
        role: LiquidityRole,
        price: Price,
        shares: Shares,
        fill_at: DateTime<Utc>,
    ) -> Result<Option<DeferredVenueIncentive>, FeeError> {
        self.validate_at(fill_at)?;
        if role != LiquidityRole::Maker
            || !shares.is_positive()
            || !self.fees_enabled
            || self.rebate_rate.is_zero()
        {
            return Ok(None);
        }
        let expected_rebate_usd = Usd::new(quantize_venue_amount(
            fee_schedule.fee_equivalent(price, shares, fill_at)?.inner() * self.rebate_rate,
        ));
        let program_date = fill_at.date_naive();
        let expected_credit_at = DateTime::<Utc>::from_naive_utc_and_offset(
            program_date
                .checked_add_days(Days::new(1))
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .ok_or(FeeError::InvalidFill)?,
            Utc,
        );
        Ok(Some(DeferredVenueIncentive {
            expected_rebate_usd,
            program_date,
            expected_credit_at,
            source_schedule_hash: self.schedule_hash,
            eligibility: MakerRebateEligibility::EligibleMakerFill,
        }))
    }
}

impl PitMarketExecutionEconomics {
    /// Resolve the independent CLOB/Gamma sources and reject any visible
    /// fee-curve disagreement at the decision boundary.
    pub fn resolve(
        fee_schedule: &MarketFeeSchedule,
        maker_rebate_schedule: Option<&MarketMakerRebateSchedule>,
        decision_at: DateTime<Utc>,
    ) -> Result<Self, FeeError> {
        if maker_rebate_schedule
            .is_some_and(|schedule| schedule.market_id != fee_schedule.market_id)
        {
            return Err(FeeError::SourceMismatch);
        }
        let fee_schedule = PitFeeSchedule::from_market_fee_schedule(fee_schedule)?;
        fee_schedule.validate_at(decision_at)?;
        let maker_rebate_schedule = match maker_rebate_schedule
            .map(PitMakerRebateSchedule::from_market_schedule)
            .transpose()?
        {
            Some(schedule) => match schedule.validate_at(decision_at) {
                Ok(()) => Some(schedule),
                Err(FeeError::NotPointInTime) => None,
                Err(error) => return Err(error),
            },
            None => None,
        };
        if let Some(rebate) = &maker_rebate_schedule {
            let clob_fees_enabled = !fee_schedule.platform_rate.is_zero();
            if rebate.fees_enabled != clob_fees_enabled
                || rebate.platform_rate != fee_schedule.platform_rate
                || rebate.exponent != fee_schedule.exponent
                || rebate.taker_only != fee_schedule.taker_only
            {
                return Err(FeeError::SourceMismatch);
            }
        }
        let composite_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/pit-market-execution-economics",
            2,
            &(
                fee_schedule.schedule_hash,
                maker_rebate_schedule
                    .as_ref()
                    .map(|schedule| schedule.schedule_hash),
            ),
        )
        .map_err(|_| FeeError::InvalidSchedule)?;
        Ok(Self {
            fee_schedule,
            maker_rebate_schedule,
            composite_hash,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeeError {
    NotPointInTime,
    InvalidSchedule,
    InvalidFill,
    CashBudgetInvariant,
    SourceMismatch,
}

/// Resolution economics of one already-walked binary-token BUY.
///
/// `cash_outlay` is the exact principal-plus-fee account debit. Dividing it by
/// `filled_shares` therefore yields the all-in executable price consumed by
/// realized-return accounting and economic-tier construction, not a midpoint,
/// top-of-book quote, or fee-blind VWAP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionBuyEconomics {
    pub cash_outlay: Usd,
    pub filled_shares: Shares,
    pub entry_fee: Usd,
    pub all_in_price: Price,
}

impl ResolutionBuyEconomics {
    /// Derive binary-resolution economics from a successful cash-budget walk.
    pub fn from_fill(fill: &BookWalkFill) -> Result<Self, FeeError> {
        let raw_cash_outlay = -fill.account_cash_delta_usd;
        if fill.outcome == BookWalkOutcome::Unfilled
            || !fill.filled_shares.is_positive()
            || raw_cash_outlay <= Decimal::ZERO
            || fill.immediate_cost.cash_outlay_usd.inner() != raw_cash_outlay
        {
            return Err(FeeError::InvalidFill);
        }
        let cash_outlay = quantize_venue_amount(raw_cash_outlay);
        let entry_fee = quantize_venue_amount(fill.immediate_cost.total_fee_usd().inner());
        let all_in = cash_outlay / fill.filled_shares.inner();
        if all_in <= Decimal::ZERO || all_in > Decimal::ONE {
            return Err(FeeError::InvalidFill);
        }
        Ok(Self {
            cash_outlay: Usd::new(cash_outlay),
            filled_shares: fill.filled_shares,
            entry_fee: Usd::new(entry_fee),
            all_in_price: Price::new(all_in),
        })
    }

    /// Settle the bought token at its exact resolved payout ratio.
    #[must_use]
    pub fn settle(self, payout_ratio: PayoutRatio) -> ResolutionBuySettlement {
        let cash_outlay = self.cash_outlay.inner();
        let payout = quantize_venue_amount(self.filled_shares.inner() * payout_ratio.inner());
        let realized_pnl = payout - cash_outlay;
        ResolutionBuySettlement {
            economics: self,
            payout_usd: Usd::new(payout),
            realized_pnl_usd: Usd::new(realized_pnl),
            realized_return_bps: Bps::new(realized_pnl / cash_outlay * Decimal::from(10_000)),
        }
    }
}

/// Realized cash flows of an executable BUY held to resolution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolutionBuySettlement {
    pub economics: ResolutionBuyEconomics,
    pub payout_usd: Usd,
    pub realized_pnl_usd: Usd,
    pub realized_return_bps: Bps,
}

/// Worst-price limit for an aggressive BUY relative to the visible best ask.
#[must_use]
pub fn aggressive_buy_limit(best_ask: Price, max_slippage_bps: Bps) -> Price {
    Price::new(
        (best_ask.inner() * (Decimal::ONE + max_slippage_bps.to_fraction())).min(Decimal::ONE),
    )
}

/// Post-only BUY price resolved from the same placement semantics used by OOS replay.
pub fn passive_buy_limit(
    best_bid: Price,
    best_ask: Price,
    placement: PassivePlacement,
    tick_size: TickSize,
) -> Result<Price, FeeError> {
    if !best_bid.is_positive() || best_ask <= best_bid || best_ask > Price::ONE {
        return Err(FeeError::InvalidFill);
    }
    let limit = match placement {
        PassivePlacement::JoinBestBid => best_bid,
        PassivePlacement::ImproveBestBidByTicks { ticks } => Price::new(
            (best_bid.inner() + tick_size.as_decimal() * Decimal::from(ticks))
                .min(best_ask.inner() - tick_size.as_decimal()),
        ),
    };
    if !limit.is_positive() || limit >= best_ask {
        return Err(FeeError::InvalidFill);
    }
    Ok(limit)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BookWalkOutcome {
    Filled,
    Partial,
    Unfilled,
}

/// Deterministic aggregate of one FOK/FAK ladder walk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookWalkFill {
    pub outcome: BookWalkOutcome,
    pub vwap: Option<Price>,
    pub worst_price: Option<Price>,
    pub filled_shares: Shares,
    /// Immediate principal and independently attributed fee components.
    pub immediate_cost: ImmediateExecutionCost,
    /// Signed account cash movement: negative for BUY, positive for SELL.
    pub account_cash_delta_usd: Decimal,
    pub unfilled_cash_budget: Usd,
    pub unfilled_shares: Shares,
}

impl BookWalkFill {
    const fn unfilled(unfilled_cash_budget: Usd, unfilled_shares: Shares) -> Self {
        Self {
            outcome: BookWalkOutcome::Unfilled,
            vwap: None,
            worst_price: None,
            filled_shares: Shares::ZERO,
            immediate_cost: ImmediateExecutionCost {
                principal_usd: Usd::ZERO,
                venue_fee_usd: Usd::ZERO,
                builder_fee_usd: Usd::ZERO,
                cash_outlay_usd: Usd::ZERO,
            },
            account_cash_delta_usd: Decimal::ZERO,
            unfilled_cash_budget,
            unfilled_shares,
        }
    }
}

/// Prepare a BUY from a maximum total cash budget, inclusive of all fees.
///
/// Venue market orders encode principal only. This converts the governed cash
/// budget into principal while preserving `gross + fee <= cash_budget` at the
/// venue's five-decimal fee precision.
pub fn walk_buy_cash_budget(
    asks: &[BookLevel],
    cash_budget: Usd,
    limit_price: Price,
    requirement: FillRequirement,
    fees: &PitFeeSchedule,
    role: LiquidityRole,
    fill_at: DateTime<Utc>,
) -> Result<BookWalkFill, FeeError> {
    if !cash_budget.is_positive() || !limit_price.is_positive() {
        return Ok(BookWalkFill::unfilled(cash_budget, Shares::ZERO));
    }
    fees.validate_at(fill_at)?;
    let mut remaining = cash_budget.inner();
    let mut shares = Decimal::ZERO;
    let mut gross = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    let mut worst = None;
    for level in asks
        .iter()
        .take_while(|level| level.price_decimal() <= limit_price)
    {
        if remaining <= Decimal::ZERO {
            break;
        }
        let price = level.price_decimal();
        let level_shares = shares_affordable_with_cash(
            remaining,
            price,
            level.size_decimal(),
            fees,
            role,
            fill_at,
        )?;
        if !level_shares.is_positive() {
            continue;
        }
        let consume = level_shares.inner() * price.inner();
        let level_fee = fees.fee(role, price, level_shares, fill_at)?.inner();
        let cash = consume + level_fee;
        if cash > remaining {
            return Err(FeeError::CashBudgetInvariant);
        }
        fee += level_fee;
        shares += level_shares.inner();
        gross += consume;
        remaining -= cash;
        worst = Some(price);
    }
    let complete = remaining < Decimal::new(1, 5);
    if !complete && requirement == FillRequirement::AllOrNothing {
        return Ok(BookWalkFill::unfilled(cash_budget, Shares::ZERO));
    }
    Ok((WalkResultParts {
        side: Side::Buy,
        shares,
        gross,
        fee,
        worst,
        complete,
        requirement,
        unfilled_cash_budget: Usd::new(remaining.max(Decimal::ZERO)),
        unfilled_shares: Shares::ZERO,
    })
    .finish_walk())
}

fn shares_affordable_with_cash(
    cash: Decimal,
    price: Price,
    available: Shares,
    fees: &PitFeeSchedule,
    role: LiquidityRole,
    fill_at: DateTime<Utc>,
) -> Result<Shares, FeeError> {
    let full_fee = fees.fee(role, price, available, fill_at)?.inner();
    let full_cash = available.inner() * price.inner() + full_fee;
    if full_cash <= cash {
        return Ok(available);
    }

    let mut low = Decimal::ZERO;
    let mut high = available.inner();
    for _ in 0..96 {
        let midpoint = (low + high) / Decimal::TWO;
        let candidate = Shares::new(midpoint);
        let candidate_cash =
            midpoint * price.inner() + fees.fee(role, price, candidate, fill_at)?.inner();
        if candidate_cash <= cash {
            low = midpoint;
        } else {
            high = midpoint;
        }
    }

    let share_quantum = Decimal::new(1, 6);
    let affordable = (low / share_quantum).floor() * share_quantum;
    Ok(Shares::new(affordable.max(Decimal::ZERO)))
}

/// Buy an exact share quantity from the best-first ask ladder.
pub fn walk_buy_exact_shares(
    asks: &[BookLevel],
    target: Shares,
    limit_price: Price,
    requirement: FillRequirement,
    fees: &PitFeeSchedule,
    role: LiquidityRole,
    fill_at: DateTime<Utc>,
) -> Result<BookWalkFill, FeeError> {
    if !target.is_positive() || !limit_price.is_positive() {
        return Ok(BookWalkFill::unfilled(Usd::ZERO, target));
    }
    let mut remaining = target.inner();
    let mut shares = Decimal::ZERO;
    let mut gross = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    let mut worst = None;
    for level in asks
        .iter()
        .take_while(|level| level.price_decimal() <= limit_price)
    {
        if remaining <= Decimal::ZERO {
            break;
        }
        let price = level.price_decimal();
        let consume = remaining.min(level.size_decimal().inner());
        let level_shares = Shares::new(consume);
        fee += fees.fee(role, price, level_shares, fill_at)?.inner();
        shares += consume;
        gross += consume * price.inner();
        remaining -= consume;
        worst = Some(price);
    }
    if remaining > Decimal::ZERO && requirement == FillRequirement::AllOrNothing {
        return Ok(BookWalkFill::unfilled(Usd::ZERO, target));
    }
    Ok((WalkResultParts {
        side: Side::Buy,
        shares,
        gross,
        fee,
        worst,
        complete: remaining <= Decimal::ZERO,
        requirement,
        unfilled_cash_budget: Usd::ZERO,
        unfilled_shares: Shares::new(remaining.max(Decimal::ZERO)),
    })
    .finish_walk())
}

/// Sell an exact share quantity into the best-first bid ladder.
pub fn walk_sell_exact_shares(
    bids: &[BookLevel],
    target: Shares,
    limit_price: Price,
    requirement: FillRequirement,
    fees: &PitFeeSchedule,
    role: LiquidityRole,
    fill_at: DateTime<Utc>,
) -> Result<BookWalkFill, FeeError> {
    if !target.is_positive() || !limit_price.is_positive() {
        return Ok(BookWalkFill::unfilled(Usd::ZERO, target));
    }
    let mut remaining = target.inner();
    let mut shares = Decimal::ZERO;
    let mut gross = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    let mut worst = None;
    for level in bids
        .iter()
        .take_while(|level| level.price_decimal() >= limit_price)
    {
        if remaining <= Decimal::ZERO {
            break;
        }
        let price = level.price_decimal();
        let consume = remaining.min(level.size_decimal().inner());
        let level_shares = Shares::new(consume);
        fee += fees.fee(role, price, level_shares, fill_at)?.inner();
        shares += consume;
        gross += consume * price.inner();
        remaining -= consume;
        worst = Some(price);
    }
    if remaining > Decimal::ZERO && requirement == FillRequirement::AllOrNothing {
        return Ok(BookWalkFill::unfilled(Usd::ZERO, target));
    }
    Ok((WalkResultParts {
        side: Side::Sell,
        shares,
        gross,
        fee,
        worst,
        complete: remaining <= Decimal::ZERO,
        requirement,
        unfilled_cash_budget: Usd::ZERO,
        unfilled_shares: Shares::new(remaining.max(Decimal::ZERO)),
    })
    .finish_walk())
}

#[derive(Clone, Copy)]
struct WalkResultParts {
    side: Side,
    shares: Decimal,
    gross: Decimal,
    fee: Decimal,
    worst: Option<Price>,
    complete: bool,
    requirement: FillRequirement,
    unfilled_cash_budget: Usd,
    unfilled_shares: Shares,
}

impl WalkResultParts {
    fn finish_walk(self) -> BookWalkFill {
        let Self {
            side,
            shares,
            gross,
            fee,
            worst,
            complete,
            requirement,
            unfilled_cash_budget,
            unfilled_shares,
        } = self;
        debug_assert!(complete || requirement == FillRequirement::AllowPartial);
        if shares <= Decimal::ZERO {
            return BookWalkFill::unfilled(unfilled_cash_budget, unfilled_shares);
        }
        let principal = quantize_venue_amount(gross);
        let venue_fee = quantize_venue_amount(fee);
        let cash_outlay = quantize_venue_amount(principal + venue_fee);
        let cash_proceeds = quantize_venue_amount(principal - venue_fee);
        let immediate_cost = ImmediateExecutionCost {
            principal_usd: Usd::new(principal),
            venue_fee_usd: Usd::new(venue_fee),
            builder_fee_usd: Usd::ZERO,
            cash_outlay_usd: Usd::new(cash_outlay),
        };
        BookWalkFill {
            outcome: if complete {
                BookWalkOutcome::Filled
            } else {
                BookWalkOutcome::Partial
            },
            vwap: Some(Price::new(gross / shares)),
            worst_price: worst,
            filled_shares: Shares::new(shares),
            immediate_cost,
            account_cash_delta_usd: match side {
                Side::Buy => -cash_outlay,
                Side::Sell => cash_proceeds,
            },
            unfilled_cash_budget,
            unfilled_shares,
        }
    }
}

/// Conservative queue-ahead lower-bound model for one passive buy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassiveQueueState {
    pub stream_session_id: Uuid,
    pub price: Price,
    pub queue_ahead: Shares,
    pub remaining_shares: Shares,
    pub filled_shares: Shares,
}

impl PassiveQueueState {
    pub const fn new(
        stream_session_id: Uuid,
        price: Price,
        visible_same_side_size: Shares,
        requested_shares: Shares,
    ) -> Self {
        Self {
            stream_session_id,
            price,
            queue_ahead: visible_same_side_size,
            remaining_shares: requested_shares,
            filled_shares: Shares::ZERO,
        }
    }

    /// Opposing executions at or through the resting BUY price consume queue.
    pub fn apply_trade(&mut self, trade: PassiveTrade) -> Shares {
        if trade.stream_session_id != self.stream_session_id
            || trade.side != Side::Sell
            || trade.price > self.price
            || !trade.shares.is_positive()
        {
            return Shares::ZERO;
        }
        let after_queue = (trade.shares.inner() - self.queue_ahead.inner()).max(Decimal::ZERO);
        self.queue_ahead =
            Shares::new((self.queue_ahead.inner() - trade.shares.inner()).max(Decimal::ZERO));
        let fill = after_queue.min(self.remaining_shares.inner());
        self.remaining_shares = Shares::new(self.remaining_shares.inner() - fill);
        self.filled_shares = Shares::new(self.filled_shares.inner() + fill);
        Shares::new(fill)
    }

    /// L2 cancellations never count as fills or queue consumption.
    pub const fn apply_cancellation(&mut self, _cancelled: Shares) {}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassiveTrade {
    pub stream_session_id: Uuid,
    pub side: Side,
    pub price: Price,
    pub shares: Shares,
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::market::{
            book::BookLevel,
            fee::{BuilderFeeAttribution, MarketFeeSchedule, MarketMakerRebateSchedule},
        },
        enums::{common::Side, quant::FillRequirement},
        types::{Bps, ClobMarketInfoVersionId, ContentHash, MarketId, Price, Shares, Usd},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use super::{
        BookWalkOutcome, FeeError, LiquidityRole, PassiveQueueState, PassiveTrade, PitFeeSchedule,
        PitMakerRebateSchedule, PitMarketExecutionEconomics, walk_buy_cash_budget,
        walk_sell_exact_shares,
    };

    fn level(price: Decimal, size: Decimal) -> BookLevel {
        BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(size))
    }

    impl PitFeeSchedule {
        fn semantics_fixture() -> Self {
            let at = Utc.timestamp_opt(1_700_000_000, 0).single().expect("time");
            Self {
                schedule_hash: ContentHash::parse(&format!("blake3:{}", "1".repeat(64)))
                    .expect("hash"),
                effective_at: at,
                available_at: at,
                platform_rate: dec!(0.07),
                exponent: Decimal::ONE,
                taker_only: true,
                builder_maker_fee_bps: Bps::ZERO,
                builder_taker_fee_bps: Bps::ZERO,
                builder_attribution: BuilderFeeAttribution::NoBuilderCode,
            }
        }
    }

    #[test]
    fn fok_atomic_fak_partial() {
        let asks = [level(dec!(0.5), dec!(10))];
        let at = PitFeeSchedule::semantics_fixture().effective_at;
        let fok = walk_buy_cash_budget(
            &asks,
            Usd::new(dec!(10)),
            Price::new(dec!(0.6)),
            FillRequirement::AllOrNothing,
            &PitFeeSchedule::semantics_fixture(),
            LiquidityRole::Taker,
            at,
        )
        .expect("walk");
        assert_eq!(fok.outcome, BookWalkOutcome::Unfilled);
        assert_eq!(fok.filled_shares, Shares::ZERO);
        assert_eq!(fok.unfilled_cash_budget, Usd::new(dec!(10)));
        let fak = walk_buy_cash_budget(
            &asks,
            Usd::new(dec!(10)),
            Price::new(dec!(0.6)),
            FillRequirement::AllowPartial,
            &PitFeeSchedule::semantics_fixture(),
            LiquidityRole::Taker,
            at,
        )
        .expect("walk");
        assert_eq!(fak.outcome, BookWalkOutcome::Partial);
        assert_eq!(fak.immediate_cost.principal_usd, Usd::new(dec!(5)));
        assert!(fak.immediate_cost.cash_outlay_usd.inner() <= dec!(10));
    }

    #[test]
    fn sell_walk_fee_aware() {
        let bids = [level(dec!(0.9), dec!(10)), level(dec!(0.8), dec!(10))];
        let at = PitFeeSchedule::semantics_fixture().effective_at;
        let fill = walk_sell_exact_shares(
            &bids,
            Shares::new(dec!(15)),
            Price::new(dec!(0.7)),
            FillRequirement::AllowPartial,
            &PitFeeSchedule::semantics_fixture(),
            LiquidityRole::Taker,
            at,
        )
        .expect("walk");
        assert_eq!(fill.immediate_cost.principal_usd, Usd::new(dec!(13)));
        assert!(fill.immediate_cost.total_fee_usd().is_positive());
        assert_eq!(
            fill.account_cash_delta_usd,
            fill.immediate_cost.principal_usd.inner() - fill.immediate_cost.total_fee_usd().inner()
        );
    }

    #[test]
    fn advertised_not_without_code() {
        let mut fees = PitFeeSchedule::semantics_fixture();
        fees.taker_only = false;
        fees.platform_rate = Decimal::ZERO;
        fees.builder_maker_fee_bps = Bps::new(dec!(25));
        let maker_fee = fees
            .fee(
                LiquidityRole::Maker,
                Price::new(dec!(0.5)),
                Shares::new(dec!(100)),
                fees.effective_at,
            )
            .expect("maker fee");
        assert_eq!(maker_fee, Usd::ZERO);
    }

    #[test]
    fn platform_fee_contract_vectors() {
        let mut fees = PitFeeSchedule::semantics_fixture();
        fees.platform_rate = dec!(0.25);
        fees.exponent = dec!(2);
        let at = fees.effective_at;
        for (price, expected) in [
            (dec!(0.1), dec!(0.2025)),
            (dec!(0.5), dec!(1.5625)),
            (dec!(0.9), dec!(0.2025)),
        ] {
            assert_eq!(
                fees.fee(
                    LiquidityRole::Taker,
                    Price::new(price),
                    Shares::new(dec!(100)),
                    at,
                )
                .expect("V2 fee golden vector"),
                Usd::new(expected),
            );
        }

        fees.platform_rate = dec!(0.0175);
        fees.exponent = Decimal::ONE;
        assert_eq!(
            fees.fee(
                LiquidityRole::Taker,
                Price::new(dec!(0.5)),
                Shares::new(dec!(100)),
                at,
            )
            .expect("linear fee golden vector"),
            Usd::new(dec!(0.4375)),
        );

        // Current venue category rates, expressed for a $100 principal order
        // as shares = amount / price.
        fees.exponent = Decimal::ONE;
        for (price, rate, expected) in [
            (dec!(0.5), dec!(0.07), dec!(3.5)),
            (dec!(0.3), dec!(0.07), dec!(4.9)),
            (dec!(0.7), dec!(0.07), dec!(2.1)),
            (dec!(0.5), dec!(0.05), dec!(2.5)),
            (dec!(0.5), dec!(0.04), dec!(2.0)),
            (dec!(0.5), dec!(0.03), dec!(1.5)),
            (dec!(0.5), Decimal::ZERO, Decimal::ZERO),
        ] {
            fees.platform_rate = rate;
            assert_eq!(
                fees.fee(
                    LiquidityRole::Taker,
                    Price::new(price),
                    Shares::new(dec!(100) / price),
                    at,
                )
                .expect("SDK production fee vector"),
                Usd::new(expected),
            );
        }

        fees.platform_rate = dec!(0.000001);
        assert_eq!(
            fees.fee(
                LiquidityRole::Taker,
                Price::new(dec!(0.5)),
                Shares::new(Decimal::ONE),
                at,
            )
            .expect("sub-minimum fee vector"),
            Usd::ZERO,
        );
    }

    #[test]
    fn maker_platform_fee_fact() {
        let mut fees = PitFeeSchedule::semantics_fixture();
        let at = fees.effective_at;
        assert_eq!(
            fees.fee(
                LiquidityRole::Maker,
                Price::new(dec!(0.5)),
                Shares::new(dec!(100)),
                at,
            )
            .expect("taker-only maker fee"),
            Usd::ZERO,
        );
        fees.taker_only = false;
        assert_eq!(
            fees.fee(
                LiquidityRole::Maker,
                Price::new(dec!(0.5)),
                Shares::new(dec!(100)),
                at,
            )
            .expect("two-sided maker fee"),
            Usd::new(dec!(1.75)),
        );
    }

    #[test]
    fn fee_pool_share_cancels() {
        let fees = PitFeeSchedule::semantics_fixture();
        let own = fees
            .fee_equivalent(
                Price::new(dec!(0.5)),
                Shares::new(dec!(100)),
                fees.effective_at,
            )
            .expect("own fee equivalent")
            .inner();
        let others = dec!(5.25);
        let pool = own + others;
        let rebate_rate = dec!(0.20);
        let share_weighted_award = own / pool * (pool * rebate_rate);
        assert_eq!(share_weighted_award, own * rebate_rate);
    }

    #[test]
    fn rebate_requires_maker_fill() {
        let fees = PitFeeSchedule::semantics_fixture();
        let rebate = PitMakerRebateSchedule {
            schedule_hash: ContentHash::parse(&format!("blake3:{}", "2".repeat(64))).expect("hash"),
            catalog_change_hash: ContentHash::parse(&format!("blake3:{}", "3".repeat(64)))
                .expect("hash"),
            effective_at: fees.effective_at,
            available_at: fees.available_at,
            fees_enabled: true,
            platform_rate: fees.platform_rate,
            exponent: fees.exponent,
            taker_only: fees.taker_only,
            rebate_rate: dec!(0.20),
        };
        let incentive = rebate
            .expected_incentive(
                &fees,
                LiquidityRole::Maker,
                Price::new(dec!(0.5)),
                Shares::new(dec!(100)),
                fees.effective_at,
            )
            .expect("rebate estimate")
            .expect("eligible maker fill");
        assert_eq!(incentive.expected_rebate_usd, Usd::new(dec!(0.35)));
        assert!(
            rebate
                .expected_incentive(
                    &fees,
                    LiquidityRole::Taker,
                    Price::new(dec!(0.5)),
                    Shares::new(dec!(100)),
                    fees.effective_at,
                )
                .expect("taker check")
                .is_none()
        );
        assert!(
            rebate
                .expected_incentive(
                    &fees,
                    LiquidityRole::Maker,
                    Price::new(dec!(0.5)),
                    Shares::ZERO,
                    fees.effective_at,
                )
                .expect("no-fill check")
                .is_none()
        );
    }

    #[test]
    fn pit_mismatch_fails_closed() {
        let fees = PitFeeSchedule::semantics_fixture();
        let market_id = MarketId::new("0xmarket");
        let clob = MarketFeeSchedule {
            market_id: market_id.clone(),
            market_info_version_id: ClobMarketInfoVersionId::from_v7(),
            market_info_payload_hash: fees.schedule_hash,
            platform_rate: fees.platform_rate,
            exponent: fees.exponent,
            taker_only: fees.taker_only,
            builder_maker_fee_bps: Bps::ZERO,
            builder_taker_fee_bps: Bps::ZERO,
            builder_attribution: BuilderFeeAttribution::NoBuilderCode,
            effective_at: fees.effective_at,
            available_at: fees.available_at,
        };
        let gamma = MarketMakerRebateSchedule {
            market_id,
            fees_enabled: true,
            platform_rate: dec!(0.05),
            exponent: fees.exponent,
            taker_only: fees.taker_only,
            rebate_rate: dec!(0.20),
            effective_at: fees.effective_at,
            available_at: fees.available_at,
            catalog_change_hash: ContentHash::parse(&format!("blake3:{}", "4".repeat(64)))
                .expect("hash"),
            schedule_hash: ContentHash::parse(&format!("blake3:{}", "5".repeat(64))).expect("hash"),
        };
        assert_eq!(
            PitMarketExecutionEconomics::resolve(&clob, Some(&gamma), fees.effective_at),
            Err(FeeError::SourceMismatch)
        );

        let mut wrong_market = gamma;
        wrong_market.platform_rate = fees.platform_rate;
        wrong_market.market_id = MarketId::new("0xother-market");
        assert_eq!(
            PitMarketExecutionEconomics::resolve(&clob, Some(&wrong_market), fees.effective_at,),
            Err(FeeError::SourceMismatch)
        );
    }

    #[test]
    fn future_rebate_is_zero() {
        let fees = PitFeeSchedule::semantics_fixture();
        let market_id = MarketId::new("0xmarket");
        let clob = MarketFeeSchedule {
            market_id: market_id.clone(),
            market_info_version_id: ClobMarketInfoVersionId::from_v7(),
            market_info_payload_hash: fees.schedule_hash,
            platform_rate: fees.platform_rate,
            exponent: fees.exponent,
            taker_only: fees.taker_only,
            builder_maker_fee_bps: Bps::ZERO,
            builder_taker_fee_bps: Bps::ZERO,
            builder_attribution: BuilderFeeAttribution::NoBuilderCode,
            effective_at: fees.effective_at,
            available_at: fees.available_at,
        };
        let gamma = MarketMakerRebateSchedule {
            market_id,
            fees_enabled: true,
            platform_rate: fees.platform_rate,
            exponent: fees.exponent,
            taker_only: fees.taker_only,
            rebate_rate: dec!(0.20),
            effective_at: fees.effective_at,
            available_at: fees.available_at + Duration::seconds(1),
            catalog_change_hash: ContentHash::parse(&format!("blake3:{}", "4".repeat(64)))
                .expect("hash"),
            schedule_hash: ContentHash::parse(&format!("blake3:{}", "5".repeat(64))).expect("hash"),
        };

        let resolved = PitMarketExecutionEconomics::resolve(&clob, Some(&gamma), fees.effective_at)
            .expect("future rebate source is unavailable, not a fee failure");
        assert!(resolved.maker_rebate_schedule.is_none());
    }

    #[test]
    fn visible_invalid_rebate_rejected() {
        let fees = PitFeeSchedule::semantics_fixture();
        let market_id = MarketId::new("0xmarket");
        let clob = MarketFeeSchedule {
            market_id: market_id.clone(),
            market_info_version_id: ClobMarketInfoVersionId::from_v7(),
            market_info_payload_hash: fees.schedule_hash,
            platform_rate: fees.platform_rate,
            exponent: fees.exponent,
            taker_only: fees.taker_only,
            builder_maker_fee_bps: Bps::ZERO,
            builder_taker_fee_bps: Bps::ZERO,
            builder_attribution: BuilderFeeAttribution::NoBuilderCode,
            effective_at: fees.effective_at,
            available_at: fees.available_at,
        };
        let gamma = MarketMakerRebateSchedule {
            market_id,
            fees_enabled: true,
            platform_rate: fees.platform_rate,
            exponent: fees.exponent,
            taker_only: fees.taker_only,
            rebate_rate: dec!(1.01),
            effective_at: fees.effective_at,
            available_at: fees.available_at,
            catalog_change_hash: ContentHash::parse(&format!("blake3:{}", "4".repeat(64)))
                .expect("hash"),
            schedule_hash: ContentHash::parse(&format!("blake3:{}", "5".repeat(64))).expect("hash"),
        };

        assert_eq!(
            PitMarketExecutionEconomics::resolve(&clob, Some(&gamma), fees.effective_at),
            Err(FeeError::InvalidSchedule)
        );
    }

    #[test]
    fn buy_respects_total_budget() {
        for budget in [dec!(0.01), dec!(1), dec!(25), dec!(100), dec!(500)] {
            for price in [dec!(0.01), dec!(0.1), dec!(0.5), dec!(0.9), dec!(0.99)] {
                for rate in [Decimal::ZERO, dec!(0.02), dec!(0.25)] {
                    for exponent in [Decimal::ZERO, Decimal::ONE, dec!(2)] {
                        let mut fees = PitFeeSchedule::semantics_fixture();
                        fees.platform_rate = rate;
                        fees.exponent = exponent;
                        let asks = [level(price, dec!(1000000))];
                        let fill = walk_buy_cash_budget(
                            &asks,
                            Usd::new(budget),
                            Price::new(price),
                            FillRequirement::AllOrNothing,
                            &fees,
                            LiquidityRole::Taker,
                            fees.effective_at,
                        )
                        .expect("cash-budget walk");
                        assert!(
                            fill.immediate_cost.cash_outlay_usd.inner() <= budget,
                            "budget={budget} price={price} rate={rate} exponent={exponent} fill={fill:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn passive_ignores_cancels_prints() {
        let session = Uuid::now_v7();
        let mut queue = PassiveQueueState::new(
            session,
            Price::new(dec!(0.5)),
            Shares::new(dec!(10)),
            Shares::new(dec!(5)),
        );
        queue.apply_cancellation(Shares::new(dec!(10)));
        assert_eq!(queue.queue_ahead, Shares::new(dec!(10)));
        let filled = queue.apply_trade(PassiveTrade {
            stream_session_id: session,
            side: Side::Sell,
            price: Price::new(dec!(0.5)),
            shares: Shares::new(dec!(20)),
        });
        assert_eq!(filled, Shares::new(dec!(5)));
        let exhausted = queue.apply_trade(PassiveTrade {
            stream_session_id: session,
            side: Side::Sell,
            price: Price::new(dec!(0.5)),
            shares: Shares::new(dec!(12)),
        });
        assert_eq!(exhausted, Shares::ZERO);
    }

    #[test]
    fn price_through_consumes_queue() {
        let session = Uuid::now_v7();
        let mut queue = PassiveQueueState::new(
            session,
            Price::new(dec!(0.5)),
            Shares::new(dec!(10)),
            Shares::new(dec!(5)),
        );
        let filled = queue.apply_trade(PassiveTrade {
            stream_session_id: session,
            side: Side::Sell,
            price: Price::new(dec!(0.49)),
            shares: Shares::new(dec!(12)),
        });

        assert_eq!(filled, Shares::new(dec!(2)));
        assert_eq!(queue.queue_ahead, Shares::ZERO);
        assert_eq!(queue.remaining_shares, Shares::new(dec!(3)));
    }
}
