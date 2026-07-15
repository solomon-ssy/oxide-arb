//! Pure venue execution semantics shared by research and serving.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    domain::market::book::BookLevel,
    enums::{clickhouse::ChTradeReconciliationStatus, common::Side, quant::FillRequirement},
    types::{Bps, ContentHash, Price, Shares, Usd},
};
use rust_decimal::{Decimal, MathematicalOps, RoundingStrategy};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Versioned identity of the shared book-walk, queue, and fee semantics.
pub const EXECUTION_SEMANTICS_VERSION: &str = "polymarket_execution_semantics_v1";

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
    /// Builder fees are charged only when this exact order carries an explicit
    /// builder attribution. A market advertising builder rates is not evidence
    /// that the order was attributed.
    pub builder_attributed: bool,
}

impl PitFeeSchedule {
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
        let curve_base = price.inner() * (Decimal::ONE - price.inner());
        let curve = curve_base.powd(self.exponent);
        let platform_rate = match role {
            LiquidityRole::Maker => Decimal::ZERO,
            LiquidityRole::Taker => self.platform_rate,
        };
        let builder_bps = match (self.builder_attributed, role) {
            (false, _) => Bps::ZERO,
            (true, LiquidityRole::Maker) => self.builder_maker_fee_bps,
            (true, LiquidityRole::Taker) => self.builder_taker_fee_bps,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeeError {
    NotPointInTime,
    InvalidSchedule,
    InvalidFill,
    CashBudgetInvariant,
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
    /// Principal encoded in the venue order, excluding fees.
    pub gross_order_amount: Usd,
    pub expected_fee: Usd,
    /// Signed account cash movement: negative for BUY, positive for SELL.
    pub total_cash_delta: Decimal,
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
            gross_order_amount: Usd::ZERO,
            expected_fee: Usd::ZERO,
            total_cash_delta: Decimal::ZERO,
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
    Ok(finish_walk(WalkResultParts {
        side: Side::Buy,
        shares,
        gross,
        fee,
        worst,
        complete,
        requirement,
        unfilled_cash_budget: Usd::new(remaining.max(Decimal::ZERO)),
        unfilled_shares: Shares::ZERO,
    }))
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
    Ok(finish_walk(WalkResultParts {
        side: Side::Buy,
        shares,
        gross,
        fee,
        worst,
        complete: remaining <= Decimal::ZERO,
        requirement,
        unfilled_cash_budget: Usd::ZERO,
        unfilled_shares: Shares::new(remaining.max(Decimal::ZERO)),
    }))
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
    Ok(finish_walk(WalkResultParts {
        side: Side::Sell,
        shares,
        gross,
        fee,
        worst,
        complete: remaining <= Decimal::ZERO,
        requirement,
        unfilled_cash_budget: Usd::ZERO,
        unfilled_shares: Shares::new(remaining.max(Decimal::ZERO)),
    }))
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

fn finish_walk(parts: WalkResultParts) -> BookWalkFill {
    let WalkResultParts {
        side,
        shares,
        gross,
        fee,
        worst,
        complete,
        requirement,
        unfilled_cash_budget,
        unfilled_shares,
    } = parts;
    debug_assert!(complete || requirement == FillRequirement::AllowPartial);
    if shares <= Decimal::ZERO {
        return BookWalkFill::unfilled(unfilled_cash_budget, unfilled_shares);
    }
    BookWalkFill {
        outcome: if complete {
            BookWalkOutcome::Filled
        } else {
            BookWalkOutcome::Partial
        },
        vwap: Some(Price::new(gross / shares)),
        worst_price: worst,
        filled_shares: Shares::new(shares),
        gross_order_amount: Usd::new(gross),
        expected_fee: Usd::new(fee),
        total_cash_delta: match side {
            Side::Buy => -(gross + fee),
            Side::Sell => gross - fee,
        },
        unfilled_cash_budget,
        unfilled_shares,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PassiveQueueAvailability {
    Available,
    UnknownAfterReset,
}

/// Conservative queue-ahead lower-bound model for one passive buy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassiveQueueState {
    pub stream_session_id: Uuid,
    pub price: Price,
    pub queue_ahead: Shares,
    pub remaining_shares: Shares,
    pub filled_shares: Shares,
    pub availability: PassiveQueueAvailability,
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
            availability: PassiveQueueAvailability::Available,
        }
    }

    /// Only reconciled opposing prints at the exact price consume queue.
    pub fn apply_trade(&mut self, trade: PassiveTrade) -> Shares {
        if self.availability != PassiveQueueAvailability::Available
            || trade.stream_session_id != self.stream_session_id
            || trade.reconciliation_status != ChTradeReconciliationStatus::Matched
            || trade.side != Side::Sell
            || trade.price != self.price
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

    pub fn reset_session(&mut self, new_session_id: Uuid) {
        if new_session_id != self.stream_session_id {
            self.availability = PassiveQueueAvailability::UnknownAfterReset;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassiveTrade {
    pub stream_session_id: Uuid,
    pub side: Side,
    pub price: Price,
    pub shares: Shares,
    pub reconciliation_status: ChTradeReconciliationStatus,
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use quant_pivot_models::{
        domain::market::book::BookLevel,
        enums::{clickhouse::ChTradeReconciliationStatus, common::Side, quant::FillRequirement},
        types::{Bps, ContentHash, Price, Shares, Usd},
    };
    use rust_decimal::Decimal;

    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use super::{
        BookWalkOutcome, LiquidityRole, PassiveQueueState, PassiveTrade, PitFeeSchedule,
        walk_buy_cash_budget, walk_sell_exact_shares,
    };

    fn level(price: Decimal, size: Decimal) -> BookLevel {
        BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(size))
    }

    fn schedule() -> PitFeeSchedule {
        let at = chrono::Utc
            .timestamp_opt(1_700_000_000, 0)
            .single()
            .expect("time");
        PitFeeSchedule {
            schedule_hash: ContentHash::parse(format!("blake3:{}", "1".repeat(64))).expect("hash"),
            effective_at: at,
            available_at: at,
            platform_rate: dec!(0.07),
            exponent: Decimal::ONE,
            taker_only: true,
            builder_maker_fee_bps: Bps::ZERO,
            builder_taker_fee_bps: Bps::ZERO,
            builder_attributed: false,
        }
    }

    #[test]
    fn fok_is_atomic_and_fak_is_partial() {
        let asks = [level(dec!(0.5), dec!(10))];
        let at = schedule().effective_at;
        let fok = walk_buy_cash_budget(
            &asks,
            Usd::new(dec!(10)),
            Price::new(dec!(0.6)),
            FillRequirement::AllOrNothing,
            &schedule(),
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
            &schedule(),
            LiquidityRole::Taker,
            at,
        )
        .expect("walk");
        assert_eq!(fak.outcome, BookWalkOutcome::Partial);
        assert_eq!(fak.gross_order_amount, Usd::new(dec!(5)));
        assert!(fak.gross_order_amount.inner() + fak.expected_fee.inner() <= dec!(10));
    }

    #[test]
    fn sell_walk_and_fee_are_price_aware() {
        let bids = [level(dec!(0.9), dec!(10)), level(dec!(0.8), dec!(10))];
        let at = schedule().effective_at;
        let fill = walk_sell_exact_shares(
            &bids,
            Shares::new(dec!(15)),
            Price::new(dec!(0.7)),
            FillRequirement::AllowPartial,
            &schedule(),
            LiquidityRole::Taker,
            at,
        )
        .expect("walk");
        assert_eq!(fill.gross_order_amount, Usd::new(dec!(13)));
        assert!(fill.expected_fee.is_positive());
        assert_eq!(
            fill.total_cash_delta,
            fill.gross_order_amount.inner() - fill.expected_fee.inner()
        );
    }

    #[test]
    fn maker_platform_fee_is_zero_and_builder_fee_requires_attribution() {
        let mut fees = schedule();
        fees.taker_only = false;
        fees.builder_maker_fee_bps = Bps::new(dec!(25));
        let maker_without_attribution = fees
            .fee(
                LiquidityRole::Maker,
                Price::new(dec!(0.5)),
                Shares::new(dec!(100)),
                fees.effective_at,
            )
            .expect("maker fee");
        assert_eq!(maker_without_attribution, Usd::ZERO);

        fees.builder_attributed = true;
        let attributed_builder_fee = fees
            .fee(
                LiquidityRole::Maker,
                Price::new(dec!(0.5)),
                Shares::new(dec!(100)),
                fees.effective_at,
            )
            .expect("attributed builder fee");
        assert_eq!(attributed_builder_fee, Usd::new(dec!(0.125)));
    }

    #[test]
    fn every_prepared_buy_respects_total_cash_budget() {
        for budget in [dec!(0.01), dec!(1), dec!(25), dec!(100), dec!(500)] {
            for price in [dec!(0.01), dec!(0.1), dec!(0.5), dec!(0.9), dec!(0.99)] {
                for rate in [Decimal::ZERO, dec!(0.02), dec!(0.25)] {
                    for exponent in [Decimal::ZERO, Decimal::ONE, dec!(2)] {
                        let mut fees = schedule();
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
                            fill.gross_order_amount.inner() + fill.expected_fee.inner() <= budget,
                            "budget={budget} price={price} rate={rate} exponent={exponent} fill={fill:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn passive_queue_ignores_cancels_and_unreconciled_prints() {
        let session = Uuid::now_v7();
        let mut queue = PassiveQueueState::new(
            session,
            Price::new(dec!(0.5)),
            Shares::new(dec!(10)),
            Shares::new(dec!(5)),
        );
        queue.apply_cancellation(Shares::new(dec!(10)));
        assert_eq!(queue.queue_ahead, Shares::new(dec!(10)));
        let unavailable = queue.apply_trade(PassiveTrade {
            stream_session_id: session,
            side: Side::Sell,
            price: Price::new(dec!(0.5)),
            shares: Shares::new(dec!(20)),
            reconciliation_status: ChTradeReconciliationStatus::Pending,
        });
        assert_eq!(unavailable, Shares::ZERO);
        let filled = queue.apply_trade(PassiveTrade {
            stream_session_id: session,
            side: Side::Sell,
            price: Price::new(dec!(0.5)),
            shares: Shares::new(dec!(12)),
            reconciliation_status: ChTradeReconciliationStatus::Matched,
        });
        assert_eq!(filled, Shares::new(dec!(2)));
    }
}
