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
}

impl PitFeeSchedule {
    pub fn validate_at(&self, fill_at: DateTime<Utc>) -> Result<(), FeeError> {
        if self.effective_at > fill_at || self.available_at > fill_at {
            return Err(FeeError::NotPointInTime);
        }
        if self.platform_rate < Decimal::ZERO
            || self.platform_rate > Decimal::ONE
            || self.exponent <= Decimal::ZERO
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
            LiquidityRole::Maker if self.taker_only => Decimal::ZERO,
            LiquidityRole::Maker | LiquidityRole::Taker => self.platform_rate,
        };
        let builder_bps = match role {
            LiquidityRole::Maker => self.builder_maker_fee_bps,
            LiquidityRole::Taker => self.builder_taker_fee_bps,
        };
        let platform = shares.inner() * platform_rate * curve;
        let notional = shares.inner() * price.inner();
        let builder = notional * builder_bps.to_fraction();
        let fee =
            (platform + builder).round_dp_with_strategy(5, RoundingStrategy::MidpointAwayFromZero);
        Ok(Usd::new(fee.max(Decimal::ZERO)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeeError {
    NotPointInTime,
    InvalidSchedule,
    InvalidFill,
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
    pub gross_notional: Usd,
    pub fee: Usd,
}

impl BookWalkFill {
    const fn unfilled() -> Self {
        Self {
            outcome: BookWalkOutcome::Unfilled,
            vwap: None,
            worst_price: None,
            filled_shares: Shares::ZERO,
            gross_notional: Usd::ZERO,
            fee: Usd::ZERO,
        }
    }
}

/// Buy an exact gross USD tier from the best-first ask ladder.
pub fn walk_buy_exact_usd(
    asks: &[BookLevel],
    target: Usd,
    limit_price: Price,
    requirement: FillRequirement,
    fees: &PitFeeSchedule,
    role: LiquidityRole,
    fill_at: DateTime<Utc>,
) -> Result<BookWalkFill, FeeError> {
    if !target.is_positive() || !limit_price.is_positive() {
        return Ok(BookWalkFill::unfilled());
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
        let available = level.size_decimal().inner() * price.inner();
        let consume = remaining.min(available);
        let level_shares = Shares::new(consume / price.inner());
        fee += fees.fee(role, price, level_shares, fill_at)?.inner();
        shares += level_shares.inner();
        gross += consume;
        remaining -= consume;
        worst = Some(price);
    }
    Ok(finish_walk(
        shares,
        gross,
        fee,
        worst,
        remaining <= Decimal::ZERO,
        requirement,
    ))
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
        return Ok(BookWalkFill::unfilled());
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
    Ok(finish_walk(
        shares,
        gross,
        fee,
        worst,
        remaining <= Decimal::ZERO,
        requirement,
    ))
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
        return Ok(BookWalkFill::unfilled());
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
    Ok(finish_walk(
        shares,
        gross,
        fee,
        worst,
        remaining <= Decimal::ZERO,
        requirement,
    ))
}

fn finish_walk(
    shares: Decimal,
    gross: Decimal,
    fee: Decimal,
    worst: Option<Price>,
    complete: bool,
    requirement: FillRequirement,
) -> BookWalkFill {
    if !complete && requirement == FillRequirement::AllOrNothing {
        return BookWalkFill::unfilled();
    }
    if shares <= Decimal::ZERO {
        return BookWalkFill::unfilled();
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
        gross_notional: Usd::new(gross),
        fee: Usd::new(fee),
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
        walk_buy_exact_usd, walk_sell_exact_shares,
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
        }
    }

    #[test]
    fn fok_is_atomic_and_fak_is_partial() {
        let asks = [level(dec!(0.5), dec!(10))];
        let at = schedule().effective_at;
        let fok = walk_buy_exact_usd(
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
        let fak = walk_buy_exact_usd(
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
        assert_eq!(fak.gross_notional, Usd::new(dec!(5)));
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
        assert_eq!(fill.gross_notional, Usd::new(dec!(13)));
        assert!(fill.fee.is_positive());
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
