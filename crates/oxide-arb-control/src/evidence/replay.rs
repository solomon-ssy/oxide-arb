use oxide_arb_models::{
    domain::book::BookLevel,
    enums::clickhouse::ChSide,
    types::{Price, Shares, Usd},
};
use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FokReplayRequest {
    pub side: ChSide,
    pub limit_price: Price,
    pub buy_budget: Option<Usd>,
    pub sell_shares: Option<Shares>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FokReplayResult {
    pub strict_fill: bool,
    pub vwap: Price,
    pub filled_shares: Shares,
    pub notional: Usd,
    pub unfilled_amount: Decimal,
    pub depth_consumed_pct_bps: u64,
}

#[must_use]
pub fn replay_fok(levels: &[BookLevel], request: FokReplayRequest) -> Option<FokReplayResult> {
    match request.side {
        ChSide::Buy => replay_buy(levels, request.limit_price, request.buy_budget?),
        ChSide::Sell => replay_sell(levels, request.limit_price, request.sell_shares?),
    }
}

#[must_use]
pub fn replay_buy(
    asks: &[BookLevel],
    limit_price: Price,
    budget_usd: Usd,
) -> Option<FokReplayResult> {
    let total_depth = notional_up_to(asks, limit_price.inner());
    if total_depth <= Decimal::ZERO {
        return None;
    }
    let mut remaining = budget_usd.inner();
    let mut filled_shares = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    for level in asks {
        let price = level.price_decimal().inner();
        if price > limit_price.inner() || remaining <= Decimal::ZERO {
            break;
        }
        let level_notional = price * level.size_decimal().inner();
        let used_notional = remaining.min(level_notional);
        notional += used_notional;
        filled_shares += used_notional / price;
        remaining -= used_notional;
    }
    build_result(
        remaining <= Decimal::ZERO,
        filled_shares,
        notional,
        remaining.max(Decimal::ZERO),
        notional,
        total_depth,
    )
}

#[must_use]
pub fn replay_sell(
    bids: &[BookLevel],
    limit_price: Price,
    requested_shares: Shares,
) -> Option<FokReplayResult> {
    let total_depth = share_depth_down_to(bids, limit_price.inner());
    if total_depth <= Decimal::ZERO {
        return None;
    }
    let mut remaining = requested_shares.inner();
    let mut filled_shares = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    for level in bids {
        let price = level.price_decimal().inner();
        if price < limit_price.inner() || remaining <= Decimal::ZERO {
            break;
        }
        let used_shares = remaining.min(level.size_decimal().inner());
        filled_shares += used_shares;
        notional += used_shares * price;
        remaining -= used_shares;
    }
    build_result(
        remaining <= Decimal::ZERO,
        filled_shares,
        notional,
        remaining.max(Decimal::ZERO),
        filled_shares,
        total_depth,
    )
}

#[must_use]
pub fn stress_levels(
    levels: &[BookLevel],
    side: ChSide,
    adverse_selection_bps: u32,
) -> Vec<BookLevel> {
    let factor = Decimal::from(adverse_selection_bps) / Decimal::from(10_000_u32);
    levels
        .iter()
        .filter_map(|level| {
            let price = level.price_decimal().inner();
            let stressed_price = match side {
                ChSide::Buy => price * (Decimal::ONE + factor),
                ChSide::Sell => price * (Decimal::ONE - factor),
            };
            BookLevel::try_from_decimal(
                Price::new(stressed_price.max(Decimal::ZERO).min(Decimal::ONE)),
                level.size_decimal(),
            )
        })
        .collect()
}

fn build_result(
    strict_fill: bool,
    filled_shares: Decimal,
    notional: Decimal,
    unfilled_amount: Decimal,
    consumed_depth: Decimal,
    total_depth: Decimal,
) -> Option<FokReplayResult> {
    if filled_shares <= Decimal::ZERO {
        return None;
    }
    let vwap = Price::new(notional / filled_shares);
    Some(FokReplayResult {
        strict_fill,
        vwap,
        filled_shares: Shares::new(filled_shares),
        notional: Usd::new(notional),
        unfilled_amount,
        depth_consumed_pct_bps: decimal_bps(consumed_depth.min(total_depth), total_depth),
    })
}

fn notional_up_to(levels: &[BookLevel], limit_price: Decimal) -> Decimal {
    levels.iter().fold(Decimal::ZERO, |acc, level| {
        let price = level.price_decimal().inner();
        if price <= limit_price {
            acc + price * level.size_decimal().inner()
        } else {
            acc
        }
    })
}

fn share_depth_down_to(levels: &[BookLevel], limit_price: Decimal) -> Decimal {
    levels.iter().fold(Decimal::ZERO, |acc, level| {
        let price = level.price_decimal().inner();
        if price >= limit_price {
            acc + level.size_decimal().inner()
        } else {
            acc
        }
    })
}

fn decimal_bps(numerator: Decimal, denominator: Decimal) -> u64 {
    if denominator <= Decimal::ZERO {
        return 0;
    }
    let value = (numerator / denominator * Decimal::from(10_000_u64)).round();
    value.try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use oxide_arb_models::{
        domain::BookLevel,
        enums::clickhouse::ChSide,
        types::{Price, Shares, Usd},
    };
    use rust_decimal_macros::dec;

    use crate::evidence::replay::{FokReplayRequest, replay_fok, stress_levels};

    #[test]
    fn buy_fok_uses_usd_notional() {
        let levels = vec![level(dec!(0.50), dec!(10)), level(dec!(0.55), dec!(10))];
        let result = replay_fok(
            &levels,
            FokReplayRequest {
                side: ChSide::Buy,
                limit_price: Price::new(dec!(0.55)),
                buy_budget: Some(Usd::new(dec!(10))),
                sell_shares: None,
            },
        )
        .expect("buy replay");

        assert!(result.strict_fill);
        assert_eq!(result.notional, Usd::new(dec!(10)));
    }

    #[test]
    fn sell_fok_uses_share_depth() {
        let levels = vec![level(dec!(0.60), dec!(5)), level(dec!(0.55), dec!(5))];
        let result = replay_fok(
            &levels,
            FokReplayRequest {
                side: ChSide::Sell,
                limit_price: Price::new(dec!(0.55)),
                buy_budget: None,
                sell_shares: Some(Shares::new(dec!(10))),
            },
        )
        .expect("sell replay");

        assert!(result.strict_fill);
        assert_eq!(result.filled_shares, Shares::new(dec!(10)));
    }

    #[test]
    fn adverse_stress_worsens_buy_price() {
        let levels = vec![level(dec!(0.50), dec!(10))];
        let stressed = stress_levels(&levels, ChSide::Buy, 100);

        assert!(stressed[0].price_decimal() > levels[0].price_decimal());
    }

    fn level(price: rust_decimal::Decimal, size: rust_decimal::Decimal) -> BookLevel {
        BookLevel::try_from_decimal(Price::new(price), Shares::new(size)).expect("valid level")
    }
}
