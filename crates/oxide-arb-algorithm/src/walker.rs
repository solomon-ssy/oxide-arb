//! Fixed-point orderbook walk simulation for entry cost and slippage estimation.
//!
//! All interior arithmetic uses [`MicroPrice`] / [`MicroShares`] / [`MicroUsd`].

use oxide_arb_models::domain::BookLevel;
use oxide_arb_models::types::{MicroPrice, MicroShares, MicroUsd, Price, Shares, Usd};

/// Result of walking through orderbook levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalkResult {
    pub shares: MicroShares,
    pub vwap: MicroPrice,
    pub total_cost: MicroUsd,
    pub levels_consumed: usize,
    pub fully_filled: bool,
    /// Percentage of total available depth consumed (0–100 integer).
    pub depth_used_pct: i64,
}

/// Stateless orderbook walker.
pub struct OrderbookWalker;

impl OrderbookWalker {
    /// Walk ask levels to simulate buying up to `max_cost_usd` worth of shares.
    ///
    /// Levels **must** be sorted ascending by price. Stops when budget exhausted,
    /// no levels remain, or price drops below `price_floor`.
    #[must_use]
    #[inline]
    pub fn walk_asks_by_cost(
        asks: &[BookLevel],
        max_cost_usd: MicroUsd,
        price_floor: MicroPrice,
        total_ask_depth_usd: MicroUsd,
    ) -> Option<WalkResult> {
        let mut remaining_budget = max_cost_usd;
        let mut total_shares = MicroShares::ZERO;
        let mut total_cost = MicroUsd::ZERO;
        let mut levels_consumed = 0_usize;

        for level in asks {
            let price = level.price;
            let size = level.size;
            if price.micro() <= 0 || size.micro() <= 0 {
                continue;
            }
            if price.micro() < price_floor.micro() {
                break;
            }
            if remaining_budget.micro() <= 0 {
                break;
            }

            let affordable = price.affordable_shares(remaining_budget);
            let fill = MicroShares::from_micro(affordable.micro().min(size.micro()));
            let cost = price.mul_shares(fill);

            total_shares = MicroShares::from_micro(total_shares.micro() + fill.micro());
            total_cost = MicroUsd::from_micro(total_cost.micro() + cost.micro());
            remaining_budget = MicroUsd::from_micro(remaining_budget.micro() - cost.micro());
            levels_consumed += 1;
        }

        if total_shares.is_zero() {
            return None;
        }

        let vwap = MicroPrice::vwap_from_cost(total_cost, total_shares);
        let depth_used_pct = total_cost.percent_of(total_ask_depth_usd);
        let fully_filled = remaining_budget.is_positive();

        Some(WalkResult {
            shares: total_shares,
            vwap,
            total_cost,
            levels_consumed,
            fully_filled,
            depth_used_pct,
        })
    }

    /// Walk ask levels to buy exactly `target_shares` (or as many as available).
    #[must_use]
    #[inline]
    pub fn walk_asks_by_shares(
        asks: &[BookLevel],
        target_shares: MicroShares,
        price_floor: MicroPrice,
        total_ask_depth_usd: MicroUsd,
    ) -> Option<WalkResult> {
        let mut remaining = target_shares;
        let mut total_shares = MicroShares::ZERO;
        let mut total_cost = MicroUsd::ZERO;
        let mut levels_consumed = 0_usize;

        for level in asks {
            if remaining.is_zero() {
                break;
            }
            let price = level.price;
            let size = level.size;
            if price.micro() <= 0 || size.micro() <= 0 {
                continue;
            }
            if price.micro() < price_floor.micro() {
                break;
            }

            let fill = MicroShares::from_micro(remaining.micro().min(size.micro()));
            let cost = price.mul_shares(fill);

            total_shares = MicroShares::from_micro(total_shares.micro() + fill.micro());
            total_cost = MicroUsd::from_micro(total_cost.micro() + cost.micro());
            remaining = MicroShares::from_micro(remaining.micro() - fill.micro());
            levels_consumed += 1;
        }

        if total_shares.is_zero() {
            return None;
        }

        let vwap = MicroPrice::vwap_from_cost(total_cost, total_shares);
        let depth_used_pct = total_cost.percent_of(total_ask_depth_usd);
        let fully_filled = remaining.is_zero();

        Some(WalkResult {
            shares: total_shares,
            vwap,
            total_cost,
            levels_consumed,
            fully_filled,
            depth_used_pct,
        })
    }

    /// Decimal boundary helpers for tests and persistence adapters.
    #[must_use]
    #[inline]
    pub fn walk_result_decimal(walk: WalkResult) -> (Shares, Price, Usd, i64) {
        (
            Shares::new(walk.shares.to_decimal()),
            Price::new(walk.vwap.to_decimal()),
            Usd::new(walk.total_cost.to_decimal()),
            walk.depth_used_pct,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::domain::book::{BookLevel, total_depth_usd};
    use oxide_arb_models::types::{MicroPrice, MicroUsd, Price, Shares};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn level(price: Decimal, size: Decimal) -> BookLevel {
        BookLevel::from_decimal(Price::new(price), Shares::new(size)).unwrap()
    }

    fn micro_usd(d: Decimal) -> MicroUsd {
        MicroUsd::try_from_decimal(d).unwrap()
    }

    fn micro_price(d: Decimal) -> MicroPrice {
        MicroPrice::try_from_decimal(d).unwrap()
    }

    #[test]
    fn walk_single_level_full_fill() {
        let asks = [level(dec!(0.97), dec!(100))];
        let depth = total_depth_usd(&asks);
        let walk = OrderbookWalker::walk_asks_by_cost(
            &asks,
            micro_usd(dec!(97)),
            micro_price(dec!(0.95)),
            depth,
        )
        .unwrap();
        assert_eq!(walk.shares.to_decimal(), dec!(100));
        assert_eq!(walk.total_cost.to_decimal(), dec!(97));
        assert_eq!(walk.vwap.to_decimal(), dec!(0.97));
        assert!(!walk.fully_filled);
    }

    #[test]
    fn walk_stops_at_price_floor() {
        let asks = [level(dec!(0.98), dec!(50)), level(dec!(0.94), dec!(50))];
        let depth = total_depth_usd(&asks);
        let walk = OrderbookWalker::walk_asks_by_cost(
            &asks,
            micro_usd(dec!(100)),
            micro_price(dec!(0.95)),
            depth,
        )
        .unwrap();
        assert_eq!(walk.levels_consumed, 1);
    }

    #[test]
    fn depth_used_pct_from_precomputed_total() {
        let asks = [level(dec!(0.97), dec!(50)), level(dec!(0.98), dec!(50))];
        let depth = total_depth_usd(&asks);
        let walk = OrderbookWalker::walk_asks_by_cost(
            &asks,
            micro_usd(dec!(48.5)),
            micro_price(dec!(0.95)),
            depth,
        )
        .unwrap();
        assert!(walk.depth_used_pct > 0);
        assert!(walk.depth_used_pct <= 100);
    }

    #[test]
    fn zero_shares_returns_none() {
        let asks = [level(dec!(0.97), dec!(0))];
        let depth = total_depth_usd(&asks);
        assert!(
            OrderbookWalker::walk_asks_by_cost(
                &asks,
                micro_usd(dec!(100)),
                micro_price(dec!(0.95)),
                depth,
            )
            .is_none()
        );
    }
}
