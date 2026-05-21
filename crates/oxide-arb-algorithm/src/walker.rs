//! Orderbook walk simulation for entry cost and slippage estimation.
//!
//! Simulates filling a FOK order by walking through orderbook levels.
//! All arithmetic uses `rust_decimal::Decimal` — no floating-point.

use oxide_arb_models::{
    domain::BookLevel,
    types::{Bps, Price, Shares, Usd},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Result of walking through orderbook levels.
#[derive(Debug, Clone)]
pub struct WalkResult {
    /// Total shares that would be filled.
    pub shares: Shares,
    /// Volume-weighted average price across consumed levels.
    pub vwap: Price,
    /// Total USD cost (sum of `price × size` per consumed level).
    pub total_cost: Usd,
    /// Number of price levels partially or fully consumed.
    pub levels_consumed: usize,
    /// Whether the walk exhausted all available liquidity before budget/target.
    pub fully_filled: bool,
    /// Percentage of total available depth consumed (0–100).
    pub depth_used_pct: Decimal,
}

/// Stateless orderbook walker.
pub struct OrderbookWalker;

impl OrderbookWalker {
    /// Walk ask levels to simulate buying up to `max_cost_usd` worth of shares.
    ///
    /// Levels **must** be sorted ascending by price. The walk stops when:
    /// - The budget is exhausted.
    /// - No more levels remain.
    /// - A level's price drops below `price_floor` (if set).
    ///
    /// Returns `None` if zero shares would be filled.
    #[must_use]
    pub fn walk_asks_by_cost(
        asks: &[BookLevel],
        max_cost_usd: Decimal,
        price_floor: Option<Decimal>,
    ) -> Option<WalkResult> {
        let total_available_depth: Decimal =
            asks.iter().map(|l| l.price.inner() * l.size.inner()).sum();

        let mut remaining_budget = max_cost_usd;
        let mut total_shares = Decimal::ZERO;
        let mut total_cost = Decimal::ZERO;
        let mut levels_consumed = 0_usize;

        for level in asks {
            if let Some(floor) = price_floor {
                if level.price.inner() < floor {
                    break;
                }
            }

            if remaining_budget <= Decimal::ZERO {
                break;
            }

            let affordable = remaining_budget / level.price.inner();
            let fill = affordable.min(level.size.inner());
            let cost = fill * level.price.inner();

            total_shares += fill;
            total_cost += cost;
            remaining_budget -= cost;
            levels_consumed += 1;
        }

        if total_shares.is_zero() {
            return None;
        }

        let vwap = total_cost / total_shares;
        let depth_used_pct = if total_available_depth.is_zero() {
            Decimal::ZERO
        } else {
            total_cost / total_available_depth * dec!(100)
        };
        let fully_filled = remaining_budget > Decimal::ZERO;

        Some(WalkResult {
            shares: Shares::new(total_shares),
            vwap: Price::new(vwap),
            total_cost: Usd::new(total_cost),
            levels_consumed,
            fully_filled,
            depth_used_pct,
        })
    }

    /// Walk ask levels to buy exactly `target_shares` (or as many as available).
    ///
    /// Levels **must** be sorted ascending by price.
    #[must_use]
    pub fn walk_asks_by_shares(
        asks: &[BookLevel],
        target_shares: Decimal,
        price_floor: Option<Decimal>,
    ) -> Option<WalkResult> {
        let total_available_depth: Decimal =
            asks.iter().map(|l| l.price.inner() * l.size.inner()).sum();

        let mut remaining = target_shares;
        let mut total_shares = Decimal::ZERO;
        let mut total_cost = Decimal::ZERO;
        let mut levels_consumed = 0_usize;

        for level in asks {
            if let Some(floor) = price_floor {
                if level.price.inner() < floor {
                    break;
                }
            }

            if remaining <= Decimal::ZERO {
                break;
            }

            let fill = remaining.min(level.size.inner());
            let cost = fill * level.price.inner();

            total_shares += fill;
            total_cost += cost;
            remaining -= fill;
            levels_consumed += 1;
        }

        if total_shares.is_zero() {
            return None;
        }

        let vwap = total_cost / total_shares;
        let depth_used_pct = if total_available_depth.is_zero() {
            Decimal::ZERO
        } else {
            total_cost / total_available_depth * dec!(100)
        };
        let fully_filled = remaining <= Decimal::ZERO;

        Some(WalkResult {
            shares: Shares::new(total_shares),
            vwap: Price::new(vwap),
            total_cost: Usd::new(total_cost),
            levels_consumed,
            fully_filled,
            depth_used_pct,
        })
    }

    /// Walk bid levels to simulate selling `target_shares`.
    ///
    /// Levels **must** be sorted descending by price.
    #[must_use]
    pub fn walk_bids_by_shares(bids: &[BookLevel], target_shares: Decimal) -> Option<WalkResult> {
        let total_available_depth: Decimal =
            bids.iter().map(|l| l.price.inner() * l.size.inner()).sum();

        let mut remaining = target_shares;
        let mut total_shares = Decimal::ZERO;
        let mut total_cost = Decimal::ZERO;
        let mut levels_consumed = 0_usize;

        for level in bids {
            if remaining <= Decimal::ZERO {
                break;
            }

            let fill = remaining.min(level.size.inner());
            let cost = fill * level.price.inner();

            total_shares += fill;
            total_cost += cost;
            remaining -= fill;
            levels_consumed += 1;
        }

        if total_shares.is_zero() {
            return None;
        }

        let vwap = total_cost / total_shares;
        let depth_used_pct = if total_available_depth.is_zero() {
            Decimal::ZERO
        } else {
            total_cost / total_available_depth * dec!(100)
        };
        let fully_filled = remaining <= Decimal::ZERO;

        Some(WalkResult {
            shares: Shares::new(total_shares),
            vwap: Price::new(vwap),
            total_cost: Usd::new(total_cost),
            levels_consumed,
            fully_filled,
            depth_used_pct,
        })
    }
}

/// Estimate slippage in basis points relative to a reference price.
///
/// Positive bps means the execution price is worse than reference (higher
/// for buys). Returns `None` when no shares could be filled.
#[must_use]
pub fn estimate_slippage(
    asks: &[BookLevel],
    target_shares: Decimal,
    reference_price: Price,
) -> Option<Bps> {
    let walk = OrderbookWalker::walk_asks_by_shares(asks, target_shares, None)?;
    Bps::spread(walk.vwap, reference_price)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(price: Decimal, size: Decimal) -> BookLevel {
        BookLevel {
            price: Price::new(price),
            size: Shares::new(size),
        }
    }

    #[test]
    fn single_level_full_fill() {
        let asks = vec![level(dec!(0.97), dec!(100))];
        let walk = OrderbookWalker::walk_asks_by_cost(&asks, dec!(500), None).unwrap();

        assert_eq!(walk.shares.inner(), dec!(100));
        assert_eq!(walk.vwap.inner(), dec!(0.97));
        assert_eq!(walk.total_cost.inner(), dec!(97));
        assert_eq!(walk.levels_consumed, 1);
        assert!(walk.fully_filled);
    }

    #[test]
    fn budget_exhaustion_mid_level() {
        let asks = vec![level(dec!(0.97), dec!(1000))];
        let walk = OrderbookWalker::walk_asks_by_cost(&asks, dec!(97), None).unwrap();

        assert_eq!(walk.shares.inner(), dec!(100));
        assert_eq!(walk.vwap.inner(), dec!(0.97));
        assert_eq!(walk.total_cost.inner(), dec!(97));
    }

    #[test]
    fn multi_level_vwap() {
        let asks = vec![level(dec!(0.96), dec!(100)), level(dec!(0.97), dec!(100))];
        let walk = OrderbookWalker::walk_asks_by_cost(&asks, dec!(500), None).unwrap();

        assert_eq!(walk.shares.inner(), dec!(200));
        assert_eq!(walk.vwap.inner(), dec!(0.965));
        assert_eq!(walk.total_cost.inner(), dec!(193));
        assert_eq!(walk.levels_consumed, 2);
    }

    #[test]
    fn price_floor_stops_walk() {
        let asks = vec![level(dec!(0.97), dec!(100)), level(dec!(0.94), dec!(100))];
        let walk = OrderbookWalker::walk_asks_by_cost(&asks, dec!(500), Some(dec!(0.95))).unwrap();

        assert_eq!(walk.levels_consumed, 1);
        assert_eq!(walk.shares.inner(), dec!(100));
    }

    #[test]
    fn empty_asks_returns_none() {
        let result = OrderbookWalker::walk_asks_by_cost(&[], dec!(500), None);
        assert!(result.is_none());
    }

    #[test]
    fn walk_by_shares_exact_fill() {
        let asks = vec![level(dec!(0.97), dec!(200))];
        let walk = OrderbookWalker::walk_asks_by_shares(&asks, dec!(100), None).unwrap();

        assert_eq!(walk.shares.inner(), dec!(100));
        assert!(walk.fully_filled);
    }

    #[test]
    fn walk_by_shares_partial_fill() {
        let asks = vec![level(dec!(0.97), dec!(50))];
        let walk = OrderbookWalker::walk_asks_by_shares(&asks, dec!(100), None).unwrap();

        assert_eq!(walk.shares.inner(), dec!(50));
        assert!(!walk.fully_filled);
    }

    #[test]
    fn walk_bids_by_shares_basic() {
        let bids = vec![level(dec!(0.97), dec!(100)), level(dec!(0.96), dec!(100))];
        let walk = OrderbookWalker::walk_bids_by_shares(&bids, dec!(150)).unwrap();

        assert_eq!(walk.shares.inner(), dec!(150));
        let expected_cost = dec!(100) * dec!(0.97) + dec!(50) * dec!(0.96);
        assert_eq!(walk.total_cost.inner(), expected_cost);
    }

    #[test]
    fn depth_used_pct_calculated() {
        let asks = vec![level(dec!(0.97), dec!(1000))];
        let walk = OrderbookWalker::walk_asks_by_cost(&asks, dec!(97), None).unwrap();

        let expected_pct = dec!(97) / dec!(970) * dec!(100);
        assert_eq!(walk.depth_used_pct, expected_pct);
    }

    #[test]
    fn slippage_estimation() {
        let asks = vec![level(dec!(0.97), dec!(50)), level(dec!(0.98), dec!(50))];
        let slippage = estimate_slippage(&asks, dec!(100), Price::new(dec!(0.97)));
        assert!(slippage.is_some());
        let bps = slippage.unwrap();
        assert!(bps.inner() > Decimal::ZERO);
    }
}
