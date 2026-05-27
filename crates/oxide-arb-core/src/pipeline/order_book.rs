use oxide_arb_models::domain::book::{BookLevel, BookSnapshot};
use oxide_arb_models::enums::common::Side;
use oxide_arb_models::types::{MicroPrice, Price, Shares, TokenId};
use rust_decimal::Decimal;
use std::cmp::{Ordering, Reverse};
use std::sync::Arc;

/// Single-token L2 orderbook (mutable writer-side state).
///
/// Sides are [`Arc<[BookLevel]>`]; [`Self::publish_cow`] shares storage with readers
/// via refcount clone. Deltas copy a side only when a published snapshot still holds it.
pub struct OrderBook {
    bids: Arc<[BookLevel]>,
    asks: Arc<[BookLevel]>,
    last_update_ms: u64,
    token_id: TokenId,
}

impl OrderBook {
    pub fn new(token_id: TokenId) -> Self {
        Self {
            bids: Arc::from([]),
            asks: Arc::from([]),
            last_update_ms: 0,
            token_id,
        }
    }

    /// Replace the entire book (WS snapshot).
    pub fn apply_snapshot(
        &mut self,
        mut bids: Vec<BookLevel>,
        mut asks: Vec<BookLevel>,
        timestamp_ms: u64,
    ) {
        bids.sort_by_key(|b| Reverse(b.price));
        asks.sort_by_key(|a| a.price);

        bids.retain(|l| l.size.is_positive());
        asks.retain(|l| l.size.is_positive());

        self.bids = Arc::from(bids);
        self.asks = Arc::from(asks);
        self.last_update_ms = timestamp_ms;
    }

    /// Apply pre-sorted, pre-filtered snapshot sides without copying level data.
    pub fn apply_snapshot_validated(
        &mut self,
        bids: Arc<[BookLevel]>,
        asks: Arc<[BookLevel]>,
        timestamp_ms: u64,
    ) {
        self.bids = bids;
        self.asks = asks;
        self.last_update_ms = timestamp_ms;
    }

    /// Apply pre-built snapshot sides validated at WS ingress.
    pub fn apply_snapshot_arc(
        &mut self,
        bids: &Arc<[BookLevel]>,
        asks: &Arc<[BookLevel]>,
        timestamp_ms: u64,
    ) {
        self.apply_snapshot_validated(Arc::clone(bids), Arc::clone(asks), timestamp_ms);
    }

    pub fn apply_delta<I>(&mut self, changes: I, timestamp_ms: u64)
    where
        I: IntoIterator<Item = (Side, Price, Shares)>,
    {
        self.apply_delta_cow(changes, timestamp_ms);
    }

    /// Incremental update with explicit copy-on-write when a published snapshot still references a side.
    pub fn apply_delta_cow<I>(&mut self, changes: I, timestamp_ms: u64)
    where
        I: IntoIterator<Item = (Side, Price, Shares)>,
    {
        for (side, price, size) in changes {
            let Ok(level) = BookLevel::from_decimal(price, size) else {
                continue;
            };
            let on_bids =
                matches!(side, Side::Buy) || find_level(&self.bids, level.price, true).is_ok();
            let on_asks =
                matches!(side, Side::Sell) || find_level(&self.asks, level.price, false).is_ok();

            if on_bids && !on_asks {
                mutate_side(&mut self.bids, |levels| {
                    apply_level_delta(levels, level, true);
                });
            } else if on_asks && !on_bids {
                mutate_side(&mut self.asks, |levels| {
                    apply_level_delta(levels, level, false);
                });
            } else if on_bids {
                mutate_side(&mut self.bids, |levels| {
                    apply_level_delta(levels, level, true);
                });
            } else if on_asks {
                mutate_side(&mut self.asks, |levels| {
                    apply_level_delta(levels, level, false);
                });
            } else if level.size.is_positive() {
                if matches!(side, Side::Buy) || self.should_place_on_bids(price) {
                    mutate_side(&mut self.bids, |levels| {
                        apply_level_delta(levels, level, true);
                    });
                } else {
                    mutate_side(&mut self.asks, |levels| {
                        apply_level_delta(levels, level, false);
                    });
                }
            }
        }
        self.last_update_ms = timestamp_ms;
    }

    /// Publish a refcount-only snapshot (no level copy while uniquely owned).
    #[must_use]
    #[inline]
    pub fn publish_cow(&self, version: u64) -> BookSnapshot {
        BookSnapshot::new(
            Arc::clone(&self.bids),
            Arc::clone(&self.asks),
            self.last_update_ms,
            version,
        )
    }

    #[must_use]
    #[inline]
    pub fn publish(&self, version: u64) -> BookSnapshot {
        self.publish_cow(version)
    }

    #[inline]
    pub fn bids(&self) -> &[BookLevel] {
        &self.bids
    }

    #[inline]
    pub fn asks(&self) -> &[BookLevel] {
        &self.asks
    }

    #[inline]
    pub fn best_bid(&self) -> Option<Price> {
        self.bids.first().map(|l| l.price_decimal())
    }

    #[inline]
    pub fn best_ask(&self) -> Option<Price> {
        self.asks.first().map(|l| l.price_decimal())
    }

    #[inline]
    pub fn spread(&self) -> Option<Price> {
        match (self.best_ask(), self.best_bid()) {
            (Some(ask), Some(bid)) => Some(Price::new(ask.inner() - bid.inner())),
            _ => None,
        }
    }

    #[inline]
    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => bid >= ask,
            _ => false,
        }
    }

    pub fn bid_depth(&self) -> Shares {
        self.bids.iter().fold(Shares::ZERO, |acc, l| {
            Shares::new(acc.inner() + l.size.to_decimal())
        })
    }

    pub fn ask_depth(&self) -> Shares {
        self.asks.iter().fold(Shares::ZERO, |acc, l| {
            Shares::new(acc.inner() + l.size.to_decimal())
        })
    }

    #[inline]
    pub const fn last_update_ms(&self) -> u64 {
        self.last_update_ms
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    #[inline]
    pub const fn token_id(&self) -> &TokenId {
        &self.token_id
    }

    fn should_place_on_bids(&self, price: Price) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => {
                let mid = Price::new((bid.inner() + ask.inner()) / Decimal::TWO);
                price < mid
            }
            (Some(bid), None) => price <= bid,
            (None, Some(ask)) => price < ask,
            (None, None) => true,
        }
    }
}

#[inline]
fn mutate_side(side: &mut Arc<[BookLevel]>, mut f: impl FnMut(&mut Vec<BookLevel>)) {
    let mut levels = side.to_vec();
    f(&mut levels);
    *side = Arc::from(levels);
}

#[inline]
fn find_level(levels: &[BookLevel], price: MicroPrice, descending: bool) -> Result<usize, usize> {
    let cmp_fn = |probe: &BookLevel| -> Ordering {
        if descending {
            probe.price.cmp(&price).reverse()
        } else {
            probe.price.cmp(&price)
        }
    };
    levels.binary_search_by(cmp_fn)
}

#[inline]
fn apply_level_delta(levels: &mut Vec<BookLevel>, level: BookLevel, descending: bool) {
    match find_level(levels, level.price, descending) {
        Ok(idx) => {
            if level.size.is_positive() {
                levels[idx].size = level.size;
            } else {
                levels.remove(idx);
            }
        }
        Err(idx) => {
            if level.size.is_positive() {
                levels.insert(idx, level);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn lvl(p: Decimal, s: Decimal) -> BookLevel {
        BookLevel::from_decimal(Price::new(p), Shares::new(s)).unwrap()
    }

    #[test]
    fn snapshot_sorts_and_filters() {
        let mut ob = OrderBook::new(TokenId::new("t1"));
        ob.apply_snapshot(
            vec![
                lvl(dec!(0.5), dec!(10)),
                lvl(dec!(0.6), dec!(5)),
                lvl(dec!(0.55), dec!(0)),
            ],
            vec![lvl(dec!(0.7), dec!(8)), lvl(dec!(0.65), dec!(3))],
            100,
        );
        assert_eq!(ob.best_bid().unwrap().inner(), dec!(0.6));
        assert_eq!(ob.best_ask().unwrap().inner(), dec!(0.65));
        assert_eq!(ob.bids.len(), 2);
    }

    #[test]
    fn publish_cow_refcount_without_level_copy() {
        let mut ob = OrderBook::new(TokenId::new("t1"));
        ob.apply_snapshot(
            vec![lvl(dec!(0.5), dec!(10))],
            vec![lvl(dec!(0.6), dec!(5))],
            1,
        );
        let snap = ob.publish_cow(1);
        assert_eq!(snap.bids.len(), 1);
        assert_eq!(snap.best_bid().unwrap().inner(), dec!(0.5));
        assert!(snap.total_ask_depth_usd.is_positive());
        assert_eq!(Arc::strong_count(&snap.bids), 2);
    }

    #[test]
    fn delta_cow_clones_side_when_snapshot_shared() {
        let mut ob = OrderBook::new(TokenId::new("t1"));
        ob.apply_snapshot(
            vec![lvl(dec!(0.5), dec!(10))],
            vec![lvl(dec!(0.7), dec!(5))],
            100,
        );
        let snap = ob.publish_cow(1);
        assert_eq!(Arc::strong_count(&snap.bids), 2);

        ob.apply_delta_cow(
            [(Side::Buy, Price::new(dec!(0.5)), Shares::new(dec!(20)))],
            200,
        );
        assert_eq!(Arc::strong_count(&snap.bids), 1);
        assert_eq!(ob.bids[0].size.to_decimal(), dec!(20));
        assert_eq!(snap.bids[0].size.to_decimal(), dec!(10));
        assert!(!Arc::ptr_eq(&ob.bids, &snap.bids));
    }

    #[test]
    fn delta_insert_update_remove() {
        let mut ob = OrderBook::new(TokenId::new("t1"));
        ob.apply_snapshot(
            vec![lvl(dec!(0.5), dec!(10))],
            vec![lvl(dec!(0.7), dec!(5))],
            100,
        );

        ob.apply_delta_cow(
            [
                (Side::Buy, Price::new(dec!(0.5)), Shares::new(dec!(20))),
                (Side::Buy, Price::new(dec!(0.55)), Shares::new(dec!(15))),
                (Side::Sell, Price::new(dec!(0.7)), Shares::ZERO),
                (Side::Sell, Price::new(dec!(0.65)), Shares::new(dec!(8))),
            ],
            200,
        );

        assert_eq!(ob.best_bid().unwrap().inner(), dec!(0.55));
        assert_eq!(ob.bids[0].size.to_decimal(), dec!(15));
        assert_eq!(ob.best_ask().unwrap().inner(), dec!(0.65));
        assert_eq!(ob.asks.len(), 1);
    }
}
