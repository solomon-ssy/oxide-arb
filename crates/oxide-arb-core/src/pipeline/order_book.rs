use oxide_arb_models::domain::book::{BookLevel, BookSnapshot};
use oxide_arb_models::types::{MicroPrice, Price, Shares, TokenId};
use rust_decimal::Decimal;
use std::cmp::{Ordering, Reverse};
use std::sync::Arc;

/// Single-token L2 orderbook (mutable writer-side state).
///
/// `bids` / `asks` are [`Arc<Vec<BookLevel>>`]; deltas use [`Arc::make_mut`].
/// [`Self::publish`] refcount-clones into [`BookSnapshot`] without copying levels.
pub struct OrderBook {
    bids: Arc<Vec<BookLevel>>,
    asks: Arc<Vec<BookLevel>>,
    last_update_ms: u64,
    token_id: TokenId,
}

impl OrderBook {
    pub fn new(token_id: TokenId) -> Self {
        Self {
            bids: Arc::new(Vec::with_capacity(64)),
            asks: Arc::new(Vec::with_capacity(64)),
            last_update_ms: 0,
            token_id,
        }
    }

    /// Replace the entire book (WS `BookSnapshot` event).
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

        self.bids = Arc::new(bids);
        self.asks = Arc::new(asks);
        self.last_update_ms = timestamp_ms;
    }

    pub fn apply_delta<I>(&mut self, changes: I, timestamp_ms: u64)
    where
        I: IntoIterator<Item = (Price, Shares)>,
    {
        for (price, size) in changes {
            let Ok(level) = BookLevel::from_decimal(price, size) else {
                continue;
            };
            let on_bids = find_level(self.bids.as_ref(), level.price, true).is_ok();
            let on_asks = find_level(self.asks.as_ref(), level.price, false).is_ok();

            if on_bids {
                apply_level_delta(Arc::make_mut(&mut self.bids), level, true);
            } else if on_asks {
                apply_level_delta(Arc::make_mut(&mut self.asks), level, false);
            } else if level.size.is_positive() {
                if self.should_place_on_bids(price) {
                    apply_level_delta(Arc::make_mut(&mut self.bids), level, true);
                } else {
                    apply_level_delta(Arc::make_mut(&mut self.asks), level, false);
                }
            }
        }
        self.last_update_ms = timestamp_ms;
    }

    /// Publish a refcount-only snapshot (no level copy when arcs are unshared).
    #[must_use]
    #[inline]
    pub fn publish(&self, version: u64) -> BookSnapshot {
        BookSnapshot::new(
            Arc::clone(&self.bids),
            Arc::clone(&self.asks),
            self.last_update_ms,
            version,
        )
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
    fn publish_refcount_clones_arc_without_level_copy() {
        let mut ob = OrderBook::new(TokenId::new("t1"));
        ob.apply_snapshot(
            vec![lvl(dec!(0.5), dec!(10))],
            vec![lvl(dec!(0.6), dec!(5))],
            1,
        );
        let snap = ob.publish(1);
        assert_eq!(snap.bids.len(), 1);
        assert_eq!(snap.best_bid().unwrap().inner(), dec!(0.5));
        assert!(snap.total_ask_depth_usd.is_positive());
        assert_eq!(Arc::strong_count(&snap.bids), 2);
    }

    #[test]
    fn delta_insert_update_remove() {
        let mut ob = OrderBook::new(TokenId::new("t1"));
        ob.apply_snapshot(
            vec![lvl(dec!(0.5), dec!(10))],
            vec![lvl(dec!(0.7), dec!(5))],
            100,
        );

        ob.apply_delta(
            [
                (Price::new(dec!(0.5)), Shares::new(dec!(20))),
                (Price::new(dec!(0.55)), Shares::new(dec!(15))),
                (Price::new(dec!(0.7)), Shares::ZERO),
                (Price::new(dec!(0.65)), Shares::new(dec!(8))),
            ],
            200,
        );

        assert_eq!(ob.best_bid().unwrap().inner(), dec!(0.55));
        assert_eq!(ob.bids[0].size.to_decimal(), dec!(15));
        assert_eq!(ob.best_ask().unwrap().inner(), dec!(0.65));
        assert_eq!(ob.asks.len(), 1);
    }
}
