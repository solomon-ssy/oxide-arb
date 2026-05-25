use oxide_arb_models::domain::book::{BookLevel, OrderbookSide};
use oxide_arb_models::types::{Price, Shares, TokenId};
use rust_decimal::Decimal;
use std::cmp::{Ordering, Reverse};

/// Single-token L2 orderbook.
///
/// `bids` are sorted descending by price; `asks` are sorted ascending.
/// Uses `Vec<BookLevel>` for cache-friendly iteration at typical Polymarket
/// depth (~50 levels), outperforming `BTreeMap` on the hot path.
pub struct OrderBook {
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
    last_update_ms: u64,
    token_id: TokenId,
}

impl OrderBook {
    pub const fn new(token_id: TokenId) -> Self {
        Self {
            bids: Vec::new(),
            asks: Vec::new(),
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

        bids.retain(|l| l.size.inner() > Decimal::ZERO);
        asks.retain(|l| l.size.inner() > Decimal::ZERO);

        self.bids = bids;
        self.asks = asks;
        self.last_update_ms = timestamp_ms;
    }

    /// Incremental update (WS `PriceChange` event).
    ///
    /// For each `(price, size)` pair:
    ///   - size > 0 → insert or update that price level
    ///   - size == 0 → remove that price level
    ///
    /// Each price is looked up on both sides; an existing level is updated
    /// in-place on whichever side it's found. A new insert (price not on
    /// either side) is placed on bids if ≥ current mid, asks otherwise.
    pub fn apply_delta(&mut self, changes: &[(Price, Shares)], timestamp_ms: u64) {
        for &(price, size) in changes {
            let on_bids = find_level(&self.bids, price, true).is_ok();
            let on_asks = find_level(&self.asks, price, false).is_ok();

            if on_bids {
                apply_level_delta(&mut self.bids, price, size, true);
            } else if on_asks {
                apply_level_delta(&mut self.asks, price, size, false);
            } else if size.inner() > Decimal::ZERO {
                if self.should_place_on_bids(price) {
                    apply_level_delta(&mut self.bids, price, size, true);
                } else {
                    apply_level_delta(&mut self.asks, price, size, false);
                }
            }
        }
        self.last_update_ms = timestamp_ms;
    }

    pub fn best_bid(&self) -> Option<Price> {
        self.bids.first().map(|l| l.price)
    }

    pub fn best_ask(&self) -> Option<Price> {
        self.asks.first().map(|l| l.price)
    }

    /// Bid-ask spread, `None` if either side is empty.
    pub fn spread(&self) -> Option<Price> {
        match (self.best_ask(), self.best_bid()) {
            (Some(ask), Some(bid)) => Some(Price::new(ask.inner() - bid.inner())),
            _ => None,
        }
    }

    /// `true` when `best_bid` >= `best_ask` (anomalous).
    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => bid >= ask,
            _ => false,
        }
    }

    pub fn bid_depth(&self) -> Shares {
        self.bids.iter().map(|l| l.size).sum()
    }

    pub fn ask_depth(&self) -> Shares {
        self.asks.iter().map(|l| l.size).sum()
    }

    pub fn bid_side(&self) -> OrderbookSide {
        OrderbookSide {
            levels: self.bids.clone(),
            timestamp_ms: self.last_update_ms,
        }
    }

    pub fn ask_side(&self) -> OrderbookSide {
        OrderbookSide {
            levels: self.asks.clone(),
            timestamp_ms: self.last_update_ms,
        }
    }

    pub const fn last_update_ms(&self) -> u64 {
        self.last_update_ms
    }

    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    pub const fn token_id(&self) -> &TokenId {
        &self.token_id
    }

    /// Heuristic: determine whether a new price level belongs on the bid side.
    ///
    /// Bids sit below the midpoint; asks sit above.
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

/// Binary search for `price` within a sorted side.
fn find_level(levels: &[BookLevel], price: Price, descending: bool) -> Result<usize, usize> {
    let cmp_fn = |probe: &BookLevel| -> Ordering {
        if descending {
            probe.price.cmp(&price).reverse()
        } else {
            probe.price.cmp(&price)
        }
    };
    levels.binary_search_by(cmp_fn)
}

/// Apply a single price-level delta to one side of the book.
///
/// `descending` = true for bids (price desc), false for asks (price asc).
fn apply_level_delta(levels: &mut Vec<BookLevel>, price: Price, size: Shares, descending: bool) {
    match find_level(levels, price, descending) {
        Ok(idx) => {
            if size.inner() > Decimal::ZERO {
                levels[idx].size = size;
            } else {
                levels.remove(idx);
            }
        }
        Err(idx) => {
            if size.inner() > Decimal::ZERO {
                levels.insert(idx, BookLevel { price, size });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn lvl(p: Decimal, s: Decimal) -> BookLevel {
        BookLevel {
            price: Price::new(p),
            size: Shares::new(s),
        }
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
    fn delta_insert_update_remove() {
        let mut ob = OrderBook::new(TokenId::new("t1"));
        ob.apply_snapshot(
            vec![lvl(dec!(0.5), dec!(10))],
            vec![lvl(dec!(0.7), dec!(5))],
            100,
        );

        ob.apply_delta(
            &[
                (Price::new(dec!(0.5)), Shares::new(dec!(20))),
                (Price::new(dec!(0.55)), Shares::new(dec!(15))),
                (Price::new(dec!(0.7)), Shares::ZERO),
                (Price::new(dec!(0.65)), Shares::new(dec!(8))),
            ],
            200,
        );

        assert_eq!(ob.best_bid().unwrap().inner(), dec!(0.55));
        assert_eq!(ob.bids[0].size.inner(), dec!(15));
        assert_eq!(ob.bids[1].size.inner(), dec!(20));
        assert_eq!(ob.best_ask().unwrap().inner(), dec!(0.65));
        assert_eq!(ob.asks.len(), 1);
        assert_eq!(ob.last_update_ms(), 200);
    }

    #[test]
    fn spread_and_crossed() {
        let mut ob = OrderBook::new(TokenId::new("t1"));
        assert!(ob.spread().is_none());
        assert!(!ob.is_crossed());

        ob.apply_snapshot(
            vec![lvl(dec!(0.6), dec!(10))],
            vec![lvl(dec!(0.7), dec!(10))],
            1,
        );
        assert_eq!(ob.spread().unwrap().inner(), dec!(0.1));
        assert!(!ob.is_crossed());

        ob.apply_snapshot(
            vec![lvl(dec!(0.7), dec!(10))],
            vec![lvl(dec!(0.6), dec!(10))],
            2,
        );
        assert!(ob.is_crossed());
    }

    #[test]
    fn depth_calculation() {
        let mut ob = OrderBook::new(TokenId::new("t1"));
        ob.apply_snapshot(
            vec![lvl(dec!(0.5), dec!(10)), lvl(dec!(0.4), dec!(20))],
            vec![lvl(dec!(0.6), dec!(5))],
            1,
        );
        assert_eq!(ob.bid_depth().inner(), dec!(30));
        assert_eq!(ob.ask_depth().inner(), dec!(5));
    }

    #[test]
    fn empty_book() {
        let ob = OrderBook::new(TokenId::new("t1"));
        assert!(ob.is_empty());
        assert_eq!(ob.last_update_ms(), 0);
    }
}
