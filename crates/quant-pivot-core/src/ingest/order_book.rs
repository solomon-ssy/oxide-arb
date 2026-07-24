use std::{
    cmp::{Ordering, Reverse},
    sync::Arc,
};

use quant_pivot_models::{
    domain::market::book::{BookLevel, BookSnapshot},
    enums::common::Side,
    types::{Price, Shares, TokenId},
};

/// Sides whose immutable storage changed during one canonical delta batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookMutation {
    pub bids_changed: bool,
    pub asks_changed: bool,
}

#[derive(Debug, Clone, Copy)]
struct SequencedLevel {
    level: BookLevel,
    sequence: usize,
}

/// Reusable partition-local buffers for delta normalization and linear merge.
#[derive(Default)]
pub(crate) struct BookDeltaScratch {
    bid_changes: Vec<SequencedLevel>,
    ask_changes: Vec<SequencedLevel>,
    merged: Vec<BookLevel>,
}

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

    pub fn apply_delta<I>(&mut self, changes: I, timestamp_ms: u64) -> BookMutation
    where
        I: IntoIterator<Item = (Side, Price, Shares)>,
    {
        self.apply_delta_cow(changes, timestamp_ms)
    }

    /// Sort, coalesce and linearly merge one canonical delta batch.
    ///
    /// The last value for each `(side, price)` wins. Each changed side creates
    /// at most one new immutable allocation; an unchanged side keeps the exact
    /// same [`Arc`] so readers never pay for the other side's mutation.
    pub fn apply_delta_cow<I>(&mut self, changes: I, timestamp_ms: u64) -> BookMutation
    where
        I: IntoIterator<Item = (Side, Price, Shares)>,
    {
        let mut scratch = BookDeltaScratch::default();
        self.apply_delta_with_scratch(changes, timestamp_ms, &mut scratch)
    }

    pub(crate) fn apply_delta_with_scratch<I>(
        &mut self,
        changes: I,
        timestamp_ms: u64,
        scratch: &mut BookDeltaScratch,
    ) -> BookMutation
    where
        I: IntoIterator<Item = (Side, Price, Shares)>,
    {
        scratch.bid_changes.clear();
        scratch.ask_changes.clear();
        for (sequence, (side, price, size)) in changes.into_iter().enumerate() {
            let Ok(level) = BookLevel::from_decimal(price, size) else {
                continue;
            };
            let change = SequencedLevel { level, sequence };
            match side {
                Side::Buy => scratch.bid_changes.push(change),
                Side::Sell => scratch.ask_changes.push(change),
            }
        }
        canonicalize_changes(&mut scratch.bid_changes, true);
        canonicalize_changes(&mut scratch.ask_changes, false);
        let bids_changed = merge_side(
            &mut self.bids,
            &scratch.bid_changes,
            true,
            &mut scratch.merged,
        );
        let asks_changed = merge_side(
            &mut self.asks,
            &scratch.ask_changes,
            false,
            &mut scratch.merged,
        );
        self.last_update_ms = timestamp_ms;
        BookMutation {
            bids_changed,
            asks_changed,
        }
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
}

fn canonicalize_changes(changes: &mut Vec<SequencedLevel>, descending: bool) {
    if changes.is_empty() {
        return;
    }
    changes.sort_unstable_by(|left, right| {
        left.level
            .price
            .cmp(&right.level.price)
            .then_with(|| left.sequence.cmp(&right.sequence))
    });
    let mut read = 0;
    let mut write = 0;
    while read < changes.len() {
        let price = changes[read].level.price;
        let mut last = changes[read];
        read += 1;
        while read < changes.len() && changes[read].level.price == price {
            last = changes[read];
            read += 1;
        }
        changes[write] = last;
        write += 1;
    }
    changes.truncate(write);
    if descending {
        changes.reverse();
    }
}

fn merge_side(
    side: &mut Arc<[BookLevel]>,
    changes: &[SequencedLevel],
    descending: bool,
    merged: &mut Vec<BookLevel>,
) -> bool {
    if changes.is_empty() {
        return false;
    }
    let compare = |left: &BookLevel, right: &BookLevel| -> Ordering {
        if descending {
            left.price.cmp(&right.price).reverse()
        } else {
            left.price.cmp(&right.price)
        }
    };
    merged.clear();
    merged.reserve(side.len().saturating_add(changes.len()));
    let mut current_index = 0;
    let mut change_index = 0;
    while current_index < side.len() && change_index < changes.len() {
        let current = side[current_index];
        let change = changes[change_index].level;
        match compare(&current, &change) {
            Ordering::Less => {
                merged.push(current);
                current_index += 1;
            }
            Ordering::Greater => {
                if change.size.is_positive() {
                    merged.push(change);
                }
                change_index += 1;
            }
            Ordering::Equal => {
                if change.size.is_positive() {
                    merged.push(change);
                }
                current_index += 1;
                change_index += 1;
            }
        }
    }
    merged.extend_from_slice(&side[current_index..]);
    merged.extend(
        changes[change_index..]
            .iter()
            .map(|change| change.level)
            .filter(|level| level.size.is_positive()),
    );
    if merged.as_slice() == side.as_ref() {
        return false;
    }
    *side = Arc::from(merged.as_slice());
    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::{
        bool::ANY as ANY_BOOL, collection::vec as strategy_vec, prop_assert_eq, proptest,
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;
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
    fn publish_cow_without_copy() {
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
    fn delta_cow_clones_shared() {
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

    #[test]
    fn delta_reuses_duplicate_wins() {
        let mut ob = OrderBook::new(TokenId::new("t1"));
        ob.apply_snapshot(
            vec![lvl(dec!(0.5), dec!(10))],
            vec![lvl(dec!(0.7), dec!(5))],
            100,
        );
        let before = ob.publish_cow(1);
        let mutation = ob.apply_delta_cow(
            [
                (Side::Buy, Price::new(dec!(0.5)), Shares::new(dec!(20))),
                (Side::Buy, Price::new(dec!(0.5)), Shares::new(dec!(30))),
            ],
            200,
        );
        let after = ob.publish_cow(2);

        assert_eq!(
            mutation,
            BookMutation {
                bids_changed: true,
                asks_changed: false,
            }
        );
        assert_eq!(after.bids[0].size_decimal(), Shares::new(dec!(30)));
        assert!(Arc::ptr_eq(&before.asks, &after.asks));
        assert!(!Arc::ptr_eq(&before.bids, &after.bids));
    }

    proptest! {
        #[test]
        fn linear_merge_matches_map(
            initial_bids in strategy_vec((1_u8..100, 1_u16..1_000), 0..80),
            initial_asks in strategy_vec((1_u8..100, 1_u16..1_000), 0..80),
            changes in strategy_vec((ANY_BOOL, 1_u8..100, 0_u16..1_000), 0..200),
        ) {
            let mut reference_bids = initial_bids.into_iter().collect::<BTreeMap<_, _>>();
            let mut reference_asks = initial_asks.into_iter().collect::<BTreeMap<_, _>>();
            let mut ob = OrderBook::new(TokenId::new("property"));
            let to_level = |(price, size): (&u8, &u16)| {
                lvl(
                    Decimal::new(i64::from(*price), 2),
                    Decimal::from(*size),
                )
            };
            let bids = reference_bids.iter().rev().map(to_level).collect();
            let asks = reference_asks.iter().map(to_level).collect();
            ob.apply_snapshot(bids, asks, 1);

            let deltas = changes
                .iter()
                .map(|(buy, price, size)| {
                    let side = if *buy { Side::Buy } else { Side::Sell };
                    (
                        side,
                        Price::new(Decimal::new(i64::from(*price), 2)),
                        Shares::new(Decimal::from(*size)),
                    )
                })
                .collect::<Vec<_>>();
            for (buy, price, size) in changes {
                let side = if buy {
                    &mut reference_bids
                } else {
                    &mut reference_asks
                };
                if size == 0 {
                    side.remove(&price);
                } else {
                    side.insert(price, size);
                }
            }
            ob.apply_delta_cow(deltas, 2);

            let observed_bids = ob
                .bids()
                .iter()
                .map(|level| (level.price_decimal().inner(), level.size_decimal().inner()))
                .collect::<Vec<_>>();
            let observed_asks = ob
                .asks()
                .iter()
                .map(|level| (level.price_decimal().inner(), level.size_decimal().inner()))
                .collect::<Vec<_>>();
            let expected_bids = reference_bids
                .iter()
                .rev()
                .map(|(price, size)| {
                    (Decimal::new(i64::from(*price), 2), Decimal::from(*size))
                })
                .collect::<Vec<_>>();
            let expected_asks = reference_asks
                .iter()
                .map(|(price, size)| {
                    (Decimal::new(i64::from(*price), 2), Decimal::from(*size))
                })
                .collect::<Vec<_>>();
            prop_assert_eq!(observed_bids, expected_bids);
            prop_assert_eq!(observed_asks, expected_asks);
        }
    }
}
