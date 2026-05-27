//! Property tests: `OrderBook` COW delta invariants.

use oxide_arb_core::pipeline::order_book::OrderBook;
use oxide_arb_models::domain::book::BookLevel;
use oxide_arb_models::enums::common::Side;
use oxide_arb_models::types::{Price, Shares, TokenId};
use proptest::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn level(price: Decimal, size: Decimal) -> BookLevel {
    BookLevel::from_decimal(Price::new(price), Shares::new(size)).unwrap()
}

fn naive_tob(bids: &[BookLevel], asks: &[BookLevel]) -> (Option<Decimal>, Option<Decimal>) {
    let best_bid = bids.iter().map(|l| l.price_decimal().inner()).max();
    let best_ask = asks.iter().map(|l| l.price_decimal().inner()).min();
    (best_bid, best_ask)
}

proptest! {
    #[test]
    fn delta_preserves_tob(
        bid_price in 0.01f64..0.99,
        ask_price in 0.01f64..0.99,
        delta_price in 0.01f64..0.99,
        delta_size in 1.0f64..100.0,
    ) {
        let bid = Decimal::try_from(bid_price).unwrap();
        let ask = Decimal::try_from(ask_price.max(bid_price + 0.01)).unwrap_or(dec!(0.99));
        let d_price = Decimal::try_from(delta_price).unwrap();
        let d_size = Decimal::try_from(delta_size).unwrap();

        let mut ob = OrderBook::new(TokenId::new("t"));
        ob.apply_snapshot(
            vec![level(bid, dec!(10))],
            vec![level(ask, dec!(10))],
            1,
        );

        let before = ob.publish_cow(1);
        ob.apply_delta_cow(
            [(Side::Buy, Price::new(d_price), Shares::new(d_size))],
            2,
        );
        let after = ob.publish_cow(2);

        let (bb, ba) = naive_tob(&after.bids, &after.asks);
        prop_assert_eq!(after.best_bid().map(Price::inner), bb);
        prop_assert_eq!(after.best_ask().map(Price::inner), ba);

        let had_bid = before.bids.iter().any(|l| l.price_decimal().inner() == d_price);
        if d_size == dec!(0) && had_bid {
            prop_assert!(after.bids.iter().all(|l| l.price_decimal().inner() != d_price));
        }
    }

    #[test]
    fn version_monotonic(deltas in prop::collection::vec((0.01f64..0.99, 1.0f64..50.0), 1..20)) {
        let mut ob = OrderBook::new(TokenId::new("t"));
        ob.apply_snapshot(vec![level(dec!(0.5), dec!(10))], vec![level(dec!(0.55), dec!(10))], 1);
        let mut version = 1u64;
        for (p, s) in deltas {
            version += 1;
            let price = Decimal::try_from(p).unwrap();
            let size = Decimal::try_from(s).unwrap();
            ob.apply_delta_cow([(Side::Buy, Price::new(price), Shares::new(size))], version);
            let snap = ob.publish_cow(version);
            prop_assert_eq!(snap.version, version);
        }
    }
}
