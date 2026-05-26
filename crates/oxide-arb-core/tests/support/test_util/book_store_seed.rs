//! Book store seeding for execution integration tests.

use num_traits::ToPrimitive;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_core::pipeline::book_store::BookStore;
use oxide_arb_models::domain::book::BookLevel;
use oxide_arb_models::types::{Price, Shares};
use rust_decimal_macros::dec;

pub fn seed_book_store(store: &BookStore, scored: &ScoredOpportunity) {
    let now_ms = chrono::Utc::now()
        .timestamp_millis()
        .max(0)
        .to_u64()
        .unwrap_or(0);
    let yes_asks = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.92)),
        Shares::new(dec!(1000)),
    )];
    let no_bids = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.07)),
        Shares::new(dec!(1000)),
    )];
    let no_asks = vec![BookLevel::from_decimal_unchecked(
        Price::new(dec!(0.08)),
        Shares::new(dec!(1000)),
    )];
    store.apply_snapshot(&scored.token_yes, vec![], yes_asks, now_ms, None);
    store.apply_snapshot(&scored.token_no, no_bids, no_asks, now_ms, None);
}
