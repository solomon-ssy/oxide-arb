use oxide_arb_models::domain::book::EndgameBookSnapshot;
use oxide_arb_models::types::TokenId;

use super::book_store::BookStore;

/// Assembles a paired YES+NO orderbook snapshot for the endgame detector.
///
/// This is a stateless utility — all state lives in `BookStore`.
pub struct DualBookAssembler;

impl DualBookAssembler {
    /// Build an `EndgameBookSnapshot` from two token orderbooks.
    ///
    /// Returns `None` if either token's book is missing from the store.
    /// Lock order: YES first, then NO (consistent to prevent deadlocks).
    pub fn assemble(
        book_store: &BookStore,
        token_yes: &TokenId,
        token_no: &TokenId,
    ) -> Option<EndgameBookSnapshot> {
        let yes_lock = book_store.get(token_yes)?;
        let no_lock = book_store.get(token_no)?;

        let yes_guard = yes_lock.read();
        let no_guard = no_lock.read();

        Some(EndgameBookSnapshot {
            yes_bids: yes_guard.bid_side(),
            yes_asks: yes_guard.ask_side(),
            no_bids: no_guard.bid_side(),
            no_asks: no_guard.ask_side(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::metrics_hub::MetricsHub;
    use oxide_arb_models::domain::book::BookLevel;
    use oxide_arb_models::types::{Price, Shares};
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    #[test]
    fn assemble_from_two_books() {
        let metrics = Arc::new(MetricsHub::new());
        let store = BookStore::new(metrics);

        let yes = TokenId::new("yes-tok");
        let no = TokenId::new("no-tok");

        store.apply_snapshot(
            &yes,
            vec![BookLevel {
                price: Price::new(dec!(0.95)),
                size: Shares::new(dec!(100)),
            }],
            vec![BookLevel {
                price: Price::new(dec!(0.96)),
                size: Shares::new(dec!(50)),
            }],
            1000,
        );
        store.apply_snapshot(
            &no,
            vec![BookLevel {
                price: Price::new(dec!(0.04)),
                size: Shares::new(dec!(80)),
            }],
            vec![BookLevel {
                price: Price::new(dec!(0.05)),
                size: Shares::new(dec!(60)),
            }],
            1000,
        );

        let snap = DualBookAssembler::assemble(&store, &yes, &no).unwrap();
        assert_eq!(snap.yes_bids.levels.len(), 1);
        assert_eq!(snap.no_asks.levels.len(), 1);
    }

    #[test]
    fn missing_token_returns_none() {
        let metrics = Arc::new(MetricsHub::new());
        let store = BookStore::new(metrics);
        let yes = TokenId::new("yes");
        let no = TokenId::new("no");
        assert!(DualBookAssembler::assemble(&store, &yes, &no).is_none());
    }
}
