use oxide_arb_models::domain::book::EndgameBookPair;
use oxide_arb_models::types::TokenId;

use super::book_store::BookStore;

/// Assembles a paired YES+NO orderbook for the endgame detector (zero-copy).
pub struct DualBookAssembler;

impl DualBookAssembler {
    #[inline]
    pub fn assemble(
        book_store: &BookStore,
        token_yes: &TokenId,
        token_no: &TokenId,
    ) -> Option<EndgameBookPair> {
        book_store.load_pair(token_yes, token_no)
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
            vec![BookLevel::from_decimal_unchecked(
                Price::new(dec!(0.95)),
                Shares::new(dec!(100)),
            )],
            vec![BookLevel::from_decimal_unchecked(
                Price::new(dec!(0.96)),
                Shares::new(dec!(50)),
            )],
            1000,
            None,
        );
        store.apply_snapshot(
            &no,
            vec![BookLevel::from_decimal_unchecked(
                Price::new(dec!(0.04)),
                Shares::new(dec!(80)),
            )],
            vec![BookLevel::from_decimal_unchecked(
                Price::new(dec!(0.05)),
                Shares::new(dec!(60)),
            )],
            1000,
            None,
        );

        let pair = DualBookAssembler::assemble(&store, &yes, &no).unwrap();
        assert_eq!(pair.view().yes_bids.levels.len(), 1);
        assert_eq!(pair.view().no_asks.levels.len(), 1);
    }
}
