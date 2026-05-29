use oxide_arb_models::{
    domain::{
        BookSideView,
        book::{BookGateError, EndgameBookPair, EndgameBookView},
    },
    types::TokenId,
};

/// Quality gate for orderbooks before entering the detection pipeline.
pub struct BookGate;

impl BookGate {
    /// Collect all quality issues found in `book`.
    pub fn check(
        book: EndgameBookView<'_>,
        now_ms: u64,
        expired_threshold_ms: u64,
        token_yes: &TokenId,
        token_no: &TokenId,
    ) -> Vec<BookGateError> {
        let mut errors = Vec::new();

        Self::check_side_empty(book.yes_bids, token_yes, "bids", &mut errors);
        Self::check_side_empty(book.yes_asks, token_yes, "asks", &mut errors);
        Self::check_side_empty(book.no_bids, token_no, "bids", &mut errors);
        Self::check_side_empty(book.no_asks, token_no, "asks", &mut errors);

        Self::check_crossed(book.yes_bids, book.yes_asks, token_yes, &mut errors);
        Self::check_crossed(book.no_bids, book.no_asks, token_no, &mut errors);

        let max_age = book.max_staleness_ms(now_ms);
        if max_age > expired_threshold_ms {
            let (leg, token_id, age_ms) = book.stalest_leg(now_ms, token_yes, token_no);
            errors.push(BookGateError::Stale {
                leg,
                token_id,
                age_ms,
                threshold_ms: expired_threshold_ms,
            });
        }

        errors
    }

    /// `true` when the book passes all quality checks (zero allocation).
    #[inline]
    pub fn pass(
        pair: &EndgameBookPair,
        now_ms: u64,
        acceptable_threshold_ms: u64,
        _token_yes: &TokenId,
        _token_no: &TokenId,
    ) -> bool {
        let book = pair.view();
        if book.yes_bids.is_empty()
            || book.yes_asks.is_empty()
            || book.no_bids.is_empty()
            || book.no_asks.is_empty()
        {
            return false;
        }
        if Self::is_crossed(book.yes_bids, book.yes_asks)
            || Self::is_crossed(book.no_bids, book.no_asks)
        {
            return false;
        }
        book.max_staleness_ms(now_ms) <= acceptable_threshold_ms
    }

    #[inline]
    fn is_crossed(bids: BookSideView<'_>, asks: BookSideView<'_>) -> bool {
        match (bids.best_price(), asks.best_price()) {
            (Some(bid), Some(ask)) => bid >= ask,
            _ => false,
        }
    }

    fn check_side_empty(
        side: BookSideView<'_>,
        token_id: &TokenId,
        side_name: &'static str,
        errors: &mut Vec<BookGateError>,
    ) {
        if side.is_empty() {
            errors.push(BookGateError::EmptySide {
                token_id: token_id.clone(),
                side: side_name,
            });
        }
    }

    fn check_crossed(
        bids: BookSideView<'_>,
        asks: BookSideView<'_>,
        token_id: &TokenId,
        errors: &mut Vec<BookGateError>,
    ) {
        if let (Some(best_bid), Some(best_ask)) = (bids.best_price(), asks.best_price()) {
            if best_bid >= best_ask {
                errors.push(BookGateError::CrossedBook {
                    token_id: token_id.clone(),
                    best_bid,
                    best_ask,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::{
        domain::book::{BookGateError, BookGateLeg, BookLevel, BookSnapshot, EndgameBookPair},
        types::{Price, Shares},
    };
    use rust_decimal_macros::dec;
    use std::sync::Arc;
    fn snap(price: rust_decimal::Decimal, ts: u64) -> Arc<BookSnapshot> {
        let bids = Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(price),
            Shares::new(dec!(10)),
        )]);
        let asks = Arc::from([BookLevel::from_decimal_unchecked(
            Price::new(price + dec!(0.01)),
            Shares::new(dec!(10)),
        )]);
        Arc::new(BookSnapshot::new(bids, asks, ts, 0))
    }

    #[test]
    fn healthy_snapshot_passes() {
        let pair = EndgameBookPair {
            yes: snap(dec!(0.95), 1000),
            no: snap(dec!(0.04), 1000),
        };
        let yes = TokenId::new("y");
        let no = TokenId::new("n");
        assert!(BookGate::pass(&pair, 1100, 5000, &yes, &no));
    }

    #[test]
    fn rejects_book_in_stale_window() {
        let pair = EndgameBookPair {
            yes: snap(dec!(0.95), 1000),
            no: snap(dec!(0.04), 1000),
        };
        let yes = TokenId::new("y");
        let no = TokenId::new("n");
        assert!(!BookGate::pass(&pair, 20_000, 5_000, &yes, &no));
    }

    #[test]
    fn stale_error_attributes_no_leg_when_no_is_older() {
        let pair = EndgameBookPair {
            yes: snap(dec!(0.95), 9_000),
            no: snap(dec!(0.04), 1_000),
        };
        let yes = TokenId::new("yes-token");
        let no = TokenId::new("no-token");
        let errors = BookGate::check(pair.view(), 10_000, 5_000, &yes, &no);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            BookGateError::Stale {
                leg: BookGateLeg::No,
                token_id,
                age_ms: 9_000,
                threshold_ms: 5_000,
            } if token_id.as_str() == "no-token"
        ));
    }

    #[test]
    fn stale_error_attributes_yes_leg_when_yes_is_older() {
        let pair = EndgameBookPair {
            yes: snap(dec!(0.95), 500),
            no: snap(dec!(0.04), 8_000),
        };
        let yes = TokenId::new("yes-token");
        let no = TokenId::new("no-token");
        let errors = BookGate::check(pair.view(), 10_000, 5_000, &yes, &no);
        assert!(matches!(
            &errors[0],
            BookGateError::Stale {
                leg: BookGateLeg::Yes,
                token_id,
                ..
            } if token_id.as_str() == "yes-token"
        ));
    }
}
