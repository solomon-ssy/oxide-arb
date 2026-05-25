use oxide_arb_models::domain::book::{BookGateError, EndgameBookSnapshot, OrderbookSide};
use oxide_arb_models::types::TokenId;

/// Quality gate for `EndgameBookSnapshot` before entering the detection pipeline.
pub struct BookGate;

impl BookGate {
    /// Collect all quality issues found in `snapshot`.
    pub fn check(
        snapshot: &EndgameBookSnapshot,
        now_ms: u64,
        expired_threshold_ms: u64,
        token_yes: &TokenId,
        token_no: &TokenId,
    ) -> Vec<BookGateError> {
        let mut errors = Vec::new();

        Self::check_side_empty(&snapshot.yes_bids, token_yes, "bids", &mut errors);
        Self::check_side_empty(&snapshot.yes_asks, token_yes, "asks", &mut errors);
        Self::check_side_empty(&snapshot.no_bids, token_no, "bids", &mut errors);
        Self::check_side_empty(&snapshot.no_asks, token_no, "asks", &mut errors);

        Self::check_crossed(
            &snapshot.yes_bids,
            &snapshot.yes_asks,
            token_yes,
            &mut errors,
        );
        Self::check_crossed(&snapshot.no_bids, &snapshot.no_asks, token_no, &mut errors);

        let max_age = snapshot.max_staleness_ms(now_ms);
        if max_age > expired_threshold_ms {
            errors.push(BookGateError::Stale {
                token_id: token_yes.clone(),
                age_ms: max_age,
                threshold_ms: expired_threshold_ms,
            });
        }

        errors
    }

    /// `true` when the snapshot passes all quality checks.
    pub fn pass(
        snapshot: &EndgameBookSnapshot,
        now_ms: u64,
        expired_threshold_ms: u64,
        token_yes: &TokenId,
        token_no: &TokenId,
    ) -> bool {
        Self::check(snapshot, now_ms, expired_threshold_ms, token_yes, token_no).is_empty()
    }

    fn check_side_empty(
        side: &OrderbookSide,
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
        bids: &OrderbookSide,
        asks: &OrderbookSide,
        token_id: &TokenId,
        errors: &mut Vec<BookGateError>,
    ) {
        let Some(best_bid) = bids.best_price() else {
            return;
        };
        let Some(best_ask) = asks.best_price() else {
            return;
        };
        if best_bid >= best_ask {
            errors.push(BookGateError::CrossedBook {
                token_id: token_id.clone(),
                best_bid,
                best_ask,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::domain::book::{BookLevel, OrderbookSide};
    use oxide_arb_models::types::{Price, Shares};
    use rust_decimal_macros::dec;

    fn side(price: rust_decimal::Decimal, ts: u64) -> OrderbookSide {
        OrderbookSide {
            levels: vec![BookLevel {
                price: Price::new(price),
                size: Shares::new(dec!(10)),
            }],
            timestamp_ms: ts,
        }
    }

    fn empty_side(ts: u64) -> OrderbookSide {
        OrderbookSide {
            levels: vec![],
            timestamp_ms: ts,
        }
    }

    #[test]
    fn healthy_snapshot_passes() {
        let snap = EndgameBookSnapshot {
            yes_bids: side(dec!(0.95), 1000),
            yes_asks: side(dec!(0.96), 1000),
            no_bids: side(dec!(0.04), 1000),
            no_asks: side(dec!(0.05), 1000),
        };
        let yes = TokenId::new("y");
        let no = TokenId::new("n");
        assert!(BookGate::pass(&snap, 1100, 5000, &yes, &no));
    }

    #[test]
    fn empty_side_detected() {
        let snap = EndgameBookSnapshot {
            yes_bids: empty_side(1000),
            yes_asks: side(dec!(0.96), 1000),
            no_bids: side(dec!(0.04), 1000),
            no_asks: side(dec!(0.05), 1000),
        };
        let yes = TokenId::new("y");
        let no = TokenId::new("n");
        let errs = BookGate::check(&snap, 1100, 5000, &yes, &no);
        assert_eq!(errs.len(), 1);
        assert!(matches!(
            &errs[0],
            BookGateError::EmptySide { side: "bids", .. }
        ));
    }

    #[test]
    fn stale_data_detected() {
        let snap = EndgameBookSnapshot {
            yes_bids: side(dec!(0.95), 100),
            yes_asks: side(dec!(0.96), 100),
            no_bids: side(dec!(0.04), 100),
            no_asks: side(dec!(0.05), 100),
        };
        let yes = TokenId::new("y");
        let no = TokenId::new("n");
        let errs = BookGate::check(&snap, 10000, 5000, &yes, &no);
        assert_eq!(errs.len(), 1);
        assert!(matches!(&errs[0], BookGateError::Stale { .. }));
    }

    #[test]
    fn crossed_book_detected() {
        let snap = EndgameBookSnapshot {
            yes_bids: side(dec!(0.97), 1000),
            yes_asks: side(dec!(0.96), 1000),
            no_bids: side(dec!(0.04), 1000),
            no_asks: side(dec!(0.05), 1000),
        };
        let yes = TokenId::new("y");
        let no = TokenId::new("n");
        let errs = BookGate::check(&snap, 1100, 5000, &yes, &no);
        assert_eq!(errs.len(), 1);
        assert!(matches!(&errs[0], BookGateError::CrossedBook { .. }));
    }
}
