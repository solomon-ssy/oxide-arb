//! Deterministic reconciliation verdict.
//!
//! Pure function over structured venue facts — same facts, same verdict, no
//! I/O. The decisive evidence is the CLOB order status (still resting?) and the
//! CLOB trades (realized fill); token/account balances corroborate. Anything
//! the evidence cannot prove resolves to [`ReconciliationResult::Unresolvable`]
//! (never guess), and a still-uncertain order returns
//! [`ReconciliationResult::Pending`] for the next sweep.

use quant_pivot_models::{
    enums::execution::ReconciliationResult,
    types::{Price, Shares},
};

/// How the order presents at the venue this pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenuePresence {
    /// Positively identified (has a venue order id) and still resting.
    Resting,
    /// Positively identified and no longer on the venue (settled/removed).
    Settled,
    /// Could not be identified at the venue (e.g. a submit timeout with no
    /// venue order id). Unattributable orders resolve fail-closed.
    Unattributable,
}

/// Structured venue facts derived from the collected evidence. The sole input
/// to [`decide`]; built only when the venue was reachable this pass.
#[derive(Debug, Clone)]
pub struct ReconcileFacts {
    /// Shares the order requested.
    pub order_shares: Shares,
    /// How the order presents at the venue.
    pub presence: VenuePresence,
    /// Total venue-confirmed filled shares attributed to this order.
    pub filled_shares: Shares,
    /// Volume-weighted average fill price (`None` when nothing filled).
    pub avg_price: Option<Price>,
    /// Current conditional-token balance (absolute corroboration of holdings).
    pub token_balance: Shares,
    /// Order age exceeded the force-terminal staleness deadline.
    pub past_stale_deadline: bool,
    /// The order's GTD expiry has elapsed.
    pub gtd_expired: bool,
}

/// The verdict for one reconcilable order. `result == Pending` means no terminal
/// decision yet — the service applies no ledger correction and retries next pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationDecision {
    pub result: ReconciliationResult,
    pub filled_shares: Shares,
    pub avg_price: Option<Price>,
}

/// Decide the reconciliation verdict from venue facts (deterministic).
#[must_use]
pub fn decide(facts: &ReconcileFacts) -> ReconciliationDecision {
    let filled = facts.filled_shares;
    let result = if filled > facts.order_shares {
        // Venue reports more shares than ordered — impossible; never guess.
        ReconciliationResult::Unresolvable
    } else if filled.is_positive() && facts.token_balance < filled {
        // We believe shares filled, but the account does not hold them: a hard
        // contradiction between trades and the token balance.
        ReconciliationResult::Unresolvable
    } else {
        match facts.presence {
            // Unattributable (no venue id) or still resting: no terminal truth
            // yet. The service attempts a cancel before deciding when stale; if
            // we are still here past the deadline we cannot terminate safely —
            // freeze for an operator (fail-closed). Otherwise retry next sweep.
            VenuePresence::Unattributable | VenuePresence::Resting => {
                if facts.past_stale_deadline {
                    ReconciliationResult::Unresolvable
                } else {
                    ReconciliationResult::Pending
                }
            }
            // Settled at the venue: trades are the terminal fill truth.
            VenuePresence::Settled => {
                if filled == facts.order_shares {
                    ReconciliationResult::Filled
                } else if filled.is_positive() {
                    ReconciliationResult::PartiallyFilled
                } else if facts.gtd_expired {
                    ReconciliationResult::NotFilled
                } else {
                    ReconciliationResult::Cancelled
                }
            }
        }
    };

    ReconciliationDecision {
        result,
        filled_shares: filled,
        avg_price: facts.avg_price,
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;

    fn facts(filled: Decimal, order: Decimal) -> ReconcileFacts {
        ReconcileFacts {
            order_shares: Shares::new(order),
            presence: VenuePresence::Settled,
            filled_shares: Shares::new(filled),
            avg_price: Some(Price::new(dec!(0.6))),
            // Plenty of token balance so corroboration passes by default.
            token_balance: Shares::new(dec!(1000)),
            past_stale_deadline: false,
            gtd_expired: false,
        }
    }

    #[test]
    fn full_fill_is_filled() {
        assert_eq!(
            decide(&facts(dec!(100), dec!(100))).result,
            ReconciliationResult::Filled
        );
    }

    #[test]
    fn partial_terminal_is_partially_filled() {
        assert_eq!(
            decide(&facts(dec!(40), dec!(100))).result,
            ReconciliationResult::PartiallyFilled
        );
    }

    #[test]
    fn zero_fill_terminal_is_cancelled() {
        assert_eq!(
            decide(&facts(dec!(0), dec!(100))).result,
            ReconciliationResult::Cancelled
        );
    }

    #[test]
    fn zero_fill_gtd_expired_is_not_filled() {
        let mut f = facts(dec!(0), dec!(100));
        f.gtd_expired = true;
        assert_eq!(decide(&f).result, ReconciliationResult::NotFilled);
    }

    #[test]
    fn overfill_is_unresolvable() {
        assert_eq!(
            decide(&facts(dec!(150), dec!(100))).result,
            ReconciliationResult::Unresolvable
        );
    }

    #[test]
    fn fill_exceeding_token_balance_is_unresolvable() {
        let mut f = facts(dec!(100), dec!(100));
        f.token_balance = Shares::new(dec!(10));
        assert_eq!(decide(&f).result, ReconciliationResult::Unresolvable);
    }

    #[test]
    fn still_open_before_deadline_is_pending() {
        let mut f = facts(dec!(0), dec!(100));
        f.presence = VenuePresence::Resting;
        assert_eq!(decide(&f).result, ReconciliationResult::Pending);
    }

    #[test]
    fn still_open_past_deadline_is_unresolvable() {
        let mut f = facts(dec!(0), dec!(100));
        f.presence = VenuePresence::Resting;
        f.past_stale_deadline = true;
        assert_eq!(decide(&f).result, ReconciliationResult::Unresolvable);
    }

    #[test]
    fn unattributable_before_deadline_is_pending() {
        let mut f = facts(dec!(0), dec!(100));
        f.presence = VenuePresence::Unattributable;
        assert_eq!(decide(&f).result, ReconciliationResult::Pending);
    }

    #[test]
    fn unattributable_past_deadline_is_unresolvable() {
        let mut f = facts(dec!(0), dec!(100));
        f.presence = VenuePresence::Unattributable;
        f.past_stale_deadline = true;
        assert_eq!(decide(&f).result, ReconciliationResult::Unresolvable);
    }
}
