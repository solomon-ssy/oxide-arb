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

/// Directional current-lot basis for projecting newly observed cumulative
/// fills.
///
/// `previously_applied` is read from the order's reconciliation summary;
/// `lot_remaining` is the current per-intent lot after every committed entry or
/// exit delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareSettlementBasis {
    BuyReceipt {
        lot_remaining: Shares,
        previously_applied: Shares,
    },
    SellDebit {
        lot_remaining: Shares,
        previously_applied: Shares,
    },
}

/// Delta and expected per-intent lot state implied by one cumulative venue
/// observation. The repository recomputes the delta under its transaction lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareSettlementProjection {
    BuyReceipt {
        unapplied_receipt: Shares,
        expected_remaining: Shares,
    },
    SellDebit {
        unapplied_debit: Shares,
        expected_remaining: Shares,
    },
}

impl ShareSettlementBasis {
    fn project(self, cumulative: Shares) -> Option<ShareSettlementProjection> {
        let (lot_remaining, previously_applied) = match self {
            Self::BuyReceipt {
                lot_remaining,
                previously_applied,
            }
            | Self::SellDebit {
                lot_remaining,
                previously_applied,
            } => (lot_remaining, previously_applied),
        };
        if cumulative < previously_applied {
            return None;
        }
        let delta = cumulative - previously_applied;
        match self {
            Self::BuyReceipt { .. } => Some(ShareSettlementProjection::BuyReceipt {
                unapplied_receipt: delta,
                expected_remaining: lot_remaining + delta,
            }),
            Self::SellDebit { .. } if delta <= lot_remaining => {
                Some(ShareSettlementProjection::SellDebit {
                    unapplied_debit: delta,
                    expected_remaining: lot_remaining - delta,
                })
            }
            Self::SellDebit { .. } => None,
        }
    }
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
    /// Directional basis for applying only the newly observed fill delta.
    pub share_basis: ShareSettlementBasis,
    /// Current account-wide conditional-token balance. Diagnostic only: tokens
    /// are fungible across lots and later legitimate orders can change it.
    pub observed_token_balance: Option<Shares>,
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
    /// Whether the venue has removed the order and no further fill can occur.
    pub venue_terminal: bool,
    /// Whether a terminal removal was caused by the GTD deadline.
    pub expired: bool,
    /// Directional lot projection for a valid cumulative fill observation.
    pub share_projection: Option<ShareSettlementProjection>,
}

impl ReconcileFacts {
    /// Decide the reconciliation verdict from venue facts (deterministic).
    #[must_use]
    pub fn decide(&self) -> ReconciliationDecision {
        let filled = self.filled_shares;
        let share_projection = self.share_basis.project(filled);
        let result = if filled > self.order_shares || share_projection.is_none() {
            // Overfill, cumulative regression, or a SELL debit beyond the
            // current remaining lot is contradictory; never guess.
            ReconciliationResult::Unresolvable
        } else {
            match self.presence {
                // Unattributable (no venue id) or still resting: no terminal truth
                // yet. The service attempts a cancel before deciding when stale; if
                // we are still here past the deadline we cannot terminate safely —
                // freeze for an operator (fail-closed). Otherwise retry next sweep.
                VenuePresence::Unattributable => {
                    if self.past_stale_deadline {
                        ReconciliationResult::Unresolvable
                    } else {
                        ReconciliationResult::Pending
                    }
                }
                VenuePresence::Resting => {
                    if self.past_stale_deadline {
                        ReconciliationResult::Unresolvable
                    } else if filled.is_positive() {
                        ReconciliationResult::PartiallyFilled
                    } else {
                        ReconciliationResult::Pending
                    }
                }
                // Settled at the venue: trades are the terminal fill truth.
                VenuePresence::Settled => {
                    if filled == self.order_shares {
                        ReconciliationResult::Filled
                    } else if filled.is_positive() {
                        ReconciliationResult::PartiallyFilled
                    } else if self.gtd_expired {
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
            avg_price: self.avg_price,
            venue_terminal: self.presence == VenuePresence::Settled,
            expired: self.gtd_expired,
            share_projection,
        }
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
            share_basis: ShareSettlementBasis::BuyReceipt {
                lot_remaining: Shares::ZERO,
                previously_applied: Shares::ZERO,
            },
            observed_token_balance: Some(Shares::new(dec!(1000))),
            past_stale_deadline: false,
            gtd_expired: false,
        }
    }

    #[test]
    fn full_fill_is_filled() {
        assert_eq!(
            facts(dec!(100), dec!(100)).decide().result,
            ReconciliationResult::Filled
        );
    }

    #[test]
    fn partial_terminal_partially_filled() {
        assert_eq!(
            facts(dec!(40), dec!(100)).decide().result,
            ReconciliationResult::PartiallyFilled
        );
    }

    #[test]
    fn zero_fill_terminal_cancelled() {
        assert_eq!(
            facts(dec!(0), dec!(100)).decide().result,
            ReconciliationResult::Cancelled
        );
    }

    #[test]
    fn zero_fill_not_filled() {
        let mut f = facts(dec!(0), dec!(100));
        f.gtd_expired = true;
        assert_eq!(f.decide().result, ReconciliationResult::NotFilled);
    }

    #[test]
    fn overfill_is_unresolvable() {
        assert_eq!(
            facts(dec!(150), dec!(100)).decide().result,
            ReconciliationResult::Unresolvable
        );
    }

    #[test]
    fn balance_is_diagnostic_only() {
        let mut f = facts(dec!(100), dec!(100));
        f.observed_token_balance = Some(Shares::ZERO);
        assert_eq!(f.decide().result, ReconciliationResult::Filled);
        f.observed_token_balance = None;
        assert_eq!(f.decide().result, ReconciliationResult::Filled);
    }

    #[test]
    fn still_open_before_pending() {
        let mut f = facts(dec!(0), dec!(100));
        f.presence = VenuePresence::Resting;
        assert_eq!(f.decide().result, ReconciliationResult::Pending);
    }

    #[test]
    fn partial_is_actionable_progress() {
        let mut f = facts(dec!(40), dec!(100));
        f.presence = VenuePresence::Resting;
        let decision = f.decide();
        assert_eq!(decision.result, ReconciliationResult::PartiallyFilled);
        assert!(!decision.venue_terminal);
    }

    #[test]
    fn still_open_past_unresolvable() {
        let mut f = facts(dec!(0), dec!(100));
        f.presence = VenuePresence::Resting;
        f.past_stale_deadline = true;
        assert_eq!(f.decide().result, ReconciliationResult::Unresolvable);
    }

    #[test]
    fn unattributable_before_deadline_pending() {
        let mut f = facts(dec!(0), dec!(100));
        f.presence = VenuePresence::Unattributable;
        assert_eq!(f.decide().result, ReconciliationResult::Pending);
    }

    #[test]
    fn unattributable_past_deadline_unresolvable() {
        let mut f = facts(dec!(0), dec!(100));
        f.presence = VenuePresence::Unattributable;
        f.past_stale_deadline = true;
        assert_eq!(f.decide().result, ReconciliationResult::Unresolvable);
    }

    #[test]
    fn buy_receipt_replay_increment() {
        let mut replay = facts(dec!(40), dec!(100));
        replay.share_basis = ShareSettlementBasis::BuyReceipt {
            lot_remaining: Shares::new(dec!(40)),
            previously_applied: Shares::new(dec!(40)),
        };
        assert_eq!(
            replay.decide().share_projection,
            Some(ShareSettlementProjection::BuyReceipt {
                unapplied_receipt: Shares::ZERO,
                expected_remaining: Shares::new(dec!(40)),
            })
        );

        replay.filled_shares = Shares::new(dec!(70));
        assert_eq!(
            replay.decide().share_projection,
            Some(ShareSettlementProjection::BuyReceipt {
                unapplied_receipt: Shares::new(dec!(30)),
                expected_remaining: Shares::new(dec!(70)),
            })
        );
    }

    #[test]
    fn sell_debit_replay_matrix() {
        let mut full = facts(dec!(100), dec!(100));
        full.share_basis = ShareSettlementBasis::SellDebit {
            lot_remaining: Shares::new(dec!(100)),
            previously_applied: Shares::ZERO,
        };
        full.observed_token_balance = Some(Shares::ZERO);
        assert_eq!(full.decide().result, ReconciliationResult::Filled);
        assert_eq!(
            full.decide().share_projection,
            Some(ShareSettlementProjection::SellDebit {
                unapplied_debit: Shares::new(dec!(100)),
                expected_remaining: Shares::ZERO,
            })
        );

        let mut partial = facts(dec!(40), dec!(100));
        partial.share_basis = ShareSettlementBasis::SellDebit {
            lot_remaining: Shares::new(dec!(100)),
            previously_applied: Shares::ZERO,
        };
        assert_eq!(
            partial.decide().share_projection,
            Some(ShareSettlementProjection::SellDebit {
                unapplied_debit: Shares::new(dec!(40)),
                expected_remaining: Shares::new(dec!(60)),
            })
        );

        partial.share_basis = ShareSettlementBasis::SellDebit {
            lot_remaining: Shares::new(dec!(60)),
            previously_applied: Shares::new(dec!(40)),
        };
        assert_eq!(
            partial.decide().share_projection,
            Some(ShareSettlementProjection::SellDebit {
                unapplied_debit: Shares::ZERO,
                expected_remaining: Shares::new(dec!(60)),
            })
        );

        partial.filled_shares = Shares::new(dec!(70));
        assert_eq!(
            partial.decide().share_projection,
            Some(ShareSettlementProjection::SellDebit {
                unapplied_debit: Shares::new(dec!(30)),
                expected_remaining: Shares::new(dec!(30)),
            })
        );
    }

    #[test]
    fn settlement_regression_is_unresolvable() {
        let mut f = facts(dec!(30), dec!(100));
        f.share_basis = ShareSettlementBasis::BuyReceipt {
            lot_remaining: Shares::new(dec!(40)),
            previously_applied: Shares::new(dec!(40)),
        };
        assert_eq!(f.decide().result, ReconciliationResult::Unresolvable);

        f.filled_shares = Shares::new(dec!(70));
        f.share_basis = ShareSettlementBasis::SellDebit {
            lot_remaining: Shares::new(dec!(20)),
            previously_applied: Shares::new(dec!(40)),
        };
        assert_eq!(f.decide().result, ReconciliationResult::Unresolvable);
    }
}
