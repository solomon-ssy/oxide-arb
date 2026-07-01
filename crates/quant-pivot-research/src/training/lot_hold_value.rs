//! Terminal hold-value oracle for hold-vs-exit labels (Phase 06.1).
//!
//! For a closed/settled lot with full ledger visibility, computes the net cash
//! a holder of `remaining_shares@t` would realize by holding through the lot's
//! actual terminal outcome (exits + settlement), net of fees already embedded in
//! the recorded proceeds.

use chrono::{DateTime, Utc};
use quant_pivot_models::types::{Shares, Usd};
use rust_decimal::Decimal;

use crate::precision::RESEARCH_DECIMAL_SCALE;

/// One realized exit slice on a lot timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LotExitEvent {
    /// When the exit fill settled.
    pub at: DateTime<Utc>,
    /// Shares sold in this exit.
    pub shares: Shares,
    /// Net proceeds from this exit (after fee).
    pub net_proceeds: Usd,
}

/// Frozen terminal snapshot for a closed lot (full-information hold oracle).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LotTerminalSnapshot {
    /// Entry-filled share quantity (opportunistic denominator).
    pub entry_shares: Shares,
    /// When the lot opened.
    pub opened_at: DateTime<Utc>,
    /// Economic close / settlement time.
    pub closed_at: DateTime<Utc>,
    /// Total net cash realized for the full lot (all exits + settlement).
    pub total_net_proceeds: Usd,
    /// Exit fills in ascending time order.
    pub exit_events: Vec<LotExitEvent>,
}

/// Shares still open strictly after `as_of` (before any exit at/after `as_of`).
#[must_use]
pub fn remaining_shares_at(snapshot: &LotTerminalSnapshot, as_of: DateTime<Utc>) -> Shares {
    let mut sold = Decimal::ZERO;
    for event in &snapshot.exit_events {
        if event.at < as_of {
            sold += event.shares.inner();
        }
    }
    Shares::new((snapshot.entry_shares.inner() - sold).max(Decimal::ZERO))
}

/// Net proceeds already realized strictly before `as_of`.
#[must_use]
pub fn proceeds_before(snapshot: &LotTerminalSnapshot, as_of: DateTime<Utc>) -> Usd {
    let gross = snapshot
        .exit_events
        .iter()
        .filter(|event| event.at < as_of)
        .map(|event| event.net_proceeds.inner())
        .sum::<Decimal>();
    Usd::new(gross.round_dp(RESEARCH_DECIMAL_SCALE))
}

/// Net cash from holding `remaining_shares@as_of` through the lot terminal.
///
/// Equals total lot proceeds minus cash already received before `as_of`.
#[must_use]
pub fn hold_terminal_proceeds(snapshot: &LotTerminalSnapshot, as_of: DateTime<Utc>) -> Usd {
    if as_of >= snapshot.closed_at {
        return Usd::ZERO;
    }
    let remaining = remaining_shares_at(snapshot, as_of);
    if !remaining.is_positive() {
        return Usd::ZERO;
    }
    let future = (snapshot.total_net_proceeds.inner() - proceeds_before(snapshot, as_of).inner())
        .max(Decimal::ZERO);
    Usd::new(future.round_dp(RESEARCH_DECIMAL_SCALE))
}

#[cfg(test)]
mod tests {
    use super::{LotExitEvent, LotTerminalSnapshot, hold_terminal_proceeds, remaining_shares_at};
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::types::{Shares, Usd};
    use rust_decimal_macros::dec;

    fn ts(secs: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(1_000_000 + secs, 0).single().expect("ts")
    }

    #[test]
    fn hold_terminal_is_total_minus_realized_before() {
        let snapshot = LotTerminalSnapshot {
            entry_shares: Shares::new(dec!(100)),
            opened_at: ts(0),
            closed_at: ts(1000),
            total_net_proceeds: Usd::new(dec!(80)),
            exit_events: vec![LotExitEvent {
                at: ts(500),
                shares: Shares::new(dec!(40)),
                net_proceeds: Usd::new(dec!(20)),
            }],
        };
        assert_eq!(
            remaining_shares_at(&snapshot, ts(100)),
            Shares::new(dec!(100))
        );
        assert_eq!(
            remaining_shares_at(&snapshot, ts(600)),
            Shares::new(dec!(60))
        );
        assert_eq!(hold_terminal_proceeds(&snapshot, ts(100)).inner(), dec!(80));
        assert_eq!(hold_terminal_proceeds(&snapshot, ts(600)).inner(), dec!(60));
    }
}
