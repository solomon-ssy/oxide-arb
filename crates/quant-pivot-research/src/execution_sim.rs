//! Historical exit-fill simulation (Phase 06.1).
//!
//! [`ExitFillSimulator`] answers the counterfactual "what would selling this lot
//! into the book at time `t` realize?" used to build point-in-time hold-vs-exit
//! labels for the Sell scorer. It is the sell-side mirror of the entry
//! slippage ask-walk in `quant-pivot-core`'s admission check.
//!
//! Two fidelities, both fail-closed and deterministic:
//!
//! - [`ExitFillSimulator::simulate_l2`]: walk the resolved L2 bid ladder
//!   (best-first), consuming shares level by level for a true VWAP fill. Used
//!   when a PIT `book_snapshots` L2 is available (≤ 180d retention).
//! - [`ExitFillSimulator::simulate_fallback`]: a single-price fill at the best
//!   bid capped by an aggregate depth figure, for older lots where only
//!   `book_microstructure_1s` (best bid + depth aggregate) survives. Rows built
//!   from the fallback are tagged [`BookFidelity::MicrostructureFallback`] so the
//!   dataset coverage can down-weight or exclude them.
//!
//! The fee is charged as a fraction of gross proceeds (`fee_bps`), matching the
//! venue exit-fee convention; the caller supplies the governed rate.

use quant_pivot_models::{
    domain::market::book::BookLevel,
    types::{Bps, Price, Shares, Usd},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::precision::RESEARCH_DECIMAL_SCALE;

/// The book fidelity a simulated fill was produced from (audit + coverage gate).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BookFidelity {
    /// Full L2 bid ladder (best long-horizon fidelity within snapshot retention).
    L2,
    /// Best bid + aggregate depth only (`book_microstructure_1s` fallback).
    MicrostructureFallback,
}

/// The outcome of selling a target quantity into the bid side at one instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitFill {
    /// Volume-weighted average fill price across consumed bid levels.
    pub avg_price: Price,
    /// Shares actually fillable (≤ requested; capped by available bid depth).
    pub filled_shares: Shares,
    /// Gross proceeds before fee (`Σ level_price × consumed_shares`).
    pub gross_proceeds: Usd,
    /// Exit fee charged on the fill.
    pub fee: Usd,
    /// Net proceeds (`gross − fee`).
    pub net_proceeds: Usd,
    /// Whether the book could not fully fill the requested quantity.
    pub partial: bool,
    /// Fidelity of the book the fill was simulated against.
    pub fidelity: BookFidelity,
}

/// Simulates selling a lot into a resolved historical book, net of exit fee.
#[derive(Debug, Clone, Copy)]
pub struct ExitFillSimulator {
    fee_bps: Bps,
}

impl ExitFillSimulator {
    /// Build a simulator charging `fee_bps` on gross proceeds.
    #[must_use]
    pub const fn new(fee_bps: Bps) -> Self {
        Self { fee_bps }
    }

    /// The fee fraction (`fee_bps / 10_000`).
    fn fee_fraction(self) -> Decimal {
        self.fee_bps.inner() / Decimal::from(10_000)
    }

    /// Assemble the [`ExitFill`] from a completed walk's gross/filled totals.
    fn finish(
        self,
        gross: Decimal,
        filled: Decimal,
        requested: Shares,
        fidelity: BookFidelity,
    ) -> ExitFill {
        let avg_price = if filled > Decimal::ZERO {
            Price::new((gross / filled).round_dp(RESEARCH_DECIMAL_SCALE))
        } else {
            Price::ZERO
        };
        let gross_proceeds = Usd::new(gross.round_dp(RESEARCH_DECIMAL_SCALE));
        let fee = Usd::new((gross * self.fee_fraction()).round_dp(RESEARCH_DECIMAL_SCALE));
        let net_proceeds = Usd::new((gross_proceeds.inner() - fee.inner()).max(Decimal::ZERO));
        ExitFill {
            avg_price,
            filled_shares: Shares::new(filled),
            gross_proceeds,
            fee,
            net_proceeds,
            partial: filled < requested.inner(),
            fidelity,
        }
    }

    /// Walk the L2 bid ladder (best-first) selling `target` shares for a true
    /// VWAP fill. `bids` must be sorted best-first (descending price), the
    /// canonical [`BookSnapshotAt`](crate::pit::BookSnapshotAt) ordering.
    #[must_use]
    pub fn simulate_l2(self, bids: &[BookLevel], target: Shares) -> ExitFill {
        let mut remaining = target.inner();
        let mut gross = Decimal::ZERO;
        let mut filled = Decimal::ZERO;
        for level in bids {
            if remaining <= Decimal::ZERO {
                break;
            }
            let available = level.size_decimal().inner();
            if available <= Decimal::ZERO {
                continue;
            }
            let take = available.min(remaining);
            gross += take * level.price_decimal().inner();
            filled += take;
            remaining -= take;
        }
        self.finish(gross, filled, target, BookFidelity::L2)
    }

    /// Single-price fill at `best_bid` capped by `available_depth`, for lots
    /// whose L2 ladder is no longer retained. Degraded fidelity: a large lot is
    /// filled at one price rather than walking down the (unknown) ladder, so it
    /// under-estimates slippage — flagged [`BookFidelity::MicrostructureFallback`].
    #[must_use]
    pub fn simulate_fallback(
        self,
        best_bid: Price,
        available_depth: Shares,
        target: Shares,
    ) -> ExitFill {
        let filled = available_depth
            .inner()
            .min(target.inner())
            .max(Decimal::ZERO);
        let gross = filled * best_bid.inner();
        self.finish(gross, filled, target, BookFidelity::MicrostructureFallback)
    }
}

#[cfg(test)]
mod tests {
    use super::{BookFidelity, ExitFillSimulator};
    use quant_pivot_models::{
        domain::market::book::BookLevel,
        types::{Bps, Price, Shares},
    };
    use rust_decimal_macros::dec;

    fn level(price: rust_decimal::Decimal, size: rust_decimal::Decimal) -> BookLevel {
        BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(size))
    }

    #[test]
    fn l2_walk_computes_vwap_and_partial() {
        let sim = ExitFillSimulator::new(Bps::new(dec!(0)));
        let bids = [level(dec!(0.90), dec!(10)), level(dec!(0.80), dec!(10))];
        // Sell 15: 10 @ 0.90 + 5 @ 0.80 = 9.0 + 4.0 = 13.0 gross, VWAP 13/15.
        let fill = sim.simulate_l2(&bids, Shares::new(dec!(15)));
        assert_eq!(fill.filled_shares, Shares::new(dec!(15)));
        assert_eq!(fill.gross_proceeds.inner(), dec!(13.0));
        assert!(!fill.partial);
        assert_eq!(fill.fidelity, BookFidelity::L2);

        // Sell 25 into 20 depth → partial fill of 20.
        let partial = sim.simulate_l2(&bids, Shares::new(dec!(25)));
        assert_eq!(partial.filled_shares, Shares::new(dec!(20)));
        assert!(partial.partial);
    }

    #[test]
    fn fee_reduces_net_proceeds() {
        let sim = ExitFillSimulator::new(Bps::new(dec!(100))); // 1%
        let bids = [level(dec!(0.50), dec!(100))];
        let fill = sim.simulate_l2(&bids, Shares::new(dec!(100)));
        assert_eq!(fill.gross_proceeds.inner(), dec!(50.0));
        assert_eq!(fill.fee.inner(), dec!(0.5));
        assert_eq!(fill.net_proceeds.inner(), dec!(49.5));
    }

    #[test]
    fn fallback_fills_flat_at_best_bid() {
        let sim = ExitFillSimulator::new(Bps::new(dec!(0)));
        let fill = sim.simulate_fallback(
            Price::new(dec!(0.60)),
            Shares::new(dec!(50)),
            Shares::new(dec!(80)),
        );
        assert_eq!(fill.filled_shares, Shares::new(dec!(50)));
        assert_eq!(fill.gross_proceeds.inner(), dec!(30.0));
        assert!(fill.partial);
        assert_eq!(fill.fidelity, BookFidelity::MicrostructureFallback);
    }
}
