//! Drawdown guard tests.
//!
//! Validates equity tracking, drawdown percentage computation, sizing factor,
//! and halt vs. reduce actions at various drawdown levels.

use oxide_arb_models::types::Usd;
use oxide_arb_risk::sizing::DrawdownGuard;
use oxide_arb_risk::types::DrawdownAction;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

// ── No drawdown ─────────────────────────────────────────────────────────────

#[test]
fn no_drawdown_returns_normal() {
    let guard = DrawdownGuard::new(
        Usd::new(dec!(1000)),
        dec!(10),  // max_drawdown_pct
        dec!(0.5), // reduction_factor
    );

    let (dd_pct, action) = guard.evaluate(Usd::new(dec!(1000)));
    assert_eq!(action, DrawdownAction::Normal);
    assert_eq!(dd_pct, Decimal::ZERO);
    assert_eq!(guard.sizing_factor(Usd::new(dec!(1000))), Decimal::ONE);
}

// ── Equity above HWM updates ───────────────────────────────────────────────

#[test]
fn equity_above_hwm_updates_hwm() {
    let mut guard = DrawdownGuard::new(Usd::new(dec!(1000)), dec!(10), dec!(0.5));

    assert_eq!(guard.hwm(), Usd::new(dec!(1000)));

    guard.update_equity(Usd::new(dec!(1200)));
    assert_eq!(guard.hwm(), Usd::new(dec!(1200)));

    // Below HWM should not update
    guard.update_equity(Usd::new(dec!(1100)));
    assert_eq!(guard.hwm(), Usd::new(dec!(1200)));
}

// ── Drawdown below max reduces ─────────────────────────────────────────────

#[test]
fn drawdown_below_max_reduces_sizing() {
    let guard = DrawdownGuard::new(
        Usd::new(dec!(1000)),
        dec!(10),  // 10% max drawdown
        dec!(0.5), // reduction factor
    );

    // 5% drawdown: equity = 950 (50/1000 = 5%)
    let equity = Usd::new(dec!(950));
    let (dd_pct, action) = guard.evaluate(equity);

    assert_eq!(action, DrawdownAction::Reduce);
    assert_eq!(dd_pct, dec!(5));

    // sizing_factor = 1.0 - (dd_pct/max_dd) * (1 - reduction_factor)
    // = 1.0 - (5/10) * (1 - 0.5) = 1.0 - 0.5 * 0.5 = 0.75
    let factor = guard.sizing_factor(equity);
    assert_eq!(factor, dec!(0.75));
}

// ── Drawdown at max halts ──────────────────────────────────────────────────

#[test]
fn drawdown_at_max_halts() {
    let guard = DrawdownGuard::new(
        Usd::new(dec!(1000)),
        dec!(10), // 10% max drawdown
        dec!(0.5),
    );

    // Exactly 10% drawdown: equity = 900
    let equity = Usd::new(dec!(900));
    let (dd_pct, action) = guard.evaluate(equity);

    assert_eq!(action, DrawdownAction::Halt);
    assert_eq!(dd_pct, dec!(10));
    assert_eq!(guard.sizing_factor(equity), Decimal::ZERO);
}

// ── Drawdown beyond max halts ──────────────────────────────────────────────

#[test]
fn drawdown_beyond_max_halts() {
    let guard = DrawdownGuard::new(Usd::new(dec!(1000)), dec!(10), dec!(0.5));

    // 15% drawdown: equity = 850
    let equity = Usd::new(dec!(850));
    let (dd_pct, action) = guard.evaluate(equity);

    assert_eq!(action, DrawdownAction::Halt);
    assert_eq!(dd_pct, dec!(15));
    assert_eq!(guard.sizing_factor(equity), Decimal::ZERO);
}

// ── Sizing factor zero on halt ─────────────────────────────────────────────

#[test]
fn sizing_factor_zero_on_halt() {
    let guard = DrawdownGuard::new(Usd::new(dec!(5000)), dec!(10), dec!(0.5));

    // 10% drawdown from 5000 → equity = 4500
    let equity = Usd::new(dec!(4500));
    let factor = guard.sizing_factor(equity);
    assert_eq!(factor, Decimal::ZERO);
}

// ── Zero HWM returns Normal ────────────────────────────────────────────────

#[test]
fn zero_hwm_returns_normal() {
    let guard = DrawdownGuard::new(Usd::ZERO, dec!(10), dec!(0.5));

    let (dd_pct, action) = guard.evaluate(Usd::ZERO);
    assert_eq!(action, DrawdownAction::Normal);
    assert_eq!(dd_pct, Decimal::ZERO);
}

// ── from_snapshot preserves hwm ────────────────────────────────────────────

#[test]
fn from_snapshot_preserves_hwm() {
    let guard = DrawdownGuard::from_snapshot(Usd::new(dec!(2000)), dec!(10), dec!(0.5));

    assert_eq!(guard.hwm(), Usd::new(dec!(2000)));

    let (_, action) = guard.evaluate(Usd::new(dec!(1800)));
    assert_eq!(action, DrawdownAction::Halt); // 10% = halts
}
