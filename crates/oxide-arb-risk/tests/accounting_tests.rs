//! Daily, weekly, and hourly accounting tests.
//!
//! Validates rollover semantics, budget tracking, and snapshot recovery.

use chrono::{Timelike, Utc};
use oxide_arb_models::enums::common::TradeOutcome;
use oxide_arb_models::types::Usd;
use oxide_arb_risk::accounting::{DailyAccounting, HourlyAccounting, WeeklyAccounting};
use oxide_arb_risk::types::PeriodStats;
use rust_decimal_macros::dec;

// ── Daily rollover ─────────────────────────────────────────────────────────

#[test]
fn daily_rollover_resets_stats() {
    let yesterday = Utc::now().date_naive() - chrono::Duration::days(1);
    let stats = PeriodStats {
        loss: Usd::new(dec!(10)),
        fees: Usd::new(dec!(2)),
        pnl: Usd::new(dec!(-10)),
        trade_count: 5,
        success_count: 2,
        miss_count: 3,
        max_single_loss: Usd::new(dec!(5)),
        max_single_profit: Usd::new(dec!(3)),
    };
    let budget = Usd::new(dec!(100));
    let spent = Usd::new(dec!(50));

    let mut daily = DailyAccounting::from_snapshot(yesterday, stats, budget, spent);
    assert_eq!(daily.budget_remaining(), Usd::new(dec!(50)));

    // Recording a trade today should trigger rollover
    let rolled = daily.record_trade(
        Usd::new(dec!(5)),
        Usd::new(dec!(0.5)),
        Usd::new(dec!(10)),
        TradeOutcome::Success,
    );

    assert!(rolled, "should rollover when window_start is in the past");
    // After rollover, stats were reset before the new trade was recorded
    assert_eq!(daily.stats().trade_count, 1);
    assert_eq!(daily.stats().success_count, 1);
    assert_eq!(daily.stats().miss_count, 0);
    assert_eq!(daily.window_start(), Utc::now().date_naive());
}

#[test]
fn daily_budget_resets_on_rollover() {
    let yesterday = Utc::now().date_naive() - chrono::Duration::days(1);
    let budget = Usd::new(dec!(100));
    let spent = Usd::new(dec!(90));

    let mut daily =
        DailyAccounting::from_snapshot(yesterday, PeriodStats::default(), budget, spent);
    assert_eq!(daily.budget_remaining(), Usd::new(dec!(10)));

    // Trigger rollover
    daily.record_trade(
        Usd::new(dec!(1)),
        Usd::ZERO,
        Usd::new(dec!(5)),
        TradeOutcome::Success,
    );

    // Budget should be reset to initial minus the new trade's cost
    assert_eq!(daily.budget_remaining(), Usd::new(dec!(95)));
}

// ── Weekly rollover ────────────────────────────────────────────────────────

#[test]
fn weekly_rollover_at_monday_boundary() {
    // Use a date from 2 weeks ago to guarantee rollover
    let two_weeks_ago = Utc::now().date_naive() - chrono::Duration::weeks(2);
    let stats = PeriodStats {
        loss: Usd::new(dec!(50)),
        trade_count: 10,
        ..PeriodStats::default()
    };
    let mut weekly = WeeklyAccounting::from_snapshot(two_weeks_ago, stats);

    let rolled = weekly.record_trade(
        Usd::new(dec!(3)),
        Usd::new(dec!(0.2)),
        TradeOutcome::Success,
    );

    assert!(rolled, "should rollover when week_start is old");
    assert_eq!(weekly.stats().trade_count, 1);
    assert_eq!(weekly.weekly_loss(), Usd::ZERO);
}

// ── No rollover within same day ────────────────────────────────────────────

#[test]
fn no_rollover_within_same_day() {
    let mut daily = DailyAccounting::new(Usd::new(dec!(100)));

    // Record first trade
    daily.record_trade(
        Usd::new(dec!(5)),
        Usd::new(dec!(0.5)),
        Usd::new(dec!(10)),
        TradeOutcome::Success,
    );

    // Record second trade — should NOT roll over
    let rolled = daily.record_trade(
        Usd::new(dec!(-3)),
        Usd::new(dec!(0.3)),
        Usd::new(dec!(8)),
        TradeOutcome::Miss,
    );

    assert!(!rolled);
    assert_eq!(daily.stats().trade_count, 2);
    assert_eq!(daily.stats().success_count, 1);
    assert_eq!(daily.stats().miss_count, 1);
    assert_eq!(daily.daily_pnl(), Usd::new(dec!(2))); // 5 + (-3)
    assert_eq!(daily.daily_loss(), Usd::new(dec!(3))); // abs(-3)
}

// ── from_snapshot restores correctly ───────────────────────────────────────

#[test]
fn from_snapshot_restores_correctly() {
    let today = Utc::now().date_naive();
    let stats = PeriodStats {
        loss: Usd::new(dec!(15)),
        fees: Usd::new(dec!(3)),
        pnl: Usd::new(dec!(-15)),
        trade_count: 4,
        success_count: 1,
        miss_count: 3,
        max_single_loss: Usd::new(dec!(8)),
        max_single_profit: Usd::new(dec!(2)),
    };
    let budget = Usd::new(dec!(200));
    let spent = Usd::new(dec!(60));

    let daily = DailyAccounting::from_snapshot(today, stats, budget, spent);

    assert_eq!(daily.window_start(), today);
    assert_eq!(daily.daily_loss(), Usd::new(dec!(15)));
    assert_eq!(daily.daily_pnl(), Usd::new(dec!(-15)));
    assert_eq!(daily.budget_remaining(), Usd::new(dec!(140)));
    assert_eq!(daily.budget_spent(), Usd::new(dec!(60)));
    assert_eq!(daily.stats().trade_count, 4);
    assert!(!daily.is_budget_exhausted());
}

#[test]
fn budget_exhausted_when_spent_exceeds_budget() {
    let today = Utc::now().date_naive();
    let budget = Usd::new(dec!(50));
    let spent = Usd::new(dec!(50));

    let daily = DailyAccounting::from_snapshot(today, PeriodStats::default(), budget, spent);

    assert!(daily.is_budget_exhausted());
    assert_eq!(daily.budget_remaining(), Usd::ZERO);
}

#[test]
fn weekly_from_snapshot_restores_correctly() {
    let today = Utc::now().date_naive();
    let stats = PeriodStats {
        loss: Usd::new(dec!(30)),
        trade_count: 8,
        success_count: 5,
        miss_count: 3,
        ..PeriodStats::default()
    };

    let weekly = WeeklyAccounting::from_snapshot(today, stats);

    assert_eq!(weekly.week_start(), today);
    assert_eq!(weekly.weekly_loss(), Usd::new(dec!(30)));
    assert_eq!(weekly.stats().trade_count, 8);
}

// ── Hourly rollover ───────────────────────────────────────────────────────

#[test]
fn hourly_from_snapshot_restores_loss_and_counts() {
    let now = Utc::now();
    let stats = PeriodStats {
        loss: Usd::new(dec!(12)),
        trade_count: 6,
        success_count: 4,
        miss_count: 2,
        ..PeriodStats::default()
    };

    let hourly = HourlyAccounting::from_snapshot(now.hour(), now.date_naive(), stats);

    assert_eq!(hourly.hourly_loss(), Usd::new(dec!(12)));
    assert_eq!(hourly.stats().trade_count, 6);
    assert_eq!(hourly.stats().success_count, 4);
    assert_eq!(hourly.stats().miss_count, 2);
}

#[test]
fn hourly_no_rollover_within_same_hour() {
    let mut hourly = HourlyAccounting::new();

    hourly.record_trade(Usd::new(dec!(-3)), Usd::new(dec!(0.1)), TradeOutcome::Miss);
    let rolled = hourly.record_trade(Usd::new(dec!(-2)), Usd::new(dec!(0.1)), TradeOutcome::Miss);

    assert!(!rolled);
    assert_eq!(hourly.stats().trade_count, 2);
    assert_eq!(hourly.hourly_loss(), Usd::new(dec!(5)));
}

#[test]
fn hourly_rollover_resets_on_stale_snapshot() {
    let yesterday = Utc::now().date_naive() - chrono::Duration::days(1);
    let stats = PeriodStats {
        loss: Usd::new(dec!(20)),
        trade_count: 10,
        success_count: 3,
        miss_count: 7,
        ..PeriodStats::default()
    };

    let mut hourly = HourlyAccounting::from_snapshot(23, yesterday, stats);

    let rolled = hourly.record_trade(Usd::new(dec!(-1)), Usd::new(dec!(0.1)), TradeOutcome::Miss);

    assert!(
        rolled,
        "should rollover when snapshot is from a different date/hour"
    );
    assert_eq!(hourly.stats().trade_count, 1);
    assert_eq!(hourly.hourly_loss(), Usd::new(dec!(1)));
}

#[test]
fn hourly_maybe_rollover_standalone() {
    let yesterday = Utc::now().date_naive() - chrono::Duration::days(1);
    let stats = PeriodStats {
        loss: Usd::new(dec!(15)),
        trade_count: 5,
        ..PeriodStats::default()
    };

    let mut hourly = HourlyAccounting::from_snapshot(0, yesterday, stats);

    let rolled = hourly.maybe_rollover();
    assert!(
        rolled,
        "standalone rollover should trigger for stale window"
    );
    assert_eq!(hourly.hourly_loss(), Usd::ZERO);
    assert_eq!(hourly.stats().trade_count, 0);
}
