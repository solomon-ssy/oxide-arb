//! Weekly accounting with automatic period rollover at Monday UTC boundary.

use crate::types::PeriodStats;
use chrono::{Datelike, NaiveDate, Utc};
use oxide_arb_models::enums::common::TradeOutcome;
use oxide_arb_models::types::Usd;

pub struct WeeklyAccounting {
    week_start: NaiveDate,
    stats: PeriodStats,
}

impl WeeklyAccounting {
    #[must_use]
    pub fn new() -> Self {
        Self {
            week_start: Self::current_monday(),
            stats: PeriodStats::default(),
        }
    }

    #[must_use]
    pub const fn from_snapshot(week_start: NaiveDate, stats: PeriodStats) -> Self {
        Self { week_start, stats }
    }

    pub fn record_trade(&mut self, net_profit: Usd, fees: Usd, outcome: TradeOutcome) -> bool {
        let rolled = self.maybe_rollover();
        self.stats.trade_count += 1;
        self.stats.pnl += net_profit;
        self.stats.fees += fees;
        match outcome {
            TradeOutcome::Success => self.stats.success_count += 1,
            TradeOutcome::Miss => self.stats.miss_count += 1,
            _ => {}
        }
        if net_profit.is_negative() {
            self.stats.loss += net_profit.abs();
        }
        rolled
    }

    #[must_use]
    pub const fn weekly_loss(&self) -> Usd {
        self.stats.loss
    }

    #[must_use]
    pub const fn stats(&self) -> &PeriodStats {
        &self.stats
    }

    #[must_use]
    pub const fn week_start(&self) -> NaiveDate {
        self.week_start
    }

    pub fn maybe_rollover(&mut self) -> bool {
        let monday = Self::current_monday();
        if monday > self.week_start {
            tracing::info!(
                previous = %self.week_start,
                new = %monday,
                final_pnl = %self.stats.pnl,
                "weekly accounting rollover"
            );
            self.week_start = monday;
            self.stats = PeriodStats::default();
            true
        } else {
            false
        }
    }

    fn current_monday() -> NaiveDate {
        let today = Utc::now().date_naive();
        let weekday = today.weekday().num_days_from_monday();
        today - chrono::Duration::days(i64::from(weekday))
    }
}

impl Default for WeeklyAccounting {
    fn default() -> Self {
        Self::new()
    }
}
