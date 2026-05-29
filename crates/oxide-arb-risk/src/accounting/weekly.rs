//! Weekly accounting with automatic period rollover at Monday UTC boundary.

use crate::{clock::Clock, types::PeriodStats};
use chrono::{Datelike, NaiveDate};
use oxide_arb_models::{enums::common::TradeBusinessOutcome, types::Usd};
use std::sync::Arc;

pub struct WeeklyAccounting {
    week_start: NaiveDate,
    stats: PeriodStats,
    clock: Arc<dyn Clock>,
}

impl WeeklyAccounting {
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        let week_start = Self::current_monday_from(&*clock);
        Self {
            week_start,
            stats: PeriodStats::default(),
            clock,
        }
    }

    #[must_use]
    pub fn from_snapshot(week_start: NaiveDate, stats: PeriodStats, clock: Arc<dyn Clock>) -> Self {
        Self {
            week_start,
            stats,
            clock,
        }
    }

    pub fn record_trade(
        &mut self,
        net_profit: Usd,
        fees: Usd,
        outcome: TradeBusinessOutcome,
    ) -> bool {
        let rolled = self.maybe_rollover();
        self.stats.trade_count += 1;
        self.stats.pnl += net_profit;
        self.stats.fees += fees;
        match outcome {
            TradeBusinessOutcome::Success => self.stats.success_count += 1,
            TradeBusinessOutcome::Miss => self.stats.miss_count += 1,
            TradeBusinessOutcome::Failed => {}
        }
        if net_profit.is_negative() {
            self.stats.loss += net_profit.abs();
        }
        rolled
    }

    #[must_use]
    #[inline]
    pub const fn weekly_loss(&self) -> Usd {
        self.stats.loss
    }

    #[must_use]
    #[inline]
    pub const fn stats(&self) -> &PeriodStats {
        &self.stats
    }

    #[must_use]
    #[inline]
    pub const fn week_start(&self) -> NaiveDate {
        self.week_start
    }

    pub fn maybe_rollover(&mut self) -> bool {
        let monday = self.current_monday();
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

    fn current_monday(&self) -> NaiveDate {
        Self::current_monday_from(&*self.clock)
    }

    fn current_monday_from(clock: &dyn Clock) -> NaiveDate {
        let today = clock.today();
        let weekday = today.weekday().num_days_from_monday();
        today - chrono::Duration::days(i64::from(weekday))
    }
}
