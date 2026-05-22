//! Hourly rolling-window loss accounting.

use crate::types::PeriodStats;
use chrono::{NaiveDate, Timelike, Utc};
use oxide_arb_models::enums::common::TradeOutcome;
use oxide_arb_models::types::Usd;

pub struct HourlyAccounting {
    window_start_hour: u32,
    window_start_date: NaiveDate,
    stats: PeriodStats,
}

impl HourlyAccounting {
    #[must_use]
    pub fn new() -> Self {
        let now = Utc::now();
        Self {
            window_start_hour: now.hour(),
            window_start_date: now.date_naive(),
            stats: PeriodStats::default(),
        }
    }

    #[must_use]
    pub const fn from_snapshot(hour: u32, date: NaiveDate, stats: PeriodStats) -> Self {
        Self {
            window_start_hour: hour,
            window_start_date: date,
            stats,
        }
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
            let abs_loss = net_profit.abs();
            self.stats.loss += abs_loss;
            self.stats.max_single_loss = self.stats.max_single_loss.max(abs_loss);
        } else {
            self.stats.max_single_profit = self.stats.max_single_profit.max(net_profit);
        }
        rolled
    }

    #[must_use]
    pub const fn hourly_loss(&self) -> Usd {
        self.stats.loss
    }

    #[must_use]
    pub const fn stats(&self) -> &PeriodStats {
        &self.stats
    }

    pub fn maybe_rollover(&mut self) -> bool {
        if self.should_rollover() {
            let now = Utc::now();
            let new_hour = now.hour();
            let new_date = now.date_naive();
            tracing::info!(
                previous_hour = self.window_start_hour,
                new_hour,
                previous_date = %self.window_start_date,
                new_date = %new_date,
                final_pnl = %self.stats.pnl,
                "hourly accounting rollover"
            );
            self.window_start_hour = new_hour;
            self.window_start_date = new_date;
            self.stats = PeriodStats::default();
            true
        } else {
            false
        }
    }

    fn should_rollover(&self) -> bool {
        let now = Utc::now();
        now.date_naive() != self.window_start_date || now.hour() != self.window_start_hour
    }
}

impl Default for HourlyAccounting {
    fn default() -> Self {
        Self::new()
    }
}
