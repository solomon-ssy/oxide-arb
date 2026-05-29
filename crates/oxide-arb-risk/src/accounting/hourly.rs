//! Hourly rolling-window loss accounting.

use crate::{clock::Clock, types::PeriodStats};
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use oxide_arb_models::{enums::common::TradeBusinessOutcome, types::Usd};
use std::sync::Arc;

#[derive(Clone)]
pub struct HourlyAccounting {
    window_start_hour: u32,
    window_start_date: NaiveDate,
    stats: PeriodStats,
    clock: Arc<dyn Clock>,
}

impl HourlyAccounting {
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        let now = clock.now();
        Self {
            window_start_hour: now.hour(),
            window_start_date: now.date_naive(),
            stats: PeriodStats::default(),
            clock,
        }
    }

    #[must_use]
    pub fn from_snapshot(
        hour: u32,
        date: NaiveDate,
        stats: PeriodStats,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            window_start_hour: hour,
            window_start_date: date,
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
            let abs_loss = net_profit.abs();
            self.stats.loss += abs_loss;
            self.stats.max_single_loss = self.stats.max_single_loss.max(abs_loss);
        } else {
            self.stats.max_single_profit = self.stats.max_single_profit.max(net_profit);
        }
        rolled
    }

    #[must_use]
    #[inline]
    pub const fn hourly_loss(&self) -> Usd {
        self.stats.loss
    }

    #[must_use]
    #[inline]
    pub const fn fees(&self) -> Usd {
        self.stats.fees
    }

    #[must_use]
    #[inline]
    pub const fn stats(&self) -> &PeriodStats {
        &self.stats
    }

    pub fn maybe_rollover(&mut self) -> bool {
        if self.should_rollover() {
            let now = self.clock.now();
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

    #[inline]
    pub const fn window_start(&self) -> DateTime<Utc> {
        self.window_start_date
            .and_hms_opt(self.window_start_hour, 0, 0)
            .unwrap()
            .and_utc()
    }

    fn should_rollover(&self) -> bool {
        let now = self.clock.now();
        now.date_naive() != self.window_start_date || now.hour() != self.window_start_hour
    }
}
