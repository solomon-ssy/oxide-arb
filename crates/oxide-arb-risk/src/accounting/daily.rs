//! Daily accounting with automatic period rollover.

use crate::{clock::Clock, types::PeriodStats};
use chrono::NaiveDate;
use oxide_arb_models::{enums::common::TradeBusinessOutcome, types::Usd};
use std::sync::Arc;

#[derive(Clone)]
pub struct DailyAccounting {
    window_start: NaiveDate,
    stats: PeriodStats,
    budget_remaining: Usd,
    initial_budget: Usd,
    clock: Arc<dyn Clock>,
}

impl DailyAccounting {
    #[must_use]
    pub fn new(budget: Usd, clock: Arc<dyn Clock>) -> Self {
        Self {
            window_start: clock.today(),
            stats: PeriodStats::default(),
            budget_remaining: budget,
            initial_budget: budget,
            clock,
        }
    }

    #[must_use]
    pub fn from_snapshot(
        window_start: NaiveDate,
        stats: PeriodStats,
        budget: Usd,
        spent: Usd,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            window_start,
            stats,
            budget_remaining: budget - spent,
            initial_budget: budget,
            clock,
        }
    }

    /// Hot-reload the daily budget (runtime-config activation).
    ///
    /// The amount already spent today is preserved: the remaining budget is
    /// adjusted by the delta between the new and old budget (floored at zero
    /// when the new budget is below today's spend — fail-closed).
    pub fn set_budget(&mut self, budget: Usd) {
        let spent = self.budget_spent();
        self.initial_budget = budget;
        self.budget_remaining = (budget - spent).max(Usd::ZERO);
    }

    pub fn record_trade(
        &mut self,
        net_profit: Usd,
        fees: Usd,
        cost: Usd,
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
        self.budget_remaining -= cost;
        rolled
    }

    #[must_use]
    #[inline]
    pub const fn daily_loss(&self) -> Usd {
        self.stats.loss
    }

    #[must_use]
    #[inline]
    pub const fn daily_pnl(&self) -> Usd {
        self.stats.pnl
    }

    #[must_use]
    #[inline]
    pub const fn fees(&self) -> Usd {
        self.stats.fees
    }

    #[must_use]
    #[inline]
    pub const fn budget_remaining(&self) -> Usd {
        self.budget_remaining
    }

    #[must_use]
    #[inline]
    pub fn is_budget_exhausted(&self) -> bool {
        self.budget_remaining <= Usd::ZERO
    }

    #[must_use]
    #[inline]
    pub const fn stats(&self) -> &PeriodStats {
        &self.stats
    }

    #[must_use]
    #[inline]
    pub const fn window_start(&self) -> NaiveDate {
        self.window_start
    }

    #[must_use]
    #[inline]
    pub fn budget_spent(&self) -> Usd {
        self.initial_budget - self.budget_remaining
    }

    pub fn maybe_rollover(&mut self) -> bool {
        let today = self.clock.today();
        if today > self.window_start {
            tracing::info!(
                previous = %self.window_start,
                new = %today,
                final_pnl = %self.stats.pnl,
                "daily accounting rollover"
            );
            self.window_start = today;
            self.stats = PeriodStats::default();
            self.budget_remaining = self.initial_budget;
            true
        } else {
            false
        }
    }
}
