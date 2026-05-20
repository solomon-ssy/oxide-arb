//! Profit and loss domain models.

use crate::types::Usd;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

/// Daily P&L summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyPnl {
    pub date: NaiveDate,
    pub realized_pnl: Usd,
    pub unrealized_pnl: Usd,
    pub fees_paid: Usd,
    pub gas_paid: Usd,
    pub net_pnl: Usd,
    pub trade_count: u32,
    pub win_count: u32,
    pub loss_count: u32,
}

/// Weekly P&L summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeeklyPnl {
    /// Monday of the week.
    pub week_start: NaiveDate,
    pub realized_pnl: Usd,
    pub unrealized_pnl: Usd,
    pub fees_paid: Usd,
    pub gas_paid: Usd,
    pub net_pnl: Usd,
    pub trade_count: u32,
    pub win_rate: rust_decimal::Decimal,
}

/// High-level cash flow summary for treasury monitoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CashFlowSummary {
    pub total_deposited: Usd,
    pub total_withdrawn: Usd,
    pub current_balance: Usd,
    pub locked_in_positions: Usd,
    pub locked_in_reservations: Usd,
    pub available_for_trading: Usd,
}
