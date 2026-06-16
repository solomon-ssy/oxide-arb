//! Analytics dashboard API contract.
//!
//! All windowed analytics endpoints share [`AnalyticsQuery`] → [`AnalyticsScope`]:
//!
//! - **Settlement series** (`/analytics/daily`) reads persisted daily reports whose
//!   calendar `date` falls in the inclusive UTC settlement range derived from the
//!   execution window — the same day boundaries used by [`ReportGenerator`].
//! - **Execution aggregates** (edge histogram, market performance) query the `trade`
//!   table over the half-open execution window `[from, to)` with an optional
//!   [`ExecutionMode`] filter so simulated fills never mix with live capital.

use crate::{
    domain::{
        TimeWindowQuery, WindowQueryError,
        query::{TimeWindow, TradeAnalyticsFilter},
        trade::DailyReport,
    },
    enums::common::ExecutionMode,
    types::{MarketId, Usd},
};
use chrono::{Days, NaiveDate, NaiveTime};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Inbound query shared by all windowed analytics endpoints.
#[derive(Debug, Default, Deserialize)]
pub struct AnalyticsQuery {
    #[serde(flatten)]
    pub window: TimeWindowQuery,
    /// When set, trade-derived aggregates include only this execution mode.
    /// Omitted = all modes (matches the report generator's trade rollup).
    pub execution_mode: Option<ExecutionMode>,
}

/// Resolved analytics scope — single source of truth for one dashboard refresh.
#[derive(Debug, Clone, Copy)]
pub struct AnalyticsScope {
    /// Half-open UTC execution window `[from, to)` — identical to
    /// `TradeRepository::aggregate_between` / report-generator day bounds.
    pub execution_window: TimeWindow,
    /// Inclusive UTC calendar dates for persisted daily settlement reports.
    pub settlement_from: NaiveDate,
    pub settlement_to: NaiveDate,
    pub execution_mode: Option<ExecutionMode>,
}

impl AnalyticsScope {
    /// Project the trade-table filter used by edge histogram and market performance.
    #[must_use]
    pub const fn trade_filter(self) -> TradeAnalyticsFilter {
        TradeAnalyticsFilter {
            window: self.execution_window,
            execution_mode: self.execution_mode,
        }
    }
}

impl AnalyticsQuery {
    /// Resolve and harden the inbound query into an [`AnalyticsScope`].
    pub fn resolve(
        &self,
        default_lookback: chrono::Duration,
        max_days: i64,
    ) -> Result<AnalyticsScope, WindowQueryError> {
        let execution_window = self.window.resolve(default_lookback, max_days)?;
        let (settlement_from, settlement_to) =
            settlement_dates_for_execution_window(execution_window);
        Ok(AnalyticsScope {
            execution_window,
            settlement_from,
            settlement_to,
            execution_mode: self.execution_mode,
        })
    }
}

/// Derive the inclusive UTC settlement calendar dates covered by a half-open
/// execution window. Matches report-generator semantics: a daily report for
/// date `D` covers `[start_of_day(D), start_of_day(D+1))`.
fn settlement_dates_for_execution_window(window: TimeWindow) -> (NaiveDate, NaiveDate) {
    let settlement_from = window.from.date_naive();
    let settlement_to = if window.to.time() == NaiveTime::MIN {
        window
            .to
            .date_naive()
            .checked_sub_days(Days::new(1))
            .unwrap_or_else(|| window.to.date_naive())
    } else {
        window.to.date_naive()
    };
    (settlement_from, settlement_to)
}

/// One day of the analytics settlement `PnL` series (`GET /analytics/daily`).
#[derive(Debug, Clone, Serialize)]
pub struct AnalyticsDailyPoint {
    pub date: NaiveDate,
    /// Settled realized `PnL` for this UTC calendar day (authoritative ledger).
    pub daily_pnl: Usd,
    /// Running sum of `daily_pnl` within the requested window (ascending).
    pub cumulative_pnl: Usd,
    pub trade_count: u32,
    pub success_count: u32,
}

/// Settlement-basis daily `PnL` series for charting.
#[derive(Debug, Clone, Serialize)]
pub struct AnalyticsDailySeries {
    pub points: Vec<AnalyticsDailyPoint>,
}

impl AnalyticsDailySeries {
    /// Build the chart-ready series from persisted daily reports (any input order).
    #[must_use]
    pub fn from_daily_reports(mut reports: Vec<DailyReport>) -> Self {
        reports.sort_by_key(|report| report.date);
        let mut cumulative = Usd::ZERO;
        let points = reports
            .into_iter()
            .map(|report| {
                cumulative += report.total_pnl;
                AnalyticsDailyPoint {
                    date: report.date,
                    daily_pnl: report.total_pnl,
                    cumulative_pnl: cumulative,
                    trade_count: report.trade_count,
                    success_count: report.success_count,
                }
            })
            .collect();
        Self { points }
    }
}

/// A single detected-edge histogram bucket over a trade-history window.
#[derive(Debug, Clone, Serialize)]
pub struct EdgeBucket {
    /// Stable bucket label (basis-point range), e.g. `"0-50"`.
    pub label: &'static str,
    /// Number of trades whose detected edge fell in this bucket.
    pub count: u64,
}

/// Per-market execution performance aggregate over a trade-history window.
#[derive(Debug, Clone, Serialize)]
pub struct MarketPerformanceRow {
    pub market_id: MarketId,
    pub trade_count: u64,
    pub success_count: u64,
    /// Sum of fill-level `net_profit_usd` (execution basis, not settlement ledger).
    pub net_profit_usd: Usd,
    pub total_cost_usd: Usd,
    /// Average detected edge in basis points for trades with an edge estimate.
    pub avg_edge_bps: Option<Decimal>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{ReportRiskSummary, ReportTradeStats, SettledPnlStats, trade::DailyReport},
        enums::report::ReportSchemaVersion,
        types::Usd,
    };
    use chrono::{TimeZone, Utc};
    use rust_decimal::Decimal;

    fn empty_daily(date: NaiveDate, pnl: i64) -> DailyReport {
        let pnl = Usd::new(Decimal::from(pnl));
        DailyReport {
            date,
            schema_version: ReportSchemaVersion::V1,
            generated_at: Utc::now(),
            period_start: date,
            period_end: date,
            settled_pnl: SettledPnlStats {
                realized_pnl: pnl,
                total_payout: Usd::ZERO,
                total_cost: Usd::ZERO,
                total_fees: Usd::ZERO,
                settled_position_count: 0,
                winning_position_count: 0,
                losing_position_count: 0,
                unsettled_position_count: 0,
                failed_accounting_count: 0,
                largest_single_profit: Usd::ZERO,
                largest_single_loss: Usd::ZERO,
                total_gas_paid: Usd::ZERO,
            },
            execution: ReportTradeStats {
                trade_count: 2,
                success_count: 1,
                miss_count: 0,
                failed_count: 0,
                total_fill_cost: Usd::ZERO,
                total_fill_fees: Usd::ZERO,
                fill_expected_pnl: Usd::ZERO,
            },
            risk: ReportRiskSummary {
                daily_pnl: pnl,
                daily_loss: Usd::ZERO,
                weekly_loss: Usd::ZERO,
                total_exposure: Usd::ZERO,
                open_position_count: 0,
            },
            total_pnl: pnl,
            total_fees_paid: Usd::ZERO,
            total_gas_paid: Usd::ZERO,
            trade_count: 2,
            success_count: 1,
            miss_count: 0,
            largest_single_loss: Usd::ZERO,
            largest_single_profit: Usd::ZERO,
        }
    }

    #[test]
    fn settlement_dates_use_half_open_midnight_end() {
        let from = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let to = Utc.with_ymd_and_hms(2026, 6, 8, 0, 0, 0).unwrap();
        let (start, end) = settlement_dates_for_execution_window(TimeWindow::new(from, to));
        assert_eq!(start, NaiveDate::from_ymd_opt(2026, 6, 1).unwrap());
        assert_eq!(end, NaiveDate::from_ymd_opt(2026, 6, 7).unwrap());
    }

    #[test]
    fn daily_series_accumulates_in_date_order() {
        let day = |d: u32| NaiveDate::from_ymd_opt(2026, 6, d).unwrap();
        let series = AnalyticsDailySeries::from_daily_reports(vec![
            empty_daily(day(3), 5),
            empty_daily(day(1), 10),
            empty_daily(day(2), -2),
        ]);
        assert_eq!(series.points.len(), 3);
        assert_eq!(series.points[0].date, day(1));
        assert_eq!(series.points[0].cumulative_pnl, Usd::new(Decimal::from(10)));
        assert_eq!(series.points[2].cumulative_pnl, Usd::new(Decimal::from(13)));
    }
}
