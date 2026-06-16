//! `PnL` API contract (outbound live snapshot view + daily history series).

use crate::{
    domain::{DailyReport, RiskEngineState},
    types::Usd,
};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

/// Live in-memory `PnL` snapshot, projected from the risk-engine state so the
/// wire contract is decoupled from the engine internals.
///
/// `daily_pnl` is the current trading day's realized `PnL`; `total_realized_pnl`
/// is the lifetime cumulative realized `PnL` on the same accounting basis, so a
/// `sync` snapshot agrees with the `pnl.update` push.
#[derive(Debug, Clone, Serialize)]
pub struct LivePnlView {
    pub daily_pnl: Usd,
    pub daily_loss_usd: Usd,
    pub total_realized_pnl: Usd,
    pub total_exposure: Usd,
}

impl From<&RiskEngineState> for LivePnlView {
    fn from(state: &RiskEngineState) -> Self {
        Self {
            daily_pnl: state.daily_pnl,
            daily_loss_usd: state.daily_loss_usd,
            total_realized_pnl: state.total_realized_pnl,
            total_exposure: state.total_exposure,
        }
    }
}

/// Inbound query for `GET /pnl/daily-series`.
#[derive(Debug, Default, Deserialize)]
pub struct DailyPnlSeriesQuery {
    /// Look-back length in days (default 7, max 90).
    pub days: Option<u32>,
}

/// Why a [`DailyPnlSeriesQuery`] failed to resolve: `days` was zero or
/// exceeded the endpoint's maximum look-back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DailySeriesRangeError {
    /// Maximum permitted look-back (days).
    pub max_days: u32,
}

impl fmt::Display for DailySeriesRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`days` must be between 1 and {}", self.max_days)
    }
}

impl Error for DailySeriesRangeError {}

impl DailyPnlSeriesQuery {
    /// Default look-back when `days` is unspecified.
    pub const DEFAULT_DAYS: u32 = 7;
    /// Maximum permitted look-back, aligned with the analytics window cap.
    pub const MAX_DAYS: u32 = 90;

    /// Resolve to a validated day count (`1..=MAX_DAYS`, default
    /// [`Self::DEFAULT_DAYS`]). Returns a domain error — never a web error —
    /// mapped to HTTP 400 web-side.
    pub fn resolve_days(&self) -> Result<u32, DailySeriesRangeError> {
        let days = self.days.unwrap_or(Self::DEFAULT_DAYS);
        if days == 0 || days > Self::MAX_DAYS {
            return Err(DailySeriesRangeError {
                max_days: Self::MAX_DAYS,
            });
        }
        Ok(days)
    }
}

/// One day of the `PnL` history series.
#[derive(Debug, Clone, Serialize)]
pub struct DailyPnlSeriesPoint {
    pub date: NaiveDate,
    /// Cumulative realized `PnL` within the series window (running sum of
    /// `daily_pnl`, ascending by date).
    pub total_pnl: Usd,
    /// Realized `PnL` settled on this day (the daily report's `total_pnl`).
    pub daily_pnl: Usd,
}

/// Outbound daily `PnL` series, ascending by date.
///
/// An empty window serializes as `{ "points": [] }` so the dashboard renders
/// an empty chart state instead of handling a 404.
#[derive(Debug, Clone, Serialize)]
pub struct DailyPnlSeries {
    pub points: Vec<DailyPnlSeriesPoint>,
}

impl DailyPnlSeries {
    /// Build the series from per-day settlement reports (any input order):
    /// sorts ascending by date and accumulates the running window total.
    #[must_use]
    pub fn from_daily_reports(mut reports: Vec<DailyReport>) -> Self {
        reports.sort_by_key(|report| report.date);
        let mut running = Usd::ZERO;
        let points = reports
            .into_iter()
            .map(|report| {
                running += report.total_pnl;
                DailyPnlSeriesPoint {
                    date: report.date,
                    total_pnl: running,
                    daily_pnl: report.total_pnl,
                }
            })
            .collect();
        Self { points }
    }
}

#[cfg(test)]
mod tests {
    use super::{DailyPnlSeries, DailyPnlSeriesQuery};
    use crate::{
        domain::{DailyReport, ReportRiskSummary, ReportTradeStats, SettledPnlStats},
        enums::report::ReportSchemaVersion,
        types::Usd,
    };
    use chrono::{NaiveDate, Utc};
    use rust_decimal_macros::dec;

    fn daily_report(date: NaiveDate, pnl: Usd) -> DailyReport {
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
                trade_count: 0,
                success_count: 0,
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
            trade_count: 0,
            success_count: 0,
            miss_count: 0,
            largest_single_loss: Usd::ZERO,
            largest_single_profit: Usd::ZERO,
        }
    }

    fn date(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 6, day).expect("valid date")
    }

    #[test]
    fn series_sorts_ascending_and_accumulates_running_total() {
        // Repository order is newest-first; the series must re-sort ascending.
        let reports = vec![
            daily_report(date(3), Usd::new(dec!(-2))),
            daily_report(date(2), Usd::new(dec!(5))),
            daily_report(date(1), Usd::new(dec!(10))),
        ];
        let series = DailyPnlSeries::from_daily_reports(reports);
        let dates: Vec<_> = series.points.iter().map(|p| p.date).collect();
        assert_eq!(dates, vec![date(1), date(2), date(3)]);
        let cumulative: Vec<_> = series.points.iter().map(|p| p.total_pnl).collect();
        assert_eq!(
            cumulative,
            vec![Usd::new(dec!(10)), Usd::new(dec!(15)), Usd::new(dec!(13))]
        );
        assert_eq!(series.points[2].daily_pnl, Usd::new(dec!(-2)));
    }

    #[test]
    fn empty_reports_produce_empty_series() {
        assert!(
            DailyPnlSeries::from_daily_reports(Vec::new())
                .points
                .is_empty()
        );
    }

    #[test]
    fn query_days_defaults_and_bounds() {
        assert_eq!(
            DailyPnlSeriesQuery::default().resolve_days(),
            Ok(DailyPnlSeriesQuery::DEFAULT_DAYS)
        );
        assert_eq!(
            DailyPnlSeriesQuery { days: Some(90) }.resolve_days(),
            Ok(90)
        );
        assert!(
            DailyPnlSeriesQuery { days: Some(0) }
                .resolve_days()
                .is_err()
        );
        assert!(
            DailyPnlSeriesQuery { days: Some(91) }
                .resolve_days()
                .is_err()
        );
    }
}
