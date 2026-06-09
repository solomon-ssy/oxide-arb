use crate::observability::alert_dispatcher::{Alert, AlertDispatcher};
use chrono::{DateTime, Datelike, Days, NaiveDate, TimeZone, Utc, Weekday};
use oxide_arb_error::OxideError;
use oxide_arb_models::{
    domain::{
        ReportRiskSummary, ReportTradeStats, SettledPnlStats, SettledPositionStats,
        trade::{DailyReport, WeeklyReport},
    },
    enums::{common::AlertLevel, report::ReportSchemaVersion},
    types::Usd,
};
use oxide_arb_repository::{
    postgres::{PgPositionRepository, PgReportRepository, PgTradeRepository},
    traits::{PositionRepository, ReportRepository, TradeRepository},
};
use oxide_arb_risk::{engine::RiskEngine, traits::RiskMetrics};
use std::sync::Arc;

pub struct ReportGenerator {
    trade_repo: Arc<PgTradeRepository>,
    position_repo: Arc<PgPositionRepository>,
    report_repo: Arc<PgReportRepository>,
    risk_engine: Arc<RiskEngine>,
    risk_metrics: Arc<dyn RiskMetrics>,
    alerts: Arc<AlertDispatcher>,
}

impl ReportGenerator {
    pub const fn new(
        trade_repo: Arc<PgTradeRepository>,
        position_repo: Arc<PgPositionRepository>,
        report_repo: Arc<PgReportRepository>,
        risk_engine: Arc<RiskEngine>,
        risk_metrics: Arc<dyn RiskMetrics>,
        alerts: Arc<AlertDispatcher>,
    ) -> Self {
        Self {
            trade_repo,
            position_repo,
            report_repo,
            risk_engine,
            risk_metrics,
            alerts,
        }
    }

    pub async fn generate_daily(&self, date: NaiveDate) -> Result<DailyReport, OxideError> {
        let start = start_of_day(date)?;
        let end = start_of_day(
            date.checked_add_days(Days::new(1))
                .ok_or_else(|| OxideError::Internal("daily report date overflow".into()))?,
        )?;
        let execution = self.trade_repo.aggregate_between(start, end).await?;
        let settled = self
            .position_repo
            .aggregate_settled_between(start, end)
            .await?;
        let report = self.daily_from_parts(date, execution, &settled);
        let payload = serde_json::to_value(&report)
            .map_err(|error| OxideError::Internal(error.to_string()))?;
        self.report_repo.save_daily(date, payload).await?;
        self.dispatch_daily_alert(&report).await;
        Ok(report)
    }

    pub async fn generate_weekly(&self, week_start: NaiveDate) -> Result<WeeklyReport, OxideError> {
        let week_end = week_start
            .checked_add_days(Days::new(6))
            .ok_or_else(|| OxideError::Internal("weekly report date overflow".into()))?;
        let start = start_of_day(week_start)?;
        let end = start_of_day(
            week_end
                .checked_add_days(Days::new(1))
                .ok_or_else(|| OxideError::Internal("weekly report end overflow".into()))?,
        )?;
        let execution = self.trade_repo.aggregate_between(start, end).await?;
        let settled = self
            .position_repo
            .aggregate_settled_between(start, end)
            .await?;
        let mut daily_reports = Vec::with_capacity(7);
        for offset in 0..7 {
            let date = week_start
                .checked_add_days(Days::new(offset))
                .ok_or_else(|| OxideError::Internal("weekly daily date overflow".into()))?;
            daily_reports.push(self.generate_daily(date).await?);
        }
        let report = WeeklyReport {
            week_start,
            week_end,
            schema_version: ReportSchemaVersion::V1,
            generated_at: Utc::now(),
            settled_pnl: SettledPnlStats::from(&settled),
            execution,
            risk: self.risk_summary(),
            daily_reports,
        };
        let payload = serde_json::to_value(&report)
            .map_err(|error| OxideError::Internal(error.to_string()))?;
        self.report_repo
            .save_weekly(week_start, week_end, payload)
            .await?;
        self.dispatch_weekly_alert(&report).await;
        Ok(report)
    }

    fn daily_from_parts(
        &self,
        date: NaiveDate,
        execution: ReportTradeStats,
        settled: &SettledPositionStats,
    ) -> DailyReport {
        let settled_pnl = SettledPnlStats::from(settled);
        DailyReport {
            date,
            schema_version: ReportSchemaVersion::V1,
            generated_at: Utc::now(),
            period_start: date,
            period_end: date,
            total_pnl: settled_pnl.realized_pnl,
            total_fees_paid: settled_pnl.total_fees,
            total_gas_paid: Usd::ZERO,
            trade_count: execution.trade_count,
            success_count: execution.success_count,
            miss_count: execution.miss_count,
            largest_single_loss: settled_pnl.largest_single_loss,
            largest_single_profit: settled_pnl.largest_single_profit,
            settled_pnl,
            execution,
            risk: self.risk_summary(),
        }
    }

    fn risk_summary(&self) -> ReportRiskSummary {
        let snapshot = self.risk_engine.snapshot(self.risk_metrics.as_ref());
        ReportRiskSummary {
            daily_pnl: snapshot.daily_pnl,
            daily_loss: snapshot.daily_loss_usd,
            weekly_loss: snapshot.weekly_loss_usd,
            total_exposure: snapshot.total_exposure,
            open_position_count: u32::try_from(self.risk_metrics.open_position_count())
                .unwrap_or(u32::MAX),
        }
    }

    async fn dispatch_daily_alert(&self, report: &DailyReport) {
        let severity = if report.settled_pnl.failed_accounting_count > 0 {
            AlertLevel::Warning
        } else {
            AlertLevel::Info
        };
        self.alerts
            .dispatch(Alert {
                severity,
                title: format!("Daily report {}", report.date),
                body: format!(
                    "settled_pnl={} trades={} settled_positions={} failed_accounting={}",
                    report.total_pnl,
                    report.trade_count,
                    report.settled_pnl.settled_position_count,
                    report.settled_pnl.failed_accounting_count
                ),
                timestamp: Utc::now(),
            })
            .await;
    }

    async fn dispatch_weekly_alert(&self, report: &WeeklyReport) {
        self.alerts
            .dispatch(Alert {
                severity: AlertLevel::Info,
                title: format!("Weekly report {}", report.week_start),
                body: format!(
                    "settled_pnl={} trades={} settled_positions={}",
                    report.settled_pnl.realized_pnl,
                    report.execution.trade_count,
                    report.settled_pnl.settled_position_count
                ),
                timestamp: Utc::now(),
            })
            .await;
    }
}

pub fn previous_utc_day(now: DateTime<Utc>) -> NaiveDate {
    now.date_naive()
        .checked_sub_days(Days::new(1))
        .unwrap_or_else(|| now.date_naive())
}

pub fn previous_utc_week_start(now: DateTime<Utc>) -> NaiveDate {
    let today = now.date_naive();
    let days_since_monday = u64::from(today.weekday().num_days_from_monday());
    let this_monday = today
        .checked_sub_days(Days::new(days_since_monday))
        .unwrap_or(today);
    if today.weekday() == Weekday::Mon {
        this_monday
            .checked_sub_days(Days::new(7))
            .unwrap_or(this_monday)
    } else {
        this_monday
    }
}

fn start_of_day(date: NaiveDate) -> Result<DateTime<Utc>, OxideError> {
    Utc.from_local_datetime(
        &date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| OxideError::Internal("invalid report date".into()))?,
    )
    .single()
    .ok_or_else(|| OxideError::Internal("invalid UTC report date".into()))
}
