//! Post-commit report publisher.

use std::{
    fmt::Write as _,
    sync::{Arc, OnceLock},
};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::{
        quant::{
            OrderIntentInfo, PublishReportOutcome, RecommendationReportInfo,
            ReportFactDeliveryInfo, ReportRunInfo, ReportScheduleGapInfo, ReportScheduleHealthInfo,
        },
        runtime::{CoreEvent, CoreEventPublisher, ReportLifecycleEvent, ReportRunLifecycleEvent},
    },
    enums::{
        common::{AlertCategory, AlertLevel, AlertSource},
        quant::{ReportFactDeliveryStatus, ReportRunStatus},
    },
    runtime_config::ReportDeliveryPolicy,
};

use super::types::ReportNotificationPayload;
use crate::{
    execution::IntentTerminalEventSink,
    observability::{
        alert_dispatcher::{Alert, AlertDispatcher},
        metrics_hub::MetricsHub,
    },
};

/// Dependencies for [`ReportPublisher`].
pub struct ReportPublisherDeps {
    pub events: CoreEventPublisher,
    pub alerts: Arc<AlertDispatcher>,
    pub metrics: Arc<MetricsHub>,
}

/// Publishes non-authoritative side effects after the report PG transaction commits.
pub struct ReportPublisher {
    events: CoreEventPublisher,
    alerts: Arc<AlertDispatcher>,
    metrics: Arc<MetricsHub>,
    intent_terminal_events: OnceLock<Arc<dyn IntentTerminalEventSink>>,
}

fn non_negative_seconds(duration: Duration) -> f64 {
    duration
        .to_std()
        .map_or(0.0, |duration| duration.as_secs_f64())
}

impl ReportPublisher {
    /// Build a publisher.
    #[must_use]
    pub fn new(deps: ReportPublisherDeps) -> Self {
        Self {
            events: deps.events,
            alerts: deps.alerts,
            metrics: deps.metrics,
            intent_terminal_events: OnceLock::new(),
        }
    }

    /// Install the post-commit sink for atomically invalidated intents.
    pub fn set_intent_terminal_event_sink(&self, sink: Arc<dyn IntentTerminalEventSink>) {
        let _ = self.intent_terminal_events.set(sink);
    }

    /// Publish post-commit invalidation hints for governed report mutations.
    pub fn publish_invalidated_intents(
        &self,
        intents: &[OrderIntentInfo],
        occurred_at: DateTime<Utc>,
    ) {
        if let Some(sink) = self.intent_terminal_events.get() {
            sink.publish_invalidated(intents, occurred_at);
        }
    }

    /// Publish durable report/run-graph revision hints after publication commit.
    pub fn publish_publication(&self, outcome: &PublishReportOutcome, occurred_at: DateTime<Utc>) {
        let primary = if outcome.published() {
            ReportLifecycleEvent::committed(&outcome.report)
        } else {
            ReportLifecycleEvent::obsolete(&outcome.report)
        };
        self.events.publish(CoreEvent::Report(primary));
        for report in &outcome.superseded_reports {
            self.events
                .publish(CoreEvent::Report(ReportLifecycleEvent::superseded(report)));
        }
        for report in &outcome.obsoleted_reports {
            self.events
                .publish(CoreEvent::Report(ReportLifecycleEvent::obsolete(report)));
        }
        if let Some(sink) = self.intent_terminal_events.get() {
            sink.publish_invalidated(&outcome.invalidated_intents, occurred_at);
        }
    }

    /// Publish a newly committed Prepared artifact.
    pub fn publish_prepared(&self, report: &RecommendationReportInfo) {
        self.events
            .publish(CoreEvent::Report(ReportLifecycleEvent::prepared(report)));
    }

    /// Publish a durable report-run state revision hint.
    pub fn publish_run(&self, run: &ReportRunInfo, occurred_at: DateTime<Utc>) {
        self.metrics
            .report_run_total
            .with_label_values(&[
                run.trigger_kind.as_str(),
                run.status.as_str(),
                run.terminal_reason.map_or("none", |reason| reason.as_str()),
            ])
            .inc();
        if run.status == ReportRunStatus::Running {
            if let Some(started_at) = run.started_at {
                let queue_latency = non_negative_seconds(started_at - run.requested_at);
                self.metrics
                    .report_run_queue_latency_seconds
                    .with_label_values(&[run.trigger_kind.as_str()])
                    .observe(queue_latency);
            }
        } else if run.status.is_terminal()
            && let (Some(started_at), Some(finished_at)) = (run.started_at, run.finished_at)
        {
            let duration = non_negative_seconds(finished_at - started_at);
            self.metrics
                .report_run_duration_seconds
                .with_label_values(&[run.trigger_kind.as_str(), run.status.as_str()])
                .observe(duration);
        }
        if let (Some(schedule_id), Some(scheduled_for), Some(decision_at)) =
            (&run.schedule_id, run.scheduled_for, run.decision_at)
        {
            let lateness = non_negative_seconds(decision_at - scheduled_for);
            self.metrics
                .report_schedule_lateness_seconds
                .with_label_values(&[schedule_id])
                .observe(lateness);
        }
        self.events
            .publish(CoreEvent::ReportRun(ReportRunLifecycleEvent::from_run(
                run,
                occurred_at,
            )));
        if run.status == ReportRunStatus::Running {
            self.metrics.report_run_active.set(1);
        } else if run.status.is_terminal() {
            self.metrics.report_run_active.set(0);
        }
        if matches!(
            run.status,
            ReportRunStatus::Failed | ReportRunStatus::Abandoned
        ) {
            self.alerts.dispatch_background(
                Alert::new(
                    format!(
                        "quant-report-run:{}:{}",
                        run.trigger_key,
                        run.status.as_str()
                    ),
                    AlertLevel::Warning,
                    AlertCategory::SchedulerHealth,
                    AlertSource::Scheduler,
                    format!("Report run {}", run.status.as_str()),
                    run.error_summary
                        .clone()
                        .or_else(|| run.terminal_reason.map(|reason| reason.as_str().to_owned()))
                        .unwrap_or_else(|| "durable report run terminated".to_owned()),
                    occurred_at,
                )
                .with_affects_trading(false)
                .with_visible_toast(true)
                .with_dedupe_secs(300),
            );
        }
    }

    /// Record one committed append-only schedule gap.
    pub fn record_schedule_gap(&self, gap: &ReportScheduleGapInfo) -> QuantResult<()> {
        let missed_count =
            u64::try_from(gap.missed_count).map_err(|error| ReportError::NumericOverflow {
                field: "report_schedule_gap.missed_count",
                detail: error.to_string(),
            })?;
        self.metrics
            .report_schedule_gap_total
            .with_label_values(&[gap.schedule_id.as_str(), gap.reason.as_str()])
            .inc_by(missed_count);
        self.alerts.dispatch_background(
            Alert::new(
                format!(
                    "quant-report-schedule-gap:{}:{}",
                    gap.schedule_id,
                    gap.reason.as_str()
                ),
                AlertLevel::Warning,
                AlertCategory::SchedulerHealth,
                AlertSource::Scheduler,
                format!("Report schedule gap: {}", gap.schedule_id),
                format!(
                    "{} occurrence(s) missed from {} through {}; reason={}",
                    gap.missed_count,
                    gap.first_scheduled_for,
                    gap.last_scheduled_for,
                    gap.reason.as_str()
                ),
                gap.detected_at,
            )
            .with_affects_trading(false)
            .with_visible_toast(true)
            .with_dedupe_secs(900),
        );
        Ok(())
    }

    /// Publish durable queue gauges from a `PostgreSQL` health snapshot.
    pub fn record_schedule_health(&self, health: &ReportScheduleHealthInfo) -> QuantResult<()> {
        let queued_run_count = i64::try_from(health.queued_run_count).map_err(|error| {
            ReportError::NumericOverflow {
                field: "report_schedule_health.queued_run_count",
                detail: error.to_string(),
            }
        })?;
        let prepared_report_count =
            i64::try_from(health.prepared_report_count).map_err(|error| {
                ReportError::NumericOverflow {
                    field: "report_schedule_health.prepared_report_count",
                    detail: error.to_string(),
                }
            })?;
        self.metrics
            .report_run_active
            .set(i64::from(health.active_run.is_some()));
        self.metrics.report_run_queued.set(queued_run_count);
        self.metrics
            .report_prepared_backlog
            .set(prepared_report_count);
        self.metrics.report_current_age_seconds.reset();
        for current in &health.current_reports {
            if let Some(published_at) = current.published_at {
                self.metrics
                    .report_current_age_seconds
                    .with_label_values(&[current.profile_id.as_str(), current.report_kind.as_str()])
                    .set((health.observed_at - published_at).num_seconds().max(0));
            }
        }
        self.publish_health_alerts(health);
        Ok(())
    }

    fn publish_health_alerts(&self, health: &ReportScheduleHealthInfo) {
        if health.current_reports.is_empty() {
            self.alerts.dispatch_background(
                Alert::new(
                    "quant-report-health:no-current",
                    AlertLevel::Critical,
                    AlertCategory::TradingSafety,
                    AlertSource::Scheduler,
                    "No current recommendation report",
                    "No profile/report-kind scope has a current Published authority; new entry is unavailable.",
                    health.observed_at,
                )
                .with_affects_trading(true)
                .with_visible_toast(true)
                .with_dedupe_secs(900),
            );
        }
        for current in &health.current_reports {
            if current
                .valid_until
                .is_some_and(|valid_until| valid_until <= health.observed_at)
            {
                self.alerts.dispatch_background(
                    Alert::new(
                        format!(
                            "quant-report-health:expired-current:{}:{}",
                            current.profile_id,
                            current.report_kind.as_str()
                        ),
                        AlertLevel::Critical,
                        AlertCategory::TradingSafety,
                        AlertSource::Scheduler,
                        "Current recommendation report is past validity",
                        format!(
                            "Current report {} for {}:{} is past valid_until and must fail closed for new entry.",
                            current.recommendation_report_id,
                            current.profile_id,
                            current.report_kind.as_str()
                        ),
                        health.observed_at,
                    )
                    .with_affects_trading(true)
                    .with_visible_toast(true)
                    .with_dedupe_secs(900),
                );
            }
        }
        if health.prepared_report_count > 0 {
            self.alerts.dispatch_background(
                Alert::new(
                    "quant-report-health:prepared-backlog",
                    AlertLevel::Warning,
                    AlertCategory::Infrastructure,
                    AlertSource::ReportGenerator,
                    "Prepared report publication backlog",
                    format!(
                        "{} Prepared report(s) await fact verification/publication.",
                        health.prepared_report_count
                    ),
                    health.observed_at,
                )
                .with_affects_trading(false)
                .with_visible_toast(false)
                .with_dedupe_secs(900),
            );
        }
        if let Some(run) = &health.active_run
            && run
                .lease_expires_at
                .is_some_and(|lease_expires_at| lease_expires_at <= health.observed_at)
        {
            self.alerts.dispatch_background(
                Alert::new(
                    format!("quant-report-health:lease-overdue:{}", run.report_run_id),
                    AlertLevel::Warning,
                    AlertCategory::SchedulerHealth,
                    AlertSource::Scheduler,
                    "Report run lease is overdue",
                    format!(
                        "Running report {} has an expired lease and awaits abandonment recovery.",
                        run.report_run_id
                    ),
                    health.observed_at,
                )
                .with_affects_trading(false)
                .with_visible_toast(false)
                .with_dedupe_secs(900),
            );
        }
        for schedule in &health.schedules {
            if schedule.enabled && schedule.next_scheduled_for <= health.observed_at {
                self.alerts.dispatch_background(
                    Alert::new(
                        format!(
                            "quant-report-health:next-fire-overdue:{}",
                            schedule.schedule_id
                        ),
                        AlertLevel::Warning,
                        AlertCategory::SchedulerHealth,
                        AlertSource::Scheduler,
                        "Report schedule cursor is overdue",
                        format!(
                            "Schedule {} next occurrence {} is not yet materialized.",
                            schedule.schedule_id, schedule.next_scheduled_for
                        ),
                        health.observed_at,
                    )
                    .with_affects_trading(false)
                    .with_visible_toast(false)
                    .with_dedupe_secs(900),
                );
            }
        }
    }

    /// Publish a durable fact-delivery retry/failure hint.
    pub fn publish_delivery_state(&self, report: &RecommendationReportInfo, terminal: bool) {
        let event = if terminal {
            ReportLifecycleEvent::delivery_failed(report)
        } else {
            ReportLifecycleEvent::delivery_retrying(report)
        };
        self.events.publish(CoreEvent::Report(event));
        if terminal {
            self.alerts.dispatch_background(
                Alert::new(
                    format!(
                        "quant-report-fact-delivery:{}:failed",
                        report.recommendation_report_id
                    ),
                    AlertLevel::Critical,
                    AlertCategory::Infrastructure,
                    AlertSource::ReportGenerator,
                    "Report fact delivery exhausted retries",
                    format!(
                        "Report {} remains Prepared; current authority was not replaced. Governed publication retry is required.",
                        report.recommendation_report_id
                    ),
                    Utc::now(),
                )
                .with_affects_trading(false)
                .with_visible_toast(true)
                .with_dedupe_secs(900),
            );
        }
    }

    pub fn publish_fact_claim_lost(
        &self,
        operation: &'static str,
        delivery: &ReportFactDeliveryInfo,
    ) {
        if delivery.status == ReportFactDeliveryStatus::Cancelled {
            return;
        }
        self.alerts.dispatch_background(
            Alert::new(
                format!(
                    "quant-report-fact-claim-lost:{}:{operation}:{}",
                    delivery.recommendation_report_id,
                    delivery.status.as_str()
                ),
                AlertLevel::Warning,
                AlertCategory::Infrastructure,
                AlertSource::ReportGenerator,
                "Report fact delivery claim lost",
                format!(
                    "Report {} lost its {operation} claim while durable status is {}; another worker must recover it.",
                    delivery.recommendation_report_id,
                    delivery.status.as_str()
                ),
                Utc::now(),
            )
            .with_affects_trading(false)
            .with_visible_toast(false)
            .with_dedupe_secs(300),
        );
    }

    pub fn publish_fact_worker_error(&self) {
        self.alerts.dispatch_background(
            Alert::new(
                "quant-report-fact-worker:process-error",
                AlertLevel::Warning,
                AlertCategory::Infrastructure,
                AlertSource::ReportGenerator,
                "Report fact worker poll failed",
                "The durable worker remains alive and will retry after backoff; inspect structured logs and prepared backlog.",
                Utc::now(),
            )
            .with_affects_trading(false)
            .with_visible_toast(false)
            .with_dedupe_secs(300),
        );
    }

    /// Publish side effects only after both `ClickHouse` fact commitments have
    /// been independently verified and acknowledged in Postgres.
    pub async fn publish_verified(
        &self,
        report: &RecommendationReportInfo,
        notification: &ReportNotificationPayload,
        delivery_policy: ReportDeliveryPolicy,
        notify_operators: bool,
    ) {
        let status = report.status.as_str();
        let kind = report.report_kind.as_str();
        let published_count = report.summary_json.published_recommendation_count;

        self.metrics
            .report_generated_total
            .with_label_values(&[kind, status])
            .inc();
        self.metrics
            .report_recommendations_total
            .with_label_values(&[kind, status])
            .inc_by(u64::from(published_count));

        if delivery_policy == ReportDeliveryPolicy::StoreOnly || !notify_operators {
            return;
        }

        self.alerts
            .dispatch_operator_notification(
                Alert::new(
                    format!("quant-report:{}", report.recommendation_report_id),
                    AlertLevel::Info,
                    AlertCategory::OperatorNotice,
                    AlertSource::ReportGenerator,
                    format!("Quant report {}", report.recommendation_report_id),
                    notification_body(report, notification),
                    Utc::now(),
                )
                .with_affects_trading(false)
                .with_visible_toast(false)
                .with_dedupe_secs(0),
            )
            .await;
    }

    /// Publish a committed revoke state.
    pub fn publish_revoked(&self, report: &RecommendationReportInfo) {
        self.events
            .publish(CoreEvent::Report(ReportLifecycleEvent::revoked(report)));
    }

    /// Publish a committed expiry state.
    pub fn publish_expired(&self, report: &RecommendationReportInfo) {
        self.events
            .publish(CoreEvent::Report(ReportLifecycleEvent::expired(report)));
    }
}

/// Relative path to a report's detail view, embedded in operator notifications.
fn report_path(report: &RecommendationReportInfo) -> String {
    format!("/quant/reports/{}", report.recommendation_report_id)
}

fn notification_body(report: &RecommendationReportInfo, n: &ReportNotificationPayload) -> String {
    let mut body = format!(
        "status={} mode={} recommendations={} total_suggested_usd={}",
        n.status,
        n.runtime_mode.as_str(),
        n.published_count,
        n.total_suggested_usd,
    );
    if let Some(reason) = n.empty_reason {
        let _ = write!(body, " empty_reason={}", reason.as_str());
    }
    for (idx, rec) in n.top3.iter().enumerate() {
        let _ = write!(
            body,
            "\n  #{rank} {market} {side} score={score} usd={usd}",
            rank = idx + 1,
            market = rec.market_id,
            side = rec.outcome_side.as_str(),
            score = rec.score,
            usd = rec
                .suggested_usd
                .map_or_else(|| "unavailable".to_owned(), |value| value.to_string()),
        );
    }
    for warning in &n.warnings {
        let _ = write!(body, "\n  warning: {warning}");
    }
    let _ = write!(body, "\n  report: {}", report_path(report));
    body
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        enums::quant::{OutcomeSide, QuantRuntimeMode, RecommendationReportStatus, ReportKind},
        types::{Probability, RecommendationReportId, Usd},
    };
    use rust_decimal_macros::dec;

    use super::{ReportNotificationPayload, notification_body};
    use crate::{report::NotificationRecommendation, test_fixtures::report_fixtures};

    #[test]
    fn published_notification_contains_top3_total_mode() {
        let report = report_fixtures::report(
            RecommendationReportId::from_v7(),
            ReportKind::TopN,
            RecommendationReportStatus::Published,
        );
        let payload = ReportNotificationPayload {
            report_id: report.recommendation_report_id.clone(),
            kind: ReportKind::TopN,
            status: "published".to_owned(),
            runtime_mode: QuantRuntimeMode::SemiAuto,
            published_count: 2,
            total_suggested_usd: Usd::new(dec!(500)),
            top3: vec![
                NotificationRecommendation {
                    market_id: "0xA".to_owned(),
                    outcome_side: OutcomeSide::Yes,
                    score: Probability::new(dec!(0.71)),
                    suggested_usd: Some(Usd::new(dec!(300))),
                },
                NotificationRecommendation {
                    market_id: "0xB".to_owned(),
                    outcome_side: OutcomeSide::No,
                    score: Probability::new(dec!(0.66)),
                    suggested_usd: Some(Usd::new(dec!(200))),
                },
            ],
            warnings: vec!["thin book".to_owned()],
            empty_reason: None,
        };
        let body = notification_body(&report, &payload);
        assert!(body.contains("mode=semi_auto"), "{body}");
        assert!(body.contains("total_suggested_usd=500"), "{body}");
        assert!(body.contains("0xA"), "top-1 market present: {body}");
        assert!(body.contains("0xB"), "top-2 market present: {body}");
        assert!(body.contains("warning: thin book"), "{body}");
        assert!(
            body.contains(&format!(
                "/quant/reports/{}",
                report.recommendation_report_id
            )),
            "report link present: {body}"
        );
    }
}
