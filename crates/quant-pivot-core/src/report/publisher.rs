//! Post-commit report publisher.

use std::{fmt::Write as _, sync::Arc};

use chrono::Utc;
use quant_pivot_models::{
    domain::{CoreEvent, CoreEventPublisher, RecommendationReportInfo, ReportLifecycleEvent},
    enums::common::{AlertCategory, AlertLevel, AlertSource},
    runtime_config::ReportDeliveryPolicy,
};

use crate::observability::{
    alert_dispatcher::{Alert, AlertDispatcher},
    metrics_hub::MetricsHub,
    recommendation_fact_writer::RecommendationEventWriter,
};

use super::types::{ComposedReport, ReportNotificationPayload};

/// Dependencies for [`ReportPublisher`].
pub struct ReportPublisherDeps {
    pub events: CoreEventPublisher,
    pub recommendation_writer: Arc<RecommendationEventWriter>,
    pub alerts: Arc<AlertDispatcher>,
    pub metrics: Arc<MetricsHub>,
}

/// Publishes non-authoritative side effects after the report PG transaction commits.
pub struct ReportPublisher {
    events: CoreEventPublisher,
    recommendation_writer: Arc<RecommendationEventWriter>,
    alerts: Arc<AlertDispatcher>,
    metrics: Arc<MetricsHub>,
}

impl ReportPublisher {
    /// Build a publisher.
    #[must_use]
    pub fn new(deps: ReportPublisherDeps) -> Self {
        Self {
            events: deps.events,
            recommendation_writer: deps.recommendation_writer,
            alerts: deps.alerts,
            metrics: deps.metrics,
        }
    }

    /// Publish all post-commit side effects. Best-effort mirrors never change PG
    /// authority; failures are recorded as metrics/logs.
    pub async fn publish_committed(
        &self,
        report: &RecommendationReportInfo,
        composed: &ComposedReport,
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

        self.recommendation_writer
            .write_batch(composed.ch_rows.clone());

        self.events
            .publish(CoreEvent::Report(ReportLifecycleEvent::committed(report)));

        if composed.delivery_policy == ReportDeliveryPolicy::StoreOnly || !composed.notify_operators
        {
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
                    notification_body(report, &composed.notification),
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

    /// Publish an ephemeral lifecycle signal (`started` / `failed`) that has no
    /// backing report row. WebSocket-only; never raises an operator notification.
    pub fn publish_ephemeral(&self, event: ReportLifecycleEvent) {
        self.events.publish(CoreEvent::Report(event));
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
            usd = rec.suggested_usd,
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
    use super::{ReportNotificationPayload, notification_body};
    use quant_pivot_models::{
        enums::quant::{OutcomeSide, QuantRuntimeMode, RecommendationReportStatus, ReportKind},
        types::{Probability, RecommendationReportId, Usd},
    };
    use quant_pivot_test_support::report_fixtures;
    use rust_decimal_macros::dec;

    use crate::report::NotificationRecommendation;

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
                    suggested_usd: Usd::new(dec!(300)),
                },
                NotificationRecommendation {
                    market_id: "0xB".to_owned(),
                    outcome_side: OutcomeSide::No,
                    score: Probability::new(dec!(0.66)),
                    suggested_usd: Usd::new(dec!(200)),
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
