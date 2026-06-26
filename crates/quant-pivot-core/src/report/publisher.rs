//! Post-commit report publisher.

use std::sync::Arc;

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

use super::types::ComposedReport;

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
            .publish(CoreEvent::ReportPublished(lifecycle_event(report)));

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
                    notification_body(composed),
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
            .publish(CoreEvent::ReportRevoked(lifecycle_event(report)));
    }

    /// Publish a committed expiry state.
    pub fn publish_expired(&self, report: &RecommendationReportInfo) {
        self.events
            .publish(CoreEvent::ReportExpired(lifecycle_event(report)));
    }
}

fn lifecycle_event(report: &RecommendationReportInfo) -> ReportLifecycleEvent {
    ReportLifecycleEvent {
        recommendation_report_id: report.recommendation_report_id.to_string(),
        report_kind: report.report_kind,
        status: report.status,
        as_of: report.as_of,
        published_at: report.published_at,
        recommendation_count: report.summary_json.published_recommendation_count,
        empty_reason: report.summary_json.empty_reason,
        status_reason: report.status_reason.clone(),
    }
}

fn notification_body(composed: &ComposedReport) -> String {
    let notification = &composed.notification;
    notification.empty_reason.map_or_else(
        || {
            format!(
                "status={}, recommendations={}",
                notification.status, notification.published_count
            )
        },
        |reason| {
            format!(
                "status={}, recommendations=0, empty_reason={}",
                notification.status,
                reason.as_str()
            )
        },
    )
}
