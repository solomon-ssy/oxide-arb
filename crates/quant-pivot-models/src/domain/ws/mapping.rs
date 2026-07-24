//! [`CoreEvent`] → wire-envelope projection.

use serde_json::Value;

use crate::{
    domain::{
        runtime::CoreEvent,
        ws::{
            channel::{SubscriptionKey, WsChannel},
            envelope::WsEnvelope,
        },
    },
    types::MarketId,
};

impl CoreEvent {
    /// Map a [`CoreEvent`] to its fan-out [`SubscriptionKey`] and [`WsEnvelope`].
    #[must_use]
    pub fn event_envelope(&self) -> Option<(SubscriptionKey, WsEnvelope)> {
        let (channel, market, data): (WsChannel, Option<MarketId>, Value) = match self {
            Self::SystemStatusChanged(status) => (
                WsChannel::SystemStatus,
                None,
                serde_json::to_value(status).ok()?,
            ),
            Self::Alert(alert) => (
                WsChannel::SystemAlert,
                None,
                serde_json::to_value(alert).ok()?,
            ),
            Self::MarketResolved { market_id, outcome } => (
                WsChannel::MarketResolved,
                None,
                serde_json::json!({ "market_id": market_id, "outcome": outcome }),
            ),
            Self::MarketBookUpdate { market_id, view } => (
                WsChannel::MarketBookUpdate,
                Some(market_id.clone()),
                serde_json::to_value(view.as_ref()).ok()?,
            ),
            Self::ConfigActivated { version_id } => (
                WsChannel::ConfigActivated,
                None,
                serde_json::json!({ "version_id": version_id }),
            ),
            Self::Report(payload) => (
                WsChannel::QuantReport,
                None,
                serde_json::to_value(payload).ok()?,
            ),
            Self::ReportRun(payload) => (
                WsChannel::QuantReportRun,
                None,
                serde_json::to_value(payload).ok()?,
            ),
            Self::Intent(payload) => (
                WsChannel::QuantIntent,
                None,
                serde_json::to_value(payload).ok()?,
            ),
            Self::Condition(payload) => (
                WsChannel::QuantCondition,
                None,
                serde_json::to_value(payload).ok()?,
            ),
            Self::MaterializationRun(payload) => (
                WsChannel::MaterializationRunUpdate,
                None,
                serde_json::to_value(payload).ok()?,
            ),
            Self::Reconciliation(payload) => (
                WsChannel::QuantReconciliation,
                None,
                serde_json::to_value(payload).ok()?,
            ),
            Self::Settlement(payload) => (
                WsChannel::QuantSettlement,
                None,
                serde_json::to_value(payload).ok()?,
            ),
        };

        let key = SubscriptionKey::new(channel, market);
        Some((key, WsEnvelope::channel(channel, data)))
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::{
        domain::{
            api::{MarketBookView, SystemCapabilities, SystemStatusView},
            governance::SystemStatus,
            runtime::{
                CoreEvent, MaterializationRunEvent, MaterializationRunKind,
                MaterializationRunStatus, ReconciliationLifecycleEvent, ReportEventKind,
                ReportLifecycleEvent, ReportRunLifecycleEvent, SettlementRedeemLifecycleEvent,
            },
            ws::{SubscriptionKey, channel::WsChannel},
        },
        enums::{
            execution::ReconciliationResult,
            quant::{
                QuantRuntimeMode, RecommendationReportStatus, ReportKind, ReportRunStatus,
                TrainingDatasetStatus,
            },
            settlement::SettlementCaseState,
            system::CapabilityReason,
        },
        types::{MarketId, RecommendationReportId, ReportRunId, ResearchProfileId},
    };

    #[test]
    fn market_book_maps_key() {
        let event = CoreEvent::MarketBookUpdate {
            market_id: MarketId::new("0xabc"),
            view: Box::new(MarketBookView {
                market_id: MarketId::new("0xabc"),
                yes: None,
                no: None,
            }),
        };
        let (key, envelope) = event.event_envelope().expect("book update maps");
        assert_eq!(
            key,
            SubscriptionKey::scoped(WsChannel::MarketBookUpdate, MarketId::new("0xabc"))
        );
        assert_eq!(envelope.kind.as_str(), "market.book_update");
    }

    #[test]
    fn system_status_maps_key() {
        let event = CoreEvent::SystemStatusChanged(Box::new(SystemStatusView {
            runtime: SystemStatus::bootstrap(QuantRuntimeMode::ReportOnly),
            capabilities: SystemCapabilities::fail_closed(CapabilityReason::ControlPlaneNotReady),
        }));
        let (key, envelope) = event.event_envelope().expect("status maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::SystemStatus));
        assert_eq!(envelope.kind.as_str(), "system.status");
    }

    #[test]
    fn durable_report_maps_channel() {
        let event = CoreEvent::Report(ReportLifecycleEvent {
            event: ReportEventKind::Prepared,
            recommendation_report_id: "report-1".to_owned(),
            profile_id: ResearchProfileId::new("weather_forecast_24h"),
            report_kind: ReportKind::TopN,
            runtime_mode: QuantRuntimeMode::ReportOnly,
            status: RecommendationReportStatus::Prepared,
            decision_at: Utc::now(),
            published_at: None,
            recommendation_count: 0,
            empty_reason: None,
            error_code: None,
            status_reason: None,
        });
        let (key, envelope) = event.event_envelope().expect("report revision maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::QuantReport));
        assert_eq!(envelope.kind.as_str(), "quant.report");
        assert_eq!(envelope.data["event"], "prepared");
        assert_eq!(envelope.data["recommendation_report_id"], "report-1");
    }

    #[test]
    fn durable_run_maps_channel() {
        let run_id = ReportRunId::from_v7();
        let event = CoreEvent::ReportRun(ReportRunLifecycleEvent {
            report_run_id: run_id,
            status: ReportRunStatus::Running,
            terminal_reason: None,
            output_report_id: None,
            occurred_at: Utc::now(),
        });
        let (key, envelope) = event.event_envelope().expect("report run revision maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::QuantReportRun));
        assert_eq!(envelope.kind.as_str(), "quant.report_run");
        assert_eq!(envelope.data["report_run_id"], run_id.to_string());
        assert_eq!(envelope.data["status"], "running");
    }

    #[test]
    fn report_run_report_backing() {
        let report = CoreEvent::Report(ReportLifecycleEvent {
            event: ReportEventKind::Published,
            recommendation_report_id: "durable-report-id".to_owned(),
            profile_id: ResearchProfileId::new("weather_forecast_24h"),
            report_kind: ReportKind::TopN,
            runtime_mode: QuantRuntimeMode::ReportOnly,
            status: RecommendationReportStatus::Published,
            decision_at: Utc::now(),
            published_at: Some(Utc::now()),
            recommendation_count: 3,
            empty_reason: None,
            error_code: None,
            status_reason: None,
        });
        let (report_key, report_envelope) = report.event_envelope().expect("map durable report");
        assert_eq!(report_key, SubscriptionKey::global(WsChannel::QuantReport));
        assert_eq!(
            report_envelope.data["recommendation_report_id"],
            "durable-report-id"
        );
        assert_eq!(report_envelope.data["status"], "published");

        let run_id = ReportRunId::from_v7();
        let run = CoreEvent::ReportRun(ReportRunLifecycleEvent {
            report_run_id: run_id,
            status: ReportRunStatus::Succeeded,
            terminal_reason: None,
            output_report_id: Some(RecommendationReportId::from_v7()),
            occurred_at: Utc::now(),
        });
        let (run_key, run_envelope) = run.event_envelope().expect("map durable run");
        assert_eq!(run_key, SubscriptionKey::global(WsChannel::QuantReportRun));
        assert_eq!(run_envelope.data["report_run_id"], run_id.to_string());
        assert_eq!(run_envelope.data["status"], "succeeded");
    }

    #[test]
    fn dataset_build_maps_status() {
        assert_eq!(
            MaterializationRunStatus::from(TrainingDatasetStatus::Failed),
            MaterializationRunStatus::Failed
        );
        assert_eq!(
            MaterializationRunStatus::from(TrainingDatasetStatus::InsufficientLabels),
            MaterializationRunStatus::Failed
        );
        assert_eq!(
            MaterializationRunStatus::from(TrainingDatasetStatus::Building),
            MaterializationRunStatus::Running
        );
    }

    #[test]
    fn materialization_run_maps_channel() {
        let event = CoreEvent::MaterializationRun(MaterializationRunEvent::revision(
            "run-1",
            MaterializationRunKind::Training,
            MaterializationRunStatus::Completed,
        ));
        let (key, envelope) = event.event_envelope().expect("materialization maps");
        assert_eq!(
            key,
            SubscriptionKey::global(WsChannel::MaterializationRunUpdate)
        );
        assert_eq!(envelope.kind.as_str(), "materialization.run_update");
    }

    #[test]
    fn reconciliation_maps_global_channel() {
        let event = CoreEvent::Reconciliation(ReconciliationLifecycleEvent {
            execution_order_id: "eo-1".to_owned(),
            order_intent_id: "oi-1".to_owned(),
            result: ReconciliationResult::Filled,
            operator_resolved: true,
        });
        let (key, envelope) = event.event_envelope().expect("reconciliation maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::QuantReconciliation));
        assert_eq!(envelope.kind.as_str(), "quant.reconciliation");
    }

    #[test]
    fn settlement_maps_global_channel() {
        let event = CoreEvent::Settlement(SettlementRedeemLifecycleEvent {
            settlement_redeem_id: "sr-1".to_owned(),
            market_id: MarketId::new("0xabc"),
            state: SettlementCaseState::Confirmed,
        });
        let (key, envelope) = event.event_envelope().expect("settlement maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::QuantSettlement));
        assert_eq!(envelope.kind.as_str(), "quant.settlement");
    }
}
