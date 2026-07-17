//! [`CoreEvent`] → wire-envelope projection (Phase 0).

use crate::{
    domain::{
        CoreEvent,
        ws::{
            channel::{SubscriptionKey, WsChannel},
            envelope::WsEnvelope,
        },
    },
    types::MarketId,
};
use serde_json::Value;

/// Map a [`CoreEvent`] to its fan-out [`SubscriptionKey`] and [`WsEnvelope`].
#[must_use]
pub fn event_envelope(event: &CoreEvent) -> Option<(SubscriptionKey, WsEnvelope)> {
    let (channel, market, data): (WsChannel, Option<MarketId>, Value) = match event {
        CoreEvent::SystemStatusChanged(status) => (
            WsChannel::SystemStatus,
            None,
            serde_json::to_value(status).ok()?,
        ),
        CoreEvent::Alert(alert) => (
            WsChannel::SystemAlert,
            None,
            serde_json::to_value(alert).ok()?,
        ),
        CoreEvent::MarketResolved { market_id, outcome } => (
            WsChannel::MarketResolved,
            None,
            serde_json::json!({ "market_id": market_id, "outcome": outcome }),
        ),
        CoreEvent::MarketBookUpdate { market_id, view } => (
            WsChannel::MarketBookUpdate,
            Some(market_id.clone()),
            serde_json::to_value(view.as_ref()).ok()?,
        ),
        CoreEvent::ConfigActivated { version_id } => (
            WsChannel::ConfigActivated,
            None,
            serde_json::json!({ "version_id": version_id }),
        ),
        CoreEvent::Report(payload) => (
            WsChannel::QuantReport,
            None,
            serde_json::to_value(payload).ok()?,
        ),
        CoreEvent::ReportRun(payload) => (
            WsChannel::QuantReportRun,
            None,
            serde_json::to_value(payload).ok()?,
        ),
        CoreEvent::Intent(payload) => (
            WsChannel::QuantIntent,
            None,
            serde_json::to_value(payload).ok()?,
        ),
        CoreEvent::Condition(payload) => (
            WsChannel::QuantCondition,
            None,
            serde_json::to_value(payload).ok()?,
        ),
        CoreEvent::MaterializationRun(payload) => (
            WsChannel::MaterializationRunUpdate,
            None,
            serde_json::to_value(payload).ok()?,
        ),
        CoreEvent::Reconciliation(payload) => (
            WsChannel::QuantReconciliation,
            None,
            serde_json::to_value(payload).ok()?,
        ),
        CoreEvent::Settlement(payload) => (
            WsChannel::QuantSettlement,
            None,
            serde_json::to_value(payload).ok()?,
        ),
    };

    let key = SubscriptionKey::new(channel, market);
    Some((key, WsEnvelope::channel(channel, data)))
}

#[cfg(test)]
mod tests {
    use super::event_envelope;
    use crate::{
        domain::{
            BootstrapView, CoreEvent, MarketBookView, MaterializationRunEvent,
            MaterializationRunKind, MaterializationRunStatus, ReconciliationLifecycleEvent,
            ReportEventKind, ReportLifecycleEvent, ReportRunLifecycleEvent,
            SettlementRedeemLifecycleEvent, SubscriptionKey, SystemCapabilities, SystemStatus,
            SystemStatusView, ws::channel::WsChannel,
        },
        enums::{
            execution::{ReconciliationResult, SettlementRedeemState},
            quant::{
                QuantRuntimeMode, RecommendationReportStatus, ReportKind, ReportRunStatus,
                TrainingDatasetStatus,
            },
            system::{BootstrapPhase, CapabilityReason},
        },
        types::{MarketId, RecommendationReportId, ReportRunId},
    };
    use chrono::Utc;

    #[test]
    fn market_book_update_maps_to_market_scoped_key() {
        let event = CoreEvent::MarketBookUpdate {
            market_id: MarketId::new("0xabc"),
            view: Box::new(MarketBookView {
                market_id: MarketId::new("0xabc"),
                yes: None,
                no: None,
            }),
        };
        let (key, envelope) = event_envelope(&event).expect("book update maps");
        assert_eq!(
            key,
            SubscriptionKey::scoped(WsChannel::MarketBookUpdate, MarketId::new("0xabc"))
        );
        assert_eq!(envelope.kind.as_str(), "market.book_update");
    }

    #[test]
    fn system_status_maps_to_global_key() {
        let event = CoreEvent::SystemStatusChanged(Box::new(SystemStatusView {
            runtime: SystemStatus::bootstrap(QuantRuntimeMode::ReportOnly),
            bootstrap: BootstrapView {
                phase: BootstrapPhase::Initializing,
                bootstrap_contract_version: 1,
                state_revision: 0,
            },
            capabilities: SystemCapabilities::fail_closed(CapabilityReason::BootstrapInitializing),
        }));
        let (key, envelope) = event_envelope(&event).expect("status maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::SystemStatus));
        assert_eq!(envelope.kind.as_str(), "system.status");
    }

    #[test]
    fn durable_report_revision_maps_to_report_channel() {
        let event = CoreEvent::Report(ReportLifecycleEvent {
            event: ReportEventKind::Prepared,
            recommendation_report_id: "report-1".to_owned(),
            profile_id: "weather_forecast_24h".to_owned(),
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
        let (key, envelope) = event_envelope(&event).expect("report revision maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::QuantReport));
        assert_eq!(envelope.kind.as_str(), "quant.report");
        assert_eq!(envelope.data["event"], "prepared");
        assert_eq!(envelope.data["recommendation_report_id"], "report-1");
    }

    #[test]
    fn durable_run_revision_maps_to_dedicated_run_channel() {
        let run_id = ReportRunId::from_v7();
        let event = CoreEvent::ReportRun(ReportRunLifecycleEvent {
            report_run_id: run_id.clone(),
            status: ReportRunStatus::Running,
            terminal_reason: None,
            output_report_id: None,
            occurred_at: Utc::now(),
        });
        let (key, envelope) = event_envelope(&event).expect("report run revision maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::QuantReportRun));
        assert_eq!(envelope.kind.as_str(), "quant.report_run");
        assert_eq!(envelope.data["report_run_id"], run_id.to_string());
        assert_eq!(envelope.data["status"], "running");
    }

    #[test]
    fn report_run_and_report_events_have_durable_backing() {
        let report = CoreEvent::Report(ReportLifecycleEvent {
            event: ReportEventKind::Published,
            recommendation_report_id: "durable-report-id".to_owned(),
            profile_id: "weather_forecast_24h".to_owned(),
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
        let (report_key, report_envelope) = event_envelope(&report).expect("map durable report");
        assert_eq!(report_key, SubscriptionKey::global(WsChannel::QuantReport));
        assert_eq!(
            report_envelope.data["recommendation_report_id"],
            "durable-report-id"
        );
        assert_eq!(report_envelope.data["status"], "published");

        let run_id = ReportRunId::from_v7();
        let run = CoreEvent::ReportRun(ReportRunLifecycleEvent {
            report_run_id: run_id.clone(),
            status: ReportRunStatus::Succeeded,
            terminal_reason: None,
            output_report_id: Some(RecommendationReportId::from_v7()),
            occurred_at: Utc::now(),
        });
        let (run_key, run_envelope) = event_envelope(&run).expect("map durable run");
        assert_eq!(run_key, SubscriptionKey::global(WsChannel::QuantReportRun));
        assert_eq!(run_envelope.data["report_run_id"], run_id.to_string());
        assert_eq!(run_envelope.data["status"], "succeeded");
    }

    #[test]
    fn dataset_build_status_maps_to_materialization_run_status() {
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
    fn materialization_run_maps_to_global_channel() {
        let event = CoreEvent::MaterializationRun(MaterializationRunEvent::revision(
            "run-1",
            MaterializationRunKind::Training,
            MaterializationRunStatus::Completed,
        ));
        let (key, envelope) = event_envelope(&event).expect("materialization maps");
        assert_eq!(
            key,
            SubscriptionKey::global(WsChannel::MaterializationRunUpdate)
        );
        assert_eq!(envelope.kind.as_str(), "materialization.run_update");
    }

    #[test]
    fn reconciliation_maps_to_global_channel() {
        let event = CoreEvent::Reconciliation(ReconciliationLifecycleEvent {
            execution_order_id: "eo-1".to_owned(),
            order_intent_id: "oi-1".to_owned(),
            result: ReconciliationResult::Filled,
            operator_resolved: true,
        });
        let (key, envelope) = event_envelope(&event).expect("reconciliation maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::QuantReconciliation));
        assert_eq!(envelope.kind.as_str(), "quant.reconciliation");
    }

    #[test]
    fn settlement_maps_to_global_channel() {
        let event = CoreEvent::Settlement(SettlementRedeemLifecycleEvent {
            settlement_redeem_id: "sr-1".to_owned(),
            market_id: MarketId::new("0xabc"),
            state: SettlementRedeemState::Confirmed,
        });
        let (key, envelope) = event_envelope(&event).expect("settlement maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::QuantSettlement));
        assert_eq!(envelope.kind.as_str(), "quant.settlement");
    }
}
