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
        CoreEvent::Intent(payload) => (
            WsChannel::QuantIntent,
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
            CoreEvent, MarketBookView, MaterializationRunEvent, MaterializationRunKind,
            MaterializationRunStatus, ReconciliationLifecycleEvent, SettlementRedeemLifecycleEvent,
            SubscriptionKey, SystemStatus, ws::channel::WsChannel,
        },
        enums::{
            execution::{ReconciliationResult, SettlementRedeemState},
            quant::{QuantRuntimeMode, TrainingDatasetStatus},
        },
        types::MarketId,
    };

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
        let event =
            CoreEvent::SystemStatusChanged(SystemStatus::bootstrap(QuantRuntimeMode::ReportOnly));
        let (key, envelope) = event_envelope(&event).expect("status maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::SystemStatus));
        assert_eq!(envelope.kind.as_str(), "system.status");
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
