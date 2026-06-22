//! [`CoreEvent`] → wire-envelope projection (Phase 0).

use serde_json::Value;

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
    };

    let key = SubscriptionKey::new(channel, market);
    Some((key, WsEnvelope::channel(channel, data)))
}

#[cfg(test)]
mod tests {
    use super::event_envelope;
    use crate::{
        domain::{
            CoreEvent, MarketBookView, SubscriptionKey, SystemStatus, ws::channel::WsChannel,
        },
        enums::quant::QuantRuntimeMode,
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
        let event = CoreEvent::SystemStatusChanged(SystemStatus::report_only_bootstrap(
            QuantRuntimeMode::ReportOnly,
        ));
        let (key, envelope) = event_envelope(&event).expect("status maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::SystemStatus));
        assert_eq!(envelope.kind.as_str(), "system.status");
    }
}
