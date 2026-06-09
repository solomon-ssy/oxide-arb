//! [`CoreEvent`] → wire-envelope projection.
//!
//! A single exhaustive `match` maps each event to its channel, optional market
//! scope, and JSON payload. The market-scoped `MarketBookUpdate` is just another
//! arm yielding `Some(market_id)`; [`SubscriptionKey::new`] then normalizes the
//! scope, so there is no special-case early return and no unreachable arm.

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
///
/// Returns `None` only if the payload fails to serialize, which never happens
/// for the well-formed domain types carried by [`CoreEvent`].
#[must_use]
pub fn event_envelope(event: &CoreEvent) -> Option<(SubscriptionKey, WsEnvelope)> {
    let (channel, market, data): (WsChannel, Option<MarketId>, Value) = match event {
        CoreEvent::SystemStatusChanged(status) => (
            WsChannel::SystemStatus,
            None,
            serde_json::to_value(status).ok()?,
        ),
        CoreEvent::Alert { level, message } => (
            WsChannel::SystemAlert,
            None,
            serde_json::json!({ "level": level, "message": message }),
        ),
        CoreEvent::CircuitBreakerTripped { level, reason } => (
            WsChannel::RiskCircuitBreaker,
            None,
            serde_json::json!({ "level": level, "reason": reason }),
        ),
        CoreEvent::PositionChanged(position) => (
            WsChannel::RiskPositionUpdate,
            None,
            serde_json::to_value(position).ok()?,
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
        CoreEvent::ControlPublished {
            publication_id,
            mode,
        } => (
            WsChannel::ControlPublished,
            None,
            serde_json::json!({ "publication_id": publication_id, "mode": mode }),
        ),
        CoreEvent::ConfigActivated { version_id } => (
            WsChannel::ConfigActivated,
            None,
            serde_json::json!({ "version_id": version_id }),
        ),
        CoreEvent::OpportunityDetected(opp) => (
            WsChannel::OpportunityDetected,
            None,
            serde_json::to_value(opp).ok()?,
        ),
        CoreEvent::OpportunityExpired(id) => (
            WsChannel::OpportunityExpired,
            None,
            serde_json::json!({ "opportunity_id": id }),
        ),
        CoreEvent::TradeOpened(trade) => (
            WsChannel::TradeOpened,
            None,
            serde_json::to_value(trade).ok()?,
        ),
        CoreEvent::TradeFilled(trade) => (
            WsChannel::TradeFilled,
            None,
            serde_json::to_value(trade).ok()?,
        ),
        CoreEvent::TradeSettled {
            trade_id,
            outcome,
            pnl,
        } => (
            WsChannel::TradeSettled,
            None,
            serde_json::json!({ "trade_id": trade_id, "outcome": outcome, "pnl": pnl }),
        ),
        CoreEvent::PnlUpdate { daily, total } => (
            WsChannel::PnlUpdate,
            None,
            serde_json::json!({ "daily": daily, "total": total }),
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
            CoreEvent, MarketBookView,
            ws::channel::{SubscriptionKey, WsChannel},
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
    fn circuit_breaker_tripped_maps_to_global_risk_key() {
        let event = CoreEvent::CircuitBreakerTripped {
            level: 3,
            reason: "daily loss cap".to_owned(),
        };
        let (key, envelope) = event_envelope(&event).expect("breaker maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::RiskCircuitBreaker));
        assert_eq!(envelope.kind.as_str(), "risk.circuit_breaker");
    }

    #[test]
    fn market_resolved_is_globally_scoped() {
        let event = CoreEvent::MarketResolved {
            market_id: MarketId::new("0xabc"),
            outcome: true,
        };
        let (key, _envelope) = event_envelope(&event).expect("resolved maps");
        assert_eq!(key, SubscriptionKey::global(WsChannel::MarketResolved));
        assert_eq!(key.market, None, "market rides in the payload, not the key");
    }
}
