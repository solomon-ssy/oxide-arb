//! Cross-subsystem event bus payload + non-blocking publisher.
//!
//! Lives in the shared model crate so both `oxide-arb-core` (hot-path producers)
//! and `oxide-arb-web` (governance control-plane producers + the WebSocket
//! broadcaster consumer) can use it without a circular dependency.

use crate::{
    domain::control_factor::ControlFactorMaterializationRunInfo,
    domain::{MarketBookView, Opportunity, PositionInfo, SystemStatus, TradeInfo},
    enums::{
        common::{AlertCategory, AlertLevel, AlertSource, TradeBusinessOutcome},
        control_factor::PublicationMode,
    },
    types::{MarketId, TradeId, Usd},
};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Structured `system.alert` payload shared by alert routing and WebSocket
/// clients.
///
/// `affects_trading` is the explicit boundary used by clients to distinguish
/// trading degradation from non-money-critical operator notices. `visible_toast`
/// controls transient UI popups; notification centers may still retain the
/// alert for auditability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemAlertEvent {
    pub idempotency_key: String,
    pub level: AlertLevel,
    pub category: AlertCategory,
    pub source: AlertSource,
    pub title: String,
    pub message: String,
    pub affects_trading: bool,
    pub visible_toast: bool,
    pub dedupe_secs: u64,
}

/// Real-time event emitted by core subsystems and the governance control plane,
/// consumed by the WebSocket broadcaster and fanned out to subscribers.
///
/// Every variant has exactly one wire channel (see `domain::ws::mapping`) and a
/// stable [`Self::kind`] label used for drop accounting. Hot-path producers
/// (scanner / post-trade / settlement / risk) publish through the non-blocking
/// [`CoreEventPublisher`]; the governance control plane publishes the
/// control / config / system / alert variants.
#[derive(Debug, Clone)]
pub enum CoreEvent {
    /// A scored opportunity was detected (projected to the public `Opportunity`).
    OpportunityDetected(Opportunity),
    /// A trade reached its terminal fill outcome (real DB-projected `TradeInfo`).
    TradeFilled(TradeInfo),
    /// A trade's position was settled after market resolution + redemption.
    TradeSettled {
        trade_id: TradeId,
        outcome: TradeBusinessOutcome,
        pnl: Usd,
    },
    /// Realized-`PnL` change: `daily` is the current trading day, `total` is the
    /// lifetime cumulative realized `PnL` (same accounting basis as `daily`).
    PnlUpdate {
        daily: Usd,
        total: Usd,
    },
    SystemStatusChanged(SystemStatus),
    CircuitBreakerTripped {
        level: u8,
        reason: String,
    },
    PositionChanged(PositionInfo),
    /// Throttled, coalesced per-market order-book update emitted by the
    /// `BookUpdateCoalescer` (never on the detection hot path) for markets a
    /// dashboard is actively watching.
    MarketBookUpdate {
        market_id: MarketId,
        view: Box<MarketBookView>,
    },
    MarketResolved {
        market_id: MarketId,
        outcome: bool,
    },
    ControlPublished {
        publication_id: String,
        mode: PublicationMode,
    },
    ConfigActivated {
        version_id: String,
    },
    Alert(SystemAlertEvent),
    /// Materialization / replay run status changed (queued → running → terminal).
    MaterializationRunUpdated(ControlFactorMaterializationRunInfo),
}

impl CoreEvent {
    /// Stable, allocation-free label for this event kind.
    ///
    /// Matches the wire channel name (`domain::ws::channel::WsChannel::as_str`)
    /// so drop metrics and fan-out share one vocabulary.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::OpportunityDetected(_) => "opportunity.detected",
            Self::TradeFilled(_) => "trade.filled",
            Self::TradeSettled { .. } => "trade.settled",
            Self::PnlUpdate { .. } => "pnl.update",
            Self::SystemStatusChanged(_) => "system.status",
            Self::CircuitBreakerTripped { .. } => "risk.circuit_breaker",
            Self::PositionChanged(_) => "risk.position_update",
            Self::MarketBookUpdate { .. } => "market.book_update",
            Self::MarketResolved { .. } => "market.resolved",
            Self::ControlPublished { .. } => "control.published",
            Self::ConfigActivated { .. } => "config.activated",
            Self::Alert(_) => "system.alert",
            Self::MaterializationRunUpdated(_) => "materialization.run_update",
        }
    }
}

/// Per-kind drop observer for the event bus.
///
/// Invoked with [`CoreEvent::kind`] each time an event is dropped on a
/// full/disconnected channel. Wired by the core process to a labeled Prometheus
/// counter (`oxide_arb_ws_event_dropped_total{kind}`).
pub type DropObserver = Arc<dyn Fn(&'static str) + Send + Sync>;

/// Non-blocking producer handle for [`CoreEvent`]s.
///
/// Backed by a bounded channel: on a full channel the event is dropped and a
/// counter incremented rather than blocking the producer. This is a hard
/// invariant — the money-critical hot path must never block on event emission.
#[derive(Clone)]
pub struct CoreEventPublisher {
    tx: flume::Sender<CoreEvent>,
    on_drop: Option<DropObserver>,
    dropped: Arc<AtomicU64>,
}

impl CoreEventPublisher {
    /// Create a publisher together with its bounded receiver.
    #[must_use]
    pub fn bounded(capacity: usize) -> (Self, flume::Receiver<CoreEvent>) {
        let (tx, rx) = flume::bounded(capacity);
        (
            Self {
                tx,
                on_drop: None,
                dropped: Arc::new(AtomicU64::new(0)),
            },
            rx,
        )
    }

    /// Attach a per-kind drop observer (consumed builder-style at wiring time).
    #[must_use]
    pub fn with_drop_hook(mut self, observer: DropObserver) -> Self {
        self.on_drop = Some(observer);
        self
    }

    /// Publish an event without ever blocking. Drops and counts on a full or
    /// disconnected channel, invoking the per-kind drop observer.
    pub fn publish(&self, event: CoreEvent) {
        let kind = event.kind();
        if self.tx.try_send(event).is_err() {
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if let Some(observer) = &self.on_drop {
                observer(kind);
            }
            tracing::warn!(dropped, kind, "core event channel full; dropping event");
        }
    }

    /// Total events dropped because the channel was full.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::{CoreEvent, CoreEventPublisher, SystemAlertEvent};
    use crate::{
        enums::common::{AlertCategory, AlertLevel, AlertSource},
        types::Usd,
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    fn alert() -> CoreEvent {
        CoreEvent::Alert(SystemAlertEvent {
            idempotency_key: "test.alert".to_owned(),
            level: AlertLevel::Warning,
            category: AlertCategory::OperatorNotice,
            source: AlertSource::System,
            title: "test".to_owned(),
            message: "x".to_owned(),
            affects_trading: false,
            visible_toast: false,
            dedupe_secs: 60,
        })
    }

    #[test]
    fn kind_matches_wire_channel_names() {
        assert_eq!(alert().kind(), "system.alert");
        assert_eq!(
            CoreEvent::PnlUpdate {
                daily: Usd::ZERO,
                total: Usd::ZERO
            }
            .kind(),
            "pnl.update"
        );
    }

    #[test]
    fn publish_never_blocks_and_drops_on_full_channel() {
        // Capacity 1: the second publish must drop instead of blocking, and the
        // drop counter must advance — the hard non-blocking invariant.
        let (publisher, rx) = CoreEventPublisher::bounded(1);
        publisher.publish(alert());
        publisher.publish(alert());
        assert_eq!(publisher.dropped(), 1, "second publish dropped");
        assert!(rx.try_recv().is_ok(), "first event buffered");
        assert!(rx.try_recv().is_err(), "only one event buffered");
    }

    #[test]
    fn drop_hook_receives_per_kind_label() {
        let hits = Arc::new(AtomicUsize::new(0));
        let last: Arc<Mutex<Option<&'static str>>> = Arc::new(Mutex::new(None));
        let hits_hook = Arc::clone(&hits);
        let last_hook = Arc::clone(&last);
        // Keep `rx` alive so the channel stays connected: the first publish
        // buffers, only the capacity-overflow second publish drops.
        let (publisher, _rx) = CoreEventPublisher::bounded(1);
        let publisher = publisher.with_drop_hook(Arc::new(move |kind| {
            hits_hook.fetch_add(1, Ordering::Relaxed);
            *last_hook.lock().expect("hook mutex") = Some(kind);
        }));
        publisher.publish(alert());
        publisher.publish(alert());
        assert_eq!(hits.load(Ordering::Relaxed), 1, "hook fired once on drop");
        assert_eq!(*last.lock().expect("assert mutex"), Some("system.alert"));
    }
}
