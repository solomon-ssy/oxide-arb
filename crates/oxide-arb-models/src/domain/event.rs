//! Cross-subsystem event bus payload + non-blocking publisher.
//!
//! Lives in the shared model crate so both `oxide-arb-core` (hot-path producers)
//! and `oxide-arb-web` (governance control-plane producers + the WebSocket
//! broadcaster consumer) can use it without a circular dependency.

use crate::{
    domain::{MarketBookView, Opportunity, PositionInfo, SystemStatus, TradeInfo},
    enums::{
        common::{AlertLevel, TradeBusinessOutcome},
        control_factor::PublicationMode,
    },
    types::{MarketId, OpportunityId, TradeId, Usd},
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Real-time event emitted by core subsystems and the governance control plane,
/// consumed by the WebSocket broadcaster and fanned out to subscribers.
///
/// Hot-path variants (`OpportunityDetected`, `Trade*`, `PnlUpdate`,
/// `PositionChanged`) are defined here but only emitted once Phase 6.7 wires the
/// detection / execution / post-trade instrumentation. Phase 6.6 emits the
/// non-hot-path variants (control / config / alert / system / risk).
#[derive(Debug, Clone)]
pub enum CoreEvent {
    OpportunityDetected(Opportunity),
    OpportunityExpired(OpportunityId),
    TradeOpened(TradeInfo),
    TradeFilled(TradeInfo),
    TradeSettled {
        trade_id: TradeId,
        outcome: TradeBusinessOutcome,
        pnl: Usd,
    },
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
    Alert {
        level: AlertLevel,
        message: String,
    },
}

/// Non-blocking producer handle for [`CoreEvent`]s.
///
/// Backed by a bounded channel: on a full channel the event is dropped and a
/// counter incremented rather than blocking the producer. This is a hard
/// invariant — the money-critical hot path must never block on event emission.
#[derive(Clone)]
pub struct CoreEventPublisher {
    tx: flume::Sender<CoreEvent>,
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
                dropped: Arc::new(AtomicU64::new(0)),
            },
            rx,
        )
    }

    /// Publish an event without ever blocking. Drops and counts on a full or
    /// disconnected channel.
    pub fn publish(&self, event: CoreEvent) {
        if self.tx.try_send(event).is_err() {
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::warn!(dropped, "core event channel full; dropping event");
        }
    }

    /// Total events dropped because the channel was full.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}
