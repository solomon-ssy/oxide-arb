//! In-process runtime event bus for control-plane notifications.

use crate::{
    domain::{MarketBookView, governance::system::SystemStatus},
    enums::common::{AlertCategory, AlertLevel, AlertSource},
    types::MarketId,
};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Operator-facing alert payload published on the core event bus.
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

/// Cross-subsystem runtime events consumed by web and observability layers.
#[derive(Debug, Clone)]
pub enum CoreEvent {
    SystemStatusChanged(SystemStatus),
    MarketBookUpdate {
        market_id: MarketId,
        view: Box<MarketBookView>,
    },
    MarketResolved {
        market_id: MarketId,
        outcome: bool,
    },
    ConfigActivated {
        version_id: String,
    },
    Alert(SystemAlertEvent),
}

impl CoreEvent {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::SystemStatusChanged(_) => "system.status",
            Self::MarketBookUpdate { .. } => "market.book_update",
            Self::MarketResolved { .. } => "market.resolved",
            Self::ConfigActivated { .. } => "config.activated",
            Self::Alert(_) => "system.alert",
        }
    }
}

pub type DropObserver = Arc<dyn Fn(&'static str) + Send + Sync>;

/// Non-blocking publisher for [`CoreEvent`] with drop counting.
#[derive(Clone)]
pub struct CoreEventPublisher {
    tx: flume::Sender<CoreEvent>,
    on_drop: Option<DropObserver>,
    dropped: Arc<AtomicU64>,
}

impl CoreEventPublisher {
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

    #[must_use]
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}
