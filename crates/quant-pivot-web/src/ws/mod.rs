//! WebSocket real-time push infrastructure.
//!
//! - [`handler`] performs the upgrade with query-token authentication (fixing the
//!   ng-gateway unauthenticated-WS defect);
//! - [`session`] runs the per-connection loop (subscriptions, heartbeat, sync);
//! - [`protocol`] defines the envelope + client command grammar;
//! - [`SessionRegistry`] + [`spawn_ws_broadcaster`] fan `CoreEvent`s out to the
//!   sessions subscribed to each event's channel.
//!
//! The broadcaster is spawned by `oxide-arb-core` (so it joins the unified
//! staged shutdown) over the shared [`SessionRegistry`] held in `AppState`.

pub mod handler;
pub mod session;

use dashmap::DashMap;
use oxide_arb_models::{
    domain::{CoreEvent, SubscriptionKey, WsChannel, event_envelope},
    types::MarketId,
};
use std::{
    collections::HashSet,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio_util::sync::CancellationToken;

/// Per-connection identifier within the [`SessionRegistry`].
pub type SessionId = u64;

/// Broadcaster-visible handle to one live session: its outbound queue and the
/// set of subscription keys it is subscribed to.
#[derive(Clone)]
pub struct SessionHandle {
    /// Non-blocking outbound queue drained by the session task into the socket.
    pub outbound: flume::Sender<String>,
    /// Subscription keys this session holds (shared with the session task).
    pub subscriptions: Arc<RwLock<HashSet<SubscriptionKey>>>,
}

/// Concurrent registry of live WebSocket sessions, shared between the upgrade
/// handler (registration) and the broadcaster (fan-out).
#[derive(Clone, Default)]
pub struct SessionRegistry {
    sessions: Arc<DashMap<SessionId, SessionHandle>>,
    next_id: Arc<AtomicU64>,
}

impl SessionRegistry {
    /// Register a session, returning its assigned id.
    pub fn register(&self, handle: SessionHandle) -> SessionId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.sessions.insert(id, handle);
        id
    }

    /// Remove a session on disconnect.
    pub fn deregister(&self, id: SessionId) {
        self.sessions.remove(&id);
    }

    /// Send `text` to every session subscribed to `key` (drops on a full
    /// outbound queue so a slow client never blocks the broadcaster).
    pub fn fanout(&self, key: &SubscriptionKey, text: &str) {
        for entry in self.sessions.iter() {
            let subscribed = entry
                .value()
                .subscriptions
                .read()
                .is_ok_and(|set| set.contains(key));
            if subscribed {
                let _ = entry.value().outbound.try_send(text.to_owned());
            }
        }
    }

    /// Push operator-global frames to every connected session without requiring
    /// an explicit subscribe (system status + alerts are always-on).
    pub fn fanout_channel(&self, channel: WsChannel, text: &str) {
        if matches!(channel, WsChannel::SystemStatus | WsChannel::SystemAlert) {
            for entry in self.sessions.iter() {
                let _ = entry.value().outbound.try_send(text.to_owned());
            }
            return;
        }
        self.fanout(&SubscriptionKey::global(channel), text);
    }

    /// Number of live sessions (diagnostics / tests).
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Distinct markets currently subscribed on the
    /// [`WsChannel::MarketBookUpdate`] channel across all live sessions.
    ///
    /// The core book-update coalescer polls this each tick so it only emits a
    /// `MarketBookUpdate` for markets a dashboard is actively watching — markets
    /// with no subscriber never reach the bounded event bus.
    #[must_use]
    pub fn subscribed_markets(&self) -> HashSet<MarketId> {
        let mut markets = HashSet::new();
        for entry in self.sessions.iter() {
            if let Ok(subscriptions) = entry.value().subscriptions.read() {
                for key in subscriptions.iter() {
                    if key.channel == WsChannel::MarketBookUpdate {
                        if let Some(market) = &key.market {
                            markets.insert(market.clone());
                        }
                    }
                }
            }
        }
        markets
    }
}

/// Consume `CoreEvent`s and fan each out to its subscribed sessions until
/// `shutdown` is cancelled or the bus is closed.
pub async fn spawn_ws_broadcaster(
    rx: flume::Receiver<CoreEvent>,
    registry: SessionRegistry,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                tracing::info!("ws broadcaster shutting down");
                return;
            }
            event = rx.recv_async() => {
                let Ok(event) = event else {
                    tracing::info!("ws broadcaster event bus closed");
                    return;
                };
                if let Some((key, envelope)) = event_envelope(&event) {
                    registry.fanout_channel(key.channel, &envelope.to_text());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SessionHandle, SessionRegistry};
    use oxide_arb_models::{
        domain::{SubscriptionKey, WsChannel},
        types::MarketId,
    };
    use std::{
        collections::HashSet,
        sync::{Arc, RwLock},
    };

    fn handle_with(keys: Vec<SubscriptionKey>) -> SessionHandle {
        let (outbound, _rx) = flume::bounded::<String>(8);
        let subscriptions: HashSet<SubscriptionKey> = keys.into_iter().collect();
        SessionHandle {
            outbound,
            subscriptions: Arc::new(RwLock::new(subscriptions)),
        }
    }

    fn book(market: &str) -> SubscriptionKey {
        SubscriptionKey::scoped(WsChannel::MarketBookUpdate, MarketId::new(market))
    }

    #[test]
    fn subscribed_markets_extracts_only_book_update_markets() {
        let registry = SessionRegistry::default();
        registry.register(handle_with(vec![
            SubscriptionKey::global(WsChannel::SystemStatus),
            book("0xaaa"),
            book("0xbbb"),
        ]));
        registry.register(handle_with(vec![
            book("0xaaa"),
            SubscriptionKey::global(WsChannel::PnlUpdate),
        ]));

        let markets = registry.subscribed_markets();
        assert_eq!(
            markets.len(),
            2,
            "deduped across sessions, non-book keys ignored"
        );
        assert!(markets.contains(&MarketId::new("0xaaa")));
        assert!(markets.contains(&MarketId::new("0xbbb")));
    }

    #[test]
    fn subscribed_markets_empty_without_book_subscriptions() {
        let registry = SessionRegistry::default();
        registry.register(handle_with(vec![
            SubscriptionKey::global(WsChannel::SystemStatus),
            SubscriptionKey::global(WsChannel::PnlUpdate),
        ]));
        assert!(registry.subscribed_markets().is_empty());
    }
}
