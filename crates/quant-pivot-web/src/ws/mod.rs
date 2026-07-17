//! WebSocket real-time push infrastructure.
//!
//! - [`handler`] consumes a short-lived single-use ticket from the negotiated
//!   `Sec-WebSocket-Protocol` during upgrade;
//! - [`session`] runs the per-connection loop (subscriptions, heartbeat, sync);
//! - [`protocol`] defines the envelope + client command grammar;
//! - [`SessionRegistry`] + [`spawn_ws_broadcaster`] fan `CoreEvent`s out to the
//!   sessions subscribed to each event's channel.
//!
//! The broadcaster is spawned by `quant-pivot-core` (so it joins the unified
//! staged shutdown) over the shared [`SessionRegistry`] held in `AppState`.

pub mod handler;
pub mod session;

use dashmap::DashMap;
use quant_pivot_models::{
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
    /// Authenticated subject owning the socket.
    pub subject: String,
    /// Refresh-session family owning the socket.
    pub family_id: String,
    /// Whether this session may receive always-on control-plane events.
    pub can_read_system: bool,
    /// Immediate lifecycle cancellation on logout, replay, or account disable.
    pub cancellation: CancellationToken,
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

    /// Close every live socket issued from one refresh-session family.
    pub fn close_family(&self, family_id: &str) {
        for entry in self.sessions.iter() {
            if entry.value().family_id == family_id {
                entry.value().cancellation.cancel();
            }
        }
    }

    /// Close every live socket owned by a user whose account was disabled or deleted.
    pub fn close_subject(&self, subject: &str) {
        for entry in self.sessions.iter() {
            if entry.value().subject == subject {
                entry.value().cancellation.cancel();
            }
        }
    }

    /// Close all sockets after a global RBAC policy revision changes.
    pub fn close_all(&self) {
        for entry in self.sessions.iter() {
            entry.value().cancellation.cancel();
        }
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

    /// Deliver one event frame according to its subscription key.
    ///
    /// System status + alerts are always-on: every connected session receives
    /// them without an explicit subscribe. Every other channel — including
    /// market-scoped `market.book_update` keys — is delivered only to sessions
    /// holding the exact [`SubscriptionKey`] (channel + optional market).
    pub fn fanout_event(&self, key: &SubscriptionKey, text: &str) {
        if matches!(
            key.channel,
            WsChannel::SystemStatus | WsChannel::SystemAlert
        ) {
            for entry in self.sessions.iter() {
                if entry.value().can_read_system {
                    let _ = entry.value().outbound.try_send(text.to_owned());
                }
            }
            return;
        }
        self.fanout(key, text);
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
                    if key.channel == WsChannel::MarketBookUpdate
                        && let Some(market) = &key.market
                    {
                        markets.insert(market.clone());
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
                    registry.fanout_event(&key, &envelope.to_text());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SessionHandle, SessionRegistry};
    use quant_pivot_models::{
        domain::{SubscriptionKey, WsChannel},
        types::MarketId,
    };
    use std::{
        collections::HashSet,
        sync::{Arc, RwLock},
    };

    fn handle_with(keys: Vec<SubscriptionKey>) -> (SessionHandle, flume::Receiver<String>) {
        let (outbound, rx) = flume::bounded::<String>(8);
        let subscriptions: HashSet<SubscriptionKey> = keys.into_iter().collect();
        (
            SessionHandle {
                outbound,
                subscriptions: Arc::new(RwLock::new(subscriptions)),
                subject: "test-user".to_owned(),
                family_id: "test-family".to_owned(),
                can_read_system: true,
                cancellation: tokio_util::sync::CancellationToken::new(),
            },
            rx,
        )
    }

    fn book(market: &str) -> SubscriptionKey {
        SubscriptionKey::scoped(WsChannel::MarketBookUpdate, MarketId::new(market))
    }

    #[test]
    fn subscribed_markets_extracts_only_book_update_markets() {
        let registry = SessionRegistry::default();
        let (first, _rx1) = handle_with(vec![
            SubscriptionKey::global(WsChannel::SystemStatus),
            book("0xaaa"),
            book("0xbbb"),
        ]);
        registry.register(first);
        let (second, _rx2) = handle_with(vec![
            book("0xaaa"),
            SubscriptionKey::global(WsChannel::QuantReport),
        ]);
        registry.register(second);

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
        let (handle, _rx) = handle_with(vec![
            SubscriptionKey::global(WsChannel::SystemStatus),
            SubscriptionKey::global(WsChannel::QuantReport),
        ]);
        registry.register(handle);
        assert!(registry.subscribed_markets().is_empty());
    }

    #[test]
    fn fanout_event_delivers_scoped_book_update_to_matching_market_only() {
        let registry = SessionRegistry::default();
        let (watcher, watcher_rx) = handle_with(vec![book("0xaaa")]);
        registry.register(watcher);
        let (other_market, other_market_rx) = handle_with(vec![book("0xbbb")]);
        registry.register(other_market);
        let (global_only, global_only_rx) =
            handle_with(vec![SubscriptionKey::global(WsChannel::QuantReport)]);
        registry.register(global_only);

        registry.fanout_event(&book("0xaaa"), "book-frame");

        assert_eq!(watcher_rx.try_recv().as_deref(), Ok("book-frame"));
        assert!(
            other_market_rx.try_recv().is_err(),
            "different market must not receive scoped frame"
        );
        assert!(
            global_only_rx.try_recv().is_err(),
            "session without book subscription must not receive scoped frame"
        );
    }

    #[test]
    fn fanout_event_global_channel_requires_subscription() {
        let registry = SessionRegistry::default();
        let (subscribed, subscribed_rx) =
            handle_with(vec![SubscriptionKey::global(WsChannel::MarketResolved)]);
        registry.register(subscribed);
        let (unsubscribed, unsubscribed_rx) = handle_with(vec![book("0xaaa")]);
        registry.register(unsubscribed);

        registry.fanout_event(
            &SubscriptionKey::global(WsChannel::MarketResolved),
            "resolved-frame",
        );

        assert_eq!(subscribed_rx.try_recv().as_deref(), Ok("resolved-frame"));
        assert!(unsubscribed_rx.try_recv().is_err());
    }

    #[test]
    fn fanout_event_always_on_channels_reach_every_session() {
        let registry = SessionRegistry::default();
        let (with_subs, with_subs_rx) = handle_with(vec![book("0xaaa")]);
        registry.register(with_subs);
        let (bare, bare_rx) = handle_with(vec![]);
        registry.register(bare);

        registry.fanout_event(
            &SubscriptionKey::global(WsChannel::SystemStatus),
            "status-frame",
        );

        assert_eq!(with_subs_rx.try_recv().as_deref(), Ok("status-frame"));
        assert_eq!(bare_rx.try_recv().as_deref(), Ok("status-frame"));
    }

    #[test]
    fn close_all_cancels_every_registered_session() {
        let registry = SessionRegistry::default();
        let (first, _first_rx) = handle_with(vec![]);
        let first_cancel = first.cancellation.clone();
        registry.register(first);
        let (second, _second_rx) = handle_with(vec![]);
        let second_cancel = second.cancellation.clone();
        registry.register(second);

        registry.close_all();

        assert!(first_cancel.is_cancelled());
        assert!(second_cancel.is_cancelled());
    }
}
