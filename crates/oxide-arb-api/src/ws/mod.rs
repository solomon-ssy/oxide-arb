//! Sharded WebSocket connection manager for Polymarket CLOB.
//!
//! Each shard manages one SDK WebSocket connection to Polymarket,
//! subscribing to up to `max_subscriptions_per_connection` tokens.
//! All events are normalized into [`PipelineEvent`] and dispatched to a
//! unified bounded channel.

mod drop_hook;
mod health;
mod ingest_hooks;
pub mod normalize;
mod reconnect;
mod router;
mod shard;
mod token_intern;

pub use drop_hook::WsEventDropHook;
pub use health::{ShardHealthBoard, ShardHealthSummary};
pub use ingest_hooks::BookLevelRejectHook;
pub use reconnect::ReconnectPolicy;
pub use token_intern::{TOKEN_INTERN, TokenInternPool, intern_str, intern_u256};

use crate::infra::retry::RetryPolicy;
use flume::Receiver;
use num_traits::ToPrimitive;
use oxide_arb_models::{
    config::{PolymarketConfig, WebSocketConfig},
    domain::pipeline::PipelineEvent,
    types::TokenId,
};
use router::ShardRouter;
use shard::ShardDeps;
use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

/// Who requested a token subscription.
///
/// The CLOB transport is shared between the trading engine (which subscribes to
/// the tokens it is actively trading) and the web control plane (which lets an
/// operator watch arbitrary markets). Tagging every subscription with its source
/// lets the manager refcount the union so the web plane can never unsubscribe a
/// token the engine still depends on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionSource {
    /// The trading engine baseline (protected — never removed by the web plane).
    Engine,
    /// The web control plane overlay (operator dashboard book watching).
    Web,
}

/// Per-source desired token sets, used to refcount the transport subscription.
///
/// The underlying transport is subscribed to a token iff **at least one** source
/// holds it; transport subscribe/unsubscribe is driven only by `0 ↔ 1` union
/// transitions, so each source can add/remove its own tokens without disturbing
/// the other's baseline.
#[derive(Default)]
struct SubscriptionState {
    engine: HashSet<TokenId>,
    web: HashSet<TokenId>,
}

impl SubscriptionState {
    /// Split borrow: `(this source's set, the other source's set)`.
    const fn split(
        &mut self,
        source: SubscriptionSource,
    ) -> (&mut HashSet<TokenId>, &HashSet<TokenId>) {
        match source {
            SubscriptionSource::Engine => (&mut self.engine, &self.web),
            SubscriptionSource::Web => (&mut self.web, &self.engine),
        }
    }

    /// Add `tokens` to `source`; returns the tokens that newly entered the union
    /// (and therefore must be subscribed on the transport).
    fn add(&mut self, source: SubscriptionSource, tokens: &[TokenId]) -> Vec<TokenId> {
        let (target, other) = self.split(source);
        let mut newly = Vec::new();
        for token in tokens {
            let in_union = target.contains(token) || other.contains(token);
            if target.insert(token.clone()) && !in_union {
                newly.push(token.clone());
            }
        }
        newly
    }

    /// Remove `tokens` from `source`; returns the tokens that left the union
    /// entirely (and therefore must be unsubscribed on the transport). Tokens the
    /// other source still holds are retained.
    fn remove(&mut self, source: SubscriptionSource, tokens: &[TokenId]) -> Vec<TokenId> {
        let (target, other) = self.split(source);
        let mut gone = Vec::new();
        for token in tokens {
            if target.remove(token) && !other.contains(token) {
                gone.push(token.clone());
            }
        }
        gone
    }

    /// Replace `source`'s set with `desired`; returns `(to_subscribe, to_unsubscribe)`
    /// reflecting only union `0 ↔ 1` transitions (the other source is protected).
    fn sync(
        &mut self,
        source: SubscriptionSource,
        desired: HashSet<TokenId>,
    ) -> (Vec<TokenId>, Vec<TokenId>) {
        let (target, other) = self.split(source);
        let to_subscribe: Vec<TokenId> = desired
            .iter()
            .filter(|token| !target.contains(*token) && !other.contains(*token))
            .cloned()
            .collect();
        let to_unsubscribe: Vec<TokenId> = target
            .iter()
            .filter(|token| !desired.contains(*token) && !other.contains(*token))
            .cloned()
            .collect();
        *target = desired;
        (to_subscribe, to_unsubscribe)
    }
}

/// Upper bound on simultaneous connection establishments across all shards.
/// Bounds the TLS-handshake burst after a catalog sync or network flap so the
/// herd cannot overwhelm a proxy (e.g. TUN) or trip server-side rate limits.
const MAX_CONCURRENT_CONNECTS: usize = 4;

/// Manages sharded WebSocket connections to Polymarket CLOB.
///
/// Each shard handles up to `max_subscriptions_per_connection` tokens.
/// Messages are normalized and dispatched to a unified output channel.
pub struct ClobWsManager {
    router: ShardRouter,
    output_rx: Receiver<PipelineEvent>,
    ws_url: String,
    last_message_at: Arc<parking_lot::Mutex<Option<Instant>>>,
    subscriptions: parking_lot::Mutex<SubscriptionState>,
    health: Arc<ShardHealthBoard>,
}

impl ClobWsManager {
    /// Create a new manager. Shard actors are spawned on first `subscribe` call
    /// and stay resident until shutdown.
    pub fn new(
        polymarket_config: &PolymarketConfig,
        ws_config: &WebSocketConfig,
        shutdown: tokio_util::sync::CancellationToken,
        on_events_dropped: Option<WsEventDropHook>,
        on_book_level_rejected: Option<BookLevelRejectHook>,
    ) -> Self {
        let (output_tx, output_rx) = flume::bounded(8192);
        let last_message_at = Arc::new(parking_lot::Mutex::new(None));
        let health = Arc::new(ShardHealthBoard::default());
        // `[market_data.websocket]` reconnect knobs drive both backoff layers:
        // the shard loop and the SDK-internal reconnect.
        let initial_backoff = Duration::from_millis(ws_config.reconnect_delay_ms.max(1));
        let max_backoff = Duration::from_millis(
            ws_config
                .max_reconnect_delay_ms
                .max(ws_config.reconnect_delay_ms),
        );
        let reconnect_policy = ReconnectPolicy::new(RetryPolicy {
            max_attempts: None,
            initial_interval_ms: ws_config.reconnect_delay_ms.max(1),
            max_interval_ms: ws_config
                .max_reconnect_delay_ms
                .max(ws_config.reconnect_delay_ms),
            randomization_factor: 0.2,
            multiplier: 2.0,
            max_elapsed_time_ms: None,
        });
        let router = ShardRouter::new(
            ws_config.max_subscriptions_per_connection,
            ShardDeps {
                output_tx,
                ws_url: polymarket_config.clob_ws_url.clone(),
                shutdown,
                last_message_at: Arc::clone(&last_message_at),
                on_events_dropped,
                on_book_level_rejected,
                reconnect_policy,
                sdk_initial_backoff: initial_backoff,
                sdk_max_backoff: max_backoff,
                connect_limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTS)),
                health: Arc::clone(&health),
            },
        );

        Self {
            router,
            output_rx,
            ws_url: polymarket_config.clob_ws_url.clone(),
            last_message_at,
            subscriptions: parking_lot::Mutex::new(SubscriptionState::default()),
            health,
        }
    }

    /// Subscribe `source` to orderbook updates for `tokens`.
    ///
    /// The transport is only told to subscribe to tokens that newly enter the
    /// union of all sources, so re-subscribing a token another source already
    /// holds is a no-op on the wire.
    pub fn subscribe_tokens(&self, source: SubscriptionSource, tokens: &[TokenId]) {
        let newly = self.subscriptions.lock().add(source, tokens);
        if !newly.is_empty() {
            self.router.assign_tokens(&newly);
        }
    }

    /// Unsubscribe `source` from `tokens`.
    ///
    /// A token is removed from the transport only when **no** source still holds
    /// it — so the web control plane can drop its overlay without ever tearing
    /// down a token the trading engine baseline depends on.
    pub fn unsubscribe_tokens(&self, source: SubscriptionSource, tokens: &[TokenId]) {
        let gone = self.subscriptions.lock().remove(source, tokens);
        if !gone.is_empty() {
            self.router.remove_tokens(&gone);
        }
    }

    /// Replace `source`'s desired token set with `tokens`, reconciling the
    /// transport against the new union (the other source's baseline is protected).
    pub fn sync_tokens(&self, source: SubscriptionSource, tokens: &[TokenId]) {
        let desired: HashSet<TokenId> = tokens.iter().cloned().collect();
        let (to_subscribe, to_unsubscribe) = self.subscriptions.lock().sync(source, desired);
        if !to_subscribe.is_empty() {
            self.router.assign_tokens(&to_subscribe);
        }
        if !to_unsubscribe.is_empty() {
            self.router.remove_tokens(&to_unsubscribe);
        }
    }

    /// Number of tokens a given source currently holds (diagnostics / tests).
    #[must_use]
    pub fn source_subscription_count(&self, source: SubscriptionSource) -> usize {
        let mut state = self.subscriptions.lock();
        state.split(source).0.len()
    }

    /// Returns the unified event receiver for all shards.
    pub const fn events(&self) -> &Receiver<PipelineEvent> {
        &self.output_rx
    }

    /// Returns the number of active shards.
    pub fn shard_count(&self) -> usize {
        self.router.shard_count()
    }

    /// Aggregated per-shard connection health (operator summaries).
    #[must_use]
    pub fn shard_health(&self) -> ShardHealthSummary {
        self.health.summary()
    }

    /// Get the WebSocket URL.
    pub fn ws_url(&self) -> &str {
        &self.ws_url
    }

    /// Milliseconds since last WS message received from any shard.
    pub fn last_message_age_ms(&self) -> Option<u64> {
        self.last_message_at
            .lock()
            .and_then(|ts| ToPrimitive::to_u64(&ts.elapsed().as_millis()))
    }

    /// Mark WS as connected for integration tests (no live socket required).
    #[doc(hidden)]
    pub fn seed_test_connectivity(&self) {
        *self.last_message_at.lock() = Some(Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::{SubscriptionSource, SubscriptionState};
    use oxide_arb_models::types::TokenId;
    use std::collections::HashSet;

    fn tok(s: &str) -> TokenId {
        TokenId::new(s)
    }

    #[test]
    fn first_subscriber_drives_transport_subscribe() {
        let mut state = SubscriptionState::default();
        let newly = state.add(SubscriptionSource::Engine, &[tok("a"), tok("b")]);
        assert_eq!(newly.len(), 2, "both tokens newly enter the union");
    }

    #[test]
    fn overlapping_source_is_noop_on_the_wire() {
        let mut state = SubscriptionState::default();
        state.add(SubscriptionSource::Engine, &[tok("a"), tok("b")]);
        // Web overlaps `b` and adds `c`; only `c` newly enters the union.
        let newly = state.add(SubscriptionSource::Web, &[tok("b"), tok("c")]);
        assert_eq!(newly, vec![tok("c")]);
    }

    #[test]
    fn web_unsubscribe_never_drops_an_engine_token() {
        let mut state = SubscriptionState::default();
        state.add(SubscriptionSource::Engine, &[tok("a"), tok("b")]);
        state.add(SubscriptionSource::Web, &[tok("b"), tok("c")]);

        // Web drops everything it added. `b` stays (engine baseline), only `c`
        // leaves the union and is torn down on the transport.
        let gone = state.remove(SubscriptionSource::Web, &[tok("b"), tok("c")]);
        assert_eq!(gone, vec![tok("c")]);

        // Engine still holds both of its tokens.
        assert_eq!(state.split(SubscriptionSource::Engine).0.len(), 2);
    }

    #[test]
    fn web_cannot_remove_a_token_it_never_held() {
        let mut state = SubscriptionState::default();
        state.add(SubscriptionSource::Engine, &[tok("a")]);
        let gone = state.remove(SubscriptionSource::Web, &[tok("a")]);
        assert!(gone.is_empty(), "web never held `a`, transport untouched");
        assert!(
            state
                .split(SubscriptionSource::Engine)
                .0
                .contains(&tok("a"))
        );
    }

    #[test]
    fn engine_sync_protects_the_web_overlay() {
        let mut state = SubscriptionState::default();
        state.add(SubscriptionSource::Engine, &[tok("a"), tok("b")]);
        state.add(SubscriptionSource::Web, &[tok("b"), tok("c")]);

        // Engine reconciles to {a}: it drops `b` from its own set, but `b` stays
        // on the wire because the web overlay still holds it; only engine-only
        // tokens would be removed (none here besides what web protects).
        let desired: HashSet<TokenId> = std::iter::once(tok("a")).collect();
        let (to_subscribe, to_unsubscribe) = state.sync(SubscriptionSource::Engine, desired);
        assert!(to_subscribe.is_empty());
        assert!(
            to_unsubscribe.is_empty(),
            "`b` is protected by the web overlay; `a` stays"
        );
    }
}
