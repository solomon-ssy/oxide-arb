//! Sharded WebSocket connection manager for Polymarket CLOB.
//!
//! Each shard manages one SDK WebSocket connection to Polymarket,
//! subscribing to up to `max_subscriptions_per_connection` tokens.
//! All events are normalized into [`PipelineEvent`] and dispatched to a
//! unified bounded channel.

mod health;
mod ingest_hooks;
pub mod normalize;
mod reconnect;
mod router;
mod session_hook;
mod shard;
mod token_intern;

use std::{
    collections::HashSet,
    slice,
    sync::Arc,
    time::{Duration, Instant},
};

use flume::Receiver;
pub use health::{ShardHealthBoard, ShardHealthSummary, TokenFreshnessBoard, WsShardHealthPort};
pub use ingest_hooks::BookLevelRejectHook;
use num_traits::ToPrimitive;
use parking_lot::Mutex;
use quant_pivot_models::{
    config::{PolymarketConfig, WebSocketConfig},
    domain::data_plane::pipeline::PipelineEvent,
    types::TokenId,
};
pub use reconnect::ReconnectPolicy;
use router::ShardRouter;
pub use session_hook::WsSessionInvalidationHook;
use shard::ShardDeps;
pub use token_intern::{TOKEN_INTERN, TokenInternPool, intern_str, intern_u256};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::infra::retry::RetryPolicy;

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

    fn active_tokens(&self) -> HashSet<TokenId> {
        self.engine.union(&self.web).cloned().collect()
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
    last_message_at: Arc<Mutex<Option<Instant>>>,
    subscriptions: Mutex<SubscriptionState>,
    health: Arc<ShardHealthBoard>,
    token_freshness: Arc<TokenFreshnessBoard>,
}

impl ClobWsManager {
    /// Create a new manager. Shard actors are spawned on first `subscribe` call
    /// and stay resident until shutdown.
    pub fn new(
        polymarket_config: &PolymarketConfig,
        ws_config: &WebSocketConfig,
        shutdown: CancellationToken,
        on_session_invalidated: Option<WsSessionInvalidationHook>,
        on_book_level_rejected: Option<BookLevelRejectHook>,
    ) -> Self {
        let (output_tx, output_rx) = flume::bounded(8192);
        let last_message_at = Arc::new(Mutex::new(None));
        let health = Arc::new(ShardHealthBoard::default());
        let token_freshness = Arc::new(TokenFreshnessBoard::default());
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
                on_session_invalidated,
                on_book_level_rejected,
                reconnect_policy,
                sdk_initial_backoff: initial_backoff,
                sdk_max_backoff: max_backoff,
                connect_limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_CONNECTS)),
                health: Arc::clone(&health),
                token_freshness: Arc::clone(&token_freshness),
            },
        );

        Self {
            router,
            output_rx,
            ws_url: polymarket_config.clob_ws_url.clone(),
            last_message_at,
            subscriptions: Mutex::new(SubscriptionState::default()),
            health,
            token_freshness,
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
        let (gone, active_tokens) = {
            let mut subscriptions = self.subscriptions.lock();
            let gone = subscriptions.remove(source, tokens);
            (gone, subscriptions.active_tokens())
        };
        if !gone.is_empty() {
            self.router.remove_tokens(&gone);
            self.token_freshness.prune_tokens(&active_tokens);
        }
    }

    /// Replace `source`'s desired token set with `tokens`, reconciling the
    /// transport against the new union (the other source's baseline is protected).
    pub fn sync_tokens(&self, source: SubscriptionSource, tokens: &[TokenId]) {
        let desired: HashSet<TokenId> = tokens.iter().cloned().collect();
        let (to_subscribe, to_unsubscribe, active_tokens) = {
            let mut subscriptions = self.subscriptions.lock();
            let (to_subscribe, to_unsubscribe) = subscriptions.sync(source, desired);
            (to_subscribe, to_unsubscribe, subscriptions.active_tokens())
        };
        if !to_subscribe.is_empty() {
            self.router.assign_tokens(&to_subscribe);
        }
        if !to_unsubscribe.is_empty() {
            self.router.remove_tokens(&to_unsubscribe);
            self.token_freshness.prune_tokens(&active_tokens);
        }
    }

    /// Number of tokens a given source currently holds (diagnostics / tests).
    #[must_use]
    pub fn source_subscription_count(&self, source: SubscriptionSource) -> usize {
        let mut state = self.subscriptions.lock();
        state.split(source).0.len()
    }

    /// Subset of `tokens` currently live on the transport (union of all
    /// sources), resolved under a single lock so callers can label a whole
    /// page of markets consistently.
    #[must_use]
    pub fn subscribed_tokens(&self, tokens: &[TokenId]) -> HashSet<TokenId> {
        let state = self.subscriptions.lock();
        tokens
            .iter()
            .filter(|token| state.engine.contains(*token) || state.web.contains(*token))
            .cloned()
            .collect()
    }

    /// All tokens currently live on the transport (engine baseline ∪ web overlay).
    #[must_use]
    pub fn all_subscribed_tokens(&self) -> HashSet<TokenId> {
        self.subscriptions.lock().active_tokens()
    }

    /// Returns the unified event receiver for all shards.
    pub const fn events(&self) -> &Receiver<PipelineEvent> {
        &self.output_rx
    }

    /// Advance token freshness only after canonical persistence and book apply.
    pub fn mark_token_applied(&self, token_id: &TokenId, at: Instant) {
        self.token_freshness.mark_token(token_id, at);
    }

    /// Fail closed when a stream/session boundary invalidates token continuity.
    pub fn invalidate_token(&self, token_id: &TokenId) {
        self.invalidate_tokens(slice::from_ref(token_id));
    }

    /// Fail closed for a whole stream session and coalesce transport restarts by shard.
    pub fn invalidate_tokens(&self, token_ids: &[TokenId]) {
        self.token_freshness.invalidate_tokens(token_ids);
        self.router.restart_tokens(token_ids);
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

    /// Milliseconds since a token last produced a normalized WS event.
    #[must_use]
    pub fn token_message_age_ms(&self, token_id: &TokenId) -> Option<u64> {
        self.token_freshness.token_age_ms(token_id)
    }
}

impl WsShardHealthPort for ClobWsManager {
    fn shard_health(&self) -> ShardHealthSummary {
        self.health.summary()
    }

    fn last_message_age_ms(&self) -> Option<u64> {
        Self::last_message_age_ms(self)
    }

    fn token_message_age_ms(&self, token_id: &TokenId) -> Option<u64> {
        Self::token_message_age_ms(self, token_id)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, iter};

    use quant_pivot_models::types::TokenId;

    use super::{SubscriptionSource, SubscriptionState};

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
        let desired: HashSet<TokenId> = iter::once(tok("a")).collect();
        let (to_subscribe, to_unsubscribe) = state.sync(SubscriptionSource::Engine, desired);
        assert!(to_subscribe.is_empty());
        assert!(
            to_unsubscribe.is_empty(),
            "`b` is protected by the web overlay; `a` stays"
        );
    }
}
