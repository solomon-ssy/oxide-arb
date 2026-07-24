//! Sharded WebSocket connection manager for Polymarket CLOB.
//!
//! Each shard manages one SDK WebSocket connection to Polymarket,
//! subscribing to up to `max_subscriptions_per_connection` tokens.
//! All events are normalized into [`PipelineEvent`] and dispatched to a
//! unified bounded channel.

mod health;
mod ingest_hooks;
mod ingress;
pub mod normalize;
mod reconnect;
mod router;
mod session_hook;
mod shard;
mod token_resolver;

use std::{
    collections::HashSet,
    slice,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use flume::Receiver;
pub use health::{ShardHealthBoard, ShardHealthSummary, WsShardHealthPort};
pub use ingest_hooks::BookLevelRejectHook;
pub use ingress::{
    INGRESS_MAILBOX_CAPACITY, INGRESS_MEMORY_BUDGET_BYTES, INGRESS_PERMIT_BYTES,
    NormalizedIngressBatch, estimated_event_bytes,
};
use num_traits::ToPrimitive;
use parking_lot::Mutex;
use quant_pivot_models::{
    config::{PolymarketConfig, WebSocketConfig},
    types::TokenId,
};
pub use reconnect::ReconnectPolicy;
use router::ShardRouter;
pub use session_hook::WsSessionInvalidationHook;
use shard::ShardDeps;
pub use token_resolver::{TokenKeyResolver, UnregisteredToken};
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

/// Transport ownership that ended after all subscription sources released the
/// token set. Sessions allocated through `through_epoch` are stale and must
/// never reactivate retired mutable books.
#[derive(Debug, Clone)]
pub struct TransportRetirement {
    pub tokens: Arc<[TokenId]>,
    pub through_epoch: u64,
}

/// Cold-path handoff from transport ownership to the core retirement barrier.
pub type TransportRetirementHook = Arc<dyn Fn(TransportRetirement) + Send + Sync>;

/// Successful normalized-ingress enqueue observation used by the production
/// performance harness. `event_count` excludes transport/session control
/// events so the histogram describes only market-data work.
pub type IngressEnqueueObserver = Arc<dyn Fn(Duration, usize) + Send + Sync>;

/// Cold-path lifecycle and performance observation hooks supplied by the
/// composition root. Missing hooks disable only the corresponding observation,
/// never transport correctness behavior.
#[derive(Default)]
pub struct ClobWsManagerHooks {
    pub on_session_invalidated: Option<WsSessionInvalidationHook>,
    pub on_book_level_rejected: Option<BookLevelRejectHook>,
    pub on_transport_retired: Option<TransportRetirementHook>,
    pub ingress_enqueue_observer: Option<IngressEnqueueObserver>,
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

    fn owns_all(&self, tokens: &[TokenId]) -> bool {
        tokens
            .iter()
            .all(|token| self.engine.contains(token) || self.web.contains(token))
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
    output_rx: Receiver<NormalizedIngressBatch>,
    ws_url: String,
    message_epoch: Arc<Instant>,
    last_message_tick: Arc<AtomicU64>,
    subscriptions: Mutex<SubscriptionState>,
    health: Arc<ShardHealthBoard>,
    session_epoch: Arc<AtomicU64>,
    on_transport_retired: Option<TransportRetirementHook>,
}

impl ClobWsManager {
    /// Create a new manager. Shard actors are spawned on first `subscribe` call
    /// and stay resident until shutdown.
    pub fn new(
        polymarket_config: &PolymarketConfig,
        ws_config: &WebSocketConfig,
        shutdown: CancellationToken,
        token_resolver: Arc<dyn TokenKeyResolver>,
        hooks: ClobWsManagerHooks,
    ) -> Self {
        let ClobWsManagerHooks {
            on_session_invalidated,
            on_book_level_rejected,
            on_transport_retired,
            ingress_enqueue_observer,
        } = hooks;
        let (output_tx, output_rx) = flume::bounded(INGRESS_MAILBOX_CAPACITY);
        let ingress_budget = Arc::new(Semaphore::new(
            INGRESS_MEMORY_BUDGET_BYTES / INGRESS_PERMIT_BYTES,
        ));
        let message_epoch = Arc::new(Instant::now());
        let last_message_tick = Arc::new(AtomicU64::new(0));
        let session_epoch = Arc::new(AtomicU64::new(0));
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
                ingress_budget,
                ws_url: polymarket_config.clob_ws_url.clone(),
                shutdown,
                message_epoch: Arc::clone(&message_epoch),
                last_message_tick: Arc::clone(&last_message_tick),
                session_epoch: Arc::clone(&session_epoch),
                token_resolver,
                on_session_invalidated,
                on_book_level_rejected,
                ingress_enqueue_observer,
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
            message_epoch,
            last_message_tick,
            subscriptions: Mutex::new(SubscriptionState::default()),
            health,
            session_epoch,
            on_transport_retired,
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
        let gone = {
            let mut subscriptions = self.subscriptions.lock();
            subscriptions.remove(source, tokens)
        };
        if !gone.is_empty() {
            self.router.remove_tokens(&gone);
            self.notify_transport_retired(gone);
        }
    }

    /// Replace `source`'s desired token set with `tokens`, reconciling the
    /// transport against the new union (the other source's baseline is protected).
    pub fn sync_tokens(&self, source: SubscriptionSource, tokens: &[TokenId]) {
        let desired: HashSet<TokenId> = tokens.iter().cloned().collect();
        let (to_subscribe, to_unsubscribe) = {
            let mut subscriptions = self.subscriptions.lock();
            subscriptions.sync(source, desired)
        };
        if !to_subscribe.is_empty() {
            self.router.assign_tokens(&to_subscribe);
        }
        if !to_unsubscribe.is_empty() {
            self.router.remove_tokens(&to_unsubscribe);
            self.notify_transport_retired(to_unsubscribe);
        }
    }

    fn notify_transport_retired(&self, tokens: Vec<TokenId>) {
        let Some(notify) = &self.on_transport_retired else {
            return;
        };
        notify(TransportRetirement {
            tokens: Arc::from(tokens),
            through_epoch: self.session_epoch.load(Ordering::Acquire),
        });
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

    /// Verify one physical session scope against the current transport union
    /// under a single ownership lock.
    #[must_use]
    pub fn owns_all_tokens(&self, tokens: &[TokenId]) -> bool {
        let state = self.subscriptions.lock();
        state.owns_all(tokens)
    }

    /// Returns the unified event receiver for all shards.
    pub const fn events(&self) -> &Receiver<NormalizedIngressBatch> {
        &self.output_rx
    }

    /// Restart transport ownership for a token whose canonical continuity failed.
    pub fn invalidate_token(&self, token_id: &TokenId) {
        self.invalidate_tokens(slice::from_ref(token_id));
    }

    /// Fail closed for a whole stream session and coalesce transport restarts by shard.
    pub fn invalidate_tokens(&self, token_ids: &[TokenId]) {
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
        let last = self.last_message_tick.load(Ordering::Acquire);
        if last == 0 {
            return None;
        }
        Some(monotonic_tick(&self.message_epoch).saturating_sub(last))
    }
}

impl WsShardHealthPort for ClobWsManager {
    fn shard_health(&self) -> ShardHealthSummary {
        self.health.summary()
    }

    fn last_message_age_ms(&self) -> Option<u64> {
        Self::last_message_age_ms(self)
    }
}

fn monotonic_tick(epoch: &Instant) -> u64 {
    ToPrimitive::to_u64(&epoch.elapsed().as_millis())
        .unwrap_or(u64::MAX - 1)
        .saturating_add(1)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, iter};

    use quant_pivot_models::types::TokenId;

    use super::{SubscriptionSource, SubscriptionState};

    #[test]
    fn first_subscriber_drives_subscribe() {
        let mut state = SubscriptionState::default();
        let newly = state.add(
            SubscriptionSource::Engine,
            &[TokenId::new("a"), TokenId::new("b")],
        );
        assert_eq!(newly.len(), 2, "both tokens newly enter the union");
    }

    #[test]
    fn overlapping_source_noop_wire() {
        let mut state = SubscriptionState::default();
        state.add(
            SubscriptionSource::Engine,
            &[TokenId::new("a"), TokenId::new("b")],
        );
        // Web overlaps `b` and adds `c`; only `c` newly enters the union.
        let newly = state.add(
            SubscriptionSource::Web,
            &[TokenId::new("b"), TokenId::new("c")],
        );
        assert_eq!(newly, vec![TokenId::new("c")]);
    }

    #[test]
    fn web_never_drops_token() {
        let mut state = SubscriptionState::default();
        state.add(
            SubscriptionSource::Engine,
            &[TokenId::new("a"), TokenId::new("b")],
        );
        state.add(
            SubscriptionSource::Web,
            &[TokenId::new("b"), TokenId::new("c")],
        );

        // Web drops everything it added. `b` stays (engine baseline), only `c`
        // leaves the union and is torn down on the transport.
        let gone = state.remove(
            SubscriptionSource::Web,
            &[TokenId::new("b"), TokenId::new("c")],
        );
        assert_eq!(gone, vec![TokenId::new("c")]);

        // Engine still holds both of its tokens.
        assert_eq!(state.split(SubscriptionSource::Engine).0.len(), 2);
    }

    #[test]
    fn web_cannot_remove_unheld() {
        let mut state = SubscriptionState::default();
        state.add(SubscriptionSource::Engine, &[TokenId::new("a")]);
        let gone = state.remove(SubscriptionSource::Web, &[TokenId::new("a")]);
        assert!(gone.is_empty(), "web never held `a`, transport untouched");
        assert!(
            state
                .split(SubscriptionSource::Engine)
                .0
                .contains(&TokenId::new("a"))
        );
    }

    #[test]
    fn engine_sync_protects_overlay() {
        let mut state = SubscriptionState::default();
        state.add(
            SubscriptionSource::Engine,
            &[TokenId::new("a"), TokenId::new("b")],
        );
        state.add(
            SubscriptionSource::Web,
            &[TokenId::new("b"), TokenId::new("c")],
        );

        // Engine reconciles to {a}: it drops `b` from its own set, but `b` stays
        // on the wire because the web overlay still holds it; only engine-only
        // tokens would be removed (none here besides what web protects).
        let desired: HashSet<TokenId> = iter::once(TokenId::new("a")).collect();
        let (to_subscribe, to_unsubscribe) = state.sync(SubscriptionSource::Engine, desired);
        assert!(to_subscribe.is_empty());
        assert!(
            to_unsubscribe.is_empty(),
            "`b` is protected by the web overlay; `a` stays"
        );
    }

    #[test]
    fn physical_session_scope_union() {
        let mut state = SubscriptionState::default();
        state.add(
            SubscriptionSource::Engine,
            &[TokenId::new("a"), TokenId::new("b")],
        );
        assert!(state.owns_all(&[TokenId::new("a"), TokenId::new("b")]));

        assert_eq!(
            state.remove(SubscriptionSource::Engine, &[TokenId::new("b")]),
            vec![TokenId::new("b")]
        );
        assert!(!state.owns_all(&[TokenId::new("a"), TokenId::new("b")]));
        assert!(state.owns_all(&[TokenId::new("a")]));
    }
}
