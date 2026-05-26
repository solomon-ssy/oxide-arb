//! Sharded WebSocket connection manager for Polymarket CLOB.
//!
//! Each shard manages one SDK WebSocket connection to Polymarket,
//! subscribing to up to `max_subscriptions_per_connection` tokens.
//! All events are normalized into [`PipelineEvent`] and dispatched to a
//! unified bounded channel.

pub mod normalize;
mod reconnect;
mod router;
mod shard;
mod token_intern;

pub use reconnect::ReconnectPolicy;
pub use token_intern::{TOKEN_INTERN, TokenInternPool, intern_str, intern_u256};

use flume::Receiver;
use num_traits::ToPrimitive;
use oxide_arb_models::config::{PolymarketConfig, WebSocketConfig};
use oxide_arb_models::domain::pipeline::PipelineEvent;
use oxide_arb_models::types::TokenId;
use router::ShardRouter;
use std::sync::Arc;
use std::time::Instant;

/// Manages sharded WebSocket connections to Polymarket CLOB.
///
/// Each shard handles up to `max_subscriptions_per_connection` tokens.
/// Messages are normalized and dispatched to a unified output channel.
pub struct ClobWsManager {
    router: ShardRouter,
    output_rx: Receiver<PipelineEvent>,
    ws_url: String,
    last_message_at: Arc<parking_lot::Mutex<Option<Instant>>>,
}

impl ClobWsManager {
    /// Create a new manager. Shard tasks are spawned on first `subscribe` call.
    pub fn new(
        polymarket_config: &PolymarketConfig,
        ws_config: &WebSocketConfig,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Self {
        let (output_tx, output_rx) = flume::bounded(8192);
        let last_message_at = Arc::new(parking_lot::Mutex::new(None));
        let router = ShardRouter::new(
            ws_config.max_subscriptions_per_connection,
            output_tx,
            polymarket_config.clob_ws_url.clone(),
            shutdown,
            Arc::clone(&last_message_at),
        );

        Self {
            router,
            output_rx,
            ws_url: polymarket_config.clob_ws_url.clone(),
            last_message_at,
        }
    }

    /// Subscribe to orderbook updates for the given tokens.
    pub fn subscribe(&self, tokens: &[TokenId]) {
        self.router.assign_tokens(tokens);
    }

    /// Unsubscribe from tokens (removes from routing, shards will not re-subscribe on reconnect).
    pub fn unsubscribe(&self, tokens: &[TokenId]) {
        self.router.remove_tokens(tokens);
    }

    /// Returns the unified event receiver for all shards.
    pub const fn events(&self) -> &Receiver<PipelineEvent> {
        &self.output_rx
    }

    /// Returns the number of active shards.
    pub fn shard_count(&self) -> usize {
        self.router.shard_count()
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
