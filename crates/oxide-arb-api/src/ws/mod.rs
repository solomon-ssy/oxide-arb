//! Sharded WebSocket connection manager for Polymarket CLOB.
//!
//! Each shard manages one SDK WebSocket connection to Polymarket,
//! subscribing to up to `max_subscriptions_per_connection` tokens.
//! All events are normalized into [`WsEvent`] and dispatched to a
//! unified bounded channel.

mod event;
mod reconnect;
mod router;
mod shard;

pub use event::{PriceLevel, PriceLevelDelta, ShardConnectionStatus, WsEvent};
pub use reconnect::ReconnectPolicy;

use flume::Receiver;
use oxide_arb_models::config::{PolymarketConfig, WebSocketConfig};
use oxide_arb_models::types::TokenId;
use router::ShardRouter;

/// Manages sharded WebSocket connections to Polymarket CLOB.
///
/// Each shard handles up to `max_subscriptions_per_connection` tokens.
/// Messages are normalized and dispatched to a unified output channel.
pub struct ClobWsManager {
    router: ShardRouter,
    output_rx: Receiver<WsEvent>,
    ws_url: String,
}

impl ClobWsManager {
    /// Create a new manager. Shard tasks are spawned on first `subscribe` call.
    pub fn new(
        polymarket_config: &PolymarketConfig,
        ws_config: &WebSocketConfig,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Self {
        let (output_tx, output_rx) = flume::bounded(8192);
        let router = ShardRouter::new(
            ws_config.max_subscriptions_per_connection,
            output_tx,
            polymarket_config.clob_ws_url.clone(),
            shutdown,
        );

        Self {
            router,
            output_rx,
            ws_url: polymarket_config.clob_ws_url.clone(),
        }
    }

    /// Subscribe to orderbook updates for the given tokens.
    ///
    /// Assigns tokens to shards and spawns shard tasks if needed.
    pub fn subscribe(&self, tokens: &[TokenId]) {
        self.router.assign_tokens(tokens);
    }

    /// Unsubscribe from tokens (removes from routing, shards will not re-subscribe on reconnect).
    pub fn unsubscribe(&self, tokens: &[TokenId]) {
        self.router.remove_tokens(tokens);
    }

    /// Returns the unified event receiver for all shards.
    pub const fn events(&self) -> &Receiver<WsEvent> {
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
}
