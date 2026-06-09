//! Keeps CLOB websocket subscriptions aligned with the active Gamma catalog.
//!
//! This is the trading engine's subscription baseline. It registers under the
//! [`SubscriptionSource::Engine`] tag so the union-refcounted manager protects
//! these tokens from the web control plane: a web `unsubscribe` can only drop
//! the web overlay, never a token the engine is actively trading.

use oxide_arb_api::ws::{ClobWsManager, SubscriptionSource};
use oxide_arb_models::types::TokenId;
use std::sync::Arc;

pub struct WsSubscriptionCoordinator {
    ws_manager: Arc<ClobWsManager>,
}

impl WsSubscriptionCoordinator {
    pub const fn new(ws_manager: Arc<ClobWsManager>) -> Self {
        Self { ws_manager }
    }

    /// Reconcile the engine baseline to exactly `desired`. The manager diffs
    /// against the previous engine set and the web overlay, touching the
    /// transport only for union `0 ↔ 1` transitions.
    pub fn sync_to_tokens(&self, desired: &[TokenId]) {
        self.ws_manager
            .sync_tokens(SubscriptionSource::Engine, desired);
    }

    #[must_use]
    pub fn subscribed_count(&self) -> usize {
        self.ws_manager
            .source_subscription_count(SubscriptionSource::Engine)
    }
}
