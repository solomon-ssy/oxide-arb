//! Core implementation of the web-facing [`MarketDataPort`].
//!
//! Bridges the live `BookStore` (published, lock-free snapshots) and the CLOB
//! WebSocket manager to the web layer without exposing core types, mirroring the
//! dependency inversion used by [`crate::control::mode_transition::CoreRuntimeControl`].

use crate::pipeline::book_store::BookStore;
use async_trait::async_trait;
use oxide_arb_api::ws::{ClobWsManager, SubscriptionSource};
use oxide_arb_models::{
    domain::{MarketDataPort, RuntimeControlError, market::book::BookSnapshot},
    types::TokenId,
};
use std::sync::Arc;

/// Live market-data port backing the markets dashboard book read + WS controls.
pub struct CoreMarketData {
    book_store: Arc<BookStore>,
    ws_manager: Arc<ClobWsManager>,
}

impl CoreMarketData {
    #[must_use]
    pub const fn new(book_store: Arc<BookStore>, ws_manager: Arc<ClobWsManager>) -> Self {
        Self {
            book_store,
            ws_manager,
        }
    }
}

#[async_trait]
impl MarketDataPort for CoreMarketData {
    fn book(
        &self,
        yes_token: &TokenId,
        no_token: &TokenId,
    ) -> (Option<Arc<BookSnapshot>>, Option<Arc<BookSnapshot>>) {
        (
            self.book_store.load(yes_token),
            self.book_store.load(no_token),
        )
    }

    async fn subscribe(&self, token_ids: Vec<TokenId>) -> Result<(), RuntimeControlError> {
        // Web overlay: adds the operator's tokens without disturbing the engine
        // baseline (union-refcounted in the manager).
        self.ws_manager
            .subscribe_tokens(SubscriptionSource::Web, &token_ids);
        Ok(())
    }

    async fn unsubscribe(&self, token_ids: Vec<TokenId>) -> Result<(), RuntimeControlError> {
        // Only drops the web overlay; tokens the engine still holds stay live.
        self.ws_manager
            .unsubscribe_tokens(SubscriptionSource::Web, &token_ids);
        Ok(())
    }
}
