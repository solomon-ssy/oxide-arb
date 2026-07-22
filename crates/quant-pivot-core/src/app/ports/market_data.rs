//! Live book + CLOB subscription port for the Admin API.

use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use quant_pivot_api::ws::{ClobWsManager, SubscriptionSource};
use quant_pivot_error::control::ControlError;
use quant_pivot_models::{
    domain::{market::BookSnapshot, ports::MarketDataPort},
    types::TokenId,
};

use crate::ingest::book_store::BookStore;

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
    fn book_for_token(&self, token_id: &TokenId) -> Option<Arc<BookSnapshot>> {
        self.book_store.load_by_id(token_id)
    }

    fn book(
        &self,
        yes_token: &TokenId,
        no_token: &TokenId,
    ) -> (Option<Arc<BookSnapshot>>, Option<Arc<BookSnapshot>>) {
        (
            self.book_store.load_by_id(yes_token),
            self.book_store.load_by_id(no_token),
        )
    }

    fn subscribed_tokens(&self, token_ids: &[TokenId]) -> HashSet<TokenId> {
        self.ws_manager.subscribed_tokens(token_ids)
    }

    fn all_subscribed_tokens(&self) -> HashSet<TokenId> {
        self.ws_manager.all_subscribed_tokens()
    }

    async fn subscribe(&self, token_ids: Vec<TokenId>) -> Result<(), ControlError> {
        self.ws_manager
            .subscribe_tokens(SubscriptionSource::Web, &token_ids);
        Ok(())
    }

    async fn unsubscribe(&self, token_ids: Vec<TokenId>) -> Result<(), ControlError> {
        self.ws_manager
            .unsubscribe_tokens(SubscriptionSource::Web, &token_ids);
        Ok(())
    }
}
