//! Keeps CLOB websocket subscriptions aligned with the active Gamma catalog.

use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_models::types::TokenId;
use std::{collections::HashSet, sync::Arc};

pub struct WsSubscriptionCoordinator {
    ws_manager: Arc<ClobWsManager>,
    subscribed: parking_lot::Mutex<HashSet<TokenId>>,
}

impl WsSubscriptionCoordinator {
    pub fn new(ws_manager: Arc<ClobWsManager>) -> Self {
        Self {
            ws_manager,
            subscribed: parking_lot::Mutex::new(HashSet::new()),
        }
    }

    pub fn sync_to_tokens(&self, desired: Vec<TokenId>) {
        let desired_set: HashSet<TokenId> = desired.into_iter().collect();
        let mut subscribed = self.subscribed.lock();

        let to_subscribe: Vec<TokenId> = desired_set.difference(&subscribed).cloned().collect();
        let to_unsubscribe: Vec<TokenId> = subscribed.difference(&desired_set).cloned().collect();

        if !to_subscribe.is_empty() {
            self.ws_manager.subscribe(&to_subscribe);
        }
        if !to_unsubscribe.is_empty() {
            self.ws_manager.unsubscribe(&to_unsubscribe);
        }

        *subscribed = desired_set;
    }

    #[must_use]
    pub fn subscribed_count(&self) -> usize {
        self.subscribed.lock().len()
    }
}
