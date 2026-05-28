use oxide_arb_models::{
    domain::settlement::MarketSettlementRequest,
    types::{MarketId, TokenId},
};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

pub struct SettlementDedup {
    inner: Mutex<HashMap<(MarketId, TokenId), Instant>>,
    window: Duration,
}

impl SettlementDedup {
    pub fn new(window: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            window,
        }
    }

    pub fn should_process(&self, req: &MarketSettlementRequest) -> bool {
        let now = Instant::now();
        let key = (req.market_id.clone(), req.winning_token_id.clone());
        let mut inner = self.inner.lock();
        inner.retain(|_, seen_at| now.duration_since(*seen_at) <= self.window);

        if inner
            .get(&key)
            .is_some_and(|seen_at| now.duration_since(*seen_at) <= self.window)
        {
            return false;
        }

        inner.insert(key, now);
        true
    }
}
