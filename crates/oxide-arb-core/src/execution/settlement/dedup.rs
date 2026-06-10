use oxide_arb_models::{
    domain::settlement::MarketSettlementRequest,
    runtime_config::SettlementLifecycleConfig,
    types::{MarketId, TokenId},
};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

pub struct SettlementDedup {
    inner: Mutex<HashMap<(MarketId, TokenId), Instant>>,
    window_secs: AtomicU64,
}

impl SettlementDedup {
    pub fn new(window: Duration) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            window_secs: AtomicU64::new(window.as_secs()),
        }
    }

    /// Hot-reload the dedup window (runtime-config activation). Caution:
    /// shrinking the window can admit a duplicate trigger for a market that
    /// settled within the previous, longer window.
    pub fn reload(&self, config: &SettlementLifecycleConfig) {
        self.window_secs
            .store(config.dedup_window_secs, Ordering::Relaxed);
    }

    pub fn should_process(&self, req: &MarketSettlementRequest) -> bool {
        let now = Instant::now();
        let window = Duration::from_secs(self.window_secs.load(Ordering::Relaxed));
        let key = (req.market_id.clone(), req.winning_token_id.clone());
        let mut inner = self.inner.lock();
        inner.retain(|_, seen_at| now.duration_since(*seen_at) <= window);

        if inner
            .get(&key)
            .is_some_and(|seen_at| now.duration_since(*seen_at) <= window)
        {
            return false;
        }

        inner.insert(key, now);
        true
    }
}
