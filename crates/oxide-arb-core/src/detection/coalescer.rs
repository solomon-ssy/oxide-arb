use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

use oxide_arb_error::OxideError;
use oxide_arb_models::types::{MarketId, TokenId};

use crate::observability::metrics_hub::MetricsHub;
use crate::pipeline::market_registry::MarketRegistry;

/// Debounces per-token book updates into market-level scan triggers.
///
/// When a token book is updated, the coalescer records the market's first
/// pending update timestamp. Once the coalesce window elapses, it emits
/// the market ID downstream for scanning, preventing redundant scans when
/// both YES and NO books update in quick succession.
pub struct Coalescer {
    pending: DashMap<MarketId, Instant>,
    market_registry: Arc<MarketRegistry>,
    coalesce_window: Duration,
    token_tx: flume::Sender<MarketId>,
    metrics: Arc<MetricsHub>,
    shutdown: CancellationToken,
}

impl Coalescer {
    pub fn new(
        market_registry: Arc<MarketRegistry>,
        coalesce_window: Duration,
        token_tx: flume::Sender<MarketId>,
        metrics: Arc<MetricsHub>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            pending: DashMap::new(),
            market_registry,
            coalesce_window,
            token_tx,
            metrics,
            shutdown,
        }
    }

    /// Called by the data pipeline when a token book is updated.
    pub fn notify_token_update(&self, token_id: &TokenId) {
        if let Some(market_id) = self.market_registry.market_for_token(token_id) {
            self.pending.entry(market_id).or_insert_with(Instant::now);
        }
    }

    /// Main loop: periodically flush markets whose coalesce window has elapsed.
    pub async fn run(&self) -> Result<(), OxideError> {
        let mut interval = tokio::time::interval(Duration::from_millis(25));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return Ok(()),
                _ = interval.tick() => {
                    self.flush_ready();
                }
            }
        }
    }

    fn flush_ready(&self) {
        let now = Instant::now();
        let mut ready = Vec::with_capacity(self.pending.len());

        self.pending.retain(|market_id, first_seen| {
            if now.duration_since(*first_seen) >= self.coalesce_window {
                ready.push(market_id.clone());
                false
            } else {
                true
            }
        });

        for market_id in ready {
            let _ = self.token_tx.try_send(market_id);
            self.metrics.coalesced_scans.inc();
        }
    }
}
