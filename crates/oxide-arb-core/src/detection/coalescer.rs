use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;

use oxide_arb_error::OxideError;
use oxide_arb_models::types::{MarketId, TokenId};

use crate::observability::metrics_hub::MetricsHub;
use crate::pipeline::market_registry::MarketRegistry;

const COALESCE_TICK_MS: u64 = 10;

#[derive(Debug)]
struct PendingMarket {
    first_seen: Instant,
    yes_updated: bool,
    no_updated: bool,
}

/// Debounces per-token book updates into market-level scan triggers.
///
/// Emits immediately when both YES and NO tokens have updated; otherwise waits
/// up to `coalesce_window` for the second leg.
pub struct Coalescer {
    pending: DashMap<MarketId, PendingMarket>,
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
    #[inline]
    pub fn notify_token_update(&self, token_id: &TokenId) {
        let Some(market_id) = self.market_registry.market_for_token(token_id) else {
            return;
        };
        let Some((yes, no)) = self.market_registry.token_pair(&market_id) else {
            return;
        };

        let mut flush_now = false;
        let mut entry = self
            .pending
            .entry(market_id.clone())
            .or_insert_with(|| PendingMarket {
                first_seen: Instant::now(),
                yes_updated: false,
                no_updated: false,
            });

        if token_id == &yes {
            entry.yes_updated = true;
        }
        if token_id == &no {
            entry.no_updated = true;
        }
        if entry.yes_updated && entry.no_updated {
            flush_now = true;
        }
        drop(entry);

        if flush_now {
            self.pending.remove(&market_id);
            self.emit_market(market_id);
        }
    }

    /// Main loop: periodically flush markets whose max-wait window has elapsed.
    pub async fn run(&self) -> Result<(), OxideError> {
        self.run_with_ingress(None).await
    }

    /// Consumes token updates from the data pipeline and runs the coalesce tick loop.
    pub async fn run_with_ingress(
        &self,
        token_rx: Option<flume::Receiver<TokenId>>,
    ) -> Result<(), OxideError> {
        let mut interval = tokio::time::interval(Duration::from_millis(COALESCE_TICK_MS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return Ok(()),
                token = async {
                    match &token_rx {
                        Some(rx) => rx.recv_async().await.ok(),
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(token_id) = token {
                        self.notify_token_update(&token_id);
                    } else {
                        return Ok(());
                    }
                }
                _ = interval.tick() => {
                    self.flush_expired();
                }
            }
        }
    }

    fn flush_expired(&self) {
        let now = Instant::now();
        let mut ready = Vec::with_capacity(self.pending.len());

        self.pending.retain(|market_id, entry| {
            if now.duration_since(entry.first_seen) >= self.coalesce_window {
                ready.push(market_id.clone());
                false
            } else {
                true
            }
        });

        for market_id in ready {
            self.emit_market(market_id);
        }
    }

    fn emit_market(&self, market_id: MarketId) {
        if self.token_tx.try_send(market_id).is_ok() {
            self.metrics.coalesced_scans.inc();
        } else {
            self.metrics.coalescer_dropped.inc();
        }
    }
}
