//! Scanner task — consumes market scan triggers from the coalescer,
//! looks up market data, invokes the scanner, and submits results to
//! the funnel for rate-limited dispatch.

use std::sync::Arc;

use oxide_arb_error::OxideError;
use oxide_arb_models::types::MarketId;
use tokio_util::sync::CancellationToken;

use crate::detection::scanner::Scanner;
use crate::observability::metrics_hub::MetricsHub;
use crate::pipeline::market_cache::MarketCache;

use super::funnel::Funnel;

pub struct ScannerTask {
    rx: flume::Receiver<MarketId>,
    scanner: Arc<Scanner>,
    market_cache: Arc<MarketCache>,
    funnel: Arc<Funnel>,
    shutdown: CancellationToken,
    metrics: Arc<MetricsHub>,
}

impl ScannerTask {
    pub const fn new(
        rx: flume::Receiver<MarketId>,
        scanner: Arc<Scanner>,
        market_cache: Arc<MarketCache>,
        funnel: Arc<Funnel>,
        shutdown: CancellationToken,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            rx,
            scanner,
            market_cache,
            funnel,
            shutdown,
            metrics,
        }
    }

    pub async fn run(&self) -> Result<(), OxideError> {
        loop {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => {
                    tracing::info!("scanner task shutting down");
                    return Ok(());
                }
                market_id = self.rx.recv_async() => {
                    if let Ok(id) = market_id {
                        self.scan_market(&id);
                    } else {
                        tracing::warn!("market scan channel closed");
                        return Ok(());
                    }
                }
            }
        }
    }

    fn scan_market(&self, market_id: &MarketId) {
        let Some(entry) = self.market_cache.get(market_id) else {
            return;
        };
        let now = chrono::Utc::now();
        if let Some(scored) = self.scanner.scan_market(&entry, now) {
            self.funnel.submit(scored);
            self.metrics.opportunities_detected.inc();
        }
    }
}
