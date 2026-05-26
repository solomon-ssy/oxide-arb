//! Scanner task — consumes market scan triggers from the coalescer,
//! looks up market data, invokes the scanner, and dispatches results.

use std::sync::Arc;

use oxide_arb_error::OxideError;
use oxide_arb_models::types::MarketId;
use rust_decimal::Decimal;
use tokio_util::sync::CancellationToken;

use crate::detection::scanner::Scanner;
use crate::observability::metrics_hub::MetricsHub;
use crate::pipeline::market_cache::MarketCache;

use super::funnel::{FastLaneDispatch, Funnel};

/// Dependencies injected into [`ScannerTask`].
pub struct ScannerTaskDeps {
    pub rx: flume::Receiver<MarketId>,
    pub scanner: Arc<Scanner>,
    pub market_cache: Arc<MarketCache>,
    pub funnel: Arc<Funnel>,
    pub dispatch_immediate_threshold: Decimal,
    pub shutdown: CancellationToken,
    pub metrics: Arc<MetricsHub>,
}

pub struct ScannerTask {
    rx: flume::Receiver<MarketId>,
    scanner: Arc<Scanner>,
    market_cache: Arc<MarketCache>,
    funnel: Arc<Funnel>,
    dispatch_immediate_threshold: Decimal,
    shutdown: CancellationToken,
    metrics: Arc<MetricsHub>,
}

impl ScannerTask {
    pub fn new(deps: ScannerTaskDeps) -> Self {
        Self {
            rx: deps.rx,
            scanner: deps.scanner,
            market_cache: deps.market_cache,
            funnel: deps.funnel,
            dispatch_immediate_threshold: deps.dispatch_immediate_threshold,
            shutdown: deps.shutdown,
            metrics: deps.metrics,
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
            self.metrics.opportunities_detected.inc();
            let mut scored = scored;
            if scored.score >= self.dispatch_immediate_threshold {
                match self.funnel.try_dispatch_immediate(scored) {
                    FastLaneDispatch::Dispatched => return,
                    FastLaneDispatch::Backpressure(s) => scored = s,
                }
            }
            self.funnel.submit(scored);
        }
    }
}
