//! Scanner task — consumes market scan triggers from the coalescer,
//! looks up market data, invokes the scanner, and dispatches results.

use super::funnel::{FastLaneDispatch, Funnel};
use crate::{
    detection::scanner::Scanner, pipeline::market_cache::MarketCache,
    runtime_config::RuntimeConfigStore,
};
use oxide_arb_error::OxideError;
use oxide_arb_models::types::{MarketId, MicroScore};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Dependencies injected into [`ScannerTask`].
pub struct ScannerTaskDeps {
    pub rx: flume::Receiver<MarketId>,
    pub scanner: Arc<Scanner>,
    pub market_cache: Arc<MarketCache>,
    pub funnel: Arc<Funnel>,
    /// Live runtime config: the fast-lane threshold is read per scan so a
    /// runtime-config activation applies without a restart.
    pub runtime: Arc<RuntimeConfigStore>,
    pub shutdown: CancellationToken,
}

pub struct ScannerTask {
    rx: flume::Receiver<MarketId>,
    scanner: Arc<Scanner>,
    market_cache: Arc<MarketCache>,
    funnel: Arc<Funnel>,
    runtime: Arc<RuntimeConfigStore>,
    shutdown: CancellationToken,
}

impl ScannerTask {
    pub fn new(deps: ScannerTaskDeps) -> Self {
        Self {
            rx: deps.rx,
            scanner: deps.scanner,
            market_cache: deps.market_cache,
            funnel: deps.funnel,
            runtime: deps.runtime,
            shutdown: deps.shutdown,
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
            let threshold = MicroScore::try_from_decimal(
                self.runtime
                    .load()
                    .execution
                    .endgame_latency
                    .dispatch_immediate_threshold,
            )
            .unwrap_or(MicroScore::ZERO);
            let mut scored = scored;
            if scored.score >= threshold {
                match self.funnel.try_dispatch_immediate(Arc::clone(&scored)) {
                    FastLaneDispatch::Dispatched => return,
                    FastLaneDispatch::Backpressure(arc) => scored = arc,
                }
            }
            self.funnel.submit(scored);
        }
    }
}
