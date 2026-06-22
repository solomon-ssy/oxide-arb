use crate::{
    execution::settlement::{dedup::SettlementDedup, service::MarketSettlementService},
    observability::metrics_hub::MetricsHub,
};
use flume::Receiver;
use oxide_arb_error::OxideError;
use oxide_arb_models::domain::settlement::MarketSettlementRequest;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct MarketSettlementTask {
    rx: Receiver<MarketSettlementRequest>,
    service: Arc<MarketSettlementService>,
    dedup: Arc<SettlementDedup>,
    metrics: Arc<MetricsHub>,
    shutdown: CancellationToken,
}

impl MarketSettlementTask {
    pub const fn new(
        rx: Receiver<MarketSettlementRequest>,
        service: Arc<MarketSettlementService>,
        dedup: Arc<SettlementDedup>,
        metrics: Arc<MetricsHub>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            rx,
            service,
            dedup,
            metrics,
            shutdown,
        }
    }

    pub async fn run(self) -> Result<(), OxideError> {
        loop {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => {
                    while let Ok(req) = self.rx.try_recv() {
                        self.process(req).await;
                    }
                    return Ok(());
                }
                req = self.rx.recv_async() => {
                    match req {
                        Ok(req) => self.process(req).await,
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }

    async fn process(&self, req: MarketSettlementRequest) {
        if !self.dedup.should_process(&req) {
            tracing::debug!(market_id = %req.market_id, "duplicate settlement request skipped");
            return;
        }

        self.metrics
            .settlement_requests_total
            .with_label_values(&[req.source.as_str()])
            .inc();
        if let Err(error) = self.service.settle_market(&req).await {
            tracing::error!(%error, market_id = %req.market_id, "market settlement failed");
        }
    }
}
