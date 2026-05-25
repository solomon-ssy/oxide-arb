use std::sync::Arc;

use oxide_arb_api::ws::{ClobWsManager, WsEvent};
use oxide_arb_error::OxideError;
use oxide_arb_models::domain::book::BookLevel;
use oxide_arb_models::types::{Price, Shares, TokenId};
use tokio_util::sync::CancellationToken;

use super::book_store::BookStore;
use super::market_registry::MarketRegistry;
use crate::observability::metrics_hub::MetricsHub;

/// Main WS event loop that routes exchange events into `BookStore`
/// and notifies downstream detection via a `flume::Sender<TokenId>`.
#[allow(dead_code)]
pub struct DataPipeline {
    ws_manager: Arc<ClobWsManager>,
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    coalescer_tx: flume::Sender<TokenId>,
    metrics: Arc<MetricsHub>,
    shutdown: CancellationToken,
}

impl DataPipeline {
    pub const fn new(
        ws_manager: Arc<ClobWsManager>,
        book_store: Arc<BookStore>,
        market_registry: Arc<MarketRegistry>,
        coalescer_tx: flume::Sender<TokenId>,
        metrics: Arc<MetricsHub>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            ws_manager,
            book_store,
            market_registry,
            coalescer_tx,
            metrics,
            shutdown,
        }
    }

    /// Run the event loop until shutdown or channel close.
    pub async fn run(&self) -> Result<(), OxideError> {
        let rx = self.ws_manager.events();
        loop {
            tokio::select! {
                biased;

                () = self.shutdown.cancelled() => {
                    tracing::info!("DataPipeline shutting down");
                    return Ok(());
                }

                event = rx.recv_async() => {
                    if let Ok(ws_event) = event { self.handle_event(ws_event) } else {
                        tracing::error!("WS event channel closed unexpectedly");
                        return Err(OxideError::Internal(
                            "WS event channel closed".into(),
                        ));
                    }
                }
            }
        }
    }

    fn handle_event(&self, event: WsEvent) {
        self.metrics.ws_events_received.inc();

        match event {
            WsEvent::BookSnapshot {
                asset_id,
                bids,
                asks,
                timestamp_ms,
                ..
            } => {
                let bid_levels: Vec<BookLevel> = bids
                    .into_iter()
                    .map(|pl| BookLevel {
                        price: pl.price,
                        size: pl.size,
                    })
                    .collect();
                let ask_levels: Vec<BookLevel> = asks
                    .into_iter()
                    .map(|pl| BookLevel {
                        price: pl.price,
                        size: pl.size,
                    })
                    .collect();

                self.book_store
                    .apply_snapshot(&asset_id, bid_levels, ask_levels, timestamp_ms);
                let _ = self.coalescer_tx.try_send(asset_id);
                self.metrics.book_snapshots_applied.inc();
            }

            WsEvent::PriceChange {
                asset_id,
                changes,
                timestamp_ms,
            } => {
                let deltas: Vec<(Price, Shares)> =
                    changes.iter().map(|d| (d.price, d.size)).collect();
                self.book_store
                    .apply_delta(&asset_id, &deltas, timestamp_ms);
                let _ = self.coalescer_tx.try_send(asset_id);
                self.metrics.price_changes_applied.inc();
            }

            WsEvent::MarketResolved { market_id, .. } => {
                tracing::info!(%market_id, "Market resolved via WS");
                self.metrics.markets_resolved_ws.inc();
            }

            WsEvent::TickSizeChange {
                asset_id, new_tick, ..
            } => {
                tracing::info!(%asset_id, ?new_tick, "Tick size changed");
            }

            WsEvent::ShardStatus { shard_id, status } => {
                tracing::info!(shard_id, ?status, "Shard status change");
                self.metrics.shard_status_changes.inc();
            }

            _ => {
                self.metrics.ws_events_ignored.inc();
            }
        }
    }
}
