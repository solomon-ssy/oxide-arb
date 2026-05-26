use std::sync::Arc;
use std::thread::{self, JoinHandle};

use flume::{Receiver, Sender};
use oxide_arb_api::ws::{ClobWsManager, WsEvent};
use oxide_arb_error::OxideError;
use oxide_arb_models::domain::book::BookLevel;
use oxide_arb_models::types::TokenId;
use tokio_util::sync::CancellationToken;

use super::book_store::BookStore;
use super::market_registry::MarketRegistry;
use crate::observability::metrics_hub::MetricsHub;

/// Main WS event loop: Tokio receives frames, dedicated OS thread applies books.
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

    /// Run until shutdown or channel close.
    pub async fn run(&self) -> Result<(), OxideError> {
        let (book_tx, book_rx) = flume::bounded(8192);
        let worker = BookApplyWorker {
            book_store: Arc::clone(&self.book_store),
            market_registry: Arc::clone(&self.market_registry),
            coalescer_tx: self.coalescer_tx.clone(),
            metrics: Arc::clone(&self.metrics),
        };
        let book_thread: JoinHandle<()> = thread::spawn(move || worker.run(&book_rx));

        let rx = self.ws_manager.events();
        loop {
            tokio::select! {
                biased;

                () = self.shutdown.cancelled() => {
                    tracing::info!("DataPipeline shutting down");
                    break;
                }

                event = rx.recv_async() => {
                    if let Ok(ws_event) = event {
                        if book_tx.try_send(ws_event).is_err() {
                            self.metrics.book_apply_dropped.inc();
                        }
                    } else {
                        tracing::error!("WS event channel closed unexpectedly");
                        drop(book_tx);
                        book_thread.join().ok();
                        return Err(OxideError::Internal(
                            "WS event channel closed".into(),
                        ));
                    }
                }
            }
        }

        drop(book_tx);
        book_thread.join().ok();
        Ok(())
    }
}

struct BookApplyWorker {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    coalescer_tx: Sender<TokenId>,
    metrics: Arc<MetricsHub>,
}

impl BookApplyWorker {
    fn run(self, rx: &Receiver<WsEvent>) {
        while let Ok(event) = rx.recv() {
            self.handle_event(event);
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
                let mut bid_levels = Vec::with_capacity(bids.len());
                bid_levels.extend(
                    bids.into_iter()
                        .map(|pl| BookLevel::from_decimal_unchecked(pl.price, pl.size)),
                );
                let mut ask_levels = Vec::with_capacity(asks.len());
                ask_levels.extend(
                    asks.into_iter()
                        .map(|pl| BookLevel::from_decimal_unchecked(pl.price, pl.size)),
                );

                self.book_store
                    .apply_snapshot(&asset_id, bid_levels, ask_levels, timestamp_ms);
                if self.coalescer_tx.try_send(asset_id).is_err() {
                    self.metrics.coalescer_dropped.inc();
                }
                self.metrics.book_snapshots_applied.inc();
            }

            WsEvent::PriceChange {
                asset_id,
                changes,
                timestamp_ms,
            } => {
                self.book_store.apply_delta(
                    &asset_id,
                    changes.iter().map(|d| (d.price, d.size)),
                    timestamp_ms,
                );
                if self.coalescer_tx.try_send(asset_id).is_err() {
                    self.metrics.coalescer_dropped.inc();
                }
                self.metrics.price_changes_applied.inc();
            }

            WsEvent::MarketResolved { market_id, .. } => {
                let known = self.market_registry.get_market(&market_id).is_some();
                tracing::info!(%market_id, known, "Market resolved via WS");
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
