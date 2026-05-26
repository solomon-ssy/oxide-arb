use std::sync::Arc;
use std::thread;

use flume::{Receiver, Sender};
use oxide_arb_api::ws::{ClobWsManager, WsEvent};
use oxide_arb_error::OxideError;
use oxide_arb_models::domain::book::BookLevel;
use oxide_arb_models::types::TokenId;
use tokio_util::sync::CancellationToken;

use super::book_store::BookStore;
use super::market_registry::MarketRegistry;
use crate::execution::fsm::ExecutionFSM;
use crate::execution::runner::shard_index;
use crate::observability::drop_halt::DropHaltGuard;
use crate::observability::metrics_hub::MetricsHub;

/// Sharded book-apply workers for ~500 markets / ~1000 tokens on one host.
pub const DEFAULT_BOOK_SHARD_COUNT: usize = 4;
pub const DEFAULT_BOOK_CHANNEL_CAPACITY: usize = 2048;

/// Dependencies injected into [`DataPipeline`].
pub struct DataPipelineDeps {
    pub ws_manager: Arc<ClobWsManager>,
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub coalescer_tx: flume::Sender<TokenId>,
    pub metrics: Arc<MetricsHub>,
    pub drop_halt: Option<DropHaltGuard>,
    pub book_shard_count: usize,
    pub book_channel_capacity: usize,
    pub shutdown: CancellationToken,
}

/// Main WS event loop: Tokio receives frames, dedicated OS threads apply books per shard.
pub struct DataPipeline {
    ws_manager: Arc<ClobWsManager>,
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    coalescer_tx: flume::Sender<TokenId>,
    metrics: Arc<MetricsHub>,
    drop_halt: Option<DropHaltGuard>,
    book_shard_count: usize,
    book_channel_capacity: usize,
    shutdown: CancellationToken,
}

impl DataPipeline {
    pub fn new(deps: DataPipelineDeps) -> Self {
        Self {
            ws_manager: deps.ws_manager,
            book_store: deps.book_store,
            market_registry: deps.market_registry,
            coalescer_tx: deps.coalescer_tx,
            metrics: deps.metrics,
            drop_halt: deps.drop_halt,
            book_shard_count: deps.book_shard_count,
            book_channel_capacity: deps.book_channel_capacity,
            shutdown: deps.shutdown,
        }
    }

    /// Run until shutdown or channel close.
    pub async fn run(&self) -> Result<(), OxideError> {
        let shard_count = self.book_shard_count.max(1);
        let mut book_senders = Vec::with_capacity(shard_count);
        let mut book_receivers = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (tx, rx) = flume::bounded(self.book_channel_capacity);
            book_senders.push(tx);
            book_receivers.push(rx);
        }

        let mut book_threads = Vec::with_capacity(shard_count);
        for rx in book_receivers {
            let worker = BookApplyWorker {
                book_store: Arc::clone(&self.book_store),
                market_registry: Arc::clone(&self.market_registry),
                coalescer_tx: self.coalescer_tx.clone(),
                metrics: Arc::clone(&self.metrics),
                drop_halt: self.drop_halt.clone(),
            };
            book_threads.push(thread::spawn(move || worker.run(&rx)));
        }

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
                        let shard = book_shard_for_event(&ws_event, shard_count);
                        if book_senders[shard].try_send(ws_event).is_err() {
                            if let Some(ref guard) = self.drop_halt {
                                guard.on_book_apply_drop();
                            } else {
                                self.metrics.book_apply_dropped.inc();
                            }
                        }
                    } else {
                        tracing::error!("WS event channel closed unexpectedly");
                        drop(book_senders);
                        for handle in book_threads {
                            handle.join().ok();
                        }
                        return Err(OxideError::Internal(
                            "WS event channel closed".into(),
                        ));
                    }
                }
            }
        }

        drop(book_senders);
        for handle in book_threads {
            handle.join().ok();
        }
        Ok(())
    }
}

fn book_shard_for_event(event: &WsEvent, shard_count: usize) -> usize {
    let id = match event {
        WsEvent::BookSnapshot { asset_id, .. }
        | WsEvent::PriceChange { asset_id, .. }
        | WsEvent::BestBidAsk { asset_id, .. }
        | WsEvent::TickSizeChange { asset_id, .. }
        | WsEvent::LastTradePrice { asset_id, .. } => asset_id.as_str(),
        _ => return 0,
    };
    shard_index(id, shard_count)
}

struct BookApplyWorker {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    coalescer_tx: Sender<TokenId>,
    metrics: Arc<MetricsHub>,
    drop_halt: Option<DropHaltGuard>,
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
                self.notify_coalescer(asset_id);
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
                self.notify_coalescer(asset_id);
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

    fn notify_coalescer(&self, asset_id: TokenId) {
        if self.coalescer_tx.try_send(asset_id).is_err() {
            if let Some(ref guard) = self.drop_halt {
                guard.on_coalescer_drop();
            } else {
                self.metrics.coalescer_dropped.inc();
            }
        }
    }
}

/// Convenience builder for [`DropHaltGuard`] from metrics + FSM.
pub const fn drop_halt_guard(metrics: Arc<MetricsHub>, fsm: Arc<ExecutionFSM>) -> DropHaltGuard {
    DropHaltGuard::new(metrics, fsm)
}
