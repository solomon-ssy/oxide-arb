use super::{
    book_store::BookStore, event_source::PipelineEventSource, market_registry::MarketRegistry,
};
use crate::{
    infra::sharding::shard_index,
    observability::{
        backpressure::BackpressurePolicy, book_fact_writer::BookFactWriter, metrics_hub::MetricsHub,
    },
    service::system_status_nudge::SystemStatusNudge,
};
use flume::Receiver;
use quant_pivot_error::QuantError;
use quant_pivot_models::{
    domain::{BookSnapshotCmd, PriceDeltaCmd, latency::LatencyTrace, pipeline::PipelineEvent},
    enums::{
        clickhouse::{ChBookEventType, ChSnapshotReason},
        system::ShardConnectionStatus,
    },
    types::Shares,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};
use tokio_util::sync::CancellationToken;

/// Sharded book-apply workers for ~500 markets / ~1000 tokens on one host.
pub const DEFAULT_BOOK_SHARD_COUNT: usize = 4;
pub const DEFAULT_BOOK_CHANNEL_CAPACITY: usize = 2048;

/// Dependencies injected into [`DataPipeline`].
pub struct DataPipelineDeps {
    pub event_source: Arc<dyn PipelineEventSource>,
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub metrics: Arc<MetricsHub>,
    pub backpressure: Arc<BackpressurePolicy>,
    pub book_fact_writer: Arc<BookFactWriter>,
    pub book_shard_count: usize,
    pub book_channel_capacity: usize,
    pub shutdown: CancellationToken,
    pub status_nudge: SystemStatusNudge,
}

/// Main WS event loop: Tokio receives frames, dedicated OS threads apply books per shard.
pub struct DataPipeline {
    event_source: Arc<dyn PipelineEventSource>,
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    metrics: Arc<MetricsHub>,
    backpressure: Arc<BackpressurePolicy>,
    book_fact_writer: Arc<BookFactWriter>,
    book_shard_count: usize,
    book_channel_capacity: usize,
    shutdown: CancellationToken,
    status_nudge: SystemStatusNudge,
    market_data_nudged: Arc<AtomicBool>,
}

impl DataPipeline {
    pub fn new(deps: DataPipelineDeps) -> Self {
        Self {
            event_source: deps.event_source,
            book_store: deps.book_store,
            market_registry: deps.market_registry,
            metrics: deps.metrics,
            backpressure: deps.backpressure,
            book_fact_writer: deps.book_fact_writer,
            book_shard_count: deps.book_shard_count,
            book_channel_capacity: deps.book_channel_capacity,
            shutdown: deps.shutdown,
            status_nudge: deps.status_nudge,
            market_data_nudged: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Run until shutdown or channel close.
    pub async fn run(&self) -> Result<(), QuantError> {
        let shard_count = self.book_shard_count.max(1);
        let mut book_senders = Vec::with_capacity(shard_count);
        let mut book_receivers = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (tx, rx) = flume::bounded(self.book_channel_capacity);
            book_senders.push(tx);
            book_receivers.push(rx);
        }

        let book_fact_writer = Arc::clone(&self.book_fact_writer);
        let mut book_threads = Vec::with_capacity(shard_count);
        for (shard_id, rx) in book_receivers.into_iter().enumerate() {
            let worker = BookApplyWorker {
                shard_id,
                book_store: Arc::clone(&self.book_store),
                market_registry: Arc::clone(&self.market_registry),
                metrics: Arc::clone(&self.metrics),
                backpressure: Arc::clone(&self.backpressure),
                book_fact_writer: Arc::clone(&book_fact_writer),
            };
            book_threads.push(thread::spawn(move || worker.run(&rx)));
        }

        let rx = self.event_source.events();
        loop {
            tokio::select! {
                biased;

                () = self.shutdown.cancelled() => {
                    tracing::info!("DataPipeline shutting down");
                    break;
                }

                event = rx.recv_async() => {
                    if let Ok(pipeline_event) = event {
                        if pipeline_event.is_market_data_event()
                            && !self.market_data_nudged.swap(true, Ordering::AcqRel)
                        {
                            self.status_nudge.nudge();
                        }
                        let shard = pipeline_event
                            .asset_id()
                            .map_or(0, |asset_id| shard_index(asset_id.as_str(), shard_count));
                        if let Err(error) = book_senders[shard].try_send(pipeline_event) {
                            self.backpressure
                                .on_book_channel_full(shard, error.into_inner());
                        }
                    } else {
                        tracing::error!("Pipeline event channel closed unexpectedly");
                        drop(book_senders);
                        for handle in book_threads {
                            handle.join().ok();
                        }
                        self.book_fact_writer.flush_pending_microstructure();
                        return Err(QuantError::Internal(
                            "Pipeline event channel closed".into(),
                        ));
                    }
                }
            }
        }

        drop(book_senders);
        for handle in book_threads {
            handle.join().ok();
        }
        // Flush open microstructure buckets before analytics writers drain (WsIngress
        // stage precedes Analytics in shutdown ordering).
        self.book_fact_writer.flush_pending_microstructure();
        Ok(())
    }
}

struct BookApplyWorker {
    shard_id: usize,
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    metrics: Arc<MetricsHub>,
    backpressure: Arc<BackpressurePolicy>,
    book_fact_writer: Arc<BookFactWriter>,
}

impl BookApplyWorker {
    fn run(self, rx: &Receiver<PipelineEvent>) {
        while let Ok(event) = rx.recv() {
            self.handle_event(event);
            self.backpressure
                .drain_book_coalesce(self.shard_id, |event| self.handle_event(event));
        }
    }

    #[inline]
    fn handle_event(&self, event: PipelineEvent) {
        self.metrics.ws_events_received.inc();

        match event {
            PipelineEvent::BookSnapshot(cmd) => self.handle_book_snapshot(&cmd),
            PipelineEvent::PriceDelta(cmd) => self.handle_price_delta(&cmd),

            PipelineEvent::MarketResolved {
                market_id,
                winning_token_id,
                winning_outcome,
                asset_ids,
                timestamp_ms,
                ..
            } => {
                let known = self.market_registry.get_market(&market_id).is_some();
                tracing::info!(%market_id, known, "Market resolved via WS (ingest only)");
                self.metrics.markets_resolved_ws.inc();
                self.book_fact_writer.write_market_resolved(
                    &market_id,
                    &winning_token_id,
                    &winning_outcome,
                    &asset_ids,
                    timestamp_ms,
                );
            }

            PipelineEvent::TickSizeChange {
                asset_id,
                old_tick,
                new_tick,
                ..
            } => {
                tracing::info!(%asset_id, %old_tick, %new_tick, "Tick size changed");
                self.book_fact_writer.write_tick_size_change(
                    &asset_id,
                    self.market_registry.market_for_token(&asset_id),
                    old_tick,
                    new_tick,
                );
            }

            PipelineEvent::ShardStatus { shard_id, status } => {
                self.on_shard_status(shard_id, status);
            }

            PipelineEvent::BestBidAsk {
                asset_id,
                best_bid,
                best_ask,
                timestamp_ms,
                ..
            } => {
                self.book_fact_writer.write_bbo(
                    &asset_id,
                    self.market_registry.market_for_token(&asset_id),
                    best_bid,
                    best_ask,
                    timestamp_ms,
                );
            }

            PipelineEvent::LastTradePrice {
                asset_id,
                price,
                timestamp_ms,
                ..
            } => {
                self.book_fact_writer.write_last_trade(
                    &asset_id,
                    self.market_registry.market_for_token(&asset_id),
                    price,
                    timestamp_ms,
                );
            }
        }
    }

    fn handle_book_snapshot(&self, cmd: &BookSnapshotCmd) {
        let version = self.book_store.apply_snapshot(
            &cmd.asset_id,
            Arc::clone(&cmd.bids.levels),
            Arc::clone(&cmd.asks.levels),
            cmd.timestamp_ms,
            Some(LatencyTrace::from_ingress(cmd.trace.mono)),
        );
        self.book_fact_writer.write_snapshot(
            cmd,
            self.market_registry.market_for_token(&cmd.asset_id),
            version,
        );
        self.metrics.book_snapshots_applied.inc();
    }

    fn handle_price_delta(&self, cmd: &PriceDeltaCmd) {
        let version = self.book_store.apply_delta(
            &cmd.asset_id,
            cmd.changes.iter().map(|d| (d.side, d.price, d.size)),
            cmd.timestamp_ms,
            Some(LatencyTrace::from_ingress(cmd.trace.mono)),
        );
        let market_id = self.market_registry.market_for_token(&cmd.asset_id);
        self.book_fact_writer
            .write_delta(cmd, market_id.clone(), version);
        if let Some(snapshot) = self.book_store.load(&cmd.asset_id) {
            let delete_count = cmd
                .changes
                .iter()
                .filter(|change| change.size <= Shares::ZERO)
                .count();
            self.book_fact_writer.write_microstructure_snapshot(
                &cmd.asset_id,
                market_id,
                &snapshot,
                ChBookEventType::Delta,
                u64::try_from(delete_count).unwrap_or(u64::MAX),
            );
        }
        self.metrics.price_changes_applied.inc();
    }

    /// Shard connectivity surfaces: per-transition detail stays at debug —
    /// aggregate health is the `HealthChecker` summary plus a per-shard gauge.
    fn on_shard_status(&self, shard_id: usize, status: ShardConnectionStatus) {
        tracing::debug!(shard_id, ?status, "Shard status change");
        self.metrics.shard_status_changes.inc();
        let connected = matches!(status, ShardConnectionStatus::Connected);
        self.metrics
            .ws_shard_connected
            .with_label_values(&[&shard_id.to_string()])
            .set(i64::from(connected));
        self.write_shard_status_facts(shard_id, status);
    }

    fn write_shard_status_facts(&self, shard_id: usize, status: ShardConnectionStatus) {
        self.book_fact_writer.write_shard_status(shard_id, status);
        let reason = match status {
            ShardConnectionStatus::Reconnecting { .. } => Some(ChSnapshotReason::Reconnect),
            ShardConnectionStatus::Disconnected => Some(ChSnapshotReason::Gap),
            ShardConnectionStatus::Connected => None,
        };
        if let Some(reason) = reason {
            for (token_id, snapshot) in self.book_store.published_snapshots() {
                if snapshot.timestamp_ms > 0 {
                    self.book_fact_writer.write_published_snapshot(
                        &token_id,
                        self.market_registry.market_for_token(&token_id),
                        reason,
                        &snapshot,
                    );
                }
            }
        }
    }
}
