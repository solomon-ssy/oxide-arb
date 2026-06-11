use super::{
    book_store::BookStore, event_source::PipelineEventSource, market_registry::MarketRegistry,
};
use crate::{
    infra::sharding::shard_index,
    observability::{
        alert_dispatcher::{Alert, AlertDispatcher},
        backpressure::BackpressurePolicy,
        book_fact_writer::BookFactWriter,
        metrics_hub::MetricsHub,
    },
};
use chrono::Utc;
use flume::{Receiver, Sender};
use oxide_arb_error::OxideError;
use oxide_arb_models::{
    domain::{
        BookSnapshotCmd, PriceDeltaCmd, latency::LatencyTrace, pipeline::PipelineEvent,
        settlement::MarketSettlementRequest,
    },
    enums::{
        clickhouse::ChSnapshotReason,
        common::{AlertLevel, SettlementTrigger},
        pipeline::ShardConnectionStatus,
    },
    types::TokenId,
};
use std::{sync::Arc, thread};
use tokio_util::sync::CancellationToken;

/// Sharded book-apply workers for ~500 markets / ~1000 tokens on one host.
pub const DEFAULT_BOOK_SHARD_COUNT: usize = 4;
pub const DEFAULT_BOOK_CHANNEL_CAPACITY: usize = 2048;

/// Dependencies injected into [`DataPipeline`].
pub struct DataPipelineDeps {
    pub event_source: Arc<dyn PipelineEventSource>,
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub coalescer_tx: flume::Sender<TokenId>,
    pub settlement_tx: flume::Sender<MarketSettlementRequest>,
    pub metrics: Arc<MetricsHub>,
    pub alerts: Arc<AlertDispatcher>,
    pub backpressure: Arc<BackpressurePolicy>,
    pub book_fact_writer: Option<Arc<BookFactWriter>>,
    pub book_shard_count: usize,
    pub book_channel_capacity: usize,
    pub shutdown: CancellationToken,
}

/// Main WS event loop: Tokio receives frames, dedicated OS threads apply books per shard.
pub struct DataPipeline {
    event_source: Arc<dyn PipelineEventSource>,
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    coalescer_tx: flume::Sender<TokenId>,
    settlement_tx: flume::Sender<MarketSettlementRequest>,
    metrics: Arc<MetricsHub>,
    alerts: Arc<AlertDispatcher>,
    backpressure: Arc<BackpressurePolicy>,
    book_fact_writer: Option<Arc<BookFactWriter>>,
    book_shard_count: usize,
    book_channel_capacity: usize,
    shutdown: CancellationToken,
}

impl DataPipeline {
    pub fn new(deps: DataPipelineDeps) -> Self {
        Self {
            event_source: deps.event_source,
            book_store: deps.book_store,
            market_registry: deps.market_registry,
            coalescer_tx: deps.coalescer_tx,
            settlement_tx: deps.settlement_tx,
            metrics: deps.metrics,
            alerts: deps.alerts,
            backpressure: deps.backpressure,
            book_fact_writer: deps.book_fact_writer,
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
        for (shard_id, rx) in book_receivers.into_iter().enumerate() {
            let worker = BookApplyWorker {
                shard_id,
                book_store: Arc::clone(&self.book_store),
                market_registry: Arc::clone(&self.market_registry),
                coalescer_tx: self.coalescer_tx.clone(),
                settlement_tx: self.settlement_tx.clone(),
                metrics: Arc::clone(&self.metrics),
                alerts: Arc::clone(&self.alerts),
                backpressure: Arc::clone(&self.backpressure),
                book_fact_writer: self.book_fact_writer.as_ref().map(Arc::clone),
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
                        let shard = book_shard_for_event(&pipeline_event, shard_count);
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
                        return Err(OxideError::Internal(
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
        Ok(())
    }
}

fn book_shard_for_event(event: &PipelineEvent, shard_count: usize) -> usize {
    let id = match event {
        PipelineEvent::BookSnapshot(cmd) => cmd.asset_id.as_str(),
        PipelineEvent::PriceDelta(cmd) => cmd.asset_id.as_str(),
        PipelineEvent::BestBidAsk { asset_id, .. }
        | PipelineEvent::TickSizeChange { asset_id, .. }
        | PipelineEvent::LastTradePrice { asset_id, .. } => asset_id.as_str(),
        _ => return 0,
    };
    shard_index(id, shard_count)
}

struct BookApplyWorker {
    shard_id: usize,
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    coalescer_tx: Sender<TokenId>,
    settlement_tx: Sender<MarketSettlementRequest>,
    metrics: Arc<MetricsHub>,
    alerts: Arc<AlertDispatcher>,
    backpressure: Arc<BackpressurePolicy>,
    book_fact_writer: Option<Arc<BookFactWriter>>,
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
                tracing::info!(%market_id, known, "Market resolved via WS");
                self.metrics.markets_resolved_ws.inc();
                if let Some(writer) = &self.book_fact_writer {
                    writer.write_market_resolved(
                        &market_id,
                        &winning_token_id,
                        &winning_outcome,
                        &asset_ids,
                        timestamp_ms,
                    );
                }
                let request = MarketSettlementRequest {
                    market_id: market_id.clone(),
                    winning_token_id,
                    winning_outcome,
                    source: SettlementTrigger::Ws,
                    observed_at: Utc::now(),
                };
                if let Err(error) = self.settlement_tx.try_send(request) {
                    self.metrics.settlement_channel_dropped_total.inc();
                    tracing::error!(%error, %market_id, "settlement channel send failed");
                    self.alerts.dispatch_background(Alert {
                        severity: AlertLevel::Warning,
                        title: "Settlement channel dropped".to_owned(),
                        body: format!(
                            "Market {market_id} resolution event was dropped; Gamma resolved-market retry is the safety net"
                        ),
                        timestamp: Utc::now(),
                    });
                }
            }

            PipelineEvent::TickSizeChange {
                asset_id,
                old_tick,
                new_tick,
                ..
            } => {
                tracing::info!(%asset_id, %old_tick, %new_tick, "Tick size changed");
                if let Some(writer) = &self.book_fact_writer {
                    writer.write_tick_size_change(
                        &asset_id,
                        self.market_registry.market_for_token(&asset_id),
                        old_tick,
                        new_tick,
                    );
                }
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
                if let Some(writer) = &self.book_fact_writer {
                    writer.write_bbo(
                        &asset_id,
                        self.market_registry.market_for_token(&asset_id),
                        best_bid,
                        best_ask,
                        timestamp_ms,
                    );
                }
            }

            PipelineEvent::LastTradePrice {
                asset_id,
                price,
                timestamp_ms,
                ..
            } => {
                if let Some(writer) = &self.book_fact_writer {
                    writer.write_last_trade(
                        &asset_id,
                        self.market_registry.market_for_token(&asset_id),
                        price,
                        timestamp_ms,
                    );
                }
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
        if let Some(writer) = &self.book_fact_writer {
            writer.write_snapshot(
                cmd,
                self.market_registry.market_for_token(&cmd.asset_id),
                version,
            );
        }
        self.notify_coalescer(&cmd.asset_id);
        self.metrics.book_snapshots_applied.inc();
    }

    fn handle_price_delta(&self, cmd: &PriceDeltaCmd) {
        let version = self.book_store.apply_delta(
            &cmd.asset_id,
            cmd.changes.iter().map(|d| (d.side, d.price, d.size)),
            cmd.timestamp_ms,
            Some(LatencyTrace::from_ingress(cmd.trace.mono)),
        );
        if let Some(writer) = &self.book_fact_writer {
            writer.write_delta(
                cmd,
                self.market_registry.market_for_token(&cmd.asset_id),
                version,
            );
        }
        self.notify_coalescer(&cmd.asset_id);
        self.metrics.price_changes_applied.inc();
    }

    fn notify_coalescer(&self, asset_id: &TokenId) {
        if self.coalescer_tx.try_send(asset_id.clone()).is_ok() {
            self.backpressure.on_coalescer_notify_success(asset_id);
        } else {
            self.backpressure.on_coalescer_channel_full(asset_id);
        }
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
        if let Some(writer) = &self.book_fact_writer {
            self.write_shard_status_facts(writer, shard_id, status);
        }
    }

    fn write_shard_status_facts(
        &self,
        writer: &BookFactWriter,
        shard_id: usize,
        status: ShardConnectionStatus,
    ) {
        writer.write_shard_status(shard_id, status);
        let reason = match status {
            ShardConnectionStatus::Reconnecting { .. } => Some(ChSnapshotReason::Reconnect),
            ShardConnectionStatus::Disconnected => Some(ChSnapshotReason::Gap),
            ShardConnectionStatus::Connected => None,
        };
        if let Some(reason) = reason {
            for (token_id, snapshot) in self.book_store.published_snapshots() {
                if snapshot.timestamp_ms > 0 {
                    writer.write_published_snapshot(
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
