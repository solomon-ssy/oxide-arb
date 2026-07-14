use super::{
    book_store::BookStore, event_source::PipelineEventSource, market_registry::MarketRegistry,
};
use crate::{
    infra::sharding::shard_index,
    observability::{
        book_fact_writer::{BookFactWriter, MarketWsTradeFact},
        metrics_hub::MetricsHub,
    },
    service::system_status_nudge::SystemStatusNudge,
};
use dashmap::DashSet;
use flume::Receiver;
use quant_pivot_error::{QuantError, infra::InfraError};
use quant_pivot_models::{
    clickhouse::{BookStreamSessionRow, ChSchemaVersion},
    domain::{
        BookSnapshotCmd, PriceDeltaCmd,
        latency::LatencyTrace,
        pipeline::{IngressTrace, PipelineEvent, StreamSessionEndReason},
    },
    enums::{
        clickhouse::{ChBookEventType, ChStreamSessionEndReason, ChStreamSessionState},
        system::ShardConnectionStatus,
    },
    types::{Shares, TokenId},
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Sharded book-apply workers for ~500 markets / ~1000 tokens on one host.
pub const DEFAULT_BOOK_SHARD_COUNT: usize = 4;
pub const DEFAULT_BOOK_CHANNEL_CAPACITY: usize = 2048;
const BOOK_CHANNEL_TIMEOUT: Duration = Duration::from_millis(250);

/// Dependencies injected into [`DataPipeline`].
pub struct DataPipelineDeps {
    pub event_source: Arc<dyn PipelineEventSource>,
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub metrics: Arc<MetricsHub>,
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
        let invalid_sessions = Arc::new(DashSet::new());
        let mut book_threads = Vec::with_capacity(shard_count);
        for rx in book_receivers {
            let worker = BookApplyWorker {
                book_store: Arc::clone(&self.book_store),
                market_registry: Arc::clone(&self.market_registry),
                metrics: Arc::clone(&self.metrics),
                event_source: Arc::clone(&self.event_source),
                book_fact_writer: Arc::clone(&book_fact_writer),
                stream_state: HashMap::new(),
                invalid_sessions: Arc::clone(&invalid_sessions),
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
                        let token_id = pipeline_event.asset_id().cloned();
                        if !matches!(
                            timeout(BOOK_CHANNEL_TIMEOUT, book_senders[shard].send_async(pipeline_event)).await,
                            Ok(Ok(()))
                        ) {
                            self.book_store.mark_gap();
                            if let Some(token_id) = token_id {
                                self.event_source.invalidate_token(&token_id);
                            }
                            return Err(InfraError::ChannelTimeout { name: "book_apply" }.into());
                        }
                    } else {
                        tracing::error!("Pipeline event channel closed unexpectedly");
                        drop(book_senders);
                        for handle in book_threads {
                            handle.join().ok();
                        }
                        self.book_fact_writer.flush_pending_microstructure();
                        return Err(InfraError::ChannelClosed {
                            name: "pipeline_events",
                        }
                        .into());
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
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    metrics: Arc<MetricsHub>,
    event_source: Arc<dyn PipelineEventSource>,
    book_fact_writer: Arc<BookFactWriter>,
    stream_state: HashMap<TokenId, TokenStreamState>,
    invalid_sessions: Arc<DashSet<uuid::Uuid>>,
}

#[derive(Debug, Clone, Copy)]
struct TokenStreamState {
    session_id: uuid::Uuid,
    last_sequence: u64,
    has_fresh_snapshot: bool,
}

struct SessionClose {
    stream_session_id: uuid::Uuid,
    shard_id: u32,
    subscription_token_hash: quant_pivot_models::types::ContentHash,
    subscription_token_count: u32,
    received_sequences: Arc<[(TokenId, u64)]>,
    opened_at_ms: i64,
    closed_at_ms: i64,
    reason: StreamSessionEndReason,
}

impl BookApplyWorker {
    fn run(mut self, rx: &Receiver<PipelineEvent>) {
        while let Ok(event) = rx.recv() {
            self.handle_event(event);
        }
    }

    #[inline]
    fn handle_event(&mut self, event: PipelineEvent) {
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
                trace,
            } => {
                tracing::info!(%asset_id, %old_tick, %new_tick, "Tick size changed");
                if self.accept_sequence(&asset_id, trace)
                    && !self.book_fact_writer.write_tick_size_change(
                        &asset_id,
                        self.market_registry.market_for_token(&asset_id),
                        old_tick,
                        new_tick,
                        trace,
                    )
                {
                    self.invalid_sessions.insert(trace.stream_session_id);
                    self.invalidate_token(&asset_id);
                }
            }

            PipelineEvent::ShardStatus { shard_id, status } => {
                self.on_shard_status(shard_id, status);
            }

            PipelineEvent::BestBidAsk { .. } => {}

            PipelineEvent::LastTradePrice {
                market_id,
                asset_id,
                price,
                side,
                size,
                fee_rate_bps,
                timestamp_ms,
                trace,
            } => {
                if self.accept_sequence(&asset_id, trace)
                    && !self.book_fact_writer.write_last_trade(MarketWsTradeFact {
                        token_id: &asset_id,
                        market_id,
                        price,
                        side,
                        trade_size: size,
                        fee_rate_bps,
                        timestamp_ms,
                        trace,
                    })
                {
                    self.invalid_sessions.insert(trace.stream_session_id);
                    self.invalidate_token(&asset_id);
                }
            }
            PipelineEvent::StreamSessionOpened {
                stream_session_id,
                shard_id,
                subscription_token_hash,
                subscription_token_count,
                opened_at_ms,
            } => {
                if !self.book_fact_writer.write_stream_session_open(
                    stream_session_id,
                    shard_id,
                    subscription_token_hash,
                    subscription_token_count,
                    opened_at_ms,
                ) {
                    self.invalid_sessions.insert(stream_session_id);
                    self.book_store.mark_gap();
                }
            }
            PipelineEvent::StreamSessionClosed {
                stream_session_id,
                shard_id,
                subscription_token_hash,
                subscription_token_count,
                received_sequences,
                opened_at_ms,
                closed_at_ms,
                reason,
            } => self.handle_session_close(SessionClose {
                stream_session_id,
                shard_id,
                subscription_token_hash,
                subscription_token_count,
                received_sequences,
                opened_at_ms,
                closed_at_ms,
                reason,
            }),
            PipelineEvent::StreamGap {
                asset_id,
                stream_session_id,
                shard_id,
                last_received_sequence,
                timestamp_ms,
            } => {
                let _ = self.book_fact_writer.write_gap(
                    &asset_id,
                    self.market_registry.market_for_token(&asset_id),
                    stream_session_id,
                    shard_id,
                    last_received_sequence.saturating_add(1),
                    timestamp_ms,
                );
                self.invalid_sessions.insert(stream_session_id);
                self.invalidate_token(&asset_id);
            }
        }
    }

    fn handle_book_snapshot(&mut self, cmd: &BookSnapshotCmd) {
        if !self.accept_sequence(&cmd.asset_id, cmd.trace) {
            self.invalid_sessions.insert(cmd.trace.stream_session_id);
            self.invalidate_token(&cmd.asset_id);
            return;
        }
        let market_id = self.market_registry.market_for_token(&cmd.asset_id);
        let Some(source_event_hash) = self
            .book_fact_writer
            .write_snapshot_event(cmd, market_id.clone())
        else {
            self.invalid_sessions.insert(cmd.trace.stream_session_id);
            self.invalidate_token(&cmd.asset_id);
            return;
        };
        self.book_store.apply_snapshot(
            &cmd.asset_id,
            Arc::clone(&cmd.bids.levels),
            Arc::clone(&cmd.asks.levels),
            cmd.timestamp_ms,
            Some(LatencyTrace::from_ingress(cmd.trace.mono)),
        );
        if let Some(state) = self.stream_state.get_mut(&cmd.asset_id) {
            state.has_fresh_snapshot = true;
        }
        if let Some(snapshot) = self.book_store.load(&cmd.asset_id) {
            let checkpoint_persisted = self.book_fact_writer.write_checkpoint(
                &cmd.asset_id,
                market_id.clone(),
                &snapshot,
                cmd.trace.stream_session_id,
                cmd.trace.token_sequence,
                source_event_hash,
            );
            if !checkpoint_persisted {
                self.invalid_sessions.insert(cmd.trace.stream_session_id);
                self.invalidate_token(&cmd.asset_id);
                return;
            }
            self.book_fact_writer.write_microstructure_snapshot(
                &cmd.asset_id,
                market_id,
                &snapshot,
                ChBookEventType::Snapshot,
                0,
            );
        }
        self.event_source
            .mark_token_applied(&cmd.asset_id, Instant::now());
        self.metrics.book_snapshots_applied.inc();
    }

    fn handle_price_delta(&mut self, cmd: &PriceDeltaCmd) {
        if !self.accept_sequence(&cmd.asset_id, cmd.trace) {
            self.invalid_sessions.insert(cmd.trace.stream_session_id);
            self.invalidate_token(&cmd.asset_id);
            return;
        }
        let market_id = self.market_registry.market_for_token(&cmd.asset_id);
        if self
            .book_fact_writer
            .write_delta_event(cmd, market_id.clone())
            .is_none()
        {
            self.invalid_sessions.insert(cmd.trace.stream_session_id);
            self.invalidate_token(&cmd.asset_id);
            return;
        }
        if !self
            .stream_state
            .get(&cmd.asset_id)
            .is_some_and(|state| state.has_fresh_snapshot)
        {
            self.invalid_sessions.insert(cmd.trace.stream_session_id);
            self.invalidate_token(&cmd.asset_id);
            return;
        }
        self.book_store.apply_delta(
            &cmd.asset_id,
            cmd.changes.iter().map(|d| (d.side, d.price, d.size)),
            cmd.timestamp_ms,
            Some(LatencyTrace::from_ingress(cmd.trace.mono)),
        );
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
        self.event_source
            .mark_token_applied(&cmd.asset_id, Instant::now());
        self.metrics.price_changes_applied.inc();
    }

    fn accept_sequence(&mut self, token_id: &TokenId, trace: IngressTrace) -> bool {
        if trace.stream_session_id.is_nil() || trace.token_sequence == 0 {
            return false;
        }
        match self.stream_state.get_mut(token_id) {
            Some(state) if state.session_id == trace.stream_session_id => {
                if trace.token_sequence != state.last_sequence.saturating_add(1) {
                    state.has_fresh_snapshot = false;
                    return false;
                }
                state.last_sequence = trace.token_sequence;
            }
            Some(state) => {
                *state = TokenStreamState {
                    session_id: trace.stream_session_id,
                    last_sequence: trace.token_sequence,
                    has_fresh_snapshot: false,
                };
            }
            None => {
                self.stream_state.insert(
                    token_id.clone(),
                    TokenStreamState {
                        session_id: trace.stream_session_id,
                        last_sequence: trace.token_sequence,
                        has_fresh_snapshot: false,
                    },
                );
            }
        }
        true
    }

    fn invalidate_token(&mut self, token_id: &TokenId) {
        if let Some(state) = self.stream_state.get_mut(token_id) {
            state.has_fresh_snapshot = false;
        }
        self.book_store.mark_gap();
        self.event_source.invalidate_token(token_id);
    }

    fn handle_session_close(&mut self, close: SessionClose) {
        let sequences = close
            .received_sequences
            .iter()
            .map(|(token, sequence)| (token.as_str(), *sequence))
            .collect::<BTreeMap<_, _>>();
        let Ok(sequence_json) = serde_json::to_string(&sequences) else {
            self.book_store.mark_gap();
            return;
        };
        let (mut state, mut end_reason) = match close.reason {
            StreamSessionEndReason::Normal => (
                ChStreamSessionState::Sealed,
                ChStreamSessionEndReason::Normal,
            ),
            StreamSessionEndReason::Resubscribe => (
                ChStreamSessionState::Sealed,
                ChStreamSessionEndReason::Resubscribe,
            ),
            StreamSessionEndReason::Overflow => (
                ChStreamSessionState::Invalidated,
                ChStreamSessionEndReason::Overflow,
            ),
            StreamSessionEndReason::Disconnect => (
                ChStreamSessionState::Invalidated,
                ChStreamSessionEndReason::Disconnect,
            ),
            StreamSessionEndReason::Shutdown => (
                ChStreamSessionState::Sealed,
                ChStreamSessionEndReason::Shutdown,
            ),
        };
        if self
            .invalid_sessions
            .remove(&close.stream_session_id)
            .is_some()
        {
            state = ChStreamSessionState::Invalidated;
            end_reason = ChStreamSessionEndReason::Overflow;
        }
        let persisted = self
            .book_fact_writer
            .write_stream_session_close(BookStreamSessionRow {
                stream_session_id: close.stream_session_id,
                shard_id: close.shard_id,
                ledger_sequence: 2,
                state,
                end_reason,
                subscription_token_hash: close.subscription_token_hash,
                subscription_token_count: close.subscription_token_count,
                received_sequence_json: sequence_json.clone(),
                persisted_sequence_json: sequence_json,
                opened_at: close.opened_at_ms,
                recorded_at: close.closed_at_ms,
                schema_version: ChSchemaVersion(2),
            });
        if !persisted || state == ChStreamSessionState::Invalidated {
            for (token_id, _) in close.received_sequences.iter() {
                self.invalidate_token(token_id);
            }
        }
    }

    /// Shard connectivity surfaces: per-transition detail stays at debug —
    /// aggregate health is the `HealthChecker` summary plus a per-shard gauge.
    fn on_shard_status(&self, shard_id: usize, status: ShardConnectionStatus) {
        tracing::debug!(shard_id, ?status, "Shard status change");
        self.metrics.shard_status_changes.inc();
        let connected = matches!(status, ShardConnectionStatus::Connected);
        if !connected {
            self.book_store.mark_gap();
        }
        self.metrics
            .ws_shard_connected
            .with_label_values(&[&shard_id.to_string()])
            .set(i64::from(connected));
    }
}
