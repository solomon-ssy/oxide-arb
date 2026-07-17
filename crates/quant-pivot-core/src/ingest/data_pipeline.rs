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
use flume::{Receiver, Sender};
use futures_util::future::join_all;
use parking_lot::Mutex;
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
    types::{ContentHash, Shares, TokenId},
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{task::JoinSet, time::timeout};
use tokio_util::sync::CancellationToken;

const MIN_BOOK_SHARD_COUNT: usize = 4;
// Canonical L2 persistence is one shared batching plane. Hundreds of async
// token-affine workers are enough to fill it without creating one task and one
// independently tiny queue per configured token.
const MAX_BOOK_SHARD_COUNT: usize = 512;
// A worker micro-batch must fit in its mailbox. Four micro-batches of headroom
// absorb the measured snapshot/checkpoint barrier and five-minute-market burst;
// sustained overload still reaches the bounded timeout and fails closed.
const MIN_BOOK_CHANNEL_CAPACITY: usize = 1_024;
const BOOK_CHANNEL_HEADROOM_PER_TOKEN: usize = 256;
const BOOK_CHANNEL_TIMEOUT: Duration = Duration::from_millis(250);
const SHUTDOWN_DRAIN_QUIET_PERIOD: Duration = Duration::from_millis(250);
const BACKPRESSURE_WARN_INTERVAL: Duration = Duration::from_secs(5);
const MAX_CANONICAL_MICRO_BATCH_SIZE: usize = 256;
const CANONICAL_COALESCE_WINDOW: Duration = Duration::from_millis(2);

const fn canonical_identity(event: &PipelineEvent) -> Option<(&TokenId, IngressTrace)> {
    match event {
        PipelineEvent::BookSnapshot(command) => Some((&command.asset_id, command.trace)),
        PipelineEvent::PriceDelta(command) => Some((&command.asset_id, command.trace)),
        PipelineEvent::TickSizeChange {
            asset_id, trace, ..
        }
        | PipelineEvent::LastTradePrice {
            asset_id, trace, ..
        } => Some((asset_id, *trace)),
        _ => None,
    }
}

/// Size token-affine workers from the configured maximum live subscription set.
///
/// Each async worker waits for canonical `ClickHouse` persistence, while
/// independent token shards wait concurrently. Worker count is capped by the
/// shared sink's useful concurrency and channel capacity is derived from a
/// fixed amount of burst headroom per configured token. Flume grows its queue
/// on demand, so the upper bound does not preallocate the worst-case capacity.
#[must_use]
pub fn book_apply_topology(max_subscription_tokens: usize) -> (usize, usize) {
    let token_budget = max_subscription_tokens.max(1);
    let shard_count = token_budget
        .checked_next_power_of_two()
        .unwrap_or(MAX_BOOK_SHARD_COUNT)
        .clamp(MIN_BOOK_SHARD_COUNT, MAX_BOOK_SHARD_COUNT);
    let total_capacity = token_budget
        .saturating_mul(BOOK_CHANNEL_HEADROOM_PER_TOKEN)
        .max(shard_count.saturating_mul(MIN_BOOK_CHANNEL_CAPACITY));
    let channel_capacity = total_capacity.div_ceil(shard_count);
    (shard_count, channel_capacity)
}

fn book_worker_index(event: &PipelineEvent, shard_count: usize) -> usize {
    match event {
        PipelineEvent::StreamSessionOpened { shard_id, .. }
        | PipelineEvent::StreamSessionClosed { shard_id, .. } => {
            usize::try_from(*shard_id).unwrap_or(usize::MAX) % shard_count
        }
        PipelineEvent::ShardStatus { shard_id, .. } => *shard_id % shard_count,
        PipelineEvent::MarketResolved {
            winning_token_id, ..
        } => shard_index(winning_token_id.as_str(), shard_count),
        _ => event
            .asset_id()
            .map_or(0, |asset_id| shard_index(asset_id.as_str(), shard_count)),
    }
}

const fn pipeline_event_kind(event: &PipelineEvent) -> &'static str {
    match event {
        PipelineEvent::BookSnapshot(_) => "book_snapshot",
        PipelineEvent::PriceDelta(_) => "price_delta",
        PipelineEvent::TickSizeChange { .. } => "tick_size_change",
        PipelineEvent::LastTradePrice { .. } => "last_trade_price",
        PipelineEvent::MarketResolved { .. } => "market_resolved",
        PipelineEvent::ShardStatus { .. } => "shard_status",
        PipelineEvent::StreamSessionOpened { .. } => "stream_session_opened",
        PipelineEvent::StreamSessionClosed { .. } => "stream_session_closed",
        PipelineEvent::StreamGap { .. } => "stream_gap",
    }
}

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

/// Main WS event loop with token-affine async book workers.
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
    book_apply_timeouts_since_warn: AtomicU64,
    last_book_apply_warn: Mutex<Option<Instant>>,
}

#[derive(Clone)]
enum BackpressureScope {
    None,
    Token(TokenId),
    Subscription(Arc<[TokenId]>),
    Received(Arc<[(TokenId, u64)]>),
}

impl BackpressureScope {
    fn from_event(event: &PipelineEvent) -> Self {
        match event {
            PipelineEvent::StreamSessionOpened {
                subscription_tokens,
                ..
            } => Self::Subscription(Arc::clone(subscription_tokens)),
            PipelineEvent::StreamSessionClosed {
                received_sequences, ..
            } => Self::Received(Arc::clone(received_sequences)),
            _ => event.asset_id().cloned().map_or(Self::None, Self::Token),
        }
    }

    fn invalidate(self, event_source: &dyn PipelineEventSource) -> usize {
        match self {
            Self::None => 0,
            Self::Token(token_id) => {
                event_source.invalidate_token(&token_id);
                1
            }
            Self::Subscription(token_ids) => {
                event_source.invalidate_tokens(&token_ids);
                token_ids.len()
            }
            Self::Received(received_sequences) => {
                let token_ids = received_sequences
                    .iter()
                    .map(|(token_id, _)| token_id.clone())
                    .collect::<Vec<_>>();
                event_source.invalidate_tokens(&token_ids);
                token_ids.len()
            }
        }
    }

    fn diagnostic_token(&self) -> Option<&TokenId> {
        match self {
            Self::Token(token_id) => Some(token_id),
            Self::Subscription(token_ids) => token_ids.first(),
            Self::Received(received_sequences) => {
                received_sequences.first().map(|(token_id, _)| token_id)
            }
            Self::None => None,
        }
    }
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
            book_apply_timeouts_since_warn: AtomicU64::new(0),
            last_book_apply_warn: Mutex::new(None),
        }
    }

    /// Run until shutdown or channel close.
    pub async fn run(&self) -> Result<(), QuantError> {
        let shard_count = self.book_shard_count.max(1);
        tracing::info!(
            shard_count,
            channel_capacity = self.book_channel_capacity,
            "book-apply topology initialized"
        );
        let mut book_senders = Vec::with_capacity(shard_count);
        let mut book_receivers = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let (tx, rx) = flume::bounded(self.book_channel_capacity);
            book_senders.push(tx);
            book_receivers.push(rx);
        }

        let book_fact_writer = Arc::clone(&self.book_fact_writer);
        let invalid_sessions = Arc::new(DashSet::new());
        let mut book_tasks = JoinSet::new();
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
            book_tasks.spawn(worker.run(rx));
        }

        let rx = self.event_source.events();
        let failure = loop {
            tokio::select! {
                biased;

                () = self.shutdown.cancelled() => {
                    tracing::info!("DataPipeline draining ingress after shutdown");
                    break self
                        .drain_ingress(rx, &book_senders, shard_count)
                        .await
                        .err();
                }

                event = rx.recv_async() => {
                    let Ok(pipeline_event) = event else {
                        tracing::error!("Pipeline event channel closed unexpectedly");
                        break Some(InfraError::ChannelClosed {
                            name: "pipeline_events",
                        }.into());
                    };
                    if let Err(error) = self
                        .dispatch_to_book_worker(pipeline_event, &book_senders, shard_count)
                        .await
                    {
                        break Some(error);
                    }
                }
            }
        };

        drop(book_senders);
        let mut failure = failure;
        while let Some(result) = book_tasks.join_next().await {
            if let Err(error) = result
                && failure.is_none()
            {
                failure = Some(
                    InfraError::BlockingTaskJoin {
                        detail: format!("book-apply task failed: {error}"),
                    }
                    .into(),
                );
            }
        }
        // Flush open microstructure buckets before analytics writers drain (WsIngress
        // stage precedes Analytics in shutdown ordering).
        self.book_fact_writer.flush_pending_microstructure();
        failure.map_or(Ok(()), Err)
    }

    async fn drain_ingress(
        &self,
        rx: &Receiver<PipelineEvent>,
        book_senders: &[Sender<PipelineEvent>],
        shard_count: usize,
    ) -> Result<(), QuantError> {
        let mut drained = 0_u64;
        loop {
            let Ok(Ok(event)) = timeout(SHUTDOWN_DRAIN_QUIET_PERIOD, rx.recv_async()).await else {
                tracing::info!(drained, "DataPipeline ingress drain complete");
                return Ok(());
            };
            self.dispatch_to_book_worker(event, book_senders, shard_count)
                .await?;
            drained = drained.saturating_add(1);
        }
    }

    async fn dispatch_to_book_worker(
        &self,
        pipeline_event: PipelineEvent,
        book_senders: &[Sender<PipelineEvent>],
        shard_count: usize,
    ) -> Result<(), QuantError> {
        if pipeline_event.is_market_data_event()
            && !self.market_data_nudged.swap(true, Ordering::AcqRel)
        {
            self.status_nudge.nudge();
        }
        let shard = book_worker_index(&pipeline_event, shard_count);
        let event_kind = pipeline_event_kind(&pipeline_event);
        let backpressure_scope = BackpressureScope::from_event(&pipeline_event);
        match timeout(
            BOOK_CHANNEL_TIMEOUT,
            book_senders[shard].send_async(pipeline_event),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => {
                tracing::error!(shard, "Book-apply worker channel closed unexpectedly");
                Err(InfraError::ChannelClosed { name: "book_apply" }.into())
            }
            Err(_) => {
                self.handle_book_apply_timeout(
                    shard,
                    event_kind,
                    book_senders[shard].len(),
                    backpressure_scope,
                );
                Ok(())
            }
        }
    }

    fn handle_book_apply_timeout(
        &self,
        shard: usize,
        event_kind: &'static str,
        queue_depth: usize,
        scope: BackpressureScope,
    ) {
        self.book_store.mark_gap();
        let diagnostic_token = scope.diagnostic_token().cloned();
        let affected_tokens = scope.invalidate(self.event_source.as_ref());
        self.metrics
            .book_apply_backpressure_invalidations
            .inc_by(u64::try_from(affected_tokens.max(1)).unwrap_or(u64::MAX));
        self.book_apply_timeouts_since_warn
            .fetch_add(1, Ordering::Relaxed);

        let mut last_warn = self.last_book_apply_warn.lock();
        if last_warn.is_some_and(|at| at.elapsed() < BACKPRESSURE_WARN_INTERVAL) {
            return;
        }
        *last_warn = Some(Instant::now());
        drop(last_warn);
        let timeouts_since_last = self
            .book_apply_timeouts_since_warn
            .swap(0, Ordering::Relaxed);
        tracing::warn!(
            shard,
            event_kind,
            queue_depth,
            channel_capacity = self.book_channel_capacity,
            affected_tokens,
            token_id = diagnostic_token.as_ref().map(TokenId::as_str),
            timeouts_since_last,
            "Book-apply queue saturated; continuity invalidated and owning WS shards restarted"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::book_apply_topology;

    #[test]
    fn topology_scales_durable_ack_concurrency_without_unbounded_queues() {
        assert_eq!(book_apply_topology(2_000), (512, 1_024));
        assert_eq!(book_apply_topology(1), (4, 1_024));
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
    subscription_token_hash: ContentHash,
    subscription_token_count: u32,
    received_sequences: Arc<[(TokenId, u64)]>,
    opened_at_ms: i64,
    closed_at_ms: i64,
    reason: StreamSessionEndReason,
}

impl BookApplyWorker {
    async fn run(mut self, rx: Receiver<PipelineEvent>) {
        let mut deferred = None;
        loop {
            let event = if let Some(event) = deferred.take() {
                event
            } else {
                let Ok(event) = rx.recv_async().await else {
                    return;
                };
                event
            };
            if canonical_identity(&event).is_none() {
                self.handle_event(event).await;
                continue;
            }

            let mut events = Vec::with_capacity(MAX_CANONICAL_MICRO_BATCH_SIZE);
            events.push(event);
            let mut disconnected = false;
            while events.len() < MAX_CANONICAL_MICRO_BATCH_SIZE {
                match timeout(CANONICAL_COALESCE_WINDOW, rx.recv_async()).await {
                    Ok(Ok(event)) if canonical_identity(&event).is_some() => events.push(event),
                    Ok(Ok(event)) => {
                        deferred = Some(event);
                        break;
                    }
                    Ok(Err(_)) => {
                        disconnected = true;
                        break;
                    }
                    Err(_) => break,
                }
            }
            self.handle_canonical_batch(events).await;
            if disconnected {
                return;
            }
        }
    }

    #[inline]
    async fn handle_event(&mut self, event: PipelineEvent) {
        self.metrics.ws_events_received.inc();

        match event {
            PipelineEvent::BookSnapshot(_)
            | PipelineEvent::PriceDelta(_)
            | PipelineEvent::TickSizeChange { .. }
            | PipelineEvent::LastTradePrice { .. } => {
                unreachable!("canonical events are handled by handle_canonical_batch")
            }

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

            PipelineEvent::ShardStatus { shard_id, status } => {
                self.on_shard_status(shard_id, status);
            }

            PipelineEvent::StreamSessionOpened {
                stream_session_id,
                shard_id,
                subscription_token_hash,
                subscription_token_count,
                subscription_tokens: _,
                opened_at_ms,
            } => {
                if !self
                    .book_fact_writer
                    .write_stream_session_open(
                        stream_session_id,
                        shard_id,
                        subscription_token_hash,
                        subscription_token_count,
                        opened_at_ms,
                    )
                    .await
                {
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
            } => {
                self.handle_session_close(SessionClose {
                    stream_session_id,
                    shard_id,
                    subscription_token_hash,
                    subscription_token_count,
                    received_sequences,
                    opened_at_ms,
                    closed_at_ms,
                    reason,
                })
                .await;
            }
            PipelineEvent::StreamGap {
                asset_id,
                stream_session_id,
                shard_id,
                last_received_sequence,
                timestamp_ms,
            } => {
                let _ = self
                    .book_fact_writer
                    .write_gap(
                        &asset_id,
                        self.market_registry.market_for_token(&asset_id),
                        stream_session_id,
                        shard_id,
                        last_received_sequence.saturating_add(1),
                        timestamp_ms,
                    )
                    .await;
                self.invalid_sessions.insert(stream_session_id);
                self.invalidate_token(&asset_id);
            }
        }
    }

    async fn handle_canonical_batch(&mut self, events: Vec<PipelineEvent>) {
        let started_at = Instant::now();
        self.metrics
            .ws_events_received
            .inc_by(u64::try_from(events.len()).unwrap_or(u64::MAX));

        let mut failed_sessions = HashSet::new();
        for event in &events {
            let Some((token_id, trace)) = canonical_identity(event) else {
                continue;
            };
            if failed_sessions.contains(&trace.stream_session_id) {
                continue;
            }
            let accepted = self.accept_sequence(token_id, trace);
            let fresh_enough = match event {
                PipelineEvent::BookSnapshot(_) => {
                    if let Some(state) = self.stream_state.get_mut(token_id) {
                        state.has_fresh_snapshot = true;
                    }
                    true
                }
                PipelineEvent::PriceDelta(_) => self
                    .stream_state
                    .get(token_id)
                    .is_some_and(|state| state.has_fresh_snapshot),
                _ => true,
            };
            if !accepted || !fresh_enough {
                failed_sessions.insert(trace.stream_session_id);
            }
        }

        let persisted = join_all(events.iter().map(|event| async {
            let Some((_, trace)) = canonical_identity(event) else {
                return true;
            };
            failed_sessions.contains(&trace.stream_session_id)
                || self.persist_canonical_event(event).await
        }))
        .await;
        for (event, persisted) in events.iter().zip(persisted) {
            if !persisted && let Some((_, trace)) = canonical_identity(event) {
                failed_sessions.insert(trace.stream_session_id);
            }
        }

        if !failed_sessions.is_empty() {
            self.invalidate_batch_sessions(&events, &failed_sessions);
        }
        let events = events
            .into_iter()
            .filter(|event| {
                canonical_identity(event)
                    .is_none_or(|(_, trace)| !failed_sessions.contains(&trace.stream_session_id))
            })
            .collect::<Vec<_>>();
        let persistence_elapsed = started_at.elapsed();
        let event_count = events.len();
        let unique_token_count = events
            .iter()
            .filter_map(PipelineEvent::asset_id)
            .collect::<HashSet<_>>()
            .len();
        self.apply_canonical_events(events);
        let total_elapsed = started_at.elapsed();
        if total_elapsed >= Duration::from_millis(50) {
            tracing::debug!(
                event_count,
                unique_tokens = unique_token_count,
                persistence_ms = persistence_elapsed.as_millis(),
                apply_ms = total_elapsed
                    .saturating_sub(persistence_elapsed)
                    .as_millis(),
                total_ms = total_elapsed.as_millis(),
                "slow heterogeneous canonical micro-batch"
            );
        }
    }

    async fn persist_canonical_event(&self, event: &PipelineEvent) -> bool {
        match event {
            PipelineEvent::BookSnapshot(command) => self
                .book_fact_writer
                .write_snapshot_bundle(
                    command,
                    self.market_registry.market_for_token(&command.asset_id),
                )
                .await
                .is_some(),
            PipelineEvent::PriceDelta(command) => self
                .book_fact_writer
                .write_delta_event(
                    command,
                    self.market_registry.market_for_token(&command.asset_id),
                )
                .await
                .is_some(),
            PipelineEvent::TickSizeChange {
                asset_id,
                old_tick,
                new_tick,
                trace,
            } => {
                self.book_fact_writer
                    .write_tick_size_change(
                        asset_id,
                        self.market_registry.market_for_token(asset_id),
                        *old_tick,
                        *new_tick,
                        *trace,
                    )
                    .await
            }
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
                self.book_fact_writer
                    .write_last_trade(MarketWsTradeFact {
                        token_id: asset_id,
                        market_id: market_id.clone(),
                        price: *price,
                        side: *side,
                        trade_size: *size,
                        fee_rate_bps: *fee_rate_bps,
                        timestamp_ms: *timestamp_ms,
                        trace: *trace,
                    })
                    .await
            }
            _ => true,
        }
    }

    fn apply_canonical_events(&mut self, events: Vec<PipelineEvent>) {
        let mut deltas = Vec::new();
        for event in events {
            match event {
                PipelineEvent::PriceDelta(command) => deltas.push(command),
                event => {
                    self.apply_price_delta_batch(&deltas);
                    deltas.clear();
                    match event {
                        PipelineEvent::BookSnapshot(command) => {
                            self.apply_book_snapshot(&command);
                        }
                        PipelineEvent::TickSizeChange {
                            asset_id,
                            old_tick,
                            new_tick,
                            ..
                        } => {
                            tracing::info!(%asset_id, %old_tick, %new_tick, "Tick size changed");
                        }
                        PipelineEvent::LastTradePrice { .. } => {}
                        _ => unreachable!("only canonical events are applied here"),
                    }
                }
            }
        }
        self.apply_price_delta_batch(&deltas);
    }

    fn apply_book_snapshot(&mut self, cmd: &BookSnapshotCmd) {
        let market_id = self.market_registry.market_for_token(&cmd.asset_id);
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

    fn apply_price_delta_batch(&self, commands: &[PriceDeltaCmd]) {
        if commands.is_empty() {
            return;
        }
        let market_ids = commands
            .iter()
            .map(|command| self.market_registry.market_for_token(&command.asset_id))
            .collect::<Vec<_>>();
        let mut token_order = Vec::new();
        let mut token_commands = HashMap::<TokenId, Vec<usize>>::new();
        for (index, command) in commands.iter().enumerate() {
            if !token_commands.contains_key(&command.asset_id) {
                token_order.push(command.asset_id.clone());
            }
            token_commands
                .entry(command.asset_id.clone())
                .or_default()
                .push(index);
        }
        for token_id in token_order {
            let Some(indices) = token_commands.remove(&token_id) else {
                continue;
            };
            let Some(last_index) = indices.last().copied() else {
                continue;
            };
            let last_command = &commands[last_index];
            self.book_store.apply_delta(
                &token_id,
                indices.iter().flat_map(|index| {
                    commands[*index]
                        .changes
                        .iter()
                        .map(|delta| (delta.side, delta.price, delta.size))
                }),
                last_command.timestamp_ms,
                Some(LatencyTrace::from_ingress(last_command.trace.mono)),
            );
            if let Some(snapshot) = self.book_store.load(&token_id) {
                let delete_count = indices
                    .iter()
                    .flat_map(|index| commands[*index].changes.iter())
                    .filter(|change| change.size <= Shares::ZERO)
                    .count();
                self.book_fact_writer.write_microstructure_snapshot(
                    &token_id,
                    market_ids[last_index].clone(),
                    &snapshot,
                    ChBookEventType::Delta,
                    u64::try_from(delete_count).unwrap_or(u64::MAX),
                );
            }
            self.event_source
                .mark_token_applied(&token_id, Instant::now());
        }
        self.metrics
            .price_changes_applied
            .inc_by(u64::try_from(commands.len()).unwrap_or(u64::MAX));
    }

    fn invalidate_batch_sessions(
        &mut self,
        events: &[PipelineEvent],
        failed_sessions: &HashSet<uuid::Uuid>,
    ) {
        let mut invalidated_tokens = HashSet::new();
        for event in events {
            let Some((token_id, trace)) = canonical_identity(event) else {
                continue;
            };
            if failed_sessions.contains(&trace.stream_session_id) {
                self.invalid_sessions.insert(trace.stream_session_id);
                invalidated_tokens.insert(token_id.clone());
                if let Some(state) = self.stream_state.get_mut(token_id) {
                    state.has_fresh_snapshot = false;
                }
            }
        }
        if invalidated_tokens.is_empty() {
            return;
        }
        self.book_store.mark_gap();
        self.event_source
            .invalidate_tokens(&invalidated_tokens.into_iter().collect::<Vec<_>>());
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

    async fn handle_session_close(&mut self, close: SessionClose) {
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
            })
            .await;
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
