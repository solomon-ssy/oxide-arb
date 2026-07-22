use std::{
    array,
    collections::{BTreeMap, HashMap, HashSet},
    mem, slice,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use ahash::AHashMap;
use chrono::Utc;
use flume::{Receiver, Sender as FlumeSender};
use parking_lot::Mutex;
use quant_pivot_api::ws::{NormalizedIngressBatch, estimated_event_bytes};
use quant_pivot_error::{QuantError, infra::InfraError};
use quant_pivot_models::{
    clickhouse::{BookL2LedgerRow, BookStreamSessionRow, ChSchemaVersion},
    domain::{
        data_plane::{
            BookSnapshotCmd, PriceDeltaCmd,
            latency::LatencyTrace,
            pipeline::{IngressTrace, PipelineEvent, StreamSessionEndReason},
        },
        market::book::BookSnapshot,
    },
    enums::{
        clickhouse::{ChBookEventType, ChStreamSessionEndReason, ChStreamSessionState},
        system::ShardConnectionStatus,
    },
    types::{ContentHash, PartitionBatchId, PartitionId, Shares, TokenId, TokenKey},
};
use tokio::{
    sync::{
        Notify, OwnedSemaphorePermit,
        mpsc::{self, Receiver as MpscReceiver, Sender as MpscSender},
    },
    task::JoinSet,
    time::{MissedTickBehavior, interval, timeout},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    book_store::BookStore,
    data_plane_index::TokenSlotState,
    event_source::PipelineEventSource,
    market_registry::MarketRegistry,
    order_book::{BookDeltaScratch, OrderBook},
};
use crate::{
    observability::{
        book_fact_writer::{BookFactWriter, MarketWsTradeFact, MicrostructureAccumulator},
        ledger_persistence::{LEDGER_PARTITION_COUNT, PartitionLedgerClient},
        metrics_hub::MetricsHub,
    },
    service::system_status_nudge::SystemStatusNudge,
};

pub const PARTITION_COUNT: usize = LEDGER_PARTITION_COUNT;
const PARTITION_MAILBOX_CAPACITY: usize = 256;
const MAX_PARTITION_BATCH_EVENTS: usize = 1_024;
const MAX_PARTITION_BATCH_BYTES: usize = 1_024 * 1_024;
const BOOK_CHANNEL_TIMEOUT: Duration = Duration::from_millis(250);
const SHUTDOWN_DRAIN_QUIET_PERIOD: Duration = Duration::from_millis(250);
const BACKPRESSURE_WARN_INTERVAL: Duration = Duration::from_secs(5);
const MAX_CANONICAL_MICRO_BATCH_SIZE: usize = 256;

const fn session_generation(session_id: Uuid) -> u64 {
    let bytes = session_id.as_bytes();
    let upper = u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    let lower = u64::from_be_bytes([
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ]);
    upper ^ lower
}

const fn canonical_identity(event: &PipelineEvent) -> Option<(TokenKey, IngressTrace)> {
    match event {
        PipelineEvent::BookSnapshot(command) => Some((command.token, command.trace)),
        PipelineEvent::PriceDelta(command) => Some((command.token, command.trace)),
        PipelineEvent::TickSizeChange { token, trace, .. }
        | PipelineEvent::LastTradePrice { token, trace, .. } => Some((*token, *trace)),
        _ => None,
    }
}

fn partition_index(event: &PipelineEvent) -> usize {
    match event {
        PipelineEvent::StreamSessionOpened { shard_id, .. }
        | PipelineEvent::StreamSessionClosed { shard_id, .. } => {
            usize::try_from(*shard_id).unwrap_or(usize::MAX) % PARTITION_COUNT
        }
        PipelineEvent::ShardStatus { shard_id, .. } => *shard_id % PARTITION_COUNT,
        PipelineEvent::MarketResolved { winning_token, .. } => {
            winning_token.index() % PARTITION_COUNT
        }
        _ => event
            .token()
            .map_or(0, |token| token.index() % PARTITION_COUNT),
    }
}

const fn partition_batch_would_overflow(
    event_count: usize,
    retained_bytes: usize,
    next_event_bytes: usize,
) -> bool {
    event_count >= MAX_PARTITION_BATCH_EVENTS
        || retained_bytes.saturating_add(next_event_bytes) > MAX_PARTITION_BATCH_BYTES
}

fn accept_token_sequence(
    stream_state: &mut HashMap<TokenKey, TokenStreamState>,
    token: TokenKey,
    trace: IngressTrace,
) -> bool {
    if trace.stream_session_id.is_nil() || trace.token_sequence == 0 {
        return false;
    }
    match stream_state.get_mut(&token) {
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
            stream_state.insert(
                token,
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
    shutdown: CancellationToken,
    status_nudge: SystemStatusNudge,
    market_data_nudged: Arc<AtomicBool>,
    book_apply_timeouts_since_warn: AtomicU64,
    last_book_apply_warn: Mutex<Option<Instant>>,
}

#[derive(Clone)]
enum BackpressureScope {
    None,
    Tokens(Vec<TokenKey>),
    Subscription(Arc<[TokenId]>),
    Received(Arc<[(TokenKey, u64)]>),
}

impl BackpressureScope {
    fn from_events(events: &[PipelineEvent]) -> Self {
        if let Some(scope) = events.iter().find_map(|event| match event {
            PipelineEvent::StreamSessionOpened {
                subscription_tokens,
                ..
            } => Some(Self::Subscription(Arc::clone(subscription_tokens))),
            PipelineEvent::StreamSessionClosed {
                received_sequences, ..
            } => Some(Self::Received(Arc::clone(received_sequences))),
            _ => None,
        }) {
            return scope;
        }
        let mut tokens = Vec::new();
        for token in events.iter().filter_map(PipelineEvent::token) {
            if !tokens.contains(&token) {
                tokens.push(token);
            }
        }
        if tokens.is_empty() {
            Self::None
        } else {
            Self::Tokens(tokens)
        }
    }

    fn invalidate(
        &self,
        event_source: &dyn PipelineEventSource,
        book_store: &BookStore,
        registry: &MarketRegistry,
    ) -> usize {
        match self {
            Self::None => 0,
            Self::Tokens(tokens) => {
                book_store.invalidate_tokens(tokens);
                let token_ids = tokens
                    .iter()
                    .filter_map(|token| registry.token_id(*token))
                    .collect::<Vec<_>>();
                event_source.invalidate_tokens(&token_ids);
                tokens.len()
            }
            Self::Subscription(token_ids) => {
                book_store.invalidate_ids(token_ids);
                event_source.invalidate_tokens(token_ids);
                token_ids.len()
            }
            Self::Received(received_sequences) => {
                let tokens = received_sequences
                    .iter()
                    .map(|(token, _)| *token)
                    .collect::<Vec<_>>();
                book_store.invalidate_tokens(&tokens);
                let token_ids = tokens
                    .iter()
                    .filter_map(|token| registry.token_id(*token))
                    .collect::<Vec<_>>();
                event_source.invalidate_tokens(&token_ids);
                tokens.len()
            }
        }
    }

    fn diagnostic_token(&self, registry: &MarketRegistry) -> Option<TokenId> {
        match self {
            Self::Tokens(tokens) => registry.token_id(*tokens.first()?),
            Self::Subscription(token_ids) => token_ids.first().cloned(),
            Self::Received(received_sequences) => registry.token_id(received_sequences.first()?.0),
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
            shutdown: deps.shutdown,
            status_nudge: deps.status_nudge,
            market_data_nudged: Arc::new(AtomicBool::new(false)),
            book_apply_timeouts_since_warn: AtomicU64::new(0),
            last_book_apply_warn: Mutex::new(None),
        }
    }

    /// Run until shutdown or channel close.
    pub async fn run(&self) -> Result<(), QuantError> {
        tracing::info!(
            partition_count = PARTITION_COUNT,
            mailbox_capacity = PARTITION_MAILBOX_CAPACITY,
            "fixed token-affine partition topology initialized"
        );
        let mut partition_senders = Vec::with_capacity(PARTITION_COUNT);
        let mut recycle_receivers = Vec::with_capacity(PARTITION_COUNT);
        let mut partition_tasks = JoinSet::new();
        for partition in 0..PARTITION_COUNT {
            let (tx, rx) = mpsc::channel(PARTITION_MAILBOX_CAPACITY);
            let (recycle_tx, recycle_rx) = flume::unbounded();
            partition_senders.push(tx);
            recycle_receivers.push(recycle_rx);
            let partition_id = PartitionId::new(u8::try_from(partition).unwrap_or(u8::MAX));
            let ledger = self
                .book_fact_writer
                .ledger_client(partition_id)
                .ok_or_else(|| InfraError::Misconfigured {
                    detail: format!("missing ledger commit cursor for partition {partition}"),
                })?;
            let actor = PartitionActor {
                partition_id,
                book_store: Arc::clone(&self.book_store),
                market_registry: Arc::clone(&self.market_registry),
                metrics: Arc::clone(&self.metrics),
                event_source: Arc::clone(&self.event_source),
                book_fact_writer: Arc::clone(&self.book_fact_writer),
                ledger,
                next_batch_id: 0,
                stream_state: HashMap::new(),
                invalid_sessions: HashSet::new(),
                books: AHashMap::new(),
                delta_command_order: Vec::new(),
                delta_scratch: BookDeltaScratch::default(),
                delta_commands: Vec::new(),
            };
            partition_tasks.spawn(actor.run(rx, recycle_tx));
        }

        let mut buffers = array::from_fn(|_| Vec::new());
        let mut buffer_bytes = [0_usize; PARTITION_COUNT];
        let failure = loop {
            tokio::select! {
                biased;

                () = self.shutdown.cancelled() => {
                    tracing::info!("DataPipeline draining ingress after shutdown");
                    break self
                        .drain_ingress(
                            self.event_source.events(),
                            &partition_senders,
                            &recycle_receivers,
                            &mut buffers,
                            &mut buffer_bytes,
                        )
                        .await
                        .err();
                }

                batch = self.event_source.events().recv_async() => {
                    let Ok(batch) = batch else {
                        tracing::error!("Pipeline event channel closed unexpectedly");
                        break Some(InfraError::ChannelClosed {
                            name: "pipeline_events",
                        }.into());
                    };
                    if let Err(error) = self
                        .dispatch_ingress_batch(
                            batch,
                            &partition_senders,
                            &recycle_receivers,
                            &mut buffers,
                            &mut buffer_bytes,
                        )
                        .await
                    {
                        break Some(error);
                    }
                }
            }
        };

        drop(partition_senders);
        let mut failure = failure;
        while let Some(result) = partition_tasks.join_next().await {
            if let Err(error) = result
                && failure.is_none()
            {
                failure = Some(
                    InfraError::BlockingTaskJoin {
                        detail: format!("partition actor failed: {error}"),
                    }
                    .into(),
                );
            }
        }
        failure.map_or(Ok(()), Err)
    }

    async fn drain_ingress(
        &self,
        rx: &Receiver<NormalizedIngressBatch>,
        partition_senders: &[MpscSender<PartitionMessage>],
        recycle_receivers: &[Receiver<Vec<PipelineEvent>>],
        buffers: &mut [Vec<PipelineEvent>; PARTITION_COUNT],
        buffer_bytes: &mut [usize; PARTITION_COUNT],
    ) -> Result<(), QuantError> {
        let mut drained = 0_u64;
        loop {
            let Ok(Ok(batch)) = timeout(SHUTDOWN_DRAIN_QUIET_PERIOD, rx.recv_async()).await else {
                tracing::info!(drained, "DataPipeline ingress drain complete");
                return Ok(());
            };
            self.dispatch_ingress_batch(
                batch,
                partition_senders,
                recycle_receivers,
                buffers,
                buffer_bytes,
            )
            .await?;
            drained = drained.saturating_add(1);
        }
    }

    async fn dispatch_ingress_batch(
        &self,
        batch: NormalizedIngressBatch,
        partition_senders: &[MpscSender<PartitionMessage>],
        recycle_receivers: &[Receiver<Vec<PipelineEvent>>],
        buffers: &mut [Vec<PipelineEvent>; PARTITION_COUNT],
        buffer_bytes: &mut [usize; PARTITION_COUNT],
    ) -> Result<(), QuantError> {
        if batch.events.iter().any(PipelineEvent::is_market_data_event)
            && !self.market_data_nudged.swap(true, Ordering::AcqRel)
        {
            self.status_nudge.nudge();
        }
        let backpressure_scope = BackpressureScope::from_events(&batch.events);
        if !self
            .await_session_barrier(&batch.events, partition_senders, &backpressure_scope)
            .await?
        {
            return Ok(());
        }
        if let Some(event) = batch
            .events
            .iter()
            .find(|event| estimated_event_bytes(event) > MAX_PARTITION_BATCH_BYTES)
        {
            let partition = partition_index(event);
            self.handle_book_apply_timeout(
                partition,
                pipeline_event_kind(event),
                partition_queue_depth(&partition_senders[partition]),
                &backpressure_scope,
                "single event exceeds partition byte limit",
            );
            return Ok(());
        }
        let memory_permit = batch.memory_permit;
        for event in batch.events {
            let partition = partition_index(&event);
            let event_bytes = estimated_event_bytes(&event);
            if !buffers[partition].is_empty()
                && partition_batch_would_overflow(
                    buffers[partition].len(),
                    buffer_bytes[partition],
                    event_bytes,
                )
            {
                let events = mem::take(&mut buffers[partition]);
                buffer_bytes[partition] = 0;
                if !self
                    .send_partition_batch(
                        partition,
                        events,
                        Arc::clone(&memory_permit),
                        partition_senders,
                        &backpressure_scope,
                    )
                    .await?
                {
                    return Ok(());
                }
            }
            if buffers[partition].capacity() == 0
                && let Ok(recycled) = recycle_receivers[partition].try_recv()
            {
                buffers[partition] = recycled;
            }
            buffer_bytes[partition] = buffer_bytes[partition].saturating_add(event_bytes);
            buffers[partition].push(event);
        }

        for partition in 0..PARTITION_COUNT {
            if buffers[partition].is_empty() {
                continue;
            }
            let events = mem::take(&mut buffers[partition]);
            buffer_bytes[partition] = 0;
            if !self
                .send_partition_batch(
                    partition,
                    events,
                    Arc::clone(&memory_permit),
                    partition_senders,
                    &backpressure_scope,
                )
                .await?
            {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn send_partition_batch(
        &self,
        partition: usize,
        mut events: Vec<PipelineEvent>,
        memory_permit: Arc<OwnedSemaphorePermit>,
        partition_senders: &[MpscSender<PartitionMessage>],
        backpressure_scope: &BackpressureScope,
    ) -> Result<bool, QuantError> {
        let event_kind = events.first().map_or("empty", pipeline_event_kind);
        events.reverse();
        let batch = PartitionIngressBatch {
            events,
            memory_permit,
        };
        match timeout(
            BOOK_CHANNEL_TIMEOUT,
            partition_senders[partition].send(PartitionMessage::Events(batch)),
        )
        .await
        {
            Ok(Ok(())) => Ok(true),
            Ok(Err(_)) => {
                tracing::error!(partition, "Partition actor channel closed unexpectedly");
                Err(InfraError::ChannelClosed {
                    name: "partition_actor",
                }
                .into())
            }
            Err(_) => {
                self.handle_book_apply_timeout(
                    partition,
                    event_kind,
                    partition_queue_depth(&partition_senders[partition]),
                    backpressure_scope,
                    "partition mailbox timed out",
                );
                Ok(false)
            }
        }
    }

    async fn await_session_barrier(
        &self,
        events: &[PipelineEvent],
        partition_senders: &[MpscSender<PartitionMessage>],
        backpressure_scope: &BackpressureScope,
    ) -> Result<bool, QuantError> {
        let mut partitions = [false; PARTITION_COUNT];
        for received_sequences in events.iter().filter_map(|event| match event {
            PipelineEvent::StreamSessionClosed {
                received_sequences, ..
            } => Some(received_sequences.as_ref()),
            _ => None,
        }) {
            for (token, _) in received_sequences {
                partitions[token.index() % PARTITION_COUNT] = true;
            }
        }
        let partition_count = partitions.iter().filter(|included| **included).count();
        if partition_count == 0 {
            return Ok(true);
        }
        let barrier = Arc::new(PartitionBarrier::new(
            u8::try_from(partition_count).unwrap_or(u8::MAX),
        ));
        let send_and_wait = async {
            for (partition, included) in partitions.into_iter().enumerate() {
                if !included {
                    continue;
                }
                partition_senders[partition]
                    .send(PartitionMessage::Barrier(Arc::clone(&barrier)))
                    .await
                    .map_err(|_| partition)?;
            }
            barrier.wait().await;
            Ok::<(), usize>(())
        };
        match timeout(BOOK_CHANNEL_TIMEOUT, send_and_wait).await {
            Ok(Ok(())) => Ok(true),
            Ok(Err(partition)) => Err(InfraError::ChannelClosed {
                name: if partition < PARTITION_COUNT {
                    "partition_barrier"
                } else {
                    "partition_actor"
                },
            }
            .into()),
            Err(_) => {
                let partition = partitions
                    .iter()
                    .position(|included| *included)
                    .unwrap_or(0);
                self.handle_book_apply_timeout(
                    partition,
                    "stream_session_closed",
                    partition_queue_depth(&partition_senders[partition]),
                    backpressure_scope,
                    "session drain barrier timed out",
                );
                Ok(false)
            }
        }
    }

    fn handle_book_apply_timeout(
        &self,
        partition: usize,
        event_kind: &'static str,
        queue_depth: usize,
        scope: &BackpressureScope,
        reason: &'static str,
    ) {
        self.book_store.mark_gap();
        let diagnostic_token = scope.diagnostic_token(&self.market_registry);
        let affected_tokens = scope.invalidate(
            self.event_source.as_ref(),
            &self.book_store,
            &self.market_registry,
        );
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
            partition,
            event_kind,
            queue_depth,
            channel_capacity = PARTITION_MAILBOX_CAPACITY,
            affected_tokens,
            token_id = diagnostic_token.as_ref().map(TokenId::as_str),
            timeouts_since_last,
            reason,
            "Partition queue rejected a batch; continuity invalidated and owning WS shards restarted"
        );
    }
}

fn partition_queue_depth(sender: &MpscSender<PartitionMessage>) -> usize {
    sender.max_capacity().saturating_sub(sender.capacity())
}

struct PartitionIngressBatch {
    events: Vec<PipelineEvent>,
    memory_permit: Arc<OwnedSemaphorePermit>,
}

enum PartitionMessage {
    Events(PartitionIngressBatch),
    Barrier(Arc<PartitionBarrier>),
}

struct PartitionBarrier {
    remaining: AtomicU8,
    completed: Notify,
}

impl PartitionBarrier {
    const fn new(partitions: u8) -> Self {
        Self {
            remaining: AtomicU8::new(partitions),
            completed: Notify::const_new(),
        }
    }

    fn arrive(&self) {
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.completed.notify_one();
        }
    }

    async fn wait(&self) {
        loop {
            let completed = self.completed.notified();
            if self.remaining.load(Ordering::Acquire) == 0 {
                return;
            }
            completed.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Instant};

    use quant_pivot_models::{
        domain::data_plane::pipeline::{IngressTrace, PipelineEvent},
        enums::system::ShardConnectionStatus,
        types::TokenKey,
    };
    use uuid::Uuid;

    use super::{
        MAX_PARTITION_BATCH_BYTES, MAX_PARTITION_BATCH_EVENTS, PARTITION_COUNT,
        PARTITION_MAILBOX_CAPACITY, PartitionBarrier, accept_token_sequence,
        partition_batch_would_overflow, partition_index,
    };

    #[test]
    fn token_partition_is_fixed_and_affine() {
        for value in 0..2_000_u32 {
            let event = PipelineEvent::StreamGap {
                token: TokenKey::new(value),
                stream_session_id: Uuid::nil(),
                shard_id: 0,
                last_received_sequence: 0,
                timestamp_ms: 0,
            };
            assert_eq!(partition_index(&event), value as usize % PARTITION_COUNT);
        }
        assert_eq!(PARTITION_COUNT, 8);
        assert_eq!(PARTITION_MAILBOX_CAPACITY, 256);
    }

    #[test]
    fn control_events_are_bounded_to_the_same_partition_set() {
        let event = PipelineEvent::ShardStatus {
            shard_id: usize::MAX,
            status: ShardConnectionStatus::Connected,
        };
        assert!(partition_index(&event) < PARTITION_COUNT);
    }

    #[tokio::test]
    async fn session_barrier_waits_for_every_affected_partition() {
        let barrier = Arc::new(PartitionBarrier::new(2));
        barrier.arrive();
        let waiter = {
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move { barrier.wait().await })
        };
        assert!(!waiter.is_finished());
        barrier.arrive();
        waiter.await.expect("barrier waiter");
    }

    #[test]
    fn partition_batch_caps_are_enforced_before_push() {
        assert!(partition_batch_would_overflow(
            MAX_PARTITION_BATCH_EVENTS,
            0,
            1
        ));
        assert!(partition_batch_would_overflow(
            1,
            MAX_PARTITION_BATCH_BYTES,
            1
        ));
        assert!(!partition_batch_would_overflow(
            MAX_PARTITION_BATCH_EVENTS - 1,
            MAX_PARTITION_BATCH_BYTES - 1,
            1
        ));
    }

    #[test]
    fn token_sequence_is_monotonic_and_resets_on_new_session() {
        let token = TokenKey::new(7);
        let first_session = Uuid::new_v4();
        let second_session = Uuid::new_v4();
        let mut states = HashMap::new();
        let trace = |session, sequence| IngressTrace {
            mono: Instant::now(),
            ingress_time_ms: 0,
            ws_timestamp_ms: 0,
            stream_session_id: session,
            shard_id: 0,
            token_sequence: sequence,
        };

        assert!(accept_token_sequence(
            &mut states,
            token,
            trace(first_session, 1)
        ));
        assert!(!accept_token_sequence(
            &mut states,
            token,
            trace(first_session, 3)
        ));
        assert!(accept_token_sequence(
            &mut states,
            token,
            trace(second_session, 1)
        ));
    }
}

struct PartitionActor {
    partition_id: PartitionId,
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    metrics: Arc<MetricsHub>,
    event_source: Arc<dyn PipelineEventSource>,
    book_fact_writer: Arc<BookFactWriter>,
    ledger: PartitionLedgerClient,
    next_batch_id: u64,
    stream_state: HashMap<TokenKey, TokenStreamState>,
    invalid_sessions: HashSet<Uuid>,
    books: AHashMap<TokenKey, MutableBookState>,
    delta_command_order: Vec<usize>,
    delta_scratch: BookDeltaScratch,
    delta_commands: Vec<PriceDeltaCmd>,
}

struct MutableBookState {
    book: OrderBook,
    version: u64,
    microstructure: MicrostructureAccumulator,
}

impl MutableBookState {
    fn new(token_id: TokenId, version: u64) -> Self {
        Self {
            book: OrderBook::new(token_id),
            version,
            microstructure: MicrostructureAccumulator::default(),
        }
    }

    fn next_snapshot(&mut self) -> BookSnapshot {
        self.version = self.version.saturating_add(1);
        self.book.publish_cow(self.version)
    }
}

#[derive(Debug, Clone, Copy)]
struct TokenStreamState {
    session_id: Uuid,
    last_sequence: u64,
    has_fresh_snapshot: bool,
}

struct SessionClose {
    stream_session_id: Uuid,
    shard_id: u32,
    subscription_token_hash: ContentHash,
    subscription_token_count: u32,
    received_sequences: Arc<[(TokenKey, u64)]>,
    opened_at_ms: i64,
    closed_at_ms: i64,
    reason: StreamSessionEndReason,
}

impl PartitionActor {
    async fn run(
        mut self,
        mut rx: MpscReceiver<PartitionMessage>,
        recycle_tx: FlumeSender<Vec<PipelineEvent>>,
    ) {
        let mut telemetry_flush = interval(Duration::from_secs(1));
        telemetry_flush.set_missed_tick_behavior(MissedTickBehavior::Skip);
        telemetry_flush.tick().await;
        loop {
            let Some(message) = (tokio::select! {
                message = rx.recv() => message,
                _ = telemetry_flush.tick() => {
                    self.flush_elapsed_microstructure();
                    continue;
                }
            }) else {
                break;
            };
            let (mut events, memory_permit) = match message {
                PartitionMessage::Events(PartitionIngressBatch {
                    events,
                    memory_permit,
                }) => (events, memory_permit),
                PartitionMessage::Barrier(barrier) => {
                    barrier.arrive();
                    continue;
                }
            };
            let mut canonical =
                Vec::with_capacity(events.len().min(MAX_CANONICAL_MICRO_BATCH_SIZE));
            events.reverse();
            while let Some(event) = events.pop() {
                debug_assert_eq!(
                    partition_index(&event),
                    usize::from(self.partition_id.get()),
                    "router violated token affinity"
                );
                if canonical_identity(&event).is_some() {
                    canonical.push(event);
                    if canonical.len() == MAX_CANONICAL_MICRO_BATCH_SIZE {
                        self.handle_canonical_batch(mem::take(&mut canonical)).await;
                    }
                    continue;
                }
                if !canonical.is_empty() {
                    self.handle_canonical_batch(mem::take(&mut canonical)).await;
                }
                self.handle_event(event).await;
            }
            if !canonical.is_empty() {
                self.handle_canonical_batch(canonical).await;
            }
            events.clear();
            let _ = recycle_tx.try_send(events);
            drop(memory_permit);
        }
        for state in self.books.values_mut() {
            if let Some(row) = state.microstructure.flush() {
                self.book_fact_writer.write_microstructure_row(row);
            }
        }
    }

    fn flush_elapsed_microstructure(&mut self) {
        let now_ms = Utc::now().timestamp_millis();
        for state in self.books.values_mut() {
            if let Some(row) = state.microstructure.flush_elapsed(now_ms) {
                self.book_fact_writer.write_microstructure_row(row);
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
                winning_token,
                winning_outcome,
                tokens,
                timestamp_ms,
                ..
            } => {
                let known = self.market_registry.get_market(&market_id).is_some();
                tracing::info!(%market_id, known, "Market resolved via WS (ingest only)");
                self.metrics.markets_resolved_ws.inc();
                let Some(winning_token_id) = self.market_registry.token_id(winning_token) else {
                    tracing::error!(
                        ?winning_token,
                        "resolved event lost registered winning token"
                    );
                    return;
                };
                let asset_ids = tokens
                    .iter()
                    .filter_map(|token| self.market_registry.token_id(*token))
                    .collect::<Vec<_>>();
                if asset_ids.len() != tokens.len() {
                    tracing::error!(%market_id, "resolved event lost registered outcome token");
                    return;
                }
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
                subscription_tokens,
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
                    self.book_store.invalidate_ids(&subscription_tokens);
                    self.event_source.invalidate_tokens(&subscription_tokens);
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
                token,
                stream_session_id,
                shard_id,
                last_received_sequence,
                timestamp_ms,
            } => {
                let Some(token_id) = self.market_registry.token_id(token) else {
                    tracing::error!(?token, "stream gap lost registered token");
                    return;
                };
                if let Some(row) = BookFactWriter::gap_ledger_row(
                    &token_id,
                    self.market_registry.market_for_key(token),
                    stream_session_id,
                    shard_id,
                    last_received_sequence.saturating_add(1),
                    timestamp_ms,
                ) && let Some(batch_id) = self.allocate_batch_id()
                    && let Err(error) = self
                        .ledger
                        .persist(batch_id, vec![row], BOOK_CHANNEL_TIMEOUT)
                        .await
                {
                    tracing::error!(?error, ?token, "gap ledger persistence failed");
                }
                self.invalid_sessions.insert(stream_session_id);
                self.invalidate_token(token);
            }
        }
    }

    async fn handle_canonical_batch(&mut self, events: Vec<PipelineEvent>) {
        let started_at = Instant::now();
        self.metrics
            .ws_events_received
            .inc_by(u64::try_from(events.len()).unwrap_or(u64::MAX));

        let batch_scope = events
            .iter()
            .filter_map(|event| {
                canonical_identity(event).map(|(token, trace)| (token, trace.stream_session_id))
            })
            .collect::<Vec<_>>();
        let mut failed_sessions = HashSet::new();
        for event in &events {
            let Some((token, trace)) = canonical_identity(event) else {
                continue;
            };
            if failed_sessions.contains(&trace.stream_session_id) {
                continue;
            }
            let accepted = accept_token_sequence(&mut self.stream_state, token, trace);
            let fresh_enough = match event {
                PipelineEvent::BookSnapshot(_) => {
                    if let Some(state) = self.stream_state.get_mut(&token) {
                        state.has_fresh_snapshot = true;
                    }
                    true
                }
                PipelineEvent::PriceDelta(_) => self
                    .stream_state
                    .get(&token)
                    .is_some_and(|state| state.has_fresh_snapshot),
                _ => true,
            };
            if !accepted || !fresh_enough {
                failed_sessions.insert(trace.stream_session_id);
            }
        }

        let mut prepared = Vec::with_capacity(events.len());
        for event in events {
            let Some((_, trace)) = canonical_identity(&event) else {
                continue;
            };
            if failed_sessions.contains(&trace.stream_session_id) {
                continue;
            }
            if let Some(row) = self.prepare_ledger_event(&event) {
                prepared.push((event, row));
            } else {
                failed_sessions.insert(trace.stream_session_id);
            }
        }
        prepared.retain(|(event, _)| {
            canonical_identity(event)
                .is_some_and(|(_, trace)| !failed_sessions.contains(&trace.stream_session_id))
        });

        let mut rows = Vec::with_capacity(prepared.len());
        let mut committed = Vec::with_capacity(prepared.len());
        for (event, row) in prepared {
            rows.push(row);
            committed.push(event);
        }
        if !rows.is_empty() {
            let persisted = if let Some(batch_id) = self.allocate_batch_id() {
                self.ledger
                    .persist(batch_id, rows, BOOK_CHANNEL_TIMEOUT)
                    .await
                    .is_ok()
            } else {
                false
            };
            if !persisted {
                failed_sessions.extend(committed.iter().filter_map(|event| {
                    canonical_identity(event).map(|(_, trace)| trace.stream_session_id)
                }));
            }
        }

        if !failed_sessions.is_empty() {
            self.invalidate_batch_sessions(&batch_scope, &failed_sessions);
        }
        let events = committed
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
            .filter_map(PipelineEvent::token)
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

    fn prepare_ledger_event(&self, event: &PipelineEvent) -> Option<BookL2LedgerRow> {
        match event {
            PipelineEvent::BookSnapshot(command) => {
                let token_id = self.market_registry.token_id(command.token)?;
                BookFactWriter::snapshot_ledger_row(
                    command,
                    &token_id,
                    self.market_registry.market_for_key(command.token),
                )
            }
            PipelineEvent::PriceDelta(command) => {
                let token_id = self.market_registry.token_id(command.token)?;
                BookFactWriter::delta_ledger_row(
                    command,
                    &token_id,
                    self.market_registry.market_for_key(command.token),
                )
            }
            PipelineEvent::TickSizeChange {
                token,
                old_tick,
                new_tick,
                trace,
            } => {
                let token_id = self.market_registry.token_id(*token)?;
                BookFactWriter::tick_size_ledger_row(
                    &token_id,
                    self.market_registry.market_for_key(*token),
                    *old_tick,
                    *new_tick,
                    *trace,
                )
            }
            PipelineEvent::LastTradePrice {
                market_id,
                token,
                price,
                side,
                size,
                fee_rate_bps,
                timestamp_ms,
                trace,
            } => {
                let token_id = self.market_registry.token_id(*token)?;
                BookFactWriter::last_trade_ledger_row(&MarketWsTradeFact {
                    token_id: &token_id,
                    market_id: market_id.clone(),
                    price: *price,
                    side: *side,
                    trade_size: *size,
                    fee_rate_bps: *fee_rate_bps,
                    timestamp_ms: *timestamp_ms,
                    trace: *trace,
                })
            }
            _ => None,
        }
    }

    fn allocate_batch_id(&mut self) -> Option<PartitionBatchId> {
        self.next_batch_id = self.next_batch_id.checked_add(1)?;
        Some(PartitionBatchId::new(self.next_batch_id))
    }

    fn apply_canonical_events(&mut self, events: Vec<PipelineEvent>) {
        let mut deltas = mem::take(&mut self.delta_commands);
        deltas.clear();
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
                            token,
                            old_tick,
                            new_tick,
                            trace,
                        } => {
                            tracing::info!(?token, %old_tick, %new_tick, "Tick size changed");
                            self.book_store.mark_canonical_fresh(
                                token,
                                trace.token_sequence,
                                session_generation(trace.stream_session_id),
                            );
                        }
                        PipelineEvent::LastTradePrice { token, trace, .. } => {
                            self.book_store.mark_canonical_fresh(
                                token,
                                trace.token_sequence,
                                session_generation(trace.stream_session_id),
                            );
                        }
                        _ => unreachable!("only canonical events are applied here"),
                    }
                }
            }
        }
        self.apply_price_delta_batch(&deltas);
        deltas.clear();
        self.delta_commands = deltas;
    }

    fn apply_book_snapshot(&mut self, cmd: &BookSnapshotCmd) {
        let market_id = self.market_registry.market_for_key(cmd.token);
        let Some(token_id) = self.market_registry.token_id(cmd.token) else {
            tracing::error!(token = ?cmd.token, "snapshot lost registered token metadata");
            self.invalidate_token(cmd.token);
            return;
        };
        let initial_version = self.book_store.book_version(cmd.token);
        let state = self
            .books
            .entry(cmd.token)
            .or_insert_with(|| MutableBookState::new(token_id.clone(), initial_version));
        state
            .book
            .apply_snapshot_arc(&cmd.bids.levels, &cmd.asks.levels, cmd.timestamp_ms);
        let snapshot = state.next_snapshot();
        let stale_microstructure = state.microstructure.observe(
            &token_id,
            market_id,
            &snapshot,
            ChBookEventType::Snapshot,
            0,
        );
        if !self.book_store.publish(
            cmd.token,
            snapshot,
            cmd.trace.token_sequence,
            session_generation(cmd.trace.stream_session_id),
            Some(LatencyTrace::from_ingress(cmd.trace.mono)),
        ) {
            self.invalidate_token(cmd.token);
            return;
        }
        if let Some(row) = stale_microstructure {
            self.book_fact_writer.write_microstructure_row(row);
        }
        if let Some(state) = self.stream_state.get_mut(&cmd.token) {
            state.has_fresh_snapshot = true;
        }
        self.metrics.book_snapshots_applied.inc();
    }

    fn apply_price_delta_batch(&mut self, commands: &[PriceDeltaCmd]) {
        if commands.is_empty() {
            return;
        }
        self.delta_command_order.clear();
        self.delta_command_order.extend(0..commands.len());
        self.delta_command_order
            .sort_unstable_by_key(|index| (commands[*index].token.index(), *index));
        let mut start = 0;
        while start < self.delta_command_order.len() {
            let token = commands[self.delta_command_order[start]].token;
            let mut end = start + 1;
            while end < self.delta_command_order.len()
                && commands[self.delta_command_order[end]].token == token
            {
                end += 1;
            }
            let indices = &self.delta_command_order[start..end];
            let last_index = indices[indices.len() - 1];
            let last_command = &commands[last_index];
            let Some(token_id) = self.market_registry.token_id(token) else {
                tracing::error!(?token, "delta lost registered token metadata");
                start = end;
                continue;
            };
            let Some(state) = self.books.get_mut(&token) else {
                tracing::error!(
                    ?token,
                    "delta reached partition actor without mutable snapshot"
                );
                self.invalidate_token(token);
                start = end;
                continue;
            };
            state.book.apply_delta_with_scratch(
                indices.iter().flat_map(|index| {
                    commands[*index]
                        .changes
                        .iter()
                        .map(|delta| (delta.side, delta.price, delta.size))
                }),
                last_command.timestamp_ms,
                &mut self.delta_scratch,
            );
            let snapshot = state.next_snapshot();
            let delete_count = indices
                .iter()
                .flat_map(|index| commands[*index].changes.iter())
                .filter(|change| change.size <= Shares::ZERO)
                .count();
            let stale_microstructure = state.microstructure.observe(
                &token_id,
                self.market_registry.market_for_key(token),
                &snapshot,
                ChBookEventType::Delta,
                u64::try_from(delete_count).unwrap_or(u64::MAX),
            );
            if !self.book_store.publish(
                token,
                snapshot,
                last_command.trace.token_sequence,
                session_generation(last_command.trace.stream_session_id),
                Some(LatencyTrace::from_ingress(last_command.trace.mono)),
            ) {
                self.invalidate_token(token);
                start = end;
                continue;
            }
            if let Some(row) = stale_microstructure {
                self.book_fact_writer.write_microstructure_row(row);
            }
            start = end;
        }
        self.metrics
            .price_changes_applied
            .inc_by(u64::try_from(commands.len()).unwrap_or(u64::MAX));
    }

    fn invalidate_batch_sessions(
        &mut self,
        batch_scope: &[(TokenKey, Uuid)],
        failed_sessions: &HashSet<Uuid>,
    ) {
        let mut invalidated_tokens = HashSet::new();
        for (token, session_id) in batch_scope {
            if failed_sessions.contains(session_id) {
                self.invalid_sessions.insert(*session_id);
                invalidated_tokens.insert(*token);
                if let Some(state) = self.stream_state.get_mut(token) {
                    state.has_fresh_snapshot = false;
                }
            }
        }
        if invalidated_tokens.is_empty() {
            return;
        }
        self.book_store.mark_gap();
        let invalidated_tokens = invalidated_tokens.into_iter().collect::<Vec<_>>();
        self.book_store.invalidate_tokens(&invalidated_tokens);
        let token_ids = invalidated_tokens
            .iter()
            .filter_map(|token| self.market_registry.token_id(*token))
            .collect::<Vec<_>>();
        self.event_source.invalidate_tokens(&token_ids);
    }

    fn invalidate_token(&mut self, token: TokenKey) {
        if let Some(state) = self.stream_state.get_mut(&token) {
            state.has_fresh_snapshot = false;
        }
        self.book_store.invalidate_tokens(slice::from_ref(&token));
        self.book_store.mark_gap();
        if let Some(token_id) = self.market_registry.token_id(token) {
            self.event_source.invalidate_token(&token_id);
        }
    }

    async fn handle_session_close(&mut self, close: SessionClose) {
        let expected_generation = session_generation(close.stream_session_id);
        let continuity_invalid = close.received_sequences.iter().any(|(token, sequence)| {
            self.book_store.freshness(*token).is_none_or(|freshness| {
                freshness.state != TokenSlotState::Fresh
                    || freshness.session_generation != expected_generation
                    || freshness.sequence != *sequence
            })
        });
        let mut sequences = BTreeMap::new();
        for (token, sequence) in close.received_sequences.iter() {
            let Some(token_id) = self.market_registry.token_id(*token) else {
                self.book_store.mark_gap();
                return;
            };
            sequences.insert(token_id, *sequence);
        }
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
        if self.invalid_sessions.remove(&close.stream_session_id) || continuity_invalid {
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
            for (token, _) in close.received_sequences.iter() {
                self.invalidate_token(*token);
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
