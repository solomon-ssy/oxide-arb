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

use super::{
    book_store::BookStore,
    data_plane_index::TokenSlotState,
    event_source::PipelineEventSource,
    market_registry::MarketRegistry,
    order_book::{BookDeltaScratch, OrderBook},
    session_directory::SessionDirectory,
};
use crate::{
    observability::{
        book_fact_writer::{
            BookFactWriter, CANONICAL_WRITE_TIMEOUT, MarketWsTradeFact, MicrostructureAccumulator,
        },
        ledger_persistence::{LEDGER_PARTITION_COUNT, PartitionLedgerClient},
        metrics_hub::MetricsHub,
    },
    service::system_status_nudge::SystemStatusNudge,
};
use ahash::AHashMap;
use chrono::{DateTime, Utc};
use flume::{Receiver, Sender as FlumeSender};
use parking_lot::Mutex;
use quant_pivot_api::ws::{NormalizedIngressBatch, TransportRetirement, estimated_event_bytes};
use quant_pivot_error::{QuantError, QuantResult, infra::InfraError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::{BookL2LedgerRow, BookMicrostructureRow, BookStreamSessionRow, ChSchemaVersion},
    domain::{
        data_plane::{
            BookSnapshotCmd, PriceDeltaCmd,
            latency::LatencyTrace,
            pipeline::{IngressTrace, PipelineEvent, StreamSessionEndReason, StreamSessionTicket},
        },
        market::book::BookSnapshot,
    },
    enums::{
        clickhouse::{ChBookEventType, ChStreamSessionEndReason, ChStreamSessionState},
        system::ShardConnectionStatus,
    },
    hashing::CanonicalDigest,
    types::{ContentHash, PartitionBatchId, PartitionId, Shares, TokenId, TokenKey},
};
use tokio::{
    sync::{
        Notify, OwnedSemaphorePermit,
        mpsc::{
            self, OwnedPermit as MpscOwnedPermit, Receiver as MpscReceiver, Sender as MpscSender,
        },
        oneshot::{self, Sender as OneshotSender},
    },
    task::JoinSet,
    time::{MissedTickBehavior, interval, timeout},
};
use tokio_util::sync::CancellationToken;

pub const PARTITION_COUNT: usize = LEDGER_PARTITION_COUNT;
const PARTITION_MAILBOX_CAPACITY: usize = 256;
const MAX_PARTITION_BATCH_EVENTS: usize = 1_024;
const MAX_PARTITION_BATCH_BYTES: usize = 1_024 * 1_024;
const BOOK_CHANNEL_TIMEOUT: Duration = Duration::from_millis(250);
const SHUTDOWN_DRAIN_QUIET_PERIOD: Duration = Duration::from_millis(250);
const BACKPRESSURE_WARN_INTERVAL: Duration = Duration::from_secs(5);
const MAX_CANONICAL_MICRO_BATCH_SIZE: usize = 256;
const MICROSTRUCTURE_COMMIT_CAPACITY: usize = 16;

/// Report/replay port proving that every live microstructure bucket visible at
/// `source_cutoff` has crossed the durable `ClickHouse` acknowledgement boundary.
#[async_trait::async_trait]
pub trait MicrostructureCommitBarrier: Send + Sync {
    /// Persist every partition's eligible rows before a report reads features.
    async fn commit_through(&self, source_cutoff: DateTime<Utc>) -> QuantResult<()>;
}

struct MicrostructureCommitRequest {
    source_cutoff_ms: i64,
    response: OneshotSender<QuantResult<()>>,
}

/// Market-data publication kind observed after the durable ledger cursor has
/// acknowledged the canonical row and the fresh `BookStore` slot is visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableBookPublishKind {
    Snapshot,
    Delta,
}

/// One parser-ingress to durable-publication observation.
///
/// The hook is absent in the production composition and enabled only by the
/// system performance runner, keeping evidence collection explicit and
/// allocation-free here.
#[derive(Debug, Clone, Copy)]
pub struct DurableBookPublishSample {
    pub kind: DurableBookPublishKind,
    pub token: TokenKey,
    pub token_sequence: u64,
    pub session: StreamSessionTicket,
    pub ws_ingress: Instant,
    pub published_at: Instant,
}

pub type DurableBookPublishObserver = Arc<dyn Fn(DurableBookPublishSample) + Send + Sync>;

const fn canonical_identity(event: &PipelineEvent) -> Option<(TokenKey, IngressTrace)> {
    match event {
        PipelineEvent::BookSnapshot(command) => Some((command.token, command.trace)),
        PipelineEvent::PriceDelta(command) => Some((command.token, command.trace)),
        PipelineEvent::TickSizeChange { token, trace, .. }
        | PipelineEvent::LastTradePrice { token, trace, .. } => Some((*token, *trace)),
        _ => None,
    }
}

const fn pipeline_event_session(event: &PipelineEvent) -> Option<StreamSessionTicket> {
    match event {
        PipelineEvent::BookSnapshot(command) => Some(command.trace.session),
        PipelineEvent::PriceDelta(command) => Some(command.trace.session),
        PipelineEvent::TickSizeChange { trace, .. }
        | PipelineEvent::LastTradePrice { trace, .. } => Some(trace.session),
        PipelineEvent::StreamSessionOpened { session, .. }
        | PipelineEvent::StreamSessionClosed { session, .. }
        | PipelineEvent::StreamGap { session, .. } => Some(*session),
        PipelineEvent::MarketResolved { .. } | PipelineEvent::ShardStatus { .. } => None,
    }
}

fn split_events_by_session(events: Vec<PipelineEvent>) -> Vec<Vec<PipelineEvent>> {
    let mut group_index = AHashMap::<Option<StreamSessionTicket>, usize>::new();
    let mut groups = Vec::<Vec<PipelineEvent>>::new();
    for event in events {
        let session = pipeline_event_session(&event);
        let existing = group_index.get(&session).copied();
        let index = existing.unwrap_or_else(|| {
            let index = groups.len();
            group_index.insert(session, index);
            groups.push(Vec::new());
            index
        });
        groups[index].push(event);
    }
    groups
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
    if !trace.session.is_valid() || trace.token_sequence == 0 {
        return false;
    }
    match stream_state.get_mut(&token) {
        Some(state) if state.session == trace.session => {
            if trace.token_sequence != state.last_sequence.saturating_add(1) {
                state.has_fresh_snapshot = false;
                return false;
            }
            state.last_sequence = trace.token_sequence;
        }
        Some(state) => {
            *state = TokenStreamState {
                session: trace.session,
                last_sequence: trace.token_sequence,
                has_fresh_snapshot: false,
            };
        }
        None => {
            stream_state.insert(
                token,
                TokenStreamState {
                    session: trace.session,
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
    pub retirement_rx: Receiver<TransportRetirement>,
    pub durable_publish_observer: Option<DurableBookPublishObserver>,
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
    sessions: Arc<SessionDirectory>,
    retirement_rx: Receiver<TransportRetirement>,
    microstructure_commit_tx: FlumeSender<MicrostructureCommitRequest>,
    microstructure_commit_rx: Receiver<MicrostructureCommitRequest>,
    durable_publish_observer: Option<DurableBookPublishObserver>,
}

#[derive(Clone)]
enum BackpressureScope {
    None,
    Tokens {
        sessions: Vec<StreamSessionTicket>,
        tokens: Vec<TokenKey>,
    },
    Subscription {
        session: StreamSessionTicket,
        token_ids: Arc<[TokenId]>,
    },
    Closed {
        session: StreamSessionTicket,
        token_ids: Arc<[TokenId]>,
        sequences: Arc<[(TokenKey, u64)]>,
    },
}

struct InvalidationOutcome {
    affected_tokens: usize,
    transport_restart_requested: bool,
}

impl BackpressureScope {
    fn from_events(events: &[PipelineEvent]) -> Self {
        if let Some(scope) = events.iter().find_map(|event| match event {
            PipelineEvent::StreamSessionOpened {
                session,
                subscription_tokens,
                ..
            } => Some(Self::Subscription {
                session: *session,
                token_ids: Arc::clone(subscription_tokens),
            }),
            PipelineEvent::StreamSessionClosed {
                session,
                subscription_tokens,
                received_sequences,
                ..
            } => Some(Self::Closed {
                session: *session,
                token_ids: Arc::clone(subscription_tokens),
                sequences: Arc::clone(received_sequences),
            }),
            _ => None,
        }) {
            return scope;
        }
        let mut tokens = Vec::new();
        let mut sessions = Vec::new();
        for token in events.iter().filter_map(PipelineEvent::token) {
            if !tokens.contains(&token) {
                tokens.push(token);
            }
        }
        for session in events
            .iter()
            .filter_map(canonical_identity)
            .map(|(_, trace)| trace.session)
        {
            if !sessions.contains(&session) {
                sessions.push(session);
            }
        }
        if tokens.is_empty() {
            Self::None
        } else {
            Self::Tokens { sessions, tokens }
        }
    }

    fn invalidate(
        &self,
        event_source: &dyn PipelineEventSource,
        book_store: &BookStore,
        registry: &MarketRegistry,
        sessions: &SessionDirectory,
    ) -> InvalidationOutcome {
        match self {
            Self::None => InvalidationOutcome {
                affected_tokens: 0,
                transport_restart_requested: false,
            },
            Self::Tokens {
                sessions: tickets,
                tokens,
            } => {
                let mut invalidated_token_ids = Vec::new();
                for ticket in tickets {
                    if let Some(scope) = sessions.poison(*ticket) {
                        invalidated_token_ids.extend(scope.iter().cloned());
                    }
                }
                invalidated_token_ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
                invalidated_token_ids.dedup_by(|a, b| a.as_str() == b.as_str());
                book_store.invalidate_tokens(tokens);
                invalidated_token_ids
                    .extend(tokens.iter().filter_map(|token| registry.token_id(*token)));
                invalidated_token_ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
                invalidated_token_ids.dedup_by(|a, b| a.as_str() == b.as_str());
                book_store.invalidate_ids(&invalidated_token_ids);
                event_source.invalidate_tokens(&invalidated_token_ids);
                InvalidationOutcome {
                    affected_tokens: invalidated_token_ids.len().max(tokens.len()),
                    transport_restart_requested: true,
                }
            }
            Self::Subscription { session, token_ids } => {
                let token_ids = sessions
                    .poison(*session)
                    .unwrap_or_else(|| Arc::clone(token_ids));
                book_store.invalidate_ids(&token_ids);
                event_source.invalidate_tokens(&token_ids);
                InvalidationOutcome {
                    affected_tokens: token_ids.len(),
                    transport_restart_requested: true,
                }
            }
            Self::Closed {
                session,
                token_ids,
                sequences,
            } => {
                let session_token_ids = sessions.poison(*session);
                let tokens = sequences
                    .iter()
                    .map(|(token, _)| *token)
                    .collect::<Vec<_>>();
                book_store.invalidate_tokens(&tokens);
                let mut invalidated_ids =
                    session_token_ids.map_or_else(Vec::new, |ids| ids.to_vec());
                invalidated_ids.extend(token_ids.iter().cloned());
                invalidated_ids.extend(tokens.iter().filter_map(|token| registry.token_id(*token)));
                invalidated_ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
                invalidated_ids.dedup_by(|a, b| a.as_str() == b.as_str());
                book_store.invalidate_ids(&invalidated_ids);
                InvalidationOutcome {
                    affected_tokens: invalidated_ids.len().max(tokens.len()),
                    // The socket that produced a close batch has already ended;
                    // its shard loop is the sole reconnect owner.
                    transport_restart_requested: false,
                }
            }
        }
    }

    fn diagnostic_token(&self, registry: &MarketRegistry) -> Option<TokenId> {
        match self {
            Self::Tokens { tokens, .. } => registry.token_id(*tokens.first()?),
            Self::Subscription { token_ids, .. } => token_ids.first().cloned(),
            Self::Closed {
                token_ids,
                sequences,
                ..
            } => token_ids
                .first()
                .cloned()
                .or_else(|| registry.token_id(sequences.first()?.0)),
            Self::None => None,
        }
    }
}

struct PreparedPartitionBatch {
    partition: usize,
    event_kind: &'static str,
    batch: PartitionIngressBatch,
}

#[async_trait::async_trait]
impl MicrostructureCommitBarrier for DataPipeline {
    async fn commit_through(&self, source_cutoff: DateTime<Utc>) -> QuantResult<()> {
        let (response, receive_response) = oneshot::channel();
        let request = MicrostructureCommitRequest {
            source_cutoff_ms: source_cutoff.timestamp_millis(),
            response,
        };
        match timeout(
            BOOK_CHANNEL_TIMEOUT,
            self.microstructure_commit_tx.send_async(request),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return Err(InfraError::ChannelClosed {
                    name: "microstructure_commit",
                }
                .into());
            }
            Err(_) => {
                return Err(InfraError::ChannelTimeout {
                    name: "microstructure_commit",
                }
                .into());
            }
        }
        let response_timeout = CANONICAL_WRITE_TIMEOUT
            .saturating_add(BOOK_CHANNEL_TIMEOUT)
            .saturating_add(BOOK_CHANNEL_TIMEOUT);
        match timeout(response_timeout, receive_response).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(InfraError::ChannelClosed {
                name: "microstructure_commit_response",
            }
            .into()),
            Err(_) => Err(InfraError::ChannelTimeout {
                name: "microstructure_commit_response",
            }
            .into()),
        }
    }
}

impl DataPipeline {
    pub fn new(deps: DataPipelineDeps) -> Self {
        let sessions = deps.book_store.session_directory();
        let (microstructure_commit_tx, microstructure_commit_rx) =
            flume::bounded(MICROSTRUCTURE_COMMIT_CAPACITY);
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
            sessions,
            retirement_rx: deps.retirement_rx,
            microstructure_commit_tx,
            microstructure_commit_rx,
            durable_publish_observer: deps.durable_publish_observer,
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
                sessions: Arc::clone(&self.sessions),
                durable_publish_observer: self.durable_publish_observer.clone(),
                books: AHashMap::new(),
                microstructure_outbox: Vec::new(),
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

                retirement = self.retirement_rx.recv_async() => {
                    let Ok(retirement) = retirement else {
                        tracing::error!("token retirement control queue closed unexpectedly");
                        break Some(InfraError::ChannelClosed {
                            name: "token_retirement",
                        }.into());
                    };
                    if let Err(error) = self
                        .retire_transport_tokens(retirement, &partition_senders)
                        .await
                    {
                        self.shutdown.cancel();
                        break Some(error);
                    }
                }

                request = self.microstructure_commit_rx.recv_async() => {
                    let Ok(request) = request else {
                        tracing::error!("microstructure commit control queue closed unexpectedly");
                        break Some(InfraError::ChannelClosed {
                            name: "microstructure_commit",
                        }.into());
                    };
                    let result = self
                        .commit_microstructure(request.source_cutoff_ms, &partition_senders)
                        .await;
                    let _ = request.response.send(result);
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

    async fn commit_microstructure(
        &self,
        source_cutoff_ms: i64,
        partition_senders: &[MpscSender<PartitionMessage>],
    ) -> QuantResult<()> {
        let barrier = Arc::new(PartitionCommitBarrier::new(
            u8::try_from(partition_senders.len()).unwrap_or(u8::MAX),
        ));
        let commit = async {
            for sender in partition_senders {
                sender
                    .send(PartitionMessage::CommitMicrostructure {
                        source_cutoff_ms,
                        barrier: Arc::clone(&barrier),
                    })
                    .await
                    .map_err(|_| {
                        QuantError::from(InfraError::ChannelClosed {
                            name: "partition_microstructure_commit",
                        })
                    })?;
            }
            Ok::<bool, QuantError>(barrier.wait().await)
        };
        let deadline = CANONICAL_WRITE_TIMEOUT.saturating_add(BOOK_CHANNEL_TIMEOUT);
        match timeout(deadline, commit).await {
            Ok(Ok(true)) => Ok(()),
            Ok(Ok(false)) => Err(StorageError::InvariantViolation {
                entity: Some("book_microstructure_1s"),
                detail: "one or more partitions failed the durable feature-fact commit".to_owned(),
            }
            .into()),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(InfraError::ChannelTimeout {
                name: "partition_microstructure_commit",
            }
            .into()),
        }
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
        for events in split_events_by_session(batch.events) {
            self.dispatch_session_batch(
                events,
                Arc::clone(&batch.memory_permit),
                partition_senders,
                recycle_receivers,
                buffers,
                buffer_bytes,
            )
            .await?;
        }
        Ok(())
    }

    async fn dispatch_session_batch(
        &self,
        events: Vec<PipelineEvent>,
        memory_permit: Arc<OwnedSemaphorePermit>,
        partition_senders: &[MpscSender<PartitionMessage>],
        recycle_receivers: &[Receiver<Vec<PipelineEvent>>],
        buffers: &mut [Vec<PipelineEvent>; PARTITION_COUNT],
        buffer_bytes: &mut [usize; PARTITION_COUNT],
    ) -> Result<(), QuantError> {
        if !self.register_session_open_events(&events) {
            return Ok(());
        }
        let backpressure_scope = BackpressureScope::from_events(&events);
        if !self
            .await_session_barrier(&events, partition_senders, &backpressure_scope)
            .await?
        {
            return Ok(());
        }
        if let Some(event) = events
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
        let mut prepared = Vec::new();
        for event in events {
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
                prepared.push(Self::prepare_partition_batch(
                    partition,
                    events,
                    Arc::clone(&memory_permit),
                ));
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
            prepared.push(Self::prepare_partition_batch(
                partition,
                events,
                Arc::clone(&memory_permit),
            ));
        }
        self.send_prepared_batches(prepared, partition_senders, &backpressure_scope)
            .await
    }

    fn register_session_open_events(&self, events: &[PipelineEvent]) -> bool {
        for event in events {
            let PipelineEvent::StreamSessionOpened {
                session,
                subscription_tokens,
                ..
            } = event
            else {
                continue;
            };
            if !self.event_source.owns_all_tokens(subscription_tokens) {
                self.book_store.invalidate_ids(subscription_tokens);
                self.book_store.mark_gap();
                self.event_source.invalidate_tokens(subscription_tokens);
                tracing::error!(
                    stream_session_id = %session.stream_session_id,
                    session_epoch = session.epoch,
                    token_count = subscription_tokens.len(),
                    "Rejected stream session whose scope no longer has transport ownership"
                );
                return false;
            }
            if self
                .sessions
                .open(*session, Arc::clone(subscription_tokens))
            {
                self.book_store.begin_session(*session, subscription_tokens);
                continue;
            }
            self.book_store.invalidate_ids(subscription_tokens);
            self.book_store.mark_gap();
            self.event_source.invalidate_tokens(subscription_tokens);
            tracing::error!(
                stream_session_id = %session.stream_session_id,
                session_epoch = session.epoch,
                token_count = subscription_tokens.len(),
                "Rejected invalid or conflicting stream session registration"
            );
            return false;
        }
        true
    }

    async fn retire_transport_tokens(
        &self,
        retirement: TransportRetirement,
        partition_senders: &[MpscSender<PartitionMessage>],
    ) -> Result<(), QuantError> {
        let mut commands: [Vec<RetireToken>; PARTITION_COUNT] = array::from_fn(|_| Vec::new());
        for token_id in retirement.tokens.iter() {
            let Some(token) = self.market_registry.data_plane().token_key(token_id) else {
                continue;
            };
            commands[token.index() % PARTITION_COUNT].push(RetireToken {
                token,
                through_epoch: retirement.through_epoch,
            });
        }
        let partition_count = commands.iter().filter(|batch| !batch.is_empty()).count();
        if partition_count == 0 {
            return Ok(());
        }
        let barrier = Arc::new(PartitionBarrier::new(
            u8::try_from(partition_count).unwrap_or(u8::MAX),
        ));
        let retire = async {
            let mut permits = Vec::with_capacity(partition_count);
            for (partition, batch) in commands.iter().enumerate() {
                if batch.is_empty() {
                    continue;
                }
                let permit = partition_senders[partition]
                    .clone()
                    .reserve_owned()
                    .await
                    .map_err(|_| InfraError::ChannelClosed {
                        name: "token_retirement_partition",
                    })?;
                permits.push((partition, permit));
            }
            for (partition, permit) in permits {
                permit.send(PartitionMessage::Retire {
                    tokens: mem::take(&mut commands[partition]),
                    barrier: Arc::clone(&barrier),
                });
            }
            barrier.wait().await;
            Ok::<(), InfraError>(())
        };
        timeout(BOOK_CHANNEL_TIMEOUT, retire)
            .await
            .map_err(|_| InfraError::ChannelTimeout {
                name: "token_retirement_barrier",
            })??;
        self.book_store.mark_gap();
        Ok(())
    }

    fn prepare_partition_batch(
        partition: usize,
        mut events: Vec<PipelineEvent>,
        memory_permit: Arc<OwnedSemaphorePermit>,
    ) -> PreparedPartitionBatch {
        let event_kind = events.first().map_or("empty", pipeline_event_kind);
        events.reverse();
        PreparedPartitionBatch {
            partition,
            event_kind,
            batch: PartitionIngressBatch {
                events,
                memory_permit,
            },
        }
    }

    async fn send_prepared_batches(
        &self,
        prepared: Vec<PreparedPartitionBatch>,
        partition_senders: &[MpscSender<PartitionMessage>],
        backpressure_scope: &BackpressureScope,
    ) -> Result<(), QuantError> {
        if prepared.is_empty() {
            return Ok(());
        }
        match timeout(
            BOOK_CHANNEL_TIMEOUT,
            reserve_partition_mailboxes(&prepared, partition_senders),
        )
        .await
        {
            Ok(Ok(permits)) => {
                for (item, permit) in prepared.into_iter().zip(permits) {
                    permit.send(PartitionMessage::Events(item.batch));
                }
                Ok(())
            }
            Ok(Err(partition)) => {
                tracing::error!(partition, "Partition actor channel closed unexpectedly");
                Err(InfraError::ChannelClosed {
                    name: "partition_actor",
                }
                .into())
            }
            Err(_) => {
                let Some(congested) = prepared
                    .iter()
                    .max_by_key(|item| partition_queue_depth(&partition_senders[item.partition]))
                else {
                    return Ok(());
                };
                self.handle_book_apply_timeout(
                    congested.partition,
                    congested.event_kind,
                    partition_queue_depth(&partition_senders[congested.partition]),
                    backpressure_scope,
                    "atomic partition mailbox reservation timed out",
                );
                Ok(())
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
        let outcome = scope.invalidate(
            self.event_source.as_ref(),
            &self.book_store,
            &self.market_registry,
            &self.sessions,
        );
        self.metrics
            .book_apply_backpressure_invalidations
            .inc_by(u64::try_from(outcome.affected_tokens.max(1)).unwrap_or(u64::MAX));
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
            affected_tokens = outcome.affected_tokens,
            transport_restart_requested = outcome.transport_restart_requested,
            token_id = diagnostic_token.as_ref().map(TokenId::as_str),
            timeouts_since_last,
            reason,
            "Partition queue rejected a batch; continuity invalidated"
        );
    }
}

fn partition_queue_depth(sender: &MpscSender<PartitionMessage>) -> usize {
    sender.max_capacity().saturating_sub(sender.capacity())
}

async fn reserve_partition_mailboxes(
    prepared: &[PreparedPartitionBatch],
    partition_senders: &[MpscSender<PartitionMessage>],
) -> Result<Vec<MpscOwnedPermit<PartitionMessage>>, usize> {
    let mut permits = Vec::with_capacity(prepared.len());
    for item in prepared {
        let permit = partition_senders[item.partition]
            .clone()
            .reserve_owned()
            .await
            .map_err(|_| item.partition)?;
        permits.push(permit);
    }
    Ok(permits)
}

struct PartitionIngressBatch {
    events: Vec<PipelineEvent>,
    memory_permit: Arc<OwnedSemaphorePermit>,
}

enum PartitionMessage {
    Events(PartitionIngressBatch),
    Barrier(Arc<PartitionBarrier>),
    CommitMicrostructure {
        source_cutoff_ms: i64,
        barrier: Arc<PartitionCommitBarrier>,
    },
    Retire {
        tokens: Vec<RetireToken>,
        barrier: Arc<PartitionBarrier>,
    },
}

struct PartitionCommitBarrier {
    remaining: AtomicU8,
    failed: AtomicBool,
    completed: Notify,
}

impl PartitionCommitBarrier {
    const fn new(partitions: u8) -> Self {
        Self {
            remaining: AtomicU8::new(partitions),
            failed: AtomicBool::new(false),
            completed: Notify::const_new(),
        }
    }

    fn arrive(&self, persisted: bool) {
        if !persisted {
            self.failed.store(true, Ordering::Release);
        }
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.completed.notify_one();
        }
    }

    async fn wait(&self) -> bool {
        loop {
            let completed = self.completed.notified();
            if self.remaining.load(Ordering::Acquire) == 0 {
                return !self.failed.load(Ordering::Acquire);
            }
            completed.await;
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RetireToken {
    token: TokenKey,
    through_epoch: u64,
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
    use std::{
        collections::HashMap,
        slice,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, Instant},
    };

    use ahash::AHashMap;
    use flume::Receiver;
    use quant_pivot_api::ws::NormalizedIngressBatch;
    use quant_pivot_models::{
        domain::data_plane::pipeline::{IngressTrace, PipelineEvent, StreamSessionTicket},
        enums::system::ShardConnectionStatus,
        types::{TokenId, TokenKey},
    };
    use tokio::{
        sync::{Semaphore, mpsc},
        time::timeout,
    };
    use uuid::Uuid;

    use super::{
        BackpressureScope, MAX_PARTITION_BATCH_BYTES, MAX_PARTITION_BATCH_EVENTS, MutableBookState,
        PARTITION_COUNT, PARTITION_MAILBOX_CAPACITY, PartitionBarrier, PartitionCommitBarrier,
        PartitionIngressBatch, PartitionMessage, PreparedPartitionBatch, RetireToken,
        TokenStreamState, accept_token_sequence, partition_batch_would_overflow, partition_index,
        reserve_partition_mailboxes, split_events_by_session, take_retired_mutable_book,
    };
    use crate::{
        ingest::{
            book_store::BookStore, data_plane_index::DataPlane, event_source::PipelineEventSource,
            market_registry::MarketRegistry,
        },
        observability::metrics_hub::MetricsHub,
    };

    struct RecordingEventSource {
        events: Receiver<NormalizedIngressBatch>,
        restarts: AtomicU64,
    }

    impl Default for RecordingEventSource {
        fn default() -> Self {
            let (_tx, events) = flume::bounded(1);
            Self {
                events,
                restarts: AtomicU64::new(0),
            }
        }
    }

    impl PipelineEventSource for RecordingEventSource {
        fn events(&self) -> &Receiver<NormalizedIngressBatch> {
            &self.events
        }

        fn owns_all_tokens(&self, _token_ids: &[TokenId]) -> bool {
            true
        }

        fn invalidate_token(&self, _token_id: &TokenId) {
            self.restarts.fetch_add(1, Ordering::Relaxed);
        }

        fn invalidate_tokens(&self, _token_ids: &[TokenId]) {
            self.restarts.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn token_partition_fixed_affine() {
        for value in 0..2_000_u32 {
            let event = PipelineEvent::StreamGap {
                token: TokenKey::new(value),
                session: StreamSessionTicket::new(Uuid::from_u128(1), 1)
                    .expect("valid session ticket"),
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
    fn catalog_churn_bounds_window() {
        const CATALOG_TOKENS: usize = 10_000;
        const ACTIVE_TOKENS: usize = 2_000;

        let token_ids = (0..CATALOG_TOKENS)
            .map(|index| TokenId::new(index.to_string()))
            .collect::<Vec<_>>();
        let data_plane = Arc::new(DataPlane::new());
        data_plane.register_test_tokens(&token_ids);
        let store = BookStore::new(data_plane, Arc::new(MetricsHub::new()));
        let mut books = AHashMap::new();
        let mut stream_state = HashMap::new();
        for (index, token_id) in token_ids.iter().enumerate() {
            let token = store.resolve(token_id).expect("registered churn token");
            let epoch = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1);
            let session = StreamSessionTicket::new(Uuid::from_u128(u128::from(epoch)), epoch)
                .expect("valid churn session");
            books.insert(token, MutableBookState::new(token_id.clone(), 0));
            stream_state.insert(
                token,
                TokenStreamState {
                    session,
                    last_sequence: 1,
                    has_fresh_snapshot: true,
                },
            );
            if index >= ACTIVE_TOKENS {
                let retired_index = index - ACTIVE_TOKENS;
                let retired = TokenKey::new(u32::try_from(retired_index).unwrap_or(u32::MAX));
                assert!(
                    take_retired_mutable_book(
                        &store,
                        &mut books,
                        &mut stream_state,
                        RetireToken {
                            token: retired,
                            through_epoch: u64::try_from(retired_index)
                                .unwrap_or(u64::MAX)
                                .saturating_add(1),
                        },
                    )
                    .is_some()
                );
            }
            assert!(books.len() <= ACTIVE_TOKENS);
            assert!(stream_state.len() <= ACTIVE_TOKENS);
        }
        for index in CATALOG_TOKENS - ACTIVE_TOKENS..CATALOG_TOKENS {
            let token = TokenKey::new(u32::try_from(index).unwrap_or(u32::MAX));
            assert!(
                take_retired_mutable_book(
                    &store,
                    &mut books,
                    &mut stream_state,
                    RetireToken {
                        token,
                        through_epoch: u64::try_from(index).unwrap_or(u64::MAX).saturating_add(1),
                    },
                )
                .is_some()
            );
        }
        assert!(books.is_empty());
        assert!(stream_state.is_empty());
        assert_eq!(store.token_count(), CATALOG_TOKENS);
    }

    #[test]
    fn control_events_bounded_set() {
        let event = PipelineEvent::ShardStatus {
            shard_id: usize::MAX,
            status: ShardConnectionStatus::Connected,
        };
        assert!(partition_index(&event) < PARTITION_COUNT);
    }

    #[test]
    fn normalized_batch_split_session() {
        let first =
            StreamSessionTicket::new(Uuid::from_u128(1), 1).expect("valid first session ticket");
        let second =
            StreamSessionTicket::new(Uuid::from_u128(2), 2).expect("valid second session ticket");
        let events = vec![
            PipelineEvent::StreamGap {
                token: TokenKey::new(0),
                session: first,
                shard_id: 0,
                last_received_sequence: 1,
                timestamp_ms: 0,
            },
            PipelineEvent::StreamGap {
                token: TokenKey::new(1),
                session: second,
                shard_id: 1,
                last_received_sequence: 1,
                timestamp_ms: 0,
            },
            PipelineEvent::StreamGap {
                token: TokenKey::new(2),
                session: first,
                shard_id: 0,
                last_received_sequence: 1,
                timestamp_ms: 0,
            },
            PipelineEvent::ShardStatus {
                shard_id: 0,
                status: ShardConnectionStatus::Connected,
            },
        ];

        let groups = split_events_by_session(events);

        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[1].len(), 1);
        assert_eq!(groups[2].len(), 1);
    }

    #[test]
    fn closed_scope_no_restart() {
        let token_id = TokenId::new("1");
        let data_plane = Arc::new(DataPlane::new());
        data_plane.register_test_tokens(slice::from_ref(&token_id));
        let metrics = Arc::new(MetricsHub::new());
        let book_store = BookStore::new(Arc::clone(&data_plane), metrics);
        let registry = MarketRegistry::new(data_plane);
        let token = book_store.resolve(&token_id).expect("registered token");
        let session = StreamSessionTicket::new(Uuid::new_v4(), 1).expect("valid session ticket");
        let sessions = book_store.session_directory();
        let token_ids: Arc<[TokenId]> = Arc::from([token_id]);
        assert!(sessions.open(session, Arc::clone(&token_ids)));
        let source = RecordingEventSource::default();
        let scope = BackpressureScope::Closed {
            session,
            token_ids,
            sequences: Arc::from([(token, 7)]),
        };

        let outcome = scope.invalidate(&source, &book_store, &registry, &sessions);

        assert_eq!(outcome.affected_tokens, 1);
        assert!(!outcome.transport_restart_requested);
        assert_eq!(source.restarts.load(Ordering::Relaxed), 0);
        drop(source);
        assert!(!sessions.is_active(session));
    }

    #[tokio::test]
    async fn session_barrier_waits_partition() {
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

    #[tokio::test]
    async fn commit_barrier_reports_failure() {
        let barrier = Arc::new(PartitionCommitBarrier::new(2));
        barrier.arrive(true);
        let waiter = {
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move { barrier.wait().await })
        };
        assert!(!waiter.is_finished());
        barrier.arrive(false);
        assert!(!waiter.await.expect("commit barrier waiter"));
    }

    #[tokio::test]
    async fn mailbox_failure_no_batch() {
        let (first_tx, mut first_rx) = mpsc::channel(1);
        let (second_tx, _second_rx) = mpsc::channel(1);
        second_tx
            .send(PartitionMessage::Barrier(Arc::new(PartitionBarrier::new(
                1,
            ))))
            .await
            .expect("fill second mailbox");
        let memory = Arc::new(
            Arc::new(Semaphore::new(1))
                .acquire_owned()
                .await
                .expect("memory permit"),
        );
        let prepared = vec![
            PreparedPartitionBatch {
                partition: 0,
                event_kind: "test",
                batch: PartitionIngressBatch {
                    events: Vec::new(),
                    memory_permit: Arc::clone(&memory),
                },
            },
            PreparedPartitionBatch {
                partition: 1,
                event_kind: "test",
                batch: PartitionIngressBatch {
                    events: Vec::new(),
                    memory_permit: memory,
                },
            },
        ];
        let senders = vec![first_tx.clone(), second_tx];

        assert!(
            timeout(
                Duration::from_millis(10),
                reserve_partition_mailboxes(&prepared, &senders)
            )
            .await
            .is_err()
        );
        assert_eq!(first_tx.capacity(), 1);
        assert!(first_rx.try_recv().is_err());
    }

    #[test]
    fn partition_batch_before_push() {
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
    fn token_sequence_monotonic_session() {
        let token = TokenKey::new(7);
        let first_session = Uuid::new_v4();
        let second_session = Uuid::new_v4();
        let mut states = HashMap::new();
        let trace = |session, epoch, sequence| IngressTrace {
            mono: Instant::now(),
            ingress_time_ms: 0,
            ws_timestamp_ms: 0,
            session: StreamSessionTicket::new(session, epoch).expect("valid session ticket"),
            shard_id: 0,
            token_sequence: sequence,
        };

        assert!(accept_token_sequence(
            &mut states,
            token,
            trace(first_session, 1, 1)
        ));
        assert!(!accept_token_sequence(
            &mut states,
            token,
            trace(first_session, 1, 3)
        ));
        assert!(accept_token_sequence(
            &mut states,
            token,
            trace(second_session, 2, 1)
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
    sessions: Arc<SessionDirectory>,
    durable_publish_observer: Option<DurableBookPublishObserver>,
    books: AHashMap<TokenKey, MutableBookState>,
    microstructure_outbox: Vec<BookMicrostructureRow>,
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

fn take_retired_mutable_book(
    book_store: &BookStore,
    books: &mut AHashMap<TokenKey, MutableBookState>,
    stream_state: &mut HashMap<TokenKey, TokenStreamState>,
    command: RetireToken,
) -> Option<MutableBookState> {
    if !book_store.retire_token(command.token, command.through_epoch) {
        return None;
    }
    stream_state.remove(&command.token);
    books.remove(&command.token)
}

#[derive(Debug, Clone, Copy)]
struct TokenStreamState {
    session: StreamSessionTicket,
    last_sequence: u64,
    has_fresh_snapshot: bool,
}

struct SessionClose {
    session: StreamSessionTicket,
    shard_id: u32,
    subscription_token_hash: ContentHash,
    subscription_token_count: u32,
    subscription_tokens: Arc<[TokenId]>,
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
                    self.flush_elapsed_microstructure().await;
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
                PartitionMessage::CommitMicrostructure {
                    source_cutoff_ms,
                    barrier,
                } => {
                    let persisted = self.commit_microstructure(source_cutoff_ms).await;
                    barrier.arrive(persisted);
                    continue;
                }
                PartitionMessage::Retire { tokens, barrier } => {
                    self.retire_tokens(&tokens).await;
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
                self.microstructure_outbox.push(row);
            }
        }
        let _ = self.persist_microstructure().await;
        self.metrics
            .mutable_book_count
            .sub(i64::try_from(self.books.len()).unwrap_or(i64::MAX));
    }

    async fn flush_elapsed_microstructure(&mut self) {
        let now_ms = Utc::now().timestamp_millis();
        for state in self.books.values_mut() {
            if let Some(row) = state.microstructure.flush_elapsed(now_ms) {
                self.microstructure_outbox.push(row);
            }
        }
        let _ = self.persist_microstructure().await;
    }

    async fn retire_tokens(&mut self, tokens: &[RetireToken]) {
        for command in tokens {
            if let Some(mut state) = take_retired_mutable_book(
                &self.book_store,
                &mut self.books,
                &mut self.stream_state,
                *command,
            ) {
                self.metrics.mutable_book_count.dec();
                if let Some(row) = state.microstructure.flush() {
                    self.microstructure_outbox.push(row);
                }
            }
        }
        let _ = self.persist_microstructure().await;
    }

    async fn commit_microstructure(&mut self, source_cutoff_ms: i64) -> bool {
        for state in self.books.values_mut() {
            if let Some(row) = state.microstructure.flush_elapsed(source_cutoff_ms) {
                self.microstructure_outbox.push(row);
            }
        }
        self.persist_microstructure().await
    }

    async fn persist_microstructure(&mut self) -> bool {
        if self.microstructure_outbox.is_empty() {
            return true;
        }
        let rows = mem::take(&mut self.microstructure_outbox);
        let affected_tokens = rows
            .iter()
            .filter_map(|row| self.market_registry.data_plane().token_key(&row.token_id))
            .collect::<HashSet<_>>();
        if let Err(error) = self.book_fact_writer.write_microstructure_rows(rows).await {
            tracing::error!(
                partition_id = self.partition_id.get(),
                ?error,
                "microstructure feature-fact persistence failed"
            );
            self.book_store.mark_gap();
            for token in affected_tokens {
                self.invalidate_token(token);
            }
            return false;
        }
        true
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
                self.metrics.markets_resolved_ws.inc();
                let winning_token_registered =
                    self.market_registry.token_id(winning_token).is_some();
                let registered_token_count = tokens
                    .iter()
                    .filter(|token| self.market_registry.token_id(**token).is_some())
                    .count();
                tracing::info!(
                    %market_id,
                    known,
                    winning_token_registered,
                    event_token_count = tokens.len(),
                    registered_token_count,
                    winning_outcome,
                    timestamp_ms,
                    "observed winner-only WS resolution signal; canonical payout reconciliation required"
                );
                if !winning_token_registered {
                    tracing::error!(
                        ?winning_token,
                        "resolved event lost registered winning token"
                    );
                    return;
                }
                if registered_token_count != tokens.len() {
                    tracing::error!(%market_id, "resolved event lost registered outcome token");
                }
            }

            PipelineEvent::ShardStatus { shard_id, status } => {
                self.on_shard_status(shard_id, status);
            }

            PipelineEvent::StreamSessionOpened {
                session,
                shard_id,
                subscription_token_hash,
                subscription_token_count,
                subscription_tokens: _,
                opened_at_ms,
            } => {
                if !self
                    .book_fact_writer
                    .write_stream_session_open(
                        session.stream_session_id,
                        shard_id,
                        subscription_token_hash,
                        subscription_token_count,
                        opened_at_ms,
                    )
                    .await
                {
                    self.poison_session(session);
                }
            }
            PipelineEvent::StreamSessionClosed {
                session,
                shard_id,
                subscription_token_hash,
                subscription_token_count,
                subscription_tokens,
                received_sequences,
                opened_at_ms,
                closed_at_ms,
                reason,
            } => {
                self.handle_session_close(SessionClose {
                    session,
                    shard_id,
                    subscription_token_hash,
                    subscription_token_count,
                    subscription_tokens,
                    received_sequences,
                    opened_at_ms,
                    closed_at_ms,
                    reason,
                })
                .await;
            }
            PipelineEvent::StreamGap {
                token,
                session,
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
                    session.stream_session_id,
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
                self.invalidate_closed_token(token);
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
                canonical_identity(event).map(|(token, trace)| (token, trace.session))
            })
            .collect::<Vec<_>>();
        let mut failed_sessions = HashSet::new();
        for event in &events {
            let Some((token, trace)) = canonical_identity(event) else {
                continue;
            };
            if failed_sessions.contains(&trace.session) {
                continue;
            }
            if !self.sessions.is_active(trace.session) {
                failed_sessions.insert(trace.session);
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
                failed_sessions.insert(trace.session);
            }
        }

        let mut prepared = Vec::with_capacity(events.len());
        for event in events {
            let Some((_, trace)) = canonical_identity(&event) else {
                continue;
            };
            if failed_sessions.contains(&trace.session) {
                continue;
            }
            if let Some(row) = self.prepare_ledger_event(&event) {
                prepared.push((event, row));
            } else {
                failed_sessions.insert(trace.session);
            }
        }
        prepared.retain(|(event, _)| {
            canonical_identity(event)
                .is_some_and(|(_, trace)| !failed_sessions.contains(&trace.session))
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
                failed_sessions.extend(
                    committed.iter().filter_map(|event| {
                        canonical_identity(event).map(|(_, trace)| trace.session)
                    }),
                );
            }
        }

        if !failed_sessions.is_empty() {
            self.invalidate_batch_sessions(&batch_scope, &failed_sessions);
        }
        let events = committed
            .into_iter()
            .filter(|event| {
                canonical_identity(event).is_none_or(|(_, trace)| {
                    !failed_sessions.contains(&trace.session)
                        && self.sessions.is_active(trace.session)
                })
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
                transaction_hash,
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
                    transaction_hash: transaction_hash.as_ref(),
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
                            self.book_store.mark_canonical_fresh_session(
                                token,
                                trace.token_sequence,
                                trace.session,
                            );
                        }
                        PipelineEvent::LastTradePrice { token, trace, .. } => {
                            self.book_store.mark_canonical_fresh_session(
                                token,
                                trace.token_sequence,
                                trace.session,
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
        let initial_version = self.book_store.last_known_version(cmd.token);
        let is_new = !self.books.contains_key(&cmd.token);
        let state = self
            .books
            .entry(cmd.token)
            .or_insert_with(|| MutableBookState::new(token_id.clone(), initial_version));
        if is_new {
            self.metrics.mutable_book_count.inc();
        }
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
        if !self.book_store.publish_snapshot_session(
            cmd.token,
            snapshot,
            cmd.trace.token_sequence,
            cmd.trace.session,
            Some(LatencyTrace::from_ingress(cmd.trace.mono)),
        ) {
            self.invalidate_token(cmd.token);
            return;
        }
        self.observe_durable_publish(
            DurableBookPublishKind::Snapshot,
            cmd.token,
            cmd.trace.token_sequence,
            cmd.trace.session,
            cmd.trace.mono,
        );
        if let Some(row) = stale_microstructure {
            self.microstructure_outbox.push(row);
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
            if !self.book_store.publish_update_session(
                token,
                snapshot,
                last_command.trace.token_sequence,
                last_command.trace.session,
                Some(LatencyTrace::from_ingress(last_command.trace.mono)),
            ) {
                self.invalidate_token(token);
                start = end;
                continue;
            }
            self.observe_durable_publish(
                DurableBookPublishKind::Delta,
                token,
                last_command.trace.token_sequence,
                last_command.trace.session,
                last_command.trace.mono,
            );
            if let Some(row) = stale_microstructure {
                self.microstructure_outbox.push(row);
            }
            start = end;
        }
        self.metrics
            .price_changes_applied
            .inc_by(u64::try_from(commands.len()).unwrap_or(u64::MAX));
    }

    fn observe_durable_publish(
        &self,
        kind: DurableBookPublishKind,
        token: TokenKey,
        token_sequence: u64,
        session: StreamSessionTicket,
        ws_ingress: Instant,
    ) {
        if let Some(observer) = &self.durable_publish_observer {
            observer(DurableBookPublishSample {
                kind,
                token,
                token_sequence,
                session,
                ws_ingress,
                published_at: Instant::now(),
            });
        }
    }

    fn invalidate_batch_sessions(
        &mut self,
        batch_scope: &[(TokenKey, StreamSessionTicket)],
        failed_sessions: &HashSet<StreamSessionTicket>,
    ) {
        let mut invalidated_tokens = HashSet::new();
        for (token, session) in batch_scope {
            if failed_sessions.contains(session) {
                invalidated_tokens.insert(*token);
                if let Some(state) = self.stream_state.get_mut(token) {
                    state.has_fresh_snapshot = false;
                }
            }
        }
        for session in failed_sessions {
            if let Some(token_ids) = self.sessions.poison(*session) {
                for token_id in token_ids.iter() {
                    if let Some(token) = self.market_registry.data_plane().token_key(token_id) {
                        invalidated_tokens.insert(token);
                    }
                }
                self.event_source.invalidate_tokens(&token_ids);
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

    fn invalidate_closed_token(&mut self, token: TokenKey) {
        if let Some(state) = self.stream_state.get_mut(&token) {
            state.has_fresh_snapshot = false;
        }
        self.book_store.invalidate_tokens(slice::from_ref(&token));
        self.book_store.mark_gap();
    }

    fn invalidate_closed_scope(&mut self, token_ids: &[TokenId]) {
        self.book_store.invalidate_ids(token_ids);
        self.book_store.mark_gap();
        for token_id in token_ids {
            if let Some(token) = self.market_registry.data_plane().token_key(token_id)
                && let Some(state) = self.stream_state.get_mut(&token)
            {
                state.has_fresh_snapshot = false;
            }
        }
    }

    fn poison_session(&mut self, session: StreamSessionTicket) {
        let Some(token_ids) = self.sessions.poison(session) else {
            return;
        };
        self.book_store.invalidate_ids(&token_ids);
        self.book_store.mark_gap();
        self.event_source.invalidate_tokens(&token_ids);
        for token_id in token_ids.iter() {
            if let Some(token) = self.market_registry.data_plane().token_key(token_id)
                && let Some(state) = self.stream_state.get_mut(&token)
            {
                state.has_fresh_snapshot = false;
            }
        }
    }

    async fn handle_session_close(&mut self, close: SessionClose) {
        let expected_generation = close.session.epoch;
        let session_poisoned = !self.sessions.is_active(close.session);
        // Non-normal closes invalidate BookStore synchronously at the transport
        // boundary, so slot freshness cannot classify their already-drained
        // durable prefix. An apparently normal seal must still prove exact
        // generation/sequence continuity after the partition barrier.
        let continuity_invalid = close.reason == StreamSessionEndReason::Normal
            && close.received_sequences.iter().any(|(token, sequence)| {
                self.book_store.freshness(*token).is_none_or(|freshness| {
                    freshness.state != TokenSlotState::Fresh
                        || freshness.session_generation != expected_generation
                        || freshness.sequence != *sequence
                })
            });
        let mut sequences = BTreeMap::new();
        let mut sequence_scope_valid = true;
        for (token, sequence) in close.received_sequences.iter() {
            let Some(token_id) = self.market_registry.token_id(*token) else {
                sequence_scope_valid = false;
                continue;
            };
            sequences.insert(token_id, *sequence);
        }
        let sequence_json = match serde_json::to_string(&sequences) {
            Ok(sequence_json) => sequence_json,
            Err(error) => {
                tracing::error!(%error, "stream-session sequence ledger encoding failed");
                sequence_scope_valid = false;
                "{}".to_owned()
            }
        };
        let token_ids = close
            .subscription_tokens
            .iter()
            .map(TokenId::as_str)
            .collect::<Vec<_>>();
        let scope_hash_valid = CanonicalDigest::content_hash_json(&token_ids)
            .is_ok_and(|hash| hash == close.subscription_token_hash);
        let scope_count_valid = u32::try_from(close.subscription_tokens.len())
            .is_ok_and(|count| count == close.subscription_token_count);
        let session_scope_valid = self
            .sessions
            .tokens(close.session)
            .is_some_and(|tokens| tokens.as_ref() == close.subscription_tokens.as_ref());
        let scope_invalid = !sequence_scope_valid
            || !scope_hash_valid
            || !scope_count_valid
            || !session_scope_valid;
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
        if session_poisoned || continuity_invalid || scope_invalid {
            state = ChStreamSessionState::Invalidated;
            end_reason = ChStreamSessionEndReason::Overflow;
        }
        let persisted = self
            .book_fact_writer
            .write_stream_session_close(BookStreamSessionRow {
                stream_session_id: close.session.stream_session_id,
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
        if !persisted
            || close.reason != StreamSessionEndReason::Normal
            || state == ChStreamSessionState::Invalidated
        {
            self.invalidate_closed_scope(&close.subscription_tokens);
        }
        self.sessions.close(close.session);
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
