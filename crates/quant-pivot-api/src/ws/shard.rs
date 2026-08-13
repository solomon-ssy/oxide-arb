//! Single WebSocket shard: a resident actor owning one CLOB connection.
//!
//! Each shard is spawned **once** by the router and lives until shutdown. The
//! desired token set arrives over a `tokio::sync::watch` channel (full-state,
//! last-write-wins): changes are debounced and applied by closing the owned
//! socket before resubscribing — there is never more than one connection per
//! shard. An empty token set parks the actor instead of busy-looping.

use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::Utc;
use flume::Sender;
use futures_util::{SinkExt, StreamExt};
use polymarket_client_sdk_v2::{
    clob::ws::{
        SubscriptionRequest, interest::MessageInterest, types::response::parse_if_interested,
    },
    types::U256,
};
use quant_pivot_models::{
    domain::data_plane::pipeline::{PipelineEvent, StreamSessionEndReason, StreamSessionTicket},
    enums::system::ShardConnectionStatus,
    hashing::CanonicalDigest,
    types::{ContentHash, TokenId, TokenKey},
};
use tokio::{
    net::TcpStream,
    sync::{Semaphore, watch::Receiver},
    time::{MissedTickBehavior, interval, sleep, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Error as WsTransportError, Message},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    IngressEnqueueObserver,
    health::ShardHealthBoard,
    ingest_hooks::BookLevelRejectHook,
    ingress::{NormalizedIngressBatch, ingress_permits},
    normalize::normalize_ws_message,
    reconnect::{ReconnectPolicy, ReconnectState},
    session_hook::WsSessionInvalidationHook,
    token_resolver::TokenKeyResolver,
};

/// Full desired shard state. `restart_generation` makes a forced transport
/// restart observable even when the token set itself is unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ShardAssignment {
    pub tokens: Arc<HashSet<TokenId>>,
    pub restart_generation: u64,
}

impl ShardAssignment {
    #[must_use]
    pub(super) fn empty() -> Self {
        Self {
            tokens: Arc::new(HashSet::new()),
            restart_generation: 0,
        }
    }
}

/// Debounce window for token-set changes: bursts of assign/remove during a
/// catalog sync coalesce into a single teardown + resubscribe.
const TOKEN_DEBOUNCE: Duration = Duration::from_millis(500);

/// Startup stagger: shard `n` waits `(n % SLOTS) * STEP` before its first
/// connection attempt so dozens of shards never handshake simultaneously.
const STARTUP_STAGGER_STEP: Duration = Duration::from_millis(250);
const STARTUP_STAGGER_SLOTS: usize = 16;
const OUTPUT_ENQUEUE_TIMEOUT: Duration = Duration::from_millis(250);
const SESSION_LEDGER_TIMEOUT: Duration = Duration::from_secs(2);
/// Polymarket's market-channel protocol requires a text `PING` every ten
/// seconds and answers with text `PONG`. Missing the response before the next
/// documented cadence retires the socket so the shard's sole reconnect owner
/// can recover it.
const MARKET_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

type MarketSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Shared construction dependencies, owned by the router and cloned per shard.
#[derive(Clone)]
pub(super) struct ShardDeps {
    pub output_tx: Sender<NormalizedIngressBatch>,
    pub ingress_budget: Arc<Semaphore>,
    pub ws_url: String,
    pub shutdown: CancellationToken,
    pub message_epoch: Arc<Instant>,
    pub last_message_tick: Arc<AtomicU64>,
    pub session_epoch: Arc<AtomicU64>,
    pub token_resolver: Arc<dyn TokenKeyResolver>,
    pub on_session_invalidated: Option<WsSessionInvalidationHook>,
    pub on_book_level_rejected: Option<BookLevelRejectHook>,
    pub ingress_enqueue_observer: Option<IngressEnqueueObserver>,
    /// Shard-level reconnect backoff (from `[market_data.websocket]`).
    pub reconnect_policy: ReconnectPolicy,
    /// Global connect-concurrency limiter shared by every shard.
    pub connect_limiter: Arc<Semaphore>,
    /// Aggregated connection-state board for health summaries.
    pub health: Arc<ShardHealthBoard>,
}

/// Why a streaming session ended.
enum StreamEnd {
    /// Process shutdown requested.
    Shutdown,
    /// The desired token set changed — rebuild the subscription immediately.
    Resubscribe,
    /// Connection / subscription failure — back off before retrying.
    Failed(String),
    /// A bounded output queue timed out. The session is invalid and must reconnect.
    Overflow(String),
    /// The router dropped the token channel (manager torn down).
    RouterDropped,
}

struct OpenStreamSession {
    ticket: StreamSessionTicket,
    subscription_token_hash: ContentHash,
    subscription_token_count: u32,
    subscription_tokens: Arc<[TokenId]>,
    opened_at_ms: i64,
    token_sequences: HashMap<TokenKey, u64>,
}

impl ShardDeps {
    async fn connect_market(
        &self,
        assignment_rx: &mut Receiver<ShardAssignment>,
        asset_ids: Vec<U256>,
    ) -> Result<MarketSocket, StreamEnd> {
        let subscription = serde_json::to_string(
            &SubscriptionRequest::market(asset_ids).with_custom_features(true),
        )
        .map_err(|error| StreamEnd::Failed(format!("WS subscription encoding failed: {error}")))?;
        // This future owns the socket directly. Dropping it cancels the
        // handshake or closes the connection; no detached reconnect task can
        // retain transport resources.
        let connect_permit = tokio::select! {
            () = self.shutdown.cancelled() => return Err(StreamEnd::Shutdown),
            permit = Arc::clone(&self.connect_limiter).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => return Err(StreamEnd::Shutdown),
            },
        };
        let connected = tokio::select! {
            biased;
            () = self.shutdown.cancelled() => return Err(StreamEnd::Shutdown),
            changed = assignment_rx.changed() => {
                if changed.is_err() {
                    return Err(StreamEnd::RouterDropped);
                }
                debounce_assignment_changes(assignment_rx, &self.shutdown).await;
                return Err(StreamEnd::Resubscribe);
            }
            result = connect_async(&self.ws_url) => result,
        };
        let (mut socket, _) = connected
            .map_err(|error| StreamEnd::Failed(format!("WS connection failed: {error}")))?;
        let subscribed = tokio::select! {
            biased;
            () = self.shutdown.cancelled() => return Err(StreamEnd::Shutdown),
            changed = assignment_rx.changed() => {
                if changed.is_err() {
                    return Err(StreamEnd::RouterDropped);
                }
                debounce_assignment_changes(assignment_rx, &self.shutdown).await;
                return Err(StreamEnd::Resubscribe);
            }
            result = socket.send(Message::Text(subscription.into())) => result,
        };
        subscribed
            .map_err(|error| StreamEnd::Failed(format!("WS subscription failed: {error}")))?;
        // TLS/WebSocket establishment and the initial subscription write are
        // complete, so release the global connect budget synchronously.
        drop(connect_permit);
        Ok(socket)
    }
}

impl OpenStreamSession {
    async fn open(
        deps: &ShardDeps,
        shard_id: usize,
        tokens: &HashSet<TokenId>,
    ) -> Result<Self, StreamEnd> {
        let stream_session_id = Uuid::now_v7();
        let Some(ticket) = deps
            .session_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .ok()
            .and_then(|previous| StreamSessionTicket::new(stream_session_id, previous + 1))
        else {
            tracing::error!(shard_id, "stream session epoch exhausted");
            deps.shutdown.cancel();
            return Err(StreamEnd::Shutdown);
        };
        let opened_at_ms = Utc::now().timestamp_millis();
        let mut subscription_tokens = tokens.iter().cloned().collect::<Vec<_>>();
        subscription_tokens.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
        let subscription_token_hash =
            subscription_token_hash(&subscription_tokens).map_err(StreamEnd::Failed)?;
        let subscription_tokens: Arc<[TokenId]> = Arc::from(subscription_tokens);
        let subscription_token_count = u32::try_from(subscription_tokens.len()).unwrap_or(u32::MAX);
        if !send_session_event(
            deps,
            PipelineEvent::StreamSessionOpened {
                session: ticket,
                shard_id: u32::try_from(shard_id).unwrap_or(u32::MAX),
                subscription_token_hash,
                subscription_token_count,
                subscription_tokens: Arc::clone(&subscription_tokens),
                opened_at_ms,
            },
        )
        .await
        {
            if let Some(hook) = &deps.on_session_invalidated {
                hook(&subscription_tokens);
            }
            return Err(StreamEnd::Overflow(
                "stream-session open ledger enqueue timed out".to_owned(),
            ));
        }
        Ok(Self {
            ticket,
            subscription_token_hash,
            subscription_token_count,
            subscription_tokens,
            opened_at_ms,
            token_sequences: HashMap::new(),
        })
    }

    async fn pump(
        &mut self,
        socket: &mut MarketSocket,
        deps: &ShardDeps,
        shard_id: usize,
        assignment_rx: &mut Receiver<ShardAssignment>,
        assignment: &ShardAssignment,
    ) -> StreamEnd {
        let mut heartbeat = interval(MARKET_HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        heartbeat.tick().await;
        let mut awaiting_pong = false;

        loop {
            tokio::select! {
                biased;
                () = deps.shutdown.cancelled() => break StreamEnd::Shutdown,
                changed = assignment_rx.changed() => {
                    if changed.is_err() {
                        break StreamEnd::RouterDropped;
                    }
                    debounce_assignment_changes(assignment_rx, &deps.shutdown).await;
                    if *assignment_rx.borrow_and_update() != *assignment {
                        break StreamEnd::Resubscribe;
                    }
                }
                frame = socket.next() => {
                    match self.handle_frame(socket, deps, shard_id, frame).await {
                        Ok(true) => awaiting_pong = false,
                        Ok(false) => {}
                        Err(end) => break end,
                    }
                }
                _ = heartbeat.tick() => {
                    if awaiting_pong {
                        break StreamEnd::Failed(format!(
                            "WS heartbeat missed PONG within the documented {}s cadence",
                            MARKET_HEARTBEAT_INTERVAL.as_secs(),
                        ));
                    }
                    let sent = tokio::select! {
                        () = deps.shutdown.cancelled() => break StreamEnd::Shutdown,
                        result = socket.send(Message::Text("PING".into())) => result,
                    };
                    if let Err(error) = sent {
                        break StreamEnd::Failed(format!("WS heartbeat PING failed: {error}"));
                    }
                    awaiting_pong = true;
                }
            }
        }
    }

    async fn handle_frame(
        &mut self,
        socket: &mut MarketSocket,
        deps: &ShardDeps,
        shard_id: usize,
        frame: Option<Result<Message, WsTransportError>>,
    ) -> Result<bool, StreamEnd> {
        match frame {
            Some(Ok(Message::Text(text))) if text == "PONG" => Ok(true),
            Some(Ok(Message::Text(text))) => {
                let events =
                    normalize_market_frame(deps, text.as_bytes()).map_err(StreamEnd::Failed)?;
                dispatch_events(
                    deps,
                    shard_id,
                    self.ticket,
                    &mut self.token_sequences,
                    events,
                )
                .await
                .map_err(StreamEnd::Overflow)?;
                Ok(false)
            }
            Some(Ok(Message::Ping(payload))) => {
                let sent = tokio::select! {
                    () = deps.shutdown.cancelled() => return Err(StreamEnd::Shutdown),
                    result = socket.send(Message::Pong(payload)) => result,
                };
                sent.map_err(|error| {
                    StreamEnd::Failed(format!("WS protocol PONG failed: {error}"))
                })?;
                Ok(false)
            }
            Some(Ok(Message::Pong(_))) => Ok(true),
            Some(Ok(Message::Close(_))) => Err(StreamEnd::Failed(
                "WS peer closed the connection".to_owned(),
            )),
            Some(Ok(Message::Binary(_))) => Err(StreamEnd::Failed(
                "WS market channel sent binary data".to_owned(),
            )),
            Some(Ok(_)) => Err(StreamEnd::Failed(
                "WS market channel sent an unsupported frame".to_owned(),
            )),
            Some(Err(error)) => Err(StreamEnd::Failed(format!("WS receive failed: {error}"))),
            None => Err(StreamEnd::Failed("WS stream closed".to_owned())),
        }
    }

    async fn close(self, deps: &ShardDeps, shard_id: usize, reason: StreamSessionEndReason) {
        let closing = ClosingStreamSession {
            session: self.ticket,
            shard_id,
            subscription_token_hash: self.subscription_token_hash,
            subscription_token_count: self.subscription_token_count,
            subscription_tokens: &self.subscription_tokens,
            opened_at_ms: self.opened_at_ms,
            token_sequences: &self.token_sequences,
        };
        close_stream_session(deps, closing, reason).await;
    }
}

/// A single shard actor owning one market-channel WebSocket connection.
pub struct WsShard {
    shard_id: usize,
    assignment_rx: Receiver<ShardAssignment>,
    deps: ShardDeps,
}

impl WsShard {
    pub(super) const fn new(
        shard_id: usize,
        assignment_rx: Receiver<ShardAssignment>,
        deps: ShardDeps,
    ) -> Self {
        Self {
            shard_id,
            assignment_rx,
            deps,
        }
    }

    /// Resident actor loop — runs until shutdown or router teardown.
    pub async fn run_loop(self) {
        let Self {
            shard_id,
            mut assignment_rx,
            deps,
        } = self;
        let mut reconnect = ReconnectState::new(shard_id, &deps.reconnect_policy);

        // Stagger initial connects across shards (thundering-herd protection).
        let stagger = STARTUP_STAGGER_STEP * stagger_slot(shard_id);
        if !stagger.is_zero() {
            tokio::select! {
                () = deps.shutdown.cancelled() => return,
                () = sleep(stagger) => {}
            }
        }

        loop {
            if deps.shutdown.is_cancelled() {
                break;
            }

            // Park while the shard owns no tokens — no reconnect churn on idle.
            if assignment_rx.borrow_and_update().tokens.is_empty() {
                tokio::select! {
                    () = deps.shutdown.cancelled() => break,
                    changed = assignment_rx.changed() => {
                        if changed.is_err() {
                            break;
                        }
                        debounce_assignment_changes(&mut assignment_rx, &deps.shutdown).await;
                        continue;
                    }
                }
            }

            emit_status(
                &deps,
                shard_id,
                ShardConnectionStatus::Reconnecting {
                    attempt: reconnect.retries_used(),
                },
            );

            let end = connect_and_stream(&deps, shard_id, &mut assignment_rx, &mut reconnect).await;
            deps.health.set_connected(shard_id, false);
            emit_status(&deps, shard_id, ShardConnectionStatus::Disconnected);

            match end {
                StreamEnd::Shutdown | StreamEnd::RouterDropped => break,
                StreamEnd::Resubscribe => reconnect.reset(),
                StreamEnd::Failed(error) | StreamEnd::Overflow(error) => {
                    let Some(delay) = reconnect.next_delay() else {
                        tracing::error!(shard_id, "WS shard reconnection budget exhausted");
                        break;
                    };
                    // Reconnect spam is aggregated by the health checker;
                    // per-attempt detail stays at debug.
                    tracing::debug!(
                        shard_id,
                        error,
                        backoff_ms = delay.as_millis(),
                        "shard stream ended — reconnect scheduled"
                    );
                    tokio::select! {
                        () = deps.shutdown.cancelled() => break,
                        () = sleep(delay) => {}
                        changed = assignment_rx.changed() => {
                            // Token changes cut the backoff short: resubscribe
                            // with the fresh set right away.
                            if changed.is_err() {
                                break;
                            }
                            debounce_assignment_changes(&mut assignment_rx, &deps.shutdown).await;
                        }
                    }
                }
            }
        }

        tracing::info!(shard_id, "WS shard shutting down");
    }
}

/// Map `shard_id` onto a bounded stagger slot multiplier.
fn stagger_slot(shard_id: usize) -> u32 {
    u32::try_from(shard_id % STARTUP_STAGGER_SLOTS).unwrap_or(0)
}

/// Wait for the token set to settle: every further change restarts the window.
async fn debounce_assignment_changes(
    assignment_rx: &mut Receiver<ShardAssignment>,
    shutdown: &CancellationToken,
) {
    loop {
        let deadline = sleep(TOKEN_DEBOUNCE);
        tokio::pin!(deadline);
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = &mut deadline => return,
            changed = assignment_rx.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

/// Own one market-channel socket over the current token set and pump it until
/// shutdown, failure, or a (debounced) token-set change.
async fn connect_and_stream(
    deps: &ShardDeps,
    shard_id: usize,
    assignment_rx: &mut Receiver<ShardAssignment>,
    reconnect: &mut ReconnectState,
) -> StreamEnd {
    let assignment = assignment_rx.borrow_and_update().clone();
    let tokens = Arc::clone(&assignment.tokens);
    let asset_ids: Vec<U256> = tokens
        .iter()
        .filter_map(|t| U256::from_str(t.as_str()).ok())
        .collect();
    if asset_ids.is_empty() {
        // Non-empty set with zero parseable ids: back off instead of spinning.
        return StreamEnd::Failed("no subscribable asset ids in token set".to_owned());
    }

    let mut socket = match deps.connect_market(assignment_rx, asset_ids).await {
        Ok(socket) => socket,
        Err(end) => return end,
    };
    reconnect.reset();
    deps.health.set_connected(shard_id, true);
    emit_status(deps, shard_id, ShardConnectionStatus::Connected);
    let mut session = match OpenStreamSession::open(deps, shard_id, &tokens).await {
        Ok(session) => session,
        Err(end) => return end,
    };
    let end = session
        .pump(&mut socket, deps, shard_id, assignment_rx, &assignment)
        .await;
    if let Err(error) = socket.close(None).await {
        tracing::debug!(shard_id, %error, "WS socket close handshake failed");
    }
    session.close(deps, shard_id, end.stream_end_reason()).await;
    end
}

fn normalize_market_frame(deps: &ShardDeps, payload: &[u8]) -> Result<Vec<PipelineEvent>, String> {
    let ws_ingress = Instant::now();
    let messages = parse_if_interested(payload, &MessageInterest::MARKET)
        .map_err(|error| format!("WS market payload decoding failed: {error}"))?;
    let mut events = Vec::new();
    for message in messages {
        events.extend(
            normalize_ws_message(
                message,
                ws_ingress,
                deps.on_book_level_rejected.as_ref(),
                deps.token_resolver.as_ref(),
            )
            .map_err(|error| error.to_string())?,
        );
    }
    Ok(events)
}

async fn dispatch_events(
    deps: &ShardDeps,
    shard_id: usize,
    session: StreamSessionTicket,
    token_sequences: &mut HashMap<TokenKey, u64>,
    mut events: Vec<PipelineEvent>,
) -> Result<(), String> {
    let received_at = Instant::now();
    if !events.is_empty() {
        let tick = u64::try_from(received_at.duration_since(*deps.message_epoch).as_millis())
            .unwrap_or(u64::MAX - 1)
            .saturating_add(1);
        deps.last_message_tick.store(tick, Ordering::Release);
    }
    for event in &mut events {
        let token_sequence = event.token().map_or(0, |token| {
            let sequence = token_sequences.entry(token).or_insert(0);
            *sequence = sequence.saturating_add(1);
            *sequence
        });
        event.assign_stream_provenance(
            session,
            u32::try_from(shard_id).unwrap_or(u32::MAX),
            token_sequence,
        );
    }
    send_event_batch(deps, events, OUTPUT_ENQUEUE_TIMEOUT).await
}

fn subscription_token_hash(tokens: &[TokenId]) -> Result<ContentHash, String> {
    let token_ids = tokens.iter().map(TokenId::as_str).collect::<Vec<_>>();
    CanonicalDigest::content_hash_json(&token_ids).map_err(|error| error.to_string())
}

impl StreamEnd {
    const fn stream_end_reason(&self) -> StreamSessionEndReason {
        match self {
            Self::Shutdown | Self::RouterDropped => StreamSessionEndReason::Shutdown,
            Self::Resubscribe => StreamSessionEndReason::Resubscribe,
            Self::Overflow(_) => StreamSessionEndReason::Overflow,
            Self::Failed(_) => StreamSessionEndReason::Disconnect,
        }
    }
}

async fn send_session_event(deps: &ShardDeps, event: PipelineEvent) -> bool {
    send_session_events(deps, vec![event]).await
}

async fn send_session_events(deps: &ShardDeps, events: Vec<PipelineEvent>) -> bool {
    send_event_batch(deps, events, SESSION_LEDGER_TIMEOUT)
        .await
        .is_ok()
}

async fn send_event_batch(
    deps: &ShardDeps,
    events: Vec<PipelineEvent>,
    enqueue_timeout: Duration,
) -> Result<(), String> {
    if events.is_empty() {
        return Ok(());
    }
    let event_count = events
        .iter()
        .filter(|event| event.token().is_some())
        .count();
    let enqueue_started = Instant::now();
    let permits = ingress_permits(&events);
    let batch = NormalizedIngressBatch::new(
        events,
        timeout(
            enqueue_timeout,
            Arc::clone(&deps.ingress_budget).acquire_many_owned(permits),
        )
        .await
        .map_err(|_| {
            "WS ingress memory budget timed out; canonical session invalidated".to_owned()
        })?
        .map_err(|_| "WS ingress memory budget closed".to_owned())?,
    );
    timeout(enqueue_timeout, deps.output_tx.send_async(batch))
        .await
        .map_err(|_| "WS output queue timed out; canonical session invalidated".to_owned())?
        .map_err(|_| "WS output queue closed".to_owned())?;
    if event_count > 0
        && let Some(observer) = &deps.ingress_enqueue_observer
    {
        observer(enqueue_started.elapsed(), event_count);
    }
    Ok(())
}

struct ClosingStreamSession<'a> {
    session: StreamSessionTicket,
    shard_id: usize,
    subscription_token_hash: ContentHash,
    subscription_token_count: u32,
    subscription_tokens: &'a [TokenId],
    opened_at_ms: i64,
    token_sequences: &'a HashMap<TokenKey, u64>,
}

async fn close_stream_session(
    deps: &ShardDeps,
    session: ClosingStreamSession<'_>,
    reason: StreamSessionEndReason,
) {
    let ClosingStreamSession {
        session,
        shard_id,
        subscription_token_hash,
        subscription_token_count,
        subscription_tokens,
        opened_at_ms,
        token_sequences,
    } = session;
    if reason != StreamSessionEndReason::Normal
        && let Some(hook) = &deps.on_session_invalidated
    {
        hook(subscription_tokens);
    }
    let closed_at_ms = Utc::now().timestamp_millis();
    let mut received_sequences = token_sequences
        .iter()
        .map(|(token_id, sequence)| (*token_id, *sequence))
        .collect::<Vec<_>>();
    received_sequences.sort_unstable_by_key(|entry| entry.0);
    let received_sequences: Arc<[(TokenKey, u64)]> = Arc::from(received_sequences);
    let closed = PipelineEvent::StreamSessionClosed {
        session,
        shard_id: u32::try_from(shard_id).unwrap_or(u32::MAX),
        subscription_token_hash,
        subscription_token_count,
        subscription_tokens: Arc::from(subscription_tokens),
        received_sequences: Arc::clone(&received_sequences),
        opened_at_ms,
        closed_at_ms,
        reason,
    };
    let mut events = Vec::with_capacity(received_sequences.len().saturating_add(1));
    events.push(closed);
    if reason != StreamSessionEndReason::Normal {
        events.extend(
            received_sequences
                .iter()
                .map(|(token, last_received_sequence)| PipelineEvent::StreamGap {
                    token: *token,
                    session,
                    shard_id: u32::try_from(shard_id).unwrap_or(u32::MAX),
                    last_received_sequence: *last_received_sequence,
                    timestamp_ms: u64::try_from(closed_at_ms).unwrap_or(0),
                }),
        );
    }
    if !send_session_events(deps, events).await {
        tracing::error!(
            stream_session_id = %session.stream_session_id,
            shard_id,
            affected_tokens = subscription_tokens.len(),
            ?reason,
            "failed to enqueue stream-session close ledger; gap fan-out suppressed"
        );
    }
}

fn emit_status(deps: &ShardDeps, shard_id: usize, status: ShardConnectionStatus) {
    let events = vec![PipelineEvent::ShardStatus { shard_id, status }];
    let permits = ingress_permits(&events);
    let Ok(permit) = Arc::clone(&deps.ingress_budget).try_acquire_many_owned(permits) else {
        return;
    };
    let _ = deps
        .output_tx
        .try_send(NormalizedIngressBatch::new(events, permit));
}

#[cfg(test)]
mod tests {
    use std::{
        slice,
        sync::atomic::{AtomicU64, Ordering},
    };

    use flume::Sender;
    use tokio::{
        net::TcpListener,
        sync::{
            mpsc::{self, UnboundedSender},
            watch,
        },
        time::timeout,
    };
    use tokio_tungstenite::accept_async;

    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum SocketStage {
        Subscribed(u8),
        Closed(u8),
    }

    async fn observe_socket_lifecycle(
        listener: TcpListener,
        stage_tx: UnboundedSender<SocketStage>,
    ) -> Result<(), String> {
        for generation in 1..=2 {
            let (stream, _) = listener.accept().await.map_err(|error| error.to_string())?;
            let mut socket = accept_async(stream)
                .await
                .map_err(|error| error.to_string())?;
            match socket.next().await {
                Some(Ok(Message::Text(subscription)))
                    if subscription.contains("\"assets_ids\"") => {}
                other => return Err(format!("missing subscription frame: {other:?}")),
            }
            stage_tx
                .send(SocketStage::Subscribed(generation))
                .map_err(|_| "socket stage receiver closed".to_owned())?;
            loop {
                match socket.next().await {
                    Some(Ok(Message::Text(text))) if text == "PING" => socket
                        .send(Message::Text("PONG".into()))
                        .await
                        .map_err(|error| error.to_string())?,
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error.to_string()),
                }
            }
            stage_tx
                .send(SocketStage::Closed(generation))
                .map_err(|_| "socket stage receiver closed".to_owned())?;
        }
        Ok(())
    }

    fn test_deps(
        output_tx: Sender<NormalizedIngressBatch>,
        hook: Option<WsSessionInvalidationHook>,
        ws_url: String,
        shutdown: CancellationToken,
    ) -> ShardDeps {
        ShardDeps {
            output_tx,
            ingress_budget: Arc::new(Semaphore::new(256)),
            ws_url,
            shutdown,
            message_epoch: Arc::new(Instant::now()),
            last_message_tick: Arc::new(AtomicU64::new(0)),
            session_epoch: Arc::new(AtomicU64::new(0)),
            token_resolver: Arc::new(|token: U256| Some(TokenKey::new(token.to::<u32>()))),
            on_session_invalidated: hook,
            on_book_level_rejected: None,
            ingress_enqueue_observer: None,
            reconnect_policy: ReconnectPolicy::default(),
            connect_limiter: Arc::new(Semaphore::new(4)),
            health: Arc::new(ShardHealthBoard::default()),
        }
    }

    fn ticket() -> StreamSessionTicket {
        StreamSessionTicket::new(Uuid::new_v4(), 1).expect("valid session ticket")
    }

    #[tokio::test]
    async fn dispatch_preserves_event_available() {
        let (tx, rx) = flume::bounded(3);
        let dropped = Arc::new(AtomicU64::new(0));
        let hook: WsSessionInvalidationHook = {
            let dropped = Arc::clone(&dropped);
            Arc::new(move |tokens| {
                dropped.fetch_add(
                    u64::try_from(tokens.len()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
            })
        };
        let deps = test_deps(tx, Some(hook), "ws://test".into(), CancellationToken::new());

        let status = |_n| PipelineEvent::ShardStatus {
            shard_id: 0,
            status: ShardConnectionStatus::Connected,
        };

        let mut sequences = HashMap::new();
        dispatch_events(
            &deps,
            0,
            ticket(),
            &mut sequences,
            vec![status(1), status(2), status(3)],
        )
        .await
        .expect("dispatch");

        assert_eq!(rx.len(), 1);
        assert_eq!(rx.recv().expect("batch").events.len(), 3);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn batch_rejects_after_wait() {
        let (tx, _rx) = flume::bounded(1);
        tx.send(NormalizedIngressBatch::new(
            vec![PipelineEvent::ShardStatus {
                shard_id: 0,
                status: ShardConnectionStatus::Connected,
            }],
            Arc::new(Semaphore::new(1))
                .try_acquire_owned()
                .expect("permit"),
        ))
        .expect("fill output queue");
        let deps = test_deps(tx, None, "ws://test".into(), CancellationToken::new());
        let mut sequences = HashMap::new();

        let error = dispatch_events(
            &deps,
            0,
            ticket(),
            &mut sequences,
            vec![PipelineEvent::ShardStatus {
                shard_id: 0,
                status: ShardConnectionStatus::Connected,
            }],
        )
        .await
        .expect_err("full ingress queue must time out");
        drop(deps);

        assert!(error.contains("timed out"));
    }

    #[tokio::test]
    async fn session_close_gaps_batch() {
        let (tx, rx) = flume::bounded(1);
        let deps = test_deps(tx, None, "ws://test".into(), CancellationToken::new());
        let token = TokenId::new("1");
        let sequences = HashMap::from([(TokenKey::new(1), 7)]);

        close_stream_session(
            &deps,
            ClosingStreamSession {
                session: ticket(),
                shard_id: 0,
                subscription_token_hash: subscription_token_hash(slice::from_ref(&token))
                    .expect("subscription hash"),
                subscription_token_count: 1,
                subscription_tokens: slice::from_ref(&token),
                opened_at_ms: 1,
                token_sequences: &sequences,
            },
            StreamSessionEndReason::Overflow,
        )
        .await;

        let batch = rx.recv().expect("close batch");
        assert_eq!(batch.events.len(), 2, "one close plus one gap");
        drop(batch);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_session_without_fanout() {
        let (tx, rx) = flume::bounded(1);
        tx.send(NormalizedIngressBatch::new(
            vec![PipelineEvent::ShardStatus {
                shard_id: 0,
                status: ShardConnectionStatus::Connected,
            }],
            Arc::new(Semaphore::new(1))
                .try_acquire_owned()
                .expect("permit"),
        ))
        .expect("fill output queue");
        let invalidations = Arc::new(AtomicU64::new(0));
        let hook: WsSessionInvalidationHook = {
            let invalidations = Arc::clone(&invalidations);
            Arc::new(move |tokens| {
                invalidations.fetch_add(
                    u64::try_from(tokens.len()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
            })
        };
        let deps = test_deps(tx, Some(hook), "ws://test".into(), CancellationToken::new());
        let token = TokenId::new("1");
        let session_id = Uuid::new_v4();
        let sequences = HashMap::from([(TokenKey::new(1), 7)]);

        close_stream_session(
            &deps,
            ClosingStreamSession {
                session: StreamSessionTicket::new(session_id, 1).expect("valid session ticket"),
                shard_id: 0,
                subscription_token_hash: subscription_token_hash(slice::from_ref(&token))
                    .expect("subscription hash"),
                subscription_token_count: 1,
                subscription_tokens: slice::from_ref(&token),
                opened_at_ms: 1,
                token_sequences: &sequences,
            },
            StreamSessionEndReason::Overflow,
        )
        .await;

        assert_eq!(
            rx.len(),
            1,
            "no per-token gaps are queued after close failure"
        );
        assert_eq!(invalidations.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn debounce_waits_token_settle() {
        let (tx, mut rx) = watch::channel(ShardAssignment::empty());
        let shutdown = CancellationToken::new();

        tx.send_replace(ShardAssignment {
            tokens: Arc::new(HashSet::from([TokenId::new("1")])),
            restart_generation: 0,
        });
        rx.changed().await.expect("sender alive");

        let debounce = tokio::spawn(async move {
            debounce_assignment_changes(&mut rx, &shutdown).await;
            rx.borrow().clone()
        });

        // A second update inside the window must be folded into one rebuild.
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send_replace(ShardAssignment {
            tokens: Arc::new(HashSet::from([TokenId::new("1"), TokenId::new("2")])),
            restart_generation: 0,
        });

        let settled = debounce.await.expect("debounce task");
        assert_eq!(
            settled.tokens.len(),
            2,
            "debounced set reflects the last write"
        );
    }

    #[tokio::test]
    async fn restart_closes_owned_socket() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket listener");
        let address = listener.local_addr().expect("loopback listener address");
        let (stage_tx, mut stage_rx) = mpsc::unbounded_channel();
        let server = tokio::spawn(observe_socket_lifecycle(listener, stage_tx));
        let (output_tx, _output_rx) = flume::bounded(64);
        let initial = ShardAssignment {
            tokens: Arc::new(HashSet::from([TokenId::new("1")])),
            restart_generation: 0,
        };
        let (assignment_tx, assignment_rx) = watch::channel(initial.clone());
        let shutdown = CancellationToken::new();
        let shard = tokio::spawn(
            WsShard::new(
                0,
                assignment_rx,
                test_deps(output_tx, None, format!("ws://{address}"), shutdown.clone()),
            )
            .run_loop(),
        );

        assert_eq!(
            timeout(Duration::from_secs(5), stage_rx.recv())
                .await
                .expect("first subscription deadline"),
            Some(SocketStage::Subscribed(1)),
        );
        assignment_tx.send_replace(ShardAssignment {
            restart_generation: 1,
            ..initial
        });
        assert_eq!(
            timeout(Duration::from_secs(5), stage_rx.recv())
                .await
                .expect("first socket close deadline"),
            Some(SocketStage::Closed(1)),
        );
        assert_eq!(
            timeout(Duration::from_secs(5), stage_rx.recv())
                .await
                .expect("second subscription deadline"),
            Some(SocketStage::Subscribed(2)),
        );

        shutdown.cancel();
        assert_eq!(
            timeout(Duration::from_secs(5), stage_rx.recv())
                .await
                .expect("second socket close deadline"),
            Some(SocketStage::Closed(2)),
        );
        timeout(Duration::from_secs(5), shard)
            .await
            .expect("shard shutdown deadline")
            .expect("shard task");
        timeout(Duration::from_secs(5), server)
            .await
            .expect("server shutdown deadline")
            .expect("server task")
            .expect("socket lifecycle");
    }

    #[test]
    fn restart_changes_without_churn() {
        let tokens = Arc::new(HashSet::from([TokenId::new("1")]));
        let first = ShardAssignment {
            tokens: Arc::clone(&tokens),
            restart_generation: 1,
        };
        let restarted = ShardAssignment {
            tokens,
            restart_generation: 2,
        };

        assert_ne!(first, restarted);
        assert_eq!(first.tokens, restarted.tokens);
    }
}
