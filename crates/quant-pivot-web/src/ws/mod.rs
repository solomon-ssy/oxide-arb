//! WebSocket real-time push infrastructure.
//!
//! [`SessionHub`] is the only writer for session lifecycle, subscriptions and
//! fan-out indexes. Connection tasks communicate through a bounded command
//! queue; the event broadcaster serializes each envelope once into a shared
//! [`ByteString`] before dispatch.

pub mod feedback;
pub mod handler;
pub mod session;

use std::{
    hash::Hash,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use ahash::{AHashMap, AHashSet};
use arc_swap::ArcSwap;
use bytestring::ByteString;
use flume::Receiver as FlumeReceiver;
use parking_lot::Mutex;
use prometheus::{GaugeVec, Histogram, IntCounter, IntGauge, IntGaugeVec};
use quant_pivot_models::{
    domain::{
        runtime::CoreEvent,
        ws::{SubscriptionKey, WsChannel},
    },
    types::{MarketId, UserId},
};
use smallvec::SmallVec;
use tokio::{
    sync::{
        Notify, OwnedSemaphorePermit, Semaphore,
        mpsc::{self, Receiver, Sender, error::TrySendError},
        oneshot::{self, Sender as OneshotSender},
    },
    time::timeout,
};
use tokio_util::sync::CancellationToken;

const CONTROL_TIMEOUT: Duration = Duration::from_millis(100);
const HUB_CONTROL_CAPACITY: usize = 1_024;
const HUB_RELIABLE_CAPACITY: usize = 2_048;
const HUB_BEST_EFFORT_TOPIC_CAPACITY: usize = 8_192;
const HUB_FRAME_BUDGET_BYTES: usize = 64 * 1_024 * 1_024;
const HUB_MAX_FRAME_BYTES: usize = 1_024 * 1_024;
pub const SESSION_REPLAY_CAPACITY: usize = 128;

/// Per-connection identifier within the process-local session hub.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(u64);

/// Queue-full policy for one outbound event class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryClass {
    /// Fresh state supersedes older state. A full client queue drops the frame.
    BestEffort,
    /// Lifecycle/control state must not be silently skipped. A full queue closes
    /// the slow session so it reconnects and performs a canonical sync.
    Reliable,
}

impl DeliveryClass {
    #[must_use]
    pub const fn for_channel(channel: WsChannel) -> Self {
        match channel {
            WsChannel::MarketBookUpdate | WsChannel::SystemStatus => Self::BestEffort,
            WsChannel::SystemAlert
            | WsChannel::MarketResolved
            | WsChannel::ConfigActivated
            | WsChannel::QuantReport
            | WsChannel::QuantReportRun
            | WsChannel::QuantIntent
            | WsChannel::QuantExecutionOrder
            | WsChannel::QuantCondition
            | WsChannel::MaterializationRunUpdate
            | WsChannel::ResearchFeedback
            | WsChannel::QuantReconciliation
            | WsChannel::QuantSettlement => Self::Reliable,
        }
    }
}

/// Immutable registration payload transferred from one connection task.
pub struct SessionRegistration {
    pub outbound: Sender<SharedFrame>,
    pub subject: UserId,
    pub family_id: String,
    pub can_read_system: bool,
    pub cancellation: CancellationToken,
}

/// Mutable state owned exclusively by [`SessionHub`].
pub struct SessionRecord {
    pub outbound: Sender<SharedFrame>,
    pub cancellation: CancellationToken,
    subject: UserId,
    family_id: String,
    subscriptions: AHashSet<SubscriptionKey>,
}

/// Prometheus counters updated by the single hub writer.
#[derive(Clone)]
pub struct SessionHubMetrics {
    pub best_effort_dropped: IntCounter,
    pub best_effort_coalesced: IntCounter,
    pub reliable_disconnects: IntCounter,
    pub control_timeouts: IntCounter,
    pub control_latency_seconds: Histogram,
    pub queue_depth: IntGaugeVec,
    pub queue_oldest_age_seconds: GaugeVec,
    pub frame_bytes: IntGauge,
}

/// One encoded frame charged exactly once against the process-wide byte
/// budget. Every outbox clone shares both allocation and byte permit.
#[derive(Clone)]
pub struct SharedFrame(Arc<SharedFrameInner>);

struct SharedFrameInner {
    text: ByteString,
    _byte_permit: OwnedSemaphorePermit,
    frame_bytes: IntGauge,
}

impl SharedFrame {
    fn new(text: ByteString, byte_permit: OwnedSemaphorePermit, frame_bytes: IntGauge) -> Self {
        frame_bytes.add(i64::try_from(text.len()).unwrap_or(i64::MAX));
        Self(Arc::new(SharedFrameInner {
            text,
            _byte_permit: byte_permit,
            frame_bytes,
        }))
    }

    #[must_use]
    pub fn text(&self) -> &ByteString {
        &self.0.text
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.text.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.text.is_empty()
    }
}

impl Deref for SharedFrame {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.text.as_ref()
    }
}

impl Drop for SharedFrameInner {
    fn drop(&mut self) {
        self.frame_bytes
            .sub(i64::try_from(self.text.len()).unwrap_or(i64::MAX));
    }
}

/// Security and subscription commands accepted by the high-priority lane.
pub enum SessionControlCommand {
    Register {
        session_id: SessionId,
        registration: SessionRegistration,
        completion: OneshotSender<bool>,
    },
    Deregister {
        session_id: SessionId,
        completion: OneshotSender<bool>,
    },
    Subscribe {
        session_id: SessionId,
        key: SubscriptionKey,
        completion: OneshotSender<bool>,
    },
    Unsubscribe {
        session_id: SessionId,
        key: SubscriptionKey,
        completion: OneshotSender<bool>,
    },
    Replay {
        session_id: SessionId,
        frames: Vec<SharedFrame>,
        completion: OneshotSender<bool>,
    },
    CloseSession {
        session_id: SessionId,
        completion: OneshotSender<bool>,
    },
    CloseSubject {
        subject: UserId,
        completion: OneshotSender<bool>,
    },
    CloseFamily {
        family_id: String,
        completion: OneshotSender<bool>,
    },
    CloseAll {
        completion: OneshotSender<bool>,
    },
    DisconnectTopic {
        key: SubscriptionKey,
        completion: OneshotSender<bool>,
    },
}

struct ReliableFanout {
    key: SubscriptionKey,
    frame: SharedFrame,
    enqueued_at: Instant,
}

struct PendingBestEffort {
    frame: SharedFrame,
    enqueued_at: Instant,
}

struct BestEffortState {
    pending: AHashMap<SubscriptionKey, PendingBestEffort>,
}

struct BestEffortCoalescer {
    state: Mutex<BestEffortState>,
    notify: Notify,
    capacity: usize,
    metrics: SessionHubMetrics,
}

/// Cloneable command/read handle shared by HTTP and connection tasks.
#[derive(Clone)]
pub struct SessionRegistry {
    control: Sender<SessionControlCommand>,
    reliable: Sender<ReliableFanout>,
    best_effort: Arc<BestEffortCoalescer>,
    frame_budget: Arc<Semaphore>,
    fail_closed: CancellationToken,
    metrics: SessionHubMetrics,
    next_id: Arc<AtomicU64>,
    session_count: Arc<AtomicUsize>,
    watched_markets: Arc<ArcSwap<AHashSet<MarketId>>>,
}

impl SessionRegistry {
    /// Create the shared handle and its unique single-writer actor.
    #[must_use]
    pub fn new(metrics: SessionHubMetrics) -> (Self, SessionHub) {
        let (control, control_rx) = mpsc::channel(HUB_CONTROL_CAPACITY);
        let (reliable, reliable_rx) = mpsc::channel(HUB_RELIABLE_CAPACITY);
        let session_count = Arc::new(AtomicUsize::new(0));
        let watched_markets = Arc::new(ArcSwap::from_pointee(AHashSet::new()));
        let fail_closed = CancellationToken::new();
        let best_effort = Arc::new(BestEffortCoalescer {
            state: Mutex::new(BestEffortState {
                pending: AHashMap::new(),
            }),
            notify: Notify::new(),
            capacity: HUB_BEST_EFFORT_TOPIC_CAPACITY,
            metrics: metrics.clone(),
        });
        (
            Self {
                control,
                reliable,
                best_effort: Arc::clone(&best_effort),
                frame_budget: Arc::new(Semaphore::new(HUB_FRAME_BUDGET_BYTES)),
                fail_closed: fail_closed.clone(),
                metrics: metrics.clone(),
                next_id: Arc::new(AtomicU64::new(1)),
                session_count: Arc::clone(&session_count),
                watched_markets: Arc::clone(&watched_markets),
            },
            SessionHub::new(
                control_rx,
                reliable_rx,
                best_effort,
                session_count,
                watched_markets,
                metrics,
                fail_closed,
            ),
        )
    }

    /// Register a session only after the actor has installed all reverse indexes.
    pub async fn register(&self, registration: SessionRegistration) -> Option<SessionId> {
        let session_id =
            match self
                .next_id
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    current.checked_add(1)
                }) {
                Ok(id) if id != 0 => SessionId(id),
                Ok(_) | Err(_) => {
                    self.fail_closed.cancel();
                    return None;
                }
            };
        self.confirm(|completion| SessionControlCommand::Register {
            session_id,
            registration,
            completion,
        })
        .await
        .then_some(session_id)
    }

    /// Remove one disconnected session and all of its topic memberships.
    pub async fn deregister(&self, session_id: SessionId) {
        self.confirm(|completion| SessionControlCommand::Deregister {
            session_id,
            completion,
        })
        .await;
    }

    /// Add one exact channel/scope subscription.
    pub async fn subscribe(&self, session_id: SessionId, key: SubscriptionKey) -> bool {
        self.confirm(|completion| SessionControlCommand::Subscribe {
            session_id,
            key,
            completion,
        })
        .await
    }

    /// Remove one exact channel/scope subscription.
    pub async fn unsubscribe(&self, session_id: SessionId, key: SubscriptionKey) -> bool {
        self.confirm(|completion| SessionControlCommand::Unsubscribe {
            session_id,
            key,
            completion,
        })
        .await
    }

    /// Queue one bounded durable replay batch for an exact session.
    pub async fn replay(&self, session_id: SessionId, frames: Vec<ByteString>) -> bool {
        if frames.len() > SESSION_REPLAY_CAPACITY {
            self.close_session(session_id).await;
            return false;
        }
        let Some(charged) = self.charge_replay(frames).await else {
            self.close_session(session_id).await;
            return false;
        };
        self.confirm(move |completion| SessionControlCommand::Replay {
            session_id,
            frames: charged,
            completion,
        })
        .await
    }

    /// Close one exact session after a fail-closed protocol condition.
    pub async fn close_session(&self, session_id: SessionId) {
        self.confirm(|completion| SessionControlCommand::CloseSession {
            session_id,
            completion,
        })
        .await;
    }

    /// Close every live socket owned by one user.
    pub async fn close_subject(&self, subject: UserId) {
        self.confirm(|completion| SessionControlCommand::CloseSubject {
            subject,
            completion,
        })
        .await;
    }

    /// Close every live socket issued from one refresh-session family.
    pub async fn close_family(&self, family_id: &str) {
        self.confirm(|completion| SessionControlCommand::CloseFamily {
            family_id: family_id.to_owned(),
            completion,
        })
        .await;
    }

    /// Close all sockets after a global RBAC policy revision.
    pub async fn close_all(&self) {
        self.confirm(|completion| SessionControlCommand::CloseAll { completion })
            .await;
    }

    /// Queue one already-serialized frame for indexed fan-out.
    pub async fn fanout(
        &self,
        key: SubscriptionKey,
        frame: ByteString,
        delivery: DeliveryClass,
    ) -> bool {
        if self.fail_closed.is_cancelled() {
            return false;
        }
        let Some(frame) = self.charge_frame(frame, delivery).await else {
            return match delivery {
                DeliveryClass::BestEffort => true,
                DeliveryClass::Reliable => self.disconnect_topic(key).await,
            };
        };
        match delivery {
            DeliveryClass::BestEffort => {
                self.best_effort.push(key, frame);
                true
            }
            DeliveryClass::Reliable => {
                let command = ReliableFanout {
                    key,
                    frame,
                    enqueued_at: Instant::now(),
                };
                match self.reliable.try_send(command) {
                    Ok(()) => {
                        self.update_queue_depth("reliable", &self.reliable);
                        true
                    }
                    Err(TrySendError::Full(command)) => self.disconnect_topic(command.key).await,
                    Err(TrySendError::Closed(_)) => {
                        self.fail_closed.cancel();
                        false
                    }
                }
            }
        }
    }

    /// Number of sessions installed in the actor (diagnostics only).
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.session_count.load(Ordering::Relaxed)
    }

    /// Borrow the immutable watched-market snapshot without an Arc refcount bump.
    pub fn read_watched_markets<R>(&self, read: impl FnOnce(&AHashSet<MarketId>) -> R) -> R {
        let snapshot = self.watched_markets.load();
        read(snapshot.as_ref())
    }

    #[must_use]
    pub fn fail_closed_token(&self) -> CancellationToken {
        self.fail_closed.clone()
    }

    async fn charge_frame(
        &self,
        frame: ByteString,
        delivery: DeliveryClass,
    ) -> Option<SharedFrame> {
        if frame.len() > HUB_MAX_FRAME_BYTES {
            if delivery == DeliveryClass::BestEffort {
                self.metrics.best_effort_dropped.inc();
            }
            return None;
        }
        let permits = u32::try_from(frame.len()).ok()?;
        let permit = match delivery {
            DeliveryClass::BestEffort => Arc::clone(&self.frame_budget)
                .try_acquire_many_owned(permits)
                .ok(),
            DeliveryClass::Reliable => timeout(
                CONTROL_TIMEOUT,
                Arc::clone(&self.frame_budget).acquire_many_owned(permits),
            )
            .await
            .ok()
            .and_then(Result::ok),
        };
        let Some(permit) = permit else {
            if delivery == DeliveryClass::BestEffort {
                self.metrics.best_effort_dropped.inc();
            }
            return None;
        };
        Some(SharedFrame::new(
            frame,
            permit,
            self.metrics.frame_bytes.clone(),
        ))
    }

    async fn charge_replay(&self, frames: Vec<ByteString>) -> Option<Vec<SharedFrame>> {
        let total_bytes = frames.iter().try_fold(0usize, |total, frame| {
            if frame.len() > HUB_MAX_FRAME_BYTES {
                None
            } else {
                total.checked_add(frame.len())
            }
        })?;
        if total_bytes > HUB_FRAME_BUDGET_BYTES {
            return None;
        }
        let permits = u32::try_from(total_bytes).ok()?;
        let mut batch_permit = timeout(
            CONTROL_TIMEOUT,
            Arc::clone(&self.frame_budget).acquire_many_owned(permits),
        )
        .await
        .ok()
        .and_then(Result::ok)?;
        let mut charged = Vec::with_capacity(frames.len());
        for frame in frames {
            let permit = batch_permit.split(frame.len())?;
            charged.push(SharedFrame::new(
                frame,
                permit,
                self.metrics.frame_bytes.clone(),
            ));
        }
        drop(batch_permit);
        Some(charged)
    }

    async fn disconnect_topic(&self, key: SubscriptionKey) -> bool {
        self.confirm(|completion| SessionControlCommand::DisconnectTopic { key, completion })
            .await
    }

    fn update_queue_depth<T>(&self, lane: &str, sender: &Sender<T>) {
        self.metrics.queue_depth.with_label_values(&[lane]).set(
            i64::try_from(sender.max_capacity().saturating_sub(sender.capacity()))
                .unwrap_or(i64::MAX),
        );
    }

    async fn confirm(
        &self,
        command: impl FnOnce(OneshotSender<bool>) -> SessionControlCommand,
    ) -> bool {
        if self.fail_closed.is_cancelled() {
            return false;
        }
        let (completion, applied) = oneshot::channel();
        let started_at = Instant::now();
        let confirmed = timeout(CONTROL_TIMEOUT, async {
            self.control
                .send(command(completion))
                .await
                .map_err(|_| ())?;
            self.update_queue_depth("control", &self.control);
            applied.await.map_err(|_| ())
        })
        .await;
        self.metrics
            .control_latency_seconds
            .observe(started_at.elapsed().as_secs_f64());
        self.metrics
            .queue_oldest_age_seconds
            .with_label_values(&["control"])
            .set(started_at.elapsed().as_secs_f64());
        if let Ok(Ok(applied)) = confirmed {
            applied
        } else {
            self.metrics.control_timeouts.inc();
            self.fail_closed.cancel();
            false
        }
    }
}

impl BestEffortCoalescer {
    fn push(&self, key: SubscriptionKey, frame: SharedFrame) {
        let mut state = self.state.lock();
        if state.pending.contains_key(&key) {
            self.metrics.best_effort_coalesced.inc();
        } else if state.pending.len() >= self.capacity {
            self.metrics.best_effort_dropped.inc();
            return;
        }
        state.pending.insert(
            key,
            PendingBestEffort {
                frame,
                enqueued_at: Instant::now(),
            },
        );
        self.metrics
            .queue_depth
            .with_label_values(&["best_effort"])
            .set(i64::try_from(state.pending.len()).unwrap_or(i64::MAX));
        drop(state);
        self.notify.notify_one();
    }

    fn pop(&self) -> Option<(SubscriptionKey, PendingBestEffort)> {
        let mut state = self.state.lock();
        let key = state.pending.keys().next()?.clone();
        let pending = state.pending.remove(&key)?;
        self.metrics
            .queue_depth
            .with_label_values(&["best_effort"])
            .set(i64::try_from(state.pending.len()).unwrap_or(i64::MAX));
        let oldest = state
            .pending
            .values()
            .map(|entry| entry.enqueued_at.elapsed().as_secs_f64())
            .fold(0.0_f64, f64::max);
        self.metrics
            .queue_oldest_age_seconds
            .with_label_values(&["best_effort"])
            .set(oldest);
        if !state.pending.is_empty() {
            self.notify.notify_one();
        }
        Some((key, pending))
    }
}

/// Single writer for every WebSocket session and reverse index.
pub struct SessionHub {
    control: Receiver<SessionControlCommand>,
    reliable: Receiver<ReliableFanout>,
    best_effort: Arc<BestEffortCoalescer>,
    fail_closed: CancellationToken,
    sessions: AHashMap<SessionId, SessionRecord>,
    topic_subscribers: AHashMap<SubscriptionKey, AHashSet<SessionId>>,
    subject_sessions: AHashMap<UserId, AHashSet<SessionId>>,
    family_sessions: AHashMap<String, AHashSet<SessionId>>,
    system_readers: AHashSet<SessionId>,
    watched_market_refcounts: AHashMap<MarketId, u32>,
    session_count: Arc<AtomicUsize>,
    watched_markets: Arc<ArcSwap<AHashSet<MarketId>>>,
    metrics: SessionHubMetrics,
}

impl SessionHub {
    fn new(
        control: Receiver<SessionControlCommand>,
        reliable: Receiver<ReliableFanout>,
        best_effort: Arc<BestEffortCoalescer>,
        session_count: Arc<AtomicUsize>,
        watched_markets: Arc<ArcSwap<AHashSet<MarketId>>>,
        metrics: SessionHubMetrics,
        fail_closed: CancellationToken,
    ) -> Self {
        Self {
            control,
            reliable,
            best_effort,
            fail_closed,
            sessions: AHashMap::new(),
            topic_subscribers: AHashMap::new(),
            subject_sessions: AHashMap::new(),
            family_sessions: AHashMap::new(),
            system_readers: AHashSet::new(),
            watched_market_refcounts: AHashMap::new(),
            session_count,
            watched_markets,
            metrics,
        }
    }

    /// Process commands until staged shutdown or all handles are dropped.
    pub async fn run(mut self, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                biased;

                () = shutdown.cancelled() => {
                    self.close_all_sessions();
                    tracing::info!("ws session hub shutting down");
                    return;
                }
                () = self.fail_closed.cancelled() => {
                    self.close_all_sessions();
                    tracing::error!("ws session hub entered fail-closed cancellation");
                    return;
                }
                command = self.control.recv() => {
                    let Some(command) = command else {
                        self.close_all_sessions();
                        self.fail_closed.cancel();
                        tracing::error!("ws session hub control lane closed");
                        return;
                    };
                    self.apply_control(command);
                    self.update_receiver_depth("control", &self.control);
                }
                command = self.reliable.recv() => {
                    let Some(command) = command else {
                        self.close_all_sessions();
                        self.fail_closed.cancel();
                        tracing::error!("ws session hub reliable lane closed");
                        return;
                    };
                    self.metrics
                        .queue_oldest_age_seconds
                        .with_label_values(&["reliable"])
                        .set(command.enqueued_at.elapsed().as_secs_f64());
                    self.fanout(&command.key, &command.frame, DeliveryClass::Reliable);
                    self.update_receiver_depth("reliable", &self.reliable);
                }
                () = self.best_effort.notify.notified() => {
                    if let Some((key, pending)) = self.best_effort.pop() {
                        self.fanout(&key, &pending.frame, DeliveryClass::BestEffort);
                    }
                }
            }
        }
    }

    fn update_receiver_depth<T>(&self, lane: &str, receiver: &Receiver<T>) {
        self.metrics
            .queue_depth
            .with_label_values(&[lane])
            .set(i64::try_from(receiver.len()).unwrap_or(i64::MAX));
    }

    fn apply_control(&mut self, command: SessionControlCommand) {
        match command {
            SessionControlCommand::Register {
                session_id,
                registration,
                completion,
            } => {
                let applied = self.register(session_id, registration);
                let _ = completion.send(applied);
            }
            SessionControlCommand::Deregister {
                session_id,
                completion,
            } => {
                self.remove_session(session_id, false);
                let _ = completion.send(true);
            }
            SessionControlCommand::Subscribe {
                session_id,
                key,
                completion,
            } => {
                let applied = self.subscribe(session_id, key);
                let _ = completion.send(applied);
            }
            SessionControlCommand::Unsubscribe {
                session_id,
                key,
                completion,
            } => {
                let applied = self.unsubscribe(session_id, &key);
                let _ = completion.send(applied);
            }
            SessionControlCommand::Replay {
                session_id,
                frames,
                completion,
            } => {
                let applied = self.replay(session_id, frames);
                let _ = completion.send(applied);
            }
            SessionControlCommand::CloseSession {
                session_id,
                completion,
            } => {
                self.remove_session(session_id, true);
                let _ = completion.send(true);
            }
            SessionControlCommand::CloseSubject {
                subject,
                completion,
            } => {
                self.close_subject(subject);
                let _ = completion.send(true);
            }
            SessionControlCommand::CloseFamily {
                family_id,
                completion,
            } => {
                self.close_family(&family_id);
                let _ = completion.send(true);
            }
            SessionControlCommand::CloseAll { completion } => {
                self.close_all_sessions();
                let _ = completion.send(true);
            }
            SessionControlCommand::DisconnectTopic { key, completion } => {
                self.disconnect_topic(&key);
                let _ = completion.send(true);
            }
        }
    }

    fn register(&mut self, session_id: SessionId, registration: SessionRegistration) -> bool {
        if self.sessions.contains_key(&session_id) {
            return false;
        }
        self.subject_sessions
            .entry(registration.subject)
            .or_default()
            .insert(session_id);
        self.family_sessions
            .entry(registration.family_id.clone())
            .or_default()
            .insert(session_id);
        if registration.can_read_system {
            self.system_readers.insert(session_id);
        }
        self.sessions.insert(
            session_id,
            SessionRecord {
                outbound: registration.outbound,
                cancellation: registration.cancellation,
                subject: registration.subject,
                family_id: registration.family_id,
                subscriptions: AHashSet::new(),
            },
        );
        self.update_session_count();
        true
    }

    fn subscribe(&mut self, session_id: SessionId, key: SubscriptionKey) -> bool {
        let Some(record) = self.sessions.get_mut(&session_id) else {
            return false;
        };
        if !record.subscriptions.insert(key.clone()) {
            return true;
        }
        self.increment_watched_market(&key);
        self.topic_subscribers
            .entry(key)
            .or_default()
            .insert(session_id);
        true
    }

    fn unsubscribe(&mut self, session_id: SessionId, key: &SubscriptionKey) -> bool {
        let Some(record) = self.sessions.get_mut(&session_id) else {
            return false;
        };
        if !record.subscriptions.remove(key) {
            return true;
        }
        self.remove_subscription_indexes(session_id, key);
        true
    }

    fn replay(&mut self, session_id: SessionId, frames: Vec<SharedFrame>) -> bool {
        let accepted = self.sessions.get(&session_id).is_some_and(|record| {
            if record.outbound.capacity() < frames.len() {
                return false;
            }
            frames
                .into_iter()
                .all(|frame| record.outbound.try_send(frame).is_ok())
        });
        if !accepted {
            self.metrics.reliable_disconnects.inc();
            self.remove_session(session_id, true);
        }
        accepted
    }

    fn close_subject(&mut self, subject: UserId) {
        let session_ids = self
            .subject_sessions
            .get(&subject)
            .map(|sessions| sessions.iter().copied().collect::<SmallVec<[_; 8]>>())
            .unwrap_or_default();
        for session_id in session_ids {
            self.remove_session(session_id, true);
        }
    }

    fn close_family(&mut self, family_id: &str) {
        let session_ids = self
            .family_sessions
            .get(family_id)
            .map(|sessions| sessions.iter().copied().collect::<SmallVec<[_; 8]>>())
            .unwrap_or_default();
        for session_id in session_ids {
            self.remove_session(session_id, true);
        }
    }

    fn fanout(&mut self, key: &SubscriptionKey, frame: &SharedFrame, delivery: DeliveryClass) {
        let mut disconnected = SmallVec::<[SessionId; 8]>::new();
        {
            let recipients = if matches!(
                key.channel,
                WsChannel::SystemStatus | WsChannel::SystemAlert
            ) {
                &self.system_readers
            } else {
                let Some(recipients) = self.topic_subscribers.get(key) else {
                    return;
                };
                recipients
            };
            for session_id in recipients {
                let Some(record) = self.sessions.get(session_id) else {
                    continue;
                };
                match record.outbound.try_send(frame.clone()) {
                    Ok(()) => {}
                    Err(TrySendError::Full(_)) if delivery == DeliveryClass::BestEffort => {
                        self.metrics.best_effort_dropped.inc();
                    }
                    Err(TrySendError::Full(_)) => {
                        self.metrics.reliable_disconnects.inc();
                        record.cancellation.cancel();
                        disconnected.push(*session_id);
                    }
                    Err(TrySendError::Closed(_)) => disconnected.push(*session_id),
                }
            }
        }
        for session_id in disconnected {
            self.remove_session(session_id, false);
        }
    }

    fn disconnect_topic(&mut self, key: &SubscriptionKey) -> usize {
        let session_ids = if matches!(
            key.channel,
            WsChannel::SystemStatus | WsChannel::SystemAlert
        ) {
            self.system_readers.iter().copied().collect::<Vec<_>>()
        } else {
            self.topic_subscribers
                .get(key)
                .map(|sessions| sessions.iter().copied().collect())
                .unwrap_or_default()
        };
        let disconnected = session_ids.len();
        self.metrics
            .reliable_disconnects
            .inc_by(u64::try_from(disconnected).unwrap_or(u64::MAX));
        for session_id in session_ids {
            self.remove_session(session_id, true);
        }
        disconnected
    }

    fn remove_session(&mut self, session_id: SessionId, cancel: bool) {
        let Some(record) = self.sessions.remove(&session_id) else {
            return;
        };
        if cancel {
            record.cancellation.cancel();
        }
        self.system_readers.remove(&session_id);
        remove_reverse_membership(&mut self.subject_sessions, &record.subject, session_id);
        remove_reverse_membership(&mut self.family_sessions, &record.family_id, session_id);
        for key in record.subscriptions {
            self.remove_subscription_indexes(session_id, &key);
        }
        self.update_session_count();
    }

    fn remove_subscription_indexes(&mut self, session_id: SessionId, key: &SubscriptionKey) {
        let remove_topic = self.topic_subscribers.get_mut(key).is_some_and(|sessions| {
            sessions.remove(&session_id);
            sessions.is_empty()
        });
        if remove_topic {
            self.topic_subscribers.remove(key);
        }
        self.decrement_watched_market(key);
    }

    fn increment_watched_market(&mut self, key: &SubscriptionKey) {
        if key.channel != WsChannel::MarketBookUpdate {
            return;
        }
        let Some(market_id) = &key.market else {
            return;
        };
        let count = self
            .watched_market_refcounts
            .entry(market_id.clone())
            .or_insert(0);
        *count = count.saturating_add(1);
        if *count == 1 {
            self.publish_watched_markets();
        }
    }

    fn decrement_watched_market(&mut self, key: &SubscriptionKey) {
        if key.channel != WsChannel::MarketBookUpdate {
            return;
        }
        let Some(market_id) = &key.market else {
            return;
        };
        let remove = self
            .watched_market_refcounts
            .get_mut(market_id)
            .is_some_and(|count| {
                *count = count.saturating_sub(1);
                *count == 0
            });
        if remove {
            self.watched_market_refcounts.remove(market_id);
            self.publish_watched_markets();
        }
    }

    fn publish_watched_markets(&self) {
        let snapshot = self
            .watched_market_refcounts
            .keys()
            .cloned()
            .collect::<AHashSet<_>>();
        self.watched_markets.store(Arc::new(snapshot));
    }

    fn close_all_sessions(&mut self) {
        for (_, record) in self.sessions.drain() {
            record.cancellation.cancel();
        }
        self.topic_subscribers.clear();
        self.subject_sessions.clear();
        self.family_sessions.clear();
        self.system_readers.clear();
        self.watched_market_refcounts.clear();
        self.watched_markets.store(Arc::new(AHashSet::new()));
        self.update_session_count();
    }

    fn update_session_count(&self) {
        self.session_count
            .store(self.sessions.len(), Ordering::Relaxed);
    }
}

fn remove_reverse_membership<K: Eq + Hash>(
    index: &mut AHashMap<K, AHashSet<SessionId>>,
    key: &K,
    session_id: SessionId,
) {
    let remove_key = index.get_mut(key).is_some_and(|sessions| {
        sessions.remove(&session_id);
        sessions.is_empty()
    });
    if remove_key {
        index.remove(key);
    }
}

/// Serialize each [`CoreEvent`] once and enqueue it for indexed hub dispatch.
pub async fn spawn_ws_broadcaster(
    rx: FlumeReceiver<CoreEvent>,
    registry: SessionRegistry,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                tracing::info!("ws broadcaster shutting down");
                return;
            }
            event = rx.recv_async() => {
                let Ok(event) = event else {
                    tracing::info!("ws broadcaster event bus closed");
                    return;
                };
                if let Some((key, envelope)) = event.event_envelope() {
                    let delivery = DeliveryClass::for_channel(key.channel);
                    let frame = ByteString::from(envelope.to_text());
                    if !registry.fanout(key, frame, delivery).await {
                        tracing::info!("ws session hub command queue closed");
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::Instant,
    };

    use bytestring::ByteString;
    use prometheus::{GaugeVec, Histogram, HistogramOpts, IntCounter, IntGauge, IntGaugeVec, Opts};
    use quant_pivot_models::{
        domain::ws::{SubscriptionKey, WsChannel},
        types::{MarketId, UserId},
    };
    use tokio::{
        sync::mpsc::{self, Receiver},
        time::timeout,
    };
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{
        CONTROL_TIMEOUT, DeliveryClass, HUB_MAX_FRAME_BYTES, HUB_RELIABLE_CAPACITY,
        SESSION_REPLAY_CAPACITY, SessionHub, SessionHubMetrics, SessionId, SessionRegistration,
        SessionRegistry, SharedFrame,
    };

    static METRIC_ID: AtomicU64 = AtomicU64::new(1);

    fn test_hub() -> (
        SessionRegistry,
        SessionHub,
        SessionHubMetrics,
        CancellationToken,
    ) {
        let id = METRIC_ID.fetch_add(1, Ordering::Relaxed);
        let metrics = SessionHubMetrics {
            best_effort_dropped: IntCounter::new(
                format!("test_ws_best_effort_dropped_{id}"),
                "test",
            )
            .expect("best-effort counter"),
            best_effort_coalesced: IntCounter::new(
                format!("test_ws_best_effort_coalesced_{id}"),
                "test",
            )
            .expect("coalesced counter"),
            reliable_disconnects: IntCounter::new(
                format!("test_ws_reliable_disconnects_{id}"),
                "test",
            )
            .expect("reliable counter"),
            control_timeouts: IntCounter::new(format!("test_ws_control_timeouts_{id}"), "test")
                .expect("timeout counter"),
            control_latency_seconds: Histogram::with_opts(HistogramOpts::new(
                format!("test_ws_control_latency_{id}"),
                "test",
            ))
            .expect("control latency"),
            queue_depth: IntGaugeVec::new(
                Opts::new(format!("test_ws_queue_depth_{id}"), "test"),
                &["lane"],
            )
            .expect("queue depth"),
            queue_oldest_age_seconds: GaugeVec::new(
                Opts::new(format!("test_ws_queue_oldest_age_{id}"), "test"),
                &["lane"],
            )
            .expect("queue age"),
            frame_bytes: IntGauge::new(format!("test_ws_frame_bytes_{id}"), "test")
                .expect("frame bytes"),
        };
        let (registry, hub) = SessionRegistry::new(metrics.clone());
        (registry, hub, metrics, CancellationToken::new())
    }

    fn registration(
        capacity: usize,
        subject: UserId,
        family_id: &str,
        can_read_system: bool,
    ) -> (
        SessionRegistration,
        Receiver<SharedFrame>,
        CancellationToken,
    ) {
        let (outbound, receiver) = mpsc::channel(capacity);
        let cancellation = CancellationToken::new();
        (
            SessionRegistration {
                outbound,
                subject,
                family_id: family_id.to_owned(),
                can_read_system,
                cancellation: cancellation.clone(),
            },
            receiver,
            cancellation,
        )
    }

    fn book(market: &str) -> SubscriptionKey {
        SubscriptionKey::scoped(WsChannel::MarketBookUpdate, MarketId::new(market))
    }

    fn user(id: u128) -> UserId {
        UserId::new(Uuid::from_u128(id))
    }

    #[test]
    fn bytestring_conversion_clone_allocation() {
        let encoded = String::from(r#"{"type":"market.book_update"}"#);
        let allocation = encoded.as_ptr();
        let frame = ByteString::from(encoded);
        let recipient = frame.clone();
        assert_eq!(frame.as_ptr(), allocation);
        assert_eq!(recipient.as_ptr(), frame.as_ptr());
    }

    #[test]
    fn feedback_delivery_is_reliable() {
        assert_eq!(
            DeliveryClass::for_channel(WsChannel::ResearchFeedback),
            DeliveryClass::Reliable
        );
    }

    #[tokio::test]
    async fn replay_targets_exact_session() {
        let (registry, hub, _metrics, shutdown) = test_hub();
        let task = tokio::spawn(hub.run(shutdown.clone()));
        let (first, mut first_rx, _) = registration(8, user(1), "family-1", false);
        let first_id = registry.register(first).await.expect("register first");
        let (second, mut second_rx, _) = registration(8, user(2), "family-2", false);
        registry.register(second).await.expect("register second");

        assert!(
            registry
                .replay(
                    first_id,
                    vec![
                        ByteString::from_static("revision-1"),
                        ByteString::from_static("revision-2"),
                    ],
                )
                .await
        );
        assert_eq!(first_rx.recv().await.as_deref(), Some("revision-1"));
        assert_eq!(first_rx.recv().await.as_deref(), Some("revision-2"));
        assert!(second_rx.try_recv().is_err());
        shutdown.cancel();
        task.await.expect("hub task");
    }

    #[tokio::test]
    async fn oversized_replay_disconnects() {
        let (registry, hub, metrics, shutdown) = test_hub();
        let task = tokio::spawn(hub.run(shutdown.clone()));
        let (registration, _receiver, cancellation) =
            registration(8, user(1), "replay-overflow", false);
        let session_id = registry.register(registration).await.expect("register");
        let frames = (0..=SESSION_REPLAY_CAPACITY)
            .map(|_| ByteString::from_static("revision"))
            .collect();

        assert!(!registry.replay(session_id, frames).await);
        assert!(cancellation.is_cancelled());
        assert_eq!(metrics.reliable_disconnects.get(), 0);
        assert_eq!(registry.session_count(), 0);
        shutdown.cancel();
        task.await.expect("hub task");
    }

    #[tokio::test]
    async fn topic_index_delivers_subscribers() {
        let (registry, hub, _metrics, shutdown) = test_hub();
        let task = tokio::spawn(hub.run(shutdown.clone()));
        let (first, mut first_rx, _) = registration(8, user(1), "family-1", false);
        let first_id = registry.register(first).await.expect("register first");
        assert!(registry.subscribe(first_id, book("0xaaa")).await);
        let (second, mut second_rx, _) = registration(8, user(2), "family-2", false);
        let second_id = registry.register(second).await.expect("register second");
        assert!(registry.subscribe(second_id, book("0xbbb")).await);

        assert!(
            registry
                .fanout(
                    book("0xaaa"),
                    ByteString::from_static("book-frame"),
                    DeliveryClass::BestEffort,
                )
                .await
        );

        assert_eq!(
            first_rx.recv().await.as_deref(),
            Some("book-frame"),
            "exact market subscriber receives the shared frame"
        );
        assert!(second_rx.try_recv().is_err());
        shutdown.cancel();
        task.await.expect("hub task");
    }

    #[tokio::test]
    async fn watched_market_tracks_subscription() {
        let (registry, hub, _metrics, shutdown) = test_hub();
        let task = tokio::spawn(hub.run(shutdown.clone()));
        let (first, _first_rx, _) = registration(8, user(1), "family-1", false);
        let first_id = registry.register(first).await.expect("register first");
        let (second, _second_rx, _) = registration(8, user(2), "family-2", false);
        let second_id = registry.register(second).await.expect("register second");
        assert!(registry.subscribe(first_id, book("0xaaa")).await);
        assert!(registry.subscribe(second_id, book("0xaaa")).await);
        registry.read_watched_markets(|markets| {
            assert_eq!(markets.len(), 1);
            assert!(markets.contains(&MarketId::new("0xaaa")));
        });

        assert!(registry.unsubscribe(first_id, book("0xaaa")).await);
        registry.read_watched_markets(|markets| assert_eq!(markets.len(), 1));
        assert!(registry.unsubscribe(second_id, book("0xaaa")).await);
        registry.read_watched_markets(|markets| assert!(markets.is_empty()));
        shutdown.cancel();
        task.await.expect("hub task");
    }

    #[tokio::test]
    async fn system_frames_without_subscription() {
        let (registry, hub, _metrics, shutdown) = test_hub();
        let task = tokio::spawn(hub.run(shutdown.clone()));
        let (reader, mut reader_rx, _) = registration(8, user(1), "family-1", true);
        registry.register(reader).await.expect("register reader");
        let (other, mut other_rx, _) = registration(8, user(2), "family-2", false);
        registry.register(other).await.expect("register other");

        assert!(
            registry
                .fanout(
                    SubscriptionKey::global(WsChannel::SystemStatus),
                    ByteString::from_static("status-frame"),
                    DeliveryClass::BestEffort,
                )
                .await
        );
        assert_eq!(reader_rx.recv().await.as_deref(), Some("status-frame"));
        assert!(other_rx.try_recv().is_err());
        shutdown.cancel();
        task.await.expect("hub task");
    }

    #[tokio::test]
    async fn subject_family_indexes_sessions() {
        let (registry, hub, _metrics, shutdown) = test_hub();
        let task = tokio::spawn(hub.run(shutdown.clone()));
        let (first, _first_rx, first_cancel) = registration(8, user(1), "family-1", false);
        registry.register(first).await.expect("register first");
        let (second, _second_rx, second_cancel) = registration(8, user(1), "family-2", false);
        registry.register(second).await.expect("register second");
        let (third, _third_rx, third_cancel) = registration(8, user(2), "family-3", false);
        registry.register(third).await.expect("register third");

        registry.close_family("family-1").await;
        assert!(first_cancel.is_cancelled());
        assert!(!second_cancel.is_cancelled());
        assert!(!third_cancel.is_cancelled());
        registry.close_subject(user(1)).await;
        assert!(second_cancel.is_cancelled());
        assert!(!third_cancel.is_cancelled());
        assert_eq!(registry.session_count(), 1);
        shutdown.cancel();
        task.await.expect("hub task");
    }

    #[tokio::test]
    async fn slow_client_drops_lifecycle() {
        let (registry, hub, metrics, shutdown) = test_hub();
        let task = tokio::spawn(hub.run(shutdown.clone()));
        let (registration, mut receiver, cancellation) = registration(1, user(1), "family", false);
        let session_id = registry.register(registration).await.expect("register");
        let key = book("0xaaa");
        assert!(registry.subscribe(session_id, key.clone()).await);
        assert!(
            registry
                .fanout(
                    key.clone(),
                    ByteString::from_static("state-1"),
                    DeliveryClass::BestEffort,
                )
                .await
        );
        timeout(CONTROL_TIMEOUT, async {
            while !registry.best_effort.state.lock().pending.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first best-effort dispatch");
        assert!(
            registry
                .fanout(
                    key.clone(),
                    ByteString::from_static("state-2"),
                    DeliveryClass::BestEffort,
                )
                .await
        );
        timeout(CONTROL_TIMEOUT, async {
            while metrics.best_effort_dropped.get() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("best-effort drop observed");
        assert_eq!(metrics.best_effort_dropped.get(), 1);
        assert!(!cancellation.is_cancelled());
        assert_eq!(receiver.recv().await.as_deref(), Some("state-1"));

        assert!(
            registry
                .fanout(
                    key.clone(),
                    ByteString::from_static("lifecycle-1"),
                    DeliveryClass::Reliable,
                )
                .await
        );
        timeout(CONTROL_TIMEOUT, async {
            while receiver.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first reliable dispatch");
        assert!(
            registry
                .fanout(
                    key,
                    ByteString::from_static("lifecycle-2"),
                    DeliveryClass::Reliable,
                )
                .await
        );
        timeout(CONTROL_TIMEOUT, cancellation.cancelled())
            .await
            .expect("reliable overflow disconnect");
        assert!(cancellation.is_cancelled());
        assert_eq!(metrics.reliable_disconnects.get(), 1);
        assert_eq!(registry.session_count(), 0);
        shutdown.cancel();
        task.await.expect("hub task");
    }

    #[tokio::test]
    async fn best_effort_keeps_topic() {
        let (registry, _hub, metrics, _shutdown) = test_hub();
        let key = book("0xlatest");
        assert!(
            registry
                .fanout(
                    key.clone(),
                    ByteString::from_static("state-1"),
                    DeliveryClass::BestEffort,
                )
                .await
        );
        assert!(
            registry
                .fanout(
                    key.clone(),
                    ByteString::from_static("state-2"),
                    DeliveryClass::BestEffort,
                )
                .await
        );

        let (pending_key, pending) = registry.best_effort.pop().expect("pending latest frame");
        assert_eq!(pending_key, key);
        assert_eq!(&pending.frame.text()[..], "state-2");
        assert_eq!(metrics.best_effort_coalesced.get(), 1);
        assert!(registry.best_effort.pop().is_none());
    }

    #[tokio::test]
    async fn missing_control_cancels_rejects() {
        let (registry, _hub, metrics, _shutdown) = test_hub();

        registry.close_all().await;

        assert!(registry.fail_closed_token().is_cancelled());
        assert_eq!(metrics.control_timeouts.get(), 1);
    }

    #[tokio::test]
    async fn session_id_exhaustion_rejects() {
        let (registry, _hub, _metrics, _shutdown) = test_hub();
        registry.next_id.store(u64::MAX, Ordering::Relaxed);
        let (registration, _receiver, _cancellation) =
            registration(1, user(1), "id-overflow", false);

        assert!(registry.register(registration).await.is_none());
        assert!(registry.fail_closed_token().is_cancelled());
    }

    #[tokio::test]
    async fn oversized_rejected_before_queueing() {
        let (registry, _hub, metrics, _shutdown) = test_hub();
        let oversized = ByteString::from("x".repeat(HUB_MAX_FRAME_BYTES + 1));

        assert!(
            registry
                .fanout(book("0xoversized"), oversized, DeliveryClass::BestEffort,)
                .await
        );

        assert_eq!(metrics.best_effort_dropped.get(), 1);
        assert_eq!(metrics.frame_bytes.get(), 0);
        assert!(registry.best_effort.pop().is_none());
    }

    #[tokio::test]
    async fn reliable_lane_overflow_topic() {
        let (registry, mut hub, metrics, shutdown) = test_hub();
        let key = book("0xreliable-overflow");
        let (registration, _receiver, cancellation) =
            registration(8, user(1), "overflow-family", false);
        let session_id = SessionId(1);
        assert!(hub.register(session_id, registration));
        assert!(hub.subscribe(session_id, key.clone()));
        for _ in 0..HUB_RELIABLE_CAPACITY {
            let frame = registry
                .charge_frame(ByteString::from_static("reliable"), DeliveryClass::Reliable)
                .await
                .expect("charged reliable frame");
            assert!(
                registry
                    .reliable
                    .try_send(super::ReliableFanout {
                        key: key.clone(),
                        frame,
                        enqueued_at: Instant::now(),
                    })
                    .is_ok()
            );
        }
        let overflow_registry = registry.clone();
        let overflow_key = key.clone();
        let overflow = tokio::spawn(async move {
            overflow_registry
                .fanout(
                    overflow_key,
                    ByteString::from_static("overflow"),
                    DeliveryClass::Reliable,
                )
                .await
        });
        tokio::task::yield_now().await;
        let task = tokio::spawn(hub.run(shutdown.clone()));

        assert!(overflow.await.expect("overflow task"));
        assert!(cancellation.is_cancelled());
        assert_eq!(metrics.reliable_disconnects.get(), 1);
        shutdown.cancel();
        task.await.expect("hub overflow task");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ten_thousand_cannot_revoke() {
        const SESSION_COUNT: usize = 10_000;
        const SUBSCRIBER_COUNT: usize = 1_000;
        const FLOOD_EVENTS: usize = 10_000;

        let (registry, hub, _metrics, shutdown) = test_hub();
        let task = tokio::spawn(hub.run(shutdown.clone()));
        let topic = book("0xflood");
        let target_subject = user(1);
        let mut receivers = Vec::with_capacity(SESSION_COUNT);
        let mut target_cancellation = None;
        for index in 0..SESSION_COUNT {
            let subject = user(u128::try_from(index).unwrap_or(u128::MAX) + 1);
            let (registration, receiver, cancellation) =
                registration(4, subject, &format!("family-{index}"), false);
            let session_id = registry
                .register(registration)
                .await
                .expect("register flood session");
            if index < SUBSCRIBER_COUNT {
                assert!(registry.subscribe(session_id, topic.clone()).await);
            }
            if index == 0 {
                target_cancellation = Some(cancellation);
            }
            receivers.push(receiver);
        }
        for _ in 0..FLOOD_EVENTS {
            assert!(
                registry
                    .fanout(
                        topic.clone(),
                        ByteString::from_static("flood"),
                        DeliveryClass::BestEffort,
                    )
                    .await
            );
        }

        let started_at = Instant::now();
        registry.close_subject(target_subject).await;
        let control_latency = started_at.elapsed();

        assert!(
            control_latency <= CONTROL_TIMEOUT,
            "revoke latency {control_latency:?} exceeded {CONTROL_TIMEOUT:?}"
        );
        assert!(
            target_cancellation
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
        );
        assert!(!registry.fail_closed_token().is_cancelled());
        shutdown.cancel();
        task.await.expect("hub flood task");
        drop(receivers);
    }
}
