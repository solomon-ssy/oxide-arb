//! WebSocket real-time push infrastructure.
//!
//! [`SessionHub`] is the only writer for session lifecycle, subscriptions and
//! fan-out indexes. Connection tasks communicate through a bounded command
//! queue; the event broadcaster serializes each envelope once into a shared
//! [`ByteString`] before dispatch.

pub mod handler;
pub mod session;

use std::{
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use ahash::{AHashMap, AHashSet};
use arc_swap::ArcSwap;
use bytestring::ByteString;
use flume::Receiver as FlumeReceiver;
use prometheus::IntCounter;
use quant_pivot_models::{
    domain::{
        runtime::CoreEvent,
        ws::{SubscriptionKey, WsChannel, event_envelope},
    },
    types::{MarketId, UserId},
};
use smallvec::SmallVec;
use tokio::sync::{
    mpsc::{self, Receiver, Sender, error::TrySendError},
    oneshot::{self, Sender as OneshotSender},
};
use tokio_util::sync::CancellationToken;

const HUB_COMMAND_CAPACITY: usize = 16_384;

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
            | WsChannel::QuantCondition
            | WsChannel::MaterializationRunUpdate
            | WsChannel::QuantReconciliation
            | WsChannel::QuantSettlement => Self::Reliable,
        }
    }
}

/// Immutable registration payload transferred from one connection task.
pub struct SessionRegistration {
    pub outbound: Sender<ByteString>,
    pub subject: UserId,
    pub family_id: String,
    pub can_read_system: bool,
    pub cancellation: CancellationToken,
}

/// Mutable state owned exclusively by [`SessionHub`].
pub struct SessionRecord {
    pub outbound: Sender<ByteString>,
    pub cancellation: CancellationToken,
    subject: UserId,
    family_id: String,
    subscriptions: AHashSet<SubscriptionKey>,
}

/// Prometheus counters updated by the single hub writer.
#[derive(Clone)]
pub struct SessionHubMetrics {
    pub best_effort_dropped: IntCounter,
    pub reliable_disconnects: IntCounter,
}

/// Commands accepted by the single-writer hub.
pub enum SessionHubCommand {
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
    Fanout {
        key: SubscriptionKey,
        frame: ByteString,
        delivery: DeliveryClass,
    },
}

/// Cloneable command/read handle shared by HTTP and connection tasks.
#[derive(Clone)]
pub struct SessionRegistry {
    commands: Sender<SessionHubCommand>,
    next_id: Arc<AtomicU64>,
    session_count: Arc<AtomicUsize>,
    watched_markets: Arc<ArcSwap<AHashSet<MarketId>>>,
}

impl SessionRegistry {
    /// Create the shared handle and its unique single-writer actor.
    #[must_use]
    pub fn new(metrics: SessionHubMetrics) -> (Self, SessionHub) {
        let (commands, receiver) = mpsc::channel(HUB_COMMAND_CAPACITY);
        let session_count = Arc::new(AtomicUsize::new(0));
        let watched_markets = Arc::new(ArcSwap::from_pointee(AHashSet::new()));
        (
            Self {
                commands,
                next_id: Arc::new(AtomicU64::new(1)),
                session_count: Arc::clone(&session_count),
                watched_markets: Arc::clone(&watched_markets),
            },
            SessionHub::new(receiver, session_count, watched_markets, metrics),
        )
    }

    /// Register a session only after the actor has installed all reverse indexes.
    pub async fn register(&self, registration: SessionRegistration) -> Option<SessionId> {
        let session_id = SessionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.confirm(|completion| SessionHubCommand::Register {
            session_id,
            registration,
            completion,
        })
        .await
        .then_some(session_id)
    }

    /// Remove one disconnected session and all of its topic memberships.
    pub async fn deregister(&self, session_id: SessionId) {
        self.confirm(|completion| SessionHubCommand::Deregister {
            session_id,
            completion,
        })
        .await;
    }

    /// Add one exact channel/scope subscription.
    pub async fn subscribe(&self, session_id: SessionId, key: SubscriptionKey) -> bool {
        self.confirm(|completion| SessionHubCommand::Subscribe {
            session_id,
            key,
            completion,
        })
        .await
    }

    /// Remove one exact channel/scope subscription.
    pub async fn unsubscribe(&self, session_id: SessionId, key: SubscriptionKey) -> bool {
        self.confirm(|completion| SessionHubCommand::Unsubscribe {
            session_id,
            key,
            completion,
        })
        .await
    }

    /// Close every live socket owned by one user.
    pub async fn close_subject(&self, subject: UserId) {
        self.confirm(|completion| SessionHubCommand::CloseSubject {
            subject,
            completion,
        })
        .await;
    }

    /// Close every live socket issued from one refresh-session family.
    pub async fn close_family(&self, family_id: &str) {
        self.confirm(|completion| SessionHubCommand::CloseFamily {
            family_id: family_id.to_owned(),
            completion,
        })
        .await;
    }

    /// Close all sockets after a global RBAC policy revision.
    pub async fn close_all(&self) {
        self.confirm(|completion| SessionHubCommand::CloseAll { completion })
            .await;
    }

    /// Queue one already-serialized frame for indexed fan-out.
    pub async fn fanout(
        &self,
        key: SubscriptionKey,
        frame: ByteString,
        delivery: DeliveryClass,
    ) -> bool {
        self.commands
            .send(SessionHubCommand::Fanout {
                key,
                frame,
                delivery,
            })
            .await
            .is_ok()
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

    async fn confirm(
        &self,
        command: impl FnOnce(OneshotSender<bool>) -> SessionHubCommand,
    ) -> bool {
        let (completion, applied) = oneshot::channel();
        if self.commands.send(command(completion)).await.is_err() {
            return false;
        }
        applied.await.unwrap_or(false)
    }
}

/// Single writer for every WebSocket session and reverse index.
pub struct SessionHub {
    commands: Receiver<SessionHubCommand>,
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
        commands: Receiver<SessionHubCommand>,
        session_count: Arc<AtomicUsize>,
        watched_markets: Arc<ArcSwap<AHashSet<MarketId>>>,
        metrics: SessionHubMetrics,
    ) -> Self {
        Self {
            commands,
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
                () = shutdown.cancelled() => {
                    self.close_all_sessions();
                    tracing::info!("ws session hub shutting down");
                    return;
                }
                command = self.commands.recv() => {
                    let Some(command) = command else {
                        self.close_all_sessions();
                        tracing::info!("ws session hub command queue closed");
                        return;
                    };
                    self.apply(command);
                }
            }
        }
    }

    fn apply(&mut self, command: SessionHubCommand) {
        match command {
            SessionHubCommand::Register {
                session_id,
                registration,
                completion,
            } => {
                let applied = self.register(session_id, registration);
                let _ = completion.send(applied);
            }
            SessionHubCommand::Deregister {
                session_id,
                completion,
            } => {
                self.remove_session(session_id, false);
                let _ = completion.send(true);
            }
            SessionHubCommand::Subscribe {
                session_id,
                key,
                completion,
            } => {
                let applied = self.subscribe(session_id, key);
                let _ = completion.send(applied);
            }
            SessionHubCommand::Unsubscribe {
                session_id,
                key,
                completion,
            } => {
                let applied = self.unsubscribe(session_id, &key);
                let _ = completion.send(applied);
            }
            SessionHubCommand::CloseSubject {
                subject,
                completion,
            } => {
                self.close_subject(subject);
                let _ = completion.send(true);
            }
            SessionHubCommand::CloseFamily {
                family_id,
                completion,
            } => {
                self.close_family(&family_id);
                let _ = completion.send(true);
            }
            SessionHubCommand::CloseAll { completion } => {
                self.close_all_sessions();
                let _ = completion.send(true);
            }
            SessionHubCommand::Fanout {
                key,
                frame,
                delivery,
            } => self.fanout(&key, &frame, delivery),
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

    fn fanout(&mut self, key: &SubscriptionKey, frame: &ByteString, delivery: DeliveryClass) {
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
                if let Some((key, envelope)) = event_envelope(&event) {
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
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytestring::ByteString;
    use prometheus::IntCounter;
    use quant_pivot_models::{
        domain::ws::{SubscriptionKey, WsChannel},
        types::{MarketId, UserId},
    };
    use tokio::sync::mpsc::{self, Receiver};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{
        DeliveryClass, SessionHub, SessionHubMetrics, SessionRegistration, SessionRegistry,
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
            reliable_disconnects: IntCounter::new(
                format!("test_ws_reliable_disconnects_{id}"),
                "test",
            )
            .expect("reliable counter"),
        };
        let (registry, hub) = SessionRegistry::new(metrics.clone());
        (registry, hub, metrics, CancellationToken::new())
    }

    fn registration(
        capacity: usize,
        subject: UserId,
        family_id: &str,
        can_read_system: bool,
    ) -> (SessionRegistration, Receiver<ByteString>, CancellationToken) {
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
    fn bytestring_conversion_and_clone_share_the_encoded_allocation() {
        let encoded = String::from(r#"{"type":"market.book_update"}"#);
        let allocation = encoded.as_ptr();
        let frame = ByteString::from(encoded);
        let recipient = frame.clone();
        assert_eq!(frame.as_ptr(), allocation);
        assert_eq!(recipient.as_ptr(), frame.as_ptr());
    }

    #[tokio::test]
    async fn topic_index_delivers_only_to_exact_subscribers() {
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
    async fn watched_market_snapshot_tracks_first_and_last_subscription() {
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
    async fn system_frames_use_permission_index_without_explicit_subscription() {
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
    async fn subject_and_family_indexes_close_only_matching_sessions() {
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
    async fn slow_client_policy_drops_state_but_closes_on_lifecycle() {
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
        assert!(
            registry
                .fanout(
                    key.clone(),
                    ByteString::from_static("state-2"),
                    DeliveryClass::BestEffort,
                )
                .await
        );
        assert!(registry.subscribe(session_id, key.clone()).await, "barrier");
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
        assert!(
            registry
                .fanout(
                    key,
                    ByteString::from_static("lifecycle-2"),
                    DeliveryClass::Reliable,
                )
                .await
        );
        registry.close_family("unrelated").await;
        assert!(cancellation.is_cancelled());
        assert_eq!(metrics.reliable_disconnects.get(), 1);
        assert_eq!(registry.session_count(), 0);
        shutdown.cancel();
        task.await.expect("hub task");
    }
}
