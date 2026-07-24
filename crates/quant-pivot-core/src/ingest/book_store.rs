use std::{
    ops::Deref,
    slice,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use num_traits::ToPrimitive;
use quant_pivot_models::{
    domain::{
        data_plane::{latency::LatencyTrace, pipeline::StreamSessionTicket},
        market::book::BookSnapshot,
    },
    types::{TokenId, TokenKey},
};
use tokio::sync::Notify;

use crate::{
    ingest::{
        data_plane_index::{DataPlane, DataPlaneIndex, TokenFreshness, TokenSlot, TokenSlotState},
        session_directory::SessionDirectory,
    },
    observability::metrics_hub::MetricsHub,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookUnavailable {
    UnknownToken,
    Unseen,
    Invalid,
    Retired,
    PoisonedSession,
}

/// Owned, semantically fresh snapshot safe to carry across an async/task
/// boundary. It can only be constructed after coherent slot and session checks.
pub struct FreshBook {
    snapshot: Arc<BookSnapshot>,
    freshness: TokenFreshness,
}

impl FreshBook {
    #[must_use]
    pub const fn freshness(&self) -> TokenFreshness {
        self.freshness
    }

    #[must_use]
    pub fn into_snapshot(self) -> Arc<BookSnapshot> {
        self.snapshot
    }
}

impl Deref for FreshBook {
    type Target = BookSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.snapshot
    }
}

/// Diagnostic-only last-known state. This type deliberately does not deref to
/// `BookSnapshot`, so it cannot satisfy execution/report market-data ports.
pub struct LastKnownBook {
    pub snapshot: Option<Arc<BookSnapshot>>,
    pub freshness: Option<TokenFreshness>,
    pub availability: Result<(), BookUnavailable>,
}

/// Read facade over stable [`TokenKey`] slots owned by the shared data plane.
pub struct BookStore {
    data_plane: Arc<DataPlane>,
    metrics: Arc<MetricsHub>,
    metric_update_counter: AtomicU64,
    gap_generation: AtomicU64,
    update_notify: Notify,
    sessions: Arc<SessionDirectory>,
}

impl BookStore {
    pub fn new(data_plane: Arc<DataPlane>, metrics: Arc<MetricsHub>) -> Self {
        Self {
            data_plane,
            metrics,
            metric_update_counter: AtomicU64::new(0),
            gap_generation: AtomicU64::new(0),
            update_notify: Notify::new(),
            sessions: Arc::new(SessionDirectory::default()),
        }
    }

    #[must_use]
    pub fn session_directory(&self) -> Arc<SessionDirectory> {
        Arc::clone(&self.sessions)
    }

    fn update_token_count_metric(&self) {
        let n = self.metric_update_counter.fetch_add(1, Ordering::Relaxed);
        if n.is_multiple_of(1024) {
            self.metrics
                .book_store_token_count
                .set(ToPrimitive::to_i64(&self.token_count()).unwrap_or(i64::MAX));
        }
    }

    #[must_use]
    #[inline]
    pub fn resolve(&self, token_id: &TokenId) -> Option<TokenKey> {
        self.data_plane.token_key(token_id)
    }

    /// Borrow a coherent fresh snapshot without incrementing its `Arc` refcount.
    ///
    /// The callback must remain synchronous and must never retain the reference
    /// across an `.await`. It must also be free of external side effects because
    /// a concurrent slot transition can require one retry before returning.
    #[inline]
    pub fn read_fresh<R>(
        &self,
        token: TokenKey,
        mut read: impl FnMut(&BookSnapshot, TokenFreshness) -> R,
    ) -> Result<R, BookUnavailable> {
        self.data_plane
            .with_slot(token, |slot| {
                loop {
                    let (snapshot, freshness, version) = slot.snapshot_with_freshness();
                    self.ensure_fresh(freshness)?;
                    let output = read(&snapshot, freshness);
                    if slot.coherent_version_is(version)
                        && self.sessions.is_epoch_active(freshness.session_generation)
                    {
                        return Ok(output);
                    }
                }
            })
            .ok_or(BookUnavailable::UnknownToken)?
    }

    /// Load an owned fresh snapshot for transfer across task or await boundaries.
    #[inline]
    pub fn load_fresh_owned(&self, token: TokenKey) -> Result<FreshBook, BookUnavailable> {
        self.data_plane
            .with_slot(token, |slot| {
                loop {
                    let (snapshot, freshness, version) = slot.snapshot_with_freshness();
                    self.ensure_fresh(freshness)?;
                    let snapshot = Arc::clone(&snapshot);
                    if slot.coherent_version_is(version)
                        && self.sessions.is_epoch_active(freshness.session_generation)
                    {
                        return Ok(FreshBook {
                            snapshot,
                            freshness,
                        });
                    }
                }
            })
            .ok_or(BookUnavailable::UnknownToken)?
    }

    /// Resolve a wire/catalog token at the boundary and return an owned fresh snapshot.
    pub fn load_fresh_by_id(&self, token_id: &TokenId) -> Result<FreshBook, BookUnavailable> {
        let token = self
            .resolve(token_id)
            .ok_or(BookUnavailable::UnknownToken)?;
        self.load_fresh_owned(token)
    }

    /// Load diagnostic last-known state without granting semantic freshness.
    #[must_use]
    pub fn load_last_known(&self, token: TokenKey) -> LastKnownBook {
        self.data_plane
            .with_slot(token, |slot| {
                let (snapshot, freshness, _) = slot.snapshot_with_freshness();
                LastKnownBook {
                    snapshot: Some(Arc::clone(&snapshot)),
                    freshness: Some(freshness),
                    availability: self.ensure_fresh(freshness),
                }
            })
            .unwrap_or(LastKnownBook {
                snapshot: None,
                freshness: None,
                availability: Err(BookUnavailable::UnknownToken),
            })
    }

    #[must_use]
    pub fn load_known_book(&self, token_id: &TokenId) -> LastKnownBook {
        self.resolve(token_id).map_or(
            LastKnownBook {
                snapshot: None,
                freshness: None,
                availability: Err(BookUnavailable::UnknownToken),
            },
            |token| self.load_last_known(token),
        )
    }

    #[inline]
    pub(crate) fn last_known_version(&self, token: TokenKey) -> u64 {
        self.load_last_known(token)
            .snapshot
            .map_or(0, |snapshot| snapshot.version)
    }

    fn ensure_fresh(&self, freshness: TokenFreshness) -> Result<(), BookUnavailable> {
        match freshness.state {
            TokenSlotState::Unseen => Err(BookUnavailable::Unseen),
            TokenSlotState::Retired => Err(BookUnavailable::Retired),
            TokenSlotState::Invalid => {
                if freshness.session_generation != 0
                    && !self.sessions.is_epoch_active(freshness.session_generation)
                {
                    Err(BookUnavailable::PoisonedSession)
                } else {
                    Err(BookUnavailable::Invalid)
                }
            }
            TokenSlotState::Fresh => {
                if self.sessions.is_epoch_active(freshness.session_generation) {
                    Ok(())
                } else {
                    Err(BookUnavailable::PoisonedSession)
                }
            }
        }
    }

    /// Source-discontinuity generation shared by all CLOB token books.
    #[inline]
    pub fn gap_generation(&self) -> u64 {
        self.gap_generation.load(Ordering::Acquire)
    }

    /// Mark an observed CLOB stream discontinuity. Price-condition continuity
    /// must restart even when the recovered best ask is numerically unchanged.
    pub fn mark_gap(&self) {
        self.gap_generation.fetch_add(1, Ordering::AcqRel);
        self.update_notify.notify_one();
    }

    /// Coalesced wake for the durable condition worker; Postgres remains the queue.
    pub async fn wait_for_update(&self) {
        self.update_notify.notified().await;
    }

    /// Publish a snapshot built by the token-affine partition actor.
    ///
    /// This is the only writer entry point. The store never owns or locks a
    /// mutable order book; it atomically publishes immutable state and then
    /// commits the matching freshness tuple under the slot version protocol.
    fn publish_snapshot(
        &self,
        token: TokenKey,
        snapshot: BookSnapshot,
        sequence: u64,
        session_generation: u64,
        latency: Option<LatencyTrace>,
    ) -> bool {
        let applied_tick = self.data_plane.now_tick();
        let latency_tick = latency.map_or(0, |_| applied_tick);
        let published = self
            .data_plane
            .with_slot(token, |slot| {
                slot.publish_snapshot(
                    Arc::new(snapshot),
                    sequence,
                    session_generation,
                    applied_tick,
                    latency_tick,
                )
            })
            .unwrap_or(false);
        if !published {
            return false;
        }
        self.update_token_count_metric();
        self.update_notify.notify_one();
        true
    }

    /// Publish only while the complete physical stream ticket remains active.
    /// A concurrent poison after the slot write immediately invalidates the
    /// publication; semantic readers additionally fence on the same directory.
    pub fn publish_snapshot_session(
        &self,
        token: TokenKey,
        snapshot: BookSnapshot,
        sequence: u64,
        session: StreamSessionTicket,
        latency: Option<LatencyTrace>,
    ) -> bool {
        if !self.sessions.is_active(session) {
            return false;
        }
        if !self.publish_snapshot(token, snapshot, sequence, session.epoch, latency) {
            return false;
        }
        if self.sessions.is_active(session) {
            true
        } else {
            self.invalidate_tokens(slice::from_ref(&token));
            false
        }
    }

    /// Publish a delta-derived snapshot only when the slot is already Fresh
    /// under this exact physical stream epoch.
    pub fn publish_update_session(
        &self,
        token: TokenKey,
        snapshot: BookSnapshot,
        sequence: u64,
        session: StreamSessionTicket,
        latency: Option<LatencyTrace>,
    ) -> bool {
        if !self.sessions.is_active(session) {
            return false;
        }
        let applied_tick = self.data_plane.now_tick();
        let latency_tick = latency.map_or(0, |_| applied_tick);
        let published = self
            .data_plane
            .with_slot(token, |slot| {
                slot.publish_update(
                    Arc::new(snapshot),
                    sequence,
                    session.epoch,
                    applied_tick,
                    latency_tick,
                )
            })
            .unwrap_or(false);
        if !published {
            return false;
        }
        self.update_notify.notify_one();
        if self.sessions.is_active(session) {
            true
        } else {
            self.invalidate_tokens(slice::from_ref(&token));
            false
        }
    }

    #[must_use]
    #[inline]
    pub(crate) fn freshness(&self, token: TokenKey) -> Option<TokenFreshness> {
        self.data_plane.with_slot(token, TokenSlot::freshness)
    }

    #[must_use]
    pub(crate) fn freshness_age_ms(&self, token: TokenKey) -> Option<u64> {
        let freshness = self.freshness(token)?;
        if freshness.state != TokenSlotState::Fresh || freshness.freshness_tick == 0 {
            return None;
        }
        Some(
            self.data_plane
                .now_tick()
                .saturating_sub(freshness.freshness_tick),
        )
    }

    /// Publish canonical stream provenance while preserving the apply latency tick.
    fn mark_canonical_fresh(
        &self,
        token: TokenKey,
        sequence: u64,
        session_generation: u64,
    ) -> bool {
        self.data_plane
            .with_slot(token, |slot| {
                let current = slot.freshness();
                slot.mark_fresh(
                    sequence,
                    session_generation,
                    self.data_plane.now_tick(),
                    current.latency_tick,
                )
            })
            .unwrap_or(false)
    }

    pub(crate) fn mark_canonical_fresh_session(
        &self,
        token: TokenKey,
        sequence: u64,
        session: StreamSessionTicket,
    ) -> bool {
        if !self.sessions.is_active(session) {
            return false;
        }
        if !self.mark_canonical_fresh(token, sequence, session.epoch) {
            return false;
        }
        if self.sessions.is_active(session) {
            true
        } else {
            self.invalidate_tokens(slice::from_ref(&token));
            false
        }
    }

    /// Invalidate process-local token keys without boundary lookup or allocation.
    pub(crate) fn invalidate_tokens(&self, tokens: &[TokenKey]) -> usize {
        self.data_plane.with_index(|index| {
            let mut invalidated = 0;
            for token in tokens {
                let Some(slot) = index.token_slot(*token) else {
                    continue;
                };
                slot.invalidate();
                invalidated += 1;
            }
            invalidated
        })
    }

    /// Invalidate a complete transport session scope without allocating keys.
    pub fn invalidate_ids(&self, token_ids: &[TokenId]) -> usize {
        self.data_plane.with_index(|index| {
            let mut invalidated = 0;
            for token_id in token_ids {
                let Some(token) = index.token_key(token_id) else {
                    continue;
                };
                let Some(slot) = index.token_slot(token) else {
                    continue;
                };
                slot.invalidate();
                invalidated += 1;
            }
            invalidated
        })
    }

    pub fn token_count(&self) -> usize {
        self.data_plane.with_index(DataPlaneIndex::token_count)
    }

    /// Bind a newly opened physical session and make every scoped token
    /// unavailable until its complete snapshot is durably published.
    pub(crate) fn begin_session(
        &self,
        session: StreamSessionTicket,
        token_ids: &[TokenId],
    ) -> usize {
        self.data_plane.with_index(|index| {
            token_ids
                .iter()
                .filter_map(|token_id| index.token_key(token_id))
                .filter(|token| {
                    index
                        .token_slot(*token)
                        .is_some_and(|slot| slot.begin_session(session.epoch))
                })
                .count()
        })
    }

    /// Release the slot's large Arc sides and mark it Retired unless a newer
    /// physical session has already taken ownership.
    pub(crate) fn retire_token(&self, token: TokenKey, through_epoch: u64) -> bool {
        self.data_plane
            .with_slot(token, |slot| slot.retire(through_epoch))
            .unwrap_or(false)
    }

    pub fn diagnostic_books(&self) -> Vec<(TokenKey, TokenId, LastKnownBook)> {
        self.data_plane.with_index(|index| {
            (0..index.token_count())
                .filter_map(|raw| {
                    let token = TokenKey::new(u32::try_from(raw).ok()?);
                    let metadata = index.token_metadata(token)?;
                    Some((
                        token,
                        metadata.token_id.clone(),
                        self.load_last_known(token),
                    ))
                })
                .collect()
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        slice,
        sync::{Arc, Barrier},
        thread,
    };

    use loom::{
        model,
        sync::{
            Arc as LoomArc,
            atomic::{
                AtomicBool as LoomAtomicBool, AtomicU8 as LoomAtomicU8, Ordering as LoomOrdering,
            },
        },
        thread as loom_thread,
    };
    use quant_pivot_models::{
        domain::market::BookLevel,
        types::{Price, Shares},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use super::*;
    fn make_level(price: Decimal, size: Decimal) -> BookLevel {
        BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(size))
    }

    fn empty_snapshot(version: u64) -> BookSnapshot {
        BookSnapshot::new(Arc::from([]), Arc::from([]), version, version)
    }

    fn publish(
        store: &BookStore,
        token: TokenKey,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        version: u64,
    ) {
        let token_id = store
            .data_plane
            .with_index(|index| {
                index
                    .token_metadata(token)
                    .map(|metadata| metadata.token_id.clone())
            })
            .expect("registered token metadata");
        let epoch = u64::try_from(token.index()).expect("token index fits") + 1;
        let session = StreamSessionTicket::new(Uuid::from_u128(u128::from(epoch)), epoch)
            .expect("valid session ticket");
        assert!(store.sessions.open(session, Arc::from([token_id])));
        assert!(store.publish_snapshot_session(
            token,
            BookSnapshot::new(Arc::from(bids), Arc::from(asks), version, version),
            version,
            session,
            None,
        ));
    }

    #[test]
    fn snapshot_creates_and_updates() {
        let metrics = Arc::new(MetricsHub::new());
        let tid = TokenId::new("tok-1");
        let data_plane = Arc::new(DataPlane::new());
        data_plane.register_test_tokens(slice::from_ref(&tid));
        let store = BookStore::new(data_plane, metrics);
        let token = store.resolve(&tid).expect("registered token");
        publish(
            &store,
            token,
            vec![make_level(dec!(0.5), dec!(10))],
            vec![make_level(dec!(0.6), dec!(5))],
            1,
        );
        assert_eq!(store.token_count(), 1);

        let snap = store.load_fresh_owned(token).expect("fresh snapshot");
        assert_eq!(snap.best_bid().unwrap().inner(), dec!(0.5));
        assert_eq!(snap.best_ask().unwrap().inner(), dec!(0.6));
        assert_eq!(snap.version, 1);
    }

    #[test]
    fn poisoned_ticket_cannot_recovers() {
        let metrics = Arc::new(MetricsHub::new());
        let token_id = TokenId::new("tok-session");
        let data_plane = Arc::new(DataPlane::new());
        data_plane.register_test_tokens(slice::from_ref(&token_id));
        let store = BookStore::new(data_plane, metrics);
        let token = store.resolve(&token_id).expect("registered token");
        let old_session =
            StreamSessionTicket::new(Uuid::from_u128(1), 1).expect("valid old session ticket");
        assert!(
            store
                .sessions
                .open(old_session, Arc::from([token_id.clone()]))
        );
        assert!(store.publish_snapshot_session(token, empty_snapshot(1), 1, old_session, None));

        assert!(store.sessions.poison(old_session).is_some());
        store.invalidate_ids(slice::from_ref(&token_id));
        assert!(!store.publish_snapshot_session(token, empty_snapshot(2), 2, old_session, None));
        assert_eq!(
            store.freshness(token).map(|freshness| freshness.state),
            Some(TokenSlotState::Invalid)
        );

        let new_session =
            StreamSessionTicket::new(Uuid::from_u128(2), 2).expect("valid new session ticket");
        assert!(store.sessions.open(new_session, Arc::from([token_id])));
        assert!(store.publish_snapshot_session(token, empty_snapshot(3), 1, new_session, None));
        assert_eq!(
            store.freshness(token).map(|freshness| freshness.state),
            Some(TokenSlotState::Fresh)
        );
    }

    #[test]
    fn retirement_releases_requires_session() {
        let token_id = TokenId::new("tok-retire");
        let data_plane = Arc::new(DataPlane::new());
        data_plane.register_test_tokens(slice::from_ref(&token_id));
        let store = BookStore::new(data_plane, Arc::new(MetricsHub::new()));
        let token = store.resolve(&token_id).expect("registered token");
        let old_session = StreamSessionTicket::new(Uuid::from_u128(10), 10).expect("old session");
        assert!(
            store
                .sessions
                .open(old_session, Arc::from([token_id.clone()]))
        );
        assert_eq!(
            store.begin_session(old_session, slice::from_ref(&token_id)),
            1
        );
        assert!(store.publish_snapshot_session(token, empty_snapshot(1), 1, old_session, None));

        assert!(store.retire_token(token, old_session.epoch));
        let retired = store.load_last_known(token);
        assert_eq!(retired.availability, Err(BookUnavailable::Retired));
        let retired_snapshot = retired.snapshot.expect("retired diagnostic snapshot");
        assert!(retired_snapshot.bids.is_empty());
        assert!(retired_snapshot.asks.is_empty());
        assert!(!store.publish_snapshot_session(token, empty_snapshot(2), 2, old_session, None));

        let new_session = StreamSessionTicket::new(Uuid::from_u128(11), 11).expect("new session");
        assert!(
            store
                .sessions
                .open(new_session, Arc::from([token_id.clone()]))
        );
        assert_eq!(
            store.begin_session(new_session, slice::from_ref(&token_id)),
            1
        );
        assert!(store.publish_snapshot_session(token, empty_snapshot(3), 1, new_session, None));
        assert!(store.load_fresh_owned(token).is_ok());
        assert!(!store.retire_token(token, old_session.epoch));
        assert!(store.load_fresh_owned(token).is_ok());
    }

    #[test]
    fn concurrent_poison_cannot_book() {
        let token_id = TokenId::new("tok-race");
        let data_plane = Arc::new(DataPlane::new());
        data_plane.register_test_tokens(slice::from_ref(&token_id));
        let store = Arc::new(BookStore::new(data_plane, Arc::new(MetricsHub::new())));
        let token = store.resolve(&token_id).expect("registered token");
        let session =
            StreamSessionTicket::new(Uuid::from_u128(9), 9).expect("valid session ticket");
        assert!(store.sessions.open(session, Arc::from([token_id.clone()])));
        assert_eq!(store.begin_session(session, slice::from_ref(&token_id)), 1);
        let start = Arc::new(Barrier::new(3));
        let publisher = {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                store.publish_snapshot_session(token, empty_snapshot(1), 1, session, None)
            })
        };
        let poisoner = {
            let store = Arc::clone(&store);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                store.sessions.poison(session);
                store.invalidate_ids(slice::from_ref(&token_id));
            })
        };
        start.wait();
        let _ = publisher.join().expect("publisher thread");
        poisoner.join().expect("poisoner thread");

        assert!(matches!(
            store.load_fresh_owned(token),
            Err(BookUnavailable::PoisonedSession)
        ));
    }

    #[test]
    fn loom_publish_never_fresh() {
        model(|| {
            const INVALID: u8 = 0;
            const FRESH: u8 = 1;
            let active = LoomArc::new(LoomAtomicBool::new(true));
            let state = LoomArc::new(LoomAtomicU8::new(INVALID));
            let publisher = {
                let active = LoomArc::clone(&active);
                let state = LoomArc::clone(&state);
                loom_thread::spawn(move || {
                    if active.load(LoomOrdering::SeqCst) {
                        state.store(FRESH, LoomOrdering::SeqCst);
                        if !active.load(LoomOrdering::SeqCst) {
                            state.store(INVALID, LoomOrdering::SeqCst);
                        }
                    }
                })
            };
            let poisoner = {
                let active = LoomArc::clone(&active);
                let state = LoomArc::clone(&state);
                loom_thread::spawn(move || {
                    active.store(false, LoomOrdering::SeqCst);
                    state.store(INVALID, LoomOrdering::SeqCst);
                })
            };
            publisher.join().expect("loom publisher");
            poisoner.join().expect("loom poisoner");
            let semantically_fresh =
                active.load(LoomOrdering::SeqCst) && state.load(LoomOrdering::SeqCst) == FRESH;
            assert!(!semantically_fresh);
        });
    }

    #[test]
    fn independently_loaded_keeps_sides() {
        let metrics = Arc::new(MetricsHub::new());
        let yes = TokenId::new("yes");
        let no = TokenId::new("no");
        let data_plane = Arc::new(DataPlane::new());
        data_plane.register_test_tokens(&[yes.clone(), no.clone()]);
        let store = BookStore::new(data_plane, metrics);
        let yes_key = store.resolve(&yes).expect("registered YES token");
        let no_key = store.resolve(&no).expect("registered NO token");
        publish(
            &store,
            yes_key,
            vec![make_level(dec!(0.95), dec!(10))],
            vec![],
            1,
        );
        publish(
            &store,
            no_key,
            vec![],
            vec![make_level(dec!(0.05), dec!(10))],
            1,
        );
        let yes_book = store.load_fresh_owned(yes_key).expect("fresh YES snapshot");
        let no_book = store.load_fresh_owned(no_key).expect("fresh NO snapshot");
        assert_eq!(yes_book.bids.len(), 1);
        assert_eq!(no_book.asks.len(), 1);
    }

    #[test]
    fn publish_replaces_version() {
        let metrics = Arc::new(MetricsHub::new());
        let tid = TokenId::new("t");
        let data_plane = Arc::new(DataPlane::new());
        data_plane.register_test_tokens(slice::from_ref(&tid));
        let store = BookStore::new(data_plane, metrics);
        let token = store.resolve(&tid).expect("registered token");
        publish(
            &store,
            token,
            vec![make_level(dec!(0.5), dec!(1))],
            vec![],
            1,
        );
        publish(
            &store,
            token,
            vec![make_level(dec!(0.55), dec!(2))],
            vec![],
            2,
        );
        assert_eq!(
            store
                .load_fresh_owned(token)
                .expect("fresh snapshot")
                .version,
            2
        );
    }
}
