use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use num_traits::ToPrimitive;
use quant_pivot_models::{
    domain::{
        data_plane::latency::LatencyTrace,
        market::book::{BinaryBookPair, BookSnapshot, TopOfBook},
    },
    types::{MarketId, TokenId, TokenKey},
};
use tokio::sync::Notify;

use crate::{
    ingest::{
        data_plane_index::{DataPlane, DataPlaneIndex, TokenFreshness, TokenSlot, TokenSlotState},
        market_registry::MarketRegistry,
    },
    observability::metrics_hub::MetricsHub,
};

/// Read facade over stable [`TokenKey`] slots owned by the shared data plane.
pub struct BookStore {
    data_plane: Arc<DataPlane>,
    metrics: Arc<MetricsHub>,
    metric_update_counter: AtomicU64,
    gap_generation: AtomicU64,
    update_notify: Notify,
}

impl BookStore {
    pub fn new(data_plane: Arc<DataPlane>, metrics: Arc<MetricsHub>) -> Self {
        Self {
            data_plane,
            metrics,
            metric_update_counter: AtomicU64::new(0),
            gap_generation: AtomicU64::new(0),
            update_notify: Notify::new(),
        }
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

    /// Borrow a snapshot without incrementing its `Arc` refcount.
    ///
    /// The callback must remain synchronous and must never retain the reference
    /// across an `.await`.
    #[inline]
    pub fn read<R>(&self, token: TokenKey, read: impl FnOnce(&BookSnapshot) -> R) -> Option<R> {
        self.data_plane.with_slot(token, |slot| {
            let snapshot = slot.published.load();
            read(&snapshot)
        })
    }

    /// Load an owned snapshot for transfer across task or await boundaries.
    #[must_use]
    #[inline]
    pub fn load_owned(&self, token: TokenKey) -> Option<Arc<BookSnapshot>> {
        self.data_plane
            .with_slot(token, |slot| slot.published.load_full())
    }

    /// Resolve a wire/catalog token at the boundary and return an owned snapshot.
    #[must_use]
    pub fn load_by_id(&self, token_id: &TokenId) -> Option<Arc<BookSnapshot>> {
        self.load_owned(self.resolve(token_id)?)
    }

    /// Monotonic publish sequence for a token (0 if unknown).
    #[inline]
    pub fn book_version(&self, token: TokenKey) -> u64 {
        self.read(token, |snapshot| snapshot.version).unwrap_or(0)
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
    pub fn publish(
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
                slot.publish(
                    Arc::new(snapshot),
                    sequence,
                    session_generation,
                    applied_tick,
                    latency_tick,
                );
            })
            .is_some();
        if !published {
            return false;
        }
        self.update_token_count_metric();
        self.update_notify.notify_one();
        true
    }

    #[must_use]
    #[inline]
    pub fn freshness(&self, token: TokenKey) -> Option<TokenFreshness> {
        self.data_plane.with_slot(token, TokenSlot::freshness)
    }

    #[must_use]
    pub fn freshness_age_ms(&self, token: TokenKey) -> Option<u64> {
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

    pub fn mark_fresh(
        &self,
        token: TokenKey,
        sequence: u64,
        session_generation: u64,
        latency_tick: u64,
    ) -> bool {
        self.data_plane
            .with_slot(token, |slot| {
                slot.mark_fresh(
                    sequence,
                    session_generation,
                    self.data_plane.now_tick(),
                    latency_tick,
                );
            })
            .is_some()
    }

    /// Publish canonical stream provenance while preserving the apply latency tick.
    pub fn mark_canonical_fresh(
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
                );
            })
            .is_some()
    }

    pub fn invalidate(&self, token: TokenKey, session_generation: u64) -> bool {
        self.data_plane
            .with_slot(token, |slot| slot.invalidate(session_generation))
            .is_some()
    }

    /// Invalidate process-local token keys without boundary lookup or allocation.
    pub fn invalidate_tokens(&self, tokens: &[TokenKey]) -> usize {
        self.data_plane.with_index(|index| {
            let mut invalidated = 0;
            for token in tokens {
                let Some(slot) = index.token_slot(*token) else {
                    continue;
                };
                let next_generation = slot.freshness().session_generation.saturating_add(1);
                slot.invalidate(next_generation);
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
                let next_generation = slot.freshness().session_generation.saturating_add(1);
                slot.invalidate(next_generation);
                invalidated += 1;
            }
            invalidated
        })
    }

    pub fn token_count(&self) -> usize {
        self.data_plane.with_index(DataPlaneIndex::token_count)
    }

    pub fn published_snapshots(&self) -> Vec<(TokenKey, TokenId, Arc<BookSnapshot>)> {
        self.data_plane.with_index(|index| {
            (0..index.token_count())
                .filter_map(|raw| {
                    let token = TokenKey::new(u32::try_from(raw).ok()?);
                    let metadata = index.token_metadata(token)?;
                    let slot = index.token_slot(token)?;
                    if slot.freshness().state == TokenSlotState::Unseen {
                        return None;
                    }
                    Some((token, metadata.token_id.clone(), slot.published.load_full()))
                })
                .collect()
        })
    }

    /// Load YES+NO published snapshots without copying level data.
    #[inline]
    pub fn load_pair(&self, token_yes: TokenKey, token_no: TokenKey) -> Option<BinaryBookPair> {
        let yes = self.load_owned(token_yes)?;
        let no = self.load_owned(token_no)?;
        Some(BinaryBookPair { yes, no })
    }

    /// Top-of-book for execution validation (four prices + staleness + versions).
    #[inline]
    pub fn top_of_book_tokens(
        &self,
        token_yes: TokenKey,
        token_no: TokenKey,
        now_ms: u64,
    ) -> Option<TopOfBook> {
        let yes = self.load_owned(token_yes)?;
        let no = self.load_owned(token_no)?;
        Some(TopOfBook {
            yes_best_bid: yes.best_bid(),
            yes_best_ask: yes.best_ask(),
            no_best_bid: no.best_bid(),
            no_best_ask: no.best_ask(),
            max_staleness_ms: now_ms
                .saturating_sub(yes.timestamp_ms)
                .max(now_ms.saturating_sub(no.timestamp_ms)),
            yes_version: yes.version,
            no_version: no.version,
        })
    }

    /// Top-of-book via registry lookup (prefer [`Self::top_of_book_tokens`] on hot path).
    pub fn top_of_book(
        &self,
        registry: &MarketRegistry,
        market_id: &MarketId,
        now_ms: u64,
    ) -> Option<TopOfBook> {
        let pair = registry
            .data_plane()
            .with_index(|index| index.market_token_pair(market_id))?;
        self.top_of_book_tokens(pair.yes, pair.no, now_ms)
    }
}

#[cfg(test)]
mod tests {
    use std::slice;

    use quant_pivot_models::{
        domain::market::BookLevel,
        types::{Price, Shares},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;
    fn make_level(price: Decimal, size: Decimal) -> BookLevel {
        BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(size))
    }

    fn publish(
        store: &BookStore,
        token: TokenKey,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        version: u64,
    ) {
        assert!(store.publish(
            token,
            BookSnapshot::new(Arc::from(bids), Arc::from(asks), version, version),
            version,
            1,
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

        let snap = store.load_owned(token).unwrap();
        assert_eq!(snap.best_bid().unwrap().inner(), dec!(0.5));
        assert_eq!(snap.best_ask().unwrap().inner(), dec!(0.6));
        assert_eq!(store.book_version(token), 1);
    }

    #[test]
    fn load_pair_zero_copy() {
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
        let pair = store.load_pair(yes_key, no_key).unwrap();
        assert_eq!(pair.view().yes_bids.levels.len(), 1);
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
        assert_eq!(store.book_version(token), 2);
    }
}
