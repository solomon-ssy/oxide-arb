use super::order_book::OrderBook;
use crate::{observability::metrics_hub::MetricsHub, pipeline::market_registry::MarketRegistry};
use arc_swap::ArcSwap;
use dashmap::DashMap;
use num_traits::ToPrimitive;
use parking_lot::Mutex;
use quant_pivot_models::{
    domain::{
        BookLevel,
        book::{BookSnapshot, EndgameBookPair, TopOfBook},
        latency::LatencyTrace,
    },
    enums::common::Side,
    types::{MarketId, Price, Shares, TokenId},
};
use std::sync::atomic::AtomicU64 as StdSyncAtomicU64;
use std::sync::atomic::Ordering as StdSyncOrdering;
use std::{
    sync::{
        Arc, atomic,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

struct TokenBookState {
    live: Mutex<OrderBook>,
    published: ArcSwap<BookSnapshot>,
    version: AtomicU64,
}

/// Central orderbook store keyed by token.
///
/// Writers mutate `live` under a short mutex; readers load `published` via
/// `ArcSwap` with zero locking. Publish bumps `version` and refcount-clones arcs.
pub struct BookStore {
    books: DashMap<TokenId, Arc<TokenBookState>>,
    token_latency_traces: DashMap<TokenId, Arc<LatencyTrace>>,
    metrics: Arc<MetricsHub>,
    metric_update_counter: atomic::AtomicU64,
}

impl BookStore {
    pub fn new(metrics: Arc<MetricsHub>) -> Self {
        Self {
            books: DashMap::new(),
            token_latency_traces: DashMap::new(),
            metrics,
            metric_update_counter: StdSyncAtomicU64::new(0),
        }
    }

    fn get_or_create_state(&self, token_id: &TokenId) -> Arc<TokenBookState> {
        if let Some(entry) = self.books.get(token_id) {
            return Arc::clone(entry.value());
        }

        let empty: Arc<[BookLevel]> = Arc::from([]);
        let state = Arc::new(TokenBookState {
            live: Mutex::new(OrderBook::new(token_id.clone())),
            published: ArcSwap::from_pointee(BookSnapshot::new(
                Arc::clone(&empty),
                Arc::clone(&empty),
                0,
                0,
            )),
            version: AtomicU64::new(0),
        });
        let inserted = self
            .books
            .entry(token_id.clone())
            .or_insert_with(|| Arc::clone(&state));
        let result = Arc::clone(inserted.value());
        drop(inserted);
        if Arc::ptr_eq(&result, &state) {
            self.update_token_count_metric(true);
        }
        result
    }

    fn bump_and_publish(state: &TokenBookState, book: &OrderBook) -> u64 {
        let version = state.version.fetch_add(1, Ordering::AcqRel) + 1;
        state.published.store(Arc::new(book.publish_cow(version)));
        version
    }

    fn update_token_count_metric(&self, force: bool) {
        if force {
            self.metrics
                .book_store_token_count
                .set(ToPrimitive::to_i64(&self.books.len()).unwrap_or(i64::MAX));
            return;
        }
        let n = self
            .metric_update_counter
            .fetch_add(1, StdSyncOrdering::Relaxed);
        if n % 1024 == 0 {
            self.metrics
                .book_store_token_count
                .set(ToPrimitive::to_i64(&self.books.len()).unwrap_or(i64::MAX));
        }
    }

    #[inline]
    pub fn load(&self, token_id: &TokenId) -> Option<Arc<BookSnapshot>> {
        self.books
            .get(token_id)
            .map(|entry| entry.value().published.load_full())
    }

    /// Monotonic publish sequence for a token (0 if unknown).
    #[inline]
    pub fn book_version(&self, token_id: &TokenId) -> u64 {
        self.books
            .get(token_id)
            .map_or(0, |e| e.value().version.load(Ordering::Acquire))
    }

    pub fn apply_snapshot(
        &self,
        token_id: &TokenId,
        bids: impl Into<Arc<[BookLevel]>>,
        asks: impl Into<Arc<[BookLevel]>>,
        timestamp_ms: u64,
        latency: Option<LatencyTrace>,
    ) -> u64 {
        let bids = bids.into();
        let asks = asks.into();
        let state = self.get_or_create_state(token_id);
        let mut book = state.live.lock();
        book.apply_snapshot_arc(&bids, &asks, timestamp_ms);
        let version = Self::bump_and_publish(&state, &book);
        drop(book);
        if let Some(mut lat) = latency {
            lat.book_applied = Some(Instant::now());
            self.token_latency_traces
                .insert(token_id.clone(), Arc::new(lat));
        }
        version
    }

    pub fn apply_delta<I>(
        &self,
        token_id: &TokenId,
        changes: I,
        timestamp_ms: u64,
        latency: Option<LatencyTrace>,
    ) -> u64
    where
        I: IntoIterator<Item = (Side, Price, Shares)>,
    {
        let state = self.get_or_create_state(token_id);
        let mut book = state.live.lock();
        book.apply_delta(changes, timestamp_ms);
        let version = Self::bump_and_publish(&state, &book);
        drop(book);
        if let Some(mut lat) = latency {
            lat.book_applied = Some(Instant::now());
            self.token_latency_traces
                .insert(token_id.clone(), Arc::new(lat));
        }
        version
    }

    #[inline]
    pub fn token_latency_trace(&self, token_id: &TokenId) -> Option<Arc<LatencyTrace>> {
        self.token_latency_traces
            .get(token_id)
            .map(|entry| Arc::clone(entry.value()))
    }

    pub fn remove(&self, token_id: &TokenId) {
        self.books.remove(token_id);
        self.update_token_count_metric(true);
    }

    pub fn token_count(&self) -> usize {
        self.books.len()
    }

    pub fn published_snapshots(&self) -> Vec<(TokenId, Arc<BookSnapshot>)> {
        self.books
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().published.load_full()))
            .collect()
    }

    /// Load YES+NO published snapshots without copying level data.
    #[inline]
    pub fn load_pair(&self, token_yes: &TokenId, token_no: &TokenId) -> Option<EndgameBookPair> {
        let yes = self.load(token_yes)?;
        let no = self.load(token_no)?;
        Some(EndgameBookPair { yes, no })
    }

    /// Top-of-book for execution validation (four prices + staleness + versions).
    #[inline]
    pub fn top_of_book_tokens(
        &self,
        token_yes: &TokenId,
        token_no: &TokenId,
        now_ms: u64,
    ) -> Option<TopOfBook> {
        let yes = self.load(token_yes)?;
        let no = self.load(token_no)?;
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
        let (token_yes, token_no) = registry.token_pair(market_id)?;
        self.top_of_book_tokens(&token_yes, &token_no, now_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_pivot_models::enums::common::Side;
    use rust_decimal_macros::dec;
    fn make_level(price: rust_decimal::Decimal, size: rust_decimal::Decimal) -> BookLevel {
        BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(size))
    }

    #[test]
    fn snapshot_creates_and_updates() {
        let metrics = Arc::new(MetricsHub::new());
        let store = BookStore::new(metrics);

        let tid = TokenId::new("tok-1");
        let v = store.apply_snapshot(
            &tid,
            vec![make_level(dec!(0.5), dec!(10))],
            vec![make_level(dec!(0.6), dec!(5))],
            100,
            None,
        );
        assert_eq!(v, 1);
        assert_eq!(store.token_count(), 1);

        let snap = store.load(&tid).unwrap();
        assert_eq!(snap.best_bid().unwrap().inner(), dec!(0.5));
        assert_eq!(snap.best_ask().unwrap().inner(), dec!(0.6));
        assert_eq!(store.book_version(&tid), 1);
    }

    #[test]
    fn load_pair_zero_copy() {
        let metrics = Arc::new(MetricsHub::new());
        let store = BookStore::new(metrics);
        let yes = TokenId::new("yes");
        let no = TokenId::new("no");
        store.apply_snapshot(
            &yes,
            vec![make_level(dec!(0.95), dec!(10))],
            vec![],
            1,
            None,
        );
        store.apply_snapshot(&no, vec![], vec![make_level(dec!(0.05), dec!(10))], 1, None);
        let pair = store.load_pair(&yes, &no).unwrap();
        assert_eq!(pair.view().yes_bids.levels.len(), 1);
    }

    #[test]
    fn version_increments_on_delta() {
        let metrics = Arc::new(MetricsHub::new());
        let store = BookStore::new(metrics);
        let tid = TokenId::new("t");
        store.apply_snapshot(&tid, vec![make_level(dec!(0.5), dec!(1))], vec![], 1, None);
        let v2 = store.apply_delta(
            &tid,
            [(Side::Buy, Price::new(dec!(0.55)), Shares::new(dec!(2)))],
            2,
            None,
        );
        assert_eq!(v2, 2);
    }
}
