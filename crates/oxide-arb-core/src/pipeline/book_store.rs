use std::sync::Arc;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use num_traits::ToPrimitive;
use oxide_arb_models::domain::BookLevel;
use parking_lot::Mutex;

use oxide_arb_models::domain::book::{BookSnapshot, EndgameBookPair, TopOfBook};
use oxide_arb_models::types::{MarketId, Price, Shares, TokenId};

use super::order_book::OrderBook;
use crate::observability::metrics_hub::MetricsHub;
use crate::pipeline::market_registry::MarketRegistry;

struct TokenBookState {
    live: Mutex<OrderBook>,
    published: ArcSwap<BookSnapshot>,
}

/// Central orderbook store keyed by token.
///
/// Writers mutate `live` under a short mutex; readers load `published` via
/// `ArcSwap` with zero locking and zero cloning.
pub struct BookStore {
    books: DashMap<TokenId, Arc<TokenBookState>>,
    metrics: Arc<MetricsHub>,
    metric_update_counter: std::sync::atomic::AtomicU64,
}

impl BookStore {
    pub fn new(metrics: Arc<MetricsHub>) -> Self {
        Self {
            books: DashMap::new(),
            metrics,
            metric_update_counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn get_or_create_state(&self, token_id: &TokenId) -> Arc<TokenBookState> {
        if let Some(entry) = self.books.get(token_id) {
            return Arc::clone(entry.value());
        }

        let state = Arc::new(TokenBookState {
            live: Mutex::new(OrderBook::new(token_id.clone())),
            published: ArcSwap::from_pointee(BookSnapshot::new(Arc::from([]), Arc::from([]), 0)),
        });
        self.books.insert(token_id.clone(), Arc::clone(&state));
        self.update_token_count_metric(true);
        state
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
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

    pub fn apply_snapshot(
        &self,
        token_id: &TokenId,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        timestamp_ms: u64,
    ) {
        let state = self.get_or_create_state(token_id);
        let mut book = state.live.lock();
        book.apply_snapshot(bids, asks, timestamp_ms);
        state.published.store(Arc::new(book.publish()));
    }

    pub fn apply_delta<I>(&self, token_id: &TokenId, changes: I, timestamp_ms: u64)
    where
        I: IntoIterator<Item = (Price, Shares)>,
    {
        let state = self.get_or_create_state(token_id);
        let mut book = state.live.lock();
        book.apply_delta(changes, timestamp_ms);
        state.published.store(Arc::new(book.publish()));
    }

    pub fn remove(&self, token_id: &TokenId) {
        self.books.remove(token_id);
        self.update_token_count_metric(true);
    }

    pub fn token_count(&self) -> usize {
        self.books.len()
    }

    /// Load YES+NO published snapshots without copying level data.
    #[inline]
    pub fn load_pair(&self, token_yes: &TokenId, token_no: &TokenId) -> Option<EndgameBookPair> {
        let yes = self.load(token_yes)?;
        let no = self.load(token_no)?;
        Some(EndgameBookPair { yes, no })
    }

    /// Top-of-book for execution validation (4 prices + staleness, zero depth clone).
    pub fn top_of_book(
        &self,
        registry: &MarketRegistry,
        market_id: &MarketId,
        now_ms: u64,
    ) -> Option<TopOfBook> {
        let (token_yes, token_no) = registry.token_pair(market_id)?;
        let yes = self.load(&token_yes)?;
        let no = self.load(&token_no)?;
        Some(TopOfBook {
            yes_best_bid: yes.best_bid(),
            yes_best_ask: yes.best_ask(),
            no_best_bid: no.best_bid(),
            no_best_ask: no.best_ask(),
            max_staleness_ms: EndgameBookPair { yes, no }.max_staleness_ms(now_ms),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxide_arb_models::domain::book::BookLevel;
    use rust_decimal_macros::dec;

    fn make_level(price: rust_decimal::Decimal, size: rust_decimal::Decimal) -> BookLevel {
        BookLevel::from_decimal_unchecked(Price::new(price), Shares::new(size))
    }

    #[test]
    fn snapshot_creates_and_updates() {
        let metrics = Arc::new(MetricsHub::new());
        let store = BookStore::new(metrics);

        let tid = TokenId::new("tok-1");
        store.apply_snapshot(
            &tid,
            vec![make_level(dec!(0.5), dec!(10))],
            vec![make_level(dec!(0.6), dec!(5))],
            100,
        );
        assert_eq!(store.token_count(), 1);

        let snap = store.load(&tid).unwrap();
        assert_eq!(snap.best_bid().unwrap().inner(), dec!(0.5));
        assert_eq!(snap.best_ask().unwrap().inner(), dec!(0.6));
    }

    #[test]
    fn load_pair_zero_copy() {
        let metrics = Arc::new(MetricsHub::new());
        let store = BookStore::new(metrics);
        let yes = TokenId::new("yes");
        let no = TokenId::new("no");
        store.apply_snapshot(&yes, vec![make_level(dec!(0.95), dec!(10))], vec![], 1);
        store.apply_snapshot(&no, vec![], vec![make_level(dec!(0.05), dec!(10))], 1);
        let pair = store.load_pair(&yes, &no).unwrap();
        assert_eq!(pair.view().yes_bids.levels.len(), 1);
    }
}
