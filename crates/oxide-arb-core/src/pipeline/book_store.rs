use std::sync::Arc;

use dashmap::DashMap;
use num_traits::ToPrimitive;
use parking_lot::RwLock;

use oxide_arb_models::domain::book::BookLevel;
use oxide_arb_models::types::{Price, Shares, TokenId};

use super::order_book::OrderBook;
use crate::observability::metrics_hub::MetricsHub;

/// Central orderbook store keyed by token.
///
/// Uses `DashMap` for shard-level locking: the WS event loop writes while
/// the Scanner reads concurrently. Inner `parking_lot::RwLock` protects
/// each individual `OrderBook` — hold time is sub-microsecond (Vec clone).
pub struct BookStore {
    books: DashMap<TokenId, Arc<RwLock<OrderBook>>>,
    metrics: Arc<MetricsHub>,
}

impl BookStore {
    pub fn new(metrics: Arc<MetricsHub>) -> Self {
        Self {
            books: DashMap::new(),
            metrics,
        }
    }

    /// Return (or lazily create) the orderbook for `token_id`.
    pub fn get_or_create(&self, token_id: &TokenId) -> Arc<RwLock<OrderBook>> {
        let book = self
            .books
            .entry(token_id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(OrderBook::new(token_id.clone()))))
            .value()
            .clone();

        self.metrics
            .book_store_token_count
            .set(ToPrimitive::to_i64(&self.books.len()).unwrap_or(i64::MAX));
        book
    }

    /// Retrieve an existing orderbook without creating one.
    pub fn get(&self, token_id: &TokenId) -> Option<Arc<RwLock<OrderBook>>> {
        self.books.get(token_id).map(|r| r.value().clone())
    }

    /// Apply a full snapshot to the book for `token_id`.
    pub fn apply_snapshot(
        &self,
        token_id: &TokenId,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        timestamp_ms: u64,
    ) {
        let book = self.get_or_create(token_id);
        book.write().apply_snapshot(bids, asks, timestamp_ms);
    }

    /// Apply an incremental delta to the book for `token_id`.
    pub fn apply_delta(&self, token_id: &TokenId, changes: &[(Price, Shares)], timestamp_ms: u64) {
        let book = self.get_or_create(token_id);
        book.write().apply_delta(changes, timestamp_ms);
    }

    /// Remove a token's orderbook (e.g. market delisted).
    pub fn remove(&self, token_id: &TokenId) {
        self.books.remove(token_id);
        self.metrics
            .book_store_token_count
            .set(ToPrimitive::to_i64(&self.books.len()).unwrap_or(i64::MAX));
    }

    /// Number of tokens currently tracked.
    pub fn token_count(&self) -> usize {
        self.books.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn make_level(price: rust_decimal::Decimal, size: rust_decimal::Decimal) -> BookLevel {
        BookLevel {
            price: Price::new(price),
            size: Shares::new(size),
        }
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

        let book = store.get(&tid).unwrap();
        let guard = book.read();
        assert_eq!(guard.best_bid().unwrap().inner(), dec!(0.5));
        assert_eq!(guard.best_ask().unwrap().inner(), dec!(0.6));
        drop(guard);
    }

    #[test]
    fn remove_token() {
        let metrics = Arc::new(MetricsHub::new());
        let store = BookStore::new(metrics);
        let tid = TokenId::new("tok-1");
        store.apply_snapshot(&tid, vec![], vec![], 1);
        assert_eq!(store.token_count(), 1);
        store.remove(&tid);
        assert_eq!(store.token_count(), 0);
        assert!(store.get(&tid).is_none());
    }
}
