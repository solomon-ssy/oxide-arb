//! `ClickHouse`-backed read repository for quant facts (feature window inputs +
//! historical point-in-time state).

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{BookMicrostructureRow, BookSnapshotRow, MarketResolutionRow},
    types::{MarketId, TokenId},
};
use quant_pivot_storage::clickhouse::ClickHousePool;

use crate::traits::QuantFactReadRepository;

/// Quant fact source, queried straight from `ClickHouse`.
pub struct ChQuantFactReadRepository {
    pool: Arc<ClickHousePool>,
}

impl ChQuantFactReadRepository {
    /// Build a read repository over a `ClickHouse` pool.
    #[must_use]
    pub const fn new(pool: Arc<ClickHousePool>) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl QuantFactReadRepository for ChQuantFactReadRepository {
    async fn microstructure_window(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM book_microstructure_1s \
                 WHERE token_id IN ? \
                 AND bucket_time >= fromUnixTimestamp64Milli(?) \
                 AND bucket_time < fromUnixTimestamp64Milli(?) \
                 ORDER BY token_id, bucket_time",
            )
            .bind(token_ids)
            .bind(from_ms)
            .bind(to_ms)
            .fetch_all::<BookMicrostructureRow>()
            .await?;
        Ok(rows)
    }

    async fn book_snapshot_at(
        &self,
        token_id: &TokenId,
        as_of_ms: i64,
    ) -> Result<Option<BookSnapshotRow>, StorageError> {
        let rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM book_snapshots \
                 WHERE token_id = ? \
                 AND event_time <= fromUnixTimestamp64Milli(?) \
                 ORDER BY event_time DESC, ingestion_time DESC, sequence DESC \
                 LIMIT 1",
            )
            .bind(token_id.clone())
            .bind(as_of_ms)
            .fetch_all::<BookSnapshotRow>()
            .await?;
        Ok(rows.into_iter().next())
    }

    async fn book_snapshots_between(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<BookSnapshotRow>, StorageError> {
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM book_snapshots \
                 WHERE token_id IN ? \
                 AND event_time >= fromUnixTimestamp64Milli(?) \
                 AND event_time <= fromUnixTimestamp64Milli(?) \
                 ORDER BY token_id, event_time, ingestion_time, sequence",
            )
            .bind(token_ids)
            .bind(from_ms)
            .bind(to_ms)
            .fetch_all::<BookSnapshotRow>()
            .await?;
        Ok(rows)
    }

    async fn resolution_at(
        &self,
        market_id: &MarketId,
        as_of_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        let rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM market_resolution_event \
                 WHERE market_id = ? \
                 AND resolved_at <= fromUnixTimestamp64Milli(?) \
                 ORDER BY resolved_at DESC, observed_at DESC, sequence DESC \
                 LIMIT 1",
            )
            .bind(market_id.clone())
            .bind(as_of_ms)
            .fetch_all::<MarketResolutionRow>()
            .await?;
        Ok(rows.into_iter().next())
    }

    async fn resolutions_between(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError> {
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM market_resolution_event \
                 WHERE market_id IN ? \
                 AND resolved_at >= fromUnixTimestamp64Milli(?) \
                 AND resolved_at <= fromUnixTimestamp64Milli(?) \
                 ORDER BY market_id, resolved_at, observed_at, sequence",
            )
            .bind(market_ids)
            .bind(from_ms)
            .bind(to_ms)
            .fetch_all::<MarketResolutionRow>()
            .await?;
        Ok(rows)
    }
}
