//! `ClickHouse`-backed read repository for quant facts (feature window inputs +
//! historical point-in-time state).

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, MarketResolutionRow, MidPriceBucketRow,
        TickEventRow, TradeTapeRow,
    },
    enums::clickhouse::{ChBookEventType, ChTradeTapeSource},
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

    async fn microstructure_series(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        minute: bool,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        // The 1s and 1m tables share an identical column schema, so only the
        // relation name differs — never interpolate untrusted input here.
        let sql = if minute {
            "SELECT ?fields FROM book_microstructure_1m \
             WHERE token_id IN ? \
             AND bucket_time >= fromUnixTimestamp64Milli(?) \
             AND bucket_time < fromUnixTimestamp64Milli(?) \
             ORDER BY token_id, bucket_time"
        } else {
            "SELECT ?fields FROM book_microstructure_1s \
             WHERE token_id IN ? \
             AND bucket_time >= fromUnixTimestamp64Milli(?) \
             AND bucket_time < fromUnixTimestamp64Milli(?) \
             ORDER BY token_id, bucket_time"
        };
        let rows = self
            .pool
            .client()
            .query(sql)
            .bind(token_ids)
            .bind(from_ms)
            .bind(to_ms)
            .fetch_all::<BookMicrostructureRow>()
            .await?;
        Ok(rows)
    }

    async fn last_trades(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        limit: u64,
    ) -> Result<Vec<TickEventRow>, StorageError> {
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM tick_events \
                 WHERE token_id IN ? \
                 AND event_type = ? \
                 AND last_trade_price IS NOT NULL \
                 AND event_time >= fromUnixTimestamp64Milli(?) \
                 AND event_time < fromUnixTimestamp64Milli(?) \
                 ORDER BY event_time DESC, ingestion_time DESC, sequence DESC \
                 LIMIT ?",
            )
            .bind(token_ids)
            .bind(ChBookEventType::LastTrade)
            .bind(from_ms)
            .bind(to_ms)
            .bind(limit)
            .fetch_all::<TickEventRow>()
            .await?;
        Ok(rows)
    }

    async fn trade_tape_window_by_market(
        &self,
        market_ids: Vec<MarketId>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        if market_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .pool
            .client()
            .query(
                "SELECT ?fields FROM quant_trade_tape FINAL \
                 WHERE market_id IN ? \
                 AND source = ? \
                 AND event_time >= fromUnixTimestamp64Milli(?) \
                 AND event_time < fromUnixTimestamp64Milli(?) \
                 ORDER BY market_id, event_time, ingestion_time, trade_id, participant_role",
            )
            .bind(market_ids)
            .bind(ChTradeTapeSource::OnChain)
            .bind(from_ms)
            .bind(to_ms)
            .fetch_all::<TradeTapeRow>()
            .await?;
        Ok(rows)
    }

    async fn mid_price_series(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
        bucket_secs: u32,
    ) -> Result<Vec<MidPriceBucketRow>, StorageError> {
        if token_ids.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self
            .pool
            .client()
            .query(
                "SELECT token_id, \
                 toUnixTimestamp64Milli(toStartOfInterval(bucket_time, toIntervalSecond(?))) \
                 AS bucket_ms, \
                 argMax(mid_price_close, bucket_time) AS mid_price \
                 FROM book_microstructure_1s \
                 WHERE token_id IN ? \
                 AND bucket_time >= fromUnixTimestamp64Milli(?) \
                 AND bucket_time < fromUnixTimestamp64Milli(?) \
                 GROUP BY token_id, bucket_ms \
                 ORDER BY token_id, bucket_ms",
            )
            .bind(bucket_secs)
            .bind(token_ids)
            .bind(from_ms)
            .bind(to_ms)
            .fetch_all::<MidPriceBucketRow>()
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

    async fn observed_markets_between(
        &self,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<MarketId>, StorageError> {
        // `market_id` is Nullable in `book_snapshots`; `assumeNotNull` after the
        // `IS NOT NULL` guard yields a non-nullable column the row can decode.
        let rows = self
            .pool
            .client()
            .query(
                "SELECT DISTINCT assumeNotNull(market_id) AS market_id FROM book_snapshots \
                 WHERE market_id IS NOT NULL \
                 AND event_time >= fromUnixTimestamp64Milli(?) \
                 AND event_time <= fromUnixTimestamp64Milli(?) \
                 ORDER BY market_id",
            )
            .bind(from_ms)
            .bind(to_ms)
            .fetch_all::<ObservedMarketRow>()
            .await?;
        Ok(rows.into_iter().map(|row| row.market_id).collect())
    }
}

/// Single-column projection for [`ChQuantFactReadRepository::observed_markets_between`].
#[derive(clickhouse::Row, serde::Deserialize)]
struct ObservedMarketRow {
    market_id: MarketId,
}
