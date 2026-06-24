//! `ClickHouse`-backed read repository for quant facts (feature window inputs).

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{clickhouse::BookMicrostructureRow, types::TokenId};
use quant_pivot_storage::clickhouse::ClickHousePool;

use crate::traits::QuantFactReadRepository;

/// One-second microstructure fact source, queried straight from `ClickHouse`.
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
}
