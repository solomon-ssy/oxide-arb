//! Point-in-time read port over quant `ClickHouse` facts.
//!
//! The feature plane pre-fetches windowed microstructure facts once per round
//! (never a query inside the build loop). The **online** implementation reads
//! recent facts bounded by the PIT cutoff; the **historical**, `as_of`-bounded
//! implementation for backtests / training datasets lands in 3.5.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{clickhouse::BookMicrostructureRow, types::TokenId};

/// Read port over persisted quant facts, used to materialize feature windows.
#[async_trait::async_trait]
pub trait QuantFactReadRepository: Send + Sync {
    /// One-second microstructure buckets for `token_ids` whose `bucket_time`
    /// falls in `[from_ms, to_ms)` (epoch milliseconds), ordered by token then
    /// time. `to_ms` is the caller's PIT cutoff, so no look-ahead is possible.
    async fn microstructure_window(
        &self,
        token_ids: Vec<TokenId>,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError>;
}
