use quant_pivot_error::storage::StorageError;
use quant_pivot_models::types::Usd;

/// Read-only aggregation of capital reserved by in-flight execution allocations.
///
/// Report sizing (Phase 4 account provider) reads through this port; the aggregate
/// is sourced from `quant_capital_allocation`, not order-intent status sums.
#[async_trait::async_trait]
pub trait ReservedCapitalRepository: Send + Sync {
    /// Total net reserved USD (zero when none).
    ///
    /// See [`sum_reserved_usd`](crate::postgres::quant::capital_allocation::sum_reserved_usd).
    async fn sum_reserved_usd(&self) -> Result<Usd, StorageError>;
}
