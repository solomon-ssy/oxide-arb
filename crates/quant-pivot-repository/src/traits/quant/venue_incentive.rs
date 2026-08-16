use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{NewVenueIncentiveEvent, VenueIncentiveReconciliation},
    types::{ExecutionAccountId, Usd},
};

/// Append-only venue incentive persistence and wallet-credit accounting.
#[async_trait::async_trait]
pub trait VenueIncentiveRepository: Send + Sync {
    /// Idempotently record venue evidence. An identity replay with changed
    /// economics fails closed.
    async fn record(&self, events: Vec<NewVenueIncentiveEvent>) -> Result<(), StorageError>;

    /// Cumulative wallet-confirmed credits visible by the PIT boundary.
    async fn credited_cumulative(
        &self,
        execution_account_id: &ExecutionAccountId,
        as_of: DateTime<Utc>,
    ) -> Result<Usd, StorageError>;

    /// Cumulative latest-observation totals for each independent incentive
    /// stage. Revisions of one venue award partition count exactly once.
    async fn reconciliation_cumulative(
        &self,
        execution_account_id: &ExecutionAccountId,
        as_of: DateTime<Utc>,
    ) -> Result<VenueIncentiveReconciliation, StorageError>;
}
