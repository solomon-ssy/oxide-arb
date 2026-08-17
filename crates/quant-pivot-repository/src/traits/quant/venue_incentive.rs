use chrono::{DateTime, NaiveDate, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::quant_incentive::VenueIncentiveEventListQuery,
        pagination::Paginated,
        quant::venue_incentive::{
            NewVenueIncentiveEvent, NewVenueIncentiveReconciliationScan,
            NewVenueIncentiveReportedAccrualSnapshot, VenueIncentiveEventInfo,
            VenueIncentiveReconciliation, VenueIncentiveReconciliationScanInfo,
        },
    },
    types::{ExecutionAccountId, Usd},
};

/// Append-only venue incentive persistence and wallet-credit accounting.
#[async_trait::async_trait]
pub trait VenueIncentiveRepository: Send + Sync {
    /// Idempotently record venue evidence. An identity replay with changed
    /// economics fails closed.
    async fn record(&self, events: Vec<NewVenueIncentiveEvent>) -> Result<(), StorageError>;

    /// Atomically commit an immutable scan manifest with its wallet-credit events.
    async fn record_scan(
        &self,
        scan: NewVenueIncentiveReconciliationScan,
        events: Vec<NewVenueIncentiveEvent>,
    ) -> Result<(), StorageError>;

    /// Apply a complete maker-award snapshot and retract partitions absent
    /// from the new response in the same transaction as the scan manifest.
    async fn apply_reported_accrual_snapshot(
        &self,
        snapshot: NewVenueIncentiveReportedAccrualSnapshot,
    ) -> Result<(), StorageError>;

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

    /// Observation time of the oldest venue-awarded maker amount that remains
    /// unmatched by wallet credits under deterministic FIFO attribution.
    async fn maker_credit_pending_since(
        &self,
        execution_account_id: &ExecutionAccountId,
        as_of: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, StorageError>;

    /// All scan attempts covering a closed-day health window.
    async fn scans(
        &self,
        execution_account_id: &ExecutionAccountId,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<VenueIncentiveReconciliationScanInfo>, StorageError>;

    /// Complete maker estimate/reported-accrual/credit ledger visible at the
    /// valuation boundary. Callers collapse revisioned source partitions.
    async fn maker_valuation_events(
        &self,
        execution_account_id: &ExecutionAccountId,
        as_of: DateTime<Utc>,
    ) -> Result<Vec<VenueIncentiveEventInfo>, StorageError>;

    /// Paginated immutable incentive events for operator audit.
    async fn page_events(
        &self,
        execution_account_id: &ExecutionAccountId,
        query: VenueIncentiveEventListQuery,
    ) -> Result<Paginated<VenueIncentiveEventInfo>, StorageError>;
}
