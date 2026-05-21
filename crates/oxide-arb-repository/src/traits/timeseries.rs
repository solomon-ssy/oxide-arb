use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::clickhouse::{
    BookSnapshotRow, CalibrationSnapshotRow, OpportunityAuditRow, SignalDataRow, TickEventRow,
};

pub trait TimeseriesRepository: Send + Sync {
    async fn insert_tick_events(&self, events: &[TickEventRow]) -> Result<(), StorageError>;
    async fn insert_book_snapshot(&self, snapshot: &BookSnapshotRow) -> Result<(), StorageError>;
    async fn insert_opportunity_audit(
        &self,
        audit: &OpportunityAuditRow,
    ) -> Result<(), StorageError>;
    async fn insert_signal_data(&self, signals: &[SignalDataRow]) -> Result<(), StorageError>;
    async fn insert_calibration_snapshot(
        &self,
        snapshot: &CalibrationSnapshotRow,
    ) -> Result<(), StorageError>;

    async fn query_tick_events(
        &self,
        token_id: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<TickEventRow>, StorageError>;

    async fn query_opportunity_audit(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<OpportunityAuditRow>, StorageError>;

    async fn query_calibration_history(
        &self,
        category: &str,
        price_zone: &str,
        duration_bucket: &str,
        days: u32,
    ) -> Result<Vec<CalibrationSnapshotRow>, StorageError>;
}
