use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::clickhouse::{
    BookSnapshotRow, CalibrationSnapshotRow, OpportunityAuditRow, OpportunityDetectionRow,
    TickEventL2Row, TickEventRow,
};
use oxide_arb_models::enums::clickhouse::ChMarketCategory;
use oxide_arb_models::types::{EventId, MarketId, OpportunityId, TokenId};

#[derive(Debug, Clone, Copy)]
pub struct TimeWindow {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

impl TimeWindow {
    #[must_use]
    pub const fn new(from: DateTime<Utc>, to: DateTime<Utc>) -> Self {
        Self { from, to }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MarketFilter {
    pub market_ids: Vec<MarketId>,
    pub event_ids: Vec<EventId>,
    pub token_ids: Vec<TokenId>,
    pub category: Option<ChMarketCategory>,
}

#[async_trait::async_trait]
pub trait TimeseriesFactWriter: Send + Sync {
    async fn insert_tick_events(&self, rows: &[TickEventRow]) -> Result<(), StorageError>;

    async fn insert_l2_events(&self, rows: &[TickEventL2Row]) -> Result<(), StorageError>;

    async fn insert_book_snapshots(&self, rows: &[BookSnapshotRow]) -> Result<(), StorageError>;

    async fn insert_detections(&self, rows: &[OpportunityDetectionRow])
    -> Result<(), StorageError>;

    async fn insert_audits(&self, rows: &[OpportunityAuditRow]) -> Result<(), StorageError>;

    async fn insert_calibration_snapshots(
        &self,
        rows: &[CalibrationSnapshotRow],
    ) -> Result<(), StorageError>;
}

#[async_trait::async_trait]
pub trait EvidenceTimeseriesRepository: Send + Sync {
    async fn tick_events(
        &self,
        token_ids: &[TokenId],
        window: TimeWindow,
        limit: u64,
    ) -> Result<Vec<TickEventRow>, StorageError>;

    async fn l2_events(
        &self,
        token_ids: &[TokenId],
        window: TimeWindow,
    ) -> Result<Vec<TickEventL2Row>, StorageError>;

    async fn book_snapshots_before(
        &self,
        token_ids: &[TokenId],
        before: DateTime<Utc>,
        limit_per_token: usize,
    ) -> Result<Vec<BookSnapshotRow>, StorageError>;

    async fn detections(
        &self,
        filter: MarketFilter,
        window: TimeWindow,
    ) -> Result<Vec<OpportunityDetectionRow>, StorageError>;

    async fn audits(
        &self,
        opportunity_ids: &[OpportunityId],
    ) -> Result<Vec<OpportunityAuditRow>, StorageError>;

    async fn calibration_snapshots(
        &self,
        window: TimeWindow,
    ) -> Result<Vec<CalibrationSnapshotRow>, StorageError>;
}
