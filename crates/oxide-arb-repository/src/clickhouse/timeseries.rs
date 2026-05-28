use crate::traits::TimeseriesRepository;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    clickhouse::{
        BookSnapshotRow, CalibrationSnapshotRow, OpportunityAuditRow, OpportunityDetectionRow,
        TickEventRow,
    },
    config::AnalyticsConfig,
};
use oxide_arb_storage::clickhouse::{BatchInserter, ChWriteManager, ChWriteMetrics};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

/// `ClickHouse` timeseries repository backed by per-table `BatchInserter` instances.
///
/// Writes are non-blocking: rows are enqueued into the appropriate inserter's
/// channel and flushed in batches by background tasks. The `ChWriteManager`
/// provides semaphore-based concurrency control and lag-based backpressure.
pub struct ChTimeseriesRepository {
    client: clickhouse::Client,
    write_manager: Arc<ChWriteManager>,
    tick_inserter: BatchInserter<TickEventRow>,
    book_inserter: BatchInserter<BookSnapshotRow>,
    audit_inserter: BatchInserter<OpportunityAuditRow>,
    detection_inserter: BatchInserter<OpportunityDetectionRow>,
    calibration_inserter: BatchInserter<CalibrationSnapshotRow>,
}

impl ChTimeseriesRepository {
    pub fn new(
        client: clickhouse::Client,
        config: &AnalyticsConfig,
        write_manager: Arc<ChWriteManager>,
        shutdown: CancellationToken,
    ) -> Self {
        let batch_size = config.batch_size;
        let flush_interval = Duration::from_secs(config.flush_interval_secs);
        let metrics = write_manager.metrics().clone();

        Self {
            tick_inserter: BatchInserter::new(
                client.clone(),
                "tick_events",
                batch_size,
                flush_interval,
                metrics.clone(),
                shutdown.clone(),
            ),
            book_inserter: BatchInserter::new(
                client.clone(),
                "book_snapshots",
                batch_size,
                flush_interval,
                metrics.clone(),
                shutdown.clone(),
            ),
            audit_inserter: BatchInserter::new(
                client.clone(),
                "opportunity_audit",
                batch_size,
                flush_interval,
                metrics.clone(),
                shutdown.clone(),
            ),
            detection_inserter: BatchInserter::new(
                client.clone(),
                "opportunity_detection",
                batch_size,
                flush_interval,
                metrics.clone(),
                shutdown.clone(),
            ),
            calibration_inserter: BatchInserter::new(
                client.clone(),
                "calibration_snapshots",
                batch_size,
                flush_interval,
                metrics,
                shutdown,
            ),
            client,
            write_manager,
        }
    }

    pub fn write_metrics(&self) -> &Arc<ChWriteMetrics> {
        self.write_manager.metrics()
    }

    pub fn is_lagging(&self) -> bool {
        self.write_manager.is_lagging()
    }
}

#[async_trait]
impl TimeseriesRepository for ChTimeseriesRepository {
    async fn insert_tick_events(&self, events: &[TickEventRow]) -> Result<(), StorageError> {
        for event in events {
            self.tick_inserter.insert(event.clone()).await?;
        }
        Ok(())
    }

    async fn insert_book_snapshot(&self, snapshot: &BookSnapshotRow) -> Result<(), StorageError> {
        self.book_inserter.insert(snapshot.clone()).await
    }

    async fn insert_opportunity_audit(
        &self,
        audit: &OpportunityAuditRow,
    ) -> Result<(), StorageError> {
        self.audit_inserter.insert(audit.clone()).await
    }

    async fn insert_calibration_snapshot(
        &self,
        snapshot: &CalibrationSnapshotRow,
    ) -> Result<(), StorageError> {
        self.calibration_inserter.insert(snapshot.clone()).await
    }

    async fn insert_detection_batch(
        &self,
        rows: &[OpportunityDetectionRow],
    ) -> Result<(), StorageError> {
        for row in rows {
            self.detection_inserter.insert(row.clone()).await?;
        }
        Ok(())
    }

    async fn query_tick_events(
        &self,
        token_id: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<TickEventRow>, StorageError> {
        self.client
            .query(
                "SELECT * FROM tick_events \
                 WHERE token_id = ? AND received_at >= ? AND received_at < ? \
                 ORDER BY received_at DESC LIMIT ?",
            )
            .bind(token_id)
            .bind(from.timestamp())
            .bind(to.timestamp())
            .bind(limit)
            .fetch_all::<TickEventRow>()
            .await
            .map_err(Into::into)
    }

    async fn query_opportunity_audit(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<OpportunityAuditRow>, StorageError> {
        self.client
            .query(
                "SELECT * FROM opportunity_audit \
                 WHERE detected_at >= ? AND detected_at < ? \
                 ORDER BY detected_at DESC",
            )
            .bind(from.timestamp())
            .bind(to.timestamp())
            .fetch_all::<OpportunityAuditRow>()
            .await
            .map_err(Into::into)
    }

    async fn query_opportunity_lifecycle(
        &self,
        opportunity_id: &str,
    ) -> Result<Vec<OpportunityAuditRow>, StorageError> {
        self.client
            .query(
                "SELECT * FROM opportunity_audit \
                 WHERE opportunity_id = ? \
                 ORDER BY stage_order ASC, stage_at ASC, updated_at ASC",
            )
            .bind(opportunity_id)
            .fetch_all::<OpportunityAuditRow>()
            .await
            .map_err(Into::into)
    }

    async fn query_calibration_history(
        &self,
        category: &str,
        price_zone: &str,
        duration_bucket: &str,
        days: u32,
    ) -> Result<Vec<CalibrationSnapshotRow>, StorageError> {
        self.client
            .query(
                "SELECT * FROM calibration_snapshots \
                 WHERE category = ? AND price_zone = ? AND duration_bucket = ? \
                   AND snapshot_time >= now() - INTERVAL ? DAY \
                 ORDER BY snapshot_time DESC",
            )
            .bind(category)
            .bind(price_zone)
            .bind(duration_bucket)
            .bind(days)
            .fetch_all::<CalibrationSnapshotRow>()
            .await
            .map_err(Into::into)
    }
}
