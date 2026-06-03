use crate::traits::{EvidenceTimeseriesRepository, MarketFilter, TimeWindow, TimeseriesFactWriter};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    clickhouse::{
        BookSnapshotRow, CalibrationSnapshotRow, OpportunityAuditRow, OpportunityDetectionRow,
        TickEventL2Row, TickEventRow,
    },
    config::AnalyticsConfig,
    types::{EventId, MarketId, OpportunityId, TokenId},
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
    l2_inserter: BatchInserter<TickEventL2Row>,
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
            l2_inserter: BatchInserter::new(
                client.clone(),
                "tick_events_l2",
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
impl TimeseriesFactWriter for ChTimeseriesRepository {
    async fn insert_tick_events(&self, events: &[TickEventRow]) -> Result<(), StorageError> {
        for event in events {
            self.tick_inserter.insert(event.clone()).await?;
        }
        Ok(())
    }

    async fn insert_l2_events(&self, rows: &[TickEventL2Row]) -> Result<(), StorageError> {
        for row in rows {
            self.l2_inserter.insert(row.clone()).await?;
        }
        Ok(())
    }

    async fn insert_book_snapshots(&self, rows: &[BookSnapshotRow]) -> Result<(), StorageError> {
        for row in rows {
            self.book_inserter.insert(row.clone()).await?;
        }
        Ok(())
    }

    async fn insert_detections(
        &self,
        rows: &[OpportunityDetectionRow],
    ) -> Result<(), StorageError> {
        for row in rows {
            self.detection_inserter.insert(row.clone()).await?;
        }
        Ok(())
    }

    async fn insert_audits(&self, rows: &[OpportunityAuditRow]) -> Result<(), StorageError> {
        for row in rows {
            self.audit_inserter.insert(row.clone()).await?;
        }
        Ok(())
    }

    async fn insert_calibration_snapshots(
        &self,
        rows: &[CalibrationSnapshotRow],
    ) -> Result<(), StorageError> {
        for row in rows {
            self.calibration_inserter.insert(row.clone()).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl EvidenceTimeseriesRepository for ChTimeseriesRepository {
    async fn tick_events(
        &self,
        token_ids: &[TokenId],
        window: TimeWindow,
        limit: u64,
    ) -> Result<Vec<TickEventRow>, StorageError> {
        let token_ids = token_ids_as_strings(token_ids);
        self.client
            .query(
                "SELECT * FROM tick_events \
                 WHERE token_id IN ? \
                   AND event_time >= fromUnixTimestamp64Milli(?) \
                   AND event_time < fromUnixTimestamp64Milli(?) \
                 ORDER BY event_time ASC, ingestion_time ASC, sequence ASC \
                 LIMIT ?",
            )
            .bind(token_ids)
            .bind(window.from.timestamp_millis())
            .bind(window.to.timestamp_millis())
            .bind(limit)
            .fetch_all::<TickEventRow>()
            .await
            .map_err(Into::into)
    }

    async fn l2_events(
        &self,
        token_ids: &[TokenId],
        window: TimeWindow,
    ) -> Result<Vec<TickEventL2Row>, StorageError> {
        let token_ids = token_ids_as_strings(token_ids);
        self.client
            .query(
                "SELECT * FROM tick_events_l2 \
                 WHERE token_id IN ? \
                   AND event_time >= fromUnixTimestamp64Milli(?) \
                   AND event_time < fromUnixTimestamp64Milli(?) \
                 ORDER BY event_time ASC, ingestion_time ASC, sequence ASC",
            )
            .bind(token_ids)
            .bind(window.from.timestamp_millis())
            .bind(window.to.timestamp_millis())
            .fetch_all::<TickEventL2Row>()
            .await
            .map_err(Into::into)
    }

    async fn book_snapshots_before(
        &self,
        token_ids: &[TokenId],
        before: DateTime<Utc>,
        limit_per_token: usize,
    ) -> Result<Vec<BookSnapshotRow>, StorageError> {
        let token_ids = token_ids_as_strings(token_ids);
        self.client
            .query(
                "SELECT * FROM book_snapshots \
                 WHERE token_id IN ? \
                   AND event_time < fromUnixTimestamp64Milli(?) \
                 ORDER BY token_id ASC, event_time DESC, ingestion_time DESC, sequence DESC \
                 LIMIT ? BY token_id",
            )
            .bind(token_ids)
            .bind(before.timestamp_millis())
            .bind(u64::try_from(limit_per_token).unwrap_or(u64::MAX))
            .fetch_all::<BookSnapshotRow>()
            .await
            .map_err(Into::into)
    }

    async fn detections(
        &self,
        filter: MarketFilter,
        window: TimeWindow,
    ) -> Result<Vec<OpportunityDetectionRow>, StorageError> {
        let market_ids = market_ids_as_strings(&filter.market_ids);
        let event_ids = event_ids_as_strings(&filter.event_ids);
        let token_ids = token_ids_as_strings(&filter.token_ids);
        self.client
            .query(
                "SELECT * FROM opportunity_detection \
                 WHERE detected_at >= fromUnixTimestamp64Milli(?) \
                   AND detected_at < fromUnixTimestamp64Milli(?) \
                   AND (empty(?) OR market_id IN ?) \
                   AND (empty(?) OR event_id IN ?) \
                   AND (empty(?) OR token_id IN ?) \
                   AND (? IS NULL OR category = ?) \
                 ORDER BY detected_at ASC, ingestion_time ASC, sequence ASC",
            )
            .bind(window.from.timestamp_millis())
            .bind(window.to.timestamp_millis())
            .bind(market_ids.clone())
            .bind(market_ids)
            .bind(event_ids.clone())
            .bind(event_ids)
            .bind(token_ids.clone())
            .bind(token_ids)
            .bind(filter.category)
            .bind(filter.category)
            .fetch_all::<OpportunityDetectionRow>()
            .await
            .map_err(Into::into)
    }

    async fn audits(
        &self,
        opportunity_ids: &[OpportunityId],
    ) -> Result<Vec<OpportunityAuditRow>, StorageError> {
        let opportunity_ids = opportunity_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        self.client
            .query(
                "SELECT * FROM opportunity_audit \
                 WHERE opportunity_id IN ? \
                 ORDER BY stage_at ASC, ingestion_time ASC, sequence ASC",
            )
            .bind(opportunity_ids)
            .fetch_all::<OpportunityAuditRow>()
            .await
            .map_err(Into::into)
    }

    async fn calibration_snapshots(
        &self,
        window: TimeWindow,
    ) -> Result<Vec<CalibrationSnapshotRow>, StorageError> {
        self.client
            .query(
                "SELECT * FROM calibration_snapshots \
                 WHERE event_time >= fromUnixTimestamp64Milli(?) \
                   AND event_time < fromUnixTimestamp64Milli(?) \
                 ORDER BY event_time ASC, ingestion_time ASC, sequence ASC",
            )
            .bind(window.from.timestamp_millis())
            .bind(window.to.timestamp_millis())
            .fetch_all::<CalibrationSnapshotRow>()
            .await
            .map_err(Into::into)
    }
}

fn token_ids_as_strings(token_ids: &[TokenId]) -> Vec<String> {
    token_ids.iter().map(ToString::to_string).collect()
}

fn market_ids_as_strings(market_ids: &[MarketId]) -> Vec<String> {
    market_ids.iter().map(ToString::to_string).collect()
}

fn event_ids_as_strings(event_ids: &[EventId]) -> Vec<String> {
    event_ids.iter().map(ToString::to_string).collect()
}
