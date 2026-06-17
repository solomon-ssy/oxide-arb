use crate::traits::{
    AuditFunnelStats, EvidenceTimeseriesRepository, TimeseriesFactWriter, evidence_query_result,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    clickhouse::{
        AuditStageCountRow, BookDecisionContextRow, BookL2ReplayRow, BookMicrostructureRow,
        BookSnapshotRow, CalibrationSnapshotRow, OpportunityAuditRow, OpportunityDetectionRow,
        TickEventRow,
    },
    config::ClickHouseConfig,
    domain::{MarketFilter, PageRequest, Paginated, TimeWindow, evidence::EvidenceQueryResult},
    enums::clickhouse::ChOpportunityAuditStage,
    types::{OpportunityId, TokenId},
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
    l2_replay_inserter: BatchInserter<BookL2ReplayRow>,
    book_inserter: BatchInserter<BookSnapshotRow>,
    decision_context_inserter: BatchInserter<BookDecisionContextRow>,
    microstructure_1s_inserter: BatchInserter<BookMicrostructureRow>,
    audit_inserter: BatchInserter<OpportunityAuditRow>,
    detection_inserter: BatchInserter<OpportunityDetectionRow>,
    calibration_inserter: BatchInserter<CalibrationSnapshotRow>,
}

/// Scalar `count()` projection for paginated total-row queries.
#[derive(clickhouse::Row, serde::Deserialize)]
struct CountRow {
    count: u64,
}

impl ChTimeseriesRepository {
    pub fn new(
        client: clickhouse::Client,
        config: &ClickHouseConfig,
        write_manager: Arc<ChWriteManager>,
        shutdown: CancellationToken,
    ) -> Self {
        let batch_size = config.batch_size;
        let flush_interval = Duration::from_secs(config.flush_interval_secs);

        Self {
            tick_inserter: BatchInserter::new(
                client.clone(),
                "tick_events",
                batch_size,
                flush_interval,
                Arc::clone(&write_manager),
                shutdown.clone(),
            ),
            l2_replay_inserter: BatchInserter::new(
                client.clone(),
                "book_l2_replay_hot",
                batch_size,
                flush_interval,
                Arc::clone(&write_manager),
                shutdown.clone(),
            ),
            book_inserter: BatchInserter::new(
                client.clone(),
                "book_snapshots",
                batch_size,
                flush_interval,
                Arc::clone(&write_manager),
                shutdown.clone(),
            ),
            decision_context_inserter: BatchInserter::new(
                client.clone(),
                "book_decision_contexts",
                batch_size,
                flush_interval,
                Arc::clone(&write_manager),
                shutdown.clone(),
            ),
            microstructure_1s_inserter: BatchInserter::new(
                client.clone(),
                "book_microstructure_1s",
                batch_size,
                flush_interval,
                Arc::clone(&write_manager),
                shutdown.clone(),
            ),
            audit_inserter: BatchInserter::new(
                client.clone(),
                "opportunity_audit",
                batch_size,
                flush_interval,
                Arc::clone(&write_manager),
                shutdown.clone(),
            ),
            detection_inserter: BatchInserter::new(
                client.clone(),
                "opportunity_detection",
                batch_size,
                flush_interval,
                Arc::clone(&write_manager),
                shutdown.clone(),
            ),
            calibration_inserter: BatchInserter::new(
                client.clone(),
                "calibration_snapshots",
                batch_size,
                flush_interval,
                Arc::clone(&write_manager),
                shutdown,
            ),
            client,
            write_manager,
        }
    }

    pub fn write_metrics(&self) -> &Arc<ChWriteMetrics> {
        self.write_manager.metrics()
    }
}

#[async_trait]
impl TimeseriesFactWriter for ChTimeseriesRepository {
    async fn insert_tick_events(&self, events: Vec<TickEventRow>) -> Result<(), StorageError> {
        self.tick_inserter.insert_batch(events).await
    }

    async fn insert_book_l2_replay(&self, rows: Vec<BookL2ReplayRow>) -> Result<(), StorageError> {
        self.l2_replay_inserter.insert_batch(rows).await
    }

    async fn insert_book_snapshots(&self, rows: Vec<BookSnapshotRow>) -> Result<(), StorageError> {
        self.book_inserter.insert_batch(rows).await
    }

    async fn insert_book_decision_contexts(
        &self,
        rows: Vec<BookDecisionContextRow>,
    ) -> Result<(), StorageError> {
        self.decision_context_inserter.insert_batch(rows).await
    }

    async fn insert_book_microstructure_1s(
        &self,
        rows: Vec<BookMicrostructureRow>,
    ) -> Result<(), StorageError> {
        self.microstructure_1s_inserter.insert_batch(rows).await
    }

    async fn insert_detections(
        &self,
        rows: Vec<OpportunityDetectionRow>,
    ) -> Result<(), StorageError> {
        self.detection_inserter.insert_batch(rows).await
    }

    async fn insert_audits(&self, rows: Vec<OpportunityAuditRow>) -> Result<(), StorageError> {
        self.audit_inserter.insert_batch(rows).await
    }

    async fn insert_calibration_snapshots(
        &self,
        rows: Vec<CalibrationSnapshotRow>,
    ) -> Result<(), StorageError> {
        self.calibration_inserter.insert_batch(rows).await
    }
}

#[async_trait]
impl EvidenceTimeseriesRepository for ChTimeseriesRepository {
    async fn tick_events(
        &self,
        token_ids: &[TokenId],
        window: TimeWindow,
        limit: u64,
    ) -> Result<EvidenceQueryResult<TickEventRow>, StorageError> {
        let rows = self
            .client
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
            .map_err(StorageError::from)?;
        evidence_query_result(
            "ChTimeseriesRepository",
            "tick_events",
            &(token_ids, window, limit),
            vec![
                "event_time ASC".to_owned(),
                "ingestion_time ASC".to_owned(),
                "sequence ASC".to_owned(),
            ],
            Some(2),
            rows,
        )
    }

    async fn book_l2_replay(
        &self,
        token_ids: &[TokenId],
        window: TimeWindow,
    ) -> Result<EvidenceQueryResult<BookL2ReplayRow>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT * FROM book_l2_replay_hot \
                 WHERE token_id IN ? \
                   AND event_time >= fromUnixTimestamp64Milli(?) \
                   AND event_time < fromUnixTimestamp64Milli(?) \
                 ORDER BY event_time ASC, ingestion_time ASC, sequence ASC",
            )
            .bind(token_ids)
            .bind(window.from.timestamp_millis())
            .bind(window.to.timestamp_millis())
            .fetch_all::<BookL2ReplayRow>()
            .await
            .map_err(StorageError::from)?;
        evidence_query_result(
            "ChTimeseriesRepository",
            "book_l2_replay",
            &(token_ids, window),
            vec![
                "event_time ASC".to_owned(),
                "ingestion_time ASC".to_owned(),
                "sequence ASC".to_owned(),
            ],
            Some(2),
            rows,
        )
    }

    async fn book_snapshots_before(
        &self,
        token_ids: &[TokenId],
        before: DateTime<Utc>,
        limit_per_token: usize,
    ) -> Result<EvidenceQueryResult<BookSnapshotRow>, StorageError> {
        let rows = self
            .client
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
            .map_err(StorageError::from)?;
        evidence_query_result(
            "ChTimeseriesRepository",
            "book_snapshots_before",
            &(token_ids, before, limit_per_token),
            vec![
                "token_id ASC".to_owned(),
                "event_time DESC".to_owned(),
                "ingestion_time DESC".to_owned(),
                "sequence DESC".to_owned(),
                "LIMIT BY token_id".to_owned(),
            ],
            Some(2),
            rows,
        )
    }

    async fn book_decision_contexts(
        &self,
        filter: MarketFilter,
        window: TimeWindow,
    ) -> Result<EvidenceQueryResult<BookDecisionContextRow>, StorageError> {
        let market_ids = filter.market_ids;
        let token_ids = filter.token_ids;
        let params = (market_ids.clone(), token_ids.clone(), window);
        let rows = self
            .client
            .query(
                "SELECT * FROM book_decision_contexts \
                 WHERE decision_time >= fromUnixTimestamp64Milli(?) \
                   AND decision_time < fromUnixTimestamp64Milli(?) \
                   AND (empty(?) OR market_id IN ?) \
                   AND (empty(?) OR yes_token_id IN ? OR no_token_id IN ?) \
                 ORDER BY decision_time ASC, ingestion_time ASC, sequence ASC, context_id ASC",
            )
            .bind(window.from.timestamp_millis())
            .bind(window.to.timestamp_millis())
            .bind(market_ids.clone())
            .bind(market_ids)
            .bind(token_ids.clone())
            .bind(token_ids.clone())
            .bind(token_ids)
            .fetch_all::<BookDecisionContextRow>()
            .await
            .map_err(StorageError::from)?;
        evidence_query_result(
            "ChTimeseriesRepository",
            "book_decision_contexts",
            &params,
            vec![
                "decision_time ASC".to_owned(),
                "ingestion_time ASC".to_owned(),
                "sequence ASC".to_owned(),
                "context_id ASC".to_owned(),
            ],
            Some(1),
            rows,
        )
    }

    async fn book_microstructure_1m(
        &self,
        token_ids: &[TokenId],
        window: TimeWindow,
    ) -> Result<EvidenceQueryResult<BookMicrostructureRow>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT * FROM book_microstructure_1m \
                 WHERE token_id IN ? \
                   AND bucket_time >= fromUnixTimestamp64Milli(?) \
                   AND bucket_time < fromUnixTimestamp64Milli(?) \
                 ORDER BY token_id ASC, bucket_time ASC",
            )
            .bind(token_ids)
            .bind(window.from.timestamp_millis())
            .bind(window.to.timestamp_millis())
            .fetch_all::<BookMicrostructureRow>()
            .await
            .map_err(StorageError::from)?;
        evidence_query_result(
            "ChTimeseriesRepository",
            "book_microstructure_1m",
            &(token_ids, window),
            vec!["token_id ASC".to_owned(), "bucket_time ASC".to_owned()],
            Some(1),
            rows,
        )
    }

    async fn detections(
        &self,
        filter: MarketFilter,
        window: TimeWindow,
    ) -> Result<EvidenceQueryResult<OpportunityDetectionRow>, StorageError> {
        let market_ids = filter.market_ids;
        let event_ids = filter.event_ids;
        let token_ids = filter.token_ids;
        let categories = filter.categories;
        let params = (
            market_ids.clone(),
            event_ids.clone(),
            token_ids.clone(),
            categories.clone(),
            window,
        );
        let rows = self
            .client
            .query(
                "SELECT * FROM opportunity_detection \
                 WHERE detected_at >= fromUnixTimestamp64Milli(?) \
                   AND detected_at < fromUnixTimestamp64Milli(?) \
                   AND (empty(?) OR market_id IN ?) \
                   AND (empty(?) OR event_id IN ?) \
                   AND (empty(?) OR token_id IN ?) \
                   AND (empty(?) OR category IN ?) \
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
            .bind(categories.clone())
            .bind(categories)
            .fetch_all::<OpportunityDetectionRow>()
            .await
            .map_err(StorageError::from)?;
        evidence_query_result(
            "ChTimeseriesRepository",
            "detections",
            &params,
            vec![
                "detected_at ASC".to_owned(),
                "ingestion_time ASC".to_owned(),
                "sequence ASC".to_owned(),
            ],
            Some(2),
            rows,
        )
    }

    async fn detections_page(
        &self,
        filter: MarketFilter,
        window: TimeWindow,
        page: PageRequest,
    ) -> Result<Paginated<OpportunityDetectionRow>, StorageError> {
        let market_ids = filter.market_ids;
        let event_ids = filter.event_ids;
        let token_ids = filter.token_ids;
        let categories = filter.categories;
        let window_filter = "detected_at >= fromUnixTimestamp64Milli(?) \
             AND detected_at < fromUnixTimestamp64Milli(?) \
             AND (empty(?) OR market_id IN ?) \
             AND (empty(?) OR event_id IN ?) \
             AND (empty(?) OR token_id IN ?) \
             AND (empty(?) OR category IN ?)";
        let total = self
            .client
            .query(&format!(
                "SELECT count() AS count FROM opportunity_detection WHERE {window_filter}"
            ))
            .bind(window.from.timestamp_millis())
            .bind(window.to.timestamp_millis())
            .bind(market_ids.clone())
            .bind(market_ids.clone())
            .bind(event_ids.clone())
            .bind(event_ids.clone())
            .bind(token_ids.clone())
            .bind(token_ids.clone())
            .bind(categories.clone())
            .bind(categories.clone())
            .fetch_one::<CountRow>()
            .await
            .map_err(StorageError::from)?
            .count;
        let rows = self
            .client
            .query(&format!(
                "SELECT * FROM opportunity_detection WHERE {window_filter} \
                 ORDER BY detected_at ASC, ingestion_time ASC, sequence ASC \
                 LIMIT ? OFFSET ?"
            ))
            .bind(window.from.timestamp_millis())
            .bind(window.to.timestamp_millis())
            .bind(market_ids.clone())
            .bind(market_ids)
            .bind(event_ids.clone())
            .bind(event_ids)
            .bind(token_ids.clone())
            .bind(token_ids)
            .bind(categories.clone())
            .bind(categories)
            .bind(page.limit())
            .bind(page.offset())
            .fetch_all::<OpportunityDetectionRow>()
            .await
            .map_err(StorageError::from)?;
        Ok(Paginated::from_request(rows, total, &page))
    }

    async fn audits(
        &self,
        opportunity_ids: &[OpportunityId],
    ) -> Result<EvidenceQueryResult<OpportunityAuditRow>, StorageError> {
        let rows = self
            .client
            .query(
                "SELECT * FROM opportunity_audit \
                 WHERE opportunity_id IN ? \
                 ORDER BY stage_at ASC, ingestion_time ASC, sequence ASC",
            )
            .bind(opportunity_ids)
            .fetch_all::<OpportunityAuditRow>()
            .await
            .map_err(StorageError::from)?;
        evidence_query_result(
            "ChTimeseriesRepository",
            "audits",
            &opportunity_ids,
            vec![
                "stage_at ASC".to_owned(),
                "ingestion_time ASC".to_owned(),
                "sequence ASC".to_owned(),
            ],
            Some(2),
            rows,
        )
    }

    async fn terminal_audits(
        &self,
        opportunity_ids: &[OpportunityId],
    ) -> Result<EvidenceQueryResult<OpportunityAuditRow>, StorageError> {
        let terminal_stages = vec![
            ChOpportunityAuditStage::Filled,
            ChOpportunityAuditStage::Missed,
            ChOpportunityAuditStage::Failed,
        ];
        let params = (opportunity_ids, terminal_stages.clone());
        let rows = self
            .client
            .query(
                "SELECT * FROM opportunity_audit FINAL \
                 WHERE opportunity_id IN ? \
                   AND stage IN ? \
                 ORDER BY opportunity_id ASC, stage_order DESC, stage_at DESC, ingestion_time DESC, sequence DESC \
                 LIMIT 1 BY opportunity_id",
            )
            .bind(opportunity_ids)
            .bind(terminal_stages)
            .fetch_all::<OpportunityAuditRow>()
            .await
            .map_err(StorageError::from)?;
        evidence_query_result(
            "ChTimeseriesRepository",
            "terminal_audits",
            &params,
            vec![
                "opportunity_id ASC".to_owned(),
                "stage_order DESC".to_owned(),
                "stage_at DESC".to_owned(),
                "ingestion_time DESC".to_owned(),
                "sequence DESC".to_owned(),
                "LIMIT BY opportunity_id".to_owned(),
            ],
            Some(2),
            rows,
        )
    }

    async fn audit_funnel(
        &self,
        filter: MarketFilter,
        window: TimeWindow,
    ) -> Result<EvidenceQueryResult<OpportunityAuditRow>, StorageError> {
        let market_ids = filter.market_ids;
        let event_ids = filter.event_ids;
        let token_ids = filter.token_ids;
        let categories = filter.categories;
        let params = (
            market_ids.clone(),
            event_ids.clone(),
            token_ids.clone(),
            categories.clone(),
            window,
        );
        let rows = self
            .client
            .query(
                "SELECT * FROM opportunity_audit FINAL \
                 WHERE detected_at >= fromUnixTimestamp64Milli(?) \
                   AND detected_at < fromUnixTimestamp64Milli(?) \
                   AND (empty(?) OR market_id IN ?) \
                   AND (empty(?) OR event_id IN ?) \
                   AND (empty(?) OR token_id IN ?) \
                   AND (empty(?) OR category IN ?) \
                 ORDER BY stage_at ASC, ingestion_time ASC, sequence ASC",
            )
            .bind(window.from.timestamp_millis())
            .bind(window.to.timestamp_millis())
            .bind(market_ids.clone())
            .bind(market_ids)
            .bind(event_ids.clone())
            .bind(event_ids)
            .bind(token_ids.clone())
            .bind(token_ids)
            .bind(categories.clone())
            .bind(categories)
            .fetch_all::<OpportunityAuditRow>()
            .await
            .map_err(StorageError::from)?;
        evidence_query_result(
            "ChTimeseriesRepository",
            "audit_funnel",
            &params,
            vec![
                "stage_at ASC".to_owned(),
                "ingestion_time ASC".to_owned(),
                "sequence ASC".to_owned(),
            ],
            Some(2),
            rows,
        )
    }

    async fn audit_funnel_page(
        &self,
        filter: MarketFilter,
        window: TimeWindow,
        page: PageRequest,
    ) -> Result<Paginated<OpportunityAuditRow>, StorageError> {
        let market_ids = filter.market_ids;
        let event_ids = filter.event_ids;
        let token_ids = filter.token_ids;
        let categories = filter.categories;
        let window_filter = "detected_at >= fromUnixTimestamp64Milli(?) \
             AND detected_at < fromUnixTimestamp64Milli(?) \
             AND (empty(?) OR market_id IN ?) \
             AND (empty(?) OR event_id IN ?) \
             AND (empty(?) OR token_id IN ?) \
             AND (empty(?) OR category IN ?)";
        let total = self
            .client
            .query(&format!(
                "SELECT count() AS count FROM opportunity_audit FINAL WHERE {window_filter}"
            ))
            .bind(window.from.timestamp_millis())
            .bind(window.to.timestamp_millis())
            .bind(market_ids.clone())
            .bind(market_ids.clone())
            .bind(event_ids.clone())
            .bind(event_ids.clone())
            .bind(token_ids.clone())
            .bind(token_ids.clone())
            .bind(categories.clone())
            .bind(categories.clone())
            .fetch_one::<CountRow>()
            .await
            .map_err(StorageError::from)?
            .count;
        let rows = self
            .client
            .query(&format!(
                "SELECT * FROM opportunity_audit FINAL WHERE {window_filter} \
                 ORDER BY stage_at ASC, ingestion_time ASC, sequence ASC \
                 LIMIT ? OFFSET ?"
            ))
            .bind(window.from.timestamp_millis())
            .bind(window.to.timestamp_millis())
            .bind(market_ids.clone())
            .bind(market_ids)
            .bind(event_ids.clone())
            .bind(event_ids)
            .bind(token_ids.clone())
            .bind(token_ids)
            .bind(categories.clone())
            .bind(categories)
            .bind(page.limit())
            .bind(page.offset())
            .fetch_all::<OpportunityAuditRow>()
            .await
            .map_err(StorageError::from)?;
        Ok(Paginated::from_request(rows, total, &page))
    }

    async fn audit_funnel_stats(
        &self,
        filter: MarketFilter,
        window: TimeWindow,
    ) -> Result<AuditFunnelStats, StorageError> {
        let market_ids = filter.market_ids;
        let event_ids = filter.event_ids;
        let token_ids = filter.token_ids;
        let categories = filter.categories;
        let window_filter = "detected_at >= fromUnixTimestamp64Milli(?) \
             AND detected_at < fromUnixTimestamp64Milli(?) \
             AND (empty(?) OR market_id IN ?) \
             AND (empty(?) OR event_id IN ?) \
             AND (empty(?) OR token_id IN ?) \
             AND (empty(?) OR category IN ?)";
        let total_detected = self
            .client
            .query(&format!(
                "SELECT uniqExact(opportunity_id) AS count \
                 FROM opportunity_detection WHERE {window_filter}"
            ))
            .bind(window.from.timestamp_millis())
            .bind(window.to.timestamp_millis())
            .bind(market_ids.clone())
            .bind(market_ids.clone())
            .bind(event_ids.clone())
            .bind(event_ids.clone())
            .bind(token_ids.clone())
            .bind(token_ids.clone())
            .bind(categories.clone())
            .bind(categories.clone())
            .fetch_one::<CountRow>()
            .await
            .map_err(StorageError::from)?
            .count;
        let stages = self
            .client
            .query(&format!(
                "SELECT stage, uniqExact(opportunity_id) AS count \
                 FROM opportunity_audit FINAL WHERE {window_filter} \
                 GROUP BY stage \
                 ORDER BY stage ASC"
            ))
            .bind(window.from.timestamp_millis())
            .bind(window.to.timestamp_millis())
            .bind(market_ids.clone())
            .bind(market_ids)
            .bind(event_ids.clone())
            .bind(event_ids)
            .bind(token_ids.clone())
            .bind(token_ids)
            .bind(categories.clone())
            .bind(categories)
            .fetch_all::<AuditStageCountRow>()
            .await
            .map_err(StorageError::from)?;
        Ok(AuditFunnelStats {
            total_detected,
            stages,
        })
    }

    async fn calibration_snapshots(
        &self,
        window: TimeWindow,
    ) -> Result<EvidenceQueryResult<CalibrationSnapshotRow>, StorageError> {
        let rows = self
            .client
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
            .map_err(StorageError::from)?;
        evidence_query_result(
            "ChTimeseriesRepository",
            "calibration_snapshots",
            &window,
            vec![
                "event_time ASC".to_owned(),
                "ingestion_time ASC".to_owned(),
                "sequence ASC".to_owned(),
            ],
            Some(2),
            rows,
        )
    }
}
