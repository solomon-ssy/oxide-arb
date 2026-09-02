//! `ClickHouse` write concurrency control.
//!
//! The `ChWriteManager` wraps a counting semaphore limiting concurrent insert
//! operations plus Prometheus metrics for observability (rows written, insert
//! durations/errors, permits in use). Backpressure against an overloaded
//! server is provided by the semaphore: batched inserts queue on permits
//! instead of piling additional requests onto a slow `ClickHouse`.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use clickhouse::{Client, RowOwned, RowWrite, error::Error as ClickHouseError};
use num_traits::ToPrimitive;
use prometheus::{Error, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::BookL2LedgerRow,
    config::{
        CLICKHOUSE_INSERT_MAX_ATTEMPTS, CLICKHOUSE_INSERT_RETRY_BACKOFF_BASE_MS, ClickHouseIoConfig,
    },
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::timeout,
};
use tracing::{error, warn};

use super::deadline::{ClickHouseIoDeadlines, InsertDeadlines};

const CRITICAL_WRITE_PERMITS: usize = 1;
const CANONICAL_LEDGER_TABLE: &str = "quant_book_l2_ledger";
const WRITE_LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChWriteLane {
    Critical,
    Bulk,
}

impl ChWriteLane {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::Bulk => "bulk",
        }
    }

    const fn server_priority(self) -> &'static str {
        match self {
            Self::Critical => "1",
            Self::Bulk => "4",
        }
    }

    const fn concurrency_control(self) -> &'static str {
        match self {
            Self::Critical => "0",
            Self::Bulk => "1",
        }
    }

    const fn query_log_probability(self) -> &'static str {
        match self {
            Self::Critical => "1",
            Self::Bulk => "0.01",
        }
    }

    const fn attempt_operation(self) -> &'static str {
        match self {
            Self::Critical => "clickhouse.insert.critical.attempt",
            Self::Bulk => "clickhouse.insert.bulk.attempt",
        }
    }

    const fn send_operation(self) -> &'static str {
        match self {
            Self::Critical => "clickhouse.insert.critical.send",
            Self::Bulk => "clickhouse.insert.bulk.send",
        }
    }

    const fn end_operation(self) -> &'static str {
        match self {
            Self::Critical => "clickhouse.insert.critical.end",
            Self::Bulk => "clickhouse.insert.bulk.end",
        }
    }
}

/// Metrics specific to `ClickHouse` write operations.
pub struct ChWriteMetrics {
    pub rows_written: IntCounterVec,
    pub insert_duration_seconds: HistogramVec,
    pub permit_wait_seconds: HistogramVec,
    pub insert_errors: IntCounterVec,
    pub semaphore_permits_used: IntGaugeVec,
}

impl ChWriteMetrics {
    pub fn new() -> Self {
        Self {
            rows_written: IntCounterVec::new(
                Opts::new("ch_rows_written_total", "Total rows written to ClickHouse"),
                &["table", "lane"],
            )
            .expect("ch_rows_written_total"),
            insert_duration_seconds: HistogramVec::new(
                HistogramOpts::new(
                    "ch_insert_duration_seconds",
                    "ClickHouse insert batch end-to-end duration including lane wait, retries, and backoff",
                )
                .buckets(WRITE_LATENCY_BUCKETS.to_vec()),
                &["table", "lane"],
            )
            .expect("ch_insert_duration_seconds"),
            permit_wait_seconds: HistogramVec::new(
                HistogramOpts::new(
                    "ch_write_permit_wait_seconds",
                    "Time waiting for a ClickHouse write-lane permit",
                )
                .buckets(WRITE_LATENCY_BUCKETS.to_vec()),
                &["lane"],
            )
            .expect("ch_write_permit_wait_seconds"),
            insert_errors: IntCounterVec::new(
                Opts::new("ch_insert_errors_total", "Total ClickHouse insert errors"),
                &["table", "lane"],
            )
            .expect("ch_insert_errors_total"),
            semaphore_permits_used: IntGaugeVec::new(
                Opts::new(
                    "ch_semaphore_permits_used",
                    "Currently held ClickHouse write permits by lane",
                ),
                &["lane"],
            )
            .expect("ch_semaphore_permits_used"),
        }
    }

    pub fn register(&self, registry: &Registry) -> Result<(), Error> {
        registry.register(Box::new(self.rows_written.clone()))?;
        registry.register(Box::new(self.insert_duration_seconds.clone()))?;
        registry.register(Box::new(self.permit_wait_seconds.clone()))?;
        registry.register(Box::new(self.insert_errors.clone()))?;
        registry.register(Box::new(self.semaphore_permits_used.clone()))?;
        Ok(())
    }
}

impl Default for ChWriteMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Manages concurrent write access to `ClickHouse`.
pub struct ChWriteManager {
    critical_semaphore: Arc<Semaphore>,
    bulk_semaphore: Arc<Semaphore>,
    metrics: Arc<ChWriteMetrics>,
    deadlines: ClickHouseIoDeadlines,
}

impl ChWriteManager {
    /// Create a write manager with the given insert concurrency
    /// (`db.clickhouse.max_concurrent_inserts`). The validated deployment
    /// contract reserves one permit for the canonical ledger and assigns the
    /// remainder to bulk/telemetry writers.
    pub fn new(max_concurrent: usize, io: &ClickHouseIoConfig) -> Self {
        let bulk_permits = max_concurrent.saturating_sub(CRITICAL_WRITE_PERMITS);
        Self {
            critical_semaphore: Arc::new(Semaphore::new(CRITICAL_WRITE_PERMITS)),
            bulk_semaphore: Arc::new(Semaphore::new(bulk_permits)),
            metrics: Arc::new(ChWriteMetrics::new()),
            deadlines: ClickHouseIoDeadlines::from(io),
        }
    }

    /// Acquire a write permit. Awaits when the semaphore is exhausted.
    /// Returns the permit guard; dropping it releases the permit.
    async fn acquire_write_permit(&self, lane: ChWriteLane) -> Result<WritePermit, StorageError> {
        let started = Instant::now();
        let lane_semaphore = match lane {
            ChWriteLane::Critical => Arc::clone(&self.critical_semaphore),
            ChWriteLane::Bulk => Arc::clone(&self.bulk_semaphore),
        };
        let lane_permit = lane_semaphore
            .acquire_owned()
            .await
            .map_err(|_| StorageError::ClickHouseWriteSemaphoreClosed)?;
        self.metrics
            .permit_wait_seconds
            .with_label_values(&[lane.as_str()])
            .observe(started.elapsed().as_secs_f64());
        self.metrics
            .semaphore_permits_used
            .with_label_values(&[lane.as_str()])
            .inc();

        Ok(WritePermit {
            _lane_permit: lane_permit,
            lane,
            metrics: Arc::clone(&self.metrics),
        })
    }

    pub const fn metrics(&self) -> &Arc<ChWriteMetrics> {
        &self.metrics
    }

    /// Durable batch insert sink: acquire a permit, insert all rows, retry with
    /// exponential backoff, and record metrics. Returns the last error after
    /// `CLICKHOUSE_INSERT_MAX_ATTEMPTS` so the caller (e.g. an `AsyncWriter` flush) can
    /// log and drop, while honoring server backpressure via the semaphore.
    pub async fn write_batch<T>(
        &self,
        client: &Client,
        table: &'static str,
        rows: Vec<T>,
    ) -> Result<(), StorageError>
    where
        T: RowOwned + RowWrite + Send + Sync,
    {
        self.write_batch_borrowed(client, table, &rows).await
    }

    /// Persist a borrowed batch while retaining caller allocation capacity.
    pub async fn write_batch_borrowed<T>(
        &self,
        client: &Client,
        table: &'static str,
        rows: &[T],
    ) -> Result<(), StorageError>
    where
        T: RowOwned + RowWrite + Send + Sync,
    {
        Self::require_bulk_table(table)?;
        self.write_borrowed_mode(client, table, rows, ChWriteLane::Bulk)
            .await
    }

    /// Persist the canonical L2 ledger synchronously through its reserved
    /// critical lane.
    ///
    /// The application coordinator already performs bounded aggregation. A
    /// second server-side async-insert queue would add an unbounded scheduling
    /// interval between admission and durable acknowledgement, which is
    /// incompatible with the ledger's publication deadline. The fixed row type
    /// and table prevent bulk facts from consuming the reserved capacity
    /// through a generic public API.
    pub async fn write_canonical_ledger(
        &self,
        client: &Client,
        rows: &[BookL2LedgerRow],
    ) -> Result<(), StorageError> {
        self.write_borrowed_mode(client, CANONICAL_LEDGER_TABLE, rows, ChWriteLane::Critical)
            .await
    }

    async fn write_borrowed_mode<T>(
        &self,
        client: &Client,
        table: &'static str,
        rows: &[T],
        lane: ChWriteLane,
    ) -> Result<(), StorageError>
    where
        T: RowOwned + RowWrite + Send + Sync,
    {
        if rows.is_empty() {
            return Ok(());
        }
        let count = rows.len();
        let mut last_error: Option<StorageError> = None;
        let started = Instant::now();

        for attempt in 0..CLICKHOUSE_INSERT_MAX_ATTEMPTS {
            let result = self.insert_attempt(client, table, rows, None, lane).await;

            match result {
                Ok(()) => {
                    self.metrics
                        .rows_written
                        .with_label_values(&[table, lane.as_str()])
                        .inc_by(ToPrimitive::to_u64(&count).unwrap_or(u64::MAX));
                    self.metrics
                        .insert_duration_seconds
                        .with_label_values(&[table, lane.as_str()])
                        .observe(started.elapsed().as_secs_f64());
                    return Ok(());
                }
                Err(error) => {
                    if attempt + 1 < CLICKHOUSE_INSERT_MAX_ATTEMPTS {
                        let delay = Duration::from_millis(
                            CLICKHOUSE_INSERT_RETRY_BACKOFF_BASE_MS * 2u64.pow(attempt),
                        );
                        warn!(table, attempt = attempt + 1, rows = count, %error, "ClickHouse insert failed; retrying in {delay:?}");
                        tokio::time::sleep(delay).await;
                    } else {
                        error!(table, rows = count, %error, "ClickHouse insert failed after {CLICKHOUSE_INSERT_MAX_ATTEMPTS} attempts");
                    }
                    last_error = Some(error);
                }
            }
        }

        self.metrics
            .insert_errors
            .with_label_values(&[table, lane.as_str()])
            .inc();
        self.metrics
            .insert_duration_seconds
            .with_label_values(&[table, lane.as_str()])
            .observe(started.elapsed().as_secs_f64());
        Err(last_error.unwrap_or_else(|| {
            StorageError::Connection("ClickHouse insert exhausted retries".into())
        }))
    }

    /// Durable insert with a deterministic `ClickHouse` deduplication token.
    /// The token must identify one immutable chunk, not one retry attempt.
    pub async fn write_batch_deduplicated<T>(
        &self,
        client: &Client,
        table: &'static str,
        deduplication_token: &str,
        rows: Vec<T>,
    ) -> Result<(), StorageError>
    where
        T: RowOwned + RowWrite + Send + Sync,
    {
        Self::require_bulk_table(table)?;
        if rows.is_empty() {
            return Ok(());
        }
        let count = rows.len();
        let mut last_error: Option<StorageError> = None;
        let started = Instant::now();
        for attempt in 0..CLICKHOUSE_INSERT_MAX_ATTEMPTS {
            let lane = ChWriteLane::Bulk;
            let result = self
                .insert_attempt(client, table, &rows, Some(deduplication_token), lane)
                .await;
            match result {
                Ok(()) => {
                    self.metrics
                        .rows_written
                        .with_label_values(&[table, lane.as_str()])
                        .inc_by(ToPrimitive::to_u64(&count).unwrap_or(u64::MAX));
                    self.metrics
                        .insert_duration_seconds
                        .with_label_values(&[table, lane.as_str()])
                        .observe(started.elapsed().as_secs_f64());
                    return Ok(());
                }
                Err(error) => {
                    if attempt + 1 < CLICKHOUSE_INSERT_MAX_ATTEMPTS {
                        let delay = Duration::from_millis(
                            CLICKHOUSE_INSERT_RETRY_BACKOFF_BASE_MS * 2u64.pow(attempt),
                        );
                        warn!(table, attempt = attempt + 1, rows = count, %error, "ClickHouse deduplicated insert failed; retrying in {delay:?}");
                        tokio::time::sleep(delay).await;
                    } else {
                        error!(table, rows = count, %error, "ClickHouse deduplicated insert failed after {CLICKHOUSE_INSERT_MAX_ATTEMPTS} attempts");
                    }
                    last_error = Some(error);
                }
            }
        }
        self.metrics
            .insert_errors
            .with_label_values(&[table, ChWriteLane::Bulk.as_str()])
            .inc();
        self.metrics
            .insert_duration_seconds
            .with_label_values(&[table, ChWriteLane::Bulk.as_str()])
            .observe(started.elapsed().as_secs_f64());
        Err(last_error.unwrap_or_else(|| {
            StorageError::Connection("ClickHouse deduplicated insert exhausted retries".into())
        }))
    }

    async fn insert_attempt<T>(
        &self,
        client: &Client,
        table: &str,
        rows: &[T],
        deduplication_token: Option<&str>,
        lane: ChWriteLane,
    ) -> Result<(), StorageError>
    where
        T: RowOwned + RowWrite + Send + Sync,
    {
        let deadlines = match lane {
            ChWriteLane::Critical => self.deadlines.critical_insert(),
            ChWriteLane::Bulk => self.deadlines.bulk_insert(),
        };
        timeout(deadlines.attempt(), async {
            let _permit = self.acquire_write_permit(lane).await?;
            Self::insert_rows(client, table, rows, deduplication_token, lane, deadlines).await
        })
        .await
        .map_err(|_| StorageError::ClickHouseTimeout {
            operation: lane.attempt_operation(),
            duration: deadlines.attempt(),
        })?
    }

    async fn insert_rows<T>(
        client: &Client,
        table: &str,
        rows: &[T],
        deduplication_token: Option<&str>,
        lane: ChWriteLane,
        deadlines: InsertDeadlines,
    ) -> Result<(), StorageError>
    where
        T: RowOwned + RowWrite + Send + Sync,
    {
        let mut insert = client.insert::<T>(table).await.map_err(|error| {
            map_clickhouse_timeout(error, lane.attempt_operation(), deadlines.attempt())
        })?;
        if let Some(token) = deduplication_token {
            insert = insert.with_setting("insert_deduplication_token", token);
        }
        // Query priority does not govern background merges. It is still useful
        // among foreground work after system-log merges are removed and reads
        // are admission-controlled. Critical inserts use one thread and bypass
        // the cooperative query concurrency pool; bulk writes remain governed.
        insert = insert
            .with_setting("priority", lane.server_priority())
            .with_setting("use_concurrency_control", lane.concurrency_control())
            .with_setting("max_threads", "1")
            .with_setting("max_insert_threads", "1")
            .with_setting("log_queries", "1")
            .with_setting("log_queries_probability", lane.query_log_probability())
            .with_setting("log_query_threads", "0")
            .with_setting("log_processors_profiles", "0")
            .with_setting(
                "max_execution_time",
                deadlines.attempt_seconds_ceil().to_string(),
            )
            .with_setting("timeout_overflow_mode", "throw");
        // Application writers own bounded batching. Server-side async inserts
        // would create a second scheduler and tiny-part cadence, so every lane
        // explicitly performs one synchronous, deduplicated insert.
        insert = insert
            .with_setting("async_insert", "0")
            .with_setting("insert_deduplicate", "1")
            .with_timeouts(Some(deadlines.send()), Some(deadlines.end()));
        for row in rows {
            insert.write(row).await.map_err(|error| {
                map_clickhouse_timeout(error, lane.send_operation(), deadlines.send())
            })?;
        }
        insert.end().await.map_err(|error| {
            map_clickhouse_timeout(error, lane.end_operation(), deadlines.end())
        })?;
        Ok(())
    }

    fn require_bulk_table(table: &'static str) -> Result<(), StorageError> {
        if table == CANONICAL_LEDGER_TABLE {
            return Err(StorageError::invariant_violation(
                Some(CANONICAL_LEDGER_TABLE),
                "canonical L2 ledger writes must use the reserved critical lane",
            ));
        }
        Ok(())
    }
}

fn map_clickhouse_timeout(
    error: ClickHouseError,
    operation: &'static str,
    duration: Duration,
) -> StorageError {
    match error {
        ClickHouseError::TimedOut => StorageError::ClickHouseTimeout {
            operation,
            duration,
        },
        other => StorageError::from(other),
    }
}

/// RAII guard for a write permit. Releasing it decrements the semaphore and metrics.
pub struct WritePermit {
    _lane_permit: OwnedSemaphorePermit,
    lane: ChWriteLane,
    metrics: Arc<ChWriteMetrics>,
}

impl Drop for WritePermit {
    fn drop(&mut self) {
        self.metrics
            .semaphore_permits_used
            .with_label_values(&[self.lane.as_str()])
            .dec();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clickhouse::{Client, Row};
    use quant_pivot_error::storage::StorageError;
    use quant_pivot_models::config::{ClickHouseInsertIoConfig, ClickHouseIoConfig};
    use serde::Serialize;
    use tokio::time::{Instant as TokioInstant, timeout};

    use super::{ChWriteLane, ChWriteManager};
    use crate::clickhouse::test_support::NeverResponseServer;

    #[derive(Row, Serialize)]
    struct TestRow {
        value: u8,
    }

    #[test]
    fn lanes_have_server_order() {
        assert_eq!(ChWriteLane::Critical.server_priority(), "1");
        assert_eq!(ChWriteLane::Bulk.server_priority(), "4");
        assert_eq!(ChWriteLane::Critical.concurrency_control(), "0");
        assert_eq!(ChWriteLane::Bulk.concurrency_control(), "1");
        assert_eq!(ChWriteLane::Critical.query_log_probability(), "1");
        assert_eq!(ChWriteLane::Bulk.query_log_probability(), "0.01");
    }

    #[test]
    fn bulk_lane_rejects_ledger() {
        assert!(ChWriteManager::require_bulk_table("quant_book_l2_ledger").is_err());
        assert!(ChWriteManager::require_bulk_table("quant_feature_event").is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn critical_lane_reserves_capacity() {
        let manager = ChWriteManager::new(2, &ClickHouseIoConfig::default());
        let bulk = manager
            .acquire_write_permit(ChWriteLane::Bulk)
            .await
            .expect("bulk permit");
        assert!(
            timeout(
                Duration::from_millis(1),
                manager.acquire_write_permit(ChWriteLane::Bulk),
            )
            .await
            .is_err()
        );
        let critical = timeout(
            Duration::from_millis(1),
            manager.acquire_write_permit(ChWriteLane::Critical),
        )
        .await
        .expect("critical lane deadline")
        .expect("reserved critical permit");

        drop(critical);
        drop(bulk);
    }

    #[tokio::test(start_paused = true)]
    async fn write_deadline_releases_permit() {
        let server = NeverResponseServer::start().await;
        let client = Client::default()
            .with_url(server.url())
            .with_database("quant_pivot");
        let io = ClickHouseIoConfig {
            bulk_insert: ClickHouseInsertIoConfig {
                send_timeout_ms: 20,
                end_timeout_ms: 30,
                attempt_timeout_ms: 50,
            },
            ..ClickHouseIoConfig::default()
        };
        let manager = ChWriteManager::new(2, &io);
        let started = TokioInstant::now();

        let error = manager
            .write_batch_borrowed(&client, "test_fact", &[TestRow { value: 1 }])
            .await
            .expect_err("never-response insert must exhaust bounded attempts");

        assert!(matches!(
            error,
            StorageError::ClickHouseTimeout {
                operation: "clickhouse.insert.bulk.attempt",
                duration
            } if duration == Duration::from_millis(50)
        ));
        assert_eq!(manager.bulk_semaphore.available_permits(), 1);
        assert_eq!(started.elapsed(), Duration::from_millis(450));
    }

    #[tokio::test(start_paused = true)]
    async fn write_end_deadline_bounds() {
        let server = NeverResponseServer::start().await;
        let client = Client::default()
            .with_url(server.url())
            .with_database("quant_pivot")
            .with_validation(false);
        let io = ClickHouseIoConfig {
            bulk_insert: ClickHouseInsertIoConfig {
                send_timeout_ms: 20,
                end_timeout_ms: 30,
                attempt_timeout_ms: 50,
            },
            ..ClickHouseIoConfig::default()
        };
        let manager = ChWriteManager::new(2, &io);
        let started = TokioInstant::now();

        let error = manager
            .write_batch_borrowed(&client, "test_fact", &[TestRow { value: 1 }])
            .await
            .expect_err("never-response insert end must exhaust bounded attempts");

        assert!(matches!(
            error,
            StorageError::ClickHouseTimeout {
                operation: "clickhouse.insert.bulk.end",
                duration
            } if duration == Duration::from_millis(30)
        ));
        assert_eq!(manager.bulk_semaphore.available_permits(), 1);
        assert_eq!(started.elapsed(), Duration::from_millis(390));
    }

    #[tokio::test(start_paused = true)]
    async fn write_admission_is_bounded() {
        let client = Client::default().with_url("http://127.0.0.1:1");
        let io = ClickHouseIoConfig {
            bulk_insert: ClickHouseInsertIoConfig {
                send_timeout_ms: 20,
                end_timeout_ms: 30,
                attempt_timeout_ms: 50,
            },
            ..ClickHouseIoConfig::default()
        };
        let manager = ChWriteManager::new(2, &io);
        let held = manager
            .acquire_write_permit(ChWriteLane::Bulk)
            .await
            .expect("hold bulk write permit");
        let started = TokioInstant::now();

        let error = manager
            .write_batch_borrowed(&client, "test_fact", &[TestRow { value: 1 }])
            .await
            .expect_err("write admission wait must exhaust bounded attempts");

        assert!(matches!(
            error,
            StorageError::ClickHouseTimeout {
                operation: "clickhouse.insert.bulk.attempt",
                duration
            } if duration == Duration::from_millis(50)
        ));
        assert_eq!(started.elapsed(), Duration::from_millis(450));
        drop(held);
        assert_eq!(manager.bulk_semaphore.available_permits(), 1);
    }
}
