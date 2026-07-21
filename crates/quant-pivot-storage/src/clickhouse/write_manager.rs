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

use clickhouse::{Client, RowOwned, RowWrite};
use num_traits::ToPrimitive;
use prometheus::{Error, GaugeVec, IntCounterVec, IntGauge, Opts, Registry};
use quant_pivot_error::storage::StorageError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{error, warn};

/// Maximum insert attempts (initial + retries) before a batch is surfaced as an error.
const MAX_INSERT_ATTEMPTS: u32 = 3;

/// Metrics specific to `ClickHouse` write operations.
pub struct ChWriteMetrics {
    pub rows_written: IntCounterVec,
    pub insert_duration_seconds: GaugeVec,
    pub insert_errors: IntCounterVec,
    pub semaphore_permits_used: IntGauge,
}

impl ChWriteMetrics {
    pub fn new() -> Self {
        Self {
            rows_written: IntCounterVec::new(
                Opts::new("ch_rows_written_total", "Total rows written to ClickHouse"),
                &["table"],
            )
            .expect("ch_rows_written_total"),
            insert_duration_seconds: GaugeVec::new(
                Opts::new(
                    "ch_insert_duration_seconds",
                    "Last insert batch duration in seconds",
                ),
                &["table"],
            )
            .expect("ch_insert_duration_seconds"),
            insert_errors: IntCounterVec::new(
                Opts::new("ch_insert_errors_total", "Total ClickHouse insert errors"),
                &["table"],
            )
            .expect("ch_insert_errors_total"),
            semaphore_permits_used: IntGauge::new(
                "ch_semaphore_permits_used",
                "Currently held write permits",
            )
            .expect("ch_semaphore_permits_used"),
        }
    }

    pub fn register(&self, registry: &Registry) -> Result<(), Error> {
        registry.register(Box::new(self.rows_written.clone()))?;
        registry.register(Box::new(self.insert_duration_seconds.clone()))?;
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
    semaphore: Arc<Semaphore>,
    metrics: Arc<ChWriteMetrics>,
}

impl ChWriteManager {
    /// Create a write manager with the given insert concurrency
    /// (`db.clickhouse.max_concurrent_inserts`).
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            metrics: Arc::new(ChWriteMetrics::new()),
        }
    }

    /// Acquire a write permit. Awaits when the semaphore is exhausted.
    /// Returns the permit guard; dropping it releases the permit.
    pub async fn acquire_write_permit(&self) -> Result<WritePermit, StorageError> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StorageError::ClickHouseWriteSemaphoreClosed)?;

        self.metrics.semaphore_permits_used.inc();

        Ok(WritePermit {
            _permit: permit,
            metrics: Arc::clone(&self.metrics),
        })
    }

    pub const fn metrics(&self) -> &Arc<ChWriteMetrics> {
        &self.metrics
    }

    /// Durable batch insert sink: acquire a permit, insert all rows, retry with
    /// exponential backoff, and record metrics. Returns the last error after
    /// `MAX_INSERT_ATTEMPTS` so the caller (e.g. an `AsyncWriter` flush) can
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
        if rows.is_empty() {
            return Ok(());
        }
        let count = rows.len();
        let mut last_error: Option<StorageError> = None;

        for attempt in 0..MAX_INSERT_ATTEMPTS {
            let start = Instant::now();
            let permit = self.acquire_write_permit().await?;
            let result = Self::insert_rows(client, table, &rows, None).await;
            drop(permit);

            match result {
                Ok(()) => {
                    self.metrics
                        .rows_written
                        .with_label_values(&[table])
                        .inc_by(ToPrimitive::to_u64(&count).unwrap_or(u64::MAX));
                    self.metrics
                        .insert_duration_seconds
                        .with_label_values(&[table])
                        .set(start.elapsed().as_secs_f64());
                    return Ok(());
                }
                Err(error) => {
                    if attempt + 1 < MAX_INSERT_ATTEMPTS {
                        let delay = Duration::from_millis(100 * 2u64.pow(attempt));
                        warn!(table, attempt = attempt + 1, rows = count, %error, "ClickHouse insert failed; retrying in {delay:?}");
                        tokio::time::sleep(delay).await;
                    } else {
                        error!(table, rows = count, %error, "ClickHouse insert failed after {MAX_INSERT_ATTEMPTS} attempts");
                    }
                    last_error = Some(error);
                }
            }
        }

        self.metrics.insert_errors.with_label_values(&[table]).inc();
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
        if rows.is_empty() {
            return Ok(());
        }
        let count = rows.len();
        let mut last_error: Option<StorageError> = None;
        for attempt in 0..MAX_INSERT_ATTEMPTS {
            let start = Instant::now();
            let permit = self.acquire_write_permit().await?;
            let result = Self::insert_rows(client, table, &rows, Some(deduplication_token)).await;
            drop(permit);
            match result {
                Ok(()) => {
                    self.metrics
                        .rows_written
                        .with_label_values(&[table])
                        .inc_by(ToPrimitive::to_u64(&count).unwrap_or(u64::MAX));
                    self.metrics
                        .insert_duration_seconds
                        .with_label_values(&[table])
                        .set(start.elapsed().as_secs_f64());
                    return Ok(());
                }
                Err(error) => {
                    if attempt + 1 < MAX_INSERT_ATTEMPTS {
                        let delay = Duration::from_millis(100 * 2u64.pow(attempt));
                        warn!(table, attempt = attempt + 1, rows = count, %error, "ClickHouse deduplicated insert failed; retrying in {delay:?}");
                        tokio::time::sleep(delay).await;
                    } else {
                        error!(table, rows = count, %error, "ClickHouse deduplicated insert failed after {MAX_INSERT_ATTEMPTS} attempts");
                    }
                    last_error = Some(error);
                }
            }
        }
        self.metrics.insert_errors.with_label_values(&[table]).inc();
        Err(last_error.unwrap_or_else(|| {
            StorageError::Connection("ClickHouse deduplicated insert exhausted retries".into())
        }))
    }

    async fn insert_rows<T>(
        client: &Client,
        table: &str,
        rows: &[T],
        deduplication_token: Option<&str>,
    ) -> Result<(), StorageError>
    where
        T: RowOwned + RowWrite + Send + Sync,
    {
        let mut insert = client.insert::<T>(table).await?;
        if let Some(token) = deduplication_token {
            insert = insert.with_setting("insert_deduplication_token", token);
        }
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }
}

/// RAII guard for a write permit. Releasing it decrements the semaphore and metrics.
pub struct WritePermit {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<ChWriteMetrics>,
}

impl Drop for WritePermit {
    fn drop(&mut self) {
        self.metrics.semaphore_permits_used.dec();
    }
}
