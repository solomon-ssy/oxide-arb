//! `ClickHouse` write concurrency control.
//!
//! The `ChWriteManager` wraps a counting semaphore limiting concurrent insert
//! operations plus Prometheus metrics for observability (rows written, insert
//! durations/errors, permits in use). Backpressure against an overloaded
//! server is provided by the semaphore: batched inserts queue on permits
//! instead of piling additional requests onto a slow `ClickHouse`.

use prometheus::{GaugeVec, IntCounterVec, IntGauge, Opts, Registry};
use quant_pivot_error::storage::StorageError;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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

    pub fn register(&self, registry: &Registry) -> Result<(), prometheus::Error> {
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
