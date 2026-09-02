//! Fail-closed limits and process-wide admission for native `ClickHouse` reads.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use clickhouse::{Client, RowOwned, RowRead, query::Query, sql::Bind};
use prometheus::{
    Error as PrometheusError, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts,
    Registry,
};
use quant_pivot_error::storage::StorageError;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::timeout,
};

use super::pool::ClickHousePool;
/// Server-enforced result limits attached to one native query family.
///
/// `ClickHouse` aborts with `throw` when either limit is exceeded, so callers
/// cannot accidentally accept a silently truncated result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClickHouseQueryLimits {
    operation: &'static str,
    max_result_rows: u64,
    max_result_bytes: u64,
}

impl ClickHouseQueryLimits {
    #[must_use]
    pub const fn new(operation: &'static str, max_result_rows: u64, max_result_bytes: u64) -> Self {
        Self {
            operation,
            max_result_rows,
            max_result_bytes,
        }
    }

    #[must_use]
    pub const fn max_result_rows(self) -> u64 {
        self.max_result_rows
    }

    /// Build an analytical query carrying server-side limits, a low-priority
    /// workload identity, and process-wide admission control. The permit is
    /// acquired only when the query executes and is retained through complete
    /// response decoding.
    pub fn query(self, pool: &ClickHousePool, sql: &str) -> GovernedQuery {
        let deadline = pool.query_deadline();
        GovernedQuery {
            query: self
                .configure(pool.client(), sql)
                .with_setting(
                    "max_execution_time",
                    pool.query_server_seconds().to_string(),
                )
                .with_setting("timeout_overflow_mode", "throw"),
            permits: pool.read_permits(),
            metrics: Arc::clone(pool.read_metrics()),
            operation: self.operation,
            deadline,
        }
    }

    /// Build a client-deadline boot/maintenance query. These operations may run
    /// before the runtime pool exists and are serialized by the schema-mutation
    /// lease; server execution settings are intentionally not injected into
    /// DDL or `SYSTEM` statements.
    pub(crate) fn maintenance_query(
        self,
        client: &ClickHouseMaintenanceClient,
        sql: &str,
    ) -> MaintenanceQuery {
        MaintenanceQuery {
            query: self.configure(client.client(), sql),
            operation: self.operation,
            deadline: client.deadline(),
        }
    }

    fn configure(self, client: &Client, sql: &str) -> Query {
        client
            .query(sql)
            .with_setting("log_comment", self.operation)
            .with_setting("max_result_rows", self.max_result_rows.to_string())
            .with_setting("max_result_bytes", self.max_result_bytes.to_string())
            .with_setting("result_overflow_mode", "throw")
    }
}

/// Query wrapper retaining one analytical-read permit until execution and
/// response decoding finish.
pub struct GovernedQuery {
    query: Query,
    permits: Arc<Semaphore>,
    metrics: Arc<ChReadMetrics>,
    operation: &'static str,
    deadline: Duration,
}

impl GovernedQuery {
    #[track_caller]
    #[must_use]
    pub fn bind(mut self, value: impl Bind) -> Self {
        self.query = self.query.bind(value);
        self
    }

    pub async fn execute(self) -> Result<(), StorageError> {
        let operation = self.operation;
        let deadline = self.deadline;
        timeout(deadline, async move {
            let _permit = self.acquire().await?;
            self.query.execute().await.map_err(StorageError::from)
        })
        .await
        .map_err(|_| StorageError::ClickHouseTimeout {
            operation,
            duration: deadline,
        })?
    }

    pub async fn fetch_one<T>(self) -> Result<T, StorageError>
    where
        T: RowOwned + RowRead,
    {
        let operation = self.operation;
        let deadline = self.deadline;
        timeout(deadline, async move {
            let _permit = self.acquire().await?;
            self.query
                .fetch_one::<T>()
                .await
                .map_err(StorageError::from)
        })
        .await
        .map_err(|_| StorageError::ClickHouseTimeout {
            operation,
            duration: deadline,
        })?
    }

    pub async fn fetch_optional<T>(self) -> Result<Option<T>, StorageError>
    where
        T: RowOwned + RowRead,
    {
        let operation = self.operation;
        let deadline = self.deadline;
        timeout(deadline, async move {
            let _permit = self.acquire().await?;
            self.query
                .fetch_optional::<T>()
                .await
                .map_err(StorageError::from)
        })
        .await
        .map_err(|_| StorageError::ClickHouseTimeout {
            operation,
            duration: deadline,
        })?
    }

    pub async fn fetch_all<T>(self) -> Result<Vec<T>, StorageError>
    where
        T: RowOwned + RowRead,
    {
        let operation = self.operation;
        let deadline = self.deadline;
        timeout(deadline, async move {
            let _permit = self.acquire().await?;
            self.query
                .fetch_all::<T>()
                .await
                .map_err(StorageError::from)
        })
        .await
        .map_err(|_| StorageError::ClickHouseTimeout {
            operation,
            duration: deadline,
        })?
    }

    async fn acquire(&self) -> Result<ReadPermit, StorageError> {
        let started = Instant::now();
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| {
                self.metrics
                    .admission_rejections
                    .with_label_values(&[self.operation])
                    .inc();
                StorageError::ChannelClosed(
                    "ClickHouse analytical-read admission closed".to_owned(),
                )
            })?;
        self.metrics
            .admission_wait_seconds
            .with_label_values(&[self.operation])
            .observe(started.elapsed().as_secs_f64());
        self.metrics
            .permits_used
            .with_label_values(&[self.operation])
            .inc();
        Ok(ReadPermit {
            _permit: permit,
            metrics: Arc::clone(&self.metrics),
            operation: self.operation,
        })
    }
}

pub struct ClickHouseMaintenanceClient {
    client: Client,
    deadline: Duration,
}

impl ClickHouseMaintenanceClient {
    #[must_use]
    pub const fn new(client: Client, deadline: Duration) -> Self {
        Self { client, deadline }
    }

    const fn client(&self) -> &Client {
        &self.client
    }

    const fn deadline(&self) -> Duration {
        self.deadline
    }
}

pub struct MaintenanceQuery {
    query: Query,
    operation: &'static str,
    deadline: Duration,
}

impl MaintenanceQuery {
    #[track_caller]
    #[must_use]
    pub fn bind(mut self, value: impl Bind) -> Self {
        self.query = self.query.bind(value);
        self
    }

    #[must_use]
    pub(crate) fn with_setting(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.query = self.query.with_setting(name, value);
        self
    }

    pub async fn execute(self) -> Result<(), StorageError> {
        let operation = self.operation;
        let deadline = self.deadline;
        timeout(deadline, self.query.execute())
            .await
            .map_err(|_| StorageError::ClickHouseTimeout {
                operation,
                duration: deadline,
            })?
            .map_err(StorageError::from)
    }

    pub async fn fetch_one<T>(self) -> Result<T, StorageError>
    where
        T: RowOwned + RowRead,
    {
        let operation = self.operation;
        let deadline = self.deadline;
        timeout(deadline, self.query.fetch_one::<T>())
            .await
            .map_err(|_| StorageError::ClickHouseTimeout {
                operation,
                duration: deadline,
            })?
            .map_err(StorageError::from)
    }

    pub async fn fetch_optional<T>(self) -> Result<Option<T>, StorageError>
    where
        T: RowOwned + RowRead,
    {
        let operation = self.operation;
        let deadline = self.deadline;
        timeout(deadline, self.query.fetch_optional::<T>())
            .await
            .map_err(|_| StorageError::ClickHouseTimeout {
                operation,
                duration: deadline,
            })?
            .map_err(StorageError::from)
    }

    pub async fn fetch_all<T>(self) -> Result<Vec<T>, StorageError>
    where
        T: RowOwned + RowRead,
    {
        let operation = self.operation;
        let deadline = self.deadline;
        timeout(deadline, self.query.fetch_all::<T>())
            .await
            .map_err(|_| StorageError::ClickHouseTimeout {
                operation,
                duration: deadline,
            })?
            .map_err(StorageError::from)
    }
}

pub struct ChReadMetrics {
    pub admission_wait_seconds: HistogramVec,
    pub permits_used: IntGaugeVec,
    pub admission_rejections: IntCounterVec,
}

impl ChReadMetrics {
    pub fn new() -> Self {
        Self {
            admission_wait_seconds: HistogramVec::new(
                HistogramOpts::new(
                    "ch_read_admission_wait_seconds",
                    "Time analytical ClickHouse reads wait for process-wide admission",
                ),
                &["operation"],
            )
            .expect("ch_read_admission_wait_seconds"),
            permits_used: IntGaugeVec::new(
                Opts::new(
                    "ch_read_permits_used",
                    "Currently admitted analytical ClickHouse reads",
                ),
                &["operation"],
            )
            .expect("ch_read_permits_used"),
            admission_rejections: IntCounterVec::new(
                Opts::new(
                    "ch_read_admission_rejections_total",
                    "Analytical ClickHouse reads rejected because admission closed",
                ),
                &["operation"],
            )
            .expect("ch_read_admission_rejections_total"),
        }
    }

    pub fn register(&self, registry: &Registry) -> Result<(), PrometheusError> {
        registry.register(Box::new(self.admission_wait_seconds.clone()))?;
        registry.register(Box::new(self.permits_used.clone()))?;
        registry.register(Box::new(self.admission_rejections.clone()))?;
        Ok(())
    }
}

impl Default for ChReadMetrics {
    fn default() -> Self {
        Self::new()
    }
}

struct ReadPermit {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<ChReadMetrics>,
    operation: &'static str,
}

impl Drop for ReadPermit {
    fn drop(&mut self) {
        self.metrics
            .permits_used
            .with_label_values(&[self.operation])
            .dec();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quant_pivot_error::storage::StorageError;
    use quant_pivot_models::config::ClickHouseConfig;

    use super::ClickHouseQueryLimits;
    use crate::clickhouse::{ClickHousePool, test_support::NeverResponseServer};

    #[tokio::test(start_paused = true)]
    async fn read_deadline_releases_permit() {
        let server = NeverResponseServer::start().await;
        let mut config = ClickHouseConfig::default();
        server.url().clone_into(&mut config.url);
        config.io.query_timeout_ms = 50;
        config.max_concurrent_reads = 1;
        let pool = ClickHousePool::from_config(&config);

        let error = ClickHouseQueryLimits::new("test.read.never_response", 1, 1_024)
            .query(&pool, "SELECT 1")
            .fetch_one::<u8>()
            .await
            .expect_err("never-response read must reach its total deadline");

        assert!(matches!(
            error,
            StorageError::ClickHouseTimeout {
                operation: "test.read.never_response",
                duration
            } if duration == Duration::from_millis(50)
        ));
        assert_eq!(pool.read_permits().available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn read_admission_is_bounded() {
        let server = NeverResponseServer::start().await;
        let mut config = ClickHouseConfig::default();
        server.url().clone_into(&mut config.url);
        config.io.query_timeout_ms = 50;
        config.max_concurrent_reads = 1;
        let pool = ClickHousePool::from_config(&config);
        let held = pool
            .read_permits()
            .acquire_owned()
            .await
            .expect("hold analytical read permit");

        let error = ClickHouseQueryLimits::new("test.read.admission", 1, 1_024)
            .query(&pool, "SELECT 1")
            .fetch_one::<u8>()
            .await
            .expect_err("analytical admission wait must reach its total deadline");

        assert!(matches!(
            error,
            StorageError::ClickHouseTimeout {
                operation: "test.read.admission",
                duration
            } if duration == Duration::from_millis(50)
        ));
        drop(held);
        assert_eq!(pool.read_permits().available_permits(), 1);
    }
}
