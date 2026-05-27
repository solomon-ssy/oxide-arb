//! `ClickHouse` write concurrency control: semaphore + lag-based backpressure.
//!
//! The `ChWriteManager` wraps:
//! - A counting semaphore limiting concurrent insert operations.
//! - A periodic lag probe that monitors replication/insert delay and pauses
//!   writes when lag exceeds the configured threshold.
//! - Prometheus metrics for observability (permits acquired, lag values, throttle events).

use oxide_arb_error::storage::StorageError;
use oxide_arb_models::config::AnalyticsConfig;
use prometheus::{Gauge, GaugeVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

/// Metrics specific to `ClickHouse` write operations.
pub struct ChWriteMetrics {
    pub rows_written: IntCounterVec,
    pub insert_duration_seconds: GaugeVec,
    pub insert_errors: IntCounterVec,
    pub lag_seconds: Gauge,
    pub throttle_events: IntCounter,
    pub semaphore_permits_used: IntGauge,
    pub health_probes: IntCounter,
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
            lag_seconds: Gauge::new("ch_lag_seconds", "ClickHouse replication/insert lag")
                .expect("ch_lag_seconds"),
            throttle_events: IntCounter::new(
                "ch_throttle_events_total",
                "Times writes were throttled due to lag",
            )
            .expect("ch_throttle_events_total"),
            semaphore_permits_used: IntGauge::new(
                "ch_semaphore_permits_used",
                "Currently held write permits",
            )
            .expect("ch_semaphore_permits_used"),
            health_probes: IntCounter::new(
                "ch_health_probes_total",
                "Total lag health probes executed",
            )
            .expect("ch_health_probes_total"),
        }
    }

    pub fn register(&self, registry: &Registry) -> Result<(), prometheus::Error> {
        registry.register(Box::new(self.rows_written.clone()))?;
        registry.register(Box::new(self.insert_duration_seconds.clone()))?;
        registry.register(Box::new(self.insert_errors.clone()))?;
        registry.register(Box::new(self.lag_seconds.clone()))?;
        registry.register(Box::new(self.throttle_events.clone()))?;
        registry.register(Box::new(self.semaphore_permits_used.clone()))?;
        registry.register(Box::new(self.health_probes.clone()))?;
        Ok(())
    }
}

impl Default for ChWriteMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Manages concurrent write access to `ClickHouse` with backpressure.
pub struct ChWriteManager {
    semaphore: Arc<Semaphore>,
    lagging: Arc<AtomicBool>,
    max_lag_secs: f64,
    metrics: Arc<ChWriteMetrics>,
    _lag_probe_handle: Option<JoinHandle<()>>,
}

impl ChWriteManager {
    pub fn new(
        config: &AnalyticsConfig,
        client: clickhouse::Client,
        shutdown: CancellationToken,
    ) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_inserts));
        let lagging = Arc::new(AtomicBool::new(false));
        let metrics = Arc::new(ChWriteMetrics::new());
        let max_lag_secs = config.max_lag_secs;

        let handle = {
            let lagging = lagging.clone();
            let metrics = metrics.clone();
            let probe_interval = Duration::from_secs(config.lag_probe_interval_secs);
            tokio::spawn(Self::lag_probe_loop(
                client,
                lagging,
                metrics,
                max_lag_secs,
                probe_interval,
                shutdown,
            ))
        };

        Self {
            semaphore,
            lagging,
            max_lag_secs,
            metrics,
            _lag_probe_handle: Some(handle),
        }
    }

    /// Create a write manager without lag probing (for tests).
    pub fn new_without_probe(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            lagging: Arc::new(AtomicBool::new(false)),
            max_lag_secs: f64::MAX,
            metrics: Arc::new(ChWriteMetrics::new()),
            _lag_probe_handle: None,
        }
    }

    /// Acquire a write permit. Blocks if semaphore is exhausted or lag is detected.
    /// Returns the permit guard; dropping it releases the permit.
    pub async fn acquire_write_permit(&self) -> Result<WritePermit, StorageError> {
        // Check lag backpressure
        if self.lagging.load(Ordering::Relaxed) {
            self.metrics.throttle_events.inc();
            warn!(
                max_lag_secs = self.max_lag_secs,
                "ClickHouse lag detected, throttling write"
            );
            // Wait with exponential back-off for lag to subside
            self.wait_for_lag_recovery().await?;
        }

        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StorageError::ClickHouseWriteSemaphoreClosed)?;

        self.metrics.semaphore_permits_used.inc();

        Ok(WritePermit {
            _permit: permit,
            metrics: self.metrics.clone(),
        })
    }

    pub const fn metrics(&self) -> &Arc<ChWriteMetrics> {
        &self.metrics
    }

    pub fn is_lagging(&self) -> bool {
        self.lagging.load(Ordering::Relaxed)
    }

    async fn wait_for_lag_recovery(&self) -> Result<(), StorageError> {
        let mut backoff = Duration::from_millis(100);
        let max_wait = Duration::from_secs(30);
        let mut total_waited = Duration::ZERO;

        while self.lagging.load(Ordering::Relaxed) {
            if total_waited >= max_wait {
                return Err(StorageError::ClickHouseLagTimeout);
            }
            tokio::time::sleep(backoff).await;
            total_waited += backoff;
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }

        Ok(())
    }

    async fn lag_probe_loop(
        client: clickhouse::Client,
        lagging: Arc<AtomicBool>,
        metrics: Arc<ChWriteMetrics>,
        max_lag_secs: f64,
        interval: Duration,
        shutdown: CancellationToken,
    ) {
        let mut ticker = tokio::time::interval(interval);

        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    debug!("ClickHouse lag probe loop shutting down");
                    break;
                }
                _ = ticker.tick() => {
                    metrics.health_probes.inc();
                    match Self::probe_lag(&client).await {
                        Ok(lag_secs) => {
                            metrics.lag_seconds.set(lag_secs);
                            let was_lagging = lagging.load(Ordering::Relaxed);
                            let now_lagging = lag_secs > max_lag_secs;
                            lagging.store(now_lagging, Ordering::Relaxed);

                            if now_lagging && !was_lagging {
                                warn!(lag_secs, max_lag_secs, "ClickHouse lag threshold exceeded, enabling backpressure");
                            } else if !now_lagging && was_lagging {
                                debug!(lag_secs, "ClickHouse lag recovered, backpressure released");
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "Failed to probe ClickHouse lag");
                            // On probe failure, assume lag to be safe
                            lagging.store(true, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }

    /// Query `ClickHouse` for replication lag or max insertion delay.
    /// Uses `system.replicas` when available, falls back to a simple latency check.
    async fn probe_lag(client: &clickhouse::Client) -> Result<f64, clickhouse::error::Error> {
        // Try replicated table lag first
        let result = client
            .query(
                "SELECT max(absolute_delay) as max_lag FROM system.replicas \
                 WHERE is_readonly = 0",
            )
            .fetch_one::<f64>()
            .await;

        if let Ok(lag) = result {
            Ok(lag)
        } else {
            // Fallback: use a simple round-trip latency as a proxy.
            // If the server is overwhelmed, even SELECT 1 will be slow.
            let start = Instant::now();
            client.query("SELECT 1").fetch_one::<u8>().await?;
            let rtt = start.elapsed().as_secs_f64();
            Ok(rtt)
        }
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
