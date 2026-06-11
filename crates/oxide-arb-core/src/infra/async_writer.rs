use crate::observability::metrics_hub::MetricsHub;
use oxide_arb_error::OxideError;
use parking_lot::Mutex;
use prometheus::IntCounter;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

type AsyncWriterWorker = Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>;

/// Minimum interval between aggregated drop warnings per writer.
const DROP_WARN_INTERVAL: Duration = Duration::from_secs(5);

/// Construction parameters for an [`AsyncWriter`].
///
/// `capacity` bounds the fire-and-forget channel between the hot path and the
/// background flush worker; size it to absorb the expected ingest burst for
/// `capacity / rate` seconds. Defaults: capacity 4096, batch 100, flush 1s.
#[derive(Debug, Clone, Copy)]
pub struct AsyncWriterConfig {
    pub name: &'static str,
    pub capacity: usize,
    pub batch_size: usize,
    pub flush_interval: Duration,
}

impl AsyncWriterConfig {
    /// Defaults for a low-volume writer; override per call site.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            capacity: 4096,
            batch_size: 100,
            flush_interval: Duration::from_secs(1),
        }
    }

    #[must_use]
    pub const fn capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    #[must_use]
    pub const fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    #[must_use]
    pub const fn flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }
}

pub struct AsyncWriter<T: Send + 'static> {
    tx: flume::Sender<T>,
    name: &'static str,
    drops: IntCounter,
    /// Drops accumulated since the last aggregated warning.
    drops_since_warn: AtomicU64,
    last_warn_at: Mutex<Option<Instant>>,
}

impl<T: Send + 'static> AsyncWriter<T> {
    pub fn new<F>(
        config: AsyncWriterConfig,
        flush_fn: F,
        metrics: Arc<MetricsHub>,
        shutdown: CancellationToken,
    ) -> (Self, AsyncWriterWorker)
    where
        F: Fn(Vec<T>) -> Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>
            + Send
            + 'static,
    {
        let AsyncWriterConfig {
            name,
            capacity,
            batch_size,
            flush_interval,
        } = config;
        let (tx, rx) = flume::bounded(capacity);
        let drops = metrics.async_writer_dropped.with_label_values(&[name]);
        drop(metrics);
        let writer = Self {
            tx,
            name,
            drops,
            drops_since_warn: AtomicU64::new(0),
            last_warn_at: Mutex::new(None),
        };

        let worker = async move {
            let mut buffer = Vec::with_capacity(batch_size);
            let mut interval = tokio::time::interval(flush_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        while let Ok(item) = rx.try_recv() {
                            buffer.push(item);
                        }
                        if !buffer.is_empty() {
                            let batch = std::mem::take(&mut buffer);
                            if let Err(e) = flush_fn(batch).await {
                                tracing::warn!(writer = %name, error = %e, "final flush failed");
                            }
                        }
                        return Ok(());
                    }
                    item = rx.recv_async() => {
                        if let Ok(item) = item {
                            buffer.push(item);
                            if buffer.len() >= batch_size {
                                let batch = std::mem::take(&mut buffer);
                                if let Err(e) = flush_fn(batch).await {
                                    tracing::warn!(writer = %name, error = %e, "batch flush failed");
                                }
                            }
                        } else {
                            if !buffer.is_empty() {
                                let batch = std::mem::take(&mut buffer);
                                let _ = flush_fn(batch).await;
                            }
                            return Ok(());
                        }
                    }
                    _ = interval.tick() => {
                        if !buffer.is_empty() {
                            let batch = std::mem::take(&mut buffer);
                            if let Err(e) = flush_fn(batch).await {
                                tracing::warn!(writer = %name, error = %e, "interval flush failed");
                            }
                        }
                    }
                }
            }
        };

        (writer, Box::pin(worker))
    }

    pub fn write(&self, item: T) {
        if self.tx.try_send(item).is_err() {
            self.drops.inc();
            self.note_drop();
        }
    }

    /// Aggregate drop warnings: at most one log line per
    /// [`DROP_WARN_INTERVAL`], carrying the count accumulated in between.
    /// The Prometheus counter still increments on every drop.
    fn note_drop(&self) {
        self.drops_since_warn.fetch_add(1, Ordering::Relaxed);
        let mut last_warn_at = self.last_warn_at.lock();
        let due = last_warn_at.is_none_or(|at| at.elapsed() >= DROP_WARN_INTERVAL);
        if due {
            *last_warn_at = Some(Instant::now());
            drop(last_warn_at);
            let dropped = self.drops_since_warn.swap(0, Ordering::Relaxed);
            tracing::warn!(
                writer = self.name,
                dropped_since_last = dropped,
                "channel full or closed — items dropped"
            );
        }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }
}
