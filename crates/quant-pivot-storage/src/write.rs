//! Generic fire-and-forget batched async writer.
//!
//! [`AsyncWriter`] is the single buffering primitive for **telemetry-class**
//! writes across the platform — high-volume, append-only streams that may be
//! dropped under extreme backpressure rather than block a hot path (`ClickHouse`
//! book/quant facts, the `Postgres` operation-log audit). Durable business state
//! (reports, order intents, execution orders) must **not** use this writer; it
//! is written synchronously through repositories so it can never be lost.
//!
//! The sink is backend-agnostic: `ClickHouse` facts flush through
//! `ChWriteManager::write_batch`, `Postgres` audit rows through a repository.
//!
//! The producer calls [`AsyncWriter::write`] from any thread (including the
//! synchronous book-apply OS threads): it is non-blocking and drops the item if
//! the bounded channel is full, incrementing a Prometheus counter and emitting a
//! rate-limited warning. The paired [`AsyncWriterWorker`] batches items by size
//! and interval and flushes them through a caller-supplied sink, draining
//! cleanly on shutdown so no buffered item is lost on a graceful stop.

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use prometheus::{IntCounter, IntGauge};
use quant_pivot_error::storage::StorageError;
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Minimum interval between aggregated drop warnings per writer.
const DROP_WARN_INTERVAL: Duration = Duration::from_secs(5);

/// Boxed batch flush sink: receives a drained batch and persists it.
type FlushFn<T> = Box<
    dyn Fn(Vec<T>) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send>> + Send + Sync,
>;

/// Callback reporting the worst enqueue→flush-ack latency (ms) of a batch.
///
/// This is the true ingest pipeline lag (queue wait + persist), independent of
/// venue event age. Kept as a closure so the storage layer stays agnostic of
/// the core lag tracker / metrics it feeds.
pub type FlushLagReporter = Arc<dyn Fn(u64) + Send + Sync>;

/// Optional Prometheus handles for queue depth and flush failures, plus the
/// ingest-pipeline-lag reporter.
#[derive(Clone, Default)]
pub struct AsyncWriterObservability {
    pub queue_depth: Option<IntGauge>,
    pub flush_failed: Option<IntCounter>,
    /// Reports enqueue→flush-ack latency (ms) after each successful batch flush.
    pub flush_lag_ms: Option<FlushLagReporter>,
}

/// One queued item stamped with its enqueue instant so the worker can measure
/// enqueue→flush-ack latency (ingest pipeline lag) without inspecting `T`.
struct Queued<T> {
    item: T,
    enqueued: Instant,
}

/// Construction parameters for an [`AsyncWriter`].
///
/// `capacity` bounds the fire-and-forget channel between the hot path and the
/// background flush worker; size it to absorb the expected burst for
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

/// Producer handle: non-blocking enqueue into the bounded flush channel.
///
/// Cheap to clone (clones the `flume` sender and shares the drop counter).
pub struct AsyncWriter<T: Send + 'static> {
    tx: flume::Sender<Queued<T>>,
    name: &'static str,
    drops: IntCounter,
    observability: AsyncWriterObservability,
    drops_since_warn: AtomicU64,
    last_warn_at: Mutex<Option<Instant>>,
}

impl<T: Send + 'static> AsyncWriter<T> {
    /// Build a writer handle and its paired worker.
    ///
    /// `flush` is the durable sink (e.g. `ChWriteManager::write_batch` or
    /// `repo.append_batch`). `drops` is the Prometheus counter incremented once
    /// per dropped item. Spawn the returned worker on the task registry; the
    /// handle goes to producers.
    pub fn new<F>(
        config: AsyncWriterConfig,
        flush: F,
        drops: IntCounter,
        observability: AsyncWriterObservability,
    ) -> (Self, AsyncWriterWorker<T>)
    where
        F: Fn(Vec<T>) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        let AsyncWriterConfig {
            name,
            capacity,
            batch_size,
            flush_interval,
        } = config;
        let (tx, rx) = flume::bounded(capacity);
        let writer = Self {
            tx,
            name,
            drops,
            observability: observability.clone(),
            drops_since_warn: AtomicU64::new(0),
            last_warn_at: Mutex::new(None),
        };
        let worker = AsyncWriterWorker {
            rx,
            flush: Box::new(flush),
            name,
            batch_size,
            flush_interval,
            observability,
        };
        (writer, worker)
    }

    /// Enqueue an item without blocking. Returns `false` and counts a drop when
    /// the channel is full or closed — the caller must never block on this. The
    /// item is stamped with its enqueue instant for ingest-lag measurement.
    pub fn write(&self, item: T) -> bool {
        let queued = Queued {
            item,
            enqueued: Instant::now(),
        };
        if self.tx.try_send(queued).is_err() {
            self.drops.inc();
            self.note_drop();
            return false;
        }
        self.publish_queue_depth();
        true
    }

    /// Current number of items waiting in the bounded channel.
    #[must_use]
    pub fn queue_depth(&self) -> usize {
        self.tx.len()
    }

    fn publish_queue_depth(&self) {
        if let Some(gauge) = &self.observability.queue_depth {
            gauge.set(i64::try_from(self.tx.len()).unwrap_or(i64::MAX));
        }
    }

    /// Aggregate drop warnings: at most one log line per [`DROP_WARN_INTERVAL`],
    /// carrying the count accumulated in between. The counter still increments
    /// on every drop.
    fn note_drop(&self) {
        self.drops_since_warn.fetch_add(1, Ordering::Relaxed);
        let mut last_warn_at = self.last_warn_at.lock();
        let due = last_warn_at.is_none_or(|at| at.elapsed() >= DROP_WARN_INTERVAL);
        if due {
            *last_warn_at = Some(Instant::now());
            drop(last_warn_at);
            let dropped = self.drops_since_warn.swap(0, Ordering::Relaxed);
            warn!(
                writer = self.name,
                dropped_since_last = dropped,
                "channel full or closed — items dropped"
            );
        }
    }

    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

/// Background drain that batches items and flushes them through the sink.
pub struct AsyncWriterWorker<T: Send + 'static> {
    rx: flume::Receiver<Queued<T>>,
    flush: FlushFn<T>,
    name: &'static str,
    batch_size: usize,
    flush_interval: Duration,
    observability: AsyncWriterObservability,
}

impl<T: Send + 'static> AsyncWriterWorker<T> {
    /// Run until `shutdown` is cancelled or all producers drop, flushing on a
    /// size threshold, a periodic timer, and one final time on stop. Flush
    /// failures are logged and the batch dropped — this writer is best-effort.
    pub async fn run(self, shutdown: CancellationToken) {
        let Self {
            rx,
            flush,
            name,
            batch_size,
            flush_interval,
            observability,
        } = self;
        let mut buffer = Vec::with_capacity(batch_size);
        let mut interval = tokio::time::interval(flush_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let publish_queue_depth = || {
            if let Some(gauge) = &observability.queue_depth {
                gauge.set(i64::try_from(rx.len()).unwrap_or(i64::MAX));
            }
        };

        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    while let Ok(item) = rx.try_recv() {
                        buffer.push(item);
                    }
                    Self::flush_batch(&flush, name, &observability, &mut buffer).await;
                    publish_queue_depth();
                    return;
                }
                item = rx.recv_async() => {
                    let Ok(item) = item else {
                        Self::flush_batch(&flush, name, &observability, &mut buffer).await;
                        publish_queue_depth();
                        return;
                    };
                    buffer.push(item);
                    publish_queue_depth();
                    if buffer.len() >= batch_size {
                        Self::flush_batch(&flush, name, &observability, &mut buffer).await;
                    }
                }
                _ = interval.tick() => {
                    publish_queue_depth();
                    Self::flush_batch(&flush, name, &observability, &mut buffer).await;
                }
            }
        }
    }

    async fn flush_batch(
        flush: &FlushFn<T>,
        name: &'static str,
        observability: &AsyncWriterObservability,
        buffer: &mut Vec<Queued<T>>,
    ) {
        if buffer.is_empty() {
            return;
        }
        let queued = std::mem::take(buffer);
        // Oldest enqueue instant in the batch → worst-case pipeline lag once the
        // flush is acknowledged below.
        let oldest_enqueued = queued.iter().map(|q| q.enqueued).min();
        let batch: Vec<T> = queued.into_iter().map(|q| q.item).collect();
        match flush(batch).await {
            Ok(()) => {
                if let (Some(report), Some(oldest)) = (&observability.flush_lag_ms, oldest_enqueued)
                {
                    let lag_ms = u64::try_from(oldest.elapsed().as_millis()).unwrap_or(u64::MAX);
                    report(lag_ms);
                }
            }
            Err(error) => {
                if let Some(counter) = &observability.flush_failed {
                    counter.inc();
                }
                warn!(writer = name, %error, "batch flush failed; batch dropped");
            }
        }
    }
}
