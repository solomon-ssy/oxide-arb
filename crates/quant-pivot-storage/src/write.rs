//! Generic fire-and-forget batched async writer.
//!
//! [`AsyncWriter`] is the single buffering primitive for **telemetry-class**
//! writes across the platform — high-volume, append-only streams that may be
//! dropped under extreme backpressure rather than block a hot path (`ClickHouse`
//! book/quant facts, the `Postgres` operation-log audit). Durable business state
//! (reports, order intents, execution orders) must **not** use this writer; it
//! is written synchronously through repositories so it can never be lost.
//! Training-serving evidence and its run-completion marker are also durability
//! barriers and must use an acknowledged sink directly.
//!
//! The sink is backend-agnostic: `ClickHouse` facts flush through
//! `ChWriteManager::write_batch`, `Postgres` audit rows through a repository.
//!
//! The producer calls [`AsyncWriter::write`] from synchronous or asynchronous
//! code: it is non-blocking and drops the item if the bounded channel is full,
//! incrementing a Prometheus counter and emitting a rate-limited warning. The
//! paired [`AsyncWriterWorker`] batches items by size and interval and flushes
//! them through a caller-supplied sink, draining cleanly on shutdown so no
//! buffered item is lost on a graceful stop.

use std::{
    future::Future,
    mem,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use flume::{Receiver, RecvTimeoutError, SendTimeoutError, Sender};
use parking_lot::Mutex;
use prometheus::{IntCounter, IntGauge};
use quant_pivot_error::storage::StorageError;
use tokio::time::{MissedTickBehavior, timeout};
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

/// Construction parameters for an acknowledged canonical-fact writer.
///
/// This is intentionally distinct from [`AsyncWriterConfig`]. Canonical
/// producers wait for a persistence acknowledgement, so their maximum batching
/// delay must remain short and must never inherit the multi-second analytics
/// flush interval.
#[derive(Debug, Clone, Copy)]
pub struct DurableWriterConfig {
    pub name: &'static str,
    pub capacity: usize,
    pub max_batch_size: usize,
    pub max_batch_delay: Duration,
}

impl DurableWriterConfig {
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            capacity: 8_192,
            max_batch_size: 256,
            max_batch_delay: Duration::from_millis(5),
        }
    }

    #[must_use]
    pub const fn capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    #[must_use]
    pub const fn max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = max_batch_size;
        self
    }

    #[must_use]
    pub const fn max_batch_delay(mut self, max_batch_delay: Duration) -> Self {
        self.max_batch_delay = max_batch_delay;
        self
    }
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
    tx: Sender<Queued<T>>,
    name: &'static str,
    drops: IntCounter,
    observability: AsyncWriterObservability,
    drops_since_warn: AtomicU64,
    last_warn_at: Mutex<Option<Instant>>,
}

/// Failure observed by a canonical-fact producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableWriteError {
    QueueTimeout,
    QueueClosed,
    PersistenceFailed,
    AcknowledgementTimeout,
}

struct DurableQueued<T> {
    item: T,
    enqueued: Instant,
    acknowledgement: Sender<bool>,
}

/// Bounded writer whose producer observes both queue admission and persistence.
///
/// This is reserved for canonical evidence. A timeout is not converted into a
/// drop: callers must invalidate the enclosing stream session and reconnect.
pub struct DurableWriter<T: Send + 'static> {
    tx: Sender<DurableQueued<T>>,
    name: &'static str,
    observability: AsyncWriterObservability,
}

impl<T: Send + 'static> DurableWriter<T> {
    pub fn new<F>(
        config: DurableWriterConfig,
        flush: F,
        observability: AsyncWriterObservability,
    ) -> (Self, DurableWriterWorker<T>)
    where
        F: Fn(Vec<T>) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        let (tx, rx) = flume::bounded(config.capacity.max(1));
        let writer = Self {
            tx,
            name: config.name,
            observability: observability.clone(),
        };
        let worker = DurableWriterWorker {
            rx,
            flush: Box::new(flush),
            name: config.name,
            max_batch_size: config.max_batch_size.max(1),
            max_batch_delay: config.max_batch_delay,
            observability,
        };
        (writer, worker)
    }

    pub fn write_timeout(&self, item: T, timeout: Duration) -> Result<(), DurableWriteError> {
        let (acknowledgement, ack_rx) = flume::bounded(1);
        let queued = DurableQueued {
            item,
            enqueued: Instant::now(),
            acknowledgement,
        };
        self.tx
            .send_timeout(queued, timeout)
            .map_err(|error| match error {
                SendTimeoutError::Timeout(_) => DurableWriteError::QueueTimeout,
                SendTimeoutError::Disconnected(_) => DurableWriteError::QueueClosed,
            })?;
        self.publish_queue_depth();
        ack_rx
            .recv_timeout(timeout)
            .map_err(|error| match error {
                RecvTimeoutError::Timeout => DurableWriteError::AcknowledgementTimeout,
                RecvTimeoutError::Disconnected => DurableWriteError::QueueClosed,
            })?
            .then_some(())
            .ok_or(DurableWriteError::PersistenceFailed)
    }

    /// Enqueue one canonical fact and asynchronously wait for persistence.
    ///
    /// This preserves the same two bounded waits as [`Self::write_timeout`]
    /// without blocking a Tokio worker. It allows independent token streams to
    /// fill one durable batch concurrently while callers retain a strict
    /// persistence barrier before publishing derived state.
    pub async fn write_async_timeout(
        &self,
        item: T,
        timeout_duration: Duration,
    ) -> Result<(), DurableWriteError> {
        let (acknowledgement, ack_rx) = flume::bounded(1);
        let queued = DurableQueued {
            item,
            enqueued: Instant::now(),
            acknowledgement,
        };
        match timeout(timeout_duration, self.tx.send_async(queued)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(DurableWriteError::QueueClosed),
            Err(_) => return Err(DurableWriteError::QueueTimeout),
        }
        self.publish_queue_depth();
        match timeout(timeout_duration, ack_rx.recv_async()).await {
            Ok(Ok(true)) => Ok(()),
            Ok(Ok(false)) => Err(DurableWriteError::PersistenceFailed),
            Ok(Err(_)) => Err(DurableWriteError::QueueClosed),
            Err(_) => Err(DurableWriteError::AcknowledgementTimeout),
        }
    }

    fn publish_queue_depth(&self) {
        if let Some(gauge) = &self.observability.queue_depth {
            gauge.set(i64::try_from(self.tx.len()).unwrap_or(i64::MAX));
        }
    }

    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }
}

pub struct DurableWriterWorker<T: Send + 'static> {
    rx: Receiver<DurableQueued<T>>,
    flush: FlushFn<T>,
    name: &'static str,
    max_batch_size: usize,
    max_batch_delay: Duration,
    observability: AsyncWriterObservability,
}

impl<T: Send + 'static> DurableWriterWorker<T> {
    pub async fn run(self, shutdown: CancellationToken) {
        let Self {
            rx,
            flush,
            name,
            max_batch_size,
            max_batch_delay,
            observability,
        } = self;
        let mut buffer = Vec::with_capacity(max_batch_size);
        loop {
            let first = tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    Self::drain_and_flush(
                        &rx,
                        &flush,
                        name,
                        &observability,
                        max_batch_size,
                        &mut buffer,
                    ).await;
                    return;
                }
                item = rx.recv_async() => {
                    let Ok(item) = item else {
                        Self::flush_batch(&flush, name, &observability, &mut buffer).await;
                        return;
                    };
                    item
                }
            };
            buffer.push(first);

            let deadline = tokio::time::sleep(max_batch_delay);
            tokio::pin!(deadline);
            let mut disconnected = false;
            while buffer.len() < max_batch_size {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        Self::drain_and_flush(
                            &rx,
                            &flush,
                            name,
                            &observability,
                            max_batch_size,
                            &mut buffer,
                        ).await;
                        return;
                    }
                    item = rx.recv_async() => if let Ok(item) = item {
                        buffer.push(item);
                    } else {
                        disconnected = true;
                        break;
                    },
                    () = &mut deadline => break,
                }
            }
            Self::flush_batch(&flush, name, &observability, &mut buffer).await;
            if let Some(gauge) = &observability.queue_depth {
                gauge.set(i64::try_from(rx.len()).unwrap_or(i64::MAX));
            }
            if disconnected {
                return;
            }
        }
    }

    async fn drain_and_flush(
        rx: &Receiver<DurableQueued<T>>,
        flush: &FlushFn<T>,
        name: &'static str,
        observability: &AsyncWriterObservability,
        max_batch_size: usize,
        buffer: &mut Vec<DurableQueued<T>>,
    ) {
        while let Ok(item) = rx.try_recv() {
            buffer.push(item);
            if buffer.len() >= max_batch_size {
                Self::flush_batch(flush, name, observability, buffer).await;
            }
        }
        Self::flush_batch(flush, name, observability, buffer).await;
        if let Some(gauge) = &observability.queue_depth {
            gauge.set(i64::try_from(rx.len()).unwrap_or(i64::MAX));
        }
    }

    async fn flush_batch(
        flush: &FlushFn<T>,
        name: &'static str,
        observability: &AsyncWriterObservability,
        buffer: &mut Vec<DurableQueued<T>>,
    ) {
        if buffer.is_empty() {
            return;
        }
        let queued = mem::take(buffer);
        let oldest_enqueued = queued.iter().map(|item| item.enqueued).min();
        let (batch, acknowledgements): (Vec<T>, Vec<Sender<bool>>) = queued
            .into_iter()
            .map(|item| (item.item, item.acknowledgement))
            .unzip();
        let persisted = match flush(batch).await {
            Ok(()) => {
                if let (Some(report), Some(oldest)) = (&observability.flush_lag_ms, oldest_enqueued)
                {
                    report(u64::try_from(oldest.elapsed().as_millis()).unwrap_or(u64::MAX));
                }
                true
            }
            Err(error) => {
                if let Some(counter) = &observability.flush_failed {
                    counter.inc();
                }
                warn!(writer = name, %error, "canonical batch persistence failed");
                false
            }
        };
        for acknowledgement in acknowledgements {
            let _ = acknowledgement.send(persisted);
        }
    }
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
    rx: Receiver<Queued<T>>,
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
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

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
        let queued = mem::take(buffer);
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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use parking_lot::Mutex;
    use tokio::task::JoinSet;
    use tokio_util::sync::CancellationToken;

    use super::{AsyncWriterObservability, DurableWriteError, DurableWriter, DurableWriterConfig};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_writer_acknowledges_contract() {
        let persisted = Arc::new(Mutex::new(Vec::<u32>::new()));
        let observed = Arc::clone(&persisted);
        let (writer, worker) = DurableWriter::new(
            DurableWriterConfig::new("durable-test")
                .capacity(8)
                .max_batch_size(8)
                .max_batch_delay(Duration::from_millis(10)),
            move |rows| {
                let observed = Arc::clone(&observed);
                Box::pin(async move {
                    observed.lock().extend(rows);
                    Ok(())
                })
            },
            AsyncWriterObservability::default(),
        );
        let shutdown = CancellationToken::new();
        let worker_task = tokio::spawn(worker.run(shutdown.clone()));
        let writer = Arc::new(writer);

        let result = tokio::task::spawn_blocking({
            let writer = Arc::clone(&writer);
            move || writer.write_timeout(7, Duration::from_millis(250))
        })
        .await
        .expect("blocking producer task");

        assert_eq!(result, Ok(()));
        assert_eq!(*persisted.lock(), vec![7]);
        shutdown.cancel();
        worker_task.await.expect("durable worker shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_durable_producers_batch() {
        let persisted = Arc::new(Mutex::new(Vec::<u32>::new()));
        let observed = Arc::clone(&persisted);
        let (writer, worker) = DurableWriter::new(
            DurableWriterConfig::new("async-durable-test")
                .capacity(8)
                .max_batch_size(8)
                .max_batch_delay(Duration::from_millis(50)),
            move |rows| {
                let observed = Arc::clone(&observed);
                Box::pin(async move {
                    observed.lock().extend(rows);
                    Ok(())
                })
            },
            AsyncWriterObservability::default(),
        );
        let shutdown = CancellationToken::new();
        let worker_task = tokio::spawn(worker.run(shutdown.clone()));
        let writer = Arc::new(writer);
        let mut producers = JoinSet::new();
        for value in 0..8 {
            let writer = Arc::clone(&writer);
            producers.spawn(async move {
                writer
                    .write_async_timeout(value, Duration::from_millis(250))
                    .await
            });
        }
        while let Some(result) = producers.join_next().await {
            assert_eq!(result.expect("async producer task"), Ok(()));
        }

        let mut values = persisted.lock().clone();
        values.sort_unstable();
        assert_eq!(values, (0..8).collect::<Vec<_>>());
        shutdown.cancel();
        worker_task.await.expect("durable worker shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_writer_without_acknowledgement() {
        let (writer, worker) = DurableWriter::new(
            DurableWriterConfig::new("closed-test"),
            |_rows: Vec<u32>| Box::pin(async { Ok(()) }),
            AsyncWriterObservability::default(),
        );
        drop(worker);

        assert_eq!(
            writer.write_timeout(1, Duration::from_millis(10)),
            Err(DurableWriteError::QueueClosed)
        );
    }
}
