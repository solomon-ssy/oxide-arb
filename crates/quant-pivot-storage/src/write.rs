//! Generic fire-and-forget batched async writer.
//!
//! [`AsyncWriter`] is the single buffering primitive for **telemetry-class**
//! writes across the platform — high-volume, append-only streams that may be
//! dropped under extreme backpressure rather than block a hot path (`ClickHouse`
//! telemetry facts, the `Postgres` operation-log audit). Durable business state
//! (reports, order intents, execution orders) must **not** use this writer; it
//! is written synchronously through repositories so it can never be lost.
//! Training-serving inputs (including microstructure windows), evidence, and
//! run-completion markers are also durability barriers and must use an
//! acknowledged sink directly.
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
    slice,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use flume::{Receiver, RecvTimeoutError, SendTimeoutError, Sender, TryRecvError};
use parking_lot::Mutex;
use prometheus::{IntCounter, IntGauge};
use quant_pivot_error::storage::StorageError;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{MissedTickBehavior, timeout},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

/// Minimum interval between aggregated drop warnings per writer.
const DROP_WARN_INTERVAL: Duration = Duration::from_secs(5);

/// Boxed batch flush sink: receives a drained batch and persists it.
type FlushFn<T> = Box<
    dyn Fn(Vec<T>) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send>> + Send + Sync,
>;

/// Resident-byte accounting for one queued durable item.
type WeightFn<T> = Arc<dyn Fn(&T) -> usize + Send + Sync>;

/// Callback reporting the worst enqueue→flush-ack latency (ms) of a batch.
///
/// This is the true ingest pipeline lag (queue wait + persist), independent of
/// venue event age. Kept as a closure so the storage layer stays agnostic of
/// the core lag tracker / metrics it feeds.
pub type FlushLagReporter = Arc<dyn Fn(u64) + Send + Sync>;

/// Callback reporting a bounded durable-writer stage latency in milliseconds.
pub type FlushStageReporter = Arc<dyn Fn(&'static str, u64) + Send + Sync>;

/// Optional Prometheus handles for queue depth and flush failures, plus the
/// ingest-pipeline-lag reporter.
#[derive(Clone, Default)]
pub struct AsyncWriterObservability {
    pub queue_depth: Option<IntGauge>,
    /// Items admitted by a weighted durable writer, including rows already
    /// received by its worker but not yet committed by the producer.
    pub inflight_items: Option<IntGauge>,
    /// Resident bytes admitted by a weighted durable writer. The gauge remains
    /// charged through sink acknowledgement and the producer's cursor commit.
    pub inflight_bytes: Option<IntGauge>,
    pub flush_failed: Option<IntCounter>,
    /// Reports enqueue→flush-ack latency (ms) after each successful batch flush.
    pub flush_lag_ms: Option<FlushLagReporter>,
    /// Reports internal stage latency when a durability coordinator exposes it.
    pub stage_lag_ms: Option<FlushStageReporter>,
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
    CapacityExceeded,
    PayloadTooLarge,
    PersistenceFailed,
    AcknowledgementTimeout,
    AlreadyAcknowledged,
}

/// Independent bounded waits for queue admission and durable acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurableWriteTimeouts {
    enqueue: Duration,
    acknowledgement: Duration,
}

impl DurableWriteTimeouts {
    #[must_use]
    pub const fn new(enqueue: Duration, acknowledgement: Duration) -> Self {
        Self {
            enqueue,
            acknowledgement,
        }
    }

    #[must_use]
    pub const fn enqueue(self) -> Duration {
        self.enqueue
    }

    #[must_use]
    pub const fn acknowledgement(self) -> Duration {
        self.acknowledgement
    }
}

struct DurableQueued<T> {
    items: Vec<T>,
    enqueued: Instant,
    acknowledgement: Sender<Result<(), DurableWriteError>>,
    reservation: Option<Arc<DurableResourceReservation>>,
}

enum DurableCommand<T> {
    Write(DurableQueued<T>),
    Flush(Sender<()>),
}

struct DurableResourceReservation {
    _byte_permit: Option<OwnedSemaphorePermit>,
    _item_permit: OwnedSemaphorePermit,
    bytes: usize,
    items: usize,
    item_gauge: Option<IntGauge>,
    gauge: Option<IntGauge>,
}

impl Drop for DurableResourceReservation {
    fn drop(&mut self) {
        if let Some(gauge) = &self.item_gauge {
            gauge.sub(i64::try_from(self.items).unwrap_or(i64::MAX));
        }
        if let Some(gauge) = &self.gauge {
            gauge.sub(i64::try_from(self.bytes).unwrap_or(i64::MAX));
        }
    }
}

/// Receipt for one admitted durable write.
///
/// The receipt owns the producer side of any item/byte reservation. It
/// must be acknowledged in source order before the producer advances its
/// durable cursor.
#[must_use = "durable writes must be acknowledged before advancing source state"]
pub struct DurableWriteReceipt {
    acknowledgement: Option<Receiver<Result<(), DurableWriteError>>>,
    deadline: Instant,
    reservation: Option<Arc<DurableResourceReservation>>,
}

/// Resident-byte guard returned after the sink acknowledges a write.
///
/// Keep this guard until the state derived from the write, such as a source
/// cursor, has committed. Dropping it releases the final item/byte reservation.
#[must_use = "hold the acknowledgement guard through the dependent state commit"]
pub struct DurableWriteAcknowledgement {
    _reservation: Option<Arc<DurableResourceReservation>>,
}

impl DurableWriteReceipt {
    pub async fn acknowledge(&mut self) -> Result<DurableWriteAcknowledgement, DurableWriteError> {
        let acknowledgement = self
            .acknowledgement
            .as_ref()
            .ok_or(DurableWriteError::AlreadyAcknowledged)?;
        let result = match acknowledgement.try_recv() {
            Ok(result) => result,
            Err(TryRecvError::Disconnected) => Err(DurableWriteError::QueueClosed),
            Err(TryRecvError::Empty) => {
                let remaining = self.deadline.saturating_duration_since(Instant::now());
                match timeout(remaining, acknowledgement.recv_async()).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) => Err(DurableWriteError::QueueClosed),
                    Err(_) => Err(DurableWriteError::AcknowledgementTimeout),
                }
            }
        };
        let _ = self.acknowledgement.take();
        let reservation = self.reservation.take();
        result?;
        Ok(DurableWriteAcknowledgement {
            _reservation: reservation,
        })
    }
}

/// Bounded writer for acknowledged evidence and source-fact paths.
///
/// Canonical hot paths may enqueue and await one barrier directly. High-rate
/// source owners may split admission from receipt acknowledgement so one
/// application worker can batch across sources, but must consume receipts in
/// source order before committing dependent cursors. A timeout is never
/// converted into a drop: callers invalidate or recover the enclosing stream.
pub struct DurableWriter<T: Send + 'static> {
    tx: Sender<DurableCommand<T>>,
    name: &'static str,
    observability: AsyncWriterObservability,
    byte_budget: Option<Arc<Semaphore>>,
    byte_limit: Option<usize>,
    item_budget: Arc<Semaphore>,
    item_limit: usize,
    weight: Option<WeightFn<T>>,
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
        Self::build(config, None, None, flush, observability)
    }

    /// Build a count- and resident-byte-bounded durable writer.
    ///
    /// The resource reservation remains owned jointly by the worker and receipt,
    /// so cancellation or a delayed cursor commit cannot release capacity
    /// while either side still retains the admitted payload.
    pub fn new_weighted<F, W>(
        config: DurableWriterConfig,
        max_inflight_bytes: usize,
        weight: W,
        flush: F,
        observability: AsyncWriterObservability,
    ) -> Result<(Self, DurableWriterWorker<T>), StorageError>
    where
        F: Fn(Vec<T>) -> Pin<Box<dyn Future<Output = Result<(), StorageError>> + Send>>
            + Send
            + Sync
            + 'static,
        W: Fn(&T) -> usize + Send + Sync + 'static,
    {
        if !(1..=Semaphore::MAX_PERMITS).contains(&config.capacity)
            || !(1..=config.capacity).contains(&config.max_batch_size)
        {
            return Err(StorageError::invariant_violation(
                Some(config.name),
                format!(
                    "weighted durable writer requires 1 <= max_batch_size <= capacity <= {}",
                    Semaphore::MAX_PERMITS
                ),
            ));
        }
        if !(1..=Semaphore::MAX_PERMITS).contains(&max_inflight_bytes) {
            return Err(StorageError::invariant_violation(
                Some(config.name),
                format!(
                    "durable writer byte budget must be between 1 and {} bytes",
                    Semaphore::MAX_PERMITS
                ),
            ));
        }
        Ok(Self::build(
            config,
            Some(max_inflight_bytes),
            Some(Arc::new(weight)),
            flush,
            observability,
        ))
    }

    fn build<F>(
        config: DurableWriterConfig,
        byte_limit: Option<usize>,
        weight: Option<WeightFn<T>>,
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
            byte_budget: byte_limit.map(|limit| Arc::new(Semaphore::new(limit))),
            byte_limit,
            item_budget: Arc::new(Semaphore::new(config.capacity.max(1))),
            item_limit: config.capacity.max(1),
            weight,
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

    pub fn write(&self, item: T, timeouts: DurableWriteTimeouts) -> Result<(), DurableWriteError> {
        let reservation = self.reserve_resources_sync(slice::from_ref(&item))?;
        let (acknowledgement, ack_rx) = flume::bounded(1);
        let queued = DurableQueued {
            items: vec![item],
            enqueued: Instant::now(),
            acknowledgement,
            reservation: reservation.as_ref().map(Arc::clone),
        };
        self.tx
            .send_timeout(DurableCommand::Write(queued), timeouts.enqueue)
            .map_err(|error| match error {
                SendTimeoutError::Timeout(_) => DurableWriteError::QueueTimeout,
                SendTimeoutError::Disconnected(_) => DurableWriteError::QueueClosed,
            })?;
        self.publish_queue_depth();
        let result =
            ack_rx
                .recv_timeout(timeouts.acknowledgement)
                .map_err(|error| match error {
                    RecvTimeoutError::Timeout => DurableWriteError::AcknowledgementTimeout,
                    RecvTimeoutError::Disconnected => DurableWriteError::QueueClosed,
                })?;
        result?;
        drop(reservation);
        Ok(())
    }

    /// Enqueue one canonical fact and asynchronously wait for persistence.
    ///
    /// This preserves the same two bounded waits as [`Self::write`]
    /// without blocking a Tokio worker. It allows independent token streams to
    /// fill one durable batch concurrently while callers retain a strict
    /// persistence barrier before publishing derived state.
    pub async fn write_async(
        &self,
        item: T,
        timeouts: DurableWriteTimeouts,
    ) -> Result<(), DurableWriteError> {
        self.write_batch_async(vec![item], timeouts).await
    }

    /// Enqueue one canonical batch and asynchronously wait for persistence.
    ///
    /// The batch receives one acknowledgement after every row in it is
    /// durably flushed. This lets a partition publish a single bounded batch
    /// barrier instead of awaiting one insert acknowledgement per token.
    pub async fn write_batch_async(
        &self,
        items: Vec<T>,
        timeouts: DurableWriteTimeouts,
    ) -> Result<(), DurableWriteError> {
        let mut receipt = self.enqueue_batch(items, timeouts).await?;
        let acknowledgement = receipt.acknowledge().await?;
        drop(acknowledgement);
        Ok(())
    }

    /// Admit one immutable batch and return its separately awaited receipt.
    ///
    /// Queue slots and resident bytes share the same enqueue deadline. The
    /// acknowledgement deadline starts only after successful admission and is
    /// carried by the receipt, so postponing receipt polling cannot extend it.
    pub async fn enqueue_batch(
        &self,
        items: Vec<T>,
        timeouts: DurableWriteTimeouts,
    ) -> Result<DurableWriteReceipt, DurableWriteError> {
        let (acknowledgement, ack_rx) = flume::bounded(1);
        if items.is_empty() {
            let _ = acknowledgement.send(Ok(()));
            return Ok(DurableWriteReceipt {
                acknowledgement: Some(ack_rx),
                deadline: Instant::now() + timeouts.acknowledgement,
                reservation: None,
            });
        }
        let started = Instant::now();
        let reservation = self.reserve_resources(&items, timeouts.enqueue).await?;
        let remaining = timeouts.enqueue.saturating_sub(started.elapsed());
        let queued = DurableQueued {
            items,
            enqueued: Instant::now(),
            acknowledgement,
            reservation: reservation.as_ref().map(Arc::clone),
        };
        match timeout(remaining, self.tx.send_async(DurableCommand::Write(queued))).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(DurableWriteError::QueueClosed),
            Err(_) => return Err(DurableWriteError::QueueTimeout),
        }
        self.publish_queue_depth();
        Ok(DurableWriteReceipt {
            acknowledgement: Some(ack_rx),
            deadline: Instant::now() + timeouts.acknowledgement,
            reservation,
        })
    }

    /// Force every write admitted before this command through the sink.
    ///
    /// The flush command shares the data channel, which gives it an exact FIFO
    /// barrier relative to the caller's preceding writes. It is used when an
    /// ingress owner stops production and must drain receipts before the later
    /// analytics shutdown stage.
    pub async fn flush(&self, deadline: Duration) -> Result<(), DurableWriteError> {
        let started = Instant::now();
        let (acknowledgement, ack_rx) = flume::bounded(1);
        match timeout(
            deadline,
            self.tx.send_async(DurableCommand::Flush(acknowledgement)),
        )
        .await
        {
            Ok(Ok(())) => self.publish_queue_depth(),
            Ok(Err(_)) => return Err(DurableWriteError::QueueClosed),
            Err(_) => return Err(DurableWriteError::QueueTimeout),
        }
        let remaining = deadline.saturating_sub(started.elapsed());
        match timeout(remaining, ack_rx.recv_async()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(DurableWriteError::QueueClosed),
            Err(_) => Err(DurableWriteError::AcknowledgementTimeout),
        }
    }

    async fn reserve_resources(
        &self,
        items: &[T],
        enqueue_timeout: Duration,
    ) -> Result<Option<Arc<DurableResourceReservation>>, DurableWriteError> {
        if items.len() > self.item_limit {
            return Err(DurableWriteError::CapacityExceeded);
        }
        let item_permits =
            u32::try_from(items.len()).map_err(|_| DurableWriteError::CapacityExceeded)?;
        let started = Instant::now();
        let item_permit = timeout(
            enqueue_timeout,
            Arc::clone(&self.item_budget).acquire_many_owned(item_permits),
        )
        .await
        .map_err(|_| DurableWriteError::QueueTimeout)?
        .map_err(|_| DurableWriteError::QueueClosed)?;
        let (byte_permit, bytes) = if let Some(byte_budget) = &self.byte_budget {
            let remaining = enqueue_timeout.saturating_sub(started.elapsed());
            let bytes = self.batch_weight(items)?;
            let byte_permits =
                u32::try_from(bytes).map_err(|_| DurableWriteError::PayloadTooLarge)?;
            let permit = timeout(
                remaining,
                Arc::clone(byte_budget).acquire_many_owned(byte_permits),
            )
            .await
            .map_err(|_| DurableWriteError::QueueTimeout)?
            .map_err(|_| DurableWriteError::QueueClosed)?;
            (Some(permit), bytes)
        } else {
            (None, 0)
        };
        if let Some(gauge) = &self.observability.inflight_items {
            gauge.add(i64::try_from(items.len()).unwrap_or(i64::MAX));
        }
        if let Some(gauge) = &self.observability.inflight_bytes {
            gauge.add(i64::try_from(bytes).unwrap_or(i64::MAX));
        }
        Ok(Some(Arc::new(DurableResourceReservation {
            _byte_permit: byte_permit,
            _item_permit: item_permit,
            bytes,
            items: items.len(),
            item_gauge: self.observability.inflight_items.clone(),
            gauge: self.observability.inflight_bytes.clone(),
        })))
    }

    fn reserve_resources_sync(
        &self,
        items: &[T],
    ) -> Result<Option<Arc<DurableResourceReservation>>, DurableWriteError> {
        if items.len() > self.item_limit {
            return Err(DurableWriteError::CapacityExceeded);
        }
        let item_permits =
            u32::try_from(items.len()).map_err(|_| DurableWriteError::CapacityExceeded)?;
        let item_permit = Arc::clone(&self.item_budget)
            .try_acquire_many_owned(item_permits)
            .map_err(|_| DurableWriteError::QueueTimeout)?;
        let (byte_permit, bytes) = if let Some(byte_budget) = &self.byte_budget {
            let bytes = self.batch_weight(items)?;
            let byte_permits =
                u32::try_from(bytes).map_err(|_| DurableWriteError::PayloadTooLarge)?;
            let permit = Arc::clone(byte_budget)
                .try_acquire_many_owned(byte_permits)
                .map_err(|_| DurableWriteError::QueueTimeout)?;
            (Some(permit), bytes)
        } else {
            (None, 0)
        };
        if let Some(gauge) = &self.observability.inflight_items {
            gauge.add(i64::try_from(items.len()).unwrap_or(i64::MAX));
        }
        if let Some(gauge) = &self.observability.inflight_bytes {
            gauge.add(i64::try_from(bytes).unwrap_or(i64::MAX));
        }
        Ok(Some(Arc::new(DurableResourceReservation {
            _byte_permit: byte_permit,
            _item_permit: item_permit,
            bytes,
            items: items.len(),
            item_gauge: self.observability.inflight_items.clone(),
            gauge: self.observability.inflight_bytes.clone(),
        })))
    }

    fn batch_weight(&self, items: &[T]) -> Result<usize, DurableWriteError> {
        let Some(weight) = &self.weight else {
            return Ok(0);
        };
        let bytes = items.iter().try_fold(0_usize, |total, item| {
            total.checked_add(weight(item).max(1))
        });
        match (bytes, self.byte_limit) {
            (Some(bytes), Some(limit)) if bytes <= limit => Ok(bytes),
            _ => Err(DurableWriteError::PayloadTooLarge),
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

    #[must_use]
    pub const fn byte_limit(&self) -> Option<usize> {
        self.byte_limit
    }

    #[must_use]
    pub const fn item_limit(&self) -> usize {
        self.item_limit
    }
}

pub struct DurableWriterWorker<T: Send + 'static> {
    rx: Receiver<DurableCommand<T>>,
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
        let mut buffered_items = 0_usize;
        loop {
            match tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    Self::drain_and_flush(
                        &rx,
                        &flush,
                        name,
                        &observability,
                        max_batch_size,
                        &mut buffer,
                        &mut buffered_items,
                    ).await;
                    drop(buffer);
                    return;
                }
                item = rx.recv_async() => {
                    let Ok(item) = item else {
                        Self::flush_batch(&flush, name, &observability, &mut buffer).await;
                        drop(buffer);
                        return;
                    };
                    item
                }
            } {
                DurableCommand::Write(first) => {
                    buffered_items = buffered_items.saturating_add(first.items.len());
                    buffer.push(first);
                }
                DurableCommand::Flush(acknowledgement) => {
                    Self::flush_batch(&flush, name, &observability, &mut buffer).await;
                    buffered_items = 0;
                    let _ = acknowledgement.send(());
                    Self::publish_queue_depth(&rx, &observability);
                    continue;
                }
            }

            let deadline = tokio::time::sleep(max_batch_delay);
            tokio::pin!(deadline);
            let mut disconnected = false;
            while buffered_items < max_batch_size {
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
                        &mut buffered_items,
                    ).await;
                        drop(buffer);
                        return;
                    }
                    command = rx.recv_async() => match command {
                        Ok(DurableCommand::Write(item)) => {
                            buffered_items = buffered_items.saturating_add(item.items.len());
                            buffer.push(item);
                        }
                        Ok(DurableCommand::Flush(acknowledgement)) => {
                            Self::flush_batch(&flush, name, &observability, &mut buffer).await;
                            let _ = acknowledgement.send(());
                            break;
                        }
                        Err(_) => {
                            disconnected = true;
                            break;
                        }
                    },
                    () = &mut deadline => break,
                }
            }
            Self::flush_batch(&flush, name, &observability, &mut buffer).await;
            buffered_items = 0;
            Self::publish_queue_depth(&rx, &observability);
            if disconnected {
                drop(buffer);
                return;
            }
        }
    }

    async fn drain_and_flush(
        rx: &Receiver<DurableCommand<T>>,
        flush: &FlushFn<T>,
        name: &'static str,
        observability: &AsyncWriterObservability,
        max_batch_size: usize,
        buffer: &mut Vec<DurableQueued<T>>,
        buffered_items: &mut usize,
    ) {
        while let Ok(command) = rx.try_recv() {
            match command {
                DurableCommand::Write(item) => {
                    *buffered_items = buffered_items.saturating_add(item.items.len());
                    buffer.push(item);
                    if *buffered_items >= max_batch_size {
                        Self::flush_batch(flush, name, observability, buffer).await;
                        *buffered_items = 0;
                    }
                }
                DurableCommand::Flush(acknowledgement) => {
                    Self::flush_batch(flush, name, observability, buffer).await;
                    *buffered_items = 0;
                    let _ = acknowledgement.send(());
                }
            }
        }
        Self::flush_batch(flush, name, observability, buffer).await;
        *buffered_items = 0;
        Self::publish_queue_depth(rx, observability);
    }

    fn publish_queue_depth(
        rx: &Receiver<DurableCommand<T>>,
        observability: &AsyncWriterObservability,
    ) {
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
        let oldest_enqueued = buffer.iter().map(|item| item.enqueued).min();
        let mut batch = Vec::new();
        let mut acknowledgements = Vec::with_capacity(buffer.len());
        let mut reservations = Vec::with_capacity(buffer.len());
        for item in mem::take(buffer) {
            batch.extend(item.items);
            acknowledgements.push(item.acknowledgement);
            if let Some(reservation) = item.reservation {
                reservations.push(reservation);
            }
        }
        let persisted = match flush(batch).await {
            Ok(()) => {
                if let (Some(report), Some(oldest)) = (&observability.flush_lag_ms, oldest_enqueued)
                {
                    report(u64::try_from(oldest.elapsed().as_millis()).unwrap_or(u64::MAX));
                }
                Ok(())
            }
            Err(error) => {
                if let Some(counter) = &observability.flush_failed {
                    counter.inc();
                }
                warn!(writer = name, %error, "canonical batch persistence failed");
                Err(DurableWriteError::PersistenceFailed)
            }
        };
        for acknowledgement in acknowledgements {
            let _ = acknowledgement.send(persisted);
        }
        drop(reservations);
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
    use prometheus::IntGauge;
    use quant_pivot_error::storage::StorageError;
    use tokio::{task::JoinSet, time::sleep};
    use tokio_util::sync::CancellationToken;

    use super::{
        AsyncWriterObservability, DurableWriteError, DurableWriteTimeouts, DurableWriter,
        DurableWriterConfig,
    };

    const TEST_TIMEOUTS: DurableWriteTimeouts =
        DurableWriteTimeouts::new(Duration::from_millis(250), Duration::from_millis(250));

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
            move || writer.write(7, TEST_TIMEOUTS)
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
            producers.spawn(async move { writer.write_async(value, TEST_TIMEOUTS).await });
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

    #[tokio::test]
    async fn durable_batch_persists_rows() {
        let persisted = Arc::new(Mutex::new(Vec::<u32>::new()));
        let observed = Arc::clone(&persisted);
        let (writer, worker) = DurableWriter::new(
            DurableWriterConfig::new("durable-batch-test")
                .capacity(3)
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

        assert_eq!(
            writer.write_batch_async(vec![1, 2, 3], TEST_TIMEOUTS).await,
            Ok(())
        );
        assert_eq!(*persisted.lock(), vec![1, 2, 3]);
        shutdown.cancel();
        worker_task.await.expect("durable worker shutdown");
    }

    #[tokio::test]
    async fn durable_failure_reaches_producer() {
        let (writer, worker) = DurableWriter::new(
            DurableWriterConfig::new("durable-failure-test")
                .capacity(1)
                .max_batch_delay(Duration::from_millis(1)),
            |_rows: Vec<u32>| {
                Box::pin(async {
                    Err(StorageError::Connection(
                        "injected persistence failure".to_owned(),
                    ))
                })
            },
            AsyncWriterObservability::default(),
        );
        let shutdown = CancellationToken::new();
        let worker_task = tokio::spawn(worker.run(shutdown.clone()));

        assert_eq!(
            writer.write_async(1, TEST_TIMEOUTS).await,
            Err(DurableWriteError::PersistenceFailed)
        );
        shutdown.cancel();
        worker_task.await.expect("durable worker shutdown");
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_ack_survives_tail() {
        let persisted = Arc::new(Mutex::new(Vec::<u32>::new()));
        let observed = Arc::clone(&persisted);
        let (writer, worker) = DurableWriter::new(
            DurableWriterConfig::new("delayed-ack-test")
                .capacity(1)
                .max_batch_delay(Duration::from_millis(5)),
            move |rows| {
                let observed = Arc::clone(&observed);
                Box::pin(async move {
                    sleep(Duration::from_secs(7)).await;
                    observed.lock().extend(rows);
                    Ok(())
                })
            },
            AsyncWriterObservability::default(),
        );
        let shutdown = CancellationToken::new();
        let worker_task = tokio::spawn(worker.run(shutdown.clone()));

        assert_eq!(
            writer
                .write_async(
                    1,
                    DurableWriteTimeouts::new(Duration::from_millis(250), Duration::from_secs(12),),
                )
                .await,
            Ok(())
        );
        assert_eq!(*persisted.lock(), vec![1]);
        shutdown.cancel();
        worker_task.await.expect("durable worker shutdown");
    }

    #[tokio::test(start_paused = true)]
    async fn queue_timeout_stays_bounded() {
        let (writer, _worker) = DurableWriter::new(
            DurableWriterConfig::new("queue-timeout-test").capacity(1),
            |_rows: Vec<u32>| Box::pin(async { Ok(()) }),
            AsyncWriterObservability::default(),
        );
        assert_eq!(
            writer
                .write_async(
                    1,
                    DurableWriteTimeouts::new(
                        Duration::from_millis(250),
                        Duration::from_millis(1),
                    ),
                )
                .await,
            Err(DurableWriteError::AcknowledgementTimeout)
        );
        assert_eq!(
            writer.write_async(2, TEST_TIMEOUTS).await,
            Err(DurableWriteError::QueueTimeout)
        );
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
            writer.write(
                1,
                DurableWriteTimeouts::new(Duration::from_millis(10), Duration::from_millis(10),),
            ),
            Err(DurableWriteError::QueueClosed)
        );
    }

    #[tokio::test]
    async fn receipt_batches_single_producer() {
        let batches = Arc::new(Mutex::new(Vec::<Vec<u32>>::new()));
        let observed = Arc::clone(&batches);
        let (writer, worker) = DurableWriter::new(
            DurableWriterConfig::new("receipt-batch-test")
                .capacity(8)
                .max_batch_size(8)
                .max_batch_delay(Duration::from_secs(1)),
            move |rows| {
                let observed = Arc::clone(&observed);
                Box::pin(async move {
                    observed.lock().push(rows);
                    Ok(())
                })
            },
            AsyncWriterObservability::default(),
        );
        let shutdown = CancellationToken::new();
        let worker_task = tokio::spawn(worker.run(shutdown.clone()));
        let mut receipts = Vec::new();
        for value in 0..8 {
            receipts.push(
                writer
                    .enqueue_batch(vec![value], TEST_TIMEOUTS)
                    .await
                    .expect("admit high-frequency item"),
            );
        }
        for receipt in &mut receipts {
            let acknowledgement = receipt.acknowledge().await.expect("batch acknowledgement");
            drop(acknowledgement);
        }
        assert!(matches!(
            receipts[0].acknowledge().await,
            Err(DurableWriteError::AlreadyAcknowledged)
        ));
        drop(receipts);

        assert_eq!(*batches.lock(), vec![(0..8).collect::<Vec<_>>()]);
        shutdown.cancel();
        worker_task.await.expect("durable worker shutdown");
    }

    #[tokio::test]
    async fn receipt_batches_multiple_producers() {
        let batches = Arc::new(Mutex::new(Vec::<Vec<u32>>::new()));
        let observed = Arc::clone(&batches);
        let (writer, worker) = DurableWriter::new(
            DurableWriterConfig::new("receipt-multi-source-test")
                .capacity(8)
                .max_batch_size(8)
                .max_batch_delay(Duration::from_secs(1)),
            move |rows| {
                let observed = Arc::clone(&observed);
                Box::pin(async move {
                    observed.lock().push(rows);
                    Ok(())
                })
            },
            AsyncWriterObservability::default(),
        );
        let shutdown = CancellationToken::new();
        let worker_task = tokio::spawn(worker.run(shutdown.clone()));
        let writer = Arc::new(writer);
        let source = |offset: u32| {
            let writer = Arc::clone(&writer);
            async move {
                let mut receipts = Vec::new();
                for value in 0..4 {
                    receipts.push(
                        writer
                            .enqueue_batch(vec![offset + value], TEST_TIMEOUTS)
                            .await
                            .expect("admit source item"),
                    );
                }
                for receipt in &mut receipts {
                    let acknowledgement = receipt
                        .acknowledge()
                        .await
                        .expect("multi-source acknowledgement");
                    drop(acknowledgement);
                }
                drop(receipts);
            }
        };
        tokio::join!(source(0), source(100));

        {
            let batches = batches.lock();
            assert_eq!(batches.len(), 1);
            let first = batches[0]
                .iter()
                .copied()
                .filter(|value| *value < 100)
                .collect::<Vec<_>>();
            let second = batches[0]
                .iter()
                .copied()
                .filter(|value| *value >= 100)
                .collect::<Vec<_>>();
            drop(batches);
            assert_eq!(first, vec![0, 1, 2, 3]);
            assert_eq!(second, vec![100, 101, 102, 103]);
        }
        shutdown.cancel();
        worker_task.await.expect("durable worker shutdown");
    }

    #[tokio::test]
    async fn failed_batch_rejects_receipts() {
        let (writer, worker) = DurableWriter::new(
            DurableWriterConfig::new("receipt-failure-test")
                .capacity(3)
                .max_batch_size(3)
                .max_batch_delay(Duration::from_secs(1)),
            |_rows: Vec<u32>| {
                Box::pin(async {
                    Err(StorageError::Connection(
                        "injected receipt batch failure".to_owned(),
                    ))
                })
            },
            AsyncWriterObservability::default(),
        );
        let shutdown = CancellationToken::new();
        let worker_task = tokio::spawn(worker.run(shutdown.clone()));
        let mut receipts = Vec::new();
        for value in 0..3 {
            receipts.push(
                writer
                    .enqueue_batch(vec![value], TEST_TIMEOUTS)
                    .await
                    .expect("admit failing item"),
            );
        }
        for receipt in &mut receipts {
            assert!(matches!(
                receipt.acknowledge().await,
                Err(DurableWriteError::PersistenceFailed)
            ));
        }
        drop(receipts);
        shutdown.cancel();
        worker_task.await.expect("durable worker shutdown");
    }

    #[tokio::test]
    async fn acknowledgement_holds_bytes() {
        let inflight_items =
            IntGauge::new("weighted_receipt_test_items", "weighted receipt test items")
                .expect("item gauge");
        let inflight_bytes =
            IntGauge::new("weighted_receipt_test_bytes", "weighted receipt test bytes")
                .expect("byte gauge");
        let (writer, worker) = DurableWriter::new_weighted(
            DurableWriterConfig::new("weighted-receipt-test")
                .capacity(2)
                .max_batch_size(1)
                .max_batch_delay(Duration::from_millis(1)),
            2,
            |_value: &u32| 2,
            |_rows| Box::pin(async { Ok(()) }),
            AsyncWriterObservability {
                inflight_items: Some(inflight_items.clone()),
                inflight_bytes: Some(inflight_bytes.clone()),
                ..AsyncWriterObservability::default()
            },
        )
        .expect("weighted writer");
        let shutdown = CancellationToken::new();
        let worker_task = tokio::spawn(worker.run(shutdown.clone()));
        let mut first = writer
            .enqueue_batch(vec![1], TEST_TIMEOUTS)
            .await
            .expect("admit first weighted item");
        let acknowledgement = first.acknowledge().await.expect("acknowledge first item");
        assert_eq!(inflight_items.get(), 1);
        assert_eq!(inflight_bytes.get(), 2);

        assert!(matches!(
            writer
                .enqueue_batch(
                    vec![2],
                    DurableWriteTimeouts::new(
                        Duration::from_millis(10),
                        Duration::from_millis(250),
                    ),
                )
                .await,
            Err(DurableWriteError::QueueTimeout)
        ));
        drop(acknowledgement);
        assert_eq!(inflight_items.get(), 0);
        assert_eq!(inflight_bytes.get(), 0);
        let mut second = writer
            .enqueue_batch(vec![2], TEST_TIMEOUTS)
            .await
            .expect("admit after cursor guard release");
        let acknowledgement = second.acknowledge().await.expect("acknowledge second item");
        drop(acknowledgement);
        shutdown.cancel();
        worker_task.await.expect("durable worker shutdown");
    }

    #[tokio::test]
    async fn acknowledgement_holds_items() {
        let (writer, worker) = DurableWriter::new_weighted(
            DurableWriterConfig::new("weighted-count-test")
                .capacity(2)
                .max_batch_size(2)
                .max_batch_delay(Duration::from_millis(1)),
            100,
            |_value: &u32| 1,
            |_rows| Box::pin(async { Ok(()) }),
            AsyncWriterObservability::default(),
        )
        .expect("weighted writer");
        let shutdown = CancellationToken::new();
        let worker_task = tokio::spawn(worker.run(shutdown.clone()));
        let mut first = writer
            .enqueue_batch(vec![1, 2], TEST_TIMEOUTS)
            .await
            .expect("admit count-sized batch");
        let acknowledgement = first.acknowledge().await.expect("acknowledge count batch");

        assert!(matches!(
            writer
                .enqueue_batch(
                    vec![3],
                    DurableWriteTimeouts::new(
                        Duration::from_millis(10),
                        Duration::from_millis(250),
                    ),
                )
                .await,
            Err(DurableWriteError::QueueTimeout)
        ));
        drop(acknowledgement);
        let mut second = writer
            .enqueue_batch(vec![3], TEST_TIMEOUTS)
            .await
            .expect("admit after count guard release");
        let acknowledgement = second.acknowledge().await.expect("acknowledge next item");
        drop(acknowledgement);
        shutdown.cancel();
        worker_task.await.expect("durable worker shutdown");
    }

    #[tokio::test]
    async fn unweighted_receipt_holds_count() {
        let (writer, worker) = DurableWriter::new(
            DurableWriterConfig::new("unweighted-count-test")
                .capacity(1)
                .max_batch_size(1)
                .max_batch_delay(Duration::from_millis(1)),
            |_rows: Vec<u32>| Box::pin(async { Ok(()) }),
            AsyncWriterObservability::default(),
        );
        let shutdown = CancellationToken::new();
        let worker_task = tokio::spawn(worker.run(shutdown.clone()));
        let mut first = writer
            .enqueue_batch(vec![1], TEST_TIMEOUTS)
            .await
            .expect("admit unweighted item");
        let acknowledgement = first
            .acknowledge()
            .await
            .expect("acknowledge unweighted item");

        assert!(matches!(
            writer
                .enqueue_batch(
                    vec![2],
                    DurableWriteTimeouts::new(
                        Duration::from_millis(10),
                        Duration::from_millis(250),
                    ),
                )
                .await,
            Err(DurableWriteError::QueueTimeout)
        ));
        drop(acknowledgement);
        let mut second = writer
            .enqueue_batch(vec![2], TEST_TIMEOUTS)
            .await
            .expect("admit after unweighted guard release");
        let acknowledgement = second
            .acknowledge()
            .await
            .expect("acknowledge next unweighted item");
        drop(acknowledgement);
        shutdown.cancel();
        worker_task.await.expect("durable worker shutdown");
    }

    #[tokio::test]
    async fn flush_barrier_drains_receipts() {
        let persisted = Arc::new(Mutex::new(Vec::<u32>::new()));
        let observed = Arc::clone(&persisted);
        let (writer, worker) = DurableWriter::new(
            DurableWriterConfig::new("receipt-drain-test")
                .capacity(3)
                .max_batch_size(8)
                .max_batch_delay(Duration::from_mins(1)),
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
        let mut receipts = Vec::new();
        for value in 0..3 {
            receipts.push(
                writer
                    .enqueue_batch(vec![value], TEST_TIMEOUTS)
                    .await
                    .expect("admit shutdown item"),
            );
        }
        writer
            .flush(Duration::from_millis(250))
            .await
            .expect("force ingress shutdown flush");
        for receipt in &mut receipts {
            let acknowledgement = receipt.acknowledge().await.expect("shutdown drain ACK");
            drop(acknowledgement);
        }
        drop(receipts);
        shutdown.cancel();
        worker_task.await.expect("durable worker shutdown");
        assert_eq!(*persisted.lock(), vec![0, 1, 2]);
    }
}
