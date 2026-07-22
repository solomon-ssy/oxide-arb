//! Batched persistence barrier for the canonical L2 ledger.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use quant_pivot_models::{
    clickhouse::BookL2LedgerRow,
    types::{PartitionBatchId, PartitionId},
};
use quant_pivot_repository::traits::FactWriter;
use quant_pivot_storage::write::AsyncWriterObservability;
use tokio::{
    sync::{
        mpsc::{self, Receiver as MpscReceiver, Sender as MpscSender},
        watch::{self, Receiver as WatchReceiver, Sender as WatchSender},
    },
    time::timeout,
};
use tokio_util::sync::CancellationToken;

pub const LEDGER_PARTITION_COUNT: usize = 8;
const REQUEST_CAPACITY: usize = LEDGER_PARTITION_COUNT * 2;
const MAX_AGGREGATED_ROWS: usize = 8_192;
const MAX_AGGREGATION_DELAY: Duration = Duration::from_millis(20);

/// One partition-owned canonical ledger batch.
pub struct LedgerWriteRequest {
    pub partition_id: PartitionId,
    pub batch_id: PartitionBatchId,
    pub rows: Vec<BookL2LedgerRow>,
}

/// Latest durable state published to one partition's persistent cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionCommit {
    Pending,
    Committed(PartitionBatchId),
    Failed {
        batch_id: PartitionBatchId,
        generation: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedgerPersistenceError {
    QueueTimeout,
    QueueClosed,
    CommitTimeout,
    CommitCursorClosed,
    CommitFailed {
        batch_id: PartitionBatchId,
        generation: u64,
    },
}

/// Factory for the eight long-lived partition commit cursors.
#[derive(Clone)]
pub struct LedgerPersistenceHandle {
    request_tx: MpscSender<QueuedLedgerWriteRequest>,
    commit_receivers: Arc<[WatchReceiver<PartitionCommit>]>,
}

impl LedgerPersistenceHandle {
    #[must_use]
    pub fn partition(&self, partition_id: PartitionId) -> Option<PartitionLedgerClient> {
        let commit = self
            .commit_receivers
            .get(usize::from(partition_id.get()))?
            .clone();
        Some(PartitionLedgerClient {
            partition_id,
            request_tx: self.request_tx.clone(),
            commit,
            in_flight: None,
        })
    }
}

/// Single-partition producer. `&mut self` serializes submit/wait operations so
/// one partition never has more than one batch in flight.
pub struct PartitionLedgerClient {
    partition_id: PartitionId,
    request_tx: MpscSender<QueuedLedgerWriteRequest>,
    commit: WatchReceiver<PartitionCommit>,
    in_flight: Option<PartitionBatchId>,
}

impl PartitionLedgerClient {
    pub async fn persist(
        &mut self,
        batch_id: PartitionBatchId,
        rows: Vec<BookL2LedgerRow>,
        timeout_duration: Duration,
    ) -> Result<(), LedgerPersistenceError> {
        if let Some(in_flight) = self.in_flight {
            let recovered = self.wait_with_timeout(in_flight, timeout_duration).await;
            if !matches!(recovered, Err(LedgerPersistenceError::CommitTimeout)) {
                self.in_flight = None;
            }
            recovered?;
        }
        let request = QueuedLedgerWriteRequest {
            request: LedgerWriteRequest {
                partition_id: self.partition_id,
                batch_id,
                rows,
            },
            enqueued_at: Instant::now(),
        };
        match timeout(timeout_duration, self.request_tx.send(request)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(LedgerPersistenceError::QueueClosed),
            Err(_) => return Err(LedgerPersistenceError::QueueTimeout),
        }
        self.in_flight = Some(batch_id);
        let result = self.wait_with_timeout(batch_id, timeout_duration).await;
        if !matches!(result, Err(LedgerPersistenceError::CommitTimeout)) {
            self.in_flight = None;
        }
        result
    }

    async fn wait_with_timeout(
        &mut self,
        batch_id: PartitionBatchId,
        timeout_duration: Duration,
    ) -> Result<(), LedgerPersistenceError> {
        timeout(timeout_duration, self.wait_for(batch_id))
            .await
            .map_or(Err(LedgerPersistenceError::CommitTimeout), |result| result)
    }

    async fn wait_for(&mut self, batch_id: PartitionBatchId) -> Result<(), LedgerPersistenceError> {
        loop {
            let state = *self.commit.borrow_and_update();
            match state {
                PartitionCommit::Committed(committed) if committed >= batch_id => return Ok(()),
                PartitionCommit::Failed {
                    batch_id: failed,
                    generation,
                } if failed >= batch_id => {
                    return Err(LedgerPersistenceError::CommitFailed {
                        batch_id: failed,
                        generation,
                    });
                }
                PartitionCommit::Pending
                | PartitionCommit::Committed(_)
                | PartitionCommit::Failed { .. } => {}
            }
            self.commit
                .changed()
                .await
                .map_err(|_| LedgerPersistenceError::CommitCursorClosed)?;
        }
    }
}

struct QueuedLedgerWriteRequest {
    request: LedgerWriteRequest,
    enqueued_at: Instant,
}

struct PendingCommit {
    partition_id: PartitionId,
    batch_id: PartitionBatchId,
    enqueued_at: Instant,
}

/// The only writer task allowed to insert into `quant_book_l2_ledger`.
pub struct LedgerPersistenceCoordinator {
    request_rx: MpscReceiver<QueuedLedgerWriteRequest>,
    commit_senders: Vec<WatchSender<PartitionCommit>>,
    failure_generations: [u64; LEDGER_PARTITION_COUNT],
    sink: Arc<dyn FactWriter<BookL2LedgerRow>>,
    observability: AsyncWriterObservability,
    rows: Vec<BookL2LedgerRow>,
    commits: Vec<PendingCommit>,
    pending: Option<QueuedLedgerWriteRequest>,
}

impl LedgerPersistenceCoordinator {
    #[must_use]
    pub fn new(
        sink: Arc<dyn FactWriter<BookL2LedgerRow>>,
        observability: AsyncWriterObservability,
    ) -> (LedgerPersistenceHandle, Self) {
        let (request_tx, request_rx) = mpsc::channel(REQUEST_CAPACITY);
        let mut commit_senders = Vec::with_capacity(LEDGER_PARTITION_COUNT);
        let mut commit_receivers = Vec::with_capacity(LEDGER_PARTITION_COUNT);
        for _ in 0..LEDGER_PARTITION_COUNT {
            let (sender, receiver) = watch::channel(PartitionCommit::Pending);
            commit_senders.push(sender);
            commit_receivers.push(receiver);
        }
        let handle = LedgerPersistenceHandle {
            request_tx,
            commit_receivers: Arc::from(commit_receivers),
        };
        let coordinator = Self {
            request_rx,
            commit_senders,
            failure_generations: [0; LEDGER_PARTITION_COUNT],
            sink,
            observability,
            rows: Vec::with_capacity(MAX_AGGREGATED_ROWS),
            commits: Vec::with_capacity(LEDGER_PARTITION_COUNT),
            pending: None,
        };
        (handle, coordinator)
    }

    pub async fn run(mut self, shutdown: CancellationToken) {
        loop {
            let first = if let Some(request) = self.pending.take() {
                Some(request)
            } else {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        self.drain().await;
                        return;
                    }
                    request = self.request_rx.recv() => request,
                }
            };
            let Some(first) = first else {
                self.flush().await;
                return;
            };
            self.push(first);

            let deadline = tokio::time::sleep(MAX_AGGREGATION_DELAY);
            tokio::pin!(deadline);
            let mut shutting_down = false;
            while self.rows.len() < MAX_AGGREGATED_ROWS {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        shutting_down = true;
                        break;
                    }
                    request = self.request_rx.recv() => {
                        let Some(request) = request else {
                            shutting_down = true;
                            break;
                        };
                        if self.rows.len().saturating_add(request.request.rows.len())
                            > MAX_AGGREGATED_ROWS
                        {
                            self.pending = Some(request);
                            break;
                        }
                        self.push(request);
                    }
                    () = &mut deadline => break,
                }
            }
            self.flush().await;
            if shutting_down {
                self.drain().await;
                return;
            }
        }
    }

    fn push(&mut self, mut queued: QueuedLedgerWriteRequest) {
        let request = &mut queued.request;
        self.commits.push(PendingCommit {
            partition_id: request.partition_id,
            batch_id: request.batch_id,
            enqueued_at: queued.enqueued_at,
        });
        self.rows.append(&mut request.rows);
    }

    async fn drain(&mut self) {
        if let Some(request) = self.pending.take() {
            self.push(request);
        }
        while let Ok(request) = self.request_rx.try_recv() {
            if self.rows.len().saturating_add(request.request.rows.len()) > MAX_AGGREGATED_ROWS {
                self.flush().await;
            }
            self.push(request);
        }
        self.flush().await;
    }

    async fn flush(&mut self) {
        if self.commits.is_empty() {
            return;
        }
        let persisted = self.sink.write_batch_borrowed(&self.rows).await;
        match persisted {
            Ok(()) => {
                if let Some(report) = &self.observability.flush_lag_ms
                    && let Some(oldest) = self.commits.iter().map(|commit| commit.enqueued_at).min()
                {
                    report(u64::try_from(oldest.elapsed().as_millis()).unwrap_or(u64::MAX));
                }
                for commit in &self.commits {
                    if let Some(sender) = self
                        .commit_senders
                        .get(usize::from(commit.partition_id.get()))
                    {
                        sender.send_replace(PartitionCommit::Committed(commit.batch_id));
                    }
                }
            }
            Err(error) => {
                if let Some(counter) = &self.observability.flush_failed {
                    counter.inc();
                }
                tracing::error!(rows = self.rows.len(), %error, "L2 ledger batch persistence failed");
                for commit in &self.commits {
                    let index = usize::from(commit.partition_id.get());
                    let Some(generation) = self.failure_generations.get_mut(index) else {
                        continue;
                    };
                    *generation = generation.saturating_add(1);
                    if let Some(sender) = self.commit_senders.get(index) {
                        sender.send_replace(PartitionCommit::Failed {
                            batch_id: commit.batch_id,
                            generation: *generation,
                        });
                    }
                }
            }
        }
        self.rows.clear();
        self.commits.clear();
        if let Some(gauge) = &self.observability.queue_depth {
            gauge.set(i64::try_from(self.request_rx.len()).unwrap_or(i64::MAX));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };

    use loom::{
        sync::{Arc as LoomArc, Condvar as LoomCondvar, Mutex as LoomMutex},
        thread as loom_thread,
    };
    use parking_lot::Mutex;
    use quant_pivot_error::storage::StorageError;
    use quant_pivot_models::{
        clickhouse::{BookL2LedgerRow, ChDigest},
        enums::clickhouse::ChCanonicalBookEventType,
        types::{MarketId, PartitionBatchId, PartitionId, TokenId},
    };
    use quant_pivot_repository::traits::FactWriter;
    use quant_pivot_storage::write::AsyncWriterObservability;
    use tokio::{
        sync::{mpsc, watch},
        time::sleep,
    };
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{
        LedgerPersistenceCoordinator, LedgerPersistenceError, PartitionCommit,
        PartitionLedgerClient, QueuedLedgerWriteRequest,
    };

    #[derive(Default)]
    struct RecordingSink {
        batches: Mutex<Vec<Vec<BookL2LedgerRow>>>,
        failures_remaining: AtomicU64,
    }

    #[async_trait::async_trait]
    impl FactWriter<BookL2LedgerRow> for RecordingSink {
        async fn write_batch(&self, rows: Vec<BookL2LedgerRow>) -> Result<(), StorageError> {
            if self
                .failures_remaining
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(StorageError::Connection(
                    "injected ledger failure".to_owned(),
                ));
            }
            self.batches.lock().push(rows);
            Ok(())
        }
    }

    fn row(partition: u8, sequence: u64) -> BookL2LedgerRow {
        BookL2LedgerRow {
            stream_session_id: Uuid::from_u128(u128::from(partition) + 1),
            shard_id: u32::from(partition),
            token_id: TokenId::new(format!("token-{partition}")),
            market_id: Some(MarketId::new("market")),
            token_sequence: sequence,
            event_type: ChCanonicalBookEventType::Gap,
            bid_prices: Vec::new(),
            bid_sizes: Vec::new(),
            ask_prices: Vec::new(),
            ask_sizes: Vec::new(),
            old_tick_size: None,
            new_tick_size: None,
            trade_price: None,
            trade_side: None,
            trade_size: None,
            fee_rate_bps: None,
            venue_event_time: i64::from(partition),
            ingress_time: i64::from(partition),
            persisted_time: i64::from(partition),
            event_hash: ChDigest::new([partition; 32]),
            schema_version: BookL2LedgerRow::SCHEMA_VERSION,
        }
    }

    #[tokio::test]
    async fn coordinator_aggregates_partitions_and_advances_persistent_cursors() {
        let sink = Arc::new(RecordingSink::default());
        let (handle, coordinator) = LedgerPersistenceCoordinator::new(
            Arc::clone(&sink) as Arc<dyn FactWriter<BookL2LedgerRow>>,
            AsyncWriterObservability::default(),
        );
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(coordinator.run(shutdown.clone()));
        let mut first = handle.partition(PartitionId::new(0)).expect("partition 0");
        let mut second = handle.partition(PartitionId::new(1)).expect("partition 1");

        let (first_result, second_result) = tokio::join!(
            first.persist(
                PartitionBatchId::new(1),
                vec![row(0, 1)],
                Duration::from_secs(1),
            ),
            second.persist(
                PartitionBatchId::new(1),
                vec![row(1, 1)],
                Duration::from_secs(1),
            ),
        );
        assert_eq!(first_result, Ok(()));
        assert_eq!(second_result, Ok(()));
        assert_eq!(sink.batches.lock()[0].len(), 2);

        shutdown.cancel();
        task.await.expect("coordinator");
    }

    #[tokio::test]
    async fn failed_batch_advances_generation_and_next_batch_can_commit() {
        let sink = Arc::new(RecordingSink::default());
        sink.failures_remaining.store(1, Ordering::Release);
        let (handle, coordinator) = LedgerPersistenceCoordinator::new(
            Arc::clone(&sink) as Arc<dyn FactWriter<BookL2LedgerRow>>,
            AsyncWriterObservability::default(),
        );
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(coordinator.run(shutdown.clone()));
        let mut client = handle.partition(PartitionId::new(2)).expect("partition 2");

        assert_eq!(
            client
                .persist(
                    PartitionBatchId::new(1),
                    vec![row(2, 1)],
                    Duration::from_secs(1),
                )
                .await,
            Err(LedgerPersistenceError::CommitFailed {
                batch_id: PartitionBatchId::new(1),
                generation: 1,
            })
        );
        assert_eq!(
            client
                .persist(
                    PartitionBatchId::new(2),
                    vec![row(2, 2)],
                    Duration::from_secs(1),
                )
                .await,
            Ok(())
        );
        assert_eq!(sink.batches.lock().len(), 1);

        shutdown.cancel();
        task.await.expect("coordinator");
    }

    #[tokio::test]
    async fn cursor_observes_preexisting_jump_and_closed_state_without_race() {
        let (request_tx, _request_rx) = mpsc::channel::<QueuedLedgerWriteRequest>(1);
        let (commit_tx, commit_rx) = watch::channel(PartitionCommit::Pending);
        let mut client = PartitionLedgerClient {
            partition_id: PartitionId::new(0),
            request_tx,
            commit: commit_rx,
            in_flight: None,
        };
        commit_tx.send_replace(PartitionCommit::Committed(PartitionBatchId::new(5)));
        assert_eq!(client.wait_for(PartitionBatchId::new(3)).await, Ok(()));

        let (closed_tx, closed_rx) = watch::channel(PartitionCommit::Pending);
        client.commit = closed_rx;
        drop(closed_tx);
        assert_eq!(
            client.wait_for(PartitionBatchId::new(6)).await,
            Err(LedgerPersistenceError::CommitCursorClosed)
        );
    }

    #[tokio::test]
    async fn late_commit_is_recovered_before_the_next_batch_is_submitted() {
        let (request_tx, mut request_rx) = mpsc::channel::<QueuedLedgerWriteRequest>(1);
        let (commit_tx, commit_rx) = watch::channel(PartitionCommit::Pending);
        let mut client = PartitionLedgerClient {
            partition_id: PartitionId::new(0),
            request_tx,
            commit: commit_rx,
            in_flight: None,
        };
        let committer = tokio::spawn(async move {
            let first = request_rx.recv().await.expect("first queued batch");
            sleep(Duration::from_millis(20)).await;
            commit_tx.send_replace(PartitionCommit::Committed(first.request.batch_id));
            let second = request_rx.recv().await.expect("second queued batch");
            commit_tx.send_replace(PartitionCommit::Committed(second.request.batch_id));
        });

        assert_eq!(
            client
                .persist(
                    PartitionBatchId::new(1),
                    vec![row(0, 1)],
                    Duration::from_millis(5),
                )
                .await,
            Err(LedgerPersistenceError::CommitTimeout)
        );
        assert_eq!(
            client
                .persist(
                    PartitionBatchId::new(2),
                    vec![row(0, 2)],
                    Duration::from_secs(1),
                )
                .await,
            Ok(())
        );
        committer.await.expect("late committer");
    }

    #[test]
    fn loom_cursor_check_then_wait_has_no_lost_notification() {
        loom::model(|| {
            let cursor = LoomArc::new((LoomMutex::new(0_u64), LoomCondvar::new()));
            let reader = LoomArc::clone(&cursor);
            let waiter = loom_thread::spawn(move || {
                let (state, changed) = &*reader;
                let mut committed = state.lock().expect("cursor lock");
                while *committed < 7 {
                    committed = changed.wait(committed).expect("cursor wait");
                }
                assert_eq!(*committed, 7);
                drop(committed);
            });
            let writer = loom_thread::spawn(move || {
                let (state, changed) = &*cursor;
                *state.lock().expect("cursor lock") = 7;
                changed.notify_all();
            });
            writer.join().expect("writer");
            waiter.join().expect("waiter");
        });
    }
}
