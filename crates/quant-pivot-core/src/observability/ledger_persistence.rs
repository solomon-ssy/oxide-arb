//! Batched persistence barrier for the canonical L2 ledger.

use std::{sync::Arc, time::Duration};

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
    time::{Instant, timeout},
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
    QueueClosed {
        batch_id: PartitionBatchId,
    },
    PublicationQuarantined {
        batch_id: PartitionBatchId,
    },
    ReconciliationTimeout {
        batch_id: PartitionBatchId,
    },
    CommitCursorClosed {
        batch_id: PartitionBatchId,
    },
    CommitFailed {
        batch_id: PartitionBatchId,
        generation: u64,
    },
    ClientFenced {
        batch_id: PartitionBatchId,
    },
}

impl LedgerPersistenceError {
    #[must_use]
    pub(crate) const fn requires_fail_stop(self) -> bool {
        matches!(
            self,
            Self::QueueClosed { .. }
                | Self::ReconciliationTimeout { .. }
                | Self::CommitCursorClosed { .. }
                | Self::ClientFenced { .. }
        )
    }

    #[must_use]
    pub(crate) const fn batch_id(self) -> Option<PartitionBatchId> {
        match self {
            Self::QueueClosed { batch_id }
            | Self::PublicationQuarantined { batch_id }
            | Self::ReconciliationTimeout { batch_id }
            | Self::CommitCursorClosed { batch_id }
            | Self::CommitFailed { batch_id, .. }
            | Self::ClientFenced { batch_id } => Some(batch_id),
            Self::QueueTimeout => None,
        }
    }
}

/// Independent admission, publication-quarantine, and final reconciliation
/// budgets for one ledger request. Performance gates own latency SLOs.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LedgerPersistenceBudgets {
    enqueue: Duration,
    publication_quarantine: Duration,
    reconciliation_ceiling: Duration,
}

impl LedgerPersistenceBudgets {
    #[must_use]
    pub(crate) const fn new(
        enqueue: Duration,
        publication_quarantine: Duration,
        reconciliation_ceiling: Duration,
    ) -> Self {
        Self {
            enqueue,
            publication_quarantine,
            reconciliation_ceiling,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct InFlightCommit {
    batch_id: PartitionBatchId,
    reconcile_by: Instant,
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
            fenced_on: None,
        })
    }
}

/// Single-partition producer. `&mut self` serializes submit/wait operations so
/// one partition never has more than one batch in flight.
pub struct PartitionLedgerClient {
    partition_id: PartitionId,
    request_tx: MpscSender<QueuedLedgerWriteRequest>,
    commit: WatchReceiver<PartitionCommit>,
    in_flight: Option<InFlightCommit>,
    fenced_on: Option<PartitionBatchId>,
}

impl PartitionLedgerClient {
    /// Submit one immutable partition batch and wait for its bounded durable
    /// commit cursor before the caller publishes derived book state.
    pub(crate) async fn persist(
        &mut self,
        batch_id: PartitionBatchId,
        rows: Vec<BookL2LedgerRow>,
        budgets: LedgerPersistenceBudgets,
    ) -> Result<(), LedgerPersistenceError> {
        if let Some(batch_id) = self.fenced_on {
            return Err(LedgerPersistenceError::ClientFenced { batch_id });
        }
        self.reconcile_outstanding().await?;
        let submitted_at = Instant::now();
        let request = QueuedLedgerWriteRequest {
            request: LedgerWriteRequest {
                partition_id: self.partition_id,
                batch_id,
                rows,
            },
            enqueued_at: submitted_at,
        };
        match timeout(budgets.enqueue, self.request_tx.send(request)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(LedgerPersistenceError::QueueClosed { batch_id }),
            Err(_) => return Err(LedgerPersistenceError::QueueTimeout),
        }
        let publication_by = submitted_at + budgets.publication_quarantine;
        self.in_flight = Some(InFlightCommit {
            batch_id,
            reconcile_by: submitted_at + budgets.reconciliation_ceiling,
        });
        let remaining = publication_by.saturating_duration_since(Instant::now());
        match timeout(remaining, self.wait_for(batch_id)).await {
            Ok(result) => self.finish_terminal(result),
            Err(_) => self.observed_terminal(batch_id).map_or(
                Err(LedgerPersistenceError::PublicationQuarantined { batch_id }),
                |result| self.finish_terminal(result),
            ),
        }
    }

    pub(crate) async fn reconcile_outstanding(&mut self) -> Result<(), LedgerPersistenceError> {
        let Some(in_flight) = self.in_flight else {
            return Ok(());
        };
        let remaining = in_flight
            .reconcile_by
            .saturating_duration_since(Instant::now());
        let result = timeout(remaining, self.wait_for(in_flight.batch_id))
            .await
            .unwrap_or_else(|_| {
                self.observed_terminal(in_flight.batch_id).unwrap_or(Err(
                    LedgerPersistenceError::ReconciliationTimeout {
                        batch_id: in_flight.batch_id,
                    },
                ))
            });
        if matches!(
            result,
            Err(LedgerPersistenceError::ReconciliationTimeout { .. }
                | LedgerPersistenceError::CommitCursorClosed { .. })
        ) {
            self.fenced_on = Some(in_flight.batch_id);
        } else {
            self.in_flight = None;
        }
        result
    }

    const fn finish_terminal(
        &mut self,
        result: Result<(), LedgerPersistenceError>,
    ) -> Result<(), LedgerPersistenceError> {
        if let Err(LedgerPersistenceError::CommitCursorClosed { batch_id }) = result {
            self.fenced_on = Some(batch_id);
        } else {
            self.in_flight = None;
        }
        result
    }

    async fn wait_for(&mut self, batch_id: PartitionBatchId) -> Result<(), LedgerPersistenceError> {
        loop {
            if let Some(result) = self.observed_terminal(batch_id) {
                return result;
            }
            self.commit
                .changed()
                .await
                .map_err(|_| LedgerPersistenceError::CommitCursorClosed { batch_id })?;
        }
    }

    fn observed_terminal(
        &mut self,
        batch_id: PartitionBatchId,
    ) -> Option<Result<(), LedgerPersistenceError>> {
        let state = *self.commit.borrow_and_update();
        match state {
            PartitionCommit::Committed(committed) if committed >= batch_id => Some(Ok(())),
            PartitionCommit::Failed {
                batch_id: failed,
                generation,
            } if failed >= batch_id => Some(Err(LedgerPersistenceError::CommitFailed {
                batch_id: failed,
                generation,
            })),
            PartitionCommit::Pending
            | PartitionCommit::Committed(_)
            | PartitionCommit::Failed { .. } => None,
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
        if let Some(oldest) = self.commits.iter().map(|commit| commit.enqueued_at).min() {
            self.observe_stage("admission_to_sink", oldest);
        }
        let sink_started = Instant::now();
        let persisted = self.sink.write_batch_borrowed(&self.rows).await;
        self.observe_stage("sink_ack", sink_started);
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

    fn observe_stage(&self, stage: &'static str, started: Instant) {
        if let Some(report) = &self.observability.stage_lag_ms {
            report(
                stage,
                u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            );
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
        time::{Instant, sleep},
    };
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{
        LedgerPersistenceBudgets, LedgerPersistenceCoordinator, LedgerPersistenceError,
        LedgerWriteRequest, PartitionCommit, PartitionLedgerClient, QueuedLedgerWriteRequest,
    };

    #[derive(Default)]
    struct RecordingSink {
        batches: Mutex<Vec<Vec<BookL2LedgerRow>>>,
        failures_remaining: AtomicU64,
        write_delay: Duration,
    }

    #[async_trait::async_trait]
    impl FactWriter<BookL2LedgerRow> for RecordingSink {
        async fn write_batch(&self, rows: Vec<BookL2LedgerRow>) -> Result<(), StorageError> {
            if !self.write_delay.is_zero() {
                sleep(self.write_delay).await;
            }
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
            trade_transaction_hash: None,
            venue_event_time: i64::from(partition),
            ingress_time: i64::from(partition),
            persisted_time: i64::from(partition),
            event_hash: ChDigest::new([partition; 32]),
            schema_version: BookL2LedgerRow::SCHEMA_VERSION,
        }
    }

    const TEST_BUDGETS: LedgerPersistenceBudgets = LedgerPersistenceBudgets::new(
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(5),
    );

    #[tokio::test]
    async fn coordinator_aggregates_advances_cursors() {
        let sink = Arc::new(RecordingSink::default());
        let stages = Arc::new(Mutex::new(Vec::new()));
        let observed_stages = Arc::clone(&stages);
        let (handle, coordinator) = LedgerPersistenceCoordinator::new(
            Arc::clone(&sink) as Arc<dyn FactWriter<BookL2LedgerRow>>,
            AsyncWriterObservability {
                stage_lag_ms: Some(Arc::new(move |stage, _| {
                    observed_stages.lock().push(stage);
                })),
                ..AsyncWriterObservability::default()
            },
        );
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(coordinator.run(shutdown.clone()));
        let mut first = handle.partition(PartitionId::new(0)).expect("partition 0");
        let mut second = handle.partition(PartitionId::new(1)).expect("partition 1");

        let (first_result, second_result) = tokio::join!(
            first.persist(PartitionBatchId::new(1), vec![row(0, 1)], TEST_BUDGETS),
            second.persist(PartitionBatchId::new(1), vec![row(1, 1)], TEST_BUDGETS),
        );
        assert_eq!(first_result, Ok(()));
        assert_eq!(second_result, Ok(()));
        assert_eq!(sink.batches.lock()[0].len(), 2);
        assert_eq!(*stages.lock(), ["admission_to_sink", "sink_ack"]);

        shutdown.cancel();
        task.await.expect("coordinator");
    }

    #[tokio::test]
    async fn failed_batch_advances_commit() {
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
                .persist(PartitionBatchId::new(1), vec![row(2, 1)], TEST_BUDGETS,)
                .await,
            Err(LedgerPersistenceError::CommitFailed {
                batch_id: PartitionBatchId::new(1),
                generation: 1,
            })
        );
        assert_eq!(
            client
                .persist(PartitionBatchId::new(2), vec![row(2, 2)], TEST_BUDGETS,)
                .await,
            Ok(())
        );
        assert_eq!(sink.batches.lock().len(), 1);

        shutdown.cancel();
        task.await.expect("coordinator");
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_commit_within_budget() {
        let delay = Duration::from_millis(300);
        let budgets = LedgerPersistenceBudgets::new(
            Duration::from_millis(250),
            Duration::from_secs(2),
            Duration::from_secs(12),
        );
        let sink = Arc::new(RecordingSink {
            write_delay: delay,
            ..RecordingSink::default()
        });
        let (handle, coordinator) = LedgerPersistenceCoordinator::new(
            Arc::clone(&sink) as Arc<dyn FactWriter<BookL2LedgerRow>>,
            AsyncWriterObservability::default(),
        );
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(coordinator.run(shutdown.clone()));
        let mut client = handle.partition(PartitionId::new(0)).expect("partition 0");

        assert_eq!(
            client
                .persist(PartitionBatchId::new(1), vec![row(0, 1)], budgets)
                .await,
            Ok(())
        );
        assert_eq!(sink.batches.lock().len(), 1);

        shutdown.cancel();
        task.await.expect("coordinator");
    }

    #[tokio::test(start_paused = true)]
    async fn enqueue_timeout_stays_bounded() {
        let (request_tx, _request_rx) = mpsc::channel::<QueuedLedgerWriteRequest>(1);
        request_tx
            .send(QueuedLedgerWriteRequest {
                request: LedgerWriteRequest {
                    partition_id: PartitionId::new(0),
                    batch_id: PartitionBatchId::new(1),
                    rows: vec![row(0, 1)],
                },
                enqueued_at: Instant::now(),
            })
            .await
            .expect("prefill ledger request queue");
        let (_commit_tx, commit_rx) = watch::channel(PartitionCommit::Pending);
        let mut client = PartitionLedgerClient {
            partition_id: PartitionId::new(0),
            request_tx,
            commit: commit_rx,
            in_flight: None,
            fenced_on: None,
        };

        assert_eq!(
            client
                .persist(
                    PartitionBatchId::new(2),
                    vec![row(0, 2)],
                    LedgerPersistenceBudgets::new(
                        Duration::from_millis(250),
                        Duration::from_secs(2),
                        Duration::from_secs(12),
                    ),
                )
                .await,
            Err(LedgerPersistenceError::QueueTimeout)
        );
    }

    #[tokio::test]
    async fn cursor_observes_without_race() {
        let (request_tx, _request_rx) = mpsc::channel::<QueuedLedgerWriteRequest>(1);
        let (commit_tx, commit_rx) = watch::channel(PartitionCommit::Pending);
        let mut client = PartitionLedgerClient {
            partition_id: PartitionId::new(0),
            request_tx,
            commit: commit_rx,
            in_flight: None,
            fenced_on: None,
        };
        commit_tx.send_replace(PartitionCommit::Committed(PartitionBatchId::new(5)));
        assert_eq!(client.wait_for(PartitionBatchId::new(3)).await, Ok(()));

        let (closed_tx, closed_rx) = watch::channel(PartitionCommit::Pending);
        client.commit = closed_rx;
        drop(closed_tx);
        assert_eq!(
            client.wait_for(PartitionBatchId::new(6)).await,
            Err(LedgerPersistenceError::CommitCursorClosed {
                batch_id: PartitionBatchId::new(6),
            })
        );
    }

    #[tokio::test]
    async fn late_commit_before_submitted() {
        let (request_tx, mut request_rx) = mpsc::channel::<QueuedLedgerWriteRequest>(1);
        let (commit_tx, commit_rx) = watch::channel(PartitionCommit::Pending);
        let mut client = PartitionLedgerClient {
            partition_id: PartitionId::new(0),
            request_tx,
            commit: commit_rx,
            in_flight: None,
            fenced_on: None,
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
                    LedgerPersistenceBudgets::new(
                        Duration::from_secs(1),
                        Duration::from_millis(5),
                        Duration::from_secs(1),
                    ),
                )
                .await,
            Err(LedgerPersistenceError::PublicationQuarantined {
                batch_id: PartitionBatchId::new(1),
            })
        );
        assert_eq!(
            client
                .persist(PartitionBatchId::new(2), vec![row(0, 2)], TEST_BUDGETS,)
                .await,
            Ok(())
        );
        committer.await.expect("late committer");
    }

    #[tokio::test(start_paused = true)]
    async fn barrier_reconciles_late_commit() {
        let (request_tx, mut request_rx) = mpsc::channel::<QueuedLedgerWriteRequest>(1);
        let (commit_tx, commit_rx) = watch::channel(PartitionCommit::Pending);
        let mut client = PartitionLedgerClient {
            partition_id: PartitionId::new(0),
            request_tx,
            commit: commit_rx,
            in_flight: None,
            fenced_on: None,
        };
        let committer = tokio::spawn(async move {
            let first = request_rx.recv().await.expect("first queued batch");
            sleep(Duration::from_secs(3)).await;
            commit_tx.send_replace(PartitionCommit::Committed(first.request.batch_id));
            let second = request_rx.recv().await.expect("second queued batch");
            commit_tx.send_replace(PartitionCommit::Committed(second.request.batch_id));
        });
        let budgets = LedgerPersistenceBudgets::new(
            Duration::from_millis(250),
            Duration::from_secs(2),
            Duration::from_secs(12),
        );

        assert_eq!(
            client
                .persist(PartitionBatchId::new(1), vec![row(0, 1)], budgets)
                .await,
            Err(LedgerPersistenceError::PublicationQuarantined {
                batch_id: PartitionBatchId::new(1),
            })
        );
        assert_eq!(client.reconcile_outstanding().await, Ok(()));
        assert_eq!(
            client
                .persist(PartitionBatchId::new(2), vec![row(0, 2)], budgets)
                .await,
            Ok(())
        );
        committer.await.expect("late committer");
    }

    #[tokio::test(start_paused = true)]
    async fn barrier_reports_commit_failure() {
        let (request_tx, mut request_rx) = mpsc::channel::<QueuedLedgerWriteRequest>(1);
        let (commit_tx, commit_rx) = watch::channel(PartitionCommit::Pending);
        let mut client = PartitionLedgerClient {
            partition_id: PartitionId::new(0),
            request_tx,
            commit: commit_rx,
            in_flight: None,
            fenced_on: None,
        };
        let committer = tokio::spawn(async move {
            let first = request_rx.recv().await.expect("first queued batch");
            sleep(Duration::from_secs(3)).await;
            commit_tx.send_replace(PartitionCommit::Failed {
                batch_id: first.request.batch_id,
                generation: 1,
            });
            let second = request_rx.recv().await.expect("second queued batch");
            commit_tx.send_replace(PartitionCommit::Committed(second.request.batch_id));
        });
        let budgets = LedgerPersistenceBudgets::new(
            Duration::from_millis(250),
            Duration::from_secs(2),
            Duration::from_secs(12),
        );

        assert_eq!(
            client
                .persist(PartitionBatchId::new(1), vec![row(0, 1)], budgets)
                .await,
            Err(LedgerPersistenceError::PublicationQuarantined {
                batch_id: PartitionBatchId::new(1),
            })
        );
        assert_eq!(
            client.reconcile_outstanding().await,
            Err(LedgerPersistenceError::CommitFailed {
                batch_id: PartitionBatchId::new(1),
                generation: 1,
            })
        );
        assert_eq!(
            client
                .persist(PartitionBatchId::new(2), vec![row(0, 2)], budgets)
                .await,
            Ok(())
        );
        committer.await.expect("late committer");
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_commit_fences() {
        let (request_tx, _request_rx) = mpsc::channel::<QueuedLedgerWriteRequest>(1);
        let (_commit_tx, commit_rx) = watch::channel(PartitionCommit::Pending);
        let mut client = PartitionLedgerClient {
            partition_id: PartitionId::new(0),
            request_tx,
            commit: commit_rx,
            in_flight: None,
            fenced_on: None,
        };
        let budgets = LedgerPersistenceBudgets::new(
            Duration::from_millis(250),
            Duration::from_secs(2),
            Duration::from_secs(12),
        );

        assert_eq!(
            client
                .persist(PartitionBatchId::new(1), vec![row(0, 1)], budgets)
                .await,
            Err(LedgerPersistenceError::PublicationQuarantined {
                batch_id: PartitionBatchId::new(1),
            })
        );
        assert_eq!(
            client
                .persist(PartitionBatchId::new(2), vec![row(0, 2)], budgets)
                .await,
            Err(LedgerPersistenceError::ReconciliationTimeout {
                batch_id: PartitionBatchId::new(1),
            })
        );
        assert_eq!(
            client
                .persist(PartitionBatchId::new(3), vec![row(0, 3)], budgets)
                .await,
            Err(LedgerPersistenceError::ClientFenced {
                batch_id: PartitionBatchId::new(1),
            })
        );
    }

    #[test]
    fn loom_cursor_no_notification() {
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
