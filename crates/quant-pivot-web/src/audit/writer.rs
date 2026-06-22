//! Background drain that batches buffered rows into the operation log.
//!
//! Mirrors the risk-decision audit drain: a `select!` loop flushes on a size
//! threshold, on a periodic timer, and one final time on shutdown so no buffered
//! row is lost on graceful stop. Persistence failures are logged and the batch
//! dropped — the operation log is best-effort and must never wedge the process.

use std::{sync::Arc, time::Duration};

use flume::Receiver;
use quant_pivot_models::domain::NewOperationLog;
use quant_pivot_repository::traits::OperationLogRepository;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

/// Drain `rx` into `repo`, batching by size and time until `shutdown`.
///
/// Runs until the channel closes or `shutdown` is cancelled, flushing any
/// buffered rows before returning. Intended to be spawned as a background task
/// (by `spawn_web_server` in production, or the test harness).
pub async fn spawn_operation_log_writer(
    rx: Receiver<NewOperationLog>,
    repo: Arc<dyn OperationLogRepository>,
    batch_size: usize,
    flush_interval: Duration,
    shutdown: CancellationToken,
) {
    let mut batch: Vec<NewOperationLog> = Vec::with_capacity(batch_size);
    let mut flush_timer = tokio::time::interval(flush_interval);
    flush_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                drain_remaining(&rx, &mut batch);
                flush_batch(repo.as_ref(), &mut batch).await;
                return;
            }
            _ = flush_timer.tick(), if !batch.is_empty() => {
                flush_batch(repo.as_ref(), &mut batch).await;
            }
            received = rx.recv_async() => {
                // All senders dropped: flush the tail and exit.
                let Ok(log) = received else {
                    flush_batch(repo.as_ref(), &mut batch).await;
                    return;
                };
                batch.push(log);
                if batch.len() >= batch_size {
                    flush_batch(repo.as_ref(), &mut batch).await;
                }
            }
        }
    }
}

/// Non-blockingly pull every immediately-available row into `batch` (shutdown
/// path, so no further row is lost once the timer/recv arm stops running).
fn drain_remaining(rx: &Receiver<NewOperationLog>, batch: &mut Vec<NewOperationLog>) {
    while let Ok(log) = rx.try_recv() {
        batch.push(log);
    }
}

/// Persist and clear `batch`. Best-effort: a failed write is logged and the
/// batch dropped rather than retried, so the writer never wedges.
async fn flush_batch(repo: &dyn OperationLogRepository, batch: &mut Vec<NewOperationLog>) {
    if batch.is_empty() {
        return;
    }
    let rows = std::mem::take(batch);
    let count = rows.len();
    if let Err(error) = repo.append_batch(rows).await {
        tracing::warn!(%error, dropped = count, "operation-log batch write failed; dropping");
    }
}
