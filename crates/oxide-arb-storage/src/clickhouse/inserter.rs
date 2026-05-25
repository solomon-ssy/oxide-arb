//! Async batch inserter for `ClickHouse` with backpressure, retry, and metrics.

use crate::clickhouse::ChWriteMetrics;
use num_traits::ToPrimitive;
use oxide_arb_error::storage::StorageError;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub struct BatchInserter<T: clickhouse::RowOwned + clickhouse::RowWrite + Send> {
    tx: mpsc::Sender<T>,
    _handle: JoinHandle<()>,
}

impl<T: clickhouse::RowOwned + clickhouse::RowWrite + Send> BatchInserter<T> {
    /// Create a new `BatchInserter` with integrated metrics reporting.
    ///
    /// Every successful flush records `rows_written` and `insert_duration`.
    /// Every failed flush (after retries exhausted) increments `insert_errors`.
    pub fn new(
        client: clickhouse::Client,
        table: &'static str,
        batch_size: usize,
        flush_interval: Duration,
        metrics: Arc<ChWriteMetrics>,
        shutdown: CancellationToken,
    ) -> Self {
        let (tx, rx) = mpsc::channel(batch_size * 4);

        let handle = tokio::spawn(Self::flush_loop(
            client,
            table,
            rx,
            batch_size,
            flush_interval,
            metrics,
            shutdown,
        ));

        Self {
            tx,
            _handle: handle,
        }
    }

    pub async fn insert(&self, row: T) -> Result<(), StorageError> {
        self.tx
            .send(row)
            .await
            .map_err(|_| StorageError::ChannelClosed("BatchInserter channel closed".into()))
    }

    /// Initiate graceful shutdown by dropping the sender side of the channel.
    ///
    /// For explicit shutdown, prefer cancelling the `CancellationToken` passed
    /// to `new()` — this drains the buffer and flushes remaining rows before
    /// the background loop exits. Dropping the `BatchInserter` achieves the
    /// same via channel closure.
    pub fn shutdown(self) {
        drop(self.tx);
    }

    async fn flush_loop(
        client: clickhouse::Client,
        table: &'static str,
        mut rx: mpsc::Receiver<T>,
        batch_size: usize,
        flush_interval: Duration,
        metrics: Arc<ChWriteMetrics>,
        shutdown: CancellationToken,
    ) {
        let mut buffer = Vec::with_capacity(batch_size);
        let mut interval = tokio::time::interval(flush_interval);

        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    while let Ok(row) = rx.try_recv() {
                        buffer.push(row);
                    }
                    if !buffer.is_empty() {
                        Self::flush_with_retry(&client, table, &mut buffer, &metrics).await;
                    }
                    info!(table, "BatchInserter shut down via cancellation, all data flushed");
                    break;
                }
                Some(row) = rx.recv() => {
                    buffer.push(row);
                    if buffer.len() >= batch_size {
                        Self::flush_with_retry(&client, table, &mut buffer, &metrics).await;
                    }
                }
                _ = interval.tick() => {
                    if !buffer.is_empty() {
                        Self::flush_with_retry(&client, table, &mut buffer, &metrics).await;
                    }
                }
                else => {
                    while let Ok(row) = rx.try_recv() {
                        buffer.push(row);
                    }
                    if !buffer.is_empty() {
                        Self::flush_with_retry(&client, table, &mut buffer, &metrics).await;
                    }
                    info!(table, "BatchInserter channel closed, all data flushed");
                    break;
                }
            }
        }
    }

    async fn flush_with_retry(
        client: &clickhouse::Client,
        table: &'static str,
        buffer: &mut Vec<T>,
        metrics: &ChWriteMetrics,
    ) {
        const MAX_RETRIES: u32 = 3;
        let count = buffer.len();

        for attempt in 0..MAX_RETRIES {
            let start = Instant::now();
            match Self::flush(client, table, buffer).await {
                Ok(()) => {
                    let elapsed = start.elapsed().as_secs_f64();
                    metrics
                        .rows_written
                        .with_label_values(&[table])
                        .inc_by(ToPrimitive::to_u64(&count).unwrap_or(u64::MAX));
                    metrics
                        .insert_duration_seconds
                        .with_label_values(&[table])
                        .set(elapsed);
                    return;
                }
                Err(e) => {
                    if attempt + 1 < MAX_RETRIES {
                        let delay = Duration::from_millis(100 * 2u64.pow(attempt));
                        warn!(
                            table,
                            attempt = attempt + 1,
                            rows = count,
                            error = %e,
                            "Flush failed, retrying in {delay:?}"
                        );
                        tokio::time::sleep(delay).await;
                    } else {
                        error!(
                            table,
                            rows = count,
                            error = %e,
                            "Flush failed after {MAX_RETRIES} attempts, dropping batch"
                        );
                        metrics.insert_errors.with_label_values(&[table]).inc();
                        buffer.clear();
                    }
                }
            }
        }
    }

    async fn flush(
        client: &clickhouse::Client,
        table: &str,
        buffer: &mut Vec<T>,
    ) -> Result<(), StorageError> {
        let mut insert = client.insert::<T>(table).await?;

        for row in buffer.drain(..) {
            insert.write(&row).await?;
        }

        insert.end().await?;
        Ok(())
    }
}
