//! Async batch inserter for `ClickHouse` with backpressure, retry, and metrics.

use crate::clickhouse::{ChWriteManager, ChWriteMetrics};
use clickhouse::{Client, RowOwned, RowWrite};
use num_traits::ToPrimitive;
use quant_pivot_error::storage::StorageError;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub struct BatchInserter<T: RowOwned + RowWrite + Clone + Send + Sync> {
    tx: mpsc::Sender<InsertMessage<T>>,
    _handle: JoinHandle<()>,
}

enum InsertMessage<T> {
    Row(T),
    Batch(Vec<T>),
}

struct FlushLoop<T: RowOwned + RowWrite + Clone + Send + Sync> {
    client: Client,
    table: &'static str,
    rx: mpsc::Receiver<InsertMessage<T>>,
    batch_size: usize,
    flush_interval: Duration,
    write_manager: Arc<ChWriteManager>,
    metrics: Arc<ChWriteMetrics>,
    shutdown: CancellationToken,
}

impl<T: RowOwned + RowWrite + Clone + Send + Sync> BatchInserter<T> {
    /// Create a new `BatchInserter` with integrated metrics reporting.
    ///
    /// Every successful flush records `rows_written` and `insert_duration`.
    /// Every failed flush (after retries exhausted) increments `insert_errors`.
    pub fn new(
        client: Client,
        table: &'static str,
        batch_size: usize,
        flush_interval: Duration,
        write_manager: Arc<ChWriteManager>,
        shutdown: CancellationToken,
    ) -> Self {
        let (tx, rx) = mpsc::channel(batch_size * 4);
        let metrics = Arc::clone(write_manager.metrics());

        let handle = tokio::spawn(Self::flush_loop(FlushLoop {
            client,
            table,
            rx,
            batch_size,
            flush_interval,
            write_manager,
            metrics,
            shutdown,
        }));

        Self {
            tx,
            _handle: handle,
        }
    }

    pub async fn insert(&self, row: T) -> Result<(), StorageError> {
        self.tx
            .send(InsertMessage::Row(row))
            .await
            .map_err(|_| StorageError::ChannelClosed("BatchInserter channel closed".into()))
    }

    pub async fn insert_many(&self, rows: impl IntoIterator<Item = T>) -> Result<(), StorageError> {
        self.insert_batch(rows.into_iter().collect()).await
    }

    pub async fn insert_batch(&self, rows: Vec<T>) -> Result<(), StorageError> {
        if rows.is_empty() {
            return Ok(());
        }
        self.tx
            .send(InsertMessage::Batch(rows))
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

    async fn flush_loop(args: FlushLoop<T>) {
        let FlushLoop {
            client,
            table,
            mut rx,
            batch_size,
            flush_interval,
            write_manager,
            metrics,
            shutdown,
        } = args;
        let mut buffer = Vec::with_capacity(batch_size);
        let mut interval = tokio::time::interval(flush_interval);

        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    while let Ok(message) = rx.try_recv() {
                        push_message(&mut buffer, message);
                    }
                    if !buffer.is_empty() {
                        Self::flush_with_retry(
                            &client,
                            table,
                            &mut buffer,
                            &write_manager,
                            &metrics,
                        )
                        .await;
                    }
                    info!(table, "BatchInserter shut down via cancellation, all data flushed");
                    break;
                }
                Some(message) = rx.recv() => {
                    push_message(&mut buffer, message);
                    if buffer.len() >= batch_size {
                        Self::flush_with_retry(
                            &client,
                            table,
                            &mut buffer,
                            &write_manager,
                            &metrics,
                        )
                        .await;
                    }
                }
                _ = interval.tick() => {
                    if !buffer.is_empty() {
                        Self::flush_with_retry(
                            &client,
                            table,
                            &mut buffer,
                            &write_manager,
                            &metrics,
                        )
                        .await;
                    }
                }
                else => {
                    while let Ok(message) = rx.try_recv() {
                        push_message(&mut buffer, message);
                    }
                    if !buffer.is_empty() {
                        Self::flush_with_retry(
                            &client,
                            table,
                            &mut buffer,
                            &write_manager,
                            &metrics,
                        )
                        .await;
                    }
                    info!(table, "BatchInserter channel closed, all data flushed");
                    break;
                }
            }
        }
    }

    async fn flush_with_retry(
        client: &Client,
        table: &'static str,
        buffer: &mut Vec<T>,
        write_manager: &ChWriteManager,
        metrics: &ChWriteMetrics,
    ) {
        const MAX_RETRIES: u32 = 3;
        let count = buffer.len();

        for attempt in 0..MAX_RETRIES {
            let start = Instant::now();
            let permit = match write_manager.acquire_write_permit().await {
                Ok(permit) => permit,
                Err(e) => {
                    metrics.insert_errors.with_label_values(&[table]).inc();
                    error!(table, rows = count, error = %e, "ClickHouse write permit unavailable");
                    return;
                }
            };
            let result = Self::flush(client, table, buffer).await;
            drop(permit);
            match result {
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

    async fn flush(client: &Client, table: &str, buffer: &mut Vec<T>) -> Result<(), StorageError> {
        let mut insert = client.insert::<T>(table).await?;

        for row in buffer.iter() {
            insert.write(row).await?;
        }

        insert.end().await?;
        buffer.clear();
        Ok(())
    }
}

fn push_message<T>(buffer: &mut Vec<T>, message: InsertMessage<T>) {
    match message {
        InsertMessage::Row(row) => buffer.push(row),
        InsertMessage::Batch(rows) => buffer.extend(rows),
    }
}
