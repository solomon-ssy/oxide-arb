use crate::observability::metrics_hub::MetricsHub;
use oxide_arb_error::OxideError;
use prometheus::IntCounter;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

type AsyncWriterWorker = Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>;

pub struct AsyncWriter<T: Send + 'static> {
    tx: flume::Sender<T>,
    name: String,
    drops: IntCounter,
}

impl<T: Send + 'static> AsyncWriter<T> {
    pub fn new<F>(
        name: impl Into<String>,
        batch_size: usize,
        flush_interval: Duration,
        flush_fn: F,
        metrics: Arc<MetricsHub>,
        shutdown: CancellationToken,
    ) -> (Self, AsyncWriterWorker)
    where
        F: Fn(Vec<T>) -> Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>
            + Send
            + 'static,
    {
        let (tx, rx) = flume::bounded(4096);
        let name = name.into();
        let drops = metrics
            .async_writer_dropped
            .with_label_values(&[name.as_str()]);
        drop(metrics);
        let writer = Self {
            tx,
            name: name.clone(),
            drops,
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
        if let Err(e) = self.tx.try_send(item) {
            self.drops.inc();
            tracing::warn!(writer = %self.name, "channel full or closed, dropping item: {e}");
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
