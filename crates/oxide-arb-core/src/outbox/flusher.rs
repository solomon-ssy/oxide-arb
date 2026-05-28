use super::{consumer::OutboxConsumer, event_store::EventStore};
use crate::observability::metrics_hub::MetricsHub;
use num_traits::ToPrimitive;
use oxide_arb_error::OxideError;
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub struct OutboxFlusher {
    event_store: Arc<dyn EventStore>,
    consumers: Vec<Arc<dyn OutboxConsumer>>,
    batch_size: usize,
    max_retries: i32,
    metrics: Arc<MetricsHub>,
    shutdown: CancellationToken,
}

impl OutboxFlusher {
    pub fn new(
        event_store: Arc<dyn EventStore>,
        consumers: Vec<Arc<dyn OutboxConsumer>>,
        batch_size: usize,
        max_retries: i32,
        metrics: Arc<MetricsHub>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            event_store,
            consumers,
            batch_size,
            max_retries,
            metrics,
            shutdown,
        }
    }

    pub async fn run(&self) -> Result<(), OxideError> {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => {
                    self.flush_once().await?;
                    return Ok(());
                }
                _ = interval.tick() => {
                    if let Err(e) = self.flush_once().await {
                        tracing::warn!(error = %e, "outbox flush failed");
                    }
                }
            }
        }
    }

    async fn flush_once(&self) -> Result<(), OxideError> {
        if self.consumers.is_empty() {
            return Ok(());
        }

        let events = self.event_store.fetch_pending(self.batch_size).await?;
        if events.is_empty() {
            return Ok(());
        }

        let mut published = Vec::new();
        for event in &events {
            let mut all_ok = true;
            for consumer in &self.consumers {
                if let Err(e) = consumer.consume(event).await {
                    tracing::warn!(
                        event_id = %event.event_id,
                        consumer = consumer.name(),
                        error = %e,
                        "consumer failed"
                    );
                    all_ok = false;
                }
            }

            if all_ok {
                published.push(event.event_id.clone());
            } else if event.publish_attempts >= self.max_retries {
                self.event_store
                    .mark_dead_letter(&event.event_id, "max retries exceeded")
                    .await?;
                self.metrics.outbox_dead_letters.inc();
            }
        }

        for id in &published {
            self.event_store.mark_published(id).await?;
        }
        self.metrics
            .outbox_flushed
            .inc_by(ToPrimitive::to_u64(&published.len()).unwrap_or(u64::MAX));

        Ok(())
    }
}
