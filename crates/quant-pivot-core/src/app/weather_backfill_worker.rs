//! Dedicated historical Weather forecast recovery lifecycle.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::app::weather_ingest_worker::WeatherIngestWorker;

pub struct WeatherBackfillWorker {
    ingest: Arc<WeatherIngestWorker>,
}

impl WeatherBackfillWorker {
    #[must_use]
    pub const fn new(ingest: Arc<WeatherIngestWorker>) -> Self {
        Self { ingest }
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        Arc::clone(&self.ingest).run_backfill(shutdown).await;
    }
}
