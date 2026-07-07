//! External domain-source ingestion worker.

use std::{sync::Arc, time::Duration};

use quant_pivot_error::QuantResult;
use tokio_util::sync::CancellationToken;

use crate::{infra::periodic_task::PeriodicTask, service::domain_ingest::DomainIngestor};

/// Periodically ingests external domain observations into `quant_domain_observation`.
pub struct DomainIngestWorker {
    ingestor: Arc<DomainIngestor>,
    poll_secs: u64,
}

impl DomainIngestWorker {
    #[must_use]
    pub const fn new(ingestor: Arc<DomainIngestor>, poll_secs: u64) -> Self {
        Self {
            ingestor,
            poll_secs,
        }
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) -> QuantResult<()> {
        let poll_secs = self.poll_secs.max(1);
        let worker = Arc::clone(&self);
        PeriodicTask::run(
            "domain-ingest-worker",
            move || Duration::from_secs(poll_secs),
            0.05,
            false,
            shutdown,
            move || {
                let worker = Arc::clone(&worker);
                async move { worker.ingestor.run_once().await }
            },
        )
        .await
    }
}
