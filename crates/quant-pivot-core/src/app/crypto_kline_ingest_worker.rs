//! Binance kline ingest and historical recovery worker.

use std::{sync::Arc, time::Duration};

use quant_pivot_error::QuantResult;
use tokio_util::sync::CancellationToken;

use crate::{
    app::domain_source_supervisor::DomainSourceSupervisor,
    infra::periodic_task::PeriodicTask,
    service::crypto_kline_ingest::{CryptoKlineBindingOutcome, CryptoKlineIngestor},
};

/// Periodically ingests external domain observations into `quant_domain_observation`.
pub struct CryptoKlineIngestWorker {
    ingestor: Arc<CryptoKlineIngestor>,
    source_supervisor: Arc<DomainSourceSupervisor>,
    poll_secs: u64,
}

impl CryptoKlineIngestWorker {
    #[must_use]
    pub const fn new(
        ingestor: Arc<CryptoKlineIngestor>,
        source_supervisor: Arc<DomainSourceSupervisor>,
        poll_secs: u64,
    ) -> Self {
        Self {
            ingestor,
            source_supervisor,
            poll_secs,
        }
    }

    /// Execute one finite tick and reconcile durable source health only after
    /// the ingestor has committed the matching cursor state.
    pub async fn run_once(&self) -> QuantResult<()> {
        for outcome in self.ingestor.run_once().await? {
            match outcome {
                CryptoKlineBindingOutcome::Recovered {
                    source_id,
                    instrument_key,
                } => {
                    self.source_supervisor
                        .mark_source_recovered(&source_id, &instrument_key)
                        .await?;
                }
                CryptoKlineBindingOutcome::Failed {
                    source_id,
                    instrument_key,
                    reason,
                } => {
                    self.source_supervisor
                        .mark_source_failed(&source_id, &instrument_key, reason)
                        .await?;
                }
            }
        }
        Ok(())
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
                async move { worker.run_once().await }
            },
        )
        .await
    }
}
