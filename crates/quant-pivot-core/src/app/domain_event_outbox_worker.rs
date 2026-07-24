//! Durable `PostgreSQL` outbox publisher for derived domain events.

use std::{sync::Arc, time::Duration};

use chrono::{Duration as ChronoDuration, Utc};
use quant_pivot_error::{
    QuantError, QuantResult,
    storage::{StorageError, entity::QUANT_DOMAIN_EVENT_OUTBOX},
};
use quant_pivot_models::{clickhouse::DomainEventRow, types::WorkerId};
use quant_pivot_repository::traits::{DomainProjectionRepository, FactWriter};
use tokio_util::sync::CancellationToken;

use crate::infra::periodic_task::PeriodicTask;

const BATCH_SIZE: u64 = 500;
const LEASE_DURATION: Duration = Duration::from_secs(30);

/// Publishes append-only derived domain events.
///
/// The typed `PostgreSQL` projection commits before publication. `ClickHouse`
/// writes are idempotent on `event_id`, so a crash safely replays the envelope.
pub struct DomainEventOutboxWorker {
    worker_id: WorkerId,
    projections: Arc<dyn DomainProjectionRepository>,
    writer: Arc<dyn FactWriter<DomainEventRow>>,
}

impl DomainEventOutboxWorker {
    #[must_use]
    pub fn new(
        projections: Arc<dyn DomainProjectionRepository>,
        writer: Arc<dyn FactWriter<DomainEventRow>>,
    ) -> Self {
        Self {
            worker_id: WorkerId::from_v7(),
            projections,
            writer,
        }
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) -> QuantResult<()> {
        let worker = Arc::clone(&self);
        PeriodicTask::run(
            "domain-event-outbox-worker",
            || Duration::from_secs(1),
            0.0,
            false,
            shutdown,
            move || {
                let worker = Arc::clone(&worker);
                async move { worker.run_once().await }
            },
        )
        .await
    }

    async fn run_once(&self) -> QuantResult<()> {
        let now = Utc::now();
        let lease_duration = ChronoDuration::from_std(LEASE_DURATION).map_err(|error| {
            QuantError::config(format!("invalid domain outbox lease duration: {error}"))
        })?;
        let events = self
            .projections
            .claim_pending_events(self.worker_id, now, now + lease_duration, BATCH_SIZE)
            .await?;
        if events.is_empty() {
            return Ok(());
        }
        let rows = events
            .iter()
            .map(|event| {
                DomainEventRow::try_from(event).map_err(|error| {
                    StorageError::invariant_violation(
                        Some(QUANT_DOMAIN_EVENT_OUTBOX),
                        format!("domain event payload serialization failed: {error}"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if let Err(error) = self.writer.write_batch(rows).await {
            for event in &events {
                if let Err(mark_error) = self
                    .projections
                    .mark_event_failed(&event.id, self.worker_id, error.to_string())
                    .await
                {
                    tracing::error!(
                        event_id = %event.id,
                        %mark_error,
                        "failed to record domain-event outbox delivery failure"
                    );
                }
            }
            return Err(error.into());
        }
        let published_at = Utc::now();
        for event in events {
            self.projections
                .mark_event_published(&event.id, self.worker_id, published_at)
                .await?;
        }
        Ok(())
    }
}
