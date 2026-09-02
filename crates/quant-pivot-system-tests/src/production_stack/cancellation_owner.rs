//! Live ownership for the governed browser cancellation fixture.

use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{enums::quant::FeedbackCycleStatus, types::FeedbackCycleId};
use quant_pivot_repository::{
    postgres::PgFeedbackCycleRepository,
    traits::{FeedbackCycleClaim, FeedbackCycleRepository},
};
use sea_orm::DatabaseConnection;
use tokio::{
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod tests;

pub(super) struct FixtureCancellationOwner {
    cycle_id: FeedbackCycleId,
    cancellation: CancellationToken,
    task: Option<JoinHandle<Result<()>>>,
}

impl FixtureCancellationOwner {
    pub(super) fn start(
        db: DatabaseConnection,
        claim: FeedbackCycleClaim,
        lease_secs: u64,
    ) -> Self {
        let cycle_id = claim.cycle.feedback_cycle_id;
        let cancellation = CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            FixtureLease {
                repository: PgFeedbackCycleRepository::new(db),
                claim,
                lease_secs,
            }
            .run(worker_cancellation)
            .await
        });
        Self {
            cycle_id,
            cancellation,
            task: Some(task),
        }
    }

    pub(super) const fn cycle_id(&self) -> FeedbackCycleId {
        self.cycle_id
    }

    pub(super) async fn shutdown(mut self) -> Result<()> {
        self.cancellation.cancel();
        let mut task = self
            .task
            .take()
            .context("cancellation owner task is missing")?;
        match timeout(Duration::from_secs(30), &mut task).await {
            Ok(result) => result.context("join governed cancellation fixture owner")?,
            Err(error) => {
                task.abort();
                let drain = task.await;
                ensure!(
                    drain.is_err_and(|error| error.is_cancelled()),
                    "timed-out cancellation owner did not abort cleanly"
                );
                Err(error).context("drain governed cancellation fixture owner")
            }
        }
    }
}

impl Drop for FixtureCancellationOwner {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = &self.task {
            // Normal shutdown joins and releases before the database closes.
            // An unwinding owner must not leave a detached renewal task alive.
            task.abort();
        }
    }
}

struct FixtureLease {
    repository: PgFeedbackCycleRepository,
    claim: FeedbackCycleClaim,
    lease_secs: u64,
}

impl FixtureLease {
    async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        ensure!(
            (3..=3_600).contains(&self.lease_secs),
            "fixture lease must satisfy the production three-to-3600-second budget"
        );
        let renewal_interval = Duration::from_secs(self.lease_secs) / 3;
        let io_budget = renewal_interval.min(Duration::from_secs(10));
        let poll_interval = renewal_interval.min(Duration::from_secs(1));
        let mut renew_at = Instant::now();
        loop {
            let stopping = cancellation.is_cancelled();
            let renewing = Instant::now() >= renew_at;
            let released = timeout(io_budget, self.maintain(stopping, renewing))
                .await
                .context("governed cancellation fixture lease I/O timed out")??;
            if released {
                return Ok(());
            }
            if renewing {
                renew_at = Instant::now() + renewal_interval;
            }
            tokio::select! {
                () = cancellation.cancelled() => {},
                () = sleep(poll_interval) => {},
            }
        }
    }

    async fn maintain(&mut self, stopping: bool, renewing: bool) -> Result<bool> {
        // The governed cancel command legitimately advances generation while
        // retaining this lease. Re-read it before any CAS, including shutdown.
        for attempt in 0..3 {
            let cycle = self
                .repository
                .find_cycle(&self.claim.lease.feedback_cycle_id)
                .await?
                .context("governed cancellation fixture cycle disappeared")?;
            ensure!(
                cycle.status == FeedbackCycleStatus::Running
                    && cycle.lease_owner == Some(self.claim.lease.worker_id),
                "governed cancellation fixture lost its live running owner: {}",
                cycle.feedback_cycle_id
            );
            let releasing = stopping || cycle.cancel_requested_at.is_some();
            self.claim.lease = self.claim.lease.with_generation(cycle.generation);
            self.claim.cycle = cycle;
            if !releasing && !renewing {
                return Ok(false);
            }
            let mutation = if releasing {
                self.repository.release_cycle_lease(self.claim.lease).await
            } else {
                self.repository
                    .renew_cycle_lease(self.claim.lease, self.lease_secs)
                    .await
            };
            match mutation {
                Ok(cycle) => {
                    self.claim.lease = self.claim.lease.with_generation(cycle.generation);
                    self.claim.cycle = cycle;
                    return Ok(releasing);
                }
                Err(StorageError::StateConflict { .. }) if attempt < 2 => {
                    // A concurrent cancellation must be observed on the next CAS attempt.
                }
                Err(error) => return Err(error).context("maintain exact fixture cycle lease"),
            }
        }
        bail!("fixture cancellation lease exhausted bounded CAS retries")
    }
}
