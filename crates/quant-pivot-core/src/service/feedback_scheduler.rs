//! Database-authoritative automatic feedback-cycle scheduler.

use std::{cmp, sync::Arc, time::Duration};

use quant_pivot_error::{QuantResult, feedback::FeedbackError};
use quant_pivot_models::{domain::quant::FeedbackSchedulerClaim, types::WorkerId};
use quant_pivot_repository::traits::FeedbackSchedulerRepository;
use tokio::time::{Instant, MissedTickBehavior, interval, interval_at};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::app::ports::feedback_mutation::CoreFeedbackMutationPort;

const MAX_CLAIMS_PER_TICK: usize = 64;
const MAX_ERROR_CHARS: usize = 4_096;
const BASE_RETRY_SECS: u64 = 5;
const MAX_RETRY_SECS: u64 = 300;

/// Runtime bounds for the durable scheduler loop.
#[derive(Debug, Clone, Copy)]
pub struct FeedbackSchedulerConfig {
    poll_interval: Duration,
    lease_secs: u64,
}

impl FeedbackSchedulerConfig {
    pub fn try_new(poll_secs: u64, lease_secs: u64) -> QuantResult<Self> {
        if poll_secs == 0 || !(3..=3_600).contains(&lease_secs) {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "feedback scheduler poll/lease bounds are invalid".to_owned(),
            }
            .into());
        }
        Ok(Self {
            poll_interval: Duration::from_secs(poll_secs),
            lease_secs,
        })
    }
}

/// Single-resident scheduled retraining materializer with durable DB recovery.
pub struct FeedbackScheduler {
    repository: Arc<dyn FeedbackSchedulerRepository>,
    mutation: Arc<CoreFeedbackMutationPort>,
    worker_id: WorkerId,
    config: FeedbackSchedulerConfig,
}

impl FeedbackScheduler {
    #[must_use]
    pub fn new(
        repository: Arc<dyn FeedbackSchedulerRepository>,
        mutation: Arc<CoreFeedbackMutationPort>,
        worker_id: WorkerId,
        config: FeedbackSchedulerConfig,
    ) -> Self {
        Self {
            repository,
            mutation,
            worker_id,
            config,
        }
    }

    pub async fn run(&self, shutdown: CancellationToken) {
        let mut poll = interval(self.config.poll_interval);
        poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                _ = poll.tick() => {
                    if let Err(error) = self.tick().await {
                        warn!(%error, "feedback scheduler tick failed");
                    }
                }
            }
        }
    }

    async fn tick(&self) -> QuantResult<()> {
        self.mutation.sync_scheduler_profiles().await?;
        for _ in 0..MAX_CLAIMS_PER_TICK {
            let claim = self
                .repository
                .claim_due(self.worker_id, self.config.lease_secs)
                .await?;
            let Some(claim) = claim else {
                break;
            };
            self.process_claim(claim).await;
        }
        Ok(())
    }

    async fn process_claim(&self, claim: FeedbackSchedulerClaim) {
        let mut lease = claim.lease.clone();
        let heartbeat_secs = cmp::max(1, self.config.lease_secs / 3);
        let mut heartbeat = interval_at(
            Instant::now() + Duration::from_secs(heartbeat_secs),
            Duration::from_secs(heartbeat_secs),
        );
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut materialization = Box::pin(self.mutation.materialize_scheduled(&claim));
        let result = loop {
            tokio::select! {
                result = &mut materialization => break Some(result),
                _ = heartbeat.tick() => {
                    match self
                        .repository
                        .renew_lease(lease.clone(), self.config.lease_secs)
                        .await
                    {
                        Ok(renewed) => lease = renewed,
                        Err(error) => {
                            warn!(
                                profile_id = %claim.state.research_profile_id,
                                %error,
                                "feedback scheduler lost its materialization lease"
                            );
                            break None;
                        }
                    }
                }
            }
        };
        let Some(result) = result else {
            return;
        };
        match result {
            Ok(success) => match self.repository.settle_success(lease, success).await {
                Ok(state) => info!(
                    profile_id = %state.research_profile_id,
                    cycle_id = ?state.last_cycle_id,
                    cutoff = ?state.last_cutoff,
                    cooldown_until = ?state.cooldown_until,
                    "scheduled feedback cycle materialized"
                ),
                Err(error) => warn!(
                    profile_id = %claim.state.research_profile_id,
                    %error,
                    "feedback scheduler could not commit its success cursor"
                ),
            },
            Err(error) => {
                let summary = bounded_error(&error.to_string());
                let retry_delay_secs = retry_delay(claim.state.attempt);
                if let Err(settlement_error) = self
                    .repository
                    .settle_retry(lease, retry_delay_secs, summary)
                    .await
                {
                    warn!(
                        profile_id = %claim.state.research_profile_id,
                        %error,
                        %settlement_error,
                        "feedback scheduler could not persist retry state"
                    );
                } else {
                    warn!(
                        profile_id = %claim.state.research_profile_id,
                        attempt = claim.state.attempt,
                        retry_delay_secs,
                        %error,
                        "scheduled feedback materialization deferred"
                    );
                }
            }
        }
    }
}

fn retry_delay(attempt: i32) -> u64 {
    let exponent = u32::try_from(attempt.saturating_sub(1))
        .unwrap_or_default()
        .min(8);
    BASE_RETRY_SECS
        .saturating_mul(1_u64 << exponent)
        .min(MAX_RETRY_SECS)
}

fn bounded_error(error: &str) -> String {
    error.chars().take(MAX_ERROR_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::retry_delay;

    #[test]
    fn retry_backoff_is_bounded() {
        assert_eq!(retry_delay(1), 5);
        assert_eq!(retry_delay(2), 10);
        assert_eq!(retry_delay(20), 300);
    }
}
