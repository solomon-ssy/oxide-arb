//! Durable `PostgreSQL` outbox publisher for entry-condition evaluation traces.

use std::{sync::Arc, time::Duration};

use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::clickhouse::EntryConditionEvaluationEventRow;
use quant_pivot_repository::traits::{EntryConditionRepository, FactWriter};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::infra::periodic_task::PeriodicTask;

const BATCH_SIZE: u64 = 500;
const LEASE_DURATION: Duration = Duration::from_secs(30);

/// Delivers committed condition traces to `ClickHouse` with crash-safe replay.
pub struct EntryConditionEvaluationOutboxWorker {
    worker_id: Uuid,
    conditions: Arc<dyn EntryConditionRepository>,
    writer: Arc<dyn FactWriter<EntryConditionEvaluationEventRow>>,
}

impl EntryConditionEvaluationOutboxWorker {
    #[must_use]
    pub fn new(
        conditions: Arc<dyn EntryConditionRepository>,
        writer: Arc<dyn FactWriter<EntryConditionEvaluationEventRow>>,
    ) -> Self {
        Self {
            worker_id: Uuid::now_v7(),
            conditions,
            writer,
        }
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) -> QuantResult<()> {
        let worker = Arc::clone(&self);
        PeriodicTask::run(
            "entry-condition-evaluation-outbox-worker",
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
        let now = chrono::Utc::now();
        let lease_duration = chrono::Duration::from_std(LEASE_DURATION).map_err(|error| {
            QuantError::config(format!("invalid evaluation outbox lease duration: {error}"))
        })?;
        let evaluations = self
            .conditions
            .claim_pending_evaluations(self.worker_id, now, now + lease_duration, BATCH_SIZE)
            .await?;
        if evaluations.is_empty() {
            return Ok(());
        }
        if let Err(error) = self.writer.write_batch(evaluations.clone()).await {
            for evaluation in &evaluations {
                if let Err(mark_error) = self
                    .conditions
                    .mark_evaluation_failed(
                        &evaluation.evaluation_id,
                        self.worker_id,
                        error.to_string(),
                    )
                    .await
                {
                    tracing::error!(
                        evaluation_id = %evaluation.evaluation_id,
                        %mark_error,
                        "failed to record condition-evaluation outbox delivery failure"
                    );
                }
            }
            return Err(error.into());
        }
        let published_at = chrono::Utc::now();
        for evaluation in evaluations {
            self.conditions
                .mark_evaluation_published(&evaluation.evaluation_id, self.worker_id, published_at)
                .await?;
        }
        Ok(())
    }
}
