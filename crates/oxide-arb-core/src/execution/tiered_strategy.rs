//! FOK-first order strategy for Endgame single-platform execution.
//!
//! ADR-001 removed multi-leg GTD hedging. Live mode uses FOK with configurable
//! retry; Paper/DryRun delegate to the dispatcher's simulated fills.

use std::sync::Arc;
use std::time::Duration;

use oxide_arb_api::clob::ClobClient;
use oxide_arb_models::config::TieredExecutionConfig;
use oxide_arb_models::domain::execution::ExecutionPlan;
use oxide_arb_models::enums::common::ExecutionMode;
use oxide_arb_models::enums::execution::ExecutionOutcome;

use crate::execution::dispatcher::Dispatcher;
use crate::observability::metrics_hub::MetricsHub;

const TIER_FOK: &str = "fok";

pub struct OrderStrategy {
    execution_mode: ExecutionMode,
    tier_config: TieredExecutionConfig,
    clob_client: Option<Arc<ClobClient>>,
    metrics: Arc<MetricsHub>,
}

impl OrderStrategy {
    pub const fn new(
        execution_mode: ExecutionMode,
        tier_config: TieredExecutionConfig,
        clob_client: Option<Arc<ClobClient>>,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            execution_mode,
            tier_config,
            clob_client,
            metrics,
        }
    }

    pub async fn execute(&self, dispatcher: &Dispatcher, plan: &ExecutionPlan) -> ExecutionOutcome {
        match self.execution_mode {
            ExecutionMode::DryRun | ExecutionMode::Paper => {
                let outcome = dispatcher.dispatch(plan);
                self.record_tier_metrics(&outcome);
                outcome
            }
            ExecutionMode::Live => self.execute_live_fok(dispatcher, plan).await,
        }
    }

    async fn execute_live_fok(
        &self,
        dispatcher: &Dispatcher,
        plan: &ExecutionPlan,
    ) -> ExecutionOutcome {
        if self.clob_client.is_none() {
            tracing::error!(
                "Live mode requested but ClobClient is unavailable — falling back to paper"
            );
            let outcome = dispatcher.dispatch(plan);
            self.record_tier_metrics(&outcome);
            return outcome;
        }

        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(self.tier_config.fok_timeout_ms);
        let mut attempts = 0u32;

        while tokio::time::Instant::now() < deadline
            && attempts <= self.tier_config.max_retries_per_tier
        {
            attempts += 1;
            let outcome = dispatcher.dispatch(plan);
            if matches!(outcome, ExecutionOutcome::Filled { .. }) {
                self.metrics.tier_fills.with_label_values(&[TIER_FOK]).inc();
                return outcome;
            }
            if matches!(outcome, ExecutionOutcome::Failed { .. }) {
                self.metrics
                    .tier_misses
                    .with_label_values(&[TIER_FOK])
                    .inc();
                return outcome;
            }
            // Non-blocking retry — sleep would break SLO-1 execute-intent budget.
            tokio::task::yield_now().await;
        }

        self.metrics
            .tier_misses
            .with_label_values(&[TIER_FOK])
            .inc();
        ExecutionOutcome::Miss {
            reason: format!(
                "FOK not filled after {attempts} attempts within {}ms",
                self.tier_config.fok_timeout_ms
            ),
            execution_mode: ExecutionMode::Live,
        }
    }

    fn record_tier_metrics(&self, outcome: &ExecutionOutcome) {
        match outcome {
            ExecutionOutcome::Filled { .. } => {
                self.metrics.tier_fills.with_label_values(&[TIER_FOK]).inc();
            }
            ExecutionOutcome::Miss { .. } | ExecutionOutcome::Failed { .. } => {
                self.metrics
                    .tier_misses
                    .with_label_values(&[TIER_FOK])
                    .inc();
            }
        }
    }
}
