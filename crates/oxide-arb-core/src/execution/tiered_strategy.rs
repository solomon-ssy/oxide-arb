//! FOK-first order strategy for Endgame single-platform execution.
//!
//! Live mode submits real FOK orders via [`ClobClient`]; Paper/DryRun delegate to
//! the dispatcher's simulated fills.

use std::sync::Arc;
use std::time::Instant;

use oxide_arb_api::clob::ClobClient;
use oxide_arb_models::domain::execution::ExecutionPlan;
use oxide_arb_models::domain::order::OrderRequest;
use oxide_arb_models::enums::common::{ExecutionMode, OrderType};
use oxide_arb_models::enums::execution::ExecutionOutcome;

use crate::execution::clob_outcome::map_order_response;
use crate::execution::dispatcher::Dispatcher;
use crate::observability::metrics_hub::MetricsHub;

const TIER_FOK: &str = "fok";

pub struct OrderStrategy {
    execution_mode: ExecutionMode,
    clob_client: Option<Arc<ClobClient>>,
    metrics: Arc<MetricsHub>,
}

impl OrderStrategy {
    pub const fn new(
        execution_mode: ExecutionMode,
        clob_client: Option<Arc<ClobClient>>,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            execution_mode,
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
            ExecutionMode::Live => self.execute_live_fok(plan).await,
        }
    }

    async fn execute_live_fok(&self, plan: &ExecutionPlan) -> ExecutionOutcome {
        let Some(clob) = &self.clob_client else {
            tracing::error!("Live mode requested but ClobClient is unavailable");
            return ExecutionOutcome::Failed {
                error: "ClobClient unavailable in Live mode".into(),
                execution_mode: ExecutionMode::Live,
            };
        };

        let started = Instant::now();
        let req = OrderRequest {
            market_id: plan.market_id.clone(),
            token_id: plan.token_id.clone(),
            side: plan.side,
            shares: plan.shares,
            price: plan.limit_price,
            order_type: OrderType::Fok,
            neg_risk: plan.neg_risk,
        };

        match clob.place_order(&req).await {
            Ok(resp) => {
                let outcome = map_order_response(resp, plan, ExecutionMode::Live, started);
                self.record_tier_metrics(&outcome);
                outcome
            }
            Err(e) => {
                self.metrics
                    .tier_misses
                    .with_label_values(&[TIER_FOK])
                    .inc();
                ExecutionOutcome::Failed {
                    error: e.to_string(),
                    execution_mode: ExecutionMode::Live,
                }
            }
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
