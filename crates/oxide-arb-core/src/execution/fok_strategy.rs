//! FOK-only order strategy for Endgame single-platform execution.
//!
//! Live mode submits real FOK orders via [`ClobClient`]. Paper and `DryRun`
//! delegate to the dispatcher for deterministic simulated outcomes.

use crate::{
    execution::{clob_outcome::map_order_response, dispatcher::Dispatcher},
    observability::{latency::observe_tick_to_http, metrics_hub::MetricsHub},
};
use oxide_arb_api::{clob::ClobClient, fees::FeeCalculator};
use oxide_arb_models::{
    domain::{execution::ExecutionPlan, latency::LatencyTrace, order::OrderRequest},
    enums::{
        common::{ExecutionMode, OrderType},
        execution::ExecutionOutcome,
    },
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const FOK_LABEL: &str = "fok";

pub struct FokOrderStrategy {
    execution_mode: ExecutionMode,
    clob_client: Option<Arc<ClobClient>>,
    fee_calculator: Arc<FeeCalculator>,
    dispatcher_timeout_ms: u64,
    metrics: Arc<MetricsHub>,
}

impl FokOrderStrategy {
    pub const fn new(
        execution_mode: ExecutionMode,
        clob_client: Option<Arc<ClobClient>>,
        fee_calculator: Arc<FeeCalculator>,
        dispatcher_timeout_ms: u64,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            execution_mode,
            clob_client,
            fee_calculator,
            dispatcher_timeout_ms,
            metrics,
        }
    }

    pub async fn execute(
        &self,
        dispatcher: &Dispatcher,
        plan: &ExecutionPlan,
        trace: &mut LatencyTrace,
    ) -> ExecutionOutcome {
        match self.execution_mode {
            ExecutionMode::DryRun | ExecutionMode::Paper => {
                trace.mark_http_sent();
                observe_tick_to_http(trace, &self.metrics);
                let outcome = dispatcher.dispatch(plan);
                self.record_fok_metrics(&outcome);
                outcome
            }
            ExecutionMode::Live => self.execute_live_fok(plan, trace).await,
        }
    }

    async fn execute_live_fok(
        &self,
        plan: &ExecutionPlan,
        trace: &mut LatencyTrace,
    ) -> ExecutionOutcome {
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

        trace.mark_http_sent();
        observe_tick_to_http(trace, &self.metrics);

        let timeout = Duration::from_millis(self.dispatcher_timeout_ms);
        match tokio::time::timeout(timeout, clob.place_order(&req)).await {
            Ok(Ok(resp)) => {
                let outcome = map_order_response(
                    resp,
                    plan,
                    ExecutionMode::Live,
                    started,
                    &self.fee_calculator,
                    plan.category,
                    &plan.token_id,
                );
                self.record_fok_metrics(&outcome);
                outcome
            }
            Ok(Err(e)) => {
                self.metrics
                    .tier_misses
                    .with_label_values(&[FOK_LABEL])
                    .inc();
                ExecutionOutcome::Failed {
                    error: e.to_string(),
                    execution_mode: ExecutionMode::Live,
                }
            }
            Err(_) => {
                self.metrics
                    .tier_misses
                    .with_label_values(&[FOK_LABEL])
                    .inc();
                tracing::error!(
                    execution_id = %plan.execution_id,
                    timeout_ms = self.dispatcher_timeout_ms,
                    "CLOB FOK order timed out"
                );
                ExecutionOutcome::Failed {
                    error: format!(
                        "CLOB FOK order timeout after {}ms",
                        self.dispatcher_timeout_ms
                    ),
                    execution_mode: ExecutionMode::Live,
                }
            }
        }
    }

    fn record_fok_metrics(&self, outcome: &ExecutionOutcome) {
        match outcome {
            ExecutionOutcome::Filled { .. } => {
                self.metrics
                    .tier_fills
                    .with_label_values(&[FOK_LABEL])
                    .inc();
            }
            ExecutionOutcome::Miss { .. } | ExecutionOutcome::Failed { .. } => {
                self.metrics
                    .tier_misses
                    .with_label_values(&[FOK_LABEL])
                    .inc();
            }
        }
    }
}
