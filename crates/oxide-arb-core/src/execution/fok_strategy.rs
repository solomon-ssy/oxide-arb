//! FOK-only order strategy for Endgame single-platform execution.
//!
//! Live mode submits real FOK orders via [`ClobClient`]. Paper and `DryRun`
//! delegate to the dispatcher for deterministic simulated outcomes.

use crate::{
    bridge::execution_mode::ExecutionModeHandle,
    execution::{clob_outcome::map_order_response, dispatcher::Dispatcher},
    observability::{latency::observe_tick_to_http, metrics_hub::MetricsHub},
};
use oxide_arb_api::{clob::ClobClient, fees::FeeCalculator};
use oxide_arb_models::{
    domain::{
        execution::ExecutionPlan,
        latency::LatencyTrace,
        order::{OrderAmount, OrderRequest},
    },
    enums::{
        common::{ExecutionMode, OrderType, Side},
        execution::ExecutionOutcome,
    },
    runtime_config::TradeTimeoutConfig,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

pub struct FokOrderStrategy {
    mode: ExecutionModeHandle,
    clob_client: Option<Arc<ClobClient>>,
    fee_calculator: Arc<FeeCalculator>,
    dispatcher_timeout_ms: AtomicU64,
    metrics: Arc<MetricsHub>,
}

impl FokOrderStrategy {
    pub const fn new(
        mode: ExecutionModeHandle,
        clob_client: Option<Arc<ClobClient>>,
        fee_calculator: Arc<FeeCalculator>,
        dispatcher_timeout_ms: u64,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            mode,
            clob_client,
            fee_calculator,
            dispatcher_timeout_ms: AtomicU64::new(dispatcher_timeout_ms),
            metrics,
        }
    }

    /// Hot-reload the FOK dispatch timeout (runtime-config activation).
    ///
    /// Consumes only `dispatcher_timeout_ms`; the other budgets in the
    /// timeout section belong to the validator and the post-trade relay.
    pub fn reload(&self, config: &TradeTimeoutConfig) {
        self.dispatcher_timeout_ms
            .store(config.dispatcher_timeout_ms, Ordering::Relaxed);
    }

    pub async fn execute(
        &self,
        dispatcher: &Dispatcher,
        plan: &ExecutionPlan,
        trace: &mut LatencyTrace,
    ) -> ExecutionOutcome {
        match self.mode.current() {
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
            amount: match plan.side {
                Side::Buy => OrderAmount::Usd(plan.estimated_cost),
                Side::Sell => OrderAmount::Shares(plan.shares),
            },
            price: plan.limit_price,
            order_type: OrderType::Fok,
            neg_risk: plan.neg_risk,
        };

        trace.mark_http_sent();
        observe_tick_to_http(trace, &self.metrics);

        let timeout_ms = self.dispatcher_timeout_ms.load(Ordering::Relaxed);
        let timeout = Duration::from_millis(timeout_ms);
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
                self.metrics.fok_misses.inc();
                tracing::error!(
                    execution_id = %plan.execution_id,
                    error = %e,
                    "CLOB FOK order failed"
                );
                ExecutionOutcome::Failed {
                    error: e.to_string(),
                    execution_mode: ExecutionMode::Live,
                }
            }
            Err(_) => {
                self.metrics.fok_misses.inc();
                tracing::error!(
                    execution_id = %plan.execution_id,
                    timeout_ms,
                    "CLOB FOK order timed out with unknown venue outcome"
                );
                ExecutionOutcome::Unknown {
                    reason: format!("CLOB FOK order timeout after {timeout_ms}ms"),
                    execution_mode: ExecutionMode::Live,
                }
            }
        }
    }

    fn record_fok_metrics(&self, outcome: &ExecutionOutcome) {
        match outcome {
            ExecutionOutcome::Filled { .. } => {
                self.metrics.fok_fills.inc();
            }
            ExecutionOutcome::Miss { .. }
            | ExecutionOutcome::Failed { .. }
            | ExecutionOutcome::Unknown { .. } => {
                self.metrics.fok_misses.inc();
            }
        }
    }
}
