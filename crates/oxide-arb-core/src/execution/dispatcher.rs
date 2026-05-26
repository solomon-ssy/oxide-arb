use std::sync::Arc;

use oxide_arb_models::domain::execution::ExecutionPlan;
use oxide_arb_models::enums::common::ExecutionMode;
use oxide_arb_models::enums::execution::ExecutionOutcome;
use oxide_arb_models::types::OrderId;

use crate::observability::metrics_hub::MetricsHub;

pub struct Dispatcher {
    execution_mode: ExecutionMode,
    metrics: Arc<MetricsHub>,
}

impl Dispatcher {
    pub const fn new(execution_mode: ExecutionMode, metrics: Arc<MetricsHub>) -> Self {
        Self {
            execution_mode,
            metrics,
        }
    }

    pub fn dispatch(&self, plan: &ExecutionPlan) -> ExecutionOutcome {
        let outcome = match self.execution_mode {
            ExecutionMode::DryRun => Self::dry_run(plan),
            ExecutionMode::Paper => Self::paper_trade(plan),
            ExecutionMode::Live => ExecutionOutcome::Failed {
                error: "Live dispatch must go through OrderStrategy + ClobClient".into(),
                execution_mode: ExecutionMode::Live,
            },
        };
        self.record_outcome_metrics(&outcome);
        outcome
    }

    fn record_outcome_metrics(&self, outcome: &ExecutionOutcome) {
        match outcome {
            ExecutionOutcome::Filled { .. } => self.metrics.trades_filled.inc(),
            ExecutionOutcome::Miss { .. } => self.metrics.trades_missed.inc(),
            ExecutionOutcome::Failed { .. } => self.metrics.trades_failed.inc(),
        }
    }

    fn dry_run(plan: &ExecutionPlan) -> ExecutionOutcome {
        tracing::info!(
            execution_id = %plan.execution_id,
            market_id = %plan.market_id,
            shares = %plan.shares,
            price = %plan.limit_price,
            "[DRY RUN] Would place order"
        );
        ExecutionOutcome::Filled {
            order_id: OrderId::new(format!("dry-{}", plan.execution_id)),
            filled_shares: plan.shares,
            avg_fill_price: Some(plan.limit_price),
            fee_paid: plan.estimated_fee,
            tx_hash: None,
            execution_mode: ExecutionMode::DryRun,
            latency_ms: 0,
        }
    }

    fn paper_trade(plan: &ExecutionPlan) -> ExecutionOutcome {
        ExecutionOutcome::Filled {
            order_id: OrderId::new(format!("paper-{}", plan.execution_id)),
            filled_shares: plan.shares,
            avg_fill_price: Some(plan.limit_price),
            fee_paid: plan.estimated_fee,
            tx_hash: None,
            execution_mode: ExecutionMode::Paper,
            latency_ms: 5,
        }
    }
}
