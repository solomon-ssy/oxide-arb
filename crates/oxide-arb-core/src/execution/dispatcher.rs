use crate::{observability::metrics_hub::MetricsHub, pipeline::book_store::BookStore};
use oxide_arb_api::fees::FeeCalculator;
use oxide_arb_models::{
    domain::execution::ExecutionPlan,
    enums::{
        common::{ExecutionMode, Side},
        execution::ExecutionOutcome,
    },
    types::{OrderId, Price, Shares, TokenId},
};
use std::sync::Arc;

pub struct Dispatcher {
    execution_mode: ExecutionMode,
    book_store: Option<Arc<BookStore>>,
    fee_calculator: Arc<FeeCalculator>,
    metrics: Arc<MetricsHub>,
}

impl Dispatcher {
    pub const fn new(
        execution_mode: ExecutionMode,
        book_store: Option<Arc<BookStore>>,
        fee_calculator: Arc<FeeCalculator>,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            execution_mode,
            book_store,
            fee_calculator,
            metrics,
        }
    }

    pub fn dispatch(&self, plan: &ExecutionPlan) -> ExecutionOutcome {
        let outcome = match self.execution_mode {
            ExecutionMode::DryRun => self.dry_run(plan),
            ExecutionMode::Paper => self.paper_trade(plan),
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

    fn dry_run(&self, plan: &ExecutionPlan) -> ExecutionOutcome {
        tracing::info!(
            execution_id = %plan.execution_id,
            market_id = %plan.market_id,
            shares = %plan.shares,
            price = %plan.limit_price,
            "[DRY RUN] Would place order"
        );
        let fee = self.fee_calculator.calculate(
            plan.shares,
            plan.limit_price,
            plan.category,
            &plan.token_id,
        );
        ExecutionOutcome::Filled {
            order_id: OrderId::new(format!("dry-{}", plan.execution_id)),
            filled_shares: plan.shares,
            avg_fill_price: Some(plan.limit_price),
            fee_paid: fee,
            tx_hash: None,
            execution_mode: ExecutionMode::DryRun,
            latency_ms: 0,
        }
    }

    fn paper_trade(&self, plan: &ExecutionPlan) -> ExecutionOutcome {
        if let Some(book_store) = &self.book_store {
            let available = Self::available_depth_at_price(
                book_store,
                &plan.token_id,
                plan.side,
                plan.limit_price,
            );
            if available < plan.shares {
                return ExecutionOutcome::Miss {
                    reason: format!(
                        "paper: insufficient depth ({available} < {} shares at {})",
                        plan.shares, plan.limit_price
                    ),
                    execution_mode: ExecutionMode::Paper,
                };
            }
        }

        let fee = self.fee_calculator.calculate(
            plan.shares,
            plan.limit_price,
            plan.category,
            &plan.token_id,
        );
        ExecutionOutcome::Filled {
            order_id: OrderId::new(format!("paper-{}", plan.execution_id)),
            filled_shares: plan.shares,
            avg_fill_price: Some(plan.limit_price),
            fee_paid: fee,
            tx_hash: None,
            execution_mode: ExecutionMode::Paper,
            latency_ms: 5,
        }
    }

    fn available_depth_at_price(
        book_store: &BookStore,
        token_id: &TokenId,
        side: Side,
        limit_price: Price,
    ) -> Shares {
        let Some(book) = book_store.load(token_id) else {
            return Shares::ZERO;
        };
        match side {
            Side::Buy => book.ask_depth_up_to(limit_price),
            Side::Sell => book.bid_depth_down_to(limit_price),
        }
    }
}
