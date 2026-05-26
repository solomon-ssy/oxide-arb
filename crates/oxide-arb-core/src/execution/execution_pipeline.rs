//! Execution pipeline orchestration — validate → risk → size → reserve → dispatch → audit.

use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_error::OxideError;
use oxide_arb_models::domain::execution::{
    ExecutionOutcomeSummary, ExecutionPlan, ExecutionResult, ReservationHandle,
};
use oxide_arb_models::domain::opportunity::Opportunity;
use oxide_arb_models::domain::trade::PostTradeInput;
use oxide_arb_models::enums::common::{ExecutionMode, TradeOutcome};
use oxide_arb_models::enums::execution::ExecutionOutcome;
use oxide_arb_models::enums::risk::TradeAccountingPhase;
use oxide_arb_models::types::{MarketId, Price, TokenId, TradeId, Usd};
use oxide_arb_risk::engine::RiskEngine;
use oxide_arb_risk::types::ReportMode;
use tokio_util::sync::CancellationToken;

use crate::bridge::risk_metrics::CoreRiskMetrics;
use crate::execution::capital_manager::CapitalManager;
use crate::execution::clob_outcome::{filled_cost, filled_net_profit};
use crate::execution::dispatcher::Dispatcher;
use crate::execution::fsm::ExecutionFSM;
use crate::execution::market_inflight::MarketInFlightRegistry;
use crate::execution::plan_builder::PlanBuilder;
use crate::execution::probability_input::build_probability_input;
use crate::execution::tiered_strategy::OrderStrategy;
use crate::execution::validator::Validator;
use crate::observability::backpressure::BackpressurePolicy;
use crate::observability::metrics_hub::MetricsHub;
use crate::outbox::in_memory::SharedInMemoryEventStore;

/// Slim async post-trade work item — ids and outcome only, no full opportunity clone.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PostTradeJob {
    pub trade_id: TradeId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub entry_price: Price,
    pub net_profit: Usd,
    pub outcome: ExecutionOutcome,
}

/// Dependencies injected into [`ExecutionPipeline`].
pub struct ExecutionPipelineDeps {
    pub validator: Validator,
    pub plan_builder: PlanBuilder,
    pub dispatcher: Dispatcher,
    pub order_strategy: OrderStrategy,
    pub capital_manager: Arc<CapitalManager>,
    pub risk_engine: Arc<RiskEngine>,
    pub risk_metrics: Arc<CoreRiskMetrics>,
    pub fsm: Arc<ExecutionFSM>,
    pub market_inflight: Arc<MarketInFlightRegistry>,
    pub metrics: Arc<MetricsHub>,
    pub execution_mode: ExecutionMode,
    pub outcome_tx: flume::Sender<PostTradeJob>,
    pub backpressure: Arc<BackpressurePolicy>,
}

pub struct ExecutionPipeline {
    validator: Validator,
    plan_builder: PlanBuilder,
    dispatcher: Dispatcher,
    order_strategy: OrderStrategy,
    capital_manager: Arc<CapitalManager>,
    risk_engine: Arc<RiskEngine>,
    risk_metrics: Arc<CoreRiskMetrics>,
    fsm: Arc<ExecutionFSM>,
    market_inflight: Arc<MarketInFlightRegistry>,
    metrics: Arc<MetricsHub>,
    execution_mode: ExecutionMode,
    outcome_tx: flume::Sender<PostTradeJob>,
    backpressure: Arc<BackpressurePolicy>,
}

impl ExecutionPipeline {
    pub fn new(deps: ExecutionPipelineDeps) -> Self {
        Self {
            validator: deps.validator,
            plan_builder: deps.plan_builder,
            dispatcher: deps.dispatcher,
            order_strategy: deps.order_strategy,
            capital_manager: deps.capital_manager,
            risk_engine: deps.risk_engine,
            risk_metrics: deps.risk_metrics,
            fsm: deps.fsm,
            market_inflight: deps.market_inflight,
            metrics: deps.metrics,
            execution_mode: deps.execution_mode,
            outcome_tx: deps.outcome_tx,
            backpressure: deps.backpressure,
        }
    }

    pub const fn backpressure(&self) -> &Arc<BackpressurePolicy> {
        &self.backpressure
    }

    pub fn post_trade_spill(&self) -> &SharedInMemoryEventStore {
        self.backpressure.post_trade_spill()
    }

    pub fn outcome_channel() -> (flume::Sender<PostTradeJob>, flume::Receiver<PostTradeJob>) {
        flume::bounded(1024)
    }

    pub async fn spawn_outcome_drain(
        rx: flume::Receiver<PostTradeJob>,
        risk_engine: Arc<RiskEngine>,
        risk_metrics: Arc<CoreRiskMetrics>,
        fsm: Arc<ExecutionFSM>,
        post_trade_spill: SharedInMemoryEventStore,
        shutdown: CancellationToken,
    ) -> Result<(), OxideError> {
        loop {
            while let Some(job) = post_trade_spill.try_pop_post_trade() {
                process_post_trade_job(&risk_engine, risk_metrics.as_ref(), &fsm, job).await;
            }

            tokio::select! {
                () = shutdown.cancelled() => {
                    while let Ok(job) = rx.try_recv() {
                        process_post_trade_job(&risk_engine, risk_metrics.as_ref(), &fsm, job).await;
                    }
                    for job in post_trade_spill.drain_post_trade_jobs() {
                        process_post_trade_job(&risk_engine, risk_metrics.as_ref(), &fsm, job).await;
                    }
                    return Ok(());
                }
                job = rx.recv_async() => {
                    match job {
                        Ok(job) => process_post_trade_job(&risk_engine, risk_metrics.as_ref(), &fsm, job).await,
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }

    /// Process a single scored opportunity through the full pipeline.
    pub async fn execute(&self, scored: Arc<ScoredOpportunity>) -> ExecutionResult {
        let started_at = Utc::now();
        let intent_started = Instant::now();
        let timer = self.metrics.execution_latency.start_timer();
        let opp = scored.opportunity.as_ref();

        if self.fsm.is_emergency() || !self.risk_engine.allows_trading() {
            return Self::reject("halted", "execution halted — trading blocked");
        }

        let Some(_inflight) = self.market_inflight.try_acquire(&opp.market_id) else {
            self.metrics.execution_market_busy.inc();
            return Self::reject("inflight", "market already executing");
        };

        if let Err(e) = self.validator.validate(
            opp,
            &scored.token_yes,
            &scored.token_no,
            scored.book_yes_version,
            scored.book_no_version,
        ) {
            self.metrics.validation_failures.inc();
            return Self::reject("validation", e);
        }

        let probability = build_probability_input(&scored);
        let risk_decision = self.risk_engine.pre_trade_check_core(
            opp,
            &probability,
            self.risk_metrics.as_ref(),
            ReportMode::ShortCircuit,
        );

        if !risk_decision.allowed {
            self.metrics.risk_denials.inc();
            return Self::reject(
                "risk",
                risk_decision
                    .denial_reason
                    .unwrap_or_else(|| "risk denied".into()),
            );
        }

        let approved_size = risk_decision
            .recommended_size
            .map_or(Usd::ZERO, |s| s.bet_usd);
        if approved_size <= Usd::ZERO {
            self.metrics.sizing_zero.inc();
            return Self::reject("sizing", "Kelly sizing returned zero");
        }

        let reservation = match self
            .capital_manager
            .reserve_sync(&opp.market_id, approved_size)
        {
            Ok(handle) => handle,
            Err(e) => {
                self.metrics.reservation_failures.inc();
                return Self::reject("reservation", e);
            }
        };

        let plan = self.plan_builder.build(opp, approved_size, &reservation);
        let mut trace = scored.trace.clone();
        if trace.dispatch_started.is_none() {
            trace.mark_dispatch_started();
        }
        let outcome = self
            .order_strategy
            .execute(&self.dispatcher, &plan, &mut trace)
            .await;
        self.metrics
            .execute_intent_to_http_us
            .observe(intent_started.elapsed().as_secs_f64() * 1_000_000.0);

        self.settle_reservation(&outcome, &reservation);
        let (job_net_profit, job_entry_price) = Self::filled_job_fields(opp, &plan, &outcome);
        let outcome_summary = ExecutionOutcomeSummary::from_outcome(&outcome);
        self.enqueue_post_trade(opp, job_entry_price, job_net_profit, outcome);
        timer.observe_duration();
        ExecutionResult {
            outcome_summary: Some(outcome_summary),
            rejection_reason: None,
            rejection_stage: None,
            started_at,
            completed_at: Utc::now(),
        }
    }

    fn settle_reservation(&self, outcome: &ExecutionOutcome, reservation: &ReservationHandle) {
        match outcome {
            ExecutionOutcome::Filled { .. } => {
                if let Err(e) = self.capital_manager.confirm_sync(reservation) {
                    tracing::error!(error = %e, "reservation confirm failed");
                    self.fsm.enter_emergency("reservation confirm failed");
                }
            }
            ExecutionOutcome::Miss { .. } | ExecutionOutcome::Failed { .. } => {
                if let Err(e) = self.capital_manager.release_sync(reservation) {
                    tracing::error!(error = %e, "reservation release failed");
                    self.fsm.enter_emergency("reservation release failed");
                }
            }
        }
    }

    fn filled_job_fields(
        opp: &Opportunity,
        plan: &ExecutionPlan,
        outcome: &ExecutionOutcome,
    ) -> (Usd, Price) {
        match outcome {
            ExecutionOutcome::Filled {
                filled_shares,
                avg_fill_price,
                ..
            } => {
                let price = avg_fill_price.unwrap_or(opp.entry_price);
                (filled_net_profit(opp, *filled_shares, plan.shares), price)
            }
            _ => (opp.net_profit, opp.entry_price),
        }
    }

    fn enqueue_post_trade(
        &self,
        opp: &Opportunity,
        entry_price: Price,
        net_profit: Usd,
        outcome: ExecutionOutcome,
    ) {
        let job = PostTradeJob {
            trade_id: TradeId::new(opp.opportunity_id.as_str()),
            market_id: opp.market_id.clone(),
            token_id: opp.token_id.clone(),
            entry_price,
            net_profit,
            outcome,
        };
        if let Err(send_err) = self.outcome_tx.try_send(job) {
            self.backpressure
                .on_post_trade_channel_full(send_err.into_inner());
        }
    }

    pub const fn execution_mode(&self) -> ExecutionMode {
        self.execution_mode
    }

    #[cold]
    fn reject(stage: &'static str, reason: impl std::fmt::Display) -> ExecutionResult {
        ExecutionResult::rejected(stage, reason)
    }
}

async fn process_post_trade_job(
    risk_engine: &RiskEngine,
    metrics: &CoreRiskMetrics,
    fsm: &ExecutionFSM,
    job: PostTradeJob,
) {
    let (trade_outcome, cost, fee, net_profit) = match &job.outcome {
        ExecutionOutcome::Filled {
            filled_shares,
            avg_fill_price,
            fee_paid,
            ..
        } => {
            let price = avg_fill_price.unwrap_or(job.entry_price);
            let cost = filled_cost(*filled_shares, price);
            let scaled_profit = job.net_profit;
            (TradeOutcome::Success, cost, *fee_paid, Some(scaled_profit))
        }
        ExecutionOutcome::Miss { .. } => (TradeOutcome::Miss, Usd::ZERO, Usd::ZERO, None),
        ExecutionOutcome::Failed { .. } => (TradeOutcome::TradeFailed, Usd::ZERO, Usd::ZERO, None),
    };

    let fill_input = PostTradeInput {
        trade_id: job.trade_id.clone(),
        market_id: job.market_id.clone(),
        token_id: job.token_id.clone(),
        outcome: trade_outcome,
        cost_usd: cost,
        fee_usd: fee,
        net_profit_usd: net_profit,
    };

    if let Err(e) = risk_engine
        .on_trade_result(TradeAccountingPhase::Fill, &fill_input, metrics)
        .await
    {
        tracing::error!(error = %e, "post-trade fill accounting failed");
        fsm.enter_emergency("post-trade fill persist failed");
        return;
    }

    if trade_outcome == TradeOutcome::Success {
        let settlement = PostTradeInput {
            net_profit_usd: net_profit,
            ..fill_input
        };
        match risk_engine
            .on_trade_result(TradeAccountingPhase::Settlement, &settlement, metrics)
            .await
        {
            Ok(report) => {
                if report.breaker_tripped.is_some() {
                    fsm.enter_emergency("circuit breaker tripped after settlement");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "post-trade settlement accounting failed");
                fsm.enter_emergency("post-trade settlement persist failed");
            }
        }
    }
}
