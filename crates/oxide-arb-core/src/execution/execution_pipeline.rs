//! Execution pipeline orchestration — validate → risk → size → reserve → dispatch → audit.

use crate::{
    bridge::risk_metrics::CoreRiskMetrics,
    execution::{
        capital_manager::CapitalManager, dispatcher::Dispatcher, fsm::ExecutionFSM,
        market_inflight::MarketInFlightRegistry, plan_builder::PlanBuilder,
        tiered_strategy::OrderStrategy, validator::Validator,
    },
    observability::{
        alert_dispatcher::AlertDispatcher, backpressure::BackpressurePolicy,
        execution_audit::ExecutionAuditWriter, metrics_hub::MetricsHub,
    },
    outbox::in_memory::SharedInMemoryEventStore,
    service::risk_metrics::{RiskMetricsRefreshService, RiskMetricsState},
};
use chrono::Utc;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_error::OxideError;
use oxide_arb_models::{
    domain::{
        execution::{
            ExecutionOutcomeSummary, ExecutionPlan, ExecutionResult, PostTradeJob,
            ReservationHandle, ResolvedOutcome,
        },
        opportunity::Opportunity,
        position::NewPosition,
        risk::ProbabilityInput,
        scored_snapshot::ScoredOpportunitySnapshot,
        trade::NewTrade,
    },
    enums::{
        common::{ExecutionMode, RedeemStatus, TradeOutcome},
        execution::ExecutionOutcome,
        risk::TradeAccountingPhase,
    },
    types::{ExecutionId, TradeId, Usd},
};
use oxide_arb_repository::{
    postgres::PgTradeRepository,
    traits::{PositionRepository, TradeRepository},
};
use oxide_arb_risk::{engine::RiskEngine, types::ReportMode};
use rust_decimal_macros::dec;
use std::{fmt::Display, sync::Arc, time::Instant};
use tokio_util::sync::CancellationToken;

/// Dependencies injected into [`ExecutionPipeline`].
pub struct ExecutionPipelineDeps<R: TradeRepository + Send + Sync + 'static = PgTradeRepository> {
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
    pub trade_repo: Arc<R>,
    pub audit_writer: Arc<ExecutionAuditWriter>,
    pub outcome_tx: flume::Sender<PostTradeJob>,
    pub backpressure: Arc<BackpressurePolicy>,
}

/// Dependencies for the post-trade outcome drain task.
pub struct PostTradeDrainDeps<R: TradeRepository + Send + Sync + 'static> {
    pub risk_engine: Arc<RiskEngine>,
    pub risk_metrics: Arc<CoreRiskMetrics>,
    pub fsm: Arc<ExecutionFSM>,
    pub trade_repo: Arc<R>,
    pub position_repo: Arc<dyn PositionRepository>,
    pub audit_writer: Arc<ExecutionAuditWriter>,
    pub alerts: Arc<AlertDispatcher>,
    pub post_trade_spill: SharedInMemoryEventStore,
    pub metrics_state: Arc<RiskMetricsState>,
    pub metrics_refresh: Option<Arc<RiskMetricsRefreshService>>,
    pub execution_mode: ExecutionMode,
}

pub struct ExecutionPipeline<R: TradeRepository + Send + Sync + 'static = PgTradeRepository> {
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
    trade_repo: Arc<R>,
    audit_writer: Arc<ExecutionAuditWriter>,
    outcome_tx: flume::Sender<PostTradeJob>,
    backpressure: Arc<BackpressurePolicy>,
}

struct PreparedDispatch {
    trade_id: TradeId,
    plan: ExecutionPlan,
    reservation: ReservationHandle,
    snapshot: ScoredOpportunitySnapshot,
}

struct PostTradeJobCtx<'a, R> {
    risk_engine: &'a RiskEngine,
    metrics: &'a CoreRiskMetrics,
    fsm: &'a ExecutionFSM,
    trade_repo: &'a R,
    position_repo: &'a dyn PositionRepository,
    audit_writer: &'a ExecutionAuditWriter,
    metrics_state: &'a RiskMetricsState,
    metrics_refresh: Option<&'a RiskMetricsRefreshService>,
    execution_mode: ExecutionMode,
}

struct PostTradeEnqueueInput<'a> {
    opp: &'a Opportunity,
    plan: &'a ExecutionPlan,
    trade_id: TradeId,
    scored_snapshot: ScoredOpportunitySnapshot,
    outcome: ExecutionOutcome,
}

impl<R: TradeRepository + Send + Sync + 'static> ExecutionPipeline<R> {
    pub fn new(deps: ExecutionPipelineDeps<R>) -> Self {
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
            trade_repo: deps.trade_repo,
            audit_writer: deps.audit_writer,
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

    pub async fn spawn_outcome_drain(
        rx: flume::Receiver<PostTradeJob>,
        deps: PostTradeDrainDeps<R>,
        shutdown: CancellationToken,
    ) -> Result<(), OxideError> {
        let ctx = PostTradeJobCtx {
            risk_engine: deps.risk_engine.as_ref(),
            metrics: deps.risk_metrics.as_ref(),
            fsm: deps.fsm.as_ref(),
            trade_repo: deps.trade_repo.as_ref(),
            position_repo: deps.position_repo.as_ref(),
            audit_writer: deps.audit_writer.as_ref(),
            metrics_state: deps.metrics_state.as_ref(),
            metrics_refresh: deps.metrics_refresh.as_deref(),
            execution_mode: deps.execution_mode,
        };
        loop {
            while let Some(job) = deps.post_trade_spill.try_pop_post_trade() {
                process_post_trade_job(&ctx, job).await;
            }

            tokio::select! {
                () = shutdown.cancelled() => {
                    while let Ok(job) = rx.try_recv() {
                        process_post_trade_job(&ctx, job).await;
                    }
                    for job in deps.post_trade_spill.drain_post_trade_jobs() {
                        process_post_trade_job(&ctx, job).await;
                    }
                    return Ok(());
                }
                job = rx.recv_async() => {
                    match job {
                        Ok(job) => process_post_trade_job(&ctx, job).await,
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
        let execution_id = ExecutionId::generate();

        if self.fsm.is_emergency() || !self.risk_engine.allows_trading() {
            return Self::reject("halted", "execution halted — trading blocked");
        }

        let Some(_inflight) = self.market_inflight.try_acquire(&opp.market_id) else {
            self.metrics.execution_market_busy.inc();
            return Self::reject("inflight", "market already executing");
        };

        let prepared = match self.prepare_dispatch(&scored, opp, execution_id).await {
            Ok(prepared) => prepared,
            Err(result) => return result,
        };

        let mut trace = Arc::clone(&scored.trace);
        {
            let trace_mut = Arc::make_mut(&mut trace);
            if trace_mut.dispatch_started.is_none() {
                trace_mut.mark_dispatch_started();
            }
        }
        let outcome = self
            .order_strategy
            .execute(&self.dispatcher, &prepared.plan, Arc::make_mut(&mut trace))
            .await;
        self.metrics
            .execute_intent_to_http_us
            .observe(intent_started.elapsed().as_secs_f64() * 1_000_000.0);

        self.settle_reservation(&outcome, &prepared.reservation);
        let outcome_summary = ExecutionOutcomeSummary::from_outcome(&outcome);
        self.enqueue_post_trade(PostTradeEnqueueInput {
            opp,
            plan: &prepared.plan,
            trade_id: prepared.trade_id,
            scored_snapshot: prepared.snapshot,
            outcome,
        });
        timer.observe_duration();
        ExecutionResult {
            outcome_summary: Some(outcome_summary),
            rejection_reason: None,
            rejection_stage: None,
            started_at,
            completed_at: Utc::now(),
        }
    }

    async fn prepare_dispatch(
        &self,
        scored: &ScoredOpportunity,
        opp: &Opportunity,
        execution_id: ExecutionId,
    ) -> Result<PreparedDispatch, ExecutionResult> {
        let (approved_size, snapshot) = self.validate_and_size(
            scored,
            opp,
            &execution_id,
            &ScoredOpportunitySnapshot::from_opportunity(opp),
        )?;
        self.persist_dispatch_plan(opp, approved_size, snapshot, execution_id)
            .await
    }

    fn validate_and_size(
        &self,
        scored: &ScoredOpportunity,
        opp: &Opportunity,
        execution_id: &ExecutionId,
        snapshot: &ScoredOpportunitySnapshot,
    ) -> Result<(Usd, ScoredOpportunitySnapshot), ExecutionResult> {
        if let Err(e) = self.validator.validate(
            opp,
            &scored.token_yes,
            &scored.token_no,
            scored.book_yes_version,
            scored.book_no_version,
        ) {
            self.metrics.validation_failures.inc();
            self.audit_writer.write_rejection(
                execution_id,
                opp,
                "validation",
                &e.to_string(),
                snapshot,
            );
            return Err(Self::reject("validation", e));
        }
        tracing::info!(
            opportunity_id = %opp.opportunity_id,
            execution_id = %execution_id,
            phase = "validated",
        );

        let probability = build_probability_input(scored);
        let risk_decision = self.risk_engine.pre_trade_check_core(
            opp,
            &probability,
            self.risk_metrics.as_ref(),
            ReportMode::ShortCircuit,
        );

        if !risk_decision.allowed {
            self.metrics.risk_denials.inc();
            let reason = risk_decision
                .denial_reason
                .unwrap_or_else(|| "risk denied".into());
            self.audit_writer
                .write_rejection(execution_id, opp, "risk", &reason, snapshot);
            return Err(Self::reject("risk", reason));
        }
        tracing::info!(
            opportunity_id = %opp.opportunity_id,
            execution_id = %execution_id,
            phase = "risk_checked",
        );

        let approved_size = risk_decision
            .recommended_size
            .map_or(Usd::ZERO, |s| s.bet_usd);
        if approved_size <= Usd::ZERO {
            self.metrics.sizing_zero.inc();
            self.audit_writer.write_rejection(
                execution_id,
                opp,
                "sizing",
                "Kelly sizing returned zero",
                snapshot,
            );
            return Err(Self::reject("sizing", "Kelly sizing returned zero"));
        }
        tracing::info!(
            opportunity_id = %opp.opportunity_id,
            execution_id = %execution_id,
            phase = "sized",
            approved_size_usd = %approved_size,
        );

        Ok((approved_size, snapshot.clone()))
    }

    async fn persist_dispatch_plan(
        &self,
        opp: &Opportunity,
        approved_size: Usd,
        snapshot: ScoredOpportunitySnapshot,
        execution_id: ExecutionId,
    ) -> Result<PreparedDispatch, ExecutionResult> {
        let reservation = match self
            .capital_manager
            .reserve_sync(&opp.market_id, approved_size)
        {
            Ok(handle) => handle,
            Err(e) => {
                self.metrics.reservation_failures.inc();
                self.audit_writer.write_rejection(
                    &execution_id,
                    opp,
                    "reservation",
                    &e.to_string(),
                    &snapshot,
                );
                return Err(Self::reject("reservation", e));
            }
        };

        let trade_id = TradeId::generate();
        let plan = self
            .plan_builder
            .build(opp, approved_size, &reservation, execution_id);
        let pending_trade = build_pending_trade(&trade_id, &plan, opp, self.execution_mode);
        if let Err(e) = self.trade_repo.create(pending_trade).await {
            tracing::error!(error = %e, trade_id = %trade_id, "trade pending insert failed");
            let _ = self.capital_manager.release_sync(&reservation);
            self.audit_writer.write_rejection(
                &plan.execution_id,
                opp,
                "trade_persist",
                &e.to_string(),
                &snapshot,
            );
            return Err(Self::reject("trade_persist", e));
        }

        tracing::info!(
            opportunity_id = %opp.opportunity_id,
            execution_id = %plan.execution_id,
            trade_id = %trade_id,
            phase = "dispatched",
        );

        Ok(PreparedDispatch {
            trade_id,
            plan,
            reservation,
            snapshot,
        })
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

    fn enqueue_post_trade(&self, input: PostTradeEnqueueInput<'_>) {
        let PostTradeEnqueueInput {
            opp,
            plan,
            trade_id,
            scored_snapshot,
            outcome,
        } = input;
        let job = PostTradeJob {
            trade_id,
            execution_id: plan.execution_id.clone(),
            opportunity_id: opp.opportunity_id.clone(),
            market_id: opp.market_id.clone(),
            event_id: opp.event_id.clone(),
            token_id: opp.token_id.clone(),
            side: opp.side,
            plan_shares: plan.shares,
            entry_price: opp.entry_price,
            execution_mode: self.execution_mode,
            edge_bps: Some(opp.edge_bps),
            detected_profit: Some(opp.expected_net_profit),
            detected_at: opp.detected_at,
            category: plan.category,
            scored_snapshot,
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
    fn reject(stage: &'static str, reason: impl Display) -> ExecutionResult {
        ExecutionResult::rejected(stage, reason)
    }
}

#[inline]
fn build_probability_input(scored: &ScoredOpportunity) -> ProbabilityInput {
    let opp = &scored.opportunity;
    let cal = &opp.calibration;

    ProbabilityInput {
        calibrated_win_prob: cal.fused_probability,
        fill_prob: scored.fill_probability.to_decimal(),
        calibration_confidence: opp.meta.confidence,
        sample_size: cal.sample_size,
        model_staleness_secs: 0,
        expected_slippage_pct: dec!(0.005),
        expected_failure_cost_pct: dec!(0.002),
    }
}

fn build_pending_trade(
    trade_id: &TradeId,
    plan: &ExecutionPlan,
    opp: &Opportunity,
    execution_mode: ExecutionMode,
) -> NewTrade {
    NewTrade {
        trade_id: trade_id.clone(),
        execution_id: plan.execution_id.clone(),
        opportunity_id: opp.opportunity_id.clone(),
        market_id: opp.market_id.clone(),
        event_id: opp.event_id.clone(),
        token_id: opp.token_id.clone(),
        side: opp.side,
        shares: plan.shares,
        price: plan.limit_price,
        cost_usd: plan.estimated_cost,
        fee_usd: plan.estimated_fee,
        detected_edge_bps: Some(opp.edge_bps),
        detected_profit_usd: Some(opp.expected_net_profit),
        execution_mode,
    }
}

async fn process_post_trade_job<R: TradeRepository>(
    ctx: &PostTradeJobCtx<'_, R>,
    job: PostTradeJob,
) {
    let resolved = ResolvedOutcome::resolve(&job);

    if let Err(e) = ctx
        .trade_repo
        .update(&job.trade_id, resolved.to_trade_update())
        .await
    {
        tracing::error!(error = %e, trade_id = %job.trade_id, "trade outcome update failed");
        ctx.fsm.enter_emergency("trade outcome update failed");
        return;
    }

    ctx.audit_writer.write_terminal(&job, &resolved);

    if !apply_post_trade_risk(ctx, &job, &resolved).await {
        return;
    }

    if resolved.trade_outcome == TradeOutcome::Success {
        let position = NewPosition {
            trade_id: job.trade_id.clone(),
            market_id: job.market_id.clone(),
            token_id: job.token_id.clone(),
            side: job.side,
            shares: resolved.filled_shares,
            avg_entry_price: resolved.avg_fill_price,
            total_cost_usd: resolved.cost_usd,
            total_fees_usd: resolved.fee_usd,
            redeem_status: RedeemStatus::initial_for_mode(ctx.execution_mode),
        };

        if let Err(error) = ctx.position_repo.create(position).await {
            tracing::error!(
                %error,
                trade_id = %job.trade_id,
                market_id = %job.market_id,
                "position creation failed after fill"
            );
            ctx.fsm.enter_emergency("position create failed");
            return;
        }

        ctx.risk_engine.refresh_positions(ctx.metrics);
    }

    ctx.metrics_state.mark_stale();
    if let Some(refresher) = ctx.metrics_refresh {
        if let Err(error) = refresher.refresh().await {
            tracing::warn!(%error, "post-trade metrics refresh failed");
        }
    }
}

async fn apply_post_trade_risk<R: TradeRepository>(
    ctx: &PostTradeJobCtx<'_, R>,
    job: &PostTradeJob,
    resolved: &ResolvedOutcome,
) -> bool {
    let fill_input = resolved.to_risk_input(job);

    if let Err(e) = ctx
        .risk_engine
        .on_trade_result(TradeAccountingPhase::Fill, &fill_input, ctx.metrics)
        .await
    {
        tracing::error!(error = %e, "post-trade fill accounting failed");
        ctx.fsm.enter_emergency("post-trade fill persist failed");
        return false;
    }

    true
}
