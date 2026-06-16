//! Execution pipeline orchestration — validate → risk → size → reserve → submit → observe.
//!
//! The hot path persists the venue outcome onto the durable `trade` row
//! (`submitted` → `*_observed`) and rings the post-trade relay. All derived
//! bookkeeping (position, risk accounting, audit) is applied asynchronously and
//! idempotently by [`crate::post_trade`], replayed from the row on crash.

use crate::{
    bridge::{execution_mode::ExecutionModeHandle, risk_metrics::CoreRiskMetrics},
    control::{
        factor_shadow::{ShadowDecisionWriter, ShadowEvaluator},
        factor_snapshot::FactorSnapshotStore,
    },
    execution::{
        capital_manager::CapitalManager, dispatcher::Dispatcher, fok_strategy::FokOrderStrategy,
        fsm::ExecutionFSM, market_inflight::MarketInFlightRegistry, plan_builder::PlanBuilder,
        validator::Validator,
    },
    observability::{execution_audit::ExecutionAuditWriter, metrics_hub::MetricsHub},
    runtime_config::RuntimeConfigStore,
    service::risk_metrics::RiskMetricsState,
};
use chrono::Utc;
use oxide_arb_algorithm::scorer::ScoredOpportunity;
use oxide_arb_models::{
    domain::{
        book::BookSnapshot,
        control_factor::{
            AppliedControlFactor, BucketRiskDimensions, ControlFactorSnapshot,
            ExecutionQualityDimensions, FactorDecisionContext, effective_slippage_limit_bps,
            execution_quality_dimensions,
        },
        execution::{
            ExecutionPlan, ExecutionResult, ReservationHandle, ResolvedOutcome, ValidationResult,
        },
        opportunity::Opportunity,
        risk::ProbabilityInput,
        scored_snapshot::ScoredOpportunitySnapshot,
        trade::NewTrade,
    },
    enums::{
        common::ExecutionMode,
        execution::{ExecutionOutcome, ExecutionOutcomeSummary},
    },
    types::{ExecutionId, TokenId, TradeId, Usd},
};
use oxide_arb_repository::{postgres::PgTradeRepository, traits::TradeRepository};
use oxide_arb_risk::{
    context::SettlementGateInput,
    engine::RiskEngine,
    types::{ReportMode, RiskDecision},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::{fmt::Display, sync::Arc, time::Instant};
use tokio::sync::Notify;

/// Dependencies injected into [`ExecutionPipeline`].
pub struct ExecutionPipelineDeps<R: TradeRepository + Send + Sync + 'static = PgTradeRepository> {
    pub validator: Arc<Validator>,
    pub plan_builder: PlanBuilder,
    pub dispatcher: Dispatcher,
    pub order_strategy: Arc<FokOrderStrategy>,
    pub capital_manager: Arc<CapitalManager>,
    pub risk_engine: Arc<RiskEngine>,
    pub risk_metrics: Arc<CoreRiskMetrics>,
    pub fsm: Arc<ExecutionFSM>,
    pub market_inflight: Arc<MarketInFlightRegistry>,
    pub metrics: Arc<MetricsHub>,
    pub mode: ExecutionModeHandle,
    pub trade_repo: Arc<R>,
    pub audit_writer: Arc<ExecutionAuditWriter>,
    /// Rung after each durable `*_observed` write to wake the post-trade relay.
    pub relay_notify: Arc<Notify>,
    pub metrics_state: Arc<RiskMetricsState>,
    pub runtime_config: Arc<RuntimeConfigStore>,
    /// Live control-factor snapshots (published read for the decision context;
    /// shadow read for the shadow evaluator).
    pub factors: Arc<FactorSnapshotStore>,
    /// Backpressure-safe shadow-decision writer; `None` disables shadow.
    pub shadow_writer: Option<ShadowDecisionWriter>,
}

pub struct ExecutionPipeline<R: TradeRepository + Send + Sync + 'static = PgTradeRepository> {
    validator: Arc<Validator>,
    plan_builder: PlanBuilder,
    dispatcher: Dispatcher,
    order_strategy: Arc<FokOrderStrategy>,
    capital_manager: Arc<CapitalManager>,
    risk_engine: Arc<RiskEngine>,
    risk_metrics: Arc<CoreRiskMetrics>,
    fsm: Arc<ExecutionFSM>,
    market_inflight: Arc<MarketInFlightRegistry>,
    metrics: Arc<MetricsHub>,
    mode: ExecutionModeHandle,
    trade_repo: Arc<R>,
    audit_writer: Arc<ExecutionAuditWriter>,
    relay_notify: Arc<Notify>,
    metrics_state: Arc<RiskMetricsState>,
    runtime_config: Arc<RuntimeConfigStore>,
    factors: Arc<FactorSnapshotStore>,
    shadow_writer: Option<ShadowDecisionWriter>,
}

struct PreparedDispatch {
    trade_id: TradeId,
    plan: ExecutionPlan,
    reservation: ReservationHandle,
    snapshot: ScoredOpportunitySnapshot,
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
            mode: deps.mode,
            trade_repo: deps.trade_repo,
            audit_writer: deps.audit_writer,
            relay_notify: deps.relay_notify,
            metrics_state: deps.metrics_state,
            runtime_config: deps.runtime_config,
            factors: deps.factors,
            shadow_writer: deps.shadow_writer,
        }
    }

    /// Process a single scored opportunity through the full pipeline.
    pub async fn execute(&self, scored: Arc<ScoredOpportunity>) -> ExecutionResult {
        let started_at = Utc::now();
        let intent_started = Instant::now();
        let timer = self.metrics.execution_latency.start_timer();
        let opp = scored.opportunity.as_ref();
        let execution_id = ExecutionId::from_v7();

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

        // Persist "order submitted, outcome unknown" before the venue round-trip so a
        // crash leaves a `submitted` row the orphan scan can reconcile (fail-closed).
        match self
            .trade_repo
            .mark_submitted(&prepared.trade_id, Utc::now())
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                tracing::error!(trade_id = %prepared.trade_id, "trade was not in intent state");
                let _ = self.capital_manager.release_sync(&prepared.reservation);
                self.fsm.enter_emergency("mark submitted skipped");
                return Self::reject("submit_persist", "trade was not in intent state");
            }
            Err(error) => {
                tracing::error!(%error, trade_id = %prepared.trade_id, "mark submitted failed");
                let _ = self.capital_manager.release_sync(&prepared.reservation);
                self.fsm.enter_emergency("mark submitted failed");
                return Self::reject("submit_persist", error);
            }
        }

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
        self.observe_outcome(&prepared, &outcome).await;
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
        let snapshot = ScoredOpportunitySnapshot::from_opportunity(opp)
            .with_score_components(
                scored.fill_probability,
                scored.score,
                scored.urgency_factor,
                scored.category_weight,
                scored.staleness_discount,
            )
            .with_book_context(
                scored.token_yes.clone(),
                scored.token_no.clone(),
                scored.book_yes_version,
                scored.book_no_version,
            );
        let (approved_size, snapshot) =
            self.validate_and_size(scored, opp, &execution_id, snapshot)?;
        self.persist_dispatch_plan(opp, approved_size, snapshot, execution_id)
            .await
    }

    fn validate_and_size(
        &self,
        scored: &ScoredOpportunity,
        opp: &Opportunity,
        execution_id: &ExecutionId,
        snapshot: ScoredOpportunitySnapshot,
    ) -> Result<(Usd, ScoredOpportunitySnapshot), ExecutionResult> {
        let published = self.factors.published();
        let publication_id = published.publication_id.clone();

        // Stamp detection-time applied factors so any early rejection audit
        // still preserves the factor trace.
        let mut snapshot =
            snapshot.with_applied_control_factors(publication_id.clone(), &scored.applied_factors);

        let validation = match self.validator.validate(
            opp,
            &scored.token_yes,
            &scored.token_no,
            scored.book_yes_version,
            scored.book_no_version,
        ) {
            Ok(validation) => validation,
            Err(e) => {
                self.metrics.validation_failures.inc();
                self.audit_writer.write_rejection(
                    execution_id,
                    opp,
                    "validation",
                    &e.to_string(),
                    &snapshot,
                );
                return Err(Self::reject("validation", e));
            }
        };
        tracing::info!(
            opportunity_id = %opp.opportunity_id,
            execution_id = %execution_id,
            phase = "validated",
        );

        // Execution-quality factor: reject before risk sizing when the live book
        // is stricter than the factor's depth/slippage tolerance.
        if let Some(reason) = self.execution_quality_violation(&published, scored, opp, &validation)
        {
            self.metrics.control_factor_validation_rejections.inc();
            self.audit_writer.write_rejection(
                execution_id,
                opp,
                "factor_validation",
                &reason,
                &snapshot,
            );
            return Err(Self::reject("factor_validation", reason));
        }

        // Resolve the execution-time factor decision bundle from the current
        // published snapshot (safety factors act on the freshest information).
        let fail_closed = self.mode.current() == ExecutionMode::Live;
        let factor_context = Self::build_factor_context(&published, opp, Utc::now(), fail_closed);

        // Merge detection- and execution-time factor traces for the audit.
        let mut applied = scored.applied_factors.to_vec();
        applied.extend(factor_context.applied_factors.iter().cloned());
        snapshot = snapshot.with_applied_control_factors(publication_id, &applied);

        let probability = build_probability_input(scored);
        let settlement_gate = SettlementGateInput {
            mode: self.mode.current(),
            market_neg_risk: self.plan_builder.market_registry().neg_risk(&opp.market_id),
            redeem_policy: Some(&self.runtime_config.load().settlement.redeem),
        };
        let risk_decision = self.risk_engine.pre_trade_check_core(
            opp,
            &probability,
            self.risk_metrics.as_ref(),
            Some(&factor_context),
            settlement_gate,
            ReportMode::ShortCircuit,
        );

        if !risk_decision.allowed {
            return Err(self.record_risk_denial(execution_id, opp, &risk_decision, &snapshot));
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
                &snapshot,
            );
            return Err(Self::reject("sizing", "Kelly sizing returned zero"));
        }
        tracing::info!(
            opportunity_id = %opp.opportunity_id,
            execution_id = %execution_id,
            phase = "sized",
            approved_size_usd = %approved_size,
        );

        // Shadow consumption: record what the Shadow publication would do versus
        // this baseline. Never affects the real order path.
        self.record_shadow(&published, scored, opp, approved_size);

        Ok((approved_size, snapshot))
    }

    /// Load the snapshot of the token actually being bought.
    fn traded_book(
        &self,
        scored: &ScoredOpportunity,
        opp: &Opportunity,
    ) -> Option<Arc<BookSnapshot>> {
        let token: &TokenId = if opp.meta.predicted_yes {
            &scored.token_yes
        } else {
            &scored.token_no
        };
        self.validator.book_store().load(token)
    }

    /// Execution-quality dimensions for the traded token from the live book.
    fn execution_quality_dims(
        &self,
        scored: &ScoredOpportunity,
        opp: &Opportunity,
    ) -> Option<ExecutionQualityDimensions> {
        let book = self.traded_book(scored, opp)?;
        let now_ms = u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0);
        Some(execution_quality_dimensions(
            opp.category,
            opp.meta.price_zone,
            opp.staleness,
            &book,
            now_ms,
        ))
    }

    /// Returns a rejection reason when a published `ExecutionQualityFactor` is
    /// stricter than the live book's depth/slippage; `None` otherwise.
    fn execution_quality_violation(
        &self,
        published: &ControlFactorSnapshot,
        scored: &ScoredOpportunity,
        opp: &Opportunity,
        validation: &ValidationResult,
    ) -> Option<String> {
        published.publication_id.as_ref()?;
        let dims = self.execution_quality_dims(scored, opp)?;
        let found = published.execution_quality.lookup(&dims)?;
        let payload = &found.payload;

        if let Some(max_depth_usage) = payload.max_depth_usage_pct {
            let used_fraction = opp.depth_used_pct / Decimal::from(100);
            if used_fraction > max_depth_usage {
                return Some(format!(
                    "execution-quality depth usage {used_fraction} exceeds factor cap {max_depth_usage}"
                ));
            }
        }

        let effective_limit = effective_slippage_limit_bps(
            self.validator.max_slippage_bps(),
            payload.slippage_bps_addon,
        );
        if effective_limit < Decimal::ZERO {
            return Some(
                "execution-quality slippage addon leaves no admissible slippage".to_owned(),
            );
        }
        if validation.slippage_bps.inner() > effective_limit {
            return Some(format!(
                "slippage {} exceeds execution-quality tightened limit {effective_limit} bps",
                validation.slippage_bps.inner()
            ));
        }
        None
    }

    /// Resolve the execution-time factor decision bundle from the published snapshot.
    fn build_factor_context(
        published: &ControlFactorSnapshot,
        opp: &Opportunity,
        now: chrono::DateTime<Utc>,
        fail_closed: bool,
    ) -> FactorDecisionContext {
        let Some(publication_id) = published.publication_id.clone() else {
            return FactorDecisionContext::neutral();
        };

        let snapshot_expired = published.is_expired_at(now);
        let reconciliation_health = published.reconciliation_health.decision(&publication_id);
        let market_anomaly =
            published
                .market_anomalies
                .decision(&publication_id, &opp.market_id, &opp.event_id);
        let portfolio_risk = published
            .portfolio_risk
            .decision(&publication_id, opp.category);

        let bucket_dims = BucketRiskDimensions::coarse(
            opp.category,
            opp.meta.price_zone,
            opp.meta.duration_bucket,
        );
        let bucket = published.bucket_risk.lookup(&bucket_dims);
        let bucket_size_multiplier =
            bucket.map_or(Decimal::ONE, |found| found.payload.size_multiplier);

        let mut applied: Vec<AppliedControlFactor> = Vec::new();
        if let Some(source) = reconciliation_health.source.clone() {
            applied.push(source);
        }
        if let Some(source) = market_anomaly.source.clone() {
            applied.push(source);
        }
        if let Some(source) = portfolio_risk.source.clone() {
            applied.push(source);
        }

        FactorDecisionContext {
            publication_id: Some(publication_id),
            snapshot_expired,
            fail_closed,
            reconciliation_health,
            market_anomaly,
            portfolio_risk,
            bucket_size_multiplier,
            applied_factors: applied,
        }
    }

    /// Enqueue a shadow decision comparing the Shadow publication to the
    /// published baseline. Best-effort; failures never affect the live order.
    fn record_shadow(
        &self,
        published: &ControlFactorSnapshot,
        scored: &ScoredOpportunity,
        opp: &Opportunity,
        baseline_size: Usd,
    ) {
        let Some(writer) = &self.shadow_writer else {
            return;
        };
        let shadow = self.factors.shadow();
        if shadow.publication_id.is_none() {
            return;
        }
        let Some(eq_dims) = self.execution_quality_dims(scored, opp) else {
            return;
        };
        let bucket_dims = BucketRiskDimensions::coarse(
            opp.category,
            opp.meta.price_zone,
            opp.meta.duration_bucket,
        );
        if let Some(decision) = ShadowEvaluator::evaluate(
            published,
            &shadow,
            opp,
            &bucket_dims,
            &eq_dims,
            baseline_size,
        ) {
            writer.record(decision);
        }
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

        let trade_id = TradeId::from_v7();
        let plan = self
            .plan_builder
            .build(opp, approved_size, &reservation, execution_id);
        let pending_trade =
            match build_pending_trade(&trade_id, &plan, opp, &snapshot, self.mode.current()) {
                Ok(trade) => trade,
                Err(e) => {
                    let _ = self.capital_manager.release_sync(&reservation);
                    self.fsm
                        .enter_emergency("scored snapshot serialization failed");
                    return Err(Self::reject("trade_persist", e));
                }
            };
        if let Err(e) = self.trade_repo.create(pending_trade).await {
            tracing::error!(error = %e, trade_id = %trade_id, "trade intent insert failed");
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

    /// Durably record the venue outcome on the trade row, then wake the relay.
    async fn observe_outcome(&self, prepared: &PreparedDispatch, outcome: &ExecutionOutcome) {
        let resolved = ResolvedOutcome::resolve(
            outcome,
            prepared.plan.limit_price,
            prepared.snapshot.resolution_prob,
        );
        if let Err(error) = self
            .trade_repo
            .mark_observed(&prepared.trade_id, resolved.to_observation())
            .await
        {
            tracing::error!(%error, trade_id = %prepared.trade_id, "mark observed failed");
            self.fsm.enter_emergency("mark observed failed");
            return;
        }
        self.metrics_state.mark_stale();
        // Near-instant happy-path processing; the relay's periodic poll is the
        // crash-recovery safety net if this wake is missed.
        self.relay_notify.notify_one();
    }

    pub fn execution_mode(&self) -> ExecutionMode {
        self.mode.current()
    }

    fn record_risk_denial(
        &self,
        execution_id: &ExecutionId,
        opp: &Opportunity,
        risk_decision: &RiskDecision,
        snapshot: &ScoredOpportunitySnapshot,
    ) -> ExecutionResult {
        self.metrics.risk_denials.inc();
        let reason = risk_decision
            .denial_reason
            .clone()
            .unwrap_or_else(|| "risk denied".into());
        if reason.starts_with("MarketAnomalyBlock")
            || reason.starts_with("ReconciliationMaintenance")
            || reason.starts_with("ControlFactorSnapshotExpired")
        {
            self.metrics.control_factor_hard_rejects.inc();
        }
        self.audit_writer
            .write_rejection(execution_id, opp, "risk", &reason, snapshot);
        Self::reject("risk", reason)
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
    snapshot: &ScoredOpportunitySnapshot,
    execution_mode: ExecutionMode,
) -> Result<NewTrade, serde_json::Error> {
    Ok(NewTrade {
        trade_id: trade_id.clone(),
        execution_id: plan.execution_id.clone(),
        reservation_id: plan.reservation_id.clone(),
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
        scored_snapshot: serde_json::to_value(snapshot)?,
        category: plan.category,
        execution_mode,
    })
}
