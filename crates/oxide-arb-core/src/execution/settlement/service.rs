use crate::{
    bridge::risk_metrics::CoreRiskMetrics,
    execution::{fsm::ExecutionFSM, settlement::payout::compute_settlement_economics},
    observability::{
        alert_dispatcher::{Alert, AlertDispatcher},
        execution_audit::ExecutionAuditWriter,
        metrics_hub::MetricsHub,
    },
    pipeline::market_registry::MarketRegistry,
    service::risk_metrics::RiskMetricsRefreshService,
};
use chrono::Utc;
use oxide_arb_api::ctf::{
    client::CtfRedeemClient,
    types::{RedeemOutcome, RedeemRequest},
};
use oxide_arb_api::{VotingOracle, oracle::types::ResolutionVerdict};
use oxide_arb_error::{OxideError, redeem::RedeemError};
use oxide_arb_models::{
    config::settlement::SettlementConfig,
    domain::{
        CoreEvent, CoreEventPublisher,
        position::{MarkRedeemedParams, PositionInfo},
        settlement::{
            MarketSettlementInput, MarketSettlementRequest, NewResolutionEvent, SettlementEconomics,
        },
    },
    enums::common::{AlertLevel, ExecutionMode, RedeemStatus, SettlementTrigger},
    types::{ResolutionEventId, TokenId},
};
use oxide_arb_repository::{
    postgres::{PgPositionRepository, PgResolutionEventRepository, PgTradeRepository},
    traits::{PositionRepository, ResolutionEventRepository, TradeRepository},
};
use oxide_arb_risk::engine::RiskEngine;
use std::{sync::Arc, time::Instant};

pub struct MarketSettlementService {
    position_repo: Arc<PgPositionRepository>,
    resolution_event_repo: Arc<PgResolutionEventRepository>,
    trade_repo: Arc<PgTradeRepository>,
    risk_engine: Arc<RiskEngine>,
    risk_metrics: Arc<CoreRiskMetrics>,
    fsm: Arc<ExecutionFSM>,
    ctf_redeem: Option<Arc<CtfRedeemClient>>,
    market_registry: Arc<MarketRegistry>,
    voting_oracle: Arc<VotingOracle>,
    metrics: Arc<MetricsHub>,
    alerts: Arc<AlertDispatcher>,
    audit_writer: Arc<ExecutionAuditWriter>,
    metrics_refresh: Option<Arc<RiskMetricsRefreshService>>,
    events: CoreEventPublisher,
    config: Arc<SettlementConfig>,
}

pub struct MarketSettlementServiceDeps {
    pub position_repo: Arc<PgPositionRepository>,
    pub resolution_event_repo: Arc<PgResolutionEventRepository>,
    pub trade_repo: Arc<PgTradeRepository>,
    pub risk_engine: Arc<RiskEngine>,
    pub risk_metrics: Arc<CoreRiskMetrics>,
    pub fsm: Arc<ExecutionFSM>,
    pub ctf_redeem: Option<Arc<CtfRedeemClient>>,
    pub market_registry: Arc<MarketRegistry>,
    pub voting_oracle: Arc<VotingOracle>,
    pub metrics: Arc<MetricsHub>,
    pub alerts: Arc<AlertDispatcher>,
    pub audit_writer: Arc<ExecutionAuditWriter>,
    pub metrics_refresh: Option<Arc<RiskMetricsRefreshService>>,
    pub events: CoreEventPublisher,
    pub config: Arc<SettlementConfig>,
}

impl MarketSettlementService {
    pub fn new(deps: MarketSettlementServiceDeps) -> Self {
        Self {
            position_repo: deps.position_repo,
            resolution_event_repo: deps.resolution_event_repo,
            trade_repo: deps.trade_repo,
            risk_engine: deps.risk_engine,
            risk_metrics: deps.risk_metrics,
            fsm: deps.fsm,
            ctf_redeem: deps.ctf_redeem,
            market_registry: deps.market_registry,
            voting_oracle: deps.voting_oracle,
            metrics: deps.metrics,
            alerts: deps.alerts,
            audit_writer: deps.audit_writer,
            metrics_refresh: deps.metrics_refresh,
            events: deps.events,
            config: deps.config,
        }
    }

    /// Effective redeem mode for a position, derived from its persisted
    /// `redeem_status` rather than the live global execution mode.
    ///
    /// Money-critical: a position opened in Live carries `redeem_status =
    /// Pending` and must be redeemed on-chain even if the bot has since been
    /// switched to a simulated mode; conversely a simulated position
    /// (`NotRequired`) must never trigger a real redeem after a switch into
    /// Live. The trade-origin mode is the source of truth, not the global one.
    const fn position_redeem_mode(pos: &PositionInfo) -> ExecutionMode {
        match pos.redeem_status {
            RedeemStatus::Pending => ExecutionMode::Live,
            _ => ExecutionMode::DryRun,
        }
    }

    pub async fn settle_market(&self, req: &MarketSettlementRequest) -> Result<(), OxideError> {
        let started = Instant::now();
        let positions = self
            .position_repo
            .find_open_by_market(&req.market_id)
            .await?;
        if positions.is_empty() {
            self.metrics.settlement_no_open_positions_total.inc();
            tracing::debug!(market_id = %req.market_id, "settlement no-op: no open positions");
            return Ok(());
        }

        for pos in positions {
            self.settle_position(req, pos).await?;
        }

        // Surface the resolution to the real-time bus. `outcome` is `true` when
        // the winning token is the market's YES leg (best-effort: skipped when
        // the market is not in the live registry).
        if let Some(market) = self.market_registry.get_market(&req.market_id) {
            self.events.publish(CoreEvent::MarketResolved {
                market_id: req.market_id.clone(),
                outcome: req.winning_token_id == market.token_yes,
            });
        }

        if let Some(refresh) = &self.metrics_refresh {
            if let Err(error) = refresh.refresh().await {
                tracing::warn!(%error, "settlement metrics refresh failed");
            }
        }

        if self.config.oracle.enabled {
            self.audit_oracle(req).await;
        }

        self.metrics
            .settlement_duration_ms
            .observe(started.elapsed().as_secs_f64() * 1000.0);
        Ok(())
    }

    async fn settle_position(
        &self,
        req: &MarketSettlementRequest,
        pos: PositionInfo,
    ) -> Result<(), OxideError> {
        if pos.redeem_attempts
            >= i32::try_from(self.config.lifecycle.max_redeem_attempts).unwrap_or(i32::MAX)
        {
            let reason = format!(
                "redeem max attempts reached: {} >= {}",
                pos.redeem_attempts, self.config.lifecycle.max_redeem_attempts
            );
            tracing::error!(
                position_id = %pos.position_id,
                attempts = pos.redeem_attempts,
                reason = %reason,
                "redeem terminal failure"
            );
            self.alerts.dispatch_background(Alert {
                severity: AlertLevel::Critical,
                title: "Redeem terminal failure".to_owned(),
                body: format!(
                    "Position {} in market {} reached max redeem attempts: {reason}",
                    pos.position_id, pos.market_id
                ),
                timestamp: Utc::now(),
            });
            self.position_repo
                .mark_redeem_terminal(
                    &pos.position_id,
                    u32::try_from(pos.redeem_attempts).unwrap_or(u32::MAX),
                    &req.winning_token_id,
                    req.source,
                    reason,
                )
                .await?;
            self.metrics.settlement_redeem_failure_total.inc();
            return Err(OxideError::Internal("redeem max attempts reached".into()));
        }

        let economics = compute_settlement_economics(
            pos.shares,
            pos.total_cost_usd,
            pos.total_fees_usd,
            &pos.token_id,
            &req.winning_token_id,
        );
        let Some(redeem_outcome) = self.redeem_or_record_failure(req, &pos).await? else {
            return Ok(());
        };
        let settled = self
            .persist_position_settlement(req, &pos, &economics, redeem_outcome)
            .await?;
        self.apply_risk_settlement(req, &settled, economics).await?;
        self.metrics.settlement_positions_settled_total.inc();
        Ok(())
    }

    async fn redeem_or_record_failure(
        &self,
        req: &MarketSettlementRequest,
        pos: &PositionInfo,
    ) -> Result<Option<RedeemOutcome>, OxideError> {
        match self.redeem_position(req, pos).await {
            Ok(outcome) => {
                if Self::position_redeem_mode(pos) == ExecutionMode::Live {
                    self.metrics.settlement_redeem_success_total.inc();
                }
                Ok(Some(outcome))
            }
            Err(error) if error.is_retryable() => {
                let attempts =
                    u32::try_from(pos.redeem_attempts.saturating_add(1)).unwrap_or(u32::MAX);
                self.position_repo
                    .record_redeem_failure(
                        &pos.position_id,
                        attempts,
                        &req.winning_token_id,
                        req.source,
                    )
                    .await?;
                self.metrics.settlement_redeem_failure_total.inc();
                tracing::error!(%error, position_id = %pos.position_id, "redeem failed");
                Ok(None)
            }
            Err(error) if error.is_terminal_success_equivalent() => {
                Ok(Some(RedeemOutcome::dry_run()))
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn persist_position_settlement(
        &self,
        req: &MarketSettlementRequest,
        pos: &PositionInfo,
        economics: &SettlementEconomics,
        redeem_outcome: RedeemOutcome,
    ) -> Result<PositionInfo, OxideError> {
        self.position_repo
            .mark_redeemed(
                &pos.position_id,
                MarkRedeemedParams {
                    winning_token_id: req.winning_token_id.clone(),
                    settlement_payout_usd: economics.payout_usd,
                    realized_pnl: economics.realized_pnl_usd,
                    redeem_tx_hash: redeem_outcome.tx_hash,
                    redeem_status: RedeemStatus::settled_for_mode(Self::position_redeem_mode(pos)),
                    settlement_trigger: req.source,
                    redeem_terminal_reason: None,
                },
            )
            .await
            .map_err(Into::into)
    }

    async fn apply_risk_settlement(
        &self,
        req: &MarketSettlementRequest,
        settled: &PositionInfo,
        economics: SettlementEconomics,
    ) -> Result<(), OxideError> {
        let input = MarketSettlementInput {
            trade_id: settled.trade_id.clone(),
            market_id: settled.market_id.clone(),
            token_id: settled.token_id.clone(),
            side: settled.side,
            shares: settled.shares,
            entry_price: settled.avg_entry_price,
            cost_usd: settled.total_cost_usd,
            fee_usd: settled.total_fees_usd,
            realized_pnl_usd: economics.realized_pnl_usd,
            winning_token_id: req.winning_token_id.clone(),
            settlement_trigger: req.source,
        };
        let report = match self
            .risk_engine
            .on_market_settled(&input, self.risk_metrics.as_ref())
            .await
        {
            Ok(report) => report,
            Err(error) => {
                self.position_repo
                    .mark_accounting_failed(&settled.position_id, error.to_string())
                    .await?;
                return Err(error);
            }
        };
        self.position_repo
            .mark_accounted(&settled.position_id, Utc::now())
            .await?;
        self.write_settlement_audit(req, settled, &economics).await;
        if report
            .as_ref()
            .is_some_and(|report| report.breaker_tripped.is_some())
        {
            self.fsm
                .enter_emergency("circuit breaker tripped after market settlement");
        }
        Ok(())
    }

    async fn write_settlement_audit(
        &self,
        req: &MarketSettlementRequest,
        settled: &PositionInfo,
        economics: &SettlementEconomics,
    ) {
        match self.trade_repo.find_by_id(&settled.trade_id).await {
            Ok(Some(trade)) => {
                self.audit_writer
                    .write_settlement(&trade, settled, req, economics);
            }
            Ok(None) => {
                tracing::error!(
                    trade_id = %settled.trade_id,
                    position_id = %settled.position_id,
                    "settlement audit skipped: trade not found"
                );
            }
            Err(error) => {
                tracing::error!(
                    %error,
                    trade_id = %settled.trade_id,
                    position_id = %settled.position_id,
                    "settlement audit lookup failed"
                );
            }
        }
    }

    pub async fn retry_pending(&self) -> Result<(), OxideError> {
        let resolved_open_positions = self
            .position_repo
            .find_open_for_resolved_markets(1024)
            .await?;
        for position in resolved_open_positions {
            let Some(winning_token_id) = self.winning_token_for_retry(&position).await else {
                tracing::warn!(
                    market_id = %position.market_id,
                    position_id = %position.position_id,
                    "settlement retry deferred: winning token unavailable"
                );
                continue;
            };
            let request = MarketSettlementRequest {
                market_id: position.market_id.clone(),
                winning_token_id,
                winning_outcome: String::new(),
                source: SettlementTrigger::PeriodicRetry,
                observed_at: Utc::now(),
            };
            self.settle_market(&request).await?;
        }

        let redeem_positions = self
            .position_repo
            .find_redeem_retry_candidates(self.config.lifecycle.max_redeem_attempts)
            .await?;
        for position in redeem_positions {
            let Some(winning_token_id) = position.winning_token_id.clone() else {
                continue;
            };
            let request = MarketSettlementRequest {
                market_id: position.market_id.clone(),
                winning_token_id,
                winning_outcome: String::new(),
                source: SettlementTrigger::PeriodicRetry,
                observed_at: Utc::now(),
            };
            self.settle_market(&request).await?;
        }
        let accounting_positions = self
            .position_repo
            .find_accounting_retry_candidates(self.config.lifecycle.max_redeem_attempts)
            .await?;
        for position in accounting_positions {
            let Some(winning_token_id) = position.winning_token_id.clone() else {
                continue;
            };
            let economics = compute_settlement_economics(
                position.shares,
                position.total_cost_usd,
                position.total_fees_usd,
                &position.token_id,
                &winning_token_id,
            );
            let request = MarketSettlementRequest {
                market_id: position.market_id.clone(),
                winning_token_id,
                winning_outcome: String::new(),
                source: SettlementTrigger::PeriodicRetry,
                observed_at: Utc::now(),
            };
            self.apply_risk_settlement(&request, &position, economics)
                .await?;
        }
        Ok(())
    }

    async fn winning_token_for_retry(&self, position: &PositionInfo) -> Option<TokenId> {
        if let Some(token_id) = &position.winning_token_id {
            return Some(token_id.clone());
        }

        let latest = match self
            .resolution_event_repo
            .latest_by_source(&position.market_id, "ws")
            .await
            .ok()
            .flatten()
        {
            Some(event) => Some(event),
            None => self
                .resolution_event_repo
                .latest_for_market(&position.market_id)
                .await
                .ok()
                .flatten(),
        };
        let market = self.market_registry.get_market(&position.market_id)?;
        let outcome = latest
            .as_ref()
            .and_then(|event| {
                let outcome = event.outcome.trim();
                (!outcome.is_empty()).then_some(outcome)
            })
            .or(market.outcome.as_deref())
            .unwrap_or_default();
        if outcome.eq_ignore_ascii_case("yes") {
            Some(market.token_yes.clone())
        } else if outcome.eq_ignore_ascii_case("no") {
            Some(market.token_no.clone())
        } else {
            None
        }
    }

    async fn audit_oracle(&self, req: &MarketSettlementRequest) {
        let Some(market) = self.market_registry.get_market(&req.market_id) else {
            tracing::warn!(market_id = %req.market_id, "oracle audit skipped: unknown market");
            return;
        };

        let verdict = self
            .voting_oracle
            .resolve(&req.market_id, req.market_id.as_str())
            .await;
        match verdict {
            Ok(ResolutionVerdict::Resolved { actual_yes, .. }) => {
                let oracle_token = if actual_yes {
                    market.token_yes.clone()
                } else {
                    market.token_no.clone()
                };
                self.persist_resolution_event(
                    req,
                    "oracle",
                    Some(oracle_token == req.winning_token_id),
                    None,
                    serde_json::json!({ "actual_yes": actual_yes }),
                )
                .await;
                if oracle_token != req.winning_token_id {
                    self.metrics.settlement_oracle_mismatch_total.inc();
                    tracing::error!(
                        market_id = %req.market_id,
                        ws_winning_token_id = %req.winning_token_id,
                        oracle_winning_token_id = %oracle_token,
                        "oracle mismatch after WS-primary settlement"
                    );
                }
            }
            Ok(ResolutionVerdict::Disputed { .. }) => {
                self.persist_resolution_event(
                    req,
                    "oracle_disputed",
                    None,
                    None,
                    serde_json::json!({ "verdict": "disputed" }),
                )
                .await;
                tracing::error!(market_id = %req.market_id, "oracle disputed settled market");
            }
            Ok(ResolutionVerdict::Unresolved { reason }) => {
                self.persist_resolution_event(
                    req,
                    "oracle_unresolved",
                    None,
                    None,
                    serde_json::json!({ "reason": reason }),
                )
                .await;
                tracing::warn!(market_id = %req.market_id, %reason, "oracle unresolved during audit");
            }
            Err(error) => {
                tracing::warn!(market_id = %req.market_id, %error, "oracle audit failed");
            }
        }
    }

    async fn persist_resolution_event(
        &self,
        req: &MarketSettlementRequest,
        source: &str,
        gamma_agrees: Option<bool>,
        ctf_agrees: Option<bool>,
        evidence: serde_json::Value,
    ) {
        let event = NewResolutionEvent {
            resolution_id: ResolutionEventId::from_v7(),
            market_id: req.market_id.clone(),
            outcome: req.winning_outcome.clone(),
            source: source.to_owned(),
            gamma_agrees,
            ctf_agrees,
            evidence: Some(evidence),
            resolved_at: req.observed_at,
        };
        if let Err(error) = self.resolution_event_repo.append(event).await {
            tracing::warn!(%error, market_id = %req.market_id, "resolution event persist failed");
        }
    }

    async fn redeem_position(
        &self,
        req: &MarketSettlementRequest,
        pos: &PositionInfo,
    ) -> Result<RedeemOutcome, RedeemError> {
        let mode = Self::position_redeem_mode(pos);
        let market = self.market_registry.get_market(&req.market_id);
        let neg_risk = self
            .market_registry
            .neg_risk(&req.market_id)
            .unwrap_or(false);
        let request = RedeemRequest {
            condition_id: req.market_id.clone(),
            market_id: req.market_id.clone(),
            yes_token_id: market.as_ref().map_or_else(
                || req.winning_token_id.clone(),
                |market| market.token_yes.clone(),
            ),
            no_token_id: market.as_ref().map_or_else(
                || req.winning_token_id.clone(),
                |market| market.token_no.clone(),
            ),
            neg_risk,
            execution_mode: mode,
        };

        match mode {
            ExecutionMode::DryRun => Ok(RedeemOutcome::dry_run()),
            ExecutionMode::Paper => Ok(RedeemOutcome::paper(&req.market_id)),
            ExecutionMode::Live => {
                let Some(client) = &self.ctf_redeem else {
                    return Err(RedeemError::RpcTimeout(
                        "CTF redeem client unavailable in Live mode".into(),
                    ));
                };
                client.redeem(&request).await
            }
        }
    }
}
