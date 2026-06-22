//! Reconciliation worker orchestration (evidence ladder, defer-only policy).

mod close;
mod defer;
mod economics;
mod evidence;

pub use close::CloseUnresolvableService;
pub use evidence::{EvidenceVerdict, evaluate_evidence_ladder};

use crate::{
    execution::{
        capital_manager::CapitalManager,
        fsm::{EmergencyClass, ExecutionFSM},
    },
    runtime_config::RuntimeConfigStore,
    service::risk_metrics::RiskMetricsState,
};
use chrono::Utc;
use defer::next_defer_until;
use economics::{
    reconciled_fill_economics, reservation_amount_after_fill, resolution_prob_from_trade,
};
use oxide_arb_api::{
    clob::{ClobClient, ClobTrade},
    ctf::client::CtfRedeemClient,
    fees::FeeCalculator,
};
use oxide_arb_models::{
    domain::{
        execution::ReservationHandle,
        trade::{TradeInfo, TradeObservation},
    },
    enums::common::{TradeReconcileResolution, TradeState},
    runtime_config::ReconciliationConfig,
    types::{Price, Shares, Usd},
};
use oxide_arb_repository::traits::TradeRepository;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const DEFAULT_BATCH_SIZE: u64 = 128;
const POLL_INTERVAL: Duration = Duration::from_secs(5);

struct ReconcileEvidenceInputs {
    clob_trades: Vec<ClobTrade>,
    ctf_balance: Shares,
    competing: bool,
}

struct FilledReconcileContext<'a> {
    shares: Shares,
    price: Price,
    clob_trade: Option<Box<ClobTrade>>,
    note: &'a str,
    config: &'a ReconciliationConfig,
    now: chrono::DateTime<Utc>,
}

pub struct ReconciliationWorker {
    trade_repo: Arc<dyn TradeRepository>,
    clob_client: Arc<ClobClient>,
    ctf_redeem: Arc<CtfRedeemClient>,
    fee_calculator: Arc<FeeCalculator>,
    holder_address: String,
    capital_manager: Arc<CapitalManager>,
    fsm: Arc<ExecutionFSM>,
    metrics_state: Arc<RiskMetricsState>,
    runtime_config: Arc<RuntimeConfigStore>,
    reconcile_notify: Arc<Notify>,
    relay_notify: Arc<Notify>,
    batch_size: u64,
}

pub struct ReconciliationWorkerDeps {
    pub trade_repo: Arc<dyn TradeRepository>,
    pub clob_client: Arc<ClobClient>,
    pub ctf_redeem: Arc<CtfRedeemClient>,
    pub fee_calculator: Arc<FeeCalculator>,
    pub holder_address: String,
    pub capital_manager: Arc<CapitalManager>,
    pub fsm: Arc<ExecutionFSM>,
    pub metrics_state: Arc<RiskMetricsState>,
    pub runtime_config: Arc<RuntimeConfigStore>,
    pub reconcile_notify: Arc<Notify>,
    pub relay_notify: Arc<Notify>,
}

impl ReconciliationWorker {
    #[must_use]
    pub fn new(deps: ReconciliationWorkerDeps) -> Self {
        Self {
            trade_repo: deps.trade_repo,
            clob_client: deps.clob_client,
            ctf_redeem: deps.ctf_redeem,
            fee_calculator: deps.fee_calculator,
            holder_address: deps.holder_address,
            capital_manager: deps.capital_manager,
            fsm: deps.fsm,
            metrics_state: deps.metrics_state,
            runtime_config: deps.runtime_config,
            reconcile_notify: deps.reconcile_notify,
            relay_notify: deps.relay_notify,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub async fn run(self, shutdown: CancellationToken) {
        loop {
            self.drain_once().await;
            tokio::select! {
                () = shutdown.cancelled() => {
                    self.drain_once().await;
                    return;
                }
                () = self.reconcile_notify.notified() => {}
                () = tokio::time::sleep(POLL_INTERVAL) => {}
            }
        }
    }

    async fn drain_once(&self) {
        let trades = match self
            .trade_repo
            .find_needs_reconcile(self.batch_size, 0)
            .await
        {
            Ok(trades) => trades,
            Err(error) => {
                tracing::warn!(%error, "reconciliation worker scan failed");
                return;
            }
        };
        for trade in trades {
            self.process_trade(&trade).await;
        }
    }

    async fn process_trade(&self, trade: &TradeInfo) {
        let config = self.runtime_config.load().execution.reconciliation.clone();
        let now = Utc::now();
        let Some(inputs) = self.load_evidence_inputs(trade, &config).await else {
            return;
        };
        let verdict = evaluate_evidence_ladder(
            trade,
            &inputs.clob_trades,
            inputs.ctf_balance,
            inputs.competing,
            &config,
            now,
        );
        match verdict {
            EvidenceVerdict::Filled {
                shares,
                price,
                clob_trade,
                note,
                ..
            } => {
                self.apply_filled_verdict(
                    trade,
                    FilledReconcileContext {
                        shares,
                        price,
                        clob_trade,
                        note: &note,
                        config: &config,
                        now,
                    },
                )
                .await;
            }
            EvidenceVerdict::Miss { note } => {
                self.apply_miss_verdict(trade, &note, now).await;
            }
            EvidenceVerdict::Defer { note } => {
                self.record_defer_verdict(trade, &note, &config, now).await;
            }
        }
    }

    async fn load_evidence_inputs(
        &self,
        trade: &TradeInfo,
        config: &ReconciliationConfig,
    ) -> Option<ReconcileEvidenceInputs> {
        let now = Utc::now();
        let clob_trades = match self.fetch_clob_trades(trade).await {
            Ok(trades) => trades,
            Err(error) => {
                tracing::warn!(%error, trade_id = %trade.trade_id, "reconciliation CLOB fetch failed");
                let _ = self
                    .trade_repo
                    .record_reconcile_defer(
                        &trade.trade_id,
                        next_defer_until(config, trade.reconcile_attempts + 1, now),
                        &error,
                    )
                    .await;
                return None;
            }
        };
        let ctf_balance = match self
            .ctf_redeem
            .position_balance(&self.holder_address, &trade.token_id)
            .await
        {
            Ok(balance) => balance,
            Err(error) => {
                tracing::warn!(%error, trade_id = %trade.trade_id, "reconciliation CTF balance failed");
                let _ = self
                    .trade_repo
                    .record_reconcile_defer(
                        &trade.trade_id,
                        next_defer_until(config, trade.reconcile_attempts + 1, now),
                        &error.to_string(),
                    )
                    .await;
                return None;
            }
        };
        let competing = self
            .trade_repo
            .count_competing_pending_reconcile(&trade.market_id, trade.submitted_at)
            .await
            .unwrap_or(0)
            > 1;
        Some(ReconcileEvidenceInputs {
            clob_trades,
            ctf_balance,
            competing,
        })
    }

    async fn apply_filled_verdict(&self, trade: &TradeInfo, ctx: FilledReconcileContext<'_>) {
        let FilledReconcileContext {
            shares,
            price,
            clob_trade,
            note,
            config,
            now,
        } = ctx;
        let resolution_prob = match resolution_prob_from_trade(trade) {
            Ok(prob) => prob,
            Err(error) => {
                tracing::warn!(%error, trade_id = %trade.trade_id, "resolution_prob missing — defer");
                let _ = self
                    .trade_repo
                    .record_reconcile_defer(
                        &trade.trade_id,
                        next_defer_until(config, trade.reconcile_attempts + 1, now),
                        &error,
                    )
                    .await;
                return;
            }
        };
        let observation = match reconciled_fill_economics(
            trade,
            clob_trade.as_deref(),
            shares,
            price,
            &self.fee_calculator,
            resolution_prob,
            trade.execution_mode,
        ) {
            Ok(observation) => observation,
            Err(error) => {
                tracing::warn!(%error, trade_id = %trade.trade_id, "fee quote failed — defer");
                let _ = self
                    .trade_repo
                    .record_reconcile_defer(
                        &trade.trade_id,
                        next_defer_until(config, trade.reconcile_attempts + 1, now),
                        &error.to_string(),
                    )
                    .await;
                return;
            }
        };
        self.apply_filled(trade, observation, note).await;
    }

    async fn apply_miss_verdict(&self, trade: &TradeInfo, note: &str, now: chrono::DateTime<Utc>) {
        let observation = TradeObservation {
            state: TradeState::MissObserved,
            shares: Shares::ZERO,
            price: trade.price,
            cost_usd: Usd::ZERO,
            fee_usd: Usd::ZERO,
            order_id: None,
            tx_hash: None,
            net_profit_usd: None,
            latency_ms: None,
            error_message: Some(note.to_owned()),
            confirmed_at: now,
        };
        self.apply_miss(trade, observation, note).await;
    }

    async fn record_defer_verdict(
        &self,
        trade: &TradeInfo,
        note: &str,
        config: &ReconciliationConfig,
        now: chrono::DateTime<Utc>,
    ) {
        let defer_until = next_defer_until(config, trade.reconcile_attempts + 1, now);
        if let Err(error) = self
            .trade_repo
            .record_reconcile_defer(&trade.trade_id, defer_until, note)
            .await
        {
            tracing::error!(%error, trade_id = %trade.trade_id, "reconcile defer persist failed");
            self.fsm.enter_emergency(
                EmergencyClass::PersistenceFault,
                "reconcile defer persist failed",
            );
        }
    }

    async fn fetch_clob_trades(
        &self,
        trade: &TradeInfo,
    ) -> Result<Vec<oxide_arb_api::clob::ClobTrade>, String> {
        let config = self.runtime_config.load().execution.reconciliation.clone();
        let after = trade
            .submitted_at
            .map(|ts| ts.timestamp().saturating_sub(config.trade_lookback_secs));
        let mut trades = self
            .clob_client
            .get_trades(Some(&trade.market_id), Some(&trade.token_id), after)
            .await
            .map_err(|error| error.to_string())?;
        trades.sort_by_key(|item| item.matched_at);
        Ok(trades)
    }

    async fn apply_filled(&self, trade: &TradeInfo, observation: TradeObservation, note: &str) {
        let reservation = ReservationHandle {
            id: trade.reservation_id.clone(),
            amount: trade.cost_usd + trade.fee_usd,
            market_id: trade.market_id.clone(),
        };
        let actual_reserved =
            reservation_amount_after_fill(observation.cost_usd, observation.fee_usd);
        if actual_reserved < reservation.amount {
            if let Err(error) = self
                .capital_manager
                .resize_sync(&reservation, actual_reserved)
            {
                tracing::error!(%error, trade_id = %trade.trade_id, "reconciled fill reservation resize failed");
                self.fsm.enter_emergency(
                    EmergencyClass::ReservationFault,
                    "reconciled fill reservation resize failed",
                );
                return;
            }
        }
        match self
            .trade_repo
            .mark_reconciled_observed(
                &trade.trade_id,
                observation,
                TradeReconcileResolution::Filled,
                note,
            )
            .await
        {
            Ok(true) => {
                self.metrics_state.mark_stale();
                self.relay_notify.notify_one();
            }
            Ok(false) => {
                tracing::debug!(trade_id = %trade.trade_id, "trade reconciliation already resolved");
            }
            Err(error) => {
                tracing::error!(%error, trade_id = %trade.trade_id, "reconciled fill persist failed");
                self.fsm.enter_emergency(
                    EmergencyClass::PersistenceFault,
                    "reconciled fill persist failed",
                );
            }
        }
    }

    async fn apply_miss(&self, trade: &TradeInfo, observation: TradeObservation, note: &str) {
        match self
            .trade_repo
            .mark_reconciled_observed(
                &trade.trade_id,
                observation,
                TradeReconcileResolution::Miss,
                note,
            )
            .await
        {
            Ok(true) => {
                let reservation = ReservationHandle {
                    id: trade.reservation_id.clone(),
                    amount: trade.cost_usd + trade.fee_usd,
                    market_id: trade.market_id.clone(),
                };
                if let Err(error) = self.capital_manager.release_sync(&reservation) {
                    tracing::error!(%error, trade_id = %trade.trade_id, "reconciled miss reservation release failed");
                    self.fsm.enter_emergency(
                        EmergencyClass::ReservationFault,
                        "reconciled miss reservation release failed",
                    );
                }
                self.metrics_state.mark_stale();
                self.relay_notify.notify_one();
            }
            Ok(false) => {
                tracing::debug!(trade_id = %trade.trade_id, "trade reconciliation already resolved");
            }
            Err(error) => {
                tracing::error!(%error, trade_id = %trade.trade_id, "reconciled miss persist failed");
                self.fsm.enter_emergency(
                    EmergencyClass::PersistenceFault,
                    "reconciled miss persist failed",
                );
            }
        }
    }
}
