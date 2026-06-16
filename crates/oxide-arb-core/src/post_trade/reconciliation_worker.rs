//! Reconciliation worker for unknown venue outcomes.
//!
//! A local HTTP timeout does not prove whether the CLOB accepted and filled the
//! FOK order. This worker scans durable `needs_reconcile` trades, queries
//! external venue/on-chain evidence, and converts the row back into the normal
//! post-trade relay path only when the outcome is proven.

use crate::{
    execution::{capital_manager::CapitalManager, fsm::ExecutionFSM},
    service::risk_metrics::RiskMetricsState,
};
use chrono::Utc;
use oxide_arb_api::{
    clob::{ClobClient, ClobTrade},
    ctf::client::CtfRedeemClient,
};
use oxide_arb_models::{
    domain::{
        execution::ReservationHandle,
        trade::{TradeInfo, TradeObservation},
    },
    enums::common::{TradeReconcileResolution, TradeState},
    types::{OrderId, Price, Shares, Usd},
};
use oxide_arb_repository::traits::TradeRepository;
use rust_decimal::Decimal;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const DEFAULT_BATCH_SIZE: u64 = 128;
const POLL_INTERVAL: Duration = Duration::from_secs(5);
const TRADE_LOOKBACK_SECS: i64 = 60;

pub struct ReconciliationWorker {
    trade_repo: Arc<dyn TradeRepository>,
    clob_client: Arc<ClobClient>,
    ctf_redeem: Arc<CtfRedeemClient>,
    holder_address: String,
    capital_manager: Arc<CapitalManager>,
    fsm: Arc<ExecutionFSM>,
    metrics_state: Arc<RiskMetricsState>,
    reconcile_notify: Arc<Notify>,
    relay_notify: Arc<Notify>,
    batch_size: u64,
}

pub struct ReconciliationWorkerDeps {
    pub trade_repo: Arc<dyn TradeRepository>,
    pub clob_client: Arc<ClobClient>,
    pub ctf_redeem: Arc<CtfRedeemClient>,
    pub holder_address: String,
    pub capital_manager: Arc<CapitalManager>,
    pub fsm: Arc<ExecutionFSM>,
    pub metrics_state: Arc<RiskMetricsState>,
    pub reconcile_notify: Arc<Notify>,
    pub relay_notify: Arc<Notify>,
}

enum ReconcileDecision {
    Filled {
        shares: Shares,
        price: Price,
        order_id: Option<OrderId>,
        tx_hash: Option<String>,
        note: String,
    },
    Miss {
        note: String,
    },
}

impl ReconciliationWorker {
    #[must_use]
    pub fn new(deps: ReconciliationWorkerDeps) -> Self {
        Self {
            trade_repo: deps.trade_repo,
            clob_client: deps.clob_client,
            ctf_redeem: deps.ctf_redeem,
            holder_address: deps.holder_address,
            capital_manager: deps.capital_manager,
            fsm: deps.fsm,
            metrics_state: deps.metrics_state,
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
        let trades = match self.trade_repo.find_needs_reconcile(self.batch_size).await {
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
        let decision = match self.decide(trade).await {
            Ok(decision) => decision,
            Err(error) => {
                tracing::warn!(%error, trade_id = %trade.trade_id, "trade reconciliation deferred");
                return;
            }
        };

        match decision {
            ReconcileDecision::Filled {
                shares,
                price,
                order_id,
                tx_hash,
                note,
            } => {
                let cost = shares * price;
                let observation = TradeObservation {
                    state: TradeState::FillObserved,
                    shares,
                    price,
                    cost_usd: cost,
                    fee_usd: Usd::ZERO,
                    order_id,
                    tx_hash,
                    net_profit_usd: None,
                    latency_ms: None,
                    error_message: Some(note.clone()),
                    confirmed_at: Utc::now(),
                };
                self.apply_observation(
                    trade,
                    observation,
                    TradeReconcileResolution::Filled,
                    &note,
                    false,
                )
                .await;
            }
            ReconcileDecision::Miss { note } => {
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
                    error_message: Some(note.clone()),
                    confirmed_at: Utc::now(),
                };
                self.apply_observation(
                    trade,
                    observation,
                    TradeReconcileResolution::Miss,
                    &note,
                    true,
                )
                .await;
            }
        }
    }

    async fn decide(&self, trade: &TradeInfo) -> Result<ReconcileDecision, String> {
        let after = trade
            .submitted_at
            .map(|ts| ts.timestamp().saturating_sub(TRADE_LOOKBACK_SECS));
        let mut trades = self
            .clob_client
            .get_trades(Some(&trade.market_id), Some(&trade.token_id), after)
            .await
            .map_err(|error| error.to_string())?;
        trades.sort_by_key(|item| item.matched_at);
        if let Some(clob_trade) = trades
            .into_iter()
            .find(|item| item.side == trade.side && item.size.inner() > Decimal::ZERO)
        {
            return Ok(Self::filled_from_clob(clob_trade));
        }

        let chain_balance = self
            .ctf_redeem
            .position_balance(&self.holder_address, &trade.token_id)
            .await
            .map_err(|error| error.to_string())?;
        if chain_balance >= trade.shares && chain_balance.is_positive() {
            return Ok(ReconcileDecision::Filled {
                shares: trade.shares,
                price: trade.price,
                order_id: trade.order_id.clone(),
                tx_hash: trade.tx_hash.clone(),
                note: format!("reconciled from CTF balance {chain_balance}"),
            });
        }

        Ok(ReconcileDecision::Miss {
            note: "no CLOB trade and no matching CTF balance after unknown FOK outcome".to_owned(),
        })
    }

    fn filled_from_clob(clob_trade: ClobTrade) -> ReconcileDecision {
        ReconcileDecision::Filled {
            shares: clob_trade.size,
            price: clob_trade.price,
            order_id: Some(clob_trade.order_id),
            tx_hash: Some(clob_trade.tx_hash),
            note: "reconciled from CLOB trades history".to_owned(),
        }
    }

    async fn apply_observation(
        &self,
        trade: &TradeInfo,
        observation: TradeObservation,
        resolution: TradeReconcileResolution,
        note: &str,
        release_reservation: bool,
    ) {
        match self
            .trade_repo
            .mark_reconciled_observed(&trade.trade_id, observation, resolution, note)
            .await
        {
            Ok(true) => {
                if release_reservation {
                    let reservation = ReservationHandle {
                        id: trade.reservation_id.clone(),
                        amount: trade.cost_usd,
                        market_id: trade.market_id.clone(),
                    };
                    if let Err(error) = self.capital_manager.release_sync(&reservation) {
                        tracing::error!(%error, trade_id = %trade.trade_id, "reconciled miss reservation release failed");
                        self.fsm
                            .enter_emergency("reconciled miss reservation release failed");
                    }
                }
                self.metrics_state.mark_stale();
                self.relay_notify.notify_one();
            }
            Ok(false) => {
                tracing::debug!(trade_id = %trade.trade_id, "trade reconciliation already resolved");
            }
            Err(error) => {
                tracing::error!(%error, trade_id = %trade.trade_id, "reconciled observation persist failed");
                self.fsm
                    .enter_emergency("reconciled observation persist failed");
            }
        }
    }
}
