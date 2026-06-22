//! Operator closure for unresolvable reconciliation trades.

use crate::{
    execution::{capital_manager::CapitalManager, fsm::ExecutionFSM},
    observability::alert_dispatcher::{Alert, AlertDispatcher},
    service::risk_metrics::RiskMetricsState,
};
use chrono::Utc;
use oxide_arb_error::OxideError;
use oxide_arb_models::{
    domain::execution::ReservationHandle,
    enums::common::{AlertCategory, AlertLevel, AlertSource, TradeState},
    types::TradeId,
};
use oxide_arb_repository::traits::TradeRepository;
use std::sync::Arc;

/// Dependencies for closing orphaned trades as operator-declared unresolvable.
#[derive(Clone)]
pub struct CloseUnresolvableService {
    trade_repo: Arc<dyn TradeRepository>,
    capital_manager: Arc<CapitalManager>,
    fsm: Arc<ExecutionFSM>,
    alerts: Arc<AlertDispatcher>,
    metrics_state: Arc<RiskMetricsState>,
}

impl CloseUnresolvableService {
    /// Build a closure service from shared runtime handles.
    #[must_use]
    pub fn new(
        trade_repo: Arc<dyn TradeRepository>,
        capital_manager: Arc<CapitalManager>,
        fsm: Arc<ExecutionFSM>,
        alerts: Arc<AlertDispatcher>,
        metrics_state: Arc<RiskMetricsState>,
    ) -> Self {
        Self {
            trade_repo,
            capital_manager,
            fsm,
            alerts,
            metrics_state,
        }
    }

    /// Close an orphaned trade as operator-declared unresolvable.
    ///
    /// Releases the reservation, marks the row terminal (`Failed`), and emits a
    /// critical alert. Fail-closed on persistence or reservation release errors.
    pub async fn close(
        &self,
        trade_id: &TradeId,
        note: &str,
        operator: &str,
    ) -> Result<bool, OxideError> {
        let trade = self
            .trade_repo
            .find_by_id(trade_id)
            .await?
            .ok_or_else(|| OxideError::Internal(format!("trade not found: {trade_id}")))?;
        if trade.state != TradeState::Orphaned || !trade.needs_reconcile {
            return Ok(false);
        }

        let reservation = ReservationHandle {
            id: trade.reservation_id.clone(),
            amount: trade.cost_usd + trade.fee_usd,
            market_id: trade.market_id.clone(),
        };
        if let Err(error) = self.capital_manager.release_sync(&reservation) {
            self.fsm.enter_emergency(
                crate::execution::fsm::EmergencyClass::ReservationFault,
                "unresolvable close reservation release failed",
            );
            return Err(error.into());
        }

        let applied = self
            .trade_repo
            .close_unresolvable_terminal(trade_id, note, operator, Utc::now())
            .await?;
        if !applied {
            return Ok(false);
        }

        self.metrics_state.mark_stale();
        self.alerts.dispatch_background(
            Alert::new(
                "reconciliation.unresolvable",
                AlertLevel::Critical,
                AlertCategory::TradingSafety,
                AlertSource::Execution,
                "Trade marked unresolvable",
                format!("trade {trade_id} closed by {operator}: {note}"),
                Utc::now(),
            )
            .with_affects_trading(true),
        );
        Ok(true)
    }
}
