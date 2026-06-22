//! Boot rehydration, snapshot refresh, and planned halt when durable rows block admission.

use crate::{
    execution::{capital_manager::CapitalManager, fsm::ExecutionFSM},
    exposure::in_memory::InMemoryExposureReservation,
    observability::alert_dispatcher::{Alert, AlertDispatcher},
    trade_integrity::store::TradeIntegrityStoreHandle,
};
use chrono::Utc;
use oxide_arb_error::{OxideError, OxideResult, storage::StorageError};
use oxide_arb_models::{
    domain::{TradeInfo, execution::ReservationHandle},
    enums::common::{AlertCategory, AlertLevel, AlertSource, TradeState},
};
use oxide_arb_repository::traits::TradeRepository;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::runtime_config::RuntimeConfigStore;

/// Coordinates durable-trade blocking state and in-memory reservation rehydration.
pub struct TradeIntegrityStore {
    inner: TradeIntegrityStoreHandle,
    trade_repo: Arc<dyn TradeRepository>,
    exposure: Arc<InMemoryExposureReservation>,
    fsm: Arc<ExecutionFSM>,
    runtime_config: Arc<RuntimeConfigStore>,
    alerts: Arc<AlertDispatcher>,
}

impl TradeIntegrityStore {
    pub fn new(
        trade_repo: Arc<dyn TradeRepository>,
        exposure: Arc<InMemoryExposureReservation>,
        fsm: Arc<ExecutionFSM>,
        runtime_config: Arc<RuntimeConfigStore>,
        alerts: Arc<AlertDispatcher>,
    ) -> Self {
        Self {
            inner: TradeIntegrityStoreHandle::default(),
            trade_repo,
            exposure,
            fsm,
            runtime_config,
            alerts,
        }
    }

    #[must_use]
    pub const fn handle(&self) -> &TradeIntegrityStoreHandle {
        &self.inner
    }

    #[must_use]
    pub fn load(&self) -> Arc<oxide_arb_models::domain::TradeIntegritySnapshot> {
        self.inner.load()
    }

    /// Reload counts from Postgres and in-memory reservations.
    pub async fn refresh_async(&self) -> Result<(), StorageError> {
        let blocking_count = self.trade_repo.count_blocking_trades().await?;
        let needs_reconcile_count = self.trade_repo.count_needs_reconcile().await?;
        let intent_orphan_count = self.trade_repo.count_intent_orphans().await?;
        let oldest_blocking_age_secs = self.trade_repo.oldest_blocking_age_secs().await?;

        let snapshot = oxide_arb_models::domain::TradeIntegritySnapshot {
            blocking_count: u32::try_from(blocking_count).unwrap_or(u32::MAX),
            needs_reconcile_count: u32::try_from(needs_reconcile_count).unwrap_or(u32::MAX),
            intent_orphan_count: u32::try_from(intent_orphan_count).unwrap_or(u32::MAX),
            oldest_blocking_age_secs,
            active_reservation_count: u32::try_from(self.exposure.active_count_sync())
                .unwrap_or(u32::MAX),
            reserved_usd: self.exposure.total_reserved_usd_sync(),
            checked_at: Utc::now(),
        };
        self.inner.publish(snapshot);
        Ok(())
    }

    /// Restore in-memory reservations from durable rows, refresh snapshot, halt if blocked.
    pub async fn boot_rehydrate(&self, capital: &CapitalManager) -> OxideResult<()> {
        let ttl_secs = self.runtime_config.current().risk.reservation_ttl_secs;
        let obligations = self
            .trade_repo
            .find_reservation_obligations(10_000)
            .await
            .map_err(OxideError::from)?;

        for trade in &obligations {
            rehydrate_trade_reservation(capital, trade, ttl_secs)?;
            if trade.state == TradeState::Intent {
                dispatch_intent_orphan_alert(&self.alerts, trade);
            }
        }

        self.refresh_async().await.map_err(OxideError::from)?;

        if self.load().blocking_count > 0 {
            self.fsm.enter_planned_halt(
                "durable trades unresolved at boot — reconcile queue must drain before trading",
            );
        }

        Ok(())
    }
}

fn rehydrate_trade_reservation(
    capital: &CapitalManager,
    trade: &TradeInfo,
    ttl_secs: u64,
) -> OxideResult<()> {
    let (reconcile_pinned, expires_at) = reservation_lifecycle(trade, ttl_secs);
    let handle = ReservationHandle {
        id: trade.reservation_id.clone(),
        amount: trade.cost_usd,
        market_id: trade.market_id.clone(),
    };
    capital
        .restore_sync(&handle, reconcile_pinned, expires_at)
        .map_err(OxideError::from)
}

fn reservation_lifecycle(trade: &TradeInfo, ttl_secs: u64) -> (bool, Instant) {
    match trade.state {
        TradeState::Submitted => (false, Instant::now() + Duration::from_secs(ttl_secs)),
        TradeState::Intent => (true, Instant::now() + Duration::from_secs(86400 * 365)),
        TradeState::FillObserved | TradeState::FillProcessing => (
            false,
            Instant::now() + Duration::from_secs(ttl_secs.saturating_mul(24)),
        ),
        _ => (
            true,
            Instant::now() + Duration::from_secs(ttl_secs.saturating_mul(24)),
        ),
    }
}

fn dispatch_intent_orphan_alert(alerts: &Arc<AlertDispatcher>, trade: &TradeInfo) {
    let alert = Alert::new(
        format!("integrity.intent_orphan.{}", trade.trade_id),
        AlertLevel::Emergency,
        AlertCategory::TradingSafety,
        AlertSource::Execution,
        "Crash-orphaned trade intent requires operator review",
        format!(
            "trade {} ({}) persisted in Intent with reservation {} — verify venue state before resuming",
            trade.trade_id, trade.market_id, trade.reservation_id
        ),
        Utc::now(),
    )
    .with_affects_trading(true)
    .with_dedupe_secs(300);

    alerts.dispatch_background(alert);
}
