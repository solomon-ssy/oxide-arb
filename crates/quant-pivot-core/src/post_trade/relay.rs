//! Post-trade relay: notify-woken + periodic drain of durable trade work.

use crate::{
    execution::capital_manager::CapitalManager, observability::metrics_hub::MetricsHub,
    post_trade::consumer::PostTradeConsumer, runtime_config::RuntimeConfigStore,
};
use chrono::{Duration as ChronoDuration, Utc};
use oxide_arb_error::OxideError;
use oxide_arb_models::domain::{execution::ReservationHandle, trade::TradeInfo};
use oxide_arb_repository::traits::TradeRepository;
use std::{process, sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Drains claimed post-trade work into terminal state.
///
/// The execution path wakes it via [`Notify`]. The periodic tick is the
/// crash-recovery floor that reclaims expired leases after restart or missed wake.
/// Confirmation timing (`execution.timeout.trade_confirm_*`) is read from the
/// runtime-config store on every cycle, so activations apply without a restart.
pub struct PostTradeRelay {
    consumer: PostTradeConsumer,
    trade_repo: Arc<dyn TradeRepository>,
    notify: Arc<Notify>,
    capital_manager: Arc<CapitalManager>,
    batch_size: u64,
    runtime: Arc<RuntimeConfigStore>,
    claim_owner: String,
    metrics: Arc<MetricsHub>,
}

/// One relay cycle's view of `execution.timeout.trade_confirm_*`.
struct ConfirmTiming {
    /// Crash-recovery poll cadence.
    poll_interval: Duration,
    /// Age after which a `Submitted` trade is treated as orphaned.
    stale_submitted_after: Duration,
}

impl ConfirmTiming {
    /// Claim lease: generous multiple of the poll cadence so a healthy relay
    /// never loses a claim to a competing instance.
    fn claim_lease(&self) -> Duration {
        self.poll_interval
            .saturating_mul(3)
            .max(Duration::from_secs(5))
    }
}

pub struct PostTradeRelayDeps {
    pub consumer: PostTradeConsumer,
    pub trade_repo: Arc<dyn TradeRepository>,
    pub notify: Arc<Notify>,
    pub capital_manager: Arc<CapitalManager>,
    pub batch_size: u64,
    pub runtime: Arc<RuntimeConfigStore>,
    pub metrics: Arc<MetricsHub>,
}

impl PostTradeRelay {
    pub fn new(deps: PostTradeRelayDeps) -> Self {
        let claim_owner = format!("post-trade-relay:{}:{}", process::id(), Uuid::new_v4());
        Self {
            consumer: deps.consumer,
            trade_repo: deps.trade_repo,
            notify: deps.notify,
            capital_manager: deps.capital_manager,
            batch_size: deps.batch_size,
            runtime: deps.runtime,
            claim_owner,
            metrics: deps.metrics,
        }
    }

    /// Snapshot the confirmation timing knobs from the runtime-config store.
    ///
    /// Read exactly once per relay cycle so one cycle never observes a mix of
    /// pre- and post-activation values (poll cadence vs orphan timeout).
    fn confirm_timing(&self) -> ConfirmTiming {
        let config = self.runtime.load();
        let timeout = &config.execution.timeout;
        ConfirmTiming {
            // Clamped to >= 1s to avoid busy-looping.
            poll_interval: Duration::from_secs(timeout.trade_confirm_poll_interval_secs.max(1)),
            stale_submitted_after: Duration::from_secs(timeout.trade_confirm_timeout_secs),
        }
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), OxideError> {
        loop {
            let timing = self.confirm_timing();
            self.drain(&timing).await;
            self.scan_stale_submitted(&timing).await;
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    self.drain(&timing).await;
                    self.scan_stale_submitted(&timing).await;
                    return Ok(());
                }
                () = self.notify.notified() => {}
                () = tokio::time::sleep(timing.poll_interval) => {}
            }
        }
    }

    /// Claim and process unprocessed trades until the backlog is exhausted.
    async fn drain(&self, timing: &ConfirmTiming) {
        loop {
            let now = Utc::now();
            let Ok(lease) = ChronoDuration::from_std(timing.claim_lease()) else {
                tracing::warn!("post-trade relay claim lease exceeds chrono range");
                return;
            };
            let lease_expired_before = now - lease;
            let batch = match self
                .trade_repo
                .claim_unprocessed(
                    self.batch_size,
                    &self.claim_owner,
                    now,
                    lease_expired_before,
                )
                .await
            {
                Ok(batch) => batch,
                Err(error) => {
                    tracing::warn!(%error, "post-trade relay claim failed");
                    return;
                }
            };
            if batch.is_empty() {
                self.metrics.post_trade_relay_pending.set(0);
                return;
            }
            let claimed = batch.len();
            self.metrics
                .post_trade_relay_pending
                .set(i64::try_from(claimed).unwrap_or(i64::MAX));
            for trade in &batch {
                self.consumer.process(trade).await;
            }
            if (claimed as u64) < self.batch_size {
                self.metrics.post_trade_relay_pending.set(0);
                return;
            }
        }
    }

    async fn scan_stale_submitted(&self, timing: &ConfirmTiming) {
        let Ok(timeout) = ChronoDuration::from_std(timing.stale_submitted_after) else {
            tracing::warn!("post-trade relay stale timeout exceeds chrono range");
            return;
        };
        let older_than = Utc::now() - timeout;
        let stale = match self
            .trade_repo
            .find_stale_submitted(older_than, self.batch_size)
            .await
        {
            Ok(stale) => stale,
            Err(error) => {
                tracing::warn!(%error, "post-trade relay stale submitted scan failed");
                return;
            }
        };

        for trade in stale {
            self.mark_orphaned(&trade).await;
        }
    }

    async fn mark_orphaned(&self, trade: &TradeInfo) {
        match self.trade_repo.mark_orphaned(&trade.trade_id).await {
            Ok(true) => {
                let reservation = ReservationHandle {
                    id: trade.reservation_id.clone(),
                    amount: trade.cost_usd,
                    market_id: trade.market_id.clone(),
                };
                if let Err(error) = self
                    .capital_manager
                    .pin_for_reconciliation_sync(&reservation)
                {
                    tracing::error!(
                        %error,
                        trade_id = %trade.trade_id,
                        reservation_id = %trade.reservation_id,
                        "stale submitted reservation pin failed"
                    );
                }
                tracing::warn!(
                    trade_id = %trade.trade_id,
                    submitted_at = ?trade.submitted_at,
                    "stale submitted trade marked orphaned"
                );
            }
            Ok(false) => {
                tracing::debug!(trade_id = %trade.trade_id, "stale submitted trade already moved");
            }
            Err(error) => {
                tracing::warn!(%error, trade_id = %trade.trade_id, "mark orphaned failed");
            }
        }
    }
}
