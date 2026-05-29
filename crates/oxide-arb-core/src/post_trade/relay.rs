//! Post-trade relay: notify-woken + periodic drain of durable trade work.

use crate::{
    execution::capital_manager::CapitalManager, observability::metrics_hub::MetricsHub,
    post_trade::consumer::PostTradeConsumer,
};
use chrono::{Duration as ChronoDuration, Utc};
use oxide_arb_error::OxideError;
use oxide_arb_models::domain::{execution::ReservationHandle, trade::TradeInfo};
use oxide_arb_repository::traits::TradeRepository;
use std::{process, sync::Arc, time::Duration};
use tokio::{sync::Notify, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Drains claimed post-trade work into terminal state.
///
/// The execution path wakes it via [`Notify`]. The periodic tick is the
/// crash-recovery floor that reclaims expired leases after restart or missed wake.
pub struct PostTradeRelay {
    consumer: PostTradeConsumer,
    trade_repo: Arc<dyn TradeRepository>,
    notify: Arc<Notify>,
    capital_manager: Arc<CapitalManager>,
    batch_size: u64,
    poll_interval: Duration,
    stale_submitted_after: Duration,
    claim_lease: Duration,
    claim_owner: String,
    metrics: Arc<MetricsHub>,
}

pub struct PostTradeRelayDeps {
    pub consumer: PostTradeConsumer,
    pub trade_repo: Arc<dyn TradeRepository>,
    pub notify: Arc<Notify>,
    pub capital_manager: Arc<CapitalManager>,
    pub batch_size: u64,
    pub poll_interval: Duration,
    pub stale_submitted_after: Duration,
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
            poll_interval: deps.poll_interval,
            stale_submitted_after: deps.stale_submitted_after,
            claim_lease: deps
                .poll_interval
                .saturating_mul(3)
                .max(Duration::from_secs(5)),
            claim_owner,
            metrics: deps.metrics,
        }
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), OxideError> {
        let mut interval = tokio::time::interval(self.poll_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            self.drain().await;
            self.scan_stale_submitted().await;
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    self.drain().await;
                    self.scan_stale_submitted().await;
                    return Ok(());
                }
                () = self.notify.notified() => {}
                _ = interval.tick() => {}
            }
        }
    }

    /// Claim and process unprocessed trades until the backlog is exhausted.
    async fn drain(&self) {
        loop {
            let now = Utc::now();
            let Ok(lease) = ChronoDuration::from_std(self.claim_lease) else {
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

    async fn scan_stale_submitted(&self) {
        let Ok(timeout) = ChronoDuration::from_std(self.stale_submitted_after) else {
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
                if let Err(error) = self.capital_manager.release_sync(&reservation) {
                    tracing::debug!(
                        %error,
                        trade_id = %trade.trade_id,
                        reservation_id = %trade.reservation_id,
                        "stale submitted reservation was already absent"
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
