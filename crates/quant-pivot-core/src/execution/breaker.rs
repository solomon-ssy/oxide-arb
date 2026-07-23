//! Execution breaker — stateful venue-health accumulator + auto kill-switch trip.
//!
//! admission is **stateless**; the breaker is the single component
//! that carries cross-decision accumulated safety state. It observes venue
//! submit/cancel outcomes over a rolling, monotone-time window and publishes two
//! read paths:
//!
//! - [`VenueHealthHandle`] (`ArcSwap<VenueHealth>`) — zero-lock hot read by
//!   admission `#18` (`VenueGuardCheck`).
//! - the held [`KillSwitchPort`] — sustained failure escalates to
//!   `execution_halted` (latched: operator ack required to clear).
//!
//! **Single source of truth for the latch is the kill-switch.** The breaker is
//! a *transient* venue-health accumulator only — it publishes `Healthy` /
//! `Degraded` and trips the kill-switch on sustained failure, but it never
//! latches its own halt state (which previously created a second, in-memory
//! source of truth that an operator could not clear without a restart):
//!
//! - **Transient degradation** → [`VenueHealth::Degraded`] (admission defers `#18`),
//!   self-recovers after `cooldown_secs` of no failures via [`ExecutionBreaker::tick`];
//!   never touches the kill-switch.
//! - **Sustained failure** → trips the kill-switch to `execution_halted` (latched:
//!   operator ack required). The authoritative *deny* of new entries comes from
//!   admission `#17` reading the kill-switch — not from the breaker. While the
//!   trip is in effect the breaker stays `Degraded`; once an operator clears the
//!   kill-switch back to a new-entry-admitting state, the next [`ExecutionBreaker::tick`]
//!   observes that and resets the failure window (no restart needed).

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use chrono::{DateTime, NaiveDate, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        governance::{KillSwitchView, NewOperationLog},
        ports::{KillSwitchPort, SetKillSwitchCommand},
    },
    enums::{
        execution::KillSwitchState,
        operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
        rbac::ResourceType,
    },
    hashing,
    runtime_config::ExecutionBreakerConfig,
    types::{OperationDetailDocument, OperationLogId, Usd},
};
use quant_pivot_repository::traits::OperationLogRepository;
use rust_decimal::Decimal;

use crate::{execution::admission::VenueHealth, observability::metrics_hub::MetricsHub};

/// Audit actor recorded for breaker-initiated kill-switch escalations.
const BREAKER_ACTOR: &str = "system:execution_breaker";
/// Breaker dimension label for venue-health metrics and operation logs.
const DIMENSION_VENUE: &str = "venue";
/// Breaker dimension label for the daily realized-loss escalation.
const DIMENSION_DAILY_LOSS: &str = "daily_loss";
/// Fraction of the daily realized-loss cap that degrades venue health (defers
/// admission `#18`) before the hard cap trips the kill-switch.
const DAILY_LOSS_DEGRADE_FRACTION: (i64, i64) = (8, 10);

/// Lock-free venue-health hot read shared with admission `#18`.
#[derive(Debug, Clone)]
pub struct VenueHealthHandle {
    inner: Arc<ArcSwap<VenueHealth>>,
}

impl VenueHealthHandle {
    #[must_use]
    pub fn new(initial: VenueHealth) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    /// Current venue health (clone of the published snapshot).
    #[must_use]
    pub fn current(&self) -> VenueHealth {
        self.inner.load().as_ref().clone()
    }

    fn store(&self, health: VenueHealth) {
        self.inner.store(Arc::new(health));
    }
}

impl Default for VenueHealthHandle {
    fn default() -> Self {
        Self::new(VenueHealth::Healthy)
    }
}

/// One observed venue outcome within the rolling window.
struct Sample {
    at: Instant,
    success: bool,
}

/// Mutex-guarded accumulator (low-frequency, off the admission hot path).
#[derive(Default)]
struct BreakerInner {
    samples: VecDeque<Sample>,
    consecutive_failures: u32,
    last_failure_at: Option<Instant>,
    health: VenueHealth,
    /// Whether the breaker has already tripped the kill-switch for the current
    /// failure episode. Latches the *trip action* (so a sustained failure trips
    /// exactly once on the rising edge), not the venue-health state. Cleared by
    /// [`BreakerInner::reset`] when the operator clears the kill-switch.
    tripped: bool,
    /// UTC day the realized-loss accumulator is scoped to (rolls over at the day
    /// boundary, using the same accounting basis as the equity snapshot.
    day: Option<NaiveDate>,
    /// Signed cumulative realized `PnL` for `day` (USD); the loss is `max(0, -·)`.
    realized_pnl_today: Decimal,
    /// Whether the daily realized-loss cap has already tripped the kill-switch
    /// for `day` (rising-edge latch; cleared on day rollover).
    daily_tripped: bool,
}

/// Outcome of folding one observation into the accumulator.
struct RecordOutcome {
    /// `true` only on the rising edge into sustained failure (trip the kill-switch).
    tripped: bool,
}

impl BreakerInner {
    /// Clear the transient venue failure window back to a clean `Healthy` slate.
    /// The daily realized-loss accumulator is intentionally left intact.
    fn reset(&mut self) {
        self.samples.clear();
        self.consecutive_failures = 0;
        self.last_failure_at = None;
        self.tripped = false;
        self.health = VenueHealth::Healthy;
    }

    /// Reset the daily realized-loss accumulator when the UTC day rolls over.
    /// A latched daily kill-switch trip is cleared here only as a *counter*
    /// reset — the operational kill-switch itself stays latched until operator
    /// ack (fail-closed).
    fn roll_day(&mut self, today: NaiveDate) {
        if self.day != Some(today) {
            self.day = Some(today);
            self.realized_pnl_today = Decimal::ZERO;
            self.daily_tripped = false;
        }
    }

    /// Cumulative same-day realized loss (`max(0, -ΣPnL)`).
    fn daily_loss(&self) -> Decimal {
        (-self.realized_pnl_today).max(Decimal::ZERO)
    }

    fn record(
        &mut self,
        now: Instant,
        success: bool,
        config: &ExecutionBreakerConfig,
    ) -> RecordOutcome {
        let window = Duration::from_secs(config.venue_window_secs);
        while let Some(front) = self.samples.front() {
            if now.duration_since(front.at) > window {
                self.samples.pop_front();
            } else {
                break;
            }
        }
        self.samples.push_back(Sample { at: now, success });
        if success {
            self.consecutive_failures = 0;
        } else {
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            self.last_failure_at = Some(now);
        }

        let total = self.samples.len();
        let failures = self.samples.iter().filter(|sample| !sample.success).count();
        let error_rate_bps = u32::try_from(
            failures
                .saturating_mul(10_000)
                .checked_div(total)
                .unwrap_or(0),
        )
        .unwrap_or(u32::MAX);

        let sustained_failure = self.consecutive_failures
            >= config.venue_consecutive_failures_to_halt
            || (u32::try_from(total).unwrap_or(u32::MAX) >= config.venue_min_window_samples
                && error_rate_bps >= config.venue_error_rate_bps_to_halt);
        let degraded = self.consecutive_failures >= config.venue_consecutive_failures_to_degrade;

        // SSOT: the breaker never latches its own halt. Sustained failure trips
        // the kill-switch (the single authoritative latch); the breaker's own
        // published health is at most a transient `Degraded` (admission `#18`
        // defers — the authoritative deny comes from `#17` reading the kill-switch).
        self.health = if sustained_failure {
            VenueHealth::Degraded {
                reason: format!(
                    "venue sustained failure: {} consecutive, {error_rate_bps} bps error rate",
                    self.consecutive_failures
                ),
            }
        } else if degraded {
            VenueHealth::Degraded {
                reason: format!(
                    "venue degraded: {} consecutive failures",
                    self.consecutive_failures
                ),
            }
        } else {
            // Below the degrade threshold: keep an existing Degraded until the
            // cooldown heals it in `tick` (avoids flapping).
            match &self.health {
                VenueHealth::Degraded { .. } => self.health.clone(),
                VenueHealth::Healthy => VenueHealth::Healthy,
            }
        };

        // Trip the kill-switch once per failure episode (rising edge only).
        let tripped = sustained_failure && !self.tripped;
        if sustained_failure {
            self.tripped = true;
        }
        RecordOutcome { tripped }
    }
}

/// Hot-swappable threshold snapshot: the governed config plus the pre-parsed
/// daily realized-loss cap. Held behind an [`ArcSwap`] so a runtime-config
/// activation atomically replaces the thresholds without touching the rolling
/// window accumulator in [`BreakerInner`].
pub(crate) struct BreakerThresholds {
    config: ExecutionBreakerConfig,
    /// Parsed daily realized-loss cap (USD). `0` disables the dimension.
    daily_loss_cap: Decimal,
}

impl BreakerThresholds {
    fn new(config: ExecutionBreakerConfig) -> QuantResult<Self> {
        let daily_loss_cap = config.daily_realized_loss_cap_usd.value;
        if daily_loss_cap < Decimal::ZERO {
            return Err(QuantError::config(
                "execution.breaker.daily_realized_loss_cap_usd must be non-negative",
            ));
        }
        Ok(Self {
            config,
            daily_loss_cap,
        })
    }
}

/// Stateful venue-health + daily-loss breaker that auto-trips the operational
/// kill-switch.
pub struct ExecutionBreaker {
    /// Hot-reloadable thresholds (swapped by the runtime-config applicator).
    thresholds: ArcSwap<BreakerThresholds>,
    inner: Mutex<BreakerInner>,
    health: VenueHealthHandle,
    kill_switch: Arc<dyn KillSwitchPort>,
    operation_log: Arc<dyn OperationLogRepository>,
    metrics: Arc<MetricsHub>,
}

impl ExecutionBreaker {
    pub fn new(
        config: ExecutionBreakerConfig,
        kill_switch: Arc<dyn KillSwitchPort>,
        operation_log: Arc<dyn OperationLogRepository>,
        metrics: Arc<MetricsHub>,
    ) -> QuantResult<Self> {
        Ok(Self {
            thresholds: ArcSwap::from_pointee(BreakerThresholds::new(config)?),
            inner: Mutex::new(BreakerInner::default()),
            health: VenueHealthHandle::default(),
            kill_switch,
            operation_log,
            metrics,
        })
    }

    /// Hot-reload the breaker thresholds from a newly activated runtime config.
    ///
    /// Only the thresholds/cooldown/daily-loss cap are replaced; the rolling
    /// failure window and daily accumulator in [`BreakerInner`] are preserved so
    /// an activation never resets in-flight safety state. The published venue
    /// health is recomputed immediately so a tightened daily-loss cap can move
    /// the breaker into the 80% degrade band without waiting for the next event.
    pub(crate) fn prepare_reload(
        config: &ExecutionBreakerConfig,
    ) -> QuantResult<BreakerThresholds> {
        BreakerThresholds::new(config.clone())
    }

    pub(crate) fn publish_reload(&self, thresholds: BreakerThresholds) {
        self.thresholds.store(Arc::new(thresholds));
        let (venue_health, daily_loss) = {
            let inner = self.lock();
            (inner.health.clone(), inner.daily_loss())
        };
        self.health
            .store(self.combined_health(&venue_health, daily_loss));
    }

    /// Clone of the venue-health hot-read handle (injected into admission).
    #[must_use]
    pub fn venue_health(&self) -> VenueHealthHandle {
        self.health.clone()
    }

    /// Fold one venue submit/cancel outcome into the accumulator.
    ///
    /// `success` is `false` only for *unconfirmed* venue calls (timeout / 5xx /
    /// unparseable); a clean venue rejection is a successful round-trip. On the
    /// escalation edge this trips the kill-switch (latched) outside the lock.
    pub async fn observe_venue(&self, success: bool, detail: &str) {
        let (tripped, venue_health, daily_loss) = {
            let thresholds = self.thresholds.load();
            let mut inner = self.lock();
            let tripped = inner
                .record(Instant::now(), success, &thresholds.config)
                .tripped;
            (tripped, inner.health.clone(), inner.daily_loss())
        };
        self.health
            .store(self.combined_health(&venue_health, daily_loss));
        if tripped {
            self.trip_kill_switch(DIMENSION_VENUE, detail).await;
        }
    }

    /// Fold one **realized** exit `PnL` into the daily accumulator.
    ///
    /// Cumulative same-day realized **loss** (`max(0, -ΣPnL)`) drives the third
    /// breaker dimension: `≥ 80%` of the cap degrades venue health (admission
    /// `#18` defers, slowing new entries); `≥` the cap trips the kill-switch to
    /// `execution_halted` (latched, operator ack required). The accumulator
    /// resets at the UTC day boundary (a latched kill-switch is **not** cleared
    /// by the rollover — fail-closed). `0` cap disables the dimension.
    pub async fn observe_realized_pnl(&self, pnl: Usd, now: DateTime<Utc>) {
        let daily_loss_cap = self.thresholds.load().daily_loss_cap;
        if daily_loss_cap <= Decimal::ZERO {
            return;
        }
        let (tripped, venue_health, loss) = {
            let mut inner = self.lock();
            inner.roll_day(now.date_naive());
            inner.realized_pnl_today += pnl.inner();
            let loss = inner.daily_loss();
            let tripped = loss >= daily_loss_cap && !inner.daily_tripped;
            if tripped {
                inner.daily_tripped = true;
            }
            (tripped, inner.health.clone(), loss)
        };
        self.health.store(self.combined_health(&venue_health, loss));
        if tripped {
            let detail = format!("daily realized loss {loss} reached cap {daily_loss_cap}");
            self.trip_kill_switch(DIMENSION_DAILY_LOSS, &detail).await;
        }
    }

    /// Combine the venue dimension with the daily-loss dimension into the single
    /// published [`VenueHealth`] admission reads. Takes plain values so the
    /// caller can drop the accumulator lock before computing.
    fn combined_health(&self, venue_health: &VenueHealth, daily_loss: Decimal) -> VenueHealth {
        let venue_reason = match venue_health {
            VenueHealth::Degraded { reason } => Some(reason.clone()),
            VenueHealth::Healthy => None,
        };
        let daily_loss_cap = self.thresholds.load().daily_loss_cap;
        let degrade_at = daily_loss_cap * Decimal::from(DAILY_LOSS_DEGRADE_FRACTION.0)
            / Decimal::from(DAILY_LOSS_DEGRADE_FRACTION.1);
        let daily_reason = (daily_loss_cap > Decimal::ZERO && daily_loss >= degrade_at)
            .then(|| format!("daily realized loss {daily_loss} ≥ 80% of cap {daily_loss_cap}"));
        match (venue_reason, daily_reason) {
            (None, None) => VenueHealth::Healthy,
            (Some(v), Some(d)) => VenueHealth::Degraded {
                reason: format!("{v}; {d}"),
            },
            (Some(reason), None) | (None, Some(reason)) => VenueHealth::Degraded { reason },
        }
    }

    /// Periodic self-heal. Two cases, in priority order:
    ///
    /// 1. **Operator cleared the kill-switch after a trip.** The kill-switch is
    ///    the single source of truth for the latch; once it is loosened back to a
    ///    new-entry-admitting state the breaker resets its failure window (no
    ///    process restart needed). While the trip is still in effect the breaker
    ///    holds `Degraded` and does *not* self-heal.
    /// 2. **Transient degradation (no trip).** `Degraded → Healthy` after
    ///    `cooldown_secs` of no failures.
    pub fn tick(&self) {
        let (venue_health, daily_loss) = {
            let mut inner = self.lock();
            // Roll the daily realized-loss accumulator at the UTC day boundary so
            // a stale 80%-degrade does not persist into a fresh day.
            inner.roll_day(Utc::now().date_naive());
            if inner.tripped {
                if self.kill_switch.current().allows_new_entry() {
                    inner.reset();
                }
                // Otherwise the kill-switch latch is still held — stay Degraded
                // (no self-heal; only an operator ack clears the latch).
            } else if let VenueHealth::Degraded { .. } = inner.health {
                let cooldown_secs = self.thresholds.load().config.cooldown_secs;
                let healed = inner.last_failure_at.is_none_or(|last| {
                    Instant::now().duration_since(last) >= Duration::from_secs(cooldown_secs)
                });
                if healed {
                    inner.samples.clear();
                    inner.consecutive_failures = 0;
                    inner.health = VenueHealth::Healthy;
                }
            }
            (inner.health.clone(), inner.daily_loss())
        };
        self.health
            .store(self.combined_health(&venue_health, daily_loss));
    }

    /// Clear the transient failure window back to a clean `Healthy` slate.
    ///
    /// The breaker also self-clears via [`Self::tick`] when it observes the
    /// operator has cleared the kill-switch; this is the explicit form for tests
    /// and any future synchronous clear path.
    pub fn reset(&self) {
        let (venue_health, daily_loss) = {
            let mut inner = self.lock();
            inner.reset();
            (inner.health.clone(), inner.daily_loss())
        };
        self.health
            .store(self.combined_health(&venue_health, daily_loss));
    }

    fn lock(&self) -> MutexGuard<'_, BreakerInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub async fn trip_kill_switch(&self, dimension: &str, detail: &str) {
        let reason = format!("execution breaker tripped ({dimension}): {detail}");
        let before = self.kill_switch.view();
        let command = SetKillSwitchCommand {
            expected_revision: before.revision,
            target: KillSwitchState::ExecutionHalted,
            actor: BREAKER_ACTOR.to_owned(),
            reason: reason.clone(),
            ack: false,
            latch: true,
        };
        match self.kill_switch.set(command).await {
            Ok(after) => {
                self.metrics.inc_execution_breaker_trip(dimension);
                self.write_audit(dimension, &reason, &before, &after).await;
            }
            Err(error) => {
                tracing::error!(%error, "execution breaker failed to trip kill-switch");
            }
        }
    }

    /// Best-effort WORM audit for the system-initiated escalation (the operation
    /// audit middleware only covers HTTP-origin kill-switch changes).
    async fn write_audit(
        &self,
        dimension: &str,
        reason: &str,
        before: &KillSwitchView,
        after: &KillSwitchView,
    ) {
        let before_hash = match hashing::canonical_state_hash(before) {
            Ok(hash) => Some(hash),
            Err(error) => {
                tracing::error!(%error, "execution breaker failed to hash kill-switch before state");
                None
            }
        };
        let after_hash = match hashing::canonical_state_hash(after) {
            Ok(hash) => Some(hash),
            Err(error) => {
                tracing::error!(%error, "execution breaker failed to hash kill-switch after state");
                None
            }
        };
        let detail = match OperationDetailDocument::from_serializable(&serde_json::json!({
            "dimension": dimension,
            "reason": reason,
        })) {
            Ok(detail) => detail,
            Err(error) => {
                tracing::error!(%error, "execution breaker rejected unsafe audit detail");
                return;
            }
        };
        let log = NewOperationLog {
            id: OperationLogId::from_v7(),
            request_id: format!("execution-breaker:{dimension}").into(),
            actor_user_id: None,
            actor_username: Some(BREAKER_ACTOR.to_owned()),
            acting_role: Some("execution_breaker".into()),
            category: OperationCategory::Governance,
            action: "system.kill_switch.breaker_trip".into(),
            resource_type: Some(ResourceType::System),
            resource_id: Some("kill_switch".to_owned()),
            http_method: OperationHttpMethod::System,
            http_path: "/system/execution-breaker/trip".to_owned(),
            http_status: 200,
            outcome: OperationOutcome::Success,
            client_ip: None,
            user_agent: None,
            latency_ms: 0,
            detail,
            before_hash,
            after_hash,
            governance_audit_event_id: None,
            governance_audit_sequence: None,
        };
        if let Err(error) = self.operation_log.append(log).await {
            tracing::error!(%error, "execution breaker failed to write trip audit log");
        }
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::runtime_config::{DecimalValue, ExecutionBreakerConfig};
    use rust_decimal_macros::dec;

    use super::BreakerThresholds;

    #[test]
    fn breaker_thresholds_reject_negative_daily_cap() {
        let config = ExecutionBreakerConfig {
            daily_realized_loss_cap_usd: DecimalValue::new(dec!(-1)),
            ..ExecutionBreakerConfig::default()
        };
        assert!(BreakerThresholds::new(config).is_err());
    }
}
