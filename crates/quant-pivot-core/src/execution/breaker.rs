//! Execution breaker — stateful venue-health accumulator + auto kill-switch trip.
//!
//! Phase 05.3 admission is **stateless**; the breaker is the single component
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
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use quant_pivot_models::{
    domain::{KillSwitchPort, NewOperationLog, SetKillSwitchCommand},
    enums::{
        execution::KillSwitchState,
        operation_log::{OperationCategory, OperationOutcome},
        rbac::ResourceType,
    },
    runtime_config::ExecutionBreakerConfig,
    types::OperationLogId,
};
use quant_pivot_repository::traits::OperationLogRepository;

use crate::{execution::admission::VenueHealth, observability::metrics_hub::MetricsHub};

/// Audit actor recorded for breaker-initiated kill-switch escalations.
const BREAKER_ACTOR: &str = "system:execution_breaker";
/// Breaker dimension label for metrics / op-log (venue health, 05.4).
const DIMENSION_VENUE: &str = "venue";

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
}

/// Outcome of folding one observation into the accumulator.
struct RecordOutcome {
    health: VenueHealth,
    /// `true` only on the rising edge into sustained failure (trip the kill-switch).
    tripped: bool,
}

impl BreakerInner {
    /// Clear the transient failure window back to a clean `Healthy` slate.
    fn reset(&mut self) {
        self.samples.clear();
        self.consecutive_failures = 0;
        self.last_failure_at = None;
        self.tripped = false;
        self.health = VenueHealth::Healthy;
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
        RecordOutcome {
            health: self.health.clone(),
            tripped,
        }
    }
}

/// Stateful venue-health breaker that auto-trips the operational kill-switch.
pub struct ExecutionBreaker {
    config: ExecutionBreakerConfig,
    inner: Mutex<BreakerInner>,
    health: VenueHealthHandle,
    kill_switch: Arc<dyn KillSwitchPort>,
    operation_log: Arc<dyn OperationLogRepository>,
    metrics: Arc<MetricsHub>,
}

impl ExecutionBreaker {
    #[must_use]
    pub fn new(
        config: ExecutionBreakerConfig,
        kill_switch: Arc<dyn KillSwitchPort>,
        operation_log: Arc<dyn OperationLogRepository>,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        Self {
            config,
            inner: Mutex::new(BreakerInner::default()),
            health: VenueHealthHandle::default(),
            kill_switch,
            operation_log,
            metrics,
        }
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
        let outcome = {
            let mut inner = self.lock();
            inner.record(Instant::now(), success, &self.config)
        };
        self.health.store(outcome.health);
        if outcome.tripped {
            self.trip_kill_switch(DIMENSION_VENUE, detail).await;
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
        let health = {
            let mut inner = self.lock();
            if inner.tripped {
                if self.kill_switch.current().allows_new_entry() {
                    inner.reset();
                }
                // Otherwise the kill-switch latch is still held — stay Degraded
                // (no self-heal; only an operator ack clears the latch).
            } else if let VenueHealth::Degraded { .. } = inner.health {
                let healed = inner.last_failure_at.is_none_or(|last| {
                    Instant::now().duration_since(last)
                        >= Duration::from_secs(self.config.cooldown_secs)
                });
                if healed {
                    inner.samples.clear();
                    inner.consecutive_failures = 0;
                    inner.health = VenueHealth::Healthy;
                }
            }
            inner.health.clone()
        };
        self.health.store(health);
    }

    /// Clear the transient failure window back to a clean `Healthy` slate.
    ///
    /// The breaker also self-clears via [`Self::tick`] when it observes the
    /// operator has cleared the kill-switch; this is the explicit form for tests
    /// and any future synchronous clear path.
    pub fn reset(&self) {
        self.lock().reset();
        self.health.store(VenueHealth::Healthy);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BreakerInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub async fn trip_kill_switch(&self, dimension: &str, detail: &str) {
        let reason = format!("execution breaker tripped ({dimension}): {detail}");
        let command = SetKillSwitchCommand {
            target: KillSwitchState::ExecutionHalted,
            actor: BREAKER_ACTOR.to_owned(),
            reason: reason.clone(),
            ack: false,
            latch: true,
        };
        match self.kill_switch.set(command).await {
            Ok(_) => {
                self.metrics.inc_execution_breaker_trip(dimension);
                self.write_audit(dimension, &reason).await;
            }
            Err(error) => {
                tracing::error!(%error, "execution breaker failed to trip kill-switch");
            }
        }
    }

    /// Best-effort WORM audit for the system-initiated escalation (the operation
    /// audit middleware only covers HTTP-origin kill-switch changes).
    async fn write_audit(&self, dimension: &str, reason: &str) {
        let log = NewOperationLog {
            id: OperationLogId::from_v7(),
            request_id: format!("execution-breaker:{dimension}"),
            actor_user_id: None,
            actor_username: Some(BREAKER_ACTOR.to_owned()),
            acting_role: Some("execution_breaker".to_owned()),
            category: OperationCategory::Governance,
            action: "system.kill_switch.breaker_trip".to_owned(),
            resource_type: Some(ResourceType::System),
            resource_id: Some("kill_switch".to_owned()),
            http_method: "SYSTEM".to_owned(),
            http_path: "/system/execution-breaker/trip".to_owned(),
            http_status: 200,
            outcome: OperationOutcome::Success,
            client_ip: None,
            user_agent: None,
            latency_ms: 0,
            detail: serde_json::json!({ "dimension": DIMENSION_VENUE, "reason": reason }),
            governance_audit_event_id: None,
            governance_audit_sequence: None,
        };
        if let Err(error) = self.operation_log.append(log).await {
            tracing::error!(%error, "execution breaker failed to write trip audit log");
        }
    }
}
