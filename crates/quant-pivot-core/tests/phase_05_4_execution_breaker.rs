//! Phase 05.4 — execution breaker + venue-error classification unit tests.
//!
//! Pure / in-memory: the breaker's rolling window, two-level health output, and
//! latched kill-switch trip are exercised with stub `KillSwitchPort` /
//! `OperationLogRepository`, no DB or venue.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use quant_pivot_core::{
    execution::{ExecutionBreaker, VenueHealth, VenueOutcome},
    observability::metrics_hub::MetricsHub,
};
use quant_pivot_error::{QuantResult, api::ApiError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        KillSwitchPort, KillSwitchView, NewOperationLog, OperationLogInfo, OperationLogQuery,
        Paginated, SetKillSwitchCommand,
    },
    enums::execution::KillSwitchState,
    runtime_config::ExecutionBreakerConfig,
};
use quant_pivot_repository::traits::OperationLogRepository;

/// Records every governed kill-switch transition and mirrors the current state
/// (so `current()` reflects a breaker trip, as the real control plane does).
struct RecordingKillSwitch {
    sets: Mutex<Vec<SetKillSwitchCommand>>,
    state: Mutex<KillSwitchState>,
}

impl Default for RecordingKillSwitch {
    fn default() -> Self {
        Self {
            sets: Mutex::new(Vec::new()),
            state: Mutex::new(KillSwitchState::Closed),
        }
    }
}

impl RecordingKillSwitch {
    /// Simulate an operator clearing the kill-switch back to `Closed`.
    fn operator_clear(&self) {
        *self.state.lock().unwrap() = KillSwitchState::Closed;
    }
}

#[async_trait]
impl KillSwitchPort for RecordingKillSwitch {
    fn current(&self) -> KillSwitchState {
        *self.state.lock().unwrap()
    }

    fn view(&self) -> KillSwitchView {
        KillSwitchView {
            state: self.current(),
            requires_operator_ack: false,
            last_reason: "test".to_owned(),
            changed_by: "test".to_owned(),
            changed_at: chrono::Utc::now(),
        }
    }

    async fn set(&self, command: SetKillSwitchCommand) -> QuantResult<KillSwitchView> {
        let view = KillSwitchView {
            state: command.target,
            requires_operator_ack: command.target.is_emergency() || command.latch,
            last_reason: command.reason.clone(),
            changed_by: command.actor.clone(),
            changed_at: chrono::Utc::now(),
        };
        *self.state.lock().unwrap() = command.target;
        self.sets.lock().unwrap().push(command);
        Ok(view)
    }
}

/// Counts appended operation-log rows (breaker trip audit).
#[derive(Default)]
struct RecordingOpLog {
    appended: Mutex<u32>,
}

#[async_trait]
impl OperationLogRepository for RecordingOpLog {
    async fn append(&self, _log: NewOperationLog) -> Result<(), StorageError> {
        *self.appended.lock().unwrap() += 1;
        Ok(())
    }

    async fn append_batch(&self, logs: Vec<NewOperationLog>) -> Result<(), StorageError> {
        *self.appended.lock().unwrap() += u32::try_from(logs.len()).unwrap_or(u32::MAX);
        Ok(())
    }

    async fn page(
        &self,
        _query: OperationLogQuery,
    ) -> Result<Paginated<OperationLogInfo>, StorageError> {
        Ok(Paginated::empty(1, 0))
    }
}

const fn config(degrade: u32, halt: u32, cooldown_secs: u64) -> ExecutionBreakerConfig {
    ExecutionBreakerConfig {
        venue_consecutive_failures_to_degrade: degrade,
        venue_consecutive_failures_to_halt: halt,
        // Disable the error-rate gate for these consecutive-failure tests.
        venue_error_rate_bps_to_halt: 10_001,
        venue_min_window_samples: u32::MAX,
        venue_window_secs: 60,
        cooldown_secs,
    }
}

fn breaker(
    config: ExecutionBreakerConfig,
) -> (
    Arc<ExecutionBreaker>,
    Arc<RecordingKillSwitch>,
    Arc<RecordingOpLog>,
    Arc<MetricsHub>,
) {
    let kill_switch = Arc::new(RecordingKillSwitch::default());
    let op_log = Arc::new(RecordingOpLog::default());
    let metrics = Arc::new(MetricsHub::new());
    let breaker = Arc::new(ExecutionBreaker::new(
        config,
        Arc::clone(&kill_switch) as Arc<dyn KillSwitchPort>,
        Arc::clone(&op_log) as Arc<dyn OperationLogRepository>,
        Arc::clone(&metrics),
    ));
    (breaker, kill_switch, op_log, metrics)
}

#[tokio::test]
async fn degrades_to_defer_then_self_recovers_after_cooldown() {
    let (breaker, kill_switch, _op_log, _metrics) = breaker(config(2, 99, 0));

    breaker.observe_venue(false, "timeout").await;
    assert_eq!(breaker.venue_health().current(), VenueHealth::Healthy);
    breaker.observe_venue(false, "timeout").await;
    assert!(matches!(
        breaker.venue_health().current(),
        VenueHealth::Degraded { .. }
    ));
    // Transient degradation must never touch the kill-switch.
    assert!(kill_switch.sets.lock().unwrap().is_empty());

    // cooldown_secs = 0 → next tick heals.
    breaker.tick();
    assert_eq!(breaker.venue_health().current(), VenueHealth::Healthy);
}

#[tokio::test]
async fn degraded_does_not_heal_before_cooldown() {
    let (breaker, _ks, _op, _m) = breaker(config(2, 99, 3_600));
    breaker.observe_venue(false, "x").await;
    breaker.observe_venue(false, "x").await;
    assert!(matches!(
        breaker.venue_health().current(),
        VenueHealth::Degraded { .. }
    ));
    breaker.tick();
    assert!(
        matches!(
            breaker.venue_health().current(),
            VenueHealth::Degraded { .. }
        ),
        "must stay degraded until the cooldown elapses",
    );
}

#[tokio::test]
async fn sustained_failure_trips_latched_kill_switch_breaker_stays_degraded() {
    let (breaker, kill_switch, op_log, metrics) = breaker(config(2, 3, 0));

    breaker.observe_venue(false, "e1").await;
    breaker.observe_venue(false, "e2").await;
    breaker.observe_venue(false, "e3").await;

    // SSOT: the breaker never latches its own halt — it stays `Degraded` (#18
    // defers) and the authoritative deny comes from #17 (the tripped kill-switch).
    assert!(matches!(
        breaker.venue_health().current(),
        VenueHealth::Degraded { .. }
    ));
    let sets = kill_switch.sets.lock().unwrap();
    assert_eq!(sets.len(), 1, "exactly one trip on the escalation edge");
    assert_eq!(sets[0].target, KillSwitchState::ExecutionHalted);
    assert!(
        sets[0].latch,
        "breaker trip must latch (operator ack to clear)"
    );
    assert!(!sets[0].ack);
    assert_eq!(sets[0].actor, "system:execution_breaker");
    drop(sets);
    assert_eq!(
        *op_log.appended.lock().unwrap(),
        1,
        "trip writes one audit row"
    );
    assert_eq!(
        metrics
            .execution_breaker_trips
            .with_label_values(&["venue"])
            .get(),
        1
    );
}

#[tokio::test]
async fn trip_does_not_re_fire_kill_switch_while_latched() {
    let (breaker, kill_switch, _op, _m) = breaker(config(2, 3, 0));
    for tag in ["e1", "e2", "e3", "e4", "e5"] {
        breaker.observe_venue(false, tag).await;
    }
    assert_eq!(
        kill_switch.sets.lock().unwrap().len(),
        1,
        "sustained failure trips the kill-switch exactly once (rising edge only)",
    );
}

#[tokio::test]
async fn venue_success_resets_failure_window() {
    let (breaker, kill_switch, _op, _m) = breaker(config(3, 99, 0));
    breaker.observe_venue(false, "x").await;
    breaker.observe_venue(false, "x").await;
    breaker.observe_venue(true, "ok").await; // resets consecutive failures
    breaker.observe_venue(false, "x").await;
    breaker.observe_venue(false, "x").await;
    assert_eq!(
        breaker.venue_health().current(),
        VenueHealth::Healthy,
        "two failures after a success must not reach the degrade threshold of 3",
    );
    assert!(kill_switch.sets.lock().unwrap().is_empty());
}

#[tokio::test]
async fn trip_stays_degraded_until_operator_clears_then_tick_resets() {
    let (breaker, kill_switch, _op, _m) = breaker(config(2, 2, 0));
    breaker.observe_venue(false, "x").await;
    breaker.observe_venue(false, "x").await;
    // Tripped: breaker is Degraded and the (latched) kill-switch is the deny SSOT.
    assert!(matches!(
        breaker.venue_health().current(),
        VenueHealth::Degraded { .. }
    ));
    assert_eq!(kill_switch.current(), KillSwitchState::ExecutionHalted);

    // While the kill-switch latch is held, a later success + tick must NOT clear
    // the breaker (only an operator ack clears the latch).
    breaker.observe_venue(true, "ok").await;
    breaker.tick();
    assert!(matches!(
        breaker.venue_health().current(),
        VenueHealth::Degraded { .. }
    ));

    // Operator clears the kill-switch → the next tick resets the breaker window
    // back to Healthy (no process restart needed; SSOT = kill-switch).
    kill_switch.operator_clear();
    breaker.tick();
    assert_eq!(breaker.venue_health().current(), VenueHealth::Healthy);
}

#[tokio::test]
async fn halts_on_error_rate_bps_after_min_window_samples() {
    let (breaker, kill_switch, _op, metrics) = breaker(ExecutionBreakerConfig {
        venue_consecutive_failures_to_degrade: 99,
        venue_consecutive_failures_to_halt: 99,
        venue_error_rate_bps_to_halt: 5_000,
        venue_min_window_samples: 4,
        venue_window_secs: 60,
        cooldown_secs: 0,
    });

    breaker.observe_venue(false, "e1").await;
    breaker.observe_venue(true, "ok").await;
    breaker.observe_venue(false, "e2").await;
    breaker.observe_venue(false, "e3").await;

    // Error-rate breach trips the kill-switch; the breaker stays Degraded (SSOT).
    assert!(matches!(
        breaker.venue_health().current(),
        VenueHealth::Degraded { .. }
    ));
    assert_eq!(kill_switch.sets.lock().unwrap().len(), 1);
    assert_eq!(
        metrics
            .execution_breaker_trips
            .with_label_values(&["venue"])
            .get(),
        1
    );
}

#[test]
fn venue_errors_classify_unconfirmed_as_ambiguous() {
    // Cleanly-rejected (never reached the matching engine) → safe to release.
    assert_eq!(
        VenueOutcome::from(&ApiError::Http {
            method: "POST",
            url: "https://clob/order".to_owned(),
            status: 400,
            body: "bad".to_owned(),
            retryable: false,
        }),
        VenueOutcome::Rejected,
    );
    assert_eq!(
        VenueOutcome::from(&ApiError::RateLimited {
            retry_after_ms: 100,
            bucket: "order".to_owned(),
        }),
        VenueOutcome::Ambiguous,
        "429 after place_order may have executed — hold capital",
    );
    assert_eq!(
        VenueOutcome::from(&ApiError::Http {
            method: "POST",
            url: "https://clob/order".to_owned(),
            status: 429,
            body: "rate limited".to_owned(),
            retryable: true,
        }),
        VenueOutcome::Ambiguous,
    );
    assert_eq!(
        VenueOutcome::from(&ApiError::Clob {
            endpoint: "order".to_owned(),
            code: "geoblock".to_owned(),
            message: "blocked".to_owned(),
            retryable: false,
        }),
        VenueOutcome::Rejected,
    );

    // Unconfirmed (may have executed) → hold capital, reconcile.
    assert_eq!(
        VenueOutcome::from(&ApiError::Timeout {
            operation: "post_order".to_owned(),
            elapsed_ms: 5_000,
        }),
        VenueOutcome::Ambiguous,
    );
    assert_eq!(
        VenueOutcome::from(&ApiError::Http {
            method: "POST",
            url: "https://clob/order".to_owned(),
            status: 503,
            body: "down".to_owned(),
            retryable: true,
        }),
        VenueOutcome::Ambiguous,
    );
    assert_eq!(
        VenueOutcome::from(&ApiError::Deserialize {
            context: "post_order".to_owned(),
            detail: "garbled".to_owned(),
        }),
        VenueOutcome::Ambiguous,
    );
    assert_eq!(
        VenueOutcome::from(&ApiError::Sdk("unknown".to_owned())),
        VenueOutcome::Ambiguous,
    );
}

#[test]
fn restriction_rank_orders_loosening() {
    // Loosening = lower rank; the governed set() guard requires ack to loosen a latch.
    assert!(
        KillSwitchState::Closed.restriction_rank()
            < KillSwitchState::ExecutionHalted.restriction_rank()
    );
    assert!(
        KillSwitchState::ExecutionHalted.restriction_rank()
            < KillSwitchState::EmergencyHalted.restriction_rank()
    );
    assert!(
        KillSwitchState::ExitOnly.restriction_rank()
            < KillSwitchState::ExecutionHalted.restriction_rank()
    );
}
