//! 5-state circuit breaker FSM with 4 severity levels (L1-L4).
//!
//! ```text
//!                     ┌── Halted (L3/L4) ── acknowledge_and_resume ──┐
//!                     │   NO tick transition                          │
//!                     └──────────▲───────────────────────────────────┘
//!                                │ halt()                             ▼
//! Closed ──trip──▶ Open ──cooldown──▶ HalfOpen ──probes──▶ Recovered ──▶ Closed
//!   ▲                ▲                    │
//!   │                └── probe fail ──────┘
//! ```
//!
//! - L2 Session: `trip()` → Open (auto-recovery via `HalfOpen`)
//! - L3 Daily / L4 System: `halt()` → Halted (requires `acknowledge_and_resume()`)
//! - `reset()` → any state → Closed (testing / escape hatch)

use crate::{clock::Clock, types::BreakerState};
use chrono::{DateTime, Utc};
use num_traits::ToPrimitive;
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::{
    config::CircuitBreakerConfig,
    domain::risk::RiskEngineState,
    enums::risk::{BreakerStateName, CircuitBreakerLevel},
};
use std::sync::Arc;

#[derive(Clone)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    clock: Arc<dyn Clock>,
    state: BreakerState,
    l2_trip_count: u32,
    heartbeat_failures: u32,
    last_transition_at: DateTime<Utc>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker in Closed state.
    #[must_use]
    pub fn new(config: CircuitBreakerConfig, clock: Arc<dyn Clock>) -> Self {
        let now = clock.now();
        Self {
            config,
            clock,
            state: BreakerState::Closed,
            l2_trip_count: 0,
            heartbeat_failures: 0,
            last_transition_at: now,
        }
    }

    /// Restore from a persisted snapshot (crash recovery).
    ///
    /// **Fail-closed**: if the snapshot is in `Open` state but missing
    /// `breaker_level`, `halt_reason`, or `cooldown_until`, this returns
    /// an error rather than silently defaulting.
    pub fn from_snapshot(
        config: CircuitBreakerConfig,
        clock: Arc<dyn Clock>,
        snapshot: &RiskEngineState,
    ) -> OxideResult<Self> {
        let state = match snapshot.breaker_state {
            BreakerStateName::Closed => BreakerState::Closed,
            BreakerStateName::Open => {
                let level = snapshot.breaker_level.ok_or_else(|| {
                    OxideError::Internal(
                        "corrupt breaker snapshot: Open state missing breaker_level".into(),
                    )
                })?;
                let reason = snapshot.halt_reason.clone().ok_or_else(|| {
                    OxideError::Internal(
                        "corrupt breaker snapshot: Open state missing halt_reason".into(),
                    )
                })?;
                let cooldown_until = snapshot.cooldown_until.ok_or_else(|| {
                    OxideError::Internal(
                        "corrupt breaker snapshot: Open state missing cooldown_until".into(),
                    )
                })?;
                BreakerState::Open {
                    level,
                    reason,
                    tripped_at: snapshot.snapshot_at,
                    cooldown_until,
                }
            }
            BreakerStateName::HalfOpen => {
                let level = snapshot.breaker_level.ok_or_else(|| {
                    OxideError::Internal(
                        "corrupt breaker snapshot: HalfOpen state missing breaker_level".into(),
                    )
                })?;
                BreakerState::HalfOpen {
                    level,
                    entered_at: snapshot.snapshot_at,
                    successful_probes: 0,
                    required_probes: config.half_open_probes,
                }
            }
            BreakerStateName::Recovered => BreakerState::Recovered {
                entered_at: snapshot.snapshot_at,
                observation_until: snapshot.snapshot_at
                    + chrono::Duration::seconds(
                        ToPrimitive::to_i64(&config.recovery_observation_secs).unwrap_or(i64::MAX),
                    ),
            },
            BreakerStateName::Halted => {
                let level = snapshot.breaker_level.ok_or_else(|| {
                    OxideError::Internal(
                        "corrupt breaker snapshot: Halted state missing breaker_level".into(),
                    )
                })?;
                let reason = snapshot.halt_reason.clone().ok_or_else(|| {
                    OxideError::Internal(
                        "corrupt breaker snapshot: Halted state missing halt_reason".into(),
                    )
                })?;
                BreakerState::Halted {
                    level,
                    reason,
                    halted_at: snapshot.snapshot_at,
                }
            }
        };

        Ok(Self {
            config,
            clock,
            state,
            l2_trip_count: ToPrimitive::to_u32(&snapshot.cooldown_multiplier).unwrap_or(0),
            heartbeat_failures: 0,
            last_transition_at: snapshot.snapshot_at,
        })
    }

    #[must_use]
    #[inline]
    pub const fn state(&self) -> &BreakerState {
        &self.state
    }

    #[must_use]
    #[inline]
    pub const fn allows_trading(&self) -> bool {
        self.state.allows_trading()
    }

    #[must_use]
    #[inline]
    pub const fn is_probe_mode(&self) -> bool {
        self.state.is_probe_mode()
    }

    /// Trip the breaker to Open state for **L2 Session** level only.
    ///
    /// For L3 Daily / L4 System, use `halt()` instead. This enforces the
    /// design invariant that L3/L4 require manual operator acknowledgement.
    ///
    /// Higher level overrides lower. Same or higher level refreshes cooldown.
    pub fn trip(&mut self, level: CircuitBreakerLevel, reason: String) {
        let now = self.clock.now();
        let cooldown_secs = self.cooldown_for_level(level);
        let cooldown_until = now
            + chrono::Duration::seconds(ToPrimitive::to_i64(&cooldown_secs).unwrap_or(i64::MAX));

        if level == CircuitBreakerLevel::Session {
            self.l2_trip_count += 1;
        }

        let should_trip = match &self.state {
            BreakerState::Halted { .. } => false,
            BreakerState::Closed
            | BreakerState::HalfOpen { .. }
            | BreakerState::Recovered { .. } => true,
            BreakerState::Open {
                level: current_level,
                ..
            } => level >= *current_level,
        };

        if should_trip {
            tracing::warn!(
                %level,
                %reason,
                cooldown_secs,
                l2_trip_count = self.l2_trip_count,
                "circuit breaker tripped"
            );
            self.state = BreakerState::Open {
                level,
                reason,
                tripped_at: now,
                cooldown_until,
            };
            self.last_transition_at = now;
        }
    }

    /// Halt the breaker for **L3 Daily / L4 System** — no automatic recovery.
    ///
    /// Requires `acknowledge_and_resume()` to return to Closed.
    /// Can override Open/HalfOpen/Recovered but never downgrades an existing Halted.
    pub fn halt(&mut self, level: CircuitBreakerLevel, reason: String) {
        let should_halt = match &self.state {
            BreakerState::Halted { level: current, .. } => level > *current,
            _ => true,
        };

        if should_halt {
            let now = self.clock.now();
            tracing::error!(
                %level,
                %reason,
                "circuit breaker HALTED — manual intervention required"
            );
            self.state = BreakerState::Halted {
                level,
                reason,
                halted_at: now,
            };
            self.last_transition_at = now;
        }
    }

    /// Operator intervention — resume from Halted state to Closed.
    ///
    /// Returns `true` if the breaker was in Halted state and has been resumed.
    /// Returns `false` if the breaker was not halted (no-op).
    pub fn acknowledge_and_resume(&mut self, operator_ack: &str) -> bool {
        if let BreakerState::Halted { level, reason, .. } = &self.state {
            tracing::warn!(
                %level,
                previous_reason = %reason,
                ack = operator_ack,
                "circuit breaker resumed from Halted by operator"
            );
            self.state = BreakerState::Closed;
            self.l2_trip_count = 0;
            self.last_transition_at = self.clock.now();
            true
        } else {
            false
        }
    }

    /// Periodic tick — drives time-based state transitions.
    ///
    /// Returns `true` if a state transition occurred.
    /// **Halted** state is never auto-transitioned — requires explicit
    /// `acknowledge_and_resume()`.
    #[must_use]
    pub fn tick(&mut self) -> bool {
        let now = self.clock.now();
        match &self.state {
            BreakerState::Open {
                level,
                cooldown_until,
                ..
            } if now >= *cooldown_until => {
                let level = *level;
                tracing::info!(
                    %level,
                    "cooldown expired, transitioning to HalfOpen"
                );
                self.state = BreakerState::HalfOpen {
                    level,
                    entered_at: now,
                    successful_probes: 0,
                    required_probes: self.config.half_open_probes,
                };
                self.last_transition_at = now;
                true
            }
            BreakerState::Recovered {
                observation_until, ..
            } if now >= *observation_until => {
                tracing::info!("observation period complete, returning to Closed");
                self.state = BreakerState::Closed;
                self.l2_trip_count = 0;
                self.last_transition_at = now;
                true
            }
            _ => false,
        }
    }

    /// Report a trade result while in `HalfOpen` state.
    pub fn on_trade_result(&mut self, success: bool) {
        let now = self.clock.now();
        if let BreakerState::HalfOpen {
            level,
            successful_probes,
            required_probes,
            ..
        } = &mut self.state
        {
            if success {
                *successful_probes += 1;
                tracing::info!(
                    successful = *successful_probes,
                    required = *required_probes,
                    "HalfOpen probe succeeded"
                );
                if *successful_probes >= *required_probes {
                    let observation_until = now
                        + chrono::Duration::seconds(
                            ToPrimitive::to_i64(&self.config.recovery_observation_secs)
                                .unwrap_or(i64::MAX),
                        );
                    self.state = BreakerState::Recovered {
                        entered_at: now,
                        observation_until,
                    };
                    self.last_transition_at = now;
                }
            } else {
                let level = *level;
                let cooldown_secs = self.cooldown_for_level(level) * 2;
                let cooldown_secs = cooldown_secs.min(self.config.max_cooldown_secs);
                tracing::warn!(
                    %level,
                    cooldown_secs,
                    "HalfOpen probe failed, returning to Open"
                );
                self.state = BreakerState::Open {
                    level,
                    reason: "probe trade failed in HalfOpen".into(),
                    tripped_at: now,
                    cooldown_until: now
                        + chrono::Duration::seconds(
                            ToPrimitive::to_i64(&cooldown_secs).unwrap_or(i64::MAX),
                        ),
                };
                self.last_transition_at = now;
            }
        }
    }

    /// Manual operator intervention — force back to Closed.
    pub fn reset(&mut self, operator_reason: &str) {
        tracing::warn!(
            reason = operator_reason,
            previous_state = ?self.state.to_name(),
            "circuit breaker manually reset to Closed"
        );
        self.state = BreakerState::Closed;
        self.l2_trip_count = 0;
        self.last_transition_at = self.clock.now();
    }

    #[must_use]
    pub const fn l2_trip_count(&self) -> u32 {
        self.l2_trip_count
    }

    #[must_use]
    pub const fn heartbeat_failures(&self) -> u32 {
        self.heartbeat_failures
    }

    /// Record a heartbeat probe failure. Returns `true` if L4 halt was triggered.
    pub fn on_heartbeat_failure(&mut self, max_failures: u32) -> bool {
        self.heartbeat_failures = self.heartbeat_failures.saturating_add(1);
        if self.heartbeat_failures >= max_failures {
            let reason = format!("{} consecutive heartbeat failures", self.heartbeat_failures);
            self.halt(CircuitBreakerLevel::System, reason);
            true
        } else {
            false
        }
    }

    /// Reset the heartbeat failure counter after a successful probe.
    pub const fn on_heartbeat_success(&mut self) {
        self.heartbeat_failures = 0;
    }

    #[must_use]
    pub const fn last_transition_at(&self) -> DateTime<Utc> {
        self.last_transition_at
    }

    /// Compute cooldown duration (seconds) for a given level.
    ///
    /// L2 uses exponential back-off:
    ///   `cooldown = min(l2_cooldown × 2^(trip_count - 1), max_cooldown)`
    fn cooldown_for_level(&self, level: CircuitBreakerLevel) -> u64 {
        match level {
            CircuitBreakerLevel::Trade => self.config.l1_cooldown_secs,
            CircuitBreakerLevel::Session => {
                let base = self.config.l2_cooldown_secs;
                let exponent = self.l2_trip_count.saturating_sub(1);
                let multiplied = base.saturating_mul(2_u64.saturating_pow(exponent));
                multiplied.min(self.config.max_cooldown_secs)
            }
            CircuitBreakerLevel::Daily => self.config.l3_cooldown_secs,
            CircuitBreakerLevel::System => self.config.l4_cooldown_secs,
        }
    }
}
