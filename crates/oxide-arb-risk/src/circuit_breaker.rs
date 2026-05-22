//! 4-state circuit breaker FSM with 4 severity levels (L1-L4).
//!
//! ```text
//! Closed ──trip──▶ Open ──cooldown expires──▶ HalfOpen ──probes pass──▶ Recovered
//!   ▲                ▲                           │                         │
//!   │                └───── probe fails ──────────┘                         │
//!   └──────────────── observation period expires ──────────────────────────┘
//! ```
//!
//! Additional edge: `reset()` → any state → Closed (operator intervention).

use crate::types::BreakerState;
use chrono::{DateTime, Utc};
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::config::CircuitBreakerConfig;
use oxide_arb_models::domain::risk::RiskEngineSnapshot;
use oxide_arb_models::enums::risk::{BreakerStateName, CircuitBreakerLevel};

pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: BreakerState,
    l2_trip_count: u32,
    last_transition_at: DateTime<Utc>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker in Closed state.
    #[must_use]
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: BreakerState::Closed,
            l2_trip_count: 0,
            last_transition_at: Utc::now(),
        }
    }

    /// Restore from a persisted snapshot (crash recovery).
    ///
    /// **Fail-closed**: if the snapshot is in `Open` state but missing
    /// `breaker_level`, `breaker_reason`, or `cooling_until`, this returns
    /// an error rather than silently defaulting.
    pub fn from_snapshot(
        config: CircuitBreakerConfig,
        snapshot: &RiskEngineSnapshot,
    ) -> OxideResult<Self> {
        let state = match snapshot.breaker_state {
            BreakerStateName::Closed => BreakerState::Closed,
            BreakerStateName::Open => {
                let level = snapshot.breaker_level.ok_or_else(|| {
                    OxideError::Internal(
                        "corrupt breaker snapshot: Open state missing breaker_level".into(),
                    )
                })?;
                let reason = snapshot.breaker_reason.clone().ok_or_else(|| {
                    OxideError::Internal(
                        "corrupt breaker snapshot: Open state missing breaker_reason".into(),
                    )
                })?;
                let cooldown_until = snapshot.cooling_until.ok_or_else(|| {
                    OxideError::Internal(
                        "corrupt breaker snapshot: Open state missing cooling_until".into(),
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
                        i64::try_from(config.recovery_observation_secs).unwrap_or(i64::MAX),
                    ),
            },
        };

        Ok(Self {
            config,
            state,
            l2_trip_count: snapshot.l2_trip_count,
            last_transition_at: snapshot.snapshot_at,
        })
    }

    #[must_use]
    pub const fn state(&self) -> &BreakerState {
        &self.state
    }

    #[must_use]
    pub const fn allows_trading(&self) -> bool {
        self.state.allows_trading()
    }

    #[must_use]
    pub const fn is_probe_mode(&self) -> bool {
        self.state.is_probe_mode()
    }

    /// Trip the breaker to Open state at the given level.
    ///
    /// Higher level overrides lower. Same or higher level refreshes cooldown.
    pub fn trip(&mut self, level: CircuitBreakerLevel, reason: String) {
        let now = Utc::now();
        let cooldown_secs = self.cooldown_for_level(level);
        let cooldown_until =
            now + chrono::Duration::seconds(i64::try_from(cooldown_secs).unwrap_or(i64::MAX));

        if level == CircuitBreakerLevel::Session {
            self.l2_trip_count += 1;
        }

        let should_trip = match &self.state {
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

    /// Periodic tick — drives time-based state transitions.
    ///
    /// Returns `true` if a state transition occurred.
    #[must_use]
    pub fn tick(&mut self) -> bool {
        let now = Utc::now();
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
        let now = Utc::now();
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
                            i64::try_from(self.config.recovery_observation_secs)
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
                            i64::try_from(cooldown_secs).unwrap_or(i64::MAX),
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
        self.last_transition_at = Utc::now();
    }

    #[must_use]
    pub const fn l2_trip_count(&self) -> u32 {
        self.l2_trip_count
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
