use std::sync::Arc;

use oxide_arb_error::trading::TradingError;
use oxide_arb_models::enums::execution::ExecState;

use crate::observability::metrics_hub::MetricsHub;

pub struct ExecutionFSM {
    state: parking_lot::RwLock<ExecState>,
    metrics: Arc<MetricsHub>,
}

impl ExecutionFSM {
    pub const fn new(metrics: Arc<MetricsHub>) -> Self {
        Self {
            state: parking_lot::RwLock::new(ExecState::Idle),
            metrics,
        }
    }

    /// Attempt state transition. Returns Err on invalid transition (never panics).
    pub fn transition(&self, target: ExecState) -> Result<(), TradingError> {
        let mut state = self.state.write();
        let current = *state;

        if Self::is_valid_transition(current, target) {
            tracing::debug!(from = %current, to = %target, "FSM transition");
            *state = target;
            drop(state);
            self.metrics
                .fsm_transitions
                .with_label_values(&[current.as_str(), target.as_str()])
                .inc();
            Ok(())
        } else {
            drop(state);
            tracing::error!(from = %current, to = %target, "invalid FSM transition attempted");
            self.metrics.fsm_invalid_transitions.inc();
            Err(TradingError::InvalidStateTransition {
                from: current.as_str(),
                to: target.as_str(),
            })
        }
    }

    /// Force into Emergency state from any state.
    pub fn enter_emergency(&self, reason: &str) {
        let mut state = self.state.write();
        let prev = *state;
        *state = ExecState::Emergency;
        drop(state);
        tracing::error!(from = %prev, reason = reason, "FSM forced to Emergency");
        self.metrics.fsm_emergency_entries.inc();
    }

    pub fn current(&self) -> ExecState {
        *self.state.read()
    }

    pub fn is_idle(&self) -> bool {
        self.current() == ExecState::Idle
    }

    const fn is_valid_transition(from: ExecState, to: ExecState) -> bool {
        matches!(
            (from, to),
            (ExecState::Idle, ExecState::Validate)
                | (ExecState::Validate, ExecState::Exec | ExecState::Idle)
                | (ExecState::Exec | ExecState::Emergency, ExecState::Idle)
        )
    }
}
