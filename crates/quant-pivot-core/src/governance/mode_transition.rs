//! Runtime-mode transition matrix gate.
//!
//! The gate is the first fail-closed door on a mode switch: it admits only the
//! edges defined by the governance spec and rejects every other transition with
//! [`ExecutionError::ModeTransitionForbidden`]. Upgrades additionally require the
//! [`ModePreflight`](crate::governance::ModePreflight) to pass; downgrades
//! (tightening) are always allowed and skip business preflight.
//!
//! Allowed edges (everything else forbidden):
//!
//! ```text
//! report_only   -> semi_auto         (upgrade, preflight)
//! semi_auto     -> report_only       (downgrade)
//! semi_auto     -> auto_execution    (upgrade, preflight)
//! auto_execution-> semi_auto         (downgrade)
//! auto_execution-> report_only       (downgrade)
//! ```
//!
//! `report_only -> auto_execution` is forbidden: an operator must pass through a
//! `semi_auto` shadow period before enabling unattended execution.

use quant_pivot_error::execution::ExecutionError;
use quant_pivot_models::enums::quant::QuantRuntimeMode;

/// Runtime-mode transition matrix.
pub trait ModeTransitionGate: Send + Sync {
    /// Returns `Ok(())` for an allowed edge, `ModeTransitionForbidden` otherwise.
    ///
    /// `from == to` is treated as allowed (the caller short-circuits it as a
    /// no-op before invoking the gate).
    fn check(&self, from: QuantRuntimeMode, to: QuantRuntimeMode) -> Result<(), ExecutionError>;
}

/// Spec transition matrix implementation.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultModeTransitionGate;

impl DefaultModeTransitionGate {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Pure matrix predicate (also used in tests).
    #[must_use]
    pub const fn is_allowed(from: QuantRuntimeMode, to: QuantRuntimeMode) -> bool {
        // Tightening to report_only/semi_auto is always allowed (incl. no-ops);
        // auto_execution is reachable only from semi_auto or itself.
        matches!(
            (from, to),
            (_, QuantRuntimeMode::ReportOnly | QuantRuntimeMode::SemiAuto)
                | (
                    QuantRuntimeMode::SemiAuto | QuantRuntimeMode::AutoExecution,
                    QuantRuntimeMode::AutoExecution
                )
        )
    }
}

impl ModeTransitionGate for DefaultModeTransitionGate {
    fn check(&self, from: QuantRuntimeMode, to: QuantRuntimeMode) -> Result<(), ExecutionError> {
        if Self::is_allowed(from, to) {
            Ok(())
        } else {
            Err(ExecutionError::ModeTransitionForbidden {
                reason: format!(
                    "{} -> {} is not a permitted transition (report_only must reach \
                     auto_execution via semi_auto)",
                    from.as_str(),
                    to.as_str()
                ),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DefaultModeTransitionGate, ModeTransitionGate};
    use quant_pivot_models::enums::quant::QuantRuntimeMode::{AutoExecution, ReportOnly, SemiAuto};

    #[test]
    fn matrix_allows_only_spec_edges() {
        let gate = DefaultModeTransitionGate::new();
        // Allowed upgrades.
        assert!(gate.check(ReportOnly, SemiAuto).is_ok());
        assert!(gate.check(SemiAuto, AutoExecution).is_ok());
        // Allowed downgrades.
        assert!(gate.check(SemiAuto, ReportOnly).is_ok());
        assert!(gate.check(AutoExecution, SemiAuto).is_ok());
        assert!(gate.check(AutoExecution, ReportOnly).is_ok());
        // Forbidden: report_only must pass through semi_auto first.
        assert!(gate.check(ReportOnly, AutoExecution).is_err());
    }

    #[test]
    fn same_mode_is_allowed_by_matrix() {
        let gate = DefaultModeTransitionGate::new();
        assert!(gate.check(ReportOnly, ReportOnly).is_ok());
        assert!(gate.check(SemiAuto, SemiAuto).is_ok());
        assert!(gate.check(AutoExecution, AutoExecution).is_ok());
    }

    #[test]
    fn upgrade_classification() {
        assert!(ReportOnly.is_upgrade_to(SemiAuto));
        assert!(SemiAuto.is_upgrade_to(AutoExecution));
        assert!(ReportOnly.is_upgrade_to(AutoExecution));
        assert!(!SemiAuto.is_upgrade_to(ReportOnly));
        assert!(!AutoExecution.is_upgrade_to(SemiAuto));
        assert!(!ReportOnly.is_upgrade_to(ReportOnly));
    }
}
