//! Runtime-mode transition preflight report DTOs.
//!
//! A [`PreflightReport`] is the read-only aggregate produced before an *upgrade*
//! mode transition (`report_only -> semi_auto`, `semi_auto -> auto_execution`).
//! It is fail-closed: the transition is only allowed when every **hard** check
//! passes. Soft (informational) checks never block but are surfaced for audit.

use serde::{Deserialize, Serialize};

use crate::enums::quant::QuantRuntimeMode;

/// One preflight check outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightCheck {
    /// Stable check identifier (e.g. `"credentials_loaded"`).
    pub name: String,
    /// Whether failing this check blocks the transition.
    pub hard: bool,
    /// Whether the check passed.
    pub passed: bool,
    /// Human-readable evidence for operators and the audit trail.
    pub detail: String,
}

impl PreflightCheck {
    /// A hard (blocking) check.
    #[must_use]
    pub fn hard(name: &'static str, passed: bool, detail: impl Into<String>) -> Self {
        Self {
            name: name.to_owned(),
            hard: true,
            passed,
            detail: detail.into(),
        }
    }

    /// A soft (informational, non-blocking) check.
    #[must_use]
    pub fn soft(name: &'static str, passed: bool, detail: impl Into<String>) -> Self {
        Self {
            name: name.to_owned(),
            hard: false,
            passed,
            detail: detail.into(),
        }
    }
}

/// Aggregate preflight outcome for a target mode upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreflightReport {
    pub target: QuantRuntimeMode,
    pub checks: Vec<PreflightCheck>,
    /// `true` when every hard check passed (the transition may proceed).
    pub passed: bool,
}

impl PreflightReport {
    /// Build a report and derive `passed` from the hard checks.
    #[must_use]
    pub fn new(target: QuantRuntimeMode, checks: Vec<PreflightCheck>) -> Self {
        let passed = checks
            .iter()
            .filter(|check| check.hard)
            .all(|check| check.passed);
        Self {
            target,
            checks,
            passed,
        }
    }

    /// Comma-separated `name: detail` summary of the failed hard checks, used as
    /// the `ExecutionError::ModePreflightDenied` reason.
    #[must_use]
    pub fn summary(&self) -> String {
        let failed: Vec<String> = self
            .checks
            .iter()
            .filter(|check| check.hard && !check.passed)
            .map(|check| format!("{}: {}", check.name, check.detail))
            .collect();
        if failed.is_empty() {
            "all preflight checks passed".to_owned()
        } else {
            failed.join("; ")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PreflightCheck, PreflightReport};
    use crate::enums::quant::QuantRuntimeMode;

    #[test]
    fn passed_requires_all_hard_checks() {
        let report = PreflightReport::new(
            QuantRuntimeMode::SemiAuto,
            vec![
                PreflightCheck::hard("a", true, "ok"),
                PreflightCheck::hard("b", true, "ok"),
            ],
        );
        assert!(report.passed);
    }

    #[test]
    fn soft_failure_does_not_block() {
        let report = PreflightReport::new(
            QuantRuntimeMode::SemiAuto,
            vec![
                PreflightCheck::hard("a", true, "ok"),
                PreflightCheck::soft("b", false, "informational"),
            ],
        );
        assert!(report.passed, "soft failures must not block the transition");
    }

    #[test]
    fn hard_failure_blocks_and_summarizes() {
        let report = PreflightReport::new(
            QuantRuntimeMode::AutoExecution,
            vec![
                PreflightCheck::hard("kill_switch_closed", false, "state = execution_halted"),
                PreflightCheck::soft("exit_monitor_healthy", true, "deferred"),
            ],
        );
        assert!(!report.passed);
        let summary = report.summary();
        assert!(summary.contains("kill_switch_closed"));
        assert!(!summary.contains("exit_monitor_healthy"));
    }
}
