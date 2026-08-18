//! Entry-authorization transition preflight evidence.
//!
//! An [`AuthorizationPreflightReport`] is the read-only aggregate produced
//! before enabling policy-automatic authorization.
//! It is fail-closed: the transition is only allowed when every **hard** check
//! passes. Soft (informational) checks never block but are surfaced for audit.

use serde::{Deserialize, Serialize};

use crate::enums::quant::EntryAuthorizationPolicy;

/// One preflight check outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationPreflightCheck {
    /// Stable check identifier (e.g. `"credentials_loaded"`).
    pub name: String,
    /// Whether failing this check blocks the transition.
    pub hard: bool,
    /// Whether the check passed.
    pub passed: bool,
    /// Human-readable evidence for operators and the audit trail.
    pub detail: String,
}

impl AuthorizationPreflightCheck {
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
pub struct AuthorizationPreflightReport {
    pub target: EntryAuthorizationPolicy,
    pub checks: Vec<AuthorizationPreflightCheck>,
    /// `true` when every hard check passed (the transition may proceed).
    pub passed: bool,
}

impl AuthorizationPreflightReport {
    /// Build a report and derive `passed` from the hard checks.
    #[must_use]
    pub fn new(target: EntryAuthorizationPolicy, checks: Vec<AuthorizationPreflightCheck>) -> Self {
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
    /// the `ExecutionError::AuthorizationPreflightDenied` reason.
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
    use super::{AuthorizationPreflightCheck, AuthorizationPreflightReport};
    use crate::enums::quant::EntryAuthorizationPolicy;

    #[test]
    fn passed_requires_hard_checks() {
        let report = AuthorizationPreflightReport::new(
            EntryAuthorizationPolicy::OperatorApprovalRequired,
            vec![
                AuthorizationPreflightCheck::hard("a", true, "ok"),
                AuthorizationPreflightCheck::hard("b", true, "ok"),
            ],
        );
        assert!(report.passed);
    }

    #[test]
    fn soft_failure_never_blocks() {
        let report = AuthorizationPreflightReport::new(
            EntryAuthorizationPolicy::OperatorApprovalRequired,
            vec![
                AuthorizationPreflightCheck::hard("a", true, "ok"),
                AuthorizationPreflightCheck::soft("b", false, "informational"),
            ],
        );
        assert!(report.passed, "soft failures must not block the transition");
    }

    #[test]
    fn hard_failure_blocks_summarizes() {
        let report = AuthorizationPreflightReport::new(
            EntryAuthorizationPolicy::PolicyAutomatic,
            vec![
                AuthorizationPreflightCheck::hard(
                    "kill_switch_closed",
                    false,
                    "state = execution_halted",
                ),
                AuthorizationPreflightCheck::soft("exit_monitor_healthy", true, "deferred"),
            ],
        );
        assert!(!report.passed);
        let summary = report.summary();
        assert!(summary.contains("kill_switch_closed"));
        assert!(!summary.contains("exit_monitor_healthy"));
    }
}
