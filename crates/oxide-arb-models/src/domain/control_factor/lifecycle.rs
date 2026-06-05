//! Factor registry status machine frozen by Phase 5.0.

use crate::enums::control_factor::FactorStatus;
use oxide_arb_error::control::FactorValueError;

/// Allowed `FactorStatus` transitions for the control-factor registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FactorLifecycle;

/// Directed edges of the factor lifecycle graph (excluding self-transitions).
const TRANSITIONS: &[(FactorStatus, FactorStatus)] = &[
    // Materialization / quality-gate outputs
    (FactorStatus::Draft, FactorStatus::Candidate),
    (FactorStatus::Draft, FactorStatus::Rejected),
    (FactorStatus::Draft, FactorStatus::ReportOnly),
    (FactorStatus::ReportOnly, FactorStatus::Draft),
    (FactorStatus::ReportOnly, FactorStatus::Rejected),
    // Promotion funnel
    (FactorStatus::Candidate, FactorStatus::Shadow),
    (FactorStatus::Candidate, FactorStatus::Rejected),
    (FactorStatus::Shadow, FactorStatus::Published),
    (FactorStatus::Shadow, FactorStatus::Rejected),
    // Publication supersession and rollback
    (FactorStatus::Published, FactorStatus::Superseded),
    (FactorStatus::Published, FactorStatus::Expired),
    (FactorStatus::Published, FactorStatus::RolledBack),
    (FactorStatus::Superseded, FactorStatus::RolledBack),
    // TTL expiry sweeps
    (FactorStatus::Candidate, FactorStatus::Expired),
    (FactorStatus::Shadow, FactorStatus::Expired),
    (FactorStatus::ReportOnly, FactorStatus::Expired),
];

impl FactorLifecycle {
    /// Returns whether `from -> to` is an allowed registry transition.
    #[must_use]
    pub fn can_transition(from: FactorStatus, to: FactorStatus) -> bool {
        from == to || TRANSITIONS.contains(&(from, to))
    }

    /// Statuses a materialization run may persist directly.
    ///
    /// Quality gates run inside the materialization run, so a completed run may
    /// emit `Candidate` (gates passed) alongside `Draft` (gates-off), `Rejected`
    /// (gates failed), and `ReportOnly` (insufficient evidence). Governed
    /// statuses driven by publication/expiry (`Shadow` / `Published` /
    /// `Superseded` / `Expired` / `RolledBack`) must never be written by a run.
    #[must_use]
    pub const fn is_materialization_output(status: FactorStatus) -> bool {
        matches!(
            status,
            FactorStatus::Draft
                | FactorStatus::Candidate
                | FactorStatus::Rejected
                | FactorStatus::ReportOnly
        )
    }

    /// Targets that require full evidence and payload safety validation.
    #[must_use]
    pub const fn requires_governed_evidence(target: FactorStatus) -> bool {
        matches!(
            target,
            FactorStatus::Candidate | FactorStatus::Shadow | FactorStatus::Published
        )
    }

    /// Asserts `from -> to` is legal, returning a typed error otherwise.
    pub fn assert_transition(from: FactorStatus, to: FactorStatus) -> Result<(), FactorValueError> {
        if Self::can_transition(from, to) {
            return Ok(());
        }
        Err(FactorValueError::IllegalTransition {
            from: from.to_string(),
            to: to.to_string(),
        })
    }

    /// Report-only rows are visible for audit but must never enter the promotion funnel.
    pub fn assert_not_report_only_promotion(
        from: FactorStatus,
        target: FactorStatus,
    ) -> Result<(), FactorValueError> {
        if matches!(
            (from, target),
            (
                FactorStatus::ReportOnly,
                FactorStatus::Candidate | FactorStatus::Shadow | FactorStatus::Published
            )
        ) {
            return Err(FactorValueError::ReportOnlyPromotionForbidden {
                target: target.to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{FactorLifecycle, TRANSITIONS};
    use crate::enums::control_factor::FactorStatus;

    #[test]
    fn transition_table_has_no_duplicate_edges() {
        for (index, edge) in TRANSITIONS.iter().enumerate() {
            assert!(
                !TRANSITIONS[..index].contains(edge),
                "duplicate edge: {:?} -> {:?}",
                edge.0,
                edge.1
            );
        }
    }

    #[test]
    fn report_only_cannot_promote_to_candidate() {
        assert!(!FactorLifecycle::can_transition(
            FactorStatus::ReportOnly,
            FactorStatus::Candidate
        ));
    }

    #[test]
    fn draft_can_enter_report_only() {
        assert!(FactorLifecycle::can_transition(
            FactorStatus::Draft,
            FactorStatus::ReportOnly
        ));
    }

    #[test]
    fn shadow_only_advances_to_published() {
        assert!(FactorLifecycle::can_transition(
            FactorStatus::Shadow,
            FactorStatus::Published
        ));
        assert!(!FactorLifecycle::can_transition(
            FactorStatus::Candidate,
            FactorStatus::Published
        ));
    }

    #[test]
    fn every_declared_edge_is_accepted() {
        for &(from, to) in TRANSITIONS {
            assert!(
                FactorLifecycle::can_transition(from, to),
                "missing edge: {from} -> {to}"
            );
        }
    }
}
