//! Health probe API contract.

use serde::Serialize;

use crate::runtime_config::PolicyApplyReadiness;

/// Per-dependency readiness probe result.
#[derive(Debug, Clone, Serialize)]
pub struct DependencyCheck {
    pub name: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Liveness probe payload (`GET /health`).
#[derive(Debug, Serialize)]
pub struct HealthStatus {
    /// Always `"ok"` when the process is up.
    pub status: &'static str,
}

/// Readiness probe payload (`GET /ready`).
#[derive(Debug, Serialize)]
pub struct ReadinessStatus {
    /// `"ready"` when all required dependencies are reachable; `"not_ready"` otherwise.
    pub status: &'static str,
    /// Per-dependency probes (`PostgreSQL`, Redis, and committed policy generation).
    pub checks: Vec<DependencyCheck>,
}

/// Internal readiness report returned by the runtime-control readiness port.
#[derive(Debug, Clone)]
pub struct ReadinessReport {
    pub ready: bool,
    pub checks: Vec<DependencyCheck>,
}

impl From<PolicyApplyReadiness> for DependencyCheck {
    fn from(readiness: PolicyApplyReadiness) -> Self {
        Self {
            name: "policy_generation".to_owned(),
            ok: readiness.is_ready(),
            detail: Some(readiness.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        runtime_config::{PolicyApplyDegradedCause, PolicyApplyReadiness, PolicyBundleIdentity},
        types::{ContentHash, DecisionPolicySnapshotId, PolicyBundleGeneration},
    };

    use super::DependencyCheck;

    fn identity(generation: i64, hash_byte: u8) -> PolicyBundleIdentity {
        let snapshot_hash = ContentHash::from_bytes([hash_byte; 32]);
        PolicyBundleIdentity {
            generation: PolicyBundleGeneration::try_new(generation)
                .expect("positive test generation"),
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_content_hash(
                &snapshot_hash,
            ),
            snapshot_hash,
        }
    }

    #[test]
    fn policy_generation_check() {
        let applied = identity(1, 1);
        let ready = DependencyCheck::from(PolicyApplyReadiness::Ready { applied });
        assert!(ready.ok);
        assert_eq!(ready.name, "policy_generation");
        assert!(
            ready
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("ready generation=1"))
        );

        let desired = identity(2, 2);
        let degraded = DependencyCheck::from(PolicyApplyReadiness::Degraded {
            desired,
            applied,
            cause: PolicyApplyDegradedCause::PrepareFailed,
        });
        assert!(!degraded.ok);
        assert!(
            degraded
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("prepare_failed"))
        );
        assert!(
            degraded
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("desired_generation=2"))
        );
    }
}
