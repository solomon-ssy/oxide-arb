//! Canonical verification of persisted decision-policy snapshot preimages.

use quant_pivot_error::{QuantError, research::ResearchError};
use quant_pivot_models::{
    domain::governance::DecisionPolicySnapshotInfo,
    types::{DecisionPolicySnapshotId, model_serving::ModelServingPolicySnapshotBinding},
};

/// A policy-snapshot binding derived only after its complete persisted
/// preimage, content address, profile artifacts, and relational revision
/// projections have been revalidated.
pub struct VerifiedPolicySnapshotBinding(ModelServingPolicySnapshotBinding);

impl VerifiedPolicySnapshotBinding {
    #[must_use]
    pub(crate) const fn binding(&self) -> &ModelServingPolicySnapshotBinding {
        &self.0
    }
}

impl From<VerifiedPolicySnapshotBinding> for ModelServingPolicySnapshotBinding {
    fn from(verified: VerifiedPolicySnapshotBinding) -> Self {
        verified.0
    }
}

impl TryFrom<&DecisionPolicySnapshotInfo> for VerifiedPolicySnapshotBinding {
    type Error = QuantError;

    fn try_from(info: &DecisionPolicySnapshotInfo) -> Result<Self, Self::Error> {
        let recomputed_hash = info.snapshot.persistence_hash().map_err(|error| {
            ResearchError::InvalidModelArtifact {
                detail: format!("policy snapshot persistence hash failed: {error}"),
            }
        })?;
        let recomputed_id = DecisionPolicySnapshotId::from_content_hash(&recomputed_hash);
        if recomputed_hash != info.snapshot_hash
            || recomputed_id != info.decision_policy_snapshot_id
        {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "policy snapshot identity mismatch: row id={} hash={}, recomputed id={} \
                     hash={recomputed_hash}",
                    info.decision_policy_snapshot_id, info.snapshot_hash, recomputed_id,
                ),
            }
            .into());
        }
        let revisions = &info.snapshot.revisions;
        let revisions_match = revisions.recommendation_policy.as_ref()
            == Some(&info.recommendation_policy_revision_id)
            && revisions.execution_risk_policy.as_ref()
                == Some(&info.execution_risk_policy_revision_id)
            && revisions.model_routing.as_ref() == Some(&info.model_routing_revision_id)
            && revisions.report_schedule.as_ref() == Some(&info.report_schedule_revision_id)
            && revisions.operations_policy.as_ref() == Some(&info.operations_policy_revision_id)
            && revisions.execution_automation_policy.as_ref()
                == Some(&info.execution_automation_policy_revision_id);
        if !revisions_match {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "policy snapshot revision projections differ from the persisted row"
                    .to_owned(),
            }
            .into());
        }
        let profile_artifacts = info
            .snapshot
            .profile_artifacts
            .references()
            .map_err(|error| ResearchError::InvalidModelArtifact {
                detail: format!("policy profile preimage verification failed: {error}"),
            })?;
        Ok(Self(ModelServingPolicySnapshotBinding {
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            snapshot_hash: info.snapshot_hash,
            profile_artifacts,
        }))
    }
}
