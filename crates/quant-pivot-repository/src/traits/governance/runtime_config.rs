use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::governance::{
        ActivePolicyResourceInfo, ConfigActivityInfo, ConfigResourceInventoryInfo,
        DecisionPolicySnapshotInfo, DecisionPolicySnapshotOptionInfo, NewDecisionPolicySnapshot,
        NewPolicyActivation, NewPolicyRevision, PolicyActivationCommit, PolicyActivationInfo,
        PolicyApprovalInfo, PolicyRevisionInfo, RecordPolicyApproval,
    },
    enums::runtime_config::ConfigResourceKind,
    runtime_config::{
        ActivePolicyBundle, ImmutableProfileArtifactReferences, ImmutableProfileArtifacts,
        PolicyValidationEvidence,
    },
    types::{ContentHash, DecisionPolicySnapshotId, PolicyApprovalId, PolicyRevisionId},
};

#[async_trait::async_trait]
pub trait PolicyRepository: Send + Sync {
    /// Persist or verify the four content-addressed WORM profile artifacts.
    async fn ensure_policy_profile_artifacts(
        &self,
        _artifacts: &ImmutableProfileArtifacts,
        _actor_label: &str,
        _reason: &str,
    ) -> Result<ImmutableProfileArtifactReferences, StorageError> {
        Err(StorageError::invariant_violation(
            Some("policy_profile_artifact"),
            "repository does not implement policy profile artifact persistence",
        ))
    }

    async fn create_revision(
        &self,
        revision: NewPolicyRevision,
    ) -> Result<PolicyRevisionInfo, StorageError>;

    async fn mark_revision_validated(
        &self,
        revision_id: &PolicyRevisionId,
        validation_evidence: PolicyValidationEvidence,
        preflight_token_hash: ContentHash,
        preflight_expires_at: DateTime<Utc>,
    ) -> Result<PolicyRevisionInfo, StorageError>;

    async fn load_revision(
        &self,
        revision_id: &PolicyRevisionId,
    ) -> Result<Option<PolicyRevisionInfo>, StorageError>;

    async fn list_revisions(
        &self,
        kind: ConfigResourceKind,
        limit: u64,
    ) -> Result<Vec<PolicyRevisionInfo>, StorageError>;

    /// Load the cross-resource activity feed in one persistence query.
    async fn list_all_revisions(&self, limit: u64)
    -> Result<Vec<PolicyRevisionInfo>, StorageError>;

    /// Load the globally ordered cross-resource activity feed in one statement.
    async fn list_activity(&self, limit: u64) -> Result<Vec<ConfigActivityInfo>, StorageError>;

    /// Load the DB-authoritative Config inventory in one statement.
    async fn load_resource_inventory(&self) -> Result<ConfigResourceInventoryInfo, StorageError>;

    async fn record_approval(
        &self,
        approval: RecordPolicyApproval,
    ) -> Result<PolicyApprovalInfo, StorageError>;

    async fn load_approval(
        &self,
        approval_id: &PolicyApprovalId,
    ) -> Result<Option<PolicyApprovalInfo>, StorageError>;

    async fn list_valid_approvals(
        &self,
        kind: Option<ConfigResourceKind>,
        limit: u64,
    ) -> Result<Vec<PolicyApprovalInfo>, StorageError>;

    async fn list_approvals(
        &self,
        kind: Option<ConfigResourceKind>,
        limit: u64,
    ) -> Result<Vec<PolicyApprovalInfo>, StorageError> {
        self.list_valid_approvals(kind, limit).await
    }

    /// Atomically verify CAS/preflight/approval, persist the resolved bundle,
    /// and append one resource activation.
    async fn activate_resource(
        &self,
        activation: NewPolicyActivation,
        snapshot: NewDecisionPolicySnapshot,
    ) -> Result<PolicyActivationCommit, StorageError>;

    /// Load the singleton activation generation and exact committed snapshot together.
    ///
    /// Snapshot artifacts are content-addressed and can be activated by more
    /// than one generation, so implementations must resolve the generation
    /// from the activation guard or ledger rather than from the artifact row.
    async fn load_current_bundle(&self) -> Result<Option<ActivePolicyBundle>, StorageError>;

    async fn load_current_activation(
        &self,
        kind: Option<ConfigResourceKind>,
    ) -> Result<Option<PolicyActivationInfo>, StorageError>;

    async fn load_current_activations(&self) -> Result<Vec<PolicyActivationInfo>, StorageError> {
        let mut activations = Vec::with_capacity(ConfigResourceKind::ALL.len());
        for kind in ConfigResourceKind::ALL {
            if let Some(activation) = self.load_current_activation(Some(kind)).await? {
                activations.push(activation);
            }
        }
        Ok(activations)
    }

    async fn count_valid_approvals(
        &self,
    ) -> Result<BTreeMap<ConfigResourceKind, u64>, StorageError> {
        let mut counts = BTreeMap::new();
        for approval in self.list_valid_approvals(None, 10_000).await? {
            *counts.entry(approval.resource_kind).or_insert(0) += 1;
        }
        Ok(counts)
    }

    async fn load_current_resource(
        &self,
        kind: ConfigResourceKind,
    ) -> Result<Option<ActivePolicyResourceInfo>, StorageError> {
        let Some(activation) = self.load_current_activation(Some(kind)).await? else {
            return Ok(None);
        };
        let revision = self
            .load_revision(&activation.policy_revision_id)
            .await?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("policy_activation"),
                    format!(
                        "activation {} references a missing policy revision",
                        activation.policy_activation_id
                    ),
                )
            })?;
        Ok(Some(ActivePolicyResourceInfo {
            activation,
            revision,
        }))
    }

    async fn load_current_revision(
        &self,
        kind: ConfigResourceKind,
    ) -> Result<Option<PolicyRevisionInfo>, StorageError>;

    async fn load_snapshot(
        &self,
        snapshot_id: &DecisionPolicySnapshotId,
    ) -> Result<Option<DecisionPolicySnapshotInfo>, StorageError>;

    async fn load_current(&self) -> Result<Option<DecisionPolicySnapshotInfo>, StorageError>;

    async fn load_active_at(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Option<DecisionPolicySnapshotInfo>, StorageError>;

    async fn list_snapshots(
        &self,
        limit: u64,
    ) -> Result<Vec<DecisionPolicySnapshotInfo>, StorageError>;

    /// Load immutable snapshot identities in one statement without artifact documents.
    async fn list_snapshot_options(
        &self,
        limit: u64,
    ) -> Result<Vec<DecisionPolicySnapshotOptionInfo>, StorageError>;

    async fn list_activations(
        &self,
        kind: Option<ConfigResourceKind>,
        limit: u64,
    ) -> Result<Vec<PolicyActivationInfo>, StorageError>;
}
