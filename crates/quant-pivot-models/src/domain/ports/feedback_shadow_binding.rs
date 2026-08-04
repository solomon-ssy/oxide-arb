//! Route-owned shadow-binding job, receipt, and artifact contracts.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, feedback::FeedbackError};
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    domain::quant::{JobProgressSink, ResearchJobArtifactRef},
    enums::quant::ShadowBindingStatus,
    hashing::CanonicalDigest,
    runtime_config::BuyModelRoute,
    types::{
        AuditEventId, ContentHash, DecisionPolicySnapshotId, FeedbackCycleId,
        ModelCandidateManifestId, ModelVersionId, PolicyActivationId, PolicyBundleGeneration,
        PolicyIdempotencyKey, PolicyRevisionId, ResearchJobId, ResearchProfileRef, RoleCode,
        ShadowBindingArtifactId, TrainingDatasetId, UserId,
    },
};

use super::feedback_execution::FeedbackComparisonArtifactRef;

const SHADOW_BINDING_INPUT_DOMAIN: &str = "quant-pivot/shadow-binding-input";
const SHADOW_BINDING_RECEIPT_DOMAIN: &str = "quant-pivot/shadow-binding-receipt";
const SHADOW_BINDING_ARTIFACT_DOMAIN: &str = "quant-pivot/shadow-binding-artifact";
const SHADOW_BINDING_VERSION: u32 = 1;
const SHADOW_REJECTION_REQUEST_DOMAIN: &str = "quant-pivot/shadow-binding-rejection-request";
const SHADOW_REJECTION_REQUEST_VERSION: u32 = 1;
const SHADOW_CANCELLATION_REQUEST_DOMAIN: &str = "quant-pivot/shadow-binding-cancellation-request";
const SHADOW_CANCELLATION_REQUEST_VERSION: u32 = 1;
const MAX_REJECTION_NOTE_BYTES: usize = 2_048;
const MAX_REJECTION_REASON_BYTES: usize = 128;
const MAX_REJECTION_ROLE_BYTES: usize = 64;

/// Coordinator-owned, exact-CAS command for releasing the route shadow of a
/// cancelled feedback cycle.
#[derive(Debug, Clone)]
pub struct CancelShadowBinding {
    pub binding_id: ShadowBindingArtifactId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub expected_lifecycle_generation: u64,
    pub expected_binding_generation: u64,
    pub expected_policy_generation: PolicyBundleGeneration,
    pub idempotency_key: PolicyIdempotencyKey,
    pub reason_code: String,
    pub note: String,
}

impl CancelShadowBinding {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        let reason_valid = !self.reason_code.is_empty()
            && self.reason_code.len() <= MAX_REJECTION_REASON_BYTES
            && self
                .reason_code
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        if self.binding_id != ShadowBindingArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.expected_binding_generation == 0
            || !reason_valid
            || self.note.trim().is_empty()
            || self.note.len() > MAX_REJECTION_NOTE_BYTES
        {
            return Err(FeedbackError::ShadowBindingConflict {
                detail: "shadow cancellation identity, generation, reason, or note is invalid"
                    .to_owned(),
            });
        }
        Ok(())
    }

    pub fn request_hash(&self) -> Result<ContentHash, FeedbackError> {
        self.validate()?;
        CanonicalDigest::content_hash_typed(
            SHADOW_CANCELLATION_REQUEST_DOMAIN,
            SHADOW_CANCELLATION_REQUEST_VERSION,
            &(
                self.binding_id,
                self.feedback_cycle_id,
                self.expected_lifecycle_generation,
                self.expected_binding_generation,
                self.expected_policy_generation,
                &self.idempotency_key,
                &self.reason_code,
                &self.note,
            ),
        )
        .map_err(Into::into)
    }
}

/// Immutable projection of the system activation that released one exact
/// route shadow after cycle cancellation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowBindingCancellationReceipt {
    pub binding_id: ShadowBindingArtifactId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub route: BuyModelRoute,
    pub champion_model_version_id: ModelVersionId,
    pub cancelled_model_version_id: ModelVersionId,
    pub previous_lifecycle_generation: u64,
    pub committed_lifecycle_generation: u64,
    pub previous_binding_generation: u64,
    pub cleared_route_generation: u64,
    pub previous_policy_generation: PolicyBundleGeneration,
    pub committed_policy_generation: PolicyBundleGeneration,
    pub previous_model_routing_revision_id: PolicyRevisionId,
    pub committed_model_routing_revision_id: PolicyRevisionId,
    pub committed_snapshot_id: DecisionPolicySnapshotId,
    pub committed_snapshot_hash: ContentHash,
    pub policy_activation_id: PolicyActivationId,
    pub audit_event_id: AuditEventId,
    pub request_hash: ContentHash,
    pub idempotency_key: PolicyIdempotencyKey,
    pub reason_code: String,
    pub note: String,
    pub cancelled_by_label: String,
    pub cancelled_by_role: RoleCode,
    pub cancelled_at: DateTime<Utc>,
}

/// Authenticated, exact-CAS command for releasing one `CandidateReady` route
/// shadow without mutating the immutable feedback cycle.
#[derive(Debug, Clone)]
pub struct RejectShadowBinding {
    pub binding_id: ShadowBindingArtifactId,
    pub expected_binding_generation: u64,
    pub expected_policy_generation: PolicyBundleGeneration,
    pub idempotency_key: PolicyIdempotencyKey,
    pub reason_code: String,
    pub note: String,
    pub actor_user_id: UserId,
    pub actor_role: RoleCode,
}

impl RejectShadowBinding {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        let role = self.actor_role.as_str();
        let reason_valid = !self.reason_code.is_empty()
            && self.reason_code.len() <= MAX_REJECTION_REASON_BYTES
            && self
                .reason_code
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        let role_valid = !role.is_empty()
            && role.len() <= MAX_REJECTION_ROLE_BYTES
            && role
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
        if self.expected_binding_generation == 0
            || !reason_valid
            || !role_valid
            || self.note.trim().is_empty()
            || self.note.len() > MAX_REJECTION_NOTE_BYTES
        {
            return Err(FeedbackError::ShadowBindingConflict {
                detail: "shadow rejection generation, reason, note, or actor role is invalid"
                    .to_owned(),
            });
        }
        Ok(())
    }

    pub fn request_hash(&self) -> Result<ContentHash, FeedbackError> {
        self.validate()?;
        CanonicalDigest::content_hash_typed(
            SHADOW_REJECTION_REQUEST_DOMAIN,
            SHADOW_REJECTION_REQUEST_VERSION,
            &(
                self.binding_id,
                self.expected_binding_generation,
                self.expected_policy_generation,
                &self.idempotency_key,
                &self.reason_code,
                &self.note,
                self.actor_user_id,
                &self.actor_role,
            ),
        )
        .map_err(Into::into)
    }
}

/// Immutable projection of the policy activation that cleared one exact
/// route-owned shadow binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowBindingRejectionReceipt {
    pub binding_id: ShadowBindingArtifactId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub route: BuyModelRoute,
    pub champion_model_version_id: ModelVersionId,
    pub rejected_model_version_id: ModelVersionId,
    pub previous_binding_generation: u64,
    pub cleared_route_generation: u64,
    pub previous_policy_generation: PolicyBundleGeneration,
    pub committed_policy_generation: PolicyBundleGeneration,
    pub previous_model_routing_revision_id: PolicyRevisionId,
    pub committed_model_routing_revision_id: PolicyRevisionId,
    pub committed_snapshot_id: DecisionPolicySnapshotId,
    pub committed_snapshot_hash: ContentHash,
    pub policy_activation_id: PolicyActivationId,
    pub audit_event_id: AuditEventId,
    pub request_hash: ContentHash,
    pub idempotency_key: PolicyIdempotencyKey,
    pub reason_code: String,
    pub note: String,
    pub rejected_by_user_id: UserId,
    pub rejected_by_username: String,
    pub rejected_by_role: RoleCode,
    pub rejected_at: DateTime<Utc>,
}

/// Current lifecycle projection of one immutable binding identity.
///
/// The sealed binding receipt remains immutable; this projection exposes the
/// exact terminal transition so operator reads never present a rejected or
/// promoted slot as active.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowBindingLifecycle {
    pub binding_id: ShadowBindingArtifactId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub route: BuyModelRoute,
    pub status: ShadowBindingStatus,
    pub lifecycle_generation: u64,
    pub binding_generation: u64,
    pub champion_model_version_id: ModelVersionId,
    pub candidate_model_version_id: ModelVersionId,
    pub committed_policy_generation: PolicyBundleGeneration,
    pub bound_at: DateTime<Utc>,
    pub terminated_at: Option<DateTime<Utc>>,
    pub termination_policy_activation_id: Option<PolicyActivationId>,
    pub termination_reason_code: Option<String>,
}

/// Complete immutable preimage for the coordinator-owned `ShadowBind` job.
pub struct ShadowBindingJobInput {
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub prepared_at: DateTime<Utc>,
    pub profile_ref: ResearchProfileRef,
    pub route: BuyModelRoute,
    pub comparison: FeedbackComparisonArtifactRef,
    pub candidate_recipe_hash: ContentHash,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_model_version_id: ModelVersionId,
    pub candidate_artifact_hash: ContentHash,
    pub candidate_serving_contract_hash: ContentHash,
    pub candidate_manifest_id: ModelCandidateManifestId,
    pub candidate_manifest_hash: ContentHash,
    pub candidate_training_dataset_id: TrainingDatasetId,
    pub expected_policy_generation: PolicyBundleGeneration,
    pub expected_snapshot_id: DecisionPolicySnapshotId,
    pub expected_snapshot_hash: ContentHash,
    pub expected_model_routing_revision_id: PolicyRevisionId,
    pub expected_route_generation: u64,
    pub reserved_model_bytes: u64,
    pub total_shadow_model_budget_bytes: u64,
}

/// Frozen, replayable input for one route-owned shadow CAS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowBindingJobParams {
    pub format_version: u32,
    pub artifact_id: ShadowBindingArtifactId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub prepared_at: DateTime<Utc>,
    pub profile_ref: ResearchProfileRef,
    pub route: BuyModelRoute,
    pub comparison: FeedbackComparisonArtifactRef,
    pub candidate_recipe_hash: ContentHash,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_model_version_id: ModelVersionId,
    pub candidate_artifact_hash: ContentHash,
    pub candidate_serving_contract_hash: ContentHash,
    pub candidate_manifest_id: ModelCandidateManifestId,
    pub candidate_manifest_hash: ContentHash,
    pub candidate_training_dataset_id: TrainingDatasetId,
    pub expected_policy_generation: PolicyBundleGeneration,
    pub expected_snapshot_id: DecisionPolicySnapshotId,
    pub expected_snapshot_hash: ContentHash,
    pub expected_model_routing_revision_id: PolicyRevisionId,
    pub expected_route_generation: u64,
    pub reserved_model_bytes: u64,
    pub total_shadow_model_budget_bytes: u64,
}

impl ShadowBindingJobParams {
    pub fn try_new(input: ShadowBindingJobInput) -> Result<Self, FeedbackError> {
        let params = Self {
            format_version: SHADOW_BINDING_VERSION,
            artifact_id: ShadowBindingArtifactId::from_cycle_id(input.feedback_cycle_id),
            feedback_cycle_id: input.feedback_cycle_id,
            cycle_idempotency_hash: input.cycle_idempotency_hash,
            prepared_at: input.prepared_at,
            profile_ref: input.profile_ref,
            route: input.route,
            comparison: input.comparison,
            candidate_recipe_hash: input.candidate_recipe_hash,
            champion_model_version_id: input.champion_model_version_id,
            champion_serving_contract_hash: input.champion_serving_contract_hash,
            candidate_model_version_id: input.candidate_model_version_id,
            candidate_artifact_hash: input.candidate_artifact_hash,
            candidate_serving_contract_hash: input.candidate_serving_contract_hash,
            candidate_manifest_id: input.candidate_manifest_id,
            candidate_manifest_hash: input.candidate_manifest_hash,
            candidate_training_dataset_id: input.candidate_training_dataset_id,
            expected_policy_generation: input.expected_policy_generation,
            expected_snapshot_id: input.expected_snapshot_id,
            expected_snapshot_hash: input.expected_snapshot_hash,
            expected_model_routing_revision_id: input.expected_model_routing_revision_id,
            expected_route_generation: input.expected_route_generation,
            reserved_model_bytes: input.reserved_model_bytes,
            total_shadow_model_budget_bytes: input.total_shadow_model_budget_bytes,
        };
        params.validate()?;
        Ok(params)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.profile_ref
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        let candidate_is_distinct =
            self.candidate_model_version_id != self.champion_model_version_id;
        let budget_valid = self.reserved_model_bytes > 0
            && self.reserved_model_bytes <= self.total_shadow_model_budget_bytes;
        if self.format_version != SHADOW_BINDING_VERSION
            || self.artifact_id != ShadowBindingArtifactId::from_cycle_id(self.feedback_cycle_id)
            || FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
                != self.feedback_cycle_id
            || self.comparison.feedback_cycle_id != self.feedback_cycle_id
            || self.comparison.candidate_family_hash == ContentHash::from_bytes([0; 32])
            || self.expected_route_generation == 0
            || !candidate_is_distinct
            || !budget_valid
        {
            return Err(invalid(
                "shadow-binding cycle, comparison, route generation, candidate, or budget is invalid",
            ));
        }
        Ok(())
    }

    pub fn input_hash(&self) -> Result<ContentHash, FeedbackError> {
        self.validate()?;
        CanonicalDigest::content_hash_typed(
            SHADOW_BINDING_INPUT_DOMAIN,
            SHADOW_BINDING_VERSION,
            self,
        )
        .map_err(Into::into)
    }
}

/// Fields jointly sealed into the immutable binding receipt.
pub struct ShadowBindingReceiptInput {
    pub params: ShadowBindingJobParams,
    pub bound_at: DateTime<Utc>,
    pub binding_generation: u64,
    pub committed_policy_generation: PolicyBundleGeneration,
    pub committed_snapshot_id: DecisionPolicySnapshotId,
    pub committed_snapshot_hash: ContentHash,
    pub committed_model_routing_revision_id: PolicyRevisionId,
    pub policy_activation_id: PolicyActivationId,
    pub audit_event_id: AuditEventId,
}

/// Durable receipt for one exact route-owned shadow binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ShadowBindingReceipt {
    pub format_version: u32,
    pub binding_id: ShadowBindingArtifactId,
    pub receipt_hash: ContentHash,
    pub job_input_hash: ContentHash,
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub route: BuyModelRoute,
    pub profile_ref: ResearchProfileRef,
    pub candidate_recipe_hash: ContentHash,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_model_version_id: ModelVersionId,
    pub candidate_artifact_hash: ContentHash,
    pub candidate_serving_contract_hash: ContentHash,
    pub candidate_manifest_id: ModelCandidateManifestId,
    pub candidate_manifest_hash: ContentHash,
    pub candidate_training_dataset_id: TrainingDatasetId,
    pub bound_at: DateTime<Utc>,
    pub previous_route_generation: u64,
    pub binding_generation: u64,
    pub reserved_model_bytes: u64,
    pub previous_policy_generation: PolicyBundleGeneration,
    pub previous_snapshot_id: DecisionPolicySnapshotId,
    pub previous_snapshot_hash: ContentHash,
    pub previous_model_routing_revision_id: PolicyRevisionId,
    pub committed_policy_generation: PolicyBundleGeneration,
    pub committed_snapshot_id: DecisionPolicySnapshotId,
    pub committed_snapshot_hash: ContentHash,
    pub committed_model_routing_revision_id: PolicyRevisionId,
    pub policy_activation_id: PolicyActivationId,
    pub audit_event_id: AuditEventId,
}

#[derive(Serialize)]
struct ShadowBindingReceiptPreimage<'a> {
    format_version: u32,
    binding_id: ShadowBindingArtifactId,
    job_input_hash: ContentHash,
    feedback_cycle_id: FeedbackCycleId,
    cycle_idempotency_hash: ContentHash,
    route: BuyModelRoute,
    profile_ref: &'a ResearchProfileRef,
    candidate_recipe_hash: ContentHash,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    candidate_model_version_id: ModelVersionId,
    candidate_artifact_hash: ContentHash,
    candidate_serving_contract_hash: ContentHash,
    candidate_manifest_id: ModelCandidateManifestId,
    candidate_manifest_hash: ContentHash,
    candidate_training_dataset_id: TrainingDatasetId,
    bound_at: DateTime<Utc>,
    previous_route_generation: u64,
    binding_generation: u64,
    reserved_model_bytes: u64,
    previous_policy_generation: PolicyBundleGeneration,
    previous_snapshot_id: DecisionPolicySnapshotId,
    previous_snapshot_hash: ContentHash,
    previous_model_routing_revision_id: PolicyRevisionId,
    committed_policy_generation: PolicyBundleGeneration,
    committed_snapshot_id: DecisionPolicySnapshotId,
    committed_snapshot_hash: ContentHash,
    committed_model_routing_revision_id: PolicyRevisionId,
    policy_activation_id: PolicyActivationId,
    audit_event_id: AuditEventId,
}

impl ShadowBindingReceipt {
    pub fn try_seal(input: ShadowBindingReceiptInput) -> Result<Self, FeedbackError> {
        input.params.validate()?;
        let next_generation = input
            .params
            .expected_policy_generation
            .checked_next()
            .map_err(|error| invalid(error.to_string()))?;
        if input.committed_policy_generation != next_generation
            || input.binding_generation
                != input
                    .params
                    .expected_route_generation
                    .checked_add(1)
                    .ok_or_else(|| invalid("shadow binding generation overflowed"))?
        {
            return Err(invalid(
                "shadow-binding receipt does not carry the exact next policy and route generations",
            ));
        }
        let mut receipt = Self {
            format_version: SHADOW_BINDING_VERSION,
            binding_id: input.params.artifact_id,
            receipt_hash: ContentHash::from_bytes([0; 32]),
            job_input_hash: input.params.input_hash()?,
            feedback_cycle_id: input.params.feedback_cycle_id,
            cycle_idempotency_hash: input.params.cycle_idempotency_hash,
            route: input.params.route,
            profile_ref: input.params.profile_ref,
            candidate_recipe_hash: input.params.candidate_recipe_hash,
            champion_model_version_id: input.params.champion_model_version_id,
            champion_serving_contract_hash: input.params.champion_serving_contract_hash,
            candidate_model_version_id: input.params.candidate_model_version_id,
            candidate_artifact_hash: input.params.candidate_artifact_hash,
            candidate_serving_contract_hash: input.params.candidate_serving_contract_hash,
            candidate_manifest_id: input.params.candidate_manifest_id,
            candidate_manifest_hash: input.params.candidate_manifest_hash,
            candidate_training_dataset_id: input.params.candidate_training_dataset_id,
            bound_at: input.bound_at,
            previous_route_generation: input.params.expected_route_generation,
            binding_generation: input.binding_generation,
            reserved_model_bytes: input.params.reserved_model_bytes,
            previous_policy_generation: input.params.expected_policy_generation,
            previous_snapshot_id: input.params.expected_snapshot_id,
            previous_snapshot_hash: input.params.expected_snapshot_hash,
            previous_model_routing_revision_id: input.params.expected_model_routing_revision_id,
            committed_policy_generation: input.committed_policy_generation,
            committed_snapshot_id: input.committed_snapshot_id,
            committed_snapshot_hash: input.committed_snapshot_hash,
            committed_model_routing_revision_id: input.committed_model_routing_revision_id,
            policy_activation_id: input.policy_activation_id,
            audit_event_id: input.audit_event_id,
        };
        receipt.receipt_hash = receipt.derive_hash()?;
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.profile_ref
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        let next_policy = self
            .previous_policy_generation
            .checked_next()
            .map_err(|error| invalid(error.to_string()))?;
        if self.format_version != SHADOW_BINDING_VERSION
            || self.binding_id != ShadowBindingArtifactId::from_cycle_id(self.feedback_cycle_id)
            || self.job_input_hash == ContentHash::from_bytes([0; 32])
            || FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
                != self.feedback_cycle_id
            || self.candidate_model_version_id == self.champion_model_version_id
            || self.binding_generation
                != self
                    .previous_route_generation
                    .checked_add(1)
                    .ok_or_else(|| invalid("shadow binding generation overflowed"))?
            || self.reserved_model_bytes == 0
            || self.committed_policy_generation != next_policy
            || self.previous_snapshot_id
                != DecisionPolicySnapshotId::from_content_hash(&self.previous_snapshot_hash)
            || self.committed_snapshot_id
                != DecisionPolicySnapshotId::from_content_hash(&self.committed_snapshot_hash)
            || self.previous_model_routing_revision_id == self.committed_model_routing_revision_id
            || self.receipt_hash != self.derive_hash()?
        {
            return Err(invalid(
                "shadow-binding receipt identity, policy delta, route delta, or hash is invalid",
            ));
        }
        Ok(())
    }

    pub fn validate_for(&self, params: &ShadowBindingJobParams) -> Result<(), FeedbackError> {
        params.validate()?;
        self.validate()?;
        if self.binding_id != params.artifact_id
            || self.job_input_hash != params.input_hash()?
            || self.feedback_cycle_id != params.feedback_cycle_id
            || self.cycle_idempotency_hash != params.cycle_idempotency_hash
            || self.route != params.route
            || self.profile_ref != params.profile_ref
            || self.candidate_recipe_hash != params.candidate_recipe_hash
            || self.champion_model_version_id != params.champion_model_version_id
            || self.champion_serving_contract_hash != params.champion_serving_contract_hash
            || self.candidate_model_version_id != params.candidate_model_version_id
            || self.candidate_artifact_hash != params.candidate_artifact_hash
            || self.candidate_serving_contract_hash != params.candidate_serving_contract_hash
            || self.candidate_manifest_id != params.candidate_manifest_id
            || self.candidate_manifest_hash != params.candidate_manifest_hash
            || self.candidate_training_dataset_id != params.candidate_training_dataset_id
            || self.previous_route_generation != params.expected_route_generation
            || self.reserved_model_bytes != params.reserved_model_bytes
            || self.previous_policy_generation != params.expected_policy_generation
            || self.previous_snapshot_id != params.expected_snapshot_id
            || self.previous_snapshot_hash != params.expected_snapshot_hash
            || self.previous_model_routing_revision_id != params.expected_model_routing_revision_id
        {
            return Err(invalid(
                "shadow-binding receipt differs from its frozen job parameters",
            ));
        }
        Ok(())
    }

    fn derive_hash(&self) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            SHADOW_BINDING_RECEIPT_DOMAIN,
            SHADOW_BINDING_VERSION,
            &ShadowBindingReceiptPreimage {
                format_version: self.format_version,
                binding_id: self.binding_id,
                job_input_hash: self.job_input_hash,
                feedback_cycle_id: self.feedback_cycle_id,
                cycle_idempotency_hash: self.cycle_idempotency_hash,
                route: self.route,
                profile_ref: &self.profile_ref,
                candidate_recipe_hash: self.candidate_recipe_hash,
                champion_model_version_id: self.champion_model_version_id,
                champion_serving_contract_hash: self.champion_serving_contract_hash,
                candidate_model_version_id: self.candidate_model_version_id,
                candidate_artifact_hash: self.candidate_artifact_hash,
                candidate_serving_contract_hash: self.candidate_serving_contract_hash,
                candidate_manifest_id: self.candidate_manifest_id,
                candidate_manifest_hash: self.candidate_manifest_hash,
                candidate_training_dataset_id: self.candidate_training_dataset_id,
                bound_at: self.bound_at,
                previous_route_generation: self.previous_route_generation,
                binding_generation: self.binding_generation,
                reserved_model_bytes: self.reserved_model_bytes,
                previous_policy_generation: self.previous_policy_generation,
                previous_snapshot_id: self.previous_snapshot_id,
                previous_snapshot_hash: self.previous_snapshot_hash,
                previous_model_routing_revision_id: self.previous_model_routing_revision_id,
                committed_policy_generation: self.committed_policy_generation,
                committed_snapshot_id: self.committed_snapshot_id,
                committed_snapshot_hash: self.committed_snapshot_hash,
                committed_model_routing_revision_id: self.committed_model_routing_revision_id,
                policy_activation_id: self.policy_activation_id,
                audit_event_id: self.audit_event_id,
            },
        )
        .map_err(Into::into)
    }
}

/// Immutable object-store artifact emitted after the binding transaction and
/// runtime convergence both succeed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowBindingArtifact {
    pub format_version: u32,
    pub artifact_id: ShadowBindingArtifactId,
    pub artifact_hash: ContentHash,
    pub feedback_cycle_id: FeedbackCycleId,
    pub cycle_idempotency_hash: ContentHash,
    pub job_input_hash: ContentHash,
    pub receipt: ShadowBindingReceipt,
}

/// Immutable pointer from `Shadow` to the exact committed `ShadowBind`
/// predecessor and its Comparison lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShadowBindingArtifactRef {
    pub feedback_cycle_id: FeedbackCycleId,
    pub job_id: ResearchJobId,
    pub artifact_id: ShadowBindingArtifactId,
    pub input_hash: ContentHash,
    pub route: BuyModelRoute,
    pub bound_at: DateTime<Utc>,
    pub binding_generation: u64,
    pub receipt_hash: ContentHash,
    pub committed_policy_generation: PolicyBundleGeneration,
    pub committed_snapshot_id: DecisionPolicySnapshotId,
    pub committed_snapshot_hash: ContentHash,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_model_version_id: ModelVersionId,
    pub candidate_serving_contract_hash: ContentHash,
    pub comparison: FeedbackComparisonArtifactRef,
    pub artifact: ResearchJobArtifactRef,
}

impl ShadowBindingArtifactRef {
    pub fn validate_for(&self, feedback_cycle_id: FeedbackCycleId) -> Result<(), FeedbackError> {
        self.comparison.validate_for(feedback_cycle_id)?;
        if self.feedback_cycle_id != feedback_cycle_id
            || self.artifact_id != ShadowBindingArtifactId::from_cycle_id(feedback_cycle_id)
            || self.binding_generation == 0
            || self.champion_model_version_id == self.candidate_model_version_id
            || self.champion_serving_contract_hash == self.candidate_serving_contract_hash
            || self.committed_snapshot_id
                != DecisionPolicySnapshotId::from_content_hash(&self.committed_snapshot_hash)
            || self.input_hash == ContentHash::from_bytes([0; 32])
            || self.receipt_hash == ContentHash::from_bytes([0; 32])
        {
            return Err(invalid(
                "ShadowBind reference differs from its cycle, route binding, or Comparison",
            ));
        }
        Ok(())
    }
}

impl ShadowBindingArtifact {
    pub fn try_seal(
        params: &ShadowBindingJobParams,
        receipt: ShadowBindingReceipt,
    ) -> Result<Self, FeedbackError> {
        receipt.validate()?;
        let mut artifact = Self {
            format_version: SHADOW_BINDING_VERSION,
            artifact_id: params.artifact_id,
            artifact_hash: ContentHash::from_bytes([0; 32]),
            feedback_cycle_id: params.feedback_cycle_id,
            cycle_idempotency_hash: params.cycle_idempotency_hash,
            job_input_hash: params.input_hash()?,
            receipt,
        };
        artifact.artifact_hash = artifact.derive_hash()?;
        artifact.validate_for(params)?;
        Ok(artifact)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.receipt.validate()?;
        if self.format_version != SHADOW_BINDING_VERSION
            || self.artifact_id != ShadowBindingArtifactId::from_cycle_id(self.feedback_cycle_id)
            || FeedbackCycleId::from_idempotency_hash(&self.cycle_idempotency_hash)
                != self.feedback_cycle_id
            || self.receipt.binding_id != self.artifact_id
            || self.receipt.feedback_cycle_id != self.feedback_cycle_id
            || self.receipt.cycle_idempotency_hash != self.cycle_idempotency_hash
            || self.receipt.job_input_hash != self.job_input_hash
            || self.artifact_hash != self.derive_hash()?
        {
            return Err(invalid(
                "shadow-binding artifact identity, receipt, or hash is invalid",
            ));
        }
        Ok(())
    }

    pub fn validate_for(&self, params: &ShadowBindingJobParams) -> Result<(), FeedbackError> {
        params.validate()?;
        self.validate()?;
        self.receipt.validate_for(params)?;
        if self.artifact_id != params.artifact_id
            || self.feedback_cycle_id != params.feedback_cycle_id
            || self.cycle_idempotency_hash != params.cycle_idempotency_hash
            || self.job_input_hash != params.input_hash()?
            || self.receipt.binding_id != self.artifact_id
            || self.receipt.candidate_recipe_hash != params.candidate_recipe_hash
            || self.receipt.candidate_model_version_id != params.candidate_model_version_id
            || self.artifact_hash != self.derive_hash()?
        {
            return Err(invalid(
                "shadow-binding artifact differs from its job or receipt preimage",
            ));
        }
        Ok(())
    }

    fn derive_hash(&self) -> Result<ContentHash, FeedbackError> {
        CanonicalDigest::content_hash_typed(
            SHADOW_BINDING_ARTIFACT_DOMAIN,
            SHADOW_BINDING_VERSION,
            &(
                self.format_version,
                self.artifact_id,
                self.feedback_cycle_id,
                self.cycle_idempotency_hash,
                self.job_input_hash,
                &self.receipt,
            ),
        )
        .map_err(Into::into)
    }
}

/// Result of one committed and converged route-owned shadow binding.
pub struct ShadowBindingExecutionResult {
    pub artifact_id: ShadowBindingArtifactId,
    pub artifact: ResearchJobArtifactRef,
}

/// Internal production execution boundary for the `ShadowBind` stage.
#[async_trait]
pub trait ShadowBindingExecutionPort: Send + Sync {
    async fn bind_shadow(
        &self,
        params: ShadowBindingJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<ShadowBindingExecutionResult>;
}

fn invalid(detail: impl Into<String>) -> FeedbackError {
    FeedbackError::InvalidJobContract {
        detail: detail.into(),
    }
}
