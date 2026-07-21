//! Idempotent typed six-resource bootstrap through the production governance workflow.

use std::fmt::Display;

use chrono::{Duration, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::governance::{
        NewDecisionPolicySnapshot, NewPolicyActivation, NewPolicyRevision, RecordPolicyApproval,
    },
    enums::runtime_config::{
        ConfigResourceKind, DecisionPolicySnapshotSource, PolicyActivationKind, PolicyActorKind,
        PolicyApprovalDecision, PolicyRevisionStatus,
    },
    hashing::CanonicalDigest,
    runtime_config::{
        ActivePolicyBundle, DecisionPolicySnapshot, DecisionPolicySnapshotDocument,
        POLICY_RESOURCE_SCHEMA_VERSION, PolicyRevisionBundle, PolicyValidationEvidence,
        PolicyValidationSubject,
    },
    types::{
        AuditEventId, ContentHash, DecisionPolicySnapshotId, PolicyActivationId, PolicyApprovalId,
        PolicyBundleGeneration, PolicyIdempotencyKey, PolicyRevisionId,
    },
};

use crate::traits::PolicyRepository;

#[derive(Debug, Clone)]
struct PreparedResource {
    kind: ConfigResourceKind,
    revision_id: PolicyRevisionId,
    approval_id: PolicyApprovalId,
    document_hash: ContentHash,
    preflight_token_hash: ContentHash,
}

struct PreparedBootPolicy {
    resources: Vec<PreparedResource>,
    document: DecisionPolicySnapshotDocument,
    candidate_hash: ContentHash,
    validation_subject: PolicyValidationSubject,
}

fn prepare_boot_policy(
    snapshot: &mut DecisionPolicySnapshot,
) -> Result<PreparedBootPolicy, StorageError> {
    let mut resources = Vec::with_capacity(ConfigResourceKind::ALL.len());
    for kind in ConfigResourceKind::ALL {
        let revision_id = PolicyRevisionId::from_v7();
        snapshot.set_resource_revision_id(kind, revision_id.clone());
        let document_hash = CanonicalDigest::content_hash_json(&snapshot.resource_document(kind))
            .map_err(hash_error)?;
        let preflight_token_hash = CanonicalDigest::content_hash_json(&(
            "quant-pivot/boot-policy-preflight/v1",
            kind,
            document_hash.as_str(),
        ))
        .map_err(hash_error)?;
        resources.push(PreparedResource {
            kind,
            revision_id,
            approval_id: PolicyApprovalId::from_v7(),
            document_hash,
            preflight_token_hash,
        });
    }
    let document = snapshot.persistence_document().map_err(|error| {
        StorageError::invariant_violation(Some("decision_policy_snapshot"), error.to_string())
    })?;
    let candidate_hash = CanonicalDigest::content_hash_json(&document).map_err(hash_error)?;
    let validation_subject = PolicyValidationSubject {
        base_generation: PolicyBundleGeneration::FIRST,
        base_revision_vector: PolicyRevisionBundle::default(),
        candidate_bundle_hash: candidate_hash.clone(),
    };
    Ok(PreparedBootPolicy {
        resources,
        document,
        candidate_hash,
        validation_subject,
    })
}

async fn seed_resource_governance(
    repository: &dyn PolicyRepository,
    snapshot: &DecisionPolicySnapshot,
    prepared: &PreparedBootPolicy,
    actor_label: &str,
    reason: &str,
) -> Result<(), StorageError> {
    for resource in &prepared.resources {
        repository
            .create_revision(NewPolicyRevision {
                policy_revision_id: resource.revision_id.clone(),
                resource_kind: resource.kind,
                schema_version: POLICY_RESOURCE_SCHEMA_VERSION,
                revision_hash: resource.document_hash.clone(),
                document: snapshot.resource_document(resource.kind),
                status: PolicyRevisionStatus::Draft,
                validation_evidence: None,
                validated_at: None,
                preflight_token_hash: None,
                preflight_expires_at: None,
                created_by_kind: PolicyActorKind::System,
                created_by_user_id: None,
                created_by_label: actor_label.to_owned(),
                reason: reason.to_owned(),
            })
            .await?;
        repository
            .mark_revision_validated(
                &resource.revision_id,
                PolicyValidationEvidence {
                    subject: Some(prepared.validation_subject.clone()),
                    ..PolicyValidationEvidence::default()
                },
                resource.preflight_token_hash.clone(),
                Utc::now() + Duration::hours(1),
            )
            .await?;
        repository
            .record_approval(RecordPolicyApproval {
                policy_approval_id: resource.approval_id.clone(),
                policy_revision_id: resource.revision_id.clone(),
                resource_kind: resource.kind,
                decision: PolicyApprovalDecision::Approved,
                decided_by_kind: PolicyActorKind::System,
                decided_by_user_id: None,
                decided_by_label: actor_label.to_owned(),
                reason: reason.to_owned(),
                decided_at: Utc::now(),
                expires_at: None,
            })
            .await?;
    }
    Ok(())
}

struct BootActivationContext<'a> {
    generation: PolicyBundleGeneration,
    snapshot_id: &'a DecisionPolicySnapshotId,
    candidate_hash: &'a ContentHash,
    persisted_snapshot: &'a NewDecisionPolicySnapshot,
    actor_label: &'a str,
    reason: &'a str,
}

async fn activate_boot_resources(
    repository: &dyn PolicyRepository,
    resources: Vec<PreparedResource>,
    context: BootActivationContext<'_>,
) -> Result<(), StorageError> {
    for (index, resource) in resources.into_iter().enumerate() {
        let expected_generation = if index == 0 {
            PolicyBundleGeneration::FIRST
        } else {
            context.generation
        };
        let idempotency_key = format!("boot-{}-{}", resource.kind, context.snapshot_id)
            .parse::<PolicyIdempotencyKey>()
            .map_err(|error| {
                StorageError::invariant_violation(Some("policy_activation"), error.to_string())
            })?;
        let request_hash = CanonicalDigest::content_hash_json(&(
            "quant-pivot/boot-policy-activation/v1",
            idempotency_key.as_str(),
            context.candidate_hash.as_str(),
        ))
        .map_err(hash_error)?;
        repository
            .activate_resource(
                NewPolicyActivation {
                    bundle_generation: context.generation,
                    expected_bundle_generation: expected_generation,
                    policy_activation_id: PolicyActivationId::from_v7(),
                    resource_kind: resource.kind,
                    policy_revision_id: resource.revision_id,
                    decision_policy_snapshot_id: context.snapshot_id.clone(),
                    policy_approval_id: resource.approval_id,
                    activated_by_kind: PolicyActorKind::System,
                    activated_by_user_id: None,
                    activated_by_label: context.actor_label.to_owned(),
                    reason: context.reason.to_owned(),
                    activation_kind: PolicyActivationKind::Initial,
                    expected_active_revision_id: None,
                    previous_policy_revision_id: None,
                    rollback_target_revision_id: None,
                    preflight_token_hash: resource.preflight_token_hash,
                    idempotency_key,
                    activation_request_hash: request_hash,
                    audit_event_id: AuditEventId::from_v7(),
                },
                context.persisted_snapshot.clone(),
            )
            .await?;
    }
    Ok(())
}

/// Seed the canonical typed boot policy bundle exactly once.
pub async fn ensure_default_policy_bundle(
    repository: &dyn PolicyRepository,
    actor_label: &str,
    reason: &str,
) -> Result<ActivePolicyBundle, StorageError> {
    if actor_label.trim().is_empty() || reason.trim().is_empty() {
        return Err(StorageError::invariant_violation(
            Some("decision_policy_snapshot"),
            "boot policy actor and reason must be non-empty",
        ));
    }
    if let Some(current) = repository.load_current_bundle().await? {
        return Ok(current);
    }

    let mut snapshot = DecisionPolicySnapshot::default();
    repository
        .ensure_policy_profile_artifacts(&snapshot.profile_artifacts, actor_label, reason)
        .await?;
    let prepared = prepare_boot_policy(&mut snapshot)?;
    seed_resource_governance(repository, &snapshot, &prepared, actor_label, reason).await?;
    let PreparedBootPolicy {
        resources,
        document: snapshot_document,
        candidate_hash,
        ..
    } = prepared;

    let generation = PolicyBundleGeneration::FIRST
        .checked_next()
        .map_err(|error| {
            StorageError::invariant_violation(Some("decision_policy_snapshot"), error.to_string())
        })?;
    let snapshot_id = DecisionPolicySnapshotId::from_content_hash(&candidate_hash);
    let persisted_snapshot = NewDecisionPolicySnapshot {
        bundle_generation: generation,
        decision_policy_snapshot_id: snapshot_id.clone(),
        snapshot_hash: candidate_hash.clone(),
        snapshot: snapshot_document,
        recommendation_policy_revision_id: required_revision(
            &snapshot,
            ConfigResourceKind::RecommendationPolicy,
        )?,
        execution_risk_policy_revision_id: required_revision(
            &snapshot,
            ConfigResourceKind::ExecutionRiskPolicy,
        )?,
        model_routing_revision_id: required_revision(&snapshot, ConfigResourceKind::ModelRouting)?,
        report_schedule_revision_id: required_revision(
            &snapshot,
            ConfigResourceKind::ReportSchedule,
        )?,
        operational_control_revision_id: required_revision(
            &snapshot,
            ConfigResourceKind::OperationalControl,
        )?,
        execution_authorization_revision_id: required_revision(
            &snapshot,
            ConfigResourceKind::ExecutionAuthorization,
        )?,
        source: DecisionPolicySnapshotSource::Bootstrap,
        created_by_kind: PolicyActorKind::System,
        created_by_user_id: None,
        created_by_label: actor_label.to_owned(),
        reason: reason.to_owned(),
    };

    activate_boot_resources(
        repository,
        resources,
        BootActivationContext {
            generation,
            snapshot_id: &snapshot_id,
            candidate_hash: &candidate_hash,
            persisted_snapshot: &persisted_snapshot,
            actor_label,
            reason,
        },
    )
    .await?;
    let committed = repository.load_current_bundle().await?.ok_or_else(|| {
        StorageError::invariant_violation(
            Some("decision_policy_snapshot"),
            "boot policy activation committed no current bundle",
        )
    })?;
    if committed.generation != generation
        || committed.decision_policy_snapshot_id != snapshot_id
        || committed.snapshot_hash != candidate_hash
    {
        return Err(StorageError::invariant_violation(
            Some("decision_policy_snapshot"),
            "boot policy bundle identity differs after activation",
        ));
    }
    Ok(committed)
}

fn required_revision(
    snapshot: &DecisionPolicySnapshot,
    kind: ConfigResourceKind,
) -> Result<PolicyRevisionId, StorageError> {
    snapshot.resource_revision_id(kind).cloned().ok_or_else(|| {
        StorageError::invariant_violation(
            Some("decision_policy_snapshot"),
            format!("boot policy has no {} revision", kind.as_str()),
        )
    })
}

fn hash_error(error: impl Display) -> StorageError {
    StorageError::invariant_violation(
        Some("decision_policy_snapshot"),
        format!("boot policy hash failed: {error}"),
    )
}
