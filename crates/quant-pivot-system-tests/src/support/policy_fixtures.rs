//! Typed policy-governance fixtures owned by system tests.

use chrono::{Duration, Utc};
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
        DecisionPolicySnapshot, POLICY_RESOURCE_SCHEMA_VERSION, PolicyRevisionBundle,
        PolicySnapshotError, PolicyValidationEvidence, PolicyValidationSubject,
    },
    types::{
        AuditEventId, ContentHash, DecisionPolicySnapshotId, PolicyActivationId, PolicyApprovalId,
        PolicyBundleGeneration, PolicyIdempotencyKey, PolicyRevisionId,
    },
};
use quant_pivot_repository::{postgres::PgPolicyRepository, traits::PolicyRepository};
use sea_orm::DatabaseConnection;

#[derive(Debug, Clone)]
struct PreparedResource {
    kind: ConfigResourceKind,
    revision_id: PolicyRevisionId,
    approval_id: PolicyApprovalId,
    preflight_token_hash: ContentHash,
}

struct FixtureActivationContext<'a> {
    snapshot_id: &'a DecisionPolicySnapshotId,
    snapshot_hash: &'a ContentHash,
    persisted_snapshot: &'a NewDecisionPolicySnapshot,
    actor: &'a str,
    reason: &'a str,
}

struct PolicySnapshotFixture<'a> {
    snapshot_id: DecisionPolicySnapshotId,
    snapshot_hash: ContentHash,
    snapshot: &'a DecisionPolicySnapshot,
    source: DecisionPolicySnapshotSource,
    actor: &'a str,
    reason: &'a str,
}

impl TryFrom<PolicySnapshotFixture<'_>> for NewDecisionPolicySnapshot {
    type Error = PolicySnapshotError;

    fn try_from(fixture: PolicySnapshotFixture<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            decision_policy_snapshot_id: fixture.snapshot_id,
            snapshot_hash: fixture.snapshot_hash,
            snapshot: fixture.snapshot.persistence_document()?,
            recommendation_policy_revision_id: required_revision(
                fixture.snapshot,
                ConfigResourceKind::RecommendationPolicy,
            ),
            execution_risk_policy_revision_id: required_revision(
                fixture.snapshot,
                ConfigResourceKind::ExecutionRiskPolicy,
            ),
            model_routing_revision_id: required_revision(
                fixture.snapshot,
                ConfigResourceKind::ModelRouting,
            ),
            report_schedule_revision_id: required_revision(
                fixture.snapshot,
                ConfigResourceKind::ReportSchedule,
            ),
            operations_policy_revision_id: required_revision(
                fixture.snapshot,
                ConfigResourceKind::OperationsPolicy,
            ),
            execution_automation_policy_revision_id: required_revision(
                fixture.snapshot,
                ConfigResourceKind::ExecutionAutomationPolicy,
            ),
            source: fixture.source,
            created_by_kind: PolicyActorKind::System,
            created_by_user_id: None,
            created_by_label: fixture.actor.to_owned(),
            reason: fixture.reason.to_owned(),
        })
    }
}

async fn activate_prepared_resources(
    repo: &dyn PolicyRepository,
    prepared: Vec<PreparedResource>,
    context: FixtureActivationContext<'_>,
) {
    let committed_generation = PolicyBundleGeneration::FIRST
        .checked_next()
        .expect("boot generation has a successor");
    for (index, resource) in prepared.into_iter().enumerate() {
        let expected_bundle_generation = if index == 0 {
            PolicyBundleGeneration::FIRST
        } else {
            committed_generation
        };
        let idempotency_key = format!("test-boot-{}-{}", resource.kind, context.snapshot_id)
            .parse::<PolicyIdempotencyKey>()
            .expect("valid test policy idempotency key");
        repo.activate_resource(
            NewPolicyActivation {
                bundle_generation: committed_generation,
                expected_bundle_generation,
                policy_activation_id: PolicyActivationId::from_v7(),
                resource_kind: resource.kind,
                policy_revision_id: resource.revision_id,
                decision_policy_snapshot_id: *context.snapshot_id,
                policy_approval_id: resource.approval_id,
                activated_by_kind: PolicyActorKind::System,
                activated_by_user_id: None,
                activated_by_label: context.actor.to_owned(),
                reason: context.reason.to_owned(),
                activation_kind: PolicyActivationKind::Initial,
                expected_active_revision_id: None,
                previous_policy_revision_id: None,
                rollback_target_revision_id: None,
                preflight_token_hash: resource.preflight_token_hash,
                activation_request_hash: CanonicalDigest::content_hash_json(&(
                    "test-policy-activation",
                    idempotency_key.as_str(),
                    context.snapshot_hash,
                ))
                .expect("hash test activation request"),
                idempotency_key,
                audit_event_id: AuditEventId::from_v7(),
            },
            context.persisted_snapshot.clone(),
        )
        .await
        .expect("activate typed policy revision");
    }
}

/// Persist a complete six-resource boot bundle through the production
/// revision → validation → approval → activation workflow.
pub async fn bootstrap_policy_bundle(
    repo: &dyn PolicyRepository,
    config: &DecisionPolicySnapshot,
    actor: &str,
    reason: &str,
) -> DecisionPolicySnapshotId {
    if let Some(current) = repo
        .load_current()
        .await
        .expect("load current policy bundle")
    {
        return current.decision_policy_snapshot_id;
    }

    let mut snapshot = config.clone();
    let validation = snapshot.validate_runtime_config();
    assert!(
        !validation.has_errors(),
        "fixture policy must pass production semantic validation before any persistence: {validation}"
    );
    repo.ensure_policy_profile_artifacts(&snapshot.profile_artifacts, actor, reason)
        .await
        .expect("persist typed policy profile artifacts");
    let mut prepared = Vec::with_capacity(ConfigResourceKind::ALL.len());
    for kind in ConfigResourceKind::ALL {
        let revision_id = PolicyRevisionId::from_v7();
        snapshot.set_resource_revision_id(kind, revision_id);
        let document = snapshot.resource_document(kind);
        let revision_hash =
            CanonicalDigest::content_hash_json(&document).expect("hash typed policy document");
        let preflight_token_hash =
            CanonicalDigest::content_hash_json(&("test-policy-preflight", kind, revision_hash))
                .expect("hash test preflight token");
        repo.create_revision(NewPolicyRevision {
            policy_revision_id: revision_id,
            resource_kind: kind,
            schema_version: POLICY_RESOURCE_SCHEMA_VERSION,
            revision_hash,
            document,
            status: PolicyRevisionStatus::Draft,
            validation_evidence: None,
            validated_at: None,
            preflight_token_hash: None,
            preflight_expires_at: None,
            created_by_kind: PolicyActorKind::System,
            created_by_user_id: None,
            created_by_label: actor.to_owned(),
            reason: reason.to_owned(),
        })
        .await
        .expect("create typed policy revision");
        let approval_id = PolicyApprovalId::from_v7();
        prepared.push(PreparedResource {
            kind,
            revision_id,
            approval_id,
            preflight_token_hash,
        });
    }

    let snapshot_hash = snapshot
        .persistence_hash()
        .expect("hash decision policy persistence document");
    let snapshot_id = DecisionPolicySnapshotId::from_content_hash(&snapshot_hash);
    for resource in &prepared {
        repo.mark_revision_validated(
            &resource.revision_id,
            PolicyValidationEvidence {
                subject: Some(PolicyValidationSubject {
                    base_generation: PolicyBundleGeneration::FIRST,
                    base_revision_vector: PolicyRevisionBundle::default(),
                    candidate_bundle_hash: snapshot_hash,
                }),
                ..PolicyValidationEvidence::default()
            },
            resource.preflight_token_hash,
            Utc::now() + Duration::days(1),
        )
        .await
        .expect("bind policy validation to the complete boot bundle");
        repo.record_approval(RecordPolicyApproval {
            policy_approval_id: resource.approval_id,
            policy_revision_id: resource.revision_id,
            resource_kind: resource.kind,
            decision: PolicyApprovalDecision::Approved,
            decided_by_kind: PolicyActorKind::System,
            decided_by_user_id: None,
            decided_by_label: actor.to_owned(),
            reason: reason.to_owned(),
            decided_at: Utc::now(),
            expires_at: None,
        })
        .await
        .expect("approve complete boot validation subject");
    }
    let persisted_snapshot = NewDecisionPolicySnapshot::try_from(PolicySnapshotFixture {
        snapshot_id,
        snapshot_hash,
        snapshot: &snapshot,
        source: DecisionPolicySnapshotSource::Bootstrap,
        actor,
        reason,
    })
    .expect("build typed persistence policy document");

    activate_prepared_resources(
        repo,
        prepared,
        FixtureActivationContext {
            snapshot_id: &snapshot_id,
            snapshot_hash: &snapshot_hash,
            persisted_snapshot: &persisted_snapshot,
            actor,
            reason,
        },
    )
    .await;
    snapshot_id
}

/// Persist and activate one changed resource against the exact current bundle.
///
/// This exercises the production revision → validation → approval → CAS
/// activation workflow, so tests never manufacture an in-memory policy row or
/// mistake the idempotent boot helper for a second immutable snapshot.
pub async fn activate_policy_bundle(
    repo: &dyn PolicyRepository,
    kind: ConfigResourceKind,
    actor: &str,
    reason: &str,
    mutate: impl FnOnce(&mut DecisionPolicySnapshot),
) -> DecisionPolicySnapshotId {
    let base = repo
        .load_current_bundle()
        .await
        .expect("load current policy bundle")
        .expect("policy bundle must be bootstrapped before activation");
    let active = repo
        .load_current_activations()
        .await
        .expect("load current policy activations");
    let expected_active_revision_id = active
        .iter()
        .find(|activation| activation.resource_kind == kind)
        .map(|activation| activation.policy_revision_id)
        .expect("active revision for changed policy resource");

    let mut candidate = base.snapshot;
    mutate(&mut candidate);
    repo.ensure_policy_profile_artifacts(&candidate.profile_artifacts, actor, reason)
        .await
        .expect("persist candidate policy profile artifacts");
    let revision_id = PolicyRevisionId::from_v7();
    candidate.set_resource_revision_id(kind, revision_id);
    let document = candidate.resource_document(kind);
    let revision_hash =
        CanonicalDigest::content_hash_json(&document).expect("hash candidate policy resource");
    repo.create_revision(NewPolicyRevision {
        policy_revision_id: revision_id,
        resource_kind: kind,
        schema_version: POLICY_RESOURCE_SCHEMA_VERSION,
        revision_hash,
        document,
        status: PolicyRevisionStatus::Draft,
        validation_evidence: None,
        validated_at: None,
        preflight_token_hash: None,
        preflight_expires_at: None,
        created_by_kind: PolicyActorKind::System,
        created_by_user_id: None,
        created_by_label: actor.to_owned(),
        reason: reason.to_owned(),
    })
    .await
    .expect("create candidate policy revision");

    let snapshot_hash = candidate
        .persistence_hash()
        .expect("hash candidate policy bundle");
    let preflight_token_hash = CanonicalDigest::content_hash_json(&(
        "test-policy-candidate-preflight",
        kind,
        revision_hash,
        snapshot_hash,
    ))
    .expect("hash candidate policy preflight");
    repo.mark_revision_validated(
        &revision_id,
        PolicyValidationEvidence {
            subject: Some(PolicyValidationSubject {
                base_generation: base.generation,
                base_revision_vector: base.revision_vector,
                candidate_bundle_hash: snapshot_hash,
            }),
            ..PolicyValidationEvidence::default()
        },
        preflight_token_hash,
        Utc::now() + Duration::days(1),
    )
    .await
    .expect("validate candidate policy revision");
    let approval_id = PolicyApprovalId::from_v7();
    repo.record_approval(RecordPolicyApproval {
        policy_approval_id: approval_id,
        policy_revision_id: revision_id,
        resource_kind: kind,
        decision: PolicyApprovalDecision::Approved,
        decided_by_kind: PolicyActorKind::System,
        decided_by_user_id: None,
        decided_by_label: actor.to_owned(),
        reason: reason.to_owned(),
        decided_at: Utc::now(),
        expires_at: None,
    })
    .await
    .expect("approve candidate policy revision");

    let next_generation = base
        .generation
        .checked_next()
        .expect("policy generation has a successor");
    let snapshot_id = DecisionPolicySnapshotId::from_content_hash(&snapshot_hash);
    let persisted_snapshot = NewDecisionPolicySnapshot::try_from(PolicySnapshotFixture {
        snapshot_id,
        snapshot_hash,
        snapshot: &candidate,
        source: DecisionPolicySnapshotSource::Activation,
        actor,
        reason,
    })
    .expect("build candidate policy persistence document");
    let idempotency_key = format!("test-promote-{kind}-{snapshot_id}")
        .parse::<PolicyIdempotencyKey>()
        .expect("valid candidate activation idempotency key");
    repo.activate_resource(
        NewPolicyActivation {
            bundle_generation: next_generation,
            expected_bundle_generation: base.generation,
            policy_activation_id: PolicyActivationId::from_v7(),
            resource_kind: kind,
            policy_revision_id: revision_id,
            decision_policy_snapshot_id: snapshot_id,
            policy_approval_id: approval_id,
            activated_by_kind: PolicyActorKind::System,
            activated_by_user_id: None,
            activated_by_label: actor.to_owned(),
            reason: reason.to_owned(),
            activation_kind: PolicyActivationKind::Promote,
            expected_active_revision_id: Some(expected_active_revision_id),
            previous_policy_revision_id: None,
            rollback_target_revision_id: None,
            preflight_token_hash,
            activation_request_hash: CanonicalDigest::content_hash_json(&(
                "test-policy-candidate-activation",
                idempotency_key.as_str(),
                snapshot_hash,
            ))
            .expect("hash candidate policy activation"),
            idempotency_key,
            audit_event_id: AuditEventId::from_v7(),
        },
        persisted_snapshot,
    )
    .await
    .expect("activate candidate policy revision");
    snapshot_id
}

/// Persist the default typed policy bundle for a Postgres integration test.
pub async fn bootstrap_default_policy_bundle(
    db: &DatabaseConnection,
    actor: &str,
    reason: &str,
) -> DecisionPolicySnapshotId {
    bootstrap_policy_bundle(
        &PgPolicyRepository::new(db.clone()),
        &DecisionPolicySnapshot::default(),
        actor,
        reason,
    )
    .await
}

const fn required_revision(
    snapshot: &DecisionPolicySnapshot,
    kind: ConfigResourceKind,
) -> PolicyRevisionId {
    snapshot
        .resource_revision_id(kind)
        .copied()
        .expect("complete test policy revision bundle")
}
