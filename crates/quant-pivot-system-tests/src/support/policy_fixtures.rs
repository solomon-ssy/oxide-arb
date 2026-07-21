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
        PolicyValidationEvidence, PolicyValidationSubject,
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
                decision_policy_snapshot_id: context.snapshot_id.clone(),
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
                    context.snapshot_hash.as_str(),
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
    repo.ensure_policy_profile_artifacts(&snapshot.profile_artifacts, actor, reason)
        .await
        .expect("persist typed policy profile artifacts");
    let mut prepared = Vec::with_capacity(ConfigResourceKind::ALL.len());
    for kind in ConfigResourceKind::ALL {
        let revision_id = PolicyRevisionId::from_v7();
        snapshot.set_resource_revision_id(kind, revision_id.clone());
        let document = snapshot.resource_document(kind);
        let revision_hash =
            CanonicalDigest::content_hash_json(&document).expect("hash typed policy document");
        let preflight_token_hash = CanonicalDigest::content_hash_json(&(
            "test-policy-preflight",
            kind,
            revision_hash.as_str(),
        ))
        .expect("hash test preflight token");
        repo.create_revision(NewPolicyRevision {
            policy_revision_id: revision_id.clone(),
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

    let snapshot_id = DecisionPolicySnapshotId::from_v7();
    let snapshot_hash = snapshot
        .persistence_hash()
        .expect("hash decision policy persistence document");
    for resource in &prepared {
        repo.mark_revision_validated(
            &resource.revision_id,
            PolicyValidationEvidence {
                subject: Some(PolicyValidationSubject {
                    base_generation: PolicyBundleGeneration::FIRST,
                    base_revision_vector: PolicyRevisionBundle::default(),
                    candidate_bundle_hash: snapshot_hash.clone(),
                }),
                ..PolicyValidationEvidence::default()
            },
            resource.preflight_token_hash.clone(),
            Utc::now() + Duration::days(1),
        )
        .await
        .expect("bind policy validation to the complete boot bundle");
        repo.record_approval(RecordPolicyApproval {
            policy_approval_id: resource.approval_id.clone(),
            policy_revision_id: resource.revision_id.clone(),
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
    let persisted_snapshot = NewDecisionPolicySnapshot {
        bundle_generation: PolicyBundleGeneration::FIRST
            .checked_next()
            .expect("boot generation has a successor"),
        decision_policy_snapshot_id: snapshot_id.clone(),
        snapshot_hash: snapshot_hash.clone(),
        snapshot: snapshot
            .persistence_document()
            .expect("build typed persistence policy document"),
        recommendation_policy_revision_id: required_revision(
            &snapshot,
            ConfigResourceKind::RecommendationPolicy,
        ),
        execution_risk_policy_revision_id: required_revision(
            &snapshot,
            ConfigResourceKind::ExecutionRiskPolicy,
        ),
        model_routing_revision_id: required_revision(&snapshot, ConfigResourceKind::ModelRouting),
        report_schedule_revision_id: required_revision(
            &snapshot,
            ConfigResourceKind::ReportSchedule,
        ),
        operational_control_revision_id: required_revision(
            &snapshot,
            ConfigResourceKind::OperationalControl,
        ),
        execution_authorization_revision_id: required_revision(
            &snapshot,
            ConfigResourceKind::ExecutionAuthorization,
        ),
        source: DecisionPolicySnapshotSource::Bootstrap,
        created_by_kind: PolicyActorKind::System,
        created_by_user_id: None,
        created_by_label: actor.to_owned(),
        reason: reason.to_owned(),
    };

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

fn required_revision(
    snapshot: &DecisionPolicySnapshot,
    kind: ConfigResourceKind,
) -> PolicyRevisionId {
    snapshot
        .resource_revision_id(kind)
        .cloned()
        .expect("complete test policy revision bundle")
}
