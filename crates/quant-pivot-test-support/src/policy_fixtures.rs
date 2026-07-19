//! Typed policy-governance fixtures shared by integration tests.

use chrono::{Duration, Utc};
use quant_pivot_models::{
    domain::{
        NewDecisionPolicySnapshot, NewPolicyActivation, NewPolicyRevision, RecordPolicyApproval,
    },
    enums::runtime_config::{
        ConfigResourceKind, DecisionPolicySnapshotSource, PolicyActivationKind, PolicyActorKind,
        PolicyApprovalDecision, PolicyRevisionStatus,
    },
    hashing::CanonicalDigest,
    runtime_config::{
        DecisionPolicySnapshot, POLICY_RESOURCE_SCHEMA_VERSION, PolicyValidationEvidence,
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, PolicyActivationId, PolicyApprovalId,
        PolicyIdempotencyKey, PolicyRevisionId,
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
        repo.mark_revision_validated(
            &revision_id,
            PolicyValidationEvidence::default(),
            preflight_token_hash.clone(),
            Utc::now() + Duration::days(1),
        )
        .await
        .expect("validate typed policy revision");
        let approval_id = PolicyApprovalId::from_v7();
        repo.record_approval(RecordPolicyApproval {
            policy_approval_id: approval_id.clone(),
            policy_revision_id: revision_id.clone(),
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
        .expect("approve typed policy revision");
        prepared.push(PreparedResource {
            kind,
            revision_id,
            approval_id,
            preflight_token_hash,
        });
    }

    let snapshot_id = DecisionPolicySnapshotId::from_v7();
    let snapshot_hash =
        CanonicalDigest::content_hash_json(&snapshot).expect("hash decision policy snapshot");
    let persisted_snapshot = NewDecisionPolicySnapshot {
        decision_policy_snapshot_id: snapshot_id.clone(),
        snapshot_hash,
        snapshot: snapshot.clone(),
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

    for resource in prepared {
        repo.activate_resource(
            NewPolicyActivation {
                policy_activation_id: PolicyActivationId::from_v7(),
                resource_kind: resource.kind,
                policy_revision_id: resource.revision_id,
                decision_policy_snapshot_id: snapshot_id.clone(),
                policy_approval_id: resource.approval_id,
                activated_by_kind: PolicyActorKind::System,
                activated_by_user_id: None,
                activated_by_label: actor.to_owned(),
                reason: reason.to_owned(),
                activation_kind: PolicyActivationKind::Initial,
                expected_active_revision_id: None,
                previous_policy_revision_id: None,
                rollback_target_revision_id: None,
                preflight_token_hash: resource.preflight_token_hash,
                idempotency_key: format!("test-boot-{}-{snapshot_id}", resource.kind)
                    .parse::<PolicyIdempotencyKey>()
                    .expect("valid test policy idempotency key"),
                audit_event_id: None,
            },
            persisted_snapshot.clone(),
        )
        .await
        .expect("activate typed policy revision");
    }
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
