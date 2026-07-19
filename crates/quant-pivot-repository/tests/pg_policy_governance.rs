//! Typed policy-governance persistence integration tests (`PostgreSQL`).

use chrono::{Duration, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        NewDecisionPolicySnapshot, NewPolicyActivation, NewPolicyRevision, RecordPolicyApproval,
    },
    enums::runtime_config::{
        ConfigResourceKind, DecisionPolicySnapshotSource, PolicyActivationKind, PolicyActorKind,
        PolicyApprovalDecision, PolicyRevisionStatus,
    },
    hashing::CanonicalDigest,
    runtime_config::{POLICY_RESOURCE_SCHEMA_VERSION, PolicyValidationEvidence},
    types::{
        DecisionPolicySnapshotId, PolicyActivationId, PolicyApprovalId, PolicyIdempotencyKey,
        PolicyRevisionId,
    },
};
use quant_pivot_repository::{postgres::PgPolicyRepository, traits::PolicyRepository};
use quant_pivot_test_support::{pg::setup_pg, policy_fixtures::bootstrap_default_policy_bundle};

#[tokio::test]
#[ignore = "requires Docker"]
async fn active_resources_are_loaded_in_one_typed_set_and_approvals_are_single_use() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    bootstrap_default_policy_bundle(
        &db,
        "policy-governance-it",
        "bootstrap complete typed policy bundle",
    )
    .await;
    let repo = PgPolicyRepository::new(db);

    let active = repo
        .load_current_activations()
        .await
        .expect("load current activations");
    assert_eq!(active.len(), ConfigResourceKind::ALL.len());
    assert!(ConfigResourceKind::ALL.iter().all(|kind| {
        active
            .iter()
            .filter(|activation| activation.resource_kind == *kind)
            .count()
            == 1
    }));
    assert!(
        repo.count_valid_approvals()
            .await
            .expect("count pending approvals")
            .is_empty(),
        "boot approvals are already consumed by their exact activations"
    );

    let current = repo
        .load_current()
        .await
        .expect("load current policy bundle")
        .expect("boot bundle exists");
    let kind = ConfigResourceKind::RecommendationPolicy;
    let mut candidate = current.snapshot;
    candidate.recommendation.data_quality.max_book_age_ms += 1;
    let revision_id = PolicyRevisionId::from_v7();
    candidate.set_resource_revision_id(kind, revision_id.clone());
    let document = candidate.resource_document(kind);
    let revision_hash =
        CanonicalDigest::content_hash_json(&document).expect("hash typed policy document");
    repo.create_revision(NewPolicyRevision {
        policy_revision_id: revision_id.clone(),
        resource_kind: kind,
        schema_version: POLICY_RESOURCE_SCHEMA_VERSION,
        revision_hash: revision_hash.clone(),
        document,
        status: PolicyRevisionStatus::Draft,
        validation_evidence: None,
        validated_at: None,
        preflight_token_hash: None,
        preflight_expires_at: None,
        created_by_kind: PolicyActorKind::System,
        created_by_user_id: None,
        created_by_label: "policy-governance-it".to_owned(),
        reason: "exercise a fresh typed revision".to_owned(),
    })
    .await
    .expect("create revision");
    let preflight_token_hash = CanonicalDigest::content_hash_json(&(
        "policy-governance-it-preflight",
        revision_hash.as_str(),
    ))
    .expect("hash preflight proof");
    repo.mark_revision_validated(
        &revision_id,
        PolicyValidationEvidence::default(),
        preflight_token_hash.clone(),
        Utc::now() + Duration::minutes(10),
    )
    .await
    .expect("validate revision");
    let approval_id = PolicyApprovalId::from_v7();
    repo.record_approval(RecordPolicyApproval {
        policy_approval_id: approval_id.clone(),
        policy_revision_id: revision_id.clone(),
        resource_kind: kind,
        decision: PolicyApprovalDecision::Approved,
        decided_by_kind: PolicyActorKind::System,
        decided_by_user_id: None,
        decided_by_label: "policy-governance-it".to_owned(),
        reason: "approve exact revision hash".to_owned(),
        decided_at: Utc::now(),
        expires_at: None,
    })
    .await
    .expect("record approval");
    assert_eq!(
        repo.count_valid_approvals()
            .await
            .expect("count pending approval")
            .get(&kind),
        Some(&1)
    );

    let snapshot_id = DecisionPolicySnapshotId::from_v7();
    let new_snapshot = persisted_snapshot(snapshot_id.clone(), candidate.clone());
    let first_activation = activation(ActivationFixture {
        kind,
        revision_id: revision_id.clone(),
        snapshot_id: snapshot_id.clone(),
        approval_id: approval_id.clone(),
        expected_active_revision_id: Some(
            active
                .iter()
                .find(|row| row.resource_kind == kind)
                .expect("recommendation activation")
                .policy_revision_id
                .clone(),
        ),
        preflight_token_hash: preflight_token_hash.clone(),
        idempotency_key: "policy-governance-it-activation-1",
    });
    repo.activate_resource(first_activation, new_snapshot.clone())
        .await
        .expect("activate approved revision");
    assert!(
        repo.count_valid_approvals()
            .await
            .expect("count consumed approval")
            .is_empty()
    );

    let replay = activation(ActivationFixture {
        kind,
        revision_id: revision_id.clone(),
        snapshot_id,
        approval_id,
        expected_active_revision_id: Some(revision_id),
        preflight_token_hash,
        idempotency_key: "policy-governance-it-activation-2",
    });
    assert!(matches!(
        repo.activate_resource(replay, new_snapshot).await,
        Err(StorageError::StateConflict { entity, .. }) if entity == "policy_activation"
    ));
}

fn persisted_snapshot(
    snapshot_id: DecisionPolicySnapshotId,
    snapshot: quant_pivot_models::runtime_config::DecisionPolicySnapshot,
) -> NewDecisionPolicySnapshot {
    NewDecisionPolicySnapshot {
        decision_policy_snapshot_id: snapshot_id,
        snapshot_hash: CanonicalDigest::content_hash_json(&snapshot).expect("hash policy snapshot"),
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
        snapshot,
        source: DecisionPolicySnapshotSource::Activation,
        created_by_kind: PolicyActorKind::System,
        created_by_user_id: None,
        created_by_label: "policy-governance-it".to_owned(),
        reason: "freeze exact decision policy bundle".to_owned(),
    }
}

struct ActivationFixture<'a> {
    kind: ConfigResourceKind,
    revision_id: PolicyRevisionId,
    snapshot_id: DecisionPolicySnapshotId,
    approval_id: PolicyApprovalId,
    expected_active_revision_id: Option<PolicyRevisionId>,
    preflight_token_hash: quant_pivot_models::types::ContentHash,
    idempotency_key: &'a str,
}

fn activation(fixture: ActivationFixture<'_>) -> NewPolicyActivation {
    NewPolicyActivation {
        policy_activation_id: PolicyActivationId::from_v7(),
        resource_kind: fixture.kind,
        policy_revision_id: fixture.revision_id,
        decision_policy_snapshot_id: fixture.snapshot_id,
        policy_approval_id: fixture.approval_id,
        activated_by_kind: PolicyActorKind::System,
        activated_by_user_id: None,
        activated_by_label: "policy-governance-it".to_owned(),
        reason: "activate exact approved revision".to_owned(),
        activation_kind: PolicyActivationKind::Promote,
        expected_active_revision_id: fixture.expected_active_revision_id,
        previous_policy_revision_id: None,
        rollback_target_revision_id: None,
        preflight_token_hash: fixture.preflight_token_hash,
        idempotency_key: fixture
            .idempotency_key
            .parse::<PolicyIdempotencyKey>()
            .expect("valid policy idempotency key"),
        audit_event_id: None,
    }
}

fn required_revision(
    snapshot: &quant_pivot_models::runtime_config::DecisionPolicySnapshot,
    kind: ConfigResourceKind,
) -> PolicyRevisionId {
    snapshot
        .resource_revision_id(kind)
        .cloned()
        .expect("complete policy revision bundle")
}
