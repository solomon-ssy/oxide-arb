//! Typed policy-governance persistence system contracts.

use chrono::{Duration, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::governance::{
        ConfigActivityInfo, DecisionPolicySnapshotInfo, NewDecisionPolicySnapshot,
        NewPolicyActivation, NewPolicyRevision, PolicyActivationCommit, PolicyActivationInfo,
        PolicyActivationOutcome, RecordPolicyApproval,
    },
    entities::{
        policy_activation, policy_activation_audit, policy_activation_audit::Entity,
        policy_activation_event_outbox,
        policy_activation_event_outbox::Entity as PolicyActivationEventOutboxEntity,
    },
    enums::runtime_config::{
        ConfigResourceKind, DecisionPolicySnapshotSource, PolicyActivationKind, PolicyActorKind,
        PolicyApprovalDecision, PolicyRevisionStatus,
    },
    hashing::CanonicalDigest,
    runtime_config::{
        ActivePolicyBundle, DecisionPolicySnapshot, POLICY_RESOURCE_SCHEMA_VERSION,
        PolicyValidationEvidence, PolicyValidationSubject,
    },
    types::{
        AuditEventId, ContentHash, DecisionPolicySnapshotId, PolicyActivationId, PolicyApprovalId,
        PolicyBundleGeneration, PolicyIdempotencyKey, PolicyRevisionId,
    },
};
use quant_pivot_repository::{postgres::PgPolicyRepository, traits::PolicyRepository};
use quant_pivot_system_tests::{
    postgres::setup_pg, support::policy_fixtures::bootstrap_default_policy_bundle,
};
use sea_orm::{ConnectionTrait, DatabaseConnection, EntityTrait};

async fn assert_boot_state(
    repo: &PgPolicyRepository,
    active: &[PolicyActivationInfo],
    current: &DecisionPolicySnapshotInfo,
) {
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
    let inventory = repo
        .load_resource_inventory()
        .await
        .expect("load Config resource inventory in one statement");
    assert_eq!(inventory.resources.len(), ConfigResourceKind::ALL.len());
    assert_eq!(inventory.bundle_generation, current.bundle_generation);
    assert_eq!(
        inventory.active_snapshot_id.as_ref(),
        Some(&current.decision_policy_snapshot_id)
    );
}

async fn assert_committed_read_models(
    repo: &PgPolicyRepository,
    committed: &PolicyActivationCommit,
    kind: ConfigResourceKind,
    revision_id: &PolicyRevisionId,
    revision_hash: &ContentHash,
) {
    let latest_activity = repo
        .list_activity(1)
        .await
        .expect("load globally ordered activity");
    assert_eq!(latest_activity.len(), 1, "global limit is applied by SQL");
    assert!(
        matches!(
            &latest_activity[0],
            ConfigActivityInfo::Activation(activation)
                if activation.policy_activation_id == committed.activation.policy_activation_id
        ),
        "latest activity does not match committed activation: {latest_activity:#?}"
    );
    let snapshot_options = repo
        .list_snapshot_options(1)
        .await
        .expect("load snapshot options in one projection");
    assert_eq!(snapshot_options.len(), 1);
    assert_eq!(
        snapshot_options[0].decision_policy_snapshot_id,
        committed.bundle.decision_policy_snapshot_id
    );
    let inventory = repo
        .load_resource_inventory()
        .await
        .expect("reload committed Config resource inventory");
    let resource = inventory
        .resources
        .iter()
        .find(|resource| resource.resource_kind == kind)
        .expect("committed resource inventory row");
    assert_eq!(resource.active_revision_id.as_ref(), Some(revision_id));
    assert_eq!(resource.active_revision_hash.as_ref(), Some(revision_hash));
}

async fn assert_atomic_activation_ledger(
    db: &DatabaseConnection,
    committed: &PolicyActivationCommit,
) {
    let audit = Entity::find_by_id(committed.activation.audit_event_id.clone())
        .one(db)
        .await
        .expect("load atomic activation audit")
        .expect("activation audit exists");
    let outbox =
        PolicyActivationEventOutboxEntity::find_by_id(committed.activation.audit_event_id.clone())
            .one(db)
            .await
            .expect("load atomic activation outbox")
            .expect("activation outbox exists");
    assert_eq!(
        audit.policy_activation_id,
        committed.activation.policy_activation_id
    );
    assert_eq!(outbox.policy_activation_id, audit.policy_activation_id);
    assert_eq!(outbox.bundle_generation, committed.bundle.generation);
    assert_eq!(outbox.snapshot_hash, committed.bundle.snapshot_hash);
}

fn assert_first_concurrent_bundle(
    base: &ActivePolicyBundle,
    bundle: &ActivePolicyBundle,
    winner: ConfigResourceKind,
    expected_book_age: u64,
    expected_open_intents: u32,
) {
    assert_eq!(
        bundle.generation,
        base.generation.checked_next().expect("generation two")
    );
    assert_eq!(
        bundle.snapshot.recommendation.data_quality.max_book_age_ms,
        if winner == ConfigResourceKind::RecommendationPolicy {
            expected_book_age
        } else {
            base.snapshot.recommendation.data_quality.max_book_age_ms
        }
    );
    assert_eq!(
        bundle.snapshot.execution_risk.capital.max_open_intents,
        if winner == ConfigResourceKind::ExecutionRiskPolicy {
            expected_open_intents
        } else {
            base.snapshot.execution_risk.capital.max_open_intents
        }
    );
}

fn assert_merged_concurrent_bundle(
    commit: &PolicyActivationCommit,
    previous: &ActivePolicyBundle,
    expected_book_age: u64,
    expected_open_intents: u32,
) {
    assert_eq!(commit.outcome, PolicyActivationOutcome::Committed);
    assert_eq!(
        commit.bundle.generation,
        previous
            .generation
            .checked_next()
            .expect("generation three")
    );
    assert_eq!(
        commit
            .bundle
            .snapshot
            .recommendation
            .data_quality
            .max_book_age_ms,
        expected_book_age
    );
    assert_eq!(
        commit
            .bundle
            .snapshot
            .execution_risk
            .capital
            .max_open_intents,
        expected_open_intents
    );
}

pub async fn active_resources_are_loaded_in_one_typed_set_and_approvals_are_single_use() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    bootstrap_default_policy_bundle(
        &db,
        "policy-governance-it",
        "bootstrap complete typed policy bundle",
    )
    .await;
    let repo = PgPolicyRepository::new(db.clone());

    let active = repo
        .load_current_activations()
        .await
        .expect("load current activations");
    let current = repo
        .load_current()
        .await
        .expect("load current policy bundle")
        .expect("boot bundle exists");
    assert_boot_state(&repo, &active, &current).await;
    let kind = ConfigResourceKind::RecommendationPolicy;
    let base_generation = current.bundle_generation;
    let base_revision_vector = current.snapshot.revisions.clone();
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
    let candidate_hash = candidate
        .persistence_hash()
        .expect("hash candidate policy persistence document");
    repo.mark_revision_validated(
        &revision_id,
        PolicyValidationEvidence {
            subject: Some(PolicyValidationSubject {
                base_generation,
                base_revision_vector,
                candidate_bundle_hash: candidate_hash,
            }),
            ..PolicyValidationEvidence::default()
        },
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
    let next_generation = base_generation.checked_next().expect("next generation");
    let new_snapshot = persisted_snapshot(next_generation, snapshot_id.clone(), &candidate);
    let first_activation = activation(ActivationFixture {
        bundle_generation: next_generation,
        expected_bundle_generation: base_generation,
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
    let committed = repo
        .activate_resource(first_activation.clone(), new_snapshot.clone())
        .await
        .expect("activate approved revision");
    assert_eq!(committed.outcome, PolicyActivationOutcome::Committed);
    assert_atomic_activation_ledger(&db, &committed).await;
    assert!(
        repo.count_valid_approvals()
            .await
            .expect("count consumed approval")
            .is_empty()
    );

    let exact_replay = repo
        .activate_resource(first_activation.clone(), new_snapshot.clone())
        .await
        .expect("exact idempotent replay");
    assert_eq!(exact_replay.outcome, PolicyActivationOutcome::ExactReplay);
    assert_eq!(
        exact_replay.activation.policy_activation_id,
        committed.activation.policy_activation_id
    );

    assert_committed_read_models(&repo, &committed, kind, &revision_id, &revision_hash).await;

    let mut conflicting_replay = first_activation;
    conflicting_replay.activation_request_hash = CanonicalDigest::content_hash_json(&(
        "different-request",
        conflicting_replay.idempotency_key.as_str(),
    ))
    .expect("hash conflicting request");
    assert!(matches!(
        repo.activate_resource(conflicting_replay, new_snapshot).await,
        Err(StorageError::StateConflict { entity, .. }) if entity == "policy_activation"
    ));
}

pub async fn outbox_failure_rolls_back_activation_snapshot_guard_and_approval_consumption() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    bootstrap_default_policy_bundle(
        &db,
        "policy-atomicity-it",
        "bootstrap policy activation atomicity fixture",
    )
    .await;
    let repo = PgPolicyRepository::new(db.clone());
    let base = repo
        .load_current_bundle()
        .await
        .expect("load base policy bundle")
        .expect("boot bundle exists");
    let active = repo
        .load_current_activations()
        .await
        .expect("load base policy activations");
    let candidate = prepare_candidate(
        &repo,
        &base,
        &active,
        ConfigResourceKind::RecommendationPolicy,
        None,
        "forced-outbox-failure",
        |snapshot| snapshot.recommendation.data_quality.max_book_age_ms += 1,
    )
    .await;

    // Test-only corruption boundary: force the final ledger insert to fail so
    // the transaction must roll back every earlier activation mutation.
    db.execute_unprepared(
        "ALTER TABLE policy_activation_event_outbox \
         ADD CONSTRAINT test_force_outbox_failure CHECK (false) NOT VALID",
    )
    .await
    .expect("install test-only outbox failure constraint");

    let activation_id = candidate.activation.policy_activation_id.clone();
    let audit_event_id = candidate.activation.audit_event_id.clone();
    let snapshot_id = candidate.snapshot.decision_policy_snapshot_id.clone();
    assert!(
        repo.activate_resource(candidate.activation, candidate.snapshot)
            .await
            .is_err(),
        "a failed outbox insert must reject the complete activation"
    );

    let after = repo
        .load_current_bundle()
        .await
        .expect("reload durable bundle after failed activation")
        .expect("base bundle remains active");
    assert_eq!(after, base);
    assert!(
        repo.load_snapshot(&snapshot_id)
            .await
            .expect("look up rolled-back snapshot")
            .is_none()
    );
    assert!(
        policy_activation::Entity::find_by_id(activation_id)
            .one(&db)
            .await
            .expect("look up rolled-back activation")
            .is_none()
    );
    assert!(
        policy_activation_audit::Entity::find_by_id(audit_event_id.clone())
            .one(&db)
            .await
            .expect("look up rolled-back audit")
            .is_none()
    );
    assert!(
        policy_activation_event_outbox::Entity::find_by_id(audit_event_id)
            .one(&db)
            .await
            .expect("look up rolled-back outbox")
            .is_none()
    );
    assert_eq!(
        repo.count_valid_approvals()
            .await
            .expect("approval remains usable after rollback")
            .get(&ConfigResourceKind::RecommendationPolicy),
        Some(&1)
    );
}

pub async fn rollback_records_a_new_generation_when_content_hash_matches_history() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    bootstrap_default_policy_bundle(
        &db,
        "policy-rollback-it",
        "bootstrap rollback policy bundle",
    )
    .await;
    let repo = PgPolicyRepository::new(db);
    let base = repo
        .load_current_bundle()
        .await
        .expect("load base bundle")
        .expect("boot bundle exists");
    let base_activations = repo
        .load_current_activations()
        .await
        .expect("load base activations");
    let kind = ConfigResourceKind::RecommendationPolicy;
    let base_revision_id = base
        .snapshot
        .resource_revision_id(kind)
        .expect("base recommendation revision")
        .clone();
    let base_book_age = base.snapshot.recommendation.data_quality.max_book_age_ms;
    let forward = prepare_candidate(
        &repo,
        &base,
        &base_activations,
        kind,
        None,
        "rollback-forward",
        |candidate| {
            candidate.recommendation.data_quality.max_book_age_ms = base_book_age + 1;
        },
    )
    .await;
    let forward_commit = repo
        .activate_resource(forward.activation, forward.snapshot)
        .await
        .expect("activate forward revision");
    let forward_bundle = forward_commit.bundle;
    let forward_activations = repo
        .load_current_activations()
        .await
        .expect("load activations before rollback");
    let rollback = prepare_candidate(
        &repo,
        &forward_bundle,
        &forward_activations,
        kind,
        Some(base_revision_id),
        "rollback-to-history",
        |candidate| {
            candidate.recommendation.data_quality.max_book_age_ms = base_book_age;
        },
    )
    .await;
    assert_eq!(rollback.snapshot.snapshot_hash, base.snapshot_hash);
    assert_ne!(
        rollback.snapshot.decision_policy_snapshot_id,
        base.decision_policy_snapshot_id
    );
    let rollback_commit = repo
        .activate_resource(rollback.activation, rollback.snapshot)
        .await
        .expect("activate rollback with historical content hash");
    assert_eq!(rollback_commit.bundle.snapshot_hash, base.snapshot_hash);
    assert_eq!(
        rollback_commit.bundle.generation,
        forward_bundle
            .generation
            .checked_next()
            .expect("rollback generation")
    );
    assert_ne!(
        rollback_commit.bundle.decision_policy_snapshot_id,
        base.decision_policy_snapshot_id
    );
    let matching_history = repo
        .list_snapshots(10)
        .await
        .expect("list snapshot lineage")
        .into_iter()
        .filter(|snapshot| snapshot.snapshot_hash == base.snapshot_hash)
        .collect::<Vec<_>>();
    assert_eq!(matching_history.len(), 2);
    assert_ne!(
        matching_history[0].decision_policy_snapshot_id,
        matching_history[1].decision_policy_snapshot_id
    );
    assert_ne!(
        matching_history[0].bundle_generation,
        matching_history[1].bundle_generation
    );
}

pub async fn concurrent_resource_activations_fail_stale_then_rebase_without_lost_updates() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    bootstrap_default_policy_bundle(
        &db,
        "policy-concurrency-it",
        "bootstrap concurrent policy bundle",
    )
    .await;
    let repo = PgPolicyRepository::new(db.clone());
    let base = repo
        .load_current_bundle()
        .await
        .expect("load base bundle")
        .expect("boot bundle exists");
    let base_activations = repo
        .load_current_activations()
        .await
        .expect("load base activations");
    let expected_book_age = base.snapshot.recommendation.data_quality.max_book_age_ms + 17;
    let expected_open_intents = base.snapshot.execution_risk.capital.max_open_intents + 1;

    let recommendation = prepare_candidate(
        &repo,
        &base,
        &base_activations,
        ConfigResourceKind::RecommendationPolicy,
        None,
        "recommendation-concurrent",
        |candidate| {
            candidate.recommendation.data_quality.max_book_age_ms = expected_book_age;
        },
    )
    .await;
    let execution = prepare_candidate(
        &repo,
        &base,
        &base_activations,
        ConfigResourceKind::ExecutionRiskPolicy,
        None,
        "execution-concurrent",
        |candidate| {
            candidate.execution_risk.capital.max_open_intents = expected_open_intents;
        },
    )
    .await;
    assert_eq!(
        repo.count_valid_approvals()
            .await
            .expect("count base-generation approvals")
            .values()
            .sum::<u64>(),
        2
    );

    let (winner, stale) = activate_concurrent_candidates(&db, &recommendation, &execution).await;
    assert_eq!(winner.1.outcome, PolicyActivationOutcome::Committed);
    assert!(matches!(
        stale.1,
        StorageError::StateConflict { entity, .. } if entity == "policy_activation_guard"
    ));

    let after_first = repo
        .load_current_bundle()
        .await
        .expect("load first concurrent winner")
        .expect("winner bundle exists");
    assert_first_concurrent_bundle(
        &base,
        &after_first,
        winner.0,
        expected_book_age,
        expected_open_intents,
    );
    assert!(
        repo.count_valid_approvals()
            .await
            .expect("stale approvals are not actionable")
            .is_empty(),
        "approval inventory must exclude candidates bound to an old bundle generation"
    );

    let current_activations = repo
        .load_current_activations()
        .await
        .expect("load activations before rebase");
    let stale_candidate = if stale.0 == ConfigResourceKind::RecommendationPolicy {
        &recommendation
    } else {
        &execution
    };
    let rebased = prepare_candidate(
        &repo,
        &after_first,
        &current_activations,
        stale.0,
        Some(stale_candidate.revision_id.clone()),
        "rebased-concurrent-loser",
        |candidate| match stale.0 {
            ConfigResourceKind::RecommendationPolicy => {
                candidate.recommendation.data_quality.max_book_age_ms = expected_book_age;
            }
            ConfigResourceKind::ExecutionRiskPolicy => {
                candidate.execution_risk.capital.max_open_intents = expected_open_intents;
            }
            other => panic!("unexpected concurrent candidate kind: {other:?}"),
        },
    )
    .await;
    let mut old_approval_replay = rebased.activation.clone();
    old_approval_replay.policy_activation_id = PolicyActivationId::from_v7();
    old_approval_replay.policy_approval_id = stale_candidate.approval_id.clone();
    old_approval_replay.idempotency_key = "stale-approval-after-revalidation"
        .parse::<PolicyIdempotencyKey>()
        .expect("valid stale approval replay key");
    old_approval_replay.activation_request_hash = CanonicalDigest::content_hash_json(&(
        "stale-approval-after-revalidation",
        stale_candidate.approval_id.to_string(),
        after_first.generation.get(),
    ))
    .expect("hash stale approval replay");
    assert!(matches!(
        repo.activate_resource(old_approval_replay, rebased.snapshot.clone())
            .await,
        Err(StorageError::StateConflict { entity, .. }) if entity == "policy_approval"
    ));
    let second = repo
        .activate_resource(rebased.activation, rebased.snapshot)
        .await
        .expect("activate loser after explicit revalidation against committed bundle");
    assert_merged_concurrent_bundle(
        &second,
        &after_first,
        expected_book_age,
        expected_open_intents,
    );
}

async fn activate_concurrent_candidates(
    db: &DatabaseConnection,
    recommendation: &PreparedCandidate,
    execution: &PreparedCandidate,
) -> (
    (ConfigResourceKind, PolicyActivationCommit),
    (ConfigResourceKind, StorageError),
) {
    let recommendation_repo = PgPolicyRepository::new(db.clone());
    let execution_repo = PgPolicyRepository::new(db.clone());
    let (recommendation_result, execution_result) = tokio::join!(
        recommendation_repo.activate_resource(
            recommendation.activation.clone(),
            recommendation.snapshot.clone(),
        ),
        execution_repo.activate_resource(execution.activation.clone(), execution.snapshot.clone()),
    );
    match (recommendation_result, execution_result) {
        (Ok(winner), Err(stale)) => (
            (ConfigResourceKind::RecommendationPolicy, winner),
            (ConfigResourceKind::ExecutionRiskPolicy, stale),
        ),
        (Err(stale), Ok(winner)) => (
            (ConfigResourceKind::ExecutionRiskPolicy, winner),
            (ConfigResourceKind::RecommendationPolicy, stale),
        ),
        results => panic!("exactly one same-generation activation must commit: {results:?}"),
    }
}

struct PreparedCandidate {
    activation: NewPolicyActivation,
    approval_id: PolicyApprovalId,
    revision_id: PolicyRevisionId,
    snapshot: NewDecisionPolicySnapshot,
}

async fn prepare_candidate(
    repo: &PgPolicyRepository,
    base: &ActivePolicyBundle,
    active: &[PolicyActivationInfo],
    kind: ConfigResourceKind,
    existing_revision_id: Option<PolicyRevisionId>,
    key: &str,
    mutate: impl FnOnce(&mut DecisionPolicySnapshot),
) -> PreparedCandidate {
    let mut candidate = base.snapshot.clone();
    mutate(&mut candidate);
    let revision_id = existing_revision_id
        .clone()
        .unwrap_or_else(PolicyRevisionId::from_v7);
    candidate.set_resource_revision_id(kind, revision_id.clone());
    let document = candidate.resource_document(kind);
    let revision_hash =
        CanonicalDigest::content_hash_json(&document).expect("hash candidate resource document");
    if existing_revision_id.is_none() {
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
            created_by_label: "policy-concurrency-it".to_owned(),
            reason: format!("prepare {key}"),
        })
        .await
        .expect("create concurrent candidate revision");
    }
    let preflight_token_hash = CanonicalDigest::content_hash_json(&(
        "policy-concurrency-preflight",
        key,
        revision_hash.as_str(),
    ))
    .expect("hash candidate preflight token");
    let candidate_hash = candidate
        .persistence_hash()
        .expect("hash complete concurrent candidate bundle");
    repo.mark_revision_validated(
        &revision_id,
        PolicyValidationEvidence {
            subject: Some(PolicyValidationSubject {
                base_generation: base.generation,
                base_revision_vector: base.revision_vector.clone(),
                candidate_bundle_hash: candidate_hash,
            }),
            ..PolicyValidationEvidence::default()
        },
        preflight_token_hash.clone(),
        Utc::now() + Duration::minutes(10),
    )
    .await
    .expect("validate concurrent candidate revision");
    let approval_id = PolicyApprovalId::from_v7();
    repo.record_approval(RecordPolicyApproval {
        policy_approval_id: approval_id.clone(),
        policy_revision_id: revision_id.clone(),
        resource_kind: kind,
        decision: PolicyApprovalDecision::Approved,
        decided_by_kind: PolicyActorKind::System,
        decided_by_user_id: None,
        decided_by_label: "policy-concurrency-it".to_owned(),
        reason: format!("approve {key}"),
        decided_at: Utc::now(),
        expires_at: None,
    })
    .await
    .expect("approve concurrent candidate revision");
    let next_generation = base.generation.checked_next().expect("next generation");
    let snapshot_id = DecisionPolicySnapshotId::from_v7();
    let snapshot = persisted_snapshot(next_generation, snapshot_id.clone(), &candidate);
    let activation = activation(ActivationFixture {
        bundle_generation: next_generation,
        expected_bundle_generation: base.generation,
        kind,
        revision_id: revision_id.clone(),
        snapshot_id,
        approval_id: approval_id.clone(),
        expected_active_revision_id: Some(
            active
                .iter()
                .find(|row| row.resource_kind == kind)
                .expect("active revision for candidate resource")
                .policy_revision_id
                .clone(),
        ),
        preflight_token_hash,
        idempotency_key: key,
    });
    PreparedCandidate {
        activation,
        approval_id,
        revision_id,
        snapshot,
    }
}

fn persisted_snapshot(
    bundle_generation: PolicyBundleGeneration,
    snapshot_id: DecisionPolicySnapshotId,
    snapshot: &DecisionPolicySnapshot,
) -> NewDecisionPolicySnapshot {
    let snapshot_document = snapshot
        .persistence_document()
        .expect("build policy persistence document");
    NewDecisionPolicySnapshot {
        bundle_generation,
        decision_policy_snapshot_id: snapshot_id,
        snapshot_hash: CanonicalDigest::content_hash_json(&snapshot_document)
            .expect("hash policy persistence document"),
        recommendation_policy_revision_id: required_revision(
            snapshot,
            ConfigResourceKind::RecommendationPolicy,
        ),
        execution_risk_policy_revision_id: required_revision(
            snapshot,
            ConfigResourceKind::ExecutionRiskPolicy,
        ),
        model_routing_revision_id: required_revision(snapshot, ConfigResourceKind::ModelRouting),
        report_schedule_revision_id: required_revision(
            snapshot,
            ConfigResourceKind::ReportSchedule,
        ),
        operational_control_revision_id: required_revision(
            snapshot,
            ConfigResourceKind::OperationalControl,
        ),
        execution_authorization_revision_id: required_revision(
            snapshot,
            ConfigResourceKind::ExecutionAuthorization,
        ),
        snapshot: snapshot_document,
        source: DecisionPolicySnapshotSource::Activation,
        created_by_kind: PolicyActorKind::System,
        created_by_user_id: None,
        created_by_label: "policy-governance-it".to_owned(),
        reason: "freeze exact decision policy bundle".to_owned(),
    }
}

struct ActivationFixture<'a> {
    bundle_generation: PolicyBundleGeneration,
    expected_bundle_generation: PolicyBundleGeneration,
    kind: ConfigResourceKind,
    revision_id: PolicyRevisionId,
    snapshot_id: DecisionPolicySnapshotId,
    approval_id: PolicyApprovalId,
    expected_active_revision_id: Option<PolicyRevisionId>,
    preflight_token_hash: ContentHash,
    idempotency_key: &'a str,
}

fn activation(fixture: ActivationFixture<'_>) -> NewPolicyActivation {
    let idempotency_key = fixture
        .idempotency_key
        .parse::<PolicyIdempotencyKey>()
        .expect("valid policy idempotency key");
    NewPolicyActivation {
        bundle_generation: fixture.bundle_generation,
        expected_bundle_generation: fixture.expected_bundle_generation,
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
        activation_request_hash: CanonicalDigest::content_hash_json(&(
            "policy-governance-it-activation",
            idempotency_key.as_str(),
            fixture.bundle_generation.get(),
        ))
        .expect("hash activation request"),
        idempotency_key,
        audit_event_id: AuditEventId::from_v7(),
    }
}

fn required_revision(
    snapshot: &DecisionPolicySnapshot,
    kind: ConfigResourceKind,
) -> PolicyRevisionId {
    snapshot
        .resource_revision_id(kind)
        .cloned()
        .expect("complete policy revision bundle")
}
