//! Feedback-cycle repository contracts against a real `PostgreSQL` instance.

use std::time::Duration as StdDuration;

use quant_pivot_error::{
    feedback::FeedbackCycleCommandError,
    rbac::RbacError,
    storage::{StorageError, entity},
};
use quant_pivot_models::{
    domain::{
        api::{BuildTrainingDatasetRequest, DriftReportListQuery, FeedbackCycleListQuery},
        pagination::PageRequest,
        quant::{
            FeedbackCycleActor, FeedbackCycleKey, FeedbackCycleKeyInput, FeedbackCycleTerminal,
            FeedbackOutboxSource, FeedbackStageEventInput, FeedbackStageJobIdentity,
            GovernedFeedbackCancellation, GovernedFeedbackTrigger, NewFeedbackCycle,
            NewFeedbackSchedulerState, NewFeedbackStageEvent, NewResearchJob,
        },
        rbac::{AssignPermissions, AssignRoles, NewRole, NewUser, Permission},
    },
    entities::quant_feedback_cycle::Entity as QuantFeedbackCycleEntity,
    enums::rbac::{Operation, ResourceType, RoleKind, RoleStatus, UserStatus},
    enums::{
        model::ModelFamily,
        quant::{
            DatasetPurpose, FeedbackCycleStatus, FeedbackDecision, FeedbackDriftKind,
            FeedbackDriftMetric, FeedbackEvaluationMode, FeedbackStage, FeedbackStageEventKind,
            FeedbackTriggerFamily, ResearchJobKind, ResearchJobStatus,
        },
    },
    runtime_config::BuyModelRoute,
    types::{
        ContentHash, DecisionPolicySnapshotId, FeedbackCycleId, ModelSpecId, ModelVersionId,
        PolicyBundleGeneration, PolicyIdempotencyKey, ResearchJobId, ResearchJobParams,
        ResearchProfileId, RoleCode, RoleId, SchemaVersion, TrainingDatasetId,
        TrainingSampleSources, UserId, WorkerId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgFeedbackCycleRepository, PgFeedbackSchedulerRepository, PgResearchJobRepository,
        PgRolePermissionRepository, PgRoleRepository, PgUserRepository, PgUserRoleRepository,
    },
    traits::{
        DriftReportWriteOutcome, FeedbackCycleCasOutcome, FeedbackCycleClaim,
        FeedbackCycleClaimMode, FeedbackCycleGeneration, FeedbackCycleRepository,
        FeedbackCycleWriteOutcome, FeedbackEvaluationWriteOutcome, FeedbackOutboxRepository,
        FeedbackSchedulerRepository, FeedbackStageWriteOutcome, FeedbackTriggerCommit,
        FeedbackTriggerWriteOutcome, ResearchJobRepository, RolePermissionRepository,
        RoleRepository, UserRepository, UserRoleRepository,
    },
};
use quant_pivot_system_tests::postgres::setup_pg;
use sea_orm::{
    DatabaseConnection, DatabaseTransaction, EntityTrait, QuerySelect, TransactionTrait,
    sea_query::{LockBehavior, LockType},
};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use super::feedback_boot_schema::{FeedbackSchemaFixture, content_hash, prepare_fixture};

macro_rules! assert_cycle_conflict {
    ($error:expr) => {
        assert!(matches!(
            $error,
            StorageError::StateConflict {
                entity: owner,
                ..
            } if owner == entity::QUANT_FEEDBACK_CYCLE
        ));
    };
}

async fn cancellation_event(
    repository: &PgFeedbackCycleRepository,
    cycle_id: FeedbackCycleId,
    sequence: i64,
) -> NewFeedbackStageEvent {
    NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
        feedback_cycle_id: cycle_id,
        event_sequence: sequence,
        stage: FeedbackStage::Coverage,
        event_kind: FeedbackStageEventKind::CancellationRequested,
        trigger_family: None,
        research_job_id: None,
        actor: Some("operator".to_owned()),
        reason_code: Some("operator_cancelled".to_owned()),
        evidence_uri: None,
        evidence_hash: None,
        occurred_at: repository
            .database_time()
            .await
            .expect("read cancellation database time"),
    })
    .expect("seal cancellation request")
}

fn stage_event(
    fixture: &FeedbackSchemaFixture,
    job_id: ResearchJobId,
    sequence: i64,
    kind: FeedbackStageEventKind,
) -> NewFeedbackStageEvent {
    NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
        feedback_cycle_id: fixture.cycle_id,
        event_sequence: sequence,
        stage: FeedbackStage::Coverage,
        event_kind: kind,
        trigger_family: None,
        research_job_id: Some(job_id),
        actor: None,
        reason_code: None,
        evidence_uri: None,
        evidence_hash: None,
        occurred_at: fixture.observed_at,
    })
    .expect("seal worker stage event")
}

impl FeedbackSchemaFixture {
    fn coverage_job(&self) -> NewResearchJob {
        NewResearchJob {
            job_id: ResearchJobId::from_v7(),
            feedback_cycle_id: None,
            feedback_stage: None,
            kind: ResearchJobKind::DatasetBuild,
            status: ResearchJobStatus::Queued,
            model_spec_id: None,
            decision_policy_snapshot_id: None,
            params_json: ResearchJobParams::DatasetBuild(BuildTrainingDatasetRequest {
                model_spec_id: ModelSpecId::from_v7(),
                profile_ref: self.profile_ref.clone(),
                purpose: DatasetPurpose::Training,
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                window_start: self.evaluation_window_start,
                window_end: self.evaluation_window_end,
                pit_cutoff: self.label_cutoff,
                sample_interval_secs: 60,
                horizons_secs: vec![3_600],
                knowledge_lag_secs: 1,
                feature_schema_version: SchemaVersion::FIRST,
                sample_sources: TrainingSampleSources::default(),
                reason: "feedback-cycle-repository".to_owned(),
                training_dataset_id: Some(TrainingDatasetId::from_v7()),
                fit_seal_id: Uuid::now_v7().into(),
                fit_seal_hash: ContentHash::from_bytes([2; 32]),
            }),
            requested_by: None,
            acting_role: RoleCode::new("system"),
            parent_job_id: None,
            recovery_attempt: 0,
            max_recovery_attempts: 3,
        }
        .try_bind_feedback(
            FeedbackStageJobIdentity::try_root(self.cycle_id, FeedbackStage::Coverage)
                .expect("freeze feedback-stage job identity"),
        )
        .expect("bind feedback-stage job identity")
    }

    async fn assert_initial_outbox(&self, repo: &PgFeedbackCycleRepository) {
        let retry = repo
            .record_trigger(
                self.cycle.clone(),
                self.stage_event(self.cycle_id, "scheduler"),
            )
            .await
            .expect("retry exact trigger");
        assert!(matches!(
            retry,
            FeedbackTriggerCommit {
                cycle: FeedbackCycleWriteOutcome::AlreadyPresent(_),
                stage: FeedbackStageWriteOutcome::AlreadyPresent(_),
                trigger: FeedbackTriggerWriteOutcome::AlreadyPresent(_),
            }
        ));
        let replay = repo.list_outbox(0, 10).await.expect("list trigger outbox");
        assert_eq!(replay.len(), 2, "exact trigger retry cannot fork revision");
        assert!(
            replay
                .windows(2)
                .all(|pair| pair[0].revision < pair[1].revision)
        );
        assert_eq!(replay[0].source.feedback_cycle_id(), self.cycle_id);
        assert_eq!(replay[1].source.feedback_cycle_id(), self.second_cycle_id);
        assert!(
            replay
                .iter()
                .all(|entry| entry.profile_id == self.profile_ref.id)
        );
        let snapshot = repo.queue_snapshot().await.expect("read queue snapshot");
        assert_eq!((snapshot.queued, snapshot.running), (2, 0));
        assert_eq!(snapshot.pending_outbox, 2);
        assert!(snapshot.oldest_queued_at.is_some());
        assert!(snapshot.oldest_running_at.is_none());
    }
}

async fn record_cycles(repo: &PgFeedbackCycleRepository, fixture: &FeedbackSchemaFixture) {
    let first = repo
        .record_trigger(
            fixture.cycle.clone(),
            fixture.stage_event(fixture.cycle_id, "scheduler"),
        )
        .await
        .expect("record first trigger");
    assert!(matches!(
        first,
        FeedbackTriggerCommit {
            cycle: FeedbackCycleWriteOutcome::Inserted(_),
            stage: FeedbackStageWriteOutcome::Inserted(_),
            trigger: FeedbackTriggerWriteOutcome::Inserted(_),
        }
    ));
    let second = repo
        .record_trigger(
            fixture.second_cycle.clone(),
            fixture.stage_event(fixture.second_cycle_id, "operator"),
        )
        .await
        .expect("record second trigger");
    assert!(matches!(
        second,
        FeedbackTriggerCommit {
            cycle: FeedbackCycleWriteOutcome::Inserted(_),
            stage: FeedbackStageWriteOutcome::Inserted(_),
            trigger: FeedbackTriggerWriteOutcome::Inserted(_),
        }
    ));
}

async fn feedback_actor(
    db: &DatabaseConnection,
    code: &str,
    operations: &[Operation],
) -> FeedbackCycleActor {
    let users = PgUserRepository::new(db.clone());
    let roles = PgRoleRepository::new(db.clone());
    let memberships = PgUserRoleRepository::new(db.clone());
    let permissions = PgRolePermissionRepository::new(db.clone());
    let role = roles
        .create(NewRole {
            id: RoleId::from_v7(),
            code: RoleCode::new(code),
            name: code.to_owned(),
            description: None,
            kind: RoleKind::Custom,
            status: RoleStatus::Enabled,
            sort: 0,
        })
        .await
        .expect("create feedback role");
    permissions
        .set_permissions_for_role(AssignPermissions {
            role_id: role.id,
            permissions: operations
                .iter()
                .map(|operation| Permission::new(ResourceType::Materialization, *operation))
                .collect(),
        })
        .await
        .expect("assign feedback permissions");
    let user = users
        .create(NewUser {
            id: UserId::from_v7(),
            username: format!("{code}_user"),
            password_hash: "argon2id$feedback-test".to_owned(),
            nickname: format!("{code} user"),
            avatar: None,
            email: None,
            phone: None,
            status: UserStatus::Active,
        })
        .await
        .expect("create feedback actor");
    memberships
        .set_roles_for_user(AssignRoles {
            user_id: user.id,
            role_ids: vec![role.id],
        })
        .await
        .expect("assign feedback role");
    FeedbackCycleActor {
        user_id: user.id,
        acting_role: role.code,
    }
}

pub async fn trigger_exact_retry() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let repo = PgFeedbackCycleRepository::new(db);
    record_cycles(&repo, &fixture).await;

    let retry = repo
        .record_trigger(
            fixture.cycle.clone(),
            fixture.stage_event(fixture.cycle_id, "scheduler"),
        )
        .await
        .expect("retry exact trigger");
    assert!(matches!(
        retry,
        FeedbackTriggerCommit {
            cycle: FeedbackCycleWriteOutcome::AlreadyPresent(_),
            stage: FeedbackStageWriteOutcome::AlreadyPresent(_),
            trigger: FeedbackTriggerWriteOutcome::AlreadyPresent(_),
        }
    ));

    let converged = repo
        .record_trigger(
            fixture.cycle.clone(),
            fixture.stage_event(fixture.cycle_id, "different-actor"),
        )
        .await
        .expect("another provenance event must converge on the canonical cycle");
    assert!(matches!(
        converged,
        FeedbackTriggerCommit {
            cycle: FeedbackCycleWriteOutcome::AlreadyPresent(_),
            stage: FeedbackStageWriteOutcome::AlreadyPresent(_),
            trigger: FeedbackTriggerWriteOutcome::Inserted(_),
        }
    ));
    let provenance = repo
        .list_trigger_events(&fixture.cycle_id)
        .await
        .expect("list converged trigger provenance");
    assert_eq!(provenance.len(), 2);
    assert_eq!(provenance[0].actor_label, "scheduler");
    assert_eq!(provenance[1].actor_label, "different-actor");
}

async fn assert_trigger_contracts(
    repo: &PgFeedbackCycleRepository,
    fixture: &FeedbackSchemaFixture,
    owner: &FeedbackCycleActor,
    second_owner: &FeedbackCycleActor,
    reader: &FeedbackCycleActor,
) -> i64 {
    let trigger = GovernedFeedbackTrigger {
        actor: owner.clone(),
        cycle: fixture.cycle.clone(),
        idempotency_key: PolicyIdempotencyKey::parse("operator-retrain-owner")
            .expect("owner idempotency key"),
        reason_code: "operator_retrain".to_owned(),
    };

    assert!(matches!(
        repo.record_governed_trigger(GovernedFeedbackTrigger {
            actor: reader.clone(),
            cycle: fixture.cycle.clone(),
            idempotency_key: PolicyIdempotencyKey::parse("operator-retrain-reader")
                .expect("reader idempotency key"),
            reason_code: "operator_retrain".to_owned(),
        })
        .await,
        Err(FeedbackCycleCommandError::Authorization(
            RbacError::PermissionDenied { .. }
        ))
    ));
    assert!(
        repo.find_cycle(&fixture.cycle_id)
            .await
            .expect("check denied trigger")
            .is_none(),
        "RBAC denial must persist no cycle"
    );

    let before_trigger = repo.database_time().await.expect("read database time");
    let first = repo
        .record_governed_trigger(trigger.clone())
        .await
        .expect("record governed trigger");
    let FeedbackTriggerCommit {
        cycle: FeedbackCycleWriteOutcome::Inserted(triggered),
        stage: FeedbackStageWriteOutcome::Inserted(event),
        trigger: FeedbackTriggerWriteOutcome::Inserted(provenance),
    } = first
    else {
        panic!("first governed trigger must insert cycle, lifecycle, and provenance");
    };
    assert_eq!(
        event.actor.as_deref(),
        Some("feedback_owner_user@feedback_owner")
    );
    assert_eq!(event.reason_code.as_deref(), Some("operator_retrain"));
    assert_eq!(provenance.reason_code, "operator_retrain");
    assert!(event.occurred_at >= before_trigger);

    sleep(StdDuration::from_millis(2)).await;
    assert!(matches!(
        repo.record_governed_trigger(trigger.clone())
            .await
            .expect("retry governed trigger across database time"),
        FeedbackTriggerCommit {
            cycle: FeedbackCycleWriteOutcome::AlreadyPresent(_),
            stage: FeedbackStageWriteOutcome::AlreadyPresent(_),
            trigger: FeedbackTriggerWriteOutcome::AlreadyPresent(_),
        }
    ));
    assert!(matches!(
        repo.record_governed_trigger(GovernedFeedbackTrigger {
            actor: second_owner.clone(),
            cycle: fixture.cycle.clone(),
            idempotency_key: PolicyIdempotencyKey::parse("operator-retrain-second-owner")
                .expect("second owner idempotency key"),
            reason_code: "operator_retrain".to_owned(),
        })
        .await
        .expect("another authorized trigger converges on the cadence cycle"),
        FeedbackTriggerCommit {
            cycle: FeedbackCycleWriteOutcome::AlreadyPresent(_),
            stage: FeedbackStageWriteOutcome::AlreadyPresent(_),
            trigger: FeedbackTriggerWriteOutcome::Inserted(_),
        }
    ));
    triggered.generation
}

async fn assert_cancel_contracts(
    repo: &PgFeedbackCycleRepository,
    fixture: &FeedbackSchemaFixture,
    owner: &FeedbackCycleActor,
    reader: &FeedbackCycleActor,
    triggered_generation: i64,
) {
    let cancellation = GovernedFeedbackCancellation {
        actor: owner.clone(),
        feedback_cycle_id: fixture.cycle_id,
        expected_generation: triggered_generation,
        expected_event_sequence: 2,
        stage: FeedbackStage::TruthFreeze,
        reason_code: "operator_cancelled".to_owned(),
    };
    assert!(matches!(
        repo.request_governed_cancel(GovernedFeedbackCancellation {
            actor: reader.clone(),
            ..cancellation.clone()
        })
        .await,
        Err(FeedbackCycleCommandError::Authorization(
            RbacError::PermissionDenied { .. }
        ))
    ));
    let first_cancel = repo
        .request_governed_cancel(cancellation.clone())
        .await
        .expect("request governed cancellation");
    let (
        FeedbackCycleCasOutcome::Applied(cancelled),
        FeedbackStageWriteOutcome::Inserted(cancel_event),
    ) = first_cancel
    else {
        panic!("first governed cancellation must apply");
    };
    assert_eq!(cancelled.status, FeedbackCycleStatus::Cancelled);
    assert_eq!(cancelled.generation, triggered_generation + 1);
    assert_eq!(
        cancel_event.actor.as_deref(),
        Some("feedback_owner_user@feedback_owner")
    );
    assert_eq!(
        cancel_event.reason_code.as_deref(),
        Some("operator_cancelled")
    );
    assert!(matches!(
        repo.request_governed_cancel(cancellation)
            .await
            .expect("retry governed cancellation"),
        (
            FeedbackCycleCasOutcome::AlreadyApplied(_),
            FeedbackStageWriteOutcome::AlreadyPresent(_)
        )
    ));
    let events = repo
        .list_stage_events(&fixture.cycle_id)
        .await
        .expect("read governed timeline");
    let triggers = repo
        .list_trigger_events(&fixture.cycle_id)
        .await
        .expect("read governed trigger provenance");
    let outbox = repo.list_outbox(0, 10).await.expect("read governed outbox");
    assert_eq!((events.len(), outbox.len()), (2, 3));
    assert_eq!(triggers.len(), 2);
    let [
        first_trigger_revision,
        second_trigger_revision,
        cancellation_revision,
    ] = outbox.as_slice()
    else {
        panic!("governed trigger/cancellation outbox cardinality changed");
    };
    let FeedbackOutboxSource::Trigger(first_trigger) = &first_trigger_revision.source else {
        panic!("initial governed trigger must publish trigger provenance");
    };
    let FeedbackOutboxSource::Trigger(second_trigger) = &second_trigger_revision.source else {
        panic!("second governed actor must publish distinct trigger provenance");
    };
    let FeedbackOutboxSource::Stage(cancellation_event) = &cancellation_revision.source else {
        panic!("governed cancellation must publish lifecycle stage evidence");
    };
    assert_eq!(
        (
            first_trigger.actor_label.as_str(),
            second_trigger.actor_label.as_str(),
            cancellation_event.event_kind,
        ),
        (
            "feedback_owner_user",
            "feedback_backup_user",
            FeedbackStageEventKind::CancellationRequested,
        )
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_sequence)
            .collect::<Vec<_>>(),
        [1, 2]
    );
}

pub async fn governed_mutation_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let repo = PgFeedbackCycleRepository::new(db.clone());
    let profile = fixture
        .profile_ref
        .resolve_builtin_research_profile()
        .expect("resolve governed trigger profile");
    let database_now = repo.database_time().await.expect("read scheduler clock");
    PgFeedbackSchedulerRepository::new(db.clone())
        .sync_state(
            NewFeedbackSchedulerState::try_new(&profile, database_now)
                .expect("derive governed scheduler state"),
        )
        .await
        .expect("persist governed scheduler state");
    let owner = feedback_actor(&db, "feedback_owner", &[Operation::Create]).await;
    let second_owner = feedback_actor(&db, "feedback_backup", &[Operation::Create]).await;
    let reader = feedback_actor(&db, "feedback_reader", &[Operation::Read]).await;
    let triggered_generation =
        assert_trigger_contracts(&repo, &fixture, &owner, &second_owner, &reader).await;
    assert_cancel_contracts(&repo, &fixture, &owner, &reader, triggered_generation).await;
}

fn forced_cycle(
    fixture: &FeedbackSchemaFixture,
    parent_cycle_id: FeedbackCycleId,
    idempotency_key: PolicyIdempotencyKey,
) -> NewFeedbackCycle {
    let profile = fixture
        .profile_ref
        .resolve_builtin_research_profile()
        .expect("resolve forced-child profile");
    let evaluation = fixture.candidate_family.shared_evaluation();
    NewFeedbackCycle::try_seal(
        FeedbackCycleKey::try_new(FeedbackCycleKeyInput {
            profile_ref: fixture.profile_ref.clone(),
            feedback_policy_hash: profile
                .spec
                .feedback_policy
                .content_hash()
                .expect("hash forced-child feedback policy"),
            label_cutoff: fixture.label_cutoff,
            champion_model_version_id: fixture.champion_model_version_id,
            champion_serving_contract_hash: fixture.champion_serving_contract_hash,
            champion_model_spec_id: evaluation.model_spec_id,
            champion_model_spec_definition_hash: evaluation.model_spec_definition_hash,
            champion_model_family: ModelFamily::WeightedFactor,
            route: BuyModelRoute::Pooled,
            decision_policy_snapshot_id: evaluation.source_lineage.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: evaluation.source_lineage.runtime_config_hash,
            policy_bundle_generation: PolicyBundleGeneration::FIRST,
            route_generation: 1,
            evaluation_mode: FeedbackEvaluationMode::ForcedRetraining,
            parent_cycle_id: Some(parent_cycle_id),
            forced_idempotency_key: Some(idempotency_key),
        })
        .expect("freeze forced-child identity"),
    )
    .expect("seal forced-child cycle")
}

pub async fn forced_child_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let repository = PgFeedbackCycleRepository::new(db.clone());
    let profile = fixture
        .profile_ref
        .resolve_builtin_research_profile()
        .expect("resolve forced-child scheduler profile");
    PgFeedbackSchedulerRepository::new(db.clone())
        .sync_state(
            NewFeedbackSchedulerState::try_new(
                &profile,
                repository
                    .database_time()
                    .await
                    .expect("read forced-child scheduler clock"),
            )
            .expect("derive forced-child scheduler state"),
        )
        .await
        .expect("persist forced-child scheduler state");
    let owner = feedback_actor(&db, "forced_owner", &[Operation::Create]).await;
    repository
        .record_trigger(
            fixture.cycle.clone(),
            fixture.stage_event(fixture.cycle_id, "scheduler"),
        )
        .await
        .expect("persist conditional parent");

    let forced_key = PolicyIdempotencyKey::parse("forced-child-attempt-1")
        .expect("forced child idempotency key");
    let child = forced_cycle(&fixture, fixture.cycle_id, forced_key.clone());
    let premature = repository
        .record_governed_trigger(GovernedFeedbackTrigger {
            actor: owner.clone(),
            cycle: child.clone(),
            idempotency_key: forced_key.clone(),
            reason_code: "force_retraining".to_owned(),
        })
        .await
        .expect_err("running parent cannot spawn forced retraining");
    assert!(matches!(
        premature,
        FeedbackCycleCommandError::Storage(StorageError::StateConflict {
            entity: owner,
            ..
        }) if owner == entity::QUANT_FEEDBACK_CYCLE
    ));

    let claim = repository
        .claim_cycle(WorkerId::from_v7(), 30)
        .await
        .expect("claim conditional parent")
        .expect("conditional parent is queued");
    repository
        .finalize_cycle(
            claim.lease,
            FeedbackCycleTerminal::try_succeeded(
                FeedbackDecision::NoAction,
                "conditional_no_action".to_owned(),
            )
            .expect("seal parent NoAction terminal"),
        )
        .await
        .expect("finalize conditional parent NoAction");
    let scheduler_repository = PgFeedbackSchedulerRepository::new(db.clone());
    let scheduler_before = scheduler_repository
        .find_state(&fixture.profile_ref.id)
        .await
        .expect("load pre-forced scheduler state")
        .expect("pre-forced scheduler state exists");

    let first = repository
        .record_governed_trigger(GovernedFeedbackTrigger {
            actor: owner.clone(),
            cycle: child.clone(),
            idempotency_key: forced_key.clone(),
            reason_code: "force_retraining".to_owned(),
        })
        .await
        .expect("persist explicit forced child");
    let FeedbackTriggerCommit {
        cycle: FeedbackCycleWriteOutcome::Inserted(stored),
        stage: FeedbackStageWriteOutcome::Inserted(_),
        trigger: FeedbackTriggerWriteOutcome::Inserted(provenance),
    } = first
    else {
        panic!("first forced child must insert all durable identities");
    };
    assert_eq!(stored.parent_cycle_id, Some(fixture.cycle_id));
    assert_eq!(stored.forced_idempotency_key.as_ref(), Some(&forced_key));
    assert_eq!(
        stored.evaluation_mode,
        FeedbackEvaluationMode::ForcedRetraining
    );
    assert_eq!(stored.label_cutoff, fixture.label_cutoff);
    assert_eq!(
        provenance.evaluation_mode,
        FeedbackEvaluationMode::ForcedRetraining
    );
    assert_eq!(provenance.idempotency_key, forced_key);
    let scheduler_after = scheduler_repository
        .find_state(&fixture.profile_ref.id)
        .await
        .expect("load post-forced scheduler state")
        .expect("post-forced scheduler state exists");
    assert_eq!(scheduler_after.next_due_at, scheduler_before.next_due_at);
    assert_eq!(
        scheduler_after.last_cycle_id,
        scheduler_before.last_cycle_id
    );
    assert_eq!(scheduler_after.last_cutoff, scheduler_before.last_cutoff);
    assert!(scheduler_after.cooldown_until.is_some());

    assert!(matches!(
        repository
            .record_governed_trigger(GovernedFeedbackTrigger {
                actor: owner,
                cycle: child,
                idempotency_key: provenance.idempotency_key,
                reason_code: "force_retraining".to_owned(),
            })
            .await
            .expect("replay exact forced child"),
        FeedbackTriggerCommit {
            cycle: FeedbackCycleWriteOutcome::AlreadyPresent(_),
            stage: FeedbackStageWriteOutcome::AlreadyPresent(_),
            trigger: FeedbackTriggerWriteOutcome::AlreadyPresent(_),
        }
    ));
}

pub async fn read_page_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let repo = PgFeedbackCycleRepository::new(db);
    record_cycles(&repo, &fixture).await;

    let first = repo
        .page_cycles(FeedbackCycleListQuery {
            profile_id: Some(fixture.profile_ref.id.clone()),
            status: Some(FeedbackCycleStatus::Queued),
            trigger_family: None,
            page: PageRequest::new(1, 1),
        })
        .await
        .expect("page first feedback cycle");
    let second = repo
        .page_cycles(FeedbackCycleListQuery {
            profile_id: Some(fixture.profile_ref.id.clone()),
            status: Some(FeedbackCycleStatus::Queued),
            trigger_family: None,
            page: PageRequest::new(2, 1),
        })
        .await
        .expect("page second feedback cycle");
    assert_eq!((first.total, first.page, first.size), (2, 1, 1));
    assert!(first.has_next);
    assert_eq!((second.total, second.page, second.size), (2, 2, 1));
    assert!(!second.has_next);
    assert_ne!(
        first.items[0].feedback_cycle_id,
        second.items[0].feedback_cycle_id
    );

    let manual = repo
        .page_cycles(FeedbackCycleListQuery {
            profile_id: None,
            status: None,
            trigger_family: Some(FeedbackTriggerFamily::Manual),
            page: PageRequest::default(),
        })
        .await
        .expect("filter manual feedback cycle");
    assert_eq!(manual.total, 1);
    assert_eq!(manual.items[0].feedback_cycle_id, fixture.second_cycle_id);

    let missing = repo
        .page_cycles(FeedbackCycleListQuery {
            profile_id: Some(ResearchProfileId::new("missing_profile")),
            status: None,
            trigger_family: None,
            page: PageRequest::new(0, 1_000),
        })
        .await
        .expect("filter missing feedback profile");
    assert!(missing.items.is_empty());
    assert_eq!((missing.total, missing.page, missing.size), (0, 1, 100));

    let outbox = repo
        .list_outbox(0, 10)
        .await
        .expect("read durable revisions");
    assert_eq!(
        repo.latest_outbox_revision()
            .await
            .expect("read latest durable revision"),
        outbox.last().expect("trigger outbox is non-empty").revision
    );
}

pub async fn outbox_delivery_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let repo = PgFeedbackCycleRepository::new(db.clone());
    record_cycles(&repo, &fixture).await;
    fixture.assert_initial_outbox(&repo).await;

    let worker_a = WorkerId::from_v7();
    let worker_b = WorkerId::from_v7();
    let first = repo
        .claim_outbox(worker_a, 30, 1)
        .await
        .expect("claim first revision");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].publish_attempts, 1);
    let second = repo
        .claim_outbox(worker_b, 30, 1)
        .await
        .expect("claim next unlocked revision");
    assert_eq!(second.len(), 1);
    assert!(second[0].revision > first[0].revision);
    assert!(matches!(
        repo.publish_outbox(first[0].revision, worker_b)
            .await
            .expect_err("wrong owner cannot publish"),
        StorageError::StateConflict {
            entity: owner,
            ..
        } if owner == entity::QUANT_FEEDBACK_EVENT_OUTBOX
    ));

    repo.fail_outbox(
        first[0].revision,
        worker_a,
        "transient downstream failure".to_owned(),
    )
    .await
    .expect("release failed first delivery");
    repo.fail_outbox(
        first[0].revision,
        worker_a,
        "transient downstream failure".to_owned(),
    )
    .await
    .expect("retry exact failure result");
    let retried = repo
        .claim_outbox(worker_a, 30, 1)
        .await
        .expect("reclaim failed revision");
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].revision, first[0].revision);
    assert_eq!(retried[0].publish_attempts, 2);
    repo.publish_outbox(retried[0].revision, worker_a)
        .await
        .expect("publish reclaimed revision");
    repo.publish_outbox(retried[0].revision, worker_a)
        .await
        .expect("retry exact publish");
    repo.publish_outbox(second[0].revision, worker_b)
        .await
        .expect("publish second revision");
    assert!(
        repo.claim_outbox(WorkerId::from_v7(), 30, 10)
            .await
            .expect("claim empty published queue")
            .is_empty()
    );

    let claim = repo
        .claim_cycle(WorkerId::from_v7(), 30)
        .await
        .expect("claim cycle for stage append")
        .expect("queued cycle exists");
    let job = fixture.coverage_job();
    let job_id = job.job_id;
    PgResearchJobRepository::new(db)
        .enqueue(job)
        .await
        .expect("persist stage job");
    let event = stage_event(&fixture, job_id, 2, FeedbackStageEventKind::Started);
    assert!(matches!(
        repo.append_stage(claim.lease, event.clone())
            .await
            .expect("append stage with outbox"),
        FeedbackStageWriteOutcome::Inserted(_)
    ));
    assert!(matches!(
        repo.append_stage(claim.lease, event)
            .await
            .expect("retry exact stage append"),
        FeedbackStageWriteOutcome::AlreadyPresent(_)
    ));
    let stage_replay = repo
        .list_outbox(second[0].revision, 10)
        .await
        .expect("replay stage revision");
    assert_eq!(stage_replay.len(), 1);
    let FeedbackOutboxSource::Stage(stage_event) = &stage_replay[0].source else {
        panic!("non-trigger lifecycle append must publish a stage-source invalidation");
    };
    assert_eq!(stage_event.research_job_id, Some(job_id));
    assert_eq!(stage_event.event_kind, FeedbackStageEventKind::Started);
    assert_eq!(stage_replay[0].profile_id, fixture.profile_ref.id);
    assert_eq!(
        repo.queue_snapshot()
            .await
            .expect("read post-stage queue snapshot")
            .pending_outbox,
        1
    );

    for error in [
        repo.list_outbox(-1, 1)
            .await
            .expect_err("negative revision cursor must fail closed"),
        repo.list_outbox(0, 0)
            .await
            .expect_err("zero replay limit must fail closed"),
        repo.claim_outbox(WorkerId::from_v7(), 30, 1_001)
            .await
            .expect_err("oversized claim must fail closed"),
    ] {
        assert!(matches!(
            error,
            StorageError::InvariantViolation {
                entity: Some(owner),
                ..
            } if owner == entity::QUANT_FEEDBACK_EVENT_OUTBOX
        ));
    }
}

async fn lock_cycle(db: &DatabaseConnection, cycle_id: FeedbackCycleId) -> DatabaseTransaction {
    let transaction = db.begin().await.expect("begin competing transaction");
    let locked = QuantFeedbackCycleEntity::find_by_id(cycle_id)
        .lock_with_behavior(LockType::Update, LockBehavior::Nowait)
        .one(&transaction)
        .await
        .expect("lock oldest feedback cycle");
    assert!(locked.is_some(), "cycle selected for lock must exist");
    transaction
}

pub async fn skip_locked_claims() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let repo = PgFeedbackCycleRepository::new(db.clone());
    record_cycles(&repo, &fixture).await;
    let lock = lock_cycle(&db, fixture.cycle_id).await;

    let worker = WorkerId::from_v7();
    let claimed = timeout(StdDuration::from_secs(2), repo.claim_cycle(worker, 30))
        .await
        .expect("SKIP LOCKED claim must not wait")
        .expect("claim unlocked cycle")
        .expect("one unlocked cycle is eligible");
    assert_eq!(claimed.mode, FeedbackCycleClaimMode::Started);
    assert_eq!(claimed.cycle.feedback_cycle_id, fixture.second_cycle_id);
    lock.rollback().await.expect("release cycle lock");

    let contender_a = PgFeedbackCycleRepository::new(db.clone());
    let contender_b = PgFeedbackCycleRepository::new(db);
    let worker_a = WorkerId::from_v7();
    let worker_b = WorkerId::from_v7();
    let (claim_a, claim_b) = tokio::join!(
        contender_a.claim_cycle(worker_a, 30),
        contender_b.claim_cycle(worker_b, 30),
    );
    let claims = [claim_a.expect("claim A"), claim_b.expect("claim B")]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(claims.len(), 1, "one queued cycle has one lease winner");
    assert_eq!(claims[0].cycle.feedback_cycle_id, fixture.cycle_id);
}

pub async fn lease_cas_recovery() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let repo = PgFeedbackCycleRepository::new(db);
    record_cycles(&repo, &fixture).await;

    let owner_a = WorkerId::from_v7();
    let first = repo
        .claim_cycle(owner_a, 1)
        .await
        .expect("claim cycle")
        .expect("queued cycle");
    sleep(StdDuration::from_millis(1_100)).await;

    let owner_b = WorkerId::from_v7();
    let recovered = repo
        .claim_cycle(owner_b, 30)
        .await
        .expect("recover expired cycle")
        .expect("expired cycle");
    assert_eq!(recovered.mode, FeedbackCycleClaimMode::LeaseRecovered);
    assert_eq!(
        recovered.cycle.feedback_cycle_id,
        first.cycle.feedback_cycle_id
    );
    assert_eq!(recovered.cycle.generation, first.cycle.generation + 1);

    assert_cycle_conflict!(
        repo.renew_cycle_lease(first.lease, 30)
            .await
            .expect_err("stale owner cannot renew")
    );
    let terminal =
        FeedbackCycleTerminal::try_succeeded(FeedbackDecision::NoAction, "no_action".to_owned())
            .expect("valid successful terminal");
    assert_cycle_conflict!(
        repo.finalize_cycle(first.lease, terminal.clone())
            .await
            .expect_err("stale owner cannot finalize")
    );

    let renewed = repo
        .renew_cycle_lease(recovered.lease, 30)
        .await
        .expect("renew current lease");
    let cancel_event = cancellation_event(&repo, renewed.feedback_cycle_id, 2).await;
    let cancel = repo
        .request_cancel(
            FeedbackCycleGeneration::from(&renewed),
            cancel_event.clone(),
        )
        .await
        .expect("request running cancellation");
    let (FeedbackCycleCasOutcome::Applied(cancelled), FeedbackStageWriteOutcome::Inserted(_)) =
        cancel
    else {
        panic!("first cancellation request must atomically apply");
    };
    assert_eq!(cancelled.status, FeedbackCycleStatus::Running);
    assert!(cancelled.cancel_requested_at.is_some());

    let retry = repo
        .request_cancel(FeedbackCycleGeneration::from(&renewed), cancel_event)
        .await
        .expect("retry cancellation request");
    assert!(matches!(
        retry,
        (
            FeedbackCycleCasOutcome::AlreadyApplied(_),
            FeedbackStageWriteOutcome::AlreadyPresent(_)
        )
    ));

    let current_lease = recovered.lease.with_generation(cancelled.generation);
    let finalized = repo
        .finalize_cycle(
            current_lease,
            FeedbackCycleTerminal::try_cancelled("operator_cancelled".to_owned())
                .expect("valid cancellation terminal"),
        )
        .await
        .expect("terminalize cancelled cycle");
    assert!(matches!(finalized, FeedbackCycleCasOutcome::Applied(_)));

    let queued_cancel = cancellation_event(&repo, fixture.second_cycle_id, 2).await;
    let queued = repo
        .find_cycle(&fixture.second_cycle_id)
        .await
        .expect("load queued cycle")
        .expect("queued cycle exists");
    let result = repo
        .request_cancel(
            FeedbackCycleGeneration::from(&queued),
            queued_cancel.clone(),
        )
        .await
        .expect("cancel queued cycle");
    let (FeedbackCycleCasOutcome::Applied(cancelled), FeedbackStageWriteOutcome::Inserted(_)) =
        result
    else {
        panic!("first queued cancellation must atomically apply");
    };
    assert_eq!(cancelled.status, FeedbackCycleStatus::Cancelled);
    assert!(matches!(
        repo.request_cancel(FeedbackCycleGeneration::from(&queued), queued_cancel)
            .await
            .expect("retry queued cancellation"),
        (
            FeedbackCycleCasOutcome::AlreadyApplied(_),
            FeedbackStageWriteOutcome::AlreadyPresent(_)
        )
    ));
    assert!(
        repo.claim_cycle(WorkerId::from_v7(), 30)
            .await
            .expect("claim after queued cancellation")
            .is_none(),
        "terminal queued cancellation cannot be claimed"
    );
}

async fn running_cycle(
    repo: &PgFeedbackCycleRepository,
    fixture: &FeedbackSchemaFixture,
) -> FeedbackCycleClaim {
    record_cycles(repo, fixture).await;
    repo.claim_cycle(WorkerId::from_v7(), 30)
        .await
        .expect("claim feedback cycle")
        .expect("queued feedback cycle")
}

async fn stage_append_contracts(
    repo: &PgFeedbackCycleRepository,
    fixture: &FeedbackSchemaFixture,
    claim: &FeedbackCycleClaim,
    job_id: ResearchJobId,
) {
    let stage = stage_event(fixture, job_id, 2, FeedbackStageEventKind::Started);
    assert!(matches!(
        repo.append_stage(claim.lease, stage.clone())
            .await
            .expect("append stage"),
        FeedbackStageWriteOutcome::Inserted(_)
    ));
    assert!(matches!(
        repo.append_stage(claim.lease, stage)
            .await
            .expect("retry stage"),
        FeedbackStageWriteOutcome::AlreadyPresent(_)
    ));
    let stage_conflict = stage_event(fixture, job_id, 2, FeedbackStageEventKind::JobLinked);
    assert!(matches!(
        repo.append_stage(claim.lease, stage_conflict)
            .await
            .expect_err("sequence conflict must fail"),
        StorageError::StateConflict {
            entity: owner,
            ..
        } if owner == entity::QUANT_FEEDBACK_STAGE_EVENT
    ));
}

async fn drift_append_contracts(
    repo: &PgFeedbackCycleRepository,
    fixture: &FeedbackSchemaFixture,
    claim: &FeedbackCycleClaim,
) {
    let drift = fixture.drift_report(
        fixture.label_cutoff,
        rust_decimal_macros::dec!(0.20),
        content_hash('1'),
    );
    assert!(matches!(
        repo.append_drift(claim.lease, drift.clone())
            .await
            .expect("append drift"),
        DriftReportWriteOutcome::Inserted(_)
    ));
    assert!(matches!(
        repo.append_drift(claim.lease, drift)
            .await
            .expect("retry drift"),
        DriftReportWriteOutcome::AlreadyPresent(_)
    ));
    let drift_conflict = fixture.drift_report(
        fixture.label_cutoff,
        rust_decimal_macros::dec!(0.25),
        content_hash('2'),
    );
    assert!(matches!(
        repo.append_drift(claim.lease, drift_conflict)
            .await
            .expect_err("one metric cannot bind different evidence"),
        StorageError::StateConflict {
            entity: owner,
            ..
        } if owner == entity::QUANT_DRIFT_REPORT
    ));
}

async fn evaluation_contracts(
    repo: &PgFeedbackCycleRepository,
    fixture: &FeedbackSchemaFixture,
    claim: &FeedbackCycleClaim,
) {
    let evaluation = fixture.evaluation_use(
        fixture.cycle_id,
        fixture.candidate_family_hash,
        fixture.evaluation_dataset_hash,
        content_hash('3'),
    );
    assert!(matches!(
        repo.append_evaluation(claim.lease, evaluation.clone())
            .await
            .expect("append evaluation use"),
        FeedbackEvaluationWriteOutcome::Inserted(_)
    ));
    assert!(matches!(
        repo.append_evaluation(claim.lease, evaluation)
            .await
            .expect("retry evaluation use"),
        FeedbackEvaluationWriteOutcome::AlreadyPresent(_)
    ));
    let second_claim = repo
        .claim_cycle(WorkerId::from_v7(), 30)
        .await
        .expect("claim second feedback cycle")
        .expect("second feedback cycle remains queued");
    assert_eq!(
        second_claim.cycle.feedback_cycle_id,
        fixture.second_cycle_id
    );
    let reused = fixture.evaluation_use(
        fixture.second_cycle_id,
        fixture.second_candidate_family_hash,
        fixture.evaluation_dataset_hash,
        content_hash('4'),
    );
    assert!(matches!(
        repo.append_evaluation(second_claim.lease, reused)
            .await
            .expect_err("evaluation dataset cannot be reused"),
        StorageError::StateConflict {
            entity: owner,
            ..
        } if owner == entity::QUANT_FEEDBACK_EVALUATION_USE
    ));
}

async fn terminal_contracts(
    repo: &PgFeedbackCycleRepository,
    fixture: &FeedbackSchemaFixture,
    claim: &FeedbackCycleClaim,
    job_id: ResearchJobId,
) {
    let terminal = FeedbackCycleTerminal::try_succeeded(
        FeedbackDecision::ChallengerRejected,
        "challenger_rejected".to_owned(),
    )
    .expect("valid terminal");
    let finalized = repo
        .finalize_cycle(claim.lease, terminal.clone())
        .await
        .expect("finalize cycle");
    let FeedbackCycleCasOutcome::Applied(done) = finalized else {
        panic!("first finalize must apply");
    };
    assert_eq!(done.status, FeedbackCycleStatus::Succeeded);
    assert!(matches!(
        repo.finalize_cycle(claim.lease, terminal)
            .await
            .expect("retry exact terminal"),
        FeedbackCycleCasOutcome::AlreadyApplied(_)
    ));
    let post_terminal = stage_event(fixture, job_id, 3, FeedbackStageEventKind::JobLinked);
    assert_cycle_conflict!(
        repo.append_stage(claim.lease, post_terminal)
            .await
            .expect_err("terminal cycle cannot append new worker evidence")
    );
}

pub async fn evidence_append_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = Box::pin(prepare_fixture(&db)).await;
    let repo = PgFeedbackCycleRepository::new(db.clone());
    let claim = running_cycle(&repo, &fixture).await;
    assert_eq!(claim.cycle.feedback_cycle_id, fixture.cycle_id);

    let job = fixture.coverage_job();
    let job_id = job.job_id;
    PgResearchJobRepository::new(db)
        .enqueue(job)
        .await
        .expect("persist stage job");
    stage_append_contracts(&repo, &fixture, &claim, job_id).await;
    drift_append_contracts(&repo, &fixture, &claim).await;
    evaluation_contracts(&repo, &fixture, &claim).await;
    terminal_contracts(&repo, &fixture, &claim, job_id).await;

    let events = repo
        .list_stage_events(&fixture.cycle_id)
        .await
        .expect("read stage timeline");
    let reports = repo
        .list_drift_reports(&fixture.cycle_id)
        .await
        .expect("read drift reports");
    let uses = repo
        .list_evaluation_uses(&fixture.cycle_id)
        .await
        .expect("read evaluation uses");
    assert_eq!((events.len(), reports.len(), uses.len()), (2, 1, 1));

    let drift_page = repo
        .page_drift_reports(DriftReportListQuery {
            feedback_cycle_id: Some(fixture.cycle_id),
            profile_id: Some(fixture.profile_ref.id.clone()),
            kind: Some(FeedbackDriftKind::Data),
            metric: Some(FeedbackDriftMetric::PopulationStabilityIndex),
            page: PageRequest::new(0, 1_000),
        })
        .await
        .expect("page exact drift lineage");
    assert_eq!(
        (drift_page.total, drift_page.page, drift_page.size),
        (1, 1, PageRequest::MAX_SIZE)
    );
    assert_eq!(drift_page.items[0].detail_hash, content_hash('1'));
    let missing_drift = repo
        .page_drift_reports(DriftReportListQuery {
            feedback_cycle_id: None,
            profile_id: Some(ResearchProfileId::new("missing_profile")),
            kind: None,
            metric: None,
            page: PageRequest::default(),
        })
        .await
        .expect("filter absent drift profile");
    assert!(missing_drift.items.is_empty());
    assert_eq!(missing_drift.total, 0);

    let evaluation_page = repo
        .page_model_evaluation_uses(
            &fixture.champion_model_version_id,
            PageRequest::new(0, 1_000),
        )
        .await
        .expect("page champion evaluation lineage");
    assert_eq!(
        (
            evaluation_page.total,
            evaluation_page.page,
            evaluation_page.size,
        ),
        (1, 1, PageRequest::MAX_SIZE)
    );
    assert_eq!(
        evaluation_page.items[0].champion_model_version_id,
        fixture.champion_model_version_id
    );
    let missing_evaluation = repo
        .page_model_evaluation_uses(&ModelVersionId::from_v7(), PageRequest::default())
        .await
        .expect("filter absent model evaluation lineage");
    assert!(missing_evaluation.items.is_empty());
    assert_eq!(missing_evaluation.total, 0);
}
