//! Promotion-permit lifecycle and governed service contracts on real `PostgreSQL`.

use std::{slice, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::governance::PromotionPermitService;
use quant_pivot_error::{QuantError, feedback::FeedbackError, rbac::RbacError};
use quant_pivot_models::{
    domain::{
        api::PromotionPermitListQuery,
        pagination::PageRequest,
        quant::{
            CandidateExplanationValidation, IssuePromotionPermit, ModelCandidateManifestDocument,
            ModelCandidateManifestInfo, ModelCandidateManifestInput, NewModelCandidateManifest,
            NewPromotionPermit, PromotionGateArtifact, PromotionGateArtifactInput,
            PromotionPermitActor, PromotionPermitInfo, PromotionPermitIssueInput,
            PromotionPermitScope, PromotionPermitScopeInput, PromotionPermitStatus,
            RevokePromotionPermit,
        },
        rbac::{AssignPermissions, AssignRoles, NewRole, NewUser, Permission},
    },
    entities::{
        decision_policy_snapshot::Entity as PolicySnapshotEntity,
        policy_activation_guard::Entity as PolicyGuardEntity,
        quant_feedback_promotion_permit::{
            Column as PromotionPermitColumn, Entity as PromotionPermitEntity,
        },
        user::{Column as UserColumn, Entity as UserEntity},
    },
    enums::{
        common::MarketCategory,
        quant::QuantRuntimeMode,
        rbac::{Operation, ResourceType, RoleKind, RoleStatus, UserStatus},
    },
    types::{
        BacktestPathSetId, ContentHash, DecisionPolicySnapshotId, FeedbackCycleId,
        ModelCandidateManifestId, ModelVersionId, PolicyBundleGeneration, PolicyIdempotencyKey,
        PromotionPermitId, ResearchProfileId, ResearchProfileRef, RoleCode, RoleId, UserId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgModelCandidateManifestRepository, PgModelRegistryRepository, PgPromotionPermitRepository,
        PgRolePermissionRepository, PgRoleRepository, PgUserRepository, PgUserRoleRepository,
    },
    traits::{
        ModelCandidateManifestRepository, ModelCandidateManifestWriteOutcome,
        ModelRegistryRepository, PromotionPermitIssueOutcome, PromotionPermitRepository,
        PromotionPermitRevokeOutcome, RolePermissionRepository, RoleRepository, UserRepository,
        UserRoleRepository,
    },
};
use quant_pivot_system_tests::{
    postgres::{PostgresClock, setup_pg},
    support::{
        model_serving_fixtures::{ModelVersionFixture, ModelVersionFixtureSeed},
        model_spec_fixtures,
    },
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, DbBackend, DbErr,
    EntityTrait, IntoActiveModel, PaginatorTrait, QueryFilter, Statement, TransactionTrait,
};
use tokio::task::JoinSet;

use super::feedback_boot_schema::{content_hash, prepare_profile_fixture};

#[derive(Clone)]
struct PermitContext {
    feedback_cycle_id: FeedbackCycleId,
    profile_ref: ResearchProfileRef,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    candidate_model_version_id: ModelVersionId,
    candidate_manifest_id: ModelCandidateManifestId,
    candidate_manifest_hash: ContentHash,
    promotion_gate_hash: ContentHash,
    expected_policy_generation: PolicyBundleGeneration,
    expected_decision_policy_snapshot_id: DecisionPolicySnapshotId,
    expected_snapshot_hash: ContentHash,
    actor_user_id: UserId,
}

impl PermitContext {
    async fn prepare(db: &DatabaseConnection) -> Self {
        let feedback = Box::pin(prepare_profile_fixture(
            db,
            model_spec_fixtures::crypto_profile_ref(),
            model_spec_fixtures::crypto_horizon_secs(),
        ))
        .await;
        feedback
            .cycle
            .clone()
            .into_active_model()
            .insert(db)
            .await
            .expect("insert permit feedback cycle");
        let registry = PgModelRegistryRepository::new(db.clone());
        let champion = registry
            .find_model_version(&feedback.champion_model_version_id)
            .await
            .expect("load permit champion model")
            .expect("permit champion model exists");
        let candidate_model_version_id = ModelVersionId::from_v7();
        let candidate = ModelVersionFixture::prepare(
            db,
            ModelVersionFixtureSeed::training(
                format!("promotion-permit:{candidate_model_version_id}"),
                candidate_model_version_id,
                champion.model_spec_id,
                content_hash('e'),
            ),
        )
        .await
        .expect("prepare permit candidate model");
        registry
            .create_model_version(candidate)
            .await
            .expect("persist permit candidate model");
        let manifest = Self::candidate_manifest(
            db,
            feedback.cycle_id,
            feedback.cycle.feedback_policy_hash(),
            candidate_model_version_id,
        )
        .await;
        let guard = PolicyGuardEntity::find_by_id(1_i16)
            .one(db)
            .await
            .expect("load policy activation guard")
            .expect("policy activation guard exists");
        let snapshot_id = guard
            .current_snapshot_id
            .expect("boot policy snapshot is active");
        let snapshot = PolicySnapshotEntity::find_by_id(snapshot_id)
            .one(db)
            .await
            .expect("load active policy snapshot")
            .expect("active policy snapshot exists");
        let actor = UserEntity::find()
            .filter(UserColumn::Username.eq("admin"))
            .one(db)
            .await
            .expect("load bootstrap admin")
            .expect("bootstrap admin exists");
        Self {
            feedback_cycle_id: feedback.cycle_id,
            profile_ref: model_spec_fixtures::crypto_profile_ref(),
            champion_model_version_id: feedback.champion_model_version_id,
            champion_serving_contract_hash: feedback.champion_serving_contract_hash,
            candidate_model_version_id,
            candidate_manifest_id: manifest.manifest_id,
            candidate_manifest_hash: manifest.manifest_hash,
            promotion_gate_hash: manifest.promotion_gate_hash,
            expected_policy_generation: guard.generation,
            expected_decision_policy_snapshot_id: snapshot.decision_policy_snapshot_id,
            expected_snapshot_hash: snapshot.snapshot_hash,
            actor_user_id: actor.id,
        }
    }

    async fn candidate_manifest(
        db: &DatabaseConnection,
        feedback_cycle_id: FeedbackCycleId,
        feedback_policy_hash: ContentHash,
        candidate_model_version_id: ModelVersionId,
    ) -> ModelCandidateManifestInfo {
        let candidate = PgModelRegistryRepository::new(db.clone())
            .find_model_version(&candidate_model_version_id)
            .await
            .expect("load permit candidate model")
            .expect("permit candidate model exists");
        let contract = candidate
            .verified_serving_contract()
            .expect("verify permit candidate serving contract");
        let bindings = contract.bindings();
        let explanation = CandidateExplanationValidation::try_from(bindings)
            .expect("derive permit candidate explanation validation");
        let cpcv_path_set_id = BacktestPathSetId::from_v7();
        let cpcv_path_set_hash = content_hash('c');
        let promotion_gate = PromotionGateArtifact::try_seal(PromotionGateArtifactInput {
            feedback_cycle_id,
            candidate_recipe_hash: content_hash('b'),
            candidate_model_version_id,
            profile_ref: candidate.profile_ref.clone(),
            category: MarketCategory::Crypto,
            feedback_policy_hash,
            decision_policy_snapshot_hash: bindings.policy_snapshot.snapshot_hash,
            truth_freeze_hash: content_hash('1'),
            attribution_manifest_hash: content_hash('2'),
            validation_artifact_hash: content_hash('3'),
            quality_gate_report_hash: content_hash('4'),
            comparison_artifact_hash: content_hash('5'),
            cpcv_path_set_id,
            cpcv_path_set_hash,
            explanation_validation_hash: explanation.report_hash,
        })
        .expect("seal permit promotion gate");
        let calibration = bindings
            .model
            .calibration
            .as_ref()
            .map(|binding| (binding.artifact_id, binding.content_hash));
        let training_dataset_id = candidate
            .training_dataset_id
            .expect("permit candidate training dataset");
        let document = ModelCandidateManifestDocument::try_new(ModelCandidateManifestInput {
            feedback_cycle_id,
            candidate_recipe_hash: content_hash('b'),
            model_version_id: candidate.model_version_id,
            model_spec_id: candidate.model_spec_id,
            model_family: candidate.model_family,
            model_artifact_hash: candidate.artifact_hash,
            serving_contract_hash: candidate.serving_contract_hash,
            training_dataset_id,
            training_dataset_hash: bindings.transform.training_dataset_hash,
            feature_schema_hash: bindings.schemas.feature_schema_hash,
            input_contract_hash: bindings.transform.input_contract_hash,
            input_transform_hash: bindings.transform.input_transform_hash,
            calibration_artifact_id: calibration.map(|(id, _)| id),
            calibration_artifact_hash: calibration.map(|(_, hash)| hash),
            cpcv_path_set_id,
            cpcv_path_set_hash,
            profile_ref: candidate.profile_ref.clone(),
            category: MarketCategory::Crypto,
            feedback_policy_hash,
            decision_policy_snapshot_hash: bindings.policy_snapshot.snapshot_hash,
            explanation_validation: explanation,
            promotion_gate,
        })
        .expect("seal permit candidate manifest document");
        let manifest =
            NewModelCandidateManifest::try_new(document).expect("seal permit candidate manifest");
        match PgModelCandidateManifestRepository::new(db.clone())
            .insert(manifest)
            .await
            .expect("persist permit candidate manifest")
        {
            ModelCandidateManifestWriteOutcome::Inserted(manifest)
            | ModelCandidateManifestWriteOutcome::AlreadyPresent(manifest) => manifest,
        }
    }

    fn scope(&self, expires_at: DateTime<Utc>, hash_seed: char) -> PromotionPermitScope {
        PromotionPermitScope::try_new(PromotionPermitScopeInput {
            feedback_cycle_id: self.feedback_cycle_id,
            profile_ref: self.profile_ref.clone(),
            category: MarketCategory::Crypto,
            expected_policy_generation: self.expected_policy_generation,
            expected_runtime_control_revision: 0,
            expected_decision_policy_snapshot_id: self.expected_decision_policy_snapshot_id,
            expected_snapshot_hash: self.expected_snapshot_hash,
            expected_route_generation: 1,
            champion_model_version_id: self.champion_model_version_id,
            champion_serving_contract_hash: self.champion_serving_contract_hash,
            candidate_model_version_id: self.candidate_model_version_id,
            candidate_manifest_id: self.candidate_manifest_id,
            candidate_manifest_hash: self.candidate_manifest_hash,
            promotion_gate_hash: self.promotion_gate_hash,
            allowed_runtime_modes: vec![QuantRuntimeMode::ReportOnly, QuantRuntimeMode::SemiAuto],
            non_route_policy_hash: content_hash(hash_seed),
            serving_constraints_hash: content_hash('7'),
            expires_at,
        })
        .expect("seal promotion-permit scope")
    }

    fn seal(
        &self,
        idempotency_key: &str,
        expires_at: DateTime<Utc>,
        hash_seed: char,
    ) -> NewPromotionPermit {
        NewPromotionPermit::try_seal(PromotionPermitIssueInput {
            idempotency_key: idempotency_key
                .parse::<PolicyIdempotencyKey>()
                .expect("valid promotion-permit idempotency key"),
            scope: self.scope(expires_at, hash_seed),
            preflight_hash: content_hash('8'),
            issued_by_user_id: self.actor_user_id,
            issued_by_username: "admin".to_owned(),
            issued_by_role: RoleCode::new("risk_owner"),
            issuance_reason: "authorize exact Crypto route replacement".to_owned(),
        })
        .expect("seal promotion permit")
    }
}

struct PermitServiceFixture {
    db: DatabaseConnection,
    context: PermitContext,
    service: Arc<PromotionPermitService>,
}

impl PermitServiceFixture {
    async fn prepare(db: DatabaseConnection) -> Self {
        let context = Box::pin(PermitContext::prepare(&db)).await;
        let repository = Arc::new(PgPromotionPermitRepository::new(db.clone()));
        let service = Arc::new(PromotionPermitService::new(
            repository as Arc<dyn PromotionPermitRepository>,
        ));
        Self {
            db,
            context,
            service,
        }
    }

    async fn actor(&self, code: &str, operations: &[Operation]) -> PromotionPermitActor {
        let users = PgUserRepository::new(self.db.clone());
        let roles = PgRoleRepository::new(self.db.clone());
        let memberships = PgUserRoleRepository::new(self.db.clone());
        let permissions = PgRolePermissionRepository::new(self.db.clone());
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
            .expect("create permit role");
        permissions
            .set_permissions_for_role(AssignPermissions {
                role_id: role.id,
                permissions: operations
                    .iter()
                    .map(|operation| Permission::new(ResourceType::Publication, *operation))
                    .collect(),
            })
            .await
            .expect("assign permit permissions");
        let user = users
            .create(NewUser {
                id: UserId::from_v7(),
                username: format!("{code}_user"),
                password_hash: "argon2id$permit-test".to_owned(),
                nickname: format!("{code} user"),
                avatar: None,
                email: None,
                phone: None,
                status: UserStatus::Active,
            })
            .await
            .expect("create permit actor");
        memberships
            .set_roles_for_user(AssignRoles {
                user_id: user.id,
                role_ids: vec![role.id],
            })
            .await
            .expect("assign permit role");
        PromotionPermitActor {
            user_id: user.id,
            acting_role: role.code,
        }
    }

    fn issue(
        &self,
        actor: PromotionPermitActor,
        idempotency_key: &str,
        expires_at: DateTime<Utc>,
        hash_seed: char,
        reason: &str,
    ) -> IssuePromotionPermit {
        IssuePromotionPermit {
            actor,
            idempotency_key: idempotency_key
                .parse::<PolicyIdempotencyKey>()
                .expect("valid service idempotency key"),
            scope: self.context.scope(expires_at, hash_seed),
            preflight_hash: content_hash('8'),
            reason: reason.to_owned(),
        }
    }

    async fn verify_issue_guards(
        &self,
        owner: &PromotionPermitActor,
        publish_only: &PromotionPermitActor,
        no_publish: &PromotionPermitActor,
        database_now: DateTime<Utc>,
    ) {
        let denied_command = self.issue(
            no_publish.clone(),
            "permit-denied-0001",
            database_now + Duration::minutes(10),
            'a',
            "must be rejected by server-side RBAC",
        );
        assert!(matches!(
            self.service.issue(denied_command).await,
            Err(QuantError::Rbac(RbacError::PermissionDenied { .. }))
        ));
        assert_eq!(
            PromotionPermitEntity::find()
                .filter(PromotionPermitColumn::IdempotencyKey.eq("permit-denied-0001"))
                .count(&self.db)
                .await
                .expect("count denied permits"),
            0
        );

        let expired = self.issue(
            owner.clone(),
            "permit-expired-0002",
            database_now - Duration::seconds(1),
            'c',
            "expired commands never create authority",
        );
        assert!(matches!(
            self.service.issue(expired).await,
            Err(QuantError::Feedback(
                FeedbackError::InvalidPromotionPermit { .. }
            ))
        ));

        let publish_only_command = self.issue(
            publish_only.clone(),
            "permit-publish-only-0003",
            database_now + Duration::minutes(10),
            'd',
            "issuer may create but cannot revoke",
        );
        let publish_only_permit = match self
            .service
            .issue(publish_only_command)
            .await
            .expect("publish-only issue")
        {
            PromotionPermitIssueOutcome::Issued(permit) => permit,
            PromotionPermitIssueOutcome::ExactReplay(_) => {
                panic!("publish-only first issue replayed")
            }
        };
        assert!(matches!(
            self.service
                .revoke(RevokePromotionPermit {
                    promotion_permit_id: publish_only_permit.promotion_permit_id,
                    expected_revision: 0,
                    actor: publish_only.clone(),
                    reason: "issuer cannot withdraw without retire authority".to_owned(),
                })
                .await,
            Err(QuantError::Rbac(RbacError::PermissionDenied { .. }))
        ));
    }

    async fn verify_issue_fields(
        &self,
        owner: &PromotionPermitActor,
        publish_only: &PromotionPermitActor,
        database_now: DateTime<Utc>,
    ) {
        let command = self.issue(
            owner.clone(),
            "permit-service-0001",
            database_now + Duration::minutes(10),
            'b',
            "authorize one exact governed route",
        );
        let issued = match self
            .service
            .issue(command.clone())
            .await
            .expect("issue permit")
        {
            PromotionPermitIssueOutcome::Issued(permit) => permit,
            PromotionPermitIssueOutcome::ExactReplay(_) => {
                panic!("first permit command unexpectedly replayed")
            }
        };
        assert_eq!(issued.issued_by_username, "permit_owner_user");
        assert_eq!(issued.issued_by_role, owner.acting_role);
        assert!(issued.issued_at >= database_now);

        let replayed = match self
            .service
            .issue(command.clone())
            .await
            .expect("replay permit")
        {
            PromotionPermitIssueOutcome::ExactReplay(permit) => permit,
            PromotionPermitIssueOutcome::Issued(_) => {
                panic!("exact permit retry inserted another row")
            }
        };
        assert_eq!(replayed, issued);

        let mut reason_drift = command.clone();
        "different immutable authority reason".clone_into(&mut reason_drift.reason);
        assert!(matches!(
            self.service.issue(reason_drift).await,
            Err(QuantError::Feedback(
                FeedbackError::PromotionPermitConflict { .. }
            ))
        ));
        let mut preflight_drift = command.clone();
        preflight_drift.preflight_hash = content_hash('9');
        assert!(matches!(
            self.service.issue(preflight_drift).await,
            Err(QuantError::Feedback(
                FeedbackError::PromotionPermitConflict { .. }
            ))
        ));
        let mut actor_drift = command;
        actor_drift.actor = publish_only.clone();
        assert!(matches!(
            self.service.issue(actor_drift).await,
            Err(QuantError::Feedback(
                FeedbackError::PromotionPermitConflict { .. }
            ))
        ));
        assert!(matches!(
            self.service
                .revoke(RevokePromotionPermit {
                    promotion_permit_id: issued.promotion_permit_id,
                    expected_revision: 1,
                    actor: owner.clone(),
                    reason: "invalid base revision cannot mutate authority".to_owned(),
                })
                .await,
            Err(QuantError::Feedback(
                FeedbackError::PromotionPermitConflict { .. }
            ))
        ));
        assert_eq!(
            load_permit(&self.db, issued.promotion_permit_id)
                .await
                .revision,
            0
        );
    }

    async fn race_issue(
        &self,
        owner: &PromotionPermitActor,
        database_now: DateTime<Utc>,
    ) -> (IssuePromotionPermit, PromotionPermitInfo) {
        let command = self.issue(
            owner.clone(),
            "permit-concurrent-0004",
            database_now + Duration::minutes(10),
            'e',
            "concurrent exact issue converges",
        );
        let mut tasks = JoinSet::new();
        for _ in 0..8 {
            let service = Arc::clone(&self.service);
            let retry = command.clone();
            tasks.spawn(async move { service.issue(retry).await });
        }
        let mut issued_count = 0;
        let mut replay_count = 0;
        let mut concurrent_permit = None;
        while let Some(result) = tasks.join_next().await {
            match result
                .expect("join issue task")
                .expect("concurrent exact issue")
            {
                PromotionPermitIssueOutcome::Issued(permit) => {
                    issued_count += 1;
                    concurrent_permit = Some(permit);
                }
                PromotionPermitIssueOutcome::ExactReplay(permit) => {
                    replay_count += 1;
                    concurrent_permit = Some(permit);
                }
            }
        }
        assert_eq!((issued_count, replay_count), (1, 7));
        let permit = concurrent_permit.expect("concurrent permit result");
        assert_eq!(
            PromotionPermitEntity::find()
                .filter(PromotionPermitColumn::IdempotencyKey.eq("permit-concurrent-0004"))
                .count(&self.db)
                .await
                .expect("count concurrent permit"),
            1
        );
        (command, permit)
    }

    async fn race_revoke(
        &self,
        owner: &PromotionPermitActor,
        issue_command: IssuePromotionPermit,
        permit: PromotionPermitInfo,
        database_now: DateTime<Utc>,
    ) {
        let revoke = RevokePromotionPermit {
            promotion_permit_id: permit.promotion_permit_id,
            expected_revision: 0,
            actor: owner.clone(),
            reason: "withdraw concurrent authority".to_owned(),
        };
        let before_revoke = self.db.statement_time().await;
        let mut tasks = JoinSet::new();
        for _ in 0..8 {
            let service = Arc::clone(&self.service);
            let retry = revoke.clone();
            tasks.spawn(async move { service.revoke(retry).await });
        }
        let mut revoked_count = 0;
        let mut replay_count = 0;
        let mut revoked_permit = None;
        while let Some(result) = tasks.join_next().await {
            match result
                .expect("join revoke task")
                .expect("concurrent exact revoke")
            {
                PromotionPermitRevokeOutcome::Revoked(permit) => {
                    revoked_count += 1;
                    revoked_permit = Some(permit);
                }
                PromotionPermitRevokeOutcome::ExactReplay(permit) => {
                    replay_count += 1;
                    revoked_permit = Some(permit);
                }
            }
        }
        assert_eq!((revoked_count, replay_count), (1, 7));
        let revoked_permit = revoked_permit.expect("revoked permit result");
        assert_eq!(revoked_permit.revision, 1);
        assert!(revoked_permit.revoked_at.expect("database revocation time") >= before_revoke);
        assert_eq!(
            revoked_permit.updated_at,
            revoked_permit
                .revoked_at
                .expect("database revocation time remains present")
        );

        let post_revoke_issue = self
            .service
            .issue(issue_command)
            .await
            .expect("issuance replay after revoke");
        assert!(matches!(
            post_revoke_issue,
            PromotionPermitIssueOutcome::ExactReplay(ref stored)
                if stored.promotion_permit_id == revoked_permit.promotion_permit_id
                    && stored.revision == 1
        ));

        let conflict_command = self.issue(
            owner.clone(),
            "permit-revoke-race-0005",
            database_now + Duration::minutes(10),
            'f',
            "different revoke reasons cannot both win",
        );
        let conflict_permit = match self
            .service
            .issue(conflict_command)
            .await
            .expect("issue revoke-race permit")
        {
            PromotionPermitIssueOutcome::Issued(permit) => permit,
            PromotionPermitIssueOutcome::ExactReplay(_) => {
                panic!("revoke-race first issue replayed")
            }
        };
        let mut drift_tasks = JoinSet::new();
        for reason in ["withdraw reason alpha", "withdraw reason beta"] {
            let service = Arc::clone(&self.service);
            let actor = owner.clone();
            drift_tasks.spawn(async move {
                service
                    .revoke(RevokePromotionPermit {
                        promotion_permit_id: conflict_permit.promotion_permit_id,
                        expected_revision: 0,
                        actor,
                        reason: reason.to_owned(),
                    })
                    .await
            });
        }
        let mut drift_success = 0;
        let mut drift_conflict = 0;
        while let Some(result) = drift_tasks.join_next().await {
            match result.expect("join drift revoke task") {
                Ok(PromotionPermitRevokeOutcome::Revoked(_)) => drift_success += 1,
                Err(QuantError::Feedback(FeedbackError::PromotionPermitConflict { .. })) => {
                    drift_conflict += 1;
                }
                other => panic!("unexpected revoke-race result: {other:?}"),
            }
        }
        assert_eq!((drift_success, drift_conflict), (1, 1));
    }

    async fn verify_rbac_state(&self, database_now: DateTime<Utc>) {
        let roles = PgRoleRepository::new(self.db.clone());
        let disabled = self
            .actor(
                "permit_disabled",
                &[Operation::Authorize, Operation::Retire],
            )
            .await;
        let disabled_role = roles
            .find_by_code(disabled.acting_role.as_str())
            .await
            .expect("load role")
            .expect("disabled test role");
        roles
            .change_status(&disabled_role.id, RoleStatus::Disabled)
            .await
            .expect("disable role");
        let disabled_command = self.issue(
            disabled,
            "permit-disabled-0006",
            database_now + Duration::minutes(10),
            '1',
            "disabled role must fail closed",
        );
        assert!(matches!(
            self.service.issue(disabled_command).await,
            Err(QuantError::Rbac(RbacError::PermissionDenied { .. }))
        ));

        let users = PgUserRepository::new(self.db.clone());
        let inactive = self
            .actor(
                "permit_inactive",
                &[Operation::Authorize, Operation::Retire],
            )
            .await;
        users
            .change_status(&inactive.user_id, UserStatus::Disabled)
            .await
            .expect("disable permit actor");
        let inactive_command = self.issue(
            inactive,
            "permit-inactive-0007",
            database_now + Duration::minutes(10),
            '2',
            "inactive user must fail closed",
        );
        assert!(matches!(
            self.service.issue(inactive_command).await,
            Err(QuantError::Rbac(RbacError::PermissionDenied { .. }))
        ));

        let admin = UserEntity::find()
            .filter(UserColumn::Username.eq("admin"))
            .one(&self.db)
            .await
            .expect("load bootstrap admin")
            .expect("bootstrap admin exists");
        let command = self.issue(
            PromotionPermitActor {
                user_id: admin.id,
                acting_role: RoleCode::new("super_admin"),
            },
            "permit-super-admin-0008",
            database_now + Duration::minutes(10),
            '3',
            "super-admin bypass remains explicitly attributed",
        );
        let super_admin = self
            .service
            .issue(command)
            .await
            .expect("super-admin issue");
        assert!(matches!(
            super_admin,
            PromotionPermitIssueOutcome::Issued(ref permit)
                if permit.issued_by_username == "admin"
                    && permit.issued_by_role.as_str() == "super_admin"
        ));
    }

    async fn verify_page_contracts(&self) {
        let repository = PgPromotionPermitRepository::new(self.db.clone());
        let active = repository
            .page_permits(PromotionPermitListQuery {
                profile_id: Some(self.context.profile_ref.id.clone()),
                category: Some(MarketCategory::Crypto),
                status: Some(PromotionPermitStatus::Active),
                page: PageRequest::new(0, 1_000),
            })
            .await
            .expect("page active permits");
        assert!(!active.permits.items.is_empty());
        assert_eq!(
            (active.permits.page, active.permits.size),
            (1, PageRequest::MAX_SIZE)
        );
        assert!(active.permits.items.iter().all(|permit| {
            matches!(
                permit.status_at(active.observed_at),
                Ok(PromotionPermitStatus::Active)
            )
        }));

        let revoked = repository
            .page_permits(PromotionPermitListQuery {
                profile_id: Some(self.context.profile_ref.id.clone()),
                category: Some(MarketCategory::Crypto),
                status: Some(PromotionPermitStatus::Revoked),
                page: PageRequest::default(),
            })
            .await
            .expect("page revoked permits");
        assert!(!revoked.permits.items.is_empty());
        assert!(revoked.permits.items.iter().all(|permit| {
            matches!(
                permit.status_at(revoked.observed_at),
                Ok(PromotionPermitStatus::Revoked)
            )
        }));

        let missing = repository
            .page_permits(PromotionPermitListQuery {
                profile_id: Some(ResearchProfileId::new("missing_profile")),
                category: None,
                status: None,
                page: PageRequest::default(),
            })
            .await
            .expect("page missing profile permits");
        assert_eq!(missing.permits.total, 0);
        assert!(missing.permits.items.is_empty());
    }
}

async fn load_permit(db: &DatabaseConnection, permit_id: PromotionPermitId) -> PromotionPermitInfo {
    PromotionPermitEntity::find_by_id(permit_id)
        .into_partial_model::<PromotionPermitInfo>()
        .one(db)
        .await
        .expect("load promotion permit")
        .expect("promotion permit exists")
}

fn assert_rejected<T>(result: Result<T, DbErr>, expected: &str) {
    let Err(error) = result else {
        panic!("database unexpectedly accepted promotion-permit drift");
    };
    assert!(
        error.to_string().contains(expected),
        "expected database error containing `{expected}`, got `{error}`"
    );
}

pub async fn schema_roundtrip_revoke() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let context = Box::pin(PermitContext::prepare(&db)).await;
    let observed_at = db.statement_time().await;
    let permit = context.seal(
        "permit-schema-roundtrip-0001",
        observed_at + Duration::minutes(5),
        '6',
    );
    let permit_id = permit.promotion_permit_id();
    permit
        .into_active_model()
        .insert(&db)
        .await
        .expect("insert promotion permit");

    let inserted = load_permit(&db, permit_id).await;
    inserted.validate().expect("validate persisted permit");
    assert_eq!(inserted.revision, 0);
    assert_eq!(inserted.updated_at, inserted.issued_at);
    assert_eq!(
        inserted
            .status_at(inserted.expires_at - Duration::nanoseconds(1))
            .expect("active before expiry"),
        PromotionPermitStatus::Active
    );
    assert_eq!(
        inserted
            .status_at(inserted.expires_at)
            .expect("expired at exact boundary"),
        PromotionPermitStatus::Expired
    );

    let rolled_back = context.seal(
        "permit-schema-rollback-0002",
        observed_at + Duration::minutes(10),
        '9',
    );
    let rolled_back_id = rolled_back.promotion_permit_id();
    let transaction = db.begin().await.expect("begin permit rollback transaction");
    rolled_back
        .into_active_model()
        .insert(&transaction)
        .await
        .expect("insert rollback permit");
    transaction
        .rollback()
        .await
        .expect("rollback permit transaction");
    assert!(
        PromotionPermitEntity::find_by_id(rolled_back_id)
            .one(&db)
            .await
            .expect("check rolled-back permit")
            .is_none()
    );

    let revoke_reason = "promotion authority withdrawn";
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE quant_feedback_promotion_permit
         SET revoked_by_user_id = $1, revoked_by_username = $2, revoked_by_role = $3,
             revocation_reason = $4, revision = revision + 1
         WHERE promotion_permit_id = $5",
        [
            context.actor_user_id.as_uuid().into(),
            "admin".into(),
            "risk_owner".into(),
            revoke_reason.into(),
            permit_id.as_uuid().into(),
        ],
    ))
    .await
    .expect("revoke promotion permit");
    let revoked = load_permit(&db, permit_id).await;
    revoked.validate().expect("validate revoked permit");
    assert_eq!(revoked.revision, 1);
    assert_eq!(revoked.updated_at, revoked.revoked_at.expect("revoked at"));
    assert_eq!(
        revoked
            .status_at(revoked.revoked_at.expect("revoked at"))
            .expect("status at revocation"),
        PromotionPermitStatus::Revoked
    );

    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "UPDATE quant_feedback_promotion_permit
         SET revoked_by_user_id = $1, revoked_by_username = $2, revoked_by_role = $3,
             revocation_reason = $4, revision = 1
         WHERE promotion_permit_id = $5",
        [
            context.actor_user_id.as_uuid().into(),
            "admin".into(),
            "risk_owner".into(),
            revoke_reason.into(),
            permit_id.as_uuid().into(),
        ],
    ))
    .await
    .expect("exact revoke row replay is a no-op");

    assert_rejected(
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_feedback_promotion_permit
             SET revocation_reason = 'different immutable reason', revision = 2
             WHERE promotion_permit_id = $1",
            [permit_id.as_uuid().into()],
        ))
        .await,
        "promotion permit is already revoked",
    );
    assert_rejected(
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "UPDATE quant_feedback_promotion_permit
             SET category = 'weather'::qp_market_category
             WHERE promotion_permit_id = $1",
            [permit_id.as_uuid().into()],
        ))
        .await,
        "immutable issuance cannot change",
    );
    assert_rejected(
        db.execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "DELETE FROM quant_feedback_promotion_permit WHERE promotion_permit_id = $1",
            [permit_id.as_uuid().into()],
        ))
        .await,
        "DELETE is not permitted",
    );

    let unchanged = load_permit(&db, permit_id).await;
    assert_eq!(unchanged, revoked);
}

pub async fn governed_service_contracts() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let fixture = PermitServiceFixture::prepare(db.clone()).await;
    let owner = fixture
        .actor("permit_owner", &[Operation::Authorize, Operation::Retire])
        .await;
    let publish_only = fixture
        .actor("permit_issuer", slice::from_ref(&Operation::Authorize))
        .await;
    let no_publish = fixture
        .actor("permit_reader", slice::from_ref(&Operation::Read))
        .await;
    let database_now = db.statement_time().await;
    Box::pin(fixture.verify_issue_guards(&owner, &publish_only, &no_publish, database_now)).await;
    Box::pin(fixture.verify_issue_fields(&owner, &publish_only, database_now)).await;
    let (issue_command, permit) = Box::pin(fixture.race_issue(&owner, database_now)).await;
    Box::pin(fixture.race_revoke(&owner, issue_command, permit, database_now)).await;
    Box::pin(fixture.verify_rbac_state(database_now)).await;
    Box::pin(fixture.verify_page_contracts()).await;
}
