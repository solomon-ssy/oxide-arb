//! Atomic `PostgreSQL` owner of governed model-route promotion.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{
    feedback::{FeedbackError, PromotionCommitError},
    storage::{
        StorageError,
        entity::{POLICY_ACTIVATION, QUANT_FEEDBACK_CYCLE, QUANT_RESEARCH_JOB},
    },
};
use quant_pivot_models::{
    domain::{
        governance::{
            NewDecisionPolicySnapshot, NewModelPromotionActivation, NewPolicyApproval,
            NewPolicyRevision,
        },
        ports::FeedbackShadowSubject,
        quant::{
            CommitModelRoutePromotion, FeedbackCycleInfo, ModelCandidateManifestInfo,
            ModelGovernanceAuditDetail, ModelGovernanceAuditInfo, ModelRoutePromotionPolicy,
            ModelRoutePromotionRecord, ModelRoutePromotionRecordInput, ModelRoutePromotionRoute,
            ModelVersionInfo, NewRoutePromotionAudit, PromotionPermitInfo, PromotionPermitStatus,
            PromotionPolicyProjection, PromotionPreflight,
        },
    },
    entities::{
        decision_policy_snapshot::{Entity as SnapshotEntity, Model as SnapshotModel},
        policy_activation::{
            Column as ActivationColumn, Entity as ActivationEntity, Model as ActivationModel,
        },
        policy_activation_audit::Entity as ActivationAuditEntity,
        policy_activation_event_outbox::Entity as ActivationOutboxEntity,
        policy_activation_guard::{
            Column as ActivationGuardColumn, Entity as ActivationGuardEntity,
            Model as ActivationGuardModel,
        },
        policy_approval::Entity as ApprovalEntity,
        policy_revision::Entity as RevisionEntity,
        quant_feedback_cycle::{Column as CycleColumn, Entity as CycleEntity, Model as CycleModel},
        quant_feedback_promotion_permit::Entity as PermitEntity,
        quant_feedback_stage_event::{
            Column as StageEventColumn, Entity as StageEventEntity, Model as StageEventModel,
        },
        quant_model_candidate_manifest::Entity as CandidateManifestEntity,
        quant_model_governance_audit::{Entity as ModelAuditEntity, Model as ModelAuditModel},
        quant_model_spec::Entity as ModelSpecEntity,
        quant_model_version::{Column as ModelVersionColumn, Entity as ModelVersionEntity},
        quant_research_job::{Entity as ResearchJobEntity, Model as ResearchJobModel},
        system_runtime_control::Entity as RuntimeControlEntity,
    },
    enums::{
        quant::{
            FeedbackCycleStatus, FeedbackDecision, FeedbackStage, FeedbackStageEventKind,
            ModelGovernanceAction, ResearchJobKind, ResearchJobResultKind, ResearchJobStatus,
        },
        rbac::{Operation, ResourceType},
        runtime_config::{
            CheckOutcome, ConfigResourceKind, DecisionPolicySnapshotSource, PolicyActivationKind,
            PolicyActorKind, PolicyApprovalDecision, PolicyPreflightCheckKind,
            PolicyPreflightDetailCode, PolicyRevisionStatus,
        },
    },
    hashing::CanonicalDigest,
    runtime_config::{
        ActivePolicyBundle, DecisionPolicySnapshot, PolicyDocument, PolicyPreflightResult,
        PolicyValidationEvidence, PolicyValidationSubject,
    },
    types::{
        AuditEventId, ContentHash, DecisionPolicySnapshotId, FeedbackCycleId,
        ModelGovernanceAuditId, PolicyActivationId, PolicyApprovalId, PolicyBundleGeneration,
        PolicyRevisionId, PromotionPermitId, ResearchJobParams,
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait, ExprTrait,
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait, sea_query::Expr,
};

use crate::{
    postgres::{
        authorization::{self, AuthorizedGovernedActor},
        governance::PgPolicyRepository,
        primitives,
        quant::{
            feature_parity::PgFeatureParityRepository, model_registry::PgModelRegistryRepository,
        },
        runtime_control::SYSTEM_RUNTIME_CONTROL_ID,
    },
    traits::{
        ModelRoutePromotionCommit, ModelRoutePromotionOutcome, ModelRoutePromotionRepository,
    },
};

struct LockedStage {
    event: StageEventModel,
    job: ResearchJobModel,
}

struct LockedModels {
    champion: ModelVersionInfo,
    candidate: ModelVersionInfo,
}

struct LockedPermit {
    permit: PromotionPermitInfo,
    observed_at: DateTime<Utc>,
}

struct PromotionRows {
    snapshot: NewDecisionPolicySnapshot,
    revision: NewPolicyRevision,
    approval: NewPolicyApproval,
    activation: NewModelPromotionActivation,
    audit: NewRoutePromotionAudit,
    record: ModelRoutePromotionRecord,
}

struct ResolvedCommit {
    commit: ModelRoutePromotionCommit,
    record: ModelRoutePromotionRecord,
}

/// Sole transaction owner for model publication and one category route.
pub struct PgModelRoutePromotionRepository {
    db: DatabaseConnection,
}

impl PgModelRoutePromotionRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn conflict(detail: impl Into<String>) -> PromotionCommitError {
        FeedbackError::PromotionTransactionConflict {
            detail: detail.into(),
        }
        .into()
    }

    async fn lock_permit(
        transaction: &DatabaseTransaction,
        command: &CommitModelRoutePromotion,
    ) -> Result<LockedPermit, PromotionCommitError> {
        let permit = PermitEntity::find_by_id(command.promotion_permit_id())
            .lock_exclusive()
            .into_partial_model::<PromotionPermitInfo>()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    "quant_feedback_promotion_permit",
                    command.promotion_permit_id(),
                )
            })?;
        let observed_at = primitives::statement_timestamp(transaction).await?;
        permit.validate()?;
        let scope = permit.scope()?;
        let preflight = command.preflight();
        if permit.status_at(observed_at)? != PromotionPermitStatus::Active
            || permit.revision != 0
            || scope != *preflight.scope()
            || permit.preflight_hash != preflight.preflight_hash()
        {
            return Err(Self::conflict(
                "permit lifecycle, scope, or preflight hash changed before promotion",
            ));
        }
        Ok(LockedPermit {
            permit,
            observed_at,
        })
    }

    async fn lock_runtime(
        transaction: &DatabaseTransaction,
        preflight: &PromotionPreflight,
    ) -> Result<(), PromotionCommitError> {
        let runtime = RuntimeControlEntity::find_by_id(SYSTEM_RUNTIME_CONTROL_ID)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("system_runtime_control", SYSTEM_RUNTIME_CONTROL_ID)
            })?;
        if runtime.quant_runtime_mode != preflight.current_runtime_mode()
            || runtime.revision != preflight.runtime_control_revision()
            || !preflight.scope().allows_mode(runtime.quant_runtime_mode)
        {
            return Err(Self::conflict(
                "runtime mode or runtime-control revision changed before promotion",
            ));
        }
        Ok(())
    }

    async fn lock_stage(
        transaction: &DatabaseTransaction,
        cycle_id: FeedbackCycleId,
        stage: FeedbackStage,
    ) -> Result<LockedStage, PromotionCommitError> {
        let event = StageEventEntity::find()
            .filter(StageEventColumn::FeedbackCycleId.eq(cycle_id))
            .filter(StageEventColumn::Stage.eq(stage))
            .filter(StageEventColumn::EventKind.eq(FeedbackStageEventKind::Succeeded))
            .order_by_desc(StageEventColumn::EventSequence)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                Self::conflict(format!("feedback cycle has no succeeded {stage} event"))
            })?;
        let job_id = event.research_job_id.ok_or_else(|| {
            Self::conflict(format!("succeeded {stage} event has no research job"))
        })?;
        let job = ResearchJobEntity::find_by_id(job_id)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RESEARCH_JOB, job_id))?;
        if job.feedback_cycle_id != Some(cycle_id)
            || job.feedback_stage != Some(stage)
            || job.status != ResearchJobStatus::Succeeded
            || event.evidence_uri.as_ref() != job.result_artifact_uri.as_ref()
            || event.evidence_hash != job.result_artifact_hash
        {
            return Err(Self::conflict(format!(
                "{stage} job and succeeded event no longer form one terminal WORM reference"
            )));
        }
        Ok(LockedStage { event, job })
    }

    fn verify_decision(
        locked: &LockedStage,
        preflight: &PromotionPreflight,
    ) -> Result<(), PromotionCommitError> {
        let ResearchJobParams::FeedbackDecision(params) = &locked.job.params_json else {
            return Err(Self::conflict("Decision job lost its typed parameters"));
        };
        params.validate()?;
        let exact_result = locked.job.kind == ResearchJobKind::FeedbackDecision
            && locked.job.result_kind == Some(ResearchJobResultKind::FeedbackDecisionArtifact)
            && locked.job.result_ref == Some(params.artifact_id.as_uuid())
            && locked.job.result_artifact_hash == Some(preflight.decision_object_hash())
            && params.artifact_id == preflight.decision_artifact_id()
            && params.input_hash()? == preflight.decision_job_input_hash()
            && params.feedback_cycle_id == preflight.feedback_cycle_id()
            && params.cycle_idempotency_hash == preflight.cycle_idempotency_hash()
            && params.shadow.artifact_id == preflight.shadow_artifact_id()
            && params.shadow.artifact.content_hash == preflight.shadow_object_hash();
        if !exact_result || locked.event.evidence_hash != Some(preflight.decision_object_hash()) {
            return Err(Self::conflict(
                "Decision job identity, input, result, or shadow lineage changed",
            ));
        }
        Ok(())
    }

    fn verify_shadow(
        locked: &LockedStage,
        preflight: &PromotionPreflight,
    ) -> Result<(), PromotionCommitError> {
        let ResearchJobParams::FeedbackShadow(params) = &locked.job.params_json else {
            return Err(Self::conflict("Shadow job lost its typed parameters"));
        };
        params.validate()?;
        let FeedbackShadowSubject::Candidate {
            candidate_recipe_hash,
            contract,
        } = &params.subject
        else {
            return Err(Self::conflict(
                "Shadow job no longer carries a candidate subject",
            ));
        };
        let exact_result = locked.job.kind == ResearchJobKind::FeedbackShadow
            && locked.job.result_kind == Some(ResearchJobResultKind::FeedbackShadowArtifact)
            && locked.job.result_ref == Some(params.artifact_id.as_uuid())
            && locked.job.result_artifact_hash == Some(preflight.shadow_object_hash())
            && params.artifact_id == preflight.shadow_artifact_id()
            && params.feedback_cycle_id == preflight.feedback_cycle_id()
            && params.cycle_idempotency_hash == preflight.cycle_idempotency_hash()
            && *candidate_recipe_hash == preflight.candidate_recipe_hash()
            && contract.contract_hash() == preflight.shadow_contract_hash()
            && contract.candidate_model_version_id()
                == preflight.serving_constraints().candidate_model_version_id()
            && contract.candidate_serving_contract_hash()
                == preflight
                    .serving_constraints()
                    .candidate_serving_contract_hash();
        if !exact_result || locked.event.evidence_hash != Some(preflight.shadow_object_hash()) {
            return Err(Self::conflict(
                "Shadow job identity, candidate, contract, or result changed",
            ));
        }
        Ok(())
    }

    async fn lock_evidence(
        transaction: &DatabaseTransaction,
        preflight: &PromotionPreflight,
    ) -> Result<CycleModel, PromotionCommitError> {
        let cycle_id = preflight.feedback_cycle_id();
        let cycle = CycleEntity::find_by_id(cycle_id)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_FEEDBACK_CYCLE, cycle_id))?;
        let info: FeedbackCycleInfo = cycle.clone().into();
        info.validate()?;
        let cycle_exact = cycle.status == FeedbackCycleStatus::Succeeded
            && cycle.decision == Some(FeedbackDecision::CandidateReady)
            && cycle.idempotency_hash == preflight.cycle_idempotency_hash()
            && cycle.profile_ref == *preflight.scope().profile_ref()
            && cycle.champion_model_version_id == preflight.scope().champion_model_version_id()
            && cycle.champion_serving_contract_hash
                == preflight.scope().champion_serving_contract_hash()
            && cycle
                .candidate_family
                .candidate(preflight.candidate_recipe_hash())
                .is_some();
        if !cycle_exact {
            return Err(Self::conflict(
                "terminal CandidateReady cycle or its frozen lineage changed",
            ));
        }
        let decision = Self::lock_stage(transaction, cycle_id, FeedbackStage::Decision).await?;
        let shadow = Self::lock_stage(transaction, cycle_id, FeedbackStage::Shadow).await?;
        Self::verify_decision(&decision, preflight)?;
        Self::verify_shadow(&shadow, preflight)?;
        Ok(cycle)
    }

    async fn lock_models(
        transaction: &DatabaseTransaction,
        preflight: &PromotionPreflight,
    ) -> Result<LockedModels, PromotionCommitError> {
        let constraints = preflight.serving_constraints();
        let spec = ModelSpecEntity::find_by_id(constraints.candidate_model_spec_id())
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("quant_model_spec", constraints.candidate_model_spec_id())
            })?;
        if spec.definition_hash != constraints.candidate_spec_hash() {
            return Err(Self::conflict(
                "candidate model-spec definition changed before promotion",
            ));
        }

        let champion_id = preflight.scope().champion_model_version_id();
        let candidate_id = constraints.candidate_model_version_id();
        let mut ids = [champion_id, candidate_id];
        ids.sort_by_key(|id| id.as_uuid());
        let locked = ModelVersionEntity::find()
            .filter(ModelVersionColumn::ModelVersionId.is_in(ids))
            .order_by_asc(ModelVersionColumn::ModelVersionId)
            .lock_exclusive()
            .all(transaction)
            .await
            .map_err(StorageError::from)?;
        if locked.len() != 2 {
            return Err(Self::conflict(
                "champion and candidate model locks did not resolve exactly two rows",
            ));
        }
        let candidate_row = locked
            .iter()
            .find(|model| model.model_version_id == candidate_id)
            .ok_or_else(|| Self::conflict("candidate model lock disappeared"))?;
        let champion =
            PgModelRegistryRepository::require_version_info(transaction, &champion_id).await?;
        let candidate =
            PgModelRegistryRepository::require_version_info(transaction, &candidate_id).await?;
        preflight.scope().validate_champion(&champion)?;
        constraints.validate_model(&candidate)?;
        let evidence_hash = PgModelRegistryRepository::verify_parity_permit(
            transaction,
            &constraints.feature_parity_run_id(),
            candidate_row,
        )
        .await?;
        if evidence_hash != constraints.feature_parity_evidence_hash() {
            return Err(Self::conflict(
                "candidate full-parity subject differs from the frozen preflight",
            ));
        }
        Ok(LockedModels {
            champion,
            candidate,
        })
    }

    async fn lock_manifest(
        transaction: &DatabaseTransaction,
        preflight: &PromotionPreflight,
        candidate: &ModelVersionInfo,
    ) -> Result<(), PromotionCommitError> {
        let constraints = preflight.serving_constraints();
        let manifest = CandidateManifestEntity::find_by_id(constraints.candidate_manifest_id())
            .lock_shared()
            .into_partial_model::<ModelCandidateManifestInfo>()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    "quant_model_candidate_manifest",
                    constraints.candidate_manifest_id(),
                )
            })?;
        manifest
            .validate()
            .map_err(|error| Self::conflict(error.to_string()))?;
        let document = &manifest.document;
        if manifest.manifest_hash != constraints.candidate_manifest_hash()
            || manifest.promotion_gate_hash != constraints.promotion_gate_hash()
            || manifest.feedback_cycle_id != preflight.feedback_cycle_id()
            || manifest.candidate_recipe_hash != preflight.candidate_recipe_hash()
            || manifest.model_version_id != candidate.model_version_id
            || document.model_spec_id != candidate.model_spec_id
            || document.model_family != candidate.model_family
            || document.model_artifact_hash != candidate.artifact_hash
            || document.serving_contract_hash != candidate.serving_contract_hash
            || document.training_dataset_id != constraints.candidate_training_dataset_id()
            || document.promotion_gate.feature_parity_run_id != constraints.feature_parity_run_id()
            || document.promotion_gate.feature_parity_state_id
                != constraints.feature_parity_state_id()
            || document.promotion_gate.feature_parity_evidence_hash
                != constraints.feature_parity_evidence_hash()
            || document.profile_ref != *constraints.profile_ref()
            || document.category != constraints.category()
        {
            return Err(Self::conflict(
                "candidate manifest differs from the frozen promotion preflight",
            ));
        }
        Ok(())
    }

    async fn lock_parity(
        transaction: &DatabaseTransaction,
        preflight: &PromotionPreflight,
    ) -> Result<(), PromotionCommitError> {
        PgFeatureParityRepository::verify_clear_latch_generation(
            transaction,
            &preflight.serving_constraints().feature_parity_state_id(),
        )
        .await
        .map_err(Into::into)
    }

    fn passed_preflight() -> Vec<PolicyPreflightResult> {
        [
            (
                PolicyPreflightCheckKind::TypedSchema,
                PolicyPreflightDetailCode::TypedDocumentDecoded,
            ),
            (
                PolicyPreflightCheckKind::SemanticValidation,
                PolicyPreflightDetailCode::SemanticValidationPassed,
            ),
            (
                PolicyPreflightCheckKind::ConsumerPreparation,
                PolicyPreflightDetailCode::ConsumerPreparationPassed,
            ),
            (
                PolicyPreflightCheckKind::ArtifactCompatibility,
                PolicyPreflightDetailCode::ConsumerPreparationPassed,
            ),
        ]
        .into_iter()
        .map(|(check, detail_code)| PolicyPreflightResult {
            check,
            outcome: CheckOutcome::Passed,
            detail_code,
            failure_detail: None,
        })
        .collect()
    }

    fn snapshot_row(
        snapshot: &DecisionPolicySnapshot,
        snapshot_id: DecisionPolicySnapshotId,
        snapshot_hash: ContentHash,
        record: &ModelRoutePromotionRecord,
    ) -> Result<NewDecisionPolicySnapshot, PromotionCommitError> {
        let document = snapshot
            .persistence_document()
            .map_err(|error| Self::conflict(error.to_string()))?;
        let revisions = &document.revisions;
        let required = |revision: Option<PolicyRevisionId>, kind: ConfigResourceKind| {
            revision
                .ok_or_else(|| Self::conflict(format!("candidate snapshot has no {kind} revision")))
        };
        Ok(NewDecisionPolicySnapshot {
            decision_policy_snapshot_id: snapshot_id,
            snapshot_hash,
            recommendation_policy_revision_id: required(
                revisions.recommendation_policy,
                ConfigResourceKind::RecommendationPolicy,
            )?,
            execution_risk_policy_revision_id: required(
                revisions.execution_risk_policy,
                ConfigResourceKind::ExecutionRiskPolicy,
            )?,
            model_routing_revision_id: required(
                revisions.model_routing,
                ConfigResourceKind::ModelRouting,
            )?,
            report_schedule_revision_id: required(
                revisions.report_schedule,
                ConfigResourceKind::ReportSchedule,
            )?,
            operational_control_revision_id: required(
                revisions.operational_control,
                ConfigResourceKind::OperationalControl,
            )?,
            execution_authorization_revision_id: required(
                revisions.execution_authorization,
                ConfigResourceKind::ExecutionAuthorization,
            )?,
            snapshot: document,
            source: DecisionPolicySnapshotSource::Activation,
            created_by_kind: PolicyActorKind::Operator,
            created_by_user_id: Some(record.actor_user_id()),
            created_by_label: record.actor_username().to_owned(),
            reason: record.audit_reason(),
        })
    }

    fn build_rows(
        permit: &PromotionPermitInfo,
        command: &CommitModelRoutePromotion,
        authorized: &AuthorizedGovernedActor,
        current: &ActivePolicyBundle,
        projection: &PromotionPolicyProjection,
        models: &LockedModels,
        database_now: DateTime<Utc>,
    ) -> Result<PromotionRows, PromotionCommitError> {
        let preflight = command.preflight();
        let old_revision = current
            .snapshot
            .resource_revision_id(ConfigResourceKind::ModelRouting)
            .copied()
            .ok_or_else(|| Self::conflict("active snapshot has no ModelRouting revision"))?;
        let new_revision = PolicyRevisionId::from_v7();
        let new_generation = current
            .generation
            .checked_next()
            .map_err(|error| Self::conflict(error.to_string()))?;
        let mut candidate_snapshot = projection.prospective_snapshot().clone();
        candidate_snapshot.set_resource_revision_id(ConfigResourceKind::ModelRouting, new_revision);
        projection.validate_candidate(&candidate_snapshot)?;
        let validation = candidate_snapshot.validate_runtime_config();
        if validation.has_errors() {
            return Err(Self::conflict(format!(
                "candidate policy snapshot is invalid: {validation}"
            )));
        }
        let snapshot_hash = candidate_snapshot
            .persistence_hash()
            .map_err(|error| Self::conflict(error.to_string()))?;
        let snapshot_id = DecisionPolicySnapshotId::from_content_hash(&snapshot_hash);
        let policy_approval_id = PolicyApprovalId::from_v7();
        let policy_activation_id = PolicyActivationId::from_v7();
        let policy = ModelRoutePromotionPolicy {
            previous_generation: current.generation,
            transaction_revision: new_generation,
            previous_snapshot_id: current.decision_policy_snapshot_id,
            previous_snapshot_hash: current.snapshot_hash,
            committed_snapshot_id: snapshot_id,
            committed_snapshot_hash: snapshot_hash,
            previous_model_routing_revision_id: old_revision,
            committed_model_routing_revision_id: new_revision,
            policy_approval_id,
            policy_activation_id,
        };
        let route = ModelRoutePromotionRoute {
            category: projection.category(),
            champion_model_version_id: models.champion.model_version_id,
            champion_artifact_hash: models.champion.artifact_hash,
            champion_serving_contract_hash: models.champion.serving_contract_hash,
            candidate_model_version_id: models.candidate.model_version_id,
            candidate_artifact_hash: models.candidate.artifact_hash,
            candidate_serving_contract_hash: models.candidate.serving_contract_hash,
            consumed_shadow_model_version_id: models.candidate.model_version_id,
        };
        let record = ModelRoutePromotionRecord::try_seal(ModelRoutePromotionRecordInput {
            promotion_permit_id: permit.promotion_permit_id,
            permit_issuance_hash: permit.issuance_hash,
            permit_issued_at: permit.issued_at,
            preflight: preflight.clone(),
            actor_user_id: authorized.user_id,
            actor_username: authorized.username.clone(),
            actor_role: authorized.role.clone(),
            idempotency_key: command.idempotency_key().clone(),
            reason_code: command.reason_code().to_owned(),
            note: command.note().to_owned(),
            route,
            policy,
        })?;
        Self::finish_rows(current, &candidate_snapshot, models, database_now, record)
    }

    fn finish_rows(
        current: &ActivePolicyBundle,
        candidate_snapshot: &DecisionPolicySnapshot,
        models: &LockedModels,
        database_now: DateTime<Utc>,
        record: ModelRoutePromotionRecord,
    ) -> Result<PromotionRows, PromotionCommitError> {
        let policy = record.policy();
        let transaction_hash = record.transaction_hash();
        let audit_event_id = AuditEventId::from_content_hash(&transaction_hash);
        let model_audit_id = ModelGovernanceAuditId::from_content_hash(&transaction_hash);
        let model_document = PolicyDocument::ModelRouting(candidate_snapshot.model_routing.clone());
        let revision_hash =
            CanonicalDigest::content_hash_json(&model_document).map_err(FeedbackError::from)?;
        let subject = PolicyValidationSubject {
            base_generation: current.generation,
            base_revision_vector: current.revision_vector.clone(),
            candidate_bundle_hash: policy.committed_snapshot_hash,
        };
        let evidence = PolicyValidationEvidence {
            subject: Some(subject.clone()),
            issues: Vec::new(),
            preflight: Self::passed_preflight(),
        };
        let snapshot = Self::snapshot_row(
            candidate_snapshot,
            policy.committed_snapshot_id,
            policy.committed_snapshot_hash,
            &record,
        )?;
        let audit_reason = record.audit_reason();
        let revision = NewPolicyRevision {
            policy_revision_id: policy.committed_model_routing_revision_id,
            resource_kind: ConfigResourceKind::ModelRouting,
            schema_version: candidate_snapshot.model_routing.schema_version,
            revision_hash,
            document: model_document,
            status: PolicyRevisionStatus::Validated,
            validation_evidence: Some(evidence),
            validated_at: Some(database_now),
            preflight_token_hash: Some(record.preflight().preflight_hash()),
            preflight_expires_at: Some(record.preflight().scope().expires_at()),
            created_by_kind: PolicyActorKind::Operator,
            created_by_user_id: Some(record.actor_user_id()),
            created_by_label: record.actor_username().to_owned(),
            reason: audit_reason.clone(),
        };
        let approval = NewPolicyApproval {
            policy_approval_id: policy.policy_approval_id,
            policy_revision_id: policy.committed_model_routing_revision_id,
            resource_kind: ConfigResourceKind::ModelRouting,
            revision_hash,
            validation_subject: Some(subject),
            decision: PolicyApprovalDecision::Approved,
            decided_by_kind: PolicyActorKind::Operator,
            decided_by_user_id: Some(record.actor_user_id()),
            decided_by_label: record.actor_username().to_owned(),
            reason: audit_reason.clone(),
            decided_at: database_now,
            expires_at: Some(record.preflight().scope().expires_at()),
        };
        let audit = NewRoutePromotionAudit {
            audit_id: model_audit_id,
            model_version_id: Some(models.candidate.model_version_id),
            training_dataset_id: models.candidate.training_dataset_id,
            action: ModelGovernanceAction::PromoteRoute,
            actor_user_id: Some(record.actor_user_id()),
            actor_username: record.actor_username().to_owned(),
            actor_role: Some(record.actor_role().clone()),
            reason: audit_reason.clone(),
            detail: ModelGovernanceAuditDetail::PromoteRoute {
                record: Box::new(record.clone()),
            },
            audit_event_id,
            promotion_permit_id: record.promotion_permit_id(),
            promotion_transaction_hash: transaction_hash,
        };
        let activation = NewModelPromotionActivation {
            bundle_generation: policy.transaction_revision,
            expected_bundle_generation: policy.previous_generation,
            policy_activation_id: policy.policy_activation_id,
            resource_kind: ConfigResourceKind::ModelRouting,
            policy_revision_id: policy.committed_model_routing_revision_id,
            decision_policy_snapshot_id: policy.committed_snapshot_id,
            policy_approval_id: policy.policy_approval_id,
            activated_by_kind: PolicyActorKind::Operator,
            activated_by_user_id: record.actor_user_id(),
            activated_by_label: record.actor_username().to_owned(),
            reason: audit_reason,
            activation_kind: PolicyActivationKind::ModelPromotion,
            expected_active_revision_id: policy.previous_model_routing_revision_id,
            previous_policy_revision_id: policy.previous_model_routing_revision_id,
            rollback_target_revision_id: None,
            preflight_token_hash: record.preflight().preflight_hash(),
            idempotency_key: record.idempotency_key().clone(),
            activation_request_hash: transaction_hash,
            audit_event_id,
            promotion_permit_id: record.promotion_permit_id(),
            promotion_transaction_hash: transaction_hash,
            model_governance_audit_id: model_audit_id,
        };
        Ok(PromotionRows {
            snapshot,
            revision,
            approval,
            activation,
            audit,
            record,
        })
    }

    async fn insert_rows(
        transaction: &DatabaseTransaction,
        guard: &ActivationGuardModel,
        cycle: &CycleModel,
        rows: PromotionRows,
        database_now: DateTime<Utc>,
    ) -> Result<ResolvedCommit, PromotionCommitError> {
        RevisionEntity::insert(rows.revision.into_active_model())
            .exec(transaction)
            .await
            .map_err(StorageError::from)?;
        ApprovalEntity::insert(rows.approval.into_active_model())
            .exec(transaction)
            .await
            .map_err(StorageError::from)?;
        PgPolicyRepository::insert_snapshot_if_absent(transaction, rows.snapshot.clone()).await?;
        ModelAuditEntity::insert(rows.audit.into_active_model())
            .exec(transaction)
            .await
            .map_err(StorageError::from)?;
        let activation = ActivationEntity::insert(rows.activation.into_active_model())
            .exec_with_returning(transaction)
            .await
            .map_err(StorageError::from)?;
        PgPolicyRepository::insert_activation_ledger(transaction, &activation, &rows.snapshot)
            .await?;
        Self::advance_guard(transaction, guard, &rows.snapshot, database_now).await?;
        Self::mark_promoted(transaction, cycle).await?;
        let resolved = Self::load_commit(transaction, rows.record.promotion_permit_id())
            .await?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(POLICY_ACTIVATION),
                    "committed promotion graph disappeared before transaction commit",
                )
            })?;
        if resolved.record != rows.record {
            return Err(Self::conflict(
                "inserted promotion graph differs from the sealed transaction record",
            ));
        }
        Ok(resolved)
    }

    async fn advance_guard(
        transaction: &DatabaseTransaction,
        guard: &ActivationGuardModel,
        snapshot: &NewDecisionPolicySnapshot,
        database_now: DateTime<Utc>,
    ) -> Result<(), PromotionCommitError> {
        let next = guard
            .generation
            .checked_next()
            .map_err(|error| Self::conflict(error.to_string()))?;
        let updated = ActivationGuardEntity::update_many()
            .col_expr(ActivationGuardColumn::Generation, Expr::value(next))
            .col_expr(
                ActivationGuardColumn::CurrentSnapshotId,
                Expr::value(Some(snapshot.decision_policy_snapshot_id)),
            )
            .col_expr(
                ActivationGuardColumn::CurrentSnapshotHash,
                Expr::value(Some(snapshot.snapshot_hash)),
            )
            .col_expr(ActivationGuardColumn::UpdatedAt, Expr::value(database_now))
            .filter(ActivationGuardColumn::Id.eq(guard.id))
            .filter(ActivationGuardColumn::Generation.eq(guard.generation))
            .filter(ActivationGuardColumn::CurrentSnapshotId.eq(guard.current_snapshot_id))
            .filter(ActivationGuardColumn::CurrentSnapshotHash.eq(guard.current_snapshot_hash))
            .exec(transaction)
            .await
            .map_err(StorageError::from)?;
        if updated.rows_affected != 1 {
            return Err(Self::conflict(
                "policy activation guard CAS affected no row",
            ));
        }
        Ok(())
    }

    async fn mark_promoted(
        transaction: &DatabaseTransaction,
        cycle: &CycleModel,
    ) -> Result<(), PromotionCommitError> {
        let updated = CycleEntity::update_many()
            .col_expr(
                CycleColumn::Decision,
                primitives::enum_value(&FeedbackDecision::Promoted),
            )
            .col_expr(
                CycleColumn::Generation,
                Expr::col(CycleColumn::Generation).add(1),
            )
            .filter(CycleColumn::FeedbackCycleId.eq(cycle.feedback_cycle_id))
            .filter(CycleColumn::Status.eq(FeedbackCycleStatus::Succeeded))
            .filter(CycleColumn::Decision.eq(FeedbackDecision::CandidateReady))
            .filter(CycleColumn::Generation.eq(cycle.generation))
            .exec(transaction)
            .await
            .map_err(StorageError::from)?;
        if updated.rows_affected != 1 {
            return Err(Self::conflict(
                "CandidateReady-to-Promoted cycle CAS affected no row",
            ));
        }
        Ok(())
    }

    fn promotion_record(
        audit: &ModelAuditModel,
    ) -> Result<ModelRoutePromotionRecord, PromotionCommitError> {
        let ModelGovernanceAuditDetail::PromoteRoute { record } = &audit.detail else {
            return Err(Self::conflict(
                "promotion audit lost its typed transaction record",
            ));
        };
        record.validate()?;
        Ok(record.as_ref().clone())
    }

    async fn load_commit(
        db: &impl ConnectionTrait,
        permit_id: PromotionPermitId,
    ) -> Result<Option<ResolvedCommit>, PromotionCommitError> {
        let Some(activation) = ActivationEntity::find()
            .filter(ActivationColumn::PromotionPermitId.eq(permit_id))
            .one(db)
            .await
            .map_err(StorageError::from)?
        else {
            return Ok(None);
        };
        let audit_id = activation.model_governance_audit_id.ok_or_else(|| {
            Self::conflict("model-promotion activation has no model audit identity")
        })?;
        let audit = ModelAuditEntity::find_by_id(audit_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("quant_model_governance_audit", audit_id))?;
        let record = Self::promotion_record(&audit)?;
        Self::verify_activation(db, &activation, &audit, &record).await?;
        let snapshot_model = SnapshotEntity::find_by_id(activation.decision_policy_snapshot_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    "decision_policy_snapshot",
                    activation.decision_policy_snapshot_id,
                )
            })?;
        let snapshot = PgPolicyRepository::resolve_snapshot_model(db, snapshot_model).await?;
        let bundle = ActivePolicyBundle::from_parts(
            activation.bundle_generation,
            snapshot.decision_policy_snapshot_id,
            snapshot.snapshot_hash,
            snapshot.snapshot,
        );
        Self::verify_policy(db, &activation, &record, &bundle).await?;
        Self::verify_cycle(db, &record).await?;
        let audit_info: ModelGovernanceAuditInfo = audit.into();
        Ok(Some(ResolvedCommit {
            commit: ModelRoutePromotionCommit {
                activation: activation.into(),
                bundle,
                audit: audit_info,
                transaction_hash: record.transaction_hash(),
                outcome: ModelRoutePromotionOutcome::ExactReplay,
            },
            record,
        }))
    }

    async fn verify_activation(
        db: &impl ConnectionTrait,
        activation: &ActivationModel,
        audit: &ModelAuditModel,
        record: &ModelRoutePromotionRecord,
    ) -> Result<(), PromotionCommitError> {
        let policy = record.policy();
        let route = record.route();
        let transaction_hash = record.transaction_hash();
        let audit_reason = record.audit_reason();
        let audit_exact = audit.audit_id
            == ModelGovernanceAuditId::from_content_hash(&transaction_hash)
            && audit.model_version_id == Some(route.candidate_model_version_id)
            && audit.training_dataset_id
                == Some(
                    record
                        .preflight()
                        .serving_constraints()
                        .candidate_training_dataset_id(),
                )
            && audit.action == ModelGovernanceAction::PromoteRoute
            && audit.actor_user_id == Some(record.actor_user_id())
            && audit.actor_username == record.actor_username()
            && audit.actor_role.as_ref() == Some(record.actor_role())
            && audit.reason == audit_reason
            && audit.audit_event_id == AuditEventId::from_content_hash(&transaction_hash)
            && audit.promotion_permit_id == Some(record.promotion_permit_id())
            && audit.promotion_transaction_hash == Some(transaction_hash);
        let activation_exact = activation.activation_kind == PolicyActivationKind::ModelPromotion
            && activation.resource_kind == ConfigResourceKind::ModelRouting
            && activation.bundle_generation == policy.transaction_revision
            && activation.expected_bundle_generation == policy.previous_generation
            && activation.policy_activation_id == policy.policy_activation_id
            && activation.policy_revision_id == policy.committed_model_routing_revision_id
            && activation.decision_policy_snapshot_id == policy.committed_snapshot_id
            && activation.policy_approval_id == policy.policy_approval_id
            && activation.activated_by_kind == PolicyActorKind::Operator
            && activation.activated_by_user_id == Some(record.actor_user_id())
            && activation.activated_by_label == record.actor_username()
            && activation.reason == audit_reason
            && activation.expected_active_revision_id
                == Some(policy.previous_model_routing_revision_id)
            && activation.previous_policy_revision_id
                == Some(policy.previous_model_routing_revision_id)
            && activation.rollback_target_revision_id.is_none()
            && activation.preflight_token_hash == record.preflight().preflight_hash()
            && activation.idempotency_key == *record.idempotency_key()
            && activation.activation_request_hash == transaction_hash
            && activation.audit_event_id == audit.audit_event_id
            && activation.promotion_permit_id == Some(record.promotion_permit_id())
            && activation.promotion_transaction_hash == Some(transaction_hash)
            && activation.model_governance_audit_id == Some(audit.audit_id);
        if !audit_exact || !activation_exact {
            return Err(Self::conflict(
                "model audit or activation differs from the promotion transaction record",
            ));
        }
        Self::verify_ledgers(db, activation).await
    }

    async fn verify_ledgers(
        db: &impl ConnectionTrait,
        activation: &ActivationModel,
    ) -> Result<(), PromotionCommitError> {
        let audit = ActivationAuditEntity::find_by_id(activation.audit_event_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("policy_activation_audit", activation.audit_event_id)
            })?;
        let outbox = ActivationOutboxEntity::find_by_id(activation.audit_event_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("policy_activation_event_outbox", activation.audit_event_id)
            })?;
        let actor_exact = (
            &audit.actor_kind,
            &audit.actor_user_id,
            audit.actor_label.as_str(),
            audit.reason.as_str(),
        ) == (
            &activation.activated_by_kind,
            &activation.activated_by_user_id,
            activation.activated_by_label.as_str(),
            activation.reason.as_str(),
        );
        let occurrence_exact = audit.occurred_at == activation.activated_at;
        let audit_exact = audit.policy_activation_id == activation.policy_activation_id
            && audit.bundle_generation == activation.bundle_generation
            && audit.resource_kind == activation.resource_kind
            && audit.policy_revision_id == activation.policy_revision_id
            && audit.decision_policy_snapshot_id == activation.decision_policy_snapshot_id
            && audit.activation_request_hash == activation.activation_request_hash
            && actor_exact
            && occurrence_exact
            && audit.promotion_permit_id == activation.promotion_permit_id
            && audit.promotion_transaction_hash == activation.promotion_transaction_hash
            && audit.model_governance_audit_id == activation.model_governance_audit_id;
        let outbox_exact = outbox.policy_activation_id == activation.policy_activation_id
            && outbox.bundle_generation == activation.bundle_generation
            && outbox.decision_policy_snapshot_id == activation.decision_policy_snapshot_id
            && outbox.snapshot_hash == audit.snapshot_hash
            && outbox.promotion_permit_id == activation.promotion_permit_id
            && outbox.promotion_transaction_hash == activation.promotion_transaction_hash
            && outbox.model_governance_audit_id == activation.model_governance_audit_id;
        if !audit_exact || !outbox_exact {
            return Err(Self::conflict(
                "policy audit or durable outbox differs from its activation",
            ));
        }
        Ok(())
    }

    async fn verify_policy(
        db: &impl ConnectionTrait,
        activation: &ActivationModel,
        record: &ModelRoutePromotionRecord,
        committed: &ActivePolicyBundle,
    ) -> Result<(), PromotionCommitError> {
        let policy = record.policy();
        if committed.generation != policy.transaction_revision
            || committed.decision_policy_snapshot_id != policy.committed_snapshot_id
            || committed.snapshot_hash != policy.committed_snapshot_hash
            || committed
                .snapshot
                .resource_revision_id(ConfigResourceKind::ModelRouting)
                != Some(&policy.committed_model_routing_revision_id)
        {
            return Err(Self::conflict(
                "committed policy bundle differs from the promotion record",
            ));
        }
        let previous = Self::load_historical_bundle(
            db,
            policy.previous_generation,
            policy.previous_snapshot_id,
            policy.previous_snapshot_hash,
        )
        .await?;
        let projection = PromotionPolicyProjection::try_new(
            &previous,
            record.route().category,
            record.route().candidate_model_version_id,
        )?;
        projection.validate_candidate(&committed.snapshot)?;
        if previous
            .snapshot
            .resource_revision_id(ConfigResourceKind::ModelRouting)
            != Some(&policy.previous_model_routing_revision_id)
            || projection.non_route_policy_hash()
                != record.preflight().scope().non_route_policy_hash()
        {
            return Err(Self::conflict(
                "previous policy bundle or non-route projection differs from the record",
            ));
        }
        Self::verify_revision(db, activation, record, &previous, committed).await
    }

    async fn load_historical_bundle(
        db: &impl ConnectionTrait,
        generation: PolicyBundleGeneration,
        snapshot_id: DecisionPolicySnapshotId,
        snapshot_hash: ContentHash,
    ) -> Result<ActivePolicyBundle, PromotionCommitError> {
        let model: SnapshotModel = SnapshotEntity::find_by_id(snapshot_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("decision_policy_snapshot", snapshot_id))?;
        let snapshot = PgPolicyRepository::resolve_snapshot_model(db, model).await?;
        if snapshot.snapshot_hash != snapshot_hash {
            return Err(Self::conflict(
                "historical policy snapshot hash differs from the promotion record",
            ));
        }
        Ok(ActivePolicyBundle::from_parts(
            generation,
            snapshot.decision_policy_snapshot_id,
            snapshot.snapshot_hash,
            snapshot.snapshot,
        ))
    }

    async fn verify_revision(
        db: &impl ConnectionTrait,
        activation: &ActivationModel,
        record: &ModelRoutePromotionRecord,
        previous: &ActivePolicyBundle,
        committed: &ActivePolicyBundle,
    ) -> Result<(), PromotionCommitError> {
        let revision = RevisionEntity::find_by_id(activation.policy_revision_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("policy_revision", activation.policy_revision_id)
            })?;
        let approval = ApprovalEntity::find_by_id(activation.policy_approval_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("policy_approval", activation.policy_approval_id)
            })?;
        let document = committed
            .snapshot
            .resource_document(ConfigResourceKind::ModelRouting);
        let revision_hash =
            CanonicalDigest::content_hash_json(&document).map_err(FeedbackError::from)?;
        let subject = PolicyValidationSubject {
            base_generation: previous.generation,
            base_revision_vector: previous.revision_vector.clone(),
            candidate_bundle_hash: committed.snapshot_hash,
        };
        let evidence = PolicyValidationEvidence {
            subject: Some(subject.clone()),
            issues: Vec::new(),
            preflight: Self::passed_preflight(),
        };
        let audit_reason = record.audit_reason();
        let revision_exact = revision.resource_kind == ConfigResourceKind::ModelRouting
            && revision.revision_hash == revision_hash
            && revision.document == document
            && revision.status == PolicyRevisionStatus::Validated
            && revision.validation_evidence == Some(evidence)
            && revision.validated_at.is_some()
            && revision.preflight_token_hash == Some(record.preflight().preflight_hash())
            && revision.preflight_expires_at == Some(record.preflight().scope().expires_at())
            && revision.created_by_kind == PolicyActorKind::Operator
            && revision.created_by_user_id == Some(record.actor_user_id())
            && revision.created_by_label == record.actor_username()
            && revision.reason == audit_reason;
        let approval_exact = approval.policy_revision_id == revision.policy_revision_id
            && approval.resource_kind == ConfigResourceKind::ModelRouting
            && approval.revision_hash == revision_hash
            && approval.validation_subject == Some(subject)
            && approval.decision == PolicyApprovalDecision::Approved
            && approval.decided_by_kind == PolicyActorKind::Operator
            && approval.decided_by_user_id == Some(record.actor_user_id())
            && approval.decided_by_label == record.actor_username()
            && approval.reason == audit_reason
            && approval.expires_at == Some(record.preflight().scope().expires_at());
        if !revision_exact || !approval_exact {
            return Err(Self::conflict(
                "policy revision or permit-derived approval differs from the record",
            ));
        }
        Ok(())
    }

    async fn verify_cycle(
        db: &impl ConnectionTrait,
        record: &ModelRoutePromotionRecord,
    ) -> Result<(), PromotionCommitError> {
        let cycle_id = record.preflight().feedback_cycle_id();
        let cycle = CycleEntity::find_by_id(cycle_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_FEEDBACK_CYCLE, cycle_id))?;
        if cycle.status != FeedbackCycleStatus::Succeeded
            || cycle.decision != Some(FeedbackDecision::Promoted)
            || cycle.idempotency_hash != record.preflight().cycle_idempotency_hash()
        {
            return Err(Self::conflict(
                "committed promotion is not reflected by the exact Promoted cycle",
            ));
        }
        Ok(())
    }

    async fn current_projection(
        transaction: &DatabaseTransaction,
        guard: &ActivationGuardModel,
        preflight: &PromotionPreflight,
    ) -> Result<(ActivePolicyBundle, PromotionPolicyProjection), PromotionCommitError> {
        let scope = preflight.scope();
        let bundle = PgPolicyRepository::load_current_bundle_from(transaction)
            .await?
            .ok_or_else(|| Self::conflict("no current policy bundle exists"))?;
        let guard_exact = guard.generation == scope.expected_policy_generation()
            && guard.current_snapshot_id == Some(scope.expected_snapshot_id())
            && guard.current_snapshot_hash == Some(scope.expected_snapshot_hash());
        if !guard_exact
            || bundle.generation != scope.expected_policy_generation()
            || bundle.decision_policy_snapshot_id != scope.expected_snapshot_id()
            || bundle.snapshot_hash != scope.expected_snapshot_hash()
        {
            return Err(Self::conflict(
                "policy generation, snapshot, or activation guard changed",
            ));
        }
        let projection = PromotionPolicyProjection::try_new(
            &bundle,
            scope.category(),
            preflight.serving_constraints().candidate_model_version_id(),
        )?;
        if projection.non_route_policy_hash() != scope.non_route_policy_hash()
            || projection.champion_model_version_id() != scope.champion_model_version_id()
        {
            return Err(Self::conflict(
                "category route, champion, or non-route policy changed",
            ));
        }
        Ok((bundle, projection))
    }

    fn command_matches(
        record: &ModelRoutePromotionRecord,
        command: &CommitModelRoutePromotion,
        authorized: &AuthorizedGovernedActor,
    ) -> bool {
        record.preflight() == command.preflight()
            && record.actor_user_id() == authorized.user_id
            && record.actor_username() == authorized.username
            && record.actor_role() == &authorized.role
            && record.idempotency_key() == command.idempotency_key()
            && record.reason_code() == command.reason_code()
            && record.note() == command.note()
    }
}

#[async_trait]
impl ModelRoutePromotionRepository for PgModelRoutePromotionRepository {
    async fn find_committed(
        &self,
        promotion_permit_id: &PromotionPermitId,
        feedback_cycle_id: &FeedbackCycleId,
    ) -> Result<Option<ModelRoutePromotionCommit>, PromotionCommitError> {
        let Some(resolved) = Self::load_commit(&self.db, *promotion_permit_id).await? else {
            return Ok(None);
        };
        if resolved.record.preflight().feedback_cycle_id() != *feedback_cycle_id {
            return Err(Self::conflict(
                "promotion permit is already committed for a different feedback cycle",
            ));
        }
        Ok(Some(resolved.commit))
    }

    async fn commit(
        &self,
        command: CommitModelRoutePromotion,
    ) -> Result<ModelRoutePromotionCommit, PromotionCommitError> {
        command.preflight().validate()?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let authorized = authorization::authorize_actor::<PromotionCommitError>(
            &transaction,
            command.actor().user_id,
            &command.actor().acting_role,
            ResourceType::Publication,
            Operation::Publish,
        )
        .await?;
        let guard = PgPolicyRepository::acquire_activation_lock(&transaction).await?;
        if let Some(resolved) =
            Self::load_commit(&transaction, command.promotion_permit_id()).await?
        {
            if !Self::command_matches(&resolved.record, &command, &authorized) {
                return Err(Self::conflict(
                    "promotion permit is committed with a different activation request",
                ));
            }
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(resolved.commit);
        }
        let permit = Self::lock_permit(&transaction, &command).await?;
        Self::lock_runtime(&transaction, command.preflight()).await?;
        let cycle = Self::lock_evidence(&transaction, command.preflight()).await?;
        let (bundle, projection) =
            Self::current_projection(&transaction, &guard, command.preflight()).await?;
        Self::lock_parity(&transaction, command.preflight()).await?;
        let models = Self::lock_models(&transaction, command.preflight()).await?;
        Self::lock_manifest(&transaction, command.preflight(), &models.candidate).await?;
        let rows = Self::build_rows(
            &permit.permit,
            &command,
            &authorized,
            &bundle,
            &projection,
            &models,
            permit.observed_at,
        )?;
        let mut resolved = Box::pin(Self::insert_rows(
            &transaction,
            &guard,
            &cycle,
            rows,
            permit.observed_at,
        ))
        .await?;
        transaction.commit().await.map_err(StorageError::from)?;
        resolved.commit.outcome = ModelRoutePromotionOutcome::Committed;
        Ok(resolved.commit)
    }
}
