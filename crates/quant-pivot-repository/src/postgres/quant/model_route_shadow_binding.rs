//! Atomic `PostgreSQL` owner of one route-owned shadow binding.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{
    QuantError, QuantResult,
    feedback::FeedbackError,
    storage::{
        StorageError,
        entity::{
            POLICY_ACTIVATION, QUANT_FEEDBACK_CYCLE, QUANT_MODEL_CANDIDATE_MANIFEST,
            QUANT_MODEL_ROUTE_SHADOW_BINDING, QUANT_RESEARCH_JOB,
        },
    },
};
use quant_pivot_models::{
    domain::{
        governance::{
            NewDecisionPolicySnapshot, NewPolicyActivation, NewPolicyApproval, NewPolicyRevision,
            PolicyActivationInfo,
        },
        ports::{
            CancelShadowBinding, FeedbackComparisonArtifactRef, RejectShadowBinding,
            ShadowBindingCancellationReceipt, ShadowBindingJobParams, ShadowBindingLifecycle,
            ShadowBindingReceipt, ShadowBindingReceiptInput, ShadowBindingRejectionReceipt,
        },
        quant::ModelCandidateManifestInfo,
    },
    entities::{
        decision_policy_snapshot::Entity as SnapshotEntity,
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
        quant_feedback_cycle::{Entity as CycleEntity, Model as CycleModel},
        quant_feedback_stage_event::{
            Column as StageEventColumn, Entity as StageEventEntity, Model as StageEventModel,
        },
        quant_model_candidate_manifest::{
            Entity as CandidateManifestEntity, Model as CandidateManifestModel,
        },
        quant_model_route_shadow_binding::{
            Column as ShadowBindingColumn, Entity as ShadowBindingEntity,
            Model as ShadowBindingModel,
        },
        quant_research_job::{Entity as ResearchJobEntity, Model as ResearchJobModel},
    },
    enums::{
        quant::{
            FeedbackCycleStatus, FeedbackStage, FeedbackStageEventKind, ResearchJobKind,
            ResearchJobResultKind, ResearchJobStatus, ShadowBindingStatus,
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
        ActivePolicyBundle, DecisionPolicySnapshot, ModelBinding, ModelBindingSource,
        PolicyDocument, PolicyPreflightResult, PolicyValidationEvidence, PolicyValidationSubject,
    },
    types::{
        AuditEventId, ContentHash, DecisionPolicySnapshotId, PolicyActivationId, PolicyApprovalId,
        PolicyBundleGeneration, PolicyIdempotencyKey, PolicyRevisionId, ResearchJobParams,
        RoleCode, ShadowBindingArtifactId,
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait, sea_query::Expr,
};

use crate::{
    postgres::{
        authorization::{self, AuthorizedGovernedActor},
        governance::PgPolicyRepository,
        primitives,
        quant::model_registry::PgModelRegistryRepository,
    },
    traits::{
        ModelRouteShadowBindingRepository, ShadowBindingCancelCommit, ShadowBindingCancelOutcome,
        ShadowBindingCommit, ShadowBindingCommitOutcome, ShadowBindingRejectCommit,
        ShadowBindingRejectOutcome,
    },
};

const SYSTEM_ACTOR: &str = "feedback-coordinator";
const SYSTEM_ACTOR_ROLE: &str = "feedback_coordinator";

struct LockedComparison {
    event: StageEventModel,
    job: ResearchJobModel,
}

struct ShadowBindingRows {
    snapshot: NewDecisionPolicySnapshot,
    revision: NewPolicyRevision,
    approval: NewPolicyApproval,
    activation: NewPolicyActivation,
    binding: ShadowBindingModel,
}

/// Sole transaction owner for route-slot reservation and the corresponding
/// model-routing policy activation.
pub struct PgModelRouteShadowBindingRepository {
    db: DatabaseConnection,
}

impl PgModelRouteShadowBindingRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn conflict(detail: impl Into<String>) -> FeedbackError {
        FeedbackError::ShadowBindingConflict {
            detail: detail.into(),
        }
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

    async fn lock_cycle(
        transaction: &DatabaseTransaction,
        params: &ShadowBindingJobParams,
    ) -> QuantResult<CycleModel> {
        let cycle = CycleEntity::find_by_id(params.feedback_cycle_id)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_FEEDBACK_CYCLE, params.feedback_cycle_id)
            })?;
        let route_generation = i64::try_from(params.expected_route_generation)
            .map_err(|error| Self::conflict(format!("route generation overflow: {error}")))?;
        let cycle_identity_exact = cycle.idempotency_hash == params.cycle_idempotency_hash;
        let snapshot_id_exact = cycle.decision_policy_snapshot_id == params.expected_snapshot_id;
        let snapshot_hash_exact =
            cycle.decision_policy_snapshot_hash == params.expected_snapshot_hash;
        let policy_generation_exact =
            cycle.policy_bundle_generation == params.expected_policy_generation;
        if cycle.status != FeedbackCycleStatus::Running
            || cycle.decision.is_some()
            || cycle.cancel_requested_at.is_some()
            || !cycle_identity_exact
            || cycle.profile_ref != params.profile_ref
            || cycle.route != params.route
            || cycle.champion_model_version_id != params.champion_model_version_id
            || cycle.champion_serving_contract_hash != params.champion_serving_contract_hash
            || !snapshot_id_exact
            || !snapshot_hash_exact
            || !policy_generation_exact
            || cycle.route_generation != route_generation
        {
            return Err(Self::conflict(
                "running cycle or its frozen route/policy lineage changed before shadow binding",
            )
            .into());
        }
        Ok(cycle)
    }

    async fn lock_cancellation_cycle(
        transaction: &DatabaseTransaction,
        command: &CancelShadowBinding,
    ) -> QuantResult<CycleModel> {
        let cycle = CycleEntity::find_by_id(command.feedback_cycle_id)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_FEEDBACK_CYCLE, command.feedback_cycle_id)
            })?;
        if cycle.status != FeedbackCycleStatus::Running
            || cycle.decision.is_some()
            || cycle.cancel_requested_at.is_none()
            || command.binding_id != ShadowBindingArtifactId::from_cycle_id(cycle.feedback_cycle_id)
        {
            return Err(Self::conflict(
                "shadow cancellation requires the exact running cycle cancellation request",
            )
            .into());
        }
        let requests = StageEventEntity::find()
            .filter(StageEventColumn::FeedbackCycleId.eq(command.feedback_cycle_id))
            .filter(StageEventColumn::EventKind.eq(FeedbackStageEventKind::CancellationRequested))
            .order_by_asc(StageEventColumn::EventSequence)
            .lock_shared()
            .all(transaction)
            .await
            .map_err(StorageError::from)?;
        let [request] = requests.as_slice() else {
            return Err(Self::conflict(
                "shadow cancellation cycle must have one exact cancellation event",
            )
            .into());
        };
        if request.reason_code.as_deref() != Some(command.reason_code.as_str())
            || cycle.cancel_requested_at != Some(request.occurred_at)
        {
            return Err(Self::conflict(
                "shadow cancellation command differs from its governed cycle event",
            )
            .into());
        }
        Ok(cycle)
    }

    async fn lock_comparison(
        transaction: &DatabaseTransaction,
        reference: &FeedbackComparisonArtifactRef,
    ) -> QuantResult<LockedComparison> {
        reference.validate_for(reference.feedback_cycle_id)?;
        let event = StageEventEntity::find()
            .filter(StageEventColumn::FeedbackCycleId.eq(reference.feedback_cycle_id))
            .filter(StageEventColumn::Stage.eq(FeedbackStage::Comparison))
            .filter(StageEventColumn::EventKind.eq(FeedbackStageEventKind::Succeeded))
            .order_by_desc(StageEventColumn::EventSequence)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                Self::conflict("feedback cycle has no succeeded Comparison stage event")
            })?;
        if event.research_job_id != Some(reference.job_id)
            || event.evidence_uri.as_ref() != Some(&reference.artifact.uri)
            || event.evidence_hash != Some(reference.artifact.content_hash)
        {
            return Err(Self::conflict(
                "Comparison stage event differs from the frozen artifact reference",
            )
            .into());
        }
        let job = ResearchJobEntity::find_by_id(reference.job_id)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RESEARCH_JOB, reference.job_id))?;
        let ResearchJobParams::FeedbackComparison(comparison) = &job.params_json else {
            return Err(Self::conflict("Comparison job lost its typed parameters").into());
        };
        comparison.validate()?;
        if job.feedback_cycle_id != Some(reference.feedback_cycle_id)
            || job.feedback_stage != Some(FeedbackStage::Comparison)
            || job.kind != ResearchJobKind::FeedbackComparison
            || job.status != ResearchJobStatus::Succeeded
            || job.result_kind != Some(ResearchJobResultKind::FeedbackComparisonArtifact)
            || job.result_ref != Some(reference.artifact_id.as_uuid())
            || job.result_artifact_uri.as_ref() != Some(&reference.artifact.uri)
            || job.result_artifact_hash != Some(reference.artifact.content_hash)
            || comparison.artifact_id != reference.artifact_id
            || comparison.input_hash()? != reference.input_hash
            || comparison.candidate_family_hash != reference.candidate_family_hash
            || comparison.decision_policy_snapshot_id != reference.decision_policy_snapshot_id
        {
            return Err(Self::conflict(
                "Comparison job identity, input, or terminal result changed",
            )
            .into());
        }
        Ok(LockedComparison { event, job })
    }

    fn verify_candidate_selection(
        locked: &LockedComparison,
        params: &ShadowBindingJobParams,
    ) -> QuantResult<()> {
        let ResearchJobParams::FeedbackComparison(comparison) = &locked.job.params_json else {
            return Err(Self::conflict("Comparison job lost its typed parameters").into());
        };
        let selected = comparison
            .candidates
            .iter()
            .find(|candidate| candidate.candidate_recipe_hash == params.candidate_recipe_hash)
            .ok_or_else(|| Self::conflict("selected recipe is absent from Comparison"))?;
        if selected.model_version_id != params.candidate_model_version_id
            || selected.serving_contract_hash != params.candidate_serving_contract_hash
            || locked.event.evidence_hash != Some(params.comparison.artifact.content_hash)
        {
            return Err(Self::conflict(
                "selected Comparison candidate or terminal evidence changed",
            )
            .into());
        }
        Ok(())
    }

    async fn lock_manifest(
        transaction: &DatabaseTransaction,
        params: &ShadowBindingJobParams,
    ) -> QuantResult<ModelCandidateManifestInfo> {
        let row = CandidateManifestEntity::find_by_id(params.candidate_manifest_id)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    QUANT_MODEL_CANDIDATE_MANIFEST,
                    params.candidate_manifest_id,
                )
            })?;
        Self::verify_manifest(row, params)
    }

    fn verify_manifest(
        row: CandidateManifestModel,
        params: &ShadowBindingJobParams,
    ) -> QuantResult<ModelCandidateManifestInfo> {
        let manifest = ModelCandidateManifestInfo::from(row);
        manifest
            .validate()
            .map_err(|error| Self::conflict(error.to_string()))?;
        let document = &manifest.document;
        let candidate_model_exact = manifest.model_version_id == params.candidate_model_version_id;
        if manifest.feedback_cycle_id != params.feedback_cycle_id
            || manifest.candidate_recipe_hash != params.candidate_recipe_hash
            || !candidate_model_exact
            || manifest.manifest_hash != params.candidate_manifest_hash
            || document.model_artifact_hash != params.candidate_artifact_hash
            || document.serving_contract_hash != params.candidate_serving_contract_hash
            || document.training_dataset_id != params.candidate_training_dataset_id
            || document.profile_ref != params.profile_ref
            || document.category
                != params.route.category().ok_or_else(|| {
                    Self::conflict(
                        "ResearchOnly pooled route cannot own a promotable shadow binding",
                    )
                })?
            || document.decision_policy_snapshot_hash != params.expected_snapshot_hash
        {
            return Err(Self::conflict(
                "candidate manifest differs from the frozen binding subject",
            )
            .into());
        }
        Ok(manifest)
    }

    async fn verify_models(
        transaction: &DatabaseTransaction,
        params: &ShadowBindingJobParams,
        manifest: &ModelCandidateManifestInfo,
    ) -> QuantResult<()> {
        let champion = PgModelRegistryRepository::require_version_info(
            transaction,
            &params.champion_model_version_id,
        )
        .await?;
        let candidate = PgModelRegistryRepository::require_version_info(
            transaction,
            &params.candidate_model_version_id,
        )
        .await?;
        if champion.serving_contract_hash != params.champion_serving_contract_hash
            || candidate.artifact_hash != params.candidate_artifact_hash
            || candidate.serving_contract_hash != params.candidate_serving_contract_hash
            || candidate.training_dataset_id != Some(params.candidate_training_dataset_id)
            || candidate.profile_ref != params.profile_ref
            || candidate.category_scope != params.route.category()
            || candidate.model_family != manifest.document.model_family
        {
            return Err(Self::conflict(
                "champion or candidate registry identity changed before shadow binding",
            )
            .into());
        }
        Ok(())
    }

    fn verify_current(
        guard: &ActivationGuardModel,
        bundle: &ActivePolicyBundle,
        params: &ShadowBindingJobParams,
    ) -> QuantResult<()> {
        let route = bundle
            .snapshot
            .model_routing
            .model
            .route_binding(params.route)
            .map_err(|error| Self::conflict(error.to_string()))?;
        let guard_exact = guard.generation == params.expected_policy_generation
            && guard.current_snapshot_id == Some(params.expected_snapshot_id)
            && guard.current_snapshot_hash == Some(params.expected_snapshot_hash);
        if !guard_exact
            || bundle.generation != params.expected_policy_generation
            || bundle.decision_policy_snapshot_id != params.expected_snapshot_id
            || bundle.snapshot_hash != params.expected_snapshot_hash
            || bundle.revision_vector.model_routing
                != Some(params.expected_model_routing_revision_id)
            || route.champion.model_version_id != params.champion_model_version_id
            || route.champion.generation != params.expected_route_generation
        {
            return Err(Self::conflict(
                "policy guard, snapshot, target champion, or route generation changed",
            )
            .into());
        }
        if route.shadow.is_some() {
            return Err(FeedbackError::ShadowOccupied {
                route: format!("{:?}", params.route).to_lowercase(),
                binding_id: "policy-route-shadow".to_owned(),
            }
            .into());
        }
        Ok(())
    }

    async fn reserve_budget(
        transaction: &DatabaseTransaction,
        bundle: &ActivePolicyBundle,
        params: &ShadowBindingJobParams,
    ) -> QuantResult<()> {
        let active = ShadowBindingEntity::find()
            .filter(ShadowBindingColumn::Status.eq(ShadowBindingStatus::Active))
            .lock_shared()
            .all(transaction)
            .await
            .map_err(StorageError::from)?;
        let mut active_bytes = 0_u64;
        for binding in active {
            if binding.route == params.route {
                return Err(FeedbackError::ShadowOccupied {
                    route: format!("{:?}", params.route).to_lowercase(),
                    binding_id: binding.binding_id.to_string(),
                }
                .into());
            }
            let reserved = u64::try_from(binding.reserved_model_bytes).map_err(|error| {
                Self::conflict(format!(
                    "active shadow {} has invalid memory reservation: {error}",
                    binding.binding_id
                ))
            })?;
            active_bytes = active_bytes.checked_add(reserved).ok_or_else(|| {
                Self::conflict("active shadow memory reservation total overflowed")
            })?;
            let route = bundle
                .snapshot
                .model_routing
                .model
                .route_binding(binding.route)
                .map_err(|error| Self::conflict(error.to_string()))?;
            let Some(shadow) = &route.shadow else {
                return Err(Self::conflict(format!(
                    "active binding {} has no matching policy shadow",
                    binding.binding_id
                ))
                .into());
            };
            if shadow.model_version_id != binding.candidate_model_version_id
                || shadow.generation
                    != u64::try_from(binding.binding_generation).map_err(|error| {
                        Self::conflict(format!(
                            "active shadow {} has invalid binding generation: {error}",
                            binding.binding_id
                        ))
                    })?
            {
                return Err(Self::conflict(format!(
                    "active binding {} differs from its policy route",
                    binding.binding_id
                ))
                .into());
            }
        }
        let requested_total = active_bytes
            .checked_add(params.reserved_model_bytes)
            .ok_or_else(|| Self::conflict("shadow memory reservation total overflowed"))?;
        if requested_total > params.total_shadow_model_budget_bytes {
            return Err(FeedbackError::ShadowMemoryBudgetExceeded {
                active_bytes,
                requested_bytes: params.reserved_model_bytes,
                limit_bytes: params.total_shadow_model_budget_bytes,
            }
            .into());
        }
        Ok(())
    }

    fn prospective_snapshot(
        current: &ActivePolicyBundle,
        params: &ShadowBindingJobParams,
        database_now: DateTime<Utc>,
        revision_id: PolicyRevisionId,
    ) -> QuantResult<(DecisionPolicySnapshot, PolicyBundleGeneration, u64)> {
        let next_policy_generation = current
            .generation
            .checked_next()
            .map_err(|error| Self::conflict(error.to_string()))?;
        let binding_generation = params
            .expected_route_generation
            .checked_add(1)
            .ok_or_else(|| Self::conflict("shadow binding generation overflowed"))?;
        let mut snapshot = current.snapshot.clone();
        let route = snapshot
            .model_routing
            .model
            .buy_routes
            .get_mut(&params.route)
            .ok_or_else(|| Self::conflict("target route disappeared from model routing"))?;
        route.shadow = Some(ModelBinding::new(
            params.candidate_model_version_id,
            ModelBindingSource::Feedback {
                feedback_cycle_id: params.feedback_cycle_id,
            },
            database_now,
            next_policy_generation,
            binding_generation,
        ));
        snapshot.set_resource_revision_id(ConfigResourceKind::ModelRouting, revision_id);
        let validation = snapshot.validate_runtime_config();
        if validation.has_errors() {
            return Err(Self::conflict(format!(
                "prospective shadow policy snapshot is invalid: {validation}"
            ))
            .into());
        }
        Ok((snapshot, next_policy_generation, binding_generation))
    }

    fn snapshot_row(
        snapshot: &DecisionPolicySnapshot,
        snapshot_id: DecisionPolicySnapshotId,
        snapshot_hash: ContentHash,
        reason: &str,
    ) -> QuantResult<NewDecisionPolicySnapshot> {
        let document = snapshot
            .persistence_document()
            .map_err(|error| Self::conflict(error.to_string()))?;
        let required = |revision: Option<PolicyRevisionId>,
                        kind: ConfigResourceKind|
         -> QuantResult<PolicyRevisionId> {
            revision.ok_or_else(|| {
                Self::conflict(format!("candidate snapshot has no {kind} revision")).into()
            })
        };
        Ok(NewDecisionPolicySnapshot {
            decision_policy_snapshot_id: snapshot_id,
            snapshot_hash,
            recommendation_policy_revision_id: required(
                document.revisions.recommendation_policy,
                ConfigResourceKind::RecommendationPolicy,
            )?,
            execution_risk_policy_revision_id: required(
                document.revisions.execution_risk_policy,
                ConfigResourceKind::ExecutionRiskPolicy,
            )?,
            model_routing_revision_id: required(
                document.revisions.model_routing,
                ConfigResourceKind::ModelRouting,
            )?,
            report_schedule_revision_id: required(
                document.revisions.report_schedule,
                ConfigResourceKind::ReportSchedule,
            )?,
            operations_policy_revision_id: required(
                document.revisions.operations_policy,
                ConfigResourceKind::OperationsPolicy,
            )?,
            execution_automation_policy_revision_id: required(
                document.revisions.execution_automation_policy,
                ConfigResourceKind::ExecutionAutomationPolicy,
            )?,
            snapshot: document,
            source: DecisionPolicySnapshotSource::Activation,
            created_by_kind: PolicyActorKind::System,
            created_by_user_id: None,
            created_by_label: SYSTEM_ACTOR.to_owned(),
            reason: reason.to_owned(),
        })
    }

    fn build_rows(
        current: &ActivePolicyBundle,
        params: &ShadowBindingJobParams,
        database_now: DateTime<Utc>,
    ) -> QuantResult<ShadowBindingRows> {
        let input_hash = params.input_hash()?;
        let previous_revision = params.expected_model_routing_revision_id;
        let revision_id = PolicyRevisionId::from_v7();
        let approval_id = PolicyApprovalId::from_v7();
        let activation_id = PolicyActivationId::from_v7();
        let audit_event_id = AuditEventId::from_content_hash(&input_hash);
        let (snapshot, next_policy_generation, binding_generation) =
            Self::prospective_snapshot(current, params, database_now, revision_id)?;
        let snapshot_hash = snapshot
            .persistence_hash()
            .map_err(|error| Self::conflict(error.to_string()))?;
        let snapshot_id = DecisionPolicySnapshotId::from_content_hash(&snapshot_hash);
        let model_document = PolicyDocument::ModelRouting(snapshot.model_routing.clone());
        let revision_hash =
            CanonicalDigest::content_hash_json(&model_document).map_err(FeedbackError::from)?;
        let validation_subject = PolicyValidationSubject {
            base_generation: current.generation,
            base_revision_vector: current.revision_vector.clone(),
            candidate_bundle_hash: snapshot_hash,
        };
        let reason = format!(
            "bind feedback cycle {} candidate {} to {:?} shadow",
            params.feedback_cycle_id, params.candidate_model_version_id, params.route
        );
        let receipt = ShadowBindingReceipt::try_seal(ShadowBindingReceiptInput {
            params: params.clone(),
            bound_at: database_now,
            binding_generation,
            committed_policy_generation: next_policy_generation,
            committed_snapshot_id: snapshot_id,
            committed_snapshot_hash: snapshot_hash,
            committed_model_routing_revision_id: revision_id,
            policy_activation_id: activation_id,
            audit_event_id,
        })?;
        let policy_idempotency_key = format!("shadow-bind:{}", params.artifact_id)
            .parse::<PolicyIdempotencyKey>()
            .map_err(|error| Self::conflict(error.to_string()))?;
        let snapshot_row = Self::snapshot_row(&snapshot, snapshot_id, snapshot_hash, &reason)?;
        let revision = NewPolicyRevision {
            policy_revision_id: revision_id,
            resource_kind: ConfigResourceKind::ModelRouting,
            schema_version: snapshot.model_routing.schema_version,
            revision_hash,
            document: model_document,
            status: PolicyRevisionStatus::Validated,
            validation_evidence: Some(PolicyValidationEvidence {
                subject: Some(validation_subject.clone()),
                issues: Vec::new(),
                preflight: Self::passed_preflight(),
            }),
            validated_at: Some(database_now),
            preflight_token_hash: Some(input_hash),
            preflight_expires_at: None,
            created_by_kind: PolicyActorKind::System,
            created_by_user_id: None,
            created_by_label: SYSTEM_ACTOR.to_owned(),
            reason: reason.clone(),
        };
        let approval = NewPolicyApproval {
            policy_approval_id: approval_id,
            policy_revision_id: revision_id,
            resource_kind: ConfigResourceKind::ModelRouting,
            revision_hash,
            validation_subject: Some(validation_subject),
            decision: PolicyApprovalDecision::Approved,
            decided_by_kind: PolicyActorKind::System,
            decided_by_user_id: None,
            decided_by_label: SYSTEM_ACTOR.to_owned(),
            reason: reason.clone(),
            decided_at: database_now,
            expires_at: None,
        };
        let activation = NewPolicyActivation {
            bundle_generation: next_policy_generation,
            expected_bundle_generation: current.generation,
            policy_activation_id: activation_id,
            resource_kind: ConfigResourceKind::ModelRouting,
            policy_revision_id: revision_id,
            decision_policy_snapshot_id: snapshot_id,
            policy_approval_id: approval_id,
            activated_by_kind: PolicyActorKind::System,
            activated_by_user_id: None,
            activated_by_label: SYSTEM_ACTOR.to_owned(),
            reason,
            activation_kind: PolicyActivationKind::ModelShadowBinding,
            expected_active_revision_id: Some(previous_revision),
            previous_policy_revision_id: Some(previous_revision),
            rollback_target_revision_id: None,
            preflight_token_hash: input_hash,
            idempotency_key: policy_idempotency_key,
            activation_request_hash: input_hash,
            audit_event_id,
        };
        let binding_generation_i64 = i64::try_from(binding_generation)
            .map_err(|error| Self::conflict(format!("binding generation overflow: {error}")))?;
        let reserved_model_bytes = i64::try_from(params.reserved_model_bytes)
            .map_err(|error| Self::conflict(format!("model memory budget overflow: {error}")))?;
        let binding = ShadowBindingModel {
            binding_id: params.artifact_id,
            feedback_cycle_id: params.feedback_cycle_id,
            route: params.route,
            status: ShadowBindingStatus::Active,
            lifecycle_generation: 0,
            binding_generation: binding_generation_i64,
            champion_model_version_id: params.champion_model_version_id,
            champion_serving_contract_hash: params.champion_serving_contract_hash,
            candidate_model_version_id: params.candidate_model_version_id,
            candidate_serving_contract_hash: params.candidate_serving_contract_hash,
            candidate_recipe_hash: params.candidate_recipe_hash,
            candidate_manifest_id: params.candidate_manifest_id,
            candidate_manifest_hash: params.candidate_manifest_hash,
            reserved_model_bytes,
            committed_policy_generation: next_policy_generation,
            policy_activation_id: activation_id,
            audit_event_id,
            receipt_hash: receipt.receipt_hash,
            receipt,
            bound_at: database_now,
            terminated_at: None,
            termination_policy_activation_id: None,
            termination_request_hash: None,
            termination_reason_code: None,
            termination_note: None,
            termination_actor_role: None,
            created_at: database_now,
            updated_at: database_now,
        };
        Ok(ShadowBindingRows {
            snapshot: snapshot_row,
            revision,
            approval,
            activation,
            binding,
        })
    }

    async fn advance_guard(
        transaction: &DatabaseTransaction,
        guard: &ActivationGuardModel,
        snapshot: &NewDecisionPolicySnapshot,
        database_now: DateTime<Utc>,
    ) -> QuantResult<()> {
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
            return Err(Self::conflict("policy activation guard CAS affected no row").into());
        }
        Ok(())
    }

    async fn insert_rows(
        transaction: &DatabaseTransaction,
        guard: &ActivationGuardModel,
        rows: ShadowBindingRows,
        database_now: DateTime<Utc>,
    ) -> QuantResult<()> {
        RevisionEntity::insert(rows.revision.into_active_model())
            .exec(transaction)
            .await
            .map_err(StorageError::from)?;
        ApprovalEntity::insert(rows.approval.into_active_model())
            .exec(transaction)
            .await
            .map_err(StorageError::from)?;
        PgPolicyRepository::insert_snapshot_if_absent(transaction, rows.snapshot.clone()).await?;
        let activation = ActivationEntity::insert(rows.activation.into_active_model())
            .exec_with_returning(transaction)
            .await
            .map_err(StorageError::from)?;
        PgPolicyRepository::insert_activation_ledger(transaction, &activation, &rows.snapshot)
            .await?;
        Self::advance_guard(transaction, guard, &rows.snapshot, database_now).await?;
        ShadowBindingEntity::insert(rows.binding.into_active_model())
            .exec(transaction)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }

    async fn load_commit(
        db: &impl ConnectionTrait,
        binding_id: ShadowBindingArtifactId,
    ) -> QuantResult<Option<ShadowBindingCommit>> {
        let Some(binding) = ShadowBindingEntity::find_by_id(binding_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
        else {
            return Ok(None);
        };
        if binding.status != ShadowBindingStatus::Active {
            return Err(FeedbackError::ShadowBindingConflict {
                detail: format!(
                    "shadow binding {} is terminal with status {:?}; it cannot be replayed as active",
                    binding.binding_id, binding.status
                ),
            }
            .into());
        }
        binding.receipt.validate()?;
        if binding.binding_id != binding.receipt.binding_id
            || binding.feedback_cycle_id != binding.receipt.feedback_cycle_id
            || binding.route != binding.receipt.route
            || binding.binding_generation
                != i64::try_from(binding.receipt.binding_generation)
                    .map_err(|error| Self::conflict(error.to_string()))?
            || binding.champion_model_version_id != binding.receipt.champion_model_version_id
            || binding.candidate_model_version_id != binding.receipt.candidate_model_version_id
            || binding.reserved_model_bytes
                != i64::try_from(binding.receipt.reserved_model_bytes)
                    .map_err(|error| Self::conflict(error.to_string()))?
            || binding.committed_policy_generation != binding.receipt.committed_policy_generation
            || binding.policy_activation_id != binding.receipt.policy_activation_id
            || binding.audit_event_id != binding.receipt.audit_event_id
            || binding.receipt_hash != binding.receipt.receipt_hash
            || binding.bound_at != binding.receipt.bound_at
        {
            return Err(Self::conflict(
                "shadow-binding ledger row differs from its sealed receipt",
            )
            .into());
        }
        let activation = ActivationEntity::find_by_id(binding.policy_activation_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(POLICY_ACTIVATION, binding.policy_activation_id)
            })?;
        Self::verify_activation(db, &activation, &binding.receipt).await?;
        let snapshot = SnapshotEntity::find_by_id(activation.decision_policy_snapshot_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    "decision_policy_snapshot",
                    activation.decision_policy_snapshot_id,
                )
            })?;
        let snapshot = PgPolicyRepository::resolve_snapshot_model(db, snapshot).await?;
        let bundle = ActivePolicyBundle::from_parts(
            activation.bundle_generation,
            snapshot.decision_policy_snapshot_id,
            snapshot.snapshot_hash,
            snapshot.snapshot,
        );
        Self::verify_bundle(&bundle, &binding.receipt)?;
        Ok(Some(ShadowBindingCommit {
            receipt: binding.receipt,
            activation: PolicyActivationInfo::from(activation),
            bundle,
            outcome: ShadowBindingCommitOutcome::ExactReplay,
        }))
    }

    async fn verify_activation(
        db: &impl ConnectionTrait,
        activation: &ActivationModel,
        receipt: &ShadowBindingReceipt,
    ) -> QuantResult<()> {
        let activation_exact = activation.activation_kind
            == PolicyActivationKind::ModelShadowBinding
            && activation.resource_kind == ConfigResourceKind::ModelRouting
            && activation.bundle_generation == receipt.committed_policy_generation
            && activation.expected_bundle_generation == receipt.previous_policy_generation
            && activation.policy_revision_id == receipt.committed_model_routing_revision_id
            && activation.decision_policy_snapshot_id == receipt.committed_snapshot_id
            && activation.expected_active_revision_id
                == Some(receipt.previous_model_routing_revision_id)
            && activation.previous_policy_revision_id
                == Some(receipt.previous_model_routing_revision_id)
            && activation.rollback_target_revision_id.is_none()
            && activation.activation_request_hash == receipt.job_input_hash
            && activation.preflight_token_hash == receipt.job_input_hash
            && activation.audit_event_id == receipt.audit_event_id
            && activation.promotion_permit_id.is_none()
            && activation.promotion_transaction_hash.is_none()
            && activation.model_governance_audit_id.is_none();
        if !activation_exact {
            return Err(Self::conflict(
                "shadow-binding policy activation differs from its receipt",
            )
            .into());
        }
        let audit = ActivationAuditEntity::find_by_id(receipt.audit_event_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("policy_activation_audit", receipt.audit_event_id)
            })?;
        let outbox = ActivationOutboxEntity::find_by_id(receipt.audit_event_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("policy_activation_event_outbox", receipt.audit_event_id)
            })?;
        let audit_generation_exact = audit.bundle_generation == receipt.committed_policy_generation;
        let audit_snapshot_id_exact =
            audit.decision_policy_snapshot_id == receipt.committed_snapshot_id;
        let audit_snapshot_hash_exact = audit.snapshot_hash == receipt.committed_snapshot_hash;
        let audit_request_exact = audit.activation_request_hash == receipt.job_input_hash;
        let audit_exact = audit.policy_activation_id == receipt.policy_activation_id
            && audit_generation_exact
            && audit_snapshot_id_exact
            && audit_snapshot_hash_exact
            && audit_request_exact;
        let outbox_generation_exact =
            outbox.bundle_generation == receipt.committed_policy_generation;
        let outbox_snapshot_id_exact =
            outbox.decision_policy_snapshot_id == receipt.committed_snapshot_id;
        let outbox_snapshot_hash_exact = outbox.snapshot_hash == receipt.committed_snapshot_hash;
        let outbox_exact = outbox.policy_activation_id == receipt.policy_activation_id
            && outbox_generation_exact
            && outbox_snapshot_id_exact
            && outbox_snapshot_hash_exact;
        if !audit_exact || !outbox_exact {
            return Err(Self::conflict(
                "shadow-binding activation audit or outbox differs from its receipt",
            )
            .into());
        }
        Ok(())
    }

    fn verify_bundle(
        bundle: &ActivePolicyBundle,
        receipt: &ShadowBindingReceipt,
    ) -> QuantResult<()> {
        let route = bundle
            .snapshot
            .model_routing
            .model
            .route_binding(receipt.route)
            .map_err(|error| Self::conflict(error.to_string()))?;
        let Some(shadow) = &route.shadow else {
            return Err(Self::conflict(
                "committed shadow-binding snapshot has an empty target route slot",
            )
            .into());
        };
        if bundle.generation != receipt.committed_policy_generation
            || bundle.decision_policy_snapshot_id != receipt.committed_snapshot_id
            || bundle.snapshot_hash != receipt.committed_snapshot_hash
            || bundle.revision_vector.model_routing
                != Some(receipt.committed_model_routing_revision_id)
            || route.champion.model_version_id != receipt.champion_model_version_id
            || shadow.model_version_id != receipt.candidate_model_version_id
            || shadow.source
                != (ModelBindingSource::Feedback {
                    feedback_cycle_id: receipt.feedback_cycle_id,
                })
            || shadow.bound_at != receipt.bound_at
            || shadow.config_revision != receipt.committed_policy_generation
            || shadow.generation != receipt.binding_generation
        {
            return Err(Self::conflict(
                "committed policy bundle differs from the route-owned binding receipt",
            )
            .into());
        }
        Ok(())
    }

    fn rejection_reason(command: &RejectShadowBinding) -> String {
        format!("{}: {}", command.reason_code, command.note)
    }

    async fn load_rejection(
        db: &impl ConnectionTrait,
        idempotency_key: &PolicyIdempotencyKey,
    ) -> QuantResult<Option<ShadowBindingRejectCommit>> {
        let Some(activation) = ActivationEntity::find()
            .filter(ActivationColumn::ActivationKind.eq(PolicyActivationKind::ModelShadowRejection))
            .filter(ActivationColumn::IdempotencyKey.eq(idempotency_key.as_str()))
            .one(db)
            .await
            .map_err(StorageError::from)?
        else {
            return Ok(None);
        };
        let binding = ShadowBindingEntity::find()
            .filter(
                ShadowBindingColumn::TerminationPolicyActivationId
                    .eq(activation.policy_activation_id),
            )
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                Self::conflict(
                    "shadow-rejection activation has no exact terminal binding projection",
                )
            })?;
        let snapshot = SnapshotEntity::find_by_id(activation.decision_policy_snapshot_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    "decision_policy_snapshot",
                    activation.decision_policy_snapshot_id,
                )
            })?;
        let snapshot = PgPolicyRepository::resolve_snapshot_model(db, snapshot).await?;
        let bundle = ActivePolicyBundle::from_parts(
            activation.bundle_generation,
            snapshot.decision_policy_snapshot_id,
            snapshot.snapshot_hash,
            snapshot.snapshot,
        );
        let previous_binding_generation =
            u64::try_from(binding.binding_generation).map_err(|error| {
                Self::conflict(format!(
                    "stored shadow binding generation is invalid: {error}"
                ))
            })?;
        let cleared_route_generation = previous_binding_generation
            .checked_add(1)
            .ok_or_else(|| Self::conflict("stored shadow rejection route generation overflowed"))?;
        let route = bundle
            .snapshot
            .model_routing
            .model
            .route_binding(binding.route)
            .map_err(|error| Self::conflict(error.to_string()))?;
        let request_hash = binding.termination_request_hash.ok_or_else(|| {
            Self::conflict("rejected shadow binding has no termination request hash")
        })?;
        let reason_code = binding.termination_reason_code.clone().ok_or_else(|| {
            Self::conflict("rejected shadow binding has no termination reason code")
        })?;
        let note = binding
            .termination_note
            .clone()
            .ok_or_else(|| Self::conflict("rejected shadow binding has no termination note"))?;
        let rejected_by_role = binding.termination_actor_role.clone().ok_or_else(|| {
            Self::conflict("rejected shadow binding has no termination actor role")
        })?;
        let rejected_by_user_id = activation.activated_by_user_id.ok_or_else(|| {
            Self::conflict("shadow-rejection activation has no authenticated user")
        })?;
        let previous_model_routing_revision_id =
            activation.previous_policy_revision_id.ok_or_else(|| {
                Self::conflict("shadow-rejection activation has no previous routing revision")
            })?;
        let terminated_at = binding.terminated_at.ok_or_else(|| {
            Self::conflict("rejected shadow binding has no database termination timestamp")
        })?;
        if binding.status != ShadowBindingStatus::Rejected
            || binding.termination_policy_activation_id != Some(activation.policy_activation_id)
            || activation.activation_kind != PolicyActivationKind::ModelShadowRejection
            || activation.resource_kind != ConfigResourceKind::ModelRouting
            || activation.bundle_generation != bundle.generation
            || activation.expected_active_revision_id != Some(previous_model_routing_revision_id)
            || activation.rollback_target_revision_id.is_some()
            || activation.activation_request_hash != request_hash
            || activation.preflight_token_hash != request_hash
            || activation.audit_event_id != AuditEventId::from_content_hash(&request_hash)
            || activation.reason != format!("{reason_code}: {note}")
            || activation.activated_at != terminated_at
            || bundle.revision_vector.model_routing != Some(activation.policy_revision_id)
            || route.champion.model_version_id != binding.champion_model_version_id
            || route.champion.generation != cleared_route_generation
            || route.champion.config_revision != bundle.generation
            || route.shadow.is_some()
        {
            return Err(Self::conflict(
                "shadow-rejection activation, terminal binding, or cleared route diverged",
            )
            .into());
        }
        Ok(Some(ShadowBindingRejectCommit {
            receipt: ShadowBindingRejectionReceipt {
                binding_id: binding.binding_id,
                feedback_cycle_id: binding.feedback_cycle_id,
                route: binding.route,
                champion_model_version_id: binding.champion_model_version_id,
                rejected_model_version_id: binding.candidate_model_version_id,
                previous_binding_generation,
                cleared_route_generation,
                previous_policy_generation: activation.expected_bundle_generation,
                committed_policy_generation: activation.bundle_generation,
                previous_model_routing_revision_id,
                committed_model_routing_revision_id: activation.policy_revision_id,
                committed_snapshot_id: bundle.decision_policy_snapshot_id,
                committed_snapshot_hash: bundle.snapshot_hash,
                policy_activation_id: activation.policy_activation_id,
                audit_event_id: activation.audit_event_id,
                request_hash,
                idempotency_key: activation.idempotency_key.clone(),
                reason_code,
                note,
                rejected_by_user_id,
                rejected_by_username: activation.activated_by_label.clone(),
                rejected_by_role,
                rejected_at: activation.activated_at,
            },
            activation: PolicyActivationInfo::from(activation),
            bundle,
            outcome: ShadowBindingRejectOutcome::ExactReplay,
        }))
    }

    fn verify_rejection(
        command: &RejectShadowBinding,
        commit: &ShadowBindingRejectCommit,
    ) -> QuantResult<()> {
        if commit.receipt.binding_id != command.binding_id
            || commit.receipt.previous_binding_generation != command.expected_binding_generation
            || commit.receipt.previous_policy_generation != command.expected_policy_generation
            || commit.receipt.idempotency_key != command.idempotency_key
            || commit.receipt.reason_code != command.reason_code
            || commit.receipt.note != command.note
            || commit.receipt.rejected_by_user_id != command.actor_user_id
            || commit.receipt.rejected_by_role != command.actor_role
            || commit.receipt.request_hash != command.request_hash()?
        {
            return Err(Self::conflict(
                "shadow-rejection idempotency key was replayed with intent drift",
            )
            .into());
        }
        Ok(())
    }

    fn cancellation_reason(command: &CancelShadowBinding) -> String {
        format!("{}: {}", command.reason_code, command.note)
    }

    async fn load_cancellation(
        db: &impl ConnectionTrait,
        idempotency_key: &PolicyIdempotencyKey,
    ) -> QuantResult<Option<ShadowBindingCancelCommit>> {
        let Some(activation) = ActivationEntity::find()
            .filter(
                ActivationColumn::ActivationKind.eq(PolicyActivationKind::ModelShadowCancellation),
            )
            .filter(ActivationColumn::IdempotencyKey.eq(idempotency_key.as_str()))
            .one(db)
            .await
            .map_err(StorageError::from)?
        else {
            return Ok(None);
        };
        let binding = ShadowBindingEntity::find()
            .filter(
                ShadowBindingColumn::TerminationPolicyActivationId
                    .eq(activation.policy_activation_id),
            )
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                Self::conflict(
                    "shadow-cancellation activation has no exact terminal binding projection",
                )
            })?;
        let snapshot = SnapshotEntity::find_by_id(activation.decision_policy_snapshot_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    "decision_policy_snapshot",
                    activation.decision_policy_snapshot_id,
                )
            })?;
        let snapshot = PgPolicyRepository::resolve_snapshot_model(db, snapshot).await?;
        let bundle = ActivePolicyBundle::from_parts(
            activation.bundle_generation,
            snapshot.decision_policy_snapshot_id,
            snapshot.snapshot_hash,
            snapshot.snapshot,
        );
        let committed_lifecycle_generation =
            u64::try_from(binding.lifecycle_generation).map_err(|error| {
                Self::conflict(format!(
                    "stored shadow lifecycle generation is invalid: {error}"
                ))
            })?;
        let previous_lifecycle_generation = committed_lifecycle_generation
            .checked_sub(1)
            .ok_or_else(|| Self::conflict("stored shadow cancellation lifecycle is invalid"))?;
        let previous_binding_generation =
            u64::try_from(binding.binding_generation).map_err(|error| {
                Self::conflict(format!(
                    "stored shadow binding generation is invalid: {error}"
                ))
            })?;
        let cleared_route_generation =
            previous_binding_generation.checked_add(1).ok_or_else(|| {
                Self::conflict("stored shadow cancellation route generation overflowed")
            })?;
        let route = bundle
            .snapshot
            .model_routing
            .model
            .route_binding(binding.route)
            .map_err(|error| Self::conflict(error.to_string()))?;
        let request_hash = binding.termination_request_hash.ok_or_else(|| {
            Self::conflict("cancelled shadow binding has no termination request hash")
        })?;
        let reason_code = binding.termination_reason_code.clone().ok_or_else(|| {
            Self::conflict("cancelled shadow binding has no termination reason code")
        })?;
        let note = binding
            .termination_note
            .clone()
            .ok_or_else(|| Self::conflict("cancelled shadow binding has no termination note"))?;
        let cancelled_by_role = binding.termination_actor_role.clone().ok_or_else(|| {
            Self::conflict("cancelled shadow binding has no termination actor role")
        })?;
        let previous_model_routing_revision_id =
            activation.previous_policy_revision_id.ok_or_else(|| {
                Self::conflict("shadow-cancellation activation has no previous routing revision")
            })?;
        let terminated_at = binding.terminated_at.ok_or_else(|| {
            Self::conflict("cancelled shadow binding has no database termination timestamp")
        })?;
        if binding.status != ShadowBindingStatus::Cancelled
            || binding.termination_policy_activation_id != Some(activation.policy_activation_id)
            || activation.activation_kind != PolicyActivationKind::ModelShadowCancellation
            || activation.resource_kind != ConfigResourceKind::ModelRouting
            || activation.activated_by_kind != PolicyActorKind::System
            || activation.activated_by_user_id.is_some()
            || activation.activated_by_label != SYSTEM_ACTOR
            || cancelled_by_role.as_str() != SYSTEM_ACTOR_ROLE
            || activation.bundle_generation != bundle.generation
            || activation.expected_active_revision_id != Some(previous_model_routing_revision_id)
            || activation.rollback_target_revision_id.is_some()
            || activation.activation_request_hash != request_hash
            || activation.preflight_token_hash != request_hash
            || activation.audit_event_id != AuditEventId::from_content_hash(&request_hash)
            || activation.reason != format!("{reason_code}: {note}")
            || activation.activated_at != terminated_at
            || bundle.revision_vector.model_routing != Some(activation.policy_revision_id)
            || route.champion.model_version_id != binding.champion_model_version_id
            || route.champion.generation != cleared_route_generation
            || route.champion.config_revision != bundle.generation
            || route.shadow.is_some()
        {
            return Err(Self::conflict(
                "shadow-cancellation activation, terminal binding, or cleared route diverged",
            )
            .into());
        }
        Ok(Some(ShadowBindingCancelCommit {
            receipt: ShadowBindingCancellationReceipt {
                binding_id: binding.binding_id,
                feedback_cycle_id: binding.feedback_cycle_id,
                route: binding.route,
                champion_model_version_id: binding.champion_model_version_id,
                cancelled_model_version_id: binding.candidate_model_version_id,
                previous_lifecycle_generation,
                committed_lifecycle_generation,
                previous_binding_generation,
                cleared_route_generation,
                previous_policy_generation: activation.expected_bundle_generation,
                committed_policy_generation: activation.bundle_generation,
                previous_model_routing_revision_id,
                committed_model_routing_revision_id: activation.policy_revision_id,
                committed_snapshot_id: bundle.decision_policy_snapshot_id,
                committed_snapshot_hash: bundle.snapshot_hash,
                policy_activation_id: activation.policy_activation_id,
                audit_event_id: activation.audit_event_id,
                request_hash,
                idempotency_key: activation.idempotency_key.clone(),
                reason_code,
                note,
                cancelled_by_label: activation.activated_by_label.clone(),
                cancelled_by_role,
                cancelled_at: activation.activated_at,
            },
            activation: PolicyActivationInfo::from(activation),
            bundle,
            outcome: ShadowBindingCancelOutcome::ExactReplay,
        }))
    }

    fn verify_cancellation(
        command: &CancelShadowBinding,
        commit: &ShadowBindingCancelCommit,
    ) -> QuantResult<()> {
        if commit.receipt.binding_id != command.binding_id
            || commit.receipt.feedback_cycle_id != command.feedback_cycle_id
            || commit.receipt.previous_lifecycle_generation != command.expected_lifecycle_generation
            || commit.receipt.previous_binding_generation != command.expected_binding_generation
            || commit.receipt.previous_policy_generation != command.expected_policy_generation
            || commit.receipt.idempotency_key != command.idempotency_key
            || commit.receipt.reason_code != command.reason_code
            || commit.receipt.note != command.note
            || commit.receipt.cancelled_by_label != SYSTEM_ACTOR
            || commit.receipt.cancelled_by_role.as_str() != SYSTEM_ACTOR_ROLE
            || commit.receipt.request_hash != command.request_hash()?
        {
            return Err(Self::conflict(
                "shadow-cancellation idempotency key was replayed with intent drift",
            )
            .into());
        }
        Ok(())
    }

    fn clear_shadow(
        current: &ActivePolicyBundle,
        binding: &ShadowBindingModel,
        expected_binding_generation: u64,
        revision_id: PolicyRevisionId,
        action: &str,
    ) -> QuantResult<(DecisionPolicySnapshot, PolicyBundleGeneration)> {
        let next_policy_generation = current
            .generation
            .checked_next()
            .map_err(|error| Self::conflict(error.to_string()))?;
        let next_route_generation =
            expected_binding_generation.checked_add(1).ok_or_else(|| {
                Self::conflict(format!("shadow {action} route generation overflowed"))
            })?;
        let mut snapshot = current.snapshot.clone();
        let route = snapshot
            .model_routing
            .model
            .buy_routes
            .get_mut(&binding.route)
            .ok_or_else(|| Self::conflict(format!("shadow {action} target route disappeared")))?;
        let Some(shadow) = &route.shadow else {
            return Err(
                Self::conflict(format!("shadow {action} target route is already empty")).into(),
            );
        };
        if route.champion.model_version_id != binding.champion_model_version_id
            || shadow.model_version_id != binding.candidate_model_version_id
            || shadow.generation != expected_binding_generation
        {
            return Err(Self::conflict(format!(
                "shadow {action} target route differs from its active binding"
            ))
            .into());
        }
        route.shadow = None;
        route.champion.config_revision = next_policy_generation;
        route.champion.generation = next_route_generation;
        snapshot.set_resource_revision_id(ConfigResourceKind::ModelRouting, revision_id);
        let validation = snapshot.validate_runtime_config();
        if validation.has_errors() {
            return Err(Self::conflict(format!(
                "shadow-{action} policy snapshot is invalid: {validation}"
            ))
            .into());
        }
        Ok((snapshot, next_policy_generation))
    }

    async fn insert_cancellation(
        transaction: &DatabaseTransaction,
        guard: &ActivationGuardModel,
        binding: &ShadowBindingModel,
        command: &CancelShadowBinding,
        current: &ActivePolicyBundle,
        database_now: DateTime<Utc>,
    ) -> QuantResult<()> {
        let request_hash = command.request_hash()?;
        let revision_id = PolicyRevisionId::from_v7();
        let approval_id = PolicyApprovalId::from_v7();
        let activation_id = PolicyActivationId::from_v7();
        let audit_event_id = AuditEventId::from_content_hash(&request_hash);
        let reason = Self::cancellation_reason(command);
        let (snapshot, next_policy_generation) = Self::clear_shadow(
            current,
            binding,
            command.expected_binding_generation,
            revision_id,
            "cancellation",
        )?;
        let snapshot_hash = snapshot
            .persistence_hash()
            .map_err(|error| Self::conflict(error.to_string()))?;
        let snapshot_id = DecisionPolicySnapshotId::from_content_hash(&snapshot_hash);
        let document = PolicyDocument::ModelRouting(snapshot.model_routing.clone());
        let revision_hash =
            CanonicalDigest::content_hash_json(&document).map_err(FeedbackError::from)?;
        let validation_subject = PolicyValidationSubject {
            base_generation: current.generation,
            base_revision_vector: current.revision_vector.clone(),
            candidate_bundle_hash: snapshot_hash,
        };
        let snapshot_row = Self::snapshot_row(&snapshot, snapshot_id, snapshot_hash, &reason)?;
        let revision = NewPolicyRevision {
            policy_revision_id: revision_id,
            resource_kind: ConfigResourceKind::ModelRouting,
            schema_version: snapshot.model_routing.schema_version,
            revision_hash,
            document,
            status: PolicyRevisionStatus::Validated,
            validation_evidence: Some(PolicyValidationEvidence {
                subject: Some(validation_subject.clone()),
                issues: Vec::new(),
                preflight: Self::passed_preflight(),
            }),
            validated_at: Some(database_now),
            preflight_token_hash: Some(request_hash),
            preflight_expires_at: None,
            created_by_kind: PolicyActorKind::System,
            created_by_user_id: None,
            created_by_label: SYSTEM_ACTOR.to_owned(),
            reason: reason.clone(),
        };
        let approval = NewPolicyApproval {
            policy_approval_id: approval_id,
            policy_revision_id: revision_id,
            resource_kind: ConfigResourceKind::ModelRouting,
            revision_hash,
            validation_subject: Some(validation_subject),
            decision: PolicyApprovalDecision::Approved,
            decided_by_kind: PolicyActorKind::System,
            decided_by_user_id: None,
            decided_by_label: SYSTEM_ACTOR.to_owned(),
            reason: reason.clone(),
            decided_at: database_now,
            expires_at: None,
        };
        let activation = NewPolicyActivation {
            bundle_generation: next_policy_generation,
            expected_bundle_generation: current.generation,
            policy_activation_id: activation_id,
            resource_kind: ConfigResourceKind::ModelRouting,
            policy_revision_id: revision_id,
            decision_policy_snapshot_id: snapshot_id,
            policy_approval_id: approval_id,
            activated_by_kind: PolicyActorKind::System,
            activated_by_user_id: None,
            activated_by_label: SYSTEM_ACTOR.to_owned(),
            reason,
            activation_kind: PolicyActivationKind::ModelShadowCancellation,
            expected_active_revision_id: current.revision_vector.model_routing,
            previous_policy_revision_id: current.revision_vector.model_routing,
            rollback_target_revision_id: None,
            preflight_token_hash: request_hash,
            idempotency_key: command.idempotency_key.clone(),
            activation_request_hash: request_hash,
            audit_event_id,
        };
        RevisionEntity::insert(revision.into_active_model())
            .exec(transaction)
            .await
            .map_err(StorageError::from)?;
        ApprovalEntity::insert(approval.into_active_model())
            .exec(transaction)
            .await
            .map_err(StorageError::from)?;
        PgPolicyRepository::insert_snapshot_if_absent(transaction, snapshot_row.clone()).await?;
        let activation = ActivationEntity::insert(activation.into_active_model())
            .exec_with_returning(transaction)
            .await
            .map_err(StorageError::from)?;
        PgPolicyRepository::insert_activation_ledger(transaction, &activation, &snapshot_row)
            .await?;
        Self::advance_guard(transaction, guard, &snapshot_row, database_now).await?;
        let next_lifecycle_generation = binding
            .lifecycle_generation
            .checked_add(1)
            .ok_or_else(|| Self::conflict("shadow cancellation lifecycle overflowed"))?;
        let updated = ShadowBindingEntity::update_many()
            .col_expr(
                ShadowBindingColumn::Status,
                Expr::value(ShadowBindingStatus::Cancelled),
            )
            .col_expr(
                ShadowBindingColumn::LifecycleGeneration,
                Expr::value(next_lifecycle_generation),
            )
            .col_expr(
                ShadowBindingColumn::TerminationPolicyActivationId,
                Expr::value(Some(activation_id)),
            )
            .col_expr(
                ShadowBindingColumn::TerminationRequestHash,
                Expr::value(Some(request_hash)),
            )
            .col_expr(
                ShadowBindingColumn::TerminationReasonCode,
                Expr::value(Some(command.reason_code.clone())),
            )
            .col_expr(
                ShadowBindingColumn::TerminationNote,
                Expr::value(Some(command.note.clone())),
            )
            .col_expr(
                ShadowBindingColumn::TerminationActorRole,
                Expr::value(Some(RoleCode::new(SYSTEM_ACTOR_ROLE))),
            )
            .filter(ShadowBindingColumn::BindingId.eq(binding.binding_id))
            .filter(ShadowBindingColumn::FeedbackCycleId.eq(command.feedback_cycle_id))
            .filter(ShadowBindingColumn::Status.eq(ShadowBindingStatus::Active))
            .filter(ShadowBindingColumn::LifecycleGeneration.eq(binding.lifecycle_generation))
            .filter(ShadowBindingColumn::BindingGeneration.eq(binding.binding_generation))
            .filter(ShadowBindingColumn::TerminationPolicyActivationId.is_null())
            .exec(transaction)
            .await
            .map_err(StorageError::from)?;
        if updated.rows_affected != 1 {
            return Err(Self::conflict("shadow-cancellation terminal CAS affected no row").into());
        }
        Ok(())
    }

    async fn insert_rejection(
        transaction: &DatabaseTransaction,
        guard: &ActivationGuardModel,
        binding: &ShadowBindingModel,
        command: &RejectShadowBinding,
        authorized: &AuthorizedGovernedActor,
        current: &ActivePolicyBundle,
        database_now: DateTime<Utc>,
    ) -> QuantResult<()> {
        let request_hash = command.request_hash()?;
        let revision_id = PolicyRevisionId::from_v7();
        let approval_id = PolicyApprovalId::from_v7();
        let activation_id = PolicyActivationId::from_v7();
        let audit_event_id = AuditEventId::from_content_hash(&request_hash);
        let reason = Self::rejection_reason(command);
        let (snapshot, next_policy_generation) = Self::clear_shadow(
            current,
            binding,
            command.expected_binding_generation,
            revision_id,
            "rejection",
        )?;
        let snapshot_hash = snapshot
            .persistence_hash()
            .map_err(|error| Self::conflict(error.to_string()))?;
        let snapshot_id = DecisionPolicySnapshotId::from_content_hash(&snapshot_hash);
        let document = PolicyDocument::ModelRouting(snapshot.model_routing.clone());
        let revision_hash =
            CanonicalDigest::content_hash_json(&document).map_err(FeedbackError::from)?;
        let validation_subject = PolicyValidationSubject {
            base_generation: current.generation,
            base_revision_vector: current.revision_vector.clone(),
            candidate_bundle_hash: snapshot_hash,
        };
        let snapshot_row = Self::snapshot_row(&snapshot, snapshot_id, snapshot_hash, &reason)?;
        let revision = NewPolicyRevision {
            policy_revision_id: revision_id,
            resource_kind: ConfigResourceKind::ModelRouting,
            schema_version: snapshot.model_routing.schema_version,
            revision_hash,
            document,
            status: PolicyRevisionStatus::Validated,
            validation_evidence: Some(PolicyValidationEvidence {
                subject: Some(validation_subject.clone()),
                issues: Vec::new(),
                preflight: Self::passed_preflight(),
            }),
            validated_at: Some(database_now),
            preflight_token_hash: Some(request_hash),
            preflight_expires_at: None,
            created_by_kind: PolicyActorKind::Operator,
            created_by_user_id: Some(authorized.user_id),
            created_by_label: authorized.username.clone(),
            reason: reason.clone(),
        };
        let approval = NewPolicyApproval {
            policy_approval_id: approval_id,
            policy_revision_id: revision_id,
            resource_kind: ConfigResourceKind::ModelRouting,
            revision_hash,
            validation_subject: Some(validation_subject),
            decision: PolicyApprovalDecision::Approved,
            decided_by_kind: PolicyActorKind::Operator,
            decided_by_user_id: Some(authorized.user_id),
            decided_by_label: authorized.username.clone(),
            reason: reason.clone(),
            decided_at: database_now,
            expires_at: None,
        };
        let activation = NewPolicyActivation {
            bundle_generation: next_policy_generation,
            expected_bundle_generation: current.generation,
            policy_activation_id: activation_id,
            resource_kind: ConfigResourceKind::ModelRouting,
            policy_revision_id: revision_id,
            decision_policy_snapshot_id: snapshot_id,
            policy_approval_id: approval_id,
            activated_by_kind: PolicyActorKind::Operator,
            activated_by_user_id: Some(authorized.user_id),
            activated_by_label: authorized.username.clone(),
            reason,
            activation_kind: PolicyActivationKind::ModelShadowRejection,
            expected_active_revision_id: current.revision_vector.model_routing,
            previous_policy_revision_id: current.revision_vector.model_routing,
            rollback_target_revision_id: None,
            preflight_token_hash: request_hash,
            idempotency_key: command.idempotency_key.clone(),
            activation_request_hash: request_hash,
            audit_event_id,
        };
        RevisionEntity::insert(revision.into_active_model())
            .exec(transaction)
            .await
            .map_err(StorageError::from)?;
        ApprovalEntity::insert(approval.into_active_model())
            .exec(transaction)
            .await
            .map_err(StorageError::from)?;
        PgPolicyRepository::insert_snapshot_if_absent(transaction, snapshot_row.clone()).await?;
        let activation = ActivationEntity::insert(activation.into_active_model())
            .exec_with_returning(transaction)
            .await
            .map_err(StorageError::from)?;
        PgPolicyRepository::insert_activation_ledger(transaction, &activation, &snapshot_row)
            .await?;
        Self::advance_guard(transaction, guard, &snapshot_row, database_now).await?;
        let updated = ShadowBindingEntity::update_many()
            .col_expr(
                ShadowBindingColumn::Status,
                Expr::value(ShadowBindingStatus::Rejected),
            )
            .col_expr(
                ShadowBindingColumn::LifecycleGeneration,
                Expr::value(binding.lifecycle_generation + 1),
            )
            .col_expr(
                ShadowBindingColumn::TerminationPolicyActivationId,
                Expr::value(Some(activation_id)),
            )
            .col_expr(
                ShadowBindingColumn::TerminationRequestHash,
                Expr::value(Some(request_hash)),
            )
            .col_expr(
                ShadowBindingColumn::TerminationReasonCode,
                Expr::value(Some(command.reason_code.clone())),
            )
            .col_expr(
                ShadowBindingColumn::TerminationNote,
                Expr::value(Some(command.note.clone())),
            )
            .col_expr(
                ShadowBindingColumn::TerminationActorRole,
                Expr::value(Some(authorized.role.clone())),
            )
            .filter(ShadowBindingColumn::BindingId.eq(binding.binding_id))
            .filter(ShadowBindingColumn::Status.eq(ShadowBindingStatus::Active))
            .filter(ShadowBindingColumn::LifecycleGeneration.eq(binding.lifecycle_generation))
            .filter(ShadowBindingColumn::BindingGeneration.eq(binding.binding_generation))
            .filter(ShadowBindingColumn::TerminationPolicyActivationId.is_null())
            .exec(transaction)
            .await
            .map_err(StorageError::from)?;
        if updated.rows_affected != 1 {
            return Err(Self::conflict("shadow-rejection terminal CAS affected no row").into());
        }
        Ok(())
    }
}

#[async_trait]
impl ModelRouteShadowBindingRepository for PgModelRouteShadowBindingRepository {
    async fn find_lifecycle(
        &self,
        binding_id: &ShadowBindingArtifactId,
    ) -> QuantResult<Option<ShadowBindingLifecycle>> {
        let Some(binding) = ShadowBindingEntity::find_by_id(*binding_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Ok(None);
        };
        let lifecycle_generation = u64::try_from(binding.lifecycle_generation)
            .map_err(|error| Self::conflict(format!("lifecycle generation is invalid: {error}")))?;
        let binding_generation = u64::try_from(binding.binding_generation)
            .map_err(|error| Self::conflict(format!("binding generation is invalid: {error}")))?;
        Ok(Some(ShadowBindingLifecycle {
            binding_id: binding.binding_id,
            feedback_cycle_id: binding.feedback_cycle_id,
            route: binding.route,
            status: binding.status,
            lifecycle_generation,
            binding_generation,
            champion_model_version_id: binding.champion_model_version_id,
            candidate_model_version_id: binding.candidate_model_version_id,
            committed_policy_generation: binding.committed_policy_generation,
            bound_at: binding.bound_at,
            terminated_at: binding.terminated_at,
            termination_policy_activation_id: binding.termination_policy_activation_id,
            termination_reason_code: binding.termination_reason_code,
        }))
    }

    async fn find_committed(
        &self,
        binding_id: &ShadowBindingArtifactId,
    ) -> QuantResult<Option<ShadowBindingCommit>> {
        Self::load_commit(&self.db, *binding_id).await
    }

    async fn commit(&self, params: ShadowBindingJobParams) -> QuantResult<ShadowBindingCommit> {
        params.validate()?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let guard = PgPolicyRepository::acquire_activation_lock(&transaction).await?;
        if let Some(commit) = Self::load_commit(&transaction, params.artifact_id).await? {
            commit.receipt.validate_for(&params)?;
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(commit);
        }
        Self::lock_cycle(&transaction, &params).await?;
        let comparison = Self::lock_comparison(&transaction, &params.comparison).await?;
        Self::verify_candidate_selection(&comparison, &params)?;
        let manifest = Self::lock_manifest(&transaction, &params).await?;
        Self::verify_models(&transaction, &params, &manifest).await?;
        let current = PgPolicyRepository::load_current_bundle_from(&transaction)
            .await?
            .ok_or_else(|| Self::conflict("no current policy bundle exists"))?;
        Self::verify_current(&guard, &current, &params)?;
        Self::reserve_budget(&transaction, &current, &params).await?;
        let database_now = primitives::statement_timestamp(&transaction).await?;
        let rows = Self::build_rows(&current, &params, database_now)?;
        Self::insert_rows(&transaction, &guard, rows, database_now).await?;
        let mut commit = Self::load_commit(&transaction, params.artifact_id)
            .await?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_MODEL_ROUTE_SHADOW_BINDING),
                    "committed shadow-binding graph disappeared before transaction commit",
                )
            })?;
        commit.receipt.validate_for(&params)?;
        transaction.commit().await.map_err(StorageError::from)?;
        commit.outcome = ShadowBindingCommitOutcome::Committed;
        Ok(commit)
    }

    async fn cancel(&self, command: CancelShadowBinding) -> QuantResult<ShadowBindingCancelCommit> {
        command.validate()?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let guard = PgPolicyRepository::acquire_activation_lock(&transaction).await?;
        if let Some(commit) =
            Self::load_cancellation(&transaction, &command.idempotency_key).await?
        {
            Self::verify_cancellation(&command, &commit)?;
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(commit);
        }
        Self::lock_cancellation_cycle(&transaction, &command).await?;
        let binding = ShadowBindingEntity::find_by_id(command.binding_id)
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_MODEL_ROUTE_SHADOW_BINDING, command.binding_id)
            })?;
        let expected_lifecycle_generation = i64::try_from(command.expected_lifecycle_generation)
            .map_err(|error| Self::conflict(format!("lifecycle generation overflow: {error}")))?;
        let expected_binding_generation = i64::try_from(command.expected_binding_generation)
            .map_err(|error| Self::conflict(format!("binding generation overflow: {error}")))?;
        if binding.feedback_cycle_id != command.feedback_cycle_id
            || binding.status != ShadowBindingStatus::Active
            || binding.lifecycle_generation != expected_lifecycle_generation
            || binding.binding_generation != expected_binding_generation
            || binding.termination_policy_activation_id.is_some()
        {
            return Err(Self::conflict(
                "shadow cancellation cycle, status, lifecycle, or binding generation changed",
            )
            .into());
        }
        let current = PgPolicyRepository::load_current_bundle_from(&transaction)
            .await?
            .ok_or_else(|| Self::conflict("no current policy bundle exists"))?;
        if guard.generation != command.expected_policy_generation
            || current.generation != command.expected_policy_generation
            || guard.current_snapshot_id != Some(current.decision_policy_snapshot_id)
            || guard.current_snapshot_hash != Some(current.snapshot_hash)
        {
            return Err(Self::conflict(
                "shadow cancellation policy guard or model-routing revision changed",
            )
            .into());
        }
        let database_now = primitives::statement_timestamp(&transaction).await?;
        Self::insert_cancellation(
            &transaction,
            &guard,
            &binding,
            &command,
            &current,
            database_now,
        )
        .await?;
        let mut commit = Self::load_cancellation(&transaction, &command.idempotency_key)
            .await?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_MODEL_ROUTE_SHADOW_BINDING),
                    "shadow-cancellation graph disappeared before transaction commit",
                )
            })?;
        Self::verify_cancellation(&command, &commit)?;
        transaction.commit().await.map_err(StorageError::from)?;
        commit.outcome = ShadowBindingCancelOutcome::Cancelled;
        Ok(commit)
    }

    async fn reject(&self, command: RejectShadowBinding) -> QuantResult<ShadowBindingRejectCommit> {
        command.validate()?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let authorized = authorization::authorize_actor::<QuantError>(
            &transaction,
            command.actor_user_id,
            &command.actor_role,
            ResourceType::Publication,
            Operation::Reject,
        )
        .await?;
        let guard = PgPolicyRepository::acquire_activation_lock(&transaction).await?;
        if let Some(commit) = Self::load_rejection(&transaction, &command.idempotency_key).await? {
            Self::verify_rejection(&command, &commit)?;
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(commit);
        }
        let binding = ShadowBindingEntity::find_by_id(command.binding_id)
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(QUANT_MODEL_ROUTE_SHADOW_BINDING, command.binding_id)
            })?;
        let expected_binding_generation = i64::try_from(command.expected_binding_generation)
            .map_err(|error| Self::conflict(format!("binding generation overflow: {error}")))?;
        if binding.status != ShadowBindingStatus::Active
            || binding.binding_generation != expected_binding_generation
            || binding.termination_policy_activation_id.is_some()
        {
            return Err(Self::conflict(
                "shadow rejection binding status, generation, or policy generation changed",
            )
            .into());
        }
        let current = PgPolicyRepository::load_current_bundle_from(&transaction)
            .await?
            .ok_or_else(|| Self::conflict("no current policy bundle exists"))?;
        if guard.generation != command.expected_policy_generation
            || current.generation != command.expected_policy_generation
            || guard.current_snapshot_id != Some(current.decision_policy_snapshot_id)
            || guard.current_snapshot_hash != Some(current.snapshot_hash)
        {
            return Err(Self::conflict(
                "shadow rejection policy guard or model-routing revision changed",
            )
            .into());
        }
        let database_now = primitives::statement_timestamp(&transaction).await?;
        Self::insert_rejection(
            &transaction,
            &guard,
            &binding,
            &command,
            &authorized,
            &current,
            database_now,
        )
        .await?;
        let mut commit = Self::load_rejection(&transaction, &command.idempotency_key)
            .await?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_MODEL_ROUTE_SHADOW_BINDING),
                    "shadow-rejection graph disappeared before transaction commit",
                )
            })?;
        Self::verify_rejection(&command, &commit)?;
        transaction.commit().await.map_err(StorageError::from)?;
        commit.outcome = ShadowBindingRejectOutcome::Rejected;
        Ok(commit)
    }
}
