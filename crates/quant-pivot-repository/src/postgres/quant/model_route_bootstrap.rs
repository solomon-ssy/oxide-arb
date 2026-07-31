//! Atomic `PostgreSQL` owner of a first-champion Buy route.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{
    feedback::{FeedbackError, RouteBootstrapCommitError},
    storage::StorageError,
};
use quant_pivot_models::{
    domain::{
        governance::{
            NewDecisionPolicySnapshot, NewModelBootstrapActivation, NewPolicyApproval,
            NewPolicyRevision,
        },
        quant::{
            BacktestPathSetInfo, BacktestReportInfo, CommitModelRouteBootstrap,
            ModelBootstrapPolicyProjection, ModelGovernanceAuditDetail, ModelRouteBootstrapPolicy,
            ModelRouteBootstrapPreflight, ModelRouteBootstrapRecord,
            ModelRouteBootstrapRecordInput, ModelRouteBootstrapRoute, ModelVersionInfo,
            NewModelGovernanceAudit,
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
        quant_backtest_path_set::Entity as PathSetEntity,
        quant_backtest_report::Entity as BacktestEntity,
        quant_calibration_artifact::Entity as CalibrationEntity,
        quant_model_governance_audit::{Entity as ModelAuditEntity, Model as ModelAuditModel},
        quant_model_spec::Entity as ModelSpecEntity,
        quant_model_version::{Entity as ModelVersionEntity, Model as ModelVersionModel},
        quant_training_dataset::Entity as TrainingDatasetEntity,
        system_runtime_control::Entity as RuntimeControlEntity,
    },
    enums::{
        quant::{CalibrationKind, ModelGovernanceAction, QuantRuntimeMode, TrainingDatasetStatus},
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
        ContentHash, DecisionPolicySnapshotId, PolicyActivationId, PolicyApprovalId,
        PolicyBundleGeneration, PolicyIdempotencyKey, PolicyRevisionId,
    },
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, QueryFilter, QuerySelect, TransactionTrait, sea_query::Expr,
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
        ModelRouteBootstrapCommit, ModelRouteBootstrapOutcome, ModelRouteBootstrapRepository,
    },
};

struct BootstrapRows {
    snapshot: NewDecisionPolicySnapshot,
    revision: NewPolicyRevision,
    approval: NewPolicyApproval,
    activation: NewModelBootstrapActivation,
    audit: NewModelGovernanceAudit,
    record: ModelRouteBootstrapRecord,
}

struct ResolvedCommit {
    commit: ModelRouteBootstrapCommit,
    record: ModelRouteBootstrapRecord,
}

/// Transaction owner for the dedicated empty-route bootstrap transition.
pub struct PgModelRouteBootstrapRepository {
    db: DatabaseConnection,
}

impl PgModelRouteBootstrapRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn conflict(detail: impl Into<String>) -> RouteBootstrapCommitError {
        FeedbackError::BootstrapTransactionConflict {
            detail: detail.into(),
        }
        .into()
    }

    async fn lock_runtime(
        transaction: &DatabaseTransaction,
        preflight: &ModelRouteBootstrapPreflight,
    ) -> Result<(), RouteBootstrapCommitError> {
        let runtime = RuntimeControlEntity::find_by_id(SYSTEM_RUNTIME_CONTROL_ID)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("system_runtime_control", SYSTEM_RUNTIME_CONTROL_ID)
            })?;
        if runtime.quant_runtime_mode != QuantRuntimeMode::ReportOnly
            || runtime.quant_runtime_mode != preflight.current_runtime_mode()
            || runtime.revision != preflight.expected_runtime_revision()
        {
            return Err(Self::conflict(
                "runtime mode or runtime-control revision changed before bootstrap",
            ));
        }
        Ok(())
    }

    async fn current_projection(
        transaction: &DatabaseTransaction,
        guard: &ActivationGuardModel,
        preflight: &ModelRouteBootstrapPreflight,
    ) -> Result<(ActivePolicyBundle, ModelBootstrapPolicyProjection), RouteBootstrapCommitError>
    {
        let bundle = PgPolicyRepository::load_current_bundle_from(transaction)
            .await?
            .ok_or_else(|| Self::conflict("no current policy bundle exists"))?;
        let guard_exact = guard.generation == preflight.expected_policy_generation()
            && guard.current_snapshot_id == Some(preflight.expected_snapshot_id())
            && guard.current_snapshot_hash == Some(preflight.expected_snapshot_hash());
        if !guard_exact
            || bundle.generation != preflight.expected_policy_generation()
            || bundle.decision_policy_snapshot_id != preflight.expected_snapshot_id()
            || bundle.snapshot_hash != preflight.expected_snapshot_hash()
            || bundle
                .snapshot
                .resource_revision_id(ConfigResourceKind::ModelRouting)
                != Some(&preflight.expected_route_revision())
        {
            return Err(Self::conflict(
                "policy generation, snapshot, route revision, or activation guard changed",
            ));
        }
        let manifest = preflight.manifest();
        let projection = ModelBootstrapPolicyProjection::try_new(
            &bundle,
            manifest.route(),
            manifest.model_version_id(),
        )?;
        if projection.non_route_policy_hash() != preflight.non_route_policy_hash() {
            return Err(Self::conflict(
                "non-route policy differs from the frozen bootstrap preflight",
            ));
        }
        Ok((bundle, projection))
    }

    async fn lock_parity(
        transaction: &DatabaseTransaction,
        preflight: &ModelRouteBootstrapPreflight,
    ) -> Result<(), RouteBootstrapCommitError> {
        PgFeatureParityRepository::verify_clear_latch_generation(
            transaction,
            &preflight.manifest().feature_parity_state_id(),
        )
        .await
        .map_err(Into::into)
    }

    async fn lock_model(
        transaction: &DatabaseTransaction,
        preflight: &ModelRouteBootstrapPreflight,
    ) -> Result<(ModelVersionModel, ModelVersionInfo), RouteBootstrapCommitError> {
        let manifest = preflight.manifest();
        let model_id = manifest.model_version_id();
        let row = ModelVersionEntity::find_by_id(model_id)
            .lock_exclusive()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("quant_model_version", model_id))?;
        let spec = ModelSpecEntity::find_by_id(manifest.model_spec_id())
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("quant_model_spec", manifest.model_spec_id()))?;
        if spec.definition_hash != manifest.model_spec_hash() {
            return Err(Self::conflict(
                "model-spec definition changed before bootstrap",
            ));
        }
        let info = PgModelRegistryRepository::require_version_info(transaction, &model_id).await?;
        manifest.validate_model(&info)?;
        Ok((row, info))
    }

    async fn lock_dataset(
        transaction: &DatabaseTransaction,
        preflight: &ModelRouteBootstrapPreflight,
        model: &ModelVersionInfo,
    ) -> Result<(), RouteBootstrapCommitError> {
        let manifest = preflight.manifest();
        let dataset_id = manifest.training_dataset_id();
        let row = TrainingDatasetEntity::find_by_id(dataset_id)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("quant_training_dataset", dataset_id))?;
        let bindings = model
            .verified_serving_contract()
            .map_err(|error| Self::conflict(error.to_string()))?
            .bindings();
        if row.status != TrainingDatasetStatus::Ready
            || row.model_spec_id != model.model_spec_id
            || row.model_spec_definition_hash != model.model_spec_definition_hash
            || row.research_profile_artifact_id != model.profile_ref.artifact_id()
            || row.dataset_hash != Some(bindings.transform.training_dataset_hash)
            || row.manifest_hash != Some(bindings.dataset.manifest_hash)
            || row.artifact_bytes_hash != Some(bindings.dataset.artifact_bytes_hash)
        {
            return Err(Self::conflict(
                "training dataset differs from the sealed serving contract",
            ));
        }
        Ok(())
    }

    async fn lock_validation(
        transaction: &DatabaseTransaction,
        preflight: &ModelRouteBootstrapPreflight,
        model: &ModelVersionInfo,
    ) -> Result<(), RouteBootstrapCommitError> {
        let manifest = preflight.manifest();
        let path_set: BacktestPathSetInfo = PathSetEntity::find_by_id(manifest.cpcv_path_set_id())
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("quant_backtest_path_set", manifest.cpcv_path_set_id())
            })?
            .into();
        path_set
            .verify_hash()
            .map_err(|error| Self::conflict(error.to_string()))?;
        if path_set.model_version_id != model.model_version_id
            || path_set.training_dataset_id != manifest.training_dataset_id()
            || path_set.decision_policy_snapshot_id != preflight.expected_snapshot_id()
            || path_set.path_set_hash != manifest.cpcv_path_set_hash()
        {
            return Err(Self::conflict(
                "CPCV path set differs from the bootstrap manifest",
            ));
        }
        let backtest: BacktestReportInfo =
            BacktestEntity::find_by_id(manifest.backtest_report_id())
                .lock_shared()
                .one(transaction)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| {
                    StorageError::not_found("quant_backtest_report", manifest.backtest_report_id())
                })?
                .into();
        backtest.verify_hash().map_err(Self::conflict)?;
        if backtest.model_version_id != model.model_version_id
            || backtest.decision_policy_snapshot_id != preflight.expected_snapshot_id()
            || backtest.report_hash != manifest.backtest_report_hash()
        {
            return Err(Self::conflict(
                "backtest report differs from the bootstrap manifest",
            ));
        }
        let report = manifest.quality_gate_report();
        report
            .validate()
            .map_err(|error| Self::conflict(error.to_string()))?;
        if !report.passed {
            return Err(Self::conflict(
                "bootstrap quality-gate report no longer authorizes the candidate",
            ));
        }
        Ok(())
    }

    async fn lock_calibration(
        transaction: &DatabaseTransaction,
        model: &ModelVersionInfo,
    ) -> Result<(), RouteBootstrapCommitError> {
        let binding = model
            .verified_serving_contract()
            .map_err(|error| Self::conflict(error.to_string()))?
            .bindings()
            .model
            .calibration
            .as_ref()
            .ok_or_else(|| Self::conflict("bootstrap candidate has no calibration binding"))?;
        let artifact = CalibrationEntity::find_by_id(binding.artifact_id)
            .lock_shared()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("quant_calibration_artifact", binding.artifact_id)
            })?;
        if !artifact.active
            || artifact.kind != CalibrationKind::ModelScore
            || artifact.content_hash != binding.content_hash
        {
            return Err(Self::conflict(
                "bootstrap calibration artifact is inactive or hash-incompatible",
            ));
        }
        Ok(())
    }

    async fn verify_parity(
        transaction: &DatabaseTransaction,
        preflight: &ModelRouteBootstrapPreflight,
        model: &ModelVersionModel,
    ) -> Result<(), RouteBootstrapCommitError> {
        let manifest = preflight.manifest();
        let evidence_hash = PgModelRegistryRepository::verify_parity_permit(
            transaction,
            &manifest.feature_parity_run_id(),
            model,
        )
        .await?;
        if evidence_hash != manifest.feature_parity_hash() {
            return Err(Self::conflict(
                "full-parity subject differs from the bootstrap manifest",
            ));
        }
        Ok(())
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
        record: &ModelRouteBootstrapRecord,
    ) -> Result<NewDecisionPolicySnapshot, RouteBootstrapCommitError> {
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
        command: &CommitModelRouteBootstrap,
        authorized: &AuthorizedGovernedActor,
        current: &ActivePolicyBundle,
        projection: &ModelBootstrapPolicyProjection,
        model: &ModelVersionInfo,
        database_now: DateTime<Utc>,
    ) -> Result<BootstrapRows, RouteBootstrapCommitError> {
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
        let policy = ModelRouteBootstrapPolicy {
            previous_generation: current.generation,
            transaction_revision: new_generation,
            previous_snapshot_id: current.decision_policy_snapshot_id,
            previous_snapshot_hash: current.snapshot_hash,
            committed_snapshot_id: snapshot_id,
            committed_snapshot_hash: snapshot_hash,
            previous_model_routing_revision_id: old_revision,
            committed_model_routing_revision_id: new_revision,
            policy_approval_id: PolicyApprovalId::from_v7(),
            policy_activation_id: PolicyActivationId::from_v7(),
        };
        let record = ModelRouteBootstrapRecord::try_seal(ModelRouteBootstrapRecordInput {
            preflight: command.preflight().clone(),
            actor_user_id: authorized.user_id,
            actor_username: authorized.username.clone(),
            actor_role: authorized.role.clone(),
            idempotency_key: command.request().idempotency_key.clone(),
            reason_code: command.request().reason_code.clone(),
            note: command.request().note.clone(),
            route: ModelRouteBootstrapRoute {
                route: projection.route(),
                model_version_id: model.model_version_id,
                model_artifact_hash: model.artifact_hash,
                serving_contract_hash: model.serving_contract_hash,
            },
            policy,
        })?;
        Self::finish_rows(current, &candidate_snapshot, model, database_now, record)
    }

    fn finish_rows(
        current: &ActivePolicyBundle,
        candidate_snapshot: &DecisionPolicySnapshot,
        model: &ModelVersionInfo,
        database_now: DateTime<Utc>,
        record: ModelRouteBootstrapRecord,
    ) -> Result<BootstrapRows, RouteBootstrapCommitError> {
        let policy = record.policy();
        let transaction_hash = record.transaction_hash();
        let audit_event_id = record.audit_event_id();
        let model_audit_id = record.audit_id();
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
            preflight_expires_at: None,
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
            expires_at: None,
        };
        let audit = NewModelGovernanceAudit {
            audit_id: model_audit_id,
            model_version_id: Some(model.model_version_id),
            training_dataset_id: model.training_dataset_id,
            action: ModelGovernanceAction::BootstrapRoute,
            actor_user_id: Some(record.actor_user_id()),
            actor_username: record.actor_username().to_owned(),
            actor_role: Some(record.actor_role().clone()),
            reason: audit_reason.clone(),
            detail: ModelGovernanceAuditDetail::BootstrapRoute {
                record: Box::new(record.clone()),
            },
            audit_event_id,
        };
        let activation = NewModelBootstrapActivation {
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
            activation_kind: PolicyActivationKind::ModelBootstrap,
            expected_active_revision_id: policy.previous_model_routing_revision_id,
            previous_policy_revision_id: policy.previous_model_routing_revision_id,
            rollback_target_revision_id: None,
            preflight_token_hash: record.preflight().preflight_hash(),
            idempotency_key: record.idempotency_key().clone(),
            activation_request_hash: transaction_hash,
            audit_event_id,
            model_governance_audit_id: model_audit_id,
        };
        Ok(BootstrapRows {
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
        rows: BootstrapRows,
        database_now: DateTime<Utc>,
    ) -> Result<ResolvedCommit, RouteBootstrapCommitError> {
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
        let resolved = Self::load_commit(transaction, rows.record.idempotency_key())
            .await?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("policy_activation"),
                    "committed bootstrap graph disappeared before transaction commit",
                )
            })?;
        if resolved.record != rows.record {
            return Err(Self::conflict(
                "inserted bootstrap graph differs from the sealed transaction record",
            ));
        }
        Ok(resolved)
    }

    async fn advance_guard(
        transaction: &DatabaseTransaction,
        guard: &ActivationGuardModel,
        snapshot: &NewDecisionPolicySnapshot,
        database_now: DateTime<Utc>,
    ) -> Result<(), RouteBootstrapCommitError> {
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

    fn bootstrap_record(
        audit: &ModelAuditModel,
    ) -> Result<ModelRouteBootstrapRecord, RouteBootstrapCommitError> {
        let ModelGovernanceAuditDetail::BootstrapRoute { record } = &audit.detail else {
            return Err(Self::conflict(
                "bootstrap audit lost its typed transaction record",
            ));
        };
        record.validate()?;
        Ok(record.as_ref().clone())
    }

    async fn load_commit(
        db: &impl ConnectionTrait,
        idempotency_key: &PolicyIdempotencyKey,
    ) -> Result<Option<ResolvedCommit>, RouteBootstrapCommitError> {
        let Some(activation) = ActivationEntity::find()
            .filter(ActivationColumn::IdempotencyKey.eq(idempotency_key.clone()))
            .one(db)
            .await
            .map_err(StorageError::from)?
        else {
            return Ok(None);
        };
        if activation.activation_kind != PolicyActivationKind::ModelBootstrap {
            return Err(Self::conflict(
                "bootstrap idempotency key belongs to another activation kind",
            ));
        }
        let audit_id = activation.model_governance_audit_id.ok_or_else(|| {
            Self::conflict("model-bootstrap activation has no model audit identity")
        })?;
        let audit = ModelAuditEntity::find_by_id(audit_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("quant_model_governance_audit", audit_id))?;
        let record = Self::bootstrap_record(&audit)?;
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
        Ok(Some(ResolvedCommit {
            commit: ModelRouteBootstrapCommit {
                activation: activation.into(),
                bundle,
                audit: audit.into(),
                transaction_hash: record.transaction_hash(),
                outcome: ModelRouteBootstrapOutcome::ExactReplay,
            },
            record,
        }))
    }

    async fn verify_activation(
        db: &impl ConnectionTrait,
        activation: &ActivationModel,
        audit: &ModelAuditModel,
        record: &ModelRouteBootstrapRecord,
    ) -> Result<(), RouteBootstrapCommitError> {
        let policy = record.policy();
        let transaction_hash = record.transaction_hash();
        let audit_reason = record.audit_reason();
        let audit_exact = audit.audit_id == record.audit_id()
            && audit.model_version_id == Some(record.route().model_version_id)
            && audit.training_dataset_id
                == Some(record.preflight().manifest().training_dataset_id())
            && audit.action == ModelGovernanceAction::BootstrapRoute
            && audit.actor_user_id == Some(record.actor_user_id())
            && audit.actor_username == record.actor_username()
            && audit.actor_role.as_ref() == Some(record.actor_role())
            && audit.reason == audit_reason
            && audit.audit_event_id == record.audit_event_id()
            && audit.promotion_permit_id.is_none()
            && audit.promotion_transaction_hash.is_none();
        let activation_exact = activation.activation_kind == PolicyActivationKind::ModelBootstrap
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
            && activation.promotion_permit_id.is_none()
            && activation.promotion_transaction_hash.is_none()
            && activation.model_governance_audit_id == Some(audit.audit_id);
        if !audit_exact || !activation_exact {
            return Err(Self::conflict(
                "model audit or activation differs from the bootstrap transaction record",
            ));
        }
        Self::verify_ledgers(db, activation).await
    }

    async fn verify_ledgers(
        db: &impl ConnectionTrait,
        activation: &ActivationModel,
    ) -> Result<(), RouteBootstrapCommitError> {
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
        let activation_time_matches = audit.occurred_at == activation.activated_at;
        let audit_exact = audit.policy_activation_id == activation.policy_activation_id
            && audit.bundle_generation == activation.bundle_generation
            && audit.resource_kind == activation.resource_kind
            && audit.policy_revision_id == activation.policy_revision_id
            && audit.decision_policy_snapshot_id == activation.decision_policy_snapshot_id
            && audit.activation_request_hash == activation.activation_request_hash
            && activation_time_matches
            && actor_exact
            && audit.promotion_permit_id.is_none()
            && audit.promotion_transaction_hash.is_none()
            && audit.model_governance_audit_id == activation.model_governance_audit_id;
        let outbox_exact = outbox.policy_activation_id == activation.policy_activation_id
            && outbox.bundle_generation == activation.bundle_generation
            && outbox.decision_policy_snapshot_id == activation.decision_policy_snapshot_id
            && outbox.snapshot_hash == audit.snapshot_hash
            && outbox.promotion_permit_id.is_none()
            && outbox.promotion_transaction_hash.is_none()
            && outbox.model_governance_audit_id == activation.model_governance_audit_id;
        if !audit_exact || !outbox_exact {
            return Err(Self::conflict(
                "policy audit or durable outbox differs from bootstrap activation",
            ));
        }
        Ok(())
    }

    async fn verify_policy(
        db: &impl ConnectionTrait,
        activation: &ActivationModel,
        record: &ModelRouteBootstrapRecord,
        committed: &ActivePolicyBundle,
    ) -> Result<(), RouteBootstrapCommitError> {
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
                "committed policy bundle differs from the bootstrap record",
            ));
        }
        let previous = Self::load_historical_bundle(
            db,
            policy.previous_generation,
            policy.previous_snapshot_id,
            policy.previous_snapshot_hash,
        )
        .await?;
        let projection = ModelBootstrapPolicyProjection::try_new(
            &previous,
            record.route().route,
            record.route().model_version_id,
        )?;
        projection.validate_candidate(&committed.snapshot)?;
        if previous
            .snapshot
            .resource_revision_id(ConfigResourceKind::ModelRouting)
            != Some(&policy.previous_model_routing_revision_id)
            || projection.non_route_policy_hash() != record.preflight().non_route_policy_hash()
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
    ) -> Result<ActivePolicyBundle, RouteBootstrapCommitError> {
        let model: SnapshotModel = SnapshotEntity::find_by_id(snapshot_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("decision_policy_snapshot", snapshot_id))?;
        let snapshot = PgPolicyRepository::resolve_snapshot_model(db, model).await?;
        if snapshot.snapshot_hash != snapshot_hash {
            return Err(Self::conflict(
                "historical policy snapshot hash differs from the bootstrap record",
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
        record: &ModelRouteBootstrapRecord,
        previous: &ActivePolicyBundle,
        committed: &ActivePolicyBundle,
    ) -> Result<(), RouteBootstrapCommitError> {
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
            && revision.preflight_expires_at.is_none()
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
            && approval.expires_at.is_none();
        if !revision_exact || !approval_exact {
            return Err(Self::conflict(
                "policy revision or approval differs from the bootstrap record",
            ));
        }
        Ok(())
    }

    fn command_matches(
        record: &ModelRouteBootstrapRecord,
        command: &CommitModelRouteBootstrap,
        authorized: &AuthorizedGovernedActor,
    ) -> bool {
        let request = command.request();
        record.preflight().manifest().model_version_id() == request.model_version_id
            && record.preflight().expected_policy_generation() == request.expected_policy_generation
            && record.preflight().expected_runtime_revision()
                == request.expected_runtime_control_revision
            && record.actor_user_id() == authorized.user_id
            && record.actor_username() == authorized.username
            && record.actor_role() == &authorized.role
            && record.idempotency_key() == &request.idempotency_key
            && record.reason_code() == request.reason_code
            && record.note() == request.note
    }
}

#[async_trait]
impl ModelRouteBootstrapRepository for PgModelRouteBootstrapRepository {
    async fn find_committed(
        &self,
        idempotency_key: &PolicyIdempotencyKey,
    ) -> Result<Option<ModelRouteBootstrapCommit>, RouteBootstrapCommitError> {
        Self::load_commit(&self.db, idempotency_key)
            .await
            .map(|resolved| resolved.map(|value| value.commit))
    }

    async fn commit(
        &self,
        command: CommitModelRouteBootstrap,
    ) -> Result<ModelRouteBootstrapCommit, RouteBootstrapCommitError> {
        command.preflight().validate()?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let authorized = authorization::authorize_actor::<RouteBootstrapCommitError>(
            &transaction,
            command.request().actor.user_id,
            &command.request().actor.acting_role,
            ResourceType::Publication,
            Operation::Publish,
        )
        .await?;
        let guard = PgPolicyRepository::acquire_activation_lock(&transaction).await?;
        if let Some(resolved) =
            Self::load_commit(&transaction, &command.request().idempotency_key).await?
        {
            if !Self::command_matches(&resolved.record, &command, &authorized) {
                return Err(Self::conflict(
                    "bootstrap idempotency key is committed with a different request",
                ));
            }
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(resolved.commit);
        }
        Self::lock_runtime(&transaction, command.preflight()).await?;
        let (bundle, projection) =
            Self::current_projection(&transaction, &guard, command.preflight()).await?;
        Self::lock_parity(&transaction, command.preflight()).await?;
        let (model_row, model) = Self::lock_model(&transaction, command.preflight()).await?;
        Self::lock_dataset(&transaction, command.preflight(), &model).await?;
        Self::lock_validation(&transaction, command.preflight(), &model).await?;
        Self::lock_calibration(&transaction, &model).await?;
        Self::verify_parity(&transaction, command.preflight(), &model_row).await?;
        let database_now = primitives::statement_timestamp(&transaction).await?;
        let rows = Self::build_rows(
            &command,
            &authorized,
            &bundle,
            &projection,
            &model,
            database_now,
        )?;
        let mut resolved =
            Box::pin(Self::insert_rows(&transaction, &guard, rows, database_now)).await?;
        transaction.commit().await.map_err(StorageError::from)?;
        resolved.commit.outcome = ModelRouteBootstrapOutcome::Committed;
        Ok(resolved.commit)
    }
}
