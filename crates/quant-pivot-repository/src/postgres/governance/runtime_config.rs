use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::governance::{
        ActivePolicyResourceInfo, ConfigActivityInfo, ConfigResourceInventoryInfo,
        DecisionPolicySnapshotInfo, DecisionPolicySnapshotOptionInfo, NewDecisionPolicySnapshot,
        NewPolicyActivation, NewPolicyApproval, NewPolicyProfileArtifact, NewPolicyRevision,
        PolicyActivationCommit, PolicyActivationInfo, PolicyActivationOutcome, PolicyApprovalInfo,
        PolicyRevisionInfo, RecordPolicyApproval,
    },
    entities::{
        decision_policy_snapshot::{
            Column as SnapshotColumn, Entity as SnapshotEntity, Model as SnapshotModel,
        },
        policy_activation::{
            Column as ActivationColumn, Entity as ActivationEntity, Model as ActivationModel,
        },
        policy_activation_audit::{
            ActiveModel as ActivationAuditActiveModel, Entity as ActivationAuditEntity,
        },
        policy_activation_event_outbox::{
            ActiveModel as ActivationOutboxActiveModel, Entity as ActivationOutboxEntity,
        },
        policy_activation_guard::{
            Column as ActivationGuardColumn, Entity as ActivationGuardEntity,
            Model as ActivationGuardModel,
        },
        policy_approval::{
            Column as ApprovalColumn, Entity as ApprovalEntity, Relation as ApprovalRelation,
        },
        policy_profile_artifact::{
            Column as ProfileArtifactColumn, Entity as ProfileArtifactEntity,
            Model as ProfileArtifactModel,
        },
        policy_revision::{
            Column as RevisionColumn, Entity as RevisionEntity, Model as RevisionModel,
        },
    },
    enums::runtime_config::{
        ConfigResourceKind, PolicyActivationKind, PolicyActorKind, PolicyApprovalDecision,
        PolicyRevisionStatus,
    },
    hashing::CanonicalDigest,
    runtime_config::{
        ActivePolicyBundle, ImmutableProfileArtifactReferences, ImmutableProfileArtifacts,
        PolicyProfileDocument, PolicySnapshotError, PolicyValidationEvidence,
    },
    types::{
        ContentHash, DecisionPolicySnapshotId, PolicyApprovalId, PolicyRevisionId,
        ProfileArtifactId,
    },
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    DatabaseTransaction, EntityTrait, ExprTrait, IntoActiveModel, JoinType, QueryFilter,
    QueryOrder, QuerySelect, RelationTrait, Set, TransactionTrait, TryInsertResult,
    sea_query::{Expr, Query, extension::postgres::PgExpr},
};

use crate::{
    postgres::{error, governance::config_resources, primitives},
    traits::PolicyRepository,
};

const POLICY_ACTIVATION_GUARD_ID: i16 = 1;

fn profile_error(error: &PolicySnapshotError) -> StorageError {
    StorageError::invariant_violation(Some("policy_profile_artifact"), error.to_string())
}

impl PgPolicyRepository {
    async fn resolve_idempotent_activation(
        db: &impl ConnectionTrait,
        activation: &NewPolicyActivation,
    ) -> Result<Option<(PolicyActivationInfo, ActivePolicyBundle)>, StorageError> {
        let Some(existing) = ActivationEntity::find()
            .filter(ActivationColumn::IdempotencyKey.eq(&activation.idempotency_key))
            .one(db)
            .await
            .map_err(StorageError::from)?
        else {
            return Ok(None);
        };
        if existing.activation_request_hash != activation.activation_request_hash {
            return Err(StorageError::state_conflict(
                "policy_activation",
                Some(&activation.idempotency_key),
                "idempotency key is already bound to a different activation",
            ));
        }
        let committed_snapshot = SnapshotEntity::find_by_id(existing.decision_policy_snapshot_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("policy_activation"),
                    "idempotent activation references a missing snapshot",
                )
            })?;
        let committed_info = Self::resolve_snapshot_model(db, committed_snapshot).await?;
        let bundle = ActivePolicyBundle::from_parts(
            existing.bundle_generation,
            committed_info.decision_policy_snapshot_id,
            committed_info.snapshot_hash,
            committed_info.snapshot,
        );
        Ok(Some((existing.into(), bundle)))
    }
}

impl PgPolicyRepository {
    async fn insert_activation_ledger(
        db: &impl ConnectionTrait,
        inserted: &ActivationModel,
        snapshot: &NewDecisionPolicySnapshot,
    ) -> Result<(), StorageError> {
        ActivationAuditEntity::insert(ActivationAuditActiveModel {
            audit_event_id: Set(inserted.audit_event_id),
            policy_activation_id: Set(inserted.policy_activation_id),
            bundle_generation: Set(inserted.bundle_generation),
            resource_kind: Set(inserted.resource_kind),
            policy_revision_id: Set(inserted.policy_revision_id),
            decision_policy_snapshot_id: Set(inserted.decision_policy_snapshot_id),
            snapshot_hash: Set(snapshot.snapshot_hash),
            activation_request_hash: Set(inserted.activation_request_hash),
            actor_kind: Set(inserted.activated_by_kind),
            actor_user_id: Set(inserted.activated_by_user_id),
            actor_label: Set(inserted.activated_by_label.clone()),
            reason: Set(inserted.reason.clone()),
            occurred_at: Set(inserted.activated_at),
            created_at: Set(inserted.created_at),
        })
        .exec(db)
        .await
        .map_err(StorageError::from)?;
        ActivationOutboxEntity::insert(ActivationOutboxActiveModel {
            audit_event_id: Set(inserted.audit_event_id),
            policy_activation_id: Set(inserted.policy_activation_id),
            bundle_generation: Set(inserted.bundle_generation),
            decision_policy_snapshot_id: Set(inserted.decision_policy_snapshot_id),
            snapshot_hash: Set(snapshot.snapshot_hash),
            created_at: Set(inserted.created_at),
        })
        .exec(db)
        .await
        .map_err(StorageError::from)?;
        Ok(())
    }
}

fn verify_profile_artifact_row(row: &ProfileArtifactModel) -> Result<(), StorageError> {
    let document_hash = row
        .document
        .content_hash()
        .map_err(|error| profile_error(&error))?;
    let expected_id = ProfileArtifactId::from_content_address(row.kind.as_str(), &document_hash);
    if row.document.kind() != row.kind
        || row.document.schema_version() != row.schema_version
        || document_hash != row.content_hash
        || expected_id != row.profile_artifact_id
    {
        return Err(StorageError::invariant_violation(
            Some("policy_profile_artifact"),
            format!(
                "artifact {} kind/schema/hash/content address is inconsistent",
                row.profile_artifact_id
            ),
        ));
    }
    Ok(())
}

fn verify_snapshot_model(model: &SnapshotModel) -> Result<(), StorageError> {
    let actual_hash = CanonicalDigest::content_hash_json(&model.snapshot).map_err(|error| {
        StorageError::invariant_violation(
            Some("decision_policy_snapshot"),
            format!("failed to hash persisted decision snapshot: {error}"),
        )
    })?;
    let expected_id = DecisionPolicySnapshotId::from_content_hash(&actual_hash);
    let projected_revisions = [
        (
            ConfigResourceKind::RecommendationPolicy,
            model.recommendation_policy_revision_id,
        ),
        (
            ConfigResourceKind::ExecutionRiskPolicy,
            model.execution_risk_policy_revision_id,
        ),
        (
            ConfigResourceKind::ModelRouting,
            model.model_routing_revision_id,
        ),
        (
            ConfigResourceKind::ReportSchedule,
            model.report_schedule_revision_id,
        ),
        (
            ConfigResourceKind::OperationalControl,
            model.operational_control_revision_id,
        ),
        (
            ConfigResourceKind::ExecutionAuthorization,
            model.execution_authorization_revision_id,
        ),
    ];
    if actual_hash != model.snapshot_hash
        || expected_id != model.decision_policy_snapshot_id
        || projected_revisions.iter().any(|(kind, revision_id)| {
            model.snapshot.resource_revision_id(*kind) != Some(revision_id)
        })
    {
        return Err(StorageError::invariant_violation(
            Some("decision_policy_snapshot"),
            format!(
                "snapshot {} has inconsistent content hash, content-addressed id, or revision projection",
                model.decision_policy_snapshot_id
            ),
        ));
    }
    Ok(())
}

impl PgPolicyRepository {
    async fn load_profile_documents(
        db: &impl ConnectionTrait,
        references: &ImmutableProfileArtifactReferences,
    ) -> Result<Vec<(ProfileArtifactId, PolicyProfileDocument)>, StorageError> {
        let ids = references
            .all()
            .into_iter()
            .map(|reference| reference.profile_artifact_id)
            .collect::<Vec<_>>();
        let rows = ProfileArtifactEntity::find()
            .filter(ProfileArtifactColumn::ProfileArtifactId.is_in(ids))
            .all(db)
            .await
            .map_err(StorageError::from)?;
        if rows.len() != references.all().len() {
            return Err(StorageError::invariant_violation(
                Some("policy_profile_artifact"),
                format!(
                    "snapshot references {} profile artifacts but {} rows were loaded",
                    references.all().len(),
                    rows.len()
                ),
            ));
        }
        rows.into_iter()
            .map(|row| {
                verify_profile_artifact_row(&row)?;
                Ok((row.profile_artifact_id, row.document))
            })
            .collect()
    }
}

impl PgPolicyRepository {
    async fn resolve_snapshot_model(
        db: &impl ConnectionTrait,
        model: SnapshotModel,
    ) -> Result<DecisionPolicySnapshotInfo, StorageError> {
        verify_snapshot_model(&model)?;
        let documents =
            Self::load_profile_documents(db, &model.snapshot.profile_artifact_refs).await?;
        let snapshot = model
            .snapshot
            .clone()
            .resolve(documents)
            .map_err(|error| profile_error(&error))?;
        Ok(DecisionPolicySnapshotInfo {
            decision_policy_snapshot_id: model.decision_policy_snapshot_id,
            snapshot_hash: model.snapshot_hash,
            snapshot,
            recommendation_policy_revision_id: model.recommendation_policy_revision_id,
            execution_risk_policy_revision_id: model.execution_risk_policy_revision_id,
            model_routing_revision_id: model.model_routing_revision_id,
            report_schedule_revision_id: model.report_schedule_revision_id,
            operational_control_revision_id: model.operational_control_revision_id,
            execution_authorization_revision_id: model.execution_authorization_revision_id,
            source: model.source,
            created_by_kind: model.created_by_kind,
            created_by_user_id: model.created_by_user_id,
            created_by_label: model.created_by_label,
            reason: model.reason,
            created_at: model.created_at,
        })
    }
}

impl PgPolicyRepository {
    async fn resolve_snapshot_models(
        db: &impl ConnectionTrait,
        models: Vec<SnapshotModel>,
    ) -> Result<Vec<DecisionPolicySnapshotInfo>, StorageError> {
        let ids = models
            .iter()
            .flat_map(|model| model.snapshot.profile_artifact_refs.all())
            .map(|reference| reference.profile_artifact_id)
            .collect::<Vec<_>>();
        let rows = if ids.is_empty() {
            Vec::new()
        } else {
            ProfileArtifactEntity::find()
                .filter(ProfileArtifactColumn::ProfileArtifactId.is_in(ids))
                .all(db)
                .await
                .map_err(StorageError::from)?
        };
        let documents = rows
            .into_iter()
            .map(|row| {
                verify_profile_artifact_row(&row)?;
                Ok((row.profile_artifact_id, row.document))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        models
            .into_iter()
            .map(|model| {
                verify_snapshot_model(&model)?;
                let snapshot = model
                    .snapshot
                    .clone()
                    .resolve(documents.clone())
                    .map_err(|error| profile_error(&error))?;
                Ok(DecisionPolicySnapshotInfo {
                    decision_policy_snapshot_id: model.decision_policy_snapshot_id,
                    snapshot_hash: model.snapshot_hash,
                    snapshot,
                    recommendation_policy_revision_id: model.recommendation_policy_revision_id,
                    execution_risk_policy_revision_id: model.execution_risk_policy_revision_id,
                    model_routing_revision_id: model.model_routing_revision_id,
                    report_schedule_revision_id: model.report_schedule_revision_id,
                    operational_control_revision_id: model.operational_control_revision_id,
                    execution_authorization_revision_id: model.execution_authorization_revision_id,
                    source: model.source,
                    created_by_kind: model.created_by_kind,
                    created_by_user_id: model.created_by_user_id,
                    created_by_label: model.created_by_label,
                    reason: model.reason,
                    created_at: model.created_at,
                })
            })
            .collect()
    }
}

pub struct PgPolicyRepository {
    db: DatabaseConnection,
}

impl PgPolicyRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub(crate) async fn acquire_activation_lock(
        transaction: &DatabaseTransaction,
    ) -> Result<ActivationGuardModel, StorageError> {
        ActivationGuardEntity::find_by_id(POLICY_ACTIVATION_GUARD_ID)
            .lock_exclusive()
            .one(transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("policy_activation_guard"),
                    "boot seed row is missing",
                )
            })
    }

    pub(crate) async fn load_current_from(
        db: &impl ConnectionTrait,
    ) -> Result<Option<DecisionPolicySnapshotInfo>, StorageError> {
        let row = ActivationGuardEntity::find_by_id(POLICY_ACTIVATION_GUARD_ID)
            .find_also_related(SnapshotEntity)
            .one(db)
            .await
            .map_err(StorageError::from)?;
        match row {
            None => Err(StorageError::invariant_violation(
                Some("policy_activation_guard"),
                "boot seed row is missing",
            )),
            Some((guard, None)) if guard.current_snapshot_id.is_none() => Ok(None),
            Some((guard, None)) => Err(StorageError::invariant_violation(
                Some("policy_activation_guard"),
                format!(
                    "generation {} references missing decision snapshot {}",
                    guard.generation,
                    guard
                        .current_snapshot_id
                        .map_or_else(|| "<none>".to_owned(), |id| id.to_string())
                ),
            )),
            Some((guard, Some(snapshot))) => {
                verify_guard_snapshot(&guard, &snapshot)?;
                Self::resolve_snapshot_model(db, snapshot).await.map(Some)
            }
        }
    }
}

impl PgPolicyRepository {
    async fn load_current_activation_from(
        db: &impl ConnectionTrait,
        kind: Option<ConfigResourceKind>,
    ) -> Result<Option<PolicyActivationInfo>, StorageError> {
        let mut query = ActivationEntity::find();
        if let Some(kind) = kind {
            query = query.filter(ActivationColumn::ResourceKind.eq(kind));
        }
        query
            .order_by_desc(ActivationColumn::ActivatedAt)
            .order_by_desc(ActivationColumn::PolicyActivationId)
            .one(db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }
}

impl PgPolicyRepository {
    async fn load_current_bundle_from(
        db: &impl ConnectionTrait,
    ) -> Result<Option<ActivePolicyBundle>, StorageError> {
        let row = ActivationGuardEntity::find_by_id(POLICY_ACTIVATION_GUARD_ID)
            .find_also_related(SnapshotEntity)
            .one(db)
            .await
            .map_err(StorageError::from)?;
        match row {
            None => Err(StorageError::invariant_violation(
                Some("policy_activation_guard"),
                "boot seed row is missing",
            )),
            Some((guard, None)) if guard.current_snapshot_id.is_none() => Ok(None),
            Some((_guard, None)) => Err(StorageError::invariant_violation(
                Some("policy_activation_guard"),
                "current snapshot relation is missing",
            )),
            Some((guard, Some(snapshot))) => {
                verify_guard_snapshot(&guard, &snapshot)?;
                let generation = guard.generation;
                let info = Self::resolve_snapshot_model(db, snapshot).await?;
                Ok(Some(ActivePolicyBundle::from_parts(
                    generation,
                    info.decision_policy_snapshot_id,
                    info.snapshot_hash,
                    info.snapshot,
                )))
            }
        }
    }
}

fn verify_guard_snapshot(
    guard: &ActivationGuardModel,
    snapshot: &SnapshotModel,
) -> Result<(), StorageError> {
    if guard.current_snapshot_id.as_ref() != Some(&snapshot.decision_policy_snapshot_id)
        || guard.current_snapshot_hash.as_ref() != Some(&snapshot.snapshot_hash)
    {
        return Err(StorageError::invariant_violation(
            Some("policy_activation_guard"),
            "guard id/hash does not match its current decision snapshot",
        ));
    }
    Ok(())
}

impl PgPolicyRepository {
    async fn insert_snapshot_if_absent(
        db: &impl ConnectionTrait,
        snapshot: NewDecisionPolicySnapshot,
    ) -> Result<(), StorageError> {
        let snapshot_id = snapshot.decision_policy_snapshot_id;
        let outcome = SnapshotEntity::insert(snapshot.clone().into_active_model())
            .on_conflict_do_nothing_on([SnapshotColumn::DecisionPolicySnapshotId])
            .exec_without_returning(db)
            .await
            .map_err(StorageError::from)?;
        match outcome {
            TryInsertResult::Inserted(1) => Ok(()),
            TryInsertResult::Inserted(0) | TryInsertResult::Conflicted => {
                let existing = SnapshotEntity::find_by_id(snapshot_id)
                    .one(db)
                    .await
                    .map_err(StorageError::from)?
                    .ok_or_else(|| {
                        StorageError::invariant_violation(
                            Some("decision_policy_snapshot"),
                            "conflicting snapshot disappeared before verification",
                        )
                    })?;
                verify_snapshot_model(&existing)?;
                if existing.snapshot_hash != snapshot.snapshot_hash
                    || existing.snapshot != snapshot.snapshot
                    || existing.recommendation_policy_revision_id
                        != snapshot.recommendation_policy_revision_id
                    || existing.execution_risk_policy_revision_id
                        != snapshot.execution_risk_policy_revision_id
                    || existing.model_routing_revision_id != snapshot.model_routing_revision_id
                    || existing.report_schedule_revision_id != snapshot.report_schedule_revision_id
                    || existing.operational_control_revision_id
                        != snapshot.operational_control_revision_id
                    || existing.execution_authorization_revision_id
                        != snapshot.execution_authorization_revision_id
                {
                    return Err(StorageError::state_conflict(
                        "decision_policy_snapshot",
                        Some(&snapshot_id),
                        "snapshot id already exists with different immutable content or revision projection",
                    ));
                }
                Ok(())
            }
            TryInsertResult::Inserted(rows) => Err(StorageError::invariant_violation(
                Some("decision_policy_snapshot"),
                format!("single snapshot insert affected {rows} rows"),
            )),
            TryInsertResult::Empty => Err(StorageError::invariant_violation(
                Some("decision_policy_snapshot"),
                "single snapshot insert unexpectedly had no input",
            )),
        }
    }
}

impl PgPolicyRepository {
    async fn validate_activation_evidence(
        db: &impl ConnectionTrait,
        activation: &NewPolicyActivation,
    ) -> Result<RevisionModel, StorageError> {
        let evidence = ApprovalEntity::find_by_id(activation.policy_approval_id)
            .join(JoinType::LeftJoin, ApprovalRelation::Activation.def())
            .filter(ActivationColumn::PolicyActivationId.is_null())
            .filter(ApprovalColumn::PolicyRevisionId.eq(activation.policy_revision_id))
            .filter(ApprovalColumn::ResourceKind.eq(activation.resource_kind))
            .filter(ApprovalColumn::Decision.eq(PolicyApprovalDecision::Approved))
            .filter(
                Condition::any()
                    .add(ApprovalColumn::ExpiresAt.is_null())
                    .add(
                        Expr::col((ApprovalEntity, ApprovalColumn::ExpiresAt))
                            .gt(Expr::current_timestamp()),
                    ),
            )
            .find_also_related(RevisionEntity)
            .filter(RevisionColumn::ResourceKind.eq(activation.resource_kind))
            .filter(RevisionColumn::Status.eq(PolicyRevisionStatus::Validated))
            .filter(RevisionColumn::PreflightTokenHash.eq(Some(activation.preflight_token_hash)))
            .filter(
                Expr::col((RevisionEntity, RevisionColumn::PreflightExpiresAt))
                    .gt(Expr::current_timestamp()),
            )
            .one(db)
            .await
            .map_err(StorageError::from)?;
        let Some((approval, Some(revision))) = evidence else {
            return Err(StorageError::state_conflict(
                "policy_activation",
                Some(&activation.policy_revision_id),
                "approval, typed validation, or preflight evidence is missing or expired",
            ));
        };
        if approval.policy_revision_id != revision.policy_revision_id
            || approval.resource_kind != revision.resource_kind
            || approval.revision_hash != revision.revision_hash
            || approval.decision != PolicyApprovalDecision::Approved
        {
            return Err(StorageError::state_conflict(
                "policy_approval",
                Some(&activation.policy_approval_id),
                "approval is not valid for the exact validated policy revision",
            ));
        }
        let revision_subject = revision
            .validation_evidence
            .as_ref()
            .and_then(|evidence| evidence.subject.as_ref());
        if approval.validation_subject.as_ref() != revision_subject {
            return Err(StorageError::state_conflict(
                "policy_approval",
                Some(&activation.policy_approval_id),
                "approval is bound to a different validation subject",
            ));
        }
        Ok(revision)
    }
}

impl PgPolicyRepository {
    async fn verify_resource_cas(
        db: &impl ConnectionTrait,
        activation: &NewPolicyActivation,
    ) -> Result<(), StorageError> {
        let current =
            Self::load_current_activation_from(db, Some(activation.resource_kind)).await?;
        let current_revision = current.as_ref().map(|row| &row.policy_revision_id);
        if current_revision != activation.expected_active_revision_id.as_ref() {
            return Err(StorageError::state_conflict(
                "policy_activation",
                activation.expected_active_revision_id.as_ref(),
                format!(
                    "active revision changed; current is {}",
                    current_revision.map_or_else(|| "<none>".to_owned(), ToString::to_string)
                ),
            ));
        }
        Ok(())
    }
}

fn verify_activation_subject(
    guard: &ActivationGuardModel,
    current_bundle: Option<&ActivePolicyBundle>,
    activation: &NewPolicyActivation,
    snapshot: &NewDecisionPolicySnapshot,
    revision: &RevisionModel,
    attaching_initial_ledger: bool,
) -> Result<(), StorageError> {
    let subject = revision
        .validation_evidence
        .as_ref()
        .and_then(|evidence| evidence.subject.as_ref())
        .ok_or_else(|| {
            StorageError::state_conflict(
                "policy_revision",
                Some(&revision.policy_revision_id),
                "typed validation is not bound to a policy bundle subject",
            )
        })?;
    let current_revision_vector = current_bundle
        .map(|bundle| &bundle.revision_vector)
        .cloned()
        .unwrap_or_default();
    let current_subject_matches = subject.base_generation == guard.generation
        && subject.base_generation == activation.expected_bundle_generation
        && subject.base_revision_vector == current_revision_vector;
    if !current_subject_matches && !attaching_initial_ledger {
        return Err(StorageError::state_conflict(
            "policy_activation_guard",
            Some(&activation.expected_bundle_generation),
            "validated bundle generation or revision vector is stale",
        ));
    }
    let computed_hash =
        CanonicalDigest::content_hash_json(&snapshot.snapshot).map_err(|error| {
            StorageError::invariant_violation(
                Some("decision_policy_snapshot"),
                format!("failed to hash typed decision snapshot: {error}"),
            )
        })?;
    if computed_hash != snapshot.snapshot_hash
        || DecisionPolicySnapshotId::from_content_hash(&computed_hash)
            != snapshot.decision_policy_snapshot_id
        || subject.candidate_bundle_hash != snapshot.snapshot_hash
    {
        return Err(StorageError::state_conflict(
            "decision_policy_snapshot",
            Some(&snapshot.decision_policy_snapshot_id),
            "candidate snapshot hash differs from validated evidence",
        ));
    }
    if snapshot
        .snapshot
        .resource_revision_id(activation.resource_kind)
        != Some(&revision.policy_revision_id)
        || snapshot
            .snapshot
            .resource_document(activation.resource_kind)
            != revision.document
    {
        return Err(StorageError::state_conflict(
            "policy_revision",
            Some(&revision.policy_revision_id),
            "candidate snapshot does not contain the exact validated resource revision",
        ));
    }
    let revisions = &snapshot.snapshot.revisions;
    if revisions.recommendation_policy.as_ref() != Some(&snapshot.recommendation_policy_revision_id)
        || revisions.execution_risk_policy.as_ref()
            != Some(&snapshot.execution_risk_policy_revision_id)
        || revisions.model_routing.as_ref() != Some(&snapshot.model_routing_revision_id)
        || revisions.report_schedule.as_ref() != Some(&snapshot.report_schedule_revision_id)
        || revisions.operational_control.as_ref() != Some(&snapshot.operational_control_revision_id)
        || revisions.execution_authorization.as_ref()
            != Some(&snapshot.execution_authorization_revision_id)
    {
        return Err(StorageError::invariant_violation(
            Some("decision_policy_snapshot"),
            "snapshot revision columns differ from the typed revision vector",
        ));
    }
    Ok(())
}

#[async_trait::async_trait]
impl PolicyRepository for PgPolicyRepository {
    async fn ensure_policy_profile_artifacts(
        &self,
        artifacts: &ImmutableProfileArtifacts,
        actor_label: &str,
        reason: &str,
    ) -> Result<ImmutableProfileArtifactReferences, StorageError> {
        if actor_label.trim().is_empty() || reason.trim().is_empty() {
            return Err(StorageError::invariant_violation(
                Some("policy_profile_artifact"),
                "profile artifact actor and reason must be non-empty",
            ));
        }
        let references = artifacts
            .references()
            .map_err(|error| profile_error(&error))?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        for document in artifacts.documents() {
            let kind = document.kind();
            let content_hash = document
                .content_hash()
                .map_err(|error| profile_error(&error))?;
            let profile_artifact_id =
                ProfileArtifactId::from_content_address(kind.as_str(), &content_hash);
            let insert = NewPolicyProfileArtifact {
                profile_artifact_id,
                kind,
                schema_version: document.schema_version(),
                document: document.clone(),
                content_hash,
                created_by_kind: PolicyActorKind::System,
                created_by_user_id: None,
                created_by_label: actor_label.to_owned(),
                reason: reason.to_owned(),
            };
            let outcome = ProfileArtifactEntity::insert(insert.into_active_model())
                .on_conflict_do_nothing_on([ProfileArtifactColumn::ProfileArtifactId])
                .exec_without_returning(&transaction)
                .await
                .map_err(StorageError::from)?;
            if !matches!(
                outcome,
                TryInsertResult::Inserted(1 | 0) | TryInsertResult::Conflicted
            ) {
                return Err(StorageError::invariant_violation(
                    Some("policy_profile_artifact"),
                    "single profile artifact insert returned an invalid row count",
                ));
            }
            let persisted = ProfileArtifactEntity::find_by_id(profile_artifact_id)
                .one(&transaction)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some("policy_profile_artifact"),
                        "profile artifact disappeared after insert",
                    )
                })?;
            verify_profile_artifact_row(&persisted)?;
            if persisted.kind != kind
                || persisted.schema_version != document.schema_version()
                || persisted.document != document
                || persisted.content_hash != content_hash
            {
                return Err(StorageError::state_conflict(
                    "policy_profile_artifact",
                    Some(&profile_artifact_id),
                    "content address already exists with different artifact content",
                ));
            }
        }
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(references)
    }

    async fn create_revision(
        &self,
        revision: NewPolicyRevision,
    ) -> Result<PolicyRevisionInfo, StorageError> {
        let key = format!("{}:{}", revision.resource_kind, revision.revision_hash);
        RevisionEntity::insert(revision.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map(Into::into)
            .map_err(|error| error::map_unique(error, "policy_revision", &key))
    }

    async fn mark_revision_validated(
        &self,
        revision_id: &PolicyRevisionId,
        validation_evidence: PolicyValidationEvidence,
        preflight_token_hash: ContentHash,
        preflight_expires_at: DateTime<Utc>,
    ) -> Result<PolicyRevisionInfo, StorageError> {
        let mut rows = RevisionEntity::update_many()
            .col_expr(
                RevisionColumn::Status,
                primitives::enum_value(&PolicyRevisionStatus::Validated),
            )
            .col_expr(
                RevisionColumn::ValidationEvidence,
                Expr::value(Some(validation_evidence)),
            )
            .col_expr(RevisionColumn::ValidatedAt, Expr::current_timestamp())
            .col_expr(
                RevisionColumn::PreflightTokenHash,
                Expr::value(Some(preflight_token_hash)),
            )
            .col_expr(
                RevisionColumn::PreflightExpiresAt,
                Expr::value(Some(preflight_expires_at)),
            )
            .filter(RevisionColumn::PolicyRevisionId.eq(*revision_id))
            .filter(
                Condition::any()
                    .add(RevisionColumn::Status.eq(PolicyRevisionStatus::Draft))
                    .add(RevisionColumn::Status.eq(PolicyRevisionStatus::Validated)),
            )
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        rows.pop().map(Into::into).ok_or_else(|| {
            StorageError::state_conflict(
                "policy_revision",
                Some(revision_id),
                "only a draft or previously validated revision can be validated",
            )
        })
    }

    async fn load_revision(
        &self,
        revision_id: &PolicyRevisionId,
    ) -> Result<Option<PolicyRevisionInfo>, StorageError> {
        RevisionEntity::find_by_id(*revision_id)
            .one(&self.db)
            .await
            .map(|row| row.map(Into::into))
            .map_err(StorageError::from)
    }

    async fn list_revisions(
        &self,
        kind: ConfigResourceKind,
        limit: u64,
    ) -> Result<Vec<PolicyRevisionInfo>, StorageError> {
        RevisionEntity::find()
            .filter(RevisionColumn::ResourceKind.eq(kind))
            .order_by_desc(RevisionColumn::CreatedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
            .map_err(StorageError::from)
    }

    async fn list_all_revisions(
        &self,
        limit: u64,
    ) -> Result<Vec<PolicyRevisionInfo>, StorageError> {
        RevisionEntity::find()
            .order_by_desc(RevisionColumn::CreatedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
            .map_err(StorageError::from)
    }

    async fn list_activity(&self, limit: u64) -> Result<Vec<ConfigActivityInfo>, StorageError> {
        Self::list(&self.db, limit).await
    }

    async fn load_resource_inventory(&self) -> Result<ConfigResourceInventoryInfo, StorageError> {
        Self::load(&self.db).await
    }

    async fn record_approval(
        &self,
        approval: RecordPolicyApproval,
    ) -> Result<PolicyApprovalInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let mut query = RevisionEntity::find_by_id(approval.policy_revision_id)
            .filter(RevisionColumn::ResourceKind.eq(approval.resource_kind));
        if approval.decision == PolicyApprovalDecision::Approved {
            query = query
                .filter(RevisionColumn::Status.eq(PolicyRevisionStatus::Validated))
                .filter(
                    Expr::col((RevisionEntity, RevisionColumn::PreflightExpiresAt))
                        .gt(Expr::current_timestamp()),
                );
        }
        let revision = query
            .lock_shared()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::state_conflict(
                    "policy_revision",
                    Some(&approval.policy_revision_id),
                    "revision is missing, belongs to another resource, or lacks current validation evidence",
                )
            })?;
        let validation_subject = revision
            .validation_evidence
            .as_ref()
            .and_then(|evidence| evidence.subject.clone());
        if approval.decision == PolicyApprovalDecision::Approved && validation_subject.is_none() {
            return Err(StorageError::state_conflict(
                "policy_revision",
                Some(&approval.policy_revision_id),
                "approved decision requires a complete validation subject",
            ));
        }
        let inserted = ApprovalEntity::insert(
            NewPolicyApproval {
                policy_approval_id: approval.policy_approval_id,
                policy_revision_id: approval.policy_revision_id,
                resource_kind: approval.resource_kind,
                revision_hash: revision.revision_hash,
                validation_subject,
                decision: approval.decision,
                decided_by_kind: approval.decided_by_kind,
                decided_by_user_id: approval.decided_by_user_id,
                decided_by_label: approval.decided_by_label,
                reason: approval.reason,
                decided_at: approval.decided_at,
                expires_at: approval.expires_at,
            }
            .into_active_model(),
        )
        .exec_with_returning(&transaction)
        .await
        .map(Into::into)
        .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(inserted)
    }

    async fn load_approval(
        &self,
        approval_id: &PolicyApprovalId,
    ) -> Result<Option<PolicyApprovalInfo>, StorageError> {
        ApprovalEntity::find_by_id(*approval_id)
            .one(&self.db)
            .await
            .map(|row| row.map(Into::into))
            .map_err(StorageError::from)
    }

    async fn list_valid_approvals(
        &self,
        kind: Option<ConfigResourceKind>,
        limit: u64,
    ) -> Result<Vec<PolicyApprovalInfo>, StorageError> {
        let active_generation = Query::select()
            .column(ActivationGuardColumn::Generation)
            .from(ActivationGuardEntity)
            .and_where(Expr::col(ActivationGuardColumn::Id).eq(POLICY_ACTIVATION_GUARD_ID))
            .to_owned();
        let mut query = ApprovalEntity::find()
            .join(JoinType::LeftJoin, ApprovalRelation::Activation.def())
            .join(JoinType::InnerJoin, ApprovalRelation::Revision.def())
            .filter(ActivationColumn::PolicyActivationId.is_null())
            .filter(ApprovalColumn::Decision.eq(PolicyApprovalDecision::Approved))
            .filter(RevisionColumn::Status.eq(PolicyRevisionStatus::Validated))
            .filter(
                Expr::col((RevisionEntity, RevisionColumn::ResourceKind))
                    .equals((ApprovalEntity, ApprovalColumn::ResourceKind)),
            )
            .filter(
                Expr::col((RevisionEntity, RevisionColumn::RevisionHash))
                    .equals((ApprovalEntity, ApprovalColumn::RevisionHash)),
            )
            .filter(
                Expr::col((ApprovalEntity, ApprovalColumn::ValidationSubject)).eq(Expr::col((
                    RevisionEntity,
                    RevisionColumn::ValidationEvidence,
                ))
                .get_json_field("subject")),
            )
            .filter(
                Expr::col((RevisionEntity, RevisionColumn::PreflightExpiresAt))
                    .gt(Expr::current_timestamp()),
            )
            .filter(config_resources::approved_base_generation().eq(active_generation))
            .filter(
                Condition::any()
                    .add(ApprovalColumn::ExpiresAt.is_null())
                    .add(
                        Expr::col((ApprovalEntity, ApprovalColumn::ExpiresAt))
                            .gt(Expr::current_timestamp()),
                    ),
            );
        if let Some(kind) = kind {
            query = query.filter(ApprovalColumn::ResourceKind.eq(kind));
        }
        query
            .order_by_desc(ApprovalColumn::DecidedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
            .map_err(StorageError::from)
    }

    async fn list_approvals(
        &self,
        kind: Option<ConfigResourceKind>,
        limit: u64,
    ) -> Result<Vec<PolicyApprovalInfo>, StorageError> {
        let mut query = ApprovalEntity::find();
        if let Some(kind) = kind {
            query = query.filter(ApprovalColumn::ResourceKind.eq(kind));
        }
        query
            .order_by_desc(ApprovalColumn::DecidedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
            .map_err(StorageError::from)
    }

    async fn activate_resource(
        &self,
        mut activation: NewPolicyActivation,
        snapshot: NewDecisionPolicySnapshot,
    ) -> Result<PolicyActivationCommit, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let guard = Self::acquire_activation_lock(&transaction).await?;
        if let Some((existing, bundle)) =
            Self::resolve_idempotent_activation(&transaction, &activation).await?
        {
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(PolicyActivationCommit {
                activation: existing,
                bundle,
                outcome: PolicyActivationOutcome::ExactReplay,
            });
        }
        if guard.generation != activation.expected_bundle_generation {
            return Err(StorageError::state_conflict(
                "policy_activation_guard",
                Some(&activation.expected_bundle_generation),
                format!("active bundle generation changed to {}", guard.generation),
            ));
        }
        let current_bundle = Self::load_current_bundle_from(&transaction).await?;
        let attaching_initial_ledger = activation.activation_kind == PolicyActivationKind::Initial
            && current_bundle.as_ref().is_some_and(|bundle| {
                let bundle_generation = bundle.generation;
                bundle.decision_policy_snapshot_id == snapshot.decision_policy_snapshot_id
                    && bundle.snapshot_hash == snapshot.snapshot_hash
                    && bundle_generation == activation.bundle_generation
            })
            && Self::load_current_activation_from(&transaction, Some(activation.resource_kind))
                .await?
                .is_none();
        let next_generation = guard.generation.checked_next().map_err(|error| {
            StorageError::invariant_violation(Some("policy_activation_guard"), error.to_string())
        })?;
        let committed_generation = if attaching_initial_ledger {
            guard.generation
        } else {
            next_generation
        };
        if activation.bundle_generation != committed_generation
            || activation.decision_policy_snapshot_id != snapshot.decision_policy_snapshot_id
        {
            return Err(StorageError::invariant_violation(
                Some("policy_activation"),
                "activation must bind the exact next bundle generation and snapshot id",
            ));
        }
        let revision = Self::validate_activation_evidence(&transaction, &activation).await?;
        verify_activation_subject(
            &guard,
            current_bundle.as_ref(),
            &activation,
            &snapshot,
            &revision,
            attaching_initial_ledger,
        )?;
        Self::verify_resource_cas(&transaction, &activation).await?;
        activation.previous_policy_revision_id = activation.expected_active_revision_id;
        if activation.policy_revision_id != revision.policy_revision_id {
            return Err(StorageError::invariant_violation(
                Some("policy_activation"),
                "validated revision changed during activation",
            ));
        }
        Self::insert_snapshot_if_absent(&transaction, snapshot.clone()).await?;
        let inserted = ActivationEntity::insert(activation.into_active_model())
            .exec_with_returning(&transaction)
            .await
            .map_err(StorageError::from)?;
        Self::insert_activation_ledger(&transaction, &inserted, &snapshot).await?;
        if !attaching_initial_ledger {
            let mut guard_update = guard.into_active_model();
            guard_update.generation = Set(next_generation);
            guard_update.current_snapshot_id = Set(Some(snapshot.decision_policy_snapshot_id));
            guard_update.current_snapshot_hash = Set(Some(snapshot.snapshot_hash));
            guard_update.updated_at = Set(Utc::now());
            guard_update
                .update(&transaction)
                .await
                .map_err(StorageError::from)?;
        }
        let snapshot_model = SnapshotEntity::find_by_id(snapshot.decision_policy_snapshot_id)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("decision_policy_snapshot"),
                    "committed snapshot disappeared before bundle resolution",
                )
            })?;
        let committed_info = Self::resolve_snapshot_model(&transaction, snapshot_model).await?;
        let bundle = ActivePolicyBundle::from_parts(
            inserted.bundle_generation,
            committed_info.decision_policy_snapshot_id,
            committed_info.snapshot_hash,
            committed_info.snapshot,
        );
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(PolicyActivationCommit {
            activation: inserted.into(),
            bundle,
            outcome: PolicyActivationOutcome::Committed,
        })
    }

    async fn load_current_bundle(&self) -> Result<Option<ActivePolicyBundle>, StorageError> {
        Self::load_current_bundle_from(&self.db).await
    }

    async fn load_current_activation(
        &self,
        kind: Option<ConfigResourceKind>,
    ) -> Result<Option<PolicyActivationInfo>, StorageError> {
        Self::load_current_activation_from(&self.db, kind).await
    }

    async fn load_current_activations(&self) -> Result<Vec<PolicyActivationInfo>, StorageError> {
        ActivationEntity::find()
            .distinct_on([(ActivationEntity, ActivationColumn::ResourceKind)])
            .order_by_asc(Expr::col((
                ActivationEntity,
                ActivationColumn::ResourceKind,
            )))
            .order_by_desc(ActivationColumn::ActivatedAt)
            .order_by_desc(ActivationColumn::PolicyActivationId)
            .all(&self.db)
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
            .map_err(StorageError::from)
    }

    async fn count_valid_approvals(
        &self,
    ) -> Result<BTreeMap<ConfigResourceKind, u64>, StorageError> {
        Ok(Self::load(&self.db)
            .await?
            .resources
            .into_iter()
            .filter_map(|resource| {
                (resource.pending_approval_count > 0)
                    .then_some((resource.resource_kind, resource.pending_approval_count))
            })
            .collect())
    }

    async fn load_current_resource(
        &self,
        kind: ConfigResourceKind,
    ) -> Result<Option<ActivePolicyResourceInfo>, StorageError> {
        let row = ActivationEntity::find()
            .filter(ActivationColumn::ResourceKind.eq(kind))
            .order_by_desc(ActivationColumn::ActivatedAt)
            .order_by_desc(ActivationColumn::PolicyActivationId)
            .find_also_related(RevisionEntity)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        match row {
            None => Ok(None),
            Some((activation, Some(revision))) => Ok(Some(ActivePolicyResourceInfo {
                activation: activation.into(),
                revision: revision.into(),
            })),
            Some((activation, None)) => Err(StorageError::invariant_violation(
                Some("policy_activation"),
                format!(
                    "activation {} references missing policy revision {}",
                    activation.policy_activation_id, activation.policy_revision_id
                ),
            )),
        }
    }

    async fn load_current_revision(
        &self,
        kind: ConfigResourceKind,
    ) -> Result<Option<PolicyRevisionInfo>, StorageError> {
        let row = ActivationEntity::find()
            .filter(ActivationColumn::ResourceKind.eq(kind))
            .order_by_desc(ActivationColumn::ActivatedAt)
            .order_by_desc(ActivationColumn::PolicyActivationId)
            .find_also_related(RevisionEntity)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        match row {
            None => Ok(None),
            Some((_activation, Some(revision))) => Ok(Some(revision.into())),
            Some((activation, None)) => Err(StorageError::invariant_violation(
                Some("policy_activation"),
                format!(
                    "activation {} references missing policy revision {}",
                    activation.policy_activation_id, activation.policy_revision_id
                ),
            )),
        }
    }

    async fn load_snapshot(
        &self,
        snapshot_id: &DecisionPolicySnapshotId,
    ) -> Result<Option<DecisionPolicySnapshotInfo>, StorageError> {
        let model = SnapshotEntity::find_by_id(*snapshot_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        match model {
            Some(model) => Self::resolve_snapshot_model(&self.db, model)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    async fn load_current(&self) -> Result<Option<DecisionPolicySnapshotInfo>, StorageError> {
        Self::load_current_from(&self.db).await
    }

    async fn load_active_at(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Option<DecisionPolicySnapshotInfo>, StorageError> {
        let row = ActivationEntity::find()
            .filter(ActivationColumn::ActivatedAt.lte(at))
            .order_by_desc(ActivationColumn::ActivatedAt)
            .order_by_desc(ActivationColumn::PolicyActivationId)
            .find_also_related(SnapshotEntity)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        match row {
            None => Ok(None),
            Some((activation, None)) => Err(StorageError::invariant_violation(
                Some("policy_activation"),
                format!(
                    "activation {} references a missing decision snapshot",
                    activation.policy_activation_id
                ),
            )),
            Some((_activation, Some(snapshot))) => Self::resolve_snapshot_model(&self.db, snapshot)
                .await
                .map(Some),
        }
    }

    async fn list_snapshots(
        &self,
        limit: u64,
    ) -> Result<Vec<DecisionPolicySnapshotInfo>, StorageError> {
        let models = SnapshotEntity::find()
            .order_by_desc(SnapshotColumn::CreatedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Self::resolve_snapshot_models(&self.db, models).await
    }

    async fn list_snapshot_options(
        &self,
        limit: u64,
    ) -> Result<Vec<DecisionPolicySnapshotOptionInfo>, StorageError> {
        SnapshotEntity::find()
            .order_by_desc(SnapshotColumn::CreatedAt)
            .limit(limit)
            .into_partial_model::<DecisionPolicySnapshotOptionInfo>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)
    }

    async fn list_activations(
        &self,
        kind: Option<ConfigResourceKind>,
        limit: u64,
    ) -> Result<Vec<PolicyActivationInfo>, StorageError> {
        let mut query = ActivationEntity::find();
        if let Some(kind) = kind {
            query = query.filter(ActivationColumn::ResourceKind.eq(kind));
        }
        query
            .order_by_desc(ActivationColumn::ActivatedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
            .map_err(StorageError::from)
    }
}

#[cfg(test)]
mod query_budget_tests {
    use std::collections::BTreeMap;

    use quant_pivot_error::storage::StorageError;
    use sea_orm::{DbBackend, MockDatabase, Value};

    use super::{PgPolicyRepository, PolicyRepository};

    #[tokio::test]
    async fn snapshot_options_executes_projection() -> Result<(), StorageError> {
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([Vec::<BTreeMap<String, Value>>::new()])
            .into_connection();
        let repository = PgPolicyRepository::new(db.clone());

        let options = repository.list_snapshot_options(50).await?;

        assert!(options.is_empty());
        assert_eq!(db.into_transaction_log().len(), 1);
        Ok(())
    }
}
