use std::collections::BTreeMap;

use crate::{
    postgres::{error, primitives},
    traits::PolicyRepository,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        ActivePolicyResourceInfo, DecisionPolicySnapshotInfo, NewDecisionPolicySnapshot,
        NewPolicyActivation, NewPolicyApproval, NewPolicyRevision, NewProductionBaseline,
        PolicyActivationInfo, PolicyApprovalInfo, PolicyRevisionInfo, ProductionBaselineInfo,
        RecordPolicyApproval,
    },
    entities::{
        decision_policy_snapshot::{Column as SnapshotColumn, Entity as SnapshotEntity},
        policy_activation::{
            Column as ActivationColumn, Entity as ActivationEntity, Relation as ActivationRelation,
        },
        policy_activation_guard::Entity as ActivationGuardEntity,
        policy_approval::{
            self, Column as ApprovalColumn, Entity as ApprovalEntity, Relation as ApprovalRelation,
        },
        policy_revision::{
            Column as RevisionColumn, Entity as RevisionEntity, Model as RevisionModel,
        },
        system_production_baseline::{
            Column as ProductionBaselineColumn, Entity as ProductionBaselineEntity,
        },
    },
    enums::runtime_config::{ConfigResourceKind, PolicyApprovalDecision, PolicyRevisionStatus},
    runtime_config::PolicyValidationEvidence,
    types::{ContentHash, DecisionPolicySnapshotId, PolicyApprovalId, PolicyRevisionId},
};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, EntityTrait, ExprTrait,
    FromQueryResult, IntoActiveModel, JoinType, QueryFilter, QueryOrder, QuerySelect,
    RelationTrait, TransactionTrait, TryInsertResult, sea_query::Expr,
};

const POLICY_ACTIVATION_GUARD_ID: i16 = 1;

#[derive(Debug, FromQueryResult)]
struct ApprovalCountRow {
    resource_kind: ConfigResourceKind,
    approval_count: i64,
}

pub struct PgPolicyRepository {
    db: DatabaseConnection,
}

impl PgPolicyRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

pub(crate) async fn acquire_activation_lock(
    transaction: &sea_orm::DatabaseTransaction,
) -> Result<(), StorageError> {
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
        })?;
    Ok(())
}

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

pub(crate) async fn do_load_current(
    db: &impl ConnectionTrait,
) -> Result<Option<DecisionPolicySnapshotInfo>, StorageError> {
    let row = ActivationEntity::find()
        .order_by_desc(ActivationColumn::ActivatedAt)
        .order_by_desc(ActivationColumn::PolicyActivationId)
        .find_also_related(SnapshotEntity)
        .one(db)
        .await
        .map_err(StorageError::from)?;
    match row {
        None => Ok(None),
        Some((_activation, Some(snapshot))) => Ok(Some(snapshot.into())),
        Some((activation, None)) => Err(StorageError::invariant_violation(
            Some("policy_activation"),
            format!(
                "activation {} references missing decision snapshot {}",
                activation.policy_activation_id, activation.decision_policy_snapshot_id
            ),
        )),
    }
}

async fn insert_snapshot_if_absent(
    db: &impl ConnectionTrait,
    snapshot: NewDecisionPolicySnapshot,
) -> Result<(), StorageError> {
    let snapshot_id = snapshot.decision_policy_snapshot_id.clone();
    let expected_hash = snapshot.snapshot_hash.clone();
    let expected_snapshot = snapshot.snapshot.clone();
    let outcome = SnapshotEntity::insert(snapshot.into_active_model())
        .on_conflict_do_nothing_on([SnapshotColumn::DecisionPolicySnapshotId])
        .exec_without_returning(db)
        .await
        .map_err(StorageError::from)?;
    match outcome {
        TryInsertResult::Inserted(1) => Ok(()),
        TryInsertResult::Inserted(0) | TryInsertResult::Conflicted => {
            let existing = SnapshotEntity::find_by_id(snapshot_id.clone())
                .one(db)
                .await
                .map_err(StorageError::from)?
                .ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some("decision_policy_snapshot"),
                        "conflicting snapshot disappeared before verification",
                    )
                })?;
            if existing.snapshot_hash != expected_hash || existing.snapshot != expected_snapshot {
                return Err(StorageError::state_conflict(
                    "decision_policy_snapshot",
                    Some(&snapshot_id),
                    "snapshot id already exists with different content",
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

async fn validate_activation_evidence(
    db: &impl ConnectionTrait,
    activation: &NewPolicyActivation,
) -> Result<RevisionModel, StorageError> {
    let evidence = ApprovalEntity::find_by_id(activation.policy_approval_id.clone())
        .join(JoinType::LeftJoin, ApprovalRelation::Activation.def())
        .filter(ActivationColumn::PolicyActivationId.is_null())
        .filter(ApprovalColumn::PolicyRevisionId.eq(activation.policy_revision_id.clone()))
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
        .filter(
            RevisionColumn::PreflightTokenHash.eq(Some(activation.preflight_token_hash.clone())),
        )
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
    Ok(revision)
}

async fn verify_resource_cas(
    db: &impl ConnectionTrait,
    activation: &NewPolicyActivation,
) -> Result<(), StorageError> {
    let current = load_current_activation_from(db, Some(activation.resource_kind)).await?;
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

#[async_trait::async_trait]
impl PolicyRepository for PgPolicyRepository {
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
            .filter(RevisionColumn::PolicyRevisionId.eq(revision_id.clone()))
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
        RevisionEntity::find_by_id(revision_id.clone())
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

    async fn record_approval(
        &self,
        approval: RecordPolicyApproval,
    ) -> Result<PolicyApprovalInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let mut query = RevisionEntity::find_by_id(approval.policy_revision_id.clone())
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
        let inserted = ApprovalEntity::insert(
            NewPolicyApproval {
                policy_approval_id: approval.policy_approval_id,
                policy_revision_id: approval.policy_revision_id,
                resource_kind: approval.resource_kind,
                revision_hash: revision.revision_hash,
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
        ApprovalEntity::find_by_id(approval_id.clone())
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
        let mut query = ApprovalEntity::find()
            .join(JoinType::LeftJoin, ApprovalRelation::Activation.def())
            .filter(ActivationColumn::PolicyActivationId.is_null())
            .filter(policy_approval::Column::Decision.eq(PolicyApprovalDecision::Approved))
            .filter(
                Condition::any()
                    .add(policy_approval::Column::ExpiresAt.is_null())
                    .add(
                        Expr::col((ApprovalEntity, ApprovalColumn::ExpiresAt))
                            .gt(Expr::current_timestamp()),
                    ),
            );
        if let Some(kind) = kind {
            query = query.filter(policy_approval::Column::ResourceKind.eq(kind));
        }
        query
            .order_by_desc(policy_approval::Column::DecidedAt)
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
    ) -> Result<PolicyActivationInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        acquire_activation_lock(&transaction).await?;
        if let Some(existing) = ActivationEntity::find()
            .filter(ActivationColumn::IdempotencyKey.eq(&activation.idempotency_key))
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
        {
            if existing.resource_kind == activation.resource_kind
                && existing.policy_revision_id == activation.policy_revision_id
                && existing.policy_approval_id == activation.policy_approval_id
            {
                transaction.commit().await.map_err(StorageError::from)?;
                return Ok(existing.into());
            }
            return Err(StorageError::state_conflict(
                "policy_activation",
                Some(&activation.idempotency_key),
                "idempotency key is already bound to a different activation",
            ));
        }
        let revision = validate_activation_evidence(&transaction, &activation).await?;
        verify_resource_cas(&transaction, &activation).await?;
        activation.previous_policy_revision_id = activation.expected_active_revision_id.clone();
        if activation.policy_revision_id != revision.policy_revision_id {
            return Err(StorageError::invariant_violation(
                Some("policy_activation"),
                "validated revision changed during activation",
            ));
        }
        insert_snapshot_if_absent(&transaction, snapshot).await?;
        let inserted = ActivationEntity::insert(activation.into_active_model())
            .exec_with_returning(&transaction)
            .await
            .map(Into::into)
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(inserted)
    }

    async fn load_current_activation(
        &self,
        kind: Option<ConfigResourceKind>,
    ) -> Result<Option<PolicyActivationInfo>, StorageError> {
        load_current_activation_from(&self.db, kind).await
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
        let rows = ApprovalEntity::find()
            .join_rev(JoinType::LeftJoin, ActivationRelation::Approval.def())
            .select_only()
            .column(ApprovalColumn::ResourceKind)
            .column_as(ApprovalColumn::PolicyApprovalId.count(), "approval_count")
            .filter(ActivationColumn::PolicyActivationId.is_null())
            .filter(ApprovalColumn::Decision.eq(PolicyApprovalDecision::Approved))
            .filter(
                Condition::any()
                    .add(ApprovalColumn::ExpiresAt.is_null())
                    .add(
                        Expr::col((ApprovalEntity, ApprovalColumn::ExpiresAt))
                            .gt(Expr::current_timestamp()),
                    ),
            )
            .group_by(ApprovalColumn::ResourceKind)
            .into_model::<ApprovalCountRow>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        rows.into_iter()
            .map(|row| {
                u64::try_from(row.approval_count)
                    .map(|count| (row.resource_kind, count))
                    .map_err(|error| {
                        StorageError::invariant_violation(
                            Some("policy_approval"),
                            format!("negative approval count: {error}"),
                        )
                    })
            })
            .collect()
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
        SnapshotEntity::find_by_id(snapshot_id.clone())
            .one(&self.db)
            .await
            .map(|row| row.map(Into::into))
            .map_err(StorageError::from)
    }

    async fn load_by_hash(
        &self,
        snapshot_hash: &ContentHash,
    ) -> Result<Option<DecisionPolicySnapshotInfo>, StorageError> {
        SnapshotEntity::find()
            .filter(SnapshotColumn::SnapshotHash.eq(snapshot_hash))
            .one(&self.db)
            .await
            .map(|row| row.map(Into::into))
            .map_err(StorageError::from)
    }

    async fn load_current(&self) -> Result<Option<DecisionPolicySnapshotInfo>, StorageError> {
        do_load_current(&self.db).await
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
        row.map_or(Ok(None), |(activation, snapshot)| {
            snapshot.map(Into::into).map(Some).ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("policy_activation"),
                    format!(
                        "activation {} references a missing decision snapshot",
                        activation.policy_activation_id
                    ),
                )
            })
        })
    }

    async fn list_snapshots(
        &self,
        limit: u64,
    ) -> Result<Vec<DecisionPolicySnapshotInfo>, StorageError> {
        SnapshotEntity::find()
            .order_by_desc(SnapshotColumn::CreatedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
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

    async fn load_production_baseline(
        &self,
    ) -> Result<Option<ProductionBaselineInfo>, StorageError> {
        ProductionBaselineEntity::find()
            .order_by_desc(ProductionBaselineColumn::SealedAt)
            .one(&self.db)
            .await
            .map(|row| row.map(Into::into))
            .map_err(StorageError::from)
    }

    async fn seal_production_baseline(
        &self,
        baseline: NewProductionBaseline,
    ) -> Result<ProductionBaselineInfo, StorageError> {
        let key = baseline.production_baseline_id.to_string();
        ProductionBaselineEntity::insert(baseline.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map(Into::into)
            .map_err(|error| error::map_unique(error, "system_production_baseline", &key))
    }
}
