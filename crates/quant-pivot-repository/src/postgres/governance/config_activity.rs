//! Single-statement projection of the Config governance activity ledger.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::governance::{
        ConfigActivityInfo, PolicyActivationInfo, PolicyApprovalInfo, PolicyRevisionInfo,
    },
    entities::{
        policy_activation::{Column as PolicyActivationColumn, Entity as PolicyActivationEntity},
        policy_approval::{Column as PolicyApprovalColumn, Entity as PolicyApprovalEntity},
        policy_revision::{Column, Entity},
    },
    enums::runtime_config::{
        ConfigResourceKind, PolicyActivationKind, PolicyActorKind, PolicyApprovalDecision,
        PolicyRevisionStatus,
    },
    runtime_config::{PolicyDocument, PolicyValidationEvidence, PolicyValidationSubject},
    types::{
        AuditEventId, ContentHash, DecisionPolicySnapshotId, PolicyActivationId, PolicyApprovalId,
        PolicyBundleGeneration, PolicyIdempotencyKey, PolicyRevisionId, SchemaVersion, UserId,
    },
};
use sea_orm::{
    ConnectionTrait, FromQueryResult, Order,
    sea_query::{Expr, Iden, Query, SelectStatement, UnionType},
};

use crate::postgres::{governance::runtime_config::PgPolicyRepository, primitives::enum_null};

#[derive(Debug, Clone, Copy)]
enum ActivityRecordKind {
    Revision,
    Approval,
    Activation,
}

impl ActivityRecordKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Revision => "revision",
            Self::Approval => "approval",
            Self::Activation => "activation",
        }
    }

    const fn lifecycle_rank(self) -> i32 {
        match self {
            Self::Revision => 1,
            Self::Approval => 2,
            Self::Activation => 3,
        }
    }

    fn rank_expr(self) -> Expr {
        Expr::value(self.lifecycle_rank())
    }

    fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "revision" => Ok(Self::Revision),
            "approval" => Ok(Self::Approval),
            "activation" => Ok(Self::Activation),
            _ => Err(StorageError::invariant_violation(
                Some("config_activity"),
                format!("unknown activity record kind `{value}`"),
            )),
        }
    }
}

#[derive(Iden)]
enum ActivityColumn {
    EventType,
    EventAt,
    EventRank,
    SortId,
    PolicyRevisionId,
    ResourceKind,
    RevisionHash,
    Reason,
    ActorKind,
    ActorUserId,
    ActorLabel,
    CreatedAt,
    SchemaVersion,
    Document,
    RevisionStatus,
    ValidationEvidence,
    ValidatedAt,
    PreflightTokenHash,
    PreflightExpiresAt,
    PolicyApprovalId,
    ApprovalValidationSubject,
    ApprovalDecision,
    DecidedAt,
    ApprovalExpiresAt,
    BundleGeneration,
    ExpectedBundleGeneration,
    PolicyActivationId,
    DecisionPolicySnapshotId,
    ActivatedAt,
    ActivationKind,
    ExpectedActiveRevisionId,
    PreviousPolicyRevisionId,
    RollbackTargetRevisionId,
    IdempotencyKey,
    ActivationRequestHash,
    AuditEventId,
}

#[derive(Debug, FromQueryResult)]
struct ActivityRow {
    event_type: String,
    policy_revision_id: PolicyRevisionId,
    resource_kind: ConfigResourceKind,
    revision_hash: Option<ContentHash>,
    reason: String,
    actor_kind: PolicyActorKind,
    actor_user_id: Option<UserId>,
    actor_label: String,
    created_at: DateTime<Utc>,
    schema_version: Option<SchemaVersion>,
    document: Option<PolicyDocument>,
    revision_status: Option<PolicyRevisionStatus>,
    validation_evidence: Option<PolicyValidationEvidence>,
    validated_at: Option<DateTime<Utc>>,
    preflight_token_hash: Option<ContentHash>,
    preflight_expires_at: Option<DateTime<Utc>>,
    policy_approval_id: Option<PolicyApprovalId>,
    approval_validation_subject: Option<PolicyValidationSubject>,
    approval_decision: Option<PolicyApprovalDecision>,
    decided_at: Option<DateTime<Utc>>,
    approval_expires_at: Option<DateTime<Utc>>,
    bundle_generation: Option<PolicyBundleGeneration>,
    expected_bundle_generation: Option<PolicyBundleGeneration>,
    policy_activation_id: Option<PolicyActivationId>,
    decision_policy_snapshot_id: Option<DecisionPolicySnapshotId>,
    activated_at: Option<DateTime<Utc>>,
    activation_kind: Option<PolicyActivationKind>,
    expected_active_revision_id: Option<PolicyRevisionId>,
    previous_policy_revision_id: Option<PolicyRevisionId>,
    rollback_target_revision_id: Option<PolicyRevisionId>,
    idempotency_key: Option<PolicyIdempotencyKey>,
    activation_request_hash: Option<ContentHash>,
    audit_event_id: Option<AuditEventId>,
}

impl TryFrom<ActivityRow> for ConfigActivityInfo {
    type Error = StorageError;

    fn try_from(row: ActivityRow) -> Result<Self, Self::Error> {
        let kind = ActivityRecordKind::parse(&row.event_type)?;
        match kind {
            ActivityRecordKind::Revision => Ok(Self::Revision(Box::new(PolicyRevisionInfo {
                policy_revision_id: row.policy_revision_id,
                resource_kind: row.resource_kind,
                schema_version: required(row.schema_version, kind, "schema_version")?,
                revision_hash: required(row.revision_hash, kind, "revision_hash")?,
                document: required(row.document, kind, "document")?,
                status: required(row.revision_status, kind, "revision_status")?,
                validation_evidence: row.validation_evidence,
                validated_at: row.validated_at,
                preflight_token_hash: row.preflight_token_hash,
                preflight_expires_at: row.preflight_expires_at,
                created_by_kind: row.actor_kind,
                created_by_user_id: row.actor_user_id,
                created_by_label: row.actor_label,
                reason: row.reason,
                created_at: row.created_at,
            }))),
            ActivityRecordKind::Approval => Ok(Self::Approval(PolicyApprovalInfo {
                policy_approval_id: required(row.policy_approval_id, kind, "policy_approval_id")?,
                policy_revision_id: row.policy_revision_id,
                resource_kind: row.resource_kind,
                revision_hash: required(row.revision_hash, kind, "revision_hash")?,
                validation_subject: row.approval_validation_subject,
                decision: required(row.approval_decision, kind, "approval_decision")?,
                decided_by_kind: row.actor_kind,
                decided_by_user_id: row.actor_user_id,
                decided_by_label: row.actor_label,
                reason: row.reason,
                decided_at: required(row.decided_at, kind, "decided_at")?,
                expires_at: row.approval_expires_at,
                created_at: row.created_at,
            })),
            ActivityRecordKind::Activation => Ok(Self::Activation(PolicyActivationInfo {
                bundle_generation: required(row.bundle_generation, kind, "bundle_generation")?,
                expected_bundle_generation: required(
                    row.expected_bundle_generation,
                    kind,
                    "expected_bundle_generation",
                )?,
                policy_activation_id: required(
                    row.policy_activation_id,
                    kind,
                    "policy_activation_id",
                )?,
                resource_kind: row.resource_kind,
                policy_revision_id: row.policy_revision_id,
                decision_policy_snapshot_id: required(
                    row.decision_policy_snapshot_id,
                    kind,
                    "decision_policy_snapshot_id",
                )?,
                policy_approval_id: required(row.policy_approval_id, kind, "policy_approval_id")?,
                activated_at: required(row.activated_at, kind, "activated_at")?,
                activated_by_kind: row.actor_kind,
                activated_by_user_id: row.actor_user_id,
                activated_by_label: row.actor_label,
                reason: row.reason,
                activation_kind: required(row.activation_kind, kind, "activation_kind")?,
                expected_active_revision_id: row.expected_active_revision_id,
                previous_policy_revision_id: row.previous_policy_revision_id,
                rollback_target_revision_id: row.rollback_target_revision_id,
                preflight_token_hash: required(
                    row.preflight_token_hash,
                    kind,
                    "preflight_token_hash",
                )?,
                idempotency_key: required(row.idempotency_key, kind, "idempotency_key")?,
                activation_request_hash: required(
                    row.activation_request_hash,
                    kind,
                    "activation_request_hash",
                )?,
                audit_event_id: required(row.audit_event_id, kind, "audit_event_id")?,
                created_at: row.created_at,
            })),
        }
    }
}

fn required<T>(
    value: Option<T>,
    kind: ActivityRecordKind,
    field: &'static str,
) -> Result<T, StorageError> {
    value.ok_or_else(|| {
        StorageError::invariant_violation(
            Some("config_activity"),
            format!("{} activity row is missing `{field}`", kind.as_str()),
        )
    })
}

fn revision_query_head() -> SelectStatement {
    Query::select()
        .expr_as(
            Expr::value(ActivityRecordKind::Revision.as_str()),
            ActivityColumn::EventType,
        )
        .expr_as(
            Expr::col((Entity, Column::CreatedAt)),
            ActivityColumn::EventAt,
        )
        .expr_as(
            ActivityRecordKind::Revision.rank_expr(),
            ActivityColumn::EventRank,
        )
        .expr_as(
            Expr::col((Entity, Column::PolicyRevisionId)),
            ActivityColumn::SortId,
        )
        .expr_as(
            Expr::col((Entity, Column::PolicyRevisionId)),
            ActivityColumn::PolicyRevisionId,
        )
        .expr_as(
            Expr::col((Entity, Column::ResourceKind)),
            ActivityColumn::ResourceKind,
        )
        .expr_as(
            Expr::col((Entity, Column::RevisionHash)),
            ActivityColumn::RevisionHash,
        )
        .expr_as(Expr::col((Entity, Column::Reason)), ActivityColumn::Reason)
        .expr_as(
            Expr::col((Entity, Column::CreatedByKind)),
            ActivityColumn::ActorKind,
        )
        .expr_as(
            Expr::col((Entity, Column::CreatedByUserId)),
            ActivityColumn::ActorUserId,
        )
        .expr_as(
            Expr::col((Entity, Column::CreatedByLabel)),
            ActivityColumn::ActorLabel,
        )
        .expr_as(
            Expr::col((Entity, Column::CreatedAt)),
            ActivityColumn::CreatedAt,
        )
        .to_owned()
}

fn revision_query() -> SelectStatement {
    let null_generation = || Expr::value(Option::<PolicyBundleGeneration>::None);
    let null_activation_id = || Expr::value(Option::<PolicyActivationId>::None);
    let null_snapshot_id = || Expr::value(Option::<DecisionPolicySnapshotId>::None);
    let null_timestamp = || Expr::value(Option::<DateTime<Utc>>::None);
    let null_revision_id = || Expr::value(Option::<PolicyRevisionId>::None);
    let null_approval_id = || Expr::value(Option::<PolicyApprovalId>::None);
    let null_audit_id = || Expr::value(Option::<AuditEventId>::None);
    let null_content_hash = || Expr::value(Option::<ContentHash>::None);
    let null_idempotency_key = || Expr::value(Option::<PolicyIdempotencyKey>::None);
    let null_validation_subject = || Expr::value(Option::<PolicyValidationSubject>::None);
    let mut query = revision_query_head();
    query
        .expr_as(
            Expr::col((Entity, Column::SchemaVersion)),
            ActivityColumn::SchemaVersion,
        )
        .expr_as(
            Expr::col((Entity, Column::Document)),
            ActivityColumn::Document,
        )
        .expr_as(
            Expr::col((Entity, Column::Status)),
            ActivityColumn::RevisionStatus,
        )
        .expr_as(
            Expr::col((Entity, Column::ValidationEvidence)),
            ActivityColumn::ValidationEvidence,
        )
        .expr_as(
            Expr::col((Entity, Column::ValidatedAt)),
            ActivityColumn::ValidatedAt,
        )
        .expr_as(
            Expr::col((Entity, Column::PreflightTokenHash)),
            ActivityColumn::PreflightTokenHash,
        )
        .expr_as(
            Expr::col((Entity, Column::PreflightExpiresAt)),
            ActivityColumn::PreflightExpiresAt,
        )
        .expr_as(null_approval_id(), ActivityColumn::PolicyApprovalId)
        .expr_as(
            null_validation_subject(),
            ActivityColumn::ApprovalValidationSubject,
        )
        .expr_as(
            enum_null::<PolicyApprovalDecision>(),
            ActivityColumn::ApprovalDecision,
        )
        .expr_as(null_timestamp(), ActivityColumn::DecidedAt)
        .expr_as(null_timestamp(), ActivityColumn::ApprovalExpiresAt)
        .expr_as(null_generation(), ActivityColumn::BundleGeneration)
        .expr_as(null_generation(), ActivityColumn::ExpectedBundleGeneration)
        .expr_as(null_activation_id(), ActivityColumn::PolicyActivationId)
        .expr_as(null_snapshot_id(), ActivityColumn::DecisionPolicySnapshotId)
        .expr_as(null_timestamp(), ActivityColumn::ActivatedAt)
        .expr_as(
            enum_null::<PolicyActivationKind>(),
            ActivityColumn::ActivationKind,
        )
        .expr_as(null_revision_id(), ActivityColumn::ExpectedActiveRevisionId)
        .expr_as(null_revision_id(), ActivityColumn::PreviousPolicyRevisionId)
        .expr_as(null_revision_id(), ActivityColumn::RollbackTargetRevisionId)
        .expr_as(null_idempotency_key(), ActivityColumn::IdempotencyKey)
        .expr_as(null_content_hash(), ActivityColumn::ActivationRequestHash)
        .expr_as(null_audit_id(), ActivityColumn::AuditEventId)
        .from(Entity)
        .to_owned()
}

fn approval_query() -> SelectStatement {
    let null_generation = || Expr::value(Option::<PolicyBundleGeneration>::None);
    let null_activation_id = || Expr::value(Option::<PolicyActivationId>::None);
    let null_snapshot_id = || Expr::value(Option::<DecisionPolicySnapshotId>::None);
    let null_timestamp = || Expr::value(Option::<DateTime<Utc>>::None);
    let null_revision_id = || Expr::value(Option::<PolicyRevisionId>::None);
    let null_audit_id = || Expr::value(Option::<AuditEventId>::None);
    let null_schema_version = || Expr::value(Option::<SchemaVersion>::None);
    let null_document = || Expr::value(Option::<PolicyDocument>::None);
    let null_validation_evidence = || Expr::value(Option::<PolicyValidationEvidence>::None);
    let null_content_hash = || Expr::value(Option::<ContentHash>::None);
    let null_idempotency_key = || Expr::value(Option::<PolicyIdempotencyKey>::None);
    Query::select()
        .expr_as(
            Expr::value(ActivityRecordKind::Approval.as_str()),
            ActivityColumn::EventType,
        )
        .expr_as(
            Expr::col((PolicyApprovalEntity, PolicyApprovalColumn::CreatedAt)),
            ActivityColumn::EventAt,
        )
        .expr_as(
            ActivityRecordKind::Approval.rank_expr(),
            ActivityColumn::EventRank,
        )
        .expr_as(
            Expr::col((PolicyApprovalEntity, PolicyApprovalColumn::PolicyApprovalId)),
            ActivityColumn::SortId,
        )
        .expr_as(
            Expr::col((PolicyApprovalEntity, PolicyApprovalColumn::PolicyRevisionId)),
            ActivityColumn::PolicyRevisionId,
        )
        .expr_as(
            Expr::col((PolicyApprovalEntity, PolicyApprovalColumn::ResourceKind)),
            ActivityColumn::ResourceKind,
        )
        .expr_as(
            Expr::col((PolicyApprovalEntity, PolicyApprovalColumn::RevisionHash)),
            ActivityColumn::RevisionHash,
        )
        .expr_as(
            Expr::col((PolicyApprovalEntity, PolicyApprovalColumn::Reason)),
            ActivityColumn::Reason,
        )
        .expr_as(
            Expr::col((PolicyApprovalEntity, PolicyApprovalColumn::DecidedByKind)),
            ActivityColumn::ActorKind,
        )
        .expr_as(
            Expr::col((PolicyApprovalEntity, PolicyApprovalColumn::DecidedByUserId)),
            ActivityColumn::ActorUserId,
        )
        .expr_as(
            Expr::col((PolicyApprovalEntity, PolicyApprovalColumn::DecidedByLabel)),
            ActivityColumn::ActorLabel,
        )
        .expr_as(
            Expr::col((PolicyApprovalEntity, PolicyApprovalColumn::CreatedAt)),
            ActivityColumn::CreatedAt,
        )
        .expr_as(null_schema_version(), ActivityColumn::SchemaVersion)
        .expr_as(null_document(), ActivityColumn::Document)
        .expr_as(
            enum_null::<PolicyRevisionStatus>(),
            ActivityColumn::RevisionStatus,
        )
        .expr_as(
            null_validation_evidence(),
            ActivityColumn::ValidationEvidence,
        )
        .expr_as(null_timestamp(), ActivityColumn::ValidatedAt)
        .expr_as(null_content_hash(), ActivityColumn::PreflightTokenHash)
        .expr_as(null_timestamp(), ActivityColumn::PreflightExpiresAt)
        .expr_as(
            Expr::col((PolicyApprovalEntity, PolicyApprovalColumn::PolicyApprovalId)),
            ActivityColumn::PolicyApprovalId,
        )
        .expr_as(
            Expr::col((
                PolicyApprovalEntity,
                PolicyApprovalColumn::ValidationSubject,
            )),
            ActivityColumn::ApprovalValidationSubject,
        )
        .expr_as(
            Expr::col((PolicyApprovalEntity, PolicyApprovalColumn::Decision)),
            ActivityColumn::ApprovalDecision,
        )
        .expr_as(
            Expr::col((PolicyApprovalEntity, PolicyApprovalColumn::DecidedAt)),
            ActivityColumn::DecidedAt,
        )
        .expr_as(
            Expr::col((PolicyApprovalEntity, PolicyApprovalColumn::ExpiresAt)),
            ActivityColumn::ApprovalExpiresAt,
        )
        .expr_as(null_generation(), ActivityColumn::BundleGeneration)
        .expr_as(null_generation(), ActivityColumn::ExpectedBundleGeneration)
        .expr_as(null_activation_id(), ActivityColumn::PolicyActivationId)
        .expr_as(null_snapshot_id(), ActivityColumn::DecisionPolicySnapshotId)
        .expr_as(null_timestamp(), ActivityColumn::ActivatedAt)
        .expr_as(
            enum_null::<PolicyActivationKind>(),
            ActivityColumn::ActivationKind,
        )
        .expr_as(null_revision_id(), ActivityColumn::ExpectedActiveRevisionId)
        .expr_as(null_revision_id(), ActivityColumn::PreviousPolicyRevisionId)
        .expr_as(null_revision_id(), ActivityColumn::RollbackTargetRevisionId)
        .expr_as(null_idempotency_key(), ActivityColumn::IdempotencyKey)
        .expr_as(null_content_hash(), ActivityColumn::ActivationRequestHash)
        .expr_as(null_audit_id(), ActivityColumn::AuditEventId)
        .from(PolicyApprovalEntity)
        .to_owned()
}

fn activation_query_head() -> SelectStatement {
    let null_timestamp = || Expr::value(Option::<DateTime<Utc>>::None);
    let null_schema_version = || Expr::value(Option::<SchemaVersion>::None);
    let null_document = || Expr::value(Option::<PolicyDocument>::None);
    let null_validation_evidence = || Expr::value(Option::<PolicyValidationEvidence>::None);
    let null_content_hash = || Expr::value(Option::<ContentHash>::None);
    Query::select()
        .expr_as(
            Expr::value(ActivityRecordKind::Activation.as_str()),
            ActivityColumn::EventType,
        )
        .expr_as(
            Expr::col((PolicyActivationEntity, PolicyActivationColumn::CreatedAt)),
            ActivityColumn::EventAt,
        )
        .expr_as(
            ActivityRecordKind::Activation.rank_expr(),
            ActivityColumn::EventRank,
        )
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::PolicyActivationId,
            )),
            ActivityColumn::SortId,
        )
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::PolicyRevisionId,
            )),
            ActivityColumn::PolicyRevisionId,
        )
        .expr_as(
            Expr::col((PolicyActivationEntity, PolicyActivationColumn::ResourceKind)),
            ActivityColumn::ResourceKind,
        )
        .expr_as(null_content_hash(), ActivityColumn::RevisionHash)
        .expr_as(
            Expr::col((PolicyActivationEntity, PolicyActivationColumn::Reason)),
            ActivityColumn::Reason,
        )
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::ActivatedByKind,
            )),
            ActivityColumn::ActorKind,
        )
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::ActivatedByUserId,
            )),
            ActivityColumn::ActorUserId,
        )
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::ActivatedByLabel,
            )),
            ActivityColumn::ActorLabel,
        )
        .expr_as(
            Expr::col((PolicyActivationEntity, PolicyActivationColumn::CreatedAt)),
            ActivityColumn::CreatedAt,
        )
        .expr_as(null_schema_version(), ActivityColumn::SchemaVersion)
        .expr_as(null_document(), ActivityColumn::Document)
        .expr_as(
            enum_null::<PolicyRevisionStatus>(),
            ActivityColumn::RevisionStatus,
        )
        .expr_as(
            null_validation_evidence(),
            ActivityColumn::ValidationEvidence,
        )
        .expr_as(null_timestamp(), ActivityColumn::ValidatedAt)
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::PreflightTokenHash,
            )),
            ActivityColumn::PreflightTokenHash,
        )
        .expr_as(null_timestamp(), ActivityColumn::PreflightExpiresAt)
        .to_owned()
}

fn activation_query() -> SelectStatement {
    let null_validation_subject = || Expr::value(Option::<PolicyValidationSubject>::None);
    let null_timestamp = || Expr::value(Option::<DateTime<Utc>>::None);
    let mut query = activation_query_head();
    query
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::PolicyApprovalId,
            )),
            ActivityColumn::PolicyApprovalId,
        )
        .expr_as(
            null_validation_subject(),
            ActivityColumn::ApprovalValidationSubject,
        )
        .expr_as(
            enum_null::<PolicyApprovalDecision>(),
            ActivityColumn::ApprovalDecision,
        )
        .expr_as(null_timestamp(), ActivityColumn::DecidedAt)
        .expr_as(null_timestamp(), ActivityColumn::ApprovalExpiresAt)
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::BundleGeneration,
            )),
            ActivityColumn::BundleGeneration,
        )
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::ExpectedBundleGeneration,
            )),
            ActivityColumn::ExpectedBundleGeneration,
        )
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::PolicyActivationId,
            )),
            ActivityColumn::PolicyActivationId,
        )
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::DecisionPolicySnapshotId,
            )),
            ActivityColumn::DecisionPolicySnapshotId,
        )
        .expr_as(
            Expr::col((PolicyActivationEntity, PolicyActivationColumn::ActivatedAt)),
            ActivityColumn::ActivatedAt,
        )
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::ActivationKind,
            )),
            ActivityColumn::ActivationKind,
        )
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::ExpectedActiveRevisionId,
            )),
            ActivityColumn::ExpectedActiveRevisionId,
        )
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::PreviousPolicyRevisionId,
            )),
            ActivityColumn::PreviousPolicyRevisionId,
        )
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::RollbackTargetRevisionId,
            )),
            ActivityColumn::RollbackTargetRevisionId,
        )
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::IdempotencyKey,
            )),
            ActivityColumn::IdempotencyKey,
        )
        .expr_as(
            Expr::col((
                PolicyActivationEntity,
                PolicyActivationColumn::ActivationRequestHash,
            )),
            ActivityColumn::ActivationRequestHash,
        )
        .expr_as(
            Expr::col((PolicyActivationEntity, PolicyActivationColumn::AuditEventId)),
            ActivityColumn::AuditEventId,
        )
        .from(PolicyActivationEntity)
        .to_owned()
}

impl PgPolicyRepository {
    pub(super) async fn list(
        db: &impl ConnectionTrait,
        limit: u64,
    ) -> Result<Vec<ConfigActivityInfo>, StorageError> {
        // Global ordering follows the database commit clock (`created_at`). Domain
        // timestamps such as `decided_at` may originate on another host and cannot
        // establish causal order across revision, approval, and activation tables.
        let mut query = revision_query();
        query
            .union(UnionType::All, approval_query())
            .union(UnionType::All, activation_query())
            .order_by(ActivityColumn::EventAt, Order::Desc)
            .order_by(ActivityColumn::EventRank, Order::Desc)
            .order_by(ActivityColumn::SortId, Order::Desc)
            .limit(limit);
        let rows = ActivityRow::find_by_statement(db.get_database_backend().build(&query))
            .all(db)
            .await
            .map_err(StorageError::from)?;
        rows.into_iter().map(ConfigActivityInfo::try_from).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use quant_pivot_error::storage::StorageError;
    use sea_orm::{DbBackend, MockDatabase, Value};

    use super::PgPolicyRepository;

    #[tokio::test]
    async fn list_executes_one_statement() -> Result<(), StorageError> {
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([Vec::<BTreeMap<String, Value>>::new()])
            .into_connection();

        let activity = PgPolicyRepository::list(&db, 50).await?;

        assert!(activity.is_empty());
        assert_eq!(db.into_transaction_log().len(), 1);
        Ok(())
    }
}
