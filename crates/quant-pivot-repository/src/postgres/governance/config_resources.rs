//! Single-statement, DB-authoritative Config resource inventory.

use crate::postgres::primitives::enum_value;
use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ConfigResourceInventoryInfo, ConfigResourceInventoryRow},
    entities::{
        decision_policy_snapshot, policy_activation, policy_activation_guard, policy_approval,
        policy_revision,
    },
    enums::runtime_config::{ConfigResourceKind, PolicyApprovalDecision, PolicyRevisionStatus},
    types::{ContentHash, DecisionPolicySnapshotId, PolicyBundleGeneration, PolicyRevisionId},
};
use sea_orm::{ConnectionTrait, FromQueryResult};
use sea_query::{
    Alias, Condition, Expr, ExprTrait, Iden, JoinType, Order, Query, SelectStatement,
    extension::postgres::PgExpr,
};

const VALIDATION_SUBJECT_FIELD: &str = "subject";
const VALIDATION_BASE_GENERATION_FIELD: &str = "base_generation";

pub(super) fn approved_base_generation() -> Expr {
    Expr::col((
        policy_approval::Entity,
        policy_approval::Column::ValidationSubject,
    ))
    .cast_json_field(VALIDATION_BASE_GENERATION_FIELD)
    .cast_as(Alias::new("bigint"))
}

#[derive(Iden)]
enum Resources {
    Table,
    ResourceKind,
}

#[derive(Iden)]
enum CurrentActivation {
    Table,
    ResourceKind,
    PolicyRevisionId,
    ActivatedAt,
}

#[derive(Iden)]
enum PendingApproval {
    Table,
    ResourceKind,
    ApprovalCount,
}

#[derive(Iden)]
enum InventoryColumn {
    ResourceKind,
    GuardGeneration,
    GuardSnapshotId,
    GuardSnapshotHash,
    SnapshotGeneration,
    SnapshotId,
    SnapshotHash,
    ActiveRevisionId,
    ActiveRevisionHash,
    LastActivatedAt,
    PendingApprovalCount,
}

#[derive(Debug, FromQueryResult)]
struct InventoryRow {
    resource_kind: ConfigResourceKind,
    guard_generation: PolicyBundleGeneration,
    guard_snapshot_id: Option<DecisionPolicySnapshotId>,
    guard_snapshot_hash: Option<ContentHash>,
    snapshot_generation: Option<PolicyBundleGeneration>,
    snapshot_id: Option<DecisionPolicySnapshotId>,
    snapshot_hash: Option<ContentHash>,
    active_revision_id: Option<PolicyRevisionId>,
    active_revision_hash: Option<ContentHash>,
    last_activated_at: Option<DateTime<Utc>>,
    pending_approval_count: Option<i64>,
}

fn resources_query() -> SelectStatement {
    Query::select()
        .distinct()
        .expr_as(
            Expr::col((
                policy_revision::Entity,
                policy_revision::Column::ResourceKind,
            )),
            Resources::ResourceKind,
        )
        .from(policy_revision::Entity)
        .to_owned()
}

fn current_activation_query() -> SelectStatement {
    Query::select()
        .distinct_on([(
            policy_activation::Entity,
            policy_activation::Column::ResourceKind,
        )])
        .expr_as(
            Expr::col((
                policy_activation::Entity,
                policy_activation::Column::ResourceKind,
            )),
            CurrentActivation::ResourceKind,
        )
        .expr_as(
            Expr::col((
                policy_activation::Entity,
                policy_activation::Column::PolicyRevisionId,
            )),
            CurrentActivation::PolicyRevisionId,
        )
        .expr_as(
            Expr::col((
                policy_activation::Entity,
                policy_activation::Column::ActivatedAt,
            )),
            CurrentActivation::ActivatedAt,
        )
        .from(policy_activation::Entity)
        .order_by(
            (
                policy_activation::Entity,
                policy_activation::Column::ResourceKind,
            ),
            Order::Asc,
        )
        .order_by(
            (
                policy_activation::Entity,
                policy_activation::Column::ActivatedAt,
            ),
            Order::Desc,
        )
        .order_by(
            (
                policy_activation::Entity,
                policy_activation::Column::PolicyActivationId,
            ),
            Order::Desc,
        )
        .to_owned()
}

fn pending_approval_query() -> SelectStatement {
    Query::select()
        .expr_as(
            Expr::col((
                policy_approval::Entity,
                policy_approval::Column::ResourceKind,
            )),
            PendingApproval::ResourceKind,
        )
        .expr_as(
            Expr::col((
                policy_approval::Entity,
                policy_approval::Column::PolicyApprovalId,
            ))
            .count(),
            PendingApproval::ApprovalCount,
        )
        .from(policy_approval::Entity)
        .join(
            JoinType::LeftJoin,
            policy_activation::Entity,
            Expr::col((
                policy_approval::Entity,
                policy_approval::Column::PolicyApprovalId,
            ))
            .equals((
                policy_activation::Entity,
                policy_activation::Column::PolicyApprovalId,
            )),
        )
        .join(
            JoinType::InnerJoin,
            policy_revision::Entity,
            Expr::col((
                policy_approval::Entity,
                policy_approval::Column::PolicyRevisionId,
            ))
            .equals((
                policy_revision::Entity,
                policy_revision::Column::PolicyRevisionId,
            )),
        )
        .cross_join(policy_activation_guard::Entity)
        .and_where(
            Expr::col((
                policy_activation::Entity,
                policy_activation::Column::PolicyActivationId,
            ))
            .is_null(),
        )
        .and_where(
            Expr::col((policy_approval::Entity, policy_approval::Column::Decision))
                .eq(enum_value(&PolicyApprovalDecision::Approved)),
        )
        .and_where(
            Expr::col((policy_revision::Entity, policy_revision::Column::Status))
                .eq(enum_value(&PolicyRevisionStatus::Validated)),
        )
        .and_where(
            Expr::col((
                policy_revision::Entity,
                policy_revision::Column::ResourceKind,
            ))
            .equals((
                policy_approval::Entity,
                policy_approval::Column::ResourceKind,
            )),
        )
        .and_where(
            Expr::col((
                policy_approval::Entity,
                policy_approval::Column::ValidationSubject,
            ))
            .eq(Expr::col((
                policy_revision::Entity,
                policy_revision::Column::ValidationEvidence,
            ))
            .get_json_field(VALIDATION_SUBJECT_FIELD)),
        )
        .and_where(
            Expr::col((
                policy_revision::Entity,
                policy_revision::Column::RevisionHash,
            ))
            .equals((
                policy_approval::Entity,
                policy_approval::Column::RevisionHash,
            )),
        )
        .and_where(
            Expr::col((
                policy_revision::Entity,
                policy_revision::Column::PreflightExpiresAt,
            ))
            .gt(Expr::current_timestamp()),
        )
        .and_where(approved_base_generation().eq(Expr::col((
            policy_activation_guard::Entity,
            policy_activation_guard::Column::Generation,
        ))))
        .and_where(
            Expr::col((
                policy_activation_guard::Entity,
                policy_activation_guard::Column::Id,
            ))
            .eq(1),
        )
        .cond_where(
            Condition::any()
                .add(
                    Expr::col((policy_approval::Entity, policy_approval::Column::ExpiresAt))
                        .is_null(),
                )
                .add(
                    Expr::col((policy_approval::Entity, policy_approval::Column::ExpiresAt))
                        .gt(Expr::current_timestamp()),
                ),
        )
        .group_by_col((
            policy_approval::Entity,
            policy_approval::Column::ResourceKind,
        ))
        .to_owned()
}

fn inventory_query() -> SelectStatement {
    Query::select()
        .expr_as(
            Expr::col((Resources::Table, Resources::ResourceKind)),
            InventoryColumn::ResourceKind,
        )
        .expr_as(
            Expr::col((
                policy_activation_guard::Entity,
                policy_activation_guard::Column::Generation,
            )),
            InventoryColumn::GuardGeneration,
        )
        .expr_as(
            Expr::col((
                policy_activation_guard::Entity,
                policy_activation_guard::Column::CurrentSnapshotId,
            )),
            InventoryColumn::GuardSnapshotId,
        )
        .expr_as(
            Expr::col((
                policy_activation_guard::Entity,
                policy_activation_guard::Column::CurrentSnapshotHash,
            )),
            InventoryColumn::GuardSnapshotHash,
        )
        .expr_as(
            Expr::col((
                decision_policy_snapshot::Entity,
                decision_policy_snapshot::Column::BundleGeneration,
            )),
            InventoryColumn::SnapshotGeneration,
        )
        .expr_as(
            Expr::col((
                decision_policy_snapshot::Entity,
                decision_policy_snapshot::Column::DecisionPolicySnapshotId,
            )),
            InventoryColumn::SnapshotId,
        )
        .expr_as(
            Expr::col((
                decision_policy_snapshot::Entity,
                decision_policy_snapshot::Column::SnapshotHash,
            )),
            InventoryColumn::SnapshotHash,
        )
        .expr_as(
            Expr::col((
                CurrentActivation::Table,
                CurrentActivation::PolicyRevisionId,
            )),
            InventoryColumn::ActiveRevisionId,
        )
        .expr_as(
            Expr::col((
                policy_revision::Entity,
                policy_revision::Column::RevisionHash,
            )),
            InventoryColumn::ActiveRevisionHash,
        )
        .expr_as(
            Expr::col((CurrentActivation::Table, CurrentActivation::ActivatedAt)),
            InventoryColumn::LastActivatedAt,
        )
        .expr_as(
            Expr::col((PendingApproval::Table, PendingApproval::ApprovalCount)),
            InventoryColumn::PendingApprovalCount,
        )
        .from_subquery(resources_query(), Resources::Table)
        .cross_join(policy_activation_guard::Entity)
        .join(
            JoinType::LeftJoin,
            decision_policy_snapshot::Entity,
            Expr::col((
                policy_activation_guard::Entity,
                policy_activation_guard::Column::CurrentSnapshotId,
            ))
            .equals((
                decision_policy_snapshot::Entity,
                decision_policy_snapshot::Column::DecisionPolicySnapshotId,
            )),
        )
        .join_subquery(
            JoinType::LeftJoin,
            current_activation_query(),
            CurrentActivation::Table,
            Expr::col((Resources::Table, Resources::ResourceKind))
                .equals((CurrentActivation::Table, CurrentActivation::ResourceKind)),
        )
        .join(
            JoinType::LeftJoin,
            policy_revision::Entity,
            Expr::col((
                CurrentActivation::Table,
                CurrentActivation::PolicyRevisionId,
            ))
            .equals((
                policy_revision::Entity,
                policy_revision::Column::PolicyRevisionId,
            )),
        )
        .join_subquery(
            JoinType::LeftJoin,
            pending_approval_query(),
            PendingApproval::Table,
            Expr::col((Resources::Table, Resources::ResourceKind))
                .equals((PendingApproval::Table, PendingApproval::ResourceKind)),
        )
        .and_where(
            Expr::col((
                policy_activation_guard::Entity,
                policy_activation_guard::Column::Id,
            ))
            .eq(1),
        )
        .order_by((Resources::Table, Resources::ResourceKind), Order::Asc)
        .to_owned()
}

fn validate_global_identity(
    row: &InventoryRow,
) -> Result<
    (
        PolicyBundleGeneration,
        Option<DecisionPolicySnapshotId>,
        Option<ContentHash>,
    ),
    StorageError,
> {
    match (
        &row.guard_snapshot_id,
        &row.guard_snapshot_hash,
        row.snapshot_generation,
        &row.snapshot_id,
        &row.snapshot_hash,
    ) {
        (None, None, None, None, None) => Ok((row.guard_generation, None, None)),
        (Some(guard_id), Some(guard_hash), Some(snapshot_generation), Some(id), Some(hash))
            if guard_id == id
                && guard_hash == hash
                && row.guard_generation == snapshot_generation =>
        {
            Ok((
                row.guard_generation,
                Some(guard_id.clone()),
                Some(guard_hash.clone()),
            ))
        }
        _ => Err(StorageError::invariant_violation(
            Some("policy_activation_guard"),
            "guard generation/id/hash does not match its current decision snapshot",
        )),
    }
}

pub(super) async fn load(
    db: &impl ConnectionTrait,
) -> Result<ConfigResourceInventoryInfo, StorageError> {
    let rows = InventoryRow::find_by_statement(db.get_database_backend().build(&inventory_query()))
        .all(db)
        .await
        .map_err(StorageError::from)?;
    let first = rows.first().ok_or_else(|| {
        StorageError::invariant_violation(
            Some("policy_activation_guard"),
            "boot seed row is missing",
        )
    })?;
    let (bundle_generation, active_snapshot_id, active_snapshot_hash) =
        validate_global_identity(first)?;
    let mut by_kind = BTreeMap::new();
    for row in rows {
        let identity = validate_global_identity(&row)?;
        if identity
            != (
                bundle_generation,
                active_snapshot_id.clone(),
                active_snapshot_hash.clone(),
            )
        {
            return Err(StorageError::invariant_violation(
                Some("policy_activation_guard"),
                "Config inventory statement returned inconsistent bundle identities",
            ));
        }
        let activation_shape = (
            row.active_revision_id.is_some(),
            row.active_revision_hash.is_some(),
            row.last_activated_at.is_some(),
        );
        if activation_shape != (false, false, false) && activation_shape != (true, true, true) {
            return Err(StorageError::invariant_violation(
                Some("policy_activation"),
                format!(
                    "{} current activation has incomplete revision lineage",
                    row.resource_kind
                ),
            ));
        }
        let pending_approval_count = u64::try_from(row.pending_approval_count.unwrap_or(0))
            .map_err(|error| {
                StorageError::invariant_violation(
                    Some("policy_approval"),
                    format!("negative approval count: {error}"),
                )
            })?;
        let resource_kind = row.resource_kind;
        if by_kind
            .insert(
                resource_kind,
                ConfigResourceInventoryRow {
                    resource_kind,
                    active_revision_id: row.active_revision_id,
                    active_revision_hash: row.active_revision_hash,
                    last_activated_at: row.last_activated_at,
                    pending_approval_count,
                },
            )
            .is_some()
        {
            return Err(StorageError::invariant_violation(
                Some("policy_activation"),
                format!("duplicate Config inventory row for {resource_kind}"),
            ));
        }
    }
    let resources = ConfigResourceKind::ALL
        .into_iter()
        .map(|kind| {
            by_kind.remove(&kind).ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("policy_activation"),
                    format!("Config inventory is missing {kind}"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !by_kind.is_empty() {
        return Err(StorageError::invariant_violation(
            Some("policy_activation"),
            "Config inventory returned unknown resource rows",
        ));
    }
    Ok(ConfigResourceInventoryInfo {
        bundle_generation,
        active_snapshot_id,
        active_snapshot_hash,
        resources,
    })
}

#[cfg(test)]
mod tests {
    use super::load;
    use std::collections::BTreeMap;

    use quant_pivot_error::storage::StorageError;
    use sea_orm::{DbBackend, MockDatabase, Value};

    #[tokio::test]
    async fn load_executes_one_consistent_inventory_statement() {
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([Vec::<BTreeMap<String, Value>>::new()])
            .into_connection();

        let result = load(&db).await;

        assert!(matches!(
            result,
            Err(StorageError::InvariantViolation { .. })
        ));
        assert_eq!(db.into_transaction_log().len(), 1);
    }
}
