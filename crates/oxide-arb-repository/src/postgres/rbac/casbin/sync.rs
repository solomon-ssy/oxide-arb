//! Transaction-scoped helpers that write the backing Casbin rows (`g` grouping
//! and `p` permission lines) for the RBAC assignment repositories.
//!
//! These run inside the same transaction as the join-table writes so that the
//! relational state and the Casbin policy table can never diverge — the
//! repository transaction is the single source of truth. Reloading the live
//! enforcer is the service layer's responsibility (`CasbinService::reload`).
//!
//! Row layout is single-sourced in [`oxide_arb_models::enums::rbac::casbin`]:
//! - `g`: `v0 = user_id`, `v1 = role_code`
//! - `p`: `v0 = role_code`, `v1 = resource`, `v2 = operation`, `v3 = "resource"`

use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::Permission,
    entities::casbin_rule::{Column, Entity},
    enums::rbac::{
        casbin::{PTYPE_GROUPING, PTYPE_POLICY},
        parse_permission,
    },
    types::UserId,
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, sea_query::Condition};

use crate::postgres::rbac::casbin::row;

/// Grant a single `g(user_id, role_code)` grouping, idempotent on the full tuple.
pub async fn do_grant_role(
    conn: &impl ConnectionTrait,
    user_id: &UserId,
    role_code: &str,
) -> Result<(), StorageError> {
    Entity::insert(row::grouping_row(user_id, role_code))
        .on_conflict(row::full_tuple_conflict().do_nothing().to_owned())
        .exec_without_returning(conn)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

/// Revoke a single `g(user_id, role_code)` grouping.
pub async fn do_revoke_role(
    conn: &impl ConnectionTrait,
    user_id: &UserId,
    role_code: &str,
) -> Result<(), StorageError> {
    Entity::delete_many()
        .filter(Column::Ptype.eq(PTYPE_GROUPING))
        .filter(Column::V0.eq(user_id.to_string()))
        .filter(Column::V1.eq(role_code))
        .exec(conn)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

/// Remove every grouping line for a user (used when deleting the user).
pub async fn do_revoke_all_roles_for_user(
    conn: &impl ConnectionTrait,
    user_id: &UserId,
) -> Result<(), StorageError> {
    Entity::delete_many()
        .filter(Column::Ptype.eq(PTYPE_GROUPING))
        .filter(Column::V0.eq(user_id.to_string()))
        .exec(conn)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

/// Remove every trace of a role code: its `p` permissions and any `g` grouping
/// that grants it to a subject (used when deleting the role).
pub async fn do_purge_role_code(
    conn: &impl ConnectionTrait,
    role_code: &str,
) -> Result<(), StorageError> {
    Entity::delete_many()
        .filter(
            Condition::any()
                .add(
                    Condition::all()
                        .add(Column::Ptype.eq(PTYPE_POLICY))
                        .add(Column::V0.eq(role_code)),
                )
                .add(
                    Condition::all()
                        .add(Column::Ptype.eq(PTYPE_GROUPING))
                        .add(Column::V1.eq(role_code)),
                ),
        )
        .exec(conn)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

/// Replace the entire `p` permission set of a role: drop the role's existing
/// policies, then insert the validated set (idempotent on the full tuple).
pub async fn do_replace_role_policies(
    conn: &impl ConnectionTrait,
    role_code: &str,
    permissions: &[Permission],
) -> Result<(), StorageError> {
    Entity::delete_many()
        .filter(Column::Ptype.eq(PTYPE_POLICY))
        .filter(Column::V0.eq(role_code))
        .exec(conn)
        .await
        .map_err(StorageError::from)?;

    if permissions.is_empty() {
        return Ok(());
    }

    let rows = permissions
        .iter()
        .map(|perm| row::policy_row(role_code, perm.resource, perm.operation))
        .collect::<Vec<_>>();
    Entity::insert_many(rows)
        .on_conflict(row::full_tuple_conflict().do_nothing().to_owned())
        .exec_without_returning(conn)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

/// List a role's permissions, parsed back from its stored `p` rows.
pub async fn do_list_role_policies(
    conn: &impl ConnectionTrait,
    role_code: &str,
) -> Result<Vec<Permission>, StorageError> {
    let rows = Entity::find()
        .filter(Column::Ptype.eq(PTYPE_POLICY))
        .filter(Column::V0.eq(role_code))
        .all(conn)
        .await
        .map_err(StorageError::from)?;

    rows.into_iter()
        .map(|policy| {
            parse_permission(&policy.v1, &policy.v2)
                .map(|(resource, operation)| Permission::new(resource, operation))
                .map_err(|error| StorageError::Codec(error.to_string()))
        })
        .collect()
}
