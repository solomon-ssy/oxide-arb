//! Transaction-scoped helpers that write the backing Casbin rows (`g` grouping
//! and `p` permission lines) for the RBAC assignment repositories.
//!
//! These run inside the same transaction as the join-table writes so that the
//! relational state and the Casbin policy table can never diverge — the
//! repository transaction is the single source of truth. Reloading the live
//! enforcer is the service layer's responsibility (`CasbinService::reload`).
//!
//! Row layout is single-sourced in [`quant_pivot_models::enums::rbac::casbin`]:
//! - `g`: `v0 = user_id`, `v1 = role_code`
//! - `p`: `v0 = role_code`, `v1 = resource`, `v2 = operation`, `v3 = "resource"`

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::rbac::Permission,
    entities::casbin_rule::{Column, Entity},
    enums::rbac::{
        casbin::{PTYPE_GROUPING, PTYPE_POLICY},
        parse_permission,
    },
    types::{RoleCode, UserId},
};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, sea_query::Condition};

use crate::postgres::{rbac::casbin::row, write::upsert_many_chunked};

/// Grant a bounded batch of `g(user_id, role_code)` groupings, idempotent on
/// the full tuple and chunked only at `PostgreSQL`'s bind limit.
pub async fn do_grant_roles(
    conn: &impl ConnectionTrait,
    user_id: &UserId,
    role_codes: &[RoleCode],
) -> Result<(), StorageError> {
    let rows = role_codes
        .iter()
        .map(|role_code| row::grouping_row(user_id, role_code.as_str()))
        .collect();
    upsert_many_chunked::<Entity, _>(
        conn,
        rows,
        row::full_tuple_conflict().do_nothing().to_owned(),
    )
    .await?;
    Ok(())
}

/// Revoke a bounded batch of `g(user_id, role_code)` groupings in one delete.
pub async fn do_revoke_roles(
    conn: &impl ConnectionTrait,
    user_id: &UserId,
    role_codes: &[RoleCode],
) -> Result<(), StorageError> {
    if role_codes.is_empty() {
        return Ok(());
    }
    Entity::delete_many()
        .filter(Column::Ptype.eq(PTYPE_GROUPING))
        .filter(Column::V0.eq(user_id.to_string()))
        .filter(Column::V1.is_in(role_codes.iter().map(RoleCode::as_str)))
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

/// Suspend a role's effect without losing data: drop every `g` grouping that
/// binds a subject to `role_code`, leaving the role's `p` permissions and the
/// relational `user_role` rows intact.
///
/// This is the disable half of the role-status lifecycle — once the groupings
/// are gone, no subject resolves the role in the matcher, so its permissions
/// stop granting immediately (after the enforcer reloads). Re-enabling rebuilds
/// the groupings from the surviving `user_role` rows via
/// [`do_rebuild_role_bindings`].
pub async fn do_revoke_role_bindings(
    conn: &impl ConnectionTrait,
    role_code: &RoleCode,
) -> Result<(), StorageError> {
    Entity::delete_many()
        .filter(Column::Ptype.eq(PTYPE_GROUPING))
        .filter(Column::V1.eq(role_code.as_str()))
        .exec(conn)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

/// Rebuild a role's `g` groupings from its current relational membership: insert
/// one `g(user_id, role_code)` per holder, idempotent on the full tuple.
///
/// This is the enable half of the role-status lifecycle. `user_role` is the
/// source of truth for membership, so the groupings can always be reconstructed
/// losslessly after a disable.
pub async fn do_rebuild_role_bindings(
    conn: &impl ConnectionTrait,
    role_code: &RoleCode,
    user_ids: &[UserId],
) -> Result<(), StorageError> {
    if user_ids.is_empty() {
        return Ok(());
    }
    let rows = user_ids
        .iter()
        .map(|user_id| row::grouping_row(user_id, role_code.as_str()))
        .collect::<Vec<_>>();
    upsert_many_chunked::<Entity, _>(
        conn,
        rows,
        row::full_tuple_conflict().do_nothing().to_owned(),
    )
    .await?;
    Ok(())
}

/// Remove every trace of a role code: its `p` permissions and any `g` grouping
/// that grants it to a subject (used when deleting the role).
pub async fn do_purge_role_code(
    conn: &impl ConnectionTrait,
    role_code: &RoleCode,
) -> Result<(), StorageError> {
    Entity::delete_many()
        .filter(
            Condition::any()
                .add(
                    Condition::all()
                        .add(Column::Ptype.eq(PTYPE_POLICY))
                        .add(Column::V0.eq(role_code.as_str())),
                )
                .add(
                    Condition::all()
                        .add(Column::Ptype.eq(PTYPE_GROUPING))
                        .add(Column::V1.eq(role_code.as_str())),
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
    role_code: &RoleCode,
    permissions: &[Permission],
) -> Result<(), StorageError> {
    Entity::delete_many()
        .filter(Column::Ptype.eq(PTYPE_POLICY))
        .filter(Column::V0.eq(role_code.as_str()))
        .exec(conn)
        .await
        .map_err(StorageError::from)?;

    if permissions.is_empty() {
        return Ok(());
    }

    let rows = permissions
        .iter()
        .map(|perm| row::policy_row(role_code.as_str(), perm.resource, perm.operation))
        .collect::<Vec<_>>();
    upsert_many_chunked::<Entity, _>(
        conn,
        rows,
        row::full_tuple_conflict().do_nothing().to_owned(),
    )
    .await?;
    Ok(())
}

/// List a role's permissions, parsed back from its stored `p` rows.
pub async fn do_list_role_policies(
    conn: &impl ConnectionTrait,
    role_code: &RoleCode,
) -> Result<Vec<Permission>, StorageError> {
    let rows = Entity::find()
        .filter(Column::Ptype.eq(PTYPE_POLICY))
        .filter(Column::V0.eq(role_code.as_str()))
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
