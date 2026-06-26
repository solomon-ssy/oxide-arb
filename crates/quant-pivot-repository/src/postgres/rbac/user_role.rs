//! Postgres implementation of [`UserRoleRepository`] with replace-set semantics
//! and atomic Casbin `g` synchronisation.

use std::collections::{HashMap, HashSet};

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{AssignRoles, RoleInfo},
    entities::{role, user, user_role},
    enums::rbac::RoleStatus,
    types::{RoleId, UserId},
};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder, TransactionTrait, sea_query::OnConflict,
};

use crate::{
    postgres::rbac::{casbin::sync, junction, util},
    traits::rbac::UserRoleRepository,
};

/// User→role assignment repository backed by Postgres.
pub struct PgUserRoleRepository {
    db: DatabaseConnection,
}

impl PgUserRoleRepository {
    /// Create a repository over the given connection handle.
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

async fn do_set_roles(
    db: &DatabaseConnection,
    assignment: AssignRoles,
) -> Result<(), StorageError> {
    let AssignRoles { user_id, role_ids } = assignment;
    let target: HashSet<RoleId> = role_ids.into_iter().collect();

    let txn = db.begin().await.map_err(StorageError::from)?;

    if user::Entity::find_by_id(user_id.clone())
        .one(&txn)
        .await
        .map_err(StorageError::from)?
        .is_none()
    {
        txn.rollback().await.map_err(StorageError::from)?;
        return Err(util::not_found("user", &user_id));
    }

    // Resolve every target role to its code, rejecting unknown ids.
    let target_roles = role::Entity::find()
        .filter(role::Column::Id.is_in(target.iter().cloned()))
        .all(&txn)
        .await
        .map_err(StorageError::from)?;
    if target_roles.len() != target.len() {
        let found: HashSet<RoleId> = target_roles.iter().map(|role| role.id.clone()).collect();
        let missing = target
            .iter()
            .find(|id| !found.contains(id))
            .map_or_else(|| "<unknown>".to_owned(), ToString::to_string);
        txn.rollback().await.map_err(StorageError::from)?;
        return Err(util::not_found("role", missing));
    }
    // Only *enabled* roles project a Casbin grouping (`g`): a disabled role keeps
    // its relational `user_role` membership but grants nothing until re-enabled,
    // at which point its groupings are rebuilt from that membership.
    let enabled_code_of: HashMap<RoleId, String> = target_roles
        .iter()
        .filter(|role| role.status == RoleStatus::Enabled)
        .map(|role| (role.id.clone(), role.code.clone()))
        .collect();

    let current: HashSet<RoleId> = user_role::Entity::find()
        .filter(user_role::Column::UserId.eq(user_id.clone()))
        .all(&txn)
        .await
        .map_err(StorageError::from)?
        .into_iter()
        .map(|row| row.role_id)
        .collect();

    let (added, removed) = junction::replace_set_diff(&target, &current);

    // Codes for removed roles (their ids are guaranteed to exist via FK).
    let removed_codes = if removed.is_empty() {
        HashMap::new()
    } else {
        role::Entity::find()
            .filter(role::Column::Id.is_in(removed.iter().cloned()))
            .all(&txn)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(|role| (role.id, role.code))
            .collect::<HashMap<RoleId, String>>()
    };

    if !added.is_empty() {
        let rows = added.iter().map(|role_id| user_role::ActiveModel {
            user_id: Set(user_id.clone()),
            role_id: Set(role_id.clone()),
            ..Default::default()
        });
        junction::insert_junction_rows::<user_role::Entity>(
            &txn,
            rows,
            OnConflict::columns([user_role::Column::UserId, user_role::Column::RoleId])
                .do_nothing()
                .to_owned(),
        )
        .await?;
        for role_id in &added {
            if let Some(code) = enabled_code_of.get(role_id) {
                sync::do_grant_role(&txn, &user_id, code).await?;
            }
        }
    }

    if !removed.is_empty() {
        user_role::Entity::delete_many()
            .filter(user_role::Column::UserId.eq(user_id.clone()))
            .filter(user_role::Column::RoleId.is_in(removed.iter().cloned()))
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        for role_id in &removed {
            if let Some(code) = removed_codes.get(role_id) {
                sync::do_revoke_role(&txn, &user_id, code).await?;
            }
        }
    }

    txn.commit().await.map_err(StorageError::from)?;
    Ok(())
}

async fn do_list_roles_for_user(
    db: &impl ConnectionTrait,
    user_id: &UserId,
) -> Result<Vec<RoleInfo>, StorageError> {
    let role_ids: Vec<RoleId> = user_role::Entity::find()
        .filter(user_role::Column::UserId.eq(user_id.clone()))
        .all(db)
        .await
        .map_err(StorageError::from)?
        .into_iter()
        .map(|row| row.role_id)
        .collect();
    if role_ids.is_empty() {
        return Ok(Vec::new());
    }

    let roles = role::Entity::find()
        .filter(role::Column::Id.is_in(role_ids))
        .order_by_asc(role::Column::Sort)
        .order_by_asc(role::Column::Code)
        .all(db)
        .await
        .map_err(StorageError::from)?;
    Ok(roles.into_iter().map(Into::into).collect())
}

#[async_trait::async_trait]
impl UserRoleRepository for PgUserRoleRepository {
    async fn set_roles_for_user(&self, assignment: AssignRoles) -> Result<(), StorageError> {
        do_set_roles(&self.db, assignment).await
    }

    async fn list_roles_for_user(&self, user_id: &UserId) -> Result<Vec<RoleInfo>, StorageError> {
        do_list_roles_for_user(&self.db, user_id).await
    }
}
