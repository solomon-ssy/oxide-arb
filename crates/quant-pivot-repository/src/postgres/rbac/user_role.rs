//! Postgres implementation of [`UserRoleRepository`] with replace-set semantics
//! and atomic Casbin `g` synchronisation.

use std::collections::{HashMap, HashSet};

use quant_pivot_error::storage::{
    StorageError,
    entity::{ROLE, USER},
};
use quant_pivot_models::{
    domain::rbac::{AssignRoles, RoleInfo},
    entities::{
        role::{Column, Entity as RoleEntity},
        user::Entity,
        user_role::{ActiveModel, Column as UserRoleColumn, Entity as UserRoleEntity},
    },
    enums::rbac::RoleStatus,
    types::{RoleCode, RoleId, UserId},
};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder, TransactionTrait, sea_query::OnConflict,
};

use crate::{
    postgres::{
        error,
        rbac::{casbin::sync, junction},
    },
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

    if Entity::find_by_id(user_id.clone())
        .one(&txn)
        .await
        .map_err(StorageError::from)?
        .is_none()
    {
        txn.rollback().await.map_err(StorageError::from)?;
        return Err(error::not_found(USER, &user_id));
    }

    // Resolve every target role to its code, rejecting unknown ids.
    let target_roles = RoleEntity::find()
        .filter(Column::Id.is_in(target.iter().cloned()))
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
        return Err(error::not_found(ROLE, missing));
    }
    // Only *enabled* roles project a Casbin grouping (`g`): a disabled role keeps
    // its relational `user_role` membership but grants nothing until re-enabled,
    // at which point its groupings are rebuilt from that membership.
    let enabled_code_of: HashMap<RoleId, RoleCode> = target_roles
        .iter()
        .filter(|role| role.status == RoleStatus::Enabled)
        .map(|role| (role.id.clone(), role.code.clone()))
        .collect();

    let current: HashSet<RoleId> = UserRoleEntity::find()
        .filter(UserRoleColumn::UserId.eq(user_id.clone()))
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
        RoleEntity::find()
            .filter(Column::Id.is_in(removed.iter().cloned()))
            .all(&txn)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(|role| (role.id, role.code))
            .collect::<HashMap<RoleId, RoleCode>>()
    };

    if !added.is_empty() {
        let rows = added.iter().map(|role_id| ActiveModel {
            user_id: Set(user_id.clone()),
            role_id: Set(role_id.clone()),
            ..Default::default()
        });
        junction::insert_junction_rows::<UserRoleEntity>(
            &txn,
            rows,
            OnConflict::columns([UserRoleColumn::UserId, UserRoleColumn::RoleId])
                .do_nothing()
                .to_owned(),
        )
        .await?;
        let codes = added
            .iter()
            .filter_map(|role_id| enabled_code_of.get(role_id))
            .cloned()
            .collect::<Vec<_>>();
        sync::do_grant_roles(&txn, &user_id, &codes).await?;
    }

    if !removed.is_empty() {
        UserRoleEntity::delete_many()
            .filter(UserRoleColumn::UserId.eq(user_id.clone()))
            .filter(UserRoleColumn::RoleId.is_in(removed.iter().cloned()))
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        let codes = removed
            .iter()
            .filter_map(|role_id| removed_codes.get(role_id))
            .cloned()
            .collect::<Vec<_>>();
        sync::do_revoke_roles(&txn, &user_id, &codes).await?;
    }

    txn.commit().await.map_err(StorageError::from)?;
    Ok(())
}

async fn do_list_roles_for_user(
    db: &impl ConnectionTrait,
    user_id: &UserId,
) -> Result<Vec<RoleInfo>, StorageError> {
    let role_ids: Vec<RoleId> = UserRoleEntity::find()
        .filter(UserRoleColumn::UserId.eq(user_id.clone()))
        .all(db)
        .await
        .map_err(StorageError::from)?
        .into_iter()
        .map(|row| row.role_id)
        .collect();
    if role_ids.is_empty() {
        return Ok(Vec::new());
    }

    let roles = RoleEntity::find()
        .filter(Column::Id.is_in(role_ids))
        .order_by_asc(Column::Sort)
        .order_by_asc(Column::Code)
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
