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
    ActiveValue::Set, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder,
    TransactionTrait, sea_query::OnConflict,
};

use crate::{
    postgres::rbac::{casbin::CasbinPolicyStore, junction},
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

#[async_trait::async_trait]
impl UserRoleRepository for PgUserRoleRepository {
    async fn set_roles_for_user(&self, assignment: AssignRoles) -> Result<(), StorageError> {
        let AssignRoles { user_id, role_ids } = assignment;
        let target = role_ids.into_iter().collect::<HashSet<RoleId>>();
        let txn = self.db.begin().await.map_err(StorageError::from)?;

        if Entity::find_by_id(user_id)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .is_none()
        {
            txn.rollback().await.map_err(StorageError::from)?;
            return Err(StorageError::not_found(USER, user_id));
        }

        let target_roles = RoleEntity::find()
            .filter(Column::Id.is_in(target.iter().copied()))
            .all(&txn)
            .await
            .map_err(StorageError::from)?;
        if target_roles.len() != target.len() {
            let found = target_roles
                .iter()
                .map(|role| role.id)
                .collect::<HashSet<RoleId>>();
            let missing = target
                .iter()
                .find(|id| !found.contains(id))
                .map_or_else(|| "<unknown>".to_owned(), ToString::to_string);
            txn.rollback().await.map_err(StorageError::from)?;
            return Err(StorageError::not_found(ROLE, missing));
        }

        // Disabled roles retain relational membership but project no Casbin group.
        let enabled_code_of = target_roles
            .iter()
            .filter(|role| role.status == RoleStatus::Enabled)
            .map(|role| (role.id, role.code.clone()))
            .collect::<HashMap<RoleId, RoleCode>>();
        let current = UserRoleEntity::find()
            .filter(UserRoleColumn::UserId.eq(user_id))
            .all(&txn)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(|row| row.role_id)
            .collect::<HashSet<RoleId>>();
        let (added, removed) = junction::replace_set_diff(&target, &current);

        let removed_codes = if removed.is_empty() {
            HashMap::new()
        } else {
            RoleEntity::find()
                .filter(Column::Id.is_in(removed.iter().copied()))
                .all(&txn)
                .await
                .map_err(StorageError::from)?
                .into_iter()
                .map(|role| (role.id, role.code))
                .collect::<HashMap<RoleId, RoleCode>>()
        };
        let policies = CasbinPolicyStore::new(&txn);

        if !added.is_empty() {
            let rows = added.iter().map(|role_id| ActiveModel {
                user_id: Set(user_id),
                role_id: Set(*role_id),
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
            policies.grant_roles(&user_id, &codes).await?;
        }

        if !removed.is_empty() {
            UserRoleEntity::delete_many()
                .filter(UserRoleColumn::UserId.eq(user_id))
                .filter(UserRoleColumn::RoleId.is_in(removed.iter().copied()))
                .exec(&txn)
                .await
                .map_err(StorageError::from)?;
            let codes = removed
                .iter()
                .filter_map(|role_id| removed_codes.get(role_id))
                .cloned()
                .collect::<Vec<_>>();
            policies.revoke_roles(&user_id, &codes).await?;
        }

        txn.commit().await.map_err(StorageError::from)?;
        Ok(())
    }

    async fn list_roles_for_user(&self, user_id: &UserId) -> Result<Vec<RoleInfo>, StorageError> {
        let role_ids = UserRoleEntity::find()
            .filter(UserRoleColumn::UserId.eq(*user_id))
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(|row| row.role_id)
            .collect::<Vec<RoleId>>();
        if role_ids.is_empty() {
            return Ok(Vec::new());
        }

        let roles = RoleEntity::find()
            .filter(Column::Id.is_in(role_ids))
            .order_by_asc(Column::Sort)
            .order_by_asc(Column::Code)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(roles.into_iter().map(Into::into).collect())
    }
}
