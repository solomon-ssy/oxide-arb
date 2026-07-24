//! Postgres implementation of [`RoleRepository`].

use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity::ROLE};
use quant_pivot_models::{
    domain::rbac::{NewRole, RoleInfo, RolePatch},
    entities::{
        role::{Column, Entity},
        role_menu::{Column as RoleMenuColumn, Entity as RoleMenuEntity},
        user_role::{Column as UserRoleColumn, Entity as UserRoleEntity},
    },
    enums::rbac::{RoleKind, RoleStatus},
    types::{RoleId, UserId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseConnection, DbErr, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, TransactionTrait,
};

use crate::{
    postgres::{error, primitives, rbac::casbin::CasbinPolicyStore},
    traits::rbac::RoleRepository,
};

/// Role repository backed by Postgres.
pub struct PgRoleRepository {
    db: DatabaseConnection,
}

impl PgRoleRepository {
    /// Create a repository over the given connection handle.
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

/// Transition a role's status, keeping Casbin grouping (`g`) bindings in lockstep
/// so the change takes effect the instant the enforcer reloads.
///
/// - **Disable** drops every `g` binding for the role (its `p` permissions and
///   `user_role` membership survive), so no subject resolves the role anymore.
/// - **Enable** rebuilds `g` from the surviving `user_role` membership.
///
/// All of it runs in one transaction, so the relational status column and the
/// policy table can never diverge.
#[async_trait::async_trait]
impl RoleRepository for PgRoleRepository {
    async fn list(&self) -> Result<Vec<RoleInfo>, StorageError> {
        let roles = Entity::find()
            .order_by_asc(Column::Sort)
            .order_by_asc(Column::Code)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(roles.into_iter().map(Into::into).collect())
    }

    async fn find_by_id(&self, id: &RoleId) -> Result<RoleInfo, StorageError> {
        Entity::find_by_id(*id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Into::into)
            .ok_or_else(|| StorageError::not_found(ROLE, id))
    }

    async fn find_by_code(&self, code: &str) -> Result<Option<RoleInfo>, StorageError> {
        Ok(Entity::find()
            .filter(Column::Code.eq(code))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Into::into))
    }

    async fn create(&self, role: NewRole) -> Result<RoleInfo, StorageError> {
        let code = role.code.clone();
        let model = Entity::insert(role.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(|error| error::map_unique(error, ROLE, code.as_str()))?;
        Ok(model.into())
    }

    async fn update(&self, id: &RoleId, patch: RolePatch) -> Result<RoleInfo, StorageError> {
        let mut active = patch.into_active_model();
        active.id = Set(*id);
        active.updated_at = Set(Utc::now());
        match active.update(&self.db).await {
            Ok(model) => Ok(model.into()),
            Err(DbErr::RecordNotUpdated) => Err(StorageError::not_found(ROLE, id)),
            Err(error) => Err(StorageError::from(error)),
        }
    }

    async fn change_status(&self, id: &RoleId, status: RoleStatus) -> Result<(), StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let Some(role) = Entity::find_by_id(*id)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            txn.rollback().await.map_err(StorageError::from)?;
            return Err(StorageError::not_found(ROLE, id));
        };

        if role.status == status {
            txn.rollback().await.map_err(StorageError::from)?;
            return Ok(());
        }

        Entity::update_many()
            .col_expr(Column::Status, primitives::enum_value(&status))
            .filter(Column::Id.eq(*id))
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;

        let policies = CasbinPolicyStore::new(&txn);
        match status {
            RoleStatus::Disabled => policies.revoke_role_bindings(&role.code).await?,
            RoleStatus::Enabled => {
                let holders = UserRoleEntity::find()
                    .filter(UserRoleColumn::RoleId.eq(*id))
                    .all(&txn)
                    .await
                    .map_err(StorageError::from)?
                    .into_iter()
                    .map(|row| row.user_id)
                    .collect::<Vec<UserId>>();
                policies.rebuild_role_bindings(&role.code, &holders).await?;
            }
        }

        txn.commit().await.map_err(StorageError::from)?;
        Ok(())
    }

    async fn delete(&self, id: &RoleId) -> Result<(), StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let Some(role) = Entity::find_by_id(*id)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            txn.rollback().await.map_err(StorageError::from)?;
            return Err(StorageError::not_found(ROLE, id));
        };
        if matches!(role.kind, RoleKind::Builtin) {
            txn.rollback().await.map_err(StorageError::from)?;
            return Err(StorageError::state_conflict(
                ROLE,
                Some(id),
                format!("built-in role cannot be deleted: {}", role.code),
            ));
        }

        RoleMenuEntity::delete_many()
            .filter(RoleMenuColumn::RoleId.eq(*id))
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        UserRoleEntity::delete_many()
            .filter(UserRoleColumn::RoleId.eq(*id))
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        CasbinPolicyStore::new(&txn)
            .purge_role_code(&role.code)
            .await?;
        Entity::delete_by_id(*id)
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(())
    }
}
