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
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, ConnectionTrait, DatabaseConnection, DbErr,
    EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, TransactionTrait,
};

use crate::{
    postgres::{error, primitives, rbac::casbin::sync},
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

async fn do_list(db: &impl ConnectionTrait) -> Result<Vec<RoleInfo>, StorageError> {
    let models = Entity::find()
        .order_by_asc(Column::Sort)
        .order_by_asc(Column::Code)
        .all(db)
        .await
        .map_err(StorageError::from)?;
    Ok(models.into_iter().map(Into::into).collect())
}

async fn do_find_by_id(db: &impl ConnectionTrait, id: &RoleId) -> Result<RoleInfo, StorageError> {
    Entity::find_by_id(id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .map(Into::into)
        .ok_or_else(|| error::not_found(ROLE, id))
}

async fn do_find_by_code(
    db: &impl ConnectionTrait,
    code: &str,
) -> Result<Option<RoleInfo>, StorageError> {
    Ok(Entity::find()
        .filter(Column::Code.eq(code))
        .one(db)
        .await
        .map_err(StorageError::from)?
        .map(Into::into))
}

async fn do_create(db: &impl ConnectionTrait, new: NewRole) -> Result<RoleInfo, StorageError> {
    let code = new.code.clone();
    let model = Entity::insert(new.into_active_model())
        .exec_with_returning(db)
        .await
        .map_err(|error| error::map_unique(error, ROLE, code.as_str()))?;
    Ok(model.into())
}

async fn do_update(
    db: &impl ConnectionTrait,
    id: &RoleId,
    patch: RolePatch,
) -> Result<RoleInfo, StorageError> {
    let mut active = patch.into_active_model();
    active.id = Set(id.clone());
    active.updated_at = Set(Utc::now());
    match active.update(db).await {
        Ok(model) => Ok(model.into()),
        Err(DbErr::RecordNotUpdated) => Err(error::not_found(ROLE, id)),
        Err(error) => Err(StorageError::from(error)),
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
async fn do_change_status(
    db: &DatabaseConnection,
    id: &RoleId,
    status: RoleStatus,
) -> Result<(), StorageError> {
    let txn = db.begin().await.map_err(StorageError::from)?;

    let Some(role) = Entity::find_by_id(id.clone())
        .one(&txn)
        .await
        .map_err(StorageError::from)?
    else {
        txn.rollback().await.map_err(StorageError::from)?;
        return Err(error::not_found(ROLE, id));
    };

    // No-op transitions skip the policy churn entirely.
    if role.status == status {
        txn.rollback().await.map_err(StorageError::from)?;
        return Ok(());
    }

    Entity::update_many()
        .col_expr(Column::Status, primitives::enum_value(&status))
        .filter(Column::Id.eq(id.clone()))
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;

    match status {
        RoleStatus::Disabled => sync::do_revoke_role_bindings(&txn, &role.code).await?,
        RoleStatus::Enabled => {
            let holders: Vec<UserId> = UserRoleEntity::find()
                .filter(UserRoleColumn::RoleId.eq(id.clone()))
                .all(&txn)
                .await
                .map_err(StorageError::from)?
                .into_iter()
                .map(|row| row.user_id)
                .collect();
            sync::do_rebuild_role_bindings(&txn, &role.code, &holders).await?;
        }
    }

    txn.commit().await.map_err(StorageError::from)?;
    Ok(())
}

async fn do_delete(db: &DatabaseConnection, id: &RoleId) -> Result<(), StorageError> {
    let txn = db.begin().await.map_err(StorageError::from)?;

    let role = Entity::find_by_id(id.clone())
        .one(&txn)
        .await
        .map_err(StorageError::from)?;
    let Some(role) = role else {
        txn.rollback().await.map_err(StorageError::from)?;
        return Err(error::not_found(ROLE, id));
    };
    if matches!(role.kind, RoleKind::Builtin) {
        txn.rollback().await.map_err(StorageError::from)?;
        return Err(error::state_conflict(
            ROLE,
            Some(id),
            format!("built-in role cannot be deleted: {}", role.code),
        ));
    }

    RoleMenuEntity::delete_many()
        .filter(RoleMenuColumn::RoleId.eq(id.clone()))
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;
    UserRoleEntity::delete_many()
        .filter(UserRoleColumn::RoleId.eq(id.clone()))
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;
    sync::do_purge_role_code(&txn, &role.code).await?;
    Entity::delete_by_id(id.clone())
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;

    txn.commit().await.map_err(StorageError::from)?;
    Ok(())
}

#[async_trait::async_trait]
impl RoleRepository for PgRoleRepository {
    async fn list(&self) -> Result<Vec<RoleInfo>, StorageError> {
        do_list(&self.db).await
    }

    async fn find_by_id(&self, id: &RoleId) -> Result<RoleInfo, StorageError> {
        do_find_by_id(&self.db, id).await
    }

    async fn find_by_code(&self, code: &str) -> Result<Option<RoleInfo>, StorageError> {
        do_find_by_code(&self.db, code).await
    }

    async fn create(&self, role: NewRole) -> Result<RoleInfo, StorageError> {
        do_create(&self.db, role).await
    }

    async fn update(&self, id: &RoleId, patch: RolePatch) -> Result<RoleInfo, StorageError> {
        do_update(&self.db, id, patch).await
    }

    async fn change_status(&self, id: &RoleId, status: RoleStatus) -> Result<(), StorageError> {
        do_change_status(&self.db, id, status).await
    }

    async fn delete(&self, id: &RoleId) -> Result<(), StorageError> {
        do_delete(&self.db, id).await
    }
}
