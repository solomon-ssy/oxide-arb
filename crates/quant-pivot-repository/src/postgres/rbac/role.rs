//! Postgres implementation of [`RoleRepository`].

use chrono::Utc;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{NewRole, RoleInfo, RolePatch},
    entities::{role, role_menu, user_role},
    enums::rbac::{RoleKind, RoleStatus},
    schema::column,
    types::{RoleId, UserId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, ConnectionTrait, DatabaseConnection,
    EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, TransactionTrait,
};

use crate::{
    postgres::rbac::{casbin::sync, util},
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
    let models = role::Entity::find()
        .order_by_asc(role::Column::Sort)
        .order_by_asc(role::Column::Code)
        .all(db)
        .await
        .map_err(StorageError::from)?;
    Ok(models.into_iter().map(Into::into).collect())
}

async fn do_find_by_id(db: &impl ConnectionTrait, id: &RoleId) -> Result<RoleInfo, StorageError> {
    role::Entity::find_by_id(id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .map(Into::into)
        .ok_or_else(|| util::not_found("role", id))
}

async fn do_find_by_code(
    db: &impl ConnectionTrait,
    code: &str,
) -> Result<Option<RoleInfo>, StorageError> {
    Ok(role::Entity::find()
        .filter(role::Column::Code.eq(code))
        .one(db)
        .await
        .map_err(StorageError::from)?
        .map(Into::into))
}

async fn do_create(db: &impl ConnectionTrait, new: NewRole) -> Result<RoleInfo, StorageError> {
    let code = new.code.clone();
    let model = role::Entity::insert(new.into_active_model())
        .exec_with_returning(db)
        .await
        .map_err(|error| util::map_unique(error, "role", &code))?;
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
        Err(sea_orm::DbErr::RecordNotUpdated) => Err(util::not_found("role", id)),
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

    let Some(role) = role::Entity::find_by_id(id.clone())
        .one(&txn)
        .await
        .map_err(StorageError::from)?
    else {
        txn.rollback().await.map_err(StorageError::from)?;
        return Err(util::not_found("role", id));
    };

    // No-op transitions skip the policy churn entirely.
    if role.status == status {
        txn.rollback().await.map_err(StorageError::from)?;
        return Ok(());
    }

    role::Entity::update_many()
        .col_expr(role::Column::Status, column::pg_enum_value(&status))
        .filter(role::Column::Id.eq(id.clone()))
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;

    match status {
        RoleStatus::Disabled => sync::do_revoke_role_bindings(&txn, &role.code).await?,
        RoleStatus::Enabled => {
            let holders: Vec<UserId> = user_role::Entity::find()
                .filter(user_role::Column::RoleId.eq(id.clone()))
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

    let role = role::Entity::find_by_id(id.clone())
        .one(&txn)
        .await
        .map_err(StorageError::from)?;
    let Some(role) = role else {
        txn.rollback().await.map_err(StorageError::from)?;
        return Err(util::not_found("role", id));
    };
    if matches!(role.kind, RoleKind::Builtin) {
        txn.rollback().await.map_err(StorageError::from)?;
        return Err(StorageError::Conflict(format!(
            "built-in role cannot be deleted: {}",
            role.code
        )));
    }

    role_menu::Entity::delete_many()
        .filter(role_menu::Column::RoleId.eq(id.clone()))
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;
    user_role::Entity::delete_many()
        .filter(user_role::Column::RoleId.eq(id.clone()))
        .exec(&txn)
        .await
        .map_err(StorageError::from)?;
    sync::do_purge_role_code(&txn, &role.code).await?;
    role::Entity::delete_by_id(id.clone())
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
