//! Postgres implementation of [`RolePermissionRepository`].
//!
//! The logical layer over a role's Casbin `p` policies, with catalog validation
//! and replace-set semantics. Keyed by [`RoleId`]; the Casbin subject
//! (`role_code`) is resolved inside the transaction.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{AssignPermissions, Permission},
    entities::role,
    types::RoleId,
};
use sea_orm::{ConnectionTrait, DatabaseConnection, EntityTrait, TransactionTrait};

use crate::{
    postgres::rbac::{casbin::sync, util},
    traits::rbac::RolePermissionRepository,
};

/// Role→permission assignment repository backed by Postgres.
pub struct PgRolePermissionRepository {
    db: DatabaseConnection,
}

impl PgRolePermissionRepository {
    /// Create a repository over the given connection handle.
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

/// Resolve a role's immutable `code` by id, or `NotFound`.
async fn role_code_of(conn: &impl ConnectionTrait, id: &RoleId) -> Result<String, StorageError> {
    role::Entity::find_by_id(id.clone())
        .one(conn)
        .await
        .map_err(StorageError::from)?
        .map(|model| model.code)
        .ok_or_else(|| util::not_found("role", id))
}

async fn do_set_permissions(
    db: &DatabaseConnection,
    assignment: AssignPermissions,
) -> Result<(), StorageError> {
    let AssignPermissions {
        role_id,
        permissions,
    } = assignment;

    // Defense-in-depth: reject any pair outside the permission catalog before
    // touching the database. The web layer rejects these earlier as a 400.
    if let Some(invalid) = permissions.iter().find(|perm| !perm.is_valid()) {
        return Err(StorageError::Conflict(format!(
            "invalid permission for role `{role_id}`: {}:{}",
            invalid.resource.as_str(),
            invalid.operation.as_str()
        )));
    }

    let txn = db.begin().await.map_err(StorageError::from)?;

    let role_code = role_code_of(&txn, &role_id).await?;
    sync::do_replace_role_policies(&txn, &role_code, &permissions).await?;

    txn.commit().await.map_err(StorageError::from)?;
    Ok(())
}

async fn do_list_permissions(
    db: &DatabaseConnection,
    role_id: &RoleId,
) -> Result<Vec<Permission>, StorageError> {
    let txn = db.begin().await.map_err(StorageError::from)?;
    let role_code = role_code_of(&txn, role_id).await?;
    let permissions = sync::do_list_role_policies(&txn, &role_code).await?;
    txn.commit().await.map_err(StorageError::from)?;
    Ok(permissions)
}

#[async_trait::async_trait]
impl RolePermissionRepository for PgRolePermissionRepository {
    async fn set_permissions_for_role(
        &self,
        assignment: AssignPermissions,
    ) -> Result<(), StorageError> {
        do_set_permissions(&self.db, assignment).await
    }

    async fn list_permissions(&self, role_id: &RoleId) -> Result<Vec<Permission>, StorageError> {
        do_list_permissions(&self.db, role_id).await
    }
}
