//! Postgres implementation of [`RolePermissionRepository`].
//!
//! The logical layer over a role's Casbin `p` policies, with catalog validation
//! and replace-set semantics. Keyed by [`RoleId`]; the Casbin subject
//! (`role_code`) is resolved inside the transaction.

use quant_pivot_error::storage::{StorageError, entity::ROLE};
use quant_pivot_models::{
    domain::rbac::{AssignPermissions, Permission},
    entities::role::Entity,
    types::{RoleCode, RoleId},
};
use sea_orm::{ConnectionTrait, DatabaseConnection, EntityTrait, TransactionTrait};

use crate::{postgres::rbac::casbin::CasbinPolicyStore, traits::rbac::RolePermissionRepository};

/// Role→permission assignment repository backed by Postgres.
pub struct PgRolePermissionRepository {
    db: DatabaseConnection,
}

impl PgRolePermissionRepository {
    /// Create a repository over the given connection handle.
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    /// Resolve a role's immutable code inside the caller's transaction scope.
    async fn role_code(
        connection: &impl ConnectionTrait,
        id: &RoleId,
    ) -> Result<RoleCode, StorageError> {
        Entity::find_by_id(*id)
            .one(connection)
            .await
            .map_err(StorageError::from)?
            .map(|model| model.code)
            .ok_or_else(|| StorageError::not_found(ROLE, id))
    }
}

#[async_trait::async_trait]
impl RolePermissionRepository for PgRolePermissionRepository {
    async fn set_permissions_for_role(
        &self,
        assignment: AssignPermissions,
    ) -> Result<(), StorageError> {
        let AssignPermissions {
            role_id,
            permissions,
        } = assignment;

        // Defense-in-depth: reject any pair outside the permission catalog before
        // touching the database. The web layer rejects these earlier as a 400.
        if let Some(invalid) = permissions.iter().find(|permission| !permission.is_valid()) {
            return Err(StorageError::invariant_violation(
                Some(ROLE),
                format!(
                    "invalid permission for role `{role_id}`: {}:{}",
                    invalid.resource.as_str(),
                    invalid.operation.as_str()
                ),
            ));
        }

        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let role_code = Self::role_code(&txn, &role_id).await?;
        CasbinPolicyStore::new(&txn)
            .replace_role_policies(&role_code, &permissions)
            .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(())
    }

    async fn list_permissions(&self, role_id: &RoleId) -> Result<Vec<Permission>, StorageError> {
        let role_code = Self::role_code(&self.db, role_id).await?;
        CasbinPolicyStore::new(&self.db)
            .list_role_policies(&role_code)
            .await
    }
}
