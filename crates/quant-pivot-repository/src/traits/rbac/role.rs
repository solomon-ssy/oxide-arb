//! Role repository contract.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::rbac::{NewRole, RoleInfo, RolePatch},
    enums::rbac::RoleStatus,
    types::RoleId,
};

/// Persistence operations for roles. `code` is the immutable Casbin subject and
/// is never updated after creation.
#[async_trait::async_trait]
pub trait RoleRepository: Send + Sync {
    /// All roles ordered by `(sort, code)`.
    async fn list(&self) -> Result<Vec<RoleInfo>, StorageError>;

    /// Fetch a role by id, erroring with `NotFound` when absent.
    async fn find_by_id(&self, id: &RoleId) -> Result<RoleInfo, StorageError>;

    /// Look up a role by its unique code, or `None` if absent.
    async fn find_by_code(&self, code: &str) -> Result<Option<RoleInfo>, StorageError>;

    /// Insert a new role. A duplicate code surfaces as `Conflict`.
    async fn create(&self, role: NewRole) -> Result<RoleInfo, StorageError>;

    /// Apply a partial update (name/description/status/sort) and return the row.
    async fn update(&self, id: &RoleId, patch: RolePatch) -> Result<RoleInfo, StorageError>;

    /// Toggle the role status flag.
    async fn change_status(&self, id: &RoleId, status: RoleStatus) -> Result<(), StorageError>;

    /// Delete a role, cascading `role_menu`, `user_role`, and its Casbin `p`/`g`
    /// rows in the same transaction. Built-in roles cannot be deleted and
    /// surface as `Conflict`.
    async fn delete(&self, id: &RoleId) -> Result<(), StorageError>;
}
