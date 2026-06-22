//! Role→permission assignment repository contract (Casbin `p` policies).

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{AssignPermissions, Permission},
    types::RoleId,
};

/// Logical layer over the Casbin `p` policies for a role.
///
/// Keyed by [`RoleId`] for symmetry with the other assignment repositories and
/// the `/roles/{id}/permissions` route; the immutable `role_code` (the Casbin
/// subject) is resolved inside the transaction. Permissions are validated
/// against `RESOURCE_OPERATIONS` before any write, so no phantom
/// `resource × operation` combination can be persisted. Uses **replace-set**
/// semantics.
#[async_trait::async_trait]
pub trait RolePermissionRepository: Send + Sync {
    /// Replace a role's permission set. Validates every pair against the
    /// permission catalog, then atomically swaps the role's `p` rows.
    async fn set_permissions_for_role(
        &self,
        assignment: AssignPermissions,
    ) -> Result<(), StorageError>;

    /// The permissions currently granted to a role, parsed back from the stored
    /// Casbin `p` rows.
    async fn list_permissions(&self, role_id: &RoleId) -> Result<Vec<Permission>, StorageError>;
}
