//! User→role assignment repository contract.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{AssignRoles, RoleInfo},
    types::UserId,
};

/// Assignment of roles to users, kept consistent with the Casbin `g` grouping
/// rows. Uses **replace-set** semantics: the submitted `role_ids` become the
/// user's complete role set.
#[async_trait::async_trait]
pub trait UserRoleRepository: Send + Sync {
    /// Replace a user's role set. In one transaction: validate the user and
    /// every role exists, diff `user_role` (insert missing / delete extra), and
    /// mirror the change into Casbin `g = (user_id, role_code)`.
    async fn set_roles_for_user(&self, assignment: AssignRoles) -> Result<(), StorageError>;

    /// The roles currently assigned to a user, ordered by `(sort, code)`.
    async fn list_roles_for_user(&self, user_id: &UserId) -> Result<Vec<RoleInfo>, StorageError>;
}
