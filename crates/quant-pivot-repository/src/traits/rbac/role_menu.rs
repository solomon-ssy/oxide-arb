//! Role→menu assignment repository contract.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::rbac::{AssignMenus, MenuInfo},
    types::RoleId,
};

/// Assignment of menus to roles. Uses **replace-set** semantics: the submitted
/// `menu_ids` become the role's complete visible menu set.
#[async_trait::async_trait]
pub trait RoleMenuRepository: Send + Sync {
    /// Replace a role's menu set in one transaction: validate the role and every
    /// menu exists, then diff `role_menu` (insert missing / delete extra).
    async fn set_menus_for_role(&self, assignment: AssignMenus) -> Result<(), StorageError>;

    /// The menus currently visible to a role, ordered by `(parent_id, sort)`.
    async fn list_menus_for_role(&self, role_id: &RoleId) -> Result<Vec<MenuInfo>, StorageError>;
}
