//! Menu repository contract.

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::rbac::{MenuInfo, MenuPatch, MenuTreeNode, NewMenu},
    types::{MenuId, RoleId},
};

/// Persistence operations for the navigation menu tree.
#[async_trait::async_trait]
pub trait MenuRepository: Send + Sync {
    /// The full menu tree, assembled into nested `MenuTreeNode`s ordered by
    /// `(parent_id, sort)`.
    async fn tree(&self) -> Result<Vec<MenuTreeNode>, StorageError>;

    /// The subtree visible to any of the given roles, with parent chains
    /// preserved so the returned forest is structurally complete.
    async fn accessible_for_roles(
        &self,
        role_ids: &[RoleId],
    ) -> Result<Vec<MenuTreeNode>, StorageError>;

    /// Fetch a menu node by id, erroring with `NotFound` when absent.
    async fn find_by_id(&self, id: &MenuId) -> Result<MenuInfo, StorageError>;

    /// Insert a new menu node.
    async fn create(&self, menu: NewMenu) -> Result<MenuInfo, StorageError>;

    /// Apply a partial update and return the refreshed row.
    async fn update(&self, id: &MenuId, patch: MenuPatch) -> Result<MenuInfo, StorageError>;

    /// Delete a leaf menu node, cascading its `role_menu` rows. Deleting a node
    /// that still has children surfaces as `Conflict`.
    async fn delete(&self, id: &MenuId) -> Result<(), StorageError>;
}
