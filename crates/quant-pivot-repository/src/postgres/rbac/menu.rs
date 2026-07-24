//! Postgres implementation of [`MenuRepository`].

use std::collections::{HashMap, HashSet};

use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity::MENU};
use quant_pivot_models::{
    domain::rbac::{MenuInfo, MenuPatch, MenuTreeNode, NewMenu},
    entities::{
        menu::{Column, Entity},
        role_menu::{Column as RoleMenuColumn, Entity as RoleMenuEntity},
    },
    types::{MenuId, RoleId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue::Set, ColumnTrait, DatabaseConnection, DbErr, EntityTrait,
    IntoActiveModel, PaginatorTrait, QueryFilter, QueryOrder, TransactionTrait,
};

use crate::traits::rbac::MenuRepository;

/// Menu repository backed by Postgres.
pub struct PgMenuRepository {
    db: DatabaseConnection,
}

impl PgMenuRepository {
    /// Create a repository over the given connection handle.
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    /// Fetch every menu row ordered by `(sort, id)`.
    async fn load_all(&self) -> Result<Vec<MenuInfo>, StorageError> {
        let menus = Entity::find()
            .order_by_asc(Column::Sort)
            .order_by_asc(Column::Id)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(menus.into_iter().map(Into::into).collect())
    }
}

/// Assemble a flat, pre-sorted menu list into nested tree nodes. Children
/// inherit their parent's encounter order, preserving the `(sort, id)` ordering.
fn assemble_tree(menus: Vec<MenuInfo>) -> Vec<MenuTreeNode> {
    let mut children: HashMap<MenuId, Vec<MenuInfo>> = HashMap::new();
    let mut roots: Vec<MenuInfo> = Vec::new();
    for node in menus {
        match node.parent_id {
            Some(parent) => children.entry(parent).or_default().push(node),
            None => roots.push(node),
        }
    }
    roots
        .into_iter()
        .map(|root| build_node(root, &mut children))
        .collect()
}

fn build_node(node: MenuInfo, children: &mut HashMap<MenuId, Vec<MenuInfo>>) -> MenuTreeNode {
    let kids = children.remove(&node.id).unwrap_or_default();
    let nested = kids
        .into_iter()
        .map(|child| build_node(child, children))
        .collect();
    MenuTreeNode {
        menu: node,
        children: nested,
    }
}

#[async_trait::async_trait]
impl MenuRepository for PgMenuRepository {
    async fn tree(&self) -> Result<Vec<MenuTreeNode>, StorageError> {
        Ok(assemble_tree(self.load_all().await?))
    }

    async fn accessible_for_roles(
        &self,
        role_ids: &[RoleId],
    ) -> Result<Vec<MenuTreeNode>, StorageError> {
        if role_ids.is_empty() {
            return Ok(Vec::new());
        }

        let granted = RoleMenuEntity::find()
            .filter(RoleMenuColumn::RoleId.is_in(role_ids.iter().copied()))
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        if granted.is_empty() {
            return Ok(Vec::new());
        }

        let all = self.load_all().await?;
        let parent_of = all
            .iter()
            .map(|menu| (menu.id, menu.parent_id))
            .collect::<HashMap<MenuId, Option<MenuId>>>();

        // Include each granted menu plus its full ancestor chain so the returned
        // forest is structurally complete (no orphaned children).
        let mut included = HashSet::<MenuId>::new();
        for row in &granted {
            let mut cursor = Some(row.menu_id);
            while let Some(id) = cursor {
                if !included.insert(id) {
                    break;
                }
                cursor = parent_of.get(&id).and_then(Clone::clone);
            }
        }

        let filtered = all
            .into_iter()
            .filter(|menu| included.contains(&menu.id))
            .collect();
        Ok(assemble_tree(filtered))
    }

    async fn find_by_id(&self, id: &MenuId) -> Result<MenuInfo, StorageError> {
        Entity::find_by_id(*id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Into::into)
            .ok_or_else(|| StorageError::not_found(MENU, id))
    }

    async fn create(&self, menu: NewMenu) -> Result<MenuInfo, StorageError> {
        let model = Entity::insert(menu.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(model.into())
    }

    async fn update(&self, id: &MenuId, patch: MenuPatch) -> Result<MenuInfo, StorageError> {
        let mut active = patch.into_active_model();
        active.id = Set(*id);
        active.updated_at = Set(Utc::now());
        match active.update(&self.db).await {
            Ok(model) => Ok(model.into()),
            Err(DbErr::RecordNotUpdated) => Err(StorageError::not_found(MENU, id)),
            Err(error) => Err(StorageError::from(error)),
        }
    }

    async fn delete(&self, id: &MenuId) -> Result<(), StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let child_count = Entity::find()
            .filter(Column::ParentId.eq(*id))
            .count(&txn)
            .await
            .map_err(StorageError::from)?;
        if child_count > 0 {
            txn.rollback().await.map_err(StorageError::from)?;
            return Err(StorageError::state_conflict(
                MENU,
                Some(id),
                format!("menu has {child_count} child node(s) and cannot be deleted"),
            ));
        }

        RoleMenuEntity::delete_many()
            .filter(RoleMenuColumn::MenuId.eq(*id))
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        let result = Entity::delete_by_id(*id)
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        if result.rows_affected == 0 {
            txn.rollback().await.map_err(StorageError::from)?;
            return Err(StorageError::not_found(MENU, id));
        }

        txn.commit().await.map_err(StorageError::from)?;
        Ok(())
    }
}
