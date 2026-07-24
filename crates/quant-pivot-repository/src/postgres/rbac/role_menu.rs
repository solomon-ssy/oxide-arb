//! Postgres implementation of [`RoleMenuRepository`] with replace-set semantics.

use std::collections::HashSet;

use quant_pivot_error::storage::{
    StorageError,
    entity::{MENU, ROLE},
};
use quant_pivot_models::{
    domain::rbac::{AssignMenus, MenuInfo},
    entities::{
        menu::{Column, Entity as MenuEntity},
        role::Entity,
        role_menu::{ActiveModel, Column as RoleMenuColumn, Entity as RoleMenuEntity},
    },
    types::{MenuId, RoleId},
};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, TransactionTrait, sea_query::OnConflict,
};

use crate::{postgres::rbac::junction, traits::rbac::RoleMenuRepository};

/// Role→menu assignment repository backed by Postgres.
pub struct PgRoleMenuRepository {
    db: DatabaseConnection,
}

impl PgRoleMenuRepository {
    /// Create a repository over the given connection handle.
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl RoleMenuRepository for PgRoleMenuRepository {
    async fn set_menus_for_role(&self, assignment: AssignMenus) -> Result<(), StorageError> {
        let AssignMenus { role_id, menu_ids } = assignment;
        let target = menu_ids.into_iter().collect::<HashSet<MenuId>>();
        let txn = self.db.begin().await.map_err(StorageError::from)?;

        if Entity::find_by_id(role_id)
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .is_none()
        {
            txn.rollback().await.map_err(StorageError::from)?;
            return Err(StorageError::not_found(ROLE, role_id));
        }

        if !target.is_empty() {
            let present = MenuEntity::find()
                .filter(Column::Id.is_in(target.iter().copied()))
                .count(&txn)
                .await
                .map_err(StorageError::from)?;
            if present != target.len() as u64 {
                txn.rollback().await.map_err(StorageError::from)?;
                return Err(StorageError::not_found(MENU, "<one or more menu ids>"));
            }
        }

        let current = RoleMenuEntity::find()
            .filter(RoleMenuColumn::RoleId.eq(role_id))
            .all(&txn)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(|row| row.menu_id)
            .collect::<HashSet<MenuId>>();
        let (added, removed) = junction::replace_set_diff(&target, &current);

        if !added.is_empty() {
            let rows = added.iter().map(|menu_id| ActiveModel {
                role_id: Set(role_id),
                menu_id: Set(*menu_id),
                ..Default::default()
            });
            junction::insert_junction_rows::<RoleMenuEntity>(
                &txn,
                rows,
                OnConflict::columns([RoleMenuColumn::RoleId, RoleMenuColumn::MenuId])
                    .do_nothing()
                    .to_owned(),
            )
            .await?;
        }

        if !removed.is_empty() {
            RoleMenuEntity::delete_many()
                .filter(RoleMenuColumn::RoleId.eq(role_id))
                .filter(RoleMenuColumn::MenuId.is_in(removed))
                .exec(&txn)
                .await
                .map_err(StorageError::from)?;
        }

        txn.commit().await.map_err(StorageError::from)?;
        Ok(())
    }

    async fn list_menus_for_role(&self, role_id: &RoleId) -> Result<Vec<MenuInfo>, StorageError> {
        let menu_ids = RoleMenuEntity::find()
            .filter(RoleMenuColumn::RoleId.eq(*role_id))
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(|row| row.menu_id)
            .collect::<Vec<MenuId>>();
        if menu_ids.is_empty() {
            return Ok(Vec::new());
        }

        let menus = MenuEntity::find()
            .filter(Column::Id.is_in(menu_ids))
            .order_by_asc(Column::Sort)
            .order_by_asc(Column::Id)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(menus.into_iter().map(Into::into).collect())
    }
}
