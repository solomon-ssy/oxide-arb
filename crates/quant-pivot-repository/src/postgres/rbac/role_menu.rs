//! Postgres implementation of [`RoleMenuRepository`] with replace-set semantics.

use std::collections::HashSet;

use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{AssignMenus, MenuInfo},
    entities::{menu, role, role_menu},
    types::{MenuId, RoleId},
};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait,
    PaginatorTrait, QueryFilter, QueryOrder, TransactionTrait, sea_query::OnConflict,
};

use crate::{postgres::rbac::util, traits::rbac::RoleMenuRepository};

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

async fn do_set_menus(
    db: &DatabaseConnection,
    assignment: AssignMenus,
) -> Result<(), StorageError> {
    let AssignMenus { role_id, menu_ids } = assignment;
    let target: HashSet<MenuId> = menu_ids.into_iter().collect();

    let txn = db.begin().await.map_err(StorageError::from)?;

    if role::Entity::find_by_id(role_id.clone())
        .one(&txn)
        .await
        .map_err(StorageError::from)?
        .is_none()
    {
        txn.rollback().await.map_err(StorageError::from)?;
        return Err(util::not_found("role", &role_id));
    }

    if !target.is_empty() {
        let present = menu::Entity::find()
            .filter(menu::Column::Id.is_in(target.iter().cloned()))
            .count(&txn)
            .await
            .map_err(StorageError::from)?;
        if present != target.len() as u64 {
            txn.rollback().await.map_err(StorageError::from)?;
            return Err(util::not_found("menu", "<one or more menu ids>"));
        }
    }

    let current: HashSet<MenuId> = role_menu::Entity::find()
        .filter(role_menu::Column::RoleId.eq(role_id.clone()))
        .all(&txn)
        .await
        .map_err(StorageError::from)?
        .into_iter()
        .map(|row| row.menu_id)
        .collect();

    let added: Vec<MenuId> = target.difference(&current).cloned().collect();
    let removed: Vec<MenuId> = current.difference(&target).cloned().collect();

    if !added.is_empty() {
        let rows = added.iter().map(|menu_id| role_menu::ActiveModel {
            role_id: Set(role_id.clone()),
            menu_id: Set(menu_id.clone()),
            ..Default::default()
        });
        role_menu::Entity::insert_many(rows)
            .on_conflict(
                OnConflict::columns([role_menu::Column::RoleId, role_menu::Column::MenuId])
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&txn)
            .await
            .map_err(StorageError::from)?;
    }

    if !removed.is_empty() {
        role_menu::Entity::delete_many()
            .filter(role_menu::Column::RoleId.eq(role_id.clone()))
            .filter(role_menu::Column::MenuId.is_in(removed))
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
    }

    txn.commit().await.map_err(StorageError::from)?;
    Ok(())
}

async fn do_list_menus_for_role(
    db: &impl ConnectionTrait,
    role_id: &RoleId,
) -> Result<Vec<MenuInfo>, StorageError> {
    let menu_ids: Vec<MenuId> = role_menu::Entity::find()
        .filter(role_menu::Column::RoleId.eq(role_id.clone()))
        .all(db)
        .await
        .map_err(StorageError::from)?
        .into_iter()
        .map(|row| row.menu_id)
        .collect();
    if menu_ids.is_empty() {
        return Ok(Vec::new());
    }

    let menus = menu::Entity::find()
        .filter(menu::Column::Id.is_in(menu_ids))
        .order_by_asc(menu::Column::Sort)
        .order_by_asc(menu::Column::Id)
        .all(db)
        .await
        .map_err(StorageError::from)?;
    Ok(menus.into_iter().map(Into::into).collect())
}

#[async_trait::async_trait]
impl RoleMenuRepository for PgRoleMenuRepository {
    async fn set_menus_for_role(&self, assignment: AssignMenus) -> Result<(), StorageError> {
        do_set_menus(&self.db, assignment).await
    }

    async fn list_menus_for_role(&self, role_id: &RoleId) -> Result<Vec<MenuInfo>, StorageError> {
        do_list_menus_for_role(&self.db, role_id).await
    }
}
