//! Menu DTOs (read model, insert, partial update, tree projection).

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    domain::patch::{NullablePatch, Patch},
    enums::rbac::{MenuKind, RoleStatus},
    types::MenuId,
};

/// DB row projection for the `menu` table.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::menu::Entity")]
pub struct MenuInfo {
    pub id: MenuId,
    pub parent_id: Option<MenuId>,
    pub name: String,
    pub kind: MenuKind,
    pub path: Option<String>,
    pub component: Option<String>,
    pub title: String,
    pub icon: Option<String>,
    pub permission_code: Option<String>,
    pub sort: i32,
    pub keep_alive: bool,
    pub hide_in_menu: bool,
    pub affix_tab: bool,
    pub status: RoleStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(MenuInfo, crate::entities::menu::Model, {
    id, parent_id, name, kind, path, component, title, icon, permission_code,
    sort, keep_alive, hide_in_menu, affix_tab, status, created_at, updated_at,
});

/// Insert payload for a new menu node.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::menu::ActiveModel")]
pub struct NewMenu {
    pub id: MenuId,
    pub parent_id: Option<MenuId>,
    pub name: String,
    pub kind: MenuKind,
    pub path: Option<String>,
    pub component: Option<String>,
    pub title: String,
    pub icon: Option<String>,
    pub permission_code: Option<String>,
    pub sort: i32,
    pub keep_alive: bool,
    pub hide_in_menu: bool,
    pub affix_tab: bool,
    pub status: RoleStatus,
}

/// Partial update for a menu node.
#[derive(Debug, Clone, Default, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::menu::ActiveModel")]
pub struct MenuPatch {
    pub parent_id: NullablePatch<MenuId>,
    pub name: Patch<String>,
    pub kind: Patch<MenuKind>,
    pub path: NullablePatch<String>,
    pub component: NullablePatch<String>,
    pub title: Patch<String>,
    pub icon: NullablePatch<String>,
    pub permission_code: NullablePatch<String>,
    pub sort: Patch<i32>,
    pub keep_alive: Patch<bool>,
    pub hide_in_menu: Patch<bool>,
    pub affix_tab: Patch<bool>,
    pub status: Patch<RoleStatus>,
}

/// A menu node with its descendants — the nested form returned by tree/menu
/// accessibility endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MenuTreeNode {
    #[serde(flatten)]
    pub menu: MenuInfo,
    pub children: Vec<Self>,
}

impl MenuTreeNode {
    /// Create a leaf node (no children).
    #[must_use]
    pub const fn leaf(menu: MenuInfo) -> Self {
        Self {
            menu,
            children: Vec::new(),
        }
    }
}
