//! Menu management API contract.

use crate::{
    domain::{
        MenuPatch,
        patch::{NullablePatch, Patch},
    },
    enums::rbac::{MenuKind, RoleStatus},
    types::MenuId,
};
use serde::Deserialize;
use serde_with::rust::double_option;
use validator::Validate;

/// Create-menu payload.
#[derive(Debug, Deserialize, Validate)]
pub struct CreateMenuRequest {
    pub parent_id: Option<MenuId>,
    #[validate(length(min = 1, max = 128))]
    pub name: String,
    pub kind: MenuKind,
    #[validate(length(max = 256))]
    pub path: Option<String>,
    #[validate(length(max = 256))]
    pub component: Option<String>,
    #[validate(length(min = 1, max = 128))]
    pub title: String,
    #[validate(length(max = 128))]
    pub icon: Option<String>,
    #[validate(length(max = 128))]
    pub permission_code: Option<String>,
    #[serde(default)]
    pub sort: i32,
    #[serde(default)]
    pub keep_alive: bool,
    #[serde(default)]
    pub hide_in_menu: bool,
    /// Defaults to enabled when omitted.
    pub status: Option<RoleStatus>,
}

/// Partial menu update. Absent fields keep; explicit `null` clears nullables.
#[derive(Debug, Deserialize, Validate)]
pub struct UpdateMenuRequest {
    #[serde(default, with = "double_option")]
    pub parent_id: Option<Option<MenuId>>,
    #[validate(length(min = 1, max = 128))]
    pub name: Option<String>,
    pub kind: Option<MenuKind>,
    #[serde(default, with = "double_option")]
    pub path: Option<Option<String>>,
    #[serde(default, with = "double_option")]
    pub component: Option<Option<String>>,
    #[validate(length(min = 1, max = 128))]
    pub title: Option<String>,
    #[serde(default, with = "double_option")]
    pub icon: Option<Option<String>>,
    #[serde(default, with = "double_option")]
    pub permission_code: Option<Option<String>>,
    pub sort: Option<i32>,
    pub keep_alive: Option<bool>,
    pub hide_in_menu: Option<bool>,
    pub status: Option<RoleStatus>,
}

impl From<UpdateMenuRequest> for MenuPatch {
    fn from(request: UpdateMenuRequest) -> Self {
        Self {
            parent_id: NullablePatch::from_nested_option(request.parent_id),
            name: Patch::from_option(request.name),
            kind: Patch::from_option(request.kind),
            path: NullablePatch::from_nested_option(request.path),
            component: NullablePatch::from_nested_option(request.component),
            title: Patch::from_option(request.title),
            icon: NullablePatch::from_nested_option(request.icon),
            permission_code: NullablePatch::from_nested_option(request.permission_code),
            sort: Patch::from_option(request.sort),
            keep_alive: Patch::from_option(request.keep_alive),
            hide_in_menu: Patch::from_option(request.hide_in_menu),
            status: Patch::from_option(request.status),
        }
    }
}
