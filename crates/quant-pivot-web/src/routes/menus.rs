//! Menu management endpoints (RBAC `Menu` resource).
//!
//! CRUD over the menu tree. The authenticated menu projection is returned by
//! `GET /api/auth/me`; menus are not part of the Casbin policy.

use actix_web::{
    http::Method,
    web::{Data, Path},
};
use quant_pivot_models::{
    domain::{
        api::{CreateMenuRequest, UpdateMenuRequest},
        rbac::{MenuInfo, MenuTreeNode, NewMenu},
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType, RoleStatus},
    },
    types::MenuId,
};

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::ValidatedJson,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Menu resource routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/menus",
            Rule::ResourceOp(ResourceType::Menu, Operation::Read),
            list,
        ),
        spec(
            Method::POST,
            "/menus",
            Rule::ResourceOp(ResourceType::Menu, Operation::Create),
            create,
        ),
        spec(
            Method::PUT,
            "/menus/{id}",
            Rule::ResourceOp(ResourceType::Menu, Operation::Update),
            update,
        ),
        spec(
            Method::DELETE,
            "/menus/{id}",
            Rule::ResourceOp(ResourceType::Menu, Operation::Delete),
            delete,
        ),
    ]
}

/// `GET /api/menus` — the full menu tree.
pub async fn list(state: Data<AppState>) -> Result<WebResponse<Vec<MenuTreeNode>>, WebError> {
    Ok(WebResponse::ok(state.menus.tree().await?))
}

/// `POST /api/menus` — create a menu node.
pub async fn create(
    state: Data<AppState>,
    op_ctx: OperationCtx,
    body: ValidatedJson<CreateMenuRequest>,
) -> Result<WebResponse<MenuInfo>, WebError> {
    let request = body.into_inner();
    let new = NewMenu {
        id: MenuId::from_v7(),
        parent_id: request.parent_id,
        name: request.name,
        kind: request.kind,
        path: request.path,
        component: request.component,
        title: request.title,
        icon: request.icon,
        permission_code: request.permission_code,
        sort: request.sort,
        keep_alive: request.keep_alive,
        hide_in_menu: request.hide_in_menu,
        affix_tab: request.affix_tab,
        status: request.status.unwrap_or(RoleStatus::Enabled),
    };
    let menu = state.menus.create(new).await?;
    op_ctx.set_action(OperationCategory::Rbac, "menu.create");
    op_ctx.set_resource(ResourceType::Menu, menu.id.to_string());
    Ok(WebResponse::ok(menu))
}

/// `PUT /api/menus/{id}` — partial menu update.
pub async fn update(
    state: Data<AppState>,
    id: Path<MenuId>,
    op_ctx: OperationCtx,
    body: ValidatedJson<UpdateMenuRequest>,
) -> Result<WebResponse<MenuInfo>, WebError> {
    let menu = state.menus.update(&id, body.into_inner().into()).await?;
    op_ctx.set_action(OperationCategory::Rbac, "menu.update");
    op_ctx.set_resource(ResourceType::Menu, id.to_string());
    Ok(WebResponse::ok(menu))
}

/// `DELETE /api/menus/{id}` — delete a menu node.
pub async fn delete(
    state: Data<AppState>,
    id: Path<MenuId>,
    op_ctx: OperationCtx,
) -> Result<WebResponse<()>, WebError> {
    state.menus.delete(&id).await?;
    op_ctx.set_action(OperationCategory::Rbac, "menu.delete");
    op_ctx.set_resource(ResourceType::Menu, id.to_string());
    Ok(WebResponse::ok(()))
}
