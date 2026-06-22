//! Menu management endpoints (RBAC `Menu` resource).
//!
//! CRUD over the menu tree, plus the self-service `accessible` projection that
//! returns only the menus the caller's **enabled** roles grant. Menus are not
//! part of the Casbin policy, so none of these endpoints touch the enforcer.

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{CreateMenuRequest, MenuInfo, MenuTreeNode, NewMenu, UpdateMenuRequest},
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
    extractors::{AuthedActor, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Menu resource routes (the static `accessible` precedes the dynamic `{id}`).
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
            Method::GET,
            "/menus/accessible",
            Rule::AuthenticatedOnly,
            accessible,
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
pub async fn list(state: web::Data<AppState>) -> Result<WebResponse<Vec<MenuTreeNode>>, WebError> {
    Ok(WebResponse::ok(state.menus.tree().await?))
}

/// `POST /api/menus` — create a menu node.
pub async fn create(
    state: web::Data<AppState>,
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
    state: web::Data<AppState>,
    id: web::Path<MenuId>,
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
    state: web::Data<AppState>,
    id: web::Path<MenuId>,
    op_ctx: OperationCtx,
) -> Result<WebResponse<()>, WebError> {
    state.menus.delete(&id).await?;
    op_ctx.set_action(OperationCategory::Rbac, "menu.delete");
    op_ctx.set_resource(ResourceType::Menu, id.to_string());
    Ok(WebResponse::ok(()))
}

/// `GET /api/menus/accessible` — the menu tree the caller's enabled roles grant.
pub async fn accessible(
    state: web::Data<AppState>,
    actor: AuthedActor,
) -> Result<WebResponse<Vec<MenuTreeNode>>, WebError> {
    let menus = state
        .menus
        .accessible_for_roles(&actor.roles.enabled_ids())
        .await?;
    Ok(WebResponse::ok(menus))
}
