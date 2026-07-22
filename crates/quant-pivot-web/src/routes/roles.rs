//! Role management endpoints (RBAC `Role` resource).
//!
//! CRUD plus permission and menu assignment. API-created roles are always
//! `Custom` (built-in roles are seeded and protected from deletion). Status
//! transitions and permission assignment mutate the Casbin policy table inside
//! the repository transaction, so each is followed by an enforcer reload.

use actix_web::{
    http::Method,
    web::{Data, Path},
};
use quant_pivot_models::{
    domain::{
        api::{
            AssignMenusRequest, AssignPermissionsRequest, ChangeRoleStatusRequest,
            CreateRoleRequest, UpdateRoleRequest,
        },
        rbac::{AssignMenus, AssignPermissions, MenuInfo, NewRole, Permission, RoleInfo},
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType, RoleKind, RoleStatus},
    },
    types::{RoleCode, RoleId},
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

/// Role resource routes (CRUD + status + permission/menu assignment).
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/roles",
            Rule::ResourceOp(ResourceType::Role, Operation::Read),
            list,
        ),
        spec(
            Method::POST,
            "/roles",
            Rule::ResourceOp(ResourceType::Role, Operation::Create),
            create,
        ),
        spec(
            Method::GET,
            "/roles/{id}",
            Rule::ResourceOp(ResourceType::Role, Operation::Read),
            get,
        ),
        spec(
            Method::PUT,
            "/roles/{id}",
            Rule::ResourceOp(ResourceType::Role, Operation::Update),
            update,
        ),
        spec(
            Method::DELETE,
            "/roles/{id}",
            Rule::ResourceOp(ResourceType::Role, Operation::Delete),
            delete,
        ),
        spec(
            Method::PUT,
            "/roles/{id}/status",
            Rule::ResourceOp(ResourceType::Role, Operation::Update),
            change_status,
        ),
        spec(
            Method::GET,
            "/roles/{id}/permissions",
            Rule::ResourceOp(ResourceType::Permission, Operation::Read),
            list_permissions,
        ),
        spec(
            Method::PUT,
            "/roles/{id}/permissions",
            Rule::ResourceOp(ResourceType::Role, Operation::Assign),
            set_permissions,
        ),
        spec(
            Method::GET,
            "/roles/{id}/menus",
            Rule::ResourceOp(ResourceType::Role, Operation::Read),
            list_menus,
        ),
        spec(
            Method::PUT,
            "/roles/{id}/menus",
            Rule::ResourceOp(ResourceType::Role, Operation::Assign),
            set_menus,
        ),
    ]
}

/// `GET /api/roles` — the full role catalog (ordered by sort, then code).
pub async fn list(state: Data<AppState>) -> Result<WebResponse<Vec<RoleInfo>>, WebError> {
    Ok(WebResponse::ok(state.roles.list().await?))
}

/// `POST /api/roles` — create a custom role.
pub async fn create(
    state: Data<AppState>,
    op_ctx: OperationCtx,
    body: ValidatedJson<CreateRoleRequest>,
) -> Result<WebResponse<RoleInfo>, WebError> {
    let request = body.into_inner();
    let new = NewRole {
        id: RoleId::from_v7(),
        code: RoleCode::new(request.code),
        name: request.name,
        description: request.description,
        kind: RoleKind::Custom,
        status: RoleStatus::Enabled,
        sort: request.sort,
    };
    let role = state.roles.create(new).await?;
    op_ctx.set_action(OperationCategory::Rbac, "role.create");
    op_ctx.set_resource(ResourceType::Role, role.id.to_string());
    op_ctx.set_detail(serde_json::json!({ "code": role.code }))?;
    Ok(WebResponse::ok(role))
}

/// `GET /api/roles/{id}` — fetch a single role.
pub async fn get(
    state: Data<AppState>,
    id: Path<RoleId>,
) -> Result<WebResponse<RoleInfo>, WebError> {
    Ok(WebResponse::ok(state.roles.find_by_id(&id).await?))
}

/// `PUT /api/roles/{id}` — partial role update.
pub async fn update(
    state: Data<AppState>,
    id: Path<RoleId>,
    op_ctx: OperationCtx,
    body: ValidatedJson<UpdateRoleRequest>,
) -> Result<WebResponse<RoleInfo>, WebError> {
    let role = state.roles.update(&id, body.into_inner().into()).await?;
    op_ctx.set_action(OperationCategory::Rbac, "role.update");
    op_ctx.set_resource(ResourceType::Role, id.to_string());
    Ok(WebResponse::ok(role))
}

/// `DELETE /api/roles/{id}` — delete a custom role, then reload the enforcer
/// (its `p` policies and `g` groupings were purged in the same transaction).
pub async fn delete(
    state: Data<AppState>,
    id: Path<RoleId>,
    op_ctx: OperationCtx,
) -> Result<WebResponse<()>, WebError> {
    state.roles.delete(&id).await?;
    state.ws_sessions.close_all().await;
    state.casbin.reload().await?;
    op_ctx.set_action(OperationCategory::Rbac, "role.delete");
    op_ctx.set_resource(ResourceType::Role, id.to_string());
    Ok(WebResponse::ok(()))
}

/// `PUT /api/roles/{id}/status` — enable/disable a role.
///
/// Disabling drops the role's groupings (revoking it instantly); enabling
/// rebuilds them from the surviving membership. Either way the enforcer is
/// reloaded so the change takes effect at once.
pub async fn change_status(
    state: Data<AppState>,
    id: Path<RoleId>,
    op_ctx: OperationCtx,
    body: ValidatedJson<ChangeRoleStatusRequest>,
) -> Result<WebResponse<()>, WebError> {
    let status = body.into_inner().status;
    state.roles.change_status(&id, status).await?;
    state.ws_sessions.close_all().await;
    state.casbin.reload().await?;
    op_ctx.set_action(OperationCategory::Rbac, "role.change_status");
    op_ctx.set_resource(ResourceType::Role, id.to_string());
    op_ctx.set_detail(serde_json::json!({ "status": status }))?;
    Ok(WebResponse::ok(()))
}

/// `GET /api/roles/{id}/permissions` — the role's current permission set.
pub async fn list_permissions(
    state: Data<AppState>,
    id: Path<RoleId>,
) -> Result<WebResponse<Vec<Permission>>, WebError> {
    Ok(WebResponse::ok(
        state.role_permissions.list_permissions(&id).await?,
    ))
}

/// `PUT /api/roles/{id}/permissions` — replace the role's permission set
/// (rejecting unknown `resource × operation` pairs), then reload the enforcer.
pub async fn set_permissions(
    state: Data<AppState>,
    id: Path<RoleId>,
    op_ctx: OperationCtx,
    body: ValidatedJson<AssignPermissionsRequest>,
) -> Result<WebResponse<()>, WebError> {
    let role_id = id.into_inner();
    let permissions = body.into_inner().permissions;
    op_ctx.set_action(OperationCategory::Rbac, "role.assign_permissions");
    op_ctx.set_resource(ResourceType::Role, role_id.to_string());
    op_ctx.set_detail(serde_json::json!({ "permission_count": permissions.len() }))?;
    state
        .role_permissions
        .set_permissions_for_role(AssignPermissions {
            role_id,
            permissions,
        })
        .await?;
    state.ws_sessions.close_all().await;
    state.casbin.reload().await?;
    Ok(WebResponse::ok(()))
}

/// `GET /api/roles/{id}/menus` — the menus currently granted to the role.
pub async fn list_menus(
    state: Data<AppState>,
    id: Path<RoleId>,
) -> Result<WebResponse<Vec<MenuInfo>>, WebError> {
    Ok(WebResponse::ok(
        state.role_menus.list_menus_for_role(&id).await?,
    ))
}

/// `PUT /api/roles/{id}/menus` — replace the role's menu set. Menus are not part
/// of the Casbin policy, so no enforcer reload is needed.
pub async fn set_menus(
    state: Data<AppState>,
    id: Path<RoleId>,
    op_ctx: OperationCtx,
    body: ValidatedJson<AssignMenusRequest>,
) -> Result<WebResponse<()>, WebError> {
    let role_id = id.into_inner();
    let menu_ids = body.into_inner().menu_ids;
    op_ctx.set_action(OperationCategory::Rbac, "role.assign_menus");
    op_ctx.set_resource(ResourceType::Role, role_id.to_string());
    op_ctx.set_detail(serde_json::json!({ "menu_count": menu_ids.len() }))?;
    state
        .role_menus
        .set_menus_for_role(AssignMenus { role_id, menu_ids })
        .await?;
    Ok(WebResponse::ok(()))
}
