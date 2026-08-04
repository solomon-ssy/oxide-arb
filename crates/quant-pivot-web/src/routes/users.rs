//! User management endpoints (RBAC `User` resource).
//!
//! CRUD plus dedicated, single-purpose transitions for status, password, and
//! role assignment. Credentials are argon2id-hashed here before they reach the
//! repository, and responses always project through [`UserView`] so the stored
//! `password_hash` never crosses the wire.
//!
//! Casbin is reloaded only where a write actually mutated the policy table:
//! deleting a user (cascades its `g` groupings) and replacing a user's roles.

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use quant_pivot_models::{
    domain::{
        api::{
            AssignRolesRequest, ChangePasswordRequest, ChangeUserStatusRequest, CreateUserRequest,
            UpdateUserRequest, UserPageQuery, UserView,
        },
        pagination::Paginated,
        rbac::{AssignRoles, ChangeUserPassword, NewUser, UserInfo},
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType, UserStatus},
    },
    types::UserId,
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

/// User resource routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/users",
            Rule::ResourceOp(ResourceType::User, Operation::Read),
            list,
        ),
        spec(
            Method::POST,
            "/users",
            Rule::ResourceOp(ResourceType::User, Operation::Create),
            create,
        ),
        spec(
            Method::GET,
            "/users/{id}",
            Rule::ResourceOp(ResourceType::User, Operation::Read),
            get,
        ),
        spec(
            Method::PUT,
            "/users/{id}",
            Rule::ResourceOp(ResourceType::User, Operation::Update),
            update,
        ),
        spec(
            Method::DELETE,
            "/users/{id}",
            Rule::ResourceOp(ResourceType::User, Operation::Delete),
            delete,
        ),
        spec(
            Method::PUT,
            "/users/{id}/status",
            Rule::ResourceOp(ResourceType::User, Operation::Update),
            change_status,
        ),
        spec(
            Method::PUT,
            "/users/{id}/password",
            Rule::ResourceOp(ResourceType::User, Operation::Update),
            change_password,
        ),
        spec(
            Method::PUT,
            "/users/{id}/roles",
            Rule::ResourceOp(ResourceType::User, Operation::Assign),
            set_roles,
        ),
    ]
}

/// `GET /api/users` — paginated, filtered user listing.
pub async fn list(
    state: Data<AppState>,
    query: Query<UserPageQuery>,
) -> Result<WebResponse<Paginated<UserView>>, WebError> {
    let result = state.users.page(query.into_inner()).await?;
    Ok(WebResponse::ok(project_page(result)))
}

/// `POST /api/users` — create a user with an argon2id-hashed password.
pub async fn create(
    state: Data<AppState>,
    op_ctx: OperationCtx,
    body: ValidatedJson<CreateUserRequest>,
) -> Result<WebResponse<UserView>, WebError> {
    let request = body.into_inner();
    let password_hash = state.password_crypto.hash(request.password).await?;
    let new = NewUser {
        id: UserId::from_v7(),
        username: request.username,
        password_hash,
        nickname: request.nickname,
        avatar: request.avatar,
        email: request.email,
        phone: request.phone,
        status: request.status.unwrap_or(UserStatus::Active),
    };
    let user = state.users.create(new).await?;
    op_ctx.set_action(OperationCategory::Rbac, "user.create");
    op_ctx.set_resource(ResourceType::User, user.id.to_string());
    // Redacted: the username identifies the row; the password never appears.
    op_ctx.set_detail(serde_json::json!({ "username": user.username }))?;
    Ok(WebResponse::ok(UserView::from(user)))
}

/// `GET /api/users/{id}` — fetch a single user.
pub async fn get(
    state: Data<AppState>,
    id: Path<UserId>,
) -> Result<WebResponse<UserView>, WebError> {
    let user = state.users.find_by_id(&id).await?;
    Ok(WebResponse::ok(UserView::from(user)))
}

/// `PUT /api/users/{id}` — partial profile update.
pub async fn update(
    state: Data<AppState>,
    id: Path<UserId>,
    op_ctx: OperationCtx,
    body: ValidatedJson<UpdateUserRequest>,
) -> Result<WebResponse<UserView>, WebError> {
    let user = state.users.update(&id, body.into_inner().into()).await?;
    op_ctx.set_action(OperationCategory::Rbac, "user.update");
    op_ctx.set_resource(ResourceType::User, id.to_string());
    Ok(WebResponse::ok(UserView::from(user)))
}

/// `DELETE /api/users/{id}` — delete a user and reload the enforcer (its `g`
/// groupings were cascaded away in the same repository transaction).
pub async fn delete(
    state: Data<AppState>,
    id: Path<UserId>,
    op_ctx: OperationCtx,
) -> Result<WebResponse<()>, WebError> {
    state.jwt.revoke_subject_sessions(&id.to_string()).await?;
    state.users.delete(&id).await?;
    state.ws_sessions.close_subject(*id).await;
    state.casbin.reload().await?;
    op_ctx.set_action(OperationCategory::Rbac, "user.delete");
    op_ctx.set_resource(ResourceType::User, id.to_string());
    Ok(WebResponse::ok(()))
}

/// `PUT /api/users/{id}/status` — activate/deactivate an account.
pub async fn change_status(
    state: Data<AppState>,
    id: Path<UserId>,
    op_ctx: OperationCtx,
    body: ValidatedJson<ChangeUserStatusRequest>,
) -> Result<WebResponse<()>, WebError> {
    let status = body.into_inner().status;
    if status != UserStatus::Active {
        state.jwt.revoke_subject_sessions(&id.to_string()).await?;
    }
    state.users.change_status(&id, status).await?;
    if status != UserStatus::Active {
        state.ws_sessions.close_subject(*id).await;
    }
    op_ctx.set_action(OperationCategory::Rbac, "user.change_status");
    op_ctx.set_resource(ResourceType::User, id.to_string());
    op_ctx.set_detail(serde_json::json!({ "status": status }))?;
    Ok(WebResponse::ok(()))
}

/// `PUT /api/users/{id}/password` — reset a user's password.
pub async fn change_password(
    state: Data<AppState>,
    id: Path<UserId>,
    op_ctx: OperationCtx,
    body: ValidatedJson<ChangePasswordRequest>,
) -> Result<WebResponse<()>, WebError> {
    let password_hash = state
        .password_crypto
        .hash(body.into_inner().password)
        .await?;
    state.jwt.revoke_subject_sessions(&id.to_string()).await?;
    state
        .users
        .change_password(&id, ChangeUserPassword { password_hash })
        .await?;
    state.ws_sessions.close_subject(*id).await;
    // Record the act only — never the new password or its hash.
    op_ctx.set_action(OperationCategory::Rbac, "user.change_password");
    op_ctx.set_resource(ResourceType::User, id.to_string());
    Ok(WebResponse::ok(()))
}

/// `PUT /api/users/{id}/roles` — replace a user's role set, then reload the
/// enforcer so the new groupings take effect immediately.
pub async fn set_roles(
    state: Data<AppState>,
    id: Path<UserId>,
    op_ctx: OperationCtx,
    body: ValidatedJson<AssignRolesRequest>,
) -> Result<WebResponse<()>, WebError> {
    let user_id = id.into_inner();
    let role_ids = body.into_inner().role_ids;
    op_ctx.set_action(OperationCategory::Rbac, "user.assign_roles");
    op_ctx.set_resource(ResourceType::User, user_id.to_string());
    op_ctx.set_detail(serde_json::json!({ "role_count": role_ids.len() }))?;
    state
        .user_roles
        .set_roles_for_user(AssignRoles { user_id, role_ids })
        .await?;
    state.ws_sessions.close_subject(user_id).await;
    state.casbin.reload().await?;
    Ok(WebResponse::ok(()))
}

/// Project a paginated [`UserInfo`] page into the credential-free [`UserView`].
fn project_page(page: Paginated<UserInfo>) -> Paginated<UserView> {
    Paginated {
        items: page.items.into_iter().map(UserView::from).collect(),
        total: page.total,
        page: page.page,
        size: page.size,
        has_next: page.has_next,
    }
}
