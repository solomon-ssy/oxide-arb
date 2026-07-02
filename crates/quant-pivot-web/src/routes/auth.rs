//! Authentication endpoints: login / refresh / logout / me.
//!
//! Security properties:
//!
//! - **No user enumeration / timing oracle**: a failed login always returns the
//!   same `401 invalid credentials`, and an unknown username still runs one
//!   argon2id verification against a throwaway hash so the response time does
//!   not reveal account existence.
//! - **Refresh rotation**: a successful refresh revokes the presenting refresh
//!   token's `jti`, so a leaked refresh token cannot be replayed after use.
//! - **Logout**: revokes the access token and, when supplied, the refresh
//!   token, immediately invalidating the session.
//! - **`/me`** never exposes the password hash; it projects [`UserInfo`] into a
//!   credential-free view plus roles and the role-accessible menu tree.

use std::sync::OnceLock;

use actix_web::{http::Method, web};
use quant_pivot_error::{auth::AuthError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        LoginRequest, LogoutRequest, MeResponse, RefreshRequest, RoleView, TokenResponse, UserInfo,
        UserView,
    },
    enums::{operation_log::OperationCategory, rbac::UserStatus},
    security::{hash_password, verify_password},
    types::UserId,
};

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{AuthedActor, ValidatedJson},
    jwt::TokenType,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Protected auth routes (login/refresh are public and registered in [`super::configure`]).
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::POST,
            "/auth/logout",
            Rule::AuthenticatedOnly,
            logout,
        ),
        spec(Method::GET, "/auth/me", Rule::AuthenticatedOnly, me),
    ]
}

/// `POST /api/auth/login` (`v1`) — verify credentials and issue a token pair.
pub async fn login(
    state: web::Data<AppState>,
    op_ctx: OperationCtx,
    body: ValidatedJson<LoginRequest>,
) -> Result<WebResponse<TokenResponse>, WebError> {
    let request = body.into_inner();
    // Attribute the attempt to the supplied username (never the password) so a
    // failed login is auditable without leaking credentials.
    op_ctx.set_action(OperationCategory::Auth, "auth.login");
    op_ctx.set_actor_username(&request.username);
    let candidate = state.users.find_by_username(&request.username).await?;

    // Always run a verification so an unknown username takes the same time as a
    // wrong password (constant-time-ish defense against enumeration).
    let stored_hash = candidate.as_ref().map_or_else(
        || dummy_hash().to_owned(),
        |user| user.password_hash.clone(),
    );
    let verified = verify_password(&request.password, &stored_hash);

    let user = match candidate {
        Some(user) if verified && user.status == UserStatus::Active => user,
        _ => return Err(AuthError::InvalidCredentials.into()),
    };

    // Promote the attribution to the fully-resolved actor on success.
    op_ctx.set_actor(user.id.clone(), &user.username);
    let tokens = issue_pair(&state, &user)?;
    Ok(WebResponse::ok(tokens))
}

/// `POST /api/auth/refresh` (`v1`) — rotate a refresh token into a fresh pair.
pub async fn refresh(
    state: web::Data<AppState>,
    op_ctx: OperationCtx,
    body: ValidatedJson<RefreshRequest>,
) -> Result<WebResponse<TokenResponse>, WebError> {
    op_ctx.set_action(OperationCategory::Auth, "auth.refresh");
    let request = body.into_inner();
    let claims = state
        .jwt
        .decode(&request.refresh_token, TokenType::Refresh)?;

    if state.jwt.is_revoked(&claims.jti).await? {
        return Err(AuthError::Blacklisted.into());
    }

    let user_id = claims
        .sub
        .parse::<UserId>()
        .map_err(|_| WebError::from(AuthError::InvalidToken))?;
    let user = match state.users.find_by_id(&user_id).await {
        Ok(user) => user,
        Err(StorageError::NotFound { .. }) => return Err(AuthError::InvalidToken.into()),
        Err(error) => return Err(error.into()),
    };
    if user.status != UserStatus::Active {
        return Err(AuthError::InvalidToken.into());
    }

    // Revoke the presented refresh token before minting a new pair so it cannot
    // be replayed.
    state.jwt.revoke(&claims).await?;

    op_ctx.set_actor(user.id.clone(), &user.username);
    let tokens = issue_pair(&state, &user)?;
    Ok(WebResponse::ok(tokens))
}

/// `POST /api/auth/logout` (`v1`) — revoke the access token (and refresh).
pub async fn logout(
    state: web::Data<AppState>,
    actor: AuthedActor,
    op_ctx: OperationCtx,
    body: Option<web::Json<LogoutRequest>>,
) -> Result<WebResponse<()>, WebError> {
    op_ctx.set_action(OperationCategory::Auth, "auth.logout");
    state.jwt.revoke(&actor.claims).await?;

    if let Some(payload) = body
        && let Some(refresh_token) = payload.into_inner().refresh_token
    {
        // Best-effort: only a well-formed refresh token can be revoked; a
        // malformed one is already useless.
        if let Ok(claims) = state.jwt.decode(&refresh_token, TokenType::Refresh) {
            state.jwt.revoke(&claims).await?;
        }
    }

    Ok(WebResponse::ok(()))
}

/// `GET /api/auth/me` (`v1`) — the current user, roles, and accessible menus.
pub async fn me(
    state: web::Data<AppState>,
    actor: AuthedActor,
) -> Result<WebResponse<MeResponse>, WebError> {
    let user_id = actor
        .claims
        .sub
        .parse::<UserId>()
        .map_err(|_| WebError::from(AuthError::InvalidToken))?;
    let user = state.users.find_by_id(&user_id).await?;
    let menus = state
        .menus
        .accessible_for_roles(&actor.roles.enabled_ids())
        .await?;
    let roles = actor.roles.as_slice().iter().map(RoleView::from).collect();

    Ok(WebResponse::ok(MeResponse {
        user: UserView::from(user),
        roles,
        menus,
    }))
}

/// Sign an access + refresh token pair for `user`.
fn issue_pair(state: &AppState, user: &UserInfo) -> Result<TokenResponse, WebError> {
    let access = state.jwt.encode_access(user)?;
    let refresh = state.jwt.encode_refresh(user)?;
    Ok(TokenResponse {
        access_token: access.token,
        refresh_token: refresh.token,
        token_type: "Bearer",
        expires_in: state.jwt.access_ttl_secs(),
    })
}

/// A lazily-computed argon2id hash of a throwaway password.
///
/// Used as the verification target when a username is unknown, so the login
/// path performs the same hashing work regardless of account existence.
fn dummy_hash() -> &'static str {
    static HASH: OnceLock<String> = OnceLock::new();
    HASH.get_or_init(|| {
        hash_password("quant-pivot::login-timing-guard")
            .expect("argon2id hashing of a static string must not fail")
    })
    .as_str()
}
