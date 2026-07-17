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
//! - **Logout**: revokes the access token and the refresh-cookie token family,
//!   immediately invalidating the session.
//! - **`/me`** never exposes the password hash; it projects [`UserInfo`] into a
//!   credential-free view plus roles and the role-accessible menu tree.

use std::sync::OnceLock;

use actix_web::{
    HttpRequest, HttpResponse,
    cookie::{Cookie, SameSite, time::Duration as CookieDuration},
    http::Method,
    web,
};
use quant_pivot_error::{auth::AuthError, storage::StorageError};
use quant_pivot_models::{
    domain::{LoginRequest, MeResponse, RoleView, TokenResponse, UserInfo, UserView},
    enums::{operation_log::OperationCategory, rbac::UserStatus},
    security::{hash_password, verify_password},
    types::UserId,
};

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{AuthedActor, ValidatedJson},
    jwt::{RefreshFamilyRotation, TokenUse},
    request_security::ensure_same_origin_mutation,
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
) -> Result<HttpResponse, WebError> {
    let request = body.into_inner();
    // Attribute the attempt to the supplied username (never the password) so a
    // failed login is auditable without leaking credentials.
    op_ctx.set_action(OperationCategory::Auth, "auth.login");
    op_ctx.set_actor_username(&request.username);
    let candidate = state.users.find_by_username(&request.username).await?;

    // Always run a verification so an unknown username takes the same time as a
    // wrong password (constant-time-ish defense against enumeration).
    let stored_hash = match candidate.as_ref() {
        Some(user) => user.password_hash.clone(),
        None => dummy_hash()?.to_owned(),
    };
    let verified = verify_password(&request.password, &stored_hash);

    let user = match candidate {
        Some(user) if verified && user.status == UserStatus::Active => user,
        _ => return Err(AuthError::InvalidCredentials.into()),
    };

    // Promote the attribution to the fully-resolved actor on success.
    op_ctx.set_actor(user.id.clone(), &user.username);
    let pair = issue_pair(&state, &user, None, None, 0)?;
    state
        .jwt
        .create_refresh_family(
            &pair.family_id,
            &pair.refresh_jti,
            &user.id.to_string(),
            pair.session_exp,
        )
        .await?;
    Ok(token_response(pair))
}

/// `POST /api/auth/refresh` (`v1`) — rotate a refresh token into a fresh pair.
pub async fn refresh(
    state: web::Data<AppState>,
    request: HttpRequest,
    op_ctx: OperationCtx,
) -> Result<HttpResponse, WebError> {
    op_ctx.set_action(OperationCategory::Auth, "auth.refresh");
    ensure_same_origin_mutation(&request, &state.deploy)?;
    let refresh_token = request
        .cookie(REFRESH_COOKIE_NAME)
        .ok_or(AuthError::InvalidToken)?;
    let claims = state.jwt.decode(refresh_token.value(), TokenUse::Refresh)?;

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

    op_ctx.set_actor(user.id.clone(), &user.username);
    let family_id = claims.family_id.clone();
    let child_generation = claims
        .generation
        .checked_add(1)
        .ok_or(AuthError::InvalidToken)?;
    let pair = issue_pair(
        &state,
        &user,
        Some(&family_id),
        Some(claims.session_exp),
        child_generation,
    )?;
    match state
        .jwt
        .rotate_refresh_family(&claims, &pair.refresh_jti, child_generation)
        .await?
    {
        RefreshFamilyRotation::Rotated => {}
        RefreshFamilyRotation::ReplayOrStale | RefreshFamilyRotation::RevokedOrMissing => {
            state.ws_sessions.close_family(&family_id);
            return Err(AuthError::Blacklisted.into());
        }
    }
    Ok(token_response(pair))
}

/// `POST /api/auth/logout` (`v1`) — revoke the access token (and refresh).
pub async fn logout(
    state: web::Data<AppState>,
    request: HttpRequest,
    actor: AuthedActor,
    op_ctx: OperationCtx,
) -> Result<HttpResponse, WebError> {
    op_ctx.set_action(OperationCategory::Auth, "auth.logout");
    ensure_same_origin_mutation(&request, &state.deploy)?;
    state.jwt.revoke(&actor.claims).await?;
    state.jwt.revoke_family(&actor.claims.family_id).await?;
    state.ws_sessions.close_family(&actor.claims.family_id);

    if let Some(refresh_token) = request.cookie(REFRESH_COOKIE_NAME) {
        // Best-effort: only a well-formed refresh token can be revoked; a
        // malformed one is already useless.
        if let Ok(claims) = state.jwt.decode(refresh_token.value(), TokenUse::Refresh) {
            state.jwt.revoke_family(&claims.family_id).await?;
            state.ws_sessions.close_family(&claims.family_id);
        }
    }

    Ok(HttpResponse::Ok()
        .cookie(expired_refresh_cookie())
        .json(WebResponse::ok(())))
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
const REFRESH_COOKIE_NAME: &str = "qp_refresh";

struct IssuedPair {
    response: TokenResponse,
    refresh_token: String,
    refresh_jti: String,
    refresh_exp: i64,
    family_id: String,
    session_exp: i64,
}

fn issue_pair(
    state: &AppState,
    user: &UserInfo,
    family_id: Option<&str>,
    session_exp: Option<i64>,
    generation: u64,
) -> Result<IssuedPair, WebError> {
    let owned_family = family_id.map_or_else(|| uuid::Uuid::now_v7().to_string(), str::to_owned);
    let absolute_session_exp = session_exp
        .unwrap_or_else(|| chrono::Utc::now().timestamp() + state.jwt.absolute_session_ttl_secs());
    let access =
        state
            .jwt
            .encode_access_in_family(user, &owned_family, absolute_session_exp, generation)?;
    let refresh = state.jwt.encode_refresh_in_family(
        user,
        &owned_family,
        absolute_session_exp,
        generation,
    )?;
    Ok(IssuedPair {
        response: TokenResponse {
            access_token: access.token,
            token_type: "Bearer",
            expires_in: (access.exp - chrono::Utc::now().timestamp()).max(0),
        },
        refresh_token: refresh.token,
        refresh_jti: refresh.jti,
        refresh_exp: refresh.exp,
        family_id: owned_family,
        session_exp: absolute_session_exp,
    })
}

fn token_response(pair: IssuedPair) -> HttpResponse {
    let cookie_max_age = (pair.refresh_exp - chrono::Utc::now().timestamp()).max(1);
    HttpResponse::Ok()
        .cookie(refresh_cookie(pair.refresh_token, cookie_max_age))
        .json(WebResponse::ok(pair.response))
}

fn refresh_cookie(token: String, max_age_secs: i64) -> Cookie<'static> {
    Cookie::build(REFRESH_COOKIE_NAME, token)
        .http_only(true)
        .secure(true)
        .same_site(SameSite::Strict)
        .path("/api/auth")
        .max_age(CookieDuration::seconds(max_age_secs))
        .finish()
}

fn expired_refresh_cookie() -> Cookie<'static> {
    Cookie::build(REFRESH_COOKIE_NAME, "")
        .http_only(true)
        .secure(true)
        .same_site(SameSite::Strict)
        .path("/api/auth")
        .max_age(CookieDuration::ZERO)
        .finish()
}

/// A lazily-computed argon2id hash of a throwaway password.
///
/// Used as the verification target when a username is unknown, so the login
/// path performs the same hashing work regardless of account existence.
fn dummy_hash() -> Result<&'static str, WebError> {
    static HASH: OnceLock<String> = OnceLock::new();
    if let Some(hash) = HASH.get() {
        return Ok(hash.as_str());
    }

    let generated = hash_password("quant-pivot::login-timing-guard")
        .map_err(|error| WebError::Internal(error.to_string()))?;
    let _already_initialized = HASH.set(generated);
    HASH.get()
        .map(String::as_str)
        .ok_or_else(|| WebError::Internal("login timing guard initialization failed".to_owned()))
}
