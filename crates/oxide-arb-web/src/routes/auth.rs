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

use actix_web::web;
use chrono::{DateTime, Utc};
use oxide_arb_error::{auth::AuthError, storage::StorageError};
use oxide_arb_models::{
    domain::{MenuTreeNode, RoleInfo, UserInfo},
    enums::rbac::UserStatus,
    security::{hash_password, verify_password},
    types::{RoleId, UserId},
};
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    error::WebError,
    extractors::{AuthedActor, ValidatedJson},
    jwt::TokenType,
    response::WebResponse,
    state::AppState,
};

/// Login credentials.
#[derive(Debug, Deserialize, Validate)]
pub struct LoginRequest {
    /// Account username.
    #[validate(length(min = 1, max = 128))]
    pub username: String,
    /// Plaintext password (verified against the stored argon2id hash).
    #[validate(length(min = 1, max = 256))]
    pub password: String,
}

/// Refresh-token rotation request.
#[derive(Debug, Deserialize, Validate)]
pub struct RefreshRequest {
    /// The refresh token to exchange for a fresh access/refresh pair.
    #[validate(length(min = 1))]
    pub refresh_token: String,
}

/// Optional logout payload carrying the refresh token to revoke alongside the
/// access token.
#[derive(Debug, Deserialize)]
pub struct LogoutRequest {
    /// Refresh token to revoke (optional; the access token is always revoked).
    pub refresh_token: Option<String>,
}

/// Token pair issued on login/refresh.
#[derive(Debug, Serialize)]
pub struct TokenResponse {
    /// Short-lived access token.
    pub access_token: String,
    /// Long-lived refresh token.
    pub refresh_token: String,
    /// Always `"Bearer"`.
    pub token_type: &'static str,
    /// Access-token lifetime in seconds.
    pub expires_in: i64,
}

/// Credential-free projection of a user account.
#[derive(Debug, Serialize)]
pub struct UserView {
    /// Stable user id.
    pub id: UserId,
    /// Username.
    pub username: String,
    /// Display name.
    pub nickname: String,
    /// Optional avatar URL.
    pub avatar: Option<String>,
    /// Optional email.
    pub email: Option<String>,
    /// Optional phone.
    pub phone: Option<String>,
    /// Account status.
    pub status: UserStatus,
    /// Creation timestamp.
    pub created_at: DateTime<Utc>,
    /// Last update timestamp.
    pub updated_at: DateTime<Utc>,
}

impl From<UserInfo> for UserView {
    fn from(user: UserInfo) -> Self {
        // `password_hash` is intentionally dropped here.
        Self {
            id: user.id,
            username: user.username,
            nickname: user.nickname,
            avatar: user.avatar,
            email: user.email,
            phone: user.phone,
            status: user.status,
            created_at: user.created_at,
            updated_at: user.updated_at,
        }
    }
}

/// Compact role projection for the `/me` response.
#[derive(Debug, Serialize)]
pub struct RoleView {
    /// Role id.
    pub id: RoleId,
    /// Role code (Casbin subject).
    pub code: String,
    /// Display name.
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
}

impl From<&RoleInfo> for RoleView {
    fn from(role: &RoleInfo) -> Self {
        Self {
            id: role.id.clone(),
            code: role.code.clone(),
            name: role.name.clone(),
            description: role.description.clone(),
        }
    }
}

/// `/me` projection: the current user, their roles, and accessible menus.
#[derive(Debug, Serialize)]
pub struct MeResponse {
    /// The authenticated user.
    pub user: UserView,
    /// The user's roles.
    pub roles: Vec<RoleView>,
    /// The menu tree the user's roles grant access to.
    pub menus: Vec<MenuTreeNode>,
}

/// `POST /api/auth/login` (`v1`) — verify credentials and issue a token pair.
pub async fn login(
    state: web::Data<AppState>,
    body: ValidatedJson<LoginRequest>,
) -> Result<WebResponse<TokenResponse>, WebError> {
    let request = body.into_inner();
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

    let tokens = issue_pair(&state, &user)?;
    Ok(WebResponse::ok(tokens))
}

/// `POST /api/auth/refresh` (`v1`) — rotate a refresh token into a fresh pair.
pub async fn refresh(
    state: web::Data<AppState>,
    body: ValidatedJson<RefreshRequest>,
) -> Result<WebResponse<TokenResponse>, WebError> {
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

    let tokens = issue_pair(&state, &user)?;
    Ok(WebResponse::ok(tokens))
}

/// `POST /api/auth/logout` (`v1`) — revoke the access token (and refresh).
pub async fn logout(
    state: web::Data<AppState>,
    actor: AuthedActor,
    body: Option<web::Json<LogoutRequest>>,
) -> Result<WebResponse<()>, WebError> {
    state.jwt.revoke(&actor.claims).await?;

    if let Some(payload) = body {
        if let Some(refresh_token) = payload.into_inner().refresh_token {
            // Best-effort: only a well-formed refresh token can be revoked; a
            // malformed one is already useless.
            if let Ok(claims) = state.jwt.decode(&refresh_token, TokenType::Refresh) {
                state.jwt.revoke(&claims).await?;
            }
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
    let menus = state.menus.accessible_for_roles(&actor.roles.ids()).await?;
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
        hash_password("oxide-arb::login-timing-guard")
            .expect("argon2id hashing of a static string must not fail")
    })
    .as_str()
}
