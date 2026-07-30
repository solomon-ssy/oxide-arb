//! JWT authentication middleware (protected scopes only).
//!
//! Pipeline: extract the `Authorization: Bearer` token → decode + validate it
//! as an **access** token → check the revocation blacklist (fail-closed on a
//! store outage) → load the actor's roles fresh from the database → inject
//! [`Claims`] and [`ActorRoles`] into request extensions for downstream
//! extractors.
//!
//! Public routes (health/ready, login/refresh) are registered outside the
//! scope wrapped by this middleware, so they are never gated here.

use actix_web::{
    Error, HttpMessage,
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    http::header::AUTHORIZATION,
    middleware::Next,
    web::Data,
};
use quant_pivot_error::{auth::AuthError, storage::StorageError};
use quant_pivot_models::{enums::rbac::UserStatus, types::UserId};

use crate::{error::WebError, extractors::ActorRoles, jwt::TokenUse, state::AppState};

/// Authenticate the request and attach the actor's identity + roles.
pub async fn authn<B: MessageBody>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<B>, Error> {
    let state = req
        .app_data::<Data<AppState>>()
        .cloned()
        .ok_or_else(|| WebError::Internal("application state missing".to_owned()))?;

    let token = bearer_token(&req)?;
    let claims = state
        .jwt
        .decode(&token, TokenUse::Access)
        .map_err(WebError::from)?;

    if !state
        .jwt
        .session_active(&claims.jti, &claims.family_id)
        .await
        .map_err(WebError::from)?
    {
        return Err(WebError::from(AuthError::Blacklisted).into());
    }

    let user_id = claims
        .sub
        .parse::<UserId>()
        .map_err(|_| WebError::from(AuthError::InvalidToken))?;
    let user = match state.users.find_by_id(&user_id).await {
        Ok(user) => user,
        Err(StorageError::NotFound { .. }) => {
            return Err(WebError::from(AuthError::InvalidToken).into());
        }
        Err(error) => return Err(WebError::from(error).into()),
    };
    if user.status != UserStatus::Active {
        return Err(WebError::from(AuthError::Blacklisted).into());
    }
    let roles = state
        .user_roles
        .list_roles_for_user(&user_id)
        .await
        .map_err(WebError::from)?;
    drop(state);

    {
        let mut extensions = req.extensions_mut();
        extensions.insert(claims);
        extensions.insert(ActorRoles::new(roles));
    }

    next.call(req).await
}

/// Extract a non-empty bearer token from the `Authorization` header.
fn bearer_token(req: &ServiceRequest) -> Result<String, WebError> {
    let header = req
        .headers()
        .get(AUTHORIZATION)
        .ok_or_else(|| WebError::from(AuthError::MissingToken))?;
    let raw = header
        .to_str()
        .map_err(|_| WebError::from(AuthError::InvalidToken))?;
    let token = raw
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| WebError::from(AuthError::MissingToken))?;
    Ok(token.to_owned())
}
