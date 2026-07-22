//! Casbin authorization middleware (protected scope only, after authn).
//!
//! Pipeline: read the actor identity that [`authn`](super::authn) injected →
//! resolve the matched route pattern → consult the [`PermChecker`] →
//! allow/deny. A protected route with no registered rule is **denied**
//! (fail-closed), and a request that somehow reached here without an identity is
//! rejected rather than trusted.
//!
//! On a governed rule, the resolved `acting_role` (from the `X-Acting-Role`
//! header) is injected into request extensions for the handler's audit envelope.
//! The middleware itself never reads the request body.
//!
//! [`PermChecker`]: crate::auth::casbin::PermChecker

use actix_web::{
    Error, HttpMessage,
    body::MessageBody,
    dev::{ServiceRequest, ServiceResponse},
    middleware::Next,
    web::Data,
};
use quant_pivot_error::auth::AuthError;

use crate::{
    error::WebError,
    extractors::{ActingRole, ActorRoles},
    jwt::Claims,
    state::AppState,
};

/// Header carrying the explicit role a caller acts as on governed endpoints.
const ACTING_ROLE_HEADER: &str = "x-acting-role";

/// Authorize the request against the route's registered rule.
pub async fn authz<B: MessageBody>(
    req: ServiceRequest,
    next: Next<B>,
) -> Result<ServiceResponse<B>, Error> {
    let state = req
        .app_data::<Data<AppState>>()
        .cloned()
        .ok_or_else(|| WebError::Internal("application state missing".to_owned()))?;

    // authn runs first (outer scope); its identity must be present. If it is
    // not, fail closed rather than treating the request as anonymous-allowed.
    let (claims, roles) = {
        let extensions = req.extensions();
        match (
            extensions.get::<Claims>().cloned(),
            extensions.get::<ActorRoles>().cloned(),
        ) {
            (Some(claims), Some(roles)) => (claims, roles),
            _ => return Err(WebError::from(AuthError::MissingToken).into()),
        }
    };

    let method = req.method().clone();
    // No matched pattern means no registered route owns this request — deny.
    let pattern = req.match_pattern().ok_or(WebError::Forbidden)?;
    let acting_role = req
        .headers()
        .get(ACTING_ROLE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    let outcome = state
        .perm_checker
        .check(
            &method,
            &pattern,
            &claims,
            &roles,
            &state.casbin,
            acting_role.as_deref(),
        )
        .await?;
    drop(state);

    if let Some(role) = outcome.acting_role {
        req.extensions_mut().insert(ActingRole(role));
    }

    next.call(req).await
}
