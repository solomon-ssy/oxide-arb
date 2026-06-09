//! WebSocket upgrade handler with pre-upgrade authentication.
//!
//! `GET /api/v1/ws?token=<access>` — the access token is supplied as a query
//! parameter because browsers cannot set custom headers on a WebSocket
//! handshake. The token is decoded and checked against the revocation blacklist
//! **before** the upgrade; failure returns 401 and no socket is established
//! (fixing the ng-gateway unauthenticated-WS defect).

use actix_web::{HttpRequest, HttpResponse, rt, web};
use serde::Deserialize;

use crate::{
    error::WebError,
    jwt::TokenType,
    state::AppState,
    ws::session::{self, SessionContext},
};

/// Query parameters for the WebSocket handshake.
#[derive(Debug, Deserialize)]
pub struct WsAuthQuery {
    /// A valid access token.
    pub token: String,
}

/// `GET /api/v1/ws` — authenticate, upgrade, and spawn the session task.
pub async fn ws_upgrade(
    req: HttpRequest,
    body: web::Payload,
    state: web::Data<AppState>,
    query: web::Query<WsAuthQuery>,
) -> Result<HttpResponse, actix_web::Error> {
    let claims = state
        .jwt
        .decode(&query.token, TokenType::Access)
        .map_err(WebError::from)?;
    if state
        .jwt
        .is_revoked(&claims.jti)
        .await
        .map_err(WebError::from)?
    {
        return Err(WebError::Unauthorized("unauthorized".to_owned()).into());
    }

    let (response, ws_session, msg_stream) = actix_ws::handle(&req, body)?;
    let ctx = SessionContext {
        state: state.clone(),
        registry: state.ws_sessions.clone(),
        user_id: claims.sub,
    };
    rt::spawn(session::run(ws_session, msg_stream, ctx));
    Ok(response)
}
