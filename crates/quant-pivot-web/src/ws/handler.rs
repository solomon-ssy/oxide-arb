//! WebSocket upgrade handler with pre-upgrade authentication.
//!
//! A Bearer-authenticated `POST /api/ws/tickets` mints a 30-second single-use
//! ticket. The browser presents it in `Sec-WebSocket-Protocol`; the upgrade
//! atomically consumes it before a socket is established.

use actix_web::{
    Error, HttpRequest, HttpResponse,
    http::header::{HeaderValue, SEC_WEBSOCKET_PROTOCOL},
    rt,
    web::{Data, Payload},
};
use chrono::Utc;
use quant_pivot_error::auth::AuthError;
use quant_pivot_models::{
    enums::rbac::{Operation, ResourceType, UserStatus},
    types::UserId,
};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    error::WebError,
    extractors::AuthedActor,
    jwt::WsTicketClaims,
    request_security::ensure_allowed_origin,
    response::WebResponse,
    state::AppState,
    ws::session::{self, SessionContext},
};

const WS_TICKET_TTL_SECS: u64 = 30;
const WS_TICKET_PROTOCOL_PREFIX: &str = "qp-ticket.";

#[derive(Serialize)]
pub struct WsTicketResponse {
    pub ticket: String,
    pub expires_in: u64,
}

pub async fn issue_ws_ticket(
    state: Data<AppState>,
    actor: AuthedActor,
) -> Result<WebResponse<WsTicketResponse>, WebError> {
    if !state.casbin.is_healthy() {
        return Err(WebError::ServiceUnavailable(
            "authorization policy is unavailable".to_owned(),
        ));
    }
    let ticket = state
        .jwt_blacklist
        .issue_ws_ticket(
            &actor.claims,
            state.casbin.authorization_revision(),
            WS_TICKET_TTL_SECS,
        )
        .await?;
    Ok(WebResponse::ok(WsTicketResponse {
        ticket,
        expires_in: WS_TICKET_TTL_SECS,
    }))
}

/// `GET /api/ws` — authenticate, upgrade, and spawn the session task.
pub async fn ws_upgrade(
    req: HttpRequest,
    body: Payload,
    state: Data<AppState>,
) -> Result<HttpResponse, Error> {
    ensure_allowed_origin(&req, &state.deploy)?;
    let protocol = ticket_protocol(&req)
        .ok_or_else(|| WebError::Unauthorized("missing websocket ticket protocol".to_owned()))?;
    let ticket = protocol
        .strip_prefix(WS_TICKET_PROTOCOL_PREFIX)
        .ok_or_else(|| WebError::Unauthorized("invalid websocket ticket protocol".to_owned()))?;
    Uuid::parse_str(ticket)
        .map_err(|_| WebError::Unauthorized("invalid websocket ticket format".to_owned()))?;
    let ticket_claims = state
        .jwt_blacklist
        .consume_ws_ticket(ticket)
        .await
        .map_err(WebError::from)?
        .ok_or_else(|| WebError::Unauthorized("invalid or consumed websocket ticket".to_owned()))?;
    let subject_id = validate_ticket(&state, &ticket_claims).await?;
    let can_read_system = state
        .casbin
        .enforce(
            &ticket_claims.subject,
            ResourceType::System.as_str(),
            Operation::Read.as_str(),
        )
        .await?;

    let (mut response, ws_session, msg_stream) = actix_ws::handle(&req, body)?;
    response.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_str(&protocol)
            .map_err(|_| WebError::Unauthorized("invalid websocket protocol".to_owned()))?,
    );
    rt::spawn(session::run(
        ws_session,
        msg_stream,
        SessionContext {
            state: state.clone(),
            registry: state.ws_sessions.clone(),
            subject_id,
            user_id: ticket_claims.subject,
            family_id: ticket_claims.family_id,
            access_jti: ticket_claims.access_jti,
            authorization_revision: ticket_claims.authorization_revision,
            can_read_system,
        },
    ));
    Ok(response)
}

fn ticket_protocol(req: &HttpRequest) -> Option<String> {
    let protocols = req
        .headers()
        .get(SEC_WEBSOCKET_PROTOCOL)?
        .to_str()
        .ok()?
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if protocols.len() != 1 || !protocols[0].starts_with(WS_TICKET_PROTOCOL_PREFIX) {
        return None;
    }
    Some(protocols[0].to_owned())
}

async fn validate_ticket(state: &AppState, ticket: &WsTicketClaims) -> Result<UserId, WebError> {
    if !state.casbin.is_healthy()
        || ticket.session_exp <= Utc::now().timestamp()
        || state.jwt.is_revoked(&ticket.access_jti).await?
        || !state.jwt.family_active(&ticket.family_id).await?
    {
        return Err(WebError::from(AuthError::Blacklisted));
    }
    let user_id = ticket
        .subject
        .parse::<UserId>()
        .map_err(|_| WebError::from(AuthError::InvalidToken))?;
    let user = state.users.find_by_id(&user_id).await?;
    if user.status != UserStatus::Active {
        return Err(WebError::from(AuthError::InvalidCredentials));
    }
    if state.casbin.authorization_revision() != ticket.authorization_revision {
        return Err(WebError::from(AuthError::Blacklisted));
    }
    Ok(user_id)
}
