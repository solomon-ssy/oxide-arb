//! `/ws` WebSocket route registration.
//!
//! A protected HTTP request first mints a short-lived, single-use ticket. The
//! WebSocket upgrade atomically consumes that ticket from
//! `Sec-WebSocket-Protocol`, so it sits outside the Bearer-header `authn` scope
//! and [`super::version::ApiV1Guard`]. Per-channel authorization is enforced
//! inside the session against the
//! same `(resource, operation)` pairs as the HTTP routes (via
//! [`quant_pivot_models::domain::ws::WsChannel::resource`]), so a socket cannot bypass
//! route-level authorization.
//!
//! The handshake + session implementation lives in [`crate::ws`]; this module
//! is only the registration point so all routes are wired through `routes/`.

use actix_web::{
    http::Method,
    web::{self, ServiceConfig},
};

use crate::{
    auth::casbin::Rule,
    routes::registry::{RouteSpec, spec},
    ws::handler::{issue_ws_ticket, ws_upgrade},
};

pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![spec(
        Method::POST,
        "/ws/tickets",
        Rule::AuthenticatedOnly,
        issue_ws_ticket,
    )]
}

/// Register the `/ws` upgrade route onto the API-prefixed scope (outside
/// [`super::version::ApiV1Guard`]).
pub fn configure(cfg: &mut ServiceConfig) {
    cfg.route("/ws", web::get().to(ws_upgrade));
}
