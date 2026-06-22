//! `/ws` WebSocket route registration.
//!
//! The WebSocket upgrade self-authenticates via a query-string access token
//! *before* the protocol upgrade (browsers cannot set handshake headers on a
//! WebSocket request), so it deliberately sits **outside** the RBAC route
//! manifest ([`super::protected_route_specs`]), the Bearer-header `authn`
//! scope, and [`super::version::ApiV1Guard`] (browsers cannot send
//! `Accept-Api-Version` on upgrade). Per-channel authorization is enforced
//! inside the session against the
//! same `(resource, operation)` pairs as the HTTP routes (via
//! [`quant_pivot_models::domain::WsChannel::resource`]), so a socket cannot bypass
//! route-level authorization.
//!
//! The handshake + session implementation lives in [`crate::ws`]; this module
//! is only the registration point so all routes are wired through `routes/`.

use actix_web::web::{self, ServiceConfig};

use crate::ws::handler::ws_upgrade;

/// Register the `/ws` upgrade route onto the API-prefixed scope (outside
/// [`super::version::ApiV1Guard`]).
pub fn configure(cfg: &mut ServiceConfig) {
    cfg.route("/ws", web::get().to(ws_upgrade));
}
