//! Liveness/readiness probes (public, unauthenticated).

use actix_web::Responder;
use oxide_arb_models::domain::HealthStatus;

use crate::response::WebResponse;

/// Liveness probe — the process is up.
pub async fn health() -> impl Responder {
    WebResponse::ok(HealthStatus { status: "ok" })
}

/// Readiness probe — the process is ready to serve traffic.
///
/// This sub-phase has no external readiness gate; later sub-phases may add
/// dependency checks (DB/Redis) here.
pub async fn ready() -> impl Responder {
    WebResponse::ok(HealthStatus { status: "ready" })
}
