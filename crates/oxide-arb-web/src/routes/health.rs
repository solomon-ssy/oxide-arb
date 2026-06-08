//! Liveness/readiness probes (public, unauthenticated).

use actix_web::Responder;
use serde::Serialize;

use crate::response::WebResponse;

/// Probe payload.
#[derive(Debug, Serialize)]
struct HealthStatus {
    status: &'static str,
}

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
