//! Liveness/readiness probes (public, unauthenticated).

use actix_web::{HttpResponse, Responder, web};

use oxide_arb_models::domain::{HealthStatus, ReadinessStatus};

use crate::{response::WebResponse, state::AppState};

/// Liveness probe — the process is up.
pub async fn health() -> impl Responder {
    WebResponse::ok(HealthStatus { status: "ok" })
}

/// Readiness probe — `PostgreSQL` + Redis must be reachable before traffic is admitted.
///
/// Returns HTTP 200 with `status: "ready"` when all required dependencies pass;
/// HTTP 503 with `status: "not_ready"` and per-check detail otherwise so
/// orchestrators stop routing before auth/session infrastructure is usable.
pub async fn ready(state: web::Data<AppState>) -> impl Responder {
    let report = state.readiness.check().await;
    let body = WebResponse::ok(ReadinessStatus {
        status: if report.ready { "ready" } else { "not_ready" },
        checks: report.checks,
    });
    if report.ready {
        HttpResponse::Ok().json(body)
    } else {
        HttpResponse::ServiceUnavailable().json(body)
    }
}
