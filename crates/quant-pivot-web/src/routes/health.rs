//! Liveness/readiness probes (public, unauthenticated).

use actix_web::{
    HttpResponse, Responder, web,
    web::{Data, ServiceConfig},
};
use quant_pivot_models::domain::api::{HealthStatus, ReadinessStatus};

use crate::{response::WebResponse, state::AppState};

/// Liveness probe — the process is up.
pub async fn health() -> impl Responder {
    WebResponse::ok(HealthStatus { status: "ok" })
}

/// Startup probe. The HTTP server is bound only after schema verification and
/// runtime bootstrap have completed, so reaching this handler proves startup.
pub async fn startup() -> impl Responder {
    WebResponse::ok(HealthStatus { status: "started" })
}

/// Readiness probe — `PostgreSQL` + Redis must be reachable before traffic is admitted.
pub async fn ready(state: Data<AppState>) -> impl Responder {
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

/// Mount public health probes and metrics at the HTTP root.
pub fn configure(cfg: &mut ServiceConfig) {
    cfg.route("/health", web::get().to(health))
        .route("/startup", web::get().to(startup))
        .route("/ready", web::get().to(ready));
}
