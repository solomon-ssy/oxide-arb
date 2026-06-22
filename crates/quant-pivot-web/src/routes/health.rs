//! Liveness/readiness probes (public, unauthenticated).

use actix_web::{HttpResponse, Responder, web};

use quant_pivot_models::domain::{HealthStatus, ReadinessStatus};

use crate::{response::WebResponse, state::AppState};

/// Liveness probe — the process is up.
pub async fn health() -> impl Responder {
    WebResponse::ok(HealthStatus { status: "ok" })
}

/// Readiness probe — `PostgreSQL` + Redis must be reachable before traffic is admitted.
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

/// Mount public health probes and metrics at the HTTP root.
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.route("/health", web::get().to(health))
        .route("/ready", web::get().to(ready));
}
