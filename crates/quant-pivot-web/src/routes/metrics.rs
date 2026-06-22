//! Prometheus metrics scrape endpoint (`GET /metrics`).

use actix_web::{HttpResponse, Responder, web};

use crate::state::AppState;

/// `GET /metrics` — Prometheus text exposition format.
pub async fn metrics(state: web::Data<AppState>) -> impl Responder {
    let body = state.metrics.gather_prometheus();
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4; charset=utf-8")
        .body(body)
}
