//! Prometheus metrics scrape endpoint (`GET /metrics`).
//!
//! Mounted at the HTTP root (outside `/api`) so Kubernetes `ServiceMonitor` /
//! `PodMonitor` configs stay trivial, matching ng-gateway's public metrics
//! route. The handler is unauthenticated — network policy / bind address must
//! restrict scrape access in production.

use actix_web::{HttpResponse, Responder, web};
use tracing::warn;

use crate::{error::WebError, state::AppState};

/// `GET /metrics` — Prometheus text exposition format.
pub async fn metrics(state: web::Data<AppState>) -> Result<impl Responder, WebError> {
    let payload = state.metrics.scrape_prometheus().map_err(|error| {
        warn!(%error, "prometheus scrape encode failed");
        WebError::Internal("metrics scrape failed".to_owned())
    })?;
    Ok(HttpResponse::Ok()
        .content_type(payload.content_type)
        .body(payload.body))
}
