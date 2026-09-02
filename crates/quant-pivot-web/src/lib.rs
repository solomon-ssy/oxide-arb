//! `quant-pivot-web` — HTTP API and RBAC control plane.
//!
//! Delivers the full web tier: JWT authentication (access/refresh + Redis
//! revocation blacklist, fail-closed on store outage), Casbin authorization,
//! dual-track audit (governance hash chain + operation log), governance routes
//! (`ActingRoleGoverned` for money-critical runtime mutations), business reads
//! and controls, WebSocket fanout, and production SPA static serving.
//!
//! [`spawn_web_server`] is the production entry point (bind + graceful
//! shutdown). It is queued by `quant-pivot-core::AppContext::queue_web_server`.
//! The operation-log writer is owned by core for unified staged shutdown.
//! Integration tests build an equivalent `App` via [`cors_from`],
//! [`middleware::request_id`], and [`routes::configure`].

use quant_pivot_allocator as _;

pub mod audit;
pub mod auth;
pub mod error;
pub mod extractors;
pub mod jwt;
pub mod middleware;
pub mod readiness;
mod request_security;
mod request_tracing;
pub mod response;
pub mod routes;
mod runtime_scope;
mod server_lifecycle;
pub mod state;
pub mod static_files;
pub mod ws;

use std::time::Duration;

use actix_cors::Cors;
use actix_web::{App, HttpServer, middleware::from_fn, web::Data};
use quant_pivot_error::{QuantResult, infra::InfraError};
use quant_pivot_models::config::WebConfig;
use request_tracing::HttpRootSpanBuilder;
use server_lifecycle::ServerLifecycle;
use state::AppState;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use tracing_actix_web::TracingLogger;

/// Build the CORS middleware from configuration.
///
/// An empty `cors_allowed_origins` list yields a policy that admits no
/// cross-origin requests (same-origin only).
pub fn cors_from(config: &WebConfig) -> Cors {
    let mut cors = Cors::default()
        .allow_any_header()
        .allow_any_method()
        .max_age(3600);
    for origin in &config.cors_allowed_origins {
        cors = cors.allowed_origin(origin);
    }
    cors
}

/// Run the HTTP server until `shutdown` is cancelled.
///
/// Binds `listen_host:listen_port`, wraps every request with the operation-audit,
/// request-id, tracing, and CORS middleware, and serves the configured routes.
/// On cancellation it performs a graceful stop (draining in-flight requests).
/// `shutdown_grace` is supplied by the process shutdown-budget owner and must
/// leave time in that stage for worker cancellation and the final stop ACK.
///
/// The operation-log writer is a separate process-level task owned by
/// `quant-pivot-core` (`queue_operation_log_writer`), draining the receiver paired
/// with `state.operation_log`; it is intentionally *not* spawned here so that it
/// participates in the unified staged shutdown.
pub async fn spawn_web_server(
    state: AppState,
    config: WebConfig,
    shutdown: CancellationToken,
    shutdown_grace: Duration,
) -> QuantResult<()> {
    let data = Data::new(state);
    let bind_addr = (config.listen_host.clone(), config.listen_port);
    let app_config = config.clone();
    let process_runtime = Handle::current();

    let server = HttpServer::new(move || {
        let static_config = app_config.clone();
        let request_runtime = process_runtime.clone();
        App::new()
            .app_data(data.clone())
            .wrap(TracingLogger::<HttpRootSpanBuilder>::new())
            .wrap(cors_from(&app_config))
            .wrap(from_fn(middleware::request_id))
            // Outermost: observes the final status + inner-injected attributes.
            .wrap(from_fn(middleware::operation_audit))
            // Pooled I/O must survive HTTP workers through later drain stages.
            .wrap(from_fn(move |request, next| {
                runtime_scope::request_runtime(request, next, request_runtime.clone())
            }))
            .configure(routes::configure)
            // Static SPA registered last so API routes take precedence.
            .configure(move |cfg| static_files::configure_static(cfg, &static_config))
    })
    .workers(1)
    .worker_max_blocking_threads(1)
    // The application lifecycle is the sole OS-signal and shutdown owner.
    .disable_signals()
    .shutdown_timeout(shutdown_grace.as_secs())
    .bind(bind_addr)
    .map_err(|error| InfraError::ServerBind {
        detail: error.to_string(),
    })?
    .run();

    // Shared persistence pools stay open through the later core drain stages.
    ServerLifecycle { server, shutdown }.run().await
}
