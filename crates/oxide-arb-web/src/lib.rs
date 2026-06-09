//! `oxide-arb-web` — HTTP API + RBAC control-plane (Phase 6 complete).
//!
//! Delivers the full web tier: JWT authentication (access/refresh + Redis
//! revocation blacklist, fail-closed on store outage), Casbin authorization,
//! dual-track audit (governance hash chain + operation log), governance routes
//! (`ActingRoleGoverned` for money-critical runtime mutations), business reads
//! and controls, WebSocket fanout, and production SPA static serving.
//!
//! [`spawn_web_server`] is the production entry point (bind + graceful
//! shutdown). It is queued by `oxide-arb-core::AppContext::queue_web_server`.
//! The operation-log writer is owned by core for unified staged shutdown.
//! Integration tests build an equivalent `App` via [`cors_from`],
//! [`middleware::request_id`], and [`routes::configure`].

pub mod audit;
pub mod auth;
pub mod error;
pub mod extractors;
pub mod jwt;
pub mod middleware;
pub mod readiness;
pub mod response;
pub mod routes;
pub mod state;
pub mod static_files;
pub mod ws;

use std::sync::Arc;

use actix_cors::Cors;
use actix_web::{App, HttpServer, middleware::from_fn, web};
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::config::WebConfig;
use tokio_util::sync::CancellationToken;
use tracing_actix_web::TracingLogger;

pub use state::AppState;

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
///
/// The operation-log writer is a separate process-level task owned by
/// `oxide-arb-core` (`queue_operation_log_writer`), draining the receiver paired
/// with `state.operation_log`; it is intentionally *not* spawned here so that it
/// participates in the unified staged shutdown.
pub async fn spawn_web_server(
    state: AppState,
    config: WebConfig,
    shutdown: CancellationToken,
) -> OxideResult<()> {
    let jwt_blacklist = Arc::clone(&state.jwt_blacklist);
    let data = web::Data::new(state);
    let bind_addr = (config.listen_host.clone(), config.listen_port);
    let app_config = config.clone();

    let server = HttpServer::new(move || {
        let static_config = app_config.clone();
        App::new()
            .app_data(data.clone())
            .wrap(TracingLogger::default())
            .wrap(cors_from(&app_config))
            .wrap(from_fn(middleware::request_id))
            // Outermost: observes the final status + inner-injected attributes.
            .wrap(from_fn(middleware::operation_audit))
            .configure(routes::configure)
            // Static SPA registered last so API routes take precedence.
            .configure(move |cfg| static_files::configure_static(cfg, &static_config))
    })
    .bind(bind_addr)
    .map_err(|error| OxideError::Internal(format!("web server bind failed: {error}")))?
    .run();

    let handle = server.handle();
    tokio::select! {
        biased;
        () = shutdown.cancelled() => {
            handle.stop(true).await;
            jwt_blacklist.close_pool();
            Ok(())
        }
        result = server => {
            result.map_err(|error| OxideError::Internal(format!("web server error: {error}")))
        }
    }
}
