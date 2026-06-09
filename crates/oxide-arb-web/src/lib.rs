//! `oxide-arb-web` — HTTP API + RBAC control-plane.
//!
//! Phase 6.3 scope: the web foundation (state, response/error envelopes,
//! extractors), JWT authentication (access/refresh + Redis revocation
//! blacklist), the request-id and authn middleware, and the auth routes
//! (`login` / `refresh` / `logout` / `me`). Authorization (Casbin) and the
//! business/governance routes arrive in later sub-phases.
//!
//! [`spawn_web_server`] is the production entry point (bind + graceful
//! shutdown). It is not yet wired into `oxide-arb-core`; that happens in Phase
//! 6.6. Integration tests build an equivalent `App` via [`cors_from`],
//! [`middleware::request_id`], and [`routes::configure`].

pub mod audit;
pub mod auth;
pub mod error;
pub mod extractors;
pub mod jwt;
pub mod middleware;
pub mod response;
pub mod routes;
pub mod state;

use std::{sync::Arc, time::Duration};

use actix_cors::Cors;
use actix_web::{App, HttpServer, middleware::from_fn, web};
use flume::Receiver;
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::{config::WebConfig, domain::NewOperationLog};
use tokio_util::sync::CancellationToken;
use tracing_actix_web::TracingLogger;

pub use state::AppState;

/// Operation-log writer flush threshold (rows) and cadence.
const OPERATION_LOG_BATCH_SIZE: usize = 64;
const OPERATION_LOG_FLUSH_INTERVAL: Duration = Duration::from_millis(250);

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
/// Binds `listen_host:listen_port`, spawns the operation-log writer task (draining
/// `operation_log_rx` into the operation log until `shutdown`), wraps every
/// request with the operation-audit, request-id, tracing, and CORS middleware,
/// and serves the configured routes. On cancellation it performs a graceful stop
/// (draining in-flight requests); the writer flushes its tail on the same token.
///
/// `operation_log_rx` is the receiver paired with the
/// [`OperationLogBuffer`](audit::OperationLogBuffer) held in
/// `state.operation_log` — create both with
/// [`OperationLogBuffer::new`](audit::OperationLogBuffer::new).
pub async fn spawn_web_server(
    state: AppState,
    config: WebConfig,
    operation_log_rx: Receiver<NewOperationLog>,
    shutdown: CancellationToken,
) -> OxideResult<()> {
    // Drain buffered operation-log rows into Postgres in the background.
    let writer_repo = Arc::clone(&state.operation_logs);
    let writer_shutdown = shutdown.clone();
    tokio::spawn(audit::spawn_operation_log_writer(
        operation_log_rx,
        writer_repo,
        OPERATION_LOG_BATCH_SIZE,
        OPERATION_LOG_FLUSH_INTERVAL,
        writer_shutdown,
    ));

    let data = web::Data::new(state);
    let bind_addr = (config.listen_host.clone(), config.listen_port);
    let cors_config = config.clone();

    let server = HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .wrap(TracingLogger::default())
            .wrap(cors_from(&cors_config))
            .wrap(from_fn(middleware::request_id))
            // Outermost: observes the final status + inner-injected attributes.
            .wrap(from_fn(middleware::operation_audit))
            .configure(routes::configure)
    })
    .bind(bind_addr)
    .map_err(|error| OxideError::Internal(format!("web server bind failed: {error}")))?
    .run();

    let handle = server.handle();
    tokio::select! {
        biased;
        () = shutdown.cancelled() => {
            handle.stop(true).await;
            Ok(())
        }
        result = server => {
            result.map_err(|error| OxideError::Internal(format!("web server error: {error}")))
        }
    }
}
