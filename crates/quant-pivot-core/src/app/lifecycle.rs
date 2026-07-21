//! Graceful shutdown signal handling.

use std::process::exit;

use tokio::signal::{unix, unix::SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Wait for the first OS termination signal and cancel `token`.
pub async fn shutdown_signal(token: CancellationToken) {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    let terminate = async {
        unix::signal(SignalKind::terminate())
            .expect("SIGTERM handler registration is infallible in a running tokio runtime")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = future::pending::<()>();

    tokio::select! {
        () = async { ctrl_c.await.expect("ctrl-c listener is infallible in a running tokio runtime") } => {
            info!("Received SIGINT — initiating graceful shutdown");
        }
        () = terminate => {
            info!("Received SIGTERM — initiating graceful shutdown");
        }
    }

    token.cancel();
}

/// Watch for a second termination signal during the drain window.
pub async fn force_exit_on_second_signal() {
    let ctrl_c = tokio::signal::ctrl_c();

    #[cfg(unix)]
    let terminate = async {
        unix::signal(SignalKind::terminate())
            .expect("SIGTERM handler registration is infallible in a running tokio runtime")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = future::pending::<()>();

    tokio::select! {
        () = async { ctrl_c.await.expect("ctrl-c listener is infallible in a running tokio runtime") } => {
            warn!("Received second signal — forcing immediate exit");
        }
        () = terminate => {
            warn!("Received second SIGTERM — forcing immediate exit");
        }
    }

    exit(1);
}
