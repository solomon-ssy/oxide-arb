//! Process bootstrap, observability, and runtime infrastructure errors.

use thiserror::Error;

/// Failures during startup wiring, metrics, channels, and server runtime.
#[derive(Debug, Error)]
pub enum InfraError {
    #[error("metrics registration failed for {subsystem}: {detail}")]
    MetricsRegistration {
        subsystem: &'static str,
        detail: String,
    },

    #[error("misconfigured startup: {detail}")]
    Misconfigured { detail: String },

    #[error("channel closed: {name}")]
    ChannelClosed { name: &'static str },

    #[error("blocking task join failed: {detail}")]
    BlockingTaskJoin { detail: String },

    #[error("web server bind failed: {detail}")]
    ServerBind { detail: String },

    #[error("web server runtime error: {detail}")]
    ServerRuntime { detail: String },
}
