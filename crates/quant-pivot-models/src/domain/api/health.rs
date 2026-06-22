//! Health probe API contract.

use serde::Serialize;

/// Per-dependency readiness probe result.
#[derive(Debug, Clone, Serialize)]
pub struct DependencyCheck {
    pub name: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Liveness probe payload (`GET /health`).
#[derive(Debug, Serialize)]
pub struct HealthStatus {
    /// Always `"ok"` when the process is up.
    pub status: &'static str,
}

/// Readiness probe payload (`GET /ready`).
#[derive(Debug, Serialize)]
pub struct ReadinessStatus {
    /// `"ready"` when all required dependencies are reachable; `"not_ready"` otherwise.
    pub status: &'static str,
    /// Per-dependency probe results (`PostgreSQL` + Redis for the web tier).
    pub checks: Vec<DependencyCheck>,
}

/// Internal readiness report returned by [`ReadinessPort::check`].
#[derive(Debug, Clone)]
pub struct ReadinessReport {
    pub ready: bool,
    pub checks: Vec<DependencyCheck>,
}
