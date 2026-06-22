//! Health probe API contract.

use crate::domain::DependencyCheck;
use serde::Serialize;

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
