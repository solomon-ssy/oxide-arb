//! Health probe API contract.

use serde::Serialize;

/// Liveness/readiness probe payload.
#[derive(Debug, Serialize)]
pub struct HealthStatus {
    /// `"ok"` for liveness, `"ready"` for readiness.
    pub status: &'static str,
}
