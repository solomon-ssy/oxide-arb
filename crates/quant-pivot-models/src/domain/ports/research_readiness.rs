//! Read-only operational research-readiness evidence boundary.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;

/// Verified, current evidence used by the operator dashboard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchReadinessSnapshot {
    pub observed_at: DateTime<Utc>,
    pub required_history_days: u32,
    pub observed_history_days: Option<u32>,
    pub retention_ready: bool,
    pub latency_ready: bool,
}

/// Fail-closed read surface over signed operational readiness evidence.
#[async_trait]
pub trait ResearchReadinessPort: Send + Sync {
    async fn snapshot(&self) -> QuantResult<Option<ResearchReadinessSnapshot>>;
}
