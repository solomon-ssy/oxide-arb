//! System status and health reporting domain models.

use crate::enums::common::ExecutionMode;
use crate::enums::lifecycle::ShutdownStage;
use crate::enums::risk::BreakerStateName;
use crate::types::Usd;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Overall system status reported by the health endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatus {
    pub execution_mode: ExecutionMode,
    pub breaker_state: BreakerStateName,
    pub uptime_secs: u64,
    pub active_markets: u32,
    pub open_positions: u32,
    pub pending_reservations: u32,
    pub total_exposure: Usd,
    pub daily_pnl: Usd,
    pub checked_at: DateTime<Utc>,
}

/// Health check results for all subsystems.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    pub overall_healthy: bool,
    pub checks: Vec<SubsystemHealth>,
    pub checked_at: DateTime<Utc>,
}

/// Health status of a single subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubsystemHealth {
    pub name: String,
    pub healthy: bool,
    pub latency_ms: Option<u64>,
    pub detail: Option<String>,
}

/// Shutdown progress tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownProgress {
    pub stage: ShutdownStage,
    pub inflight_trades: u32,
    pub pending_flushes: u32,
    pub started_at: DateTime<Utc>,
}
