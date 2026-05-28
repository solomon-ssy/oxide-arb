//! System status, lifecycle, config, accounting, and reporting domain models.

use crate::{
    enums::{
        common::{ExecutionMode, ReportType},
        lifecycle::ShutdownStage,
        risk::BreakerStateName,
        runtime_config::RuntimeConfigKey,
    },
    types::{PeriodId, Probability, Usd},
};
use chrono::{DateTime, NaiveDate, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
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

// ── Runtime config ───────────────────────────────────────────────────

/// DB row projection for the `runtime_config` table.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::runtime_config::Entity")]
pub struct RuntimeConfigInfo {
    pub key: RuntimeConfigKey,
    pub value: serde_json::Value,
    pub description: Option<String>,
    pub updated_by: String,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(RuntimeConfigInfo, crate::entities::runtime_config::Model, {
    key, value, description, updated_by, updated_at,
});

/// Upsert payload for the `runtime_config` table.
#[derive(Debug, Clone)]
pub struct UpsertRuntimeConfig {
    pub key: RuntimeConfigKey,
    pub value: serde_json::Value,
    pub updated_by: String,
}

// ── Accounting ───────────────────────────────────────────────────────

/// DB row projection for the `accounting_period` table.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::accounting_period::Entity")]
pub struct AccountingPeriodInfo {
    pub period_id: PeriodId,
    pub period_type: ReportType,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub realized_pnl: Usd,
    pub total_fees: Usd,
    pub trade_count: i32,
    pub win_count: i32,
    pub loss_count: i32,
    pub miss_count: i32,
    pub max_drawdown: Usd,
    pub sharpe_ratio: Option<Probability>,
    pub finalized: bool,
    pub created_at: DateTime<Utc>,
}

info_from_model!(AccountingPeriodInfo, crate::entities::accounting_period::Model, {
    period_id, period_type, start_date, end_date, realized_pnl, total_fees,
    trade_count, win_count, loss_count, miss_count, max_drawdown,
    sharpe_ratio, finalized, created_at,
});

/// Write DTO for creating a new accounting period.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::accounting_period::ActiveModel")]
pub struct NewAccountingPeriod {
    pub period_id: PeriodId,
    pub period_type: ReportType,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
}

/// Partial update for an accounting period.
#[derive(Debug, Clone, Default)]
pub struct UpdateAccountingPeriod {
    pub realized_pnl: Option<Usd>,
    pub total_fees: Option<Usd>,
    pub trade_count: Option<i32>,
    pub win_count: Option<i32>,
    pub loss_count: Option<i32>,
    pub miss_count: Option<i32>,
    pub max_drawdown: Option<Usd>,
    pub sharpe_ratio: Option<Probability>,
    pub finalized: Option<bool>,
}
