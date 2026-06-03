//! System status, lifecycle, config, accounting, and reporting domain models.

use crate::{
    domain::{NullablePatch, Patch},
    enums::{
        common::{ExecutionMode, ReportType},
        lifecycle::ShutdownStage,
        risk::BreakerStateName,
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    types::{PeriodId, Probability, RuntimeConfigActivationId, RuntimeConfigVersionId, Usd},
};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
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

/// Versioned runtime configuration document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfigDocument {
    pub schema_version: i32,
    pub operator: OperatorRuntimeConfig,
    pub detection: DetectionRuntimeConfig,
    pub execution: ExecutionRuntimeConfig,
    pub sizing: SizingRuntimeConfig,
    pub risk_limits: RiskLimitRuntimeConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorRuntimeConfig {
    pub maintenance_mode: bool,
    pub dry_run_mode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectionRuntimeConfig {
    pub min_profit_threshold_usd: Decimal,
    pub endgame_hours_before_close: u32,
    pub convergence_threshold: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRuntimeConfig {
    pub max_slippage_bps: u32,
    pub order_timeout_secs: u32,
    pub cooldown_after_trade_secs: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SizingRuntimeConfig {
    pub kelly_fraction: Decimal,
    pub max_position_fraction_of_book: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskLimitRuntimeConfig {
    pub max_portfolio_exposure_usd: Usd,
    pub max_single_position_usd: Usd,
    pub max_daily_loss_usd: Usd,
    pub circuit_breaker_threshold: u32,
}

/// DB row projection for the immutable `runtime_config_version` table.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::runtime_config_version::Entity")]
pub struct RuntimeConfigVersionInfo {
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub config_hash: String,
    pub schema_version: i32,
    pub config_json: serde_json::Value,
    pub source: RuntimeConfigVersionSource,
    pub created_by: String,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

info_from_model!(RuntimeConfigVersionInfo, crate::entities::runtime_config_version::Model, {
    runtime_config_version_id, config_hash, schema_version, config_json, source,
    created_by, reason, created_at,
});

/// Insert payload for `runtime_config_version`.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::runtime_config_version::ActiveModel")]
pub struct NewRuntimeConfigVersion {
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub config_hash: String,
    pub schema_version: i32,
    pub config_json: serde_json::Value,
    pub source: RuntimeConfigVersionSource,
    pub created_by: String,
    pub reason: String,
}

/// DB row projection for append-only runtime config activation history.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::runtime_config_activation::Entity")]
pub struct RuntimeConfigActivationInfo {
    pub runtime_config_activation_id: RuntimeConfigActivationId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub activated_at: DateTime<Utc>,
    pub activated_by: String,
    pub reason: String,
    pub activation_kind: RuntimeConfigActivationKind,
    pub previous_runtime_config_version_id: Option<RuntimeConfigVersionId>,
    pub rollback_target_version_id: Option<RuntimeConfigVersionId>,
    pub audit_event_id: Option<i64>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    RuntimeConfigActivationInfo,
    crate::entities::runtime_config_activation::Model,
    {
        runtime_config_activation_id,
        runtime_config_version_id,
        activated_at,
        activated_by,
        reason,
        activation_kind,
        previous_runtime_config_version_id,
        rollback_target_version_id,
        audit_event_id,
        created_at,
    }
);

/// Insert payload for `runtime_config_activation`.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::runtime_config_activation::ActiveModel")]
pub struct NewRuntimeConfigActivation {
    pub runtime_config_activation_id: RuntimeConfigActivationId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub activated_at: DateTime<Utc>,
    pub activated_by: String,
    pub reason: String,
    pub activation_kind: RuntimeConfigActivationKind,
    pub previous_runtime_config_version_id: Option<RuntimeConfigVersionId>,
    pub rollback_target_version_id: Option<RuntimeConfigVersionId>,
    pub audit_event_id: Option<i64>,
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
#[sea_orm(active_model = "crate::entities::accounting_period::ActiveModel")]
pub struct NewAccountingPeriod {
    pub period_id: PeriodId,
    pub period_type: ReportType,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
}

/// Partial update for an accounting period.
#[derive(Debug, Clone, Default, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::accounting_period::ActiveModel")]
pub struct AccountingPeriodPatch {
    pub realized_pnl: Patch<Usd>,
    pub total_fees: Patch<Usd>,
    pub trade_count: Patch<i32>,
    pub win_count: Patch<i32>,
    pub loss_count: Patch<i32>,
    pub miss_count: Patch<i32>,
    pub max_drawdown: Patch<Usd>,
    pub sharpe_ratio: NullablePatch<Probability>,
    pub finalized: Patch<bool>,
}

#[cfg(test)]
mod tests {
    use super::AccountingPeriodPatch;
    use crate::domain::{NullablePatch, Patch};
    use sea_orm::{ActiveValue, IntoActiveModel};

    #[test]
    fn accounting_patch_maps_keep_set_and_clear_to_active_values() {
        let active = AccountingPeriodPatch {
            finalized: Patch::set(true),
            sharpe_ratio: NullablePatch::clear(),
            ..Default::default()
        }
        .into_active_model();

        assert!(matches!(active.realized_pnl, ActiveValue::NotSet));
        assert!(matches!(active.finalized, ActiveValue::Set(true)));
        assert!(matches!(active.sharpe_ratio, ActiveValue::Set(None)));
    }
}
