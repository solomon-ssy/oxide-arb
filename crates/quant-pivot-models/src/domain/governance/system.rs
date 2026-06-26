//! System status, lifecycle, config, accounting, and reporting domain models.

use crate::{
    domain::{
        lifecycle::{MarketDataConnectivity, OperationalPhase, WsShardConnectivity},
        ports::runtime_control::CatalogState,
    },
    enums::{
        quant::QuantRuntimeMode,
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
        system::ShutdownStage,
    },
    types::{
        AuditEventId, ContentHash, RuntimeConfigActivationId, RuntimeConfigVersionId, SchemaVersion,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Execution kill-switch class exposed on operator dashboards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionEmergencyClassView {
    VenueFault,
    ReservationFault,
    PersistenceFault,
}

/// Execution kill-switch snapshot for operator dashboards and WS `system.status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionEmergencyView {
    pub active: bool,
    pub class: ExecutionEmergencyClassView,
    pub requires_operator_ack: bool,
    pub last_reason: Option<String>,
}

impl ExecutionEmergencyView {
    /// Idle snapshot when the execution FSM is not in emergency halt.
    #[must_use]
    pub const fn idle() -> Self {
        Self {
            active: false,
            class: ExecutionEmergencyClassView::VenueFault,
            requires_operator_ack: false,
            last_reason: None,
        }
    }
}

/// Overall system status reported by the health endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatus {
    pub quant_runtime_mode: QuantRuntimeMode,
    pub uptime_secs: u64,
    pub active_markets: u32,
    /// Market-catalog warmup state; report generation is gated until `Ready`.
    pub catalog: CatalogState,
    /// Authoritative operator lifecycle for report and optional execution modes.
    pub operational_phase: OperationalPhase,
    /// CLOB websocket market-data readiness snapshot.
    pub market_data: MarketDataConnectivity,
    /// Global execution kill-switch snapshot.
    pub execution_emergency: ExecutionEmergencyView,
    pub checked_at: DateTime<Utc>,
}

/// Health check results for all subsystems.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    pub overall_healthy: bool,
    pub checks: Vec<SubsystemHealth>,
    pub checked_at: DateTime<Utc>,
}

/// Outcome of a single subsystem health probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SubsystemCheckStatus {
    Healthy,
    Unhealthy,
    Skipped { reason: String },
}

/// Health status of a single subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubsystemHealth {
    pub name: String,
    pub status: SubsystemCheckStatus,
    pub latency_ms: Option<u64>,
    pub detail: Option<String>,
}

impl SubsystemHealth {
    /// Whether this probe counts toward `HealthReport::overall_healthy`.
    #[must_use]
    pub const fn counts_toward_overall(&self) -> bool {
        !matches!(self.status, SubsystemCheckStatus::Skipped { .. })
    }

    /// Legacy-style healthy flag for metrics and quick checks.
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        matches!(self.status, SubsystemCheckStatus::Healthy)
    }

    #[must_use]
    pub fn healthy(name: impl Into<String>, latency_ms: Option<u64>) -> Self {
        Self {
            name: name.into(),
            status: SubsystemCheckStatus::Healthy,
            latency_ms,
            detail: None,
        }
    }

    #[must_use]
    pub fn unhealthy(
        name: impl Into<String>,
        latency_ms: Option<u64>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            status: SubsystemCheckStatus::Unhealthy,
            latency_ms,
            detail: Some(detail.into()),
        }
    }

    #[must_use]
    pub fn skipped(name: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: SubsystemCheckStatus::Skipped {
                reason: reason.into(),
            },
            latency_ms: None,
            detail: None,
        }
    }
}

impl SystemStatus {
    /// Minimal Phase 0 bootstrap status projection.
    #[must_use]
    pub fn report_only_bootstrap(quant_runtime_mode: QuantRuntimeMode) -> Self {
        Self {
            quant_runtime_mode,
            uptime_secs: 0,
            active_markets: 0,
            catalog: CatalogState::Warming,
            operational_phase: OperationalPhase::CatalogWarming,
            market_data: MarketDataConnectivity {
                ready: false,
                last_message_age_ms: None,
                ws_shards: WsShardConnectivity {
                    total: 0,
                    disconnected: 0,
                    oldest_disconnected_secs: None,
                    connected_ratio_bps: 0,
                },
            },
            execution_emergency: ExecutionEmergencyView::idle(),
            checked_at: Utc::now(),
        }
    }
}

impl HealthReport {
    /// Recompute aggregate health from non-skipped subsystem probes.
    #[must_use]
    pub fn from_checks(checks: Vec<SubsystemHealth>, checked_at: DateTime<Utc>) -> Self {
        let overall_healthy = checks
            .iter()
            .filter(|check| check.counts_toward_overall())
            .all(SubsystemHealth::is_healthy);
        Self {
            overall_healthy,
            checks,
            checked_at,
        }
    }
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
//
// The typed schema for `config_json` is `crate::runtime_config::RuntimeConfig`
// (`schema_version = 5`). This module only carries the persistence DTOs.

/// DB row projection for the immutable `runtime_config_version` table.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::runtime_config_version::Entity")]
pub struct RuntimeConfigVersionInfo {
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub config_hash: ContentHash,
    pub schema_version: SchemaVersion,
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
    pub config_hash: ContentHash,
    pub schema_version: SchemaVersion,
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
    pub audit_event_id: Option<AuditEventId>,
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
    pub audit_event_id: Option<AuditEventId>,
}

// ── System runtime state (operational control singleton) ─────────────

/// DB row projection for the `system_runtime_state` singleton.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::system_runtime_state::Entity")]
pub struct SystemRuntimeStateInfo {
    pub id: i32,
    pub quant_runtime_mode: crate::enums::quant::QuantRuntimeMode,
    pub changed_by: String,
    pub reason: String,
    pub changed_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    SystemRuntimeStateInfo,
    crate::entities::system_runtime_state::Model,
    {
        id,
        quant_runtime_mode,
        changed_by,
        reason,
        changed_at,
        updated_at,
    }
);

/// Upsert payload for the runtime-mode singleton (`id` is always the singleton key).
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::system_runtime_state::ActiveModel")]
pub struct UpsertSystemRuntimeState {
    pub id: i32,
    pub quant_runtime_mode: QuantRuntimeMode,
    pub changed_by: String,
    pub reason: String,
    pub changed_at: DateTime<Utc>,
}
