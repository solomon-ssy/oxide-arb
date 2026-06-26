//! Web-facing runtime control ports (dependency inversion).

use crate::{
    domain::{
        ReadinessReport,
        data_plane::DataQualitySnapshot,
        governance::{
            kill_switch::KillSwitchView,
            mode::PreflightReport,
            system::{HealthReport, SystemStatus},
        },
        market::book::BookSnapshot,
    },
    enums::{execution::KillSwitchState, quant::QuantRuntimeMode},
    runtime_config::RuntimeConfig,
    types::TokenId,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, control::ControlError};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc};

/// Gamma catalog warmup state for operator dashboards.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum CatalogState {
    Warming,
    Ready {
        markets: u64,
        synced_at: DateTime<Utc>,
    },
}

impl CatalogState {
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }
}

/// Catalog readiness surface (dependency-inverted).
pub trait CatalogStatusPort: Send + Sync {
    fn catalog_state(&self) -> CatalogState;
    fn is_ready(&self) -> bool;
}

/// Outcome of a successful governed quant runtime mode transition.
#[derive(Debug, Clone, Serialize)]
pub struct QuantModeTransitionReport {
    pub from: QuantRuntimeMode,
    pub to: QuantRuntimeMode,
    /// Preflight evidence for an upgrade transition; `None` for no-ops and
    /// downgrades (which skip business preflight).
    pub preflight: Option<PreflightReport>,
}

#[async_trait]
pub trait RuntimeControlPort: Send + Sync {
    fn quant_runtime_mode(&self) -> QuantRuntimeMode;

    /// Run the transition gate + (upgrade-only) preflight and persist the new
    /// mode fail-closed. Forbidden edges / failed preflight return a typed
    /// [`ExecutionError`](quant_pivot_error::execution::ExecutionError) and do
    /// **not** mutate state.
    async fn switch_quant_mode(
        &self,
        target: QuantRuntimeMode,
        actor: &str,
        reason: &str,
    ) -> QuantResult<QuantModeTransitionReport>;

    fn system_status(&self) -> SystemStatus;

    async fn health(&self) -> HealthReport;
}

/// Governed request to transition the operational kill-switch.
#[derive(Debug, Clone)]
pub struct SetKillSwitchCommand {
    /// Target FSM state.
    pub target: KillSwitchState,
    /// Acting operator identity (audit `changed_by`).
    pub actor: String,
    /// Mandatory operator justification.
    pub reason: String,
    /// Operator acknowledgement, required to clear `emergency_halted`.
    pub ack: bool,
}

/// Operational kill-switch control boundary consumed by the web layer.
///
/// The hot-path query methods (`allows_new_entry` etc.) live on
/// [`KillSwitchState`](crate::enums::execution::KillSwitchState) and the
/// in-process handle; this port exposes only the governed read/write surface
/// needed by the HTTP control plane.
#[async_trait]
pub trait KillSwitchPort: Send + Sync {
    /// Current operational state (lock-free read).
    fn current(&self) -> KillSwitchState;

    /// Full operator projection of the current singleton.
    fn view(&self) -> KillSwitchView;

    /// Persist, hot-swap, audit, and meter a governed state transition.
    async fn set(&self, command: SetKillSwitchCommand) -> QuantResult<KillSwitchView>;
}

#[async_trait]
pub trait RuntimeConfigPort: Send + Sync {
    fn current(&self) -> Arc<RuntimeConfig>;
    fn preflight(&self, candidate: &RuntimeConfig) -> Result<(), ControlError>;
    async fn apply(&self, config: RuntimeConfig) -> Result<(), ControlError>;
}

#[async_trait]
pub trait MarketDataPort: Send + Sync {
    fn book(
        &self,
        yes_token: &TokenId,
        no_token: &TokenId,
    ) -> (Option<Arc<BookSnapshot>>, Option<Arc<BookSnapshot>>);

    fn subscribed_tokens(&self, token_ids: &[TokenId]) -> HashSet<TokenId>;

    async fn subscribe(&self, token_ids: Vec<TokenId>) -> Result<(), ControlError>;

    async fn unsubscribe(&self, token_ids: Vec<TokenId>) -> Result<(), ControlError>;
}

pub trait MetricsScrapePort: Send + Sync {
    fn gather_prometheus(&self) -> String;
}

/// Read-only data-quality observability surface (dependency-inverted).
pub trait DataQualityPort: Send + Sync {
    /// Aggregate classification of the live book plane at call time.
    fn snapshot(&self) -> DataQualitySnapshot;
}

#[async_trait]
pub trait ReadinessPort: Send + Sync {
    async fn check(&self) -> ReadinessReport;
}
