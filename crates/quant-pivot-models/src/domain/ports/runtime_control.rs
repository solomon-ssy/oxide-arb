//! Web-facing runtime control ports (dependency inversion).

use crate::{
    domain::{
        ActivateBootstrapRequest, BootstrapView, ReadinessReport, SystemCapabilities,
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

#[async_trait]
pub trait BootstrapPort: Send + Sync {
    fn view(&self) -> BootstrapView;
    fn subscribe(&self) -> tokio::sync::watch::Receiver<BootstrapView>;
    fn capability_snapshot(&self) -> SystemCapabilities;
    fn subscribe_capabilities(&self) -> tokio::sync::watch::Receiver<SystemCapabilities>;
    fn refresh_operational_capabilities(&self, status: &SystemStatus) -> SystemCapabilities;
    async fn capabilities(&self, status: &SystemStatus) -> QuantResult<SystemCapabilities>;
    async fn mark_catalog_ready(&self) -> QuantResult<BootstrapView>;
    async fn activate(
        &self,
        request: ActivateBootstrapRequest,
        actor: &str,
        acting_role: &str,
    ) -> QuantResult<BootstrapView>;
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
    /// Operator acknowledgement, required to loosen a latched state.
    pub ack: bool,
    /// Latch this transition: clearing/loosening it later requires operator ack.
    ///
    /// Set by automated escalation (e.g. the execution breaker tripping
    /// `execution_halted`). `emergency_halted` always latches regardless.
    pub latch: bool,
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
    /// Resolve and validate every fallible dependency without mutating live state.
    async fn prepare(&self, config: RuntimeConfig) -> Result<PreparedRuntimeConfig, ControlError>;
}

/// One-shot, fully validated runtime snapshot publication command.
///
/// The callback contains only infallible in-memory swaps. Constructing this
/// value is fallible; consuming it after the durable activation commits is not.
pub struct PreparedRuntimeConfig {
    config: Arc<RuntimeConfig>,
    publish: Box<dyn FnOnce() + Send + 'static>,
}

impl PreparedRuntimeConfig {
    #[must_use]
    pub fn new(config: Arc<RuntimeConfig>, publish: impl FnOnce() + Send + 'static) -> Self {
        Self {
            config,
            publish: Box::new(publish),
        }
    }

    #[must_use]
    pub const fn config(&self) -> &Arc<RuntimeConfig> {
        &self.config
    }

    pub fn publish(self) {
        (self.publish)();
    }
}

#[async_trait]
pub trait MarketDataPort: Send + Sync {
    /// Load one token's current immutable L2 snapshot.
    fn book_for_token(&self, token_id: &TokenId) -> Option<Arc<BookSnapshot>> {
        self.book(token_id, token_id).0
    }

    fn book(
        &self,
        yes_token: &TokenId,
        no_token: &TokenId,
    ) -> (Option<Arc<BookSnapshot>>, Option<Arc<BookSnapshot>>);

    fn subscribed_tokens(&self, token_ids: &[TokenId]) -> HashSet<TokenId>;

    /// Union of all tokens currently live on the CLOB WS transport (engine +
    /// web overlay). Used to resolve `MarketPageQuery::subscribed` server-side.
    fn all_subscribed_tokens(&self) -> HashSet<TokenId>;

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
