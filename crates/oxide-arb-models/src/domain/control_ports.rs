//! Web-facing runtime control ports (dependency inversion).
//!
//! `oxide-arb-web` must not depend on `oxide-arb-core`, yet the control plane
//! has to drive money-critical runtime operations — halt/resume, execution-mode
//! hot-swap, circuit-breaker reset, blacklist mutations — that live in core.
//! These ports invert that dependency: the trait and its DTOs live here in the
//! shared model crate, `oxide-arb-core` implements them, and the web `AppState`
//! holds the abstraction as `Arc<dyn RuntimeControlPort>`.

use crate::{
    domain::{
        BlacklistInfo, HealthReport, RiskEngineState, SystemStatus,
        control_factor::{
            ControlFactorMaterializationRunInfo, MarketFilterSpec, ReplayAccountScope,
        },
        market::book::BookSnapshot,
    },
    enums::{common::ExecutionMode, control_factor::ControlFactorType, risk::BlacklistReason},
    runtime_config::RuntimeConfig,
    types::{MarketId, TokenId},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use thiserror::Error;

/// Outcome of a successful governed execution-mode transition.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ModeTransitionReport {
    pub from: ExecutionMode,
    pub to: ExecutionMode,
}

/// Failure modes of a runtime control operation; mapped to HTTP by the web layer.
#[derive(Debug, Error)]
pub enum RuntimeControlError {
    /// A precondition for the requested transition was not satisfied, e.g.
    /// switching to Live without loaded credentials or a metrics refresher.
    #[error("precondition failed: {0}")]
    Precondition(String),
    /// The trading loop did not quiesce within the allotted budget. The system
    /// remains halted and no mode commit occurred (fail-closed).
    #[error("quiesce timed out after {waited_secs}s: {detail}")]
    QuiesceTimeout { waited_secs: u64, detail: String },
    /// Post-commit activation failed; the system remains halted (fail-closed).
    #[error("activation failed: {0}")]
    Activation(String),
    /// An underlying engine operation failed.
    #[error("control operation failed: {0}")]
    Engine(String),
}

/// Money-critical runtime control surface exposed to the web control plane.
///
/// All mutating methods drive the live engine (never a stale persisted copy) so
/// that halt, mode transitions, and blacklist changes take effect on the hot
/// path immediately and fail closed on error.
#[async_trait]
pub trait RuntimeControlPort: Send + Sync {
    /// Currently active execution mode (lock-free live read).
    fn execution_mode(&self) -> ExecutionMode;

    /// Governed runtime execution-mode hot-swap: preflight → quiesce → atomic
    /// commit → activate → resume. Fail-closed: never partially commits.
    async fn switch_execution_mode(
        &self,
        target: ExecutionMode,
        operator_ack: &str,
    ) -> Result<ModeTransitionReport, RuntimeControlError>;

    /// Halt trading: risk halt + execution kill switch.
    async fn halt(&self, reason: String);

    /// Resume trading after operator acknowledgement.
    async fn resume(&self, operator_ack: &str) -> Result<(), RuntimeControlError>;

    /// Force the circuit breaker back to `Closed`.
    async fn reset_circuit_breaker(&self, reason: &str) -> Result<(), RuntimeControlError>;

    /// Live risk-engine snapshot (breaker, exposure, daily loss / pnl).
    fn risk_snapshot(&self) -> RiskEngineState;

    /// Count of currently open positions (live in-memory view).
    fn open_position_count(&self) -> u32;

    /// Active blacklist entries (live in-memory view).
    fn blacklist(&self) -> Vec<BlacklistInfo>;

    /// Add a market to the runtime blacklist (persists + republishes snapshot).
    async fn add_blacklist(
        &self,
        market_id: MarketId,
        reason: BlacklistReason,
    ) -> Result<(), RuntimeControlError>;

    /// Remove a market from the runtime blacklist.
    async fn remove_blacklist(
        &self,
        market_id: &MarketId,
        reason: &str,
    ) -> Result<(), RuntimeControlError>;

    /// Assemble the aggregate system-status view.
    async fn system_status(&self) -> SystemStatus;

    /// Run all subsystem health checks.
    async fn health(&self) -> HealthReport;
}

/// Hot-reload surface for the versioned runtime configuration
/// (dependency-inverted).
///
/// Implemented by the core `RuntimeConfigApplicator`: web governance handlers
/// validate and persist a version through the audited registry, then call
/// [`Self::apply`] so the activation reaches every live subscriber (risk
/// engine, detection chain, execution chain, settlement, alerts) immediately.
#[async_trait]
pub trait RuntimeConfigPort: Send + Sync {
    /// Currently active runtime config (lock-free snapshot read).
    fn current(&self) -> Arc<RuntimeConfig>;

    /// Money-state activation preflight (fail-closed): rejects candidates
    /// whose exposure ceilings fall below capital that is already committed,
    /// and mode-sensitive invariants for the mode that is currently running.
    fn preflight(&self, candidate: &RuntimeConfig) -> Result<(), RuntimeControlError>;

    /// Swap the in-process store and propagate to every registered subscriber
    /// in the fixed order (risk before execution). Must only be called after a
    /// successful [`Self::preflight`] and a durable, audited activation.
    async fn apply(&self, config: RuntimeConfig) -> Result<(), RuntimeControlError>;
}

/// Live market-data surface exposed to the web layer (dependency-inverted).
///
/// Backs the markets dashboard's order-book read and the operator WS
/// subscription controls without leaking the core `BookStore` / WS manager
/// types into `oxide-arb-web`.
#[async_trait]
pub trait MarketDataPort: Send + Sync {
    /// Published YES / NO book snapshots for a market's tokens (lock-free read;
    /// `None` for a token with no published book yet).
    fn book(
        &self,
        yes_token: &TokenId,
        no_token: &TokenId,
    ) -> (Option<Arc<BookSnapshot>>, Option<Arc<BookSnapshot>>);

    /// Add tokens to the live CLOB WS subscription set (best-effort, immediate).
    async fn subscribe(&self, token_ids: Vec<TokenId>) -> Result<(), RuntimeControlError>;

    /// Remove tokens from the live CLOB WS subscription set.
    async fn unsubscribe(&self, token_ids: Vec<TokenId>) -> Result<(), RuntimeControlError>;
}

/// Operator-driven replay (backfill materialization run) request.
///
/// Builds a `Backfill`-triggered materialization run over an explicit time
/// window, reusing the production simulation/quality-gate defaults so a web
/// replay is consistent with the scheduler's runs.
#[derive(Debug, Clone)]
pub struct ReplayEnqueueRequest {
    /// Inclusive window start (PIT replay lower bound).
    pub from: DateTime<Utc>,
    /// Exclusive window end (PIT replay upper bound).
    pub to: DateTime<Utc>,
    /// Market / event / token / category scope for the replay.
    pub markets: MarketFilterSpec,
    /// Control-factor types to (re)materialize.
    pub requested_factor_types: Vec<ControlFactorType>,
    /// Optional account boundary for balance / token-balance evidence.
    pub replay_account_scope: Option<ReplayAccountScope>,
    /// Operator justification (recorded on the run + operation log).
    pub reason: String,
    /// Force a brand-new run even if an equivalent one exists (dedupe override).
    pub force_new_run: bool,
}

/// Result of enqueuing a replay run.
#[derive(Debug, Clone)]
pub struct ReplayEnqueueResult {
    /// The created (or deduplicated) run.
    pub run: ControlFactorMaterializationRunInfo,
    /// `true` when a new `Queued` run was created; `false` on dedupe.
    pub created: bool,
}

/// Replay enqueue surface exposed to the web layer (dependency-inverted).
///
/// The enqueue path only seals a manifest and writes a `Queued` row (the
/// execute worker later runs it), so this port never performs heavy resolver
/// work synchronously.
#[async_trait]
pub trait ReplayPort: Send + Sync {
    /// Seal a replay manifest and enqueue a `Queued` materialization run.
    async fn enqueue(
        &self,
        request: ReplayEnqueueRequest,
    ) -> Result<ReplayEnqueueResult, RuntimeControlError>;
}

/// Prometheus text exposition payload returned by [`MetricsScrapePort`].
#[derive(Debug, Clone)]
pub struct PrometheusTextPayload {
    /// MIME type (typically `text/plain; version=0.0.4; charset=utf-8`).
    pub content_type: String,
    /// Encoded metric families in Prometheus text format.
    pub body: Vec<u8>,
}

/// Prometheus scrape surface for the web `/metrics` handler (dependency-inverted).
///
/// Implemented by `oxide-arb-core` over [`MetricsHub`](../../oxide-arb-core) so
/// the web crate never depends on the core observability module directly.
pub trait MetricsScrapePort: Send + Sync {
    /// Gather all registered metrics in Prometheus text exposition format.
    fn scrape_prometheus(&self) -> Result<PrometheusTextPayload, String>;
}

/// One dependency probe result inside a [`ReadinessReport`].
#[derive(Debug, Clone, Serialize)]
pub struct DependencyCheck {
    /// Stable dependency name (`postgresql`, `redis`, …).
    pub name: &'static str,
    /// Whether the probe succeeded.
    pub ok: bool,
    /// Error detail when `ok` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Outcome of a readiness probe (orchestrator-facing).
#[derive(Debug, Clone, Serialize)]
pub struct ReadinessReport {
    /// True only when every required dependency probe succeeded.
    pub ready: bool,
    /// Per-dependency probe results (always populated for required deps).
    pub checks: Vec<DependencyCheck>,
}

/// Readiness probe surface for `GET /ready` (dependency-inverted).
///
/// Web handlers treat a failed probe as HTTP 503 so load balancers stop
/// routing traffic before auth/session infrastructure is usable.
#[async_trait]
pub trait ReadinessPort: Send + Sync {
    /// Run all required dependency probes.
    async fn check(&self) -> ReadinessReport;
}

/// Market-catalog warmup state.
///
/// The catalog is `Warming` from process start until the first successful
/// Gamma full sync; detection is gated off (fail-closed) while warming, but
/// the web control plane stays fully available.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CatalogState {
    /// First Gamma sync has not completed yet.
    Warming,
    /// Catalog synced — markets registered and detection unlocked.
    Ready {
        /// Markets registered by the most recent successful sync.
        markets: u64,
        /// Completion time of the most recent successful sync.
        synced_at: DateTime<Utc>,
    },
}

impl CatalogState {
    /// Whether the first catalog sync has completed.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }
}

/// Catalog readiness surface (dependency-inverted).
///
/// Implemented by `oxide-arb-core` over its `CatalogReadiness` watch state so
/// the web readiness report can include the catalog without depending on core.
pub trait CatalogStatusPort: Send + Sync {
    /// Current warmup state snapshot.
    fn catalog_state(&self) -> CatalogState;

    /// Cheap flag: `true` once the first sync completed (hot-path friendly).
    fn is_ready(&self) -> bool;
}
