//! Shared application state injected into every request.
//!
//! `AppState` is cheap to clone (every field is an [`Arc`] or a cloneable
//! handle) and is registered once as actix `web::Data`. It bundles the
//! authentication service, the RBAC repositories, the live Casbin enforcer, the
//! route-level permission registry, the governance control-plane (registry +
//! read repositories), and the operation-log buffer. Later sub-phases extend it
//! further (business repositories, WebSocket broadcaster).

use std::sync::Arc;

use chrono::Utc;
use oxide_arb_control::{governance::ControlFactorRegistry, scheduler::SchedulePolicy};
use oxide_arb_models::{
    config::DeployConfig,
    domain::{
        CoreEventPublisher, MarketDataPort, MaterializationScheduleStatusView, MetricsScrapePort,
        ReadinessPort, ReplayPort, RuntimeConfigPort, RuntimeConfigRef, RuntimeControlPort,
    },
    enums::control_factor::MaterializationRunStatus,
};
use oxide_arb_repository::traits::{
    ControlFactorRepository, ControlFactorShadowDecisionRepository, EvidenceTimeseriesRepository,
    MarketRepository, MenuRepository, OperationLogRepository, PositionRepository, ReportRepository,
    RiskAuditRepository, RoleMenuRepository, RolePermissionRepository, RoleRepository,
    RuntimeConfigVersionRepository, TradeRepository, UserRepository, UserRoleRepository,
};

use crate::{
    audit::OperationLogBuffer,
    auth::casbin::{CasbinService, PermChecker},
    error::WebError,
    jwt::{JwtService, RedisTokenBlacklist},
    ws::SessionRegistry,
};

/// Dependency bundle shared by all handlers and middleware.
#[derive(Clone)]
pub struct AppState {
    /// Deploy configuration (read-only; surfaced masked via the system API).
    pub deploy: Arc<DeployConfig>,
    /// Hot-reload surface for the versioned runtime config: preflight before
    /// the durable activation, apply after it (dependency-inverted to core).
    pub runtime_config_apply: Arc<dyn RuntimeConfigPort>,
    /// JWT signer/validator with its revocation blacklist.
    pub jwt: Arc<JwtService>,
    /// JWT revocation pool handle (for graceful `close` on shutdown).
    pub jwt_blacklist: Arc<RedisTokenBlacklist>,
    /// User account access (login, profile, CRUD).
    pub users: Arc<dyn UserRepository>,
    /// Role catalog access (CRUD, status transitions).
    pub roles: Arc<dyn RoleRepository>,
    /// Menu access (tree, accessibility, CRUD).
    pub menus: Arc<dyn MenuRepository>,
    /// User→role assignment (replace-set, per-request role resolution).
    pub user_roles: Arc<dyn UserRoleRepository>,
    /// Role→menu assignment.
    pub role_menus: Arc<dyn RoleMenuRepository>,
    /// Role→permission assignment (Casbin `p` projection).
    pub role_permissions: Arc<dyn RolePermissionRepository>,
    /// Live Casbin enforcer (read + reload).
    pub casbin: Arc<CasbinService>,
    /// Route-level authorization registry (fail-closed).
    pub perm_checker: Arc<PermChecker>,
    /// Governance control-plane: state-machine mutations that write the audit
    /// hash chain transactionally (publish / rollback / reject / runtime-config).
    pub registry: Arc<ControlFactorRegistry>,
    /// Read access to control-factor state, publications, and the audit chain.
    pub control_factors: Arc<dyn ControlFactorRepository>,
    /// Read access to immutable runtime-config versions and activations.
    pub runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    /// Read access to shadow-publication decision evidence.
    pub shadow_decisions: Arc<dyn ControlFactorShadowDecisionRepository>,
    /// Append-only operation log (paginated forensic queries).
    pub operation_logs: Arc<dyn OperationLogRepository>,
    /// Non-blocking producer handle for the operation-log writer pipeline.
    pub operation_log: OperationLogBuffer,
    /// Money-critical runtime control surface (mode/halt/resume/CB/blacklist),
    /// dependency-inverted so the web layer never depends on `oxide-arb-core`.
    pub control: Arc<dyn RuntimeControlPort>,
    /// Live market-data surface (published book reads + WS subscription control).
    pub market_data: Arc<dyn MarketDataPort>,
    /// Replay enqueue surface (`POST /replay` → queued materialization run).
    pub replay: Arc<dyn ReplayPort>,
    /// Non-blocking producer for the real-time event bus (WS broadcaster +
    /// governance/control-plane emissions).
    pub events: CoreEventPublisher,
    /// Open-position reads for the risk dashboard.
    pub positions: Arc<dyn PositionRepository>,
    /// Trade history reads (paginated list / detail) for the trades dashboard.
    pub trades: Arc<dyn TradeRepository>,
    /// Market metadata reads (paginated list / detail) for the markets dashboard.
    pub markets: Arc<dyn MarketRepository>,
    /// Settled `PnL` report reads (daily / weekly) for the `PnL` + analytics views.
    pub reports: Arc<dyn ReportRepository>,
    /// `ClickHouse` evidence timeseries reads (opportunity detections / audits).
    pub evidence: Arc<dyn EvidenceTimeseriesRepository>,
    /// Risk decision audit reads (trade decisions, risk events) over a window.
    pub risk_audit: Arc<dyn RiskAuditRepository>,
    /// Live WebSocket session registry (shared with the broadcaster task).
    pub ws_sessions: SessionRegistry,
    /// Prometheus scrape surface (`GET /metrics`).
    pub metrics: Arc<dyn MetricsScrapePort>,
    /// Readiness probe surface (`GET /ready`).
    pub readiness: Arc<dyn ReadinessPort>,
}

const MATERIALIZATION_SUCCESS_STATUSES: &[MaterializationRunStatus] = &[
    MaterializationRunStatus::Completed,
    MaterializationRunStatus::CompletedWithRejectedFactors,
    MaterializationRunStatus::ReportOnly,
];

impl AppState {
    /// Mode-aware materialization schedule status projection for REST and WS sync.
    pub async fn materialization_schedule_statuses(
        &self,
    ) -> Result<Vec<MaterializationScheduleStatusView>, WebError> {
        let now = Utc::now();
        let policy = SchedulePolicy::for_mode(
            self.control.execution_mode(),
            RuntimeConfigRef::ActiveAt { at: now },
            "scheduler",
            option_env!("GIT_SHA").unwrap_or("unknown"),
        );
        let mut views = Vec::with_capacity(policy.tasks.len());
        for task in policy.tasks {
            let latest_any = self
                .control_factors
                .latest_run_for_schedule(&task.schedule_id, &[])
                .await?;
            let latest_success = self
                .control_factors
                .latest_run_for_schedule(&task.schedule_id, MATERIALIZATION_SUCCESS_STATUSES)
                .await?;
            let last_run_at = latest_any.as_ref().map(|run| run.created_at);
            let last_success_at = latest_success
                .as_ref()
                .map(|run| run.finished_at.unwrap_or(run.created_at));
            let last_terminal_status = latest_any.as_ref().map(|run| run.status).filter(|status| {
                !matches!(
                    *status,
                    MaterializationRunStatus::Queued | MaterializationRunStatus::Running
                )
            });
            let next_due_at = if task.activation.is_runnable() {
                Some(last_run_at.map_or(now, |last| last + task.cadence))
            } else {
                None
            };
            views.push(MaterializationScheduleStatusView {
                schedule_id: task.schedule_id,
                activation: task.activation.into(),
                mode_contract: task.mode_contract.into(),
                last_run_at,
                last_success_at,
                last_terminal_status,
                next_due_at,
            });
        }
        Ok(views)
    }
}
