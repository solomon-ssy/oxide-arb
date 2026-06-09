//! Shared application state injected into every request.
//!
//! `AppState` is cheap to clone (every field is an [`Arc`] or a cloneable
//! handle) and is registered once as actix `web::Data`. It bundles the
//! authentication service, the RBAC repositories, the live Casbin enforcer, the
//! route-level permission registry, the governance control-plane (registry +
//! read repositories), and the operation-log buffer. Later sub-phases extend it
//! further (business repositories, WebSocket broadcaster).

use std::sync::Arc;

use oxide_arb_control::governance::ControlFactorRegistry;
use oxide_arb_models::domain::{
    CoreEventPublisher, MarketDataPort, MetricsScrapePort, ReadinessPort, ReplayPort,
    RuntimeControlPort,
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
    jwt::JwtService,
    ws::SessionRegistry,
};

/// Dependency bundle shared by all handlers and middleware.
#[derive(Clone)]
pub struct AppState {
    /// JWT signer/validator with its revocation blacklist.
    pub jwt: Arc<JwtService>,
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
