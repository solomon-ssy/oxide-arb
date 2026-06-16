//! Integration-test harness.
//!
//! Wires the real Postgres-backed repositories and the Redis-backed JWT
//! blacklist into the production [`AppState`], and serves requests through an
//! actix `App` that mirrors `spawn_web_server` (request-id middleware + the
//! full route table). The app itself is stateless — all state lives in Postgres
//! and Redis — so [`call`] builds a fresh service per request, which keeps the
//! helper's return type nameable while still sharing the external backends.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use actix_web::{
    App,
    body::to_bytes,
    http::{StatusCode, header::HeaderMap},
    middleware::from_fn,
    test, web,
};
use async_trait::async_trait;
use chrono::Utc;
use oxide_arb_control::governance::ControlFactorRegistry;
use oxide_arb_core::{
    bridge::metrics_scrape::CoreMetricsScrape, observability::metrics_hub::MetricsHub,
};
use oxide_arb_error::auth::AuthError;
use oxide_arb_models::{
    config::{CacheConfig, DeployConfig, JwtConfig, RedisConfig},
    domain::{
        BlacklistInfo, CatalogState, CoreEventPublisher, HealthReport, MarketDataPort,
        ModeTransitionReport, ReplayEnqueueRequest, ReplayEnqueueResult, ReplayPort,
        RiskEngineState, RuntimeConfigPort, RuntimeControlError, RuntimeControlPort,
        SystemBalanceSource, SystemBalanceView, SystemStatus, market::book::BookSnapshot,
    },
    enums::{
        common::ExecutionMode,
        risk::{BlacklistReason, BlacklistScope, BreakerStateName},
    },
    runtime_config::{
        RuntimeConfig,
        validation::{RuntimePreflightContext, preflight_runtime_config},
    },
    types::{MarketId, TokenId, Usd},
};
use oxide_arb_repository::{
    postgres::{
        PgControlFactorRepository, PgFactDataRepository, PgMarketRepository, PgMenuRepository,
        PgOperationLogRepository, PgPositionRepository, PgReportRepository, PgRiskAuditRepository,
        PgRoleMenuRepository, PgRolePermissionRepository, PgRoleRepository,
        PgRuntimeConfigVersionRepository, PgTradeRepository, PgUserRepository,
        PgUserRoleRepository,
    },
    traits::{
        ControlFactorRepository, ControlFactorShadowDecisionRepository, OperationLogRepository,
        RuntimeConfigVersionRepository,
    },
};
use oxide_arb_storage::cache::connect_pool;
use oxide_arb_test_support::mocks::MockTimeseriesRepository;
use oxide_arb_web::{
    AppState,
    audit::{OperationLogBuffer, spawn_operation_log_writer},
    auth::casbin::CasbinService,
    jwt::{JwtService, RedisTokenBlacklist, TokenBlacklist},
    middleware,
    readiness::PgRedisReadiness,
    routes,
    ws::{SessionRegistry, spawn_ws_broadcaster},
};
use rust_decimal::Decimal;
use sea_orm::DatabaseConnection;
use serde_json::Value;
use std::sync::Mutex;
use testcontainers::ContainerAsync;
use testcontainers_modules::{postgres::Postgres, redis::Redis};
use tokio_util::sync::CancellationToken;

use crate::{pg, redis};

/// The version header every `/api/auth/*` request must carry to match the `v1`
/// scope (see `ApiV1Guard`).
pub const API_VERSION: (&str, &str) = ("Accept-Api-Version", "v1");

const TEST_JWT_SECRET: &str = "oxide-arb-integration-test-secret-not-for-production";
const TEST_ISSUER: &str = "oxide-arb-test";

/// A live test environment: migrated Postgres + running Redis behind the
/// production `AppState`.
pub struct TestEnv {
    /// The state injected into the actix app.
    pub state: AppState,
    /// Typed handle to the runtime-config apply double (preflight context).
    pub runtime_config_apply: Arc<MockRuntimeConfigApply>,
    /// Typed handle to the in-memory evidence repository (seed detections / audits).
    pub evidence: Arc<MockTimeseriesRepository>,
    /// Shared Postgres connection (pool kept alive by `_pg`).
    pub db: DatabaseConnection,
    // Postgres container guard. `DatabaseConnection` clones inside the
    // repositories keep the connection pool alive, so the pool wrapper itself
    // need not be retained — only the container must outlive the test.
    _pg: ContainerAsync<Postgres>,
    // Redis container guard; [`take_redis`] simulates an outage in auth/ready tests.
    redis: Option<ContainerAsync<Redis>>,
    // Cancels the background operation-log writer when the test env is dropped.
    writer_shutdown: CancellationToken,
    // Cancels the WebSocket broadcaster task.
    ws_shutdown: CancellationToken,
}

/// Connect the shared Redis pool, wrap the JWT revocation store over it, and
/// verify it is reachable (mirrors the production composition root).
async fn connect_blacklist(redis_cfg: &RedisConfig) -> Arc<RedisTokenBlacklist> {
    let pool = connect_pool(redis_cfg).await.expect("redis pool");
    let jwt_blacklist = Arc::new(RedisTokenBlacklist::new(pool, &redis_cfg.key_prefix));
    jwt_blacklist
        .health_check()
        .await
        .expect("redis blacklist health");
    jwt_blacklist
}

impl TestEnv {
    /// Bring up Postgres + Redis and assemble the production `AppState`.
    pub async fn start() -> Self {
        let (pool, pg_container) = pg::setup_pg().await;
        let (redis_port, redis_container) = redis::setup_redis().await;
        let redis_cfg = redis::test_redis_config(redis_port);

        let jwt_blacklist = connect_blacklist(&redis_cfg).await;
        let blacklist = Arc::clone(&jwt_blacklist) as Arc<dyn TokenBlacklist>;
        let jwt = Arc::new(JwtService::new(&jwt_config(), Arc::clone(&blacklist)));

        let db = pool.connection().clone();
        let casbin = Arc::new(
            CasbinService::new(db.clone())
                .await
                .expect("casbin service"),
        );
        let perm_checker = Arc::new(routes::init_rbac_rules());

        // Governance control-plane: one registry over the shared repositories,
        // plus read handles exposed directly on the state.
        let control_factors: Arc<dyn ControlFactorRepository> =
            Arc::new(PgControlFactorRepository::new(db.clone()));
        let runtime_config: Arc<dyn RuntimeConfigVersionRepository> =
            Arc::new(PgRuntimeConfigVersionRepository::new(db.clone()));
        let shadow_decisions: Arc<dyn ControlFactorShadowDecisionRepository> =
            Arc::new(PgFactDataRepository::new(db.clone()));
        let operation_logs: Arc<dyn OperationLogRepository> =
            Arc::new(PgOperationLogRepository::new(db.clone()));
        let registry = Arc::new(ControlFactorRegistry::new(
            Arc::clone(&control_factors),
            Arc::clone(&runtime_config),
        ));

        // Operation-log pipeline: buffer in the state, writer drains to Postgres.
        // A short flush interval keeps audit-trail assertions fast.
        let (operation_log, operation_log_rx) = OperationLogBuffer::new(1024);
        let writer_shutdown = CancellationToken::new();
        tokio::spawn(spawn_operation_log_writer(
            operation_log_rx,
            Arc::clone(&operation_logs),
            64,
            Duration::from_millis(50),
            writer_shutdown.clone(),
        ));

        let (events, event_rx) = CoreEventPublisher::bounded(256);
        let ws_sessions = SessionRegistry::default();
        let ws_shutdown = CancellationToken::new();
        tokio::spawn(spawn_ws_broadcaster(
            event_rx,
            ws_sessions.clone(),
            ws_shutdown.clone(),
        ));

        let metrics = Arc::new(CoreMetricsScrape::new(Arc::new(MetricsHub::new())));
        let readiness = Arc::new(PgRedisReadiness::new(
            db.clone(),
            Arc::clone(&blacklist),
            None,
        ));

        let runtime_config_apply = Arc::new(MockRuntimeConfigApply::default());
        let evidence = Arc::new(MockTimeseriesRepository::default());
        let state = AppState {
            deploy: Arc::new(test_deploy_config()),
            runtime_config_apply: Arc::clone(&runtime_config_apply) as _,
            jwt,
            jwt_blacklist,
            users: Arc::new(PgUserRepository::new(db.clone())),
            roles: Arc::new(PgRoleRepository::new(db.clone())),
            menus: Arc::new(PgMenuRepository::new(db.clone())),
            user_roles: Arc::new(PgUserRoleRepository::new(db.clone())),
            role_menus: Arc::new(PgRoleMenuRepository::new(db.clone())),
            role_permissions: Arc::new(PgRolePermissionRepository::new(db.clone())),
            positions: Arc::new(PgPositionRepository::new(db.clone())),
            trades: Arc::new(PgTradeRepository::new(db.clone())),
            markets: Arc::new(PgMarketRepository::new(db.clone())),
            reports: Arc::new(PgReportRepository::new(db.clone())),
            evidence: Arc::clone(&evidence) as _,
            risk_audit: Arc::new(PgRiskAuditRepository::new(db.clone())),
            casbin,
            perm_checker,
            registry,
            control_factors,
            runtime_config,
            shadow_decisions,
            operation_logs,
            operation_log,
            control: Arc::new(MockRuntimeControl::default()),
            market_data: Arc::new(MockMarketData),
            replay: Arc::new(MockReplay),
            events,
            ws_sessions,
            metrics,
            readiness,
        };

        Self {
            state,
            runtime_config_apply,
            evidence,
            db,
            _pg: pg_container,
            redis: Some(redis_container),
            writer_shutdown,
            ws_shutdown,
        }
    }

    /// Take the Redis container guard (auth / readiness outage tests only).
    pub const fn take_redis(&mut self) -> Option<ContainerAsync<Redis>> {
        self.redis.take()
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        // Stop background tasks so they do not outlive the test's Postgres/Redis.
        self.writer_shutdown.cancel();
        self.ws_shutdown.cancel();
    }
}

fn jwt_config() -> JwtConfig {
    JwtConfig {
        secret: TEST_JWT_SECRET.to_owned(),
        issuer: TEST_ISSUER.to_owned(),
        access_ttl_secs: 900,
        refresh_ttl_secs: 604_800,
    }
}

/// Deploy config exposed through the read-only `/api/system/deploy-config`
/// endpoint. Secrets are deliberately non-empty (the shipped defaults are
/// scrubbed to empty strings) so the masking tests verify that configured
/// credentials are actually redacted, never echoed.
fn test_deploy_config() -> DeployConfig {
    DeployConfig {
        cache: CacheConfig {
            redis: RedisConfig {
                password: "harness-redis-secret".to_owned(),
                ..RedisConfig::default()
            },
            ..CacheConfig::default()
        },
        ..DeployConfig::default()
    }
}

/// A captured HTTP response (status + headers + raw body), normalized so that
/// errors surfaced by middleware are rendered exactly as the production app
/// renders them — via `ResponseError` — rather than panicking the test.
pub struct Resp {
    /// HTTP status code.
    pub status: StatusCode,
    headers: HeaderMap,
    body: web::Bytes,
}

impl Resp {
    /// A response header value as a string slice, if present and valid UTF-8.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }

    /// Parse the body as JSON (panics if the body is not valid JSON).
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("response body must be valid JSON")
    }

    /// Raw response body bytes.
    pub fn body_bytes(&self) -> &[u8] {
        &self.body
    }
}

/// Build a fresh service over `state` and execute a single request.
///
/// Handlers convert their `Err` into a `ServiceResponse` internally, but a
/// middleware (`authn`) returning `Err` bubbles up as a raw service error. The
/// real server renders that through `ResponseError`; we do the same here so the
/// unified error envelope is asserted on the exact bytes a client would see.
pub async fn call(state: &AppState, req: test::TestRequest) -> Resp {
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state.clone()))
            .wrap(from_fn(middleware::request_id))
            // Outermost (registered last), mirroring `spawn_web_server`.
            .wrap(from_fn(middleware::operation_audit))
            .configure(routes::configure),
    )
    .await;

    match test::try_call_service(&app, req.to_request()).await {
        Ok(res) => {
            let status = res.status();
            let headers = res.headers().clone();
            let body = test::read_body(res).await;
            Resp {
                status,
                headers,
                body,
            }
        }
        Err(err) => {
            let response = err.error_response();
            let status = response.status();
            let headers = response.headers().clone();
            let body = to_bytes(response.into_body())
                .await
                .expect("read error response body");
            Resp {
                status,
                headers,
                body,
            }
        }
    }
}

/// A zeroed [`RiskEngineState`] for test responses.
fn zero_risk_state(is_halted: bool) -> RiskEngineState {
    let now = Utc::now();
    let today = now.date_naive();
    RiskEngineState {
        breaker_state: BreakerStateName::Closed,
        breaker_level: None,
        is_halted,
        halt_reason: None,
        cooldown_until: None,
        total_exposure: Usd::ZERO,
        hourly_loss_usd: Usd::ZERO,
        hourly_fee_usd: Usd::ZERO,
        hourly_trade_count: 0,
        hourly_success_count: 0,
        hourly_miss_count: 0,
        hourly_window_start: now,
        daily_pnl: Usd::ZERO,
        daily_loss_usd: Usd::ZERO,
        daily_fee_usd: Usd::ZERO,
        daily_budget_spent: Usd::ZERO,
        daily_trade_count: 0,
        daily_success_count: 0,
        daily_miss_count: 0,
        daily_window_start: today,
        weekly_loss_usd: Usd::ZERO,
        weekly_trade_count: 0,
        weekly_window_start: today,
        consecutive_misses: 0,
        cooldown_multiplier: 1,
        hwm_equity: Usd::ZERO,
        total_realized_pnl: Usd::ZERO,
        last_emergency_at: None,
        last_emergency_reason: None,
        snapshot_at: now,
    }
}

/// In-memory [`RuntimeConfigPort`] double for web tests: applies activations
/// to a process-local snapshot and runs the **real** money-state preflight
/// against a configurable context (defaults to zero reservations, so it
/// accepts everything unless a test commits capital via [`Self::set_reserved`]).
pub struct MockRuntimeConfigApply {
    current: Mutex<Arc<RuntimeConfig>>,
    preflight_ctx: Mutex<RuntimePreflightContext>,
    fail_next_apply: AtomicBool,
}

impl Default for MockRuntimeConfigApply {
    fn default() -> Self {
        Self {
            current: Mutex::new(Arc::new(RuntimeConfig::default())),
            preflight_ctx: Mutex::new(RuntimePreflightContext {
                mode: ExecutionMode::Paper,
                reserved_total_usd: Decimal::ZERO,
                max_market_reserved_usd: Decimal::ZERO,
            }),
            fail_next_apply: AtomicBool::new(false),
        }
    }
}

impl MockRuntimeConfigApply {
    /// Simulate in-flight capital so the activation preflight has live money
    /// state to reject against.
    pub fn set_reserved(&self, total_usd: Decimal, max_market_usd: Decimal) {
        let mut ctx = self.preflight_ctx.lock().unwrap();
        ctx.reserved_total_usd = total_usd;
        ctx.max_market_reserved_usd = max_market_usd;
    }

    /// Make the next [`RuntimeConfigPort::apply`] fail after preflight passed,
    /// simulating a live propagation failure *after* the durable activation —
    /// the split-brain scenario the web layer must compensate.
    pub fn fail_next_apply(&self) {
        self.fail_next_apply.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl RuntimeConfigPort for MockRuntimeConfigApply {
    fn current(&self) -> Arc<RuntimeConfig> {
        Arc::clone(&self.current.lock().unwrap())
    }

    fn preflight(&self, candidate: &RuntimeConfig) -> Result<(), RuntimeControlError> {
        let ctx = *self.preflight_ctx.lock().unwrap();
        let report = preflight_runtime_config(candidate, &ctx);
        if report.has_errors() {
            return Err(RuntimeControlError::Precondition(report.to_string()));
        }
        Ok(())
    }

    async fn apply(&self, config: RuntimeConfig) -> Result<(), RuntimeControlError> {
        if self.fail_next_apply.swap(false, Ordering::SeqCst) {
            return Err(RuntimeControlError::Activation(
                "injected apply failure".into(),
            ));
        }
        self.preflight(&config)?;
        *self.current.lock().unwrap() = Arc::new(config);
        Ok(())
    }
}

/// In-memory [`RuntimeControlPort`] double for web tests: records halt/mode/
/// blacklist mutations and returns canned status without touching a live engine.
#[derive(Default)]
pub struct MockRuntimeControl {
    mode: Mutex<Option<ExecutionMode>>,
    halted: Mutex<bool>,
    blacklist: Mutex<Vec<BlacklistInfo>>,
}

#[async_trait]
impl RuntimeControlPort for MockRuntimeControl {
    fn execution_mode(&self) -> ExecutionMode {
        self.mode.lock().unwrap().unwrap_or(ExecutionMode::DryRun)
    }

    async fn switch_execution_mode(
        &self,
        target: ExecutionMode,
        _operator_ack: &str,
    ) -> Result<ModeTransitionReport, RuntimeControlError> {
        let from = self.execution_mode();
        *self.mode.lock().unwrap() = Some(target);
        Ok(ModeTransitionReport { from, to: target })
    }

    async fn halt(&self, _reason: String) {
        *self.halted.lock().unwrap() = true;
    }

    async fn resume(&self, _operator_ack: &str) -> Result<(), RuntimeControlError> {
        *self.halted.lock().unwrap() = false;
        Ok(())
    }

    async fn reset_circuit_breaker(&self, _reason: &str) -> Result<(), RuntimeControlError> {
        Ok(())
    }

    fn risk_snapshot(&self) -> RiskEngineState {
        zero_risk_state(*self.halted.lock().unwrap())
    }

    fn open_position_count(&self) -> u32 {
        0
    }

    fn blacklist(&self) -> Vec<BlacklistInfo> {
        self.blacklist.lock().unwrap().clone()
    }

    async fn add_blacklist(
        &self,
        market_id: MarketId,
        blacklist_reason: BlacklistReason,
        _operator_reason: &str,
    ) -> Result<(), RuntimeControlError> {
        self.blacklist.lock().unwrap().push(BlacklistInfo {
            market_id,
            token_id: None,
            scope: BlacklistScope::Full,
            reason: blacklist_reason,
            expires_at: None,
            miss_count: 0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        Ok(())
    }

    async fn remove_blacklist(
        &self,
        market_id: &MarketId,
        _reason: &str,
    ) -> Result<(), RuntimeControlError> {
        self.blacklist
            .lock()
            .unwrap()
            .retain(|entry| &entry.market_id != market_id);
        Ok(())
    }

    async fn system_status(&self) -> SystemStatus {
        SystemStatus {
            execution_mode: self.execution_mode(),
            breaker_state: BreakerStateName::Closed,
            uptime_secs: 0,
            active_markets: 0,
            open_positions: 0,
            pending_reservations: 0,
            total_exposure: Usd::ZERO,
            daily_pnl: Usd::ZERO,
            catalog: CatalogState::Ready {
                markets: 1,
                synced_at: Utc::now(),
            },
            checked_at: Utc::now(),
        }
    }

    async fn system_balance(&self) -> SystemBalanceView {
        SystemBalanceView {
            execution_mode: self.execution_mode(),
            source: SystemBalanceSource::SimulatedDryRun,
            cash_balance_usd: Usd::new(rust_decimal_macros::dec!(1000)),
            position_mark_value_usd: Usd::ZERO,
            equity_usd: Usd::new(rust_decimal_macros::dec!(1000)),
            bankroll_cap_usd: Usd::new(rust_decimal_macros::dec!(1000)),
            reserve_balance_usd: Usd::new(rust_decimal_macros::dec!(100)),
            reserved_usd: Usd::ZERO,
            total_exposure_usd: Usd::ZERO,
            available_before_potential_loss_usd: Usd::new(rust_decimal_macros::dec!(900)),
            open_position_count: 0,
            active_reservation_count: 0,
            metrics_age_secs: 0,
            is_authoritative: true,
            is_stale: false,
            checked_at: Utc::now(),
        }
    }

    async fn health(&self) -> HealthReport {
        HealthReport {
            overall_healthy: true,
            checks: Vec::new(),
            checked_at: Utc::now(),
        }
    }
}

/// No-op market-data port for the harness (no live book / WS in tests).
#[derive(Default)]
pub struct MockMarketData;

#[async_trait]
impl MarketDataPort for MockMarketData {
    fn book(
        &self,
        _yes_token: &TokenId,
        _no_token: &TokenId,
    ) -> (Option<Arc<BookSnapshot>>, Option<Arc<BookSnapshot>>) {
        (None, None)
    }

    fn subscribed_tokens(&self, _token_ids: &[TokenId]) -> HashSet<TokenId> {
        HashSet::new()
    }

    async fn subscribe(&self, _token_ids: Vec<TokenId>) -> Result<(), RuntimeControlError> {
        Ok(())
    }

    async fn unsubscribe(&self, _token_ids: Vec<TokenId>) -> Result<(), RuntimeControlError> {
        Ok(())
    }
}

/// No-op replay port for the harness (enqueue is exercised in core, not here).
#[derive(Default)]
pub struct MockReplay;

#[async_trait]
impl ReplayPort for MockReplay {
    async fn enqueue(
        &self,
        _request: ReplayEnqueueRequest,
    ) -> Result<ReplayEnqueueResult, RuntimeControlError> {
        Err(RuntimeControlError::Engine(
            "replay enqueue not supported in the test harness".to_owned(),
        ))
    }
}

/// No-op blacklist used solely to mint test tokens; never consulted during the
/// authn decode path (decode rejects expired/wrong-type tokens before any
/// revocation check runs).
pub struct NoopBlacklist;

#[async_trait]
impl TokenBlacklist for NoopBlacklist {
    async fn revoke(&self, _jti: &str, _ttl: Duration) -> Result<(), AuthError> {
        Ok(())
    }

    async fn is_revoked(&self, _jti: &str) -> Result<bool, AuthError> {
        Ok(false)
    }

    async fn health_check(&self) -> Result<(), AuthError> {
        Ok(())
    }
}
