//! Integration-test harness (Phase 0).

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use actix_test::TestRequest;
use actix_web::{
    App,
    body::to_bytes,
    http::{StatusCode, header::HeaderMap},
    middleware::from_fn,
    test, web,
};
use async_trait::async_trait;
use quant_pivot_core::{
    app::execution_read::CoreExecutionReadPort, execution::IntentLifecyclePublisher,
    report::AdHocReportRequest,
};
use quant_pivot_error::{
    QuantError, QuantResult, auth::AuthError, control::ControlError, execution::ExecutionError,
    storage::StorageError,
};
use quant_pivot_models::{
    clickhouse::{
        BookMicrostructureRow, BookSnapshotRow, MarketResolutionRow, MidPriceBucketRow,
        TickEventRow,
    },
    config::{CacheConfig, DeployConfig, JwtConfig, RedisConfig},
    domain::{
        BacktestPort, BacktestReportInfo, BacktestReportListQuery, BacktestReportView,
        BookSnapshot, BuildTrainingDatasetRequest, CatalogState, CatalogStatusPort,
        ComparisonReportListQuery, CoreEventPublisher, DataQualityPort, DataQualitySnapshot,
        ExecutionOrderInfo, ExecutionReadPort, ExecutionRecoveryPort, ExecutionRecoveryView,
        ExecutionSubmitPort, FactorCollinearityView, FactorDefinitionInfo,
        FactorDefinitionListQuery, FactorGovernancePort, GatePreviewIntent, GovernanceActor,
        HealthReport, KillSwitchPort, KillSwitchView, MarketDataPort, MetricsScrapePort,
        ModelComparisonReportInfo, ModelGovernancePort, ModelSpecInfo, ModelSpecListQuery,
        ModelTrainingPort, ModelVersionInfo, ModelVersionListQuery, Paginated,
        PromoteDatasetRequest, PublishFactorCommand, PublishModelCommand, QualityGateReportView,
        QuantModeTransitionReport, ReconciliationPort, ResearchCatalogPort,
        ResolveReconciliationCommand, ResolveReconciliationOutcome, RetireFactorCommand,
        RetireModelCommand, RollbackModelCommand, RunBacktestRequest, RuntimeConfigPort,
        RuntimeControlPort, SetKillSwitchCommand, SystemStatus, TrainModelRequest,
        TrainedModelView, TrainingDatasetInfo, TrainingDatasetListQuery, TrainingDatasetPlanView,
        TrainingDatasetPort, TrainingDatasetView, empty_catalog_page,
    },
    enums::{execution::KillSwitchState, quant::QuantRuntimeMode},
    runtime_config::RuntimeConfig,
    types::{
        BacktestReportId, FactorDefinitionId, MarketId, ModelComparisonReportId, ModelVersionId,
        OrderIntentId, TokenId, TrainingDatasetId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgAttributionRepository, PgExecutionOrderRepository, PgPositionRepository,
        PgReconciliationRepository, PgSettlementRedeemRepository,
    },
    traits::{
        AttributionRepository, ExecutionOrderRepository, PositionRepository,
        QuantFactReadRepository, ReconciliationRepository, SettlementRedeemRepository,
    },
};
use quant_pivot_storage::cache::connect_pool;
use quant_pivot_test_support::{
    account::core_account_read_port, report_pipeline_harness::FixtureReportSeedContext,
};
use quant_pivot_web::{
    AppState,
    audit::OperationLogBuffer,
    auth::casbin::CasbinService,
    jwt::{JwtService, RedisTokenBlacklist, TokenBlacklist},
    middleware,
    readiness::PgRedisReadiness,
    routes,
    ws::{SessionRegistry, spawn_ws_broadcaster},
};
use sea_orm::DatabaseConnection;
use serde_json::Value;
use std::{collections::HashSet, sync::Mutex};
use testcontainers::ContainerAsync;
use testcontainers_modules::{postgres::Postgres, redis::Redis};
use tokio_util::sync::CancellationToken;

use quant_pivot_models::domain::OrderIntentPort;

use crate::{
    core_report_port::CoreReportTestHandle, order_intent_port::build_order_intent_service, pg,
    redis, repos::WebHarnessRepos,
};

pub const API_VERSION: (&str, &str) = ("Accept-Api-Version", "v1");

const TEST_JWT_SECRET: &str = "quant-pivot-integration-test-secret-not-for-production";
const TEST_ISSUER: &str = "quant-pivot-test";

pub struct TestEnv {
    pub state: AppState,
    /// Postgres connection (migrations applied; RBAC seeded).
    pub db: DatabaseConnection,
    core_report: CoreReportTestHandle,
    /// Ad-hoc enqueue capture from [`FakeReportScheduleRunner`].
    pub ad_hoc_enqueued: Arc<Mutex<Vec<AdHocReportRequest>>>,
    pg_container: ContainerAsync<Postgres>,
    redis: Option<ContainerAsync<Redis>>,
    ws_shutdown: CancellationToken,
}

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
    pub async fn start() -> Self {
        Self::start_with_core_report_port().await
    }

    /// Integration harness with real [`CoreQuantReportPort`] backed by Postgres.
    pub async fn start_with_core_report_port() -> Self {
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
        let repos = WebHarnessRepos::from_connection(&db);

        let operation_log = OperationLogBuffer::direct(Arc::clone(&repos.operation_logs));

        let (events, event_rx) = CoreEventPublisher::bounded(256);
        let intent_lifecycle = Arc::new(IntentLifecyclePublisher::new(events.clone()));
        let ws_sessions = SessionRegistry::default();
        let ws_shutdown = CancellationToken::new();
        tokio::spawn(spawn_ws_broadcaster(
            event_rx,
            ws_sessions.clone(),
            ws_shutdown.clone(),
        ));

        let runtime_config_apply = Arc::new(MockRuntimeConfigApply::default());
        let catalog = Arc::new(MockCatalogStatus);
        let data_quality = Arc::new(MockDataQuality);
        let core_report =
            crate::core_report_port::build_core_report_stack(&db, events.clone()).await;
        let order_intents = build_order_intent_service(&db, Arc::clone(&intent_lifecycle));
        let state = AppState {
            deploy: Arc::new(test_deploy_config()),
            runtime_config_apply: Arc::clone(&runtime_config_apply) as Arc<dyn RuntimeConfigPort>,
            jwt,
            jwt_blacklist,
            users: Arc::clone(&repos.users),
            roles: Arc::clone(&repos.roles),
            menus: Arc::clone(&repos.menus),
            user_roles: Arc::clone(&repos.user_roles),
            role_menus: Arc::clone(&repos.role_menus),
            role_permissions: Arc::clone(&repos.role_permissions),
            casbin,
            perm_checker,
            runtime_config: Arc::clone(&repos.runtime_config),
            operation_logs: Arc::clone(&repos.operation_logs),
            operation_log,
            control: Arc::new(MockRuntimeControl::default()),
            kill_switch: Arc::new(MockKillSwitch::default()),
            market_data: Arc::new(MockMarketData),
            catalog: Arc::clone(&catalog) as Arc<dyn CatalogStatusPort>,
            data_quality: Arc::clone(&data_quality) as Arc<dyn DataQualityPort>,
            events,
            markets: Arc::clone(&repos.markets),
            quant_facts: Arc::new(MockQuantFactRead),
            ws_sessions,
            metrics: Arc::new(MockMetricsScrape),
            readiness: Arc::new(PgRedisReadiness::new(
                db.clone(),
                blacklist,
                Some(Arc::clone(&catalog) as Arc<dyn CatalogStatusPort>),
            )),
            training_datasets: Arc::new(MockTrainingDatasetPort),
            model_training: Arc::new(MockModelTrainingPort),
            backtests: Arc::new(MockBacktestPort),
            model_governance: Arc::new(MockModelGovernancePort),
            factor_governance: Arc::new(MockFactorGovernancePort),
            research_catalog: Arc::new(MockResearchCatalogPort),
            quant_reports: core_report.port.clone(),
            account_read: core_account_read_port(
                &db,
                Arc::clone(&runtime_config_apply) as Arc<dyn RuntimeConfigPort>,
            ),
            order_intents: order_intents as Arc<dyn OrderIntentPort>,
            execution_read: Arc::new(CoreExecutionReadPort::new(
                Arc::new(PgExecutionOrderRepository::new(db.clone()))
                    as Arc<dyn ExecutionOrderRepository>,
                Arc::new(PgPositionRepository::new(db.clone())) as Arc<dyn PositionRepository>,
                Arc::new(PgAttributionRepository::new(db.clone()))
                    as Arc<dyn AttributionRepository>,
                Arc::new(PgReconciliationRepository::new(db.clone()))
                    as Arc<dyn ReconciliationRepository>,
                Arc::new(PgSettlementRedeemRepository::new(db.clone()))
                    as Arc<dyn SettlementRedeemRepository>,
            )) as Arc<dyn ExecutionReadPort>,
            execution_submit: Arc::new(MockExecutionSubmit) as Arc<dyn ExecutionSubmitPort>,
            reconciliation: Arc::new(MockReconciliationPort) as Arc<dyn ReconciliationPort>,
            execution_recovery: Arc::new(MockExecutionRecoveryPort)
                as Arc<dyn ExecutionRecoveryPort>,
        };

        Self {
            state,
            db,
            ad_hoc_enqueued: Arc::clone(&core_report.enqueued),
            core_report,
            pg_container,
            redis: Some(redis_container),
            ws_shutdown,
        }
    }

    pub const fn take_redis(&mut self) -> Option<ContainerAsync<Redis>> {
        self.redis.take()
    }

    /// Bootstrap ids for [`seed_fixture_published_report`].
    #[must_use]
    pub fn fixture_report_ctx(&self) -> FixtureReportSeedContext {
        self.core_report.fixture_ctx.clone()
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        std::hint::black_box(&self.pg_container);
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

pub struct HttpResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub raw_body: Vec<u8>,
    pub body: Value,
}

pub type Resp = HttpResponse;

impl HttpResponse {
    #[must_use]
    pub const fn json(&self) -> &Value {
        &self.body
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name)?.to_str().ok()
    }

    #[must_use]
    pub fn body_bytes(&self) -> &[u8] {
        &self.raw_body
    }
}

pub async fn call(state: &AppState, request: TestRequest) -> HttpResponse {
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state.clone()))
            .wrap(from_fn(middleware::operation_audit))
            .wrap(from_fn(middleware::request_id))
            .configure(routes::configure),
    )
    .await;

    let resp = match test::try_call_service(&app, request.to_request()).await {
        Ok(resp) => {
            let status = resp.status();
            let headers = resp.headers().clone();
            let bytes = to_bytes(resp.into_body()).await.unwrap_or_default();
            (status, headers, bytes)
        }
        Err(err) => {
            let resp = err.error_response();
            let status = resp.status();
            let headers = resp.headers().clone();
            let bytes = to_bytes(resp.into_body()).await.unwrap_or_default();
            (status, headers, bytes)
        }
    };
    let body = serde_json::from_slice(&resp.2).unwrap_or(Value::Null);
    HttpResponse {
        status: resp.0,
        headers: resp.1,
        raw_body: resp.2.to_vec(),
        body,
    }
}

#[derive(Default)]
struct MockCatalogStatus;

impl CatalogStatusPort for MockCatalogStatus {
    fn catalog_state(&self) -> CatalogState {
        CatalogState::Ready {
            markets: 1,
            synced_at: chrono::Utc::now(),
        }
    }

    fn is_ready(&self) -> bool {
        true
    }
}

struct MockDataQuality;

impl DataQualityPort for MockDataQuality {
    fn snapshot(&self) -> DataQualitySnapshot {
        DataQualitySnapshot::empty(chrono::Utc::now(), 5_000, 30_000)
    }
}

struct MockMetricsScrape;

impl MetricsScrapePort for MockMetricsScrape {
    fn gather_prometheus(&self) -> String {
        String::new()
    }
}

/// No-op quant fact read port for web integration tests. The market-detail
/// microstructure endpoint is exercised for routing / RBAC only; the live
/// ClickHouse-backed series are covered by repository integration tests.
struct MockQuantFactRead;

#[async_trait]
impl QuantFactReadRepository for MockQuantFactRead {
    async fn microstructure_window(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn microstructure_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _minute: bool,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn last_trades(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _limit: u64,
    ) -> Result<Vec<TickEventRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn mid_price_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _bucket_secs: u32,
    ) -> Result<Vec<MidPriceBucketRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn book_snapshot_at(
        &self,
        _token_id: &TokenId,
        _as_of_ms: i64,
    ) -> Result<Option<BookSnapshotRow>, StorageError> {
        Ok(None)
    }

    async fn book_snapshots_between(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
    ) -> Result<Vec<BookSnapshotRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn resolution_at(
        &self,
        _market_id: &MarketId,
        _as_of_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        Ok(None)
    }

    async fn resolutions_between(
        &self,
        _market_ids: Vec<MarketId>,
        _from_ms: i64,
        _to_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError> {
        Ok(Vec::new())
    }
}

struct MockMarketData;

#[async_trait]
impl MarketDataPort for MockMarketData {
    fn book(
        &self,
        _yes: &TokenId,
        _no: &TokenId,
    ) -> (Option<Arc<BookSnapshot>>, Option<Arc<BookSnapshot>>) {
        (None, None)
    }

    fn subscribed_tokens(&self, _token_ids: &[TokenId]) -> HashSet<TokenId> {
        HashSet::new()
    }

    fn all_subscribed_tokens(&self) -> HashSet<TokenId> {
        HashSet::new()
    }

    async fn subscribe(&self, _token_ids: Vec<TokenId>) -> Result<(), ControlError> {
        Ok(())
    }

    async fn unsubscribe(&self, _token_ids: Vec<TokenId>) -> Result<(), ControlError> {
        Ok(())
    }
}

pub struct MockRuntimeConfigApply {
    current: Mutex<Arc<RuntimeConfig>>,
    fail_next_apply: AtomicBool,
}

impl Default for MockRuntimeConfigApply {
    fn default() -> Self {
        Self {
            current: Mutex::new(Arc::new(RuntimeConfig::default())),
            fail_next_apply: AtomicBool::new(false),
        }
    }
}

#[async_trait]
impl RuntimeConfigPort for MockRuntimeConfigApply {
    fn current(&self) -> Arc<RuntimeConfig> {
        Arc::clone(&self.current.lock().unwrap())
    }

    fn preflight(&self, _candidate: &RuntimeConfig) -> Result<(), ControlError> {
        Ok(())
    }

    async fn apply(&self, config: RuntimeConfig) -> Result<(), ControlError> {
        if self.fail_next_apply.swap(false, Ordering::SeqCst) {
            return Err(ControlError::Engine("injected apply failure".into()));
        }
        *self.current.lock().unwrap() = Arc::new(config);
        Ok(())
    }
}

#[derive(Default)]
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

#[derive(Default)]
pub struct MockRuntimeControl {
    mode: Mutex<QuantRuntimeMode>,
}

/// Execution-submit port stub for web integration tests. Submission requires
/// real venue + admission wiring (covered by core integration tests), so the
/// web harness only exercises routing / RBAC and fails closed here.
pub struct MockExecutionSubmit;

#[derive(Default)]
pub struct MockReconciliationPort;

#[async_trait]
impl ReconciliationPort for MockReconciliationPort {
    async fn resolve_operator(
        &self,
        _: ResolveReconciliationCommand,
    ) -> QuantResult<ResolveReconciliationOutcome> {
        Err(ExecutionError::ReconciliationNotResolvable {
            reconciliation_id: "mock".into(),
            result: "mock".into(),
        }
        .into())
    }
}

#[derive(Default)]
pub struct MockExecutionRecoveryPort;

#[async_trait]
impl ExecutionRecoveryPort for MockExecutionRecoveryPort {
    async fn view(&self) -> QuantResult<ExecutionRecoveryView> {
        Ok(ExecutionRecoveryView {
            summary: SystemStatus::bootstrap(QuantRuntimeMode::ReportOnly).execution_recovery,
            blocking_reconciliations: Vec::new(),
            kill_switch: KillSwitchView {
                state: KillSwitchState::Closed,
                requires_operator_ack: false,
                last_reason: "mock".into(),
                changed_by: "mock".into(),
                changed_at: chrono::Utc::now(),
            },
        })
    }
}

#[async_trait]
impl ExecutionSubmitPort for MockExecutionSubmit {
    async fn submit_if_admitted(
        &self,
        intent_id: &OrderIntentId,
    ) -> QuantResult<ExecutionOrderInfo> {
        Err(ExecutionError::NotSubmittable {
            intent_id: intent_id.to_string(),
            state: "mock".to_owned(),
        }
        .into())
    }
}

/// In-memory kill-switch port for web integration tests.
pub struct MockKillSwitch {
    view: Mutex<KillSwitchView>,
}

impl Default for MockKillSwitch {
    fn default() -> Self {
        Self {
            view: Mutex::new(KillSwitchView {
                state: KillSwitchState::Closed,
                requires_operator_ack: false,
                last_reason: "test bootstrap".to_owned(),
                changed_by: "test".to_owned(),
                changed_at: chrono::Utc::now(),
            }),
        }
    }
}

#[async_trait]
impl KillSwitchPort for MockKillSwitch {
    fn current(&self) -> KillSwitchState {
        self.view.lock().unwrap().state
    }

    fn view(&self) -> KillSwitchView {
        self.view.lock().unwrap().clone()
    }

    async fn set(&self, command: SetKillSwitchCommand) -> QuantResult<KillSwitchView> {
        let mut guard = self.view.lock().unwrap();
        *guard = KillSwitchView {
            state: command.target,
            requires_operator_ack: command.target.is_emergency(),
            last_reason: command.reason,
            changed_by: command.actor,
            changed_at: chrono::Utc::now(),
        };
        Ok(guard.clone())
    }
}

/// No-op model-training port for web integration tests.
pub struct MockModelTrainingPort;

#[async_trait]
impl ModelTrainingPort for MockModelTrainingPort {
    async fn train(&self, _request: TrainModelRequest) -> QuantResult<TrainedModelView> {
        Err(QuantError::NotImplemented("model train".into()))
    }

    async fn find_version(
        &self,
        _model_version_id: &ModelVersionId,
    ) -> QuantResult<Option<ModelVersionInfo>> {
        Ok(None)
    }
}

/// No-op backtest port for web integration tests.
pub struct MockBacktestPort;

#[async_trait]
impl BacktestPort for MockBacktestPort {
    async fn run(
        &self,
        _model_version_id: ModelVersionId,
        _request: RunBacktestRequest,
    ) -> QuantResult<BacktestReportView> {
        Err(QuantError::NotImplemented("backtest run".into()))
    }

    async fn find_report(
        &self,
        _backtest_report_id: &BacktestReportId,
    ) -> QuantResult<Option<BacktestReportView>> {
        Ok(None)
    }

    async fn comparison_ids_for_backtest_reports(
        &self,
        _backtest_report_ids: &[BacktestReportId],
    ) -> QuantResult<HashMap<BacktestReportId, ModelComparisonReportId>> {
        Ok(HashMap::new())
    }

    async fn find_comparison_report(
        &self,
        _comparison_report_id: &ModelComparisonReportId,
    ) -> QuantResult<Option<ModelComparisonReportInfo>> {
        Ok(None)
    }
}

/// No-op research catalog port for web integration tests (empty pages).
pub struct MockResearchCatalogPort;

#[async_trait]
impl ResearchCatalogPort for MockResearchCatalogPort {
    async fn list_training_datasets(
        &self,
        query: TrainingDatasetListQuery,
    ) -> QuantResult<Paginated<TrainingDatasetInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn list_models(
        &self,
        query: ModelVersionListQuery,
    ) -> QuantResult<Paginated<ModelVersionInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn list_model_specs(
        &self,
        query: ModelSpecListQuery,
    ) -> QuantResult<Paginated<ModelSpecInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn list_backtest_reports(
        &self,
        query: BacktestReportListQuery,
    ) -> QuantResult<Paginated<BacktestReportInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn list_comparison_reports(
        &self,
        query: ComparisonReportListQuery,
    ) -> QuantResult<Paginated<ModelComparisonReportInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn list_factors(
        &self,
        query: FactorDefinitionListQuery,
    ) -> QuantResult<Paginated<FactorDefinitionInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn find_factor(
        &self,
        _factor_definition_id: &FactorDefinitionId,
    ) -> QuantResult<Option<FactorDefinitionInfo>> {
        Ok(None)
    }

    async fn factor_collinearity(
        &self,
        lookback_secs: u64,
        threshold: rust_decimal::Decimal,
    ) -> QuantResult<FactorCollinearityView> {
        Ok(FactorCollinearityView {
            factors: Vec::new(),
            matrix: Vec::new(),
            violations: Vec::new(),
            threshold,
            observation_count: 0,
            lookback_secs,
        })
    }
}

/// No-op model-governance port for web integration tests.
pub struct MockModelGovernancePort;

#[async_trait]
impl ModelGovernancePort for MockModelGovernancePort {
    async fn publish(
        &self,
        _command: PublishModelCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo> {
        Err(QuantError::NotImplemented("model publish".into()))
    }

    async fn rollback(
        &self,
        _command: RollbackModelCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo> {
        Err(QuantError::NotImplemented("model rollback".into()))
    }

    async fn retire(
        &self,
        _command: RetireModelCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo> {
        Err(QuantError::NotImplemented("model retire".into()))
    }

    async fn promote_dataset_ready(
        &self,
        _request: PromoteDatasetRequest,
        _actor: GovernanceActor,
    ) -> QuantResult<TrainingDatasetInfo> {
        Err(QuantError::NotImplemented("dataset promote".into()))
    }

    async fn preview_gate(
        &self,
        _model_version_id: &ModelVersionId,
        _intent: GatePreviewIntent,
        _backtest_report_id: Option<&BacktestReportId>,
    ) -> QuantResult<QualityGateReportView> {
        Err(QuantError::NotImplemented(
            "model quality-gate preview".into(),
        ))
    }
}

/// No-op factor-governance port for web integration tests.
pub struct MockFactorGovernancePort;

#[async_trait]
impl FactorGovernancePort for MockFactorGovernancePort {
    async fn find_definition(
        &self,
        _factor_definition_id: &FactorDefinitionId,
    ) -> QuantResult<Option<quant_pivot_models::domain::FactorDefinitionInfo>> {
        Ok(None)
    }

    async fn publish(
        &self,
        _command: PublishFactorCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<quant_pivot_models::domain::FactorDefinitionInfo> {
        Err(QuantError::NotImplemented("factor publish".into()))
    }

    async fn retire(
        &self,
        _command: RetireFactorCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<quant_pivot_models::domain::FactorDefinitionInfo> {
        Err(QuantError::NotImplemented("factor retire".into()))
    }
}

/// No-op training-dataset port for web integration tests.
pub struct MockTrainingDatasetPort;

#[async_trait]
impl TrainingDatasetPort for MockTrainingDatasetPort {
    async fn find_by_id(
        &self,
        _training_dataset_id: &TrainingDatasetId,
    ) -> QuantResult<Option<TrainingDatasetInfo>> {
        Ok(None)
    }

    async fn plan(
        &self,
        _request: BuildTrainingDatasetRequest,
    ) -> QuantResult<TrainingDatasetPlanView> {
        Err(QuantError::NotImplemented("training dataset plan".into()))
    }

    async fn build(
        &self,
        _request: BuildTrainingDatasetRequest,
    ) -> QuantResult<TrainingDatasetView> {
        Err(QuantError::NotImplemented("training dataset build".into()))
    }
}

#[async_trait]
impl RuntimeControlPort for MockRuntimeControl {
    fn quant_runtime_mode(&self) -> QuantRuntimeMode {
        *self.mode.lock().unwrap()
    }

    async fn switch_quant_mode(
        &self,
        target: QuantRuntimeMode,
        _actor: &str,
        _reason: &str,
    ) -> QuantResult<QuantModeTransitionReport> {
        let from = self.quant_runtime_mode();
        *self.mode.lock().unwrap() = target;
        Ok(QuantModeTransitionReport {
            from,
            to: target,
            preflight: None,
        })
    }

    fn system_status(&self) -> SystemStatus {
        SystemStatus::bootstrap(self.quant_runtime_mode())
    }

    async fn health(&self) -> HealthReport {
        HealthReport::from_checks(Vec::new(), chrono::Utc::now())
    }
}
