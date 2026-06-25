//! Integration-test harness (Phase 0).

use std::{
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
use quant_pivot_error::auth::AuthError;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::{CacheConfig, DeployConfig, JwtConfig, RedisConfig},
    domain::{
        BacktestPort, BacktestReportInfo, BacktestReportView, BookSnapshot,
        BuildTrainingDatasetRequest, CatalogState, CatalogStatusPort, CoreEventPublisher,
        DataQualityPort, DataQualitySnapshot, HealthReport, MarketDataPort, MetricsScrapePort,
        ModelComparisonReportInfo, ModelTrainingPort, ModelVersionInfo, QuantModeTransitionReport,
        RunBacktestRequest, RuntimeConfigPort, RuntimeControlError, RuntimeControlPort,
        SystemStatus, TrainModelRequest, TrainedModelView, TrainingDatasetInfo,
        TrainingDatasetPlanView, TrainingDatasetPort, TrainingDatasetView,
    },
    enums::quant::QuantRuntimeMode,
    runtime_config::RuntimeConfig,
    types::{
        BacktestReportId, ModelComparisonReportId, ModelVersionId, TokenId, TrainingDatasetId,
    },
};
use quant_pivot_storage::{
    cache::connect_pool,
    write::{AsyncWriter, AsyncWriterConfig, AsyncWriterObservability},
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
use serde_json::Value;
use std::{collections::HashSet, sync::Mutex};
use testcontainers::ContainerAsync;
use testcontainers_modules::{postgres::Postgres, redis::Redis};
use tokio_util::sync::CancellationToken;

use crate::{pg, redis, repos::WebHarnessRepos};

pub const API_VERSION: (&str, &str) = ("Accept-Api-Version", "v1");

const TEST_JWT_SECRET: &str = "quant-pivot-integration-test-secret-not-for-production";
const TEST_ISSUER: &str = "quant-pivot-test";

pub struct TestEnv {
    pub state: AppState,
    pg_container: ContainerAsync<Postgres>,
    redis: Option<ContainerAsync<Redis>>,
    writer_shutdown: CancellationToken,
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

        let writer_shutdown = CancellationToken::new();
        let op_log_repo = Arc::clone(&repos.operation_logs);
        let (op_log_writer, op_log_worker) = AsyncWriter::new(
            AsyncWriterConfig::new("operation_log")
                .capacity(1024)
                .batch_size(64)
                .flush_interval(Duration::from_millis(50)),
            move |rows| {
                let repo = Arc::clone(&op_log_repo);
                Box::pin(async move { repo.append_batch(rows).await })
            },
            prometheus::IntCounter::new("test_operation_log_drops", "test drop counter")
                .expect("counter"),
            AsyncWriterObservability::default(),
        );
        let operation_log = OperationLogBuffer::new(Arc::new(op_log_writer));
        tokio::spawn(op_log_worker.run(writer_shutdown.clone()));

        let (events, event_rx) = CoreEventPublisher::bounded(256);
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
            market_data: Arc::new(MockMarketData),
            catalog: Arc::clone(&catalog) as Arc<dyn CatalogStatusPort>,
            data_quality: Arc::clone(&data_quality) as Arc<dyn DataQualityPort>,
            events,
            markets: Arc::clone(&repos.markets),
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
        };

        Self {
            state,
            pg_container,
            redis: Some(redis_container),
            writer_shutdown,
            ws_shutdown,
        }
    }

    pub const fn take_redis(&mut self) -> Option<ContainerAsync<Redis>> {
        self.redis.take()
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        std::hint::black_box(&self.pg_container);
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
            .wrap(from_fn(middleware::request_id))
            .configure(routes::configure),
    )
    .await;

    let resp = test::call_service(&app, request.to_request()).await;
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = to_bytes(resp.into_body()).await.unwrap_or_default();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    HttpResponse {
        status,
        headers,
        raw_body: bytes.to_vec(),
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

    async fn subscribe(&self, _token_ids: Vec<TokenId>) -> Result<(), RuntimeControlError> {
        Ok(())
    }

    async fn unsubscribe(&self, _token_ids: Vec<TokenId>) -> Result<(), RuntimeControlError> {
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

    fn preflight(&self, _candidate: &RuntimeConfig) -> Result<(), RuntimeControlError> {
        Ok(())
    }

    async fn apply(&self, config: RuntimeConfig) -> Result<(), RuntimeControlError> {
        if self.fail_next_apply.swap(false, Ordering::SeqCst) {
            return Err(RuntimeControlError::Engine("injected apply failure".into()));
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
    ) -> QuantResult<Option<BacktestReportInfo>> {
        Ok(None)
    }

    async fn find_comparison_report(
        &self,
        _comparison_report_id: &ModelComparisonReportId,
    ) -> QuantResult<Option<ModelComparisonReportInfo>> {
        Ok(None)
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
        _reason: &str,
    ) -> Result<QuantModeTransitionReport, RuntimeControlError> {
        let from = self.quant_runtime_mode();
        *self.mode.lock().unwrap() = target;
        Ok(QuantModeTransitionReport { from, to: target })
    }

    fn system_status(&self) -> SystemStatus {
        SystemStatus::report_only_bootstrap(self.quant_runtime_mode())
    }

    async fn health(&self) -> HealthReport {
        HealthReport::from_checks(Vec::new(), chrono::Utc::now())
    }
}
