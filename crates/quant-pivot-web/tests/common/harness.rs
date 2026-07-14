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
    app::ports::execution_read::CoreExecutionReadPort, execution::IntentLifecyclePublisher,
    report::AdHocReportRequest,
};
use quant_pivot_error::{
    QuantError, QuantResult, auth::AuthError, control::ControlError, execution::ExecutionError,
    storage::StorageError,
};
use quant_pivot_models::{
    clickhouse::{
        BookL2CheckpointRow, BookMicrostructureRow, DomainObservationRow,
        EntryConditionEvaluationEventRow, MarketResolutionRow, MidPriceBucketRow, TradeTapeRow,
    },
    config::{CacheConfig, DeployConfig, JwtConfig, RedisConfig},
    domain::{
        AcknowledgeFeatureParityLatchRequest, BacktestPathSetInfo, BacktestPathSetListQuery,
        BacktestPathSetView, BacktestPort, BacktestReportInfo, BacktestReportListQuery,
        BacktestReportView, BasisAlertInfo, BasisAlertListQuery, BiasTableFitJobParams,
        BiasTableFitOutcome, BindCalibrationRequest, BindPublishPathSetRequest, BookSnapshot,
        BuildTrainingDatasetRequest, CalibrationArtifactFitPort, CalibrationArtifactInfo,
        CalibrationArtifactListQuery, CatalogState, CatalogStatusPort, ComparisonReportListQuery,
        CoreEventPublisher, CpcvBacktestPort, CreateModelSpecCommand, DataQualityPort,
        DataQualitySnapshot, DecisionBoundary, DomainSourceCursorInfo, ExecutionReadPort,
        ExecutionRecoveryPort, ExecutionRecoveryView, FactorCollinearitySource,
        FactorCollinearityView, FactorDefinitionInfo, FactorDefinitionListQuery,
        FactorGovernancePort, FeatureContractEntryView, FeatureContractView,
        FeatureIntegrityActionContext, FeatureIntegrityLatchView, FeatureIntegrityPort,
        FeatureIntegritySummaryView, FeatureNullPolicyView, FeatureParityEventListQuery,
        FeatureParityEventView, FeatureParityRunListQuery, FeatureParityRunView,
        FitBiasTableRequest, FitModelCalibratorRequest, GatePreviewIntent, GovernanceActor,
        HealthReport, JobProgressSink, JobSubmitContext, KillSwitchPort, KillSwitchView,
        LinkageResolveSummaryView, MarketDataPort, MarketLinkageGovernancePort, MarketLinkageInfo,
        MarketLinkageListQuery, MetricsScrapePort, MissingReasonCountView,
        ModelCalibrationFitJobParams, ModelCalibrationFitOutcome, ModelCalibrationFitPort,
        ModelCalibrationFitPreflightView, ModelComparisonReportInfo, ModelGovernancePort,
        ModelPublishedCatalogQuery, ModelSpecInfo, ModelSpecListQuery, ModelSpecPort,
        ModelTrainingPort, ModelVersionInfo, ModelVersionListQuery, NegRiskEventDriftView,
        NewBasisAlert, NewMarketLinkage, OverrideLinkageRequest, Paginated,
        ParticipantConcentrationDetailView, ParticipantConcentrationSummaryView,
        PublishFactorCommand, PublishFactorsBatchCommand, PublishModelCommand,
        PublishedModelOptionView, QualityGateReportView, QuantModeTransitionReport,
        ReconciliationPort, RegisterFactorDefinitionsCommand, ResearchCatalogPort,
        ResearchJobListQuery, ResearchJobPort, ResearchJobView, ResolveReconciliationCommand,
        ResolveReconciliationOutcome, RetireFactorCommand, RetireModelCommand,
        RollbackModelCommand, RunBacktestRequest, RunCpcvBacktestRequest,
        RunFullFeatureParityRequest, RuntimeConfigPort, RuntimeControlPort, SetKillSwitchCommand,
        StructuralMonitorPort, SystemStatus, TradePolicyArtifactInfo, TradePolicyAuditListQuery,
        TradePolicyFitPreflightView, TradePolicyGovernanceAuditInfo, TradePolicyListQuery,
        TradePolicyPort, TradeTapeCoverageView, TradeTapeSourceHealthView, TrainModelRequest,
        TrainedModelView, TrainingDatasetInfo, TrainingDatasetListQuery, TrainingDatasetPlanView,
        TrainingDatasetPort, TrainingDatasetView, UpsertDomainSourceCursor, empty_catalog_page,
    },
    entities::quant_entry_condition_evaluation_outbox,
    enums::{execution::KillSwitchState, quant::QuantRuntimeMode},
    runtime_config::{FeatureFamily, RuntimeConfig},
    types::{
        BacktestPathSetId, BacktestReportId, BasisAlertId, CalibrationArtifactId, ContentHash,
        DomainInstrumentKey, DomainSourceId, EntryConditionInstanceId, FactorDefinitionId,
        MarketId, MarketLinkageId, ModelComparisonReportId, ModelSpecId, ModelVersionId,
        ResearchJobId, RuntimeConfigVersionId, SchemaVersion, TokenId, TrainingDatasetId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgAttributionRepository, PgEntryConditionRepository, PgExecutionOrderRepository,
        PgPositionRepository, PgReconciliationRepository, PgSettlementRedeemRepository,
    },
    traits::{
        AttributionRepository, BasisAlertRepository, DomainSourceCursorRepository,
        ExecutionOrderRepository, MarketLinkageRepository, PositionRepository,
        QuantFactReadRepository, ReconciliationRepository, SettlementRedeemRepository,
    },
};
use quant_pivot_research::features::FeatureValueKind;
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
use rust_decimal::Decimal;
use sea_orm::{DatabaseConnection, EntityTrait, QueryOrder};
use serde_json::Value;
use std::{collections::HashSet, sync::Mutex};
use testcontainers::ContainerAsync;
use testcontainers_modules::{postgres::Postgres, redis::Redis};
use tokio_util::sync::CancellationToken;

use quant_pivot_models::domain::OrderIntentPort;

use crate::{
    core_report_port::{self, CoreReportTestHandle},
    order_intent_port::build_order_intent_service,
    pg, redis,
    repos::WebHarnessRepos,
};

pub const API_VERSION: (&str, &str) = ("Accept-Api-Version", "v1");

const TEST_JWT_SECRET: &str = "quant-pivot-integration-test-secret-not-for-production";
const TEST_ISSUER: &str = "quant-pivot-test";

struct WebHarnessAppStateInput<'a> {
    db: &'a DatabaseConnection,
    repos: &'a WebHarnessRepos,
    jwt: Arc<JwtService>,
    jwt_blacklist: Arc<RedisTokenBlacklist>,
    token_blacklist: Arc<dyn TokenBlacklist>,
    casbin: Arc<CasbinService>,
    operation_log: OperationLogBuffer,
    events: CoreEventPublisher,
    ws_sessions: SessionRegistry,
    runtime_config_apply: Arc<MockRuntimeConfigApply>,
    catalog: Arc<MockCatalogStatus>,
    data_quality: Arc<MockDataQuality>,
    core_report: &'a CoreReportTestHandle,
    order_intents: Arc<dyn OrderIntentPort>,
}

fn web_harness_app_state(input: WebHarnessAppStateInput<'_>) -> AppState {
    AppState {
        deploy: Arc::new(test_deploy_config()),
        runtime_config_apply: Arc::clone(&input.runtime_config_apply) as Arc<dyn RuntimeConfigPort>,
        jwt: input.jwt,
        jwt_blacklist: input.jwt_blacklist,
        users: Arc::clone(&input.repos.users),
        roles: Arc::clone(&input.repos.roles),
        menus: Arc::clone(&input.repos.menus),
        user_roles: Arc::clone(&input.repos.user_roles),
        role_menus: Arc::clone(&input.repos.role_menus),
        role_permissions: Arc::clone(&input.repos.role_permissions),
        casbin: input.casbin,
        perm_checker: Arc::new(routes::init_rbac_rules()),
        runtime_config: Arc::clone(&input.repos.runtime_config),
        operation_logs: Arc::clone(&input.repos.operation_logs),
        operation_log: input.operation_log,
        control: Arc::new(MockRuntimeControl::default()),
        kill_switch: Arc::new(MockKillSwitch::default()),
        market_data: Arc::new(MockMarketData),
        catalog: Arc::clone(&input.catalog) as Arc<dyn CatalogStatusPort>,
        data_quality: Arc::clone(&input.data_quality) as Arc<dyn DataQualityPort>,
        events: input.events,
        markets: Arc::clone(&input.repos.markets),
        quant_facts: Arc::new(MockQuantFactRead::default()),
        ws_sessions: input.ws_sessions,
        metrics: Arc::new(MockMetricsScrape),
        readiness: Arc::new(PgRedisReadiness::new(
            input.db.clone(),
            input.token_blacklist,
            Some(Arc::clone(&input.catalog) as Arc<dyn CatalogStatusPort>),
        )),
        training_datasets: Arc::new(MockTrainingDatasetPort),
        trade_policies: Arc::new(MockTradePolicyPort),
        model_training: Arc::new(MockModelTrainingPort),
        backtests: Arc::new(MockBacktestPort),
        cpcv_backtests: Arc::new(MockCpcvBacktestPort),
        model_governance: Arc::new(MockModelGovernancePort),
        factor_governance: Arc::new(MockFactorGovernancePort),
        model_spec: Arc::new(MockModelSpecPort),
        research_catalog: Arc::new(MockResearchCatalogPort),
        research_jobs: Arc::new(MockResearchJobPort),
        feature_integrity: Arc::new(MockFeatureIntegrityPort),
        calibration_artifacts: Arc::new(MockCalibrationArtifactFitPort),
        model_calibration_fit: Arc::new(MockModelCalibrationFitPort),
        market_linkages: Arc::new(MockMarketLinkageRepository),
        domain_source_cursors: Arc::new(MockDomainSourceCursorRepository),
        basis_alerts: Arc::new(MockBasisAlertRepository),
        linkage_governance: Arc::new(MockMarketLinkageGovernancePort),
        structural_monitor: Arc::new(MockStructuralMonitorPort),
        quant_reports: input.core_report.port.clone(),
        account_read: core_account_read_port(
            input.db,
            Arc::clone(&input.runtime_config_apply) as Arc<dyn RuntimeConfigPort>,
        ),
        order_intents: input.order_intents,
        entry_conditions: Arc::new(PgEntryConditionRepository::new(input.db.clone())),
        execution_read: Arc::new(CoreExecutionReadPort::new(
            Arc::new(PgExecutionOrderRepository::new(input.db.clone()))
                as Arc<dyn ExecutionOrderRepository>,
            Arc::new(PgPositionRepository::new(input.db.clone())) as Arc<dyn PositionRepository>,
            Arc::new(PgAttributionRepository::new(input.db.clone()))
                as Arc<dyn AttributionRepository>,
            Arc::new(PgReconciliationRepository::new(input.db.clone()))
                as Arc<dyn ReconciliationRepository>,
            Arc::new(PgSettlementRedeemRepository::new(input.db.clone()))
                as Arc<dyn SettlementRedeemRepository>,
        )) as Arc<dyn ExecutionReadPort>,
        reconciliation: Arc::new(MockReconciliationPort) as Arc<dyn ReconciliationPort>,
        execution_recovery: Arc::new(MockExecutionRecoveryPort) as Arc<dyn ExecutionRecoveryPort>,
    }
}

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
        let jwt_blacklist = connect_blacklist(&redis::test_redis_config(redis_port)).await;
        let blacklist = Arc::clone(&jwt_blacklist) as Arc<dyn TokenBlacklist>;
        let jwt = Arc::new(JwtService::new(&jwt_config(), Arc::clone(&blacklist)));
        let db = pool.connection().clone();
        let casbin = Arc::new(
            CasbinService::new(db.clone())
                .await
                .expect("casbin service"),
        );
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
        let core_report = core_report_port::build_core_report_stack(&db, events.clone()).await;
        let order_intents = build_order_intent_service(&db, Arc::clone(&intent_lifecycle));
        let state = web_harness_app_state(WebHarnessAppStateInput {
            db: &db,
            repos: &repos,
            jwt,
            jwt_blacklist,
            token_blacklist: blacklist,
            casbin,
            operation_log,
            events,
            ws_sessions,
            runtime_config_apply,
            catalog,
            data_quality,
            core_report: &core_report,
            order_intents: order_intents as Arc<dyn OrderIntentPort>,
        });

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
#[derive(Default)]
pub struct MockQuantFactRead {
    evaluation_db: Option<DatabaseConnection>,
}

impl MockQuantFactRead {
    pub const fn with_evaluation_outbox(db: DatabaseConnection) -> Self {
        Self {
            evaluation_db: Some(db),
        }
    }
}

#[async_trait]
impl QuantFactReadRepository for MockQuantFactRead {
    async fn latest_applied_entry_condition_evaluation(
        &self,
        instance_id: &EntryConditionInstanceId,
    ) -> Result<Option<EntryConditionEvaluationEventRow>, StorageError> {
        let Some(db) = &self.evaluation_db else {
            return Ok(None);
        };
        let rows = quant_entry_condition_evaluation_outbox::Entity::find()
            .order_by_desc(quant_entry_condition_evaluation_outbox::Column::CreatedAt)
            .all(db)
            .await
            .map_err(StorageError::from)?;
        Ok(rows.into_iter().map(|row| row.event_json).find(|event| {
            event.condition_instance_id == *instance_id && event.applied_revision.is_some()
        }))
    }

    async fn microstructure_window(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn microstructure_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _available_by_ms: i64,
        _minute: bool,
    ) -> Result<Vec<BookMicrostructureRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn trade_tape_window_by_market(
        &self,
        _market_ids: Vec<MarketId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn last_trades(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _limit: u64,
    ) -> Result<Vec<TradeTapeRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn mid_price_series(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
        _bucket_secs: u32,
    ) -> Result<Vec<MidPriceBucketRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn book_checkpoint_at(
        &self,
        _token_id: &TokenId,
        _as_of_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<BookL2CheckpointRow>, StorageError> {
        Ok(None)
    }

    async fn book_checkpoints_between(
        &self,
        _token_ids: Vec<TokenId>,
        _from_ms: i64,
        _to_ms: i64,
        _available_by_ms: i64,
    ) -> Result<Vec<BookL2CheckpointRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn resolution_at(
        &self,
        _market_id: &MarketId,
        _source_cutoff_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<MarketResolutionRow>, StorageError> {
        Ok(None)
    }

    async fn resolutions_between(
        &self,
        _market_ids: Vec<MarketId>,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<MarketResolutionRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn domain_observations_between(
        &self,
        _instrument_keys: Vec<DomainInstrumentKey>,
        _from_ms: i64,
        _to_ms: i64,
        _publish_cutoff_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<DomainObservationRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn domain_observation_at(
        &self,
        _instrument_key: &DomainInstrumentKey,
        _metric: &str,
        _as_of_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Option<DomainObservationRow>, StorageError> {
        Ok(None)
    }

    async fn observed_markets_between(
        &self,
        _from_ms: i64,
        _to_ms: i64,
        _decision_at_ms: i64,
    ) -> Result<Vec<MarketId>, StorageError> {
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

pub struct MockTradePolicyPort;

#[async_trait]
impl TradePolicyPort for MockTradePolicyPort {
    async fn preflight(
        &self,
        _: &quant_pivot_models::domain::TradePolicyFitPreflightRequest,
    ) -> QuantResult<TradePolicyFitPreflightView> {
        Err(StorageError::NotFound {
            entity: "trade_policy_artifact",
            id: "mock".into(),
        }
        .into())
    }

    async fn fit(
        &self,
        _: quant_pivot_models::domain::FitTradePolicyRequest,
    ) -> QuantResult<TradePolicyArtifactInfo> {
        Err(StorageError::NotFound {
            entity: "trade_policy_artifact",
            id: "mock".into(),
        }
        .into())
    }

    async fn find(
        &self,
        _: &quant_pivot_models::types::TradePolicyArtifactId,
    ) -> QuantResult<Option<TradePolicyArtifactInfo>> {
        Ok(None)
    }

    async fn page(
        &self,
        query: TradePolicyListQuery,
    ) -> QuantResult<Paginated<TradePolicyArtifactInfo>> {
        Ok(Paginated::empty(query.page.page, query.page.size))
    }

    async fn page_audits(
        &self,
        _: &quant_pivot_models::types::TradePolicyArtifactId,
        query: TradePolicyAuditListQuery,
    ) -> QuantResult<Paginated<TradePolicyGovernanceAuditInfo>> {
        Ok(Paginated::empty(query.page.page, query.page.size))
    }

    async fn transition(
        &self,
        _: &quant_pivot_models::types::TradePolicyArtifactId,
        _: quant_pivot_models::enums::quant::TradePolicyStatus,
        _: uuid::Uuid,
        _: String,
    ) -> QuantResult<TradePolicyArtifactInfo> {
        Err(StorageError::NotFound {
            entity: "trade_policy_artifact",
            id: "mock".into(),
        }
        .into())
    }
}

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
    async fn train(
        &self,
        _model_version_id: ModelVersionId,
        _request: TrainModelRequest,
        _progress: Arc<dyn JobProgressSink>,
        _cancel: CancellationToken,
    ) -> QuantResult<TrainedModelView> {
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
        _progress: Arc<dyn JobProgressSink>,
        _cancel: CancellationToken,
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

/// No-op CPCV backtest port for web integration tests.
pub struct MockCpcvBacktestPort;

#[async_trait]
impl CpcvBacktestPort for MockCpcvBacktestPort {
    async fn run(
        &self,
        _model_version_id: ModelVersionId,
        _request: RunCpcvBacktestRequest,
        _progress: Arc<dyn JobProgressSink>,
        _cancel: CancellationToken,
    ) -> QuantResult<BacktestPathSetView> {
        Err(QuantError::NotImplemented("cpcv backtest run".into()))
    }

    async fn find_path_set(
        &self,
        _path_set_id: &BacktestPathSetId,
    ) -> QuantResult<Option<BacktestPathSetView>> {
        Ok(None)
    }

    async fn latest_path_set(
        &self,
        _model_version_id: &ModelVersionId,
    ) -> QuantResult<Option<BacktestPathSetView>> {
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

    async fn list_published_model_options(
        &self,
        _query: ModelPublishedCatalogQuery,
    ) -> QuantResult<Vec<PublishedModelOptionView>> {
        Ok(Vec::new())
    }

    async fn list_backtest_reports(
        &self,
        query: BacktestReportListQuery,
    ) -> QuantResult<Paginated<BacktestReportInfo>> {
        Ok(empty_catalog_page(&query))
    }

    async fn list_backtest_path_sets(
        &self,
        query: BacktestPathSetListQuery,
    ) -> QuantResult<Paginated<BacktestPathSetInfo>> {
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
        source: FactorCollinearitySource,
        _neutralize_by_category: bool,
    ) -> QuantResult<FactorCollinearityView> {
        Ok(FactorCollinearityView {
            factors: Vec::new(),
            matrix: Vec::new(),
            violations: Vec::new(),
            threshold,
            observation_count: 0,
            lookback_secs,
            panel_source: source,
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

    async fn bind_calibration(
        &self,
        _model_version_id: &ModelVersionId,
        _request: BindCalibrationRequest,
        _actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo> {
        Err(QuantError::NotImplemented("model bind calibration".into()))
    }

    async fn bind_publish_path_set(
        &self,
        _model_version_id: &ModelVersionId,
        _request: BindPublishPathSetRequest,
        _actor: GovernanceActor,
    ) -> QuantResult<ModelVersionInfo> {
        Err(QuantError::NotImplemented(
            "model bind publish path set".into(),
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
    ) -> QuantResult<Option<FactorDefinitionInfo>> {
        Ok(None)
    }

    async fn publish(
        &self,
        _command: PublishFactorCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<FactorDefinitionInfo> {
        Err(QuantError::NotImplemented("factor publish".into()))
    }

    async fn retire(
        &self,
        _command: RetireFactorCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<FactorDefinitionInfo> {
        Err(QuantError::NotImplemented("factor retire".into()))
    }

    async fn register_enabled_definitions(
        &self,
        _command: RegisterFactorDefinitionsCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<Vec<FactorDefinitionInfo>> {
        Err(QuantError::NotImplemented("factor register".into()))
    }

    async fn publish_batch(
        &self,
        _command: PublishFactorsBatchCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<Vec<FactorDefinitionInfo>> {
        Err(QuantError::NotImplemented("factor publish batch".into()))
    }
}

/// No-op model-spec authoring port for web integration tests.
pub struct MockModelSpecPort;

#[async_trait]
impl ModelSpecPort for MockModelSpecPort {
    async fn feature_contract(&self) -> QuantResult<FeatureContractView> {
        Ok(FeatureContractView {
            feature_schema_hash: ContentHash::parse(concat!(
                "blake3:",
                "0000000000000000000000000000000000000000000000000000000000000000"
            ))
            .expect("canonical feature schema hash fixture"),
            feature_schema_version: SchemaVersion::new(6),
            features: vec![FeatureContractEntryView {
                name: "book.mid".to_owned(),
                compute_revision: 1,
                family: FeatureFamily::PriceBook,
                value_kind: FeatureValueKind::Probability,
                unit: "probability".to_owned(),
                null_policy: FeatureNullPolicyView {
                    policy: "reject_market".to_owned(),
                    value: None,
                },
                source: "published_l2_book".to_owned(),
                point_in_time_rule: "book_version_at_or_before_source_cutoff".to_owned(),
                staleness_policy: "max_book_age".to_owned(),
            }],
        })
    }

    async fn create(
        &self,
        _command: CreateModelSpecCommand,
        _actor: GovernanceActor,
    ) -> QuantResult<ModelSpecInfo> {
        Err(QuantError::NotImplemented("model spec create".into()))
    }

    async fn find(&self, _model_spec_id: &ModelSpecId) -> QuantResult<Option<ModelSpecInfo>> {
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
        _progress: Arc<dyn JobProgressSink>,
        _cancel: CancellationToken,
    ) -> QuantResult<TrainingDatasetView> {
        Err(QuantError::NotImplemented("training dataset build".into()))
    }
}

/// No-op research-job engine port for web integration tests.
pub struct MockResearchJobPort;

#[async_trait]
impl ResearchJobPort for MockResearchJobPort {
    async fn enqueue_dataset_build(
        &self,
        _request: BuildTrainingDatasetRequest,
        _ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        Err(QuantError::NotImplemented("enqueue dataset build".into()))
    }

    async fn enqueue_model_train(
        &self,
        _request: TrainModelRequest,
        _ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        Err(QuantError::NotImplemented("enqueue model train".into()))
    }

    async fn enqueue_backtest(
        &self,
        _model_version_id: ModelVersionId,
        _request: RunBacktestRequest,
        _ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        Err(QuantError::NotImplemented("enqueue backtest".into()))
    }

    async fn enqueue_cpcv_backtest(
        &self,
        _model_version_id: ModelVersionId,
        _request: RunCpcvBacktestRequest,
        _ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        Err(QuantError::NotImplemented("enqueue cpcv backtest".into()))
    }

    async fn enqueue_bias_table_fit(
        &self,
        _request: FitBiasTableRequest,
        _runtime_config_version_id: RuntimeConfigVersionId,
        _ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        Err(QuantError::NotImplemented("enqueue bias table fit".into()))
    }

    async fn enqueue_model_calibration_fit(
        &self,
        _request: FitModelCalibratorRequest,
        _runtime_config_version_id: RuntimeConfigVersionId,
        _ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        Err(QuantError::NotImplemented(
            "enqueue model calibrator fit".into(),
        ))
    }

    async fn enqueue_trade_policy_fit(
        &self,
        _request: quant_pivot_models::domain::FitTradePolicyRequest,
        _ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        Err(QuantError::NotImplemented(
            "enqueue trade policy fit".into(),
        ))
    }

    async fn list(&self, _query: ResearchJobListQuery) -> QuantResult<Paginated<ResearchJobView>> {
        Err(QuantError::NotImplemented("research job list".into()))
    }

    async fn get(&self, _job_id: &ResearchJobId) -> QuantResult<Option<ResearchJobView>> {
        Ok(None)
    }

    async fn cancel(
        &self,
        _job_id: &ResearchJobId,
        _reason: String,
        _ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        Err(QuantError::NotImplemented("research job cancel".into()))
    }

    async fn retry(
        &self,
        _job_id: &ResearchJobId,
        _reason: String,
        _ctx: JobSubmitContext,
    ) -> QuantResult<ResearchJobView> {
        Err(QuantError::NotImplemented("research job retry".into()))
    }
}

pub struct MockFeatureIntegrityPort;

#[async_trait]
impl FeatureIntegrityPort for MockFeatureIntegrityPort {
    async fn summary(&self) -> QuantResult<FeatureIntegritySummaryView> {
        Err(QuantError::NotImplemented(
            "feature integrity summary".into(),
        ))
    }

    async fn list_runs(
        &self,
        _query: FeatureParityRunListQuery,
    ) -> QuantResult<Paginated<FeatureParityRunView>> {
        Err(QuantError::NotImplemented("feature parity runs".into()))
    }

    async fn list_events(
        &self,
        query: FeatureParityEventListQuery,
    ) -> QuantResult<Paginated<FeatureParityEventView>> {
        Ok(Paginated::empty_for(&query))
    }

    async fn request_full_run(
        &self,
        _request: RunFullFeatureParityRequest,
        _ctx: FeatureIntegrityActionContext,
    ) -> QuantResult<ResearchJobView> {
        Err(QuantError::NotImplemented("feature parity full run".into()))
    }

    async fn acknowledge_latch(
        &self,
        _request: AcknowledgeFeatureParityLatchRequest,
        _ctx: FeatureIntegrityActionContext,
    ) -> QuantResult<FeatureIntegrityLatchView> {
        Err(QuantError::NotImplemented(
            "feature parity acknowledge".into(),
        ))
    }
}

pub struct MockCalibrationArtifactFitPort;

#[async_trait]
impl CalibrationArtifactFitPort for MockCalibrationArtifactFitPort {
    async fn fit(
        &self,
        _params: BiasTableFitJobParams,
        _progress: Arc<dyn JobProgressSink>,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> QuantResult<BiasTableFitOutcome> {
        Err(QuantError::NotImplemented("bias table fit".into()))
    }

    async fn find(
        &self,
        _artifact_id: &CalibrationArtifactId,
    ) -> QuantResult<Option<CalibrationArtifactInfo>> {
        Ok(None)
    }

    async fn page(
        &self,
        query: CalibrationArtifactListQuery,
    ) -> QuantResult<Paginated<CalibrationArtifactInfo>> {
        Ok(Paginated::empty_for(&query))
    }

    async fn mark_active(
        &self,
        _artifact_id: &CalibrationArtifactId,
    ) -> QuantResult<CalibrationArtifactInfo> {
        Err(QuantError::NotImplemented(
            "mark calibration artifact active".into(),
        ))
    }
}

pub struct MockModelCalibrationFitPort;

#[async_trait]
impl ModelCalibrationFitPort for MockModelCalibrationFitPort {
    async fn fit(
        &self,
        _params: ModelCalibrationFitJobParams,
        _progress: Arc<dyn JobProgressSink>,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> QuantResult<ModelCalibrationFitOutcome> {
        Err(QuantError::NotImplemented("model calibrator fit".into()))
    }

    async fn preflight(
        &self,
        _model_version_id: &ModelVersionId,
        _calibration_dataset_id: &TrainingDatasetId,
    ) -> QuantResult<ModelCalibrationFitPreflightView> {
        Err(QuantError::NotImplemented(
            "model calibrator fit preflight".into(),
        ))
    }
}

pub struct MockStructuralMonitorPort;

#[async_trait]
impl StructuralMonitorPort for MockStructuralMonitorPort {
    async fn negrisk_events(&self) -> QuantResult<Vec<NegRiskEventDriftView>> {
        Ok(Vec::new())
    }

    async fn trade_tape_coverage(&self) -> QuantResult<TradeTapeCoverageView> {
        let now = chrono::Utc::now();
        Ok(TradeTapeCoverageView {
            decision_at: now,
            knowledge_cutoff: now,
            window_secs: 86_400,
            knowledge_lag_secs: 60,
            active_market_count: 0,
            token_cursor_count: 0,
            market_cursor_count: 0,
            covered_market_ratio: Decimal::ZERO,
            source_health: vec![TradeTapeSourceHealthView {
                source: "on_chain".to_owned(),
                enabled: true,
                token_cursor_count: 0,
                bootstrap_count: 0,
                catching_up_count: 0,
                live_count: 0,
                empty_count: 0,
                error_count: 0,
                worst_lag_blocks: None,
                last_updated_at: None,
            }],
            missing_reason_breakdown: vec![MissingReasonCountView {
                reason: "trade_tape_unavailable".to_owned(),
                count: 0,
            }],
        })
    }

    async fn participant_concentration(&self) -> QuantResult<ParticipantConcentrationSummaryView> {
        let now = chrono::Utc::now();
        Ok(ParticipantConcentrationSummaryView {
            decision_at: now,
            knowledge_cutoff: now,
            window_secs: 86_400,
            knowledge_lag_secs: 60,
            min_unique_participants: 5,
            min_notional_usd: Decimal::ZERO,
            min_coverage_ratio: Decimal::ZERO,
            markets: Vec::new(),
            missing_reason_breakdown: Vec::new(),
        })
    }

    async fn participant_concentration_market(
        &self,
        _market_id: &MarketId,
    ) -> QuantResult<Option<ParticipantConcentrationDetailView>> {
        Ok(None)
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

#[derive(Default)]
pub struct MockMarketLinkageRepository;

#[async_trait]
impl MarketLinkageRepository for MockMarketLinkageRepository {
    async fn append(&self, _linkage: NewMarketLinkage) -> Result<MarketLinkageInfo, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("quant_market_linkage"),
            detail: "mock".to_owned(),
        })
    }

    async fn valid_at(
        &self,
        _market_id: &MarketId,
        _boundary: &DecisionBoundary,
    ) -> Result<Option<MarketLinkageInfo>, StorageError> {
        Ok(None)
    }

    async fn valid_at_for_markets(
        &self,
        _market_ids: &[MarketId],
        _boundary: &DecisionBoundary,
    ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn latest_for_markets(
        &self,
        _market_ids: &[MarketId],
    ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn latest_for_active_markets(&self) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn ledger_for_markets(
        &self,
        _market_ids: &[MarketId],
        _end_boundary: &DecisionBoundary,
    ) -> Result<Vec<MarketLinkageInfo>, StorageError> {
        Ok(Vec::new())
    }

    async fn find_by_id(
        &self,
        _linkage_id: &MarketLinkageId,
    ) -> Result<Option<MarketLinkageInfo>, StorageError> {
        Ok(None)
    }

    async fn page(
        &self,
        query: MarketLinkageListQuery,
    ) -> Result<Paginated<MarketLinkageInfo>, StorageError> {
        Ok(Paginated::empty_for(&query))
    }
}

#[derive(Default)]
pub struct MockMarketLinkageGovernancePort;

#[async_trait]
impl MarketLinkageGovernancePort for MockMarketLinkageGovernancePort {
    async fn resolve_changed_markets(
        &self,
        _market_ids: &[MarketId],
    ) -> QuantResult<LinkageResolveSummaryView> {
        Ok(LinkageResolveSummaryView {
            examined: 0,
            appended: 0,
            unchanged: 0,
            resolved: 0,
            unresolved: 0,
        })
    }

    async fn apply_override(
        &self,
        _market_id: &MarketId,
        _request: OverrideLinkageRequest,
        _actor: String,
    ) -> QuantResult<MarketLinkageInfo> {
        Err(QuantError::from(StorageError::InvariantViolation {
            entity: Some("quant_market_linkage"),
            detail: "mock".to_owned(),
        }))
    }
}

#[derive(Default)]
pub struct MockDomainSourceCursorRepository;

#[async_trait]
impl DomainSourceCursorRepository for MockDomainSourceCursorRepository {
    async fn find(
        &self,
        _source_id: &DomainSourceId,
        _instrument_key: &DomainInstrumentKey,
    ) -> Result<Option<DomainSourceCursorInfo>, StorageError> {
        Ok(None)
    }

    async fn upsert(
        &self,
        _cursor: UpsertDomainSourceCursor,
    ) -> Result<DomainSourceCursorInfo, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("quant_domain_source_cursor"),
            detail: "mock".to_owned(),
        })
    }

    async fn list_all(&self) -> Result<Vec<DomainSourceCursorInfo>, StorageError> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
pub struct MockBasisAlertRepository;

#[async_trait]
impl BasisAlertRepository for MockBasisAlertRepository {
    async fn record(&self, _alert: NewBasisAlert) -> Result<BasisAlertInfo, StorageError> {
        Err(StorageError::InvariantViolation {
            entity: Some("quant_basis_alert"),
            detail: "mock".to_owned(),
        })
    }

    async fn latest_for_market(
        &self,
        _market_id: &MarketId,
    ) -> Result<Option<BasisAlertInfo>, StorageError> {
        Ok(None)
    }

    async fn page(
        &self,
        query: BasisAlertListQuery,
    ) -> Result<Paginated<BasisAlertInfo>, StorageError> {
        Ok(Paginated::empty_for(&query))
    }

    async fn acknowledge(
        &self,
        alert_id: &BasisAlertId,
        _actor: String,
    ) -> Result<BasisAlertInfo, StorageError> {
        Err(StorageError::NotFound {
            entity: "quant_basis_alert",
            id: alert_id.to_string(),
        })
    }
}
