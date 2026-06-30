//! Web admin surface assembly for Phase 0.

use super::AppContext;
use crate::{
    app::{
        account_read::CoreAccountReadPort, backtest::CoreBacktestPort,
        execution_read::CoreExecutionReadPort, model_training::CoreModelTrainingPort,
        quant_report::CoreQuantReportPort, task_id::TaskId, task_registry::AppRunner,
        training_dataset::CoreTrainingDatasetPort,
    },
    pipeline::book_store::BookStore,
};
use async_trait::async_trait;
use quant_pivot_api::ws::{ClobWsManager, SubscriptionSource};
use quant_pivot_error::{QuantResult, control::ControlError, infra::InfraError};
use quant_pivot_models::{
    domain::{
        BookSnapshot, CatalogStatusPort, DataQualityPort, MarketDataPort, MetricsScrapePort,
        NewOperationLog, OrderIntentPort, RuntimeConfigPort,
    },
    types::TokenId,
};
use quant_pivot_repository::{
    pg_arc_repo,
    postgres::{
        PgAccountSnapshotRepository, PgAttributionRepository, PgEquitySnapshotRepository,
        PgExecutionOrderRepository, PgMarketRepository, PgMenuRepository, PgOperationLogRepository,
        PgPositionRepository, PgRecommendationReportRepository, PgRecommendationRepository,
        PgRoleMenuRepository, PgRolePermissionRepository, PgRoleRepository,
        PgRuntimeConfigVersionRepository, PgUserRepository, PgUserRoleRepository,
    },
    traits::{
        AccountSnapshotRepository, AttributionRepository, EquitySnapshotRepository,
        ExecutionOrderRepository, OperationLogRepository, PositionRepository,
        RecommendationReportRepository, RecommendationRepository,
    },
};
use quant_pivot_storage::write::{
    AsyncWriter, AsyncWriterConfig, AsyncWriterObservability, AsyncWriterWorker,
};
use quant_pivot_web::{
    AppState,
    audit::OperationLogBuffer,
    auth::casbin::CasbinService,
    jwt::{JwtService, TokenBlacklist},
    readiness::PgRedisReadiness,
    routes, spawn_web_server,
    ws::{SessionRegistry, spawn_ws_broadcaster},
};
use std::{collections::HashSet, sync::Arc, time::Duration};

const OPERATION_LOG_BATCH_SIZE: usize = 64;
const OPERATION_LOG_FLUSH_INTERVAL: Duration = Duration::from_millis(250);
const OPERATION_LOG_BUFFER_CAPACITY: usize = 4096;

impl AppContext {
    pub async fn register_web_services(
        &self,
        runner: &mut AppRunner,
        order_intents: Arc<dyn OrderIntentPort>,
    ) -> QuantResult<()> {
        let (operation_log, op_log_worker) = build_operation_log_writer(self);
        let state = build_app_state(self, order_intents, operation_log).await?;

        let event_rx = self
            .event_rx
            .lock()
            .take()
            .ok_or_else(|| InfraError::Misconfigured {
                detail: "event_rx already taken".into(),
            })?;
        let ws_sessions = state.ws_sessions.clone();

        let web_config = self.config.web.clone();
        let shutdown = self.shutdown.clone();
        runner.spawn(TaskId::WebServer, move |token| async move {
            if let Err(error) = spawn_web_server(state, web_config, token).await {
                tracing::error!(%error, "web server exited");
            }
            shutdown.cancel();
        });

        runner.spawn(TaskId::WsBroadcaster, move |token| async move {
            spawn_ws_broadcaster(event_rx, ws_sessions, token).await;
        });

        runner.spawn(TaskId::OperationLogWriter, move |token| {
            op_log_worker.run(token)
        });

        Ok(())
    }
}

async fn build_app_state(
    ctx: &AppContext,
    order_intents: Arc<dyn OrderIntentPort>,
    operation_log: OperationLogBuffer,
) -> QuantResult<AppState> {
    let pg = ctx.infra.pg.connection();
    let perm_checker = Arc::new(routes::init_rbac_rules());
    let casbin = Arc::new(CasbinService::new(pg.clone()).await.map_err(|error| {
        InfraError::Misconfigured {
            detail: error.to_string(),
        }
    })?);
    let jwt = Arc::new(JwtService::new(
        &ctx.config.web.jwt,
        Arc::clone(&ctx.infra.jwt_blacklist) as Arc<dyn TokenBlacklist>,
    ));

    Ok(AppState {
        deploy: Arc::clone(&ctx.config),
        runtime_config_apply: Arc::clone(&ctx.governance.applicator) as Arc<dyn RuntimeConfigPort>,
        jwt,
        jwt_blacklist: Arc::clone(&ctx.infra.jwt_blacklist),
        users: pg_arc_repo!(pg, PgUserRepository),
        roles: pg_arc_repo!(pg, PgRoleRepository),
        menus: pg_arc_repo!(pg, PgMenuRepository),
        user_roles: pg_arc_repo!(pg, PgUserRoleRepository),
        role_menus: pg_arc_repo!(pg, PgRoleMenuRepository),
        role_permissions: pg_arc_repo!(pg, PgRolePermissionRepository),
        casbin,
        perm_checker,
        runtime_config: pg_arc_repo!(pg, PgRuntimeConfigVersionRepository),
        operation_logs: pg_arc_repo!(pg, PgOperationLogRepository),
        operation_log,
        control: Arc::clone(&ctx.governance.runtime_control),
        kill_switch: Arc::clone(&ctx.governance.kill_switch),
        market_data: Arc::new(CoreMarketData {
            book_store: Arc::clone(&ctx.data.book_store),
            ws_manager: Arc::clone(&ctx.data.ws_manager),
        }),
        catalog: Arc::clone(&ctx.data.catalog) as Arc<dyn CatalogStatusPort>,
        data_quality: Arc::clone(&ctx.data.data_quality) as Arc<dyn DataQualityPort>,
        events: ctx.events.clone(),
        markets: pg_arc_repo!(pg, PgMarketRepository),
        ws_sessions: SessionRegistry::default(),
        metrics: Arc::new(CoreMetricsScrape {
            registry: ctx.infra.metrics.registry.clone(),
        }),
        readiness: Arc::new(PgRedisReadiness::new(
            pg.clone(),
            Arc::clone(&ctx.infra.jwt_blacklist) as Arc<dyn TokenBlacklist>,
            Some(Arc::clone(&ctx.data.catalog) as Arc<dyn CatalogStatusPort>),
        )),
        training_datasets: Arc::new(CoreTrainingDatasetPort::from_research(
            &ctx.research,
            pg_arc_repo!(pg, PgRuntimeConfigVersionRepository),
        )),
        model_training: Arc::new(CoreModelTrainingPort::from_research(
            &ctx.research,
            pg_arc_repo!(pg, PgRuntimeConfigVersionRepository),
        )),
        backtests: Arc::new(CoreBacktestPort::from_research(
            &ctx.research,
            pg_arc_repo!(pg, PgRuntimeConfigVersionRepository),
        )),
        model_governance: Arc::clone(&ctx.research.model_governance),
        factor_governance: Arc::clone(&ctx.research.factor_governance),
        quant_reports: Arc::new(CoreQuantReportPort::new(
            Arc::new(PgRecommendationReportRepository::new(pg.clone()))
                as Arc<dyn RecommendationReportRepository>,
            Arc::new(PgRecommendationRepository::new(pg.clone()))
                as Arc<dyn RecommendationRepository>,
            Arc::clone(&ctx.report.lifecycle),
            Arc::clone(&ctx.report.scheduler),
        )),
        order_intents,
        account_read: Arc::new(CoreAccountReadPort::new(
            Arc::new(PgAccountSnapshotRepository::new(pg.clone()))
                as Arc<dyn AccountSnapshotRepository>,
            Arc::new(PgEquitySnapshotRepository::new(pg.clone()))
                as Arc<dyn EquitySnapshotRepository>,
            Arc::clone(&ctx.account.provider_factory),
            Arc::clone(&ctx.governance.applicator) as Arc<dyn RuntimeConfigPort>,
        )),
        execution_read: Arc::new(CoreExecutionReadPort::new(
            Arc::new(PgExecutionOrderRepository::new(pg.clone()))
                as Arc<dyn ExecutionOrderRepository>,
            Arc::new(PgPositionRepository::new(pg.clone())) as Arc<dyn PositionRepository>,
            Arc::new(PgAttributionRepository::new(pg.clone())) as Arc<dyn AttributionRepository>,
        )),
        execution_submit: ctx.execution_dispatcher(),
    })
}

/// Wire the best-effort Postgres operation-log `AsyncWriter`.
fn build_operation_log_writer(
    ctx: &AppContext,
) -> (OperationLogBuffer, AsyncWriterWorker<NewOperationLog>) {
    let op_log_repo = Arc::clone(&ctx.infra.operation_log_repo);
    let op_log_drops = ctx
        .infra
        .metrics
        .async_writer_dropped
        .with_label_values(&["operation_log"]);
    let (op_log_writer, op_log_worker) = AsyncWriter::new(
        AsyncWriterConfig::new("operation_log")
            .capacity(OPERATION_LOG_BUFFER_CAPACITY)
            .batch_size(OPERATION_LOG_BATCH_SIZE)
            .flush_interval(OPERATION_LOG_FLUSH_INTERVAL),
        move |rows: Vec<NewOperationLog>| {
            let repo = Arc::clone(&op_log_repo);
            Box::pin(async move { repo.append_batch(rows).await })
        },
        op_log_drops,
        AsyncWriterObservability {
            queue_depth: Some(
                ctx.infra
                    .metrics
                    .async_writer_queue_depth
                    .with_label_values(&["operation_log"]),
            ),
            flush_failed: Some(
                ctx.infra
                    .metrics
                    .async_writer_flush_failed
                    .with_label_values(&["operation_log"]),
            ),
        },
    );
    (
        OperationLogBuffer::new(Arc::new(op_log_writer)),
        op_log_worker,
    )
}

struct CoreMetricsScrape {
    registry: prometheus::Registry,
}

impl MetricsScrapePort for CoreMetricsScrape {
    fn gather_prometheus(&self) -> String {
        use prometheus::Encoder;
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        let encoder = prometheus::TextEncoder::new();
        let _ = encoder.encode(&metric_families, &mut buffer);
        String::from_utf8_lossy(&buffer).into_owned()
    }
}

struct CoreMarketData {
    book_store: Arc<BookStore>,
    ws_manager: Arc<ClobWsManager>,
}

#[async_trait]
impl MarketDataPort for CoreMarketData {
    fn book(
        &self,
        yes_token: &TokenId,
        no_token: &TokenId,
    ) -> (Option<Arc<BookSnapshot>>, Option<Arc<BookSnapshot>>) {
        (
            self.book_store.load(yes_token),
            self.book_store.load(no_token),
        )
    }

    fn subscribed_tokens(&self, token_ids: &[TokenId]) -> HashSet<TokenId> {
        self.ws_manager.subscribed_tokens(token_ids)
    }

    async fn subscribe(&self, token_ids: Vec<TokenId>) -> Result<(), ControlError> {
        self.ws_manager
            .subscribe_tokens(SubscriptionSource::Web, &token_ids);
        Ok(())
    }

    async fn unsubscribe(&self, token_ids: Vec<TokenId>) -> Result<(), ControlError> {
        self.ws_manager
            .unsubscribe_tokens(SubscriptionSource::Web, &token_ids);
        Ok(())
    }
}
