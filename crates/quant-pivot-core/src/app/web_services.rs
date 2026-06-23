//! Web admin surface assembly for Phase 0.

use super::AppContext;
use crate::{
    app::{task_id::TaskId, task_registry::AppRunner},
    pipeline::book_store::BookStore,
};
use async_trait::async_trait;
use quant_pivot_api::ws::SubscriptionSource;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        BookSnapshot, CatalogStatusPort, DataQualityPort, MarketDataPort, MetricsScrapePort,
        NewOperationLog, RuntimeConfigPort, RuntimeControlError,
    },
    types::TokenId,
};
use quant_pivot_repository::{
    pg_arc_repo,
    postgres::{
        PgMarketRepository, PgMenuRepository, PgOperationLogRepository, PgRoleMenuRepository,
        PgRolePermissionRepository, PgRoleRepository, PgRuntimeConfigVersionRepository,
        PgUserRepository, PgUserRoleRepository,
    },
    traits::OperationLogRepository,
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
    pub async fn register_web_services(&self, runner: &mut AppRunner) -> QuantResult<()> {
        let pg = self.infra.pg.connection();
        let perm_checker = Arc::new(routes::init_rbac_rules());
        let casbin = Arc::new(
            CasbinService::new(pg.clone())
                .await
                .map_err(|error| QuantError::Internal(error.to_string()))?,
        );
        let jwt = Arc::new(JwtService::new(
            &self.config.web.jwt,
            Arc::clone(&self.infra.jwt_blacklist) as Arc<dyn TokenBlacklist>,
        ));
        let (operation_log, op_log_worker) = build_operation_log_writer(self);

        let event_rx = self
            .event_rx
            .lock()
            .take()
            .ok_or_else(|| QuantError::Internal("event_rx already taken".into()))?;

        let ws_sessions = SessionRegistry::default();
        let ws_registry = ws_sessions.clone();

        let state = AppState {
            deploy: Arc::clone(&self.config),
            runtime_config_apply: Arc::clone(&self.governance.applicator)
                as Arc<dyn RuntimeConfigPort>,
            jwt,
            jwt_blacklist: Arc::clone(&self.infra.jwt_blacklist),
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
            control: Arc::clone(&self.governance.runtime_control),
            market_data: Arc::new(CoreMarketData {
                book_store: Arc::clone(&self.data.book_store),
                ws_manager: Arc::clone(&self.data.ws_manager),
            }),
            catalog: Arc::clone(&self.data.catalog) as Arc<dyn CatalogStatusPort>,
            data_quality: Arc::clone(&self.data.data_quality) as Arc<dyn DataQualityPort>,
            events: self.events.clone(),
            markets: pg_arc_repo!(pg, PgMarketRepository),
            ws_sessions,
            metrics: Arc::new(CoreMetricsScrape {
                registry: self.infra.metrics.registry.clone(),
            }),
            readiness: Arc::new(PgRedisReadiness::new(
                pg.clone(),
                Arc::clone(&self.infra.jwt_blacklist) as Arc<dyn TokenBlacklist>,
                Some(Arc::clone(&self.data.catalog) as Arc<dyn CatalogStatusPort>),
            )),
        };

        let web_config = self.config.web.clone();
        let shutdown = self.shutdown.clone();
        runner.spawn(TaskId::WebServer, move |token| async move {
            if let Err(error) = spawn_web_server(state, web_config, token).await {
                tracing::error!(%error, "web server exited");
            }
            shutdown.cancel();
        });

        runner.spawn(TaskId::WsBroadcaster, move |token| async move {
            spawn_ws_broadcaster(event_rx, ws_registry, token).await;
        });

        runner.spawn(TaskId::OperationLogWriter, move |token| {
            op_log_worker.run(token)
        });

        Ok(())
    }
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
    ws_manager: Arc<quant_pivot_api::ws::ClobWsManager>,
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

    async fn subscribe(&self, token_ids: Vec<TokenId>) -> Result<(), RuntimeControlError> {
        self.ws_manager
            .subscribe_tokens(SubscriptionSource::Web, &token_ids);
        Ok(())
    }

    async fn unsubscribe(&self, token_ids: Vec<TokenId>) -> Result<(), RuntimeControlError> {
        self.ws_manager
            .unsubscribe_tokens(SubscriptionSource::Web, &token_ids);
        Ok(())
    }
}
