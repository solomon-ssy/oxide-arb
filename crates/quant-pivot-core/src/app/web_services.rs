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
        BookSnapshot, CatalogStatusPort, MarketDataPort, MetricsScrapePort, RuntimeConfigPort,
        RuntimeControlError,
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
};
use quant_pivot_web::{
    AppState,
    audit::{OperationLogBuffer, spawn_operation_log_writer},
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
        let (operation_log, op_log_rx) = OperationLogBuffer::new(OPERATION_LOG_BUFFER_CAPACITY);
        let operation_log_repo = Arc::clone(&self.infra.operation_log_repo);

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
            control: Arc::clone(&self.runtime_control),
            market_data: Arc::new(CoreMarketData {
                book_store: Arc::clone(&self.data.book_store),
                ws_manager: Arc::clone(&self.data.ws_manager),
            }),
            catalog: Arc::clone(&self.catalog) as Arc<dyn CatalogStatusPort>,
            events: self.events.clone(),
            markets: pg_arc_repo!(pg, PgMarketRepository),
            ws_sessions,
            metrics: Arc::new(CoreMetricsScrape {
                registry: self.infra.metrics.registry.clone(),
            }),
            readiness: Arc::new(PgRedisReadiness::new(
                pg.clone(),
                Arc::clone(&self.infra.jwt_blacklist) as Arc<dyn TokenBlacklist>,
                Some(Arc::clone(&self.catalog) as Arc<dyn CatalogStatusPort>),
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

        let op_repo = operation_log_repo;
        runner.spawn(TaskId::OperationLogWriter, move |token| async move {
            spawn_operation_log_writer(
                op_log_rx,
                op_repo,
                OPERATION_LOG_BATCH_SIZE,
                OPERATION_LOG_FLUSH_INTERVAL,
                token,
            )
            .await;
        });

        Ok(())
    }
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
