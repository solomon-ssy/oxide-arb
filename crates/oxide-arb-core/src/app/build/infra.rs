//! Bootstrap phase: pools, runtime-config seed, mode restore, and [`BuildInfra::connect`].
//!
//! **Owns:** Postgres/Redis/ClickHouse connection, repository bundle construction,
//! runtime-config activation seed, operational mode restore, persistence writers,
//! and control-factor wiring inputs.
//!
//! **Does not own:** trading stack assembly, applicator wiring, or final
//! [`BuildInfra::finalize`] — those live in [`super::assembly`].

use super::types::{
    BuildInfra, BuildInfraCoreParts, BuildInfraParts, BuildRepos, ControlFactorWiring,
    RISK_DECISION_AUDIT_CHANNEL_CAPACITY,
};
use crate::{
    bridge::risk_audit_sink::new_audit_sink,
    infra::persistence_writers::{
        PersistenceBackgroundWorkers, PersistenceBundle, PersistenceWireInput,
    },
    observability::{
        alert_dispatcher::AlertDispatcher, balance_fact_writer::BalanceFactWriter,
        metrics_hub::MetricsHub,
    },
    runtime_config::RuntimeConfigStore,
    service::catalog_readiness::CatalogReadiness,
};
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::{
    config::DeployConfig,
    domain::{NewRuntimeConfigActivation, NewRuntimeConfigVersion, runtime_config_hash},
    enums::{
        common::ExecutionMode,
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    runtime_config::{RUNTIME_CONFIG_SCHEMA_VERSION, RuntimeConfig},
    types::{RuntimeConfigActivationId, RuntimeConfigVersionId},
};
use oxide_arb_repository::{
    clickhouse::ChTimeseriesRepository,
    postgres::PgSystemRuntimeStateRepository,
    traits::{RuntimeConfigVersionRepository, SystemRuntimeStateRepository},
};
use oxide_arb_storage::{
    cache::{CacheManager, MokaBackend, RedisBackend, TieredCache, connect_pool},
    clickhouse::{ChWriteManager, ClickHousePool},
    postgres::{
        PostgresPool,
        migration::{Migrator, MigratorTrait},
    },
};
use oxide_arb_web::jwt::RedisTokenBlacklist;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

impl BuildInfra {
    /// Connect pools, migrate, seed runtime config, and wire infra-scoped services.
    pub(super) async fn connect(
        deploy: &DeployConfig,
        shutdown: CancellationToken,
    ) -> OxideResult<(Self, PersistenceBackgroundWorkers)> {
        let metrics = Arc::new(MetricsHub::new());

        let pg_pool = Arc::new(PostgresPool::connect(&deploy.db.postgres).await?);
        Migrator::up(pg_pool.connection(), None).await?;

        let repos = BuildRepos::from_pool(&pg_pool);
        let runtime =
            Self::ensure_runtime_config_activation(repos.runtime_config().as_ref()).await?;
        let alerts = Arc::new(AlertDispatcher::new(&runtime.notification));
        let runtime_store = Arc::new(RuntimeConfigStore::new(runtime));

        let (risk_decision_audit, risk_decision_audit_rx) =
            new_audit_sink(RISK_DECISION_AUDIT_CHANNEL_CAPACITY);

        let ch_pool = Arc::new(ClickHousePool::connect(&deploy.db.clickhouse).await?);
        ch_pool.ensure_schema().await?;

        let redis_pool = connect_pool(&deploy.cache.redis).await?;
        tracing::info!(
            endpoint = %deploy.cache.redis.endpoint(),
            prefix = %deploy.cache.redis.key_prefix,
            "Redis connected (shared pool: cache L2 + JWT revocation)"
        );

        let cache = Arc::new(CacheManager::new(
            TieredCache::new(
                MokaBackend::new(deploy.cache.moka.max_capacity),
                RedisBackend::new(redis_pool.clone(), &deploy.cache.redis.key_prefix),
            ),
            &deploy.cache,
        ));
        cache.register_metrics(&metrics.registry).map_err(|error| {
            OxideError::Internal(format!("cache metrics registration: {error}"))
        })?;

        let jwt_blacklist = Arc::new(RedisTokenBlacklist::new(
            redis_pool.clone(),
            &deploy.cache.redis.key_prefix,
        ));

        let catalog = Arc::new(CatalogReadiness::new());

        let write_manager = Arc::new(ChWriteManager::new(
            deploy.db.clickhouse.max_concurrent_inserts,
        ));
        let timeseries = Arc::new(ChTimeseriesRepository::new(
            ch_pool.client().clone(),
            &deploy.db.clickhouse,
            write_manager,
            shutdown.clone(),
        ));
        let balance_fact_writer = Arc::new(BalanceFactWriter::new(Arc::clone(repos.fact_data())));

        let execution_mode = Self::restore_execution_mode(&PgSystemRuntimeStateRepository::new(
            pg_pool.connection().clone(),
        ))
        .await?;

        let (factor_store, factor_refresher, factor_registry, shadow_writer, shadow_writer_task) =
            ControlFactorWiring::wire(&repos, &metrics, execution_mode)
                .await?
                .into_bootstrap_parts();

        let (persistence, persistence_workers) = PersistenceBundle::wire(PersistenceWireInput {
            metrics: Arc::clone(&metrics),
            shutdown,
            trade_repo: Arc::clone(repos.trade()),
            timeseries,
        });

        Ok((
            Self::assembled(BuildInfraParts {
                execution_mode,
                runtime_store,
                core: BuildInfraCoreParts {
                    pg_pool,
                    ch_pool,
                    redis_pool,
                    cache,
                    jwt_blacklist,
                    catalog,
                    metrics,
                    alerts,
                    risk_decision_audit,
                    risk_decision_audit_rx,
                    repos,
                    balance_fact_writer,
                    factor_store,
                    factor_refresher,
                    factor_registry,
                    shadow_writer_task,
                },
                persistence,
                shadow_writer,
            }),
            persistence_workers,
        ))
    }

    /// Restore the persisted operational execution mode (the single source of truth).
    async fn restore_execution_mode(
        repo: &PgSystemRuntimeStateRepository,
    ) -> OxideResult<ExecutionMode> {
        if let Some(state) = repo.load().await? {
            return Ok(state.execution_mode);
        }
        tracing::warn!("system_runtime_state singleton missing; re-seeding DryRun (fail-closed)");
        let mode = ExecutionMode::DryRun;
        repo.upsert_execution_mode(mode, "bootstrap", "fail-closed re-seed (row missing)")
            .await?;
        Ok(mode)
    }

    /// Seed / load the active runtime configuration from Postgres.
    async fn ensure_runtime_config_activation(
        repo: &dyn RuntimeConfigVersionRepository,
    ) -> OxideResult<RuntimeConfig> {
        let current = repo.load_current().await?;
        if let Some(version) = &current {
            match RuntimeConfig::from_json(&version.config_json) {
                Ok(config) => return Ok(config),
                Err(error) => {
                    tracing::warn!(
                        %error,
                        version_id = %version.runtime_config_version_id,
                        "active runtime config is not a valid schema_version document — \
                         reseeding defaults"
                    );
                }
            }
        }

        let config = RuntimeConfig::default();
        let config_json = config.to_json();
        let config_hash = runtime_config_hash(&config_json);
        let version = match repo.load_by_hash(&config_hash).await? {
            Some(version) => version,
            None => {
                repo.create_version(NewRuntimeConfigVersion {
                    runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
                    config_hash: config_hash.clone(),
                    schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
                    config_json,
                    source: RuntimeConfigVersionSource::Bootstrap,
                    created_by: "system".to_owned(),
                    reason: format!(
                        "bootstrap default runtime config (schema_version={RUNTIME_CONFIG_SCHEMA_VERSION})"
                    ),
                })
                .await?
            }
        };

        repo.activate_version(NewRuntimeConfigActivation {
            runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
            runtime_config_version_id: version.runtime_config_version_id.clone(),
            activated_at: chrono::Utc::now(),
            activated_by: "system".to_owned(),
            reason: "bootstrap runtime config activation".to_owned(),
            activation_kind: if current.is_some() {
                RuntimeConfigActivationKind::Promote
            } else {
                RuntimeConfigActivationKind::Initial
            },
            previous_runtime_config_version_id: current
                .map(|version| version.runtime_config_version_id),
            rollback_target_version_id: None,
            audit_event_id: None,
        })
        .await?;
        Ok(config)
    }
}
