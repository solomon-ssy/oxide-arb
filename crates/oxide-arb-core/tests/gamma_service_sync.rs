//! `GammaService` startup sync tests (wiremock Gamma + Docker Postgres/Redis).

use oxide_arb_api::{fees::FeeCalculator, gamma::GammaClient};
use oxide_arb_core::{
    observability::metrics_hub::MetricsHub,
    pipeline::{
        market_cache::MarketCache, market_registry::MarketRegistry,
        universe_filter::MarketUniverseFilter,
    },
    runtime_config::RuntimeConfigStore,
    service::gamma::{GammaService, GammaServiceDeps},
};
use oxide_arb_error::{OxideError, market::MarketError};
use oxide_arb_models::runtime_config::RuntimeConfig;
use oxide_arb_models::{
    config::{CacheConfig, GammaConfig, PostgresConfig, RedisConfig},
    types::TokenId,
};
use oxide_arb_repository::{
    pg_arc_repo,
    postgres::{PgEventRepository, PgMarketRepository},
};
use oxide_arb_storage::{
    cache::{CacheManager, MokaBackend, RedisBackend, TieredCache, connect_pool},
    postgres::{
        PostgresPool,
        migration::{Migrator, MigratorTrait},
    },
};
use std::sync::Arc;
use testcontainers::{ImageExt, runners::AsyncRunner};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

fn test_pg_config(port: u16) -> PostgresConfig {
    PostgresConfig {
        host: "localhost".into(),
        port,
        user: "postgres".into(),
        password: "postgres".into(),
        database: "test_oxide_arb".into(),
        schema: "public".into(),
        max_connections: 5,
        min_connections: 1,
        connect_timeout_secs: 10,
        idle_timeout_secs: 300,
        acquire_timeout_secs: 10,
        max_lifetime_secs: 1800,
        statement_timeout_ms: 30_000,
        idle_in_transaction_timeout_ms: 60_000,
        lock_timeout_ms: 5_000,
        work_mem: "16MB".into(),
        verify_session_params: false,
        statement_cache_capacity: 100,
        application_name: "oxide-arb-gamma-test".into(),
    }
}

fn test_redis_config(port: u16) -> RedisConfig {
    RedisConfig {
        host: "127.0.0.1".into(),
        port,
        pool_size: 5,
        timeout_ms: 5000,
        key_prefix: "gamma-test:".into(),
        ..RedisConfig::default()
    }
}

async fn mount_active_events_keyset_mock(server: &MockServer, body: serde_json::Value) {
    Mock::given(method("GET"))
        .and(path("/events/keyset"))
        .and(query_param("closed", "false"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

async fn build_gamma_service(server_uri: &str) -> (GammaService, Arc<MarketRegistry>) {
    let pg_container = testcontainers_modules::postgres::Postgres::default()
        .with_db_name("test_oxide_arb")
        .with_user("postgres")
        .with_password("postgres")
        .with_tag("16")
        .start()
        .await
        .expect("PG container");
    let pg_port = pg_container
        .get_host_port_ipv4(5432)
        .await
        .expect("PG port");
    let pg_pool = PostgresPool::connect(&test_pg_config(pg_port))
        .await
        .expect("PG connect");
    Migrator::up(pg_pool.connection(), None)
        .await
        .expect("migrate");

    let redis_container = testcontainers_modules::redis::Redis::default()
        .with_tag("7-alpine")
        .start()
        .await
        .expect("Redis container");
    let redis_port = redis_container
        .get_host_port_ipv4(6379)
        .await
        .expect("Redis port");

    let market_registry = Arc::new(MarketRegistry::new());
    let universe = Arc::new(MarketUniverseFilter::default());
    let market_cache = Arc::new(MarketCache::new(
        Arc::clone(&market_registry),
        Arc::clone(&universe),
    ));
    let metrics = Arc::new(MetricsHub::new());
    let fee_calculator = Arc::new(FeeCalculator::default());
    let redis_cfg = test_redis_config(redis_port);
    let redis_pool = connect_pool(&redis_cfg).await.expect("Redis connect");
    let cache = Arc::new(CacheManager::new(
        TieredCache::new(
            MokaBackend::new(100),
            RedisBackend::new(redis_pool, &redis_cfg.key_prefix),
        ),
        &CacheConfig::default(),
    ));

    let db = pg_pool.connection().clone();
    let service = GammaService::new(GammaServiceDeps {
        gamma_client: Arc::new(GammaClient::new(GammaConfig {
            base_url: server_uri.to_owned(),
            ..GammaConfig::default()
        })),
        market_registry: Arc::clone(&market_registry),
        market_cache,
        universe,
        fee_calculator,
        market_repo: pg_arc_repo!(db, PgMarketRepository),
        event_repo: pg_arc_repo!(db, PgEventRepository),
        cache,
        metrics,
        runtime_config: Arc::new(RuntimeConfigStore::new(RuntimeConfig::default())),
        ws_subscription: None,
        full_sync_interval_secs: 300,
    });

    (service, market_registry)
}

fn sample_gamma_events_payload() -> serde_json::Value {
    serde_json::json!({
        "events": [{
            "id": "evt-gamma-1",
            "title": "Gamma sync test event",
            "slug": "gamma-sync-test",
            "markets": [{
                "conditionId": "0xgamma_sync_market",
                "question": "Will gamma sync populate the registry?",
                "category": "sports",
                "active": true,
                "closed": false,
                "feesEnabled": true,
                "clobTokenIds": ["1001", "1002"],
                "outcomes": ["Yes", "No"]
            }]
        }]
    })
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn gamma_sync_populates_registry() {
    let server = MockServer::start().await;
    mount_active_events_keyset_mock(&server, sample_gamma_events_payload()).await;

    let (service, registry) = build_gamma_service(&server.uri()).await;

    service.sync().await.expect("gamma sync should succeed");
    assert!(
        registry.market_count() > 0,
        "registry must contain markets after startup sync"
    );
    assert!(
        registry.market_for_token(&TokenId::new("1001")).is_some(),
        "token → market routing must work after sync"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn gamma_sync_fails_on_api_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/events/keyset"))
        .and(query_param("closed", "false"))
        .respond_with(ResponseTemplate::new(503).set_body_string("gamma unavailable"))
        .mount(&server)
        .await;

    let (service, registry) = build_gamma_service(&server.uri()).await;

    let error = service
        .sync()
        .await
        .expect_err("gamma API failure must fail sync");
    assert_eq!(registry.market_count(), 0);
    assert!(
        error.to_string().contains("gamma"),
        "expected gamma API error, got {error}"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn gamma_sync_rejects_empty_catalog() {
    let server = MockServer::start().await;
    mount_active_events_keyset_mock(&server, serde_json::json!({ "events": [] })).await;

    let (service, registry) = build_gamma_service(&server.uri()).await;

    let error = service
        .sync()
        .await
        .expect_err("empty catalog must fail closed");
    assert_eq!(registry.market_count(), 0);
    assert!(
        matches!(error, OxideError::Market(MarketError::EmptyCatalog)),
        "expected EmptyCatalog, got {error}"
    );
}
