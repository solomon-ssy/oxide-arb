//! Shared helpers for `PostgreSQL` repository integration tests.

use chrono::Utc;
use oxide_arb_models::{
    config::PostgresConfig,
    domain::{UpsertEvent, UpsertMarket},
    enums::{
        common::{MarketCategory, TickSize},
        market::{EventStatus, MarketStatus},
    },
    types::*,
};
use oxide_arb_storage::postgres::{
    PostgresPool,
    migration::{Migrator, MigratorTrait},
};
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

pub fn test_pg_config(port: u16) -> PostgresConfig {
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
        application_name: "oxide-arb-test".into(),
    }
}

pub async fn setup_pg() -> (PostgresPool, ContainerAsync<Postgres>) {
    let container = Postgres::default()
        .with_db_name("test_oxide_arb")
        .with_user("postgres")
        .with_password("postgres")
        .start()
        .await
        .expect("PG container");

    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let config = test_pg_config(port);
    let pool = PostgresPool::connect(&config).await.expect("connect");
    Migrator::up(pool.connection(), None)
        .await
        .expect("migrate");

    (pool, container)
}

pub fn make_event(id: &str, title: &str, slug: &str, category: MarketCategory) -> UpsertEvent {
    UpsertEvent {
        event_id: EventId::new(id),
        title: title.into(),
        slug: slug.into(),
        category,
        status: EventStatus::Active,
        neg_risk: false,
        end_date: None,
        raw_gamma: None,
    }
}

pub fn make_market(
    market_id: &str,
    event_id: &str,
    question: &str,
    slug: &str,
    category: MarketCategory,
    end_date: Option<chrono::DateTime<Utc>>,
) -> UpsertMarket {
    UpsertMarket {
        market_id: MarketId::new(market_id),
        event_id: EventId::new(event_id),
        question: question.into(),
        slug: slug.into(),
        category,
        status: MarketStatus::Active,
        outcome: None,
        yes_token_id: TokenId::new("12345"),
        no_token_id: TokenId::new("67890"),
        tick_size: TickSize::Hundredth,
        neg_risk: false,
        end_date,
        resolved_at: None,
        fees_enabled: true,
        fee_rate: None,
        fee_exponent: None,
        fee_taker_only: None,
        fee_rebate_rate: None,
        fee_source: None,
        fee_observed_at: None,
    }
}
