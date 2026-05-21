//! Shared helpers for `PostgreSQL` repository integration tests.

use chrono::Utc;
use oxide_arb_models::config::PostgresConfig;
use oxide_arb_models::entities::{event, market};
use oxide_arb_models::enums::common::MarketCategory;
use oxide_arb_models::enums::market::MarketStatus;
use oxide_arb_models::types::*;
use oxide_arb_storage::postgres::PostgresPool;
use oxide_arb_storage::postgres::migration::{Migrator, MigratorTrait};
use sea_orm::ActiveValue::Set;

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

pub async fn setup_pg() -> (
    PostgresPool,
    testcontainers::ContainerAsync<testcontainers_modules::postgres::Postgres>,
) {
    use testcontainers::runners::AsyncRunner;
    let container = testcontainers_modules::postgres::Postgres::default()
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

pub fn make_event(
    id: &str,
    title: &str,
    slug: &str,
    category: MarketCategory,
) -> event::ActiveModel {
    event::ActiveModel {
        event_id: Set(EventId::new(id)),
        title: Set(title.into()),
        slug: Set(slug.into()),
        category: Set(category),
        status: Set("active".into()),
        neg_risk: Set(false),
        end_date: Set(None),
        raw_gamma: Set(None),
        created_at: Set(Utc::now()),
        updated_at: Set(Utc::now()),
    }
}

pub fn make_market(
    market_id: &str,
    event_id: &str,
    question: &str,
    slug: &str,
    category: MarketCategory,
    end_date: Option<chrono::DateTime<Utc>>,
) -> market::ActiveModel {
    market::ActiveModel {
        market_id: Set(MarketId::new(market_id)),
        event_id: Set(EventId::new(event_id)),
        question: Set(question.into()),
        slug: Set(slug.into()),
        category: Set(category),
        status: Set(MarketStatus::Active),
        outcome: Set(None),
        yes_token_id: Set(TokenId::new("12345")),
        no_token_id: Set(TokenId::new("67890")),
        tick_size: Set("0.01".into()),
        neg_risk: Set(false),
        end_date: Set(end_date),
        resolved_at: Set(None),
        created_at: Set(Utc::now()),
        updated_at: Set(Utc::now()),
    }
}
