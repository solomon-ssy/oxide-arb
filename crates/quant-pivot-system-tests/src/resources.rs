//! Per-scenario resource identities on the shared system-test stack.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::Result;
use quant_pivot_models::config::{ClickHouseConfig, PostgresConfig, RedisConfig};

use crate::stack::SystemStack;

tokio::task_local! {
    static ACTIVE_SUITE: Arc<ResourceSuite>;
}

struct ResourceSuite {
    stack: SystemStack,
    next_identity: AtomicU64,
}

impl ResourceSuite {
    fn identity(&self, scope: &str) -> String {
        let sequence = self.next_identity.fetch_add(1, Ordering::Relaxed);
        format!("{scope}_{sequence:03}")
    }

    fn postgres_config(&self, scope: &str) -> PostgresConfig {
        let mut config = self.stack.postgres_config.clone();
        config.database = format!("quant_pivot_{}", self.identity(scope));
        "quant-pivot-infrastructure-system-tests".clone_into(&mut config.application_name);
        config
    }

    fn clickhouse_config(&self, scope: &str) -> ClickHouseConfig {
        let mut config = self.stack.clickhouse_config.clone();
        config.database = format!("quant_pivot_{}", self.identity(scope));
        "infrastructure-system-tests".clone_into(&mut config.deployment_id);
        config
    }

    fn redis_config(&self, scope: &str) -> RedisConfig {
        let mut config = self.stack.redis_config.clone();
        config.key_prefix = format!("quant-pivot:system:{}:", self.identity(scope));
        config
    }
}

/// Run infrastructure scenarios against one disposable three-service stack.
pub async fn with_resource_suite<F>(future: F) -> Result<F::Output>
where
    F: Future,
{
    let suite = Arc::new(ResourceSuite {
        stack: Box::pin(SystemStack::start()).await?,
        next_identity: AtomicU64::new(1),
    });
    Ok(ACTIVE_SUITE.scope(suite, future).await)
}

/// Allocate a unique, initially absent `PostgreSQL` database identity.
#[must_use]
pub fn fresh_postgres_config(scope: &str) -> PostgresConfig {
    ACTIVE_SUITE
        .try_with(|suite| suite.postgres_config(scope))
        .expect("PostgreSQL scenario must run inside with_resource_suite")
}

/// Allocate a unique, initially absent `ClickHouse` database identity.
#[must_use]
pub fn fresh_clickhouse_config(scope: &str) -> ClickHouseConfig {
    ACTIVE_SUITE
        .try_with(|suite| suite.clickhouse_config(scope))
        .expect("ClickHouse scenario must run inside with_resource_suite")
}

/// Allocate a unique Redis key namespace on the shared server.
#[must_use]
pub fn fresh_redis_config(scope: &str) -> RedisConfig {
    ACTIVE_SUITE
        .try_with(|suite| suite.redis_config(scope))
        .expect("Redis scenario must run inside with_resource_suite")
}
