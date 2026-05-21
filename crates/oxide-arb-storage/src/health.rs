//! Combined storage health checking.

use crate::cache::RedisBackend;
use crate::clickhouse::ClickHousePool;
use crate::error::StorageError;
use crate::postgres::PostgresPool;
use tracing::warn;

pub struct StorageHealth<'a> {
    pg: &'a PostgresPool,
    ch: Option<&'a ClickHousePool>,
    redis: Option<&'a RedisBackend>,
}

impl<'a> StorageHealth<'a> {
    pub const fn new(
        pg: &'a PostgresPool,
        ch: Option<&'a ClickHousePool>,
        redis: Option<&'a RedisBackend>,
    ) -> Self {
        Self { pg, ch, redis }
    }

    pub async fn check_all(&self) -> Vec<(&'static str, Result<(), StorageError>)> {
        let mut results = Vec::new();

        let pg_result = self.pg.health_check().await;
        if let Err(ref e) = pg_result {
            warn!("PostgreSQL health check failed: {e}");
        }
        results.push(("postgresql", pg_result));

        if let Some(ch) = self.ch {
            let ch_result = ch.health_check().await;
            if let Err(ref e) = ch_result {
                warn!("ClickHouse health check failed: {e}");
            }
            results.push(("clickhouse", ch_result));
        }

        if let Some(redis) = self.redis {
            let redis_result = redis.health_check().await;
            if let Err(ref e) = redis_result {
                warn!("Redis health check failed: {e}");
            }
            results.push(("redis", redis_result));
        }

        results
    }

    pub async fn is_healthy(&self) -> bool {
        self.check_all().await.iter().all(|(_, r)| r.is_ok())
    }
}
