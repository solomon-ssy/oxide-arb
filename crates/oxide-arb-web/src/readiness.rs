//! `PostgreSQL` + Redis readiness probes for `GET /ready`.
//!
//! The web tier requires both stores before it can authenticate requests (JWT
//! blacklist) or serve RBAC-backed handlers, so readiness fails closed when
//! either dependency is unreachable.

use std::sync::Arc;

use async_trait::async_trait;
use oxide_arb_models::domain::{DependencyCheck, ReadinessPort, ReadinessReport};
use sea_orm::{ConnectionTrait, DatabaseConnection, Statement};
use tracing::warn;

use crate::jwt::TokenBlacklist;

/// Readiness probe over the web tier's mandatory dependencies.
pub struct PgRedisReadiness {
    db: DatabaseConnection,
    blacklist: Arc<dyn TokenBlacklist>,
}

impl PgRedisReadiness {
    /// Build a probe over the shared Postgres connection and JWT blacklist store.
    #[must_use]
    pub fn new(db: DatabaseConnection, blacklist: Arc<dyn TokenBlacklist>) -> Self {
        Self { db, blacklist }
    }

    async fn check_postgres(&self) -> DependencyCheck {
        let start = std::time::Instant::now();
        let result = self
            .db
            .execute(Statement::from_string(
                self.db.get_database_backend(),
                "SELECT 1".to_owned(),
            ))
            .await;
        match result {
            Ok(_) => DependencyCheck {
                name: "postgresql",
                ok: true,
                detail: None,
            },
            Err(error) => {
                warn!(%error, elapsed_ms = start.elapsed().as_millis(), "postgres readiness failed");
                DependencyCheck {
                    name: "postgresql",
                    ok: false,
                    detail: Some(error.to_string()),
                }
            }
        }
    }

    async fn check_redis(&self) -> DependencyCheck {
        match self.blacklist.health_check().await {
            Ok(()) => DependencyCheck {
                name: "redis",
                ok: true,
                detail: None,
            },
            Err(error) => {
                warn!(%error, "redis readiness failed");
                DependencyCheck {
                    name: "redis",
                    ok: false,
                    detail: Some(error.to_string()),
                }
            }
        }
    }
}

#[async_trait]
impl ReadinessPort for PgRedisReadiness {
    async fn check(&self) -> ReadinessReport {
        let (postgres, redis) = tokio::join!(self.check_postgres(), self.check_redis());
        let checks = vec![postgres, redis];
        let ready = checks.iter().all(|check| check.ok);
        ReadinessReport { ready, checks }
    }
}
