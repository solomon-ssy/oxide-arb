//! `PostgreSQL` + Redis readiness probes for `GET /ready`.
//!
//! The web tier requires both stores before it can authenticate requests (JWT
//! blacklist) or serve RBAC-backed handlers, so readiness fails closed when
//! either dependency is unreachable.

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use quant_pivot_models::domain::{
    CatalogState, CatalogStatusPort, DependencyCheck, ReadinessPort, ReadinessReport,
};
use sea_orm::{ConnectionTrait, DatabaseConnection, Statement};
use tracing::warn;

use crate::jwt::TokenBlacklist;

/// Readiness probe over the web tier's mandatory dependencies.
///
/// Postgres and Redis gate `ready` (the web tier cannot authenticate without
/// them). The market-catalog check is **informational only**: the control
/// plane must stay routable during catalog warmup, so a `Warming` catalog is
/// reported in `checks` but never flips `ready` to false.
pub struct PgRedisReadiness {
    db: DatabaseConnection,
    blacklist: Arc<dyn TokenBlacklist>,
    catalog: Option<Arc<dyn CatalogStatusPort>>,
}

impl PgRedisReadiness {
    /// Build a probe over the shared Postgres connection, the JWT blacklist
    /// store, and (optionally) the market-catalog warmup gate.
    #[must_use]
    pub fn new(
        db: DatabaseConnection,
        blacklist: Arc<dyn TokenBlacklist>,
        catalog: Option<Arc<dyn CatalogStatusPort>>,
    ) -> Self {
        Self {
            db,
            blacklist,
            catalog,
        }
    }

    async fn check_postgres(&self) -> DependencyCheck {
        let start = Instant::now();
        let result = self
            .db
            .execute(Statement::from_string(
                self.db.get_database_backend(),
                "SELECT 1".to_owned(),
            ))
            .await;
        match result {
            Ok(_) => DependencyCheck {
                name: "postgresql".to_owned(),
                ok: true,
                detail: None,
            },
            Err(error) => {
                warn!(%error, elapsed_ms = start.elapsed().as_millis(), "postgres readiness failed");
                DependencyCheck {
                    name: "postgresql".to_owned(),
                    ok: false,
                    detail: Some(error.to_string()),
                }
            }
        }
    }

    async fn check_redis(&self) -> DependencyCheck {
        match self.blacklist.health_check().await {
            Ok(()) => DependencyCheck {
                name: "redis".to_owned(),
                ok: true,
                detail: None,
            },
            Err(error) => {
                warn!(%error, "redis readiness failed");
                DependencyCheck {
                    name: "redis".to_owned(),
                    ok: false,
                    detail: Some(error.to_string()),
                }
            }
        }
    }

    /// Informational catalog check (never gates `ready`).
    fn check_catalog(&self) -> Option<DependencyCheck> {
        let catalog = self.catalog.as_ref()?;
        Some(match catalog.catalog_state() {
            CatalogState::Ready { markets, synced_at } => DependencyCheck {
                name: "catalog".to_owned(),
                ok: true,
                detail: Some(format!("{markets} markets, synced at {synced_at}")),
            },
            CatalogState::Warming => DependencyCheck {
                name: "catalog".to_owned(),
                ok: false,
                detail: Some("warming — first Gamma catalog sync pending".to_owned()),
            },
        })
    }
}

#[async_trait]
impl ReadinessPort for PgRedisReadiness {
    async fn check(&self) -> ReadinessReport {
        let (postgres, redis) = tokio::join!(self.check_postgres(), self.check_redis());
        let mut checks = vec![postgres, redis];
        // `ready` is computed over the required dependencies only — the
        // catalog check below is informational (warmup must not stop traffic).
        let ready = checks.iter().all(|check| check.ok);
        if let Some(catalog) = self.check_catalog() {
            checks.push(catalog);
        }
        ReadinessReport { ready, checks }
    }
}
