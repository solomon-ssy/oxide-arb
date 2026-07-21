//! The live Casbin enforcer wrapper.
//!
//! [`CasbinService`] owns an in-memory [`Enforcer`] loaded from the shared
//! `casbin_rule` table (through the repository's `PgCasbinAdapter`). It is
//! deliberately **read + reload only**:
//!
//! - All policy *writes* (`g` user→role groupings, `p` role→permission lines)
//!   happen inside the RBAC repository transactions, which are the single
//!   source of truth. Exposing write methods here would create a second write
//!   path that could drift from the relational join tables.
//! - After a repository write succeeds, the route handler calls [`reload`] to
//!   refresh this enforcer's in-memory snapshot, so authorization decisions are
//!   immediately consistent with the database.
//!
//! Reads ([`enforce`] / [`has_policy`]) hold a shared lock and never touch the
//! database; the write lock is taken only during [`reload`].
//!
//! [`reload`]: CasbinService::reload
//! [`enforce`]: CasbinService::enforce
//! [`has_policy`]: CasbinService::has_policy

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(test)]
use casbin::MemoryAdapter;
use casbin::{CoreApi, DefaultModel, Enforcer, MgmtApi};
use quant_pivot_models::enums::rbac::casbin::OBJECT_TYPE_RESOURCE;
use quant_pivot_repository::postgres::PgCasbinAdapter;
use sea_orm::DatabaseConnection;
use tokio::sync::RwLock;

use crate::{auth::casbin::model::CASBIN_MODEL, error::WebError};

/// Thread-safe wrapper around a live Casbin [`Enforcer`].
pub struct CasbinService {
    enforcer: RwLock<Enforcer>,
    healthy: AtomicBool,
    authorization_revision: AtomicU64,
}

impl CasbinService {
    /// Build the enforcer from the [`CASBIN_MODEL`] and a Postgres-backed policy
    /// adapter, loading the current policy set into memory.
    pub async fn new(db: DatabaseConnection) -> Result<Self, WebError> {
        let model = DefaultModel::from_str(CASBIN_MODEL)
            .await
            .map_err(|error| WebError::Internal(format!("casbin model load failed: {error}")))?;
        let enforcer = Enforcer::new(model, PgCasbinAdapter::new(db))
            .await
            .map_err(|error| WebError::Internal(format!("casbin enforcer init failed: {error}")))?;
        Ok(Self {
            enforcer: RwLock::new(enforcer),
            healthy: AtomicBool::new(true),
            authorization_revision: AtomicU64::new(1),
        })
    }

    /// Whether `subject` (a stable `user_id`) may perform `act` on a `resource`
    /// of type `obj`. Honors the `super_admin` bypass encoded in the matcher.
    pub async fn enforce(&self, subject: &str, obj: &str, act: &str) -> Result<bool, WebError> {
        if !self.healthy.load(Ordering::Acquire) {
            return Err(WebError::ServiceUnavailable(
                "authorization policy is unavailable".to_owned(),
            ));
        }
        self.enforcer
            .read()
            .await
            .enforce((subject, obj, act, OBJECT_TYPE_RESOURCE))
            .map_err(|error| WebError::Internal(format!("casbin enforce failed: {error}")))
    }

    /// Whether the role `role_code` directly holds the `(obj, act)` permission.
    ///
    /// This is a pure policy-membership check (it does not evaluate the matcher
    /// or any grouping), used to validate an explicit `acting_role` on governed
    /// endpoints.
    pub async fn has_policy(&self, role_code: &str, obj: &str, act: &str) -> bool {
        if !self.healthy.load(Ordering::Acquire) {
            return false;
        }
        self.enforcer.read().await.has_policy(vec![
            role_code.to_owned(),
            obj.to_owned(),
            act.to_owned(),
            OBJECT_TYPE_RESOURCE.to_owned(),
        ])
    }

    /// Reload the in-memory policy from the database.
    ///
    /// Called by handlers after a repository write that mutated `casbin_rule`
    /// (role/permission/grouping changes, role enable/disable, user/role
    /// deletion), so subsequent authorization decisions see the new state.
    pub async fn reload(&self) -> Result<(), WebError> {
        self.healthy.store(false, Ordering::Release);
        let result = self
            .enforcer
            .write()
            .await
            .load_policy()
            .await
            .map_err(|error| WebError::Internal(format!("casbin reload failed: {error}")));
        if result.is_ok() {
            self.authorization_revision.fetch_add(1, Ordering::AcqRel);
            self.healthy.store(true, Ordering::Release);
        }
        result
    }

    /// Monotonic revision of the successfully loaded authorization snapshot.
    #[must_use]
    pub fn authorization_revision(&self) -> u64 {
        self.authorization_revision.load(Ordering::Acquire)
    }

    /// Whether the in-memory policy is known to match the persistence source.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }
}

#[cfg(test)]
impl CasbinService {
    /// Build a database-free enforcer over an empty in-memory policy set.
    pub(crate) async fn in_memory() -> Self {
        let model = DefaultModel::from_str(CASBIN_MODEL)
            .await
            .expect("casbin model");
        let enforcer = Enforcer::new(model, MemoryAdapter::default())
            .await
            .expect("in-memory enforcer");
        Self {
            enforcer: RwLock::new(enforcer),
            healthy: AtomicBool::new(true),
            authorization_revision: AtomicU64::new(1),
        }
    }

    /// Seed a single `p(role_code, obj, act, "resource")` policy line.
    pub(crate) async fn add_test_policy(&self, role_code: &str, obj: &str, act: &str) {
        self.enforcer
            .write()
            .await
            .add_policy(vec![
                role_code.to_owned(),
                obj.to_owned(),
                act.to_owned(),
                OBJECT_TYPE_RESOURCE.to_owned(),
            ])
            .await
            .expect("seed test policy");
    }

    /// Seed a single `g(user_id, role_code)` grouping line.
    pub(crate) async fn add_test_grouping(&self, user_id: &str, role_code: &str) {
        self.enforcer
            .write()
            .await
            .add_grouping_policy(vec![user_id.to_owned(), role_code.to_owned()])
            .await
            .expect("seed test grouping");
    }
}
