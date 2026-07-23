//! Versioned seed execution contract.

use std::{future::Future, pin::Pin};

use sea_orm::{DatabaseTransaction, DbErr};

use crate::seed::{SeedConflictPolicy, SeedContext, rbac, system_runtime_control};

/// Typed key for passing database-hydrated artifacts between ordered seeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SeedArtifactKey(pub &'static str);

impl SeedArtifactKey {
    pub const fn new(value: &'static str) -> Self {
        Self(value)
    }
}

/// A dependency required before a seed can run.
#[derive(Clone, Copy)]
pub enum SeedDependency {
    Seed { id: &'static str, version: u32 },
    Artifact(SeedArtifactKey),
}

impl SeedDependency {
    pub const fn seed(id: &'static str, version: u32) -> Self {
        Self::Seed { id, version }
    }

    pub const fn artifact(key: &'static str) -> Self {
        Self::Artifact(SeedArtifactKey::new(key))
    }
}

/// Artifact produced by a seed for downstream graph seeds.
#[derive(Clone, Copy)]
pub struct SeedArtifact {
    pub key: SeedArtifactKey,
    pub produced_by: &'static str,
}

impl SeedArtifact {
    pub const fn new(key: SeedArtifactKey, produced_by: &'static str) -> Self {
        Self { key, produced_by }
    }
}

pub type SeedApply = for<'a> fn(
    &'a DatabaseTransaction,
    &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<u64, DbErr>> + Send + 'a>>;

pub type SeedHydrate = for<'a> fn(
    &'a DatabaseTransaction,
    &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<(), DbErr>> + Send + 'a>>;

/// A versioned, auditable seed unit.
#[derive(Clone, Copy)]
pub struct SeedSpec {
    pub id: &'static str,
    pub version: u32,
    pub target_table: &'static str,
    pub depends_on: &'static [SeedDependency],
    pub produces: &'static [SeedArtifact],
    pub conflict_policy: SeedConflictPolicy,
    pub checksum: &'static str,
    pub apply: SeedApply,
    pub hydrate: SeedHydrate,
}

/// Canonical seed registry. Explicit ownership avoids link-time schema discovery.
#[must_use]
pub fn all_specs() -> Vec<SeedSpec> {
    vec![
        system_runtime_control::SYSTEM_RUNTIME_CONTROL_SEED,
        rbac::menus::MENUS_SEED,
        rbac::roles::ROLES_SEED,
        rbac::admin_user::ADMIN_USER_SEED,
        rbac::casbin::CASBIN_SEED,
        rbac::role_menu::ROLE_MENU_SEED,
        rbac::user_role::USER_ROLE_SEED,
    ]
}
