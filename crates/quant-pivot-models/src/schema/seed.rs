//! Versioned seed metadata.

use sea_orm::DbErr;

use crate::seed::{SeedConflictPolicy, SeedContext};
use std::future::Future;
use std::pin::Pin;

/// Typed key for passing seed artifacts between graph-ordered seeds.
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
    Table(fn() -> String),
    Seed { id: &'static str, version: u32 },
    Artifact(SeedArtifactKey),
}

impl SeedDependency {
    pub const fn table(table_name: fn() -> String) -> Self {
        Self::Table(table_name)
    }

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

/// Function signature for seed loaders.
pub type SeedLoader = for<'a> fn(
    &'a dyn sea_orm::ConnectionTrait,
    &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<u64, DbErr>> + Send + 'a>>;

/// A versioned, auditable seed unit.
#[derive(Clone, Copy)]
pub struct SeedSpec {
    pub id: &'static str,
    pub version: u32,
    pub target_table: fn() -> String,
    pub depends_on: &'static [SeedDependency],
    pub produces: &'static [SeedArtifact],
    pub conflict_policy: SeedConflictPolicy,
    pub checksum: &'static str,
    pub loader: SeedLoader,
}
