//! Conflict resolution strategies for seed insertion.

/// Determines how a seed handles pre-existing rows during bootstrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedConflictPolicy {
    /// `ON CONFLICT DO NOTHING` — never update existing rows.
    ///
    /// For operational singletons like `risk_engine_state` where production
    /// state must never be overwritten by a seed.
    InsertIfAbsent,

    /// `ON CONFLICT (key) DO NOTHING` — insert new keys, skip existing.
    ///
    /// For key-value stores like `runtime_config` where operator-modified
    /// values must survive re-migration.
    InsertKeyIfAbsent,

    /// `ON CONFLICT UPDATE` — upsert columns on conflict.
    ///
    /// For data patches where existing rows should be updated to new
    /// seed values. Idempotency is provided by the migration framework.
    UpsertPatch,

    /// Dependency-ordered insertion with `SeedContext` propagation.
    ///
    /// For RBAC graphs where downstream seeds (relation, casbin) read
    /// upstream entity IDs from the context.
    GraphOrdered,
}
