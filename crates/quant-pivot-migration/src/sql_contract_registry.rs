//! Compiled native-SQL registry owned by deploy-only `PostgreSQL` migration code.

use quant_pivot_sql_contract::{SqlBudget, SqlContract, SqlDialect, SqlSafetyClass};

const KIB: u64 = 1_024;

pub(crate) const POSTGRES_PREPRODUCTION_INSPECT: SqlContract = SqlContract::new(
    "pg.migration.preproduction_inspect.v1",
    SqlDialect::Postgres,
    "quant_pivot_migration::inspect_preproduction_postgres",
    "ValidatedPreproductionTarget",
    "PreproductionPostgresInventory",
    SqlBudget::new(5, 1, 64 * KIB),
    SqlSafetyClass::LifecycleRead,
);

pub(crate) const POSTGRES_PREPRODUCTION_RESET: SqlContract = SqlContract::new(
    "pg.migration.preproduction_reset.v1",
    SqlDialect::Postgres,
    "quant_pivot_migration::reset_preproduction_postgres",
    "ValidatedPreproductionTarget + OfflineAuthorization",
    "()",
    SqlBudget::new(2, 1, KIB),
    SqlSafetyClass::LifecycleMutation,
);

pub(crate) const POSTGRES_LIFECYCLE_LEASE: SqlContract = SqlContract::new(
    "pg.migration.lifecycle_lease.v1",
    SqlDialect::Postgres,
    "quant_pivot_migration::LifecycleLease",
    "CanonicalPostgresDatabase + LifecycleLockKey",
    "LifecycleLeaseHeartbeat",
    SqlBudget::new(1, 1, KIB),
    SqlSafetyClass::LifecycleMutation,
);

pub(crate) const POSTGRES_MIGRATION_INSPECT: SqlContract = SqlContract::new(
    "pg.migration.schema_inspect.v1",
    SqlDialect::Postgres,
    "quant_pivot_migration::plan/verify",
    "CompiledMigrationManifest",
    "MigrationPlan + AuditRows",
    SqlBudget::new(16, 10_000, 16 * 1_024 * KIB),
    SqlSafetyClass::LifecycleRead,
);

pub(crate) const POSTGRES_SCHEMA_EXTENSION: SqlContract = SqlContract::new(
    "pg.migration.schema_extension.v1",
    SqlDialect::Postgres,
    "quant_pivot_migration::migrations::support::v1",
    "ValidatedConstraint/TriggerSpec",
    "FreshBootV1Schema",
    SqlBudget::new(512, 10_000, 16 * 1_024 * KIB),
    SqlSafetyClass::LifecycleMutation,
);

const MIGRATION_SQL_CONTRACTS: &[SqlContract] = &[
    POSTGRES_PREPRODUCTION_INSPECT,
    POSTGRES_PREPRODUCTION_RESET,
    POSTGRES_LIFECYCLE_LEASE,
    POSTGRES_MIGRATION_INSPECT,
    POSTGRES_SCHEMA_EXTENSION,
];

/// Return the compiled migration-owned native-SQL registry.
#[must_use]
pub const fn migration_sql_contracts() -> &'static [SqlContract] {
    MIGRATION_SQL_CONTRACTS
}

#[cfg(test)]
mod tests {
    use quant_pivot_sql_contract::validate_registry;

    use super::MIGRATION_SQL_CONTRACTS;

    #[test]
    fn migration_registry_is_valid() {
        assert!(validate_registry(MIGRATION_SQL_CONTRACTS).is_ok());
    }
}
