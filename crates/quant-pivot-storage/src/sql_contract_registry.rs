//! Compiled native-SQL registry owned by `quant-pivot-storage`.

use quant_pivot_sql_contract::{SqlBudget, SqlContract, SqlDialect, SqlSafetyClass};

const KIB: u64 = 1_024;

pub(crate) const POSTGRES_HEALTH: SqlContract = SqlContract::new(
    "pg.storage.health.v1",
    SqlDialect::Postgres,
    "quant_pivot_storage::postgres::PostgresPool::health_check",
    "()",
    "()",
    SqlBudget::new(1, 1, 64),
    SqlSafetyClass::OperationalRead,
);

pub(crate) const POSTGRES_SESSION_PARAMETER: SqlContract = SqlContract::new(
    "pg.storage.session_parameter.v1",
    SqlDialect::Postgres,
    "quant_pivot_storage::postgres::PostgresPool::show_guc",
    "TimeoutGuc",
    "String",
    SqlBudget::new(1, 1, KIB),
    SqlSafetyClass::OperationalRead,
);

pub(crate) const POSTGRES_DATABASE_BOOTSTRAP: SqlContract = SqlContract::new(
    "pg.storage.database_bootstrap.v1",
    SqlDialect::Postgres,
    "quant_pivot_storage::postgres::ensure_database",
    "PostgresConfig + DatabaseOwner",
    "EnsureDatabaseOutcome",
    SqlBudget::new(2, 1, KIB),
    SqlSafetyClass::LifecycleMutation,
);

pub(crate) const POSTGRES_SCHEMA_DEPLOY: SqlContract = SqlContract::new(
    "pg.storage.schema_deploy.v1",
    SqlDialect::Postgres,
    "quant_pivot_storage::postgres::migration::finalize_schema_deployment",
    "MigrationManifest + RuntimeRole + SeedBundle",
    "PostgresSchemaStatus",
    SqlBudget::new(32, 10_000, 16 * 1_024 * KIB),
    SqlSafetyClass::LifecycleMutation,
);

pub(crate) const POSTGRES_SCHEMA_VERIFY: SqlContract = SqlContract::new(
    "pg.storage.schema_verify.v1",
    SqlDialect::Postgres,
    "quant_pivot_storage::postgres::migration::verify_schema",
    "MigrationManifest + SemanticSchemaManifest",
    "PostgresSchemaStatus",
    SqlBudget::new(16, 10_000, 16 * 1_024 * KIB),
    SqlSafetyClass::LifecycleRead,
);

pub(crate) const CLICKHOUSE_HEALTH: SqlContract = SqlContract::new(
    "ch.storage.health.v1",
    SqlDialect::ClickHouse,
    "quant_pivot_storage::clickhouse::ClickHousePool::health_check",
    "()",
    "u8",
    SqlBudget::new(1, 1, 64),
    SqlSafetyClass::OperationalRead,
);

pub(crate) const CLICKHOUSE_DATABASE_BOOTSTRAP: SqlContract = SqlContract::new(
    "ch.storage.database_bootstrap.v1",
    SqlDialect::ClickHouse,
    "quant_pivot_storage::clickhouse::ensure_database",
    "ClickHouseConfig",
    "EnsureDatabaseOutcome",
    SqlBudget::new(2, 1, KIB),
    SqlSafetyClass::LifecycleMutation,
);

pub(crate) const CLICKHOUSE_DATABASE_OBJECT_COUNT: SqlContract = SqlContract::new(
    "ch.storage.database_object_count.v1",
    SqlDialect::ClickHouse,
    "quant_pivot_storage::clickhouse::database_object_count",
    "ClickHouseConfig",
    "u64",
    SqlBudget::new(1, 1, 64),
    SqlSafetyClass::LifecycleRead,
);

pub(crate) const CLICKHOUSE_PREPRODUCTION_INSPECT: SqlContract = SqlContract::new(
    "ch.storage.preproduction_inspect.v1",
    SqlDialect::ClickHouse,
    "quant_pivot_storage::clickhouse::active_preproduction_query_count",
    "ClickHouseConfig",
    "u64",
    SqlBudget::new(1, 1, 64),
    SqlSafetyClass::LifecycleRead,
);

pub(crate) const CLICKHOUSE_PREPRODUCTION_RESET: SqlContract = SqlContract::new(
    "ch.storage.preproduction_reset.v1",
    SqlDialect::ClickHouse,
    "quant_pivot_storage::clickhouse::reset_preproduction_database",
    "ValidatedPreproductionTarget",
    "()",
    SqlBudget::new(2, 1, KIB),
    SqlSafetyClass::LifecycleMutation,
);

pub(crate) const CLICKHOUSE_RAW_HISTORY_READINESS: SqlContract = SqlContract::new(
    "ch.storage.raw_history_readiness.v1",
    SqlDialect::ClickHouse,
    "quant_pivot_storage::clickhouse::ClickHousePool::observe_raw_history_table",
    "ResearchSourceBinding + AsOf",
    "RawHistoryObservation",
    SqlBudget::new(3, 3, 64 * KIB),
    SqlSafetyClass::OperationalRead,
);

pub(crate) const CLICKHOUSE_BOOK_LATENCY_READINESS: SqlContract = SqlContract::new(
    "ch.storage.book_latency_readiness.v1",
    SqlDialect::ClickHouse,
    "quant_pivot_storage::clickhouse::ClickHousePool::observe_book_latency",
    "HalfOpenTimeWindow",
    "BookLatencyObservation",
    SqlBudget::new(1, 1, KIB),
    SqlSafetyClass::AggregateRead,
);

pub(crate) const CLICKHOUSE_SCHEMA_APPLY: SqlContract = SqlContract::new(
    "ch.storage.schema_apply.v1",
    SqlDialect::ClickHouse,
    "quant_pivot_storage::clickhouse::migration::apply_schema_migrations",
    "CompiledMigrationRegistry + OfflineAuthorization",
    "ClickHouseSchemaStatus",
    SqlBudget::new(64, 10_000, 64 * 1_024 * KIB),
    SqlSafetyClass::LifecycleMutation,
);

pub(crate) const CLICKHOUSE_SCHEMA_VERIFY: SqlContract = SqlContract::new(
    "ch.storage.schema_verify.v1",
    SqlDialect::ClickHouse,
    "quant_pivot_storage::clickhouse::migration::verify_schema",
    "CompiledMigrationRegistry + SchemaManifest",
    "ClickHouseSchemaStatus",
    SqlBudget::new(64, 10_000, 64 * 1_024 * KIB),
    SqlSafetyClass::LifecycleRead,
);

const STORAGE_SQL_CONTRACTS: &[SqlContract] = &[
    POSTGRES_HEALTH,
    POSTGRES_SESSION_PARAMETER,
    POSTGRES_DATABASE_BOOTSTRAP,
    POSTGRES_SCHEMA_DEPLOY,
    POSTGRES_SCHEMA_VERIFY,
    CLICKHOUSE_HEALTH,
    CLICKHOUSE_DATABASE_BOOTSTRAP,
    CLICKHOUSE_DATABASE_OBJECT_COUNT,
    CLICKHOUSE_PREPRODUCTION_INSPECT,
    CLICKHOUSE_PREPRODUCTION_RESET,
    CLICKHOUSE_RAW_HISTORY_READINESS,
    CLICKHOUSE_BOOK_LATENCY_READINESS,
    CLICKHOUSE_SCHEMA_APPLY,
    CLICKHOUSE_SCHEMA_VERIFY,
];

/// Return the compiled storage-owned native-SQL registry.
#[must_use]
pub const fn storage_sql_contracts() -> &'static [SqlContract] {
    STORAGE_SQL_CONTRACTS
}

#[cfg(test)]
mod tests {
    use super::{CLICKHOUSE_SCHEMA_APPLY, POSTGRES_SCHEMA_DEPLOY, STORAGE_SQL_CONTRACTS};
    use quant_pivot_sql_contract::validate_registry;

    #[test]
    fn storage_registry_is_valid() {
        assert!(validate_registry(STORAGE_SQL_CONTRACTS).is_ok());
    }

    #[test]
    fn lifecycle_statement_budgets_are_stable() {
        assert_eq!(POSTGRES_SCHEMA_DEPLOY.statement_budget(), 32);
        assert_eq!(CLICKHOUSE_SCHEMA_APPLY.statement_budget(), 64);
    }
}
