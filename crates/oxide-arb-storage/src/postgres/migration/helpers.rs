//! Shared migration execution pipeline.
//!
//! Every migration file follows the same lifecycle:
//!
//! ```text
//! up:   create_tables → create_indexes → specials → seeding_data
//! down: drop_tables (reverse dependency order)
//! ```
//!
//! Declarative phases (`create_tables`, `create_indexes`, `drop_tables`) return
//! `SeaORM` statement builders. Imperative phases (`specials`, `seeding_data`) run
//! async logic — raw SQL, partial indexes, seed rows. Phases with no work delegate
//! to [`noop`]; the function is always present in each migration module.

use sea_orm_migration::prelude::*;

/// Apply all table DDL in declaration order (respects foreign-key dependencies).
pub async fn create_tables(
    manager: &SchemaManager<'_>,
    tables: impl IntoIterator<Item = TableCreateStatement>,
) -> Result<(), DbErr> {
    for table in tables {
        manager.create_table(table).await?;
    }
    Ok(())
}

/// Apply all SeaORM-managed index DDL.
pub async fn create_indexes(
    manager: &SchemaManager<'_>,
    indexes: impl IntoIterator<Item = IndexCreateStatement>,
) -> Result<(), DbErr> {
    for index in indexes {
        manager.create_index(index).await?;
    }
    Ok(())
}

/// Execute raw SQL statements `SeaORM` cannot express (partial indexes, extensions, etc.).
pub async fn execute_sql(
    manager: &SchemaManager<'_>,
    statements: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<(), DbErr> {
    let conn = manager.get_connection();
    for sql in statements {
        conn.execute_unprepared(sql.as_ref()).await?;
    }
    Ok(())
}

/// Drop tables in reverse dependency order.
pub async fn drop_tables(
    manager: &SchemaManager<'_>,
    tables: impl IntoIterator<Item = TableDropStatement>,
) -> Result<(), DbErr> {
    for table in tables {
        manager.drop_table(table).await?;
    }
    Ok(())
}

/// Run the canonical `up` pipeline in fixed order.
pub async fn migrate_up(
    manager: &SchemaManager<'_>,
    tables: impl IntoIterator<Item = TableCreateStatement>,
    indexes: impl IntoIterator<Item = IndexCreateStatement>,
    specials: impl std::future::Future<Output = Result<(), DbErr>>,
    seeding_data: impl std::future::Future<Output = Result<(), DbErr>>,
) -> Result<(), DbErr> {
    create_tables(manager, tables).await?;
    create_indexes(manager, indexes).await?;
    specials.await?;
    seeding_data.await?;
    Ok(())
}

/// Run a schema-only migration in fixed DDL order.
///
/// Use this helper for `create_*`, `alter_*`, and trigger/function migrations
/// that do not seed application data.
pub async fn migrate_schema(
    manager: &SchemaManager<'_>,
    tables: impl IntoIterator<Item = TableCreateStatement>,
    indexes: impl IntoIterator<Item = IndexCreateStatement>,
    specials: impl std::future::Future<Output = Result<(), DbErr>>,
) -> Result<(), DbErr> {
    create_tables(manager, tables).await?;
    create_indexes(manager, indexes).await?;
    specials.await?;
    Ok(())
}

/// Run an idempotent seed migration.
///
/// Seed migrations must not create or alter schema. They may only populate
/// data into tables created by prior schema migrations.
pub async fn migrate_seed(
    seeding_data: impl std::future::Future<Output = Result<(), DbErr>>,
) -> Result<(), DbErr> {
    seeding_data.await
}

/// Run a data-only migration such as a backfill or data repair.
///
/// Data migrations must document their required input schema in the migration
/// module docs and must be idempotent when possible.
pub async fn migrate_data(
    data_migration: impl std::future::Future<Output = Result<(), DbErr>>,
) -> Result<(), DbErr> {
    data_migration.await
}

/// No-op phase for migrations with nothing to run in `specials` or `seeding_data`.
pub async fn noop(_manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    Ok(())
}
