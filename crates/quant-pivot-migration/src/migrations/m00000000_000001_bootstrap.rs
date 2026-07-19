//! Single clean-install `PostgreSQL` baseline.

use sea_orm::{
    ActiveValue::{NotSet, Set},
    ConnectionTrait, DbBackend, EntityTrait, Schema,
    sea_query::Expr,
};
use sea_orm_migration::{prelude::*, sea_query::extension::postgres::Type};

use crate::{
    MigrationSpec, audit, migration_spec,
    snapshots::v1::{
        ARTIFACTS, ENUMS, MODULE_PREFIX, TABLES, policy_activation_guard,
        sea_orm_active_enums::{QpCatalogFilterReason, QpMarketCategory},
    },
};

use super::support::{column_defaults, query_indexes, relational_invariants, v1, worm_triggers};

const NAME: &str = "m00000000_000001_bootstrap";
const SOURCE: &str = include_str!("m00000000_000001_bootstrap.rs");

#[derive(DeriveIden)]
enum SchemaMigrationAudit {
    Table,
    Version,
    ChecksumAlgorithm,
    Checksum,
    ArtifactLength,
    MigrationEngine,
    AppliedAt,
}

#[derive(DeriveIden)]
enum PgTables {
    Table,
    Schemaname,
    Tablename,
}

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        NAME
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert_empty_boot_target(manager).await?;
        let db = manager.get_connection();
        let schema = Schema::new(DbBackend::Postgres);
        for enum_statement in [
            schema.create_enum_from_active_enum::<QpCatalogFilterReason>(),
            schema.create_enum_from_active_enum::<QpMarketCategory>(),
        ] {
            manager
                .create_type(enum_statement.ok_or_else(|| {
                    DbErr::Custom("PostgreSQL native enum definition was not generated".to_owned())
                })?)
                .await?;
        }
        // SeaORM rc.43 does not discover an enum nested in a PostgreSQL array
        // column, so the two array element types are created explicitly above.
        // `sync` then recognizes and reuses them while creating the empty boot schema.
        db.get_schema_registry(MODULE_PREFIX).sync(db).await?;
        column_defaults::apply(manager).await?;
        seed_boot_rows(manager).await?;
        create_migration_audit(manager).await?;
        relational_invariants::apply(manager).await?;
        query_indexes::apply(manager).await?;
        worm_triggers::apply(manager).await?;
        audit::record(manager, spec()).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        audit::remove(manager, NAME).await?;
        for table in TABLES.iter().rev() {
            manager
                .drop_table(
                    Table::drop()
                        .table((Alias::new("public"), Alias::new(*table)))
                        .cascade()
                        .to_owned(),
                )
                .await?;
        }
        manager
            .drop_table(
                Table::drop()
                    .table((Alias::new("public"), SchemaMigrationAudit::Table))
                    .to_owned(),
            )
            .await?;
        for enum_name in ENUMS.iter().rev() {
            manager
                .drop_type(
                    Type::drop()
                        .name((Alias::new("public"), Alias::new(*enum_name)))
                        .to_owned(),
                )
                .await?;
        }
        v1::drop_trigger_programs(manager).await
    }
}

async fn seed_boot_rows(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    policy_activation_guard::Entity::insert(policy_activation_guard::ActiveModel {
        id: Set(1),
        generation: Set(0),
        created_at: NotSet,
    })
    .exec_without_returning(manager.get_connection())
    .await?;
    Ok(())
}

async fn assert_empty_boot_target(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let query = Query::select()
        .expr_as(Expr::col(Asterisk).count(), Alias::new("table_count"))
        .from((Alias::new("pg_catalog"), PgTables::Table))
        .and_where(Expr::col(PgTables::Schemaname).eq("public"))
        .and_where(Expr::col(PgTables::Tablename).ne("seaql_migrations"))
        .to_owned();
    let row = manager
        .get_connection()
        .query_one(&query)
        .await?
        .ok_or_else(|| {
            DbErr::Custom("PostgreSQL catalog returned no boot preflight row".to_owned())
        })?;
    let table_count = row.try_get::<i64>("", "table_count")?;
    if table_count != 0 {
        return Err(DbErr::Custom(format!(
            "boot migration requires an empty public schema; found {table_count} tables. Clear PostgreSQL and bootstrap again"
        )));
    }
    Ok(())
}

async fn create_migration_audit(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table((Alias::new("public"), SchemaMigrationAudit::Table))
                .col(
                    ColumnDef::new(SchemaMigrationAudit::Version)
                        .string()
                        .not_null()
                        .primary_key(),
                )
                .col(
                    ColumnDef::new(SchemaMigrationAudit::ChecksumAlgorithm)
                        .string()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(SchemaMigrationAudit::Checksum)
                        .string()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(SchemaMigrationAudit::ArtifactLength)
                        .big_integer()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(SchemaMigrationAudit::MigrationEngine)
                        .string()
                        .not_null(),
                )
                .col(
                    ColumnDef::new(SchemaMigrationAudit::AppliedAt)
                        .timestamp_with_time_zone()
                        .not_null()
                        .default(Expr::current_timestamp()),
                )
                .to_owned(),
        )
        .await
}

pub fn spec() -> MigrationSpec {
    let mut artifacts = Vec::with_capacity(ARTIFACTS.len() + 6);
    artifacts.push(SOURCE.as_bytes());
    artifacts.extend_from_slice(ARTIFACTS);
    artifacts.extend([
        relational_invariants::SOURCE,
        query_indexes::SOURCE,
        worm_triggers::SOURCE,
        column_defaults::SOURCE,
        v1::SOURCE,
    ]);
    migration_spec(NAME, &artifacts)
}
