//! Single clean-install `PostgreSQL` baseline.

use sea_orm::{
    ActiveValue::{NotSet, Set},
    DbBackend, EntityTrait, Schema,
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

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        NAME
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        v1::assert_empty_boot_target(manager).await?;
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
        // SeaORM 2.0 does not discover an enum nested only in a PostgreSQL
        // array column. All columns that share those types use an explicit
        // custom column type so Entity First has one deterministic enum owner.
        // The target is proven empty, therefore Entity First `apply` is the
        // deterministic boot primitive; experimental non-destructive `sync`
        // must never hide a schema mismatch.
        db.get_schema_registry(MODULE_PREFIX)
            .apply(db)
            .await
            .map_err(|error| DbErr::Custom(format!("Entity First schema apply failed: {error}")))?;
        column_defaults::apply(manager)
            .await
            .map_err(|error| DbErr::Custom(format!("column defaults failed: {error}")))?;
        seed_boot_rows(manager)
            .await
            .map_err(|error| DbErr::Custom(format!("boot row seed failed: {error}")))?;
        create_migration_audit(manager)
            .await
            .map_err(|error| DbErr::Custom(format!("migration audit schema failed: {error}")))?;
        relational_invariants::apply(manager)
            .await
            .map_err(|error| DbErr::Custom(format!("relational invariants failed: {error}")))?;
        query_indexes::apply(manager)
            .await
            .map_err(|error| DbErr::Custom(format!("query indexes failed: {error}")))?;
        worm_triggers::apply(manager)
            .await
            .map_err(|error| DbErr::Custom(format!("WORM triggers failed: {error}")))?;
        audit::record(manager, spec())
            .await
            .map_err(|error| DbErr::Custom(format!("migration artifact audit failed: {error}")))
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
        generation: Set(1),
        current_snapshot_id: Set(None),
        current_snapshot_hash: Set(None),
        created_at: NotSet,
        updated_at: NotSet,
    })
    .exec_without_returning(manager.get_connection())
    .await?;
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
