//! Catalog-driven Postgres migration helpers.

use quant_pivot_error::seed::SeedError;
use quant_pivot_models::{
    schema::{
        catalog,
        graph::{create_order, drop_order},
        index::IndexStatement,
        seed::{SeedDependency, SeedSpec},
        trigger::TriggerKind,
    },
    seed::SeedContext,
};
use sea_orm::{
    ConnectionTrait, Statement,
    sea_query::{PostgresQueryBuilder, TableCreateStatement},
};
use sea_orm_migration::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use tracing::info;

/// Unified catalog migration runner.
pub struct SchemaRunner<'a> {
    manager: &'a SchemaManager<'a>,
}

impl<'a> SchemaRunner<'a> {
    pub const fn new(manager: &'a SchemaManager<'a>) -> Self {
        Self { manager }
    }

    pub async fn create_schema(&self) -> Result<(), DbErr> {
        for spec in create_order() {
            create_postgres_table(self.manager, (spec.table)(), &(spec.table_name)()).await?;
        }

        execute_sql(
            self.manager,
            ["CREATE OR REPLACE FUNCTION trigger_set_updated_at() \
          RETURNS TRIGGER AS $$ \
          BEGIN \
              IF ROW(NEW.*) IS DISTINCT FROM ROW(OLD.*) THEN \
                  NEW.updated_at = statement_timestamp(); \
              END IF; \
              RETURN NEW; \
          END; \
          $$ LANGUAGE plpgsql"],
        )
        .await?;

        execute_sql(
            self.manager,
            ["CREATE OR REPLACE FUNCTION trigger_deny_write() \
          RETURNS TRIGGER AS $$ \
          BEGIN \
              RAISE EXCEPTION 'table % is append-only (WORM); % is not permitted', \
                  TG_TABLE_NAME, TG_OP; \
          END; \
          $$ LANGUAGE plpgsql"],
        )
        .await?;

        for spec in catalog::tables() {
            for trigger in (spec.triggers)() {
                let table = (trigger.table_name)();
                match trigger.kind {
                    TriggerKind::UpdatedAt => {
                        execute_sql(self.manager, [create_updated_at_trigger(&table)]).await?;
                    }
                    TriggerKind::AppendOnly => {
                        execute_sql(self.manager, [create_append_only_trigger(&table)]).await?;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn drop_schema(&self) -> Result<(), DbErr> {
        for spec in catalog::tables() {
            for trigger in (spec.triggers)() {
                let table = (trigger.table_name)();
                match trigger.kind {
                    TriggerKind::UpdatedAt => {
                        execute_sql(self.manager, [drop_updated_at_trigger(&table)]).await?;
                    }
                    TriggerKind::AppendOnly => {
                        execute_sql(self.manager, [drop_append_only_trigger(&table)]).await?;
                    }
                }
            }
        }

        execute_sql(
            self.manager,
            ["DROP FUNCTION IF EXISTS trigger_set_updated_at"],
        )
        .await?;

        execute_sql(self.manager, ["DROP FUNCTION IF EXISTS trigger_deny_write"]).await?;

        for spec in drop_order() {
            self.manager
                .drop_table(
                    Table::drop()
                        .table(Alias::new((spec.table_name)()))
                        .if_exists()
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }

    pub async fn create_indexes(&self) -> Result<(), DbErr> {
        for spec in catalog::tables() {
            for index in (spec.indexes)() {
                match index.statement {
                    IndexStatement::SeaQuery(stmt) => self.manager.create_index(*stmt).await?,
                    IndexStatement::RawSql(sql) => execute_sql(self.manager, [sql]).await?,
                }
            }
        }
        Ok(())
    }

    pub async fn drop_indexes(&self) -> Result<(), DbErr> {
        for spec in catalog::tables().into_iter().rev() {
            for index in (spec.indexes)().into_iter().rev() {
                execute_sql(
                    self.manager,
                    [format!("DROP INDEX IF EXISTS {}", index.name)],
                )
                .await?;
            }
        }
        Ok(())
    }

    pub async fn run_seeds(&self) -> Result<(), DbErr> {
        let db = self.manager.get_connection();
        let mut ctx = SeedContext::new();

        for seed in ordered_seeds()? {
            if seed_already_applied(db, &seed).await? {
                info!(
                    seed_id = seed.id,
                    seed_version = seed.version,
                    "catalog seed already applied"
                );
                continue;
            }

            let rows = (seed.loader)(db, &mut ctx).await?;
            record_seed_application(db, &seed, rows).await?;
            info!(
                seed_id = seed.id,
                seed_version = seed.version,
                rows,
                "catalog seed applied"
            );
        }

        Ok(())
    }
}

async fn create_postgres_table(
    manager: &SchemaManager<'_>,
    statement: TableCreateStatement,
    table: &str,
) -> Result<(), DbErr> {
    let sql = statement.to_string(PostgresQueryBuilder);
    execute_sql(manager, [sql])
        .await
        .map_err(|error| DbErr::Custom(format!("create table `{table}` failed: {error}")))
}

/// Execute raw SQL statements `SeaORM` cannot express (partial indexes, extensions, etc.).
pub async fn execute_sql(
    manager: &SchemaManager<'_>,
    statements: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<(), DbErr> {
    let conn = manager.get_connection();
    for sql in statements {
        let sql = sql.as_ref();
        conn.execute_unprepared(sql)
            .await
            .map_err(|error| DbErr::Custom(format!("raw SQL failed: {error}; sql: {sql}")))?;
    }
    Ok(())
}

/// Create the canonical `updated_at` trigger for a table.
///
/// The table name is double-quoted so reserved identifiers (e.g. `user`) are
/// handled correctly.
pub fn create_updated_at_trigger(table: &str) -> String {
    format!(
        "CREATE TRIGGER trg_{table}_updated_at \
         BEFORE UPDATE ON \"{table}\" \
         FOR EACH ROW \
         EXECUTE FUNCTION trigger_set_updated_at()"
    )
}

/// Drop the canonical `updated_at` trigger for a table.
pub fn drop_updated_at_trigger(table: &str) -> String {
    format!("DROP TRIGGER IF EXISTS trg_{table}_updated_at ON \"{table}\"")
}

/// Create the append-only (WORM) guard trigger for a table.
///
/// Fires before any `UPDATE` or `DELETE` and raises an exception, making the
/// table insert-only at the database level. The table name is double-quoted to
/// support reserved identifiers.
pub fn create_append_only_trigger(table: &str) -> String {
    format!(
        "CREATE TRIGGER trg_{table}_append_only \
         BEFORE UPDATE OR DELETE ON \"{table}\" \
         FOR EACH ROW \
         EXECUTE FUNCTION trigger_deny_write()"
    )
}

/// Drop the append-only (WORM) guard trigger for a table.
pub fn drop_append_only_trigger(table: &str) -> String {
    format!("DROP TRIGGER IF EXISTS trg_{table}_append_only ON \"{table}\"")
}

fn ordered_seeds() -> Result<Vec<SeedSpec>, DbErr> {
    let seeds = catalog::seeds();
    let by_key = seeds
        .iter()
        .map(|seed| (format!("{}#{}", seed.id, seed.version), *seed))
        .collect::<BTreeMap<_, _>>();
    let mut artifact_producers = BTreeMap::new();

    for seed in &seeds {
        for artifact in seed.produces {
            let previous =
                artifact_producers.insert(artifact.key.0, format!("{}#{}", seed.id, seed.version));
            if previous.is_some() {
                return Err(DbErr::Custom(
                    SeedError::DuplicateArtifactProducer {
                        key: artifact.key.0,
                    }
                    .to_string(),
                ));
            }
        }
    }

    let mut incoming = by_key
        .keys()
        .map(|key| (key.clone(), BTreeSet::<String>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = by_key
        .keys()
        .map(|key| (key.clone(), BTreeSet::<String>::new()))
        .collect::<BTreeMap<_, _>>();

    for seed in &seeds {
        let key = format!("{}#{}", seed.id, seed.version);
        for dep in seed.depends_on {
            let parent = match dep {
                SeedDependency::Table(_) => continue,
                SeedDependency::Seed { id, version } => format!("{id}#{version}"),
                SeedDependency::Artifact(artifact) => {
                    artifact_producers.get(artifact.0).cloned().ok_or_else(|| {
                        DbErr::Custom(
                            SeedError::MissingDependency {
                                dependency: artifact.0,
                            }
                            .to_string(),
                        )
                    })?
                }
            };
            if !by_key.contains_key(&parent) {
                return Err(DbErr::Custom(
                    SeedError::MissingDependency {
                        dependency: seed.id,
                    }
                    .to_string(),
                ));
            }
            incoming
                .entry(key.clone())
                .or_default()
                .insert(parent.clone());
            outgoing.entry(parent).or_default().insert(key.clone());
        }
    }

    let mut ready = incoming
        .iter()
        .filter_map(|(key, deps)| deps.is_empty().then_some(key.clone()))
        .collect::<BTreeSet<_>>();
    let mut ordered = Vec::with_capacity(seeds.len());

    while let Some(key) = ready.pop_first() {
        ordered.push(key.clone());
        let children = outgoing.remove(&key).unwrap_or_default();
        for child in children {
            let deps = incoming
                .get_mut(&child)
                .expect("seed child must exist in incoming map");
            deps.remove(&key);
            if deps.is_empty() {
                ready.insert(child);
            }
        }
    }

    if ordered.len() != seeds.len() {
        return Err(DbErr::Custom(SeedError::Cycle.to_string()));
    }

    Ok(ordered.into_iter().map(|key| by_key[&key]).collect())
}

#[inline]
async fn seed_already_applied(db: &dyn ConnectionTrait, seed: &SeedSpec) -> Result<bool, DbErr> {
    let stmt = Statement::from_sql_and_values(
        db.get_database_backend(),
        "SELECT checksum FROM seed_application WHERE seed_id = $1 AND seed_version = $2",
        [seed.id.into(), seed_version_i32(seed)?.into()],
    );

    let Some(row) = db.query_one(stmt).await? else {
        return Ok(false);
    };

    let checksum: String = row.try_get_by_index(0)?;
    if checksum == seed.checksum {
        Ok(true)
    } else {
        Err(DbErr::Custom(format!(
            "seed `{}` v{} checksum mismatch: ledger has `{checksum}`, code has `{}`",
            seed.id, seed.version, seed.checksum
        )))
    }
}

#[inline]
async fn record_seed_application(
    db: &dyn ConnectionTrait,
    seed: &SeedSpec,
    rows: u64,
) -> Result<(), DbErr> {
    let rows = <i64 as TryFrom<u64>>::try_from(rows)
        .map_err(|_| DbErr::Custom(format!("seed `{}` affected too many rows", seed.id)))?;
    let stmt = Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO seed_application (seed_id, seed_version, checksum, rows_affected) \
         VALUES ($1, $2, $3, $4)",
        [
            seed.id.into(),
            seed_version_i32(seed)?.into(),
            seed.checksum.into(),
            rows.into(),
        ],
    );
    db.execute(stmt).await?;
    Ok(())
}

#[inline]
fn seed_version_i32(seed: &SeedSpec) -> Result<i32, DbErr> {
    <i32 as TryFrom<u32>>::try_from(seed.version)
        .map_err(|_| DbErr::Custom(format!("seed `{}` version exceeds i32", seed.id)))
}
