//! Catalog-driven Postgres migration helpers.

use oxide_arb_error::seed::SeedError;
use oxide_arb_models::{
    schema::{
        catalog,
        graph::{create_order, drop_order},
        index::IndexStatement,
        seed::{SeedDependency, SeedSpec},
        trigger::TriggerKind,
    },
    seed::SeedContext,
};
use sea_orm::{ConnectionTrait, Statement};
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
            self.manager.create_table((spec.table)()).await?;
        }

        self.execute_sql(["CREATE OR REPLACE FUNCTION trigger_set_updated_at() \
          RETURNS TRIGGER AS $$ \
          BEGIN \
              NEW.updated_at = statement_timestamp(); \
              RETURN NEW; \
          END; \
          $$ LANGUAGE plpgsql"])
            .await?;

        for spec in catalog::tables() {
            for trigger in (spec.triggers)() {
                match trigger.kind {
                    TriggerKind::UpdatedAt => {
                        let table = (trigger.table_name)();
                        self.execute_sql([create_updated_at_trigger(&table)])
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn drop_schema(&self) -> Result<(), DbErr> {
        for spec in catalog::tables() {
            for trigger in (spec.triggers)() {
                match trigger.kind {
                    TriggerKind::UpdatedAt => {
                        let table = (trigger.table_name)();
                        self.execute_sql([drop_updated_at_trigger(&table)]).await?;
                    }
                }
            }
        }

        self.execute_sql(["DROP FUNCTION IF EXISTS trigger_set_updated_at"])
            .await?;

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
                    IndexStatement::RawSql(sql) => self.execute_sql([sql]).await?,
                }
            }
        }
        Ok(())
    }

    pub async fn drop_indexes(&self) -> Result<(), DbErr> {
        for spec in catalog::tables().into_iter().rev() {
            for index in (spec.indexes)().into_iter().rev() {
                self.execute_sql([format!("DROP INDEX IF EXISTS {}", index.name)])
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

    pub async fn execute_sql(
        &self,
        statements: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<(), DbErr> {
        let conn = self.manager.get_connection();
        for sql in statements {
            conn.execute_unprepared(sql.as_ref()).await?;
        }
        Ok(())
    }
}

/// Database-side timestamp for managed write-time columns.
///
/// `PostgreSQL` `CURRENT_TIMESTAMP` is fixed at transaction start, while
/// `clock_timestamp()` can vary row-by-row within one statement. Statement time
/// keeps one stable value per SQL command without going stale across long
/// transactions.
pub fn write_timestamp() -> SimpleExpr {
    Expr::cust("statement_timestamp()")
}

/// Build a required `timestamptz` column with the canonical write-time default.
pub fn timestamp_with_write_default(column: impl IntoIden) -> ColumnDef {
    let mut column_def = ColumnDef::new(column);
    column_def
        .timestamp_with_time_zone()
        .not_null()
        .default(write_timestamp());
    column_def
}

/// Create the canonical `updated_at` trigger for a table.
pub fn create_updated_at_trigger(table: &str) -> String {
    format!(
        "CREATE TRIGGER trg_{table}_updated_at \
         BEFORE UPDATE ON {table} \
         FOR EACH ROW \
         WHEN (OLD.* IS DISTINCT FROM NEW.*) \
         EXECUTE FUNCTION trigger_set_updated_at()"
    )
}

/// Drop the canonical `updated_at` trigger for a table.
pub fn drop_updated_at_trigger(table: &str) -> String {
    format!("DROP TRIGGER IF EXISTS trg_{table}_updated_at ON {table}")
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
