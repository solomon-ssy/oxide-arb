//! Sealed `PostgreSQL` DDL primitives not currently modeled by `SeaQuery`.

use sea_orm::{DbBackend, Statement};
use sea_orm_migration::prelude::*;

pub(in crate::migrations) const SOURCE: &[u8] = include_bytes!("v1.rs");

const EMPTY_BOOT_TARGET_SQL: &str = "WITH target_objects AS (\
    SELECT 'relation:' || c.relname AS object_name \
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
    WHERE n.nspname = 'public' \
      AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f') \
      AND c.relname <> 'seaql_migrations' \
    UNION ALL \
    SELECT 'type:' || t.typname FROM pg_type t \
    JOIN pg_namespace n ON n.oid = t.typnamespace \
    WHERE n.nspname = 'public' AND t.typtype IN ('e', 'd', 'r') \
    UNION ALL \
    SELECT 'function:' || p.proname FROM pg_proc p \
    JOIN pg_namespace n ON n.oid = p.pronamespace \
    WHERE n.nspname = 'public' \
    UNION ALL \
    SELECT 'trigger:' || g.tgname FROM pg_trigger g \
    JOIN pg_class c ON c.oid = g.tgrelid \
    JOIN pg_namespace n ON n.oid = c.relnamespace \
    WHERE n.nspname = 'public' AND NOT g.tgisinternal \
) SELECT COUNT(*)::bigint AS object_count, \
    COALESCE((SELECT string_agg(object_name, ', ') FROM (\
        SELECT object_name FROM target_objects ORDER BY object_name LIMIT 20\
    ) sample), '') AS object_summary FROM target_objects";

/// Fail closed unless `public` contains only the migration infrastructure.
/// `PostgreSQL`'s heterogeneous catalog cannot be expressed by `SeaQuery`, so the
/// complete static statement is sealed inside this versioned dialect module.
pub(in crate::migrations) async fn assert_empty_boot_target(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    let row = manager
        .get_connection()
        .query_one_raw(Statement::from_string(
            DbBackend::Postgres,
            EMPTY_BOOT_TARGET_SQL,
        ))
        .await?
        .ok_or_else(|| {
            DbErr::Custom("PostgreSQL catalog returned no boot preflight row".to_owned())
        })?;
    let object_count = row.try_get::<i64>("", "object_count")?;
    if object_count == 0 {
        return Ok(());
    }
    let object_summary = row.try_get::<String>("", "object_summary")?;
    Err(DbErr::Custom(format!(
        "boot migration requires an empty public schema; found {object_count} tables, views, materialized views, sequences, types, functions, or triggers ({object_summary}). Clear PostgreSQL and bootstrap again"
    )))
}

/// Table constraint kinds supported by the audited `PostgreSQL` extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::migrations) enum ConstraintKind {
    Check,
    ForeignKey,
    Unique,
}

/// Immutable table constraint captured from the canonical schema contract.
#[derive(Debug, Clone, Copy)]
pub(in crate::migrations) struct ConstraintSpec {
    pub name: &'static str,
    pub table: &'static str,
    pub kind: ConstraintKind,
    pub definition: &'static str,
}

/// The only trigger programs owned by the initial schema contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::migrations) enum TriggerProgram {
    DenyWrite,
    SetUpdatedAt,
}

impl TriggerProgram {
    const fn function_name(self) -> &'static str {
        match self {
            Self::DenyWrite => "trigger_deny_write",
            Self::SetUpdatedAt => "trigger_set_updated_at",
        }
    }
}

/// Trigger event sets supported by the initial schema contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::migrations) enum TriggerEvents {
    Update,
    DeleteOrUpdate,
}

impl TriggerEvents {
    const fn sql(self) -> &'static str {
        match self {
            Self::Update => "UPDATE",
            Self::DeleteOrUpdate => "DELETE OR UPDATE",
        }
    }
}

/// Immutable trigger binding captured from the canonical schema contract.
#[derive(Debug, Clone, Copy)]
pub(in crate::migrations) struct TriggerSpec {
    pub name: &'static str,
    pub table: &'static str,
    pub events: TriggerEvents,
    pub program: TriggerProgram,
}

pub(in crate::migrations) async fn create_constraint(
    manager: &SchemaManager<'_>,
    spec: ConstraintSpec,
) -> Result<(), DbErr> {
    (spec).validate_constraint_definition()?;
    execute(
        manager,
        format!(
            "ALTER TABLE {} ADD CONSTRAINT {} {}",
            qualified_table(spec.table),
            quote_identifier(spec.name),
            spec.definition
        ),
    )
    .await
}

pub(in crate::migrations) async fn create_trigger_programs(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    for statement in [
        "CREATE FUNCTION public.trigger_deny_write() RETURNS trigger LANGUAGE plpgsql AS \
         $function$ BEGIN RAISE EXCEPTION 'table % is append-only (WORM); % is not permitted', \
         TG_TABLE_NAME, TG_OP; END; $function$",
        "CREATE FUNCTION public.trigger_set_updated_at() RETURNS trigger LANGUAGE plpgsql AS \
         $function$ BEGIN IF (to_jsonb(NEW) - 'updated_at') IS DISTINCT FROM \
         (to_jsonb(OLD) - 'updated_at') THEN NEW.updated_at = statement_timestamp(); ELSE \
         NEW.updated_at = OLD.updated_at; END IF; RETURN NEW; END; $function$",
    ] {
        execute(manager, statement.to_owned()).await?;
    }
    Ok(())
}

pub(in crate::migrations) async fn drop_trigger_programs(
    manager: &SchemaManager<'_>,
) -> Result<(), DbErr> {
    for name in ["trigger_set_updated_at", "trigger_deny_write"] {
        execute(
            manager,
            format!("DROP FUNCTION public.{}()", quote_identifier(name)),
        )
        .await?;
    }
    Ok(())
}

pub(in crate::migrations) async fn create_trigger(
    manager: &SchemaManager<'_>,
    spec: TriggerSpec,
) -> Result<(), DbErr> {
    execute(
        manager,
        format!(
            "CREATE TRIGGER {} BEFORE {} ON {} FOR EACH ROW EXECUTE FUNCTION public.{}()",
            quote_identifier(spec.name),
            spec.events.sql(),
            qualified_table(spec.table),
            quote_identifier(spec.program.function_name())
        ),
    )
    .await
}

pub(in crate::migrations) fn index_predicate(predicate: &'static str) -> Result<SimpleExpr, DbErr> {
    if predicate.is_empty() || predicate.contains(';') {
        return Err(DbErr::Custom("invalid partial-index predicate".to_owned()));
    }
    Ok(Expr::cust(predicate))
}

impl ConstraintSpec {
    fn validate_constraint_definition(self) -> Result<(), DbErr> {
        let valid = match self.kind {
            ConstraintKind::Check => self.definition.starts_with("CHECK ("),
            ConstraintKind::ForeignKey => self.definition.starts_with("FOREIGN KEY ("),
            ConstraintKind::Unique => self.definition.starts_with("UNIQUE ("),
        };
        if !valid || self.definition.contains(';') {
            return Err(DbErr::Custom(format!(
                "invalid {:?} definition for constraint `{}`",
                self.kind, self.name
            )));
        }
        Ok(())
    }
}

fn qualified_table(table: &str) -> String {
    format!("public.{}", quote_identifier(table))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

async fn execute(manager: &SchemaManager<'_>, sql: String) -> Result<(), DbErr> {
    manager
        .get_connection()
        .execute_unprepared(&sql)
        .await
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::{ConstraintKind, ConstraintSpec, index_predicate};

    #[test]
    fn rejects_constraint_mismatch_separator() {
        let unique_as_check = ConstraintSpec {
            name: "bad",
            table: "sample",
            kind: ConstraintKind::Check,
            definition: "UNIQUE (id)",
        };
        assert!((unique_as_check).validate_constraint_definition().is_err());

        let injected = ConstraintSpec {
            name: "bad",
            table: "sample",
            kind: ConstraintKind::Check,
            definition: "CHECK (id > 0); DROP TABLE sample",
        };
        assert!((injected).validate_constraint_definition().is_err());
    }

    #[test]
    fn rejects_empty_multi_predicate() {
        assert!(index_predicate("").is_err());
        assert!(index_predicate("status = 'active'; DROP TABLE sample").is_err());
    }
}
