//! Sealed `PostgreSQL` DDL primitives not currently modeled by `SeaQuery`.

use sea_orm_migration::prelude::*;

pub(in crate::migrations) const SOURCE: &[u8] = include_bytes!("v1.rs");

/// Table constraint kinds supported by the audited `PostgreSQL` extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::migrations) enum ConstraintKind {
    Check,
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
    validate_constraint_definition(spec)?;
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

fn validate_constraint_definition(spec: ConstraintSpec) -> Result<(), DbErr> {
    let valid = match spec.kind {
        ConstraintKind::Check => spec.definition.starts_with("CHECK ("),
        ConstraintKind::Unique => spec.definition.starts_with("UNIQUE ("),
    };
    if !valid || spec.definition.contains(';') {
        return Err(DbErr::Custom(format!(
            "invalid {:?} definition for constraint `{}`",
            spec.kind, spec.name
        )));
    }
    Ok(())
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
    use super::{ConstraintKind, ConstraintSpec, index_predicate, validate_constraint_definition};

    #[test]
    fn rejects_constraint_kind_mismatch_and_statement_separator() {
        let unique_as_check = ConstraintSpec {
            name: "bad",
            table: "sample",
            kind: ConstraintKind::Check,
            definition: "UNIQUE (id)",
        };
        assert!(validate_constraint_definition(unique_as_check).is_err());

        let injected = ConstraintSpec {
            name: "bad",
            table: "sample",
            kind: ConstraintKind::Check,
            definition: "CHECK (id > 0); DROP TABLE sample",
        };
        assert!(validate_constraint_definition(injected).is_err());
    }

    #[test]
    fn rejects_empty_or_multi_statement_index_predicate() {
        assert!(index_predicate("").is_err());
        assert!(index_predicate("status = 'active'; DROP TABLE sample").is_err());
    }
}
