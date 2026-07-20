//! Infrastructure-only contracts for native SQL statements.
//!
//! Ordinary `PostgreSQL` CRUD remains expressed through `SeaORM`/`SeaQuery`. A
//! [`SqlContract`] is required only when a runtime query must use native SQL.
//! The contract binds a stable observability identity, typed boundary, and
//! deterministic result budget to that exception. Deploy-only native SQL is
//! registered here as well and remains additionally governed by immutable
//! migration manifests.

use std::collections::BTreeSet;

use sea_orm::Statement;

/// Database dialect required by a native SQL statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Postgres,
    ClickHouse,
}

impl SqlDialect {
    const fn id_prefix(self) -> &'static str {
        match self {
            Self::Postgres => "pg.",
            Self::ClickHouse => "ch.",
        }
    }
}

/// Operational risk attached to a native SQL boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlSafetyClass {
    /// User- or worker-facing read with a bounded result.
    BoundedRead,
    /// Aggregate/metadata read that returns a fixed small shape.
    AggregateRead,
    /// Health or readiness observation with no business mutation.
    OperationalRead,
    /// Read-only lifecycle inspection used by deploy/reset tooling.
    LifecycleRead,
    /// Explicit schema/database lifecycle mutation outside application runtime.
    LifecycleMutation,
}

/// Audited runtime contract for one logical native-SQL operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlContract {
    id: &'static str,
    dialect: SqlDialect,
    owner: &'static str,
    input: &'static str,
    output: &'static str,
    statement_budget: u16,
    result_row_budget: u64,
    result_byte_budget: u64,
    safety_class: SqlSafetyClass,
}

/// Deterministic statement and result limits for one operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlBudget {
    pub statements: u16,
    pub result_rows: u64,
    pub result_bytes: u64,
}

impl SqlBudget {
    #[must_use]
    pub const fn new(statements: u16, result_rows: u64, result_bytes: u64) -> Self {
        Self {
            statements,
            result_rows,
            result_bytes,
        }
    }
}

impl SqlContract {
    /// Define one native-SQL operation. Registry validation is performed by
    /// `sql-contract-audit` and crate tests rather than by a fallible runtime
    /// constructor on every request.
    #[must_use]
    pub const fn new(
        id: &'static str,
        dialect: SqlDialect,
        owner: &'static str,
        input: &'static str,
        output: &'static str,
        budget: SqlBudget,
        safety_class: SqlSafetyClass,
    ) -> Self {
        Self {
            id,
            dialect,
            owner,
            input,
            output,
            statement_budget: budget.statements,
            result_row_budget: budget.result_rows,
            result_byte_budget: budget.result_bytes,
            safety_class,
        }
    }

    #[must_use]
    pub const fn id(&self) -> &'static str {
        self.id
    }

    #[must_use]
    pub const fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    #[must_use]
    pub const fn owner(&self) -> &'static str {
        self.owner
    }

    #[must_use]
    pub const fn input(&self) -> &'static str {
        self.input
    }

    #[must_use]
    pub const fn output(&self) -> &'static str {
        self.output
    }

    #[must_use]
    pub const fn statement_budget(&self) -> u16 {
        self.statement_budget
    }

    #[must_use]
    pub const fn result_row_budget(&self) -> u64 {
        self.result_row_budget
    }

    #[must_use]
    pub const fn result_byte_budget(&self) -> u64 {
        self.result_byte_budget
    }

    #[must_use]
    pub const fn safety_class(&self) -> SqlSafetyClass {
        self.safety_class
    }

    /// Apply the contract's result budgets to a `ClickHouse` query. `ClickHouse`
    /// aborts with `throw` instead of returning a silently truncated result.
    pub fn clickhouse_query(
        &self,
        client: &clickhouse::Client,
        sql: &str,
    ) -> clickhouse::query::Query {
        client
            .query(sql)
            .with_setting("log_comment", self.id)
            .with_setting("max_result_rows", self.result_row_budget.to_string())
            .with_setting("max_result_bytes", self.result_byte_budget.to_string())
            .with_setting("result_overflow_mode", "throw")
    }

    /// Bind a `PostgreSQL` raw statement to this contract. `PostgreSQL` result
    /// cardinality remains enforced by the fixed query shape; the AST audit
    /// prevents uncontracted raw statements from entering runtime code.
    #[must_use]
    pub const fn postgres_statement(&self, statement: Statement) -> Statement {
        statement
    }

    /// Associate borrowed `PostgreSQL` SQL with this contract. This covers
    /// lifecycle `sqlx` and `SeaORM` `execute_unprepared` calls.
    #[must_use]
    pub const fn postgres_query<'a>(&self, sql: &'a str) -> &'a str {
        sql
    }

    /// Associate dynamically rendered `PostgreSQL` SQL with this contract after
    /// every interpolated identifier has been validated and quoted by the
    /// owning boundary.
    #[must_use]
    pub const fn postgres_owned_query(&self, sql: String) -> String {
        sql
    }

    /// Validate registry metadata without executing SQL.
    pub fn validate(&self) -> Result<(), String> {
        if !self.id.starts_with(self.dialect.id_prefix())
            || !self.id.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._".contains(&byte)
            })
        {
            return Err(format!(
                "SQL contract id `{}` is not canonical for {:?}",
                self.id, self.dialect
            ));
        }
        if self.owner.is_empty() || self.input.is_empty() || self.output.is_empty() {
            return Err(format!(
                "SQL contract `{}` has an empty typed boundary",
                self.id
            ));
        }
        if self.statement_budget == 0 || self.result_row_budget == 0 || self.result_byte_budget == 0
        {
            return Err(format!(
                "SQL contract `{}` has an unbounded zero budget",
                self.id
            ));
        }
        Ok(())
    }
}

/// Validate a compiled registry, including global contract-id uniqueness.
pub fn validate_registry(contracts: &[SqlContract]) -> Result<(), String> {
    let mut ids = BTreeSet::new();
    for contract in contracts {
        contract.validate()?;
        if !ids.insert(contract.id()) {
            return Err(format!("duplicate SQL contract id `{}`", contract.id()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{SqlBudget, SqlContract, SqlDialect, SqlSafetyClass, validate_registry};

    const VALID: SqlContract = SqlContract::new(
        "ch.storage.health.v1",
        SqlDialect::ClickHouse,
        "quant_pivot_storage::clickhouse::ClickHousePool::health_check",
        "()",
        "u8",
        SqlBudget::new(1, 1, 64),
        SqlSafetyClass::OperationalRead,
    );

    #[test]
    fn registry_rejects_duplicate_ids_and_zero_budgets() {
        assert!(validate_registry(&[VALID]).is_ok());
        assert!(validate_registry(&[VALID, VALID]).is_err());
        let invalid = SqlContract::new(
            "ch.storage.invalid.v1",
            SqlDialect::ClickHouse,
            "owner",
            "()",
            "u8",
            SqlBudget::new(0, 0, 0),
            SqlSafetyClass::OperationalRead,
        );
        assert!(validate_registry(&[invalid]).is_err());
    }
}
