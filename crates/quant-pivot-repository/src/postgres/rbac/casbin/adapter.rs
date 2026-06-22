//! A Casbin [`Adapter`] persisting policies into the `casbin_rule` table.
//!
//! Unlike the ng-gateway adapter — which de-duplicates inserts by `ptype` alone
//! and therefore silently drops every policy after the first of a given type —
//! this adapter matches on the **full `(ptype, v0..v5)` tuple** for add, remove,
//! and idempotent inserts. The `uq_casbin_rule` unique index is the database
//! guarantee behind exact de-duplication; `ON CONFLICT DO NOTHING` makes adds
//! idempotent without ever swallowing a distinct policy.
//!
//! Per the casbin-rs contract, the mutating methods report whether storage was
//! actually changed: `add_policy` / `add_policies` return `false` when the
//! rule(s) already exist, mirroring the in-tree `MemoryAdapter` (and
//! `add_policies` is all-or-nothing). In this system the RBAC repository
//! transactions are the *sole* writer of `casbin_rule` (join table + policy rows
//! committed atomically), and the service reloads the enforcer afterwards — so
//! these write paths exist to satisfy the trait and stay correct if the adapter
//! is ever driven directly, not to carry production mutations.

use casbin::{Adapter, Filter, Model, error::AdapterError};
use quant_pivot_models::{
    entities::casbin_rule::{Column, Entity},
    enums::rbac::casbin::SECTIONS,
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, TransactionTrait,
    sea_query::Condition,
};

use crate::postgres::rbac::casbin::row;

/// Persists Casbin policies into the `casbin_rule` table.
pub struct PgCasbinAdapter {
    db: DatabaseConnection,
    is_filtered: bool,
}

impl PgCasbinAdapter {
    /// Create an adapter over the given connection handle.
    pub const fn new(db: DatabaseConnection) -> Self {
        Self {
            db,
            is_filtered: false,
        }
    }
}

/// Wrap any storage/database error as a Casbin adapter error.
fn adapter_error<E>(error: E) -> casbin::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    casbin::Error::from(AdapterError(Box::new(error)))
}

/// Build a Casbin adapter error from a static reason.
fn adapter_reason(reason: &'static str) -> casbin::Error {
    casbin::Error::from(AdapterError(reason.into()))
}

/// Whether a policy line is excluded by the supplied filter for its section.
fn filtered_out(filter: &Filter<'_>, section: &str, tokens: &[String]) -> bool {
    let pattern = if section == "g" { &filter.g } else { &filter.p };
    for (index, expected) in pattern.iter().enumerate() {
        if expected.is_empty() {
            continue;
        }
        match tokens.get(index) {
            Some(actual) if actual == expected => {}
            _ => return true,
        }
    }
    false
}

#[async_trait::async_trait]
impl Adapter for PgCasbinAdapter {
    async fn load_policy(&mut self, model: &mut dyn Model) -> casbin::Result<()> {
        let rows = Entity::find().all(&self.db).await.map_err(adapter_error)?;
        for db_row in rows {
            if let Some((section, ptype, tokens)) = row::row_to_policy(db_row) {
                model.add_policy(&section, &ptype, tokens);
            }
        }
        self.is_filtered = false;
        Ok(())
    }

    async fn load_filtered_policy<'a>(
        &mut self,
        model: &mut dyn Model,
        filter: Filter<'a>,
    ) -> casbin::Result<()> {
        let rows = Entity::find().all(&self.db).await.map_err(adapter_error)?;
        for db_row in rows {
            if let Some((section, ptype, tokens)) = row::row_to_policy(db_row) {
                if filtered_out(&filter, &section, &tokens) {
                    continue;
                }
                model.add_policy(&section, &ptype, tokens);
            }
        }
        self.is_filtered = true;
        Ok(())
    }

    async fn save_policy(&mut self, model: &mut dyn Model) -> casbin::Result<()> {
        // A filtered enforcer holds only a subset in memory; persisting it would
        // erase the rest of the table. Casbin disables save under filtering.
        if self.is_filtered {
            return Err(adapter_reason(
                "save_policy is disabled while the enforcer is filtered",
            ));
        }

        let mut rows = Vec::new();
        for section in SECTIONS {
            if let Some(assertions) = model.get_model().get(section) {
                for (ptype, assertion) in assertions {
                    for rule in assertion.get_policy() {
                        rows.push(row::policy_to_row(ptype, rule));
                    }
                }
            }
        }

        let txn = self.db.begin().await.map_err(adapter_error)?;
        Entity::delete_many()
            .exec(&txn)
            .await
            .map_err(adapter_error)?;
        if !rows.is_empty() {
            Entity::insert_many(rows)
                .exec_without_returning(&txn)
                .await
                .map_err(adapter_error)?;
        }
        txn.commit().await.map_err(adapter_error)?;
        self.is_filtered = false;
        Ok(())
    }

    async fn clear_policy(&mut self) -> casbin::Result<()> {
        Entity::delete_many()
            .exec(&self.db)
            .await
            .map_err(adapter_error)?;
        Ok(())
    }

    fn is_filtered(&self) -> bool {
        self.is_filtered
    }

    async fn add_policy(
        &mut self,
        _sec: &str,
        ptype: &str,
        rule: Vec<String>,
    ) -> casbin::Result<bool> {
        let affected = Entity::insert(row::policy_to_row(ptype, &rule))
            .on_conflict(row::full_tuple_conflict().do_nothing().to_owned())
            .exec_without_returning(&self.db)
            .await
            .map_err(adapter_error)?;
        Ok(affected > 0)
    }

    async fn add_policies(
        &mut self,
        _sec: &str,
        ptype: &str,
        rules: Vec<Vec<String>>,
    ) -> casbin::Result<bool> {
        if rules.is_empty() {
            return Ok(false);
        }

        let txn = self.db.begin().await.map_err(adapter_error)?;
        // All-or-nothing, matching `MemoryAdapter`: if any rule already exists,
        // add none and report that storage was not modified.
        for rule in &rules {
            let existing = Entity::find()
                .filter(row::exact_match(ptype, rule))
                .count(&txn)
                .await
                .map_err(adapter_error)?;
            if existing > 0 {
                txn.rollback().await.map_err(adapter_error)?;
                return Ok(false);
            }
        }

        let rows = rules
            .iter()
            .map(|rule| row::policy_to_row(ptype, rule))
            .collect::<Vec<_>>();
        Entity::insert_many(rows)
            .on_conflict(row::full_tuple_conflict().do_nothing().to_owned())
            .exec_without_returning(&txn)
            .await
            .map_err(adapter_error)?;
        txn.commit().await.map_err(adapter_error)?;
        Ok(true)
    }

    async fn remove_policy(
        &mut self,
        _sec: &str,
        ptype: &str,
        rule: Vec<String>,
    ) -> casbin::Result<bool> {
        let result = Entity::delete_many()
            .filter(row::exact_match(ptype, &rule))
            .exec(&self.db)
            .await
            .map_err(adapter_error)?;
        Ok(result.rows_affected > 0)
    }

    async fn remove_policies(
        &mut self,
        _sec: &str,
        ptype: &str,
        rules: Vec<Vec<String>>,
    ) -> casbin::Result<bool> {
        let mut removed_any = false;
        let txn = self.db.begin().await.map_err(adapter_error)?;
        for rule in rules {
            let result = Entity::delete_many()
                .filter(row::exact_match(ptype, &rule))
                .exec(&txn)
                .await
                .map_err(adapter_error)?;
            removed_any |= result.rows_affected > 0;
        }
        txn.commit().await.map_err(adapter_error)?;
        Ok(removed_any)
    }

    async fn remove_filtered_policy(
        &mut self,
        _sec: &str,
        ptype: &str,
        field_index: usize,
        field_values: Vec<String>,
    ) -> casbin::Result<bool> {
        let mut condition = Condition::all().add(Column::Ptype.eq(ptype));
        for (offset, value) in field_values.iter().enumerate() {
            if value.is_empty() {
                continue;
            }
            if let Some(column) = row::value_column(field_index + offset) {
                condition = condition.add(column.eq(value.as_str()));
            }
        }
        let result = Entity::delete_many()
            .filter(condition)
            .exec(&self.db)
            .await
            .map_err(adapter_error)?;
        Ok(result.rows_affected > 0)
    }
}
