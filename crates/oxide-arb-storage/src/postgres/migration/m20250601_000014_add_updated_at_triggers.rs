//! Add a reusable `trigger_set_updated_at()` PL/pgSQL function and attach
//! `BEFORE UPDATE` triggers to every table that has an `updated_at` column.
//!
//! This is the database-level safety net: even if application code forgets to
//! set `updated_at`, or uses a `SeaORM` path that bypasses `ActiveModelBehavior`
//! hooks (e.g. `Entity::update_many`, `Entity::insert(...).on_conflict`), the
//! trigger guarantees the column is always fresh.

use super::{create_updated_at_trigger, drop_updated_at_trigger, execute_sql};
use oxide_arb_models::idens::{
    blacklist_entry::BlacklistEntry, calibration::EndgameCalibrationBucket, event::Event,
    market::Market, risk_state::RiskEngineState, runtime_config::RuntimeConfig, trade::Trade,
};
use sea_orm::Iden;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

fn tables_with_updated_at() -> Vec<String> {
    vec![
        Event::Table.to_string(),
        Market::Table.to_string(),
        Trade::Table.to_string(),
        RiskEngineState::Table.to_string(),
        RuntimeConfig::Table.to_string(),
        EndgameCalibrationBucket::Table.to_string(),
        BlacklistEntry::Table.to_string(),
    ]
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let mut stmts = vec![
            "CREATE OR REPLACE FUNCTION trigger_set_updated_at() \
             RETURNS TRIGGER AS $$ \
             BEGIN \
                 NEW.updated_at = statement_timestamp(); \
                 RETURN NEW; \
             END; \
             $$ LANGUAGE plpgsql"
                .to_owned(),
        ];

        for table in tables_with_updated_at() {
            stmts.push(create_updated_at_trigger(&table));
        }

        execute_sql(manager, stmts).await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let mut stmts: Vec<String> = tables_with_updated_at()
            .iter()
            .map(|table| drop_updated_at_trigger(table))
            .collect();

        stmts.push("DROP FUNCTION IF EXISTS trigger_set_updated_at()".to_owned());

        execute_sql(manager, stmts).await
    }
}
