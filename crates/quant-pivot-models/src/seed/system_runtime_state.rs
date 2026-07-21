//! System runtime-state singleton seed — inserts the canonical id=1 row.
//!
//! Fresh deployments boot in [`QuantRuntimeMode::ReportOnly`] by construction.

use std::{future::Future, pin::Pin};

use chrono::Utc;
use sea_orm::{DatabaseTransaction, DbErr, EntityTrait, IntoActiveModel, sea_query::OnConflict};

use crate::{
    domain::governance::UpsertSystemRuntimeState,
    entities::system_runtime_state::{Column, Entity},
    enums::{quant::QuantRuntimeMode, system::BootstrapPhase},
    seed::{SeedArtifact, SeedConflictPolicy, SeedContext, SeedDependency, SeedSpec},
};

const DEPENDS_ON: &[SeedDependency] = &[];
const PRODUCES: &[SeedArtifact] = &[];

const SINGLETON_ID: i32 = 1;

pub const SYSTEM_RUNTIME_STATE_SEED: SeedSpec = SeedSpec {
    id: "operational.system_runtime_state.bootstrap",
    version: 1,
    target_table: "system_runtime_state",
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::InsertIfAbsent,
    checksum: "system_runtime_state.bootstrap.v1.boot",
    apply: load_boxed,
    hydrate: hydrate_boxed,
};

pub async fn load(db: &DatabaseTransaction, _ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let am = UpsertSystemRuntimeState {
        id: SINGLETON_ID,
        quant_runtime_mode: QuantRuntimeMode::ReportOnly,
        bootstrap_phase: BootstrapPhase::Initializing,
        bootstrap_contract_version: 1,
        state_revision: 0,
        changed_by: "bootstrap".to_owned(),
        reason: "bootstrap seed".to_owned(),
        changed_at: Utc::now(),
    }
    .into_active_model();
    Entity::insert(am)
        .on_conflict(OnConflict::column(Column::Id).do_nothing().to_owned())
        .exec_without_returning(db)
        .await
}

fn load_boxed<'a>(
    db: &'a DatabaseTransaction,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<u64, DbErr>> + Send + 'a>> {
    Box::pin(load(db, ctx))
}

async fn hydrate(db: &DatabaseTransaction, _ctx: &mut SeedContext) -> Result<(), DbErr> {
    let rows = Entity::find().all(db).await?;
    let [state] = rows.as_slice() else {
        return Err(DbErr::Custom(format!(
            "system_runtime_state must contain exactly singleton id=1; found {} rows",
            rows.len()
        )));
    };
    if state.id != SINGLETON_ID || state.bootstrap_contract_version != 1 || state.state_revision < 0
    {
        return Err(DbErr::Custom(format!(
            "invalid system_runtime_state singleton id={} bootstrap_contract_version={} state_revision={}",
            state.id, state.bootstrap_contract_version, state.state_revision
        )));
    }
    Ok(())
}

fn hydrate_boxed<'a>(
    db: &'a DatabaseTransaction,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<(), DbErr>> + Send + 'a>> {
    Box::pin(hydrate(db, ctx))
}
