//! System runtime-state singleton seed — inserts the canonical id=1 row.
//!
//! Fresh deployments boot in [`QuantRuntimeMode::ReportOnly`] by construction.

use std::{future::Future, pin::Pin};

use crate::{
    domain::UpsertSystemRuntimeState,
    entities::system_runtime_state,
    enums::{quant::QuantRuntimeMode, system::BootstrapPhase},
    seed::{SeedArtifact, SeedConflictPolicy, SeedContext, SeedDependency, SeedSpec},
};
use chrono::Utc;
use sea_orm::{DbErr, EntityTrait, IntoActiveModel, sea_query::OnConflict};

const DEPENDS_ON: &[SeedDependency] = &[];
const PRODUCES: &[SeedArtifact] = &[];

const SINGLETON_ID: i32 = 1;

pub const SYSTEM_RUNTIME_STATE_SEED: SeedSpec = SeedSpec {
    id: "operational.system_runtime_state.bootstrap",
    version: 4,
    target_table: "system_runtime_state",
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::InsertIfAbsent,
    checksum: "system_runtime_state.bootstrap.v4.contract-and-state-revision",
    apply: load_boxed,
    hydrate: hydrate_boxed,
};

pub async fn load(db: &sea_orm::DatabaseTransaction, _ctx: &mut SeedContext) -> Result<u64, DbErr> {
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
    system_runtime_state::Entity::insert(am)
        .on_conflict(
            OnConflict::column(system_runtime_state::Column::Id)
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
}

fn load_boxed<'a>(
    db: &'a sea_orm::DatabaseTransaction,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<u64, DbErr>> + Send + 'a>> {
    Box::pin(load(db, ctx))
}

async fn hydrate(db: &sea_orm::DatabaseTransaction, _ctx: &mut SeedContext) -> Result<(), DbErr> {
    let rows = system_runtime_state::Entity::find().all(db).await?;
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
    db: &'a sea_orm::DatabaseTransaction,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<(), DbErr>> + Send + 'a>> {
    Box::pin(hydrate(db, ctx))
}
