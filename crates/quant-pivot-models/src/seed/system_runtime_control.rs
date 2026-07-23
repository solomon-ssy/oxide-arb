//! Atomic runtime-control singleton seed.

use std::{future::Future, pin::Pin};

use chrono::Utc;
use sea_orm::{ActiveValue::Set, DatabaseTransaction, DbErr, EntityTrait, sea_query::OnConflict};

use crate::{
    entities::system_runtime_control::{ActiveModel, Column, Entity},
    enums::{
        execution::KillSwitchState, quant::QuantRuntimeMode, settlement::SettlementWritePolicy,
    },
    seed::{SeedArtifact, SeedConflictPolicy, SeedContext, SeedDependency, SeedSpec},
};

const DEPENDS_ON: &[SeedDependency] = &[];
const PRODUCES: &[SeedArtifact] = &[];
const SINGLETON_ID: i32 = 1;

pub const SYSTEM_RUNTIME_CONTROL_SEED: SeedSpec = SeedSpec {
    id: "operational.system_runtime_control.bootstrap",
    version: 1,
    target_table: "system_runtime_control",
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::InsertIfAbsent,
    checksum: "system_runtime_control.bootstrap.v1.report-only-disabled-closed",
    apply: load_boxed,
    hydrate: hydrate_boxed,
};

pub async fn load(db: &DatabaseTransaction, _ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let now = Utc::now();
    Entity::insert(ActiveModel {
        id: Set(SINGLETON_ID),
        quant_runtime_mode: Set(QuantRuntimeMode::ReportOnly),
        settlement_write_policy: Set(SettlementWritePolicy::Disabled),
        kill_switch_state: Set(KillSwitchState::Closed),
        kill_switch_requires_ack: Set(false),
        revision: Set(0),
        changed_by: Set("bootstrap".to_owned()),
        reason: Set("fresh boot safe defaults".to_owned()),
        changed_at: Set(now),
        updated_at: Set(now),
    })
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
            "system_runtime_control must contain exactly singleton id=1; found {} rows",
            rows.len()
        )));
    };
    if state.id != SINGLETON_ID || state.revision < 0 {
        return Err(DbErr::Custom(format!(
            "invalid system_runtime_control singleton id={} revision={}",
            state.id, state.revision
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
