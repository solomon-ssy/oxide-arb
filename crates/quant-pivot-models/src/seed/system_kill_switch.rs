//! System kill-switch singleton seed — inserts the canonical id=1 row.

use std::{future::Future, pin::Pin};

use chrono::Utc;
use sea_orm::{ActiveValue::Set, DatabaseTransaction, DbErr, EntityTrait, sea_query::OnConflict};

use crate::{
    entities::system_kill_switch::{ActiveModel, Column, Entity},
    enums::execution::KillSwitchState,
    seed::{SeedArtifact, SeedConflictPolicy, SeedContext, SeedDependency, SeedSpec},
};

const DEPENDS_ON: &[SeedDependency] = &[];
const PRODUCES: &[SeedArtifact] = &[];
const SINGLETON_ID: i32 = 1;

pub const SYSTEM_KILL_SWITCH_SEED: SeedSpec = SeedSpec {
    id: "operational.system_kill_switch.bootstrap",
    version: 1,
    target_table: "system_kill_switch",
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::InsertIfAbsent,
    checksum: "system_kill_switch.bootstrap.v1.boot-report-only",
    apply: load_boxed,
    hydrate: hydrate_boxed,
};

pub async fn load(db: &DatabaseTransaction, _ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let now = Utc::now();
    let model = ActiveModel {
        id: Set(SINGLETON_ID),
        state: Set(KillSwitchState::ReportOnlyForced),
        changed_by: Set("bootstrap".to_owned()),
        reason: Set("bootstrap requires explicit operator activation".to_owned()),
        requires_operator_ack: Set(true),
        changed_at: Set(now),
        updated_at: Set(now),
    };
    Entity::insert(model)
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
            "system_kill_switch must contain exactly singleton id=1; found {} rows",
            rows.len()
        )));
    };
    if state.id != SINGLETON_ID {
        return Err(DbErr::Custom(format!(
            "system_kill_switch singleton must use id=1; found id={}",
            state.id
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
