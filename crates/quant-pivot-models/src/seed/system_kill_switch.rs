//! System kill-switch singleton seed — inserts the canonical id=1 row.

use std::{future::Future, pin::Pin};

use chrono::Utc;
use sea_orm::{
    ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait, Iden, QueryTrait, sea_query::OnConflict,
};

use crate::{
    entities::system_kill_switch,
    enums::execution::KillSwitchState,
    idens::system_kill_switch::SystemKillSwitch,
    schema::seed::{SeedArtifact, SeedDependency, SeedSpec},
    seed::{SeedConflictPolicy, SeedContext},
};

const DEPENDS_ON: &[SeedDependency] = &[];
const PRODUCES: &[SeedArtifact] = &[];
const SINGLETON_ID: i32 = 1;

pub const SYSTEM_KILL_SWITCH_SEED: SeedSpec = SeedSpec {
    id: "operational.system_kill_switch.bootstrap",
    version: 1,
    target_table: system_kill_switch_table_name,
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::InsertIfAbsent,
    checksum: "system_kill_switch.bootstrap.v1",
    loader: load_boxed,
};

pub async fn load(db: &dyn ConnectionTrait, _ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let now = Utc::now();
    let model = system_kill_switch::ActiveModel {
        id: Set(SINGLETON_ID),
        state: Set(KillSwitchState::Closed),
        changed_by: Set("bootstrap".to_owned()),
        reason: Set("bootstrap seed".to_owned()),
        requires_operator_ack: Set(false),
        changed_at: Set(now),
        updated_at: Set(now),
    };
    let backend = db.get_database_backend();
    let stmt = system_kill_switch::Entity::insert(model)
        .on_conflict(
            OnConflict::column(SystemKillSwitch::Id)
                .do_nothing()
                .to_owned(),
        )
        .build(backend);
    let result = db.execute(stmt).await?;
    Ok(result.rows_affected())
}

fn load_boxed<'a>(
    db: &'a dyn ConnectionTrait,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<u64, DbErr>> + Send + 'a>> {
    Box::pin(load(db, ctx))
}

fn system_kill_switch_table_name() -> String {
    SystemKillSwitch::Table.to_string()
}
