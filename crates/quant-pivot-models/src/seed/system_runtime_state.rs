//! System runtime-state singleton seed — inserts the canonical id=1 row.
//!
//! The active execution mode is a money-critical, governed value: the **only**
//! sanctioned path to escalate beyond `DryRun` is the audited `/system/mode`
//! transition (preflight + quiesce + hash chain + operation log). A fresh
//! deployment must therefore boot in the safest mode by construction, never from
//! an editable config file.
//!
//! This seed creates the singleton row with [`ExecutionMode::DryRun`] using
//! `ON CONFLICT DO NOTHING` ([`SeedConflictPolicy::InsertIfAbsent`]), exactly
//! like `risk_engine_state`. The `DO NOTHING` clause is essential: it guarantees
//! a re-migration never resets an operator's deliberate mode (e.g. `Live`) back
//! to `DryRun`.

use std::pin::Pin;

use crate::{
    domain::UpsertSystemRuntimeState,
    entities::system_runtime_state,
    enums::common::ExecutionMode,
    idens::system_runtime_state::SystemRuntimeState,
    schema::seed::{SeedArtifact, SeedDependency, SeedSpec},
    seed::{SeedConflictPolicy, SeedContext},
};
use chrono::Utc;
use sea_orm::{
    ConnectionTrait, DbErr, EntityTrait, Iden, IntoActiveModel, QueryTrait, sea_query::OnConflict,
};

const DEPENDS_ON: &[SeedDependency] = &[];
const PRODUCES: &[SeedArtifact] = &[];

/// The fixed primary key of the operational singleton row.
const SINGLETON_ID: i32 = 1;

pub const SYSTEM_RUNTIME_STATE_SEED: SeedSpec = SeedSpec {
    id: "operational.system_runtime_state.bootstrap",
    version: 1,
    target_table: system_runtime_state_table_name,
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::InsertIfAbsent,
    checksum: "system_runtime_state.bootstrap.v1",
    loader: load_boxed,
};

/// Insert the singleton runtime-state row in the safest mode.
///
/// Uses `ON CONFLICT DO NOTHING` so an operator's persisted mode is never
/// clobbered by a later re-migration.
pub async fn load(db: &dyn ConnectionTrait, _ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let am = UpsertSystemRuntimeState {
        id: SINGLETON_ID,
        execution_mode: ExecutionMode::DryRun,
        changed_by: "bootstrap".to_owned(),
        reason: "bootstrap seed".to_owned(),
        changed_at: Utc::now(),
    }
    .into_active_model();
    let backend = db.get_database_backend();
    let stmt = system_runtime_state::Entity::insert(am)
        .on_conflict(
            OnConflict::column(SystemRuntimeState::Id)
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

fn system_runtime_state_table_name() -> String {
    SystemRuntimeState::Table.to_string()
}
