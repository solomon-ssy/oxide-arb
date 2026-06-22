//! System runtime-state singleton seed — inserts the canonical id=1 row.
//!
//! Fresh deployments boot in [`QuantRuntimeMode::ReportOnly`] by construction.

use std::pin::Pin;

use crate::{
    domain::UpsertSystemRuntimeState,
    entities::system_runtime_state,
    enums::quant::QuantRuntimeMode,
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

const SINGLETON_ID: i32 = 1;

pub const SYSTEM_RUNTIME_STATE_SEED: SeedSpec = SeedSpec {
    id: "operational.system_runtime_state.bootstrap",
    version: 2,
    target_table: system_runtime_state_table_name,
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::InsertIfAbsent,
    checksum: "system_runtime_state.bootstrap.v2",
    loader: load_boxed,
};

pub async fn load(db: &dyn ConnectionTrait, _ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let am = UpsertSystemRuntimeState {
        id: SINGLETON_ID,
        quant_runtime_mode: QuantRuntimeMode::ReportOnly,
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
