//! Grants the `super_admin` role visibility of every seeded menu node.

use std::{future::Future, pin::Pin};

use sea_orm::{
    ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait, QueryTrait, sea_query::OnConflict,
};

use crate::{
    entities::role_menu,
    idens::role_menu::role_menu_table_name,
    schema::seed::{SeedArtifact, SeedDependency, SeedSpec},
    seed::{
        SeedConflictPolicy, SeedContext,
        rbac::{MENUS_ARTIFACT, ROLE_SUPER_ADMIN, ROLES_ARTIFACT, RoleIdMap},
    },
    types::MenuId,
};

const SEED_ID: &str = "rbac.role_menu.bootstrap";

const DEPENDS_ON: &[SeedDependency] = &[
    SeedDependency::Artifact(ROLES_ARTIFACT),
    SeedDependency::Artifact(MENUS_ARTIFACT),
];
const PRODUCES: &[SeedArtifact] = &[];

pub const ROLE_MENU_SEED: SeedSpec = SeedSpec {
    id: SEED_ID,
    version: 1,
    target_table: role_menu_table_name,
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::GraphOrdered,
    checksum: "rbac.role_menu.bootstrap.v1",
    loader: load_boxed,
};

/// Assign every seeded menu to `super_admin`.
pub async fn load(db: &dyn ConnectionTrait, ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let roles = ctx
        .require::<RoleIdMap>(ROLES_ARTIFACT)
        .map_err(|error| DbErr::Custom(error.to_string()))?;
    let super_admin_id = roles
        .get(ROLE_SUPER_ADMIN)
        .cloned()
        .ok_or_else(|| DbErr::Custom("super_admin role missing from seed context".to_owned()))?;
    let menu_ids = ctx
        .require::<Vec<MenuId>>(MENUS_ARTIFACT)
        .map_err(|error| DbErr::Custom(error.to_string()))?
        .clone();

    let models = menu_ids
        .into_iter()
        .map(|menu_id| role_menu::ActiveModel {
            role_id: Set(super_admin_id.clone()),
            menu_id: Set(menu_id),
            ..Default::default()
        })
        .collect::<Vec<_>>();

    let backend = db.get_database_backend();
    let stmt = role_menu::Entity::insert_many(models)
        .on_conflict(
            OnConflict::columns([role_menu::Column::RoleId, role_menu::Column::MenuId])
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
