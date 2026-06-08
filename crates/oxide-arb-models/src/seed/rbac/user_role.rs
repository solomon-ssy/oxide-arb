//! Binds the bootstrap admin user to the `super_admin` role.

use std::{future::Future, pin::Pin};

use sea_orm::{
    ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait, QueryTrait, sea_query::OnConflict,
};

use crate::{
    entities::user_role,
    idens::user_role::user_role_table_name,
    schema::seed::{SeedArtifact, SeedDependency, SeedSpec},
    seed::{
        SeedConflictPolicy, SeedContext,
        rbac::{ADMIN_USER_ARTIFACT, ROLE_SUPER_ADMIN, ROLES_ARTIFACT, RoleIdMap},
    },
    types::{UserId, UserRoleId},
};

const SEED_ID: &str = "rbac.user_role.bootstrap";

const DEPENDS_ON: &[SeedDependency] = &[
    SeedDependency::Artifact(ROLES_ARTIFACT),
    SeedDependency::Artifact(ADMIN_USER_ARTIFACT),
];
const PRODUCES: &[SeedArtifact] = &[];

pub const USER_ROLE_SEED: SeedSpec = SeedSpec {
    id: SEED_ID,
    version: 1,
    target_table: user_role_table_name,
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::GraphOrdered,
    checksum: "rbac.user_role.bootstrap.v1",
    loader: load_boxed,
};

/// Assign `super_admin` to the bootstrap admin user.
pub async fn load(db: &dyn ConnectionTrait, ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let roles = ctx
        .require::<RoleIdMap>(ROLES_ARTIFACT)
        .map_err(|error| DbErr::Custom(error.to_string()))?;
    let super_admin_id = roles
        .get(ROLE_SUPER_ADMIN)
        .cloned()
        .ok_or_else(|| DbErr::Custom("super_admin role missing from seed context".to_owned()))?;
    let admin_id = ctx
        .require::<UserId>(ADMIN_USER_ARTIFACT)
        .map_err(|error| DbErr::Custom(error.to_string()))?
        .clone();

    let model = user_role::ActiveModel {
        id: Set(UserRoleId::new_v7()),
        user_id: Set(admin_id),
        role_id: Set(super_admin_id),
        ..Default::default()
    };

    let backend = db.get_database_backend();
    let stmt = user_role::Entity::insert(model)
        .on_conflict(
            OnConflict::columns([user_role::Column::UserId, user_role::Column::RoleId])
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
