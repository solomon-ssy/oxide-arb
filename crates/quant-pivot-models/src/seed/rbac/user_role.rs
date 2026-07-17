//! Binds the bootstrap admin user to the `super_admin` role.

use crate::{
    entities::user_role,
    seed::{
        SeedArtifact, SeedConflictPolicy, SeedContext, SeedDependency, SeedSpec,
        rbac::{ADMIN_USER_ARTIFACT, ROLE_SUPER_ADMIN, ROLES_ARTIFACT, RoleIdMap},
    },
    types::UserId,
};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, DbErr, EntityTrait, QueryFilter, sea_query::OnConflict,
};
use std::{future::Future, pin::Pin};

const SEED_ID: &str = "rbac.user_role.bootstrap";

const DEPENDS_ON: &[SeedDependency] = &[
    SeedDependency::Artifact(ROLES_ARTIFACT),
    SeedDependency::Artifact(ADMIN_USER_ARTIFACT),
];
const PRODUCES: &[SeedArtifact] = &[];

pub const USER_ROLE_SEED: SeedSpec = SeedSpec {
    id: SEED_ID,
    version: 1,
    target_table: "user_role",
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::GraphOrdered,
    checksum: "rbac.user_role.bootstrap.v1",
    apply: load_boxed,
    hydrate: hydrate_boxed,
};

/// Assign `super_admin` to the bootstrap admin user.
pub async fn load(db: &sea_orm::DatabaseTransaction, ctx: &mut SeedContext) -> Result<u64, DbErr> {
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
        user_id: Set(admin_id),
        role_id: Set(super_admin_id),
        ..Default::default()
    };

    user_role::Entity::insert(model)
        .on_conflict(
            OnConflict::columns([user_role::Column::UserId, user_role::Column::RoleId])
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
}

async fn hydrate(db: &sea_orm::DatabaseTransaction, ctx: &SeedContext) -> Result<(), DbErr> {
    let roles = ctx
        .require::<RoleIdMap>(ROLES_ARTIFACT)
        .map_err(|error| DbErr::Custom(error.to_string()))?;
    let role_id = roles
        .get(ROLE_SUPER_ADMIN)
        .cloned()
        .ok_or_else(|| DbErr::Custom("super_admin role missing from seed context".to_owned()))?;
    let user_id = ctx
        .require::<UserId>(ADMIN_USER_ARTIFACT)
        .map_err(|error| DbErr::Custom(error.to_string()))?
        .clone();
    let exists = user_role::Entity::find()
        .filter(user_role::Column::UserId.eq(user_id))
        .filter(user_role::Column::RoleId.eq(role_id))
        .one(db)
        .await?
        .is_some();
    if !exists {
        return Err(DbErr::Custom(
            "bootstrap admin is not assigned to super_admin".to_owned(),
        ));
    }
    Ok(())
}

fn load_boxed<'a>(
    db: &'a sea_orm::DatabaseTransaction,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<u64, DbErr>> + Send + 'a>> {
    Box::pin(load(db, ctx))
}

fn hydrate_boxed<'a>(
    db: &'a sea_orm::DatabaseTransaction,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<(), DbErr>> + Send + 'a>> {
    Box::pin(hydrate(db, ctx))
}
