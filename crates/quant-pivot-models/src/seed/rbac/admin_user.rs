//! Seeds the bootstrap admin user from a deploy-supplied Argon2id hash.

use std::{future::Future, pin::Pin};

use sea_orm::{
    ActiveValue::Set, ColumnTrait, DbErr, EntityTrait, QueryFilter, sea_query::OnConflict,
};

use crate::{
    entities::user,
    enums::rbac::UserStatus,
    seed::{
        SeedArtifact, SeedConflictPolicy, SeedContext, SeedDependency, SeedSpec,
        rbac::{
            ADMIN_USER_ARTIFACT, BOOTSTRAP_ADMIN_PASSWORD_HASH_INPUT, DEFAULT_ADMIN_NICKNAME,
            DEFAULT_ADMIN_USERNAME, ROLES_ARTIFACT,
        },
    },
    types::UserId,
};

const SEED_ID: &str = "rbac.admin_user.bootstrap";

const DEPENDS_ON: &[SeedDependency] = &[SeedDependency::Artifact(ROLES_ARTIFACT)];
const PRODUCES: &[SeedArtifact] = &[SeedArtifact::new(ADMIN_USER_ARTIFACT, SEED_ID)];

pub const ADMIN_USER_SEED: SeedSpec = SeedSpec {
    id: SEED_ID,
    version: 2,
    target_table: "user",
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::GraphOrdered,
    checksum: "rbac.admin_user.bootstrap.v2.deploy-secret",
    apply: load_boxed,
    hydrate: hydrate_boxed,
};

/// Insert the bootstrap admin user and publish its `UserId` to the context.
pub async fn load(db: &sea_orm::DatabaseTransaction, ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let id = UserId::from_v7();
    let password_hash = ctx
        .require::<String>(BOOTSTRAP_ADMIN_PASSWORD_HASH_INPUT)
        .map_err(|error| DbErr::Custom(error.to_string()))?
        .clone();

    let model = user::ActiveModel {
        id: Set(id.clone()),
        username: Set(DEFAULT_ADMIN_USERNAME.to_owned()),
        password_hash: Set(password_hash),
        nickname: Set(DEFAULT_ADMIN_NICKNAME.to_owned()),
        avatar: Set(None),
        email: Set(None),
        phone: Set(None),
        status: Set(UserStatus::Active),
        ..Default::default()
    };

    let rows_affected = user::Entity::insert(model)
        .on_conflict(
            OnConflict::column(user::Column::Username)
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(db)
        .await?;

    Ok(rows_affected)
}

async fn hydrate(db: &sea_orm::DatabaseTransaction, ctx: &mut SeedContext) -> Result<(), DbErr> {
    let row = user::Entity::find()
        .filter(user::Column::Username.eq(DEFAULT_ADMIN_USERNAME))
        .one(db)
        .await?
        .ok_or_else(|| DbErr::Custom("bootstrap admin user is missing".to_owned()))?;
    if row.nickname != DEFAULT_ADMIN_NICKNAME || row.status != UserStatus::Active {
        return Err(DbErr::Custom(
            "bootstrap admin user differs from seed contract".to_owned(),
        ));
    }
    ctx.put(ADMIN_USER_ARTIFACT, row.id);
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
