//! Seeds the bootstrap admin user (argon2id-hashed password).

use std::{future::Future, pin::Pin};

use sea_orm::{
    ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait, QueryTrait, sea_query::OnConflict,
};

use crate::{
    entities::user,
    enums::rbac::UserStatus,
    idens::user::user_table_name,
    schema::seed::{SeedArtifact, SeedDependency, SeedSpec},
    security::hash_password,
    seed::{
        SeedConflictPolicy, SeedContext,
        rbac::{
            ADMIN_USER_ARTIFACT, DEFAULT_ADMIN_NICKNAME, DEFAULT_ADMIN_PASSWORD,
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
    version: 1,
    target_table: user_table_name,
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::GraphOrdered,
    checksum: "rbac.admin_user.bootstrap.v1",
    loader: load_boxed,
};

/// Insert the bootstrap admin user and publish its `UserId` to the context.
pub async fn load(db: &dyn ConnectionTrait, ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let id = UserId::new_v7();
    let password_hash =
        hash_password(DEFAULT_ADMIN_PASSWORD).map_err(|error| DbErr::Custom(error.to_string()))?;

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

    let backend = db.get_database_backend();
    let stmt = user::Entity::insert(model)
        .on_conflict(
            OnConflict::column(user::Column::Username)
                .do_nothing()
                .to_owned(),
        )
        .build(backend);
    let result = db.execute(stmt).await?;

    ctx.put(ADMIN_USER_ARTIFACT, id);
    Ok(result.rows_affected())
}

fn load_boxed<'a>(
    db: &'a dyn ConnectionTrait,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<u64, DbErr>> + Send + 'a>> {
    Box::pin(load(db, ctx))
}
