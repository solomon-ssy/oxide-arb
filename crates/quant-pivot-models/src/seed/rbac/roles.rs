//! Seeds the six built-in RBAC roles and publishes their IDs to the context.

use std::{future::Future, pin::Pin};

use sea_orm::{
    ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait, QueryTrait, sea_query::OnConflict,
};

use crate::{
    entities::role,
    enums::rbac::{RoleKind, RoleStatus},
    idens::role::role_table_name,
    schema::seed::{SeedArtifact, SeedDependency, SeedSpec},
    seed::{
        SeedConflictPolicy, SeedContext,
        rbac::{
            ROLE_ADMIN, ROLE_EMERGENCY_OPERATOR, ROLE_OPERATOR, ROLE_RISK_OWNER, ROLE_SUPER_ADMIN,
            ROLE_VIEWER, ROLES_ARTIFACT, RoleIdMap,
        },
    },
    types::RoleId,
};

const SEED_ID: &str = "rbac.roles.bootstrap";

const DEPENDS_ON: &[SeedDependency] = &[];
const PRODUCES: &[SeedArtifact] = &[SeedArtifact::new(ROLES_ARTIFACT, SEED_ID)];

/// Built-in roles: `(code, display name, description, sort)`.
const BUILTIN_ROLES: &[(&str, &str, &str, i32)] = &[
    (
        ROLE_SUPER_ADMIN,
        "Super Administrator",
        "Full system access; bypasses all permission checks.",
        0,
    ),
    (
        ROLE_ADMIN,
        "Administrator",
        "Platform and RBAC administration.",
        10,
    ),
    (
        ROLE_RISK_OWNER,
        "Risk Owner",
        "Governance and money-risk approval authority.",
        20,
    ),
    (
        ROLE_OPERATOR,
        "Operator",
        "Day-to-day operational controls.",
        30,
    ),
    (ROLE_VIEWER, "Viewer", "Read-only access.", 40),
    (
        ROLE_EMERGENCY_OPERATOR,
        "Emergency Operator",
        "Break-glass emergency controls.",
        50,
    ),
];

pub const ROLES_SEED: SeedSpec = SeedSpec {
    id: SEED_ID,
    version: 1,
    target_table: role_table_name,
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::GraphOrdered,
    checksum: "rbac.roles.bootstrap.v1",
    loader: load_boxed,
};

/// Insert built-in roles and publish the `code -> RoleId` map to the context.
pub async fn load(db: &dyn ConnectionTrait, ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let mut id_map = RoleIdMap::new();
    let mut models = Vec::with_capacity(BUILTIN_ROLES.len());
    for (code, name, description, sort) in BUILTIN_ROLES {
        let id = RoleId::from_v7();
        id_map.insert(*code, id.clone());
        models.push(role::ActiveModel {
            id: Set(id),
            code: Set((*code).to_owned()),
            name: Set((*name).to_owned()),
            description: Set(Some((*description).to_owned())),
            kind: Set(RoleKind::Builtin),
            status: Set(RoleStatus::Enabled),
            sort: Set(*sort),
            ..Default::default()
        });
    }

    let backend = db.get_database_backend();
    let stmt = role::Entity::insert_many(models)
        .on_conflict(
            OnConflict::column(role::Column::Code)
                .do_nothing()
                .to_owned(),
        )
        .build(backend);
    let result = db.execute(stmt).await?;

    ctx.put(ROLES_ARTIFACT, id_map);
    Ok(result.rows_affected())
}

fn load_boxed<'a>(
    db: &'a dyn ConnectionTrait,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<u64, DbErr>> + Send + 'a>> {
    Box::pin(load(db, ctx))
}
