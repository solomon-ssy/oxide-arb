//! Seeds the built-in RBAC roles and publishes their IDs to the context.

use crate::{
    entities::role,
    enums::rbac::{RoleKind, RoleStatus},
    seed::{
        SeedArtifact, SeedConflictPolicy, SeedContext, SeedDependency, SeedSpec,
        rbac::{
            ROLE_ADMIN, ROLE_ANALYST, ROLE_EMERGENCY_OPERATOR, ROLE_OPERATOR, ROLE_RISK_OWNER,
            ROLE_SUPER_ADMIN, ROLE_VIEWER, ROLES_ARTIFACT, RoleIdMap,
        },
    },
    types::{RoleCode, RoleId},
};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, DbErr, EntityTrait, QueryFilter, sea_query::OnConflict,
};
use std::{future::Future, pin::Pin};

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
        ROLE_ANALYST,
        "Analyst",
        "Read-only access plus ad-hoc recommendation report generation.",
        25,
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
    target_table: "role",
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::GraphOrdered,
    checksum: "rbac.roles.bootstrap.v1",
    apply: load_boxed,
    hydrate: hydrate_boxed,
};

/// Insert built-in roles and publish the `code -> RoleId` map to the context.
pub async fn load(db: &sea_orm::DatabaseTransaction, ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let mut models = Vec::with_capacity(BUILTIN_ROLES.len());
    for (code, name, description, sort) in BUILTIN_ROLES {
        let id = RoleId::from_v7();
        models.push(role::ActiveModel {
            id: Set(id),
            code: Set(RoleCode::new(*code)),
            name: Set((*name).to_owned()),
            description: Set(Some((*description).to_owned())),
            kind: Set(RoleKind::Builtin),
            status: Set(RoleStatus::Enabled),
            sort: Set(*sort),
            ..Default::default()
        });
    }

    let rows_affected = role::Entity::insert_many(models)
        .on_conflict(
            OnConflict::column(role::Column::Code)
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(db)
        .await?;

    let _ = ctx;
    Ok(rows_affected)
}

async fn hydrate(db: &sea_orm::DatabaseTransaction, ctx: &mut SeedContext) -> Result<(), DbErr> {
    let codes = BUILTIN_ROLES
        .iter()
        .map(|(code, _, _, _)| *code)
        .collect::<Vec<_>>();
    let rows = role::Entity::find()
        .filter(role::Column::Code.is_in(codes))
        .all(db)
        .await?;
    if rows.len() != BUILTIN_ROLES.len() {
        return Err(DbErr::Custom(format!(
            "built-in role seed expected {} rows; found {}",
            BUILTIN_ROLES.len(),
            rows.len()
        )));
    }

    let mut id_map = RoleIdMap::new();
    for (code, name, description, sort) in BUILTIN_ROLES {
        let row = rows
            .iter()
            .find(|row| row.code.as_str() == *code)
            .ok_or_else(|| DbErr::Custom(format!("built-in role `{code}` is missing")))?;
        if row.name != *name
            || row.description.as_deref() != Some(*description)
            || row.kind != RoleKind::Builtin
            || row.status != RoleStatus::Enabled
            || row.sort != *sort
        {
            return Err(DbErr::Custom(format!(
                "built-in role `{code}` differs from seed contract"
            )));
        }
        id_map.insert(*code, row.id.clone());
    }
    ctx.put(ROLES_ARTIFACT, id_map);
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
