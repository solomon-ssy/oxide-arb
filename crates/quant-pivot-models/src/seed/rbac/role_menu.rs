//! Grants built-in roles visibility of menu nodes aligned with Casbin policies.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};

use sea_orm::{
    ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait, QueryTrait, sea_query::OnConflict,
};

use crate::{
    entities::role_menu,
    enums::rbac::MenuKind,
    idens::role_menu::role_menu_table_name,
    schema::seed::{SeedArtifact, SeedDependency, SeedSpec},
    seed::{
        SeedConflictPolicy, SeedContext,
        rbac::{
            MENU_GRANTS_ARTIFACT, ROLE_SUPER_ADMIN, ROLES_ARTIFACT, RoleIdMap,
            casbin::builtin_role_policies, menus::MenuGrantSpec,
        },
    },
};

const SEED_ID: &str = "rbac.role_menu.bootstrap";

const DEPENDS_ON: &[SeedDependency] = &[
    SeedDependency::Artifact(ROLES_ARTIFACT),
    SeedDependency::Artifact(MENU_GRANTS_ARTIFACT),
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

fn policy_sets() -> HashMap<&'static str, HashSet<String>> {
    builtin_role_policies()
        .into_iter()
        .map(|(role_code, permissions)| {
            let codes = permissions
                .into_iter()
                .map(|(resource, operation)| {
                    format!("{}:{}", resource.as_str(), operation.as_str())
                })
                .collect();
            (role_code, codes)
        })
        .collect()
}

fn menu_granted(kind: MenuKind, permission_code: Option<&str>, policies: &HashSet<String>) -> bool {
    match kind {
        MenuKind::Directory => false,
        MenuKind::Menu => permission_code.is_none_or(|code| policies.contains(code)),
        MenuKind::Button => permission_code.is_some_and(|code| policies.contains(code)),
    }
}

/// Assign menu nodes to every built-in role (`super_admin` receives the full tree).
pub async fn load(db: &dyn ConnectionTrait, ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let roles = ctx
        .require::<RoleIdMap>(ROLES_ARTIFACT)
        .map_err(|error| DbErr::Custom(error.to_string()))?;
    let menu_grants = ctx
        .require::<Vec<MenuGrantSpec>>(MENU_GRANTS_ARTIFACT)
        .map_err(|error| DbErr::Custom(error.to_string()))?
        .clone();

    let policies_by_role = policy_sets();
    let mut models = Vec::new();

    for row in &menu_grants {
        for (role_code, role_id) in roles {
            let grant = if *role_code == ROLE_SUPER_ADMIN {
                true
            } else {
                let Some(policies) = policies_by_role.get(role_code) else {
                    continue;
                };
                menu_granted(row.kind, row.permission_code.as_deref(), policies)
            };

            if grant {
                models.push(role_menu::ActiveModel {
                    role_id: Set(role_id.clone()),
                    menu_id: Set(row.id.clone()),
                    ..Default::default()
                });
            }
        }
    }

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

#[cfg(test)]
mod tests {
    use super::{menu_granted, policy_sets};
    use crate::enums::rbac::MenuKind;

    #[test]
    fn viewer_policy_excludes_mutating_button_codes() {
        let sets = policy_sets();
        let policies = sets.get("viewer").expect("viewer policies");
        assert!(!menu_granted(
            MenuKind::Button,
            Some("market:update"),
            policies,
        ));
        assert!(menu_granted(MenuKind::Menu, Some("market:read"), policies));
    }
}
