//! Grants built-in roles visibility of menu nodes aligned with Casbin policies.

use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
};

use sea_orm::{
    ActiveValue::Set, ColumnTrait, DatabaseTransaction, DbErr, EntityTrait, QueryFilter,
    sea_query::OnConflict,
};

use crate::{
    entities::role_menu::{ActiveModel, Column, Entity},
    enums::rbac::MenuKind,
    seed::{
        SeedArtifact, SeedConflictPolicy, SeedContext, SeedDependency, SeedSpec,
        rbac::{
            MENU_GRANTS_ARTIFACT, ROLE_SUPER_ADMIN, ROLES_ARTIFACT, RoleIdMap,
            casbin::builtin_role_policies, menus::MenuGrantSpec,
        },
    },
    types::{MenuId, RoleId},
};

const SEED_ID: &str = "rbac.role_menu.bootstrap";

const DEPENDS_ON: &[SeedDependency] = &[
    SeedDependency::Artifact(ROLES_ARTIFACT),
    SeedDependency::Artifact(MENU_GRANTS_ARTIFACT),
];
const PRODUCES: &[SeedArtifact] = &[];

pub const ROLE_MENU_SEED: SeedSpec = SeedSpec {
    id: SEED_ID,
    // v4 binds grants to the hierarchical menu identity contract.
    version: 1,
    target_table: "role_menu",
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::GraphOrdered,
    checksum: "rbac.role_menu.bootstrap.v1.workspace-ia",
    apply: load_boxed,
    hydrate: hydrate_boxed,
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

impl SeedContext {
    /// Assign menu nodes to every built-in role (`super_admin` receives the full tree).
    fn expected_grants(&self) -> Result<Vec<(RoleId, MenuId)>, DbErr> {
        let roles = self
            .require::<RoleIdMap>(ROLES_ARTIFACT)
            .map_err(|error| DbErr::Custom(error.to_string()))?;
        let menu_grants = self
            .require::<Vec<MenuGrantSpec>>(MENU_GRANTS_ARTIFACT)
            .map_err(|error| DbErr::Custom(error.to_string()))?
            .clone();

        let policies_by_role = policy_sets();
        let mut grants = Vec::new();

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
                    grants.push((*role_id, row.id));
                }
            }
        }

        Ok(grants)
    }
}

pub async fn load(db: &DatabaseTransaction, ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let models = (ctx)
        .expected_grants()?
        .into_iter()
        .map(|(role_id, menu_id)| ActiveModel {
            role_id: Set(role_id),
            menu_id: Set(menu_id),
            ..Default::default()
        })
        .collect::<Vec<_>>();
    Entity::insert_many(models)
        .on_conflict(
            OnConflict::columns([Column::RoleId, Column::MenuId])
                .do_nothing()
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
}

async fn hydrate(db: &DatabaseTransaction, ctx: &SeedContext) -> Result<(), DbErr> {
    let expected = (ctx).expected_grants()?.into_iter().collect::<HashSet<_>>();
    let role_ids = ctx
        .require::<RoleIdMap>(ROLES_ARTIFACT)
        .map_err(|error| DbErr::Custom(error.to_string()))?
        .values()
        .copied()
        .collect::<Vec<_>>();
    let actual = Entity::find()
        .filter(Column::RoleId.is_in(role_ids))
        .all(db)
        .await?
        .into_iter()
        .map(|row| (row.role_id, row.menu_id))
        .collect::<HashSet<_>>();
    if actual != expected {
        return Err(DbErr::Custom(format!(
            "built-in role-menu seed drift: expected={} actual={}",
            expected.len(),
            actual.len()
        )));
    }
    Ok(())
}

fn load_boxed<'a>(
    db: &'a DatabaseTransaction,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<u64, DbErr>> + Send + 'a>> {
    Box::pin(load(db, ctx))
}

fn hydrate_boxed<'a>(
    db: &'a DatabaseTransaction,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<(), DbErr>> + Send + 'a>> {
    Box::pin(hydrate(db, ctx))
}

#[cfg(test)]
mod tests {
    use super::{menu_granted, policy_sets};
    use crate::enums::rbac::MenuKind;

    #[test]
    fn viewer_policy_excludes_codes() {
        let sets = policy_sets();
        let policies = sets.get("viewer").expect("viewer policies");
        assert!(!menu_granted(
            MenuKind::Button,
            Some("market:update"),
            policies,
        ));
        assert!(menu_granted(MenuKind::Menu, Some("market:read"), policies));
    }

    #[test]
    fn workspace_menu_action_policy() {
        let sets = policy_sets();
        let viewer = sets.get("viewer").expect("viewer policies");
        let risk_owner = sets.get("risk_owner").expect("risk-owner policies");

        // The consolidated research workspace remains visible to read-only
        // roles, while governed controls stay absent from their action set.
        assert!(menu_granted(
            MenuKind::Menu,
            Some("materialization:read"),
            viewer,
        ));
        assert!(!menu_granted(
            MenuKind::Button,
            Some("materialization:create"),
            viewer,
        ));
        assert!(menu_granted(
            MenuKind::Menu,
            Some("materialization:read"),
            risk_owner,
        ));
        assert!(menu_granted(
            MenuKind::Button,
            Some("materialization:create"),
            risk_owner,
        ));
        assert!(menu_granted(
            MenuKind::Button,
            Some("publication:publish"),
            risk_owner,
        ));
        assert!(!menu_granted(
            MenuKind::Button,
            Some("publication:publish"),
            viewer,
        ));
    }
}
