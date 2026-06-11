//! Seeds the full navigation menu tree (directories, pages, button permissions).

use std::{future::Future, pin::Pin};

use sea_orm::{
    ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait, QueryTrait, sea_query::OnConflict,
};
use uuid::Uuid;

use crate::{
    entities::menu,
    enums::rbac::{MenuKind, Operation, ResourceType, RoleStatus},
    idens::menu::menu_table_name,
    schema::seed::{SeedArtifact, SeedDependency, SeedSpec},
    seed::{
        SeedConflictPolicy, SeedContext,
        rbac::{MENU_GRANTS_ARTIFACT, MENUS_ARTIFACT},
    },
    types::MenuId,
};

const SEED_ID: &str = "rbac.menus.bootstrap";

const DEPENDS_ON: &[SeedDependency] = &[];
const PRODUCES: &[SeedArtifact] = &[
    SeedArtifact::new(MENUS_ARTIFACT, SEED_ID),
    SeedArtifact::new(MENU_GRANTS_ARTIFACT, SEED_ID),
];

pub const MENUS_SEED: SeedSpec = SeedSpec {
    id: SEED_ID,
    version: 1,
    target_table: menu_table_name,
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::GraphOrdered,
    checksum: "rbac.menus.bootstrap.v1",
    loader: load_boxed,
};

/// Stable namespace for deterministic menu UUIDs (v5 over node `name`).
fn menu_namespace() -> Uuid {
    Uuid::from_u128(0x0000_0040_0080_0000_0000_0000_0000_0001)
}

/// Format a `resource:operation` permission code from typed values.
fn perm(resource: ResourceType, operation: Operation) -> String {
    format!("{}:{}", resource.as_str(), operation.as_str())
}

fn stable_menu_id(name: &str) -> MenuId {
    MenuId::new(Uuid::new_v5(&menu_namespace(), name.as_bytes()))
}

/// Minimal menu projection for role-menu assignment seeds.
#[derive(Debug, Clone)]
pub struct MenuGrantSpec {
    /// Stable menu id.
    pub id: MenuId,
    /// Structural kind.
    pub kind: MenuKind,
    /// Optional Casbin permission gate.
    pub permission_code: Option<String>,
}

/// Accumulates menu rows while assigning stable monotonic sort keys per parent.
struct MenuTree {
    models: Vec<menu::ActiveModel>,
    grants: Vec<MenuGrantSpec>,
    ids: Vec<MenuId>,
    next_sort: i32,
}

/// Specification for one menu node, consumed by [`MenuTree::push`].
struct NodeSpec<'a> {
    parent: Option<&'a MenuId>,
    kind: MenuKind,
    name: &'a str,
    title: &'a str,
    path: Option<&'a str>,
    component: Option<&'a str>,
    icon: Option<&'a str>,
    permission_code: Option<String>,
}

impl Default for NodeSpec<'_> {
    fn default() -> Self {
        Self {
            parent: None,
            kind: MenuKind::Menu,
            name: "",
            title: "",
            path: None,
            component: None,
            icon: None,
            permission_code: None,
        }
    }
}

impl MenuTree {
    const fn new() -> Self {
        Self {
            models: Vec::new(),
            grants: Vec::new(),
            ids: Vec::new(),
            next_sort: 0,
        }
    }

    fn push(&mut self, spec: NodeSpec<'_>) -> MenuId {
        let id = stable_menu_id(spec.name);
        let sort = self.next_sort;
        self.next_sort += 1;
        let hide_in_menu = matches!(spec.kind, MenuKind::Button);
        let permission_code = spec.permission_code.clone();
        self.grants.push(MenuGrantSpec {
            id: id.clone(),
            kind: spec.kind,
            permission_code: permission_code.clone(),
        });
        self.models.push(menu::ActiveModel {
            id: Set(id.clone()),
            parent_id: Set(spec.parent.cloned()),
            name: Set(spec.name.to_owned()),
            kind: Set(spec.kind),
            path: Set(spec.path.map(str::to_owned)),
            component: Set(spec.component.map(str::to_owned)),
            title: Set(spec.title.to_owned()),
            icon: Set(spec.icon.map(str::to_owned)),
            permission_code: Set(spec.permission_code),
            sort: Set(sort),
            keep_alive: Set(false),
            hide_in_menu: Set(hide_in_menu),
            status: Set(RoleStatus::Enabled),
            ..Default::default()
        });
        self.ids.push(id.clone());
        id
    }

    fn dir(&mut self, name: &str, title: &str, icon: &str) -> MenuId {
        self.push(NodeSpec {
            kind: MenuKind::Directory,
            name,
            title,
            icon: Some(icon),
            ..NodeSpec::default()
        })
    }

    fn page(
        &mut self,
        parent: &MenuId,
        name: &str,
        title: &str,
        path: &str,
        component: &str,
        permission_code: Option<String>,
    ) -> MenuId {
        self.push(NodeSpec {
            parent: Some(parent),
            kind: MenuKind::Menu,
            name,
            title,
            path: Some(path),
            component: Some(component),
            permission_code,
            ..NodeSpec::default()
        })
    }

    fn button(&mut self, parent: &MenuId, name: &str, title: &str, permission_code: String) {
        self.push(NodeSpec {
            parent: Some(parent),
            kind: MenuKind::Button,
            name,
            title,
            permission_code: Some(permission_code),
            ..NodeSpec::default()
        });
    }
}

fn build_tree() -> MenuTree {
    let mut t = MenuTree::new();
    build_dashboard(&mut t);
    build_trading(&mut t);
    build_risk(&mut t);
    build_analytics(&mut t);
    build_operations(&mut t);
    build_access_control(&mut t);
    t
}

fn build_dashboard(t: &mut MenuTree) {
    let dashboard_root = t.dir(
        "dashboard_root",
        "page.menu.group.dashboard",
        "dashboard",
    );
    let overview = t.page(
        &dashboard_root,
        "dashboard",
        "page.menu.dashboard",
        "/dashboard",
        "dashboard/index",
        None,
    );
    t.button(
        &overview,
        "system:halt",
        "Halt",
        perm(ResourceType::System, Operation::Halt),
    );
    t.button(
        &overview,
        "system:resume",
        "Resume",
        perm(ResourceType::System, Operation::Resume),
    );
    t.button(
        &overview,
        "system:switch_mode",
        "Switch Mode",
        perm(ResourceType::System, Operation::SwitchMode),
    );
}

fn build_trading(t: &mut MenuTree) {
    let trading = t.dir("trading", "page.menu.group.trading", "trading");
    let markets = t.page(
        &trading,
        "markets",
        "page.menu.markets",
        "/markets",
        "markets/index",
        Some(perm(ResourceType::Market, Operation::Read)),
    );
    t.button(
        &markets,
        "market:update",
        "Subscribe / Unsubscribe",
        perm(ResourceType::Market, Operation::Update),
    );
    t.page(
        &trading,
        "opportunities",
        "page.menu.opportunities",
        "/opportunities",
        "opportunities/index",
        Some(perm(ResourceType::Opportunity, Operation::Read)),
    );
    t.page(
        &trading,
        "trades",
        "page.menu.trades",
        "/trades",
        "trades/index",
        Some(perm(ResourceType::Trade, Operation::Read)),
    );
}

fn build_risk(t: &mut MenuTree) {
    let risk = t.dir("risk", "page.menu.group.risk", "risk");
    let risk_overview = t.page(
        &risk,
        "risk-overview",
        "page.menu.risk",
        "/risk",
        "risk/index",
        Some(perm(ResourceType::Risk, Operation::Read)),
    );
    t.button(
        &risk_overview,
        "risk:reset",
        "Reset Circuit Breaker",
        perm(ResourceType::Risk, Operation::Reset),
    );
    let blacklist = t.page(
        &risk,
        "blacklist",
        "page.menu.blacklist",
        "/blacklist",
        "blacklist/index",
        Some(perm(ResourceType::Blacklist, Operation::Read)),
    );
    t.button(
        &blacklist,
        "blacklist:create",
        "Add Entry",
        perm(ResourceType::Blacklist, Operation::Create),
    );
    t.button(
        &blacklist,
        "blacklist:delete",
        "Remove Entry",
        perm(ResourceType::Blacklist, Operation::Delete),
    );
}

fn build_analytics(t: &mut MenuTree) {
    let analytics_root = t.dir(
        "analytics-root",
        "page.menu.group.analytics",
        "analytics",
    );
    t.page(
        &analytics_root,
        "analytics",
        "page.menu.analytics",
        "/analytics",
        "analytics/index",
        Some(perm(ResourceType::Analytics, Operation::Read)),
    );
}

fn build_operations(t: &mut MenuTree) {
    let operations = t.dir(
        "operations",
        "page.menu.group.operations",
        "governance",
    );
    let runtime_config = t.page(
        &operations,
        "runtime-config",
        "page.menu.runtimeConfig",
        "/runtime-config",
        "runtime-config/index",
        Some(perm(ResourceType::RuntimeConfig, Operation::Read)),
    );
    t.button(
        &runtime_config,
        "runtime_config:create",
        "Create Version",
        perm(ResourceType::RuntimeConfig, Operation::Create),
    );
    t.button(
        &runtime_config,
        "runtime_config:activate",
        "Activate Version",
        perm(ResourceType::RuntimeConfig, Operation::Activate),
    );
    t.button(
        &runtime_config,
        "runtime_config:rollback",
        "Rollback Version",
        perm(ResourceType::RuntimeConfig, Operation::Rollback),
    );
    let control_factors = t.page(
        &operations,
        "control-factors",
        "page.menu.controlFactors",
        "/control-factors",
        "control-factors/index",
        Some(perm(ResourceType::ControlFactor, Operation::Read)),
    );
    t.button(
        &control_factors,
        "control_factor:reject",
        "Reject",
        perm(ResourceType::ControlFactor, Operation::Reject),
    );
    t.button(
        &control_factors,
        "control_factor:shadow",
        "Promote to Shadow",
        perm(ResourceType::ControlFactor, Operation::Shadow),
    );
    t.button(
        &control_factors,
        "control_factor:publish",
        "Publish",
        perm(ResourceType::ControlFactor, Operation::Publish),
    );
    t.button(
        &control_factors,
        "control_factor:emergency",
        "Emergency Publish",
        perm(ResourceType::ControlFactor, Operation::Emergency),
    );
    let publications = t.page(
        &operations,
        "publications",
        "page.menu.publications",
        "/publications",
        "publications/index",
        Some(perm(ResourceType::ControlFactor, Operation::Read)),
    );
    t.button(
        &publications,
        "publication:rollback",
        "Rollback",
        perm(ResourceType::Publication, Operation::Rollback),
    );
    let replay = t.page(
        &operations,
        "replay",
        "page.menu.replay",
        "/replay",
        "replay/index",
        Some(perm(ResourceType::Replay, Operation::Read)),
    );
    t.button(
        &replay,
        "replay:create",
        "Start Replay",
        perm(ResourceType::Replay, Operation::Create),
    );
    t.page(
        &operations,
        "audit",
        "page.menu.audit",
        "/audit",
        "audit/index",
        Some(perm(ResourceType::Audit, Operation::Read)),
    );
    t.page(
        &operations,
        "operation-log",
        "page.menu.operationLog",
        "/operation-log",
        "operation-log/index",
        Some(perm(ResourceType::OperationLog, Operation::Read)),
    );
}

fn build_access_control(t: &mut MenuTree) {
    let access = t.dir(
        "access-control",
        "page.menu.group.accessControl",
        "access",
    );
    let users = t.page(
        &access,
        "users",
        "page.menu.users",
        "/users",
        "users/index",
        Some(perm(ResourceType::User, Operation::Read)),
    );
    t.button(
        &users,
        "user:create",
        "Create User",
        perm(ResourceType::User, Operation::Create),
    );
    t.button(
        &users,
        "user:update",
        "Edit User",
        perm(ResourceType::User, Operation::Update),
    );
    t.button(
        &users,
        "user:delete",
        "Delete User",
        perm(ResourceType::User, Operation::Delete),
    );
    t.button(
        &users,
        "user:assign",
        "Assign Roles",
        perm(ResourceType::User, Operation::Assign),
    );
    let roles_page = t.page(
        &access,
        "roles",
        "page.menu.roles",
        "/roles",
        "roles/index",
        Some(perm(ResourceType::Role, Operation::Read)),
    );
    t.button(
        &roles_page,
        "role:create",
        "Create Role",
        perm(ResourceType::Role, Operation::Create),
    );
    t.button(
        &roles_page,
        "role:update",
        "Edit Role",
        perm(ResourceType::Role, Operation::Update),
    );
    t.button(
        &roles_page,
        "role:delete",
        "Delete Role",
        perm(ResourceType::Role, Operation::Delete),
    );
    t.button(
        &roles_page,
        "role:assign",
        "Assign Permissions / Menus",
        perm(ResourceType::Role, Operation::Assign),
    );
    t.button(
        &roles_page,
        "permission:read",
        "View Permission Catalog",
        perm(ResourceType::Permission, Operation::Read),
    );
    let menus_page = t.page(
        &access,
        "menus",
        "page.menu.menus",
        "/menus",
        "menus/index",
        Some(perm(ResourceType::Menu, Operation::Read)),
    );
    t.button(
        &menus_page,
        "menu:create",
        "Create Menu",
        perm(ResourceType::Menu, Operation::Create),
    );
    t.button(
        &menus_page,
        "menu:update",
        "Edit Menu",
        perm(ResourceType::Menu, Operation::Update),
    );
    t.button(
        &menus_page,
        "menu:delete",
        "Delete Menu",
        perm(ResourceType::Menu, Operation::Delete),
    );
}

/// Insert the menu tree and publish all menu IDs to the context.
pub async fn load(db: &dyn ConnectionTrait, ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let tree = build_tree();
    let ids = tree.ids.clone();

    let backend = db.get_database_backend();
    let stmt = menu::Entity::insert_many(tree.models)
        .on_conflict(OnConflict::column(menu::Column::Id).do_nothing().to_owned())
        .build(backend);
    let result = db.execute(stmt).await?;

    ctx.put(MENUS_ARTIFACT, ids);
    ctx.put(MENU_GRANTS_ARTIFACT, tree.grants);
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
    use std::collections::HashSet;

    use super::{build_tree, stable_menu_id};

    #[test]
    fn menu_ids_are_stable_for_node_name() {
        let first = stable_menu_id("markets");
        let second = stable_menu_id("markets");
        assert_eq!(first, second);
    }

    #[test]
    fn seed_tree_has_no_removed_pages() {
        let tree = build_tree();
        let names: HashSet<_> = tree
            .models
            .iter()
            .map(|model| {
                if let sea_orm::ActiveValue::Set(name) = &model.name {
                    name.clone()
                } else {
                    String::new()
                }
            })
            .collect();
        assert!(!names.contains("pnl"));
        assert!(!names.contains("materializations"));
        assert!(!names.contains("system-control"));
        assert!(!names.contains("permissions"));
    }
}
