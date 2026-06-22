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
    version: 5,
    target_table: menu_table_name,
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::GraphOrdered,
    checksum: "rbac.menus.bootstrap.v5",
    loader: load_boxed,
};

/// Stable namespace for deterministic menu UUIDs (v5 over node `name`).
const fn menu_namespace() -> Uuid {
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
    affix_tab: bool,
}

/// Route page fields for [`MenuTree::page`].
struct PageSpec<'a> {
    parent: &'a MenuId,
    name: &'a str,
    title: &'a str,
    path: &'a str,
    component: &'a str,
    permission_code: Option<String>,
    icon: &'a str,
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
            affix_tab: false,
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
        self.grants.push(MenuGrantSpec {
            id: id.clone(),
            kind: spec.kind,
            permission_code: spec.permission_code.clone(),
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
            affix_tab: Set(spec.affix_tab),
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

    fn page(&mut self, spec: PageSpec<'_>) -> MenuId {
        self.push_page(spec, false)
    }

    fn page_affixed(&mut self, spec: PageSpec<'_>) -> MenuId {
        self.push_page(spec, true)
    }

    fn push_page(&mut self, spec: PageSpec<'_>, affix_tab: bool) -> MenuId {
        self.push(NodeSpec {
            parent: Some(spec.parent),
            kind: MenuKind::Menu,
            name: spec.name,
            title: spec.title,
            path: Some(spec.path),
            component: Some(spec.component),
            icon: Some(spec.icon),
            permission_code: spec.permission_code,
            affix_tab,
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
    build_operations(&mut t);
    build_access_control(&mut t);
    t
}

fn build_dashboard(t: &mut MenuTree) {
    let dashboard_root = t.dir(
        "dashboard_root",
        "page.menu.group.dashboard",
        "lucide:layout-dashboard",
    );
    let overview = t.page_affixed(PageSpec {
        parent: &dashboard_root,
        name: "dashboard",
        title: "page.menu.dashboard",
        path: "/dashboard",
        component: "dashboard/index",
        permission_code: None,
        icon: "lucide:home",
    });
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
    t.page(PageSpec {
        parent: &dashboard_root,
        name: "analytics",
        title: "page.menu.analytics",
        path: "/analytics",
        component: "analytics/index",
        permission_code: Some(perm(ResourceType::Analytics, Operation::Read)),
        icon: "lucide:line-chart",
    });
}

fn build_trading(t: &mut MenuTree) {
    let trading = t.dir("trading", "page.menu.group.trading", "lucide:trending-up");
    let markets = t.page(PageSpec {
        parent: &trading,
        name: "markets",
        title: "page.menu.markets",
        path: "/markets",
        component: "markets/index",
        permission_code: Some(perm(ResourceType::Market, Operation::Read)),
        icon: "lucide:store",
    });
    t.button(
        &markets,
        "market:update",
        "Subscribe / Unsubscribe",
        perm(ResourceType::Market, Operation::Update),
    );
    t.page(PageSpec {
        parent: &trading,
        name: "quant-reports",
        title: "page.menu.quantReports",
        path: "/quant/reports",
        component: "quant/reports/index",
        permission_code: Some(perm(ResourceType::QuantReport, Operation::Read)),
        icon: "lucide:bar-chart-3",
    });
    let trades_page = t.page(PageSpec {
        parent: &trading,
        name: "trades",
        title: "page.menu.trades",
        path: "/trades",
        component: "trades/index",
        permission_code: Some(perm(ResourceType::Trade, Operation::Read)),
        icon: "lucide:receipt",
    });
    t.button(
        &trades_page,
        "trade:reconcile",
        "page.menu.tradeReconcile",
        perm(ResourceType::Trade, Operation::Update),
    );
}

fn build_risk(t: &mut MenuTree) {
    let risk = t.dir("risk", "page.menu.group.risk", "lucide:shield");
    let risk_overview = t.page(PageSpec {
        parent: &risk,
        name: "risk-overview",
        title: "page.menu.risk",
        path: "/risk",
        component: "risk/index",
        permission_code: Some(perm(ResourceType::Risk, Operation::Read)),
        icon: "lucide:shield-alert",
    });
    t.button(
        &risk_overview,
        "risk:reset",
        "Reset Circuit Breaker",
        perm(ResourceType::Risk, Operation::Reset),
    );
    let blacklist = t.page(PageSpec {
        parent: &risk,
        name: "blacklist",
        title: "page.menu.blacklist",
        path: "/blacklist",
        component: "blacklist/index",
        permission_code: Some(perm(ResourceType::Blacklist, Operation::Read)),
        icon: "lucide:ban",
    });
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

fn build_operations(t: &mut MenuTree) {
    let operations = t.dir(
        "operations",
        "page.menu.group.operations",
        "lucide:settings-2",
    );
    build_operations_runtime_config(t, &operations);
    let quant_models = t.page(PageSpec {
        parent: &operations,
        name: "quant-models",
        title: "page.menu.quantModels",
        path: "/quant/models",
        component: "quant/models/index",
        permission_code: Some(perm(ResourceType::Publication, Operation::Read)),
        icon: "lucide:git-branch",
    });
    t.button(
        &quant_models,
        "quant_model:reject",
        "Reject",
        perm(ResourceType::Publication, Operation::Reject),
    );
    t.button(
        &quant_models,
        "quant_model:shadow",
        "Promote to Shadow",
        perm(ResourceType::Publication, Operation::Shadow),
    );
    t.button(
        &quant_models,
        "quant_model:publish",
        "Publish",
        perm(ResourceType::Publication, Operation::Publish),
    );
    t.button(
        &quant_models,
        "quant_model:emergency",
        "Emergency Publish",
        perm(ResourceType::Publication, Operation::Emergency),
    );
    let publications = t.page(PageSpec {
        parent: &operations,
        name: "publications",
        title: "page.menu.publications",
        path: "/publications",
        component: "publications/index",
        permission_code: Some(perm(ResourceType::ControlFactor, Operation::Read)),
        icon: "lucide:rocket",
    });
    t.button(
        &publications,
        "publication:rollback",
        "Rollback",
        perm(ResourceType::Publication, Operation::Rollback),
    );
    let replay = t.page(PageSpec {
        parent: &operations,
        name: "replay",
        title: "page.menu.replay",
        path: "/replay",
        component: "replay/index",
        permission_code: Some(perm(ResourceType::Replay, Operation::Read)),
        icon: "lucide:history",
    });
    t.button(
        &replay,
        "replay:create",
        "Start Replay",
        perm(ResourceType::Replay, Operation::Create),
    );
    t.page(PageSpec {
        parent: &operations,
        name: "audit",
        title: "page.menu.audit",
        path: "/audit",
        component: "audit/index",
        permission_code: Some(perm(ResourceType::Audit, Operation::Read)),
        icon: "lucide:file-search",
    });
    t.page(PageSpec {
        parent: &operations,
        name: "operation-log",
        title: "page.menu.operationLog",
        path: "/operation-log",
        component: "operation-log/index",
        permission_code: Some(perm(ResourceType::OperationLog, Operation::Read)),
        icon: "lucide:scroll-text",
    });
}

/// Runtime-config page + version-lifecycle buttons under `operations`.
fn build_operations_runtime_config(t: &mut MenuTree, operations: &MenuId) {
    let runtime_config = t.page(PageSpec {
        parent: operations,
        name: "runtime-config",
        title: "page.menu.runtimeConfig",
        path: "/runtime-config",
        component: "runtime-config/index",
        permission_code: Some(perm(ResourceType::RuntimeConfig, Operation::Read)),
        icon: "lucide:sliders-horizontal",
    });
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
}

fn build_access_control(t: &mut MenuTree) {
    let access = t.dir(
        "access-control",
        "page.menu.group.accessControl",
        "lucide:lock",
    );
    build_access_control_users(t, &access);
    build_access_control_roles(t, &access);
    build_access_control_menus(t, &access);
}

fn build_access_control_users(t: &mut MenuTree, access: &MenuId) {
    let users = t.page(PageSpec {
        parent: access,
        name: "users",
        title: "page.menu.users",
        path: "/users",
        component: "users/index",
        permission_code: Some(perm(ResourceType::User, Operation::Read)),
        icon: "lucide:users",
    });
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
}

fn build_access_control_roles(t: &mut MenuTree, access: &MenuId) {
    let roles_page = t.page(PageSpec {
        parent: access,
        name: "roles",
        title: "page.menu.roles",
        path: "/roles",
        component: "roles/index",
        permission_code: Some(perm(ResourceType::Role, Operation::Read)),
        icon: "lucide:key-round",
    });
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
}

fn build_access_control_menus(t: &mut MenuTree, access: &MenuId) {
    let menus_page = t.page(PageSpec {
        parent: access,
        name: "menus",
        title: "page.menu.menus",
        path: "/menus",
        component: "menus/index",
        permission_code: Some(perm(ResourceType::Menu, Operation::Read)),
        icon: "lucide:menu",
    });
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
    use crate::enums::rbac::MenuKind;

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
        assert!(!names.contains("analytics-root"));
    }

    #[test]
    fn dashboard_overview_is_affixed_by_default() {
        let tree = build_tree();
        let dashboard = tree
            .models
            .iter()
            .find(|model| {
                matches!(&model.name, sea_orm::ActiveValue::Set(name) if name == "dashboard")
            })
            .expect("dashboard menu node");
        assert_eq!(dashboard.affix_tab, sea_orm::ActiveValue::Set(true),);
    }

    #[test]
    fn analytics_page_is_under_dashboard() {
        let tree = build_tree();
        let dashboard_root = stable_menu_id("dashboard_root");
        let analytics = tree
            .models
            .iter()
            .find(|model| {
                matches!(&model.name, sea_orm::ActiveValue::Set(name) if name == "analytics")
            })
            .expect("analytics menu node");
        assert_eq!(
            analytics.parent_id,
            sea_orm::ActiveValue::Set(Some(dashboard_root))
        );
    }

    #[test]
    fn directory_and_menu_nodes_use_iconify_icons() {
        let tree = build_tree();
        for model in &tree.models {
            let kind = match &model.kind {
                sea_orm::ActiveValue::Set(kind) => *kind,
                _ => continue,
            };
            if !matches!(kind, MenuKind::Directory | MenuKind::Menu) {
                continue;
            }
            let icon = match &model.icon {
                sea_orm::ActiveValue::Set(Some(icon)) => icon.as_str(),
                _ => panic!("menu node missing icon"),
            };
            assert!(
                icon.contains(':'),
                "icon must be Iconify collection:name format"
            );
        }
    }
}
