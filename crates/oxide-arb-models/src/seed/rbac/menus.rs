//! Seeds the full navigation menu tree (directories, pages, button permissions).

use std::{future::Future, pin::Pin};

use sea_orm::{
    ActiveValue::Set, ConnectionTrait, DbErr, EntityTrait, QueryTrait, sea_query::OnConflict,
};

use crate::{
    entities::menu,
    enums::rbac::{MenuKind, Operation, ResourceType, RoleStatus},
    idens::menu::menu_table_name,
    schema::seed::{SeedArtifact, SeedDependency, SeedSpec},
    seed::{SeedConflictPolicy, SeedContext, rbac::MENUS_ARTIFACT},
    types::MenuId,
};

const SEED_ID: &str = "rbac.menus.bootstrap";

const DEPENDS_ON: &[SeedDependency] = &[];
const PRODUCES: &[SeedArtifact] = &[SeedArtifact::new(MENUS_ARTIFACT, SEED_ID)];

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

/// Format a `resource:operation` permission code from typed values.
fn perm(resource: ResourceType, operation: Operation) -> String {
    format!("{}:{}", resource.as_str(), operation.as_str())
}

/// Accumulates menu rows while assigning stable monotonic sort keys per parent.
struct MenuTree {
    models: Vec<menu::ActiveModel>,
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
            ids: Vec::new(),
            next_sort: 0,
        }
    }

    fn push(&mut self, spec: NodeSpec<'_>) -> MenuId {
        let id = MenuId::new_v7();
        let sort = self.next_sort;
        self.next_sort += 1;
        let hide_in_menu = matches!(spec.kind, MenuKind::Button);
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

    /// Top-level directory node.
    fn dir(&mut self, name: &str, title: &str, icon: &str) -> MenuId {
        self.push(NodeSpec {
            kind: MenuKind::Directory,
            name,
            title,
            icon: Some(icon),
            ..NodeSpec::default()
        })
    }

    /// Page node under a directory, gated by a read permission.
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

    /// Action button under a page, gated by a mutating permission point.
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
    build_governance(&mut t);
    build_analytics(&mut t);
    build_replay(&mut t);
    build_system(&mut t);
    build_access_control(&mut t);
    t
}

fn build_dashboard(t: &mut MenuTree) {
    let dashboard_root = t.dir("dashboard_root", "Dashboard", "dashboard");
    t.page(
        &dashboard_root,
        "dashboard",
        "Overview",
        "/dashboard",
        "dashboard/index",
        None,
    );
}

fn build_trading(t: &mut MenuTree) {
    let trading = t.dir("trading", "Trading", "trading");
    let markets = t.page(
        &trading,
        "markets",
        "Markets",
        "/markets",
        "markets/index",
        Some(perm(ResourceType::Market, Operation::Read)),
    );
    t.button(
        &markets,
        "markets:subscribe",
        "Subscribe / Unsubscribe",
        perm(ResourceType::Market, Operation::Update),
    );
    t.page(
        &trading,
        "opportunities",
        "Opportunities",
        "/opportunities",
        "opportunities/index",
        Some(perm(ResourceType::Opportunity, Operation::Read)),
    );
    t.page(
        &trading,
        "trades",
        "Trades",
        "/trades",
        "trades/index",
        Some(perm(ResourceType::Trade, Operation::Read)),
    );
    t.page(
        &trading,
        "pnl",
        "PnL",
        "/pnl",
        "pnl/index",
        Some(perm(ResourceType::Pnl, Operation::Read)),
    );
}

fn build_risk(t: &mut MenuTree) {
    let risk = t.dir("risk", "Risk", "risk");
    let risk_overview = t.page(
        &risk,
        "risk-overview",
        "Risk Overview",
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
        "Blacklist",
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

fn build_governance(t: &mut MenuTree) {
    let governance = t.dir("governance", "Governance", "governance");
    let control_factors = t.page(
        &governance,
        "control-factors",
        "Control Factors",
        "/control-factors",
        "control-factors/index",
        Some(perm(ResourceType::ControlFactor, Operation::Read)),
    );
    t.button(
        &control_factors,
        "control-factor:reject",
        "Reject",
        perm(ResourceType::ControlFactor, Operation::Reject),
    );
    t.button(
        &control_factors,
        "control-factor:shadow",
        "Promote to Shadow",
        perm(ResourceType::ControlFactor, Operation::Shadow),
    );
    t.button(
        &control_factors,
        "control-factor:publish",
        "Publish",
        perm(ResourceType::ControlFactor, Operation::Publish),
    );
    t.button(
        &control_factors,
        "control-factor:emergency",
        "Emergency Publish",
        perm(ResourceType::ControlFactor, Operation::Emergency),
    );
    let publications = t.page(
        &governance,
        "publications",
        "Publications",
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
    let materializations = t.page(
        &governance,
        "materializations",
        "Materializations",
        "/materializations",
        "materializations/index",
        Some(perm(ResourceType::ControlFactor, Operation::Read)),
    );
    t.button(
        &materializations,
        "materialization:enqueue",
        "Enqueue Run",
        perm(ResourceType::Materialization, Operation::Enqueue),
    );
    let runtime_config = t.page(
        &governance,
        "runtime-config",
        "Runtime Config",
        "/runtime-config",
        "runtime-config/index",
        Some(perm(ResourceType::RuntimeConfig, Operation::Read)),
    );
    t.button(
        &runtime_config,
        "runtime-config:create",
        "Create Version",
        perm(ResourceType::RuntimeConfig, Operation::Create),
    );
    t.button(
        &runtime_config,
        "runtime-config:activate",
        "Activate Version",
        perm(ResourceType::RuntimeConfig, Operation::Activate),
    );
    t.button(
        &runtime_config,
        "runtime-config:rollback",
        "Rollback Version",
        perm(ResourceType::RuntimeConfig, Operation::Rollback),
    );
    t.page(
        &governance,
        "audit",
        "Audit Chain",
        "/audit",
        "audit/index",
        Some(perm(ResourceType::Audit, Operation::Read)),
    );
}

fn build_analytics(t: &mut MenuTree) {
    let analytics_root = t.dir("analytics-root", "Analytics", "analytics");
    t.page(
        &analytics_root,
        "analytics",
        "Analytics",
        "/analytics",
        "analytics/index",
        Some(perm(ResourceType::Analytics, Operation::Read)),
    );
}

fn build_replay(t: &mut MenuTree) {
    let replay_dir = t.dir("replay-root", "Replay", "replay");
    let replay = t.page(
        &replay_dir,
        "replay",
        "Replay",
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
}

fn build_system(t: &mut MenuTree) {
    let system = t.dir("system", "System", "system");
    let system_control = t.page(
        &system,
        "system-control",
        "System Control",
        "/system",
        "system/index",
        Some(perm(ResourceType::System, Operation::Read)),
    );
    t.button(
        &system_control,
        "system:halt",
        "Halt",
        perm(ResourceType::System, Operation::Halt),
    );
    t.button(
        &system_control,
        "system:resume",
        "Resume",
        perm(ResourceType::System, Operation::Resume),
    );
    t.button(
        &system_control,
        "system:switch-mode",
        "Switch Mode",
        perm(ResourceType::System, Operation::SwitchMode),
    );
    t.page(
        &system,
        "operation-log",
        "Operation Log",
        "/operation-log",
        "operation-log/index",
        Some(perm(ResourceType::OperationLog, Operation::Read)),
    );
}

fn build_access_control(t: &mut MenuTree) {
    let access = t.dir("access-control", "Access Control", "access");
    let users = t.page(
        &access,
        "users",
        "Users",
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
        "Roles",
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
    let menus_page = t.page(
        &access,
        "menus",
        "Menus",
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
    t.page(
        &access,
        "permissions",
        "Permissions",
        "/permissions",
        "permissions/index",
        Some(perm(ResourceType::Permission, Operation::Read)),
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
    Ok(result.rows_affected())
}

fn load_boxed<'a>(
    db: &'a dyn ConnectionTrait,
    ctx: &'a mut SeedContext,
) -> Pin<Box<dyn Future<Output = Result<u64, DbErr>> + Send + 'a>> {
    Box::pin(load(db, ctx))
}
