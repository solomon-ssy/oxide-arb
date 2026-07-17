//! Seeds the full navigation menu tree (directories, pages, button permissions).

use crate::{
    entities::menu,
    enums::rbac::{MenuKind, Operation, ResourceType, RoleStatus},
    seed::{
        SeedArtifact, SeedConflictPolicy, SeedContext, SeedDependency, SeedSpec,
        rbac::{MENU_GRANTS_ARTIFACT, MENUS_ARTIFACT},
    },
    types::MenuId,
};
use sea_orm::{
    ActiveValue::Set, ColumnTrait, DbErr, EntityTrait, QueryFilter, sea_query::OnConflict,
};
use std::{future::Future, pin::Pin};
use uuid::Uuid;

const SEED_ID: &str = "rbac.menus.bootstrap";

const DEPENDS_ON: &[SeedDependency] = &[];
const PRODUCES: &[SeedArtifact] = &[
    SeedArtifact::new(MENUS_ARTIFACT, SEED_ID),
    SeedArtifact::new(MENU_GRANTS_ARTIFACT, SEED_ID),
];

pub const MENUS_SEED: SeedSpec = SeedSpec {
    id: SEED_ID,
    // v17 fixes identity collisions for same-name nodes under different parents.
    // The loader is idempotent (`on_conflict do_nothing`).
    version: 18,
    target_table: "menu",
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::GraphOrdered,
    checksum: "rbac.menus.bootstrap.v18.governed-bootstrap-and-approval",
    apply: load_boxed,
    hydrate: hydrate_boxed,
};

/// Stable namespace for deterministic menu UUIDs (v5 over node `name`).
const fn menu_namespace() -> Uuid {
    Uuid::from_u128(0x0000_0040_0080_0000_0000_0000_0000_0001)
}

/// Format a `resource:operation` permission code from typed values.
fn perm(resource: ResourceType, operation: Operation) -> String {
    format!("{}:{}", resource.as_str(), operation.as_str())
}

fn stable_menu_id(parent: Option<&MenuId>, name: &str) -> MenuId {
    let identity = parent.map_or_else(
        || format!("root/{name}"),
        |parent_id| format!("{parent_id}/{name}"),
    );
    MenuId::new(Uuid::new_v5(&menu_namespace(), identity.as_bytes()))
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
    /// Route the SPA can navigate to but never renders in the sidebar (detail
    /// pages). `Button` nodes are always hidden regardless of this flag.
    hide_in_menu: bool,
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
            hide_in_menu: false,
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
        let id = stable_menu_id(spec.parent, spec.name);
        let sort = self.next_sort;
        self.next_sort += 1;
        let hide_in_menu = spec.hide_in_menu || matches!(spec.kind, MenuKind::Button);
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
        self.push_page(spec, false, false)
    }

    fn page_affixed(&mut self, spec: PageSpec<'_>) -> MenuId {
        self.push_page(spec, true, false)
    }

    /// A navigable route that never appears in the sidebar (e.g. a detail page
    /// reached from a table row). Parent it under a directory, not another
    /// page, so it renders as a sibling route rather than nesting inside the
    /// parent page's component.
    fn page_hidden(&mut self, spec: PageSpec<'_>) -> MenuId {
        self.push_page(spec, false, true)
    }

    fn push_page(&mut self, spec: PageSpec<'_>, affix_tab: bool, hide_in_menu: bool) -> MenuId {
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
            hide_in_menu,
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
    build_command_center(&mut t);
    build_trading(&mut t);
    build_execution(&mut t);
    build_research(&mut t);
    build_governance(&mut t);
    build_access_control(&mut t);
    build_audit(&mut t);
    t
}

/// Command center: the operator首屏 dashboard. System control button permissions
/// are canonical here (header state lights + dashboard quick actions); UI
/// visibility still comes from the role's global access-code set.
fn build_command_center(t: &mut MenuTree) {
    let command_center = t.dir(
        "command-center",
        "page.menu.group.commandCenter",
        "lucide:layout-dashboard",
    );
    let dashboard = t.page_affixed(PageSpec {
        parent: &command_center,
        name: "dashboard",
        title: "page.menu.dashboard",
        path: "/dashboard",
        component: "dashboard/index",
        permission_code: None,
        icon: "lucide:home",
    });
    t.button(
        &dashboard,
        "system:switch_mode",
        "Switch Runtime Mode",
        perm(ResourceType::System, Operation::SwitchMode),
    );
    t.button(
        &dashboard,
        "system:bootstrap_activate",
        "Activate Bootstrap",
        perm(ResourceType::System, Operation::BootstrapActivate),
    );
    t.button(
        &dashboard,
        "system:halt",
        "Halt",
        perm(ResourceType::System, Operation::Halt),
    );
    t.button(
        &dashboard,
        "system:resume",
        "Resume",
        perm(ResourceType::System, Operation::Resume),
    );
    t.button(
        &dashboard,
        "system:emergency",
        "Emergency Halt",
        perm(ResourceType::System, Operation::Emergency),
    );
}

/// Trading plane: market registry + the `RecommendationReport` primary artifact.
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
        "Subscribe / Block",
        perm(ResourceType::Market, Operation::Update),
    );
    // Full-screen market detail (live book + microstructure charts), reached
    // from a table row — navigable but hidden from the sidebar.
    t.page_hidden(PageSpec {
        parent: &trading,
        name: "market-detail",
        title: "page.menu.marketDetail",
        path: "/markets/:id",
        component: "markets/detail/index",
        permission_code: Some(perm(ResourceType::Market, Operation::Read)),
        icon: "lucide:line-chart",
    });
    let quant_reports = t.page(PageSpec {
        parent: &trading,
        name: "quant-reports",
        title: "page.menu.quantReports",
        path: "/quant/reports",
        component: "quant/reports/index",
        permission_code: Some(perm(ResourceType::QuantReport, Operation::Read)),
        icon: "lucide:bar-chart-3",
    });
    t.button(
        &quant_reports,
        "quant_report:enqueue",
        "Run Ad-hoc Report",
        perm(ResourceType::QuantReport, Operation::Enqueue),
    );
    t.button(
        &quant_reports,
        "quant_report:revoke",
        "Revoke Report",
        perm(ResourceType::QuantReport, Operation::Revoke),
    );
    // Full-screen report detail (overview + summary, ranked recommendations,
    // structural diff), reached from the report list — navigable but hidden.
    t.page_hidden(PageSpec {
        parent: &trading,
        name: "report-detail",
        title: "page.menu.reportDetail",
        path: "/quant/reports/:id",
        component: "quant/reports/detail/index",
        permission_code: Some(perm(ResourceType::QuantReport, Operation::Read)),
        icon: "lucide:file-text",
    });
    // Full-screen recommendation detail (score / plans / factors / evidence /
    // attribution), deep-linkable and reached from a report's recommendations.
    t.page_hidden(PageSpec {
        parent: &trading,
        name: "recommendation-detail",
        title: "page.menu.recommendationDetail",
        path: "/quant/recommendations/:id",
        component: "quant/recommendations/detail",
        permission_code: Some(perm(ResourceType::QuantReport, Operation::Read)),
        icon: "lucide:target",
    });
    // Structural Alpha dashboard (Phase 11.2.1+): trade-tape participant
    // concentration, source coverage, and neg-risk leg-sum drift.
    t.page(PageSpec {
        parent: &trading,
        name: "structural-monitor",
        title: "page.menu.structuralMonitor",
        path: "/quant/structural",
        component: "quant/structural/index",
        permission_code: Some(perm(ResourceType::QuantReport, Operation::Read)),
        icon: "lucide:git-compare-arrows",
    });
}

/// Execution plane: intent审批台, CLOB submission ledger, system-lot positions,
/// reconciliation queue, settlement redeems, and the live venue account.
fn build_execution(t: &mut MenuTree) {
    let execution = t.dir("execution", "page.menu.group.execution", "lucide:zap");
    let intents = t.page(PageSpec {
        parent: &execution,
        name: "order-intents",
        title: "page.menu.orderIntents",
        path: "/quant/intents",
        component: "quant/intents/index",
        permission_code: Some(perm(ResourceType::OrderIntent, Operation::Read)),
        icon: "lucide:list-checks",
    });
    t.button(
        &intents,
        "order_intent:create",
        "Create Intent",
        perm(ResourceType::OrderIntent, Operation::Create),
    );
    t.button(
        &intents,
        "order_intent:approve",
        "Approve Intent",
        perm(ResourceType::OrderIntent, Operation::Approve),
    );
    t.button(
        &intents,
        "order_intent:reject",
        "Reject Intent",
        perm(ResourceType::OrderIntent, Operation::Reject),
    );
    t.button(
        &intents,
        "order_intent:cancel",
        "Cancel Intent",
        perm(ResourceType::OrderIntent, Operation::Cancel),
    );
    t.page(PageSpec {
        parent: &execution,
        name: "execution-orders",
        title: "page.menu.executionOrders",
        path: "/quant/execution-orders",
        component: "quant/execution-orders/index",
        permission_code: Some(perm(ResourceType::ExecutionOrder, Operation::Read)),
        icon: "lucide:receipt",
    });
    t.page(PageSpec {
        parent: &execution,
        name: "positions",
        title: "page.menu.positions",
        path: "/quant/positions",
        component: "quant/positions/index",
        permission_code: Some(perm(ResourceType::Position, Operation::Read)),
        icon: "lucide:layers",
    });
    let reconciliations = t.page(PageSpec {
        parent: &execution,
        name: "reconciliations",
        title: "page.menu.reconciliations",
        path: "/quant/reconciliations",
        component: "quant/reconciliations/index",
        permission_code: Some(perm(ResourceType::Reconciliation, Operation::Read)),
        icon: "lucide:scale",
    });
    t.button(
        &reconciliations,
        "reconciliation:resolve",
        "Resolve",
        perm(ResourceType::Reconciliation, Operation::Resolve),
    );
    t.page(PageSpec {
        parent: &execution,
        name: "settlement-redeems",
        title: "page.menu.settlementRedeems",
        path: "/quant/settlement-redeems",
        component: "quant/settlement-redeems/index",
        permission_code: Some(perm(ResourceType::SettlementRedeem, Operation::Read)),
        icon: "lucide:banknote",
    });
    t.page(PageSpec {
        parent: &execution,
        name: "account",
        title: "page.menu.account",
        path: "/quant/account",
        component: "quant/account/index",
        permission_code: Some(perm(ResourceType::AccountSnapshot, Operation::Read)),
        icon: "lucide:wallet",
    });
    // Full-screen intent detail (frozen entry / exit policy / risk envelope,
    // approval + admission trace, linked execution orders), deep-linkable and
    // reached from the approval console or a create-intent handoff.
    t.page_hidden(PageSpec {
        parent: &execution,
        name: "order-intent-detail",
        title: "page.menu.orderIntentDetail",
        path: "/quant/intents/:id",
        component: "quant/intents/detail/index",
        permission_code: Some(perm(ResourceType::OrderIntent, Operation::Read)),
        icon: "lucide:list-checks",
    });
}

/// Model-spec catalog — the offline research lifecycle root (spec → dataset → version).
fn build_research_model_specs(t: &mut MenuTree, research: &MenuId) {
    let model_specs = t.page(PageSpec {
        parent: research,
        name: "research-model-specs",
        title: "page.menu.researchModelSpecs",
        path: "/research/model-specs",
        component: "research/model-specs/index",
        permission_code: Some(perm(ResourceType::Materialization, Operation::Read)),
        icon: "lucide:file-cog",
    });
    t.button(
        &model_specs,
        "materialization:create",
        "Create Model Spec",
        perm(ResourceType::Materialization, Operation::Create),
    );
}

/// Governed trade-policy fitting and publication workbench.
fn build_research_trade_policies(t: &mut MenuTree, research: &MenuId) {
    let trade_policies = t.page(PageSpec {
        parent: research,
        name: "research-trade-policies",
        title: "page.menu.researchTradePolicies",
        path: "/research/trade-policies",
        component: "research/trade-policies/index",
        permission_code: Some(perm(ResourceType::Materialization, Operation::Read)),
        icon: "lucide:route",
    });
    t.button(
        &trade_policies,
        "materialization:create",
        "Fit / Validate Trade Policy",
        perm(ResourceType::Materialization, Operation::Create),
    );
    t.button(
        &trade_policies,
        "publication:publish",
        "Publish Trade Policy",
        perm(ResourceType::Publication, Operation::Publish),
    );
    t.button(
        &trade_policies,
        "publication:retire",
        "Retire Trade Policy",
        perm(ResourceType::Publication, Operation::Retire),
    );
}

/// Research plane: real catalog pages backing the operator workbench —
/// training-dataset ledger, trained-model registry, backtest reports, and factor
/// governance. Each page pages a `GET /research/*` list endpoint (10.5 §2);
/// governed mutations (plan/build/train/backtest/publish/rollback/retire) are
/// button permissions on the page they belong to. The pairwise comparison report
/// is a deep-linkable detail reached from a backtest (hidden from the sidebar).
fn build_research(t: &mut MenuTree) {
    let research = t.dir(
        "research",
        "page.menu.group.research",
        "lucide:flask-conical",
    );
    build_research_model_specs(t, &research);
    build_research_trade_policies(t, &research);
    let datasets = t.page(PageSpec {
        parent: &research,
        name: "research-datasets",
        title: "page.menu.researchDatasets",
        path: "/research/datasets",
        component: "research/datasets/index",
        permission_code: Some(perm(ResourceType::Materialization, Operation::Read)),
        icon: "lucide:database",
    });
    t.button(
        &datasets,
        "materialization:create",
        "Plan / Build Dataset",
        perm(ResourceType::Materialization, Operation::Create),
    );
    let models = t.page(PageSpec {
        parent: &research,
        name: "research-models",
        title: "page.menu.researchModels",
        path: "/research/models",
        component: "research/models/index",
        permission_code: Some(perm(ResourceType::Materialization, Operation::Read)),
        icon: "lucide:brain-circuit",
    });
    t.button(
        &models,
        "replay:create",
        "Run Backtest",
        perm(ResourceType::Replay, Operation::Create),
    );
    t.button(
        &models,
        "publication:publish",
        "Publish Model",
        perm(ResourceType::Publication, Operation::Publish),
    );
    t.button(
        &models,
        "publication:rollback",
        "Rollback Model",
        perm(ResourceType::Publication, Operation::Rollback),
    );
    t.button(
        &models,
        "publication:retire",
        "Retire Model",
        perm(ResourceType::Publication, Operation::Retire),
    );
    t.page(PageSpec {
        parent: &research,
        name: "research-backtests",
        title: "page.menu.researchBacktests",
        path: "/research/backtests",
        component: "research/backtests/index",
        permission_code: Some(perm(ResourceType::Replay, Operation::Read)),
        icon: "lucide:line-chart",
    });
    build_research_jobs(t, &research);
    build_research_feature_integrity(t, &research);
    let factors = t.page(PageSpec {
        parent: &research,
        name: "research-factors",
        title: "page.menu.researchFactors",
        path: "/research/factors",
        component: "research/factors/index",
        permission_code: Some(perm(ResourceType::FactorDefinition, Operation::Read)),
        icon: "lucide:sigma",
    });
    t.button(
        &factors,
        "factor_definition:publish",
        "Publish Factor",
        perm(ResourceType::FactorDefinition, Operation::Publish),
    );
    t.button(
        &factors,
        "factor_definition:retire",
        "Retire Factor",
        perm(ResourceType::FactorDefinition, Operation::Retire),
    );
    build_research_calibration_artifacts(t, &research);
    build_research_domain_governance(t, &research);
    // Pairwise comparison report — deep-linkable from a backtest, hidden from
    // the sidebar (parented under the directory, not another page).
    t.page_hidden(PageSpec {
        parent: &research,
        name: "research-comparison-detail",
        title: "page.menu.researchComparisonDetail",
        path: "/research/comparisons/:id",
        component: "research/comparisons/detail",
        permission_code: Some(perm(ResourceType::Replay, Operation::Read)),
        icon: "lucide:git-compare",
    });
}

/// Deterministic serving/replay diagnostics and governed parity recovery.
fn build_research_feature_integrity(t: &mut MenuTree, research: &MenuId) {
    let feature_integrity = t.page(PageSpec {
        parent: research,
        name: "research-feature-integrity",
        title: "page.menu.researchFeatureIntegrity",
        path: "/research/feature-integrity",
        component: "research/feature-integrity/index",
        permission_code: Some(perm(ResourceType::Materialization, Operation::Read)),
        icon: "lucide:scan-search",
    });
    t.button(
        &feature_integrity,
        "feature_integrity:govern",
        "Run Full Parity / Clear Latch",
        perm(ResourceType::Materialization, Operation::Create),
    );
}

/// Unified calibration-artifact catalog (Phase 11.3 §3.4): content-addressed
/// `market_price_bias` and `model_score` artifacts fit via governed research
/// jobs; bias tables activate into runtime config, model calibrators bind to
/// model versions.
fn build_research_calibration_artifacts(t: &mut MenuTree, research: &MenuId) {
    let calibration_artifacts = t.page(PageSpec {
        parent: research,
        name: "research-calibration-artifacts",
        title: "page.menu.researchCalibrationArtifacts",
        path: "/research/calibration-artifacts",
        component: "research/calibration-artifacts/index",
        permission_code: Some(perm(ResourceType::Materialization, Operation::Read)),
        icon: "lucide:scale",
    });
    t.button(
        &calibration_artifacts,
        "materialization:create",
        "Fit Calibration Artifact",
        perm(ResourceType::Materialization, Operation::Create),
    );
    t.button(
        &calibration_artifacts,
        "runtime_config:create",
        "Activate Bias Table",
        perm(ResourceType::RuntimeConfig, Operation::Create),
    );
}

/// Market-linkage ledger + domain-source ingest health (Phase 11.2.2).
fn build_research_domain_governance(t: &mut MenuTree, research: &MenuId) {
    let linkages = t.page(PageSpec {
        parent: research,
        name: "research-market-linkages",
        title: "page.menu.researchMarketLinkages",
        path: "/research/market-linkages",
        component: "research/market-linkages/index",
        permission_code: Some(perm(ResourceType::Materialization, Operation::Read)),
        icon: "lucide:link-2",
    });
    t.button(
        &linkages,
        "materialization:create",
        "Resolve / Override Linkage",
        perm(ResourceType::Materialization, Operation::Create),
    );
    t.page(PageSpec {
        parent: research,
        name: "research-domain-sources",
        title: "page.menu.researchDomainSources",
        path: "/research/domain-sources",
        component: "research/domain-sources/index",
        permission_code: Some(perm(ResourceType::Materialization, Operation::Read)),
        icon: "lucide:activity",
    });
    t.page(PageSpec {
        parent: research,
        name: "research-basis-alerts",
        title: "page.menu.researchBasisAlerts",
        path: "/research/basis-alerts",
        component: "research/basis-alerts/index",
        permission_code: Some(perm(ResourceType::Materialization, Operation::Read)),
        icon: "lucide:triangle-alert",
    });
}

/// Task center: the durable async research-job engine (dataset build / model
/// train / backtest) — live progress, cancel, retry, and crash-recovery status.
fn build_research_jobs(t: &mut MenuTree, research: &MenuId) {
    t.page(PageSpec {
        parent: research,
        name: "research-jobs",
        title: "page.menu.researchJobs",
        path: "/research/jobs",
        component: "research/jobs/index",
        permission_code: Some(perm(ResourceType::Materialization, Operation::Read)),
        icon: "lucide:list-checks",
    });
}

/// Governance plane: runtime-config version lifecycle (hot-activatable config).
fn build_governance(t: &mut MenuTree) {
    let governance = t.dir(
        "governance",
        "page.menu.group.governance",
        "lucide:settings-2",
    );
    let runtime_config = t.page(PageSpec {
        parent: &governance,
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
        "runtime_config:approve",
        "Approve Version",
        perm(ResourceType::RuntimeConfig, Operation::Approve),
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

/// Audit trail: the operation log is the single generic audit entry point.
fn build_audit(t: &mut MenuTree) {
    let audit = t.dir("audit-trail", "page.menu.group.audit", "lucide:file-search");
    t.page(PageSpec {
        parent: &audit,
        name: "operation-log",
        title: "page.menu.operationLog",
        path: "/operation-log",
        component: "operation-log/index",
        permission_code: Some(perm(ResourceType::OperationLog, Operation::Read)),
        icon: "lucide:scroll-text",
    });
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
pub async fn load(db: &sea_orm::DatabaseTransaction, ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let tree = build_tree();

    let rows_affected = menu::Entity::insert_many(tree.models)
        .on_conflict(OnConflict::column(menu::Column::Id).do_nothing().to_owned())
        .exec_without_returning(db)
        .await?;

    let _ = ctx;
    Ok(rows_affected)
}

async fn hydrate(db: &sea_orm::DatabaseTransaction, ctx: &mut SeedContext) -> Result<(), DbErr> {
    let tree = build_tree();
    let rows = menu::Entity::find()
        .filter(menu::Column::Id.is_in(tree.ids.clone()))
        .all(db)
        .await?;
    if rows.len() != tree.ids.len() {
        return Err(DbErr::Custom(format!(
            "menu seed expected {} rows; found {}",
            tree.ids.len(),
            rows.len()
        )));
    }
    for expected in &tree.grants {
        let row = rows
            .iter()
            .find(|row| row.id == expected.id)
            .ok_or_else(|| DbErr::Custom(format!("seeded menu {} is missing", expected.id)))?;
        if row.kind != expected.kind
            || row.permission_code != expected.permission_code
            || row.status != RoleStatus::Enabled
        {
            return Err(DbErr::Custom(format!(
                "seeded menu {} differs from seed contract",
                expected.id
            )));
        }
    }
    ctx.put(MENUS_ARTIFACT, tree.ids);
    ctx.put(MENU_GRANTS_ARTIFACT, tree.grants);
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

#[cfg(test)]
mod tests {
    use super::{build_tree, stable_menu_id};
    use crate::enums::rbac::MenuKind;
    use std::collections::HashSet;

    #[test]
    fn menu_ids_are_stable_for_node_name() {
        let first = stable_menu_id(None, "markets");
        let second = stable_menu_id(None, "markets");
        assert_eq!(first, second);
    }

    #[test]
    fn same_name_under_different_parents_has_distinct_id() {
        let left = stable_menu_id(None, "left");
        let right = stable_menu_id(None, "right");
        assert_ne!(
            stable_menu_id(Some(&left), "create"),
            stable_menu_id(Some(&right), "create")
        );
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
        // Endgame leftovers removed in Phase 04.4.
        assert!(!names.contains("trades"));
        assert!(!names.contains("risk"));
        assert!(!names.contains("risk-overview"));
        assert!(!names.contains("blacklist"));
        // Phase 10.0 removals: old operations/analytics/audit surfaces.
        assert!(!names.contains("analytics"));
        assert!(!names.contains("audit"));
        assert!(!names.contains("publications"));
        assert!(!names.contains("replay"));
        assert!(!names.contains("quant-models"));
        assert!(!names.contains("operations"));
        // Stale button name replaced by the canonical permission code.
        assert!(!names.contains("quant_report:run"));
        assert!(!names.contains("quant_model:reject"));
        // Phase 10.5: the single ID-driven workbench is replaced by real catalogs.
        assert!(!names.contains("research-workbench"));
    }

    #[test]
    fn seed_tree_has_phase_10_execution_pages_and_dashboard_system_buttons() {
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
        for expected in [
            "order-intents",
            "order-intent-detail",
            "execution-orders",
            "positions",
            "reconciliations",
            "settlement-redeems",
            "account",
            "research-model-specs",
            "research-datasets",
            "research-models",
            "research-backtests",
            "research-jobs",
            "research-feature-integrity",
            "feature_integrity:govern",
            "research-factors",
            "report-detail",
            "recommendation-detail",
            "quant_report:enqueue",
            "system:switch_mode",
            "system:halt",
            "system:resume",
            "system:emergency",
        ] {
            assert!(names.contains(expected), "missing menu node `{expected}`");
        }
        assert!(
            !names.contains("system"),
            "legacy /system page must be removed"
        );
    }

    #[test]
    fn feature_integrity_page_uses_materialization_read_and_real_component() {
        let tree = build_tree();
        let page = tree
            .models
            .iter()
            .find(|model| {
                matches!(
                    &model.name,
                    sea_orm::ActiveValue::Set(name) if name == "research-feature-integrity"
                )
            })
            .expect("feature-integrity menu page");
        assert_eq!(
            page.permission_code,
            sea_orm::ActiveValue::Set(Some("materialization:read".to_owned()))
        );
        assert_eq!(
            page.path,
            sea_orm::ActiveValue::Set(Some("/research/feature-integrity".to_owned()))
        );
        assert_eq!(
            page.component,
            sea_orm::ActiveValue::Set(Some("research/feature-integrity/index".to_owned()))
        );
        assert_eq!(page.hide_in_menu, sea_orm::ActiveValue::Set(false));

        let action = tree
            .models
            .iter()
            .find(|model| {
                matches!(
                    &model.name,
                    sea_orm::ActiveValue::Set(name) if name == "feature_integrity:govern"
                )
            })
            .expect("feature-integrity governed action");
        assert_eq!(
            action.parent_id,
            sea_orm::ActiveValue::Set(Some(stable_menu_id(
                Some(&stable_menu_id(None, "research")),
                "research-feature-integrity",
            )))
        );
        assert_eq!(
            action.permission_code,
            sea_orm::ActiveValue::Set(Some("materialization:create".to_owned()))
        );
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
    fn dashboard_is_under_command_center() {
        let tree = build_tree();
        let command_center = stable_menu_id(None, "command-center");
        let dashboard = tree
            .models
            .iter()
            .find(|model| {
                matches!(&model.name, sea_orm::ActiveValue::Set(name) if name == "dashboard")
            })
            .expect("dashboard menu node");
        assert_eq!(
            dashboard.parent_id,
            sea_orm::ActiveValue::Set(Some(command_center))
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
