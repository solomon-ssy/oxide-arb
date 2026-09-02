//! Seeds the canonical five-domain workspace navigation and action permissions.

use std::{future::Future, pin::Pin};

use sea_orm::{
    ActiveValue::Set, ColumnTrait, DatabaseTransaction, DbErr, EntityTrait, QueryFilter,
    sea_query::OnConflict,
};
use uuid::Uuid;

use crate::{
    entities::menu::{ActiveModel, Column, Entity},
    enums::rbac::{MenuKind, Operation, ResourceType, RoleStatus},
    seed::{
        SeedArtifact, SeedConflictPolicy, SeedContext, SeedDependency, SeedSpec,
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
    target_table: "menu",
    depends_on: DEPENDS_ON,
    produces: PRODUCES,
    conflict_policy: SeedConflictPolicy::GraphOrdered,
    checksum: "rbac.menus.bootstrap.v1.workspace-ia.i18n-actions",
    apply: load_boxed,
    hydrate: hydrate_boxed,
};

/// Stable namespace for deterministic menu UUIDs (v5 over the hierarchical name).
const fn menu_namespace() -> Uuid {
    Uuid::from_u128(0x0000_0040_0080_0000_0000_0000_0000_0001)
}

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

/// Minimal menu projection consumed by the derived role-menu seed.
#[derive(Debug, Clone)]
pub struct MenuGrantSpec {
    /// Stable menu id.
    pub id: MenuId,
    /// Structural kind.
    pub kind: MenuKind,
    /// Optional Casbin permission gate.
    pub permission_code: Option<String>,
}

struct MenuTree {
    models: Vec<ActiveModel>,
    grants: Vec<MenuGrantSpec>,
    ids: Vec<MenuId>,
    next_sort: i32,
}

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
    hide_in_menu: bool,
}

struct PageSpec<'a> {
    parent: &'a MenuId,
    name: &'a str,
    title: &'a str,
    path: &'a str,
    component: &'a str,
    permission_code: Option<String>,
    icon: &'a str,
}

struct ActionSpec<'a> {
    name: &'a str,
    title: &'a str,
    resource: ResourceType,
    operation: Operation,
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
    fn bootstrap() -> Self {
        let mut tree = Self::new();
        tree.build_command_center();
        tree.build_trading_signals();
        tree.build_execution_capital();
        tree.build_research_models();
        tree.build_system_governance();
        tree
    }

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
            id,
            kind: spec.kind,
            permission_code: spec.permission_code.clone(),
        });
        self.models.push(ActiveModel {
            id: Set(id),
            parent_id: Set(spec.parent.copied()),
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
        self.ids.push(id);
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

    fn affixed_page(&mut self, spec: PageSpec<'_>) -> MenuId {
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
            hide_in_menu: false,
        })
    }

    fn buttons(&mut self, parent: &MenuId, specs: &[ActionSpec<'_>]) {
        for spec in specs {
            self.push(NodeSpec {
                parent: Some(parent),
                kind: MenuKind::Button,
                name: spec.name,
                title: spec.title,
                permission_code: Some(perm(spec.resource, spec.operation)),
                ..NodeSpec::default()
            });
        }
    }

    fn build_command_center(&mut self) {
        let directory = self.dir(
            "command-center",
            "page.menu.group.commandCenter",
            "lucide:layout-dashboard",
        );
        let dashboard = self.affixed_page(PageSpec {
            parent: &directory,
            name: "dashboard",
            title: "page.menu.dashboard",
            path: "/dashboard",
            component: "dashboard/index",
            permission_code: None,
            icon: "lucide:home",
        });
        self.buttons(
            &dashboard,
            &[
                ActionSpec {
                    name: "system:update_runtime_control",
                    title: "page.menu.action.updateRuntimeControl",
                    resource: ResourceType::System,
                    operation: Operation::UpdateRuntimeControl,
                },
                ActionSpec {
                    name: "system:halt",
                    title: "page.menu.action.halt",
                    resource: ResourceType::System,
                    operation: Operation::Halt,
                },
                ActionSpec {
                    name: "system:resume",
                    title: "page.menu.action.resume",
                    resource: ResourceType::System,
                    operation: Operation::Resume,
                },
                ActionSpec {
                    name: "system:emergency",
                    title: "page.menu.action.emergencyHalt",
                    resource: ResourceType::System,
                    operation: Operation::Emergency,
                },
            ],
        );
        self.page(PageSpec {
            parent: &directory,
            name: "runtime-activity",
            title: "page.menu.runtimeActivity",
            path: "/runtime/activity",
            component: "runtime/activity/index",
            permission_code: Some(perm(ResourceType::System, Operation::Read)),
            icon: "lucide:activity",
        });
    }

    fn build_trading_signals(&mut self) {
        let directory = self.dir(
            "trading-signals",
            "page.menu.group.tradingSignals",
            "lucide:chart-no-axes-combined",
        );
        let intelligence = self.page(PageSpec {
            parent: &directory,
            name: "market-intelligence",
            title: "page.menu.marketIntelligence",
            path: "/trading/market-intelligence",
            component: "trading/market-intelligence/index",
            permission_code: Some(perm(ResourceType::Market, Operation::Read)),
            icon: "lucide:radar",
        });
        self.buttons(
            &intelligence,
            &[ActionSpec {
                name: "market:update",
                title: "page.menu.action.updateMarket",
                resource: ResourceType::Market,
                operation: Operation::Update,
            }],
        );
        let recommendations = self.page(PageSpec {
            parent: &directory,
            name: "recommendations",
            title: "page.menu.recommendations",
            path: "/trading/recommendations",
            component: "trading/recommendations/index",
            permission_code: Some(perm(ResourceType::QuantReport, Operation::Read)),
            icon: "lucide:scan-search",
        });
        self.buttons(
            &recommendations,
            &[
                ActionSpec {
                    name: "quant_report:enqueue",
                    title: "page.menu.action.runReport",
                    resource: ResourceType::QuantReport,
                    operation: Operation::Enqueue,
                },
                ActionSpec {
                    name: "quant_report:revoke",
                    title: "page.menu.action.revokeReport",
                    resource: ResourceType::QuantReport,
                    operation: Operation::Revoke,
                },
            ],
        );
    }

    fn build_execution_capital(&mut self) {
        let directory = self.dir(
            "execution-capital",
            "page.menu.group.executionCapital",
            "lucide:landmark",
        );
        let orders = self.page(PageSpec {
            parent: &directory,
            name: "execution-orders",
            title: "page.menu.executionOrders",
            path: "/execution/orders",
            component: "execution/orders/index",
            permission_code: Some(perm(ResourceType::OrderIntent, Operation::Read)),
            icon: "lucide:list-checks",
        });
        self.buttons(
            &orders,
            &[
                ActionSpec {
                    name: "order_intent:create",
                    title: "page.menu.action.createIntent",
                    resource: ResourceType::OrderIntent,
                    operation: Operation::Create,
                },
                ActionSpec {
                    name: "order_intent:approve",
                    title: "page.menu.action.approveIntent",
                    resource: ResourceType::OrderIntent,
                    operation: Operation::Approve,
                },
                ActionSpec {
                    name: "order_intent:reject",
                    title: "page.menu.action.rejectIntent",
                    resource: ResourceType::OrderIntent,
                    operation: Operation::Reject,
                },
                ActionSpec {
                    name: "order_intent:cancel",
                    title: "page.menu.action.cancelIntent",
                    resource: ResourceType::OrderIntent,
                    operation: Operation::Cancel,
                },
            ],
        );
        self.page(PageSpec {
            parent: &directory,
            name: "execution-portfolio",
            title: "page.menu.executionPortfolio",
            path: "/execution/portfolio",
            component: "execution/portfolio/index",
            permission_code: Some(perm(ResourceType::AccountSnapshot, Operation::Read)),
            icon: "lucide:wallet-cards",
        });
        let post_trade = self.page(PageSpec {
            parent: &directory,
            name: "execution-post-trade",
            title: "page.menu.executionPostTrade",
            path: "/execution/post-trade",
            component: "execution/post-trade/index",
            permission_code: Some(perm(ResourceType::Reconciliation, Operation::Read)),
            icon: "lucide:scale",
        });
        self.buttons(
            &post_trade,
            &[
                ActionSpec {
                    name: "reconciliation:resolve",
                    title: "page.menu.action.resolveReconciliation",
                    resource: ResourceType::Reconciliation,
                    operation: Operation::Resolve,
                },
                ActionSpec {
                    name: "settlement_redeem:create",
                    title: "page.menu.action.createSettlement",
                    resource: ResourceType::SettlementRedeem,
                    operation: Operation::Create,
                },
                ActionSpec {
                    name: "settlement_redeem:approve",
                    title: "page.menu.action.approveSettlement",
                    resource: ResourceType::SettlementRedeem,
                    operation: Operation::Approve,
                },
                ActionSpec {
                    name: "settlement_redeem:revoke",
                    title: "page.menu.action.revokeSettlement",
                    resource: ResourceType::SettlementRedeem,
                    operation: Operation::Revoke,
                },
            ],
        );
    }

    fn build_research_models(&mut self) {
        let directory = self.dir(
            "research-models",
            "page.menu.group.researchModels",
            "lucide:flask-conical",
        );
        let lab = self.page(PageSpec {
            parent: &directory,
            name: "research-lab",
            title: "page.menu.researchLab",
            path: "/research/lab",
            component: "research/lab/index",
            permission_code: Some(perm(ResourceType::Materialization, Operation::Read)),
            icon: "lucide:workflow",
        });
        self.buttons(
            &lab,
            &[
                ActionSpec {
                    name: "materialization:create",
                    title: "page.menu.action.createResearchArtifact",
                    resource: ResourceType::Materialization,
                    operation: Operation::Create,
                },
                ActionSpec {
                    name: "replay:create",
                    title: "page.menu.action.runBacktest",
                    resource: ResourceType::Replay,
                    operation: Operation::Create,
                },
                ActionSpec {
                    name: "publication:publish",
                    title: "page.menu.action.publishModel",
                    resource: ResourceType::Publication,
                    operation: Operation::Publish,
                },
                ActionSpec {
                    name: "publication:rollback",
                    title: "page.menu.action.rollbackModel",
                    resource: ResourceType::Publication,
                    operation: Operation::Rollback,
                },
                ActionSpec {
                    name: "publication:retire",
                    title: "page.menu.action.retireModel",
                    resource: ResourceType::Publication,
                    operation: Operation::Retire,
                },
            ],
        );
        let learning = self.page(PageSpec {
            parent: &directory,
            name: "research-learning-policy",
            title: "page.menu.researchLearningPolicy",
            path: "/research/learning-policy",
            component: "research/learning-policy/index",
            permission_code: Some(perm(ResourceType::Materialization, Operation::Read)),
            icon: "lucide:route",
        });
        self.buttons(
            &learning,
            &[
                ActionSpec {
                    name: "materialization:create",
                    title: "page.menu.action.fitPolicy",
                    resource: ResourceType::Materialization,
                    operation: Operation::Create,
                },
                ActionSpec {
                    name: "publication:publish",
                    title: "page.menu.action.publishPolicy",
                    resource: ResourceType::Publication,
                    operation: Operation::Publish,
                },
                ActionSpec {
                    name: "publication:authorize",
                    title: "page.menu.action.authorizeCandidate",
                    resource: ResourceType::Publication,
                    operation: Operation::Authorize,
                },
                ActionSpec {
                    name: "publication:activate",
                    title: "page.menu.action.activateCandidate",
                    resource: ResourceType::Publication,
                    operation: Operation::Activate,
                },
                ActionSpec {
                    name: "publication:reject",
                    title: "page.menu.action.rejectCandidate",
                    resource: ResourceType::Publication,
                    operation: Operation::Reject,
                },
                ActionSpec {
                    name: "publication:retire",
                    title: "page.menu.action.retirePolicy",
                    resource: ResourceType::Publication,
                    operation: Operation::Retire,
                },
            ],
        );
        let reliability = self.page(PageSpec {
            parent: &directory,
            name: "research-data-reliability",
            title: "page.menu.researchDataReliability",
            path: "/research/data-reliability",
            component: "research/data-reliability/index",
            permission_code: Some(perm(ResourceType::Materialization, Operation::Read)),
            icon: "lucide:shield-check",
        });
        self.buttons(
            &reliability,
            &[ActionSpec {
                name: "materialization:create",
                title: "page.menu.action.recoverReliability",
                resource: ResourceType::Materialization,
                operation: Operation::Create,
            }],
        );
    }

    fn build_system_governance(&mut self) {
        let directory = self.dir(
            "system-governance",
            "page.menu.group.systemGovernance",
            "lucide:settings-2",
        );
        let config = self.page(PageSpec {
            parent: &directory,
            name: "system-config",
            title: "page.menu.systemConfig",
            path: "/system/config",
            component: "system/config/index",
            permission_code: Some(perm(ResourceType::DecisionPolicySnapshot, Operation::Read)),
            icon: "lucide:sliders-horizontal",
        });
        self.buttons(
            &config,
            &[
                ActionSpec {
                    name: "config:create",
                    title: "page.menu.action.createConfigDraft",
                    resource: ResourceType::DecisionPolicySnapshot,
                    operation: Operation::Create,
                },
                ActionSpec {
                    name: "config:approve",
                    title: "page.menu.action.approveConfigRevision",
                    resource: ResourceType::DecisionPolicySnapshot,
                    operation: Operation::Approve,
                },
                ActionSpec {
                    name: "config:activate",
                    title: "page.menu.action.activateConfigRevision",
                    resource: ResourceType::DecisionPolicySnapshot,
                    operation: Operation::Activate,
                },
                ActionSpec {
                    name: "config:rollback",
                    title: "page.menu.action.rollbackConfigRevision",
                    resource: ResourceType::DecisionPolicySnapshot,
                    operation: Operation::Rollback,
                },
            ],
        );
        self.page(PageSpec {
            parent: &directory,
            name: "system-audit",
            title: "page.menu.systemAudit",
            path: "/system/audit",
            component: "system/audit/index",
            permission_code: Some(perm(ResourceType::OperationLog, Operation::Read)),
            icon: "lucide:scroll-text",
        });
    }
}

/// Insert the fresh-boot menu tree and publish its grants during hydration.
pub async fn load(db: &DatabaseTransaction, ctx: &mut SeedContext) -> Result<u64, DbErr> {
    let tree = MenuTree::bootstrap();
    let rows_affected = Entity::insert_many(tree.models)
        .on_conflict(OnConflict::column(Column::Id).do_nothing().to_owned())
        .exec_without_returning(db)
        .await?;
    let _ = ctx;
    Ok(rows_affected)
}

async fn hydrate(db: &DatabaseTransaction, ctx: &mut SeedContext) -> Result<(), DbErr> {
    let tree = MenuTree::bootstrap();
    let rows = Entity::find()
        .filter(Column::Id.is_in(tree.ids.clone()))
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
    use std::collections::HashSet;

    use sea_orm::ActiveValue;

    use super::{MenuTree, stable_menu_id};
    use crate::enums::rbac::MenuKind;

    #[test]
    fn menu_ids_stable() {
        let first = stable_menu_id(None, "markets");
        let second = stable_menu_id(None, "markets");
        assert_eq!(first, second);
    }

    #[test]
    fn different_parents_differ() {
        let left = stable_menu_id(None, "left");
        let right = stable_menu_id(None, "right");
        assert_ne!(
            stable_menu_id(Some(&left), "create"),
            stable_menu_id(Some(&right), "create")
        );
    }

    #[test]
    fn visible_workspace_contract() {
        let tree = MenuTree::bootstrap();
        let directories = tree
            .models
            .iter()
            .filter(|model| matches!(&model.kind, ActiveValue::Set(MenuKind::Directory)))
            .count();
        let workspaces = tree
            .models
            .iter()
            .filter_map(|model| {
                if !matches!(&model.kind, ActiveValue::Set(MenuKind::Menu))
                    || !matches!(&model.hide_in_menu, ActiveValue::Set(false))
                {
                    return None;
                }
                let ActiveValue::Set(name) = &model.name else {
                    return None;
                };
                let ActiveValue::Set(Some(path)) = &model.path else {
                    return None;
                };
                let ActiveValue::Set(Some(component)) = &model.component else {
                    return None;
                };
                Some((name.clone(), path.clone(), component.clone()))
            })
            .collect::<HashSet<_>>();
        let expected = [
            ("dashboard", "/dashboard", "dashboard/index"),
            (
                "runtime-activity",
                "/runtime/activity",
                "runtime/activity/index",
            ),
            (
                "market-intelligence",
                "/trading/market-intelligence",
                "trading/market-intelligence/index",
            ),
            (
                "recommendations",
                "/trading/recommendations",
                "trading/recommendations/index",
            ),
            (
                "execution-orders",
                "/execution/orders",
                "execution/orders/index",
            ),
            (
                "execution-portfolio",
                "/execution/portfolio",
                "execution/portfolio/index",
            ),
            (
                "execution-post-trade",
                "/execution/post-trade",
                "execution/post-trade/index",
            ),
            ("research-lab", "/research/lab", "research/lab/index"),
            (
                "research-learning-policy",
                "/research/learning-policy",
                "research/learning-policy/index",
            ),
            (
                "research-data-reliability",
                "/research/data-reliability",
                "research/data-reliability/index",
            ),
            ("system-config", "/system/config", "system/config/index"),
            ("system-audit", "/system/audit", "system/audit/index"),
        ]
        .into_iter()
        .map(|(name, path, component)| (name.to_owned(), path.to_owned(), component.to_owned()))
        .collect::<HashSet<_>>();

        assert_eq!(directories, 5);
        assert_eq!(workspaces, expected);
    }

    #[test]
    fn legacy_spa_paths_absent() {
        let tree = MenuTree::bootstrap();
        for model in &tree.models {
            let ActiveValue::Set(Some(path)) = &model.path else {
                continue;
            };
            assert!(!path.starts_with("/quant/"), "legacy path `{path}`");
            assert_ne!(path, "/markets");
            assert_ne!(path, "/operation-log");
            assert!(!path.starts_with("/research/jobs"));
            assert!(!path.starts_with("/research/models"));
            assert!(!path.starts_with("/research/datasets"));
        }
    }

    #[test]
    fn workspace_actions_complete() {
        let tree = MenuTree::bootstrap();
        let permission_codes = tree
            .models
            .iter()
            .filter_map(|model| {
                if !matches!(&model.kind, ActiveValue::Set(MenuKind::Button)) {
                    return None;
                }
                match &model.permission_code {
                    ActiveValue::Set(Some(code)) => Some(code.as_str()),
                    _ => None,
                }
            })
            .collect::<HashSet<_>>();
        for expected in [
            "system:update_runtime_control",
            "system:halt",
            "system:resume",
            "system:emergency",
            "market:update",
            "quant_report:enqueue",
            "quant_report:revoke",
            "order_intent:create",
            "order_intent:approve",
            "order_intent:reject",
            "order_intent:cancel",
            "reconciliation:resolve",
            "settlement_redeem:create",
            "settlement_redeem:approve",
            "settlement_redeem:revoke",
            "materialization:create",
            "replay:create",
            "publication:publish",
            "publication:authorize",
            "publication:activate",
            "publication:reject",
            "publication:rollback",
            "publication:retire",
            "config:create",
            "config:approve",
            "config:activate",
            "config:rollback",
        ] {
            assert!(
                permission_codes.contains(expected),
                "missing workspace action `{expected}`"
            );
        }
    }

    #[test]
    fn dashboard_affixed() {
        let tree = MenuTree::bootstrap();
        let dashboard = tree
            .models
            .iter()
            .find(|model| matches!(&model.name, ActiveValue::Set(name) if name == "dashboard"))
            .expect("dashboard menu node");
        assert_eq!(dashboard.affix_tab, ActiveValue::Set(true));
    }

    #[test]
    fn menu_icons_valid() {
        let tree = MenuTree::bootstrap();
        for model in &tree.models {
            let kind = match &model.kind {
                ActiveValue::Set(kind) => *kind,
                _ => continue,
            };
            if !matches!(kind, MenuKind::Directory | MenuKind::Menu) {
                continue;
            }
            let icon = match &model.icon {
                ActiveValue::Set(Some(icon)) => icon.as_str(),
                _ => panic!("menu node missing icon"),
            };
            assert!(
                icon.contains(':'),
                "icon must be Iconify collection:name format"
            );
        }
    }
}
