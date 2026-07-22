//! RBAC, operation-log, and Casbin adapter persistence system contracts.
//!
//! Requires Docker (testcontainers Postgres). The migrated database already
//! carries the seeded RBAC graph (`admin` user, the built-in roles, and the
//! `g(admin, super_admin)` + built-in `p` matrix), so tests that need isolation
//! create their own users/roles/menus with unique keys.

use std::{collections::HashSet, slice};

use casbin::{Adapter, CoreApi, DefaultModel, Enforcer, Model};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        api::{OperationLogQuery, UserPageQuery},
        governance::NewOperationLog,
        pagination::PageRequest,
        patch::Patch,
        rbac::{
            AssignMenus, AssignPermissions, AssignRoles, ChangeUserPassword, NewMenu, NewRole,
            NewUser, Permission, RolePatch, UserPatch,
        },
    },
    entities::{
        casbin_rule,
        casbin_rule::{Column, Entity},
        operation_log::{Column as OperationLogColumn, Entity as OperationLogEntity},
    },
    enums::{
        operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
        rbac::{MenuKind, Operation, ResourceType, RoleKind, RoleStatus, UserStatus},
    },
    types::{
        ContentHash, MenuId, OperationDetailDocument, OperationLogId, RoleCode, RoleId, UserId,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgCasbinAdapter, PgMenuRepository, PgOperationLogRepository, PgRoleMenuRepository,
        PgRolePermissionRepository, PgRoleRepository, PgUserRepository, PgUserRoleRepository,
    },
    traits::{
        MenuRepository, OperationLogRepository, RoleMenuRepository, RolePermissionRepository,
        RoleRepository, UserRepository, UserRoleRepository,
    },
};
use quant_pivot_system_tests::postgres::setup_pg;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, sea_query::Expr,
};

/// Casbin model mirroring the production 4-tuple + `super_admin` bypass.
const CASBIN_MODEL: &str = "\
[request_definition]
r = sub, obj, act, typ
[policy_definition]
p = sub, obj, act, typ
[role_definition]
g = _, _
[policy_effect]
e = some(where (p.eft == allow))
[matchers]
m = g(r.sub, \"super_admin\") || (p.typ == \"resource\" && g(r.sub, p.sub) && r.obj == p.obj && r.act == p.act)
";

fn new_user(username: &str) -> NewUser {
    NewUser {
        id: UserId::from_v7(),
        username: username.to_owned(),
        password_hash: "argon2id$placeholder".to_owned(),
        nickname: username.to_owned(),
        avatar: None,
        email: None,
        phone: None,
        status: UserStatus::Active,
    }
}

fn new_role(code: &str) -> NewRole {
    NewRole {
        id: RoleId::from_v7(),
        code: RoleCode::new(code),
        name: code.to_owned(),
        description: None,
        kind: RoleKind::Custom,
        status: RoleStatus::Enabled,
        sort: 0,
    }
}

/// Count `g(subject, role_code)` grouping rows currently in `casbin_rule`.
async fn count_grouping(db: &DatabaseConnection, subject: &str, role_code: &str) -> u64 {
    Entity::find()
        .filter(Column::Ptype.eq("g"))
        .filter(Column::V0.eq(subject))
        .filter(Column::V1.eq(role_code))
        .count(db)
        .await
        .expect("count grouping")
}

/// Count `p` permission rows currently held by `role_code`.
async fn count_role_policies(db: &DatabaseConnection, role_code: &str) -> u64 {
    Entity::find()
        .filter(Column::Ptype.eq("p"))
        .filter(Column::V0.eq(role_code))
        .count(db)
        .await
        .expect("count policies")
}

/// Build a raw Casbin `p` rule line `(role_code, obj, act, "resource")`.
fn p_rule(role_code: &str, obj: &str, act: &str) -> Vec<String> {
    vec![
        role_code.to_owned(),
        obj.to_owned(),
        act.to_owned(),
        "resource".to_owned(),
    ]
}

fn new_menu(title: &str, parent_id: Option<MenuId>, kind: MenuKind, sort: i32) -> NewMenu {
    NewMenu {
        id: MenuId::from_v7(),
        parent_id,
        name: title.to_owned(),
        kind,
        path: None,
        component: None,
        title: title.to_owned(),
        icon: None,
        permission_code: None,
        sort,
        keep_alive: false,
        hide_in_menu: false,
        affix_tab: false,
        status: RoleStatus::Enabled,
    }
}

pub async fn user_crud_paging_and_delete() {
    let (pool, _container) = setup_pg().await;
    let repo = PgUserRepository::new(pool.connection().clone());

    let created = repo.create(new_user("alice")).await.expect("create user");
    assert_eq!(created.username, "alice");

    let by_name = repo
        .find_by_username("alice")
        .await
        .expect("find_by_username")
        .expect("present");
    assert_eq!(by_name.id, created.id);
    assert_eq!(
        repo.find_by_id(&created.id).await.expect("by id").nickname,
        "alice"
    );

    // Duplicate username is a Duplicate, not a raw DB error.
    let dup = repo.create(new_user("alice")).await;
    assert!(matches!(
        dup,
        Err(StorageError::Duplicate {
            entity: entity::USER,
            ..
        })
    ));

    // Partial update + status + password.
    let patched = repo
        .update(
            &created.id,
            UserPatch {
                nickname: Patch::set("Alice Liddell".to_owned()),
                ..Default::default()
            },
        )
        .await
        .expect("update");
    assert_eq!(patched.nickname, "Alice Liddell");
    repo.change_status(&created.id, UserStatus::Disabled)
        .await
        .expect("change_status");
    repo.change_password(
        &created.id,
        ChangeUserPassword {
            password_hash: "argon2id$rotated".to_owned(),
        },
    )
    .await
    .expect("change_password");
    let reloaded = repo.find_by_id(&created.id).await.expect("reload");
    assert_eq!(reloaded.status, UserStatus::Disabled);
    assert_eq!(reloaded.password_hash, "argon2id$rotated");

    // Paging with keyword filter (excludes the seeded `admin`).
    for name in ["pager-1", "pager-2", "pager-3"] {
        repo.create(new_user(name)).await.expect("create pager");
    }
    let page1 = repo
        .page(UserPageQuery {
            keyword: Some("pager".to_owned()),
            status: None,
            page: PageRequest::new(1, 2),
        })
        .await
        .expect("page 1");
    assert_eq!(page1.total, 3);
    assert_eq!(page1.items.len(), 2);
    assert!(page1.has_next);
    let page2 = repo
        .page(UserPageQuery {
            keyword: Some("pager".to_owned()),
            status: None,
            page: PageRequest::new(2, 2),
        })
        .await
        .expect("page 2");
    assert_eq!(page2.items.len(), 1);
    assert!(!page2.has_next);

    repo.delete(&created.id).await.expect("delete");
    assert!(matches!(
        repo.find_by_id(&created.id).await,
        Err(StorageError::NotFound { .. })
    ));
}

pub async fn role_crud_and_builtin_protection() {
    let (pool, _container) = setup_pg().await;
    let repo = PgRoleRepository::new(pool.connection().clone());

    let created = repo
        .create(new_role("custom_role_a"))
        .await
        .expect("create role");
    assert!(matches!(
        repo.create(new_role("custom_role_a")).await,
        Err(StorageError::Duplicate {
            entity: entity::ROLE,
            ..
        })
    ));

    let by_code = repo
        .find_by_code("custom_role_a")
        .await
        .expect("find_by_code")
        .expect("present");
    assert_eq!(by_code.id, created.id);

    repo.update(
        &created.id,
        RolePatch {
            name: Patch::set("Renamed".to_owned()),
            ..Default::default()
        },
    )
    .await
    .expect("update role");
    assert_eq!(
        repo.find_by_id(&created.id).await.expect("by id").name,
        "Renamed"
    );

    let all = repo.list().await.expect("list");
    assert!(all.iter().any(|r| r.code.as_str() == "custom_role_a"));
    assert!(all.iter().any(|r| r.code.as_str() == "super_admin"));

    // Built-in roles are protected from deletion; custom ones are deletable.
    let builtin = repo
        .find_by_code("super_admin")
        .await
        .expect("find super_admin")
        .expect("seeded");
    assert!(matches!(
        repo.delete(&builtin.id).await,
        Err(StorageError::StateConflict {
            entity: entity::ROLE,
            ..
        })
    ));
    repo.delete(&created.id).await.expect("delete custom role");
    assert!(
        repo.find_by_code("custom_role_a")
            .await
            .expect("lookup")
            .is_none()
    );
}

pub async fn menu_tree_accessibility_and_delete_guard() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let menus = PgMenuRepository::new(db.clone());
    let roles = PgRoleRepository::new(db.clone());
    let role_menu = PgRoleMenuRepository::new(db.clone());

    let root = menus
        .create(new_menu("rbac-root", None, MenuKind::Directory, 0))
        .await
        .expect("create root");
    let child = menus
        .create(new_menu("rbac-child", Some(root.id), MenuKind::Menu, 0))
        .await
        .expect("create child");

    let tree = menus.tree().await.expect("tree");
    let root_node = tree
        .iter()
        .find(|node| node.menu.id == root.id)
        .expect("root in tree");
    assert!(root_node.children.iter().any(|c| c.menu.id == child.id));

    // Parent with children cannot be deleted.
    assert!(matches!(
        menus.delete(&root.id).await,
        Err(StorageError::StateConflict {
            entity: entity::MENU,
            ..
        })
    ));

    // Accessibility: a role granted only the child still yields the full chain.
    let role = roles.create(new_role("menu_role")).await.expect("role");
    role_menu
        .set_menus_for_role(AssignMenus {
            role_id: role.id,
            menu_ids: vec![child.id],
        })
        .await
        .expect("assign menu");
    let accessible = menus
        .accessible_for_roles(slice::from_ref(&role.id))
        .await
        .expect("accessible");
    let acc_root = accessible
        .iter()
        .find(|node| node.menu.id == root.id)
        .expect("ancestor included");
    assert!(acc_root.children.iter().any(|c| c.menu.id == child.id));

    assert_eq!(
        role_menu
            .list_menus_for_role(&role.id)
            .await
            .expect("list menus")
            .len(),
        1
    );

    // Leaf deletion succeeds and cascades role_menu rows.
    menus.delete(&child.id).await.expect("delete leaf");
    assert!(
        role_menu
            .list_menus_for_role(&role.id)
            .await
            .expect("list after delete")
            .is_empty()
    );
}

pub async fn assign_roles_replaces_join_and_casbin_grouping() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let users = PgUserRepository::new(db.clone());
    let roles = PgRoleRepository::new(db.clone());
    let user_role = PgUserRoleRepository::new(db.clone());

    let user = users.create(new_user("assignee")).await.expect("user");
    let role_a = roles.create(new_role("set_role_a")).await.expect("role a");
    let role_b = roles.create(new_role("set_role_b")).await.expect("role b");

    let grouping_count = |code: &'static str| {
        let db = db.clone();
        let subject = user.id.to_string();
        async move {
            Entity::find()
                .filter(Column::Ptype.eq("g"))
                .filter(Column::V0.eq(subject))
                .filter(Column::V1.eq(code))
                .count(&db)
                .await
                .expect("count grouping")
        }
    };

    user_role
        .set_roles_for_user(AssignRoles {
            user_id: user.id,
            role_ids: vec![role_a.id],
        })
        .await
        .expect("assign a");
    let roles_now = user_role.list_roles_for_user(&user.id).await.expect("list");
    assert_eq!(roles_now.len(), 1);
    assert_eq!(roles_now[0].code.as_str(), "set_role_a");
    assert_eq!(grouping_count("set_role_a").await, 1);

    // Replace set: B in, A out — relational and Casbin `g` both follow.
    user_role
        .set_roles_for_user(AssignRoles {
            user_id: user.id,
            role_ids: vec![role_b.id],
        })
        .await
        .expect("assign b");
    let roles_now = user_role.list_roles_for_user(&user.id).await.expect("list");
    assert_eq!(roles_now.len(), 1);
    assert_eq!(roles_now[0].code.as_str(), "set_role_b");
    assert_eq!(grouping_count("set_role_a").await, 0);
    assert_eq!(grouping_count("set_role_b").await, 1);

    // Idempotent re-apply.
    user_role
        .set_roles_for_user(AssignRoles {
            user_id: user.id,
            role_ids: vec![role_b.id],
        })
        .await
        .expect("re-assign b");
    assert_eq!(grouping_count("set_role_b").await, 1);

    // Clear all.
    user_role
        .set_roles_for_user(AssignRoles {
            user_id: user.id,
            role_ids: vec![],
        })
        .await
        .expect("clear");
    assert!(
        user_role
            .list_roles_for_user(&user.id)
            .await
            .expect("list empty")
            .is_empty()
    );
    assert_eq!(grouping_count("set_role_b").await, 0);

    // Unknown role id is rejected.
    assert!(matches!(
        user_role
            .set_roles_for_user(AssignRoles {
                user_id: user.id,
                role_ids: vec![RoleId::from_v7()],
            })
            .await,
        Err(StorageError::NotFound { .. })
    ));
}

pub async fn assign_permissions_validates_and_round_trips() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let roles = PgRoleRepository::new(db.clone());
    let perms = PgRolePermissionRepository::new(db.clone());

    let role = roles.create(new_role("perm_role")).await.expect("role");

    perms
        .set_permissions_for_role(AssignPermissions {
            role_id: role.id,
            permissions: vec![
                Permission::new(ResourceType::Market, Operation::Read),
                Permission::new(ResourceType::Market, Operation::Update),
            ],
        })
        .await
        .expect("set permissions");
    let listed: HashSet<Permission> = perms
        .list_permissions(&role.id)
        .await
        .expect("list permissions")
        .into_iter()
        .collect();
    assert_eq!(
        listed,
        [
            Permission::new(ResourceType::Market, Operation::Read),
            Permission::new(ResourceType::Market, Operation::Update),
        ]
        .into_iter()
        .collect()
    );

    // Full replacement.
    perms
        .set_permissions_for_role(AssignPermissions {
            role_id: role.id,
            permissions: vec![Permission::new(ResourceType::OperationLog, Operation::Read)],
        })
        .await
        .expect("replace permissions");
    assert_eq!(
        perms.list_permissions(&role.id).await.expect("list again"),
        vec![Permission::new(ResourceType::OperationLog, Operation::Read)]
    );

    // Invalid resource×operation pair is rejected.
    assert!(matches!(
        perms
            .set_permissions_for_role(AssignPermissions {
                role_id: role.id,
                permissions: vec![Permission::new(
                    ResourceType::QuantReport,
                    Operation::Delete
                )],
            })
            .await,
        Err(StorageError::InvariantViolation { .. })
    ));
}

pub async fn set_permissions_for_unknown_role_is_not_found() {
    let (pool, _container) = setup_pg().await;
    let perms = PgRolePermissionRepository::new(pool.connection().clone());

    let result = perms
        .set_permissions_for_role(AssignPermissions {
            role_id: RoleId::from_v7(),
            permissions: vec![Permission::new(ResourceType::Market, Operation::Read)],
        })
        .await;
    assert!(matches!(result, Err(StorageError::NotFound { .. })));

    let listed = perms.list_permissions(&RoleId::from_v7()).await;
    assert!(matches!(listed, Err(StorageError::NotFound { .. })));
}

pub async fn casbin_adapter_matches_full_tuple() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let mut adapter = PgCasbinAdapter::new(db.clone());

    let count_for = |code: &'static str| {
        let db = db.clone();
        async move {
            Entity::find()
                .filter(Column::Ptype.eq("p"))
                .filter(Column::V0.eq(code))
                .count(&db)
                .await
                .expect("count policies")
        }
    };

    let read = p_rule("adapter_role", "market", "read");
    let update = p_rule("adapter_role", "market", "update");

    // Two policies sharing the same ptype must COEXIST (ng-gateway regression),
    // and each fresh add reports that storage was modified.
    assert!(
        adapter
            .add_policy("p", "p", read.clone())
            .await
            .expect("add read")
    );
    assert!(
        adapter
            .add_policy("p", "p", update.clone())
            .await
            .expect("add update")
    );
    assert_eq!(count_for("adapter_role").await, 2);

    // Re-adding an existing policy is idempotent and reports no modification.
    assert!(
        !adapter
            .add_policy("p", "p", read.clone())
            .await
            .expect("re-add")
    );
    assert_eq!(count_for("adapter_role").await, 2);

    // add_policies is all-or-nothing: a batch containing an already-present rule
    // inserts none and reports no modification.
    let risk = p_rule("adapter_role", "risk", "read");
    let pnl = p_rule("adapter_role", "pnl", "read");
    assert!(
        !adapter
            .add_policies("p", "p", vec![risk.clone(), read.clone()])
            .await
            .expect("add_policies with a duplicate")
    );
    assert_eq!(count_for("adapter_role").await, 2);

    // A wholly-new batch is inserted atomically and reports modification.
    assert!(
        adapter
            .add_policies("p", "p", vec![risk, pnl])
            .await
            .expect("add_policies fresh batch")
    );
    assert_eq!(count_for("adapter_role").await, 4);

    // A short tuple and a longer tuple with the same prefix are distinct. Exact
    // deletion must pad the short tuple and preserve the longer one.
    let short = vec!["adapter_role".to_owned(), "audit".to_owned()];
    let long = vec![
        "adapter_role".to_owned(),
        "audit".to_owned(),
        "read".to_owned(),
    ];
    assert!(
        adapter
            .add_policy("p", "p", short.clone())
            .await
            .expect("short")
    );
    assert!(adapter.add_policy("p", "p", long).await.expect("long"));
    assert!(
        adapter
            .remove_policy("p", "p", short)
            .await
            .expect("remove short")
    );
    assert_eq!(
        casbin_rule::Entity::find()
            .filter(casbin_rule::Column::Ptype.eq("p"))
            .filter(casbin_rule::Column::V0.eq("adapter_role"))
            .filter(casbin_rule::Column::V1.eq("audit"))
            .filter(casbin_rule::Column::V2.eq("read"))
            .count(&db)
            .await
            .expect("count long tuple"),
        1
    );

    // Batch removal is one atomic statement over the exact tuple set.
    assert!(
        adapter
            .remove_policies(
                "p",
                "p",
                vec![
                    p_rule("adapter_role", "risk", "read"),
                    p_rule("adapter_role", "pnl", "read"),
                ],
            )
            .await
            .expect("remove policies")
    );
    assert_eq!(count_for("adapter_role").await, 3);

    // Exact removal deletes only the matching tuple.
    assert!(adapter.remove_policy("p", "p", read).await.expect("remove"));
    assert_eq!(count_for("adapter_role").await, 2);

    // save_policy is a full snapshot: clear then bulk insert from the model.
    adapter.clear_policy().await.expect("clear");
    let mut model = DefaultModel::from_str(CASBIN_MODEL).await.expect("model");
    model.add_policy(
        "p",
        "p",
        vec![
            "snap_role".to_owned(),
            "risk".to_owned(),
            "read".to_owned(),
            "resource".to_owned(),
        ],
    );
    model.add_policy(
        "g",
        "g",
        vec!["snap_user".to_owned(), "snap_role".to_owned()],
    );
    adapter.save_policy(&mut model).await.expect("save");
    assert_eq!(count_for("snap_role").await, 1);
    let grouping = Entity::find()
        .filter(Column::Ptype.eq("g"))
        .filter(Column::V0.eq("snap_user"))
        .filter(Column::V1.eq("snap_role"))
        .count(&db)
        .await
        .expect("count g");
    assert_eq!(grouping, 1);
}

pub async fn enforce_reflects_assignments_and_super_admin_bypass() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let users = PgUserRepository::new(db.clone());
    let roles = PgRoleRepository::new(db.clone());
    let user_role = PgUserRoleRepository::new(db.clone());
    let perms = PgRolePermissionRepository::new(db.clone());

    let role = roles.create(new_role("enforce_role")).await.expect("role");
    perms
        .set_permissions_for_role(AssignPermissions {
            role_id: role.id,
            permissions: vec![Permission::new(ResourceType::Market, Operation::Read)],
        })
        .await
        .expect("perms");
    let user = users.create(new_user("enforcee")).await.expect("user");
    user_role
        .set_roles_for_user(AssignRoles {
            user_id: user.id,
            role_ids: vec![role.id],
        })
        .await
        .expect("assign");

    let model = DefaultModel::from_str(CASBIN_MODEL).await.expect("model");
    let enforcer = Enforcer::new(model, PgCasbinAdapter::new(db.clone()))
        .await
        .expect("enforcer");

    let subject = user.id.to_string();
    assert!(
        enforcer
            .enforce((subject.as_str(), "market", "read", "resource"))
            .expect("enforce read")
    );
    assert!(
        !enforcer
            .enforce((subject.as_str(), "market", "update", "resource"))
            .expect("enforce update")
    );

    // Seeded admin → super_admin bypass grants everything.
    let admin = users
        .find_by_username("admin")
        .await
        .expect("admin lookup")
        .expect("seeded admin");
    let admin_subject = admin.id.to_string();
    assert!(
        enforcer
            .enforce((
                admin_subject.as_str(),
                "runtime_config",
                "activate",
                "resource"
            ))
            .expect("admin enforce")
    );
}

pub async fn role_disable_revokes_then_enable_rebuilds_grouping() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let users = PgUserRepository::new(db.clone());
    let roles = PgRoleRepository::new(db.clone());
    let user_role = PgUserRoleRepository::new(db.clone());
    let perms = PgRolePermissionRepository::new(db.clone());

    let role = roles.create(new_role("toggle_role")).await.expect("role");
    perms
        .set_permissions_for_role(AssignPermissions {
            role_id: role.id,
            permissions: vec![Permission::new(ResourceType::Market, Operation::Read)],
        })
        .await
        .expect("perms");
    let user = users.create(new_user("toggler")).await.expect("user");
    user_role
        .set_roles_for_user(AssignRoles {
            user_id: user.id,
            role_ids: vec![role.id],
        })
        .await
        .expect("assign");

    let subject = user.id.to_string();
    let allows = |db: &DatabaseConnection| {
        let db = db.clone();
        let subject = subject.clone();
        async move {
            let model = DefaultModel::from_str(CASBIN_MODEL).await.expect("model");
            let enforcer = Enforcer::new(model, PgCasbinAdapter::new(db))
                .await
                .expect("enforcer");
            enforcer
                .enforce((subject.as_str(), "market", "read", "resource"))
                .expect("enforce")
        }
    };

    assert!(
        allows(&db).await,
        "enabled role should grant its permission"
    );

    // Disable: the grouping is dropped, so the permission stops granting, but the
    // `p` policy and the `user_role` membership survive.
    roles
        .change_status(&role.id, RoleStatus::Disabled)
        .await
        .expect("disable");
    assert!(!allows(&db).await, "disabled role must grant nothing");
    assert_eq!(
        count_grouping(&db, &subject, "toggle_role").await,
        0,
        "disable removes the g binding"
    );
    assert_eq!(
        count_role_policies(&db, "toggle_role").await,
        1,
        "disable preserves the p policy"
    );

    // Re-enable: the grouping is rebuilt from the surviving membership.
    roles
        .change_status(&role.id, RoleStatus::Enabled)
        .await
        .expect("enable");
    assert!(allows(&db).await, "re-enabled role grants again");
    assert_eq!(
        count_grouping(&db, &subject, "toggle_role").await,
        1,
        "enable rebuilds the g binding from user_role"
    );
}

pub async fn assigning_a_disabled_role_writes_no_grouping() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let users = PgUserRepository::new(db.clone());
    let roles = PgRoleRepository::new(db.clone());
    let user_role = PgUserRoleRepository::new(db.clone());

    let role = roles.create(new_role("born_disabled")).await.expect("role");
    roles
        .change_status(&role.id, RoleStatus::Disabled)
        .await
        .expect("disable");
    let user = users.create(new_user("late_joiner")).await.expect("user");
    user_role
        .set_roles_for_user(AssignRoles {
            user_id: user.id,
            role_ids: vec![role.id],
        })
        .await
        .expect("assign");

    // Membership is recorded relationally, but no Casbin grouping is projected.
    assert_eq!(
        user_role
            .list_roles_for_user(&user.id)
            .await
            .expect("list")
            .len(),
        1,
        "membership is recorded even for a disabled role"
    );
    assert_eq!(
        count_grouping(&db, &user.id.to_string(), "born_disabled").await,
        0,
        "a disabled role projects no grouping on assignment"
    );
}

pub async fn operation_log_appends_and_pages_and_is_worm() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let repo = PgOperationLogRepository::new(db.clone());

    let make = |action: &str, outcome: OperationOutcome| NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: format!("req-{action}").into(),
        actor_user_id: Some(UserId::from_v7()),
        actor_username: Some("admin".to_owned()),
        acting_role: Some("super_admin".into()),
        category: OperationCategory::Rbac,
        action: action.into(),
        resource_type: Some(ResourceType::User),
        resource_id: Some(UserId::from_v7().to_string()),
        http_method: OperationHttpMethod::Post,
        http_path: "/api/v1/users".to_owned(),
        http_status: 200,
        outcome,
        client_ip: None,
        user_agent: None,
        latency_ms: 5,
        detail: OperationDetailDocument::try_from(serde_json::json!({"k": "v"}))
            .expect("static operation detail"),
        before_hash: Some(
            ContentHash::parse(
                "blake3:0000000000000000000000000000000000000000000000000000000000000001",
            )
            .expect("canonical before hash"),
        ),
        after_hash: Some(
            ContentHash::parse(
                "blake3:0000000000000000000000000000000000000000000000000000000000000002",
            )
            .expect("canonical after hash"),
        ),
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    };

    repo.append(make("single", OperationOutcome::Success))
        .await
        .expect("append");
    repo.append_batch(vec![
        make("batch-1", OperationOutcome::Success),
        make("batch-2", OperationOutcome::Denied),
    ])
    .await
    .expect("append_batch");

    let denied = repo
        .page(OperationLogQuery {
            outcome: Some(OperationOutcome::Denied),
            ..Default::default()
        })
        .await
        .expect("page denied");
    assert_eq!(denied.total, 1);
    assert_eq!(denied.items.len(), 1);
    assert_eq!(denied.items[0].action.as_str(), "batch-2");

    let all = repo
        .page(OperationLogQuery {
            category: Some(OperationCategory::Rbac),
            ..Default::default()
        })
        .await
        .expect("page rbac");
    assert_eq!(all.total, 3);

    // WORM: UPDATE and DELETE are rejected by the append-only trigger.
    let update = OperationLogEntity::update_many()
        .col_expr(OperationLogColumn::Action, Expr::value("tampered"))
        .exec(&db)
        .await;
    assert!(update.is_err());
    let delete = OperationLogEntity::delete_many().exec(&db).await;
    assert!(delete.is_err());
}
