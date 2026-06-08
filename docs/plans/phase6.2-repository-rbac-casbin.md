# Phase 6.2 — Repository 层 + Casbin Adapter

> **状态**: Production Design Target
> **父计划**: `docs/plans/phase6-web-layer.md`
> **前置依赖**: Phase 6.1（模型/枚举/ID/表/seed/密码原语已落地）
> **覆盖父计划章节**: §8.3, §10
> **目标**: 在 `oxide-arb-repository` 落地 RBAC + operation_log 的 trait + Postgres 实现，以及自写 Casbin `Adapter`（精确匹配，修复 ng-gateway `ptype`-only 去重缺陷）。保证 user-role(`g`) / role-permission(`p`) 的 DB 写入与 enforcer reload 的事务一致性。本子 phase **不引入 web 框架**。

---

## 0. 工作范围

### 0.1 交付物

| 交付物 | 位置 | 说明 |
|---|---|---|
| RBAC traits | `traits/rbac/*.rs` | User/Role/Menu/UserRole/RoleMenu/RolePermission/Casbin repository trait |
| RBAC pg impl | `postgres/rbac/*.rs` | 对应 `Pg*Repository` + `Pg*RepositoryTxn`（需事务者） |
| OperationLog repo | `traits/operation_log.rs` + `postgres/operation_log.rs` | append-only 写入 + 分页查询 |
| Casbin adapter | `postgres/rbac/casbin_adapter.rs` | `impl casbin::Adapter`，精确 `ptype + v0..v5` 匹配 |
| 导出 | `traits/mod.rs` + `postgres/mod.rs` | `pub use` 具体类型与 trait |

### 0.2 非目标

- 不创建 `CasbinService`（`CachedEnforcer` 封装）— 归 6.4（依赖 casbin crate + model）。
- 不写任何 actix / HTTP 代码。
- 不接线 `BuildRepos` / core — 归 6.6。

---

## 1. 约定（对齐既有 `PgRiskStateRepository`）

- 所有方法返回 `Result<_, StorageError>`；SeaORM 错误 `.map_err(StorageError::from)?`。
- 入参用 6.1 的 `New*` / DTO；输出用 `*Info` 读模型（不泄漏 SeaORM `Model`）。
- 结构 `PgXRepository { db: DatabaseConnection }` + `new(db)`；需事务者另出 `PgXRepositoryTxn<'a> { txn: &'a DatabaseTransaction }`，二者共享 `do_*(conn: &impl ConnectionTrait, ...)` 自由函数。
- 缺失实体 → `StorageError::NotFound { entity, id }`。
- 唯一冲突 → 映射为领域语义（见 §6 `RbacError`，最终 HTTP 映射在 6.3 web error 层）。repository 层保持 `StorageError`，在能精确判定唯一冲突处产出 `StorageError::Conflict`（携带 entity/key）。

---

## 2. RBAC traits

```rust
pub trait UserRepository: Send + Sync {
    async fn find_by_username(&self, username: &str) -> Result<Option<UserInfo>, StorageError>;
    async fn find_by_id(&self, id: &UserId) -> Result<UserInfo, StorageError>;
    async fn create(&self, user: NewUser) -> Result<UserInfo, StorageError>;
    async fn update(&self, id: &UserId, patch: UpdateUser) -> Result<UserInfo, StorageError>;
    async fn delete(&self, id: &UserId) -> Result<(), StorageError>;
    async fn change_status(&self, id: &UserId, status: UserStatus) -> Result<(), StorageError>;
    async fn change_password(&self, id: &UserId, password_hash: String) -> Result<(), StorageError>;
    async fn page(&self, query: UserPageQuery) -> Result<Paginated<UserInfo>, StorageError>;
}

pub trait RoleRepository: Send + Sync {
    async fn list(&self) -> Result<Vec<RoleInfo>, StorageError>;
    async fn find_by_id(&self, id: &RoleId) -> Result<RoleInfo, StorageError>;
    async fn find_by_code(&self, code: &str) -> Result<Option<RoleInfo>, StorageError>;
    async fn create(&self, role: NewRole) -> Result<RoleInfo, StorageError>;
    async fn update(&self, id: &RoleId, patch: UpdateRole) -> Result<RoleInfo, StorageError>;
    async fn delete(&self, id: &RoleId) -> Result<(), StorageError>;
    async fn change_status(&self, id: &RoleId, status: RoleStatus) -> Result<(), StorageError>;
}

pub trait MenuRepository: Send + Sync {
    async fn tree(&self) -> Result<Vec<MenuTreeNode>, StorageError>;
    async fn accessible_for_roles(&self, role_ids: &[RoleId]) -> Result<Vec<MenuTreeNode>, StorageError>;
    async fn find_by_id(&self, id: &MenuId) -> Result<MenuInfo, StorageError>;
    async fn create(&self, menu: NewMenu) -> Result<MenuInfo, StorageError>;
    async fn update(&self, id: &MenuId, patch: UpdateMenu) -> Result<MenuInfo, StorageError>;
    async fn delete(&self, id: &MenuId) -> Result<(), StorageError>;
}
```

---

## 3. 事务一致性（`g` / `p` 与 enforcer）

写策略类操作必须 DB 写入 + casbin 同步在**同一逻辑边界**完成；分配成功后由上层（6.4 service）调 `casbin.load_policy()` reload。Repository 暴露既写 join 表又写 `casbin_rule` 的事务方法：

```rust
pub trait UserRoleRepository: Send + Sync {
    /// Assign roles to user inside ONE txn: write user_role rows + casbin `g`.
    async fn assign(&self, user_id: &UserId, role_ids: &[RoleId]) -> Result<(), StorageError>;
    async fn revoke(&self, user_id: &UserId, role_ids: &[RoleId]) -> Result<(), StorageError>;
    async fn list_roles_for_user(&self, user_id: &UserId) -> Result<Vec<RoleInfo>, StorageError>;
}

pub trait RoleMenuRepository: Send + Sync {
    async fn assign(&self, role_id: &RoleId, menu_ids: &[MenuId]) -> Result<(), StorageError>;
    async fn revoke(&self, role_id: &RoleId, menu_ids: &[MenuId]) -> Result<(), StorageError>;
    async fn list_menus_for_role(&self, role_id: &RoleId) -> Result<Vec<MenuInfo>, StorageError>;
}

/// Logical layer over casbin `p` policies. resource×op pairs validated against
/// RESOURCE_OPERATIONS before write.
pub trait RolePermissionRepository: Send + Sync {
    async fn assign_permissions(&self, role_code: &str, perms: &[(ResourceType, Operation)]) -> Result<(), StorageError>;
    async fn list_permissions(&self, role_code: &str) -> Result<Vec<(ResourceType, Operation)>, StorageError>;
}
```

- `assign`：txn 内 `user_role` upsert（`ON CONFLICT (user_id, role_id) DO NOTHING`）+ `casbin_rule` 写 `g=(user_id, role_code)`（精确去重）。`g` 的 subject 为 `user_id`（不是 username）。
- `assign_permissions`：先用 `RESOURCE_OPERATIONS` 校验合法组合；txn 内全量替换该 role 的 `p` 集合（先删后插，或 diff）。
- `list_permissions`：读 `casbin_rule` `p where v0=role_code`，`v1/v2` → `ResourceType::from_str` / `Operation::from_str`（6.1 提供）。
- enforcer reload 不在 repository（无 enforcer 句柄）；由 6.4 service 在 repo 调用后触发。

---

## 4. Casbin Adapter（自写，精确匹配）

```rust
/// Persists Casbin policies into `casbin_rule`. Unlike the ng-gateway adapter
/// (which dedupes by `ptype` only and silently drops new policies), this matches
/// on the full (ptype, v0..v5) tuple for add/remove and idempotent inserts.
pub struct PgCasbinAdapter { db: DatabaseConnection }

impl casbin::Adapter for PgCasbinAdapter {
    async fn load_policy(&mut self, model: &mut dyn Model) -> casbin::Result<()>;     // SELECT * → load into model
    async fn save_policy(&mut self, model: &mut dyn Model) -> casbin::Result<()>;     // TRUNCATE + bulk insert (full snapshot)
    async fn add_policy(&mut self, sec, ptype, rule) -> casbin::Result<bool>;          // exact-match INSERT IF NOT EXISTS
    async fn add_policies(&mut self, sec, ptype, rules) -> casbin::Result<bool>;
    async fn remove_policy(&mut self, sec, ptype, rule) -> casbin::Result<bool>;        // exact (ptype + v0..v5) DELETE
    async fn remove_policies(&mut self, sec, ptype, rules) -> casbin::Result<bool>;
    async fn remove_filtered_policy(&mut self, sec, ptype, idx, values) -> casbin::Result<bool>;
    // is_filtered / set_filtered ... per casbin::Adapter
}
```

- `casbin_rule` PK `i64` auto_increment；`ptype` + `v0..v5`（NULL 为缺省）。
- `add_policy` 写前查精确组合是否存在（依赖 6.1 `uq_casbin_rule` 唯一约束 + `ON CONFLICT DO NOTHING`）。
- `remove_policy` 按 `ptype` + 提供的 `v*` 全字段精确 DELETE（对齐 ng-gateway 正确的 remove，但 add 必须修复）。
- 错误 `StorageError → casbin::Error`（adapter 边界），上层再映射。

> 6.4 的 `CasbinService` 用 `CachedEnforcer::new(model, PgCasbinAdapter)` + `load_policy()`；分配类写入后 `load_policy()` 刷新缓存。

---

## 5. OperationLog Repository

```rust
pub trait OperationLogRepository: Send + Sync {
    /// Append one immutable row. Best-effort by caller (async writer); never UPDATE/DELETE.
    async fn append(&self, log: NewOperationLog) -> Result<(), StorageError>;
    /// Batched append for the async writer drain.
    async fn append_batch(&self, logs: Vec<NewOperationLog>) -> Result<(), StorageError>;
    async fn page(&self, query: OperationLogQuery) -> Result<Paginated<OperationLogInfo>, StorageError>;
}
```

- 仅 INSERT（DB 触发器兜底拒绝 UPDATE/DELETE）。
- `append_batch`：6.5 异步 writer 批量落库，单 txn 多行 insert。
- `page`：按 `occurred_at desc` + 过滤（actor/category/resource/action/outcome/时间窗）。

---

## 6. `Paginated<T>` 与 `RbacError`

- `Paginated<T> { items: Vec<T>, total: u64, page: u64, size: u64, has_next: bool }`：放 `oxide-arb-models::domain`（web envelope 与 repository 共用）。
- `RbacError`（`oxide-arb-error`）：`NotFound { entity, id }` / `Duplicate { entity, key }` / `UnknownPermission { resource, operation }` / `InvalidAssignment`。repository 层主要产出 `StorageError`；`RbacError` 多用于 service/web 层语义。6.2 仅定义 + 在能精确判定唯一冲突处产出 `StorageError::Conflict`（携带 entity/key），由 6.3 web error 映射为 409。

---

## 7. 导出与组织

- `traits/rbac/mod.rs`：`pub mod user; pub mod role; ...` + 顶部 `pub use`。
- `postgres/rbac/mod.rs`：`pub use` 各 `Pg*Repository` + `PgCasbinAdapter`。
- 在 `traits/mod.rs` / `postgres/mod.rs` 挂接 rbac 与 operation_log 模块。

---

## 8. 测试策略（testcontainers PG）

| 测试 | 场景 |
|---|---|
| User CRUD | create/find_by_username/find_by_id/update/change_status/change_password/delete/page |
| Role CRUD | create/find_by_code/update/change_status/delete |
| Menu | tree 组装、accessible_for_roles 过滤、CRUD |
| user_role 事务 | assign 同时写 user_role + casbin `g`；revoke 双删；唯一约束幂等 |
| role_menu | assign/revoke/list |
| role_permission | assign_permissions 校验非法组合被拒；list_permissions 字符串→枚举往返；全量替换 |
| Casbin adapter | load/save/add/remove **精确匹配**：连续 add 多条 `p` 不被 ptype 去重吞掉；remove 精确删除单条；重复 add 幂等 |
| operation_log | append/append_batch；page 过滤 + 分页；UPDATE/DELETE 被 DB 触发器拒绝（断言报错） |
| enforce 生效 | assign_permissions 后（在 6.4 service 测试中）`enforce` 命中——本子 phase 可用直接 enforcer + adapter 的最小集成测试预演 |

---

## 9. 退出条件

1. 全部 RBAC + operation_log repository 编译通过、testcontainers 测试绿。
2. Casbin adapter 精确匹配：多条同 `ptype` 策略可共存（修复缺陷）；add 幂等、remove 精确。
3. user-role / role-permission 写入与 casbin 同步在同一事务内原子完成。
4. operation_log 只可 append，UPDATE/DELETE 被拒。
5. `list_permissions` 字符串↔枚举往返无损。

## 10. 阻止进入 6.3 的情况

- Casbin adapter 仍按 `ptype` 去重（任一"多策略共存"测试失败）。
- assign 出现 DB 写成功但 casbin 未同步（或反之）的不一致窗口。
- operation_log 可被 UPDATE/DELETE。
