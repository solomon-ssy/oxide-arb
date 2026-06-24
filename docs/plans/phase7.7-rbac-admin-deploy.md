# Phase 7.7 — RBAC Admin(用户/角色/菜单)与部署集成

> **产出**: Access Control 三页(Users / Roles / Menus)+ 构建产物集成 Rust 静态服务
>
> **前置**: [phase7.1](phase7.1-rbac-auth-request-types.md);可与 7.3–7.6 并行
>
> **范式蓝本**: ng-gateway `views/system/user`(列表骨架 + Drawer 协议照搬)

---

## 1. Users(`views/users/`)

### 1.1 概要

| 项 | 值 |
|---|---|
| 路由 / component | `/users` / `users/index` |
| 菜单码 | `user:read`;按钮码 `user:create / update / delete / assign` |
| grid/drawer | 标准列表骨架;创建/编辑 `useVbenDrawer`(connectedComponent);角色分配 `useVbenModal`;重置密码 `prompt()` |
| i18n | `page.users.*`;`entity.user` |
| types | `UserView / RoleInfo`(`oxide/rbac.ts`) |

### 1.2 布局与交互

```text
┌ 搜索: username/nickname 关键词 | 状态 | 时间范围                            ┐
├ Grid: 用户名 | 昵称 | 邮箱 | 手机 | 状态(CellSwitch user:update)            ┤
│       | 创建时间(CellDateTime)                                              │
│       | 操作(CellOperation: 编辑 / 分配角色 / 重置密码 / 删除)              │
│ toolbar: [+ 新建用户 user:create]                                           │
├ UserFormDrawer: username/nickname/email/phone/password(仅创建)/status      ┤
└ AssignRolesModal: 角色多选(checkbox 列表,预选当前角色)                     ┘
```

- **创建/编辑**:标准 connectedComponent 协议(7.0 §4.2);EDIT 时 `getUserById` 回填,密码字段 `removeSchemaByFields`。提交:`POST /users` / `PUT /users/{id}`。
- **状态开关**:`CellSwitch` + `beforeChange` → `PUT /users/{id}/status {status}`(`user:update`)。
- **重置密码**(`user:update`):`prompt()` 输入新密码 → `PUT /users/{id}/password {password}`。
- **分配角色**(`user:assign`):Modal 载入 `fetchRoles()` 全量 + 当前用户角色(`getUserById` 含 roles?以实际 `UserView` 为准,缺则后端对齐)→ 提交 `PUT /users/{id}/roles {role_ids}`;成功后提示「目标用户下次请求即生效(Casbin 已重载)」。
- **删除**(`user:delete`):`confirm` + `DELETE /users/{id}`;禁止删除自己(前端拦截 + 后端兜底)。

### 1.3 API(`api/users.ts`)

`fetchUserPage` / `getUserById` / `createUser` / `updateUser` / `deleteUser` / `changeUserStatus` / `resetUserPassword` / `assignUserRoles` → 对应 `GET/POST /users`、`GET/PUT/DELETE /users/{id}`、`PUT /users/{id}/status|password|roles`。

---

## 2. Roles(`views/roles/`)

### 2.1 概要

| 项 | 值 |
|---|---|
| 路由 / component | `/roles` / `roles/index` |
| 菜单码 | `role:read`;按钮码 `role:create / update / delete / assign / permission:read` |
| grid/drawer | 列表骨架(`GET /roles` 全量 → 本地分页);创建/编辑 `useVbenDrawer`;**权限矩阵** `useVbenModal`(大);**菜单分配** `useVbenModal`(树) |
| i18n | `page.roles.*`;`entity.role` |
| types | `RoleInfo / Permission / PermissionCatalogEntry / MenuInfo`(`oxide/rbac.ts`) |

### 2.2 布局与交互

```text
┌ Grid: code | 名称 | 描述 | 状态(CellSwitch role:update) | 操作              ┐
│   操作(CellOperation): 编辑 / 权限矩阵 / 菜单分配 / 删除                    │
│   内置角色(super_admin 等 seed 角色)删除项 show=false                       │
├ RoleFormDrawer: code(编辑时只读)/name/description/status                    ┤
├ PermissionMatrixModal(role:assign + permission:read):                       ┤
│   行 = resource(GET /permissions/catalog 19 资源)                           │
│   列 = operation(资源各自的合法 operations)                                 │
│   单元格 = Checkbox;行全选/列批量;预选 GET /roles/{id}/permissions         │
│   提交 PUT /roles/{id}/permissions { permissions: Permission[] }            │
└ AssignMenusModal(role:assign): 菜单树 Checkbox(GET /menus 全量树)           ┘
   预选 GET /roles/{id}/menus;提交 PUT /roles/{id}/menus { menu_ids }
```

要点:

- **权限矩阵**(`permission-matrix-modal.vue`):catalog 行列驱动,不写死资源清单(后端 `RESOURCE_OPERATIONS` 为唯一事实源);保存后提示「权限即时生效(Casbin 重载)」。
- **菜单分配与权限矩阵的关系**在 Modal 顶部说明:菜单控制**可见性**,权限控制**可操作性**;两者需配套(分配页面菜单时建议同步勾选对应 read 权限,UI 提供「按菜单推导权限」辅助按钮——根据所选菜单节点的 `permission_code` 自动勾选矩阵,单向辅助不强制)。
- **删除角色**:`confirm`(提示影响:持有该角色用户将失去对应权限)→ `DELETE /roles/{id}`。

### 2.3 API(`api/roles.ts` + `api/permissions.ts` + `api/menus.ts` 部分)

`fetchRoles` / `getRoleById` / `createRole` / `updateRole` / `deleteRole` / `changeRoleStatus` / `getRolePermissions` / `assignRolePermissions` / `getRoleMenus` / `assignRoleMenus` / `getPermissionCatalog` → 对应 `GET/POST /roles`、`GET/PUT/DELETE /roles/{id}`、`PUT /roles/{id}/status`、`GET/PUT /roles/{id}/permissions`、`GET/PUT /roles/{id}/menus`、`GET /permissions/catalog`。

---

## 3. Menus(`views/menus/`)

### 3.1 概要

| 项 | 值 |
|---|---|
| 路由 / component | `/menus` / `menus/index` |
| 菜单码 | `menu:read`;按钮码 `menu:create / update / delete` |
| grid/drawer | **树形表格**:`useVbenVxeGrid` treeConfig(`GET /menus` 全量树,无分页/搜索);创建/编辑 `useVbenDrawer` |
| i18n | `page.menus.*`;`entity.menu` |
| types | `MenuInfo / MenuTreeNode / MenuKind`(`oxide/rbac.ts`) |

### 3.2 布局与交互

```text
┌ Tree Grid: 标题($t(title) 渲染 + 原始 key tooltip) | kind(CellTag)          ┐
│   | path | component | permission_code | 排序 | 状态(CellTag)               │
│   | hideInMenu/keepAlive(图标) | 操作(编辑 / 添加子级 / 删除)               │
│ toolbar: [+ 新建根目录 menu:create] [展开/折叠]                              │
└ MenuFormDrawer(动态 schema,按 kind 切换字段组):                            ┘
   Directory: name/title/icon/sort/status
   Menu:      + path/component/permission_code/keep_alive/hide_in_menu
   Button:    name/title/permission_code/sort/status(无 path/component)
```

要点:

- **kind 驱动动态表单**:`form.ts` 导出 `useMenuFormSchema(kind)` factory,字段随 kind 显隐(`dependencies` 机制);`permission_code` 提供 catalog 联想(Select 数据 `GET /permissions/catalog` 拍平)。
- **component 字段校验辅助**:输入时校验形如 `xxx/index` 的路径格式,并提示「需与前端 `views/**/index.vue` 对齐,否则运行时 fallback 404」。
- **添加子级**:行操作携带 `parent_id` 打开 Drawer;Button 节点不可再有子级(前端禁用)。
- **删除**:`confirm`(子节点级联警告)→ `DELETE /menus/{id}`。
- 变更后提示:「菜单变更对各用户在**下次登录/刷新访问检查**后生效」;当前用户立即 `accessStore.setIsAccessChecked(false)` + 重新生成路由(自助即时生效)。

### 3.3 API(`api/menus.ts`)

`fetchMenuTree` / `createMenu` / `updateMenu` / `deleteMenu` → `GET/POST /menus`、`PUT/DELETE /menus/{id}`(注意 update 走 patch 语义 body,字段对齐 `domain/api/menu.rs` 的 `Option<Option<T>>` 形状:未传 = 不变,传 null = 清空)。

### 3.4 文件清单(三页)

```text
views/users/
└── index.vue + modules/{schemas/{index,search-form,table-columns,form}.ts,
                         widgets/{form.vue, assign-roles-modal.vue}}
views/roles/
└── index.vue + modules/{schemas/{index,table-columns,form}.ts,
                         widgets/{form.vue, permission-matrix-modal.vue, assign-menus-modal.vue}}
views/menus/
└── index.vue + modules/{schemas/{index,table-columns,form}.ts,
                         widgets/form.vue}
```

---

## 4. 部署集成

### 4.1 构建与产物布局

```bash
cd ui && pnpm install && pnpm build:antdv-next   # turbo 过滤 @vben/web-antdv-next
# 产物 apps/web-antdv-next/dist/ → 同步到运行目录 static/ui/
```

- 提供根脚本 `scripts/deploy/sync-ui.sh`(quant-pivot 仓库侧):`rsync --delete dist/ → <运行目录>/static/ui/`。
- Rust 侧零改动:`WebConfig.serve_static_ui = true` + `static_ui_dir = "static/ui"`(`config/quant-pivot.toml` 或 `QUANT_PIVOT__WEB__SERVE_STATIC_UI`);`static_files.rs` 已实现 SPA fallback(extensionless → index.html no-cache;hashed 资产 immutable 缓存)。

### 4.2 开发模式

```ts
// apps/web-antdv-next/vite.config.ts proxy
'/api': { target: 'http://localhost:8080', changeOrigin: true, ws: true }   // ws: true 代理 /api/ws 升级
```

- `.env.development`:`VITE_GLOB_API_URL=/api`;WS 同源推导(`location.host`),无需独立 `VITE_WS_URL`(生产同源;开发经 vite ws 代理)。
- 后端 dev:`cargo run -- serve`(8080);前端 `pnpm dev:antdv-next`(5555/5173)。

### 4.3 产物预算与检查

- gzip 后首屏 JS ≤ 1.5MB,总产物 ≤ 8MB(ECharts/vxe-table 按需分包,`@vben/vite-config` 已有 chunk 策略,超标时审视 import 粒度)。
- CI 检查项:`pnpm typecheck`、`pnpm lint`、build 成功、产物体积阈值。

---

## 5. 验收清单

- [ ] Users:CRUD + 状态开关 + 重置密码 + 角色分配闭环;分配后目标用户权限即时生效(后端 Casbin reload 验证)
- [ ] Roles:权限矩阵行列与 `GET /permissions/catalog` 一致(不硬编码);保存后矩阵重读一致;菜单分配树正确预选;「按菜单推导权限」辅助可用
- [ ] 新建测试角色(仅 market:read + opportunity:read + 对应菜单)→ 新用户登录仅见 Markets/Opportunities 菜单且无任何 mutating 按钮
- [ ] Menus:树形展示/动态表单/子级创建/级联删除警告;新增页面菜单 component 与前端视图对齐后路由可达;当前用户菜单变更自助即时生效
- [ ] 内置 seed 角色与 super_admin 不可删除(操作项隐藏)
- [ ] `pnpm build:antdv-next` 产物经 `static/ui` 由 Rust 二进制服务:`/` 可登录使用、深链刷新不 404、`/api/*` 不被静态层吞掉
- [ ] 开发模式 vite 代理(REST + WS)全链路可用
- [ ] 产物体积达标;CI 四项检查通过
