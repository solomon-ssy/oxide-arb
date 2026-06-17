# Phase 7 — UI 层(伞形索引)

> **产出**: `oxide-arb-ui/apps/web-antdv-next` 完整 Dashboard 应用 + `.cursor/rules/oxide-arb-ui.mdc` 架构规则
>
> **前置条件**: Phase 0–6 已全部落地(以当前实现为准);`oxide-arb-web` 提供 75 个受保护 REST 路由 + 12 个 WS 推送通道 + Casbin RBAC + 菜单 seed
>
> **脚手架**: `oxide-arb-ui`(vue-vben-admin 5.7.0 fork)中的 `apps/web-antdv-next`;monorepo 其余 app 暂不清理,所有工作在 `web-antdv-next` + `packages/*` 增量进行
>
> **验收标准**: 见 §7 整体验收清单;各子 phase 文档自带独立验收

---

## 0. 子文档地图

Phase 7 拆分为 8 个子 phase,按依赖顺序推进:

| 子文档 | 范围 | 依赖 |
|---|---|---|
| [phase7.0-architecture-rules-scaffold.md](phase7.0-architecture-rules-scaffold.md) | 架构规则、目录规范、adapter 层、组件交互统一范式、通用 hooks、types/i18n 分层、`.cursor/rules` 落地 | — |
| [phase7.1-rbac-auth-request-types.md](phase7.1-rbac-auth-request-types.md) | 请求层改造、认证流、backend 动态菜单 RBAC、权限码聚合、治理动作 composable、基础类型层、菜单 seed v2 对齐 | 7.0 |
| [phase7.2-overview-realtime-header.md](phase7.2-overview-realtime-header.md) | WebSocket 实时数据层、Pinia 领域 store、顶栏三组件(状态灯/模式切换/WS 徽标)、Overview 首页 | 7.1 |
| [phase7.3-business-markets-opportunities-trades.md](phase7.3-business-markets-opportunities-trades.md) | Markets / Opportunities / Trades 三个业务页 | 7.2 |
| [phase7.4-risk.md](phase7.4-risk.md) | Risk Overview 页 + Blacklist 页(治理动作) | 7.2 |
| [phase7.5-analytics.md](phase7.5-analytics.md) | Analytics 分析页(多图表) | 7.2 |
| [phase7.6-operations-governance-preferences.md](phase7.6-operations-governance-preferences.md) | Runtime Config 版本页、Control Factors、Publications、Replay、Audit Chain、Operation Log + 偏好抽屉「运行时配置」编辑器 | 7.2 |
| [phase7.7-rbac-admin-deploy.md](phase7.7-rbac-admin-deploy.md) | Users / Roles / Menus 三个 RBAC admin 页 + 构建产物集成部署 | 7.1 |

并行性:7.3 / 7.4 / 7.5 / 7.6 / 7.7 在 7.2 完成后可并行。

---

## 1. 已确认的设计决策

1. **RBAC 接入模式**:`accessMode: 'backend'` 动态菜单。菜单/路由由后端按角色下发(`GET /api/menus/accessible`),前端将 `component` 字符串映射到 `views/**/index.vue`;按钮级权限从菜单树聚合 `permission_code` 写入 `accessStore.accessCodes`,通过 `v-access:code` / `AccessControl` 网关。
2. **RBAC admin 范围**:完整套件——用户管理、角色管理(含权限矩阵分配 + 菜单分配)、菜单管理。
3. **配置分层**:runtime-config 的「编辑当前配置 → 提交新版本并激活(reason + X-Acting-Role)」整合进右上角**偏好设置抽屉**新 Tab;版本历史 / 激活 / 回滚 / diff 作为「Operations 治理」菜单下的独立页面;**不做**独立的「配置编辑」菜单页。
4. **无单独 System Control 页**:halt / resume / switch-mode 等系统控制动作收敛到**顶栏组件 + Overview 首页**,菜单 seed 同步调整(见 7.1 §seed v2)。
5. **文档即契约**:每个页面的内容、布局、交互、权限、API、WS 订阅、schemas/widgets 文件清单全部在子文档中写死;实现不得偏离,偏离需先改文档。

---

## 2. 技术栈

| 技术 | 版本 | 用途 |
|---|---|---|
| Vue 3 | ^3.5 | UI 框架 |
| TypeScript | ^5.x | 类型安全 |
| Vite | ^6/^7 | 构建(`@vben/vite-config`) |
| antdv-next | monorepo catalog | 组件库(`web-antdv-next` 默认) |
| vxe-table | `@vben/plugins/vxe-table` | 表格(经 adapter 封装) |
| Pinia | catalog | 状态管理(`@vben/stores` + app store) |
| ECharts | `@vben/plugins/echarts` | 图表(PnL 曲线、edge 分布、gauge、heatmap) |
| VueUse | catalog | `useWebSocket` 等工具 |
| pnpm + Turbo | 11.x / 2.x | monorepo 工具链 |

---

## 3. 信息架构(IA)

### 3.1 菜单树(与后端 seed v2 一一对应)

菜单由后端 `crates/oxide-arb-models/src/seed/rbac/menus.rs` 播种、按角色过滤后经 `GET /api/menus/accessible` 下发。前端 `views/` 目录与 `component` 字符串严格对齐:

```text
Dashboard
└── Overview            /dashboard        dashboard/index        (无权限码,登录即见,区块按权限降级)
Trading
├── Markets             /markets          markets/index          market:read
├── Opportunities       /opportunities    opportunities/index    opportunity:read
└── Trades              /trades           trades/index           trade:read
Risk
├── Risk Overview       /risk             risk/index             risk:read
└── Blacklist           /blacklist        blacklist/index        blacklist:read
Analytics
└── Analytics           /analytics        analytics/index        analytics:read
Operations
├── Runtime Config      /runtime-config   runtime-config/index   runtime_config:read
├── Control Factors     /control-factors  control-factors/index  control_factor:read
├── Publications        /publications     publications/index     control_factor:read
├── Replay              /replay           replay/index           replay:read
├── Audit Chain         /audit            audit/index            audit:read
└── Operation Log       /operation-log    operation-log/index    operation_log:read
Access Control
├── Users               /users            users/index            user:read
├── Roles               /roles            roles/index            role:read
└── Menus               /menus            menus/index            menu:read
```

按钮级权限码(`MenuKind::Button` 节点)挂载关系见 [phase7.1](phase7.1-rbac-auth-request-types.md) §菜单 seed v2 对齐清单。`defaultHomePath = /dashboard`。

### 3.2 顶栏

```text
[Logo: oxide-arb] [面包屑] ... [系统状态指示灯] [执行模式切换器] [WS 连接徽标] [全局搜索] [偏好按钮] [主题] [语言] [全屏] [通知] [用户下拉]
```

- **系统状态指示灯**:`breaker_state` + halted 聚合(绿/黄/红),点击弹出系统状态 Popover(含 halt/resume 治理动作)。
- **执行模式切换器**:显示 `dry_run | paper | live`,运行时切换走治理流(`POST /api/system/mode`,`X-Acting-Role` + reason);`live` 切换二次确认。
- **WS 连接徽标**:connected / reconnecting / disconnected 三态。
- 其余为 vben 内置 widget,经 `preferences.widget.*` 控制。

详细规格见 [phase7.2](phase7.2-overview-realtime-header.md)。

### 3.3 偏好设置抽屉(右上角)

在 vben 内置 Tabs(外观/布局/快捷键/通用)基础上新增:

- **「运行时配置」Tab**:schema 驱动表单(`GET /api/runtime-config/schema`),编辑当前生效配置,提交即「创建新版本 + 激活」治理流。参考 ng-gateway `preferences/blocks/system` 的 `CardShell` 范式。

详细规格见 [phase7.6](phase7.6-operations-governance-preferences.md)。

---

## 4. 跨切面契约总览

以下契约在 [phase7.0](phase7.0-architecture-rules-scaffold.md)(范式)与 [phase7.1](phase7.1-rbac-auth-request-types.md)(实现)中定义,**所有页面文档默认引用,不重复展开**:

| 契约 | 要点 |
|---|---|
| HTTP 请求层 | 统一注入 `Authorization: Bearer` / `Accept-Language` / `Accept-Api-Version: v1` / `X-Request-Id`;envelope `{code:200,message,data}` 解包;401 → refresh 轮换(双 token 同落) |
| API 模块 | 一域一文件 `api/<domain>.ts`;`XxxApi` namespace(path 常量 + 入参 interface);`fetchXxxPage / getXxxById / createXxx / ...` 命名 |
| 治理动作 | `useGovernedAction()`:弹窗收集 `reason` + 从用户启用角色选 `X-Acting-Role`;覆盖全部 10 个 governed 路由 |
| WebSocket | `ws://host/api/ws?token=...`;单例 composable;断线重连 + `sync`;12 通道分发 Pinia store;**禁止**订阅 `trade.opened` / `opportunity.expired`(已删除) |
| 表格/表单 | 一律走 `#/adapter/vxe-table` 的 `useVbenVxeGrid` 与 `#/adapter/form` 的 `useVbenForm`;弹层走 `useVbenDrawer` / `useVbenModal` + `connectedComponent` 协议 |
| 通用 hooks | `useRequestHandler` / `useGovernedAction` / `usePolling`,页面 handler 禁止裸 try/catch |
| 页面结构 | `views/<domain>/index.vue` 编排 + `modules/schemas`(search-form / table-columns / form 单一职责)+ `modules/widgets`(Drawer/Modal/图表) |
| 类型 | 领域实体/枚举/DTO 在 `packages/types/src/oxide/*`;金额/价格/bps 一律 string 承载 Decimal |
| i18n | 框架文案 `packages/locales`;业务文案 `apps/web-antdv-next/src/locales`(`page.*` / `entity.*` / `enum.*`) |

每个页面子文档统一给出:**路由 / 权限(菜单码+按钮码)/ 布局 / 交互 / REST API(method+path+权限+req/resp)/ WS 订阅 / schemas 文件清单 / widgets 文件清单 / grid-drawer-modal 选型与 cell 渲染器 / i18n key 前缀 / 涉及 types**。

---

## 5. 后端事实基线(以代码为准,文档据此编写)

- 路由总量:75 个受保护 `/api/*` + 2 个公开 auth + 1 个 WS + 3 个根探针(`/health` `/ready` `/metrics`);路径**无** URL 版本号,版本经 `Accept-Api-Version: v1` 头协商。路由清单:`crates/oxide-arb-web/src/routes/mod.rs` `protected_route_specs()`。
- 认证:`POST /api/auth/login|refresh|logout` + `GET /api/auth/me`;JWT HS256,access 900s / refresh 7d,refresh 轮换 + Redis 黑名单(fail-closed)。
- RBAC:Casbin `(sub, obj, act)`,权限码 `resource:operation`(56 对,`crates/oxide-arb-models/src/enums/rbac.rs`);内置角色 `super_admin / admin / risk_owner / operator / viewer / emergency_operator`;**无** `/auth/codes` 端点——前端从 `/auth/me` 菜单树聚合权限码。
- 治理路由(10 个):`X-Acting-Role` 头 + body `reason`——`system/mode`、`runtime-config/versions[/activate|/rollback]`、`control-factors/{id}/reject`、`publications/shadow|publish|emergency|{id}/rollback`、`risk/circuit-breaker/reset`、`risk/blacklist[+remove]`、`replay`。
- WS:`GET /api/ws?token=<jwt>`;客户端指令 `subscribe / unsubscribe / sync / ping`;订阅需对应资源 `read` 权限;12 个推送通道(见 7.2);`trade.opened` / `opportunity.expired` 已在 Phase 6.7 删除。
- 静态服务:`crates/oxide-arb-web/src/static_files.rs`,`WebConfig.serve_static_ui` + `static_ui_dir`(默认 `static/ui`),SPA fallback,非嵌入二进制。

---

## 6. 已知数据源缺口与后端对齐项

实现各页面时如确认缺口仍存在,按下表处理(标注于对应子文档,实现时同步补后端):

| 缺口 | 影响页面 | 处理 |
|---|---|---|
| ~~无「账户余额 / 可用资金」端点~~ | Overview KPI | **已补齐** `GET /api/system/balance`（`available_for_sizing_usd`、`binding_exposure_limit`、integrity counts）；Overview 应优先使用该端点而非本地重算 |
| 无 replay run **列表**端点(仅 `GET /api/replay/{run_id}`) | Replay 页 | **已补齐** `GET /api/replay` 分页列表 + WS `materialization.run_update` 列表刷新 |
| 菜单 seed v1 与最终 IA 有差异(PnL 页 / Materializations 页 / System Control 页冗余) | 全局导航 | 7.1 给出 seed v2 对齐清单(后端小改:bump `MENUS_SEED` version + checksum) |
| 菜单 `title` 为英文明文 | 菜单国际化 | seed v2 将 `title` 改为 i18n key(`page.menu.*`),前端 `meta.title` 走 `$t` |

---

## 7. 整体验收清单

- [ ] 8 个子 phase 全部通过各自验收
- [ ] 登录 → `/auth/me` → 动态菜单/路由生成 → 按钮级 `v-access` 全链路工作;`viewer` 角色看不到任何 mutating 按钮
- [ ] 全部 API 请求携带 `Accept-Api-Version: v1` + Bearer + `X-Request-Id`;治理请求带 `X-Acting-Role` + `reason`
- [ ] WS 断线 ≤5s 自动重连,重连后 `sync` 恢复全量状态;顶栏徽标三态正确
- [ ] 执行模式运行时切换(治理流)生效并经 WS `system.status` 回显到顶栏
- [ ] 偏好抽屉「运行时配置」Tab 完成「编辑 → 创建版本 → 激活」闭环,审计可在 Operation Log / Audit Chain 页查到
- [ ] 所有列表页走 `useVbenVxeGrid` + adapter cell 渲染器;所有表单弹层走 `useVbenDrawer`/`useVbenModal` + `connectedComponent` 协议,零例外
- [ ] `pnpm build` 产物经 `static/ui` 由 Rust 二进制服务,SPA 刷新/深链正常
- [ ] Dark/Light 主题、zh-CN/en-US 双语言完整覆盖
- [ ] `.cursor/rules/oxide-arb-ui.mdc` 与最终实现一致
