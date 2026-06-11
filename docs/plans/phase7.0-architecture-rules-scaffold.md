# Phase 7.0 — UI 架构规则与脚手架收敛

> **产出**: web-antdv-next 目录规范 + adapter 层 + 组件交互统一范式 + 通用 hooks + types/i18n 分层 + `.cursor/rules/oxide-arb-ui.mdc`
>
> **前置**: 无(Phase 7 起点)
>
> **范式来源**: ng-gateway-ui(`apps/web-antd`)的生产实践,逐条核验后移植;vben 原生抽象(`packages/@core`、`packages/effects`)不重复造轮子

---

## 0. 总原则

1. **adapter 层为唯一入口**:页面**禁止**直接 import `@vben/plugins/vxe-table`、antdv-next 的 `Table` / `Form`;一律走 `#/adapter/vxe-table` 与 `#/adapter/form`。
2. **范式零例外**:列表页骨架、Drawer/Modal 协议、hooks 用法全局统一;任何页面偏离范式视为缺陷。
3. **结构化数据强约束**:跨页面/跨层的实体、枚举、DTO 必须落 `packages/types`;禁止页面内 ad-hoc interface 重复声明领域模型。
4. **抽象分层**:vben 框架级通用 → `packages/*`;oxide-arb 领域级复用 → `apps/web-antdv-next/src/shared/*`;单页面专用 → `views/<domain>/modules/*`。
5. **i18n 全覆盖**:所有用户可见文案走 `$t()`;schema 文件内同样如此。

---

## 1. 应用目录结构(目标态)

```text
apps/web-antdv-next/src/
├── adapter/                      # vben 抽象 → antdv-next 绑定(全局唯一)
│   ├── component/                # 表单组件注册(脚手架已有,按需扩展)
│   ├── form.ts                   # setupVbenForm + useVbenForm + z
│   └── vxe-table.ts              # setupVbenVxeTable + useVbenVxeGrid + cell 渲染器
├── api/
│   ├── request.ts                # RequestClient 双实例 + 拦截器链(7.1)
│   ├── index.ts                  # barrel
│   ├── core/                     # 框架契约域:auth.ts / user.ts / menu.ts
│   ├── system.ts                 # 业务域:一域一文件
│   ├── markets.ts / opportunities.ts / trades.ts / pnl.ts
│   ├── risk.ts / analytics.ts
│   ├── runtime-config.ts / control-factors.ts / replay.ts / operation-logs.ts
│   └── users.ts / roles.ts / menus.ts / permissions.ts
├── layouts/
│   ├── basic.vue                 # BasicLayout + #header-right-* 插槽接线(7.2)
│   ├── auth.vue
│   └── index.ts
├── locales/                      # 业务文案(page.* / entity.* / enum.*)
│   ├── index.ts
│   └── langs/{zh-CN,en-US}/{page,entity,enum}.json
├── router/
│   ├── access.ts                 # generateAccess(backend 模式,7.1)
│   ├── guard.ts                  # 权限守卫(保持 vben 骨架)
│   ├── menu-adapter.ts           # MenuTreeNode → RouteRecordStringComponent + 权限码聚合(7.1)
│   └── routes/                   # 仅 core 路由(登录/fallback);业务路由全部后端下发
├── shared/                       # oxide-arb 领域级复用(跨页面)
│   ├── components/               # 领域组件(图表封装/状态徽标/治理弹窗等)
│   └── composables/              # 领域 composable(use-oxide-ws 等)
├── store/                        # Pinia:auth + 领域实时 store(7.2)
│   ├── auth.ts
│   ├── system.ts / risk.ts / market.ts / trade.ts / opportunity.ts / pnl.ts
│   └── ws.ts                     # WS 连接状态
├── views/
│   ├── _core/                    # 登录/fallback/profile(保留 vben)
│   ├── dashboard/                # 每个目录与菜单 seed component 一一对应
│   ├── markets/ opportunities/ trades/
│   ├── risk/ blacklist/
│   ├── analytics/
│   ├── runtime-config/ control-factors/ publications/ replay/ audit/ operation-log/
│   └── users/ roles/ menus/
├── preferences.ts                # accessMode: 'backend' + defaultHomePath: '/dashboard'
├── bootstrap.ts / main.ts / app.vue
```

**清理项**(实现 7.1 时一并执行):删除 `views/demos/`、`views/dashboard/analytics|workspace`(demo 内容)、`router/routes/modules/{demos,vben,dashboard}.ts`、`locales/langs/*/demos.json`;修复 `api/core/index.ts` 中悬空的 `./upload` 导出。

### 1.1 页面目录范式(强制)

每个业务页面 = `index.vue` + `modules/`:

```text
views/<domain>/
├── index.vue                     # 编排层:grid/布局 + 弹层连接 + 业务 handler;不含 schema 细节
└── modules/
    ├── schemas/
    │   ├── index.ts              # barrel,唯一出口
    │   ├── search-form.ts        # searchFormSchema: VbenFormSchema[]
    │   ├── table-columns.ts      # useColumns(onActionClick): VxeGridProps['columns']
    │   ├── form.ts               # formSchema 或多个 useXxxFormSchema() factory
    │   └── options.ts            # (可选)枚举 → Select options 映射
    └── widgets/
        ├── form.vue              # 创建/编辑 Drawer(connectedComponent)
        ├── <x>-drawer.vue        # 详情只读抽屉
        ├── <x>-modal.vue         # 子资源/轻确认 Modal
        └── <x>-panel.vue         # 页面内图表/面板子组件
```

**schemas 拆分粒度规则**:

- 一文件一职责:search-form / table-columns / form 各一个文件;`index.ts` barrel 统一导出。
- 表格列必须是 `useColumns(onActionClick)` **factory 函数**(列内含 `$t()` 与操作列回调,需运行时求值)。
- 多段表单(Tab/分组)在 `form.ts` 内导出**多个 factory**(`useBasicFormSchema` / `useXxxFormSchema`),不要单一大数组。
- 页面局部类型(仅该页使用的 view-model)放 `schemas/types.ts`;一旦被第二个页面引用,立刻上移 `packages/types`。

---

## 2. adapter 层设计

### 2.1 `adapter/vxe-table.ts`

以 ng-gateway `apps/web-antd/src/adapter/vxe-table.ts` 为蓝本,组件库换 antdv-next:

```ts
setupVbenVxeTable({
  configVxeTable: (vxeUI) => {
    vxeUI.setConfig({
      grid: {
        align: 'center',
        border: false,
        columnConfig: { resizable: true },
        formConfig: { enabled: false },        // 搜索一律走 formOptions,禁用 vxe 内建表单
        minHeight: 180,
        proxyConfig: {
          autoLoad: true,
          response: { result: 'items', total: 'total', list: '' },  // 对齐后端 Paginated{items,total}
          showActiveMsg: true,
          showResponseMsg: false,
        },
        round: true,
        showOverflow: true,
        size: 'small',
      },
    });
    // ... cell 渲染器注册(下表)
  },
  useVbenForm,
});

export const useVbenVxeGrid = /* 类型化包装,同 ng-gateway */;
export type OnActionClickParams<T> = { code: string; extra?: Recordable<any>; row: T };
export type OnActionClickFn<T> = (params: OnActionClickParams<T>) => void;
```

**cell 渲染器清单**(注册名即契约,列定义用 `cellRender: { name: '...' }`):

| 渲染器 | 来源 | 行为 |
|---|---|---|
| `CellOperation` | 移植 ng-gateway | 行操作按钮组(text/icon/tooltip/dropdown/show 谓词),回调 `attrs.onClick({code,row})` |
| `CellTag` | 移植 ng-gateway | options `{label,value,color}` 映射 Tag |
| `CellSwitch` | 移植 ng-gateway | 开关 + `beforeChange` 异步钩子 + 行级 loading |
| `CellLink` | 移植 ng-gateway | link button |
| `CellUsd` | **新增** | string Decimal → `$1,234.56` 格式化;正绿负红;空值 `—` |
| `CellPrice` | **新增** | string Decimal → 4 位小数价格;不着色 |
| `CellBps` | **新增** | 数值 → `450 bps`;按阈值着色(props 可配) |
| `CellPercent` | **新增** | 0–1 string → `68.5%` |
| `CellExecutionMode` | **新增** | `dry_run/paper/live` → Tag(灰/蓝/红)+ i18n `enum.executionMode.*` |
| `CellBreakerState` | **新增** | breaker FSM 状态 → 状态点 + Tag(`enum.breakerState.*`) |
| `CellMarketId` | **新增** | `0x…` 66 位截断显示(前 6 后 4)+ hover 全量 + 点击复制 |
| `CellDateTime` | **新增** | ISO string → 本地时区 `YYYY-MM-DD HH:mm:ss`,hover 显示 UTC |

> 金额/价格/bps 渲染逻辑(纯函数)实现于 `apps/web-antdv-next/src/shared/components/format/`(或直接 util),渲染器只做接线——便于 Overview 卡片等非表格场景复用同一格式化。

### 2.2 `adapter/form.ts`

照搬 ng-gateway 结构,适配 antdv-next:

- `setupVbenForm<ComponentType>`:`baseModelPropName: 'value'` + `modelPropNameMap`(`Checkbox/Radio/Switch → checked`,`Upload → fileList`)。
- `defineRules`:i18n 化 `required` / `selectRequired`(`ui.formRules.*`)。
- 导出:`useVbenForm`、`VbenFormSchema`、`z`(zod)。

### 2.3 禁止事项

- 页面/组件中直接 `import { useVbenVxeGrid } from '@vben/plugins/vxe-table'` → 必须 `from '#/adapter/vxe-table'`。
- 在列定义里手写 `h(Tag, ...)` 渲染状态 → 必须用/扩展 cell 渲染器。
- 表格内联分页逻辑 → 必须 `proxyConfig.ajax.query` + `fetchXxxPage`。

---

## 3. 列表页骨架范式(强制)

所有「搜索 + 表格 + 行操作 + 弹层」页面遵循同一骨架(蓝本:ng-gateway `views/system/user/index.vue`):

```vue
<script lang="ts" setup>
const { handleRequest } = useRequestHandler();

const formOptions: VbenFormProps = { collapsed: true, schema: searchFormSchema, showCollapseButton: true, submitOnEnter: false };

const gridOptions: VxeGridProps<XxxInfo> = {
  columns: useColumns(onActionClick),
  height: 'auto',
  keepSource: true,
  proxyConfig: { ajax: { query: async ({ page }, formValues) =>
    fetchXxxPage({ page: page.currentPage, size: page.pageSize, ...formValues }) } },
  toolbarConfig: { custom: true, refresh: true, zoom: true },
};

const [Grid, gridApi] = useVbenVxeGrid({ formOptions, gridOptions });
const [FormDrawer, formDrawerApi] = useVbenDrawer({ connectedComponent: XxxForm });

function onActionClick({ code, row }: OnActionClickParams<XxxInfo>) {
  switch (code) { /* edit / delete / ... 分发到 handler */ }
}
</script>

<template>
  <Page auto-content-height>
    <Grid>
      <template #toolbar-actions>
        <Button v-access:code="'xxx:create'" type="primary" @click="handleCreate">…</Button>
      </template>
    </Grid>
    <FormDrawer @submit="handleFormSubmit" />
  </Page>
</template>
```

要点:

1. 容器统一 `Page auto-content-height`(`@vben/common-ui`);grid `height: 'auto'`。
2. 新建按钮放 `#toolbar-actions`,带 `v-access:code`。
3. 行操作统一 `CellOperation` 列 + `onActionClick` switch 分发;操作项用 `show: (row) => boolean` 谓词控制可见性,权限控制在 `useColumns` 内通过 `hasAccessByCodes` 注入。
4. 删除/危险操作统一 `confirm()`(`@vben/common-ui`)+ `common.action.*` 文案;成功提示 `message.success($t('common.action.xxxSuccess'))` 后 `gridApi.query()` 刷新。
5. 页面 handler 一律经 `handleRequest`(或治理动作经 `useGovernedAction`),**禁止**裸 try/catch。

---

## 4. Drawer / Modal 协议(强制)

### 4.1 选型

| 场景 | 组件 |
|---|---|
| 创建/编辑表单(字段 ≥4 或含分组) | `useVbenDrawer`(`class: 'w-1/2'` 起) |
| 详情只读(决策链/审计 diff/市场详情) | `useVbenDrawer`(只读渲染,无 Form) |
| 轻量确认 + 单字段输入(治理 reason 等) | `useVbenModal` 或 `prompt()` |
| 子资源管理(角色的权限矩阵/菜单分配) | `useVbenModal`(内嵌树/矩阵) |

### 4.2 connectedComponent 协议(蓝本:ng-gateway `user/modules/widgets/form.vue`)

- 弹层一律是独立 `modules/widgets/*.vue`,**不内联** template。
- 父页:`const [FormDrawer, formDrawerApi] = useVbenDrawer({ connectedComponent: XxxForm })`;打开时 `formDrawerApi.setData({ type: FormOpenType.CREATE|EDIT, id }).setState({ title }).open()`。
- 子组件:
  - `useVbenForm({ handleSubmit, schema, showDefaultActions: false })` + `useVbenDrawer({ onConfirm: () => formApi.validateAndSubmitForm(), onOpenChange: init })`;
  - `init()` 中 `modalApi.getData<FormOpenData>()` 取 `type/id`,EDIT 时 `handleRequest(() => getXxxById(id), (data) => formApi.setValues(data))`;
  - `handleSubmit` 中 `emit('submit', type, id, values)` 并 `modalApi.close()`。
- 父页 `handleFormSubmit(type, id, values)` 统一调 create/update API + `gridApi.query()`。

> 该协议保证:API 调用归属父页(便于权限/治理统一)、弹层组件可复用、表单校验集中在 schema。

---

## 5. 通用 hooks(`packages/effects/hooks`)

### 5.1 `useRequestHandler`(移植 ng-gateway,原样)

```ts
const { handleRequest } = useRequestHandler();
await handleRequest(() => deleteUser(id), () => message.success(...), (err) => ...);
```

### 5.2 `useGovernedAction`(新增,oxide 治理专用)

覆盖全部 10 个 governed 路由的统一交互:

```ts
const { governed } = useGovernedAction();

// 弹出治理确认 Modal:展示动作摘要 + 必填 reason 文本域 + acting-role 选择器
// (默认取用户唯一可用角色;多角色时下拉,选项 = userStore.roles 中 enabled 且持有该权限的角色)
const result = await governed(
  ({ actingRole, reason }) => resetCircuitBreaker({ reason }, { headers: { 'X-Acting-Role': actingRole } }),
  {
    title: $t('governance.confirm.resetBreaker'),
    danger: true,              // live 模式切换、emergency publish 等强确认(需输入确认词)
    permissionCode: 'risk:reset',
  },
);
// 取消返回 null;失败经 useRequestHandler 路径返回 null 并已 toast
```

实现要点:

- 内部组合 `useRequestHandler`;Modal 用 `useVbenModal` 函数式打开(`shared/components/governed-action-modal.vue`)。
- `reason` 必填(min 4 字符);`actingRole` 从 `userStore.userInfo.roles` 过滤 `status === enabled`;`super_admin` 角色直接可选自身。
- `danger: true` 时要求输入确认词(如目标模式名 `live`)才允许提交。
- 类型:`governed<T>(fn: (ctx: GovernedContext) => Promise<T>, opts: GovernedOptions): Promise<T | null>`。

### 5.3 `usePolling`(新增)

无 WS 通道数据的补偿轮询(replay run 状态、materialization 进度):

```ts
const { start, stop } = usePolling(() => getReplayRun(runId), {
  interval: 5_000,
  pauseOnHidden: true,          // document.visibilityState 暂停
  until: (run) => run.status === 'completed' || run.status === 'failed',
});
```

### 5.4 放置与归属

- 三个 hooks 均放 `packages/effects/hooks/src/`(`use-request-handler.ts` / `use-governed-action.ts` / `use-polling.ts`),经包 barrel 导出。
- `useGovernedAction` 依赖的治理 Modal 组件放 `apps/web-antdv-next/src/shared/components/`(领域 UI),hook 通过参数注入打开函数,保持 `packages` 不反向依赖 app——若实现中发现注入过重,可整体下沉 app `shared/composables/`,以实现简洁为准,**二选一,不许两处都有**。

---

## 6. 组件抽象位置决策表

| 组件类型 | 位置 | 示例 |
|---|---|---|
| vben 框架级(与 oxide 无关) | `packages/effects/common-ui` 等(已有,不动) | Page / JsonViewer / fallback |
| oxide 领域级、跨页面复用 | `apps/web-antdv-next/src/shared/components/` | `EchartsCard`(标题+loading+空态的图表壳)、`StatCard`(KPI 卡)、`BreakerBadge`、`ExecutionModeTag`、`MarketIdText`、`GovernedActionModal`、`ReasonField` |
| 顶栏 widget | `shared/components/header/` | `SystemStatusIndicator` / `ExecutionModeSwitcher` / `WsStatusBadge`(7.2) |
| 偏好抽屉扩展块 | `packages/effects/layouts/src/widgets/preferences/blocks/runtime-config/` | fork 级扩展(7.6,参照 ng-gateway system/logging 块) |
| 单页专用 | `views/<domain>/modules/widgets/` | 决策链时间线、orderbook 面板 |

升级规则:`modules/widgets` 内组件被第二个页面需要时,**移动**(不是复制)到 `shared/components/` 并改为通用 props。

---

## 7. types 分层(`packages/types`)

```text
packages/types/src/
├── index.ts                  # 既有 re-export + oxide barrel
└── oxide/
    ├── common.ts             # ApiEnvelope<T> / Paginated<T> / PageQuery / TimeRangeQuery / IdType
    ├── enums.ts              # ExecutionMode / BreakerState / UserStatus / RoleStatus / MenuKind /
    │                         #   OperationOutcome / PublicationMode|Status / FactorStatus / ReplayRunStatus ...
    ├── rbac.ts               # UserView / RoleInfo / MenuInfo / MenuTreeNode / Permission / PermissionCatalogEntry / MeResponse
    ├── system.ts             # SystemStatus / HealthReport / ModeTransitionReport
    ├── market.ts             # MarketView / MarketBookView / BookLevel
    ├── opportunity.ts        # OpportunityView / OpportunityAuditRow / OpportunityStatsRow
    ├── trade.ts              # TradeView / TradeDecisionRow
    ├── pnl.ts                # DailyReport / WeeklyReport / LivePnlView
    ├── risk.ts               # RiskEngineStateView / PositionView / BlacklistInfo
    ├── runtime-config.ts     # RuntimeConfigCurrentView / VersionView / SchemaFieldView / ActivationInfo
    ├── control-factor.ts     # ControlFactorValueInfo / PublicationView / AuditChainResponse / ShadowDecisionRow
    ├── replay.ts             # ReplayCreateRequest / ReplayEnqueueView / MaterializationRunView / StageReport
    ├── operation-log.ts      # OperationLogView
    └── ws.ts                 # WsChannel / WsEnvelope<T> / WsCommand / SyncSnapshot / 各通道 payload
```

规则:

1. **字段命名与后端 wire 格式一致**(snake_case JSON → TS interface 同名 snake_case 字段;不做 camelCase 转换层,避免双向映射成本)。
2. **金额/价格/份额/bps 一律 `string`** 承载 `rust_decimal`(类型别名 `UsdString` / `PriceString` / `SharesString` 增强语义);**禁止** `number` 承载金额。
3. 枚举值与 Rust serde 输出一致(如 `'dry_run' | 'paper' | 'live'`),用 `as const` 对象 + 派生 union type 的范式(对齐 ng-gateway `CommonStatus`)。
4. **仅 API 入参**(query/body 形状)留在 `api/<domain>.ts` 的 `XxxApi` namespace;响应实体一律 `packages/types`。
5. `apps` 引用统一走 `@vben/types`(`import type { MarketView } from '@vben/types'`)。

---

## 8. i18n 分层

| 层 | 路径 | 内容 | key 前缀 |
|---|---|---|---|
| 框架共享 | `packages/locales/src/langs/{zh-CN,en-US}/` | vben 既有 `common/authentication/preferences/ui` + 新增治理通用文案 | `common.*` `ui.*` `preferences.*` `authentication.*` `governance.*` |
| App 业务 | `apps/web-antdv-next/src/locales/langs/{zh-CN,en-US}/` | 页面文案 `page.json`、实体名 `entity.json`、枚举值 `enum.json` | `page.*` `entity.*` `enum.*` |

规则:

1. **判定标准**:文案是否与 oxide 领域耦合——「确认/重置成功/必填」等进 `packages/locales`;「市场/熔断器/执行模式」等进 app locales。
2. 菜单标题:seed v2 存 i18n key(`page.menu.markets`),由 vben `meta.title` 自动翻译;`page.menu.*` 维护在 app `page.json`。
3. 枚举展示:`enum.executionMode.live` / `enum.breakerState.open` …,cell 渲染器与表单 options 共用。
4. 实体名:`entity.market` / `entity.role` …,配合 `common.action.deleteConfirm` 模板插值。
5. 偏好抽屉「运行时配置」Tab 文案进 `packages/locales` 的 `preferences.json`(`preferences.runtimeConfig.*`,因为块组件位于 packages 层)。

---

## 9. 命名规范

| 对象 | 规范 | 示例 |
|---|---|---|
| 目录 | kebab-case | `control-factors/`、`operation-log/` |
| Vue 组件文件 | kebab-case | `market-detail-drawer.vue` |
| 组件 `defineOptions.name` | PascalCase | `MarketDetailDrawer` |
| API 函数 | `fetchXxxPage / getXxxById / createXxx / updateXxx / deleteXxx / 动作动词Xxx` | `activateRuntimeConfigVersion` |
| API namespace | `XxxApi` + `const base` + 函数式带参路径 | `MarketApi.book = (id) => \`${base}/${id}/book\`` |
| Pinia store | `useXxxStore`,文件 `store/<domain>.ts` | `useRiskStore` |
| composable | `useXxx`,文件 `use-xxx.ts` | `use-oxide-ws.ts` |
| i18n key | dot-path camelCase 叶子 | `page.markets.subscribeConfirm` |
| 权限码 | 后端原样 `resource:operation`(snake_case) | `runtime_config:activate` |

---

## 10. `.cursor/rules/oxide-arb-ui.mdc`

本 phase 同步产出长期规则文档(已随本文档落地,见 `oxide-arb/.cursor/rules/oxide-arb-ui.mdc`),内容为本文档 §0–§9 的浓缩可执行版,glob 限定 `oxide-arb-ui/**`。规则文档与本文档冲突时,以本文档为准并同步修订规则。

---

## 11. 验收清单

- [ ] `adapter/vxe-table.ts` 完成全局配置 + 12 个 cell 渲染器注册,导出 `OnActionClickParams/Fn`
- [ ] `adapter/form.ts` 完成 `setupVbenForm` + i18n rules
- [ ] `packages/effects/hooks` 提供 `useRequestHandler` / `useGovernedAction` / `usePolling` 并有类型完备的签名
- [ ] `packages/types/src/oxide/` 基础骨架(common/enums)建立并被 `@vben/types` barrel 导出
- [ ] demo 路由/视图/locale 清理完毕,`pnpm dev:antdv-next` 可启动
- [ ] `.cursor/rules/oxide-arb-ui.mdc` 落地
- [ ] 全仓 grep 验证:无页面直接 import `@vben/plugins/vxe-table`;无 `number` 类型金额字段
