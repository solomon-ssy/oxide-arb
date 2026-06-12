# Phase 7.2 — WebSocket 实时数据层、顶栏组件与 Overview 首页

> **产出**: `use-oxide-ws` 单例 + 领域 Pinia store 簇 + 顶栏三组件(系统状态灯 / 执行模式切换 / WS 徽标)+ Overview 首页
>
> **前置**: [phase7.1](phase7.1-rbac-auth-request-types.md)(认证 + 权限码 + 治理设施)

---

## 1. WebSocket 实时数据层

### 1.1 连接管理(`shared/composables/use-oxide-ws.ts`)

单例 composable(模块级状态,多组件共享同一连接):

```ts
export function useOxideWs() {
  // VueUse useWebSocket 封装:
  // url: `${wsBase}/api/ws?token=${encodeURIComponent(accessStore.accessToken)}`
  // autoReconnect: { retries: Infinity, delay: 1000(指数退避至 30s) }
  // heartbeat: { message: '{"action":"ping"}', interval: 15000, pongTimeout: 30000 }
  // onConnected: 按 accessCodes 批量 subscribe 全局通道 + send({action:'sync'}) + 重放 per-market 订阅
  // onMessage: dispatch(envelope) → 各 store
  return { status, subscribeMarket, unsubscribeMarket, connect, disconnect };
}
```

要点:

- **生命周期**:登录成功(`isAccessChecked` 后)在 `basic.vue` 布局挂载时 `connect()`;登出 `disconnect()`。token 刷新后**不主动重连**(连接期校验只在握手时);若服务端因 token 失效断开,重连时自动取最新 token。
- **`sync` 快照**:`{type:'sync', data: SyncSnapshot}`,字段(按权限省略):`system_status / risk / open_positions / pnl / recent_opportunities`;dispatch 时整体覆盖对应 store(重连后状态收敛)。
- **订阅模型**:后端 broadcaster 仅向**已 subscribe** 的 session 推送(connect 时服务端只主动 push 一次 `system.status`)。前端 onConnected 按 `accessCodes` 过滤通道权限表后批量 `{action:'subscribe', channel}` 全部授权全局通道,再发 `sync`;`market.book_update` 为 per-market,经 `subscribeMarket(marketId)` 发送 `{action:'subscribe', channel:'market.book_update', market_id}`,组件卸载时 unsubscribe;连接级 refCount 注册表保证重连后重放。
- **错误帧**:`{type:'error'}` 记 console.warn,不 toast(避免风暴)。
- **禁止**订阅 `trade.opened` / `opportunity.expired`(后端已删除)。

### 1.2 通道 → store 分发表

| WS `type` | payload | 目标 store | 动作 |
|---|---|---|---|
| `system.status` | `SystemStatus` | `useSystemStore` | 覆盖 `status`;后端 **5s 周期广播** + catalog sync / 控制面操作 **即时 nudge** |
| `system.alert` | `{level,message}` | — | `notification[level]` 全局提示 |
| `risk.circuit_breaker` | `{level,reason}` | `useRiskStore` | 更新 breaker + 红色 notification |
| `risk.position_update` | `PositionInfo` | `useRiskStore` | upsert `positions` |
| `market.resolved` | `{market_id,outcome}` | `useMarketStore` | 标记 resolved + info 提示 |
| `market.book_update` | `MarketBookView` | `useMarketStore` | 写 `books[market_id]` |
| `control.published` | `{publication_id,mode}` | — | info 提示;Publications 页 `usePolling`/手动刷新 |
| `config.activated` | `{version_id}` | `useSystemStore` | 记录 `activeConfigVersion` + info 提示 |
| `opportunity.detected` | `OpportunityView`(后端 wire 统一,与 `sync.recent_opportunities` 同形) | `useOpportunityStore` | 头插 `feed`(环形缓冲,cap 200) |
| `trade.filled` | `TradeView`(后端 wire 统一,与 REST `GET /trades` 同形;forensic 字段已剥离) | `useTradeStore` | 头插 `recent`(cap 50) |
| `trade.settled` | `{trade_id,outcome,pnl}` | `useTradeStore` | 更新对应 trade |
| `pnl.update` | `{daily,total}` | `usePnlStore` | 覆盖 + 追加 `intradaySeries` 点(供曲线实时延伸) |
| `materialization.run_update` | `ControlFactorMaterializationRunView` | `useReplayStore` | upsert 活跃 run(Queued/Running);terminal 后移除 |
| `sync` | `SyncSnapshot` | 多 store | 全量覆盖(含 `active_materialization_runs`) |
| `pong` | — | `useWsStore` | 心跳活性 |

### 1.3 store 簇(`src/store/`)

| store | state 要点 |
|---|---|
| `ws.ts` | `status`、`lastSyncAt`、**`lastSystemStatusAt`**(状态流遥测) |
| `system.ts` | `status: SystemStatus \| null`、`activeConfigVersion` |
| `risk.ts` | `breaker: RiskEngineStateView \| null`、`positions: PositionView[]` |
| `market.ts` | `books: Record<MarketId, MarketBookView>`、`resolved: Set<MarketId>` |
| `opportunity.ts` | `feed: OpportunityView[]`(cap 200) |
| `trade.ts` | `recent: TradeView[]`(cap 50) |
| `pnl.ts` | `live: {daily,total} \| null`、`intradaySeries: [ts, UsdString][]` |

规则:store 只存 wire 类型(`@vben/types`),格式化在组件层;REST 首屏数据与 WS 增量共用同一 store(页面 `onMounted` 拉 REST 填充,WS 持续更新)。

## 2. 顶栏组件(`shared/components/header/`)

接线:`layouts/basic.vue` 经 `BasicLayout` 插槽 `#header-right-{N}`(N 控制排序,小者靠左):

```vue
<template #header-right-10><SystemStatusIndicator /></template>
<template #header-right-20><ExecutionModeSwitcher /></template>
<template #header-right-30><WsStatusBadge /></template>
```

### 2.1 `SystemStatusIndicator`

- **显示**:状态点 + 简短文案。聚合规则:`breaker_state` 正常且未 halted → 绿「Running」;breaker 非 Closed/Recovered 或有 WARN → 黄;halted / breaker Halted → 红。数据源 `useSystemStore().status`(WS 实时)。
- **Popover**(点击):`SystemStatus` 全字段卡(mode/breaker/uptime/active_markets/open_positions/exposure/daily_pnl/catalog)+ 两个治理按钮:
  - **Halt**(`v-access:code="'system:halt'"`):halt 非 governed(无 acting-role),但需 reason(1–1024 字符)→ 专用 HaltReasonModal(与治理弹窗同风格,非浏览器 prompt)收集后 `POST /system/halt {reason}`;
  - **Resume**(`system:resume`):ResumeAckModal 收集**字符串** `operator_ack`(1–256 字符,后端 `ResumeRequest.operator_ack: String`,非 boolean)后 `POST /system/resume {operator_ack}`。
- 权限:无 `system:read` 时整个组件隐藏(WS 也不会推 system.status)。

### 2.2 `ExecutionModeSwitcher`

- **显示**:当前模式 Tag(`ExecutionModeTag`:dry_run 灰 / paper 蓝 / live 红),来源 `useSystemStore().status.execution_mode`。
- **交互**:`v-access:code="'system:switch_mode'"` 才可点击;下拉选目标模式 → `useGovernedAction`:
  - API:`POST /api/system/mode`,body `{mode, reason}` + `X-Acting-Role`;
  - 切到 `live` 时 `danger: true`(需输入确认词 `live`);
  - 成功后等待 WS `system.status` 回显(不乐观更新),resp `ModeTransitionReport{from,to}` 用于 toast。
- 无权限用户仅见只读 Tag。

### 2.3 `WsStatusBadge`

- 三态:绿(connected)/ 黄旋转(reconnecting)/ 红(disconnected),数据 `useWsStore().status`;tooltip 显示 `lastSyncAt`;点击手动 `connect()`。

## 3. Overview 首页(`views/dashboard/`)

### 3.1 概要

| 项 | 值 |
|---|---|
| 路由 | `/dashboard`(`defaultHomePath`) |
| 菜单权限 | 无权限码(登录即见);区块按权限码降级渲染(无权限区块隐藏并自动重排) |
| 按钮码 | `system:halt` / `system:resume` / `system:switch_mode`(seed v2 挂本页) |
| grid/drawer 选型 | 无 vxe-grid(纯仪表盘);最近交易用轻量 `Table`?**否**——用 `useVbenVxeGrid`(无 formOptions、无分页、`maxHeight` 固定)保持范式统一 |
| i18n 前缀 | `page.dashboard.*` |
| types | `SystemStatus / RiskEngineStateView / LivePnlView / DailyPnlSeries / TradeView / OpportunityView` |

### 3.2 布局(四行网格,≥1280px 基准)

```text
┌─────────────────────────────────────────────────────────────────┐
│ Row1 KPI StatCard ×4: 日盈亏 | 总盈亏 | 资金占用 | 持仓数        │
├──────────────────────────────────┬──────────────────────────────┤
│ Row2L PnL 曲线(7d 日线+今日实时) │ Row2R 系统状态卡 + 熔断器卡   │
├──────────────────────────────────┼──────────────────────────────┤
│ Row3L 运行中任务卡               │ Row3R 实时机会 Feed(迷你)     │
├──────────────────────────────────┴──────────────────────────────┤
│ Row4 最近交易表(10 条,WS 实时头插) + 快捷入口卡               │
└─────────────────────────────────────────────────────────────────┘
```

### 3.3 区块规格

| 区块 | 内容与交互 | 数据源(REST 首屏 + WS 增量) | 权限 |
|---|---|---|---|
| KPI 卡(`StatCard`×4) | 日盈亏(`CellUsd` 着色规则同款格式化)、总盈亏、资金占用(`total_exposure` + `pending_reservations`,tooltip 标注「敞口口径,非钱包余额」)、持仓数(`open_positions / active_markets`) | `GET /pnl/live` + `GET /system/status`;WS `pnl.update` `system.status` | `pnl:read` / `system:read` |
| PnL 曲线(`EchartsCard` + line) | 近 7 日累计曲线 + 今日实时段(`intradaySeries` 追加);hover tooltip;点击跳 `/analytics` | `GET /pnl/daily-series?days=7`(升序 + 窗口内累计,空窗口返回 `points: []`);WS `pnl.update` | `pnl:read` |
| 系统状态卡 | mode(`ExecutionModeTag`)、uptime、active_markets、checked_at;Halt/Resume 按钮(同 §2.1 Popover 逻辑,复用 `shared` handler) | `GET /system/status`;WS `system.status` | `system:read`(按钮另查 halt/resume 码) |
| 熔断器卡(`BreakerBadge`) | breaker FSM 状态大徽标 + reason;非 Closed 时红底;「去风控页」链接 | `GET /risk/circuit-breaker`;WS `risk.circuit_breaker` | `risk:read` |
| 运行中任务卡 | 活跃市场订阅数(`active_markets`)、in-flight 持仓(`open_positions`)、breaker 状态摘要、最近 replay 入队(本地 store 记录的 run_id 列表 + `usePolling` 状态);每项可跳对应页 | `system/status` + 本地 replay 记录 | `system:read` |
| 实时机会 Feed | 最近 20 条 `opportunity.detected`(时间/市场/edge bps/预估利润),新条目顶部插入高亮 2s;点击跳 `/opportunities` | WS `opportunity.detected` + `sync.recent_opportunities` | `opportunity:read` |
| 最近交易表 | `useVbenVxeGrid` 静态 10 行:时间(`CellDateTime`)、市场(`CellMarketId`)、方向、数量、价格(`CellPrice`)、PnL(`CellUsd`)、结果(`CellTag`);WS 头插 | `GET /trades?page=1&size=10`;WS `trade.filled/settled` | `trade:read` |
| 快捷入口卡 | 图标链接:Markets / Risk / Runtime Config / Operation Log / Users(各自带权限码过滤) | 静态 | 各目标页码 |

### 3.4 文件清单

```text
views/dashboard/
├── index.vue                      # 网格编排 + 区块权限降级(hasAccessByCodes)
└── modules/
    └── widgets/
        ├── kpi-cards.vue
        ├── pnl-curve.vue          # EchartsCard 包装
        ├── system-status-card.vue
        ├── breaker-card.vue
        ├── running-tasks-card.vue
        ├── opportunity-feed.vue
        ├── recent-trades.vue      # useVbenVxeGrid(无分页)
        └── quick-links.vue
shared/components/
├── stat-card.vue                  # KPI 通用卡(标题/值/增减色/loading/空态)
├── echarts-card.vue               # 图表通用壳(标题/loading/empty/resize)
├── breaker-badge.vue
├── execution-mode-tag.vue
└── header/{system-status-indicator,execution-mode-switcher,ws-status-badge}.vue
```

> 本页无 schemas(无搜索表单/复杂列定义;`recent-trades` 列定义内联于 widget,因其是固定迷你视图,不参与复用)。

## 4. REST API 引用汇总(本 phase 新增 api 模块)

| 模块 | 函数 | 端点 |
|---|---|---|
| `api/system.ts` | `getSystemStatus / getSystemHealth / haltSystem / resumeSystem / switchExecutionMode` | `GET /system/status`、`GET /system/health`、`POST /system/halt`、`POST /system/resume`、`POST /system/mode` |
| `api/pnl.ts` | `getLivePnl / getWeeklyPnl / getDailyPnlSeries` | `GET /pnl/live`、`/pnl/weekly`、`/pnl/daily-series?days=`(默认 7,上限 90) |
| `api/trades.ts`(部分) | `fetchTradePage` | `GET /trades` |
| `api/risk.ts`(部分) | `getCircuitBreaker` | `GET /risk/circuit-breaker` |

## 5. 验收清单

- [ ] 登录后 WS 自动连接,`sync` 填充各 store;断网 ≤5s 重连并重新 `sync`;徽标三态正确
- [ ] 顶栏状态灯随 WS `system.status` / `risk.circuit_breaker` 实时变色;Popover halt/resume 闭环(带 reason / operator_ack)
- [ ] 执行模式切换:viewer 只读;operator 经治理弹窗切换,`live` 需确认词;成功后顶栏经 WS 回显新模式
- [ ] Overview 各区块按权限降级:`viewer` 全可见(全读权限);裁剪角色对应区块隐藏
- [ ] PnL 曲线随 `pnl.update` 实时延伸;最近交易随 `trade.filled` 头插
- [ ] 区块跳转链接全部可达
- [ ] zh/en 文案完整;dark 模式下图表配色正常(ECharts 主题跟随)
