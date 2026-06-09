# Phase 7 — UI 层

> **产出**: `oxide-arb-ui`（基于 vue-vben-admin fork）
>
> **前置条件**: Phase 6 Web 服务层 REST + WebSocket 端点已就绪
>
> **验收标准**: 所有 Dashboard 页面功能完整；WebSocket 实时推送正常工作（断线自动重连）；所有 CLI/配置操作可通过 UI 完成；构建产物可嵌入 Rust 二进制作为静态文件服务

---

## 0. API 契约（与 Phase 6 对齐）

Phase 7 **必须**遵循 Phase 6 已落地的 HTTP/WS 契约，UI 不得假设旧版 `/api/v1/...` 路径或单用户 API key。

### 0.1 版本化

- **路径**：`/api/users`、`/api/trades` 等 — **无** URL 内嵌版本号。
- **版本头**：每个 `/api/*` 请求携带 `Accept-Api-Version: v1`（axios/fetch 拦截器统一注入）。兼容 fallback：`X-API-Version: v1`。
- **探针**：`GET /health`、`GET /ready`、`GET /metrics` 无版本头（K8s / Prometheus 直连）。

### 0.2 认证

- **登录**：`POST /api/auth/login` → `{ access_token, refresh_token, expires_in }`。
- **刷新**：`POST /api/auth/refresh`（旋转 refresh，旧 refresh 入黑名单）。
- **登出**：`POST /api/auth/logout` + Bearer access + body `{ refresh_token }`。
- **业务请求**：`Authorization: Bearer <access_token>`（非 API key）。
- **治理变更**：除 Bearer 外，带 `X-Acting-Role: <role_code>` + `X-Request-Id` + body `reason`。

### 0.3 配置变更（无裸 PATCH）

- **禁止** `PATCH /api/config` 或任意 in-place 热更。
- 流程：`POST /api/runtime-config/versions` → `POST .../activate` 或 `.../rollback`；读当前：`GET /api/runtime-config`。
- UI Config 页编辑的是**新版本草稿**，提交走 create + activate，审计见 operation log + 治理链。

### 0.4 WebSocket

- **URL**：`ws(s)://<host>/api/ws?token=<access_token>`（浏览器无法在 handshake 设 Bearer，故 query token）。
- **客户端指令**：`subscribe` / `unsubscribe` / `sync` / `ping`。
- **已删除事件**（Phase 6.7）：`trade.opened`（FOK 无驻留挂单）、`opportunity.expired`（无领域过期源）。UI 不得订阅或渲染这两者。
- **配置推送**：`config.activated`（非 `config.changed`）。

### 0.5 Materialization / Replay

- Operator 触发 backfill：`POST /api/replay`（`Replay:Create`），非独立 materialization 路径。
- 状态：`GET /api/replay/{run_id}`、`GET /api/replay/{run_id}/history`。

---

## 1. 工作范围（原 §0）

1. Fork vue-vben-admin 并裁剪为 oxide-arb 专用 shell
2. 构建 7 个核心 Dashboard 页面
3. 实现 WebSocket 实时数据层
4. 所有系统配置、风控操作可通过 UI 完成
5. 构建产物集成到 Rust monolith 部署

---

## 2. Fork 策略与定制方案

### 1.1 Fork 来源

```bash
git clone https://github.com/vbenjs/vue-vben-admin.git oxide-arb-ui
cd oxide-arb-ui
git remote rename origin upstream
git remote add origin <your-repo>
```

### 1.2 技术栈

| 技术 | 版本 | 用途 |
|---|---|---|
| Vue 3 | ^3.5 | UI 框架 |
| TypeScript | ^5.6 | 类型安全 |
| Vite | ^6 | 构建工具 |
| Ant Design Vue | ^4 | 组件库（vben-admin 默认） |
| Pinia | ^2 | 状态管理 |
| Vue Router | ^4 | 路由 |
| ECharts | ^5 | 图表（PnL 曲线、edge 分布、heatmap） |
| VueUse | ^12 | 工具函数（useWebSocket, useDark） |

### 1.3 定制范围

保留 vben-admin 的：
- Layout shell（sidebar + header + content）
- 主题系统（dark/light）
- 权限路由框架
- 国际化基础设施
- 表格/表单组件封装

移除/替换：
- 示例页面（全部删除）
- Mock 数据（替换为真实 API）
- 多角色权限（简化为 **JWT + 动态 RBAC**，非单 API key）
- 不需要的依赖（地图、富文本编辑器等）

新增：
- WebSocket 实时数据层
- oxide-arb 专用组件（orderbook heatmap, circuit breaker indicator 等）
- 7 个核心 Dashboard 页面

---

## 2. 页面布局与导航结构

### 2.1 侧边栏导航

```
📊 Overview          /dashboard
📈 Markets           /markets
⚡ Opportunities     /opportunities
💰 Trades            /trades
🛡️ Risk              /risk
⚙️ Config            /config
📉 Analytics         /analytics
```

### 2.2 顶部栏

```
[Logo: oxide-arb]  [系统状态指示灯]  [执行模式: Live|Paper|DryRun]  [Dark/Light 切换]  [连接状态]
```

### 2.3 响应式策略

- 主要面向桌面端（≥1280px）
- 平板端（≥768px）：sidebar 折叠为 icon-only
- 移动端（<768px）：sidebar 转为 drawer，表格简化列

---

## 3. Dashboard 页面设计

### 3.1 Overview（概览页）

**路由**: `/dashboard`

**布局**: 四行网格

```
┌──────────────────────────────────────────────────────┐
│ Row 1: KPI Cards (4 columns)                          │
│ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │
│ │ Daily PnL │ │ Total PnL│ │ Win Rate │ │ Open Pos │ │
│ │ +$127.50  │ │ +$3,450  │ │ 68.5%    │ │ 3/10     │ │
│ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │
├──────────────────────────────────────────────────────┤
│ Row 2: PnL Curve (ECharts line chart, last 7 days)   │
│ [实时更新 via WS: pnl.update]                         │
├──────────────────────────────────────────────────────┤
│ Row 3: System Status + Circuit Breaker               │
│ ┌─────────────────────┐ ┌─────────────────────────┐  │
│ │ System Status       │ │ Circuit Breaker          │  │
│ │ Mode: Live          │ │ ●L1: OK  ●L2: OK        │  │
│ │ Uptime: 12h 34m     │ │ ●L3: OK  ●L4: OK        │  │
│ │ Markets: 45 active  │ │ Daily loss: $23 / $500   │  │
│ └─────────────────────┘ └─────────────────────────┘  │
├──────────────────────────────────────────────────────┤
│ Row 4: Recent Trades (last 10, auto-scroll)          │
│ [实时更新 via WS: trade.filled, trade.settled]        │
└──────────────────────────────────────────────────────┘
```

**数据源**:
- KPI Cards: `GET /api/trades/pnl` + `GET /api/risk/positions` + WS `pnl.update`
- PnL Curve: `GET /api/trades/pnl/daily` + WS `pnl.update` 追加最新点
- System Status: `GET /api/system/status` + WS `system.status`
- Circuit Breaker: `GET /api/risk/circuit-breaker` + WS `risk.circuit_breaker`
- Recent Trades: `GET /api/trades?limit=10` + WS `trade.filled` / `trade.settled`

### 3.2 Markets（市场页）

**路由**: `/markets`

**布局**: 上筛选 + 下表格 + 右侧抽屉

```
┌──────────────────────────────────────────────────────┐
│ Filter Bar                                            │
│ [Status ▼] [Category ▼] [Search: condition_id/name]  │
├──────────────────────────────────────────────────────┤
│ Market Table (sortable, paginated)                    │
│ ┌─────┬──────────┬──────┬──────┬──────┬──────┬─────┐ │
│ │ Sub │ Name     │ Bid  │ Ask  │ Vol  │Depth │ Act │ │
│ ├─────┼──────────┼──────┼──────┼──────┼──────┼─────┤ │
│ │ ☑   │ Trump... │ 0.65 │ 0.67 │ $12K │ $5K  │ ... │ │
│ │ ☐   │ ETH>... │ 0.42 │ 0.44 │ $8K  │ $3K  │ ... │ │
│ └─────┴──────────┴──────┴──────┴──────┴──────┴─────┘ │
├──────────────────────────────────────────────────────┤
│ Orderbook Depth Heatmap (for selected market)        │
│ [ECharts heatmap: price × time, color = depth]       │
└──────────────────────────────────────────────────────┘
```

**交互**:
- 复选框 Subscribe/Unsubscribe → `POST /api/markets/{id}/subscribe`
- 点击行展开 → 右侧抽屉显示详情（`GET /api/markets/{id}`）
- Orderbook heatmap 实时更新 via WS `market.book_update`

### 3.3 Opportunities（机会页）

**路由**: `/opportunities`

**布局**: 双面板 — 上实时 Feed + 下统计

```
┌──────────────────────────────────────────────────────┐
│ Live Opportunity Feed (auto-scroll, newest first)     │
│ [实时: WS opportunity.detected]                       │
│ ┌────────────────────────────────────────────────────┐│
│ │ 10:30:15 | Trump Win | edge 450bps | $2.50 profit ││
│ │ 10:29:03 | ETH>5K    | edge 320bps | $1.80 profit ││
│ │ 10:28:41 | Fed Rate  | edge 280bps | $0.90 profit ││
│ └────────────────────────────────────────────────────┘│
├──────────────────────────────────────────────────────┤
│ Statistics Panel (tabs: 24h | 7d | 30d)              │
│ ┌─────────────────────┐ ┌─────────────────────────┐  │
│ │ Detection Count     │ │ Edge Distribution        │  │
│ │ Line chart (hourly) │ │ Histogram (bps buckets)  │  │
│ └─────────────────────┘ └─────────────────────────┘  │
│ ┌─────────────────────┐ ┌─────────────────────────┐  │
│ │ Top Markets (by opp)│ │ Hit Rate vs Edge         │  │
│ │ Bar chart           │ │ Scatter plot             │  │
│ └─────────────────────┘ └─────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

**数据源**:
- Live Feed: WS `opportunity.detected` only（无 `opportunity.expired`）
- Statistics: `GET /api/opportunities/stats?period=24h`
- Edge Distribution: `GET /api/analytics/edge-distribution`

### 3.4 Trades（交易页）

**路由**: `/trades`

**布局**: 筛选 + 表格 + 详情 Drawer

```
┌──────────────────────────────────────────────────────┐
│ Filter Bar                                            │
│ [Date Range] [Outcome ▼] [Market ▼] [Min PnL] [Sort]│
├──────────────────────────────────────────────────────┤
│ Trade Table (paginated)                               │
│ ┌──────┬────────┬──────┬──────┬──────┬───────┬─────┐ │
│ │ Time │ Market │ Side │ Size │ PnL  │Outcome│ Det │ │
│ ├──────┼────────┼──────┼──────┼──────┼───────┼─────┤ │
│ │ 10:30│ Trump  │ Buy  │ $50  │+$2.5 │Success│ 👁  │ │
│ │ 10:15│ ETH    │ Buy  │ $30  │-$1.2 │Miss   │ 👁  │ │
│ └──────┴────────┴──────┴──────┴──────┴───────┴─────┘ │
└──────────────────────────────────────────────────────┘

Trade Detail Drawer (click 👁):
┌──────────────────────────────────────────────────────┐
│ Decision Chain                                        │
│ 1. Opportunity detected (edge 450bps, confidence 85%)│
│ 2. Risk check passed (all 4 levels OK)               │
│ 3. Position sized ($50, quarter-Kelly)               │
│ 4. Order submitted (FOK, price 0.65)                 │
│ 5. Fill confirmed (price 0.648, shares 76.92)        │
│ 6. Settlement: YES → PnL +$2.50                     │
├──────────────────────────────────────────────────────┤
│ PnL Attribution                                       │
│ Gross: $27.02 | Fees: $0.02 | Edge: +$2.50 net      │
└──────────────────────────────────────────────────────┘
```

**数据源**:
- Trade List: `GET /api/trades?...`
- Trade Detail: `GET /api/trades/{id}`
- Decision Chain: `GET /api/trades/{id}/decisions`
- 实时新增: WS `trade.filled` / `trade.settled`（无 `trade.opened`）

### 3.5 Risk（风控页）

**路由**: `/risk`

**布局**: 三区域网格

```
┌──────────────────────────────────────────────────────┐
│ Circuit Breaker Panel                                 │
│ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐         │
│ │ L1     │ │ L2     │ │ L3     │ │ L4     │         │
│ │ ● OK   │ │ ● OK   │ │ ⚠ WARN │ │ ● OK   │         │
│ │ Per-mkt│ │ Daily  │ │ System │ │ Emerg  │         │
│ └────────┘ └────────┘ └────────┘ └────────┘         │
│ [Reset L3 Button]                                    │
├────────────────────────┬─────────────────────────────┤
│ Daily Loss Gauge       │ Position Exposure            │
│ ┌────────────────────┐ │ ┌───────────────────────┐   │
│ │ Gauge: $23 / $500  │ │ │ Market   │ Size │ PnL │   │
│ │ ████░░░░░░░░ 4.6%  │ │ │ Trump    │ $50  │+$2  │   │
│ │                    │ │ │ ETH      │ $30  │-$1  │   │
│ └────────────────────┘ │ │ Total    │ $80  │+$1  │   │
│                        │ └───────────────────────┘   │
├────────────────────────┴─────────────────────────────┤
│ Blacklist Management                                  │
│ ┌────────────────────────────────────────────────────┐│
│ │ Scope   │ Target        │ Reason     │ Added  │ ✕ ││
│ │ Market  │ 0x1234...     │ Low liq    │ Jan 15 │ ✕ ││
│ │ Event   │ 0x5678...     │ Manual     │ Jan 14 │ ✕ ││
│ └────────────────────────────────────────────────────┘│
│ [+ Add to Blacklist] modal: scope + target + reason  │
└──────────────────────────────────────────────────────┘
```

**交互**:
- Reset Circuit Breaker → `POST /api/risk/circuit-breaker/reset`
- Add Blacklist → `POST /api/risk/blacklist`
- Remove Blacklist → `POST /api/risk/blacklist/{id}/remove`（`X-Acting-Role` + body `reason`）
- 实时: WS `risk.circuit_breaker`, `risk.position_update`

### 3.6 Config（配置页）

**路由**: `/config`

**布局**: 分组表单 + 审计日志

```
┌──────────────────────────────────────────────────────┐
│ Configuration Groups (Tabs / Accordion)               │
│ ┌────────────────────────────────────────────────────┐│
│ │ [Detection] [Execution] [Risk] [Sizing] [Notify]  ││
│ ├────────────────────────────────────────────────────┤│
│ │ Detection Config                                   ││
│ │ ┌────────────────────┬──────────┬────────────────┐ ││
│ │ │ Field              │ Value    │ Constraint     │ ││
│ │ ├────────────────────┼──────────┼────────────────┤ ││
│ │ │ min_edge_bps       │ [200   ] │ 100–2000       │ ││
│ │ │ min_depth_usd      │ [200   ] │ 50–10000       │ ││
│ │ │ max_depth_pct      │ [30    ] │ 5–80           │ ││
│ │ │ confidence_floor   │ [0.70  ] │ 0.0–1.0        │ ││
│ │ └────────────────────┴──────────┴────────────────┘ ││
│ │ [Save Changes]  [Reset to Defaults]                ││
│ └────────────────────────────────────────────────────┘│
├──────────────────────────────────────────────────────┤
│ Audit Log (recent config changes)                     │
│ ┌────────────────────────────────────────────────────┐│
│ │ Time       │ Path                │ Old  → New     ││
│ │ 10:30:15   │ risk.l1_loss_usd    │ $50  → $100   ││
│ │ Yesterday  │ sizing.max_pos_usd  │ $100 → $200   ││
│ └────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────┘
```

**交互**:
- 创建新版本 + Activate → `POST /api/runtime-config/versions` + `POST .../activate`（治理 acting-role + reason）
- 版本历史 / 回滚 → `GET /api/runtime-config/versions`、`POST .../rollback`
- 审计：operation log + `GET /api/control-factors/audit`（治理链）

### 3.7 Analytics（分析页）

**路由**: `/analytics`

**布局**: 多图表仪表盘

```
┌──────────────────────────────────────────────────────┐
│ Date Range Selector: [Last 7d ▼] [Custom range]      │
├──────────────────────────────────────────────────────┤
│ Row 1: Cumulative PnL Trend + Daily PnL Bars         │
│ ┌─────────────────────┐ ┌─────────────────────────┐  │
│ │ Cumulative PnL      │ │ Daily PnL Bars           │  │
│ │ Line chart          │ │ Bar chart (green/red)    │  │
│ └─────────────────────┘ └─────────────────────────┘  │
├──────────────────────────────────────────────────────┤
│ Row 2: Edge Distribution + Market Performance        │
│ ┌─────────────────────┐ ┌─────────────────────────┐  │
│ │ Edge Distribution   │ │ Market Performance Top10 │  │
│ │ Histogram           │ │ Horizontal bar chart     │  │
│ └─────────────────────┘ └─────────────────────────┘  │
├──────────────────────────────────────────────────────┤
│ Row 3: Replay Results + Win Rate Trend               │
│ ┌─────────────────────┐ ┌─────────────────────────┐  │
│ │ Historical Replay   │ │ Win Rate Trend           │  │
│ │ Summary Table       │ │ Line chart (7d rolling)  │  │
│ │ (from /replay/hist) │ │                          │  │
│ └─────────────────────┘ └─────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

**数据源**:
- PnL Trend: `GET /api/trades/pnl/daily`
- Edge Distribution: `GET /api/analytics/edge-distribution`
- Market Performance: `GET /api/analytics/market-performance`
- Replay Results: `GET /api/replay` history endpoints

---

## 4. WebSocket 集成

### 4.1 连接管理 Composable

```typescript
// src/composables/useOxideWs.ts

import { useWebSocket } from '@vueuse/core'

export function useOxideWs() {
  const auth = useAuthStore()
  const baseUrl = import.meta.env.VITE_WS_URL || `ws://${window.location.host}`

  const { status, data, send, open, close } = useWebSocket(
    () => `${baseUrl}/api/ws?token=${encodeURIComponent(auth.accessToken)}`,
    {
      autoReconnect: {
        retries: Infinity,
        delay: 1000,
        maxDelay: 30000,
      },
      heartbeat: {
        message: JSON.stringify({ action: 'ping' }),
        interval: 15000,
        pongTimeout: 30000,
      },
      onConnected() {
        send(JSON.stringify({ action: 'sync' }))
      },
      onMessage(_ws, event) {
        const msg = JSON.parse(event.data) as WsMessage
        dispatchToStores(msg)
      },
    }
  )

  return { status, data, send, open, close }
}
```

```typescript
// src/api/client.ts — 所有 REST 请求统一注入版本头 + Bearer
axios.interceptors.request.use((config) => {
  config.headers['Accept-Api-Version'] = 'v1'
  const token = useAuthStore().accessToken
  if (token) config.headers.Authorization = `Bearer ${token}`
  return config
})
```

### 4.2 Store 分发

```typescript
function dispatchToStores(msg: WsMessage) {
  switch (msg.type) {
    case 'opportunity.detected':
      useOpportunityStore().addOpportunity(msg.data)
      break
    case 'trade.filled':
    case 'trade.settled':
      useTradeStore().updateTrade(msg.data)
      break
    case 'pnl.update':
      usePnlStore().updatePnl(msg.data)
      break
    case 'system.status':
      useSystemStore().updateStatus(msg.data)
      break
    case 'risk.circuit_breaker':
      useRiskStore().updateCircuitBreaker(msg.data)
      break
    case 'market.book_update':
      useMarketStore().updateBook(msg.data)
      break
    case 'config.activated':
      useConfigStore().applyActivation(msg.data)
      break
    case 'system.alert':
      useNotification().warning({ message: msg.data.message })
      break
  }
}
```

### 4.3 状态同步策略

| 场景 | 策略 |
|---|---|
| 首次连接 | 服务端推送 `system.status` → 客户端发送 `sync` → 服务端回传全量状态 |
| 断线重连 | 同首次连接 |
| 页面切换 | 不断开 WS，仅切换 store 的 active view |
| 长时间离开 | WS 保持连接，回来时数据自动是最新的 |

---

## 5. 关键 UI 组件

### 5.1 自定义组件清单

| 组件 | 说明 | 用途 |
|---|---|---|
| `OrderbookHeatmap` | ECharts heatmap 展示 orderbook depth 随时间变化 | Markets 页 |
| `CircuitBreakerIndicator` | 四格状态灯（L1–L4），支持实时颜色变化 | Overview + Risk 页 |
| `PnlCurve` | ECharts line chart，支持实时追加数据点 | Overview + Analytics 页 |
| `EdgeDistributionChart` | ECharts histogram，按 bps 区间统计检测次数 | Opportunities + Analytics 页 |
| `DailyLossGauge` | 环形进度条，显示当日亏损 vs 限额 | Risk 页 |
| `DecisionChainTimeline` | 垂直时间线组件，展示单笔交易的决策全过程 | Trades 详情 Drawer |
| `LiveFeed` | 实时滚动列表，新条目顶部插入并高亮 | Opportunities 页 |
| `ConfigEditor` | 分组表单，字段类型自动匹配 input 组件 | Config 页 |
| `ConnectionStatusBadge` | WS 连接状态指示器（绿/黄/红） | Header |
| `ExecutionModeSwitcher` | 下拉切换 Live/Paper/DryRun，带确认弹窗 | Header |

### 5.2 通用组件

基于 vben-admin 已有组件进行适配：

- **BasicTable**: 分页表格（复用 vben `BasicTable`，添加自定义列渲染）
- **BasicForm**: 配置表单（复用 vben `BasicForm`，添加 Decimal / Bps 类型支持）
- **PageWrapper**: 页面容器（标题 + breadcrumb + loading state）
- **DrawerDetail**: 右侧抽屉（复用 vben `BasicDrawer`）

---

## 6. 部署

### 6.1 构建流程

```bash
cd oxide-arb-ui
pnpm install
pnpm build
# 产物在 dist/
```

### 6.2 集成到 Rust 二进制

```bash
# 构建 UI 并复制到 Rust 项目
cp -r oxide-arb-ui/dist/ oxide-arb/static/ui/

# Rust 二进制在启动时检测 static/ui/ 是否存在
# 存在 → 注册 actix-files 静态文件路由
# 不存在 → 仅 API 模式
```

### 6.3 开发模式

```bash
# Terminal 1: Rust API server (port 8080)
cargo run -- serve

# Terminal 2: Vue dev server (port 5173, proxy API to 8080)
cd oxide-arb-ui
pnpm dev
```

Vite proxy 配置：

```typescript
// vite.config.ts
export default defineConfig({
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
      // WS 与 API 同 host；开发时也可直连 /api/ws
    },
  },
})
```

### 6.4 环境变量

```bash
VITE_API_BASE_URL=http://localhost:8080   # 开发模式 API 地址（可选，默认同源）
VITE_WS_URL=ws://localhost:8080           # 开发模式 WS 地址（可选）
# 认证走 login 流程，不使用静态 API key
```

生产模式下这些变量不需要设置（API 和 UI 同源）。

---

## 7. 验收检查清单

- [ ] Fork vue-vben-admin 成功，`pnpm dev` 可启动开发服务器
- [ ] 移除所有示例页面，仅保留 oxide-arb 7 个 Dashboard 页面
- [ ] **Overview 页**: 4 个 KPI card 正确显示数据；PnL 曲线实时更新；系统状态 + 熔断器指示灯工作
- [ ] **Markets 页**: 市场表格支持筛选/排序/分页；Subscribe/Unsubscribe 按钮正确调用 API；Orderbook heatmap 实时更新
- [ ] **Opportunities 页**: Live feed 实时显示新检测到的机会；统计图表按时间段切换正确
- [ ] **Trades 页**: 交易列表支持多条件筛选；Detail drawer 显示完整决策链；PnL attribution 数字正确
- [ ] **Risk 页**: 熔断器面板实时更新；Reset 按钮工作；黑名单增/删正确；Daily loss gauge 准确
- [ ] **Config 页**: runtime-config 版本 create/activate/rollback；治理 acting-role + reason；审计链可查看
- [ ] **Analytics 页**: 所有图表正确渲染；日期范围选择器工作；Replay 历史记录可查看
- [ ] WebSocket 连接建立后收到初始状态
- [ ] WebSocket 断线后 ≤ 5s 自动重连
- [ ] 重连后 `sync` 正确恢复全量状态
- [ ] Dark / Light 主题切换正常
- [ ] `pnpm build` 产物 ≤ 5MB（gzip 后）
- [ ] 构建产物嵌入 Rust 二进制后，访问 `http://host:port/` 可正常使用
- [ ] 所有 API 请求携带 `Accept-Api-Version: v1` + Bearer token
- [ ] 响应式布局在 1280px / 768px / 375px 三个断点表现正常
