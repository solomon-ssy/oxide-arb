# Phase 7.3 — 业务页:Markets / Opportunities / Trades

> **产出**: 三个 Trading 域页面,全部遵循 [phase7.0](phase7.0-architecture-rules-scaffold.md) 列表页骨架与 Drawer 协议
>
> **前置**: [phase7.2](phase7.2-overview-realtime-header.md)(WS 层 + store)

---

## 1. Markets(`views/markets/`)

### 1.1 概要

| 项 | 值 |
|---|---|
| 路由 / component | `/markets` / `markets/index` |
| 菜单码 | `market:read` |
| 按钮码 | `market:update`(subscribe/unsubscribe) |
| grid/drawer | `useVbenVxeGrid`(搜索+分页)+ 详情 `useVbenDrawer`(connectedComponent,只读) |
| WS | `market.book_update`(per-market,详情抽屉打开时订阅)、`market.resolved` |
| i18n | `page.markets.*` |
| types | `MarketView / MarketBookSummaryView / MarketBookView / BookLevelView`(`packages/types/src/market.ts`,types 平铺无 `oxide/` 子目录) |

### 1.2 布局与交互

```text
┌ 搜索表单(formOptions): 关键词(question/slug) | 状态 | 类目 ┐
├ Grid: 名称 | MarketId(CellMarketId) | Yes Bid/Ask(CellPrice) | 深度(CellUsd) | 状态(CellTag) | 订阅(CellSwitch) | 操作(CellOperation) ┤
└ MarketDetailDrawer(点击行/操作"详情"): 基础信息 + 实时订单簿面板 ┘
```

- **订阅开关**:`CellSwitch` + `beforeChange` 钩子 → `handleRequest(() => subscribeMarket(id))` / `unsubscribeMarket`;无 `market:update` 码时列只读(`useColumns` 内 `hasAccessByCodes` 控制 disabled)。该操作非治理(无 reason)。
- **详情抽屉**(`market-detail-drawer.vue`):打开时 `getMarketById` + `getMarketBook` 首屏,同时 `useOxideWs().subscribeMarket(id)` 实时刷新订单簿;关闭时 unsubscribe。订单簿面板 `orderbook-panel.vue`:Yes/No 双侧 5 档深度条形图(横向 bar,买绿卖红),数据 `useMarketStore().books[id]`。
- **resolved 标记**:`market.resolved` 推送后行内状态 Tag 变更(store 驱动,`gridApi` 局部刷新)。
- **偏差说明**:搜索表单不含「是否已订阅」过滤 —— `subscribed` 是运行时内存态(CLOB WS 订阅集合),无法进入 Postgres 分页查询;仅作列展示 + 开关。

### 1.3 API(`api/markets.ts`)

| 函数 | 端点 | 权限 |
|---|---|---|
| `fetchMarketPage(params: MarketApi.MarketPageParams)` | `GET /markets`(`MarketPageQuery` + page/size) | market:read |
| `getMarketById(id)` | `GET /markets/{market_id}` | market:read |
| `getMarketBook(id)` | `GET /markets/{market_id}/book` | market:read |
| `subscribeMarket(id)` | `POST /markets/{market_id}/subscribe`(无 body) | market:update |
| `unsubscribeMarket(id)` | `POST /markets/{market_id}/unsubscribe` | market:update |

### 1.4 文件清单

```text
views/markets/
├── index.vue
└── modules/
    ├── schemas/{index,search-form,table-columns}.ts
    └── widgets/{market-detail-drawer.vue, orderbook-panel.vue}
```

---

## 2. Opportunities(`views/opportunities/`)

### 2.1 概要

| 项 | 值 |
|---|---|
| 路由 / component | `/opportunities` / `opportunities/index` |
| 菜单码 | `opportunity:read`(无 mutating 按钮) |
| grid/drawer | 上半 LiveFeed(自定义组件)+ 下半 Tabs:历史 `useVbenVxeGrid` / 漏斗统计 `useVbenVxeGrid`;审计明细 `useVbenDrawer`(只读) |
| WS | `opportunity.detected`(全局,store 驱动 Feed) |
| i18n | `page.opportunities.*` |
| types | `OpportunityListView / OpportunityAuditView / OpportunityFunnelView`(`packages/types/src/opportunity.ts`);WS feed 仍是 5 字段 `OpportunityView` |

### 2.2 布局与交互

```text
┌ LiveFeed(opportunity-feed.vue 复用 shared 版,本页满血形态):              ┐
│  实时滚动列表(cap 200,暂停滚动按钮,点击条目 → 审计抽屉)                │
├ Tabs:                                                                      ┤
│  [近 24h] Grid: 时间 | 市场 | edge(CellBps) | 置信度(CellPercent) | 预估利润(CellUsd) | 操作(审计) │
│  [历史]   搜索(时间范围 + market_id) + 同列 Grid(fetchOpportunityHistory)  │
│  [漏斗]   搜索(时间范围) + Grid: 检测→评分→风控→执行 各阶段计数/通过率     │
└ OpportunityAuditDrawer: GET /opportunities/{id} 审计轨迹垂直时间线          ┘
```

- LiveFeed 数据源 `useOpportunityStore().feed`(7.2 已建);「暂停」只是冻结视图,store 持续累积(恢复时显示累积条数)。组件为 `shared/components/opportunity-feed.vue`(自 dashboard **迁移**,`variant: compact | full` 双形态)。
- **审计抽屉**(`shared/components/opportunity-audit-drawer.vue`,Trades 决策链同用):`OpportunityAuditView[]` 渲染为时间线(阶段/结论/相对检测耗时/快照折叠),时间线本体为 `shared/components/audit-timeline.vue`。
- **偏差说明**:漏斗为聚合统计(阶段 × 去重机会数 × 占 detected 比率),聚合行无 market 维度 —— 原「漏斗行点击带 market_id 跳历史 Tab」联动取消。

### 2.3 API(`api/opportunities.ts`)

| 函数 | 端点 | 权限 |
|---|---|---|
| `fetchRecentOpportunities({page,size})` | `GET /opportunities/recent` | opportunity:read |
| `fetchOpportunityHistory({from,to,market_id,page,size})` | `GET /opportunities/history` | opportunity:read |
| `fetchOpportunityStats({from,to,market_id})` | `GET /opportunities/stats` → `OpportunityFunnelView`(聚合,无分页) | opportunity:read |
| `getOpportunityAudit(id)` | `GET /opportunities/{opportunity_id}` → `OpportunityAuditView[]` | opportunity:read |

### 2.4 文件清单

```text
views/opportunities/
├── index.vue
└── modules/
    └── schemas/{index,search-form,table-columns}.ts   # useDetectionColumns(recent/history 共用)/ useFunnelColumns
shared/components/opportunity-feed.vue                 # dashboard 迁移而来,compact/full 双形态
shared/components/opportunity-audit-drawer.vue         # 审计抽屉(本页 + trades 决策审计共用)
shared/components/audit-timeline.vue                   # 时间线通用组件(审计抽屉 + trades 决策链共用)
```

---

## 3. Trades(`views/trades/`)

### 3.1 概要

| 项 | 值 |
|---|---|
| 路由 / component | `/trades` / `trades/index` |
| 菜单码 | `trade:read` |
| 按钮码 | `trade:reconcile` → `trade:update`(标记不可解析,治理 mutation) |
| grid/drawer | 主 Grid `useVbenVxeGrid`;详情(决策链 + PnL 归因)`useVbenDrawer` 只读;风控决策审计 Tab 二级 Grid;**对账队列 Tab 三级 Grid** |
| WS | `trade.filled` / `trade.settled`(store 头插 → 顶部「N 条新交易」提示条,点击刷新,**不**自动打断分页浏览);对账 Tab 在 `trade.filled` 且 `needs_reconcile` 时刷新 |
| i18n | `page.trades.*`(含 `tabs.reconciliation`、`reconcile.*`) |
| types | `TradeView`(含 `needs_reconcile` / `reconcile_resolution` / `error_message`,`packages/types/src/trade.ts`)/ `RiskAuditEventView`(`packages/types/src/risk.ts`,即原 TradeDecisionRow) |

### 3.2 布局与交互

```text
┌ Tabs: [交易列表] [风控决策审计] [对账队列]                                    ┐
│ 交易列表:                                                                  │
│   搜索: 时间范围 | market_id | 方向(side) | 结果(outcome) | 状态 | 模式     │
│   Grid: 时间(CellDateTime) | 市场(CellMarketId) | 方向(CellTag) | 数量      │
│         | 价格(CellPrice) | 金额(CellUsd) | PnL(CellUsd) | 结果(CellTag)    │
│         | 操作(CellOperation: 详情)                                        │
│ 风控决策审计:                                                              │
│   搜索: 时间范围;Grid: 时间 | 市场 | 决策 | 拒绝原因 | 关联机会            │
│ 对账队列:                                                                  │
│   Tab badge = `needs_reconcile_count`(来自 `systemStore.balance`)          │
│   Grid: 时间 | 市场 | 模式 | 状态 | 结果 | 成本 | 错误信息 | 待对账 | 操作   │
│   操作: 详情(TradeDetailDrawer) | 标记不可解析(useGovernedAction)           │
└ TradeDetailDrawer:                                                         ┘
   ① 概要(全字段) ② 决策链时间线(audit-timeline:检测→风控→定容→下单→成交→结算)
   ③ PnL 归因(毛利/费用/净 edge,CellUsd 格式化)
```

- 决策链数据:`getTradeById` 概要 + 关联 `getOpportunityAudit`(经 trade 的 `opportunity_id`)拼装;PnL 归因 = `cost_usd / fee_usd / detected_profit_usd / net_profit_usd`。
- 决策审计行的 `market_id` / `rejection_reason` 为持久化时提升的可查询列(`risk_audit_event` 表),历史行(若有)显示空。
- WS 新交易提示条:`useTradeStore().recent` 与当前页首条对比计数;点击 `gridApi.query()`。
- **对账 Tab 深链**:`?tab=reconciliation`(IntegrityBanner / 顶栏状态灯 Popover 跳转);仅 `trade:update` 角色可见「标记不可解析」;`useGovernedAction` + `confirmWord: unresolvable` + `POST /trades/{trade_id}/reconcile`(resolution 固定 `unresolvable`,note = acting-role reason)。

### 3.3 API(`api/trades.ts`)

| 函数 | 端点 | 权限 |
|---|---|---|
| `fetchTradePage(params: TradeApi.TradePageParams)` | `GET /trades` | trade:read |
| `getTradeById(id)` | `GET /trades/{trade_id}` | trade:read |
| `fetchTradeDecisions({from,to,page,size})` | `GET /trades/decisions` | trade:read |
| `fetchReconciliationQueue(params: PageRequest)` | `GET /trades/reconciliation` | trade:read |
| `reconcileTrade(id, body, ctx)` | `POST /trades/{trade_id}/reconcile` | trade:update + `X-Acting-Role` |

### 3.4 文件清单

```text
views/trades/
├── index.vue
└── modules/
    ├── schemas/{index,search-form,table-columns,reconciliation-columns}.ts
    └── widgets/{trade-detail-drawer.vue, pnl-attribution.vue}
```

**跨页联动**: `IntegrityBanner` / 顶栏 `SystemStatusIndicator` Popover 中的 blocking / needs-reconcile 计数 → `/trades?tab=reconciliation`。

---

## 4. 验收清单

- [ ] 三页搜索/分页/排序走 `proxyConfig.ajax.query`,响应字段对齐 `Paginated{items,total}`
- [ ] Markets 订阅开关闭环(开关 → API → 行内状态),无 `market:update` 角色为只读
- [ ] Markets 详情抽屉打开期间订单簿经 WS 实时刷新,关闭后取消订阅(无泄漏,重复开关 5 次验证)
- [ ] Opportunities LiveFeed 实时插入 + 暂停;审计抽屉时间线完整
- [ ] Trades 决策链 + PnL 归因渲染正确;`trade.settled` 后已打开的详情数据刷新
- [ ] Trades 对账 Tab: badge 与 `needs_reconcile_count` 一致;governed「标记不可解析」仅 `trade:update`;深链 `?tab=reconciliation` 可用
- [ ] 三页所有金额/价格/bps 列经 cell 渲染器格式化,无裸 number
- [ ] zh/en 文案完整;空态/loading/错误态(EchartsCard/Grid 空提示)覆盖
