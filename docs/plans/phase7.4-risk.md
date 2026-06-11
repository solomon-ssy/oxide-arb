# Phase 7.4 — Risk 风控页与 Blacklist 黑名单页

> **产出**: `/risk`(熔断器 + 日亏损 + 持仓敞口)与 `/blacklist`(黑名单管理)两个页面,治理动作全部经 `useGovernedAction`
>
> **前置**: [phase7.2](phase7.2-overview-realtime-header.md)

---

## 1. Risk Overview(`views/risk/`)

### 1.1 概要

| 项 | 值 |
|---|---|
| 路由 / component | `/risk` / `risk/index` |
| 菜单码 | `risk:read` |
| 按钮码 | `risk:reset`(熔断器重置,**governed**) |
| grid/drawer | 上部仪表卡(无 grid)+ 下部持仓 `useVbenVxeGrid`(无搜索表单,刷新按钮) |
| WS | `risk.circuit_breaker`、`risk.position_update`(7.2 store 已接) |
| i18n | `page.risk.*`;枚举 `enum.breakerState.*` |
| types | `RiskEngineStateView / PositionView`(`oxide/risk.ts`) |

### 1.2 布局与交互

```text
┌──────────────────────────────┬──────────────────────────────┐
│ 熔断器面板                    │ 日亏损 Gauge                  │
│  BreakerBadge(大) + FSM 状态  │  ECharts gauge:               │
│  + reason + 触发时间          │  daily-loss / 配置限额        │
│  [Reset 按钮 risk:reset]      │  阈值分段着色(绿/黄/红)        │
├──────────────────────────────┴──────────────────────────────┤
│ 敞口概览 StatCard ×3: 总敞口(CellUsd) | 持仓数 | 预留中        │
├──────────────────────────────────────────────────────────────┤
│ 持仓 Grid: 市场(CellMarketId) | 方向 | 份额 | 成本(CellUsd)    │
│           | 现值(CellUsd) | 未实现 PnL(CellUsd) | 更新时间     │
└──────────────────────────────────────────────────────────────┘
```

- **熔断器面板**(`breaker-panel.vue`):状态机可视化(Closed → Open → HalfOpen → Recovered / Halted,当前态高亮);数据 `useRiskStore().breaker`(REST 首屏 `GET /risk/circuit-breaker` + WS 实时)。
- **Reset**(`v-access:code="'risk:reset'"`,仅 breaker 非 Closed 时可用):

```ts
await governed(
  ({ actingRole, reason }) => resetCircuitBreaker({ reason }, actingRole),
  { title: $t('page.risk.resetBreaker'), danger: true, permissionCode: 'risk:reset' },
);
// 成功后等待 WS risk.circuit_breaker 回显;不乐观更新
```

- **日亏损 Gauge**(`daily-loss-gauge.vue`):当前值 `GET /risk/daily-loss`;限额取当前生效 runtime-config(`GET /runtime-config` 的风控字段,无 `runtime_config:read` 时仅显示绝对值不显示占比)。
- **持仓 Grid**:数据源 `useRiskStore().positions`(REST `GET /risk/positions` 首屏 + WS upsert);`proxyConfig` 关闭(本地数据,store 驱动),用 grid 的 `data` 响应式绑定。

### 1.3 API(`api/risk.ts`)

| 函数 | 端点 | 权限 |
|---|---|---|
| `getCircuitBreaker()` | `GET /risk/circuit-breaker` | risk:read |
| `getPositions()` | `GET /risk/positions` | risk:read |
| `getExposure()` | `GET /risk/exposure` | risk:read |
| `getDailyLoss()` | `GET /risk/daily-loss` | risk:read |
| `resetCircuitBreaker({reason}, actingRole)` | `POST /risk/circuit-breaker/reset` **governed** | risk:reset |

### 1.4 文件清单

```text
views/risk/
├── index.vue
└── modules/
    ├── schemas/{index,table-columns}.ts        # usePositionColumns(无 search/form)
    └── widgets/{breaker-panel.vue, daily-loss-gauge.vue, exposure-cards.vue}
```

---

## 2. Blacklist(`views/blacklist/`)

### 2.1 概要

| 项 | 值 |
|---|---|
| 路由 / component | `/blacklist` / `blacklist/index` |
| 菜单码 | `blacklist:read` |
| 按钮码 | `blacklist:create` / `blacklist:delete`(均 **governed**) |
| grid/drawer | 标准列表页骨架:`useVbenVxeGrid` + 新增 `useVbenModal`(connectedComponent,字段少用 Modal) |
| WS | 无专用通道;增删成功后 `gridApi.query()` |
| i18n | `page.blacklist.*`;`entity.blacklist` |
| types | `BlacklistInfo / BlacklistReason`(`oxide/risk.ts`) |

### 2.2 布局与交互

```text
┌ Grid(无搜索表单,toolbar: [+ 加入黑名单 blacklist:create]):                  ┐
│  市场(CellMarketId) | 原因(CellTag: BlacklistReason 枚举) | 加入时间          │
│  | 操作人 | 操作(CellOperation: 移除 blacklist:delete)                       │
└ AddBlacklistModal(connectedComponent):                                       ┘
   market_id 输入(校验 0x+64hex) + reason 枚举 Select + 治理 reason 文本域
```

- **加入黑名单**:Modal 表单(`useVbenForm`,schema 校验 market_id 格式)+ 治理上下文合一——Modal 内含 acting-role 选择与治理 reason(复用 `GovernedActionModal` 的字段组件 `ReasonField`,不二次弹窗):提交 → `POST /risk/blacklist` body `{market_id, reason}` + `X-Acting-Role`。
- **移除**:`CellOperation` → `useGovernedAction`(纯确认场景,标准治理弹窗)→ `POST /risk/blacklist/{market_id}/remove` body `{reason}`。
- 两动作成功后 `message.success` + `gridApi.query()`。

### 2.3 API(`api/risk.ts`,续)

| 函数 | 端点 | 权限 |
|---|---|---|
| `fetchBlacklist()` | `GET /risk/blacklist` → `BlacklistInfo[]` | blacklist:read |
| `addBlacklist({market_id, reason}, actingRole)` | `POST /risk/blacklist` **governed** | blacklist:create |
| `removeBlacklist(marketId, {reason}, actingRole)` | `POST /risk/blacklist/{market_id}/remove` **governed** | blacklist:delete |

### 2.4 文件清单

```text
views/blacklist/
├── index.vue
└── modules/
    ├── schemas/{index,table-columns,form}.ts   # form.ts: 黑名单新增 schema(market_id/reason 枚举)
    └── widgets/add-blacklist-modal.vue
```

---

## 3. 验收清单

- [ ] 熔断器面板 FSM 可视化与 WS 实时联动;手工触发 breaker(测试环境)后 ≤1s 红显
- [ ] Reset 全链路:viewer 不可见按钮 → operator 治理弹窗(reason+acting-role+确认词)→ 后端 200 → WS 回显 Closed → Operation Log 留痕
- [ ] 日亏损 gauge 阈值分段正确;无 runtime_config:read 角色降级为绝对值显示
- [ ] 持仓表随 `risk.position_update` 实时 upsert(无整表闪烁)
- [ ] 黑名单增删闭环:非法 market_id 被 schema 拦截;治理头与 reason 抓包验证;移除后列表即时刷新
- [ ] 所有金额列经 `CellUsd`;枚举列经 `CellTag` + `enum.*` i18n
