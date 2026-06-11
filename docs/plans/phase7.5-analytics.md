# Phase 7.5 — Analytics 分析页

> **产出**: `/analytics` 多图表仪表盘
>
> **前置**: [phase7.2](phase7.2-overview-realtime-header.md)(`EchartsCard` / store)

---

## 1. 概要

| 项 | 值 |
|---|---|
| 路由 / component | `/analytics` / `analytics/index` |
| 菜单码 | `analytics:read`(无 mutating 按钮) |
| grid/drawer | 图表为主(`EchartsCard`);市场绩效用 `useVbenVxeGrid`(分页);无弹层 |
| WS | 无订阅(离线分析数据,手动/定时刷新);顶部「刷新」按钮 |
| i18n | `page.analytics.*` |
| types | `DailyReport / WeeklyReport / EdgeBucket / MarketPerformanceRow`(`oxide/pnl.ts` + `oxide/analytics.ts`) |

## 2. 布局

```text
┌ 工具栏: 时间范围选择(快捷: 7d/30d/90d + 自定义 RangePicker,默认 7d) [刷新] ┐
├──────────────────────────────┬───────────────────────────────┤
│ Row1L 累计 PnL 曲线(line)     │ Row1R 每日 PnL(bar,正绿负红)   │
├──────────────────────────────┼───────────────────────────────┤
│ Row2L Edge 分布(histogram)    │ Row2R 胜率趋势(line,7d 滚动)   │
├──────────────────────────────┴───────────────────────────────┤
│ Row3 市场绩效 Top Grid(useVbenVxeGrid 分页):                  │
│  市场(CellMarketId) | 交易数 | 胜率(CellPercent)               │
│  | 总 PnL(CellUsd) | 平均 edge(CellBps)                        │
└───────────────────────────────────────────────────────────────┘
```

## 3. 区块规格

| 区块 | 数据源 | 说明 |
|---|---|---|
| 累计 PnL 曲线 | `GET /analytics/daily`(`DailyReport` 序列,按时间范围) | 区域填充 line;tooltip 含当日明细 |
| 每日 PnL bar | 同上(同一次请求复用,页面级一次取数分发各图) | 正绿负红;点击某日 → 带日期跳 `/trades` |
| Edge 分布 | `GET /analytics/edge-distribution?from&to` → `EdgeBucket[]` | bps 分桶 histogram;桶宽由后端返回 |
| 胜率趋势 | `DailyReport` 序列派生(前端 7d 滚动窗口计算) | 标注 50% 参考线 |
| 周报摘要卡(工具栏下,可选展开) | `GET /analytics/weekly` | 最新 `WeeklyReport` KPI 摘要 |
| 市场绩效 Grid | `GET /analytics/market-performance?from&to&page&size` | 标准 `proxyConfig.ajax.query`;时间范围变更触发 `gridApi.query()` |

实现要点:

- **取数编排**:`index.vue` 持有 `dateRange` ref;变更时并行拉 daily 序列 + edge-distribution(`Promise.all` 经 `handleRequest`),grid 走自身 proxy;序列数据放页面局部 ref(非全局 store——本页数据为快照分析,不与 WS 联动)。
- ECharts 主题跟随 vben dark/light(`@vben/plugins/echarts` 既有 `useEcharts` 能力);所有图表 loading/empty 由 `EchartsCard` 壳统一处理。
- 时间范围序列化:`from/to` ISO 8601(UTC),与 `TimeRangeQuery` 一致。

## 4. API(`api/analytics.ts`)

| 函数 | 端点 | 权限 |
|---|---|---|
| `fetchDailyReports({from,to})` | `GET /analytics/daily` | analytics:read |
| `getWeeklyReport()` | `GET /analytics/weekly` | analytics:read |
| `fetchEdgeDistribution({from,to})` | `GET /analytics/edge-distribution` | analytics:read |
| `fetchMarketPerformance({from,to,page,size})` | `GET /analytics/market-performance` | analytics:read |

> 实现时核对 `/analytics/daily` 的实际形状(单报告 vs 序列);若仅返回最新单份,改为消费 `routes/analytics.rs` 实际支持的时间序列入口,差异回写本文档。

## 5. 文件清单

```text
views/analytics/
├── index.vue                       # dateRange 编排 + 并行取数
└── modules/
    ├── schemas/{index,table-columns}.ts     # useMarketPerformanceColumns
    └── widgets/
        ├── cumulative-pnl-chart.vue
        ├── daily-pnl-bar.vue
        ├── edge-distribution-chart.vue
        ├── win-rate-trend.vue
        └── weekly-summary-card.vue
```

## 6. 验收清单

- [ ] 时间范围切换(快捷 + 自定义)联动全部图表与 Grid
- [ ] 四图渲染正确,dark/light 主题切换无残影;窗口 resize 自适应
- [ ] 每日 PnL 点击钻取跳转 `/trades` 带日期过滤
- [ ] 空数据期间(新部署)所有区块显示统一空态而非报错
- [ ] 金额/胜率/bps 全部经统一格式化(与表格 cell 渲染器同一套纯函数)
