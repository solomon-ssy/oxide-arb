# quant-pivot Operator Console

> 状态：当前实现架构。范围为 `ui/apps/web-antdv-next`、前端共享包、后端动态菜单 seed 与对应 Web API。
>
> 部署契约：首次部署必须从空数据库 fresh boot。旧 URL、旧页面、旧菜单、旧 DTO 与旧截图不迁移、不兼容；禁止 redirect、alias、re-export、双写和旧命名转发。

## 1. 产品定位

Operator Console 围绕 `RecommendationReport` 建立研究、决策、执行与治理闭环。默认
`report_only` 仍读取真实 venue account 做 sizing；它不是 paper trading，也不是 dry-run。执行入口
只能由 `OrderIntent` 进入，并由 runtime mode、approval、admission、kill-switch 与 settlement policy
逐层 fail closed。

## 2. 信息架构

动态菜单只发布 5 个业务域、12 个页面：

| 域 | 页面 | Canonical path |
|---|---|---|
| Command | Dashboard | `/dashboard` |
| Command | Activity Center | `/runtime/activity` |
| Trading | Market Intelligence | `/trading/market-intelligence` |
| Trading | Recommendations | `/trading/recommendations` |
| Execution | Orders | `/execution/orders` |
| Execution | Portfolio | `/execution/portfolio` |
| Execution | Post-Trade | `/execution/post-trade` |
| Research | Research Lab | `/research/lab` |
| Research | Learning & Policy | `/research/learning-policy` |
| Research | Data Reliability | `/research/data-reliability` |
| System | Config | `/system/config` |
| System | Audit | `/system/audit` |

页面内功能由 `module` query 切换；对象详情统一由 `entity` + `id` 打开。任何旧 resource URL 都是
404，不做跳转。后端 menu `component` 必须直接解析到对应 workspace 文件。

## 3. 交互与视觉系统

- dark-first token 体系统一 surface、border、status、chart、density、radius、layer 与 motion；明暗主题共享语义，不共享硬编码颜色。
- Inter 与 JetBrains Mono 自托管；金额、价格、比例、hash 与 ID 使用等宽字体和 tabular numerals。
- `EnumTag` 的展示表由 Rust enum schema 生成并穷尽覆盖。未知 wire 值必须 danger 显示、记录结构化 drift，并使契约测试失败。
- 列表只负责定位；详情统一进入 Object Inspector。Research Lineage 与 Execution Flow Rail 使用同一 Inspector 结构。
- Dashboard、Activity Center、Workspace 与 ECharts 暴露确定性的 readiness 状态，供视觉证据门禁消费。
- motion 使用 token 化 duration/easing；`prefers-reduced-motion: reduce` 将动效收敛为单次 1ms，不靠截图验证动画。

## 4. 数据权威边界

- REST/数据库快照是列表和详情的唯一权威事实；WebSocket 只发 revision/invalidation hint，随后重新读取 REST。
- Pinia 只保存跨页连接状态、revision、短生命周期选择和 market book 热点缓存，不保存分页列表事实。
- 所有 money/price/shares/bps wire value 在 TypeScript 中保持 decimal string。
- mutation 必须经过统一 governed action，携带 reason、确认词、request id 与审计上下文；403/409/422 不得吞掉。
- 运行模式、审批、执行、结算和策略激活均 fail closed。页面不得通过隐藏按钮替代服务端 AuthZ。

## 5. 发布闭环

`pnpm test:e2e:ui-release-closure` 在两个全新 production-stack 环境中重复运行：

- 10 个真实后端、真实 PostgreSQL/Redis 的功能 spec；禁止 `page.route()` 伪造业务响应。
- 50 张视觉证据：12 页桌面 dark/light、12 页移动 dark、14 个桌面 dark 关键态。
- 每张图要求 `data-ui-ready=true`、HTTP drain、ECharts finished、无 skeleton/toast、字体就绪、Axe serious/critical 为 0、横向 overflow 为 0。
- 像素阈值 `threshold=0.2`、`maxDiffPixelRatio=0.002`。
- manifest 记录前后端 build id、seed/data revision、viewport、theme、locale、timezone、scenario 与截图 SHA-256；两次 fresh boot 的场景和数据 revision 必须一致。

## 6. 当前实施索引

- [Phase 10 当前实现索引](phase-10/README.md)
- [契约与 clean-break inventory](phase-10/10.0-contract-and-deletion-inventory.md)
- [导航、Dashboard 与 Market Intelligence](phase-10/10.2-navigation-dashboard-markets-account.md)
- [RecommendationReport 平面](phase-10/10.3-report-plane.md)
- [执行平面](phase-10/10.4-execution-plane.md)
- [Research、Reliability 与 System](phase-10/10.5-research-and-governance.md)
