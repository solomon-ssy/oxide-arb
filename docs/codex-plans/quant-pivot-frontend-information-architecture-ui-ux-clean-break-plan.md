# Quant Pivot 前端信息架构、设计系统与 UI/UX Clean Break 实施计划

> **For Hermes:** 按任务顺序实施并逐项验证；本计划只定义唯一目标形态，不保留旧菜单、旧路由、旧组件别名、兼容转发或双实现。

**Goal:** 将当前面向后端资源表的 25 个可见页面重构为面向用户工作流的 5 个导航域、11 个可见页面，建立统一的设计系统、枚举展示、实时活动中心和高密度仪表盘，并通过真实前后端栈、可重复的端到端功能测试和截图矩阵形成发布闭环。

**Architecture:** 后端继续拥有 RBAC 菜单、领域 API 和业务事实；前端以少量工作区路由承载同一工作流内的多个资源模块。REST 是页面数据权威，现有 WebSocket 领域事件只负责失效通知。设计层收敛为语义化 token、统一复合组件和穷尽式枚举呈现注册表。此次为 clean break：直接改写当前 v1 DTO、菜单种子和路由，不引入迁移、重定向、适配器或兼容层。

**Tech Stack:** Vue 3.5、TypeScript、Vben Admin、antdv-next 1.4.6、VXE Table、ECharts 6.1、Pinia、Vue Router、Playwright、Vitest、Rust/Axum、SeaORM、PostgreSQL、Redis、现有 WebSocket 基础设施。

---

## 0. 文档权威性与执行边界

### 0.1 权威关系

本文件是以下范围的唯一实施合同：

- 前端一级/二级菜单和页面信息架构；
- 前端路由、工作区内部模块状态和实体深链；
- Dashboard、Activity Center、Command Palette；
- UI 设计 token、动画、图表、枚举 Tag、共享组件治理；
- 与上述页面直接相关的 Dashboard/Activity API 契约；
- 前端旧代码删除、目录合并和死代码清理；
- UI 端到端功能、可访问性、响应式和截图闭环。

它在上述范围内覆盖
`docs/codex-plans/quant-pivot-global-portfolio-runtime-deploy-config-ui-ux-closure-plan.md`
中重叠的 UI 章节；后者仍是 portfolio/config 业务语义的权威。其他后端领域规则继续以项目根 `AGENTS.md`、`docs/plans/quant-pivot/` 和现有测试为准。

### 0.2 本轮计划明确不做

- 不在写计划阶段修改任何生产代码、测试、依赖或数据库；
- 不新增第二套 UI 组件库；
- 不升级 Vue、Vben、antdv-next、VXE 或 ECharts；
- 不改变交易、研究、配置和治理的核心业务语义；
- 不重置 model version、dataset format、policy revision、schema version 等业务/审计版本；
- 不把旧页面继续藏在菜单外，也不保留“以后可能用到”的旧实现；
- 不因视觉升级而虚构数据、用随机数填图或用轮询冒充已有实时事件；
- 不执行真实账户交易、签名或外部生产操作。

### 0.3 Clean break 约束

以下方案零容忍：

- 旧路由 redirect、alias、catch-all 兼容跳转；
- deprecated wrapper、旧组件别名、forwarding re-export；
- 新旧 DTO 并存、`v1`/`v2` 双版本、客户端字段兼容器；
- 菜单清理 migration、`role_menu` backfill、旧菜单 ID 映射；
- 双读、双写、影子页面、旧 localStorage 状态迁移；
- 为旧 E2E 用例保留过期 `package.json` script alias。

所有调用方必须一次性迁移到唯一新实现，然后删除旧符号和旧文件。

---

## 1. 当前状态审计结论

### 1.1 菜单问题的根因

当前可见导航大致为：Command Center 1、Trading 3、Execution 6、Research 13、Governance 1、Audit 1，共 25 个页面。Research 将 model spec、dataset、model、backtest、factor、calibration、comparison、policy、feedback、feature integrity、market linkage、domain source、basis alert、job 等后台资源逐一映射为侧边栏入口。

问题不只是“菜单数量太多”，而是导航对象错误：

- 菜单体现数据库/领域资源，而不是用户要完成的任务；
- 同一研究链路被迫跨多个页面往返，丢失上下文；
- 详情抽屉重复实现，页面之间缺少清晰的 lineage；
- 低频管理资源与高频操作资源拥有同等导航权重；
- Dashboard、任务状态和系统状态分散，用户难以回答“现在发生了什么”；
- 页面骨架相似但组件实现各异，视觉与交互规律不可预测。

### 1.2 组件与页面审计结论

- 项目已经具备成熟的 Ant/VXE/ECharts 能力，无需引入新组件库。
- `antdv-next` 不提供稳定的 `List` 组件；上游 Ant Design 已移除/废弃 List。列表场景应按数据形态使用 Table/VXE Table、Timeline、Collapse、Card 或语义化原生列表，而不是引入第二套 UI 框架。
- 原生 `<div>` 本身不是缺陷；缺陷是用裸 DOM 重造 Table、Descriptions、Tabs、状态提示、交互反馈和可访问性语义。
- `StatCard` 与 `KpiStatCard` 重叠；状态 Tag 至少存在三条呈现路径；Dashboard 卡片/图表容器重复；实体详情标题和键值布局重复。
- Research 下存在多份 1000 行以上 drawer/panel，混合查询、状态、权限、布局、动作和子资源渲染，已经超出可维护边界。
- `page-placeholder.vue`、`detail-section-card.vue`、`key-value-grid.vue`、`waterfall-chart.vue` 当前没有真实调用方或仅被 barrel 导出；`dashboardAccentStyle()` 已废弃且无调用方。

### 1.3 Dashboard 审计结论

现有 Dashboard 已有 5 个 KPI、权益/回撤、暴露、生命周期和健康状态等可用骨架，不应推倒重写。目标是：

- 保留业务含量高的已有可视化；
- 将运行时、报告、执行、数据面和任务状态补齐；
- 首屏形成“资产—风险—执行—任务—数据质量”完整态势；
- 删除装饰性空卡片，空状态必须说明原因、时间和下一动作。

---

## 2. 目标信息架构：5 个导航域、11 个可见页面

### 2.1 最终菜单树

```text
Command Center
└── Dashboard                         /dashboard

Trading & Signals
├── Market Intelligence               /trading/market-intelligence
└── Recommendations                   /trading/recommendations

Execution & Capital
├── Orders                            /execution/orders
├── Portfolio                         /execution/portfolio
└── Post-Trade                        /execution/post-trade

Research & Models
├── Research Lab                      /research/lab
├── Learning & Policy                 /research/learning-policy
└── Data Reliability                  /research/data-reliability

System Governance
├── Configuration                     /system/config
└── Audit                             /system/audit
```

另外保留一个不出现在侧边栏的鉴权路由：

```text
Activity History                     /runtime/activity
```

该路由服务于 Activity Center 的“查看全部”、浏览器可分享链接和审计回溯，不作为第 12 个菜单。

### 2.2 旧页面到新工作区的唯一映射

| 旧入口/资源 | 新页面 | 页面内部模块 |
|---|---|---|
| Markets、Structural | Market Intelligence | Overview、Live Market、Structure |
| Reports、Recommendations | Recommendations | Reports、Recommendation Queue、Detail Inspector |
| Intents、Execution Orders | Orders | Intents、Approval Queue、Orders、Execution Flow |
| Account、Positions | Portfolio | Account、Positions、Exposure、Equity |
| Reconciliations、Settlement Redeems | Post-Trade | Reconciliation、Settlement、Governed Actions |
| Model Specs、Datasets、Models、Backtests、Factors、Calibration、Comparisons | Research Lab | Lineage、Specs、Datasets、Models、Experiments、Evaluation |
| Trade Policies、Policy Fits、Feedback、Feature Integrity | Learning & Policy | Policies、Fits、Feedback Cycles、Feature Integrity |
| Market Linkages、Domain Sources、Basis Alerts、Research Jobs | Data Reliability | Sources、Linkages、Alerts、Jobs |
| Config | Configuration | Runtime、Deploy、Policy、Activation History |
| Operation Log | Audit | Operations、Governed Receipts、Entity Timeline |

### 2.3 工作区内导航规则

- URL 查询参数是可分享模块状态的唯一来源：
  `?module=<module>&entity=<entity_kind>&id=<entity_id>`。
- `module` 必须是每个工作区声明的封闭 union；非法值直接回退到该工作区默认模块，并用 `replace` 写回规范 URL。
- `entity` 与 `id` 同时存在才打开 Object Inspector；缺失或资源不可见时展示局部 Not Found，不跳转旧详情页。
- 页面内部一级切换使用 Tabs/Segmented；低频辅助设置使用 Drawer/Popover；详情使用右侧 Inspector；不再为每个资源创建菜单。
- 浏览器前进/后退必须完整恢复 module、filter、entity inspector 状态。
- 旧 `/markets`、`/quant/*` 和资源式 `/research/*` 路由全部删除，不建立 redirect/alias。
- 后端 `/api/quant/*`、`/api/research/*` 是领域 API 路径，不是旧前端导航，继续保留。

### 2.4 菜单与权限

- 后端 `crates/quant-pivot-models/src/seed/rbac/menus.rs` 直接重写为目标菜单树。
- `crates/quant-pivot-models/src/seed/rbac/role_menu.rs` 直接按新菜单 ID/权限重建种子关系。
- 菜单权限代表“能否进入工作区”；工作区内模块、数据段和操作仍由现有资源权限独立裁剪。
- 一个工作区至少有一个可见模块时才显示；不可访问模块不出现在 Tabs 中，也不泄漏数量或摘要。
- Activity Center 仅返回当前用户有权读取的领域活动；无任一活动领域权限时返回 403。

---

## 3. 设计方向：Indigo–Violet–Cyan Quant Console

### 3.1 视觉原则

- 基础色以深靛蓝为稳定锚点，紫罗兰用于研究/智能，青色用于实时数据/运行中状态。
- 渐变只承载层级、方向和状态，不在每个容器无差别铺设。
- 数据平面优先高信息密度；浮层可以 glassmorphism，数据卡片保持清晰实体表面。
- 颜色不是唯一信息编码，所有状态同时提供文本，必要时增加图标和形状。
- 图表色与枚举色分层治理：连续数据使用顺序/发散色板，离散状态使用语义色，类别使用稳定哈希色。
- 每个页面都有明确的视觉焦点，但不靠大面积空白制造“高级感”。

### 3.2 Token 层

修改：

- `ui/packages/@core/base/design/src/design-tokens/default.css`
- `ui/apps/web-antdv-next/src/styles/index.css`

新增唯一语义 token：

- 表面：`--qp-surface-base`、`--qp-surface-raised`、`--qp-surface-overlay`、`--qp-surface-inset`；
- 文本：`--qp-text-primary`、`--qp-text-secondary`、`--qp-text-muted`；
- 边界：`--qp-border-subtle`、`--qp-border-active`；
- 品牌渐变：`--qp-gradient-command`、`--qp-gradient-research`、`--qp-gradient-realtime`；
- 状态：success/warning/danger/info/neutral/running/paused；
- 图表：categorical、sequential、diverging；
- 阴影与 glow：只保留 low/medium/semantic 三档；
- 动效：duration、easing、distance、stagger；
- 密度：card padding、section gap、table row height、inspector width。

禁止组件内散落十六进制颜色、临时渐变、任意 shadow 和独立 z-index。

### 3.3 动画规范

| 场景 | 时长 | 规则 |
|---|---:|---|
| Hover/press | 120ms | 只用 transform、opacity、filter |
| Tooltip/Popover | 180ms | 淡入 + 轻微缩放 |
| 页面/模块切换 | 240ms | 8px 内位移，保持上下文 |
| 图表数据 morph | 360ms | ECharts update，不重建实例 |
| KPI 首次 count-up | 600–900ms | 仅首次数据加载；更新时 crossfade |
| 运行中 pulse | 2400ms | 只用于真实 running 状态 |

约束：

- 同屏最多 3 个持续动画，且必须表达真实语义；
- 页面隐藏时暂停图表和持续动画；
- `prefers-reduced-motion: reduce` 下关闭 count-up、pulse、粒子、平移和图表入场，只保留瞬时状态变化；
- loading、success、failure 不能只靠动画区别；
- 截图测试等待动画稳定并在 deterministic 模式关闭非必要动效；
- 正常动效和 reduced-motion 另设可控时钟契约测试，不能用截图规避验证。

### 3.4 创新交互

1. **Command Palette**：`⌘K`/`Ctrl+K` 搜索页面、工作区模块、实体和当前可执行动作；结果严格受 RBAC 约束。
2. **Aurora Command Rail**：Dashboard 顶部聚合 runtime mode、kill switch、WebSocket、数据 freshness、报告状态和活动任务，渐变光带只表达状态变化。
3. **Research Lineage Graph**：ECharts Graph/Sankey 呈现 `Spec → Dataset → Model → Backtest → Calibration → Publication`，点击节点在同页 Inspector 打开实体。
4. **Execution Flow Rail**：`Intent → Approval → Admission → Submission → Fill → Position → Reconciliation → Settlement`，每一步显示状态、耗时和可执行动作。
5. **Live KPI behavior**：首载 count-up；后续实时变更数字 crossfade，sparkline morph，绝不重复播放整段入场动画。
6. **Contextual glass overlay**：Activity Center、Command Palette、Object Inspector 使用受控玻璃表面；普通数据卡不使用。

---

## 4. 统一组件体系与替换规则

### 4.1 组件选型矩阵

| 数据/交互形态 | 唯一组件策略 |
|---|---|
| 可排序、筛选、分页的结构化集合 | VXE Table 或现有 Ant Table 适配层 |
| 轻量紧凑集合 | `CompactDataTable`，不创建伪 List |
| 时间顺序事件 | Ant Timeline |
| 渐进披露 | Ant Collapse |
| 少量动作项/活动项 | Card + 语义化 `<ul>/<li>` |
| 标签页模块 | Ant Tabs/Segmented |
| 键值详情 | Ant Descriptions + `ObjectInspector` |
| 状态/枚举 | `EnumTag` |
| 页面异步状态 | `AsyncState` |
| 图表容器 | `ChartPanel` |
| KPI | `KpiCard` |

### 4.2 必须完成的组件收敛

目标目录：`ui/apps/web-antdv-next/src/shared/components/`。

| 旧实现 | 新唯一实现 | 删除策略 |
|---|---|---|
| `stat-card.vue` + `kpi-stat-card.vue` | `kpi-card.vue` | 迁移全部调用方后删除两旧文件 |
| `state-badge.vue` + `catalog-state-tag.vue` + `format/tag-options.ts` | `enum-presentation.ts` + `enum-tag.vue` + `cell-enum-tag.ts` | 不留旧 export |
| `dashboard-panel.vue` | `insight-panel.vue` | 删除旧名 |
| `echarts-card.vue` | `chart-panel.vue` | 删除旧名 |
| `data-list.vue` | `compact-data-table.vue` | clean rename；不保留 alias |
| `entity-detail-header.vue` + `detail-section-card.vue` + `key-value-grid.vue` | `object-inspector/` | 迁移 drawer 内容后删除旧文件 |
| `entity-route-button.vue` | 直接组合 `RouterLink` + Button | 删除薄封装 |
| `detail-back-nav.vue` | Breadcrumb/Inspector close | 删除 |
| `dashboard-accent.ts` | `visual-tokens.ts` + CSS token | 删除废弃函数和旧文件 |
| `feature-parity-status-panel.vue` | Learning & Policy 私有模块 | 移出 global shared |

继续保留并规范使用：

- `async-state.vue`
- `bullet-list.vue`
- `governed-action-modal.vue`
- `inline-bar.vue`
- `input-number-with-addon.vue`
- `preflight-report-block.vue`
- `preflight-result-drawer.vue`
- `reliability-chart.vue`
- `run-report-modal.vue`
- `runtime-control-panel.vue`
- `signed-value.vue`
- `entity-route-link.vue`（仅在确实增加实体路由语义时保留）

### 4.3 Object Inspector 结构

新建：

```text
shared/components/object-inspector/
├── object-inspector.vue
├── object-inspector-header.vue
├── object-inspector-section.vue
├── object-inspector-actions.vue
├── object-inspector-timeline.vue
├── object-inspector.types.ts
└── index.ts
```

规则：

- 详情对象只负责声明 sections、fields、actions 和 timeline 数据；
- Inspector 统一处理宽度、loading、error、not found、focus trap、ESC、返回焦点和 URL 同步；
- 普通字段交给 Ant Descriptions；枚举字段必须使用 `EnumTag`；
- 危险或治理动作继续使用 `GovernedActionModal`，不可把权限逻辑下沉到展示组件；
- 超长 JSON/代码/矩阵使用专用 viewer，不塞入 Descriptions label/value。

### 4.4 页面拆分阈值

- 单一 `.vue` 文件目标不超过 500 行；超过 700 行必须在同一任务中拆分。
- 页面容器只编排路由、权限、查询和模块；业务详情下沉到模块组件。
- 抽屉/Inspector 不直接持有多个相互独立的 API 查询；每个 section 用独立 composable。
- 表格 column、filter schema、enum registry 和 action policy 分文件组织。
- 不以“shared”为理由提前抽象；只有两个以上真实调用方且语义一致才进入 global shared。

---

## 5. 枚举 Tag 统一契约

### 5.1 类型设计

新建：

- `ui/apps/web-antdv-next/src/shared/presentation/enum-presentation.ts`
- `ui/apps/web-antdv-next/src/shared/components/enum-tag.vue`
- `ui/apps/web-antdv-next/src/shared/components/cell-enum-tag.ts`

核心类型：

```ts
type EnumTone =
  | 'neutral'
  | 'info'
  | 'success'
  | 'warning'
  | 'danger'
  | 'running'
  | 'paused'
  | 'category';

interface EnumPresentation {
  readonly labelKey: string;
  readonly tone: EnumTone;
  readonly icon?: string;
  readonly emphasis?: 'subtle' | 'solid';
}
```

每个后端封闭枚举必须通过 `satisfies Record<EnumValue, EnumPresentation>` 穷尽声明。不得以 `Record<string, ...>` 逃避编译检查。

### 5.2 全局覆盖范围

以下所有只读枚举都必须呈现为颜色可区分、带文本的 Tag：

- Table/VXE cell；
- Ant Descriptions；
- Object Inspector；
- Timeline/Activity item；
- KPI footer/secondary state；
- Select 已选项与 option；
- Chart tooltip/legend 中的离散状态；
- 空状态或错误详情中的领域状态。

输入型 radio/segmented/button group 不强制 Tag，但选中结果的只读展示必须使用 Tag。

### 5.3 色彩规则

- success、warning、danger、running、paused 等状态使用固定语义色；
- side、resource kind、model family 等类别使用稳定 palette，不把类别误画成成功/失败；
- running 可以使用轻微 pulse，paused 不动画；
- unknown wire enum 必须显示显式 error Tag，并使对应契约测试失败，不默默降级成灰色文本；
- 每个 Tag 必须通过文本和可选 icon 传达信息，满足 WCAG use-of-color；边框/填充与相邻表面满足 non-text contrast。

---

## 6. Dashboard 目标形态

### 6.1 1440×900 首屏布局

1. Aurora Command Rail：runtime、kill switch、WS、freshness、latest report、running tasks。
2. 6 个 KPI：NAV、available funds、exposure utilization、active positions、pending approvals、running tasks。
3. Equity/Drawdown 主图。
4. Report-to-Execution 漏斗/流图。
5. Realtime Task Monitor。
6. Execution & Settlement 状态。
7. Data Plane：ingest latency、book freshness、sequence gap、persistence lag。

首屏必须可在 1440×900 查看上述模块标题和关键数值；可滚动查看完整 exposures、research readiness、action inbox 和 subsystem health。

### 6.2 数据契约

现有：

- `crates/quant-pivot-models/src/domain/api/dashboard.rs`
- `crates/quant-pivot-web/src/routes/dashboard.rs`
- `ui/apps/web-antdv-next/src/api/dashboard.ts`

保留现有 `DashboardOverviewView` 的 revision、generated_at、window、authority、account、equity_curve、latest_report、report_lifecycle、exposures、data_quality、research_readiness、subsystem_health、action_inbox，并在当前 v1 中直接增加：

- `runtime_activity`
- `report_runtime`
- `execution_runtime`
- `data_plane`

不创建 v2，不保留旧 v1 adapter。仍使用现有 `Accept-Api-Version: v1` 头；这里的 v1 是当前唯一 API 契约，不是兼容承诺。

### 6.3 数据与失败行为

- 每个 Dashboard section 保持独立 permission gate 和独立失败边界；
- 一个 section 失败不隐藏其他 section，错误卡展示时间、错误类别和 retry；
- 无数据状态展示“为什么为空、最近更新时间、下一动作”，不保留空白占位；
- REST snapshot 是权威；WebSocket 只使对应 section query 失效；
- 不允许客户端拼接账户真相、伪造趋势或以常量填充 KPI；
- 所有金额沿用后端 decimal string，不转为 JS `number`。

---

## 7. 全局 Activity Center

### 7.1 用户体验

- Header 增加 Activity Center 入口，显示 running/failed/attention 数量，而不是普通通知铃铛复制品。
- Drawer 默认显示摘要、过滤器和最近 25 条活动；“查看全部”进入 `/runtime/activity`。
- 支持 domain、status 过滤，支持稳定 cursor 翻页。
- 活动项展示：标题、领域、种类、状态、严重度、进度、开始/更新时间、关联实体、可用动作。
- 点击关联实体进入对应新工作区并打开 Object Inspector。
- retry/cancel/acknowledge 等动作调用现有领域 endpoint；禁止新增万能 mutation endpoint。
- 任务状态变化采用 FLIP/crossfade，running 项可有受控 pulse，失败项禁止抖动或持续闪烁。

### 7.2 后端读取模型

新建：

- `crates/quant-pivot-models/src/domain/api/runtime_activity.rs`
- `crates/quant-pivot-web/src/routes/runtime_activities.rs`

更新：

- `crates/quant-pivot-models/src/domain/api/mod.rs`
- `crates/quant-pivot-web/src/routes/mod.rs`

只聚合现有 repository 数据：

- `crates/quant-pivot-repository/src/postgres/quant/research_job.rs`
- `crates/quant-pivot-repository/src/postgres/quant/report_run.rs`
- `crates/quant-pivot-repository/src/postgres/quant/execution_order.rs`
- `crates/quant-pivot-repository/src/postgres/quant/reconciliation.rs`
- 现有 settlement repositories

不得新增 activity event 表，不得复制领域生命周期事实。

### 7.3 REST 契约

```http
GET /api/runtime/activities?domain=<domain>&status=<status>&cursor=<opaque>&limit=25
```

响应：

```ts
interface RuntimeActivityPageView {
  revision: string;
  generated_at: string;
  visible_domains: RuntimeActivityDomain[];
  summary: RuntimeActivitySummaryView;
  items: RuntimeActivityView[];
  next_cursor: string | null;
}

interface RuntimeActivityView {
  activity_id: string;
  domain: RuntimeActivityDomain;
  kind: RuntimeActivityKind;
  status: RuntimeActivityStatus;
  severity: RuntimeActivitySeverity;
  title: string;
  progress: RuntimeActivityProgressView | null;
  started_at: string | null;
  updated_at: string;
  finished_at: string | null;
  resource_ref: RuntimeActivityResourceRef | null;
  available_actions: RuntimeActivityAction[];
}
```

约束：

- `domain`、`kind`、`status`、`severity`、`available_actions` 都是封闭枚举；
- cursor 是服务器编码的 `(updated_at, domain, activity_id)`，排序固定为 descending；
- `limit` 默认 25，最大 100；
- 只统计并返回用户有权限的领域，禁止通过 summary 泄漏不可见领域数量；
- `available_actions` 同时满足 RBAC 与生命周期状态；
- 无任一可见领域返回 403，而不是空的全局摘要。

### 7.4 前端数据流

新建：

- `ui/apps/web-antdv-next/src/api/runtime-activities.ts`
- `ui/apps/web-antdv-next/src/store/runtime-activity.ts`
- `ui/apps/web-antdv-next/src/shared/components/runtime-activity/activity-center.vue`
- `ui/apps/web-antdv-next/src/shared/components/runtime-activity/activity-item.vue`
- `ui/apps/web-antdv-next/src/shared/components/runtime-activity/activity-summary.vue`
- `ui/apps/web-antdv-next/src/views/runtime/activity/index.vue`

更新：

- `ui/apps/web-antdv-next/src/layouts/basic.vue`
- `ui/apps/web-antdv-next/src/shared/composables/ws/ws-dispatch.ts`

不新增全局 WS channel。复用现有 `materialization.run_update`、report、intent、reconciliation、settlement 等领域 channel，在事件到达时 invalidate Activity store；REST 重新读取并执行权限过滤。没有相关订阅时使用有界、页面可见时才运行的低频 refresh，不能建立高频无限 polling。

---

## 8. 11 个页面的具体设计

### 8.1 Dashboard `/dashboard`

- 使用第 6 节布局；
- 顶部 rail 支持从异常状态直接跳转 Activity Center 或目标工作区；
- 图表 hover 联动对应 KPI 和时间窗口；
- action inbox 合并重复动作，显示 owner、age、severity。

### 8.2 Market Intelligence `/trading/market-intelligence`

- Overview：市场数、freshness、spread、liquidity、structural signals；
- Live Market：可搜索市场列表 + price/depth/spread/imbalance 图；
- Structure：market linkage、结构性信号和异常；
- 选择市场写入 `entity=market&id=...`，同页 Inspector 展示详情；
- 图表与订单簿在 tab 隐藏时暂停更新。

### 8.3 Recommendations `/trading/recommendations`

- Reports：最新报告、历史报告、运行状态；
- Recommendation Queue：TopN、confidence、edge、sizing、entry conditions；
- 报告详情与 recommendation 详情共享 Inspector，不再跳转独立页面；
- 报告 lifecycle 使用可交互 step/timeline，清楚区分 generated、published、acted；
- 从 recommendation 可深链到 market 和关联 intent。

### 8.4 Orders `/execution/orders`

- 顶部显示 pending approval、active orders、fill rate、latency；
- Intents、Approval Queue、Orders 三模块在同一工作区；
- Execution Flow Rail 串联 intent 到 position；
- 审批、拒绝、取消继续走 GovernedActionModal 和 preflight；
- 操作完成后局部刷新 flow、activity 和 KPI，不整页 reload。

### 8.5 Portfolio `/execution/portfolio`

- Account：NAV、collateral、available、unrealized/realized PnL；
- Positions：结构化表格、风险状态 Tag、market deep link；
- Exposure：treemap、方向/主题/时间聚合；
- Equity：equity/drawdown 和 snapshot history；
- 金额、价格、shares 全程 string/newtype 边界，不使用浮点金融计算。

### 8.6 Post-Trade `/execution/post-trade`

- Reconciliation、Settlement、Governed Actions；
- 差异使用 waterfall/bridge chart 时直接在页面私有模块实现，旧未用 `waterfall-chart.vue` 不保留；
- reconciliation/settlement 生命周期共用 Timeline 与 Inspector；
- unresolved、blocked、ready、completed 使用统一 EnumTag；
- 审计 receipt 可一键跳转 `/system/audit` 对应实体。

### 8.7 Research Lab `/research/lab`

- 默认 Lineage 总览；
- Specs、Datasets、Models、Experiments、Evaluation 为内部模块；
- Lineage Graph 是主要导航面，不是纯装饰图；
- 节点点击打开 Inspector，边展示输入输出关系和状态；
- 创建/运行/比较动作保持现有权限和服务端验证；
- comparison 作为 evaluation 内的模式，不保留独立菜单/页面。

### 8.8 Learning & Policy `/research/learning-policy`

- Policies、Fits、Feedback Cycles、Feature Integrity；
- 从 policy 到 fit、feedback、feature parity 建立上下文链；
- feature parity panel 移入该页面私有模块；
- feedback 治理动作保留 receipt、preflight 和 audit deep link；
- 对训练/评估差异使用语义 chart，不用大段裸 `dl/div`。

### 8.9 Data Reliability `/research/data-reliability`

- Sources、Linkages、Alerts、Jobs；
- 顶部 KPI：source health、freshness breaches、open basis alerts、running/failed jobs；
- jobs 使用 Activity-compatible presentation，但领域动作仍走研究 job endpoint；
- source/linkage/basis alert 详情共用 Inspector；
- acknowledgment 形成 audit receipt 并更新 Activity Center。

### 8.10 Configuration `/system/config`

- Runtime、Deploy、Policy、Activation History；
- 保留当前 typed config、preflight、activation 和 rollback 业务语义；
- 将不同配置域收敛为一致的 compare → validate → approve → activate 流程；
- 生效状态和 enum 值统一用 Tag；
- 配置 diff 使用专用 diff viewer，不用 Descriptions 承载整段 JSON。

### 8.11 Audit `/system/audit`

- Operation Log、Governed Receipts、Entity Timeline；
- 支持来自任何工作区的 `entity/id` deep link；
- Timeline 显示 actor、action、result、resource、timestamp、correlation；
- 只读页面，不在 UI 中伪造“撤销”能力；
- 旧 operation-log 页面实现迁入后删除旧目录和空 `.gitkeep`。

---

## 9. 数据库、版本与兼容策略

### 9.1 数据库迁移

本计划不新增也不修改数据库 migration。

依据：

- 当前 PostgreSQL 只有 canonical bootstrap migration：
  `crates/quant-pivot-migration/src/migrations/m00000000_000001_bootstrap.rs`；
- 菜单是 seed，不是 schema；
- Activity Center 聚合现有事实表，不新增表；
- Dashboard 只是读取模型扩展，不需要持久化结构变化。

执行时若发现 Activity Center 必须新增持久化字段，必须暂停该任务并更新本计划；不得顺手修改 bootstrap 或引入第二条兼容 migration。

### 9.2 Seed 与环境

- 直接重写 `menus.rs` 和 `role_menu.rs`；
- 使用空数据库执行 canonical bootstrap + 新 seed；
- dev/test/production-stack fixture 都从空状态验证；
- 不提供旧菜单清理 SQL、旧角色关系 backfill、旧菜单 ID 转换；
- 不迁移浏览器 localStorage，E2E 使用 fresh context；
- 不保留旧 URL bookmark 兼容。

### 9.3 API/DTO 版本

- 继续使用当前唯一 `Accept-Api-Version: v1`；
- 直接覆盖 Dashboard v1 DTO，Activity Center 以 v1 唯一契约加入；
- 不创建 v2、不提供旧字段 adapter、不双序列化字段；
- model version、dataset format、policy revision、schema version 是领域事实，不因 UI clean break 修改。

### 9.4 发布和回退

系统从未真实生产运行，因此不设计 canary、online migration、dual read/write 或兼容回滚。

回退手段只有：

1. 源码级 revert；
2. 重新创建空的 dev/test 环境；
3. 重新执行 canonical bootstrap 和当前 seed。

任何真实本地数据库清空都需要用户明确授权；Playwright 隔离的临时数据库可由测试 fixture 正常创建和销毁。

---

## 10. 详细删除与目录合并清单

### 10.1 前端页面目录

迁移调用方并删除：

```text
ui/apps/web-antdv-next/src/views/markets/
ui/apps/web-antdv-next/src/views/quant/reports/
ui/apps/web-antdv-next/src/views/quant/recommendations/
ui/apps/web-antdv-next/src/views/quant/structural/
ui/apps/web-antdv-next/src/views/quant/intents/
ui/apps/web-antdv-next/src/views/quant/execution-orders/
ui/apps/web-antdv-next/src/views/quant/account/
ui/apps/web-antdv-next/src/views/quant/positions/
ui/apps/web-antdv-next/src/views/quant/reconciliations/
ui/apps/web-antdv-next/src/views/quant/settlement-redeems/
ui/apps/web-antdv-next/src/views/research/model-specs/
ui/apps/web-antdv-next/src/views/research/datasets/
ui/apps/web-antdv-next/src/views/research/models/
ui/apps/web-antdv-next/src/views/research/backtests/
ui/apps/web-antdv-next/src/views/research/factors/
ui/apps/web-antdv-next/src/views/research/calibration-artifacts/
ui/apps/web-antdv-next/src/views/research/comparisons/
ui/apps/web-antdv-next/src/views/research/trade-policies/
ui/apps/web-antdv-next/src/views/research/trade-policy-fits/
ui/apps/web-antdv-next/src/views/research/feedback/
ui/apps/web-antdv-next/src/views/research/feature-integrity/
ui/apps/web-antdv-next/src/views/research/market-linkages/
ui/apps/web-antdv-next/src/views/research/domain-sources/
ui/apps/web-antdv-next/src/views/research/basis-alerts/
ui/apps/web-antdv-next/src/views/research/jobs/
ui/apps/web-antdv-next/src/views/config/
ui/apps/web-antdv-next/src/views/operation-log/
```

上述目录内容分别迁入新目录后整体删除：

```text
ui/apps/web-antdv-next/src/views/trading/market-intelligence/
ui/apps/web-antdv-next/src/views/trading/recommendations/
ui/apps/web-antdv-next/src/views/execution/orders/
ui/apps/web-antdv-next/src/views/execution/portfolio/
ui/apps/web-antdv-next/src/views/execution/post-trade/
ui/apps/web-antdv-next/src/views/research/lab/
ui/apps/web-antdv-next/src/views/research/learning-policy/
ui/apps/web-antdv-next/src/views/research/data-reliability/
ui/apps/web-antdv-next/src/views/system/config/
ui/apps/web-antdv-next/src/views/system/audit/
ui/apps/web-antdv-next/src/views/runtime/activity/
```

不得通过旧目录 `index.ts` re-export 新目录。

### 10.2 必须拆分的大型详情组件

以下文件的业务内容迁入对应工作区的 `modules/`、`inspectors/`、`composables/`，随后删除原文件：

- `research/models/modules/model-detail-drawer.vue`
- `research/model-specs/modules/model-spec-detail-drawer.vue`
- `research/datasets/modules/dataset-detail-drawer.vue`
- `research/backtests/modules/backtest-detail-drawer.vue`
- `research/calibration-artifacts/modules/calibration-artifact-detail-drawer.vue`
- `research/factors/modules/factor-detail-drawer.vue`
- `research/factors/modules/factor-collinearity-drawer.vue`
- `research/feedback/modules/feedback-cycle-detail-panel.vue`
- `research/feature-integrity/modules/parity-run-drawer.vue`
- `research/feature-integrity/modules/parity-event-drawer.vue`
- `research/market-linkages/modules/linkage-detail-drawer.vue`
- `research/market-linkages/modules/linkage-override-drawer.vue`
- quant 下 intent、execution-order、position、reconciliation、settlement-redeem、settlement-governed-action、equity-snapshot 的 detail drawer。

### 10.3 共享组件删除

迁移后删除：

```text
shared/components/page-placeholder.vue
shared/components/detail-section-card.vue
shared/components/key-value-grid.vue
shared/components/waterfall-chart.vue
shared/components/stat-card.vue
shared/components/kpi-stat-card.vue
shared/components/state-badge.vue
shared/components/catalog-state-tag.vue
shared/components/format/tag-options.ts
shared/components/dashboard-panel.vue
shared/components/echarts-card.vue
shared/components/data-list.vue
shared/components/entity-detail-header.vue
shared/components/entity-route-button.vue
shared/components/detail-back-nav.vue
shared/components/dashboard-accent.ts
```

同时删除所有对应 barrel export、测试 fixture、story/sample、CSS selector 和 import。`operation-log/modules/.gitkeep` 随旧目录删除。

### 10.4 路由和测试遗留

- 删除旧路由声明、route name、menu path、breadcrumb mapping 和权限快照；
- 删除 `/markets`、`/quant/*`、旧 `/research/*` 的字符串常量与测试断言；
- 删除旧截图目录和 snapshot，按新矩阵重新生成；
- 删除旧 E2E script：`test:e2e:config-visual`、feedback UI/closure 等被合并脚本；
- 不保留 npm script alias；
- `rg` 最终扫描不得发现旧路径、旧组件名或废弃函数。

---

## 11. 分阶段实施任务

### Task 1：冻结可量化基线和删除清单

**Files:**

- Create: `ui/apps/web-antdv-next/tests/contracts/ui-clean-break-inventory.test.ts`
- Modify: `ui/apps/web-antdv-next/package.json`

**Actions:**

1. 记录当前菜单数量、路由路径、页面目录、共享组件调用图和 1000+ 行组件。
2. 将本计划的旧路径/旧组件列表写成失败式 inventory 测试：实施完成时必须为 0。
3. 保存现有页面的基线截图仅用于实现对照，不作为发布 golden。
4. 确认工作树已有用户修改，逐文件避免覆盖。

**Verify:**

- inventory 在重构初期按预期失败；
- 输出的失败项与第 10 节一致，不扫描 `node_modules`、build output 或 docs。

### Task 2：改写菜单 seed 和前端路由

**Files:**

- Modify: `crates/quant-pivot-models/src/seed/rbac/menus.rs`
- Modify: `crates/quant-pivot-models/src/seed/rbac/role_menu.rs`
- Modify: `ui/apps/web-antdv-next/src/router/menu-adapter.ts`
- Modify: 前端 canonical route modules（实施时先用 `rg` 定位唯一注册点）
- Test: 菜单 seed、route、menu adapter 现有测试

**Actions:**

1. 一次性建立第 2.1 节菜单树和新 route name。
2. 删除所有旧可见 menu seed 和旧 role-menu 关系。
3. 注册 11 个可见页面和 1 个隐藏 Activity History 路由。
4. 删除旧 routes，不写 redirect/alias。
5. 更新菜单适配器，使 hidden route 不进入侧边栏，但可鉴权和深链。

**Verify:**

- fresh seed 后恰好 11 个可见 leaf page；
- 侧边栏无 Research 资源级菜单；
- 直接打开 12 个新路由均匹配；
- 旧路由全部 404/Not Found，不跳转。

### Task 3：建立 token、motion 和统一枚举呈现

**Files:**

- Modify: `ui/packages/@core/base/design/src/design-tokens/default.css`
- Modify: `ui/apps/web-antdv-next/src/styles/index.css`
- Create: `ui/apps/web-antdv-next/src/shared/presentation/enum-presentation.ts`
- Create: `ui/apps/web-antdv-next/src/shared/components/enum-tag.vue`
- Create: `ui/apps/web-antdv-next/src/shared/components/cell-enum-tag.ts`
- Test: `ui/apps/web-antdv-next/src/shared/**/__tests__/`

**Actions:**

1. 实现 light/dark 两套语义 token 和渐变。
2. 实现 reduced-motion 覆盖。
3. 将全部现有 wire enum 汇入穷尽式 registry。
4. 先迁移 Table、Descriptions 和 Select，再删除三条旧状态呈现路径。
5. 为 unknown enum、contrast、label/icon fallback 写契约测试。

**Verify:**

- TypeScript 在后端 enum 新增值但 registry 未补齐时编译失败；
- 所有 enum snapshot 使用 `EnumTag`；
- light/dark/reduced-motion 均通过视觉和 Axe 验证。

### Task 4：建立共享复合组件并清除旧实现

**Files:**

- Create: `shared/components/kpi-card.vue`
- Create: `shared/components/insight-panel.vue`
- Create: `shared/components/chart-panel.vue`
- Create: `shared/components/compact-data-table.vue`
- Create: `shared/components/object-inspector/**`
- Create: `shared/presentation/visual-tokens.ts`
- Delete: 第 10.3 节全部旧文件

**Actions:**

1. 用真实页面调用方驱动新组件 API，不保留旧 props 形状。
2. 迁移 Dashboard、Research 和 Execution 的至少一个代表页面作为契约样本。
3. 完成 focus management、keyboard、loading/error/empty、responsive 行为。
4. 迁移余下调用方后删除旧组件及 exports。

**Verify:**

- 新组件单测覆盖 keyboard、async state、enum fields、resize；
- `rg` 无旧组件 import/export；
- inventory 对共享组件部分归零。

### Task 5：实现全局 Activity Center 后端读取模型

**Files:**

- Create: `crates/quant-pivot-models/src/domain/api/runtime_activity.rs`
- Modify: `crates/quant-pivot-models/src/domain/api/mod.rs`
- Create: `crates/quant-pivot-web/src/routes/runtime_activities.rs`
- Modify: `crates/quant-pivot-web/src/routes/mod.rs`
- Modify/Test: 对应 repository query 与 web route tests

**Actions:**

1. 定义封闭 enum、page/item/summary/resource/action DTO。
2. 为现有 repository 增加必要的 cursor query；不创建通用 service 薄转发。
3. 在 web route 并行读取用户有权领域，并稳定 merge-sort。
4. 为不可见领域过滤 summary 和 items。
5. 从现有 endpoint 计算可用动作，不添加万能 mutation endpoint。

**Verify:**

- cursor 无重复/遗漏，边界同 timestamp 时顺序稳定；
- 只具 Research 权限的用户看不到 execution count/item；
- 无活动读取权限返回 403；
- limit > 100、非法 domain/status/cursor 返回 typed 4xx；
- cargo format、clippy、route tests、architecture audit 通过。

### Task 6：实现 Activity Center、Command Palette 和应用壳层

**Files:**

- Create: `ui/apps/web-antdv-next/src/api/runtime-activities.ts`
- Create: `ui/apps/web-antdv-next/src/store/runtime-activity.ts`
- Create: `ui/apps/web-antdv-next/src/shared/components/runtime-activity/**`
- Create: `ui/apps/web-antdv-next/src/shared/components/command-palette/**`
- Create: `ui/apps/web-antdv-next/src/views/runtime/activity/index.vue`
- Modify: `ui/apps/web-antdv-next/src/layouts/basic.vue`
- Modify: `ui/apps/web-antdv-next/src/shared/composables/ws/ws-dispatch.ts`

**Actions:**

1. Activity store 实现 snapshot、cursor、filters、visibility-aware refresh。
2. 现有 WS 事件只 invalidate，不将 WS payload 当权威对象。
3. Command Palette 从新菜单、可见模块、实体搜索和可用动作构建结果。
4. 完成 keyboard shortcut、focus trap、screen reader label 和 reduced-motion。
5. Header 数量只统计当前用户可见活动。

**Verify:**

- WS burst 合并为有界 refresh；
- drawer 和 full page 的过滤/分页状态一致；
- 命令搜索不显示无权页面/动作；
- Escape、Tab loop、返回焦点和 mobile drawer 通过测试。

### Task 7：升级 Dashboard 与 v1 DTO

**Files:**

- Modify: `crates/quant-pivot-models/src/domain/api/dashboard.rs`
- Modify: `crates/quant-pivot-web/src/routes/dashboard.rs`
- Modify: `ui/apps/web-antdv-next/src/api/dashboard.ts`
- Rewrite: `ui/apps/web-antdv-next/src/views/dashboard/index.vue`
- Modify/Create: `ui/apps/web-antdv-next/src/views/dashboard/modules/**`

**Actions:**

1. 增加四个 runtime section，直接更新当前 v1。
2. 用 `KpiCard`、`InsightPanel`、`ChartPanel` 重构现有骨架。
3. 实现 Aurora Command Rail 和首屏 6 KPI。
4. 保留 equity/drawdown、exposure 和 lifecycle 中有效图表，删除重复/装饰性模块。
5. 对 section failure、stale、empty、permission denied 分别建局部状态。

**Verify:**

- 1440×900 首屏满足第 6.1 节；
- Dashboard 权限切片无数据泄漏；
- WS race/旧 response 覆盖防护通过；
- 不存在随机 mock KPI 或 JS 浮点金额计算。

### Task 8：重构 Trading & Signals 两个工作区

**Files:**

- Create: `views/trading/market-intelligence/**`
- Create: `views/trading/recommendations/**`
- Migrate/Delete: `views/markets/**`、`views/quant/reports/**`、`recommendations/**`、`structural/**`

**Actions:**

1. 建立 typed module query 和同页 Inspector。
2. 迁移 live market widgets，统一图表生命周期和 resize。
3. 合并 report/recommendation 流程，建立 report → recommendation → intent deep link。
4. 删除旧页面目录、路由、测试和 exports。

**Verify:**

- market selection、report lifecycle、recommendation detail 和 deep link E2E 通过；
- hidden tab 不继续高频图表更新；
- inventory 中 Trading 旧路径归零。

### Task 9：重构 Execution & Capital 三个工作区

**Files:**

- Create: `views/execution/orders/**`
- Create: `views/execution/portfolio/**`
- Create: `views/execution/post-trade/**`
- Migrate/Delete: 第 10.1 节 quant execution 目录

**Actions:**

1. 合并 intent/order 并实现 Execution Flow Rail。
2. 合并 account/position/equity/exposure。
3. 合并 reconciliation/settlement/governed action。
4. 将大型 drawers 重构为 Object Inspector sections。
5. 保留所有 preflight、approval、audit receipt 和 fail-closed 行为。

**Verify:**

- intent approval → order → position → reconciliation → settlement 端到端链路通过；
- 金融值无 `number` 转换；
- 无权 action 不渲染且服务端拒绝；
- Execution 旧路径/旧 drawer 归零。

### Task 10：重构 Research & Models 三个工作区

**Files:**

- Create: `views/research/lab/**`
- Create: `views/research/learning-policy/**`
- Create: `views/research/data-reliability/**`
- Migrate/Delete: 第 10.1 和 10.2 节 Research 目录/组件

**Actions:**

1. Research Lab 实现 lineage graph 和 6 个内部模块。
2. Learning & Policy 串联 policy、fit、feedback、feature integrity。
3. Data Reliability 合并 sources、linkages、alerts、jobs。
4. 把 1000+ 行 drawers 拆为 query composable、section、actions、timeline。
5. 删除 13 个旧菜单对应的所有旧页面入口。

**Verify:**

- lineage 节点/边、实体 Inspector 和 module query 可深链；
- feedback/policy/data reliability 治理动作形成 audit receipt；
- 研究 job 状态同步到 Activity Center；
- Research 静态扫描只剩 3 个 route-level `index.vue`。

### Task 11：重构 System Governance 两个工作区

**Files:**

- Create: `views/system/config/**`
- Create: `views/system/audit/**`
- Migrate/Delete: `views/config/**`、`views/operation-log/**`

**Actions:**

1. 保留 typed config 与 activation 业务逻辑，重组工作流布局。
2. 用专用 diff viewer 取代超长 Descriptions/裸 JSON。
3. 将 operation log、receipt、entity timeline 合并到 Audit。
4. 实现跨工作区 audit deep link。
5. 删除旧目录、`.gitkeep`、旧 route 和旧 visual snapshots。

**Verify:**

- config validate/approve/activate/rollback 流程通过；
- audit deep link 可定位关联 receipt；
- System 旧路径归零。

### Task 12：全仓删除、导入收敛和静态审计

**Files:**

- Delete: 第 10 节全部遗留
- Modify: 所有 barrel、route、i18n、test fixture、snapshot index

**Actions:**

1. 运行 `rg` 查找旧路径、旧组件名、旧 route name、旧 menu title。
2. 删除只剩 export 的文件和无调用 CSS。
3. 使用仓库现有 dependency/unused 工具检查 orphan module；没有现成工具时以 TypeScript/Vite 构建图和 inventory test 为权威，不临时引入依赖。
4. 检查所有新页面用户可见字符串走现有 i18n。
5. 检查页面没有从组件库重造 Table/Descriptions/Modal/Tabs。

**Verify:**

- inventory test 全绿；
- `rg` 允许项只有本计划/docs，不包含运行时代码和测试；
- typecheck/build 无 unused import/export；
- 25→11 菜单断言通过。

### Task 13：重写功能 E2E 与发布证据工具

**Files:**

- Rewrite: `ui/apps/web-antdv-next/tests/e2e/deterministic-visual-matrix.ts`
- Rewrite: `ui/apps/web-antdv-next/tests/e2e/browser-failure-audit.ts`
- Modify/Create: `ui/apps/web-antdv-next/tests/e2e/*.spec.ts`
- Modify: `ui/apps/web-antdv-next/package.json`
- Delete: 被合并的旧 specs/scripts/snapshots

**Actions:**

1. 将旧 specs 收敛为：
   - `research-lab-flow`
   - `learning-policy-flow`
   - `data-reliability-flow`
   - `recommendations-flow`
   - `execution-orders-flow`
   - `portfolio-flow`
   - `post-trade-flow`
   - `dashboard-runtime-race`
   - `config-governance-flow`
   - `ui-release-closure`
2. 将 fixture/harness 统一为 `ui-release-evidence-harness`。
3. 删除旧 `config-visual`、feedback-only、w2/w4 别名脚本。
4. 建立第 12 节截图矩阵和 manifest。

**Verify:**

- 每个业务 spec 先验证功能状态，再截图；
- 不使用 frontend route mock；
- fixture 使用真实 PostgreSQL、Redis、Axum route/middleware、WebSocket；外部 venue 使用 deterministic adapter。

### Task 14：全量质量门与双跑验收

按第 12、13 节完整执行。任何失败都回到责任任务修复，禁止更新 golden 掩盖差异，禁止跳过测试。

---

## 12. 端到端截图与功能性闭环

### 12.1 页面矩阵：88 张

11 个可见页面 × 4 viewport × 2 theme：

| Viewport | 尺寸 |
|---|---:|
| Desktop | 1440×900 |
| Laptop | 1280×800 |
| Tablet | 1024×768 |
| Mobile | 390×844 |

每个页面分别生成 light/dark 截图，共 `11 × 4 × 2 = 88`。

隐藏 Activity History 由功能状态矩阵覆盖，不计入 11 个菜单页基线。

### 12.2 功能状态矩阵：34 张

以下 17 个状态在 1440×900 下分别生成 light/dark：

1. Dashboard healthy；
2. Dashboard partial degraded；
3. Activity Center running task；
4. failed task → retry → recovery；
5. Command Palette entity/action search；
6. market detail + structural signal；
7. report running；
8. report complete/published；
9. intent pending approval；
10. intent approved + linked order；
11. portfolio exposure；
12. reconciliation resolved；
13. settlement approved；
14. research lineage dataset → evaluation；
15. policy + feedback + feature integrity；
16. basis alert acknowledge + source health；
17. config activate/rollback + audit receipt。

共 `17 × 2 = 34`，总截图数为 `88 + 34 = 122`。

### 12.3 每张截图前的 readiness gate

页面必须同时满足：

- 根节点 `data-ui-ready="true"`；
- 预期 HTTP 请求 drain；
- ECharts `finished` 或应用级 chart-settled 信号；
- 无 skeleton、global spinner、未预期 toast；
- 无 browser console error、pageerror、unhandled rejection；
- 无未允许的 4xx/5xx；
- Axe 无 serious/critical violation；
- 无横向 overflow；
- 字体、图标、主题 token 加载完成；
- deterministic 模式的非必要动画已关闭并稳定。

等待使用有上限的事件/条件，不使用任意固定长 sleep。

### 12.4 功能闭环要求

截图不能代替功能断言。每个状态必须通过 UI 操作形成：

- 用户点击/键盘操作；
- 前端请求真实 Axum route；
- route 读取真实临时 PostgreSQL/Redis 数据；
- WebSocket 事件触发 store invalidation；
- REST snapshot 返回权威状态；
- 页面更新并满足领域断言；
- 最终截图和浏览器失败审计通过。

禁止用 `page.route()` 伪造后端业务接口。只允许 deterministic external adapter 隔离真实外部 venue。

### 12.5 双跑确定性

- 在两个全新测试环境各执行完整 122 张矩阵；
- 每轮 fresh DB、fresh Redis namespace、fresh browser context；
- 两轮 manifest 的场景、viewport、theme、数据 revision 完全一致；
- 截图 aggregate SHA 一致；
- `maxDiffPixels = 0`；
- 两轮均无 console/pageerror/HTTP/Axe/overflow/orphan failure；
- 动效正常模式和 reduced-motion 契约测试单独双跑，不将动态中间帧作为 screenshot golden。

### 12.6 证据目录

```text
ui/apps/web-antdv-next/test-results/ui-release-closure/<run-id>/
├── manifest.json
├── summary.json
├── screenshots/
├── playwright-report/
├── traces/
├── browser-failures.json
├── accessibility.json
└── screenshot-hashes.json
```

`manifest.json` 至少记录 git worktree hash、前后端 build id、seed revision、viewport、theme、locale、timezone、scenario、data revision 和截图 SHA-256。

---

## 13. 测试与质量门

### 13.1 前端

在 `/Users/eason/code/personal/rebirth/oxide-arb/ui` 执行，以实际 `package.json` 为最终命令来源：

```bash
pnpm -F @vben/web-antdv-next test
pnpm -F @vben/web-antdv-next typecheck
pnpm -F @vben/web-antdv-next build
pnpm lint
pnpm check
pnpm test:e2e:ui-release-closure
```

如果 workspace 已有更精确的 unit 命令，一并执行；不得为了计划临时保留旧命令 alias。

### 13.2 后端

在 `/Users/eason/code/personal/rebirth/oxide-arb` 执行：

```bash
cargo fmt --all --
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask architecture audit-functions
cargo xtask architecture check
cargo test --workspace
```

### 13.3 Fresh boot

- canonical bootstrap migration 在空 PostgreSQL 成功；
- 新 seed 生成 11 个可见页面和正确 role-menu；
- production-stack fixture 启动 PostgreSQL、Redis、后端、前端和 WS；
- 旧 URL 返回 Not Found；
- 新 URL 首次加载、刷新和 deep link 均成功；
- 不依赖任何旧 localStorage、旧数据库行或旧截图缓存。

### 13.4 静态 clean-break 审计

至少扫描：

```bash
rg -n "(/markets|/quant/|model-specs|calibration-artifacts|trade-policy-fits|feature-integrity|market-linkages|domain-sources|basis-alerts)" ui/apps/web-antdv-next/src ui/apps/web-antdv-next/tests
rg -n "(StatCard|KpiStatCard|StateBadge|CatalogStateTag|DataList|DashboardPanel|EchartsCard|DetailSectionCard|KeyValueGrid|dashboardAccentStyle)" ui/apps/web-antdv-next/src
rg -n "(redirect:|alias:)" ui/apps/web-antdv-next/src/router
```

预期结果为 0；若新业务文本确需同名，必须以 inventory allowlist 的精确文件/语义说明，而不是放宽正则。

---

## 14. 可访问性、性能与响应式验收

### 14.1 可访问性

- 所有可交互元素可键盘到达并有 visible focus；
- Drawer/Palette/Modal 有正确 focus trap 和返回焦点；
- 图表提供可读摘要和关键值，不把 canvas 当唯一信息；
- Tag 不只依赖颜色；
- reduced-motion 完整生效；
- Axe serious/critical = 0；
- 触控目标和非文本对比满足 WCAG 2.2。

### 14.2 性能预算

- 不新增第二 UI/图表库；
- route-level code split 保持 11 个工作区独立 chunk；
- Dashboard/Lineage 的 ECharts 按需注册，不全量注册 chart；
- 隐藏 tab 的图表和 refresh 停止；
- WS burst 去抖/合并，Activity Center 不产生 N+1 请求；
- 100+ 行列表使用虚拟化或服务端分页；
- 动画只使用 compositor-friendly 属性；
- 以现有 bundle analyzer/性能测试为准，重构后主入口 bundle 不得无解释显著增长。

### 14.3 响应式

- Desktop：侧栏 + 多列 dashboard + 右侧 Inspector；
- Laptop：减少图表列数，不隐藏关键 KPI；
- Tablet：侧栏折叠，Inspector 占更宽 drawer；
- Mobile：单列、底部/全屏详情、表格切换为关键列 + row detail；
- 禁止通过 `overflow-x: hidden` 掩盖布局溢出；
- 390px 下所有治理动作仍可完成，不只可查看。

---

## 15. 风险控制与实施判断点

以下情况实施时必须暂停并请求用户拍板：

1. Activity Center 无法仅通过现有事实表表达所需状态，确实需要新持久化模型；
2. 一个新工作区需要改变服务端业务权限语义，而不只是聚合展示；
3. 现有 UI 组件版本存在阻断性缺陷，必须升级依赖或引入新库；
4. 某个旧页面承载计划未覆盖且仍有真实业务价值的独立工作流；
5. 实际 E2E 必须触发真实签名/下单才能完成，而 deterministic adapter 无法安全覆盖。

不属于暂停条件：改动文件多、需要大规模目录迁移、旧测试大量失效、需要删除旧页面。它们正是本计划授权的 clean-break 工作。

---

## 16. Definition of Done

只有以下条件全部满足，前端重构才算完成：

- 侧边栏恰好 5 个导航域、11 个可见页面；
- Research 从 13 个资源入口收敛为 3 个工作区；
- 旧前端路由无 redirect/alias，全部不可访问；
- 第 10 节旧目录、旧组件、barrel export、脚本和 snapshot 全部删除；
- 全部只读 enum 在 Table、Descriptions、Inspector、Timeline、Select 和图表语义中统一为可访问 Tag；
- Dashboard 具备真实 6 KPI、实时任务、执行、报告和数据面状态，空状态不留白；
- Activity Center 聚合真实领域事实，权限不泄漏，WS 只失效通知；
- Command Palette、Lineage Graph、Execution Flow Rail 和动效/reduced-motion 契约完成；
- 11 个页面在 4 viewport × 2 theme 下通过；
- 17 个功能状态 light/dark 通过；
- 两轮各 122 张截图，`maxDiffPixels = 0`，aggregate SHA 一致；
- 浏览器 console/pageerror/HTTP/Axe/overflow/orphan failure 全部为 0；
- fresh bootstrap + seed + production-stack fixture 通过；
- 前端 test/typecheck/build/lint/check 和后端 fmt/clippy/architecture/test 全部通过；
- 没有新增 migration、兼容 DTO、旧 localStorage 转换、第二 UI 库或 dependency upgrade；
- 未提交、未推送任何变更，除非用户另行明确要求。

---

## 17. 调研依据

- [Ant Design — Navigation](https://ant.design/docs/spec/navigation/)
- [Ant Design — Research Workbench](https://ant.design/docs/spec/research-workbench/)
- [Ant Design — Data Display](https://ant.design/docs/spec/data-display/)
- [Ant Design — Motion](https://ant.design/docs/spec/motion/?locale=en-US)
- [ECharts — Dynamic Data](https://echarts.apache.org/handbook/en/how-to/data/dynamic-data/)
- [WCAG 2.2 — Animation from Interactions](https://www.w3.org/WAI/WCAG22/Understanding/animation-from-interactions)
- [WCAG 2.2 — Non-text Contrast](https://www.w3.org/WAI/WCAG22/Understanding/non-text-contrast)
- [WCAG 2.2 — Use of Color](https://www.w3.org/WAI/WCAG22/Understanding/use-of-color)
- [antdv-next repository](https://github.com/antdv-next/antdv-next)
- [Ant Design changelog（List 移除/废弃记录）](https://github.com/ant-design/ant-design/blob/master/CHANGELOG.en-US.md)

