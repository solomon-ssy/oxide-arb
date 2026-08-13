# quant-pivot Boot 基线、Config 治理与控制台 UI/UX 重构计划

> **Historical evidence only — not an implementation contract.** 本文件记录此前 boot/config 重构的决策与
> 验收历史；所有仍涉及 Runtime resource、Deploy loader/TOML、report partition、portfolio sizing、UI config
> editor 或截图门禁的规范，均不得从本文实施。唯一当前契约是
> [`quant-pivot-global-portfolio-runtime-deploy-config-ui-ux-closure-plan.md`](quant-pivot-global-portfolio-runtime-deploy-config-ui-ux-closure-plan.md)
> 及其链接的 `docs/plans/quant-pivot/` 正式架构文档。

## 一、目标与当前结论

本次采用破坏式 clean-break 重构，不保留旧配置解析器、版本兼容、字段别名或 re-export。仓库尚未正式生产运行，因此所有系统自有版本、数据库迁移和配置结构直接收敛为首个生产候选基线。

当前审计结论：

- Runtime Config 约有 226 个叶子字段，Deploy Config 约有 230 个叶子字段，代码中约有 657 个常量；存在业务策略、研究方法、基础设施容量、凭据和 UI 展示偏好混杂的问题。
- PostgreSQL 有 7 个历史 migration、ClickHouse 有 9 个历史 migration，可安全压平为单一 boot migration。
- Runtime Config schema 已递增到 18，Feature、Dataset、Model、Trade Policy 等内部版本分散在 2～7；已有测试与常量漂移，例如 dataset artifact 测试期望版本 5、代码实际为 6。
- 当前 Runtime Config UI 仍以巨型 schema 表单、版本表格和原始 JSON 抽屉为核心；激活请求没有传后端要求的 `runtime_config_approval_id`，且“一键创建并激活”破坏治理步骤闭环。
- `feedback`、hold-to-resolution 等配置目前存在无有效消费者或 no-op 路径；`MAX_STALE_RATIO_BPS=2000` 与 Runtime Config 语义重复，Dashboard 还存在硬编码 200 天查询范围。
- 当前 UI 实机截图被后端 500 页面阻塞，因此该截图只作为环境问题证据，不属于视觉验收基线。重构后使用确定性 E2E fixture 完成真实页面截图验收。

最终成功标准：

1. 全新数据库只执行一个 PostgreSQL boot migration 和一个 ClickHouse boot migration。
2. 所有系统自有 schema/manifest/evaluator 版本从首个版本重新开始，代码、测试、文档一致。
3. Runtime Config 只保留真正需要热更新、具备明确消费者和生效边界的配置。
4. 每个 Runtime Config 字段都能从 UI/API 追踪到验证、预检、原子切换、消费者和审计记录。
5. Config 控制台简洁、清晰、可访问，完整覆盖编辑、校验、审批、激活、回滚、投产封存。
6. 桌面、平板、移动端、明暗主题和 reduced-motion 均通过端到端截图验收。

---

## 二、版本、Migration 与正式投产封存

### 2.1 版本重置规则

| 版本类别 | 最终处理 | 原因 |
|---|---|---|
| Runtime Config schema 18 | 删除统一大文档版本；新策略资源各自从 `schema_version = 1` 开始 | 新模型不是旧 Runtime Config 的兼容升级 |
| Feature/Dataset/Model/TradePolicy/Catalog/Evidence/EntryCondition 等系统自有 schema | 全部重置为 `1` | 尚无生产数据需要解释 |
| ClickHouse 行 `schema_version` | 每种内部事实结构从 `1` 开始 | 配合全新表结构和数据库清空 |
| evaluator/manifest/internal format version | 重置为 `1` | 属于系统内部契约 |
| Cargo workspace/package SemVer | 统一为 `0.1.0` | 表达首个未正式发布的生产候选 |
| migration 文件序号 | PostgreSQL `m00000000_000001_bootstrap`；ClickHouse `version = 1` | 明确 boot 基线语义 |
| API 路由版本 `/api/v1` | 保留 | 它是公开 HTTP namespace，不是历史数据库迁移计数 |
| Ethereum chain ID、EIP-712、Polymarket/第三方 API 协议版本 | 保留协议真实值 | 外部协议编号不能按项目生命周期重置 |
| 算法或模型的内容哈希、训练 run ID、artifact ID | 不替换为常量版本 | 应继续由内容和运行实例唯一确定 |

所有旧版本常量、旧测试断言、旧错误信息、旧 seed 数据和 phase 文档中的“由 N 升至 N+1”全部 clean-break 更新；不建立旧值映射表，也不接受旧文档。

### 2.2 Migration 压平

- PostgreSQL 将现有 7 个 migration 的最终结构、约束、索引、触发器、枚举、初始治理记录整合进一个 boot migration。
- ClickHouse 将现有 9 个 migration 的最终表、projection、TTL、partition/order key 和 schema registry 整合进一个 bootstrap migration。
- 删除历史 alter/backfill/copy/rename 路径、兼容 view、临时列和只为旧数据服务的校验。
- boot migration 不承担旧库升级；检测到旧 migration history 或非空未知 schema 时直接 fail closed，并提示“清空基础设施后重新 bootstrap”。
- 删除旧 migration 文件和仅被旧 migration 消费的辅助代码、测试、manifest 项。
- 增加 fresh-install 验收：空 PostgreSQL、空 ClickHouse、空 Redis 从零启动，schema fingerprint 与代码声明完全一致。

不会在实施过程中自动删除用户数据库、缓存或对象存储；只提供明确的 reset plan/检查命令，实际销毁动作仍由用户单独授权执行。

### 2.3 正式投产封存

新增独立生命周期契约 `project-lifecycle.toml`：

```toml
state = "pre_production_resettable"
baseline = "boot"
```

状态只有两种：

- `pre_production_resettable`：允许破坏式 schema reset、migration squash、内部版本重新基线。
- `production_frozen`：不可逆；后续所有 schema、数据和版本变化必须使用正式 migration、兼容性评估、回滚方案和数据验证。

正式投产采用“封存”而不是普通开关：

1. Config 控制台展示环境、数据库 schema fingerprint、应用构建版本、未完成 migration、备份状态和基础 E2E 状态。
2. 操作者输入环境名和指定确认短语。
3. 后端重新执行投产预检并写入 append-only `system_production_baseline`：
   - `baseline_id`
   - `sealed_at`
   - `sealed_by`
   - build commit
   - PostgreSQL/ClickHouse schema fingerprint
   - config resource revision bundle
   - lifecycle policy hash
4. 状态一旦为 `production_frozen`，API、CLI 和 migration 工具全部拒绝 boot reset。
5. `project-lifecycle.toml`、数据库 baseline 和部署环境声明必须一致；不一致时应用拒绝启动。
6. 当前 `BootstrapPhase::Active` 继续只表示冷启动运行阶段，不复用为正式上线标记。

### 2.4 Phase 文档治理

- 给 `docs/plans/quant-pivot/` 下全部约 69 个 phase 文档增加统一 lifecycle 声明：
  - 当前项目未正式上线。
  - 当前阶段允许 boot baseline 重置。
  - `production_frozen` 后必须考虑数据、结构、版本迁移。
- 已完成 phase 保留业务决策历史，但删除会误导后续实施的旧版本递增指令，并标记其版本号已被 boot baseline 取代。
- 从 Phase 11.9 起，模板强制包含：
  - lifecycle assumption
  - schema/data/version impact
  - pre-production 行为
  - production-frozen 行为
  - rollback/data verification
- 新增文档 lint：遗漏 lifecycle、继续引用旧 schema 版本或在 frozen 状态要求 squash migration 时 CI 失败。

---

## 三、Config 逐项处置清单

### 3.1 现有 Runtime Config 顶层字段

| 当前字段 | 处理 | 新归属 | 原因 |
|---|---|---|---|
| `schema_version` | 删除统一版本 | 每个策略资源自带固定 `schema_version = 1` | 不再存在单一巨型配置文档 |
| `selection` | 合并 | `RecommendationPolicy` | 市场筛选直接决定报告结果 |
| `data_quality` | 合并 | `RecommendationPolicy.data_quality` | 是报告准入策略，必须随 report run 冻结 |
| `features` | 迁移 | 不可变 `FeatureProfile` artifact | 特征定义影响训练、回放和 lineage，不应热改 |
| `factors` | 迁移 | 不可变 `ScoringProfile` artifact | 因子构造和权重属于研究/模型方法 |
| `domain` | 拆分 | provider endpoint/binding → Deploy；语义 → `DomainProfile` artifact；active route → `ModelRouting` | 当前混合连接信息、研究语义和运行路由 |
| `model` | 收敛 | `ModelRouting` | Runtime 只决定 active/shadow/exit artifact 指针 |
| `quality_gate` | 删除当前 no-op；后续迁入 | `ModelPromotionPolicy` | 只有真正接入 promotion workflow 后才能成为 runtime policy |
| `training` | 迁移 | 每次任务冻结的 `TrainingRunSpec` | 训练输入应属于 job/artifact，而非全局热配置 |
| `reports` | 拆分 | cadence/timezone → `ReportSchedule`；TopN/horizon/TTL → `RecommendationPolicy` | 调度与报告业务语义必须独立治理 |
| `portfolio` | 合并 | `ExecutionRiskPolicy` | 使用真实账户进行 sizing，属于资金风险策略 |
| `execution` | 拆分 | 风险阈值 → `ExecutionRiskPolicy`；模式授权 → `ExecutionAuthorization`；halt/pause → `OperationalControl` | 风险、授权和即时操作不能混在一个文档 |
| `notification` | 拆分 | endpoint/secret → Deploy Credential；启停和事件路由 → `OperationalControl.notifications` | secret 不得进入 Runtime Config |
| `research` | 迁移 | 不可变 `ResearchProfile` 和每次 job spec | purge/CPCV/PBO/trials 等属于可复现研究方法 |
| `feedback` | 删除 | 能力落地后新增 `FeedbackSchedule` | 当前字段没有完整有效消费者 |

### 3.2 新 Runtime Config 资源

| 资源 | 内容 | 热更新生效边界 |
|---|---|---|
| `RecommendationPolicy` | selection、data-quality、TopN、报告 horizon、报告有效期 | 新 claim 的 `ReportRun`；运行中的 report 保持原快照 |
| `ExecutionRiskPolicy` | sizing、Kelly 安全约束、exposure、entry/exit 风险限制、breaker 阈值 | 新建 `OrderIntent` 和新的 admission decision；已提交订单不被隐式修改 |
| `ModelRouting` | category 对应 active/shadow/exit model artifact | 新 report/model evaluation run；已运行任务保持旧指针 |
| `ReportSchedule` | timezone、calendar、cadence、enabled、下一次触发 | 更新后立即 reconcile 尚未 claim 的 future runs；已 claim run 不变 |
| `OperationalControl` | report pause、execution halt、notification routing、worker admission | pause/halt admission gate 原子生效；已签名/已提交订单不自动撤销 |
| `ExecutionAuthorization` | `ReportOnly`/`SemiAuto`/`AutoExecution` 的正式授权状态和约束 | 完成 mode preflight 后原子切换；只影响后续 admission |
| `FeedbackSchedule` | 未来反馈/retraining 调度 | worker 真正落地时再引入 |
| `ModelPromotionPolicy` | 未来模型 promotion gate | promotion workflow 真正落地时再引入 |

前六项本次实现；后两项只保留明确扩展契约，不创建空表、空 UI 或 no-op 配置。

### 3.3 Deploy Config 处置

Deploy Config 最终只保留启动时才能决定的内容：

- 服务 endpoint、bind address、allowed origin、deployment identity。
- PostgreSQL、ClickHouse、Redis、artifact store 的连接位置。
- Polymarket/Gamma/CLOB/Data API 和外部 domain provider 的 endpoint/binding。
- funder、wallet kind、secret provider 和 credential 名称。
- 日志格式/级别、TLS、JWT issuer/audience。
- 七组主机资源预算：
  1. `database`
  2. `clickhouse_writer`
  3. `market_data_ingest`
  4. `cache`
  5. `research_jobs`
  6. `report_execution`
  7. `web`
- deployment lifecycle 和 production baseline expectation。

必须删除或迁出的 Deploy 字段：

| 字段类型 | 处理 | 原因 |
|---|---|---|
| bot token、private key、DB password、JWT signing key、webhook secret | 移出 TOML/env value，改用 systemd Credentials 的 credential reference | 避免 secret 出现在环境、进程信息和 config dump；遵循 [systemd Credentials](https://systemd.io/CREDENTIALS/) 与 [OWASP Secrets Management](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html) |
| selection、risk、quality、report semantic threshold | 移入对应 Runtime Policy | 需要审计和热更新 |
| feature/factor/research methodology | 移入不可变 artifact/profile | 必须参与 lineage 和内容哈希 |
| worker lease/heartbeat/poll 的重复独立值 | 合并成资源预算并派生合法比例 | 防止大量互相矛盾的时间参数 |
| protocol chain/contract/type 常量 | 移入代码常量或 provider catalog | 不应由部署人员任意修改 |
| UI pagination、展示时区、默认查询跨度 | UI preference 或命名常量 | 与进程部署无关 |
| migration-only DDL credentials | 仅允许 schema CLI credential scope | 运行进程不持有 DDL 权限 |

Deploy Config 的来源优先级收敛为：

1. source-controlled non-secret TOML；
2. environment-specific immutable override；
3. systemd Credentials 提供 secret；
4. 不再允许任意环境变量覆盖业务语义。

环境变量只保留真正需要按部署环境变化的值，避免把整个配置树暴露为难以审计的字符串集合。该边界与 [Twelve-Factor Config](https://www.12factor.net/config) 的环境差异原则一致，但不会将所有内部结构机械地环境变量化。

### 3.4 硬编码常量处置

| 常量类别 | 处理 |
|---|---|
| 系统自有 schema/evaluator/manifest 版本 | 重置为 `1`，集中在对应契约模块 |
| 外部协议编号、链 ID、数学不变量、Decimal scale | 保持代码常量 |
| 安全上限，如请求体大小、最大 schedule preview 数量 | 保持不可编辑常量并集中命名 |
| 会改变报告、资金或执行决策的阈值 | 迁入对应 Runtime Policy |
| 会改变训练/验证结果的参数 | 迁入不可变 profile/job spec |
| 依赖主机规模的并发、队列、池容量 | 迁入 Deploy resource budgets |
| 重试/backoff/timeout | 外部依赖相关且环境敏感的进入 Deploy；内部算法固定值集中为常量 |
| UI 颜色、spacing、animation duration | 使用现有 design token 和集中 motion token，不进入后端 config |
| 重复或无消费者常量 | 删除 |
| 与 schema 同时手写的验证范围 | 从 typed schema/validator 单一来源生成 |

明确修正：

- `MAX_STALE_RATIO_BPS=2000` 删除硬编码重复值，唯一来源改为 `RecommendationPolicy.data_quality.max_stale_ratio_bps`。
- Dashboard 的 200 天查询改为命名 UI preset，并提供 30/90/180/365 日选择；它不是 Runtime Config。
- `MAX_PREVIEW_OCCURRENCES` 保留为 API 防御性上限，不允许 Runtime 配置放大。
- `feedback`、hold-to-resolution 的无效字段、常量和 UI 控件一起删除。

实现时生成并提交一份机器可检查的 leaf inventory，逐项记录：

```text
path / current owner / disposition / new owner / consumer /
apply boundary / validator / secret classification / removal reason
```

CI 将对 Deploy、Runtime、常量和该 inventory 做双向覆盖检查，确保没有遗漏或“配置存在但无人消费”。

---

## 四、Runtime Config 后端闭环与公共接口

### 4.1 类型和持久化

删除：

- `RuntimeConfig`
- `RuntimeConfigVersionId`
- 统一 `runtime_config_version`/approval/activation 模型
- 旧 schema preferences 和巨型 JSON patch API
- 所有旧路径 re-export

新增：

- `ConfigResourceKind`
- `PolicyRevisionId`
- `PolicyApprovalId`
- `PolicyActivationId`
- 六类强类型 policy document
- `DecisionPolicySnapshot`
- `PreparedPolicySnapshot`
- `ProductionBaseline`

每个 report、recommendation、order intent 和 execution admission 都保存 `DecisionPolicySnapshot`，包含真正参与决策的 revision ID、artifact hash 和 effective timestamp。

数据库以 resource kind + revision 建模，revision 内容不可变；修改总是创建新 revision。

### 4.2 API

统一资源式 API：

```text
GET  /api/v1/config/resources
GET  /api/v1/config/{kind}/current
GET  /api/v1/config/{kind}/revisions
POST /api/v1/config/{kind}/drafts
POST /api/v1/config/{kind}/drafts/{id}/validate
POST /api/v1/config/{kind}/drafts/{id}/approve
POST /api/v1/config/{kind}/drafts/{id}/activate
POST /api/v1/config/{kind}/revisions/{id}/rollback
GET  /api/v1/config/deployment
GET  /api/v1/config/activity
GET  /api/v1/config/lifecycle
POST /api/v1/config/lifecycle/seal-production
```

激活请求必须包含：

- `approval_id`
- `expected_active_revision_id`
- `reason`
- `preflight_token`
- `idempotency_key`

后端执行 CAS，拒绝 stale activation。UI 本地类型必须从后端 OpenAPI/DTO 单一来源生成，禁止再次出现前端自行定义 body 导致缺字段但 typecheck 通过的问题。

### 4.3 热更新事务

完整链路：

```mermaid
flowchart LR
  A["编辑 Draft"] --> B["Typed Validation"]
  B --> C["Dependency Preflight"]
  C --> D["Approval"]
  D --> E["DB CAS Activation"]
  E --> F["Prepare Consumer Snapshots"]
  F --> G["Atomic ArcSwap"]
  G --> H["Decision Boundary Freeze"]
  H --> I["Audit + Status Event"]
```

约束：

- validation 和 dependency preflight 在 activation 前完成，参考 [AWS AppConfig validators](https://docs.aws.amazon.com/appconfig/latest/userguide/appconfig-creating-configuration-and-profile-validators.html)。
- 所有消费者先 prepare 成功，再提交数据库 activation 和内存原子切换；任一 prepare 失败都不改变 active revision。
- 不使用自动回滚。失败时保持旧 snapshot；已成功激活后的回滚必须由操作者创建显式 rollback activation。
- 严格按当前单实例 systemd 部署实现，不引入虚假的多副本协调。
- 每个字段必须登记 consumer、validator、preflight、apply handler、effective boundary 和 rollback test；缺任一项编译或架构 lint 失败。
- mode authorization、halt、schedule 和 risk policy 分开锁与事件流，避免一个巨型配置更新造成无关 worker 重建。

权限拆分为：

- `config.view`
- `config.create`
- `config.approve`
- `config.activate`
- `config.rollback`
- `config.lifecycle.seal`

同一操作者可以完成多步流程，但 UI/API 仍强制每一步独立、可审计，不提供“一键创建并激活”。

---

## 五、Config 控制台 UI/UX 设计

### 5.1 视觉方向

采用“安静、精准的控制台”：

- 延续现有 Vben、Ant Design Vue、Iconify、明暗主题和 primary blue，不创造新的视觉体系。
- 中性色作为主要表面；绿色、黄色、红色、processing blue 只表达状态，不作为装饰。
- 页面最大内容宽度约 1280px，使用统一 8px spacing grid 和现有 8px radius。
- 标题 20–24px semibold，section 14–16px semibold，正文 14px，label 13px；ID/hash 才使用 monospace。
- 减少嵌套 Card、每字段边框和重复 Tag；使用 section、divider、留白建立层级。
- 删除正常 section 上持续运行的 `BorderBeam`；焦点使用静态边框、轻微阴影和背景色。
- 原始 JSON 仅作为只读“技术详情”，不再是主要编辑模式。

### 5.2 信息架构

新增 Config 模块路由：

```text
/system/config                    配置总览
/system/config/:resource          策略资源工作区
/system/config/deployment         部署配置只读快照
/system/config/activity           审计活动
/system/config/lifecycle          生命周期与投产封存
```

总览页：

- 顶部紧凑状态条：
  - environment
  - lifecycle
  - active policy bundle
  - pending approval
  - restart required
  - last activation
- 六个资源摘要块，展示 active revision、最近变更、状态、下一生效时间和唯一上下文主操作。
- 下方使用时间线展示最近 draft、approval、activation、rollback。
- 不以大版本表格作为首屏，不显示无业务意义的大 JSON。

策略工作区：

- 桌面端三段式：
  - 左侧：分组导航与完成/错误状态。
  - 中间：字段表单、diff 和说明。
  - 右侧：sticky 影响摘要、验证结果、生效边界。
- 小屏幕变为单列；左侧导航折叠为分段选择，影响摘要进入底部抽屉。
- 字段按“常用 → 高级”组织；高级字段使用 Disclosure，不依赖长 accordion 海洋。
- 每个字段显示业务名称、单位、允许范围、当前值和简短影响说明，不暴露 Rust/JSON path。
- dirty 状态下出现 sticky action bar：变更数量、放弃、保存 Draft；编辑页没有 Activate。
- 校验失败时顶部 Error Summary 可点击跳转字段；字段旁显示具体错误和恢复方式。

### 5.3 不同资源的专用交互

- `RecommendationPolicy`：按选择、数据质量、报告内容分组；数字控件带真实单位和联动范围。
- `ExecutionRiskPolicy`：展示真实账户风险影响摘要；高风险变更需要显式 Review，不使用轻量 toggle。
- `ModelRouting`：按 category 显示 active/shadow/exit slot、artifact hash、兼容状态和 evidence；不允许自由文本输入 model ID。
- `ReportSchedule`：提供下一次运行时间线和 timezone preview；cron 仅在技术详情中展示。
- `OperationalControl`：pause/halt 使用明确动作按钮、状态说明和确认对话框，不使用容易误触的 switch。
- `ExecutionAuthorization`：展示 mode 能力矩阵、credential/preflight 结果和实际资金影响。
- `Deployment`：按 endpoint、identity、resource budget、credential health 分组；值完全脱敏，明确标记“重启后生效”。
- `Lifecycle`：pre-production 页面使用普通 warning；正式封存页使用最强风险层级、证据清单和不可逆确认，不使用装饰动画。

### 5.4 治理流程

使用全页或大尺寸工作区，而不是层层 Drawer：

1. `Edit Draft`
2. `Review & Validate`
3. `Approve`
4. `Activate`

Review 页面提供：

- 字段级 before/after diff。
- 受影响消费者和任务。
- 精确生效边界。
- restart requirement。
- validation/preflight blocker。
- approval metadata。

Activation 页面在提交前再次展示 revision、approval、当前 active revision 和 CAS expectation。成功后回到资源详情并显示 revision 已生效；不会自动跳过结果页。

### 5.5 动效规范

动效只帮助理解状态变化，遵循 Apple “purposeful, brief, precise”原则，并完整支持 `prefers-reduced-motion`；非必要交互动效必须可被禁用，符合 [WCAG Animation from Interactions](https://www.w3.org/WAI/WCAG22/Understanding/animation-from-interactions) 和 [C39](https://www.w3.org/WAI/WCAG22/Techniques/css/C39)。

允许：

| 场景 | 动效 |
|---|---|
| 页面/section 首次进入 | opacity + `translateY(8px)`，160–200ms |
| section 展开/折叠 | height + opacity，160ms |
| dirty/validated 状态变化 | color/opacity，120ms |
| Edit → Review | crossfade，180ms |
| preflight checklist | 图标和颜色逐项淡入，120–160ms，不延迟实际任务 |
| activation success | check icon + 轻微背景 wash，250–320ms，仅一次 |
| hover/focus | border/shadow 120ms，不缩放 |

禁止：

- 无限动画、持续 BorderBeam、animated gradient。
- parallax、bounce、大幅 slide、card scale。
- confetti。
- 对配置数字做 count-up。
- 用动画作为唯一状态提示。
- 动画期间锁住用户操作。
- 因 stagger 导致长列表等待；仅首屏最多 4 项使用不超过 30ms 的微 stagger。

Reduced Motion：

- 去掉 position、height interpolation 和 scale。
- 使用立即切换或不超过 100ms 的 opacity。
- 所有状态同时通过文字、图标和 ARIA live region 表达。
- 复用现有 `usePreferredReducedMotion` 和全局 media query，不另建平行实现。
- Apple 也建议 reduced-motion 下用淡入淡出替代位移、缩放和弹跳，见 [Apple Accessibility](https://developer.apple.com/design/human-interface-guidelines/accessibility/)。

### 5.6 可访问性与反馈

- 目标 WCAG 2.2 AA。
- 完整键盘导航、可见 focus、正确 label/description/error 关联。
- validation result、保存成功、preflight 状态使用 `role=status`/`aria-live`。
- 高风险操作使用具名 Dialog，并将焦点移动到标题；关闭后返回触发点。
- 错误不仅依赖颜色，遵循 [WCAG Error Identification](https://www.w3.org/WAI/WCAG22/Understanding/error-identification.html)。
- 异步状态无需抢占焦点但必须被辅助技术感知，遵循 [WCAG Status Messages](https://www.w3.org/WAI/WCAG22/Understanding/status-messages.html)。
- activation、rollback、production seal 必须提供 Review 和确认，符合 [WCAG Error Prevention](https://www.w3.org/WAI/WCAG22/Understanding/error-prevention-legal-financial-data)。

---

## 六、截图与端到端视觉验收

### 6.1 确定性环境

- Playwright 使用本地 deterministic backend fixture，覆盖完整 Config API 和权限状态。
- 固定字体、locale、timezone、系统时间和 seed 数据。
- mask UUID、hash、时间戳等动态区域。
- 禁止测试依赖真实第三方网络。
- 捕获前确认页面无 loading、无未完成动画、无 console error、无 failed request。
- 当前后端 500 截图不作为新 baseline。

### 6.2 必须截图的业务状态

1. Config Overview healthy。
2. 有 pending approval/restart-required 的 Overview。
3. RecommendationPolicy 默认详情。
4. Draft dirty 编辑状态。
5. inline validation error + Error Summary。
6. Review diff。
7. approval pending。
8. activation preflight。
9. activation success。
10. stale revision/CAS conflict。
11. rollback review 和结果。
12. ModelRouting artifact 选择。
13. ReportSchedule next-run preview。
14. OperationalControl halted。
15. Deployment redacted snapshot。
16. Lifecycle pre-production。
17. Production seal confirmation。
18. Production frozen。
19. 无权限只读状态。
20. API/backend error recovery 状态。

### 6.3 视口矩阵

- 全部核心状态：1440×900 light + dark。
- Overview、编辑、Review、Lifecycle：390×844 light + dark。
- 所有主页面：1024px 宽度 overflow 检查。
- 高内容密度场景：1280×800。
- 视觉基线运行在固定 CI 镜像；动态区域 mask 后 `maxDiffPixelRatio <= 0.001`。
- 生成 before/after contact sheet，将旧页面和新页面同 viewport 并排人工复核。

### 6.4 动效验收

截图只验收稳定状态，动效另外用 Playwright 行为断言：

- 正常模式所有 UI 动画在 350ms 内 settled。
- 页面不存在 infinite animation。
- reduced-motion 下无 position/scale 动画，并在 100ms 内 settled。
- 动效不会改变 focus 顺序或阻塞点击。
- 激活成功、校验错误、preflight 完成均有非动画状态表达。
- Playwright trace/video 作为动画问题辅助证据，不替代截图验收。

### 6.5 通过条件

- 所有 snapshot 通过。
- `axe` 在核心流程无 critical/serious violation。
- 390px 和 1024px 无横向溢出、遮挡、不可达 sticky action。
- light/dark 均满足文字、状态和 focus contrast。
- 用户能够仅用键盘完成 Draft → Review → Approve → Activate。
- 前端请求体与后端 DTO contract test 完全一致。

---

## 七、实施波次与验证

### W0：可机读审计基线

- 生成全部 Runtime、Deploy、常量、版本、migration、phase 文档 inventory。
- 固化每个 leaf 的最终处置、新 owner、消费者和删除原因。
- 增加 architecture lint，防止 inventory 漏项。
- 保存旧 Config 页面同 viewport 截图作为 before evidence。

### W1：Boot 基线

- 压平 PostgreSQL/ClickHouse migration。
- 重置系统自有版本和 package SemVer。
- 重建 fixtures、seed、schema fingerprint、manifest 和测试。
- 删除旧 migration 与版本兼容代码。

### W2：Config 领域模型

- 建立六个 typed runtime resources、revision/approval/activation 表。
- 将 feature/factor/research/training 迁入 immutable profile/job spec。
- 收敛 Deploy Config 和七组 resource budgets。
- 接入 systemd Credentials。
- 删除 no-op 字段和旧 Runtime Config。

### W3：热更新与治理

- 实现 validate/preflight/CAS/prepare/ArcSwap/audit。
- 为六类资源实现明确 effective boundary。
- 实现 manual rollback、权限、lifecycle production seal。
- 修复 activation approval contract。

### W4：Config UI/UX

- 重构路由、Overview、资源工作区和四步治理流程。
- 实现专用 schedule/model/risk/authorization 交互。
- 删除 editable raw JSON 和持续装饰动效。
- 落地统一 motion token、reduced-motion 和 accessibility。

### W5：Phase 文档治理

- 更新全部 phase 文档 lifecycle 声明。
- Phase 11.9 模板优先切换到新规则。
- 加入 lifecycle/version/migration 文档 lint。

### W6：全量验证

后端：

- fresh PostgreSQL/ClickHouse/Redis bootstrap。
- 各 policy parse/validation/property tests。
- 每字段 consumer coverage。
- apply prepare failure、CAS conflict、并发读写、snapshot freeze、manual rollback。
- schedule reconcile、mode preflight、halt 和生产封存不可逆测试。
- 无旧版本、旧 migration、旧 Runtime Config/re-export 的架构扫描。

前端：

- lint、typecheck、unit tests。
- API generated type contract。
- Playwright 全流程、axe、reduced-motion、responsive、visual snapshots。
- 明暗主题和全部截图矩阵人工复核。

仓库质量门：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
bash scripts/lint-architecture.sh
bash scripts/lint-import-style.sh
bash scripts/lint-quant-pivot-boundary.sh
bash scripts/lint-quant-pivot-errors.sh
bash scripts/lint-dead-semantics.sh
cargo test --workspace
pnpm lint
pnpm typecheck
pnpm test
pnpm playwright test
```

## 八、已锁定假设

- 项目及其数据库尚未正式生产运行，不保留任何旧数据升级路径。
- 不提供旧 Runtime Config parser、alias、兼容 DTO 或 re-export。
- 默认运行模式仍为 `ReportOnly`，并继续使用真实账户数据 fail closed。
- 当前部署目标为单实例 systemd，不提前设计多副本配置一致性协议。
- approval 与 activation 可以由同一操作者完成，但必须是两个独立审计动作。
- 激活失败保持旧 revision；不做自动回滚。
- 正式投产封存不可逆；封存后的所有演进必须恢复标准 migration/version discipline。
- UI 完全复用现有设计系统、组件和图标库，不引入新的品牌视觉或自制资产。
- 本轮处于 Plan Mode，尚未修改仓库；模式结束后的第一项执行工作固定为 W0 inventory 与旧页面视觉基线采集，随后按 W1～W6 连续推进。
