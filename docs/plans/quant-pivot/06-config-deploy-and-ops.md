# 06 — Config、Deploy 与 Lifecycle 治理

<!-- quant-pivot-deployment-contract:v1 -->
> **Deployment contract**
> - `fresh_boot_assumption`: 项目尚未正式生产上线，将从全新 `boot` / schema version `1` 部署；仓库和数据库不保存 lifecycle seal 状态。
> - `schema_data_version_impact`: 本文中的历史版本号与递增路径不再具有实施效力；当前实现不迁移测试数据、旧结构或旧版本。
> - `pre_deployment_behavior`: 允许 clean-break、migration squash 与全新基础设施 bootstrap，但任何数据销毁仍需操作者单独授权。
> - `post_deployment_behavior`: 首次部署后使用正常前向 migration、回滚与数据验证；不使用不可逆 production seal 或兼容桥。
> - `rollback_and_data_verification`: 首次部署前通过清空后的 fresh-install 验证；部署后使用备份、前向 migration 与显式回滚。

> 状态：boot baseline 的权威目标设计。旧 `RuntimeConfig` 大文档、旧版本表、旧 JSON 编辑器和兼容解析路径均已被本设计取代。

## 0. 权威来源与不变量

- 生命周期源码：[`project-lifecycle.toml`](../../../project-lifecycle.toml)。
- Deploy 类型：[`crates/quant-pivot-models/src/config/`](../../../crates/quant-pivot-models/src/config/mod.rs)。
- 六类 policy 类型：[`crates/quant-pivot-models/src/runtime_config/`](../../../crates/quant-pivot-models/src/runtime_config/mod.rs)。
- Deploy 结构与默认值：[`crates/quant-pivot-models/src/config/`](../../../crates/quant-pivot-models/src/config/mod.rs)；所有 section 使用 typed `Deserialize` 与 `deny_unknown_fields`。
- Deploy 语义校验：[`crates/quant-pivot-models/src/config/validation.rs`](../../../crates/quant-pivot-models/src/config/validation.rs)。
- Runtime Config API contract：[`schema/api/config-v1.schema.json`](../../../schema/api/config-v1.schema.json)，由 Rust DTO 生成并与前端 TypeScript decoder 双向检查。
- 持久化边界：runtime entity/DTO 的 Rust 类型、fresh-boot schema manifest、repository system contracts 与 `cargo xtask architecture check`；不维护逐字段影子清单。

系统只允许以下四种配置所有权：

1. **Runtime Policy**：必须热更新、必须审计、会改变后续业务决策的参数。
2. **Immutable Artifact / Job Spec**：会改变研究、训练、回放或 lineage 的方法定义。
3. **Deploy Config**：进程构造、外部绑定、主机容量与 secret；重启后生效。
4. **Code Constant**：协议事实、数学不变量和防御性上限，不允许操作员修改。

一个字段不能同时属于两类；没有 validator、consumer、apply boundary 或 rollback test 的字段不得成为 Runtime Policy。

## 1. 旧 Runtime Config 处置清单

| 旧顶层字段 | 最终处置 | 新 owner | 原因 |
|---|---|---|---|
| `schema_version` | 删除统一大文档版本 | 各 policy/artifact 独立 `schema_version = 1` | 系统不再存在一个可整体替换的配置文档 |
| `selection` | 合并 | `RecommendationPolicy.selection` | 决定 report eligibility，随新 `ReportRun` 冻结 |
| `data_quality` | 合并 | `RecommendationPolicy.data_quality` | 是推荐准入语义，不是基础设施参数 |
| `features` | 迁移 | `FeatureProfileArtifact` | 特征定义影响训练、服务和回放，必须内容寻址 |
| `factors` | 迁移 | `ScoringProfileArtifact` | 因子构造、归一化和研究权重必须可复现 |
| `domain` | 拆分 | provider binding → Deploy；语义 → `DomainProfileArtifact` | 外部连接与研究语义具有不同变更边界 |
| `model` | 收敛 | `ModelRouting` | Runtime 只治理 active/shadow/exit artifact 指针 |
| `quality_gate` | 迁移 | `ResearchMethodProfileArtifact.model_promotion` | promotion workflow 完整落地前不得保留 no-op runtime gate |
| `training` | 迁移 | `ResearchMethodProfileArtifact.training` / job spec | 训练输入在 enqueue 时冻结，不应全局热改 |
| `reports` | 拆分 | cadence/timezone → `ReportSchedule`；内容语义 → `RecommendationPolicy` | 调度 reconcile 与报告内容的生效边界不同 |
| `portfolio` | 合并 | `ExecutionRiskPolicy` | 使用真实账户进行 sizing，属于资金风险策略 |
| `execution` | 拆分 | risk → `ExecutionRiskPolicy`；mode authority → `ExecutionAuthorization`；pause/halt → `OperationalControl` | 风险、授权与即时操作不能共享一次 activation |
| `notification` | 拆分 | secret/endpoint → Deploy credential/binding；event routing → `OperationalControl` | secret 不进入 Runtime Policy |
| `research` | 迁移 | `ResearchMethodProfileArtifact` / job spec | CPCV、PBO、purge、trial 等是可复现方法 |
| `feedback` | 删除 | 无 | 旧字段没有完整消费者；能力落地后再新增独立资源 |
| `hold_to_resolution*` | 删除 | 无 | 旧路径为 no-op，不能伪装成治理能力 |

Deploy Config 以 Rust typed tree、`deny_unknown_fields`、mode-aware semantic validation 与实际 adapter constructor 为唯一事实源；不维护逐叶 TSV 或基于路径前缀推导 ownership 的第二份清单。Runtime Policy wire contract 由 Rust JSON Schema 生成并与前端 decoder 双向检查；持久化语义字段继续由独立 TOML 决策表与 AST 审计器治理。

## 2. 六类 Runtime Policy

| Resource | 内容 | Consumer | 原子生效边界 |
|---|---|---|---|
| `RecommendationPolicy` | selection、data quality、TopN、horizon、report TTL | market selector、data-quality gate、report coordinator | 新 claim 的 `ReportRun`；已 claim run 继续使用旧 snapshot |
| `ExecutionRiskPolicy` | sizing、exposure、entry/exit、capital、reconciliation、breaker | planner、`OrderIntent` builder、admission、execution workers | 新 `OrderIntent` 或新 admission；已提交订单不被隐式改写 |
| `ModelRouting` | category 对应 active/shadow/exit artifact ref | model router、category pointer guard | 新 report / evaluation run |
| `ReportSchedule` | timezone、calendar、cadence、enabled | durable report scheduler | 立即 reconcile 未 claim 的 future runs；已 claim run 不变 |
| `OperationalControl` | report pause、execution halt、notification routing、worker admission | admission gates、notification router、worker supervisors | 下一次 admission 原子读取；不自动撤销已签名或已提交订单 |
| `ExecutionAuthorization` | `ReportOnly` / `SemiAuto` / `AutoExecution` 权限与约束 | runtime-mode gate、execution preflight | mode preflight 成功后的后续 admission |

每个 resource 都是强类型 `PolicyDocument` enum variant；API、repository 与数据库 `resource_kind` 使用 SeaORM `ActiveEnum`，不得以自由字符串分派。Decimal、ID、hash、URI、timezone、cadence 与 mode 均使用经过验证的 newtype/enum。

### 2.1 Immutable profile artifacts

`DecisionPolicySnapshot` 同时冻结六类 revision identity 与以下不可变方法 artifact：

- `FeatureProfileArtifact`
- `ScoringProfileArtifact`
- `DomainProfileArtifact`
- `ResearchMethodProfileArtifact`

每个 artifact 独立使用 boot schema，并可计算 `ContentHash`。修改 policy resource 不得改写这些 artifacts；研究/训练流程必须把实际消费的 artifact/hash 写入 lineage。

## 3. Revision、Approval、Activation 与回滚

编辑总是创建 immutable revision，不允许原地更新：

```text
Draft -> Typed Validation -> Dependency Preflight -> Approval
      -> Consumer Prepare -> DB CAS Activation -> ArcSwap Publish -> Audit
```

Activation request 必须携带：

- `approval_id`
- `expected_active_revision_id`
- `reason`
- `preflight_token`
- `idempotency_key`

约束：

- approval 绑定 exact `resource_kind + revision_id + revision_hash`。
- preflight token 有明确过期时间，且只适用于该 revision。
- repository 通过 activation guard row 加锁，并在同一事务中校验 idempotency、evidence、CAS、snapshot 与 activation。
- active resource 通过一条 `DISTINCT ON (resource_kind)` 查询批量读取，禁止逐 resource N+1。
- snapshot insert 使用 SeaORM `TryInsert` 与 typed conflict target；正常路径一次往返，冲突时才补查并核对内容。
- consumer prepare 任一失败时不写 active state；已成功 activation 的反向操作只能创建显式 rollback activation。
- rollback 仍需要 review、approval、preflight、CAS 与 reason；不存在一键自动回滚。

权限分别为 `config.view/create/approve/activate/rollback`；lifecycle seal 使用独立 `config_lifecycle.seal`。

## 4. Deploy Config

Deploy Config 只保留启动时才能决定的内容：

- service endpoint、bind address、allowed origin、deployment identity；
- PostgreSQL、ClickHouse、Redis 与 artifact store 的连接位置；
- Gamma/CLOB/Data API、on-chain、relayer 与 domain provider binding；
- wallet kind、funder 与 secret；
- TLS、JWT issuer/audience、日志格式/级别；
- lifecycle expectation 与 production build identity；
- 七组 host resource budgets。

七组 resource budget：

1. `database`
2. `clickhouse_writer`
3. `market_data_ingest`
4. `cache`
5. `research_jobs`
6. `report_execution`
7. `web`

worker 的 lease、heartbeat、poll、queue 与 concurrency 只能由对应 budget/typed deploy section 声明并校验合法比例；不得用多个互相独立的 magic duration 形成矛盾状态。

### 4.1 Secret

private key、DB password、JWT signing key、bot token、relayer key、webhook secret 和 evidence signing key 在 TOML 中使用非空明文 string，反序列化后统一由 `SecretText` 持有。`SecretText` 使用 zeroizing storage，`Debug` 固定脱敏，且不实现 `Serialize` 或 `Display`；调用方只能在外部 client 构造边界显式借用其值。

tracked `quant-pivot.toml` 只允许空值，tracked production example 只允许显式 `REPLACE_WITH_*` 占位符。local-development 的真实值写入 gitignored `quant-pivot.local.toml`；preproduction/production 使用权限 `0600`、不进入版本控制的 deploy TOML。secret 不允许通过环境变量或进程参数传入；任何 `Debug`、错误、日志和 deployment API 只允许返回 `available/missing/invalid`，不得返回值。

PostgreSQL 与 ClickHouse 各自只有一组 `user + password`，runtime、schema CLI 与 Fresh Boot 复用同一身份。应用启动路径只执行 schema verify；migration/reset/seal mutation 只能由显式 CLI/治理入口执行，并统一持有 canonical PostgreSQL lifecycle lease。这里不维护第二组 migrator credential，也不提供 identity fallback。

### 4.2 来源优先级

1. source-controlled non-secret `quant-pivot.toml`；
2. gitignored `quant-pivot.local.toml`（仅 exact local-development 可含明文 secret）；
3. 禁止任意环境变量覆盖 Deploy Config、secret 或业务 policy。

## 5. SeaORM / PostgreSQL 规则

- 表、列、FK、index 与 native enum 以 boot snapshot entities 为源，通过 SeaORM entity-first `SchemaBuilder::apply` 建表。
- 有限状态、resource kind、actor kind、lifecycle state 与 operation 使用 `DeriveActiveEnum`；ID、hash、URI、金额和配置 JSON 使用 newtype/`FromJsonQueryResult` typed struct。
- 业务查询优先 `Entity::find`、typed `Column`、`Condition`、`Relation`、partial model 与 `find_also_related`；禁止手拼列名和 enum literal。
- 一对一读取使用 join；一对多在结果规模较小时使用 eager join，在父行会大量重复时使用 Model/Entity Loader 的批量 `IN` 查询。禁止循环内 lazy load。
- 只读取 UI 摘要所需列时使用 partial model，避免为列表页传输完整 JSONB document。
- 多行写使用 `insert_many`/batch upsert；状态转换与审计写入同一 transaction。
- raw SQL 仅允许用于 SeaQuery 无法表示的 PostgreSQL ordered-set aggregate、catalog introspection、DDL gap 与健康探针，并集中在受 lint 约束的 adapter/support 文件。
- unprepared SQL 不得接受外部输入；动态值必须绑定。

## 6. Fresh Boot 与 schema mutation

系统没有 `project-lifecycle.toml`、production seal、`system_production_baseline` 或手工
`BootstrapPhase::Active`。首次部署从 reviewed boot-v1 schema 与 seed 开始；应用启动只读验证 PostgreSQL /
ClickHouse semantic fingerprint，不自动执行 DDL。

schema mutation 和明确授权的 pre-production reset 共用单一 `SchemaMutationLease`，防止 PostgreSQL 与
ClickHouse deploy tooling 并发改写。首次部署后直接使用正常 forward migration、备份、回滚与数据验证；
资金和订单能力由 runtime control 与业务 admission 独立决定。

## 7. Config 控制台

路由：

```text
/system/config
/system/config/:resource
/system/config/deployment
/system/config/activity
/system/config/lifecycle
```

- Overview 展示 environment、lifecycle、active bundle、pending approval、restart required 与最近 activation。
- Resource workspace 使用 typed field editor、before/after diff、consumer impact、validator/preflight 与 exact apply boundary；raw JSON 只读。
- `Edit Draft -> Review & Validate -> Approve -> Activate` 为四个独立审计动作；编辑页不得直接 Activate。
- `OperationalControl` 使用具名动作按钮和确认 Dialog，不使用易误触 switch。
- `ModelRouting` 只能选择经过兼容性检查的 artifact，不接受自由文本 model ID。
- `ReportSchedule` 展示 timezone 与 next-run preview，cron 只放技术详情。
- Deployment 完全脱敏并明确标注 restart boundary。
- Lifecycle seal 展示证据清单、精确确认短语与不可逆结果。

## 8. 验收

- 空 PostgreSQL 只应用 `m00000000_000001_bootstrap`；空 ClickHouse 只应用 `version = 1` bootstrap。
- 旧 migration、旧 RuntimeConfig entity/parser/API、旧版本 alias 与 compatibility re-export 均不存在。
- 六类 policy 的每个 leaf 都有 typed validator、consumer、prepare/apply handler、effective boundary 与 rollback test。
- Config DTO 从后端契约生成，activation body 不可能漏掉 approval/CAS/preflight/idempotency 字段。
- deterministic Playwright fixture 覆盖 Overview、edit、validation error、review、approval、preflight、success、CAS conflict、rollback、deployment、lifecycle、permission 与 error recovery。
- desktop/mobile、light/dark、reduced-motion、axe 与 keyboard-only flow 全部通过；截图只在动画 settle、无 console error、无 failed request 后采集。
- `production_frozen` 后 boot reset、migration squash 与版本重新基线在 API、CLI、启动校验和文档 lint 四处 fail closed。
