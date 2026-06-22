# 07 — 实施 Phase 与验收计划

> 状态：生产级实施计划
>
> 目标：按可验证阶段完成 Endgame arbitrage -> quant-pivot 的破坏式重构。

## 0. 总推进原则

- 先删除旧主语义，再增加新主语义。
- 每个 Phase 必须有退出标准。
- 每个 Phase 必须更新 docs、tests、config、schema。
- 不允许 stub 进入下一 Phase。
- 不允许 compatibility re-export。
- 不允许旧 runtime-config v2 与新 v3 并行长期存在。

## Phase 0 — 命名、边界、删除清算

### 目标

彻底切断 Endgame arbitrage 作为活跃架构的入口。

### 工作项

- 标记旧 phase docs 为 superseded。
- 删除或归档缺失 ADR 引用。
- 更新 `AGENTS.md` 和 `.cursor/rules/*` 的 Endgame 规则。
- 删除 `quant-pivot-algorithm` crate。
- 删除 old `ExecutionMode` 设计引用。
- 删除 old runtime-config sections 的 active docs。
- 新建 quant-pivot docs 为唯一活跃计划。
- 添加 CI gate：禁止 old Endgame symbols 回流。
- **瘦身 `MetricsHub`**：删除 detection / execution / risk / settlement / control-factor 等 Endgame 指标；仅保留 ingest / catalog / hotset / shutdown（hotset **指标名** Phase 2 再 rename）。
- **删除 `quant-pivot-test-support` 旧 materialization fixtures**（本 Phase 收尾项）。
- **磁盘 `crates/oxide-arb-*`** 移入 `archive/` 或删除（非 workspace member）。

### 删除验收

- active src 不再引用 `EndgameDetector`、`OpportunityPipeline`、`ScoredOpportunity`。
- active config hot path 不再读 `detection.endgame`（deploy `engine_subscription_window_hours` 替代）。
- `MetricsHub` 不再注册 Endgame detection/execution/risk/settlement/control-factor 系列。
- 首次 Gamma sync 成功后 `catalog_ready` = 1 且 `CatalogReadiness` 为 `Ready`。
- docs 明确旧 phase 仅用于考古。
- 无 compatibility re-export。

## Phase 1 — Schema 与配置重建

### 目标

建立 quant-pivot 的 Postgres、ClickHouse、runtime-config v3 骨架。

### 工作项

- 新增 quant ID types。
- 新增 `quant_universe_snapshot`、`quant_universe_member`。
- 新增 `quant_feature_vector`。
- 新增 `quant_factor_definition`、`quant_factor_value`。
- 新增 `quant_model_spec`、`quant_model_version`、`quant_model_run`。
- 新增 `quant_recommendation_report`、`quant_recommendation`。
- 新增 `quant_order_intent`、`quant_execution_order`。
- 新增 `quant_recommendation_attribution`。
- 新增 ClickHouse quant facts。
- **删除 Postgres 旧表**（schema graph + migration + entity/idens/repository 全链路）：
  `trade`, `position`, `calibration`, `calibration_outcome`, `resolution_event`,
  `reconciliation_report`, `potential_loss_ledger`, `risk_state`, `risk_audit_event`,
  `risk_fill_applied`, `emergency_snapshot`, `blacklist_entry`, `balance_snapshot`,
  `accounting_period`, `report`, `market_pit_snapshot`。
- **重建 control plane 表**：`control_factor_*` → `quant_factor_*` / `quant_model_*`（含
  `control_factor_audit_event` 若 runtime-config 审计仍需要则保留或迁移表名）。
- runtime-config v3 root。
- deploy config 删除 old execution/settlement hot path。
- **`quant-pivot-error`**：删除 `AlgoError` / `TradingError` / `ReservationError` / `RedeemError`
  等 Endgame error arms（随表与路由删除一并完成）。
- **`quant-pivot-repository`**：删除 trading/risk/calibration/control_factor 旧 repo impl 与
  `pg_repository` 中对应集成测试。

### 验收

- schema graph tests pass。
- migration PG tests pass。
- runtime-config v3 schema snapshot exists。
- old v2 parser deleted。
- production example TOML uses quant sections。

## Phase 2 — Polymarket 数据事实层

### 目标

保留 Polymarket 数据接入，改造成 quant fact plane。

### 工作项

- 保留 Gamma catalog sync。
- 保留 CLOB WS book ingest。
- **重命名** endgame hotset policy → quant universe ingest policy（含
  `hotset_*` Prometheus 系列 → `universe_ingest_*`，`WsSubscriptionCoordinator` 与 deploy 字段命名）。
- **删除** backpressure 中 coalescer site-2 / execution-shard 路径（Phase 0 已删 execution-shard metric）。
- 保留 BookStore published snapshot。
- 新增 fact writers（wire `ChWriteManager` + `book_fact_writer`）。
- 新增 fact lag metrics。
- 新增 data quality report（含 `book_level_rejected` / ingest validation metrics，若需要）。

### 验收

- report_only 模式可启动 ingest。
- 无私钥也能运行数据和报告服务。
- ClickHouse 可写 tick/book/microstructure facts。
- data quality API 可读。
- ingest failure 不触发旧 trading halt。

## Phase 3 — Feature、Factor、Model 平面

### 目标

建立可回放、可训练、可发布的研究平面。

### 工作项

- Universe selector。
- Feature schema registry。
- 通用 feature builders。
- 通用 factor definitions。
- Weighted factor model。
- Model registry。
- Model run worker。
- PIT dataset builder。
- Backtest runner。
- Quality gates。
- Shadow model output。

### 验收

- 能对一个 universe 生成 feature vectors。
- 能生成 factor values。
- 能跑 model run。
- 能生成 signal candidates。
- PIT tests 防止未来数据泄漏。
- quality gate fail 时模型不能 publish。

## Phase 4 — TopN 报告系统

### 目标

让 `RecommendationReport` 成为系统主产物。

### 工作项

- Report scheduler。
- Portfolio planner。
- TopN selector。
- Recommendation builder。
- Report persistence。
- Report API。
- Report WebSocket events。
- Report notification。
- Empty report semantics。
- Report diff。
- Report revoke。

### 验收

- 可定时生成 TopN report。
- 报告包含 entry/sizing/exit/risk/evidence。
- 空报告有原因。
- report payload snapshot tests pass。
- Web/API 可读取 latest report。
- 不创建 order intent。

## Phase 5 — Semi-auto 执行

### 目标

支持人工审批后执行 recommendation。

### 工作项

- `QuantRuntimeMode::SemiAuto`。
- OrderIntent 创建。
- Approval API。
- Approval invalidation。
- Execution admission engine。
- Polymarket order client integration。
- Entry order lifecycle。
- Exit monitor。
- Execution events。
- Attribution。

### 验收

- report_only 无法创建 intent。
- semi_auto 可创建 pending approval。
- 未审批不能下单。
- 审批过期不能下单。
- admission gate deny 有 trace。
- filled order 会注册 exit monitor。
- attribution 写入训练反馈。

## Phase 6 — Auto-execution

### 目标

在严格风控下支持自动执行。

### 工作项

- `QuantRuntimeMode::AutoExecution`。
- auto policy config。
- mode transition preflight。
- shadow period enforcement。
- auto-created approved intent。
- kill switch。
- capital allocation。
- reconciliation blocking。
- auto downgrade to report_only。

### 验收

- 不能从 report_only 直接进 auto_execution。
- model 未 published 不能 auto。
- quality gate stale 不能 auto。
- kill switch open 不能 auto。
- unresolvable reconciliation block auto。
- 自动降级可用。

## Phase 7 — Web、UI、运维

### 目标

完成 operator 体验和生产操作闭环。

### 工作项

- Quant reports 页面。
- Recommendation detail 页面。
- Factor/model registry 页面。
- Runtime config v3 UI。
- Order intent approval 页面。
- Execution monitor 页面。
- Operation log 扩展。
- New runbooks。
- Docker compose。
- CI gates。
- Bench gates。

### 验收

- UI 不出现旧 opportunity/trade/endgame 页面。
- runtime config UI 不出现 old groups。
- 操作员能完成 semi_auto 审批。
- runbook 覆盖 report_only -> semi_auto -> auto_execution。
- CI 阻止旧符号回流。

## Phase 8 — 生产验证

### 目标

按风险递增验证生产可用性。

### 阶段

1. Historical backtest。
2. Live report_only shadow。
3. Live report_only published report。
4. Semi-auto with tiny budget。
5. Semi-auto normal budget。
6. Auto execution with tiny budget。
7. Auto execution limited rollout。

### 指标

- report SLA。
- data quality coverage。
- feature missing rate。
- model score stability。
- TopN overlap stability。
- recommendation hit rate。
- entry slippage。
- exit compliance。
- realized vs expected return。
- max drawdown。
- unresolvable execution count。

### 退出标准

- 连续 N 天 report SLA 达标。
- 无 critical data quality incident。
- shadow/live diff 在阈值内。
- semi_auto 执行无 unresolvable。
- exit monitor 无漏监控。
- operator runbook 演练通过。

## 测试矩阵

| 层 | 测试 |
|---|---|
| models | ID、money、schema、DTO、runtime-config v3 |
| repository | PG CRUD、pagination、status transition |
| ClickHouse | row serialization、batch insert、query ordering |
| data | BookStore、Gamma sync、fact writer |
| feature | feature null policy、PIT visibility |
| factor | normalization、confidence、explanation |
| model | scoring、quality gates、shadow diff |
| report | payload snapshot、empty report、diff、revoke |
| portfolio | budget constraints、TopN stability |
| execution | intent state machine、approval、admission |
| risk | envelope hash、kill switch、capital allocation |
| web | auth/RBAC、runtime-config、report APIs |
| e2e | ingest -> report -> intent -> approval -> fill -> exit -> attribution |

## Benchmark 矩阵

- feature build per market。
- factor compute per market。
- model score per market。
- universe scan 1k markets。
- TopN select 1k candidates。
- report build TopN 50。
- admission decision。
- exit monitor tick。
- ClickHouse fact encode。

## 最终完成定义

重构完成时必须满足：

- active code 没有旧 Endgame 主路径。
- active docs 以 quant-pivot 为唯一架构。
- runtime-config v3 是唯一 schema。
- report_only 可无私钥生产运行。
- semi_auto 可人工审批真实执行。
- auto_execution 可受限灰度并可快速降级。
- 每条 recommendation 可审计、可回放、可归因。
- 删除、合并、保留清单全部执行完毕。

## 详细 Phase Contract

以下是每个 Phase 的实施契约。任何 Phase 未满足契约，不允许进入下一 Phase。

## Phase 0 详细契约 — 清空旧主语义

### 必删代码入口

- `crates/quant-pivot-algorithm`
- `core/detection`
- `core/execution` old path
- `core/post_trade`
- `risk` active pre-trade pipeline
- old runtime config modules

### 必加 CI gate

```bash
rg "Endgame|ScoredOpportunity|OpportunityPipeline|ExecutionMode::DryRun|ExecutionMode::Paper|ExecutionMode::Live" crates && exit 1
rg "pub use .*Opportunity|pub use .*Trade" crates && exit 1
```

### 输出

- active docs 指向 `docs/plans/quant-pivot`。
- old docs 标记 superseded。
- workspace 不再构建 deleted crate。

### Blocker

如果任何新代码仍以 `Opportunity` 为主对象，本 Phase 失败。

## Phase 1 详细契约 — Schema First

### 必建模块

```text
models/src/domain/quant/
models/src/idens/quant_*.rs
models/src/entities/quant_*.rs
repository/src/traits/quant/
repository/src/postgres/quant/
models/src/clickhouse/quant_*.rs
```

### 必实现 trait

```rust
pub trait RecommendationReportRepository { /* create/latest/revoke */ }
pub trait ModelRegistryRepository { /* create/publish/retire */ }
pub trait QuantFactRepository { /* insert/query CH facts */ }
pub trait OrderIntentRepository { /* create/approve/transition */ }
```

### 验收测试

- schema graph includes all quant tables。
- migration creates all indexes。
- repository create/list/revoke report。
- runtime config v3 parse rejects v2。

### Blocker

如果旧 `trade` 表仍在 active schema graph，本 Phase 失败。

## Phase 2 详细契约 — Data Plane

### 必建模块

```text
core/src/data/
├── polymarket_ingest.rs
├── fact_writer.rs
├── book_store.rs
├── data_quality.rs
└── market_catalog.rs
```

### 必实现 trait

```rust
pub trait PointInTimeDataSource { /* market_context/book_snapshot */ }
pub trait FactWriter<T> { async fn write_batch(&self, rows: Vec<T>) -> QuantResult<()>; }
pub trait DataQualityService { fn classify(&self, input: DataQualityInput) -> DataQualityReport; }
```

### 伪代码入口

```rust
run_ingest_loop -> normalize event -> apply BookStore -> enqueue facts -> update data quality
```

### 验收测试

- WS event updates published book。
- fact writer batches and flushes。
- stale book marks degraded quality。
- report_only starts without keys。

### Blocker

如果 book update 仍触发 scanner/execution，本 Phase 失败。

## Phase 3 详细契约 — Research Plane

### 必建模块

```text
research/src/universe
research/src/features
research/src/factors
research/src/model
research/src/training
research/src/backtest
research/src/gates
research/src/governance
```

### 必实现 trait

```rust
UniverseSelector
FeatureBuilder
FactorComputer
ModelRunner
TrainingDatasetPlanner
TrainingDatasetBuilder
Labeler
ModelTrainer
Backtester
ModelQualityGate
```

### 第三方 crate 引入

Phase 3 第一批允许：

- `polars`
- `ndarray`
- `ndarray-stats`
- `statrs`
- `argmin`
- `rayon`

Phase 3 第二批允许二选一：

- `smartcore`
- `linfa`

不得在 Phase 3 引入：

- `burn`
- `candle`
- `ort`
- heavy LP native solver

### 伪代码入口

```rust
build_dataset -> train_model -> run_backtest -> evaluate_gates -> create_candidate_model
```

### 验收测试

- PIT leakage detector catches future feature。
- label maturity delays unresolved labels。
- weighted model artifact hash stable。
- quality gate blocks low coverage。

### Blocker

如果 training builder 读取 live `BookStore` 而不是 PIT source，本 Phase 失败。

## Phase 4 详细契约 — Report Plane

### 必建模块

```text
core/src/report/
├── scheduler.rs
├── builder.rs
├── composer.rs
├── publisher.rs
├── lifecycle.rs
└── diff.rs
```

### 必实现 trait

```rust
ReportBuilder
RecommendationComposer
ReportPublisher
ReportLifecycleService
PortfolioPlanner
```

### 第三方 crate 引入

允许：

- `tokio-cron-scheduler`，仅当现有 `PeriodicTask` 无法表达 cron/report schedule。

不允许：

- ONNX/DL 依赖进入 report builder。

### 伪代码入口

```rust
schedule_tick -> build universe -> features -> factors -> model -> portfolio -> report -> publish
```

### 验收测试

- non-empty report snapshot。
- empty report snapshot。
- stable TopN sorting。
- report revoke writes operation log。
- latest report API returns published report。

### Blocker

如果 report generation 创建 order intent，本 Phase 失败。

## Phase 5 详细契约 — Semi-auto

### 必建模块

```text
core/src/execution_intent/
├── mode_gate.rs
├── intent_service.rs
├── approval.rs
├── admission.rs
├── dispatcher.rs
├── exit_monitor.rs
└── attribution.rs
```

### 必实现 trait

```rust
RuntimeModeGate
OrderIntentService
ExecutionAdmissionEngine
ExitMonitor
AttributionService
```

### 第三方 crate 引入

允许：

- `good_lp` spike 通过后用于 portfolio allocation。

要求：

- 先保留 deterministic greedy planner。
- `good_lp` 只能通过 `PortfolioOptimizer` trait 使用。

### 伪代码入口

```rust
recommendation -> create pending intent -> approve -> admission -> submit entry -> monitor exit -> attribution
```

### 验收测试

- report_only cannot create intent。
- pending approval cannot submit。
- approved expired intent cannot submit。
- admission trace persists denial。
- exit monitor registers on fill。

### Blocker

如果 handler 直接调用 Polymarket order client，本 Phase 失败。

## Phase 6 详细契约 — Auto Execution

### 必建模块

```text
core/src/auto_execution/
├── policy.rs
├── preflight.rs
├── downgrade.rs
└── watchdog.rs
```

### 必实现接口

```rust
pub trait AutoExecutionPolicy {
    fn evaluate(&self, recommendation: &Recommendation) -> AutoExecutionDecision;
}

pub trait ModeTransitionService {
    async fn transition(&self, request: ModeTransitionRequest) -> QuantResult<QuantRuntimeMode>;
}
```

### 第三方 crate 引入

允许评估：

- `ort`，仅用于 ONNX inference。

前置：

- MSRV 决策完成。
- Docker/native dependency spike 通过。
- `OnnxInferenceEngine` trait 已隔离。

禁止：

- auto execution 直接依赖 `ort` concrete session。

### 验收测试

- cannot transition report_only -> auto_execution。
- stale quality gate denies auto。
- kill switch forces report_only。
- unresolvable reconciliation blocks policy。

### Blocker

如果 auto execution 绕过 `OrderIntent`，本 Phase 失败。

## Phase 7 详细契约 — Web/Ops

### 必建 API

- `/api/quant/reports`
- `/api/quant/recommendations`
- `/api/quant/models`
- `/api/quant/factors`
- `/api/quant/intents`
- `/api/system/quant-mode`
- `/api/system/kill-switch`

### 必删 API

- `/api/opportunities`
- `/api/trades`
- `/api/positions` old
- `/api/pnl` old
- `/api/replay` old

### 验收测试

- RBAC denies unauthorized approval。
- runtime config v3 UI schema generated。
- WebSocket emits report events。
- old routes absent from route registry。

### Blocker

如果 UI/API 仍展示 Endgame opportunity/trade 主菜单，本 Phase 失败。

## Phase 8 详细契约 — Production Rollout

### Rollout Gates

```text
backtest green
-> report_only shadow green
-> report_only published green
-> semi_auto tiny budget green
-> semi_auto normal budget green
-> auto tiny budget green
-> auto limited rollout
```

### 每阶段必须记录

- runtime config version。
- model version。
- report ids。
- incidents。
- operator approvals。
- realized vs expected metrics。

### Kill Criteria

任一条件触发立即降级：

- fact lag critical。
- report SLA 连续 miss。
- unresolvable execution。
- exit monitor missed event。
- realized drawdown breach。
- model artifact hash mismatch。

### 后续深度学习评估

只有 Phase 8 生产验证稳定后，才允许评估：

- `burn`：Rust-native deep learning training。
- `candle`：轻量推理、safetensors、Hugging Face 模型。

评估必须独立成 spike，不得影响已发布 weighted/classical model 主路径。
