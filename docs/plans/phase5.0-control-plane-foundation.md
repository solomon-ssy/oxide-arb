# Phase 5.0 — Control Plane Foundation & Architecture Contract

> **状态**: Production Design Target  
> **父计划**: `docs/plans/phase5-replay-analytics.md`  
> **覆盖原章节**: 0, 1, 3, 4, 8.2, 8.5, 9.4, 20  
> **目标**: 先冻结 Phase 5 的产品边界、领域不变量、crate 边界、typed artifact 模型、破坏式重构原则和全阶段防漂移规则。

---

## 0. 工作范围

Phase 5 不是用户可见的 replay 产品，也不是报表系统。它是 **Control Factor Materialization & Governance Plane**：

```text
Live Facts
  -> Point-in-Time Evidence
  -> Typed Control Factors
  -> Quality Gates
  -> Registry / Shadow / Publication / Rollback
  -> ArcSwap<ControlFactorSnapshot>
  -> Detector / Scorer / Risk / Sizer
```

### 0.1 本子阶段交付物

| 交付物 | 要求 |
|---|---|
| Phase 5 架构契约 | 明确 data plane、control plane、hot path 边界 |
| Crate/module ownership | 明确 `oxide-arb-control`、`oxide-arb-core`、`oxide-arb-repository`、`oxide-arb-models` 的职责 |
| Typed artifact model | 定义 `ControlFactorValue`、`FactorEvidence`、`ControlFactorPublication`、status/mode 枚举 |
| Publication-first 语义 | live 只消费 active publication，不消费任意 factor row |
| Breaking-change policy | 删除旧 replay 草案、旧 alias、旧 planned table name，不保留 re-export |
| 子阶段覆盖矩阵 | 将原 Phase 5 所有章节映射到 `phase5.0`-`phase5.8` |

### 0.2 非目标

- 不实现 materialization runner。
- 不实现 evidence stage。
- 不实现 API/UI/Scheduler。
- 不接入 live hot path。
- 不引入用户可见 `ReplayMode`、`DetectorOnly`、`Execution`、`PortfolioRisk`、`Diagnostic` 等产品模式。
- 不把复杂 factor payload 塞回 runtime config document；旧 mutable key-value `runtime_config` 不再作为 Phase 5 运行时配置事实源。
- 不设计吞掉所有语义的“总控因子”。

---

## 1. 不变量

### 1.1 领域不变量

1. `MarketId` 必须表示 Polymarket `condition_id`，`TokenId` 必须表示 CLOB token id。任何 market-level factor 都必须通过 PIT market mapping 解析 YES/NO token pair。
2. Money、price、shares 的业务计算必须使用 `Decimal` 或现有 newtypes：`Usd`、`Shares`、`Price`、`MicroUsd`、`MicroShares`、`MicroPrice`。CH row 可以为了压缩使用 primitive，但 evidence builder 和 factor builder 不得裸用 `f64` 表达业务不变量。
3. Endgame 当前是 settlement directional bet，默认 hold-to-resolution。主动 exit/stop-loss 是单独产品决策，不能隐含在 control factor 中。
4. 自动控制因子默认只能收紧风险。所有 multiplier 自动生成边界必须是 `0..=1`。降低 edge、放大 budget、放大 Kelly、提高 max positions 必须人工审批、短 TTL、可回滚、可审计。
5. 缺 evidence 的控制因子等同于未治理 runtime config version 变更，禁止进入 `Candidate` 或 `Published`。

### 1.2 Data Plane / Control Plane / Hot Path

| 层 | 角色 | 生产约束 |
|---|---|---|
| ClickHouse facts | 高容量事实与 evidence 查询输入 | append-only / replacing 型，不作为 live 决策系统 |
| Postgres trading state | trade、position、risk、settlement、reconciliation 权威 | materialization 做 PIT join；hot path 不每笔查询 |
| Postgres control registry | factor、publication、audit、runtime config version / activation 权威 | 所有状态转换可审计、可回滚 |
| In-memory snapshot | live `ControlFactorSnapshot` | `ArcSwap` 原子替换，hot path 只读 |

任何 live hot path 同步查询 ClickHouse/Postgres 都是 Phase 5 架构违规。

### 1.3 Point-in-Time 正确性

Materialization 不允许用当前状态解释过去。以下输入必须按事件时间恢复：

- market/event metadata；
- YES/NO token mapping；
- fee schedule；
- calibration snapshot；
- runtime config version；
- risk state / accounting snapshot；
- balance and token balances；
- settlement truth and reconciliation status。

默认窗口：

```text
[trigger_time - source_delay - interval, trigger_time - source_delay)
```

`source_delay` 是事实库 eventual consistency 的设计机制，不能用隐藏 sleep/retry 代替。

### 1.4 失败语义

| 场景 | 结果 |
|---|---|
| 缺 L2/book snapshot，但目标 factor 依赖 execution evidence | stage `InsufficientCoverage`，不生成生产级 factor |
| 缺 settlement truth，但目标 factor 依赖 outcome | 保持 `ReportOnly`/`Draft`，不能 Candidate |
| 缺 PIT calibration | 不能生成 `BucketRiskFactor` Candidate |
| factor payload schema mismatch | 拒绝 publication 或 live snapshot load |
| safety factor expired | 推荐 reconciliation / critical anomaly fail closed |
| non-safety factor expired | fail neutral，剔除该 factor |
| materialization run partial failed | 写完整 run report，不推进 affected factors |

---

## 2. Crate 与模块边界

### 2.1 推荐结构

```text
crates/
├── oxide-arb-models/
│   ├── domain/control_factor/
│   ├── enums/control_factor.rs
│   ├── idens/control_factor_*.rs
│   └── clickhouse/...
├── oxide-arb-control/
│   ├── materialization/
│   ├── evidence/
│   ├── factor/
│   ├── gates/
│   ├── governance/
│   └── report/
├── oxide-arb-core/
│   ├── control/factor_refresher.rs
│   ├── control/factor_snapshot.rs
│   ├── control/factor_shadow.rs
│   └── observability/fact_writers.rs
├── oxide-arb-repository/
│   ├── traits/control_factor.rs
│   ├── traits/evidence_timeseries.rs
│   ├── postgres/control_factor.rs
│   └── clickhouse/timeseries.rs
└── oxide-arb-web/
    └── routes/control_factors.rs
```

### 2.2 Ownership

| Crate | Owns | Must Not Own |
|---|---|---|
| `oxide-arb-control` | offline materialization、evidence stage、factor builder、quality gate、governance state transitions | core hot path internals |
| `oxide-arb-core` | live fact writers、live factor snapshot refresh、shadow decision writer、hot path consumer wiring | materialization engine |
| `oxide-arb-repository` | typed query/write contracts for CH/PG | scattered raw SQL clients in control logic |
| `oxide-arb-models` | enums、idens、entities、domain types、CH row types | behavior-heavy orchestration |
| `oxide-arb-web` | operator API surface | factor decision logic |

`oxide-arb-control` 可以依赖 `models`、`algorithm`、`risk`、`repository`、`error`，但不能反向依赖 `core`。

### 2.3 为什么不能放进 runtime config

Phase 5 不保留旧 mutable key-value `runtime_config` 作为事实源。运行时配置必须是 immutable `runtime_config_version` document，并通过 append-only `runtime_config_activation` 生效。

Runtime config version 是 operator baseline，不是 evidence artifact。Control factors 需要：

- typed dimensions and payloads；
- evidence and source run lineage；
- TTL and freshness policy；
- Draft/Candidate/Shadow/Published lifecycle；
- publication versioning；
- rollback target；
- immutable audit trail；
- shadow decision deltas。

把这些塞进 runtime config document 会得到 stringly typed 风险逻辑，缺少 evidence chain、publication boundary 和 typed rollback 语义。

因此：

- 旧 `runtime_config` table/entity/repository/cache/seed 必须删除。
- 不允许 compatibility view、alias repository、per-key upsert/delete API。
- Runtime config 变更只能通过 create immutable version + append activation 完成。
- Control factor 的 evidence、TTL、shadow、publication、payload、rollback 不得进入 runtime config document。

---

## 3. Control Factor Artifact Model

### 3.1 `ControlFactorValue`

```rust
pub struct ControlFactorValue {
    pub factor_id: ControlFactorId,
    pub factor_type: ControlFactorType,
    pub dimensions: FactorDimensions,
    pub payload: FactorPayload,
    pub evidence: FactorEvidence,
    pub status: FactorStatus,
    pub generated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub owner: String,
    pub schema_version: u32,
}
```

### 3.2 `ControlFactorType`

Phase 5 第一版只交付五类 typed control factor：

```rust
pub enum ControlFactorType {
    BucketRisk,
    ExecutionQuality,
    PortfolioRisk,
    ReconciliationHealth,
    MarketAnomaly,
}
```

### 3.3 `FactorStatus`

```rust
pub enum FactorStatus {
    Draft,
    ReportOnly,
    Candidate,
    Rejected,
    Shadow,
    Published,
    Superseded,
    Expired,
    RolledBack,
}
```

`Published` 不代表直接修改 factor row。live 生效由 publication 指针控制。

### 3.4 `FactorEvidence`

```rust
pub struct FactorEvidence {
    pub materialization_run_id: MaterializationRunId,
    pub stage_report_ids: Vec<StageReportId>,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    pub source_delay_secs: u64,
    pub market_count: u32,
    pub event_count: u32,
    pub opportunity_count: u32,
    pub settlement_count: u32,
    pub sample_count: u32,
    pub data_coverage: DataCoverageReport,
    pub point_in_time_inputs: PointInTimeInputManifest,
    pub baseline_config_hash: String,
    pub code_git_sha: String,
    pub query_fingerprint: String,
    pub confidence_interval: ConfidenceInterval,
    pub tail_risk: TailRiskEvidence,
    pub warnings: Vec<EvidenceWarning>,
}
```

Evidence 必须是 required field。没有 evidence 的 factor 禁止进入 Candidate/Shadow/Published。

### 3.5 `ControlFactorPublication`

```rust
pub struct ControlFactorPublication {
    pub publication_id: FactorPublicationId,
    pub mode: PublicationMode,
    pub factor_ids: Vec<ControlFactorId>,
    pub previous_publication_id: Option<FactorPublicationId>,
    pub status: PublicationStatus,
    pub effective_from: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub approved_by: Option<String>,
    pub approval_reason: String,
    pub publication_hash: String,
}

pub enum PublicationMode {
    Shadow,
    Published,
}
```

Live 消费的是 publication，不是任意 factor row。

`publication_hash` 必须为 `blake3:<hex>`，由 `oxide-arb-control::PublicationHasher` 对 canonical JSON（factor ID 排序，不含 `status` / `publication_hash`）计算；激活前必须 `verify`。

---

## 4. Payload Safety Contract

### 4.1 自动发布边界

| Payload 字段 | 自动发布边界 |
|---|---|
| `resolution_haircut_factor` | `0 <= value <= 1` |
| `size_multiplier` | `0 <= value <= 1` |
| `fill_probability_multiplier` | `0 <= value <= 1` |
| `daily_budget_multiplier` | `0 <= value <= 1` |
| `kelly_fraction_multiplier` | `0 <= value <= 1` |
| `min_edge_bps_addon` | `value >= 0` |
| `slippage_bps_addon` | `value >= 0` |
| `max_open_positions` | `value <= active_config.max_open_positions` |
| `block_*` | can only change from false to true automatically |

### 4.2 风险扩张例外

任何 risk-expanding change 必须同时满足：

- explicit manual approval flag；
- `risk_owner` role；
- factor-specific justification；
- shorter TTL；
- rollback target；
- immutable audit event；
- before/after diff；
- retrospective review requirement。

---

## 5. Expiry / Load Failure Contract

| 因子 | 过期行为 | 加载失败行为 |
|---|---|---|
| `BucketRiskFactor` | fail neutral | use empty bucket index |
| `ExecutionQualityFactor` | fail neutral | use baseline fill model |
| `PortfolioRiskFactor` | fail neutral by default | use baseline sizer |
| `ReconciliationHealthFactor` | critical fail closed，otherwise neutral | fail closed in Live if configured |
| `MarketAnomalyFactor` | neutral after TTL unless manual halt exists | use existing blacklist/manual halt |

过期行为如果改变 active snapshot membership，必须写 audit event。

类型级冻结（`oxide-arb-models`）：

- `FactorExpiryBehavior` / `FactorLoadFailureBehavior` — 与上表一一对应；
- `ControlFactorType::expiry_behavior()` / `load_failure_behavior(severity)` — live 加载时只读这些策略，禁止散落 magic if。

---

## 6. Breaking-Change Policy

Phase 5 是控制面重构，不为旧草案保留兼容层。允许并推荐：

- 删除旧 `ReplayMode`。
- 删除旧 CLI-first 设计。
- 删除旧 `DetectorOnly`/`Execution`/`PortfolioRisk`/`Diagnostic` 产品模式。
- 如果存在旧 `DetectionInput`/`DetectionResult` 草案类型，直接删除。
- Rename old analytics planned tables to `control_factor_*`。
- Delete old mutable `runtime_config`; replace runtime config with immutable version + append-only activation, and replace evidence-governed controls with typed registry。
- 重构 scorer/risk/sizer 构造方式，使其消费 `ControlFactorSnapshot`。

绝对禁止：

- compatibility re-export；
- alias endpoints for old replay mode names；
- stringly typed factor payload in hot path；
- live hot path ClickHouse/Postgres query；
- current-state replay masquerading as PIT evidence；
- risk-expanding automatic publication。

---

## 7. 子阶段拆分

| 子阶段 | 文件 | 主要责任 | 原章节覆盖 |
|---|---|---|---|
| 5.0 | `phase5.0-control-plane-foundation.md` | 架构契约、不变量、artifact、破坏式变更 | 0, 1, 3, 4, 8.2, 8.5, 9.4, 20 |
| 5.1 | `phase5.1-fact-data-plane.md` | CH/PG 事实、schema、writers、migration | 2, 10.6, 11, 15.1, 15.2 |
| 5.2 | `phase5.2-pit-materialization-runner.md` | PIT resolver、run manifest、幂等、错误码 | 1.3, 5, 6.1, 15.1.1 |
| 5.3 | `phase5.3-evidence-engine.md` | book/detector/execution/portfolio/settlement evidence | 6.2-6.6, 9.2 |
| 5.4 | `phase5.4-factor-builders-quality-gates.md` | 五类因子、统计物化、quality gates、shadow readiness | 7, 8, 9, 13 |
| 5.5 | `phase5.5-registry-governance-api-scheduler.md` | registry、publication、audit、API、RBAC、scheduler | 14, 15.3, 16 |
| 5.6 | `phase5.6-live-consumption.md` | `ControlFactorSnapshot`、hot path 接入、shadow decision | 10.1-10.5, 10.7, 17 |
| 5.8 | `phase5.8-verification-operations.md` | 退出条件、测试矩阵、观测、runbooks、防漂移审查 | 18, 19 |

---

## 8. 退出条件

Phase 5.0 完成后必须满足：

1. 所有原 Phase 5 章节都有明确子阶段归属。
2. 任何实施 PR 都能根据子阶段文件判断所属范围、前置依赖和退出条件。
3. `runtime_config_version` / `runtime_config_activation` 与 control factor registry 的边界清晰，且不保留旧 mutable `runtime_config`。
4. `oxide-arb-control` 与 `oxide-arb-core` 的 ownership 清晰。
5. 五类 factor 的 typed artifact、evidence、publication、expiry/load failure 语义冻结。
6. 破坏式变更原则清晰，明确禁止 re-export/alias/compat shim。
7. 没有任何文档建议 hot path 查询 CH/PG。
8. 没有任何文档建议自动因子放大风险。
