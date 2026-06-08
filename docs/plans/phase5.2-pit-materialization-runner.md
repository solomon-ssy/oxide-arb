# Phase 5.2 — Point-in-Time Input Resolver & Materialization Runner

> **状态**: Production Design Target  
> **父计划**: `docs/plans/phase5-replay-analytics.md`  
> **前置依赖**: Phase 5.0, Phase 5.1  
> **覆盖原章节**: 1.3, 5, 6.1, 15.1.1, 18.2, 18.3  
> **目标**: 构建可重启、可审计、可去重、可取消的 materialization run 框架，并提供所有 evidence stage 共享的 PIT 输入解析能力。

---

## 0. 工作范围

Phase 5.2 交付 `ControlFactorMaterializationRun` 的生产级执行骨架。它不做复杂 evidence 计算，但必须保证所有后续 stage 都在同一份 immutable manifest、PIT input manifest、source delay、query fingerprint 和 stage report 语义下运行。

### 0.1 交付物

| 交付物 | 说明 |
|---|---|
| PIT input resolver | market/token、fee、calibration、runtime config、risk/accounting/balance/settlement 输入 |
| Run manifest | immutable `MaterializationRunManifest` 和 `manifest_hash` |
| Run status machine | `Queued -> Running -> Completed/CompletedWithRejectedFactors/ReportOnly/Failed/Cancelled` |
| Idempotency/dedupe | run、stage、factor、publication、audit event 的幂等策略 |
| Stage contract | `StageInput<T>`、`StageOutput<T>`、stage report 通用字段 |
| Stable error codes | UI、alert、retry 可以依赖的错误码 |
| Cancellation/failure report | 可取消、可失败、可恢复，失败不推进 factor |

### 0.2 非目标

- 不实现 book reconstruction 细节。
- 不实现 factor builder。
- 不实现 publication/governance。
- 不实现 API/UI；只提供 service/repository 能力。

---

## 1. Point-in-Time Resolver

### 1.1 必需输入域

每个 materialization run 必须能按 window/event time 解析：

- `MarketId -> YES/NO TokenId` mapping；
- market/event metadata；
- fee schedule；
- runtime config version；
- calibration snapshots；
- trade/position/risk/accounting state；
- balance and token balance snapshots；
- settlement truth and reconciliation status。

### 1.2 输出

```text
InputResolutionReport
MarketReplayContext
PointInTimeInputManifest
```

`PointInTimeInputManifest` 必须记录：

- 每类输入的 source table/repository；
- query window；
- query fingerprint；
- row count；
- coverage；
- snapshot hash；
- missing input list；
- fallback policy；
- whether production factor generation is allowed。

### 1.3 Fail-closed 输入

以下情况在生产因子生成中必须 fail closed：

- market/token mapping missing；
- calibration required but unavailable；
- fee schedule required but unavailable；
- runtime config hash cannot be resolved；
- required balance source missing for reconciliation-sensitive evidence；
- required settlement truth missing for outcome-dependent factor。

Report-only run 可以继续，但必须把 failure 降级原因写入 stage report，不能产出 Candidate。

---

## 2. Run 类型

| 类型 | 触发者 | 是否写 Draft | 典型用途 |
|---|---|---:|---|
| `Scheduled` | scheduler | 是 | 小时级/天级 factor refresh |
| `Backfill` | UI/API/operator | 可选 | 修复 historical gaps |
| `Incident` | operator/alert | 是，可 emergency publish anomaly | oracle mismatch、settlement issue、critical reconciliation drift |
| `ConfigComparison` | config workflow | 默认否 | 比较 candidate runtime config 和 active config |
| `ForensicReport` | operator | 否 | 事故后分析，不产出 factors |

这些是 run reason，不是用户可见 replay mode。Stage graph 固定且显式。

---

## 3. Run Manifest

```rust
pub struct MaterializationRunManifest {
    pub run_id: MaterializationRunId,
    pub run_kind: MaterializationRunKind,
    pub trigger: RunTrigger,
    pub window: TimeWindow,
    pub source_delay_secs: u64,
    pub markets: MarketFilter,
    pub requested_factor_types: Vec<ControlFactorType>,
    pub data_requirements: DataRequirements,
    pub runtime_config_ref: RuntimeConfigRef,
    pub simulation_config: SimulationConfig,
    pub quality_gate_policy: QualityGatePolicy,
    pub output_policy: OutputPolicy,
    pub code_git_sha: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}
```

Manifest 必须 immutable。任何 retry 都复用同一 manifest hash；任何 operator 修改都创建新 run。

### 3.1 Source delay

默认窗口：

```text
[trigger_time - source_delay - interval, trigger_time - source_delay)
```

每类 scheduled job 可以有不同 source delay：

| Run | Default source delay |
|---|---:|
| execution quality hourly | 10-30 min |
| reconciliation health hourly/event | 0-10 min |
| bucket risk daily | 1-6 h |
| portfolio risk daily | 1-6 h |
| market anomaly event | 0-10 min |

---

## 4. Run 状态机

```text
Queued
  -> Running
      -> Completed
      -> CompletedWithRejectedFactors
      -> ReportOnly
      -> Failed
      -> Cancelled
```

`Completed` 表示 requested stage outputs 和 factor builders 成功执行，不代表 factors 已发布。

状态转换要求：

- `Queued -> Running`: acquire run lock，写 `started_at`。
- `Running -> Completed`: all required stage status successful，factor output 符合 output policy。
- `Running -> CompletedWithRejectedFactors`: builder/gates 有 rejected factors，但 run 自身成功。
- `Running -> ReportOnly`: output policy 或 coverage 限制禁止写 Candidate。
- `Running -> Failed`: stage fatal error，写 stable error code。
- `Running -> Cancelled`: operator/system cancellation，必须保存 partial stage reports。

---

## 5. 幂等与并发

### 5.1 Run dedupe

```text
run_dedupe_key =
  hash(run_kind, window_from, window_to, source_delay_secs,
       market_filter, requested_factor_types, runtime_config_ref,
       simulation_config_hash, quality_gate_policy_hash, code_git_sha)
```

规则：

- `Scheduled` runs use `run_dedupe_key`；等价 run 已 `Queued`/`Running` 时不能再 enqueue。
- `Backfill`/`Incident` 可通过 `force_new_run=true` 绕过 dedupe，但必须有 operator reason。
- `ForensicReport` 允许和其他 run 重叠，但不能写 Candidate。

### 5.2 Stage/factor/publication/audit idempotency

| 资源 | 幂等键 |
|---|---|
| Stage report | `(run_id, stage_name)` |
| Factor value | `(run_id, factor_type, dimensions_hash, payload_hash)` |
| Publication | explicit idempotency key，不能 silent upsert |
| Audit event | `(request_id, event_type, resource_id)` |

### 5.3 并发策略

| 范围 | 策略 |
|---|---|
| 相同 dedupe key | 只允许一个 active run |
| 相同 market 集合且窗口重叠 | `ForensicReport` 允许；`Scheduled` 默认阻止，backfill 可显式允许 |
| Publication 发布 | Phase 5.5 用 advisory/transaction lock 串行化 |
| Snapshot 刷新 | Phase 5.6 单写者，读路径 lock-free |

---

## 6. Stage Contract

```rust
pub struct StageInput<T> {
    pub run_id: MaterializationRunId,
    pub manifest: MaterializationRunManifest,
    pub prior: Option<T>,
}

pub struct StageOutput<T> {
    pub stage_report: ControlFactorStageReport,
    pub artifact: Option<T>,
}
```

### 6.1 Stage report 字段

```text
stage_name
status
started_at / finished_at
input_artifact_hashes
output_artifact_hash
coverage
records_read
records_written
warnings
errors
query_fingerprints
```

### 6.2 Stage 状态

```text
Pending
Running
Completed
CompletedWithWarnings
SkippedNotRequired
InsufficientCoverage
ReportOnly
Failed
```

Stage output artifact 必须 hash。后续 stage 只能引用 prior artifact hash，不得隐式读取 mutable global state。

---

## 7. Stable Error Codes

| 错误码 | 含义 | 是否重试 |
|---|---|---:|
| `input.market_mapping_missing` | market/token 映射缺失 | 否，需要修复数据 |
| `input.pit_config_missing` | runtime config version 不可用 | 否 |
| `input.calibration_snapshot_missing` | 必需 PIT calibration 缺失 | bucket factor 不可重试 |
| `ch.coverage_l2_insufficient` | L2 覆盖率低于阈值 | 数据 backfill 后可重试 |
| `ch.book_snapshot_gap` | bootstrap snapshot 不可用 | 数据 backfill 后可重试 |
| `audit.settlement_attribution_missing` | terminal/settlement join 不完整 | audit 修复后可重试 |
| `risk.sequence_incomplete` | trade/risk state sequence 无法重建 | PG 修复后可重试 |
| `gate.sample_insufficient` | quality gate 样本阈值失败 | 否，等待更多数据 |
| `gate.not_conservative` | payload 会放大风险 | 否，只能走人工审批 |
| `publication.lock_conflict` | publication 并发更新冲突 | 是 |
| `snapshot.schema_mismatch` | live snapshot 无法解码 payload | 否，应 rollback |

错误码必须稳定，因为 UI、告警和重试策略会依赖它们。

---

## 8. Persistence

### 8.1 `control_factor_materialization_run`

Required columns:

```text
run_id UUID primary key
run_dedupe_key text unique nullable
run_kind text not null
trigger_type text not null
trigger_ref text null
status text not null
window_from timestamptz not null
window_to timestamptz not null
source_delay_secs bigint not null
market_filter jsonb not null
requested_factor_types jsonb not null
data_requirements jsonb not null
runtime_config_ref jsonb not null
simulation_config_hash text not null
quality_gate_policy_hash text not null
output_policy text not null
code_git_sha text not null
created_by text not null
started_at timestamptz null
finished_at timestamptz null
failure_code text null
failure_detail text null
report_uri text null
manifest_hash text not null
created_at timestamptz not null
updated_at timestamptz not null
```

Indexes:

```text
idx_cfm_run_status_created_at(status, created_at)
idx_cfm_run_window(window_from, window_to)
idx_cfm_run_kind_created_at(run_kind, created_at)
uniq_cfm_run_dedupe_key(run_dedupe_key) where run_dedupe_key is not null
```

### 8.2 `control_factor_stage_report`

Required columns:

```text
stage_report_id UUID primary key
run_id UUID references control_factor_materialization_run
stage_name text not null
status text not null
started_at timestamptz not null
finished_at timestamptz null
input_artifact_hashes jsonb not null
output_artifact_hash text null
coverage jsonb not null
metrics jsonb not null
warnings jsonb not null
errors jsonb not null
query_fingerprints jsonb not null
created_at timestamptz not null
```

Unique:

```text
uniq_cfm_stage(run_id, stage_name)
```

---

## 9. 测试策略

| 测试 | 必需场景 |
|---|---|
| PIT resolver | market metadata 变化、token mapping 变化、fee 变化、calibration update、runtime config activation |
| Source delay | window 计算、edge timestamp、late-arriving fact 不污染 evidence |
| Run dedupe | scheduled duplicate、backfill force、incident force reason |
| Run restart | stage idempotent upsert、partial failure retry、same manifest hash |
| Cancellation | running run cancel 后保留 partial report，不写 Candidate |
| Error codes | fatal/nonfatal/retryable 分类稳定 |
| Query fingerprint | 输入 query 改变时 fingerprint 改变 |
| Stage contract | artifact hash、coverage、warnings/errors 必填 |

---

## 10. 退出条件

Phase 5.2 完成后必须满足：

1. 任意 timestamp 都可解析 market/token/config/calibration/fee state，缺失时显式失败。
2. Resolver 不允许静默 fallback 到 current state。
3. Run manifest immutable，`manifest_hash` 可复现。
4. Scheduled run dedupe 生效，backfill/incident force 必须带 reason。
5. Stage report upsert by `(run_id, stage_name)`，retry 不重复写。
6. Cancellation/failure 不推进 affected factors。
7. Stable error codes 已被测试锁定。
8. 所有 stage report 都包含 coverage、query fingerprint、artifact hash。

---

## 11. 阻止进入 Phase 5.3 的情况

- 任一 resolver 会读 current calibration/config/fee 来解释历史。
- Run retry 可能创建重复 run/stage/factor。
- `source_delay` 不是 manifest 的一部分。
- Stage report 缺 coverage 或 query fingerprint。
- Fatal input 缺失被降级为默认值。
- Cancellation 后可能留下半写 Candidate。
