# Phase 5.1 — Fact Data Plane, Storage Schema & Live Writers

> **状态**: Production Design Target  
> **父计划**: `docs/plans/phase5-replay-analytics.md`  
> **前置依赖**: Phase 5.0  
> **覆盖原章节**: 2, 10.6, 11, 15.1, 15.2, 18.1  
> **目标**: 补齐 materialization 所需的事实数据、schema、repository contract 和 live writer，让后续 PIT/evidence 阶段有可信输入。

---

## 0. 工作范围

Phase 5.1 只解决一件事：**历史事实必须足够完整、可查询、可证明缺失，而不是靠默认值伪装完整**。

### 0.1 交付物

| 交付物 | 说明 |
|---|---|
| CH facts contract | `tick_events_l2`、`book_snapshots`、`opportunity_detection`、`opportunity_audit`、`calibration_snapshots` 的行模型和查询契约 |
| PG control schema baseline | `control_factor_*`、`runtime_config_version`、`runtime_config_activation`、`balance_snapshot`、`token_balance_snapshot` |
| Fact writers | detection、execution terminal、settlement、calibration、balance/token balance writers |
| Repository traits | `EvidenceTimeseriesRepository`、control factor repository traits |
| Migration/schema tests | iden/entity/schema graph/migration tests |
| Missing-value policy | nullable/coverage 语义，禁止 `0`/空字符串伪造事实 |

### 0.2 非目标

- 不实现 PIT resolver。
- 不实现 materialization runner。
- 不实现 factor builder。
- 不实现 publication。
- 不为了旧 `analytics_factor_*` 或 replay 草案保留 view/alias/re-export。

---

## 1. 当前缺口与处理方式

### 1.1 ClickHouse

| 表 | 当前缺口 | Phase 5.1 处理 |
|---|---|---|
| `tick_events` | schema/repo 有，live producer 缺 | 写 BBO ticks 或采样 bars，用于 convergence、spread、price reversal |
| `tick_events_l2` | row/schema 有，repo producer 不完整 | 补 token-level L2 snapshot/delta writer 和 stable query |
| `book_snapshots` | schema/repo 有，producer 缺 | 启动、reconnect、gap、周期 top N snapshot |
| `opportunity_detection` | live 写入，字段偏 slim | 扩 score/fill/calibration/book context，或拆 snapshot 表 |
| `opportunity_audit` | settlement attribution 不完整 | terminal/settlement row 必须保留 scored snapshot 或稳定 join key |
| `calibration_snapshots` | schema/repo 有，producer 缺 | `CalibrationUpdater` 每次更新后写 PIT snapshot |

### 1.2 Postgres trading/control state

| 数据域 | Phase 5.1 要求 |
|---|---|
| `market` / `event` | PIT market context 与 token mapping 可查 |
| `trade` / `position` | 可 join CH audit，重建 sequence/settlement |
| `risk_engine_state` | 增强历史查询，供 portfolio evidence 使用 |
| `potential_loss_ledger` | 供 portfolio risk evidence 使用 |
| `blacklist_entry` | 供 market anomaly/block evidence 使用 |
| `endgame_calibration_bucket` | 只能代表 current state，不能替代 PIT snapshot |
| `endgame_calibration_outcome` | fill 后 unresolved，settlement 后 resolved |
| `reconciliation_report` | 供 reconciliation evidence 使用 |
| `runtime_config` | 降级为 coarse toggles；复杂控制迁移到 versioned config + factor registry |

### 1.3 新增状态

```text
control_factor_materialization_run
control_factor_stage_report
control_factor_value
control_factor_publication
control_factor_audit_event
control_factor_shadow_decision
runtime_config_version
runtime_config_activation
balance_snapshot
token_balance_snapshot
control_factor_training_dataset
position_exit_plan
position_exit_execution
position_unwind_audit
```

所有 Postgres 表必须遵循 `docs/persistence/schema-catalog.md`：

- 新增 iden module；
- 新增 entity；
- 新增 repository trait；
- 新增 schema graph tests；
- 新增 migration tests；
- migration 禁止裸写业务 schema；
- 禁止兼容 re-export。

---

## 2. ClickHouse 设计

### 2.1 必需事实覆盖

ClickHouse 必须覆盖：

- `tick_events_l2`: token-level L2 snapshot/delta；
- `book_snapshots`: periodic and event-triggered top N depth；
- `opportunity_detection`: score components、fill probability、calibration detail、book context；
- `opportunity_audit`: rejection/terminal/settlement attribution；
- `calibration_snapshots`: calibration update 后的 PIT state；
- optional materialized views: BBO bars、spread、depth、anomaly pre-aggregation。

ClickHouse 是 facts/evidence store，不是权威控制面。

### 2.2 Query contract

Materialization code 禁止散落 ad hoc SQL 字符串。Repository API 必须暴露 typed contracts：

```rust
#[async_trait]
pub trait EvidenceTimeseriesRepository {
    async fn l2_events(
        &self,
        token_ids: &[TokenId],
        window: TimeWindow,
    ) -> Result<Vec<TickEventL2Row>, StorageError>;

    async fn book_snapshots_before(
        &self,
        token_ids: &[TokenId],
        before: DateTime<Utc>,
        limit_per_token: usize,
    ) -> Result<Vec<BookSnapshotRow>, StorageError>;

    async fn detections(
        &self,
        market_filter: MarketFilter,
        window: TimeWindow,
    ) -> Result<Vec<OpportunityDetectionRow>, StorageError>;

    async fn audits(
        &self,
        opportunity_ids: &[OpportunityId],
    ) -> Result<Vec<OpportunityAuditRow>, StorageError>;

    async fn calibration_snapshots(
        &self,
        window: TimeWindow,
    ) -> Result<Vec<CalibrationSnapshotRow>, StorageError>;
}
```

Stable ordering:

```text
event_time ASC, ingestion_time ASC, sequence ASC
```

当 CH eventual consistency 存在时，依赖 `source_delay`，不在 query 层隐藏 sleep/retry。

### 2.3 `opportunity_detection`

最少字段：

```text
opportunity_id
market_id
event_id
token_id
side
entry_price
edge_bps
expected_net_profit_usd
resolution_prob
fill_probability
score
urgency_factor
category_weight
staleness_discount
depth_used_pct
convergence_secs
category
price_zone
duration_bucket
calibration_sample_size
calibration_fallback_tier
calibration_alpha
calibration_beta
book_age_ms
detected_at
```

推荐新增 `opportunity_scored_snapshot`，避免 detection 主表无限变宽：

```text
opportunity_id
market_id
event_id
token_yes
token_no
score_components_json
calibration_snapshot_json
book_context_json
applied_factors_json
detected_at
```

如果选择扩 `opportunity_detection`，必须同样保留 applied factor trace 与 calibration snapshot hash。

### 2.4 `opportunity_audit`

Required invariant:

```text
Every audit row that references an opportunity must be able to recover:
  category
  price_zone
  duration_bucket
  resolution_prob
  fill_probability
  depth_used_pct
  staleness
  applied_factor_ids
```

Terminal/settlement rows 必须保留：

```text
opportunity_id
execution_id
trade_id
market_id
event_id
token_id
side
stage
stage_at
outcome
rejection_stage
rejection_reason
scored_snapshot_json
fill_price
filled_shares
fees_usd
payout_usd
realized_pnl_usd
winning_token_id
settlement_status
accounting_status
```

禁止用以下值伪造缺失 attribution：

```text
resolution_prob = 0
confidence = 0
price_zone = ""
duration_bucket = ""
category = ""
```

真实缺失必须 nullable，并计入 coverage。

---

## 3. Postgres 设计

### 3.1 Control tables

```text
control_factor_materialization_run
control_factor_stage_report
control_factor_value
control_factor_publication
control_factor_audit_event
control_factor_shadow_decision
```

Phase 5.1 只创建 schema/entity/repository baseline；状态机语义在 Phase 5.5 完成。

### 3.2 Runtime config versioning

```text
runtime_config_version
runtime_config_activation
```

要求：

- immutable version；
- config hash；
- activation record；
- rollback target；
- audit event；
- materialization manifest 必须引用 fixed version/ref。

### 3.3 Balance evidence

```text
balance_snapshot
token_balance_snapshot
```

`token_balance_snapshot` 最少字段：

```text
snapshot_id UUID primary key
holder_address text not null
market_id text not null
token_id text not null
side text not null
internal_shares decimal not null
external_shares decimal not null
drift_shares decimal not null
block_number bigint null
observed_at timestamptz not null
source text not null -- CLOB API / on-chain ERC1155 / subgraph
```

### 3.4 Training dataset manifest

```text
control_factor_training_dataset
```

Required fields:

```text
dataset_id UUID primary key
run_id UUID not null
factor_type text not null
window_from timestamptz not null
window_to timestamptz not null
entity_count integer not null
example_count integer not null
label_count integer not null
dataset_hash text not null
feature_schema_hash text not null
label_schema_hash text not null
storage_uri text null
created_at timestamptz not null
```

Dataset hash 必须可复现。事实修复导致 hash 变化时，不能覆盖旧 factor，只能生成新 run/new factor。

---

## 4. Live Writers

### 4.1 Detection writer

`DetectionWriter` 必须写：

- raw detector input summary；
- score components；
- calibration snapshot hash/detail；
- book context；
- applied factor trace；
- control publication id；
- nullable missing fields with coverage impact。

### 4.2 Execution audit writer

`ExecutionAuditWriter` 必须写：

- factor rejection stage；
- applied factor ids；
- terminal execution outcome；
- fill/miss/reject reason；
- fees/slippage/depth；
- scored snapshot for terminal and settlement rows。

Settlement rows 不允许丢掉 detection/scoring attribution。

### 4.3 Calibration writer

`CalibrationUpdater` 每次更新后必须写 `calibration_snapshots`，并保留：

- bucket id/dimensions；
- alpha/beta/sample；
- fallback tier；
- config hash；
- updated_at/event_time；
- snapshot hash。

### 4.4 Balance/token writer

必须新增：

- collateral/cash balance snapshot；
- ERC1155 token balance snapshot；
- allowance observation；
- reconciliation report linkage。

---

## 5. 数据迁移策略

由于 Phase 5 允许破坏式重构：

1. Stop treating old planned `analytics_factor_*` names as valid。
2. Create new `control_factor_*` schema from catalog。
3. Extend or replace CH row types in `oxide-arb-models/src/clickhouse`。
4. Update CH DDL under `oxide-arb-storage/src/clickhouse/sql`。
5. Update `TimeseriesRepository` traits and implementations。
6. Update writers in `oxide-arb-core`。
7. Add migration tests and schema graph tests。

不要添加兼容 view，不要 re-export 旧名称。

---

## 6. 测试策略

| 测试 | 必需场景 |
|---|---|
| CH row serialization | required/nullable 字段、Decimal string/primitive 边界、schema hash |
| CH query ordering | out-of-order ingestion、same event_time tie-break |
| Detection writer | score/calibration/book/factor trace 完整写入 |
| Audit writer | rejection/terminal/settlement attribution 不丢失 |
| Calibration snapshot | PIT snapshot 可查，current state 不污染历史 |
| Balance/token snapshot | internal/external/drift 字段正确，token_id 粒度 |
| Migration tests | iden/entity/schema graph/migration consistent |
| Missing-value tests | 缺字段不被 `0`/空字符串/default enum 替代 |

---

## 7. 退出条件

Phase 5.1 完成后必须满足：

1. Integration tests 中能写入 L2 facts、book snapshots、detection snapshots、audit terminal/settlement rows、calibration snapshots、balance/token snapshots。
2. Hot path latency 不因 writer 增强而显著劣化；writer 必须异步/批量/背压安全。
3. CH rows 包含 materialization 所需 key，可稳定 join `opportunity_id`、`market_id`、`token_id`、`trade_id`。
4. Settlement audit 不再写空 bucket/空 category/零 probability 伪 attribution。
5. Repository contracts 提供 typed query，不暴露 raw client 给 `oxide-arb-control`。
6. 新增 PG schema 全部通过 schema catalog 约束。
7. `control_factor_*` baseline 表存在，且无旧 analytics alias/view/re-export。
8. Missing values 进入 coverage report，而不是进入默认值。

---

## 8. 阻止进入 Phase 5.2 的情况

- `tick_events_l2` 或 `book_snapshots` 没有 live producer。
- `opportunity_detection` 缺 score/fill/calibration/book context。
- `opportunity_audit` terminal/settlement row 无法恢复 scored snapshot。
- `calibration_snapshots` 仍只能读 current state。
- token-level balance snapshot 不存在。
- materialization 所需 queries 依赖 ad hoc SQL。
- migration 中出现裸写业务 DDL。
- 出现任何 compatibility re-export、alias endpoint、旧 planned table view。
