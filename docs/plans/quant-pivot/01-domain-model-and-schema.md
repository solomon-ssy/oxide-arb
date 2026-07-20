# 01 — 领域模型与 Schema 设计

<!-- quant-pivot-lifecycle-contract:v1 -->
> **Lifecycle contract**
> - `lifecycle_assumption`: 项目尚未正式生产上线，当前状态为 `pre_production_resettable`，系统自有基线统一为 `boot` / schema version `1`。
> - `schema_data_version_impact`: 本文中的历史版本号与递增路径不再具有实施效力；当前实现不迁移测试数据、旧结构或旧版本。
> - `pre_production_behavior`: 允许 clean-break、migration squash 与全新基础设施 bootstrap，但任何数据销毁仍需操作者单独授权。
> - `production_frozen_behavior`: 一旦完成不可逆 production seal，后续变更必须提供前向 migration、兼容性评估、回滚方案与数据验证。
> - `rollback_and_data_verification`: 封存前通过清空后的 fresh-install 验证；封存后不得回退到 boot reset。

> 状态：生产级目标设计
>
> 目标：用 quant-pivot 领域模型替换 Endgame opportunity/trade/report schema
>
> 原则：先删除旧语义，再建立新 schema；不做旧表兼容 view，不做 re-export。

## 0. 领域词汇

| 术语 | 定义 | 替换旧概念 |
|---|---|---|
| `MarketSelection` | 某次报告生成时参与评估的市场集合快照 | endgame hotset / `MarketCache` scan entries |
| `FeatureVector` | 某个 market/token 在 `as_of` 时刻 point-in-time 可见的特征集合 | endgame book pair + calibration bucket |
| `FactorDefinition` | 受治理的因子定义、输入 schema、输出解释 | control factor payload |
| `FactorValue` | 某个因子在某个 entity 上的计算结果 | bucket risk / execution quality factor |
| `ModelSpec` | 模型定义，包括 scorer 类型、权重、feature schema | hardcoded `EndgameScorer` |
| `ModelRun` | 一次训练、回测、刷新或线上推理运行 | materialization run |
| `SignalCandidate` | 模型输出的候选信号，尚未组合裁剪 | `Opportunity` |
| `Recommendation` | 组合规划后进入报告的建议 | `ScoredOpportunity` |
| `RecommendationReport` | 周期性 TopN 报告，系统主产物 | daily/weekly PnL `report` |
| `PortfolioPlan` | 把候选信号转成推荐仓位的组合计划 | risk sizing |
| `RiskEnvelope` | recommendation 可被执行的风险边界 | pre-trade risk decision |
| `OrderIntent` | 从 recommendation 派生的待审批或自动执行意图 | `NewTrade` intent |
| `EntryPlan` | 入场触发、价格、数量、有效期 | execution plan |
| `ExitPlan` | 止盈、止损、时间出场、事件出场 | settlement/redeem path |
| `Attribution` | recommendation 结果归因，用于训练反馈 | calibration outcome / PnL |

## 1. ID 体系

保留现有 typed ID 机制，但重命名和新增以下 ID：

| ID | 类型 | 说明 |
|---|---|---|
| `MarketId` | `StrId` | 保留，Polymarket `condition_id` |
| `TokenId` | `StrId` | 保留，CLOB outcome token id |
| `EventId` | `StrId` | 保留，Polymarket event id |
| `MarketSelectionId` | UUID v7 | 新增，报告输入 market selection 快照 |
| `FeatureVectorId` | UUID v7 | 新增，特征向量快照 |
| `FactorDefinitionId` | UUID v7 | 新增，因子定义 |
| `ModelSpecId` | UUID v7 | 新增，模型定义 |
| `ModelVersionId` | UUID v7 | 新增，模型版本 |
| `TradePolicyArtifactId` | content-addressed UUID | 治理型入场/退出策略 artifact，由内容哈希确定 |
| `ModelRunId` | UUID v7 | 新增，训练/回测/推理运行 |
| `SignalCandidateId` | UUID v7 | 新增，候选信号 |
| `RecommendationId` | UUID v7 | 新增，报告中的单条建议 |
| `RecommendationReportId` | UUID v7 | 新增，TopN 报告 |
| `PortfolioPlanId` | UUID v7 | 新增，组合计划 |
| `OrderIntentId` | UUID v7 | 新增，执行意图 |
| `ExecutionOrderId` | UUID v7 | 新增，内部订单生命周期 |

删除或废弃：

- `OpportunityId`：旧 opportunity 语义删除。若历史归档需要，只保留在 archived 表或 JSON evidence，不进入新主路径。
- `TradeId`：旧 `trade` lifecycle 删除。新执行路径使用 `OrderIntentId` + `ExecutionOrderId`。
- `ReservationId`：旧资本预留体系删除。新系统使用 `CapitalAllocationId` 或 `RiskEnvelopeId`。

## 2. 新 Postgres 表

所有表必须通过新的 immutable SeaORM `MigrationTrait`、dense runtime entity、domain persistence DTO 和 repository trait 增加。DDL 的唯一事实源是 `quant-pivot-migration`；禁止恢复已删除的 `quant_schema`/`idens` 双轨模型，也禁止在 repository 或 runtime 启动路径中手写 DDL。

### 2.1 Market Selection

#### `quant_market_selection`

用途：记录一次报告或模型运行使用的市场集合。

关键列：

- `market_selection_id uuid pk`
- `as_of timestamptz not null`
- `decision_policy_snapshot_id uuid not null`
- `selector_hash text not null`
- `market_count int not null`
- `exclusion_summary jsonb not null`
- `created_at timestamptz not null`

索引：

- `(as_of desc)`
- `(decision_policy_snapshot_id, as_of desc)`
- `selector_hash`

#### `quant_market_selection_member`

用途：可查询化的 market selection 成员表，避免只依赖 JSONB。

关键列：

- `market_selection_id uuid not null`
- `market_id varchar(66) not null`
- `event_id text not null`
- `category qp_market_category not null`
- `status qp_market_status not null`
- `primary_token_id text not null`
- `secondary_token_id text null`
- `liquidity_usd numeric(28,8) null`
- `volume_24h_usd numeric(28,8) null`

主键：

- `(market_selection_id, market_id)`

### 2.2 Feature and Factor

#### `quant_feature_vector`

用途：存储 point-in-time feature vector 的 canonical metadata，payload 可以在 PG 保留摘要，完整事实进入 ClickHouse。

关键列：

- `feature_vector_id uuid pk`
- `market_id varchar(66) not null`
- `token_id text null`
- `as_of timestamptz not null`
- `feature_schema_version int not null`
- `feature_hash text not null`
- `data_quality qp_data_quality_status not null`
- `staleness_ms bigint not null`
- `payload jsonb not null`
- `source_refs jsonb not null`
- `created_at timestamptz not null`

索引：

- `(market_id, as_of desc)`
- `(feature_hash)`
- `(feature_schema_version, as_of desc)`

#### `quant_factor_definition`

用途：因子定义 registry。

关键列：

- `factor_definition_id uuid pk`
- `name text not null unique`
- `factor_family qp_factor_family not null`
- `scope qp_factor_definition_scope not null`
- `input_schema_version int not null`
- `output_schema_version int not null`
- `definition jsonb not null`（`FactorDefinitionDocument`，SeaORM `FromJsonQueryResult`；`name`/`family` 与关系列由 CHECK 绑定）
- `status qp_publication_status not null`
- `created_by uuid null`
- `created_at timestamptz not null`
- `updated_at timestamptz not null`

状态：

- `draft`
- `candidate`
- `shadow`
- `published`
- `retired`
- `rejected`

#### `quant_factor_value`

用途：因子计算结果，可审计、可复现。

关键列：

- `factor_value_id uuid pk`
- `factor_definition_id uuid not null`
- `feature_vector_id uuid not null`
- `market_id varchar(66) not null`
- `as_of timestamptz not null`
- `raw_value numeric(28,12) null`
- `normalized_score numeric(20,18) not null`
- `direction qp_factor_direction not null`
- `confidence numeric(20,18) not null`
- `explanation jsonb not null`
- `created_at timestamptz not null`

索引：

- `(factor_definition_id, as_of desc)`
- `(market_id, as_of desc)`

### 2.3 Model

#### `quant_model_spec`

用途：内容寻址、append-only 的模型研究定义。人类研究论题与可执行输入/训练契约共同参与 `definition_hash`；它不是可覆写的描述字段。

关键列：

- `model_spec_id uuid pk`
- `name text not null unique`
- `model_family qp_model_family not null`
- `prediction_horizon_secs bigint not null`
- `feature_schema_version int not null`
- `label_schema_version int not null`
- `thesis jsonb not null`（typed `ModelSpecThesis`）
- `input_contract jsonb not null`（typed `ModelInputContract`）
- `training_contract jsonb not null`（typed `ModelTrainingContract`）
- `definition_hash text not null`
- `created_at timestamptz not null`

#### `quant_model_version`

用途：可发布模型版本。

关键列：

- `model_version_id uuid pk`
- `model_spec_id uuid not null`
- `version int not null`
- `artifact_hash text not null`
- `research_profile_artifact_id uuid not null`
- `training_dataset_id uuid null`
- `metrics jsonb not null`（format v1 typed `ModelVersionMetrics`）
- `training_objective jsonb not null`（format v1 typed `ModelTrainingObjective`）
- `quality_gate_report jsonb null`（`NULL` 表示尚未评估）
- `publication_status qp_publication_status not null`
- `published_at timestamptz null`
- `retired_at timestamptz null`
- `created_at timestamptz not null`

唯一约束：

- `(model_spec_id, version)`

#### `quant_model_run`

用途：训练、回测、shadow、live inference 的运行记录。

关键列：

- `model_run_id uuid pk`
- `run_kind qp_model_run_kind not null`
- `model_version_id uuid null`
- `decision_policy_snapshot_id uuid not null`
- `market_selection_id uuid null`
- `window_start timestamptz not null`
- `window_end timestamptz not null`
- `status qp_model_run_status not null`
- `input_hash text not null`
- `output_hash text null`
- `error_code text null`
- `error_message text null`
- `started_at timestamptz not null`
- `finished_at timestamptz null`

### 2.4 Recommendation

#### `quant_portfolio_plan`

用途：组合规划结果，解释候选信号如何被裁剪成 TopN。

关键列：

- `portfolio_plan_id uuid pk`
- `model_run_id uuid not null`
- `market_selection_id uuid not null`
- `as_of timestamptz not null`
- `budget_usd numeric(28,8) not null`
- `allocated_usd numeric(28,8) not null`
- `risk_budget_json jsonb not null`
- `constraints_json jsonb not null`
- `rejected_summary jsonb not null`
- `created_at timestamptz not null`

#### `quant_recommendation_report`

用途：系统主产物。

关键列：

- `recommendation_report_id uuid pk`
- `report_kind qp_report_kind not null`
- `as_of timestamptz not null`
- `horizon_secs bigint not null`
- `runtime_mode qp_quant_runtime_mode not null`
- `decision_policy_snapshot_id uuid not null`
- `model_version_id uuid not null`
- `market_selection_id uuid not null`
- `portfolio_plan_id uuid not null`
- `top_n int not null`
- `status qp_publication_status not null`
- `summary_json jsonb not null`
- `published_at timestamptz null`
- `revoked_at timestamptz null`
- `created_at timestamptz not null`

索引：

- `(report_kind, as_of desc)`
- `(status, as_of desc)`
- `(model_version_id, as_of desc)`

#### `quant_recommendation`

用途：报告中的单条建议，必须承载 entry/sizing/exit/risk/evidence。

关键列：

- `recommendation_id uuid pk`
- `recommendation_report_id uuid not null`
- `rank int not null`
- `market_id varchar(66) not null`
- `event_id text not null`
- `token_id text not null`
- `outcome_side qp_outcome_side not null`（`OutcomeSide`：`yes` / `no`；买卖方向属执行层）
- `composite_score numeric(20,18) not null`
- `risk_adjusted_score numeric(20,18) not null`
- `confidence numeric(20,18) not null`
- `expected_return_bps numeric(10,4) not null`（模型原始 `E[r]`，可审计/可训练）
- `downside_bps numeric(10,4) not null`（模型原始止损幅度 `l`）
- `entry_plan jsonb not null`
- `sizing_plan jsonb not null`
- `exit_plan jsonb not null`
- `risk_envelope jsonb not null`
- `factor_breakdown jsonb not null`
- `evidence_refs jsonb not null`
- `valid_from timestamptz not null`
- `valid_until timestamptz not null`
- `status qp_publication_status not null`
- `created_at timestamptz not null`

唯一约束：

- `(recommendation_report_id, rank)`
- `(recommendation_report_id, market_id, token_id)`

### 2.5 Execution Intent

#### `quant_order_intent`

用途：recommendation 到订单执行的受治理桥梁。

关键列：

- `order_intent_id uuid pk`
- `recommendation_id uuid not null`
- `runtime_mode qp_quant_runtime_mode not null`
- `intent_kind qp_order_intent_kind not null`
- `status qp_publication_status not null`
- `approval_status qp_approval_status not null`
- `approved_by uuid null`
- `approval_reason text null`
- `approved_at timestamptz null`
- `entry_order_json jsonb not null`
- `exit_policy_json jsonb not null`
- `risk_envelope_hash text not null`
- `expires_at timestamptz not null`
- `created_at timestamptz not null`
- `updated_at timestamptz not null`

状态：

- `draft`
- `pending_approval`
- `approved`
- `rejected`
- `expired`
- `submitted`
- `partially_filled`
- `filled`
- `cancelled`
- `failed`

#### `quant_execution_order`

用途：内部订单生命周期。替代旧 `trade` 表。

关键列：

- `execution_order_id uuid pk`
- `order_intent_id uuid not null`
- `order_phase qp_execution_order_phase not null`
- `market_id varchar(66) not null`
- `token_id text not null`
- `side qp_side not null`
- `order_type qp_order_type_kind not null`
- `gtd_expiration_at timestamptz null`
- `price numeric(20,18) not null`
- `shares numeric(38,18) not null`
- `cost_usd numeric(28,8) not null`
- `venue_order_id text null`
- `venue_status text null`
- `state qp_execution_order_state not null`
- `submitted_at timestamptz null`
- `filled_at timestamptz null`
- `cancelled_at timestamptz null`
- `error_message text null`
- `created_at timestamptz not null`
- `updated_at timestamptz not null`

### 2.6 Attribution

#### `quant_recommendation_attribution`

用途：记录 recommendation 的最终结果，进入训练闭环。

关键列：

- `recommendation_id uuid pk`
- `outcome text not null`
- `entry_outcome_json jsonb not null`
- `exit_outcome_json jsonb not null`
- `realized_pnl_usd numeric(28,8) null`
- `max_adverse_excursion_bps numeric(20,8) null`
- `max_favorable_excursion_bps numeric(20,8) null`
- `label_available_at timestamptz null`
- `attribution_json jsonb not null`
- `created_at timestamptz not null`
- `updated_at timestamptz not null`

## 3. 新 ClickHouse Facts

### 3.1 保留并重命名的 facts

| 旧 ClickHouse row | 新命名 | 说明 |
|---|---|---|
| `tick_event.rs` | `quant_tick_event` | 保留 tick/BBO 基础事实 |
| `book_snapshot.rs` | `quant_book_snapshot` | 保留 L2 snapshot |
| `book_l2_replay.rs` | `quant_book_l2_event` | 保留 replay 输入 |
| `book_microstructure.rs` | `quant_book_microstructure` | 保留并扩展 spread/depth/imbalance |
| `book_decision_context.rs` | `quant_decision_context` | 从 endgame decision 改为 recommendation context |

### 3.2 删除的 facts

| 旧 fact | 删除原因 |
|---|---|
| `opportunity_detection.rs` | 绑定 `Opportunity` / endgame detector |
| `opportunity_audit.rs` | 绑定 opportunity lifecycle |
| `calibration_snapshot.rs` | 绑定 endgame resolution calibration |

### 3.3 新 facts

#### `quant_feature_event`

字段：

- `event_time DateTime64`
- `as_of DateTime64`
- `market_id String`
- `token_id String`
- `feature_schema_version UInt32`
- `feature_name LowCardinality(String)`
- `feature_value Decimal128`
- `value_kind Enum8`
- `source_kind LowCardinality(String)`
- `staleness_ms UInt64`
- `ingestion_time DateTime64`

排序：

```text
(market_id, feature_name, as_of, ingestion_time)
```

#### `quant_factor_event`

字段：

- `event_time DateTime64`
- `as_of DateTime64`
- `market_id String`
- `factor_name LowCardinality(String)`
- `factor_family LowCardinality(String)`
- `raw_value Decimal128`
- `normalized_score Decimal128`
- `confidence Decimal128`
- `direction Enum8`
- `model_run_id String`
- `ingestion_time DateTime64`

排序：

```text
(model_run_id, market_id, factor_name, as_of)
```

#### `quant_signal_candidate_event`

字段：

- `event_time DateTime64`
- `signal_candidate_id String`
- `model_run_id String`
- `market_id String`
- `token_id String`
- `side Enum8`
- `score Decimal128`
- `confidence Decimal128`
- `entry_price Decimal128`
- `target_price Decimal128`
- `stop_price Decimal128`
- `rank_before_portfolio UInt32`
- `rejection_reason LowCardinality(String)`

排序：

```text
(model_run_id, rank_before_portfolio, market_id)
```

#### `quant_report_recommendation_fact`

> Phase 11.8 破坏式替代旧 `quant_recommendation_event`。本表是 prepare 阶段冻结的 decision fact，
> 不复制 PG recommendation/report/delivery lifecycle。

字段：

- `event_time DateTime64`
- `recommendation_report_id String`
- `recommendation_id String`
- `rank UInt32`
- `market_id String`
- `token_id String`
- `side Enum8`
- `score Decimal128`
- `risk_adjusted_score Decimal128`
- `trade_plan_available Bool`
- `suggested_usd Decimal128`
- `valid_until DateTime64`

排序：

```text
(recommendation_report_id, recommendation_id)
```

#### `quant_execution_event`

字段：

- `event_time DateTime64`
- `order_intent_id String`
- `execution_order_id String`
- `recommendation_id String`
- `event_kind LowCardinality(String)`
- `market_id String`
- `token_id String`
- `side Enum8`
- `price Decimal128`
- `shares Decimal128`
- `cost_usd Decimal128`
- `venue_order_id String`
- `ingestion_time DateTime64`

排序：

```text
(order_intent_id, event_time, ingestion_time)
```

## 4. 旧 Postgres 表命运

### 4.1 删除

| 旧表 / entity | 删除原因 | 新替代 |
|---|---|---|
| `trade` | 旧 FOK trade lifecycle | `quant_order_intent`, `quant_execution_order` |
| `position` | 旧 hold-to-resolution position | 新 position lifecycle 后续由 `quant_execution_position` 增加，不能复用旧语义 |
| `calibration` | Endgame bucket calibration | `quant_factor_definition`, `quant_model_version` |
| `calibration_outcome` | Endgame settlement labels | `quant_recommendation_attribution` |
| `risk_state` | 旧 risk engine snapshot | `quant_risk_state` 后续重建，或 runtime envelope projection |
| `risk_audit_event` | 旧 pre-trade risk trace | `quant_risk_audit_event` |
| `risk_fill_applied` | 旧 fill accounting | `quant_execution_event` + attribution |
| `potential_loss_ledger` | 旧 reservation/potential loss | `quant_risk_envelope_audit` |
| `reconciliation_report` | 旧 trade reconciliation | `quant_execution_reconciliation` 后续新建 |
| `emergency_snapshot` | 旧 ExecutionFSM emergency | 新 kill switch state |
| `blacklist_entry` | 旧 market/token blacklist | `quant_market_block` 或 market selection exclusion |
| `balance_snapshot` | 旧 trading balance | execution mode 下重建为 `quant_account_snapshot` |
| `accounting_period` | 旧 daily/weekly trade accounting | `quant_recommendation_attribution` + analytics |

### 4.2 保留但改名/改语义

| 旧表 | 新命名 | 说明 |
|---|---|---|
| `market` | `market` 保留 | Polymarket catalog 权威，字段去 Endgame 假设 |
| `event` | `event` 保留 | Polymarket event metadata |
| `report` | 删除或迁移为 `quant_recommendation_report` | 旧 daily/weekly JSON report 不再作为主表 |
| `decision_policy_snapshot` | 重建 | system-owned format v1；只引用四类 immutable profile artifact ID/hash |
| `policy_activation` | 重建 | bundle generation、完整 revision vector、audit/outbox 原子闭环 |
| `operation_log` | 保留 | 扩展 quant actions |
| `system_runtime_state` | 重建字段 | 删除 `execution_mode`，新增 `quant_runtime_mode` |
| `control_factor_*` | 大部分重命名为 `quant_factor_*` / `quant_model_*` | 不保留旧 control factor 语义 |

### 4.3 保留原样

RBAC / admin 基础表保留：

- `user`
- `role`
- `user_role`
- `menu`
- `role_menu`
- `casbin_rule`
- `seed_application`

## 5. Domain Module 布局

目标布局：

```text
crates/quant-pivot-models/src/domain/
├── quant/
│   ├── selection.rs
│   ├── feature.rs
│   ├── factor.rs
│   ├── model.rs
│   ├── signal.rs
│   ├── recommendation.rs
│   ├── portfolio.rs
│   ├── execution.rs
│   ├── attribution.rs
│   └── mod.rs
├── api/
│   ├── quant_report.rs
│   ├── quant_model.rs
│   ├── quant_factor.rs
│   ├── quant_execution.rs
│   └── ...
```

删除：

- `domain/trading/opportunity.rs`
- `domain/trading/scored_snapshot.rs`
- `domain/trading/trade.rs`
- `domain/trading/settlement.rs`
- `domain/accounting/potential_loss.rs`
- `domain/governance/calibration.rs`
- `domain/api/opportunity.rs`
- `domain/api/trade.rs`
- `domain/api/pnl.rs`
- `domain/api/replay.rs`

保留但改语义：

- `domain/market/*`
- `domain/ws/*`
- `domain/api/market.rs`
- `domain/api/analytics.rs`
- `domain/api/runtime_config.rs`

## 6. API DTO 命名

新 API DTO 必须遵循三层模型：

### 6.1 Report

- `CreateRecommendationReportRequest` 不需要暴露给普通用户，报告由调度器生成。
- `RecommendationReportQuery`
- `RecommendationReportView`
- `RecommendationView`
- `RecommendationAttributionView`

### 6.2 Model

- `CreateModelSpecRequest`
- `UpdateModelSpecRequest`
- `ModelSpecQuery`
- `ModelSpecView`
- `ModelVersionView`
- `ModelRunView`

### 6.3 Factor

- `CreateFactorDefinitionRequest`
- `UpdateFactorDefinitionRequest`
- `FactorDefinitionQuery`
- `FactorDefinitionView`
- `FactorValueView`

### 6.4 Execution

- `ApproveOrderIntentRequest`
- `RejectOrderIntentRequest`
- `CancelOrderIntentRequest`
- `OrderIntentQuery`
- `OrderIntentView`
- `ExecutionOrderView`

## 7. 枚举设计

新增枚举：

- `QuantRuntimeMode`: `ReportOnly`, `SemiAuto`, `AutoExecution`
- `ReportKind`: `TopN`, `ShadowTopN`, `PostRunAudit`
- `RecommendationStatus`: `Published`, `Revoked`, `Expired`, `IntentCreated`, `Executed`, `Attributed`
- `OutcomeSide`（recommendation/candidate 结果方向）: `Yes`, `No`
- `Side`（执行层买卖动作，`enums::common`）: `Buy`, `Sell`
- `PriceComparison`: `AtOrAbove`, `AtOrBelow`
- `FillRequirement`: `AllOrNothing`, `AllowPartial`
- `EntryTriggerState`: `NotRequired`, `Waiting`, `Confirming`, `Ready`, `Expired`
- `OrderType`: `Fok`, `Fak`, `Gtc`, `Gtd`
- `OrderIntentStatus`: `Draft`, `PendingApproval`, `Approved`, `ApprovedByPolicy`, `AdmissionPending`, `AdmissionRejected`, `Submitted`, `PartiallyFilled`, `Filled`, `Rejected`, `Cancelled`, `Failed`, `Expired`, `Invalidated`
- `TradePolicyStatus`: `Draft`, `Validated`, `Published`, `Retired`
- `ModelPublicationStatus`: `Draft`, `Candidate`, `Shadow`, `Published`, `Retired`, `Rejected`
- `DataQualityStatus`: `Fresh`, `Acceptable`, `Degraded`, `Stale`, `Insufficient`

`EntryTrigger`、`EntryOrderPolicy`、`ScaleOutTarget`、`TrailingStopPolicy`、
`ThesisInvalidationPolicy` 是带字段的正交 tagged union，不再使用把触发条件、订单类型和
退出原因混在一起的 `EntryTriggerKind` / `ExitTriggerKind`。权威闭环见
[`phase-11/11.7-labeling-entry-exit-closed-loop.md`](phase-11/11.7-labeling-entry-exit-closed-loop.md)。

删除枚举：

- `ExecutionMode`
- `EntryTriggerKind`、`ExitTriggerKind`
- Endgame-only calibration enums as active model enums: `PriceZone`, `DurationBucket` 只可作为 archived evidence 类型保留，不进入新主路径。
-旧 `TradeState`、`TradeBusinessOutcome`、`TradeReconcileResolution`。

## 8. Migration 原则

这是破坏式重构，迁移原则如下：

1. 新 schema 以新表名创建，不在旧表上逐字段演化。
2. 旧表进入 drop plan；如果需要保留历史，先导出到 archive schema 或对象存储。
3. 不创建兼容 view。
4. 不创建旧表到新表的 trigger。
5. 六类 runtime policy 与系统自有 artifact 当前均为 boot schema `1`；不保留旧 parser、alias、版本映射或双读路径。
6. 所有旧 API 路径删除或 410，不转发到新 API。
7. 所有新表必须有 schema catalog、entity、domain DTO、repository、migration test。

## 9. Schema 验收标准

- `cargo test -p quant-pivot-models schema` 覆盖新增 idens。
- Postgres migration test 覆盖新表创建、索引、外键、seed。
- ClickHouse row serialization snapshot 覆盖所有新 fact。
- DTO tests 覆盖 sensitive stripping、query normalization、status transition request。
- 删除清单中每个旧 entity 都有明确命运：drop、rename、archive、keep。

## 10. 核心 Rust 类型草图

这些类型不是最终代码，但字段语义必须进入实现。

### 10.1 `RecommendationReport`

```rust
/// Immutable TopN quant report published by the report pipeline.
pub struct RecommendationReport {
    pub recommendation_report_id: RecommendationReportId,
    pub report_kind: ReportKind,
    pub as_of: DateTime<Utc>,
    pub horizon_secs: u64,
    pub runtime_mode: QuantRuntimeMode,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_version_id: ModelVersionId,
    pub market_selection_id: MarketSelectionId,
    pub portfolio_plan_id: PortfolioPlanId,
    pub top_n: u32,
    pub status: RecommendationReportStatus,
    pub summary: ReportSummary,
    pub recommendations: Vec<Recommendation>,
}
```

### 10.2 `Recommendation`

```rust
/// Portfolio-approved actionable recommendation.
pub struct Recommendation {
    pub recommendation_id: RecommendationId,
    pub rank: u32,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub score: RecommendationScore,
    pub entry_plan: EntryPlan,
    pub sizing_plan: SizingPlan,
    pub exit_plan: ExitPlan,
    pub risk_envelope: RiskEnvelope,
    pub factor_breakdown: Vec<FactorContribution>,
    pub evidence: RecommendationEvidence,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
}
```

### 10.3 `FeatureVector`

```rust
/// Point-in-time visible feature vector for one market/token.
pub struct FeatureVector {
    pub feature_vector_id: FeatureVectorId,
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub as_of: DateTime<Utc>,
    pub schema_version: FeatureSchemaVersion,
    pub data_quality: DataQualityStatus,
    pub entries: Vec<FeatureEntry>,
    pub source_refs: Vec<EvidenceSourceRef>,
}

pub struct FeatureEntry {
    pub name: FeatureName,
    pub value: FeatureValue,
    pub observed_at: DateTime<Utc>,
    pub staleness_ms: u64,
    pub null_policy: FeatureNullPolicy,
}
```

### 10.4 `OrderIntent`

```rust
/// Governed bridge from recommendation to venue orders.
pub struct OrderIntent {
    pub order_intent_id: OrderIntentId,
    pub recommendation_id: RecommendationId,
    pub runtime_mode: QuantRuntimeMode,
    pub status: OrderIntentStatus,
    pub approval: ApprovalState,
    pub entry_order: EntryOrderSpec,
    pub exit_policy: ExitPolicy,
    pub risk_envelope_hash: Hash,
    pub expires_at: DateTime<Utc>,
}
```

## 11. Repository Trait 草图

Repository public signatures 禁止暴露 `ActiveModel` / `ActiveValue`。

```rust
#[async_trait]
pub trait RecommendationReportRepository {
    async fn create_report(
        &self,
        report: NewRecommendationReport,
        recommendations: Vec<NewRecommendation>,
    ) -> RepositoryResult<RecommendationReportInfo>;

    async fn latest_published(
        &self,
        kind: ReportKind,
    ) -> RepositoryResult<Option<RecommendationReportInfo>>;

    async fn revoke(
        &self,
        report_id: &RecommendationReportId,
        reason: RevokeReason,
    ) -> RepositoryResult<RecommendationReportInfo>;
}

#[async_trait]
pub trait ModelRegistryRepository {
    async fn create_model_spec(&self, new: NewModelSpec) -> RepositoryResult<ModelSpecInfo>;
    async fn create_model_version(&self, new: NewModelVersion) -> RepositoryResult<ModelVersionInfo>;
    async fn publish_model_version(
        &self,
        model_version_id: &ModelVersionId,
        audit: GovernanceAuditInput,
    ) -> RepositoryResult<ModelVersionInfo>;
}

#[async_trait]
pub trait OrderIntentRepository {
    async fn create_pending(&self, new: NewOrderIntent) -> RepositoryResult<OrderIntentInfo>;
    async fn approve(&self, input: ApproveOrderIntent) -> RepositoryResult<OrderIntentInfo>;
    async fn transition(
        &self,
        input: OrderIntentTransition,
    ) -> RepositoryResult<OrderIntentInfo>;
}
```

## 12. 状态迁移约束

所有状态迁移必须集中在 repository 或 service 方法内，禁止 handler 直接 patch status。

### 12.1 Report

```text
building -> published
building -> published_empty
building -> failed
published -> revoked
published -> expired
published -> intent_created
```

### 12.2 OrderIntent

```text
draft -> pending_approval
draft -> approved_by_policy
pending_approval -> approved
pending_approval -> rejected
pending_approval -> expired
approved -> submitted
approved_by_policy -> submitted
submitted -> filled | partially_filled | cancelled | failed
```

Repository 必须拒绝非法边，例如 `rejected -> approved`、`expired -> submitted`。
