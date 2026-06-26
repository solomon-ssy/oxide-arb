# 06 — 配置、部署与运维设计

> 状态：生产级目标设计
>
> 目标：用 deploy-config + runtime-config v3 支撑 quant-pivot，删除旧 Endgame trading 配置面。

## 0. 配置分层

继续保留两层配置模型：

- Deploy config：进程启动绑定，改动需要重启。
- Runtime config v3：受治理、版本化、热激活。

禁止：

- 把可热更新策略参数放进 TOML。
- 把连接池、worker 并发、credential source 放进 runtime config。
- 保留 runtime-config v2 parser。
- 允许未知 key 静默通过。

## 1. Deploy Config

### 1.1 保留 section

| Section | 命运 | 说明 |
|---|---|---|
| `[polymarket]` | 保留 | CLOB/Gamma endpoint、chain id |
| `[polymarket.onchain]` | 保留但可选 | report_only 不要求 RPC ready |
| `[polymarket.fees]` | 保留 | fee 是 feature 和 execution cost 输入 |
| `[market_data.websocket]` | 保留并改语义 | transport config，不再包含 endgame hotset |
| `[market_data.gamma]` | 保留 | market catalog sync |
| `[observability]` | 保留 | logging |
| `[db.postgres]` | 保留 | 权威状态 |
| `[db.clickhouse]` | 保留 | facts/analytics |
| `[cache]` | 保留 | Redis/Moka |
| `[web]` | 保留 | admin API/WS |

### 1.2 删除 section/field

| Path | 删除原因 |
|---|---|
| `[execution.book_apply]` | 旧 execution runner shard |
| `[settlement.lifecycle]` | 旧 settlement worker channel |
| `market_data.websocket.engine_endgame_window_hours` | Endgame-only hot subscription |
| `market_data.websocket.engine_max_subscription_tokens` | 命名绑定 engine trading hotset，改为 quant ingestion cap |
| `[keys]` mode-aware required policy / `load_credentials_in_report_only` | 纠偏：私钥不再 mode-gated——**所有 mode 都加载私钥用于读真实账户**（report_only ≠ dry-run）；仅签名/下单为 semi_auto/auto |

### 1.3 新增 deploy sections

```toml
[quant.workers]
report_expire_sweep_secs = 300
materialization_worker_count = 2
model_run_worker_count = 1
order_intent_worker_count = 1
exit_monitor_worker_count = 1

[quant.storage]
feature_batch_size = 5000
factor_batch_size = 5000
recommendation_batch_size = 1000
fact_flush_interval_secs = 5

[quant.execution]
order_worker_channel_capacity = 1024
exit_monitor_channel_capacity = 1024
reconciliation_channel_capacity = 256

[quant.account]
# Polymarket proxy/funder 地址（持有 USDC.e + outcome token；独立于 signer EOA）。
# Data API `GET /positions?user=<funder>` 用此地址读真实持仓（keyless）。
funder = "0x..."

[market_data.data_api]
# Data API base URL（公开持仓读取，keyless）。
base_url = "https://data-api.polymarket.com"
```

**私钥所有 mode 都需要（用于读真实抵押余额 + 派生 L2 读凭证）**；私钥的**签名/下单**用途
仅 `semi_auto` / `auto_execution` preflight 才要求。已删除 `load_credentials_in_report_only`
（凭证不再 mode-gated）。

## 2. Runtime Config v3

### 2.1 Root

```text
RuntimeConfig {
  schema_version: 3,
  selection,
  data_quality,
  features,
  factors,
  model,
  reports,
  portfolio,
  execution,
  notification
}
```

删除 v2：

- `detection`
- `execution` old
- `risk`
- `settlement`
- `redeem_routing`

### 2.2 `selection`

字段：

- `enabled_categories`
- `excluded_market_ids`
- `included_market_ids`
- `min_liquidity_usd`
- `min_volume_24h_usd`
- `max_spread_bps`
- `allow_near_resolution`
- `min_time_to_resolution_secs`
- `max_time_to_resolution_secs`
- `max_selection_size`

### 2.3 `data_quality`

字段：

- `max_book_age_ms`
- `max_fact_lag_secs`
- `min_book_depth_usd`
- `allow_degraded_domain_features`
- `reject_crossed_books`
- `reject_empty_books`
- `source_delay_secs`
- `feature_staleness_policy`

### 2.4 `features`

字段：

- `feature_schema_version`
- `enabled_feature_families`
- `required_features`
- `domain_feature_policy`
- `bar_windows_secs`
- `momentum_windows_secs`
- `volatility_windows_secs`
- `depth_levels`

### 2.5 `factors`

字段：

- `enabled_factor_families` — `Vec<FactorFamily>`（仅 generic 家族；domain 因子由 category 路由）
- `factor_weights`
- `min_factor_confidence`
- `missing_factor_policy`

因子集合身份由 `enabled_factor_families` 推导，经 `factor_schema_hash` 绑定模型 artifact；无独立的 factor set id 字段。

### 2.6 `model`

字段（schema 全集；**在线消费 phase 见下表**）：

| 字段 | 在线消费 phase | 说明 |
|------|----------------|------|
| `active_model_version_id` | **3.4** | active 已发布版本 |
| `shadow_model_version_id` | **3.4** | 可选 shadow |
| `min_model_confidence` | **3.4** | runner `accepted` 过滤 + CH `rejection_reason` |
| `candidate_score_floor` | **3.4** | 同上 |
| `shadow_diff_threshold` | **3.4** | shadow vs active diff（metrics）；3.7 持久化/告警 |
| `min_quality_gate_age_secs` | **3.7** | quality gate 报告 freshness；3.4 **不读** |
| `prediction_horizon_secs` | **3.6 写入 artifact** | 在线读 artifact，不读 config（3.4 §4.2） |

完整 defer 索引：[`phase-03/README.md`](phase-03/README.md) §6。

### 2.7 `reports`

字段：

- `schedules`
- `default_top_n`
- `max_top_n`
- `report_horizon_secs`
- `publish_empty_reports`
- `report_ttl_secs`
- `ad_hoc_report_enabled`
- `delivery_policy`

Schedule（`ReportScheduleConfig`）：

- `schedule_id`
- `cadence` — **二选一**（Phase 4）：
  - `interval_secs`（`> 0`）→ `ScheduleCadence::Interval`
  - `cron` — `{ expr, timezone? }` → `ScheduleCadence::Cron`（6-field croner）
- `top_n`
- `market_filter_ref`
- `model_version_ref`
- `source_delay_secs`
- `enabled`

`deploy.quant.workers.report_expire_sweep_secs`（取代已删除的 `report_scheduler_tick_secs`）：
report TTL **expire sweep** cadence（默认 300），**不是** report 主触发器；主触发由
`ReportScheduleRunner` / `tokio-cron-scheduler` 承担（见 [04 §23](04-topn-report-and-recommendation.md#23-report-schedule-runnerphase-4-调度层)）。

### 2.8 `portfolio`

破坏式三段（政策 ≠ 状态；真实资金来自账户快照，`total_budget_usd` 仅为治理护栏，见 04.1）：

`portfolio.budget`：

- `total_budget_usd`（治理护栏 = 最大可部署上限；`equity = min(真实净清算, total_budget_usd)`，
  planner 总部署 room 另受真实 `available_usd` 约束）
- `min_recommendation_usd`
- `max_single_recommendation_usd`

`portfolio.constraints`：

- `max_market_exposure_usd`
- `max_event_exposure_usd`
- `max_category_exposure_usd`
- `max_correlated_exposure_usd`（本期写入 plan 快照，真正生效 Phase 5）
- `liquidity_usage_cap_pct`

`portfolio.sizing`（tagged enum `model`）：

- `Kelly { kelly_fraction, max_position_pct, target_reward_multiple, confidence_weighting,
  drawdown_scaling }`（默认；`confidence_weighting` 为置信度收缩曲线，`target_reward_multiple`
  为目标/止损倍数 R，用于反解 Kelly 胜率）

Kelly 是唯一 production sizing model；`confidence_weighting` 只作为 Kelly 分数的估计不确定性收缩输入。

### 2.9 `execution`

新语义，不复用旧 timeout/funnel/coalescer。

字段：

- `runtime_mode`
- `semi_auto`
- `auto_execution`
- `entry_order_policy`
- `exit_order_policy`
- `admission`
- `kill_switch`
- `capital`
- `reconciliation`

`semi_auto`：

- `approval_ttl_secs`
- `required_role`
- `allow_size_reduction`

`auto_execution`：

- `enabled`
- `max_orders_per_report`
- `max_total_usd_per_report`
- `min_score`
- `min_confidence`
- `require_shadow_passed`

### 2.10 `notification`

保留并扩展：

- Telegram。
- webhook。
- report published。
- report empty。
- model gate failure。
- execution halted。
- fact lag。

## 3. Config Validation

### 3.1 Common validation

- schema_version 必须为 3。
- unknown fields reject。
- Decimal string parse。
- all USD >= 0。
- all percentages in valid range。
- schedule id unique。
- model refs valid format。

### 3.2 Mode-aware validation

`report_only`：

- **要求 private key + `quant.account.funder`**（读真实抵押 + 持仓；报告强制建立在真实账户上，
  缺失则报告生成 fail closed）。
- **不**要求 CLOB order **submission** readiness（签名/下单仅 semi_auto/auto）。
- 要求 data ingest ready。
- 要求 report schedule valid。

`semi_auto`：

- 要求 credentials source configured。
- 要求 approval role exists。
- 要求 order client can be built。
- 要求 exit monitor enabled。

`auto_execution`：

- 要求 all semi_auto checks。
- 要求 active model published。
- 要求 auto policy budget > 0。
- 要求 kill switch closed。
- 要求 quality gates fresh。
- 要求 no blocking reconciliation。

## 4. Runtime Config UI

删除 UI groups：

- Detection。
- Execution old。
- Risk old。
- Settlement。

新增 UI groups：

- Selection。
- Data Quality。
- Features。
- Factors。
- Model。
- Reports。
- Portfolio。
- Execution Mode。
- Notifications。

所有 money-critical 字段必须带确认：

- portfolio budget。
- max recommendation size。
- auto execution enable。
- risk limits。
- model publish。
- mode switch。

## 5. Deployment

### 5.1 Docker

当前只有 ClickHouse compose。目标新增：

- `docker-compose.dev.yml`
- `docker-compose.quant.yml`
- Postgres。
- Redis。
- ClickHouse。
- quant-pivot service。
- optional UI。

### 5.2 Secrets

report_only 必需：

- Postgres password。
- ClickHouse password。
- Redis password if enabled。
- JWT secret。
- **Polymarket private key**（读真实抵押余额 + 派生 L2 读凭证；report_only ≠ dry-run）。
- **`quant.account.funder`**（Data API 持仓读取）。

semi_auto / auto_execution 额外（**签名/下单**用途，非读取）：

- 同一 Polymarket private key 用于 EIP-712 订单签名 + L2 写凭证。
- Polygon RPC if attribution requires on-chain evidence。

禁止在 production example 写真实 secrets。

### 5.3 Process roles

第一版可以单进程，内部 worker 分组：

- web。
- ingest。
- research。

### 5.3.1 Offline research jobs（3.5+）

Dataset plan/build 通过 Admin HTTP API（非 xtask CLI）：

```bash
# Dry plan (requires JWT + Accept-Api-Version: v1 + X-Acting-Role)
curl -X POST .../api/research/training-datasets/plan -d '{ ... }'

# Full build (PG + ClickHouse + deploy.research.artifact_root)
curl -X POST .../api/research/training-datasets/build -d '{ ... }'
```

契约见 [`phase-03/03.5.1-training-dataset-admin-api.md`](plans/quant-pivot/phase-03/03.5.1-training-dataset-admin-api.md)。
Phase 07 UI 对接同一组 endpoint。

- report。
- execution。

后续可拆进程，但数据库 schema 和 runtime config 不应依赖拆分。

## 6. CI 与质量门禁

保留：

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- architecture lint。
- MSRV。

新增：

- no old Endgame symbols gate。
- no old `ExecutionMode` gate。
- no runtime config v2 gate。
- no compatibility re-export gate。
- schema graph includes quant tables。
- runtime config schema snapshot。
- report payload snapshot。
- ClickHouse row snapshot。

删除/替换：

- old hot path SLO。
- old endgame e2e benchmark。
- old production Live gate。

## 7. Benchmark

删除 benchmark：

- `detect_with_direction`
- `pipeline_process` old。
- `scanner_scan_market`
- `funnel_immediate_dispatch`
- `execution_pipeline_paper_sync`
- old `pre_trade_pass`

新增 benchmark：

- `feature_build_market`
- `factor_compute_market`
- `model_score_market`
- `topn_select_1000`
- `report_build_topn`
- `portfolio_plan_1000`
- `order_intent_admission`
- `clickhouse_quant_fact_encode`

## 8. Observability

### 8.1 Metrics

新增：

- `quant_report_runs_total`
- `quant_report_run_duration_seconds`
- `quant_reports_published_total`
- `quant_reports_empty_total`
- `quant_feature_vectors_built_total`
- `quant_factor_values_built_total`
- `quant_model_runs_total`
- `quant_model_quality_gate_failures_total`
- `quant_recommendations_published_total`
- `quant_order_intents_total`
- `quant_execution_halted`
- `quant_fact_writer_lag_seconds`

### 8.2 Logs

关键 structured fields：

- `report_id`
- `recommendation_id`
- `model_version_id`
- `runtime_config_version_id`
- `market_selection_id`
- `order_intent_id`
- `market_id`
- `token_id`
- `mode`

### 8.3 Alerts

Critical：

- report schedule missed。
- model quality gate failed。
- fact writer lag over threshold。
- auto execution halted。
- reconciliation unresolvable。

Warning：

- empty report。
- data quality degraded。
- feature coverage low。
- shadow/live model divergence high。

## 9. Runbooks

旧 runbook 删除或归档：

- DryRun/Paper/Live 切换。
- Endgame detector tuning。
- settlement redeem。
- bankroll and risk metrics old。

新增 runbook：

- 启动 report_only。
- 发布模型。
- 运行 ad-hoc report。
- 切换 semi_auto。
- 审批 OrderIntent。
- 切换 auto_execution。
- kill switch。
- 处理 stale report。
- 处理 fact lag。
- 处理 unresolvable execution。

## 10. Migration 风险

- stored runtime config v2 必须一次性迁移或删除。
- env var 旧 key 会因 deny_unknown_fields 导致启动失败，必须同步清理。
- UI schema snapshot 必须更新。
- old tests 会大量失败，Phase 0 必须先删旧测试或改名隔离。
- old docs 引用 ADR-001/ADR-002 缺失，必须修正链接。
- old `system_runtime_state.execution_mode` 必须迁移为 `quant_runtime_mode`。

## 11. 验收标准

- `config/quant-pivot.toml` 不包含 old execution/settlement/endgame hotset。
- `RuntimeConfig::schema_version == 3`。
- runtime schema 不包含 `detection`、old `risk`、old `settlement`。
- report_only 启动要求 private key + `quant.account.funder`（账户读取；报告强制真实账户）；缺失则报告生成 fail closed。
- semi_auto/auto_execution preflight 额外覆盖签名/下单 credentials 与 order client readiness。
- CI gate 能阻止旧 Endgame 符号回流。
- benchmark 全部以 quant report path 为中心。

## 12. 第三方依赖治理

详细选型见 [`08-third-party-crates-and-ml-stack.md`](08-third-party-crates-and-ml-stack.md)。配置和运维层必须为重依赖提供 feature gate 和 rollout 策略。

### 12.1 Dependency Gate

每个新 crate 引入前必须记录：

- crate 名称。
- 使用模块。
- 引入 Phase。
- 是否默认 feature。
- MSRV。
- native dependency。
- binary size impact。
- license。
- fallback。

### 12.2 Feature Gate

建议 workspace feature：

```toml
[features]
default = []
quant-dataframe = ["polars"]
quant-ml-classical = ["smartcore"]
quant-ml-baseline = ["linfa"]
quant-ml-onnx = ["ort"]
quant-ml-deep = ["burn", "candle-core", "candle-nn"]
quant-lp-solver = ["good_lp"]
```

`report_only` 基础服务默认不启用 `quant-ml-onnx` 和 `quant-ml-deep`。

### 12.3 MSRV Gate

当前 workspace MSRV 是 1.85。已调研的风险：

- `ort` 最新 2.x rc 可能要求 1.88。
- 引入 `ort` latest 前必须选择：
  - 升级 workspace MSRV。
  - 使用较旧 `ort` rc 并承担维护风险。
  - 暂缓 ONNX，继续 weighted/classical model。

MSRV 变化必须作为独立 Phase 决策，不夹在业务改动里。

### 12.4 Native Dependency Gate

需要重点审查：

- `ort` 的 ONNX Runtime binaries / dynamic loading。
- `good_lp` 后端 solver，如 HiGHS/CBC。
- `polars` feature 组合导致的编译时间和二进制体积。
- `burn`/`candle` GPU backend。

生产 Docker 必须明确安装或复制 native runtime，不允许依赖开发机环境。

### 12.5 Spike Gate

以下 spike 通过前不得进入主路径：

- Polars feature build benchmark。
- SmartCore artifact serialization。
- Argmin deterministic optimization。
- GoodLP solver backend comparison。
- Ort MSRV/native deployment check。
- Burn/Candle deployment spike。
