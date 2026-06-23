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
| `[keys]` mode-aware required policy | report_only 不需要私钥 |

### 1.3 新增 deploy sections

```toml
[quant.workers]
report_scheduler_tick_secs = 30
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
load_credentials_in_report_only = false
```

私钥配置仍可存在，但只有 `semi_auto` 和 `auto_execution` preflight 才要求。

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

- `enabled_factor_families`
- `factor_weights`
- `min_factor_confidence`
- `missing_factor_policy`
- `published_factor_set_id`
- `shadow_factor_set_id`

### 2.6 `model`

字段：

- `active_model_version_id`
- `shadow_model_version_id`
- `min_model_confidence`
- `min_quality_gate_age_secs`
- `prediction_horizon_secs`
- `candidate_score_floor`
- `shadow_diff_threshold`

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

Schedule：

- `schedule_id`
- `interval_secs` 或 cron。
- `top_n`
- `market_filter_ref`
- `model_version_ref`
- `source_delay_secs`
- `enabled`

### 2.8 `portfolio`

字段：

- `total_budget_usd`
- `max_single_recommendation_usd`
- `max_market_exposure_usd`
- `max_event_exposure_usd`
- `max_category_exposure_usd`
- `max_correlated_exposure_usd`
- `min_recommendation_usd`
- `liquidity_usage_cap_pct`
- `confidence_size_curve`
- `drawdown_multiplier`

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

- 不要求 private key。
- 不要求 CLOB order readiness。
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

semi_auto / auto_execution 额外：

- Polymarket private key。
- Polygon RPC if attribution requires on-chain evidence。

禁止在 production example 写真实 secrets。

### 5.3 Process roles

第一版可以单进程，内部 worker 分组：

- web。
- ingest。
- research。
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
- report_only 启动不需要 private key。
- semi_auto/auto_execution preflight 覆盖 credentials/order client。
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
