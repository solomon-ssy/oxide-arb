# 06 — 配置、部署与运维设计

> 状态：生产级目标设计（与实现对齐）
>
> 目标：用 deploy-config + runtime-config **v7** 支撑 quant-pivot，删除旧 Endgame trading 配置面。

本文件描述**当前实现态**：deploy config 树见 [`crates/quant-pivot-models/src/config/`](../../../crates/quant-pivot-models/src/config/mod.rs)，runtime config v7 见 [`crates/quant-pivot-models/src/runtime_config/`](../../../crates/quant-pivot-models/src/runtime_config/mod.rs)。

## 0. 配置分层

两层配置模型：

- **Deploy config**（`config/quant-pivot.toml`）：进程启动绑定，改动需要重启。连接、池、shard/channel、worker cadence、credential source、web server、logging。
- **Runtime config v7**：受治理、版本化（`runtime_config_version` WORM）、热激活（`ArcSwap` + applicator push），存 Postgres，只经治理 API 修改。

禁止：

- 把可热更新策略参数放进 TOML。
- 把连接池、worker 并发、credential source 放进 runtime config。
- 保留旧 runtime-config parser（v7 之前的 schema 一律拒绝；仅 bootstrap 迁移路径读旧文档，见 §10）。
- 允许未知 key 静默通过（deploy + runtime 全部 `deny_unknown_fields`）。

## 1. Deploy Config

根类型 [`DeployConfig`](../../../crates/quant-pivot-models/src/config/mod.rs)。每个 section 与 `[section]` 1:1。加载优先级（后者覆盖前者）：`QUANT_PIVOT__*` env → `quant-pivot.local.toml` → `quant-pivot.toml` → 编译内默认。

### 1.1 Section 清单（真实结构）

| Section | 字段 | 说明 |
|---|---|---|
| `[polymarket]` | `clob_base_url`, `clob_ws_url`, `chain_id` | CLOB/WS endpoint、链 id（必须 137） |
| `[polymarket.onchain]` | `rpc_url`, `rpc_timeout_ms` | Polygon JSON-RPC；`rpc_timeout_ms` 是 CTF oracle/redeem 的硬超时（reqwest 默认无超时） |
| `[polymarket.relayer]` | `base_url`, `api_key`, `api_key_address`, `request_timeout_ms` | proxy/gnosis_safe 的 gasless money-moving；EOA 忽略 |
| `[polymarket.fees]` | `exponent`, `unknown_category_rate`, `category_rates{}` | fee = C×feeRate×p×(1−p)^exponent |
| `[market_data.websocket]` | `reconnect_delay_ms`, `max_reconnect_delay_ms`, `max_subscriptions_per_connection`, `engine_max_subscription_tokens`, `engine_subscription_window_hours` | WS transport + 引擎订阅 hotset cap/窗口 |
| `[market_data.gamma]` | `base_url`, `full_sync_interval_secs`, `page_size` | market catalog sync |
| `[market_data.data_api]` | `base_url`, `page_size`, `size_threshold` | keyless 持仓读取（report capital base） |
| `[observability]` | `log_level`, `log_json` | logging（metrics 常开于 `GET /metrics`） |
| `[db.postgres]` | 连接/池/GUC（17 字段） | 权威状态 |
| `[db.clickhouse]` | `url`, `database`, `user`, `password`, `flush_interval_secs`, `batch_size`, `max_concurrent_inserts` | facts/analytics |
| `[cache]` / `[cache.redis]` / `[cache.moka]` | 见 struct | Redis L2 + Moka L1 |
| `[keys]` | `private_key` | **唯一签名凭证**；CLOB L2 由 SDK 在 connect 时派生 |
| `[web]` / `[web.jwt]` | 见 struct | admin API/WS + JWT |
| `[quant.workers]` | `report_expire_sweep_secs`, `intent_expire_sweep_secs`, `execution_dispatch_secs`, `execution_breaker_tick_secs`, `equity_snapshot_secs` | 后台 worker cadence（restart-bound） |
| `[quant.account]` | `funder`, `wallet_kind` | Data API 持仓读取地址 + 钱包形态 |
| `[research]` | `artifact_root` | artifact store 根目录 |

### 1.2 相对旧设计删除的 deploy 字段

| Path | 删除原因 |
|---|---|
| `[keys].source` / `[keys].keystore_path` / `KeySource` | keystore 未实现；只用 `private_key`。凭证来源不再有枚举，删除死设计 |
| `[execution.book_apply]` / `[settlement.lifecycle]` | 旧 execution/settlement worker 面 |
| 旧 `[keys]` mode-aware required policy / `load_credentials_in_report_only` | 私钥不再 mode-gated——**所有 mode 都加载私钥用于读真实账户**（report_only ≠ dry-run）；仅签名/下单为 semi_auto/auto |

### 1.3 关键语义

- **私钥所有 mode 都需要**（读真实抵押余额 + 派生 L2 读凭证）；**签名/下单**用途仅 `semi_auto`/`auto_execution` preflight 才要求。
- `quant.account.funder`：`eoa` 必须等于 signer EOA 派生地址；`proxy`/`gnosis_safe` 必须等于 CREATE2 推导地址（启动校验）。
- `proxy`/`gnosis_safe` 在升级到 order-submitting mode 时 preflight 硬要求 `[polymarket.relayer].api_key` + `api_key_address`。
- `polymarket.onchain.rpc_timeout_ms` 已接线进 alloy provider 的 reqwest client：redeem/oracle 调用超时即失败，绝不无限挂起。

## 2. Runtime Config v7

根类型 [`RuntimeConfig`](../../../crates/quant-pivot-models/src/runtime_config/mod.rs)，`RUNTIME_CONFIG_SCHEMA_VERSION = 7`（以代码为准）。

> **schema 版本历史**：v7 基线 → … → v9（Phase 06.1）→ **v10**（Phase 10.7：移除线上死配置 `model.prediction_horizon_secs`→训练 API；`data_quality.min_book_depth_usd` 迁移正名为 `execution.entry_order_policy.min_entry_book_depth_usd`）。

```text
RuntimeConfig {
  schema_version: 10,
  selection,
  data_quality,
  features,
  factors,
  model,
  quality_gate,
  training,
  reports,
  portfolio,
  execution,
  notification
}
```

### 2.1 `selection`

`enabled_categories`, `min_liquidity_usd`, `min_volume_24h_usd`, `max_spread_bps`, `allow_near_resolution`, `min_time_to_resolution_secs`, `max_time_to_resolution_secs`, `max_selection_size`。

人工禁用不属于 runtime-config；唯一权威是 catalog `MarketStatus::ManuallyBlocked`，经 governed block/unblock API 修改，由 selection/admission fail-closed 消费。

### 2.2 `data_quality`

`max_book_age_ms`, `max_ingest_lag_ms`, `max_feature_bucket_age_secs`, `reject_crossed_books`, `reject_empty_books`, `feature_staleness_policy`, `max_stale_book_ratio_bps`。

> **v10 迁移**：`min_book_depth_usd` 语义错位（不参与 live 订单簿质量，仅经冻结 `entry_plan.min_depth_usd` 在准入 `LiquidityDepthCheck` 生效），已迁移正名为 `execution.entry_order_policy.min_entry_book_depth_usd`（见 §2.10）。

`max_ingest_lag_ms` 衡量入库管道 enqueue→ClickHouse flush-ack 背压（实时数据质量 / 执行准入 / 选市）；`max_feature_bucket_age_secs` 衡量物化特征桶陈旧度（训练 / 回测 / 在线特征）。二者语义独立，不可混用。

> **v7 删除**：`allow_degraded_domain_features`、`knowledge_lag_secs`（前者从未消费；后者只作 ad-hoc 报告 source-delay 回退，已合并——见 §2.8）。exit 再推断的 as-of 回退改用 `max_feature_bucket_age_secs`。
> **拆分**：旧 `max_fact_lag_secs` 拆为 `max_ingest_lag_ms`（管道背压）与 `max_feature_bucket_age_secs`（特征桶陈旧度）。

### 2.3 `features`

`feature_schema_version`, `enabled_feature_families`, `required_features`, `bar_windows_secs`, `momentum_windows_secs`, `volatility_windows_secs`, `depth_levels`, `max_concurrent_market_resolves`。

> **v7 删除**：`domain_feature_policy`（从未消费）。

### 2.4 `factors`

`enabled_factor_families`（仅 generic 家族；domain 因子由 category 路由）、`factor_weights`、`min_factor_confidence`、`missing_factor_policy`。因子集合身份经 `factor_schema_hash` 绑定模型 artifact。

### 2.5 `model`

`active_model_version_id`, `shadow_model_version_id`, `active_exit_model_version_id`, `min_model_confidence`, `min_quality_gate_age_secs`, `candidate_score_floor`, `shadow_diff_threshold`。

> **v10 移除**：`prediction_horizon_secs` 是线上死配置（仅训练产物 authoring 用，在线推断读冻结 artifact 的 horizon），已从 runtime-config 移除并迁到训练 API `TrainModelRequest.prediction_horizon_secs`（校验 + 默认 86400），由 trainer 写入 artifact。

### 2.6 `quality_gate`

`min_sample_count`, `min_label_coverage`, `min_critical_feature_coverage`, `max_drawdown`, `min_liquidity_exit_feasibility`, `min_shadow_overlap_stability`, `min_rank_ic`, `max_category_concentration`, `required_shadow_window_secs`。模型发布/回滚/数据集晋升门禁消费。

### 2.7 `training`

`max_book_staleness_ms`, `min_exit_depth_usd`。离线数据集构建参数（PIT 回看窗口远宽于在线 gate）。

### 2.8 `reports`

`schedules`, `max_top_n`, `fallback_horizon_secs`, `publish_empty_reports`, `entry_window_ratio`, `ad_hoc_report_enabled`, `delivery_policy`。

Schedule（`ReportScheduleConfig`）：`schedule_id`, `cadence`（`interval_secs` 或 `cron{expr, timezone?}`）, `top_n`, `knowledge_lag_secs`, `enabled`。

> **v7 破坏式合并**：`schedule` 层是 `top_n` / `knowledge_lag` 的**唯一权威**。删除 `reports.default_top_n` 与 `data_quality.knowledge_lag_secs`。**ad-hoc 报告必须在请求中显式携带 `top_n` + `knowledge_lag_secs`**，缺失即 fail closed（无配置回退）。schedule 上删除死占位符 `market_filter_ref` / `model_version_ref`。

### 2.9 `portfolio`

三段（政策 ≠ 状态；真实资金来自账户快照，`total_budget_usd` 仅治理护栏）：

- `budget`：`total_budget_usd`（治理护栏；`equity = min(真实净清算, total_budget_usd)`）、`min_recommendation_usd`、`max_single_recommendation_usd`。
- `constraints`：`max_market_exposure_usd`、`max_event_exposure_usd`、`max_category_exposure_usd`、`max_correlated_exposure_usd`、`liquidity_usage_cap_pct`、`correlation{enabled, lookback_days, min_observations, cluster_threshold}`。
- `sizing`（Kelly 唯一）：`kelly_fraction`、`max_position_pct`、`target_reward_multiple`、`confidence_weighting`、`drawdown_scaling`。
- `optimizer`：`solver`（`microlp` 默认 / `highs` 可选 native）、`integer_inclusion`、`objective_return_weight`。求解阶梯 MILP → relaxation → 空 plan，无 wall-clock 超时（确定性重放）。

### 2.10 `execution`

`semi_auto`, `auto_execution`, `entry_order_policy`, `exit_monitor`, `kill_switch`, `capital`, `reconciliation`, `settlement_redeem`, `attribution`, `breaker`。

- `semi_auto`：`approval_ttl_secs`、`allow_size_reduction`。
- `auto_execution`：`enabled`、`max_orders_per_report`、`max_total_usd_per_report`、`min_score`、`min_confidence`。
- `entry_order_policy`：`max_slippage_bps`、`allow_market_orders`、`min_entry_book_depth_usd`（**v10 从 `data_quality.min_book_depth_usd` 迁移正名**；冻结进 `entry_plan.min_depth_usd`，准入 `LiquidityDepthCheck` 消费）。
- `exit_monitor`：`enabled`、`monitor_secs`、`signal_recheck_secs`、`signal_invalidation_ratio`、`signal_reinference{enabled, shadow_mode}`。
- `capital`：`max_reserved_usd`、`max_open_intents`（**v7 起真正生效**，见 §3.3）。
- `reconciliation`：`enabled`、`interval_secs`、`stale_open_secs`。
- `settlement_redeem`：`enabled`、`interval_secs`、`batch_size`、`max_attempts`、`retry_backoff_secs`、`confirmation_blocks`、`allow_during_emergency`、`hold_to_resolution_enabled`、`hold_to_resolution_within_secs`。
- `attribution`：`enabled`、`sweep_secs`、`batch_size`。
- `breaker`（`ExecutionBreakerConfig`）：`venue_consecutive_failures_to_degrade`、`venue_consecutive_failures_to_halt`、`venue_error_rate_bps_to_halt`、`venue_min_window_samples`、`venue_window_secs`、`cooldown_secs`、`daily_realized_loss_cap_usd`。**v7 起真正热更新**（见 §3.4）。

> **v7 删除**：`exit_order_policy` 整块（退出滑点由 Kelly exit ladder / emergency_exit 决定）、`admission{min_score, min_confidence, require_fresh_features}`（分数/置信度门槛由 `auto_execution.*` + 结构化 freshness 承担）、`auto_execution.require_shadow_passed`、`entry_order_policy.confirmation_window_secs`（从未在 admission 校验，连同 `EntryPlan.confirmation_window_secs` 一并删除）、`execution.runtime_mode`（运营 mode 权威在 `system_runtime_state` / `RuntimeModeHandle`，非 runtime config）。

### 2.11 `notification`

`telegram{bot_token, chat_id}`、`webhook{url}`、`policies{report_published}`。

> **v7 删除**：`policies.execution_halted`、`policies.config_activated`（从未消费；执行停止/配置激活通过其它路径通知）。

## 3. Config Validation 与热更新闭环

### 3.1 Common validation（[`validation.rs`](../../../crates/quant-pivot-models/src/runtime_config/validation.rs)）

- `schema_version` 必须为 10；unknown fields reject；Decimal string parse；USD ≥ 0；比例在合法区间；schedule id 非空；cadence 结构合法。

### 3.2 Mode-aware validation（deploy + preflight）

- `report_only`：要求 private key + `quant.account.funder`（读真实抵押 + 持仓）；不要求下单 readiness；要求 data ingest ready、report schedule valid。
- `semi_auto`：要求 credentials source、approval role、order client、exit monitor。
- `auto_execution`：semi_auto 全部 + active model published + auto policy budget > 0 + kill switch closed + quality gates fresh + no blocking reconciliation。

### 3.3 Capital 风控门（admission #21 / #22）

`execution.capital.{max_open_intents, max_reserved_usd}` 由 admission engine 消费（[`checks.rs`](../../../crates/quant-pivot-core/src/execution/admission/checks.rs)）：

- **#21 `MaxOpenIntents`**：`0` 禁用；否则 `open_intent_count > max_open_intents` → `Deny`（`open_intent_count` 由 `OrderIntentRepository::count_open()` 在 build 时读取，含 `OrderIntentStatus::OPEN` 全集，被审 intent 已计入，无 off-by-one）。
- **#22 `MaxReservedCapital`**：`0` 禁用；否则 `account.reserved_usd > max_reserved_usd` → `Deny`。

admission 由 20 条增至 **22 条固定顺序检查**，两门紧邻 `CapitalBudget`（#10）之后。

### 3.4 Breaker 热更新

`execution.breaker.*` 是运行时配置，须**真正热更新**：[`ExecutionBreaker`](../../../crates/quant-pivot-core/src/execution/breaker.rs) 将阈值置于 `ArcSwap<BreakerThresholds>`，applicator 激活时 push `reload(&config.execution.breaker)`，**只换阈值**，保留滚动失败窗口与日内亏损累加器状态。latched `execution_halted` 仍须 operator ack 解除。

### 3.5 热更新机制

`RuntimeConfigStore`（`ArcSwap<RuntimeConfig>`）承载 live active 快照；激活路径 `POST /api/runtime-config/versions/{id}/activate` → 校验 → 持久化激活链 → `RuntimeConfigApplicator::apply`：先 push 订阅者（market filter/cache、data quality、alerts、weight overlay、WS 订阅、**execution breaker**），再 swap store，最后 rebuild report schedule。worker（recon/exit_monitor/settlement/attribution）每轮重读 `store.current()`。报告构建按 `trigger_time` 从 PG 取 point-in-time 版本（回放确定性）。

## 4. Runtime Config UI

UI groups（[`schema/ui.rs`](../../../crates/quant-pivot-models/src/schema/ui.rs)，经 `preferences_schema` 校验与 struct 1:1）：Selection、Data Quality、Features、Factors、Model、Quality Gate、Training、Reports、Portfolio、Execution、Notifications。

money-critical 字段（`money` 语义 + 确认）：portfolio budget、max recommendation size、capital caps、auto execution enable、breaker 亏损上限、mode switch、model publish。

## 5. Deployment

### 5.1 Docker

目标：`docker-compose.dev.yml`、`docker-compose.quant.yml`（Postgres、Redis、ClickHouse、quant-pivot service、optional UI）。

### 5.2 Secrets

report_only 必需：Postgres/ClickHouse/Redis 密码、JWT secret、**Polymarket private key**（读真实抵押 + 派生 L2 读凭证）、**`quant.account.funder`**。

semi_auto / auto_execution 额外（签名/下单）：同一 private key 用于 EIP-712 签名 + L2 写；proxy/gnosis_safe 需 relayer 凭证；Polygon RPC（attribution/redeem）。

禁止在 production example 写真实 secrets。

### 5.3 Process roles

第一版单进程，内部 worker 分组：web、ingest、research、report、execution。后续可拆进程，但数据库 schema 与 runtime config 不应依赖拆分。

### 5.3.1 Offline research jobs（3.5+）

Dataset plan/build 经 Admin HTTP API（非 xtask CLI）：`POST /api/research/training-datasets/{plan,build}`。契约见 [`phase-03/03.5.1-training-dataset-admin-api.md`](phase-03/03.5.1-training-dataset-admin-api.md)。

## 6. CI 与质量门禁

保留：`cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace`、architecture lint、MSRV。

新增/维护：no old Endgame symbols gate、no old `ExecutionMode` gate、no prior-schema runtime config gate、no compatibility re-export gate、schema graph includes quant tables、**runtime config schema snapshot（schema_version=7）**、report payload snapshot、ClickHouse row snapshot。

## 7. Benchmark

以 quant report path 为中心：`feature_build_market`、`factor_compute_market`、`model_score_market`、`topn_select_1000`、`report_build_topn`、`portfolio_plan_1000`、`order_intent_admission`、`clickhouse_quant_fact_encode`。删除旧 endgame/paper benchmark。

## 8. Observability

### 8.1 Metrics

`quant_report_runs_total`、`quant_report_run_duration_seconds`、`quant_reports_published_total`、`quant_reports_empty_total`、`quant_feature_vectors_built_total`、`quant_factor_values_built_total`、`quant_model_runs_total`、`quant_model_quality_gate_failures_total`、`quant_recommendations_published_total`、`quant_order_intents_total`、`quant_execution_halted`、`quant_fact_writer_lag_seconds`、`quant_execution_breaker_trip`（venue/daily_loss）、admission_denied（按 check id，含 `max_open_intents`/`max_reserved_capital`）。

### 8.2 Logs

关键 structured fields：`report_id`、`recommendation_id`、`model_version_id`、`runtime_config_version_id`、`market_selection_id`、`order_intent_id`、`market_id`、`token_id`、`mode`。

### 8.3 Alerts

Critical：report schedule missed、model quality gate failed、fact writer lag、execution halted、reconciliation unresolvable。Warning：empty report、data quality degraded、feature coverage low、shadow/live divergence high。

## 9. Runbooks

新增：启动 report_only、标准二元 CTF auto-redeem、发布模型、运行 ad-hoc report（**须显式提供 top_n + knowledge_lag_secs**）、切换 auto_execution、Auto-execution 恢复（unresolvable / latched kill-switch）、审批 OrderIntent、kill switch、处理 stale report / fact lag / unresolvable execution。

## 10. Bootstrap 与 schema 变更

- 项目尚未正式运行，**不做** runtime config schema 迁移；bootstrap（[`governance.rs`](../../../crates/quant-pivot-core/src/app/bundles/governance.rs) `ensure_runtime_config_activation`）在直接解析失败后 fail-closed 到默认 v7 文档并激活。
- env var 旧 key（如 `QUANT_PIVOT__KEYS__SOURCE`）会因 `deny_unknown_fields` 导致启动失败，必须同步清理。
- UI schema snapshot 必须更新（schema_version=7）。
- 旧 tests 大量失败者需删除或改名隔离。
- `EntryPlan.confirmation_window_secs` 删除影响 report payload snapshot，须重生成。

## 11. 验收标准

- `config/quant-pivot.toml` / `production.example.toml` 与 `DeployConfig` struct **1:1**，无 old execution/settlement/endgame hotset、无 `[keys].source`。
- `RuntimeConfig::schema_version == 7`；runtime schema 不含 `detection`、old `risk`、old `settlement`、`exit_order_policy`、`admission`、`domain_feature_policy`、`default_top_n`。
- report_only 启动要求 private key + `quant.account.funder`；缺失则报告生成 fail closed。
- semi_auto/auto_execution preflight 额外覆盖签名/下单 credentials 与 order client readiness。
- admission 为 22 条；`capital.{max_open_intents, max_reserved_usd}` 生效（0=禁用）。
- `execution.breaker.*` 激活即热更新，无需重启。
- ad-hoc 报告缺 `top_n` / `knowledge_lag_secs` 时 fail closed。
- CI gate 能阻止旧 Endgame 符号回流。

## 12. 第三方依赖治理

详细选型见 [`08-third-party-crates-and-ml-stack.md`](08-third-party-crates-and-ml-stack.md)。配置和运维层必须为重依赖提供 feature gate 和 rollout 策略。

### 12.1 Dependency Gate

每个新 crate 引入前记录：crate 名、使用模块、引入 Phase、是否默认 feature、MSRV、native dependency、binary size impact、license、fallback。

### 12.2 Feature Gate

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

### 12.3 MSRV / Native / Spike Gate

MSRV 变化作为独立 Phase 决策；`ort`/`good_lp`/`polars`/`burn`/`candle` 的 native 依赖须审查；生产 Docker 必须明确安装或复制 native runtime。相关 spike（Polars/SmartCore/Argmin/GoodLP/Ort/Burn）通过前不得进入主路径。
