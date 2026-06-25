# 02 — Crate 重构与删除计划

> 状态：破坏式实施清单
>
> 原则：默认删除旧 Endgame/arb/trading 语义；只有明确服务 quant-pivot 的基础设施才保留。

## 0. 总策略

本重构不做“在旧系统旁边新增 quant 模块”。正确路径是先删除旧主语义，再以 quant-pivot 命名重建主路径。

执行原则：

- 不做 compatibility re-export。
- 不保留旧类型 alias。
- 不保留旧 runtime-config schema parser。
- 不保留旧 API 转发。
- 不保留旧文档作为 active plan。
- 不用旧 `Opportunity` 包装新 `Recommendation`。
- 不用旧 `Trade` 包装新 `OrderIntent`。

## 1. Workspace 目标形态

### 1.1 目标 crate 列表

建议重命名为 `quant-pivot-*`，但若项目名暂时保留 `quant-pivot`，模块和公开语义仍必须改成 quant-pivot。

| 目标 crate | 来源 | 命运 |
|---|---|---|
| `quant-pivot-models` | `quant-pivot-models` | 大幅重建，保留 typed IDs、money、schema、RBAC 基础 |
| `quant-pivot-error` | `quant-pivot-error` | 保留错误树模式，删除 trading/endgame error |
| `quant-pivot-api` | `quant-pivot-api` | 保留 Polymarket data/order 客户端，删除 endgame oracle/redeem-first 语义 |
| `quant-pivot-storage` | `quant-pivot-storage` | 保留 |
| `quant-pivot-repository` | `quant-pivot-repository` | 删除 trading repos，新增 quant repos |
| `quant-pivot-research` | `quant-pivot-control` + 新逻辑 | 负责 feature/factor/model/materialization |
| `quant-pivot-core` | `quant-pivot-core` | 重建 app orchestration，删除旧 hot execution path |
| `quant-pivot-risk` | `quant-pivot-risk` | 可选；建议重建为 portfolio/execution admission crate |
| `quant-pivot-web` | `quant-pivot-web` | 保留 web foundation，删除 trading routes |
| `quant-pivot-bin` | `quant-pivot-bin` | 重命名入口 |
| `quant-pivot-bench` | `quant-pivot-bench` | 全部替换 benchmark |
| `quant-pivot-test-support` | `quant-pivot-test-support` | 删除旧 fixtures，新增 quant fixtures |
| `quant-pivot-macros` | `quant-pivot-macros` | 保留 |
| `quant-pivot-xtask` | `quant-pivot-xtask` | 保留并改命令语义 |

### 1.2 必删 crate

| crate | 决策 | 原因 |
|---|---|---|
| `quant-pivot-algorithm` | 删除，不迁移 | 整个 crate 以 Endgame detector/scorer/calibration 为核心 |
| `quant-pivot-risk` | 删除后重建 | 当前 risk 是 pre-trade arb gate，不是 portfolio recommendation risk |
| `quant-pivot-bench` | 删除后重建 | benchmark 全部绑定 endgame hot path |

`quant-pivot-api` 不删除，因为平台仍为 Polymarket-only；但必须删除其“交易系统主路径”假设，保留为 Polymarket data/order SDK wrapper。

## 2. `quant-pivot-algorithm` 删除清单

路径：`crates/quant-pivot-algorithm/`

### 2.1 整体删除

删除整个 crate，包括：

- `src/endgame/`
- `src/calibration/`
- `src/fill_probability/`
- `src/scorer/`
- `src/pipeline.rs`
- `src/cooldown.rs`
- `src/detection_reject.rs`
- `src/urgency.rs`
- `tests/endgame_integration.rs`
- `tests/property_tests.rs` 中 Endgame property
- `tests/factor_pipeline.rs` 中 control factor 对 Endgame pipeline 的测试

### 2.2 可复制但不能原路径保留

| 旧模块 | 新归属 | 说明 |
|---|---|---|
| `walker.rs` | `quant-pivot-research/src/liquidity/orderbook_walker.rs` | 改为 liquidity/slippage feature builder |
| `staleness.rs` | `quant-pivot-research/src/data_quality/staleness.rs` | 改为 data quality policy |
| `fee.rs` | `quant-pivot-api` 或 `quant-pivot-research` | Polymarket fee estimate 可作为 feature，不是 arb gate |

### 2.3 禁止保留的类型

- `EndgameDetector`
- `EndgameDetectInput`
- `ConvergenceDirection`
- `InMemoryConvergenceTracker`
- `ConfidenceFusion`
- `ResolutionCalibrator`
- `BucketKey` as primary model key
- `EndgameScorer`
- `ScoredOpportunity`
- `OpportunityPipeline`
- `InMemoryEmissionCooldown`
- `DetectionRejectReason`

## 3. `quant-pivot-core` 重构清单

路径：`crates/quant-pivot-core/src/`

### 3.1 删除目录

| 目录 | 决策 | 替代 |
|---|---|---|
| `detection/` | 删除 | `quant/report_scheduler`, `quant/signal_pipeline` |
| `execution/` | 删除后重建 | `execution/order_intent`, `execution/order_lifecycle` |
| `post_trade/` | 删除 | `execution/attribution`, `execution/reconciliation` 新语义 |
| `exposure/` | 删除 | `portfolio/capital_allocation` |
| `trade_integrity/` | 删除 | `execution/recovery_gate` |
| `bridge/` | 删除 | 新 crate 不做旧 DI bridge |
| `control/` | 改名重建 | `research/model_refresher`, `research/factor_publication` |

### 3.2 部分保留目录

| 目录 | 保留内容 | 删除内容 |
|---|---|---|
| `pipeline/` | `BookStore`, `OrderBook`, `DataPipeline`, `MarketRegistry`, `BookGate`, `StalenessClassifier` | `DualBookAssembler` 的 Endgame 命名、market scan coupling |
| `service/` | `GammaService`, WS subscription base, readiness patterns | endgame hotset policy、equity/settlement service |
| `infra/` | async writer、periodic task、health checker、retry policy | oracle health tracker 的 settlement 语义 |
| `observability/` | metrics pattern、alert dispatcher、fact writers pattern | detection writer、execution audit writer、PnL report generator |
| `runtime_config/` | store/applicator/subscriber pattern | old section subscribers |
| `app/` | task registry、shutdown、bundle pattern | old bundle fields and runtime tasks |

### 3.3 AppContext 新 bundle

旧 bundle 删除：

- `RiskBundle`
- `ExecutionBundle`
- `TradingBundle`
- `SettlementBundle`
- `ControlFactorBundle`

新 bundle：

- `InfraBundle`
- `DataBundle`
- `ResearchBundle`
- `ReportBundle`
- `PortfolioBundle`
- `ExecutionIntentBundle`
- `GovernanceBundle`
- `RuntimeChannels`

### 3.4 Runtime tasks 替换

删除任务：

- data pipeline -> coalescer -> scanner -> funnel -> execution runner 链路。
- post-trade relay。
- reconciliation worker for old trade states。
- market settlement task。
- control factor old scheduler。
- risk metrics refresh tied to pre-trade gate。

新增任务：

- Polymarket data ingest。
- ClickHouse fact writer。
- feature materialization worker。
- model run worker。
- report scheduler。
- report publisher。
- order intent dispatcher。
- exit trigger monitor。
- attribution worker。
- model/factor publication refresher。

## 4. `quant-pivot-risk` 删除与重建

路径：`crates/quant-pivot-risk/`

### 4.1 删除

删除当前 crate 的 active API：

- `RiskEngine`
- `StaticRiskPipeline`
- `PreTradeContext`
- `SizedPreTradeInput`
- `RiskSnapshot` as pre-trade snapshot
- `QuarterKellyCalculator`
- `MultiConstraintSizer`
- `PotentialLossLedger`
- `DailyDirectionalBudgetCheck`
- `DirectionalConcentrationCheck`
- `RedeemRouteResolvableCheck`
- `DuplicateMarketCheck`
- `FeeSpendCheck` as arb gate
- `BlockingTradesCheck`

### 4.2 新建

新 `quant-pivot-risk` 只承担两个职责：

1. `PortfolioRiskEngine`：报告层组合风险裁剪。
2. `ExecutionAdmissionEngine`：执行层 admission gate。

新模块：

```text
quant-pivot-risk/src/
├── portfolio.rs
├── envelope.rs
├── admission.rs
├── kill_switch.rs
├── capital.rs
├── exposure.rs
├── audit.rs
└── types.rs
```

### 4.3 禁止迁移

旧 `QuarterKelly` 不能作为默认 sizing。quant-pivot 可以有 Kelly factor，但 sizing 必须先由 portfolio planner 结合置信度、流动性、相关性、最大损失、报告 horizon 统一裁剪。

## 5. `quant-pivot-control` 合并/改名

路径：`crates/quant-pivot-control/`

### 5.1 改名目标

`quant-pivot-control` 改为 `quant-pivot-research` 或 `quant-pivot-lab`。它不再是 live hot-path control-factor plane，而是研究、训练、回测、模型治理平面。

### 5.2 删除模块

| 模块 | 原因 |
|---|---|
| `evidence/detector.rs` | 绑定 EndgameDetector |
| `evidence/execution.rs` | 绑定 FOK trade evidence |
| `evidence/settlement.rs` | 绑定 hold-to-resolution |
| `factor/bucket.rs` | 绑定 calibration buckets |
| `scheduler/policy.rs` | 硬编码 control factor schedules |

### 5.3 保留并改造

| 模块 | 新语义 |
|---|---|
| `materialization/runner.rs` | feature/model/report materialization runner |
| `materialization/manifest.rs` | model run manifest |
| `materialization/pit.rs` | point-in-time input resolver |
| `governance/service.rs` | factor/model/report publication governance |
| `governance/hash.rs` | canonical hash |
| `gates/mod.rs` | model quality gates |
| `evidence/book.rs` | book feature evidence |
| `evidence/training.rs` | training dataset builder |

## 6. `quant-pivot-models` 删除/重建

路径：`crates/quant-pivot-models/src/`

### 6.1 删除 domain

- `domain/trading/opportunity.rs`
- `domain/trading/scored_snapshot.rs`
- `domain/trading/trade.rs`
- `domain/trading/settlement.rs`
- `domain/trading/integrity.rs`
- `domain/accounting/fee.rs` as trade accounting
- `domain/accounting/pnl.rs` as trade PnL report
- `domain/accounting/potential_loss.rs`
- `domain/governance/calibration.rs`
- `domain/risk/engine.rs` old risk snapshot
- `domain/risk/blacklist.rs` old blacklist semantics

### 6.2 删除 API DTO

- `domain/api/opportunity.rs`
- `domain/api/trade.rs`
- `domain/api/position.rs`
- `domain/api/pnl.rs`
- `domain/api/replay.rs`
- `domain/api/risk.rs` old risk API

### 6.3 删除 idens/entities

删除：

- `trade`
- `position`
- `calibration`
- `calibration_outcome`
- `resolution_event`
- `reconciliation_report`
- `potential_loss_ledger`
- `risk_fill_applied`
- `risk_state`
- `risk_audit_event`
- `blacklist_entry`
- `emergency_snapshot`
- `balance_snapshot`
- `accounting_period`

保留：

- `market`
- `event`
- RBAC tables
- `runtime_config_version`
- `runtime_config_activation`
- `operation_log`
- `seed_application`

重建：

- `report` -> `quant_recommendation_report`
- `system_runtime_state` -> 删除 `execution_mode`，新增 `quant_runtime_mode`
- `control_factor_*` -> `quant_factor_*` / `quant_model_*`

### 6.4 删除 runtime config

删除文件：

- `runtime_config/detection.rs`
- `runtime_config/execution.rs`
- `runtime_config/risk.rs`
- `runtime_config/settlement.rs`
- `runtime_config/redeem_routing.rs`

新增文件：

- `runtime_config/selection.rs`
- `runtime_config/data_quality.rs`
- `runtime_config/features.rs`
- `runtime_config/factors.rs`
- `runtime_config/model.rs`
- `runtime_config/reports.rs`
- `runtime_config/portfolio.rs`
- `runtime_config/execution_mode.rs`
- `runtime_config/execution.rs` 新语义
- `runtime_config/notification.rs` 保留扩展

## 7. `quant-pivot-api` 改造

路径：`crates/quant-pivot-api/src/`

### 7.1 保留

- `gamma/`
- `ws/`
- `clob/` market data read APIs
- `fees/` Polymarket fee formula
- rate limiter
- SDK error mapping

### 7.2 删除或降级

- `oracle/` 不能作为主路径；若保留，只作为 settlement label source。
- `ctf/` 不能作为报告系统核心；只在 execution attribution 需要时使用。
- `keystore/` 在**所有 mode** 加载（私钥用于读真实抵押余额 + 派生 L2 读凭证；report_only ≠ dry-run，报告强制建立在真实账户上）；私钥的**签名/下单**用途仅 `semi_auto` / `auto_execution`。

### 7.3 新 API 边界

新增 façade：

- `PolymarketMarketDataClient`
- `PolymarketOrderClient`
- `PolymarketOutcomeClient`

禁止让 research/model/report 代码直接依赖 SDK raw types。

## 8. `quant-pivot-web` 路由删除/新增

### 8.1 删除 routes

- `opportunities.rs`
- `trades.rs`
- `positions.rs`
- `pnl.rs`
- `replay.rs` old replay
- `risk.rs` old risk
- `control_factors.rs` old control factor semantics

### 8.2 保留 routes

- `auth.rs`
- `users.rs`
- `roles.rs`
- `menus.rs`
- `permissions.rs`
- `runtime_config.rs` with v3
- `operation_logs.rs`
- `system.rs` with quant runtime mode
- `health.rs`
- `metrics.rs`
- `ws.rs`

### 8.3 新增 routes

- `quant_reports.rs`
- `quant_recommendations.rs`
- `quant_models.rs`
- `quant_factors.rs`
- `quant_market_selection.rs`
- `quant_order_intents.rs`
- `quant_attribution.rs`
- `quant_research_runs.rs`

## 9. `quant-pivot-repository` 删除/新增

删除 traits/postgres：

- `traits/trading/*`
- `postgres/trading/*`
- `traits/accounting/*`
- `postgres/accounting/*`
- old `traits/risk/*`
- old `postgres/risk/*`
- old `traits/evidence/calibration.rs`
- old `postgres/evidence/calibration.rs`

新增：

```text
traits/quant/
├── selection.rs
├── feature.rs
├── factor.rs
├── model.rs
├── report.rs
├── recommendation.rs
├── order_intent.rs
├── execution_order.rs
└── attribution.rs
```

ClickHouse：

- 删除 `opportunity_*` repo 方法。
- 新增 quant facts insert/query。

## 10. `quant-pivot-bin` 与 xtask

### 10.1 bin

删除命令或语义：

- `serve` 中自动 trading engine 启动。
- mode preflight for DryRun/Paper/Live（旧 ExecutionMode 体系整体删除）。
- 旧「签名私钥才算就绪」的 mode-gated 凭证策略（纠偏：**所有 mode** 都需私钥 + funder
  读真实账户用于报告 sizing；私钥的**签名**用途仍仅 semi_auto/auto）。

新增命令：

- `serve`
- `migrate`
- `seed`
- `run-report --schedule-id`
- `run-model --model-spec`
- `backtest --model-version --window`
- `publish-model`
- `set-mode report_only|semi_auto|auto_execution`

### 10.2 xtask

删除：

- `test-network` 中只服务 endgame oracle 的测试入口。
- production Live gate 命名。

新增：

- `test-quant`
- `test-research`
- `test-report`
- `test-execution-intent`
- `bench-quant`

Dataset plan/build 走 Admin API（见
[`03.5.1`](phase-03/03.5.1-training-dataset-admin-api.md)），不在 xtask 暴露 research 子命令。

## 11. Config 删除清单

### 11.1 Deploy config

删除：

- `[execution.book_apply]`
- `[settlement.lifecycle]`
- `market_data.websocket.engine_endgame_window_hours`
- old `keys` required-by-mode policy

保留：

- `[polymarket]`
- `[polymarket.fees]`
- `[market_data.websocket]` transport fields
- `[market_data.gamma]`
- `[db]`
- `[cache]`
- `[web]`
- `[observability]`

新增：

- `[quant.workers]`
- `[quant.storage]`
- `[quant.execution]` only structural worker config

### 11.2 Runtime config

删除 sections：

- `detection`
- `execution` old
- `risk` old
- `settlement`
- `redeem_routing`

新增 sections：

- `selection`
- `data_quality`
- `features`
- `factors`
- `model`
- `reports`
- `portfolio`
- `execution`
- `notification`

## 12. Docs 删除/归档清单

直接标记 superseded，后续归档或删除：

- `docs/plans/phase3-algorithm.md`
- `docs/plans/phase4.1-risk.md`
- `docs/plans/phase4.2-core.md`
- `docs/plans/phase5-replay-analytics.md`
- `docs/plans/phase5.*`
- `docs/plans/phase7.3-business-markets-opportunities-trades.md`
- `docs/plans/phase7.4-risk.md`
- `docs/plans/phase7.5-analytics.md`
- `docs/operations/runbook.md`
- `docs/operations/live-production-guide.md`
- `docs/operations/bankroll-and-risk-metrics.md`
- `docs/operations/network-integration.md`

必须重写：

- `AGENTS.md`
- `.cursor/rules/quant-pivot-domain.mdc`
- `.cursor/rules/quant-pivot-rust-style.mdc` 中项目命名部分
- `.cursor/rules/quant-pivot-clickhouse-rust.mdc`
- `docs/persistence/schema-catalog.md` 的表目录
- `docs/models/dto-paradigm.md` 中示例资源名

## 13. Tests 与 Benchmark 删除清单

删除或重写：

- `crates/quant-pivot-algorithm/tests/*`
- `crates/quant-pivot-core/tests/*execution*`
- `crates/quant-pivot-core/tests/*funnel*`
- `crates/quant-pivot-core/tests/*scanner*`
- `crates/quant-pivot-core/tests/*settlement*`
- `crates/quant-pivot-risk/tests/*`
- `crates/quant-pivot-control/tests/*materialization*` 中 Endgame evidence cases
- `crates/quant-pivot-bench/benches/hot_paths.rs`
- `crates/quant-pivot-bench/benches/e2e_paths.rs`

新增：

- feature builder tests。
- factor scorer property tests。
- model run PIT tests。
- TopN selector tests。
- recommendation report snapshot tests。
- runtime mode gate tests。
- order intent approval tests。
- execution admission tests。
- attribution tests。
- quant benchmark SLO tests。

## 14. CI / scripts 修改

删除或重写：

- `scripts/check-production-gates.sh`
- `scripts/check-bench-slo.sh`
- `scripts/check-bench-regression.sh`
- old benchmark baselines

保留：

- `scripts/lint-architecture.sh` 框架，但规则全部更新。
- `scripts/check_no_entities.sh` 如仍适用。
- `scripts/pgo.sh` 可保留但目标 binary 改名。

新增 gates：

- no old endgame symbols。
- no old execution mode。
- no runtime config v2 parser。
- no compatibility re-export。
- no `Opportunity` / `ScoredOpportunity` active path。
- no `Trade` active path。

## 15. 删除完成验收

Phase 0 完成时必须满足：

- `rg "Endgame|ScoredOpportunity|OpportunityPipeline|DryRun|Paper|Live|ExecutionMode"` 在 active src 中无旧语义命中。
- `rg "pub use .*oxide_arb"` 无 compatibility re-export。
- runtime config schema version 只有 v3。
- old API routes 不在 route registry。
- old tables 不在 active schema graph。
- docs 中旧 phase 被明确标记 superseded。
- benchmark 不再引用 endgame hot path。
