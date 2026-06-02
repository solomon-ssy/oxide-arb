# Phase 5.4 — Typed Factor Builders, Statistical Materialization & Quality Gates

> **状态**: Production Design Target  
> **父计划**: `docs/plans/phase5-replay-analytics.md`  
> **前置依赖**: Phase 5.0-5.3  
> **覆盖原章节**: 7, 8, 9, 13, 18.5  
> **目标**: 将 evidence 转为五类 typed control factor draft，并用 production quality gates 决定 Draft/Candidate/Rejected/ReportOnly。因子必须可解释、保守、可审计、可 shadow。

---

## 0. 原则

Phase 5 第一版不默认引入黑盒 ML。默认交付 **StatisticallyMaterialized** 因子：从 PIT evidence 中估计失败率、偏差、分位数、漂移和风险收紧参数。

成熟度：

| 成熟度 | 生成方式 | 可发布性 | 目标 |
|---|---|---|---|
| `RuleSeeded` | 人工规则 + 历史 evidence 验证 | 可 Shadow，谨慎 Published | 快速把明显风险变成短 TTL 控制 |
| `StatisticallyMaterialized` | 批量 PIT evidence 估计分位数、失败率、置信区间 | Phase 5 默认生产形态 | 稳定、可解释、可审计 |
| `AdaptiveModelBacked` | walk-forward / online calibration / model challenger | 后续阶段，必须 shadow + champion/challenger | 样本充足后提高精度 |

任何 `AdaptiveModelBacked` 因子都必须降级为 typed payload 后才能被 live 消费。Hot path 不消费模型对象。

---

## 1. 通用 Builder Contract

每个 factor builder 必须：

1. 只消费 stage reports 和 artifact hashes。
2. 验证 required evidence coverage。
3. 生成 typed `FactorDimensions` 和 typed `FactorPayload`。
4. 附带 `FactorEvidence`：run id、stage report ids、coverage、sample、confidence interval、tail risk、config hash、code sha、dataset hash。
5. 只写 `Draft`、`Rejected` 或 `ReportOnly`，不能直接写 `Published`。
6. 对 insufficient evidence fail closed：不生成 Candidate。
7. 任何自动 payload 都只能收紧风险。

---

## 2. Factor Builders

### 2.1 `BucketRiskFactor`

用途：控制某个历史 endgame bucket 是否仍然可信。

维度：

```text
category
price_zone
duration_bucket
optional hours_to_settlement_bucket
optional neg_risk
optional fee_profile
```

依赖数据：

- `opportunity_detection`；
- `opportunity_audit`；
- PG `trade` / `position`；
- settlement truth；
- `calibration_snapshots`；
- calibration outcomes。

Payload:

```rust
BucketRisk {
    resolution_haircut_factor: Decimal,
    size_multiplier: Decimal,
    min_edge_bps_addon: Decimal,
    block_new_entries: bool,
}
```

统计方法：

```text
resolution_haircut_factor =
  clamp(observed_lower_confidence / predicted_mean, 0, 1)

min_edge_bps_addon =
  max(0, pnl_shortfall_bps_p50_or_p75)

size_multiplier =
  min(resolution_haircut_factor, drawdown_safe_multiplier)
```

写入条件：

- detector + settlement attribution 完成；
- PIT calibration 和 settlement coverage 达标；
- optimism gap 可解释；
- 样本不足时 shrink 到父 bucket，而不是生成过细 factor。

消费位置：

- detector/scorer: haircut effective resolution probability；
- sizer: cap size for weak buckets；
- audit: record factor ids and original vs adjusted values。

TTL: 7-30 days，默认 expiry fail neutral。

### 2.2 `ExecutionQualityFactor`

用途：控制某类盘口条件下 FOK 执行是否可靠。

维度：

```text
category
price_zone
spread_bucket
depth_bucket
book_age_bucket
latency_bucket
staleness_level
```

Payload:

```rust
ExecutionQuality {
    fill_probability_multiplier: Decimal,
    max_depth_usage_pct: Option<Decimal>,
    slippage_bps_addon: Decimal,
    min_liquidity_score: Option<Decimal>,
}
```

统计方法：

```text
fill_probability_multiplier =
  clamp(observed_fill_rate_lower_ci / predicted_fill_probability_mean, 0, 1)

slippage_bps_addon =
  max(0, observed_slippage_p75 - configured_slippage_assumption)
```

阶段推进：

1. 第一阶段使用 `StrictFok`，不从 partial fill 推乐观结论。
2. 第二阶段加入 `LatencyShiftedFok`。
3. 第三阶段用 shadow 复核高价值机会拒绝情况。

TTL: 1-7 days，默认 expiry fail neutral。

### 2.3 `PortfolioRiskFactor`

用途：当 sequence-level evidence 显示 capital pressure 或 drawdown risk 时收紧仓位。

维度：

```text
portfolio_regime
category
open_position_bucket
potential_loss_bucket
drawdown_bucket
settlement_backlog_bucket
```

Payload:

```rust
PortfolioRisk {
    global_size_multiplier: Decimal,
    category_size_multiplier: Option<Decimal>,
    daily_budget_multiplier: Decimal,
    max_open_positions: Option<usize>,
    kelly_fraction_multiplier: Decimal,
}
```

统计方法：

```text
global_size_multiplier =
  clamp(1 - tail_drawdown_excess_ratio, 0, 1)

daily_budget_multiplier =
  clamp(1 - daily_loss_pressure_ratio, 0, 1)

kelly_fraction_multiplier =
  min(global_size_multiplier, stability_multiplier)
```

要求：

- 使用 worst-of constraints，不用平均值掩盖尾部风险。
- 避免高频抖动，TTL 和 cadence 比 execution quality 更稳。
- 事故后 materialization 必须先 ReportOnly/Shadow。

TTL: 1-14 days，默认 expiry fail neutral。

### 2.4 `ReconciliationHealthFactor`

用途：当内部记账、CLOB collateral、链上 token balance 或 redeem 状态出现漂移时控制交易健康状态。

维度：

```text
account_scope
asset_scope
drift_severity
metrics_freshness_bucket
redeem_status_bucket
```

Payload:

```rust
ReconciliationHealth {
    trading_health: TradingHealth,
    size_multiplier: Decimal,
    require_manual_ack: bool,
    force_maintenance_mode: bool,
    fail_closed_after_secs: Option<u64>,
}
```

维护方式：

- 每次 reconciliation report 触发 health materialization。
- drift severity、metrics age、redeem failure 规则化映射到 payload。
- critical path 可 emergency Candidate/Published，但短 TTL、owner、audit、retrospective review 必须存在。

TTL: 30 minutes to 24 hours。Critical safety factor load/expiry 推荐 fail closed。

### 2.5 `MarketAnomalyFactor`

用途：当 settlement、oracle、book 或 price 行为异常时 block/cooldown market/event/category。

维度：

```text
market_id
event_id
category
anomaly_type
severity
```

Payload:

```rust
MarketAnomaly {
    block_market: bool,
    block_event: bool,
    category_cooldown_secs: Option<u64>,
    reason_code: String,
    manual_ack_required: bool,
}
```

来源：

- price reversal / abnormal book pattern；
- oracle mismatch；
- settlement delay；
- manual incident；
- category-level anomaly spike。

TTL: 1 hour to 7 days。Severe anomaly may fail closed while active。

---

## 3. 因子策略矩阵

| 因子 | 最低机会数 | 最低市场数 | 最低结算数 | 最低 L2 覆盖率 | 默认节奏 | 默认 TTL | Shadow 最低要求 |
|---|---:|---:|---:|---:|---:|---:|---|
| `BucketRiskFactor` | 100 | 20 | 50 | n/a | daily | 14d | 1d or 50 opportunities |
| `ExecutionQualityFactor` | 200 | 20 | n/a | 95% | hourly/daily | 3d | 6h or 100 opportunities |
| `PortfolioRiskFactor` | 100 | 10 | 30 | n/a | daily | 7d | 1d or one full trading cycle |
| `ReconciliationHealthFactor` | n/a | n/a | n/a | n/a | hourly/event | 2h | optional for critical |
| `MarketAnomalyFactor` | evidence-specific | 1 | optional | evidence-specific | event | 6h-3d | optional for severe |

低样本 category 可以保持 `Draft` 或 `ReportOnly`，直到 evidence 足够。禁止静默降低阈值强行 promote。

---

## 4. Consumption Formula Contract

### 4.1 Bucket risk

```text
effective_resolution_prob =
  base_resolution_prob
  * bucket_risk.resolution_haircut_factor

effective_min_edge_bps =
  base_min_edge_bps
  + bucket_risk.min_edge_bps_addon

bucket_size_cap =
  base_size
  * bucket_risk.size_multiplier
```

### 4.2 Execution quality

```text
effective_fill_probability =
  base_fill_probability
  * execution_quality.fill_probability_multiplier

effective_slippage_limit_bps =
  base_slippage_limit_bps
  - execution_quality.slippage_bps_addon
```

`effective_slippage_limit_bps` must not become negative；否则 validation rejects opportunity。

### 4.3 Portfolio risk

```text
factor_scaled_size =
  base_size
  * portfolio_risk.global_size_multiplier
  * portfolio_risk.category_size_multiplier.unwrap_or(1)
  * portfolio_risk.kelly_fraction_multiplier

factor_scaled_daily_budget =
  base_daily_budget
  * portfolio_risk.daily_budget_multiplier
```

### 4.4 Reconciliation health

```text
if reconciliation_health.force_maintenance_mode:
  reject all new entries
else:
  size = size * reconciliation_health.size_multiplier
```

### 4.5 Market anomaly

```text
if block_market(market_id) or block_event(event_id):
  reject before detector/scorer
if category_cooldown_active(category):
  skip market scan for category
```

---

## 5. Quality Gates

Quality gates 是 policy，不是 report。它们决定 Draft factor 是否可以进入 Candidate。

| Gate | 要求 |
|---|---|
| `PointInTimeGate` | all required PIT inputs resolved for the window |
| `CoverageGate` | coverage above factor-specific thresholds |
| `SampleGate` | enough markets/events/opportunities/settlements |
| `LeakageGate` | no current calibration/fee/config used for historical evidence |
| `StabilityGate` | factor value not dominated by one market/event |
| `TailRiskGate` | tail loss/drawdown evidence inside policy |
| `ConservativeGate` | automatic payload only tightens risk |
| `TtlGate` | expires_at present and compatible with materialization frequency |
| `RollbackGate` | publication path has known-good rollback target |
| `OwnerGate` | owner and approval policy are defined |

### 5.1 Coverage focus by factor

- Bucket risk：settlement truth coverage 和 calibration snapshot coverage 最重要。
- Execution quality：L2 tick coverage、book snapshot coverage、terminal execution audit 最重要。
- Portfolio risk：trade sequence、risk state、balance、potential loss、settlement coverage 最重要。
- Reconciliation health：reconciliation 和 token balance freshness 最重要。
- Market anomaly：emergency block 的 evidence completeness 可以较低，但 manual reason 和 TTL 必须存在。

---

## 6. Shadow Readiness

Published factors normally require shadow evidence：

- minimum live opportunities observed；
- would-reject count and reason distribution；
- would-size distribution；
- score delta distribution；
- no unexpected expansion of risk；
- no divergence beyond configured threshold。

Exceptions：

- `MarketAnomalyFactor` for severe active incident。
- `ReconciliationHealthFactor` for critical drift。

Emergency exception 仍然必须有 audit event、TTL、owner、retrospective review。

### 6.1 Shadow metrics

Every shadow factor must record：

```text
publication_id
factor_ids
opportunity_id
market_id
baseline_decision
shadow_decision
would_reject
would_size_usd
baseline_size_usd
size_delta_usd
baseline_score
shadow_score
score_delta
reason_codes
decided_at
```

Promotion review 必须包含：

- reject delta by reason；
- size delta distribution；
- score delta distribution；
- affected market/category distribution；
- false positive investigation for high-value opportunities；
- no evidence of risk expansion。

---

## 7. Champion / Challenger

每个 factor type 可以有一个 Published champion 和多个 Shadow challenger。

```text
champion = current Published publication
challenger = Shadow publication generated by newer materialization run
```

比较指标：

- reject delta；
- size delta；
- score delta；
- affected opportunity distribution；
- subsequent fill/miss；
- subsequent settlement outcome；
- false positive review。

Promotion 要求 challenger 在 safety metrics 上不劣于 champion，并且不能自动放大风险。

---

## 8. 禁止事项

- builder 直接写 Published。
- 训练逻辑直接进入 live hot path。
- live hot path 实时训练 factor。
- 用 shadow 结果自动放大风险。
- 用当前配置重算历史训练样本。
- payload 在决策路径中使用 untyped JSON。
- insufficient evidence 时通过降低阈值强行 Candidate。

---

## 9. 测试策略

| 测试 | 必需场景 |
|---|---|
| Factor builders | sufficient data、insufficient sample、insufficient coverage、non-conservative payload |
| Bucket risk | optimism gap、Wilson/beta-binomial lower confidence、parent bucket shrink |
| Execution quality | StrictFok、LatencyShiftedFok、slippage addon、depth cap |
| Portfolio risk | tail drawdown、capital pressure、worst-of constraints |
| Reconciliation health | stale metrics、cash/token drift、critical fail-closed payload |
| Market anomaly | incident evidence、manual reason、short TTL、category cooldown |
| Gates | PIT leakage、coverage threshold、owner/TTL/rollback missing |
| Shadow readiness | minimum opportunity count、delta distribution、risk expansion block |
| Dataset hash | same inputs same hash，fact repair new hash |

---

## 10. 退出条件

Phase 5.4 完成后必须满足：

1. 五类 factor builders 全部 strong typed。
2. 每个 factor 都有 evidence、TTL、owner、config hash、code sha、dataset hash/source refs。
3. Builder 在 insufficient PIT data/coverage/sample 时拒绝生成 Candidate。
4. Quality gates 是 blocking policy，不只是 warnings。
5. Automatic payload 不会放大风险。
6. Shadow readiness criteria 可计算且可审计。
7. Champion/challenger 比较指标已定义。
8. Factor-specific expiry/load failure policy 与 Phase 5.0 一致。

---

## 11. 阻止进入 Phase 5.5 的情况

- Payload 是 stringly typed。
- Gate failure 仍能进入 Candidate。
- 任何 builder 直接写 Shadow/Published。
- 自动 factor 放大风险且无人工审批 path。
- Evidence 缺 confidence interval/tail risk/coverage/sample。
- Shadow metrics 无法支持 promotion review。
