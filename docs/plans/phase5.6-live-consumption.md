# Phase 5.6 — Live ControlFactorSnapshot Consumption & Hot Path Integration

> **状态**: Production Design Target  
> **父计划**: `docs/plans/phase5-replay-analytics.md`  
> **前置依赖**: Phase 5.0-5.5  
> **覆盖原章节**: 10.1-10.5, 10.7, 10.8, 17, 18.8  
> **目标**: 将 Published/Shadow publication 以 immutable `ControlFactorSnapshot` 形式接入 detector、scorer、risk、sizer 和 audit。Hot path 只读内存快照，不查询 CH/PG。

---

## 0. 工作范围

### 0.1 交付物

| 交付物 | 说明 |
|---|---|
| `ControlFactorSnapshot` | active publication 编译后的只读索引 |
| `ControlFactorProvider` | hot path 获取 snapshot 的抽象 |
| Core factor refresher | startup load、periodic refresh、notify refresh、expiry/load failure |
| Shadow snapshot | 独立 shadow snapshot 和 delta writer |
| Detector/scorer 接入 | market anomaly、bucket risk、execution quality |
| Execution/risk/sizer 接入 | stricter validation、factor-adjusted probability、factor constraints |
| Audit trace | `AppliedControlFactor` 写入 detection/execution audit |

### 0.2 非目标

- 不实现 materialization。
- 不实现 governance API。
- 不做 live auto-exit。
- 不允许 hot path 查询 CH/PG。

---

## 1. Snapshot Model

```rust
pub struct ControlFactorSnapshot {
    pub publication_id: FactorPublicationId,
    pub loaded_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub source_version: String,
    pub bucket_risk: BucketRiskIndex,
    pub execution_quality: ExecutionQualityIndex,
    pub portfolio_risk: PortfolioRiskState,
    pub reconciliation_health: ReconciliationHealthState,
    pub market_anomalies: MarketAnomalyIndex,
}
```

Snapshot build 必须：

- load active publication；
- validate factor schema version；
- validate TTL；
- validate publication hash/payload hashes；
- validate conservative payload constraints；
- build typed indexes；
- reject unknown factor type/schema。

Safety factor load failure 在 Live mode 下必须按 policy fail closed。

---

## 2. Refresh

### 2.1 Startup

1. Load active Published publication from Postgres。
2. Validate schema、TTL、hashes、payload constraints。
3. Build `ControlFactorSnapshot`。
4. Store in `ArcSwap`。
5. If safety factor load fails and policy is fail closed，refuse Live startup。

### 2.2 Periodic refresh

- 每 30-120 秒轮询 publication metadata。
- 如果 version 未变化，不做任何事。
- 如果 version 变化，加载完整 factor set 并校验。
- 校验成功后原子替换 `ArcSwap`。
- refresh 失败时继续使用未过期的旧 snapshot。
- 旧 snapshot 过期且 safety policy fail closed 时，risk gate must reject new entries。

### 2.3 Notify refresh

- Publication/rollback 发出事件。
- Notify 只用于加速 refresh，periodic polling 是兜底。
- Notify handler 不得阻塞 hot path。

---

## 3. Hot Path Consumers

| 阶段 | 因子 | 行为 |
|---|---|---|
| Market scan | `MarketAnomalyFactor` | block market/event/category cooldown |
| Detector/scorer | `BucketRiskFactor` | haircut resolution probability / require higher edge |
| Scorer | `ExecutionQualityFactor` | discount fill probability / score |
| Risk gate | `ReconciliationHealthFactor`, `MarketAnomalyFactor` | reject / maintenance / degraded mode |
| Sizer | `BucketRiskFactor`, `PortfolioRiskFactor`, `ReconciliationHealthFactor` | 限制 size、budget、Kelly |
| Audit | all applied factors | record publication id、factor ids、input/output values |

---

## 4. Detector / Opportunity Pipeline

当前接入点：

- `crates/oxide-arb-algorithm/src/pipeline.rs`
  - `OpportunityPipeline::process_ref`
  - current flow: cooldown -> staleness -> detector -> min profit -> depth usage -> scorer -> min score -> emit
- `crates/oxide-arb-algorithm/src/endgame/detector.rs`
  - `EndgameDetector::detect_direction`
  - `EndgameDetector::detect_with_direction`
- `crates/oxide-arb-algorithm/src/scorer/endgame_scorer.rs`
  - `EndgameScorer::score`
  - `EndgameScorer::finalize`

### 4.1 Provider

```rust
pub trait ControlFactorProvider {
    fn snapshot(&self) -> Arc<ControlFactorSnapshot>;
}

pub struct FactorAwareOpportunityPipeline<F, C> {
    inner: OpportunityPipeline<F>,
    factors: C,
}
```

### 4.2 Application order

1. Before detector: check `MarketAnomalyFactor` by market/event/category。
2. After detector creates `Opportunity`: apply `BucketRiskFactor` to resolution probability/min edge policy。
3. Before score threshold: apply `ExecutionQualityFactor` to fill probability or scorer draft。
4. Before emit: record factor application trace in `ScoredOpportunity`。

### 4.3 Applied trace

```rust
pub struct AppliedControlFactor {
    pub factor_id: ControlFactorId,
    pub factor_type: ControlFactorType,
    pub publication_id: FactorPublicationId,
    pub input_value: Decimal,
    pub output_value: Decimal,
    pub reason: String,
}

pub struct ScoredOpportunity {
    // existing fields...
    pub applied_factors: Arc<[AppliedControlFactor]>,
}
```

If adding the field directly is too invasive during implementation, add `ScoredOpportunityControlTrace` keyed by `opportunity_id`；不要把 factor effects 只藏在日志里。

---

## 5. Scorer

推荐：

```rust
pub struct FactorAwareEndgameScorer {
    base: EndgameScorer,
    factors: ArcSwap<ControlFactorSnapshot>,
}
```

Application order：

```text
base_fill_probability
  -> execution_quality multiplier
  -> score computation
  -> bucket/category/market gates
```

`ExecutionQualityFactor` 不应只在 final score 后应用。它必须影响 `fill_probability`，因为后续 risk sizing 消费 `ProbabilityInput.fill_prob`。

---

## 6. Execution Validation

当前接入点：

- `crates/oxide-arb-core/src/execution/execution_pipeline.rs`
  - `ExecutionPipeline::execute`
  - `prepare_dispatch`
  - `validate_and_size`
- validation happens before `RiskEngine::pre_trade_check_core`。

Required flow：

```text
validate_and_size:
  validator.validate(...)
  apply execution-quality stricter depth/slippage thresholds
  build factor-adjusted ProbabilityInput
  risk_engine.pre_trade_check_core(...)
```

If `ExecutionQualityFactor.max_depth_usage_pct` is stricter than config，validation must reject before risk sizing and write `opportunity_audit` rejection stage = `factor_validation`。

---

## 7. Risk Engine

当前接入点：

- `crates/oxide-arb-risk/src/engine.rs`
  - `RiskEngine::pre_trade_check_core`
  - uses `PreTradeContext`
  - runs fixed pipeline gates
  - calls `MultiConstraintSizer::size`

Preferred model：

```rust
pub struct PreTradeContext<'a> {
    // existing fields...
    pub factor_context: Option<&'a FactorDecisionContext>,
}

pub struct FactorDecisionContext {
    pub publication_id: FactorPublicationId,
    pub reconciliation_health: ReconciliationHealthDecision,
    pub market_anomaly: MarketAnomalyDecision,
    pub portfolio_risk: PortfolioRiskDecision,
    pub applied_factors: Vec<AppliedControlFactor>,
}
```

Hard reject factors：

- `MarketAnomalyFactor.block_market`；
- `MarketAnomalyFactor.block_event`；
- `ReconciliationHealthFactor.force_maintenance_mode`。

这些必须表现为具名 risk checks，不能只是匿名 denial string。

---

## 8. Sizer

当前接入点：

- `crates/oxide-arb-risk/src/sizing.rs`
  - `QuarterKellyCalculator::calculate`
  - `MultiConstraintSizer::size`
  - constraints include Kelly, single bet, exposure, daily budget, weekly loss, available balance, drawdown。

Add explicit factor constraints：

```rust
SizeConstraint {
    name: "factor_bucket_size_cap",
    max_usd: base_size * bucket_risk.size_multiplier,
}

SizeConstraint {
    name: "factor_portfolio_size_cap",
    max_usd: base_size * portfolio_risk.global_size_multiplier,
}

SizeConstraint {
    name: "factor_reconciliation_size_cap",
    max_usd: base_size * reconciliation_health.size_multiplier,
}
```

不要把 factor multiplier 隐式揉进 bankroll。Binding constraint 必须可审计。

---

## 9. Shadow Consumption

Shadow factors are loaded into a separate shadow snapshot。Live computes：

- baseline decision；
- shadow decision；
- delta。

It writes `control_factor_shadow_decision` but does not change real orders。

Shadow writer 必须异步且背压安全；写失败不能改变真实 order path，但必须暴露 metrics/alert。

---

## 10. Core Files

新增：

```text
crates/oxide-arb-core/src/control/
  factor_refresher.rs
  factor_snapshot.rs
  factor_shadow.rs
```

职责：

- startup load current publication；
- validate TTL/schema/hash；
- build indexes；
- store `ArcSwap<ControlFactorSnapshot>`；
- periodically refresh；
- listen for notify events；
- write shadow decisions。

Repository traits：

```text
crates/oxide-arb-repository/src/traits/control_factor.rs
crates/oxide-arb-repository/src/traits/evidence_timeseries.rs
```

`oxide-arb-control` / `oxide-arb-core` 不得直接依赖 raw `clickhouse::Client` 或 `sea_orm::DatabaseConnection`。

---

## 11. 测试策略

| 测试 | 必需场景 |
|---|---|
| Snapshot load | startup success、schema mismatch、expired safety factor、hash mismatch |
| Refresh | unchanged version no-op、changed version atomic swap、failed refresh keeps valid old snapshot |
| Detector/scorer | market anomaly block、bucket haircut、execution quality fill probability discount |
| Validation | factor stricter depth/slippage reject、audit rejection stage |
| Risk gate | maintenance mode、market/event block、named checks |
| Sizer | factor constraints visible as binding constraints |
| Shadow | baseline vs shadow delta recorded，不影响 real order |
| Hot path guard | no CH/PG query in detector/scorer/risk/sizer path |

---

## 12. 退出条件

Phase 5.6 完成后必须满足：

1. Startup loads active publication into `ArcSwap<ControlFactorSnapshot>`。
2. Periodic refresh and notify refresh work。
3. Hot path reads no CH/PG。
4. Safety factor load failure can fail closed。
5. Applied factors are written to detection/execution audit。
6. `ExecutionQualityFactor` affects fill probability before score/risk sizing。
7. Factor size caps appear as explicit binding constraints。
8. Shadow deltas are recorded and never affect real orders。

---

## 13. 阻止进入 Phase 5.7/5.8 的情况

- Hot path code queries CH/PG。
- Snapshot schema mismatch is ignored。
- Expired safety factor is treated as neutral when policy says fail closed。
- Factor multipliers are hidden inside bankroll/probability without audit trace。
- Risk hard rejects are anonymous strings。
- Shadow decision can mutate live decision。
