# ADR-002: 量化信号平面（Signal Plane）与执行平面（Execution Plane）分离

> **状态**: 已批准（设计目标）
>
> **日期**: 2026-06-17
>
> **前置**: [ADR-001](./ADR-001-single-strategy-single-platform.md)（单平台 Polymarket 保留；单 endgame 热路径自动交易限制部分废止）
>
> **母计划**: [phase9-quant-signal-plane.md](./phase9-quant-signal-plane.md)

---

## 1. 决策背景

oxide-arb 当前形态（ADR-001）是 **Endgame 收敛自动交易 bot**：WS 驱动 Scanner → 即时 FOK 买入 → hold-to-resolution。

产品新目标为 **Polymarket 纯量化决策系统**：

- 多 Alpha / 多因子；
- 在指定时刻输出 **Top-N 报告**（买什么、何时买、买多少、何时卖）；
- 执行可选（Advisory / Semi-auto / Auto）；
- **不是**无风险组合套利（Frank-Wolfe / Bregman / LP 不在范围内）。

ADR-001 的「单策略、无 Strategy registry、detector 即热路径」与上述目标冲突，但 ADR-001 在 **数据平面、风控、执行安全、单平台** 上的决策仍然正确。本 ADR **部分废止** ADR-001 §2.1 中与「策略多样性」相关的限制，**保留** §2.2 单平台与 FOK 执行安全模型。

---

## 2. 核心决策

### 2.1 双平面架构

| 平面 | 职责 | 默认行为 |
|------|------|----------|
| **Signal Plane** | 因子计算、Alpha 评估、组合 Top-N、报告版本化 | **只产出报告，不下单** |
| **Execution Plane** | 风控、FOK、对账、结算、账本 | 仅在 Adopt / Auto 模式下消费报告 |

```text
Live Facts (Gamma + WS + PG + CH)
  → Signal Plane: AlphaRegistry → PortfolioConstructor → QuantRun / Top-N Report
  → [可选] Execution Plane: Risk → FOK → Post-trade
```

**不变量**：报告（`QuantRun`）是一等公民产物，必须 PIT 可复现、可审计、可回测；执行记录必须携带 `quant_run_id` + `rank` + `alpha_id` 血缘。

### 2.2 AlphaRegistry（批准引入）

| 保留 ADR-001 禁令 | ADR-002 批准 |
|-------------------|--------------|
| 多平台 `VenueId` | **禁止**（仍单 Polymarket） |
| 多 leg 对冲 FSM | **禁止**（仍无 hedging） |
| `Strategy` trait 动态多平台路由 | **禁止** |
| **单 endgame detector 即唯一决策入口** | **废止** → `AlphaRegistry` 注册多个 **纯计算** Alpha |
| **无策略注册表** | **废止** → Alpha 以 typed artifact 注册，非 runtime dynamic plugin |

AlphaRegistry **不是**旧式 `Strategy` 工厂：Alpha 只做 **信号生成**（`RawSignal`），组合与风控在 Signal Plane 下游完成。

### 2.3 因子体系：Alpha Factor vs Control Factor（批准拆分）

Phase 5 的五类 `ControlFactorType` 定位为 **风险约束因子**（只能收紧，见 phase5.0 §1.1）。  
量化系统新增 **Alpha Factor** 族（预测 edge / 排序），与 Control Factor **分表、分 registry、分 governance**，但 **共用** PIT 物料化基础设施。

| 族 | 目的 | Live 消费 |
|----|------|-----------|
| **Alpha Factor** | 预测、排序、报告归因 | Signal Plane；可选 shadow |
| **Control Factor** | 收紧 fill/resolution/budget/exposure | Risk + Scorer + Sizer |

**禁止**：用 Control Factor payload 承载 Alpha 预测逻辑（避免语义混淆与 conservative gate 误杀）。

### 2.4 执行模式（批准三档）

| 模式 | 含义 |
|------|------|
| `Advisory` | 仅报告；人决策 |
| `SemiAuto` | 报告 → 人工 Adopt → Execution Plane |
| `Auto` | 报告窗口内自动 FOK（需 Exit Engine 就绪） |

默认部署 **`Advisory`**。Legacy endgame bot（Scanner 即时 emit）在 Phase 9 过渡期内可并存，标记为 **`LegacyAutoEndgame`**，最终默认关闭。

### 2.5 出场（Exit）为一等公民（批准）

ADR-001 / phase5 默认 hold-to-resolution。**Quant 产品必须**在每条 Top-N 建议中携带显式 `exit_spec`（take-profit / stop / time-stop / resolution-hold / report-expiry）。

**批准** FOK Sell 路径与 `close_position` 生产接线（phase9.4）。

---

## 3. 明确删除 / 合并 / 保留

### 3.1 删除（Phase 9 完成后不得残留）

| 元素 | 位置 | 原因 |
|------|------|------|
| 「Endgame 是唯一策略入口」文档约束 | ADR-001 §2.1 部分条文 | 由 ADR-002 废止 |
| `EndgameDetector` 内硬编码 `Side::Buy` 作为全局策略 | `algorithm/endgame/detector.rs` | 移至 `EndgameAlpha` 默认方向，其他 Alpha 可 Sell |
| 将 `OpportunityPipeline` 同时承担「报告编排」 | `algorithm/pipeline.rs` | 拆为 `EndgameAlpha` + `SignalOrchestrator` |
| 依赖 detection emit 作为唯一「买什么」来源 | Scanner → Funnel 产品语义 | 报告以 `QuantRun` 为准 |
| phase5 §12 「主动 exit 永不实现」作为全局禁令 | `phase5-replay-analytics.md` §12 | 标记 superseded by ADR-002（exit 为 Quant 范围） |

**不删除**（澄清误区）：

- Frank-Wolfe / Bregman / LP 相关代码：**从未存在，无需删除**。
- `ControlFactorType` 五类：**保留并强化**，不合并进 Alpha。

### 3.2 合并 / 重构

| 原模块 | 目标 | 动作 |
|--------|------|------|
| `OpportunityPipeline` + `EndgameDetector` + `EndgameScorer` | `EndgameAlpha` + `LegacyAutoTradingPipeline` | 提取 Alpha；Pipeline 降级为 Legacy 自动路径 |
| `oxide-arb-control` materialization | `SignalMaterializationRun` 共用 PIT runner | 扩展 manifest，不 fork 第二套 scheduler |
| `opportunity_audit` (CH) | `quant_signal_audit` | 扩展 stage，关联 `quant_run_id` |
| `Scanner` / `Coalescer` / `Funnel` | `LegacyAutoEndgame` 专用 | 与 Signal Scheduler 并列，非替代 BookStore |
| `close_position` (repo only) | Exit / 外部成交闭环 | 生产接线 + governed API |

### 3.3 保留不变

- Polymarket 单平台：Gamma、CLOB WS、CTF、EIP-712、FeeCalculator
- BookStore `published` 快照热路径只读
- `ControlFactorSnapshot` + Phase 5 governance（Shadow / Publish / Rollback）
- `StaticRiskPipeline` + Circuit breaker + Reconciliation fail-closed
- FOK-only 执行（Buy / Sell 均 FOK，无 GTD resting）
- Money newtypes；禁止 `f64` 业务计算

---

## 4. AlphaRegistry 范围（批准的最小集 → 扩展集）

**Phase 9 MVP 必须实现**（纯 Rust、无 I/O、可单测）：

| Alpha ID | 来源 | 说明 |
|----------|------|------|
| `endgame_convergence_v1` | 现有 detector + calibrator + scorer 逻辑 | 包装迁移，非重写 |
| `microstructure_liquidity_v1` | BookStore spread / depth / imbalance | 复用 phase5.1a book facts |
| `catalyst_deadline_v1` | settlement_deadline + convergence duration | 规则型 |

**Phase 9.2+ 扩展**（需 evidence + quality gate）：

| Alpha ID | 依赖 |
|----------|------|
| `momentum_price_v1` | CH L2 / mid 序列 |
| `calibration_prior_v1` | ResolutionCalibrator bucket 输出 |
| `execution_quality_prior_v1` | Phase 5 ExecutionQuality factor 作为 **prior**，非 hard gate |

**明确不在范围**：combinatorial arb、LP、Frank-Wolfe、cross-venue stat arb。

---

## 5. Control Factor 平面演进（批准）

Phase 5 设计 **基本正确**，Quant 阶段 **扩展而非推翻**：

| 现有 | Phase 9 增强 |
|------|--------------|
| 5 类 ControlFactorType | 保持不变；文档明确「仅约束」 |
| Materialization + quality gate | 新增 parallel **`AlphaFactorMaterializationRun`** |
| `ReportOnly` output policy | **Quant 报告默认路径** |
| Shadow delta | 扩展：Alpha shadow 对 Top-N 排名影响 |
| ReconciliationHealth | 继续 fail-closed；与外部手动卖出 ingest 联动 |

新增 **`AlphaFactorType`**（示例，最终以 schema-catalog 为准）：

- `bucket_edge_prior`
- `fill_quality_prior`
- `sector_momentum_prior`

Governance 规则：**Alpha Factor 可升/降 rank weight，不可单独放大 Kelly 或 bypass risk gate**。

---

## 6. 后果

### 正面

- 产品语义清晰：报告是交付物，执行是可选下游
- 复用 Phase 5 投资（PIT、CH、governance）
- Legacy bot 可渐进退役，降低切换风险

### 负面 / 成本

- 新 crate 边界（`oxide-arb-signals` 或在 `algorithm` 内 `alpha/`）
- PG + CH 新表（`quant_run`, `quant_recommendation`, …）
- ADR-001 文档需交叉引用 ADR-002，避免新人误读

---

## 7. 参考

- [phase9-quant-signal-plane.md](./phase9-quant-signal-plane.md) — 实施母计划
- [phase5-replay-analytics.md](./phase5-replay-analytics.md) — Control Factor 平面
- [runbook.md](../operations/runbook.md) — 运维验收
