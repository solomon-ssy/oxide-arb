# Phase 06 — ML 扩展相位 子phase索引

> 状态：设计文档（本目录不含代码；**Phase 5 全部落地后**再开实现）
>
> 父文档：[`../08-third-party-crates-and-ml-stack.md`](../08-third-party-crates-and-ml-stack.md)、
> [`../03-data-factor-model-pipeline.md`](../03-data-factor-model-pipeline.md)、
> [`../05-execution-risk-and-governance.md`](../05-execution-risk-and-governance.md)
>
> 本目录承接 **Phase 5 明确延后**、且属于「研究/模型族扩展」或「跨平面增强」的能力。
> 每条延后项在 Phase 5 子phase §11 / [`phase-05/README.md`](../phase-05/README.md) §6.2
> 中已登记；**本目录给出可执行实施契约**，保证后续落地时能闭合 05.6 预留的 seam。

## 0. 为什么单独开 Phase 6

Phase 5 闭环（05.0–05.9）已覆盖：**执行 / 风险 / 治理 / 对账 / 退出监控 / 权益曲线**。
以下能力**不阻塞** Phase 5 生产闭环，但 Phase 5 已在代码与契约层**预留接口**（seam +
enum + metric + config 字段），Phase 6 必须按本目录契约**填 impl**，不得另起炉灶：

| Phase 5 预留（seam） | Phase 6 填 impl | 权威子phase |
|---|---|---|
| `ExitSignalReinferer` + `ReinferenceSignalEvaluator`（thesis invalidation 占位） | **`ModelBackedExitSignalReinferer`** + shadow 激活 | [`06.0`](06.0-exit-signal-reinference.md) |
| `ExitSignalEvaluator` + `ExitReason::Opportunistic` + `quant_exit_triggers_total{reason=opportunistic}` | 机会性 Sell 排序模型 + `CompositeExitSignalEvaluator` | [`06.1`](06.1-opportunistic-sell-exit-signal.md) |
| `UnifiedModelRunner` + model registry artifact 类型 | ONNX / classical publish 主路径（08 §17/§15） | 06.3+（待开） |
| 逐订单对账引擎（05.5） | 跨账户周期 reconciliation report | [`06.2`](06.2-cross-account-reconciliation-report.md) |

**硬依赖（Phase 6 开工前必须满足）：**

- 05.6 `ExitMonitor` + `ExitSignalEvaluator` seam + per-lot position ledger（R3）已落地。
- 05.7 attribution 已写入 PG（训练样本来源）。
- MSRV 决策 + `ort` spike（08 §12.5）若走 ONNX 路径。

## 1. 子phase索引

| 子phase | 标题 | 闭环定位 | 文档 | 依赖 |
|---|---|---|---|---|
| 06.0 | Exit Signal Re-inference | **激活 05.6 thesis-invalidation 信号退出** | [`06.0-exit-signal-reinference.md`](06.0-exit-signal-reinference.md) | 05.6 |
| 06.1 | Opportunistic Sell Exit Signal | **闭合 05.6 退出信号 seam（机会性平仓）** | [`06.1-opportunistic-sell-exit-signal.md`](06.1-opportunistic-sell-exit-signal.md) | 05.6 / **06.0** |
| 06.2 | Cross-Account Reconciliation Report | **05.5 对账平面跨账户增强** | [`06.2-cross-account-reconciliation-report.md`](06.2-cross-account-reconciliation-report.md) | 05.5/05.7 |
| 06.3 | ONNX Runtime Integration | ONNX 线上推理（08 §17） | *待开* | 3.4/3.6/08 §12.5 |
| 06.4 | Classical Model Publish Path | smartcore/linfa 主路径 publish（08 §15） | *待开* | 3.6/3.7 |

## 2. 依赖图

```mermaid
flowchart TD
    P56["05.6 Exit Monitor + ExitSignalEvaluator seam"]
    P57["05.7 Attribution"]
    P55["05.5 Reconciliation"]
    P60["06.0 Exit Signal Re-inference"]
    P61["06.1 Opportunistic Sell"]
    P62["06.2 Cross-Account Recon Report"]
    P63["06.3 ONNX Runtime"]
    P56 --> P60
    P56 --> P61
    P57 --> P61
    P60 --> P61
    P55 --> P62
    P63 --> P61
```

## 3. 与 Phase 5 延后项总表的对照

[`phase-05/README.md`](../phase-05/README.md) §6.2 中指向 Phase 6 的条目，**详细设计落点**如下：

| 延后能力 | 详细设计文档 |
|---|---|
| 研究侧 thesis-invalidation 再推理（信号失效退出） | **06.0** |
| 研究侧 `Sell` 排序模型（机会性平仓信号） | **06.1**（非 08 §20 alone — §20 仅通用多模型编排） |
| `ort` / ONNX 线上推理 | 08 §17 + **06.3**（待开） |
| classical model 主路径 publish | 08 §15 + **06.4**（待开） |
| 跨账户全量对账 / 周期 reconciliation report | **06.2**（登记于 05.5 §11） |

**不在 Phase 6、而在 Phase 5 收尾的项**见 [`05.10-auto-redeem-settlement.md`](../phase-05/05.10-auto-redeem-settlement.md)（`AutoRedeem` 链上赎回）。

**Phase 8+ 部署架构项**见 [`../phase-08/README.md`](../phase-08/README.md)（多副本 leader-elected worker、高频 trailing 评估）。

## 4. 文档契约模板

与 Phase 3/5 相同（10 节固定顺序），见 [`phase-05/README.md`](../phase-05/README.md) §7。

## 5. 质量门禁（每个子phase收尾必跑）

```bash
cargo fmt --all --
cargo clippy --workspace --all-targets -- -D warnings
bash scripts/lint-architecture.sh
bash scripts/lint-quant-pivot-boundary.sh
bash scripts/lint-quant-pivot-errors.sh
cargo test --workspace
```
