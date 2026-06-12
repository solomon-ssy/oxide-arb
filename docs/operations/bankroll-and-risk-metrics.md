# `bankroll_usd` 与风险指标模型

> 回答：**为什么 `risk.bankroll_usd` 是 runtime 配置，而不是直接读 Polymarket 钱包余额？**

---

## 1. 一句话结论

| 模式 | 现金/权益的权威来源 | `bankroll_usd` 的作用 |
|------|---------------------|----------------------|
| **DryRun** | 模拟派生账本：`bankroll − 成功成交支出(mode) + 结算回款(mode)`（纯 PG 推导） | **就是**「虚拟本金」（账本 baseline） |
| **Paper** | 模拟派生账本：同上 | **就是**「虚拟本金」（账本 baseline） |
| **Live** | **权威：CLOB API 刷新**（`RiskMetricsRefreshService`） | **上限 cap**：Kelly  sizing 取 `min(动态权益, bankroll_usd)` |

Live 模式下，真实余额来自 Polymarket CLOB；`bankroll_usd` 是**运营者设定的策略资金上限**，不是链上/交易所余额的镜像。

`RiskMetricsRefreshService` 是 **mode-aware** 的单一刷新入口：每次 `refresh()` 读取当前
`ExecutionMode`，Live 走 CLOB 权威路径（source = `AuthoritativeClob`），DryRun/Paper 走
PG 派生路径（source = `SimulatedDryRun` / `SimulatedPaper`）。模拟资金随模拟成交真实
演化，且**完全可重算**（重启/模式切换后从 PG + bankroll 推导，不依赖内存状态）。
trade 与 position 均落 `execution_mode` 列，账本聚合按模式隔离 — 模拟历史永远不会污染
Live 账本，反之亦然。

---

## 2. 设计动机

### 2.1 分离「账户总资金」与「策略可用资金」

Polymarket 钱包里可能有多余 USDC（未分配给本策略、或留给 gas / 其他用途）。若 sizing 直接用全量 CLOB 余额：

- 一次误配置可能把**整个钱包**打进 endgame 仓位；
- 多策略/多实例共用同一钱包时无法隔离；
- 运营无法通过 governance API **热更新**策略规模而不动链上余额。

因此 Live 下采用：

```text
dynamic = equity - reserve_balance_usd - reserved_usd - potential_loss
effective_bankroll = min(dynamic, bankroll_usd)   // 再交给 Kelly sizer
```

实现见 `oxide-arb-risk/src/engine.rs` 中 `available_bankroll()`。

### 2.2 DryRun / Paper 没有「权威 venue 余额」

DryRun 不向 CLOB 下单；Paper 也不占用真实 collateral。此时若仍读 CLOB：

- 未充值时余额为 0 → 所有 sizing 为 0，**无法验证检测/风控/执行链路**；
- 已充值但未在 Paper 成交 → 余额与模拟 ledger 不一致，任何对真实余额的比较都无意义。

所以 DryRun/Paper 的现金是**派生账本**，启动与每次周期刷新都从 PG 重新计算：

```text
simulated_cash = bankroll_usd
               − successful_spend_total(mode)   // trade: outcome=success, execution_mode=mode
               + settlement_payout_total(mode)  // position: settled+accounted, execution_mode=mode
```

来源标记为 `SimulatedDryRun` / `SimulatedPaper`（`RiskMetricsSource`），持仓与敞口同样按
`find_open(mode)` 隔离。模式切换回 DryRun/Paper 时账本**延续 PG 历史**（确定性重算），
不会清零重置。

### 2.3 热更新与审计

`bankroll_usd` 在 **runtime config**（Postgres 版本化），通过 `POST /api/runtime-config/versions/{id}/activate` 激活：

- DryRun/Paper：`RiskMetricsState::reload()` 对模拟现金做 **delta rebase**，保留持仓与 exposure；
- Live：**不修改** CLOB 权威快照，只改变 sizing cap。

代码注释（`runtime_config/applicator.rs`）：

> R1 — simulated cash baseline (rebased on `bankroll_usd` activation in DryRun/Paper; **never touched on the authoritative Live source**).

### 2.4 对账（reconciliation）中的 `bankroll_usd`

**Ledger reconciliation 是 Live-only 的**：外部余额对账只对真实资金有意义。
DryRun/Paper 下账本追踪的是虚拟资金，与 CLOB 真实余额的任何比较都没有信息量，
因此每个 tick 先读取当前 `ExecutionMode`，非 Live 直接跳过（debug 日志
`ledger reconciliation skipped — external balance reconciliation is Live-only`）。
模式在运行时切到 Live 后，下一个 tick 自动恢复对账，无需重启。

Live tick 读取：

```text
internal_cash = configured_bankroll - successful_spend(Live) + settled_payout(Live)
external_available = clob_client.collateral_balance()
```

- **internal**：以 `bankroll_usd` 为 baseline 的**账本模型**，聚合按
  `execution_mode = live` 过滤 — 模拟时期的历史成交不会污染 Live 账本；
- **external**：CLOB 真实 collateral。

偏差 ≥ 10 × `reconciliation_tolerance_usd` 判为 Critical 并触发 **L4 System Halted**
（fail-closed，需运维 `POST /api/system/resume` 确认恢复）。

**Live 上线前建议**：将 `bankroll_usd` 设为接近实际投入策略的 USDC，并充值对齐。

---

## 3. 三个「余额」不要混

| 概念 | 存储/来源 | 用途 |
|------|-----------|------|
| **CLOB collateral** | `ClobClient::collateral_balance()` | Live 权威现金、对账 external |
| **`risk.bankroll_usd`** | Runtime config | 模拟本金 / Live sizing 上限 |
| **`risk.reserve_balance_usd`** | Runtime config | 从 Kelly 可用资金中永久扣除的保留额（默认 $100） |

Live 下 **equity** = CLOB 现金 + 持仓 mark value（`RiskMetricsRefreshService` 刷新）。

---

## 4. 运营建议

### DryRun / Paper 验证阶段

- 将 `bankroll_usd` 设为**你打算 Live 投入的策略资金**（如 $500），便于 PnL 与 sizing 直觉一致；
- 不必与钱包余额相等（Paper 不花真钱）。

### 切 Live 前

1. 钱包充值 USDC（Polygon）；
2. `bankroll_usd` ≤ 实际策略资金 ≤ 钱包可用 USDC；
3. `reserve_balance_usd` 留足缓冲（默认 $100）；
4. 切换 Live 后确认 `GET /api/system/status` 中 metrics source 为 **AuthoritativeClob**；
5. 观察 `GET /api/pnl/live` 与 CLOB 余额一致。

### 误把 `bankroll_usd` 当钱包余额时会发生什么

| 场景 | 现象 |
|------|------|
| Live，`bankroll_usd` >> 钱包 | sizing 被 cap 在真实 equity，不会超花 |
| Live，`bankroll_usd` << 钱包 | 策略只用 cap 部分资金，其余 USDC 闲置 |
| Paper，`bankroll_usd` ≠ 钱包 | 完全无影响：模拟账本与钱包余额互不相干，对账在非 Live 模式下不运行 |

---

## 5. 相关代码索引

| 文件 | 职责 |
|------|------|
| `crates/oxide-arb-models/src/runtime_config/risk.rs` | `bankroll_usd` 字段定义（默认 $1000） |
| `crates/oxide-arb-core/src/service/risk_metrics.rs` | mode-aware 刷新（Live=CLOB / 模拟=PG 派生）、`reload()` delta rebase |
| `crates/oxide-arb-risk/src/engine.rs` | `available_bankroll()` Kelly 输入 |
| `crates/oxide-arb-core/src/app/periodic_services.rs` | Live-only 对账 + internal baseline（按模式聚合） |
| `crates/oxide-arb-core/src/control/mode_transition.rs` | 切模式时统一 `refresh()` + source 断言（fail-closed） |
| `crates/oxide-arb-repository/src/postgres/trading/` | `successful_spend_total(mode)` / `settlement_payout_total(mode)` / `find_open(mode)` |
