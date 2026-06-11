# `bankroll_usd` 与风险指标模型

> 回答：**为什么 `risk.bankroll_usd` 是 runtime 配置，而不是直接读 Polymarket 钱包余额？**

---

## 1. 一句话结论

| 模式 | 现金/权益的权威来源 | `bankroll_usd` 的作用 |
|------|---------------------|----------------------|
| **DryRun** | 模拟：`RiskMetricsState` 用 `bankroll_usd` 作为初始现金 | **就是**「虚拟本金」 |
| **Paper** | 模拟：同上 | **就是**「虚拟本金」；CLOB 余额仅用于对账 side-channel |
| **Live** | **权威：CLOB API 刷新**（`RiskMetricsRefreshService`） | **上限 cap**：Kelly  sizing 取 `min(动态权益, bankroll_usd)` |

Live 模式下，真实余额来自 Polymarket CLOB；`bankroll_usd` 是**运营者设定的策略资金上限**，不是链上/交易所余额的镜像。

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
- 已充值但未在 Paper 成交 → 余额与模拟 ledger 不一致，对账 drift 无意义。

所以 DryRun/Paper 启动时用 `bankroll_usd` **种子化**模拟快照：

```rust
// crates/oxide-arb-core/src/app/build.rs
metrics_state.seed_simulated_snapshot(mode, Usd::new(runtime.risk.bankroll_usd));
```

来源标记为 `SimulatedDryRun` / `SimulatedPaper`（`RiskMetricsSource`）。

### 2.3 热更新与审计

`bankroll_usd` 在 **runtime config**（Postgres 版本化），通过 `POST /api/runtime-config/versions/{id}/activate` 激活：

- DryRun/Paper：`RiskMetricsState::reload()` 对模拟现金做 **delta rebase**，保留持仓与 exposure；
- Live：**不修改** CLOB 权威快照，只改变 sizing cap。

代码注释（`runtime_config/applicator.rs`）：

> R1 — simulated cash baseline (rebased on `bankroll_usd` activation in DryRun/Paper; **never touched on the authoritative Live source**).

### 2.4 对账（reconciliation）中的 `bankroll_usd`

Ledger reconciliation（`periodic_services.rs` → `run_ledger_reconciliation`）每 tick 读取：

```text
internal_cash = configured_bankroll - successful_spend + settled_payout
external_available = clob_client.collateral_balance()
```

- **internal**：以 `bankroll_usd` 为 baseline 的**账本模型**；
- **external**：CLOB 真实 collateral。

DryRun/Paper 若 `bankroll_usd` 与钱包余额不一致，会产生 drift — 这是预期行为，ReconciliationHealth 因子在样本不足前可能为 `report_only`。

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
| Paper，`bankroll_usd` ≠ 钱包 | 模拟 PnL 按 cap 走；对账 drift 大，Reconciliation 因子可能 ineligible |

---

## 5. 相关代码索引

| 文件 | 职责 |
|------|------|
| `crates/oxide-arb-models/src/runtime_config/risk.rs` | `bankroll_usd` 字段定义（默认 $1000） |
| `crates/oxide-arb-core/src/service/risk_metrics.rs` | 模拟种子、`reload()`、CLOB 刷新 |
| `crates/oxide-arb-risk/src/engine.rs` | `available_bankroll()` Kelly 输入 |
| `crates/oxide-arb-core/src/app/periodic_services.rs` | 对账 internal baseline |
| `crates/oxide-arb-core/src/control/mode_transition.rs` | 切 Live 时刷新权威 metrics |
