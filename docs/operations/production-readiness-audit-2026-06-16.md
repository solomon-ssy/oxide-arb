# oxide-arb 实盘生产 Readiness 审计报告

> 日期：2026-06-16  
> 范围：`oxide-arb` 后端 workspace，覆盖数据链路、算法链路、风险链路、执行链路、资金与凭证、结算赎回、Web 控制面、测试与运维文档。  
> 结论：**P0 关闭前不建议切 Live。** 当前主交易链路已经工程化，但资金对账、FOK 超时歧义、成交后 exposure 空窗、资金状态感知、CI snapshot 等问题未关闭前，不满足真钱系统的生产级闭环。

---

## 1. 一句话结论

`oxide-arb` 已经具备比较完整的 Polymarket Endgame 套利主链路：

```text
Gamma / CLOB WS
  -> BookStore
  -> Coalescer / Scanner / Funnel
  -> Endgame algorithm / scorer
  -> RiskEngine
  -> ExecutionPipeline
  -> Live CLOB FOK
  -> PostTradeRelay / Position / Settlement / PnL / Metrics
```

代码质量、分层、fail-closed 意识、测试体系都明显超过普通原型项目；但这是直接处理真钱的系统，生产判断不能只看“能不能下单”。目前最关键的问题集中在 **资金真相、未知订单状态、持仓敞口连续性、运维感知** 四类。

最终评级：

| 维度 | 评级 | 判断 |
| --- | --- | --- |
| 架构主链路 | 较成熟 | 数据、检测、风控、执行、post-trade 已完整接线 |
| 风控 fail-closed | 较成熟 | mode、metrics、breaker、reservation、runtime config 均有 fail-closed 设计 |
| 资金业务闭环 | 不足 | Live 对账口径、充值/提现、余额 API、未知订单恢复未闭环 |
| 测试与上线门槛 | 不足 | 默认关键子集通过，但 workspace snapshot 红、ignored 生产测试未跑 |
| 当前是否可 Live | 不建议 | P0 未关闭，不应直接实盘 |
| P0 后目标 | 小资金 canary | 200-500 USDC，24-72h，强人工监控 |

---

## 2. 审计依据

本报告综合以下输入：

- 代码只读审计：`crates/oxide-arb-core`、`crates/oxide-arb-risk`、`crates/oxide-arb-api`、`crates/oxide-arb-web`、`crates/oxide-arb-models`。
- 文档审计：`docs/operations/runbook.md`、`docs/operations/bankroll-and-risk-metrics.md`、`docs/plans/phase7-ui-layer.md`。
- 测试执行：
  - `cargo test --workspace --all-targets`
  - `cargo test -p oxide-arb-risk --all-targets`
  - `cargo test -p oxide-arb-core --all-targets`
  - `cargo test -p oxide-arb-api --all-targets`
- 子任务审计：
  - 架构链路审计
  - 风险资金审计
  - 测试基准盘点

---

## 3. 当前系统做得好的地方

### 3.1 交易主链路完整

实盘主链路在 `oxide-arb-core` 中已经完整编排：

| 阶段 | 关键代码 |
| --- | --- |
| 启动组合根 | `crates/oxide-arb-core/src/app/build.rs` |
| WS/Gamma 数据接入 | `crates/oxide-arb-api/src/ws/*`、`crates/oxide-arb-core/src/service/gamma.rs` |
| BookStore 热路径 | `crates/oxide-arb-core/src/pipeline/book_store.rs` |
| Scanner/Funnel | `crates/oxide-arb-core/src/detection/*` |
| Endgame 算法 | `crates/oxide-arb-algorithm/src/*` |
| 风控 | `crates/oxide-arb-risk/src/engine.rs`、`crates/oxide-arb-risk/src/pipeline/checks.rs` |
| 执行 | `crates/oxide-arb-core/src/execution/execution_pipeline.rs` |
| CLOB FOK | `crates/oxide-arb-core/src/execution/fok_strategy.rs`、`crates/oxide-arb-api/src/clob/mod.rs` |
| Post-trade | `crates/oxide-arb-core/src/post_trade/*` |
| Settlement / redeem | `crates/oxide-arb-core/src/execution/settlement/*` |

核心执行路径是：

```text
ExecutionPipeline::execute
  -> validate_and_size
  -> RiskEngine::pre_trade_check_core
  -> CapitalManager::reserve_sync
  -> TradeRepository::create
  -> mark_submitted
  -> FokOrderStrategy::execute
  -> settle_reservation
  -> mark_observed
  -> PostTradeRelay
```

这个设计方向是正确的：先过风险，再预留，再持久化 intent，再进 venue round-trip，再异步幂等 post-trade。

### 3.2 Live 模式默认 fail-closed

系统默认 execution mode 存在 `system_runtime_state`，新库 seed 为 `dry_run`。切 Live 不是 TOML 配置，而是治理 API：

```http
POST /api/system/mode
```

`CoreRuntimeControl::switch_execution_mode` 使用以下协议：

```text
preflight
  -> halt / quiesce
  -> atomic mode store
  -> persist system_runtime_state
  -> metrics refresh / source assertion
  -> resume
```

这是真钱系统应该有的方向。尤其是：

- Live 需要 `ClobClient`。
- Live 需要 authoritative CLOB metrics。
- 切换前等待 reservation drain。
- activation 失败后保持 halted。

### 3.3 风控门比较完整

`StaticRiskPipeline` 固定顺序执行 30 个 gate，覆盖：

- manual halt
- circuit breaker
- blacklist
- token blacklist
- market anomaly block
- reconciliation maintenance
- control factor snapshot expiry
- redeem route resolvable
- metrics freshness
- min depth
- max depth usage
- staleness
- daily budget
- hourly / daily / weekly loss
- fee spend
- max single bet
- market / total exposure
- exposure pct
- potential loss cap
- max positions
- WS connectivity
- API error rate
- min balance
- directional concentration
- duplicate market
- drawdown guard

这个风险面覆盖已经具备生产基础。

### 3.4 三种模式边界清晰

| 模式 | 下单行为 | 余额来源 | 用途 |
| --- | --- | --- | --- |
| `DryRun` | 不触 CLOB，模拟 filled | PG 派生模拟账本 | 数据链路、检测频率、风控 smoke |
| `Paper` | 不触 CLOB，按 BookStore 深度模拟 | PG 派生模拟账本 | 评估 fill/miss、深度、漏斗 |
| `Live` | 真实 CLOB FOK | CLOB authoritative collateral | 真实交易 |

`FokOrderStrategy` 明确保证 Live 走真实 CLOB，DryRun/Paper 走 `Dispatcher`。

### 3.5 Post-trade 有幂等意识

系统使用 durable trade row 和 post-trade relay：

- 执行前创建 trade intent。
- venue round-trip 前 mark submitted。
- 返回后 mark observed。
- relay 幂等 claim 并推进 terminal state。
- position 创建、risk accounting、PnL update、WS event 都在 post-trade 侧统一处理。

这比同步热路径里直接写所有状态更稳健。

---

## 4. P0 风险：关闭前不应 Live

### P0-1 Live 定期对账口径与预交易余额模型不一致

#### 现象

预交易路径中，Live cash 使用：

```text
ClobClient::collateral_balance()
```

也就是 venue authoritative cash。

但定期 ledger reconciliation 使用：

```text
internal_cash = bankroll_usd - successful_spend(Live) + settled_payout(Live)
external_available = clob_client.collateral_balance()
```

问题是 `bankroll_usd` 在 Live 中设计上是 **strategy sizing cap**，不是钱包真实余额。把它作为 internal cash baseline，会把人工充值、提款、实际 wallet 余额和策略 cap 变更混在一起。

更严重的是，当前 periodic reconciliation 路径没有传真实外部持仓，可能导致内部 open position 被视为 external missing，从而触发 critical drift。

#### 影响

Live 中正常持仓或正常人工充值，都可能被识别成严重对账偏差，并触发 L4 halt。

#### 生产标准

Live cash truth 必须只有一个：

```text
authoritative venue cash = CLOB collateral balance
```

`bankroll_usd` 只能作为：

```text
sizing_cap = min(authoritative_equity, bankroll_usd)
```

不能同时作为 Live 现金账本。

#### 建议

- `bankroll_usd` 保留为策略资金上限，不参与 Live cash truth。
- Live reconciliation 改为比较：
  - venue cash
  - internal persisted open positions
  - active reservations
  - pending/unknown orders
  - optional treasury events
- 如果暂时无法查询外部 token positions，则不要把空 external positions 当成事实。

---

### P0-2 FOK HTTP 超时被直接当成 Failed

#### 现象

`FokOrderStrategy::execute_live_fok` 对 CLOB 下单使用 timeout。当前 timeout 后返回 `ExecutionOutcome::Failed`。

但 HTTP timeout 只说明本进程没等到响应，不代表 venue 没收到订单，也不代表订单未成交。

当前风险时序：

```text
mark_submitted
  -> CLOB request sent
  -> local timeout
  -> ExecutionOutcome::Failed
  -> release reservation
  -> mark FailObserved
  -> post-trade terminal Failed
```

如果 venue 实际已成交，就会出现：

- 内部无 position
- 无 fill accounting
- reservation 已释放
- 风险系统认为没有敞口
- 钱包真实余额已经减少

#### 影响

真钱系统中，这是最危险的订单状态问题之一：内部账本和真实 venue 状态永久分叉。

#### 生产标准

timeout 必须进入 unknown / needs_reconcile，而不是 terminal failed。

建议状态语义：

```text
Submitted
  -> FillObserved
  -> MissObserved
  -> FailObserved
  -> UnknownObserved / NeedsReconcile
  -> ReconciledFilled / ReconciledMiss / ReconciledFailed
```

最终实现可不完全采用这些名字，但必须保留“未知态”这个业务语义。

---

### P0-3 成交后到 position 落库前存在 exposure 空窗

#### 现象

当前执行成功后：

```text
ExecutionOutcome::Filled
  -> capital_manager.confirm_sync(reservation)
  -> reservation 从 active exposure 中移除
  -> mark_observed
  -> execute 返回
  -> market inflight guard drop
  -> PostTradeRelay 稍后创建 position
```

在 `reservation` 已移除、`position` 尚未创建的窗口中，风险快照可能看不到该 market 的 exposure。此时同 market 新机会可能通过 `DuplicateMarketCheck` 或 exposure gate。

#### 影响

可能违反“单市场单仓”策略，导致重复买入同一 endgame market。

#### 生产标准

真实成交后，exposure 必须连续存在：

```text
reservation exposure
  -> filled pending exposure
  -> persisted position exposure
```

不能出现 exposure=0 的中间态。

---

### P0-4 CI 当前不绿

#### 现象

`cargo test --workspace --all-targets` 失败在：

```text
oxide-arb-models runtime_config::preferences_schema::tests::preferences_schema_golden_snapshot
```

会话开始时已有：

```text
crates/oxide-arb-models/src/runtime_config/snapshots/oxide_arb_models__runtime_config__preferences_schema__tests__preferences_schema.snap.new
```

#### 影响

CI 不绿时不应进入 Live。尤其这类 snapshot 涉及 runtime config schema，属于资金关键配置的 UI/治理契约。

#### 建议

- review `.snap.new`。
- 如果 schema 变更符合预期，接受快照。
- 如果不符合预期，修 schema 生成源。

---

### P0-5 缺少单一资金状态 API

#### 现象

现有系统有：

- `/api/system/status`
- `/api/pnl/live`
- `/api/risk/exposure`
- `/api/risk/positions`
- Prometheus metrics

但没有一个单一 endpoint 回答：

```text
我的 bot 钱包现在有多少钱？
策略 cap 是多少？
可用资金是多少？
当前占用多少？
有多少 pending reservation？
持仓 mark value 是多少？
metrics 是否 authoritative？
最近一次 CLOB balance refresh 是什么时候？
有没有 open order invariant 违反？
```

`docs/plans/phase7-ui-layer.md` 已经记录“无账户余额/可用资金端点”。

#### 影响

运营在最关键的真钱问题上需要拼多个接口，容易误判。

#### 生产标准

新增单一资金视图，例如：

```http
GET /api/system/balance
```

或者等价的 `SystemBalanceView`，但语义必须是单一资金状态出口。

---

## 5. P1 风险：P0 后优先补强

### P1-1 `mode_transition` 缺核心协议测试

`CoreRuntimeControl::switch_execution_mode` 是防误实盘最关键控制面，但当前缺少覆盖完整协议的单元/集成测试。

至少应覆盖：

- preflight 失败不 commit
- quiesce timeout 不 commit
- active reservations drain 后 commit
- target Live 无 CLOB client 失败
- activation refresh 失败后保持 halted
- commit 后 persist 失败的行为
- resume 失败的行为

### P1-2 Web `POST /api/system/mode` 测试过于 mock

Web 层现有 harness 更偏 API contract，不足以证明真实 control adapter 的 fail-closed 语义。

需要补：

- RBAC / acting role
- Live 二次确认语义
- 缺 credentials / ClobClient 时拒绝
- operation log
- mode transition report
- WebSocket `system.status` 回显

### P1-3 `needs_reconcile` 没有完整 worker / operator API

现有 orphan 标记是好的，但还不是闭环：

```text
Submitted stale
  -> Orphaned
  -> needs_reconcile = true
```

缺：

- find needs reconcile
- venue order status query
- operator review API
- reconcile result audit
- position/risk correction

### P1-4 充值 / 提现 / Treasury 事件未建模

当前 runbook 写了人工入金流程，但系统没有 treasury event 表与 API。

这里不一定必须马上做自动链上监听，但必须有生产 SOP：

- 入金前是否 halt
- 入金后如何确认 CLOB collateral
- 提现前如何确认 no active reservation / no open order / no unresolved unknown
- 提现后如何记录 operation log
- 提现后如何验证 balance refresh

如果实现 treasury events，应避免它成为第二套 cash truth。它只能解释资金变动，不替代 CLOB authoritative balance。

### P1-5 Paper 模型偏乐观

Paper 当前基于 BookStore 深度判定 filled，未模拟：

- 网络延迟
- FOK 竞争
- CLOB 拒单
- partial/edge status
- VWAP

Paper 不应被宣传为真实收益评估，只能用于筛选候选市场和粗估 fill/miss。

---

## 6. 过度设计、重复设计、无效设计

### 6.1 `bankroll_usd` 在 Live 对账中承担了错误职责

保留：

- DryRun/Paper simulated bankroll baseline
- Live strategy cap

删除：

- Live authoritative cash baseline

### 6.2 Live cash truth 不能双轨

不应同时存在：

```text
CLOB collateral balance
bankroll - spend + payout
```

并让它们都被称为 cash truth。

建议合并为：

```text
cash truth = venue authoritative collateral
strategy cap = runtime bankroll_usd
internal exposure = persisted positions + reservations + unknown orders
```

### 6.3 FOK timeout、orphan、needs_reconcile 应合并

不应有三套互相独立的恢复语义。

建议统一为：

```text
unknown venue outcome
  -> reconciliation queue
  -> venue/operator confirmation
  -> terminal correction
```

### 6.4 Balance 展示应单一出口

不要让 UI/运营继续拼：

```text
system/status + pnl/live + risk/exposure + positions + metrics
```

资金状态必须有一个唯一入口。

---

## 7. 实盘前准备

### 7.1 钱包

必须使用专用 bot 钱包，不要用主账户。

推荐：

1. Rabby 创建专用 `oxide-arb-bot` 地址。
2. 记录 bot EOA 地址。
3. 只导出 bot 私钥。
4. 私钥只放 `OXIDE_ARB__KEYS__PRIVATE_KEY` 或 `config/oxide-arb.local.toml`。
5. 不提交任何密钥。

### 7.2 充值

Live 前才需要真实 USDC。Paper 不需要真实 USDC。

推荐小资金 canary：

| 资产 | 数量 | 用途 |
| --- | --- | --- |
| POL | 0.5-2 | redeem / gas |
| USDC | 200-500 | 小资金 Live canary |

操作：

1. 主账户或交易所向 bot Polygon EOA 转少量 POL。
2. 再转小额 USDC。
3. 确认 Rabby 中 bot 地址到账。
4. 启动后确认 CLOB collateral balance 可读。
5. 设置 `risk.bankroll_usd <= 实际策略资金`。

不要把 USDC 错转到 Polymarket 网站 Proxy/Safe，除非你明确知道该账户模型。

### 7.3 什么时候进钱

| 阶段 | 是否需要真实 USDC |
| --- | --- |
| DryRun | 不需要 |
| Paper | 不需要 |
| Live canary 前 | 需要小额 |
| 放大 Live 前 | 需要在完成 canary 和 settlement/redeem 后逐步加 |

### 7.4 怎么提现

当前系统不提供自动提现。生产 SOP 应为：

1. `POST /api/system/halt`
2. 确认 active reservations = 0
3. 确认 no unknown / needs_reconcile trades
4. 确认 open positions 是否允许提现
5. 确认 CLOB open orders = 0
6. 人工使用 Rabby / wallet 转出
7. 等待 balance refresh
8. 记录 operation log / treasury event
9. 再决定是否 resume

### 7.5 什么时候赚钱

Endgame 策略中，成交时不是最终赚钱。

收益状态应区分：

| 状态 | 含义 |
| --- | --- |
| Detected expected profit | 算法估算，不是真钱 |
| Filled unrealized | 已买入，但结果未结算 |
| Resolved | 市场有结果，但可能未 redeem |
| Redeemed / settled | realized PnL，可视为真实收益 |

只有完成 settlement/redeem 并形成 realized PnL 后，才算确定赚钱。

### 7.6 什么时候亏钱

亏钱来源：

- 买到输的 outcome
- fee 高于 edge
- slippage / FOK miss / 拒单成本
- 错误 market / token mapping
- timeout 后实际成交但内部未记账
- 无法 redeem
- 对账漂移导致停机，错过机会

### 7.7 怎么感知

当前可用：

- `GET /api/system/status`
- `GET /api/system/health`
- `GET /api/pnl/live`
- `GET /api/risk/positions`
- `GET /api/risk/exposure`
- `GET /api/trades`
- `GET /api/opportunities/stats`
- `GET /metrics`
- WebSocket `pnl.update`、`system.status`
- PG `trade`、`position`
- CH `opportunity_audit`

但生产标准要求新增单一资金状态 API。

---

## 8. 测试结果

### 8.1 已运行

```bash
cargo test --workspace --all-targets
```

结果：失败。失败点是 `oxide-arb-models` 的 insta snapshot。

```bash
cargo test -p oxide-arb-risk --all-targets
cargo test -p oxide-arb-core --all-targets
cargo test -p oxide-arb-api --all-targets
```

结果：通过。

### 8.2 未运行但实盘前必须跑

```bash
cargo test-docker
cargo test-network
cargo test -p oxide-arb-core --test production_soak -- --ignored --exact
cargo bench -p oxide-arb-bench --no-run
```

涉及：

- PG / Redis / ClickHouse 集成
- Web RBAC / runtime governance
- Polymarket wiremock network tests
- Live WS probe
- production soak
- benchmark build/SLO

---

## 9. 上线门槛

P0 全部关闭前：

```text
不得切 Live
```

P0 关闭后，小资金 canary 条件：

- 全量测试绿。
- 无 `.snap.new`。
- Live reconciliation 修复。
- FOK timeout unknown 闭环。
- exposure 连续性修复。
- 单一资金状态 API 可用。
- mode transition 核心测试通过。
- 专用 bot 钱包。
- 小额 USDC。
- 明确 halt/resume/withdraw SOP。

canary 期间：

- 单笔 5-25 USDC。
- 日预算 50 USDC 左右。
- 连续监控 24-72h。
- 等至少一个完整 settlement/redeem 周期。
- 不允许自动放大资金。

---

## 10. 最终建议

当前系统不是“不可救”，相反，工程基础不错；但真钱系统的风险集中在少数关键业务语义上。优先级应是：

1. 先修资金真相。
2. 再修未知订单状态。
3. 再修 exposure 连续性。
4. 再补资金状态 API。
5. 最后补 Live 切换测试和生产门槛测试。

这些完成后，才进入小资金 Live canary。不要在 P0 未关闭时用“主链路完整”替代“资金闭环完整”。
