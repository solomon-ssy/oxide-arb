# oxide-arb 实盘生产指南

> **文档类型：** 生产 Readiness 审计 + Live 运维 SOP（统一版）  
> **最后更新：** 2026-06-17  
> **适用范围：** `oxide-arb` 后端 workspace — 数据、检测、风控、执行、post-trade、结算、Web 控制面  
> **读者：** 运维、量化、研发、实盘决策者  

本文件合并并取代以下已删除文档：

- `live-trading-sop.md`（人工充提与 go-live SOP）
- `production-readiness-audit-2026-06-16.md`（生产 Readiness 审计）

相关但不重复的内容见：

- [runbook.md](./runbook.md) — 日常运维主手册（启动、巡检、排错）
- [bankroll-and-risk-metrics.md](./bankroll-and-risk-metrics.md) — 资金模型与 `bankroll_usd` 语义
- [docker-integration.md](./docker-integration.md) / [network-integration.md](./network-integration.md) — CI 集成测试

---

## 1. 一句话结论

`oxide-arb` 是 Polymarket **Endgame Convergence** 套利 bot。主链路已工程化：

```text
Gamma / CLOB WS
  → BookStore → Coalescer / Scanner / Funnel
  → Endgame algorithm / scorer
  → RiskEngine (32 static gates)
  → ExecutionPipeline → Live CLOB FOK
  → PostTradeRelay → Reconciliation → Settlement / Redeem → PnL / Metrics
```

**当前判断（2026-06-17）：**

| 维度 | 评级 | 说明 |
|------|------|------|
| 架构主链路 | ★★★★☆ | 检测→风控→执行→post-trade→对账→redeem 已接线 |
| fail-closed 安全 | ★★★★☆ | 模式切换、reservation、breaker、blocking trades 设计正确 |
| 资金业务闭环 | ★★★☆☆ | CLOB 权威余额已修正；Treasury/漂移历史 API 仍缺 |
| 执行策略完整度 | ★★☆☆☆ | **仅 FOK**；ADR 描述的 FOK+GTD 分层尚未实现 |
| 运维感知 | ★★★★☆ | 单一资金 API、WS、Metrics 丰富；缺仓库内 Prometheus 规则/Grafana |
| **是否可 Live** | **有条件 canary** | 200–500 USDC、24–72h、强人工监控；非无人值守生产 |

```text
┌─────────────────────────────────────────────────────────────┐
│  Live-canary-ready（有条件）                                  │
│  ✅ 主闭环 + P0 资金/Unknown 订单修复已落地                     │
│  ⚠️  仅 FOK、Live 无 publication 为 warn-only、缺外部告警栈    │
│  ❌  非 ADR 完整语义、非大资金无人值守                          │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. 系统能力矩阵

### 2.1 已闭环

| 能力 | 关键模块 | 说明 |
|------|----------|------|
| 数据 ingest + BookStore | `pipeline/book_store.rs` | 热路径只读 `published` ArcSwap |
| Endgame 检测 + 评分 | `oxide-arb-algorithm` | 纯计算；可热重载 |
| 32 gate 静态风控 | `oxide-arb-risk/pipeline/` | 确定性顺序；ShortCircuit / FullReport |
| 断路器 FSM | `circuit_breaker.rs` | Closed→Open→HalfOpen→Recovered / Halted |
| Live FOK 下单 | `fok_strategy.rs`, `api/clob` | 真实 CLOB；FOK **no retry** |
| reservation 生命周期 | `capital_manager.rs`, `exposure/in_memory.rs` | fill keep-on-fill；Unknown pin |
| 未知订单对账 | `post_trade/reconciliation/` | evidence L1–L5；defer-only，从不 blind-miss |
| post-trade 幂等 relay | `post_trade/relay.rs`, `consumer.rs` | notify + poll + stale scan |
| 市场结算 + redeem | `execution/settlement/` | WS `MarketResolved` → on-chain redeem |
| Live 账本对账 | `periodic_services.rs` | CLOB collateral 为唯一 cash truth |
| 模式切换协议 | `control/mode_transition.rs` | preflight→quiesce→commit→activate→resume |
| Trade integrity | `trade_integrity/` | boot rehydrate；`BlockingTradesCheck` |
| 单一资金 API | `GET /api/system/balance` | `SystemBalanceView` |

### 2.2 未闭环 / 已知缺口

| 缺口 | 影响 | 优先级 |
|------|------|--------|
| **FOK+GTD 分层执行** | FOK miss 即结束；与 ADR 不一致 | P1 |
| **Live 无 control-factor publication** | warn-only，neutral-pass 继续交易 | P0 运营 |
| **Treasury 事件/API** | 人工充提无法被系统解释 | P1 |
| **余额 drift 历史 HTTP API** | PG `balance_snapshot` 有写无读 | P1 |
| **Prometheus 规则 + Grafana** | 仓库内无告警配置 | P1 |
| **`needs_reconcile` 专用告警 metric** | 需人工轮询 API | P2 |
| **Paper 模拟偏乐观** | 不能当 Live 收益预测 | 认知 |
| **Proxy vs EOA 充值陷阱** | 仅文档；运行时无检测 | P2 |

---

## 3. 执行与资金语义（必读）

### 3.1 三种执行模式

| 模式 | CLOB 下单 | 余额来源 | 用途 |
|------|-----------|----------|------|
| `DryRun` | 不触 CLOB；模拟 filled | PG 派生模拟账本 | 数据链路、检测、风控 smoke |
| `Paper` | 不触 CLOB；按 BookStore 深度模拟 | PG 派生模拟账本 | fill/miss 粗估（偏乐观） |
| `Live` | 真实 CLOB FOK | **CLOB authoritative collateral** | 真钱交易 |

默认 mode 存于 `system_runtime_state`，新库 seed 为 `dry_run`。切 Live **只能**通过治理 API，不能改 TOML 静默切实盘。

### 3.2 Live 资金模型

```text
链上 USDC（人工 deposit）
    ↓
CLOB collateral balance          ← Live 唯一 cash truth
    ↓
dynamic_equity = cash + position_mark_value
    ↓
available_for_sizing = min(dynamic − reserve − reservations − potential_loss, bankroll_cap)
    ↓
Kelly Quarter sizing → FOK 下单
    ↓
CTF outcome tokens（持仓）
    ↓
market resolves → on-chain redeem → USDC 回 holder 钱包
```

- **`risk.bankroll_usd`**：策略 sizing **上限 cap**，不是钱包余额镜像。
- **`risk.reserve_balance_usd`**：永久保留缓冲（建议默认 $100）。
- 详细推导见 [bankroll-and-risk-metrics.md](./bankroll-and-risk-metrics.md)。

### 3.3 盈亏何时「确定」

| 状态 | 含义 | 是否确定盈亏 |
|------|------|--------------|
| Detected expected profit | 算法 edge 估算 | ❌ |
| Filled unrealized | 已买入，市场未结算 | ⚠️ 浮盈/浮亏 |
| Resolved | 市场有结果，可能未 redeem | ⚠️ |
| **Redeemed / settled** | realized PnL 入账 | ✅ **确定** |

**运营规则：** 以 `GET /api/pnl/daily-series`（settlement basis）和 `trade.settled` WS 为准，不用 `pnl/live` 或 Paper 曲线预测 Live 收益。

### 3.4 订单与 reservation 生命周期

```text
Intent → reserve → Submitted → FOK
  ├─ Filled   → keep reservation → PostTradeRelay → position → release
  ├─ Miss/Failed → release reservation → terminal
  └─ Unknown  → pin reservation → mark Orphaned → Reconciliation Worker
                    → ReconciledFilled/Miss/Failed → relay 或 release
```

**Unknown 触发条件：** FOK HTTP timeout、`PartiallyFilled`、ambiguous CLOB status。  
**禁止语义：** timeout 不得 terminal Failed 并 release reservation（P0-2 已修复）。

---

## 4. Remediation 追踪

### 4.1 P0（Live 阻断级 — 已全部关闭）

| ID | 问题 | 状态 | 说明 |
|----|------|------|------|
| P0-1 | Live 对账 baseline 误用 `bankroll_usd` | **RESOLVED** | `internal_cash = risk_metrics.cash_balance()`；见 `periodic_services.rs` |
| P0-2 | FOK timeout → Failed + release | **RESOLVED** | 返回 `ExecutionOutcome::Unknown` → pin → reconciliation |
| P0-3 | fill 后 exposure 空窗 | **RESOLVED** | keep-on-fill 至 post-trade release；`confirm_sync` 已删除 |
| P0-4 | CI snapshot 红 | **RESOLVED** | `cargo test --workspace --all-targets` 通过（2026-06-17） |
| P0-5 | 缺单一资金 API | **DONE** | `GET /api/system/balance` → `SystemBalanceView` |

### 4.2 P1（canary 期间并行补强）

| ID | 问题 | 状态 |
|----|------|------|
| P1-1 | `mode_transition` 核心协议测试不足 | OPEN |
| P1-2 | Web `POST /api/system/mode` 集成测试偏 mock | OPEN |
| P1-3 | reconcile operator API（venue status、审计视图） | PARTIAL — `GET /trades/reconciliation` 有；专用 review API 缺 |
| P1-4 | Treasury 事件未建模 | DEFERRED — 本 SOP §6 覆盖人工流程 |
| P1-5 | Paper 偏乐观 | KNOWN — 文档约束，非 bug |
| P1-6 | FOK+GTD 分层未实现 | OPEN |
| P1-7 | Live 无 publication 应 hard deny | OPEN — 当前 warn-only |

### 4.3 刻意 warn-only 的行为（运营须知晓）

| 行为 | 位置 | 含义 |
|------|------|------|
| Live 无 control-factor publication | `heartbeat.rs` | Warning 告警；factor gate neutral-pass；**仍允许下单** |
| Reconciliation Warning | `engine.rs` | 不 trip breaker；仅 Critical → L4 halt |
| DryRun 有 blocking trades | `BlockingTradesCheck` | 不 block 新单（模拟专用） |
| `cancel_all` 失败 | `venue_guard.rs` | warn only；halt 已生效 |
| integrity refresh 失败 | `execution_pipeline.rs` | warn only |

---

## 5. 剩余风险摘要

### 5.1 高 — 真钱语义

1. **Live 无 publication 仍可交易** — 治理因子未生效时系统以 neutral-pass 运行。
2. **仅 FOK** — miss 无 GTD 兜底；与 ADR「FOK+GTD layered execution」不符。
3. **Partial fill → Unknown** — 资金 pinned，可能触发 `BlockingTradesCheck` 暂停新单。
4. **Reconciliation Warning 不熔断** — 小幅 drift 可能持续交易。

### 5.2 中 — 运维

- 无仓库内 PrometheusRule / Grafana dashboard。
- `needs_reconcile_count` 无专用 Prometheus gauge（需轮询 `GET /api/system/balance`）。
- 充值到 EOA 而 bot 用 Proxy 时，CLOB collateral 不增加 — 仅文档警告。
- detection 与 execution 的 factor 评估时间点不同，可能 drift。

### 5.3 低 — 工程

- 部分 phase-2 risk gate 缺独立单元测试。
- Admin UI 未作为 Live gate 验证。

---

## 6. 实盘 SOP — 钱包与凭证

### 6.1 专用 bot 钱包（强制）

1. 使用 Rabby（或等价钱包）创建专用地址，例如 `oxide-arb-bot`。
2. **禁止**使用主账户私钥。
3. 私钥仅通过以下方式注入（任选其一，不可提交 git）：
   - 环境变量 `OXIDE_ARB__KEYS__PRIVATE_KEY`
   - 本地 `config/oxide-arb.local.toml`（已在 `.gitignore`）
4. 记录 bot **EOA 地址**与 **Polymarket 交易路径**（EOA 直连 vs Proxy/Safe）。
5. Live 还需要：Polygon RPC、JWT secret（admin API）、notification 凭证（Telegram 或 Webhook）。

### 6.2 Proxy 陷阱（常见资金丢失原因）

若 keystore 通过 **Proxy/Safe** 下单，USDC 必须进入 **该 Proxy 对应的 CLOB collateral 路径**。  
充值到 EOA 而 bot 用 Proxy 交易时：

- Rabby 显示 EOA 有余额
- `GET /api/system/balance` → `cash_balance_usd` **不变**
- bot 无法 sizing / 下单

**验证：** 充值后必须在 Live（或 CLOB balance 探针）确认 `source = authoritative_clob` 且 `cash_balance_usd` 增加。

---

## 7. 实盘 SOP — 分阶段上线

### Phase 0：工程质量门槛（Live 前必跑）

```bash
bash scripts/check-production-gates.sh
```

包含：fmt、clippy、architecture lint、全量 test、`test-network`、`test-docker`、bench SLO/regression、**production_soak**（500 markets，ignored）。

最低验证（无 Docker 时）：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
bash scripts/lint-architecture.sh
cargo test --workspace --all-targets
```

### Phase 1：DryRun（3–7 天）

| 项 | 要求 |
|----|------|
| 真实 USDC | **不需要** |
| 目标 | WS/Gamma 稳定、检测频率、风控 deny 分布 |
| 检查 | `GET /api/system/health` 全绿；CH opportunity funnel 有数据 |
| 因子 | 练习 shadow → publish workflow |

### Phase 2：Paper（7–14 天）

| 项 | 要求 |
|----|------|
| 真实 USDC | **不需要** |
| 目标 | fill/miss 比例、漏斗延迟、funnel spill |
| 禁止 | 用 Paper PnL 预测 Live 收益 |
| 调优 | staleness、depth、loss caps、`bankroll_usd`（模拟 baseline） |

### Phase 3：Live canary（24–72h）

| 参数 | 建议值 |
|------|--------|
| POL | 0.5–2（gas / redeem） |
| USDC | 200–500 |
| 单笔 | 5–25 USDC |
| 日预算 | ~50 USDC |
| 监控 | 连续 24–72h；至少 **1 个完整 settlement/redeem 周期** |
| 放大 | canary 成功前 **禁止** 自动或大幅加仓 |

---

## 8. 实盘 SOP — Pre-flight 检查清单

每次切 Live 或 canary 重启前，逐项确认：

### 8.1 基础设施

- [ ] `GET /api/system/health` — Postgres / Redis / ClickHouse / WS 健康
- [ ] Live 时 CLOB open orders 检查通过（FOK-only 不变量）
- [ ] `GET /metrics` 可 scrape（生产需网络策略限制，非公网暴露）

### 8.2 资金与 integrity

- [ ] `GET /api/system/balance`：
  - [ ] `blocking_trade_count = 0`
  - [ ] `needs_reconcile_count = 0`
  - [ ] `is_authoritative = true`（Live）
  - [ ] `is_stale = false`
  - [ ] `available_for_sizing_usd > 0`（若预期交易）
- [ ] `risk.bankroll_usd ≤` 实际策略 USDC
- [ ] `risk.reserve_balance_usd` 已设缓冲
- [ ] runtime config 已通过 governance activate

### 8.3 治理与因子

- [ ] **强烈建议：** 已 publish control-factor snapshot（否则 `control_factor_live_warn = true`，neutral-pass）
- [ ] notification（Telegram 或 Webhook）已配置 — Live validation 要求至少一路
- [ ] redeem route policy 与 open positions 一致（进入 Live preflight 会查 pending redeem）

### 8.4 凭证

- [ ] `OXIDE_ARB__KEYS__PRIVATE_KEY` 已配置
- [ ] Polygon RPC 可用
- [ ] 确认 holder/proxy 地址与充值目标一致

---

## 9. 实盘 SOP — 人工充值（Deposit）

系统**不提供**自动 deposit 或链上监听。运营者在外部完成转账，bot 只读 CLOB collateral。

### 9.1 操作步骤

1. **（建议）** 若已有 open reservations 或 unknown trades，先 `POST /api/system/halt`。
2. 向配置的 **holder/proxy** 地址转入 USDC（Polygon）。
3. 若钱包 setup 需要，approve Polymarket CTF exchange。
4. 等待链上确认 + CLOB 索引（通常数分钟）。
5. 启动或恢复 bot；等待 metrics refresh（默认 ~5s tick）。
6. 验证：

```http
GET /api/system/balance
```

确认：

- Live：`source = authoritative_clob`
- `cash_balance_usd` 反映充值金额（扣除已有 reservation 占用）

7. 若调整了策略规模，更新 runtime config 并 activate：

```http
POST /api/runtime-config/versions/{id}/activate
```

8. 记录 operation log（人工备注充提原因与 tx hash）。

### 9.2 什么时候需要真钱

| 阶段 | 需要 USDC |
|------|-----------|
| DryRun | 否 |
| Paper | 否 |
| Live canary 前 | 是（200–500） |
| 放大 Live | canary 至少一个 redeem 周期后逐步加 |

---

## 10. 实盘 SOP — 进入 Live

### 10.1 API 调用

```http
POST /api/system/mode
Accept-Api-Version: v1
Authorization: Bearer <jwt>
X-Acting-Role: <governed role>
Content-Type: application/json

{
  "target_mode": "live",
  "reason": "canary start — <ticket or date>"
}
```

### 10.2 模式切换协议（系统自动执行）

```text
1. Preflight   — deploy + runtime validation；Live 需 CLOB/CTF/holder + pending redeem check
               + operational_phase == operational（目录 + 行情 WS 就绪）
2. Quiesce     — halt + cancel_all + 等待 reservations = 0（30s timeout → abort，保持 halted）
3. Commit      — atomic ExecutionModeHandle::store + PG persist
4. Activate    — Live：CLOB metrics refresh + authoritative 断言
5. Resume      — blocking_trades = 0 才 clear FSM
```

**Preflight 运营就绪：** 切换 Live 前执行 `GET /api/system/status`，确认 `operational_phase.phase == operational` 且 `market_data.ready == true`。在 `catalog_warming` 或 `market_data_connecting` 阶段切 Live 会被拒绝 — 这是预期行为，不是 bug。

**Health vs 顶栏：** `GET /api/system/health` 为子系统诊断；Admin 顶栏与 Live 门控读 `operational_phase`。启动后数分钟内 health 可能显示 websocket `skipped`；**不要**将其等同于交易降级。进入 `operational` 后若 WS 持续 stale，phase 会变为 `degraded` 且 Live+Operational 场景下 infrastructure alert 可显式 `affects_trading=true`。

activation 失败 → 系统保持 **halted**（`MetricsFreshnessCheck` 阻止新单）。

### 10.3 切换后验证

```http
GET /api/system/status
GET /api/system/balance
```

确认：

| 字段 | 期望 |
|------|------|
| `execution_mode` | `live` |
| `operational_phase.phase` | `operational` |
| `market_data.ready` | `true` |
| `source` | `authoritative_clob` |
| `is_authoritative` | `true` |
| `blocking_trade_count` | `0` |
| `control_factor_live_warn` | `false`（理想） |

---

## 11. 实盘 SOP — Canary 期间监控

### 11.1 单一资金入口（首选）

```http
GET /api/system/balance
```

| 字段 | 运营含义 |
|------|----------|
| `cash_balance_usd` | CLOB 权威现金 |
| `equity_usd` | cash + 持仓 mark value |
| `bankroll_cap_usd` | 策略上限 |
| `available_for_sizing_usd` | Kelly 可用 |
| `reserved_usd` / `active_reservation_count` | 在途占用 |
| `blocking_trade_count` | >0 → Live 拒绝新单 |
| `needs_reconcile_count` | 未知 venue outcome 待对账 |
| `binding_exposure_limit` | 下一个 sizing 绑定的 cap |
| `metrics_age_secs` / `is_stale` | metrics 是否可信 |

### 11.2 PnL — 分层读取

| 接口 | 语义 | 用途 |
|------|------|------|
| `GET /api/pnl/live` | 实时已实现 + exposure | 盘中 |
| `GET /api/pnl/daily-series` | settlement basis | **真盈亏** |
| `GET /api/analytics/daily` | 分析视角日报 | 复盘 |
| Prometheus `risk_daily_pnl_usd` | 指标 | 告警 |

### 11.3 WebSocket 订阅

连接：`GET /api/ws?token=<jwt>`

建议订阅：

- `system.status` / `system.alert`
- `pnl.update`
- `trade.filled` / `trade.settled`
- `risk.circuit_breaker`

### 11.4 成交与对账

| 接口 | 用途 |
|------|------|
| `GET /api/trades?execution_mode=live` | 成交历史 |
| `GET /api/trades/reconciliation` | needs_reconcile 队列 |
| `GET /api/trades/decisions` | 风控拒绝审计 |
| `GET /api/opportunities/stats` | CH audit funnel |

### 11.5 建议最小告警栈

1. 配置 runtime `notification.telegram` 或 webhook。
2. Prometheus scrape `GET /metrics` + 自建规则：
   - `health_check_failures > 0`
   - WS disconnect 持续时间
   - `risk_daily_pnl_usd` 跌破日损阈值
3. 人工轮询（暂无专用 metric）：
   - `needs_reconcile_count`
   - `blocking_trade_count`

### 11.6 每日巡检

见 [runbook.md](./runbook.md) §11；最低限度：

- `GET /api/system/health`
- `GET /api/system/balance`
- `GET /api/pnl/daily-series`
- `GET /api/trades/reconciliation`
- PG：`trade` terminal 状态分布、`position` redeem 状态

---

## 12. 实盘 SOP — Halt → Reconcile → Withdraw

系统**不提供**自动 withdraw。提现前必须保证内部账本与 venue 一致。

### 12.1 标准顺序

```text
1. POST /api/system/halt
2. 等待 in-flight trades terminalize
3. 处理 needs_reconcile 队列 → needs_reconcile_count = 0
4. 确认 blocking_trade_count = 0
5. 确认 CLOB open orders = 0（GET /api/system/health）
6. 评估 open positions — 未 redeem 的持仓提现后 bot 无法管理
7. 人工 Rabby / wallet 转出 USDC
8. 等待 balance refresh；确认 cash_balance_usd 下降符合预期
9. 记录 operation log（tx hash、原因）
10. POST /api/system/resume — 仅 blocking=0 时成功（否则 409）
```

**禁止：** 在 open reservations、unknown venue outcomes 或 blocking trades 存在时提现或 resume。

### 12.2 Resume 409 处理

`POST /api/system/resume` 返回 `BLOCKING_TRADES_UNRESOLVED`：

1. `GET /api/trades/reconciliation`
2. 等待 reconciliation worker 或 operator 关闭 unresolvable trade
3. 确认 `blocking_trade_count = 0`
4. 重试 resume

---

## 13. 实盘 SOP — 进程重启与 blocking 队列

若 durable 行处于 `Submitted` / `Orphaned` / `Intent`：

1. Boot 执行 `TradeIntegrityStore::boot_rehydrate()`。
2. 可能进入 **planned halt**（FSM halt + `blocking_trade_count > 0`）。
3. 从 PG `reservation_id` 恢复内存 reservation。
4. Reconciliation worker 处理 unknown/orphan。
5. Operator `POST /api/system/resume`（blocking=0 后）。

| 场景 | 操作 |
|------|------|
| Boot 后 halted，`blocking_trade_count > 0` | 查 trades；处理 Submitted/Orphaned/needs_reconcile |
| Intent orphan | 告警 `integrity.intent_orphan`；关闭 trade 或走 reconciliation |
| Live 无 publication | Warning；不 block（见 §4.3） |

---

## 14. 亏钱来源与响应

| 来源 | 感知方式 | 响应 |
|------|----------|------|
| 买到输的 outcome | settled PnL 负 | 策略固有风险；检查 calibration |
| fee 侵蚀 edge | trade fee vs expected profit | 检查 neg_risk / fee category |
| FOK miss | `trades_missed` metric | 机会成本；考虑 GTD 分层（未实现） |
| Unknown → 实际成交 | `needs_reconcile_count` | 等 reconciliation；禁止 resume 直至清零 |
| redeem 失败 | settlement alert + `settlement_*` metrics | runbook settlement SOP |
| 对账 Critical drift | breaker L4 halt | 人工 ack + 调查 drift |
| 误充值地址 | cash_balance 不增 | §6.2 Proxy 陷阱 |

---

## 15. 生产上线门槛（Gate Checklist）

### 15.1 代码与测试

- [ ] `bash scripts/check-production-gates.sh` 全绿
- [ ] 无 insta `.snap.new` 残留
- [ ] `cargo test-docker` / `test-network` 通过

### 15.2 运营

- [ ] 专用 bot 钱包 + 私钥隔离
- [ ] 200–500 USDC + POL 到账且 CLOB 可读
- [ ] `bankroll_usd` / loss caps / notification 已配置
- [ ] control-factor Published（强烈建议）
- [ ] halt / resume / withdraw SOP 演练至少一次
- [ ] Telegram 或 Webhook 告警实测收到

### 15.3 Canary 退出条件（放大前）

- [ ] 连续 24–72h 无 unresolved blocking / reconcile
- [ ] 至少 1 次完整 **settlement → redeem → realized PnL**
- [ ] 无 L4 reconciliation halt
- [ ] 人工复盘 CH audit funnel + 日 PnL 与预期一致

---

## 16. 关键代码索引

| 主题 | 路径 |
|------|------|
| 模式切换 | `crates/oxide-arb-core/src/control/mode_transition.rs` |
| 执行热路径 | `crates/oxide-arb-core/src/execution/execution_pipeline.rs` |
| FOK + Unknown | `crates/oxide-arb-core/src/execution/fok_strategy.rs` |
| 对账 worker | `crates/oxide-arb-core/src/post_trade/reconciliation/` |
| 结算 redeem | `crates/oxide-arb-core/src/execution/settlement/service.rs` |
| Live 账本对账 | `crates/oxide-arb-core/src/app/periodic_services.rs` |
| 资金视图 | `crates/oxide-arb-core/src/control/status.rs` |
| 32 risk gates | `crates/oxide-arb-risk/src/pipeline/checks.rs` |
| 生产门槛脚本 | `scripts/check-production-gates.sh` |
| HTTP 路由 | `crates/oxide-arb-web/src/routes/mod.rs` |

---

## 17. 后续改进优先级（工程）

1. Live 无 publication → **hard deny**（或 canary 限时 warn 后 deny）
2. 仓库内 **Prometheus 规则 + Grafana** dashboard
3. **`GET /api/system/balance/history`**（PG drift）
4. **FOK+GTD TieredExecutionStrategy**
5. **TreasuryEvent** 表（解释充提，非第二 cash truth）
6. **mode_transition** 端到端测试
7. 同步更新 `production-p0-p1-remediation-plan.md` 状态

---

## 18. 文档变更记录

| 日期 | 变更 |
|------|------|
| 2026-06-16 | 初版审计 + 独立 SOP |
| 2026-06-17 | 合并为本文；更新 P0-2/P0-4 为 RESOLVED；反映 2026-06-17 测试通过 |
