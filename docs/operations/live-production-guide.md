# Live 生产上线指南

面向**运维 / 量化 / 决策**的 Live 推广 SOP。日常操作细节见 [runbook.md](./runbook.md)；本文聚焦 **Readiness 审计 → Canary 规模 → 上线验证 → 事故恢复 → Production Gates**。

**执行策略**：Endgame Live 为 **FOK-only**（不挂 resting GTD）。原因：收敛窗口短、resting 订单资本占用与对账复杂度高、与 endgame 时效性不匹配。

---

## 1. Readiness 审计清单

上线前逐项确认（全部通过才可切 Live）：

| 类别 | 检查项 | 通过标准 |
|------|--------|----------|
| 基础设施 | Postgres / Redis / ClickHouse 可达 | 健康检查 `GET /system/health` 全绿 |
| 凭证 | CLOB API key、链上 signer、Proxy 地址 | `Settings::ensure_valid_for_mode(Live)` 无报错 |
| 资金 | `risk.bankroll_usd` 与链上可用余额对齐 | 见 [bankroll-and-risk-metrics.md](./bankroll-and-risk-metrics.md) |
| 因子 | Control factors **published** | `control_factor_snapshot_expired = false` |
| 完整性 | 对账队列 | `blocking_trade_count = 0` |
| 模式路径 | DryRun → Paper 已跑通 | 至少 24h Paper 无未解释 reject |
| 门禁 | Production gates | `bash scripts/check-production-gates.sh` 全绿 |

---

## 2. Canary 规模模板

按资金档位选择运行时配置（`runtime_config` / `oxide-arb.toml`）：

| 档位 | `bankroll_usd` | `max_single_bet` | `max_daily_loss` | 说明 |
|------|----------------|------------------|------------------|------|
| Micro | $200 | $20 | $40 | 首次 Live，1–3 天 |
| Small | $500 | $50 | $100 | 验证 slippage / 结算后扩量 |
| Normal | 自定义 | ≤ bankroll 10% | ≤ bankroll 20% | 稳定运行档 |

Canary 期间：**禁止**同时调高 bankroll 与 exposure 上限；一次只改一个旋钮并观察 15 分钟 WS `system.status`。

---

## 3. 上线 SOP（DryRun → Paper → Live）

1. **DryRun**：确认检测、风控 reject 原因、ClickHouse 写入正常。
2. **Paper**：观察 FOK 模拟路径、对账队列始终为空。
3. **Governed 切 Live**：Admin UI 或 `POST /system/mode`（需 `X-Acting-Role` + reason，confirm word `live`）。
4. **切后 15 分钟验证**：
   - `operational_phase = operational`
   - `execution_emergency.active = false`
   - `market_data.ready = true`
   - Prometheus：`oxide_arb_execution_fok_*` 有计数且无 persistence fault 告警
   - UI Integrity Banner **无 critical 项**

---

## 4. 资金进入 / 退出 SOP

### 进入

1. EOA 充值 USDC（Polygon）。
2. 必要时经 Proxy 转入 CLOB 抵押账户。
3. 更新 `risk.bankroll_usd`（governed runtime config），等待 risk snapshot 刷新。
4. 确认 `GET /system/balance` 的 `available_for_sizing_usd` 与预期一致。

### 退出

1. `POST /system/halt`（或 UI 停机）。
2. 等待在途 trade 收敛；处理对账 Tab 直至 `blocking = 0`。
3. 结算 / redeem 开放仓位（见 runbook 结算章节）。
4. 链上 withdraw；**勿**在 blocking trades 存在时 ack emergency。

---

## 5. 盈亏感知矩阵

| 来源 | 用途 | 延迟 |
|------|------|------|
| `GET /system/balance` + Dashboard KPI | 敞口、可用资金、integrity 计数 | WS ~秒级 |
| `GET /pnl/live` | 日 / 总 realized PnL | WS + REST |
| Prometheus `oxide_arb_*` | 告警、Grafana |  scrape 间隔 |
| Telegram / 告警 dispatcher | critical / emergency | 事件驱动 |
| ClickHouse opportunity / trade facts | 复盘、因子 replay | 分钟级 |

---

## 6. 事故 Runbook（决策树）

```text
Unknown FOK 结果？
  └─→ Trades → Reconciliation Tab → 标记不可解析（governed）
      └─→ blocking 清零后再继续

PersistenceFault / ReservationFault？
  └─→ 先对账 queue（blocking = 0）
      └─→ POST /system/emergency/ack（UI: Emergency Ack）
          └─→ 若 risk halt 仍 latched → POST /system/resume

blocking trades > 0？
  └─→ Integrity Banner → 对账 Tab；禁止 emergency ack

Ledger drift L4 halt？
  └─→ 排查 Postgres vs CLOB vs CTF → resume（非 emergency ack）

Redeem terminal failure？
  └─→ runbook 结算章节 + 人工链上操作
```

**恢复顺序（强制）**：对账 queue → Emergency Ack → Resume。

---

## 7. Production Gates

Live 推广前 **必须** 本地或 CI 跑通：

```bash
bash scripts/check-production-gates.sh
```

包含：`fmt`、`clippy`、架构 lint、全 workspace 测试、`test-network`、`test-docker`、bench SLO/regression、e2e bench、production soak（ignored）。

CI 另含 `cargo test -p oxide-arb-core --test execution_pipeline_live` 防止 Live 路径回归。

---

## 8. 相关文档

- [runbook.md](./runbook.md) — 主运维手册
- [bankroll-and-risk-metrics.md](./bankroll-and-risk-metrics.md) — 资金 vs bankroll
- [ADR-001 FOK-only](../plans/ADR-001-single-strategy-single-platform.md)
