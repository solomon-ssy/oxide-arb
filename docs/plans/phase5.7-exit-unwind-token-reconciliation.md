# Phase 5.7 — Exit / Unwind Design & Token-Level Reconciliation

> **状态**: Production Design Target  
> **父计划**: `docs/plans/phase5-replay-analytics.md`  
> **前置依赖**: Phase 5.1, Phase 5.3, Phase 5.6  
> **覆盖原章节**: 12, 15.2 balance/token tables, 18.4 exit/token items, 18.7 exit UI item  
> **目标**: 把主动退出、SELL unwind、token inventory reservation、ERC1155 token-level reconciliation 和 exit evidence 明确为 Phase 5 的独立风险闭环。第一版先 report-only/manual review，禁止在 token truth 不完整时自动退出。

---

## 0. 结论

Phase 5 主线仍是 control factor materialization，但 endgame 风险闭环如果只覆盖买入和结算，会留下明显缺口：

> 开仓后如果 thesis 失效、oracle/news/market status 改变、价格离开 endgame zone、账实漂移或二级市场仍有可接受 bid，系统是否应主动减仓或退出？

结论：

- Phase 5 必须把主动退出设计清楚，并补齐 evidence/schema/API/accounting 边界。
- 是否第一版 live 自动执行 exit-sell，可以按风险分级推进。
- 完整链上 token balance 对账必须进入 Phase 5，因为没有 token-level custody truth，主动 sell/redeem/merge 都不可靠。

---

## 1. 当前代码现实

已有能力：

- `Side::Sell` 存在于领域枚举。
- `Dispatcher::available_depth_at_price` 对 `Side::Sell` 使用 `book.bid_depth_down_to(limit_price)`。
- `ClobClient::place_order` 将 `OrderRequest.side` 传给 SDK，理论上可提交 SELL。
- `ExecutionPlan.side` 来自 `Opportunity.side`。
- `RiskMetricsState` 里已有 open buy/sell count 和 daily buy/sell trades 计数。

缺口：

- `EndgameDetector` / `OpportunityPipeline` 输出 buy-only opportunity，没有 exit signal。
- `PlanBuilder::build` 根据 `approved_size / entry_price` 算 buy shares，不适合 SELL：SELL amount 是 shares，资金语义不同。
- `ExecutionPipeline` 是 entry pipeline，不区分 entry intent 与 exit intent。
- `CapitalManager` reservation 语义是 USD capital reservation，不是 token inventory reservation。
- `PositionRepository` 有 close/settle/redeem，但没有部分减仓、多次 sell fill、exit plan、exit reason、exit PnL attribution。
- `LedgerReconciler` 当前按 market exposure value 对账，不是 ERC1155 `token_id + shares` 级别对账。
- CLOB SELL 需要 conditional token allowance；当前文档/代码未把 ERC1155 approval/allowance 纳入 startup assertion。
- 没有 exit runner / exit scheduler / unwind task。

---

## 2. 是否自动退出

| 场景 | 是否建议自动退出 | 原因 |
|---|---:|---|
| 价格短暂波动但 thesis 未变，接近结算 | 默认不退出 | endgame 流动性薄，stop 可能卖飞 |
| oracle/news/market status 明确推翻 thesis | 建议减仓或退出 | 原始 probability edge 失效 |
| 价格离开 endgame zone 且持续超时 | 建议进入 exit review/time stop | 收敛 thesis 可能失效 |
| bid depth 足够且亏损可控 | 可执行 partial unwind | 有可行二级市场路径 |
| bid depth 薄或 spread 极宽 | 不强制 market sell | 强行卖出可能比 hold-to-resolution 更差 |
| reconciliation critical drift | 禁止新开仓；exit 需人工或安全策略 | token truth 不可靠时自动卖出危险 |
| resolved/redeemable | 不走 sell，走 redeem | winner token worth $1，sell 可能不必要 |

主动退出不是“价格跌了就卖”。它必须是：

```text
ExitDecision
  -> ExitPlan
  -> ExitExecution
  -> Accounting
  -> Audit
```

---

## 3. Exit Trigger / Action

```rust
pub enum ExitTriggerType {
    FixedStop,
    TrailingStop,
    TimeStop,
    EndgameZoneInvalidation,
    OracleNewsInvalidation,
    MarketStatusChange,
    ReconciliationCritical,
    ManualOperator,
}

pub enum ExitAction {
    Hold,
    Reduce { target_fraction: Decimal },
    FullExit,
    ManualReview,
    RedeemIfResolved,
}
```

### 3.1 Fixed stop

适合普通 prediction market，但 near-resolution endgame 要谨慎。

```text
trigger if:
  best_bid <= entry_price - stop_distance
  and bid_depth_at_worst_price >= min_exit_shares
  and market_status == active
  and not within no_exit_before_resolution_window
```

### 3.2 Trailing stop

只在 position 先显著盈利后激活，避免刚开仓就被薄盘口扫出。

```text
activation:
  best_bid >= entry_price + activation_delta

stop:
  max(entry_price + breakeven_fee_buffer, high_water_bid - trail_distance)
```

### 3.3 Time stop

用于“收敛太早但迟迟不结算”的场景。

```text
trigger if:
  position_age_secs > max_endgame_hold_secs
  and market not resolved
  and price no longer in endgame zone
```

### 3.4 Endgame zone invalidation

```text
trigger if:
  current_best_bid < exit_zone_floor
  for invalidation_grace_secs
```

### 3.5 Oracle/news/market status invalidation

需要事件源：

- UMA oracle status；
- Gamma market status；
- manual incident；
- trusted news/oracle adapter；
- market paused/closed/disputed。

第一版建议只做 `ManualOperator` + `MarketStatusChange` + `OracleStatusChange`。News 自动解析后续接入。

---

## 4. Exit Pipeline

主动退出不能复用 entry FOK buy path。需要新的 exit pipeline：

```text
ExitSignal
  -> PositionInventoryResolver
  -> ExitPolicyEngine
  -> ExitPlanBuilder
  -> TokenReservation
  -> SellValidator
  -> ExitRiskGate
  -> CLOB SELL FOK/FAK
  -> ExitAccounting
  -> PositionPatch
  -> Audit / Materialization facts
```

### 4.1 BUY vs SELL 差异

| 维度 | BUY 开仓 | SELL 退出 |
|---|---|---|
| amount 语义 | USD 预算 | 要卖出的 shares |
| book side | asks | bids |
| limit price | max buy price | min sell price |
| reservation | USD 资金 | ERC1155 token shares |
| allowance | pUSD / collateral | conditional token allowance |
| accounting | open/increase position | reduce/close position, realized exit PnL |
| risk | pre-trade capital risk | execution/slippage + thesis invalidation risk |

### 4.2 Execution method progression

| 方法 | 适用场景 | 风险 |
|---|---|---|
| FOK SELL | 小仓位、深 bid、必须全部退出 | 容易 miss |
| FAK SELL | 可接受部分减仓 | 需要 partial accounting |
| 分片 unwind | 仓位大于 visible depth | 执行复杂，需防止自我冲击 |
| Maker exit GTC/GTD | 想减少滑点 | 增加挂单管理和取消风险 |
| 持有到结算 | bid 太薄、thesis 仍有效 | 承担 binary tail risk |

Phase 5 第一版推进顺序：

1. 支持 `ExitPlan` 和 report-only materialization。
2. 支持 manual review。
3. 支持 FOK SELL full exit。
4. 支持 FAK partial reduce。
5. 最后考虑 sliced/maker unwind。

---

## 5. Exit Data Model

新增 PG 表：

```text
position_exit_plan
position_exit_execution
position_unwind_audit
```

### 5.1 `position_exit_plan`

```text
exit_plan_id UUID primary key
position_id UUID not null
market_id text not null
token_id text not null
trigger_type text not null
action text not null
target_shares decimal not null
min_exit_price decimal not null
reason jsonb not null
policy_version text not null
created_by text not null
status text not null
created_at timestamptz not null
updated_at timestamptz not null
```

### 5.2 `position_exit_execution`

```text
exit_execution_id UUID primary key
exit_plan_id UUID not null
order_id text null
order_type text not null
requested_shares decimal not null
filled_shares decimal not null
avg_exit_price decimal null
fee_usd decimal not null
realized_exit_pnl_usd decimal not null
outcome text not null
failure_reason text null
submitted_at timestamptz null
completed_at timestamptz null
```

### 5.3 `position_unwind_audit`

```text
audit_id UUID primary key
position_id UUID not null
event_type text not null
before_position jsonb not null
after_position jsonb not null
book_context jsonb not null
token_balance_context jsonb not null
reason text not null
created_at timestamptz not null
```

### 5.4 Position/trade extensions

Position：

```text
remaining_shares
realized_exit_pnl_usd
exit_status
last_exit_at
exit_reason
```

Trade：

```text
intent_type -- EntryBuy / ExitSell / Redeem / Merge
parent_position_id
exit_plan_id
```

---

## 6. Token-Level Reconciliation

当前 reconciliation 主要对比 internal balance/external balance/market exposure，不能证明每个 token 的实际 custody。

必须新增：

```rust
pub trait TokenBalanceQuerier {
    async fn erc1155_balances(
        &self,
        holder: Address,
        token_ids: &[TokenId],
    ) -> OxideResult<Vec<TokenBalanceSnapshot>>;

    async fn allowance_for_exchange(
        &self,
        holder: Address,
        operator: Address,
    ) -> OxideResult<bool>;
}
```

`token_balance_snapshot` 至少包含：

```text
snapshot_id UUID primary key
holder_address text not null
market_id text not null
token_id text not null
side text not null
internal_shares decimal not null
external_shares decimal not null
drift_shares decimal not null
block_number bigint null
observed_at timestamptz not null
source text not null -- CLOB API / on-chain ERC1155 / subgraph
```

Reconciliation 必须能回答：

- PG open positions 按 token 聚合后有多少 shares？
- wallet/proxy/funder 实际 ERC1155 balance 有多少 shares？
- CLOB exchange 是否有 conditional token allowance？
- position 已 settlement/redeem 后 token balance 是否归零？
- 是否存在外部 token 但 PG 无 position？
- 是否存在 PG open position 但链上无 token？

---

## 7. Exit 与 Control Factor 的关系

主动退出不应该直接塞进现有五类 factor，但需要被 Phase 5 evidence 支撑。

```rust
pub enum ExitControlPolicy {
    Disabled,
    ReportOnly,
    ManualReview,
    AutoReduce,
    AutoExit,
}
```

建议先作为 runtime config version + evidence report，而不是第六类 factor。等 exit evidence 充足后，可以引入：

```text
ExitQualityFactor
```

第一版不建议自动生成 `ExitQualityFactor`，避免把未验证的二级市场卖出逻辑直接推入 live。

---

## 8. Exit Materialization

Materialization 必须支持 report-only 模拟：

```text
For each historical filled position:
  reconstruct bid book after entry
  simulate fixed stop / trailing stop / time stop / zone invalidation
  compare:
    hold_to_resolution_pnl
    exit_pnl_after_slippage
    missed_recovery_count
    avoided_tail_loss
    false_exit_count
    executable_exit_rate
```

Exit 策略只有在以下条件满足后才能从 report-only 进入 shadow：

- 有 token-level balance reconciliation。
- 有 sell-side L2 bid book coverage。
- 有 exit accounting model。
- 有 enough historical examples。
- Shadow 显示不会系统性卖飞最终正确仓位。

---

## 9. Tests

| 测试 | 必需场景 |
|---|---|
| SELL plan | distinguish USD budget from shares amount |
| Token reservation | cannot sell more shares than reconciled inventory |
| Allowance | missing ERC1155 allowance blocks SELL startup/execution |
| FOK SELL | full fill、miss、insufficient bid depth |
| FAK SELL | partial fill accounting、remaining shares |
| Exit accounting | realized exit PnL、fees、position patch |
| Exit evidence | fixed stop、trailing stop、time stop、zone invalidation |
| Token reconciliation | internal-only、external-only、drift、redeemed position token balance |
| Critical drift | no new entries，auto-exit disabled unless policy/manual path allows |

---

## 10. 退出条件

Phase 5.7 完成后必须满足：

1. Report-only exit materialization 可运行并输出 executable/false-exit/avoided-tail-loss metrics。
2. Token-level reconciliation 能按 `token_id + shares` 对账 internal vs external balances。
3. SELL plan 区分 USD budget 和 shares amount。
4. Exit schema 支持 plan/execution/audit/partial reduce。
5. ERC1155 allowance 被纳入 startup/execution assertion。
6. Auto-exit 默认关闭；进入 ManualReview/AutoReduce/AutoExit 必须经过明确 policy activation。
7. Critical reconciliation drift 不会触发盲目自动卖出。

---

## 11. 阻止启用 Auto Exit 的情况

- 没有 token-level balance reconciliation。
- 没有 sell-side bid book coverage。
- 没有 token inventory reservation。
- 没有 ERC1155 allowance check。
- 没有 partial fill accounting。
- Exit evidence 显示系统性 false exit。
- Reconciliation critical drift 未解决。
