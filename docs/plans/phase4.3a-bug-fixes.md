# Phase 4.3a — Bug Fixes

> **状态**: 待实施
>
> **前置**: 无（可独立执行）
>
> **影响 crate**: `oxide-arb-models`, `oxide-arb-risk`, `oxide-arb-core`

---

## 1. `check_loss_caps()` breaker level 语义修正

### 现状

`oxide-arb-risk/src/engine.rs` L396-442: weekly loss cap 和 single-loss cap 都用 `CircuitBreakerLevel::Daily` halt。

### 问题

- Weekly loss 突破是比 daily 更严重的事件，应使用 `System` 级别 halt
- `highest` 的 `map_or` 链冗余且易错

### 方案

```rust
fn check_loss_caps(&self) -> Option<CircuitBreakerLevel> {
    let mut highest: Option<CircuitBreakerLevel> = None;

    // Daily — L3 halt
    let daily_loss = self.daily.read().daily_loss();
    if daily_loss.inner() >= self.config.max_daily_loss_usd {
        let reason = format!("daily loss cap breached: {daily_loss}");
        self.circuit_breaker.write().halt(CircuitBreakerLevel::Daily, reason);
        escalate(&mut highest, CircuitBreakerLevel::Daily);
    }

    // Weekly — L4 System halt (更严重)
    let weekly_loss = self.weekly.read().weekly_loss();
    if weekly_loss.inner() >= self.config.max_weekly_loss_usd {
        let reason = format!("weekly loss cap breached: {weekly_loss}");
        self.circuit_breaker.write().halt(CircuitBreakerLevel::System, reason);
        escalate(&mut highest, CircuitBreakerLevel::System);
    }

    // Single-loss — L3 Daily halt
    let max_single = self.daily.read().stats().max_single_loss;
    if max_single.inner() >= self.config.max_single_loss_usd {
        let reason = format!("single loss cap breached: {max_single}");
        self.circuit_breaker.write().halt(CircuitBreakerLevel::Daily, reason);
        escalate(&mut highest, CircuitBreakerLevel::Daily);
    }

    // Hourly — L2 Session trip (auto-recovery)
    let hourly_loss = self.hourly.read().hourly_loss();
    if hourly_loss.inner() >= self.config.max_hourly_loss_usd {
        let reason = format!("hourly loss cap breached: {hourly_loss}");
        self.circuit_breaker.write().trip(CircuitBreakerLevel::Session, reason);
        escalate(&mut highest, CircuitBreakerLevel::Session);
    }

    highest
}

#[inline]
fn escalate(current: &mut Option<CircuitBreakerLevel>, new: CircuitBreakerLevel) {
    *current = Some(current.map_or(new, |c| c.max(new)));
}
```

### 测试

`tests/engine_tests.rs` 新增：
- `weekly_loss_cap_triggers_system_halt`: 触发 weekly 后验证 breaker state 是 `Halted { level: System, .. }`
- `single_loss_cap_triggers_daily_halt`: 触发 single-loss 后验证 `Halted { level: Daily, .. }`

---

## 2. `PotentialLossInfo` shares/price 补全

### 现状

`oxide-arb-risk/src/engine.rs` L337-367 的 `apply_fill()`：`shares` 和 `entry_price` 硬编码为 `Decimal::ZERO`。

### 问题

- 持久化的 potential_loss entries 缺失关键交易信息，无法审计
- 对账时无法基于 shares/price 做细粒度 reconciliation

### 方案

**Step 1**: `oxide-arb-models/src/domain/trade.rs` — `PostTradeInput` 增加字段：

```rust
pub struct PostTradeInput {
    pub trade_id: TradeId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub outcome: TradeOutcome,
    pub cost_usd: Usd,
    pub fee_usd: Usd,
    pub net_profit_usd: Option<Usd>,
    pub shares: Shares,       // NEW
    pub entry_price: Price,   // NEW
}
```

**Step 2**: `oxide-arb-models/src/domain/trade.rs` — `From<&TradeInfo>` 实现同步更新：

```rust
impl From<&TradeInfo> for PostTradeInput {
    fn from(t: &TradeInfo) -> Self {
        Self {
            // ... existing fields ...
            shares: t.shares,
            entry_price: t.price,
        }
    }
}
```

**Step 3**: `oxide-arb-risk/src/engine.rs` `apply_fill()` 用真实值：

```rust
self.potential_loss.write().record_entry(PotentialLossInfo {
    // ...
    shares: trade.shares,         // was: Shares::new(Decimal::ZERO)
    entry_price: trade.entry_price, // was: Price::new(Decimal::ZERO)
    max_loss_usd: trade.cost_usd + trade.fee_usd,
    // ...
});
```

**Step 4**: `oxide-arb-core/src/execution/execution_pipeline.rs` `process_post_trade_job()` 构造 `PostTradeInput` 时填入真实值：

```rust
let fill_input = PostTradeInput {
    // ... existing fields ...
    shares: job.filled_shares,    // from PostTradeJob
    entry_price: job.entry_price, // from PostTradeJob
};
```

这要求 `PostTradeJob` 也增加 `filled_shares: Shares` 字段（参见 phase4.3e）。

### 兼容处理

所有 test mock 中构造 `PostTradeInput` 的地方需要补上 `shares` 和 `entry_price`。搜索 `PostTradeInput {` 全 workspace 更新。

---

## 3. `neg_risk` 硬编码修复

### 现状

`oxide-arb-core/src/execution/plan_builder.rs` L47: `neg_risk: false` hardcoded。

### 问题

Polymarket neg-risk 市场需要 `neg_risk: true` 才能正确计算费率和选择合约。Live 模式下会导致：
- Fee 计算使用错误的 exponent（neg-risk 和 non-neg-risk 的费率公式不同）
- CLOB 签名可能选错 exchange contract address

### 方案

`PlanBuilder` 增加 `market_registry: Arc<MarketRegistry>` 依赖：

```rust
pub struct PlanBuilder {
    fee_calculator: Arc<FeeCalculator>,
    market_registry: Arc<MarketRegistry>,  // NEW
}

impl PlanBuilder {
    pub fn build(&self, opp: &Opportunity, ...) -> ExecutionPlan {
        let neg_risk = self.market_registry
            .get_market(&opp.market_id)
            .is_some_and(|m| m.neg_risk);

        ExecutionPlan {
            // ...
            neg_risk,  // was: false
            // ...
        }
    }
}
```

**`build.rs` 改动**: `wire_execution_loop` 中构造 `PlanBuilder` 时传入 `detection.market_registry`。

### 测试

`tests/validator_tests.rs` 增加场景：注册一个 neg_risk market → 验证 plan.neg_risk == true。

---

## 4. `snapshot()` 中 `last_emergency_at/reason` 永远 None

### 现状

`oxide-arb-risk/src/engine.rs` L796-856: `snapshot()` 中 `last_emergency_at: None, last_emergency_reason: None` 硬编码。

### 问题

重启恢复后无法知道上次 emergency 的原因和时间。

### 方案

在 `RiskEngine` 中增加字段：

```rust
pub(crate) last_emergency: RwLock<Option<(DateTime<Utc>, String)>>,
```

**`record_emergency()`** 写入：

```rust
async fn record_emergency(&self, level: CircuitBreakerLevel, reason: &str, ...) -> OxideResult<()> {
    *self.last_emergency.write() = Some((self.clock.now(), reason.to_owned()));
    // ... existing logic ...
}
```

**`snapshot()`** 读取：

```rust
let (last_emergency_at, last_emergency_reason) = self.last_emergency.read()
    .as_ref()
    .map(|(at, reason)| (Some(*at), Some(reason.clone())))
    .unwrap_or((None, None));
```

**`builder.rs`**: 构造 `RiskEngine` 时初始化为 `RwLock::new(None)`。如果从 snapshot 恢复且 snapshot 有 emergency 信息，则预填充。

### 测试

`tests/engine_tests.rs` — halt 后调用 `snapshot()`，验证 `last_emergency_at.is_some()` 和 `last_emergency_reason` 非空。

---

## 5. `BreakerRecovered` 审计事件发出

### 现状

`CircuitBreaker::tick()` 可以完成 `Recovered → Closed` 转换，但 `RiskEngine::tick()` 不 emit `BreakerRecovered` 审计事件。

### 问题

Breaker 恢复过程在审计日志中不可见，无法追溯何时恢复。

### 方案

`oxide-arb-risk/src/engine.rs` `tick()` 中，在 `cb_transitioned` 后增加审计：

```rust
pub async fn tick(&self, metrics: &dyn RiskMetrics) -> OxideResult<bool> {
    let pre_state = self.circuit_breaker.read().state().to_name();  // NEW
    let cb_transitioned = self.circuit_breaker.write().tick();

    // ... existing rollover logic ...

    // NEW: emit BreakerRecovered audit
    if cb_transitioned {
        let post_state = self.circuit_breaker.read().state().to_name();
        if pre_state == BreakerStateName::Recovered && post_state == BreakerStateName::Closed {
            let audit = RiskAuditEvent::BreakerRecovered;
            let _ = self.persistence.create_audit(audit.into()).await;
        }
    }

    // ... existing persist/rollover audit logic ...
}
```

### 测试

`tests/engine_tests.rs` — 手动驱动 breaker 到 Recovered 状态，`tick()` 后验证 audit event 被创建。
