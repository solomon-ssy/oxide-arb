# Phase 4.3d — Execution Layer Hardening

> **状态**: 待实施
>
> **前置**: 4.3a (PostTradeInput 扩展), 4.3b (pipeline split)
>
> **影响 crate**: `oxide-arb-core`

---

## 1. Live 执行超时保护

### 现状

`oxide-arb-core/src/execution/tiered_strategy.rs` L86: `clob.place_order(&req).await` 无超时包裹。CLOB API hang 时 runner shard 永久阻塞。

### 方案

`OrderStrategy` 增加 `dispatcher_timeout_ms: u64` 字段，`execute_live_fok()` 包裹 `tokio::time::timeout`：

```rust
async fn execute_live_fok(&self, plan: &ExecutionPlan, trace: &mut LatencyTrace) -> ExecutionOutcome {
    let Some(clob) = &self.clob_client else { /* ... */ };
    let timeout = Duration::from_millis(self.dispatcher_timeout_ms);
    let started = Instant::now();
    let req = OrderRequest { /* ... */ };

    trace.mark_http_sent();
    observe_tick_to_http(trace, &self.metrics);

    match tokio::time::timeout(timeout, clob.place_order(&req)).await {
        Ok(Ok(resp)) => {
            let outcome = map_order_response(
                resp, plan, ExecutionMode::Live, started,
                &self.fee_calculator, plan.category, &plan.token_id,
            );
            self.record_tier_metrics(&outcome);
            outcome
        }
        Ok(Err(e)) => {
            self.metrics.tier_misses.with_label_values(&[TIER_FOK]).inc();
            ExecutionOutcome::Failed {
                error: e.to_string(),
                execution_mode: ExecutionMode::Live,
            }
        }
        Err(_) => {
            self.metrics.tier_misses.with_label_values(&[TIER_FOK]).inc();
            tracing::error!(
                execution_id = %plan.execution_id,
                timeout_ms = self.dispatcher_timeout_ms,
                "CLOB order timed out"
            );
            ExecutionOutcome::Failed {
                error: format!("CLOB order timeout after {}ms", self.dispatcher_timeout_ms),
                execution_mode: ExecutionMode::Live,
            }
        }
    }
}
```

**`build.rs`** 构造时传入：`settings.execution.timeout.dispatcher_timeout_ms`。

### 测试

`tests/execution_integration.rs`: wiremock 设置 delay > timeout → 验证 `ExecutionOutcome::Failed` 包含 "timeout"。

---

## 2. Fee 计算统一 — 始终用 FeeCalculator

### 现状

- `clob_outcome::map_order_response` 使用 `resp.fee_paid`（CLOB 返回值，不可靠）
- `Dispatcher::paper_trade/dry_run` 使用 `plan.estimated_fee`（detection 时估算值）

### 方案

**所有路径统一用 `FeeCalculator::calculate(filled_shares, avg_fill_price, category, token_id)` 计算。**

#### 2a. `OrderStrategy` 增加 `fee_calculator`

```rust
pub struct OrderStrategy {
    execution_mode: ExecutionMode,
    clob_client: Option<Arc<ClobClient>>,
    fee_calculator: Arc<FeeCalculator>,  // NEW
    dispatcher_timeout_ms: u64,           // NEW (from §1)
    metrics: Arc<MetricsHub>,
}
```

#### 2b. `map_order_response` 签名扩展

**文件**: `oxide-arb-core/src/execution/clob_outcome.rs`

```rust
pub fn map_order_response(
    resp: OrderResponse,
    plan: &ExecutionPlan,
    mode: ExecutionMode,
    started: Instant,
    fee_calculator: &FeeCalculator,   // NEW
    category: MarketCategory,          // NEW
    token_id: &TokenId,                // NEW
) -> ExecutionOutcome {
    // ... existing match ...
    OrderStatus::Filled | OrderStatus::PartiallyFilled => {
        let price = resp.avg_fill_price.or(Some(plan.limit_price));
        // NEW: 统一计算 fee
        let fee = fee_calculator.calculate(
            resp.filled_shares, price.unwrap_or(plan.limit_price), category, token_id
        );
        ExecutionOutcome::Filled {
            fee_paid: fee,  // was: resp.fee_paid
            // ... rest unchanged ...
        }
    }
}
```

#### 2c. `Dispatcher` 增加 `fee_calculator`

```rust
pub struct Dispatcher {
    execution_mode: ExecutionMode,
    fee_calculator: Arc<FeeCalculator>,  // NEW
    metrics: Arc<MetricsHub>,
}
```

`paper_trade` 和 `dry_run` 中用真实计算替换 `plan.estimated_fee`：

```rust
fn paper_trade(&self, plan: &ExecutionPlan) -> ExecutionOutcome {
    // ... depth check (see §4) ...
    let fee = self.fee_calculator.calculate(
        plan.shares, plan.limit_price, plan.category, &plan.token_id
    );
    ExecutionOutcome::Filled {
        fee_paid: fee,  // was: plan.estimated_fee
        // ...
    }
}
```

#### 2d. `ExecutionPlan` 需要 `category` 字段

`ExecutionPlan` 目前没有 `category: MarketCategory` 字段。需要在 `oxide-arb-models/src/domain/execution.rs` 中增加：

```rust
pub struct ExecutionPlan {
    // ... existing ...
    pub category: MarketCategory,  // NEW — needed for fee calculation
}
```

`PlanBuilder::build()` 填入 `opp.category`。

**`build.rs`** 构造 `Dispatcher` 和 `OrderStrategy` 时传入 `fee_calculator`。

---

## 3. Paper Mode Book 深度检查

### 现状

`Dispatcher::paper_trade()` 始终返回 `Filled`，不检查当前 book 深度。

### 方案

`Dispatcher` 增加 `book_store: Arc<BookStore>` 依赖（仅 Paper 模式使用，DryRun 不检查）。

#### 3a. `BookStore` 新增 depth walk 方法

**文件**: `oxide-arb-core/src/pipeline/book_store.rs`

```rust
impl BookStore {
    /// Walk the book for `token_id` on `side` up to `limit_price`,
    /// returning total available shares.
    pub fn available_depth_at_price(
        &self,
        token_id: &TokenId,
        side: Side,
        limit_price: Price,
    ) -> Shares {
        let Some(book_arc) = self.get_book(token_id) else {
            return Shares::ZERO;
        };
        let book = book_arc.load();
        match side {
            Side::Buy => book.ask_depth_up_to(limit_price),
            Side::Sell => book.bid_depth_down_to(limit_price),
        }
    }
}
```

对应需要在 `OrderBook` 上增加 `ask_depth_up_to(price)` 和 `bid_depth_down_to(price)` 方法 — 遍历 levels 累加 shares 直到超过 limit price。

#### 3b. `paper_trade` 增加深度检查

```rust
fn paper_trade(&self, plan: &ExecutionPlan) -> ExecutionOutcome {
    if let Some(book_store) = &self.book_store {
        let available = book_store.available_depth_at_price(
            &plan.token_id, plan.side, plan.limit_price,
        );
        if available < plan.shares {
            return ExecutionOutcome::Miss {
                reason: format!(
                    "paper: insufficient depth ({} < {} shares at {})",
                    available, plan.shares, plan.limit_price
                ),
                execution_mode: ExecutionMode::Paper,
            };
        }
    }
    // ... fee calc + Filled ...
}
```

`Dispatcher::new` 签名：`book_store: Option<Arc<BookStore>>` — DryRun 传 None，Paper 传 Some。

### 测试

`tests/execution_integration.rs`: 构造 thin book (5 shares) → Paper 下单 10 shares → 验证 Miss。

---

## 4. ExecutionFSM Emergency 自动恢复

### 现状

`enter_emergency()` 是单向的。`clear_emergency()` 存在但无调用方。一旦进入 emergency，系统永久停止。

### 方案

**Heartbeat 成功时自动恢复**（仅当 risk engine 也 allows trading）。

**文件**: `oxide-arb-core/src/execution/heartbeat.rs`

```rust
Ok(_) => {
    tracing::debug!("heartbeat OK");
    self.risk_engine.on_execution_event(ExecutionRiskEvent::HeartbeatSuccess);

    // NEW: auto-recovery
    if self.fsm.is_emergency() && self.risk_engine.allows_trading() {
        self.fsm.clear_emergency();
        tracing::info!("execution emergency auto-cleared: heartbeat OK + risk allows trading");
    }
}
```

**安全约束**: `risk_engine.allows_trading()` 检查 breaker state + manual halt。如果 emergency 是因为 risk persist 失败（engine halted），`allows_trading()` 返回 false → 不会自动恢复。只有当外部条件（网络恢复、operator ack）使 risk engine 回到正常状态后，下一次 heartbeat 成功才会清除 emergency。

### 测试

`tests/execution_emergency_recovery.rs`: 
1. 触发 emergency → 验证 `is_emergency() == true`
2. 模拟 heartbeat 成功 + `allows_trading() == true` → 验证 `is_emergency() == false`
3. 模拟 heartbeat 成功 + `allows_trading() == false` → 验证 emergency 不清除

---

## 5. 删除空文件 `execution/types.rs`

**文件**: `oxide-arb-core/src/execution/types.rs` — 仅 2 行注释，无任何类型定义。

**删除**:
- `crates/oxide-arb-core/src/execution/types.rs` 文件本身
- `crates/oxide-arb-core/src/execution/mod.rs` 中的 `pub mod types;` 声明
