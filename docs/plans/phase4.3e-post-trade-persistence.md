# Phase 4.3e — Post-Trade Persistence & Alerting

> **状态**: 待实施
>
> **前置**: 4.3a (PostTradeInput 扩展), 4.3d (Fee 统一计算)
>
> **影响 crate**: `oxide-arb-core`

---

## 问题总览

`process_post_trade_job()` 当前只做 risk accounting。以下三条持久化链路完全缺失：

1. **Trade DB Write** — `NewTrade` 未写入 Postgres
2. **Lifecycle Events** — `NewLifecycleEvent` 未写入 Postgres（`AsyncWriter<NewLifecycleEvent>` 未构造/spawn）
3. **ClickHouse Opportunity Audit** — `OpportunityAuditRow` 未写入 ClickHouse（`AsyncWriter<OpportunityAuditRow>` 未构造/spawn）

另外 `AlertDispatcher` 未接入 breaker trip 通知路径。

---

## 1. Trade Persistence (DB Write)

### TradeId 策略

**决策**: `TradeId = UUID v7` 独立生成，通过 `opportunity_id` 字段关联回 opportunity。

### PostTradeJob 扩展

**文件**: `oxide-arb-core/src/execution/execution_pipeline.rs`

`PostTradeJob` 需要携带构造 `NewTrade` 所需的全部字段：

```rust
pub struct PostTradeJob {
    pub trade_id: TradeId,           // NEW: UUID v7
    pub execution_id: ExecutionId,   // NEW
    pub opportunity_id: OpportunityId, // NEW (was: trade_id 复用 opp_id)
    pub market_id: MarketId,
    pub event_id: EventId,           // NEW
    pub token_id: TokenId,
    pub side: Side,                  // NEW
    pub entry_price: Price,
    pub filled_shares: Shares,       // NEW (was: only net_profit)
    pub net_profit: Usd,
    pub execution_mode: ExecutionMode, // NEW
    pub edge_bps: Option<Bps>,        // NEW
    pub detected_profit: Option<Usd>,  // NEW
    pub outcome: ExecutionOutcome,
}
```

**`enqueue_post_trade`** 签名扩展，从 `opp` + `plan` 中提取所有字段：

```rust
fn enqueue_post_trade(&self, opp: &Opportunity, plan: &ExecutionPlan, outcome: ExecutionOutcome) {
    let (filled_shares, net_profit, entry_price) = match &outcome {
        ExecutionOutcome::Filled { filled_shares, avg_fill_price, .. } => {
            let price = avg_fill_price.unwrap_or(opp.entry_price);
            (*filled_shares, filled_net_profit(opp, *filled_shares, plan.shares), price)
        }
        _ => (Shares::ZERO, opp.net_profit, opp.entry_price),
    };

    let job = PostTradeJob {
        trade_id: TradeId::new_v7(),
        execution_id: plan.execution_id.clone(),
        opportunity_id: opp.opportunity_id.clone(),
        market_id: opp.market_id.clone(),
        event_id: opp.event_id.clone(),
        token_id: opp.token_id.clone(),
        side: opp.side,
        entry_price,
        filled_shares,
        net_profit,
        execution_mode: self.execution_mode,
        edge_bps: Some(opp.edge_bps),
        detected_profit: Some(opp.expected_net_profit),
        outcome,
    };
    // ... send to channel ...
}
```

### `spawn_outcome_drain` 增加 `trade_repo`

```rust
pub async fn spawn_outcome_drain(
    rx: flume::Receiver<PostTradeJob>,
    risk_engine: Arc<RiskEngine>,
    risk_metrics: Arc<CoreRiskMetrics>,
    fsm: Arc<ExecutionFSM>,
    trade_repo: Arc<dyn TradeRepository>,  // NEW
    alerts: Arc<AlertDispatcher>,           // NEW
    post_trade_spill: SharedInMemoryEventStore,
    shutdown: CancellationToken,
) -> Result<(), OxideError> { /* ... */ }
```

### `process_post_trade_job` 增加 trade write

```rust
async fn process_post_trade_job(
    risk_engine: &RiskEngine,
    metrics: &CoreRiskMetrics,
    fsm: &ExecutionFSM,
    trade_repo: &dyn TradeRepository,  // NEW
    alerts: &AlertDispatcher,           // NEW
    job: PostTradeJob,
) {
    // 1. Trade DB write (best-effort — failure doesn't halt risk accounting)
    let new_trade = build_new_trade(&job);
    if let Err(e) = trade_repo.create(new_trade).await {
        tracing::error!(error = %e, trade_id = %job.trade_id, "trade persistence failed");
    }

    // 2. Risk accounting (existing, fail-closed)
    // ... Fill phase ...
    // ... Settlement phase ...

    // 3. Alert on breaker trip (NEW)
    // (see §4 below)
}

fn build_new_trade(job: &PostTradeJob) -> NewTrade {
    let (outcome, price, cost, fee, order_id, tx_hash, latency_ms) = match &job.outcome {
        ExecutionOutcome::Filled {
            order_id, filled_shares, avg_fill_price, fee_paid, tx_hash, latency_ms, ..
        } => {
            let price = avg_fill_price.unwrap_or(job.entry_price);
            (TradeOutcome::Success, price, filled_cost(*filled_shares, price), *fee_paid,
             Some(order_id.clone()), tx_hash.clone(), Some(*latency_ms as i32))
        }
        ExecutionOutcome::Miss { .. } => {
            (TradeOutcome::Miss, job.entry_price, Usd::ZERO, Usd::ZERO, None, None, None)
        }
        ExecutionOutcome::Failed { error, .. } => {
            (TradeOutcome::TradeFailed, job.entry_price, Usd::ZERO, Usd::ZERO, None, None, None)
        }
    };

    NewTrade {
        execution_id: job.execution_id.clone(),
        opportunity_id: job.opportunity_id.clone(),
        market_id: job.market_id.clone(),
        event_id: job.event_id.clone(),
        token_id: job.token_id.clone(),
        side: job.side,
        shares: job.filled_shares,
        price,
        cost_usd: cost,
        fee_usd: fee,
        detected_edge_bps: job.edge_bps,
        detected_profit_usd: job.detected_profit,
        execution_mode: job.execution_mode,
    }
}
```

---

## 2. Lifecycle Events 持久化

### 现状

`AsyncWriter<NewLifecycleEvent>` 在 Phase 4.2 plan 中设计但 **从未在 build.rs 中构造或 spawn**。`LifecycleRepository` trait 已实现（`PgLifecycleRepository`），`NewLifecycleEvent` DTO 已定义。

### 方案

**Phase 4.3e 范围内暂不实现完整的 lifecycle event pipeline。** 理由：

- Lifecycle events 需要在 execution pipeline 的每个阶段（detected, validated, dispatched, filled, missed, failed）emit — 这要求深度改造 `ExecutionPipeline::execute()` 的每个分支
- 当前 `process_post_trade_job` 是最小范围的 post-trade 入口，从这里 emit 只能覆盖 Filled/Miss/Failed 三个终态
- 完整的 lifecycle audit trail 应作为 phase4.3g (Position Settlement) 的一部分，当 position lifecycle 完整闭环时一起实现

**最小方案**: 在 `process_post_trade_job` 的 trade write 成功后，写一条 lifecycle event 记录终态：

```rust
let lifecycle = NewLifecycleEvent {
    event_type: match &job.outcome {
        ExecutionOutcome::Filled { .. } => LifecycleEventType::TradeFilled,
        ExecutionOutcome::Miss { .. } => LifecycleEventType::TradeMissed,
        ExecutionOutcome::Failed { .. } => LifecycleEventType::TradeFailed,
    },
    trade_id: Some(job.trade_id.clone()),
    market_id: job.market_id.clone(),
    // ...
};
if let Err(e) = lifecycle_repo.create(lifecycle).await {
    tracing::warn!(%e, "lifecycle event write failed");
}
```

**`spawn_outcome_drain`** 签名增加 `lifecycle_repo: Arc<dyn LifecycleRepository>`。

---

## 3. ClickHouse Opportunity Audit 写入

### 现状

`OpportunityAuditRow` 类型已定义（`oxide-arb-models/src/clickhouse/opportunity_audit.rs`），`ClickHousePool` 有 insert 能力，但 **从未有代码写入**。

### 方案

**Phase 4.3e 范围内暂不实现 ClickHouse 写入。** 理由：

- ClickHouse 写入是批量异步的（需要 `AsyncWriter` 构造 + worker spawn），是 "nice to have" 的分析数据
- `AsyncWriter<OpportunityAuditRow>` 的 flush function 需要 `ClickHousePool::insert` — 当前 `build.rs` 中 `ClickHousePool` 已连接但没有被传入 trading/execution 层
- 这属于 observability 增强，不影响交易正确性

**标记为 Phase 4.4 backlog**，在 periodic services 和 position settlement 完成后专项实施。

---

## 4. AlertDispatcher 接入 L3/L4 Breaker Trip

### 现状

`AlertDispatcher` 已实现 Telegram + Webhook（`observability/alert_dispatcher.rs`），但 breaker trip 后仅写 audit，不发告警。

### 方案

在 `process_post_trade_job` 中，当 settlement phase 返回 `breaker_tripped`，dispatch alert：

```rust
// After settlement phase
if trade_outcome == TradeOutcome::Success {
    match risk_engine.on_trade_result(Settlement, &settlement, metrics).await {
        Ok(report) => {
            if let Some(level) = report.breaker_tripped {
                fsm.enter_emergency("circuit breaker tripped after settlement");

                // NEW: dispatch alert for L3/L4
                if level >= CircuitBreakerLevel::Daily {
                    let alert = Alert {
                        severity: AlertSeverity::Emergency,
                        title: format!("Circuit Breaker Tripped — L{}", level_number(level)),
                        body: format!(
                            "Market: {}\nTrade: {}\nLevel: {:?}",
                            job.market_id, job.trade_id, level
                        ),
                        timestamp: Utc::now(),
                    };
                    alerts.dispatch(alert).await;
                }
            }
        }
        Err(e) => { /* existing error handling */ }
    }
}
```

### build.rs 改动

`queue_execution_outcome_drain` 中传入 `Arc::clone(&self.infra.alerts)`。

---

## 5. build.rs / bootstrap.rs 调用链更新

### `AppContext::queue_execution_outcome_drain`

**文件**: `oxide-arb-core/src/app/mod.rs`

增加 trade_repo, lifecycle_repo, alerts 传入：

```rust
pub fn queue_execution_outcome_drain(&self) {
    // ... existing rx take ...
    let trade_repo: Arc<dyn TradeRepository> = /* from infra or new construction */;
    let lifecycle_repo: Arc<dyn LifecycleRepository> = /* from infra */;
    let alerts = Arc::clone(&self.infra.alerts);

    self.pending_tasks.push(TaskId::ExecutionOutcomeDrain, move |shutdown| async move {
        if let Err(error) = ExecutionPipeline::spawn_outcome_drain(
            rx, risk_engine, risk_metrics, fsm,
            trade_repo, lifecycle_repo, alerts,  // NEW params
            post_trade_spill, shutdown,
        ).await {
            tracing::error!(%error, "execution outcome drain exited with error");
        }
    });
}
```

**需要**: `AppContext` 持有 `trade_repo` 和 `lifecycle_repo` 的 `Arc`。目前 `BuildRepos` 只有 risk-specific repos。

**方案**: `BuildInfra.repos` 增加 `trade: Arc<PgTradeRepository>` 和 `lifecycle: Arc<PgLifecycleRepository>`。或者在 `AppContext` 中新增 `PersistenceBundle`。

---

## 测试

| 场景 | 文件 | 验证 |
|------|------|------|
| Fill → trade written | `tests/execution_integration.rs` | `trade_repo.find_by_id(trade_id)` 返回 Some |
| Miss → trade written with outcome=Miss | 同上 | outcome 字段为 Miss，cost/fee 为 ZERO |
| TradeId 是 UUID v7 | 同上 | `trade_id != opportunity_id` |
| Breaker trip → alert dispatched | 同上 | mock AlertDispatcher 收到 Emergency alert |
| Lifecycle event written on fill | 同上 | `lifecycle_repo.get_recent(1)` 返回 TradeFilled event |
