# Phase 6.7 — 热路径事件插桩（WS 实时推送闭环）

> **状态**: Production Design Target
> **父计划**: `docs/plans/phase6-web-layer.md`
> **前置依赖**: Phase 6.6（`CoreEvent` 总线 + `WsBroadcaster` + 非热路径事件已就绪）
> **覆盖父计划章节**: §13.3（服务端推送类型，热路径部分）
> **目标**: 在交易热路径（scanner / execution / post-trade / risk）安全插桩 `CoreEvent` 发布点，打通 opportunity/trade/pnl/position 实时推送闭环。**这是 money 系统最敏感的改动，独立子 phase、重点 review、零热路径阻塞容忍。**

---

## 0. 设计原则（硬约束）

1. **永不阻塞热路径**：所有发布点用 `flume::Sender::try_send`（非阻塞）；channel 满即丢弃 + 计数告警，**绝不** `await`/阻塞交易决策或执行。
2. **零分配/低开销优先**：发布点构造事件应轻量；大对象用 `Arc` 共享（与既有热路径 `Arc<ScoredOpportunity>` 一致）。
3. **失败隔离**：WS/broadcaster 故障绝不回传影响交易；发布是 fire-and-forget。
4. **可观测**：丢弃计数进 metrics（`ws_event_dropped_total{kind}`）。
5. **可禁用**：事件发布可经配置开关（默认开），便于排障时隔离。

---

## 1. 发布点（emit hooks）

| CoreEvent | 插桩位置 | 现状参考 |
|---|---|---|
| `OpportunityDetected(Opportunity)` | `detection/scanner.rs` 检出 `ScoredOpportunity` 后（与 `detection_writer.write` 同处） | scanner.rs:103-109 |
| `OpportunityExpired(OpportunityId)` | funnel/coalescer 过期路径 | funnel/coalescer |
| `TradeOpened(TradeInfo)` | `ExecutionPipeline` 下单/建仓后 | execution_pipeline |
| `TradeFilled(TradeInfo)` | 成交回执处理 | post-trade / execution |
| `TradeSettled { trade_id, outcome, pnl }` | `PostTradeConsumer::process` 结算 | post_trade |
| `PnlUpdate { daily, total }` | risk engine tick / post-trade PnL 更新 | run_risk_tick / post_trade |
| `PositionChanged(PositionInfo)` | 持仓更新（post-trade / position store） | post_trade |
| `CircuitBreakerTripped { level, reason }` | risk audit sink `BreakerTripped` 或 FSM `enter_emergency` | risk audit / ExecutionFSM |
| `MarketResolved { market_id, outcome }` | `DataPipeline` market resolved（已有 AlertDispatcher 调用处） | data_pipeline |

> `SystemStatusChanged` / `Alert` / `ControlPublished` / `ConfigActivated` 已在 6.6 接入（非热路径）。

---

## 2. 发布句柄注入

- `AppContext.event_publisher()` 返回 `CoreEventPublisher`（轻封装 `flume::Sender<CoreEvent>` + drop 计数 metrics）。
- 在 `wire_risk_and_trading` / `wire_detection` 构造 scanner/execution/post-trade 时注入 `Option<CoreEventPublisher>`（None = 禁用）。
- 各组件持 `CoreEventPublisher`，在上述点 `publisher.emit(CoreEvent::...)`（内部 `try_send` + drop 计数）。

```rust
pub struct CoreEventPublisher { tx: Sender<CoreEvent>, dropped: Arc<DropCounter> }
impl CoreEventPublisher {
    /// Non-blocking emit. Drops + counts on full channel; never blocks the caller.
    pub fn emit(&self, event: CoreEvent) {
        if self.tx.try_send(event).is_err() { self.dropped.inc(/* kind */); }
    }
}
```

---

## 3. 数据形态对齐

- `Opportunity` / `TradeInfo` / `PositionInfo` / `SystemStatus` 均 `Serialize`（已确认），WS 直接序列化。
- 热路径若只持有 `Arc<ScoredOpportunity>`，转换为 WS 用 `Opportunity` 投影（避免泄漏内部 trace/算法细节）；投影函数轻量。
- `TradeSettled.outcome: TradeBusinessOutcome`（`Success`/`Miss`/`Failed`），`pnl: Usd`。

---

## 4. 测试策略

| 测试 | 场景 |
|---|---|
| 非阻塞 | channel 填满后 emit 立即返回、不阻塞；drop 计数递增 |
| 端到端推送 | 模拟检出 → WS 订阅者收到 `opportunity.detected`；模拟成交/结算 → `trade.filled`/`trade.settled`；PnL 更新 → `pnl.update` |
| 投影正确 | `Opportunity` 投影字段正确、不含内部算法 trace |
| 故障隔离 | broadcaster panic/停止不影响交易路径继续运行 |
| 背压 metrics | `ws_event_dropped_total` 正确计数 |
| 订阅过滤 | market.book_update 仅推给订阅该 market 的 session |

---

## 5. 退出条件

1. 全部热路径事件正确发布并推送到订阅者，闭环打通。
2. 压测下热路径无可测量的额外延迟；channel 满时丢弃而非阻塞。
3. broadcaster 故障与交易路径完全隔离。
4. drop 计数进 metrics 可观测。
5. 事件发布可经配置禁用。

## 6. Phase 6 整体完成判定

6.1–6.7 全绿后，对照父计划 §19 验收清单逐项确认：JWT 登录 / fail-closed authz / super_admin 旁路 / 双轨审计（哈希链 verify + operation_log）/ runtime-config 版本化 / scheduler enqueue-only + execute worker / WS 鉴权 + fanout + 心跳 / 静态 SPA / 统一 envelope + request-id tracing / 全破坏式变更落地。
