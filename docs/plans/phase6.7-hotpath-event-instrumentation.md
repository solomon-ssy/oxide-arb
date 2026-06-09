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
5. **固定开启**：总线始终开启、容量固定 4096（`CORE_EVENT_CHANNEL_CAPACITY`）。无需配置开关——fire-and-forget + drop-on-full 已保证零热路径影响，再加 kill-switch 属过度设计。

---

## 1. 发布点（emit hooks）

> **落地状态**: 已实现。下表为最终设计（与计划讨论后的拍板项一致）。

| CoreEvent | 插桩位置 | 实现 |
|---|---|---|
| `OpportunityDetected(Opportunity)` | `detection/scanner.rs::scan_market` 检出后（与 `detection_writer.write` 同处） | 投影 `(*scored.opportunity).clone()`，不泄漏算法/latency trace |
| `TradeFilled(TradeInfo)` | `post_trade/consumer.rs::process` 终态推进成功（`advance_state == Ok(true)` 且 `state.is_success()`） | 仅实际推进的 worker 发出，至少一次重放不重复 |
| `TradeSettled { trade_id, outcome, pnl }` | `execution/settlement/service.rs::apply_risk_settlement` 记账成功后 | `outcome = Success`（成交业务结果），`pnl = realized_pnl`（符号自带） |
| `PnlUpdate { daily, total }` | `oxide-arb-risk` 引擎 `persist_post_trade_followups` 末尾单点（覆盖 Fill + Settlement） | `daily` = 当日已实现，`total` = **终身累计已实现**（持久化于 `risk_engine_state.total_realized_pnl`） |
| `PositionChanged(PositionInfo)` | 建仓：`consumer.rs::ensure_position` 新建后；结算：`service.rs::apply_risk_settlement` | 建仓仅新建（非幂等重放）时发 |
| `CircuitBreakerTripped { level, reason }` | risk 引擎 `persist_post_trade_followups`（已于 6.6 接入） | 不变 |
| `MarketResolved { market_id, outcome }` | `settlement/service.rs::settle_market`（已于 6.6 接入） | 不变 |

> `SystemStatusChanged` / `Alert` / `ControlPublished` / `ConfigActivated` 已在 6.6 接入（非热路径）。

### 1.1 破坏式变更（删除）

- **删除 `TradeOpened`**：endgame 为单笔 FOK（无驻留挂单），执行管线只持 `NewTrade` 无真实 `TradeInfo`；"下单"与"成交"几乎瞬时。`CoreEvent` / `WsChannel`（`trade.opened`）/ mapping 全部移除。
- **删除 `OpportunityExpired`**：无真实领域过期源（funnel 仅在背压下淘汰低分项，语义是"丢弃"非"过期"，且高频）。`CoreEvent` / `WsChannel`（`opportunity.expired`）/ mapping 全部移除。

### 1.2 `PnlUpdate.total` 语义（终身已实现 PnL）

`daily` 与 `total` 同一记账口径：`daily` 是当日切片（跨日 rollover 重置），`total` 是自系统启动以来的累计已实现 PnL（永不重置）。终身值由 **风控引擎**唯一拥有：在 `apply_settlement` 与 `daily.record_trade` 同处累加 `net_profit`，写入 `risk_engine_state.total_realized_pnl` 持久化，启动时由 `state_store::recover_state` 经 builder 恢复 → 重启安全。该字段**纯遥测，绝不被任何 pre-trade gate 读取**。同时投影到 `LivePnlView.total_realized_pnl`，使 WS `sync` 快照与 `pnl.update` 推送一致。

---

## 2. 发布句柄注入

- `AppContext.event_publisher()` 返回 `CoreEventPublisher`（轻封装 `flume::Sender<CoreEvent>` + 全局 drop 计数 + per-kind drop hook）。
- `AppContext::build` 用 `CoreEventPublisher::bounded(CORE_EVENT_CHANNEL_CAPACITY)`（固定 4096）构造；`connect_infra` 返回后 `with_drop_hook(...)` 注入闭包，捕获 `MetricsHub::register_ws_event_dropped()` 句柄，按 `event.kind()` 打标签 `inc()`。
- scanner / post-trade consumer / 风控引擎 / 结算服务各持 `CoreEventPublisher`（`Clone`），在上述点 `publisher.publish(CoreEvent::...)`。

```rust
pub struct CoreEventPublisher {
    tx: flume::Sender<CoreEvent>,
    on_drop: Option<DropObserver>,        // Arc<dyn Fn(&'static str) + Send + Sync>
    dropped: Arc<AtomicU64>,
}
impl CoreEventPublisher {
    /// Non-blocking publish. Drops + counts (+ per-kind hook) on a
    /// full/disconnected channel; never blocks the caller.
    pub fn publish(&self, event: CoreEvent) {
        let kind = event.kind();
        if self.tx.try_send(event).is_err() {
            self.dropped.fetch_add(1, Relaxed);
            if let Some(observer) = &self.on_drop { observer(kind); }
        }
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
5. 总线固定开启、容量固定 4096（无配置开关）。

## 6. Phase 6 整体完成判定

6.1–6.7 全绿后，对照父计划 §19 验收清单逐项确认：JWT 登录 / fail-closed authz / super_admin 旁路 / 双轨审计（哈希链 verify + operation_log）/ runtime-config 版本化 / scheduler enqueue-only + execute worker / WS 鉴权 + fanout + 心跳 / 静态 SPA / 统一 envelope + request-id tracing / 全破坏式变更落地。
