# Phase 4.3c — GammaService Startup Sync + Periodic Spawn

> **状态**: 待实施
>
> **前置**: 无（可独立执行，但逻辑上应在 D/E/F/G 之前完成）
>
> **影响 crate**: `oxide-arb-core`
>
> **关键性**: **P0** — 没有 Gamma sync，`MarketRegistry` 启动时为空，Coalescer 无法将 token update 路由到 market，整个检测→执行链路完全断裂

---

## 问题剖析

### 当前数据流（断裂）

```
ClobWsManager → BookSnapshot(token_id)
  → DataPipeline → BookStore.apply_snapshot(token_id)
  → Coalescer.notify_token_update(token_id)
    → market_registry.market_for_token(token_id) → None ← DEAD END
```

`MarketRegistry::new()` 构造一个空的注册表。`GammaService` 已实现但未在 `build()` 或 `bootstrap` 中被构造或调用。

### 目标数据流（修复后）

```
AppContext::build()
  → GammaService::sync().await  (blocking, fail-closed)
  → MarketRegistry 填充 ~500 个 active markets
  → MarketCache::rebuild()
  → token intern pool prewarmed

Runtime:
  → Coalescer.notify_token_update(token_id)
    → market_registry.market_for_token(token_id) → Some(market_id) ← WORKS
```

---

## 方案

### Step 1: `build.rs` 中构造 GammaService 并执行首次同步

**文件**: `oxide-arb-core/src/app/build.rs`

在 `BuildInfra.repos` 中增加 `PgMarketRepository` 和 `PgEventRepository`（它们已在 `oxide-arb-repository` 中实现）：

```rust
struct BuildRepos {
    // ... existing repos ...
    market: Arc<PgMarketRepository>,  // NEW
    event: Arc<PgEventRepository>,    // NEW
}
```

`connect_infra()` 中构造：

```rust
let repos = BuildRepos {
    // ... existing ...
    market: Arc::new(PgMarketRepository::new(db.clone())),
    event: Arc::new(PgEventRepository::new(db.clone())),
};
```

### Step 2: `wire_detection()` 改为 `async` 并执行首次同步

**文件**: `oxide-arb-core/src/app/build.rs`

`wire_detection()` 签名改为 `async fn`（目前是 sync）。在末尾构造 GammaService 并同步：

```rust
async fn wire_detection(
    settings: &Settings,
    infra: &BuildInfra,
    clients: &BuildClients,
    shutdown: CancellationToken,
) -> OxideResult<DetectionStack> {
    // ... existing book_store, market_registry, calibrator, etc. ...

    // NEW: Gamma startup sync (fail-closed)
    let gamma_service = GammaService::new(GammaServiceDeps {
        gamma_client: clients.gamma_client.clone(),
        market_registry: Arc::clone(&market_registry),
        market_cache: Arc::clone(&market_cache),
        fee_calculator: clients.fee_calculator.clone(),
        market_repo: infra.repos.market.clone(),
        event_repo: infra.repos.event.clone(),
        cache: infra.cache.clone(),
        metrics: infra.metrics.clone(),
    });

    gamma_service.sync().await.map_err(|e| {
        tracing::error!(%e, "Gamma startup sync failed — cannot start without market catalog");
        e
    })?;

    tracing::info!(
        markets = market_registry.market_count(),
        "Gamma startup sync complete"
    );

    Ok(DetectionStack {
        // ... existing fields ...
        gamma_service: Arc::new(gamma_service),  // NEW: preserve for periodic spawn
    })
}
```

**`DetectionStack`** struct 增加 `gamma_service: Arc<GammaService>` 字段。

**`wire_trading()`** 签名也需要改为 `async` 以适配 `wire_detection` 的变化。

### Step 3: `AppContext` 持有 GammaService 引用

**文件**: `oxide-arb-core/src/app/mod.rs`

`DataBundle` 增加字段：

```rust
pub struct DataBundle {
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub data_pipeline: Arc<DataPipeline>,
    pub gamma_service: Arc<GammaService>,  // NEW
}
```

`BuildTrading` → `AppContext` 赋值链同步更新。

### Step 4: 注册周期任务（在 phase4.3f 的 `queue_periodic_services` 中）

周期 spawn 逻辑放在 phase4.3f 统一处理。此处只需确保 `AppContext` 能访问 `GammaService`。

预览注册逻辑：

```rust
fn queue_gamma_sync(&self) {
    let gamma_service = Arc::clone(&self.data.gamma_service);
    let interval = Duration::from_secs(
        self.config.market_data.gamma.full_sync_interval_secs.max(60)
    );

    self.pending_tasks.push(TaskId::GammaSync, move |shutdown| async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // skip first (already synced at startup)

        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    if let Err(e) = gamma_service.sync().await {
                        tracing::error!(%e, "gamma periodic sync failed");
                    }
                }
            }
        }
    });
}
```

---

## 测试

1. **Unit**: `service/gamma.rs` 已有 1 个 test — 增加 `sync_populates_registry` 验证 sync 后 `market_count() > 0`（mock GammaClient）
2. **Integration**: `tests/runtime_e2e.rs` 验证 `AppContext::build()` 后 `data.market_registry.market_count() > 0`
3. **Negative**: 验证 `GammaClient` 返回错误时 `build()` 返回 `Err`（fail-closed）
