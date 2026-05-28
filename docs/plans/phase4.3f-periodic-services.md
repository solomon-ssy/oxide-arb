# Phase 4.3f — Periodic Services Spawn

> **状态**: 待实施
>
> **前置**: 4.3c (GammaService 在 AppContext 中可用), 4.3e (spawn_outcome_drain 扩展完成)
>
> **影响 crate**: `oxide-arb-core`

---

## 问题总览

`TaskId` 枚举定义了 25 种任务类型，但 `bootstrap.rs` 只 spawn 了 8 种核心交易循环任务。以下周期性服务任务已有实现代码但从未被注册到 `TaskRegistry`：

| TaskId | 实现状态 | 当前 spawn 状态 |
|--------|---------|----------------|
| `GammaSync` | `GammaService::sync()` 完整 | 未 spawn（4.3c 已加入 build 阶段首次 sync） |
| `RiskTick` | `RiskEngine::tick()` 完整 | 未 spawn |
| `ExposureGc` | `InMemoryExposureReservation::gc_expired()` 完整 | 未 spawn |
| `WalletBalanceRefresh` | `WalletBalanceService::get_snapshot()` 完整 | 未 spawn |
| `CalibrationUpdater` | `CalibrationUpdater::update()` 完整 | 未 spawn |
| `LedgerReconcile` | `LedgerReconciler::reconcile()` 完整 | 未 spawn |

---

## 方案：`queue_periodic_services()` 统一入口

**文件**: `oxide-arb-core/src/app/mod.rs`

```rust
impl AppContext {
    /// Register all periodic background services into the pending task queue.
    /// Called after `queue_runtime_tasks()` in bootstrap.
    pub fn queue_periodic_services(&self) {
        self.queue_gamma_sync();
        self.queue_risk_tick();
        self.queue_exposure_gc();
        self.queue_calibration_updater();
        self.queue_wallet_balance_refresh();
        self.queue_ledger_reconcile();
    }
}
```

**文件**: `oxide-arb-core/src/app/bootstrap.rs`

```rust
pub async fn run(config_dir: &str) -> OxideResult<()> {
    // ... existing ...
    ctx.queue_runtime_tasks();
    ctx.queue_risk_decision_audit_drain(/* ... */);
    ctx.queue_periodic_services();  // NEW
    // ...
}
```

---

## 1. GammaSync 周期任务

**间隔**: `config.market_data.gamma.full_sync_interval_secs`（默认 300s）

```rust
fn queue_gamma_sync(&self) {
    let gamma_service = Arc::clone(&self.data.gamma_service);
    let interval_secs = self.config.market_data.gamma.full_sync_interval_secs.max(60);

    self.pending_tasks.push(TaskId::GammaSync, move |shutdown| async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // skip first tick (startup sync already done)

        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    if let Err(e) = gamma_service.sync().await {
                        tracing::error!(%e, "gamma periodic sync failed");
                        // non-fatal: next tick will retry
                    }
                }
            }
        }
    });
}
```

---

## 2. Risk Tick 周期任务

**间隔**: 5s（硬编码，tick 是轻量操作）

**作用**:
- CircuitBreaker FSM 时间转换（Open→HalfOpen cooldown 到期、Recovered→Closed 观察期结束）
- DailyAccounting / WeeklyAccounting / HourlyAccounting UTC 翻转
- BlacklistManager TTL GC
- 状态变更后持久化 risk_engine_state 到 PG

```rust
fn queue_risk_tick(&self) {
    let risk_engine = Arc::clone(&self.risk.engine);
    let risk_metrics = Arc::clone(&self.risk.metrics);

    self.pending_tasks.push(TaskId::RiskTick, move |shutdown| async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(5));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    if let Err(e) = risk_engine.tick(risk_metrics.as_ref()).await {
                        tracing::error!(%e, "risk tick failed — engine may halt");
                    }
                }
            }
        }
    });
}
```

---

## 3. Exposure GC 周期任务

**间隔**: 30s

**作用**: 清理 TTL 过期的 exposure reservations。不清理 → reserved capital 永久锁定 → 可用资金持续减少。

```rust
fn queue_exposure_gc(&self) {
    let exposure = Arc::clone(&self.risk.exposure);
    let metrics = Arc::clone(&self.infra.metrics);

    self.pending_tasks.push(TaskId::ExposureGc, move |shutdown| async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    let expired = exposure.gc_expired();
                    if expired > 0 {
                        tracing::info!(expired, "exposure GC cleaned expired reservations");
                        metrics.exposure_gc_cleaned.inc_by(u64::from(expired));
                    }
                }
            }
        }
    });
}
```

**MetricsHub 改动**: 增加 `exposure_gc_cleaned: IntCounter` 指标。

---

## 4. WalletBalance 刷新周期任务

**间隔**: 15s

**条件**: 仅当 `ClobClient` 可用时 spawn（DryRun/Paper 无 ClobClient 时跳过）

**作用**: `CoreRiskMetrics::cached_balance()` 返回的值需要定期从 CLOB API 刷新。不刷新 → risk engine 用过期余额做 exposure 计算 → 可能批准超出实际余额的交易。

### 方案 A: 有 ClobClient 时

```rust
fn queue_wallet_balance_refresh(&self) {
    let Some(clob_client) = &self.trading.clob_client else {
        tracing::info!("wallet balance refresh skipped — no ClobClient");
        return;
    };

    let wallet_service = Arc::new(WalletBalanceService::new(
        Arc::clone(&self.infra.cache),
        Arc::clone(clob_client),
        Arc::clone(&self.risk.exposure) as Arc<dyn ExposureReservationBackend>,
    ));
    let risk_metrics_state = Arc::clone(&self.risk.metrics_state);

    self.pending_tasks.push(TaskId::WalletBalanceRefresh, move |shutdown| async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(15));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    match wallet_service.get_snapshot().await {
                        Ok(snap) => {
                            risk_metrics_state.update_balance(snap.raw_balance);
                            tracing::debug!(balance = %snap.raw_balance, "wallet balance refreshed");
                        }
                        Err(e) => {
                            tracing::warn!(%e, "wallet balance refresh failed");
                        }
                    }
                }
            }
        }
    });
}
```

**需要**: `RiskMetricsState` 增加 `update_balance(balance: Usd)` 方法更新 cached balance。

### 方案 B: 无 ClobClient 时

在 `wire_risk()` 中初始化 `risk_metrics_state` 的 `cached_balance` 为 `config.bankroll_usd`：

```rust
risk_metrics_state.update_balance(Usd::new(settings.risk.bankroll_usd));
```

---

## 5. Calibration Refresh 周期任务

**间隔**: `config.detection.calibration.refresh_interval_secs`（默认 3600s）

**作用**: 从 DB 加载新的 resolution outcomes，更新 `ResolutionCalibrator` 的 bucket posterior。不刷新 → 概率估计基于启动时的数据 → 随时间偏移。

```rust
fn queue_calibration_updater(&self) {
    let updater = Arc::clone(&self.trading.calibration_updater);
    let interval_secs = self.config.detection.calibration.refresh_interval_secs.max(300);

    self.pending_tasks.push(TaskId::CalibrationUpdater, move |shutdown| async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // skip first (calibrator initialized at build time)

        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    if let Err(e) = updater.update().await {
                        tracing::error!(%e, "calibration refresh failed");
                    }
                }
            }
        }
    });
}
```

---

## 6. Ledger Reconciliation 周期任务

**间隔**: 300s（5 分钟）

**作用**: 比较内部 risk engine state vs CLOB API 余额/持仓，发现 drift → L4 breaker trip。不执行 → 内部账本和真实状态静默偏离。

**条件**: 仅当 `ClobClient` 可用时 spawn（需要真实余额查询）。

```rust
fn queue_ledger_reconcile(&self) {
    let Some(clob_client) = &self.trading.clob_client else {
        tracing::info!("ledger reconciliation skipped — no ClobClient");
        return;
    };

    let risk_engine = Arc::clone(&self.risk.engine);
    let risk_metrics = Arc::clone(&self.risk.metrics);
    let balance_querier = Arc::new(CoreBalanceQuerier::new(
        Arc::clone(clob_client),
        /* position_repo */ // needs wiring
    ));

    self.pending_tasks.push(TaskId::LedgerReconcile, move |shutdown| async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(300));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // skip first

        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = ticker.tick() => {
                    let reconciler = risk_engine.reconciler();
                    match reconciler.reconcile(risk_metrics.as_ref(), balance_querier.as_ref()).await {
                        Ok(report) => {
                            if let Err(e) = risk_engine.on_reconciliation_result(&report, risk_metrics.as_ref()).await {
                                tracing::error!(%e, "reconciliation result processing failed");
                            }
                        }
                        Err(e) => {
                            tracing::error!(%e, "reconciliation failed");
                        }
                    }
                }
            }
        }
    });
}
```

**依赖**: 需要 `CoreBalanceQuerier` 实现 — `oxide-arb-core/src/bridge/balance_querier.rs` 已有实现但未在 `build.rs` 中构造。需要在 `wire_risk` 中构造并暴露。

---

## 7. Fee 缓存刷新

### 现状

`service/fee_params.rs` 的 `FeeParamsService` 实现了 read-through cache，但从未被周期性刷新。`FeeCalculator` 内部已有 `ingest_gamma_markets()` 在 Gamma sync 时更新，所以 `FeeParamsService` 的 read-through 缓存主要服务于按 category 查询的场景。

### 方案

**不需要独立的周期任务**。Fee 参数通过两条路径刷新：

1. **Gamma sync 时** (`GammaService::sync()`): `fee_calculator.ingest_gamma_markets(&fee_data)` — 已在 4.3c 中接入
2. **Cache invalidation**: `invalidate_post_gamma_sync()` 在 Gamma sync 后清除 TieredCache 的 fee 相关 key — 已在 `GammaService::sync_inner()` 中调用

**确认无需额外 spawn。** `FeeParamsService` 的 cache miss 会自动从 `FeeCalculator` 重新计算，而 `FeeCalculator` 的数据由 GammaSync 周期更新。链路完整。

---

## AppContext 需要的新 Bundle 字段

为了支持上述所有 periodic services，`AppContext` 的 bundles 需要补充：

| Bundle | 新增字段 | 用途 |
|--------|---------|------|
| `DataBundle` | `gamma_service: Arc<GammaService>` | GammaSync (from 4.3c) |
| `RiskBundle` | `metrics_state: Arc<RiskMetricsState>` | 已有 |
| `TradingBundle` | `calibration_updater: Arc<CalibrationUpdater>` | 已有 |

**无需新增 Bundle** — 所有依赖已存在或可从现有字段获取。

---

## 测试

| 任务 | 测试策略 |
|------|---------|
| GammaSync periodic | `tests/runtime_e2e.rs` — build 后 registry 非空（startup sync），等 tick 后 count 不变或增加 |
| RiskTick | `tests/engine_tests.rs` 已有 tick 测试；集成测试验证 breaker 从 Open 经 tick 转 HalfOpen |
| ExposureGc | `tests/exposure_reservation_concurrent.rs` — reserve + 设短 TTL + wait + gc → verify released |
| WalletBalance | mock ClobClient 返回余额 → verify `risk_metrics.cached_balance()` 更新 |
| CalibrationUpdater | mock CalibrationDataSource → verify calibrator buckets updated |
| LedgerReconcile | mock BalanceQuerier → verify reconciliation report status |
