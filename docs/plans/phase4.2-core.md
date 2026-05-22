# Phase 4.2 — `oxide-arb-core` 详细设计

> **状态**: 待审核
>
> **日期**: 2025-06-22
>
> **依赖**: Phase 0–3（已完成）、Phase 4.1 `oxide-arb-risk`（并行开发）
>
> **约束**: ADR-001 — 单策略（Endgame）、单平台（Polymarket）、无对冲

---

## 目录

- [0. 工作范围](#0-工作范围)
- [1. Crate 架构](#1-crate-架构)
- [2. AppContext DI 设计](#2-appcontext-di-设计)
- [3. 数据管线 (Data Pipeline)](#3-数据管线-data-pipeline)
- [4. 检测触发 (Detection)](#4-检测触发-detection)
- [5. 执行管线 (Execution)](#5-执行管线-execution)
- [6. 执行状态机 (ExecutionFSM)](#6-执行状态机-executionfsm)
- [7. FOK + GTD 分层执行](#7-fok--gtd-分层执行)
- [8. 基础设施 (Infrastructure)](#8-基础设施-infrastructure)
- [9. 可观测性 (Observability)](#9-可观测性-observability)
- [10. Cache Owner Services](#10-cache-owner-services)
- [11. API 层补充](#11-api-层补充)
- [12. Outbox EventStore + Flusher](#12-outbox-eventstore--flusher)
- [13. Exposure Reservation InMemory 实现](#13-exposure-reservation-inmemory-实现)
- [14. RiskMetrics + RiskPersistence 桥接](#14-riskmetrics--riskpersistence-桥接)
- [15. CalibrationDataSource 桥接](#15-calibrationdatasource-桥接)
- [16. 测试策略](#16-测试策略)
- [17. 验收检查清单](#17-验收检查清单)
- [18. 预估工作量](#18-预估工作量)

---

## 0. 工作范围

### 0.1 交付物

| 交付物 | 说明 |
|--------|------|
| `oxide-arb-core` crate | 系统中枢：AppContext / DataPipeline / ExecutionPipeline / 基础设施 |
| `oxide-arb` binary crate | CLI 入口 — `serve`, `migrate`, `seed`, `status` 子命令 |
| ClobClient 扩展 | `collateral_balance()` 方法 |
| WS 增强 | last-message-age 跟踪 |
| InMemory ExposureReservationBackend | `DashMap + AtomicU64` CAS 实现 |
| CoreRiskMetrics / CoreRiskPersistence | 桥接 oxide-arb-risk DI trait |
| CoreCalibrationDataSource | 桥接 oxide-arb-algorithm CalibrationDataSource trait |
| Outbox EventStore + Flusher | 可靠生命周期事件投递 |
| Cache Owner Services | FeeParams / PositionSummary / WalletBalance 读穿缓存 |
| MetricsHub + AlertDispatcher | Prometheus 指标 + Telegram/Webhook 告警 |
| 集成测试套件 | 覆盖全管线端到端路径 |

### 0.2 上游依赖

```
oxide-arb-core 依赖图:
  oxide-arb-error
  oxide-arb-models       (config, domain, entities, enums, types)
  oxide-arb-api          (ClobClient, ClobWsManager, GammaClient, FeeCalculator, VotingOracle, Keystore)
  oxide-arb-storage      (PostgresPool, ClickHousePool, TieredCache, CacheKey)
  oxide-arb-repository   (12 traits + Pg*/Cached*/Ch* 实现)
  oxide-arb-algorithm    (OpportunityPipeline, CalibrationUpdater, CalibrationDataSource trait)
  oxide-arb-risk         (RiskEngine, RiskMetrics trait, RiskPersistence trait — Phase 4.1 交付)
```

### 0.3 验收标准

1. **编译通过**: `cargo build --workspace` zero warnings (clippy pedantic)
2. **单元测试**: `cargo test --workspace` 全绿，核心路径覆盖率 ≥ 80%
3. **集成测试**: paper-trade E2E 流程跑通（WS mock → detection → sizing → paper order → audit）
4. **Dry-run 模式**: `execution_mode = "dry_run"` 下完整管线无副作用运行
5. **优雅关停**: SIGTERM 30 秒内完成 drain、flush、close
6. **指标**: Prometheus `/metrics` 端点暴露所有关键计数器
7. **告警**: L4 circuit breaker trip → Telegram 消息送达
8. **状态恢复**: 进程重启后从 PG `risk_engine_state` 恢复 breaker FSM 状态

---

## 1. Crate 架构

### 1.1 完整目录结构

```
crates/oxide-arb-core/
├── Cargo.toml
└── src/
    ├── lib.rs                          # pub mod 声明 + crate doc
    │
    ├── app.rs                          # AppContext 定义 + build() + lifecycle
    ├── task_registry.rs                # TaskRegistry: JoinSet 管理 + graceful drain
    │
    ├── pipeline/
    │   ├── mod.rs
    │   ├── book_store.rs               # BookStore: DashMap<TokenId, Arc<RwLock<OrderBook>>>
    │   ├── order_book.rs               # OrderBook: bid/ask 存储 + apply_snapshot/apply_delta
    │   ├── market_registry.rs          # MarketRegistry: 市场元数据 + token↔market 映射
    │   ├── market_cache.rs             # MarketCache: 热路径活跃市场缓存
    │   ├── data_pipeline.rs            # DataPipeline: WS event loop 主循环
    │   ├── dual_book_assembler.rs      # DualBookAssembler: YES+NO → EndgameBookSnapshot
    │   ├── book_gate.rs                # BookGate: 质量检查（缺失/空/过期/交叉）
    │   └── staleness_classifier.rs     # StalenessClassifier: 基于 MarketDataConfig 阈值分级
    │
    ├── detection/
    │   ├── mod.rs
    │   ├── scanner.rs                  # Scanner: 包装 OpportunityPipeline, 构造 MarketScanInput
    │   ├── coalescer.rs                # Coalescer: 去重合并多 token 更新为一次 market scan
    │   └── funnel.rs                   # Funnel: 速率限制 + 优先队列
    │
    ├── execution/
    │   ├── mod.rs
    │   ├── execution_pipeline.rs       # ExecutionPipeline: validate → size → plan → dispatch → confirm → audit
    │   ├── validator.rs                # Validator: 新鲜度 + risk pre-check
    │   ├── plan_builder.rs             # PlanBuilder: Opportunity + approved size → ExecutionPlan
    │   ├── dispatcher.rs               # Dispatcher: 下单 + 确认
    │   ├── runner.rs                   # Runner: async 执行循环
    │   ├── capital_manager.rs          # CapitalManager: 包装 InMemory ExposureReservationBackend
    │   ├── tiered_strategy.rs          # TieredExecutionStrategy: FOK → GTD(30s) → GTD(5min)
    │   ├── fsm.rs                      # ExecutionFSM: Idle→Validate→Exec→Idle + Emergency
    │   └── types.rs                    # ExecutionPlan, ExecutionResult, ExecutionOutcome
    │
    ├── bridge/
    │   ├── mod.rs
    │   ├── risk_metrics.rs             # CoreRiskMetrics: impl RiskMetrics for oxide-arb-risk
    │   ├── risk_persistence.rs         # CoreRiskPersistence: impl RiskPersistence
    │   └── calibration_source.rs       # CoreCalibrationDataSource: impl CalibrationDataSource
    │
    ├── service/
    │   ├── mod.rs
    │   ├── fee_params_service.rs       # FeeParamsService: CacheKey::FeeParams 读穿
    │   ├── position_summary_service.rs # PositionSummaryService: CacheKey::PositionSummary 读穿
    │   ├── wallet_balance_service.rs   # WalletBalanceService: CacheKey::Balance 读穿
    │   └── cache_invalidation.rs       # CacheInvalidationCoordinator
    │
    ├── exposure/
    │   ├── mod.rs
    │   └── in_memory.rs                # InMemoryExposureReservation: DashMap + AtomicU64 CAS
    │
    ├── outbox/
    │   ├── mod.rs
    │   ├── event_store.rs              # EventStore trait + PgEventStore impl
    │   ├── flusher.rs                  # OutboxFlusher: FOR UPDATE SKIP LOCKED
    │   └── consumer.rs                 # OutboxConsumer trait + dead-letter
    │
    ├── infra/
    │   ├── mod.rs
    │   ├── async_writer.rs             # AsyncWriter<T>: mpsc → 批量 DB/CH 写入
    │   ├── debounced_writer.rs         # DebouncedWriter: 合并高频写入
    │   ├── periodic_task.rs            # PeriodicTask: interval + jitter 封装
    │   ├── health_checker.rs           # HealthChecker: WS/API/DB/Redis/CH 探针
    │   ├── retry_policy.rs             # RetryPolicy: 指数退避 + circuit breaker 集成
    │   └── oracle_health_tracker.rs    # OracleHealthTracker: 300s 滑动窗口健康评估
    │
    └── observability/
        ├── mod.rs
        ├── metrics_hub.rs              # MetricsHub: 所有 Prometheus 计数器/直方图
        ├── alert_dispatcher.rs         # AlertDispatcher: Telegram + Webhook
        └── report_generator.rs         # ReportGenerator: 每日/每周报告
```

### 1.2 `Cargo.toml`

```toml
[package]
name = "oxide-arb-core"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
publish = false

[dependencies]
# ── Workspace internal ─────────────────────────────────────────────
oxide-arb-error      = { path = "../oxide-arb-error", features = ["storage", "config", "serde"] }
oxide-arb-models     = { path = "../oxide-arb-models" }
oxide-arb-api        = { path = "../oxide-arb-api" }
oxide-arb-storage    = { path = "../oxide-arb-storage" }
oxide-arb-repository = { path = "../oxide-arb-repository" }
oxide-arb-algorithm  = { path = "../oxide-arb-algorithm" }
oxide-arb-risk       = { path = "../oxide-arb-risk" }

# ── Async runtime ──────────────────────────────────────────────────
tokio      = { workspace = true, features = ["rt-multi-thread", "macros", "signal", "time", "sync"] }
tokio-util = { workspace = true, features = ["rt"] }
futures-util = { workspace = true }
async-trait  = { workspace = true }

# ── Serialization ──────────────────────────────────────────────────
serde      = { workspace = true, features = ["derive"] }
serde_json = { workspace = true }

# ── Money / time ───────────────────────────────────────────────────
rust_decimal       = { workspace = true }
rust_decimal_macros = { workspace = true }
chrono = { workspace = true, features = ["serde"] }
uuid   = { workspace = true, features = ["v4", "v7"] }

# ── Errors / logging ──────────────────────────────────────────────
thiserror = { workspace = true }
tracing   = { workspace = true }

# ── Concurrency ────────────────────────────────────────────────────
dashmap     = { workspace = true }
parking_lot = { workspace = true }
arc-swap    = { workspace = true }
flume       = { workspace = true }

# ── Resilience ─────────────────────────────────────────────────────
backoff  = { workspace = true, features = ["tokio"] }
governor = { workspace = true }

# ── Observability ──────────────────────────────────────────────────
prometheus = { workspace = true }
teloxide   = { workspace = true, features = ["macros"] }
reqwest    = { workspace = true, features = ["json"] }

# ── DB (for outbox) ───────────────────────────────────────────────
sea-orm = { workspace = true }

# ── Cache ──────────────────────────────────────────────────────────
moka = { workspace = true, features = ["future"] }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
wiremock = { workspace = true }
testcontainers = { workspace = true }
testcontainers-modules = { workspace = true, features = ["postgres", "redis"] }
proptest = "1"
criterion = { version = "0.5", features = ["html_reports", "async_tokio"] }

[[bench]]
name = "pipeline_bench"
harness = false
```

### 1.3 模块层次

```
oxide_arb_core
├── app              (AppContext, lifecycle)
├── task_registry    (TaskRegistry)
├── pipeline         (OrderBook, BookStore, MarketRegistry, MarketCache,
│                     DataPipeline, DualBookAssembler, BookGate, StalenessClassifier)
├── detection        (Scanner, Coalescer, Funnel)
├── execution        (ExecutionPipeline, Validator, PlanBuilder, Dispatcher,
│                     Runner, CapitalManager, TieredExecutionStrategy, ExecutionFSM,
│                     ExecutionPlan, ExecutionResult, ExecutionOutcome)
├── bridge           (CoreRiskMetrics, CoreRiskPersistence, CoreCalibrationDataSource)
├── service          (FeeParamsService, PositionSummaryService,
│                     WalletBalanceService, CacheInvalidationCoordinator)
├── exposure         (InMemoryExposureReservation)
├── outbox           (EventStore, PgEventStore, OutboxFlusher, OutboxConsumer)
├── infra            (AsyncWriter, DebouncedWriter, PeriodicTask, HealthChecker,
│                     RetryPolicy, OracleHealthTracker)
└── observability    (MetricsHub, AlertDispatcher, ReportGenerator)
```

---

## 2. AppContext DI 设计

### 2.1 AppContext 完整定义

```rust
pub struct AppContext {
    // ── Configuration ─────────────────────────────────────────────
    pub settings: Arc<Settings>,

    // ── Infrastructure ────────────────────────────────────────────
    pub pg_pool: Arc<PostgresPool>,
    pub ch_pool: Arc<ClickHousePool>,
    pub cache: Arc<TieredCache>,
    pub shutdown: CancellationToken,
    pub task_registry: Arc<TaskRegistry>,
    pub metrics: Arc<MetricsHub>,
    pub alerts: Arc<AlertDispatcher>,

    // ── API clients ───────────────────────────────────────────────
    pub clob_client: Arc<ClobClient>,
    pub ws_manager: Arc<ClobWsManager>,
    pub gamma_client: Arc<GammaClient>,
    pub fee_calculator: Arc<FeeCalculator>,
    pub voting_oracle: Arc<VotingOracle>,
    pub keystore: Arc<Keystore>,

    // ── Repositories ──────────────────────────────────────────────
    pub market_repo: Arc<dyn MarketRepository>,
    pub event_repo: Arc<dyn EventRepository>,
    pub trade_repo: Arc<dyn TradeRepository>,
    pub position_repo: Arc<dyn PositionRepository>,
    pub risk_state_repo: Arc<dyn RiskStateRepository>,
    pub calibration_repo: Arc<dyn CalibrationRepository>,
    pub lifecycle_repo: Arc<dyn LifecycleRepository>,
    pub accounting_repo: Arc<dyn AccountingRepository>,
    pub report_repo: Arc<dyn ReportRepository>,
    pub potential_loss_repo: Arc<dyn PotentialLossRepository>,
    pub runtime_config_repo: Arc<dyn RuntimeConfigRepository>,
    pub timeseries_repo: Arc<dyn TimeseriesRepository>,

    // ── Algorithm layer ───────────────────────────────────────────
    pub opportunity_pipeline: Arc<OpportunityPipeline>,
    pub calibration_updater: Arc<CalibrationUpdater>,
    pub calibrator: Arc<ResolutionCalibrator>,

    // ── Risk engine (Phase 4.1) ───────────────────────────────────
    pub risk_engine: Arc<RiskEngine>,

    // ── Data pipeline ─────────────────────────────────────────────
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,
    pub data_pipeline: Arc<DataPipeline>,
    pub dual_book_assembler: Arc<DualBookAssembler>,

    // ── Detection ─────────────────────────────────────────────────
    pub scanner: Arc<Scanner>,
    pub coalescer: Arc<Coalescer>,
    pub funnel: Arc<Funnel>,

    // ── Execution ─────────────────────────────────────────────────
    pub execution_pipeline: Arc<ExecutionPipeline>,
    pub capital_manager: Arc<CapitalManager>,
    pub execution_fsm: Arc<ExecutionFSM>,
    pub tiered_strategy: Arc<TieredExecutionStrategy>,

    // ── Cache services ────────────────────────────────────────────
    pub fee_params_service: Arc<FeeParamsService>,
    pub position_summary_service: Arc<PositionSummaryService>,
    pub wallet_balance_service: Arc<WalletBalanceService>,
    pub cache_invalidation: Arc<CacheInvalidationCoordinator>,

    // ── Exposure reservation ──────────────────────────────────────
    pub exposure_backend: Arc<dyn ExposureReservationBackend>,

    // ── Outbox ────────────────────────────────────────────────────
    pub event_store: Arc<dyn EventStore>,
    pub outbox_flusher: Arc<OutboxFlusher>,

    // ── Infra services ────────────────────────────────────────────
    pub health_checker: Arc<HealthChecker>,
    pub oracle_health_tracker: Arc<OracleHealthTracker>,
    pub trade_writer: Arc<AsyncWriter<NewTrade>>,
    pub lifecycle_writer: Arc<AsyncWriter<NewLifecycleEvent>>,
    pub ch_writer: Arc<AsyncWriter<OpportunityAuditRow>>,
    pub report_generator: Arc<ReportGenerator>,
}
```

### 2.2 `AppContext::build()` 构建顺序

构建遵循严格的依赖图拓扑序。每个阶段的产物是后续阶段的输入。

```
Phase  阶段              输出                                        依赖
─────  ────────────────  ─────────────────────────────────────────   ──────
  1    Config            Settings                                    config files + env
  2    Infra-base        shutdown, MetricsHub, AlertDispatcher       (1)
  3    Storage           PostgresPool, ClickHousePool, TieredCache   (1)
  4    Migrations        DDL up-to-date                              (3)
  5    Seed              risk_engine_state row, runtime_config rows   (3,4)
  6    Repositories      12 repo traits (Pg* + Cached* wrappers)     (3)
  7    API clients       Keystore, ClobClient, ClobWsManager,        (1)
                         GammaClient, FeeCalculator, VotingOracle
  8    Exposure          InMemoryExposureReservation                  (1)
  9    Cache services    FeeParamsService, PositionSummaryService,    (6,7,3,8)
                         WalletBalanceService, CacheInvalidation
 10    Risk bridge       CoreRiskMetrics, CoreRiskPersistence         (6,9)
 11    Risk engine       RiskEngine (from oxide-arb-risk)             (10, risk_state from DB)
 12    Algorithm         ResolutionCalibrator, EndgameDetector,       (7,6)
                         EndgameScorer, OpportunityPipeline
 13    Calibration       CoreCalibrationDataSource, CalibrationUpdater (6,7,12)
 14    Data pipeline     BookStore, MarketRegistry, MarketCache,      (7,2)
                         DualBookAssembler, DataPipeline
 15    Detection         Scanner, Coalescer, Funnel                   (12,14)
 16    Execution         Validator, PlanBuilder, Dispatcher,          (7,8,9,11,15)
                         TieredStrategy, CapitalManager,
                         ExecutionFSM, Runner, ExecutionPipeline
 17    Outbox            PgEventStore, OutboxFlusher                  (3)
 18    Writers           AsyncWriter<NewTrade>, AsyncWriter<...>,     (6,3)
                         DebouncedWriter<RiskState>
 19    Infra services    HealthChecker, OracleHealthTracker,          (3,7)
                         ReportGenerator, PeriodicTasks
 20    TaskRegistry      TaskRegistry (空, 尚未 spawn)               (2)
```

```rust
impl AppContext {
    pub async fn build(config_dir: &Path) -> Result<Self, OxideError> {
        // Phase 1: Config
        let settings = Arc::new(Settings::new(config_dir)?);
        settings.validate_for_mode(settings.inner().execution.execution_mode)?;

        // Phase 2: Infra base
        let shutdown = CancellationToken::new();
        let metrics = Arc::new(MetricsHub::new());
        let alerts = Arc::new(AlertDispatcher::new(
            &settings.inner().notification,
            metrics.clone(),
        ));

        // Phase 3: Storage
        let pg_pool = Arc::new(PostgresPool::connect(&settings.inner().db.postgres).await?);
        let ch_pool = Arc::new(ClickHousePool::connect(&settings.inner().analytics).await?);
        let cache = Arc::new(TieredCache::new(
            MokaBackend::new(&settings.inner().cache.moka),
            RedisBackend::new(&settings.inner().cache.redis).await?,
        ));

        // Phase 4: Migrations
        Migrator::up(pg_pool.connection(), None).await?;
        ch_pool.ensure_schema().await?;

        // Phase 5: Seed
        let seed_ctx = SeedContext { /* ... */ };
        trading_bootstrap_v1().execute(pg_pool.connection(), &seed_ctx).await?;

        // Phase 6: Repositories (with cached wrappers for hot-path reads)
        let market_repo: Arc<dyn MarketRepository> = Arc::new(
            CachedMarketRepository::new(PgMarketRepository::new(pg_pool.clone()), cache.clone())
        );
        // ... 11 more repositories ...

        // Phase 7: API clients
        let keystore = Arc::new(Keystore::from_config(&settings.inner().keys)?);
        let clob_client = Arc::new(
            ClobClient::connect(keystore.signer().clone(), &settings.inner().polymarket).await?
        );
        let ws_manager = Arc::new(ClobWsManager::new(
            &settings.inner().polymarket,
            &settings.inner().market_data.websocket,
            shutdown.clone(),
        ));
        let gamma_client = Arc::new(GammaClient::new(settings.inner().market_data.gamma.clone()));
        let fee_calculator = Arc::new(FeeCalculator::from_config(&settings.inner().polymarket.fees));
        let voting_oracle = Arc::new(build_voting_oracle(&settings.inner())?);

        // Phase 8–20: ... (each phase depends on prior outputs)

        Ok(Self { /* all fields */ })
    }
}
```

### 2.3 生命周期: startup → run → graceful shutdown

```
┌──────────────────────────────────────────────────────────────────────┐
│                         STARTUP                                     │
│                                                                     │
│  1. AppContext::build()                                              │
│     ├── 加载 config、连接 PG/CH/Redis                                │
│     ├── 运行 migrations + seed                                      │
│     ├── 构建所有 repositories、API clients                           │
│     ├── 从 PG 加载 risk_engine_state → 恢复 breaker FSM             │
│     ├── 从 PG 加载 calibration buckets → 初始化 ResolutionCalibrator │
│     └── 构建全部 pipeline/service/infra 组件                         │
│                                                                     │
│  2. AppContext::run()                                                │
│     ├── 启动 Prometheus metrics server (HTTP :9090)                  │
│     ├── spawn DataPipeline event loop                                │
│     ├── spawn Scanner + Coalescer + Funnel                           │
│     ├── spawn ExecutionPipeline Runner                                │
│     ├── spawn OutboxFlusher                                          │
│     ├── spawn PeriodicTask: GammaSyncer (300s)                       │
│     ├── spawn PeriodicTask: CalibrationUpdater (3600s)               │
│     ├── spawn PeriodicTask: HealthChecker (30s)                      │
│     ├── spawn PeriodicTask: WalletBalanceRefresh (15s)               │
│     ├── spawn PeriodicTask: RiskStatePersist (60s)                   │
│     ├── spawn PeriodicTask: ReportGenerator (daily at 00:05 UTC)     │
│     ├── spawn PeriodicTask: ExposureGc (30s)                         │
│     ├── spawn PeriodicTask: LedgerReconciler (300s)                  │
│     ├── spawn AsyncWriter workers (trade, lifecycle, CH)             │
│     └── 所有 task 注册到 TaskRegistry                                │
│                                                                     │
│  3. 等待 shutdown 信号 (SIGTERM / SIGINT / manual trigger)          │
│                                                                     │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│                      GRACEFUL SHUTDOWN                               │
│                                                                     │
│  ShutdownStage::Draining                                            │
│    ├── 触发 CancellationToken                                       │
│    ├── DataPipeline 停止接收新 WS 事件                               │
│    ├── Scanner/Coalescer/Funnel 停止发射新机会                       │
│    ├── ExecutionFSM 拒绝新 Validate 转换                            │
│                                                                     │
│  ShutdownStage::AwaitingInflight                                    │
│    ├── 等待所有 in-flight ExecutionPipeline 完成（最多 30s）          │
│    ├── 取消超时仍在执行的 order                                     │
│                                                                     │
│  ShutdownStage::Flushing                                            │
│    ├── AsyncWriter drain (flush pending batches)                    │
│    ├── OutboxFlusher 最后一次 flush                                  │
│    ├── RiskState persist (最终快照写入 PG)                           │
│    ├── DebouncedWriter 强制 flush                                   │
│                                                                     │
│  ShutdownStage::Stopped                                             │
│    ├── TaskRegistry::drain(Duration::from_secs(30))                 │
│    ├── 关闭 PG/CH/Redis 连接                                       │
│    ├── 记录 lifecycle event: KillSwitchTriggered / Stopped          │
│    └── exit(0)                                                      │
│                                                                     │
│  超时保护: 整个 shutdown 序列硬限 45s, 超时强制 exit(1)             │
│                                                                     │
└──────────────────────────────────────────────────────────────────────┘
```

### 2.4 TaskRegistry 设计

```rust
pub struct TaskRegistry {
    tasks: Mutex<JoinSet<TaskResult>>,
    task_names: Mutex<Vec<String>>,
    shutdown: CancellationToken,
}

pub struct TaskResult {
    pub name: String,
    pub result: Result<(), OxideError>,
    pub elapsed: Duration,
}

impl TaskRegistry {
    pub fn new(shutdown: CancellationToken) -> Self;

    /// 注册一个命名异步任务。任务内部必须监听 shutdown token。
    pub fn spawn<F>(&self, name: impl Into<String>, future: F)
    where
        F: Future<Output = Result<(), OxideError>> + Send + 'static;

    /// 等待所有任务完成，最多等待 `timeout` 时长。
    /// 超时后对剩余任务调用 abort()。
    pub async fn drain(&self, timeout: Duration) -> Vec<TaskResult>;

    /// 当前活跃任务数。
    pub fn active_count(&self) -> usize;

    /// 当前所有任务名称（用于 health endpoint 展示）。
    pub fn task_names(&self) -> Vec<String>;
}
```

**关键设计决策**:

- 使用 `tokio::task::JoinSet` 而非手动 `Vec<JoinHandle>`, 因为 `JoinSet` 支持 `join_next()` 逐一收割，不会因单个任务 panic 丢失其他结果。
- drain 超时 30s：在 SIGTERM 后，如果某个任务 30s 内未退出，`abort()` 强制终止，然后继续关停流程。
- 每个 spawned task 的 future 必须在顶层 `select!` 监听 `shutdown.cancelled()`，否则 drain 会超时。

---

## 3. 数据管线 (Data Pipeline)

### 3.1 OrderBook

```rust
/// 单 token 的 L2 订单簿，hot-path 数据结构。
/// 不涉及 USD 价值计算 — 纯粹的 bid/ask level 存储。
pub struct OrderBook {
    /// bid levels: 按价格降序排列
    bids: Vec<BookLevel>,
    /// ask levels: 按价格升序排列
    asks: Vec<BookLevel>,
    /// 最近一次更新时间（exchange epoch millis）
    last_update_ms: u64,
    /// 所属 token ID（用于日志和指标标识）
    token_id: TokenId,
}

impl OrderBook {
    pub fn new(token_id: TokenId) -> Self;

    /// 完整替换订单簿（WS BookSnapshot 事件）
    pub fn apply_snapshot(&mut self, bids: Vec<BookLevel>, asks: Vec<BookLevel>, timestamp_ms: u64);

    /// 增量更新（WS PriceChange 事件）
    /// 对每个 (price, size) pair:
    ///   - size > 0: 插入或更新该价位
    ///   - size == 0: 移除该价位
    /// 更新后重新排序以维持不变量。
    pub fn apply_delta(&mut self, changes: &[(Price, Shares)], timestamp_ms: u64);

    /// 获取 bid 侧的 OrderbookSide 快照
    pub fn bid_side(&self) -> OrderbookSide;

    /// 获取 ask 侧的 OrderbookSide 快照
    pub fn ask_side(&self) -> OrderbookSide;

    /// 最近更新时间
    pub fn last_update_ms(&self) -> u64;

    /// 是否为空（无任何 level）
    pub fn is_empty(&self) -> bool;

    /// best bid price
    pub fn best_bid(&self) -> Option<Price>;

    /// best ask price
    pub fn best_ask(&self) -> Option<Price>;

    /// bid-ask spread (None if either side is empty)
    pub fn spread(&self) -> Option<Price>;

    /// 总深度（单侧）
    pub fn bid_depth(&self) -> Shares;
    pub fn ask_depth(&self) -> Shares;

    /// 检测 crossed book (best_bid >= best_ask)
    pub fn is_crossed(&self) -> bool;
}
```

**排序不变量**: `bids` 始终按 price 降序，`asks` 始终按 price 升序。`apply_delta` 使用二分查找插入/更新/删除，O(log n) 查找 + O(n) 最坏情况移位。对于 Polymarket 的典型 book 深度 (~50 levels)，这比 `BTreeMap` 更快（cache-friendly，零堆分配）。

### 3.2 BookStore

```rust
/// 所有 token 的订单簿中心存储。
/// 选择 DashMap 而非 HashMap + Mutex 因为:
/// 1. WS 事件循环是单线程写入，但 Scanner/Detector 可并发读
/// 2. DashMap 的分片锁比全局 Mutex 竞争更少
/// 3. 订单簿数量 O(1k)，DashMap 的内存开销可忽略
pub struct BookStore {
    books: DashMap<TokenId, Arc<RwLock<OrderBook>>>,
    metrics: Arc<MetricsHub>,
}

impl BookStore {
    pub fn new(metrics: Arc<MetricsHub>) -> Self;

    /// 获取或创建 token 的订单簿
    pub fn get_or_create(&self, token_id: &TokenId) -> Arc<RwLock<OrderBook>>;

    /// 获取已存在的订单簿（不创建）
    pub fn get(&self, token_id: &TokenId) -> Option<Arc<RwLock<OrderBook>>>;

    /// 应用 WS BookSnapshot
    pub fn apply_snapshot(
        &self,
        token_id: &TokenId,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
        timestamp_ms: u64,
    );

    /// 应用 WS PriceChange delta
    pub fn apply_delta(
        &self,
        token_id: &TokenId,
        changes: &[(Price, Shares)],
        timestamp_ms: u64,
    );

    /// 移除 token 的订单簿（市场下线时）
    pub fn remove(&self, token_id: &TokenId);

    /// 当前跟踪的 token 数量
    pub fn token_count(&self) -> usize;
}
```

**锁策略**: `Arc<RwLock<OrderBook>>` 使用 `parking_lot::RwLock`（非 tokio 的，因为 hold 时间极短 < 1μs，不需要异步友好的锁）。WS event loop 写入持有 write lock，Scanner 读取持有 read lock。

### 3.3 MarketRegistry

```rust
/// 市场元数据注册表 + token ↔ market 双向映射。
/// 数据源: Gamma API full_sync + incremental_sync。
pub struct MarketRegistry {
    /// market_id → MarketEntry
    markets: DashMap<MarketId, MarketEntry>,
    /// token_id → market_id (快速反查: WS 收到 token 更新时需要知道属于哪个 market)
    token_to_market: DashMap<TokenId, MarketId>,
    /// event_id → EventEntry
    events: DashMap<EventId, EventEntry>,
    /// 活跃市场 ID 集合（status = Active）
    active_market_ids: parking_lot::RwLock<Vec<MarketId>>,
}

impl MarketRegistry {
    pub fn new() -> Self;

    /// 注册或更新一个市场（来自 Gamma sync）
    pub fn upsert_market(&self, entry: MarketEntry);

    /// 批量注册（Gamma full_sync 后）
    pub fn upsert_batch(&self, entries: Vec<MarketEntry>);

    /// 注册事件
    pub fn upsert_event(&self, entry: EventEntry);

    /// token → market 反查
    pub fn market_for_token(&self, token_id: &TokenId) -> Option<MarketId>;

    /// 获取市场元数据
    pub fn get_market(&self, market_id: &MarketId) -> Option<MarketEntry>;

    /// 获取市场的 YES + NO token IDs
    pub fn token_pair(&self, market_id: &MarketId) -> Option<(TokenId, TokenId)>;

    /// 所有活跃市场 IDs
    pub fn active_market_ids(&self) -> Vec<MarketId>;

    /// 刷新活跃市场列表
    pub fn refresh_active(&self);

    /// 市场总数
    pub fn market_count(&self) -> usize;
}
```

### 3.4 MarketCache

```rust
/// 热路径活跃市场缓存。
/// 缓存 Scanner 循环中频繁访问的市场属性，避免每次 scan tick
/// 都查 DashMap 和解构 MarketEntry。
pub struct MarketCache {
    /// 活跃市场预编译扫描输入（含 token pair, category, settlement_deadline）
    scan_entries: ArcSwap<Vec<CachedMarketScanEntry>>,
    registry: Arc<MarketRegistry>,
}

/// 预编译的扫描输入条目
pub struct CachedMarketScanEntry {
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_yes: TokenId,
    pub token_no: TokenId,
    pub category: MarketCategory,
    pub tick_size: TickSize,
    pub neg_risk: bool,
    pub settlement_deadline: Option<DateTime<Utc>>,
}

impl MarketCache {
    pub fn new(registry: Arc<MarketRegistry>) -> Self;

    /// 从 MarketRegistry 重新构建热缓存
    pub fn rebuild(&self);

    /// 获取当前热缓存快照（无锁读）
    pub fn entries(&self) -> Arc<Vec<CachedMarketScanEntry>>;
}
```

**为什么需要 MarketCache**: Scanner 每 5 秒扫描所有活跃市场。如果每次都从 `DashMap` 读取 `MarketEntry` 并解构，会产生不必要的 clone 和 `Arc` ref-counting。`ArcSwap` 提供无锁原子替换，reload 只在 Gamma sync 后执行（每 5 分钟一次）。

### 3.5 DataPipeline 事件循环

```rust
pub struct DataPipeline {
    ws_manager: Arc<ClobWsManager>,
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    coalescer: Arc<Coalescer>,
    metrics: Arc<MetricsHub>,
    shutdown: CancellationToken,
}

impl DataPipeline {
    pub fn new(
        ws_manager: Arc<ClobWsManager>,
        book_store: Arc<BookStore>,
        market_registry: Arc<MarketRegistry>,
        coalescer: Arc<Coalescer>,
        metrics: Arc<MetricsHub>,
        shutdown: CancellationToken,
    ) -> Self;

    /// 主事件循环 — 作为长时间运行的 task spawn。
    ///
    /// 处理流程:
    /// 1. 从 ClobWsManager.events() 接收 WsEvent
    /// 2. 路由到对应处理器
    /// 3. 更新 BookStore
    /// 4. 通知 Coalescer 触发检测
    pub async fn run(&self) -> Result<(), OxideError> {
        let rx = self.ws_manager.events();
        loop {
            tokio::select! {
                biased;  // shutdown 优先级最高

                _ = self.shutdown.cancelled() => {
                    tracing::info!("DataPipeline shutting down");
                    return Ok(());
                }

                event = rx.recv_async() => {
                    match event {
                        Ok(ws_event) => self.handle_event(ws_event).await,
                        Err(_) => {
                            tracing::error!("WS event channel closed unexpectedly");
                            return Err(OxideError::Internal(
                                "WS event channel closed".into()
                            ));
                        }
                    }
                }
            }
        }
    }

    async fn handle_event(&self, event: WsEvent) {
        self.metrics.ws_events_received.inc();

        match event {
            WsEvent::BookSnapshot { asset_id, bids, asks, timestamp_ms, .. } => {
                let token_id = TokenId::new(&asset_id);
                let levels_bids = bids.into_iter().map(/* wire → BookLevel */).collect();
                let levels_asks = asks.into_iter().map(/* wire → BookLevel */).collect();
                self.book_store.apply_snapshot(&token_id, levels_bids, levels_asks, timestamp_ms);
                self.coalescer.notify_token_update(&token_id);
                self.metrics.book_snapshots_applied.inc();
            }
            WsEvent::PriceChange { asset_id, changes, timestamp_ms } => {
                let token_id = TokenId::new(&asset_id);
                let deltas: Vec<(Price, Shares)> = changes.iter()
                    .map(/* wire → (Price, Shares) */)
                    .collect();
                self.book_store.apply_delta(&token_id, &deltas, timestamp_ms);
                self.coalescer.notify_token_update(&token_id);
                self.metrics.price_changes_applied.inc();
            }
            WsEvent::MarketResolved { market_id, .. } => {
                tracing::info!(%market_id, "Market resolved via WS");
                self.metrics.markets_resolved_ws.inc();
                // 通知 Scanner 移除该市场
            }
            WsEvent::TickSizeChange { asset_id, new_tick, .. } => {
                // 更新 MarketRegistry 的 tick_size
                // 添加 token 到 blacklist (TickChange reason, 短期)
            }
            WsEvent::ShardStatus { shard_id, status } => {
                self.metrics.shard_status_changes.inc();
                // 如果 shard 断开，通知 OracleHealthTracker
            }
            _ => {
                // BestBidAsk, LastTradePrice — 仅更新指标，不触发检测
                self.metrics.ws_events_ignored.inc();
            }
        }
    }
}
```

### 3.6 DualBookAssembler

```rust
/// 将 YES 和 NO 两个单 token 的 OrderBook 组装为 EndgameBookSnapshot。
///
/// EndgameBookSnapshot 是 OpportunityPipeline::process() 的输入。
/// 两个 token book 的 timestamp 取各自最新值。
pub struct DualBookAssembler;

impl DualBookAssembler {
    /// 组装双边订单簿快照。
    /// 返回 None 如果任一 token book 不存在于 BookStore。
    pub fn assemble(
        book_store: &BookStore,
        token_yes: &TokenId,
        token_no: &TokenId,
    ) -> Option<EndgameBookSnapshot> {
        let yes_book = book_store.get(token_yes)?;
        let no_book = book_store.get(token_no)?;

        let yes_guard = yes_book.read();
        let no_guard = no_book.read();

        Some(EndgameBookSnapshot {
            yes_bids: yes_guard.bid_side(),
            yes_asks: yes_guard.ask_side(),
            no_bids: no_guard.bid_side(),
            no_asks: no_guard.ask_side(),
        })
    }
}
```

**锁顺序**: 总是先锁 YES 再锁 NO，防止 deadlock。因为 `parking_lot::RwLock` 是非 async 且 hold 时间极短（< 1μs 拷贝 Vec<BookLevel>），不会造成 worker starvation。

### 3.7 BookGate 质量检查

```rust
/// 订单簿质量门禁 — 在进入 OpportunityPipeline 前检查数据完整性。
pub struct BookGate;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BookGateError {
    /// 至少一侧缺失数据（WS 尚未收到 snapshot）
    MissingSide {
        token_id: TokenId,
        side: &'static str,
    },
    /// 某侧完全没有 level
    EmptySide {
        token_id: TokenId,
        side: &'static str,
    },
    /// 数据过期（超过 staleness_expired_ms 阈值）
    Stale {
        token_id: TokenId,
        age_ms: u64,
        threshold_ms: u64,
    },
    /// Crossed book: best_bid >= best_ask（异常市场状态）
    CrossedBook {
        token_id: TokenId,
        best_bid: Price,
        best_ask: Price,
    },
}

impl std::fmt::Display for BookGateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingSide { token_id, side } =>
                write!(f, "missing {side} for token {token_id}"),
            Self::EmptySide { token_id, side } =>
                write!(f, "empty {side} for token {token_id}"),
            Self::Stale { token_id, age_ms, threshold_ms } =>
                write!(f, "stale data for {token_id}: {age_ms}ms > {threshold_ms}ms"),
            Self::CrossedBook { token_id, best_bid, best_ask } =>
                write!(f, "crossed book for {token_id}: bid={best_bid} >= ask={best_ask}"),
        }
    }
}

impl BookGate {
    /// 检查 EndgameBookSnapshot 质量。返回所有发现的问题。
    pub fn check(
        snapshot: &EndgameBookSnapshot,
        now_ms: u64,
        expired_threshold_ms: u64,
        token_yes: &TokenId,
        token_no: &TokenId,
    ) -> Vec<BookGateError> {
        let mut errors = Vec::new();

        // 检查四个侧是否为空
        if snapshot.yes_bids.is_empty() {
            errors.push(BookGateError::EmptySide {
                token_id: token_yes.clone(),
                side: "bids",
            });
        }
        if snapshot.yes_asks.is_empty() {
            errors.push(BookGateError::EmptySide {
                token_id: token_yes.clone(),
                side: "asks",
            });
        }
        if snapshot.no_bids.is_empty() {
            errors.push(BookGateError::EmptySide {
                token_id: token_no.clone(),
                side: "bids",
            });
        }
        if snapshot.no_asks.is_empty() {
            errors.push(BookGateError::EmptySide {
                token_id: token_no.clone(),
                side: "asks",
            });
        }

        // 过期检查
        let max_age = snapshot.max_staleness_ms(now_ms);
        if max_age > expired_threshold_ms {
            errors.push(BookGateError::Stale {
                token_id: token_yes.clone(), // 哪个 token 不重要，取 yes 即可
                age_ms: max_age,
                threshold_ms: expired_threshold_ms,
            });
        }

        errors
    }

    /// 快速通过/拒绝 — 任一错误即失败
    pub fn pass(
        snapshot: &EndgameBookSnapshot,
        now_ms: u64,
        expired_threshold_ms: u64,
        token_yes: &TokenId,
        token_no: &TokenId,
    ) -> bool {
        Self::check(snapshot, now_ms, expired_threshold_ms, token_yes, token_no).is_empty()
    }
}
```

---

## 4. 检测触发 (Detection)

### 4.1 Scanner

```rust
/// 扫描器: 包装 OpportunityPipeline, 从 MarketCache 获取市场列表,
/// 从 BookStore/DualBookAssembler 获取实时订单簿快照,
/// 构造 MarketScanInput 并调用 pipeline.process()。
pub struct Scanner {
    pipeline: Arc<OpportunityPipeline>,
    book_store: Arc<BookStore>,
    market_cache: Arc<MarketCache>,
    staleness_classifier: StalenessClassifier,
    funnel: Arc<Funnel>,
    metrics: Arc<MetricsHub>,
}

impl Scanner {
    /// 扫描单个市场
    pub fn scan_market(
        &self,
        entry: &CachedMarketScanEntry,
        now: DateTime<Utc>,
    ) -> Option<ScoredOpportunity> {
        // 1. 组装双边订单簿
        let snapshot = DualBookAssembler::assemble(
            &self.book_store, &entry.token_yes, &entry.token_no
        )?;

        // 2. BookGate 质量检查
        let now_ms = now.timestamp_millis() as u64;
        if !BookGate::pass(&snapshot, now_ms, self.staleness_classifier.expired_ms(),
                           &entry.token_yes, &entry.token_no) {
            self.metrics.scans_gate_rejected.inc();
            return None;
        }

        // 3. 分类 staleness
        let staleness = self.staleness_classifier.classify(
            snapshot.max_staleness_ms(now_ms)
        );

        // 4. 调用 OpportunityPipeline
        self.pipeline.process(
            &entry.market_id,
            &entry.event_id,
            &entry.token_yes,
            &entry.token_no,
            &snapshot,
            entry.category,
            staleness,
            entry.settlement_deadline,
            now,
        )
    }

    /// 批量扫描所有活跃市场（fallback scan tick 使用）
    pub fn scan_all(&self, now: DateTime<Utc>) -> Vec<ScoredOpportunity> {
        let entries = self.market_cache.entries();
        let mut results = Vec::new();

        for entry in entries.iter() {
            if let Some(scored) = self.scan_market(entry, now) {
                results.push(scored);
            }
        }

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        self.metrics.scan_results_total.observe(results.len() as f64);
        results
    }
}
```

### 4.2 Coalescer

```rust
/// 去重合并器: 将多个 token 级别的 WS 更新合并为一次 market 级别的扫描。
///
/// 设计原理: Polymarket 的 YES 和 NO token 各自有独立的 WS 订阅。
/// 一次市场更新可能在 <100ms 内连续触发两个 token 的 BookSnapshot 事件。
/// 如果每个事件都触发一次 scanner.scan_market()，会造成:
///   1. 在只有单侧更新时发起扫描（另一侧尚未更新）
///   2. 重复计算浪费 CPU
///
/// Coalescer 接收 token 更新通知，等待 coalesce_window_ms (默认 300ms)，
/// 然后对该 market 发起一次扫描。
pub struct Coalescer {
    /// 收到更新但尚未触发 scan 的 market → 首次更新时间
    pending: DashMap<MarketId, Instant>,
    market_registry: Arc<MarketRegistry>,
    scanner: Arc<Scanner>,
    coalesce_window: Duration,
    shutdown: CancellationToken,
    metrics: Arc<MetricsHub>,
}

impl Coalescer {
    /// WS event loop 调用: 通知某 token 有更新
    pub fn notify_token_update(&self, token_id: &TokenId) {
        if let Some(market_id) = self.market_registry.market_for_token(token_id) {
            self.pending.entry(market_id).or_insert_with(Instant::now);
        }
    }

    /// 定期 tick 检查哪些 market 的 coalesce 窗口已到期
    pub async fn run(&self) -> Result<(), OxideError> {
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => return Ok(()),
                _ = interval.tick() => {
                    self.flush_ready().await;
                }
            }
        }
    }

    async fn flush_ready(&self) {
        let now = Instant::now();
        let mut ready = Vec::new();

        self.pending.retain(|market_id, first_seen| {
            if now.duration_since(*first_seen) >= self.coalesce_window {
                ready.push(market_id.clone());
                false // remove from pending
            } else {
                true // keep waiting
            }
        });

        for market_id in &ready {
            if let Some(entry) = self.find_cached_entry(market_id) {
                let now_utc = Utc::now();
                if let Some(scored) = self.scanner.scan_market(&entry, now_utc) {
                    self.metrics.coalesced_scans.inc();
                    // 发送到 Funnel
                    // ...
                }
            }
        }
    }
}
```

### 4.3 Funnel

```rust
/// 漏斗: 对 ScoredOpportunity 进行速率限制和优先级排序。
///
/// 防止检测层在短时间内向执行层发送过多机会（例如市场突然
/// 大面积进入收敛区域）。Funnel 维护一个有限容量的优先队列
/// (按 score 降序)，并以可配置的速率将机会逐个输出到
/// ExecutionPipeline。
pub struct Funnel {
    /// 有界优先队列
    queue: Mutex<BinaryHeap<ScoredEntry>>,
    /// 输出通道 → ExecutionPipeline
    tx: flume::Sender<ScoredOpportunity>,
    /// 最大队列深度
    max_queue_size: usize,
    /// 两次输出之间的最小间隔
    min_dispatch_interval: Duration,
    metrics: Arc<MetricsHub>,
}

struct ScoredEntry {
    scored: ScoredOpportunity,
    received_at: Instant,
}

impl Ord for ScoredEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.scored.score.partial_cmp(&other.scored.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl Funnel {
    pub fn new(
        tx: flume::Sender<ScoredOpportunity>,
        max_queue_size: usize,
        min_dispatch_interval: Duration,
        metrics: Arc<MetricsHub>,
    ) -> Self;

    /// 接收来自 Scanner/Coalescer 的机会
    pub fn submit(&self, scored: ScoredOpportunity) {
        let mut queue = self.queue.lock();
        if queue.len() >= self.max_queue_size {
            // 如果新 score > 队列最低 score, 替换
            if let Some(min) = queue.peek() {
                if scored.score > min.scored.score {
                    queue.pop();
                } else {
                    self.metrics.funnel_dropped.inc();
                    return;
                }
            }
        }
        queue.push(ScoredEntry {
            scored,
            received_at: Instant::now(),
        });
        self.metrics.funnel_enqueued.inc();
    }

    /// 输出循环 — 按节奏向 ExecutionPipeline 发送
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), OxideError> {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                _ = tokio::time::sleep(self.min_dispatch_interval) => {
                    if let Some(entry) = {
                        let mut queue = self.queue.lock();
                        queue.pop()
                    } {
                        let age_ms = entry.received_at.elapsed().as_millis() as u64;
                        self.metrics.funnel_dispatch_age_ms.observe(age_ms as f64);
                        let _ = self.tx.send_async(entry.scored).await;
                        self.metrics.funnel_dispatched.inc();
                    }
                }
            }
        }
    }
}
```

### 4.4 数据流图

```
ClobWsManager
    │
    ▼ WsEvent (flume channel)
DataPipeline::run()
    │
    ├── BookSnapshot ──► BookStore.apply_snapshot()
    │                         │
    │                         ▼
    │                    Coalescer.notify_token_update()
    │                         │
    │                         ▼ (300ms coalesce window)
    │                    Scanner.scan_market()
    │                         │
    │                         ├── DualBookAssembler.assemble()
    │                         ├── BookGate.pass()
    │                         ├── StalenessClassifier.classify()
    │                         └── OpportunityPipeline.process()
    │                              │
    │                              ▼ Option<ScoredOpportunity>
    │                         Funnel.submit()
    │                              │
    │                              ▼ (rate-limited dispatch)
    │                         ExecutionPipeline (via flume channel)
    │
    ├── PriceChange ──► BookStore.apply_delta()
    │                         └── (same flow as above)
    │
    └── [fallback: 5s periodic]
         Scanner.scan_all()
              └── 批量扫描 + Funnel.submit()
```

---

## 5. 执行管线 (Execution)

### 5.1 ExecutionPipeline

```rust
/// 完整的执行管线: validate → size → plan → dispatch → confirm → audit.
///
/// 每次处理一个 ScoredOpportunity。整个管线是同步顺序执行的
/// (非流水线), 因为:
/// 1. 只有一个策略, 不需要并发执行多个机会
/// 2. 顺序执行简化了 exposure reservation 的一致性
/// 3. ADR-001: 最大同时持仓 3 个, 不需要高吞吐执行
pub struct ExecutionPipeline {
    validator: Validator,
    plan_builder: PlanBuilder,
    dispatcher: Dispatcher,
    capital_manager: Arc<CapitalManager>,
    risk_engine: Arc<RiskEngine>,
    tiered_strategy: Arc<TieredExecutionStrategy>,
    fsm: Arc<ExecutionFSM>,
    lifecycle_writer: Arc<AsyncWriter<NewLifecycleEvent>>,
    trade_writer: Arc<AsyncWriter<NewTrade>>,
    ch_writer: Arc<AsyncWriter<OpportunityAuditRow>>,
    metrics: Arc<MetricsHub>,
    execution_mode: ExecutionMode,
}

impl ExecutionPipeline {
    /// 处理单个机会 — 完整管线
    pub async fn execute(
        &self,
        scored: ScoredOpportunity,
    ) -> ExecutionResult {
        let opp = &scored.opportunity;
        let timer = self.metrics.execution_latency.start_timer();

        // 1. FSM 转换: Idle → Validate
        if let Err(e) = self.fsm.transition(ExecState::Validate) {
            return ExecutionResult::rejected("FSM transition denied", e);
        }

        // 2. Validate: 新鲜度 + risk pre-check
        let validation = match self.validator.validate(opp).await {
            Ok(v) => v,
            Err(e) => {
                self.fsm.transition(ExecState::Idle).ok();
                self.lifecycle_writer.write(NewLifecycleEvent::rejected(opp, &e));
                return ExecutionResult::rejected("Validation failed", e);
            }
        };

        // 3. Risk check (via oxide-arb-risk RiskEngine)
        let risk_decision = self.risk_engine.pre_trade_check(opp).await;
        if !risk_decision.allowed {
            self.fsm.transition(ExecState::Idle).ok();
            self.lifecycle_writer.write(NewLifecycleEvent::risk_denied(opp, &risk_decision));
            self.metrics.risk_denials.inc();
            return ExecutionResult::risk_denied(risk_decision);
        }

        // 4. Position sizing (via oxide-arb-risk KellySizer)
        let approved_size = self.risk_engine.size_position(opp).await;
        if approved_size.is_zero() {
            self.fsm.transition(ExecState::Idle).ok();
            return ExecutionResult::rejected("Size is zero", "Kelly sizing returned 0");
        }

        // 5. Exposure reservation
        let reservation = match self.capital_manager.reserve(
            &opp.market_id,
            approved_size,
        ).await {
            Ok(r) => r,
            Err(e) => {
                self.fsm.transition(ExecState::Idle).ok();
                return ExecutionResult::rejected("Reservation failed", e);
            }
        };

        // 6. Build execution plan
        let plan = self.plan_builder.build(opp, approved_size, &reservation);

        // 7. FSM: Validate → Exec
        if let Err(e) = self.fsm.transition(ExecState::Exec) {
            self.capital_manager.release(&reservation.id).await.ok();
            return ExecutionResult::rejected("FSM Validate→Exec denied", e);
        }

        // 8. Dispatch (mode-aware: DryRun / Paper / Live)
        let outcome = match self.execution_mode {
            ExecutionMode::DryRun => {
                self.dispatcher.dry_run(&plan).await
            }
            ExecutionMode::Paper => {
                self.dispatcher.paper_trade(&plan).await
            }
            ExecutionMode::Live => {
                self.dispatcher.live_trade(&plan, &self.tiered_strategy).await
            }
        };

        // 9. Post-trade processing
        match &outcome {
            ExecutionOutcome::Filled { order_response, .. } => {
                self.capital_manager.confirm(&reservation.id).await.ok();
                self.risk_engine.record_trade_result(opp, &outcome).await;
                self.metrics.trades_filled.inc();
            }
            ExecutionOutcome::Miss { .. } | ExecutionOutcome::Failed { .. } => {
                self.capital_manager.release(&reservation.id).await.ok();
                self.risk_engine.record_trade_result(opp, &outcome).await;
                self.metrics.trades_missed.inc();
            }
        }

        // 10. FSM: Exec → Idle
        self.fsm.transition(ExecState::Idle).ok();

        // 11. Audit trail
        self.lifecycle_writer.write(NewLifecycleEvent::from_outcome(opp, &outcome));
        self.trade_writer.write(NewTrade::from_outcome(opp, &outcome));
        self.ch_writer.write(OpportunityAuditRow::from(opp, &scored, &outcome));

        timer.observe_duration();
        ExecutionResult::completed(outcome)
    }
}
```

### 5.2 Validator

```rust
pub struct Validator {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    staleness_classifier: StalenessClassifier,
    timeout_config: TradeTimeoutConfig,
    metrics: Arc<MetricsHub>,
}

impl Validator {
    /// 执行前验证:
    /// 1. 市场是否仍在注册表中且 status == Active
    /// 2. 当前 book 是否足够新鲜 (staleness <= Acceptable)
    /// 3. 当前价格 vs 检测时价格的 slippage 是否在阈值内
    /// 4. book depth 是否仍然足够
    pub async fn validate(&self, opp: &Opportunity) -> Result<ValidationResult, TradingError> {
        // tokio::time::timeout 包裹整个验证, 硬限 max_validation_time_ms
        tokio::time::timeout(
            Duration::from_millis(self.timeout_config.max_validation_time_ms),
            self.validate_inner(opp),
        ).await
        .map_err(|_| TradingError::Validation("validation timeout".into()))?
    }

    async fn validate_inner(&self, opp: &Opportunity) -> Result<ValidationResult, TradingError> {
        // a) 市场状态检查
        let market = self.market_registry.get_market(&opp.market_id)
            .ok_or(TradingError::MarketNotFound(opp.market_id.to_string()))?;
        if market.status != MarketStatus::Active {
            return Err(TradingError::Validation(
                format!("market {} is {:?}", opp.market_id, market.status)
            ));
        }

        // b) 新鲜度检查
        let (token_yes, token_no) = self.market_registry.token_pair(&opp.market_id)
            .ok_or(TradingError::MarketNotFound(opp.market_id.to_string()))?;
        let snapshot = DualBookAssembler::assemble(&self.book_store, &token_yes, &token_no)
            .ok_or(TradingError::Validation("book not available".into()))?;
        let now_ms = Utc::now().timestamp_millis() as u64;
        let staleness = self.staleness_classifier.classify(snapshot.max_staleness_ms(now_ms));
        if staleness > StalenessLevel::Acceptable {
            return Err(TradingError::Validation(
                format!("book staleness {:?} exceeds acceptable", staleness)
            ));
        }

        // c) Slippage 检查
        let current_price = match opp.side {
            Side::Buy => snapshot.yes_asks.best_price(),
            Side::Sell => snapshot.yes_bids.best_price(),
        }.ok_or(TradingError::Validation("no price on relevant side".into()))?;

        let slippage_bps = ((current_price.inner() - opp.entry_price.inner()).abs()
            / opp.entry_price.inner() * rust_decimal_macros::dec!(10000))
            .round();
        if slippage_bps > self.timeout_config.max_validation_slippage_bps {
            return Err(TradingError::Validation(
                format!("slippage {slippage_bps}bps exceeds max {}bps",
                        self.timeout_config.max_validation_slippage_bps)
            ));
        }

        Ok(ValidationResult {
            current_price,
            staleness,
            slippage_bps: Bps::new(slippage_bps),
            validated_at: Utc::now(),
        })
    }
}

pub struct ValidationResult {
    pub current_price: Price,
    pub staleness: StalenessLevel,
    pub slippage_bps: Bps,
    pub validated_at: DateTime<Utc>,
}
```

### 5.3 PlanBuilder

```rust
pub struct PlanBuilder {
    fee_calculator: Arc<FeeCalculator>,
}

impl PlanBuilder {
    /// 从 Opportunity + approved size 构建 ExecutionPlan
    pub fn build(
        &self,
        opp: &Opportunity,
        approved_size_usd: Usd,
        reservation: &ReservationHandle,
    ) -> ExecutionPlan {
        let shares = Shares::new(
            (approved_size_usd.inner() / opp.entry_price.inner()).round()
        );
        let fee = self.fee_calculator.calculate(
            shares, opp.entry_price, opp.category, &opp.token_id
        );

        ExecutionPlan {
            execution_id: ExecutionId::generate(),
            opportunity_id: opp.opportunity_id.clone(),
            market_id: opp.market_id.clone(),
            event_id: opp.event_id.clone(),
            token_id: opp.token_id.clone(),
            side: opp.side,
            shares,
            limit_price: opp.entry_price,
            estimated_cost: approved_size_usd,
            estimated_fee: fee,
            neg_risk: opp.meta.predicted_yes, // simplified: neg_risk lookup from registry
            reservation_id: reservation.id.clone(),
            detected_at: opp.detected_at,
            planned_at: Utc::now(),
        }
    }
}
```

### 5.4 Dispatcher

```rust
pub struct Dispatcher {
    clob_client: Arc<ClobClient>,
    timeout_config: TradeTimeoutConfig,
    metrics: Arc<MetricsHub>,
}

impl Dispatcher {
    /// Dry-run 模式: 仅记录，不发送任何请求
    pub async fn dry_run(&self, plan: &ExecutionPlan) -> ExecutionOutcome {
        tracing::info!(
            execution_id = %plan.execution_id,
            market_id = %plan.market_id,
            shares = %plan.shares,
            price = %plan.limit_price,
            "[DRY RUN] Would place order"
        );
        ExecutionOutcome::Filled {
            order_response: OrderResponse::simulated(plan),
            execution_mode: ExecutionMode::DryRun,
            latency_ms: 0,
        }
    }

    /// Paper 模式: 使用当前 book 模拟成交，不发送真实订单
    pub async fn paper_trade(&self, plan: &ExecutionPlan) -> ExecutionOutcome {
        // 模拟 FOK 逻辑：检查 book 是否有足够深度 fill
        // 如果能 fill → Filled, 否则 → Miss
        // 延迟模拟 10-50ms 随机
        // ...
        ExecutionOutcome::Filled {
            order_response: OrderResponse::simulated(plan),
            execution_mode: ExecutionMode::Paper,
            latency_ms: simulated_latency,
        }
    }

    /// Live 模式: 通过 TieredExecutionStrategy 执行真实订单
    pub async fn live_trade(
        &self,
        plan: &ExecutionPlan,
        tiered: &TieredExecutionStrategy,
    ) -> ExecutionOutcome {
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(self.timeout_config.dispatcher_timeout_ms);

        match tiered.execute(plan, &self.clob_client, deadline).await {
            Ok(response) => ExecutionOutcome::Filled {
                order_response: response,
                execution_mode: ExecutionMode::Live,
                latency_ms: plan.planned_at.elapsed_ms(),
            },
            Err(TradingError::Execution(msg)) if msg.contains("not filled") => {
                ExecutionOutcome::Miss {
                    reason: msg,
                    execution_mode: ExecutionMode::Live,
                }
            }
            Err(e) => ExecutionOutcome::Failed {
                error: e,
                execution_mode: ExecutionMode::Live,
            },
        }
    }
}
```

### 5.5 Runner

```rust
/// 异步执行循环 — 从 Funnel 输出通道接收机会, 逐个执行。
pub struct Runner {
    rx: flume::Receiver<ScoredOpportunity>,
    pipeline: Arc<ExecutionPipeline>,
    shutdown: CancellationToken,
    metrics: Arc<MetricsHub>,
}

impl Runner {
    pub async fn run(&self) -> Result<(), OxideError> {
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => {
                    tracing::info!("Execution runner shutting down");
                    return Ok(());
                }
                scored = self.rx.recv_async() => {
                    match scored {
                        Ok(s) => {
                            let result = self.pipeline.execute(s).await;
                            self.metrics.record_execution_result(&result);
                        }
                        Err(_) => {
                            tracing::warn!("Funnel channel closed");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}
```

### 5.6 CapitalManager

```rust
/// 资本管理器: 包装 ExposureReservationBackend, 添加业务逻辑。
pub struct CapitalManager {
    backend: Arc<dyn ExposureReservationBackend>,
    config: ExposureReservationConfig,
    wallet_balance_service: Arc<WalletBalanceService>,
    position_summary_service: Arc<PositionSummaryService>,
}

pub struct ReservationHandle {
    pub id: ReservationId,
    pub amount: Usd,
    pub market_id: MarketId,
}

impl CapitalManager {
    /// 尝试预留资本。
    /// 在调用 backend.try_reserve() 前, 先检查:
    /// 1. 钱包余额是否足够 (available = balance - reserved - reserve_balance)
    /// 2. 当前 market 是否已达到 per-market 上限
    pub async fn reserve(
        &self,
        market_id: &MarketId,
        amount: Usd,
    ) -> Result<ReservationHandle, ReservationError> {
        // 失败关闭: 如果 balance 查询失败, 拒绝预留
        let balance = self.wallet_balance_service.get_available().await
            .map_err(|e| ReservationError::Backend(format!("balance check failed: {e}")))?;

        let reserved = self.backend.total_reserved_usd().await;
        let available = balance.inner().saturating_sub(
            reserved.inner() + self.config.reserve_balance_usd()
        );

        if amount.inner() > available {
            return Err(ReservationError::ExceedsLimit {
                current_cents: (reserved.inner() * rust_decimal_macros::dec!(100)).to_u64().unwrap_or(0),
                requested_cents: (amount.inner() * rust_decimal_macros::dec!(100)).to_u64().unwrap_or(0),
                max_cents: self.config.max_total_exposure_cents,
            });
        }

        let id = self.backend.try_reserve(
            market_id, amount, self.config.default_ttl,
        ).await?;

        Ok(ReservationHandle {
            id,
            amount,
            market_id: market_id.clone(),
        })
    }

    pub async fn confirm(&self, id: &ReservationId) -> Result<(), ReservationError> {
        self.backend.confirm(id).await
    }

    pub async fn release(&self, id: &ReservationId) -> Result<(), ReservationError> {
        self.backend.release(id).await
    }
}
```

### 5.7 执行结果类型

```rust
pub struct ExecutionPlan {
    pub execution_id: ExecutionId,
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub side: Side,
    pub shares: Shares,
    pub limit_price: Price,
    pub estimated_cost: Usd,
    pub estimated_fee: Usd,
    pub neg_risk: bool,
    pub reservation_id: ReservationId,
    pub detected_at: DateTime<Utc>,
    pub planned_at: DateTime<Utc>,
}

pub enum ExecutionOutcome {
    Filled {
        order_response: OrderResponse,
        execution_mode: ExecutionMode,
        latency_ms: u64,
    },
    Miss {
        reason: String,
        execution_mode: ExecutionMode,
    },
    Failed {
        error: TradingError,
        execution_mode: ExecutionMode,
    },
}

pub struct ExecutionResult {
    pub outcome: Option<ExecutionOutcome>,
    pub rejection_reason: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
}

impl ExecutionResult {
    pub fn completed(outcome: ExecutionOutcome) -> Self;
    pub fn rejected(stage: &str, reason: impl std::fmt::Display) -> Self;
    pub fn risk_denied(decision: RiskDecision) -> Self;

    pub fn is_success(&self) -> bool;
    pub fn is_miss(&self) -> bool;
    pub fn is_rejected(&self) -> bool;
}
```

---

## 6. 执行状态机 (ExecutionFSM)

### 6.1 ExecState 枚举

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExecState {
    /// 空闲 — 等待新机会
    Idle,
    /// 验证中 — 检查新鲜度、risk pre-check、sizing
    Validate,
    /// 执行中 — 下单、等待确认
    Exec,
    /// 紧急状态 — 系统级故障 (API 不可达、DB 断连、L4 breaker)
    /// 只能从外部信号进入; 只能手动或自动恢复后回到 Idle
    Emergency,
}

impl std::fmt::Display for ExecState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => f.write_str("idle"),
            Self::Validate => f.write_str("validate"),
            Self::Exec => f.write_str("exec"),
            Self::Emergency => f.write_str("emergency"),
        }
    }
}
```

### 6.2 有效转换 (5 条边)

```
           ┌──────────────────────────────────────────┐
           │                                          │
           ▼                                          │
         IDLE ──────(1)──────► VALIDATE               │
           ▲                      │                   │
           │                     (2)                  │
           │                      ▼                   │
           │                    EXEC                  │
           │                      │                   │
          (3)                    (3)                  │
           │                      │                   │
           └──────────────────────┘                   │
                                                      │
         任意状态 ──────(4)──────► EMERGENCY           │
                                    │                 │
                                   (5)                │
                                    └─────────────────┘
```

| 编号 | from | to | 触发条件 |
|------|------|----|----------|
| 1 | Idle | Validate | Funnel 发送 ScoredOpportunity |
| 2 | Validate | Exec | 验证 + risk check 通过 |
| 3 | Validate/Exec | Idle | 任务完成 / 验证失败 / 执行完成 |
| 4 | Any | Emergency | 系统级故障 / L4 circuit breaker trip |
| 5 | Emergency | Idle | 系统恢复 + 手动确认 |

### 6.3 实现

```rust
pub struct ExecutionFSM {
    state: parking_lot::RwLock<ExecState>,
    metrics: Arc<MetricsHub>,
}

impl ExecutionFSM {
    pub fn new(metrics: Arc<MetricsHub>) -> Self {
        Self {
            state: parking_lot::RwLock::new(ExecState::Idle),
            metrics,
        }
    }

    /// 尝试状态转换。如果转换无效，log error 并返回 Err。
    /// **不 panic** — 这是金融系统, 非法转换不应导致进程崩溃。
    pub fn transition(&self, target: ExecState) -> Result<(), FsmError> {
        let mut state = self.state.write();
        let current = *state;

        if Self::is_valid_transition(current, target) {
            tracing::debug!(from = %current, to = %target, "FSM transition");
            *state = target;
            self.metrics.fsm_transitions.with_label_values(&[
                current.as_str(), target.as_str()
            ]).inc();
            Ok(())
        } else {
            tracing::error!(from = %current, to = %target, "Invalid FSM transition attempted");
            self.metrics.fsm_invalid_transitions.inc();
            Err(FsmError::InvalidTransition { from: current, to: target })
        }
    }

    /// 强制进入 Emergency 状态（不检查当前状态）
    pub fn enter_emergency(&self, reason: &str) {
        let mut state = self.state.write();
        let prev = *state;
        *state = ExecState::Emergency;
        tracing::error!(from = %prev, reason = reason, "FSM forced to Emergency");
        self.metrics.fsm_emergency_entries.inc();
    }

    pub fn current(&self) -> ExecState {
        *self.state.read()
    }

    pub fn is_idle(&self) -> bool {
        self.current() == ExecState::Idle
    }

    fn is_valid_transition(from: ExecState, to: ExecState) -> bool {
        matches!(
            (from, to),
            (ExecState::Idle, ExecState::Validate)
                | (ExecState::Validate, ExecState::Exec)
                | (ExecState::Validate, ExecState::Idle)
                | (ExecState::Exec, ExecState::Idle)
                | (ExecState::Emergency, ExecState::Idle)
        )
    }
}

impl ExecState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Validate => "validate",
            Self::Exec => "exec",
            Self::Emergency => "emergency",
        }
    }
}

#[derive(Debug)]
pub struct FsmError {
    pub kind: FsmErrorKind,
}

#[derive(Debug)]
pub enum FsmErrorKind {
    InvalidTransition { from: ExecState, to: ExecState },
}
```

**设计要点**:
- `parking_lot::RwLock` 而非 `std::sync::RwLock`：不会 poison on panic，对金融系统更安全。
- 无效转换 → `log error` + 返回 `Err`，**绝不 panic**。调用方根据 `Err` 中止当前操作即可。
- Emergency 入口是独立方法 `enter_emergency()`，绕过正常验证 — 任何状态都可以进入 Emergency。

---

## 7. FOK + GTD 分层执行

### 7.1 TieredExecutionStrategy 设计

```rust
/// 三层执行策略: FOK → GTD(30s) → GTD(5min)
///
/// 设计原理:
/// - FOK (Fill-or-Kill): 最快，立即成交或取消。适合流动性充足的市场。
/// - GTD short (30s): 允许短暂等待，用于微小 slippage 时给予时间成交。
/// - GTD long (5min): 最后手段，容忍更大价格偏差以确保成交。
///
/// 每一层可以有 max_retries_per_tier 次重试（默认 1 次）。
/// 价格在每层累积 price_tolerance_ticks 个 tick 的让步。
pub struct TieredExecutionStrategy {
    config: TieredExecutionConfig,
    metrics: Arc<MetricsHub>,
}

/// 单次下单尝试的结果
enum TierAttemptResult {
    Filled(OrderResponse),
    NotFilled { reason: String },
    Error(TradingError),
}

impl TieredExecutionStrategy {
    pub fn new(config: TieredExecutionConfig, metrics: Arc<MetricsHub>) -> Self {
        Self { config, metrics }
    }

    /// 执行完整的三层策略
    pub async fn execute(
        &self,
        plan: &ExecutionPlan,
        clob: &ClobClient,
        deadline: tokio::time::Instant,
    ) -> Result<OrderResponse, TradingError> {
        let tick_size = plan.tick_size();

        // ── Tier 1: FOK ─────────────────────────────────────────
        let fok_price = plan.limit_price;
        for attempt in 0..=self.config.max_retries_per_tier {
            if tokio::time::Instant::now() >= deadline {
                return Err(TradingError::Execution("deadline exceeded".into()));
            }

            match self.attempt_fok(plan, clob, fok_price).await {
                TierAttemptResult::Filled(resp) => {
                    self.metrics.tier_fills.with_label_values(&["fok"]).inc();
                    return Ok(resp);
                }
                TierAttemptResult::NotFilled { reason } => {
                    tracing::debug!(attempt, reason, "FOK not filled, retrying");
                    self.metrics.tier_misses.with_label_values(&["fok"]).inc();
                }
                TierAttemptResult::Error(e) if !e.is_retryable() => return Err(e),
                TierAttemptResult::Error(_) => {} // retryable, continue
            }
        }

        // ── Tier 2: GTD short (30s) ─────────────────────────────
        let gtd_short_price = self.adjust_price(
            fok_price, plan.side, tick_size, self.config.price_tolerance_ticks
        );
        for attempt in 0..=self.config.max_retries_per_tier {
            if tokio::time::Instant::now() >= deadline {
                return Err(TradingError::Execution("deadline exceeded".into()));
            }

            match self.attempt_gtd(
                plan, clob, gtd_short_price,
                self.config.gtd_short_expiry_secs,
            ).await {
                TierAttemptResult::Filled(resp) => {
                    self.metrics.tier_fills.with_label_values(&["gtd_short"]).inc();
                    return Ok(resp);
                }
                TierAttemptResult::NotFilled { .. } => {
                    self.metrics.tier_misses.with_label_values(&["gtd_short"]).inc();
                }
                TierAttemptResult::Error(e) if !e.is_retryable() => return Err(e),
                TierAttemptResult::Error(_) => {}
            }
        }

        // ── Tier 3: GTD long (5min) ─────────────────────────────
        let gtd_long_price = self.adjust_price(
            gtd_short_price, plan.side, tick_size, self.config.price_tolerance_ticks
        );
        for attempt in 0..=self.config.max_retries_per_tier {
            if tokio::time::Instant::now() >= deadline {
                return Err(TradingError::Execution("deadline exceeded".into()));
            }

            match self.attempt_gtd(
                plan, clob, gtd_long_price,
                self.config.gtd_long_expiry_secs,
            ).await {
                TierAttemptResult::Filled(resp) => {
                    self.metrics.tier_fills.with_label_values(&["gtd_long"]).inc();
                    return Ok(resp);
                }
                TierAttemptResult::NotFilled { .. } => {
                    self.metrics.tier_misses.with_label_values(&["gtd_long"]).inc();
                }
                TierAttemptResult::Error(e) => return Err(e),
            }
        }

        Err(TradingError::Execution("all tiers exhausted".into()))
    }

    /// 价格调整: Buy → 提高价格(愿意多付), Sell → 降低价格(愿意少收)
    fn adjust_price(
        &self,
        base: Price,
        side: Side,
        tick_size: Decimal,
        ticks: i32,
    ) -> Price {
        let adjustment = tick_size * Decimal::from(ticks);
        match side {
            Side::Buy => Price::new(base.inner() + adjustment),
            Side::Sell => Price::new((base.inner() - adjustment).max(Decimal::ZERO)),
        }
    }

    async fn attempt_fok(
        &self,
        plan: &ExecutionPlan,
        clob: &ClobClient,
        price: Price,
    ) -> TierAttemptResult {
        let req = OrderRequest {
            market_id: plan.market_id.clone(),
            token_id: plan.token_id.clone(),
            side: plan.side,
            shares: plan.shares,
            price,
            order_type: OrderType::Fok,
            neg_risk: plan.neg_risk,
        };

        match tokio::time::timeout(
            Duration::from_millis(self.config.fok_timeout_ms),
            clob.place_order(&req),
        ).await {
            Ok(Ok(resp)) if resp.status == OrderStatus::Filled => {
                TierAttemptResult::Filled(resp)
            }
            Ok(Ok(resp)) => TierAttemptResult::NotFilled {
                reason: format!("FOK status: {}", resp.status),
            },
            Ok(Err(e)) => TierAttemptResult::Error(TradingError::Execution(e.to_string())),
            Err(_) => TierAttemptResult::NotFilled {
                reason: "FOK timeout".into(),
            },
        }
    }

    async fn attempt_gtd(
        &self,
        plan: &ExecutionPlan,
        clob: &ClobClient,
        price: Price,
        expiry_secs: u64,
    ) -> TierAttemptResult {
        let expiration = (Utc::now().timestamp() as u64) + expiry_secs;
        let req = OrderRequest {
            market_id: plan.market_id.clone(),
            token_id: plan.token_id.clone(),
            side: plan.side,
            shares: plan.shares,
            price,
            order_type: OrderType::Gtd { expiration },
            neg_risk: plan.neg_risk,
        };

        match clob.place_order(&req).await {
            Ok(resp) if resp.status == OrderStatus::Filled => {
                TierAttemptResult::Filled(resp)
            }
            Ok(resp) if resp.status == OrderStatus::Open => {
                // 订单在 book 上挂着, 等待成交或过期
                self.poll_until_terminal(clob, &resp.order_id, expiry_secs).await
            }
            Ok(resp) => TierAttemptResult::NotFilled {
                reason: format!("GTD immediate status: {}", resp.status),
            },
            Err(e) => TierAttemptResult::Error(TradingError::Execution(e.to_string())),
        }
    }

    /// 轮询 GTD 订单直到终态 (Filled / Expired / Cancelled)
    async fn poll_until_terminal(
        &self,
        clob: &ClobClient,
        order_id: &OrderId,
        max_wait_secs: u64,
    ) -> TierAttemptResult {
        let deadline = tokio::time::Instant::now()
            + Duration::from_secs(max_wait_secs + 5); // buffer

        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            if tokio::time::Instant::now() >= deadline {
                // 超时 → 取消订单
                let _ = clob.cancel_order(order_id).await;
                return TierAttemptResult::NotFilled {
                    reason: "GTD poll timeout".into(),
                };
            }
            // TODO: poll order status via ClobClient
        }
    }
}
```

### 7.2 DryRun/Paper/Live 模式处理

| 模式 | FOK 行为 | GTD 行为 | 副作用 |
|------|----------|----------|--------|
| **DryRun** | 立即返回 `Filled(simulated)` | 跳过 | 仅写日志 + ClickHouse audit |
| **Paper** | 模拟 book 撮合 | 模拟 time-weighted fill | 写 PG trade record (标记 `paper`) |
| **Live** | 真实 CLOB `place_order` | 真实 CLOB + poll | 写 PG trade + position + lifecycle |

**在 ExecutionPipeline 中的分支逻辑**:

```rust
let outcome = match self.execution_mode {
    ExecutionMode::DryRun => self.dispatcher.dry_run(&plan).await,
    ExecutionMode::Paper => self.dispatcher.paper_trade(&plan).await,
    ExecutionMode::Live => self.dispatcher.live_trade(&plan, &self.tiered_strategy).await,
};
```

DryRun 和 Paper 模式跳过 `TieredExecutionStrategy`，直接在 `Dispatcher` 内部模拟结果。

---

## 8. 基础设施 (Infrastructure)

### 8.1 AsyncWriter\<T\>

```rust
/// 通用异步批量写入器。
///
/// 接收方通过 mpsc channel 提交待写入项, 后台 worker 按批量
/// (达到 batch_size 或 flush_interval 超时) 批量写入目标。
///
/// 用途:
/// - AsyncWriter<NewTrade> → TradeRepository::create_batch()
/// - AsyncWriter<NewLifecycleEvent> → LifecycleRepository batch insert
/// - AsyncWriter<OpportunityAuditRow> → ChWriteManager::insert()
pub struct AsyncWriter<T: Send + 'static> {
    tx: flume::Sender<T>,
    name: String,
}

struct AsyncWriterWorker<T, F>
where
    T: Send + 'static,
    F: Fn(Vec<T>) -> Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>>,
{
    rx: flume::Receiver<T>,
    flush_fn: F,
    batch_size: usize,
    flush_interval: Duration,
    buffer: Vec<T>,
    name: String,
    metrics: Arc<MetricsHub>,
    shutdown: CancellationToken,
}

impl<T: Send + 'static> AsyncWriter<T> {
    pub fn new<F>(
        name: impl Into<String>,
        batch_size: usize,
        flush_interval: Duration,
        flush_fn: F,
        metrics: Arc<MetricsHub>,
        shutdown: CancellationToken,
    ) -> (Self, impl Future<Output = Result<(), OxideError>>)
    where
        F: Fn(Vec<T>) -> Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>> + Send + 'static,
    {
        let (tx, rx) = flume::bounded(4096);
        let name = name.into();
        let writer = Self { tx, name: name.clone() };
        let worker = AsyncWriterWorker {
            rx, flush_fn, batch_size, flush_interval,
            buffer: Vec::with_capacity(batch_size),
            name, metrics, shutdown,
        };
        (writer, worker.run())
    }

    pub fn write(&self, item: T) {
        if let Err(e) = self.tx.try_send(item) {
            tracing::warn!(writer = %self.name, "AsyncWriter channel full, dropping item");
        }
    }
}
```

### 8.2 DebouncedWriter

```rust
/// 合并高频写入 — 只保留最新值并按间隔写入。
///
/// 用途: RiskState 持久化。Risk engine 每秒可能更新多次内部状态,
/// 但只需要每 60s 持久化一次最新快照到 PG。
pub struct DebouncedWriter<T: Clone + Send + 'static> {
    latest: Arc<parking_lot::Mutex<Option<T>>>,
    name: String,
}

impl<T: Clone + Send + 'static> DebouncedWriter<T> {
    pub fn new<F>(
        name: impl Into<String>,
        interval: Duration,
        write_fn: F,
        shutdown: CancellationToken,
    ) -> (Self, impl Future<Output = Result<(), OxideError>>)
    where
        F: Fn(T) -> Pin<Box<dyn Future<Output = Result<(), OxideError>> + Send>> + Send + 'static,
    {
        let latest = Arc::new(parking_lot::Mutex::new(None));
        let writer = Self { latest: latest.clone(), name: name.into() };
        let worker = async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => {
                        // 关停前 flush 最后一次
                        if let Some(val) = latest.lock().take() {
                            write_fn(val).await.ok();
                        }
                        return Ok(());
                    }
                    _ = interval_timer.tick() => {
                        if let Some(val) = latest.lock().take() {
                            if let Err(e) = write_fn(val).await {
                                tracing::warn!("DebouncedWriter flush failed: {e}");
                            }
                        }
                    }
                }
            }
        };
        (writer, worker)
    }

    pub fn update(&self, value: T) {
        *self.latest.lock() = Some(value);
    }
}
```

### 8.3 PeriodicTask

```rust
/// 定时任务包装器: interval + 随机 jitter + shutdown 感知。
///
/// jitter 防止所有定时任务同时触发造成瞬间负载尖峰。
pub struct PeriodicTask;

impl PeriodicTask {
    pub async fn run<F, Fut>(
        name: &str,
        interval: Duration,
        jitter_pct: f64,  // 0.0 .. 1.0, 例如 0.1 = ±10%
        shutdown: CancellationToken,
        task_fn: F,
    ) -> Result<(), OxideError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<(), OxideError>>,
    {
        let mut timer = tokio::time::interval(interval);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return Ok(()),
                _ = timer.tick() => {
                    // 随机 jitter
                    if jitter_pct > 0.0 {
                        let jitter_ms = (interval.as_millis() as f64 * jitter_pct * rand()) as u64;
                        tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                    }
                    if let Err(e) = task_fn().await {
                        tracing::warn!(task = name, error = %e, "Periodic task failed");
                    }
                }
            }
        }
    }
}
```

### 8.4 HealthChecker

```rust
/// 多子系统健康探针。
pub struct HealthChecker {
    pg_pool: Arc<PostgresPool>,
    ch_pool: Arc<ClickHousePool>,
    cache: Arc<TieredCache>,
    ws_manager: Arc<ClobWsManager>,
    clob_client: Arc<ClobClient>,
    fsm: Arc<ExecutionFSM>,
    alerts: Arc<AlertDispatcher>,
    metrics: Arc<MetricsHub>,
}

impl HealthChecker {
    /// 执行所有探针并返回 HealthReport
    pub async fn check_all(&self) -> HealthReport {
        let checks = futures_util::future::join_all(vec![
            self.check_postgres(),
            self.check_clickhouse(),
            self.check_redis(),
            self.check_ws(),
            self.check_clob_api(),
        ]).await;

        let overall = checks.iter().all(|c| c.healthy);
        let report = HealthReport {
            overall_healthy: overall,
            checks,
            checked_at: Utc::now(),
        };

        if !overall {
            self.alerts.dispatch(Alert {
                severity: AlertSeverity::Critical,
                title: "Health check failure".into(),
                body: format!("{:?}", report.checks.iter()
                    .filter(|c| !c.healthy)
                    .map(|c| &c.name)
                    .collect::<Vec<_>>()),
                timestamp: Utc::now(),
            }).await;
        }

        report
    }

    async fn check_postgres(&self) -> SubsystemHealth {
        let start = Instant::now();
        match self.pg_pool.health_check().await {
            Ok(()) => SubsystemHealth {
                name: "postgres".into(),
                healthy: true,
                latency_ms: Some(start.elapsed().as_millis() as u64),
                detail: None,
            },
            Err(e) => SubsystemHealth {
                name: "postgres".into(),
                healthy: false,
                latency_ms: Some(start.elapsed().as_millis() as u64),
                detail: Some(e.to_string()),
            },
        }
    }

    // check_clickhouse(), check_redis(), check_ws(), check_clob_api() 同理
}
```

### 8.5 RetryPolicy

```rust
/// 统一重试策略: 指数退避 + circuit breaker 集成。
pub struct RetryPolicy {
    max_retries: u32,
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
}

impl RetryPolicy {
    pub fn new(max_retries: u32, initial_delay: Duration, max_delay: Duration) -> Self {
        Self {
            max_retries,
            initial_delay,
            max_delay,
            multiplier: 2.0,
        }
    }

    /// 使用 backoff crate 执行带重试的异步操作
    pub async fn execute<F, Fut, T, E>(
        &self,
        operation: F,
    ) -> Result<T, E>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let backoff_config = backoff::ExponentialBackoff {
            initial_interval: self.initial_delay,
            max_interval: self.max_delay,
            max_elapsed_time: Some(self.max_delay * self.max_retries),
            multiplier: self.multiplier,
            ..Default::default()
        };

        backoff::future::retry(backoff_config, || async {
            operation().await.map_err(backoff::Error::transient)
        }).await
    }
}
```

### 8.6 OracleHealthTracker

```rust
/// Oracle 健康评估 — 基于 300s 滑动窗口的三态模型。
///
/// 从老代码库移植。用于跟踪 Gamma API、CTF RPC、UMA 等
/// 外部数据源的健康状态，为 risk engine 提供数据质量信号。
///
/// 三态:
/// - Healthy: 窗口内成功率 > 90%
/// - Degraded: 窗口内成功率 50% ~ 90%
/// - Down: 窗口内成功率 < 50% 或连续 5 次失败
pub struct OracleHealthTracker {
    sources: DashMap<String, SourceHealthWindow>,
}

pub struct SourceHealthWindow {
    /// 环形缓冲区: (timestamp, success)
    samples: VecDeque<(Instant, bool)>,
    window: Duration,
    consecutive_failures: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceHealth {
    Healthy,
    Degraded,
    Down,
}

impl OracleHealthTracker {
    pub fn new() -> Self {
        Self { sources: DashMap::new() }
    }

    /// 记录一次调用结果
    pub fn record(&self, source_id: &str, success: bool) {
        self.sources
            .entry(source_id.to_owned())
            .or_insert_with(|| SourceHealthWindow::new(Duration::from_secs(300)))
            .record(success);
    }

    /// 查询 source 健康状态
    pub fn health(&self, source_id: &str) -> SourceHealth {
        self.sources
            .get(source_id)
            .map_or(SourceHealth::Healthy, |w| w.evaluate())
    }

    /// 所有 source 是否至少 Degraded 以上
    pub fn all_healthy_or_degraded(&self) -> bool {
        self.sources.iter().all(|e| e.evaluate() != SourceHealth::Down)
    }
}

impl SourceHealthWindow {
    fn new(window: Duration) -> Self {
        Self {
            samples: VecDeque::new(),
            window,
            consecutive_failures: 0,
        }
    }

    fn record(&mut self, success: bool) {
        let now = Instant::now();
        self.samples.push_back((now, success));
        self.prune(now);

        if success {
            self.consecutive_failures = 0;
        } else {
            self.consecutive_failures += 1;
        }
    }

    fn prune(&mut self, now: Instant) {
        while let Some(&(ts, _)) = self.samples.front() {
            if now.duration_since(ts) > self.window {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    fn evaluate(&self) -> SourceHealth {
        if self.consecutive_failures >= 5 {
            return SourceHealth::Down;
        }

        if self.samples.is_empty() {
            return SourceHealth::Healthy;
        }

        let success_count = self.samples.iter().filter(|(_, s)| *s).count();
        let rate = success_count as f64 / self.samples.len() as f64;

        if rate > 0.9 {
            SourceHealth::Healthy
        } else if rate >= 0.5 {
            SourceHealth::Degraded
        } else {
            SourceHealth::Down
        }
    }
}
```

---

## 9. 可观测性 (Observability)

### 9.1 MetricsHub

```rust
/// 所有 Prometheus 指标的中心注册表。
///
/// 命名规约: `oxide_arb_{subsystem}_{metric_name}_{unit}`
/// 例如: `oxide_arb_pipeline_book_snapshots_total`
pub struct MetricsHub {
    pub registry: prometheus::Registry,

    // ── Data Pipeline ─────────────────────────────────────────────
    pub ws_events_received: IntCounter,
    pub book_snapshots_applied: IntCounter,
    pub price_changes_applied: IntCounter,
    pub ws_events_ignored: IntCounter,
    pub markets_resolved_ws: IntCounter,
    pub shard_status_changes: IntCounter,
    pub book_store_token_count: IntGauge,

    // ── Detection ─────────────────────────────────────────────────
    pub scans_gate_rejected: IntCounter,
    pub coalesced_scans: IntCounter,
    pub scan_results_total: Histogram,
    pub scan_duration_seconds: Histogram,
    pub opportunities_detected: IntCounter,
    pub opportunities_cooldown_suppressed: IntCounter,

    // ── Funnel ────────────────────────────────────────────────────
    pub funnel_enqueued: IntCounter,
    pub funnel_dispatched: IntCounter,
    pub funnel_dropped: IntCounter,
    pub funnel_dispatch_age_ms: Histogram,
    pub funnel_queue_depth: IntGauge,

    // ── Execution ─────────────────────────────────────────────────
    pub execution_latency: Histogram,
    pub trades_filled: IntCounter,
    pub trades_missed: IntCounter,
    pub trades_failed: IntCounter,
    pub risk_denials: IntCounter,
    pub validation_failures: IntCounter,
    pub sizing_zero: IntCounter,
    pub reservation_failures: IntCounter,

    // ── Tiered execution ──────────────────────────────────────────
    pub tier_fills: IntCounterVec,   // labels: ["fok", "gtd_short", "gtd_long"]
    pub tier_misses: IntCounterVec,  // labels: ["fok", "gtd_short", "gtd_long"]

    // ── FSM ───────────────────────────────────────────────────────
    pub fsm_transitions: IntCounterVec,  // labels: [from, to]
    pub fsm_invalid_transitions: IntCounter,
    pub fsm_emergency_entries: IntCounter,
    pub fsm_current_state: IntGaugeVec, // labels: [state], value: 1 if current

    // ── Risk ──────────────────────────────────────────────────────
    pub risk_checks_total: IntCounter,
    pub risk_breaker_state: IntGaugeVec, // labels: [state_name]
    pub risk_exposure_usd: Gauge,
    pub risk_daily_pnl_usd: Gauge,
    pub risk_daily_loss_usd: Gauge,
    pub risk_weekly_loss_usd: Gauge,
    pub risk_consecutive_misses: IntGauge,
    pub risk_reservations_active: IntGauge,
    pub risk_reservations_total_usd: Gauge,

    // ── Calibration ───────────────────────────────────────────────
    pub calibration_update_total: IntCounter,
    pub calibration_resolved: IntCounter,
    pub calibration_gamma_miss: IntCounter,
    pub calibration_bucket_count: IntGauge,

    // ── Cache ─────────────────────────────────────────────────────
    pub cache_hits: IntCounterVec,    // labels: [domain]
    pub cache_misses: IntCounterVec,  // labels: [domain]
    pub cache_invalidations: IntCounterVec, // labels: [domain]

    // ── System ────────────────────────────────────────────────────
    pub uptime_seconds: IntGauge,
    pub active_tasks: IntGauge,
    pub async_writer_pending: IntGaugeVec, // labels: [writer_name]
    pub async_writer_flushes: IntCounterVec, // labels: [writer_name]
    pub health_check_failures: IntCounter,
    pub outbox_pending: IntGauge,
    pub outbox_flushed: IntCounter,
    pub outbox_dead_letters: IntCounter,
}

impl MetricsHub {
    pub fn new() -> Self {
        let registry = prometheus::Registry::new();
        // 注册所有指标...
        Self { registry, /* ... */ }
    }

    pub fn record_execution_result(&self, result: &ExecutionResult) {
        match result.outcome.as_ref() {
            Some(ExecutionOutcome::Filled { .. }) => self.trades_filled.inc(),
            Some(ExecutionOutcome::Miss { .. }) => self.trades_missed.inc(),
            Some(ExecutionOutcome::Failed { .. }) => self.trades_failed.inc(),
            None => {} // rejected before dispatch
        }
    }
}
```

### 9.2 AlertDispatcher

```rust
/// 告警分发器: 支持 Telegram 和 Webhook 两个通道。
///
/// 告警有冷却期 (默认 300s) 防止相同告警刷屏。
pub struct AlertDispatcher {
    telegram: Option<TelegramChannel>,
    webhook: Option<WebhookChannel>,
    cooldown: DashMap<String, Instant>,
    cooldown_duration: Duration,
    metrics: Arc<MetricsHub>,
}

pub struct Alert {
    pub severity: AlertSeverity,
    pub title: String,
    pub body: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
    Emergency,
}

impl AlertDispatcher {
    pub fn new(config: &NotificationConfig, metrics: Arc<MetricsHub>) -> Self {
        let telegram = if config.telegram.enabled {
            Some(TelegramChannel::new(&config.telegram))
        } else {
            None
        };
        let webhook = if config.webhook.enabled {
            Some(WebhookChannel::new(&config.webhook))
        } else {
            None
        };
        Self {
            telegram,
            webhook,
            cooldown: DashMap::new(),
            cooldown_duration: Duration::from_secs(config.alerts_cooldown_secs()),
            metrics,
        }
    }

    /// 分发告警。冷却期内的重复告警被静默丢弃。
    pub async fn dispatch(&self, alert: Alert) {
        let key = format!("{}:{}", alert.severity as u8, &alert.title);
        let now = Instant::now();

        if let Some(last) = self.cooldown.get(&key) {
            if now.duration_since(*last) < self.cooldown_duration {
                return; // 冷却期内, 静默
            }
        }
        self.cooldown.insert(key, now);

        // 路由规则:
        // Emergency/Critical → Telegram + Webhook
        // Warning → Webhook only
        // Info → 仅日志
        match alert.severity {
            AlertSeverity::Emergency | AlertSeverity::Critical => {
                if let Some(tg) = &self.telegram {
                    tg.send(&alert).await;
                }
                if let Some(wh) = &self.webhook {
                    wh.send(&alert).await;
                }
            }
            AlertSeverity::Warning => {
                if let Some(wh) = &self.webhook {
                    wh.send(&alert).await;
                }
            }
            AlertSeverity::Info => {
                tracing::info!(title = %alert.title, body = %alert.body, "Alert (info)");
            }
        }
    }
}

struct TelegramChannel {
    bot: teloxide::Bot,
    chat_id: teloxide::types::ChatId,
}

impl TelegramChannel {
    fn new(config: &TelegramConfig) -> Self {
        let bot = teloxide::Bot::new(&config.bot_token);
        let chat_id = teloxide::types::ChatId(config.chat_id.parse().unwrap_or(0));
        Self { bot, chat_id }
    }

    async fn send(&self, alert: &Alert) {
        let emoji = match alert.severity {
            AlertSeverity::Emergency => "🚨",
            AlertSeverity::Critical => "⚠️",
            AlertSeverity::Warning => "⚡",
            AlertSeverity::Info => "ℹ️",
        };
        let text = format!(
            "{emoji} *{title}*\n\n{body}\n\n_{time}_",
            title = alert.title,
            body = alert.body,
            time = alert.timestamp.format("%Y-%m-%d %H:%M:%S UTC"),
        );

        if let Err(e) = self.bot.send_message(self.chat_id, &text)
            .parse_mode(teloxide::types::ParseMode::MarkdownV2)
            .await
        {
            tracing::error!(error = %e, "Failed to send Telegram alert");
        }
    }
}

struct WebhookChannel {
    client: reqwest::Client,
    url: String,
}

impl WebhookChannel {
    fn new(config: &WebhookConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: config.url.clone(),
        }
    }

    async fn send(&self, alert: &Alert) {
        let payload = serde_json::json!({
            "severity": format!("{:?}", alert.severity),
            "title": alert.title,
            "body": alert.body,
            "timestamp": alert.timestamp.to_rfc3339(),
        });

        if let Err(e) = self.client.post(&self.url)
            .json(&payload)
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            tracing::error!(error = %e, "Failed to send webhook alert");
        }
    }
}
```

**与 AlertLevel::Emergency 的集成**: 当 circuit breaker 达到 L4 (System) 级别时, `RiskEngine` 通过 `CoreRiskMetrics` 触发:

```rust
// 在 CoreRiskMetrics (§14) 中:
async fn on_breaker_trip(&self, level: CircuitBreakerLevel) {
    if level == CircuitBreakerLevel::System {
        self.fsm.enter_emergency("L4 circuit breaker tripped");
        self.alerts.dispatch(Alert {
            severity: AlertSeverity::Emergency,
            title: "Circuit Breaker L4 — System Halt".into(),
            body: "All trading suspended due to system-level fault".into(),
            timestamp: Utc::now(),
        }).await;
    }
}
```

### 9.3 ReportGenerator

```rust
/// 每日/每周报告生成器。
///
/// - 每日报告: UTC 00:05 触发, 统计前一天 trade/PnL/fee
/// - 每周报告: 每周一 UTC 00:10 触发, 统计前一周汇总
pub struct ReportGenerator {
    trade_repo: Arc<dyn TradeRepository>,
    accounting_repo: Arc<dyn AccountingRepository>,
    position_repo: Arc<dyn PositionRepository>,
    report_repo: Arc<dyn ReportRepository>,
    alerts: Arc<AlertDispatcher>,
}

impl ReportGenerator {
    pub async fn generate_daily(&self) -> Result<(), OxideError> {
        let yesterday = Utc::now().date_naive() - chrono::Duration::days(1);
        let trades = self.trade_repo.find_recent(1000).await?;
        let daily_trades: Vec<_> = trades.iter()
            .filter(|t| t.created_at.date_naive() == yesterday)
            .collect();

        let report = DailyReport {
            date: yesterday,
            total_pnl: daily_trades.iter().map(|t| t.net_profit_usd).sum(),
            total_fees_paid: daily_trades.iter().map(|t| t.fee_usd).sum(),
            total_gas_paid: Usd::ZERO,
            trade_count: daily_trades.len() as u32,
            success_count: daily_trades.iter().filter(|t| t.outcome == TradeOutcome::Success).count() as u32,
            miss_count: daily_trades.iter().filter(|t| t.outcome == TradeOutcome::Miss).count() as u32,
            largest_single_loss: daily_trades.iter().map(|t| t.net_profit_usd).min().unwrap_or(Usd::ZERO),
            largest_single_profit: daily_trades.iter().map(|t| t.net_profit_usd).max().unwrap_or(Usd::ZERO),
        };

        self.report_repo.save_daily(&report).await?;

        self.alerts.dispatch(Alert {
            severity: AlertSeverity::Info,
            title: format!("Daily Report {yesterday}"),
            body: format!(
                "PnL: ${}, Trades: {}/{} filled, Fees: ${}",
                report.total_pnl, report.success_count, report.trade_count, report.total_fees_paid
            ),
            timestamp: Utc::now(),
        }).await;

        Ok(())
    }

    pub async fn generate_weekly(&self) -> Result<(), OxideError> {
        // 类似 daily, 聚合过去 7 天
        // ...
        Ok(())
    }
}
```

---

## 10. Cache Owner Services

### 10.1 FeeParamsService

```rust
/// CacheKey::FeeParams 的所有者 — 提供按 MarketCategory 的费率参数。
///
/// 读穿: cache miss → FeeCalculator 查询 → 写入 cache → 返回。
/// 过期: 600s L2 TTL, 150s L1 TTL。
pub struct FeeParamsService {
    cache: Arc<TieredCache>,
    fee_calculator: Arc<FeeCalculator>,
}

/// 缓存的费率参数 DTO
#[derive(Debug, Clone, Serialize, Deserialize, bitcode::Encode, bitcode::Decode)]
pub struct CachedFeeParams {
    pub category: MarketCategory,
    pub maker_rate: Decimal,
    pub taker_rate: Decimal,
    pub exponent: Decimal,
    pub cached_at: DateTime<Utc>,
}

impl FeeParamsService {
    pub fn new(cache: Arc<TieredCache>, fee_calculator: Arc<FeeCalculator>) -> Self {
        Self { cache, fee_calculator }
    }

    pub async fn get(&self, category: MarketCategory) -> Result<CachedFeeParams, OxideError> {
        let key = CacheKey::FeeParams { category };

        // 先查 cache
        if let Some(cached) = self.cache.get::<CachedFeeParams>(&key).await? {
            return Ok(cached);
        }

        // cache miss → 从 FeeCalculator 获取
        let params = CachedFeeParams {
            category,
            maker_rate: self.fee_calculator.rate_for_category(category),
            taker_rate: self.fee_calculator.rate_for_category(category),
            exponent: Decimal::ONE,
            cached_at: Utc::now(),
        };

        self.cache.set(&key, &params).await?;
        Ok(params)
    }
}
```

### 10.2 PositionSummaryService

```rust
/// CacheKey::PositionSummary 的所有者 — 提供按 MarketId 的持仓汇总。
///
/// 读穿: cache miss → PositionRepository::find_by_market() → 聚合 → 缓存。
/// 过期: 30s L2 TTL, 7.5s L1 TTL（持仓数据需要较高时效性）。
/// 失败关闭: 如果 DB 查询失败, 返回 Err 而非默认空值。
pub struct PositionSummaryService {
    cache: Arc<TieredCache>,
    position_repo: Arc<dyn PositionRepository>,
}

/// 持仓汇总 DTO
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSummary {
    pub market_id: MarketId,
    pub open_positions: Vec<PositionInfo>,
    pub total_exposure_usd: Usd,
    pub total_unrealized_pnl: Usd,
    pub position_count: usize,
    pub summarized_at: DateTime<Utc>,
}

impl PositionSummaryService {
    pub async fn get(&self, market_id: &MarketId) -> Result<PositionSummary, OxideError> {
        let key = CacheKey::PositionSummary { market_id: market_id.clone() };

        if let Some(cached) = self.cache.get_json::<PositionSummary>(&key).await? {
            return Ok(cached);
        }

        // cache miss → DB read
        let positions = self.position_repo.find_by_market(market_id).await?;
        let summary = PositionSummary {
            market_id: market_id.clone(),
            total_exposure_usd: positions.iter().map(|p| p.total_cost_usd).sum(),
            total_unrealized_pnl: positions.iter().map(|p| p.unrealized_pnl).sum(),
            position_count: positions.len(),
            open_positions: positions.into_iter().map(PositionInfo::from).collect(),
            summarized_at: Utc::now(),
        };

        self.cache.set_json(&key, &summary).await?;
        Ok(summary)
    }

    /// 交易完成后主动失效
    pub async fn invalidate(&self, market_id: &MarketId) -> Result<(), OxideError> {
        self.cache.invalidate(&CacheKey::PositionSummary {
            market_id: market_id.clone()
        }).await
    }
}
```

### 10.3 WalletBalanceService

```rust
/// CacheKey::Balance 的所有者 — Polymarket 钱包可用余额。
///
/// available_balance = collateral_balance() - total_reserved_usd()
///
/// 读穿: cache miss → ClobClient::collateral_balance() → 减去预留 → 缓存。
/// 过期: 15s L2 TTL, 3.75s L1 TTL（余额变化频繁）。
/// 失败关闭: 余额查询失败 → 返回 Err → 调用方阻止交易。
pub struct WalletBalanceService {
    cache: Arc<TieredCache>,
    clob_client: Arc<ClobClient>,
    exposure_backend: Arc<dyn ExposureReservationBackend>,
}

/// 钱包余额快照 DTO
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletBalanceSnapshot {
    pub raw_balance: Usd,
    pub reserved: Usd,
    pub available: Usd,
    pub queried_at: DateTime<Utc>,
}

impl WalletBalanceService {
    pub async fn get_snapshot(&self) -> Result<WalletBalanceSnapshot, OxideError> {
        let key = CacheKey::Balance;

        if let Some(cached) = self.cache.get_json::<WalletBalanceSnapshot>(&key).await? {
            return Ok(cached);
        }

        let raw = self.clob_client.collateral_balance().await?;
        let reserved = self.exposure_backend.total_reserved_usd().await;
        let available = Usd::new((raw.inner() - reserved.inner()).max(Decimal::ZERO));

        let snapshot = WalletBalanceSnapshot {
            raw_balance: raw,
            reserved,
            available,
            queried_at: Utc::now(),
        };

        self.cache.set_json(&key, &snapshot).await?;
        Ok(snapshot)
    }

    /// 返回可用余额 (失败 → Err, 不返回默认值)
    pub async fn get_available(&self) -> Result<Usd, OxideError> {
        Ok(self.get_snapshot().await?.available)
    }

    /// 交易完成后主动失效
    pub async fn invalidate(&self) -> Result<(), OxideError> {
        self.cache.invalidate(&CacheKey::Balance).await
    }
}
```

### 10.4 CacheInvalidationCoordinator

```rust
/// 缓存失效协调器 — 在交易生命周期的关键节点触发相关 cache 失效。
pub struct CacheInvalidationCoordinator {
    position_summary: Arc<PositionSummaryService>,
    wallet_balance: Arc<WalletBalanceService>,
    cache: Arc<TieredCache>,
}

impl CacheInvalidationCoordinator {
    /// 交易成交后: 失效 position summary + balance
    pub async fn on_trade_filled(&self, market_id: &MarketId) {
        let _ = tokio::join!(
            self.position_summary.invalidate(market_id),
            self.wallet_balance.invalidate(),
            self.cache.invalidate(&CacheKey::RiskState),
        );
    }

    /// 交易失败/miss: 仅失效 balance (reservation 已释放)
    pub async fn on_trade_missed(&self) {
        let _ = self.wallet_balance.invalidate().await;
    }

    /// 市场结算: 失效 position + balance
    pub async fn on_market_settled(&self, market_id: &MarketId) {
        let _ = tokio::join!(
            self.position_summary.invalidate(market_id),
            self.wallet_balance.invalidate(),
        );
    }
}
```

---

## 11. API 层补充

### 11.1 `ClobClient::collateral_balance()`

```rust
// 在 oxide-arb-api/src/clob/mod.rs 中添加:

impl ClobClient {
    /// 查询当前可用 USDC.e 抵押品余额。
    ///
    /// 调用 Polymarket CLOB REST API: GET /balance
    /// 返回 Usd 类型 (6 decimal precision)。
    ///
    /// 错误处理:
    /// - HTTP 429 → ApiError::RateLimited (可重试)
    /// - HTTP 5xx → ApiError::Clob (可重试)
    /// - HTTP 4xx → ApiError::Clob (不可重试)
    /// - 网络超时 → ApiError::Timeout
    pub async fn collateral_balance(&self) -> Result<Usd, ApiError> {
        let url = format!("{}/balance", self.base_url);
        let response = self.client
            .get(&url)
            .headers(self.auth_headers()?)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| ApiError::Http(e.to_string()))?;

        if !response.status().is_success() {
            return Err(ApiError::Clob(format!(
                "balance query failed: HTTP {}",
                response.status()
            )));
        }

        #[derive(Deserialize)]
        struct BalanceResponse {
            #[serde(deserialize_with = "deserialize_decimal_string")]
            balance: Decimal,
        }

        let body: BalanceResponse = response.json().await
            .map_err(|e| ApiError::Deserialize(e.to_string()))?;

        Ok(Usd::new(body.balance))
    }
}
```

### 11.2 WS Manager 增强: last-message-age 跟踪

```rust
// 在 oxide-arb-api/src/ws/mod.rs 中添加:

impl ClobWsManager {
    /// 距离最近一次收到 WS 消息的时间（毫秒）。
    /// 用于 HealthChecker 判断 WS 连接是否仍然活跃。
    ///
    /// 如果从未收到消息, 返回 None。
    pub fn last_message_age_ms(&self) -> Option<u64> {
        self.last_message_at.load(Ordering::Relaxed)
            .map(|ts| {
                let now = Instant::now();
                now.duration_since(ts).as_millis() as u64
            })
    }
}
```

需要在 `ClobWsManager` 内部添加:

```rust
/// 在收到每条 WS 消息时更新:
last_message_at: Arc<AtomicCell<Option<Instant>>>,
```

HealthChecker 使用该 API 判断 WS 是否健康:

```rust
async fn check_ws(&self) -> SubsystemHealth {
    match self.ws_manager.last_message_age_ms() {
        Some(age_ms) if age_ms < self.ws_disconnect_threshold_ms => {
            SubsystemHealth { name: "websocket".into(), healthy: true, latency_ms: Some(age_ms), detail: None }
        }
        Some(age_ms) => {
            SubsystemHealth { name: "websocket".into(), healthy: false, latency_ms: Some(age_ms),
                detail: Some(format!("no message for {age_ms}ms")) }
        }
        None => {
            SubsystemHealth { name: "websocket".into(), healthy: false, latency_ms: None,
                detail: Some("never connected".into()) }
        }
    }
}
```

---

## 12. Outbox EventStore + Flusher

### 12.1 EventStore trait

```rust
/// 生命周期事件持久化 trait — 支持 outbox 模式的可靠投递。
///
/// 所有生命周期事件先写入 PG outbox 表，然后由 OutboxFlusher
/// 异步消费并转发到下游（ClickHouse audit, Telegram, webhook）。
#[async_trait::async_trait]
pub trait EventStore: Send + Sync + 'static {
    /// 写入新的生命周期事件到 outbox
    async fn append(&self, event: &NewLifecycleEvent) -> Result<i64, OxideError>;

    /// 批量写入
    async fn append_batch(&self, events: &[NewLifecycleEvent]) -> Result<Vec<i64>, OxideError>;

    /// 获取待处理事件 (FOR UPDATE SKIP LOCKED)
    async fn fetch_pending(&self, limit: usize) -> Result<Vec<OutboxEvent>, OxideError>;

    /// 标记事件为已处理
    async fn mark_processed(&self, ids: &[i64]) -> Result<(), OxideError>;

    /// 标记事件为死信 (超过最大重试次数)
    async fn mark_dead_letter(&self, id: i64, reason: &str) -> Result<(), OxideError>;

    /// 死信事件数量
    async fn dead_letter_count(&self) -> Result<u64, OxideError>;
}
```

### 12.2 NewLifecycleEvent

```rust
pub struct NewLifecycleEvent {
    pub phase: LifecyclePhase,
    pub stage: Option<String>,
    pub message: String,
    pub metadata: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

impl NewLifecycleEvent {
    pub fn detected(opp: &Opportunity) -> Self {
        Self {
            phase: LifecyclePhase::Detected,
            stage: Some("scanner".into()),
            message: format!("Opportunity detected: {} @ {}", opp.market_id, opp.entry_price),
            metadata: Some(serde_json::to_value(opp).unwrap_or_default()),
            created_at: Utc::now(),
        }
    }

    pub fn rejected(opp: &Opportunity, reason: &impl std::fmt::Display) -> Self {
        Self {
            phase: LifecyclePhase::Rejected,
            stage: Some("execution".into()),
            message: format!("Rejected {}: {}", opp.market_id, reason),
            metadata: None,
            created_at: Utc::now(),
        }
    }

    pub fn risk_denied(opp: &Opportunity, decision: &RiskDecision) -> Self {
        Self {
            phase: LifecyclePhase::RiskChecked,
            stage: Some("risk_engine".into()),
            message: format!("Risk denied: {}", decision.denial_reason.as_deref().unwrap_or("unknown")),
            metadata: Some(serde_json::to_value(decision).unwrap_or_default()),
            created_at: Utc::now(),
        }
    }

    pub fn from_outcome(opp: &Opportunity, outcome: &ExecutionOutcome) -> Self {
        match outcome {
            ExecutionOutcome::Filled { .. } => Self {
                phase: LifecyclePhase::FilledFull,
                stage: Some("dispatcher".into()),
                message: format!("Filled: {}", opp.market_id),
                metadata: None,
                created_at: Utc::now(),
            },
            ExecutionOutcome::Miss { reason, .. } => Self {
                phase: LifecyclePhase::Rejected,
                stage: Some("dispatcher".into()),
                message: format!("Miss: {reason}"),
                metadata: None,
                created_at: Utc::now(),
            },
            ExecutionOutcome::Failed { error, .. } => Self {
                phase: LifecyclePhase::Rejected,
                stage: Some("dispatcher".into()),
                message: format!("Failed: {error}"),
                metadata: None,
                created_at: Utc::now(),
            },
        }
    }
}
```

### 12.3 OutboxFlusher

```rust
/// Outbox 消费者 — 使用 FOR UPDATE SKIP LOCKED 确保单实例安全消费。
///
/// 设计:
/// 1. 每 5s tick 一次
/// 2. SELECT ... FOR UPDATE SKIP LOCKED 取最多 100 条待处理事件
/// 3. 对每条事件调用注册的 OutboxConsumer
/// 4. 成功 → mark_processed
/// 5. 连续失败 3 次 → mark_dead_letter
pub struct OutboxFlusher {
    event_store: Arc<dyn EventStore>,
    consumers: Vec<Arc<dyn OutboxConsumer>>,
    batch_size: usize,
    max_retries: u32,
    metrics: Arc<MetricsHub>,
    shutdown: CancellationToken,
}

impl OutboxFlusher {
    pub async fn run(&self) -> Result<(), OxideError> {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.cancelled() => {
                    self.flush_once().await?;
                    return Ok(());
                }
                _ = interval.tick() => {
                    if let Err(e) = self.flush_once().await {
                        tracing::warn!(error = %e, "Outbox flush failed");
                    }
                }
            }
        }
    }

    async fn flush_once(&self) -> Result<(), OxideError> {
        let events = self.event_store.fetch_pending(self.batch_size).await?;
        if events.is_empty() {
            return Ok(());
        }

        let mut processed = Vec::new();

        for event in &events {
            let mut all_ok = true;
            for consumer in &self.consumers {
                if let Err(e) = consumer.consume(event).await {
                    tracing::warn!(
                        event_id = event.id,
                        consumer = consumer.name(),
                        error = %e,
                        "Consumer failed"
                    );
                    all_ok = false;
                }
            }

            if all_ok {
                processed.push(event.id);
            } else if event.retry_count >= self.max_retries {
                self.event_store.mark_dead_letter(
                    event.id,
                    "max retries exceeded",
                ).await?;
                self.metrics.outbox_dead_letters.inc();
            }
        }

        if !processed.is_empty() {
            self.event_store.mark_processed(&processed).await?;
            self.metrics.outbox_flushed.inc_by(processed.len() as u64);
        }

        self.metrics.outbox_pending.set(
            self.event_store.dead_letter_count().await.unwrap_or(0) as i64
        );

        Ok(())
    }
}
```

### 12.4 OutboxConsumer trait

```rust
#[async_trait::async_trait]
pub trait OutboxConsumer: Send + Sync + 'static {
    fn name(&self) -> &str;
    async fn consume(&self, event: &OutboxEvent) -> Result<(), OxideError>;
}

pub struct OutboxEvent {
    pub id: i64,
    pub phase: LifecyclePhase,
    pub stage: Option<String>,
    pub message: String,
    pub metadata: Option<serde_json::Value>,
    pub retry_count: u32,
    pub created_at: DateTime<Utc>,
}
```

**死信策略**: 超过 `max_retries` (默认 3) 的事件被标记为 dead_letter。dead_letter 不再被 `fetch_pending` 返回。通过 `dead_letter_count()` 指标监控, 如果积压 > 10 → 触发 Warning alert。运维人员可通过 DB 直接查看和重试。

---

## 13. Exposure Reservation InMemory 实现

```rust
/// 纯内存实现的 ExposureReservationBackend。
///
/// 使用 DashMap + AtomicU64 CAS loop 确保 try_reserve 的原子性。
/// 适用于单进程部署（Phase 4 的唯一场景）。
///
/// 关键不变量:
/// - total_reserved_cents (AtomicU64) == reservations DashMap 中所有活跃条目的 amount_cents 之和
/// - CAS loop 保证: 只有成功更新 total_reserved_cents 的线程才会插入 reservation 条目
/// - GC 任务每 30s 扫描过期条目并释放
pub struct InMemoryExposureReservation {
    reservations: DashMap<ReservationId, ReservationEntry>,
    total_reserved_cents: AtomicU64,
    per_market_cents: DashMap<MarketId, AtomicU64>,
    config: ExposureReservationConfig,
}

struct ReservationEntry {
    market_id: MarketId,
    amount_cents: u64,
    expires_at: Instant,
    created_at: Instant,
}

#[async_trait::async_trait]
impl ExposureReservationBackend for InMemoryExposureReservation {
    async fn try_reserve(
        &self,
        market_id: &MarketId,
        amount: Usd,
        ttl: Duration,
    ) -> Result<ReservationId, ReservationError> {
        let amount_cents = (amount.inner() * rust_decimal_macros::dec!(100))
            .to_u64()
            .ok_or_else(|| ReservationError::Backend("amount overflow".into()))?;

        // CAS loop for global limit
        loop {
            let current = self.total_reserved_cents.load(Ordering::Acquire);
            let new_total = current + amount_cents;

            if new_total > self.config.max_total_exposure_cents {
                return Err(ReservationError::ExceedsLimit {
                    current_cents: current,
                    requested_cents: amount_cents,
                    max_cents: self.config.max_total_exposure_cents,
                });
            }

            if self.total_reserved_cents
                .compare_exchange_weak(current, new_total, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
            // CAS 失败 → 另一个线程同时 reserve, 重试
        }

        // Per-market limit check
        let market_total = self.per_market_cents
            .entry(market_id.clone())
            .or_insert_with(|| AtomicU64::new(0));
        let market_current = market_total.fetch_add(amount_cents, Ordering::AcqRel);
        if market_current + amount_cents > self.config.max_per_market_cents {
            // 回滚
            market_total.fetch_sub(amount_cents, Ordering::AcqRel);
            self.total_reserved_cents.fetch_sub(amount_cents, Ordering::AcqRel);
            return Err(ReservationError::ExceedsLimit {
                current_cents: market_current,
                requested_cents: amount_cents,
                max_cents: self.config.max_per_market_cents,
            });
        }

        let id = ReservationId::new_id();
        self.reservations.insert(id.clone(), ReservationEntry {
            market_id: market_id.clone(),
            amount_cents,
            expires_at: Instant::now() + ttl,
            created_at: Instant::now(),
        });

        Ok(id)
    }

    async fn confirm(&self, id: &ReservationId) -> Result<(), ReservationError> {
        let entry = self.reservations.remove(id)
            .ok_or_else(|| ReservationError::NotFound { id: id.to_string() })?;
        let (_, entry) = entry;
        self.total_reserved_cents.fetch_sub(entry.amount_cents, Ordering::AcqRel);
        if let Some(market_total) = self.per_market_cents.get(&entry.market_id) {
            market_total.fetch_sub(entry.amount_cents, Ordering::AcqRel);
        }
        Ok(())
    }

    async fn release(&self, id: &ReservationId) -> Result<(), ReservationError> {
        // release 和 confirm 逻辑相同: 移除条目, 减少计数
        self.confirm(id).await
    }

    async fn total_reserved_usd(&self) -> Usd {
        let cents = self.total_reserved_cents.load(Ordering::Acquire);
        Usd::new(Decimal::from(cents) / rust_decimal_macros::dec!(100))
    }

    async fn active_count(&self) -> usize {
        self.reservations.len()
    }
}

impl InMemoryExposureReservation {
    pub fn new(config: ExposureReservationConfig) -> Self {
        Self {
            reservations: DashMap::new(),
            total_reserved_cents: AtomicU64::new(0),
            per_market_cents: DashMap::new(),
            config,
        }
    }

    /// GC 任务: 清理过期 reservation。
    /// 应每 gc_interval (默认 30s) 调用一次。
    pub fn gc_expired(&self) -> u32 {
        let now = Instant::now();
        let mut expired_count = 0u32;

        self.reservations.retain(|_, entry| {
            if now >= entry.expires_at {
                self.total_reserved_cents.fetch_sub(entry.amount_cents, Ordering::AcqRel);
                if let Some(market_total) = self.per_market_cents.get(&entry.market_id) {
                    market_total.fetch_sub(entry.amount_cents, Ordering::AcqRel);
                }
                expired_count += 1;
                false // remove
            } else {
                true // keep
            }
        });

        expired_count
    }
}
```

---

## 14. RiskMetrics + RiskPersistence 桥接

### 14.1 CoreRiskMetrics

```rust
/// 核心层实现 RiskMetrics trait — 将 oxide-arb-risk 的查询桥接到实际服务。
///
/// RiskMetrics trait 由 oxide-arb-risk 定义, oxide-arb-core 实现。
/// 这是 DI 的核心: risk engine 不依赖 core, 但通过 trait 获取实时数据。
pub struct CoreRiskMetrics {
    wallet_balance_service: Arc<WalletBalanceService>,
    position_summary_service: Arc<PositionSummaryService>,
    exposure_backend: Arc<dyn ExposureReservationBackend>,
    ws_manager: Arc<ClobWsManager>,
    oracle_health_tracker: Arc<OracleHealthTracker>,
    fsm: Arc<ExecutionFSM>,
    alerts: Arc<AlertDispatcher>,
    metrics: Arc<MetricsHub>,
}

/// RiskMetrics trait (在 oxide-arb-risk 中定义):
///
/// ```rust
/// #[async_trait]
/// pub trait RiskMetrics: Send + Sync + 'static {
///     async fn current_balance(&self) -> Result<Usd, OxideError>;
///     async fn total_exposure(&self) -> Result<Usd, OxideError>;
///     async fn total_reserved(&self) -> Usd;
///     async fn open_position_count(&self) -> Result<usize, OxideError>;
///     async fn ws_last_message_age_ms(&self) -> Option<u64>;
///     async fn is_data_healthy(&self) -> bool;
///     async fn on_breaker_trip(&self, level: CircuitBreakerLevel);
///     async fn on_breaker_recover(&self);
/// }
/// ```

#[async_trait::async_trait]
impl RiskMetrics for CoreRiskMetrics {
    async fn current_balance(&self) -> Result<Usd, OxideError> {
        self.wallet_balance_service.get_available().await
    }

    async fn total_exposure(&self) -> Result<Usd, OxideError> {
        let positions = self.position_summary_service.get_all_open().await?;
        Ok(positions.iter().map(|p| p.total_exposure_usd).sum())
    }

    async fn total_reserved(&self) -> Usd {
        self.exposure_backend.total_reserved_usd().await
    }

    async fn open_position_count(&self) -> Result<usize, OxideError> {
        let positions = self.position_summary_service.get_all_open().await?;
        Ok(positions.iter().map(|p| p.position_count).sum())
    }

    async fn ws_last_message_age_ms(&self) -> Option<u64> {
        self.ws_manager.last_message_age_ms()
    }

    async fn is_data_healthy(&self) -> bool {
        self.oracle_health_tracker.all_healthy_or_degraded()
    }

    async fn on_breaker_trip(&self, level: CircuitBreakerLevel) {
        self.metrics.risk_breaker_state
            .with_label_values(&["open"])
            .set(1);

        if level == CircuitBreakerLevel::System {
            self.fsm.enter_emergency("L4 circuit breaker tripped");
            self.alerts.dispatch(Alert {
                severity: AlertSeverity::Emergency,
                title: "Circuit Breaker L4 — System Halt".into(),
                body: "All trading suspended due to system-level fault.".into(),
                timestamp: Utc::now(),
            }).await;
        } else if level >= CircuitBreakerLevel::Daily {
            self.alerts.dispatch(Alert {
                severity: AlertSeverity::Critical,
                title: format!("Circuit Breaker {} tripped", level),
                body: format!("Trading paused at level {level}"),
                timestamp: Utc::now(),
            }).await;
        }
    }

    async fn on_breaker_recover(&self) {
        self.metrics.risk_breaker_state
            .with_label_values(&["closed"])
            .set(1);

        if self.fsm.current() == ExecState::Emergency {
            self.fsm.transition(ExecState::Idle).ok();
        }
    }
}
```

### 14.2 CoreRiskPersistence

```rust
/// 核心层实现 RiskPersistence trait — 将 risk engine 的持久化需求桥接到 repository 层。
pub struct CoreRiskPersistence {
    risk_state_repo: Arc<dyn RiskStateRepository>,
    potential_loss_repo: Arc<dyn PotentialLossRepository>,
    trade_repo: Arc<dyn TradeRepository>,
    lifecycle_repo: Arc<dyn LifecycleRepository>,
    cache: Arc<TieredCache>,
}

/// RiskPersistence trait (在 oxide-arb-risk 中定义):
///
/// ```rust
/// #[async_trait]
/// pub trait RiskPersistence: Send + Sync + 'static {
///     async fn load_state(&self) -> Result<RiskEngineSnapshot, OxideError>;
///     async fn save_state(&self, snapshot: &RiskEngineSnapshot) -> Result<(), OxideError>;
///     async fn record_potential_loss(&self, entry: &PotentialLossEntry) -> Result<(), OxideError>;
///     async fn resolve_potential_loss(&self, entry_id: &str) -> Result<(), OxideError>;
///     async fn total_active_potential_loss(&self) -> Result<Usd, OxideError>;
///     async fn record_trade_outcome(&self, trade: &NewTrade) -> Result<(), OxideError>;
///     async fn recent_trades(&self, limit: usize) -> Result<Vec<TradeRecord>, OxideError>;
/// }
/// ```

#[async_trait::async_trait]
impl RiskPersistence for CoreRiskPersistence {
    async fn load_state(&self) -> Result<RiskEngineSnapshot, OxideError> {
        let model = self.risk_state_repo.load().await?;
        Ok(RiskEngineSnapshot {
            breaker_state: model.breaker_state,
            breaker_level: model.breaker_level,
            breaker_reason: model.halt_reason,
            cooling_until: model.cooldown_until,
            total_exposure: model.total_exposure,
            daily_pnl: model.daily_pnl,
            daily_loss: model.daily_loss_usd,
            weekly_loss: model.weekly_loss_usd,
            consecutive_misses: model.consecutive_misses as u32,
            l2_trip_count: 0, // 需要从 model 中新增字段读取
            snapshot_at: model.updated_at,
        })
    }

    async fn save_state(&self, snapshot: &RiskEngineSnapshot) -> Result<(), OxideError> {
        // 映射 snapshot → risk_state entity → repository.save()
        self.risk_state_repo.save(/* mapped model */).await?;
        self.cache.invalidate(&CacheKey::RiskState).await?;
        Ok(())
    }

    async fn record_potential_loss(&self, entry: &PotentialLossEntry) -> Result<(), OxideError> {
        self.potential_loss_repo.record(/* mapped */).await
    }

    async fn resolve_potential_loss(&self, entry_id: &str) -> Result<(), OxideError> {
        self.potential_loss_repo.resolve(entry_id).await
    }

    async fn total_active_potential_loss(&self) -> Result<Usd, OxideError> {
        self.potential_loss_repo.total_active_loss().await
    }

    async fn record_trade_outcome(&self, trade: &NewTrade) -> Result<(), OxideError> {
        self.trade_repo.create(trade.clone()).await.map(|_| ())
    }

    async fn recent_trades(&self, limit: usize) -> Result<Vec<TradeRecord>, OxideError> {
        // TradeRecord 需要从 entity::trade::Model 映射
        let models = self.trade_repo.find_recent(limit).await?;
        Ok(models.into_iter().map(TradeRecord::from).collect())
    }
}
```

---

## 15. CalibrationDataSource 桥接

### 15.1 CoreCalibrationDataSource

```rust
/// 核心层实现 CalibrationDataSource trait — 桥接 algorithm crate 的 calibration updater。
pub struct CoreCalibrationDataSource {
    calibration_repo: Arc<dyn CalibrationRepository>,
    gamma_client: Arc<GammaClient>,
    voting_oracle: Arc<VotingOracle>,
    oracle_health_tracker: Arc<OracleHealthTracker>,
}

#[async_trait::async_trait]
impl CalibrationDataSource for CoreCalibrationDataSource {
    async fn get_unresolved_outcomes(&self) -> Result<Vec<UnresolvedOutcome>, AlgoError> {
        let outcomes = self.calibration_repo.get_unresolved_outcomes().await
            .map_err(|e| AlgoError::DataSource(e.to_string()))?;

        Ok(outcomes.into_iter().map(|o| UnresolvedOutcome {
            outcome_id: o.id,
            market_id: MarketId::new(&o.market_id),
            bucket_key: BucketKey {
                category: o.category,
                price_zone: o.price_zone,
                duration_bucket: o.duration_bucket,
            },
            predicted_yes: o.predicted_yes,
        }).collect())
    }

    async fn check_gamma_resolution(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<bool>, AlgoError> {
        let result = self.gamma_client.get_resolution_status(market_id).await
            .map_err(|e| AlgoError::DataSource(e.to_string()))?;

        self.oracle_health_tracker.record("gamma", result.is_some());

        Ok(result.map(|r| r.winning_outcome == "Yes"))
    }

    async fn check_ctf_resolution(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<bool>, AlgoError> {
        match self.voting_oracle.resolve(market_id, market_id).await {
            Ok(ResolutionVerdict::Resolved { winning_yes, .. }) => {
                self.oracle_health_tracker.record("ctf", true);
                Ok(Some(winning_yes))
            }
            Ok(_) => {
                self.oracle_health_tracker.record("ctf", true);
                Ok(None)
            }
            Err(e) => {
                self.oracle_health_tracker.record("ctf", false);
                Err(AlgoError::DataSource(e.to_string()))
            }
        }
    }

    async fn save_buckets(&self, entries: &[CalibrationEntry]) -> Result<(), AlgoError> {
        for entry in entries {
            self.calibration_repo.update_bucket(/* mapped */).await
                .map_err(|e| AlgoError::DataSource(e.to_string()))?;
        }
        Ok(())
    }

    async fn resolve_outcome(&self, outcome_id: i64, actual_yes: bool) -> Result<(), AlgoError> {
        self.calibration_repo.resolve_outcome(outcome_id, actual_yes).await
            .map_err(|e| AlgoError::DataSource(e.to_string()))
    }
}
```

### 15.2 CalibrationUpdater 定期 tick 接入

在 `AppContext::run()` 中注册定时任务:

```rust
let calibration_updater = self.calibration_updater.clone();
self.task_registry.spawn("calibration_updater", async move {
    PeriodicTask::run(
        "calibration_updater",
        Duration::from_secs(settings.inner().detection.calibration.refresh_interval_secs),
        0.1, // 10% jitter
        shutdown.clone(),
        || async {
            match calibration_updater.tick().await {
                Ok(stats) => {
                    tracing::info!(
                        total = stats.total_unresolved,
                        resolved = stats.resolved,
                        gamma_miss = stats.gamma_miss,
                        "Calibration update completed"
                    );
                    Ok(())
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Calibration update failed");
                    Err(OxideError::Algorithm(e))
                }
            }
        },
    ).await
});
```

---

## 16. 测试策略

### 16.1 集成测试矩阵

| 测试类别 | 文件 | 依赖 | 说明 |
|----------|------|------|------|
| Data Pipeline E2E | `tests/pipeline_e2e.rs` | mock WS | 完整 WS→BookStore→Detection 流程 |
| Detection trigger | `tests/detection_trigger.rs` | mock BookStore | Coalescer + Scanner 联动 |
| Execution happy path | `tests/execution_happy.rs` | mock ClobClient | 完整 validate→size→dispatch→audit |
| Execution rejection | `tests/execution_reject.rs` | mock RiskEngine | risk denied + validation fail 路径 |
| FSM boundary | `tests/fsm_tests.rs` | 无外部依赖 | 所有合法/非法转换 |
| Tiered fallback | `tests/tiered_execution.rs` | mock ClobClient | FOK fail→GTD short→GTD long |
| Shutdown/drain | `tests/shutdown_drain.rs` | tokio test-util | 30s drain + inflight 处理 |
| Paper-trade E2E | `tests/paper_trade_e2e.rs` | testcontainers PG | 完整 Paper 模式端到端 |
| Exposure reservation | `tests/exposure_reservation.rs` | 无外部依赖 | CAS 并发正确性 |
| Outbox flusher | `tests/outbox_flusher.rs` | testcontainers PG | 可靠投递 + dead-letter |
| Cache services | `tests/cache_services.rs` | mock repos | 读穿 + 失效 |

### 16.2 Data Pipeline E2E 测试

```rust
#[tokio::test]
async fn test_ws_to_detection_full_flow() {
    // Setup:
    // 1. 创建 mock WS channel (flume::bounded)
    // 2. 构建 BookStore, MarketRegistry (注册一个测试市场)
    // 3. 构建 Scanner with real OpportunityPipeline
    // 4. 构建 Coalescer with test-friendly short window (50ms)
    // 5. 构建 Funnel with direct channel output

    // Act:
    // 1. 注入 WsEvent::BookSnapshot for YES token (price = 0.96)
    // 2. 注入 WsEvent::BookSnapshot for NO token (price = 0.04)
    // 3. 等待 Coalescer window + Scanner tick

    // Assert:
    // 1. BookStore 有两个 token 的 OrderBook
    // 2. Funnel output channel 收到 ScoredOpportunity
    // 3. opportunity.entry_price 合理
    // 4. 指标: book_snapshots_applied == 2, coalesced_scans == 1
}

#[tokio::test]
async fn test_stale_book_rejected_by_gate() {
    // 注入 timestamp 为 60s 前的 BookSnapshot
    // 验证 BookGate.pass() 返回 false
    // 验证 Scanner 不产出 ScoredOpportunity
}

#[tokio::test]
async fn test_crossed_book_rejected() {
    // 注入 bid = 0.97, ask = 0.96 的 crossed book
    // 验证 BookGate 返回 CrossedBook error
}
```

### 16.3 Execution 测试

```rust
#[tokio::test]
async fn test_execution_happy_path_live() {
    // Setup: mock ClobClient 返回 Filled
    // Act: ExecutionPipeline.execute(scored_opportunity)
    // Assert:
    //   - FSM 经历 Idle→Validate→Exec→Idle
    //   - trade_writer 收到 NewTrade
    //   - capital_manager.confirm() 被调用
    //   - risk_engine.record_trade_result() 被调用
}

#[tokio::test]
async fn test_execution_risk_denied() {
    // Setup: mock RiskEngine 返回 allowed=false
    // Act: ExecutionPipeline.execute(scored_opportunity)
    // Assert:
    //   - FSM 回到 Idle
    //   - reservation 已释放
    //   - lifecycle event phase == RiskChecked
}

#[tokio::test]
async fn test_execution_fok_miss_then_gtd_fill() {
    // Setup: mock ClobClient 第一次返回 Rejected, 第二次返回 Filled
    // Act: TieredExecutionStrategy.execute()
    // Assert:
    //   - tier_misses["fok"] == 1
    //   - tier_fills["gtd_short"] == 1
}

#[tokio::test]
async fn test_execution_all_tiers_exhausted() {
    // Setup: mock ClobClient 始终返回 Rejected
    // Act: TieredExecutionStrategy.execute()
    // Assert: Err("all tiers exhausted")
}

#[tokio::test]
async fn test_dry_run_no_side_effects() {
    // Setup: execution_mode = DryRun
    // Act: ExecutionPipeline.execute(scored_opportunity)
    // Assert:
    //   - ClobClient.place_order() NEVER called
    //   - trade_writer 收到 NewTrade (mode=DryRun)
    //   - outcome == Filled (simulated)
}
```

### 16.4 FSM 边界测试

```rust
#[test]
fn test_fsm_valid_transitions() {
    let fsm = ExecutionFSM::new(Arc::new(MetricsHub::new()));
    assert!(fsm.transition(ExecState::Validate).is_ok());
    assert!(fsm.transition(ExecState::Exec).is_ok());
    assert!(fsm.transition(ExecState::Idle).is_ok());
}

#[test]
fn test_fsm_invalid_idle_to_exec() {
    let fsm = ExecutionFSM::new(Arc::new(MetricsHub::new()));
    assert!(fsm.transition(ExecState::Exec).is_err());
}

#[test]
fn test_fsm_invalid_exec_to_validate() {
    let fsm = ExecutionFSM::new(Arc::new(MetricsHub::new()));
    fsm.transition(ExecState::Validate).unwrap();
    fsm.transition(ExecState::Exec).unwrap();
    assert!(fsm.transition(ExecState::Validate).is_err());
}

#[test]
fn test_fsm_emergency_from_any_state() {
    let fsm = ExecutionFSM::new(Arc::new(MetricsHub::new()));
    fsm.transition(ExecState::Validate).unwrap();
    fsm.enter_emergency("test");
    assert_eq!(fsm.current(), ExecState::Emergency);
}

#[test]
fn test_fsm_emergency_recover_to_idle() {
    let fsm = ExecutionFSM::new(Arc::new(MetricsHub::new()));
    fsm.enter_emergency("test");
    assert!(fsm.transition(ExecState::Idle).is_ok());
}

#[test]
fn test_fsm_emergency_cannot_validate() {
    let fsm = ExecutionFSM::new(Arc::new(MetricsHub::new()));
    fsm.enter_emergency("test");
    assert!(fsm.transition(ExecState::Validate).is_err());
}
```

### 16.5 Shutdown/Drain 测试

```rust
#[tokio::test]
async fn test_graceful_shutdown_30s_drain() {
    // 1. 启动 DataPipeline + Runner + 多个 PeriodicTask
    // 2. 触发 shutdown token
    // 3. 验证所有 task 在 30s 内完成
    // 4. 验证 AsyncWriter flush 被调用
    // 5. 验证 risk state 最终快照写入
}

#[tokio::test]
async fn test_shutdown_cancels_inflight_order() {
    // 1. 启动执行, mock ClobClient 延迟 10s
    // 2. 在 2s 后触发 shutdown
    // 3. 验证订单被取消
    // 4. 验证 reservation 被释放
}
```

### 16.6 Exposure Reservation 并发测试

```rust
#[tokio::test]
async fn test_concurrent_reservations_respect_limit() {
    let config = ExposureReservationConfig {
        max_total_exposure_cents: 100_00, // $100
        max_per_market_cents: 50_00,      // $50
        default_ttl: Duration::from_secs(60),
        gc_interval: Duration::from_secs(5),
    };
    let backend = InMemoryExposureReservation::new(config);

    let mut handles = Vec::new();
    for i in 0..20 {
        let backend = backend.clone(); // Arc'd
        handles.push(tokio::spawn(async move {
            let market = MarketId::new(&format!("market_{}", i % 3));
            backend.try_reserve(&market, Usd::new(dec!(10)), Duration::from_secs(60)).await
        }));
    }

    let results: Vec<_> = futures_util::future::join_all(handles).await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    let successes = results.iter().filter(|r| r.is_ok()).count();
    assert!(successes <= 10); // $100 / $10 = max 10
    assert_eq!(backend.total_reserved_usd().await, Usd::new(dec!(10) * Decimal::from(successes)));
}

#[tokio::test]
async fn test_gc_expires_old_reservations() {
    let backend = InMemoryExposureReservation::new(ExposureReservationConfig {
        default_ttl: Duration::from_millis(50),
        ..Default::default()
    });

    backend.try_reserve(&MarketId::new("m1"), Usd::new(dec!(10)), Duration::from_millis(50)).await.unwrap();
    assert_eq!(backend.active_count().await, 1);

    tokio::time::sleep(Duration::from_millis(100)).await;
    let expired = backend.gc_expired();
    assert_eq!(expired, 1);
    assert_eq!(backend.active_count().await, 0);
    assert_eq!(backend.total_reserved_usd().await, Usd::ZERO);
}
```

### 16.7 Benchmarks

```rust
// benches/pipeline_bench.rs

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_orderbook_apply_snapshot(c: &mut Criterion) {
    // 准备 50 level 的 bid/ask
    c.bench_function("orderbook_apply_snapshot_50", |b| {
        b.iter(|| {
            let mut book = OrderBook::new(TokenId::new("test"));
            book.apply_snapshot(bids_50.clone(), asks_50.clone(), 1000);
        });
    });
}

fn bench_orderbook_apply_delta(c: &mut Criterion) {
    // 准备已有 50 level 的 book + 5 个 delta
    c.bench_function("orderbook_apply_delta_5", |b| {
        b.iter(|| {
            let mut book = base_book.clone();
            book.apply_delta(&deltas_5, 1001);
        });
    });
}

fn bench_dual_book_assemble(c: &mut Criterion) {
    c.bench_function("dual_book_assemble", |b| {
        b.iter(|| {
            DualBookAssembler::assemble(&book_store, &yes_token, &no_token);
        });
    });
}

fn bench_opportunity_pipeline_process(c: &mut Criterion) {
    c.bench_function("pipeline_process_single", |b| {
        b.iter(|| {
            pipeline.process(
                &market_id, &event_id, &yes_token, &no_token,
                &snapshot, category, staleness, deadline, now,
            );
        });
    });
}

fn bench_risk_check(c: &mut Criterion) {
    c.bench_function("risk_pre_trade_check", |b| {
        b.iter(|| {
            // sync risk checks only (no async)
        });
    });
}

criterion_group!(
    benches,
    bench_orderbook_apply_snapshot,
    bench_orderbook_apply_delta,
    bench_dual_book_assemble,
    bench_opportunity_pipeline_process,
    bench_risk_check,
);
criterion_main!(benches);
```

**目标基准**:

| 操作 | 目标延迟 (p99) |
|------|----------------|
| OrderBook apply_snapshot (50 levels) | < 5 μs |
| OrderBook apply_delta (5 changes) | < 2 μs |
| DualBookAssembler::assemble | < 10 μs |
| OpportunityPipeline::process | < 100 μs |
| Risk pre-trade check (sync part) | < 50 μs |

---

## 17. 验收检查清单

### 编译与质量

- [ ] `cargo build --workspace` — zero warnings
- [ ] `cargo clippy --workspace -- -D warnings` — zero lints
- [ ] `cargo fmt --all --check` — formatted
- [ ] `cargo test --workspace` — all green
- [ ] `cargo doc --workspace --no-deps` — no broken links

### 功能验证

- [ ] **DryRun E2E**: 启动 → WS mock 数据 → 检测 → 评分 → "执行" (日志) → audit 写入
- [ ] **Paper E2E**: 启动 → WS mock → 检测 → 模拟成交 → PG trade record (mode=paper)
- [ ] **BookGate**: 空 book / 过期 book / crossed book → 拒绝进入 pipeline
- [ ] **Coalescer**: 连续两个 token 更新合并为一次 scan
- [ ] **Funnel**: 超过 max_queue_size → 低分机会被丢弃
- [ ] **FSM**: 所有 5 条合法边正常转换 + 非法边返回 Err 且不 panic
- [ ] **Tiered**: FOK miss → GTD short fill (mock 验证)
- [ ] **Risk denial**: RiskEngine 返回 allowed=false → 管线中止 + lifecycle event
- [ ] **Exposure reservation**: 并发 20 线程 reserve → 不超过上限
- [ ] **Exposure GC**: TTL 过期后 gc_expired() 清理 + 计数归零
- [ ] **Cache read-through**: PositionSummary cache miss → DB 查询 → 写入 cache → 下次 hit
- [ ] **Cache invalidation**: on_trade_filled → position + balance cache 失效
- [ ] **Balance fail-closed**: ClobClient::collateral_balance() 失败 → reserve 拒绝

### 可靠性验证

- [ ] **Graceful shutdown**: SIGTERM → 30s 内所有 task 退出 + writer flush
- [ ] **Crash recovery**: kill -9 → 重启 → risk_engine_state 从 PG 恢复 → breaker FSM 正确
- [ ] **Outbox reliability**: consumer 异常 → 重试 3 次 → dead-letter
- [ ] **WS disconnect**: last_message_age > threshold → HealthChecker 报告 unhealthy

### 可观测性验证

- [ ] **Prometheus**: `curl localhost:9090/metrics` 返回所有注册指标
- [ ] **指标正确性**: 执行一笔 paper trade → trades_filled +1, execution_latency 有值
- [ ] **Telegram alert**: L4 breaker trip → Telegram 群收到消息
- [ ] **Daily report**: 手动触发 generate_daily() → report 写入 PG + Telegram Info

### 性能验证

- [ ] **OrderBook apply_snapshot**: < 5 μs (50 levels)
- [ ] **Pipeline process**: < 100 μs (单市场)
- [ ] **Scan all (100 markets)**: < 50 ms
- [ ] **内存**: 运行 1000 市场 OrderBook < 50 MB RSS

---

## 18. 预估工作量

| 模块 | 预估工时 | 优先级 | 依赖 |
|------|----------|--------|------|
| AppContext + TaskRegistry + lifecycle | 8h | P0 | — |
| OrderBook + BookStore | 6h | P0 | — |
| MarketRegistry + MarketCache | 4h | P0 | — |
| DataPipeline + StalenessClassifier | 6h | P0 | BookStore |
| DualBookAssembler + BookGate | 3h | P0 | BookStore |
| Scanner | 4h | P0 | DataPipeline, algorithm |
| Coalescer + Funnel | 5h | P0 | Scanner |
| ExecutionFSM | 3h | P0 | — |
| Validator + PlanBuilder | 4h | P0 | BookStore, MarketRegistry |
| Dispatcher + TieredStrategy | 8h | P0 | ClobClient |
| ExecutionPipeline + Runner | 6h | P0 | 全部执行子模块 |
| CapitalManager + InMemory Exposure | 6h | P0 | — |
| CoreRiskMetrics + CoreRiskPersistence | 4h | P0 | risk crate |
| CoreCalibrationDataSource | 3h | P1 | algorithm crate |
| Cache services (Fee/Position/Balance) | 5h | P0 | repos, cache |
| CacheInvalidationCoordinator | 2h | P1 | cache services |
| ClobClient::collateral_balance() | 2h | P0 | — |
| WS last-message-age | 1h | P0 | — |
| AsyncWriter + DebouncedWriter | 4h | P1 | — |
| PeriodicTask | 1h | P1 | — |
| HealthChecker | 3h | P1 | — |
| OracleHealthTracker | 3h | P1 | — |
| RetryPolicy | 1h | P2 | — |
| MetricsHub | 4h | P0 | — |
| AlertDispatcher (Telegram + Webhook) | 4h | P1 | — |
| ReportGenerator | 3h | P2 | — |
| Outbox EventStore + Flusher | 6h | P1 | PG |
| 集成测试 | 12h | P0 | 全部模块 |
| Benchmarks | 3h | P2 | OrderBook, Pipeline |
| Binary crate (`oxide-arb`) | 4h | P1 | AppContext |

**总计**: ~133 工时 ≈ 17 工作日 (单人全时)

**关键路径**: AppContext → BookStore → DataPipeline → Scanner → ExecutionPipeline → 集成测试

**建议实施顺序**:

1. **Week 1** (P0 核心): AppContext, BookStore, DataPipeline, ExecutionFSM, Scanner, Exposure
2. **Week 2** (P0 执行): ExecutionPipeline, TieredStrategy, Validator, Cache Services, Metrics
3. **Week 3** (P1 基础设施): Bridge layers, Outbox, AsyncWriter, HealthChecker, AlertDispatcher
4. **Week 4** (P1-P2 + 测试): ReportGenerator, OracleHealthTracker, 全部集成测试, Benchmarks, Binary crate
