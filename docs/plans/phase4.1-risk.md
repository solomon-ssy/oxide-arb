# Phase 4.1 — `oxide-arb-risk` 生产级详细设计文档

> **状态**: Production Design Target  
> **作者**: oxide-arb team  
> **创建日期**: 2026-05-22  
> **前置依赖**: Phase 0 (error/macros), Phase 1 (models/api), Phase 2 (storage/repository), Phase 3 (algorithm)  
> **ADR 参考**: ADR-001 (单策略 endgame，单平台 Polymarket，无对冲)
> **运行拓扑**: 单活交易引擎（single-active execution engine），允许只读观测实例
> **一致性取向**: 资金安全优先；关键状态同步持久化；失败即 fail-closed

---

## 目录

0. [工作范围](#0-工作范围)
1. [Crate 架构](#1-crate-架构)
2. [Trait 边界 (DI)](#2-trait-边界-di)
3. [CircuitBreaker 状态机](#3-circuitbreaker-状态机)
4. [会计核算 (Accounting)](#4-会计核算-accounting)
5. [持仓追踪 (Position)](#5-持仓追踪-position)
6. [静态限制 + 敞口 (Limits)](#6-静态限制--敞口-limits)
7. [黑名单管理 (Blacklist)](#7-黑名单管理-blacklist)
8. [定量器 (Sizing)](#8-定量器-sizing)
9. [Endgame 专用规则](#9-endgame-专用规则)
10. [账本对账 (Reconciliation)](#10-账本对账-reconciliation)
11. [RiskEngine 门面](#11-riskengine-门面)
12. [测试策略](#12-测试策略)
13. [验收检查清单](#13-验收检查清单)
14. [预估工作量](#14-预估工作量)

---

## 0. 工作范围

### 0.1 交付物

Phase 4.1 创建一个**完全独立**的 `oxide-arb-risk` crate，不依赖 `oxide-arb-core`（尚不存在），通过 trait 注入与外部系统交互。交付内容：

| 组件 | 说明 |
|------|------|
| `CircuitBreaker` | 4-state FSM (Closed/Open/HalfOpen/Recovered)，4 级触发 (L1–L4) |
| `DailyAccounting` / `WeeklyAccounting` | 滚动窗口 PnL/损失/费用累计，UTC 午夜/周一自动翻转 |
| `PositionTracker` | 持仓价值跟踪，聚合 per-market 和 portfolio 级别敞口 |
| `PotentialLossLedger` | 未结算头寸最大潜在损失账本 |
| `StaticLimitChecker` | L1 级前置静态过滤 (depth, staleness, edge) |
| `ExposureLimitChecker` | 多维敞口限制 (per-market, portfolio, balance-based) |
| `BlacklistManager` | DB 权威 + `DashMap` 热路径投影，支持 TTL、scope 分级、自动拉黑、审计 |
| `QuarterKellyCalculator` | Fractional Kelly 上限计算，纳入概率置信度、fill probability、费用/失败成本折扣 |
| `MultiConstraintSizer` | 多约束风险预算器，Kelly 只是候选上限之一，最终受流动性/敞口/资金/回撤/交易所约束共同裁剪 |
| `DrawdownGuard` | HWM 追踪 + drawdown 保护 |
| `EndgameRiskRules` | Endgame 策略专用规则 (方向集中度、日方向预算) |
| `LedgerReconciler` | 内部账本 vs 链上/交易所余额对账 |
| `RiskPipeline` | 统一 check 管线，静态注册、确定顺序、可审计 trace、支持 short-circuit/full-report |
| `RiskEngine` | 门面类，编排状态恢复、check 管线、sizing、post-trade 状态机 |

### 0.2 依赖关系

```text
oxide-arb-risk
├── oxide-arb-error     (OxideError, OxideResult)
├── oxide-arb-models    (所有 domain types, config types, enums)
├── rust_decimal         (金额计算，禁止 f64)
├── chrono               (UTC 时间处理)
├── parking_lot          (RwLock, 低延迟同步)
├── dashmap              (BlacklistManager 并发 map)
├── tracing              (结构化日志)
├── async-trait          (dyn trait objects)
├── uuid                 (ReservationId 等)
└── serde                (snapshot 序列化)
```

**不依赖**: `oxide-arb-api`, `oxide-arb-storage`, `oxide-arb-repository`, `oxide-arb-algorithm`, `tokio`(运行时)。所有 I/O 通过 trait 注入。

### 0.3 验收标准

1. `cargo build -p oxide-arb-risk` 零警告编译
2. `cargo test -p oxide-arb-risk` 通过率 100%
3. `cargo clippy -p oxide-arb-risk -- -D warnings` 零 lint
4. CircuitBreaker FSM 覆盖全部 6 条边 (Closed→Open, Open→HalfOpen, HalfOpen→Recovered, HalfOpen→Open, Recovered→Closed, reset→Closed)
5. Kelly 计算结果与手工验算误差 < 0.01 USD
6. `MultiConstraintSizer` 在每种约束生效时正确识别 binding constraint
7. 会计翻转在 UTC 午夜 ±1ms 窗口内正确触发
8. 黑名单 TTL 过期后自动清除，不阻塞交易
9. Drawdown guard 在 HWM 回撤 > 阈值时正确降速/暂停
10. `RiskPipeline` 静态注册的 pre-trade checks 按序执行，short-circuit 模式第一个 hard fail 即返回，full-report 模式输出完整 trace
11. `on_trade_result()` 更新所有子系统且不 panic
12. 所有公共 API 签名有 `#[must_use]` 标注
13. 零 `f64` 用于金额路径
14. proptest 回归覆盖 Kelly 和 accounting 边界
15. 启动恢复必须从持久化权威源重建 breaker/accounting/position/potential-loss/blacklist/drawdown；任一关键状态恢复失败则拒绝交易
16. breaker、blacklist、accounting、position、potential-loss 的关键变更必须同步持久化并写审计记录；持久化失败时 fail-closed
17. `pre_trade_check()` 输出完整 `RiskDecisionTrace`，包含 check id、输入摘要、阈值、实际值、耗时、状态版本和失败原因
18. check 顺序由 `RiskPipeline` 静态注册，测试必须锁定顺序；禁止散落的 ad hoc `check_*` 被绕过
19. 生产 API 不做向前兼容 re-export；公开面必须显式、收敛、可审计

---

## 1. Crate 架构

### 1.1 目录结构

```
crates/oxide-arb-risk/
├── Cargo.toml
├── src/
│   ├── lib.rs                    # crate 根，声明公开模块；禁止兼容 re-export
│   ├── traits.rs                 # RiskMetrics, RiskPersistence, BalanceQuerier
│   ├── engine.rs                 # RiskEngine 门面
│   ├── builder.rs                # RiskEngineBuilder
│   ├── pipeline.rs               # RiskPipeline, RiskCheck trait, registry/order
│   ├── context.rs                # RiskContext, StateVersion, CheckInput snapshots
│   ├── state_store.rs            # in-memory projections + recovery invariants
│   ├── audit.rs                  # RiskAuditEvent, DecisionTrace, mutation log
│   ├── circuit_breaker.rs        # CircuitBreaker FSM + BreakerState
│   ├── accounting.rs             # DailyAccounting, WeeklyAccounting, PeriodStats
│   ├── position.rs               # PositionTracker, PotentialLossLedger
│   ├── limits.rs                 # StaticLimitChecker, ExposureLimitChecker
│   ├── blacklist.rs              # BlacklistManager
│   ├── sizing.rs                 # QuarterKellyCalculator, MultiConstraintSizer, DrawdownGuard
│   ├── endgame_rules.rs          # EndgameRiskRules
│   ├── reconciliation.rs         # LedgerReconciler, ReconciliationReport
│   └── types.rs                  # crate-local types (RiskDecision, RiskCheckResult, SizeResult, etc.)
└── tests/
    ├── circuit_breaker_tests.rs
    ├── accounting_tests.rs
    ├── sizing_tests.rs
    ├── blacklist_tests.rs
    ├── limits_tests.rs
    ├── endgame_rules_tests.rs
    ├── reconciliation_tests.rs
    ├── pipeline_tests.rs
    ├── recovery_tests.rs
    ├── audit_tests.rs
    ├── engine_tests.rs
    └── proptest_sizing.rs
```

### 1.2 Cargo.toml

```toml
[package]
name = "oxide-arb-risk"
description = "Risk engine: circuit breaker, position sizing, exposure limits, and blacklist management"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[dependencies]
oxide-arb-error = { workspace = true }
oxide-arb-models = { workspace = true }

rust_decimal = { workspace = true }
rust_decimal_macros = { workspace = true }
chrono = { workspace = true }
serde = { workspace = true }
tracing = { workspace = true }
async-trait = { workspace = true }
parking_lot = { workspace = true }
dashmap = { workspace = true }
uuid = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
rust_decimal_macros = { workspace = true }
proptest = "1"

[lints]
workspace = true
```

### 1.3 模块层次

```text
oxide_arb_risk
├── traits          (RiskMetrics, RiskPersistence, BalanceQuerier)
├── types           (RiskDecision, RiskCheckResult, SizeResult, ...)
├── context         (RiskContext, StateVersion, immutable check inputs)
├── pipeline        (RiskPipeline, RiskCheck, static registry)
├── state_store     (RecoveredState, in-memory projections)
├── audit           (RiskAuditEvent, DecisionTrace)
├── circuit_breaker (CircuitBreaker, BreakerState, BreakerLevel)
├── accounting      (DailyAccounting, WeeklyAccounting, PeriodStats)
├── position        (PositionTracker, PotentialLossLedger)
├── limits          (StaticLimitChecker, ExposureLimitChecker)
├── blacklist       (BlacklistManager)
├── sizing          (QuarterKellyCalculator, MultiConstraintSizer, DrawdownGuard)
├── endgame_rules   (EndgameRiskRules)
├── reconciliation  (LedgerReconciler, ReconciliationReport)
├── builder         (RiskEngineBuilder)
└── engine          (RiskEngine)
```

`lib.rs` 公开面策略：

```rust
pub mod traits;
pub mod types;
pub mod context;
pub mod pipeline;
pub mod state_store;
pub mod audit;
pub mod circuit_breaker;
pub mod accounting;
pub mod position;
pub mod limits;
pub mod blacklist;
pub mod sizing;
pub mod endgame_rules;
pub mod reconciliation;
pub mod builder;
pub mod engine;
```

**禁止 re-export 兼容层。** 调用方必须使用显式模块路径，例如
`oxide_arb_risk::engine::RiskEngine`、`oxide_arb_risk::builder::RiskEngineBuilder`。
Phase 4.1 是未发布新 crate，不为草稿 API 保留 alias、shim 或 re-export。

---

## 2. Trait 边界 (DI)

所有外部依赖通过三个 trait 注入。`oxide-arb-risk` 本身**不实现**这些 trait——实现方是 `oxide-arb-core`(Phase 5) 或测试 mock。

### 2.1 `RiskMetrics`

运行时查询只读指标的 trait。RiskEngine 通过它获取来自 repository/内存投影层的实时数据，不直接持有数据库连接。
这些方法位于 pre-trade 热路径，必须是无阻塞、无 I/O、带版本的新鲜快照读取；实现方不得在方法内部等待网络、数据库或远端缓存。

```rust
/// Read-only accessor for live system metrics required by risk checks.
///
/// Implementations must be `Send + Sync` and safe for concurrent access.
/// All monetary amounts use `Usd` (never `f64`). Methods are intentionally
/// synchronous — implementors should cache aggressively and never block
/// on I/O in these methods.
pub trait RiskMetrics: Send + Sync + 'static {
    /// Current total portfolio exposure across all open positions and
    /// pending reservations (USD).
    fn total_exposure(&self) -> Usd;

    /// Exposure in a single market (positions + reservations, USD).
    fn market_exposure(&self, market_id: &MarketId) -> Usd;

    /// Number of currently open positions.
    fn open_position_count(&self) -> usize;

    /// All open positions as a slice-like view.
    fn open_positions(&self) -> Vec<PositionInfo>;

    /// Last known platform balance (USDC.e on Polygon), cached.
    fn cached_balance(&self) -> Usd;

    /// Count of active exposure reservations.
    fn active_reservation_count(&self) -> usize;

    /// Total USD currently locked in pending reservations.
    fn reserved_usd(&self) -> Usd;

    /// Number of currently open positions in a given directional side
    /// across the entire portfolio. Used by EndgameRiskRules.
    fn open_directional_count(&self, side: Side) -> usize;

    /// Number of trades executed today in a given directional side.
    fn daily_directional_trades(&self, side: Side) -> u32;

    /// Count of consecutive misses for a specific market (for auto-blacklist).
    fn consecutive_market_misses(&self, market_id: &MarketId) -> u32;

    /// Seconds since last successful WebSocket heartbeat.
    fn ws_disconnect_secs(&self) -> u64;
}
```

### 2.2 `RiskPersistence`

异步持久化 trait，用于 crash recovery、状态突变和 audit trail。资金相关状态不接受 write-behind 作为成功语义：
调用方只有在持久化成功后，才能把 breaker/blacklist/accounting/position/potential-loss 变更视为 committed。

```rust
/// Async persistence interface for risk engine state.
///
/// Called by `RiskEngine` inside state transitions to ensure durability.
/// Critical mutations must be committed before this method returns `Ok`.
/// Returning `Err` means the engine must enter fail-closed mode.
#[async_trait::async_trait]
pub trait RiskPersistence: Send + Sync + 'static {
    /// Persist the full risk engine snapshot (crash recovery).
    async fn save_snapshot(&self, snapshot: &RiskEngineSnapshot) -> OxideResult<()>;

    /// Load the most recent snapshot (startup recovery).
    async fn load_snapshot(&self) -> OxideResult<Option<RiskEngineSnapshot>>;

    /// Persist a blacklist entry (add or update).
    async fn save_blacklist_entry(&self, entry: &BlacklistEntry) -> OxideResult<()>;

    /// Remove a blacklist entry by market_id.
    async fn remove_blacklist_entry(&self, market_id: &MarketId) -> OxideResult<()>;

    /// Load all active (non-expired) blacklist entries.
    async fn load_blacklist_entries(&self) -> OxideResult<Vec<BlacklistEntry>>;

    /// Persist an emergency snapshot for post-mortem analysis.
    async fn save_emergency_snapshot(&self, snapshot: &EmergencySnapshot) -> OxideResult<()>;

    /// Persist a reconciliation report.
    async fn save_reconciliation_report(
        &self,
        report: &ReconciliationReport,
    ) -> OxideResult<()>;

    /// Append an immutable audit event. This is not a best-effort log:
    /// critical state transitions and denied/allowed trade decisions require
    /// a durable audit record before they are acknowledged to the caller.
    async fn append_audit_event(&self, event: &RiskAuditEvent) -> OxideResult<()>;
}
```

### 2.3 `BalanceQuerier`

用于对账的链上/交易所余额查询 trait。

```rust
/// Query the authoritative on-chain or exchange-side balance.
///
/// This trait is separated from `RiskMetrics` because it involves
/// actual I/O (API calls or RPC) and may be slow. Called only during
/// periodic reconciliation, never on the hot trade path.
#[async_trait::async_trait]
pub trait BalanceQuerier: Send + Sync + 'static {
    /// Fetch the current USDC.e balance from the exchange/chain.
    /// Returns `(available_balance, locked_in_orders)`.
    async fn query_balance(&self) -> OxideResult<(Usd, Usd)>;

    /// Fetch per-market position values from the exchange.
    /// Returns a map of market_id → position_value_usd.
    async fn query_positions(&self) -> OxideResult<Vec<(MarketId, Usd)>>;
}
```

### 2.4 状态权威源与 fail-closed 规则

Phase 4.1 的生产原则：

1. **PostgreSQL/repository + audit event 是权威源**。`CircuitBreaker`、`DailyAccounting`、`WeeklyAccounting`、`PositionTracker`、`PotentialLossLedger`、`BlacklistManager` 的内存结构只是可重建投影。
2. **现有 `oxide-arb-storage/src/cache` 不能作为风控权威状态**。该缓存层设计为 fail-open、TTL 读加速、可丢数据；风控门禁必须 fail-closed、可恢复、可审计。
3. **pre-trade 热路径读取快照**。`RiskContextBuilder` 从已恢复且校验通过的内存投影构造不可变 `RiskContext`，所有 check 在同一版本快照上运行。
4. **post-trade 路径同步提交**。trade result 会触发 accounting、breaker、blacklist、position、potential-loss、drawdown 更新；任一关键写失败，engine 进入 manual/system halt 并拒绝后续交易。
5. **启动恢复必须完整**。恢复过程加载 snapshot、active blacklist、current accounting windows、open positions、active potential-loss entries、drawdown HWM；任何缺失、过期窗口未正确 rollover、版本不一致、数据自相矛盾都必须阻止交易。

```text
startup
  ├── load risk_engine_state
  ├── load current accounting windows
  ├── load open positions + active reservations
  ├── load active potential-loss ledger
  ├── load active blacklist entries
  ├── rebuild in-memory projections
  ├── validate invariants
  └── only then accept pre_trade_check()
```

---

## 3. CircuitBreaker 状态机

### 3.1 4-state FSM 设计

```text
                      ┌─────────────────────────────────────────────────────┐
                      │                                                     │
                      ▼                                                     │
               ┌────────────┐     trip(level)     ┌────────────┐            │
               │   Closed   │ ──────────────────▶ │    Open    │            │
               │ (正常交易)  │                      │ (冷却计时)  │            │
               └────────────┘                      └─────┬──────┘            │
                      ▲                                   │                  │
                      │                          cooldown expires            │
                      │                                   │                  │
                      │                                   ▼                  │
                      │                            ┌────────────┐            │
                      │                            │  HalfOpen  │            │
                      │                            │ (探测交易)  │            │
                      │                            └──┬─────┬───┘            │
                      │                               │     │                │
                      │                    probes ok   │     │ probe fails    │
                      │                               │     │                │
                      │                               ▼     └──────┐         │
                      │                         ┌────────────┐     │         │
                      │         observation     │ Recovered  │     │         │
                      │         period ok       │ (观察期)   │     │         │
                      └─────────────────────────┤            │     │         │
                                                └────────────┘     │         │
                                                                   │         │
                                                      ┌───────────┘         │
                                                      │ (回退 Open,         │
                                                      │  cooldown × 2)      │
                                                      ▼                     │
                                                 ┌────────────┐             │
                                                 │    Open    │─────────────┘
                                                 │ (重新冷却)  │  (同上循环)
                                                 └────────────┘

  额外边：reset() ──▶ 任意状态 → Closed  (运维人员手动恢复)
```

### 3.2 `BreakerState` 运行时枚举

持久化层现有 `BreakerStateName` 是 4 值扁平枚举，运行时使用更富的 `BreakerState`。生产设计中二者**不合并**，但必须收敛命名和边界：

- `BreakerState` 是状态机事实源，包含完成转换所需的全部元数据。
- `BreakerStateName` 只是 DB enum / reporting projection，不允许进入业务判断分支。
- 从 `RiskEngineSnapshot` 恢复时必须构造完整 `BreakerState`，缺失必需字段时恢复失败并 fail-closed。
- 对外 API 返回 `BreakerSnapshot`，包含 `state_name` 与元数据；避免调用方拿 `BreakerStateName` 做半吊子判断。

```rust
/// Runtime circuit breaker state with embedded transition metadata.
///
/// Richer than the persisted `BreakerStateName` — carries timing info
/// needed for FSM transitions without re-querying the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BreakerState {
    /// Normal operation. All checks pass, execution permitted.
    Closed,

    /// Tripped. Execution blocked until `cooldown_until`.
    Open {
        level: CircuitBreakerLevel,
        reason: String,
        tripped_at: DateTime<Utc>,
        cooldown_until: DateTime<Utc>,
    },

    /// Cooldown expired. Allowing limited probe trades.
    HalfOpen {
        level: CircuitBreakerLevel,
        entered_at: DateTime<Utc>,
        successful_probes: u32,
        required_probes: u32,
    },

    /// Probes passed. Observation period before full recovery.
    Recovered {
        entered_at: DateTime<Utc>,
        observation_until: DateTime<Utc>,
    },
}

impl BreakerState {
    /// Convert to the persisted enum for database storage.
    pub fn to_name(&self) -> BreakerStateName {
        match self {
            Self::Closed => BreakerStateName::Closed,
            Self::Open { .. } => BreakerStateName::Open,
            Self::HalfOpen { .. } => BreakerStateName::HalfOpen,
            Self::Recovered { .. } => BreakerStateName::Recovered,
        }
    }

    /// Whether the breaker currently permits trade execution.
    pub fn allows_trading(&self) -> bool {
        matches!(self, Self::Closed | Self::HalfOpen { .. })
    }

    /// Whether the breaker is in probe mode (limited trading).
    pub fn is_probe_mode(&self) -> bool {
        matches!(self, Self::HalfOpen { .. })
    }
}
```

#### 3.2.1 为什么不与 `BreakerStateName` 合并

不合并的原因不是向前兼容，而是状态建模的职责不同：

| 类型 | 职责 | 是否可用于业务判断 | 是否可持久化 |
|------|------|--------------------|--------------|
| `BreakerState` | 运行时 FSM 状态，携带 `cooldown_until`、probe 计数、观测窗口、trip reason | 是 | 可序列化，但不直接映射 DB enum |
| `BreakerStateName` | DB/reporting 的扁平标签，便于索引、dashboard、告警展示 | 否 | 是 |

如果强行合并，会出现两个生产级风险：

1. `Open` 没有 `cooldown_until`、`level`、`reason` 时无法决定是否进入 `HalfOpen`，实现会被迫反查 DB 或引入旁路字段。
2. `HalfOpen` 没有 `successful_probes`/`required_probes` 时无法保证 probe 收敛，重启后可能错误放行或永久卡死。

因此最佳实践是：**运行时使用富状态，持久化使用 projection，但 projection 不能反向替代状态机。**
为了避免类型散乱，所有转换集中在 `BreakerSnapshot::from_state()` 和 `CircuitBreaker::restore(snapshot)` 两个入口，禁止业务代码手写 `to_name()`/`from_name()` 分支。

### 3.3 `BreakerLevel` (L1–L4) 触发条件

| Level | 枚举值 | 触发条件 | 默认冷却 | 说明 |
|-------|--------|---------|---------|------|
| L1 | `CircuitBreakerLevel::Trade` | 单笔交易静态过滤连续失败（depth 不足、staleness expired、edge < min） | 60s | 轻量级，可能因为市场瞬间波动。快速恢复。 |
| L2 | `CircuitBreakerLevel::Session` | 滚动窗口违规：`consecutive_misses >= max`，hourly loss 超限 | 15min（指数退避）| 会话级别问题，可能市场结构恶化。指数退避 cooldown。 |
| L3 | `CircuitBreakerLevel::Daily` | 每日/每周损失上限突破 | 1h | 当日/当周风控上限触达。严重等级。 |
| L4 | `CircuitBreakerLevel::System` | 系统级故障：WS 断连 > 阈值、余额 < min、对账漂移 > 容忍值 | 2h | 最高级别。可能需要人工干预。触发 Emergency alert。 |

**级别升级规则**：higher level trip 覆盖 lower level。如果当前在 L1 Open 状态，收到 L3 trip，直接升级到 L3 Open，重置 cooldown。

### 3.4 `CircuitBreaker` 完整代码草稿

```rust
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: BreakerState,
    /// L2 trip count in this session for exponential cooldown.
    l2_trip_count: u32,
    /// Timestamp of last state transition (for observability).
    last_transition_at: DateTime<Utc>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker in Closed state.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: BreakerState::Closed,
            l2_trip_count: 0,
            last_transition_at: Utc::now(),
        }
    }

    /// Restore from a persisted snapshot (crash recovery).
    pub fn from_snapshot(
        config: CircuitBreakerConfig,
        snapshot: &RiskEngineSnapshot,
    ) -> Self {
        let state = match snapshot.breaker_state {
            BreakerStateName::Closed => BreakerState::Closed,
            BreakerStateName::Open => BreakerState::Open {
                level: snapshot.breaker_level.unwrap_or(CircuitBreakerLevel::Trade),
                reason: snapshot.breaker_reason.clone().unwrap_or_default(),
                tripped_at: snapshot.snapshot_at,
                cooldown_until: snapshot
                    .cooling_until
                    .unwrap_or(snapshot.snapshot_at),
            },
            BreakerStateName::HalfOpen => BreakerState::HalfOpen {
                level: snapshot.breaker_level.unwrap_or(CircuitBreakerLevel::Trade),
                entered_at: snapshot.snapshot_at,
                successful_probes: 0,
                required_probes: config.half_open_probes,
            },
            BreakerStateName::Recovered => BreakerState::Recovered {
                entered_at: snapshot.snapshot_at,
                observation_until: snapshot.snapshot_at
                    + chrono::Duration::seconds(
                        config.recovery_observation_secs as i64,
                    ),
            },
        };
        Self {
            config,
            state,
            l2_trip_count: snapshot.l2_trip_count,
            last_transition_at: snapshot.snapshot_at,
        }
    }

    /// Current state (read-only).
    pub fn state(&self) -> &BreakerState { &self.state }

    /// Whether trading is currently permitted.
    pub fn allows_trading(&self) -> bool { self.state.allows_trading() }

    /// Whether we are in probe mode (HalfOpen).
    pub fn is_probe_mode(&self) -> bool { self.state.is_probe_mode() }

    /// Trip the breaker to Open state at the given level.
    ///
    /// If already Open at a lower level, upgrades. If at same or higher
    /// level, refreshes the cooldown timer.
    pub fn trip(&mut self, level: CircuitBreakerLevel, reason: String) {
        let now = Utc::now();
        let cooldown_secs = self.cooldown_for_level(level);
        let cooldown_until = now
            + chrono::Duration::seconds(cooldown_secs as i64);

        if level == CircuitBreakerLevel::Session {
            self.l2_trip_count += 1;
        }

        let should_trip = match &self.state {
            BreakerState::Closed => true,
            BreakerState::HalfOpen { .. } => true,
            BreakerState::Recovered { .. } => true,
            BreakerState::Open {
                level: current_level, ..
            } => level >= *current_level,
        };

        if should_trip {
            tracing::warn!(
                %level,
                %reason,
                cooldown_secs,
                l2_trip_count = self.l2_trip_count,
                "circuit breaker tripped"
            );
            self.state = BreakerState::Open {
                level,
                reason,
                tripped_at: now,
                cooldown_until,
            };
            self.last_transition_at = now;
        }
    }

    /// Periodic tick — drives time-based state transitions.
    ///
    /// Must be called at least once per second (typically from a
    /// background tick loop in the runtime).
    ///
    /// Returns `true` if a state transition occurred.
    pub fn tick(&mut self) -> bool {
        let now = Utc::now();
        match &self.state {
            BreakerState::Open { level, cooldown_until, .. } => {
                if now >= *cooldown_until {
                    let level = *level;
                    tracing::info!(
                        %level,
                        "cooldown expired, transitioning to HalfOpen"
                    );
                    self.state = BreakerState::HalfOpen {
                        level,
                        entered_at: now,
                        successful_probes: 0,
                        required_probes: self.config.half_open_probes,
                    };
                    self.last_transition_at = now;
                    return true;
                }
            }
            BreakerState::Recovered { observation_until, .. } => {
                if now >= *observation_until {
                    tracing::info!("observation period complete, returning to Closed");
                    self.state = BreakerState::Closed;
                    self.l2_trip_count = 0;
                    self.last_transition_at = now;
                    return true;
                }
            }
            _ => {}
        }
        false
    }

    /// Report a trade result while in HalfOpen state.
    ///
    /// Successful probes increment the counter. When `required_probes`
    /// is reached, transitions to Recovered. A failed probe transitions
    /// back to Open with doubled cooldown.
    pub fn on_trade_result(&mut self, success: bool) {
        let now = Utc::now();
        if let BreakerState::HalfOpen {
            level,
            successful_probes,
            required_probes,
            ..
        } = &mut self.state
        {
            if success {
                *successful_probes += 1;
                tracing::info!(
                    successful = *successful_probes,
                    required = *required_probes,
                    "HalfOpen probe succeeded"
                );
                if *successful_probes >= *required_probes {
                    let observation_until = now
                        + chrono::Duration::seconds(
                            self.config.recovery_observation_secs as i64,
                        );
                    self.state = BreakerState::Recovered {
                        entered_at: now,
                        observation_until,
                    };
                    self.last_transition_at = now;
                }
            } else {
                let level = *level;
                let cooldown_secs = self.cooldown_for_level(level) * 2;
                let cooldown_secs =
                    cooldown_secs.min(self.config.max_cooldown_secs);
                tracing::warn!(
                    %level,
                    cooldown_secs,
                    "HalfOpen probe failed, returning to Open"
                );
                self.state = BreakerState::Open {
                    level,
                    reason: "probe trade failed in HalfOpen".into(),
                    tripped_at: now,
                    cooldown_until: now
                        + chrono::Duration::seconds(cooldown_secs as i64),
                };
                self.last_transition_at = now;
            }
        }
    }

    /// Manual operator intervention — force back to Closed.
    ///
    /// Resets L2 trip count and all timers. Should be logged as an
    /// operational event.
    pub fn reset(&mut self, operator_reason: &str) {
        tracing::warn!(
            reason = operator_reason,
            previous_state = ?self.state.to_name(),
            "circuit breaker manually reset to Closed"
        );
        self.state = BreakerState::Closed;
        self.l2_trip_count = 0;
        self.last_transition_at = Utc::now();
    }

    /// Current L2 trip count (for snapshot persistence).
    pub fn l2_trip_count(&self) -> u32 { self.l2_trip_count }

    // ── private ───────────────────────────────────────────────

    /// Compute cooldown duration (seconds) for a given level.
    ///
    /// L2 uses exponential back-off:
    ///   cooldown = min(l2_cooldown × 2^(trip_count - 1), max_cooldown)
    ///
    /// Other levels use their fixed configured cooldown.
    fn cooldown_for_level(&self, level: CircuitBreakerLevel) -> u64 {
        match level {
            CircuitBreakerLevel::Trade => self.config.l1_cooldown_secs,
            CircuitBreakerLevel::Session => {
                let base = self.config.l2_cooldown_secs;
                let exponent = self.l2_trip_count.saturating_sub(1);
                let multiplied = base.saturating_mul(
                    2_u64.saturating_pow(exponent),
                );
                multiplied.min(self.config.max_cooldown_secs)
            }
            CircuitBreakerLevel::Daily => self.config.l3_cooldown_secs,
            CircuitBreakerLevel::System => self.config.l4_cooldown_secs,
        }
    }
}
```

生产实现不得使用 `unwrap_or` 填补关键状态。`Open` 缺少 `breaker_level`、`breaker_reason` 或 `cooldown_until` 时应返回 `RecoveryError::CorruptBreakerSnapshot`；`HalfOpen` 缺少 probe 元数据时应回退到更安全的 `Open` 或拒绝启动，具体策略必须在测试中固定。

### 3.5 L2 指数退避公式

$$
\text{cooldown}_{\text{L2}} = \min\bigl(\text{base\_cooldown} \times 2^{(n-1)},\; \text{max\_cooldown}\bigr)
$$

其中 $n$ 是当前会话内 L2 trip 的累计次数。

| Trip # | 计算 | 结果 (默认配置) |
|--------|------|---------------|
| 1 | $900 \times 2^0$ | 900s (15min) |
| 2 | $900 \times 2^1$ | 1800s (30min) |
| 3 | $900 \times 2^2$ | 3600s (1h) |
| 4 | $900 \times 2^3$ | 7200s (2h) |
| 5 | $900 \times 2^4 = 14400$ | 14400s (4h, capped by `max_cooldown_secs`) |
| 6+ | capped | 14400s (4h) |

当 FSM 完成 Recovered → Closed 全程恢复后，`l2_trip_count` 重置为 0。

---

## 4. 会计核算 (Accounting)

### 4.1 `PeriodStats` 累积器

```rust
/// Accumulator for a single accounting period (hour/day/week).
///
/// All fields are monotonically increasing within a period. On rollover
/// they reset to zero.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeriodStats {
    /// Gross loss (sum of all negative PnL trades, absolute value).
    pub loss: Usd,
    /// Total fees paid (exchange fees + gas).
    pub fees: Usd,
    /// Net PnL (sum of all trade net profits, may be negative).
    pub pnl: Usd,
    /// Number of trades recorded.
    pub trade_count: u32,
    /// Number of successful fills.
    pub success_count: u32,
    /// Number of misses.
    pub miss_count: u32,
    /// Largest single loss in this period.
    pub max_single_loss: Usd,
    /// Largest single profit in this period.
    pub max_single_profit: Usd,
}
```

### 4.2 `DailyAccounting`

```rust
/// Daily accounting with UTC midnight rollover.
///
/// Thread-safe: wrapped in `parking_lot::RwLock` at the `RiskEngine` level.
/// The `record_trade` method handles rollover detection automatically — if
/// the current UTC date differs from `window_start`, stats are zeroed before
/// recording.
pub struct DailyAccounting {
    window_start: NaiveDate,
    stats: PeriodStats,
    /// Budget remaining (decremented on each trade cost).
    budget_remaining: Usd,
    initial_budget: Usd,
}

impl DailyAccounting {
    pub fn new(budget: Usd) -> Self {
        Self {
            window_start: Utc::now().date_naive(),
            stats: PeriodStats::default(),
            budget_remaining: budget,
            initial_budget: budget,
        }
    }

    /// Restore from snapshot (crash recovery).
    pub fn from_snapshot(
        window_start: NaiveDate,
        stats: PeriodStats,
        budget: Usd,
        spent: Usd,
    ) -> Self {
        Self {
            window_start,
            stats,
            budget_remaining: budget - spent,
            initial_budget: budget,
        }
    }

    /// Record a completed trade. Handles date rollover.
    ///
    /// Returns `true` if a rollover occurred (caller should persist).
    pub fn record_trade(
        &mut self,
        net_profit: Usd,
        fees: Usd,
        cost: Usd,
        outcome: TradeOutcome,
    ) -> bool {
        let rolled = self.maybe_rollover();

        self.stats.trade_count += 1;
        self.stats.pnl = self.stats.pnl + net_profit;
        self.stats.fees = self.stats.fees + fees;

        match outcome {
            TradeOutcome::Success => self.stats.success_count += 1,
            TradeOutcome::Miss => self.stats.miss_count += 1,
            _ => {}
        }

        if net_profit.is_negative() {
            let abs_loss = net_profit.abs();
            self.stats.loss = self.stats.loss + abs_loss;
            self.stats.max_single_loss =
                self.stats.max_single_loss.max(abs_loss);
        } else {
            self.stats.max_single_profit =
                self.stats.max_single_profit.max(net_profit);
        }

        self.budget_remaining = self.budget_remaining - cost;

        rolled
    }

    /// Current daily loss.
    pub fn daily_loss(&self) -> Usd { self.stats.loss }

    /// Current daily PnL.
    pub fn daily_pnl(&self) -> Usd { self.stats.pnl }

    /// Remaining budget for today.
    pub fn budget_remaining(&self) -> Usd { self.budget_remaining }

    /// Whether the daily budget is exhausted.
    pub fn is_budget_exhausted(&self) -> bool {
        self.budget_remaining <= Usd::ZERO
    }

    /// Get a read-only view of current period stats.
    pub fn stats(&self) -> &PeriodStats { &self.stats }

    /// Period start date.
    pub fn window_start(&self) -> NaiveDate { self.window_start }

    // ── private ─────────────────────────────────

    /// Check for UTC midnight rollover and reset if needed.
    fn maybe_rollover(&mut self) -> bool {
        let today = Utc::now().date_naive();
        if today > self.window_start {
            tracing::info!(
                previous = %self.window_start,
                new = %today,
                final_pnl = %self.stats.pnl,
                "daily accounting rollover"
            );
            self.window_start = today;
            self.stats = PeriodStats::default();
            self.budget_remaining = self.initial_budget;
            true
        } else {
            false
        }
    }
}
```

### 4.3 `WeeklyAccounting`

```rust
/// Weekly accounting with UTC Monday 00:00 rollover.
pub struct WeeklyAccounting {
    /// Monday of the current accounting week.
    week_start: NaiveDate,
    stats: PeriodStats,
}

impl WeeklyAccounting {
    pub fn new() -> Self {
        Self {
            week_start: Self::current_monday(),
            stats: PeriodStats::default(),
        }
    }

    pub fn from_snapshot(week_start: NaiveDate, stats: PeriodStats) -> Self {
        Self { week_start, stats }
    }

    /// Record a completed trade. Handles weekly rollover.
    pub fn record_trade(
        &mut self,
        net_profit: Usd,
        fees: Usd,
        outcome: TradeOutcome,
    ) -> bool {
        let rolled = self.maybe_rollover();

        self.stats.trade_count += 1;
        self.stats.pnl = self.stats.pnl + net_profit;
        self.stats.fees = self.stats.fees + fees;

        match outcome {
            TradeOutcome::Success => self.stats.success_count += 1,
            TradeOutcome::Miss => self.stats.miss_count += 1,
            _ => {}
        }

        if net_profit.is_negative() {
            let abs_loss = net_profit.abs();
            self.stats.loss = self.stats.loss + abs_loss;
        }

        rolled
    }

    pub fn weekly_loss(&self) -> Usd { self.stats.loss }
    pub fn stats(&self) -> &PeriodStats { &self.stats }
    pub fn week_start(&self) -> NaiveDate { self.week_start }

    fn maybe_rollover(&mut self) -> bool {
        let monday = Self::current_monday();
        if monday > self.week_start {
            tracing::info!(
                previous = %self.week_start,
                new = %monday,
                final_pnl = %self.stats.pnl,
                "weekly accounting rollover"
            );
            self.week_start = monday;
            self.stats = PeriodStats::default();
            true
        } else {
            false
        }
    }

    /// Compute the Monday of the current UTC week.
    fn current_monday() -> NaiveDate {
        let today = Utc::now().date_naive();
        let weekday = today.weekday().num_days_from_monday();
        today - chrono::Duration::days(weekday as i64)
    }
}
```

### 4.4 线程安全策略

`DailyAccounting` 和 `WeeklyAccounting` 自身是 `!Sync` 的可变结构体。在 `RiskEngine` 中通过 `parking_lot::RwLock` 包装：

```rust
struct RiskEngineInner {
    daily: RwLock<DailyAccounting>,
    weekly: RwLock<WeeklyAccounting>,
    // ...
}
```

- 读路径 (`pre_trade_check`): `daily.read()` — 无阻塞并发读
- 写路径 (`on_trade_result`): `daily.write()` — 独占写锁
- `parking_lot::RwLock` 选型原因：无 poisoning、writer-preferring fairness、比 `std::sync::RwLock` 更低 latency

### 4.5 生产级持久化与重启恢复语义

`DailyAccounting` / `WeeklyAccounting` **不能只存在于内存**。如果服务重启后丢失当日损失、周损失、预算消耗或 miss 统计，系统会错误放大仓位并绕过风控上限，这是不可接受的资金安全缺陷。

生产实现要求：

1. `DailyAccounting`、`WeeklyAccounting` 启动时从 `risk_engine_state` 和 `accounting_period` 当前窗口恢复；窗口不存在时创建，窗口冲突时恢复失败。
2. 每次 `on_trade_result()` 同步更新内存状态、当前 accounting period、risk snapshot 和 audit event；任一关键写失败时 engine 进入 fail-closed halt。
3. rollover 不是简单清零：旧窗口必须 finalize，新窗口必须创建，两个动作应在同一事务或可恢复的幂等流程中完成。
4. budget 消耗以实际资金占用和已确认费用为准，不能仅使用计划交易 cost；miss、partial fill、cancel、failed-after-matched 必须分别记账。
5. 所有窗口使用 UTC，并以 `Clock` trait 注入时间，测试覆盖午夜/周一边界、重复 `on_trade_result()` 幂等、恢复后继续 rollover。

```text
on_trade_result(result)
  ├── derive AccountingDelta
  ├── acquire accounting write lock
  ├── maybe finalize old period + create new period
  ├── apply delta to daily/weekly projections
  ├── persist accounting_period + risk_engine_state + audit event
  └── release lock only after durable commit
```

---

## 5. 持仓追踪 (Position)

### 5.1 `PositionTracker`

`PositionTracker` 维护 per-market 持仓的内存视图，用于快速查询敞口。它**不拥有**持仓数据——数据来源是 `RiskMetrics` trait。

```rust
/// In-memory position tracking and aggregation.
///
/// Wraps `RiskMetrics` to provide computed exposure views.
/// Not thread-safe by itself — protected by `RiskEngine`'s lock.
pub struct PositionTracker {
    /// Cached per-market exposure snapshot.
    market_exposures: HashMap<MarketId, MarketExposure>,
    /// Sum of all position values.
    total_position_value: Usd,
    /// Last refresh timestamp.
    last_refresh: DateTime<Utc>,
}

impl PositionTracker {
    pub fn new() -> Self {
        Self {
            market_exposures: HashMap::new(),
            total_position_value: Usd::ZERO,
            last_refresh: Utc::now(),
        }
    }

    /// Refresh from the live metrics source.
    pub fn refresh(&mut self, metrics: &dyn RiskMetrics) {
        self.market_exposures.clear();
        self.total_position_value = Usd::ZERO;

        for pos in metrics.open_positions() {
            let exposure = self
                .market_exposures
                .entry(pos.market_id.clone())
                .or_insert_with(|| MarketExposure {
                    market_id: pos.market_id.clone(),
                    position_value: Usd::ZERO,
                    reserved_value: Usd::ZERO,
                    total_exposure: Usd::ZERO,
                });
            exposure.position_value =
                exposure.position_value + pos.cost_basis;
            exposure.total_exposure =
                exposure.total_exposure + pos.cost_basis;
            self.total_position_value =
                self.total_position_value + pos.cost_basis;
        }
        self.last_refresh = Utc::now();
    }

    /// Get exposure for a specific market.
    pub fn market_exposure(&self, market_id: &MarketId) -> Usd {
        self.market_exposures
            .get(market_id)
            .map_or(Usd::ZERO, |e| e.total_exposure)
    }

    /// Total portfolio position value.
    pub fn total_position_value(&self) -> Usd {
        self.total_position_value
    }

    /// All per-market exposure summaries.
    pub fn all_exposures(&self) -> Vec<&MarketExposure> {
        self.market_exposures.values().collect()
    }
}
```

### 5.2 `PotentialLossLedger`

```rust
/// Tracks maximum potential loss for unsettled positions.
///
/// Each trade that opens a position creates a ledger entry recording
/// the worst-case loss (cost_basis + fees). Entries are resolved when
/// the market settles or the position is closed.
pub struct PotentialLossLedger {
    entries: HashMap<String, PotentialLossEntry>,
}

impl PotentialLossLedger {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Record a new potential loss entry when a position is opened.
    pub fn record_entry(&mut self, entry: PotentialLossEntry) {
        self.entries.insert(entry.entry_id.clone(), entry);
    }

    /// Resolve an entry (market settled, position closed).
    pub fn resolve(&mut self, entry_id: &str) {
        if let Some(entry) = self.entries.get_mut(entry_id) {
            entry.status = LedgerStatus::Resolved;
            entry.resolved_at = Some(Utc::now());
        }
    }

    /// Total maximum potential loss across all active entries.
    pub fn total_potential_loss(&self) -> Usd {
        self.entries
            .values()
            .filter(|e| e.is_active())
            .map(|e| e.max_loss)
            .sum()
    }

    /// Active entries count.
    pub fn active_count(&self) -> usize {
        self.entries.values().filter(|e| e.is_active()).count()
    }

    /// Get all active entries (for reconciliation).
    pub fn active_entries(&self) -> Vec<&PotentialLossEntry> {
        self.entries.values().filter(|e| e.is_active()).collect()
    }
}
```

### 5.3 与 `RiskMetrics` / repository 的集成

`PositionTracker` 和 `PotentialLossLedger` 同样不能只存在于内存。重启后丢失 open position、pending reservation 或 potential loss，会直接导致 exposure undercount 和 oversizing。

生产实现采用两层：

1. **权威层**：repository 中的 open positions、active reservations、active potential-loss entries。
2. **热路径投影**：`PositionTracker` / `PotentialLossLedger` 在启动恢复时重建，并在 post-trade / reservation 生命周期事件后同步更新。

`pre_trade_check()` 不应临时 `refresh(metrics)` 后再用半新半旧数据；它应读取同一个 `RiskContext` 里的 immutable position snapshot：

```text
RiskContext {
  state_version,
  market_exposure_before,
  total_exposure_before,
  active_reservation_count,
  total_potential_loss,
  open_position_count,
  exposure_snapshot_at,
}
```

`RiskMetrics` 的职责是为 `RiskContextBuilder` 提供已缓存、已校验、同版本的数据，而不是在每个 check 内部分散查询。这样可以避免单次决策中 market exposure、total exposure、balance 来自不同时间点。

### 5.4 Reservation 与 exposure 的强一致要求

单活交易引擎下仍必须实现 reservation，防止一个机会通过 pre-trade 后、下单前被后续机会重复占用同一资金头寸。

```text
pre_trade_check allowed
  ├── create pending exposure reservation
  ├── persist reservation before order submission
  ├── submit order
  ├── confirm reservation on fill
  └── release reservation on reject/cancel/timeout
```

`ExposureLimitChecker` 和 `MultiConstraintSizer` 必须同时计算 position value 与 pending reservation；任何 reservation 状态未知或过期未清理时，按已占用处理。

---

## 6. 静态限制 + 敞口 (Limits)

### 6.1 `StaticLimitChecker` — L1 级静态前置过滤

在**任何资金计算之前**执行，通过即 pass，不通过即 reject，可选触发 L1 circuit breaker。

```rust
/// Level 1 static pre-trade filters.
///
/// These checks are cheap (no I/O, no state mutation) and run first
/// in the `pre_trade_check` pipeline. Failures indicate transient
/// market conditions, not system problems.
pub struct StaticLimitChecker {
    config: RiskConfig,
}

impl StaticLimitChecker {
    pub fn new(config: &RiskConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    /// Run all static limit checks against an opportunity.
    ///
    /// Returns a vector of check results. Use `short_circuit=true` to
    /// return on the first failure.
    pub fn check(
        &self,
        opp: &Opportunity,
        short_circuit: bool,
    ) -> Vec<RiskCheckResult> {
        let mut results = Vec::with_capacity(4);

        // 1. Minimum book depth
        let depth_usd = opp.total_cost; // proxy: total cost ≈ depth consumed
        results.push(self.check_min_depth(opp));
        if short_circuit && !results.last().unwrap().passed {
            return results;
        }

        // 2. Max depth usage percentage
        results.push(self.check_depth_usage(opp));
        if short_circuit && !results.last().unwrap().passed {
            return results;
        }

        // 3. Data staleness
        results.push(self.check_staleness(opp));
        if short_circuit && !results.last().unwrap().passed {
            return results;
        }

        // 4. Minimum edge
        results.push(self.check_min_edge(opp));

        results
    }

    fn check_min_depth(&self, opp: &Opportunity) -> RiskCheckResult {
        // depth_used_pct is % of book, total_cost is how much we consume.
        // We need to infer available depth from the ratio.
        let available_depth = if opp.depth_used_pct > Decimal::ZERO {
            opp.total_cost.inner() * dec!(100) / opp.depth_used_pct
        } else {
            Decimal::MAX
        };
        let passed = available_depth >= self.config.min_depth_usd;
        RiskCheckResult {
            check_name: "min_depth",
            passed,
            detail: if passed {
                None
            } else {
                Some(format!(
                    "available depth {available_depth:.2} < min {}",
                    self.config.min_depth_usd
                ))
            },
        }
    }

    fn check_depth_usage(&self, opp: &Opportunity) -> RiskCheckResult {
        let passed = opp.depth_used_pct <= self.config.max_depth_usage_pct;
        RiskCheckResult {
            check_name: "max_depth_usage",
            passed,
            detail: if passed {
                None
            } else {
                Some(format!(
                    "depth usage {}% > max {}%",
                    opp.depth_used_pct, self.config.max_depth_usage_pct
                ))
            },
        }
    }

    fn check_staleness(&self, opp: &Opportunity) -> RiskCheckResult {
        let passed = opp.staleness < StalenessLevel::Stale;
        RiskCheckResult {
            check_name: "staleness",
            passed,
            detail: if passed {
                None
            } else {
                Some(format!("staleness level: {}", opp.staleness))
            },
        }
    }

    fn check_min_edge(&self, _opp: &Opportunity) -> RiskCheckResult {
        // Endgame always has positive edge by construction (detector threshold).
        // This check exists as a safety net.
        RiskCheckResult {
            check_name: "min_edge",
            passed: true,
            detail: None,
        }
    }
}
```

### 6.2 `ExposureLimitChecker` — 多维敞口限制

```rust
/// Multi-dimensional exposure limit checker.
///
/// Enforces per-market, portfolio-wide, and balance-based limits.
pub struct ExposureLimitChecker {
    config: RiskConfig,
}

impl ExposureLimitChecker {
    pub fn new(config: &RiskConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    /// Run all exposure checks.
    pub fn check(
        &self,
        opp: &Opportunity,
        metrics: &dyn RiskMetrics,
        short_circuit: bool,
    ) -> Vec<RiskCheckResult> {
        let mut results = Vec::with_capacity(5);

        // 1. Single bet size limit
        results.push(self.check_single_bet(opp));
        if short_circuit && !results.last().unwrap().passed {
            return results;
        }

        // 2. Per-market exposure
        results.push(self.check_market_exposure(opp, metrics));
        if short_circuit && !results.last().unwrap().passed {
            return results;
        }

        // 3. Total portfolio exposure (absolute)
        results.push(self.check_total_exposure(opp, metrics));
        if short_circuit && !results.last().unwrap().passed {
            return results;
        }

        // 4. Total exposure as % of balance
        results.push(self.check_exposure_pct(opp, metrics));
        if short_circuit && !results.last().unwrap().passed {
            return results;
        }

        // 5. Max open positions
        results.push(self.check_max_positions(opp, metrics));

        results
    }

    fn check_single_bet(&self, opp: &Opportunity) -> RiskCheckResult {
        let passed = opp.total_cost.inner() <= self.config.max_single_bet_usd;
        RiskCheckResult {
            check_name: "max_single_bet",
            passed,
            detail: if passed {
                None
            } else {
                Some(format!(
                    "bet {} > max {}",
                    opp.total_cost, self.config.max_single_bet_usd
                ))
            },
        }
    }

    fn check_market_exposure(
        &self,
        opp: &Opportunity,
        metrics: &dyn RiskMetrics,
    ) -> RiskCheckResult {
        let current = metrics.market_exposure(&opp.market_id);
        let after = current + opp.total_cost;
        let limit = Usd::new(self.config.max_single_market_exposure_usd);
        let passed = after <= limit;
        RiskCheckResult {
            check_name: "market_exposure",
            passed,
            detail: if passed {
                None
            } else {
                Some(format!(
                    "market exposure after trade {} > limit {}",
                    after, limit
                ))
            },
        }
    }

    fn check_total_exposure(
        &self,
        opp: &Opportunity,
        metrics: &dyn RiskMetrics,
    ) -> RiskCheckResult {
        let current = metrics.total_exposure();
        let after = current + opp.total_cost;
        let limit = Usd::new(self.config.max_total_exposure_usd);
        let passed = after <= limit;
        RiskCheckResult {
            check_name: "total_exposure",
            passed,
            detail: if passed {
                None
            } else {
                Some(format!(
                    "portfolio exposure after trade {} > limit {}",
                    after, limit
                ))
            },
        }
    }

    fn check_exposure_pct(
        &self,
        opp: &Opportunity,
        metrics: &dyn RiskMetrics,
    ) -> RiskCheckResult {
        let balance = metrics.cached_balance();
        if balance <= Usd::ZERO {
            return RiskCheckResult {
                check_name: "exposure_pct",
                passed: false,
                detail: Some("balance is zero or negative".into()),
            };
        }
        let current_exposure = metrics.total_exposure();
        let after = current_exposure + opp.total_cost;
        let pct = after.inner() * dec!(100) / balance.inner();
        let passed = pct <= self.config.max_total_exposure_pct;
        RiskCheckResult {
            check_name: "exposure_pct",
            passed,
            detail: if passed {
                None
            } else {
                Some(format!(
                    "exposure {}% > max {}%",
                    pct.round_dp(1),
                    self.config.max_total_exposure_pct
                ))
            },
        }
    }

    fn check_max_positions(
        &self,
        _opp: &Opportunity,
        metrics: &dyn RiskMetrics,
    ) -> RiskCheckResult {
        let count = metrics.open_position_count();
        let passed = count < self.config.max_open_positions;
        RiskCheckResult {
            check_name: "max_positions",
            passed,
            detail: if passed {
                None
            } else {
                Some(format!(
                    "open positions {} >= max {}",
                    count, self.config.max_open_positions
                ))
            },
        }
    }
}
```

---

## 7. 黑名单管理 (Blacklist)

### 7.1 `BlacklistManager` 设计

`BlacklistManager` 的热路径使用 `DashMap` 是合理的，但它不是权威存储。生产级设计中：

- `DashMap<BlacklistKey, BlacklistEntry>` 是内存投影，用于 O(1) 同步 check。
- PostgreSQL/repository + audit event 是权威源，用于启动恢复、人工操作审计、故障排查。
- 现有 `oxide-arb-storage/src/cache` / Moka / Redis tiered cache 不用于 blacklist 权威状态。

原因：

1. storage cache 当前是 fail-open 读加速，cache miss / timeout 会返回 `None`；黑名单门禁必须 fail-closed。
2. Moka/Redis TTL 只表达缓存失效，不表达业务语义中的永久拉黑、scope 升级、token+market 双索引和审计。
3. TieredCache 的 L1/L2 可能短时间不一致，不能作为资金门禁依据。
4. blacklist 需要 operator action、auto-blacklist、expiry、remove 都有持久化事件；缓存无法替代审计日志。

```rust
/// Concurrent blacklist manager backed by an in-memory `DashMap` projection.
///
/// Key design decisions:
/// - DB/audit log is source of truth; DashMap is reconstructed on startup.
/// - `DashMap<BlacklistKey, BlacklistEntry>` for lock-free concurrent reads.
/// - Lazy TTL eviction: expired entries are not proactively removed,
///   but filtered out on read. Periodic `gc()` sweeps stale entries.
/// - Permanent entries have `expires_at = None` and survive GC.
/// - Scope-based blocking: `DataPath` blocks data ingestion,
///   `TradingPath` blocks trading, `Full` blocks both.
pub struct BlacklistManager {
    entries: DashMap<BlacklistKey, BlacklistEntry>,
    config: RiskConfig,
    recovered_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BlacklistKey {
    Market(MarketId),
    Token(TokenId),
}

impl BlacklistManager {
    pub fn new(config: &RiskConfig) -> Self {
        let manager = Self {
            entries: DashMap::new(),
            config: config.clone(),
            recovered_at: Utc::now(),
        };

        // Load permanent blacklist from config
        for market_id_str in &config.permanent_blacklist_markets {
            let market_id = MarketId::new(market_id_str);
            manager.entries.insert(
                BlacklistKey::Market(market_id.clone()),
                BlacklistEntry {
                    market_id,
                    token_id: None,
                    scope: BlacklistScope::Full,
                    reason: BlacklistReason::Manual,
                    expires_at: None,
                    created_at: Utc::now(),
                    miss_count: 0,
                },
            );
        }

        manager
    }

    /// Restore entries from persistence (startup recovery).
    pub fn load_entries(&self, entries: Vec<BlacklistEntry>) {
        let now = Utc::now();
        for entry in entries {
            if !entry.is_expired(now) {
                self.entries.insert(BlacklistKey::Market(entry.market_id.clone()), entry);
            }
        }
    }

    /// Check if a market is blacklisted for the given scope.
    ///
    /// Returns `Clear` if the market is not blacklisted or the entry
    /// has expired. Expired entries are lazily removed.
    pub fn check(
        &self,
        market_id: &MarketId,
        required_scope: BlacklistScope,
    ) -> BlacklistCheckResult {
        let now = Utc::now();
        let key = BlacklistKey::Market(market_id.clone());
        if let Some(entry) = self.entries.get(&key) {
            if entry.is_expired(now) {
                drop(entry); // release the DashMap ref
                self.entries.remove(&key);
                return BlacklistCheckResult::Clear;
            }
            if entry.scope >= required_scope {
                return BlacklistCheckResult::Blocked {
                    reason: entry.reason,
                    scope: entry.scope,
                    expires_at: entry.expires_at,
                };
            }
        }
        BlacklistCheckResult::Clear
    }

    /// Add a temporary blacklist entry (auto-blacklist on consecutive misses).
    ///
    /// If an entry already exists for this market, it is upgraded if the
    /// new scope is wider or the new expiry is later.
    pub fn add_temporary(
        &self,
        market_id: MarketId,
        token_id: Option<TokenId>,
        scope: BlacklistScope,
        reason: BlacklistReason,
        duration: Duration,
        miss_count: u32,
    ) -> BlacklistEntry {
        let now = Utc::now();
        let expires_at = now + chrono::Duration::from_std(duration)
            .unwrap_or(chrono::Duration::seconds(3600));
        let entry = BlacklistEntry {
            market_id: market_id.clone(),
            token_id,
            scope,
            reason,
            expires_at: Some(expires_at),
            created_at: now,
            miss_count,
        };

        self.entries
            .entry(BlacklistKey::Market(market_id))
            .and_modify(|existing| {
                if entry.scope > existing.scope
                    || entry.expires_at > existing.expires_at
                {
                    *existing = entry.clone();
                }
            })
            .or_insert(entry.clone());

        tracing::info!(
            market_id = %entry.market_id,
            scope = %entry.scope,
            reason = %entry.reason,
            expires_at = ?entry.expires_at,
            "market blacklisted"
        );

        entry
    }

    /// Add a permanent blacklist entry (operator action).
    pub fn add_permanent(
        &self,
        market_id: MarketId,
        reason: BlacklistReason,
    ) -> BlacklistEntry {
        let entry = BlacklistEntry {
            market_id: market_id.clone(),
            token_id: None,
            scope: BlacklistScope::Full,
            reason,
            expires_at: None,
            created_at: Utc::now(),
            miss_count: 0,
        };
        self.entries.insert(BlacklistKey::Market(market_id), entry.clone());
        entry
    }

    /// Remove a blacklist entry (operator action).
    pub fn remove(&self, market_id: &MarketId) -> bool {
        self.entries
            .remove(&BlacklistKey::Market(market_id.clone()))
            .is_some()
    }

    /// Garbage-collect expired entries. Returns count of removed entries.
    pub fn gc(&self) -> usize {
        let now = Utc::now();
        let before = self.entries.len();
        self.entries.retain(|_, entry| !entry.is_expired(now));
        before - self.entries.len()
    }

    /// Check if a market should be auto-blacklisted based on consecutive misses.
    ///
    /// Called by `RiskEngine.on_trade_result()` after a miss.
    pub fn maybe_auto_blacklist(
        &self,
        market_id: &MarketId,
        consecutive_misses: u32,
    ) -> Option<BlacklistEntry> {
        if consecutive_misses >= self.config.market_miss_blacklist_count {
            let duration = Duration::from_secs(
                self.config.market_miss_blacklist_duration_secs,
            );
            Some(self.add_temporary(
                market_id.clone(),
                None,
                BlacklistScope::TradingPath,
                BlacklistReason::ConsecutiveFokFailures,
                duration,
                consecutive_misses,
            ))
        } else {
            None
        }
    }

    /// All active (non-expired) entries for persistence.
    pub fn active_entries(&self) -> Vec<BlacklistEntry> {
        let now = Utc::now();
        self.entries
            .iter()
            .filter(|e| !e.is_expired(now))
            .map(|e| e.value().clone())
            .collect()
    }

    /// Number of active blacklist entries.
    pub fn active_count(&self) -> usize {
        let now = Utc::now();
        self.entries
            .iter()
            .filter(|e| !e.is_expired(now))
            .count()
    }
}
```

### 7.2 Scope 分级阻断语义

| Scope | 阻断 Data 流 | 阻断 Trading 流 | 用例 |
|-------|-------------|-----------------|------|
| `DataPath` | ✓ 停止订阅 orderbook | ✗ | 数据源异常（DataNotFound） |
| `TradingPath` | ✗ 继续接收数据 | ✓ | 连续 FOK miss、TickChange |
| `Full` | ✓ | ✓ | 永久拉黑、严重故障 |

Scope 比较只允许在 `BlacklistManager` 内部完成。其他模块不得直接用
`entry.scope >= required_scope` 做业务判断，避免 token-level 与 market-level 规则被绕过。

### 7.3 自动拉黑触发流程

```text
on_trade_result(miss)
  │
  ├── metrics.consecutive_market_misses(market_id) → count
  │
  └── if count >= config.market_miss_blacklist_count:
        blacklist.add_temporary(
            market_id,
            scope = TradingPath,
            reason = ConsecutiveFokFailures,
            duration = market_miss_blacklist_duration_secs,
        )
        persistence.save_blacklist_entry(&entry)
```

### 7.4 持久化、恢复与审计

每个 blacklist 变更必须产生不可变审计事件：

| 操作 | 持久化要求 | 失败策略 |
|------|------------|----------|
| config permanent preload | 启动时 upsert 到权威源并写 audit | 失败则启动失败 |
| `add_temporary` | 同步 upsert entry + audit event | 失败则 fail-closed |
| `add_permanent` | 同步 upsert entry + operator reason + audit event | 失败则 fail-closed |
| `remove` | 同步 tombstone/remove + operator reason + audit event | 失败则保持 blocked |
| `gc` | 只清内存过期投影；权威源保留历史或标记 expired | 不影响交易安全 |

`check()` 发现 entry 已过期时，可以从内存投影移除，但不能删除审计历史。
若持久化层不可用，不得把未知状态解释为 clear。

---

## 8. 定量器 (Sizing)

### 8.1 Quarter-Kelly 计算器

当前 Kelly 草稿只是 sizing 的数学骨架，不能单独视为生产级闭环。生产实现必须明确：Kelly 输出是**理论上限候选**，不是最终下注金额；任何输入概率、成交概率、费用、滑点、相关性或状态新鲜度不满足质量要求时，Kelly 必须返回 zero 或显著折扣。

#### 8.1.1 公式推导

**Kelly Criterion** 给出最优下注比例 $f^*$：

$$
f^* = \frac{p \cdot b - q}{b}
$$

其中：
- $p$ = 经校准、经置信度折扣、经 fill probability 折扣后的有效胜率
- $q = 1 - p$
- $b$ = 净赔率（odds，对于 endgame：$b = \frac{1 - \text{entry\_price}}{\text{entry\_price}} - \frac{\text{fees} + \text{slippage} + \text{expected_failure_cost}}{\text{cost}}$）

**实际使用 Quarter-Kelly**（$f = 0.25 \times f^*$）以降低 variance：

$$
f_{\text{quarter}} = 0.25 \times \max\left(0,\; \frac{p \cdot b - q}{b}\right)
$$

最终下注金额：

$$
\text{bet} = f_{\text{quarter}} \times \text{bankroll}
$$

#### 8.1.2 代码草稿

```rust
/// Quarter-Kelly position size calculator.
///
/// Uses `Decimal` arithmetic exclusively — no `f64` anywhere in the
/// money path. The Kelly fraction is configurable (default 0.25).
pub struct QuarterKellyCalculator {
    kelly_fraction: Decimal,
    min_edge_bps: Decimal,
    max_kelly_fraction: Decimal,
    min_probability_confidence: Decimal,
}

impl QuarterKellyCalculator {
    pub fn new(kelly_fraction: Decimal, min_edge_bps: Decimal) -> Self {
        Self {
            kelly_fraction,
            min_edge_bps,
        }
    }

    /// Calculate the optimal bet size.
    ///
    /// # Arguments
    /// - `win_prob`: Calibrated probability after confidence/fill discounts (0..1).
    /// - `entry_price`: Price per share (0..1 for Polymarket).
    /// - `fees_pct`: Total fees as a fraction of cost (0..1).
    /// - `bankroll`: Available capital for Kelly computation.
    ///
    /// # Returns
    /// `(bet_usd, kelly_raw, kelly_fractional)` — the raw Kelly
    /// fraction, the fractional Kelly, and the resulting bet in USD.
    pub fn calculate(
        &self,
        win_prob: Decimal,
        entry_price: Decimal,
        fees_pct: Decimal,
        bankroll: Usd,
    ) -> KellyResult {
        // Edge check: require minimum edge before sizing
        let edge_bps = (win_prob - entry_price) / entry_price * dec!(10000);
        if edge_bps < self.min_edge_bps {
            return KellyResult {
                bet_usd: Usd::ZERO,
                kelly_raw: Decimal::ZERO,
                kelly_fractional: Decimal::ZERO,
                edge_bps,
                binding_reason: "below_min_edge",
            };
        }

        // Odds: net payout per dollar risked
        // For endgame: buy at `entry_price`, payout $1 if correct.
        // Gross odds = (1 - entry_price) / entry_price
        // Net odds (after fees) = gross_odds - fees_pct
        let gross_odds = (Decimal::ONE - entry_price) / entry_price;
        let net_odds = gross_odds - fees_pct;

        if net_odds <= Decimal::ZERO {
            return KellyResult {
                bet_usd: Usd::ZERO,
                kelly_raw: Decimal::ZERO,
                kelly_fractional: Decimal::ZERO,
                edge_bps,
                binding_reason: "negative_odds_after_fees",
            };
        }

        // Kelly formula: f* = (p * b - q) / b
        let q = Decimal::ONE - win_prob;
        let kelly_raw = (win_prob * net_odds - q) / net_odds;
        let kelly_raw = kelly_raw.max(Decimal::ZERO);

        let kelly_fractional = kelly_raw * self.kelly_fraction;

        let bet = bankroll.inner() * kelly_fractional;
        let bet_usd = Usd::new(bet.round_dp(2));

        KellyResult {
            bet_usd,
            kelly_raw,
            kelly_fractional,
            edge_bps,
            binding_reason: "kelly",
        }
    }
}

/// Output of Kelly calculation.
#[derive(Debug, Clone, Serialize)]
pub struct KellyResult {
    pub bet_usd: Usd,
    pub kelly_raw: Decimal,
    pub kelly_fractional: Decimal,
    pub edge_bps: Decimal,
    pub binding_reason: &'static str,
}
```

#### 8.1.3 概率质量与 Kelly 折扣

生产级 Kelly 必须消费概率质量元数据，而不是只消费一个裸 `Decimal`：

```rust
pub struct ProbabilityInput {
    pub calibrated_win_prob: Decimal,
    pub fill_prob: Decimal,
    pub calibration_confidence: Decimal,
    pub sample_size: u32,
    pub model_staleness_secs: u64,
    pub expected_slippage_pct: Decimal,
    pub expected_failure_cost_pct: Decimal,
}
```

有效胜率计算规则：

```text
effective_p =
  calibrated_win_prob
  × fill_prob
  × confidence_haircut(calibration_confidence, sample_size)
  × staleness_haircut(model_staleness_secs)
```

强制 zero 条件：

- `calibration_confidence < min_probability_confidence`
- `sample_size < min_calibration_samples`
- `model_staleness_secs > max_probability_staleness_secs`
- `effective_edge_bps < min_edge_bps`
- `net_odds <= 0`
- 任何概率字段不在 `[0, 1]`

Kelly 输出必须包含输入摘要与折扣明细，便于审计“为什么这笔建议下注是 X 美元”。

### 8.2 `MultiConstraintSizer`

多个约束独立计算 max bet，取最小值。每个约束有稳定 ID，输出标识 binding constraint。
生产实现不把“7 个约束”写死为不可扩展事实；约束集合由 `RiskPipeline` 静态注册，测试锁定顺序与结果。

```rust
/// Position sizer that computes 7 independent upper bounds on bet size
/// and returns the minimum.
///
/// Each constraint is evaluated independently. The result identifies
/// which constraint was the binding (smallest) one.
pub struct MultiConstraintSizer {
    config: RiskConfig,
    kelly: QuarterKellyCalculator,
}

impl MultiConstraintSizer {
    pub fn new(config: &RiskConfig, kelly: QuarterKellyCalculator) -> Self {
        Self {
            config: config.clone(),
            kelly,
        }
    }

    /// Compute the final position size under all constraints.
    pub fn size(
        &self,
        opp: &Opportunity,
        metrics: &dyn RiskMetrics,
        fill_prob: Decimal,
        bankroll: Usd,
        drawdown_factor: Decimal,
    ) -> SizeResult {
        let fees_pct = if opp.total_cost.inner() > Decimal::ZERO {
            opp.total_fees.inner() / opp.total_cost.inner()
        } else {
            Decimal::ZERO
        };

        let win_prob = opp.resolution_adjust * fill_prob;

        // 1. Kelly upper bound
        let kelly = self.kelly.calculate(
            win_prob,
            opp.entry_price.inner(),
            fees_pct,
            bankroll,
        );
        let c1 = SizeConstraint {
            name: "quarter_kelly",
            max_usd: kelly.bet_usd,
        };

        // 2. Max single bet
        let c2 = SizeConstraint {
            name: "max_single_bet",
            max_usd: Usd::new(self.config.max_single_bet_usd),
        };

        // 3. Max single loss
        let c3 = SizeConstraint {
            name: "max_single_loss",
            max_usd: Usd::new(self.config.max_single_loss_usd),
        };

        // 4. Per-market exposure headroom
        let market_current = metrics.market_exposure(&opp.market_id);
        let market_limit =
            Usd::new(self.config.max_single_market_exposure_usd);
        let market_headroom = market_limit - market_current;
        let c4 = SizeConstraint {
            name: "market_exposure_headroom",
            max_usd: market_headroom.max(Usd::ZERO),
        };

        // 5. Portfolio exposure headroom
        let total_current = metrics.total_exposure();
        let total_limit = Usd::new(self.config.max_total_exposure_usd);
        let total_headroom = total_limit - total_current;
        let c5 = SizeConstraint {
            name: "portfolio_exposure_headroom",
            max_usd: total_headroom.max(Usd::ZERO),
        };

        // 6. Daily budget remaining
        // (injected via `drawdown_factor` param — the engine passes
        //  daily_accounting.budget_remaining() adjusted by drawdown)
        let c6 = SizeConstraint {
            name: "daily_budget",
            max_usd: bankroll * drawdown_factor,
        };

        // 7. Balance-based exposure limit
        let balance = metrics.cached_balance();
        let reserve = Usd::new(self.config.reserve_balance_usd);
        let available = balance - reserve - total_current;
        let c7 = SizeConstraint {
            name: "available_balance",
            max_usd: available.max(Usd::ZERO),
        };

        let constraints = [c1, c2, c3, c4, c5, c6, c7];
        let binding = constraints
            .iter()
            .min_by_key(|c| c.max_usd)
            .unwrap();

        let final_usd = binding.max_usd.max(Usd::ZERO);

        SizeResult {
            bet_usd: final_usd,
            kelly_result: kelly,
            binding_constraint: binding.name,
            breakdown: SizeBreakdown {
                constraints: constraints.to_vec(),
            },
        }
    }
}

/// A single sizing constraint with its computed upper bound.
#[derive(Debug, Clone, Serialize)]
pub struct SizeConstraint {
    pub name: &'static str,
    pub max_usd: Usd,
}

/// Complete sizing output.
#[derive(Debug, Clone, Serialize)]
pub struct SizeResult {
    pub bet_usd: Usd,
    pub kelly_result: KellyResult,
    pub binding_constraint: &'static str,
    pub breakdown: SizeBreakdown,
}

/// Itemized breakdown of all 7 constraint ceilings.
#[derive(Debug, Clone, Serialize)]
pub struct SizeBreakdown {
    pub constraints: Vec<SizeConstraint>,
}
```

#### 8.2.1 生产级 sizing 约束清单

至少包含以下约束，全部进入 `SizeBreakdown`：

| 约束 | 目的 | 失败/绑定含义 |
|------|------|---------------|
| `kelly_upper_bound` | 理论收益风险上限 | 概率优势不足或 variance 过高 |
| `min_trade_size` | 避免 dust/手续费吞噬 | 小于最小交易额则 zero |
| `max_single_bet` | 单笔名义本金上限 | 防止 fat-finger |
| `max_single_loss` | 单笔最大损失上限 | 以 worst-case loss 计，不只看 cost |
| `market_exposure_headroom` | 单市场总敞口 | 包含 position + pending reservation |
| `portfolio_exposure_headroom` | 组合总敞口 | 包含 active potential loss |
| `daily_budget_remaining` | 当日风险预算 | 使用已持久化 accounting 状态 |
| `weekly_loss_headroom` | 周损失预算 | 防止周内连续亏损放大 |
| `available_balance` | 可用余额扣 reserve | balance 快照过期则 zero |
| `drawdown_factor` | HWM 回撤降速 | Halt 时 zero |
| `liquidity_depth` | 可成交深度 | 限制 depth usage 和滑点 |
| `exchange_order_bounds` | 交易所 min/max/precision | 避免生成不可提交订单 |

最终下注金额必须先向下取整到交易所允许精度，再重新验证所有约束；取整后低于 `min_trade_size` 时拒绝交易。

### 8.3 `DrawdownGuard`

```rust
/// Drawdown protection using High-Water Mark (HWM) tracking.
///
/// Monitors portfolio equity against its historical peak. When drawdown
/// exceeds the configured threshold, the guard reduces position sizes
/// or halts trading entirely.
pub struct DrawdownGuard {
    hwm: Usd,
    max_drawdown_pct: Decimal,
    reduction_factor: Decimal,
}

/// Action recommended by the drawdown guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrawdownAction {
    /// No drawdown concerns. Trade at full size.
    Normal,
    /// Drawdown detected but below halt threshold.
    /// Reduce position sizes by the returned factor (0..1).
    Reduce(/* factor */ ),
    /// Drawdown exceeds threshold. Halt all trading.
    Halt,
}

impl DrawdownGuard {
    pub fn new(
        initial_equity: Usd,
        max_drawdown_pct: Decimal,
        reduction_factor: Decimal,
    ) -> Self {
        Self {
            hwm: initial_equity,
            max_drawdown_pct,
            reduction_factor,
        }
    }

    /// Update the HWM if current equity exceeds it.
    pub fn update_equity(&mut self, current_equity: Usd) {
        if current_equity > self.hwm {
            self.hwm = current_equity;
        }
    }

    /// Compute current drawdown percentage from HWM.
    ///
    /// Returns `(drawdown_pct, DrawdownAction)`.
    pub fn evaluate(&self, current_equity: Usd) -> (Decimal, DrawdownAction) {
        if self.hwm <= Usd::ZERO {
            return (Decimal::ZERO, DrawdownAction::Normal);
        }

        let drawdown = self.hwm - current_equity;
        let drawdown_pct = if drawdown.is_positive() {
            drawdown.inner() * dec!(100) / self.hwm.inner()
        } else {
            Decimal::ZERO
        };

        if drawdown_pct >= self.max_drawdown_pct {
            (drawdown_pct, DrawdownAction::Halt)
        } else if drawdown_pct > Decimal::ZERO {
            // Linear interpolation: as drawdown approaches max,
            // reduction approaches maximum reduction.
            // factor = 1 - (drawdown_pct / max_drawdown_pct) * (1 - reduction_factor)
            let ratio = drawdown_pct / self.max_drawdown_pct;
            let _factor = Decimal::ONE
                - ratio * (Decimal::ONE - self.reduction_factor);
            (drawdown_pct, DrawdownAction::Reduce)
        } else {
            (drawdown_pct, DrawdownAction::Normal)
        }
    }

    /// Compute the sizing reduction factor (1.0 = no reduction).
    pub fn sizing_factor(&self, current_equity: Usd) -> Decimal {
        let (drawdown_pct, action) = self.evaluate(current_equity);
        match action {
            DrawdownAction::Normal => Decimal::ONE,
            DrawdownAction::Reduce => {
                let ratio = drawdown_pct / self.max_drawdown_pct;
                Decimal::ONE
                    - ratio * (Decimal::ONE - self.reduction_factor)
            }
            DrawdownAction::Halt => Decimal::ZERO,
        }
    }

    /// Current HWM.
    pub fn hwm(&self) -> Usd { self.hwm }
}
```

### 8.4 Sizing 闭环与事后校准

生产级 sizing 不是单次函数调用，而是闭环：

```text
pre_trade_check
  ├── build RiskContext
  ├── run gate checks
  ├── compute size constraints
  ├── create exposure reservation
  └── emit decision trace

on_trade_result
  ├── compare recommended vs submitted vs filled size
  ├── record realized fill probability / slippage / fees / miss reason
  ├── update accounting, positions, potential loss, drawdown
  ├── feed calibration metrics
  └── persist audit event
```

必须测试的 invariants：

- 任意输入下 `bet_usd >= 0`
- 任一风险预算下降，最终 size 不上升
- balance、exposure、reservation、drawdown 任一状态未知时 size 为 zero
- Kelly raw 可为正，但任一 hard gate 失败时最终 decision 不允许交易
- full-report 模式下 sizing 也必须输出完整 breakdown，便于诊断

---

## 9. Endgame 专用规则

### 9.1 `EndgameRiskRules`

Endgame 策略的专属风控规则，不适用于其他策略（当前系统只有 endgame）。

```rust
/// Endgame-specific risk rules.
///
/// These rules are strategy-specific and supplement the generic risk
/// checks. They enforce portfolio construction constraints that are
/// meaningful only for endgame convergence trading.
pub struct EndgameRiskRules {
    /// Max number of concurrent positions in the same directional side
    /// (e.g., max 2 YES bets open simultaneously).
    max_concurrent_directional: usize,
    /// Daily budget of directional trades per side.
    daily_directional_budget: u32,
}

impl EndgameRiskRules {
    pub fn new(
        max_concurrent_directional: usize,
        daily_directional_budget: u32,
    ) -> Self {
        Self {
            max_concurrent_directional,
            daily_directional_budget,
        }
    }

    /// Check all endgame-specific rules.
    pub fn check(
        &self,
        opp: &Opportunity,
        metrics: &dyn RiskMetrics,
        short_circuit: bool,
    ) -> Vec<RiskCheckResult> {
        let mut results = Vec::with_capacity(2);

        // 1. Directional concentration
        results.push(self.check_directional_concentration(opp, metrics));
        if short_circuit && !results.last().unwrap().passed {
            return results;
        }

        // 2. Daily directional budget
        results.push(self.check_daily_directional_budget(opp, metrics));

        results
    }

    /// Prevent excessive concentration in one direction.
    ///
    /// Example: if `max_concurrent_directional = 2`, we allow at most
    /// 2 open BUY positions across all markets simultaneously.
    fn check_directional_concentration(
        &self,
        opp: &Opportunity,
        metrics: &dyn RiskMetrics,
    ) -> RiskCheckResult {
        let current = metrics.open_directional_count(opp.side);
        let passed = current < self.max_concurrent_directional;
        RiskCheckResult {
            check_name: "directional_concentration",
            passed,
            detail: if passed {
                None
            } else {
                Some(format!(
                    "{} open {} positions >= max {}",
                    current,
                    opp.side,
                    self.max_concurrent_directional,
                ))
            },
        }
    }

    /// Limit the number of new directional trades per day per side.
    fn check_daily_directional_budget(
        &self,
        opp: &Opportunity,
        metrics: &dyn RiskMetrics,
    ) -> RiskCheckResult {
        let today_count = metrics.daily_directional_trades(opp.side);
        let passed = today_count < self.daily_directional_budget;
        RiskCheckResult {
            check_name: "daily_directional_budget",
            passed,
            detail: if passed {
                None
            } else {
                Some(format!(
                    "{} {} trades today >= budget {}",
                    today_count,
                    opp.side,
                    self.daily_directional_budget,
                ))
            },
        }
    }
}
```

### 9.2 与 `RiskMetrics` 的集成

`EndgameRiskRules` 的两个检查都依赖 `RiskMetrics` 提供的实时数据：

- `open_directional_count(side)`: 当前同方向持仓数
- `daily_directional_trades(side)`: 当日同方向已执行交易数

这些方法由 `oxide-arb-core` 实现，汇总自 repository 层。

---

## 10. 账本对账 (Reconciliation)

### 10.1 `LedgerReconciler` 接口

```rust
/// Periodic reconciliation between internal ledger and exchange/chain state.
///
/// Detects drift caused by missed events, double-counting, or bugs.
/// Does NOT auto-correct — reports mismatches for operator review.
pub struct LedgerReconciler {
    tolerance: Usd,
}

impl LedgerReconciler {
    pub fn new(tolerance_usd: Decimal) -> Self {
        Self {
            tolerance: Usd::new(tolerance_usd),
        }
    }

    /// Run a full reconciliation cycle.
    ///
    /// Compares internal accounting against exchange-reported values.
    pub async fn reconcile(
        &self,
        metrics: &dyn RiskMetrics,
        querier: &dyn BalanceQuerier,
    ) -> OxideResult<ReconciliationReport> {
        let started_at = Utc::now();

        // 1. Query authoritative external state
        let (ext_available, ext_locked) = querier.query_balance().await?;
        let ext_positions = querier.query_positions().await?;

        // 2. Read internal state
        let int_exposure = metrics.total_exposure();
        let int_balance = metrics.cached_balance();
        let int_reserved = metrics.reserved_usd();

        // 3. Compare balance
        let balance_drift = int_balance - ext_available;
        let balance_ok = balance_drift.abs() <= self.tolerance;

        // 4. Compare total exposure
        let ext_total_position: Usd =
            ext_positions.iter().map(|(_, v)| *v).sum();
        let exposure_drift = int_exposure - ext_total_position;
        let exposure_ok = exposure_drift.abs() <= self.tolerance;

        // 5. Per-market comparison
        let mut mismatches = Vec::new();
        for (market_id, ext_value) in &ext_positions {
            let int_value = metrics.market_exposure(market_id);
            let drift = int_value - *ext_value;
            if drift.abs() > self.tolerance {
                mismatches.push(ReconciliationMismatch::PositionDrift {
                    market_id: market_id.clone(),
                    internal: int_value,
                    external: *ext_value,
                    drift,
                });
            }
        }

        if !balance_ok {
            mismatches.push(ReconciliationMismatch::BalanceDrift {
                internal: int_balance,
                external: ext_available,
                drift: balance_drift,
            });
        }

        let status = if mismatches.is_empty() {
            ReconciliationStatus::Ok
        } else if mismatches.iter().any(|m| m.drift_abs() > self.tolerance * dec!(10)) {
            ReconciliationStatus::Critical
        } else {
            ReconciliationStatus::Warning
        };

        Ok(ReconciliationReport {
            status,
            mismatches,
            internal_balance: int_balance,
            external_balance: ext_available,
            internal_exposure: int_exposure,
            external_exposure: ext_total_position,
            reserved: int_reserved,
            tolerance: self.tolerance,
            checked_at: started_at,
            duration_ms: (Utc::now() - started_at).num_milliseconds() as u64,
        })
    }
}
```

### 10.2 `ReconciliationReport`

```rust
/// Full report from a reconciliation run.
#[derive(Debug, Clone, Serialize)]
pub struct ReconciliationReport {
    pub status: ReconciliationStatus,
    pub mismatches: Vec<ReconciliationMismatch>,
    pub internal_balance: Usd,
    pub external_balance: Usd,
    pub internal_exposure: Usd,
    pub external_exposure: Usd,
    pub reserved: Usd,
    pub tolerance: Usd,
    pub checked_at: DateTime<Utc>,
    pub duration_ms: u64,
}
```

### 10.3 `ReconciliationMismatch`

```rust
/// A single mismatch detected during reconciliation.
#[derive(Debug, Clone, Serialize)]
pub enum ReconciliationMismatch {
    /// Internal balance does not match exchange-reported balance.
    BalanceDrift {
        internal: Usd,
        external: Usd,
        drift: Usd,
    },
    /// Internal position value does not match exchange-reported value.
    PositionDrift {
        market_id: MarketId,
        internal: Usd,
        external: Usd,
        drift: Usd,
    },
    /// Reservation exists internally but no matching order on exchange.
    OrphanedReservation {
        reservation_id: ReservationId,
        amount: Usd,
    },
}

impl ReconciliationMismatch {
    /// Absolute drift value (for severity classification).
    pub fn drift_abs(&self) -> Usd {
        match self {
            Self::BalanceDrift { drift, .. } => drift.abs(),
            Self::PositionDrift { drift, .. } => drift.abs(),
            Self::OrphanedReservation { amount, .. } => *amount,
        }
    }
}
```

### 10.4 `ReconciliationStatus`

```rust
/// Overall reconciliation outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ReconciliationStatus {
    /// All values within tolerance.
    Ok,
    /// Some drift detected but within 10× tolerance (warning alert).
    Warning,
    /// Drift exceeds 10× tolerance (circuit breaker L4 trigger).
    Critical,
}
```

### 10.5 定期执行设计

对账由 `RiskEngine` 外部的运行时调度（`oxide-arb-core` 的 tick loop）：

1. 每 `reconciliation_interval_secs`（默认 300s）调用 `reconciler.reconcile()`
2. 结果持久化到 `RiskPersistence.save_reconciliation_report()`
3. `Warning` 状态：发送 Alert，不影响交易
4. `Critical` 状态：触发 L4 circuit breaker `trip(System, "reconciliation critical drift")`

---

## 11. RiskEngine 门面

### 11.0 统一 Check 管线

当前草稿里大量 `check_*` 函数分散在 engine、limits、blacklist、endgame、drawdown 中。生产实现需要保留确定顺序，但不能让编排散落在 `pre_trade_check()` 的长函数里。

设计原则：

1. **静态注册，不做动态插件**。资金风控不需要运行时任意注册 check；注册表由代码构造，顺序在测试中锁定。
2. **统一接口，统一 trace**。每个 check 接收同一个 immutable `RiskContext`，返回 `RiskCheckResult`；不得在 check 内部查询不同时点的 metrics。
3. **分层分类**。区分 hard gate、soft warning、sizing constraint、breaker trigger、post-trade trigger、background reconciliation。
4. **short-circuit 与 full-report 同源**。两种模式使用同一条 pipeline，只是失败后的控制流不同。
5. **所有 check 都必须可审计**。输出包含 id、severity、actual、threshold、detail、elapsed、state_version。

```rust
pub enum RiskCheckId {
    ManualHalt,
    CircuitBreaker,
    BlacklistTradingPath,
    MinDepth,
    MaxDepthUsage,
    Staleness,
    MinEdge,
    DailyBudget,
    DailyLossCap,
    WeeklyLossCap,
    MaxSingleBet,
    MarketExposure,
    TotalExposure,
    ExposurePct,
    MaxPositions,
    WsConnectivity,
    MinBalance,
    DirectionalConcentration,
    DailyDirectionalBudget,
    DrawdownGuard,
}

pub enum RiskCheckKind {
    Gate,
    SizingConstraint,
    BreakerTrigger,
    PostTradeTrigger,
    BackgroundReconciliation,
}

pub trait RiskCheck: Send + Sync {
    fn id(&self) -> RiskCheckId;
    fn kind(&self) -> RiskCheckKind;
    fn evaluate(&self, ctx: &RiskContext) -> RiskCheckResult;
}

pub struct RiskPipeline {
    checks: Vec<Box<dyn RiskCheck>>,
}
```

生产默认 pre-trade 顺序：

```text
RiskContextBuilder
  ├── state recovery/version freshness gate
  ├── manual halt
  ├── circuit breaker
  ├── blacklist trading path
  ├── static market quality gates
  ├── accounting budget/loss gates
  ├── exposure gates
  ├── connectivity/balance gates
  ├── endgame portfolio-construction gates
  ├── drawdown gate
  └── sizing constraints + reservation
```

`RiskPipeline` 是第 5 个问题的最终答案：需要抽象和注册表，但注册表应是**静态、显式、测试锁定**，由统一 manager/pipeline 调度；不要做运行时插件系统。

### 11.1 核心结构

```rust
/// The risk engine facade — single entry point for all risk operations.
///
/// Owns all risk subsystems and orchestrates them through a unified API.
/// Thread-safe: internal state is protected by `parking_lot::RwLock`.
pub struct RiskEngine {
    // ── Sub-systems ─────────────────────────────────────────────
    circuit_breaker: RwLock<CircuitBreaker>,
    daily: RwLock<DailyAccounting>,
    weekly: RwLock<WeeklyAccounting>,
    position_tracker: RwLock<PositionTracker>,
    potential_loss: RwLock<PotentialLossLedger>,
    static_limits: StaticLimitChecker,
    exposure_limits: ExposureLimitChecker,
    pipeline: RiskPipeline,
    blacklist: BlacklistManager,
    sizer: MultiConstraintSizer,
    drawdown: RwLock<DrawdownGuard>,
    endgame_rules: EndgameRiskRules,
    reconciler: LedgerReconciler,

    // ── Configuration ───────────────────────────────────────────
    config: RiskConfig,

    // ── Manual halt flag ────────────────────────────────────────
    is_halted: AtomicBool,
    halt_reason: RwLock<Option<String>>,
}
```

### 11.2 `RiskDecision`

```rust
/// Result of a pre-trade risk evaluation.
#[derive(Debug, Clone, Serialize)]
pub struct RiskDecision {
    /// Whether the trade is allowed to proceed.
    pub allowed: bool,
    /// All check results (populated in full_report mode).
    pub checks: Vec<RiskCheckResult>,
    /// If denied, the first check that failed.
    pub denial_reason: Option<String>,
    /// Recommended position size (zero if denied).
    pub recommended_size: SizeResult,
    /// Current drawdown factor applied to sizing.
    pub drawdown_factor: Decimal,
    /// Evaluated timestamp.
    pub evaluated_at: DateTime<Utc>,
    /// Immutable state version used by all checks in this decision.
    pub state_version: StateVersion,
    /// Full audit trace for diagnosis and post-mortem.
    pub trace: RiskDecisionTrace,
}
```

使用 `RiskDecision` 作为唯一决策类型，不再新增 `PreTradeDecision`。这是破坏式收敛：Phase 4.1 应直接复用/升级 models 中已有概念，而不是并存两个近义类型。

### 11.3 `RiskCheckResult`

```rust
/// Individual risk check result.
#[derive(Debug, Clone, Serialize)]
pub struct RiskCheckResult {
    pub check_name: &'static str,
    pub passed: bool,
    pub detail: Option<String>,
}
```

### 11.4 `pre_trade_check()` — Pipeline 调度流程

`pre_trade_check()` 不再手写 12 段 inline check。生产实现只负责构造 `RiskContext`、调用 `RiskPipeline`、执行 sizing、创建 reservation、写 decision audit。
具体 check 由 §11.0 的静态注册表统一调度。下面的流程是规范实现，后续代码落地应删除旧的散落 `check_*` 编排。

```rust
impl RiskEngine {
    pub fn pre_trade_check(
        &self,
        opp: &Opportunity,
        metrics: &dyn RiskMetrics,
        probability: ProbabilityInput,
        mode: ReportMode,
    ) -> RiskDecision {
        let ctx = self.context_builder.build(opp, metrics, probability)?;
        let gate_report = self.pipeline.evaluate_gates(&ctx, mode);

        if gate_report.has_failed_hard_gate() {
            return self.decision_builder.denied(ctx, gate_report);
        }

        let sizing = self.sizer.size(&ctx);
        if sizing.bet_usd <= Usd::ZERO {
            return self.decision_builder.denied_by_sizing(ctx, gate_report, sizing);
        }

        let reservation = self
            .reservation_manager
            .reserve(&ctx, sizing.bet_usd)
            .expect_or_halt("exposure reservation failed");

        self.decision_builder.allowed(ctx, gate_report, sizing, reservation)
    }
}
```

Legacy draft below is retained only as a check inventory while implementing the pipeline; it is not the target architecture.

```rust
impl RiskEngine {
    /// Evaluate all pre-trade risk checks for an opportunity.
    ///
    /// In `short_circuit` mode, returns immediately on the first failed
    /// check (fast path for production). In `full_report` mode, evaluates
    /// all checks and returns a complete report (useful for diagnostics).
    ///
    /// # Check Order (deterministic, most-likely-to-fail first)
    ///
    ///  1. Manual halt flag
    ///  2. Circuit breaker state
    ///  3. Blacklist check
    ///  4. Static limits (L1): min_depth, max_depth_usage, staleness, min_edge
    ///  5. Daily budget exhaustion
    ///  6. Daily loss cap
    ///  7. Weekly loss cap
    ///  8. Exposure limits: single_bet, market, portfolio, pct, max_positions
    ///  9. WS connectivity health
    /// 10. Minimum balance
    /// 11. Endgame-specific rules (directional concentration, daily budget)
    /// 12. Drawdown guard
    ///
    /// If all checks pass, computes position sizing via `MultiConstraintSizer`.
    pub fn pre_trade_check(
        &self,
        opp: &Opportunity,
        metrics: &dyn RiskMetrics,
        fill_prob: Decimal,
        short_circuit: bool,
    ) -> RiskDecision {
        let mut all_checks: Vec<RiskCheckResult> = Vec::with_capacity(20);
        let now = Utc::now();

        // ── 1. Manual halt ──────────────────────────────────────
        if self.is_halted.load(Ordering::Acquire) {
            let reason = self.halt_reason.read().clone();
            all_checks.push(RiskCheckResult {
                check_name: "manual_halt",
                passed: false,
                detail: reason.clone(),
            });
            if short_circuit {
                return self.denied(all_checks, now);
            }
        } else {
            all_checks.push(RiskCheckResult {
                check_name: "manual_halt",
                passed: true,
                detail: None,
            });
        }

        // ── 2. Circuit breaker ──────────────────────────────────
        {
            let cb = self.circuit_breaker.read();
            let passed = cb.allows_trading();
            all_checks.push(RiskCheckResult {
                check_name: "circuit_breaker",
                passed,
                detail: if passed {
                    None
                } else {
                    Some(format!("state: {:?}", cb.state().to_name()))
                },
            });
            if short_circuit && !passed {
                return self.denied(all_checks, now);
            }
        }

        // ── 3. Blacklist ────────────────────────────────────────
        {
            let bl_result = self.blacklist.check(
                &opp.market_id,
                BlacklistScope::TradingPath,
            );
            let passed = bl_result.is_clear();
            all_checks.push(RiskCheckResult {
                check_name: "blacklist",
                passed,
                detail: if passed {
                    None
                } else if let BlacklistCheckResult::Blocked {
                    reason, scope, ..
                } = &bl_result
                {
                    Some(format!("{reason} (scope: {scope})"))
                } else {
                    None
                },
            });
            if short_circuit && !passed {
                return self.denied(all_checks, now);
            }
        }

        // ── 4. Static limits (L1) ──────────────────────────────
        {
            let static_results =
                self.static_limits.check(opp, short_circuit);
            let any_failed = static_results.iter().any(|r| !r.passed);
            all_checks.extend(static_results);
            if short_circuit && any_failed {
                return self.denied(all_checks, now);
            }
        }

        // ── 5. Daily budget ─────────────────────────────────────
        {
            let daily = self.daily.read();
            let passed = !daily.is_budget_exhausted();
            all_checks.push(RiskCheckResult {
                check_name: "daily_budget",
                passed,
                detail: if passed {
                    None
                } else {
                    Some(format!(
                        "budget exhausted (remaining: {})",
                        daily.budget_remaining()
                    ))
                },
            });
            if short_circuit && !passed {
                return self.denied(all_checks, now);
            }
        }

        // ── 6. Daily loss cap ───────────────────────────────────
        {
            let daily = self.daily.read();
            let passed = daily.daily_loss().inner()
                < self.config.max_daily_loss_usd;
            all_checks.push(RiskCheckResult {
                check_name: "daily_loss_cap",
                passed,
                detail: if passed {
                    None
                } else {
                    Some(format!(
                        "daily loss {} >= cap {}",
                        daily.daily_loss(),
                        self.config.max_daily_loss_usd
                    ))
                },
            });
            if short_circuit && !passed {
                return self.denied(all_checks, now);
            }
        }

        // ── 7. Weekly loss cap ──────────────────────────────────
        {
            let weekly = self.weekly.read();
            let passed = weekly.weekly_loss().inner()
                < self.config.max_weekly_loss_usd;
            all_checks.push(RiskCheckResult {
                check_name: "weekly_loss_cap",
                passed,
                detail: if passed {
                    None
                } else {
                    Some(format!(
                        "weekly loss {} >= cap {}",
                        weekly.weekly_loss(),
                        self.config.max_weekly_loss_usd
                    ))
                },
            });
            if short_circuit && !passed {
                return self.denied(all_checks, now);
            }
        }

        // ── 8. Exposure limits ──────────────────────────────────
        {
            let exposure_results =
                self.exposure_limits.check(opp, metrics, short_circuit);
            let any_failed = exposure_results.iter().any(|r| !r.passed);
            all_checks.extend(exposure_results);
            if short_circuit && any_failed {
                return self.denied(all_checks, now);
            }
        }

        // ── 9. WS connectivity ──────────────────────────────────
        {
            let disconnect_secs = metrics.ws_disconnect_secs();
            let passed = disconnect_secs
                < self.config.ws_disconnect_threshold_secs;
            all_checks.push(RiskCheckResult {
                check_name: "ws_connectivity",
                passed,
                detail: if passed {
                    None
                } else {
                    Some(format!(
                        "WS disconnected for {}s (threshold: {}s)",
                        disconnect_secs,
                        self.config.ws_disconnect_threshold_secs
                    ))
                },
            });
            if short_circuit && !passed {
                return self.denied(all_checks, now);
            }
        }

        // ── 10. Minimum balance ─────────────────────────────────
        {
            let balance = metrics.cached_balance();
            let passed = balance.inner() >= self.config.min_balance_usd;
            all_checks.push(RiskCheckResult {
                check_name: "min_balance",
                passed,
                detail: if passed {
                    None
                } else {
                    Some(format!(
                        "balance {} < min {}",
                        balance, self.config.min_balance_usd
                    ))
                },
            });
            if short_circuit && !passed {
                return self.denied(all_checks, now);
            }
        }

        // ── 11. Endgame-specific rules ──────────────────────────
        {
            let endgame_results =
                self.endgame_rules.check(opp, metrics, short_circuit);
            let any_failed = endgame_results.iter().any(|r| !r.passed);
            all_checks.extend(endgame_results);
            if short_circuit && any_failed {
                return self.denied(all_checks, now);
            }
        }

        // ── 12. Drawdown guard ──────────────────────────────────
        let drawdown_factor;
        {
            let dg = self.drawdown.read();
            let equity = metrics.cached_balance();
            let (dd_pct, action) = dg.evaluate(equity);
            drawdown_factor = dg.sizing_factor(equity);
            let passed = action != DrawdownAction::Halt;
            all_checks.push(RiskCheckResult {
                check_name: "drawdown_guard",
                passed,
                detail: if passed {
                    if drawdown_factor < Decimal::ONE {
                        Some(format!(
                            "drawdown {dd_pct:.1}%, sizing factor {drawdown_factor:.2}"
                        ))
                    } else {
                        None
                    }
                } else {
                    Some(format!(
                        "drawdown {dd_pct:.1}% exceeds max {}%",
                        dg.hwm() // placeholder
                    ))
                },
            });
            if short_circuit && !passed {
                return self.denied(all_checks, now);
            }
        }

        // ── All checks passed — compute sizing ──────────────────
        let any_failed = all_checks.iter().any(|r| !r.passed);
        if any_failed {
            return self.denied(all_checks, now);
        }

        let bankroll = Usd::new(
            metrics.cached_balance().inner()
                - Usd::new(self.config.reserve_balance_usd).inner(),
        );
        let size_result = self.sizer.size(
            opp,
            metrics,
            fill_prob,
            bankroll.max(Usd::ZERO),
            drawdown_factor,
        );

        RiskDecision {
            allowed: size_result.bet_usd > Usd::ZERO,
            checks: all_checks,
            denial_reason: if size_result.bet_usd <= Usd::ZERO {
                Some(format!(
                    "sizing returned zero (binding: {})",
                    size_result.binding_constraint
                ))
            } else {
                None
            },
            recommended_size: size_result,
            drawdown_factor,
            evaluated_at: now,
        }
    }

    fn denied(
        &self,
        checks: Vec<RiskCheckResult>,
        at: DateTime<Utc>,
    ) -> RiskDecision {
        let denial_reason = checks
            .iter()
            .find(|c| !c.passed)
            .map(|c| {
                format!(
                    "{}: {}",
                    c.check_name,
                    c.detail.as_deref().unwrap_or("failed")
                )
            });
        RiskDecision {
            allowed: false,
            checks,
            denial_reason,
            recommended_size: SizeResult::zero(),
            drawdown_factor: Decimal::ONE,
            evaluated_at: at,
        }
    }
}
```

### 11.5 `on_trade_result()` 流程

```rust
impl RiskEngine {
    /// Process a completed trade result — update all subsystems.
    ///
    /// This is the post-trade mutation path. Critical persistence is not
    /// fire-and-forget: accounting, breaker, blacklist, position, potential
    /// loss, drawdown, and audit mutations must be durably committed before
    /// this method returns success. Persistence failure halts the engine.
    ///
    /// # Update sequence
    ///
    /// 1. Daily accounting: record PnL, fees, cost, outcome
    /// 2. Weekly accounting: record PnL, fees, outcome
    /// 3. Circuit breaker: report probe result (if HalfOpen)
    /// 4. Drawdown guard: update equity HWM
    /// 5. Blacklist: check for auto-blacklist on miss
    /// 6. Potential loss ledger: record entry (if success)
    /// 7. Circuit breaker: trip on loss cap breach
    ///
    /// Returns a snapshot for persistence.
    pub fn on_trade_result(
        &self,
        trade: &TradeRecord,
        metrics: &dyn RiskMetrics,
    ) -> RiskEngineSnapshot {
        // 1. Daily accounting
        {
            let mut daily = self.daily.write();
            daily.record_trade(
                trade.net_profit_usd,
                trade.total_fees_usd,
                trade.total_cost_usd,
                trade.status,
            );

            // Check daily loss cap breach → L3 trip
            if daily.daily_loss().inner() >= self.config.max_daily_loss_usd {
                drop(daily);
                self.circuit_breaker.write().trip(
                    CircuitBreakerLevel::Daily,
                    format!(
                        "daily loss cap breached: {}",
                        self.daily.read().daily_loss()
                    ),
                );
            }
        }

        // 2. Weekly accounting
        {
            let mut weekly = self.weekly.write();
            weekly.record_trade(
                trade.net_profit_usd,
                trade.total_fees_usd,
                trade.status,
            );

            // Check weekly loss cap breach → L3 trip
            if weekly.weekly_loss().inner()
                >= self.config.max_weekly_loss_usd
            {
                drop(weekly);
                self.circuit_breaker.write().trip(
                    CircuitBreakerLevel::Daily,
                    format!(
                        "weekly loss cap breached: {}",
                        self.weekly.read().weekly_loss()
                    ),
                );
            }
        }

        // 3. Circuit breaker probe result (HalfOpen)
        {
            let mut cb = self.circuit_breaker.write();
            if cb.is_probe_mode() {
                cb.on_trade_result(trade.is_success());
            }
        }

        // 4. Drawdown guard
        {
            let mut dg = self.drawdown.write();
            dg.update_equity(metrics.cached_balance());
        }

        // 5. Blacklist: auto-blacklist on consecutive misses
        if trade.is_miss() {
            let miss_count =
                metrics.consecutive_market_misses(&trade.market_id);
            self.blacklist
                .maybe_auto_blacklist(&trade.market_id, miss_count);
        }

        // 6. Potential loss ledger (on success)
        if trade.is_success() {
            let mut ledger = self.potential_loss.write();
            ledger.record_entry(PotentialLossEntry {
                entry_id: trade.trade_id.to_string(),
                market_id: trade.market_id.clone(),
                token_id: TokenId::new(""), // filled by caller
                cost_basis: trade.total_cost_usd,
                max_loss: trade.total_cost_usd + trade.total_fees_usd,
                status: LedgerStatus::Active,
                created_at: Utc::now(),
                resolved_at: None,
            });
        }

        // 7. Consecutive misses → L2 trip
        {
            let miss_count =
                metrics.consecutive_market_misses(&trade.market_id);
            if miss_count >= self.config.max_consecutive_misses {
                self.circuit_breaker.write().trip(
                    CircuitBreakerLevel::Session,
                    format!(
                        "consecutive misses: {} >= {}",
                        miss_count, self.config.max_consecutive_misses
                    ),
                );
            }
        }

        self.snapshot(metrics)
    }
}
```

生产实现必须将上面的同步草稿升级为 `async`/事务化 mutation flow：`on_trade_result()` 的返回值应是 `OxideResult<RiskEngineSnapshot>` 或 `OxideResult<PostTradeUpdateReport>`，并在内部完成持久化与 audit。只返回 snapshot 让外部“稍后保存”会打开 crash window，不符合本阶段资金安全要求。

### 11.6 其他公共 API

```rust
impl RiskEngine {
    /// Whether the engine is manually halted.
    pub fn is_halted(&self) -> bool {
        self.is_halted.load(Ordering::Acquire)
    }

    /// Manually halt the engine. All `pre_trade_check` calls will be denied.
    pub fn halt(&self, reason: String) {
        self.is_halted.store(true, Ordering::Release);
        *self.halt_reason.write() = Some(reason);
    }

    /// Resume from manual halt. Does NOT reset the circuit breaker.
    pub fn resume(&self) {
        self.is_halted.store(false, Ordering::Release);
        *self.halt_reason.write() = None;
    }

    /// Drive time-based transitions (circuit breaker, accounting rollover).
    ///
    /// Should be called at ~1 Hz from a background task.
    pub fn tick(&self) {
        self.circuit_breaker.write().tick();
        // Accounting rollover is checked lazily in record_trade(),
        // but tick() provides a guaranteed rollover for days with no trades.
        self.daily.write().maybe_rollover();
        self.weekly.write().maybe_rollover();
    }

    /// Produce a snapshot for persistence (crash recovery).
    pub fn snapshot(&self, metrics: &dyn RiskMetrics) -> RiskEngineSnapshot {
        let cb = self.circuit_breaker.read();
        let daily = self.daily.read();
        let weekly = self.weekly.read();

        RiskEngineSnapshot {
            breaker_state: cb.state().to_name(),
            breaker_level: match cb.state() {
                BreakerState::Open { level, .. } => Some(*level),
                BreakerState::HalfOpen { level, .. } => Some(*level),
                _ => None,
            },
            breaker_reason: match cb.state() {
                BreakerState::Open { reason, .. } => Some(reason.clone()),
                _ => None,
            },
            cooling_until: match cb.state() {
                BreakerState::Open { cooldown_until, .. } => {
                    Some(*cooldown_until)
                }
                _ => None,
            },
            total_exposure: metrics.total_exposure(),
            daily_pnl: daily.daily_pnl(),
            daily_loss: daily.daily_loss(),
            weekly_loss: weekly.weekly_loss(),
            consecutive_misses: 0, // from metrics in core
            l2_trip_count: cb.l2_trip_count(),
            snapshot_at: Utc::now(),
        }
    }

    /// Reset the circuit breaker manually (operator intervention).
    pub fn reset_circuit_breaker(&self, reason: &str) {
        self.circuit_breaker.write().reset(reason);
    }

    /// Access the blacklist manager for external blacklist operations.
    pub fn blacklist(&self) -> &BlacklistManager {
        &self.blacklist
    }

    /// Access the reconciler for external triggering.
    pub fn reconciler(&self) -> &LedgerReconciler {
        &self.reconciler
    }
}
```

### 11.7 `RiskEngineBuilder`

```rust
/// Builder pattern for constructing a `RiskEngine` with all sub-systems.
pub struct RiskEngineBuilder {
    config: Option<RiskConfig>,
    snapshot: Option<RiskEngineSnapshot>,
    blacklist_entries: Vec<BlacklistEntry>,
    initial_equity: Option<Usd>,
    max_concurrent_directional: usize,
    daily_directional_budget: u32,
    kelly_fraction: Decimal,
    min_edge_bps: Decimal,
}

impl RiskEngineBuilder {
    pub fn new() -> Self {
        Self {
            config: None,
            snapshot: None,
            blacklist_entries: Vec::new(),
            initial_equity: None,
            max_concurrent_directional: 3,
            daily_directional_budget: 10,
            kelly_fraction: dec!(0.25),
            min_edge_bps: dec!(200),
        }
    }

    pub fn config(mut self, config: RiskConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn snapshot(mut self, snapshot: RiskEngineSnapshot) -> Self {
        self.snapshot = Some(snapshot);
        self
    }

    pub fn blacklist_entries(
        mut self,
        entries: Vec<BlacklistEntry>,
    ) -> Self {
        self.blacklist_entries = entries;
        self
    }

    pub fn initial_equity(mut self, equity: Usd) -> Self {
        self.initial_equity = Some(equity);
        self
    }

    pub fn max_concurrent_directional(mut self, n: usize) -> Self {
        self.max_concurrent_directional = n;
        self
    }

    pub fn daily_directional_budget(mut self, n: u32) -> Self {
        self.daily_directional_budget = n;
        self
    }

    pub fn kelly_fraction(mut self, f: Decimal) -> Self {
        self.kelly_fraction = f;
        self
    }

    pub fn min_edge_bps(mut self, bps: Decimal) -> Self {
        self.min_edge_bps = bps;
        self
    }

    pub fn build(self) -> RiskEngine {
        let config = self.config.unwrap_or_default();
        let equity = self.initial_equity.unwrap_or(
            Usd::new(dec!(1000)),
        );

        let cb = match &self.snapshot {
            Some(snap) => CircuitBreaker::from_snapshot(
                config.circuit_breaker.clone(),
                snap,
            ),
            None => CircuitBreaker::new(config.circuit_breaker.clone()),
        };

        let daily = match &self.snapshot {
            Some(snap) => DailyAccounting::from_snapshot(
                snap.snapshot_at.date_naive(),
                PeriodStats {
                    loss: snap.daily_loss,
                    pnl: snap.daily_pnl,
                    ..PeriodStats::default()
                },
                Usd::new(config.daily_budget_usd),
                Usd::ZERO,
            ),
            None => DailyAccounting::new(
                Usd::new(config.daily_budget_usd),
            ),
        };

        let weekly = match &self.snapshot {
            Some(snap) => WeeklyAccounting::from_snapshot(
                snap.snapshot_at.date_naive(),
                PeriodStats {
                    loss: snap.weekly_loss,
                    ..PeriodStats::default()
                },
            ),
            None => WeeklyAccounting::new(),
        };

        let blacklist = BlacklistManager::new(&config);
        blacklist.load_entries(self.blacklist_entries);

        let kelly = QuarterKellyCalculator::new(
            self.kelly_fraction,
            self.min_edge_bps,
        );
        let sizer = MultiConstraintSizer::new(&config, kelly);

        let drawdown = DrawdownGuard::new(
            equity,
            config.stop_loss_pct, // re-use as max drawdown %
            dec!(0.5),
        );

        let endgame_rules = EndgameRiskRules::new(
            self.max_concurrent_directional,
            self.daily_directional_budget,
        );

        let reconciler = LedgerReconciler::new(
            config.reconciliation_tolerance_usd,
        );

        RiskEngine {
            circuit_breaker: RwLock::new(cb),
            daily: RwLock::new(daily),
            weekly: RwLock::new(weekly),
            position_tracker: RwLock::new(PositionTracker::new()),
            potential_loss: RwLock::new(PotentialLossLedger::new()),
            static_limits: StaticLimitChecker::new(&config),
            exposure_limits: ExposureLimitChecker::new(&config),
            blacklist,
            sizer,
            drawdown: RwLock::new(drawdown),
            endgame_rules,
            reconciler,
            config,
            is_halted: AtomicBool::new(false),
            halt_reason: RwLock::new(None),
        }
    }
}
```

---

## 12. 测试策略

### 12.1 单元测试矩阵

| 模块 | 测试文件 | 测试项 |
|------|---------|-------|
| `circuit_breaker` | `circuit_breaker_tests.rs` | 见 §12.2 |
| `accounting` | `accounting_tests.rs` | 见 §12.5 |
| `sizing` | `sizing_tests.rs` | 见 §12.3, §12.4 |
| `blacklist` | `blacklist_tests.rs` | 见 §12.6 |
| `limits` | `limits_tests.rs` | static + exposure 限制 |
| `endgame_rules` | `endgame_rules_tests.rs` | 方向集中度 + 日预算 |
| `reconciliation` | `reconciliation_tests.rs` | 对账逻辑 |
| `pipeline` | `pipeline_tests.rs` | 静态注册顺序、short-circuit、full-report trace |
| `recovery` | `recovery_tests.rs` | breaker/accounting/position/blacklist/drawdown 启动恢复 |
| `audit` | `audit_tests.rs` | 关键状态突变和 decision audit 不丢失 |
| `engine` | `engine_tests.rs` | 集成门面 |
| `proptest` | `proptest_sizing.rs` | Kelly + sizing 属性测试 |

### 12.1.1 生产级安全测试矩阵

必须新增以下高风险场景测试：

| 场景 | 断言 |
|------|------|
| 启动时 breaker snapshot 缺少 `cooldown_until` | 恢复失败并 fail-closed |
| accounting 当前窗口缺失或重复 | 恢复失败，不允许交易 |
| open position 存在但 position projection 未恢复 | exposure gate fail-closed |
| active potential-loss ledger 丢失 | sizing 返回 zero |
| blacklist 权威源不可用 | 启动失败或交易 gate blocked |
| `append_audit_event` 失败 | 状态 mutation 返回 error 并 halt |
| `RiskPipeline` 注册顺序变化 | golden test 失败 |
| full-report 模式 | 所有 check 都有 trace、threshold、actual、elapsed |
| reservation 创建失败 | 不提交订单，engine 进入安全拒绝状态 |
| post-trade 持久化中途失败 | 后续 pre-trade 全部拒绝，等待人工恢复 |

### 12.2 CircuitBreaker FSM 完整路径测试 (6 条边)

```rust
#[cfg(test)]
mod circuit_breaker_fsm_tests {
    use super::*;

    fn default_config() -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            l1_cooldown_secs: 2,
            l2_cooldown_secs: 4,
            l3_cooldown_secs: 6,
            l4_cooldown_secs: 8,
            half_open_probes: 2,
            recovery_observation_secs: 3,
            max_cooldown_secs: 60,
        }
    }

    // Edge 1: Closed → Open (trip)
    #[test]
    fn closed_to_open_on_trip() {
        let mut cb = CircuitBreaker::new(default_config());
        assert!(matches!(cb.state(), BreakerState::Closed));

        cb.trip(CircuitBreakerLevel::Trade, "test trip".into());
        assert!(matches!(cb.state(), BreakerState::Open { .. }));
        assert!(!cb.allows_trading());
    }

    // Edge 2: Open → HalfOpen (cooldown expires)
    #[test]
    fn open_to_half_open_on_cooldown_expiry() {
        let mut cb = CircuitBreaker::new(default_config());
        cb.trip(CircuitBreakerLevel::Trade, "test".into());

        // Manually set cooldown_until to the past
        if let BreakerState::Open { ref mut cooldown_until, .. } =
            cb.state_mut()
        {
            *cooldown_until = Utc::now() - chrono::Duration::seconds(1);
        }

        assert!(cb.tick()); // should transition
        assert!(matches!(cb.state(), BreakerState::HalfOpen { .. }));
        assert!(cb.allows_trading()); // probe trades allowed
    }

    // Edge 3: HalfOpen → Recovered (probes succeed)
    #[test]
    fn half_open_to_recovered_on_successful_probes() {
        let mut cb = CircuitBreaker::new(default_config());
        // Fast-forward to HalfOpen
        force_to_half_open(&mut cb);

        cb.on_trade_result(true);  // probe 1
        assert!(matches!(cb.state(), BreakerState::HalfOpen { .. }));

        cb.on_trade_result(true);  // probe 2 (required_probes = 2)
        assert!(matches!(cb.state(), BreakerState::Recovered { .. }));
    }

    // Edge 4: HalfOpen → Open (probe fails)
    #[test]
    fn half_open_to_open_on_probe_failure() {
        let mut cb = CircuitBreaker::new(default_config());
        force_to_half_open(&mut cb);

        cb.on_trade_result(false); // probe fails
        assert!(matches!(cb.state(), BreakerState::Open { .. }));
        assert!(!cb.allows_trading());
    }

    // Edge 5: Recovered → Closed (observation period expires)
    #[test]
    fn recovered_to_closed_on_observation_expiry() {
        let mut cb = CircuitBreaker::new(default_config());
        force_to_recovered(&mut cb);

        // Manually expire observation
        if let BreakerState::Recovered {
            ref mut observation_until, ..
        } = cb.state_mut()
        {
            *observation_until = Utc::now() - chrono::Duration::seconds(1);
        }

        assert!(cb.tick());
        assert!(matches!(cb.state(), BreakerState::Closed));
        assert_eq!(cb.l2_trip_count(), 0); // reset on full recovery
    }

    // Edge 6: Any → Closed (operator reset)
    #[test]
    fn reset_from_any_state_to_closed() {
        let mut cb = CircuitBreaker::new(default_config());
        cb.trip(CircuitBreakerLevel::System, "emergency".into());
        assert!(!cb.allows_trading());

        cb.reset("operator intervention");
        assert!(matches!(cb.state(), BreakerState::Closed));
        assert!(cb.allows_trading());
    }

    // ── L2 exponential cooldown tests ───────────────────────

    #[test]
    fn l2_exponential_cooldown_increases() {
        let mut cb = CircuitBreaker::new(default_config());

        // Trip 1: base cooldown
        cb.trip(CircuitBreakerLevel::Session, "trip 1".into());
        let cd1 = extract_cooldown_secs(&cb);

        cb.reset("test");
        // Trip 2: doubled
        cb.l2_trip_count = 1; // manually set since reset clears it
        cb.trip(CircuitBreakerLevel::Session, "trip 2".into());
        let cd2 = extract_cooldown_secs(&cb);

        assert!(cd2 > cd1);
    }

    #[test]
    fn l2_cooldown_capped_at_max() {
        let config = default_config();
        let mut cb = CircuitBreaker::new(config.clone());
        cb.l2_trip_count = 100; // very high count

        cb.trip(CircuitBreakerLevel::Session, "many trips".into());
        let cd = extract_cooldown_secs(&cb);
        assert!(cd <= config.max_cooldown_secs);
    }

    // ── Level escalation tests ──────────────────────────────

    #[test]
    fn higher_level_overwrites_lower() {
        let mut cb = CircuitBreaker::new(default_config());
        cb.trip(CircuitBreakerLevel::Trade, "L1".into());
        if let BreakerState::Open { level, .. } = cb.state() {
            assert_eq!(*level, CircuitBreakerLevel::Trade);
        }

        cb.trip(CircuitBreakerLevel::Daily, "L3 override".into());
        if let BreakerState::Open { level, .. } = cb.state() {
            assert_eq!(*level, CircuitBreakerLevel::Daily);
        }
    }

    #[test]
    fn lower_level_does_not_overwrite_higher() {
        let mut cb = CircuitBreaker::new(default_config());
        cb.trip(CircuitBreakerLevel::System, "L4".into());

        cb.trip(CircuitBreakerLevel::Trade, "L1 attempt".into());
        if let BreakerState::Open { level, .. } = cb.state() {
            assert_eq!(*level, CircuitBreakerLevel::System);
        }
    }

    // ── Helpers ─────────────────────────────────────────────

    fn force_to_half_open(cb: &mut CircuitBreaker) {
        cb.trip(CircuitBreakerLevel::Trade, "test".into());
        if let BreakerState::Open { ref mut cooldown_until, .. } =
            cb.state_mut()
        {
            *cooldown_until = Utc::now() - chrono::Duration::seconds(1);
        }
        cb.tick();
    }

    fn force_to_recovered(cb: &mut CircuitBreaker) {
        force_to_half_open(cb);
        for _ in 0..2 {
            cb.on_trade_result(true);
        }
    }

    fn extract_cooldown_secs(cb: &CircuitBreaker) -> u64 {
        if let BreakerState::Open {
            tripped_at,
            cooldown_until,
            ..
        } = cb.state()
        {
            (*cooldown_until - *tripped_at).num_seconds() as u64
        } else {
            panic!("not in Open state");
        }
    }
}
```

### 12.3 Kelly 精度测试 (手工验算)

```rust
#[cfg(test)]
mod kelly_precision_tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn calculator() -> QuarterKellyCalculator {
        QuarterKellyCalculator::new(dec!(0.25), dec!(200))
    }

    /// Hand-calculated: p=0.95, entry=0.92, fees=2%
    /// gross_odds = (1 - 0.92) / 0.92 = 0.08695652...
    /// net_odds = 0.08695652 - 0.02 = 0.06695652
    /// q = 0.05
    /// kelly_raw = (0.95 * 0.06695652 - 0.05) / 0.06695652
    ///           = (0.063608694 - 0.05) / 0.06695652
    ///           = 0.013608694 / 0.06695652
    ///           ≈ 0.20323
    /// kelly_quarter = 0.25 * 0.20323 ≈ 0.05081
    /// bet = 1000 * 0.05081 ≈ 50.81
    #[test]
    fn hand_calculated_endgame_scenario() {
        let calc = calculator();
        let result = calc.calculate(
            dec!(0.95),   // win_prob
            dec!(0.92),   // entry_price
            dec!(0.02),   // fees_pct
            Usd::new(dec!(1000)), // bankroll
        );
        // Allow ±0.01 tolerance
        let expected = dec!(50.81);
        let diff = (result.bet_usd.inner() - expected).abs();
        assert!(
            diff < dec!(0.01),
            "expected ~{expected}, got {}, diff={diff}",
            result.bet_usd
        );
    }

    /// Edge case: no edge → zero bet
    #[test]
    fn no_edge_returns_zero() {
        let calc = calculator();
        let result = calc.calculate(
            dec!(0.50), // coin flip
            dec!(0.50), // fair price
            dec!(0.05), // fees eat the edge
            Usd::new(dec!(1000)),
        );
        assert_eq!(result.bet_usd, Usd::ZERO);
    }

    /// Edge case: p=1.0 (certainty) → maximum kelly
    #[test]
    fn certainty_gives_max_kelly() {
        let calc = calculator();
        let result = calc.calculate(
            dec!(1.0),
            dec!(0.90),
            dec!(0.02),
            Usd::new(dec!(1000)),
        );
        assert!(result.bet_usd > Usd::ZERO);
        assert!(result.kelly_raw <= Decimal::ONE);
    }

    /// Fees exceed gross odds → zero bet
    #[test]
    fn excessive_fees_returns_zero() {
        let calc = calculator();
        let result = calc.calculate(
            dec!(0.95),
            dec!(0.98), // very tight price
            dec!(0.10), // 10% fees
            Usd::new(dec!(1000)),
        );
        assert_eq!(result.bet_usd, Usd::ZERO);
    }

    /// Bankroll zero → zero bet
    #[test]
    fn zero_bankroll_returns_zero() {
        let calc = calculator();
        let result = calc.calculate(
            dec!(0.95),
            dec!(0.90),
            dec!(0.02),
            Usd::ZERO,
        );
        assert_eq!(result.bet_usd, Usd::ZERO);
    }
}
```

### 12.4 `MultiConstraintSizer` 约束识别测试

每个测试构造一个场景，使一个特定约束成为 binding constraint：

```rust
#[cfg(test)]
mod sizer_constraint_tests {
    use super::*;

    // Scenario: Kelly suggests $50 but max_single_bet is $25
    #[test]
    fn max_single_bet_is_binding() {
        let result = size_with_overrides(|config| {
            config.max_single_bet_usd = dec!(25);
        });
        assert_eq!(result.binding_constraint, "max_single_bet");
    }

    // Scenario: market exposure headroom is smallest
    #[test]
    fn market_exposure_headroom_is_binding() {
        let result = size_with_market_exposure(dec!(490)); // limit 500
        assert_eq!(result.binding_constraint, "market_exposure_headroom");
    }

    // Scenario: portfolio exposure headroom is smallest
    #[test]
    fn portfolio_exposure_headroom_is_binding() {
        let result = size_with_total_exposure(dec!(4990)); // limit 5000
        assert_eq!(result.binding_constraint, "portfolio_exposure_headroom");
    }

    // Scenario: available balance after reserve is smallest
    #[test]
    fn available_balance_is_binding() {
        let result = size_with_balance(dec!(60)); // reserve 1000, so avail ≈ 0
        assert_eq!(result.binding_constraint, "available_balance");
    }

    // Scenario: Kelly is the natural binding constraint
    #[test]
    fn kelly_is_binding_when_limits_are_generous() {
        let result = size_with_overrides(|config| {
            config.max_single_bet_usd = dec!(10000);
            config.max_total_exposure_usd = dec!(100000);
            config.max_single_market_exposure_usd = dec!(50000);
        });
        assert_eq!(result.binding_constraint, "quarter_kelly");
    }

    // Scenario: max_single_loss is smallest
    #[test]
    fn max_single_loss_is_binding() {
        let result = size_with_overrides(|config| {
            config.max_single_loss_usd = dec!(5);
        });
        assert_eq!(result.binding_constraint, "max_single_loss");
    }

    // Scenario: daily budget is smallest
    #[test]
    fn daily_budget_is_binding() {
        let result = size_with_drawdown_factor(dec!(0.01));
        assert_eq!(result.binding_constraint, "daily_budget");
    }
}
```

### 12.5 Accounting 翻转边界测试

```rust
#[cfg(test)]
mod accounting_rollover_tests {
    use super::*;

    #[test]
    fn daily_rollover_resets_stats() {
        let mut daily = DailyAccounting::new(Usd::new(dec!(100)));
        daily.record_trade(
            Usd::new(dec!(-10)),
            Usd::new(dec!(1)),
            Usd::new(dec!(20)),
            TradeOutcome::Success,
        );
        assert!(daily.daily_loss() > Usd::ZERO);

        // Force rollover by setting window_start to yesterday
        daily.window_start =
            Utc::now().date_naive() - chrono::Duration::days(1);
        let rolled = daily.record_trade(
            Usd::new(dec!(5)),
            Usd::ZERO,
            Usd::new(dec!(10)),
            TradeOutcome::Success,
        );
        assert!(rolled);
        // Loss should be reset (only the new trade's contribution)
        assert_eq!(daily.stats().loss, Usd::ZERO); // +5 profit, no loss
    }

    #[test]
    fn daily_budget_resets_on_rollover() {
        let mut daily = DailyAccounting::new(Usd::new(dec!(50)));
        daily.record_trade(
            Usd::ZERO,
            Usd::ZERO,
            Usd::new(dec!(50)),
            TradeOutcome::Success,
        );
        assert!(daily.is_budget_exhausted());

        daily.window_start =
            Utc::now().date_naive() - chrono::Duration::days(1);
        daily.record_trade(
            Usd::ZERO,
            Usd::ZERO,
            Usd::new(dec!(1)),
            TradeOutcome::Success,
        );
        assert!(!daily.is_budget_exhausted());
    }

    #[test]
    fn weekly_rollover_at_monday_boundary() {
        let mut weekly = WeeklyAccounting::new();
        weekly.record_trade(
            Usd::new(dec!(-20)),
            Usd::new(dec!(2)),
            TradeOutcome::Miss,
        );
        assert!(weekly.weekly_loss() > Usd::ZERO);

        // Force rollover by setting week_start to previous week
        weekly.week_start =
            weekly.week_start - chrono::Duration::weeks(1);
        let rolled = weekly.record_trade(
            Usd::new(dec!(5)),
            Usd::ZERO,
            TradeOutcome::Success,
        );
        assert!(rolled);
        assert_eq!(weekly.weekly_loss(), Usd::ZERO);
    }

    #[test]
    fn no_rollover_within_same_day() {
        let mut daily = DailyAccounting::new(Usd::new(dec!(100)));
        daily.record_trade(
            Usd::new(dec!(-5)),
            Usd::new(dec!(1)),
            Usd::new(dec!(10)),
            TradeOutcome::Success,
        );
        let loss_after_first = daily.daily_loss();

        let rolled = daily.record_trade(
            Usd::new(dec!(-3)),
            Usd::new(dec!(1)),
            Usd::new(dec!(5)),
            TradeOutcome::Miss,
        );
        assert!(!rolled);
        assert!(daily.daily_loss() > loss_after_first);
    }
}
```

### 12.6 BlacklistManager 测试

```rust
#[cfg(test)]
mod blacklist_tests {
    use super::*;
    use std::time::Duration;

    fn test_config() -> RiskConfig {
        RiskConfig {
            market_miss_blacklist_count: 3,
            market_miss_blacklist_duration_secs: 2, // short for testing
            ..RiskConfig::default()
        }
    }

    #[test]
    fn clear_market_passes_check() {
        let bl = BlacklistManager::new(&test_config());
        let result = bl.check(
            &MarketId::new("market_1"),
            BlacklistScope::TradingPath,
        );
        assert!(result.is_clear());
    }

    #[test]
    fn blacklisted_market_is_blocked() {
        let bl = BlacklistManager::new(&test_config());
        bl.add_temporary(
            MarketId::new("market_1"),
            None,
            BlacklistScope::TradingPath,
            BlacklistReason::ConsecutiveFokFailures,
            Duration::from_secs(60),
            3,
        );
        let result = bl.check(
            &MarketId::new("market_1"),
            BlacklistScope::TradingPath,
        );
        assert!(!result.is_clear());
    }

    #[test]
    fn expired_entry_returns_clear() {
        let bl = BlacklistManager::new(&test_config());
        bl.add_temporary(
            MarketId::new("market_1"),
            None,
            BlacklistScope::TradingPath,
            BlacklistReason::ConsecutiveFokFailures,
            Duration::from_millis(1), // expires immediately
            3,
        );
        std::thread::sleep(Duration::from_millis(10));
        let result = bl.check(
            &MarketId::new("market_1"),
            BlacklistScope::TradingPath,
        );
        assert!(result.is_clear());
    }

    #[test]
    fn permanent_entry_never_expires() {
        let bl = BlacklistManager::new(&test_config());
        bl.add_permanent(
            MarketId::new("market_perm"),
            BlacklistReason::Manual,
        );
        // Even after "waiting"
        let result = bl.check(
            &MarketId::new("market_perm"),
            BlacklistScope::Full,
        );
        assert!(!result.is_clear());
    }

    #[test]
    fn gc_removes_expired_entries() {
        let bl = BlacklistManager::new(&test_config());
        bl.add_temporary(
            MarketId::new("market_1"),
            None,
            BlacklistScope::TradingPath,
            BlacklistReason::DepthDrop,
            Duration::from_millis(1),
            1,
        );
        bl.add_permanent(
            MarketId::new("market_2"),
            BlacklistReason::Manual,
        );
        std::thread::sleep(Duration::from_millis(10));

        let removed = bl.gc();
        assert_eq!(removed, 1);
        assert_eq!(bl.active_count(), 1); // permanent remains
    }

    #[test]
    fn scope_ordering_blocks_correctly() {
        let bl = BlacklistManager::new(&test_config());
        bl.add_temporary(
            MarketId::new("market_1"),
            None,
            BlacklistScope::DataPath, // lowest scope
            BlacklistReason::DataNotFound,
            Duration::from_secs(60),
            0,
        );

        // DataPath blocks DataPath check
        assert!(!bl.check(
            &MarketId::new("market_1"),
            BlacklistScope::DataPath,
        ).is_clear());

        // DataPath does NOT block TradingPath check
        assert!(bl.check(
            &MarketId::new("market_1"),
            BlacklistScope::TradingPath,
        ).is_clear());
    }

    #[test]
    fn auto_blacklist_triggers_at_threshold() {
        let bl = BlacklistManager::new(&test_config());
        let market = MarketId::new("miss_market");

        // Below threshold: no blacklist
        assert!(bl.maybe_auto_blacklist(&market, 2).is_none());

        // At threshold: blacklist
        let entry = bl.maybe_auto_blacklist(&market, 3);
        assert!(entry.is_some());
        assert!(!bl.check(&market, BlacklistScope::TradingPath).is_clear());
    }

    #[test]
    fn upgrade_scope_on_re_blacklist() {
        let bl = BlacklistManager::new(&test_config());
        let market = MarketId::new("market_1");

        bl.add_temporary(
            market.clone(),
            None,
            BlacklistScope::DataPath,
            BlacklistReason::DataNotFound,
            Duration::from_secs(60),
            0,
        );

        bl.add_temporary(
            market.clone(),
            None,
            BlacklistScope::Full, // higher scope
            BlacklistReason::TradeFailedAfterMatched,
            Duration::from_secs(60),
            0,
        );

        // Full scope should now block everything
        assert!(!bl.check(&market, BlacklistScope::Full).is_clear());
    }
}
```

### 12.7 DrawdownGuard 测试

```rust
#[cfg(test)]
mod drawdown_tests {
    use super::*;

    #[test]
    fn no_drawdown_returns_normal() {
        let dg = DrawdownGuard::new(
            Usd::new(dec!(1000)),
            dec!(10),
            dec!(0.5),
        );
        let (pct, action) = dg.evaluate(Usd::new(dec!(1000)));
        assert_eq!(pct, Decimal::ZERO);
        assert_eq!(action, DrawdownAction::Normal);
    }

    #[test]
    fn equity_above_hwm_updates_hwm() {
        let mut dg = DrawdownGuard::new(
            Usd::new(dec!(1000)),
            dec!(10),
            dec!(0.5),
        );
        dg.update_equity(Usd::new(dec!(1100)));
        assert_eq!(dg.hwm(), Usd::new(dec!(1100)));
    }

    #[test]
    fn drawdown_below_max_reduces_sizing() {
        let dg = DrawdownGuard::new(
            Usd::new(dec!(1000)),
            dec!(10), // 10% max drawdown
            dec!(0.5),
        );
        let (pct, action) = dg.evaluate(Usd::new(dec!(950))); // 5% dd
        assert!(pct > Decimal::ZERO);
        assert_eq!(action, DrawdownAction::Reduce);

        let factor = dg.sizing_factor(Usd::new(dec!(950)));
        assert!(factor < Decimal::ONE);
        assert!(factor > dec!(0.5));
    }

    #[test]
    fn drawdown_at_max_halts() {
        let dg = DrawdownGuard::new(
            Usd::new(dec!(1000)),
            dec!(10),
            dec!(0.5),
        );
        let (pct, action) = dg.evaluate(Usd::new(dec!(900))); // exactly 10%
        assert!(pct >= dec!(10));
        assert_eq!(action, DrawdownAction::Halt);
    }

    #[test]
    fn drawdown_beyond_max_halts() {
        let dg = DrawdownGuard::new(
            Usd::new(dec!(1000)),
            dec!(10),
            dec!(0.5),
        );
        let (_, action) = dg.evaluate(Usd::new(dec!(800))); // 20% dd
        assert_eq!(action, DrawdownAction::Halt);
    }

    #[test]
    fn sizing_factor_zero_on_halt() {
        let dg = DrawdownGuard::new(
            Usd::new(dec!(1000)),
            dec!(10),
            dec!(0.5),
        );
        let factor = dg.sizing_factor(Usd::new(dec!(850)));
        assert_eq!(factor, Decimal::ZERO);
    }

    #[test]
    fn zero_hwm_returns_normal() {
        let dg = DrawdownGuard::new(
            Usd::ZERO,
            dec!(10),
            dec!(0.5),
        );
        let (_, action) = dg.evaluate(Usd::ZERO);
        assert_eq!(action, DrawdownAction::Normal);
    }
}
```

### 12.8 proptest 策略

```rust
#[cfg(test)]
mod proptest_sizing {
    use proptest::prelude::*;

    proptest! {
        /// Kelly bet is always non-negative.
        #[test]
        fn kelly_bet_non_negative(
            win_prob in 0.01f64..1.0,
            entry_price in 0.01f64..0.99,
            fees_pct in 0.0f64..0.15,
            bankroll in 1.0f64..100_000.0,
        ) {
            let calc = QuarterKellyCalculator::new(dec!(0.25), dec!(0));
            let result = calc.calculate(
                Decimal::from_f64_retain(win_prob).unwrap(),
                Decimal::from_f64_retain(entry_price).unwrap(),
                Decimal::from_f64_retain(fees_pct).unwrap(),
                Usd::new(Decimal::from_f64_retain(bankroll).unwrap()),
            );
            prop_assert!(result.bet_usd >= Usd::ZERO);
        }

        /// Kelly bet never exceeds bankroll.
        #[test]
        fn kelly_bet_within_bankroll(
            win_prob in 0.01f64..1.0,
            entry_price in 0.01f64..0.99,
            fees_pct in 0.0f64..0.10,
            bankroll in 100.0f64..10_000.0,
        ) {
            let calc = QuarterKellyCalculator::new(dec!(0.25), dec!(0));
            let result = calc.calculate(
                Decimal::from_f64_retain(win_prob).unwrap(),
                Decimal::from_f64_retain(entry_price).unwrap(),
                Decimal::from_f64_retain(fees_pct).unwrap(),
                Usd::new(Decimal::from_f64_retain(bankroll).unwrap()),
            );
            prop_assert!(
                result.bet_usd.inner()
                    <= Decimal::from_f64_retain(bankroll).unwrap()
            );
        }

        /// Higher win_prob → higher or equal bet (monotonicity).
        #[test]
        fn kelly_monotone_in_win_prob(
            base_prob in 0.5f64..0.9,
            entry_price in 0.3f64..0.9,
        ) {
            let calc = QuarterKellyCalculator::new(dec!(0.25), dec!(0));
            let bankroll = Usd::new(dec!(1000));

            let p_low = Decimal::from_f64_retain(base_prob).unwrap();
            let p_high = Decimal::from_f64_retain(base_prob + 0.05).unwrap();
            let ep = Decimal::from_f64_retain(entry_price).unwrap();

            let r_low = calc.calculate(p_low, ep, dec!(0.02), bankroll);
            let r_high = calc.calculate(p_high, ep, dec!(0.02), bankroll);

            prop_assert!(r_high.bet_usd >= r_low.bet_usd);
        }

        /// PeriodStats loss is always non-negative after trades.
        #[test]
        fn daily_loss_non_negative(
            profits in prop::collection::vec(-100.0f64..100.0, 1..50),
        ) {
            let mut daily = DailyAccounting::new(Usd::new(dec!(10000)));
            for p in profits {
                daily.record_trade(
                    Usd::new(Decimal::from_f64_retain(p).unwrap()),
                    Usd::new(dec!(0.50)),
                    Usd::new(dec!(10)),
                    if p >= 0.0 {
                        TradeOutcome::Success
                    } else {
                        TradeOutcome::Miss
                    },
                );
            }
            prop_assert!(daily.daily_loss() >= Usd::ZERO);
        }
    }
}
```

### 12.9 集成测试 (Mock Traits)

```rust
#[cfg(test)]
mod engine_integration_tests {
    use super::*;

    /// Mock implementation of RiskMetrics for testing.
    struct MockMetrics {
        total_exposure: Usd,
        market_exposures: HashMap<MarketId, Usd>,
        balance: Usd,
        open_positions: Vec<PositionInfo>,
        ws_disconnect_secs: u64,
        directional_counts: HashMap<Side, usize>,
        daily_directional: HashMap<Side, u32>,
        consecutive_misses: HashMap<MarketId, u32>,
    }

    impl MockMetrics {
        fn default_healthy() -> Self {
            Self {
                total_exposure: Usd::new(dec!(100)),
                market_exposures: HashMap::new(),
                balance: Usd::new(dec!(5000)),
                open_positions: Vec::new(),
                ws_disconnect_secs: 0,
                directional_counts: HashMap::new(),
                daily_directional: HashMap::new(),
                consecutive_misses: HashMap::new(),
            }
        }
    }

    impl RiskMetrics for MockMetrics {
        fn total_exposure(&self) -> Usd { self.total_exposure }
        fn market_exposure(&self, mid: &MarketId) -> Usd {
            self.market_exposures.get(mid).copied().unwrap_or(Usd::ZERO)
        }
        fn open_position_count(&self) -> usize {
            self.open_positions.len()
        }
        fn open_positions(&self) -> Vec<PositionInfo> {
            self.open_positions.clone()
        }
        fn cached_balance(&self) -> Usd { self.balance }
        fn active_reservation_count(&self) -> usize { 0 }
        fn reserved_usd(&self) -> Usd { Usd::ZERO }
        fn open_directional_count(&self, side: Side) -> usize {
            self.directional_counts.get(&side).copied().unwrap_or(0)
        }
        fn daily_directional_trades(&self, side: Side) -> u32 {
            self.daily_directional.get(&side).copied().unwrap_or(0)
        }
        fn consecutive_market_misses(&self, mid: &MarketId) -> u32 {
            self.consecutive_misses.get(mid).copied().unwrap_or(0)
        }
        fn ws_disconnect_secs(&self) -> u64 { self.ws_disconnect_secs }
    }

    fn test_opportunity() -> Opportunity {
        // ... construct a valid test opportunity ...
    }

    #[test]
    fn healthy_system_allows_trade() {
        let engine = RiskEngineBuilder::new()
            .initial_equity(Usd::new(dec!(5000)))
            .build();
        let metrics = MockMetrics::default_healthy();
        let opp = test_opportunity();

        let decision = engine.pre_trade_check(
            &opp, &metrics, dec!(0.90), true,
        );
        assert!(decision.allowed);
    }

    #[test]
    fn halted_engine_denies_trade() {
        let engine = RiskEngineBuilder::new().build();
        engine.halt("test halt".into());
        let metrics = MockMetrics::default_healthy();
        let opp = test_opportunity();

        let decision = engine.pre_trade_check(
            &opp, &metrics, dec!(0.90), true,
        );
        assert!(!decision.allowed);
        assert_eq!(
            decision.checks[0].check_name,
            "manual_halt"
        );
    }

    #[test]
    fn tripped_breaker_denies_trade() {
        let engine = RiskEngineBuilder::new().build();
        engine.circuit_breaker.write().trip(
            CircuitBreakerLevel::System,
            "test".into(),
        );
        let metrics = MockMetrics::default_healthy();
        let opp = test_opportunity();

        let decision = engine.pre_trade_check(
            &opp, &metrics, dec!(0.90), true,
        );
        assert!(!decision.allowed);
    }

    #[test]
    fn low_balance_denies_trade() {
        let engine = RiskEngineBuilder::new().build();
        let metrics = MockMetrics {
            balance: Usd::new(dec!(10)), // below min_balance_usd
            ..MockMetrics::default_healthy()
        };
        let opp = test_opportunity();

        let decision = engine.pre_trade_check(
            &opp, &metrics, dec!(0.90), true,
        );
        assert!(!decision.allowed);
    }

    #[test]
    fn full_report_runs_all_checks() {
        let engine = RiskEngineBuilder::new().build();
        let metrics = MockMetrics::default_healthy();
        let opp = test_opportunity();

        let decision = engine.pre_trade_check(
            &opp, &metrics, dec!(0.90), false, // full_report
        );
        // Should have all 12+ checks regardless of pass/fail
        assert!(decision.checks.len() >= 12);
    }

    #[test]
    fn on_trade_result_updates_accounting() {
        let engine = RiskEngineBuilder::new()
            .initial_equity(Usd::new(dec!(5000)))
            .build();
        let metrics = MockMetrics::default_healthy();

        let trade = TradeRecord {
            net_profit_usd: Usd::new(dec!(-10)),
            total_fees_usd: Usd::new(dec!(1)),
            total_cost_usd: Usd::new(dec!(25)),
            status: TradeOutcome::Success,
            // ... other fields ...
        };

        let snapshot = engine.on_trade_result(&trade, &metrics);
        assert!(snapshot.daily_loss > Usd::ZERO);
    }

    #[test]
    fn tick_drives_breaker_transition() {
        let engine = RiskEngineBuilder::new().build();
        {
            let mut cb = engine.circuit_breaker.write();
            cb.trip(CircuitBreakerLevel::Trade, "test".into());
            // Expire the cooldown
            if let BreakerState::Open { ref mut cooldown_until, .. } =
                cb.state_mut()
            {
                *cooldown_until =
                    Utc::now() - chrono::Duration::seconds(1);
            }
        }
        engine.tick();
        assert!(engine.circuit_breaker.read().is_probe_mode());
    }

    #[test]
    fn resume_clears_halt() {
        let engine = RiskEngineBuilder::new().build();
        engine.halt("testing".into());
        assert!(engine.is_halted());
        engine.resume();
        assert!(!engine.is_halted());
    }
}
```

---

## 13. 验收检查清单

| # | 检查项 | 验证方式 | 对应验收标准 |
|---|--------|---------|-------------|
| 1 | `cargo build -p oxide-arb-risk` 零警告 | CI green | AC-1 |
| 2 | `cargo test -p oxide-arb-risk` 100% pass | CI green | AC-2 |
| 3 | `cargo clippy -p oxide-arb-risk -- -D warnings` 零 lint | CI green | AC-3 |
| 4 | FSM 6 条边全部覆盖 | `circuit_breaker_fsm_tests` | AC-4 |
| 5 | Kelly 手工验算误差 < 0.01 USD | `kelly_precision_tests` | AC-5 |
| 6 | 每种 binding constraint 有独立测试 | `sizer_constraint_tests` | AC-6 |
| 7 | 日翻转在 UTC 午夜 ±1ms 内正确触发 | `accounting_rollover_tests` | AC-7 |
| 8 | 黑名单 TTL 过期后不阻塞 | `blacklist_tests::expired_entry_returns_clear` | AC-8 |
| 9 | DrawdownGuard 在 HWM dd > max 时 Halt | `drawdown_tests::drawdown_at_max_halts` | AC-9 |
| 10 | `RiskPipeline` 顺序、short-circuit、full-report trace 均正确 | `pipeline_tests`, `engine_integration_tests` | AC-10 |
| 11 | `on_trade_result` 无 panic (fuzzy inputs) | proptest | AC-11 |
| 12 | 公共 API `#[must_use]` 标注 | code review | AC-12 |
| 13 | 零 `f64` 在金额路径 | `rg 'f64' crates/oxide-arb-risk/src/` | AC-13 |
| 14 | proptest 回归 Kelly + accounting | `proptest_sizing.rs` | AC-14 |
| 15 | `RiskEngineBuilder` 能从 snapshot 恢复 | `engine_integration_tests` | crash recovery |
| 16 | `BlacklistManager.gc()` 不删永久条目 | `blacklist_tests::gc_removes_expired_entries` | correctness |
| 17 | L2 exponential cooldown 公式正确 | `l2_exponential_cooldown_increases` | correctness |
| 18 | Level escalation：高级覆盖低级 | `higher_level_overwrites_lower` | correctness |
| 19 | `reconcile()` 报告 Critical 且漂移 > 10× tolerance | `reconciliation_tests` | correctness |
| 20 | `DrawdownGuard.sizing_factor()` 在 Halt 时返回 0 | `sizing_factor_zero_on_halt` | safety |
| 21 | 启动恢复缺关键状态时 fail-closed | `recovery_tests` | AC-15 |
| 22 | 关键 mutation 持久化失败会 halt | `audit_tests`, `engine_integration_tests` | AC-16 |
| 23 | decision trace 包含状态版本、阈值、实际值和耗时 | `pipeline_tests` | AC-17 |
| 24 | 散落 check 无法绕过 `RiskPipeline` | code review + `pipeline_order_golden` | AC-18 |
| 25 | public API 无兼容 re-export/alias | code review + public API snapshot | AC-19 |

---

## 14. 预估工作量

| 模块 | 源码 (LoC est.) | 测试 (LoC est.) | 备注 |
|------|----------------|----------------|------|
| `traits.rs` | ~80 | 0 (trait 定义) | RiskMetrics, RiskPersistence, BalanceQuerier |
| `types.rs` | ~120 | ~80 | RiskDecision, RiskCheckResult, RiskDecisionTrace, SizeResult |
| `context.rs` | ~180 | ~160 | RiskContextBuilder, StateVersion, immutable snapshots |
| `pipeline.rs` | ~220 | ~260 | RiskCheck trait, static registry, trace, ordering tests |
| `state_store.rs` | ~220 | ~260 | startup recovery, in-memory projections, invariant validation |
| `audit.rs` | ~140 | ~140 | RiskAuditEvent, decision/mutation audit helpers |
| `circuit_breaker.rs` | ~250 | ~350 | FSM 核心，最复杂的模块 |
| `accounting.rs` | ~280 | ~320 | Daily + Weekly + PeriodStats + durable rollover |
| `position.rs` | ~240 | ~260 | PositionTracker + PotentialLossLedger + reservation projection |
| `limits.rs` | ~200 | ~150 | Static + Exposure checkers |
| `blacklist.rs` | ~280 | ~320 | DB-backed projection + TTL + scope + token/market index + audit |
| `sizing.rs` | ~420 | ~520 | ProbabilityInput + Kelly + MultiConstraint + DrawdownGuard |
| `endgame_rules.rs` | ~80 | ~80 | 方向集中度 + 日预算 |
| `reconciliation.rs` | ~150 | ~120 | Reconciler + Report |
| `engine.rs` | ~420 | ~420 | 门面类，编排恢复、pipeline、sizing、post-trade mutation |
| `builder.rs` | ~160 | ~100 | Builder pattern + recovery dependencies |
| `lib.rs` | ~30 | 0 | explicit modules only; no compatibility re-exports |
| **合计** | **~3,430** | **~3,340** | **总计 ~6,770 LoC** |

交付周期估算：4–6 周（含代码审查、故障注入测试、repository 集成测试和生产演练）。这是资金安全版本，不按最小工作量估算。

---

> **下一步**: Phase 4.2 将在 `oxide-arb-core` 中实现 `RiskMetrics`、`RiskPersistence`、`BalanceQuerier` 的具体实现，并将 `RiskEngine` 集成到主运行时循环中。
