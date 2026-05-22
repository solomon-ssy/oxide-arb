# Phase 4 — 系统内核与风控

> **产出**: `oxide-arb-core`, `oxide-arb-risk` crates
>
> **前置条件**: Phase 0–3 全部完成
>
> **验收标准**: 完整数据管线 WS → BookStore → Detection → Execution 跑通端到端；CircuitBreaker 四级状态机在仿真负载下正确转迁；Quarter-Kelly 定量输出与手动计算一致；FOK+GTD 分层执行策略可对接 paper-trade 模式

---

## 0. 工作范围

### 0.1 自 Phase 0/1 延后的交付项（ADR-001 §5.2 / §8）

以下在架构文档中已定义，但 **故意不在 Phase 0/1 的 `oxide-arb-models` / `oxide-arb-api` 中实现**；在本 Phase 与执行管线一并落地：

| 项 | 落点 | 说明 |
|---|---|---|
| `ExecutionConfig` 扩展字段 | `oxide-arb-models/src/config/execution.rs` | `fok_timeout_ms`, `gtd_expiry_secs`, `max_retries_per_tier`, `price_tolerance_ticks` |
| `TieredExecutionStrategy` | `oxide-arb-core/src/execution/` | FOK → 短 GTD → 长 GTD 分层，读取上述配置 |
| `ExecutionFSM` 简化 | `oxide-arb-core/src/execution/state_machine.rs` | **删除 `Hedging`**；`Emergency` 仅系统级故障，非对冲失败 |
| Oracle `HealthTracker` | `oxide-arb-core/src/oracle/health.rs`（或 `oxide-arb-api` 若仅数据层需要） | 自旧版 `settlement_oracle/health.rs` 移植：300s 滑动窗口、`SourceHealth` / `Degraded` / `Down`，供 `HealthChecker` 与熔断联动 |

**Phase 0 已完成的配置原则**：`detection.min_profit_threshold_usd` 为唯一 min_profit 来源；执行/风控通过 `Settings::min_profit_threshold_usd()` 读取，不得在 `[execution]` / `[risk]` 重复 TOML 字段。

### oxide-arb-risk

独立风控 crate，**不依赖** `oxide-arb-core`。通过 trait 注入实现与核心的解耦。

1. CircuitBreaker 四级状态机
2. 日/周会计核算
3. PositionTracker + PotentialLossLedger
4. 黑名单管理
5. Kelly 定量器 + 多约束系统
6. FillProbability 整合
7. Drawdown 保护
8. Endgame 专用风控规则
9. 账本对账

### oxide-arb-core

系统中枢。编排数据流、检测、执行、可观测性。

1. App 生命周期管理 + DI
2. 数据管线（WS → BookStore → MarketCache）
3. 检测触发（scanner + coalescer + funnel）
4. 执行管线（validate → size → plan → dispatch → confirm → audit）
5. 执行状态机
6. 基础设施工具（async_writer, debounced_writer, periodic, health, retry）
7. 可观测性（metrics, alerts, reports）

---

## 1. oxide-arb-risk 目录结构

```
crates/oxide-arb-risk/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── engine.rs               # RiskEngine: 顶层 API facade
    ├── traits.rs               # RiskMetrics, RiskPersistence — DI 边界
    ├── circuit_breaker/
    │   ├── mod.rs
    │   ├── state_machine.rs    # BreakerState FSM (Closed → Open → HalfOpen → Recovered)
    │   └── levels.rs           # Level 1-4 阈值判断
    ├── accounting/
    │   ├── mod.rs
    │   ├── daily.rs            # DailyAccounting: 滚动日 PnL / 费用 / 交易计数
    │   └── weekly.rs           # WeeklyAccounting: 滚动周核算
    ├── position/
    │   ├── mod.rs
    │   ├── tracker.rs          # PositionTracker: 实时持仓追踪
    │   └── potential_loss.rs   # PotentialLossLedger: 最大潜在损失
    ├── limits/
    │   ├── mod.rs
    │   ├── static_limits.rs    # 静态预交易过滤（Level 1）
    │   └── exposure.rs         # 组合敞口限制
    ├── blacklist/
    │   ├── mod.rs
    │   ├── manager.rs          # BlacklistManager: 临时 + 永久黑名单
    │   └── reasons.rs          # BlacklistReason 枚举 + 过期逻辑
    ├── sizing/
    │   ├── mod.rs
    │   ├── kelly.rs            # Quarter-Kelly 计算器
    │   ├── constraints.rs      # MultiConstraintSizer: Kelly ∩ 风控约束取交集
    │   ├── fill_probability.rs # FillProbability 整合层
    │   └── drawdown.rs         # DrawdownGuard: HWM 回撤保护
    ├── endgame/
    │   ├── mod.rs
    │   └── rules.rs            # EndgameRiskRules: directional bet 专用检查
    └── reconciliation/
        ├── mod.rs
        └── ledger.rs           # LedgerReconciler: 定期对账 + 自动解决
```

---

## 2. oxide-arb-core 目录结构

```
crates/oxide-arb-core/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── app/
    │   ├── mod.rs
    │   ├── context.rs          # AppContext: DI 容器 (Arc-wrapped services)
    │   ├── lifecycle.rs        # Lifecycle: startup → run → shutdown
    │   └── task_registry.rs    # TaskRegistry: tokio::JoinSet 管理
    ├── data/
    │   ├── mod.rs
    │   ├── order_book.rs       # OrderBook: bid/ask 存储 + 查询
    │   ├── book_store.rs       # BookStore: token_id → OrderBook 映射
    │   ├── market_registry.rs  # MarketRegistry: 市场元数据 + token 映射
    │   ├── market_cache.rs     # MarketCache: hot-path 市场数据缓存
    │   └── pipeline.rs         # DataPipeline: WS event → BookStore → trigger
    ├── detection/
    │   ├── mod.rs
    │   ├── scanner.rs          # Scanner: 调用 OpportunityPipeline
    │   ├── coalescer.rs        # Coalescer: 事件合并去重
    │   └── funnel.rs           # Funnel: 限流 + 优先级队列
    ├── execution/
    │   ├── mod.rs
    │   ├── pipeline.rs         # ExecutionPipeline: validate → size → plan → dispatch → confirm
    │   ├── validator.rs        # Validator: freshness + risk pre-check
    │   ├── dispatcher.rs       # Dispatcher: 下单 + 确认
    │   ├── runner.rs           # Runner: 异步执行循环
    │   ├── state_machine.rs    # ExecutionFSM: IDLE → VALIDATE → EXEC → HEDGING → EMERGENCY
    │   ├── plan_builder.rs     # PlanBuilder: 构造 ExecutionPlan
    │   └── capital.rs          # CapitalManager: 资金预留 + 释放
    ├── infra/
    │   ├── mod.rs
    │   ├── async_writer.rs     # AsyncWriter: mpsc → batch DB write
    │   ├── debounced_writer.rs # DebouncedWriter: 合并高频写入
    │   ├── periodic.rs         # PeriodicTask: interval + jitter wrapper
    │   ├── health.rs           # HealthChecker: WS/API/DB/Redis 探针
    │   └── retry.rs            # RetryPolicy: 指数退避 + 断路
    ├── observability/
    │   ├── mod.rs
    │   ├── metrics.rs          # MetricsHub: prometheus counters/gauges/histograms
    │   ├── alerts.rs           # AlertDispatcher: Telegram + Webhook
    │   └── reports.rs          # ReportGenerator: 日/周报
    └── prelude.rs              # 常用 re-exports
```

---

## 3. oxide-arb-risk Trait 边界

```rust
use oxide_arb_models::types::*;
use rust_decimal::Decimal;
use chrono::{DateTime, Utc};

/// Read-only metrics the RiskEngine needs from the core system.
/// Implemented by oxide-arb-core, injected into oxide-arb-risk.
#[async_trait]
pub trait RiskMetrics: Send + Sync + 'static {
    /// Current total portfolio exposure (sum of all open positions' cost basis).
    async fn total_exposure(&self) -> Decimal;

    /// Number of currently open positions.
    async fn open_position_count(&self) -> usize;

    /// Exposure for a specific market.
    async fn market_exposure(&self, market_id: &MarketId) -> Decimal;

    /// Available USDC balance on the exchange.
    async fn available_balance(&self) -> Decimal;

    /// Current WebSocket connection health (true = all shards connected).
    async fn ws_healthy(&self) -> bool;

    /// API error rate in the current window (0.0-1.0).
    async fn api_error_rate(&self) -> Decimal;

    /// Time since last successful WebSocket message.
    async fn ws_last_message_age_secs(&self) -> u64;

    /// Count of directional positions currently open.
    async fn directional_position_count(&self) -> u32;

    /// Total directional exposure (USD).
    async fn directional_exposure(&self) -> Decimal;

    /// Daily budget consumed by directional strategies (USD).
    async fn directional_daily_spent(&self) -> Decimal;
}

/// Persistence interface for risk state.
/// Implemented by oxide-arb-repository, injected into oxide-arb-risk.
#[async_trait]
pub trait RiskPersistence: Send + Sync + 'static {
    async fn load_state(&self) -> Result<RiskEngineSnapshot, OxideError>;
    async fn save_state(&self, state: &RiskEngineSnapshot) -> Result<(), OxideError>;
    async fn load_blacklist(&self) -> Result<Vec<BlacklistEntry>, OxideError>;
    async fn save_blacklist(&self, entries: &[BlacklistEntry]) -> Result<(), OxideError>;
    async fn load_potential_loss_ledger(&self) -> Result<Vec<PotentialLossEntry>, OxideError>;
    async fn save_potential_loss_entry(&self, entry: &PotentialLossEntry) -> Result<(), OxideError>;
}
```

---

## 4. RiskEngine API

```rust
use oxide_arb_models::domain::opportunity::Opportunity;
use oxide_arb_models::domain::strategy_meta::AnyMeta;
use oxide_arb_models::domain::trade::TradeRecord;

pub struct RiskEngine {
    config: RiskConfig,
    breaker: CircuitBreaker,
    daily: DailyAccounting,
    weekly: WeeklyAccounting,
    positions: PositionTracker,
    loss_ledger: PotentialLossLedger,
    blacklist: BlacklistManager,
    sizer: MultiConstraintSizer,
    drawdown: DrawdownGuard,
    endgame_rules: EndgameRiskRules,
    reconciler: LedgerReconciler,
    metrics: Arc<dyn RiskMetrics>,
    persistence: Arc<dyn RiskPersistence>,
}

/// Pre-trade risk check result.
pub struct PreTradeDecision {
    pub approved: bool,
    pub denial_reason: Option<String>,
    pub approved_size: Option<Usd>,
    pub checks_passed: Vec<RiskCheckResult>,
}

pub struct RiskCheckResult {
    pub check_name: &'static str,
    pub passed: bool,
    pub detail: String,
}

impl RiskEngine {
    /// Full pre-trade risk assessment.
    ///
    /// Runs all checks in order; short-circuits on first denial
    /// unless `full_report` is requested (for dry-run diagnostics).
    pub async fn pre_trade_check(
        &self,
        opp: &Opportunity<AnyMeta>,
        full_report: bool,
    ) -> PreTradeDecision {
        let mut checks = Vec::with_capacity(12);

        // 1. Circuit breaker
        if self.breaker.is_open() {
            return PreTradeDecision::denied(
                format!("Circuit breaker open: level {}", self.breaker.current_level()),
                checks,
            );
        }

        // 2. Blacklist
        let bl = self.blacklist.check(&opp.market_id);
        checks.push(bl.into_result("blacklist"));
        if !bl.is_clear() && !full_report {
            return PreTradeDecision::denied(bl.reason(), checks);
        }

        // 3. Static limits (Level 1)
        let static_check = self.check_static_limits(opp).await;
        checks.push(static_check.clone());
        if !static_check.passed && !full_report {
            return PreTradeDecision::denied(static_check.detail, checks);
        }

        // 4. Rolling limits (Level 2)
        let rolling = self.check_rolling_limits().await;
        checks.push(rolling.clone());

        // 5. Daily/weekly caps (Level 3)
        let caps = self.check_daily_weekly_caps().await;
        checks.push(caps.clone());

        // 6. Connectivity (Level 4)
        let conn = self.check_connectivity().await;
        checks.push(conn.clone());

        // 7. Endgame-specific rules
        let endgame = self.endgame_rules.check(opp, &self.metrics).await;
        checks.push(endgame.clone());

        // 8. Position sizing
        let size_result = self.sizer.compute_size(opp, &self.metrics).await;
        checks.push(size_result.check.clone());

        // 9. Drawdown guard
        let dd = self.drawdown.check().await;
        checks.push(dd.clone());

        let all_passed = checks.iter().all(|c| c.passed);
        PreTradeDecision {
            approved: all_passed,
            denial_reason: if all_passed {
                None
            } else {
                checks.iter().find(|c| !c.passed).map(|c| c.detail.clone())
            },
            approved_size: if all_passed { size_result.size } else { None },
            checks_passed: checks,
        }
    }

    /// Called after a trade executes (success or failure).
    ///
    /// Updates accounting, breaker state, blacklist, and potential loss ledger.
    pub async fn on_trade_result(&self, record: &TradeRecord) -> Result<(), OxideError> {
        // Update accounting
        self.daily.record_trade(record);
        self.weekly.record_trade(record);

        // Update circuit breaker
        self.breaker.on_trade_result(record);

        // Update blacklist (on miss/failure)
        if record.is_miss() {
            self.blacklist.record_miss(&record.market_id);
        }
        if record.is_system_error() {
            self.blacklist.record_failure(&record.market_id);
        }

        // Update potential loss ledger
        if record.is_success() {
            self.loss_ledger.add_entry(record);
        }

        // Update drawdown
        self.drawdown.update_hwm(self.daily.cumulative_pnl());

        // Persist state
        self.persistence.save_state(&self.snapshot()).await?;

        Ok(())
    }

    /// Whether execution is currently halted.
    pub fn is_halted(&self) -> bool {
        self.breaker.is_open()
    }

    /// Resume from a halt (operator action).
    pub async fn resume(&self) -> Result<(), OxideError> {
        self.breaker.reset();
        self.persistence.save_state(&self.snapshot()).await?;
        Ok(())
    }
}
```

---

## 5. CircuitBreaker 状态机

```rust
/// 4-level circuit breaker with automatic recovery.
///
/// ```text
///                  ┌───────────────────────────┐
///                  │                           │
///   ┌─────────┐   │  ┌──────┐   ┌──────────┐ │  ┌───────────┐
///   │ Closed  │──L1──▶│ Open │──cooldown──▶│ HalfOpen │──success──▶│ Recovered │
///   └─────────┘   │  └──────┘   └──────────┘ │  └───────────┘
///       ▲         │      │           │        │       │
///       │         │      │L2/L3/L4   │fail    │       │
///       └─────────│──────┘           └────────│───────┘
///   (after recovery│                          │
///    period)      └───────────────────────────┘
/// ```
pub struct CircuitBreaker {
    state: parking_lot::RwLock<BreakerState>,
    config: CircuitBreakerConfig,
}

#[derive(Debug, Clone)]
pub enum BreakerState {
    Closed,
    Open {
        level: BreakerLevel,
        reason: String,
        opened_at: DateTime<Utc>,
        cooldown_until: DateTime<Utc>,
    },
    HalfOpen {
        level: BreakerLevel,
        probes_remaining: u32,
    },
    Recovered {
        from_level: BreakerLevel,
        recovered_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum BreakerLevel {
    L1, // Per-opportunity static filter failure
    L2, // Rolling window breach (misses, hourly loss, fees)
    L3, // Daily/weekly cap breach
    L4, // Connectivity / balance emergency
}

impl CircuitBreaker {
    pub fn trip(&self, level: BreakerLevel, reason: String) {
        let mut state = self.state.write();
        let cooldown = self.compute_cooldown(level);
        let now = Utc::now();
        *state = BreakerState::Open {
            level,
            reason,
            opened_at: now,
            cooldown_until: now + cooldown,
        };
    }

    /// Called on each trade result to potentially trip or recover.
    pub fn on_trade_result(&self, record: &TradeRecord) {
        let mut state = self.state.write();
        match &*state {
            BreakerState::HalfOpen { level, probes_remaining } => {
                if record.is_success() {
                    if *probes_remaining <= 1 {
                        *state = BreakerState::Recovered {
                            from_level: *level,
                            recovered_at: Utc::now(),
                        };
                    } else {
                        *state = BreakerState::HalfOpen {
                            level: *level,
                            probes_remaining: probes_remaining - 1,
                        };
                    }
                } else {
                    // Failed probe → re-open
                    let cooldown = self.compute_cooldown(*level);
                    let now = Utc::now();
                    *state = BreakerState::Open {
                        level: *level,
                        reason: "HalfOpen probe failed".into(),
                        opened_at: now,
                        cooldown_until: now + cooldown,
                    };
                }
            }
            _ => {}
        }
    }

    /// Periodic tick: transition Open → HalfOpen when cooldown expires.
    pub fn tick(&self, now: DateTime<Utc>) {
        let mut state = self.state.write();
        if let BreakerState::Open { level, cooldown_until, .. } = &*state {
            if now >= *cooldown_until {
                *state = BreakerState::HalfOpen {
                    level: *level,
                    probes_remaining: self.config.half_open_probes,
                };
            }
        }
        if let BreakerState::Recovered { recovered_at, .. } = &*state {
            if now >= *recovered_at + chrono::Duration::seconds(
                self.config.recovery_observation_secs as i64
            ) {
                *state = BreakerState::Closed;
            }
        }
    }

    pub fn is_open(&self) -> bool {
        matches!(
            *self.state.read(),
            BreakerState::Open { .. }
        )
    }

    pub fn current_level(&self) -> Option<BreakerLevel> {
        match &*self.state.read() {
            BreakerState::Open { level, .. } | BreakerState::HalfOpen { level, .. } => {
                Some(*level)
            }
            _ => None,
        }
    }

    fn compute_cooldown(&self, level: BreakerLevel) -> chrono::Duration {
        let base_secs = match level {
            BreakerLevel::L1 => self.config.l1_cooldown_secs,
            BreakerLevel::L2 => self.config.l2_cooldown_secs,
            BreakerLevel::L3 => self.config.l3_cooldown_secs,
            BreakerLevel::L4 => self.config.l4_cooldown_secs,
        };
        chrono::Duration::seconds(base_secs as i64)
    }

    pub fn reset(&self) {
        *self.state.write() = BreakerState::Closed;
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CircuitBreakerConfig {
    pub l1_cooldown_secs: u64,         // Default: 60
    pub l2_cooldown_secs: u64,         // Default: 900 (15 min)
    pub l3_cooldown_secs: u64,         // Default: 3600 (1 hour)
    pub l4_cooldown_secs: u64,         // Default: 7200 (2 hours)
    pub half_open_probes: u32,          // Default: 2
    pub recovery_observation_secs: u64, // Default: 300 (5 min)
}
```

---

## 6. PositionCalculator (Quarter-Kelly + 多约束)

```rust
/// Multi-constraint position sizer.
///
/// Computes the optimal position size as the minimum of:
/// 1. Quarter-Kelly optimal allocation
/// 2. Max single trade USD limit
/// 3. Max per-market exposure limit
/// 4. Max portfolio exposure limit
/// 5. Available balance minus reserve
/// 6. Endgame directional budget remaining
/// 7. Max depth-based size (to avoid excessive market impact)
pub struct MultiConstraintSizer {
    risk_config: RiskConfig,
    sizing_config: PositionSizingConfig,
}

pub struct SizeResult {
    pub size: Option<Usd>,
    pub check: RiskCheckResult,
    pub breakdown: SizeBreakdown,
}

pub struct SizeBreakdown {
    pub kelly_size: Usd,
    pub single_trade_cap: Usd,
    pub market_exposure_cap: Usd,
    pub portfolio_exposure_cap: Usd,
    pub balance_cap: Usd,
    pub directional_budget_cap: Usd,
    pub depth_cap: Usd,
    pub binding_constraint: &'static str,
}

impl MultiConstraintSizer {
    pub async fn compute_size(
        &self,
        opp: &Opportunity<AnyMeta>,
        metrics: &dyn RiskMetrics,
    ) -> SizeResult {
        // 1. Quarter-Kelly
        let kelly = self.quarter_kelly(opp);

        // 2. Single trade limit
        let single_cap = Usd::new(self.risk_config.directional.max_single_bet_usd);

        // 3. Per-market exposure
        let current_market_exp = metrics.market_exposure(&opp.market_id).await;
        // Phase 4 implementation note:
        // cache `market_exposure` / position summary with
        // `CacheKey::PositionSummary { market_id }` once this service owns
        // invalidation after position, trade, and settlement updates.
        let market_cap = Usd::new(
            (self.risk_config.max_single_market_exposure_usd - current_market_exp)
                .max(Decimal::ZERO),
        );

        // 4. Portfolio exposure
        let total_exp = metrics.total_exposure().await;
        let balance = metrics.available_balance().await;
        // Phase 4 implementation note:
        // cache external Polymarket balance snapshots with `CacheKey::Balance`
        // once order submission and confirmation paths own invalidation.
        let max_portfolio = balance * self.risk_config.max_total_exposure_pct / Decimal::from(100);
        let portfolio_cap = Usd::new((max_portfolio - total_exp).max(Decimal::ZERO));

        // 5. Balance - reserve
        let balance_cap = Usd::new(
            (balance - self.risk_config.reserve_balance_usd).max(Decimal::ZERO),
        );

        // 6. Directional budget
        let dir_spent = metrics.directional_daily_spent().await;
        let dir_cap = Usd::new(
            (self.risk_config.directional.daily_budget_usd - dir_spent).max(Decimal::ZERO),
        );

        // 7. Depth cap (max_investment from endgame config)
        let depth_cap = Usd::new(self.sizing_config.endgame_max_investment_usd);

        // Take the minimum
        let candidates = [
            (kelly, "kelly"),
            (single_cap, "single_trade_cap"),
            (market_cap, "market_exposure_cap"),
            (portfolio_cap, "portfolio_exposure_cap"),
            (balance_cap, "balance_cap"),
            (dir_cap, "directional_budget_cap"),
            (depth_cap, "depth_cap"),
        ];

        let (min_size, binding) = candidates
            .iter()
            .min_by_key(|(s, _)| *s)
            .unwrap();

        let approved = min_size.inner() > Decimal::ZERO;

        SizeResult {
            size: if approved { Some(*min_size) } else { None },
            check: RiskCheckResult {
                check_name: "position_sizing",
                passed: approved,
                detail: format!(
                    "Size={}, binding={}",
                    min_size, binding
                ),
            },
            breakdown: SizeBreakdown {
                kelly_size: kelly,
                single_trade_cap: single_cap,
                market_exposure_cap: market_cap,
                portfolio_exposure_cap: portfolio_cap,
                balance_cap: balance_cap,
                directional_budget_cap: dir_cap,
                depth_cap: depth_cap,
                binding_constraint: binding,
            },
        }
    }

    /// Quarter-Kelly optimal fraction.
    ///
    /// Kelly fraction f* = (p × b - q) / b
    /// where:
    ///   p = fused resolution probability
    ///   q = 1 - p
    ///   b = net odds (payout_if_correct / cost - 1)
    ///
    /// We use f*/4 (quarter-Kelly) for conservative sizing.
    fn quarter_kelly(&self, opp: &Opportunity<AnyMeta>) -> Usd {
        let p = opp.resolution_adjust;
        let q = Decimal::ONE - p;

        // For endgame: buy at `entry_price`, payout = 1.0 if correct
        // b = (1.0 / entry_price) - 1
        let entry_price = opp.legs.first()
            .map(|l| l.price.inner())
            .unwrap_or(Decimal::ONE);

        if entry_price.is_zero() {
            return Usd::ZERO;
        }

        let b = Decimal::ONE / entry_price - Decimal::ONE;
        if b.is_zero() {
            return Usd::ZERO;
        }

        let kelly_fraction = (p * b - q) / b;
        if kelly_fraction <= Decimal::ZERO {
            return Usd::ZERO;
        }

        let quarter = kelly_fraction * self.sizing_config.kelly_fraction;
        let bankroll = self.sizing_config.bankroll_usd;

        Usd::new((quarter * bankroll).round_dp(2))
    }
}
```

---

## 7. 数据管线 (oxide-arb-core)

```rust
/// Processes raw WebSocket events into structured orderbook state and
/// triggers downstream detection.
pub struct DataPipeline {
    book_store: Arc<BookStore>,
    market_registry: Arc<MarketRegistry>,
    coalescer: Arc<Coalescer>,
    metrics: Arc<MetricsHub>,
}

impl DataPipeline {
    /// Main event processing loop.
    ///
    /// Reads from the WS event channel and dispatches to the appropriate handler.
    pub async fn run(
        &self,
        mut rx: flume::Receiver<WsEvent>,
        shutdown: CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                event = rx.recv_async() => {
                    match event {
                        Ok(ws_event) => self.handle_event(ws_event).await,
                        Err(_) => break, // Channel closed
                    }
                }
            }
        }
    }

    async fn handle_event(&self, event: WsEvent) {
        match event {
            WsEvent::BookSnapshot { token_id, bids, asks, timestamp } => {
                self.book_store.apply_snapshot(&token_id, bids, asks, timestamp);
                self.metrics.ws_events_total.inc();
                self.trigger_detection(&token_id);
            }
            WsEvent::PriceChange { token_id, changes, timestamp } => {
                self.book_store.apply_delta(&token_id, changes, timestamp);
                self.metrics.ws_events_total.inc();
                self.trigger_detection(&token_id);
            }
            WsEvent::MarketResolved { market_id, winning_token_id } => {
                self.market_registry.mark_resolved(&market_id, &winning_token_id);
                self.metrics.markets_resolved_total.inc();
            }
            WsEvent::ConnectionStatus { shard_id, connected } => {
                self.metrics.ws_connected.set(if connected { 1 } else { 0 });
                tracing::info!(shard_id, connected, "WS connection status change");
            }
            _ => {}
        }
    }

    fn trigger_detection(&self, token_id: &TokenId) {
        if let Some(market_id) = self.market_registry.token_to_market(token_id) {
            self.coalescer.schedule_market_scan(market_id);
        }
    }
}
```

### 7.1 BookStore

```rust
use dashmap::DashMap;
use parking_lot::RwLock;

pub struct BookStore {
    books: DashMap<TokenId, Arc<RwLock<OrderBook>>>,
}

impl BookStore {
    pub fn get(&self, token_id: &TokenId) -> Option<OrderBookSnapshot> {
        self.books.get(token_id).map(|book| book.read().snapshot())
    }

    pub fn apply_snapshot(
        &self,
        token_id: &TokenId,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        timestamp: u64,
    ) {
        self.books
            .entry(token_id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(OrderBook::new())))
            .write()
            .replace(bids, asks, timestamp);
    }

    pub fn apply_delta(
        &self,
        token_id: &TokenId,
        changes: Vec<PriceLevelChange>,
        timestamp: u64,
    ) {
        if let Some(book) = self.books.get(token_id) {
            book.write().apply_changes(changes, timestamp);
        }
    }
}
```

---

## 8. 执行管线

```rust
/// Orchestrates the full execution lifecycle for a scored opportunity.
///
/// Pipeline stages: validate → size → plan → dispatch → confirm → audit
pub struct ExecutionPipeline {
    validator: Validator,
    risk_engine: Arc<RiskEngine>,
    plan_builder: PlanBuilder,
    dispatcher: Dispatcher,
    audit_writer: Arc<AsyncWriter<OpportunityAuditRow>>,
    state_machine: Arc<ExecutionFSM>,
    capital: Arc<CapitalManager>,
}

pub struct ExecutionResult {
    pub execution_id: ExecutionId,
    pub trades: Vec<TradeRecord>,
    pub outcome: ExecutionOutcome,
    pub latency_ms: u64,
}

pub enum ExecutionOutcome {
    Success { pnl: Usd },
    PartialFill { filled_pct: Decimal },
    Rejected { reason: String },
    Failed { error: String },
}

impl ExecutionPipeline {
    pub async fn execute(
        &self,
        scored: ScoredOpportunity,
        mode: ExecutionMode,
    ) -> ExecutionResult {
        let start = std::time::Instant::now();
        let execution_id = ExecutionId::new(uuid::Uuid::now_v7().to_string());

        // 1. Validate (freshness + risk pre-check)
        self.state_machine.transition(ExecState::Validate);
        let validation = self.validator.validate(&scored.opportunity).await;
        if let Err(reason) = validation {
            self.state_machine.transition(ExecState::Idle);
            return ExecutionResult::rejected(execution_id, reason, start.elapsed());
        }

        // 2. Risk pre-trade check + sizing
        let risk_decision = self.risk_engine
            .pre_trade_check(&scored.opportunity.erase(), mode == ExecutionMode::DryRun)
            .await;

        if !risk_decision.approved {
            self.state_machine.transition(ExecState::Idle);
            return ExecutionResult::rejected(
                execution_id,
                risk_decision.denial_reason.unwrap_or_default(),
                start.elapsed(),
            );
        }

        let approved_size = risk_decision.approved_size.unwrap();

        // 3. Reserve capital
        let reservation = self.capital.reserve(approved_size).await;
        if reservation.is_err() {
            self.state_machine.transition(ExecState::Idle);
            return ExecutionResult::rejected(
                execution_id,
                "Capital reservation failed".into(),
                start.elapsed(),
            );
        }

        // 4. Build execution plan
        let plan = self.plan_builder.build(
            &scored.opportunity,
            approved_size,
            mode,
        );

        // 5. Dispatch
        self.state_machine.transition(ExecState::Exec);
        let dispatch_result = self.dispatcher.dispatch(&plan, mode).await;

        // 6. Handle result
        let result = match dispatch_result {
            Ok(trades) => {
                let pnl = trades.iter().map(|t| t.realized_pnl()).sum::<Decimal>();

                // Notify risk engine
                for trade in &trades {
                    self.risk_engine.on_trade_result(trade).await.ok();
                }

                self.state_machine.transition(ExecState::Idle);
                self.capital.release(reservation.unwrap()).await;

                ExecutionResult {
                    execution_id,
                    trades,
                    outcome: ExecutionOutcome::Success { pnl: Usd::new(pnl) },
                    latency_ms: start.elapsed().as_millis() as u64,
                }
            }
            Err(err) => {
                self.state_machine.transition(ExecState::Idle);
                self.capital.release(reservation.unwrap()).await;

                ExecutionResult {
                    execution_id,
                    trades: vec![],
                    outcome: ExecutionOutcome::Failed { error: err.to_string() },
                    latency_ms: start.elapsed().as_millis() as u64,
                }
            }
        };

        // 7. Audit trail (async, non-blocking)
        self.audit_writer.write(result.to_audit_row()).await.ok();

        result
    }
}
```

---

## 9. 执行状态机

> **ADR-001**: Endgame 不对冲。实现时 **删除 `Hedging`**；`Emergency` 仅用于系统级故障（API 全不可用、DB 损坏等），非对冲失败。EXEC 失败直接回 IDLE 并上报 risk engine。

```rust
/// Execution finite state machine (endgame, no hedge path).
///
/// ```text
/// IDLE ──▶ VALIDATE ──▶ EXEC ──▶ IDLE (success)
///   │         │           │
///   │         │ fail      │ fail / miss
///   │         ▼           ▼
///   │       IDLE       IDLE (+ risk / audit)
///   │
///   └── (global fault) ──▶ EMERGENCY ──▶ IDLE (operator resume)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecState {
    Idle,
    Validate,
    Exec,
    Emergency,
}

pub struct ExecutionFSM {
    state: parking_lot::RwLock<ExecState>,
    last_transition: parking_lot::RwLock<DateTime<Utc>>,
}

impl ExecutionFSM {
    pub fn transition(&self, target: ExecState) {
        let current = *self.state.read();
        let valid = match (current, target) {
            (ExecState::Idle, ExecState::Validate) => true,
            (ExecState::Validate, ExecState::Exec) => true,
            (ExecState::Validate, ExecState::Idle) => true,   // validation failure
            (ExecState::Exec, ExecState::Idle) => true,       // terminal (success or miss)
            (ExecState::Emergency, ExecState::Idle) => true,  // operator resume
            _ => false,
        };

        if valid {
            *self.state.write() = target;
            *self.last_transition.write() = Utc::now();
            tracing::info!(from = ?current, to = ?target, "Execution FSM transition");
        } else {
            tracing::error!(from = ?current, to = ?target, "Invalid FSM transition");
        }
    }

    pub fn current(&self) -> ExecState {
        *self.state.read()
    }

    pub fn is_idle(&self) -> bool {
        *self.state.read() == ExecState::Idle
    }
}
```

---

## 10. FOK + GTD 分层执行策略

```rust
/// Tiered execution strategy for endgame orders.
///
/// Tier 1: FOK (Fill-or-Kill) at the detected price
///   - Fastest execution, highest fill certainty
///   - Fails if liquidity has moved since detection
///
/// Tier 2: GTD (Good-til-Date) with short expiry
///   - Used when FOK fails due to minor price movement
///   - Expiry: 30 seconds
///   - Price: detected price + 1 tick tolerance
///
/// Tier 3: GTD with longer expiry + price improvement
///   - Used for larger orders that need time to fill
///   - Expiry: 5 minutes
///   - Price: detected price + 2 ticks tolerance
pub struct TieredExecutionStrategy {
    config: ExecutionConfig,
}

pub struct ExecutionTier {
    pub order_type: OrderType,
    pub price_adjustment_ticks: i32,
    pub expiry_secs: Option<u64>,
    pub max_retries: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum OrderType {
    Fok,
    Gtd { expiry_secs: u64 },
}

impl TieredExecutionStrategy {
    pub fn tiers(&self) -> Vec<ExecutionTier> {
        vec![
            ExecutionTier {
                order_type: OrderType::Fok,
                price_adjustment_ticks: 0,
                expiry_secs: None,
                max_retries: 1,
            },
            ExecutionTier {
                order_type: OrderType::Gtd { expiry_secs: 30 },
                price_adjustment_ticks: 1,
                expiry_secs: Some(30),
                max_retries: 1,
            },
            ExecutionTier {
                order_type: OrderType::Gtd { expiry_secs: 300 },
                price_adjustment_ticks: 2,
                expiry_secs: Some(300),
                max_retries: 1,
            },
        ]
    }

    /// Execute with tier fallback.
    ///
    /// Tries each tier in order. On FOK rejection (not filled), falls through
    /// to the next tier. On API error, retries within the tier.
    pub async fn execute(
        &self,
        plan: &ExecutionPlan,
        api: &dyn OrderApi,
        mode: ExecutionMode,
    ) -> Result<TradeRecord, OxideError> {
        for tier in self.tiers() {
            let adjusted_price = self.adjust_price(
                plan.entry_price,
                plan.tick_size,
                tier.price_adjustment_ticks,
            );

            for attempt in 0..=tier.max_retries {
                let order = OrderRequest {
                    token_id: plan.token_id.clone(),
                    side: plan.side,
                    price: adjusted_price,
                    shares: plan.shares,
                    order_type: tier.order_type,
                };

                match mode {
                    ExecutionMode::DryRun => {
                        return Ok(TradeRecord::simulated(&order, plan));
                    }
                    ExecutionMode::Paper => {
                        return Ok(TradeRecord::paper(&order, plan));
                    }
                    ExecutionMode::Live => {
                        match api.place_order(&order).await {
                            Ok(response) if response.is_filled() => {
                                return Ok(TradeRecord::from_response(&order, &response, plan));
                            }
                            Ok(response) if response.is_rejected() => {
                                tracing::debug!(
                                    tier = ?tier.order_type,
                                    attempt,
                                    "Order rejected, trying next tier"
                                );
                                break; // Try next tier
                            }
                            Ok(_) => {
                                // Partially filled or pending — wait for expiry
                                if let Some(expiry) = tier.expiry_secs {
                                    tokio::time::sleep(Duration::from_secs(expiry)).await;
                                }
                                break;
                            }
                            Err(e) if attempt < tier.max_retries => {
                                tracing::warn!(error = %e, attempt, "Order API error, retrying");
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                continue;
                            }
                            Err(e) => {
                                tracing::error!(error = %e, "Order API error, exhausted retries");
                                break; // Try next tier
                            }
                        }
                    }
                }
            }
        }

        Err(OxideError::Execution("All execution tiers exhausted".into()))
    }

    fn adjust_price(&self, base: Price, tick: TickSize, ticks: i32) -> Price {
        let adjustment = Decimal::from(ticks) * tick.as_decimal();
        Price::new((base.inner() - adjustment).max(Decimal::ZERO))
    }
}
```

---

## 11. AppContext DI 设计

```rust
use std::sync::Arc;

/// Dependency injection container for the application.
///
/// All services are Arc-wrapped for shared ownership across tasks.
/// Constructed once at startup, passed to all subsystems.
pub struct AppContext {
    // Configuration
    pub config: Arc<Settings>,

    // Data layer
    pub book_store: Arc<BookStore>,
    pub market_registry: Arc<MarketRegistry>,
    pub market_cache: Arc<MarketCache>,

    // API
    pub clob_ws: Arc<ClobWsManager>,
    pub gamma_client: Arc<GammaClient>,
    pub fee_service: Arc<dyn FeeService>,
    pub oracle: Arc<VotingOracle>,
    pub order_api: Arc<dyn OrderApi>,

    // Storage
    pub pg_pool: Arc<PostgresPool>,
    pub ch_pool: Arc<ClickHousePool>,
    pub cache: Arc<TieredCache>,

    // Repositories
    pub market_repo: Arc<dyn MarketRepository>,
    pub trade_repo: Arc<dyn TradeRepository>,
    pub position_repo: Arc<dyn PositionRepository>,
    pub calibration_repo: Arc<dyn CalibrationRepository>,
    pub risk_state_repo: Arc<dyn RiskStateRepository>,
    pub timeseries_repo: Arc<dyn TimeseriesRepository>,
    pub accounting_repo: Arc<dyn AccountingRepository>,

    // Algorithm
    pub calibrator: Arc<ResolutionCalibrator>,
    pub opportunity_pipeline: Arc<OpportunityPipeline>,

    // Risk
    pub risk_engine: Arc<RiskEngine>,

    // Execution
    pub execution_pipeline: Arc<ExecutionPipeline>,
    pub execution_fsm: Arc<ExecutionFSM>,

    // Infrastructure
    pub metrics: Arc<MetricsHub>,
    pub alert_dispatcher: Arc<AlertDispatcher>,
    pub task_registry: Arc<TaskRegistry>,
    pub shutdown: CancellationToken,
}

impl AppContext {
    /// Build the full application context.
    ///
    /// Order matters: dependencies must be constructed before dependents.
    pub async fn build(config: Settings) -> Result<Self, OxideError> {
        let config = Arc::new(config);
        let shutdown = CancellationToken::new();

        // 1. Storage layer
        let pg_pool = Arc::new(PostgresPool::connect(&config.db.postgres).await?);
        let ch_pool = Arc::new(ClickHousePool::connect(&config.analytics).await?);
        let cache = Arc::new(TieredCache::new(&config.cache).await?);

        // 2. Run migrations
        Migrator::up(pg_pool.connection(), None).await?;
        ch_pool.ensure_schema().await?;

        // 3. Repositories
        let market_repo: Arc<dyn MarketRepository> = Arc::new(
            CachedMarketRepository::new(
                PgMarketRepository::new(pg_pool.connection()),
                cache.clone(),
            ),
        );
        // ... (other repositories)

        // 4. API layer
        // ... (ClobWsManager, GammaClient, etc.)

        // 5. Algorithm
        let cal_entries = calibration_repo.get_all_buckets().await?;
        let calibrator = Arc::new(ResolutionCalibrator::from_entries(
            cal_entries,
            config.detection.endgame.calibration.clone(),
        ));
        // ... (OpportunityPipeline)

        // 6. Risk engine
        let risk_engine = Arc::new(RiskEngine::new(
            config.risk.clone(),
            // ... metrics, persistence
        ));

        // 7. Data + Execution pipelines
        // ...

        todo!("Full wiring — see implementation")
    }
}
```

---

## 12. 可观测性

### 12.1 MetricsHub

```rust
use prometheus::{
    Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramVec, Registry,
    opts, register_counter_vec_with_registry,
    register_gauge_with_registry,
    register_histogram_vec_with_registry,
};

pub struct MetricsHub {
    pub registry: Registry,

    // Data pipeline
    pub ws_events_total: Counter,
    pub ws_connected: Gauge,
    pub book_update_latency: Histogram,

    // Detection
    pub opportunities_detected: CounterVec,    // labels: strategy
    pub opportunities_filtered: CounterVec,    // labels: strategy, reason
    pub detection_latency: HistogramVec,       // labels: strategy

    // Execution
    pub trades_total: CounterVec,              // labels: strategy, outcome, mode
    pub trade_pnl: HistogramVec,               // labels: strategy
    pub execution_latency: HistogramVec,       // labels: strategy, tier

    // Risk
    pub circuit_breaker_trips: CounterVec,     // labels: level
    pub circuit_breaker_level: Gauge,
    pub daily_loss_usd: Gauge,
    pub weekly_loss_usd: Gauge,
    pub open_positions: Gauge,
    pub total_exposure_usd: Gauge,
    pub blacklisted_markets: Gauge,

    // Calibration
    pub calibration_buckets_total: Gauge,
    pub calibration_outcomes_pending: Gauge,
    pub calibration_posterior_mean: GaugeVec,  // labels: category, zone

    // Cache
    pub cache_hits: CounterVec,                // labels: level, domain
    pub cache_misses: CounterVec,              // labels: domain

    // System
    pub uptime_secs: Gauge,
    pub balance_usd: Gauge,
}
```

### 12.2 AlertDispatcher

```rust
pub struct AlertDispatcher {
    telegram: Option<TelegramAlert>,
    webhook: Option<WebhookAlert>,
}

pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
    Emergency,
}

pub struct Alert {
    pub severity: AlertSeverity,
    pub title: String,
    pub message: String,
    pub timestamp: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}

impl AlertDispatcher {
    pub async fn send(&self, alert: Alert) {
        // Telegram for Warning+
        if alert.severity >= AlertSeverity::Warning {
            if let Some(tg) = &self.telegram {
                tg.send(&alert).await.ok();
            }
        }
        // Webhook for all
        if let Some(wh) = &self.webhook {
            wh.send(&alert).await.ok();
        }
    }
}
```

### 12.3 ReportGenerator

```rust
pub struct ReportGenerator {
    accounting_repo: Arc<dyn AccountingRepository>,
    alert_dispatcher: Arc<AlertDispatcher>,
}

impl ReportGenerator {
    /// Generate and send daily report.
    ///
    /// Contents:
    /// - PnL summary (realized + unrealized)
    /// - Trade count by outcome (success/miss/error)
    /// - Win rate
    /// - Max drawdown
    /// - Top 3 most profitable markets
    /// - Calibration accuracy stats
    /// - Circuit breaker trip count
    pub async fn daily_report(&self) -> Result<DailyReport, OxideError> {
        let period = self.accounting_repo.get_current_daily().await?;
        // ... aggregate and format
        todo!()
    }

    /// Generate and send weekly report.
    ///
    /// Additional contents beyond daily:
    /// - Week-over-week PnL trend
    /// - Sharpe ratio
    /// - Calibration bucket accuracy by category
    /// - System uptime percentage
    pub async fn weekly_report(&self) -> Result<WeeklyReport, OxideError> {
        todo!()
    }
}
```

---

## 13. Cargo.toml

### 13.1 oxide-arb-risk

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

### 13.2 oxide-arb-core

```toml
[package]
name = "oxide-arb-core"
description = "Core engine: data pipeline, detection, execution, and observability"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[dependencies]
oxide-arb-error = { workspace = true }
oxide-arb-models = { workspace = true }
oxide-arb-api = { workspace = true }
oxide-arb-storage = { workspace = true }
oxide-arb-repository = { workspace = true }
oxide-arb-algorithm = { workspace = true }
oxide-arb-risk = { workspace = true }

tokio = { workspace = true }
tokio-util = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
rust_decimal = { workspace = true }
rust_decimal_macros = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
tracing-appender = { workspace = true }
chrono = { workspace = true }
thiserror = { workspace = true }
prometheus = { workspace = true }
async-trait = { workspace = true }
flume = { workspace = true }
uuid = { workspace = true }
parking_lot = { workspace = true }
dashmap = { workspace = true }
arc-swap = { workspace = true }
moka = { workspace = true }
backoff = { workspace = true }
reqwest = { workspace = true }
teloxide = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
criterion = { workspace = true }
tempfile = { workspace = true }
oxide-arb-storage = { workspace = true, features = ["test-util"] }

[[bench]]
name = "order_book_bench"
harness = false

[[bench]]
name = "detection_bench"
harness = false

[[bench]]
name = "e2e_latency_bench"
harness = false

[lints]
workspace = true
```

---

## 14. 验收检查清单

### oxide-arb-risk

- [ ] `CircuitBreaker` Closed → Open(L1) 在静态限制违规时触发
- [ ] `CircuitBreaker` Open → HalfOpen 在 cooldown 过期时触发
- [ ] `CircuitBreaker` HalfOpen → Recovered 在 N 次成功 probe 后触发
- [ ] `CircuitBreaker` HalfOpen → Open 在 probe 失败时触发
- [ ] `CircuitBreaker` Recovered → Closed 在 observation 期后触发
- [ ] `CircuitBreaker::reset()` 任意状态 → Closed
- [ ] L2 cooldown 指数递增直到 max_cooldown_secs 上限
- [ ] `DailyAccounting` 在 UTC 午夜自动 rollover
- [ ] `WeeklyAccounting` 在 UTC 周一 00:00 自动 rollover
- [ ] `BlacklistManager` 临时黑名单按 TTL 自动过期
- [ ] `BlacklistManager` 永久黑名单不过期
- [ ] `MultiConstraintSizer` 输出为所有约束的最小值
- [ ] `quarter_kelly()` 在 p ≤ q/b 时输出 0（负 Kelly → 不下注）
- [ ] `quarter_kelly()` 输出与手动计算一致（精确到 2 位小数）
- [ ] `EndgameRiskRules` 验证 directional_position_count < max_concurrent
- [ ] `EndgameRiskRules` 验证 directional_daily_spent < daily_budget
- [ ] `DrawdownGuard` 在回撤超过阈值时阻断新交易
- [ ] `PotentialLossLedger` 正确追踪未结算持仓的最大潜在损失
- [ ] `LedgerReconciler` 自动解决过期条目
- [ ] 全部 trait 方法有 `Send + Sync` bound

### oxide-arb-core

- [ ] `DataPipeline` 正确处理 BookSnapshot → BookStore 更新 → detection trigger
- [ ] `DataPipeline` 正确处理 MarketResolved 事件
- [ ] `BookStore::apply_delta` 正确增量更新 orderbook
- [ ] `Coalescer` 在 coalesce window 内合并多次 trigger 为一次 scan
- [ ] `ExecutionPipeline` 端到端: detect → validate → size → plan → dispatch → audit
- [ ] `ExecutionFSM` 只允许合法状态转换
- [ ] `ExecutionFSM` 非法转换记录 error 日志但不 panic
- [ ] FOK → GTD(30s) → GTD(5min) 分层执行 fallback 正确
- [ ] FOK 在 DryRun 模式下返回 simulated TradeRecord
- [ ] `CapitalManager` reserve 后 balance 减少，release 后恢复
- [ ] `AppContext::build()` 按依赖序构造所有组件
- [ ] `TaskRegistry` 在 shutdown 时等待所有 task 完成（30s 超时）
- [ ] `MetricsHub` 所有 counter/gauge 可被 prometheus scrape
- [ ] `AlertDispatcher` Telegram 发送在 Warning+ 级别触发
- [ ] `HealthChecker` WS 断线超过阈值时返回 unhealthy
- [ ] `PeriodicTask` 支持 jitter 防止任务对齐
- [ ] Paper-trade 模式完整跑通不触发真实下单

---

## 15. 测试策略

### 15.1 oxide-arb-risk 单元测试

| 模块 | 测试点 |
|---|---|
| `CircuitBreaker` | 全部 FSM 转迁路径（6 条边） |
| `CircuitBreaker` | tick() 在正确时间触发转迁 |
| `DailyAccounting` | rollover 边界 23:59:59 → 00:00:00 UTC |
| `WeeklyAccounting` | 周日 → 周一 rollover |
| `BlacklistManager` | 临时黑名单 TTL 过期后 check() 返回 clear |
| `MultiConstraintSizer` | 各约束独立设为最小值时正确 binding |
| `quarter_kelly` | p=0.9, entry=0.97 → 手动验证 |
| `quarter_kelly` | p=0.5, entry=0.97 → 输出 0（无 edge） |
| `DrawdownGuard` | HWM 更新 + 回撤超限阻断 |

### 15.2 oxide-arb-core 集成测试

| 场景 | 描述 |
|---|---|
| Data pipeline E2E | 模拟 WS events → 验证 BookStore 状态 |
| Detection trigger | 价格更新 → coalescer → scanner → opportunity |
| Execution happy path | Paper mode: scored opp → validate → size → plan → dispatch → audit |
| Execution rejection | Risk denied → pipeline short-circuit |
| FSM boundary | 非法状态转换不 panic |
| Shutdown | CancellationToken → 所有 task 完成 |

### 15.3 Benchmark

```rust
#[bench]
fn bench_orderbook_apply_delta(b: &mut Bencher) {
    // Pre-build a 50-level orderbook, apply 10 random changes
    // Target: < 1μs per apply
}

#[bench]
fn bench_endgame_detect(b: &mut Bencher) {
    // Pre-build market + orderbook, run detect()
    // Target: < 10μs per market
}

#[bench]
fn bench_risk_pre_trade_check(b: &mut Bencher) {
    // Full risk check with mock metrics
    // Target: < 100μs
}
```

---

## 16. 预估工作量

| 组件 | 源码 LoC | 测试 LoC |
|---|---|---|
| **oxide-arb-risk** | | |
| `engine.rs` + `traits.rs` | ~400 | ~200 |
| `circuit_breaker/` | ~350 | ~400 |
| `accounting/` | ~300 | ~250 |
| `position/` | ~250 | ~200 |
| `limits/` | ~200 | ~150 |
| `blacklist/` | ~250 | ~200 |
| `sizing/` | ~500 | ~400 |
| `endgame/` | ~150 | ~150 |
| `reconciliation/` | ~200 | ~150 |
| **小计** | **~2,600** | **~2,100** |
| **oxide-arb-core** | | |
| `app/` (context + lifecycle + registry) | ~600 | ~200 |
| `data/` (book, store, registry, pipeline) | ~1,200 | ~600 |
| `detection/` (scanner, coalescer, funnel) | ~500 | ~400 |
| `execution/` (pipeline, FSM, dispatcher, tiers) | ~1,200 | ~800 |
| `infra/` (writers, periodic, health, retry) | ~600 | ~300 |
| `observability/` (metrics, alerts, reports) | ~500 | ~200 |
| **小计** | **~4,600** | **~2,500** |
| **Phase 4 合计** | **~7,200** | **~4,600** |

---

## 补充设计 A：Outbox EventStore + Flusher

> 基于 Migration #012 (`opportunity_lifecycle_event` + `outbox_event` 表) 的上层实现设计。

### A.1 EventStore trait

```rust
/// 事件持久化接口。在同一个 DB 事务中写入 lifecycle event + outbox row。
#[async_trait]
pub trait EventStore: Send + Sync {
    /// 追加一条生命周期事件，同时写入 outbox。
    /// 返回持久化后的 event_id。
    async fn append(
        &self,
        txn: &DatabaseTransaction,
        event: NewLifecycleEvent,
    ) -> Result<String, StorageError>;

    /// 批量追加（用于回放修正场景）
    async fn append_batch(
        &self,
        txn: &DatabaseTransaction,
        events: Vec<NewLifecycleEvent>,
    ) -> Result<Vec<String>, StorageError>;

    /// 获取某个 opportunity 的完整事件流
    async fn get_lifecycle(
        &self,
        opportunity_id: &OpportunityId,
    ) -> Result<Vec<LifecycleEventModel>, StorageError>;
}

pub struct NewLifecycleEvent {
    pub opportunity_id: OpportunityId,
    pub execution_id: Option<ExecutionId>,
    pub phase: LifecyclePhase,
    pub phase_data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy)]
pub enum LifecyclePhase {
    Detected,
    Scored,
    SizingApproved,
    ExposureReserved,
    OrderSubmitted,
    OrderConfirmed,
    OrderFailed,
    Settled,
    Expired,
    Cancelled,
}
```

### A.2 OutboxFlusher

```rust
/// 后台任务：定期扫描 outbox_event 表中未发布的行，
/// 转发给内部消费者（metrics, CH writer, alert dispatcher）。
pub struct OutboxFlusher {
    db: DatabaseConnection,
    consumers: Vec<Box<dyn OutboxConsumer>>,
    poll_interval: Duration,
    batch_size: usize,
    max_retries: u32,
}

#[async_trait]
pub trait OutboxConsumer: Send + Sync {
    async fn consume(&self, events: &[OutboxPayload]) -> Result<(), anyhow::Error>;
}

impl OutboxFlusher {
    pub async fn run(&self, shutdown: CancellationToken) {
        // 1. SELECT * FROM outbox_event WHERE published_at IS NULL
        //    ORDER BY created_at LIMIT batch_size FOR UPDATE SKIP LOCKED
        // 2. 对每条 event 调用所有 consumers
        // 3. 成功: UPDATE published_at = NOW()
        // 4. 失败: UPDATE publish_attempts += 1, last_error = ...
        // 5. 超过 max_retries: 告警 + 标记 dead-letter (published_at = epoch)
    }
}
```

**关键设计决策**:
- 使用 `FOR UPDATE SKIP LOCKED` 确保单进程下幂等性，未来多实例也能安全工作
- Consumers 失败不阻塞其他 consumers（独立错误处理）
- Dead-letter 策略：超过 max_retries 后设置 `published_at` 为 epoch（1970-01-01）标记已放弃

---

## 补充设计 B：Exposure Reservation（内存方案）

> 单进程部署，不需要分布式锁或 DB 级 reservation。使用 `DashMap` 实现内存级别的快速预留。

### B.1 ExposureReservationManager

```rust
use dashmap::DashMap;
use std::sync::Arc;

pub struct ExposureReservationManager {
    /// reservation_id -> ReservationEntry
    reservations: DashMap<ReservationId, ReservationEntry>,
    /// 当前总预留暴露额 (atomic for fast reads)
    total_reserved: AtomicU64, // 以 cents 存储避免浮点
    /// 配置上限
    max_total_exposure_cents: u64,
}

struct ReservationEntry {
    market_id: MarketId,
    token_id: TokenId,
    amount_cents: u64,
    created_at: Instant,
    ttl: Duration,
}

impl ExposureReservationManager {
    /// 尝试预留资金。如果超过上限则返回 Err。
    /// 预留有 TTL，超时后自动释放（通过后台 gc task）。
    pub fn try_reserve(
        &self,
        market_id: &MarketId,
        token_id: &TokenId,
        amount_usd: Usd,
        ttl: Duration,
    ) -> Result<ReservationId, ReservationError>;

    /// 确认预留（交易成功后释放 reservation，暴露转为 position）
    pub fn confirm(&self, reservation_id: &ReservationId) -> Result<(), ReservationError>;

    /// 显式释放（交易取消/失败）
    pub fn release(&self, reservation_id: &ReservationId) -> Result<(), ReservationError>;

    /// 当前总预留额
    pub fn total_reserved_usd(&self) -> Usd;

    /// 后台 GC：清理过期 reservation
    pub async fn gc_loop(&self, interval: Duration, shutdown: CancellationToken);
}
```

**设计要点**:
- `DashMap` 提供无锁并发 (sharded lock)
- TTL 防止 orphan reservations（进程崩溃重启后所有 reservation 自然过期）
- `total_reserved` 使用 `AtomicU64` 以 cents 单位，读取无锁
- 集成点：`RiskEngine.pre_trade_check()` 调用 `try_reserve()`

---

## 补充设计 C：LedgerReconciler

> 定期对账：比较 `position` 表 + `potential_loss_ledger` + Polymarket on-chain 余额，
> 确保系统状态一致。

### C.1 ReconciliationReport

```rust
pub struct ReconciliationReport {
    pub checked_at: DateTime<Utc>,
    pub positions_checked: usize,
    pub mismatches: Vec<ReconciliationMismatch>,
    pub total_drift_usd: Usd,
    pub status: ReconciliationStatus,
}

pub enum ReconciliationMismatch {
    SharesMismatch {
        market_id: MarketId,
        token_id: TokenId,
        db_shares: Shares,
        chain_shares: Shares,
    },
    OrphanPosition {
        position_id: PositionId,
        market_id: MarketId,
    },
    UnrecordedHolding {
        token_id: TokenId,
        chain_shares: Shares,
    },
    PnlDrift {
        market_id: MarketId,
        db_pnl: Usd,
        computed_pnl: Usd,
    },
}

pub enum ReconciliationStatus {
    Clean,
    DriftWithinTolerance,
    DriftExceedsTolerance,
    Critical,
}
```

### C.2 LedgerReconciler

```rust
pub struct LedgerReconciler {
    position_repo: Arc<dyn PositionRepository>,
    polymarket_api: Arc<dyn BalanceQuerier>,
    alert_dispatcher: Arc<AlertDispatcher>,
    tolerance_usd: Usd,
    check_interval: Duration,
}

impl LedgerReconciler {
    pub async fn reconcile(&self) -> Result<ReconciliationReport, StorageError> {
        // 1. 获取所有 open positions from DB
        // 2. 获取 on-chain token balances via API
        // 3. 交叉匹配，找出差异
        // 4. 计算 total drift
        // 5. 如果超过 tolerance，触发告警
        // 6. 生成报告
    }

    pub async fn run_periodic(&self, shutdown: CancellationToken) {
        // 定期执行 reconcile()
        // 将报告写入 lifecycle_event 表
        // Critical 级别触发 circuit breaker
    }
}
```

**集成点**:
- `CircuitBreaker`: 对账 Critical -> 升级到 Level 3 (HaltNew) 或 Level 4 (Emergency)
- `AlertDispatcher`: 所有 mismatch 发送通知
- 调度频率：默认每 5 分钟一次，可通过 `RuntimeConfig` 调整

**缓存延期项**:
- `CacheKey::Balance` 不能在 repository 层提前实现；它依赖 `BalanceQuerier` / wallet service 的外部快照读取，以及订单提交、成交确认、reconcile 后的失效路径。
- Phase 4 实现 `BalanceQuerier` 时必须把 `CacheKey::Balance` 接入 service 层，并在所有改变可用余额的路径后 invalidate。
- `CacheKey::PositionSummary { market_id }` 必须由风险/持仓服务持有，因为失效依赖 position lifecycle、trade outcome、settlement/reconcile 多条写路径。

---

## 补充设计 D：DrawdownManager

```rust
pub struct DrawdownManager {
    /// 会计期起始余额
    period_start_equity: Usd,
    /// 当前最高水位线
    high_water_mark: Usd,
    /// 日内最大回撤阈值（超过则触发熔断）
    max_intraday_drawdown_pct: f64,
    /// 滚动周（7日）最大回撤
    max_weekly_drawdown_pct: f64,
}

impl DrawdownManager {
    /// 更新当前权益，检查是否触发回撤保护
    pub fn update_equity(&mut self, current_equity: Usd) -> DrawdownAction;

    /// 当前回撤百分比
    pub fn current_drawdown_pct(&self) -> f64;

    /// 重置水位线（新会计期开始）
    pub fn reset_period(&mut self, new_equity: Usd);
}

pub enum DrawdownAction {
    Normal,
    Warning { pct: f64 },
    HaltNew { pct: f64 },
    Emergency { pct: f64 },
}
```

---

## Phase 4 补充 — 关键缺口修补（Phase 4+ 计划）

### P1. ExposureReservation 并发安全更新

补充设计 B 中的 `try_reserve` 必须使用 AtomicU64 CAS loop 保证原子性（避免 check-then-act 竞态）。
trait `ExposureReservationBackend` 已定义在 `oxide-arb-models/src/domain/exposure.rs`，包含：
- `try_reserve(market_id, amount, ttl)` — CAS 原子预留
- `confirm(id)` / `release(id)` — 确认/释放
- `total_reserved_usd()` / `active_count()`

InMemory 实现使用 DashMap + AtomicU64 CAS loop；Redis 实现预留为未来扩展。

### P2. Calibration 全链路 Wiring

`AppContext::build()` 步骤追加：
1. `PgCalibrationRepository` → `CachedCalibrationRepository` 包装
2. `CoreCalibrationDataSource` 注入 cached_repo + GammaClient + CtfOracle
3. `CalibrationUpdater::new(calibrator, data_source, config)`
4. `TaskRegistry` spawn periodic: `updater.tick()` every 60s
5. tick 完成后 invalidate `CacheKey::AllCalibrationBuckets`

### P3. ReportGenerator 注入 ReportRepository

`ReportGenerator` 通过 `Arc<dyn ReportRepository>` 注入（trait 已定义），生成日/周报后：
- `save_daily(date, json)` 或 `save_weekly(start, end, json)`
- 发送 AlertDispatcher 通知

### P4. CalibrationDataSource 桥接实现

`CoreCalibrationDataSource` 结构体包含：
- `calibration_repo: CachedCalibrationRepository<PgCalibrationRepository>`
- `gamma_client: Arc<GammaClient>`
- `ctf_oracle: Arc<dyn CtfOracle>`

将 `calibration_outcome::Model` 转换为 `UnresolvedOutcome`，桥接 repository 与 algorithm。
