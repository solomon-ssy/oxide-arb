# Phase 0 — 工程基座

> **产出**: `oxide-arb-error`, `oxide-arb-macros`, `oxide-arb-models`, workspace Cargo.toml
>
> **前置条件**: 无
>
> **验收标准**: `cargo build --workspace` 零 warning；`cargo test --workspace` 全绿；clippy all=deny 通过

---

## 0. 工作范围

1. 初始化全新 workspace（删除旧代码，保留 `.git` 历史）
2. 建立三个 leaf crate：`oxide-arb-error`、`oxide-arb-macros`、`oxide-arb-models`
3. 定义所有基础类型、领域模型、配置结构、枚举、常量
4. 建立 workspace lint 策略、profile 策略、依赖版本锁定

---

## 1. Workspace 根 Cargo.toml

```toml
[workspace]
resolver = "2"
members = [
  "crates/oxide-arb-error",
  "crates/oxide-arb-macros",
  "crates/oxide-arb-models",
]

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.85"
license = "MIT"

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
all = { level = "deny", priority = -1 }
pedantic = { level = "warn", priority = -1 }
nursery = { level = "warn", priority = -1 }
module_name_repetitions = "allow"
must_use_candidate = "allow"
missing_errors_doc = "allow"
missing_panics_doc = "allow"

[workspace.dependencies]
# 精确版本将在实现时锁定，以下为主要依赖声明
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
rust_decimal = { version = "1", features = ["serde-with-str", "maths"] }
rust_decimal_macros = "1"
chrono = { version = "0.4", features = ["serde"] }
thiserror = "2"
uuid = { version = "1", features = ["v4", "v7", "serde"] }
tracing = "0.1"
async-trait = "0.1"
strum = { version = "0.27", features = ["derive"] }
validator = { version = "0.18", features = ["derive"] }
bitcode = { version = "0.6", features = ["derive", "serde", "rust_decimal"] }
sea-orm = { version = "1", features = [
  "sqlx-postgres",
  "runtime-tokio-rustls",
  "macros",
  "with-chrono",
  "with-rust_decimal",
] }

# Proc-macro deps
syn = { version = "2", features = ["full", "extra-traits"] }
quote = "1"
proc-macro2 = "1"
darling = "0.23"

# Internal crates
oxide-arb-error = { path = "crates/oxide-arb-error" }
oxide-arb-macros = { path = "crates/oxide-arb-macros" }
oxide-arb-models = { path = "crates/oxide-arb-models" }

[profile.dev]
opt-level = 0
debug = "line-tables-only"
incremental = true

[profile.dev.package."*"]
opt-level = 2
debug = false

[profile.release]
opt-level = "s"
codegen-units = 1
lto = true
panic = "abort"
strip = "symbols"
```

---

## 2. oxide-arb-error

### 2.1 定位

统一错误类型，所有 crate 的错误最终收敛于此。零业务逻辑，纯错误枚举 + 转换。

### 2.2 目录结构

```
crates/oxide-arb-error/
├── Cargo.toml
└── src/
    └── lib.rs
```

### 2.3 Cargo.toml

```toml
[package]
name = "oxide-arb-error"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[dependencies]
thiserror = { workspace = true }
sea-orm = { workspace = true }

[lints]
workspace = true
```

### 2.4 核心设计

```rust
//! Unified error types for the oxide-arb platform.

use thiserror::Error;

pub type OxideResult<T> = Result<T, OxideError>;

#[derive(Debug, Error)]
pub enum OxideError {
    // --- Infrastructure ---
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Database error: {0}")]
    Database(#[from] sea_orm::DbErr),

    #[error("Database transaction error: {0}")]
    Transaction(String),

    #[error("ClickHouse error: {0}")]
    ClickHouse(String),

    #[error("Cache error: {0}")]
    Cache(String),

    // --- Network ---
    #[error("HTTP error: {0}")]
    Http(String),

    #[error("WebSocket error: {0}")]
    WebSocket(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    // --- Trading ---
    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Signing error: {0}")]
    Signing(String),

    #[error("Validation error: {0}")]
    Validation(String),

    // --- Risk ---
    #[error("Risk denial: {0}")]
    RiskDenial(String),

    #[error("Circuit breaker open: level {level}, reason: {reason}")]
    CircuitBreakerOpen { level: u8, reason: String },

    // --- Data ---
    #[error("Market not found: {0}")]
    MarketNotFound(String),

    #[error("Stale data: {0}")]
    StaleData(String),

    // --- General ---
    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),
}

impl From<sea_orm::TransactionError<OxideError>> for OxideError {
    fn from(e: sea_orm::TransactionError<OxideError>) -> Self {
        match e {
            sea_orm::TransactionError::Connection(db_err) => Self::Database(db_err),
            sea_orm::TransactionError::Transaction(oxide_err) => oxide_err,
        }
    }
}
```

---

## 3. oxide-arb-macros

### 3.1 定位

过程宏 crate。提供 SeaORM 辅助派生宏和业务域专用派生宏。

### 3.2 目录结构

```
crates/oxide-arb-macros/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── into_active_value.rs    # 枚举 → ActiveValue 自动实现
    └── typed_id.rs             # 类型安全 ID newtype 生成宏
```

### 3.3 Cargo.toml

```toml
[package]
name = "oxide-arb-macros"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[lib]
proc-macro = true

[dependencies]
syn = { workspace = true }
quote = { workspace = true }
proc-macro2 = { workspace = true }
darling = { workspace = true }

[lints]
workspace = true
```

### 3.4 宏清单

| 宏 | 用途 | 输入 | 输出 |
|---|---|---|---|
| `#[derive(IntoActiveValue)]` | 枚举自动实现 SeaORM `IntoActiveValue` | 任意枚举 | `impl IntoActiveValue<Self> for Self` |
| `#[derive(TypedId)]` | 类型安全 ID newtype | `struct MarketId(Arc<str>)` | Display, FromStr, From<String>, Serialize/Deserialize, SeaORM bindings, PartialEq, Eq, Hash, Clone |

### 3.5 TypedId 宏设计

```rust
/// 生成类型安全的 ID newtype，底层存储为 `Arc<str>`。
///
/// 自动实现：Display, FromStr, From<&str>, From<String>,
/// Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Debug,
/// SeaORM TryGetable + ValueType + sea_query::ValueType。
///
/// 用法：
/// ```rust
/// #[derive(TypedId)]
/// pub struct MarketId;
/// ```
///
/// 展开后等价于：
/// ```rust
/// #[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// pub struct MarketId(Arc<str>);
///
/// impl MarketId {
///     pub fn new(s: impl Into<Arc<str>>) -> Self { Self(s.into()) }
///     pub fn as_str(&self) -> &str { &self.0 }
/// }
/// // + Display, FromStr, Serialize, Deserialize, SeaORM bindings...
/// ```
```

---

## 4. oxide-arb-models

### 4.1 定位

所有领域类型的唯一来源。零业务逻辑，纯数据定义。

### 4.2 目录结构

```
crates/oxide-arb-models/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── constants.rs
    ├── types/
    │   ├── mod.rs
    │   ├── ids.rs              # MarketId, EventId, TokenId, OpportunityId, TradeId, ExecutionId, OrderId
    │   └── money.rs            # Usd, Price, Shares, Bps — rust_decimal newtypes
    ├── enums/
    │   ├── mod.rs
    │   ├── common.rs           # TradeOutcome, Side, ExecutionMode, StalenessLevel, TickSize, MarketCategory
    │   ├── market.rs           # MarketCategory, TickSize, MarketStatus
    │   ├── risk.rs             # CircuitBreakerLevel, BlacklistScope, BlacklistReason
    │   └── lifecycle.rs        # LifecyclePhase, ShutdownStage
    ├── domain/
    │   ├── mod.rs
    │   ├── opportunity.rs      # Opportunity (concrete), PayoutModel, EndgameMeta
    │   ├── market.rs           # MarketEntry, EventEntry, TokenDescriptor
    │   ├── trade.rs            # NewTrade, TradeInfo, TradeRecord
    │   ├── position.rs         # PositionInfo, ExposureReservation
    │   ├── risk.rs             # RiskDecision, RiskCheck, EmergencySnapshot
    │   ├── pnl.rs              # DailyPnl, WeeklyPnl, CashFlowSummary
    │   ├── calibration.rs      # CalibrationSnapshot, ResolutionContext, BucketKey, PriceZone, DurationBucket
    │   ├── order.rs            # OrderRequest, OrderResponse, OrderStatus
    │   └── system.rs           # SystemStatus, HealthReport
    ├── config/
    │   ├── mod.rs              # Settings (顶层配置聚合)
    │   ├── detection.rs        # DetectionConfig, EndgameConfig, CalibrationConfig
    │   ├── execution.rs        # ExecutionConfig, HedgingConfig
    │   ├── risk.rs             # RiskConfig, CircuitBreakerConfig, BlacklistConfig
    │   ├── sizing.rs           # PositionSizingConfig, KellyConfig, DrawdownConfig
    │   ├── market_data.rs      # MarketDataConfig, WebSocketConfig, GammaConfig
    │   ├── polymarket.rs       # PolymarketConfig, OnchainConfig, FeesConfig
    │   ├── observability.rs    # ObservabilityConfig, AlertsConfig, MetricsConfig
    │   ├── db.rs               # DatabaseConfig, PostgresConfig
    │   ├── analytics.rs        # AnalyticsConfig (ClickHouse)
    │   ├── cache.rs            # CacheConfig, RedisConfig, MokaConfig
    │   ├── treasury.rs         # TreasuryConfig, HotWalletConfig
    │   ├── keys.rs             # KeysConfig, KeySource
    │   └── notification.rs     # NotificationConfig, TelegramConfig, WebhookConfig
    ├── entities/
    │   ├── mod.rs
    │   ├── market.rs           # SeaORM entity: markets 表
    │   ├── event.rs            # SeaORM entity: events 表
    │   ├── trade.rs            # SeaORM entity: trades 表
    │   ├── position.rs         # SeaORM entity: positions 表
    │   ├── risk_state.rs       # SeaORM entity: risk_engine_state 表
    │   ├── calibration.rs      # SeaORM entity: endgame_calibration_buckets + outcomes 表
    │   ├── lifecycle_event.rs  # SeaORM entity: lifecycle_events 表
    │   └── runtime_config.rs   # SeaORM entity: runtime_config 表
    └── idens/
        ├── mod.rs
        ├── market.rs
        ├── event.rs
        ├── trade.rs
        ├── position.rs
        ├── risk_state.rs
        ├── calibration.rs
        ├── lifecycle_event.rs
        └── runtime_config.rs
```

### 4.3 Cargo.toml

```toml
[package]
name = "oxide-arb-models"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[dependencies]
oxide-arb-error = { workspace = true }
oxide-arb-macros = { workspace = true }
rust_decimal = { workspace = true }
rust_decimal_macros = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
chrono = { workspace = true }
uuid = { workspace = true }
sea-orm = { workspace = true }
strum = { workspace = true }
validator = { workspace = true }
bitcode = { workspace = true }
tracing = { workspace = true }

[lints]
workspace = true
```

### 4.4 类型系统设计

#### 4.4.1 货币类型（`types/money.rs`）

```rust
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// US Dollar amount. Never use f64 for money.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Usd(Decimal);

/// Price in [0, 1] range (probability / token price).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Price(Decimal);

/// Share quantity (non-negative).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Shares(Decimal);

/// Basis points (1 bps = 0.01%).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Bps(Decimal);
```

每个类型实现：
- `new()` / `inner()` / `ZERO` / `ONE`
- 算术操作（`Add`, `Sub`, `Mul` 限定于类型安全的交叉运算）
- `Shares × Price → Usd`
- `Usd / Price → Shares`
- `(actual - expected) / expected × 10000 → Bps`

#### 4.4.2 ID 类型（`types/ids.rs`）

使用 `#[derive(TypedId)]` 宏生成：

```rust
#[derive(TypedId)]
pub struct MarketId;      // Polymarket condition_id

#[derive(TypedId)]
pub struct EventId;       // Polymarket event_id

#[derive(TypedId)]
pub struct TokenId;       // CTF token_id (ERC1155 position ID)

#[derive(TypedId)]
pub struct OpportunityId; // UUID v7 (time-sortable)

#[derive(TypedId)]
pub struct TradeId;       // UUID v7

#[derive(TypedId)]
pub struct ExecutionId;   // UUID v7

#[derive(TypedId)]
pub struct OrderId;       // CLOB order ID (string from Polymarket)
```

#### 4.4.3 核心领域模型（`domain/opportunity.rs`）

```rust
use crate::types::*;
use crate::enums::common::*;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Detected endgame opportunity ready for evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Opportunity {
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub side: Side,
    pub payout_model: PayoutModel,
    pub shares: Shares,
    pub entry_price: Price,
    pub total_cost: Usd,
    pub total_fees: Usd,
    pub net_profit: Usd,
    pub expected_net_profit: Usd,
    pub edge_bps: Bps,
    pub resolution_adjust: Decimal,
    pub depth_used_pct: Decimal,
    pub staleness: StalenessLevel,
    pub category: MarketCategory,
    pub meta: EndgameMeta,
    pub calibration: CalibrationSnapshot,
    pub detected_at: DateTime<Utc>,
}

/// Endgame-specific metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndgameMeta {
    pub predicted_yes: bool,
    pub confidence: Decimal,
    pub convergence_duration_secs: u64,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
    pub settlement_deadline: Option<DateTime<Utc>>,
}

/// Settlement payout model for endgame strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PayoutModel {
    DirectionalSettlement {
        projected_payout_if_correct: Usd,
        expected_payout: Usd,
        predicted_side: Side,
    },
}

impl PayoutModel {
    /// Single source of truth for PnL computation.
    pub fn compute_pnl(&self, total_cost: Usd, total_fees: Usd) -> Usd {
        match self {
            Self::DirectionalSettlement { expected_payout, .. } => {
                *expected_payout - total_cost - total_fees
            }
        }
    }
}
```

#### 4.4.4 配置聚合（`config/mod.rs`）

```rust
use serde::Deserialize;
use validator::Validate;

/// Top-level application configuration.
/// Loaded from TOML + environment variable overrides (OXIDE_ARB__*).
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct Settings {
    #[validate(nested)]
    pub detection: DetectionConfig,
    #[validate(nested)]
    pub execution: ExecutionConfig,
    #[validate(nested)]
    pub risk: RiskConfig,
    #[validate(nested)]
    pub sizing: PositionSizingConfig,
    #[validate(nested)]
    pub market_data: MarketDataConfig,
    #[validate(nested)]
    pub polymarket: PolymarketConfig,
    #[validate(nested)]
    pub observability: ObservabilityConfig,
    #[validate(nested)]
    pub db: DatabaseConfig,
    #[validate(nested)]
    pub analytics: AnalyticsConfig,
    #[validate(nested)]
    pub cache: CacheConfig,
    #[validate(nested)]
    pub treasury: TreasuryConfig,
    #[validate(nested)]
    pub keys: KeysConfig,
    #[validate(nested)]
    pub notification: NotificationConfig,
}

impl Settings {
    /// Load configuration with precedence: env vars > TOML file > defaults.
    pub fn load(config_dir: &str) -> Result<Self, oxide_arb_error::OxideError> {
        // Implementation uses `config` crate
        todo!()
    }
}
```

### 4.5 枚举设计（`enums/common.rs` 摘要）

```rust
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
pub enum ExecutionMode {
    DryRun,
    Paper,
    Live,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
pub enum StalenessLevel {
    Fresh,      // < 2s
    Acceptable, // 2-5s
    Stale,      // 5-15s
    Expired,    // > 15s
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
pub enum TradeOutcome {
    Success,
    Miss,
    Stale,
    TradeFailed,
    SystemError,
}
```

### 4.6 常量（`constants.rs`）

```rust
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Polymarket CTF Exchange (standard markets)
pub const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";

/// Polymarket Neg Risk CTF Exchange
pub const NEG_RISK_CTF_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

/// USDC.e on Polygon
pub const USDC_E: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";

/// Conditional Tokens Framework
pub const CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";

/// Polygon chain ID
pub const POLYGON_CHAIN_ID: u64 = 137;

/// USDC decimals and scale only — all trading thresholds live in config.
pub const USDC_DECIMALS: u8 = 6;
pub const USDC_SCALE: u64 = 1_000_000;
```

Runtime depth limits: `[risk] min_depth_usd`, `max_depth_usage_pct` (defaults 200 / 30).
Sizing uses `[sizing] bankroll_usd` + `kelly_fraction`. Detection has no `budget_targets_usd` (ADR-001).

---

## 5. 验收检查清单

- [ ] `cargo build --workspace` 编译通过（零 warning）
- [ ] `cargo clippy --workspace -- -D warnings` 通过
- [ ] `cargo test --workspace` 全绿
- [ ] `cargo doc --workspace --no-deps` 生成文档无 broken link
- [ ] 所有 money 类型禁止 `From<f64>` 实现
- [ ] 所有 ID 类型通过 `TypedId` 宏生成，具备 SeaORM 绑定
- [ ] `Settings` 可从 `config/oxide-arb.toml` + env vars 加载
- [ ] 错误类型覆盖所有已知失败场景（基础设施 + 网络 + 交易 + 风控 + 数据）

---

## 6. 文件变更清单

### 新建文件

```
Cargo.toml                              # workspace root
crates/oxide-arb-error/Cargo.toml
crates/oxide-arb-error/src/lib.rs
crates/oxide-arb-macros/Cargo.toml
crates/oxide-arb-macros/src/lib.rs
crates/oxide-arb-macros/src/into_active_value.rs
crates/oxide-arb-macros/src/typed_id.rs
crates/oxide-arb-models/Cargo.toml
crates/oxide-arb-models/src/lib.rs
crates/oxide-arb-models/src/constants.rs
crates/oxide-arb-models/src/types/{mod,ids,money}.rs
crates/oxide-arb-models/src/enums/{mod,common,market,risk,lifecycle}.rs
crates/oxide-arb-models/src/domain/{mod,opportunity,market,trade,position,risk,pnl,calibration,order,system}.rs
crates/oxide-arb-models/src/config/{mod,detection,execution,risk,sizing,market_data,polymarket,observability,db,analytics,cache,treasury,keys,notification,validation}.rs
crates/oxide-arb-models/src/entities/{mod,market,event,trade,position,risk_state,calibration,lifecycle_event,runtime_config}.rs
crates/oxide-arb-models/src/idens/{mod,market,event,trade,position,risk_state,calibration,lifecycle_event,runtime_config}.rs
config/oxide-arb.toml                   # 默认配置文件
config/oxide-arb.production.example.toml
.github/workflows/ci.yml               # CI pipeline
```

### 删除文件

旧 workspace 的所有 crate 目录（保留 `.git/`、`docs/`、`config/` 部分文件）。

---

## 7. 预估工作量

| 组件 | 源码 LoC | 测试 LoC |
|---|---|---|
| `oxide-arb-error` | ~120 | ~80 |
| `oxide-arb-macros` | ~400 | ~200 |
| `oxide-arb-models` types + enums | ~800 | ~400 |
| `oxide-arb-models` domain | ~600 | ~300 |
| `oxide-arb-models` config | ~1,200 | ~200 |
| `oxide-arb-models` entities + idens | ~1,000 | ~100 |
| Workspace 配置 + CI | ~200 | — |
| **合计** | **~4,320** | **~1,280** |
