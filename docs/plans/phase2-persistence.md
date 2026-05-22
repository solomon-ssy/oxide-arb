# Phase 2 — 持久化体系

> **产出**: `oxide-arb-storage`, `oxide-arb-repository` crates
>
> **前置条件**: Phase 0 (models/error/macros) + Phase 1 (API 层) 完成
>
> **验收标准**: PostgreSQL CRUD 全通过；ClickHouse 写入+聚合查询正确；Redis+Moka 多级缓存 hit/miss 路径覆盖；所有 Repository trait 具备集成测试

---

## 0. 工作范围

1. `oxide-arb-storage` — 数据库连接管理、Schema 迁移、缓存后端抽象、ClickHouse 客户端封装
2. `oxide-arb-repository` — 基于 Repository Pattern 的数据访问层，为每个领域实体提供 async trait 接口

---

## 1. oxide-arb-storage 目录结构

```
crates/oxide-arb-storage/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── postgres/
    │   ├── mod.rs              # PostgresPool: 连接池初始化 + health check
    │   ├── pool.rs             # SeaORM DatabaseConnection wrapper
    │   └── migration/
    │       ├── mod.rs           # MigratorTrait 实现
    │       ├── m20250601_000001_create_events.rs
    │       ├── m20250601_000002_create_markets.rs
    │       ├── m20250601_000003_create_trades.rs
    │       ├── m20250601_000004_create_positions.rs
    │       ├── m20250601_000005_create_risk_engine_state.rs
    │       ├── m20250601_000006_create_calibration.rs
    │       ├── m20250601_000007_create_runtime_config.rs
    │       ├── m20250601_000008_create_lifecycle_events.rs
    │       ├── m20250601_000009_create_accounting_periods.rs
    │       └── m20250601_000010_create_potential_loss_ledger.rs
    ├── clickhouse/
    │   ├── mod.rs              # ClickHousePool: 连接池 + retry wrapper
    │   ├── pool.rs             # clickhouse-rs client wrapper
    │   ├── schema.rs           # DDL 语句 (MergeTree 建表)
    │   └── inserter.rs         # BatchInserter: 批量写入 + 背压控制
    ├── cache/
    │   ├── mod.rs              # CacheBackend trait + TieredCache
    │   ├── backend.rs          # CacheBackend trait 定义
    │   ├── moka.rs             # MokaBackend: 进程内 L1 缓存
    │   ├── redis.rs            # RedisBackend: 分布式 L2 缓存
    │   ├── tiered.rs           # TieredCache: L1 → L2 fallthrough
    │   ├── keys.rs             # 领域缓存键生成 (type-safe)
    │   └── metrics.rs          # 缓存命中率 prometheus counters
    └── error.rs                # StorageError 枚举
```

---

## 2. oxide-arb-repository 目录结构

```
crates/oxide-arb-repository/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── traits/
    │   ├── mod.rs
    │   ├── market.rs           # MarketRepository trait
    │   ├── event.rs            # EventRepository trait
    │   ├── trade.rs            # TradeRepository trait
    │   ├── position.rs         # PositionRepository trait
    │   ├── risk_state.rs       # RiskStateRepository trait
    │   ├── calibration.rs      # CalibrationRepository trait
    │   ├── runtime_config.rs   # RuntimeConfigRepository trait
    │   ├── lifecycle.rs        # LifecycleRepository trait
    │   ├── accounting.rs       # AccountingRepository trait
    │   └── timeseries.rs       # TimeseriesRepository trait (ClickHouse)
    ├── postgres/
    │   ├── mod.rs
    │   ├── market.rs           # PgMarketRepository
    │   ├── event.rs            # PgEventRepository
    │   ├── trade.rs            # PgTradeRepository
    │   ├── position.rs         # PgPositionRepository
    │   ├── risk_state.rs       # PgRiskStateRepository
    │   ├── calibration.rs      # PgCalibrationRepository
    │   ├── runtime_config.rs   # PgRuntimeConfigRepository
    │   ├── lifecycle.rs        # PgLifecycleRepository
    │   └── accounting.rs       # PgAccountingRepository
    ├── clickhouse/
    │   ├── mod.rs
    │   └── timeseries.rs       # ChTimeseriesRepository
    └── cached/
        ├── mod.rs
        ├── market.rs           # CachedMarketRepository (wrap PgMarketRepository + TieredCache)
        └── calibration.rs      # CachedCalibrationRepository
```

---

## 3. PostgreSQL Schema 设计

### 3.1 events 表

```sql
CREATE TABLE events (
    event_id        TEXT PRIMARY KEY,          -- Polymarket event slug/ID
    title           TEXT NOT NULL,
    category        TEXT NOT NULL,             -- MarketCategory enum
    status          TEXT NOT NULL DEFAULT 'active',
    neg_risk        BOOLEAN NOT NULL DEFAULT FALSE,
    end_date        TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_gamma       JSONB                     -- 完整 Gamma API 响应备份
);

CREATE INDEX idx_events_status ON events (status);
CREATE INDEX idx_events_category ON events (category);
CREATE INDEX idx_events_end_date ON events (end_date) WHERE end_date IS NOT NULL;
```

### 3.2 markets 表

```sql
CREATE TABLE markets (
    market_id       TEXT PRIMARY KEY,          -- condition_id
    event_id        TEXT NOT NULL REFERENCES events(event_id),
    question        TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active',  -- active / closed / resolved
    outcome         TEXT,                      -- YES / NO / NULL (未结算)
    yes_token_id    TEXT NOT NULL,
    no_token_id     TEXT NOT NULL,
    tick_size        TEXT NOT NULL DEFAULT '0.01',
    neg_risk        BOOLEAN NOT NULL DEFAULT FALSE,
    end_date        TIMESTAMPTZ,
    resolved_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_markets_event_id ON markets (event_id);
CREATE INDEX idx_markets_status ON markets (status);
CREATE INDEX idx_markets_yes_token ON markets (yes_token_id);
CREATE INDEX idx_markets_no_token ON markets (no_token_id);
CREATE INDEX idx_markets_active_endgame ON markets (end_date)
    WHERE status = 'active' AND end_date IS NOT NULL;
```

### 3.3 trades 表

```sql
CREATE TABLE trades (
    trade_id        UUID PRIMARY KEY,          -- UUID v7
    execution_id    UUID NOT NULL,
    opportunity_id  UUID NOT NULL,
    market_id       TEXT NOT NULL REFERENCES markets(market_id),
    token_id        TEXT NOT NULL,
    side            TEXT NOT NULL,              -- Buy / Sell
    shares          NUMERIC(20,8) NOT NULL,
    price           NUMERIC(10,8) NOT NULL,
    cost_usd        NUMERIC(20,8) NOT NULL,
    fee_usd         NUMERIC(20,8) NOT NULL,
    order_id        TEXT,                       -- CLOB order ID (null if rejected)
    outcome         TEXT NOT NULL,              -- Success / Miss / SystemError
    strategy        TEXT NOT NULL,              -- endgame_convergence
    execution_mode  TEXT NOT NULL,              -- DryRun / Paper / Live
    latency_ms      INTEGER,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_trades_execution_id ON trades (execution_id);
CREATE INDEX idx_trades_opportunity_id ON trades (opportunity_id);
CREATE INDEX idx_trades_market_id ON trades (market_id);
CREATE INDEX idx_trades_created_at ON trades (created_at DESC);
CREATE INDEX idx_trades_outcome ON trades (outcome);
CREATE INDEX idx_trades_strategy_date ON trades (strategy, created_at DESC);
```

### 3.4 positions 表

```sql
CREATE TABLE positions (
    position_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    market_id       TEXT NOT NULL REFERENCES markets(market_id),
    token_id        TEXT NOT NULL,
    side            TEXT NOT NULL,
    shares          NUMERIC(20,8) NOT NULL,
    avg_entry_price NUMERIC(10,8) NOT NULL,
    total_cost_usd  NUMERIC(20,8) NOT NULL,
    total_fees_usd  NUMERIC(20,8) NOT NULL,
    unrealized_pnl  NUMERIC(20,8) NOT NULL DEFAULT 0,
    realized_pnl    NUMERIC(20,8) NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'open',  -- open / closed / settled
    strategy        TEXT NOT NULL,
    opened_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    closed_at       TIMESTAMPTZ,
    settled_at      TIMESTAMPTZ
);

CREATE INDEX idx_positions_market_id ON positions (market_id);
CREATE INDEX idx_positions_status ON positions (status);
CREATE INDEX idx_positions_open_strategy ON positions (strategy) WHERE status = 'open';
CREATE UNIQUE INDEX idx_positions_open_market ON positions (market_id, token_id, side)
    WHERE status = 'open';
```

### 3.5 risk_engine_state 表

```sql
CREATE TABLE risk_engine_state (
    id                      SERIAL PRIMARY KEY,
    circuit_breaker_level   SMALLINT NOT NULL DEFAULT 0,
    breaker_state           TEXT NOT NULL DEFAULT 'Closed',
    is_halted               BOOLEAN NOT NULL DEFAULT FALSE,
    halt_reason             TEXT,
    consecutive_misses      INTEGER NOT NULL DEFAULT 0,
    consecutive_hedge_losses INTEGER NOT NULL DEFAULT 0,
    cooldown_until          TIMESTAMPTZ,
    cooldown_multiplier     INTEGER NOT NULL DEFAULT 1,
    hourly_loss_usd         NUMERIC(20,8) NOT NULL DEFAULT 0,
    hourly_fee_usd          NUMERIC(20,8) NOT NULL DEFAULT 0,
    hourly_hedge_count      INTEGER NOT NULL DEFAULT 0,
    hourly_window_start     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    daily_loss_usd          NUMERIC(20,8) NOT NULL DEFAULT 0,
    daily_fee_usd           NUMERIC(20,8) NOT NULL DEFAULT 0,
    daily_window_start      DATE NOT NULL DEFAULT CURRENT_DATE,
    weekly_loss_usd         NUMERIC(20,8) NOT NULL DEFAULT 0,
    weekly_window_start     DATE NOT NULL DEFAULT CURRENT_DATE,
    last_emergency_at       TIMESTAMPTZ,
    last_emergency_reason   TEXT,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 3.6 endgame_calibration_buckets 表

```sql
CREATE TABLE endgame_calibration_buckets (
    bucket_id       SERIAL PRIMARY KEY,
    category        TEXT NOT NULL,              -- MarketCategory
    price_zone      TEXT NOT NULL,              -- Z95 / Z96 / Z97 / Z98 / Z99
    duration_bucket TEXT NOT NULL,              -- Short / Medium / Long / VeryLong
    total_count     INTEGER NOT NULL DEFAULT 0,
    correct_count   INTEGER NOT NULL DEFAULT 0,
    alpha_prior     NUMERIC(10,4) NOT NULL DEFAULT 1.0,
    beta_prior      NUMERIC(10,4) NOT NULL DEFAULT 1.0,
    posterior_mean   NUMERIC(10,8),             -- alpha / (alpha + beta)
    last_updated    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (category, price_zone, duration_bucket)
);

CREATE INDEX idx_cal_buckets_lookup ON endgame_calibration_buckets
    (category, price_zone, duration_bucket);
```

### 3.7 endgame_calibration_outcomes 表

```sql
CREATE TABLE endgame_calibration_outcomes (
    outcome_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    market_id       TEXT NOT NULL REFERENCES markets(market_id),
    category        TEXT NOT NULL,
    price_zone      TEXT NOT NULL,
    duration_bucket TEXT NOT NULL,
    predicted_yes   BOOLEAN NOT NULL,
    actual_yes      BOOLEAN,                   -- NULL = unresolved
    entry_price     NUMERIC(10,8) NOT NULL,
    confidence_at_entry NUMERIC(10,8) NOT NULL,
    convergence_secs INTEGER NOT NULL,
    resolved_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_cal_outcomes_market ON endgame_calibration_outcomes (market_id);
CREATE INDEX idx_cal_outcomes_unresolved ON endgame_calibration_outcomes (actual_yes)
    WHERE actual_yes IS NULL;
CREATE INDEX idx_cal_outcomes_bucket ON endgame_calibration_outcomes
    (category, price_zone, duration_bucket);
```

### 3.8 runtime_config 表

```sql
CREATE TABLE runtime_config (
    key             TEXT PRIMARY KEY,
    value           JSONB NOT NULL,
    description     TEXT,
    updated_by      TEXT NOT NULL DEFAULT 'system',
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 3.9 lifecycle_events 表

```sql
CREATE TABLE lifecycle_events (
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    phase           TEXT NOT NULL,             -- Starting / Running / ShuttingDown / Stopped
    stage           TEXT,                      -- graceful sub-stage
    message         TEXT NOT NULL,
    metadata        JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_lifecycle_created ON lifecycle_events (created_at DESC);
```

### 3.10 accounting_periods 表

```sql
CREATE TABLE accounting_periods (
    period_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    period_type     TEXT NOT NULL,             -- Daily / Weekly
    start_date      DATE NOT NULL,
    end_date        DATE NOT NULL,
    realized_pnl    NUMERIC(20,8) NOT NULL DEFAULT 0,
    total_fees      NUMERIC(20,8) NOT NULL DEFAULT 0,
    trade_count     INTEGER NOT NULL DEFAULT 0,
    win_count       INTEGER NOT NULL DEFAULT 0,
    loss_count      INTEGER NOT NULL DEFAULT 0,
    miss_count      INTEGER NOT NULL DEFAULT 0,
    max_drawdown    NUMERIC(20,8) NOT NULL DEFAULT 0,
    sharpe_ratio    NUMERIC(10,6),
    finalized       BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (period_type, start_date)
);

CREATE INDEX idx_acct_period_type ON accounting_periods (period_type, start_date DESC);
```

### 3.11 potential_loss_ledger 表

```sql
CREATE TABLE potential_loss_ledger (
    ledger_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    market_id       TEXT NOT NULL REFERENCES markets(market_id),
    token_id        TEXT NOT NULL,
    shares          NUMERIC(20,8) NOT NULL,
    entry_price     NUMERIC(10,8) NOT NULL,
    max_loss_usd    NUMERIC(20,8) NOT NULL,
    strategy        TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active',  -- active / resolved / expired
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX idx_pll_status ON potential_loss_ledger (status) WHERE status = 'active';
CREATE INDEX idx_pll_market ON potential_loss_ledger (market_id);
```

---

## 4. ClickHouse 表设计

### 4.1 tick_events — 原始 WS 事件流

```sql
CREATE TABLE tick_events (
    token_id        String,
    event_type      Enum8('book_snapshot' = 1, 'price_change' = 2, 'best_bid_ask' = 3),
    best_bid        Decimal64(8),
    best_ask        Decimal64(8),
    bid_depth_usd   Decimal64(4),
    ask_depth_usd   Decimal64(4),
    spread_bps      UInt32,
    raw_payload     String CODEC(ZSTD(3)),
    received_at     DateTime64(3, 'UTC'),
    event_date      Date MATERIALIZED toDate(received_at)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (token_id, received_at)
TTL event_date + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;
```

### 4.2 book_snapshots — 定时 orderbook 快照

```sql
CREATE TABLE book_snapshots (
    token_id        String,
    snapshot_time   DateTime64(3, 'UTC'),
    bids            String CODEC(ZSTD(3)),     -- JSON array of [price, size]
    asks            String CODEC(ZSTD(3)),
    bid_depth_usd   Decimal64(4),
    ask_depth_usd   Decimal64(4),
    mid_price       Decimal64(8),
    spread_bps      UInt32,
    levels_count    UInt16,
    snapshot_date   Date MATERIALIZED toDate(snapshot_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (token_id, snapshot_time)
TTL snapshot_date + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 4096;
```

### 4.3 opportunity_audit — 机会检测审计记录

```sql
CREATE TABLE opportunity_audit (
    opportunity_id  UUID,
    market_id       String,
    event_id        String,
    strategy        String,
    side            String,
    entry_price     Decimal64(8),
    shares          Decimal64(8),
    total_cost_usd  Decimal64(8),
    total_fees_usd  Decimal64(8),
    net_profit_usd  Decimal64(8),
    expected_profit  Decimal64(8),
    edge_bps        UInt32,
    resolution_prob Decimal64(8),
    confidence      Decimal64(8),
    convergence_secs UInt32,
    price_zone      String,
    duration_bucket String,
    depth_used_pct  Decimal64(4),
    staleness       String,
    category        String,
    outcome         Nullable(String),          -- NULL until resolved
    detected_at     DateTime64(3, 'UTC'),
    audit_date      Date MATERIALIZED toDate(detected_at)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(audit_date)
ORDER BY (strategy, detected_at, opportunity_id)
TTL audit_date + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 8192;
```

### 4.4 calibration_snapshots — 校准状态快照

```sql
CREATE TABLE calibration_snapshots (
    category        String,
    price_zone      String,
    duration_bucket String,
    total_count     UInt32,
    correct_count   UInt32,
    alpha_prior     Decimal64(4),
    beta_prior      Decimal64(4),
    posterior_mean   Decimal64(8),
    snapshot_time   DateTime64(3, 'UTC'),
    snapshot_date   Date MATERIALIZED toDate(snapshot_time)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (category, price_zone, duration_bucket, snapshot_time)
TTL snapshot_date + INTERVAL 365 DAY DELETE
SETTINGS index_granularity = 4096;
```

---

## 5. 缓存架构

### 5.1 CacheBackend Trait

```rust
use async_trait::async_trait;
use std::time::Duration;

#[async_trait]
pub trait CacheBackend: Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError>;
    async fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<(), StorageError>;
    async fn delete(&self, key: &str) -> Result<bool, StorageError>;
    async fn exists(&self, key: &str) -> Result<bool, StorageError>;

    /// Bulk get — returns (found, missing_keys).
    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, StorageError>;

    /// Bulk set with uniform TTL.
    async fn mset(&self, entries: &[(&str, &[u8])], ttl: Duration) -> Result<(), StorageError>;
}
```

### 5.2 TieredCache

```rust
use bitcode::{Decode, Encode};
use serde::{de::DeserializeOwned, Serialize};

pub struct TieredCache {
    l1: MokaBackend,       // 进程内, ~10ms, 容量上限 10,000 entries
    l2: RedisBackend,      // 分布式, ~1-3ms RTT
    metrics: CacheMetrics,
}

impl TieredCache {
    /// Get with tiered fallthrough: L1 → L2 → miss.
    /// On L2 hit, backfill L1 with shorter TTL.
    pub async fn get<T: Decode + Send>(&self, key: &CacheKey) -> Result<Option<T>, StorageError> {
        let key_str = key.as_str();

        // L1 check
        if let Some(bytes) = self.l1.get(key_str).await? {
            self.metrics.record_hit("l1", key.domain());
            return Ok(Some(bitcode::decode(&bytes)?));
        }

        // L2 check
        if let Some(bytes) = self.l2.get(key_str).await? {
            self.metrics.record_hit("l2", key.domain());
            // Backfill L1 with 1/4 of L2 TTL
            let l1_ttl = key.ttl() / 4;
            self.l1.set(key_str, &bytes, l1_ttl).await?;
            return Ok(Some(bitcode::decode(&bytes)?));
        }

        self.metrics.record_miss(key.domain());
        Ok(None)
    }

    /// Set at both levels: L1 gets shorter TTL, L2 gets full TTL.
    pub async fn set<T: Encode + Send + Sync>(
        &self,
        key: &CacheKey,
        value: &T,
    ) -> Result<(), StorageError> {
        let bytes = bitcode::encode(value);
        let ttl = key.ttl();
        let l1_ttl = ttl / 4;

        // Write L1 and L2 concurrently
        let (r1, r2) = tokio::join!(
            self.l1.set(key.as_str(), &bytes, l1_ttl),
            self.l2.set(key.as_str(), &bytes, ttl),
        );
        r1?;
        r2?;
        Ok(())
    }

    /// Invalidate from both levels.
    pub async fn invalidate(&self, key: &CacheKey) -> Result<(), StorageError> {
        let (r1, r2) = tokio::join!(
            self.l1.delete(key.as_str()),
            self.l2.delete(key.as_str()),
        );
        r1?;
        r2?;
        Ok(())
    }
}
```

### 5.3 缓存键设计

```rust
use std::time::Duration;

/// Type-safe cache key builder.
pub enum CacheKey {
    MarketEntry { market_id: MarketId },
    EventEntry { event_id: EventId },
    MarketMetadata { market_id: MarketId },
    ActiveMarkets,
    CalibrationBucket {
        category: MarketCategory,
        price_zone: PriceZone,
        duration_bucket: DurationBucket,
    },
    AllCalibrationBuckets,
    PositionSummary { market_id: MarketId },
    RiskState,
    Balance,
    RuntimeConfig { key: RuntimeConfigKey },
    AllRuntimeConfig,
    FeeParams { category: MarketCategory },
}

impl CacheKey {
    pub fn as_str(&self) -> String {
        match self {
            Self::MarketEntry { market_id } => format!("mkt:{market_id}"),
            Self::EventEntry { event_id } => format!("evt:{event_id}"),
            Self::MarketMetadata { market_id } => format!("mkt_meta:{market_id}"),
            Self::ActiveMarkets => "mkt:__active__".to_owned(),
            Self::CalibrationBucket { category, price_zone, duration_bucket } => {
                format!("cal:{}:{price_zone}:{duration_bucket}", category.as_str())
            }
            Self::AllCalibrationBuckets => "cal:__all__".to_owned(),
            Self::PositionSummary { market_id } => format!("pos:{market_id}"),
            Self::RiskState => "risk:state".to_string(),
            Self::Balance => "bal:polymarket".to_owned(),
            Self::RuntimeConfig { key } => format!("cfg:{}", key.as_str()),
            Self::AllRuntimeConfig => "cfg:__all__".to_owned(),
            Self::FeeParams { category } => format!("fee:{}", category.as_str()),
        }
    }

    pub fn ttl(&self) -> Duration {
        match self {
            Self::MarketEntry { .. } | Self::EventEntry { .. } | Self::ActiveMarkets => Duration::from_secs(300),
            Self::MarketMetadata { .. } => Duration::from_secs(1800),
            Self::CalibrationBucket { .. } | Self::AllCalibrationBuckets => Duration::from_secs(3600),
            Self::PositionSummary { .. } => Duration::from_secs(30),
            Self::RiskState | Self::RuntimeConfig { .. } | Self::AllRuntimeConfig => Duration::from_secs(60),
            Self::Balance => Duration::from_secs(15),
            Self::FeeParams { .. } => Duration::from_secs(600),
        }
    }

    pub fn domain(&self) -> &'static str {
        match self {
            Self::MarketEntry { .. } | Self::ActiveMarkets => "market",
            Self::EventEntry { .. } => "event",
            Self::MarketMetadata { .. } => "market_metadata",
            Self::CalibrationBucket { .. } | Self::AllCalibrationBuckets => "calibration",
            Self::PositionSummary { .. } => "position",
            Self::RiskState => "risk",
            Self::Balance => "balance",
            Self::RuntimeConfig { .. } | Self::AllRuntimeConfig => "config",
            Self::FeeParams { .. } => "fee",
        }
    }
}
```

### 5.4 TTL 策略汇总

| 缓存键 | L2 (Redis) TTL | L1 (Moka) TTL | 说明 |
|---|---|---|---|
| `mkt:{id}` | 300s | 75s | 市场元数据变化不频繁 |
| `evt:{id}` | 300s | 75s | 事件元数据 |
| `cal:{cat}:{zone}:{dur}` | 3600s | 900s | 校准桶更新频率 ~1h |
| `risk:state` | 10s | 2s | 风控状态高频读写 |
| `cfg:{key}` | 60s | 15s | 运行时配置 |
| `fee:{cat}` | 600s | 150s | 费率参数 |
| `book:{token}` | 5s | 1s | 订单簿快照极高频 |
| `pos:{market}` | 30s | 7s | 持仓数据 |

---

## 6. MokaBackend 实现

```rust
use moka::future::Cache;

pub struct MokaBackend {
    cache: Cache<String, Vec<u8>>,
}

impl MokaBackend {
    pub fn new(max_capacity: u64) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_capacity)
            .time_to_idle(Duration::from_secs(600))
            .build();
        Self { cache }
    }
}

#[async_trait]
impl CacheBackend for MokaBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.cache.get(key).await)
    }

    async fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<(), StorageError> {
        self.cache
            .insert(key.to_string(), value.to_vec())
            .await;
        // Moka uses per-entry TTL via policy, but we use time_to_idle at cache level.
        // For per-entry TTL, use entry API:
        self.cache
            .policy()
            .set_time_to_live(ttl);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, StorageError> {
        self.cache.remove(key).await;
        Ok(true)
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        Ok(self.cache.contains_key(key))
    }

    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
        Ok(keys.iter().map(|k| {
            // moka sync get for batch (acceptable, <1μs per entry)
            self.cache.get(*k).now_or_never().flatten()
        }).collect())
    }

    async fn mset(&self, entries: &[(&str, &[u8])], _ttl: Duration) -> Result<(), StorageError> {
        for (k, v) in entries {
            self.cache.insert((*k).to_string(), v.to_vec()).await;
        }
        Ok(())
    }
}
```

---

## 7. RedisBackend 实现

```rust
use deadpool_redis::{Config, Pool, Runtime};
use redis::AsyncCommands;

pub struct RedisBackend {
    pool: Pool,
    key_prefix: String,
}

impl RedisBackend {
    pub async fn new(config: &RedisConfig) -> Result<Self, StorageError> {
        let cfg = Config::from_url(&config.url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        // Verify connectivity
        let mut conn = pool.get().await
            .map_err(|e| StorageError::Connection(e.to_string()))?;
        redis::cmd("PING").query_async::<String>(&mut conn).await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Self {
            pool,
            key_prefix: config.key_prefix.clone().unwrap_or_else(|| "oarb:".into()),
        })
    }

    fn prefixed(&self, key: &str) -> String {
        format!("{}{}", self.key_prefix, key)
    }
}

#[async_trait]
impl CacheBackend for RedisBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let mut conn = self.pool.get().await
            .map_err(|e| StorageError::Cache(e.to_string()))?;
        let result: Option<Vec<u8>> = conn.get(self.prefixed(key)).await
            .map_err(|e| StorageError::Cache(e.to_string()))?;
        Ok(result)
    }

    async fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<(), StorageError> {
        let mut conn = self.pool.get().await
            .map_err(|e| StorageError::Cache(e.to_string()))?;
        conn.set_ex(self.prefixed(key), value, ttl.as_secs())
            .await
            .map_err(|e| StorageError::Cache(e.to_string()))?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<bool, StorageError> {
        let mut conn = self.pool.get().await
            .map_err(|e| StorageError::Cache(e.to_string()))?;
        let removed: i64 = conn.del(self.prefixed(key)).await
            .map_err(|e| StorageError::Cache(e.to_string()))?;
        Ok(removed > 0)
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        let mut conn = self.pool.get().await
            .map_err(|e| StorageError::Cache(e.to_string()))?;
        let exists: bool = conn.exists(self.prefixed(key)).await
            .map_err(|e| StorageError::Cache(e.to_string()))?;
        Ok(exists)
    }

    async fn mget(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>, StorageError> {
        let mut conn = self.pool.get().await
            .map_err(|e| StorageError::Cache(e.to_string()))?;
        let prefixed: Vec<String> = keys.iter().map(|k| self.prefixed(k)).collect();
        let results: Vec<Option<Vec<u8>>> = conn.mget(prefixed).await
            .map_err(|e| StorageError::Cache(e.to_string()))?;
        Ok(results)
    }

    async fn mset(&self, entries: &[(&str, &[u8])], ttl: Duration) -> Result<(), StorageError> {
        let mut conn = self.pool.get().await
            .map_err(|e| StorageError::Cache(e.to_string()))?;
        let mut pipe = redis::pipe();
        for (k, v) in entries {
            pipe.set_ex(self.prefixed(k), *v, ttl.as_secs());
        }
        pipe.query_async(&mut conn).await
            .map_err(|e| StorageError::Cache(e.to_string()))?;
        Ok(())
    }
}
```

---

## 8. SeaORM 迁移策略

```rust
use sea_orm_migration::prelude::*;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20250601_000001_create_events::Migration),
            Box::new(m20250601_000002_create_markets::Migration),
            Box::new(m20250601_000003_create_trades::Migration),
            Box::new(m20250601_000004_create_positions::Migration),
            Box::new(m20250601_000005_create_risk_engine_state::Migration),
            Box::new(m20250601_000006_create_calibration::Migration),
            Box::new(m20250601_000007_create_runtime_config::Migration),
            Box::new(m20250601_000008_create_lifecycle_events::Migration),
            Box::new(m20250601_000009_create_accounting_periods::Migration),
            Box::new(m20250601_000010_create_potential_loss_ledger::Migration),
        ]
    }
}
```

迁移执行流程：

1. **dev 环境**: `cargo run -- migrate up` 自动执行全部 pending migrations
2. **production**: CI pipeline 在部署前执行 `sea-orm-cli migrate up --database-url $PG_URL`
3. **回滚**: 每个 migration 实现 `MigrationTrait::down()` 用于降级
4. **ClickHouse DDL**: 独立 SQL 脚本，在应用启动时通过 `ClickHousePool::ensure_schema()` 幂等执行

---

## 9. Repository Trait 定义

### 9.1 MarketRepository

```rust
use oxide_arb_models::{
    types::MarketId,
    entities::market,
};

#[async_trait]
pub trait MarketRepository: Send + Sync + 'static {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<market::Model>, StorageError>;
    async fn find_active(&self) -> Result<Vec<market::Model>, StorageError>;
    async fn find_by_event(&self, event_id: &str) -> Result<Vec<market::Model>, StorageError>;
    async fn find_endgame_candidates(
        &self,
        before_deadline: DateTime<Utc>,
    ) -> Result<Vec<market::Model>, StorageError>;
    async fn upsert(&self, model: market::ActiveModel) -> Result<market::Model, StorageError>;
    async fn upsert_batch(&self, models: Vec<market::ActiveModel>) -> Result<u64, StorageError>;
    async fn update_status(
        &self,
        id: &MarketId,
        status: &str,
        outcome: Option<&str>,
    ) -> Result<(), StorageError>;
}
```

### 9.2 TradeRepository

```rust
#[async_trait]
pub trait TradeRepository: Send + Sync + 'static {
    async fn insert(&self, trade: trade::ActiveModel) -> Result<trade::Model, StorageError>;
    async fn find_by_execution(
        &self,
        execution_id: &Uuid,
    ) -> Result<Vec<trade::Model>, StorageError>;
    async fn find_by_market(
        &self,
        market_id: &MarketId,
        limit: u64,
    ) -> Result<Vec<trade::Model>, StorageError>;
    async fn find_recent(
        &self,
        since: DateTime<Utc>,
        strategy: Option<&str>,
    ) -> Result<Vec<trade::Model>, StorageError>;
    async fn count_by_outcome(
        &self,
        since: DateTime<Utc>,
    ) -> Result<HashMap<String, i64>, StorageError>;
}
```

### 9.3 PositionRepository

```rust
#[async_trait]
pub trait PositionRepository: Send + Sync + 'static {
    async fn find_open(&self) -> Result<Vec<position::Model>, StorageError>;
    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<position::Model>, StorageError>;
    async fn open_position(
        &self,
        position: position::ActiveModel,
    ) -> Result<position::Model, StorageError>;
    async fn close_position(
        &self,
        position_id: &Uuid,
        realized_pnl: Decimal,
    ) -> Result<(), StorageError>;
    async fn settle_position(
        &self,
        position_id: &Uuid,
        realized_pnl: Decimal,
    ) -> Result<(), StorageError>;
    async fn total_exposure(&self) -> Result<Decimal, StorageError>;
    async fn count_open(&self) -> Result<usize, StorageError>;
}
```

### 9.4 CalibrationRepository

```rust
#[async_trait]
pub trait CalibrationRepository: Send + Sync + 'static {
    async fn get_bucket(
        &self,
        category: &str,
        price_zone: &str,
        duration_bucket: &str,
    ) -> Result<Option<calibration::Model>, StorageError>;

    async fn get_buckets_by_category(
        &self,
        category: &str,
    ) -> Result<Vec<calibration::Model>, StorageError>;

    async fn get_all_buckets(&self) -> Result<Vec<calibration::Model>, StorageError>;

    async fn upsert_bucket(
        &self,
        bucket: calibration::ActiveModel,
    ) -> Result<calibration::Model, StorageError>;

    async fn record_outcome(
        &self,
        outcome: calibration_outcome::ActiveModel,
    ) -> Result<(), StorageError>;

    async fn get_unresolved_outcomes(&self) -> Result<Vec<calibration_outcome::Model>, StorageError>;

    async fn resolve_outcome(
        &self,
        outcome_id: &Uuid,
        actual_yes: bool,
    ) -> Result<(), StorageError>;
}
```

### 9.5 RiskStateRepository

```rust
#[async_trait]
pub trait RiskStateRepository: Send + Sync + 'static {
    async fn load(&self) -> Result<risk_state::Model, StorageError>;
    async fn save(&self, state: risk_state::ActiveModel) -> Result<(), StorageError>;
    async fn reset_hourly_window(&self) -> Result<(), StorageError>;
    async fn reset_daily_window(&self) -> Result<(), StorageError>;
    async fn reset_weekly_window(&self) -> Result<(), StorageError>;
}
```

### 9.6 TimeseriesRepository (ClickHouse)

```rust
#[async_trait]
pub trait TimeseriesRepository: Send + Sync + 'static {
    async fn insert_tick_events(&self, events: &[TickEventRow]) -> Result<(), StorageError>;
    async fn insert_book_snapshot(&self, snapshot: &BookSnapshotRow) -> Result<(), StorageError>;
    async fn insert_opportunity_audit(&self, audit: &OpportunityAuditRow) -> Result<(), StorageError>;
    async fn insert_calibration_snapshot(
        &self,
        snapshot: &CalibrationSnapshotRow,
    ) -> Result<(), StorageError>;

    async fn query_tick_events(
        &self,
        token_id: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<TickEventRow>, StorageError>;

    async fn query_opportunity_audit(
        &self,
        strategy: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<OpportunityAuditRow>, StorageError>;

    async fn query_calibration_history(
        &self,
        category: &str,
        price_zone: &str,
        duration_bucket: &str,
        days: u32,
    ) -> Result<Vec<CalibrationSnapshotRow>, StorageError>;
}
```

### 9.7 AccountingRepository

```rust
#[async_trait]
pub trait AccountingRepository: Send + Sync + 'static {
    async fn get_current_daily(&self) -> Result<Option<accounting::Model>, StorageError>;
    async fn get_current_weekly(&self) -> Result<Option<accounting::Model>, StorageError>;
    async fn upsert_period(
        &self,
        period: accounting::ActiveModel,
    ) -> Result<accounting::Model, StorageError>;
    async fn finalize_period(&self, period_id: &Uuid) -> Result<(), StorageError>;
    async fn get_history(
        &self,
        period_type: &str,
        limit: u64,
    ) -> Result<Vec<accounting::Model>, StorageError>;
}
```

---

## 10. ClickHouse BatchInserter

```rust
use tokio::sync::mpsc;
use std::time::Duration;

pub struct BatchInserter<T: clickhouse::Row> {
    tx: mpsc::Sender<T>,
    shutdown: CancellationToken,
}

impl<T: clickhouse::Row + Send + 'static> BatchInserter<T> {
    pub fn new(
        client: clickhouse::Client,
        table: &'static str,
        batch_size: usize,       // default: 1000
        flush_interval: Duration, // default: 5s
        shutdown: CancellationToken,
    ) -> Self {
        let (tx, rx) = mpsc::channel(batch_size * 4);

        tokio::spawn(Self::flush_loop(client, table, rx, batch_size, flush_interval, shutdown.clone()));

        Self { tx, shutdown }
    }

    pub async fn insert(&self, row: T) -> Result<(), StorageError> {
        self.tx
            .send(row)
            .await
            .map_err(|_| StorageError::ChannelClosed("BatchInserter channel closed".into()))
    }

    async fn flush_loop(
        client: clickhouse::Client,
        table: &'static str,
        mut rx: mpsc::Receiver<T>,
        batch_size: usize,
        flush_interval: Duration,
        shutdown: CancellationToken,
    ) {
        let mut buffer = Vec::with_capacity(batch_size);
        let mut interval = tokio::time::interval(flush_interval);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    // Drain remaining
                    while let Ok(row) = rx.try_recv() {
                        buffer.push(row);
                    }
                    if !buffer.is_empty() {
                        let _ = Self::flush(&client, table, &mut buffer).await;
                    }
                    break;
                }
                Some(row) = rx.recv() => {
                    buffer.push(row);
                    if buffer.len() >= batch_size {
                        let _ = Self::flush(&client, table, &mut buffer).await;
                    }
                }
                _ = interval.tick() => {
                    if !buffer.is_empty() {
                        let _ = Self::flush(&client, table, &mut buffer).await;
                    }
                }
            }
        }
    }

    async fn flush(
        client: &clickhouse::Client,
        table: &str,
        buffer: &mut Vec<T>,
    ) -> Result<(), StorageError> {
        let mut insert = client.insert(table)
            .map_err(|e| StorageError::ClickHouse(e.to_string()))?;
        for row in buffer.drain(..) {
            insert.write(&row).await
                .map_err(|e| StorageError::ClickHouse(e.to_string()))?;
        }
        insert.end().await
            .map_err(|e| StorageError::ClickHouse(e.to_string()))?;
        Ok(())
    }
}
```

---

## 11. Cargo.toml

### 11.1 oxide-arb-storage

```toml
[package]
name = "oxide-arb-storage"
description = "Database initialization, connection management, schema migrations, and unified cache layer"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[features]
default = []
test-util = ["dep:testcontainers", "dep:testcontainers-modules"]

[dependencies]
oxide-arb-models = { workspace = true }
sea-orm = { workspace = true }
sea-orm-migration = { workspace = true }
tokio = { workspace = true }
tokio-util = { workspace = true }
tracing = { workspace = true }
thiserror = { workspace = true }
async-trait = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
bitcode = { workspace = true }
prometheus = { workspace = true }
moka = { workspace = true }
deadpool-redis = { workspace = true }
redis = { workspace = true }
clickhouse = { workspace = true }
chrono = { workspace = true }
testcontainers = { workspace = true, optional = true }
testcontainers-modules = { workspace = true, optional = true }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
rust_decimal = { workspace = true }
rust_decimal_macros = { workspace = true }
testcontainers = { workspace = true }
testcontainers-modules = { workspace = true, features = ["postgres", "redis", "clickhouse"] }

[lints]
workspace = true
```

### 11.2 oxide-arb-repository

```toml
[package]
name = "oxide-arb-repository"
description = "Data access layer: repository pattern for SeaORM entities and ClickHouse timeseries"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[dependencies]
oxide-arb-models = { workspace = true }
oxide-arb-storage = { workspace = true }
sea-orm = { workspace = true }
chrono = { workspace = true }
tracing = { workspace = true }
async-trait = { workspace = true }
clickhouse = { workspace = true }
uuid = { workspace = true }
rust_decimal = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
rust_decimal = { workspace = true }
rust_decimal_macros = { workspace = true }
oxide-arb-storage = { workspace = true, features = ["test-util"] }

[lints]
workspace = true
```

---

## 12. 错误类型

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Database error: {0}")]
    Database(#[from] sea_orm::DbErr),

    #[error("ClickHouse error: {0}")]
    ClickHouse(String),

    #[error("Cache error: {0}")]
    Cache(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Migration error: {0}")]
    Migration(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Channel closed: {0}")]
    ChannelClosed(String),

    #[error("Not found: {0}")]
    NotFound(String),
}

impl From<StorageError> for oxide_arb_error::OxideError {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::Database(e) => Self::Database(e),
            StorageError::ClickHouse(s) => Self::ClickHouse(s),
            StorageError::Cache(s) => Self::Cache(s),
            _ => Self::Internal(e.to_string()),
        }
    }
}
```

---

## 13. 测试策略

### 13.1 单元测试

- `CacheKey` 格式化 + TTL 映射
- `MokaBackend` get/set/delete/mget/mset
- `TieredCache` fallthrough 逻辑 (L1 miss → L2 hit → L1 backfill)
- `BatchInserter` buffer accumulation + flush on size/interval/shutdown
- bitcode 序列化/反序列化 round-trip

### 13.2 集成测试 (testcontainers)

```rust
#[tokio::test]
async fn test_market_repository_crud() {
    let pg = testcontainers::GenericImage::new("postgres", "16")
        .with_env_var("POSTGRES_DB", "test")
        .with_env_var("POSTGRES_PASSWORD", "test")
        .start()
        .await;

    let pool = PostgresPool::connect(&format!(
        "postgres://postgres:test@localhost:{}/test",
        pg.get_host_port_ipv4(5432).await
    )).await.unwrap();

    Migrator::up(&pool.connection(), None).await.unwrap();

    let repo = PgMarketRepository::new(pool.connection());
    // ... CRUD assertions
}
```

### 13.3 ClickHouse 集成测试

```rust
#[tokio::test]
async fn test_timeseries_insert_and_query() {
    let ch = testcontainers_modules::clickhouse::ClickHouse::default()
        .start()
        .await;

    let client = ClickHousePool::connect(&format!(
        "http://localhost:{}",
        ch.get_host_port_ipv4(8123).await
    )).await.unwrap();

    client.ensure_schema().await.unwrap();

    let repo = ChTimeseriesRepository::new(client);
    // Insert tick events, query back, assert ordering and values
}
```

### 13.4 Redis 集成测试

```rust
#[tokio::test]
async fn test_redis_cache_backend() {
    let redis = testcontainers::GenericImage::new("redis", "7")
        .start()
        .await;

    let backend = RedisBackend::new(&RedisConfig {
        url: format!("redis://localhost:{}", redis.get_host_port_ipv4(6379).await),
        key_prefix: Some("test:".into()),
    }).await.unwrap();

    backend.set("key1", b"value1", Duration::from_secs(60)).await.unwrap();
    let val = backend.get("key1").await.unwrap();
    assert_eq!(val, Some(b"value1".to_vec()));
}
```

---

## 14. 验收检查清单

- [ ] PostgreSQL 全部 10 个 migration 执行成功，`sea-orm-cli migrate status` 无 pending
- [ ] 所有 SeaORM entity 的 CRUD 操作通过集成测试
- [ ] `MarketRepository::find_endgame_candidates()` 正确按 `end_date` 筛选
- [ ] `PositionRepository::total_exposure()` 精确聚合（NUMERIC 精度无损）
- [ ] ClickHouse 表与物化视图 DDL 幂等创建（不包含已废弃的 `signal_data`）
- [ ] `BatchInserter` 在 shutdown 时 drain 剩余 buffer
- [ ] `TieredCache` L1 miss → L2 hit 路径自动 backfill L1
- [ ] `TieredCache` L1+L2 both miss 路径正确返回 `None`
- [ ] Redis `PING` 失败时 `RedisBackend::new()` 返回 `StorageError::Connection`
- [ ] 缓存命中率 prometheus metrics 可被 scrape
- [ ] `CalibrationRepository` 支持 4-tier fallback 查询（bucket → category → zone → global）
- [ ] bitcode 序列化 round-trip 测试覆盖所有 cached 类型
- [ ] ClickHouse TTL 策略验证（`tick_events` 90 天、`opportunity_audit` 365 天）
- [ ] 无 `unwrap()` in production code（全部返回 `StorageError`）

---

## 15. 预估工作量

| 组件 | 源码 LoC | 测试 LoC |
|---|---|---|
| `postgres/` (pool + migrations) | ~1,200 | ~400 |
| `clickhouse/` (pool + schema + inserter) | ~600 | ~300 |
| `cache/` (backends + tiered + keys + metrics) | ~800 | ~500 |
| `error.rs` | ~80 | ~30 |
| `repository/traits/` | ~400 | — |
| `repository/postgres/` | ~1,500 | ~800 |
| `repository/clickhouse/` | ~500 | ~300 |
| `repository/cached/` | ~300 | ~200 |
| **合计** | **~5,380** | **~2,530** |

---

## Phase 2 补充 — 关键缺口修补（Phase 4+ 计划）

### S1. Reports 表 Migration

**已完成**。`m20250601_000016_create_reports.rs`:
- `report` 表：`id(PK)`, `report_type`, `period_start`, `period_end`, `payload(TEXT)`, `created_at`
- 索引：`idx_report_type_period` on `(report_type, period_start DESC)`
- Entity: `entities/report.rs` (Serialize/Deserialize)
- Domain DTO: `domain/report.rs` (`NewReport::daily/weekly`)
- Repository: trait `ReportRepository` + `PgReportRepository` impl

### S2. CacheKey 扩展

**已完成**。新增 5 个 CacheKey 变体：
| 变体 | L2 TTL | 用途 |
|------|--------|------|
| `MarketMetadata { market_id }` | 30min | 检测/评分用市场元数据 |
| `AllCalibrationBuckets` | 1h | 批量 calibration 加载 |
| `PositionSummary { market_id }` | 30s | pre-trade 风控查询 |
| `RiskState` | 60s | 风控状态快照 |
| `Balance` | 15s | Polymarket 余额快照 |
| `RuntimeConfig { key }` / `AllRuntimeConfig` | 60s | 热配置查询 |
| `FeeParams { category }` | 10min | 分类 fee 参数 |

### S2.1 Deferred Service-Owned Cache Hooks

以下 `CacheKey` 已完成类型定义，但不在 repository wrapper 中立即落地，因为它们是 service 聚合结果或外部服务快照，必须由后续 service owner 掌握写路径失效：

| CacheKey | 延期原因 | 后续落地位置 |
|---|---|---|
| `PositionSummary { market_id }` | 需要风控/持仓服务聚合 open positions，并在 position/trade/settlement 更新后统一失效 | Phase 4 `RiskMetrics` / position risk service |
| `Balance` | 依赖 Polymarket wallet/balance 查询和订单提交/确认后的失效入口 | Phase 4 balance/risk service |
| `FeeParams { category }` | 依赖 fee runtime config 或 fee refresh service 拥有更新和失效入口 | Phase 1/Phase 4 fee rate source/service |

### S3. CachedCalibrationRepository

**已完成**。`cached/calibration.rs`:
- 泛型 `CachedCalibrationRepository<R: CalibrationRepository>`
- `get_bucket` / `get_all_buckets`: L1+L2 → PG fallback → backfill (serde_json codec)
- `insert_bucket` / `update_bucket`: delegate + invalidate granular + bulk keys
- Outcome 操作直接透传（写频率高但不适合缓存）

### S4. TieredCache serde_json 方法

**已完成**。`cache/tiered.rs` 新增 `get_json<T>` / `set_json<T>` 方法：
- 用于无法 derive `bitcode::Encode/Decode` 的 SeaORM entity 类型
- 通过 `StorageError::Codec(String)` 报告序列化错误
