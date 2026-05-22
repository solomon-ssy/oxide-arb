# Phase 5 — 回放与分析引擎

> **产出**: `oxide-arb-replay` crate
>
> **前置条件**: Phase 2（ClickHouse `tick_events` + `book_snapshots` 表已就绪）、Phase 3（`oxide-arb-algorithm` endgame 检测器可用）
>
> **验收标准**: CLI 可按时间范围 + 市场过滤器回放历史 tick，重建 L2 orderbook，运行 endgame 检测算法，输出 paper-trade PnL 报告；所有报告指标（命中率、边际分布、市场覆盖）与实时系统可交叉校验

---

## 0. 工作范围

1. 从 ClickHouse `tick_events` + `book_snapshots` 表高效拉取历史数据
2. 基于 tick 流重建 L2 orderbook（`BookReplayer`）
3. 在重建的 orderbook 上运行 endgame 检测算法（复用 `oxide-arb-algorithm`）
4. 使用可配置填充模型模拟交易执行（`PaperTradeSimulator`）
5. 聚合 per-market + global 统计报告（`ReplayReport`）
6. 提供 CLI 入口 + 为后续 web API 暴露预留接口

---

## 1. 目录结构

```
crates/oxide-arb-replay/
├── Cargo.toml
└── src/
    ├── lib.rs                  # 公共导出 + run_replay_cli() 入口
    ├── config.rs               # ReplayConfig, MarketFilter, StepMode, PaperTradeConfig
    ├── engine.rs               # ReplayEngine — 核心驱动循环
    ├── book_replayer.rs        # BookReplayer — L2 orderbook 重建
    ├── detector_bridge.rs      # 桥接 oxide-arb-algorithm endgame 检测器
    ├── paper_trade.rs          # PaperTradeSimulator — 模拟填充引擎
    ├── fill_model.rs           # FillModel trait + 实现（TopOfBook, TWAP, DepthWeighted）
    ├── report.rs               # ReplayReport, MarketReplayReport, EdgeDistribution
    └── ch_queries.rs           # ClickHouse 查询模板 + 优化策略
```

---

## 2. Cargo.toml

```toml
[package]
name = "oxide-arb-replay"
description = "Historical replay and paper-trade simulation over ClickHouse ticks"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[dependencies]
oxide-arb-error = { workspace = true }
oxide-arb-models = { workspace = true }
oxide-arb-storage = { workspace = true }
oxide-arb-repository = { workspace = true }
oxide-arb-algorithm = { workspace = true }

# Async
tokio = { workspace = true }
async-trait = { workspace = true }

# Data
chrono = { workspace = true }
time = { workspace = true }
rust_decimal = { workspace = true }
rust_decimal_macros = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }

# ClickHouse
clickhouse = { workspace = true }

# Metrics
prometheus = { workspace = true }

# Logging
tracing = { workspace = true }

# Error
anyhow = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
testcontainers = { workspace = true }
testcontainers-modules = { workspace = true }
tempfile = { workspace = true }

[lints]
workspace = true
```

---

## 3. ReplayConfig 设计

```rust
use chrono::{DateTime, Utc};
use oxide_arb_models::types::MarketId;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Top-level replay session configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    /// Replay window start (inclusive).
    pub from: DateTime<Utc>,
    /// Replay window end (inclusive).
    pub to: DateTime<Utc>,
    /// Which markets to replay.
    pub markets: MarketFilter,
    /// Tick processing speed control.
    pub step_mode: StepMode,
    /// Enable paper-trade simulation alongside replay.
    pub paper_trade: Option<PaperTradeConfig>,
    /// Enable endgame detection over replayed books.
    pub run_detection: bool,
    /// Output report path (JSON). None → stdout only.
    pub output_path: Option<PathBuf>,
}

/// Market selection for replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MarketFilter {
    /// Replay all markets with data in the time range.
    All,
    /// Replay only specific markets by condition_id.
    Specific(Vec<MarketId>),
    /// Replay markets matching a category filter.
    Category(MarketCategory),
}

/// Controls how fast ticks are replayed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum StepMode {
    /// Process ticks as fast as the CPU allows (benchmarking).
    AsFastAsPossible,
    /// Replay at wall-clock speed (1:1 with original timestamps).
    RealTime,
    /// Replay at N× speed (e.g. 10.0 = 10× faster than real time).
    TimeScaled(f64),
}

impl Default for ReplayConfig {
    fn default() -> Self {
        let to = Utc::now();
        let from = to - chrono::Duration::hours(1);
        Self {
            from,
            to,
            markets: MarketFilter::All,
            step_mode: StepMode::AsFastAsPossible,
            paper_trade: Some(PaperTradeConfig::default()),
            run_detection: true,
            output_path: None,
        }
    }
}
```

---

## 4. ReplayEngine 设计

核心驱动循环，协调数据拉取、book 重建、检测、paper-trade 四个阶段。

```rust
pub struct ReplayEngine {
    config: ReplayConfig,
    ch: Arc<ClickhouseClient>,
    detector: Option<Arc<EndgameDetector>>,
}

impl ReplayEngine {
    pub fn new(
        config: ReplayConfig,
        ch: Arc<ClickhouseClient>,
        detector: Option<Arc<EndgameDetector>>,
    ) -> Self { ... }

    /// Execute the full replay pipeline.
    pub async fn run(&self) -> anyhow::Result<ReplayReport> {
        // 1. resolve_markets() — 从 CH 查询时间范围内的 distinct market_id
        // 2. 遍历每个 market:
        //    a. 加载初始 book_snapshot（replay 窗口前最近的快照）
        //    b. 流式加载 tick_events（分批拉取，每批 10K ticks）
        //    c. 逐 tick 驱动 BookReplayer 重建 L2
        //    d. 如果 run_detection=true，每 tick 后调用 detector.evaluate()
        //    e. 如果 paper_trade 有值，用 PaperTradeSimulator 模拟执行
        //    f. 按 StepMode 控制 tick 处理节奏
        // 3. 聚合所有 market 报告 → ReplayReport
        todo!()
    }

    /// Query distinct market_ids from tick_events within the time range.
    async fn resolve_markets(&self) -> anyhow::Result<Vec<String>> { ... }

    /// Process a single market's tick stream.
    async fn replay_market(
        &self,
        market_id: &str,
    ) -> anyhow::Result<MarketReplayReport> { ... }
}
```

### 4.1 Step Mode 实现

```rust
/// Controls tick replay pacing based on original timestamps.
struct Pacer {
    mode: StepMode,
    last_tick_ts: Option<u64>,
}

impl Pacer {
    async fn pace(&mut self, tick_timestamp: u64) {
        match self.mode {
            StepMode::AsFastAsPossible => {},
            StepMode::RealTime => {
                if let Some(last) = self.last_tick_ts {
                    let delta = Duration::from_millis(tick_timestamp - last);
                    tokio::time::sleep(delta).await;
                }
            }
            StepMode::TimeScaled(factor) => {
                if let Some(last) = self.last_tick_ts {
                    let real_delta = tick_timestamp - last;
                    let scaled = Duration::from_millis((real_delta as f64 / factor) as u64);
                    tokio::time::sleep(scaled).await;
                }
            }
        }
        self.last_tick_ts = Some(tick_timestamp);
    }
}
```

---

## 5. BookReplayer（L2 重建）

从 tick 流增量重建双边 orderbook。内部维护 `BTreeMap<OrderedFloat<f64>, f64>` 保证价格有序。

```rust
pub struct BookReplayer {
    market_id: String,
    bids: BTreeMap<OrderedFloat, f64>,
    asks: BTreeMap<OrderedFloat, f64>,
    ticks_applied: u64,
    snapshot_loaded: bool,
    notes: Vec<String>,
}

impl BookReplayer {
    pub fn new(market_id: impl Into<String>) -> Self { ... }

    /// Seed the book from a pre-replay snapshot.
    pub fn load_snapshot(&mut self, snap: &BookSnapshotRow) { ... }

    /// Apply a single tick event, updating the appropriate side.
    pub fn apply_tick(&mut self, tick: &TickEventRow) {
        // Dispatch by event_type:
        //   PriceChange/SizeChange/BookUpdate → insert(price, size) or remove if size=0
        //   BookDelete → remove(price)
        //   Trade → decrement size at price level, remove if ≤ 0
        //   Reset/Pause/Resume → no-op (logged as note)
    }

    /// Batch apply all ticks in order.
    pub fn apply_all(&mut self, ticks: &[TickEventRow]) { ... }

    /// Current best bid price.
    pub fn best_bid(&self) -> Option<f64> { ... }

    /// Current best ask price.
    pub fn best_ask(&self) -> Option<f64> { ... }

    /// Current spread in price units.
    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        }
    }

    /// Top N levels of the bid side (price descending).
    pub fn bid_depth(&self, levels: usize) -> Vec<(f64, f64)> { ... }

    /// Top N levels of the ask side (price ascending).
    pub fn ask_depth(&self, levels: usize) -> Vec<(f64, f64)> { ... }

    /// Total liquidity in USD at top N levels on both sides.
    pub fn depth_usd(&self, levels: usize) -> f64 { ... }

    /// Produce the per-market report and consume self.
    pub fn into_report(self) -> MarketReplayReport { ... }
}
```

---

## 6. 检测器桥接（detector_bridge.rs）

将 `oxide-arb-algorithm` 的 endgame 检测器适配到 replay 上下文。replay 不需要实时市场元数据，而是从 ClickHouse 快照构建 `DetectionInput`。

```rust
use oxide_arb_algorithm::endgame::{EndgameDetector, DetectionInput, DetectionResult};

/// Adapts the live endgame detector for historical replay.
pub struct ReplayDetectorBridge {
    detector: Arc<EndgameDetector>,
}

impl ReplayDetectorBridge {
    pub fn new(detector: Arc<EndgameDetector>) -> Self { ... }

    /// Build a DetectionInput from replayed book state + historical metadata.
    pub fn evaluate(
        &self,
        market_id: &str,
        book: &BookReplayer,
        tick_timestamp: u64,
        market_meta: &ReplayMarketMeta,
    ) -> Option<DetectionResult> {
        let input = DetectionInput {
            market_id: market_id.into(),
            best_bid: book.best_bid()?,
            best_ask: book.best_ask()?,
            bid_depth: book.bid_depth(10),
            ask_depth: book.ask_depth(10),
            timestamp: tick_timestamp,
            category: market_meta.category,
            end_date: market_meta.end_date,
            // ... other fields from market metadata snapshot
        };
        self.detector.evaluate(&input)
    }
}

/// Historical market metadata loaded once per market for replay context.
#[derive(Debug, Clone)]
pub struct ReplayMarketMeta {
    pub category: MarketCategory,
    pub end_date: Option<DateTime<Utc>>,
    pub question: String,
    pub outcome_yes: String,
    pub outcome_no: String,
    pub resolved: Option<bool>,
    pub actual_outcome: Option<bool>,
}
```

---

## 7. PaperTradeSimulator 设计

模拟交易执行引擎，支持可配置的填充模型。与实时系统的 `Executor` 接口解耦，专注于历史回测的准确性。

### 7.1 配置

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperTradeConfig {
    /// Fill model to simulate order execution.
    pub fill_model: FillModelType,
    /// Maximum position size per market (USD).
    pub max_position_usd: Decimal,
    /// Taker fee rate (applied to each simulated fill).
    pub taker_fee_rate: Decimal,
    /// Minimum edge (bps) required to trigger entry.
    pub min_edge_bps: Decimal,
    /// Slippage buffer added to entry price (bps).
    pub slippage_bps: Decimal,
    /// Maximum concurrent open positions across all markets.
    pub max_open_positions: usize,
}

impl Default for PaperTradeConfig {
    fn default() -> Self {
        Self {
            fill_model: FillModelType::TopOfBook,
            max_position_usd: dec!(50),
            taker_fee_rate: dec!(0.001),
            min_edge_bps: dec!(200),
            slippage_bps: dec!(10),
            max_open_positions: 10,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FillModelType {
    /// Fill at the best available price (optimistic).
    TopOfBook,
    /// Walk the book to fill the requested size, consuming depth.
    DepthWeighted,
    /// Apply a fixed slippage penalty on top of mid-price.
    FixedSlippage,
}
```

### 7.2 模拟器核心

```rust
pub struct PaperTradeSimulator {
    config: PaperTradeConfig,
    fill_model: Box<dyn FillModel>,
    open_positions: HashMap<String, SimulatedPosition>,
    closed_trades: Vec<SimulatedTrade>,
    total_fees: Decimal,
}

impl PaperTradeSimulator {
    pub fn new(config: PaperTradeConfig) -> Self { ... }

    /// Called on each tick after book is updated + detection runs.
    pub fn on_detection(
        &mut self,
        market_id: &str,
        detection: &DetectionResult,
        book: &BookReplayer,
        tick_timestamp: u64,
    ) {
        // 1. Check if detection edge >= min_edge_bps
        // 2. Check max_open_positions limit
        // 3. Compute position size (respecting max_position_usd)
        // 4. Simulate fill via fill_model.execute()
        // 5. Record entry in open_positions
    }

    /// Called when a market resolves — settle all open positions.
    pub fn on_resolution(
        &mut self,
        market_id: &str,
        actual_yes: bool,
        settlement_price: Decimal,
    ) {
        // 1. Look up open position for this market
        // 2. Compute settlement PnL = shares × (settlement_price - entry_price) - fees
        // 3. Move to closed_trades
    }

    /// Force-close all remaining positions at current book prices.
    pub fn settle_all(&mut self, books: &HashMap<String, BookReplayer>) { ... }

    /// Produce the paper-trade report.
    pub fn into_report(self) -> PaperTradeReport { ... }
}

/// A single simulated trade record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedTrade {
    pub market_id: String,
    pub side: Side,
    pub entry_price: Decimal,
    pub exit_price: Option<Decimal>,
    pub shares: Decimal,
    pub cost: Decimal,
    pub fees: Decimal,
    pub pnl: Decimal,
    pub entry_tick: u64,
    pub exit_tick: Option<u64>,
    pub predicted_yes: bool,
    pub actual_yes: Option<bool>,
    pub outcome: TradeOutcome,
}
```

### 7.3 FillModel Trait

```rust
pub trait FillModel: Send + Sync {
    /// Simulate filling `desired_shares` against the book.
    /// Returns (actual_shares_filled, average_fill_price).
    fn execute(
        &self,
        desired_shares: Decimal,
        side: Side,
        book: &BookReplayer,
        slippage_bps: Decimal,
    ) -> Option<(Decimal, Decimal)>;
}

/// Fills at best bid/ask price, ignoring depth constraints.
pub struct TopOfBookFill;

/// Walks the book depth, computing volume-weighted average price.
pub struct DepthWeightedFill;

/// Applies a fixed slippage penalty on top of mid-price.
pub struct FixedSlippageFill;
```

---

## 8. ReplayReport 聚合

### 8.1 报告结构

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayReport {
    /// Replay session metadata.
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub config_summary: String,

    /// Coverage statistics.
    pub markets_replayed: usize,
    pub markets_skipped: usize,
    pub total_ticks: u64,
    pub time_range_hours: f64,

    /// Per-market breakdowns.
    pub market_reports: Vec<MarketReplayReport>,

    /// Detection statistics (if run_detection=true).
    pub detection_stats: Option<DetectionStats>,

    /// Paper-trade results (if paper_trade enabled).
    pub paper_trade: Option<PaperTradeReport>,

    /// Notes and warnings.
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketReplayReport {
    pub market_id: String,
    pub ticks_applied: u64,
    pub initial_snapshot_loaded: bool,
    pub final_best_bid: Option<f64>,
    pub final_best_ask: Option<f64>,
    pub bid_levels: usize,
    pub ask_levels: usize,
    /// Detections found on this market (empty if detection disabled).
    pub detections: Vec<DetectionSnapshot>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectionStats {
    /// Total opportunities detected across all markets.
    pub total_detections: usize,
    /// Markets that had at least one detection.
    pub markets_with_detections: usize,
    /// Edge distribution (bps → count).
    pub edge_distribution: EdgeDistribution,
    /// Average edge of detected opportunities (bps).
    pub avg_edge_bps: Decimal,
    /// Median edge (bps).
    pub median_edge_bps: Decimal,
    /// Detection rate: detections per 1000 ticks.
    pub detection_rate_per_1k_ticks: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeDistribution {
    /// Histogram buckets: [200-300), [300-500), [500-1000), [1000+) bps.
    pub buckets: Vec<EdgeBucket>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBucket {
    pub min_bps: u32,
    pub max_bps: Option<u32>,
    pub count: usize,
    pub avg_edge: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperTradeReport {
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub hit_rate: Decimal,
    pub gross_pnl: Decimal,
    pub total_fees: Decimal,
    pub net_pnl: Decimal,
    pub max_drawdown: Decimal,
    pub sharpe_ratio: Option<f64>,
    pub avg_hold_duration_secs: f64,
    pub trades: Vec<SimulatedTrade>,
}
```

---

## 9. ClickHouse 查询模板

高效拉取历史数据的关键在于减少全表扫描。`tick_events` 和 `book_snapshots` 均按 `(market_id, received_at)` 分区，以下查询利用分区剪裁和排序键。

### 9.1 查询 distinct markets in time range

```sql
SELECT DISTINCT market_id
FROM tick_events
WHERE received_at >= {from:DateTime64(9)}
  AND received_at <= {to:DateTime64(9)}
ORDER BY market_id
```

### 9.2 分批加载 tick_events（流式 cursor）

```sql
SELECT seq_id, market_id, token_id, event_type, side, price, size, received_at
FROM tick_events
WHERE market_id = {market_id:String}
  AND received_at >= {from:DateTime64(9)}
  AND received_at <= {to:DateTime64(9)}
ORDER BY received_at, seq_id
LIMIT {batch_size:UInt64}
OFFSET {offset:UInt64}
```

batch_size 建议 10,000 行，避免单次查询内存过高。对于长时间范围（>24h），使用 `OFFSET` 分页或改用 `seq_id > last_seen_seq` 游标分页以避免 OFFSET 性能退化。

### 9.3 加载 replay 窗口前最近的 book_snapshot

```sql
SELECT *
FROM book_snapshots
WHERE market_id = {market_id:String}
  AND snapshot_at <= {from:DateTime64(9)}
ORDER BY snapshot_at DESC
LIMIT 1
```

### 9.4 性能优化策略

| 策略 | 说明 |
|---|---|
| 分区剪裁 | `tick_events` 按月分区，查询时 WHERE 子句自动跳过无关分区 |
| 排序键利用 | 表按 `(market_id, received_at, seq_id)` 排序，范围扫描高效 |
| 批量拉取 | 每次 10K 行，避免单次大查询的内存压力 |
| 游标分页 | 长范围回放使用 `seq_id > last_seen` 而非 OFFSET |
| 预聚合跳过 | 如果 market 在 replay 范围内 tick 数 < 10，自动跳过并记录 note |
| 并发市场 | 不同 market 的查询可并发（受 CH connection pool 限制） |

---

## 10. CLI 入口

```rust
/// CLI / one-off entry: connect to CH using settings.analytics, run replay.
pub async fn run_replay_cli(settings: &Settings) -> anyhow::Result<ReplayReport> {
    tracing::info!("replay: connecting to ClickHouse");

    let registry = prometheus::Registry::new();
    let metrics = AnalyticsMetrics::new(&registry)?;
    let ch = ClickhouseClient::connect(&settings.analytics, Arc::new(metrics)).await?;

    let config = ReplayConfig::from_cli_args()?; // 或使用 default
    let detector = if config.run_detection {
        Some(Arc::new(EndgameDetector::new(&settings.detection)?))
    } else {
        None
    };
    let engine = ReplayEngine::new(config, Arc::new(ch), detector);

    let report = engine.run().await?;

    if let Some(ref path) = engine.config.output_path {
        let json = serde_json::to_string_pretty(&report)?;
        std::fs::write(path, json)?;
        tracing::info!(?path, "replay report written");
    }

    print_summary(&report);
    Ok(report)
}

fn print_summary(report: &ReplayReport) {
    println!("=== Replay Report ===");
    println!("Duration: {:.1}s", (report.finished_at - report.started_at).num_milliseconds() as f64 / 1000.0);
    println!("Markets: {} replayed, {} skipped", report.markets_replayed, report.markets_skipped);
    println!("Ticks: {}", report.total_ticks);

    if let Some(ref det) = report.detection_stats {
        println!("Detections: {} total, avg edge {:.0} bps", det.total_detections, det.avg_edge_bps);
    }

    if let Some(ref pt) = report.paper_trade {
        println!("Paper trades: {} ({} W / {} L), hit rate {:.1}%",
            pt.total_trades, pt.winning_trades, pt.losing_trades,
            pt.hit_rate * dec!(100));
        println!("Net PnL: ${}, max drawdown: ${}", pt.net_pnl, pt.max_drawdown);
    }
}
```

### Web API 暴露（预留）

`ReplayEngine` 和 `ReplayReport` 设计为 `Send + Sync`，未来 Phase 6 web 层可直接暴露：

```
POST /api/v1/replay          → 提交 ReplayConfig，异步执行
GET  /api/v1/replay/{id}     → 查询 replay 任务状态/报告
GET  /api/v1/replay/history  → 历史 replay 记录列表
```

---

## 11. 验收检查清单

- [ ] `ReplayEngine` 可连接 ClickHouse 并按时间范围 + 市场过滤器拉取 tick 数据
- [ ] `BookReplayer` 从 tick 流重建 L2 orderbook，best_bid/best_ask 与原始数据一致
- [ ] 如果有初始 `book_snapshot`，正确加载后再增量 apply ticks
- [ ] 如果无初始 snapshot，从空 book 冷启动并记录 warning
- [ ] endgame 检测器可在 replayed book 上运行，产出与实时系统格式一致的 `DetectionResult`
- [ ] `PaperTradeSimulator` 基于检测结果模拟交易，支持三种 FillModel
- [ ] 模拟交易正确计算 PnL（entry cost + fees vs settlement payout）
- [ ] `ReplayReport` 包含 per-market 统计 + global 聚合 + edge distribution
- [ ] CLI 可一行命令运行 replay 并输出 JSON 报告
- [ ] StepMode::RealTime 正确按原始时间戳间隔回放
- [ ] 回放 1 小时数据（~50K ticks）在 AsFastAsPossible 模式下 < 5 秒完成
- [ ] 所有 ClickHouse 查询利用分区键和排序键，无全表扫描

---

## 12. 预估工作量

| 组件 | 源码 LoC | 测试 LoC |
|---|---|---|
| `config.rs` | ~80 | ~40 |
| `engine.rs` | ~250 | ~100 |
| `book_replayer.rs` | ~180 | ~120 |
| `detector_bridge.rs` | ~100 | ~60 |
| `paper_trade.rs` | ~200 | ~150 |
| `fill_model.rs` | ~120 | ~80 |
| `report.rs` | ~150 | ~40 |
| `ch_queries.rs` | ~80 | ~30 |
| `lib.rs` (CLI entry) | ~60 | — |
| **合计** | **~1,220** | **~620** |

---

## Phase 5 补充 — CH Schema 粒度增强（Phase 4+ 计划）

### tick_events_l2 表

**已完成**。新增 `tick_events_l2` 表存储完整 L2 orderbook level 数据：

- 使用 `Array(Decimal64(8))` 列（bid_prices/sizes, ask_prices/sizes）避免 JSON 解析
- 支持 `snapshot`（全量替换）和 `delta`（增量更新）两种事件类型
- 90 天 TTL，按月分区
- DDL: `crates/oxide-arb-storage/src/clickhouse/sql/tick_events_l2.sql`
- Row: `crates/oxide-arb-models/src/clickhouse/tick_event_l2.rs` (`TickEventL2Row`)

### 写入路径

`DataPipeline` 收到 WS event 后双写：
1. `tick_events` — 轻量 BBO 级（现有）
2. `tick_events_l2` — 完整 L2 级（新增）

### BookReplayer 数据源变更

`BookReplayer` 优先从 `tick_events_l2` 消费 Array 型数据做 L2 重建，
fallback 到 `book_snapshots` JSON 解析（兼容历史数据）。
