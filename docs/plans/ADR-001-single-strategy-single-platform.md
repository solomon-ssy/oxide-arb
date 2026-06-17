# ADR-001: 单策略（Endgame）、单平台（Polymarket）架构决策

> **状态**: 已批准
>
> **日期**: 2025-05-20
>
> **影响范围**: 整个 oxide-arb workspace（所有 crate、配置、领域模型、API 设计）

---

## 1. 决策背景

oxide-arb 重写的根本动因是**聚焦**：放弃之前代码中为多策略（cross-book arb、statistical arb、endgame 等）和多平台（Polymarket、Kalshi、Manifold 等）预留的所有抽象层，全力做好一件事——**在 Polymarket 上执行 Endgame Convergence 策略**。

之前的设计犯了"过度工程化"的错误：
- `VenueId` 枚举预留了多平台支持
- `OpportunityType` 枚举包含了 `CrossBookArb`、`StatisticalArb`、`DirectionalBet` 等
- `Strategy` trait 为多策略抽象
- 配置层面 `VenuesConfig` 预留了多个 venue 的 slot
- 数据层 `strategy` 字段贯穿所有表（trades、positions、opportunities）

**这些抽象不仅没有价值，还增加了认知负担和出 bug 的表面积。**

---

## 2. 核心决策

### 2.1 单策略：仅 Endgame Convergence

| 保留 | 删除 |
|---|---|
| Endgame 收敛检测 | 任何多策略路由/分发逻辑 |
| Resolution Calibration 系统 | `OpportunityType` 多变体枚举 |
| Convergence Tracker | `Strategy` trait / dynamic dispatch |
| Quarter-Kelly + fill probability | 多策略评分/排序逻辑 |
| 方向性持仓管理 | Hedging 逻辑（endgame 不对冲） |
| **FOK-only** 执行 | 多腿（multi-leg）订单构建；GTD resting 订单（Endgame Live 不使用） |

**含义**：
- `OpportunityType` 枚举只保留 `DirectionalBet`（或直接去掉枚举，因为只有一种类型）
- 所有 DB 表中 `strategy` 字段硬编码为 `"endgame"` 或直接删除
- `ExecutionFSM` 中 `Hedging` 和 `Emergency`（对冲失败引发）状态可简化
- 不需要"策略注册表"或"策略工厂"模式

### 2.2 单平台：仅 Polymarket（Polygon 链）

| 保留 | 删除 |
|---|---|
| Polymarket CLOB REST/WS | `VenueId` 枚举（多 venue 路由） |
| Gamma API | 多平台数据适配层 |
| CTF Exchange 交互 | 任何跨平台套利逻辑 |
| EIP-712 签名 | 多签名方案抽象 |
| USDC.e (Polygon) | 多链/多 token 支持 |
| Polygon RPC | 多 RPC provider 抽象 |

**含义**：
- `VenueConfig` 直接代表 Polymarket，不是某个泛化 venue 的一个实例
- `constants.rs` 中的合约地址是**唯一**的合约地址，不存在"另一个平台"的可能
- 费率计算硬编码为 Polymarket 公式，不需要 `FeeCalculator` trait
- 签名逻辑硬编码为 EIP-712 Polymarket format

---

## 3. 需要删除的设计元素

### 3.1 从 Phase 0（oxide-arb-models）中移除

| 元素 | 当前位置 | 处理方式 |
|---|---|---|
| `types/venue.rs` (VenueId enum) | `oxide-arb-models/src/types/venue.rs` | **删除整个文件**。不再有 venue 抽象 |
| `OpportunityType::CrossBookArb` 等多余变体 | `enums/common.rs` | **删除枚举本身**。只有一种策略，不需要类型标记 |
| `TradeOutcome::HedgeLoss`, `Unhedged` | `enums/common.rs` | **删除**。Endgame 不对冲 |
| `constants.rs` 中的 trading thresholds | `constants.rs` lines 26-41 | **删除**。全部归入 config 结构体 |

### 3.2 从 Phase 1（oxide-arb-api）中简化

| 元素 | 处理方式 |
|---|---|
| `FeeService` trait | **改为具体 struct**。只有一个 Polymarket 费率实现 |
| `OracleSource` trait + 多 source 投票 | **保留但简化**。VotingOracle 的 2-of-3 quorum 是好设计，但不是为了支持"多平台" |
| `KeyLoader` trait | **改为具体 struct**。只需支持 hex 环境变量加载 |

### 3.3 从 Phase 4（oxide-arb-core）中移除

| 元素 | 处理方式 |
|---|---|
| `ExecState::Hedging` | **删除**。Endgame 没有对冲步骤 |
| `ExecState::Emergency`（对冲失败） | **删除**。Emergency 仅指系统级故障，不是对冲失败 |
| Scanner 的多策略路由 | **删除**。只有一个 pipeline |
| `Opportunity<M>` 泛型 | **改为具体 struct**。不再需要多态 |

### 3.4 从 DB Schema 中移除

| 字段/表 | 处理方式 |
|---|---|
| `trades.strategy` | **删除列**。所有 trade 都是 endgame |
| `positions.strategy` | **删除列** |
| `opportunity_audit.strategy` | **删除列** |

---

## 4. constants.rs 精简方案

### 4.1 保留为常量的（不可变的链上事实）

这些是链上合约地址和协议参数，**永远不会通过配置变更**：

```rust
// ── Polymarket Contract Addresses (Polygon) ─────────────────────────────
pub const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
pub const NEG_RISK_CTF_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";
pub const USDC_E: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
pub const CTF_ADDRESS: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
pub const POLYGON_CHAIN_ID: u64 = 137;

// ── USDC Decimals ───────────────────────────────────────────────────────
pub const USDC_DECIMALS: u8 = 6;
pub const USDC_SCALE: u64 = 1_000_000;
```

### 4.2 必须删除的（已冗余于 config）

| 常量 | 冗余于 | 删除原因 |
|---|---|---|
| `MIN_PROFIT_THRESHOLD` | `DetectionConfig::min_profit_threshold_usd` | 运行时可调参数，不应硬编码 |
| `MIN_EDGE_BPS` | `KellyConfig::min_edge_bps` | 运行时可调 |
| `MAX_DEPTH_USAGE_PCT` | 应在 `DetectionConfig` 或 `RiskConfig` | 运行时可调 |
| `MIN_DEPTH_USD` | 应在 `DetectionConfig` | 运行时可调 |
| `KELLY_FRACTION` | `PositionSizingConfig::kelly_fraction` | 运行时可调 |

**关键原则**：常量只存放**协议级不可变事实**。所有交易参数必须通过配置系统管理，支持运行时热更新。

---

## 5. Config 两层架构（Deploy / Runtime，已落地）

配置拆为两层，边界以「是否需要重启才能生效」为准：

### 5.1 DeployConfig（`config/oxide-arb.toml` + `OXIDE_ARB__*` env，重启生效）

只包含进程启动时结构性绑定的内容，全树 `deny_unknown_fields`（未知键/遗留段直接拒绝启动）：

```
DeployConfig (oxide_arb_models::config)
├── polymarket       ← 平台端点 + chain_id + onchain RPC + fees 费率表
├── market_data      ← 仅连接层：websocket（重连/分片）+ gamma（catalog 同步）
├── observability    ← 仅 log_level + log_json（接线 tracing init，RUST_LOG 可覆盖）
├── db               ← postgres + clickhouse（连接、池、批量写入）
├── cache            ← redis + moka(max_capacity) + 策略
├── keys             ← 凭证来源（env / keystore）
├── web              ← HTTP/WS 服务 + JWT
├── execution        ← 仅 book_apply 结构参数（shard_count / channel_capacity）
└── settlement       ← 仅 lifecycle.channel_capacity
```

### 5.2 RuntimeConfig（`runtime_config_version.config_json`，schema_version = 1，热更新）

所有运营参数走版本化治理 API（create → preflight → activate → 全订阅者传播），
存储于 Postgres，进程内 `RuntimeConfigStore`（ArcSwap）无锁读。传播是 push
模型：`RuntimeConfigApplicator` 持有 `RuntimeConfigSubscribers`（每个订阅者是
一个类型化字段，编译期完备），按固定顺序传播——先 stage 可失败的 reload
（oracle 源重建、redeem 地址解析，任一失败则零变更中止），再 commit 并依次
执行不可失败的 `reload(...)`（risk → exposure → detection → execution →
settlement → notification），store 最后 swap：

```
RuntimeConfig (oxide_arb_models::runtime_config)
├── market_data      ← staleness 四档阶梯
├── detection        ← min_profit + endgame（thresholds/scorer/fill/cooldown/tracker）+ calibration
├── execution        ← timeout / funnel / coalescer / endgame_latency
├── risk             ← 全量（限额、熔断、sizing/kelly/drawdown、黑名单、对账）
├── settlement       ← oracle 策略 / lifecycle 运营 / redeem 路由
└── notification     ← alert_cooldown + telegram + webhook（全量，读取脱敏）
```

### 5.3 关键裁决

| 裁决 | 原因 |
|---|---|
| 删除 `[treasury]` | 从未实现的第二套资金语义；能力已被 `risk.bankroll/min_balance/reserve` + exposure reservation 覆盖 |
| 删除 `detection.endgame.enabled` | Endgame-only；停交易走 execution_mode / 熔断 |
| `[analytics]` 并入 `db.clickhouse` | ClickHouse 是数据库，不是独立"分析"概念；lag probe（未接线）一并删除 |
| `settlement.contracts` → `constants` | 链上部署地址是事实不是参数；仅保留 5 个真实消费地址 |
| 删除 `observability.metrics/alerts` | metrics 永远开启走 web `/metrics`；告警冷却归入 `notification.alert_cooldown_secs` |
| 不做 schema 迁移链 | 项目从未正式运行；仅 `schema_version = 1`，非 1 文档拒绝 activate |

---

## 6. Crate 拓扑（最终目标）

```
workspace
├── oxide-arb-error          ← Phase 0 ✓ 已完成
├── oxide-arb-macros         ← Phase 0 ✓ 已完成
├── oxide-arb-models         ← Phase 0 ✓ 已完成（需精简）
├── oxide-arb-api            ← Phase 1（Polymarket CLOB + Gamma + Oracle + Keystore）
├── oxide-arb-storage        ← Phase 2（PG + CH + Cache）
├── oxide-arb-repository     ← Phase 2（Repository Pattern）
├── oxide-arb-algorithm      ← Phase 3（Endgame 检测 + Calibration + Scoring）
├── oxide-arb-risk           ← Phase 4（CircuitBreaker + Kelly + Drawdown + Blacklist）
├── oxide-arb-core           ← Phase 4（App lifecycle + DataPipeline + Execution）
├── oxide-arb-control        ← Phase 5（Control factor materialization + governance）
├── oxide-arb-web            ← Phase 6（actix-web REST + WS）
└── oxide-arb (bin)          ← 二进制入口（CLI subcommands: serve, migrate, replay）
```

**UI 作为独立仓库** `oxide-arb-ui`（git submodule）:
- Fork from vben-admin
- 两个分支：`main`（我们的），`upstream`（追踪 vben）
- 构建产物通过 `static/ui/` 嵌入 Rust 二进制

---

## 7. 被显式杀死的多平台/多策略设计模式

### 7.1 不允许的抽象

| 模式 | 为什么杀死 |
|---|---|
| `trait VenueApi` / `trait Exchange` | 只有一个平台，trait 是多余间接层 |
| `trait Strategy` / `StrategyFactory` | 只有一个策略，静态 dispatch 即可 |
| `enum VenueId { Polymarket, Kalshi, ... }` | 只有 Polymarket，enum 是空洞的 |
| `enum StrategyKind { Endgame, CrossBook, Stat }` | 只有 Endgame |
| `Box<dyn Strategy>` / `Arc<dyn Strategy>` | 没有运行时多态需求 |
| `venue_adapter` 模块 | 只有一个 venue，不需要适配器 |
| `strategy_router` 模块 | 只有一个策略，不需要路由 |

### 7.2 允许的抽象（因为它们服务于可测试性，不是多平台）

| 模式 | 为什么保留 |
|---|---|
| `trait MarketRepository` | 接口隔离，支持 mock 测试 |
| `trait CacheBackend` | 支持 Moka vs Redis vs Mock 切换 |
| `trait OracleSource` | 支持 Gamma vs CTF vs Mock 源（但都是 Polymarket 生态内的） |
| `trait RiskMetrics` | DI 注入，解耦 risk 和 core crate |
| `trait RiskPersistence` | DI 注入 |

**判断标准**：如果一个 trait 的存在是为了支持**测试中的 mock 注入**或**同一平台内不同数据源的组合**，则保留。如果它的存在是为了"未来可能接入其他平台/策略"，则删除。

---

## 8. 后续执行计划

### 立即执行（Phase 0 — 已完成）

1. ✅ 删除 `constants.rs` 中的 trading thresholds
2. ✅ `venue` → `polymarket` 配置段
3. ✅ `detection.min_profit_threshold_usd` 为唯一 min_profit 来源（已删除 `[execution]` / `[risk]` 重复字段）
4. ✅ `sizing.bankroll_usd`

### 延后到 Phase 4（执行层）

以下字段在 ADR §5.2 中描述，但 **不在 Phase 0/1 实现**；由 `oxide-arb-core` 的 `ExecutionConfig` 扩展与 `TieredExecutionStrategy` 一并交付（见 `phase4-core-and-risk.md` §0.1）：

- `fok_timeout_ms`
- `gtd_expiry_secs`
- `max_retries_per_tier`
- `price_tolerance_ticks`

### Phase 0 补全阶段

1. 删除 `types/venue.rs`（VenueId 枚举）
2. 简化 `enums/common.rs`：删除 `OpportunityType`、`TradeOutcome::HedgeLoss`/`Unhedged`
3. 将 `Opportunity<M>` 泛型改为具体 struct

### 后续 Phase

- Phase 1-8 计划中涉及多策略/多平台的所有设计点均需对照本 ADR 清理
- 任何新代码 review 中出现 "多平台"、"多策略"、"strategy 参数化" 概念一律打回

---

## 9. 不可妥协的原则

1. **不存在"未来可能"**：如果当下不需要，就不写。当真正需要时再添加
2. **不存在"向前兼容"**：我们不维护一个可以插入新 venue 的框架
3. **命名即语义**：用 `polymarket` 而不是 `venue`，用 `endgame` 而不是 `strategy`
4. **配置即运行时**：所有交易参数通过 config 管理，不硬编码为 const
5. **const 只放协议事实**：合约地址、chain ID、token decimals — 永远不变的东西
