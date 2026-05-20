# Plan Documents Review: ADR-001 对齐审查

> **审查日期**: 2025-05-20
>
> **审查依据**: ADR-001（单策略 Endgame / 单平台 Polymarket）
>
> **审查范围**: Phase 0–8 全部计划文档
>
> **结论**: Phase 0–1 需要小幅更新（实现已领先于计划文档）；Phase 2–4 需要中幅修改；Phase 5–8 基本无需变更

---

## 审查符号说明

- ❌ **必须修改** — 与 ADR-001 直接冲突
- ⚠️ **建议修改** — 措辞过时或有误导性
- ✅ **无需修改** — 已符合单策略/单平台设计

---

## Phase 0 — 工程基座

**总体评价**: 实现已经超越计划文档。代码中已经删除了 VenueId、简化了 TradeOutcome、
Opportunity 不再泛型。**计划文档需要同步更新以反映实际实现。**

### 需要修改的点

| 位置 | 问题 | 修改方案 |
|---|---|---|
| §4.2 目录结构 `types/venue.rs` | ❌ 计划列出了 `types/venue.rs — VenueId enum (仅 Polymarket)` | 删除此文件条目。实际实现中不存在此文件，也不应该存在 |
| §4.2 目录结构 `config/venues.rs` | ❌ 计划列出了 `venues.rs — VenuesConfig, PolymarketVenueConfig, OnchainConfig` | 改为 `polymarket.rs — PolymarketConfig, OnchainConfig`（已实现） |
| §4.4.3 `Opportunity` struct | ⚠️ 计划中的 struct 定义缺少一些实际已有的字段 | 同步为实际实现的版本 |
| §4.5 枚举设计 `OpportunityType` | ❌ 计划定义了 `enum OpportunityType { DirectionalBet }` | 删除。只有一种策略类型，不需要枚举标记 |
| §4.5 枚举设计 `TradeOutcome` | ❌ 计划包含 `HedgeLoss`, `Unhedged` 变体 | 删除这两个变体。实际实现已移除（Endgame 不对冲） |
| §4.6 常量 | ❌ 计划包含 `MIN_PROFIT_THRESHOLD`, `MIN_EDGE_BPS`, `MAX_DEPTH_USAGE_PCT`, `MIN_DEPTH_USD`, `KELLY_FRACTION` | 删除这些。已通过 config 管理，constants.rs 只保留协议级不可变事实 |
| §4.4.4 配置聚合 `Settings` | ❌ 有 `pub venues: VenuesConfig` | 改为 `pub polymarket: PolymarketConfig`（已实现） |

### 无需修改的点

- ✅ 错误类型 `OxideError` — 已是最终版本
- ✅ 宏 crate `TypedId`, `IntoActiveValue` — 正确
- ✅ 货币类型 `Usd`, `Price`, `Shares`, `Bps` — 正确
- ✅ ID 类型 — 正确
- ✅ Cargo.toml 配置 — 正确
- ✅ profile 策略 — 正确

---

## Phase 1 — 数据接入层

**总体评价**: 该 Phase 已经是 Polymarket-only 设计，大部分内容无需变更。有几个 trait
抽象需要根据 ADR-001 原则评估。

### 需要修改的点

| 位置 | 问题 | 修改方案 |
|---|---|---|
| §5.2 `FeeRateSource` | ⚠️ 使用了 `HashMap<MarketCategory, CategoryFeeParams>` — 这是对的，但外层 `FeeService` 如果是 trait 需要改为 struct | 如果存在 `trait FeeService`，改为 `struct PolymarketFeeService`。实际只有一个实现，不需要 trait |
| §7.1 `KeyLoader` trait | ❌ 定义了 `trait KeyLoader` + 多个实现 (`HexKeyLoader`, `EnvKeyLoader`) | 改为单一 struct。生产环境只从环境变量加载 hex 私钥，不需要 trait 抽象。可以保留一个 `KeyLoader` struct 内部判断加载方式 |
| §3.1 `ClobWsManager` 中的 `WsEvent::MarketResolved` | ⚠️ 接口定义中有 `winning_token_id` 但没有 `strategy` 字段 | ✅ 已正确（无 strategy 概念） |

### 无需修改的点

- ✅ `OracleSource` trait — 保留。虽然只服务 Polymarket 生态，但 Gamma vs CTF vs Mock 三种源需要统一接口
- ✅ `VotingOracle` 2-of-3 quorum — 保留。好的鲁棒性设计
- ✅ CLOB WebSocket 管理器 — 正确
- ✅ Gamma API Client — 正确
- ✅ Fee Calculator 公式 — 正确（Polymarket 专有）
- ✅ Order Signing (EIP-712) — 正确
- ✅ L2 HMAC credentials — 正确
- ✅ 错误类型 `ApiError` — 正确

---

## Phase 2 — 持久化体系

**总体评价**: DB Schema 中有多处 `strategy` 列需要删除。其余设计（PG + CH + Cache）
属于基础设施，与策略/平台无关，无需变更。

### 需要修改的点

| 位置 | 问题 | 修改方案 |
|---|---|---|
| §3.3 `trades` 表 `strategy TEXT NOT NULL` | ❌ endgame-only 不需要 strategy 列 | 删除此列。所有 trade 隐含为 endgame |
| §3.3 `trades` 表 `CREATE INDEX idx_trades_strategy_date` | ❌ 对应索引 | 删除此索引 |
| §3.4 `positions` 表 `strategy TEXT NOT NULL` | ❌ | 删除此列 |
| §3.4 `positions` 表 `CREATE INDEX idx_positions_open_strategy` | ❌ | 删除此索引 |
| §4.3 `opportunity_audit` CH 表 `strategy String` | ❌ | 删除此列 |
| §4.3 `opportunity_audit` 表 `ORDER BY (strategy, detected_at, ...)` | ❌ | 改为 `ORDER BY (detected_at, opportunity_id)` |
| §9.2 `TradeRepository::find_recent()` 参数 `strategy: Option<&str>` | ❌ | 删除此参数 |

### 无需修改的点

- ✅ PostgreSQL 连接管理 — 正确
- ✅ ClickHouse 表设计（除 strategy 列外） — 正确
- ✅ Redis + Moka 多级缓存架构 — 正确
- ✅ `TieredCache` 设计 — 正确
- ✅ `CacheKey` 枚举 — 正确
- ✅ `BatchInserter` — 正确
- ✅ Migration 策略 — 正确
- ✅ Repository trait 设计（用于 DI/mock） — 正确

---

## Phase 3 — 套利算法

**总体评价**: 这是最核心的 Phase，设计已经是 endgame-only。但有几处使用了泛型
`Opportunity<EndgameMeta>` 语法，需要与实际实现（具体 struct `Opportunity`）对齐。

### 需要修改的点

| 位置 | 问题 | 修改方案 |
|---|---|---|
| §2.1 `EndgameStrategy::detect()` 返回值 | ❌ `Option<Opportunity<EndgameMeta>>` | 改为 `Option<Opportunity>`。实际实现中 `Opportunity` 已经是具体 struct，`EndgameMeta` 是内嵌字段 |
| §8 Scorer `score(&self, opp: &Opportunity<EndgameMeta>)` | ❌ 泛型语法 | 改为 `&Opportunity` |
| §9 Pipeline `Opportunity<EndgameMeta>` 所有出现 | ❌ | 全部改为 `Opportunity` |
| §2.1 注释 "detect() → Vec<Opportunity<EndgameMeta>>" | ❌ | 改为 `detect() → Option<Opportunity>` |
| §10 配置中 `[detection.endgame.scorer] min_score` | ⚠️ 配置路径 OK，但实际对应的 Rust struct 名称需要确认 | 确认与 `DetectionConfig` 层级一致 |

### 无需修改的点

- ✅ `EndgameStrategy` 检测逻辑 — 正确（收敛方向检测、orderbook walk、E[PnL]）
- ✅ `ConvergenceTracker` — 正确
- ✅ Calibration 系统 (BucketKey, PriceZone, DurationBucket) — 正确
- ✅ `ResolutionCalibrator` 4-tier fallback — 正确
- ✅ `CalibrationUpdater` — 正确
- ✅ MoM Prior Estimation — 正确
- ✅ `ConfidenceFusion` — 正确
- ✅ `FillProbabilityEstimator` — 正确
- ✅ `OpportunityPipeline` 整体设计 — 正确
- ✅ Property-based 测试计划 — 正确

---

## Phase 4 — 系统内核与风控

**总体评价**: 这是影响最大的 Phase。执行状态机中的 Hedging 状态、多策略路由概念、
泛型 `Opportunity<AnyMeta>` 都需要删除。

### 需要修改的点

| 位置 | 问题 | 修改方案 |
|---|---|---|
| §9 `ExecState::Hedging` | ❌ Endgame 不对冲 | 删除此状态。FSM 简化为 `Idle → Validate → Exec → Idle` |
| §9 `ExecState::Emergency` | ❌ "对冲失败"触发 Emergency | 重定义：Emergency 仅指系统级故障（非 hedge 相关），如 API 完全不可达 |
| §9 FSM 图中 `EXEC → HEDGING → EMERGENCY` 路径 | ❌ | 删除整条路径。EXEC 失败直接回到 IDLE（上报 risk engine） |
| §4 `RiskEngine::pre_trade_check()` 参数 `Opportunity<AnyMeta>` | ❌ 泛型 | 改为 `&Opportunity` |
| §6 `MultiConstraintSizer::compute_size()` 参数 `Opportunity<AnyMeta>` | ❌ | 改为 `&Opportunity` |
| §6 `quarter_kelly()` 中 `opp.legs.first()` | ❌ Opportunity 没有 `legs` 字段 | 改为 `opp.entry_price`（实际 struct 直接有 `entry_price: Price`） |
| §4 `RiskEngine` 中 `on_trade_result` 的 hedging 相关逻辑 | ⚠️ | 移除 hedge-related 分支 |
| §2 `oxide-arb-core` 目录中 `execution/state_machine.rs` 的 FSM | ❌ | 简化状态图 |
| §10 `TieredExecutionStrategy` | ⚠️ 整体 OK（FOK → GTD 是正确的），但注释中提到 "multi-leg" | 删除任何 multi-leg 引用 |

### 无需修改的点

- ✅ `CircuitBreaker` 4 级状态机 — 正确（与 hedge 无关，是通用风控）
- ✅ `DailyAccounting`, `WeeklyAccounting` — 正确
- ✅ `PositionTracker`, `PotentialLossLedger` — 正确
- ✅ `BlacklistManager` — 正确
- ✅ `DrawdownGuard` — 正确
- ✅ `RiskMetrics` trait — 正确（DI 边界）
- ✅ `RiskPersistence` trait — 正确（DI 边界）
- ✅ `DataPipeline` (WS → BookStore) — 正确
- ✅ `BookStore` — 正确
- ✅ `MarketRegistry` — 正确
- ✅ `Coalescer` — 正确
- ✅ `AppContext` DI 容器 — 正确
- ✅ `MetricsHub` — 正确
- ✅ `AlertDispatcher` — 正确
- ✅ `CapitalManager` — 正确
- ✅ Quarter-Kelly 公式本身 — 正确
- ✅ FOK + GTD 分层执行 — 正确

---

## Phase 5 — 回放与分析引擎

**总体评价**: 已经是 endgame-only 设计。无重大问题。

### 需要修改的点

| 位置 | 问题 | 修改方案 |
|---|---|---|
| §7.1 `PaperTradeConfig` 中 `min_edge_bps` 默认值 `dec!(200)` | ⚠️ 与 constants.rs 中已删除的 `MIN_EDGE_BPS` 重复定义 | 无需修改（这是 replay 独立配置，合理存在），但文档应注明此值来源于 detection config 的默认值 |
| 无其他问题 | | |

### 无需修改的点

- ✅ `ReplayEngine` — 正确
- ✅ `BookReplayer` — 正确
- ✅ `PaperTradeSimulator` — 正确
- ✅ `FillModel` trait — 正确（服务于回测模拟，不是多平台抽象）
- ✅ `ReplayReport` — 正确
- ✅ ClickHouse 查询模板 — 正确
- ✅ CLI 入口 — 正确

---

## Phase 6 — Web 服务层

**总体评价**: 已经是单系统 API。无多策略路由。极少量措辞需要清理。

### 需要修改的点

| 位置 | 问题 | 修改方案 |
|---|---|---|
| §3.5 `GET /api/v1/trades` 查询参数 | ⚠️ 参数列表中没有 `strategy` filter（正确），但应明确文档化为什么没有 | 添加注释："不需要 strategy 参数 — 所有交易均为 endgame" |
| §4.3 `WsEvent` 中无 strategy 标记 | ✅ | 已正确 |

### 无需修改的点

- ✅ 全部 REST 端点设计 — 正确
- ✅ WebSocket 协议 — 正确
- ✅ 认证机制 — 正确
- ✅ 运行时配置热更新 — 正确
- ✅ 静态文件服务 — 正确
- ✅ CORS 策略 — 正确
- ✅ 错误 Envelope — 正确

---

## Phase 7 — UI 层

**总体评价**: UI 设计本身与 ADR-001 无冲突。但 Fork 策略章节需要更新为 submodule 方案。

### 需要修改的点

| 位置 | 问题 | 修改方案 |
|---|---|---|
| §1.1 Fork 策略中的 git 命令 | ❌ 描述的是直接 `git clone` + `rename origin` 流程 | 重写为 git submodule 方案：`oxide-arb-ui` 作为 `oxide-arb` 的 submodule，有 `main` 和 `upstream` 两个分支 |
| §6.2 "集成到 Rust 二进制" 中的路径 | ⚠️ `cp -r oxide-arb-ui/dist/ oxide-arb/static/ui/` | 因为是 submodule，路径应为 `cp -r oxide-arb-ui/dist/ static/ui/`（相对于 workspace root） |

### 无需修改的点

- ✅ 7 个 Dashboard 页面设计 — 正确
- ✅ WebSocket 集成 — 正确
- ✅ 自定义组件清单 — 正确
- ✅ 响应式策略 — 正确
- ✅ 部署流程（除路径外） — 正确
- ✅ Vite proxy 配置 — 正确

---

## Phase 8 — 运维与部署

**总体评价**: 基础设施层，与策略/平台无关。**无需任何变更。**

### 无需修改的点

- ✅ Docker Compose (dev + prod) — 正确
- ✅ Dockerfile 多阶段构建 — 正确
- ✅ GitHub Actions CI — 正确
- ✅ Prometheus + Grafana — 正确
- ✅ 备份策略 — 正确
- ✅ 部署脚本 — 正确
- ✅ 安全加固 — 正确
- ✅ 灾难恢复 Playbook — 正确

---

## 优先级排序

### P0（立即修改 — 与代码实现不一致）

1. **Phase 0** `§4.2` 目录结构：删除 `types/venue.rs`、改 `config/venues.rs` → `config/polymarket.rs`
2. **Phase 0** `§4.5` 枚举：删除 `OpportunityType`、删除 `TradeOutcome::HedgeLoss/Unhedged`
3. **Phase 0** `§4.6` 常量：删除 trading thresholds
4. **Phase 0** `§4.4.4` Settings：`venues` → `polymarket`

### P1（近期修改 — Phase 实现前必须对齐）

5. **Phase 2** DB Schema：删除所有 `strategy` 列和相关索引
6. **Phase 3** 泛型：`Opportunity<EndgameMeta>` → `Opportunity`
7. **Phase 4** FSM：删除 `Hedging`/`Emergency` 状态、简化状态图
8. **Phase 4** 泛型：`Opportunity<AnyMeta>` → `Opportunity`
9. **Phase 4** Kelly：`opp.legs.first()` → `opp.entry_price`
10. **Phase 1** Trait：`KeyLoader` trait → struct

### P2（低优先级 — 措辞优化）

11. **Phase 7** Fork 策略 → submodule 方案
12. **Phase 6** 添加注释说明无 strategy 参数
13. **Phase 5** 注明 `min_edge_bps` 来源

---

## 是否需要全面重写？

**结论：不需要全面重写。** 计划文档的核心架构（分层设计、职责划分、数据流）是正确的。
需要的是**精准修订**，而非推倒重来。上述 13 个修改点中，90% 是删除/简化操作。

唯一需要"重写"的段落是 Phase 4 的 ExecutionFSM 部分（状态图需要重新画），
以及 Phase 7 的 Fork 策略章节（改为 submodule 说明）。

---

## 代码 vs 计划的 Gap 分析

实际代码（已实现的 Phase 0）**已经领先于计划文档**：

| 维度 | 计划文档（旧） | 实际代码（新） |
|---|---|---|
| Venue | `types/venue.rs` VenueId enum | 不存在，已删除 |
| Config | `config/venues.rs` VenuesConfig | `config/polymarket.rs` PolymarketConfig |
| Constants | 包含 5 个 trading thresholds | 只有合约地址 + chain ID + USDC params |
| Opportunity | `Opportunity<M>` 泛型 | `Opportunity` 具体 struct，内嵌 `EndgameMeta` |
| TradeOutcome | 5 变体含 HedgeLoss/Unhedged | 5 变体: Success/Miss/Stale/TradeFailed/SystemError |
| OpportunityType | 存在 | 不存在（已删除） |
| Settings.venue | 存在 | 替换为 Settings.polymarket |
| PositionSizingConfig | 无 bankroll_usd | 有 bankroll_usd |

**行动项**：更新 Phase 0 计划文档使其与实际代码一致，确保后续 Phase 的实现者参考正确的基础类型。
