# Phase 6 — Web 服务层

> **产出**: `oxide-arb-web` crate（actix-web 重写）
>
> **前置条件**: Phase 0–4 核心系统可运行；Phase 5 replay 可选
>
> **验收标准**: 所有 REST 端点返回正确 JSON envelope；WebSocket 推送实时机会/交易/系统状态；Bearer token 认证拒绝未授权请求；运行时配置可通过 PATCH 热更新并记录审计日志；生产模式可服务静态 Vue 文件

---

## 0. 工作范围

1. RESTful API — 系统控制、数据查询、配置管理
2. WebSocket — 实时推送 opportunities / trades / PnL / system status
3. 静态文件服务 — 生产模式下直接 serve Vue 构建产物
4. API Key 认证 — 单用户 Bearer token
5. 运行时配置热更新 — PATCH 端点 + 审计日志
6. CORS — 开发模式下 allow_any_origin

---

## 1. 目录结构

```
crates/oxide-arb-web/
├── Cargo.toml
└── src/
    ├── lib.rs                  # spawn_web_server() 入口
    ├── state.rs                # AppState (AppContext wrapper)
    ├── response.rs             # WebResponse<T> envelope + WebError
    ├── extractors.rs           # ValidatedJson, Pagination, QueryFilter
    ├── middleware/
    │   ├── mod.rs
    │   ├── auth.rs             # ApiKeyAuth Bearer token middleware
    │   └── request_id.rs       # Request-ID tracing header
    ├── routes/
    │   ├── mod.rs              # configure() — 注册全部路由组
    │   ├── health.rs           # GET /health, GET /ready
    │   ├── metrics.rs          # GET /metrics (Prometheus)
    │   ├── system.rs           # 系统控制端点
    │   ├── markets.rs          # 市场数据端点
    │   ├── opportunities.rs    # 机会数据端点
    │   ├── trades.rs           # 交易数据端点
    │   ├── risk.rs             # 风控端点
    │   ├── config.rs           # 运行时配置端点
    │   ├── pnl.rs              # PnL / analytics 端点
    │   └── replay.rs           # Replay 触发端点
    ├── ws/
    │   ├── mod.rs              # WebSocket server 入口
    │   ├── handler.rs          # WS connection handler (upgrade + message loop)
    │   ├── session.rs          # WsSession — per-connection state
    │   ├── broadcaster.rs      # WsBroadcaster — fanout core events to all clients
    │   └── protocol.rs         # WsMessage enum (JSON wire format)
    └── static_files.rs         # 静态文件服务 (production Vue assets)
```

---

## 2. Cargo.toml

```toml
[package]
name = "oxide-arb-web"
description = "HTTP API + WebSocket server for the oxide-arb trading system"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[dependencies]
oxide-arb-error = { workspace = true }
oxide-arb-models = { workspace = true }
oxide-arb-core = { workspace = true }
oxide-arb-repository = { workspace = true }
oxide-arb-replay = { workspace = true }

# HTTP framework
actix-web = { workspace = true }
actix-cors = { workspace = true }
actix-web-actors = "4"
actix-files = "0.6"
tracing-actix-web = { workspace = true }

# Serialization
serde = { workspace = true }
serde_json = { workspace = true }

# Validation
validator = { workspace = true }

# Data types
rust_decimal = { workspace = true }
chrono = { workspace = true }
uuid = { workspace = true }

# ORM (for query building in handlers)
sea-orm = { workspace = true }

# Async
tokio = { workspace = true }
tokio-util = { workspace = true }
flume = { workspace = true }

# Logging
tracing = { workspace = true }

# Error
thiserror = { workspace = true }

[dev-dependencies]
actix-rt = { workspace = true }
tokio = { workspace = true, features = ["test-util"] }
sea-orm = { workspace = true }
sea-orm-migration = { workspace = true }
oxide-arb-storage = { workspace = true, features = ["test-util"] }
testcontainers = { workspace = true }
testcontainers-modules = { workspace = true }
reqwest = { workspace = true }

[lints]
workspace = true
```

---

## 3. 完整路由表

### 3.1 公开端点（无需认证）

| 方法 | 路径 | 说明 | 请求体 | 响应 |
|---|---|---|---|---|
| GET | `/health` | 健康检查（存活探针） | — | `{ "status": "ok" }` |
| GET | `/ready` | 就绪检查（DB + CH + Redis） | — | `{ "ready": true, "checks": {...} }` |
| GET | `/metrics` | Prometheus exposition format | — | text/plain |

### 3.2 系统控制（`/api/v1/system`）

| 方法 | 路径 | 说明 | 请求体 | 响应 |
|---|---|---|---|---|
| GET | `/api/v1/system/status` | 系统状态概览 | — | `SystemStatusResponse` |
| POST | `/api/v1/system/halt` | 紧急停止交易 | `{ "reason": "..." }` | `{ "halted": true }` |
| POST | `/api/v1/system/resume` | 恢复交易 | — | `{ "resumed": true }` |
| POST | `/api/v1/system/mode` | 切换执行模式 | `{ "mode": "Live\|Paper\|DryRun" }` | `{ "mode": "Live" }` |
| GET | `/api/v1/system/health` | 详细健康报告 | — | `HealthReport` |

### 3.3 市场数据（`/api/v1/markets`）

| 方法 | 路径 | 说明 | 请求体 | 响应 |
|---|---|---|---|---|
| GET | `/api/v1/markets` | 已监控市场列表 | `?status=active&page=1&size=50` | `Paginated<MarketSummary>` |
| GET | `/api/v1/markets/{id}` | 市场详情 + 实时 orderbook | — | `MarketDetail` |
| POST | `/api/v1/markets/{id}/subscribe` | 订阅市场数据流 | — | `{ "subscribed": true }` |
| POST | `/api/v1/markets/{id}/unsubscribe` | 取消订阅 | — | `{ "unsubscribed": true }` |
| GET | `/api/v1/markets/{id}/book` | 当前 L2 orderbook 快照 | `?depth=10` | `OrderbookSnapshot` |

### 3.4 机会数据（`/api/v1/opportunities`）

| 方法 | 路径 | 说明 | 请求体 | 响应 |
|---|---|---|---|---|
| GET | `/api/v1/opportunities/recent` | 最近 N 个检测到的机会 | `?limit=50` | `Vec<OpportunitySummary>` |
| GET | `/api/v1/opportunities/history` | 历史机会查询 | `?from=...&to=...&market_id=...` | `Paginated<OpportunitySummary>` |
| GET | `/api/v1/opportunities/{id}` | 机会详情 | — | `OpportunityDetail` |
| GET | `/api/v1/opportunities/stats` | 机会统计 | `?period=24h\|7d\|30d` | `OpportunityStats` |

### 3.5 交易数据（`/api/v1/trades`）

| 方法 | 路径 | 说明 | 请求体 | 响应 |
|---|---|---|---|---|
| GET | `/api/v1/trades` | 交易列表 | `?from=...&to=...&outcome=...&page=1&size=50` | `Paginated<TradeSummary>` |
| GET | `/api/v1/trades/{id}` | 交易详情（含决策链） | — | `TradeDetail` |
| GET | `/api/v1/trades/{id}/decisions` | 单笔交易决策链 | — | `Vec<DecisionStep>` |
| GET | `/api/v1/trades/pnl` | PnL 汇总 | `?period=daily\|weekly\|monthly` | `PnlSummary` |
| GET | `/api/v1/trades/pnl/daily` | 每日 PnL 时序 | `?from=...&to=...` | `Vec<DailyPnl>` |
| GET | `/api/v1/trades/pnl/attribution` | PnL 归因 | `?group_by=market\|category` | `Vec<PnlAttribution>` |

### 3.6 风控端点（`/api/v1/risk`）

| 方法 | 路径 | 说明 | 请求体 | 响应 |
|---|---|---|---|---|
| GET | `/api/v1/risk/circuit-breaker` | 熔断器状态 | — | `CircuitBreakerStatus` |
| POST | `/api/v1/risk/circuit-breaker/reset` | 重置熔断器 | `{ "level": 1 }` | `{ "reset": true }` |
| GET | `/api/v1/risk/blacklist` | 黑名单列表 | — | `Vec<BlacklistEntry>` |
| POST | `/api/v1/risk/blacklist` | 添加黑名单条目 | `{ "scope": "Market", "target": "...", "reason": "..." }` | `BlacklistEntry` |
| DELETE | `/api/v1/risk/blacklist/{id}` | 移除黑名单条目 | — | `{ "removed": true }` |
| GET | `/api/v1/risk/positions` | 持仓概览 | — | `Vec<PositionSummary>` |
| GET | `/api/v1/risk/exposure` | 总风险敞口 | — | `ExposureReport` |
| GET | `/api/v1/risk/daily-loss` | 当日亏损统计 | — | `DailyLossGauge` |

### 3.7 配置端点（`/api/v1/config`）

| 方法 | 路径 | 说明 | 请求体 | 响应 |
|---|---|---|---|---|
| GET | `/api/v1/config` | 当前全量运行时配置 | — | `RuntimeConfig` |
| PATCH | `/api/v1/config` | 部分更新运行时配置 | `{ "path.to.field": value }` | `RuntimeConfig` |
| GET | `/api/v1/config/audit` | 配置变更审计日志 | `?limit=50` | `Vec<ConfigAuditEntry>` |
| GET | `/api/v1/config/calibration` | 当前校准参数 | — | `CalibrationSnapshot` |
| PATCH | `/api/v1/config/calibration` | 更新校准参数 | `{ ... }` | `CalibrationSnapshot` |

### 3.8 分析端点（`/api/v1/analytics`）

| 方法 | 路径 | 说明 | 请求体 | 响应 |
|---|---|---|---|---|
| GET | `/api/v1/analytics/daily` | 每日报告 | `?date=2025-01-15` | `DailyReport` |
| GET | `/api/v1/analytics/weekly` | 每周报告 | `?week=2025-W03` | `WeeklyReport` |
| GET | `/api/v1/analytics/edge-distribution` | Edge 分布图数据 | `?period=7d` | `EdgeDistribution` |
| GET | `/api/v1/analytics/market-performance` | 市场表现排名 | `?sort=pnl&limit=20` | `Vec<MarketPerformance>` |

### 3.9 Replay 端点（`/api/v1/replay`）

| 方法 | 路径 | 说明 | 请求体 | 响应 |
|---|---|---|---|---|
| POST | `/api/v1/replay` | 提交 replay 任务 | `ReplayConfig` | `{ "task_id": "..." }` |
| GET | `/api/v1/replay/{task_id}` | 查询 replay 状态/报告 | — | `ReplayTaskStatus` |
| GET | `/api/v1/replay/history` | 历史 replay 列表 | `?limit=20` | `Vec<ReplayTaskSummary>` |

---

## 4. WebSocket 协议设计

### 4.1 连接端点

```
GET /api/v1/ws?token={api_key}
```

升级为 WebSocket 连接后，服务端推送实时事件，客户端可发送订阅/取消指令。

### 4.2 消息格式（JSON）

所有 WebSocket 消息使用统一 JSON envelope：

```json
{
  "type": "event_type",
  "timestamp": "2025-01-15T10:30:00.000Z",
  "data": { ... }
}
```

### 4.3 服务端推送消息类型

| type | 触发条件 | data 内容 |
|---|---|---|
| `opportunity.detected` | 新机会检测到 | `OpportunitySummary` |
| `opportunity.expired` | 机会过期/消失 | `{ "id": "..." }` |
| `trade.opened` | 新交易开始执行 | `TradeSummary` |
| `trade.filled` | 交易完全成交 | `TradeDetail` |
| `trade.settled` | 交易结算 | `{ "id": "...", "outcome": "...", "pnl": "..." }` |
| `pnl.update` | PnL 更新（每 5s） | `{ "daily_pnl": "...", "total_pnl": "..." }` |
| `system.status` | 系统状态变化 | `SystemStatusSnapshot` |
| `system.alert` | 系统告警 | `{ "level": "warn\|error", "message": "..." }` |
| `risk.circuit_breaker` | 熔断器状态变化 | `CircuitBreakerStatus` |
| `risk.position_update` | 持仓变动 | `PositionSummary` |
| `market.book_update` | orderbook 更新（已订阅市场） | `{ "market_id": "...", "best_bid": ..., "best_ask": ... }` |
| `market.resolved` | 市场结算 | `{ "market_id": "...", "outcome": "..." }` |
| `config.changed` | 运行时配置变更 | `{ "path": "...", "old": ..., "new": ... }` |

### 4.4 客户端指令

```json
// 订阅指定市场的 book 更新
{ "action": "subscribe", "channel": "market.book", "market_id": "0x..." }

// 取消订阅
{ "action": "unsubscribe", "channel": "market.book", "market_id": "0x..." }

// Ping (心跳)
{ "action": "ping" }
```

### 4.5 WsBroadcaster 架构

```rust
/// Central fan-out broadcaster for all real-time events.
///
/// Core subsystems emit events to a flume channel. The broadcaster receives
/// these events and fans out to all connected WsSession instances.
pub struct WsBroadcaster {
    /// Receive end of the core event channel.
    event_rx: flume::Receiver<CoreEvent>,
    /// Active WebSocket sessions.
    sessions: DashMap<Uuid, WsSessionHandle>,
}

/// Per-connection state maintained by the broadcaster.
pub struct WsSessionHandle {
    /// Sender to push messages to this client's WebSocket.
    tx: flume::Sender<WsMessage>,
    /// Channels this client has subscribed to.
    subscriptions: HashSet<String>,
    /// Connected at timestamp.
    connected_at: DateTime<Utc>,
}

impl WsBroadcaster {
    /// Main broadcast loop — runs in a dedicated tokio task.
    pub async fn run(self, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                Ok(event) = self.event_rx.recv_async() => {
                    self.fanout(&event).await;
                }
                () = shutdown.cancelled() => break,
            }
        }
    }

    /// Send an event to all sessions whose subscriptions match.
    async fn fanout(&self, event: &CoreEvent) {
        let msg = WsMessage::from(event);
        let channel = msg.channel();

        for session in self.sessions.iter() {
            if session.subscriptions.contains(channel) || msg.is_broadcast() {
                let _ = session.tx.try_send(msg.clone());
            }
        }
    }
}
```

### 4.6 重连与状态同步

客户端断线重连后的处理策略：

1. 客户端连接后立即推送一个 `system.status` 快照
2. 客户端可发送 `{ "action": "sync" }` 请求全量状态同步
3. 服务端响应包含：当前持仓、熔断器状态、最近 10 个 opportunities、当日 PnL
4. 心跳：服务端每 15s 发送 ping，客户端需 30s 内 pong，否则断开

---

## 5. 认证中间件

```rust
/// API key authentication middleware.
///
/// Extracts bearer token from `Authorization` header.
/// Health and metrics endpoints are excluded at the route level.
pub struct ApiKeyAuth;

/// Shared API key for single-user auth.
pub struct ApiKey(pub Arc<str>);
```

认证流程：

1. `Authorization: Bearer <token>` header 提取
2. 与 `Settings.keys.api_key` 比对
3. 不匹配 → `401 Unauthorized`（`WebResponse::error`）
4. 未配置 API key → 跳过认证（开发模式）

WebSocket 认证通过 URL query param `?token=<key>` 传递，在 upgrade 前校验。

---

## 6. 运行时配置 API

### 6.1 配置热更新流程

```
PATCH /api/v1/config
{
  "risk.circuit_breaker.l1_loss_usd": 100,
  "sizing.max_position_usd": 200,
  "detection.endgame.min_edge_bps": 300
}
```

处理流程：

1. 校验 JSON path 是否存在于 `RuntimeConfig` schema
2. 校验值类型和范围（validator）
3. 生成 `ConfigAuditEntry`（who, when, path, old_value, new_value）
4. 原子更新 `ArcSwap<RuntimeConfig>` 快照
5. 持久化到 PostgreSQL `runtime_config` 表
6. 通过 WsBroadcaster 推送 `config.changed` 事件
7. 返回更新后的完整配置

### 6.2 审计日志

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigAuditEntry {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub path: String,
    pub old_value: serde_json::Value,
    pub new_value: serde_json::Value,
    pub source: ConfigChangeSource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigChangeSource {
    Api,
    Cli,
    StartupDefault,
}
```

### 6.3 不可热更新字段

以下字段仅在重启时生效（PATCH 返回 warning）：

- `db.*`（数据库连接配置）
- `analytics.*`（ClickHouse 连接配置）
- `cache.redis.*`（Redis 连接配置）
- `keys.*`（密钥配置）
- `venues.polymarket.onchain.*`（链上合约地址）

---

## 7. 实时数据推送架构

```
┌─────────────────┐
│  Core Subsystems │
│  (Detection,     │
│   Execution,     │──── CoreEvent ────▶ flume::Sender
│   Risk Engine)   │
└─────────────────┘
                                          │
                                          ▼
                                  ┌───────────────┐
                                  │ WsBroadcaster  │ (dedicated tokio task)
                                  │  - filter by   │
                                  │    subscription │
                                  │  - serialize    │
                                  │    to WsMessage │
                                  └───────┬───────┘
                                          │
                          ┌───────────────┼───────────────┐
                          ▼               ▼               ▼
                    ┌──────────┐   ┌──────────┐   ┌──────────┐
                    │ Session1 │   │ Session2 │   │ Session3 │
                    │ (UI tab) │   │ (mobile) │   │ (debug)  │
                    └──────────┘   └──────────┘   └──────────┘
```

核心系统通过 `flume::Sender<CoreEvent>` 发送事件，无需知道 WS 层存在。`WsBroadcaster` 消费事件并根据每个 session 的订阅状态进行分发。

```rust
/// Events emitted by core subsystems, consumed by WsBroadcaster.
pub enum CoreEvent {
    OpportunityDetected(Opportunity),
    OpportunityExpired(OpportunityId),
    TradeOpened(TradeInfo),
    TradeFilled(TradeRecord),
    TradeSettled { trade_id: TradeId, outcome: TradeOutcome, pnl: Usd },
    PnlUpdate { daily: Usd, total: Usd },
    SystemStatusChanged(SystemStatus),
    CircuitBreakerTripped { level: u8, reason: String },
    PositionChanged(PositionInfo),
    MarketResolved { market_id: MarketId, outcome: bool },
    ConfigChanged { path: String, old: serde_json::Value, new: serde_json::Value },
    Alert { level: AlertLevel, message: String },
}
```

---

## 8. 静态文件服务

生产模式下，Rust 二进制直接 serve Vue 构建产物：

```rust
/// Register static file serving for the Vue UI in production mode.
pub fn configure_static_files(cfg: &mut web::ServiceConfig, ui_dir: &str) {
    cfg.service(
        actix_files::Files::new("/", ui_dir)
            .index_file("index.html")
            .default_handler(|req: actix_web::dev::ServiceRequest| {
                let path = format!("{}/index.html", ui_dir);
                // SPA fallback: serve index.html for all non-API routes
                // so Vue Router handles client-side routing
                async move {
                    let response = actix_files::NamedFile::open(path)?
                        .into_response(&req.request());
                    Ok(req.into_response(response))
                }
            }),
    );
}
```

构建流程：
1. `cd oxide-arb-ui && pnpm build` → 产物在 `dist/`
2. 将 `dist/` 复制到 Rust 项目 `static/ui/`
3. Rust 二进制启动时检测 `static/ui/` 是否存在
4. 存在 → 注册静态文件路由；不存在 → 仅 API 模式

---

## 9. 错误响应 Envelope

所有 API 响应使用统一 JSON 格式：

### 9.1 成功响应

```json
{
  "code": 200,
  "message": "ok",
  "data": { ... }
}
```

### 9.2 错误响应

```json
{
  "code": 400,
  "message": "validation error: min_edge_bps must be >= 100",
  "data": null
}
```

### 9.3 分页响应

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "items": [ ... ],
    "total": 1234,
    "page": 1,
    "size": 50,
    "has_next": true
  }
}
```

### 9.4 WebError 枚举

```rust
#[derive(Debug, thiserror::Error)]
pub enum WebError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("unauthorized")]
    Unauthorized,

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("risk denial [level {level}]: {reason}")]
    RiskDenial { level: u8, reason: String },

    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),
}

impl actix_web::ResponseError for WebError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::RiskDenial { .. } => StatusCode::CONFLICT,
            Self::ServiceUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    fn error_response(&self) -> HttpResponse {
        WebResponse::error(self.status_code(), self.to_string())
    }
}
```

---

## 10. 路由注册

```rust
pub fn configure(cfg: &mut web::ServiceConfig, metrics_enabled: bool, serve_ui: bool) {
    // Public endpoints (no auth)
    health::configure(cfg);
    if metrics_enabled {
        metrics::configure(cfg);
    }

    // Authenticated API
    cfg.service(
        web::scope("/api/v1")
            .wrap(ApiKeyAuth)
            .configure(system::configure)
            .configure(markets::configure)
            .configure(opportunities::configure)
            .configure(trades::configure)
            .configure(risk::configure)
            .configure(config::configure)
            .configure(pnl::configure)
            .configure(replay::configure)
            .route("/ws", web::get().to(ws::handler::ws_upgrade)),
    );

    // Static UI files (production only)
    if serve_ui {
        static_files::configure_static_files(cfg, "static/ui");
    }
}
```

---

## 11. 验收检查清单

- [ ] 所有 REST 端点返回正确的 `WebResponse` JSON envelope
- [ ] `GET /health` 返回 200 + `{ "status": "ok" }`
- [ ] `GET /ready` 检查 PostgreSQL + ClickHouse + Redis 连通性
- [ ] `GET /metrics` 返回 Prometheus text format
- [ ] Bearer token 认证正确拦截未授权请求（401）
- [ ] 未配置 API key 时跳过认证（开发模式）
- [ ] WebSocket 连接成功建立，收到初始 `system.status` 快照
- [ ] WebSocket 订阅/取消订阅指令正确过滤消息
- [ ] 心跳超时（30s 无 pong）自动断开连接
- [ ] 断线重连后 `sync` 指令返回全量状态
- [ ] `PATCH /api/v1/config` 原子更新 `ArcSwap<RuntimeConfig>`
- [ ] 配置变更写入 `runtime_config` 表并生成审计日志
- [ ] 配置变更通过 WS 推送 `config.changed` 事件
- [ ] 不可热更新字段返回 warning 而非 error
- [ ] CORS 开发模式下 allow_any_origin
- [ ] 生产模式下 serve 静态 Vue 文件，SPA fallback 正确工作
- [ ] 分页参数校验（page >= 1, size ∈ [1, 100]）
- [ ] 全部端点有 request-id tracing
- [ ] `POST /api/v1/system/halt` 正确触发紧急停止
- [ ] `POST /api/v1/replay` 异步提交 replay 任务，返回 task_id

---

## 12. 预估工作量

| 组件 | 源码 LoC | 测试 LoC |
|---|---|---|
| `lib.rs` + `state.rs` | ~100 | ~30 |
| `response.rs` + `extractors.rs` | ~200 | ~120 |
| `middleware/` (auth + request_id) | ~150 | ~80 |
| `routes/system.rs` | ~120 | ~80 |
| `routes/markets.rs` | ~180 | ~100 |
| `routes/opportunities.rs` | ~150 | ~80 |
| `routes/trades.rs` + `pnl.rs` | ~250 | ~120 |
| `routes/risk.rs` | ~200 | ~100 |
| `routes/config.rs` | ~180 | ~80 |
| `routes/replay.rs` | ~80 | ~40 |
| `ws/` (handler + session + broadcaster + protocol) | ~500 | ~200 |
| `static_files.rs` | ~50 | ~20 |
| **合计** | **~2,160** | **~1,050** |

---

## 13. Phase 5.5 治理控制面接线 (Governance Control-Plane Wiring)

> 本节是 **Phase 5.5 Governance Core 的交接说明**。Phase 5.5 故意只交付 transport-agnostic 的治理内核(`oxide-arb-control` 的 `ControlFactorRegistry` + `oxide-arb-repository` 的原子审计链),**不含 HTTP、不含 RBAC 授权**。这里明确 Phase 6 必须如何实现与接线,避免漂移。

### 13.1 职责分层(必须遵守)

| 层 | 归属 | 职责 |
|---|---|---|
| authN(身份) | `oxide-arb-web` 中间件 | JWT/API-key → 解析出 actor + role |
| authZ(角色→是否放行) | `oxide-arb-web` RBAC(移植 ng-gateway Casbin) | 路由级:哪个角色能调哪个治理 endpoint |
| 变更信封 | `oxide-arb-web` handler | 从 `Claims` + 请求体构造 `AuditActor { actor, actor_role, request_id, reason }` (+ publish 的 `idempotency_key`) |
| 治理门控 | `ControlFactorRegistry`(已在 5.5 交付) | risk-expansion flag、rollback target、`AuditActor::validate()` reason 非空 |
| 原子状态机 + 审计链 | `ControlFactorRepository`(已在 5.5 交付) | 一事务内校验+状态变更+全局哈希链 append |

**关键原则**: authZ 不进入 `oxide-arb-control`/`oxide-arb-models`。治理内核**信任** web 层已经鉴权,只负责记录 `actor_role` 到审计链并执行治理不变量。`OperatorRole` 枚举(`oxide-arb-models::enums::control_factor`)是 web Casbin 角色码与审计 `actor_role` 列之间的**唯一规范来源**。

### 13.2 移植 ng-gateway RBAC(评估结论)

ng-gateway 的 RBAC = **JWT 登录 + Casbin(Postgres 策略,`g` user→role / `p` role→resource)+ Actix 路由级 `has_any_role`/`has_resource_operation`/`has_scope`**。评估:

- **够用**:承担 authN + 路由级 authZ 完全胜任,且与本 crate 同为 actix-web。
- **不可整体作为依赖**:跨 `models/common/web/repository/storage` 多 crate、与 IoT `EntityType`/`Operation` 耦合、角色为 DB 动态字符串(只 seed 了 `SYSTEM_ADMIN`)。**移植 = 复制其 Casbin 中间件模式**,不是加依赖。

移植清单(Phase 6 实现):

1. **拷贝模式**(非整 crate):`PermRule`/`CombinedPermRule`(`ng-gateway-models/src/rbac.rs`)、`NGPermChecker` 路由规则注册表(`ng-gateway-common/src/casbin/mod.rs`)、`NGCasbinService` + Postgres adapter(`ng-gateway-common/src/casbin/`)、`Authentication`/`CasbinService` 中间件(`ng-gateway-web/src/middleware/`)。
2. **角色码**:在 Casbin `g`/`p` 策略里新增 `viewer / operator / risk_owner / admin / emergency_operator`,与 `OperatorRole` 一一对应。
3. **修复 ng-gateway 已知缺陷**(移植时必须改):
   - 路由未注册规则时 ng-gateway **默认放行**(`NGPermChecker::check` 返回 `Ok(true)`)→ 治理面必须**默认拒绝**(fail-closed)。
   - WebSocket 路由在 ng-gateway 未挂鉴权 → 治理面无关,但若复用其 WS 代码需补鉴权。
   - 超级用户 bypass 用的是 username `"system_admin"` 字面量 → 改为基于 role code。
4. **per-endpoint 角色矩阵**(对应 Phase 5.5 §7,在 `init_rbac_rules` 注册):

| Endpoint | 方法 | 允许角色 | 治理 command |
|---|---|---|---|
| `/api/v1/control-factor-materializations` | POST | operator, risk_owner, admin, emergency_operator | enqueue run |
| `/api/v1/control-factors/candidates` 等 GET | GET | 全部(含 viewer) | 只读 |
| `/api/v1/control-factors/{id}/reject` | POST | operator, risk_owner, admin | `ControlFactorRegistry::reject_factor` |
| `/api/v1/control-factors/{id}/shadow` | POST | operator, risk_owner, admin | `ControlFactorRegistry::promote_to_shadow` |
| `/api/v1/control-factors/{id}/publish` | POST | risk_owner(conservative & risk-expanding) | `ControlFactorRegistry::publish` |
| `/api/v1/control-factors/publications/{id}/rollback` | POST | risk_owner, admin | `ControlFactorRegistry::rollback_publication` |
| `/api/v1/control-factors/.../emergency` | POST | emergency_operator | `ControlFactorRegistry::publish`(short-TTL) |
| `/api/v1/runtime-config/versions[/{id}/activate]` | POST | admin | `ControlFactorRegistry::create/activate_runtime_config_version` |

> 注意:risk-expanding publish 的「角色必须是 risk_owner」由这里的路由规则保证;治理内核只校验 `manual_risk_expansion_approval` flag + justification + rollback target,二者叠加才放行。

### 13.3 endpoint → command 映射 + 信封构造

每个 mutating handler 的统一骨架:

```rust
// 1. authZ 已由 CasbinService 中间件完成(否则到不了这里)
// 2. 从 Claims + 请求体构造审计信封
let envelope = AuditActor {
    actor: claims.user_id.clone(),
    actor_role: map_casbin_role_to_operator_role(&roles)?, // → OperatorRole
    request_id: request_id_header(&req),                    // X-Request-Id / 生成
    reason: body.reason.clone(),                            // 必填,空 → 400
};
// 3. 调用治理内核(它再 validate envelope + 治理门控 + 原子审计链)
let outcome = registry.publish(envelope, command).await?;
```

`reason` 缺失 → handler 校验或 `AuditActor::validate()` 返回 `GovernanceError::MissingReason`。`idempotency_key` 对 publish 来自请求体,放进 `PublishCommand`。

### 13.4 error → HTTP status 映射(必须实现)

`RegistryError` / `GovernanceError` → status:

| 错误 | HTTP |
|---|---|
| `GovernanceError::MissingReason` / `MissingField` | 400 |
| Casbin 拒绝(角色不足) | 403 |
| `GovernanceError::RiskExpansionNotApproved` | 403 |
| `GovernanceError::RollbackTargetMissing` | 409 |
| `GovernanceError::PublicationLockConflict` | 409 |
| `PublishPublicationOutcome::AlreadyApplied`(幂等重放) | 200(返回已存在 publication) |
| `GovernanceError::FactorNotReadyForPublication` / `FactorSetMismatch` / `EmptyPublication` | 409 |
| `GovernanceError::AuditChain(_)` / `Storage(_)` | 500 |
| `StorageError::NotFound` | 404 |

### 13.5 Scheduler 进程接线(Phase 5.5 D2 交接)

Phase 5.5 D2 已交付**可测库**(`crates/oxide-arb-control/src/scheduler/`):`SchedulePolicy` / `ScheduledMaterialization`(策略数据 + `production_default(...)` 覆盖 4 个周期 cadence)、纯判定 helper(`is_due` / `staleness` / `is_overdue`)、以及 `MaterializationScheduler::tick(now) -> SchedulerCycleReport`(enqueue-only、`run_dedupe_key` 去重、missed/stale 告警作为数据返回)。进程接线在此明确:

- **依赖**:`oxide-arb-core` 当前**未**依赖 `oxide-arb-control`;接线时在 `crates/oxide-arb-core/Cargo.toml` 增加该依赖(control 不依赖 core hot path,无循环)。
- **位置 / tick 循环**:在 `crates/oxide-arb-core/src/app/bootstrap.rs` 的 `ctx.queue_periodic_services()` 旁新增 `ctx.queue_control_factor_scheduler()`,用现有 `PeriodicTask`(`crates/oxide-arb-core/src/infra/periodic_task.rs`)每个 interval 调用一次 `MaterializationScheduler::tick(Utc::now())`。
- **execute worker(Phase 6 接线)**:tick 只产出 `Queued` run。另起一个独立 worker(同样由 `PeriodicTask` 或专用消费循环驱动)轮询 `Queued` run 并调用 `MaterializationRunner::execute_run`;调度循环与 execute worker **都**是 Phase 6 进程接线,scheduler 库本身不执行 run。
- **`SchedulePolicy` 来源**:由 runtime config / `config/oxide-arb.toml` 注入(`production_default` 仅为缺省);`created_by` / `code_git_sha` 写入 manifest。
- **仓库注入**:在 `AppContext::build`(`crates/oxide-arb-core/src/app/build.rs` 的 `BuildRepos`)构造 `PgControlFactorRepository`,注入 scheduler、execute worker 与未来 web。
- **告警映射**:`SchedulerCycleReport::alerts` 中的 `ScheduleAlert::Overdue { schedule_id, last_run_at }` 与 `ScheduleAlert::Stale { schedule_id, last_success_at, threshold_secs }` 由接线层映射到现有 `AlertDispatcher`;manual backfill/incident run 写审计。
- **行为**:4 个周期 cadence(execution-quality hourly / reconciliation hourly / bucket-risk daily / portfolio-risk daily)按 `run_dedupe_key` 去重 enqueue;`market-anomaly` 为事件驱动(incident run),不在周期 scheduler 内。
- **never-publish 保证**:scheduler 只调用 `latest_run_for_schedule` + `enqueue_materialization_run`,无 `publish_publication` 访问路径(单测 `scheduler::tests` 断言 `publish_calls() == 0`)。
- **shadow 聚合(promotion review)**:`ControlFactorShadowDecisionRepository::aggregate_shadow_decisions` / `list_shadow_decisions` 已就绪;promotion-review consumer 从 `list_shadow_decisions` 原始行计算 delta 分位分布(不入库)。

### 13.6 Live refresher 接触点(Phase 5.6 备注)

publish/rollback 后,Phase 5.6 的 `oxide-arb-core/src/control/factor_refresher.rs` 通过轮询 `load_active_publication(Published)` + 校验 TTL/hash 构建 `ArcSwap<ControlFactorSnapshot>`;web 的 publish/rollback 可选地发 notify 加速 refresh(periodic poll 兜底)。本阶段不实现。
