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
