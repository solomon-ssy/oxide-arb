# Phase 1 — 数据接入层

> **产出**: `oxide-arb-api` crate
>
> **前置条件**: Phase 0 完成
>
> **验收标准**: 可独立连接 Polymarket CLOB WS 接收实时 orderbook 更新；Gamma API 全量/增量同步市场元数据；费率计算与官方公式一致；结算预言机可查询 market resolution 状态

---

## 0. 工作范围

`oxide-arb-api` 封装所有与 Polymarket 平台的交互：

1. **CLOB WebSocket** — 实时订单簿数据流（book/price_change/best_bid_ask）
2. **CLOB REST** — 订单提交/取消/查询、fee-rate 查询、book 快照
3. **Gamma API** — 市场/事件目录发现与元数据同步
4. **Fee Service** — Polymarket 专有费率计算引擎
5. **Settlement Oracle** — Gamma + CTF on-chain + UMA 交叉校验
6. **Keystore** — 私钥管理、EIP-712 签名、L2 HMAC 认证

---

## 1. 目录结构

```
crates/oxide-arb-api/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── clob/                   # CLOB REST (orders, books) + convert (Side ↔ SDK)
    ├── ws/                     # Sharded WebSocket manager (wraps SDK WS)
    ├── gamma/                  # Gamma API client + sync + mapper
    ├── fees/                   # FeeCalculator struct + formula + rate_cache
    ├── oracle/                 # OracleSource trait, Gamma + CTF, VotingOracle (2-of-2)
    ├── keystore/               # Keystore struct, OrderSigner, L2Credentials
    └── infra/                  # Shared retry policy
```

Errors live in `oxide-arb-error` (`api::ApiError`, `ws::WsError`, `rpc::RpcError`, …), not a local `error.rs`.

---

## 2. Cargo.toml

```toml
[package]
name = "oxide-arb-api"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[dependencies]
oxide-arb-error = { workspace = true }
oxide-arb-models = { workspace = true }

# Polymarket SDK (v2)
polymarket_client_sdk_v2 = { workspace = true }

# Networking
reqwest = { workspace = true }
# WebSocket via polymarket_client_sdk_v2 (`ws` + `heartbeats`) — no tokio-tungstenite

# Ethereum / Polygon
alloy = { workspace = true }

# Async runtime
tokio = { workspace = true }
tokio-util = { workspace = true }
futures-util = { workspace = true }

# Serialization
serde = { workspace = true }
serde_json = { workspace = true }

# Decimal
rust_decimal = { workspace = true }

# Time
chrono = { workspace = true }

# Concurrency
arc-swap = { workspace = true }
dashmap = { workspace = true }
parking_lot = { workspace = true }
flume = { workspace = true }

# Retry
backoff = { workspace = true }

# Logging
tracing = { workspace = true }

# Error
thiserror = { workspace = true }

# Security
zeroize = { workspace = true }
hex = { workspace = true }

# Async trait
async-trait = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
wiremock = "0.6"

[lints]
workspace = true
```

---

## 3. CLOB WebSocket 管理器

### 3.1 架构

```rust
/// Manages sharded WebSocket connections to Polymarket CLOB.
///
/// Each shard handles up to MAX_TOKENS_PER_SHARD (200) token subscriptions.
/// Messages are normalized and dispatched to a unified output channel.
pub struct ClobWsManager {
    shards: Vec<WsShard>,
    output_tx: flume::Sender<WsEvent>,
    config: WebSocketConfig,
    shutdown: CancellationToken,
}

pub enum WsEvent {
    BookSnapshot {
        token_id: TokenId,
        bids: Vec<PriceLevel>,
        asks: Vec<PriceLevel>,
        timestamp: u64,
    },
    PriceChange {
        token_id: TokenId,
        changes: Vec<PriceLevelChange>,
        timestamp: u64,
    },
    BestBidAsk {
        token_id: TokenId,
        best_bid: Price,
        best_ask: Price,
        timestamp: u64,
    },
    TickSizeChange {
        token_id: TokenId,
        old_tick: TickSize,
        new_tick: TickSize,
    },
    MarketResolved {
        market_id: MarketId,
        winning_token_id: TokenId,
    },
    ConnectionStatus {
        shard_id: usize,
        connected: bool,
    },
}
```

### 3.2 重连策略

- 断线后立即重连
- 前 3 次：1s 间隔
- 之后：指数退避 2s, 4s, 8s, 16s, max 30s
- 重连后重新订阅全部 token
- 重连期间通过 `ConnectionStatus` 事件通知上游

### 3.3 心跳

由 **`polymarket_client_sdk_v2`** 的 `heartbeats` feature 处理（`WsShard` 不实现应用层 ping）。
`[market_data.websocket]` 仅配置 **分片订阅上限** 与 **重连退避**（`reconnect_delay_ms` / `max_reconnect_delay_ms`）。

---

## 4. Gamma API Client

### 4.1 接口

```rust
pub struct GammaClient {
    http: reqwest::Client,
    base_url: String,
    config: GammaConfig,
}

impl GammaClient {
    /// Full sync: paginate all active events + their markets.
    pub async fn full_sync(&self) -> ApiResult<Vec<GammaEvent>> { ... }

    /// Incremental sync: events changed since timestamp.
    pub async fn incremental_sync(&self, since: DateTime<Utc>) -> ApiResult<Vec<GammaEvent>> { ... }

    /// Single market details.
    pub async fn get_market(&self, condition_id: &str) -> ApiResult<GammaMarket> { ... }

    /// Check if market is closed + its outcome.
    pub async fn get_resolution(&self, slug: &str) -> ApiResult<Option<GammaResolution>> { ... }
}
```

### 4.2 同步策略

| 模式 | 间隔 | 用途 |
|---|---|---|
| 全量同步 | 300s (5min) | 基线校对 + 发现新市场 |
| 增量同步 | 60s | 捕获新市场/状态变化 |
| 按需查询 | 实时 | 单市场 resolution 检查 |

---

## 5. Fee Calculator

### 5.1 公式实现

```rust
/// Polymarket fee formula:
/// fee = shares × price × feeRate × (price × (1 - price))^exponent
///
/// - feeRate and exponent are per-category parameters
/// - Precision: 4 decimal places; < 0.0001 rounds to 0
pub fn calculate_fee(
    shares: Shares,
    price: Price,
    fee_rate: Decimal,
    exponent: Decimal,
    fees_enabled: bool,
) -> Usd {
    if !fees_enabled {
        return Usd::ZERO;
    }

    let p = price.inner();
    let p_complement = Decimal::ONE - p;
    let volatility_factor = (p * p_complement).powd(exponent);
    let raw_fee = shares.inner() * p * fee_rate * volatility_factor;

    let rounded = raw_fee.round_dp(4);
    if rounded < Decimal::new(1, 4) {
        Usd::ZERO
    } else {
        Usd::new(rounded)
    }
}
```

### 5.2 Fee Rate Source

```rust
/// Maintains a lock-free snapshot of per-token fee rates.
/// Updated periodically from Gamma API category data + CLOB fee-rate endpoint.
pub struct FeeRateSource {
    snapshot: Arc<ArcSwap<FeeSnapshot>>,
}

pub struct FeeSnapshot {
    pub rates: HashMap<MarketCategory, CategoryFeeParams>,
    pub per_token_enabled: HashMap<TokenId, bool>,
    pub updated_at: DateTime<Utc>,
}

pub struct CategoryFeeParams {
    pub fee_rate: Decimal,
    pub exponent: Decimal,
}
```

---

## 6. Settlement Oracle

### 6.1 OracleSource Trait

```rust
#[async_trait]
pub trait OracleSource: Send + Sync {
    fn source_id(&self) -> &str;

    /// Query resolution status for a market.
    async fn query_resolution(
        &self,
        market_id: &MarketId,
        condition_id: &str,
    ) -> ApiResult<Option<SourceVote>>;
}

pub struct SourceVote {
    pub source_id: String,
    pub actual_yes: bool,
    pub confidence: Decimal,
    pub reported_at: DateTime<Utc>,
}
```

### 6.2 VotingOracle (2-of-3 quorum)

Production stack: **Gamma + CTF on-chain + UMA** with `[settlement_oracle].voting_quorum` (default 2).
Built via `oracle::build_voting_oracle(&polymarket, &gamma, &settlement_oracle)`.

```rust
pub struct VotingOracle {
    sources: Vec<Arc<dyn OracleSource>>,
    quorum: usize,
}

impl VotingOracle {
    /// Resolve a market by querying all sources and requiring quorum agreement.
    pub async fn resolve(&self, market_id: &MarketId, condition_id: &str)
        -> ApiResult<Option<ResolutionVerdict>>
    {
        // Query all sources concurrently
        // Require at least 2 agreeing votes
        // If disagreement → return None (manual intervention needed)
        todo!()
    }
}
```

### 6.3 CTF On-chain Source

```rust
pub struct CtfOracleSource {
    provider: Arc<alloy::providers::Provider<Http<reqwest::Client>>>,
    ctf_address: alloy::primitives::Address,
}

impl OracleSource for CtfOracleSource {
    async fn query_resolution(&self, _market_id: &MarketId, condition_id: &str)
        -> ApiResult<Option<SourceVote>>
    {
        // Call getPayouts(conditionId) on CTF contract
        // Parse payoutNumerators: [yes_payout, no_payout]
        // yes_payout == 1e18 → actual_yes = true
        todo!()
    }
}
```

---

## 7. Keystore

### 7.1 Key loading (concrete struct, ADR-001)

```rust
pub struct Keystore {
    signer: OrderSigner,
    credentials: Option<L2Credentials>,
}

impl Keystore {
    pub fn from_config(config: &KeysConfig) -> Result<Self, SigningError>;
}
```

Loads hex private key from config/env; L2 HMAC triple optional via env vars.

### 7.2 Order Signing

```rust
pub struct OrderSigner {
    key: SigningKey,
    chain_id: u64,
}

impl OrderSigner {
    /// Sign an order using EIP-712 typed data (delegated to polymarket-client-sdk).
    pub fn sign_order(&self, order: &UnsignedOrder) -> ApiResult<SignedOrder> { ... }

    /// Derive L2 HMAC credentials from the signing key.
    pub async fn derive_l2_credentials(&self) -> ApiResult<L2Credentials> { ... }
}
```

---

## 8. Error Types

Implemented in `crates/oxide-arb-error/src/{api,ws,rpc,signing,...}.rs` and composed
into `OxideError` via `#[from]`. `oxide-arb-api` re-exports `OxideResult` and `ApiError`.
`ApiError` includes `is_retryable()` / `retry_after_ms()` for smart retries.

---

## 9. 验收检查清单

- [x] `ClobWsManager` 可连接 Polymarket WS 并接收 book snapshot（`tests/integration/ws_book.rs`，`--ignored` + CI `network-integration` job）
- [x] WebSocket 断线重连策略（`ReconnectPolicy` / `infra::retry`，shard 级单测）
- [x] `GammaClient::full_sync()` / `incremental_sync()`（wiremock + 可选 live `integration/gamma_sync.rs`）
- [x] `calculate_fee()` 与官方公式一致（`fees/golden.rs` + reference）
- [x] `CtfOracleSource` 可查询已 resolve 市场（`integration/ctf_oracle.rs`，需 Alchemy RPC + `OXIDE_ARB_TEST_RESOLVED_CONDITION_ID`）
- [x] `OrderSigner` / L2 凭证（`keystore_test.rs` + `integration/clob_auth.rs`）
- [x] 网络调用超时 + 重试（`infra/retry.rs`，Gamma/CLOB wiremock 429）
- [x] Rate limiting（`clob/rate_limiter.rs` + 429 wiremock）
- [x] Fee 边界（price=0, 0.5, 1）

**网络集成测试运行方式**（见 `docs/operations/network-integration.md`）：

```bash
cargo test -p oxide-arb-api --features integration -- --ignored
```

---

## 10. 预估工作量

| 组件 | 源码 LoC | 测试 LoC |
|---|---|---|
| `client/ws.rs` (WS manager) | ~600 | ~300 |
| `client/clob.rs` (REST) | ~400 | ~200 |
| `client/gamma.rs` | ~350 | ~200 |
| `fees/` | ~300 | ~250 |
| `oracle/` | ~500 | ~300 |
| `keystore/` | ~300 | ~150 |
| `types.rs` + `error.rs` | ~250 | ~80 |
| **合计** | **~2,700** | **~1,480** |
