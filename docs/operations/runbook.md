# quant-pivot 生产运行 Runbook

> Last reviewed: 2026-07-02.
>
> This document is an operating manual for quant-pivot on Polymarket. It explains how to prepare credentials and capital, start the system, read reports, place governed orders, sell or redeem positions, and respond to incidents. It is not investment advice. Every buy/sell decision must be traceable to a published `RecommendationReport`, an `OrderIntent`, an `ExecutionOrder`, or an explicit operator incident action.

## 0. 核心原则

1. **Polymarket-only.** 本系统只支持 Polymarket Gamma、CLOB、Data API、Polygon 结算链路。
2. **主产物是 `RecommendationReport`。** 系统先给出 Top-N 推荐，推荐里包含买什么、什么时候买、买多少、什么时候卖、卖多少、依据什么。
3. **`report_only` 不是模拟。** `report_only` 不签名、不下单，但报告 sizing 基于真实 venue 账户：CLOB collateral 加 Data API positions。因此启动和生成报告也需要真实 `private_key`、`quant.account.funder` 和可读账户。
4. **私钥只在可执行模式签名。** `semi_auto` / `auto_execution` 下提交订单时才会用私钥签 CLOB order；`report_only` 只用认证客户端读取账户和 CLOB L2 凭证。
5. **所有执行默认 fail-closed。** 缺私钥、缺 funder、账户不一致、runtime-config 无效、数据质量不足、book 过期、kill switch 非 `closed`、capital/reconciliation 异常，都会拒绝或延后执行。
6. **人工只能收紧订单。** `approve` 允许降低 shares、降低 limit price、降低 max notional；不能放大报告给出的风险包络。
7. **资金真相来自 venue。** `AccountSnapshot` 使用 CLOB collateral 和 Data API positions，runtime budget 只是上限，不是凭空可花资金。
8. **先治理，后执行。** 生产订单优先走 `semi_auto` 或 `auto_execution` 的 `OrderIntent` 链路；直接在 Polymarket UI 手动交易只适合作为人工 `report_only` 操作或事故处置，审计和 attribution 会弱于系统内订单。

## 1. 外部资料与系统事实来源

运行前应核对外部接口文档，因为 Polymarket 的 bridge、wallet 类型、费用和 CLOB 约束可能变化。

| 主题 | 来源 |
|------|------|
| Polymarket CLOB、认证、签名类型 | [Trading Overview](https://docs.polymarket.com/trading/overview), [Authentication](https://docs.polymarket.com/api-reference/authentication) |
| 创建订单、tick size、allowance、order type | [Create Order](https://docs.polymarket.com/trading/orders/create) |
| 交易费用 | [Fees](https://docs.polymarket.com/trading/fees) |
| 充值 / bridge supported assets | [Deposit](https://docs.polymarket.com/trading/bridge/deposit), [Supported Assets](https://docs.polymarket.com/trading/bridge/supported-assets) |
| 提现 | [Withdraw](https://docs.polymarket.com/trading/bridge/withdraw) |
| 到期赎回 / 合并 token | [Redeem Tokens](https://docs.polymarket.com/trading/ctf/redeem), [Merge Tokens](https://docs.polymarket.com/trading/ctf/merge) |
| Gasless relayer | [Gasless Transactions](https://docs.polymarket.com/trading/gasless) |
| 本仓库 deploy config | `config/quant-pivot.toml`, `config/quant-pivot.production.example.toml`, `crates/quant-pivot-models/src/config/` |
| 本仓库 runtime config | `crates/quant-pivot-models/src/runtime_config/` |
| API routes | `crates/quant-pivot-web/src/routes/` |
| 执行 / reconciliation | `crates/quant-pivot-core/src/execution/` |

## 2. 角色与权限

| 角色 | 可以做什么 | 禁止事项 |
|------|------------|----------|
| Operator | 启停进程、健康检查、运行 ad-hoc report、切 mode、设置 kill switch、处理事故 | 不修改策略参数除非有量化/负责人授权 |
| Quant | 配置 selection、features、factors、model、portfolio、reports、execution 策略；解释推荐 | 不直接绕过治理提交订单 |
| Approver | 在 `semi_auto` 审批或拒绝 `OrderIntent` | 不扩大 shares、price、notional |
| Admin | 管理用户、角色、JWT、部署凭证、runtime-config 激活/回滚 | 不把私钥、JWT secret、relayer key 写入仓库 |

新部署会 seed `admin` / `admin`。首次启动后必须立即改密码或创建新的管理员并禁用默认口令。

## 3. Runtime mode 与 kill switch

### 3.1 Runtime mode

| Mode | 报告 | 创建 intent | 人工审批 | 自动策略批准 | 签名 / 提交订单 |
|------|------|-------------|----------|--------------|-----------------|
| `report_only` | 是 | 否 | 不适用 | 否 | 否 |
| `semi_auto` | 是 | 是，状态 `pending_approval` | 必须 | 否 | 仅 `approve` 后人工 `submit` |
| `auto_execution` | 是 | 是，状态 `approved_by_policy` | 非必需 | 是 | admission 通过后自动或人工提交 |

允许的升级/降级路径：

```mermaid
stateDiagram-v2
    [*] --> report_only
    report_only --> semi_auto: preflight
    semi_auto --> report_only: tighten
    semi_auto --> auto_execution: preflight
    auto_execution --> semi_auto: tighten
    auto_execution --> report_only: tighten
```

`report_only` 不能直接升级到 `auto_execution`。先进入 `semi_auto`，完成 shadow / readiness 后再升级。

### 3.2 Kill switch

| State | 新开仓 | 普通自动卖出 | 用途 |
|-------|--------|--------------|------|
| `closed` | 允许 | 允许 | 正常状态 |
| `report_only_forced` | 禁止 | 允许 | 强制只生成报告，不新增 exposure |
| `exit_only` | 禁止 | 允许 | 只允许退出或减仓 |
| `execution_halted` | 禁止 | 禁止 | 暂停所有自动执行，人工处理 |
| `emergency_halted` | 禁止 | 禁止普通自动退出；进入紧急处置 | 严重事故，清除时需要 operator ack |

设置 kill switch 示例：

```bash
BASE=http://127.0.0.1:8080
TOKEN=...

curl -sS -X POST "$BASE/api/system/kill-switch" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: operator" \
  -H "Content-Type: application/json" \
  -d '{
    "state": "exit_only",
    "reason": "venue instability: stop new entries, keep exits enabled",
    "ack": false
  }'
```

清除 `emergency_halted` 时必须显式设置 `ack: true`，并在 reason 里写明事故编号和复盘结论。

## 4. 运行前准备清单

### 4.1 外部账户与凭证

| 项 | 是否必须 | 用于 | 配置位置 | 说明 |
|----|----------|------|----------|------|
| Polygon / Polymarket signer private key | 所有 mode 必须 | CLOB auth、账户读取、可执行模式签订单 | `QUANT_PIVOT__KEYS__PRIVATE_KEY` 或 `[keys].private_key` | 不要提交到 git；生产用 secret manager 注入 |
| `quant.account.funder` | 所有 mode 必须 | 读取 collateral、positions、计算 capital base | `QUANT_PIVOT__QUANT__ACCOUNT__FUNDER` | EOA 必须等于 signer 地址；proxy/safe 必须是 signer 控制的钱包地址 |
| `quant.account.wallet_kind` | 所有 mode 必须 | 决定签名类型和 funder 校验 | `QUANT_PIVOT__QUANT__ACCOUNT__WALLET_KIND` | 当前代码支持 `eoa`、`proxy`、`gnosis_safe` |
| CLOB L2 credentials | 不单独配置 | CLOB trading endpoints | 自动派生 | SDK connect 时由 private key 和 wallet topology 派生 |
| Polygon RPC URL | 所有 mode 必须 | on-chain 读写、结算、赎回 | `QUANT_PIVOT__POLYMARKET__ONCHAIN__RPC_URL` | 生产必须使用可靠 RPC，配置超时 |
| Gasless relayer key/address | proxy/safe 且会提交链上交易时必须 | gasless approval/redeem/settlement | `QUANT_PIVOT__POLYMARKET__RELAYER__API_KEY`, `...__API_KEY_ADDRESS` | EOA 可直接付 gas；relayer key 不得暴露到前端 |
| JWT secret | Web API 必须 | 登录和 API 认证 | `QUANT_PIVOT__WEB__JWT__SECRET` | `auto_execution` 前必须是强 secret，不能使用默认 |
| Telegram / webhook secrets | 可选 | 通知 | runtime-config `notification.*` | runtime-config 里会 mask 敏感路径 |

注意：Polymarket 官方文档列出 Deposit Wallet / `POLY_1271` 等签名类型，但当前代码只建模 `eoa`、`proxy`、`gnosis_safe`。如果要接入 Deposit Wallet，需要先扩展 wallet topology、配置校验、CLOB client 和 relayer 路径。

### 4.2 基础设施

| 组件 | 用途 | 运行前检查 |
|------|------|------------|
| Postgres | 系统主库、runtime-config、reports、intents、orders、positions、operation log | schema 可迁移，连接池账号有读写权限 |
| ClickHouse | market facts、features、数据质量、研究分析 | database 存在，批量写入权限正常 |
| Redis | JWT revocation、缓存、运行时辅助状态 | 连接、认证、DB、key prefix 正确 |
| Web server | API、UI、WS、metrics | listen host/port、CORS、JWT 配置正确 |
| Metrics backend | `/metrics` scrape | Prometheus 或同等采集已配置 |
| Log backend | 结构化日志 | production 建议 `log_json=true` |

### 4.3 Deploy config 与 runtime-config 分工

**Deploy config** 来自：

1. 默认值；
2. `config/quant-pivot.toml`；
3. `config/quant-pivot.local.toml`；
4. `QUANT_PIVOT__...` 环境变量；
5. CLI `--config-dir` 或 `QUANT_PIVOT_CONFIG_DIR` 指定配置目录。

Deploy config 只放连接、凭证、基础设施、Polymarket endpoints、账户 topology。不要把策略、风控、报告调度写进 TOML。

**Runtime config** 是版本化 JSON，当前 schema version 是 9。它包含：

`selection`、`data_quality`、`features`、`factors`、`model`、`quality_gate`、`training`、`reports`、`portfolio`、`execution`、`notification`。

runtime-config 必须通过 API 新建版本并激活；激活失败会自动回滚到上一版。

### 4.4 最小生产环境变量示例

不要把真实值写入仓库。以下只展示 key 形状：

```bash
export QUANT_PIVOT_CONFIG_DIR=/etc/quant-pivot

export QUANT_PIVOT__KEYS__PRIVATE_KEY="0x..."
export QUANT_PIVOT__QUANT__ACCOUNT__FUNDER="0x..."
export QUANT_PIVOT__QUANT__ACCOUNT__WALLET_KIND="eoa"

export QUANT_PIVOT__POLYMARKET__CLOB_BASE_URL="https://clob.polymarket.com"
export QUANT_PIVOT__POLYMARKET__CLOB_WS_URL="wss://ws-subscriptions-clob.polymarket.com/ws/"
export QUANT_PIVOT__POLYMARKET__CHAIN_ID="137"
export QUANT_PIVOT__POLYMARKET__ONCHAIN__RPC_URL="https://polygon-rpc.example"

export QUANT_PIVOT__MARKET_DATA__GAMMA__BASE_URL="https://gamma-api.polymarket.com"
export QUANT_PIVOT__MARKET_DATA__DATA_API__BASE_URL="https://data-api.polymarket.com"

export QUANT_PIVOT__DB__POSTGRES__HOST="postgres.internal"
export QUANT_PIVOT__DB__POSTGRES__USER="quant_pivot"
export QUANT_PIVOT__DB__POSTGRES__PASSWORD="..."
export QUANT_PIVOT__DB__POSTGRES__DATABASE="quant_pivot"

export QUANT_PIVOT__DB__CLICKHOUSE__URL="http://clickhouse.internal:8123"
export QUANT_PIVOT__DB__CLICKHOUSE__USER="quant_pivot"
export QUANT_PIVOT__DB__CLICKHOUSE__PASSWORD="..."
export QUANT_PIVOT__DB__CLICKHOUSE__DATABASE="quant_pivot"

export QUANT_PIVOT__CACHE__REDIS__HOST="redis.internal"
export QUANT_PIVOT__CACHE__REDIS__PASSWORD="..."

export QUANT_PIVOT__WEB__JWT__SECRET="replace-with-strong-secret"
```

proxy/safe 且需要 relayer 时增加：

```bash
export QUANT_PIVOT__POLYMARKET__RELAYER__API_KEY="..."
export QUANT_PIVOT__POLYMARKET__RELAYER__API_KEY_ADDRESS="0x..."
```

## 5. 钱包、充值、allowance 与提现

### 5.1 选择 wallet topology

| Topology | `wallet_kind` | `funder` 应该是什么 | 适用场景 | 风险点 |
|----------|---------------|----------------------|----------|--------|
| EOA | `eoa` | signer address | 最简单，EOA 自己付 gas | 私钥直接控制资金，必须严控 secret |
| Proxy wallet | `proxy` | signer 控制的 proxy wallet 地址 | 历史 Polymarket 账户或 proxy 体系 | funder 不等于 signer，需要 relayer/代理链路校验 |
| Gnosis Safe | `gnosis_safe` | signer 控制的 Safe 地址 | 多签/机构化 custody | relayer、Safe owner、签名类型和 allowance 更复杂 |

上线前检查：

1. signer private key 能派生预期 signer address；
2. `funder` 与 `wallet_kind` 的关系满足代码校验；
3. `GET /api/system/deploy-config` 返回 `keys.private_key_present=true`；
4. `GET /api/quant/account/live` 能读到 collateral、positions 和 capital base；
5. 如果要提交订单，Polymarket 侧 allowance 足够。

### 5.2 充值 SOP

充值目标是让 `funder` 获得可用于 Polymarket CLOB 的 pUSD / collateral。Polymarket bridge 支持的 chain/token、最小额和流程会变化，操作前必须核对官方 `supported-assets`。

1. **确认收款钱包。** 使用 `quant.account.funder`，不是随手复制 signer address。EOA 下二者相同；proxy/safe 下通常不同。
2. **检查当前系统账户。**

   ```bash
   curl -sS "$BASE/api/quant/account/live" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Accept-Api-Version: v1" | jq .
   ```

3. **查 supported assets。** 按 Polymarket Bridge 文档调用 supported-assets，确认源链、token、minimum、预计时间和输出资产。
4. **生成本次充值地址。** 通过 Polymarket Bridge / UI 为目标 wallet 请求本次充值地址。不要复用旧页面或不明来源地址。
5. **从源链转入。** 只发送 supported token。错误 chain/token 可能无法找回。大额资金分批；官方文档建议超过 50k USD 的非 Polygon bridge 考虑拆分或使用第三方 bridge。
6. **等待 bridge 完成。** 跟踪 bridge status、源链 tx、Polygon 到账情况。
7. **系统侧复核。** 到账后刷新 `GET /api/quant/account/live`，确认：
   - `collateral` 增加；
   - `venue_net_liquidation = collateral + positions_value` 合理；
   - `capital_base = min(venue_net_liquidation, runtime budget cap)`；
   - `available` 足够覆盖计划交易。
8. **调整 runtime budget。** 如果新增资金只是备用，不希望策略使用全部余额，降低 `portfolio.budget.total_budget_usd` 或各 exposure caps。

充值完成前不要提高 mode；账户读取失败时不要假定资金可用。

### 5.3 订单 allowance

Polymarket CLOB 下单要求：

| 方向 | 需要的 allowance |
|------|------------------|
| BUY | pUSD / collateral allowance >= spend |
| SELL | conditional token allowance >= sell amount |

当前系统的 CLOB client 负责认证和下单，但没有在 runbook 层保证自动补齐所有 allowance。上线前必须用 Polymarket UI、官方 SDK 或 wallet 工具确认 funder 对 CLOB/exchange adapter 的 allowance 足够。allowance 不足时，系统 admission 可能通过，但 venue 提交会失败或 reconciliation 进入异常路径。

### 5.4 费用、滑点和 bridge 成本

运行前必须把三类成本纳入决策：

| 成本 | 来源 | 系统如何处理 | 操作注意 |
|------|------|--------------|----------|
| CLOB trading fee | Polymarket fee schedule | SDK/venue 层处理 fee 计算；report 和 attribution 应使用成交后事实复核 | 不要用手工估算替代实际成交和 fee 记录 |
| Slippage / spread | CLOB order book | `entry_order_policy.max_slippage_bps`、entry plan limit cap、admission `slippage` check | marketable order 也必须有 worst-price cap |
| Bridge / intermediary / gas / liquidity cost | 充值提现、跨链、relayer、RPC/on-chain | 不进入 recommendation alpha；作为运营成本单独记录 | Polymarket 文档说明平台本身可能不收充值/提现费，但中间路由、流动性、gas 和第三方服务可能产生成本 |

Polymarket 当前文档描述 taker fee 与成交额、fee rate 和价格相关，maker fee 为 0，SDK 会处理 venue fee 细节。运营上仍要用实际 execution/trade/settlement 结果做账，不要在报告阶段提前把 fee 估成确定 PnL。

### 5.5 提现 SOP

提现是资金移出系统，必须先冻结新增风险。

1. **切到安全状态。**
   - 常规提现：切 `report_only` 或设置 `report_only_forced`。
   - 有未平仓但只想停止开仓：设置 `exit_only`。
   - 不要在 `auto_execution` 且 kill switch `closed` 时提现。

2. **确认没有进行中的系统动作。**

   ```bash
   curl -sS "$BASE/api/quant/intents?status=admission_pending" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Accept-Api-Version: v1" | jq .

   curl -sS "$BASE/api/quant/execution-orders?state=submitted" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Accept-Api-Version: v1" | jq .

   curl -sS "$BASE/api/quant/reconciliations?result=pending" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Accept-Api-Version: v1" | jq .
   ```

3. **计算可提现额。** 以 venue collateral 为上限，扣除：
   - open orders / reserved capital；
   - `quant_capital_allocation` 中 `allocated`、`locked`、`impaired`；
   - 近期待 reconciliation 的 ambiguous order；
   - 计划保留的最小操作现金。

4. **通过 Polymarket Withdraw / Bridge 发起提现。**
   - 指定目标 chain、目标 token、destination address；
   - 根据官方页面生成本次 withdrawal address；
   - 不要预生成和长期保存 withdrawal address；
   - 大额提现拆分；如果流动性池不足，等待或分批；
   - pUSD 直接提现需要目标地址能识别 pUSD，否则可能需要 swap/bridge 成更通用资产。

5. **系统侧复核。** 提现完成后再次读取 `GET /api/quant/account/live`。如 capital base 下降，必须同步降低 runtime-config 中 budget/caps，否则后续 report 会被 budget exhausted 或 admission 拒绝。

6. **解除冻结。** 只有当 account snapshot、reconciliation、positions 都一致后，才把 kill switch 恢复为 `closed` 或切回目标 mode。

## 6. 启动与基础健康检查

### 6.1 构建与启动

开发或单机验证：

```bash
cargo run -p quant-pivot-bin -- --config-dir config
```

生产建议先构建 release binary，再由 systemd、Nomad、Kubernetes 或同等进程管理器托管：

```bash
cargo build --release -p quant-pivot-bin
./target/release/quant-pivot --config-dir /etc/quant-pivot
```

启动失败常见原因：

| 现象 | 可能原因 | 处理 |
|------|----------|------|
| deploy config validation failed | 缺 private key、funder、JWT secret、relayer config | 补齐环境变量或 TOML |
| authenticated CLOB client failed | private key 无效、wallet topology 不匹配、CLOB endpoint 不通 | 校验 signer/funder，检查网络和 CLOB auth |
| account provider unavailable | `funder` 缺失或 Data API/CLOB collateral 读取失败 | 修复账户配置，不能降级为模拟资金 |
| runtime-config rejected | schema version/字段错误或 unknown field | 通过 schema API 重新生成 patch |

### 6.2 登录与 header

所有 `/api/...` 受保护接口都需要：

1. `Authorization: Bearer <access_token>`
2. `Accept-Api-Version: v1`
3. 对 governed mutation 增加 `X-Acting-Role: <role-code>`

登录示例：

```bash
BASE=http://127.0.0.1:8080

TOKEN=$(
  curl -sS -X POST "$BASE/api/auth/login" \
    -H "Accept-Api-Version: v1" \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin"}' \
  | jq -r '.data.access_token'
)
```

首次登录后立即改默认用户/密码，不要继续使用 seed 口令。

### 6.3 健康检查

无需认证：

```bash
curl -sS "$BASE/health" | jq .
curl -sS "$BASE/ready" | jq .
curl -sS "$BASE/metrics" | head
```

需要认证：

```bash
curl -sS "$BASE/api/system/status" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq .

curl -sS "$BASE/api/system/health" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq .

curl -sS "$BASE/api/system/deploy-config" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq .

curl -sS "$BASE/api/quant/account/live" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq .
```

上线前必须看到：

- process running；
- Postgres/ClickHouse/Redis healthy；
- private key present；
- `quant_runtime_mode` 初始为 `report_only`；
- kill switch 为 `closed` 或明确的收紧状态；
- live account snapshot 成功；
- no pending/unresolvable reconciliation；
- market data WS 和 Gamma/Data API 正常。

### 6.4 冷启动：数据采集是否正常

首次部署时 Postgres / ClickHouse 为空，**数据摄取可以立即开始，但报告与训练有各自的前置条件**（见 §8.0）。本节说明要采集哪些数据、大致需要多久、以及如何用 API / 日志 / 指标确认采集正常。

#### 6.4.1 三层数据与用途

| 层 | 存储 | 采集内容 | 用途 | 大致可用时间 |
|----|------|----------|------|--------------|
| **L1 目录 + 实时盘口** | Postgres `market` / `event`；进程内 BookStore；CLOB WS | Gamma 全量/增量同步；订阅 token 的 L2 订单簿 | 市场列表、实时 book、报告选市（live PIT） | 启动后 **数分钟**（首次 Gamma full sync + WS shard 就绪） |
| **L2 历史盘口事实** | ClickHouse `book_snapshots`、`tick_events`、`book_l2_replay_hot`、`book_microstructure_*` | WS 增量写入；异步 fact writer 批量刷盘 | 离线训练集的 PIT 特征/标签、回测 | 持续 ingest **数小时** 起有可用窗口；训练窗口越长需要越久 |
| **L3 量化事实** | ClickHouse `quant_feature_event`、`quant_factor_event` 等 | 特征/因子/信号/报告流水线产出 | 研究分析、归因反馈、后续再训练 | 首份报告跑通后才有；冷启动阶段可忽略 |

**训练集 build** 主要消费 **L1 目录 + L2 历史盘口**（以及可选的 live attribution，需已有执行闭环）。  
**在线报告** 主要消费 **L1 实时盘口 + 已发布的 active model**（不读 ClickHouse 历史窗做 live scoring）。

#### 6.4.2 用 API 确认 L1（目录 + 实时 book）

**1. 系统生命周期 — 目录与 WS 是否就绪**

```bash
curl -sS "$BASE/api/system/status" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq '{
    catalog: .data.catalog,
    operational_phase: .data.operational_phase,
    market_data: .data.market_data,
    active_markets: .data.active_markets
  }'
```

期望（ ingest 正常时）：

| 字段 | 正常值 | 含义 |
|------|--------|------|
| `catalog.state` | `"ready"` | Gamma 首次 full sync 已完成 |
| `catalog.markets` | `> 0` | 已注册市场数 |
| `operational_phase.phase` | `"operational"` | 目录就绪且 WS 有新鲜 book 消息 |
| `market_data.ready` | `true` | 全局 CLOB WS 连通且消息未过期 |
| `market_data.ws_shards.disconnected` | `0` | 所有 WS shard 已连接 |
| `active_markets` | `> 0` | 当前活跃可检测市场数 |

若长期停留在 `catalog_warming` 或 `market_data_connecting`，检查 Gamma endpoint、CLOB WS URL、网络与进程日志（`quant_pivot_core::service::gamma`、`quant_pivot_api::ws::router`）。

**2. 市场列表 — Postgres 是否有 catalog**

```bash
curl -sS "$BASE/api/markets?page=1&size=5" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq '{total: .data.total, sample: [.data.items[] | {market_id, question, status}]}'
```

`total > 0` 表示 Gamma 持久化成功。若 API 有数据但日志曾出现 `failed to persist markets`，说明 upsert 曾失败（常见为 Postgres 枚举 cast 问题），需升级至含修复的二进制并观察下次 sync。

**3. 单市场盘口 — BookStore 是否有 L2**

```bash
MARKET_ID="<从 markets 列表取一个 market_id>"
curl -sS "$BASE/api/markets/$MARKET_ID/book" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq .
```

YES/NO 两侧应有 bid/ask 档位；若长期为空，检查该 market 是否已订阅（tier1 选市日志 `subscribed=N`）。

**4. 数据质量快照 — 实时 book 新鲜度**

```bash
curl -sS "$BASE/api/quant/data-quality" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq .
```

期望：`total_tokens > 0`；`fresh + acceptable` 占多数；`fact_lag_exceeded: false`。  
`worst_fact_lag_ms` 接近或超过 `max_fact_lag_ms` 表示 ClickHouse 写入滞后，会影响离线训练集的 PIT 精度。

**5. 子系统健康**

```bash
curl -sS "$BASE/api/system/health" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq '{overall_healthy: .data.overall_healthy, checks: .data.checks}'
```

Postgres、ClickHouse、Redis 探针应为 `healthy`。

#### 6.4.3 用日志确认采集（无需 DB 直连）

启动后日志中应出现类似条目（时间戳因环境而异）：

| 日志关键词 | 含义 |
|------------|------|
| `gamma full sync complete events=… registered=…` | Gamma 目录同步完成 |
| `CLOB websocket subscription ingest synced … subscribed=N` | WS 订阅与 tier 选市完成 |
| `WS shard spawned shard_id=…` | CLOB WS 分片就绪 |
| `ClickHouse schema ensured` | CH 表结构已就绪 |
| `Tick size changed asset_id=…` | 盘口增量正常（INFO，非错误） |

**可忽略的 WARN（若进程继续启动）**：

- `POST /auth/api-key … Could not create api key` — SDK 在 key 已存在时 create 失败后会 derive 成功。

**需要处理的 WARN/ERROR**：

- `failed to persist markets` — Postgres upsert 失败，目录不完整。
- `report generation failed … active_model_version_id is not configured` — 尚无已发布模型（冷启动预期；见 §8.0 关 schedule）。

#### 6.4.4 用 Prometheus 指标确认（可选）

从 `/metrics` 抓取（名称前缀 `quant_pivot_`）：

| 指标 | 正常趋势 |
|------|----------|
| `gamma_markets_total` | > 0，full sync 后稳定 |
| `gamma_last_sync_success` | 1（最近一次 sync 成功） |
| `fact_lag_worst_ms` | 低于 runtime-config `data_quality.max_fact_lag_ms` |
| `fact_writer_lag_seconds`（若暴露） | 无持续增大 |

#### 6.4.5 用 ClickHouse 确认 L2 历史（训练前）

直连 ClickHouse（替换连接参数）：

```sql
-- 最近是否有 book 快照写入
SELECT count() AS rows, max(ingestion_time) AS latest
FROM book_snapshots
WHERE ingestion_time > now() - INTERVAL 1 HOUR;

-- 按 token 看覆盖（抽样）
SELECT token_id, count() AS snaps, min(event_time) AS first_seen, max(event_time) AS last_seen
FROM book_snapshots
WHERE ingestion_time > now() - INTERVAL 24 HOUR
GROUP BY token_id
ORDER BY snaps DESC
LIMIT 10;
```

`rows > 0` 且 `latest` 接近当前时间，说明 L2 历史事实在积累。  
**训练集 plan 的 `planned_samples` 依赖这段历史**；窗口 `[window_start, window_end)` 内没有足够 PIT book 的样本会在 build 时被丢弃。

#### 6.4.6 训练集需要累积多久（数量级）

没有固定「日历天数」，取决于 **窗口长度、采样间隔、订阅市场数、标签 horizon** 和 **quality gate 阈值**。默认 runtime-config 参考值：

| 参数 | 默认值 | 影响 |
|------|--------|------|
| `model.prediction_horizon_secs` | `86400`（24h） | 训练标签需样本 `as_of` 之后 24h 内有 forward book 数据 |
| `quality_gate.min_sample_count` | `500` | publish 门禁：回测/数据集样本数 |
| `quality_gate.min_label_coverage` | `0.70` | 标签覆盖率 |
| `reports.schedules[default_interval].cadence` | 每 `300`s | 与训练无关；冷启动无模型时会 ERROR |

**实操估算**（默认 24h horizon、`sample_interval_secs=300`、tier1 ~1600 token）：

1. **L1 就绪**：启动后 ~5–15 分钟（Gamma + WS）。
2. **L2 可用于短窗 plan**：连续 ingest **≥ 几小时** 后可对最近 1–4 小时窗口做 plan，看 `planned_samples`。
3. **标签成熟**：窗口内每个样本的标签要求 `as_of + horizon` 之前的数据已 ingest。  
   因此 **`window_end` 应 ≤ `now - max(horizons_secs)`**（通常 ≤ now − 24h），否则大量 `labels_not_mature`。
4. **首次 publish**：在 label 成熟的前提下，往往还需要 **≥ 7–14 天** 连续 ingest + 足够跨市场样本，才能通过默认 `min_sample_count=500`；Quant 可在授权下临时调低 gate 做 bootstrap。

先用 **plan 干跑** 看数量，再决定 build（§8.2）：

```bash
curl -sS -X POST "$BASE/api/research/training-datasets/plan" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: quant" \
  -H "Content-Type: application/json" \
  -d '{
    "model_spec_id": "<已有 model spec UUID>",
    "runtime_config_version_id": "<active runtime config version UUID>",
    "window_start": "2026-06-25T00:00:00Z",
    "window_end": "2026-07-01T00:00:00Z",
    "sample_interval_secs": 300,
    "horizons_secs": [86400],
    "source_delay_secs": 10,
    "reason": "cold-start dry plan"
  }' | jq '{planned_samples: .data.planned_samples, training_dataset_id: .data.training_dataset_id}'
```

`planned_samples` 接近 0 → 继续 ingest 或扩大窗口 / 检查 ClickHouse。

Build（在 plan 满意后，复用相同 window 参数 + plan 返回的 `training_dataset_id`）：

```bash
curl -sS -X POST "$BASE/api/research/training-datasets/build" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: quant" \
  -H "Content-Type: application/json" \
  -d '{
    "training_dataset_id": "<plan 返回的 UUID>",
    "model_spec_id": "<model spec UUID>",
    "runtime_config_version_id": "<runtime config version UUID>",
    "window_start": "2026-06-25T00:00:00Z",
    "window_end": "2026-07-01T00:00:00Z",
    "sample_interval_secs": 300,
    "horizons_secs": [86400],
    "source_delay_secs": 10,
    "reason": "cold-start first dataset build"
  }' | jq '{status: .data.status, sample_count: .data.sample_count}'
```

Poll `GET /api/research/training-datasets/{id}` 直到终端态。

## 7. Runtime-config 操作

### 7.1 查看当前配置和 schema

```bash
curl -sS "$BASE/api/runtime-config" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq .

curl -sS "$BASE/api/runtime-config/schema" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq .
```

### 7.2 新建并激活版本

`config_patch` 是 dotted path 到 leaf value 的稀疏 patch；`config_json` 是完整 JSON。二者只能选一个。

```bash
VERSION_ID=$(
  curl -sS -X POST "$BASE/api/runtime-config/versions" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept-Api-Version: v1" \
    -H "X-Acting-Role: quant" \
    -H "Content-Type: application/json" \
    -d '{
      "reason": "set conservative report and portfolio envelope for initial production",
      "config_patch": {
        "reports.ad_hoc_report_enabled": true,
        "reports.max_top_n": 20,
        "portfolio.budget.total_budget_usd": "1000",
        "portfolio.budget.min_recommendation_usd": "5",
        "portfolio.budget.max_single_recommendation_usd": "50",
        "portfolio.constraints.max_market_exposure_usd": "50",
        "portfolio.constraints.max_event_exposure_usd": "100",
        "portfolio.sizing.kelly_fraction": "0.5",
        "execution.auto_execution.enabled": false,
        "execution.entry_order_policy.allow_market_orders": false,
        "execution.entry_order_policy.max_slippage_bps": 50
      }
    }' \
  | jq -r '.data.runtime_config_version_id'
)

curl -sS -X POST "$BASE/api/runtime-config/versions/$VERSION_ID/activate" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: quant" \
  -H "Content-Type: application/json" \
  -d '{"reason":"activate initial conservative runtime config"}' | jq .
```

金额字段使用 decimal string，不要用浮点数。激活失败时查看错误详情，修 patch 后重新创建版本，不要手工改数据库。

### 7.3 回滚

```bash
curl -sS "$BASE/api/runtime-config/versions?limit=20" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq .

curl -sS -X POST "$BASE/api/runtime-config/versions/$KNOWN_GOOD_VERSION_ID/rollback" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: quant" \
  -H "Content-Type: application/json" \
  -d '{"reason":"rollback after execution admission regression"}' | jq .
```

回滚也是 governed mutation，会写 operation log。

## 8. 生成与阅读报告

### 8.0 前置条件（冷启动必读）

**出报告 ≠ 只要进程跑起来。** 当前实现中，每次报告构建（定时或 ad-hoc）在选市之前会 **fail-closed** 检查：

1. **`model.active_model_version_id` 已配置** — 指向 registry 中 **`PublicationStatus::Published`** 的模型版本，且 artifact 可加载。
2. **数据 ingest 就绪** — `operational_phase` 为 `operational`（或仅收紧型 degrade 仍允许报告）；实时 book 满足 data-quality 阈值。
3. **账户可读** — 所有 mode 下 CLOB collateral + Data API positions 可用（ReportOnly 不是 dry-run）。

因此 **第一次运行、库里没有任何模型时，报告必然失败** — 这是设计行为，不是 ingest 坏了。  
默认 bootstrap runtime-config 会 **启用** `default_interval` 定时 schedule（每 300s），但 **`active_model_version_id = null`**，所以日志会出现：

`report generation failed … model.active_model_version_id is not configured`

**冷启动推荐做法**（详见 §8.2）：

1. 先确认 §6.4 数据采集正常。
2. 用 runtime-config **关闭** `default_interval` schedule，避免无意义 ERROR 刷屏。
3. 连续 ingest 直至 training-dataset **plan** 的 `planned_samples` 足够。
4. 走 **train → backtest → publish** 治理链（publish 会自动写入 `active_model_version_id`）。
5. 再开启 schedule 或手动触发 ad-hoc 报告。

**训练集与报告的关系**：

| 问题 | 答案 |
|------|------|
| 出报告是否必须有训练集？ | **不直接需要**；报告读的是 **已发布模型 artifact**，不是训练集 Parquet。 |
| 那模型从哪来？ | 标准路径是 **训练集 build → train → backtest → publish**。没有 publish 就没有 active model。 |
| 能否跳过训练手动指模型？ | 可以 patch `model.active_model_version_id` 到已有 **Published** 版本，但版本仍须先注册 artifact。 |

### 8.1 从冷启动到第一份报告（完整流程）

```mermaid
flowchart TD
    start[进程启动 ingest] --> verify[§6.4 验证 L1/L2]
    verify --> disable[关闭 default_interval schedule]
    disable --> ingest[连续 ingest 数小时至数天]
    ingest --> plan[training-datasets/plan]
    plan --> build[training-datasets/build]
    build --> train[models/train]
    train --> backtest[models/backtest]
    backtest --> publish[models/publish]
    publish --> enable[开启 schedule 或 ad-hoc]
    enable --> report[RecommendationReport 发布]
```

**Step 0 — 关闭默认定时报告（避免 ERROR 刷屏）**

```bash
VERSION_ID=$(
  curl -sS -X POST "$BASE/api/runtime-config/versions" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept-Api-Version: v1" \
    -H "X-Acting-Role: quant" \
    -H "Content-Type: application/json" \
    -d '{
      "reason": "cold start: disable scheduled reports until model published",
      "config_patch": {
        "reports.schedules": [{
          "schedule_id": "default_interval",
          "cadence": {"interval_secs": 300},
          "top_n": 20,
          "source_delay_secs": 10,
          "enabled": false
        }]
      }
    }' | jq -r '.data.runtime_config_version_id'
)

curl -sS -X POST "$BASE/api/runtime-config/versions/$VERSION_ID/activate" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: quant" \
  -H "Content-Type: application/json" \
  -d '{"reason":"activate cold-start schedule disable"}' | jq .
```

> **注意**：`reports.schedules` 是 **整数组替换**，patch 时必须带上完整 schedule 对象，不能只改 `enabled` 字段。

**Step 1 — Plan / Build 训练集**

前提：

- `quant_model_spec` 中已有目标 `model_spec_id`（Research Workbench 或 DBA 预置）；
- `runtime_config_version_id` 用当前 active 版本：

```bash
curl -sS "$BASE/api/runtime-config" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq '.data.version.runtime_config_version_id'
```

1. **Plan**（不写 ledger，只看 `planned_samples`）— 见 §6.4.6 示例。
2. **Build**（同 plan body，加上 plan 返回的 `training_dataset_id`）— 见 §6.4.6 build 示例。
3. **Poll** `GET /api/research/training-datasets/{id}` 直到 `status` 为终端态。Trainer 只吃 **`ready`** 状态。

**Step 2 — Train → Backtest → Publish**

```bash
# Train（返回 model_version_id，状态 candidate）
curl -sS -X POST "$BASE/api/research/models/train" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: quant" \
  -H "Content-Type: application/json" \
  -d '{
    "model_spec_id": "<model spec UUID>",
    "training_dataset_id": "<ready dataset UUID>",
    "runtime_config_version_id": "<runtime config version UUID>",
    "model_family": "weighted_factor",
    "label_name": "return_to_horizon",
    "label_horizon_secs": 86400,
    "reason": "cold-start first model"
  }' | jq '{model_version_id: .data.model_version_id, publication_status: .data.publication_status}'

# Backtest
curl -sS -X POST "$BASE/api/research/models/<model_version_id>/backtest" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: quant" \
  -H "Content-Type: application/json" \
  -d '{
    "training_dataset_id": "<holdout or same dataset UUID>",
    "runtime_config_version_id": "<runtime config version UUID>",
    "calibrate": true,
    "reason": "cold-start backtest before publish"
  }' | jq .

# Publish（quality gate 通过后自动 sync model.active_model_version_id）
curl -sS -X POST "$BASE/api/research/models/<model_version_id>/publish" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: quant" \
  -H "Content-Type: application/json" \
  -d '{"reason":"promote first production model"}' | jq .
```

确认指针已写入：

```bash
curl -sS "$BASE/api/runtime-config" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq '.data.config.model.active_model_version_id'
```

**Step 3 — 开启报告**

- 重新 patch `reports.schedules[].enabled=true`，或
- 启用 ad-hoc 并手动触发（§8.4）。

### 8.2 定时报告 vs Ad-hoc 报告

| 维度 | 定时报告（Scheduled） | Ad-hoc 报告 |
|------|----------------------|-------------|
| 触发 | runtime-config `reports.schedules[]` + cron/interval worker | 人工 `POST /api/quant/reports/run` 或 UI「立即生成」 |
| 默认 | bootstrap **enabled**（`default_interval`，300s） | bootstrap **disabled**（`ad_hoc_report_enabled=false`） |
| `top_n` | 取自 schedule 配置 | **请求体必填**（无配置回退） |
| `source_delay_secs` | 取自 schedule 配置 | **请求体必填** |
| 幂等键 | `schedule_id` + `trigger_time` 派生 | 请求体 `request_id`（客户端生成） |
| HTTP | 无直接 HTTP（后台 scheduler） | `POST` 返回 **202 Accepted**（异步入队） |
| 典型用途 | 生产周期性 Top-N | 事故恢复后验证、semi_auto 审批前刷新、策略变更后手动快照 |

两者走 **同一套** `ReportLifecycleService::run` 流水线；差异仅在触发源、参数来源和治理开关。

### 8.3 定时报告

默认 runtime-config 包含一个 interval schedule（`schedule_id=default_interval`，`interval_secs=300`，`top_n=20`，`source_delay_secs=10`）。

报告生成流程：

1. 读取 trigger-time 的 point-in-time runtime-config；
2. 计算 `as_of = trigger_time - source_delay_secs`；
3. **`active_requirements`** — 加载 Published active model；
4. selection 选出候选市场；
5. account provider 读取真实 venue 账户；
6. feature / factor / model 输出信号；
7. portfolio planner 做 sizing 和约束优化；
8. composer 生成 Top-N `RecommendationReport`；
9. 持久化 + WebSocket `quant.report` 事件。

Schedule 被 **disabled** 时，worker 不会触发；若误配为 enabled 且无 active model，每 tick ERROR（§8.0）。

### 8.4 Ad-hoc 报告（详细）

**是什么**：Ad-hoc（「按需 / 手动」）报告是一次 **显式触发** 的报告构建，不等待定时 schedule。  
与定时报告产出相同类型的 `RecommendationReport`（Top-N 推荐 + sizing + exit plan），但：

- 由 operator / quant **主动发起**（API 或 Admin UI）；
- **必须**在请求中指定 `top_n` 和 `source_delay_secs`（代码 fail-closed，无默认值）；
- 受 `reports.ad_hoc_report_enabled` 治理（默认 `false`）；
- **异步执行**：HTTP 只负责入队，不阻塞到报告写完。

**何时使用**：

- 冷启动完成 publish 后，**第一次验证**报告流水线；
- 数据质量事故恢复后（§16.1），确认新 report 正常再恢复交易；
- `semi_auto` 审批窗口前需要 **最新** Top-N（runbook §11 场景）；
- 策略/runtime-config 变更后，不想等到下一个 300s tick。

**启用 ad-hoc**（一次性 patch）：

```bash
curl -sS -X POST "$BASE/api/runtime-config/versions" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: quant" \
  -H "Content-Type: application/json" \
  -d '{
    "reason": "enable manual report runs for operators",
    "config_patch": {
      "reports.ad_hoc_report_enabled": true
    }
  }' | jq -r '.data.runtime_config_version_id'
# 然后 activate（同 §7.2）
```

**触发 ad-hoc**（`quant_report:enqueue` 权限；`X-Acting-Role: quant` 或 operator 角色）：

```bash
curl -sS -X POST "$BASE/api/quant/reports/run" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: quant" \
  -H "Content-Type: application/json" \
  -d '{
    "request_id": "manual-20260702-001",
    "reason": "first report after model publish",
    "top_n": 20,
    "source_delay_secs": 10
  }' | jq .
```

**响应语义**：

| HTTP | Body | 含义 |
|------|------|------|
| **202 Accepted** | `{ request_id, trigger_key }` | 已入队；**尚无** `recommendation_report_id` |
| **409 Conflict** | `ad-hoc report generation is disabled` | 未开启 `ad_hoc_report_enabled` |
| **4xx** | validation / auth | 缺 `top_n`/`source_delay_secs`、权限不足等 |

**跟踪完成**（三选一，推荐 1+2）：

1. **WebSocket** — 订阅 `quant.report` 频道，事件序列：`started` → `published` / `empty` / `failed`。
2. **轮询列表** — `GET /api/quant/reports?limit=5` 按 `created_at` 查看新行；或 `GET /api/quant/reports/latest`（仅 **published** Top-N）。
3. **Metrics** — `quant_report_runs_total`、`quant_reports_published_total`、`quant_reports_empty_total`。

**幂等**：相同 `request_id` 重复 POST 不会重复构建（回归测试 `ad_hoc_idempotent_on_trigger_key`）。客户端应使用全局唯一 `request_id`（如 `manual-<date>-<seq>`）。

**与 `publish_empty_reports` 的关系**：

- 默认 `publish_empty_reports=true`：模型跑完但无 positive signal 时仍 **持久化** empty report（带 `empty_reason`）。
- `publish_empty_reports=false`：empty 结果 **跳过 Postgres**，仅 ephemeral WS + metric。
- 无论哪种，**没有 active model 时在更早阶段失败**，不会产生 empty report。

**Ad-hoc 仍失败时的常见原因**（与定时报告相同）：

| 错误 / empty_reason | 处理 |
|---------------------|------|
| `active_model_version_id is not configured` | 完成 §8.1 publish 流程 |
| `active model … must be published` | 指向 Candidate 版本；需 publish |
| `insufficient data quality` | §6.4 数据质量 / WS |
| `no positive signal` | 正常空报告，非 ingest 故障 |

### 8.5 如何读一条 Recommendation

每条推荐至少要看这些块：

| 块 | 关键字段 | 操作意义 |
|----|----------|----------|
| Identity | report id、rank、market id、token id、outcome side、runtime mode | 买什么，来自哪份报告 |
| Signal | score、confidence、expected return、model version、factor breakdown | 为什么买，信号是否足够强 |
| Entry plan | trigger kind、limit price、max slippage、valid window、min depth、max book age | 什么时候买、以什么价格买 |
| Sizing plan | suggested USD、shares、Kelly cap、budget cap、binding constraints | 买多少，为什么不能更多 |
| Exit plan | take profit、stop loss、time exit、signal invalidation、hold-to-resolution / redeem policy | 什么时候卖，卖多少 |
| Risk envelope | market/event/category/correlation exposure、liquidity usage、downside bps | 这笔单的风险边界 |
| Evidence | feature snapshot、book age、data quality、model/factor refs | 审计依据 |
| Execution eligibility | eligible modes、auto ineligibility reasons、approval required | 能否在当前 mode 执行 |

如果报告为空，先看 empty reason：

| Empty reason | 常见原因 | 处理 |
|--------------|----------|------|
| system degraded | infra / data pipeline unhealthy | 修健康项，不要下单 |
| empty selection | selection 条件过严或市场池为空 | 检查 Gamma sync、selection config |
| insufficient data quality | book age、coverage、fact lag 不满足阈值 | 等数据恢复或收紧运营 |
| no positive signal | 模型没有正期望候选 | 不交易 |
| budget exhausted | 资金、exposure、capital allocation 不足 | 充值、平仓、降低 open intents，或调整 budget caps |

## 9. 买什么、什么时候买、买多少、依据什么

### 9.1 买什么

只考虑最新、已发布、未撤销、未过有效期报告中的推荐。人工或系统都不应该根据旧截图、聊天记录、未发布 report 或 research notebook 下单。

买入对象由推荐确定：

- `market_id`：Polymarket market；
- `token_id`：条件 token；
- `outcome_side`：YES/NO 或具体 outcome；
- `rank`：Top-N 排序；
- `recommendation_id`：创建 intent 的唯一输入。

### 9.2 什么时候买

以 `entry_plan` 为准：

1. 当前时间必须在 `entry_plan.valid_from` 到 `entry_plan.valid_until` 之间；
2. order book age 必须不超过 `max_book_age_ms`；
3. 可成交深度必须达到 `min_depth_usd`；
4. 当前价格不能突破 limit cap / slippage cap；
5. recommendation、report、runtime-config、model、data-quality 在提交前都不能失效；
6. kill switch 必须允许 new entry；
7. admission 必须返回 `allow`。

默认策略是限价单：`allow_market_orders=false` 时，entry plan 使用 `limit_price`，并带 `cancel_if_not_triggered=true`。只有 runtime-config 明确打开 `execution.entry_order_policy.allow_market_orders=true` 时，才允许 immediate entry，但仍必须带 limit cap。

### 9.3 买多少

以 `sizing_plan.suggested_usd` 和 `sizing_plan.shares` 为上限。计算链路：

```mermaid
flowchart LR
    A["venue collateral + positions"] --> B["capital_base = min(venue NLV, runtime budget)"]
    B --> C["available cash after reservations"]
    C --> D["Kelly sizing with confidence and drawdown scaling"]
    D --> E["per-rec max, market/event/category exposure caps"]
    E --> F["liquidity usage and slippage caps"]
    F --> G["correlation cap and optimizer"]
    G --> H["suggested_usd / shares"]
```

人工审批时只能：

- 使用 `suggested_usd` 原值；
- 降低 shares；
- 降低 limit price；
- 设置更低 `max_allowed_usd`；
- 拒绝交易。

不能因为主观看好而放大仓位。若要改变 sizing 逻辑，必须新建 runtime-config 版本并重新生成报告。

### 9.4 依据什么

每笔买入至少要能回答：

1. **数据依据**：Gamma market metadata、CLOB L2 book、Data API positions、ClickHouse facts 是否新鲜；
2. **信号依据**：factor breakdown、model score、confidence、expected return、downside；
3. **组合依据**：Kelly cap、budget cap、exposure cap、correlation cap、liquidity cap；
4. **治理依据**：runtime mode、kill switch、admission checks、operation log；
5. **执行依据**：entry plan、order type、limit price、valid window。

如果任一依据不可查，拒绝或延后交易。

## 10. 在 `report_only` 下人工下单

`report_only` 下系统不会创建 `OrderIntent`，也不会签名或提交订单。人工可以把 report 当成交易建议，在 Polymarket UI 或自有工具中手动下单，但必须接受这些后果：

- 系统能通过 Data API 在后续 account snapshot 中看到 position；
- 该交易没有系统内 `OrderIntent` 和 `ExecutionOrder` 审计链；
- attribution、reconciliation、capital allocation 可能不完整；
- 后续 exit monitor 不一定能按系统策略自动管理这笔外部仓位。

人工下单 SOP：

1. 读取最新报告和对应 recommendation；
2. 只在 entry window 内操作；
3. 用 recommendation 给出的 token/outcome；
4. 使用 limit price，不要高于 report 的 cap；
5. notional 不超过 `suggested_usd`；
6. 下单后记录 operator note、venue order id、tx/trade id；
7. 重新调用 `GET /api/quant/account/live` 确认 positions；
8. 如果希望系统后续可审计，下一次应切 `semi_auto` 走 intent 链路。

生产资金建议优先使用 `semi_auto`。

## 11. `semi_auto` 下单 SOP

### 11.1 切换到 `semi_auto`

```bash
curl -sS -X POST "$BASE/api/system/quant-mode" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: operator" \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "semi_auto",
    "reason": "enable governed order intents after report-only readiness checks"
  }' | jq .
```

升级会跑 preflight。失败时按返回 check 修复，不要绕过。

### 11.2 创建 intent

```bash
curl -sS -X POST "$BASE/api/quant/intents" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: trader" \
  -H "X-Request-Id: intent-20260701-001" \
  -H "Content-Type: application/json" \
  -d '{
    "recommendation_id": "00000000-0000-0000-0000-000000000000",
    "reason": "rank 1 report recommendation within entry window"
  }' | jq .
```

`semi_auto` 下返回状态应是 `pending_approval`。

### 11.3 审批或拒绝

审批并收紧订单：

```bash
curl -sS -X POST "$BASE/api/quant/intents/$INTENT_ID/approve" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: approver" \
  -H "X-Request-Id: approve-20260701-001" \
  -H "Content-Type: application/json" \
  -d '{
    "reason": "book fresh and depth sufficient; approve with smaller notional",
    "max_allowed_usd": "25",
    "override_note": "reduced because market spread widened near approval"
  }' | jq .
```

拒绝：

```bash
curl -sS -X POST "$BASE/api/quant/intents/$INTENT_ID/reject" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: approver" \
  -H "X-Request-Id: reject-20260701-001" \
  -H "Content-Type: application/json" \
  -d '{"reason":"recommendation expired before approval"}' | jq .
```

### 11.4 提交到 CLOB

这是实盘路径，会签名并提交订单。

```bash
curl -sS -X POST "$BASE/api/quant/intents/$INTENT_ID/submit" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: trader" \
  -H "X-Request-Id: submit-20260701-001" \
  -H "Content-Type: application/json" \
  -d '{"reason":"approved intent still passes admission"}' | jq .
```

结果解释：

| HTTP / state | 含义 | 行动 |
|--------------|------|------|
| 200 + `filled` / `partially_filled` | venue 已确认成交或部分成交 | 查 position 和 execution order |
| 200 + `ambiguous` | venue 响应不确定，capital held | 等 reconciliation，不要重复提交 |
| 409 | admission deny 或状态不可提交 | 读 admission trace，修根因或放弃 |
| 503 | transient defer | 等待数据/venue 恢复后重试 |

提交后复核：

```bash
curl -sS "$BASE/api/quant/intents/$INTENT_ID" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq .

curl -sS "$BASE/api/quant/execution-orders?order_intent_id=$INTENT_ID" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq .

curl -sS "$BASE/api/quant/positions?order_intent_id=$INTENT_ID" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq .
```

## 12. `auto_execution` SOP

`auto_execution` 只适合在 `semi_auto` 稳定运行后启用。它不是跳过风控：策略可自动批准 intent，但提交前仍跑 admission、kill switch、capital、data quality、book、venue、credential、exit monitor 等检查。

### 12.1 升级前条件

必须全部满足：

- JWT secret 已换成强 secret；
- private key、funder、wallet topology、relayer 配置通过 preflight；
- active runtime-config schema v9 有效；
- `execution.auto_execution.enabled=true`；
- `execution.auto_execution.max_orders_per_report`、`max_total_usd_per_report`、`min_score`、`min_confidence` 保守；
- `portfolio.budget.total_budget_usd > 0` 且 account live snapshot 可用；
- 已有 published model，且 shadow period / quality gate 通过；
- data quality healthy；
- no pending/unresolvable reconciliation；
- no impaired capital allocation；
- kill switch 为 `closed`；
- exit monitor healthy；
- 近若干个 `semi_auto` 订单 attribution 和 reconciliation 正常。

### 12.2 启用策略批准

先通过 runtime-config 打开 auto policy，建议使用极小上限：

```bash
curl -sS -X POST "$BASE/api/runtime-config/versions" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: quant" \
  -H "Content-Type: application/json" \
  -d '{
    "reason": "enable conservative auto execution policy",
    "config_patch": {
      "execution.auto_execution.enabled": true,
      "execution.auto_execution.max_orders_per_report": 1,
      "execution.auto_execution.max_total_usd_per_report": "20",
      "execution.auto_execution.min_score": "0.75",
      "execution.auto_execution.min_confidence": "0.70"
    }
  }' | jq .
```

激活后再切 mode：

```bash
curl -sS -X POST "$BASE/api/system/quant-mode" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: operator" \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "auto_execution",
    "reason": "all auto-execution preflight checks passed; conservative caps enabled"
  }' | jq .
```

### 12.3 Auto 日常监控

每个 report 周期检查：

- 最新 report 是否 published；
- auto ineligibility reasons 是否为空；
- 新 intent 数量不超过 `max_orders_per_report`；
- 单 report 总 notional 不超过 `max_total_usd_per_report`；
- execution order 没有异常积压；
- reconciliation 没有 pending 过久或 unresolvable；
- daily loss cap / breaker 未触发；
- exit monitor 正常处理 open positions。

任何异常先切 `exit_only` 或 `report_only_forced`，再排查。

## 13. 什么时候卖、卖多少、依据什么

### 13.1 卖出信息源

卖出依据来自：

1. recommendation 的 `exit_plan`；
2. position / lot 的当前状态；
3. exit monitor 的 signal recheck；
4. kill switch 和 emergency policy；
5. market resolution / settlement redeem 状态；
6. operator incident decision。

不要凭旧 entry 逻辑手动猜 exit。每次卖出都必须关联 position、trigger 和 reason。

### 13.2 Exit trigger 优先级

| 优先级 | Trigger | 典型动作 | 卖多少 |
|--------|---------|----------|--------|
| 1 | `emergency_halted` / breaker | 进入事故处置，按 emergency policy 或人工减仓 | 通常全部风险仓位，除非 operator 明确分批 |
| 2 | `stop_loss` | 价格或风险突破止损阈值 | 默认全部该 lot；如系统支持部分节点，按节点配置 |
| 3 | `signal_invalidation` | 重新推理后信号弱化或反转 | 默认全部该 lot，或按 opportunistic sell 目标 |
| 4 | `time_exit` | 到达推荐 horizon / valid horizon | 默认全部未退出 shares |
| 5 | `take_profit` | 达到 take-profit price | 默认全部该 lot；部分止盈仅在 exit plan 明确给出时允许 |
| 6 | hold-to-resolution / redeem | 接近 resolution 且策略选择持有到期 | 不卖，等待 resolve 后 redeem |

当前 composer 生成的基础 exit plan 是：

- `take_profit_price = entry_price * (1 + target_reward_multiple * downside)`，并裁剪到合法价格区间；
- `stop_loss_price = entry_price * (1 - downside)`；
- `time_exit_at = as_of + effective_horizon`；
- 如果开启 hold-to-resolution 且接近 resolution，则取消 take-profit/time-exit，保留 stop-loss，并使用 auto/manual redeem policy。

### 13.3 手动卖出 SOP

手动卖出只能减少风险：

1. 查询 position：

   ```bash
   curl -sS "$BASE/api/quant/positions?state=open" \
     -H "Authorization: Bearer $TOKEN" \
     -H "Accept-Api-Version: v1" | jq .
   ```

2. 找到原 recommendation / intent / execution order；
3. 读取 exit plan 和当前 book；
4. 如果正常策略退出，使用 limit order，价格不低于 exit plan 或 operator 事故阈值；
5. shares 不超过 open shares；
6. 下单后记录 venue order id 和 reason；
7. 等待 account snapshot / reconciliation 反映 position 变化；
8. 如系统不能自动归因，手工标注事故记录和账务差异。

### 13.4 系统自动卖出与赎回

`execution.exit_monitor.enabled=true` 时，系统会定期 recheck signal 和 exit 条件。kill switch 为 `closed`、`report_only_forced`、`exit_only` 时允许普通 auto exit；`execution_halted` 和 `emergency_halted` 不走普通自动退出。

市场 resolved 后：

- winning tokens 可按 1 token = 1 pUSD 赎回；
- losing tokens 价值为 0；
- Polymarket 文档表示没有赎回 deadline；
- redeem 会 burn 整个 condition balance，不是指定部分 amount；
- 当前 `settlement_redeem` policy 可自动批量 redeem；失败或不支持 topology 时进入 manual required。

## 14. Reconciliation 与账务闭环

订单提交后，系统按以下证据顺序收敛 truth：

1. CLOB order status；
2. CLOB trades；
3. token balance delta；
4. collateral delta；
5. Data API positions；
6. on-chain transaction receipt。

`ambiguous` 或 `pending` 时不要重复提交同一 intent。capital 会 held，直到 reconciliation 给出 `filled`、`not_filled`、`partially_filled`、`cancelled` 或 `unresolvable`。

查看：

```bash
curl -sS "$BASE/api/quant/reconciliations" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" | jq .
```

人工 resolve 只能在已经查明 venue truth 后执行：

```bash
curl -sS -X POST "$BASE/api/quant/reconciliations/$RECON_ID/resolve" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Accept-Api-Version: v1" \
  -H "X-Acting-Role: operator" \
  -H "Content-Type: application/json" \
  -d '{
    "result": "not_filled",
    "reason": "venue order not found after CLOB status, trades, balance and Data API checks"
  }' | jq .
```

如果存在 `unresolvable`，不要升级到 `auto_execution`。

## 15. 日常操作清单

### 15.1 开盘 / 开始交易前

- `GET /ready` 成功；
- `GET /api/system/health` 无 degraded；
- `GET /api/system/quant-mode` 是预期 mode；
- `GET /api/system/kill-switch` 是预期 state；
- `GET /api/quant/account/live` 账户可读，capital base 合理；
- no stale report schedule；
- no pending/unresolvable reconciliation；
- no impaired capital；
- data quality healthy；
- latest model/factor publication 是预期版本；
- runtime-config active version 是预期版本；
- allowance 足够；
- Polymarket status / RPC / bridge 无已知事故。

### 15.2 每次下单前

- 使用最新 published report；
- recommendation 未过期；
- entry window 内；
- book fresh；
- spread/slippage/depth 满足 entry plan；
- notional <= suggested；
- exposure caps 未触发；
- kill switch 允许 new entry；
- mode 和 approval chain 正确；
- admission 返回 `allow`。

### 15.3 每次下单后

- intent state 进入 submitted/filled/partially_filled 或明确失败；
- execution order 有 venue status；
- position ledger 更新；
- capital allocation 从 reserved 进入 spent/released；
- reconciliation 没有长时间 pending；
- account live snapshot 与 expected delta 一致；
- exit monitor 正在监控 open lot。

### 15.4 日终 / 停止交易

- 切 `report_only_forced` 或 `report_only`；
- 处理所有 pending approval intent；
- 检查 submitted/ambiguous orders；
- 检查 open positions 和 exit state；
- 导出 latest report、orders、positions、attribution；
- 检查 settlement redeem queue；
- 记录 PnL、drawdown、异常和下一交易日预算。

## 16. 事故处理

### 16.1 数据延迟或质量下降

症状：

- report empty: insufficient data quality；
- book age 超阈值；
- Gamma full sync lag；
- ClickHouse fact lag；
- WS reconnect 频繁。

处理：

1. 设置 `report_only_forced` 或 `execution_halted`；
2. 查看 `/api/system/health` 和 data quality snapshot；
3. 检查 CLOB WS、Gamma、Data API、ClickHouse 写入；
4. 恢复后重新跑 ad-hoc report；
5. 只有新 report 正常后才恢复交易。

### 16.2 CLOB 提交失败

常见原因：

- allowance 不足；
- order price/tick size 不合法；
- order book 变动导致 slippage 超限；
- credentials/wallet_kind/funder 不匹配；
- venue 暂时不可用。

处理：

1. 不重复 submit 同一 intent，先查 execution order；
2. 查 admission trace 和 venue response；
3. 如 ambiguous，等待 reconciliation；
4. 如 allowance 问题，按官方流程补 approval；
5. 需要重试时确认 intent 仍 submittable 且 recommendation 未过期。

### 16.3 Reconciliation unresolvable

处理：

1. 切 `report_only_forced` 或 `execution_halted`；
2. 收集 CLOB status、trades、balance、Data API、on-chain receipt；
3. 人工判定 truth；
4. 调 resolve API；
5. 确认 capital allocation 和 position ledger 修正；
6. 复盘后再恢复。

### 16.4 资金或提现异常

处理：

1. 冻结新增开仓；
2. 对账 Polymarket UI、CLOB collateral、Data API positions、链上 tx；
3. 不调整 runtime budget 来掩盖实际资金缺口；
4. bridge 卡住时按 Polymarket bridge status 和官方支持流程处理；
5. 资金恢复前不要启用 `auto_execution`。

### 16.5 模型或策略异常

处理：

1. revoke 异常 report；
2. 回滚 runtime-config；
3. rollback/retire model 或 factor publication；
4. 重新跑 backtest / shadow report；
5. 用小 budget 在 `semi_auto` 验证后再恢复 auto。

## 17. 常用 API 速查

| 操作 | Method / Path |
|------|---------------|
| 登录 | `POST /api/auth/login` |
| 当前用户 | `GET /api/auth/me` |
| 系统状态 | `GET /api/system/status` |
| 系统健康 | `GET /api/system/health` |
| 当前 mode | `GET /api/system/quant-mode` |
| 切 mode | `POST /api/system/quant-mode` |
| kill switch | `GET/POST /api/system/kill-switch` |
| masked deploy config | `GET /api/system/deploy-config` |
| 当前 runtime-config | `GET /api/runtime-config` |
| runtime schema | `GET /api/runtime-config/schema` |
| 新建 runtime-config 版本 | `POST /api/runtime-config/versions` |
| 激活版本 | `POST /api/runtime-config/versions/{id}/activate` |
| 回滚版本 | `POST /api/runtime-config/versions/{id}/rollback` |
| live account | `GET /api/quant/account/live` |
| 最新报告 | `GET /api/quant/reports/latest` |
| ad-hoc report | `POST /api/quant/reports/run` |
| report recommendations | `GET /api/quant/reports/{id}/recommendations` |
| recommendation evidence | `GET /api/quant/recommendations/{id}/evidence` |
| 创建 intent | `POST /api/quant/intents` |
| 审批 intent | `POST /api/quant/intents/{id}/approve` |
| 拒绝 intent | `POST /api/quant/intents/{id}/reject` |
| 取消 intent | `POST /api/quant/intents/{id}/cancel` |
| 提交 intent | `POST /api/quant/intents/{id}/submit` |
| execution orders | `GET /api/quant/execution-orders` |
| positions | `GET /api/quant/positions` |
| reconciliations | `GET /api/quant/reconciliations` |
| settlement redeems | `GET /api/quant/settlement-redeems` |

## 18. Done criteria

一次生产操作完成，需要满足：

1. operation log 有 actor、role、reason；
2. 相关 report / recommendation / intent / order / position id 可追踪；
3. account snapshot 与预期资金变化一致；
4. no unexpected pending reconciliation；
5. risk budget 和 runtime mode 处于预期状态；
6. 事故或人工操作已记录 reason 和外部 evidence。
