# oxide-arb 运行与验证手册

> **目标读者**：准备把后端跑起来、验证「有没有机会 / 能不能成交 / 能赚多少」、并让 `oxide-arb-control` 沉淀可发布的 control factors 的运维与量化同学。
>
> **相关文档**：[bankroll-and-risk-metrics.md](./bankroll-and-risk-metrics.md)（`bankroll_usd` 设计）、[schema-catalog.md](../persistence/schema-catalog.md)（表结构）、[replay-analytics-endgame-audit.md](../replay-analytics-endgame-audit.md)（控制面动机）。

---

## 目录

1. [总览与阶段路线](#1-总览与阶段路线)
2. [基础设施准备](#2-基础设施准备)
3. [Bot 专用钱包、凭证与 Polymarket 配置](#3-bot-专用钱包凭证与-polymarket-配置)
4. [配置：Deploy vs Runtime](#4-配置deploy-vs-runtime)
5. [编译、启动与首次验收](#5-编译启动与首次验收)
6. [三种执行模式详解](#6-三种执行模式详解)
7. [分阶段验证：机会 → 成交 → PnL](#7-分阶段验证机会--成交--pnl)
8. [观测面速查（在哪看、拿什么结果）](#8-观测面速查在哪看拿什么结果)
9. [Control Factor 沉淀与发布](#9-control-factor-沉淀与发布)
10. [因子与 Live 热路径接线](#10-因子与-live-热路径接线)
11. [Prometheus / SQL 验收清单](#11-prometheus--sql-验收清单)
12. [常见问题与排错](#12-常见问题与排错)

---

## 1. 总览与阶段路线

### 1.1 系统做什么

```text
Gamma 目录 + CLOB WebSocket 订单簿
  → BookStore → Scanner / Funnel → 算法（endgame、calibration、scoring）
  → ScoredOpportunity → 风控 → ExecutionPipeline → CLOB 订单（Live）
  → 成交 / 结算 / 对账 → Postgres + ClickHouse 证据链
  → oxide-arb-control 离线物料化 → Draft 因子 → Shadow → Published → 反哺热路径
```

### 1.2 推荐阶段

| 阶段 | 模式 | 时间 | 目的 |
|------|------|------|------|
| **0** | DryRun | 1～3 天 | 验证数据管道、检测漏斗、edge 分布（**乐观上界**） |
| **1** | Paper | 1～2 周 | 验证深度与 Miss 率（**更接近真实成交**） |
| **2** | Paper 持续 + 等结算 | 2～6 周 | 积累 settlements，让 BucketRisk 等因子过 quality gate |
| **3** | 小资金 Live | 1 周+ | 真实 FOK、slippage、结算 PnL |
| **4** | Shadow → Published 因子 | 并行 | 治理化反哺 live 决策 |

### 1.3 关键结论（先读）

- **因子沉淀不强制 Live**；`oxide-arb-control` 不检查 `ExecutionMode`。
- **权威因子**需要：足够样本 + PIT 完整 + quality gate 通过 + 人工 **Shadow → Publish**。
- **DryRun PnL 不可当真**；Paper 更可信；Live + 结算周期才是 ground truth。
- **`bankroll_usd` 不是钱包余额** → 见 [bankroll-and-risk-metrics.md](./bankroll-and-risk-metrics.md)。

---

## 2. 基础设施准备

### 2.1 硬依赖（缺一即启动失败）

| 服务 | 用途 | 默认连接 |
|------|------|----------|
| **PostgreSQL** | 交易、持仓、治理、RBAC、runtime config、control factor | `localhost:5432` / 库 `oxide_arb` |
| **ClickHouse** | 检测/审计/订单簿时序证据 | `http://localhost:8123` / 库 `oxide_arb` |
| **Redis** | L2 缓存 + JWT 黑名单 | `localhost:6379` |

启动时自动：PG migration、CH `ensure_schema()`、RBAC seed（admin 用户）。

### 2.2 本地 Docker 示例（仓库暂无 compose，可自行运行）

```bash
docker run -d --name oxide-pg \
  -e POSTGRES_USER=oxide -e POSTGRES_PASSWORD=你的密码 \
  -e POSTGRES_DB=oxide_arb -p 5432:5432 postgres:16

docker run -d --name oxide-ch \
  -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server

docker run -d --name oxide-redis -p 6379:6379 redis:7
```

### 2.3 建议硬件（单节点）

| 资源 | 建议 |
|------|------|
| CPU | 4 核+ |
| 内存 | 8 GB+（prod 示例 Moka 50k 条目） |
| 磁盘 | SSD 50 GB+（CH 增长快） |
| 网络 | 稳定低延迟到 Polymarket |

### 2.4 Deploy 配置文件

- 开发：`config/oxide-arb.toml`
- 生产模板：`config/oxide-arb.production.example.toml`
- 覆盖：`OXIDE_ARB__*` 环境变量（双下划线分隔路径）

**切勿**在 TOML 里写 `[detection]`、`[risk]` 等旧段 — 启动会 fatal。

---

## 3. Bot 专用钱包、凭证与 Polymarket 配置

本章回答：

1. 怎么在 Rabby 里**单独建 bot 子账户**、只充策略 USDC？
2. 怎么在 **oxide-arb 项目里配置**（不写钱包地址，只配 env + runtime）？
3. bot **私钥从哪里拿**？L2 凭证要不要单独配？（§3.1.4 + §3.2.1）

---

### 3.1 推荐：为 bot 单独建 Rabby 子账户

**不要用日常主账户跑 bot。** 单独子账户的好处：

- 私钥泄露时损失有上限（只充策略资金）
- 链上 / CLOB 余额与 bot 策略一一对应，对账清晰
- 与 Polymarket 网站 Proxy 地址解耦（bot 走 **EOA + API** 路径）

#### 3.1.1 在 Rabby 里新建子账户（逐步）

1. 打开浏览器 **Rabby 扩展**，解锁钱包。
2. 点击顶部的**当前地址**（或账户名），打开账户列表。
3. 点击 **「+ Add Address」/「添加地址」**。
4. 选择 **「Create new address」/「创建新地址」**（从当前助记词派生新账户；推荐再设一个独立密码库备份助记词）。
   - 若希望 bot 与主账户**助记词也完全隔离**：用另一浏览器配置文件新建 Rabby，走 **Create a new seed phrase**，专门给 bot 用。
5. 给账户起名，例如 **`oxide-arb-bot`**，便于辨认。
6. 创建完成后，复制显示的 **新地址**（形如 `0x…`，**不是**你主账户的 `0xC795…`，除非你就打算用主账户——不推荐）。
7. **记录地址到密码管理器**（地址可公开，但建议与 bot 实例对应关系写清楚）。

> Rabby 界面文案可能随版本略有不同；核心是：**新地址 = 新私钥 = bot 专用**，与主账户分开。

#### 3.1.2 给 bot 地址入金（只充策略所需）

Bot 在 **Polygon 主网（chain 137）** 上需要两种资产：

| 资产 | 用途 | 建议数量 |
|------|------|----------|
| **POL**（原 MATIC） | 链上 redeem 等交易的 gas | 少量即可，例如 **0.5～2 POL** |
| **USDC**（Polygon 原生 USDC） | CLOB 交易 collateral | **策略资金**，见下表 |

**USDC 充多少？** 与 runtime `risk.bankroll_usd` 对齐，并留缓冲：

| 场景 | 建议 USDC | 对应 runtime（见 §3.2） |
|------|-----------|-------------------------|
| Paper 验证 | **0**（可不充） | `bankroll_usd = 500` 等模拟值即可 |
| Live 试运行 | 主账户转 **$200～$500** 到 bot 地址 | `bankroll_usd = 300`，`daily_budget_usd = 50`，`max_single_bet_usd = 25` |
| Live 小规模 | bot 地址持有 ≈ `bankroll_usd + reserve + 缓冲` | 例如 bankroll $1000 → 充 **$1100～$1200** USDC |

**怎么转：**

1. 在 Rabby **主账户**选择 **Send**。
2. 网络选 **Polygon**。
3. 收款地址填 **bot 子账户地址**（§3.1.1 新建的 `0x…`）。
4. 先转少量 **POL**，再转 **USDC**。
5. 在 Rabby 切换到 **bot 子账户**，确认余额到账。

也可从交易所提 USDC 到 Polygon 的 **bot 地址**（选 Polygon 网络，代币选 USDC）。

#### 3.1.3 Bot 地址与 Polymarket 网站的关系

oxide-arb **程序化下单**用的是 **bot 子账户私钥** 对应的 EOA，**不需要**把地址写进 `oxide-arb.toml`。

两种常见情况：

| 情况 | 说明 |
|------|------|
| **A. 纯 API bot（推荐）** | USDC 在 bot EOA 上；只配 bot 私钥；Live 时 CLOB 从该地址扣款。Polymarket **网站**可不登录，或仅用于人工查看市场。 |
| **B. 网站邮箱账户 + Proxy 地址** | 网站 Deposit 页可能显示 **Proxy/Safe**，与 bot EOA **不是同一地址**。此时要么把 USDC **从 Proxy 提到 bot EOA**，要么走 Polymarket Proxy/Deposit Wallet API 路径（进阶，见 [Wallet Types](https://docs.polymarket.com/trading/overview)）。**不要**把主账户 Proxy 私钥给 bot。 |

**Paper 阶段**：不必给 bot 充 USDC；但仍建议用 **bot 专用私钥**（与 Live 同一套身份），便于对账数据一致。

#### 3.1.4 从 bot 子账户导出私钥

**只导出 bot 子账户，不要导出主账户。**

1. Rabby 切换到 **`oxide-arb-bot`** 子账户。
2. **Settings → Manage Address**（或账户卡片 **⋮**）。
3. **Export Private Key**，输入 Rabby 密码。
4. 复制 hex（`0x` + 64 位），**仅**存入本机 secrets 文件（§3.2），勿提交 git、勿贴聊天。

可选验证（需 [Foundry cast](https://book.getfoundry.sh/)）：

```bash
cast wallet address --private-key "$OXIDE_ARB__KEYS__PRIVATE_KEY"
# 应等于 bot 子账户地址，而非主账户 0xC795…
```

---

### 3.2 在项目里怎么配置

oxide-arb **不配置钱包地址**；只配置 **密钥（TOML 和/或环境变量）** + **Runtime Config（策略资金参数）**。

#### 3.2.1 密钥：只需 `private_key`

oxide-arb **只配置 bot 钱包私钥**。Polymarket CLOB 的 L2 凭证（`api_key` / `secret` / `passphrase`）**不需要**写入配置——`ClobClient::connect` 会用私钥通过 SDK 在连接时自动 derive（已用真实网络验证）。

| 环境变量 / TOML 字段 | 含义 | 从哪里获取 |
|----------------------|------|------------|
| `OXIDE_ARB__KEYS__PRIVATE_KEY` / `private_key` | bot 子账户私钥 | Rabby 导出（§3.1.4） |

配置优先级（高 → 低）：

| 优先级 | 来源 |
|--------|------|
| 1 | `OXIDE_ARB__KEYS__PRIVATE_KEY` 环境变量 |
| 2 | `config/oxide-arb.local.toml`（gitignored，本机推荐） |
| 3 | `config/oxide-arb.toml` 中 `[keys].private_key` |

**本地 TOML 示例**（`config/oxide-arb.local.toml`）：

```toml
[keys]
source = "env"
private_key = "0x你的bot私钥"
```

同一文件还可覆盖 PG/CH/Redis 密码、Alchemy RPC 等；勿提交 git。

按模式是否必填：

| 模式 | `private_key` | 说明 |
|------|---------------|------|
| **DryRun** | 可选 | 不配也能启动 |
| **Paper / Live** | **必填** | 缺失则 boot fatal（Live）或警告且无 CLOB（Paper） |

#### 3.2.2 创建本机 secrets 文件（环境变量方式）

```bash
# 创建并限制权限
touch ~/.oxide-arb.env && chmod 600 ~/.oxide-arb.env
```

编辑 `~/.oxide-arb.env`（示例：Live 试运行 $300 策略）：

```bash
# ── Bot 钱包（Rabby 子账户 oxide-arb-bot 导出）──
export OXIDE_ARB__KEYS__PRIVATE_KEY=0x........................................

# ── 基础设施（按你的环境）──
export OXIDE_ARB__DB__POSTGRES__PASSWORD=...
export OXIDE_ARB__DB__CLICKHOUSE__PASSWORD=...
export OXIDE_ARB__POLYMARKET__ONCHAIN__RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/你的KEY

# ── Web（Live 必填强随机）──
export OXIDE_ARB__WEB__JWT__SECRET=$(openssl rand -hex 32)
```

启动前加载：

```bash
set -a && source ~/.oxide-arb.env && set +a
cargo run -p oxide-arb-bin -- --config-dir config
```

#### 3.2.3 Runtime Config：让策略参数与 bot 余额一致

**`bankroll_usd` 不是链上余额**，但是 Live sizing 的**资金上限**；应与 bot 地址上的 USDC 规模一致。详见 [bankroll-and-risk-metrics.md](./bankroll-and-risk-metrics.md)。

登录 Web API 后（`admin` / 首次改密），激活一版 runtime（示例：bot 充了 $300 USDC）：

```bash
TOKEN=... # login 返回的 access_token

# 查看当前配置
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/runtime-config | jq .

# 创建新版本并修改 risk 段（具体字段以 API schema 为准）
# UI 路径：System → Runtime Config
```

建议 bot 试运行参数：

| 字段 | 建议值 | 说明 |
|------|--------|------|
| `risk.bankroll_usd` | **300** | ≤ bot 地址 USDC（略留 POL gas 不动） |
| `risk.daily_budget_usd` | **50** | 日 spend 上限 |
| `risk.max_single_bet_usd` | **25** | 单笔上限 |
| `risk.reserve_balance_usd` | **50** | 不参与 Kelly 的保留额 |
| `risk.max_daily_loss_usd` | **75** | 与默认一致或更紧 |

Live 切换：`POST /api/system/mode`（见 §5、§6）。

#### 3.2.4 启动验收（私钥 + bot 地址）

```bash
# 1. private_key 已加载
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/system/deploy-config \
  | jq '.data.keys'
# private_key_present 为 true（Paper/Live）

# 2. 日志无 "Keystore unavailable" / "ClobClient connect failed"

# 3. 对账地址 = bot 地址（跑一段时间后）
# SELECT DISTINCT holder_address FROM balance_snapshot;
```

---

### 3.3 钱包地址要不要写进项目？

**不要。** 没有 `OXIDE_ARB__WALLET_ADDRESS` 配置项。

启动时从 **bot 私钥** 自动推导 `holder_address`（`build.rs` → `Keystore::address_string()`），用于 CLOB 签名、对账、redeem。

你现有的主账户 `0xC79510F5C1754E530fc8F9F469025901213031D4` **不应**再用于 bot：请按 §3.1 新建子账户，derive 时打印的地址应是**新 bot 地址**。

---

### 3.4 Polymarket 网站注册（可选，Live 前建议）

1. 打开 [polymarket.com](https://polymarket.com) 注册 / 登录（KYC 因地区而异）。
2. 网站主要用于**浏览市场**；bot 交易走 API + bot 私钥。
3. 若曾在网站入金到 **Proxy 地址**，记得把策略 USDC **转到 bot EOA**（§3.1.3），否则 API 读到的 bot 余额为 0。

---

### 3.5 Paper / Live 与 USDC、RPC

| 项目 | Paper | Live |
|------|-------|------|
| bot 地址需要 USDC | **否** | **是** |
| 四个 key env | **必须** | **必须** |
| `OXIDE_ARB__WEB__JWT__SECRET` | 建议 | **必须**（强随机） |
| Polygon RPC | 建议 | **必须** |

Paper 不花真 USDC，但 ClobClient 仍会读 bot 地址真实余额（对账 external）；`bankroll_usd` 仍管模拟 sizing。

---

### 3.6 凭证 FAQ

**Q: 只配私钥，DryRun 能跑吗？**  
能。默认 mode 为 `dry_run`。

**Q: 私钥用了主账户而不是 bot 子账户？**  
能跑，但不推荐；主账户资产全部暴露给 bot 进程。

**Q: Polymarket L2 三件套要单独配置吗？**  
不需要。只配 `private_key`；`ClobClient::connect` 时 SDK 自动 derive L2 凭证。

**Q: Live 报 Invalid Signature？**  
检查 bot 私钥、Polymarket 账户类型（EOA vs Proxy）、`chain_id=137`；对照 [Wallet Types](https://docs.polymarket.com/trading/overview)。

---

## 4. 配置：Deploy vs Runtime

### 4.1 Deploy 配置（`oxide-arb.toml`，改完需重启）

| 段 | 内容 |
|----|------|
| `[polymarket]` | CLOB REST/WS URL、`chain_id=137` |
| `[polymarket.onchain]` | Polygon RPC |
| `[polymarket.fees]` | 品类 feeRate（影响净利门槛） |
| `[market_data.*]` | Gamma/WS 连接参数 |
| `[db.postgres]` / `[db.clickhouse]` | 数据库 |
| `[cache.redis]` | Redis |
| `[web]` / `[web.jwt]` | 监听地址、JWT |
| `[keys]` | `source = "env"`，密钥走 env |

### 4.2 Runtime 配置（Postgres，热更新，API 管理）

**不在 TOML 里。** 通过 Web API / UI：

```http
GET  /api/runtime-config
POST /api/runtime-config/versions
POST /api/runtime-config/versions/{id}/activate
```

关键默认值（`RuntimeConfig::default`）：

| 字段 | 默认 | 说明 |
|------|------|------|
| `detection.min_profit_threshold_usd` | $0.50 | 最低预期净利 |
| `detection.endgame.settlement_window_hours` | 24 | 只扫 24h 内结算市场 |
| `detection.endgame.high_threshold` | 0.95 | 收敛价门槛 |
| `risk.bankroll_usd` | $1000 | 模拟本金 / Live sizing cap |
| `risk.daily_budget_usd` | $50 | 日 spend 上限 |
| `risk.max_single_bet_usd` | $25 | 单笔上限 |
| `market_data.enabled_categories` | 空 = 全品类 | 可收窄 hot universe |

### 4.3 Execution Mode（不在 TOML）

存在 Postgres `system_runtime_state`，**新库默认 `dry_run`**。

切换仅通过治理 API：

```http
POST /api/system/mode
Authorization: Bearer <access_token>
Content-Type: application/json

{
  "mode": "paper",
  "reason": "开始 Paper 验证阶段",
  "acting_role": "risk_owner"
}
```

需要 RBAC：`System:SwitchMode` + 合法 `acting_role`（如 `risk_owner`、`super_admin`）。

---

## 5. 编译、启动与首次验收

### 5.1 编译

```bash
cd /path/to/oxide-arb
cargo build --release -p oxide-arb-bin
```

二进制名：**`oxide-arb`**（crate 名 `oxide-arb-bin`）。

### 5.2 启动

```bash
# 可选：日志
export RUST_LOG=info,oxide_arb_core=debug

cargo run -p oxide-arb-bin -- --config-dir config
# 或
./target/release/oxide-arb --config-dir config
```

CLI 仅一个参数：`--config-dir`（env：`OXIDE_ARB_CONFIG_DIR`，默认 `config`）。

### 5.3 首次登录

Migration 会 seed 管理员：

| 字段 | 值 |
|------|-----|
| 用户名 | `admin` |
| 密码 | `admin` |

```bash
curl -s -X POST http://localhost:8080/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}'
```

**立即改密**（账户 API 或 UI）。默认密码仅用于首次登录。

### 5.4 启动验收清单

- [ ] `GET /api/system/health` → 200
- [ ] `GET /api/system/status` → `execution_mode: dry_run`，WS/Gamma 状态正常
- [ ] `GET /metrics` → Prometheus 有数据
- [ ] 日志：Gamma sync、WS connected、markets registered
- [ ] PG：`SELECT count(*) FROM market;` > 0
- [ ] 运行 10 分钟后 CH：`SELECT count() FROM opportunity_detection` 可能仍为 0（取决于当时是否有 endgame 机会，正常）

---

## 6. 三种执行模式详解

### 6.1 对比总表

| 维度 | DryRun | Paper | Live |
|------|--------|-------|------|
| **CLOB 下单** | 否 | 否 | **是**（真实 FOK） |
| **成交判定** | 永远 Filled @ limit | 查 **BookStore 深度** | CLOB 响应 |
| **Order ID** | `dry-{execution_id}` | `paper-{execution_id}` | 真实 venue ID |
| **Miss** | 无 | **有**（深度不足） | **有** |
| **Latency 模拟** | 0 ms | ~5 ms | 真实 RTT |
| **Risk metrics 来源** | `SimulatedDryRun` | `SimulatedPaper` | **`AuthoritativeClob`** |
| **现金基准** | `bankroll_usd` | `bankroll_usd` | CLOB 余额 + 持仓 |
| **Settlement redeem** | dry_run 占位 | paper 占位 | 链上 CTF redeem |
| **私钥** | 可选 | 必填 | 必填 + 强 JWT |
| **Persisted `trade` 行** | 是 | 是 | 是 |
| **CH audit 证据** | 是 | 是 | 是 |

### 6.2 DryRun — 实现要点

代码：`Dispatcher::dry_run`（`crates/oxide-arb-core/src/execution/dispatcher.rs`）

- 打日志 `[DRY RUN] Would place order`
- **不检查订单簿深度**
- 永远 `ExecutionOutcome::Filled`，`avg_fill_price = limit_price`
- 费用用 `FeeCalculator` 估算

**适用**：管道 smoke、检测频率、风控 deny 逻辑。**不适用**：评估真实 fill rate。

### 6.3 Paper — 成交依据（核心）

代码：`Dispatcher::paper_trade` + `has_sufficient_depth_at_price`

**Paper 判定 Filled 的条件（必须同时满足）：**

1. `BookStore` 中存在该 `token_id` 的 **published** 快照
2. 按方向检查深度：
   - **Buy**：`book.ask_notional_up_to(limit_price) >= estimated_cost`（限价以内 ask 名义深度 ≥ 计划花费）
   - **Sell**：`book.bid_depth_down_to(limit_price) >= shares`（限价以内 bid 数量深度 ≥ 计划卖出份额）

**不满足 → `ExecutionOutcome::Miss`**，reason 形如：

```text
paper: insufficient depth for {cost} at {limit_price}
```

**仍不模拟的部分（Paper 比 Live 乐观之处）：**

- 不模拟网络延迟导致的 book 变化（仅固定 5ms latency 字段）
- 不模拟 FOK 与其他 taker 竞争
- 不模拟 CLOB 拒单 / 部分成交规则（非 0 即 Filled）
- 成交价假设为 **limit price**（非 walk the book VWAP）

因此：**Paper fill rate 是 Live 的 optimistic lower bound 之一，但不是 upper bound**；Live 可能更差。

### 6.4 Live — 实现要点

代码：`FokOrderStrategy::execute_live_fok` → `ClobClient::place_order`

- 真实 HTTP FOK
- `fok_fills` / `fok_misses` metrics
- 需要 boot 时 ClobClient 可用
- 切 Live 协议（`mode_transition.rs`）：Preflight → Quiesce → Commit mode → Refresh authoritative metrics → Resume

**Live 额外 runtime 校验：**

- `settlement.redeem.route` ≠ `disabled`
- 启用的通知渠道凭证完整
- Metrics refresh 成功且 authoritative

### 6.5 模式切换注意事项

切换 mid-flight 会 quiesce（最多 30s）等待 reservation 排空，失败则保持 halt。永远通过 API 切换，不要手改 DB。

---

## 7. 分阶段验证：机会 → 成交 → PnL

### 7.1 Phase 0 — 有没有机会（DryRun，1～3 天）

**看什么算「有机会」：**

- 在 `settlement_window_hours`（默认 24h）内，**持续**有 detection 通过净利门槛
- Audit funnel 里不只是 `Detected`，还有少量到达 `Filled`（DryRun 下 Filled 偏多）
- Edge 分布集中在高 convergence 区（price zone Z95+）

**不能下的结论：**

- DryRun 的 Filled 比例
- 真实可赚金额

### 7.2 Phase 1 — 能不能成交（Paper，1～2 周）

**核心指标：**

```text
fill_rate = fills / (fills + misses)
miss_rate = misses / (fills + misses)
```

对比 DryRun 同窗口的「理论 fill 100%」→ 差距即 **深度/竞争损耗**。

**通过标准（经验值，需按你的 universe 调整）：**

- Paper fill_rate > 50% 且稳定 → 值得小资金 Live
- miss 主因 `insufficient depth` → 考虑缩小 `max_single_bet_usd` 或收窄品类
- 大量 `RiskRejected` / `ValidationRejected` → 调 runtime config，不是市场没机会

### 7.3 Phase 2 — 能赚多少

| 数据源 | 可信度 | 说明 |
|--------|--------|------|
| DryRun `trade` / metrics | ★☆☆ | 上限参考 |
| Paper `trade` + 未结算持仓 | ★★☆ | 缺 settlement payout |
| Paper/Live **已结算** `position` | ★★★ | 需等 market resolve |
| Live + redeem 完成 | ★★★★ | ground truth |

**粗算公式（Paper/Live 已成交未全结算）：**

```text
未实现：Σ(open position mark value - cost - fees)
已实现：Σ(settled realized_pnl_usd)   -- PG position / CH audit stage=Settled
日净利 ≈ GET /api/pnl/live 或 metrics oxide_arb_risk_daily_pnl_usd
```

**Endgame 现实约束：**

- 机会集中在临近结算的高价收敛市场
- 单笔默认 max $25，日 budget 默认 $50
- crypto 等高 fee 品类 edge 被费率吃掉
- **合理预期**：小 bankroll 下日净利 **$0～几十 USD** 量级，必须以你的 Paper/Live 数据为准

### 7.4 Phase 3 — 小资金 Live

1. 充值 USDC，`bankroll_usd` 对齐策略资金
2. 收紧 `daily_budget_usd`、`max_single_bet_usd`
3. `POST /api/system/mode` → `live`
4. 观察 24～72h：`fok_misses`、真实 fee、`/api/pnl/live`
5. 等至少一个完整 **成交 → 结算 → redeem** 周期再评估 ROI

---

## 8. 观测面速查（在哪看、拿什么结果）

### 8.1 HTTP API（Base: `http://localhost:8080/api`，需 Bearer token）

#### 系统与模式

| 端点 | 用途 |
|------|------|
| `GET /system/status` | 当前 mode、组件健康、metrics source |
| `GET /system/health` | 健康探针 |
| `GET /system/deploy-config` | Deploy 配置（脱敏） |
| `POST /system/mode` | 切换 dry_run / paper / live（治理） |
| `POST /system/halt` / `/resume` | 紧急 halt |

#### Runtime 配置

| 端点 | 用途 |
|------|------|
| `GET /runtime-config` | 当前激活配置 |
| `POST /runtime-config/versions` | 创建新版本 |
| `POST /runtime-config/versions/{id}/activate` | 激活（含 `bankroll_usd` 等） |

#### 机会与漏斗（ClickHouse 证据）

| 端点 | 用途 |
|------|------|
| `GET /opportunities/recent?page=&size=` | 最近 24h detections |
| `GET /opportunities/history?from=&to=&market_id=` | 历史 detection 分页 |
| `GET /opportunities/stats?from=&to=` | **Audit funnel 行**（按 stage） |
| `GET /opportunities/{opportunity_id}` | 单条机会完整 audit trail |

**Audit stage 枚举**（CH `opportunity_audit.stage`）：

`Detected` → `ValidationRejected` / `RiskRejected` / `SizingRejected` → `Filled` / `Missed` / `Failed` → `Settled`

#### 成交与风控决策

| 端点 | 用途 |
|------|------|
| `GET /trades?page=&size=&execution_mode=` | PG 成交历史 |
| `GET /trades/{trade_id}` | 单笔详情 |
| `GET /trades/decisions?from=&to=` | 风控决策审计 |

#### PnL 与分析

| 端点 | 用途 |
|------|------|
| `GET /pnl/live` | **内存中实时 PnL 快照** |
| `GET /pnl/daily` | 最近持久化日报（hourly task 生成） |
| `GET /pnl/weekly` | 最近周报 |
| `GET /analytics/edge-distribution?from=&to=` | **Edge 直方图**（基于 trade 历史） |
| `GET /analytics/market-performance?from=&to=` | 分 market 聚合 PnL |

#### 风险仪表盘

| 端点 | 用途 |
|------|------|
| `GET /risk/circuit-breaker` | 断路器 + 快照 |
| `GET /risk/positions` | 未平仓位 |
| `GET /risk/exposure` | 总 exposure |
| `GET /risk/daily-loss` | 日亏损累计 |

#### Control Factors 与物料化

| 端点 | 用途 |
|------|------|
| `GET /control-factors?status=draft\|candidate` | 因子列表 |
| `GET /control-factors/{id}` | 单因子（含 evidence JSON） |
| `POST /control-factors/{id}/reject` | 拒绝 candidate |
| `GET /control-factors/publications?mode=` | 发布记录 |
| `POST /control-factors/publications/shadow` | **Shadow 发布** |
| `POST /control-factors/publications/publish` | **Published 发布** |
| `POST /control-factors/publications/emergency` | 紧急发布（1h TTL） |
| `GET /control-factors/publications/{id}/shadow-decisions` | Shadow 对比决策 |
| `GET /control-factors/audit` | 治理审计链 |
| `GET /replay/{run_id}` | 物料化 run 状态 |
| `GET /replay/{run_id}/history` | 各 stage 报告 |
| `POST /replay` | 手动 enqueue 回测 run（治理） |

#### WebSocket

```text
GET /api/ws?token=<access_token>
```

推送 `CoreEvent`（机会、PnL 等），供 UI 实时展示。

### 8.2 Prometheus（`GET /metrics`）

| 指标 | 含义 |
|------|------|
| `oxide_arb_detection_opportunities_total` | 检测到的机会数 |
| `oxide_arb_execution_trades_filled_total` | 成交（含 simulated） |
| `oxide_arb_execution_trades_missed_total` | Miss |
| `oxide_arb_execution_fok_fills_total` | Live FOK fill |
| `oxide_arb_execution_fok_misses_total` | Live FOK miss |
| `oxide_arb_risk_daily_pnl_usd` | 日 PnL gauge |
| `oxide_arb_risk_exposure_usd` | Exposure |
| `oxide_arb_pipeline_ws_events_received_total` | WS 事件 |
| `oxide_arb_control_factor_active_count` | 已加载因子数 |
| `oxide_arb_control_factor_shadow_decisions_total` | Shadow 决策计数 |

### 8.3 PostgreSQL 快查

```sql
-- 当前模式
SELECT execution_mode FROM system_runtime_state;

-- 今日成交按模式
SELECT execution_mode, count(*), sum(expected_net_profit_usd)
FROM trade WHERE created_at > now() - interval '1 day'
GROUP BY 1;

-- 物料化 run
SELECT schedule_id, status, terminal_status, created_at
FROM control_factor_materialization_run
ORDER BY created_at DESC LIMIT 10;

-- 因子产出
SELECT factor_type, status, created_at
FROM control_factor_value
ORDER BY created_at DESC LIMIT 20;

-- 未平仓位
SELECT * FROM position WHERE closed_at IS NULL;
```

### 8.4 ClickHouse 快查

```sql
-- 检测量按小时
SELECT toStartOfHour(detected_at) h, count()
FROM opportunity_detection
WHERE detected_at > now() - INTERVAL 7 DAY
GROUP BY h ORDER BY h;

-- Funnel stage 分布
SELECT stage, count()
FROM opportunity_audit
WHERE detected_at > now() - INTERVAL 1 DAY
GROUP BY stage;

-- Paper miss 样本
SELECT rejection_reason, count()
FROM opportunity_audit
WHERE stage = 'Missed' AND detected_at > now() - INTERVAL 1 DAY
GROUP BY rejection_reason;
```

### 8.5 UI（oxide-arb-ui，可选）

-  monorepo 路径：`oxide-arb-ui/`
- 后端 `[web].serve_static_ui = true` 且构建产物放到 `static/ui`
- 或 dev server 代理到 `:8080` API
- 菜单对应：Opportunities、Trades、PnL、Analytics、Control Factors、Runtime Config、System

---

## 9. Control Factor 沉淀与发布

### 9.1 五种因子族

| 类型 | 控制什么 | 默认 cadence |
|------|----------|--------------|
| **ExecutionQuality** | fill 概率、深度使用、slippage addon | hourly |
| **ReconciliationHealth** | 对账 drift、是否 halt | hourly |
| **BucketRisk** | 分 bucket haircut / size multiplier | daily |
| **PortfolioRisk** | 组合 throttle、drawdown | daily |
| **MarketAnomaly** | 异常 market 阻断（当前偏 report-only） | 事件驱动 |

### 9.2 是否必须 Live？

**不必须。** 物料化 pipeline **不读** `ExecutionMode`。

但需要 **持久化证据**：

| 输入 | 来源 | DryRun | Paper | Live |
|------|------|--------|-------|------|
| CH detections/audits | Scanner+Execution | ✓ | ✓ | ✓ |
| CH book_snapshots | WS + 60s publisher | ✓ | ✓ | ✓ |
| PG trade/position | Execution | ✓（sim id） | ✓ | ✓ |
| PG market_pit_snapshot | Gamma upsert | ✓ | ✓ | ✓ |
| PG resolution_event | Settlement | 需等 resolve | 需等 resolve | 需等 resolve |
| PG balance_snapshot | Ledger reconciliation | 需私钥 | 需私钥 | ✓ |
| PG reconciliation_report | Risk reconciler | 需私钥+合理 baseline | 同左 | ✓ |

### 9.3 自动调度（进程内，无需额外启动）

| 任务 | 间隔 | 行为 |
|------|------|------|
| `ControlFactorScheduler` | 300s tick | enqueue 周期性 run |
| `MaterializationExecuteWorker` | 轮询 | 执行 `Queued` run |
| `source_delay_secs` | 900s | 窗口结束后再跑，等 CH 落盘 |

Schedule IDs：

- `execution-quality-hourly`
- `reconciliation-health-hourly`
- `bucket-risk-daily`
- `portfolio-risk-daily`

### 9.4 Production Quality Gate 默认门槛

（`QualityGatePolicy::default`，`oxide-arb-models`）

| 因子 | min_opportunities | min_markets | min_settlements |
|------|-------------------|-------------|-----------------|
| ExecutionQuality | 200 | 20 | 0 |
| BucketRisk | 100 | 20 | **50** |
| PortfolioRisk | 100 | 10 | 30 |
| ReconciliationHealth | 0 | 0 | 0 |

另需：PIT `production_eligible`、L2 coverage（ExecutionQuality ≥95%）、leakage/stability/tail 等 gate。

**时间预期：**

- ExecutionQuality：约 **3～7 天**连续 Paper
- BucketRisk（50 settlements）：约 **2～6 周**（取决于成交 + 自然结算）

### 9.5 因子状态机（治理）

```text
Materialization → Draft / ReportOnly / Rejected / Candidate
       ↓ (gate 通过)
Governance: Candidate → Shadow publication → Published publication
       ↓
FactorRefresher 加载 → FactorSnapshotStore (ArcSwap) → 热路径读取
```

**物料化 never auto-publish。** 必须人工：

1. `GET /control-factors?status=candidate` 审查 evidence
2. `POST /control-factors/publications/shadow` — 进入 Shadow 对比
3. `GET .../shadow-decisions` — 观察 24h+ baseline vs shadow 差异
4. `POST /control-factors/publications/publish` — risk_owner 批准 Published
5. （可选 rollback）`POST /control-factors/publications/{id}/rollback`

### 9.6 验收物料化 run

```bash
# 1. 查最近 run（需从 PG 或 replay API 拿 run_id）
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/api/replay/{run_id}

# 2. stage 历史
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/api/replay/{run_id}/history
```

成功标准：

- `terminal_status` = 预期（非全 ReportOnly）
- `resolve_inputs` stage：PIT manifest `production_eligible = true`
- `quality_gate_evaluation`：目标因子为 `Candidate` 而非 `Rejected`
- PG `control_factor_value.status` = `draft` 或 `candidate`

---

## 10. 因子与 Live 热路径接线

### 10.1 架构（只读内存，不查 CH/PG）

```text
Postgres: Published/Shadow publication
    ↓ FactorRefresher (60s poll + notify)
FactorSnapshotStore (ArcSwap<ControlFactorSnapshot>)
    ↓ ControlFactorProvider trait
Scanner / Validator / RiskEngine / Sizer / ExecutionPipeline
    ↓
AppliedControlFactor IDs 写入 CH opportunity_audit
```

设计文档：`docs/plans/phase5.6-live-consumption.md`

### 10.2 Published vs Shadow

| 模式 | 热路径行为 |
|------|------------|
| **Published** | **实际调整** detection/scoring/risk/sizing 参数 |
| **Shadow** | 并行计算「若有因子会怎么决策」，写入 `control_factor_shadow_decision`，**不改变** Published 行为 |

Shadow 用于发布前验证；Published 用于 Live 真实生效。

### 10.3 Live fail-closed

`FactorRefreshConfig::for_live(true)`：

- 关键安全因子过期或 Published 加载失败 → **Live 启动/运行 fail-closed**
- DryRun/Paper 更宽松

### 10.4 发布后与 Live 接轨的操作顺序

1. Paper 阶段积累证据 → materialization 产出 Candidate
2. **Shadow 发布**（可在 Paper 下观察 shadow-decisions）
3. 确认 shadow 不会过度 reject 或 oversize
4. **Published 发布**
5. `FactorRefresher` 自动 reload（或等 60s poll）
6. 验证 metrics `oxide_arb_control_factor_active_count` > 0
7. 切 Live（或已在 Live）观察 audit 中 `applied_factor_ids_json`
8. 逐步放大 `bankroll_usd` / `daily_budget_usd`（通过 runtime config activate，**单独治理**）

> **注意**：因子发布与 `bankroll_usd` 是两条控制线 — 前者是 evidence-based 微调，后者是硬 cap。

---

## 11. Prometheus / SQL 验收清单

### 11.1 每日巡检（Paper 阶段）

- [ ] `oxide_arb_pipeline_ws_events_received_total` 持续增长
- [ ] `oxide_arb_detection_opportunities_total` > 0
- [ ] `fills / (fills+misses)` 符合预期
- [ ] `oxide_arb_risk_daily_pnl_usd` 无异常跳变
- [ ] CH audit stage 分布无异常堆积在 `ValidationRejected`
- [ ] PG `control_factor_materialization_run` 最近 24h 有 Succeeded

### 11.2 切 Live 前

- [ ] private_key + JWT + RPC 已配置
- [ ] `GET /system/status` metrics authoritative
- [ ] `settlement.redeem.route` 有效
- [ ] 至少一套 Published 因子或确认无因子时 baseline 行为可接受
- [ ] 断路器 Closed，halt 未激活

---

## 12. 常见问题与排错

| 现象 | 可能原因 | 处理 |
|------|----------|------|
| 启动失败 PG/CH/Redis | 连接配置错误 | 查 `oxide-arb.toml` 与 env |
| 无 detection | 当前无 24h 内 endgame 市场 / universe 过窄 | 查 Gamma markets；放宽 `enabled_categories` |
| DryRun 有 Filled、Paper 全 Miss | 深度不足 | 降 `max_single_bet_usd`；看 CH miss reason |
| Paper PnL 好、Live 差 | 竞争/latency/slippage | 正常；以 Live 为准 |
| 因子全 ReportOnly | 样本不足或 PIT ineligible | 延长运行；配私钥改善 balance_snapshot |
| 对账 drift 巨大（Paper） | `bankroll_usd` ≠ 钱包且未成交 | 预期；或对齐 bankroll / 忽略 Reconciliation 因子 |
| 切 Live 失败 | preflight：redeem/JWT/metrics | 查 API 错误体与日志 |
| ClobClient connect failed | 私钥/network | 查 `RUST_LOG` 与 Polymarket 状态 |

---

## 附录 A：环境变量速查

```bash
# 配置目录
export OXIDE_ARB_CONFIG_DIR=config

# 密钥（bot 私钥，Rabby §3.1.4；L2 凭证连接时自动 derive）
export OXIDE_ARB__KEYS__PRIVATE_KEY=0x...

# 数据库
export OXIDE_ARB__DB__POSTGRES__PASSWORD=...
export OXIDE_ARB__DB__CLICKHOUSE__PASSWORD=...

# Redis（若启用密码）
export OXIDE_ARB__CACHE__REDIS__PASSWORD=...

# Web
export OXIDE_ARB__WEB__JWT__SECRET=...

# Polygon RPC
export OXIDE_ARB__POLYMARKET__ONCHAIN__RPC_URL=...

# 日志
export RUST_LOG=info,oxide_arb_core=debug
```

## 附录 B：相关源码索引

| 模块 | 路径 |
|------|------|
| 执行模式分发 | `crates/oxide-arb-core/src/execution/dispatcher.rs` |
| Live FOK | `crates/oxide-arb-core/src/execution/fok_strategy.rs` |
| 模式切换 | `crates/oxide-arb-core/src/control/mode_transition.rs` |
| 物料化调度 | `crates/oxide-arb-core/src/app/mod.rs` |
| 因子 refresher | `crates/oxide-arb-core/src/control/factor_refresher.rs` |
| Quality gates | `crates/oxide-arb-control/src/gates/mod.rs` |
| Web 路由 | `crates/oxide-arb-web/src/routes/` |
| CH schema | `crates/oxide-arb-storage/src/clickhouse/sql/` |
