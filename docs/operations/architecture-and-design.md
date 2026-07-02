# quant-pivot 架构与详细设计

> Last reviewed: 2026-07-01.
>
> This document describes the current quant-pivot architecture implemented in this repository. Historical Endgame-phase documents are background only; current source of truth is the active quant-pivot code, runtime-config schema v9, and the operations runbook.

## 1. 系统目标

quant-pivot 是 Polymarket-only 的量化系统。它的闭环目标不是直接发现一个“机会”就立即冲单，而是产生可审计、可治理、可回放的 `RecommendationReport`，再按 runtime mode 决定是否进入订单执行链路。

核心问题：

1. 买什么：哪个 market、token、outcome；
2. 什么时候买：entry window、book freshness、depth、limit/slippage；
3. 买多少：真实 venue 账户资金、Kelly sizing、组合约束、流动性和 correlation；
4. 为什么买：features、factors、model、expected return、confidence、evidence；
5. 什么时候卖：take profit、stop loss、time exit、signal invalidation、hold-to-resolution；
6. 卖多少：position lot、exit plan、partial/full exit policy；
7. 如何闭环：execution、reconciliation、settlement redeem、attribution、training feedback。

## 2. 端到端业务闭环

```mermaid
flowchart TD
    A["Polymarket Gamma metadata"] --> B["MarketRegistry"]
    C["CLOB L2 WebSocket"] --> D["BookStore"]
    E["Polymarket Data API"] --> F["Account/Position reads"]
    B --> G["Selection"]
    D --> G
    D --> H["Feature pipeline"]
    B --> H
    H --> I["Factor engine"]
    I --> J["Model runner"]
    F --> K["AccountSnapshot"]
    J --> L["Portfolio planner"]
    K --> L
    D --> L
    L --> M["RecommendationReport"]
    M --> N{"Runtime mode"}
    N -->|"report_only"| O["Human reads report"]
    N -->|"semi_auto"| P["OrderIntent pending_approval"]
    N -->|"auto_execution"| Q["OrderIntent approved_by_policy"]
    P --> R["Human approve/reject"]
    R --> S["Admission engine"]
    Q --> S
    S -->|"allow"| T["CLOB signed order"]
    S -->|"deny/defer"| U["No venue order"]
    T --> V["ExecutionOrder"]
    V --> W["Reconciliation"]
    W --> X["Position ledger"]
    X --> Y["Exit monitor"]
    Y --> Z["Exit order or settlement redeem"]
    Z --> AA["Attribution"]
    AA --> AB["Research/training feedback"]
    AB --> H
```

关键设计点：

- 报告是 first-class artifact，不是执行副产品；
- `report_only` 仍读取真实账户资金；
- execution 是 mode-gated 和 admission-gated 的后续链路；
- 所有订单状态、资金锁定、reconciliation 和 attribution 都落 Postgres；
- 市场事实和研究事实主要进入 ClickHouse；
- runtime-config 是版本化治理对象，部署凭证不是策略配置。

## 3. 外部上下文

```mermaid
flowchart LR
    subgraph Polymarket
        Gamma["Gamma API"]
        ClobWs["CLOB L2 WebSocket"]
        ClobRest["CLOB REST"]
        DataApi["Data API"]
        Bridge["Bridge / pUSD"]
        Chain["Polygon / CTF"]
        Relayer["Gasless Relayer"]
    end

    subgraph quant-pivot
        Bin["quant-pivot-bin"]
        Core["quant-pivot-core"]
        Api["quant-pivot-api"]
        Web["quant-pivot-web"]
        Models["quant-pivot-models"]
        Storage["quant-pivot-storage"]
        Repo["quant-pivot-repository"]
        Research["quant-pivot-research"]
    end

    subgraph StoragePlane
        Pg["Postgres"]
        Ch["ClickHouse"]
        Redis["Redis"]
    end

    Admin["Admin UI / Operator"]

    Gamma --> Api
    ClobWs --> Api
    ClobRest <--> Api
    DataApi --> Api
    Chain <--> Api
    Relayer <--> Api
    Api <--> Core
    Core <--> Research
    Core <--> Repo
    Repo <--> Pg
    Storage <--> Pg
    Storage <--> Ch
    Core <--> Redis
    Web <--> Core
    Web <--> Repo
    Admin <--> Web
```

## 4. Crate 分层

| Crate | 职责 | 不应该做什么 |
|-------|------|--------------|
| `quant-pivot-bin` | CLI entrypoint、加载 deploy config、bootstrap app | 不承载业务逻辑 |
| `quant-pivot-core` | AppContext、data ingest、report builder、governance、execution、account、reconciliation | 不直接暴露 HTTP contract |
| `quant-pivot-api` | Polymarket Gamma/CLOB/Data API/on-chain/relayer clients | 不做策略 sizing |
| `quant-pivot-models` | DTO、domain structs、enums、runtime-config schema、entities/idens | 不访问外部服务 |
| `quant-pivot-storage` | DB pools、migrations、ClickHouse writer | 不编码交易策略 |
| `quant-pivot-repository` | Postgres repository traits/impl、idempotent writes | 不写 HTTP handler |
| `quant-pivot-web` | Actix routes、auth、RBAC、API envelope、WS | 不绕过 core 服务直接下单 |
| `quant-pivot-research` | features、factors、model、portfolio sizing/optimizer | 不访问私钥或提交订单 |
| `quant-pivot-test-support` | 测试 harness 和 fixtures | 不进入 production runtime |
| `quant-pivot-xtask` | 项目任务脚本 | 不作为服务依赖 |

## 5. 配置架构

### 5.1 Deploy config

Deploy config 管连接和凭证：

- Polymarket CLOB/Gamma/Data API/RPC/relayer endpoints；
- DB/ClickHouse/Redis；
- private key；
- `quant.account.funder`；
- `quant.account.wallet_kind`；
- Web listen/CORS/JWT；
- observability。

加载顺序：

```mermaid
flowchart LR
    Defaults["code defaults"] --> Base["config/quant-pivot.toml"]
    Base --> Local["config/quant-pivot.local.toml"]
    Local --> Env["QUANT_PIVOT__... env"]
    Env --> Cli["--config-dir / QUANT_PIVOT_CONFIG_DIR"]
    Cli --> Effective["DeployConfig"]
    Effective --> Validation["validate_for_quant_mode"]
```

Deploy config 拒绝 unknown fields。所有 mode 都要求 private key 和 funder；可执行 mode 下 proxy/safe 还要求 relayer credentials。

### 5.2 Runtime config v9

Runtime config 管策略和治理：

```mermaid
flowchart TD
    UI["Admin UI / API"] --> Version["POST /api/runtime-config/versions"]
    Version --> Validate["schema + semantic validation"]
    Validate --> Store["immutable version in Postgres"]
    Store --> Activate["POST /activate"]
    Activate --> Apply["RuntimeConfigStore apply"]
    Apply -->|"success"| Active["active config"]
    Apply -->|"failure"| Rollback["automatic revert to previous active"]
```

主要 section：

| Section | 职责 |
|---------|------|
| `selection` | 市场选择、黑白名单、流动性/到期窗口 |
| `data_quality` | book age、coverage、fact lag、source delay |
| `features` | 特征窗口、snapshot、feature pipeline 参数 |
| `factors` | 因子权重、方向、启用状态 |
| `model` | active model、shadow/published requirement、inference policy |
| `quality_gate` | 训练/发布质量门槛 |
| `training` | dataset 和训练调度 |
| `reports` | schedule、Top-N、ad-hoc、valid window |
| `portfolio` | budget、Kelly sizing、constraints、optimizer |
| `execution` | semi-auto/auto、entry policy、exit monitor、capital、reconciliation、redeem、breaker |
| `notification` | Telegram/webhook delivery |

## 6. 数据面设计

### 6.1 Market metadata

Gamma 同步负责发现和更新 Polymarket 市场：

1. 定时 full sync；
2. 分页拉取 Gamma markets；
3. 解析 token、outcome、resolution time、category、negRisk 等 metadata；
4. 写入 MarketRegistry / Postgres；
5. 生成订阅计划给 CLOB WS。

### 6.2 BookStore

BookStore 是 L2 order book 的热路径读模型：

- CLOB WS 接收 book snapshot/update；
- 按 token id 维护 best bid/ask、depth ladder、book timestamp；
- 提供 lock-free 或低锁读取给 selection、feature、entry/admission、exit monitor；
- 对 book age、depth、spread 输出 data quality signal。

### 6.3 ClickHouse facts

ClickHouse 存储研究和回放友好的事实：

- book snapshots / top-of-book；
- data-quality measurements；
- feature rows；
- model inference rows；
- attribution / PnL analytics。

Postgres 是业务系统 of record；ClickHouse 是事实/分析面。不能把 ClickHouse 当作订单状态真相。

## 7. 账户与资金设计

### 7.1 Account provider

```mermaid
sequenceDiagram
    participant Builder as ReportBuilder
    participant Account as VenueAccountProvider
    participant CLOB as CLOB REST
    participant Data as Polymarket Data API
    participant Repo as Postgres

    Builder->>Account: snapshot(as_of, funder)
    Account->>CLOB: collateral balance
    CLOB-->>Account: collateral
    Account->>Data: positions(funder)
    Data-->>Account: positions with value
    Account->>Account: venue_nlv = collateral + positions_value
    Account->>Account: capital_base = min(venue_nlv, runtime budget cap)
    Account->>Repo: persist account/equity snapshot
    Account-->>Builder: AccountSnapshot
```

账户真相：

- `collateral` 来自 CLOB；
- `positions` 来自 Data API；
- `venue_net_liquidation = collateral + positions_value`；
- `capital_base = min(venue_net_liquidation, portfolio.budget.total_budget_usd)`；
- `available = collateral - reserved`。

缺 credentials 或 funder 时，报告生成失败，不会 fallback 到配置预算。

### 7.2 Capital allocation

执行链路会维护 capital allocation：

| State | 语义 |
|-------|------|
| `planned` | recommendation sizing 中计划使用 |
| `allocated` | intent 创建/审批后预留 |
| `locked` | submit / venue in-flight |
| `spent` | 成交后实际花费 |
| `released` | 未成交、取消或释放 |
| `impaired` | reconciliation 异常或账务不一致 |

capital allocation 用于防止重复花同一笔现金，也用于 auto preflight。

## 8. 报告生成详细设计

```mermaid
sequenceDiagram
    participant Trigger as Schedule/AdHoc Trigger
    participant Builder as ReportBuilder
    participant Config as RuntimeConfigStore
    participant Select as MarketSelector
    participant Account as AccountProvider
    participant Feature as FeaturePipeline
    participant Model as ModelRunner
    participant Portfolio as PortfolioPlanner
    participant Composer as ReportComposer
    participant Repo as ReportRepository
    participant Pub as EventPublisher

    Trigger->>Builder: build(trigger_kind, trigger_key)
    Builder->>Config: active config
    Config-->>Builder: RuntimeConfig v9
    Builder->>Builder: as_of = trigger_time - source_delay
    Builder->>Select: candidates(as_of, selection config)
    Select-->>Builder: candidate markets/tokens
    alt no candidates
        Builder->>Repo: publish empty report(empty_selection)
    else candidates exist
        Builder->>Account: live account snapshot
        Account-->>Builder: AccountSnapshot
        Builder->>Feature: feature rows(candidates, as_of)
        Feature-->>Builder: features + data quality
        Builder->>Model: infer(features, active model)
        Model-->>Builder: signals
        Builder->>Portfolio: size and optimize(signals, account, constraints)
        Portfolio-->>Builder: planned recommendations or rejections
        Builder->>Composer: compose Top-N payloads
        Composer-->>Builder: RecommendationReport
        Builder->>Repo: persist report, recommendations, evidence
    end
    Repo-->>Builder: report id/status
    Builder->>Pub: quant.report event
```

### 8.1 Empty report

报告为空不是错误，它是 fail-closed 输出。常见 reason：

- system degraded；
- empty selection；
- insufficient data quality；
- no positive signal；
- portfolio budget exhausted。

### 8.2 Recommendation payload

推荐由这些 contract 组成：

| Payload block | 内容 | 使用者 |
|---------------|------|--------|
| Identity | market/token/outcome/rank/report refs | UI、operator、intent create |
| Signal | model score、confidence、expected return、factor contribution | quant、approver |
| EntryPlan | trigger、limit/immediate、slippage、book age、valid window | admission、operator |
| SizingPlan | suggested USD/shares、Kelly、caps、binding constraints | portfolio、approver |
| ExitPlan | TP/SL/time/signal/hold/redeem | exit monitor、operator |
| RiskEnvelope | exposure、liquidity、downside、correlation | admission、risk |
| Evidence | feature snapshot、data quality、model/factor refs | audit、debug |
| ExecutionEligibility | eligible modes、auto reasons、approval required | UI、dispatcher |

## 9. Portfolio 与 sizing 设计

### 9.1 Sizing

当前 sizing model 是 Kelly-based：

```mermaid
flowchart TD
    A["model expected return"] --> B["derive win probability"]
    C["downside bps"] --> B
    D["target_reward_multiple"] --> B
    B --> E["full Kelly"]
    F["kelly_fraction"] --> G["fractional Kelly"]
    E --> G
    H["confidence"] --> I["confidence weighting"]
    G --> I
    J["drawdown"] --> K["drawdown scaling"]
    I --> K
    L["max_position_pct"] --> M["position cap"]
    K --> M
    M --> N["raw suggested USD/shares"]
```

### 9.2 Optimizer constraints

Portfolio planner 把 raw sizing 放进约束优化：

- total budget；
- available cash；
- min/max recommendation USD；
- max market exposure；
- max event exposure；
- max category exposure；
- max correlated exposure；
- liquidity usage cap；
- Kelly cap；
- drawdown scaling；
- correlation policy。

输出可能是：

- accepted recommendation with binding constraints；
- rejected candidate with explicit reason；
- empty report if all candidates rejected。

## 10. 执行治理设计

### 10.1 Intent lifecycle

```mermaid
stateDiagram-v2
    [*] --> pending_approval: semi_auto create
    [*] --> approved_by_policy: auto_execution create
    pending_approval --> approved: approve
    pending_approval --> rejected: reject
    pending_approval --> cancelled: cancel
    approved --> admission_pending: submit
    approved_by_policy --> admission_pending: dispatcher/submit
    admission_pending --> submitted: admission allow + venue accepted
    admission_pending --> admission_rejected: admission deny
    admission_pending --> failed: venue/client failure
    submitted --> partially_filled
    submitted --> filled
    submitted --> rejected
    submitted --> failed
    submitted --> invalidated
    partially_filled --> filled
    partially_filled --> expired
```

`OrderIntent` 冻结 recommendation 的 entry/sizing/risk envelope。审批、提交、拒绝、取消都会写 operation log。

### 10.2 Admission engine

Admission 是提交前最后一道门。它检查：

| Check | 目的 |
|-------|------|
| `intent_state` | intent 必须处于可提交状态 |
| `recommendation_freshness` | 推荐未过期 |
| `report_status` | 报告仍 published 且未 revoke |
| `runtime_mode` | 当前 mode 允许订单提交 |
| `model_publication` | model 状态仍可用 |
| `data_quality` | 数据质量未退化 |
| `book_freshness` | order book 足够新 |
| `entry_trigger` | 价格/depth/entry window 符合 |
| `risk_envelope_hash` | 审批未篡改推荐风险包络 |
| `capital_budget` | 资金仍足够 |
| `max_open_intents` | open intents 未超限 |
| `max_reserved_capital` | reserved capital 未超限 |
| `market_exposure` | 单 market exposure 未超限 |
| `event_exposure` | event exposure 未超限 |
| `category_exposure` | category exposure 未超限 |
| `liquidity_depth` | 可成交深度足够 |
| `slippage` | slippage cap 未突破 |
| `manual_block` | 市场/推荐未被人工 blocked |
| `kill_switch` | kill switch 允许新开仓 |
| `venue_guard` | venue 状态和 order constraints 可用 |
| `credential_readiness` | 私钥/funder/relayer 可用 |
| `exit_monitor_readiness` | 可执行后续退出管理 |

Outcome：

- `allow`：签名并提交；
- `deny`：不可执行，通常转 `admission_rejected`；
- `defer`：临时不可执行，可稍后重试。

## 11. Entry order 与 venue 设计

```mermaid
sequenceDiagram
    participant Submit as SubmitIntentService
    participant Admission as AdmissionEngine
    participant Client as ClobClient
    participant CLOB as Polymarket CLOB
    participant Repo as Repositories
    participant Recon as ReconciliationQueue

    Submit->>Repo: claim intent atomically
    Submit->>Admission: evaluate frozen intent + live context
    Admission-->>Submit: allow / deny / defer
    alt deny
        Submit->>Repo: intent admission_rejected
    else defer
        Submit->>Repo: release claim, keep submittable
    else allow
        Submit->>Repo: lock capital, create execution_order planned
        Submit->>Client: sign and place order
        Client->>CLOB: POST order
        CLOB-->>Client: order status / error / ambiguous
        Client-->>Submit: venue result
        Submit->>Repo: settle execution_order + intent + capital
        Submit->>Recon: enqueue if ambiguous or needs truth sweep
    end
```

Order policy：

- default 是 limit entry；
- `allow_market_orders=false` 时不允许 immediate market order；
- `allow_market_orders=true` 时仍必须带 worst-price / slippage cap；
- `max_slippage_bps`、tick size、book depth、book age 都参与 admission；
- BUY 需要 pUSD allowance，SELL 需要 conditional token allowance。

## 12. Exit、settlement、attribution 设计

### 12.1 Exit monitor

Exit monitor 读取 open positions 和 exit plan：

```mermaid
flowchart TD
    A["open position lot"] --> B["load original recommendation exit plan"]
    B --> C["read fresh book and signal"]
    C --> D{"kill switch"}
    D -->|"execution_halted"| E["manual only"]
    D -->|"emergency_halted"| F["emergency path"]
    D -->|"closed/report_only_forced/exit_only"| G{"exit trigger?"}
    G -->|"stop_loss"| H["submit exit order"]
    G -->|"signal_invalidation"| H
    G -->|"time_exit"| H
    G -->|"take_profit"| H
    G -->|"hold_to_resolution"| I["wait for resolution"]
    I --> J["settlement redeem"]
    H --> K["execution order phase=exit"]
    K --> L["reconciliation"]
    J --> M["redeem record"]
    L --> N["position closed/settled"]
    M --> N
    N --> O["attribution"]
```

### 12.2 Settlement redeem

Resolved market 后：

- winning tokens redeem to pUSD；
- losing tokens settle to zero；
- redeem burns the full condition balance；
- policy 可自动批量 redeem；
- 不支持或失败时生成 manual required。

### 12.3 Attribution

Attribution 只在 truth 明确后 finalize：

- entry order terminal；
- exit order terminal 或 settlement redeem confirmed；
- reconciliation 不为 pending/unresolvable；
- position state closed/settled；
- account/equity snapshot 可对齐。

输出用于 PnL、模型反馈、factor governance 和训练数据。

## 13. Reconciliation 设计

Reconciliation 解决 “订单是否真实成交、成交多少、资金/仓位如何变化”。

```mermaid
flowchart TD
    A["ambiguous/submitted order"] --> B["CLOB order status"]
    B -->|"truth enough"| G["verdict"]
    B -->|"not enough"| C["CLOB trades"]
    C -->|"truth enough"| G
    C -->|"not enough"| D["token balance delta"]
    D -->|"truth enough"| G
    D -->|"not enough"| E["collateral delta"]
    E -->|"truth enough"| G
    E -->|"not enough"| F["Data API positions / on-chain receipt"]
    F --> G
    G --> H{"result"}
    H -->|"filled/partial"| I["position + spent capital"]
    H -->|"not_filled/cancelled"| J["release capital"]
    H -->|"unresolvable"| K["block attribution and auto preflight"]
```

Evidence is ordered and recorded so an operator can later audit or manually resolve a case.

## 14. API 与控制面

所有成功响应：

```json
{
  "code": 200,
  "message": "ok",
  "data": {}
}
```

Async enqueue 使用 HTTP 202 和 body `code: 202`。

Protected `/api/...` routes require：

- `Authorization: Bearer <access_token>`；
- `Accept-Api-Version: v1`；
- governed mutations require `X-Acting-Role`。

控制面主要接口：

| Area | Paths |
|------|-------|
| Auth | `/api/auth/login`, `/api/auth/refresh`, `/api/auth/logout`, `/api/auth/me` |
| Runtime config | `/api/runtime-config`, `/api/runtime-config/schema`, `/api/runtime-config/versions` |
| System | `/api/system/status`, `/api/system/health`, `/api/system/quant-mode`, `/api/system/kill-switch` |
| Account | `/api/quant/account/live`, `/api/quant/account/equity-snapshots` |
| Reports | `/api/quant/reports`, `/api/quant/reports/latest`, `/api/quant/reports/run` |
| Recommendations | `/api/quant/recommendations/{id}`, `/evidence`, `/attribution` |
| Execution | `/api/quant/intents`, `/api/quant/execution-orders`, `/api/quant/positions` |
| Reconciliation | `/api/quant/reconciliations`, `/api/quant/reconciliations/{id}/resolve` |
| Settlement | `/api/quant/settlement-redeems` |

## 15. Storage 设计

### 15.1 Postgres of record

Postgres 保存业务状态：

- runtime config versions and activation；
- system runtime state；
- kill switch；
- operation logs；
- market registry；
- reports；
- recommendations；
- evidence refs；
- order intents；
- execution orders；
- capital allocations；
- position ledger；
- reconciliation records；
- settlement redeem records；
- attribution；
- RBAC users/roles/permissions。

写入原则：

- idempotent writes 使用 outcome enum，而不是把 duplicate 当 generic error；
- 状态转换必须显式；
- duplicate/retry 路径保持原子性；
- operation log 记录 actor、role、reason 和 state hash。

### 15.2 ClickHouse analytics

ClickHouse 保存高频事实和研究数据：

- market data facts；
- book snapshots；
- feature materialization；
- data-quality metrics；
- training rows；
- backtest/inference analytics；
- PnL aggregation。

ClickHouse lag 会影响 data quality 和 report，但不会成为订单状态真相。

### 15.3 Redis

Redis 用于：

- JWT revocation；
- short-lived cache；
- runtime coordination；
- optional WS/session adjunct state。

Redis 不存储不可恢复的交易真相。

## 16. 安全设计

| 风险 | 防护 |
|------|------|
| 私钥泄露 | 环境变量/secret manager 注入；masked deploy view；不写 git |
| 错 funder | startup/config validation；wallet topology check |
| 账户读取失败却继续交易 | account provider fail-closed |
| 旧报告下单 | recommendation freshness + report status admission |
| 数据 stale | data-quality gate + book freshness admission |
| 人工放大仓位 | approve 只能 narrow |
| runtime 配置漂移 | immutable versions + activation audit + rollback |
| 重复提交 | atomic claim intent + idempotency + capital lock |
| venue ambiguous | reconciliation queue + held capital |
| 自动交易失控 | mode preflight + kill switch + breaker + caps |
| 提现导致资金不足 | capital/account snapshot + budget cap + operational freeze SOP |

## 17. Observability

### 17.1 Metrics

重点指标：

- process uptime / readiness；
- Gamma sync lag；
- CLOB WS reconnect count；
- book age by token/market；
- ClickHouse flush lag/errors；
- report build latency；
- empty report count by reason；
- recommendations count；
- admission outcomes by check；
- intent states；
- execution order states；
- reconciliation pending age；
- capital reserved/spent/released/impaired；
- exit monitor trigger count；
- settlement redeem failures；
- runtime-config activation failures；
- kill switch state changes；
- breaker trips；
- account snapshot failures。

### 17.2 Logs

生产日志必须能按这些字段检索：

- `request_id`；
- `actor` / `acting_role`；
- `report_id`；
- `recommendation_id`；
- `order_intent_id`；
- `execution_order_id`；
- `market_id` / `token_id`；
- `runtime_config_version_id`；
- `quant_runtime_mode`；
- `kill_switch_state`；
- `admission_check_id`；
- `reconciliation_id`。

不得记录 private key、JWT secret、relayer key、完整 auth header、PII。

## 18. Failure-mode matrix

| Failure | Detection | System behavior | Operator action |
|---------|-----------|-----------------|-----------------|
| Missing private key | startup validation | fail start / fail mode preflight | inject secret |
| Missing funder | startup/account validation | fail report generation | set correct funder |
| CLOB auth fail | bootstrap/client connect | no account/execution | check key/topology/CLOB |
| Gamma/Data lag | health/data quality | empty report or admission deny | freeze entries, wait/repair |
| Book stale | data quality/admission | deny/defer | wait for WS recovery |
| Budget exhausted | portfolio planner | empty report/rejected candidates | deposit, close positions, adjust caps |
| Allowance insufficient | venue submit/reconciliation | order failure/ambiguous | perform approval, reconcile |
| Ambiguous venue response | submit result | capital held | wait for reconciliation |
| Unresolvable recon | recon worker | blocks auto preflight/attribution | manually resolve with evidence |
| Runtime config bad | activation validation | reject/revert | fix patch or rollback |
| Model degraded | quality gate/report | no positive signal/empty | rollback model/factor |
| Breaker daily loss | breaker | trip kill switch | incident review |
| Bridge deposit stuck | account mismatch | funds unavailable | check bridge status/support |
| Withdrawal during execution | account/capital mismatch | admission deny or impaired capital | freeze, reconcile, lower budget |

## 19. Deployment lifecycle

```mermaid
flowchart TD
    A["Provision infra"] --> B["Inject deploy secrets"]
    B --> C["Start quant-pivot in report_only"]
    C --> D["Health/account checks"]
    D --> E["Activate conservative runtime-config"]
    E --> F["Generate reports"]
    F --> G["Shadow review"]
    G --> H["semi_auto"]
    H --> I["Governed small orders"]
    I --> J["Reconciliation/attribution review"]
    J --> K["Enable auto policy"]
    K --> L["auto_execution with tight caps"]
    L --> M["Scale caps only after evidence"]
```

Scaling production is a governance decision, not a code toggle. Increase budget/caps only after reports, orders, reconciliation, exits and attribution have worked at smaller size.

## 20. 设计边界与当前限制

1. 只支持 Polymarket，不存在 multi-venue abstraction。
2. 当前 wallet topology 只支持 `eoa`、`proxy`、`gnosis_safe`。
3. Report sizing 使用真实账户；没有配置预算模拟 fallback。
4. 当前 order intent kind 是 buy entry；exit order 由 execution/exit subsystem 管理。
5. 手动在 Polymarket UI 下单不会自动生成系统内 `OrderIntent`。
6. runtime-config v9 拒绝 unknown fields；旧 schema 不能激活。
7. `auto_execution` 仍受 admission、kill switch、capital 和 breaker 限制。
8. ClickHouse 是 analytics plane，不是订单 truth。

## 21. 设计验收清单

任何新增功能或改动必须保持：

- report-first artifact；
- account truth credential-gated；
- deploy config 与 runtime-config 分离；
- runtime-config versioned and auditable；
- mode transition preflight；
- kill switch fail-closed；
- admission before every venue submit；
- capital reservation atomic with intent/order transitions；
- reconciliation before final attribution；
- no `f64` money in production paths；
- no untyped internal error bucket in production paths；
- no private key or secret in logs, docs examples, tests or commits。
