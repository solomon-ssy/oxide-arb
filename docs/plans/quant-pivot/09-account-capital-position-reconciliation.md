# 09 — 账户、资本、持仓与对账设计

> 状态：生产级目标设计（设计先行，实现分相位）
>
> 范围：让 TopN 报告的 sizing 与执行闭环建立在真实/受治理的资本基数与当前持仓之上，并定义资金状态机与对账证据链。
>
> 兼容策略：零兼容。旧 `balance_snapshot`/`position`/`reconciliation_report`/`accounting_period` 表已被 [02](02-crate-refactor-and-deletion-plan.md) 删除，本文定义的是全新 quant 语义。

## 0. 为什么需要这一层

报告必须回答“买多少”（[04](04-topn-report-and-recommendation.md) §9 Sizing），执行必须回答“能不能下、占用多少资本”（[05](05-execution-risk-and-governance.md) §4/§7/§9/§11）。两者都要求一个统一的输入：

- **资本基数**（capital base）：可用于建仓的资金总量。
- **当前持仓与敞口**（positions / exposures）：用于净额、避免超配、计算 `*_exposure_after_usd`。
- **资金状态机**（capital allocation）：planned → allocated → locked → spent → released/impaired。
- **对账证据链**（reconciliation）：让内部账本与场内真值一致，`unresolvable` 阻断 auto。

当前 `build_report` 的 `portfolio_planner.plan(budget, constraints)` 只拿到配置预算，拿不到真实余额与持仓——这是闭环缺口。本文消除它。

## 1. 核心抽象：`AccountSnapshot` + `AccountProvider`

planner 与 admission **统一消费** `AccountSnapshot`，与运行模式解耦。

```rust
pub struct AccountSnapshot {
    pub as_of: DateTime<Utc>,
    pub source: AccountSource,           // Configured | Polymarket
    pub equity_usd: Usd,                 // 资本基数（可建仓上限的总锚）
    pub available_usd: Usd,              // 可用现金（pUSD 余额 − 已锁定）
    pub reserved_usd: Usd,               // 被 pending intent / open order 锁定
    pub positions: Vec<PositionSnapshot>,
    pub exposures: ExposureBreakdown,    // per market / event / category 聚合
}

pub struct PositionSnapshot {
    pub token_id: TokenId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub category: MarketCategory,
    pub side: RecommendationSide,
    pub shares: Shares,
    pub avg_price: Price,
    pub cur_price: Price,
    pub value_usd: Usd,
    pub unrealized_pnl_usd: Usd,
    pub source: AccountSource,
}

pub enum AccountSource { Configured, Polymarket }

pub trait AccountProvider: Send + Sync {
    async fn snapshot(&self, as_of: DateTime<Utc>) -> QuantResult<AccountSnapshot>;
}
```

两个 provider，按模式注入：

- **`ConfiguredAccountProvider`（report_only）**：`equity_usd = runtime_config.portfolio.budget`，`available_usd = equity − reserved`，positions 默认空（可选：配置了**公开 funder 地址**时用 Data API 拉真实持仓做敞口净额，仍**免私钥**）。确定性、可作 shadow、report_only 不加载任何私钥。
- **`PolymarketAccountProvider`（semi_auto / auto_execution）**：`available_usd` = CLOB V2 `get_balance_allowance(COLLATERAL)`（pUSD，6 位小数）；positions = Data API `GET https://data-api.polymarket.com/positions?user=<funder>`；与内部 `quant_position` 账本对账后得出 equity/exposure。

### 1.1 资本基数政策（已拍板）

report_only 的资本基数 = 配置 `portfolio.budget`（免私钥、确定性）。真实余额仅在执行模式下、且凭证就绪（[05](05-execution-risk-and-governance.md) §1.3 preflight）时通过 CLOB 读取。

## 2. planner 签名升级

```rust
pub struct PortfolioPlanInput {
    pub candidates: Vec<SignalCandidate>,
    pub account: AccountSnapshot,        // 新增
    pub budget: PortfolioBudget,
    pub constraints: PortfolioConstraints,
}
```

sizing 对 `account.available_usd` 与现有 `account.exposures` 做净额，产出 [04](04-topn-report-and-recommendation.md) §9 的 `market_exposure_after_usd` / `event_exposure_after_usd` / `category_exposure_after_usd` 与 `binding_constraint`。

> 实施要点：planner 接口在 Phase 4 一出生即消费 `AccountSnapshot`（即使 report_only 注入 `ConfiguredAccountProvider`），避免后续返工。

## 3. Polymarket 能力对照（CLOB V2 / pUSD，2026-04-28 起）

| 数据 | 来源 | 鉴权 | 说明 |
|---|---|---|---|
| 持仓 | Data API `GET /positions?user=<funder>` | 免鉴权（公开地址即可） | `asset(token)`/`conditionId`/`size`/`avgPrice`/`curPrice`/`cashPnl` |
| 抵押余额(pUSD) | CLOB `get_balance_allowance(COLLATERAL)` | 需 API 凭证（L2 auth，源于私钥） | wei，6 位小数 |
| 条件代币余额 | CLOB `get_balance_allowance(CONDITIONAL, token_id)` | 需凭证 | 单 token 持有量 |

### 3.1 façade

```rust
pub trait PolymarketAccountClient: Send + Sync {
    async fn available_collateral(&self) -> QuantResult<Usd>;             // CLOB pUSD
    async fn positions(&self, funder: &str) -> QuantResult<Vec<VenuePosition>>; // Data API
}
```

研究/报告/执行代码禁止直接依赖 SDK raw types；只经 façade。

## 4. 持久化模型（新表，全新 quant 语义）

- `quant_position`：当前持仓账本（由执行 fills + 对账派生；report_only 为空）。
  - 键：`(token_id)`；字段：market/event/category、side、shares、avg_price、cost_usd、source、updated_at。
- `quant_capital_allocation`：每 `order_intent` 的资金状态机。
  - 状态：`planned → allocated → locked → spent → released | impaired`（[05](05-execution-risk-and-governance.md) §9）。
  - 字段：order_intent_id、recommendation_id、planned_usd、locked_usd、spent_usd、state、reason、timestamps。
- `quant_reconciliation`：每 `execution_order` 的对账证据链与结果。
  - 证据顺序（[05](05-execution-risk-and-governance.md) §11）：CLOB order status → CLOB trades → token balance delta → account balance delta → book context → operator note。
  - 结果：`filled | not_filled | partially_filled | cancelled | unresolvable`；`unresolvable` 阻断 auto execution 直到人工处理。
- `quant_account_snapshot`：报告/执行决策时刻的资金与持仓快照，进 report evidence（可复现“按 $X 配置预算 / 真实 equity $Y 计算”）。

ClickHouse 镜像（可选）：`quant_execution_event` 已覆盖执行流水；资金/持仓快照可另设 fact 供分析。**铁律**：PG 为 source of truth，先持久化业务状态（await 成功）→ 再 enqueue CH 镜像 fact（fire-and-forget）。

## 5. report 证据回填

报告 header/summary 增加（[04](04-topn-report-and-recommendation.md) §2/§3/§13）：

- `account_source`（configured / polymarket）
- `capital_base_usd`（= equity_usd）
- `account_snapshot_ref`（指向 `quant_account_snapshot`）

使每条 recommendation 的 sizing 可审计、可回放。

## 6. 分相位实施

| 能力 | 相位 | 说明 |
|---|---|---|
| `AccountSnapshot`/`AccountProvider`/planner 签名 | **Phase 4 设计定稿** | 接口先行，report_only 用 `ConfiguredAccountProvider` |
| `ConfiguredAccountProvider` | **Phase 4** | 配置预算、免私钥 |
| `quant_position`/`quant_capital_allocation`/`quant_reconciliation`/`quant_account_snapshot` 表 | **Phase 5** | schema-first（迁移 + idens + entities + repo） |
| `PolymarketAccountClient` + `PolymarketAccountProvider` | **Phase 5** | 真实余额/持仓 |
| 对账 worker + capital allocation 状态机 + exit monitor | **Phase 5/6** | 与执行闭环一起 |

> 本文仅为设计；不在 Phase 2 cutover 中实现任何账户/持仓/对账代码。

## 7. 生产不变量

- report_only 永不读取私钥；资本基数来自配置预算。
- 资金状态必须可恢复；恢复失败则执行 fail closed，报告可继续（[05](05-execution-risk-and-governance.md) §9）。
- `unresolvable` 对账阻断 auto execution。
- 余额/持仓读取失败必须降级（执行模式拒绝新入场，报告层可用配置预算继续）。
- 所有资金相关数值使用 `Usd`/`Price`/`Shares` newtype，`f64` 不泄漏到 money domain。
