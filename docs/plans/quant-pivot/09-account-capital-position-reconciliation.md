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
    pub source: AccountSource,           // Polymarket（真实 venue；无模拟）
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

pub enum AccountSource { Polymarket }   // 收敛为单一真实来源（无模拟；保留枚举供 evidence/未来扩展）

pub trait AccountProvider: Send + Sync {
    async fn snapshot(&self, as_of: DateTime<Utc>) -> QuantResult<AccountSnapshot>;
}
```

**唯一 provider，按凭证就绪（credential-gated）启用，与运行模式正交：**

- **`VenueAccountProvider`（所有 mode）**：
  - `available_collateral` = CLOB V2 `get_balance_allowance(COLLATERAL)`（pUSD，6 位小数，需 L2 凭证 = 私钥派生）。
  - `positions` = Data API `GET https://data-api.polymarket.com/positions?user=<funder>`（**公开 keyless**，`funder` 是 Polymarket proxy 地址）。
  - `equity_usd = available_collateral + Σ position.current_value`，再 `min(portfolio.budget.total_budget_usd)`（净清算价值受治理护栏约束）。
  - `available_usd = available_collateral − reserved_usd`（可部署现金）。
  - 任一真实读路径失败 / 凭证缺失 → **`Err`，不生成报告（fail closed）**。
- **不存在** `ConfiguredAccountProvider` / `SimulatedAccountProvider`：报告强制建立在真实账户之上，**无模拟、无绿场、无配置预算冒充 equity**。

> 单一 provider 即可覆盖全部 mode：mode 只改变「报告之后能否下单」（[05](05-execution-risk-and-governance.md)），不改变资本来源。

### 1.1 资本基数政策（已拍板，纠偏）

> 纠正早期「report_only = 配置预算 / 免私钥」的误解：`report_only` 仅表示「报告是终产物、人工手动下单」，**不是 dry-run**。报告 sizing 必须建立在真实余额/持仓之上（与 [00](00-quant-pivot-architecture.md) §227、[05](05-execution-risk-and-governance.md) §36 一致）。

- **资本基数 = 真实净清算价值**（`available_collateral + Σ持仓现值`），所有 mode 一致。
- **`portfolio.budget.total_budget_usd` = 纯治理护栏**（最大可部署上限），`equity = min(真实净值, budget_cap)` 恒成立；budget **永不**充当 equity。
- **私钥用途拆分**：所有 mode 都用私钥**读**（CLOB L2 读凭证 + 抵押余额）；**签名/下单**仅 semi_auto/auto（[05](05-execution-risk-and-governance.md) §1.3 preflight）。`report_only` 因此**需要**私钥（用于读，不用于签名）。
- **凭证缺失（无私钥或无 funder）→ 报告不生成（fail closed）**，无降级模拟开关。

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

> 实施要点：planner 接口在 Phase 4 一出生即消费 `AccountSnapshot`（所有 mode 由唯一 `VenueAccountProvider` 提供真实账户），避免后续返工。

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

- `quant_position`：当前持仓**账本**（由系统执行 fills + 对账派生；report_only 因无系统下单而为空——真实持仓改由 Data API 快照读，落 `quant_account_snapshot.positions_json`，见 §4 末与 phase-04 README §5.1）。
  - 键：`(token_id)`；字段：market/event/category、side、shares、avg_price、cost_usd、source、updated_at。
- `quant_capital_allocation`：每 `order_intent` 的资金状态机。
  - 状态：`planned → allocated → locked → spent → released | impaired`（[05](05-execution-risk-and-governance.md) §9）。
  - 字段：order_intent_id、recommendation_id、planned_usd、locked_usd、spent_usd、state、reason、timestamps。
- `quant_reconciliation`：每 `execution_order` 的对账证据链与结果。
  - 证据顺序（[05](05-execution-risk-and-governance.md) §11）：CLOB order status → CLOB trades → token balance delta → account balance delta → book context → operator note。
  - 结果：`filled | not_filled | partially_filled | cancelled | unresolvable`；`unresolvable` 阻断 auto execution 直到人工处理。
- `quant_account_snapshot`：报告/执行决策时刻的真实资金与持仓快照，进 report evidence（可复现「真实 equity $Y = 抵押 + 持仓现值，受 budget 护栏 $X 约束」的 sizing）。

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
| `AccountSnapshot`/`AccountProvider`/planner 签名 | **Phase 4** | 接口先行，所有 mode 用唯一 `VenueAccountProvider` |
| `VenueAccountProvider` + `PolymarketAccountClient` façade | **Phase 4** | 真实抵押(CLOB) + 真实持仓(Data API)；credential-gated、fail closed |
| `ReservedCapitalReader`（只读聚合 pending intent） | **Phase 4** | 非完整资金 FSM 写入 |
| `quant_account_snapshot` 表 | **Phase 4** | 决策时刻快照，进 report evidence（catalog 驱动） |
| `quant_position`/`quant_capital_allocation`/`quant_reconciliation` 表 + 完整资金 FSM 写入 | **Phase 5** | fills 驱动账本、planned→spent 状态机、对账证据链 |
| 对账 worker + capital allocation 状态机 + exit monitor | **Phase 5/6** | 与执行闭环一起 |

> 本文仅为设计；不在 Phase 2 cutover 中实现任何账户/持仓/对账代码。

## 7. 生产不变量

- **报告强制建立在真实账户之上**：私钥（读 CLOB 抵押）+ funder（读 Data API 持仓）就绪才生成报告；凭证缺失 → fail closed，**无配置预算冒充、无模拟降级**。
- 私钥所有 mode 用于**读**；**签名/下单**仅 semi_auto/auto。
- 资本基数 = 真实净清算价值 `min` `portfolio.budget`（治理护栏）；budget 永不充当 equity。
- 资金状态必须可恢复；恢复失败则执行 fail closed（[05](05-execution-risk-and-governance.md) §9）。
- `unresolvable` 对账阻断 auto execution。
- **余额/持仓读取失败 → 报告不生成（fail closed）**；不静默降级、不用配置预算继续。
- 所有资金相关数值使用 `Usd`/`Price`/`Shares` newtype，`f64` 不泄漏到 money domain。
