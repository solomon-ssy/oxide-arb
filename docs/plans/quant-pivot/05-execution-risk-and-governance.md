# 05 — 执行、风险与治理设计

<!-- quant-pivot-deployment-contract:v1 -->
> **Deployment contract**
> - `fresh_boot_assumption`: 项目尚未正式生产上线，将从全新 `boot` / schema version `1` 部署；仓库和数据库不保存 lifecycle seal 状态。
> - `schema_data_version_impact`: 本文中的历史版本号与递增路径不再具有实施效力；当前实现不迁移测试数据、旧结构或旧版本。
> - `pre_deployment_behavior`: 允许 clean-break、migration squash 与全新基础设施 bootstrap，但任何数据销毁仍需操作者单独授权。
> - `post_deployment_behavior`: 本次实现只交付唯一终态 clean-install contract；不设计升级、降级、旧版本共存或历史数据转换。
> - `rollback_and_data_verification`: 仅在 disposable 空基础设施执行 fresh-install 验证；任何真实数据重置需要操作者另行授权。

> 状态：生产级目标设计
>
> 目标：用 Recommendation 驱动的新执行闭环替换旧 FOK Endgame trade pipeline。

> **Cross-Route risk contract**：报告风险来自同一 `GlobalPortfolioPlan`，其输入是各 Route 的
> `ExecutableEconomicTier` 与联合 `PortfolioScenarioArtifact`。执行 admission 只能收紧冻结的经济层级，
> 不能以 raw score/confidence、per-candidate Kelly、Pearson/category proxy 或 solver fallback 重算报告。

## 0. 总原则

- 执行是报告之后的可选消费路径，不是系统主路径。
- `report_only` 永不创建订单。
- `semi_auto` 必须人工审批。
- `auto_execution` 必须经过 policy approval 和 admission gate。
- 所有执行都从 `OrderIntent` 开始，不允许跳过 intent 直接下单。
- Exit plan 是 position lifecycle 的一部分，不能依赖人工记忆。

## 1. Runtime Mode

### 1.1 `QuantRuntimeMode`

```text
ReportOnly
SemiAuto
AutoExecution
```

删除旧：

```text
DryRun
Paper
Live
```

新模式不是旧模式改名：

- `report_only` 不是 DryRun；它完全不创建订单意图。
- `semi_auto` 不是 Paper；它可以真实下单，但必须人工审批。
- `auto_execution` 不是 Live；它以 Recommendation 和 RiskEnvelope 为中心。

### 1.2 模式转换

允许转换：

- `report_only -> semi_auto`
- `semi_auto -> report_only`
- `semi_auto -> auto_execution`
- `auto_execution -> semi_auto`
- `auto_execution -> report_only`

禁止直接：

- `report_only -> auto_execution`

必须先经过 semi_auto shadow period。

### 1.3 模式切换 preflight

进入 `semi_auto` 需要：

- Polymarket order client ready。
- credentials loaded。
- JWT signing key 是 Base64URL-no-pad 编码的 32 个随机字节。
- active model published or candidate approved for semi-auto。
- data quality green。
- no blocking recovery state。
- risk envelope config valid。

进入 `auto_execution` 需要额外：

- published model。
- shadow period complete。
- quality gates fresh。
- operator approval with reason。
- auto policy limits set。
- kill switch closed。
- max capital budget set。
- exit monitor healthy。

## 2. OrderIntent

### 2.1 作用

`OrderIntent` 是 recommendation 和真实订单之间的唯一桥梁。它冻结：

- recommendation id。
- runtime mode。
- entry plan。
- exit plan。
- sizing plan。
- risk envelope。
- config version。
- model version。
- approval state。

### 2.2 状态机

```mermaid
flowchart TD
    Draft["Draft"] --> PendingApproval["Pending Approval"]
    Draft --> ApprovedByPolicy["Approved By Policy"]
    PendingApproval --> Approved["Approved"]
    PendingApproval --> Rejected["Rejected"]
    PendingApproval --> Expired["Expired"]
    ApprovedByPolicy --> AdmissionPending["Admission Pending"]
    Approved --> AdmissionPending
    AdmissionPending --> Submitted["Submitted"]
    AdmissionPending --> AdmissionRejected["Admission Rejected"]
    Submitted --> PartiallyFilled["Partially Filled"]
    Submitted --> Filled["Filled"]
    Submitted --> Cancelled["Cancelled"]
    Submitted --> Failed["Failed"]
    PartiallyFilled --> Filled
    PartiallyFilled --> Cancelled
```

### 2.3 创建规则

`report_only`：

- 不创建 intent。

`semi_auto`：

- 可以从 recommendation 创建 `PendingApproval` intent。
- 创建 intent 不等于批准。
- intent 必须有 `expires_at`。

`auto_execution`：

- policy 可以创建 `ApprovedByPolicy` intent。
- policy approval 必须记录 policy id、policy hash、reason。

## 3. Approval

### 3.1 人工审批

审批请求：

- acting role。
- operator id。
- reason。
- max allowed USD。
- optional override note。

禁止：

- 修改 recommendation 核心字段。
- 修改模型输出。
- 放宽 risk envelope。
- 延长 recommendation valid_until。

允许：

- 降低 size。
- 降低 limit price。
- 拒绝。
- 要求重新生成报告。

### 3.2 审批失效

以下情况审批立即失效：

- recommendation expired。
- report revoked。
- model version retired。
- runtime config version changed in money-critical path。
- risk envelope hash mismatch。
- data quality degraded below threshold。
- kill switch opened。

## 4. Execution Admission

Admission 不是旧 `RiskPipeline` 的兼容层，而是新执行前置门。

### 4.1 输入

> `account snapshot` / `current exposure` / `filled positions` / `account balance` 的统一类型与唯一 `VenueAccountProvider`（credential-gated 真实账户，**所有 mode 一致**；`report_only` ≠ dry-run，同样读真实余额/持仓，凭证缺失则 fail closed）见 [09 — 账户、资本、持仓与对账设计](09-account-capital-position-reconciliation.md)。

- `OrderIntent`
- latest book snapshot。
- account snapshot。
- active runtime config v3。
- active risk state。
- active kill switch。
- current exposure。
- recommendation evidence refs。

### 4.2 Checks

必须按固定顺序：

1. `IntentStateCheck`
2. `RecommendationFreshnessCheck`
3. `ReportStatusCheck`
4. `RuntimeModeCheck`
5. `ModelPublicationCheck`
6. `DataQualityCheck`
7. `BookFreshnessCheck`
8. `EntryTriggerCheck`
9. `RiskEnvelopeHashCheck`
10. `CapitalBudgetCheck`
11. `MarketExposureCheck`
12. `EventExposureCheck`
13. `CategoryExposureCheck`
14. `LiquidityDepthCheck`
15. `SlippageCheck`
16. `ManualBlockCheck`
17. `KillSwitchCheck`
18. `VenueGuardCheck`
19. `CredentialReadinessCheck`
20. `ExitMonitorReadinessCheck`

任一 hard check fail，拒绝执行。

### 4.3 输出

`AdmissionDecision`：

- `allow`
- `deny`
- `defer`

必须包含：

- check trace。
- threshold。
- actual value。
- elapsed time。
- state version。
- denial reason。

## 5. Entry Order

### 5.1 Order Type

第一版支持：

- aggressive BUY：由冻结 tier 明确选择 FOK/FAK 与 worst-price cap；
- passive BUY：post-only GTD limit；
- cancel-on-timeout。

不再默认 FOK-only。FOK 是 execution tactic，不是产品架构。

### 5.2 Entry Lifecycle

状态：

- `planned`
- `submitted`
- `accepted`
- `partially_filled`
- `filled`
- `cancel_requested`
- `cancelled`
- `failed`
- `ambiguous`

`ambiguous` 进入 reconciliation，但不自动计为失败。

### 5.3 Passive post-only 闭环

- passive 只能来自 MILP 已选择的独立 passive tier；post-only reject、quote stale、market state 变化、
  recommendation invalidation 或 GTD 到期均取消剩余订单，不自动降级为 aggressive；
- 提交期间为 requested shares 的完整 limit notional 保留资金；每笔 authenticated partial fill 原子写入
  append-only fill ledger、结算对应预留并创建可立即被 exit monitor 管理的 lot；
- 剩余订单继续存活到 full fill、cancel 或 expiry；cancel/expiry 重放必须幂等并释放未使用预留；
- liquidity role 只接受 authenticated trade truth。GTC/GTD/post-only 是订单意图，不足以证明 maker。

历史 passive evidence 只来自 canonical `quant_book_l2_ledger` 的完整 `LastTrade`。同 session 内只有 opposing
SELL print 可消耗 passive BUY 的 queue ahead；gap、reset、乱序、重复冲突、缺 side/size 或跨 session 均为
coverage failure。L2 cancel 不减少 queue ahead，因为无法证明取消排在本订单之前。replay 必须持续到 requested
shares 全部成交或 GTD 到期，保存所有 partial-fill slices；cohort 未达到 95% OOS evidence coverage 不得发布。

### 5.4 Fee measurement 与 incentive ledger

即时费用、延迟 maker incentive 与实际 venue credit 分账：

- `MarketFeeSchedule` 只保存 CLOB 权威即时 fee；`MarketMakerRebateSchedule` 只保存 Gamma 权威 maker
  incentive，两者使用独立 source/content hash；同 decision boundary 下 fee 开关或曲线不一致则拒绝 candidate；
- `PreparedExpected` 与 `AuthenticatedTradeDerived` 都是 provisional measurement；只有校验 chain 137、V2
  exchange、order/account/token/side、transaction/log identity 及 BUY/SELL asset conservation 的
  `OnChainSettled` 才是 exact fee；
- 实际 maker fill 才创建 `EstimatedAccrual`，金额为
  `shares × platform_rate × (price × (1-price))^exponent × rebate_rate`；无成交严格为零，且不进入
  `cash_outlay`、hard reservation、max loss 或 spendable balance；
- daily `/rebates/current` 记录 market/day `VenueAwarded`；Data API `MAKER_REBATE` / `TAKER_REBATE`
  activity 记录 `WalletCredited`。只有 wallet credit 进入账户 incentive-credit cash component；Taker rebate
  从不进入 recommendation、MILP、CPCV 或资金可用量。

所有 fill、fee measurement 与 incentive event 都是 append-only、identity-keyed、PIT 可查询事实。venue award
的同一 account/market/day partition 允许以新 evidence identity 追加修订，读取时按 `available_at` 选择最新事实；
不得修改旧 recommendation economics 或反向伪造 taker order attribution。

## 6. Exit Lifecycle

### 6.1 Exit Monitor

每个 filled intent 必须注册 exit monitor：

- price monitor。
- time monitor。
- signal invalidation monitor。
- kill switch monitor。
- manual action monitor。

### 6.2 Exit Actions

- `take_profit_exit`
- `stop_loss_exit`
- `time_exit`
- `partial_exit`
- `signal_invalidated_exit`
- `manual_exit`
- `settlement_hold`

### 6.3 Exit 状态

- `not_started`
- `monitoring`
- `triggered`
- `order_submitted`
- `partially_exited`
- `exited`
- `failed`
- `manual_required`

### 6.4 强制退出

以下情况必须强制退出或要求人工处理：

- kill switch emergency。
- risk envelope breached。
- stop loss triggered。
- model invalidated signal。
- market status abnormal。
- data stale beyond exit threshold。

## 7. Portfolio Risk

### 7.1 报告层风险

Portfolio planner 负责：

- total capital cap。
- capital reserve 与逐 time-bucket locked-capital cap。
- per market cap。
- per event cap。
- per category cap。
- per Route cap。
- liquidity cap。
- profit-probability lower-bound admission。
- maximum scenario loss、CVaR 与 drawdown hard caps。
- market/event/outcome structural dependence。

跨 Route 依赖由不可变联合 scenario artifact 表达，不使用历史 Pearson 或 category proxy。组合只接受
唯一 HiGHS MILP 的 optimal + exact-verified 结果；任何 optimizer degradation 令 report run 失败。

### 7.2 执行层风险

Execution admission 负责：

- current exposure。
- pending intents。
- filled positions。
- actual book liquidity。
- account balance。
- venue health。
- trigger validity。

报告层风险不能替代执行层风险；执行层风险也不能修改报告，只能 allow/deny/defer。

## 8. Kill Switch

Kill switch 状态：

- `closed`
- `report_only_forced`
- `execution_halted`
- `exit_only`
- `emergency_halted`

行为：

| 状态 | 报告 | 新入场 | 出场 |
|---|---|---|---|
| `closed` | 允许 | 允许 | 允许 |
| `report_only_forced` | 允许 | 禁止 | 允许 |
| `execution_halted` | 允许 | 禁止 | 禁止自动，人工可处理 |
| `exit_only` | 允许 | 禁止 | 允许 |
| `emergency_halted` | 可降级 | 禁止 | 按 emergency policy |

### 8.1 自动触发（ExecutionBreaker）

kill-switch 除人工切换外，由**执行熔断器** `ExecutionBreaker`（设计见
[phase-05/05.4 §6.5](phase-05/05.4-entry-execution-and-venue-submission.md)）在运行时安全信号恶化时
**自动收紧**。kill-switch 仍是唯一权威运营态；breaker 是其自动触发器，所有升级经 `KillSwitchControl`
落库 + op-log + status/WS 广播（`actor = "system:execution_breaker"`）。自动 trip 维度：

| 维度 | 升级动作 | feed 落地 |
|---|---|---|
| venue 连续失败 / error-rate 击穿 | `execution_halted`（latch，需 operator ack） | 05.4 |
| 对账 `unresolvable` | `execution_halted`（latch） | 05.5 |
| 日内已实现亏损 ≥ cap | `execution_halted`（latch） | 05.6 |

- 瞬态退化（未达硬阈值）只置 `VenueHealth::Degraded` 让 admission `#18` defer，按 cooldown 自动恢复，
  **不**动 kill-switch；只有持续/硬触发才 latch 升级。
- 已被 breaker 升级的 kill-switch **不自动解除**，须 operator 经 `POST /api/system/kill-switch`
  （ack）确认（钱相关 fail-closed）。

## 9. Capital Allocation

> 资金状态机、`AccountSnapshot`、`quant_capital_allocation` / `quant_position` 表的完整设计见 [09 — 账户、资本、持仓与对账设计](09-account-capital-position-reconciliation.md)。

旧 reservation system 删除。新系统使用：

- report-level budget。
- intent-level allocation。
- execution-level locked capital。
- release on cancel/fail/exit。

状态：

- `planned`
- `allocated`
- `locked`
- `spent`
- `released`
- `impaired`

资本状态必须可恢复；恢复失败则执行 fail closed，报告可继续。

## 10. Governance

### 10.1 Governed Actions

必须受治理：

- runtime mode change。
- model publish/retire。
- factor publish/retire。
- report revoke。
- order intent approve/reject。
- auto execution policy enable。
- kill switch reset。
- manual market block。
- risk budget increase。

### 10.2 Audit Fields

每个治理动作：

- actor。
- acting role。
- action。
- target id。
- reason。
- before hash。
- after hash。
- request id。
- created_at。

### 10.3 RBAC

审批权限由 HTTP API 层 Casbin RBAC 强制执行（`ResourceType::OrderIntent` × `Operation`），
不在 runtime-config 或 report payload 中携带角色名。内置角色（seed）：

- `viewer`: 只读报告 / intents。
- `analyst`: 触发 ad-hoc report / backtest。
- `risk_owner`: 调整 runtime config / 发布模型 / revoke 报告 / reject intents（不可 approve）。
- `operator`: mode switch / kill switch / create / approve / cancel intents。
- `admin`: manage users/config。
- `emergency_operator`: halt / emergency。

## 11. Reconciliation

> `quant_reconciliation` 表、证据链与 `PolymarketAccountClient` 数据源见 [09 — 账户、资本、持仓与对账设计](09-account-capital-position-reconciliation.md)。

新 reconciliation 只服务 execution order，不服务旧 trade。

证据顺序：

1. CLOB order status。
2. CLOB trades。
3. token balance delta。
4. account balance delta。
5. book context。
6. operator note。

结果：

- `filled`
- `not_filled`
- `partially_filled`
- `cancelled`
- `unresolvable`

`unresolvable` 必须 block auto execution，直到人工处理。

## 12. Attribution

每个 recommendation 需要结果归因：

- 是否入场。
- 是否按计划入场。
- 入场滑点。
- 是否触发止盈。
- 是否触发止损。
- 是否按时间退出。
- realized PnL。
- missed opportunity return。
- factor contribution after outcome。

Attribution 进入训练样本。

## 13. API

新增：

- `POST /api/quant/intents`
- `POST /api/quant/intents/{id}/approve`
- `POST /api/quant/intents/{id}/reject`
- `POST /api/quant/intents/{id}/cancel`
- `GET /api/quant/intents`
- `GET /api/quant/intents/{id}`
- `GET /api/quant/execution-orders`
- `GET /api/quant/positions`
- `POST /api/system/quant-mode`
- `POST /api/system/kill-switch`

删除：

- `POST /system/mode` old execution mode semantics。
- old trade mutation routes。

## 14. Metrics

新增 metrics：

- `quant_order_intents_created_total`
- `quant_order_intents_approved_total`
- `quant_order_intents_rejected_total`
- `quant_admission_denied_total`
- `quant_execution_orders_submitted_total`
- `quant_execution_fills_total`
- `quant_exit_triggers_total`
- `quant_reconciliation_unresolvable_total`
- `quant_auto_execution_halted`

## 15. 验收标准

- `report_only` 无法创建或提交订单。
- `semi_auto` 未审批无法提交订单。
- `auto_execution` 仍经过 admission gate。
- recommendation 过期后 intent 拒绝执行。
- risk envelope hash mismatch 拒绝执行。
- exit monitor 对每个 filled order 注册。
- kill switch 任一状态行为有测试。
- reconciliation unresolvable 会 block auto execution。
- 所有治理动作写 operation log。

## 16. 核心 Trait 与状态迁移伪代码

### 16.1 Mode Gate

```rust
pub trait RuntimeModeGate {
    /// Decide whether a recommendation may create an order intent.
    fn intent_policy(
        &self,
        mode: QuantRuntimeMode,
        recommendation: &Recommendation,
    ) -> IntentPolicyDecision;
}

pub enum IntentPolicyDecision {
    ReportOnly,
    RequiresApproval { approval_ttl: Duration },
    ApprovedByPolicy { policy_id: PolicyId, reason: String },
    Denied { reason: ModeDenialReason },
}
```

### 16.2 OrderIntent Service

```rust
pub trait OrderIntentService {
    async fn create_from_recommendation(
        &self,
        request: CreateIntentRequest,
    ) -> QuantResult<OrderIntent>;

    async fn approve(
        &self,
        request: ApproveIntentRequest,
    ) -> QuantResult<OrderIntent>;

    async fn reject(
        &self,
        request: RejectIntentRequest,
    ) -> QuantResult<OrderIntent>;

    async fn submit_if_admitted(
        &self,
        order_intent_id: OrderIntentId,
    ) -> QuantResult<ExecutionOrder>;
}
```

### 16.3 创建 intent 伪代码

```rust
pub async fn create_intent_from_recommendation(
    request: CreateIntentRequest,
    deps: &IntentDeps,
) -> QuantResult<OrderIntent> {
    let recommendation = deps.recommendations.get(request.recommendation_id).await?;

    if recommendation.is_expired(deps.clock.now()) {
        return Err(QuantError::RecommendationExpired);
    }

    let report = deps.reports.get(recommendation.report_id()).await?;
    report.ensure_published()?;

    let mode = deps.mode.load();
    let policy = deps.mode_gate.intent_policy(mode, &recommendation);

    match policy {
        IntentPolicyDecision::ReportOnly => Err(QuantError::ReportOnlyMode),
        IntentPolicyDecision::Denied { reason } => Err(QuantError::IntentDenied(reason)),
        IntentPolicyDecision::RequiresApproval { approval_ttl } => {
            deps.intent_repo.create_pending(NewOrderIntent::pending(
                recommendation,
                deps.clock.now() + approval_ttl,
            )).await
        }
        IntentPolicyDecision::ApprovedByPolicy { policy_id, reason } => {
            deps.intent_repo.create_policy_approved(NewOrderIntent::policy_approved(
                recommendation,
                policy_id,
                reason,
            )).await
        }
    }
}
```

### 16.4 Admission Engine

```rust
pub trait ExecutionAdmissionEngine {
    /// Evaluate all checks in deterministic order.
    async fn evaluate(
        &self,
        input: AdmissionInput,
    ) -> QuantResult<AdmissionDecision>;
}

pub struct AdmissionDecision {
    pub outcome: AdmissionOutcome,
    pub trace: Vec<AdmissionCheckTrace>,
    pub state_version: StateVersion,
}
```

Admission 伪代码：

```rust
pub async fn submit_if_admitted(
    order_intent_id: OrderIntentId,
    deps: &ExecutionDeps,
) -> QuantResult<ExecutionOrder> {
    let intent = deps.intent_repo.get_for_update(order_intent_id).await?;
    intent.ensure_submittable(deps.clock.now())?;

    let input = deps.admission_input_builder.build(&intent).await?;
    let decision = deps.admission.evaluate(input).await?;

    match decision.outcome {
        AdmissionOutcome::Allow => {}
        AdmissionOutcome::Deny => {
            deps.intent_repo.mark_admission_rejected(intent.id(), decision.trace).await?;
            return Err(QuantError::AdmissionDenied);
        }
        AdmissionOutcome::Defer => {
            // Transient — retry later; intent stays submittable (never terminal-reject).
            return Err(ExecutionError::AdmissionDeferred { reason: ... }.into());
        }
    }

    let order = deps.execution_order_repo.create_entry_order(&intent).await?;
    let venue_result = deps.polymarket_orders.submit(order.to_venue_order()).await;

    deps.execution_order_repo.record_submission_result(order.id(), venue_result).await
}
```

### 16.5 Exit Monitor Trait

```rust
pub trait ExitMonitor {
    /// Evaluate exit rules for one active position/order intent.
    async fn evaluate(
        &self,
        input: ExitMonitorInput,
    ) -> QuantResult<ExitDecision>;
}

pub enum ExitDecision {
    Hold { next_check_at: DateTime<Utc> },
    SubmitExitOrder { reason: ExitReason, order: ExitOrderSpec },
    RequireManualReview { reason: ExitReason },
}
```

Exit 优先级伪代码：

```rust
fn decide_exit(input: &ExitMonitorInput) -> ExitDecision {
    if input.kill_switch.requires_emergency_exit() {
        return emergency_exit(input);
    }
    if input.stop_loss_triggered() {
        return stop_loss_exit(input);
    }
    if input.signal_invalidated() {
        return signal_invalidation_exit(input);
    }
    if input.time_exit_due() {
        return time_exit(input);
    }
    if input.take_profit_triggered() {
        return take_profit_exit(input);
    }
    ExitDecision::Hold { next_check_at: input.next_scheduled_check() }
}
```

## 17. Service 边界

```text
OrderIntentService
├── reads RecommendationReportRepository
├── writes OrderIntentRepository
└── writes OperationLogRepository

ExecutionDispatcher
├── reads OrderIntentRepository
├── calls ExecutionAdmissionEngine
├── writes ExecutionOrderRepository
└── calls PolymarketOrderClient

ExitMonitorService
├── reads active positions/intents
├── reads BookStore / market facts
├── writes ExecutionOrderRepository
└── writes AttributionRepository
```

禁止 handler 直接调用 Polymarket order client；必须通过 service。
