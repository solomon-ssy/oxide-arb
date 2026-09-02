# Phase 12 / S4 独立闭环审计（Cursor Grok，2026-09-02）

> **审计日期**：2026-09-02  
> **作者通道**：本文件是 Cursor Grok 通道的独立判定，与同目录 [`2026-09-02-phase-12-s4-closed-loop-audit.md`](2026-09-02-phase-12-s4-closed-loop-audit.md)（另一模型同日写出）**不是同一份文档**。两份都是否定 12.1「已闭合」声称的日期化审计；细节、证据切分与修补优先级以各文自己的论证为准，互不覆盖。  
> **范围**：原全系统审计 §4（Feedback 闭环 / ReportOnly 人工成交 / 长结算无 MTM），以及 Phase 12 为闭合该节而冻结的执行授权、账户单写者、break-glass 恢复、recommendation 经济结果；附带判定 Phase 12 后来吸收的订单可提交性合同（原 2026-08-13 执行摘要 S4）  
> **合同**：[`docs/plans/quant-pivot/phase-12/12.0-execution-authority-account-recovery-fast-feedback.md`](../plans/quant-pivot/phase-12/12.0-execution-authority-account-recovery-fast-feedback.md)  
> **执行台账**：[`docs/plans/quant-pivot/phase-12/12.1-implementation-ledger.md`](../plans/quant-pivot/phase-12/12.1-implementation-ledger.md)（自称 `IMPLEMENTATION CLOSED`，`operational_activation_claimed=false`）  
> **前序**：[`2026-08-13-full-system-deep-audit.md`](2026-08-13-full-system-deep-audit.md) §4 / R7 / R9；[`2026-08-05-phase-11.9-w6-business-loop-reaudit.md`](2026-08-05-phase-11.9-w6-business-loop-reaudit.md)  
> **方法**：以 12.0 为唯一设计合同，以当前 `crates/` 与 `ui/` 源码为实施真相；对照 Polymarket 官方 V2 合约、UserPausable、Sports WebSocket 免责声明与官方合约地址；不把 ledger 自称 DONE 当闭合证据  
> **立场**：defect-first。可否定计划内部张力；可否定「任务做完 = 业务闭环」。不接受最小侵入、向前兼容、re-export。不把 Operational Activation、真实 venue 下单或打开 `PolicyAutomatic` 计为本阶段缺口  
> **本文地位**：日期化审计。**不是**新的实施合同。语义变更仍必须先写入 12.1 Decision Ledger，再改 12.0。本文不得被用来恢复 `QuantRuntimeMode`、`ManualExecutionOutcome`、Sports 半实现或 MTM 训练标签

---

## 0. 一句话结论

**Phase 12 的核心架构决策是对的，而且比 2026-08-13 §4 的原建议更接近生产交易系统；但不能按「完整闭环且最佳实践已落地」验收。**

该修的不是再加一条人工成交回填，而是拆掉重复的 runtime mode、把本系统变成账户单写者、给 recommendation 一条不污染 Buy 标签的可成交经济事实。这三条已经做成。Ledger 写 `IMPLEMENTATION CLOSED` 只能证明 W1–W7-05 任务做完了。

「新入场」「账户冻结」「恢复操作」「政策评估样本」四条业务环仍有可在生产把账户锁死、把资本占住、或把操作者带偏的缺口。不要什么也不做。也不要推翻 12.0 的冻结决策。

| 维度 | 判定 |
|---|---|
| 12.0 核心决策（删 mode / 拒人工回填 / MTM 独立平面 / Sports Deferred / 单写者） | **保留** |
| Implementation Closure（软件任务是否做完） | **基本同意**（带债） |
| 业务闭环（入场授权 + 账户恢复可运营 + 经济健康真挡住新入场） | **未闭合** |
| UI/UX 运营闭环 | **未达生产 desk 标准** |
| Operational Activation | **正确保持 false**，不计入缺口 |
| 已证实的资金绕过 / 无 evidence 下单 | **无 P0** |

---

## 1. 审计对象与方法

### 1.1 两个容易混在一起的「S4」

2026-08-13 全系统审计里有两个不同的 S4：

| 标签 | 位置 | 原意 | Phase 12 关系 |
|---|---|---|---|
| **§4 Feedback 闭环** | L302–L355 | ReportOnly 人工成交无法回流；长结算无 mark-to-market 快反馈 | 12.0 的主合同；本文主范围 |
| **执行摘要 S4** | 文首最高价值发现 | 订单可提交性四缺口（tick / size / min / allowance） | 被 W7-04 吸收进 12.0 §2.4 `PolymarketOrderRules`；本文附带判定，不作为 §4 原文否决项 |

本文标题里的 S4 指 **§4 Feedback 闭环及其 Phase 12 替换合同**。订单数学只在 §8 作附带结论。

### 1.2 只认的实施真相

- 设计：12.0（`design_contract_hash` 以 12.1 checkpoint 为准）
- 状态：12.1 Evidence / Decision Ledger
- 代码：`crates/`、`ui/` 活跃源码
- **不认**：聊天记录、日期化审计正文、Phase 11 旧 mode 语义、ledger 任务表上的 DONE 字样（DONE 只证明当时跑过某条命令）

### 1.3 外部交叉验证

| 主题 | 来源 | 结论 |
|---|---|---|
| `pauseUser` / `userPauseBlockInterval` / `isUserPaused` | [UserPausable.sol](https://github.com/Polymarket/ctf-exchange-v2/blob/main/src/exchange/mixins/UserPausable.sol) | pause 不是立即生效；`effectivePauseBlock = block.number + interval`（默认 100）；`unpauseUser()` 把 mapping 置 0，可取消 pending pause |
| pause 拦的是谁 | V2 `Trading._validateOrder`：`require(!isUserPaused(order.maker), UserIsPaused())` | **每张订单的 `order.maker`**。Taker FAK 的 maker 仍是本账户，pause 生效后立即成交型退出也会 revert |
| 链上没有单笔撤单 | [Quantstamp V2 report](https://certificate.quantstamp.com/full/polymarket-ctf-exchange-v-2/a1376c48-8551-4568-a1ed-d5e499061477/index.html) | 紧急制动就是 `pauseUser()` + 延迟窗；CLOB cancel 仍是 off-chain |
| 官方 V2 交易所 | [Contracts](https://docs.polymarket.com/resources/contracts) | 仅 CTF `0xE111180000d2663C0091e4f400237545B87B996B` 与 NegRisk `0xe2222d279d744050d28e00520010520000310F59`。代码 `EXCHANGE_CONTRACTS` 与此一致 |
| Sports WS | [官方 Real-Time Data / Sports Stream](https://docs.polymarket.com/market-data/websocket/sports) | 原文：may be delayed / contain errors / omit recent events；**should not be used as the basis for a trading decision**。12.0 Deferred 正确 |
| Data API / User Channel | [Get trades](https://docs.polymarket.com/api-reference/trade/get-trades)、[Manage Orders](https://docs.polymarket.com/trading/manage-orders) | 成交可按 wallet 读；未成交订单生命周期按 **API credential / Session Key** 过滤。跨 credential 挂单无法从本系统凭证证明消失。单写者决策正确 |

本次**没有**重跑全 workspace 测试、Browser E2E 或 production-stack feedback-closure。闭合判定建立在源码路径、schema enum、UI 组件与官方协议上，不建立在「W7-05 PASS」复述上。

---

## 2. 原 §4 诉求 vs 实际落地

### 2.1 §4.1 ReportOnly 人工成交无法回流

原审计（L308–L334）认定：默认 `ReportOnly` 永不创建 intent；execution reconciliation 以 `order_intent_id` 为唯一锚点，因此 Polymarket UI 下手动单永远不会产生 `ExecutionAttemptOutcome`。建议增加 operator-attested `ManualExecutionOutcome`。

Phase 12 的替换：

- 删除整个 `QuantRuntimeMode` 轴
- 默认 `EntryAuthorizationPolicy::OperatorApprovalRequired`
- 不创建 intent、不批准 intent 就不会下单，因此不必靠全局 ReportOnly 才能安全观察报告
- 未知 finalized 账户成交原子创建/复用 `AccountRecoveryIncident`，latch `ExitOnly`
- **禁止** `ManualExecutionOutcome`，禁止事后伪造 `OrderIntent`

**判定：比原建议更好的闭合。** 原 §4.1 的根因是「默认模式禁止系统成交」，不是缺一条人工录单通道。删掉 ReportOnly 之后，操作者走系统 intent 批准，执行质量可以走既有 `ExecutionTrajectoryArtifact` / `PolicyCounterfactualOutcome`。人工 UI 成交没有本系统 order hash，没有密码学/链上身份，不应进入需要证据的执行学习路径。

活跃代码扫描（`crates/` + `ui/` 的 `*.rs` / `*.ts` / `*.tsx`）：`QuantRuntimeMode`、`ReportOnly`、`mode_gate`、`ApprovalStatus`、`OrderIntentKind`、`/system/runtime-controls/quant-mode` **零命中**。`mode_gate.rs` 文件不存在。

### 2.2 §4.2 长结算 + 无 MTM 快反馈

原审计（L336–L353）认定：Buy 标签必须等 `token_payout_ratio` / 结算；Coverage 门槛使政治/宏观类目一个 feedback cycle 以季度计。建议 Sports 缩短周期，或用 horizon 可成交价作 `SettlementPending` 的降级训练标签。

Phase 12 的替换：

- `token_payout_ratio` 仍是 Buy 模型唯一终局监督目标
- `RecommendationEconomicOutcome` 是 PolicyEvaluation / operational health 的 WORM 事实，**不进入** ModelLearning、calibration、promotion label count
- 复用 `PolicyReplayKernel`；horizon 用 full-depth executable sell walk
- Sports 明确 Deferred

**判定：运营/治理面闭合；ML 面刻意未闭合。** 把 MTM 塞进 ModelLearning 会污染 Buy 监督目标，12.0 §1.3 / §5.4 的边界是对的。Sports 官方 WS 不足以支撑执行级 alpha，Deferred 是对的。政治/宏观类目的 champion 重训周期仍以结算为准——这是产品事实，不是实现遗漏。不要用「Phase 12 已闭合 feedback」掩盖它。

### 2.3 对照表

| 原缺口 | 原建议 | Phase 12 实际 | 判定 |
|---|---|---|---|
| ReportOnly 永不建 intent | 保持 ReportOnly + 人工回填 | 删除 runtime mode；默认 operator 批准仍可建/批 intent | **已闭合，且优于原建议** |
| 人工 Polymarket UI 成交成本不可见 | `ManualExecutionOutcome` | 拒绝回填；未知成交 = incident + ExitOnly | **正确拒绝原建议** |
| 标签等结算，周期以季度计 | MTM 作训练降级标签 / Sports 捷径 | 独立 WORM 经济结果只进 PolicyEvaluation；Sports Deferred | **运营面闭合；ML 面刻意未闭合** |
| 执行摘要 S4 订单可提交性 | tick/size/min/allowance | 唯一 `PolymarketOrderRules` + final-hop `/book`+`/clob-markets` + 真实 funding | **内核已落地**（附带，见 §8） |

---

## 3. 对 12.0 计划本身：背书与否定

审计可以否定计划。下面分开写。

### 3.1 明确背书，不推翻

1. **三态 runtime mode 是重复控制轴。** 报告是否可读、intent 授权来源、自动提交许可、退出自动化、settlement write policy 已经分别有 owner。默认 `OperatorApprovalRequired` 即可安全观察报告。
2. **单写者 + 未知成交当事故。** Data API 可按 wallet 读 trades；authenticated User Channel / `/orders` 按 API credential 过滤。跨 credential 未成交订单无法从本系统凭证证明消失。V2 又没有链上单笔 cancel。外部成交必须当事故，不能当手工学习样本。
3. **三阶段费用。** Prepared projected / CLOB provisional / chain exact。删除 `FeeSettlementService` 方向正确。
4. **Lot 禁止 FIFO / 均价 / 时间窗猜测。** 多 lot 部分卖必须显式分配；全量卖出可由 share conservation 唯一分配。当前 reconciler 只在「单 lot 或 `sold_shares == total_available`」时自动切分。
5. **MTM 不进 ModelLearning / calibration / promotion label count。** Horizon 可成交强平是政策评估与 Route health 的快信号，不是 `token_payout_ratio` 的替代。
6. **Implementation Closure ≠ Operational Activation。** 真实下单、relayer、打开 `PolicyAutomatic` 不该混进本阶段。

### 3.2 计划内部张力（应改合同或改实现，并记入 Decision Ledger）

1. **§4.1「保留 governed exit」与立即 `pauseUser()` 冲突。**  
   V2 对 taker/maker 订单的 `order.maker` 都查 `isUserPaused`。Pause 一生效，系统 FAK 与 UI 立即成交型退出都会 revert。计划把「UI 先出清再 pause」写成 break-glass SOP，又把未知成交的自动反应写成立刻停入场，没有写清：系统在线检测到未知成交时，pause 应晚于撤单/出清，还是接受「冻结账户、把头寸收成 recovery lot」。实现选择了立即 pause，合同没有为这个选择提供完整语义。

2. **§5.3「Degraded / DataIncomplete 阻断该 Route 全部新入场」被实现收窄成 admission。**  
   Create / approve 仍通，且 create 就会写 capital allocation。要么改合同承认「资本预留窗口，提交时再硬拒」，要么把 live health 写进 `ExecutionEligibility` 并在 create/approve 强制执行。现在是字面合同被 12.1 W4-06 私自收窄，Decision Ledger 没有对应决策。

3. **写了但没落地、也没记账删除的形状。**  
   - §4.2 FSM 含 `Rejected`；实现只有 `Open / Reconciling / Sealed`  
   - §3.3 association 含独立 `UnknownExternal` tag；实现折叠为 `RecoveryIncident` + incident kind `UnknownExternalExecution`，并另增 `OpeningInventory`  
   - §3.3「冲突进入 quarantine」；实现是二元绑定，首次 association WORM 不可逆  
   `Rejected` 缺失的后果：无法收敛的 incident 永远停在 `Reconciling`，没有治理性拒绝出口。

4. **§6 UI 合同偏薄。** 写了 journey，没写实时性、typed blocker 展示、一键撤单、Seal/Unpause 与链上动作的一一对应。按生产运营标准，这是合同缺口，不只是实现漏了文案。

5. **§2.3 Fresh route bootstrap 必须在 `ExitOnly` 且 recovery seal healthy 时进行。** 实现的 bootstrap preflight 只要求 `OperatorApprovalRequired`。字面未兑现。

---

## 4. 已经真正落地、且质量够高的部分

这些是源码核对，不是文档自称。

### 4.1 执行授权

| 检查项 | 证据 |
|---|---|
| Legacy mode 轴删除 | `crates/` / `ui/` 无 `QuantRuntimeMode` / `ReportOnly` / `mode_gate` / `quant-mode` |
| 三轴类型 | `EntryAuthorizationPolicy`、`ExecutionAuthorityCeiling`、`AuthorizationKind` / `AuthorizationEvidence`（`quant-pivot-models` enums + `execution_payload.rs`） |
| `ExecutionEligibility` 形状 | `ceiling + blockers + policy_binding`；无 `eligible_modes` / `requires_approval` / `auto_execution_allowed`（`report_payload.rs`） |
| Intent FSM | DB enum 与 12.0 §2.2 状态集一致；`PendingAuthorization` / `AuthorizationRejected` 要求 evidence NULL，其后继非 NULL（`relational_invariants.rs`） |
| Create 路径 | Operator → `PendingAuthorization`；PolicyAutomatic → 需 `allows_policy()` + binding 匹配 + active bundle → `Authorized`（`intent_service.rs` `resolve_policy`） |
| Runtime controls API | `POST /system/runtime-controls/entry-authorization-policy` 存在；`quant-mode` 已删；三字段独立 CAS（`quant-pivot-web` `routes/system.rs`） |
| PolicyAutomatic 升级 | `EntryAuthorizationPreflight` 对每条 active Buy Route 要求 fresh `Healthy`（`entry_authorization_preflight.rs`） |
| Kill switch 独立性 | 报告在 `ExitOnly` 下仍可生成（`operational_phase.rs`）；create/approve/admission 均检查 `allows_new_entry()` |
| Admission 对 Degraded | 包括 operator 也 deny（`admission/checks.rs` `EconomicHealthCheck`）；deny 时 `release_capital`（`execution_submission.rs`） |

### 4.2 账户事实与恢复内核

| 检查项 | 证据 |
|---|---|
| 三平面分离 | CH `quant_exchange_event` / `quant_market_execution` 不承担账户权威；PG `quant_account_chain_execution` 持 exact fee |
| 旧 fill/fee 删除 | 活跃代码无 `quant_execution_fill`、`FeeSettlementService`、`quant_execution_fee_measurement` |
| `ClobTradeObservation` 收窄 | 仅 authenticated CLOB 生命周期 + provisional fee |
| Chain fee fail-closed | `OrderFilled` 无 `fee_amount` 则投影错误（`account_chain_projector.rs`） |
| 角色 | maker==taker → SelfMatch；order hash 在 taker match 集 → Taker；否则 Maker |
| 官方两所 | `CTF_EXCHANGE_V2` + `NEG_RISK_EXCHANGE_V2` 与官方 Contracts 页一致 |
| 未知成交幂等 | advisory lock + 复用 Open/Reconciling incident；association replay 返回已有行 |
| Clean funder | Maker/SelfMatch 写 WORM blocker；seal 硬拒；无 ack/clear API |
| Lot 无 FIFO | 单 lot 或全量守恒才自动切；多 lot 部分卖 `LotAllocationRequired`（`account_recovery_reconciler.rs`） |
| Pause 动态 interval | `prepare_pause` 读链上 `userPauseBlockInterval`；不硬编码 100 |
| Seal 前禁止 unpause | `unpause_incident` 要求 `seal_hash`；`unpause_and_finalize` 同样检查 |
| Restart recovery-only | 启动时若有 active incident，只注册 recovery worker，不注册 entry dispatcher（`bootstrap.rs`） |
| 禁止人工回填 | 无 `ManualExecutionOutcome`；recovery lot 的 `order_intent_id` 为 `None` |

W3-04 Evidence 行曾写「SELL 执行 FIFO」。那是中间态。W6-02 已删除猜测分配。**当前生产路径无 FIFO lot 猜测。** 12.1 该历史 PASS 行容易误导后续审计，应视为过时叙述，不是现行行为。

### 4.3 经济反馈

| 检查项 | 证据 |
|---|---|
| 六态齐全 | `EntryNotTriggered` / `EntryUnfilled` / `PolicyExited` / `HorizonLiquidated` / `ResolvedBeforeHorizon` / `Censored` |
| WORM + 唯一键 | `recommendation_id` PK；append-only trigger |
| 单一 kernel | 只调用 `PolicyReplayKernel`；无第二套 MTM simulator |
| Horizon 证据 | `HorizonLiquidated` 强制 `FullBidLadder` + `full_l2_covered`（DB CHECK + domain validate） |
| 缺数据不写零 | gap → defer（cutoff 前）或 censor（cutoff 后）；censored `net_pnl_usd = None` |
| Claim 冻结 | 首次 claim 写 `replay_until` / `source_cutoff_at` / `resolution_outcome_hash`；过期 claimant 不能写 WORM |
| Capacity vs censor | `try_acquire_offline` 失败 → `CapacityDeferred`；不移动 cutoff、不写 censor |
| Health 四态 | Insufficient / Healthy / Degraded / DataIncomplete；average-uniqueness + event-cluster bootstrap |
| Feedback 边界 | PolicyEvaluation `requires_economic_outcome()`；ModelLearning 候选 `economic_outcome().is_none()`；Buy 标签仍 `token_payout_ratio` |
| Comparison API | 只派生 planned-vs-actual；引用 economic / trajectory / counterfactual 三个 hash；`NotEvaluable` 不填零 |
| 404 vs null | 未知 recommendation → 404；已知但尚无 outcome → 200 null |

### 4.4 订单数学（附带）

`PolymarketOrderRules` 被 research、admission、exit、reconciliation 共用。Admission / order client 核对 `clob_market_info_payload_hash`。相对 2026-08-13「报告能出、单下不去」，这是数量级改进。独立钱路审计仍可再做 signing 串行锁与 funding spender 的逐字段核对，但不作为本次 §4 否决项。

---

## 5. 缺口（按生产后果）

严重度约定：

| 级 | 含义 |
|---|---|
| **P0** | 已证实可绕过 kill switch、无 authorization evidence 下单、或不可逆地把系统自有资金/头寸错配成外部事故且无法 quarantine |
| **P1** | 生产路径会错误锁死账户、在合同禁止的点预留资本、污染治理样本、或让恢复旅程在真实延迟下不可运营 |
| **P2** | 合同漂移、可观测性、名实不符、UI 误导；不立刻锁死账户，但会在下一阶段变成 P1 |

本次**没有**已证实的 P0 资金绕过。下列 P1 足以否决「完整闭环验收」。

### 5.1 P1-1 经济健康没有进入「新入场」的真实边界

**合同**：§5.3 Degraded / 持续 DataIncomplete 阻断该 Route 全部新入场；§8 operator approval 不得绕过 degraded economic health。

**实现**：health 只在 admission 与 PolicyAutomatic **升级** preflight 生效。Report compose 的 eligibility 不读 health：

```917:941:crates/quant-pivot-core/src/report/composer.rs
fn execution_eligibility(
    bootstrap: bool,
    auto_allowed: bool,
    policy_snapshot_id: &DecisionPolicySnapshotId,
) -> ExecutionEligibility {
    // ceiling 只由 bootstrap / automation cap 决定
    // blockers 仅 AutomationCapExceeded
}
```

`IneligibilityReason` 目前只有 `AutomationCapExceeded`。`IntentService::resolve_policy` 对 operator 只调用 `allows_operator()`（看 ceiling，不看 blockers，不查 health）。`approval_invalidation()` 无 economic health 字段。UI `evaluateCreateIntentGate` 同样不请求 `/research/economic-health`。

Create 路径会写入 capital allocation（`reason: "intent created"`）。Admission deny 会 `release_capital`，所以不是穿仓，但是：

- 冻结报告里的硬预留，直到 dispatch
- UI 仍启用 Create
- 与合同「阻断全部新入场」字面不一致

对 PolicyAutomatic 提交，admission 门禁是够的。对默认的 `OperatorApprovalRequired` 路径不够。

**应修**：live `RouteEconomicHealth` materialize 进 `ExecutionEligibility.blockers`（新增 typed reason），create/approve 与 UI gate 共用同一规则：Degraded/DataIncomplete 硬拒；InsufficientEvidence 仍允许 operator；fresh Healthy 才允许达到 ceiling 的 automatic。

### 5.2 P1-2 Projector 用空 allocation 自动 reconcile，和操作者抢 latest manifest

```79:87:crates/quant-pivot-core/src/execution/account_chain_projector.rs
        if let Some(incident) = self
            .recovery
            .active_incident(&self.execution_account_id)
            .await?
        {
            latch_exit_only(self.kill_switch.as_ref(), &incident).await?;
            self.pause.pause_incident(&incident, &self.funder).await?;
            self.recovery_service.advance_incident(&incident).await?;
        }
```

未 seal 时 `advance_incident` 调用 `reconcile_incident(incident, Vec::new())`。每个投影 pass 都会对账。

Seal 要求 command 引用的 manifest **就是** 最新一次 reconcile 的 id（`account_recovery_service.rs` `seal_incident`）。操作者提交精确 lot 分配得到 converged M1 之后，下一次 projector pass 用空 allocation 写出未收敛的 M2。UI 面板只在 `onMounted` 拉一次，仍显示 M1。点 Seal 因 latest 已变而失败。

这是 fail-closed，不会偷偷封错账，但会把恢复做成「点了没反应 / 总是过时」。自动对账只应在 pause 未确认或 snapshot 变化时跑，且**不得在存在 `lot_allocation_required` 时用空向量覆盖 operator 输入**。

同源问题：`pause_and_reconcile` API 名含 pause，实现只 reconcile（pause 已在 projector）。Seal 在持久化 `seal_hash` 后立刻 `unpause_incident`，UI 的两步按钮与链上动作不一致（见 §6）。

### 5.3 P1-3 Association 没有 quarantine；Maker 误判会变成不可逆 clean-funder

合同 §3.3：同一 account chain execution 只能有一个 current association；冲突进入 quarantine。

实现是二元的：`venue_order_id` 命中系统单 → `SystemOrder`，否则立刻 `RecoveryIncident`。Maker / SelfMatch 立刻写 `quant_account_clean_funder_blocker`（WORM，无 clear）。首次 association 不可逆。

危险窗口：CLOB POST `Ambiguous`（timeout / 5xx / 不可解析）时 `venue_order_id` 可为 `None`，资本被 hold、走 durable recon（`order_client.rs` 明确把这种结果标为 Ambiguous）。若在 recon 发现 order id **之前**，finalized `OrderFilled` 已经投影进来，这张**自己的单**会被当成未知 Maker，incident 永久 `CleanFunderRequired`，唯一出口是换 funder + fresh boot。

链上 finality 通常比 CLOB 回包慢，所以这不是每次下单都会炸的 bug，是 **Ambiguous ∩ 发现失败 ∩ 链上事实先到** 的不可逆事故。

Taker 角色还依赖 `OrdersMatched` 是否已落入同一批。Match 滞后时，外部 FAK 会被标成 Maker，同样触发 clean-funder。Taker 豁免写在 `ensure_clean_funder` 里，因此 **match 证据的完整性是这条不可逆门的隐藏前提**。

**应修**：第三态 `PendingSystemAssociation` / quarantine。在 recon 结束、超时、或 match 证据齐备之前，禁止写 clean-funder。冲突不得 last-write-wins，也不得第一次猜错就永久锁死账户。

### 5.4 P1-4 未知成交立即 on-chain pause，且系统不会撤单

自动 `pause_incident` 对未知成交是合理的 fail-closed **意图**（Quantstamp：没有链上单笔 cancel，pause 是唯一匹配制动）。但它和合同「保留 governed exit」冲突，也和 break-glass SOP「先立即成交退出、再撤单、再 pause」冲突。

V2 语义使冲突成为硬事实，不是文档笔误：pause 生效后本账户作为 `order.maker` 的任何撮合都会 revert，包括系统 ExitOnly 想做的 FAK。

更具体的运营缺口：**recovery 把 `OpenOrdersPresent` 当成 seal blocker，却不提供取消挂单的动作。** `get_open_orders` 只用于 snapshot hash。本 credential 的 GTC **可以** CLOB cancel，不必等 100 块 pause 生效。现在操作者必须离开 Admin UI 去 Polymarket 撤单，而恢复面板既没有步骤说明，也没有挂单列表。

Pause 延迟窗里，未知 credential 的 resting 单仍可继续撮合。这是 V2 物理限制。因此更应该：

1. 检测到未知成交 → 立刻 latch ExitOnly + Critical alert + metric
2. 立刻 cancel **本 credential** 挂单
3. `pauseUser()` 作为对「无法证明的跨 credential 挂单」的制动
4. 头寸收成 recovery lot，而不是假设还能在 pause 生效后再 FAK 出清

当前实现把 2 和 4 留给操作者，又把 3 做成每个 `project_pass` 的默认动作。

### 5.5 P1-5 PolicyEvaluation 把 Censored 经济结果算成 Eligible

```112:135:crates/quant-pivot-core/src/service/feedback_cohort.rs
    let Some(economic) = visible_economic(...)? else {
        return Ok(FeedbackCohortDecision::Censored(
            CohortCensorReason::EconomicOutcomeUnavailableAtCutoff,
        ));
    };
    Ok(FeedbackCohortDecision::Eligible(
        FeedbackCohortEvidence::PolicyEvaluation { economic, ... },
    ))
```

`visible_economic` 只检查 `available_at <= cutoff` 与 hash，**不检查 `state != Censored`**。12.0 §5.2 自己写了「Eligible 不证明 non-censored 闭环」；production-stack closure manifest 另有终态 / coverage 核对。日常 PolicyEvaluation 计数仍会被 censored 样本稀释。

Route health 把无 `net_return_bps` 的 due observation 打成 DataIncomplete，但那是另一条平面。政策对比若用 Eligible 集合，统计会被脏样本污染。

**应修**：Censored 不得进入 PolicyEvaluation Eligible；或缺数据继续 Censor，有 typed censored 经济态则显式 `EligibleCensored` / 单独桶，禁止混进 health 与 bootstrap 样本。不要靠 closure manifest 的额外断言当日常不变量。

### 5.6 P1-6 恢复 UI 不是可运营的闭环

`ui/apps/web-antdv-next/src/shared/components/account-recovery-panel.vue`：

| 能力 | 现状 |
|---|---|
| 发现 incident | 嵌在 runtime-control 页；顶栏无 banner |
| 新鲜度 | 仅 `onMounted` 拉一次；无轮询、无 WS |
| Blocker | 只显示 mismatch **个数** |
| Lot 编辑 | 只有 execution UUID 与 lot UUID，无 token / 市场 / 方向 / 已实现份额 |
| 挂单 | 不列出，不提供 cancel |
| Break-glass SOP | 无「立即成交退出 → 撤单 → 等待 pause 生效」指引 |
| Seal vs Unpause | UI 两步；后端 Seal 已 dispatch unpause |
| Finalize 之后 | 不引导 kill-switch ack；不说明可能需要 restart 才恢复 entry dispatcher |

E2E（`post-trade.spec.ts`）覆盖 reconcile → seal → unpause 的 revision / body / acting-role，用的是即时 fixture。测不到 100 块 pause 延迟，测不到 projector 把 manifest 顶掉，也测不到操作者看不懂 `CleanFunderRequired`。E2E 绿不能替代运营闭环。

经济三联卡（`recommendation-economic-feedback.vue`）是只读展示：

- `listRouteEconomicHealth({ route, size: 1 })` 不绑 recommendation 冻结的 model / profile / policy identity，可能与 admission 用的 exact identity 不一致
- 不展示 censor reason、L2 / fee / passive coverage
- comparison 有 latency / price / fill / return，没有已经算出来的 fee delta
- **不把 Degraded 接到 Create 按钮**

Create gate 对 ceiling、kill switch、有效期、report/recommendation 状态是认真对齐过的；独缺 live health。这会让操作者在错误的点得到「可以下单」的信号。

---

## 6. P2 清单

| ID | 问题 | 证据 | 备注 |
|---|---|---|---|
| P2-1 | 计划 `Rejected` 终态被静默删除 | `AccountRecoveryIncidentStatus` 仅 open/reconciling/sealed | 无法治理性拒绝 incident |
| P2-2 | Association 种类与合同不一致 | `SystemOrder` / `RecoveryIncident` / `OpeningInventory` | Unknown 折叠进 incident kind 可以更好，但没记账 |
| P2-3 | 无 recovery metric | projector 只 `dispatch_background` Critical alert | 合同 §4.1 要求 alert **+ metric** |
| P2-4 | Horizon「forced FAK」名实不符 | `replay_policy_horizon` 走 policy `fill_requirement`；测试用 `AllowPartial` | 深度不足 → 部分强平 + censor。应改合同或强制 AllOrNothing |
| P2-5 | `EconomicOutcomeCensorReason::SourceLate` 未接线 | enum 有，adapter 实际写 `SourceUnavailable` 等 | dead reason |
| P2-6 | Fresh bootstrap 未强制 ExitOnly + recovery seal | `ModelRouteBootstrapPreflight::validate` 只查 operator policy | 与 §2.3 字面不符 |
| P2-7 | 同进程 finalize 后不重新注册 entry dispatcher | `bootstrap.rs` 仅启动时分流 | 可以是刻意的；UI/runbook 必须写死 |
| P2-8 | Finalize 后 kill switch 仍 latched ExitOnly | projector latch `ack: false` | 合理；恢复面板不引导 ack |
| P2-9 | UI 残留「模式」注释 / 不存在的 `is_eligible` | `entry-authorization-policy-indicator.vue`；`execution-gate.ts` | 死语义痕迹 |
| P2-10 | `policy_binding: Option<String>` | 计划是 typed `ExecutionPolicyBinding` | 弱类型化 |
| P2-11 | `order_client_ready` 升级 preflight 仍是 boot 级 | 只验 keystore + CLOB URL | PolicyAutomatic 升级无 live CLOB probe |
| P2-12 | 12.1 W3-04 PASS 行仍写 FIFO | ledger 历史证据 | 现行代码已无；易误导后续审计 |
| P2-13 | SelfMatch 单 leg + invalid，不是「两条经济 leg」 | projector 一条 `OrderFilled`；reconciler 标 invalid | 有 typed quarantine 意味，与合同字面不完全同构 |
| P2-14 | Projector 在已有 `seal_hash` 的 Reconciling incident 上仍先调 `pause_incident` | `project_pass` 不分支 | 因既有 Pause 行按 exchange 去重，通常不致重新 pause；职责过载 |

---

## 7. UI / UX 闭环判定

**控制面拆轴：合格。** 顶栏独立 `EntryAuthorizationPolicy` 与 kill switch；无 mode selector；settlement write 独立。这与 12.0 §1.1 / §2.3 一致。

**推荐入场：半闭合。** Create-intent gate 对齐 permission、policy、kill switch、ceiling、risk envelope、validity、report/recommendation 生命周期、active intent。缺 live Route health，因此默认 operator 路径会在 Degraded 时亮起可点的 Create。

**经济反馈：只读且易误导。** 404 vs null 分对了。`LatestRequestOwner` 修过 stale overwrite。但卡片不能回答「这一单现在能不能做、为什么这条 Route 被降权、censor 是缺 L2 还是缺 fee」。health 查询身份比 admission 弱。

**账户恢复：不及格。** 见 §5.6。对照生产交易 desk 的 incident UI，现在是治理 API 的表单壳：UUID、revision、三个危险按钮、一个 mismatch 计数。Pause 是按块延迟的链上事实，UI 却当同步表单。

**职责分离痕迹。** Reconcile / Seal / Unpause 用了不同 permission code（`system:emergency` / `system:resolve` / `system:resume`），这是对的。但 Seal 后端已经 unpause，resolve 权限实际上在发链上 unpause，和按钮文案不符。

---

## 8. 性能、结构、优雅性

不是本阶段主风险。下列项在修 P1 时应一并拆，而不是再往 projector 上堆状态机。

已经做得对的：

- 经济 replay 4 GiB memory lease、非排队完整准入、`CapacityDeferred` 与 source censor 分离（修过用 claim generation 做容量退避导致饥饿）
- Crypto 显式 gap generation + ordered ACK frontier + equivocation 拒绝
- Report announcement `ClaimLost`；settlement 只对 PostgreSQL `40001` / `40P01` 做四次 bounded jitter retry
- Process-owned shutdown：关 Actix 自有 signal、HTTP/WS poll 绑进程 runtime、显式 await PG close

仍然偏糙：

- `AccountChainExecutionProjector::project_pass` 同时负责投影、latch kill switch、on-chain pause、空 allocation 对账、seal 后 unpause retry。这是 P1-2 / P1-4 的结构根因
- `pause_and_reconcile` 名实不符
- 恢复评估每个 pass 双拉 CLOB + Data API 做稳定性比较，正确但贵；应变为事件驱动，而不是每个投影 batch 打满
- Horizon 文档写 FAK，实现绑 policy fill requirement（P2-4）

---

## 9. 明确不算本阶段缺口

| 项 | 理由 |
|---|---|
| Buy 模型仍等 `token_payout_ratio` | 12.0 §1.3 / §5.4 的正确边界，不是漏做 §4.2 |
| Sports Deferred | 官方 WS 免责声明足以支撑该决策 |
| Operational Activation / 真实 venue 下单 / 打开 PolicyAutomatic | 12.0 §9 排除；ledger `operational_activation_claimed=false` |
| Maker rebate 训练目标（2026-08-13 S3） | 不在 §4 / Phase 12 合同内 |
| 原执行摘要 S4 的 tick / min / allowance 作为 §4 残留 | 已被 `PolymarketOrderRules` 吸收 |
| 兼容层 / 旧数据迁移 / re-export | 项目从未生产；fresh-boot 合同禁止 |

---

## 10. 风险登记册

| ID | 级 | 摘要 | Owner 路径 |
|---|---|---|---|
| R-P12-01 | P1 | Degraded health 仍可 create/approve 并预留资本 | `intent_service.rs`、`composer.rs`、`use-create-intent-gate.ts` |
| R-P12-02 | P1 | Projector 空 allocation 自动 reconcile 顶掉 latest manifest | `account_chain_projector.rs`、`account_recovery_service.rs` |
| R-P12-03 | P1 | 无 `venue_order_id` 的链上成交可被写成不可逆 clean-funder | `account_recovery.rs` `associate_execution` / `ensure_clean_funder` |
| R-P12-04 | P1 | 未知成交立即 pause 且不撤本 credential 挂单；pause 生效后 governed exit 无法撮合 | `account_chain_projector.rs`、`account_pause.rs`、V2 `_validateOrder` |
| R-P12-05 | P1 | PolicyEvaluation Eligible 含 Censored 经济态 | `feedback_cohort.rs` |
| R-P12-06 | P1 | 恢复 UI 无实时性、无 typed blocker、无撤单、Seal 已 unpause | `account-recovery-panel.vue` |
| R-P12-07 | P2 | `Rejected` 终态缺失 | `enums/execution.rs` |
| R-P12-08 | P2 | 无 recovery metric | projector alert 分支 |
| R-P12-09 | P2 | Horizon FAK 名实不符 | `policy_replay.rs` |
| R-P12-10 | P2 | UI health 查询身份弱于 admission | `recommendation-economic-feedback.vue` |
| R-P12-11 | P2 | Fresh bootstrap 未查 ExitOnly + recovery seal | `model_route_bootstrap.rs` |
| R-P12-12 | P2 | Finalize 后同进程不恢复 entry dispatcher | `bootstrap.rs` |

---

## 11. 若要宣称闭环，最低补齐

顺序按生产后果，不是按改动量。允许破坏式变更。

1. **Live Route economic health 进入 `ExecutionEligibility` + create/approve + UI gate。** Degraded / DataIncomplete 硬拒；InsufficientEvidence 仍允许 operator；fresh Healthy 才允许 automatic。新增 typed `IneligibilityReason`，禁止再塞派生布尔量。
2. **未知成交状态机拆开。** Latch ExitOnly + alert + metric；立即 cancel 本 credential 挂单；pause 针对无法证明的 resting order；**禁止**在存在 `lot_allocation_required` 时用空向量自动 reconcile。
3. **Ambiguous / 无 `venue_order_id` 的链上成交进 quarantine。** 在 recon 或 match 证据齐备前禁止 clean-funder。实现合同里的冲突 quarantine，或从 12.0 删除该句并记账。
4. **PolicyEvaluation Eligible 排除 Censored**（或显式分桶）。Closure manifest 的额外核对不能当日常不变量。
5. **恢复 UI 做成 incident 控制面。** 全局 banner；WS 或短轮询；typed blocker 全文；挂单列表 + 一键撤单；token 级 lot 编辑；Seal / Unpause 与链上动作一一对应；finalize 后引导 kill-switch ack 与（若仍需要）governed restart。
6. **要么实现 `Rejected`，要么从 12.0 删除并在 Decision Ledger 写理由。** 同期清理 UnknownExternal association tag、horizon FAK 字面、fresh bootstrap ExitOnly 条款。

在这六条落地之前，把 12.1 写成 `IMPLEMENTATION CLOSED` 容易让后续把「任务做完」误读成「账户事故可运营、经济健康真的挡住了新入场」。

---

## 12. 总评

当前质量可以精确描述为：

**架构决策生产级；授权轴与经济 WORM 平面的软件主环已闭合；账户恢复内核的 fail-closed 密度高，但自动 pause / 空 reconcile / 不可逆 clean-funder / 非实时 UI 使恢复旅程未达可运营闭环；经济健康门禁停在 admission，默认 operator 路径可以带着 Degraded Route 预留资本。**

这不是「再加兼容层就能收口」的状态，也不是「推倒 12.0 重来」的状态。下一步必须是一次**显式授权的修补合同**：先改 12.1 Decision Ledger 与 12.0 字面，再改代码。不得从本文直接施工，也不得把本文当成恢复 `ReportOnly` 或 `ManualExecutionOutcome` 的许可证。

---

## 13. 关键文件索引

| 域 | 路径 |
|---|---|
| 设计合同 | `docs/plans/quant-pivot/phase-12/12.0-execution-authority-account-recovery-fast-feedback.md` |
| 执行台账 | `docs/plans/quant-pivot/phase-12/12.1-implementation-ledger.md` |
| 原 §4 | `docs/audit/2026-08-13-full-system-deep-audit.md` L302–L355 |
| 同日另一通道审计 | `docs/audit/2026-09-02-phase-12-s4-closed-loop-audit.md` |
| 账户投影 | `crates/quant-pivot-core/src/execution/account_chain_projector.rs` |
| 恢复服务 | `crates/quant-pivot-core/src/execution/account_recovery_service.rs` |
| 恢复评估 | `crates/quant-pivot-core/src/execution/account_recovery_reconciler.rs` |
| Pause 编排 | `crates/quant-pivot-core/src/execution/account_pause.rs` |
| Association / clean-funder | `crates/quant-pivot-repository/src/postgres/quant/account_recovery.rs` |
| Intent 授权 | `crates/quant-pivot-core/src/execution/intent_service.rs` |
| Admission health | `crates/quant-pivot-core/src/execution/admission/checks.rs` |
| Report eligibility | `crates/quant-pivot-core/src/report/composer.rs` |
| Preflight | `crates/quant-pivot-core/src/governance/entry_authorization_preflight.rs` |
| 经济 replay | `crates/quant-pivot-core/src/service/recommendation_economic_outcome.rs` |
| Feedback 边界 | `crates/quant-pivot-core/src/service/feedback_cohort.rs` |
| 订单规则 | `crates/quant-pivot-models/src/domain/order/rules.rs` |
| 官方两所 | `crates/quant-pivot-api/src/exchange/constants.rs` |
| 恢复 UI | `ui/apps/web-antdv-next/src/shared/components/account-recovery-panel.vue` |
| 经济三联卡 | `ui/apps/web-antdv-next/src/views/trading/recommendations/modules/recommendations/modules/widgets/recommendation-economic-feedback.vue` |
| Create gate | `ui/apps/web-antdv-next/src/views/trading/recommendations/modules/recommendations/modules/use-create-intent-gate.ts` |
