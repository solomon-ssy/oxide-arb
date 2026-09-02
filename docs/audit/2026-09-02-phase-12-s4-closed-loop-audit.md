# Phase 12 / 原审计 §4+S4 闭环审计（2026-09-02）

> **地位**：日期化审计。本文件是 Phase 12「执行授权、账户恢复与快速经济反馈」是否构成生产级闭环的当前验收判定。它**否定** [`12.1-implementation-ledger.md`](../plans/quant-pivot/phase-12/12.1-implementation-ledger.md) 的 `IMPLEMENTATION CLOSED` 声称，但**不**取代 [`12.0-execution-authority-account-recovery-fast-feedback.md`](../plans/quant-pivot/phase-12/12.0-execution-authority-account-recovery-fast-feedback.md) 作为设计合同。后续实施必须另开明确授权的计划与新 ledger，不得从本文件或聊天记录恢复 active task。
>
> **范围**：原审计 [`2026-08-13-full-system-deep-audit.md`](2026-08-13-full-system-deep-audit.md) §4（L302–L355：ReportOnly 人工成交断链、长结算无 MTM）及其 R7/R9；表级 **S4**（订单可提交性四缺口）；Phase 12 冻结合同 12.0 与执行账本 12.1；对应 Rust/UI/schema 实现；Polymarket 官方 User Channel / V2 `UserPausable` / Quantstamp V2 外部交叉验证。
>
> **方法**：defect-first。不接受「契约写了但未接线」；不接受「账本 PASS 行代替运行时 owner」；不接受 Playwright 像素合同代替运营旅程。源码交叉验证 + 外部协议文档核对 + 对 12.1 Decision Ledger 的取舍评审。
>
> **立场**：只要落地质量确实达到生产级完整闭环，可以什么都不做。本次结论是否定 12.1 的闭合声称，并保留 12.0 的大部分主设计决策。不考虑最小变更、向前兼容或 re-export。
>
> **前序**：[`2026-08-13-full-system-deep-audit.md`](2026-08-13-full-system-deep-audit.md)（§4 / S4 / R7 / R9 的问题陈述）；Phase 12 是对该节的官方 replacement 计划。
>
> **本次未改代码。**

---

## 0. 一句话结论

Phase 12 把原审计 §4 的错误药方（`ManualExecutionOutcome`、MTM 进训练、Sports 骨架）否对了，也把原 S4 订单四缺口收成了唯一 `PolymarketOrderRules`；**但账户恢复的对账所有权、Route economic health 的生产写入、break-glass 退出面三条环是断的**。12.1 将 W1–W7-05 全部标 `DONE` / `IMPLEMENTATION CLOSED` 是过声称。R18 disposable rehearsal 只证明「不激活自动权时，研究反馈能在假场内转一圈」，不能证明未知成交后的唯一合法恢复路径、也不能证明 health 行会被生产出来。

**不能按 Implementation Closed 验收，更不能据此进入 Operational Activation。**

---

## 1. 范围与命名

原 [`2026-08-13-full-system-deep-audit.md`](2026-08-13-full-system-deep-audit.md) 里有两套容易混用的「S4」：

| 记号 | 位置 | 原问题 | Phase 12 对应 |
|------|------|--------|---------------|
| **§4 Feedback** | L302–L355 | 4.1 ReportOnly 下手成交无法回流；4.2 标签必须等结算、无 MTM 快反馈 | 12.0 §1–§6：删 runtime mode、单写者+recovery、`RecommendationEconomicOutcome` |
| **S4 订单可提交性** | 摘要表、§14.1–14.2 | 入场价不 tick 对齐、size/amount 精度未实现、最小订单量未进生产、下单前不查 allowance | 12.0 §2.4 + W7-04「S4 deep audit」：`PolymarketOrderRules` |

本次审计按这个**并集**判定闭环，因为 12.0 冻结合同和 12.1 W7-04 已经把两块写进同一 Phase。

对照文档：

- 设计合同：[`docs/plans/quant-pivot/phase-12/12.0-execution-authority-account-recovery-fast-feedback.md`](../plans/quant-pivot/phase-12/12.0-execution-authority-account-recovery-fast-feedback.md)
- 执行账本：[`docs/plans/quant-pivot/phase-12/12.1-implementation-ledger.md`](../plans/quant-pivot/phase-12/12.1-implementation-ledger.md)
- Phase 索引：[`docs/plans/quant-pivot/phase-12/README.md`](../plans/quant-pivot/phase-12/README.md)

文档状态互相否定：12.0 / README 仍写 **FROZEN DESIGN — IMPLEMENTATION IN PROGRESS**，12.1 写 **IMPLEMENTATION CLOSED**。这不是文案漂移，是「当前执行真相」有两个 owner。

---

## 2. 总判定矩阵

| 维度 | 判定 | 说明 |
|------|------|------|
| 12.0 主决策（删 mode、单写者、MTM 不进 ModelLearning、禁 ManualExecutionOutcome、Sports 延期） | **保留** | 比原审计 R9 / 4.2 药方更正确 |
| 原 S4 订单四缺口 | **内核已闭合** | 入场路径可提交；退出面未按 break-glass 收口 |
| §4.2 政策快反馈（WORM + PolicyReplayKernel） | **研究面闭合** | ModelLearning 隔离成立；运营 health 未接线 |
| §4.1 / 12.0 §4 账户恢复与 break-glass | **未闭合** | 检测环成立；对账所有权与退出面失败 |
| Route health → PolicyAutomatic | **断环** | 消费者齐全，生产者只有测试 |
| 12.1 `IMPLEMENTATION CLOSED` | **否定** | 见 §10 |
| UI/UX 最佳实践闭环 | **未达到** | 有骨架和截图合同，没有运营发现性 |
| Operational Activation | **禁止从本 Phase 推断** | 12.0 §9 本身也禁止；本审计维持 |

---

## 3. 对原审计建议与 12.0 的取舍

### 3.1 应保留：不要退回 2026-08-13 的药方

**拒绝 `ManualExecutionOutcome`（原 R9）。** 原审计建议操作者上传 token/price/shares/timestamp，标记为 operator-attested。这会把不可密码学验证的成交混进执行学习。Polymarket Data API trades 可以按 wallet 读成交，但 **没有**完整的跨凭证未成交订单生命周期；authenticated User Channel 按 API credential 过滤。V2 `OrderFilled` 才是 finalized 账户事实。单写者 + unknown → `AccountRecoveryIncident` 比「回填成交映射到 recommendation」正确。

因此原 4.1「手工成本相对模型假设偏高 X bps 不可见」被 **故意不关**。政策层用 horizon 可成交 replay 代替；真实滑点/成交率只认系统 `OrderIntent` 的 `ExecutionTrajectoryArtifact`。这个取舍成立。

**MTM 不得成为 `token_payout_ratio` 的降级标签。** horizon 全深度 bid walk 是 PolicyEvaluation / 运营 health 的快信号，不是 Buy 模型终局监督目标。原 4.2「用 MTM 缩短训练周期」会污染校准、promotion 与 mature-label 计数。12.0 §5.4 拆对了。

**Sports 延期。** 官方 Sports WebSocket 明确可能延迟、错误或漏事件，不足以支撑执行级 alpha。在当前 Pooled 路径加入半实现 `DomainFamily::Sports` 会制造假闭环。

**删整个 `QuantRuntimeMode`。** 报告可读性、intent 授权、自动提交、退出自动化、settlement write 本就是正交权威。默认 `OperatorApprovalRequired` 已能安全观察报告。生产 `crates/`、`ui/apps`、`schema/` 中 `QuantRuntimeMode` / `ReportOnly` / `mode_gate` / `ApprovalStatus` / `OrderIntentKind` 为零命中。

### 3.2 设计合同自身不精确，应改合同而不是把代码改回草稿

| 12.0 条文 | 问题 | 应有终态 |
|-----------|------|----------|
| §3.3 association 含 `UnknownExternal` | 与 §4.1「原子建 incident」重复 | 保持实现：unknown 绑 `RecoveryIncident`；从合同删除第三种 association kind |
| Incident FSM `Open → Reconciling → Sealed \| Rejected` | `Rejected` 未实现；enum 只有 open/reconciling/sealed | 要么实现 Rejected 语义，要么从合同删除 |
| Incident kind `BreakGlassRestart` / `OpeningInventory` | 只有 PG enum 与 CHECK，无创建路径 | 要么实现创建逻辑，要么删除死枚举 |
| 「`PolicyEvaluation Eligible` 不证明非 censored」 | cohort 层仍把 Censored 经济行计为 Eligible | **Censored 不得进入 usable policy count**；合同与实现对齐 |
| Fresh bootstrap「必须在 ExitOnly 且 recovery seal healthy」 | 实现只要求 `OperatorApprovalRequired` | 按合同补齐，或在 Decision Ledger 写明弱化理由并重算 `design_contract_hash` |

---

## 4. 六条目标环

### 4.1 环 A — 执行授权：基本闭合

落地证据：

- `EntryAuthorizationPolicy` / `ExecutionAuthorityCeiling` / `AuthorizationEvidence` 三元组分离，无派生布尔量。
- Intent 单一 FSM：`PendingAuthorization → Authorized → …`；operator create 为 pending，PolicyAutomatic 仅在 preflight 全过时原子 `Authorized`。
- 升级 `PolicyAutomatic` 必须 `EntryAuthorizationPreflight`；降级无 readiness 预检但仍走 CAS/RBAC/WORM。
- kill switch、account recovery latch 的 `ExitOnly`、`Degraded`/`DataIncomplete` health 在 admission 层对所有授权种类硬拒绝。
- UI 不再暴露 runtime mode；header / runtime-control-panel 切到 entry authorization policy。

缺口：

```781:782:crates/quant-pivot-models/src/domain/quant/model_route_bootstrap.rs
            && self.current_entry_authorization_policy
                == EntryAuthorizationPolicy::OperatorApprovalRequired
```

12.0 §2.3 要求 fresh route bootstrap 在 **ExitOnly 且账户 recovery seal healthy**。实现只检查 operator 政策。操作员仍可在 bootstrap 窗口对手动建 intent。Decision Ledger 没有撤销 ExitOnly 要求。这是比冻结合同更弱的安全包络，不是「更合理的简化」。

`IneligibilityReason` 目前只有 `AutomationCapExceeded`。经济健康、recovery 不在 recommendation blockers 里，详情页 eligibility 列无法表达「为什么现在不能自动」。

Operator 在 `AutomationCapExceeded` 时仍可手动建 intent（ceiling 仍允许 operator）——这与「blockers 不授予自动权」一致，但 UI 必须把 cap 耗尽显示成明确阻断，而不是静默可点。

### 4.2 环 B — 未知成交检测：半闭合

未知 finalized account execution 会：

1. 原子创建或复用 `AccountRecoveryIncident`（kind=`UnknownExternalExecution`）
2. association 记为 `RecoveryIncident`
3. kill switch latch `ExitOnly`
4. 首次 `incident_created` 发 `AlertLevel::Critical` / `AlertCategory::TradingSafety`

```134:150:crates/quant-pivot-core/src/execution/account_chain_projector.rs
            if let Some(incident) = association.incident.as_ref() {
                latch_exit_only(self.kill_switch.as_ref(), incident).await?;
                self.pause.pause_incident(incident, &self.funder).await?;
                if association.incident_created {
                    self.alerts.dispatch_background(
                        Alert::new(
                            format!("account-recovery:{}", incident.account_recovery_incident_id),
                            AlertLevel::Critical,
                            AlertCategory::TradingSafety,
                            AlertSource::Execution,
                            "Unknown external account execution",
                            incident.reason.clone(),
                            Utc::now(),
                        )
                        .with_affects_trading(true),
                    );
                }
            }
```

检测环成立。断在操作者动作名实不符：

```446:460:crates/quant-pivot-core/src/execution/account_recovery_service.rs
    async fn pause_and_reconcile(...) {
        ...
        self.reconcile_incident(&incident, allocations).await?;
        self.incident_view(incident_id)
```

`pause_and_reconcile` **只 reconcile**。`pauseUser` 只在 projector 路径。Runbook「Pause and reconcile 调用 pauseUser」与按钮文案都在撒谎。若 projector 的 pause 失败或尚未 dispatch，操作者会得到 `PauseIncomplete`，并以为自己已经 pause。

### 4.3 环 C — Break-glass 恢复：未闭合

已落地且应保留：

- 多 lot 部分 SELL 必须显式 allocation；单 lot / 全量卖出按份额守恒唯一分配。当前代码无 FIFO。W3-04 ledger 的 FIFO 是被 W6-02 替换的历史记录，不是双实现。
- 动态读取 `userPauseBlockInterval`，等待 `effective_block` + `isUserPaused`；未硬编码 100 blocks。
- Seal 前禁止 `unpauseUser`；finalize 要求 pause/unpause 数量对称且全部 `Confirmed`。
- 外部 Maker / SelfMatch → `quant_account_clean_funder_blocker` WORM，无 ack/clear API，直接 DELETE 被 trigger 拒绝。
- Recovery-only 启动：有 active incident 时不注册 entry dispatcher。

未闭合的三条构成 P0/P1，见 §5 F1 / F3 / F4。

### 4.4 环 D — 政策快反馈 MTM：研究面闭合

对照 12.0 §5，下列成立：

| 要求 | 证据 |
|------|------|
| 六态 WORM | `RecommendationEconomicOutcomeState`；表级 append-only trigger |
| 只走 `PolicyReplayKernel` | `recommendation_economic_outcome.rs` 调 `replay_policy_horizon` / `replay_policy_candidate` |
| Horizon 全深度 bid ladder | `execute_exit` → `walk_sell_exact_shares`；`HorizonLiquidated` 强制 FullBidLadder |
| 无 mid / last / TOB fallback | 缺 L2/continuity/fee/depth → typed censor |
| CapacityDeferred ≠ censor | busy 时 `retry_task(ComputeCapacityUnavailable)`，不写 WORM |
| ModelLearning 不消费 MTM | `evaluate_model_learning` 只走 resolution；real-PG 隔离测试断言 economic=`None` |
| 训练标签仍是 `token_payout_ratio` | `model_training.rs` |

`post_fill_markout_bps` 仍用 mid，但是诊断字段，不参与 `net_return_bps` / 状态分类。

原 4.2「模型反馈以季度计」**没有被解开**，12.0 也没声称要解。政策层可以在 profile horizon 或 PIT 可见终局转起来；训练层仍等市场结算。不得把 R18 误读成「ML 闭环变快了」。

研究面未对齐之处见 §5 F8（Censored 计入 Eligible）。

### 4.5 环 E — Route health → 自动权：断环

评估器、四态、average-uniqueness、event-cluster bootstrap、admission `EconomicHealthCheck`、preflight「全 Route fresh Healthy」**消费者和纯函数都在**。

生产写入不在：

```33:40:crates/quant-pivot-core/src/service/route_economic_health.rs
    pub async fn assess(
        &self,
        route: &BuyModelRoute,
        route_identity_hash: ContentHash,
        profile_id: ResearchProfileArtifactId,
        policy: &ResearchFeedbackPolicy,
        assessed_through: DateTime<Utc>,
```

全仓库 `.assess(` 调用点：`crates/quant-pivot-system-tests/tests/repository/research/route_economic_health.rs` 仅此一处。`ExecutionBundle` 构造了 `Arc<RouteEconomicHealthService>`，没有任何 worker 在 economic pass / feedback DAG 之后调用它。

后果：

- `latest(route_identity_hash, …)` 通常 `Missing`
- Operator 仍可下单（by design：Missing/Insufficient 允许 operator）
- **PolicyAutomatic 在生产中永久 fail-closed**
- UI 第三列通常为空
- W4-05 / W4-06 / W5-05 标 DONE = 「闸门焊上了，没有人往闸门后送证据」

即使补上 assess，`source_window` 也只滤 route+profile：

```168:174:crates/quant-pivot-repository/src/postgres/quant/route_economic_health.rs
        let outcomes = OutcomeEntity::find()
            .join(JoinType::InnerJoin, OutcomeRelation::Recommendation.def())
            .filter(RecommendationColumn::Route.eq(*route))
            .filter(OutcomeColumn::ResearchProfileArtifactId.eq(profile_id.clone()))
            .filter(OutcomeColumn::DecisionAt.gte(window_start))
```

admission / preflight 使用完整 `RouteEconomicHealthIdentity`（route + profile + `model_version_id` + `trade_policy_artifact_id`）。混版本观测会被写成某一 identity 的 Healthy/Degraded。这是「一旦接线就会以错误证据放行/阻断」的潜伏缺陷。

### 4.6 环 F — 原 S4 订单可提交性：内核闭合

`PolymarketOrderRules` 是 report / backtest / admission / entry / exit / reconciliation / SDK adapter 的唯一数学 owner。对照原 §14.1–14.2：

| 原缺口 | 2026-08-13 | 现在 |
|--------|------------|------|
| 入场 tick 对齐 | `aggressive_buy_limit` 未对齐，admission 系统性拒单 | `PolymarketOrderRules::aggressive_buy_limit`；`[tick, 1-tick]` |
| 最小订单量 | 生产路径未读 `minimum_order_size` | rules + economic + admission + final-hop |
| size/amount 精度 | 无 tick 派生舍入 | 六档 tick、SHARE_SCALE=2、wire `10^6` |
| allowance | 只查余额 | V2 exact spender + balance/allowance；禁止 auto-approve |

final-hop 同时重读 `/book` 与 `/clob-markets/{id}`，核对 identity 与 canonical payload hash；SDK tick/NegRisk cache 从 seed 到 sign 由同一 async mutex 串行，锁在 POST 前释放。这些对上了 12.0 §2.4 和官方拒单码。

残留（不够否掉入场内核，够否掉退出面「最佳实践」）：

- 提交时仍不复核 `accepting_orders`（原 §14.3 市场暂停 TOCTOU）
- 系统 exit 默认 `OrderType::Gtc`（见 F4）
- 六 tick 缺完整 `place_order` → `validate_unsigned_order` 矩阵（点测 Hundredth / QuarterCent）

---

## 5. 发现登记册

编号为本审计局部 ID，不延续 2026-08-13 的 R1–R50，以免把已替换的 R7/R9 药方重新激活。

### 5.1 P0 — 不修则不能宣称账户恢复或自动权可用

#### F1 恢复对账被 projector 冲掉

**严重度：P0。** 多 lot 部分 SELL 的唯一合法路径会在约 60 秒内自毁。

机制：

1. Reconciliation worker 每个 tick 先跑 `account_chain_projector.project_pass`。默认 `ReconciliationPolicy.interval_secs = 60`。
2. 存在未 seal incident 时，`project_pass` 调用 `advance_incident`。
3. `advance_incident` 在 `seal_hash.is_none()` 时以 **空** `Vec::new()` 再 reconcile。
4. 操作者刚用显式 allocation 写出的收敛 manifest 会被空分配的新 attempt 盖掉（`latest_manifest` 按 `attempt_no` desc）。
5. 第一次收敛已经在 `append_manifest` → `apply_allocations` 改写了 lot。
6. 再提交同一 allocation：`validate_allocation` 要求 `row.shares == before_shares`，失败为 `position lot changed after recovery assessment`。
7. 最新 manifest 非收敛，seal 拒绝。incident 可能只能走 clean-funder / 人工库修。

证据：

```79:88:crates/quant-pivot-core/src/execution/account_chain_projector.rs
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

```265:271:crates/quant-pivot-core/src/execution/account_recovery_service.rs
        if incident.seal_hash.is_none() {
            self.reconcile_incident(incident, Vec::new()).await?;
            return Ok(());
        }
```

```179:191:crates/quant-pivot-repository/src/postgres/quant/account_recovery.rs
    async fn apply_allocations(...) {
        if !draft.assessment.converged() {
            if draft.created_lots.is_empty() {
                return Ok(());
            }
```

非收敛的后继 manifest **不会回滚**已经落地的 lot 变更。

`evidence_hash` 把 `observed_at` 归一成 UNIX epoch，因此同一业务输入可幂等。空 allocation 与非空 allocation 不是同一输入，必然新 hash、新 attempt。E2E `post-trade.spec.ts` 在两次 projector tick 之间线性点完 reconcile→seal，测不到这条竞态。

**正确形状：** 操作者 allocation 是冻结输入；lot 变更只允许在 **seal 同一事务**发生一次；background 最多重捕获 venue 快照，禁止用空分配覆盖。

#### F2 Route economic health 无生产 owner

**严重度：P0（对 PolicyAutomatic / 12.0 §5.3 闭环）。** 对默认 operator 入场是 fail-closed，因此不立即亏钱；但对「快反馈变成可执行运营信号」是断环。

见 §4.5。W4-05/W4-06/W5-05 必须从 DONE 降级，或在新计划里补唯一生产 owner 后再关闭。

补接线时必须同时修 `source_window` 的 identity 过滤（F7），否则会以错误样本评估 Healthy。

#### F3 恢复库存没有合法退出面

**严重度：P0（对 12.0 §4.3 break-glass 合同）。**

| 合同要求 | 实现 |
|----------|------|
| UI 只允许立即成交型 exit；禁止 GTC/GTD | 无 break-glass 下单 UI；系统 exit 默认 GTC |
| 操作者完成后在 UI 撤销全部订单 | `cancel_all` 仅定义于 `clob/mod.rs:1509`，全仓库零调用方；UI 不展示 `open_order_ids` |
| pause 后等待 effective block | 动态 interval 已读；delay 窗口内仍可成交，且系统可继续挂单（F4） |
| 无法证明零挂单则保持 paused | OpenOrdersPresent fail-closed 成立，但操作者没有工具完成该前置 |

```213:218:crates/quant-pivot-core/src/execution/exit_dispatcher.rs
        let intent_id = lot
            .order_intent_id
            .ok_or_else(|| ExecutionError::IntentDenied {
                reason: "recovery-origin position lot is not authorized for strategy exit"
                    .to_owned(),
            })?;
```

Recovery lot 的 `origin_kind=AccountRecoveryIncident`、`order_intent_id=None`。策略退出 dispatcher 拒绝它们。系统停机时操作者只能去 Polymarket UI 卖——而这正是会再触发 unknown external 的路径。

Quantstamp 对 V2 的结论（pause **不是** cancel；链上没有 per-order cancel）与 12.0 的单写者前提一致：CLOB `DELETE` 级 `cancel_all` 必须由本系统发起。现在没有。

### 5.2 P1 — 合同违反 / 语义缺口

#### F4 ExitOnly / recovery-only 仍发 GTC

```76:91:crates/quant-pivot-core/src/app/bootstrap.rs
    match StartupExecutionScope::from(active_recovery.as_ref()) {
        StartupExecutionScope::EntryEnabled => ctx.register_execution_dispatcher(&mut runner),
        StartupExecutionScope::RecoveryOnly => {
            ctx.register_execution_recovery(&mut runner);
            ...
        }
    }
    ctx.register_reconciliation_worker(&mut runner);
    ctx.register_settlement_workers(&mut runner);
    ctx.register_exit_monitor_worker(&mut runner);
```

Exit monitor **无条件注册**。`KillSwitchState::allows_auto_exit` 包含 `Closed | ExitOnly`。非紧急退出：

```351:355:crates/quant-pivot-core/src/execution/exit_monitor.rs
    match limit {
        Some(limit) if shares.is_positive() => ExitDecision::SubmitExitOrder {
            reason,
            order: sell_order(&input.lot.token_id, shares, limit, OrderType::Gtc),
```

12.0 §4.3：「UI 只允许立即成交型 exit；禁止 GTC/GTD 或任何可能 resting 的订单。」恢复期间系统继续挂 resting 单，同时 seal 要求证明零挂单。即使把 §4.3 窄读成「只约束人工 UI」，recovery-only 下系统 GTC 仍破坏「可证明的零挂单」。

正常 `Closed` 下的策略 scale-out 用 GTC 是另一件事，必须与 break-glass / ExitOnly 分开。合同应写成：

- `Closed`：系统可按策略使用 GTD/GTC（入场 passive 已是 GTD）
- `ExitOnly` 或 active incident：只允许 FAK/FOK；禁止新 resting；强制 cancel_all

#### F5 `pause_and_reconcile` 名实不符

见 §4.2。API / UI / runbook 必须三方同一语义：要么该 mutation 真正 `pause_incident` 并等待 effective block，要么改名并拆步。

#### F6 Fresh bootstrap 弱于冻结合同

见 §4.1。`OperatorApprovalRequired` 不能替代 ExitOnly：前者仍允许新入场。

#### F7 Health `source_window` 混 identity

见 §4.5。`RouteEconomicHealthIdentity.content_hash()` 含 model/policy，source 查询不含。F2 接线时这是同一 PR 的硬依赖。

#### F8 Censored 计入 PolicyEvaluation Eligible

```112:135:crates/quant-pivot-core/src/service/feedback_cohort.rs
fn evaluate_policy(...) {
    let Some(economic) = visible_economic(...)? else {
        return Ok(FeedbackCohortDecision::Censored(
            CohortCensorReason::EconomicOutcomeUnavailableAtCutoff,
        ));
    };
    ...
    Ok(FeedbackCohortDecision::Eligible(
        FeedbackCohortEvidence::PolicyEvaluation { economic, ... },
    ))
}
```

`visible_economic` 只校验 cutoff、hash、recommendation/horizon 时钟，**不看 `state`**。`feedback_dataset` 对 PolicyEvaluation Eligible 直接 `increment_eligible`。12.0 §5.2 明确 Eligible 不证明非 censored；W7-04 只在 successor **manifest** 拒绝 censored，cohort 层未改。coverage artifact 的 `eligible_count` 因此膨胀。

正确：`state == Censored`（以及按合同需要排除的 `EntryNotTriggered` / `EntryUnfilled`）走 typed Censor/Exclusion，usable count 只含合法终态。

#### F9 SelfMatch 未按双腿入账

12.0 §3.2：「自成交必须保留两条经济 leg 或产生 typed quarantine，不能静默净额。」

```574:577:crates/quant-pivot-core/src/execution/account_recovery_service.rs
        if execution.role == AccountChainExecutionRole::SelfMatch {
            invalid.push(execution.account_chain_execution_id);
            continue;
        }
```

这比静默净额安全，但不是两腿、也不是 quarantine 表。一次 SelfMatch 就能让 incident 因 `invalid_execution_ids` 无法收敛。V2 上同一 funder 同时出现在 maker 与 taker 是真实事件，不是测试边角。

### 5.3 P2 — UI / 可观测性 / 优雅性

#### F10 Recovery 发现性失败

`AccountRecoveryPanel` 只嵌在：

- `ui/.../execution/post-trade/modules/settlement/index.vue`（Settlement 页底部 `RuntimeControlPanel`）
- `ui/.../system/config/modules/governance/resource.vue`

Dashboard / header / reconciliation 队列 **没有** incident 入口。未知成交 latch ExitOnly 后，操作员不会被领到分配 / 撤单 / pause 旅程。Mismatch 只显示 count，不渲染 `open_orders_present` 的 order ID、`clean_funder_required`、`pause_incomplete` 等 typed blocker。无 incident 历史页；`Rejected` 无 UI（也无后端状态）。

#### F11 Kill switch header 在 Closed 时隐藏

```23:32:ui/apps/web-antdv-next/src/shared/components/header/kill-switch-indicator.vue
const visible = computed(() => {
  const current = currentState.value;
  if (!current) {
    return false;
  }
  return allStates.value.some(
    (target) =>
      target !== current && killSwitchAction.canTransition(current, target),
  );
});
```

`closed` 且当前用户没有可执行 transition 时，picker **完全不渲染**。只读状态藏在 `system:read` 的 `SystemStatusIndicator` popover。运营第一眼以为「没有 kill switch」。

#### F12 Economic 三联卡状态语义不足

`recommendation-economic-feedback.vue`：

- `outcome == null` 把「horizon 未到」和请求失败挤在同一 Alert（仅 `unavailable.outcome` 改 warning/info）
- Censored 只显示 state `EnumTag`，不展示 `payload.detail` / censor reason / coverage flags
- Health 用 `listRouteEconomicHealth({ page: 1, route, size: 1 })`，取该 route **最新**一条，不是 recommendation 的 model/profile/policy identity
- 有 `LatestRequestOwner`（这点做对了）

E2E `recommendations.spec.ts` mock 的是成功 `HorizonLiquidated` 路径，不覆盖 pending / censored / missing-parent 之外的真实空窗。

#### F13 Preflight i18n 死键

```21:22:ui/apps/web-antdv-next/src/shared/components/preflight-report-block.vue
          ? $t('page.systemAdmin.mode.preflightPassed')
          : $t('page.systemAdmin.mode.preflightFailed')
```

locale 中无 `page.systemAdmin.mode.*`。升级 PolicyAutomatic 时预检 drawer 显示 raw key。这是 W6-05 删 mode i18n 时漏掉的唯一仍引用旧路径的生产组件。

#### F14 Economic replay 取消未接到 worker shutdown

```990:991:crates/quant-pivot-core/src/service/recommendation_economic_outcome.rs
        let cancel = CancellationToken::new();
        let _cancel_on_drop = cancel.clone().drop_guard();
```

12.0 §5.2 要求 job cancellation 贯穿 replay。当前 token 只在 future drop 时取消，未绑定 `OutcomeReconciliationWorker` / process shutdown。drain 期间 4 GiB lease 可能拖满 30s wait / 60s lease。

#### F15 Reconciliation `validate_prepared` 未覆盖全部 prepared 字段

已验：market/token/side/order type/GTD/limit/requested shares 与 `PolymarketOrderRules::validate_order`。未逐字段重验：`post_only`、`cash_budget`、`expected_fee`、`book_hash`、`clob_market_info_payload_hash`、fee schedule / rebate。Admission submit 前更严。这不是重签第二订单（那被正确禁止），但是 WAL 相等性合同不完整。

#### F16 提交时不复核 `accepting_orders`

原审计 §14.3 项 5。admission builder 仍不在 submit 时复核市场 `accepting_orders`。报告后、提交前市场暂停是真实 TOCTOU。final-hop 绑了 book + clob-markets identity，但未把「是否仍接受订单」提升为 typed denial。

---

## 6. 外部交叉验证

### 6.1 User Channel 与开单枚举

[Polymarket User Channel](https://docs.polymarket.com/market-data/websocket/user-channel) 与 authenticated `GET /orders` 按 API credential 过滤。官方说明：断线后必须用 REST 刷新 open orders / recent trades，流本身不是权威账户全量。

因此：

- 不能用 User Channel 证明「没有任何跨凭证 resting 单」
- `get_open_orders` 不能作为零挂单的链上证明，只是本凭证快照
- 外部 Maker / SelfMatch → clean-funder（不可 ack）是对的
- 禁止 FIFO / 时间窗猜测是对的

### 6.2 `pauseUser` 不是撤单

[`UserPausable.sol`](https://github.com/Polymarket/ctf-exchange-v2/blob/main/src/exchange/mixins/UserPausable.sol)：

- `pauseUser()`：`userPausedBlockAt = block.number + userPauseBlockInterval`（默认 100，上限 302_400）
- `isUserPaused`：仅当 `block.number >= userPausedBlockAt`
- delay 窗口内可 `unpauseUser` 取消尚未生效的 pause
- Quantstamp V2 报告明确：订单取消依赖链下逻辑；`pauseUser` 带 delay，**不**取消已 resting 的签名单

实现读动态 interval、等 effective block，这部分对。缺 `cancel_all`、缺 delay 窗口内禁止系统再挂 GTC，与协议事实直接冲突。

### 6.3 V2 `OrderFilled` 角色

Decision Ledger 2026-08-18：「V2 对 resting 和 active 订单都把 order owner 放在 `maker`；主动 taker 单由 `OrdersMatched` 识别。」projector 用 funder==maker 作为账户范围、taker 集合判定 Taker、funder==taker 判定 SelfMatch。这与合约事件形状一致。SelfMatch 的**入账**处理（F9）仍不符合「两腿或 quarantine」。

---

## 7. UI / UX 闭环评审

### 7.1 做对的

- 无 runtime mode 选择器 / filter / summary
- `use-create-intent-gate.ts` 对齐 kill switch + eligibility + 当前 entry policy
- Recommendation 详情有 ceiling / blockers / policy binding
- Economic 三联卡存在，且有 `LatestRequestOwner`
- Optional child：missing parent 404 vs known-parent null，E2E 覆盖
- Governed recovery 动作有 confirm word（RECOVER / SEAL / UNPAUSE）与 acting-role

### 7.2 未达到运营最佳实践

目标旅程：

```text
未知成交 / 停机后 UI 卖出
  → 立即可见的 incident（header 或 dashboard，不是藏在 Settlement 底部）
  → 展示 typed blockers + open order IDs
  → 系统 cancel_all + 仅 FAK/FOK 的剩余退出
  → 多 lot 显式分配
  → pause 终局
  → seal
  → unpause/finalize
  → 恢复后的 lot 仍可受治理退出
```

现状在第一步发现性和最后一步退出面上断开。截图合同（51 场景、两轮 nonce-bound backend Succeeded）证明像素与进程 drain，**不**证明上述旅程在 projector 节拍下可完成。

Kill switch / entry policy 的只读可见性对无 mutation 权限的观察者失败（F11）。Preflight 死键（F13）会在第一次真正升级 PolicyAutomatic 时暴露。

Approve intent UI 只做 RBAC+FSM，kill switch 变化依赖服务端 invalidate——安全上可接受，UX 上操作者会点一个即将被作废的按钮。

---

## 8. 原 §4 两个断点现在处在什么位置

### 8.1 原 4.1 ReportOnly 人工成交无法回流

旧 `mode_gate.rs` / `QuantRuntimeMode::ReportOnly` 已删除。不再需要全局 ReportOnly 才能安全看报告。

人工/外部成交的新语义：

- 正常运行：本系统是策略账户唯一写者
- 外部 finalized execution：unknown → incident → ExitOnly，**不**伪造 `OrderIntent`，**不**创建 `ManualExecutionOutcome`
- `GET .../execution-comparison` 只消费系统 trajectory；recovery lot 没有 intent，comparison 为 `ActualBaselineUnavailable`

「操作者看到手工成交相对模型偏高 X bps」这条可见性 **故意不提供**。提供的是：政策层 horizon 可成交经济、以及真实系统 attempt 的 planned-vs-actual。只要单写者成立，这是对的。单写者在 break-glass 上不成立（F3），可见性缺口会在真实停机时重新变成「有成交、无系统解释、且库存卡死」。

### 8.2 原 4.2 长结算 + 无 MTM

政策快反馈的 WORM 平面已存在，且不增加 ModelLearning mature-label。这是原 4.2 的正确一半。

错误一半（把 MTM 当训练标签、用公共 Sports 源加速）被正确拒绝。

运营一半（Route health 驱动自动权、UI 把 censor/pending/真实结果分开）未落地（F2/F8/F12）。

---

## 9. 性能、设计与优雅性

不是本阶段主因，但是真实的后续负担：

- 未 seal 时每个 reconciliation tick 完整 capture CLOB + Data API，并可能 append WORM manifest。成本与 F1 竞态绑在一起；修 F1（background 不再用空分配写 lot）会顺带降频。
- `RouteEconomicHealthService` 挂在 ExecutionBundle 却无调用方，是典型的「类型存在、所有权不存在」。补 owner 时应放在 economic outcome 成功提交之后、按完整 identity、有界、可 CapacityDefer，而不是新的无限扫表。
- Economic replay 本地 `CancellationToken`（F14）与 12.0 已写清的「取消不抢占同步 CPU、但必须贯穿 job token」不一致。
- Association / incident kind / origin kind 三套枚举里的 OpeningInventory 彼此 CHECK 耦合（recovery_incident_id NOT NULL），却没有创建路径。这是 schema 预支，不是演进点。
- `IneligibilityReason` 单 variant 让 recommendation 冻结的「最大执行权限」无法承载 recovery/health。要么扩展 blockers，要么 UI 明确写「ceiling 之外的运行时闸门在 admission」。现在两头不到岸。

---

## 10. 如何读 12.1 账本

12.1 的工程诚实处：大量 FAIL→PASS 有根因、有后续同命令复验、R18 明确 `operational_activation_claimed=false`、零 venue/chain/relayer 写。这些不应被本审计抹掉。

12.1 的过声称处：

- `implementation_status`: 每个任务 DONE、无 blocked、Implementation Closure complete
- W3-04 曾记录 FIFO，W6-02 声称删除——当前代码无 FIFO，但 **F1 使显式分配在生产节拍下不可靠**
- W4-05/W4-06/W5-05 DONE：评估器与闸门存在，**无生产 assess**
- W3-06/W6-02/W7-03 的 recovery E2E：线性 UI 旅程，**无 projector 并发**
- W7-04 把 ingest/parity/shutdown/sampler 大量问题拉进「S4 收口」。那些 PASS 不能回填 F1/F2/F3

R18 合法证明的边界：

- production-composed binary
- owned disposable PG/CH/Redis + loopback rejector
- 15-stage DAG、CPCV、shadow overlap、historical 4199 WORM、successor 10 条经济
- runtime 仍为 `OperatorApprovalRequired`，settlement disabled
- execution-learning 对 Published 无 rollup 为 Censored 而非伪造 Excluded

R18 **没有**证明：unknown fill 在 60s projector 下可 seal；health 行出现；recovery lot 可退出；PolicyAutomatic 可被合法证据放行。

12.0 / README 仍 IN PROGRESS，与 12.1 CLOSED 互相否定。后续代理若从「零开放任务」起步，会把上述断环当成已交付。

---

## 11. 下一步（破坏式，不求最小变更）

本文件不创建实施任务。若授权新计划，准入条件应按这个顺序，而不是「补 UI」或「放宽测试」：

1. **对账所有权。** Projector 不得用空 allocation 覆盖操作者 manifest。Lot 变更只允许在 seal 事务内发生一次。Background 只更新 venue 快照 / pause 确认。为「操作者分配后、下一 projector tick、再 seal」加 real-PG 竞态测试。
2. **Health 生产者。** 唯一 worker 在 economic WORM 提交后按完整 `RouteEconomicHealthIdentity` assess。`source_window` 同步加上 model/policy 过滤。没有行就是 Missing，禁止用「route 最新一条」冒充。
3. **Break-glass 退出面。** ExitOnly 或 active incident：禁止 GTC/GTD；系统 `cancel_all`；UI 展示 order ID 并只允许 FAK/FOK；recovery lot 必须有受治理退出，禁止把 Polymarket UI 当出口。
4. **Pause 语义。** `pause_and_reconcile` 真正 pause+等待 effective+再捕获，或拆成四个 governed 步骤并改掉所有文案。
5. **合同对齐。** Bootstrap=ExitOnly+seal healthy；Censored 不得 Eligible；SelfMatch 两腿或 quarantine；删或实现死枚举；12.0 §3.3 改成与 `RecoveryIncident` 实现一致。
6. **文档唯一真相。** 在新计划的 Decision Ledger 记录上述合同修正，重算 `design_contract_hash`，把 12.0 / README / 12.1 状态改成同一句话。在此之前，12.1 的 CLOSED 不得被引用为验收。

**不要做：**

- 恢复 `ManualExecutionOutcome` / `QuantRuntimeMode` / ReportOnly 兼容轴
- 把 MTM 写入 ModelLearning / calibration / promotion label count
- 加 Sports skeleton 或公共源降级
- 为 F1 加「重试直到 seal」或跳过 projector 的测试豁免
- 为 PolicyAutomatic 在 Missing health 时开后门
- 任何 re-export、双读、migration、向前兼容 parser

---

## 12. 证据索引

| 主题 | 路径 |
|------|------|
| 冻结合同 | `docs/plans/quant-pivot/phase-12/12.0-execution-authority-account-recovery-fast-feedback.md` |
| 被否定的闭合声称 | `docs/plans/quant-pivot/phase-12/12.1-implementation-ledger.md` §0 / §2 W7-05 |
| 原问题陈述 | `docs/audit/2026-08-13-full-system-deep-audit.md` L302–L355、摘要 S4、§14.1 |
| 未知成交 / pause / latch | `crates/quant-pivot-core/src/execution/account_chain_projector.rs` |
| 空 allocation 重对账 | `crates/quant-pivot-core/src/execution/account_recovery_service.rs` `advance_incident` / `pause_and_reconcile` |
| Lot 在非 seal 时变更 | `crates/quant-pivot-repository/src/postgres/quant/account_recovery.rs` `apply_allocations` |
| 评估 hash 忽略 observed_at | `crates/quant-pivot-core/src/execution/account_recovery_reconciler.rs` L97–107 |
| Health 无生产调用 | `crates/quant-pivot-core/src/service/route_economic_health.rs`；system-tests 唯一 `.assess(` |
| Health 混 identity | `crates/quant-pivot-repository/src/postgres/quant/route_economic_health.rs` `source_window` |
| Recovery lot 不可策略退出 | `crates/quant-pivot-core/src/execution/exit_dispatcher.rs` L213–218 |
| ExitOnly 仍 GTC | `crates/quant-pivot-core/src/execution/exit_monitor.rs` L339–355；`bootstrap.rs` L90 |
| `cancel_all` 无调用方 | `crates/quant-pivot-api/src/clob/mod.rs` L1509 |
| Censored→Eligible | `crates/quant-pivot-core/src/service/feedback_cohort.rs` `evaluate_policy` |
| 订单内核 | `crates/quant-pivot-models/src/domain/order/rules.rs` |
| Bootstrap 弱于合同 | `crates/quant-pivot-models/src/domain/quant/model_route_bootstrap.rs` L781–782 |
| Recovery UI 埋点 | `ui/.../settlement/index.vue`；`account-recovery-panel.vue` |
| Economic 三联卡 | `ui/.../widgets/recommendation-economic-feedback.vue` |
| Preflight 死键 | `ui/.../preflight-report-block.vue` |
| Kill switch 隐藏 | `ui/.../header/kill-switch-indicator.vue` |

外部：

- https://docs.polymarket.com/market-data/websocket/user-channel
- https://github.com/Polymarket/ctf-exchange-v2/blob/main/src/exchange/mixins/UserPausable.sol
- Quantstamp Polymarket CTF Exchange v2 report（pause 非 cancel；无链上 per-order cancel）
