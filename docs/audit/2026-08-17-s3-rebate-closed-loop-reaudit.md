# S3 Maker Rebate 闭环复审（2026-08-17）

> **范围**：对照 [`2026-08-17-s3-rebate-and-training-target-closed-loop-audit.md`](2026-08-17-s3-rebate-and-training-target-closed-loop-audit.md) 的工单与**本轮收尾后的工作区**，判断 S3 是否已可验收。
> **方法**：缺陷优先。已合上的项明确背书，禁止回归。不接受“加了 coverage 字段就算 fail-closed”。
> **读者**：下一轮收尾执行。本文件取代前序审计的开放项清单。
> **立场**：生产级、语义精准、零兼容 shim。冻结决策 D1–D7 仍然有效。

---

## 0. 一句话结论

**操作员合同、账户对账、Trade Policy replay 内核已经合上；S3 仍不能验收。** 原因不是“没接线”，而是 **fee-enabled 市场缺 rebate 证据时，报告层和目录层仍然按零激励发出 Passive**，而本轮新增的 `rebate_evidence_coverage == 1` 门禁把 `Unavailable` 也算作已覆盖，给了假闭合。

更精确地说：

- 系统现在**可以**让操作员看到硬预留 / 延迟 rebate / `risk_net_usd`，账户页能看到 estimate → award → wallet 与健康状态，Policy replay 用同一套 `expected_incentive`，并同时产出 expected / risk 两条净值。
- 系统现在**仍会**在 Gamma 计划不完整、或 rebate 在决策边界尚不可见时，把 Passive 送进 MILP，金额为 0。这与 D5 冲突，是原始 S3 低估的残留入口。
- 训练目标合同未回退。不要再碰 Buy 标签。

---

## 1. 前序工单对照

| 前序 ID | 本轮状态 | 证据 |
|---|---|---|
| P0-1 操作员 `suggested_usd` 断裂 | **已合** | 后端摘要/diff/CH 已改名；`QuantOperatorApiContractSchema` 生成 UI 类型；推荐详情、intent 确认框、报告表、dashboard 读 `hard_reserved_cash_usd` / `expected_maker_rebate_usd` / `risk_net_usd` |
| P0-2 权益视图 / recon API / 指标告警 | **已合** | `EquitySnapshotView.incentive_credit_cumulative_usd`；`GET /quant/incentives/reconciliation` 与 events；Prometheus + stale/incomplete/credit-overdue 告警；账户页接线 |
| P1-1 fee-enabled 缺计划 fail-open | **未合** | `normalize_maker_rebate_schedule` 仍 `Ok(None)`；单测仍把不完整计划当成功；`resolve` 仍把 `NotPointInTime` 吞成 `None`；Passive 仍按零激励构图 |
| P1-2 Policy replay 无 rebate | **内核已合，门禁未合** | 观察值有 `PitMakerRebateEvidence`；fill 调用 `expected_incentive`；`expected_net_return_bps` vs `risk_net_return_bps`；CPCV 选择走 expected。但 coverage 把 Unavailable 当覆盖 |
| P1-3 旧 catalog payload 缺字段硬失败 | **未合** | `MarketRegistryInfo.maker_rebate_schedule` 仍无 `#[serde(default)]`；`CATALOG_OBJECT_SCHEMA_VERSION` 仍为 2 |
| P1-4 `/rebates/current` 身份 | **已合** | `ClobClient.maker_address` 冻结为 `topology.funder`（下单资金钱包）；响应 date/maker 必须匹配；上游失败记 failed scan，不再把 HTTP 失败当成零奖励日 |
| D1 禁止 net-return Buy 标签 | **保持** | `ModelTrainingTarget::{OutcomePayout, HoldVsExitAlpha}` |
| D3 禁止 taker rebate 进 MILP | **保持** | portfolio 无 taker rebate；taker 只出现在账户 credit |

文档残留不算回归：`04-topn` §9 仍列出 `suggested_usd`，与同文档 §9.1 以及实现冲突。见 P2。

---

## 2. 本轮明确背书（禁止回归）

| ID | 不变量 | 证据 |
|---|---|---|
| R1 | 操作员 sizing 合同是硬预留，不是 suggested | `SizingPlan`；`total_hard_reserved_cash_usd`；CH `hard_reserved_cash_usd` |
| R2 | UI 操作员类型从 Rust `JsonSchema` 生成，禁止再手写平行 DTO | `operator_contract.rs`；`ui/packages/types/src/generated/quant-operator-api.ts` |
| R3 | 延迟 rebate 在 UI 标明不可花 | `recommendation-plans.vue` / intent 确认框 `rebateNotice` |
| R4 | 场景同时展示 `discounted_net_usd` 与 `risk_net_usd` | 推荐详情 scenario 表 |
| R5 | 权益累计激励只做归因，不得加回可用现金 | `EquitySnapshotView` 注释；账户页 `attributionOnly` |
| R6 | 对账只读：estimate / CLOB award / wallet credit + 两个 delta + scan health | `incentives.rs`；`IncentiveReconciliationView` |
| R7 | 空 award 日若上游成功，则以成功 scan 入库；缺席的旧 partition 写 `amount_usd=0` 撤回 | `apply_award_snapshot` |
| R8 | Policy replay 对 maker fill 使用与报告层同一 `expected_incentive`（fill 日 + 1 的 `expected_credit_at`） | `policy_replay.rs` `passive_rebate_improves_expected` |
| R9 | Policy CPCV **选择**走 expected（含 rebate），risk 作为平行序列。这是正确的：用 risk 做选择会再次系统性杀掉 Passive | `policy_performance_expected_selection_risk_tail_v4` |
| R10 | CLOB rebate 查询身份 = 下单 funder，且校验响应 | `clob/mod.rs` `maker_rebate_awards`；`assert_maker_identity` |
| R11 | Buy 标签仍是赔付分数；成本在经济层 | `model_input.rs`；CPCV `token_payout_ratio` |

---

## 3. P1 — 仍阻塞“S3 已闭环”

### P1-1 目录与报告在缺 rebate 证据时仍发出零激励 Passive

三处仍然是同一条失败模式：

```348:350:crates/quant-pivot-api/src/gamma/catalog.rs
    else {
        return Ok(None);
    };
```

`feesEnabled=true` 且 `feeSchedule` 缺 `takerOnly` / `rebateRate`（或根本没有 `feeSchedule`）→ `Ok(None)`。单测 `rebate_requires_complete_evidence` 把这标成正确。

```276:281:crates/quant-pivot-research/src/execution_semantics.rs
            Some(schedule) => match schedule.validate_at(decision_at) {
                Ok(()) => Some(schedule),
                Err(FeeError::NotPointInTime) => None,
                Err(error) => return Err(error),
            },
            None => None,
```

`resolve` 在 CLOB 费用可见、Gamma rebate 不可见时成功返回 `maker_rebate_schedule: None`。单测 `future_rebate_is_zero` 把这标成正确。

报告 builder 只在 `resolve` **返回 Err** 时拒绝候选。`None` 会继续走 Passive 构图，`expected_maker_rebate_usd = 0`：

```429:453:crates/quant-pivot-research/src/portfolio/economic.rs
        let full_fill_maker_rebate = input
            .execution_economics
            .maker_rebate_schedule
            .as_ref()
            .map(|schedule| { /* expected_incentive */ })
            ...
        let expected_maker_rebate_usd = Usd::new(quantize_venue_amount(
            full_fill_maker_rebate.map_or(Decimal::ZERO, |incentive| {
                incentive.expected_rebate_usd.inner()
            }) * expected_filled_shares.inner()
                / input.requested_shares.inner(),
        ));
```

官方：fee-enabled 市场有 maker rebate 计划。`None` 的合法含义只有「该市场无费用/无计划」。CLOB `platform_rate > 0` 或 Gamma `feesEnabled=true` 时，零激励 Passive 就是 S3 低估。

**正确修法（不要再加一层永远为真的 coverage）：**

1. `feesEnabled=true` 或 CLOB 费率为正，但 rebate 字段不完整 → **拒绝进入可交易目录**（`CatalogMarketReject`），不是 `Ok(None)`。
2. `feesEnabled=false` / 费率 0 → `None` 合法，且不得发出依赖 rebate 的 Passive 估值差。
3. 决策边界上 rebate 源不可见：与 fee 源不可见同等处理——**不要发出 Passive tier**（只保留 Aggressive），或拒绝该候选。禁止 `resolve` 吞 `NotPointInTime`。改掉那两条把 fail-open 当正确的单测。

### P1-2 `rebate_evidence_coverage == 1` 对 Passive 几乎永真

本轮加了看起来像 fail-closed 的发布门：

```1454:1459:crates/quant-pivot-models/src/types/trade_policy.rs
        match validation.rebate_evidence_coverage {
            None => blockers.push(...MissingRebateEvidenceCoverage),
            Some(value) if value != Decimal::ONE => {
                blockers.push(...IncompleteRebateEvidenceCoverage);
            }
```

覆盖判定是：

```1199:1207:crates/quant-pivot-research/src/policy_replay.rs
        .filter(|fill| {
            fill.side == Side::Buy
                && fill.exit_reason.is_none()
                && fill.liquidity_role == LiquidityRole::Maker
        })
        .all(|fill| fill.maker_rebate_evidence.is_some());
```

Passive 成交**总是**写入 `Some(Available | Unavailable)`。`Unavailable { NotListed }`（catalog 无计划）仍然 coverage=1、rebate=0，cohort 可以发布。

这个门禁只能抓住「maker fill 上 evidence 字段漏写」这类实现错误，抓不住「fee-enabled 市场没有 rebate 计划」。它不能替代 P1-1。

**修法**：coverage 继续可以表示“每笔 maker fill 都有显式裁决”。另外必须有一条 **Passive 发布条件**：当观察值的 CLOB/Gamma 费用已启用时，`PitMakerRebateEvidence` 必须是 `Available`。`Unavailable` 的 Passive 行要么算 gap（退出 common support），要么整条 Passive cohort 不得 publish。不要让 `NotListed` 静默变成零激励的可发布策略。

### P1-3 旧 catalog 对象缺字段仍会让 PIT 解码崩溃

`MarketRegistryInfo.maker_rebate_schedule` 注释写的是「缺失 = 不可用」，但字段没有 `#[serde(default)]`。`decode_catalog_payload` 不看 `schema_version`。`CATALOG_OBJECT_SCHEMA_VERSION` 仍是 2。

部署后，rebate 字段引入**之前**写入、且尚未产生新 change 的市场，训练/回测/报告 PIT 会 serde 失败。这比当成 0 更糟。

`#[serde(default)]` 在这里不是兼容 shim，就是该字段自己声明的语义。新对象继续写字段；旧对象解码为 `None` 后走 P1-1（fee-enabled 则拒绝 Passive / 拒绝候选）。补一条「payload 无该键 → `None`」的解码测试。

---

## 4. P2 — 不阻塞内核，但应清掉

### P2-1 架构文档自相矛盾

实现和 `04-topn` §9.1 已经是 `hard_reserved_cash_usd`。下列仍写旧名，会把下一轮代理带回去：

- `docs/plans/quant-pivot/04-topn-report-and-recommendation.md` §9 字段列表
- `docs/plans/quant-pivot/phase-04/04.1-portfolio-planner-and-sizing.md`
- `docs/plans/quant-pivot/phase-04/04.4-report-api-ws-notifications.md`
- `docs/plans/quant-pivot/phase-05/05.0-execution-foundation-and-contracts.md`
- `docs/plans/quant-pivot/phase-05/05.2-order-intent-service.md`
- `docs/operations/runbook.md` 人工下单 SOP 第 5 步
- `docs/plans/quant-pivot/01-domain-model-and-schema.md`

破坏式改名，禁止 alias。

### P2-2 推荐详情未展示冻结 `rebate_rate`

金额和“不可花”提示已经有了。冻结条款（`maker_rebate_schedule.rebate_rate` / hash）只在 payload 里。不是功能断裂。若补，从 sizing 读，不要写进预测用的 `MarketContext`。

### P2-3 estimate→award 残差只有 gauge，没有独立告警

stale / incomplete day / award 超 48h 未入账已有告警。`quant_venue_incentive_estimate_to_award_delta_usd` 可看，但没有阈值告警。账户页能看到数字。可保持现状，或给持续大残差加 Warning。

### P2-4 ClickHouse 列是 bootstrap 原地改名

`quant_report_recommendation_fact.suggested_usd` → `hard_reserved_cash_usd`。现有 CH 实例需要按本仓库惯例重建/对齐 schema hash，不要做双列兼容。

---

## 5. 本轮查过、不追加的项

| 项 | 结论 |
|---|---|
| 把 Buy 标签改成 net-return | 仍然禁止 |
| 用 `risk_net` 做 CPCV 候选选择 | 禁止。会再次低估 Passive。当前 expected 选择 + risk 平行序列是对的 |
| 把 taker rebate 折进 MILP | 禁止 |
| 解析 Gamma `feeType` | 不需要 |
| 给 `fee_equivalent` 加 taker 费地板 | 与官方 rebate 公式相反 |
| `/rebates/current` 改查 signer 而不是 funder | 不要。下单资金钱包就是 maker；本轮测试已锁 funder |
| 把空 `[]` award 当身份失败 | 身份已在 client 构造期冻结，且响应行校验 maker。空数组在身份匹配后是真·零奖励日 |
| 给 bootstrap L2-free 路径加 Passive rebate | 不要 |

---

## 6. 下一轮工单（按顺序）

1. **P1-3**：`maker_rebate_schedule` 加 `#[serde(default)]` + 旧 payload 解码测试。
2. **P1-1**：fee-enabled / 正费率 且 rebate 证据不完整或不可见 → 拒绝目录或拒绝 Passive。改掉 catalog 与 `future_rebate_is_zero` 的 fail-open 单测。
3. **P1-2**：Passive 发布要求费用启用时 evidence 为 `Available`。`Unavailable` 不得靠 coverage=1 过门。
4. **P2-1**：清掉架构/runbook 里所有 `suggested_usd`。

质量门不变：

```bash
cargo fmt --all --
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask architecture audit-functions
cargo xtask architecture check
cargo test --workspace
```

---

## 7. 验收清单

- [x] 操作员能看到硬预留、期望成交、立即费用、延迟 rebate（标明不可花）、场景 `risk_net_usd`。运行时 API/UI 不再读 `suggested_usd`。
- [x] 权益曲线返回 `incentive_credit_cumulative_usd`；存在 estimate/award/credit 对账面；scan 健康可告警。
- [ ] fee-enabled 市场缺 `rebateRate` 时不能以 rebate=0 的 Passive 进入 MILP。
- [x] Trade Policy replay 对 Available 计划使用同一 rebate 公式（含 `expected_credit_at`）。
- [ ] 旧 catalog 对象不会因缺字段让 PIT 崩溃；fee-enabled 的 `None`/`Unavailable` 走 fail-closed。
- [x] `/rebates/current` 使用下单 funder，身份不匹配失败可见。
- [x] Buy 标签仍是 `token_payout_ratio` / `hold_vs_exit_alpha_bps`。
- [x] Taker rebate 不在组合优化目标里。
- [ ] Passive 发布门禁区分 `Available` 与 `Unavailable`，而不是只检查 `Option::is_some`。

未完成前，S3 保持 **open**。
