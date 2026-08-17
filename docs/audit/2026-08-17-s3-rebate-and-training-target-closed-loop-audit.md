# S3 Maker Rebate / 训练目标闭环审计（2026-08-17）

> **Superseded（开放项状态）**：开放项与验收以 [`2026-08-17-s3-rebate-closed-loop-reaudit.md`](2026-08-17-s3-rebate-closed-loop-reaudit.md) 为准。本文保留为当时缺陷证据与冻结决策原文；不得从本文恢复已被否定的实现（net-return Buy 标签、taker rebate 进 MILP、`suggested_usd` 兼容别名）。

> **范围**：对照 [`2026-08-13-full-system-deep-audit.md`](2026-08-13-full-system-deep-audit.md) §3（S3：成本模型缺 maker/taker rebate；训练目标 mid-return vs 考核目标 net-of-cost）与**当前工作区实现**，判断该条是否已形成生产级业务闭环。
> **方法**：缺陷优先。实现交叉验证 + Polymarket 官方费用/rebate 文档 + 训练/考核合同源码。不接受“契约写了但未接线”。本文件在对话审计之后又做了一轮代码/文档/官方规范复验；§7.5、§7.6、§8.3 是复验新增项，其余为第一轮已确认、本轮复核仍成立的结论。
> **读者**：下一轮收尾执行（实现代理）。本文件是可执行工单，不是综述。
> **立场**：生产级、语义精准、零兼容 shim / re-export / 转发路径。禁止把旧 `suggested_usd` 留作别名。禁止把 Buy 标签改成 net-of-cost。禁止把 taker rebate 折进 MILP。

---

## 0. 一句话结论

**经济内核已经优于原始 S3 建议，而且正确地拒绝了“把训练标签改成 net-return”这条捷径；但操作员合同、Trade Policy OOS、以及 fee-enabled 市场缺 rebate 元数据时的 fail-open，仍在系统性地低估 maker 路径。不能验收为“S3 已闭环”。也不接受“什么都不做”。**

更精确地说：

- 系统现在**可以**按官方公式把延迟 maker rebate 从 Gamma `feeSchedule.rebateRate` 读入 PIT，写入 Passive 场景的 `discounted_net_usd`（期望）与 `risk_net_usd`（未到账强制为 0），并且**不**把 rebate 混进 `cash_outlay` / 硬预留。
- 系统现在**不能**让操作员在 Admin SPA 上看到这份经济；Trade Policy 发布 Passive cohort 时仍然按「maker fee=0、rebate=0」计价；`feesEnabled=true` 但 `rebateRate`/`takerOnly` 缺失时静默当成无计划。
- 原始审计建议的 `mid-return-at-horizon-bps@1` → `net-return-at-horizon-bps@2` **已经过时且错误**。当前 Buy 目标是 `token_payout_ratio`，成本在独立经济门禁层。禁止再把 spread/fee 折进 Buy 标签。

---

## 1. 冻结决策（本轮不得推翻，除非显式改 PLAN）

| # | 决策 | 理由 |
|---|---|---|
| D1 | **拒绝**把 Buy 训练标签改成 net-of-cost / mid-return − entry_cost | 选择偏差 + 与 Trade Policy / MILP 双重计数。当前 `ModelTrainingTarget::OutcomePayout` 合同更正确 |
| D2 | Maker rebate 进入 MILP 期望现金流，**不**进入 `cash_outlay` / 硬预留 / CVaR `risk_net_usd` | 官方计划是日结、延迟、按 fee-equivalent 份额分配；未到账不是可花费现金 |
| D3 | Taker rebate **只**做账户层可见性，**永不**进入组合优化 | 30 日加权成交量分层；Bronze 门槛 $2,000 wV；单笔成交不可归因 |
| D4 | CLOB `MarketFeeSchedule` 与 Gamma `MarketMakerRebateSchedule` 保持独立 hash；曲线字段不一致则拒绝候选 | 立即费用权威在 CLOB market-info；rebate 计划权威在 Gamma |
| D5 | 缺失 rebate 元数据 = **不可用**，禁止合成 `rebate_rate=0` 的“空计划”去冒充已建模 | 零计划会再次系统性低估 Passive，复现 S3 |
| D6 | 破坏式对齐 UI / OpenAPI / 报告摘要字段名。禁止 `suggested_usd` 兼容别名 | 后端 `SizingPlan` 已改；UI 仍读旧字段 |
| D7 | `/rebates/current` 的 maker 身份必须是真实下单身份，空数组不得默认为“本日无奖励” | Safe/proxy 与 signer 不一致时会漏记 award |

---

## 2. S3 原始主张 vs 当前现实

原始审计 §3 的两条缺口：

1. **Rebate 完全未建模**。当时全仓库几乎只有 Gamma wire 里的无关字段。建议在 `PitFeeSchedule` 上加 `maker_rebate_share`，作为带不确定性的场景项，不要混进 `cash_outlay`。
2. **训练目标 ≠ 考核目标**。当时认为训练是 mid-return bps，CPCV Rank IC 是 net-of-cost `realized_return_bps`。建议廉价改标签，或改用 walk 可成交价。

当前工作区：

| 原始主张 | 现状 | 本文件态度 |
|---|---|---|
| maker 只有 fee=0 | 独立 `PitMakerRebateSchedule` + `DeferredVenueIncentive` + 三层会计（立即 / 期望 / 风险） | **背书**，且比 `maker_rebate_share` 塞进 `PitFeeSchedule` 更干净 |
| rebate 是估计量 | 给定成交，官方公式在 `rebate_pool = rebate_rate × Σ fee_equivalent` 时退化为 `own × rebate_rate`；不确定性只在成交分布 | **背书**。不要再把 rebate_rate 本身做成场景随机变量，除非官方改计划 |
| 不要混进 cash_outlay | `ImmediateExecutionCost` 不含 rebate；Passive `hard_reserved_cash_usd` 按全额成交预留 | **背书** |
| 改 Buy 标签为 net-return | `ModelTrainingTarget::{OutcomePayout, HoldVsExitAlpha}`；CPCV `TargetRankIc` 对 `token_payout_ratio`；`policy_net_return_bps` 只是诊断 | **否定原始建议**。见 §5 |
| 系统低估 Passive | 报告层 MILP 已计入期望 rebate；**Policy replay 与 UI 仍未计入** | 业务环未闭合 |

---

## 3. 外部规范交叉验证

对照（2026-08-17 复验）：

- [Trading fees](https://docs.polymarket.com/trading/fees)
- [Maker rebates](https://docs.polymarket.com/programs/maker-rebates)
- [Taker rebates](https://docs.polymarket.com/trading/taker-rebates)
- [`GET /rebates/current`](https://docs.polymarket.com/api-reference/rebates/get-current-rebated-fees-for-a-maker)
- [Market details](https://docs.polymarket.com/market-data/market-details)：`feesEnabled` + `feeSchedule.{rate,exponent,takerOnly,rebateRate}`（另有 `feeType`，例如 `crypto_fees_v2`）

官方 maker rebate：

```
fee_equivalent = C × feeRate × p × (1 − p)
rebate = (your_fee_equivalent / total_fee_equivalent) × rebate_pool
```

当 `rebate_pool = rebate_rate × Σ fee_equivalent` 时，份额项约掉，得到 `rebate = your_fee_equivalent × rebate_rate`。仓库实现的就是这条恒等式：

```991:1005:crates/quant-pivot-research/src/execution_semantics.rs
    fn fee_pool_share_cancels() {
        // ...
        let share_weighted_award = own / pool * (pool * rebate_rate);
        assert_eq!(share_weighted_award, own * rebate_rate);
    }
```

Golden：100 shares @ 0.50，crypto `rate=0.07`，`rebate_rate=20%` → `0.35` USD。与官方示例一致。

官方还写明：

- Maker 费率在 taker-only 市场上为 0，rebate 来自该市场 taker 费池的 15–25%。
- 日结；文档提到可能有最低打款门槛（约 $1）以及“rate may change”。
- Taker rebate 按 30 日加权成交量分档，与单笔 maker fill 无关。

Gamma 字段名与 `crates/quant-pivot-api/src/gamma/wire.rs` 的 `WireFeeSchedule` 对齐（`rate` / `exponent` / `taker_only` / `rebate_rate`）。本仓库**不解析** `feeType`。这不是经济计算缺口：数值权威已经在 `rebateRate`。不必为了“完整”去猜 program id。见 §9。

---

## 4. 已合上（禁止回归）

实现代理若改到这些文件，必须保持下列不变量。

| ID | 不变量 | 证据 |
|---|---|---|
| C1 | 立即费用与延迟 rebate 分源：CLOB `MarketFeeSchedule` vs Gamma `MarketMakerRebateSchedule` | `crates/quant-pivot-models/src/domain/market/fee.rs` |
| C2 | 曲线字段（`fees_enabled`/`platform_rate`/`exponent`/`taker_only`）不一致 → `FeeError::SourceMismatch`，拒绝候选 | `PitMarketExecutionEconomics::resolve` |
| C3 | `fee_equivalent` **忽略** `taker_only` 与 builder，且**故意不**套用 taker 费的 0.00001 地板。Rebate 池按官方曲线，不是按实收 taker fee | `PitFeeSchedule::fee_equivalent` 注释与实现 |
| C4 | `expected_incentive` 只在 `LiquidityRole::Maker` + 正成交 + `fees_enabled` + 非零 `rebate_rate` 时产生 `DeferredVenueIncentive` | `PitMakerRebateSchedule::expected_incentive` |
| C5 | 立即层：`ImmediateExecutionCost.cash_outlay_usd` 不含 rebate | `ImmediateExecutionCost` |
| C6 | 期望层：场景 `discounted_net_usd` 含折现后的延迟 rebate | `ScenarioExecutionCashflow` |
| C7 | 风险层：`risk_net_usd` 把未到账激励强制为 0 | 同上 |
| C8 | `SizingPlan` 暴露 `requested_shares` / `expected_filled_shares` / `hard_reserved_cash_usd` / `immediate_fee_usd` / `expected_maker_rebate_usd` / `maker_rebate_schedule` / `reference_entry_price` | `crates/quant-pivot-models/src/types/report_payload.rs` |
| C9 | `EntryExecutionEconomics` 是内部 tagged `Aggressive \| Passive`；Passive 带 fill distribution 与冻结 rebate 条款 | `economic_tier.rs` |
| C10 | 账户现金只在 Data API `MAKER_REBATE`/`TAKER_REBATE` 入账后增加；估计与 CLOB award 不改变可花费余额 | `venue_incentive.rs`；equity 的 `incentive_credit_cumulative_usd` |
| C11 | 激励账本 append-only WORM；按 `source_partition` 取 PIT 最新 | `quant_venue_incentive_event`；`crates/quant-pivot-system-tests/tests/repository/accounting/venue_incentive.rs` |
| C12 | 配置 `venue_incentive_reconciliation_secs = 3600`，`lookback_days = 35`（覆盖 30 日 taker 窗口 + 日结滞后） | `config/quant-pivot.toml` |
| C13 | 目录对象 payload 就是 `MarketRegistryInfo`，含 `maker_rebate_schedule`；报告 builder 从 PIT catalog 读取，不是现场再猜 | `gamma.rs` encode/decode；`report/builder.rs` `capture.market.maker_rebate_schedule` |
| C14 | Buy 目标封闭枚举，自由字符串 target 反序列化失败 | `ModelTrainingTarget` `deny_unknown_fields` |
| C15 | Taker rebate 不进入 `portfolio/economic.rs` / MILP | 全 `crates/quant-pivot-research/src/portfolio` 无 `taker_rebate` |

测试夹具里大量 `maker_rebate_schedule: None` **不是**生产路径漏洞。生产路径是 Gamma mapper → `MarketRegistryInfo` → catalog object → PIT resolve。不要把夹具 `None` 当成“没接线”。

---

## 5. 训练目标：明确否定原始 S3 建议

当前合同：

```20:34:crates/quant-pivot-models/src/types/model_input.rs
pub enum ModelTrainingTarget {
    OutcomePayout,
    HoldVsExitAlpha,
}
// OutcomePayout => "token_payout_ratio"
// HoldVsExitAlpha => "hold_vs_exit_alpha_bps"
```

注释写死：可成交价、成交、费用、退出、资金成本属于**独立冻结的 Trade Policy 与全局组合层**，永不折进该预测目标。

因此原始审计 §3.3 的事实前提已经不成立：

- 训练标签不是 `mid-return-at-horizon-bps@1`。
- CPCV `TargetRankIc` 的 Spearman 对的是 `token_payout_ratio`（0 / 0.5 / 1），不是 `realized_return_bps`。
- `policy_net_return_bps` 是诊断字段，不是训练目标，也不是 Rank IC 输入。
- 经济质量门是另一条链：Trade Policy OOS → stateful replay → MILP `discounted_net_usd`。

若把 spread/fee/rebate 折进 Buy 标签：

1. **选择偏差**：标签依赖当时 book 与 fee 曲线，模型学会的是“当时贵不贵”，不是“终局赔付”。
2. **双重计数**：同一笔成本会再进 Trade Policy 与 MILP。
3. **破坏校准**：`OutcomePayout` 需要的是 `[0,1]` 赔付概率，不是 bps 收益。

**禁止实现** `net-return-at-horizon-bps@2`。HoldVsExit 已经是可成交价标签，只用于卖出侧，不要推广到入场 Buy。

S3 真正还活着的训练/考核错位是：**Passive 的经济门禁（Trade Policy OOS）仍然不计 rebate**，见 P1-2。那是门禁层缺口，不是标签层缺口。

---

## 6. P0 — 必须修，否则不能声称 S3 业务闭环

### P0-1 操作员合同断裂：后端已改 `SizingPlan`，UI / 摘要词汇还停在 `suggested_usd`

后端：

```101:118:crates/quant-pivot-models/src/types/report_payload.rs
pub struct SizingPlan {
    pub economic_tier_id: EconomicTierId,
    pub requested_shares: Shares,
    pub expected_filled_shares: Shares,
    pub hard_reserved_cash_usd: Usd,
    pub immediate_fee_usd: Usd,
    pub expected_maker_rebate_usd: Usd,
    pub maker_rebate_schedule: Option<FrozenMakerRebateSchedule>,
    pub reference_entry_price: Price,
    // ...
}
```

`ui/` 是独立仓库（`oxide-arb-ui`，branch `quant-pivot`），类型手写，**没有**从 Rust 生成。当前仍是：

```85:97:ui/packages/types/src/quant-recommendation.ts
export interface SizingPlan {
  economic_tier_id: UuidString;
  suggested_usd: UsdString;
  suggested_shares: SharesString;
  entry_vwap: PriceString;
  // ...
}
```

`ExecutableEconomicTier.entry` 仍是扁平 `notional_usd / fee_usd / slippage_usd`，没有 `Aggressive|Passive` tag，没有 `delayed_maker_rebate_usd` / `risk_net_usd`。

运行时这些旧字段是 `undefined`。受影响视图（非穷尽）：

- `recommendation-plans.vue` / `recommendation-detail-panel.vue`
- `use-create-intent-action.ts`（创建 intent 确认框展示金额）
- `report-recommendations-table.vue` / `recommendation-orbit.vue` / `dashboard/index.vue`

**同一破坏式命名清理必须一次做完**，不要只改 UI 读新字段却留下摘要谎言：

| 残留名 | 实际值来源 | 问题 |
|---|---|---|
| `ReportSummary.total_suggested_usd` | 已改为对 `hard_reserved_cash_usd` 求和（`composer.rs`） | JSON 名仍叫 suggested；Passive 下这是**全额预留**，不是期望成交金额 |
| `ReportDiff.suggested_usd_delta` / `base_total_suggested_usd` | 同样来自 hard reserve | 名实不符；UI diff 面板碰巧还能显示数字 |
| ClickHouse `QuantReportRecommendationFactRow.suggested_usd` | 事实行仍用旧列名 | 分析层继续把预留叫建议 |
| `docs/plans/quant-pivot/phase-04/04.1-portfolio-planner-and-sizing.md` | 仍写 `suggested_usd / entry_vwap` | 与 `04-topn-report-and-recommendation.md` 已更新的合同冲突 |

正确修法（一次破坏，禁止别名）：

1. UI types + 全部视图改为当前 `SizingPlan` / `EntryExecutionEconomics` / `ScenarioExecutionCashflow`。
2. 摘要、diff、CH 列、通知 bundle 把 `suggested_usd*` 重命名为 `hard_reserved_cash_usd*`（或同等精确名）。
3. 界面必须同时展示：硬预留、期望成交份额、立即费用、**延迟且不可花**的 `expected_maker_rebate_usd`、场景 `risk_net_usd`。
4. 删除 04.1 过时段落，或改到与 04 主文档一致。
5. `MarketContext` 目前只有 `fee_rate`，没有 rebate 条款。至少在推荐详情展示冻结的 `maker_rebate_schedule.rebate_rate`（来自 sizing，不必污染预测用的 market context）。

### P0-2 HTTP 账户视图丢掉已持久化的激励累计

实体与领域 `EquitySnapshotInfo` 有 `incentive_credit_cumulative_usd`。HTTP `EquitySnapshotView` 的 `From` 实现**整列丢弃**：

```125:157:crates/quant-pivot-models/src/domain/api/quant_account.rs
pub struct EquitySnapshotView {
    // ... realized / unrealized / HWM / drawdown ...
    // 无 incentive_credit_cumulative_usd
}
```

`quant-pivot-web` 账户路由与 dashboard 消费该 View。操作员无法从 API 看到钱包已入账的 maker/taker credit。

同时：

- 没有 venue-incentive 列表 / 对账 API（estimate / CLOB award / wallet credit + 两个 delta）。
- `estimate_to_award_delta` 只打在 `tracing::info`（`venue_incentive.rs`）。
- `metrics_hub.rs` 无 rebate/incentive 指标，无告警。

没有这组面，日结对账在生产上等于没有。

**修法**：View 补字段；加只读 recon API；delta 进 Prometheus + 超阈告警。不要做写接口。

---

## 7. P1 — 不修则会在生产中系统性复现 S3 低估

### P1-1 fee-enabled 但 rebate 元数据不完整 → fail-open 成“无计划”

```333:350:crates/quant-pivot-api/src/gamma/catalog.rs
    let (Some(fees_enabled), Some(platform_rate), Some(exponent), Some(taker_only), Some(rebate_rate), Some(effective_at)) = (...)
    else {
        return Ok(None);
    };
```

单测 `rebate_requires_complete_evidence` **把这个 fail-open 当成正确行为**：`feesEnabled=true` 且 `feeSchedule` 缺 `takerOnly`/`rebateRate` → `maker_rebate_schedule = None`。

官方：fee-enabled 市场有 maker rebate 计划。`None` 在下游被解释为“按严格零激励估值”（`PassiveEntryEconomics` 注释、`SizingPlan` 注释）。这与 D5 冲突，会再次让 MILP 低估 Passive。

同族问题：`PitMarketExecutionEconomics::resolve` 对 rebate 的 `FeeError::NotPointInTime` **吞成 `None`**，并有单测 `future rebate source is unavailable, not a fee failure`。CLOB fee 在同一边界不可见会拒绝；Gamma rebate 不可见却按零激励继续。候选带着“已建模、rebate=0”进入优化器。

**修法**：

- `feesEnabled=true`（或 CLOB `platform_rate > 0`）时，缺 `rebateRate` / `takerOnly` / `exponent` → **拒绝该市场进入可交易目录或拒绝该候选**，不要 `Ok(None)`。
- `feesEnabled=false` / 费率为 0 的市场 → `None` 合法（无计划）。
- 决策边界上 rebate 源不可见：与 fee 源不可见同等 fail-closed，或显式标记 `RebateEvidence::Unavailable` 并使该市场 **不能** 产生 Passive tier（只保留 Aggressive）。禁止静默零激励 Passive。

### P1-2 Trade Policy OOS 结构上没有 rebate

`PolicyReplayObservation` 只有 `fee_schedule: Option<PitFeeSchedule>`，没有 rebate 计划。构造处只从 CLOB market-info 投影费用：

```1495:1530:crates/quant-pivot-core/src/service/trade_policy_replay.rs
            let fee_schedule = market_info_at(...)
                .map(|market_info| PitFeeSchedule::from_market_fee_schedule(&market_info.fee_schedule()) ...);
            Ok(PolicyReplayObservation { ..., fee_schedule, ... })
```

`PolicyReplayFill` 有 `fee` / `cash_delta` / `fee_schedule_hash`，没有延迟激励。`net_return_bps = (exit_cash - entry_cash) / entry_cash`。Maker 在 taker-only 下 fee=0，rebate 永不加入。

后果：Passive cohort 的 publish/compare 仍是「fee=0、rebate=0」。若此处阈值卡掉 Passive，MILP **根本看不到** 报告层已经会加的 rebate 调整后的 tier。这是 S3 “系统性低估 Passive”在门禁层的残留，比 UI 更危险。

**修法**：观察值补上与报告层同一套 `PitMakerRebateSchedule`（来自 PIT catalog，不是 CLOB market-info）。Fill 增加延迟 `expected_incentive` 现金流，结算日与报告层相同（fill 日 + 1）。`net_return_bps` 是否计入 rebate 必须在文档里写死：建议 **诊断净值含折现 rebate，门槛比较同时给出 `risk_net`（rebate=0）**，避免用未到账激励放行政策。禁止只改报告层、不改 policy replay。

### P1-3 已有 catalog 对象缺少 `maker_rebate_schedule` 字段时，PIT 反序列化会硬失败

`MarketRegistryInfo` 新增了 `maker_rebate_schedule: Option<...>`，**没有** `#[serde(default)]`，也没有把 `CATALOG_OBJECT_SCHEMA_VERSION`（当前为 2）上调。`decode_catalog_payload` 不按 `schema_version` 分支，一律用当前结构体。

目录账本是内容寻址、长期存活的。部署本分支后，**尚未产生新 change 的市场**会继续解码旧 payload。缺字段 → serde 失败 → 该市场的训练/回测/报告 PIT 全断。这比“rebate 当成 0”更糟。

`#[serde(default)]` 在这里**不是**兼容 shim：该字段自己的注释已经规定「缺失 = 不可用，永不合成零利率计划」。缺省为 `None` 正是这个语义。新写入仍应带字段；旧对象解码为 `None` 后，再走 P1-1 的 fail-closed（fee-enabled 则拒绝 Passive / 拒绝候选），而不是让整个 catalog 解码崩溃。

若坚持“旧对象必须拒绝”，必须先全量重同步 catalog。不要默默上线。

### P1-4 `/rebates/current` 用 `funder` 当 `maker_address`

`VenueIncentiveService` 把 `funder` 传给 CLOB `maker_rebate_awards`。Data API 的 `proxyWallet` = funder 对入账是对的。CLOB maker 身份可能是 signer 而不是 proxy。空数组当前表示“无 award”，不是身份失败。PolyNode 文档警告 Safe/proxy 可能返回空。

**修法**：用真实下单 maker 身份查询；空结果与身份/HTTP 失败分型。身份失败 fail-closed（告警，不要记“零奖励日”）。

---

## 8. P2 — 应登记，但不阻塞宣称“内核正确”

### P2-1 官方 $1 最低打款与 rate 变更

期望 rebate 在给定成交下是确定的。未建模：最低打款、官方“rate may change”、estimate vs award 残差进入场景。残差已在对账 delta 里出现，应进 P0-2 的可观测性，不必把 rebate_rate 本身做成随机变量。

### P2-2 灰尘成交上 `fee_equivalent` 无 0.00001 地板

`PitFeeSchedule::fee` 对实收 taker fee 有地板；`fee_equivalent` 故意没有。与官方 rebate 公式一致。灰尘单可能对 rebate **微幅高估**。保持现状，除非官方明确 rebate 池按实收 fee 而非曲线。

### P2-3 Bootstrap `ReportOnlyWithLiveL2` 只有 aggressive tier

`bootstrap_candidate_tiers` 无 Passive，因此无 rebate。与 “bootstrap 报告不算可执行 Passive” 一致。不要为了闭环去给 bootstrap 造 Passive rebate。

### P2-4 `feeType` 未解析

Gamma 有 `feeType`（如 `crypto_fees_v2`）。经济计算用数值 `rebateRate` 即可。不必补 program 枚举。若以后官方按 `feeType` 切换公式，再把该字段作为 PIT 事实存下来——现在不要猜。

---

## 9. 本轮复验后明确不追加的项

这些查过了，**不要**当成缺口去“补全”：

| 项 | 为什么不动 |
|---|---|
| 把 Buy 标签改成 net-return | §5，D1 |
| 把 taker rebate 折进 MILP / `cash_outlay` | 账户级、不可单笔归因，D3 |
| 把 `maker_rebate_share` 塞进 `PitFeeSchedule` | 当前独立 Gamma 计划 + composite hash 更正确 |
| 把 rebate_rate 做成场景随机变量 | 给定成交后公式确定；不确定性在成交分布，已经在 Passive fill distribution 里 |
| 解析 `feeType` | 数值权威已在 `rebateRate` |
| 给 `fee_equivalent` 加 taker 费地板 | 与官方 rebate 公式相反 |
| 给 bootstrap L2-free 路径加 Passive rebate | bootstrap 不可执行 Passive |
| 把测试夹具里的 `maker_rebate_schedule: None` 全部改成 Some | 夹具不是生产路径 |
| 为旧 `suggested_usd` 做 serde alias | D6 禁止 |

---

## 10. 可执行工单（给下一轮实现）

顺序按依赖，不要平行把 UI 接到一个仍会 fail-open 成零激励的后端上而不修 P1-1。

1. **P1-3**：`MarketRegistryInfo.maker_rebate_schedule` 加 `#[serde(default)]`；补“旧 payload 无该字段 → `None`”的 catalog 解码测试；新对象继续写入字段。决定 fee-enabled + `None` 时的行为（接步骤 2）。
2. **P1-1**：fee-enabled / CLOB 费率为正 且 rebate 证据不完整或 PIT 不可见 → fail-closed。改掉 `rebate_requires_complete_evidence` 与 `resolve` 把 `NotPointInTime` 吞成 `None` 的单测语义。
3. **P1-2**：`PolicyReplayObservation` / `PolicyReplayFill` 接同一套 `expected_incentive`；Passive cohort 发布前必须看到 rebate。
4. **P0-1**：破坏式对齐 UI types、视图、intent 确认框、报告摘要/diff/CH/通知字段名、04.1 文档。展示硬预留 vs 期望成交 vs 延迟 rebate vs `risk_net_usd`。
5. **P0-2**：`EquitySnapshotView` 补 `incentive_credit_cumulative_usd`；只读 recon API；Prometheus + 告警。
6. **P1-4**：CLOB `/rebates/current` 使用真实 maker 身份；空 vs 失败分型。

质量门（改完必须过）：

```bash
cargo fmt --all --
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask architecture audit-functions
cargo xtask architecture check
cargo test --workspace
```

UI 侧在 `oxide-arb-ui` 同步改 types 与视图，禁止在本仓库加兼容 DTO。

---

## 11. 验收清单

S3 只有在下列全部为真时才能称为闭环：

- [ ] 操作员在推荐详情能看到：硬预留现金、期望成交、立即费用、延迟 maker rebate（标明不可花）、场景 `risk_net_usd`。旧 `suggested_usd` / `entry_vwap` 字段名从 API 与 UI 消失。
- [ ] 权益曲线 API 返回 `incentive_credit_cumulative_usd`；存在 estimate/award/credit 对账面；delta 可告警。
- [ ] fee-enabled 市场缺 `rebateRate` 时不能以 rebate=0 的 Passive 进入 MILP。
- [ ] Trade Policy OOS 的 Passive 净值与报告层使用同一 rebate 公式（含延迟结算日）。
- [ ] 旧 catalog 对象不会因缺字段而让 PIT 解码崩溃；fee-enabled 的 `None` 走 fail-closed 而不是静默低估。
- [ ] `/rebates/current` 身份与下单 maker 一致，或失败可见。
- [ ] Buy 标签仍是 `token_payout_ratio` / `hold_vs_exit_alpha_bps`。仓库中不存在 `net-return-at-horizon-bps@2`。
- [ ] Taker rebate 仍不在组合优化目标里。

未完成前，S3 保持 **open**。
