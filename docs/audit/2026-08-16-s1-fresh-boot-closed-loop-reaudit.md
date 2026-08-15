# S1 Fresh-Boot 闭环复审（2026-08-16）

> **范围**：S1 落地质量复审。对照 [`2026-08-13-full-system-deep-audit.md`](2026-08-13-full-system-deep-audit.md) §1、[`2026-08-15-s1-fresh-boot-closed-loop-audit-and-design.md`](2026-08-15-s1-fresh-boot-closed-loop-audit-and-design.md)、[`/Users/eason/Downloads/PLAN.md`](/Users/eason/Downloads/PLAN.md) 与**当前工作区实现**。
> **方法**：缺陷优先。不接受“契约写了但未接线”。已合上的项明确背书，禁止回归。
> **读者**：下一轮收尾执行（Codex / 实现代理）。本文件是可执行工单，不是综述。
> **立场**：生产级、语义精准、零兼容 shim / re-export / 转发路径。时间 SLO 永远让位于 fail-closed。禁止为了 12–36h 数字放宽 CPCV / DSR / PBO / attestation / parity。

---

## 0. 一句话结论

**数据面、预测合同、FreshBoot 状态机、Admin 进度面已经接近可验收；12–36 小时首份 Published 报告仍不是一条系统路径。**

更精确地说：

- 系统现在**可以**双源见证 Polygon finalized 成交历史、按 route `feature_contract` 训练并服务 L2-free bootstrap 模型、用 PG 状态机自动走完 dataset → train → CPCV → bootstrap。
- 系统现在**不能**在只有 pooled champion 时发布第一份报告。空 `enabled_categories` = 全部类别；报告 `represented_routes` 来自选中市场；`resolve_routes` 要求集合内每一条 route 都有 exact champion。Polymarket 上几乎永远有 Crypto 市场，因此 pooled `ReportEligible` 会 enqueue ad-hoc，然后在 Crypto/Weather 编排完成前失败重试。
- 上一轮四个 P0 已合三个（特征合同、编排器、Admin）。本轮只剩 **1 个 P0**，外加摄取/特征/可观测性上的 P1/P2。

**不能验收为“S1 已闭环”。** 也不接受“什么都不做”。

---

## 1. 冻结决策（本轮不得推翻，除非显式改 PLAN）

来自 2026-08-15 拍板，当前代码已按此落地的部分必须保持：

1. **自动编排**：activation frontier 接受且 quarantine=0 之后，系统自己走 dataset → train → cal → CPCV → parity → empty-route bootstrap → ad-hoc。质量门失败则 blocked，禁止跳过。
2. **空 `enabled_categories` 保持“全部受支持类别”**。不要改成“空列表非法”，也不要在**训练选市**层无条件剔除 Crypto/Weather。
3. **预测向量 = `route.contract.feature_contract`**。live L2 **只**用于 sizing / 可成交性，禁止写进 bootstrap `FeatureVector`。
4. **拒绝 CLOB `/prices-history` 作模型事实**。
5. **HyperSync 只做提取器，独立 archive RPC 做见证**。两侧不一致 → quarantine，禁止 fallback 单源接受。
6. **删除 trade-tape 全路径**。禁止 dual-write、兼容 view、`pub use` 转发。
7. **`ServingAuthority::ReportOnlyWithLiveL2` 永禁 OrderIntent**。intent_service 与 trade_policy_guard 双重拒绝。
8. **统计门禁不放宽**。样本不足就停在 DatasetReady 之前，禁止降 `min_sample_count` / CPCV / DSR / PBO。

本轮 **P0 的正确修法**（与第 2 条兼容，必须采用，不要再问）：

> **报告 serving 宇宙 = 当前已激活 `buy_routes` 的类别并集。**  
> 没有 champion 的类别不得进入该次报告的 market selection，也就不得进入 `represented_routes`。  
> 这不是改变空列表的配置语义，也不是把 Crypto/Weather 踢出 pooled **训练**宇宙；这是 serving 宇宙与 champion 对齐。  
> pooled 的 `ReportEligible` 只在这条边成立时才 durable enqueue 第一份 ad-hoc。

禁止的替代：

- 把空 `enabled_categories` 改成必须显式列出（推翻已拍板）。
- 让报告在缺 Crypto champion 时静默丢掉 Crypto 市场但仍把 Crypto 留在 represented set（违反原子 route 不变量）。
- 把 PLAN 改成“等三条 bootstrap 全部 committed 才出第一份报告”，却继续把 pooled 阶段标成 `ReportEligible`。

---

## 2. 已合上（禁止回归）

实现代理若改到这些文件，必须保持下列不变量。回归测试已部分存在，缺失的在 §8 补。

| ID | 不变量 | 证据 |
|---|---|---|
| C1 | 报告按 route 分 batch 构图，合同来自 champion，不是 `FullL2` 硬编码 | `crates/quant-pivot-core/src/report/builder.rs` `build_features`：`feature_contract: route.contract.feature_contract` |
| C2 | `ConfiguredFeatureBuilder::new` 已删除；所有生产路径 `new_for_contract` | `crates/quant-pivot-research/src/features/builder.rs`；`durable_feature_parity.rs` 亦走 `new_for_contract` |
| C3 | `TradeBootstrap*` schema 不含 `PublishedL2Book`；`needs_book()==false`；live book 不进预测向量 | `features/schema.rs` `trade_specs`；sizing 走 `bootstrap_candidate_tiers` |
| C4 | pooled bootstrap `cohort_contract = AllEligible`，与空列表=全类别一致 | `research_profile.rs` `pooled_trade_bootstrap` |
| C5 | PG 持久化 `quant_fresh_boot_run` / `quant_fresh_boot_run_event`；显式 `FreshBootStage::advance` | `crates/quant-pivot-models/src/domain/quant/fresh_boot.rs`；`crates/quant-pivot-core/src/app/fresh_boot_orchestrator.rs` |
| C6 | 编排顺序：coverage → dataset → train → calibration dataset → calibration → CPCV → parity → scenario → preflight → bootstrap → ad-hoc | `advance_run` match |
| C7 | coverage 要求双源一致、chunk 连续、activation+retention 窗口 `active_quarantine` 为空 | `verify_history` / `verify_chunk` |
| C8 | history worker 在 activation 追上时 stage=`ActivationReady`，不再写 `FeaturesAndLabels` | `exchange_history_worker.rs` `publish_ready` |
| C9 | Admin：`GET /system/fresh-boot`、run 详情、retry-now、supersede；SPA Dashboard 消费 | `quant-pivot-web/src/routes/system.rs`；`ui/apps/web-antdv-next/src/views/dashboard/index.vue` |
| C10 | `RESEARCH_HISTORY_GATE_DAYS = 200` 已从 dashboard 删除 | `quant-pivot-web/src/routes/dashboard.rs` |
| C11 | V1 `last_valid_block = 86_127_097` 且带 hash | `crates/quant-pivot-api/src/exchange/constants.rs` |
| C12 | canonical digest v2 含 `block_timestamp`、`model_available_timestamp`、`parent_block_hash` | `history_client.rs` `canonical_digest` |
| C13 | ETA 首块按 `[from,to]` 闭区间累加，不再 `unwrap_or(block)` 记 1 块 | `publish_accepted` |
| C14 | `ReportOnlyWithLiveL2` 与 `RecommendationPolicyProvenance::BootstrapProfile` 双重拒绝 OrderIntent | `intent_service.rs`；`trade_policy_guard.rs` |
| C15 | crates 内 `trade_tape` / `TradeTape` 运行时路径已删 | 禁止以任何名字加回来 |

---

## 3. P0 — 必须修，否则不能声称 12–36h 首报

### P0-1 原子报告宇宙与 pooled-first 编排撞车

**现象**

1. 默认 `selection.enabled_categories = []`，校验语义是全部受支持类别（`runtime_config/validation.rs`、`RepresentedRouteSet::from_enabled_categories`）。
2. 报告 `represented_routes()` 由 **included 市场类别 ∪ 账户仓位类别** 推导，不是由已激活 `buy_routes` 推导：

```2002:2014:crates/quant-pivot-core/src/report/builder.rs
fn represented_routes(
    selection: &MarketSelectionSnapshot,
    account: &AccountSnapshot,
) -> QuantResult<RepresentedRouteSet> {
    RepresentedRouteSet::from_categories(
        selection
            .included
            .iter()
            .map(|market| market.category)
            .chain(account.positions.iter().map(|position| position.category)),
    )
```

3. `ModelServingGenerationStore::resolve_routes` 对 represented 集合中每一条 route 要求 exact champion，否则：

```
serving generation {id} has no exact represented Route {route:?}
```

见 `crates/quant-pivot-core/src/service/model_serving_generation.rs` `resolve_routes`。

4. `FreshBootOrchestrator` 对 pooled / crypto / weather **各建一条 run**。pooled 在 33 天 activation frontier 完成后即可 bootstrap。crypto/weather 的 `required_from_block` 大约覆盖 93/100 天，必须等 retention 回填。
5. pooled 进入 `BootstrapCommitted` 后 `enable_report` 立刻 `run_ad_hoc`。enqueue/build 因缺 Crypto champion 失败 → `schedule_retry(DependencyUnavailable)`。状态机可以显示 `ReportEligible`，报告发不出去。

**为何是 P0 而不是产品取舍**

PLAN 与 2026-08-15 设计都写明：Crypto/Weather 是独立 run，**不阻塞 pooled**。当前代码不阻塞 pooled **训练/激活**，但用原子报告阻塞 pooled **首份 Published 报告**。编排器阶段名再次撒谎。

**正确修复（破坏式，允许改报告选市与编排边）**

1. **报告选市过滤**：一次 report build 的 candidate/selection 只保留 `buy_routes` 已有 active champion 的类别。账户仓位若落在未激活类别，要么把该仓位从 represented 推导中排除并在 funnel 结构化记录，要么 fail-closed 并让编排器停在明确的 `WaitingPeerRoutes`——**默认采用前者（排除未激活类别）**，因为 ReportOnly 首报不能被未建模的垂直仓位卡死。若仓位 notional 超过既有风控阈值，再用结构化 blocker，不要静默。
2. **`represented_routes` 必须等于**「本报告实际纳入的市场类别 ∪ 实际纳入的仓位类别」，并且是 `buy_routes` 的子集。禁止出现 represented ⊃ champions。
3. **portfolio scenario**：pooled-only 报告必须绑定 **pooled-only** route-set digest 的参考 scenario（bootstrap 已经按当时 `buy_routes` 拟合）。不要拿后来的 Pooled+Crypto 联合 scenario 去服务一份只有 Pooled 的报告，也不要拿 Pooled-only scenario 去服务含 Crypto 的报告。`PromotedPortfolioContextLoader::verify_scenario_contract` 必须继续 fail-closed。
4. **编排器**：`enable_report` 之前断言「本 run 的 route 已在 `buy_routes` 且本次 ad-hoc 的 represented set 可被当前 generation 解析」。若因他类市场被错误纳入而失败，这是 P0 回归，不是 `DependencyUnavailable` 重试。
5. **UI**：pooled run 在 champion 已提交、且 serving 宇宙可解析时才显示可出报告。缺 peer route 不再是 pooled 的 blocker。

**禁止**

- 为了让报告通过而把缺失 route 填成 pooled 模型（cross-route fallback）。
- 放宽 `resolve_routes` 的 exact 匹配。
- 改空 `enabled_categories` 的配置语义。

**测试（必须有，缺一不可）**

- 系统测试：只有 pooled bootstrap champion、catalog 含 Crypto+Sports 市场、空 `enabled_categories` → 报告 **Published**，`represented_routes == [Pooled]`，选中市场不含 Crypto。
- 负向：强行把 Crypto 市场放进 selection 但 `buy_routes` 无 Crypto → 报告 fail-closed，不发布。
- 正向：pooled 与 crypto 都 committed 后，下一份报告 represented 含 Pooled+Crypto，scenario digest 匹配联合 route-set。
- 编排：pooled `enable_report` 在上述第一份报告成功后进入 `FirstReportPublished`，不等 weather run。

---

## 4. P1 — 高优先级，生产前必须修

### P1-1 未配对 `OrderFilled` fail-open（成交双计 / 脏事实）

PLAN：missing pairing 必须阻塞 watermark。

`correlate_transaction` 只在看到 `OrdersMatched` 时排除 aggregate taker fill 并绑定 maker fill。一笔交易若只有 `OrderFilled`、没有 `OrdersMatched`，循环直接 `Ok(())`。随后：

```152:167:crates/quant-pivot-api/src/exchange/execution_projector.rs
        if let DecodedEvent::Fill(fill) = &event
            && !aggregate_ids.contains(&event_id)
        {
            let match_binding = matched_fills.get(&event_id);
            let execution = execution_from_fill(fill, match_binding, ...)?;
            executions.push(execution);
        }
```

`match_binding` 可以为 `None`，仍写成 `quant_market_execution`。match 之后残留的未绑定 fill 同样会进事实层。这会污染 volume、participant、trade.*，进而污染 bootstrap 模型。

**修复**

- 每个 `(tx_hash, contract)` 分组结束时：若存在 fill 且不存在完整 `maker fills + aggregate taker OrderFilled + OrdersMatched` 配对，返回 `ExecutionProjectionError`（新变体，例如 `UnpairedFill`）。
- worker 将该 chunk quarantine，停止该 frontier watermark。
- 现有“有 OrdersMatched 但缺 aggregate / 缺 maker / aggregate 字段不一致”的错误路径保留。

**测试**

- 仅 maker `OrderFilled`、无 `OrdersMatched` → `Err(UnpairedFill)`，零 execution。
- 完整 V2 四事件（现有 `v2_excludes_aggregate`）仍是 2 executions。
- match 后多余 fill → `Err`。
- 无事件的空 chunk 仍合法。

### P1-2 Gini / HHI 20 地址变成 bootstrap 选市函数

`trade_specs` 11 个特征全部 `NullPolicy::RejectMarket`。concentration 门：

```183:191:crates/quant-pivot-models/src/runtime_config/sections/config.rs
            execution_min_unique_participants: 20,
            execution_min_notional_usd: DecimalValue::new(dec!(100.00)),
            execution_min_coverage_ratio: DecimalValue::new(dec!(0.95)),
```

窗口 24h（`execution_window_secs: 86_400`）。薄市场整段 `RejectMarket`，33 天 pooled 样本可能过不了 `min_sample_count`。PLAN 没有把 20 独立地址写成选市函数。

**修复**

- bootstrap 合同（`TradeBootstrap*`）下，concentration 特征改为 `Optional` 或 `Penalize`，**禁止** `RejectMarket`。
- FullL2 合同可保持更严的 structural 门，但不得泄漏进 Trade 合同。
- 不要降 `min_sample_count` 来掩盖样本塌缩。

**测试**

- 3 个独立 participant、notional 足够：bootstrap 市场进入训练矩阵，Gini 为 missing/penalize，不是整行删除。
- `<3` executions 仍 `RejectMarket`（历史不足，不是浓度门）。

### P1-3 特征 / 报告消费已接受事实，不看后续 quarantine

`structural_monitor` 要求 `quarantine_count == 0`。`feature_pipeline::load_windows` 只读 `latest_accepted(Activation)` 的 `effective_through_at`，**零次**查询 `active_quarantine`。

编排器只在 `accept_coverage` 时检查 quarantine。之后 history worker 若隔离重叠窗口，训练/首报/后续 scheduled 报告仍消费前面的 accepted facts。

**修复**

- `FeaturePipelineService`（以及任何读 `quant_market_execution` 的 serving/训练 prefetch）在 finalized execution 启用时：目标窗口与任何 frontier 的 `active_quarantine` 相交 → fail-closed（typed error，不是跳过市场）。
- 报告路径同样 fail-closed，不要出一份建立在被隔离历史上的 Published 报告。

**测试**

- 接受 33 天窗口后插入 overlapping quarantine → feature pipeline / report build 失败。
- 隔离落在窗口外 → 仍可构建。

### P1-4 hydrate 按 block number 取 header，不绑定 `log.block_hash`

```886:905:crates/quant-pivot-api/src/exchange/history_client.rs
fn hydrate_log_times(...) {
    for log in logs {
        let block = required_block(blocks, log.block_number, provider)?;
        log.block_timestamp = block.timestamp;
        log.parent_block_hash.clone_from(&block.parent_hash);
        ...
    }
}
```

`required_block` 只查 number。digest 已含 timestamp，双源时间不一致会 quarantine；**同号错哈希**仍可能把错误 PIT 时间写进 log 再被接受。中间块 parent 链也未逐块校验。

**修复**

- hydrate 时 `header.hash` 必须等于 `log.block_hash`（规范化后比较，注意 0x 大小写）。
- N 与 N+12 的 header 必须来自**同一 provider 的同一批 headers**，且 `header[n].hash == header[n+1].parent_hash` 对窗口内每一对相邻块成立。
- attestor 与 extractor 的边界 hash / parent / timestamp 已在 `chunks_agree` 比较；不要把时间从单侧偷过来。

**测试**

- log.block_hash ≠ header.hash → hydrate error → chunk 不接受。
- 中间 parent 断裂 → continuity 失败。
- 合法链 → timestamp 进入 digest，两侧 agree。

### P1-5 bootstrap 深度不足时静默丢候选

```1177:1178:crates/quant-pivot-core/src/report/builder.rs
    if visible_depth < min_depth_usd.inner() {
        return Ok(Vec::new());
    }
```

候选进入组合阶段但没有 executable tier，漏斗看不到拒绝原因。

**修复**

- 返回结构化 `TierAdmissionRejection` / funnel reason（例如 `InsufficientLiveDepth`），计入 route funnel，而不是空 Vec。
- 不要把该市场当成“模型没看上”。

**测试**

- mock 可见深度低于阈值 → 报告可 Published 或空报告，但 funnel 有该市场的深度拒绝，不出现“进了 portfolio 却零 tier”的黑洞。

### P1-6 Weather bootstrap 丢掉 `WeatherForecast` fitter

Full weather profile：`policy_fitter: Some(WeatherForecast)`。  
`weather_trade_bootstrap`：`policy_fitter: None`，却叠加 GEFS / aviation / GHCNH 域特征。

垂直模型退化成普通 pooled 学习器，域特征没有对应的政策拟合器。

**修复**

- bootstrap weather 使用 `ResearchPolicyFitter::WeatherForecast`，门禁与样本要求跟 bootstrap 窗口对齐，**不**放宽 CPCV。
- 若样本不足以拟合 weather fitter，run 停在校准/训练阶段并 `blocked_reason` 明确，禁止静默用通用 fitter 顶上。

**测试**

- weather bootstrap spec / 训练路径断言 `policy_fitter == WeatherForecast`。
- 样本不足 → blocked，不是成功激活。

---

## 5. P2 — 应修，不阻断首报语义

### P2-1 ETA 无 5% 预热；SLO 立刻投影

`publish_accepted` 在 `block_rate_milli > 0` 后立刻算 48h Warning / 72h Violation。PLAN 要求首 5% block range 后再估 ETA。首块少计已修好，预热没有。

**修复**：activation 已接受块数 `< 0.05 * (target - from)` 时 `projected_completion_at = None`，`slo_status = Unknown/Warmup`（若枚举没有就加，禁止复用 OnTrack 撒谎）。

### P2-2 attestor 可与结算 RPC 同 URL

`validate` 只禁 attestor host ∈ `{envio.dev, hypersync.xyz, hyperrpc.xyz}`。不禁 `attestor.rpc_endpoint` 与 `polymarket.onchain.rpc_endpoint` 解析后相同。同一故障域冒充“独立见证”。

**修复**：enabled history 时两者规范化 URL（host+path，忽略末尾 `/`）必须不同。相同 → `ConfigValidationError`。

### P2-3 HyperSync 日志多扫 N+12

`fetch_chunk` 的 log query `to_block_excl(confirmation_end+1)`，再 `retain <= to_block`。确认缓冲里的日志会计入响应预算，触发无意义的 chunk 收缩。headers 才需要扫到 N+12。

**修复**：log 查询上界 = `to_block`；header 查询上界 = `to_block + confirmation_blocks`。

### P2-4 readiness 仍按全局 200 天 capture

`research_readiness_worker` 调用 `minimum_raw_retention_days()`（=200）做唯一一次 capture。编排器用 observation 时间窗而不是 `proven()`，所以**通常不阻塞** pooled。Dashboard / 其它消费者若仍读 `retention_ready = proven()`，会显示 200 天未就绪。

**修复**：按 profile `required_days()` 分别取证，或在 snapshot 上拆 `bootstrap_ready` / `full_l2_ready`。不要让全局 200 天灯覆盖 fresh-boot 面板。

### P2-5 无 quarantine 列表 API

`list_quarantine` 仓库方法存在，web 只在 supersede 时查 `active_quarantine`。操作员不能从 API/SPA 列出隔离证据（reason 枚举 + chunk/token/digest）。Dashboard 只有 `quarantine_count`。

**修复**：只读 `GET /system/exchange-history/quarantines`（或挂在 `/system/fresh-boot` 投影里），RBAC 与 system read 对齐。SPA 可从 count 点进列表。不要做“跳过隔离继续扫”的按钮。

### P2-6 `cohort_contract` 除 `required_sources()` 外仍无消费

`CryptoPrice` / `WeatherForecast` 写在 profile 上，selection/training 实际靠 `category` + `market_selector`。允许保持，但必须在训练/选市路径上 **显式**执行 cohort，或删掉未执行的枚举变体（破坏式，改 content hash）。禁止第三种：枚举存在着像隔离。

### P2-7 文档 / 配置清单仍写 trade-tape

`crates/` 已无运行时引用。仍漂移：

- `docs/audit/quant-pivot-current-config-field-inventory.md`（`trade_tape_on_chain`、`max_trade_tape_age_secs`）
- `docs/plans/quant-pivot/08-extreme-performance-design.md`、`09-extreme-performance-ledger.md`、`phase-11/11.2.1.1-trade-tape-participant-concentration.md`
- `docs/codex-plans/quant-pivot-typed-persistence-sql-query-ui-fresh-boot-final-closure-plan.md`

**修复**：删除或改写为“已删除，唯一路径是 `finalized_exchange_history`”。禁止留下可执行字段名。legacy 审计原文 2026-08-13 §1 的回填配方可加一行“已被 S1 实现否定，见本文件”，不要改写历史审计正文。

### P2-8 合约 registry 未显式排除 Combos / 非官方 NegRisk B

官方 Contracts 页：V2 CTF `0xE111…996B`、NegRisk `0xe222…0F59`。Combos Exchange `0xe333…` 是独立 combo/RFQ。非官方镜像上的 NegRisk B 不在本系统。

当前 4 合约（CTF/NegRisk × V1/V2）合理，但应在 `constants.rs` 用注释 + 单测列出 **明确排除的地址**，避免下次有人“顺手加上”。

### P2-9 CPCV 在校准之后；FeatureParity 是内联而不是 ResearchJob

设计文档顺序是 Train → CPCV → Cal → Parity job。实现是 Train → Cal dataset → Cal → CPCV → 内联 `verify_and_record`。行为可接受（CPCV 评估校准后模型），但 `finish_cpcv` 只检查 job success + path_set 身份，依赖 job 内部 fail-closed 门禁。

**修复（可选但建议）**：`finish_cpcv` 再读 path_set 的 DSR/PBO/gate 记录，断言 `PredictiveUtility` 且经济回放 `NotApplicable`。不要把 CPCV 改回“跳过门禁”。

### P2-10 UnknownToken 停整段 frontier，缺 per-token 可见性

fail-closed 正确。缺：quarantine 详情 API（见 P2-5）、token_id 出现在 SPA、可恢复投影（身份补齐后 resolve quarantine 重扫**同一**区间，已有 resolution 表）。补测试：UnknownToken → frontier 停、count+1、resolve 后才继续。

### P2-11 CH rewind 是软删除（`acceptance.active=0`）

可接受为 v1，但 serving 读者必须 `argMax(active, state_revision)=1`。审计重放能力未完成（raw/event/match 只写不读）。本轮不必做物理 delete。

---

## 6. 建议落地顺序

1. **P0-1** serving 宇宙 = 已激活 `buy_routes`（报告选市 + represented_routes + 编排器 enable_report + 系统测试）。
2. **P1-1** unpaired fill → projection error + quarantine + 单测。
3. **P1-2** bootstrap concentration 不再 `RejectMarket`。
4. **P1-3** feature/report 消费路径检查 `active_quarantine`。
5. **P1-4** hydrate hash + interior parent 链。
6. **P1-5** 深度不足进 funnel。
7. **P1-6** weather bootstrap 恢复 `WeatherForecast` fitter。
8. P2 按 2 → 1 → 3 → 5 → 7 → 8 → 4 → 6 → 9 → 10 处理。文档漂移可与代码同一 PR 或紧随其后，不要只改文档。

---

## 7. 质量门（收尾 PR 必须过）

```bash
cargo fmt --all --
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask architecture audit-functions
cargo xtask architecture check
cargo test --workspace
```

相关聚焦测试至少覆盖：

- `crates/quant-pivot-api` projector：unpaired fill、aggregate mismatch、V2 双计排除
- `history_client`：hash mismatch hydrate、chunks_agree、parent 断裂
- `quant-pivot-system-tests`：pooled-only 首报 represented=[Pooled]；缺 route fail-closed；quarantine 后 feature/report 失败
- UI：`fresh-boot-presentation` / dashboard snapshot 在 pooled ReportEligible 且无 crypto champion 时状态不是“卡在 vertical”

---

## 8. 明确不要做的事

- 不要恢复 `trade_tape`、`/prices-history` 事实、HyperRPC、HyperSync+RPC fallback。
- 不要为了出报告降低 CPCV/DSR/PBO/`min_sample_count`/confirmation=12/rollback=200。
- 不要把 live L2 细胞写进 `TradeBootstrap*` `FeatureVector`。
- 不要加兼容 shim、`pub use` 转发、`ConfiguredFeatureBuilder::new` 默认 FullL2。
- 不要把 `FreshBootStage` 与 history `ExchangeHistoryStage` 重新焊回同一个会撒谎的枚举。
- 不要添加“管理员强制下一阶段”按钮。retry-now / supersede 必须仍受同一证据与门禁约束。
- 不要把空 `enabled_categories` 改成非法。

---

## 9. 前序关系

| 文件 | 关系 |
|---|---|
| [`2026-08-13-full-system-deep-audit.md`](2026-08-13-full-system-deep-audit.md) §1 | 原始 S1。回填配方中的 `/prices-history` 与 `quant_trade_tape_*` **已被否定**，不要按原文实现。 |
| [`2026-08-15-s1-fresh-boot-closed-loop-audit-and-design.md`](2026-08-15-s1-fresh-boot-closed-loop-audit-and-design.md) | 上一轮设计。其中 P0-1 FullL2、P0-2 编排器、P0-3 Admin、P0-4 `NonVerticalPooled`→`AllEligible` **已落地**。本文件是对该设计实现后的复审；P0 以本文件为准。 |
| PLAN.md | 时间表与权威层级仍有效。12–36h 首报必须按本文件 P0-1 的 serving 宇宙才能成立。 |

---

## 10. 验收标准（本轮收尾做完才算 S1 闭环）

1. 新进程 fresh boot，activation frontier 双源接受、quarantine=0。
2. 编排器自动完成 pooled dataset/train/CPCV/parity/bootstrap，**不等** crypto/weather run。
3. 默认空 `enabled_categories`、catalog 含 Crypto 市场时，第一份 **Published** `RecommendationReport` 的 `represented_routes` 只有 Pooled，预测向量只有 `trade.*`（+ 该合同允许的域特征），sizing 使用 live L2。
4. 该报告不能创建 OrderIntent（ReportOnly + bootstrap provenance）。
5. 未配对 fill 的 chunk 被隔离，watermark 不前进。
6. Dashboard `GET /system/fresh-boot` 显示的 pooled 阶段与 PG 状态机一致，不再出现 ReportEligible 但报告永远失败。
7. `cargo test --workspace` 与 architecture check 全绿。
