# quant-pivot 全系统深度审计（2026-08-13）

> **Superseded**：这是日期化历史审计，不再定义 S1 实现合同。涉及旧成交事实、回填窗口或首报 gate 的内容以当前 finalized exchange-history、profile-specific FreshBoot 与运维文档为准。
>
> **§4 replacement**：Feedback 闭环、`ReportOnly`、人工/外部成交、break-glass 与 MTM
> 快速经济反馈的 current 设计和实施状态只认
> [`../plans/quant-pivot/phase-12/README.md`](../plans/quant-pivot/phase-12/README.md)；
> 本文 §4/R7/R9/旧路线图只作审计历史，不得恢复为实现合同。

> **范围**：代码质量、优雅性、极致性能、算法、回测、训练、feedback 闭环
> **方法**：两轮。第一轮 581,626 行 Rust 全量结构化探查 + 业界/学术对标；第二轮逐行交叉验证第一轮的"做得好"结论（统计公式对照原始论文），并补齐第一轮未覆盖的维度（存储层、并发正确性、Polymarket 领域适配）
> **立场**：defect-first。不接受"契约写了但未落地"；不接受"文档声称但无证据"；对做得好的部分明确背书；对第一轮自己的误判明确纠正
> **前序**：[`2026-07-31-phase-11.9-closed-loop-deep-audit.md`](2026-07-31-phase-11.9-closed-loop-deep-audit.md)、[`2026-08-05-phase-11.9-w6-business-loop-reaudit.md`](2026-08-05-phase-11.9-w6-business-loop-reaudit.md)
>
> 本报告刻意**不重复**前序审计已登记的残余项（lease-loss settle_retry、Policy CF 单策略、alert 接线、README 漂移）。那些结论仍然成立。
>
> **章节导航**：§0–10 第一轮（战略与平面审计）；§11–17 第二轮（公式交叉验证、存储层、并发容错、Polymarket 领域陷阱）；§18–24 第三轮（风控控制面、安全与依赖漏洞、报告管线与组合优化、治理执行器）；§25 总评。
>
> **风险登记册分布**：R1–R14 见 [§9](#9-风险登记册第一轮)，R15–R33 见 [§15](#15-第二轮风险追加)，R34–R50 见 [§23](#23-第三轮风险追加)。

---

## 0. 一句话结论

**这是一套工程质量罕见的量化系统——它的"防止亏钱"机制（治理、PIT、CPCV/DSR/PBO、MILP exact verification、fail-closed）做到了机构级；但它的"赚到钱"机制（alpha 来源）押注在单一路径上，而这条路径需要 200 天数据预热，且该预热在技术上是可以规避的。**

**第二轮补充：即使解除了冷启动，系统目前也下不出订单**——tick 对齐、订单精度、最小订单量、allowance 四项 venue 侧约束在生产路径上缺失或只做了一半，修复成本约 2–3 天。

**第三轮补充：风控可以被单个操作员合法关闭，且全程留下"合规"的审计记录。** 三处配置校验缺口加上审批与激活无职责分离，修复成本不到 100 行。

五个最高价值的发现，按影响排序：

| # | 发现 | 影响 | 可执行性 |
|---|------|------|---------|
| **S5**（三轮） | **风控可被单人合法掏空**：入场滑点零校验、`min_sample_count` 零校验、`max_drawdown` 允许设为 1.0、approve 与 activate 不比较操作者 | 风控失效且不可察觉 | **极高，<100 行** |
| **S1** | 200 天冷启动等待建立在"Polymarket 历史数据不可回填"的**错误前提**上。成交流与价格历史均可完整回填 | 上线时间从 ~200 天压缩到 ~2 周 | 高，2–4 周工作量 |
| **S4**（二轮） | **订单可提交性四缺口**：入场价不做 tick 对齐、size/amount 精度舍入未实现、最小订单量未进生产逻辑、下单前不校验 allowance。三条对应官方明文错误码 | 报告产出但无法成交 | **极高，2–3 天** |
| **S2** | 学术实测 Polymarket 12 个月被提取的 $39.59M 套利中，**99.76% 来自不需要任何模型的确定性再平衡套利**；本系统把它建成了特征，没建成策略 | 错过验证过的最大 alpha 池 | 中，需新增执行路径 |
| **S3** | 经济模型完全没有 maker/taker rebate（15–25% 费用返还），且训练目标（mid return）与考核目标（net-of-cost realized return）不一致 | 系统性低估 maker 路径收益 + 训练效率损失 | 高，局部改动 |

评分矩阵（含第二轮新增维度）：

| 维度 | 评级 | 依据 |
|------|------|------|
| 代码质量与优雅性 | **A-** | clippy pedantic+nursery 全 target `-D warnings` **实测零告警通过**；生产 `unwrap` 17 处、`unsafe` 0、`todo!` 0、`QuantError::Internal` 0。扣分在上帝文件与超长编排函数 |
| 数值与统计实现 | **A-** | 二轮逐行对照原始论文：PSR/DSR/PBO/Spearman/PAVA/Platt **无代数错误**。扣分在 `stddev` 文档陷阱、MinTRL 仅特例 |
| 极致性能 | **B+** | 架构不变量真落地（8 分区 actor / ArcSwap / ByteString / jemalloc）；扣分在 Linux SLO 未签收 + JSON 热路径未优化 |
| 并发正确性 | **B+** | seqlock 实现经逐行核对正确、跨 await 持锁 0 处、gap/delta fail-closed 扎实；扣分在 loom 只有 2 个玩具测试、WS 无时钟校验 |
| 算法平面 | **B** | 特征/因子/校准/MILP 都扎实；扣分在无因子 IR、无 regime 层、策略池单一 |
| 回测平面 | **A-** | CPCV φ-path + full-L2 walk + 真实费用曲线，强于绝大多数研究栈；扣分在零执行延迟假设 |
| 训练平面 | **B** | 数据集内容寻址 + 泄漏扫描 + 时间序切分做得好；扣分在无 triple-barrier、无 AFML 样本权重、无 early stopping |
| Feedback 闭环 | **B-** | 软件主环真闭合；扣分在 ReportOnly 人工成交断链 + 无 MtM 快反馈 |
| 存储层 | **B-** | PG 索引/typed JSONB/outbox 扎实（416 索引、0 处 JSONB path 查询）；扣分在 CH 无 TTL 容量失控、statement-count 仅 4 个、连接池偏紧 |
| 组合优化（MILP） | **A** | 三轮里工程正确性最强的一块：精确整数锁无 ε、唯一性 Hamming 证明、`mip_gap=0`、超时/溢出一律 fail closed 无 fallback |
| 场景模型 | **C+** | 联合 bootstrap 结构扎实、PIT 纪律严格；**但概率权重是不进 trial grid 的影子超参，且分布覆盖率从未被经验校验**，而 CVaR/robust 建立其上 |
| 安全（密钥/认证） | **C+** | `SecretText`、JWT 固定 HS256 + 原子 refresh family、Casbin 默认拒绝、WORM 审计做得好；**扣分在私钥无 KMS、`OrderSigner` 不清零、JWT 无轮换、登录无限流** |
| **风控控制面** | **D** | **三处校验缺口 + 无职责分离 → 单人可合法关闭风控**；`Critical` 风险标记只驱动 UI 不驱动流程 |
| 治理执行器 | **B** | 主体 syn AST 可靠、CI 强制无 `continue-on-error`；扣分在 34 处字符串针、`cfg(test)` 截断、allowlist 无审计、`config audit` 不在 CI |
| 测试有效性 | **B** | 2216 个测试、负例断言约 900 处（41%）、`retries=0` 不掩盖 flaky；**扣分在无 mutation/fuzz/覆盖率度量** |
| **Polymarket 领域适配** | **C** | NegRisk/pUSD/proxy/V2/非 0-1 payout 正确；**订单可提交性四缺口 + UMA 争议未建模 + 无地理封锁适配** |
| **可运营性** | **D** | **今天无法产出第一份有价值报告**，卡在数据 runway；即使解除也卡在 S4 |

---

## 1. S1 — 200 天冷启动建立在错误前提上（最高优先级）

> **后续（2026-08-15）**：落地复验与优化设计见 [`2026-08-15-s1-fresh-boot-closed-loop-audit-and-design.md`](2026-08-15-s1-fresh-boot-closed-loop-audit-and-design.md)。结论：数据面接近机构级，但 serving 合同、自动编排与 Admin UX 未闭环，不能按 S1 已落地验收。

### 1.1 现状

系统设计要求 200 天 raw retention 才能训练首个模型：

```838:851:crates/quant-pivot-models/src/types/research_profile.rs
/// Runtime v1 raw-retention floor:
/// `max(180, 2 × max(required_days(profile)))`.
pub fn minimum_raw_retention_days() -> Result<u32, String> {
```

设计文档明确拒绝任何回填：

```231:234:docs/plans/quant-pivot/08-cold-start-production-closeout.md
- The pre-reset store had 1,944,000 rows spanning about 91 days. The required
  history remains 200 days; after the authorized clean reset the local evidence
  store is empty and therefore remains fail-closed rather than being backfilled
  with fixtures or extrapolated data.
```

每个 research profile 都把 `ClobL2` 列为 required source，缺失即整体 fail closed：

```757:762:crates/quant-pivot-models/src/types/research_profile.rs
            required_sources: vec![
                ResearchProfileDataSource::CatalogLedger,
                ResearchProfileDataSource::ClobMarketInfo,
                ResearchProfileDataSource::ClobL2,
                ResearchProfileDataSource::TradeTape,
            ],
```

Binance 与天气源都有 archive 回填字段，唯独 Polymarket 没有：

```1061:1062:crates/quant-pivot-models/src/config/domain_sources.rs
    /// Official Binance bulk-data archive root used for historical PIT backfill.
    pub archive_url: String,
```

### 1.2 前提为什么是错的

"Polymarket 历史数据不可回填"只在**完整 L2 深度快照**这一项上成立。实际可得性分层如下：

| 数据 | 可回填？ | 来源 | 精度 |
|------|---------|------|------|
| 结算结果（标签） | **可以** | Polygon CTF `ConditionResolution` 日志 | 精确。**系统已实现读取器** |
| 成交流 trade tape | **可以** | 链上 `OrderFilled` 事件（V2 CTF Exchange `0xE111…996B` + NegRisk `0xe222…0F59`），经 Envio HyperSync 全链扫描 | 逐笔，含 price/size/side/maker-taker/fee |
| 价格历史 | **可以** | 官方 CLOB `/prices-history?market=&interval=&fidelity=` | 最细 1 分钟 |
| 市场元数据 | **可以** | Gamma / CLOB `/markets` | 完整 |
| **L2 深度快照** | **不可以** | 只能自录 WS，或买第三方存档 | — |

注：Polymarket 于 2026-04-28 迁移 V2 合约并停用 subgraph，旧 subgraph 路径确实失效——但这被"整体不可回填"吸收了。HyperSync 路径是免费的（需 API token），单连接可扫全链，公开开源实现已验证可行。

系统当前**一条回填通道都没接**：

```
crates/ 内 grep prices-history / OrderFilled / hypersync / goldsky → 0 命中
trade tape 唯一来源：crates/quant-pivot-api/src/exchange/normalize.rs（实时 WS）
```

### 1.3 影响量化

六族特征在"无 L2、有 trade tape + 价格历史"条件下的可算性：

| 特征族 | 无 L2 时状态 | 说明 |
|--------|------------|------|
| MarketMetadata | **完全可算** | category / TTR / neg_risk / is_active 全部来自 Gamma |
| TimeSeries | **完全可算** | return / vol / ROC / EMA slope / MACD_norm 只需价格序列 |
| Structural | **大部分可算** | shock/reversal/price_extremity 需价格；participant Gini/HHI 需 trade tape（可回填）；negrisk leg sum 需各腿价格（可回填） |
| Microstructure | **部分可算** | 成交侧的 churn / adverse selection 可从 tape 近似；queue depletion 需 L2 |
| Domain | **完全可算** | crypto 走 Binance archive，weather 走 NOAA archive，均已有回填 |
| **PriceBook** | **不可算** | best_bid/ask、spread_bps、depth_imbalance、slope、visible_liquidity 全部需 L2 |

即：**六族中四族半可以在回填数据上完整重建**。当前架构把它们和 PriceBook 绑死在同一个 required_sources 里，一起 fail closed。

### 1.4 建议：L2-free bootstrap profile

新增一个 `ResearchProfileDataSource` 组合与配套 profile，**不含 `ClobL2`**：

1. 新增回填 worker：链上 `OrderFilled` → `quant_trade_tape_*` fact；`/prices-history` → 价格序列 fact。两者都要打 `available_at` 与 source lineage，走既有 PIT 契约。
2. 新增 `ResearchProfile::bootstrap_l2_free()`，required_sources 去掉 `ClobL2`，把 PriceBook 族整体标记为 `NullPolicy::Optional` 而非 `RejectMarket`。
3. 标签仍用 `token_payout_ratio`（链上结算，可回填），CPCV/DSR/PBO 门禁**不放宽**。
4. 该 profile 训出的模型只能进入 ReportOnly bootstrap，并在 route 上标记 `l2_fidelity: None`，让 `min_l2_book_fidelity_ratio` 门禁对它 not-applicable 而非 fail。
5. 实时 L2 继续积累；满 200 天后训练 full profile，用既有 shadow/comparison 机制自然替换 v0。

**收益**：上线时间从 ~200 天压缩到"回填耗时 + 训练耗时"，量级是天。同时 v0 模型在 200 天里持续产出报告、积累 PolicyEvaluation cohort，让 feedback 闭环提前进入热身——而不是空转 7 个月。

**风险控制**：v0 的执行经济学仍然需要实时 L2（下单时的可成交价），这不受影响——回填只用于**训练与回测**，不用于**执行**。分离这两者正是本系统 PIT 架构已有的能力。

---

## 2. S2 — 策略池与已验证 alpha 分布严重错配

### 2.1 学术实测数据

IMDEA Networks / Oxford 的 AFT 2025 论文《Unravelling the Probabilistic Forest: Arbitrage in Prediction Markets》(arXiv:2508.03474) 用 8600 万条链上 `OrderFilled` 事件测量了 2024-04-01 至 2025-04-01 整年 Polymarket 上**已实现**（不是理论存在）的套利利润：

| 策略 | 已实现利润 | 占比 |
|------|-----------|------|
| 单市场再平衡 — **buying NO** | **$17,307,114** | **43.7%** |
| 单市场再平衡 — buying YES | $11,092,286 | 28.0% |
| 单条件再平衡 — buy below $1 | $5,899,287 | 14.9% |
| 单条件再平衡 — sell above $1 | $4,682,075 | 11.8% |
| 单市场再平衡 — selling YES/NO | $616,453 | 1.6% |
| **跨市场组合套利** | **$95,157** | **0.24%** |
| **合计** | **$39,587,585** | 100% |

论文同时给出两个次级结论：单个最高利润账户全年提取 $2,009,632；**体育类市场在套利图上几乎缺席**（"Sports are largely absent from the plots – maybe a less explored venue for arbitrageurs"）。

### 2.2 本系统的定位

系统把 neg-risk 一致性建成了**特征**：

```129:140:crates/quant-pivot-research/src/features/names.rs
    /// Sum of best-ask across all neg-risk YES legs (drift = sum − 1).
    pub const NEGRISK_LEG_ASK_SUM: ...
    /// Neg-risk conversion edge (buy YES basket of all-but-favorite vs NO-favorite).
    pub const NEGRISK_CONVERT_EDGE: ...
```

但没有建成**策略**。全仓库 grep `arbitrage` / `rebalanc` 无任何执行引擎命中，`splitPosition` / `mergePositions` 只出现在结算赎回路径与测试合约里：

```34:34:crates/quant-pivot-api/src/settlement/adapter.rs
use self::SettlementAdapterWrite::redeemPositionsCall;
```

即：系统能**赎回**已结算头寸，不能**主动 split/merge** 构造篮子。而论文描述的 short rebalancing 标准做法正是"对每个 condition 用 1 USDC 做 Split，然后立刻卖出 YES"。

### 2.3 判断

这不是"缺一个功能"，而是**策略假设的选择**：系统押注"用 ML 预测方向性 mid return"这一条路径，而实测数据显示这个市场上被真金白银提取的钱，99.76% 来自"检查当下盘口是否违反概率公理"——一个不需要模型、不需要历史数据、不需要 CPCV、当天就能跑的确定性计算。

三点补充判断：

1. **难度不在检测，在执行**。论文明确指出 Polymarket 的订单簿性质使这些交易**非原子**（"each of the above trades is non-atomic; thus, there is always some risk"）。真正的工程壁垒是多腿并发下单、腿风险管理、失败回滚——**而这恰恰是本系统已经建好的部分**：8 分区 token-affine actor、durable-then-publish、有界背压 fail-closed、OrderIntent 状态机、admission checks。基础设施已经在了，缺的是上面那层策略。

2. **跨市场组合套利不值得做**。0.24% 占比，13 个人工确认的依赖对里只有 5 个产生了利润。LLM + embedding + Frank-Wolfe 那套复杂度收益比极差。**不要被论文的方法论复杂度吸引到错误方向**。

3. **体育类是空白区**。论文观测到套利者几乎不碰体育，而体育市场数量多、结算快（小时到天级，而非月级）——这对本系统的**闭环延迟**问题（见 §4）是天然解药：体育市场的标签成熟速度比政治/宏观快一到两个数量级。

### 2.4 建议

新增一条与 ML 报告管线**并行**的确定性策略路径：

- `RebalancingScanner`：消费既有 `DataPlaneIndex` 快照，对每个 neg-risk event 计算 Σ(best_ask over YES legs) 与 Σ(best_bid)，对每个二元市场计算 YES_ask + NO_ask。已有的 `NEGRISK_LEG_ASK_SUM` / `NEGRISK_CONVERT_EDGE` 特征计算逻辑可直接复用。
- 边际必须走**现有的** `walk_buy_exact_shares` 全深度 walk + `PitFeeSchedule`，扣完费用与滑点后仍 > 阈值才成立。论文的执行下限是 $0.05/单位边际——低于此被执行现实吃掉。
- 多腿下单复用现有 `OrderIntent` + admission，新增 leg-risk 状态机（部分成交后的敞口处理）。
- 这条路径**不经过** CPCV/DSR/PBO——它不是统计 alpha，是账面恒等式违约，验证方式是确定性单元测试 + 影子对账，不是 backtest 显著性。
- 优先接入体育类目（空白区 + 快结算）。

---

## 3. S3 — 成本模型缺 rebate；训练目标与考核目标不一致

### 3.1 费用曲线本身是对的

费用实现是 PIT 从 venue 元数据解析的动态曲线，不是硬编码：

```86:116:crates/quant-pivot-research/src/execution_semantics.rs
        let curve_base = price.inner() * (Decimal::ONE - price.inner());
        let curve = curve_base.powd(self.exponent);
        // platform_rate * curve + builder; 5dp MidpointAwayFromZero
```

并有对齐 SDK 生产向量的 golden test（rate 0.03/0.04/0.05/0.072，exponent=1）：

```719:726:crates/quant-pivot-research/src/execution_semantics.rs
        for (price, rate, expected) in [
            (dec!(0.5), dec!(0.03), dec!(1.5)),
            (dec!(0.5), dec!(0.04), dec!(2.0)),
            (dec!(0.5), dec!(0.05), dec!(2.5)),
            (dec!(0.5), dec!(0.072), dec!(3.6)),
        ] {
```

这与 Polymarket 2026 现行的 `Fee = C × feeRate × p × (1−p)` 及分类费率（crypto 0.07、sports/economics/culture/weather/other 0.05、politics/finance/tech/mentions 0.04、geopolitics 0）一致。**这块做得好，且是从 venue 读的，不会因费率调整而失真。**

### 3.2 缺口一：rebate 完全未建模

全仓库 `rebate` 只有 1 处命中，在 `gamma/wire.rs` 的无关字段。而 Polymarket 现行有两个返还计划：

- **Maker Rebates**：maker 费率为 0，且收取的 taker 费用按 15%（sports）/ 20%（crypto）/ 25%（其余）每日返还给做市方，按单个市场内的 maker 成交份额分配。
- **Taker Rebates**：2026-05-28 上线，按 30 日加权成交量分层，每日以 pUSD 返还。

系统**支持** maker 路径（`EntryOrderPolicy::Passive` + post_only + GTD，退出监控用 GTC）：

```812:820:crates/quant-pivot-core/src/execution/intent_service.rs
        EntryOrderPolicy::Passive {
            limit_price,
            post_only,
        } => {
            if !post_only {
                return Err(ExecutionError::IntentDenied {
                    reason: "passive entry policy must be post-only".to_owned(),
                });
            }
```

但经济模型里 maker 的收益只体现为"fee = 0"，没有 rebate 收入项。这意味着 MILP 在 aggressive 与 passive 之间做权衡时，**系统性低估 passive 路径的经济价值**，低估幅度等于该市场 taker 费池的 15–25% × 自身 maker 份额。在流动性薄的小市场（本系统的目标区间），单一 maker 的份额可能很高，低估幅度不可忽略。

**建议**：在 `PitFeeSchedule` 增加 `maker_rebate_share: Decimal`（从 venue 元数据 PIT 读取，与 fee 同一 schedule_hash），在 `LiquidityRole::Maker` 分支返回负费用或独立的 `expected_rebate` 字段。注意 rebate 是**日结的、按份额分配的**，是估计量而非确定量——应作为独立的、带不确定性的经济项进入场景分布，而不是混进确定性的 `cash_outlay`。

### 3.3 缺口二：训练目标与考核目标不一致

- **训练目标**：`return_to_horizon` = **mid 价** 到 horizon 的 bps 收益

```59:60:crates/quant-pivot-research/src/training/labeler.rs
            let (semantic_version, semantic_key) = if *name == RETURN_TO_HORIZON {
                (1, "mid-return-at-horizon-bps@1")
```

- **考核目标**：CPCV path 的 `median_rank_ic`，来自回测的 `realized_return_bps`（全深度 L2 walk + 费用，net-of-cost）

```22:28:crates/quant-pivot-research/src/backtest/metrics.rs
pub fn rank_ic(samples: &[SampleOutcome]) -> Decimal {
    let scores: Vec<Decimal> = samples.iter().map(|s| s.composite_score.inner()).collect();
    let realized: Vec<Decimal> = samples.iter().map(|s| s.realized_return_bps).collect();
```

```838:844:crates/quant-pivot-research/src/gates/model_quality.rs
        ledger.hard(
            GateId::RankIc,
            path_set.median_rank_ic >= thresholds.rank_ic_min,
```

**门禁是 net-of-cost 的——这点做得对，值得强调**。问题在于优化器优化的东西和门禁考核的东西是两个量。在 Polymarket 上这个 gap 特别大：流动性一般的市场 spread 常在 4–6%，而 p=0.5 处 taker fee 是 1.75%（crypto）到 1%（politics）。一个 mid-return rank IC 优异的模型，扣完 half-spread + fee + slippage 后可能整体为负。

具体后果是**训练效率损失**：优化器在 mid 空间里爬坡，门禁在 net 空间里筛选，中间隔着一层与因子无关的成本噪声。表现为"训练指标好看但过不了门禁"，且这个失败模式在 CPCV 跑完前不可见（CPCV 是 56 folds × 21 paths，计算昂贵）。

**建议**（按投入产出排序）：

1. **低成本改法**：在 `TrainingExample` 上增加一个 PIT 的 `entry_cost_bps`（decision_at 时刻的 half-spread + 预期 fee，从已有的 L2 walk 逻辑算），把训练标签改为 `return_to_horizon - entry_cost_bps`。标签语义版本从 `mid-return-at-horizon-bps@1` 升到 `net-return-at-horizon-bps@2`，走既有的 `label_schema_hash` 破坏式变更流程。
2. **正确改法**：标签直接用可成交价——入场用 `walk_buy_*` 的 all-in 均价，出场用 `walk_sell_*`。系统已有 `hold_vs_exit_alpha_bps` 标签就是这么算的，把同样的语义推广到入场侧即可。
3. **兜底**：如果两者都不做，至少在 gate 前加一个便宜的 net-of-cost 预筛（单路径 replay，不跑 CPCV），避免把注定失败的候选送进 56 folds。

---

## 4. Feedback 闭环 — 软件闭合，业务未闭合

前序审计已确认软件主环（15-stage DAG → CandidateReady → 人工 permit/activate）真实闭合，本节只登记**前序未覆盖**的两个结构性断点。

### 4.1 ReportOnly 人工成交无法回流（设计后果，不是 bug）

默认模式下永不创建 intent：

```68:71:crates/quant-pivot-core/src/execution/mode_gate.rs
        // 1. report_only never creates an intent.
        if mode == QuantRuntimeMode::ReportOnly {
            return Ok(IntentPolicyDecision::ReportOnly);
        }
```

Execution reconciliation 以 `order_intent_id` 为唯一身份锚点，人工在 Polymarket UI 下的单没有 intent，因此**永远不会**产生 `ExecutionAttemptOutcome` / `RecommendationExecutionRollup`。

**但这个断点的严重性低于直觉**，因为 feedback 的两条学习通道对它的依赖不同：

- `PolicyEvaluation` cohort：只需**已发布的 TopN 推荐** + 市场结算，execution 可为 `None`
- `ModelLearning` cohort（成熟标签的来源）：只需 `token_payout_ratio`，即**市场结算**，与本系统是否成交无关

```116:132:crates/quant-pivot-core/src/service/feedback_cohort.rs
fn evaluate_policy(...) -> Result<FeedbackCohortDecision, ...> {
    let resolution = visible_resolution(...)?;
    let execution = visible_execution(...)?;
```

所以 ReportOnly 下**模型仍然能学习**（学的是"我的推荐方向对不对"），断的是**执行质量学习**（滑点、成交率、时机）。这是一个真实但可接受的降级。

**风险在于它不可见**：系统没有任何机制告诉操作者"你手工执行的这批推荐，实际成本比模型假设的高 X bps"。建议增加一条轻量的人工回填通道——操作者上传或录入成交记录（token / price / shares / timestamp），映射到 recommendation，走独立的 `ManualExecutionOutcome` 类型（明确标记为 operator-attested 而非 venue-reconciled，不进入需要密码学证据的路径）。这不破坏任何既有不变量，因为它是新增的、显式降级标记的证据类别。

### 4.2 长结算 + 无 mark-to-market 快反馈

标签必须等结算：

```483:510:crates/quant-pivot-research/src/training/labeler.rs
        let Some(resolution) = input.forward.resolution.as_ref() else {
            return LabelBuildOutput::NotMature {
                available_after: input.forward.data_available_until,
                reason: LabelDelayReason::SettlementPending,
            };
        };
```

Coverage 门槛是 500 个成熟标签 + 50 个新增 + 95% 覆盖率。在政治/宏观类目上，单个市场从开盘到结算是数周到数月，意味着**一个完整 feedback cycle 的周期以季度计**。Shadow 阶段提供的是"决策重叠度"快信号，不是经济 PnL 快信号。

这与 §2.4 的体育类目建议形成合力：**体育市场小时到天级结算**，能把 feedback 周期压缩一到两个数量级。如果要保留纯 ML 路径，体育类目是让闭环真正转起来的最短路径。

另一个选项是引入 mark-to-market 中间标签（用 horizon 时刻的可成交出场价而非终局 payout），作为 `LabelDelayReason::SettlementPending` 的降级替代。系统已有 `MAX_FAVORABLE_EXCURSION_BPS` / `hold_vs_exit_alpha_bps` 这类前向可成交标签，扩展成本不高。

---

## 5. 算法平面

### 5.1 做得好的（明确背书）

| 项 | 证据 |
|---|---|
| 六族特征 + 硬 PIT 契约 + `DecisionBoundary` | `features/mod.rs:67-71`、`pit/mod.rs:60-62` |
| **四态 null policy，禁止静默零填充** | `features/schema.rs:163-177`、`null_policy.rs:1-6` |
| 横截面 winsorize→z-score→clamp，小样本走训练期冻结 CDF | `normalize/cross_section.rs:129-133`、`computer.rs:492-515` |
| 权重经 LTR simplex 搜索（非手写表），serving 从密封 artifact 读 | `model/trainer.rs:1-18`、`weighted/mod.rs:487-489` |
| Isotonic/Platt 校准，**强制不相交的 holdout**（fit hash ≠ validation hash） | `calibrator/mod.rs:239-262` |
| 字典序 MILP（robust → nominal → CVaR → capital-hours）+ Decimal exact verify | `portfolio/global.rs:1328-1387`、`1406-1428` |
| 求解非 Optimal 即 fail closed，不返回部分计划 | `solver_boundary.rs:574-586` |
| 唯一 f64 边界收敛在 HiGHS 接口，有 exact-integer 护栏 | `solver_boundary.rs:1-21` |

其中"四态 null policy 禁止静默零"和"校准强制不相交 holdout"是很多机构量化栈都做不到的细节，值得明确肯定。

### 5.2 缺口

| 缺口 | 判定 | 说明 |
|------|------|------|
| 因子 IR（mean(IC)/std(IC)） | **缺失** | 有 RankIC（点估计），无 IC 时序稳定性度量。因子筛选缺少"稳定性"这一维 |
| Regime 检测层 | **基本缺失** | 只有 `volatility_regime` 这类因子级 proxy，无状态机。CPCV 的已知弱点正是"假设未来是历史 regime 的重组" |
| 在线共线性处理 | **缺失** | Spearman 矩阵只做离线 CI 诊断（`factors/collinearity.rs:1-9`），推理时不正交化、无 VIF 剔除 |
| 退化统计返回 0 而非 undefined | **风险** | `stats.rs:24-28,54-56` 零方差时 Spearman 返回 0，与"真的无相关"不可区分 |
| 动态市场冲击模型 | **缺失** | 只吃可见深度，无 temporary/permanent impact 分解。在薄市场上会低估大单成本 |
| Kelly sizing | **刻意删除** | 已有设计文档记录，改为离散 tier + MILP。这是合理的选择，不算缺陷 |
| 显式协方差约束 | **缺失（有替代）** | 用 scenario robust + CVaR 隐式表达相关性。**替代方案的有效性完全取决于 `scenario_model` 的联合分布假设质量**——这是一个未被独立验证的单点 |

最后一项值得单独强调：把协方差替换成场景分布是一个合理的架构选择，但它把"分散化是否真的分散"这个问题从"协方差矩阵估计"转移到了"场景生成质量"。`portfolio/scenario_model.rs` 有 2873 行，是整个组合层的隐含单点。建议对它做独立的历史校验——用已结算的历史数据检验场景分布的经验覆盖率（生成的 P5/P95 区间是否真的覆盖 90% 的实现值）。

---

## 6. 回测与训练平面

### 6.1 显著强于业界常见做法的部分

CPCV 实现忠实于 López de Prado Ch.7/12 + mlfinlab 的 φ-path 重构，且 purge 是按**标签区间重叠**而非仅特征时间戳：

```1:15:crates/quant-pivot-research/src/validation/cpcv.rs
//! Combinatorial Purged Cross-Validation with full φ-path reconstruction.
//! Purge and embargo follow López de Prado, *Advances in Financial Machine
//! Learning*, Ch. 12; path reconstruction follows the `mlfinlab`
//! `CombinatorialPurgedKFold._fill_backtest_paths` construction
```

Embargo 取 `max(embargo_pct × span, min_embargo_secs)`，后者由 feature lookback 注入——这正好回应了业界对 CPCV 最常见的批评（"purge 参数容易设错，太小漏、太大浪费"）：

```260:286:crates/quant-pivot-research/src/validation/purge.rs
    Ok(if pct > floor { pct } else { floor })
```

回测撮合明确拒绝 mid 定价，走全深度 walk + PIT 费用：

```127:132:crates/quant-pivot-research/src/execution_semantics.rs
/// `cash_outlay` is the exact principal-plus-fee account debit. Dividing it by
/// `filled_shares` therefore yields the all-in executable price consumed by
/// realized-return accounting and economic-tier construction, not a midpoint,
/// top-of-book quote, or fee-blind VWAP.
```

PSR / DSR / MinTRL + PBO(CSCV) 全部实现且进入硬门禁。**这套组合在开源与商业量化栈里都属于上游水平。**

### 6.2 风险与缺口

| 项 | 判定 | 说明 |
|---|------|------|
| **零执行延迟** | **风险** | `fill_at` 必须等于 `decision_at`（`backtest/runner.rs:1914-1921`），无 decision→order 延迟缓冲。相对严格延迟建模偏乐观。业界 Polymarket bot 的端到端 p99 在 600ms–1.5s 量级，这个 gap 在快速变动的市场上非平凡 |
| Portfolio 回测强制 FOK | 风险 | 真实可能部分成交；`PassiveQueue` 已实现但不在主回测路径 |
| 单路径 Sharpe 未年化 | 风险 | `periods_per_year=1`，跨采样频率比较会误导。注释已声明它不是 gate 数据源，但仍会出现在报表里 |
| Classical `train()` 内的 `validation_objective` | 风险 | 在**全训练矩阵**上算 rank_ic（`model/classical.rs`），命名易与 OOS 混淆 |
| Triple-barrier + meta-labeling | **缺失** | AFML Ch.3。当前是固定 horizon + MFE/MAE 辅助标签（已是 barrier 的一半） |
| Average uniqueness / sequential bootstrap 样本权重 | **缺失** | AFML Ch.4。重叠标签的样本被等权对待，有效样本量被高估 |
| Sortino | **缺失** | 只有 Sharpe。对预测市场这种强非对称收益分布，Sortino 更合适 |
| Early stopping / 类别不平衡处理 | **缺失** | GBDT 固定 `n_estimators`、`subsample=1.0`；LogisticRegression 默认 `alpha=0.0`（无惩罚） |
| CPCV meta-overfitting | **方法论风险** | 已有 PBO trial grid 部分缓解，但研究者反复调 recipe/模板本身的行为不在 trial 计数内 |
| 无生产级 rolling-retrain 编排 | 缺口 | CPCV 给分布，walk-forward 给"按部署方式复现"的证据。二者互补，当前只有前者的完整编排 |

其中**样本权重缺失**在预测市场上影响被放大：同一个市场在不同 `as_of` 采样点产生的样本，标签指向同一个结算结果，重叠度接近 1。等权处理会严重高估有效样本量，进而高估 DSR 的显著性。这与 `min_sample_count: 500` 的门槛相互作用——500 个样本如果实际有效样本量只有 50，DSR 的统计基础就不牢。**建议优先实现 average uniqueness 权重，它对 DSR 正确性的影响比 triple-barrier 更直接。**

---

## 7. 极致性能

### 7.1 宣称并做到（有代码证据）

| 不变量 | 证据 |
|--------|------|
| 8 个 token-affine 分区 actor，`TokenKey % 8` 路由 | `data_pipeline.rs:66-70`、`154-167`。`TokenKey` 是 catalog 稠密追加 u32，取模分布均匀，无 hash 偏斜 |
| 有界 mailbox + 共享字节信号量 + 250ms 超时 fail-closed | `data_pipeline.rs:926-959` → `mark_gap` + session invalidate |
| mutable book 单写者，热路径无 Mutex / DashMap | partition actor 独占 `AHashMap` |
| ArcSwap + seqlock 借用读，不增 refcount | `book_store.rs:114-139` |
| `Copy` UUID / ContentHash / TokenKey | `types/data_plane.rs:5-8` |
| SessionHub 单次编码 `ByteString` fanout + 主题倒排索引 | `web/ws/mod.rs:119-125` |
| durable ACK 后才发布 Fresh 快照 | `data_pipeline.rs:1960-1997` |
| jemalloc 强制链接全部目标进程，架构检查器拒绝缺链/第二 allocator | `allocator/src/lib.rs:8-14`、`xtask/architecture.rs:2486+` |

**这些不是文档，是真落地的。** 在这个规模的 Rust 项目里能把这套不变量用架构检查器机械化守住，是很强的工程执行力。

### 7.2 宣称但未签收

`08-extreme-performance-design.md:32-44` 列出了硬 SLO（BookStore 读 p99 10µs、fanout p99 2ms、normalize→enqueue 250µs、durable 250ms、10K events/s sustained），但台账自己承认未取得 Linux artifact：

```567:568:docs/plans/quant-pivot/09-extreme-performance-ledger.md
- [ ] fixed Linux runner 生成 full/soak CI artifact。未取得 artifact 前 PERF-21 保持
  `in_progress`，不得伪称 SLO 已通过。
```

现有数字全部是 macOS Criterion smoke（BookStore 读 ~5ns、fanout ~45–50µs/事件）。**台账的诚实度值得肯定**——它明确写了"不得伪称 SLO 已通过"。但结论是：**性能 SLO 目前无证据，属于未签收状态。**

### 7.3 真实性能风险（按优先级）

1. **入站 JSON 双重物化**。SDK 的 `parse_if_interested` 先 `serde_json::from_slice` 成 `Value`，再 `from_value` 成目标类型，数组路径还有 `elem.clone()`。在 10K–50K events/s 下这极可能是 ingest CPU 的主要成本。**这是当前最值得优化的一处**——比继续抠 BookStore 的 5ns 收益高几个数量级。
2. **`LastTrade` / `Resolved` 路径的 `format!("{:#x}")`**（`normalize.rs:221,241`）：每条消息一次堆分配 + 格式化。
3. **canonical 路径同步等待 ClickHouse durable**：这是正确性换延迟的架构选择（durable-then-publish），但意味着 p99 延迟受 ClickHouse 尾延迟支配。单一 `LedgerPersistenceCoordinator` 聚合 8 个分区，是 durable p99 的瓶颈点。
4. **共享 256MiB ingress semaphore**：所有 shard 共用，突发流量下全局排队。

### 7.4 一个判断

性能架构的投入产出比目前是倒置的：**BookStore 读路径被优化到 5ns 量级，而它上游的 JSON 解析可能是它的几千倍开销**。如果要继续投入性能工作，正确的下一步是给端到端 ingest 做一次 profile（不是 microbench），把预算花在 JSON 与 durable 路径上。

---

## 8. 代码质量与优雅性

### 8.1 硬性规则遵守度（实测）

| 规则 | 实测 | 判定 |
|------|------|------|
| `src/` 禁 `unwrap()` | 生产 17 处，其中 16 处在 metrics 注册宏 | **实质遵守** |
| 禁 `todo!` | **0** | 满分 |
| 禁 `unsafe` | **0**（唯一 `#[allow(unsafe_code)]` 给 `linkme::distributed_slice`） | 满分 |
| 禁 `QuantError::Internal` | **0** | 满分 |
| 禁 `ExecutionMode::DryRun` / `ScoredOpportunity` | **0** | 满分 |
| money 用 Decimal | `execution/` 无 f64 业务货币；266 行 f64 集中在 ML/HiGHS/metrics 边界 | **遵守** |
| `#[deprecated]` | crates 内 **0** | 满分 |
| `dead_code` allow | crates 内 **0** | 满分 |
| 真实技术债注释（TODO/FIXME/HACK） | **≈0** | 罕见 |
| 测试 | 2,216 个（1,837 `#[test]` + 379 `#[tokio::test]`），测试代码占 29.3%，`#[ignore]` 仅 2 个且理由正当 | 强 |

**在 58 万行的规模上做到这个遵守度，是本次审计中最令人印象深刻的部分。** 大量项目在十分之一的规模上就会失守。

### 8.2 真实问题

**上帝文件与超长编排函数**（唯一的结构性质量问题）：

| 行数 | 文件 |
|-----:|------|
| 7650 | `system-tests/src/support/feedback_closure_seed.rs` |
| 4757 | `core/src/service/cpcv_backtest.rs` |
| 4368 | `core/src/service/trade_policy.rs` |
| 4094 | `xtask/src/architecture.rs` |
| 4042 | `core/src/service/training_dataset.rs` |
| 3894 | `core/src/service/durable_feature_parity.rs` |
| 3531 | `research/src/backtest/runner.rs` |
| 2873 | `research/src/portfolio/scenario_model.rs` |

函数长度：≥100 行 459 个，≥150 行 37 个，≥200 行 **0 个**。嵌套深度最深 9 层（`feedback_coordinator.rs`、`feedback_attribution.rs`）。

判断：**没有 200 行怪兽函数说明 AST 审计器起了作用，但 150 行级编排函数成群说明约束被"贴着线"满足了**。真正的问题是 `core/service/*` 把状态机 + 校验 + 事务 + 证据封存写进同一个方法。建议按 DAG stage 拆分，让每个阶段成为独立的、可单测的单元——这也会让 `feedback_closure_seed.rs` 那 7650 行 fixture 自然瘦身。

**其余需要处理的**：

| 项 | 说明 |
|---|------|
| `AGENTS.md` §4 严重过期 | 声称"Phase 0 Runtime：只有 Infra/Data/Governance bundle，无 execution hot path"，与当前 Phase 11.9 的 execution/report/service/projection 全套代码**直接矛盾**。这是给 AI 代理和新人读的第一入口文档，误导成本最高 |
| `docs/08-third-party-crates-and-ml-stack.md` 仍在讨论 `linfa` | `Cargo.lock` 中无此依赖，规划文档未收回 |
| 28 处 `#[allow(clippy::needless_update)]` | 与 rust-style 规则"禁止 allow 压 lint"字面冲突。要么改 ActiveModel 构造方式，要么在 AGENTS 登记为 SeaORM Insert DTO 正式例外 |
| 跨 crate 同名类型 | `TrainModelRequest`（models API 层 vs research 引擎层）、`MicrostructureBucket`。不是重复建模，是命名碰撞，建议 research 侧改名 |
| `third_party/smartcore` fork | 锁定 0.5.5 + 本地 XGBoost typed export 补丁。补丁本身设计克制（只加只读导出，训练/推理字节级保持上游），但 fork 是长期维护债。建议固化 `PATCHES.md` 记录 diff 清单与上游跟踪策略 |

### 8.3 质量门禁运行结果

本次审计实测运行：

```
cargo clippy --workspace --all-targets -- -D warnings
Finished `dev` profile [unoptimized + debuginfo] target(s) in 16m 06s
exit_code: 0
```

**全 workspace、全 target（含 tests / benches / examples）在 `-D warnings` 下零告警通过。**

这个结果的分量需要放在配置上下文里看。`Cargo.toml:32-36` 把 `clippy::all` 设为 `deny`，`pedantic` 与 `nursery` 设为 `warn`，而 `-D warnings` 又把后两者一并升格为错误。也就是说，这次通过意味着 58 万行代码在 **clippy 的 pedantic + nursery 全量规则**下没有任何一条告警——只豁免了四条有明确书面理由的项目级 allow（`module_name_repetitions`、`must_use_candidate`、`missing_errors_doc`、`missing_panics_doc`）和一条架构性豁免（`future_not_send`，因为 actix-web 的请求类型设计上就是 `!Send`）。

配合 §8.1 的静态统计（`unsafe` 0、`todo!` 0、`QuantError::Internal` 0、crates 内 `#[allow]` 仅 31 处且 28 处是同一类 SeaORM Insert DTO 模式），可以确认：**本报告 §8.1 中"规则被机械化执行"的判断有实测门禁背书，不是抽样推断。**

唯一需要留意的是 16 分钟的冷编译耗时——这是 58 万行加全 target 的规模成本，对本地迭代反馈速度是真实摩擦，但不影响门禁本身的有效性。

---

## 9. 风险登记册（第一轮）

按 (影响 × 可能性) / 修复成本 排序。**第二轮追加的 R15–R33 见 [§15](#15-第二轮风险追加)。**

| ID | 风险 | 严重度 | 建议动作 |
|----|------|--------|---------|
| **R1** | 200 天数据 runway 阻塞上线，且该阻塞基于错误的"不可回填"前提 | **阻断** | 实现 `OrderFilled` + `/prices-history` 回填；新增 L2-free bootstrap profile（§1.4） |
| **R2** | 策略池未覆盖已验证占 99.76% 的确定性再平衡套利 | **高** | 新增 `RebalancingScanner` + 多腿执行；优先体育类目（§2.4） |
| **R3** | 训练目标（mid）与门禁目标（net-of-cost）不一致 | **高** | 标签改为 net-of-cost 或可成交价（§3.3） |
| **R4** | Maker/taker rebate 未建模，系统性低估 passive 路径 | **中高** | `PitFeeSchedule` 增加 rebate 项，作为带不确定性的场景项（§3.2） |
| **R5** | 无 AFML 样本权重，重叠标签导致有效样本量高估 → DSR 显著性基础不牢 | **中高** | 实现 average uniqueness 权重 |
| **R6** | 性能 SLO 全部未签收，仅有 macOS smoke | **中** | 优先 profile 端到端 ingest（JSON 路径），而非补 microbench |
| **R7** | 长结算导致 feedback 周期以季度计，无 MtM 快反馈 | **中** | 引入 MtM 中间标签；接入快结算类目 |
| **R8** | `scenario_model` 是组合层隐含单点，其分布假设未独立验证 | **中** | 用历史结算数据做经验覆盖率校验 |
| **R9** | ReportOnly 人工成交成本不可见 | **中** | 新增 operator-attested `ManualExecutionOutcome` 通道 |
| **R10** | 回测零执行延迟假设偏乐观 | **中** | 引入可配置的 decision→fill 延迟，做敏感性分析 |
| **R11** | `AGENTS.md` §4 与代码严重矛盾（AI 代理首要入口） | **中** | 立即修正 |
| **R12** | 上帝文件 + 150 行级编排函数群 | **低中** | 按 DAG stage 拆分 `core/service/*` |
| **R13** | smartcore fork 维护面 | **低** | 固化 `PATCHES.md` + 上游跟踪 |
| **R14** | CPCV meta-overfitting（研究流程本身的过拟合） | **低中** | 把 recipe 模板迭代纳入 trial 计数 |

---

## 10. 建议路线图（第一轮）

> **已被 [§16](#16-修订后的行动优先级) 修订**：第二轮发现的订单可提交性四缺口（R15–R18）成本仅 2–3 天却会阻断成交，必须并入第一阶段。本节保留为第一轮的原始判断。

### 第一阶段 — 解除上线阻塞（2–4 周）

1. **R11** 修正 `AGENTS.md` §4（半天，但影响后续所有代理协作质量）
2. **R1** 链上 `OrderFilled` 回填 worker + `/prices-history` 回填 worker，接入既有 PIT 契约
3. **R1** L2-free bootstrap research profile，PriceBook 族降级为 Optional
4. 用回填数据跑通一次完整 CPCV(8,3) → DSR → PBO → bootstrap，验证门禁在真实数据上可达

**退出条件**：产出第一份带真实模型的 ReportOnly 报告。

### 第二阶段 — 修正经济学（3–6 周）

5. **R3** 标签改为 net-of-cost（先用 `entry_cost_bps` 减法版，验证有效后上可成交价版）
6. **R4** rebate 进入经济模型
7. **R5** average uniqueness 样本权重
8. **R8** scenario model 经验覆盖率校验

**退出条件**：模型训练目标与门禁目标对齐，DSR 显著性有正确的有效样本量基础。

### 第三阶段 — 扩展 alpha 池（6–10 周）

9. **R2** `RebalancingScanner` + 多腿执行 + leg-risk 状态机，优先体育类目
10. **R7** MtM 中间标签，压缩 feedback 周期
11. **R9** operator-attested 执行回填通道

**退出条件**：存在一条不依赖 200 天历史、不依赖统计显著性的确定性收益路径在运行。

### 持续

12. **R6** 端到端 ingest profile → 优化 JSON 路径 → Linux runner SLO 签收
13. **R12** 按 stage 拆分 `core/service/*` 上帝文件
14. **R10 / R14** 延迟敏感性分析、trial 计数完备化

---

## 11. 第二轮：公式层交叉验证

第一轮对 `validation/` 的判断是"忠实于 López de Prado"，但那是基于模块注释与结构的判断。这一轮**逐行核对代数**，并与 Bailey & López de Prado 原始论文（`davidhbailey.com/dhbpapers/deflated-sharpe.pdf`）、Wikipedia、Quantdare 参考实现三方对照。

### 11.1 核对结果：核心公式无错

| 公式 | 标准形式 | 实现 | 判定 |
|------|---------|------|------|
| PSR | `Φ[(SR−SR*)·√(n−1) / √(1−γ₃·SR+((γ₄−1)/4)·SR²)]` | `dsr.rs:130-131,181-187` | **一致** |
| DSR 的 `SR₀` | `√V[SR]·[(1−γ)Φ⁻¹(1−1/N)+γΦ⁻¹(1−1/(Ne))]` | `dsr.rs:98-100` | **一致** |
| Euler–Mascheroni | 0.5772156649 | `dsr.rs:36` | **一致** |
| PBO / CSCV | Algorithm 2.3：`C(S,S/2)` 划分、IS 选最优、OOS midrank、`λ<0` 计数 | `pbo.rs:338-339,788-836,374-379` | **一致** |
| Spearman | Pearson-on-average-ranks（**非** `1−6Σd²/(n(n²−1))`） | `stats.rs:26-33,64-85` | **一致** |
| Isotonic | 先 tie 聚合再加权 PAVA，边界 clip | `isotonic.rs:66-98,133-138` | **一致** |
| Platt | Lin–Lin–Weng 稳定 NLL + 标签平滑 `t₊=(N₊+1)/(N₊+2)`、`t₋=1/(N₋+2)` | `platt.rs:74-82,234-247` | **一致** |
| Φ / Φ⁻¹ | statrs，非手写近似，`p` clamp 到 `[1e-12, 1−1e-12]` | `stats.rs:215-227` | **一致** |

三个容易写错的点我单独核了：

**分母用观测 SR 还是基准 SR₀？** 文献里两种写法都能查到——Wikipedia 的 DSR 词条写 `SR₀`，Quantdare 与原论文写观测 `SR̂`。原论文正文明确"γ₃ 是 skewness、γ₄ 是 kurtosis **of the returns distribution for the selected strategy**"，且分母在统计上是 `SR̂` 估计量的标准误 `σ(SR̂)`，本就应该是观测值的函数。项目取 `self.observed_sharpe`（`dsr.rs:166`），**符合原始定义**。

**`SR₀` 是否漏了均值项？** 论文附带的 Python 参考实现是 `getExpMaxSR(mu, sigma, numTrials) → mu + sigma*maxZ`，带 `mu`。项目只有 `variance.sqrt() * (term_a + term_b)`，没有 `mu`。这是**正确的**——DSR 的原假设是 true SR = 0，因此 `E[{SR}] = 0`，论文正文 Eq(2) 本身也不含 `mu`；Python 那个是更一般的形式。

**γ₄ 是 kurtosis 还是 excess kurtosis？** 项目用非 excess（正态 = 3），与公式中的 `(γ₄−1)/4` 配套（若用 excess 应是 `(γ₄+2)/4`）。`dsr.rs:124-128` 在 `kurtosis()` 对退化序列返回 0 时强制回退 3，是防御而非混淆。**正确。**

### 11.2 需要纠正的一处判断

有效试验数用的是：

```rust
// pbo.rs:719-727
N_eff = 1 + (N - 1) * (1 - ρ̄)
```

初看像是自创公式，展开后 `1 + (N−1)(1−ρ) = ρ + (1−ρ)N`，**正是 Quantdare 给出的 DSR 标准简化式 `N = ρ̂ + (1−ρ̂)M`**。两者代数恒等，不存在"更不保守"的问题。

真实的（较轻）缺口是另一回事：López de Prado 2018 推荐用 **ONC 聚类**或层次聚类/谱方法估计有效 N，平均相关的闭式近似只是最简版本。在试验之间相关结构不均匀（比如少数几组高度相似、其余互相独立）时，平均相关会低估聚集程度。当前 trial grid 是规则的超参笛卡尔积，相关结构较均匀，简化式基本够用——但若未来 recipe 模板大量分叉，应升级为聚类估计。

### 11.3 真实问题（均非代数错误）

| 项 | 位置 | 说明 |
|---|------|------|
| **`stddev` 文档与实现矛盾** | `stats.rs:97-114` | 注释写 "Sample standard deviation"，实现是 population（除 `n`）。**这是本轮最危险的一处**——不是当前有 bug，而是后人照注释"修正"成 `n−1` 后，会与 PSR 分子里已有的 `√(n−1)` 构成双重修正，系统性压低 Sharpe 并扭曲 DSR，且不会有任何测试报警。改注释，或改名 `population_stddev` |
| MinTRL 只实现 `SR*=0` 特例 | `dsr.rs:239-250` | 一般式分母是 `(SR−SR*)²`，实现是 `SR²`。若业务理解为"相对选样偏误基准 `SR₀` 的最短样本"，会**系统性偏短（乐观）** |
| Sharpe 无风险利率恒为 0 | `metrics.rs:187-198` | 无 `rf` 参数。若输入不是超额收益则高估 |
| MDD 用加性累计而非复利净值 | `cpcv.rs:1001-1012`、`metrics.rs:111-128` | 峰谷法本身正确，但对 `Σr` 而非 `∏(1+r)`。注释已声明 non-compounding，属口径而非 bug |
| PBO 在 behavioral equivalence class 代表列上排名 | `pbo.rs` `representative_trials` | 对完全重复试验合理；对"近似但不相同"的试验会改变有效 N 与 ω |

---

## 12. 第二轮：存储层（第一轮完全未覆盖）

### 12.1 PostgreSQL

| 指标 | 实测 |
|------|------|
| 表 | **121** |
| 索引（manifest 全量，含 PK/UNIQUE） | **416**（414 btree + 2 gin） |
| 业务 `IndexSpec` | **237**（74 unique、39 partial） |
| JSONB 列 | **109**（其中 `ExternalJsonDocument` 白名单 4） |
| 外键 | **277**，**全部 `NoAction`**（零级联） |
| 迁移 | **1** 个 bootstrap，artifact 739,590 字节 + blake3 校验 |
| `begin()` 事务点 | ~**178** |
| `RepeatableRead` / `Serializable` | **6** / **3** |
| **statement-count 测试** | **4** |

四条关键查询路径（recommendation / feedback cycle / order intent / attribution）都有覆盖索引，未发现全表扫描风险。JSONB 全部 typed，仓库内对 JSONB 做 path 查询（`->>` / `jsonb_`）**0 处**——这正是规范想要的结果。2 个 GIN 索引都在 `tags`/`categories` 数组上，不在 JSONB 上。

**问题一：statement-count 覆盖不足。** `docs/persistence/seaorm-and-typed-persistence.md:171` 要求关键 repository 必须有 statement-count 测试，实测只有 4 个，且全在 `config_resources` / `config_activity` / `runtime_config` / `model_registry`——**recommendation、feedback、order_intent、attribution 这四个真正的热点仓一个都没有**。当前靠 `BindLimitedBatch` 的设计规范防 N+1（`write.rs:36-39`、`query.rs:128-132` 都是正确的分块批量），静态扫描也没发现"对结果集逐行 `find_by_id().await`"的真 N+1，但**没有回归护栏**，一次重构就可能引入。

**问题二：连接池 `max_connections = 10`。** `config/db.rs:179-204`。同时在跑的 durable worker 有 report coordinator、feedback scheduler、settlement、outcome reconciliation、trade tape、research job、outbox 等至少七类，各自还要开事务。10 个连接会先表现为 `acquire_timeout`（默认 10s）排队而非报错，症状是"系统变慢但没有错误日志"。建议按 worker 数量重新标定。

**问题三：CAS 形态不统一。** 两种并存——feedback 走"`FOR UPDATE` 行锁 + generation 校验"（`feedback_cycle.rs:900-920`），promotion permit 走"`WHERE revision = ?` + `rows_affected` 断言"（`promotion_permit.rs:302-333`）。后者是标准乐观锁，抗 ABA 更强；前者依赖锁语义正确。两种都能用，但评审时需要分别推理，容易出错。

### 12.2 ClickHouse

| 指标 | 实测 |
|------|------|
| 表 | **24**（19 `MergeTree` + 5 `ReplacingMergeTree`） |
| **DDL TTL** | **0 个表有 TTL** |
| CODEC | 仅 **3** 处 `ZSTD(3)`；`Delta` / `DoubleDelta` / `Gorilla` **0** |
| `PREWHERE` | 全仓库 **0** 处 |
| 批写配置 | flush 5s / batch 5000 / 并发 8 |

**问题一（本轮最高优先级的运维风险）：200 天 retention 没有任何自动清理机制。**

`minimum_raw_retention_days()` = 200 是**应用层的 readiness 下限**，不是 DDL TTL。更关键的是，readiness 证据要求表**不能有** TTL——有 TTL 表达式就判 `proven() = false`：

```536:539:crates/quant-pivot-models/src/types/research_readiness.rs
        evidence.observations[0].table_ttl_expression =
            Some("persisted_time + toIntervalDay(200) DELETE".to_owned());
        assert!(!evidence.proven());
```

而 CH migration 把 `MODIFY TTL` 标为 offline-unsafe（`clickhouse/migration.rs:1117+`）。三者合起来的效果是：**数据只进不出，且架构上主动禁止了加 TTL。**

`quant_book_l2_ledger` 的容量按配置注释的峰值（~3K rows/s）估算：

| 吞吐 | 日行数 | 200 天行数 | 压缩后体积 |
|------|--------|-----------|-----------|
| 1K/s | ~8.6×10⁷ | ~1.7×10¹⁰ | ~3.5–8.6 TB |
| 3K/s | ~2.6×10⁸ | ~5.2×10¹⁰ | ~10–26 TB |

行内含 `Array(Decimal(18,8))` / `Array(Decimal(38,18))` 的档位数组，宽行下上界还会更高。这个量级需要在部署前做明确的容量规划与 partition drop 运维方案，否则 200 天 runway 走到一半会先撞磁盘。

**问题二：PIT 查询与主键不对齐。** L2 表 `ORDER BY (token_id, stream_session_id, token_sequence)`，而 PIT 快照查询是按时间倒序：

```724:729:crates/quant-pivot-repository/src/clickhouse/fact_read.rs
                "SELECT ?fields FROM quant_book_l2_ledger \
                 WHERE token_id = ? AND event_type = 'Snapshot' \
                 AND venue_event_time <= ... \
                 ORDER BY venue_event_time DESC, persisted_time DESC, token_sequence DESC \
                 LIMIT 1",
```

`token_id` 是主键前缀所以不会全表扫，但在该 token 内要跨所有 session 找时间最大的 Snapshot，需要读大量 granule。加上 `PREWHERE` 零使用、时间列无 `Delta`/`DoubleDelta` codec，训练与回放阶段的 IO 成本会比必要的高。这些是纯 DDL/查询改动，成本低收益直接。

**问题三：5 张 `ReplacingMergeTree`。** 去重只在 merge 时发生，读取若不加 `FINAL` 或不做去重投影，可能读到重复版本。需要逐表核对读者是否处理。L2 主表用的是普通 `MergeTree` + `non_replicated_deduplication_window` + 应用层 `insert_deduplication_token`，这条路径是对的。

### 12.3 Redis

只缓存三类市场元数据（`MarketInfo` / `EventInfo` TTL 5 分钟，`MarketMetadata` TTL 30 分钟），L1 Moka + L2 Redis，读失败 fail-open 当 miss。**不缓存 book、订单、账户等权威态**——这个边界划得很好。

缺口是 `get_or_load` **没有 singleflight / stampede 保护**（`manager.rs`）。TTL 集中过期时并发 loader 会同时打 Gamma API，而 Gamma 侧还有 Cloudflare 限流。加一个 per-key 的 in-flight 去重即可。

### 12.4 跨存储一致性

Outbox 表 4 个（policy activation、domain event、entry condition evaluation、feedback event），claim 用 `rows_affected != expected → conflict` 的标准 CAS。PG 内 audit + outbox 同事务，PG→CH 走 outbox worker 异步投递，Redis 非权威。**没有分布式事务，最终一致，这是正确的选择。**

备份恢复是真空白：代码侧有 preproduction reset、schema checksum、MinIO Object Lock（WORM artifact），但**没有 pg_dump / CH backup 的任何 job 或文档**。`architecture-and-design.md:829` 自己也承认 WORM restore 与 retention/capacity 待真实环境验证。

---

## 13. 第二轮：并发正确性与容错（第一轮只看了性能架构）

### 13.1 seqlock 实现正确

第一轮说"ArcSwap + seqlock"，但真正的 seqlock 不在 `book_store.rs`，在 `TokenSlot`（`data_plane_index.rs:151-369`）。逐行核对：

```174:197:crates/quant-pivot-core/src/ingest/data_plane_index.rs
    fn begin_freshness_write(&self) -> u64 {
        // load Acquire; spin while odd; CAS even->odd (success Acquire, fail Relaxed)
    }
    fn end_freshness_write(&self, even_version: u64) {
        self.freshness_version.store(even_version.wrapping_add(2), Ordering::Release);
    }
```

```338:361:crates/quant-pivot-core/src/ingest/data_plane_index.rs
            let before = self.freshness_version.load(Ordering::Acquire);
            if before & 1 == 1 { spin_loop(); continue; }
            let published = self.published.load();
            let freshness = TokenFreshness { /* Relaxed loads */ };
            fence(Ordering::Acquire);
            let after = self.freshness_version.load(Ordering::Relaxed);
            if before == after { return (...); }
```

奇偶版本号、写侧 CAS-Acquire / store-Release、读侧 Acquire fence 后重校验——这是教科书 seqlock，与 Linux `read_seqcount_retry` 同构。中间 payload 用 `Relaxed` 是正确的（由 seqlock 的 Acquire/Release 对包裹）。撕裂读被 `before != after` 重试挡住。`BookStore::read_fresh` 之上还有 `coherent_version_is` + session fence 二次校验。**实现正确。**

原子操作全局统计：**104 处 / 21 文件**，内存序分布 `Relaxed` 84、`Acquire` 24、`Release` 8、`AcqRel` 7、**`SeqCst` 0**。SeqCst 归零是好事（说明没有人用它当"反正最强"的偷懒选项），逐个核对未发现"发布-订阅语义却全程 Relaxed"的错误。

**跨 `await` 持有 `parking_lot` / `std::sync` 锁：生产路径 0 处。** 抽查的几个可疑点（`breaker.rs:322-335`、`health_alert_state.rs:65-73`、`data_pipeline.rs:1050-1055`）都用块作用域或显式 `drop` 在 await 前释放。

### 13.2 loom 覆盖是摆设

workspace 声明了 `loom = "0.7.2"`，唯一消费者是 `quant-pivot-core`，**总共 2 个 loom 测试**：

- `book_store.rs:694-726`：用三个裸 `AtomicBool`/`AtomicU8` 模拟 publish-vs-poison 竞态，**没有实例化 `TokenSlot`、`ArcSwap` 或真实 seqlock**
- `ledger_persistence.rs:563-585`：用 `Mutex`+`Condvar` 模拟 cursor 通知不丢失，**没有实例化真实的 `PartitionLedgerClient` 或 watch cursor**

也就是说，**整个系统最精巧、最难人工验证的那段并发代码（`TokenSlot` seqlock）恰好没有被 loom 覆盖**，覆盖的是两个手写的玩具模型。这不是"依赖没用"，是"用错了地方"。seqlock 的正确性目前靠代码审查（我审下来是对的）和集成测试，没有模型检查背书。把 `TokenSlot::snapshot_with_freshness` 与 `publish_*` 放进 loom model 是高价值的低成本工作。

### 13.3 数据完整性：fail-closed 做得扎实

**序列 gap**：`token_sequence` 是 shard 本地单调计数（`shard.rs:567-578`），不是 venue 原生序列号。`accept_token_sequence`（`data_pipeline.rs:179-214`）要求同 session 严格 `last+1`，违反则整个 session 进 `failed_sessions` → poison + invalidate + `mark_gap`（`data_pipeline.rs:1930-1980`）。

**delta 无 base snapshot**：直接拒绝（`data_pipeline.rs:1924-1928`），slot 侧还要求 `Fresh` 且同 session（`data_plane_index.rs:267-269`）。

两者的共同效果是：**book 永远不会以 Fresh 状态残留错误数据**，恢复前读取直接失败。这是正确的取舍。

**WS 重连**：无限重试、初始 1s、上限 30s、×2、jitter 0.20（`retry.rs:137-145`）；重连后开新 session 并把 token 置 `Unseen`，等完整 snapshot 才 Fresh。心跳 10s 一次 PING，上轮未收到 PONG 即判死（`shard.rs:85-89,282-287`）。

一个隐患：`next_delay()` 返回 `None` 时 shard 直接 `break` 停转（`shard.rs:437-439`）。当前默认无限重试不会触发，但如果将来有人配了 `max_attempts`，shard 会**静默停止**而不是告警。

### 13.4 崩溃恢复的边界

| 状态 | 持久化 | 重启行为 |
|------|--------|---------|
| Live `BookStore` / `MutableBookState` | **否** | 空 → WS 重连 → 等 snapshot 重建 |
| CH `quant_book_l2_ledger` | 是 | 供研究/回放，**不 hydrate 热 book** |
| Trade tape block cursor | Postgres | 从 cursor 续扫 |
| Settlement / outcome / report / research job | Postgres lease/queue | 租约回收后重试 |

也就是说，**durable ledger 只服务研究，不服务恢复**。进程重启后有一段"book 为空、读取 fail-closed"的窗口，长度取决于 WS 重连加 snapshot 到达。这是清晰的设计取舍（避免从 CH 回放引入状态不一致），但应该在 runbook 里写明这个窗口的预期长度。

### 13.5 时钟处理的不对称

Binance 适配器有 `max_clock_skew_ms = 2000` 的漂移检测，**Polymarket 市场 WS 没有任何 venue↔local 时钟校验**，book `timestamp_ms` 直接采信，也不拒绝倒退的时间戳。乱序完全靠 `token_sequence` 单调性兜底。

因为 sequence 是本地生成的，它能保证"我处理的顺序是连续的"，但不能发现"venue 给我的时间戳乱了"。PIT 正确性依赖 `venue_event_time`，而这个字段目前没有任何合理性校验。建议至少加一个宽松的 skew 上界与倒退计数 metric。

### 13.6 重试与熔断

| Worker | 退避 | Dead letter |
|--------|------|-------------|
| Feedback / research jobs | **指数**（`initial << attempt` capped，max 3 次） | `retry_exhausted` 状态 |
| Outcome reconciliation | **指数**（`30 × 2^min(attempt−1,8)` 秒） | 租约状态机 |
| Report coordinator | lease/heartbeat，非指数 | `fail_claimed_run` |
| Settlement | **固定 poll**，无退避 | 无 |
| Trade tape | 固定 poll + 5% jitter | cursor 不前进 |

没有统一的 dead-letter 总线，各 worker 用自己的终态表达。执行域有真正的 circuit breaker（`execution/breaker.rs`：连续失败/窗口错误率 → `VenueHealth::Degraded` → 硬阈值 trip kill-switch，日亏损 80% degrade / 100% halt，cooldown 自愈），**ingest 域没有 breaker，用 fail-closed + reconnect 替代**——对市场数据这是合理的。

优雅关闭有 10 阶段 drain（`task_registry.rs:37-48`，WsIngress → … → DbClose），每阶段有预算，超时则 abort 并标记 `STATE-AT-RISK` + metric。这是我见过做得比较完整的 shutdown 实现。

### 13.7 可观测性

`MetricsHub` 有 **95 个 metric 对象**（36 `IntCounterVec`、23 `IntGauge`、18 `IntCounter`、8 `HistogramVec`、6 `IntGaugeVec`、2 `Histogram`），延迟、错误率、队列深度、背压四个维度都有覆盖（`ingest_pipeline_lag_seconds`、`ws_hub_queue_oldest_age_seconds`、`book_apply_backpressure_invalidations`、`async_writer_dropped` 等）。覆盖面够。

两个缺口：告警是**代码驱动**（`AlertDispatcher` 在调用点发 + title cooldown），没有独立的规则表配置，运维想调阈值得改代码；tracing 有 HTTP `request_id` 但**没有 OpenTelemetry exporter，`trace_id` 字段是空占位**，跨组件追踪一次报告生成的完整链路目前做不到。

---

## 14. 第二轮：Polymarket 领域陷阱（会直接导致拒单/亏钱）

这一节是本轮价值最高的部分。以下每条都对照了 Polymarket 官方文档（`docs.polymarket.com`）的现行规则与错误码。

### 14.1 三个会导致订单被直接拒绝的缺口

官方 `POST /order` 的错误码里有三条与本系统直接相关：

```
order {id} is invalid. Price ({price}) breaks minimum tick size rule: {tick}
order {id} is invalid. Size ({size}) lower than the minimum: {min}
not enough balance / allowance
```

**缺口一：入场价不做 tick 对齐。**

系统有 `tick_aligned_price`（`report/composer.rs:693-697`），但只用于**退出**价。入场的 aggressive limit 是：

```192:196:crates/quant-pivot-research/src/execution_semantics.rs
pub fn aggressive_buy_limit(best_ask: Price, max_slippage_bps: Bps) -> Price {
    Price::new(
        (best_ask.inner() * (Decimal::ONE + max_slippage_bps.to_fraction())).min(Decimal::ONE),
    )
}
```

`best_ask × (1 + slippage)` 几乎必然落在 tick 网格之外。admission 会拦住它（`checks.rs:274-297` 检查 `(price/tick).fract().is_zero()`），所以不会真的发出坏单——但代价是**报告层产出的推荐价在执行层被系统性拒绝**，表现为"推荐很多、能下单的很少"。而且这个价格还会 `.min(Decimal::ONE)` 到 1.0，而 Polymarket 要求价格严格在 `[tick, 1−tick]` 内。

**缺口二：最小订单量完全没有进入生产逻辑。**

`min_order_size` / `minimum_order_size` 在 catalog 和 CLOB info 里都持久化了，也校验了 `> 0`。但在 `quant-pivot-core/src/execution/**` 和 `quant-pivot-research/src/portfolio/**` 里搜索这两个字段——**所有命中都在测试 fixture 里**（`dec!(5)`、`Decimal::ONE`），生产路径一次都没读过它。

这意味着 MILP 求出的经济 tier 可以是任意小的份额，economic tier 只校验现金预算与价格正性（`economic.rs:346-347`），不知道 venue 有 5 shares 或 $1 的下限。官方文档的 `/book` 响应示例里 `min_order_size: "5"`，这是真实存在的约束。后果是小额 tier 直接被拒，且**这次连 admission 都拦不住**（admission 检查了 tick，没检查 size）。

**缺口三：下单前不检查 allowance。**

`verify_buy_balance` 只查余额：

```761:784:crates/quant-pivot-api/src/clob/mod.rs
    async fn verify_buy_balance(&self, req: &OrderRequest) -> Result<(), OrderSubmissionError> {
        let available = self.collateral_balance().await...;
        if available < required {
            return Err(... "insufficient_pusd_balance" ...);
        }
```

`balance-allowance` 端点返回的 allowance 字段没有被用于门禁。结算侧有完整的 ERC-1155 `setApprovalForAll` / `isApprovedForAll` 处理（`settlement/adapter.rs:56-64`），交易侧没有对等检查。后果是订单签名并提交后才在 venue 侧失败，浪费一次往返并污染错误率统计。

### 14.2 我在核验官方文档时发现的第四个缺口

Polymarket 的下单精度规则不只是 tick size，还有 **size decimals 与 amount decimals** 的三步舍入：

| Tick size | Price decimals | Size decimals | Amount decimals |
|-----------|---------------:|--------------:|----------------:|
| 0.1 | 1 | 2 | 3 |
| 0.01 | 2 | 2 | 4 |
| 0.005 | 3 | 2 | 5 |
| 0.001 | 3 | 2 | 5 |
| 0.0001 | 4 | 2 | 6 |

官方要求的顺序是：价格按 Price decimals 表达 → 份额**向下**舍入到 Size decimals → 计算 USD 金额，超出 Amount decimals 时**先向上舍入到 Amount decimals+4，再向下舍入到 Amount decimals**。

在仓库里搜索 `size_decimals` / `amount_decimals` 及等价的舍入逻辑：**0 命中**。系统的 `Shares` / `Usd` 用的是 `rust_decimal` 的通用精度，没有按 tick size 派生的分级舍入。这比缺口一更隐蔽——即使价格对齐了 tick，份额或金额的小数位超标一样会被拒，而且错误信息不会明确指向精度问题。

### 14.3 其余领域项判定

| # | 主题 | 判定 | 关键证据/说明 |
|---|------|------|-------------|
| 4 | NegRisk 路由 | **正确** | `intent_service.rs:240-244` 按 `neg_risk` 分叉 `StandardV2`/`NegRiskV2`；admission 比对 registry↔venue（`checks.rs:281-285`） |
| 5 | 市场状态 | **部分** | 选品阶段强（`filters.rs:141-150` 只留 Active）；**提交时只拦 `ManuallyBlocked`，不复核 `accepting_orders`**（`admission/builder.rs:318-320`）→ 报告后市场暂停存在 TOCTOU |
| 6 | YES/NO 对偶最优执行 | **缺失** | 有 convert-edge 特征（`structural.rs:422-431`），无"卖 YES 差就改买 NO"的执行路由，也无 merge/convert positions |
| 7 | UMA 争议 | **部分** | 只认 `uma_resolution_status == resolved`（`gamma/catalog.rs:278-295`）且只吃链上 finalized `ConditionResolution`。**无 dispute/challenge period 状态机**——策略是"等链上终局"，把争议风险推迟而非建模 |
| 8 | 重结算 | **部分** | 内容不同则 `state_conflict`（`recommendation_resolution_outcome.rs:500-505`），链上重复 → `DuplicateResolution`。防错写正确，但**无修订流程**，真发生需人工介入 |
| 9 | 事实已定但链上未结 | **缺失** | 无软结算/提前平仓模型，一律等链上 |
| 10 | 非 0/1 payout | **正确** | `PayoutRatio` 支持 `[0,1]` 含 0.5，DB CHECK 区分 winner-take-all vs split |
| 11 | pUSD vs USDC.e | **正确** | pUSD `0xc011…2dfb` 为抵押品，USDC.e `0x2791…4174` 作 wrap 依赖，地址钉死 + code-hash 校验 |
| 13 | Gas / POL | **部分** | 结算走 relayer（gasPrice 常为 0），事后记 `gas_fee_pol`；**无 EOA/funder 原生 gas 余额预检** |
| 14 | Proxy wallet / funder | **正确** | `WalletTopology` 做 EOA signer==funder 校验、Proxy/Safe CREATE2 推导、on-chain ownership fallback，CLOB auth 绑定 `signature_type`+`funder` |
| 15 | 三方余额对账 | **部分** | 报告 sizing 用 CLOB collateral + Data API positions；成交对账用 CLOB order/trade + balance。**无持续的"CLOB ↔ 链上 pUSD ↔ Data API"三方一致性 reconciler** |
| 17 | 地理封锁 | **缺失** | 代码与配置无 geoblock / 出口代理 / 路由适配。Polymarket CLOB 对部分地区封锁，部署层需自解决 |
| 18 | V2 合约与 SDK | **正确** | SDK 钉 `=0.7.0`，连接强制 `GET /version == 2`（`clob/mod.rs:614-627`），Exchange 地址 `0xE111…996B` / `0xe222…0F59` 正确 |

### 14.4 限流：方向反了，且有一个时效性问题

项目的限流是硬编码的（`clob/rate_limiter.rs:25-68`），对照官方现行数值：

| 端点 | 项目配置 | 官方 Cloudflare 限额 | 判定 |
|------|---------|-------------------|------|
| `POST /order` | 10/s | 5,000 req/10s burst，120,000/10min sustained | **过度保守 50×** |
| `GET /book` | 30/s | 1,500 req/10s | **过度保守 5×** |
| `GET /balance-allowance` | 5/s | 200 req/10s | **过度保守 4×** |
| `GET /data/trades` 等 | **未注册 → 不限流** | General 9,000 req/10s | **无保护** |

方向和第一轮的猜测相反：不是限得太松，是**限得太紧**，把自己的吞吐压在官方额度的几十分之一。真正的风险在未注册端点——`acquire()` 对未知 endpoint 立即放行（`rate_limiter.rs:74-78`），批量拉 trades 时可能撞 Cloudflare 的全局额度，进而拖累其他调用。

**时效性问题（需要立即关注）：** Polymarket 在现有 Cloudflare IP 限流之外，新增了**按 signer 地址的 token bucket 限流**，Standard 层是 40 orders/s（burst 60），按 30 日成交量分层最高到 Elite 600/s。官方公告的时间线是 **2026-07-24 起进入 warning mode，两周后开始强制执行**——按今天（2026-08-13）算，强制执行窗口已经到了或刚过。

warning 期间被标记的请求会带 `Poly-RateLimit-Warning: true` 响应头。在仓库里搜索这个 header：**0 命中**。也就是说系统既不监控这个预警信号，也不知道自己是否会在强制执行后被拒。

好的一面是 `Retry-After` 已经正确处理了——`retry.rs:222` 的 `on_failure_with_minimum(err.retry_after())` 会把上游给的重试间隔作为退避下界。这个基础设施已经在了，只需要把 429 与新 header 接进去。

---

## 15. 第二轮风险追加

| ID | 风险 | 严重度 | 建议动作 |
|----|------|--------|---------|
| **R15** | 最小订单量未进入 sizing/admission，MILP 可产出低于 venue 下限的 tier | **高** | economic tier 与 admission 双侧加 `min_order_size` 闸；MILP 增加下界约束 |
| **R16** | 入场价不做 tick 对齐，推荐价被 admission 系统性拒绝 | **高** | `aggressive_buy_limit` 后接 `tick_aligned_price`，并把上界改为 `1 − tick` |
| **R17** | size/amount decimals 三步舍入规则完全未实现 | **高** | 按 tick size 派生精度表，在下单前统一舍入 |
| **R18** | 下单前不校验 allowance | **中高** | 消费 `balance-allowance` 的 allowance 字段，缺失则 fail-closed 并提示授权 |
| **R19** | ClickHouse 无 TTL 且架构禁止加 TTL，L2 表 200 天可达 10–26 TB | **高** | 容量规划 + partition drop 运维方案；或让 readiness 允许"已证明 200 天后"再启用 TTL |
| **R20** | per-signer 限流已进入/接近强制执行，系统不监控 `Poly-RateLimit-Warning` | **中高**（时效） | 立即接入该 header 与 429 计数告警；同时放宽过度保守的本地配额 |
| **R21** | 关键仓（recommendation/feedback/order_intent/attribution）无 statement-count 回归 | **中** | 按规范补齐 4 个热点仓的语句数测试 |
| **R22** | loom 未覆盖真实 `TokenSlot` seqlock | **中** | 把 `snapshot_with_freshness` + `publish_*` 放进 loom model |
| **R23** | `stats::stddev` 文档写 Sample、实现是 population | **中**（陷阱） | 改注释或改名 `population_stddev`，加断言测试锁定除数 |
| **R24** | PIT 查询与 CH 主键不对齐，`PREWHERE` 零使用，时间列无 Delta codec | **中** | 加时间维投影或调整 ORDER BY；热查询加 PREWHERE；时间列上 `DoubleDelta` |
| **R25** | 提交时不复核 `accepting_orders`（TOCTOU） | **中** | admission 增加市场可交易状态断言 |
| **R26** | Polymarket WS 无时钟漂移/倒退检测（Binance 有） | **中** | 加 skew 上界与倒退计数 metric |
| **R27** | 连接池 `max_connections=10` 对 7+ 类 durable worker 偏紧 | **中** | 按并发 worker 数重新标定 |
| **R28** | UMA 争议期与"事实已定未上链"未建模 | **中** | 至少在推荐层标注争议风险敞口 |
| **R29** | Redis `get_or_load` 无 singleflight | **低中** | 加 per-key in-flight 去重 |
| **R30** | MinTRL 只实现 `SR*=0` 特例 | **低中** | 支持一般 `(SR−SR*)`，或在字段名/文档标明口径 |
| **R31** | 无备份恢复方案 | **中** | pg_dump / CH backup job + 恢复演练 |
| **R32** | 无分布式 trace id，告警规则硬编码在代码里 | **低中** | 接 OTel exporter；告警阈值外置为配置 |
| **R33** | 地理封锁无应用层适配 | **低**（部署可解） | 部署文档明确出口要求 |

---

## 16. 修订后的行动优先级

第二轮之后，**第一阶段的内容需要调整**——R15/R16/R17/R18 是"报告产出后无法下单"的硬缺口，成本极低（都是局部改动），但不修的话即使解决了冷启动也走不到成交。它们应该并入第一阶段。

### 第一阶段修订版 — 解除上线阻塞（2–4 周）

1. **R11** 修正 `AGENTS.md` §4（半天）
2. **R16 + R17 + R15 + R18** 订单可提交性四件套：tick 对齐、精度舍入、最小订单量、allowance 预检。**这组加起来大约 2–3 天，是整份报告里投入产出比最高的一项**
3. **R20** 接入 `Poly-RateLimit-Warning` 与 429 告警（时效性）
4. **R1** 链上 `OrderFilled` + `/prices-history` 回填 worker
5. **R1** L2-free bootstrap research profile
6. **R19** ClickHouse 容量规划与 partition drop 方案（在开始 200 天积累**之前**定好）

### 其余阶段

第二、三阶段维持原计划（净成本标签、rebate、样本权重、scenario 校验、再平衡套利、MtM 标签）。持续项追加 R21–R33，其中 R22（loom 覆盖 seqlock）、R23（stddev 文档）、R24（CH 查询优化）优先级高于原有的上帝文件拆分。

---

## 17. 第二轮总结

第一轮的判断在这一轮基本被证实，且有两处修正：

**被证实的：** 统计公式层没有代数错误——PSR、DSR、PBO、Spearman、PAVA、Platt 全部经得起与原始论文逐行对照。seqlock 实现正确。数据完整性的 fail-closed 设计扎实。工程纪律真实。

**被修正的两处：**
- 第一轮子审计怀疑 DSR 有效试验数公式"更不保守"，展开后发现它恒等于 Quantdare 给出的标准简化式，**判断有误，实现正确**。
- 第一轮猜测限流可能"不匹配官方"，核验后发现方向相反——本地配额比官方额度紧几十倍，真正的问题是未注册端点无保护，以及新的 per-signer 限流没被监控。

**第二轮的新增价值集中在三处：**

1. **执行层的四个"必然拒单"缺口**（tick 对齐、精度舍入、最小订单量、allowance）。这些在第一轮的架构视角下完全不可见，因为它们不违反任何架构不变量、不触发任何 lint、有 fail-closed 兜底所以也不会造成事故——它们只是让系统"产出很多推荐，但一单也下不出去"。修复成本 2–3 天。

2. **ClickHouse 无 TTL 且架构禁止加 TTL**。这是一个会在 200 天 runway 中途爆发的运维炸弹，而且必须在开始积累数据**之前**规划，事后补救成本高得多。

3. **loom 用错了地方**。系统最难验证的那段并发代码没有被模型检查覆盖，覆盖的是两个玩具模型。审查下来实现是对的，但这个覆盖缺口应该补上。

---

## 18. 第三轮总览：控制面

前两轮分别看了"系统在做什么"（战略）和"系统怎么做的"（执行与实现）。第三轮看的是**谁能改变系统的行为，以及这些改变受什么约束**——安全边界、风控参数的可变性、主产物本身的假设、以及治理规则执行者自身的可靠性。

这一轮覆盖四块前两轮完全没碰的区域：

| 章节 | 内容 | 一句话结论 |
|------|------|-----------|
| [§19](#19-第三轮风控可被单人合法掏空本次审计最严重的发现) | 风控参数校验与职责分离 | **单人可合法关闭风控，审计日志显示一切正常** |
| [§20](#20-第三轮私钥与认证安全) | 私钥、JWT、RBAC、Web 攻击面、依赖漏洞 | `SecretText` 设计优秀，但私钥无 KMS；实跑 `cargo audit` 出 12 个漏洞 |
| [§21](#21-第三轮报告管线与组合优化的深层问题) | 报告内容契约、场景模型、MILP | MILP 是全库工程正确性最强的一块；场景层的概率权重是影子超参且从未被校验 |
| [§22](#22-第三轮治理执行器自身可绕过) | 架构检查器与测试有效性 | 主体可靠但有四条逃逸路径；测试负例占 41%，但无 mutation/fuzz/覆盖率 |

这一轮的发现有一个共同特征，与前两轮不同：**它们的失效是静默的**。订单被拒会有错误码，数据不够会 fail closed，但风控参数被改宽、场景分布偏了、检查器被绕过——这些都不会产生任何异常信号。

---

## 19. 第三轮：风控可被单人合法掏空（本次审计最严重的发现）

前两轮把风控当成"已实现"接受了，因为治理链路（draft → validate → preflight → approve → activate → CAS）看起来很完整。这一轮进去看**校验规则本身**，发现链路是完整的，但**链路检查的内容有洞**。

### 19.1 三个校验缺口（逐条独立核实）

**缺口一：入场滑点上限完全没有校验。**

`crates/quant-pivot-models/src/runtime_config/validation.rs` 里 `max_slippage_bps` 只出现三次，全部指向 emergency exit：

```
1308:        .max_slippage_bps
1312:            field: "execution.kill_switch.emergency_exit.max_slippage_bps",
2105:            .max_slippage_bps = 0;   // 测试
```

`execution.entry_order_policy.max_slippage_bps`（默认 50 bps）**一次都没被校验**。既没有上界也没有下界，可以设成 0（等于禁止任何滑点，所有单被拒），也可以设成 100000（1000%，等于取消滑点保护）。

**缺口二：模型晋升的最小样本量没有校验。**

在 `validation.rs` 里搜索 `min_sample_count`：**零命中**。`quality_gate.min_sample_count`（Buy 默认 500、Sell 默认 200）可以被改成 0 或 1 并通过校验。配合下面这条，模型质量门禁可以被整体掏空。

**缺口三：最大回撤门禁允许设为 100%。**

```935:crates/quant-pivot-models/src/runtime_config/validation.rs
    unit_ratio("quality_gate.max_drawdown", &gate.max_drawdown, report);
```

而 `unit_ratio` 用的是闭区间：

```1645:1652:crates/quant-pivot-models/src/runtime_config/validation.rs
fn unit_ratio(field: &'static str, value: &DecimalValue, report: &mut ConfigValidationReport) {
    if !(Decimal::ZERO..=Decimal::ONE).contains(&value.value) {
```

`max_drawdown = 1.0` 是合法值，语义是"允许 100% 回撤"，等于关闭这个硬门禁。旁边的 `half_open_unit`（`(0, 1]`）说明代码里是有区分开闭区间的意识的，这里用闭区间放行 1.0 更像是没想到这个语义后果。

**附带的两条：** portfolio 的 USD 类上限（`total_budget_usd`、`max_single_recommendation_usd` 等）只校验 `positive_decimal`，没有绝对天花板——`1e12` 能通过；execution breaker 默认 `venue_error_rate_bps_to_halt = 5000`，意思是**场馆一半请求失败才熔断**（`wire.rs:465`）。

### 19.2 没有职责分离（SoD）

审批与激活是两个独立端点，各自记录了操作者：

```463:466:crates/quant-pivot-web/src/routes/runtime_config.rs
            decided_by_user_id: Some(actor.user_id().map_err(|error| {
                WebError::Internal(format!("authenticated subject is invalid: {error}"))
            })?),
            decided_by_label: actor.claims.username.clone(),
```

```717:719:crates/quant-pivot-web/src/routes/runtime_config.rs
                activated_by_kind: PolicyActorKind::Operator,
                activated_by_user_id: Some(actor_user_id),
                activated_by_label: actor.claims.username.clone(),
```

**两者从不比较。** `activate_draft` 校验了 approval 有效、preflight token 未过期、bundle generation CAS、idempotency key——唯独没有校验"激活人 ≠ 审批人"。RBAC 侧 `DecisionPolicySnapshot` 资源同时允许 `Approve` 和 `Activate` 两个操作（`rbac.rs:285-292`），一个账号可以两步都做完。

### 19.3 完整攻击路径

把上面几条串起来，一个持有 `DecisionPolicySnapshot` 权限的操作员（不需要 `super_admin`）可以在**不触发任何校验错误、不留下任何异常审计记录**的情况下完成：

1. 创建 draft，设 `entry_order_policy.max_slippage_bps = 100000`（无校验）
2. 同一份 draft 里设 `quality_gate.min_sample_count = 0`（无校验）、`max_drawdown = 1.0`（合法）
3. 提高 `total_budget_usd` / `max_single_recommendation_usd`（只需为正）
4. 调用 validate —— **全部通过**
5. 自己 approve
6. 自己 activate —— CAS 成功，`ArcSwap` 原子换上新策略
7. 下一个 `OrderIntentCreation` 边界即生效（`apply_boundary`，`config/mod.rs:455-462`）

结果是：滑点保护取消、模型晋升门禁取消、回撤门禁取消、资金上限任意，而**审计日志会显示这是一次完全合规的策略变更**。

`ModelRouting` 是唯一的例外——它必须走 promotion permit 流程，不能通过这条路径改。这说明团队对"什么需要额外保护"是有判断的，只是这个判断没有覆盖 `ExecutionRiskPolicy`。

值得注意的是，`descriptor.rs:251-274` 已经把 portfolio budget、exposure limits、tail risk、breaker 全部标记为 `RuntimeFieldRiskLevel::Critical`。**这个元数据存在，但只驱动 UI 展示，没有驱动任何强制流程。**

### 19.4 修复建议（按成本排序）

| 动作 | 成本 | 效果 |
|------|------|------|
| `entry_order_policy.max_slippage_bps` 加 `(0, 1000]` bps 范围校验 | 10 行 | 堵住滑点洞 |
| `min_sample_count` 加下限（Buy ≥ 100、Sell ≥ 50 之类） | 10 行 | 堵住样本量洞 |
| `max_drawdown` 改用 `half_open_unit` 并加上界（如 ≤ 0.5） | 1 行 | 堵住回撤洞 |
| portfolio USD 上限加绝对天花板（从 deploy config 读，运行时不可超） | 30 行 | 资金上限双层防护 |
| `activate_draft` 增加 `decided_by_user_id != activated_by_user_id` 校验，对 `RuntimeFieldRiskLevel::Critical` 字段强制 | 20 行 | **建立真正的 SoD** |
| `venue_error_rate_bps_to_halt` 默认从 5000 降到 2000 左右 | 1 行 | 熔断更早 |

全部加起来不到 100 行。**这是整份报告里"严重度 ÷ 修复成本"最高的一项**，超过第二轮的订单可提交性四缺口。

---

## 20. 第三轮：私钥与认证安全

系统持有以太坊私钥、能签名并提交真实订单，这个维度前两轮完全没审。

### 20.1 做得好的部分（明确背书）

`SecretText` 的设计是教科书级的：

```12:39:crates/quant-pivot-models/src/config/secret.rs
/// The value is zeroized on drop and deliberately implements neither
/// `Display` nor `Serialize`. `Debug` reports only whether it is configured.
pub struct SecretText(Zeroizing<String>);
impl Debug for SecretText {
    fn fmt(...) { formatter.write_str(if self.is_empty() { "<secret:unset>" } else { "<secret:redacted>" }) }
}
```

`Zeroizing` 包装、手写 `Debug` 只报告"是否配置"、不实现 `Display` 也不实现明文 `Serialize`——这堵死了密钥泄漏最经典的三条路径（日志、调试输出、序列化）。**而且架构检查器强制了这个模式**，新增密钥字段不能绕过。

其余做得好的：

| 项 | 证据 |
|---|------|
| JWT 算法固定 HS256 + `typ` 校验（防 alg=none） | `jwt.rs:645-670` |
| Refresh family 原子轮换，错 jti/generation 即灭族（重放防护） | `jwt.rs:58-74` Lua 脚本 |
| HS256 密钥强制 Base64URL 精确 32 字节 | `signing_key_bytes` |
| Casbin **默认拒绝**，未注册路由也拒绝 | `casbin/mod.rs:14-16`，`e = some(where (p.eft == allow))` |
| WebSocket 一次性 ticket（30s）+ 订阅时 channel RBAC | `ws/handler.rs:3-5`，`ws/session.rs:178-182` |
| `WebError::Internal` 掩码 + Auth 统一文案防账号枚举 | `error.rs:86-99,130-137` |
| 操作日志 WORM 触发器（禁 DELETE/UPDATE）+ 10 个敏感 key 黑名单 | `worm_triggers.rs:114-117`，`persistence_document.rs:19-29` |
| 证据链 keyed-BLAKE3，密钥 `ZeroizeOnDrop` 且支持轮换 | `research_readiness.rs:38-39,82-100` |
| CLOB L2 凭据运行时从私钥派生，**不落配置** | `config/keys.rs:1-5` |
| 配置加载 `O_NOFOLLOW` + 属主 + 权限位（含 secret 时要求 0400/0600） | `config/mod.rs:196-236` |
| Cargo.lock 已提交、依赖 pin、vendored 仅 smartcore、build.rs 无可疑行为 | — |

### 20.2 私钥的两个缺口

**缺口一：私钥只来自明文 TOML，无 KMS/HSM。** 全仓库没有任何 Vault / AWS Secrets Manager / KMS / OS keyring 集成。防护完全依赖文件权限位。对一个能签单动钱的系统，这意味着主机被入侵、备份被拖走、或者一次误操作的 `cat` 都等于资金损失。

**缺口二：运行期私钥不清零。** `Zeroizing` 只包住了 hex 解码的临时缓冲：

```21:41:crates/quant-pivot-api/src/keystore/mod.rs
        let key_bytes = Zeroizing::new(hex::decode(...)?);
        let signer = OrderSigner::from_bytes(&key_bytes)?;
```

但长期驻留的是 alloy 的 `PrivateKeySigner`，而 `OrderSigner` 没有 `ZeroizeOnDrop`：

```12:29:crates/quant-pivot-api/src/keystore/signer.rs
pub struct OrderSigner {
    signer: PrivateKeySigner,
}
```

对比同一个代码库里的 `AttestationKey`——那个是 `#[derive(Clone, Zeroize, ZeroizeOnDrop)]`。**证据签名密钥做了清零，资金签名密钥没做**，这个不对称大概率是疏漏而非有意。

### 20.3 其余安全缺口

| 严重度 | 项 | 说明 |
|--------|---|------|
| High | **JWT 无密钥轮换** | `JwtConfig` 只有单个 `signing_key`，没有 `previous_signing_keys` / `kid`。轮换即所有在线 token 立刻失效（`jwt.rs:872-877` 的测试确认了这个行为）。同一代码库的 evidence attestation **有** `previous_signing_keys`——又一处不对称 |
| High | **登录无暴力破解防护** | 只有 Argon2 全局并发上限（防资源耗尽），没有 per-IP / per-username 限流或账号锁定（`routes/auth.rs:59-83`） |
| Medium | API 缺安全响应头 | 静态资源只有 `X-Content-Type-Options: nosniff`；全仓库无 `Content-Security-Policy` / `Strict-Transport-Security` / `X-Frame-Options` |
| Medium | 默认监听 `0.0.0.0` | `config/web.rs:118-120`，**生产 example 也是 `0.0.0.0`** |
| Medium | Argon2 用 crate 默认参数 | `Argon2::default()`（m=19MiB, t=2, p=1）是 OWASP 最低推荐档，未显式 pin，crate 升级会静默改变强度 |
| Low | CORS 方法/头全放开 | `allow_any_header()` + `allow_any_method()`，但未 `allow_any_origin()` 也未 `supports_credentials()`，空白名单=禁跨域，实际风险低 |
| Low | `Keystore` 错误文案写 `key_source: "env"` | 实际来自配置字段，误导审计 |

### 20.4 本地凭据文件（已独立核实，非泄漏事故）

`config/quant-pivot.local.toml` 含真实的 Alchemy RPC key 与数据库密码。我独立验证了三件事：

```
git ls-files --error-unmatch config/quant-pivot.local.toml → 未被追踪
git log --all -- config/quant-pivot.local.toml            → 无任何提交记录
git check-ignore -v                                        → .gitignore:12 命中
git grep -E 'private_key\s*=\s*"0x[0-9a-fA-F]{64}"'        → 无任何被追踪的私钥
```

**结论：从未进入 git 历史，不是泄漏事故。** 但这些凭据以明文形式存在于开发机磁盘上，属于运维暴露面。建议至少把 RPC key 和 DB 密码也纳入定期轮换，并考虑本地开发用独立的低权限凭据。

### 20.5 依赖漏洞扫描：本次审计实跑，12 个漏洞

`cargo audit` 此前**未安装**，CI 里也没有这个门禁。我本轮装上并对 1041 个依赖跑了一次：

```
Loaded 1216 security advisories
Scanning Cargo.lock for vulnerabilities (1041 crate dependencies)
error: 12 vulnerabilities found!
warning: 11 allowed warnings found
```

完整列表与溯源如下。**关键在于这 12 个的实际暴露面差别极大**，不能一概而论：

| Crate | 版本 | Advisory | 严重度 | 修复 |
|-------|------|----------|--------|------|
| **ruint** | 1.18.0 | RUSTSEC-2026-0220 — Uint 移位操作溢出标志错误 + 移位量截断 | — | ≥1.20.0 |
| **rustls-webpki** | 0.101.7 | RUSTSEC-2026-0098 — URI 名称约束被错误接受 | — | ≥0.103.12 |
| **rustls-webpki** | 0.101.7 | RUSTSEC-2026-0099 — 通配符证书的名称约束被错误接受 | — | ≥0.103.12 |
| **rustls-webpki** | 0.101.7 | RUSTSEC-2026-0104 — CRL 解析可达 panic | — | ≥0.103.13 |
| **quick-xml** | 0.39.4 / 0.40.1 | RUSTSEC-2026-0195 — `NsReader` 无界命名空间分配导致内存耗尽 | **7.5 High** | ≥0.41.0 |
| **quick-xml** | 0.39.4 / 0.40.1 | RUSTSEC-2026-0194 — 重复属性名检查二次方复杂度 | **7.5 High** | ≥0.41.0 |
| **quinn-proto** | 0.11.14 | RUSTSEC-2026-0185 — 乱序流重组导致远程内存耗尽 | **7.5 High** | ≥0.11.15 |
| **rkyv** | 0.7.46 | RUSTSEC-2026-0235 — Rc/Arc 归档校验不足导致越界读 | — | ≥0.8.17 |
| **crossbeam-epoch** | 0.9.18 | RUSTSEC-2026-0204 — `fmt::Pointer` 无效指针解引用 | — | ≥0.9.20 |
| **rsa** | 0.9.10 | RUSTSEC-2023-0071 — Marvin Attack 时序侧信道密钥恢复 | 5.9 Medium | **无修复版本** |

另有 11 条 warning（`anyhow` 的 `downcast_mut` unsoundness、`event-listener` 的 `!Send` 跨线程、`lru` 的 `pop()` panic 安全性、`rustls-pemfile` / `smartstring` 未维护等）。

**按实际风险重新排序（我做了依赖溯源）：**

**一、`rustls-webpki` 0.101.7 的三条——TLS 证书校验弱化，但升不动。**

依赖树里**新旧两套 rustls 并存**：

```
rustls 0.21.12  +  rustls-webpki 0.101.7   ← 有洞
rustls 0.23.40  +  rustls-webpki 0.103.13  ← 已是修复版本（advisory 要求 ≥0.103.13）
```

也就是说**修复版本早就在依赖树里了，问题不是"没有新版本"，而是旧版本还被两个上游各自拖着**。旧 rustls 0.21 有两个独立来源（实测 `cargo tree -i rustls@0.21.12`）：

```
rustls v0.21.12
├── aws-smithy-http-client v1.1.11        ← S3 artifact store 路径
├── hyper-rustls v0.24.2  ┐
├── reqwest v0.11.27      ├─ 全部来自 chainlink-data-streams-sdk v1.2.2
├── tokio-rustls v0.24.1  │   （rest + websocket features）
├── tokio-tungstenite v0.20.1 │
└── tungstenite v0.20.1   ┘
```

注意 workspace 声明的是 `reqwest = "0.13"`、`tokio-tungstenite = "0.29.0"`——0.11/0.20 这两个旧版本**完全是 chainlink SDK 拖进来的**。第一轮说的"依赖 pin 得严"在直接依赖上成立，在传递依赖上不成立。

三条漏洞里两条是"名称约束被错误接受"，属**证书校验绕过类**而非可用性问题。实际暴露面评估：这两条路径连的都是已知端点（自己的 S3 bucket、Chainlink 官方 RTDS），不是任意第三方 TLS，所以风险低于通用场景——但 artifact store 存的是模型与证据的 WORM 记录，其 TLS 通道被弱化仍是实质问题。

**这条短期升不动**（要等 aws-smithy 和 chainlink SDK 各自升 rustls）。建议做法是登记为**带复审日期的已知例外**，同时向上游确认升级计划；`domain-chainlink` 是 feature-gated 的（`quant-pivot-api/Cargo.toml:8-11`，虽在 default 里），如果 RTDS 不是必需功能，关掉它能消掉一半来源。

**二、`ruint` 1.18.0——落在链上金额算术路径上，且一条命令就能升。**

```
ruint v1.18.0
└── alloy-primitives v1.6.1 → alloy-consensus / alloy-contract / alloy-dyn-abi / ...
```

`ruint` 是 alloy 的 `U256` 实现，系统的 `IntoEvmUint`、`redeemPositions` 的 `indexSets`、余额读取全部经过它。漏洞是移位操作的溢出标志错误与移位量截断。系统代码大概率不直接做 `U256` 移位，但 alloy 内部的 ABI 编解码会用。**对一个动真钱的系统，算术库的正确性问题不应该带着上生产。**

我实测了可行性：

```
cargo update -p ruint --dry-run
    Updating ruint v1.18.0 -> v1.20.0
```

**没有版本冲突，一条命令即可。** 唯一副作用是带进 `ark-serialize` / `ark-std` 等新的可选依赖。这是本次 12 个漏洞里**最该立刻做、也最容易做**的一条。

**三、`rsa` 0.9.10——链接了但不在实际签名路径上。**

```
rsa v0.9.10
└── jsonwebtoken v10.4.0
    └── quant-pivot-web → quant-pivot-core
```

这是唯一一条直连本仓库的链，而且 **Marvin Attack 无修复版本**。但实际暴露面接近零：系统的 JWT 强制 `Algorithm::HS256`（对称 HMAC，`jwt.rs:645-670` 有硬校验），根本不走 RSA 路径。`rsa` 是 `jsonwebtoken` 的 `rust_crypto` feature 拉进来的。**可以通过收窄 feature 直接消除这个攻击面**，属于"顺手清掉"级别。

**四、其余（quick-xml / quinn-proto / rkyv / crossbeam-epoch）** 都是深层传递依赖，多为 DoS 类，且大概率不在攻击者可达的输入路径上。按常规升级节奏处理即可。

**结论与三档行动（已验证可行性）：**

| 档 | 动作 | 成本 | 依据 |
|----|------|------|------|
| **立刻** | `cargo update -p ruint`（1.18.0 → 1.20.0） | 一条命令，实测无冲突 | 金额算术路径，唯一无阻力的高价值修复 |
| **立刻** | 收窄 `jsonwebtoken` feature 去掉 `rsa` | 改一行 Cargo.toml | JWT 只用 HS256，`rsa` 纯属多余攻击面 |
| **登记例外** | `rustls-webpki` 三条 | 升不动，等上游 | 修复版本已在树里，被 aws-smithy 与 chainlink SDK 拖住；两条路径均连已知端点，风险可控 |
| **常规节奏** | quick-xml / quinn-proto / rkyv / crossbeam-epoch | 随上游 | 深层传递依赖，多为 DoS 类，不在可达输入路径上 |

更重要的是**把 `cargo audit` 接进 CI**——这次是审计才发现，说明当前没有任何机制会在新漏洞披露时提醒。成本是一个 workflow step。对短期升不动的项配显式 `ignore` 列表，**每条例外都带理由和复审日期**，这样"已知例外"和"没人看"就能区分开。

---

## 21. 第三轮：报告管线与组合优化的深层问题

### 21.1 MILP 实现质量很高（明确背书）

字典序四阶段的实现比文档描述得更严谨：

```900:922:crates/quant-pivot-research/src/portfolio/solver_boundary.rs
// Robust → lock_robust → Nominal → lock_nominal → Cvar → lock_cvar → Capital → lock_capital
```

关键在于**阶段间用精确整数锁定，不是 ε 容差**。文档写的是 "epsilon lock"，实现实际上把松弛列固定在 `0..=0`，然后后验 `verify_locks` 要求 `locked == actual`。MIP gap 设为绝对零并要求 `|gap| ≤ 1e-12`：

```484:485:crates/quant-pivot-research/src/portfolio/solver_boundary.rs
model.try_set_option("mip_rel_gap", 0.0_f64);
model.try_set_option("mip_abs_gap", 0.0_f64);
```

唯一性用多趟 BLAKE3 派生权重 + Hamming 距离证明，无法隔离出唯一解时直接失败（`solver_boundary.rs:924-955`）。整数缩放遇到比 micro-USD 更细的小数时**直接报错而非静默截断**（`global.rs:2083-2090`）。超时、`2^53` 溢出、非 Optimal 状态一律 fail closed，**没有任何降级或 fallback 路径**。

这是整个代码库里工程正确性最强的一块，值得单独肯定。

### 21.2 场景层：机制扎实，但统计校准完全没有验证

组合层的所有尾部约束（robust floor、CVaR cap、max scenario loss）都建立在 `scenario_model` 生成的联合场景之上。这一层的实现有两个特点：

**做得好的是联合结构。** 场景不是独立假设——多个 Route 的残差在同一时间桶上必须齐全（缺则 fail closed），stationary bootstrap 共享同一条时间路径同时累加各 Route 残差，得到的是经验联合依赖（`scenario_model.rs:1352-1365`）。这比"假设独立"或"拍一个相关系数"强得多。PIT 纪律也严格：fit window 必须 `≤ bound_at`，bootstrap seed 刻意不依赖残差数值以防性能泄漏进随机流。

**问题一：场景概率权重是治理先验，不从数据估计。**

```735:736:crates/quant-pivot-research/src/portfolio/scenario_model.rs
distributions: self.input.methodology.distributions.clone(),
discount_curve: self.input.methodology.discount_curve.clone(),
```

fit 阶段原样 clone 模板里的权重。也就是说，"Win/Split/Loss 各自的概率是多少"这个决定组合优化结果的核心参数，**是人在模板里写死的，不是从历史数据估出来的**。这是一个影子超参：它不出现在任何超参搜索的 trial grid 里，因此不进入 PBO 的多重检验修正，也不受 DSR 的选样偏误惩罚。改这个数会显著改变 MILP 的选择，但不会在任何质量门禁上留下痕迹。

**问题二：场景分布的经验覆盖率从未被验证。**

全仓库找不到任何"生成的 P5/P95 是否真的覆盖 90% 实现值"的回测或门禁。现有的 scenario 校验都是形状类的：三类 scenario kind 齐全、joint panel 完整、contract digest 一致、最小 bucket 数（代码注释自己说这只是 "identifiability floor"，不是统计充分性证明）。模型侧的 rank IC / DSR / PBO 门禁**管的是打分模型，不是场景模型**。

合起来的后果是：CVaR 和 robust floor 这两个最重要的风险约束，**可能非常自信地给出错误的尾部**，而系统没有任何机制能发现这件事。这是第一轮识别的"scenario_model 是隐含单点"的具体化——单点不在于代码行数（2873 行），在于**它是唯一一个没有预测性校验闭环的模型**。

**建议**：加一个 scenario coverage backtest——用已结算的历史数据检验生成分布的经验覆盖率（P5/P95 名义 90% 区间的实际覆盖率、PIT 直方图均匀性检验），并把它做成 scenario model 晋升的硬门禁。这与既有的 model quality gate 是同构的，可以复用大部分基础设施。

### 21.3 "什么时候买"这个问题实际上没有答案

`04-topn-report-and-recommendation.md` 承诺报告回答九个操作问题。逐项核实下来，八个有实质答案（买什么、买多少、什么时候卖、卖多少、止盈、止损、出场节点、风险敞口都在 `TradePlan` 里有完整字段），**只有"入场触发"是退化的**：

```582:612:crates/quant-pivot-core/src/report/composer.rs
fn immediate_entry_plan(...) -> QuantResult<EntryPlan> {
    if !matches!(... EntryConditionTemplate::Immediate) {
        return Err(... "not an immediate-entry policy cohort");
    }
```

生产路径只接受 `Immediate`，非 Immediate 的 trade policy 直接导致报告失败。所以"什么时候买"的实际答案永远是"在 `valid_until` 之前尽快买"。文档描述的多种入场触发类型没有落地。

对 ReportOnly 模式这尤其要紧——人工执行必然有延迟，而系统给出的唯一时机指导是"立刻"。配合 §21.4 的漂移问题，操作者拿到报告时的最优行动可能已经和报告假设不一致。

### 21.4 两个次级问题

**报告到执行的漂移没有组合级再优化。** 报告冻结了 `worst_price` 和 `entry_vwap`，执行时 admission 会用当前 L2 重新 walk 并检查 freshness、slippage、fillability（`admission/checks.rs:232-261,620-664`），漂移过大就 deny。防护是有的，但**不会重跑组合优化**——如果 TopN 里有三个候选价格漂移了，剩下的组合可能已经不是最优解，系统不会告诉你这件事。

**操作者看不到"为什么这个没被选中"。** 文档承诺每条推荐带 `binding_constraints`（是预算、CVaR、还是事件互斥卡住了相邻候选），实现里 `RecommendationEconomics` 和 `SizingPlan` **都没有这个字段**。有的是 marginal robust USD、CVaR contribution、factor breakdown——能解释"这条为什么好"，不能解释"那条为什么被挤掉"。对需要判断该不该信报告的人，后者往往更重要。

**CVaR 有两套定义并存。** MILP 里用 Rockafellar epigraph 形式（`η + u ≥ 0`，`solver_boundary.rs:368-377`），exact verification 里用正损失的离散 ES（`global.rs:2028-2071`）。在 `η ≥ 0` 时两者通常对齐，靠 post-check 强制相等。这不是 bug，但是持续的认知负担和潜在的分歧点。

---

## 22. 第三轮：治理执行器自身可绕过

`xtask/src/architecture.rs`（4094 行）是所有架构规则的执行者。如果它有洞，前两轮认可的"架构不变量真落地"就要打折。

**主体是可靠的。** 依赖方向、import 树、body path、`Arc<Uuid>`、`Sender<String>`、glob/forwarding re-export、JSONB 文档形状、`SecretText` 模式、函数四词命名——这些都走 `syn` AST + Visit，判定准确。CI 里 `cargo xtask architecture check` 是 `rust-static` job 的必过步骤，全部 workflow **没有 `continue-on-error`**。检查器自己也在被检查的源码集合里。

**但有四条真实的逃逸路径：**

1. **34 处字符串针检测。** 删除契约（禁止 `EndgameDetector`、`ScoredOpportunity` 之类）用的是 `source.contains("OldType")`。拆分字面量、改空白、或用 `concat!` 都能绕过。这类规则本质上是"防手滑"而非"防绕过"，但文档把它们和 AST 规则并列呈现为同等强度。

2. **`split_once("#[cfg(test)]")` 截断。** 多处字符串契约（如 `architecture.rs:689-690,957`）只扫描第一个 `#[cfg(test)]` 之前的内容。**如果一个文件在 `mod tests` 之后还有生产代码，那部分不被这些针检测。** 这在大文件里完全可能——而这个代码库恰好有不少三四千行的文件。

3. **allowlist 加了没人管。** `parallel_kernel_allowed`（4 条路径）、`insert_many` 白名单、compute/bench 的 rayon 豁免——加名单就是改 xtask 源码，靠 code review 把关，没有独立的审计记录或审批流。AGENTS.md 明说"路径和函数名 allowlist 是禁止的"，实际存在若干。

4. **宏与 `include!` 不展开。** 带 `@generated by sea-orm-codegen` 标记的文件整个跳过函数审计；proc-macro 展开后的代码不在源 AST 里；`include!` 的内容不被解析。

**另一个缺口：`cargo xtask config audit` 存在（`main.rs:166-174`）但 CI 不调用。** 配置契约漂移（比如 §19 那些校验缺口的对偶——契约与实现不一致）可以直接漏出 CI。

### 22.1 测试有效性

| 指标 | 实测 |
|------|------|
| 测试总数 | 2216 |
| `assert!(...is_err())` | **586** |
| `.expect_err(` | **250** |
| `#[should_panic]` | 2 |
| `.has_errors()` | 62 |
| 负例断言合计（可重叠） | **~900（约 41%）** |
| 架构检查器自身测试 | 25（其中 14 个是负例） |
| 函数审计器测试 | 21 |
| 关键不变量专项测试（按命名启发式） | ~36 |
| **mutation testing** | **无** |
| **fuzzing** | **无** |
| **覆盖率工具** | **无** |

负例占比 41% 是个好数字——说明测试不只验证 happy path，`fail-closed` 语义有实际验证。nextest 配置 `retries = 0`（不掩盖 flaky）、system 测试单线程 45 分钟超时，都是负责任的设置。

缺的是另一个维度：**没有任何工具在验证"这些测试真的能抓到 bug"**。2216 个测试在 58 万行代码上是什么覆盖率，没人知道（无 tarpaulin / llvm-cov 配置）；测试对代码变异的敏感度如何，也没人知道（无 cargo-mutants）。对一个已经在测试上投入 17 万行的项目，加一次 mutation testing 的边际成本很低，而它能回答"这 17 万行值不值"这个问题。

---

## 23. 第三轮风险追加

| ID | 风险 | 严重度 | 建议动作 |
|----|------|--------|---------|
| **R34** | **单人可合法掏空风控**：滑点/样本量/回撤三处校验缺口 + 无 SoD | **Critical** | 补三处校验 + `activate` 拒绝 `decided_by == activated_by`（合计 <100 行） |
| **R35** | 私钥明文 TOML，无 KMS/HSM | **Critical** | 至少接 OS keyring；理想是 KMS/HSM 签名 |
| **R36** | `OrderSigner` 未 `ZeroizeOnDrop`（而证据密钥做了） | **High** | 对齐 `AttestationKey` 的处理 |
| **R37** | 场景分布权重是不进 trial grid 的影子超参 | **High** | 纳入 PBO trial 计数，或从数据估计 |
| **R38** | 场景 P5/P95 经验覆盖率从未验证，而 CVaR/robust 建立其上 | **High** | 加 scenario coverage backtest 作为晋升硬门禁 |
| **R39** | JWT 无密钥轮换（evidence 有，JWT 没有） | **High** | 加 `previous_signing_keys` + `kid` |
| **R40** | 登录无 IP/账号级限流 | **High** | 加限流与锁定 |
| **R41** | portfolio USD 上限无绝对天花板；breaker 默认 50% 错误率才熔断 | **High** | deploy 层设硬顶；默认降到 ~20% |
| **R42** | 入场触发只支持 Immediate，"什么时候买"无实质答案 | **Medium** | 落地条件触发，或修正文档承诺 |
| **R43** | `binding_constraints` 文档有实现无，操作者看不到落选原因 | **Medium** | 从 MILP 对偶/松弛提取并落到推荐上 |
| **R44** | 架构检查器可绕过：字符串针、`cfg(test)` 截断、allowlist、宏 | **Medium** | 针改 AST；截断改为全文件扫描；allowlist 加审计 |
| **R45** | `cargo xtask config audit` 存在但 CI 不跑 | **Medium** | 接入 CI |
| **R46** | **`cargo audit` 实跑出 12 个漏洞**，且 CI 无此门禁 | **High** | 优先升 `rustls-webpki`（证书校验绕过）与 `ruint`（金额算术）；收窄 `jsonwebtoken` feature 去掉 `rsa`；`cargo audit` 接入 CI 并对无法升级项配显式 ignore + 复审日期 |
| **R47** | 无 mutation testing / fuzzing / 覆盖率度量 | **Medium** | 至少跑一次 cargo-mutants 摸底 |
| **R48** | API 缺 CSP/HSTS/X-Frame-Options；默认监听 `0.0.0.0`（生产 example 同） | **Medium** | 加安全头；生产默认 `127.0.0.1` |
| **R49** | 报告→执行漂移不触发组合级再优化 | **Medium** | 漂移超阈时标记整份报告需重算 |
| **R50** | Argon2 用 crate 默认参数，未显式 pin | **Low** | 显式 pin 并提高到 m≥64MiB |

---

## 24. 三轮审计的收敛判断

三轮下来，发现的分布很能说明问题：

| 轮次 | 视角 | 发现性质 |
|------|------|---------|
| 第一轮 | 广度测绘 + 业界对标 | **战略层**：数据可得性判断错误、策略池与已验证 alpha 错配、成本模型缺项 |
| 第二轮 | 逐行交叉验证 + 未覆盖维度 | **执行层**：订单可提交性四缺口、CH 容量炸弹、loom 用错地方 |
| 第三轮 | 安全 + 主产物 + 治理根基 | **控制层**：风控可被单人掏空、私钥无 KMS、场景层无校准闭环、检查器可绕过 |

**三轮都没有发现"代码写错了"。** 统计公式对照原始论文无误、seqlock 逐行核对正确、MILP 用精确整数锁加唯一性证明、clippy pedantic+nursery 零告警、`unsafe`/`todo!`/`Internal` 三项归零。发现的全部是"边界没画到"或"目标指错了"。

而这三层有一个共同的结构性成因：**系统的治理体系是自洽的，但它的边界停在自己画的圈里。**

- 架构检查器保证了内部一致性，但 Polymarket 的 tick/size/allowance 约束不在它的词汇表里（第二轮 §14）
- 配置校验保证了类型和范围合法，但"这个范围本身合不合理"没人校验（第三轮 §19.1）
- 模型质量门禁保证了打分模型的统计显著性，但场景模型不在门禁覆盖内（第三轮 §21.2）
- CPCV/PBO 保证了超参搜索的多重检验修正，但 methodology 模板里的权重不算超参（第三轮 §21.2）
- 审计日志完整记录了 who/what/when，但不校验 who 是不是同一个人（第三轮 §19.2）
- 直接依赖 pin 得很严（`sea-orm=2.0.0`、`sqlx=0.9.0` 这种精确锁），但传递依赖里躺着两套 rustls，旧的那套有证书校验绕过（第三轮 §20.5）
- 架构检查器守住了所有内部规则，但它自己的字符串针、`cfg(test)` 截断和 allowlist 没有被任何东西守住（第三轮 §22）

**这个模式出现了七次，说明它不是若干独立疏漏，而是"治理覆盖面"这个元问题本身没有被治理。** 系统建立了一套非常强的规则执行机制，然后把注意力全部投入到"让规则被严格执行"上，却没有人定期问"规则集本身覆盖了哪些输入、漏了哪些"。

具体建议是做一次显式的**信任边界盘点**：把系统运行时依赖的所有外部输入列成一张表，每一项标注它当前是"自动校验"、"人工责任"还是"未定义"——

| 输入类别 | 例子 | 当前状态 |
|---------|------|---------|
| Venue 约束 | tick size、min order size、精度、allowance、限流 | 部分自动，部分未定义 |
| 配置取值范围 | 滑点上限、样本量下限、回撤上限、资金天花板 | **多处未定义** |
| 模板参数 | scenario methodology 的分布权重、stress 模板 | **未定义（影子超参）** |
| 人工决策 | approve/activate 是否同人、allowlist 增补 | **未定义** |
| 传递依赖 | rustls-webpki、ruint 等的安全状态 | **未定义（无 cargo audit）** |
| 模型预测质量 | 打分模型（有门禁）vs 场景模型（无门禁） | 部分覆盖 |

把"未定义"这一列清空，比逐条修 bug 更根本，也能防止下一轮审计再发现第八个同构问题。

---

## 25. 总评

三轮审计下来，问题呈现出清晰的分层，而且**没有一层在"代码写得对不对"上**：

**第一层，代码之外的三个判断（战略）：**

1. **数据可得性判断错误**，导致一个本可两周上线的系统被自己锁在 200 天等待里。
2. **策略假设过窄**，把全部赌注压在需要统计显著性的 ML 路径上，而这个市场上被验证的钱主要在不需要模型的地方。
3. **成本模型不完整**，训练在优化一个和考核标准不同的量，且漏掉了一整个收入项。

**第二层，架构之下的执行细节（战术）：** tick 对齐、订单精度舍入、最小订单量、allowance 预检。这四项都不违反任何架构不变量，不触发任何 lint，有 fail-closed 兜底所以也不会引发事故——它们只会让系统"产出很多推荐，一单也下不出去"。

**第三层，控制面自身的边界（安全与治理）：** 风控参数可被单人合法改到失效（三处校验缺口 + 无 SoD）、私钥无 KMS 且运行期不清零、场景模型的概率权重是不进 trial grid 的影子超参且其分布从未被经验校验、架构检查器有四条逃逸路径。**这一层最危险，因为它的失效是静默且"合规"的**——审计日志会显示一切正常。

**第四层，时间维度的运维债（长期）：** ClickHouse 无 TTL 且架构主动禁止加 TTL，200 天 L2 数据可达 10–26 TB。必须在开始积累之前解决。

四层的共同特征是：**修复成本都远低于已经投入的工程量，而且所需的基础设施全部已经建好了**——PIT 引擎、L2 walk、费用曲线、tick 对齐函数（只是没用在入场）、`min_order_size` 字段（只是没读）、`Retry-After` 处理（只是没接新 header）、`RuntimeFieldRiskLevel::Critical` 标记（只驱动 UI 不驱动流程）、`ZeroizeOnDrop`（用在证据密钥没用在资金密钥）、`previous_signing_keys`（evidence 有 JWT 没有）、`cargo xtask config audit`（写了但 CI 不跑）。

这个清单本身说明了问题的性质：**几乎每一个缺口，旁边都躺着一个已经写好的、本可以填上它的东西。** 缺的不是能力，甚至不完全是意识——是把已有能力接到正确位置的最后一步。

最后一句留给这套代码本身：在 58 万行、pedantic + nursery 全开零告警、`unsafe`/`todo!`/`Internal` 三项归零、seqlock 经得起逐行核对、统计公式经得起与原始论文逐条对照、MILP 用精确整数锁加唯一性证明的前提下，我三轮找到的所有问题都是"做什么"和"边界画到哪"的问题，**没有一个是"怎么做"的问题**。这个比例，在我审过的量化代码库里是罕见的。
