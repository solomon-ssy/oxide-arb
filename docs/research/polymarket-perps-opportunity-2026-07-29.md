# Polymarket Perps 战略与量化机会调研

> 研究日期：2026-07-29  
> 研究对象：Polymarket Perps 与 quant-pivot 的可盈利结合点  
> 结论性质：策略与工程立项建议，不是收益承诺或个性化投资建议

## 1. 执行摘要

### 1.1 结论

**确实存在值得投入研究的机会，但尚不存在可以仅凭公开资料直接确认的“无风险赚钱方案”。**

最有价值的不是把 quant-pivot 改造成一个通用 Perps 趋势交易机器人，而是利用 Polymarket
同时拥有“事件概率市场”和“连续资产价格市场”这一新结构，做其他 Perps 交易者较难复制的
跨产品信息与相对价值策略：

1. **立即立项：Perps 数据增强现有预测市场 `RecommendationReport`。**  
   把 Perps 的 Mark、Index、基差、Funding、OI、成交和 L2 微观结构作为 Crypto、Finance、
   Commodities 等预测市场的 point-in-time 特征。第一阶段只读数据、继续在
   `ReportOnly` 下运行，不承担 Perps 清算风险。这条路径复用度最高、验证成本最低。
2. **优先验证：预测合约—Perps 数字期权相对价值。**  
   对“BTC 高于某价”“SPX 当日涨跌”“WTI 收于某价以上”等合约，建立数字期权公允概率、
   跨行权价无套利约束，并用同平台 Perps 做动态 Delta 对冲。已有预印本发现 Polymarket
   BTC 阈值合约与期权隐含价格之间存在持续价差，但 2026 年的新费率、结算口径和动态对冲成本
   可能吃掉大部分利润，所以只能先做 shadow。
3. **第二梯队：事件概率冲击驱动 Perps。**  
   用真正外生的 Fed、选举、战争、并购、财报等概率创新预测 SP500、NAS100、黄金、WTI
   或个股 Perps 的 1 分钟至 1 小时回报。其差异化较强，但必须先证明是事件市场领先资产价格，
   而不是资产价格先动、预测市场被动跟随。
4. **有条件探索：早期市场做市。**  
   Perps 提供完整 API、顺序号、dead-man switch，且立即成交订单有 20ms taker delay；
   但普通新账户 maker 每次成交仍支付 1.25 bps，只有 $1B 级费率档才有 0.5 bps rebate。
   Beta 的部分账户暂享顶级费率只是临时政策，不能作为长期商业模型。
5. **当前架构内不做：纯 Funding 套利和跨所基差套利。**  
   单腿收 Funding 仍有完整方向风险。要做近似 Delta-neutral 必须在 Binance、Hyperliquid、
   现货或传统期货上反向对冲，这违反当前“Polymarket-only、无跨所执行”的硬边界。

因此，本报告给出的决策是：

> **Go：投入 30–60 天做只读数据与 shadow 研究。**  
> **Conditional Go：只有净成本后统计证据、深度、账户费率、运行安全和法律意见全部通过，
> 才做低杠杆 isolated 的 `SemiAuto` 小额试验。**  
> **No-Go：现在直接接入自动执行、按 20x 杠杆交易，或把瞬时 Funding 年化后追逐。**

### 1.2 机会优先级

| 方向 | 差异化 | 复用现有系统 | 资本风险 | 当前判断 |
|---|---:|---:|---:|---|
| Perps 数据增强预测报告 | 高 | 很高 | 无（只读） | **立即做** |
| 数字期权相对价值 + Perps 对冲 | 高 | 高 | 中 | **优先 shadow** |
| 事件概率冲击 → Perps | 高 | 中高 | 中 | **收集数据后验证** |
| Perps 做市 | 中低 | 中 | 高 | **条件式研究** |
| Funding / Basis capture | 低到中 | 低 | 中高 | **当前边界内不做** |
| 通用趋势、均值回归、配对 | 低 | 中 | 中 | **不作为主线** |
| 信号产品 + Perps referral | 中 | 中 | 低交易风险 | **商业副线，先审合规** |

## 2. 研究口径与限制

本报告交叉核验了：

- Polymarket 的 Perps 产品页、官方 Perps 文档、HTTP OpenAPI、WebSocket 文档和 changelog；
- 官方 pUSD、费率、Funding、Mark、Index、Margin、Liquidation、地域限制说明；
- 2026-07-29 产品页服务端渲染的 instruments/ticker 快照；
- quant-pivot 当前架构、外部领域数据、PIT、模型、报告、账户和执行代码；
- Perps 行业规模研究和若干 Polymarket/衍生品市场微观结构预印本。

限制如下：

1. Perps 仍处于 **early access**，产品、费率、标的和 API 都可能快速变化。
2. 本研究环境无法与 `api.perpetuals.polymarket.com` 完成直接 TLS 握手；这不能解释为平台故障。
   市场快照来自官方产品页 SSR，API 合同来自官方 OpenAPI，因此本报告没有真实连续深度、
   队列位置、成交确认延迟或账户实际费率数据。
3. 单日 24h Volume 是平台自报成交量，不等于可执行深度，也未独立验证是否受 Beta 费率、
   激励或高频换手影响。
4. 引用的 arXiv 研究属于预印本，不应视为已完成同行评审或保证未来仍有同样价差。
5. 所有收益判断都以“扣除费率、点差、冲击、Funding、对冲误差和操作损失后”为准。

## 3. Polymarket Perps 到底是什么

### 3.1 不是“永续预测合约”

Polymarket 官方定义明确：Perps 是追踪指数、商品、加密资产和股票的传统永续期货，
可多空、无到期日、使用保证金和 Funding；它与以 Yes/No 事件结算的预测市场是两个产品。
目前仍需 referral code，属于 early access。参见
[Perps Overview](https://docs.polymarket.com/perps/overview) 和
[Perps FAQ](https://docs.polymarket.com/perps/faq)。

### 3.2 市场与结算结构

- 撮合、订单簿、风险检查、余额、仓位、保证金和 Funding 在链下执行；
- 充值、提现在 Polygon 结算；
- 交易所定期把链下账本的 state root 提交到链上；
- 交易本身不会逐笔上链。

这是一种“中心化低延迟撮合 + 链上托管出入金和状态承诺”的混合交易所，而不是现有预测市场
CLOB 客户端的简单新增 endpoint。参见
[Perps Architecture](https://docs.polymarket.com/perps/learn-about-trading/architecture)。

保证金资产是 pUSD：Polygon 上 6 位小数的 ERC-20，由 USDC 链上支持，可转移，但官方目前
没有外部交易所上币计划。参见
[pUSD 官方文档](https://docs.polymarket.com/concepts/pusd)。

### 3.3 价格、Funding 与清算

**Index Price** 每 200ms 发布，由 Pyth、Chainlink Data Streams、Hyperliquid 等外部源经
过期过滤、异常值过滤和加权聚合得到。**Mark Price** 同样每 200ms 更新，是以下三项的中位数：

1. 相对 Index 的 150 秒 EMA 平滑本地 order-book mid；
2. 本地 Best Bid、Best Ask、最近成交的中位数；
3. 独立外部 Mark feeds 的聚合值。

Mark 用于未实现 PnL、权益、保证金、Funding premium 和清算。该设计降低单一薄订单簿或单一
外部源操纵 Mark 的能力，但并不消除 stale feed、session 切换和极端行情中的模型风险。参见
[Mark Price](https://docs.polymarket.com/perps/learn-about-trading/mark-price) 与
[Index Price](https://docs.polymarket.com/perps/learn-about-trading/index-price)。

Funding 的关键参数是：

- 每 5 秒按双边各 1,000 pUSD 冲击 VWAP 采样 premium；
- 在 1 小时窗口内平均，并每小时结算；
- Crypto 的 scale 为 1.0，非 Crypto 为 0.5；
- 绝对上限是 4%/小时；
- Funding 是多空之间直接转移，协议不抽成。

公式和参数见
[Funding 官方说明](https://docs.polymarket.com/perps/learn-about-trading/funding)。

保证金与清算的核心公式：

```text
Equity = Collateral + UnrealizedPnL(Mark) - FeesDue - FundingDue
IM     = Notional / ConfiguredLeverage
MM     = Notional × MMR
MMR    = 0.5 / MarketMaxLeverage
```

当 `Equity < MM` 时触发清算。普通清算用 reduce-only IOC 市价扫簿，没有相对 Mark 的保护价；
严重不足时保险基金直接吸收仓位。被清算账户还需在正常 maker/taker 费率之外支付额外
liquidation fee。参见
[Margin](https://docs.polymarket.com/perps/learn-about-trading/margin) 和
[Liquidation Mechanics](https://docs.polymarket.com/perps/learn-about-trading/liquidation-mechanics)。

### 3.4 24/7 股票和商品并不等于风险连续

Perps 全天撮合，但股票、指数和商品的底层市场会收盘、周末、停牌或发生公司行动。session
只改变 Index 与 Mark 的外部 feed 集合；Funding、撮合、保证金和清算始终继续运行。
这会创造 session 切换、隔夜和周末的相对价值研究空间，同时也放大跳空、薄流动性与 oracle
风险。参见
[Market Sessions](https://docs.polymarket.com/perps/learn-about-trading/market-sessions)。

## 4. 早期市场快照

### 4.1 2026-07-29 10:25:12 UTC 官方 SSR 快照

从官方 BTC Perps 页面服务端 `perps/instruments` 查询状态提取并按
`OI contracts × Mark` 换算 OI notional：

| 类别 | 标的数 | 24h Volume | OI notional | Volume / OI |
|---|---:|---:|---:|---:|
| Index | 2 | $6.81M | $11.00M | 0.62x |
| Commodity | 3 | $14.75M | $1.96M | 7.52x |
| Crypto | 4 | $28.65M | $9.61M | 2.98x |
| Equity | 20 | $153.25M | $4.46M | 34.39x |
| **合计** | **29** | **$203.47M** | **$27.03M** | **7.53x** |

动态产品页可在
[Polymarket Perps](https://polymarket.com/perps) 和
[BTC-USD](https://polymarket.com/perps/asset/btc) 查看。

额外观察：

- 前五大成交标的占 24h Volume 的 41.9%；
- SP500、ETH、NAS100、BTC 四个标的占换算 OI 的约 73.6%；
- Equity 占成交量 75.3%，但只占 OI 16.5%，换手显著高于其他类别；
- 快照中 `SKHYNIX-USD` 的 Funding 约为 +4.96 bps/小时、Mark-Index 约 +50 bps；
  `ARM-USD` 和 `AVGO-USD` 则出现接近 -1 bps/小时的 Funding。

这些异常说明早期市场确实存在拥挤和价格偏离，但**不能直接解释成可赚收益**：
Funding 是瞬时状态，开仓冲击、对冲腿、下一小时费率回落和退出成本都可能超过收益。

### 4.2 对市场成熟度的判断

该快照说明 Perps 已不是空壳产品，但仍明显处于早期：

- 29 个标的、$27M OI 对一个 early-access 市场已经有研究价值；
- OI 高度集中，应该先研究 BTC、ETH、SP500、NAS100，而不是被高换手小 OI 股票吸引；
- Volume/OI 极高的 Equity 需要区分真实流动性、做市换手、Beta 费率和自成交/激励效应；
- 没有连续 L2 和真实 fill 数据前，不能以 24h Volume 推算策略容量。

作为行业尺度参照，CoinGecko 统计 2026 年前 12 家 Perp DEX 月均成交约 $611.57B、OI
份额约 13.5%；Polymarket 即使把当前单日成交机械乘以 30，也只是约 $6.1B，且这种外推
不是预测。参见
[State of Crypto Perpetuals 2026](https://www.coingecko.com/research/publications/state-of-crypto-perpetuals-report-2026)。

## 5. 这是战略转型吗

更准确的表述是：**从“单一预测市场”向“事件概率 + 连续衍生品 + 数据与结算基础设施”的
平台化扩张，而不是放弃预测市场。**

证据链包括：

1. 2026 年 4 月 Polymarket 完成新交易合约、重写 CLOB 和 pUSD 抵押层升级；
2. Perps changelog 从 6 月开始快速补齐 reduce-only、dead-man switch、20ms taker delay、
   cancel-all 和 rate-limit 语义；
3. Perps 以独立账户、独立代理凭证、独立撮合和独立 API 运行；
4. ICE 在 2025 年宣布最高 $2B 战略投资、约 $8B 投前估值，并成为 Polymarket 事件数据的
   全球分销方，同时合作未来 tokenization。

参见
[Perps Changelog](https://docs.polymarket.com/changelog/perps) 和
[ICE 战略投资公告](https://ir.theice.com/press/news-details/2025/ICE-Announces-Strategic-Investment-in-Polymarket/default.aspx)。

这次扩张的商业逻辑很清楚：

- 预测市场受事件和新闻周期驱动，Perps 可产生 24/7 高频复购和持续手续费；
- 同一个品牌、钱包与 pUSD 形成交叉销售；
- “某事件发生概率”可以自然转化成“对应资产做多/做空或对冲”；
- 事件概率本身可成为机构数据产品，Perps 又提供直接表达资产观点的交易层。

但通用 Perps 是极拥挤赛道。Polymarket 的真正护城河不是又一个 BTC order book，而是
**事件概率、事件语义与连续资产风险之间的联结**。quant-pivot 应围绕这个护城河构建，
不应与成熟 Perp DEX 在普通趋势、Funding 搬砖或纯延迟上正面竞争。

## 6. quant-pivot 的可复用能力与真实缺口

当前权威架构仍是：

```text
Gamma/CLOB/外部领域事实
  -> PIT selection/features/factors/models
  -> portfolio
  -> RecommendationReport
  -> ReportOnly | SemiAuto | AutoExecution
  -> binary CLOB order/position/reconciliation
```

参见
[总体架构](../plans/quant-pivot/00-quant-pivot-architecture.md)、
[数据与模型管线](../plans/quant-pivot/03-data-factor-model-pipeline.md) 和
[运行架构](../operations/architecture-and-design.md)。

### 6.1 可以复用

| 现有能力 | 对 Perps 的价值 |
|---|---|
| 有界、分区、可恢复的 CLOB L2 ingest | 可复用并发、背压、gap recovery 和 durable cursor 思路 |
| ClickHouse 事实层与 immutable PIT boundary | 可防止未来数据泄漏，并重放 Funding/Book/Trade |
| Binance Spot、USD-M Futures、Chainlink、RTDS | 可作为 Perps Index/Mark 独立核验和对冲基准 |
| `MarketLinkage` + crypto threshold/up-down 解析 | 可把预测合约映射到 BTC/ETH/SOL/HYPE Perps |
| Feature/Factor/Model registry 与 online/offline parity | 可治理新增跨产品因子 |
| CPCV、walk-forward、shadow、质量门禁 | 适合验证高度多重检验的策略 |
| `RecommendationReport`、risk envelope、审批与 kill switch | 可作为 Perps 策略治理外壳 |

尤其重要的是，系统已经明确接入 Binance USD-M Futures 的 public kline/aggTrade，并已有
`domain.crypto.distance_to_strike`、momentum、realized vol、beta 和
`basis_vs_resolution_source` 等语义。因此“用 Perps 增强预测市场”是现有垂直领域数据面的
自然扩展，而不是重写系统。

### 6.2 不能直接复用

当前执行语义与 Perps 根本不同：

- `OrderIntentKind` 只有 `Buy`；
- recommendation 绑定 `MarketId + TokenId + OutcomeSide(Yes/No)`；
- entry/exit 使用 `Price + Shares + Usd`，持仓最终可 settlement/redeem；
- 账户真值来自 prediction CLOB collateral + Data API positions；
- CLOB 凭证、订单 ID、fill、链上交易和对账语义与 Perps 不同。

Perps 则需要：

- `InstrumentId`、signed position size、Long/Short、reduce-only；
- isolated/cross、configured leverage、IM、MM、liquidation price；
- Funding、fee tier、session、Mark/Index；
- 独立 proxy signer/secret、默认一周的 delegated session；
- orders/fills/portfolio/balance/funding/deposit/withdraw 的独立账本。

因此，不应把现有 `OrderIntent` 加几个 nullable 字段后强行兼容。若进入执行阶段，应建立
Polymarket-specific 的 **Perps execution bounded context**，复用治理流程而不是复用错误的
binary DTO。项目也仍有
[PERF-21 in-progress 工作](../plans/quant-pivot/09-extreme-performance-ledger.md)，
不应在其未闭合时把新执行热路径混入当前改动。

## 7. 可盈利机会详解

### 7.1 机会 A：Perps 作为预测市场的领先/状态特征源

#### 机制

针对与资产价格直接相关的预测市场，新增：

- `perps.mark_return_{5s,1m,5m,1h}`
- `perps.index_return_*`
- `perps.mark_index_basis_bps`
- `perps.funding_rate`、`funding_zscore`、`funding_change`
- `perps.open_interest_change`
- `perps.trade_imbalance`
- `perps.book_imbalance`、`depth_10bps`、`spread_bps`
- `perps.session`、`session_transition`
- Perps 与 Binance/Chainlink/预测合约结算源之间的 basis

然后研究它们是否提高：

- Crypto/Finance 预测价格的短期回报预测；
- 阈值合约的公允概率与校准；
- prediction CLOB entry/exit 时机；
- 数据质量与异常市场排除能力。

#### 为什么可能赚钱

Perps 的杠杆、Funding 和 OI 能表达普通 spot/kline 看不到的仓位拥挤与边际风险偏好。
即使不在 Perps 下单，只要它能改善预测市场的排序、入场或退出，现有系统就能获益。

#### 为什么优先级最高

- 第一阶段零 Perps 资本、零清算风险；
- 复用现有 domain source、PIT、feature parity、backtest 和 report；
- 不需要先改现有 prediction execution；
- 即使最终没有稳定交易 alpha，数据质量、风险过滤和市场研究仍有价值。

### 7.2 机会 B：数字期权相对价值与 Perps Delta 对冲

“价格在 T 时刻高于 K”的 Yes share，本质上接近支付 $1 的数字看涨期权。简化模型下：

```text
FairProbability ≈ N(d2)
DigitalDelta     ≈ φ(d2) / (S × σ × sqrt(T))
```

可构建三层信号：

1. **同一到期日跨 strike 单调性与概率质量约束**；
2. **预测价格与模型/期权隐含数字价格的偏差**；
3. **用 Polymarket Perps 动态对冲标的 Delta，保留概率价差暴露**。

2026 年预印本
[Do Prediction Markets Match Option Prices?](https://arxiv.org/abs/2606.19517)
对 2023 年 BTC 阈值合约发现：主样本平均价差 5.6 个百分点、214 个小时观测，价差半衰期约
4 小时；合并样本约 6.3 个百分点，Delta-hedged proxy 在保守成本后仍为正，但统计精度有限。
这证明“分割市场之间存在持续 wedge”是值得实证检验的假说，而不是证明 2026 年仍可复制。

必须处理的风险：

- prediction 合约可能按 Binance candle 或 Chainlink 结算，而 Perps Index 聚合
  Pyth/Chainlink/Hyperliquid，存在 terminal basis；
- 数字期权在接近 strike/到期时 gamma 极高，离散对冲会产生大误差；
- prediction CLOB 2026 新费率可能远高于论文时期；
- 预测合约通常只能买 Yes 或 No，组合和库存管理不同于可自由卖空的期权；
- 5 分钟 BTC 合约有结算时点操纵的近期实证风险。

关于最后一点，
[Settlement Manipulation in Prediction Markets](https://arxiv.org/abs/2606.31675)
报告 5 分钟 BTC 合约结算时 spot order flow 激增并在结算后反转，而 15 分钟合约中该现象
大幅减弱。第一版应排除 5 分钟合约，优先 1 小时、日度和更长阈值市场。

### 7.3 机会 C：事件概率冲击驱动 Perps

核心变量不应是原始概率，而应是经过流动性和语义校准的概率创新，例如：

```text
event_shock = Δlogit(probability)
              × liquidity_confidence
              × cross_market_consistency
              × semantic_direction
```

适合研究的映射：

| 事件族 | 可能受影响的 Perps |
|---|---|
| Fed 加息/降息、CPI、就业 | SP500、NAS100、GOLD |
| 战争升级、制裁、产油设施中断 | WTIOIL、GOLD、SP500 |
| 财报、并购、监管、IPO | 对应 Equity、NAS100 |
| Crypto ETF、监管、协议事件 | BTC、ETH、SOL、HYPE |

这里必须冻结一张可审计的 `event -> asset -> signed exposure` 图，而不是线上调用 LLM
临时猜方向。

关键识别问题：

- 对“BTC 高于 K”这类由资产价格直接决定的市场，通常是资产价格领先预测市场，不能把它
  当成预测 Perps 的独立信息；
- 只使用真正外生、具有清晰经济方向的事件；
- 用 lead-lag、Granger/局部投影、事件研究和 placebo 检验方向；
- 按事件簇而不是每个 tick 计算独立样本数，防止显著性虚高；
- 优先研究 1–60 分钟，不做依赖微秒优势的 latency race。

现有 Polymarket 微观结构预印本还提示：
[The Anatomy of a Decentralized Prediction Market](https://arxiv.org/abs/2604.24366)
发现从 public order-book feed 推断的成交方向与链上真值仅约 59% bucket 一致，且 ingest
延迟虽中位数低于 50ms、尾部可达数秒。因此 event flow 特征必须使用权威 trade side/fill
证据，不能只靠 book delta 猜主动方。

### 7.4 机会 D：早期 Perps 做市

做市收益公式：

```text
NetMM =
  CapturedSpread
  + MakerRebate
  + FavorableFunding
  - MakerFees
  - AdverseSelection
  - InventoryMarkLoss
  - HedgeCost
  - OperationalLoss
```

有利条件：

- early-access 的标的和做市商数量可能暂时较少；
- WebSocket Book/Trade/Ticker 带 `sq` 顺序号；
- 有 post-only、reduce-only、IOC/FOK、cancel-all 和 dead-man switch；
- 立即成交的 taker order 被增加 20ms delay，对及时撤单的 maker 有一定保护；
- 默认账户每分钟 5,000 个 action token、最多 1,000 个 open order，足以做中低频报价。

不利条件：

- 普通新账户 maker 也收费，不是零费率场所；
- 20ms 保护不代表我们拥有低延迟优势；
- 本地 Mark 同时使用外部 feeds，成熟做市商可直接用同类底层价格；
- 股票/商品夜间仍持续清算，Inventory 风险高；
- 目前没有队列位置、真实 fill、撤单成功率和断线损失数据。

因此只把做市列为条件式研究。首选 OI 最大的 BTC、ETH、SP500、NAS100，避免用高成交但
低 OI 的 Beta Equity 作为第一批标的。

### 7.5 机会 E：Funding、session 与 Mark/Index 偏离

当 premium 接近零时，Funding 公式中的固定利息腿意味着理论基线约为：

- Crypto：+0.125 bps/小时；
- 非 Crypto：+0.0625 bps/小时。

所以 short 在其他条件不变时有正 carry，但仍承担完整上涨风险。瞬时极端 Funding 可以作为：

- crowding/mean-reversion 信号；
- 做市 inventory skew 输入；
- 外部对冲后的 carry 策略候选。

它不是单腿套利。若使用 Binance/Hyperliquid/传统期货对冲，就引入跨所腿风险、资金转移、
不同 oracle、不同清算、额外合规和当前架构禁区。当前只采集和建模，不接外部执行。

session transition 也值得单独研究：股票停牌或周末时 Perps 仍继续撮合和清算。应研究
Mark-Index、本地 mid、外部基准和 Funding 在 Regular→Overnight→Weekend/Disrupted/Halted
切换时的系统性偏差，但第一版 live policy 应直接禁用非 Regular session。

### 7.6 商业副线：信号产品与 referral

Perps referral 官方政策是推荐人获得被推荐交易者所付交易费的 20%，每周支付；标准 code
最多 15 人，扩大额度需联系平台。参见
[Perps Referral Program](https://docs.polymarket.com/perps/referral-program)。

其收入上限可写成：

```text
MonthlyReferralRevenue
  = Σ(ReferredVolume × EffectiveFeesPaid × 20%)
```

例如 15 位用户每月各交易 $1M、实际平均付费 3 bps，理论 referral 收入约 $900/月。
这只是说明公式量级，不是预测；maker/taker 结构、volume tier 和活跃度会显著改变结果。

quant-pivot 的可审计报告可以进一步包装成订阅研究或风险看板，但这会引入数据再分发许可、
投顾/金融推广、隐私和面向客户的地域合规，不应与内部自营交易在未经审查时混合。

## 8. 单位经济性与盈亏平衡

### 8.1 Perps 费率

官方当前费率按 30 日交易量分层：

| 30 日量 | Taker | Maker |
|---:|---:|---:|
| $0 | 4.00 bps | 1.25 bps |
| $1M | 3.70 bps | 1.00 bps |
| $5M | 3.50 bps | 0.80 bps |
| $25M | 3.00 bps | 0.50 bps |
| $100M | 2.70 bps | 0.20 bps |
| $500M | 2.50 bps | 0 |
| $1B | 2.00 bps | -0.50 bps |

参见 [Perps Fees](https://docs.polymarket.com/perps/learn-about-trading/fees)。
Beta 的部分账户暂时按顶级费率收费，但官方明确这只是过渡政策。

不含点差、冲击与 Funding 的双边 round trip：

| 账户档位 | Maker→Maker | Maker→Taker | Taker→Taker |
|---|---:|---:|---:|
| 普通新账户 | 2.50 bps | 5.25 bps | 8.00 bps |
| 顶级费率 | -1.00 bps | 1.50 bps | 4.00 bps |

所以一条全 taker 的短线信号，即使完全没有滑点，普通账户也必须先赚超过 8 bps 才开始
覆盖手续费。实盘立项门槛应至少是：

```text
ConservativeGrossEdge
  > 2 × (fees + observed_spread + p95_impact + funding + hedge_cost)
```

且 bootstrap 后净收益下置信界仍需大于零。

### 8.2 Prediction CLOB 费率不能忽略

现有预测市场与 Perps 使用不同费率。Prediction CLOB 当前 taker 费公式是：

```text
fee = shares × feeRate × p × (1 - p)
```

Maker 不收费。Crypto `feeRate=0.07`，Finance `feeRate=0.04`。参见
[Prediction Market Fees](https://docs.polymarket.com/trading/fees)。

在 `p=0.50` 时：

- Crypto taker 每股费约 1.75¢，相当于 50¢ 买入金额的 3.5%；
- Finance taker 每股费约 1.00¢，相当于买入金额的 2.0%。

这意味着数字期权相对价值策略不能只看“概率差 2–3 个百分点”。它需要 maker execution、
更大的 wedge，或足够长的价差半衰期；2023 年论文的成本结论不能直接搬到 2026 年。

### 8.3 完整净收益

每笔策略统一按以下口径评估：

```text
NetPnL =
  GrossAlpha
  + FundingReceived
  + Rebates
  - PredictionFees
  - PerpsFees
  - BidAskSpread
  - MarketImpact
  - AdverseSelection
  - HedgeBasisError
  - LiquidationLoss
  - OperationalLossAllocation
```

容量只能由目标规模下的 `p95/p99` 深度和冲击曲线决定，不能由 24h Volume 决定。

## 9. 推荐技术方案

### 9.1 Phase A：只读 Perps Data Plane

不要把 Perps 塞进现有 binary `BookStore<TokenKey>`。建议建立同平台、非 generic venue 的
专用边界：

- `PolymarketPerpsPublicClient`
- `PerpsInstrumentId` / `PerpsInstrumentKey`
- `PerpsBookStore`
- `PerpsTickerFact`
- `PerpsBookL2Ledger`
- `PerpsTradeFact`
- `PerpsFundingFact`
- `PerpsSessionFact`

实现要求：

1. 使用 decimal/newtype，禁止用 `f64` 表示价格、pUSD、数量或 Funding；
2. 记录 `effective_at`、`available_at`、ingestion time 和 source sequence；
3. WebSocket `sq` 不连续即 invalidate session，以 REST Book 重建后再发布；
4. 定期拉取并版本化 instruments、risk tiers、fee schedule、liquidation fee；
5. 用 `/v1/info/time` 校验时钟；
6. schema 漂移、未知标的、session disrupted/halted 均 fail closed；
7. 第一阶段不创建 proxy、不存私钥、不调用任何 trade/account endpoint。

官方提供 43 个 HTTP path、完整 OpenAPI，以及 Book/BBO/Trade/Ticker/Statistics/Kline/Funding
等 WebSocket channel。参见
[Perps API Overview](https://docs.polymarket.com/api-reference/perps/overview)、
[OpenAPI](https://docs.polymarket.com/api-spec/perps-openapi.json) 和
[Perps Book WebSocket](https://docs.polymarket.com/api-reference/wss/perps-book)。

### 9.2 Phase B：跨产品 Linkage 与 Shadow Model

建立独立、不可变、可审计的两类 linkage：

1. `PriceContractLinkage`：预测合约 strike/reference/observation/oracle ↔ Perps instrument；
2. `EventExposureLinkage`：事件概率 ↔ 受影响资产、方向、horizon、置信度。

所有在线和离线 feature builder 必须共享同一 linkage revision；历史回放不得使用后来修订的
映射。模型至少分开：

- `perps_enhanced_prediction_ranker`
- `digital_relative_value_model`
- `event_to_perps_return_model`

不要把三类标签混成一个 score。

### 9.3 Phase C：独立 Perps 执行边界

只有 Shadow 通过后才实现：

- `PerpsOrderIntent`
- `PerpsExecutionAccount`
- `PerpsPosition`
- `PerpsMarginSnapshot`
- `PerpsFundingLedger`
- `PerpsExecutionOrder`
- `PerpsReconciliation`

必须具备：

- proxy signer 与 secret 的密钥隔离和到期轮换；
- 每笔签名的 timestamp/salt 与 server-time 校验；
- post-only、reduce-only、client order id、幂等恢复；
- private orders/fills/portfolio/funding/balance 多源对账；
- dead-man switch、cancel-all、断线撤单；
- fee tier、IM/MM、liquidation distance 和 session admission；
- 只允许 isolated，第一版禁止 cross；
- `ReportOnly -> SemiAuto`，不直接进入 `AutoExecution`。

官方 proxy 凭证默认一周到期，且 Perps account 由 underlying EOA 标识，不等同于现有 Safe/CLOB
账户语义。参见
[Authenticated Sessions](https://docs.polymarket.com/perps/authenticated-sessions) 和
[Account Management](https://docs.polymarket.com/perps/account-management)。

## 10. 90 天验证路线

| 时间 | 工作 | 退出物 | Go/No-Go |
|---|---|---|---|
| Day 0–14 | 合规、early access、API contract spike | 法律意见、账户费率、API/ToS 清单 | 任一不通过则只保留公开数据研究 |
| Day 15–45 | 只读 ingest，覆盖工作日/周末/session 切换 | 30 天 L2/Trade/Ticker/Funding/PIT 数据 | 未恢复 sequence gap、覆盖不足则 No-Go |
| Day 46–70 | A/B/C 三类 shadow 研究 | OOS 回测、成本/容量报告、模型卡 | 净 edge 下界≤0 或仅单一标的有效则 No-Go |
| Day 71–90 | 故障演练；条件允许时 isolated 小额 `SemiAuto` | fill、撤单、对账、Funding、PnL 实证 | 任一账本不一致或 breaker 失效即停止 |

### 10.1 数据门槛

- 连续至少 30 天，覆盖不少于四个周末和主要 session transition；
- WebSocket sequence gap 均有明确 invalidate + REST recovery 证据；
- 目标标的可用时间覆盖 ≥99.9%，但不能用 forward-fill 伪造；
- instruments、fee、risk tier、liquidation fee 变更可回放；
- 所有特征严格使用 decision boundary 前可见数据；
- 公共 API 与 WebSocket 的时钟偏差、重连尾延迟和重复事件均有分布报告。

### 10.2 Alpha 门槛

- 微观结构策略至少 1,000 个可执行 shadow opportunity；
- 事件策略按独立事件簇计样本，不按 tick 虚增样本；
- 必须有 walk-forward、purge/embargo、multiple-testing correction；
- 扣除实际费率、observed spread、p95 impact、Funding 和 hedge error 后为正；
- 在 2 倍费用和 2 倍 p95 slippage stress 下仍不为负；
- OOS 净收益下置信界大于零，且结果不由单一标的/单一周贡献；
- 策略容量至少为计划 pilot notional 的 5 倍。

### 10.3 操作门槛

- 重启后 orders/fills/portfolio/funding/balance 完全对账；
- proxy 过期、时钟漂移、429、WS gap、Disrupted/Halted、余额读取失败都 fail closed；
- dead-man switch、cancel-all、reduce-only 和 liquidation-distance breaker 经过故障注入；
- 任何 ambiguous fill 都禁止新开仓；
- 费率不能假设，必须从实际账户读取并冻结到决策证据。

### 10.4 条件式 pilot 风险边界

以下仅是研究环境的保守示例，不是个性化仓位建议：

- 单独的 Perps 研究资金池；
- isolated margin，configured leverage 从 1x 开始，首阶段不超过 2x；
- 禁止 cross、禁止 20x、禁止非 Regular session 的股票/商品；
- 单标的最大初始保证金不超过 pilot equity 的 2%，总 IM 不超过 10%；
- 日内净亏损达到 pilot equity 的 0.5% 自动停止，累计回撤 2% 转回 `ReportOnly`；
- Funding、手续费、未实现 PnL 和 withdrawable balance 都进入实时风险预算；
- 先 `SemiAuto` 人工批准，至少 30 个无对账异常的交易日后才讨论自动化。

## 11. 风险矩阵

| 风险 | 为什么重要 | 控制 |
|---|---|---|
| 市场/杠杆 | 20x 时很小反向波动即可接近清算 | 1x isolated 起步、内部 margin buffer |
| 流动性 | 清算 IOC 无保护价，Volume 不等于深度 | p95/p99 impact、容量 cap |
| Funding | 每小时结算，可迅速改变持仓成本 | Funding budget、跨小时持仓 admission |
| Oracle/basis | prediction 结算源与 Perps Index 不同 | 显式 source linkage 与 terminal basis stress |
| Session/停牌 | 底层停牌但 Perps 继续撮合和清算 | 首期只 Regular；Halted/Disrupted 禁开仓 |
| Mark 模型 | Mark 决定权益和清算，不是最后成交价 | 保存 C1/C2/C3 可观测 proxy、外部源交叉验证 |
| API/Beta | 产品快速变更、费率优惠可能消失 | contract snapshot、changelog monitor、fail closed |
| 链下账本/托管 | 大部分交易状态在链下，仅周期性 state root | 小资金池、多源对账、提现演练 |
| 凭证 | Perps proxy 可直接授权交易 | 短 TTL、独立密钥、最小资金、轮换 |
| 模型过拟合 | 标的少、历史短、事件相关 | CPCV、事件簇、OOS、multiple-testing correction |
| 操纵/自成交 | 早期薄市场和短周期合约更脆弱 | 排除 5m、authoritative trade side、异常过滤 |
| 合规 | 衍生品、加密抵押、跨境服务均高度敏感 | 书面法律意见、geo block、禁止 VPN 绕过 |

## 12. 合规结论

平台自己的 Perps 限制名单目前包括美国、加拿大、古巴、伊朗、朝鲜、叙利亚以及 Crimea、
Donetsk、Luhansk；Builder 必须在任何程序化开仓前验证最终用户位置，read-only 数据不受该
下单限制。参见
[Perps Geographic Restrictions](https://docs.polymarket.com/api-reference/perps/geographic-restrictions)。
国际站同时明确其不受 CFTC 监管，Polymarket US 是独立的 CFTC-regulated DCM。

**平台未 geoblock 某地不代表当地法律允许。**

如果实际运营主体、开发团队、资金或用户位于中国大陆，2021 年十部门通知明确把虚拟货币
衍生品交易列为非法金融活动，并把境外交易所向境内居民提供服务、以及相关营销、支付结算和
技术支持列入责任范围。参见
[银发〔2021〕237号](https://www.pbc.gov.cn/tiaofasi/144941/3581332/4348658/index.html)。

因此：

- 中国大陆场景下，在取得熟悉衍生品、虚拟资产和跨境服务的律师书面意见前，
  **No-Go live trading、代客执行、referral 营销和对外技术服务**；
- 美国或加拿大物理位置下，按官方 Perps 条款 **No-Go order placement**；
- 不得使用 VPN 或其他方式绕过平台地域限制；
- 若未来加入外部对冲 venue，需要对每条执行腿重新做地域、KYC/KYB、税务和牌照审查。

## 13. 最终建议

### 应该做

批准一个边界清晰的 **“Polymarket Perps Read-Only Alpha Discovery”** 项目：

1. 不改当前 binary execution；
2. 不投入交易资本；
3. 先采集 30 天 Perps 全量公开市场数据；
4. 优先交付 `perps_enhanced_prediction_ranker` 和数字期权 relative-value shadow report；
5. 用明确成本和容量门槛决定是否继续。

以当前系统基础估计，只读生产级 adapter、事实层、PIT/replay、linkage 与首批特征需要约
4–6 个资深工程周；完整 shadow 研究再需要约 4–8 个量化/工程周。若数据否定 alpha，
应在这一步止损，不建设执行面。若通过，独立 Perps 账户、执行、保证金、Funding、对账和
故障演练还需额外约 8–14 个工程周。

### 不应该做

- 不因“最高 20x”把杠杆当成收益来源；
- 不把单小时 Funding 机械年化；
- 不把 24h Volume 当成策略容量；
- 不依赖 Beta 临时顶级费率；
- 不把现有 Yes/No `OrderIntent` 扭曲成 Perps DTO；
- 不为了 Funding 套利偷偷引入跨所执行；
- 不在法律适用主体不清楚时启动 live 或对外 referral。

**最终判断：这是一个真实但需要证伪式推进的风口。**  
Polymarket 的平台化扩张为 quant-pivot 创造了少见的同品牌跨产品 alpha surface；我们的优势
在事件语义、PIT 数据、可审计模型和执行治理，而不在比现有 Perp 做市商更快。先买数据期权，
再决定是否买交易风险，是当前最高预期价值的路径。

## 14. 主要资料

### 官方一手资料

- [Polymarket Perps Overview](https://docs.polymarket.com/perps/overview)
- [Perps Fees](https://docs.polymarket.com/perps/learn-about-trading/fees)
- [Perps Funding](https://docs.polymarket.com/perps/learn-about-trading/funding)
- [Perps Margin](https://docs.polymarket.com/perps/learn-about-trading/margin)
- [Perps Liquidation Mechanics](https://docs.polymarket.com/perps/learn-about-trading/liquidation-mechanics)
- [Perps Mark Price](https://docs.polymarket.com/perps/learn-about-trading/mark-price)
- [Perps Index Price](https://docs.polymarket.com/perps/learn-about-trading/index-price)
- [Perps Market Sessions](https://docs.polymarket.com/perps/learn-about-trading/market-sessions)
- [Perps API Overview](https://docs.polymarket.com/api-reference/perps/overview)
- [Perps Rate Limits](https://docs.polymarket.com/api-reference/perps/rate-limits)
- [Perps OpenAPI](https://docs.polymarket.com/api-spec/perps-openapi.json)
- [Perps Changelog](https://docs.polymarket.com/changelog/perps)
- [pUSD](https://docs.polymarket.com/concepts/pusd)
- [ICE Strategic Investment in Polymarket](https://ir.theice.com/press/news-details/2025/ICE-Announces-Strategic-Investment-in-Polymarket/default.aspx)
- [中国人民银行等十部门：银发〔2021〕237号](https://www.pbc.gov.cn/tiaofasi/144941/3581332/4348658/index.html)

### 行业与学术资料

- [CoinGecko — State of Crypto Perpetuals Report 2026](https://www.coingecko.com/research/publications/state-of-crypto-perpetuals-report-2026)
- [Do Prediction Markets Match Option Prices?](https://arxiv.org/abs/2606.19517)
- [The Anatomy of a Decentralized Prediction Market](https://arxiv.org/abs/2604.24366)
- [Settlement Manipulation in Prediction Markets](https://arxiv.org/abs/2606.31675)
- [Unravelling the Probabilistic Forest](https://arxiv.org/abs/2508.03474)
- [Perpetual Futures Pricing](https://arxiv.org/abs/2310.11771)
- [Funding-Aware Optimal Market Making for Perpetual DEXs](https://arxiv.org/abs/2605.06405)
