# quant-pivot 全局组合、Runtime/Deploy Config 与 UI/UX 完整闭环实施计划

> 状态：`IMPLEMENTATION_IN_PROGRESS`
>
> 兼容策略：clean break。禁止旧 parser、字段别名、deprecated API、forwarding wrapper、
> compatibility re-export、双读、双写或 solver fallback。
>
> 项目状态约束：系统从未投入生产运营，因此本计划不设计版本升级、历史 payload 转换、旧 schema
> 归档读取或线上迁移窗口。唯一目标是 clean-install 最终 schema/contract；任何 disposable 验证库均从空库
> 安装，真实数据库若需 reset 必须由操作者另行明确授权。
>
> 计划落点说明：用户消息中的 `dpcs` 按仓库既有结构解释为 `docs`；不创建拼写错误的目录。

## 1. 完成定义

本计划只有在以下条件全部有可复核证据时才可标记完成：

1. 报告不再绑定单一分类或 `BuyModelRoute`；每个 represented Route 独立完成模型、校准和
   Trade Policy 推理，统一转换为可执行、贴现后的 USD 场景现金流，再进入一个全局组合优化器。
2. 任一 represented Route 缺少兼容 Champion、Calibration、Trade Policy、Research Profile、
   `PortfolioScenarioModelArtifact`，或无法从该模型在本次决策冻结点生成 concrete
   `PortfolioScenarioArtifact` 时，整份报告 fail closed；完整 Route 的 zero-candidate 不是错误。
3. 全局优化只使用一个 production MILP 路径；不存在 score/confidence 加权、per-candidate Kelly、
   Pearson/category proxy、LP relaxation、空 plan 或 solver fallback。
4. 六类 Runtime Config clean-install contract 的 Rust 类型、数据库、API schema、generated TypeScript、domain editor、
   validation pointer、consumer 和 apply boundary 双向对齐。
5. 每个 `DeployConfig` leaf 均有唯一 descriptor、完整英文注释、严格解析、consumer、安全投影和
   两份 TOML 覆盖；production 不依赖默认值、overlay 或环境变量替换。
6. mixed-route 报告和 Config 治理的真实 production-stack Playwright visual assertions 在固定矩阵
   连续两次通过，并同时通过 axe、keyboard、overflow 与 browser-failure hard gate。
7. 同一真实 production stack 必须自然运行完整的 15-stage `feedback_closure`，从冻结 outcome truth
   到产生受治理 challenger、校准/CPCV/quality evidence、shadow、`CandidateReady`，再经独立 permit 与
   activation 生成新的 serving generation、兼容 scenario-model binding 和后续 mixed-route report；禁止预置
   terminal stage、伪造 stage event、直接写 CandidateReady、HTTP interception 或跳过任一生产 adapter。
8. Rust、架构、配置生成、数据库、UI、E2E、反馈闭环和性能门禁全部通过；不得以“实现完成”替代证据。

## 2. 当前审计结论

### 2.1 Runtime Config ↔ UI

当前六类资源为 `RecommendationPolicy`、`ExecutionRiskPolicy`、`ModelRouting`、
`ReportSchedule`、`OperationalControl`、`ExecutionAuthorization`。治理后端已具备 revision、
validate/preflight、approval、CAS activation、audit/outbox、ArcSwap publication 和 rollback，
但展示层尚未形成严格闭环：

- 当前 schema 表单核心是递归 `policy-field.vue`，而不是六个领域编辑器。
- 当前审计有 117 个可编辑叶子，按资源分布为 24 / 44 / 26 / 5 / 13 / 5；实施期由生成式
  inventory 重新计算并作为唯一精确数字。
- JSON Schema 缺少 unit、control、group/order、risk、真实 apply effect、visibility、docs link 等
  字段级 metadata。
- i18n 仍以 leaf name 为主，无法可靠区分不同资源中的同名字段。
- server-side validation 没有统一 RFC 6901 pointer，导致 summary 与 inline control 无法稳定对应。
- read-only/current 值仍可能以 disabled input 呈现；`restart_required` 等展示不是来源于真实消费者。
- live runtime control 与 revisioned policy 同页但语义隔离不足。

因此当前结论是 **不严格对齐**。目标状态以 descriptor pointer 集合与页面实际
`data-config-pointer` 集合相等作为机器验收，而不是人工声称覆盖。

### 2.2 Deploy Config ↔ TOML

当前 `DeployConfig` 根包含 `deployment`、`polymarket`、`market_data`、`domain_sources`、
`observability`、`notifications`、`db`、`cache`、`keys`、`web`、`quant`、`research`。
源类型精确审计为 61 个 config struct、310 个 leaf/path；dynamic map 使用 `.*`、数组元素使用
`[]` 表示。完整 as-is 路径、类型、owner 和敏感性基线见
[`quant-pivot-current-config-field-inventory.md`](../audit/quant-pivot-current-config-field-inventory.md)。
目标 descriptor registry 必须从该清单逐项迁移、删除或替换，禁止静默遗漏。

主要断点：

- `DeployConfig::load` 当前接收目录，base file 与 local overlay 均 optional，并通过 struct-level
  `serde(default)` 补齐缺失值。
- `config/quant-pivot.toml` 与 production example 的字段覆盖不一致；optional/union/dynamic binding
  没有统一的“列出但不激活”表示。
- 两份 TOML 注释质量不均，尚未强制 Purpose、Required、Type/Unit、Constraints、Impact、Restart、
  Sensitivity 和 cross-field rules 完整出现。
- loader 未把 absolute single-file、no-follow、regular-file、owner、mode 和 placeholder rejection
  作为一个 race-safe boot contract。
- Deployment API 不是从同一 descriptor 穷尽生成，无法证明所有字段都被安全投影且敏感 URL 不泄漏。

因此当前结论同样是 **不严格对齐**。

### 2.3 报告与组合算法

当前 `SelectionConfig -> BuyModelRoute` 强制一轮报告只解析出一个 Route；report builder 冻结一个
`ActiveModelRequirements`、一个 calibration、一个 Trade Policy，并只运行一次 feature/model/portfolio
链路。当前 allocator 使用 composite score/confidence/expected return 权重，且允许 MILP → relaxation →
空 plan 降级。历史中价 Pearson 与 event/category proxy 不能表达预测市场联合支付结构。

该实现不能可靠比较不同 Route 的赚钱可能性，也会把求解器或 artifact 故障伪装为正常报告。

## 3. 研究依据与设计拍板

- Prediction-market price 不自动等于真实概率；必须使用 Route-specific calibrated probability 与
  不确定性证据，而不是直接使用盘口或 raw score：Manski、Wolfers & Zitzewitz。
- Route 内校准使用 strictly proper scoring rules、reliability evidence 和多群体校准约束；不同模型
  输出只有转换为统一场景现金流后才可比较：Gneiting & Raftery、Multicalibration。
- 跨 Route 组合采用 robust scenario optimization 与 CVaR hard constraints；不把概率、confidence、
  score 和金额任意相加：Boyd robust portfolio / robust Kelly、Rockafellar–Uryasev CVaR。
- 多目标 MILP 采用逐级 ε-constraint lexicographic solve：最大化阶段只锁定前一目标的下界，最小化
  CVaR/capital 阶段只锁定上界，最终仍以 Decimal/newtype 对每个已锁目标和硬约束做精确等式复核。CVaR
  的 `VaR + weighted excess` 是 epigraph 表达，若把辅助变量错误锁成等式会人为制造不可行；HiGHS 官方
  numerical guide 同时要求控制系数量级并在求解后独立复核原始尺度约束。实现禁止通过放宽风险 cap、
  接受 feasible/non-optimal 或 LP fallback 处理数值问题。
- L2 容量必须是决策外生证据：买入 entry walk 消耗 candidate token 的 asks，sell-to-close 消耗同 token
  的 bids；scenario 只能对冻结的 bid-side shares 施加流动性冲击，禁止用“拟议 tier shares × stress”
  反向定义容量。Polymarket 的 CLOB 文档明确区分 buy-at-ask 与 sell-at-bid，并要求大额交易检查 depth；
  Almgren–Chriss 的 liquidity-adjusted execution risk 与 Boyd 的 liquidation-loss constraint 同样把清算成本/
  可清算性作为独立的组合约束。实现因此使用 `existing shares + proposed shares <= stressed bid capacity`
  的逐 scenario 准入合同：精确 leg 缺失仍令 artifact/report 失败，leg 完整但某 tier 超容量只拒绝该 tier，
  不把正常的可行域排除误报成 scenario artifact 损坏。
- Stateful replay 的未结算仓位必须逐 decision tick 用当时可执行 exit price 重估，而不能继续按 entry
  notional 记账。具体规则是：用同 token 的冻结 PIT bids 对全部 shares 做 sell-side L2 walk，扣除 exit fee，
  超出可执行深度的 residual shares 按零清算值计入保守 mark；缺少 exact PIT bid snapshot、时钟/market/token
  不一致或金额精度越界均 fail closed。这样 equity/drawdown 同时反映 spread、depth 与 market impact，而
  entry principal 仍保持锁定到真实 policy exit/settlement。依据是 Polymarket 官方
  [Prices & Orderbook](https://docs.polymarket.com/concepts/prices-orderbook) / [Orderbook API](https://docs.polymarket.com/trading/orderbook)
  对 buy-at-ask、sell-at-bid 和 depth 的定义，[SEC transaction-cost study](https://www.sec.gov/rules-regulations/2003/12/request-comments-measures-improve-disclosure-mutual-fund-transaction-costs)
  对 spread/market-impact cost 的定义，以及 FASB Topic 820 对 entry price 与可出售 exit price 的明确区分。
- Entry execution plane 与 liquidation mark plane 必须是两个强类型、不同 population 的冻结对象：前者只覆盖
  当前模型 cross-section 中可能买入的 token，并需要 asks/limit；后者覆盖此前任一 tick 可能建立且在当前 tick
  尚未结算的全部 token，只需要精确 bids/PIT fee。市场轮换不能让未结持仓从估值面消失。mark plane 在任何
  模型推断和 allocation 之前，由 Source Slice 对每个 retention token 的全部后续 `DecisionBoundary` 批量重放
  canonical L2 session/event chain；只有“最后一个已验证 snapshot + 所有可见 intervening events”重建出的
  contemporaneous book 才是精确 PIT state，绝不把 last observation carry-forward 当作缺数 fallback。空 bid book
  仅在该精确重放结果真实为空时计零；缺 snapshot/session/sequence/fee schedule 直接令 Comparison 失败。
  这与 Polymarket sell-at-bid 语义、[IFRS 13](https://www.ifrs.org/issued-standards/list-of-standards/ifrs-13-fair-value-measurement/)
  的 measurement-date exit price，以及 Bion-Nadal 的
  [dynamic bid-ask pricing](https://arxiv.org/abs/math/0703074) 和 Ackermann–Kruse–Urusov 的
  [stochastic LOB liquidity](https://arxiv.org/abs/2006.05843) 一致。
- 时间依赖残差必须按联合向量做 block/stationary bootstrap，而不是逐 Route 独立抽样；stationary
  bootstrap 为弱依赖平稳序列提供重采样基础，Wasserstein DRO 为有限样本 ambiguity set 提供可验证的
  worst-case distribution 语义：Politis–Romano、Mohajerin Esfahani–Kuhn。
- 统计场景的时间离散与逐时段资本约束必须是两个正交合同：`time_bucket_secs + resampling_method`
  封存残差聚合/重采样方法，`capital_time_bucket_contract_digest` 只封存严格递增的资本/贴现时间边界；每桶
  USD cap 由报告开始时冻结的 `ExecutionRiskPolicy` 直接进入优化约束，单纯修改 cap 不要求重拟合统计模型，
  但修改边界必须与 scenario-model binding 原子切换。该分离与 [Boyd et al. 的 multi-period trading](https://stanford.edu/~boyd/papers/cvx_portfolio.html)
  将未来随机量预测和每期 constraint/cost 分开建模、以及 [Skaf–Boyd](https://web.stanford.edu/~boyd/papers/pdf/dyn_port_opt.pdf)
  对逐期约束集 `C_t` 的定义一致；禁止再用一个 digest 同时表示 resampling horizon 与 capital grid。
- Feedback 必须把自然分布漂移、系统决策造成的 performative shift、selection mechanism 与已实现交易结果
  分开封存和评估；没有 behavior propensity、overlap/support 与 sensitivity evidence 时，不得声称 IPW、DR-OPE
  或策略反事实具有因果识别能力：Perdomo et al.、Jiang–Li、Kallus et al.。
- Trade Policy 是会改变标签和可执行现金流的 learned nuisance/policy，不得用同一时间样本拟合后再回填该样本。
  Promotion authority 只接受 prospective policy cohort：一个 Published Trade Policy 的全部拟合证据必须在 cohort
  `window_start` 之前可用，并经过完整 embargo；随后冻结策略、完整 scored population、实际 behavior mechanism 与
  outcome truth。这样得到的 feedback evidence 才是当前生产策略的真正 OOS 证据，而不是未来策略回看历史的结果。
- Nested/cross-fitting 仍用于离线研究：每个外层 purged temporal path 内必须重新执行模型/校准/策略选择，nuisance
  estimate 只能作用于其未参与拟合的 fold。Cawley–Talbot 要求把完整 model-selection procedure 当作被评估对象；
  Chernozhukov et al. 说明 sample splitting/cross-fitting 可去除 plug-in overfit bias；Kallus–Uehara 的 cross-fold OPE
  只在相应识别假设成立时提供估计依据。它们不能替代 prospective promotion cohort，也不能在缺少 overlap、logging
  propensity 或时间依赖隔离时被解释为可上线收益。
- Buy CPCV 的每条外层 path 采用严格的四段 estimator nesting：
  `model_fit -> calibration_fit -> scenario_fit -> outer_test`。`model_fit` 只能使用更早且相对完整 estimator
  holdout 经过二次 purge/embargo 的 groups；概率映射与 split-payout rate 只在 `calibration_fit` 拟合；reliability、
  proper-score/downside 证据及 fold-local payout residual scenario model 只在更晚且相对 `calibration_fit` 再次
  purge/embargo 的 `scenario_fit` 估计；`outer_test` 不得参与前三者。四个 population 必须非空、互斥且由独立
  content hash 绑定，任何退化都 fail closed。estimator holdout 比例与初始最小 group 数是受治理 Runtime 字段；
  raw floor 至少为 4，但 `2 + 2` 只是原始容量下界，绝不是 overlapping-label purge 后仍保留 `2 + 2` 的证明。
  每个 outer fold 必须从该比例/下限开始，按真实 label interval 只扩展到最小可行的 chronological suffix，穷举其中
  所有 calibration/scenario 时间边界并再次 purge；先最大化两者 post-purge 最小有效 group 数，再最小化不平衡，
  并列时保留更多 scenario residual groups。返回首个可行 suffix 以最大化 model-fit 历史；如果在为 model-fit 保留
  两个 groups 的前提下仍无可行边界，整 fold/report 失败。该过程是唯一算法，不是数据不足后的 fallback，也不得
  通过放宽 purge/embargo 或降低 population floor 重试。model/calibration/scenario 三个拟合 population 最终必须分别
  保留至少两个 groups。CPCV partition 数不等于 decision-time group 数，数据集必须用真实 horizon/embargo 拓扑另行
  证明每个 `C(N,k)` fold 都满足容量约束。Varma–Simon 要求完整调参与估计过程位于外层循环内；scikit-learn 的
  calibration contract 要求 calibrator 与 classifier fit data 隔离；其 `TimeSeriesSplit.gap` 也说明时间相关样本需要
  显式间隔，而不能把固定行数当作独立性证明。Niculescu-Mizil–Caruana 的结果同时说明 isotonic 在小
  calibration set 上更易过拟合，因此实现按样本量选择受约束 calibrator，并保存独立 validation reliability/
  proper-score 证据，不以 rank/AUC 代替概率质量。
- 联合 scenario model 只拟合 `realized canonical payout - fold-calibrated expected payout` 的按 decision-time 对齐向量；
  禁止使用 optimizer return、selected-recommendation PnL 或事后 allocation 作为 scenario residual，否则会把策略选择反馈
  注入不确定性模型。每条 residual 对底层模型是 OOS，最终 promotion artifact 只由完整 outer OOS residual paths 重拟合；
  block/stationary bootstrap 保留时间依赖与同一时刻的跨 Route 共变。Kaut–Wallace 要求 scenario generation 以其下游
  stochastic program 的 in-/out-of-sample stability 评价，Wang et al. 也以 backtest predictive residual 构造 distribution
  forecast；因此 report MILP 的解稳定性、coverage 与 held-out objective stability 都是 promotion hard gate。
- 模型风险治理采用当前 Federal Reserve SR 26-2 的风险分级原则与 NIST AI RMF 的 GOVERN/MAP/MEASURE/MANAGE
  闭环作为外部交叉检查；多重试验选择偏差必须显式报告 PBO/DSR，不以单次 Sharpe 或一次回测胜出作为上线证据。
- Serving model 与 feedback Evaluation 必须保留两条不可混同的 policy lineage：模型 artifact/serving contract
  永久提交其训练、校准和封存时的 build-time `DecisionPolicySnapshot`；每个 prospective Evaluation Dataset、
  Source Slice、Comparison job 与 feedback cycle 则提交产生该观察窗口的 decision-time policy。两者身份可以在
  Route/scenario 原子 activation 后不同，但只允许身份分离，不允许语义放宽：model/spec/profile、feature schema、
  factor plane、PIT reader/source schema、model input transform、calibration、Trade Policy、Route 与 scenario contract
  必须逐维兼容。Federal Reserve [SR 11-7](https://www.federalreserve.gov/supervisionreg/srletters/sr1107a1.pdf)
  要求 conceptual soundness、ongoing monitoring/process verification 与 outcomes analysis，并把实际使用环境变化纳入
  持续验证；[NIST AI RMF Core](https://airc.nist.gov/airmf-resources/airmf/5-sec-core/) 同样要求评估连接真实部署情境。
  若 Evaluation rows 来自不同 logging/behavior policy，则该问题不再是普通 Comparison replay：必须具备显式 OPE
  estimator、overlap/support、propensity 与 sensitivity evidence；否则按
  [Wang et al.](https://proceedings.mlr.press/v70/wang17a.html)、
  [Kuzborskij et al.](https://proceedings.mlr.press/v130/kuzborskij21a.html) 和
  [Saito et al.](https://proceedings.mlr.press/v162/saito22a.html) 所讨论的 logging/target-policy 偏差风险 fail closed。
  本系统当前 prospective cohort 的每行必须与其 decision-time policy lineage 精确一致，因此不以 OPE 作为隐式
  fallback，也不把 build-time policy 强行伪装成当前 policy。
- PBO 严格使用 Bailey–Borwein–López de Prado–Zhu Algorithm 2.3 所要求的同步 `T × N` trial matrix、等长
  contiguous blocks 和全部 `C(S,S/2)` IS/OOS 组合；每个 trial column 必须来自同一冻结时间轴和同一个预提交
  complete OOS CPCV path。DSR 的 `N` 与 Sharpe variance 必须来自同一受治理 trial ledger；训练折内优化轨迹不冒充
  独立 backtest trial，未进入 ledger 的人工试验则是治理违约而不是统计公式可以补救的输入。全部预注册配置及其
  不利/no-trade 结果必须永久保留；但在 PBO 排名和 DSR multiple-testing statistic 之前，按完整 OOS Decimal return
  column 的精确相等关系建立行为等价类。重复参数配置只是同一随机变量的冗余表示，不能因 grid 参数化密度而增加
  选择机会、改变 PBO 中位 rank 或重复加权 Sharpe variance。每类以最小 raw trial ID 为 canonical representative，
  member ledger、raw block sufficient statistics 和全部 raw pair cross-products 使该 partition 可独立重算。
- 若所有非冗余 representative 均有正方差，则只在 representatives 上计算
  `N = ceil(ρ̄ + (1-ρ̄)B)`，其中 `B` 是行为等价类数，不是 raw grid cardinality。若只有一个行为类，或代表类中存在
  真正 no-trade 的零收益/零方差列，Pearson matrix 不可识别：不得伪造 `corr=0`，而是直接使用完整行为类数 `B`
  作为保守 DSR count，并显式记录 zero-variance representative。这样仍完整惩罚每种不同经济行为，却不会把同一
  no-trade 策略重复八次当成八个独立研究发现。DSR 的 Sharpe variance 也只在 representatives 上计算。非零常量
  收益在 CSCV 子样本上没有可估 Sharpe，方法学 fail closed；`V=0` 严格保留为 0，不注入 epsilon floor。该口径来自
  DSR Appendix A.3 对 independent/non-redundant source 的明确要求及其“raw `M` 会高估 expected maximum”的警告；
  PBO 同样在非冗余可执行策略 population 上排名，但原始预注册 trial 始终留在审计账本中。

### 3.1 概率模型、Trade Policy 与 feedback target 的不可混淆边界

Buy Route 的 supervised target 拍板为 **canonical outcome token 的 terminal payout ratio**，不是
`policy_net_return_bps`、成交结果或任意策略收益。其原因不是为了让测试通过，而是以下金融语义必须同时成立：

1. Buy model 的产物是可由 proper scoring rule 检验、可校准且可在不同 Route 之间解释为同一事件语义的
   `P(token pays 1 | PIT information)`；下游才可以按 Bayes decision/expected utility 把该概率与价格、fee、
   L2 fill、退出规则和资本成本组合。Gneiting–Raftery 的 strictly proper scoring 结论和 Gao et al. 对
   calibrated conditional probability 支持 cost-sensitive Bayes decision 的结果都依赖预测值与实际事件同义。
2. 如果同一 score 直接训练为某个 Trade Policy 的净收益，再把该 score 校准成 `P(win)`，价格、成交和退出策略
   已经进入 label；报告阶段再次把 `P(win)` 与价格、L2 和 Trade Policy 组合会形成语义性 double counting，且策略
   替换会使所谓“概率”失去稳定含义。该 target 不能作为跨 Route 的统一概率尺度。
3. Trade Policy 是独立的可执行决策对象。它用完整的 frozen scored population、真实 L2 walk、PIT fee、FAK/FOK/
   passive fill、退出/结算和资本占用生成 policy cashflow evidence；该证据进入 CPCV、Comparison、scenario artifact
   和 global MILP，但不回写 Buy probability target。真实成交 feedback 只训练 execution/slippage/fill policy，
   不能因 decision selection censoring 回流成 outcome forecaster 的标签。
4. `policy_net_return_bps`、`policy_net_positive`、entry/exit fill ratio 仍是 Trade Policy/Execution Policy 研究标签；
   它们不再是 Buy `ModelTrainingContract` 可选择的 supervised target。若使用 logged behavior 做反事实策略比较，
   必须另行具备 propensity、overlap/support、cross-fitting 和 sensitivity evidence；不满足时只能称为 replay/
   association evidence，不能称为 unbiased OPE。

Polymarket 还允许罕见的 `Unknown/50-50` resolution：两个 token 均兑换 0.5。因此 runtime 的精确定义是
**calibrated expected payout ratio**；只有在 payout 支持集为 `{0, 1}` 时它才等于 `P(win)`。50/50 概率必须作为
scenario model 的显式第三 settlement state 进入 `profit_probability` 和 tail-risk 计算，禁止把均值反推成一个
Bernoulli 分布。该区别不影响 `E[cashflow]`，但会影响 `P(net > 0)`、VaR/CVaR 与 stress loss。

因此采用 clean-break 类型契约：`ModelTrainingContract` 不再接受自由字符串 target，而使用闭集
`ModelTrainingTarget`；`OutcomePayout` 绑定 `token_payout_ratio/0`，`HoldVsExitAlpha` 绑定
`hold_vs_exit_alpha_bps/0`；另保留显式 `ForwardReturn { horizon_secs }` 供离线回归研究，但它没有 payout
distribution，永远不能成为 production Buy Route Champion。所有 target 与 `ModelFamily` 做穷尽兼容校验。
原 `trade_policy_artifact_id` 重命名为
`evaluation_trade_policy_artifact_id`，明确它是后续可执行评估/Route readiness 的冻结 binding，而不是 target
生成器。Dataset manifest 可以继续封存解析后的 Trade Policy identity/hash，以证明整个 evaluation cohort 使用
同一 Published policy；它不得改变 outcome label。任何未知 target、Buy family + sell target、Sell family + payout
target、或 production Buy Route 缺 evaluation policy 均 fail closed。

Feedback 的 `ModelScoreLearning` cohort 必须覆盖 decision deadband、TopN 和 portfolio selection **之前**的完整
calibration/rank-score population，并只附 canonical payout truth；否则只观察已推荐/已成交样本会造成选择偏差。
同一 prospective cohort 上的 frozen Trade Policy replay 形成独立 `PolicyEvaluationArtifact`。`DatasetSeal` 只有在
target label coverage 与 policy-evaluation coverage 各自通过后才可进入 Training；两者不得用一个字段或一个
`status = Ready` 相互代替。

参考：

- https://www.nber.org/papers/w12200
- https://www.nber.org/papers/w10359
- https://stat.uw.edu/research/tech-reports/strictly-proper-scoring-rules-prediction-and-estimation-revised
- https://proceedings.mlr.press/v80/hebert-johnson18a.html
- https://stanford.edu/~boyd/papers/cvx_portfolio.html
- https://web.stanford.edu/~boyd/papers/pdf/cvx_portfolio.pdf
- https://doi.org/10.21314/JOR.2001.041
- https://docs.polymarket.com/concepts/prices-orderbook
- https://docs.polymarket.com/trading/orderbook
- https://web.stanford.edu/~boyd/papers/robust_kelly.html
- https://uryasev.ams.stonybrook.edu/publications/
- https://ergo-code.github.io/HiGHS/dev/guide/numerics/
- https://ergo-code.github.io/HiGHS/dev/guide/kkt/
- https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf
- https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf
- https://www.tandfonline.com/doi/abs/10.1080/01621459.1994.10476870
- https://arxiv.org/abs/1505.05116
- https://proceedings.mlr.press/v119/perdomo20a.html
- https://proceedings.mlr.press/v48/jiang16.html
- https://proceedings.mlr.press/v162/kallus22a.html
- https://proceedings.mlr.press/v54/gao17a.html
- https://proceedings.mlr.press/v258/perez-lebel25a.html
- https://proceedings.mlr.press/v291/qiao25a.html
- https://proceedings.mlr.press/v70/wang17a.html
- https://docs.polymarket.com/concepts/resolution
- https://www.jmlr.org/papers/v11/cawley10a.html
- https://brb.nci.nih.gov/techreport/Varma-Simon-CrossValid.pdf
- https://scikit-learn.org/stable/modules/calibration.html
- https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.TimeSeriesSplit.html
- https://robjhyndman.com/papers/cv-wp.pdf
- https://icml.cc/Conferences/2005/proceedings/papers/079_GoodProbabilities_NiculescuMizilCaruana.pdf
- https://proceedings.mlr.press/v89/vaicenavicius19a.html
- https://arxiv.org/abs/2202.07955
- https://www.sintef.no/en/publications/publication/0198cc71224f-73df150e-9187-4b36-bd55-17e240ee6ae0/
- https://academic.oup.com/ectj/article/21/1/C1/5056401
- https://jmlr.org/papers/v21/19-827.html
- https://escholarship.org/content/qt4hn4t174/qt4hn4t174.pdf
- https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551
- https://www.federalreserve.gov/supervisionreg/srletters/SR2602.htm
- https://airc.nist.gov/airmf-resources/airmf/5-sec-core/
- https://json-schema.org/understanding-json-schema/reference/annotations
- https://www.w3.org/WAI/WCAG22/Understanding/error-identification.html
- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://playwright.dev/docs/test-snapshots

## 4. 目标数据流与失败语义

```text
catalog discovery
  -> hard venue/lifecycle eligibility
  -> RepresentedRouteSet (before model-dependent filtering)
  -> atomic route readiness + frozen policy/account/serving/scenario-model bundle
  -> per-route PIT features/model/calibration/trade-policy evaluation
  -> ExecutableEconomicTier[]
  -> report-time concrete PortfolioScenarioArtifact
  -> joint scenario cash-flow matrix
  -> deterministic lexicographic global MILP
  -> exact Decimal post-solve verification
  -> leave-one-out marginal portfolio ranking
  -> atomic route-runs/plan/recommendations/report publication
```

`enabled_categories = []` 在 catalog、training cohort、PIT、report request 和 UI 中统一解释为全部受支持
分类。Category 只作为 filter、risk bucket 与解释字段，不参与报告分区。

`RepresentedRouteSet` 在模型相关筛选之前形成。发现成功但没有任何 eligible market 可以生成空报告；
catalog/data discovery 失败是 report failure。represented Route 完整但最终没有 accepted candidate 记录
`zero_candidate` route outcome；缺 artifact、contract mismatch、pipeline error、solver non-optimal 或 exact
post-check mismatch 均使整份 report run 失败且不发布 `RecommendationReport`。

## 5. 公共类型与持久化契约

新增：

- `RepresentedRouteSet`：有序、去重、非空时带 canonical digest 的 Route 集合。
- `ReportRouteRunId` / `ReportRouteRun`：每个 Route 的冻结 lineage、状态、计数和失败证据。
- `ExecutableEconomicTier`：离散可成交 tier、真实 L2/fee/slippage、场景 cashflow 与资本占用。
- `PortfolioScenarioModelArtifact`：长期、可验证且可晋升的场景生成模型；封存联合 PIT residual panel、
  校准不确定性模型、跨 Route/event 结构、stress catalog、stationary/block-bootstrap 与 ambiguity-set
  规则、discount/time-bucket contract，不包含未来具体 market/token。
- `PortfolioScenarioArtifact`：每次报告根据冻结 market universe、候选、L2/Trade Policy 输入和已晋升
  scenario model 确定性生成的具体联合场景；包含 exact market/token outcome、distribution、structural
  exclusivity 与完整 input/model lineage。
- `GlobalPortfolioPlan`：唯一 MILP 的输入 digest、阶段最优值、约束证据和 exact verifier 结果。
- `RecommendationEconomics`：profit probability、nominal/robust net USD、max loss、CVaR、capital time、
  marginal portfolio value。
- `RouteLineageView`：Route-specific model/calibration/trade-policy/research/scenario lineage。

PostgreSQL 新增 `quant_report_route_run`；recommendation 引用 route run 和 global plan。报告引用完整 Route
set digest、scenario artifact、account/policy snapshot。删除报告级单数 model/profile/run、fallback horizon、
`risk_adjusted_score`、Kelly provenance 和单 Route 假设。

项目从未生产运行，不存在需要保留的生产数据或历史 wire contract。PostgreSQL 只维护一份 clean-install
bootstrap time capsule，并直接重生成为上述最终关系模型；不新增 upgrade migration、archive table、历史
payload converter、dual reader/writer 或兼容 schema。fresh boot 后 schema manifest、SeaORM entity、repository
query 与 API 必须逐列一致。任何真实数据库销毁仍不在自动实施授权内。

## 6. 统一经济尺度与 MILP

每个 `ExecutableEconomicTier` 由真实订单簿 walking 产生，包含 entry VWAP、fee、slippage、shares、
tick/lot、每个联合场景/时间桶的 exit 或 settlement cashflow、capital-lock vector、贴现后 nominal/robust
net USD、profit-probability lower bound、max loss 与 lineage digest。

场景采用严格的两层 artifact contract。`PortfolioScenarioModelArtifact` 使用同一时间索引的联合 PIT
residual vectors、stationary/block bootstrap、校准不确定性和显式 stress catalog，编码跨 Route/event 依赖、
资本成本曲线与统一时间桶；它在 model promotion 时与完整 ordered Route contract 原子绑定。
`PortfolioScenarioArtifact` 则在每次 report decision freeze 后，以该 model artifact、具体 market universe、
calibrated candidate distributions、可执行 L2/Trade Policy paths 为输入确定性 materialize。长期 model artifact
不得预先枚举未知未来市场，具体 report artifact 也不得跨报告复用。两层都禁止 Pearson/category proxy、
逐 Route 独立抽样或 missing-data independence fallback。

唯一 production path 为安全 Rust `highs` API 直接驱动的 HiGHS MILP。每个 candidate 使用离散 tier
one-hot 变量；单次 publishable portfolio solve 只构建/上传一个 persistent model，后续 lexicographic
stage 只修改 objective、追加带固定 relaxation column 的 exact lock，并用上一阶段完整最优解作为 MIP
start；唯一性证明完成后解锁这些 relaxation columns，全部 leave-one-out 只切换 candidate tier bounds 并
在同一 model 上重优化。Historical replay 不执行只服务于报告解释的 marginal solves；
money/objective 在进入 solver 前转换为经范围证明的 fixed-scale integer coefficient，solver 输出只决定
离散 identity，所有金额由 Decimal/newtype 重算。

按四次确定性 solve 实现 lexicographic objective：

1. 最大化允许 distribution/stress set 中最小贴现预期净 USD。
2. 固定第一阶段最优值后最大化 nominal expected net USD。
3. 再最小化 CVaR 与 capital USD-hours。
4. 以稳定 Route/market/tier identity 消除多解。

约束覆盖真实现金、reserve、现有持仓、逐时间桶资本、单 recommendation/market/event/category/Route、
最大场景损失、CVaR、drawdown、TopN 和结构互斥。profit probability lower bound 是 admission，不与
USD 目标加权。求解只接受 optimal；timeout、infeasible contract、数值异常或 exact verifier mismatch
直接失败。

最终排序通过逐 recommendation leave-one-out 重新求解得到 `marginal_portfolio_value_usd`。

## 7. Runtime Config clean-install 契约

六类唯一资源：

1. `recommendation_policy`
2. `execution_risk_policy`
3. `model_routing`
4. `report_schedule`
5. `operations_policy`
6. `execution_automation_policy`

删除 `operational_control`、`execution_authorization` 和全部旧 kind/parser/DTO/UI mapping。

`ExecutionRiskPolicy.portfolio` 只包含：

- `budget`：total、reserve、max open capital。
- `exposure_limits`：recommendation/market/event/category/Route/open-count caps。
- `tail_risk`：CVaR confidence/cap、max scenario loss、drawdown、time buckets/bucket caps。
- `admission`：minimum nominal/robust net USD、profit probability lower bound、maximum probability
  interval width、liquidity buffer。

删除 sizing model、Kelly、solver selection/objective weight 和 Pearson knobs。风险字段全部 required，
禁止 zero-means-unlimited 与 serde default。`ModelRouting` 必须绑定兼容
`PortfolioScenarioModelArtifactBinding`；concrete `PortfolioScenarioArtifact` 由 report run 冻结和持久化，
不得进入长期 Runtime Config。

字段 descriptor 必须生成 RFC 6901 pointer、title/description、unit/format、required/example、bounds、
control、group/order、risk、apply effect、readonly/writeonly、visibility 和 docs link。validation issue 使用
pointer、stable code、severity、message parameters 和 remediation。

UI 使用六个 domain-specific typed editor；共享 primitive controls 可以复用，但递归 generic editor 不再
拥有主路径。descriptor editable pointer 集必须与 DOM `data-config-pointer` 集完全一致。

## 8. Deploy Config 契约与字段矩阵

`DeployConfigDescriptor` 是 Rust serde path、TOML rendering、validation、consumer inventory、safe
projection 和 CI audit 的单一来源。每个 leaf descriptor 必须记录：

```text
path / Rust owner / type / required / profile value or placeholder / unit /
constraints / sensitivity / consumer / restart impact / projection / cross-field rules
```

根 section inventory：

| Section | Owner | Consumer boundary | Apply | Sensitivity |
|---|---|---|---|---|
| `deployment` | `DeploymentConfig` | boot/reset/lifecycle gates | restart | public |
| `polymarket` | `PolymarketConfig` | API, settlement, signing adapters | restart | mixed |
| `market_data` | `MarketDataDeployConfig` | Gamma/Data API/CLOB ingest | restart | mixed |
| `domain_sources` | `DomainSourcesConfig` | vertical source adapters | restart | mixed |
| `observability` | `ObservabilityConfig` | tracing/metrics | restart | public |
| `notifications` | `NotificationChannelsConfig` | notification transports | restart | secret |
| `db` | `DatabaseConfig` | PostgreSQL/ClickHouse pools | restart | secret |
| `cache` | `CacheConfig` | Redis/Moka | restart | secret |
| `keys` | `KeysConfig` | account reads/signing | restart | secret |
| `web` | `WebConfig` | HTTP/JWT/static UI | restart | mixed |
| `quant` | `QuantDeployConfig` | bounded workers/compute budgets | restart | public |
| `research` | `ResearchDeployConfig` | artifact/evidence/serving registry | restart | mixed |

精确 as-is leaf 表已经固化在链接的 audit appendix；目标 leaf 表由
`cargo xtask config audit --format markdown` 从 descriptor 生成并替换该 appendix 的 target 部分。目标
inventory 完成前禁止修改 Config consumer。每个 leaf 在 development 和 production-example profile 中必须各出现一次；
optional 使用 commented assignment，union 每个 variant 提供完整互斥样例，dynamic map 每类 binding
提供 canonical example。

每个 TOML 字段的英文注释必须同时包含 Purpose、Required/Optional、Type/Unit、Constraints、Recommended
value、Operational impact、Restart、Sensitivity 和 cross-field dependency。

loader 只接受 required absolute `--config-file` 与 expected environment，使用 no-follow fd open，拒绝
symlink、非 regular file、错误 owner/mode、unknown/missing field、placeholder、empty secret 和环境不匹配。
production 为运行用户所有的 0400/0600；真实 local secret 同样必须 0600。禁止 base/local overlay、
config-dir、环境变量覆盖和 serde default fill。

Deployment API 从 descriptor 穷尽生成；public field 可显示，sensitive endpoint 只显示分类/状态，secret
只显示 configured/missing，禁止 raw `DeployConfig` serialization。

## 9. UI/UX 验收合同

Config UI：Current/Draft 明确分栏、领域分组、unit/risk/apply effect、inline + summary error、keyboard focus、
semantic diff、preflight、approve、activate、rollback、stale CAS；live control 与 revisioned resource 视觉隔离。

Report UI：一张全局经济排名表，Route/category 只作为 badge/filter；详情展示 route lineage、scenario、
capital occupancy、binding constraints、profit probability、nominal/robust net USD、max loss、CVaR 和 marginal
portfolio value。zero-candidate 与 failure 不得混用 empty state。

## 10. 独立 E2E Visual Gate

真实 production stack + deterministic seed，使用 `expect(page).toHaveScreenshot()`，不接受仅
`page.screenshot()`。状态覆盖 Config overview、六资源 Current/Draft、validation/review/preflight/approval/
activation/rollback/stale CAS、live controls、deployment、mixed-route report、route drawer、zero-candidate、
missing-artifact/solver failure、read-only 和 recovery。

矩阵：390×844、768×1024、1280×720、1440×900；light/dark；zh-CN；固定 timezone/clock/font/seed；
reduced motion。关键金融区域 `maxDiffPixels = 0`；只允许登记的窄范围 volatile mask。每个场景同时通过
axe、keyboard/focus、overflow、console/pageerror/response/requestfailed hard gate。保存 expected/actual/diff、
trace、log、manifest；Linux Chromium baseline 变更人工复核。两个独立 fresh-stack 全矩阵连续通过。

### 10.1 15-stage production DAG `feedback_closure` 发布门禁

确定性截图矩阵的最后一个场景不是静态 seed 展示，而是必须在同一个 fresh production stack 内运行唯一
production coordinator，并按顺序留下以下 15 个 durable stage 的自然证据：

```text
Trigger → TruthFreeze → Coverage → Attribution → Drift → RecipePlan → DatasetSeal
        → Training → Calibration → Cpcv → Validation → Comparison → ShadowBind
        → Shadow → Decision
```

每一 stage 必须同时具备：固定 cycle/stage/job identity、输入 artifact digest、policy/profile/Route lineage、
started/terminal event、duration、attempt/lease evidence、typed outcome 和下一 stage 的精确 predecessor binding。
测试从真实 REST/WS 和 PostgreSQL authority 读取证据，不把 UI fixture 当作 authority。

正向闭环必须证明：

1. `Trigger` 冻结 cadence cutoff、provenance 与当次 active decision-policy snapshot；Champion 的构建时 policy
   preimage 由 serving-contract hash 传递承诺，二者不得错误地要求相等。`TruthFreeze` 冻结 resolution、execution
   attempt/rollup 和 projector watermarks，任何 quarantine/lag/未终态事实均 fail closed。
2. `Coverage` 证明成熟标签、PIT coverage 与 selection mechanism；`Attribution` 只产出可证明的 prediction
   explanation、decision intervention replay、resolution/execution association 与 policy-counterfactual 证据，
   不把 association/SHAP 冒充因果 PnL。
3. `Drift` 明确记录 `ScheduledFloor`、`DriftTriggered` 或受治理 `OperatorForced` 的 typed trigger reason；
   `RecipePlan` 只能消费同 profile/Route/model-family、早于 cutoff 且来自前序 cycle 的 artifact，实际改变
   challenger recipe 或明确给出 no-action 原因，禁止只有 inventory 没有消费。
4. `DatasetSeal` 证明 PIT/no-leakage、purge/embargo、sample/label/feature/selection lineage；Buy model Dataset 的
   target 必须是完整 pre-decision score population 对应的 canonical payout truth，不得以 policy return 或只被选中/
   已成交子集替代。Trade Policy cashflow 是并行的 policy-evaluation evidence：其 Published artifact 必须在 cohort
   起点前已完成拟合、发布和 embargo，且整个 cohort 使用冻结 identity。当前周期中后验拟合的策略不得回填 earlier
   row；离线 cross-fit 只能进入独立 research evidence，不能替代 prospective promotion cohort。`Training` 记录完整
   trial universe、seed、code/data/environment digest 与 resource budget；`Calibration` 在独立 calibration partition
   上封存概率映射、区间和 reliability/proper-score 证据。
5. `Cpcv` 使用 purged/embargoed combinatorial paths，封存每条 OOS path、multiple-testing universe、PBO、
   DSR、turnover/cost/tail-loss 与参数稳定性；`Validation` 是唯一 `ModelQualityGate`，任一 hard gate 失败
   不得到达 Comparison。
6. `Comparison` 在相同冻结 cohort、成本、资本和 scenario contract 下比较 challenger 与当前 champion，
   使用预先声明的非劣/改进边界和统计不确定性；`ShadowBind` 原子占用精确 Route shadow slot；`Shadow`
   使用真实 production inference/feature parity、但不改变 serving 或提交交易。
7. `Decision` 最多产生 `CandidateReady`，自动 DAG 永不自行 promote。随后浏览器必须以两个独立审计动作
   完成 permit 与 activation，并证明 serving generation CAS、旧 champion 退役、receipt 可重取和 exact
   rollback deep-link；新 generation 若改变兼容 digest，必须原子绑定新的
   `PortfolioScenarioModelArtifact`，rollback 同时恢复旧 champion 与旧 scenario-model binding。
8. 激活后必须从新 scenario-model binding 与本次冻结市场输入生成新的 concrete scenario artifact，再生成
   一份 mixed-route global report，证明新 Route generation 的 calibrated probability、executable USD
   scenario cashflow、global MILP、exact verifier 与 recommendation lineage 真正消费了该闭环产物；随后
   resolution/execution outcome 能进入下一 cycle 的 eligible feedback cohort，形成 N→N+1。

浏览器正向场景由 fresh-stack harness 在后台观察真实 permit/activation/report authority；报告发布后，harness 只能通过
source-native resolution plane 注入未来 truth，等待生产 reconciliation worker 投影 outcome，再逐 Route 精确复核
`ModelLearning`、`PolicyEvaluation` 与 `ExecutionLearning` cohort 语义。Playwright 读取 run-owned content manifest，且必须
在 manifest 的 cycle/report/candidate identity 与 UI 操作完全一致后才进入 global-report/lineage 截图断言；harness 失败会
写独立 error manifest 并使场景失败。禁止 app test-only endpoint、直接 outcome row seed 或在 manifest 完成前截图冒充闭环。

必须单独覆盖并截图/留 trace 的负向路径：truth blocker、insufficient coverage、selection bias/label maturity
blocker、recipe/artifact incompatibility、PIT leakage、training failure、calibration degradation、CPCV/PBO/DSR
拒绝、quality-gate reject、shadow parity/drift failure、lease loss/restart same-identity recovery、stale CAS、
permit/activation RBAC 拒绝、scenario-binding mismatch 和 post-activation report fail-closed。失败必须停止在精确
stage，不能发布 candidate、route generation 或 report。

确定性要求：固定 UTC clock、cadence cutoff、dataset snapshot、trial universe、seed、solver/backend、font 与
Chromium/Linux image；每次运行生成 stage manifest 和 content hashes。两个互相独立的 clean bootstrap 必须
得到相同的 stage 顺序、terminal outcome、artifact/report hashes（数据库生成的 opaque ID 除外），并完整通过
截图矩阵。测试不得通过放宽 timeout、重试整条链或更新 baseline 来掩盖非确定性。

## 11. 实施波次与退出条件

| Wave | 内容 | 状态 | 退出条件 |
|---|---|---|---|
| W0 | 本文、精确 Runtime/Deploy inventory、正式架构修订 | DONE | 文档 diff clean、architecture check、Config API source/generated parity |
| W1 | descriptor、Runtime contract、JSON Pointer API、clean-bootstrap final schema | IN_PROGRESS | Rust/schema/repository/fresh-boot tests |
| W2 | represented routes、economic tiers、scenario model + per-report artifact | IN_PROGRESS | unit/property/PIT parity tests |
| W3 | global MILP、exact verifier、leave-one-out ranking | TODO | brute-force parity、determinism、SLO |
| W4 | report persistence/API/execution/UI | TODO | mixed-route system/API/UI tests |
| W5 | six domain editors、Deploy loader/TOML/projection | TODO | config audit + UI pointer equality |
| W6 | deletion/fresh boot | TODO | no legacy symbol/path, one final bootstrap manifest |
| W7 | 15-stage production DAG feedback closure | IN_PROGRESS | natural stage evidence, promotion boundary, N→N+1 consumption |
| W8 | visual E2E and full gates | TODO | two consecutive complete fresh-stack passes |

### 11.1 Evidence ledger

| Date | Wave | Command/evidence | Result |
|---|---|---|---|
| 2026-08-05 | W0 | source-derived Runtime/Deploy audit | 117 logical Runtime controls；61 Deploy structs / 310 leaves |
| 2026-08-05 | W0 | `git diff --check` | PASS |
| 2026-08-05 | W0 | `cargo xtask architecture check` | PASS；仅 macOS compact-unwind linker warning |
| 2026-08-05 | W0 | `pnpm -C ui check:config-api` | PASS；Rust schema 与 generated TypeScript 同步 |
| 2026-08-05 | W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；TruthFreeze/Coverage/Attribution/Drift/RecipePlan 成功，DatasetSeal 因 Weather fixture 的 policy-return target 与完整 score cohort 仅有 canonical payout truth 不同义而停止；cycle `9073fd26-7f5b-5336-a1f6-85bc9a6f4b1b`，run `019fd1d0-fdee-7fa0-96bd-50b19d2d9766`。该证据触发 3.1 clean-break，不通过伪造 policy label 绕过。 |
| 2026-08-08 | W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；真实 DAG 已推进到 `Cpcv`，随后拒绝 `scenario data as_of 2026-07-06` 晚于 outer replay cutoff `2026-03-06` 的泄漏。cycle `95a90fbb-808f-54e6-952e-6299b9601701`，run `019fe004-3739-78f0-8abd-02e915d6cdf6`，job `cf3c5854-8ed8-5b3a-a79b-47e8ca3b39fc`。该证据触发完整 estimator nesting 与 fold-local OOS payout residual scenario model；不通过放宽 PIT cutoff 绕过。 |
| 2026-08-08 | W2/W7 | `cargo check -p quant-pivot-research -p quant-pivot-core -p quant-pivot-system-tests --tests` | PASS；四段 nested estimator contract、fold-local calibration/scenario lineage 与 production harness 全部通过类型检查。 |
| 2026-08-08 | W2 | `cargo test -p quant-pivot-research --lib` | PASS；560 passed，覆盖独立 calibration fit/validation evidence、scenario fit 与金融研究域回归。 |
| 2026-08-08 | W2 | `cargo test -p quant-pivot-core service::cpcv_backtest --lib` | PASS；9 passed，覆盖四个 population 的互斥、双重 purge/embargo 与退化 fail-closed。 |
| 2026-08-08 | W1/W5 | `cargo xtask config audit` | PASS；314 个 Deploy descriptor 在两份 strict TOML 中完整、唯一且注释闭环。 |
| 2026-08-08 | W1/W5 | `pnpm -C ui generate:config-api && pnpm -C ui check:config-api` | PASS；103 个 Runtime JSON Pointer 的 Rust schema、generated TypeScript 与 zh-CN i18n 同步。 |
| 2026-08-08 | W1/W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；fresh stack 在 DAG 前拒绝插入 `quant_feedback_recipe_template`，因为强类型 `ResearchValidationCpcvConfig` 已包含 `nested_estimator_*`，而 PostgreSQL JSONB 精确键约束仍停留在旧集合。run `019fe073-585f-7ed2-9c6a-c3343964a811`；该证据触发 fresh-schema 单一真相源修复，不删除或放宽精确约束。 |
| 2026-08-08 | W1 | `cargo test -p quant-pivot-migration recipe_closes_cpcv_contract --lib` | PASS；当时 fresh schema 已同时约束 nested estimator 两字段存在与 holdout 为 1..9999 bps；后续真实 CPCV 容量证据要求将 group floor 进一步收紧为 4。 |
| 2026-08-08 | W7 | `cargo check -p quant-pivot-system-tests --lib` | PASS；真实浏览器 closure monitor、source-native settlement、production reconciliation 与逐 Route N→N+1 cohort verifier 通过全库类型检查。 |
| 2026-08-08 | W7/W8 | targeted UI ESLint + `pnpm -C ui check:type` | PASS；closure manifest fail/success contract 与 45-package TypeScript graph 无 lint/type error。 |
| 2026-08-08 | W1/W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；新增 fresh-schema CHECK 后，bootstrap migration 与尚未重建的 immutable deploy manifest 不同，disposable stack 在业务 seed 前拒绝启动。未手改 checksum 或跳过 identity gate。 |
| 2026-08-08 | W1 | `cargo xtask postgres-schema manifest-clean` | PASS；canonical owner 在 disposable PostgreSQL 16 完整安装并反查 semantic schema，前后两次生成一致的 `schema/postgres/migrations.json`，同时重建 `manifest.json`。 |
| 2026-08-08 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；fresh run `019fe094-630d-7402-a479-12855a8cd96f` 的自然 DAG 已依次完成 Trigger、TruthFreeze、Coverage、Attribution、Drift、RecipePlan、DatasetSeal、Training 和 Calibration，cycle `9c46ba24-d565-5bdb-b2dc-21fcf10daca7` 在 Cpcv job `61a7fd03-7a50-5ce9-a2eb-d0e823e5ea01` 因 `nested purge/embargo left no disjoint model, calibration, and scenario fit populations` 失败。该证据表明默认 nested estimator 原始 group floor 没有对 purge 后三个拟合 population 的非空性建立结构性保证；修复必须收紧方法学契约与数据充分性证据，不得减少 purge/embargo、跳过 fold 或引入 fallback。 |
| 2026-08-08 | W2/W7 | 失败 run 的 immutable training Parquet 取证 + 同契约穷举 | 确认 512 rows 只分布于 8 个 decision times，相邻 ticks 复用 market cohort，外层 purge 后不足以产生三个独立拟合 population；由此把 fixture 扩为 32 个真实时点并设 raw holdout floor 4。随后真实 run 证明“成对 overlap”测试仍低估了逐 tick 滚动 label horizon，本行只作为问题发现与中间数据拓扑变更证据，不再被当作容量证明。 |
| 2026-08-08 | W2 | `cargo test -p quant-pivot-core service::cpcv_backtest --lib` | PASS；当时 10 passed，但 56-fold helper 使用不连续的成对 horizon，而 production fixture 是每 tick 50% rolling overlap；该测试缺口已在 2026-08-09 的真实 DAG 中暴露并由精确拓扑测试替换。 |
| 2026-08-08 | W1/W2 | Runtime + PostgreSQL 方法学约束定向测试 | PASS；Runtime 拒绝 3-group nested holdout，PostgreSQL recipe JSONB CHECK 要求 `nested_estimator_min_groups >= 4`，且不再把 CPCV partition 数误当作 decision-time group 数。 |
| 2026-08-08 | W1/W5 | `cargo xtask config render && cargo xtask config audit` | PASS；两份 strict TOML 重建且 314 个 Deploy descriptors 完整、唯一。 |
| 2026-08-08 | W1/W5 | `pnpm -C ui generate:config-api && pnpm -C ui check:config-api` | PASS；Rust schema、generated TypeScript 与 103 个 Runtime pointer i18n 再次同步。 |
| 2026-08-08 | W1 | `cargo xtask postgres-schema manifest-clean` | PASS；收紧后 JSONB CHECK 经 disposable PostgreSQL 16 安装/反查后重建两份 immutable schema manifests。 |
| 2026-08-08 | W1/W2 | `cargo fmt --all -- --check` | PASS。 |
| 2026-08-08 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；fresh run `019fe0c4-a74c-7582-8703-78a7b74c8499`、cycle `0c5d2e21-3a03-564f-b1d7-c3d6afebc297` 已自然完成 Trigger、TruthFreeze、Coverage、Attribution、Drift、RecipePlan、DatasetSeal、Training、Calibration，并在 Cpcv job `84568ffd-ac83-5ba0-9c64-c2ca15c73f67` 拒绝 scenario 0：market `feedback-closure-training-market-16` 的退出容量 `2.261904` 小于 tier `2.380952` shares。取证确认旧公式以 `max_required_shares × executable_share_bps` 生成容量，只要 stress `<10000 bps` 最大 tier 必然失败，属于决策变量反向定义可行域的循环合同；未把 fixture stress 改为 100%。 |
| 2026-08-08 | W2 | bid/ask 外生容量 clean-break | `BacktestExecutionSnapshot`、production capture、economic seed 与 scenario leg 现在同时绑定真实 bid/ask L2；entry 仍 walk asks，scenario capacity 改由冻结 bid shares 经 stress 得出。超容量 tier 使用 `scenario_exit_capacity` admission/funnel reason，现有持仓先占用容量；精确 scenario leg 缺失仍 fail closed。 |
| 2026-08-08 | W2 | `cargo test -p quant-pivot-research --test economic_tier --test global_portfolio --test global_portfolio_fail_closed` | PASS；2 + 3 + 4 tests，覆盖精确 leg 缺失整份失败、mixed-route MILP/brute-force/determinism、单 tier 压力容量拒绝及 `existing + proposed` 净容量拒绝。 |
| 2026-08-08 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；fresh run `019fe0f6-94fe-7512-97f4-e7208ab7d4b2`、cycle `057e7b15-0d31-58da-ade1-94242511bbbe` 已自然完成 Trigger、TruthFreeze、Coverage、Attribution、Drift、RecipePlan、DatasetSeal、Training、Calibration；Cpcv job `77d35f87-1000-5249-9123-7009942c97a6` 完成全部折内 estimator 计算后，在历史 replay 的 global planner 拒绝 `scenario artifact decision time or model binding differs from the frozen account`。根因是旧 artifact 没有内容绑定 PIT 与 purged-CV 两种统计可见性，planner 因而把合法的 disjoint fold estimator `bound_at` 错当作 live PIT 时间；未删除时间校验或伪造早期 binding。 |
| 2026-08-08 | W2 | scenario visibility clean-break | `PortfolioScenarioArtifact` 的 canonical preimage 现显式绑定 `PointInTime` 或带非零 `fit_evidence_hash/test_groups_hash` 的 `PurgedCrossValidation`。PIT binding 仍必须在 decision/account freeze 前可见；Purged-CV 仅允许 `HistoricalReplay` account，实时 `Polymarket` account 必须 fail closed，消除验证例外逃逸到实盘的可能。 |
| 2026-08-08 | W2 | `cargo test -p quant-pivot-research --test global_portfolio_fail_closed` | PASS；7 passed，直接证明未来 PIT binding 拒绝、Purged-CV live account 拒绝、带精确 population evidence 的 Purged-CV historical replay 允许，以及容量/现有持仓/hash 的既有 fail-closed 行为。 |
| 2026-08-08 | W2/W7 | `cargo check -p quant-pivot-core -p quant-pivot-system-tests -p quant-pivot-bench --all-targets` | PASS；scenario visibility canonical field 已贯穿 report、backtest、CPCV、system fixture 与 production benchmark 的全部调用方。 |
| 2026-08-08 | W7 | `cargo test -p quant-pivot-system-tests production_stack::tests --lib` | PASS；2 passed。确定性 Polygon mock 使用单调 2-second slot clock，head 随运行推进且历史 block timestamp/hash 不变；production 120-second freshness gate 未放宽。 |
| 2026-08-08 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；fresh run `019fe129-8947-7e70-a74b-c56f75a7a44f` 已完成 production stack、浏览器预检与 500 个 OOS evaluation cohort seed，随后在主 cycle trigger 前拒绝 Source Slice ledger row：ClickHouse UUID adapter 收到 owned JSON string 而要求 borrowed string。根因是新 direct decode 使用 consuming `serde_json::from_value`，旧 canonical byte round-trip 曾偶然掩盖 adapter 的借用契约；未恢复重复序列化，而是改为从 `&serde_json::Value` 直接反序列化。 |
| 2026-08-08 | W2/W7 | `cargo test -p quant-pivot-core prefetch::source_slice::tests --lib` | PASS；4 passed。对象版本、hash、schema、row-count、排序与 PIT bounds 已在 `read_objects` 验证后，typed decode 从借用的已验证 `serde_json::Value` 直接反序列化，删除逐行 canonical serialize + parse，同时证明 Decimal/map 与真实 `BookL2LedgerRow` ClickHouse borrowed-UUID adapter 均完整解码。 |
| 2026-08-08 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；fresh run `019fe142-4904-7a20-8260-119d1d558325`、cycle `84015300-000c-555f-80fe-88fe0ae47036` 已真实完成 Trigger、TruthFreeze、Coverage、Attribution、Drift、RecipePlan、DatasetSeal、Training、Calibration，并在 Cpcv job `5925ee41-58ca-59e4-9cc6-08c45bde31f5` 完成折内估计后拒绝 `ExecutionRiskPolicy capital buckets do not match the promoted artifact`。取证确认同一旧字段把 residual resampling horizon/method hash 与 capital bucket limits hash 混作一个契约；cycle 在 ShadowBind 前失败且未发布后续决策。 |
| 2026-08-08 | W2 | capital-time contract clean-break + 文献交叉验证 | 唯一 canonical owner 现只 hash 有序 `end_secs`，字段全仓重命名为 `capital_time_bucket_contract_digest`；统计 `time_bucket_secs/resampling_method` 继续独立进入 scenario-model artifact hash。USD caps 只由冻结 policy 约束，cap 变化不 rekey，boundary 变化必 rekey；旧字段名、旧 hash domain、兼容 alias 均已删除。 |
| 2026-08-08 | W2/W7 | `cargo check -p quant-pivot-research -p quant-pivot-core -p quant-pivot-system-tests -p quant-pivot-bench --all-targets` | PASS；scenario fit/fold、concrete generation、global planner、backtest、production fixtures 与 benchmark 全调用方通过 clean-break 类型检查。 |
| 2026-08-08 | W2 | capital-time 分层测试 | PASS；contract 3/3、scenario refit 11/11、concrete generation 4/4、global fail-closed 9/9；直接证明 cap 不改变兼容 hash、boundary 改变重键/拒绝、重复边界拒绝及既有 PIT/capacity 语义。 |
| 2026-08-08 | W1/W5 | `pnpm -C ui generate:config-api && pnpm -C ui check:config-api` | PASS；Rust schema、generated TypeScript 与 103 个 Runtime pointer i18n 同步；旧 `time_bucket_contract_digest` wire field 为零命中，唯一字段为 `capital_time_bucket_contract_digest`。 |
| 2026-08-09 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；fresh run `019fe3e4-02ca-7ed0-8622-37c1285a6f15`、cycle `b26380fa-1aa0-5e40-8baa-bf459aebb25a` 自然完成 Trigger、TruthFreeze、Coverage、Attribution、Drift、RecipePlan、DatasetSeal、Training、Calibration，并在 Cpcv job `9e6c13a5-ba0f-5d1b-9608-0ed1ee325b25` 精确拒绝 `outer=19 / estimator=4 / model=14 / calibration candidates=2 / calibration fit=1 / scenario=2`。根因是 50% rolling market universe 令 calibration 边界 label 在首个 scenario tick 后成熟；固定 `2+2` raw split 把结构性 purge 损失误当作数据故障。未降低 2% embargo、7-day lookback 或 two-group population floor。 |
| 2026-08-09 | W2 | nested estimator 文献与算法交叉验证 | Varma–Simon 的完整 nested procedure、scikit-learn 的独立 calibration data 与 time-series gap、Bergmeir–Hyndman–Koo 对 dependent CV 适用边界共同支持：独立性必须由实际时间/label interval 验证，不能由固定 row count 推断。唯一实现改为从 governed lower bound 开始寻找最小可行 suffix，并以 post-purge population maximin/balance 选择边界；没有 retry/fallback 或阈值放宽。 |
| 2026-08-09 | W2 | `cargo test -p quant-pivot-core service::cpcv_backtest::tests --lib` | PASS；10 passed。56/56 folds 现在使用与 production closure 一致的 90-day、逐 tick rolling horizon、2% embargo 和 7-day lookback floor；另有定向测试证明 raw `2+2` 失效时算法保留 purge gap、扩到首个可行 suffix，并得到互斥 model/calibration/scenario population。 |
| 2026-08-09 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；fresh run `019fe40c-7332-7892-8727-2669d58ceb3d`、governed cycle `c0b5197f-16f3-5cda-a1ec-4350e52befec` 已自然完成 Trigger、TruthFreeze、Coverage、Attribution、Drift、RecipePlan、DatasetSeal、Training、Calibration；Cpcv job `5497ada2-545e-5b0a-9cbb-738b6047c51f` 越过旧 nested-split 故障，完成 CPCV 进度 45 并进入完整 16-trial `trial_grid` 进度 75，随后精确触发 research compute 900-second hard deadline。cycle 在 ShadowBind 前以 `research_job.execution_failed` 失败；无 retry、partial publication 或降级结果。backend evidence 保留于 `target/production-stack/019fe40c-7332-7892-8727-2669d58ceb3d/backend.log`。 |
| 2026-08-09 | W2/W3 | exact compute 性能方法学决策 | CSCV/PBO/DSR 的 multiple-testing universe 必须保留完整固定 trial ledger，每个 trial 仍执行相同 purged OOS estimator；不延长 900-second deadline、不缩小 trial grid、不减少 folds、不提前淘汰 trial。Cawley–Talbot 的 selection-overfitting 结论、CSCV/PBO 原论文及 nested-CV 契约共同排除改变估计量的近似路径。唯一允许的提速是复用同一 split 的 immutable fold preparation、并行 exact fits，以及在同一 HiGHS MILP model 上通过 objective mutation、exact epsilon locks 与 incumbent hot start 执行确定性 lexicographic re-optimization；每阶段仍只接受 Optimal，最终仍由整数/Decimal 域完整 post-check。 |
| 2026-08-09 | W3 | direct HiGHS persistent-model clean-break | 删除 `good_lp`/microlp 间接路径与旧 benchmark/spec 名称；唯一 production solver 直接使用 safe `highs` API。每份 portfolio 只构建一个 MILP matrix，后续 lexicographic stage 只变更 column cost、追加 exact epsilon lock 并 hot-start 上一完整解；每阶段必须为 `Optimal`、finite、zero MIP gap，随后执行整数/Decimal exact verifier。`SolverEvidence` 记录并强制 `model_build_count = 1`、`warm_start_count = solve_stage_count - 1`，违反即不发布。 |
| 2026-08-09 | W2/W7 | exact weighted-fold preparation reuse | `PreparedWeightedFold` 只冻结同一 purged split 的 reference CDF、transformed matrix、dataset 与 input hash；每个 governed objective 仍独立执行完整 coordinate search/refinement、calibration、scenario fit 与 replay。cache 只存在于一次 `TrialGridRun`，以 canonical model-group index vector 为键；subject CPCV 和 classical estimator 不经过共享 cache，不存在近似、跨 run 状态或 fallback。 |
| 2026-08-09 | W2/W3 | `cargo test -p quant-pivot-research prepared_ --lib` | PASS；2/2。缓存路径与原始 direct trainer 的 payload hash、training/input contract hash、transform hash、in-sample/validation metrics 精确相等；`folds > 1` 明确拒绝，防止 preparation 偷换 inner validation。 |
| 2026-08-09 | W2/W3 | `cargo test -p quant-pivot-research backtest::runner::tests --lib` | PASS；9/9。global replay 与 report solve 一致、hash/metrics/未分配 observation 保持确定性；新增 metadata、outcome、inference context、execution snapshot、downside trajectory 重复键均 fail closed。 |
| 2026-08-09 | W2/W3 | `cargo test -p quant-pivot-research --test economic_tier --test global_portfolio --test global_portfolio_fail_closed` | PASS；2 + 3 + 9。覆盖 mixed Route 单组合、brute-force 最优一致、输入重排 hash 不变、artifact/PIT/purged visibility/capacity/existing-position 全部 fail closed。 |
| 2026-08-09 | W2/W7 | `cargo test -p quant-pivot-core service::cpcv_backtest::tests --lib` | PASS；10/10。nested split、purge/embargo、canonical rank、OOF estimator 与 production topology 容量契约均保持。 |
| 2026-08-09 | W2/W3 | `cargo clippy -p quant-pivot-research -p quant-pivot-core --all-targets -- -D warnings` | PASS；direct HiGHS、prepared-fold cache、backtest replay 与其测试 targets 无 warning。 |
| 2026-08-09 | W2/W3 | `cargo xtask architecture audit-functions` + `cargo xtask architecture check` | PASS；function audit `0 hard / 660 review`，完整 architecture check PASS；macOS 仅报告既有 compact-unwind linker warning。 |
| 2026-08-09 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；fresh run `019fe48e-68d7-78a1-b707-f1ac8523c1e1`、cycle `96eef302-b640-50ef-99f6-4ef48b768d5e` 已自然完成 Trigger、TruthFreeze、Coverage、Attribution、Drift、RecipePlan、DatasetSeal、Training、Calibration；Cpcv job `bbf2f7f5-0446-5fb0-a973-32094b28832f` 保持约 4 核计算与健康 heartbeat，但完整 `trial_grid` 仍在 900-second hard deadline 精确失败。未到 ShadowBind，未发布 candidate、serving generation 或 report；该证据否定“prepared-fold cache + persistent HiGHS 已满足生产 SLO”，下一步必须用阶段级 deterministic profiling 定位 estimator fit、scenario fit、replay/MILP 热点，保持完整 16-trial/56-fold 估计量不变。 |
| 2026-08-09 | W2/W3/W7 | `cargo xtask production-stack feedback-closure --runs 1` + three 20-second deterministic `/usr/bin/sample` windows | REAL FAIL-CLOSED；第二个独立 fresh run `019fe4b3-a066-7ed1-b161-c41383e866b2`、cycle `5fb64b2a-3127-5c7e-a828-c7f675924032` 再次自然完成至 Calibration，Cpcv job `408f85d1-d36f-5907-a7fd-06a00ed00afa` 在完整 `trial_grid` 触发同一 900-second deadline，仍未进入 ShadowBind。subject、trial early/mid/late 样本一致指向真实 `PortfolioReplayBacktester`；mid 样本中 replay 10423/11705、scenario generation 4114、global planner 2719、artifact validation/hash 约 1272，而 lexicographic HiGHS solve 仅 63、weighted fold preparation/model fit 约 158。证据排除 solver rebuild 与 preparation cache 为主瓶颈，确认 concrete scenario 生成后立即对完整 scenarios 重复 canonical serialization/hash verification 是共同热路径；修复必须改为 full leaf verification + sealed artifact boundary，不得跳过完整性校验、减少 folds/trials 或延长 deadline。`trial_grid` 对外进度固定为 75/100 亦被登记为错误的粗粒度运维合同，必须改为逐 trial/fold 的真实 durable progress。采样证据保留于该 run 目录的 `sample-cpcv-subject.txt`、`sample-trial-early.txt`、`sample-trial-mid.txt`、`sample-trial-late.txt`。 |
| 2026-08-09 | W2/W3 | hierarchical scenario sealing + typed verification boundary | `PortfolioScenarioModelArtifact` 先封存有序 state leaf hashes；每个 concrete scenario 先封存 outcome leaf hashes，再由 scenario root 和 artifact root 逐级承诺。raw object 进入系统时仍执行 leaf→root 全量验证；可信生成器返回 `SealedPortfolioScenarioArtifact`，同一 report/replay 内消费者只接受 sealed typestate，禁止重复 canonical JSON/hash，也禁止跳过首次深验证。RFC 8785、BLAKE3 与 NIST Merkle tree 定义共同支持“canonical leaf once + ordered parent commitment + boundary full verification”的设计。 |
| 2026-08-09 | W2/W3 | scenario/backtest/global targeted verification | PASS；scenario 6/6、backtest runner 9/9、economic tier 2/2、global portfolio 3/3、global fail-closed 9/9；`cargo check` 覆盖 research/core/system-tests/bench 全 targets，`cargo fmt --all -- --check` PASS。 |
| 2026-08-09 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | PARTIAL REAL PASS / FINANCIAL FAIL-CLOSED；fresh run `019fe502-16a5-7fb3-8e25-c08763f861a0`、cycle `f6b95eb0-05b8-5af7-9fb1-f4ba76ec2f23` 首次在完整 16-trial/56-fold 合同下完成 Cpcv：约 11m57s，低于 900-second hard deadline；随后 Validation 正确终止为 `ChallengerRejected`，没有进入 Comparison/ShadowBind。不可变 validation artifact `blake3:1ee4bf192e39fc0f509baa38dba473af4ce4e6b8c795771201658bd9c546dc6e` 显示样本量 512、label/materialization/PIT、21 paths、median Rank IC 0.738105321317、PBO 0、tail loss 和 drawdown 均通过；唯一 hard failures 为 `median_turnover=0.641205837174 > 0.5` 与 `deflated_sharpe=0.171312920842 < 0.95`。不得调整门槛或把拒绝改成成功。 |
| 2026-08-09 | W2/W7 | Validation 方法学 root-cause review + primary-source cross-check | Bailey–López de Prado DSR 原论文明确 `N` 是 implied independent trials，并要求 dependent trials 由 trial-return correlation 推导；现实现却令 `trial_count == trial_grid_count == 16`。代码审计同时确认 Buy CPCV 在每个 fold 内用 `HistoricalReplay` 零持仓账户独立求解并即时把终局 PnL归属于当期，再拼接 fold returns；因此同一尚未结算资本可跨 tick 重复使用，`turnover` 只是 allocation-weight instability。目标 clean-break：fold 只生成 allocation-independent OOS prediction/calibration/scenario evidence；完整 φ-path 拼接后以事件时间、真实结算、已有持仓与资本释放执行唯一 stateful global portfolio replay。CSCV evidence新增 trial pair cross-products/correlation、average correlation、fractional implied independent count 与 conservative-ceiling DSR `N`；raw grid count 保留为完整试验账本。 |
| 2026-08-09 | W2 | 历史 schema-8 诊断（已由 schema-10 clean-break 删除） | 当时 CSCV 冻结每个 trial 的 block sums/squared sums、每对 trial 的 cross-product、精确重复标记与 Pearson correlation，并以 `ceil(rho_bar + (1-rho_bar)·M)` 估计 DSR `N`。后续真实 production evidence 证明 raw-`M` zero-variance fallback 会重复计算同一经济行为，因此该运行时 schema、枚举和 parser 已全部删除；此行只保留问题发现链路，不是可执行设计。 |
| 2026-08-09 | W2 | stateful/self-financing CPCV replay clean-break | fold evaluation 只保留 OOS inference、rank、calibration residual 与 fold-local scenario；完整 φ-path 重建后才运行唯一 `PortfolioReplayBacktester::run_precomputed`。账户从单一 all-cash snapshot 开始，entry principal+fee 立即扣现，未到 canonical `resolved_at` 不释放，已有仓位进入后续 scenario/exposure/MILP，现金不足即不能重复下注；未来真实 resolution time 只驱动账本结算，不进入当时 scenario，避免 PIT 泄漏。最终 PnL 归属于原 decision cohort，保持 Romano–Wolf/CSCV 同窗配对；equity/drawdown 仍按实际 settlement event 更新。turnover 定义为所有固定 cadence tick 的 `executed entry cash / frozen capital base` 均值，settlement/redemption 不重复计交易。 |
| 2026-08-09 | W2 | stateful/DSR targeted verification | PASS：`cargo test -p quant-pivot-research backtest::runner --lib -- --nocapture` 10/10（含 `capital_lock_until_resolution`）；`cargo test -p quant-pivot-research validation::cpcv --lib -- --nocapture` 13/13（含每条 φ-path 恰好一次 stateful replay）；`cargo test -p quant-pivot-research validation::pbo --lib -- --nocapture` 13/13（相同 trial 折为 1、orthogonal 保留 M、tamper/非重复零方差拒绝）。coverage 同步纠正为成熟 emitted-candidate 比例，sample_count 仍是实际 executed/resolved allocation 数。 |
| 2026-08-09 | W2 | `cargo test -p quant-pivot-core service::cpcv_backtest::tests --lib -- --nocapture` | PASS；最新冷构建 5m08s，10/10。nested purge/embargo、canonical rank 与 production topology 保持；CPCV core 已完整类型迁移到 path-level precomputed replay。 |
| 2026-08-09 | W2/W3 | post-stateful unified Rust gates | PASS：`cargo check --workspace --all-targets`；`cargo clippy -p quant-pivot-models -p quant-pivot-research -p quant-pivot-core -p quant-pivot-system-tests -p quant-pivot-bench --all-targets -- -D warnings`；`cargo xtask architecture audit-functions`（`0 hard / 660 review`，删除本波次新增的无边界 forwarding accessor 后回到既有 review 基线）；`cargo xtask architecture check`。 |
| 2026-08-09 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；fresh run `019fe5b0-ac62-7933-bc7b-00f889decdcf` 在 seed browser production fixture 时拒绝 path-set：raw grid `M=2`、persisted DSR `N=2`，而 pairwise dependence evidence 重算 conservative independent count 为 `1`。尚未进入业务 DAG，未产生部分闭环或 serving 变更。 |
| 2026-08-09 | W1/W2/W4 | DSR count semantic clean break | 将含糊的 path-set/DB/API/UI `trial_count` 全链路删除并改名为 `dsr_conservative_independent_trial_count`；fixture 和 production 持久化均从 sealed `CscvSelectionEvidence.trial_dependence.conservative_independent_trial_count` 派生，raw `trial_grid_count` 与 audit-only `coord_search_effective_n` 保持独立。未保留 serde alias、旧列、兼容 DTO 或双读。 |
| 2026-08-09 | W1/W2 | DSR count targeted verification | PASS：四个关键 crate `cargo check --all-targets`；path-set canonical seal/tamper tests 5/5；governance path-set gate 2/2。 |
| 2026-08-09 | W1 | `cargo xtask postgres-schema manifest-clean` | PASS；disposable PostgreSQL 16 从空库安装并双生成 immutable manifests；`quant_backtest_path_set` 只含 `dsr_conservative_independent_trial_count`，旧 `trial_count` 列零命中。 |
| 2026-08-09 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；fresh run `019fe5c6-aebf-7613-b0b8-8a40268eaa4b`、cycle `3156d808-9282-5d19-89ab-4a477ac29d68` 已自然完成 Trigger→Calibration，Cpcv job `4c8e3ebb-4410-58b8-8438-ac987913e5a0` 在约 12 分钟内完成 stateful trial replay，随后因 trial 0/1 存在“零方差但非精确重复”而拒绝 undefined Pearson。未进入 ShadowBind，未发布 candidate/permit/activation/report。证据：`target/production-stack/019fe5c6-aebf-7613-b0b8-8a40268eaa4b/backend.log`。 |
| 2026-08-10 | W2 | DSR/PBO non-redundancy 原始资料交叉验证 | [DSR Appendix A.3](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf) 明确 `N` 是 independent trials，raw `M` 在相关试验下会高估 expected maximum，并建议以 correlation、dimension reduction 或 information-theoretic redundancy 识别 non-redundant sources；[PBO Algorithm 2.3](https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf) 要求同步可估的 strategy-performance matrix。真实 run 的 16 个配置只有 2 条精确 OOS return columns，故 raw-`M` fallback 会把同一策略的重复参数化错误放大。最终口径保留全部 raw trials，但按精确 return-column equivalence classes 形成 PBO/DSR population；零方差 representative 不伪造 Pearson，直接以完整行为类数作为保守 count。 |
| 2026-08-10 | W2 | CPCV evidence schema 10 clean break | `CscvSelectionEvidence` 同时冻结 raw blocks、raw trial Sharpe、全部 raw pair cross-product、canonical `CscvTrialEquivalenceClass`、behavioral pair count 与 representative-only Sharpe variance。`CscvDsrTrialCountEvidence` 只保留 `AverageCorrelation` 和 `DirectBehavioralClassCount`；schema-9 `RawTrialCount` 已删除，无 alias、旧 parser 或 dual read。PBO champion/rank、OOS population、DSR variance 与 `N` 全部使用同一 representatives；artifact validator 从充分统计量重建等价类、zero-variance representative、count 与 variance。 |
| 2026-08-10 | W2 | behavioral-trial targeted verification | PASS：`cargo check -p quant-pivot-models -p quant-pivot-research -p quant-pivot-core --all-targets`；`cargo test -p quant-pivot-research validation::pbo::tests --lib -- --nocapture` 16/16。新增 regression 将 2 条经济路径各重复 8 次，证明 raw ledger 保留 16 trials，而等价类、DSR `N`、representative Sharpe variance、PBO 与 OOS-loss probability 严格不变；同时覆盖 no-trade direct count、全部重复折为一类、orthogonal trial 保留、tamper rejection 与非零常量收益 fail closed。 |
| 2026-08-10 | W2/W7 | schema-10 cross-layer verification | PASS：path-set artifact validator 5/5；`cargo check -p quant-pivot-system-tests --all-targets`；core CPCV orchestrator 13/13。另以真实失败 run 冻结的 observed Sharpe `2.465495593172`、32 periods、skew/kurtosis 和两个 behavioral representatives 新增 DSR regression，证明不降低 `0.95` 门槛时 gate 通过；`cargo test -p quant-pivot-research validation::dsr::tests::behavioral_classes_pass_gate --lib -- --nocapture` 1/1。代码面 `RawTrialCount` 零命中，旧 raw-`M` runtime fallback 与注释均已删除。 |
| 2026-08-10 | W2/W7 | fresh production run `019fe77b-f167-76c1-b949-5d10e6a10c3e` | PARTIAL REAL PASS / NEXT FAIL-CLOSED：governed cycle `4a4f7d66-85e8-589b-a701-0461d77f9f1f` 自然通过 Trigger→Validation。CPCV path-set `827a514a-9780-52f1-a003-947111eb2ef8` 保存 56/56 subject folds、21/21 stateful paths、16/16 raw trials、96/96 trial folds；functional/exact replay 与 schema-10 seal 均通过。真实金融证据：median Sharpe `2.465495593172`、benchmark `0.640727255426`、DSR `0.999969342837`、PBO `0`、raw `M=16`、behavioral `N=2`、representative variance `1.519667129988`；等价类为 even trials 与 odd no-trade trials。Validation artifact `da0a7ff7-cdf2-5be5-a1c2-8ac46830d5d1` 成功，未降低 `0.95` gate。Comparison 随后因错误混同 Champion build-time policy `b3af…` 与 Route decision-time policy `8c2a…` 持续 fail closed，未进入 ShadowBind/activation/report/N+1。 |
| 2026-08-10 | W2/W7 | Comparison dual-policy semantic fix | `FeedbackCycleKey` 与 `ModelServingPreimage` 已明确规定两种 policy identity 可以不同；Comparison verifier 现分别核验 immutable Champion model/contract/spec/family/profile/Route，以及 cycle/evaluation decision-time policy ID+hash，不再读取 Champion serving contract 内的 build-time snapshot 与 decision-time snapshot 比较。错误改为稳定的逐维 mismatch 集合。失败临时容器已清理，证据目录保留；`cargo check -p quant-pivot-core --all-targets` PASS。 |
| 2026-08-10 | W2/W7 | post-Comparison-fix CPCV quality gates | PASS：`cargo test -p quant-pivot-core --lib cpcv_backtest`（13 passed）；`cargo clippy -p quant-pivot-research --all-targets -- -D warnings`；`cargo clippy -p quant-pivot-core --all-targets -- -D warnings`；`cargo check -p quant-pivot-system-tests --all-targets`；`cargo fmt --all -- --check`。trial-grid 参数、fold functional-hash evidence 与 progress lock lifetime 均以领域对象/精确作用域消除 lint，未使用 `allow`。 |
| 2026-08-10 | W2/W7 | fresh production run `019fe7bc-7667-7b93-a9fb-5bc1c34a72a6` | PARTIAL REAL PASS / NEXT FAIL-CLOSED：governed cycle `8edd486a-bd99-58ea-8455-69e1af418757` 自然完成 Trigger→Validation；Cpcv job `ba369601-20e2-5da7-b737-64ade3ff685e` 完成 56/56 subject folds、21/21 stateful paths、16/16 raw trials、96/96 trial folds，path-set artifact `fbc6a496e7e805cebc2c19963f6917d9868e2703d38321405f623abeba2ec977` 保持 median Sharpe `2.465495593172`、DSR `0.999969342837`、PBO `0`、behavioral `N=2`。Comparison job `fcd3d499-9def-593e-b1c1-007250b70bc2` 随后发现第二处 build-time/decision-time policy 混同：Evaluation Dataset 已正确绑定 activation 后的 decision policy，而 replay preimage verifier 错把它要求为 Champion 的 build policy。系统在 ShadowBind 前拒绝发布，run 目录与 backend log 保留。 |
| 2026-08-10 | W2/W7 | dual-policy replay preimage clean break + primary-source cross-check | `ModelServingPreimageService` 现分别接受 immutable model build-policy binding 与 Evaluation decision-policy binding：前者验证 feature/factor/model/calibration/Trade Policy 构建语义，后者验证 Dataset/Source Slice 每行的真实 decision lineage；所有其他契约继续精确匹配。普通 Training/Calibration/backtest 仍传入同一个绑定，因此不存在隐式宽松路径。该边界按 SR 11-7、NIST AI RMF 与 OPE 原始研究复核；不同 logging policy 不会被冒充为普通 replay。无旧 parser、alias、dual read 或 compatibility wrapper。 |
| 2026-08-10 | W2/W7 | `cargo test -p quant-pivot-system-tests --test core_business feedback_comparison_shared_inputs -- --nocapture` | PASS（1/1，99.29s）；先证明 Champion/Challenger 共享同一冻结 Evaluation universe，再激活一个语义兼容但 identity 不同的 decision policy，并在下一 PIT 观察窗口持久化独立 Evaluation Dataset，旧 frozen models 均可被当前 Comparison service 精确 replay。第一次测试尝试因复用相同内容哈希被 `uq_quant_training_dataset_hash` 正确拒绝；夹具随后改为真实 N→N+1 时间窗，不放宽内容寻址唯一约束。 |
| 2026-08-09 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | PARTIAL REAL PASS / QUALITY FAIL-CLOSED；fresh run `019fe60f-c17c-7391-8e4a-323e31b98be0`、governed cycle `f032b3ea-dd5e-5214-9fe9-0df197536fff` 自然完成 Trigger、TruthFreeze、Coverage、Attribution、Drift、RecipePlan、DatasetSeal、Training、Calibration 与完整 Cpcv，随后 Validation 以 `ChallengerRejected` 终止；未进入 Comparison/ShadowBind/permit/activation/report/N+1。immutable validation artifact `76139b32-d8c2-5393-8c87-7a1e46db31cf` 显示 512 samples、label/materialization/PIT、21 paths、median Rank IC `0.738105321317`、PBO `0`、median drawdown `0`、turnover `0.243750`、lower-tail mean return `348.995625`、MinTRL 全部通过；唯一 hard failure 是 DSR `0.312951057713 < 0.95`。不得降低门槛或伪造成闭环通过。CPCV job `9426dcd6-74f2-507f-8b9a-7f754e4ee905`。 |
| 2026-08-09 | W2 | executable exit mark clean break | `PortfolioReplayBacktester` 的 open position 现保存 entry/current lineage，逐 tick 在精确 PIT bid ladder 上执行 full-share sell walk、exit fee 与保守 residual-zero mark；settlement 前 equity/drawdown 不再沿用 entry notional。缺少 PIT mark、clock/market/token 绑定漂移均 fail closed，money 只在 venue micro-USD boundary 量化。 |
| 2026-08-09 | W2 | `cargo test -p quant-pivot-research backtest::runner::tests --lib -- --nocapture` | PASS；12/12。新增 `drawdown_uses_exit_mark` 与 `open_position_requires_mark`，并保持 capital lock、global/report solve parity、deterministic hash 与 zero-allocation 语义。 |
| 2026-08-09 | W2/W7 | exact CPCV durable progress | 删除固定 `cpcv=45` / `trial_grid=75` 的伪进度。subject 以预提交 `C(N,k)` 为 fold total；trial grid 以 `trial_count × canonical_path_fold_count` 为 fold total，并同时持久化完整 trial completion。只有 fold replay 成功才递增 fold，只有 canonical OOS path 绑定/period 校验成功才递增 trial；计数超过预提交 total 即 fail closed。async supervisor 每秒送入既有 latest-value durable progress channel，Rayon worker 不做 I/O。 |
| 2026-08-09 | W2 | `cargo test -p quant-pivot-core service::cpcv_backtest::tests --lib -- --nocapture` | PASS；11/11，含 exact units 首尾快照、单调性与 overflow rejection。 |
| 2026-08-09 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | QUALITY FAIL-CLOSED；fresh run `019fe643-3900-73a3-a0b3-75f5df0d0919`、cycle `ee33a386-d00e-50dd-bd31-304cecb49896` 自然完成 Trigger→Cpcv，Validation 唯一 hard failure 为 DSR `0.193115164043 < 0.95`，未进入 Comparison/ShadowBind/permit/activation/report/N+1。sealed path-set `ce3889a7-50fc-5deb-ac42-b576eb1961bb` 的 representative Sharpe `1.911509413190`、benchmark `2.146545203646`、32 periods、skew `-0.0713071317158016423546195774`、kurtosis `2.2530952494296444503376746887`；raw `M=N=16`、trial Sharpe variance `1.421398660335`。8 个 `RankIcWeightedRanknet` trials 的 Sharpe 为 `2.20103..2.61120`，8 个 `PairwiseRanknet` trials 均为精确 no-trade 零列，因此使用 schema-9 `RawTrialCount` 分支；不得删 trial、降低 DSR 或提高 fixture signal。 |
| 2026-08-09 | W2/W7 | production progress evidence + path-level correction | 上述真实 run 证明 fold counter 单调推进 `12/56→39/56→56/56`，trial grid 推进 `12/96,0/16→…→96/96,16/16`；同时暴露 subject 在 folds `56/56` 后仍执行 21 条 stateful φ-path、却长期没有新进度的真实缺口。现以独立预承诺 `C(N,k)` fold counter 与 `φ(N,k)` path counter 合并成 `cpcv_work;folds=x/56;paths=y/21`，只有完整 path replay 成功才增加 path；任何 counter 越界均 fail closed。 |
| 2026-08-09 | W2 | `cargo test -p quant-pivot-core service::cpcv_backtest::tests --lib -- --nocapture` | PASS；12/12，新增 stateful path 完成计数及 overflow rejection；`cargo fmt --all -- --check` 同步通过。 |
| 2026-08-09 | W2 | DSR/CPCV estimator-axis audit | 发现两个必须先修复、不能靠门槛或 fixture 绕过的方法学不一致：其一，subject DSR 用 21 条 CPCV path 的 median representative return series，而每个 trial/PBO column 只用预选 path 0，导致 observed Sharpe 与 trial variance 不在同一统计 functional；其二，fold scenario `panel_hash` 把 `Validation`/`TrialPathValidation` 的角色 identity 纳入抽样 seed，使相同 objective、相同 train/test populations 也得到不同 residual draws，破坏 paired comparison。依据 [DSR](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf)、[PBO/CSCV](https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf)、[Cawley–Talbot](https://www.jmlr.org/papers/v11/cawley10a.html) 与 [common-random-number multiple comparisons](https://pubsonline.informs.org/doi/10.1287/opre.39.4.583)，最终实现必须让 subject/trials 使用同一预承诺统计 functional，并让同一 fold 的策略候选共享由经济 observation identities/partition contract 派生、与 trial role/performance 无关的抽样流；完整 trial ledger 保留。 |
| 2026-08-09 | W2 | fold scenario common-random-number clean break | fold resampling stream 现只由 methodology、Route、model/calibration/scenario population hashes 与有序 `(decision_at, market_id, token_id)` observation identities 派生；estimator role、model performance 和 residual value 均不进入 seed。完整 residual values 继续单独进入 v2 panel hash，factor lineage 同时承诺 seed 与 panel，因而 paired comparison 与 artifact integrity 两者均成立。`cargo test -p quant-pivot-research portfolio::scenario_model::tests::fold_seed_ignores_performance --lib -- --nocapture` PASS。 |
| 2026-08-09 | W2 | frozen production artifact Pairwise diagnosis | 对 run `019fe643-3900-73a3-a0b3-75f5df0d0919` 的 immutable 512-row Parquet 与真实 factor-head seed 进行一次性 operator diagnosis：`RankIcWeightedRanknet` 得到 mean Rank IC `0.731882569532`、NDCG@20 `0.959704038382`；`PairwiseRanknet` 得到 mean Rank IC `0.759294699652`、NDCG@20 `0.974577938279`，并学习到 `momentum_roc=1`。因此此前 8 个 Pairwise trial 的 no-trade 不能归因于 trainer 失效，必须由折内 calibration/scenario/Trade Policy parity invariant 继续定位；诊断 harness 不进入生产测试面。 |
| 2026-08-09 | W2 | shared selection-path statistical contract | `CpcvMethodologyBinding.trial_path` clean-break 为 subject 与全部 governed trials 的共同 selection path；方法学 hash 升至 v4。DSR 的 observed Sharpe/skew/kurtosis/period count 和 CSCV trial matrix 现在共享该预承诺路径及精确时间轴，21-path 全分布仍用于 robustness/MinTRL。WeightedFactor grid 必须恰好包含一次 serving objective；final statistics 前逐 period 用 `Decimal` 零容差验证 base trial 与 subject return series，任一训练、校准、scenario 或 replay 漂移均 typed fail closed。 |
| 2026-08-09 | W2 | selection-path targeted verification | PASS：`cargo test -p quant-pivot-core service::cpcv_backtest::tests --lib -- --nocapture` 13/13（含 selection path 不随 median representative 改变、逐期 mismatch 拒绝）；`cargo test -p quant-pivot-research validation::pbo::tests::column_constructor_transposes_layout --lib -- --nocapture` 1/1（row-major exact accessor 边界）；`cargo check -p quant-pivot-core -p quant-pivot-research` PASS。 |
| 2026-08-09 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | REAL FAIL-CLOSED；fresh run `019fe689-8548-7db1-8bda-a68b1364f7bc` 的 governed cycle `0a6b7623-1290-55e3-93da-7ddc8d11790a` 自然完成 Trigger、TruthFreeze、Coverage、Attribution、Drift、RecipePlan、DatasetSeal、Training、Calibration，并在 Cpcv job `0697fa29-ed6f-57c9-a3d4-82aee93b17c6` 完成 subject `56/56` folds、`21/21` paths 与 trial grid `16/16`、`96/96` folds。final parity gate 随后精确拒绝 selection period `2026-03-09 17:27:16.363 UTC` 的 base trial：`subject=0.35828670`、`trial=0.48303415`、`trial_id=6`；未进入 Validation/Comparison/ShadowBind，未发布 candidate、permit、activation、report 或 N+1。证据：`target/production-stack/019fe689-8548-7db1-8bda-a68b1364f7bc/backend.log`。 |
| 2026-08-09 | W2 | stochastic identity root-cause + primary-source cross-check | 取证确认 market-level idiosyncratic draw 由包含 calibration/model artifact lineage 的 `scenario_state_hash` 派生，且 MILP 最终稳定键混入随机 `SignalCandidateId` 与 lineage-derived `EconomicTierId`；同经济 estimator 因而会受到不同随机冲击并在等价目标下选择不同 tier。按 [Common Random Numbers](https://onlinelibrary.wiley.com/doi/abs/10.1002/9780470400531.eorms0166)、[multiple-comparison CRN](https://pubsonline.informs.org/doi/10.1287/opre.39.4.583) 与 [1976 simulation experiment analysis](https://journals.sagepub.com/doi/10.1177/003754977602700301) 的 paired-design 原则，clean-break 为三域：共同随机流只绑定预承诺 observation identities/partition/sampling contract；经济状态绑定 residual/calibration 数值；audit lineage 只做完整性证据，不得反向驱动随机抽样或 optimizer tie-break。 |
| 2026-08-09 | W2 | scenario CRN + economic tie-break clean break | `PortfolioScenarioModelArtifact.scenario_random_stream_hash` 成为显式必填契约；fold fit 直接使用 role/performance-independent seed，joint fit 从 PIT bucket identities、ordered Routes 与 sampling contract 派生。stationary bootstrap 与 market idiosyncratic quantile 共用该流，`scenario_state_hash` 继续完整绑定经济状态和 provenance。MILP stable key 只使用 Route/event/market/token/side/tier ordinal，重复经济 offer 直接 contract failure；candidate/tier UUID 与 lineage 不再参与赚钱决策。无旧字段 parser、alias、dual read 或升级迁移。 |
| 2026-08-09 | W2 | stochastic parity targeted verification | PASS：`cargo test -p quant-pivot-research --lib` 591/591，含 audit-lineage-independent market draw、residual-value-independent fold/joint random streams；`cargo test -p quant-pivot-research --test global_portfolio` 3/3，含 mixed Route、brute-force exact optimum 与 input-order deterministic hash；`cargo test -p quant-pivot-core service::cpcv_backtest::tests` 13/13。 |
| 2026-08-10 | W2 | exact replay functional root cause | 为每个 fold 新增 model payload、serving contract、calibration runtime function 与 scenario economic function fingerprints，并在 period-return parity 前逐边界比较。小型 `cpcv_exact_replay` 随即证明 subject 使用 full-window trainer，而 grid trial 使用 prepared-fold trainer，两个算法虽共享目标却不是同一 estimator。clean-break 后 subject 与 trial 均只走 `PreparedWeightedFold::train`；subject preparation 每 fold ephemeral，trial preparation仅在同一 selection split 内共享 immutable objective-independent matrix。同步修复 Source Slice fixture 从多 label 中按 `token_payout_ratio` 名称取 terminal truth，而非依赖排序后的 first label。 |
| 2026-08-10 | W2 | exact replay targeted verification | PASS：`cargo test -p quant-pivot-system-tests --test core_business cpcv_exact_replay -- --nocapture` 1/1；subject/base trial 的 model/calibration/scenario function、56-fold population 和逐期 stateful return 完全一致。 |
| 2026-08-10 | W2/W7 | `cargo xtask production-stack feedback-closure --runs 1` | PARTIAL REAL PASS / QUALITY FAIL-CLOSED；fresh run `019fe72c-b2f1-7d30-851b-a5fa45415aea`、cycle `90254270-d0b1-5dd6-87af-bde5188fe763` 自然完成 Trigger、TruthFreeze、Coverage、Attribution、Drift、RecipePlan、DatasetSeal、Training、Calibration 和完整 Cpcv；CPCV 56/56 subject folds、21/21 paths、16 trials × 6 = 96/96 trial folds，functional parity 与 exact period-return parity 全部通过。Validation 正确拒绝唯一 candidate：observed Sharpe `2.465495593172`、raw `M=16`、schema-9 `N=16`、benchmark `2.219505996364`、DSR `0.705491478788 < 0.95`。取证显示 8 个 trials 为同一盈利 OOS column、8 个为同一 no-trade column；该结果触发 schema-10 non-redundant behavioral class 修复，不降低门槛、不删除失败 trial。未进入 Comparison/ShadowBind/permit/activation/report/N+1。 |
| 2026-08-10 | W2/W7 | fresh production run `019fe7f2-06c5-7713-8f9f-5db122e3116b` | PARTIAL REAL PASS / NEXT FINANCIAL FAIL-CLOSED：governed cycle `c9beaac2-0a24-5e3e-9e7c-f732fb1c4ed0` 自然完成 Trigger→Validation；Cpcv job `766ad3ce-a90d-5e37-86e1-d21c9dfd77e2` 完成 56/56 folds、21/21 paths、16/16 trials、96/96 trial folds。Comparison job `1515bcb7-3318-5e58-94f3-864c3132b70f` 通过 dual-policy preflight 后，在市场轮换后的首个未结仓位 mark 精确拒绝 `token 730002 has no exact PIT liquidation snapshot at 2026-07-09 04:01:25.508 UTC`；未进入 ShadowBind/activation/report/N+1。该证据证明旧 tick execution population 错误兼任 open-position valuation population，未用 mid/last/zero/forward-fill 绕过。 |
| 2026-08-10 | W2 | entry/liquidation plane clean break + primary-source cross-check | 新增强类型 `BacktestLiquidationSnapshot`，`BacktestTick` 分别冻结当前 entry execution 与 allocation-independent retention-token liquidation population。core 先验证同 tick `DecisionBoundary` 完全一致，再从 Source Slice 对每个 token 的全部未结后续边界执行 session/sequence-aware 批量 L2 重放，逐边界绑定 PIT fee 与完整 book hash；缺 book/fee/session/gap、token-market-resolution 冲突或时间漂移全部 fail closed。依据 Polymarket orderbook、IFRS 13 measurement-date exit price、Bion-Nadal dynamic bid-ask pricing 与 stochastic LOB execution 交叉验证；无兼容字段、旧 parser 或 fallback。 |
| 2026-08-10 | W2 | liquidation plane targeted verification | PASS：`cargo test -p quant-pivot-research rotating_universe_retains_marks --lib`；当前模型 cross-section 为空且旧 token 已离开 entry plane 时，独立 liquidation plane 仍完成精确 mark、资本锁定与 settlement。`cargo check -p quant-pivot-core --all-targets` 与 `cargo clippy -p quant-pivot-research -p quant-pivot-core --all-targets -- -D warnings` 同步 PASS。 |
| 2026-08-10 | W2/W7 | fresh production run `019fe827-1697-7b32-b272-8673d24d2d28` | PARTIAL REAL PASS / NEXT GOVERNANCE FAIL-CLOSED：governed cycle `29bc4bc0-b24d-5d76-bd19-52f19d341907` 自然完成 Trigger→Comparison；Cpcv job `7ef092b7-68bb-597f-8dec-fa799a0d5c48` 完成 56/56 subject folds、21/21 stateful paths、16/16 trials 和 96/96 trial folds，证明独立 liquidation plane 经受完整 CPCV；Comparison job `a9f9170f-95f0-5487-b771-48ccbb0e24d2` 在约 6m45s 完成完整 Champion/Challenger family replay，上一轮 rotation mark 缺口不再出现。进入 ShadowBind 准备时，系统正确未发布后续状态，但反复拒绝 active scenario template：`scenario model or binding was not visible at the decision boundary`。取证显示 active binding `bound_at=2026-08-09T20:12:08Z` 早于 cycle freeze `created_at=20:19:49Z`，却晚于历史 `label_cutoff=00:00:00Z`；原调用错误用历史数据时钟同时约束治理绑定时钟。测试容器已显式清理，run log 与 sampling evidence 保留。 |
| 2026-08-10 | W2/W7 | scenario dual-clock semantic clean break | `PortfolioScenarioGenerator::verify_model` 现在必须显式接收 `decision_at` 与 `PortfolioScenarioVisibility`，不再接受含混的单一 `visible_at`。Point-in-Time 同时受 decision clock 约束；HistoricalReplay 分别要求 scenario `as_of <= decision_at` 与 binding `bound_at <= governance_frozen_at`。ShadowBind 用不可变 cycle `label_cutoff` 冻结历史估计数据，用 cycle `created_at` 冻结 active policy/binding 治理图；不删除 PIT 检查、不使用当前时钟、不引入例外 fallback。`portfolio_context` 同步复用该唯一 validator，删除重复双时钟手写判断。 |
| 2026-08-10 | W2/W7 | scenario dual-clock verification | PASS：`cargo test -p quant-pivot-research historical_binding_governance_clock --lib` 证明晚于历史 decision 但不晚于治理冻结的 binding 仅在 HistoricalReplay 合法，Point-in-Time 与越过 governance cutoff 均拒绝；`cargo check -p quant-pivot-core -p quant-pivot-system-tests --all-targets`、`cargo clippy -p quant-pivot-research -p quant-pivot-core -p quant-pivot-system-tests --all-targets -- -D warnings`、`cargo xtask architecture audit-functions`（0 hard / 661 review）与 `git diff --check` PASS。 |
| 2026-08-10 | W2/W7 | fresh production run `019fe859-4c64-7fd1-bb68-9bb2f5a01737` | PARTIAL REAL PASS / NEXT LINEAGE FAIL-CLOSED：governed cycle `7929e71c-81c1-524a-8e0a-e84a83fbdc36` 自然完成 Trigger→Comparison；Cpcv job `0541e8a4-55e2-53de-a9d9-16563b0a1c5e` 完成 56/56 subject folds、21/21 stateful paths、16/16 trials 与 96/96 trial folds，Validation `78bde337-3ddb-552f-82cc-7fe2dc8c9153`、Comparison `cba92c99-8480-5523-af83-9df72d6341e0` 均成功。ShadowBind 准备随后拒绝 `Route scenario evidence differs from its exact model or calibration contract`；数据库逐 Route 取证证明 path set 正确指向 calibrated serving child，而 calibration fit contract 正确指向其 uncalibrated source parent，旧 validator 却错误要求两者为同一 model/artifact/serving hash。未生成 ShadowBind stage event、candidate permit、activation、report 或 N+1；测试容器已按精确 ID 清理。 |
| 2026-08-10 | W2 | calibration edge 双身份 clean break + primary-source cross-check | `PortfolioScenarioRouteModelLineage` 现分别承诺 evaluated calibrated model 与 calibration source estimator 的 version/artifact/serving identities；`PromotedRouteContract` 必须是精确 `ReturnCalibration(parent, artifact)` derivation，ShadowBind 从 repository 加载 parent 并验证 root training lineage、完整 calibration fit binding、Dataset/training-input、Route/profile/horizon、Trade Policy 与 child serving contract。production CPCV path fit 强制 parent/child 三重身份均不同；nested fold 因 calibrator 在折内拟合消费而强制同一 ephemeral estimator，artifact validator 按 evidence kind 拒绝混淆。该设计与 [scikit-learn estimator+calibrator pair contract](https://scikit-learn.org/stable/modules/calibration.html)、[`CalibratedClassifierCV`](https://scikit-learn.org/stable/modules/generated/sklearn.calibration.CalibratedClassifierCV.html) 及 [Niculescu-Mizil–Caruana 2005](https://icml.cc/Conferences/2005/proceedings/papers/079_GoodProbabilities_NiculescuMizilCaruana.pdf) 的独立 calibration estimator 语义一致；没有 alias、旧 payload reader、fallback 或降级校验。 |
| 2026-08-10 | W2 | calibration edge targeted verification | PASS：`cargo check -p quant-pivot-research -p quant-pivot-core -p quant-pivot-system-tests --all-targets`；`cargo test -p quant-pivot-research refit_ --lib` 7/7，直接证明 distinct parent→calibrator→child 可确定性 refit，source alias、source serving drift、path-set drift、兼容 digest 伪造、Route 顺序与 joint coactivity 缺失均 fail closed。 |
| 2026-08-10 | W2/W7 | fresh production run `019fe8a4-7fe3-7650-9b37-56f72f8a538e` | PARTIAL REAL PASS / NEXT METHODOLOGY FAIL-CLOSED：cycle `eacd35d4-4bd5-5ed2-b0be-4d09b4fc03c0` 自然完成 Trigger→Comparison，完整 CPCV 为 56/56 subject folds、21/21 paths、16/16 trials 与 96/96 trial folds，Comparison 于 `2026-08-09T23:04:11.525925Z` 成功。ShadowBind refit 随后拒绝 Weather representative path 仅有 32 个 OOS bucket，而 active stationary-bootstrap contract 的 `max(expected_block_length=8, scenario_horizon=30) × 2` 要求至少 60 个 complete buckets。未降低 block/horizon/floor，未发布 ShadowBind、permit、activation、report 或 N+1；run evidence 已保留。 |
| 2026-08-10 | W2 | stationary-bootstrap time-grid audit + primary-source cross-check | [Politis–Romano stationary bootstrap](https://www.tandfonline.com/doi/abs/10.1080/01621459.1994.10476870)、[Politis–White block-length selection](https://www.math.ucsd.edu/~politis/SBblock-revER.pdf)、[Patton–Politis–White correction](https://www.tandfonline.com/doi/abs/10.1080/07474930802459016) 与 [multi-step forecast error dependence](https://robjhyndman.com/publications/cpts.html) 共同排除把跨日缺口压缩成相邻 bootstrap observation。production joint fitter 现在除数量下限外还要求 canonical time bucket 严格连续；任一缺口 typed fail closed。closure seed 先加载并完整验证 active scenario artifact/binding，再由真实 bucket、horizon、block 与 label-maturity boundary 推导 89 个可用日桶和 96 个 validation groups，不再把固定 32-group CPCV floor 误当成 scenario capacity。 |
| 2026-08-10 | W2/W7 | scenario-capacity targeted verification | PASS：`cargo test -p quant-pivot-research refit_ --lib` 8/8；`cargo test -p quant-pivot-system-tests support::feedback_closure_seed::tests --lib` 21/21；`cargo check -p quant-pivot-research -p quant-pivot-core -p quant-pivot-system-tests --all-targets`；同范围 `cargo clippy --all-targets -- -D warnings`；`cargo xtask architecture audit-functions`（0 hard / 661 review）；`cargo fmt --all -- --check`。fixture 精确证明 96×8、89 个连续 PIT-mature 日桶、50% rolling universe、每 tick 完整 strength/regime factorial、独立 nuisance economics 与 signal monotonicity。 |
| 2026-08-10 | W2/W7 | fresh production run `019fe8e7-519f-7bf0-ba7b-c0edb53a01d6` | PERFORMANCE FAIL-CLOSED：cycle `78c01943-a474-5434-bcae-35a8a65c0891` 在新 768-row/96-group、89 个连续 PIT-mature bucket 合同下自然完成 Trigger→Calibration；Cpcv job `dbcabf32-c613-5ac4-bcec-4d43484110d6` 完成 56/56 subject folds 与 21/21 stateful paths，并在完整 16-trial/96-fold grid 达到 10/16 trials 后触发既有 900-second hard deadline。没有缩小 folds/trials、延长 deadline、发布 partial path set 或进入 Validation/Comparison/ShadowBind。backend evidence：`target/production-stack/019fe8e7-519f-7bf0-ba7b-c0edb53a01d6/backend.log`。 |
| 2026-08-10 | W2/W7 | sealed replay scenario contract clean break | root cause 是 `BacktestScenarioContext` 按 group 深拷贝 400-state model，且 `process_tick` 对同一 fold contract 重复执行全树 canonical hash/Route/binding verification。现由 `BacktestScenarioContext::try_new` 在反序列化/fit 边界完成一次完整验证，将 binding、artifact 与 exact `RepresentedRouteSet` 冻结在单一私有 `Arc`；tick/fold/path clone 只共享 immutable contract，portfolio loop 只能取得 crate-private verified borrow。tampered artifact 仍在构造边界 fail closed，Route drift 仍逐 tick 拒绝；未改变模型 fit、scenario state、MILP、现金流、fold、path、trial 或统计 functional，也未提高 8-vCPU 线程预算。 |
| 2026-08-10 | W2 | sealed scenario targeted verification | PASS：`cargo check -p quant-pivot-research -p quant-pivot-core --all-targets`；`cargo test -p quant-pivot-research scenario_context_ --lib` 2/2（tamper rejection + `Arc::ptr_eq`）；`cargo test -p quant-pivot-research backtest::runner::tests --lib` 15/15（含 report/global solve exact parity、capital lock、liquidation mark、deterministic hash）；`cargo test -p quant-pivot-core service::cpcv_backtest::tests --lib` 13/13。真实性能改善只由下一次独占 fresh production closure 签收。 |
| 2026-08-10 | W2/W7 | fresh production run `019fe928-7a35-7aa0-ab82-789f44562105` | PERFORMANCE FAIL-CLOSED：governed cycle `31bdbffa-ba91-52d9-80f8-94800098851f` 自然完成 Trigger→Calibration；Cpcv job `9ce8d185-48c9-52f-a0db-0394e1962d64` 完成全部 56/56 subject folds、21/21 stateful paths，并推进完整 trial ledger 至 84/96 fold evaluations、14/16 raw trials 后在 900-second hard deadline 精确终止。没有发布 Validation、Comparison、ShadowBind、permit、activation、report、settlement 或 N+1 状态。该 run 证明 sealed scenario contract 已消除旧全树验证热点，但 16 个 trial 仍各自重复执行经济输入完全相同的 path-level stateful global portfolio/MILP replay，故仍未满足既定 compute SLO；backend evidence 保留于 `target/production-stack/019fe928-7a35-7aa0-ab82-789f44562105/backend.log`。 |
| 2026-08-10 | W2 | exact trial economic replay singleflight + primary-source cross-check | [CSCV/PBO 原论文](https://carmamaths.org/jon/backtest2.pdf)要求保留全部真实测试过的 P&L columns，[DSR 原论文](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf)要求以未选择 trial universe、跨 trial Sharpe 方差与有效独立试验数校正 selection bias，[Glasserman–Yao common-random-number guidelines](https://business.columbia.edu/sites/default/files-efs/pubfiles/4261/glasserman_yao_guidelines.pdf)支持在比较系统中共享完全相同的随机/输入条件。因此 16 个 governed trials、96 个 purged fold fits、全部 raw columns 与等价类 audit 均保留；只在完整 path/group time contract、account/policy/solver、entry/liquidation/downside observations、ordered calibrated payout distributions、scenario economic function 与 visibility 的 canonical digest 完全相同时复用最终经济 replay。singleflight entry 对 concurrent waiters 可取消，owner error/unwind 会标记 aborted、移除 key 并唤醒；失败不缓存且允许同 key 重试。cache 仅存活于单次 `TrialGridRun`，subject CPCV、模型训练、calibration、scenario fit、PBO/DSR/CSCV statistic 均不经过该 cache；成功时 `computed + reused == raw_trial_count`，否则 fail closed。 |
| 2026-08-10 | W2 | exact replay-cache targeted verification | PASS：`cargo test -p quant-pivot-core replay_cache_ --lib -- --nocapture` 3/3（8-way singleflight、distinct-key isolation、abort/retry）；`cargo test -p quant-pivot-research economic_hash_excludes_lineage --lib -- --nocapture` 1/1（audit lineage 不改变经济函数，economics mutation 必改 hash）；core CPCV 16/16、scenario context 2/2、backtest runner 15/15；`cargo clippy -p quant-pivot-research -p quant-pivot-core --all-targets -- -D warnings`、`cargo fmt --all -- --check`、`cargo xtask architecture audit-functions`（0 hard / 663 review）与 `git diff --check` PASS。真实 computed/reused 计数与 900-second SLO 只由下一次 fresh production closure 签收。 |
| 2026-08-10 | W2/W7 | fresh production run `019fe968-5090-79b3-8d69-f3e80d5ce6ae` | PARTIAL REAL PASS / NEXT POLICY-CONSTRUCTION FAIL-CLOSED：cycle `0e318070-e6ca-5661-9a0e-e871bf4154c9` 自然完成 Trigger→Comparison。Cpcv job `7fc7350a-de23-5150-a1b2-62fce1ba49c3` 保存完整 56/56 subject folds、21/21 stateful paths、16/16 raw trials 与 96/96 trial fold fits；backend authoritative audit 为 `computed_path_replays=2, reused_path_replays=14`，且完整 CPCV 于约 874.8 秒成功封存，未提高 900-second deadline。Validation 与 Comparison 均成功。ShadowBind job `cd7de270-ae58-571c-9d4e-6ee222ec7f53` 随后拒绝 active fixture policy：`cash_reserve_usd=0` 且三个遗留默认 capital-bucket caps 均超过 20 USD fixture 的 `max_open_capital_usd`。未生成 shadow binding、permit、activation、report、settlement 或 N+1；容器由 harness 清理，backend log 保留。 |
| 2026-08-10 | W2/W7 | initial governed-feedback fixture remediation (rejected as final design) | 根因不是模型质量或 ShadowBind validator，而是 test bootstrap 曾直接写入 `PolicyValidationEvidence::default()`，并把 20/5000 USD fixture budget 覆盖到 total/max-open，却把 reserve 设为 0、保留 3000/6000/9000 USD 默认资本桶。第一轮修复先以 10% reserve、90% max-open 和机械百分比 exposure/tail/bucket caps 使 payload 通过完整 runtime validator；该方案只证明“结构合法”，尚未证明与可执行 workset 的风险容量一致，因此不作为最终金融合同。`bootstrap_policy_bundle` 已永久改为在任何 artifact/revision/database write 前执行真实 `validate_runtime_config()`；invalid fixture 不得再伪造 validation evidence 并把故障推迟到 ShadowBind。 |
| 2026-08-10 | W2/W7 | initial fixture validation evidence | PASS（仅结构门禁）：20 USD envelope 为 reserve/open `2/18`、bucket caps `6/12/18`；5000 USD envelope 为 `500/4500`、`1500/3000/4500`，二者通过 production runtime validator；当时 `cargo test -p quant-pivot-system-tests --lib` 27/27、clippy 与 `git diff --check` PASS。后续真实生产回放证明这组机械 exposure/tail 上限可制造不必要的 MILP infeasibility，故已由下述容量推导合同取代。 |
| 2026-08-10 | W2/W7 | fresh production run `019fe9a2-a76b-7043-910e-62143fd51bf1` | FINANCIAL FAIL-CLOSED：cycle `3164b85a-d734-5ce7-8de7-be9a0c01d9a7` 自然完成 Trigger→Calibration；Cpcv job `06666e08-b1db-5808-a642-c4ef273188a6` 在完整 stateful replay 中约 163 秒即于 lexicographic stage `robust_expected_net` 得到 HiGHS `Infeasible`，未封存 path set，未进入 Validation/Comparison/ShadowBind/activation/report/settlement/N+1。该 run 使用 20 USD budget 下机械缩放的 market/event/category/Route `4/6/10/12`、tail `3/5/4` 与 bucket `6/12/18` 上限；证据将问题分类为 fixture 风险容量与真实 executable workset 未对齐，不能归因于随机 solver 波动，也不能通过 LP relaxation、空 plan 或降低方法学门槛绕过。backend evidence：`target/production-stack/019fe9a2-a76b-7043-910e-62143fd51bf1/backend.log`。 |
| 2026-08-10 | W2 | exact MILP infeasibility witness | `HighsMilpModel` 对任何 non-optimal status 先用 Decimal/newtype exact verifier 检查空选择基线；错误现在明确区分“空组合满足全部 hard constraints，故不可行来自候选/模型约束”与具体空组合 hard-constraint violation。该路径只增加诊断证据，不发布 fallback、partial plan 或伪 optimal。定向测试通过：`cargo test -p quant-pivot-research portfolio::solver_boundary::tests::dense_cvar_lock_feasible --lib -- --nocapture`，人为破坏资本桶后精确返回 bucket witness。 |
| 2026-08-10 | W2/W7 | executable-capacity-derived fixture envelope | 最终 fixture 由真实可执行容量而非任意百分比推导：total budget 保留 10% 显式现金缓冲，max-open 为 90%；每个 tier 固定 1 USD，`governed_capacity = min(max_open_capital_usd, max_open_recommendations × 1 USD)`；market/event/category/Route、CVaR、最大场景损失、drawdown 和每个资本时间桶均以该 capacity 为上限。20 USD closure 因 20 个最大 recommendation 得到 reserve/open `2/18`、capacity `18`；5000 USD fixture 得到 `500/4500`、capacity `20`。这与 [Skaf–Boyd 自融资多期组合](https://web.stanford.edu/~boyd/papers/dyn_port_opt.html)、[Boyd et al. 多期交易约束](https://web.stanford.edu/~boyd/papers/cvx_portfolio.html)和 [BIS 显式流动性缓冲原则](https://www.bis.org/basel_consolidated_guidelines/chapter/LQY/10.htm)一致；同时利用 Polymarket [fully backed outcome tokens](https://docs.polymarket.com/concepts/positions-tokens) 与 [fee schedule](https://docs.polymarket.com/trading/fees) 的可执行语义：fixture 是 fully funded long-only outcome token，最坏损失由 entry principal 加 executable fee 封顶。该结论只适用于此 fixture workset，不被泛化为真实生产风险偏好。 |
| 2026-08-10 | W2/W7 | capacity-derived policy verification | PASS：`cargo test -p quant-pivot-system-tests support::execution_pg_seed::tests::feedback_policy_is_valid --lib -- --nocapture` 证明 20/5000 USD 两种 governed fixture 均由 executable capacity 精确派生并通过完整 runtime validator；`cargo fmt --all`、`cargo clippy -p quant-pivot-research -p quant-pivot-system-tests --all-targets -- -D warnings`、`cargo xtask architecture audit-functions`（0 hard / 663 review）与 `git diff --check` PASS。 |
| 2026-08-10 | W2/W7 | fixture validation debt exposed | 新 pre-write validator 使 `cpcv_exact_replay` integration fixture 在进入 CPCV 前正确失败：Domain vertical 缺 `Domain` feature family、资本桶超过 max-open、`N=4,k=2` 仅产生 3 条 paths 却声明最少 21、且 `max_trials` 小于 family grid 6。该结果是历史 fixture/测试合同缺陷，不是 validator regression；后续必须把测试迁移到完整 `N=8,k=3` governed methodology 或明确的纯算法层 parity test，不能恢复伪 validation evidence、降低生产 gate 或增加 compatibility path。 |
| 2026-08-10 | W2/W7 | fresh production run `019fe9cb-995f-7260-b655-646a9be7c429` | 15-STAGE PASS / POST-ACTIVATION CONFIG FAIL-CLOSED：governed cycle `c815a680-d8c8-576e-8318-52582b0a0ed9` 自然完成全部 `Trigger→TruthFreeze→Coverage→Attribution→Drift→RecipePlan→DatasetSeal→Training→Calibration→Cpcv→Validation→Comparison→ShadowBind→Shadow→Decision`，终态为 `candidate_ready`。Cpcv job `d97dd7f6-013c-54f6-b6c5-1a5e71d87b64` 于约 `890.30s` 封存完整 56/56 subject folds、21/21 paths、16/16 trials 与 96/96 trial folds，且 authoritative cache audit 为 `computed=2/reused=14`；Validation 成功，Comparison job `fff441a3-f904-5942-94c5-9e2c53fcb681` 约 `6m39.62s` 成功；ShadowBind `cf2ff20f-f39e-51fb-9092-79d17d430b93` 成功。candidate `7af86727-afdd-5636-a42a-71e4c7341e96` 的 1000/1000 real ModelRunner shadow observations mean overlap `0.891`、无 hard divergence；Shadow `390a097d-7830-5f7a-bf32-c71de33780cf` 与 Decision `a333c5fd-5c11-5d36-83ff-c19065b4b181` 成功，随后独立 permit/activation HTTP actions 完成。激活后的 mixed-Route ad-hoc report enqueue 被正确拒绝为 `409 ad-hoc report generation is disabled`，故未产生 report/settlement/N+1；容器由 harness 精确清理，log 保留。 |
| 2026-08-10 | W2/W7 | post-activation report admission root cause | API、RBAC 和默认 policy 行为正确：产品默认 `ad_hoc_report_enabled=false`，受治理 endpoint 必须 409；缺陷是 closure fixture 明确承诺使用 ad-hoc canary，却没有在同一 validated policy bundle 中开启该能力。`FeedbackServingFixtureConfig::apply_runtime_controls` 现在原子设置 reconciliation、shadow window、capacity-derived portfolio、all-category selection、shadow threshold 与 ad-hoc admission；FeedbackClosure/Recovery 明确 `true`，普通 GovernedFeedback 明确 `false`。没有改变全局默认、绕过 REST/RBAC/durable queue 或直接写 report row。定向 policy test与 system-tests clippy PASS。 |
| 2026-08-10 | W2/W7 | stage-deadline/SLO research correction | `900s` 和 mixed-report `5m` 都不是金融或统计黄金常数。Google [SLO guidance](https://sre.google/sre-book/service-level-objectives/)要求从用户/workload 目标反推 SLI/SLO，以高分位分布而非均值管理尾延迟；[deadline guidance](https://sre.google/sre-book/addressing-cascading-failures/)明确 deadline 需要在昂贵请求、资源占用、整体调用树和 cancellation propagation 间权衡；[production best practices](https://sre.google/sre-book/service-best-practices/)要求用 load testing 而非传统常数建立 capacity ratio。Kubernetes [Job activeDeadlineSeconds](https://kubernetes.io/docs/concepts/workloads/controllers/job/)只规定终止语义，不给数值。金融 [SR 11-7](https://www.federalreserve.gov/supervisionreg/srletters/sr1107a1.pdf) 与 Basel [MAR30](https://www.bis.org/basel_framework/chapter/MAR/30.htm)要求概念正确、完整/独立/持续验证和 material-risk coverage，同样不规定 CPCV wall-clock。最终 clean-break 必须以 workload class、冻结输入规模/复杂度、资源 profile、调度 cadence、下游预算、历史 p95/p99、冷启动与 checkpoint/cancellation 余量生成可审计 deadline contract；任意 literal timeout 只可作为尚未校准的 bootstrap ceiling，并必须从发布门禁移除。 |
| 2026-08-10 | W2/W7 | functional/performance deadline separation | 取证确认 `production-stack feedback-closure` 运行 unoptimized debug binary，而现有 release `cpcv_orchestration_gate` 只使用 policy stub、零 scenario/MILP/trial-grid economics，正式性能账本也声明它不能签收完整 SLO。因此 fresh-stack correctness gate 将 recipe deadline 重命名为 `CLOSURE_COMPUTE_LIVENESS_SECS=30m`、cycle-to-bind liveness 设为 1h、mixed-report liveness 设为 15m，并明确只负责 cancellation/deadlock containment；不再把它们称作 production budget。production recipe 仍有 finite deadline/heartbeat/cancellation/fail-closed。真实发布性能必须由固定 Linux/CPU/RAM、release profile、完整 scenario/MILP/trial-grid 的独立 benchmark，以分阶段 work units、重复运行分布和预声明 p95/p99 签收。fmt、policy unit、system-tests clippy 与 diff check PASS。 |
| 2026-08-10 | W2/W7 | fresh production run `019fea07-0e94-7bc0-ba3b-1e1f33ee50d6` | 15-STAGE PASS / REPORT-WORKER LIVENESS FAIL-CLOSED：governed cycle `4a5bc815-b5fb-5144-b4e2-6600c790cfb9` 自然完成全部 15 stages，Cpcv job `3e85c38d-7f1e-5e3f-997b-f9eb40813c29` 封存 56/56 folds、21/21 stateful paths、16/16 trials、96/96 trial folds，authoritative replay audit 为 `computed=2/reused=14`，path artifact hash 为 `blake3:f5c824596d497a2703f9fd6e917255941c5e64be44caa6b4244969bc7fcef0ba`；Validation、Comparison、ShadowBind、1000/1000 real shadow observations（mean overlap `0.889`、0 hard divergence）、Shadow 与 Decision 均成功。独立 permit `090940a0-16f6-5aac-99df-1fb2db7b2f12` 签发且 candidate `53fd49bc-f880-5e47-9259-ae51ee5cebe3` 原子激活；随后 mixed-Route report run `019fea31-6f2b-7782-957f-b6e919672d35` 在 900 秒 functional ceiling 内始终保持 `queued`、从未获得 lease。运行时 status 精确显示 `operational_phase=market_data_connecting`、`report_generation_eligible=false`，reasons 为 `operational_phase_blocks_reports` 与 `no_serving_evidence`；因此未伪造 claim、直接调用 builder、发布 report 或写 settlement/N+1。harness 终止后只清理自身 PostgreSQL/Redis/ClickHouse/MinIO，用户既有容器未触碰。 |
| 2026-08-10 | W2/W7 | report capability + deterministic CLOB root-cause remediation | 两个独立根因均 clean-break：production-stack 原先把 `clob_ws_url` 固定为不可达 `127.0.0.1:1`，且长计算期间没有任何 normalized market frame，现改为真实 WebSocket 握手/分片订阅/initial book/DataPipeline 路径并每 5 秒发送当前 venue-time book keepalive，运行期不得绕过 production readiness；server shutdown 有 cancellation、join、error propagation 与 Drop containment。其次，pre-discovery capability 不再从 `enabled_categories=[]` 推断 Pooled/Crypto/Weather 三 Route 必须全部已有 Champion；它只证明至少一个 serving entry 存在，实际 represented Route set 仍在 immutable venue eligibility 后冻结，并由 report transaction 对每个 represented Route 的 Champion、Calibration、Trade Policy、Research Profile 与 scenario artifact 原子 fail closed。没有把 `MarketDataConnecting` 视为可报告、没有 relaxed freshness、没有 fixture-only capability override。 |
| 2026-08-10 | W1/W5/W7 | post-root-cause targeted verification | PASS：`cargo check -p quant-pivot-system-tests --all-targets`；`cargo test -p quant-pivot-core all_scope_allows_discovery -- --nocapture` 1/1；`cargo test -p quant-pivot-system-tests keepalive_refreshes_books -- --nocapture` 1/1，真实客户端观察 subscription initial book 后继续收到 keepalive book；`cargo xtask config render` 与 `cargo xtask config audit` PASS，314 个显式 Deploy descriptors 在两份 strict TOML 中完整、唯一且无 descriptor-purpose inference；`cargo fmt --all -- --check` 与 `git diff --check` PASS。 |
| 2026-08-10 | W7 | fresh run `019fea4e-d4f7-70b3-8345-321e10271e8b` early readiness audit | OPERATOR-ABORTED BEFORE EXPENSIVE DAG：production binary 已启动且上一修复使 `no_serving_evidence` 消失，但 authenticated `/api/system/status` 仍显示 `market_data_connecting`、0 shards/0 messages；Gamma reconciliation 的真实订阅统计为 `selected_tokens=0`。根因是 closure Gamma event/market 的 `endDate` 仍硬编码为 `2027-01-01`，落在 production `engine_subscription_window_hours=72` 之外，故 `WsSubscriptionCoordinator` 正确排除全部候选。主 cycle `2d2799d5-c6e3-5d79-a7e9-2ae3f2f4d329` 仅到 Coverage 时即主动终止自有 disposable harness，避免重复执行完整 CPCV；未伪造 readiness 或继续发布。 |
| 2026-08-10 | W7 | catalog/WS lifecycle single-clock contract | closure 启动时冻结唯一 `report_resolves_at`，同一 timestamp 同时进入 Gamma event/market 与 PostgreSQL mixed-Route report cohort；48-hour horizon 落在 72-hour production prewarm window内并仍覆盖 multi-day capital bucket。通用 Empty/Browser upstream 同样删除静态 2027 deadline。所有 production-stack fixture 现在在任何 DAG 前经认证轮询 `/api/system/status`，强制 `operational_phase=operational`、fresh market data、`ws_shards.total > 0`、0 disconnected 和 fixture market-count floor；一分钟未满足即携最后完整 status fail closed。 |
| 2026-08-10 | W7 | operational readiness verification | PASS：`cargo test -p quant-pivot-system-tests production_stack::tests --lib -- --nocapture` 4/4；`cargo check -p quant-pivot-system-tests --all-targets`；加强后的 `cargo xtask production-stack verify --runs 1` 在独立 fresh stack 打印并签收 `fixture=Empty active_markets=1 ws_shards=1 last_message_age_ms=101`，随后完成精确自有容器清理。该 probe 只证明真实 Gamma→subscription→WebSocket→DataPipeline→status readiness，不替代后续完整 15-stage/N→N+1 closure。 |
| 2026-08-10 | W7 | fresh run `019fea75-47ed-73a2-8d18-bf3c82f7dd58` shadow-window fail-closed | REAL PARTIAL PASS / RESOURCE-CONTENDED SHADOW REJECTION：真实 readiness 为 `active_markets=2672, ws_shards=1, last_message_age_ms=97`；cycle `e968e6e9-0dd9-5223-9d78-85b19d38b1a8` 自然完成 Trigger→ShadowBind，完整 Cpcv 再次封存 56/56 folds、21/21 paths、16/16 trials 与 96/96 trial folds，Validation、Comparison、ShadowBind 均成功。冻结 shadow window 为 `07:44:40.629043Z..07:49:40.629043Z`，同机 `quant-pivot-models` 全量 rustc 编译覆盖该窗口；production Decision 在窗口结束时只看到 882/1000 条 in-window comparison，正确发布 `shadow_insufficient / no_action`。harness 完成第 1000 条时为 `07:50:16.115106Z`，超窗 35.486 秒并拒绝 permit/activation/report/settlement/N+1。不得倒灌晚到观察、扩大 fixture window 或把 no-action 改成通过；最终签收必须在没有并发 build 的独占 fresh runs 中完成，并保存 shadow throughput/duration。 |
| 2026-08-10 | W3 | CVaR/SAA scenario-count primary-source cross-check | [L.A. et al. finite-sample CVaR concentration](https://proceedings.mlr.press/v119/l-a-20a/l-a-20a.pdf)、[chance/CVaR sample-approximation survey](https://par.nsf.gov/servlets/purl/10110826) 与 [JMLR empirical-risk concentration](https://www.jmlr.org/papers/volume23/20-965/20-965.pdf) 均表明样本需求取决于 tail probability、distribution/tail assumptions、accuracy/confidence 与 estimator；没有可把 400、900 秒或任意单一数字证明为普适黄金常数的公式。因此保留当前 promoted template 的 320 PIT + 40 calibration + 40 stress，不为速度降低方法学覆盖；部署容量改为不可拆分的 10k tiers / 400 scenarios / Top20 workload tuple。 |
| 2026-08-10 | W3 | parameterized portfolio capacity study | 同一 deterministic release harness：2k/250/Top100/30 PASS 18.633s；3k/300/Top100/30 在 robust stage 正确 timeout，60s PASS 38.487s；3k/400/Top20/30 PASS 18.439s；5k/400/Top20/60 PASS 35.598s；旧两矩阵 10k/400/Top20/180 PASS 77.471s、RSS 2.364GB。结果证明 30s 默认会误杀真实工作类，也证明独立 max fields 的笛卡尔积不能冒充已验证容量。默认改为 180s bootstrap liveness ceiling，并新增 deploy `max_top_n=20`、`max_scenarios=400`；固定 Linux 重复分布前不得称为 production p99 SLO。 |
| 2026-08-10 | W3 | single-model publishable solve + deterministic isolation proof | Objective lock 使用初始固定为 0 的 relaxation column；tie uniqueness 后统一解锁，同一 HiGHS model 完成所有 leave-one-out re-optimization。`SolverEvidence` 强制 lexicographic builds=1、additional marginal builds=0、marginal reuses=TopN；每个 solve 仍只接受 Optimal/zero gap 并执行 Decimal verifier。[Isolation Lemma 原始论文](https://people.eecs.berkeley.edu/~vazirani/pubs/matching.pdf)只支持随机权重以非零概率隔离，不足以单独签发确定性金融结果；因此稳定 identity weights 只负责产生候选解，每轮追加 exact lock 后另做最优 Hamming-distance uniqueness proof。新测试构造 pass-0 最大权重碰撞并证明 pass-1 精确隔离，禁止把 hash 碰撞概率冒充确定性。 |
| 2026-08-10 | W3/W5 | portfolio gate/config verification | 已构建 binary 同输入两次：77.910s / 78.350s、plan hash 均为 `blake3:efa064ebbae3faae5ba7033787a23034d45b14dbb7c64ae61af206bea421d298`、外部 RSS 约 2.17GB。solver boundary 2/2、global optimal/determinism 3/3、fail-closed 10/10、models config 107/107 PASS；`config render --check` 与 `config audit` PASS（315 descriptors）。旧 `report_compute_gate` 已破坏式正名为诚实的 `report_funnel_gate`，真实 `portfolio_compute_gate` 已进入 full profile 十次证据矩阵；fixed Linux artifact 仍是独立未签收项。 |

## 12. 必须通过的命令

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask architecture audit-functions
cargo xtask architecture check
cargo xtask config render --check
cargo xtask config audit
cargo test --workspace
cargo xtask production-stack verify --runs 2
cargo xtask production-stack feedback-closure --runs 2
pnpm -C ui lint
pnpm -C ui check:type
pnpm -C ui check:config-api
pnpm -C ui test:unit
pnpm -C ui test:e2e
```

另行执行 report compute、scenario generation 与 MILP SLO benchmark。候选上限下无法稳定最优求解时，
优化算法/模型规模，不增加 fallback。

## 13. 操作边界

- 不自动销毁真实 PostgreSQL、ClickHouse、Redis 或 artifact store。
- 不读取、复制、生成或提交真实 credential。
- 不执行 production cutover 或不可逆 lifecycle seal。
- 不创建版本化升级 migration、legacy archive 或历史 payload converter；只更新未投产项目的 clean-install
  bootstrap snapshot。真实数据库 reset 必须另获操作者授权。
- 不创建 commit、不 push。
- 工作树中用户已有改动必须保留并避开。
