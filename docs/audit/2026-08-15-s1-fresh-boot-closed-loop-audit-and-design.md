# S1 Fresh-Boot 闭环审计与优化设计（2026-08-15）

> **Superseded**：本文件只保留为审计历史。当前实现合同以 runtime 类型、`docs/operations/runbook.md` §7.6 和最新 S1 生产闭环为准。

> **范围**：审计原文 S1（200 天冷启动建立在错误前提上）的落地质量；对照 [`/Users/eason/Downloads/PLAN.md`](/Users/eason/Downloads/PLAN.md) 与当前仓库实现；给出可执行的破坏式优化设计。
> **前序**：[`2026-08-13-full-system-deep-audit.md`](2026-08-13-full-system-deep-audit.md) §1。
> **方法**：实现交叉验证 + Polymarket / Envio / Polygon 官方文档 + 特征存储/训练-服务偏斜文献。不接受“契约写了但未接线”。
> **已拍板决策**（2026-08-15）：
> 1. 数据就绪后做**自动编排**：回填完成 → dataset → 训练 → bootstrap → 可出报告；进度/清单必须是**真状态机**。
> 2. 空 `enabled_categories` **保持现状**（空列表 = 全部受支持类别）。
> 3. 预测向量严格按 route `feature_contract`；live L2 **只**用于 sizing / 可成交性。允许 bootstrap 报告与 FullL2 报告不再共享同一条 `build_features`。
> **立场**：生产级、语义精准、零兼容 shim / re-export。时间 SLO 永远让位于 fail-closed。

---

## 0. 一句话结论

**数据面已经接近机构级，而且在若干点上优于原始 S1；但 serving 信息体制、fresh-boot 编排和 Admin UX 没有闭环。当前不能验收为“S1 已落地”。**

更精确地说：

- 系统现在**可以**把 Polygon finalized 成交历史双源见证后写入唯一事实层，并训出一个 L2-free bootstrap 模型。
- 系统现在**不能**保证该模型按自己的合同被服务，也不能在回填完成后自动走到第一份语义正确的 Published RecommendationReport。
- 操作员看到的仍是 200 天世界观。`BootstrapActivationProgress` 的阶段名在撒谎。

本文件上半是缺陷审计，下半是按已拍板决策写出的优化设计。设计否定“在现有 `build_features` 上加 if”和“用进度枚举假装编排”。

---

## 1. S1 到底要解决什么

原始审计 S1 的错误前提是：“Polymarket 历史不可回填，所以必须本地录 200 天 L2 才能训第一个模型”。

分层可得性（审计原文，本轮复验后仍然成立）：

| 数据 | 可回填？ | 权威 |
|---|---|---|
| 结算标签 | 可以 | 链上 resolution / `token_payout_ratio` |
| 成交流 | 可以 | V1/V2 CTF + NegRisk 的 `OrderFilled` / `OrdersMatched` |
| 价格历史 API | 可以，但**不是 PIT 事实** | CLOB `/prices-history` 是插值图表序列 |
| 市场身份 | 可以 | Gamma active + closed keyset |
| **L2 深度快照** | **不可以** | 只能自录或买第三方存档 |

S1 的业务目标不是放宽 CPCV / DSR / PBO，而是：

> 用可回填的 finalized 成交历史建立 L2-free bootstrap 模型，在 ReportOnly 下用**实时** L2 做 sizing，把首份有效建议从 ~200 个自然日压到回填+训练的小时级。

PLAN 把这个目标收成 12–36 小时（保守 72 小时）fresh-boot。本文件接受该时间预算为**运维目标**，但只在门禁全部满足时成立。

---

## 2. 外部规范交叉验证

### 2.1 Polymarket 合约与事件

对照 [Contracts](https://docs.polymarket.com/resources/contracts) 与 [V2 Migration](https://docs.polymarket.com/v2-migration)：

| 合约 | 地址 | 实现 |
|---|---|---|
| CTF Exchange V1 | `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` | `constants.rs` `ctf_v1`，bootstrap `33_605_403` |
| NegRisk V1 | `0xC5d563A36AE78145C45a50134d48A1215220f80a` | `neg_risk_v1` |
| CTF Exchange V2 | `0xE111180000d2663C0091e4f400237545B87B996B` | `ctf_v2`，bootstrap `84_902_353`（与社区 indexer 一致） |
| NegRisk V2 | `0xe2222d279d744050d28e00520010520000310F59` | `neg_risk_v2`，bootstrap `85_058_176` |

事件语义（与 CTF Exchange 常见 emit 顺序一致，且有单测）：

1. 若干 maker-level `OrderFilled`
2. 一条 aggregate taker `OrderFilled`（`maker = taker`，`taker = exchange`）
3. 一条 `OrdersMatched`

经济成交只来自非 aggregate 的 maker fill。V2 单测：4 events → 2 executions，`sum(participant_notional) = 2 × execution_notional`。这是正确的，应保留。

V1 `last_valid_block = None` 是缺口：官方 V2 已替换 V1 Exchange。继续无限扫描 V1 浪费 attestor 配额，也没有迁移边界证明。设计要求补上官方迁移块。

### 2.2 `/prices-history` 不得进入模型事实层

官方 [`GET /prices-history`](https://docs.polymarket.com/api-reference/markets/get-prices-history) 返回 `{t, p}` 插值点，`fidelity` 默认 1 分钟，价格是 float。没有 `available_at`，没有修订证明，没有 maker/taker/fee。

PLAN 拒绝把它当模型事实，实现也没接。**本文件明确背书并冻结这条否定**：诊断工具可以另做，训练/回测/报告预测向量禁止读取该接口。原始审计 S1 在这一点上弱于 PLAN。

### 2.3 HyperSync 与独立见证

Envio 文档对直连 HyperSync 的要求是：自己处理 rollback guard（比较 `first_parent_hash`）；HyperIndex 才自动 rewind。Envio 对生产的建议是 HyperSync + RPC **fallback**。

本仓库做的是更强的事：HyperSync 提取、非 Envio archive RPC **见证**，count / digest / 首尾 block hash / parent 不一致即 quarantine。这是正确的权威层级，应保留。

缺口：HyperRPC 只在 attestor host 上排除（`envio.dev` / `hypersync.xyz` / `hyperrpc.xyz`）。PLAN 写“完全排除”；extractor 仍可指到同域。设计要求 extractor 也拒绝这些域。

### 2.4 Polygon finality

[Polygon PoS finality](https://docs.polygon.technology/pos/concepts/finality/finality)：Heimdall v2 milestone 给出 2–5 秒 deterministic finality，查询用 `eth_getBlockByNumber("finalized")`。

实现只接到 finalized head；`model_confirmation_blocks = 12` 是额外模型确认，不是替代 finality；`rollback_buffer_blocks = 200` 大于 Envio 对 Polygon 的 150 reorg depth。偏保守，可接受。

缺口：log digest 不含 timestamp；attestor 侧 log 的 timestamp 被写成 0；`model_available_at` 只从 HyperSync header hydrate。block hash 一致后，时间仍未经独立 RPC 见证。设计要求 N 与 N+12 的 timestamp 从 attestor `eth_getBlockByNumber` 回填，并纳入一致性检查。

### 2.5 训练-服务偏斜（第 3 点的文献结论）

Uber Michelangelo / Palette、LinkedIn Feathr、Feast 的共同结论：

> 同一个特征名，在离线训练和在线推理必须解析到**同一套变换**。训练-服务偏斜是生产 ML 里最贵、最难查的一类 bug。

它们同时承认另一种**受控不一致**：离线按历史 as-of 取特征，在线取“现在”的值。那是时间轴差异，不是合同差异。

把 live L2 深度、点差、可成交数量放进**执行/sizing 平面**，是交易系统的正常做法（你不能推荐一张当下买不进的票）。把同一份 live L2 写进**预测特征向量**，而模型是在没有历史 L2 的合同上训练的，是标准的 training-serving skew。

本系统已经有正确的词汇：`PredictiveFeatureCapture` 与 `RecommendationExecutionCapture`。实现把它们焊回了一条 `build_features(FullL2)`。第 7 节给出拆开后的规范。

---

## 3. 实现审计

### 3.1 已真正落地（背书）

| 项 | 证据 | 评价 |
|---|---|---|
| 唯一配置语义 `market_data.finalized_exchange_history` | `MarketDataDeployConfig`；crates 内 `trade_tape_on_chain` 零引用 | 破坏式替换，无 shim |
| HyperSync token 为 `SecretText` | Debug 脱敏、descriptor `DeploySensitivity::Secret` | 正确 |
| 7 张 CH 事实 + 2 张 PG 控制表 | `quant_exchange_log_raw/event/match/market_execution/execution_participant/history_acceptance` + chunk/quarantine | 读者只消费 accepted execution + participant，正确 |
| 旧 trade-tape 路径删除 | worker / entity / repo / CH model 均已删，无 `pub use` | 符合零兼容 |
| 启动先 Gamma 身份再扫链 | `bootstrap.rs`：`IdentitySync` → `gamma_service.sync()` → 注册 history worker | 身份严格，正确 |
| Gamma closed historical keyset | `historical_identity_days` 必须覆盖 retention | 与 UnknownToken fail-closed 配套 |
| 双源 agree 后才 accept | `chunks_agree` + `verify_continuity` + parent 衔接 | 权威层级正确 |
| V1/V2 投影与双计排除 | `execution_projector.rs` + `v2_excludes_aggregate` | 事件语义正确 |
| PIT 字段 | `effective_at` = block ts；`observed_at` = 抓取；`model_available_at` = N+12；读者按后者过滤 | 主体正确 |
| 33 天 activation 优先、200 天 retention 后台 | worker `advance_activation` 有活就跳过 retention | 符合“先出 pooled 报告” |
| 三个 bootstrap profile | `pooled_binary_1h_bootstrap_trade` 等；required_sources 无 `ClobL2` | 类型层正确 |
| `trade.*` 特征族 | `generic/trade.rs` 11 个特征；缺值 `RejectMarket` | 训练侧正确 |
| `ServingAuthority::ReportOnlyWithLiveL2` | `intent_service` 查 profile；bootstrap 禁混 execution route | **执行隔离已闭环** |
| 空 route bootstrap 不要求 24h shadow | `ModelBootstrapPolicyProjection` `shadow: None` | 首次激活语义正确 |
| 参考 scenario | `FinalizedReferenceReturns`；报告 sizing 走 live book | 命名与 PLAN 一致 |
| 报告契约已有 Bootstrap 分支 | `PlannedRecommendationContract::Bootstrap` | composer 侧已分叉，特征侧还没有 |

### 3.2 阻断级缺陷

#### P0-1 报告预测向量硬编码 `FullL2`

```597:597:crates/quant-pivot-core/src/report/builder.rs
                feature_contract: ResearchFeatureContract::FullL2,
```

训练 / historical replay / feature pipeline 入口已经会传 `profile.spec.feature_contract`。报告是唯一把合同丢掉的生产路径。

后果不是“多算一点特征”：

1. `ConfiguredFeatureBuilder::new_for_contract(FullL2)` 展开 PriceBook / TimeSeries / Microstructure / Structural。
2. `NullPolicyEngine::decide` 对**非** model-required 特征仍执行 spec 上的 `RejectMarket`。
3. `BEST_BID` / `BEST_ASK` / `MID` / `SPREAD_BPS` / `AGE_MS` / `CROSSED` / `EMPTY` 都是 `RejectMarket`。
4. bootstrap 模型的可评分宇宙被 live L2 质量偷偷筛过一遍。
5. 持久化向量与 evidence hash 绑定的是 FullL2 schema，训练绑定的是 `TradeBootstrap*`。
6. `classify()` 仍用 book age 参与 DQ，即使模型从不看 book。

同一文件里 sizing 已经按 authority 分叉（`bootstrap_candidate_tiers` vs `full_l2_candidate_tiers`）。**预测平面没有做对称的事。**

#### P0-1b 特征回放也把合同丢掉

`durable_feature_parity.rs` 先从 serving profile 读出 `feature_contract`，立刻用 `ConfiguredFeatureBuilder::new(...)`——而 `new()` 内部写死 `FullL2`：

```330:332:crates/quant-pivot-research/src/features/builder.rs
    pub fn new(config: &FeaturesConfig, domain_config: &DomainConfig) -> QuantResult<Self> {
        Self::new_for_contract(config, domain_config, ResearchFeatureContract::FullL2)
    }
```

然后 `verify_replay_contract` 拿这个 FullL2 schema hash 去对 bootstrap 模型。要么 bootstrap parity 根本过不了，要么“过了”的是错误合同。这是同一缺陷在回放路径上的拷贝。

#### P0-2 12–36 小时没有编排器

History worker 在 activation frontier 追上 `model_head` 时：

```968:975:crates/quant-pivot-core/src/app/exchange_history_worker.rs
    fn publish_ready(&self, target: u64) {
        ...
        progress.stage = BootstrapActivationStage::FeaturesAndLabels;
```

此时**没有**特征、没有标签、没有 dataset。`research_job_worker` 的 23 种 job 不认识 `bootstrap_trade`，也不会在 frontier 完成后入队。`sample_count` 只在有人调用 bootstrap preflight 时写入。`ManualReportReady` 要等第一份报告真正 publish。

因此 PLAN 的时间表（dataset 1–4h、CPCV/训练 2–8h、激活 0.5–2h、报告 1–15min）在代码里不是一条边。它是一份操作员手搓清单。已拍板：改成自动编排 + 真状态机。

#### P0-3 Admin 仍活在 200 天世界观

| 表面 | 实现 |
|---|---|
| 进度 API | `GET /system/bootstrap-activation` 已有 |
| SPA | **零调用** |
| Dashboard | `RESEARCH_HISTORY_GATE_DAYS = 200`，再和 `retention_ready` 取 max |
| 配置文案 | `max_trade_tape_age_secs` / `trade_tape_unavailable`；后端已是 `max_execution_age_secs` |
| first-champion 抽屉 | 存在，但与 ingestion / 编排进度断开 |

`minimum_raw_retention_days()` 仍对全部 6 个 builtin profile 取 max，得到 200。readiness worker 用这个全局值取证。对 **full-L2** profile 这是对的；对 **bootstrap** 这直接否定 S1。

#### P0-4 `NonVerticalPooled` 是死契约（本轮不靠改 selection 语义修复）

Pooled profile 写了 `cohort_contract: NonVerticalPooled`，全仓库无消费点。`category = None`。空 `enabled_categories` 被校验定义为全部类别。

已拍板：**空 `enabled_categories` 保持现状**。因此本设计**不**把“空列表改成必须显式列出”，也**不**在选市层无条件剔除 Crypto/Weather。

诚实修复是另一件事：profile 上的 `NonVerticalPooled` 与真实行为（空=全类别）矛盾。设计要求把 pooled bootstrap 的 `cohort_contract` 改成 `AllEligible`，让封印合同与已拍板语义一致。这会改变 profile content hash，允许破坏。不改 selection 语义。

### 3.3 P1 / P2

| ID | 缺陷 | 说明 |
|---|---|---|
| P1-1 | ETA 首块少计 | `publish_accepted` 在 `accepted_through_block` 为空时把 previous 设成当前 to_block；首个 chunk 只记 1 块。5% 重算 ETA 会立刻 Violation |
| P1-2 | `quarantine_reason` 字符串匹配 | `detail.contains("token")` 等。错误分类会让人修错方向 |
| P1-3 | UnknownToken 停整段 frontier | fail-closed 正确；缺 per-token 可见性与可恢复投影，不是改成跳过 |
| P1-4 | `history_client` 无 hermetic 测试 | `chunks_agree` / rewind / NegRisk / acceptance 门禁不在默认 `cargo test` |
| P1-5 | 阶段枚举被 worker 覆盖 | 编排若继续用同一个 `BootstrapActivationStage`，history 与训练会互相踩 |
| P2-1 | PIT 时间只信 HyperSync | 见 §2.4 |
| P2-2 | V1 无 `last_valid_block` | 见 §2.1 |
| P2-3 | HyperRPC 只禁 attestor | 见 §2.3 |
| P2-4 | raw/event/match 只写不读 | 不影响首份报告；审计重放能力未完成 |
| P2-5 | `ConfiguredFeatureBuilder::new` 默认 FullL2 | 任何忘记 `new_for_contract` 的调用都是偏斜入口，应删除该构造 |

### 3.4 对 PLAN 的保留与否定

**保留**

- 拒绝 `/prices-history` 作模型事实。
- HyperSync 提取 + 独立 RPC 见证，不是 fallback。
- 删除 trade-tape，fresh boot 唯一 schema。
- `ServingAuthority` 与空 route bootstrap。
- 33 天 activation + 长期 retention 后台，不阻塞已激活的 pooled ReportOnly。
- 统计门禁不放宽。

**否定 / 改写**

- 93/100 不必做成第三条 frontier。Weather bootstrap `required_days = 100`，Crypto 约 93。按 **profile runway** 解锁，不要再加配置旋钮。实现现在的 33+≥200 更干净，但 readiness 必须按 profile 拆开。
- “12–36 小时具备报告能力”不能再写成“数据面完成就算”。必须是编排状态机到达 `ReportEligible`。
- Progress 阶段名必须等于真实工作。禁止在 frontier 完成时跳到 `FeaturesAndLabels`。
- Dashboard 200 天硬门对 bootstrap 必须删除。

---

## 4. 已拍板产品决策（约束设计）

### 4.1 自动编排

回填完成之后，系统自己走完：

```text
ActivationFrontier accepted
  → DatasetBuild（pooled_binary_1h_bootstrap_trade）
  → ModelTrain
  → CpcvBacktest（PredictiveUtility）
  → ModelCalibrationFit
  → FeatureParity
  → 参考 scenario fit
  → 空 route bootstrap 事务
  → ReportEligible
  → 可手动 ad-hoc；下一个 300s schedule 自动出报告
```

质量门禁全部保持。时间不够就停在当前阶段并告警，**禁止**降阈值、跳 attestation、跳 parity、跳 bootstrap preflight。

后续 champion 替换仍走 24h shadow + permit + activate。自动编排只覆盖**空 route 的首次 bootstrap**。

Crypto / Weather bootstrap 是独立编排 run，等各自 `required_days` 被事实覆盖后启动，不阻塞 pooled。

### 4.2 空 `enabled_categories`

保持：空列表 = 全部受支持类别。选市、训练、报告宇宙继续用这条规则。

不在本设计中引入“空列表非法”或“pooled 无条件踢掉 vertical”。

因此 pooled 模型**可以**看到 Crypto/Weather 市场，只要它们出现在 catalog 且通过该 profile 的特征合同。这是已接受的产品语义，不是遗漏。profile 合同必须改成承认这一点（`AllEligible`）。

### 4.3 预测合同与 live L2

见 §7。结论先行：

> 预测特征向量的 schema 必须等于 `route.contract.feature_contract`。live L2 属于执行捕获平面。bootstrap 报告与 FullL2 报告**不得**再共享一条物化 `build_features`。共享的是报告事务与 sizing/MILP，不是特征合同。

---

## 5. 目标架构（闭环）

```text
                    Polygon finalized
                           │
              ┌────────────┴────────────┐
              │                         │
         HyperSync                 archive RPC
         (extractor)               (attestor)
              │                         │
              └────────────┬────────────┘
                     exact agree
                           │
            CH facts + PG chunk/quarantine
                           │
              FreshBootOrchestrator（真状态机）
                           │
         dataset → train → CPCV → cal → parity
                           │
              empty-route bootstrap txn
                           │
                    ReportEligible
                           │
              ┌────────────┴────────────┐
              │                         │
     PredictiveFeatureCapture   RecommendationExecutionCapture
     (per-route contract)       (live L2 + account + scenario)
              │                         │
              └────────────┬────────────┘
                    Published report
                    OrderIntent 永禁
```

两平面的边界是本设计的不变量。任何把 live book 细胞写进 bootstrap `FeatureVector` 的路径都是回归。

---

## 6. 真状态机：Fresh-Boot 编排

### 6.1 为什么现有进度对象不能继续当状态机

`BootstrapActivationProgress` 是进程内 `ArcSwap` 快照，重启即失。history worker、bootstrap preflight、报告 delivery 三个写入者抢同一个 `stage` 字段。没有持久化、没有转移表、没有幂等边、没有失败恢复。

自动编排若继续往这个结构里塞阶段，会得到另一份会撒谎的进度。

拆成两个对象：

| 对象 | 生命周期 | 职责 |
|---|---|---|
| `ExchangeHistoryFrontierProgress` | 进程快照 + PG chunk 可重建 | 只描述提取/见证/投影/quarantine/ETA |
| `FreshBootOrchestrationRun` | **PG 持久化状态机** | 描述一条 profile/route 从数据就绪到 ReportEligible |

HTTP `GET /system/bootstrap-activation` 改为返回二者的只读投影，而不是一个被覆盖的枚举。

### 6.2 持久化状态机

Fresh boot 是唯一支持路径，允许新表，禁止兼容 view。

建议表：`quant_fresh_boot_run`

| 列 | 类型语义 |
|---|---|
| `run_id` | UUID v7 |
| `profile_ref` | 内置 profile 身份 + content hash |
| `route` | `BuyModelRoute` |
| `stage` | `FreshBootStage` ActiveEnum |
| `status` | `running / blocked / succeeded / failed` |
| `activation_frontier_through` | 已接受 activation 头 |
| `source_slice_hash` | 本 run 封印的 source-slice |
| `dataset_id` / `model_version_id` / `path_set_id` / `calibration_id` / `parity_run_id` | 后继身份，可空 |
| `bootstrap_policy_activation_id` | 成功后填写 |
| `manual_report_ready_at` / `first_report_id` / `next_scheduled_report_at` | 报告面 |
| `blocked_reason` | 强类型枚举，禁止自由字符串当状态 |
| `idempotency_key` | 由 `(profile_ref, activation_window, policy_generation)` 派生 |
| `updated_at` | CAS 用 |

转移必须是显式函数，例如 `FreshBootRun::advance(from, event) -> Result<Self>`。未知事件、逆跳、跳过门禁 = 编译期或运行期硬失败。

### 6.3 `FreshBootStage`（真实工作，不是愿望）

```text
IdentitySync
ActivationExtracting
ActivationAttested          // 当前 chunk 双源一致
ActivationProjected         // 投影写入且 acceptance 可见
ActivationFrontierReady     // 33 天窗口全部 accepted，quarantine=0
DatasetQueued
DatasetRunning
DatasetReady
TrainingQueued
TrainingRunning
TrainingReady
CpcvQueued
CpcvReady                   // PredictiveUtility，门禁通过
CalibrationQueued
CalibrationReady
ParityQueued
ParityReady                 // 合同必须是该 profile 的 feature_contract
ScenarioReady
BootstrapPreflight
BootstrapCommitted
ReportEligible              // ad-hoc 可点；schedule 已武装
FirstReportPublished        // 第一份 Published 报告的事实，不是预告
```

并行、不阻塞 pooled 的旁路：

```text
RetentionExtracting → RetentionReady   // 200 天，只解锁长期 raw / 后续 full-L2
CryptoRun / WeatherRun                 // 独立 FreshBootRun，required_days 满足后启动
```

禁止出现的阶段：

- 在没有 dataset 时进入 `DatasetReady`
- 在没有模型时进入 `TrainingReady`
- 把 `FeaturesAndLabels` 当作 frontier 完成的别名
- 用 48h/72h SLO 作为转移条件

`Quarantined` / `OrchestrationFailed` 是 `status`，不是“跳过用的 stage”。blocked 时 `stage` 停在失败发生的那一格，`blocked_reason` 说明为什么。

### 6.4 转移事件（唯一合法输入）

| 事件 | 源 | 效果 |
|---|---|---|
| `IdentitySyncSucceeded` | `gamma_service.sync()` | → `ActivationExtracting` |
| `ChunkAttested { chunk_id }` | history worker | 停在 `ActivationAttested` 直到投影完成 |
| `ChunkAccepted { chunk_id }` | history worker | 更新 frontier；未到头则回到 `ActivationExtracting` |
| `ActivationWindowComplete` | history worker（头追上且 quarantine=0） | → `ActivationFrontierReady` → 入队 DatasetBuild |
| `ResearchJobSucceeded { kind, id }` | job engine | 按 kind 前进一格并入队下一 job |
| `ResearchJobFailed { kind, code }` | job engine | `status=blocked`，不跳 |
| `BootstrapCommitted { activation_id }` | governance | → `ReportEligible`，写 `next_scheduled_report_at` |
| `ReportPublished { report_id }` | fact delivery | → `FirstReportPublished` |
| `ProviderMismatch` / `UnknownToken` / … | history | `status=blocked`，`blocked_reason` 对应枚举 |

下一 job 的 `parent_job_id` 必须指向上一成功 job。禁止“编排器凭空塞一个模型 id”。

### 6.5 编排器，不是新的研究运行时

不要再造一套 job 系统。`ResearchJobKind` 已有 `DatasetBuild` / `ModelTrain` / `CpcvBacktest` / `ModelCalibrationFit` / `FeatureParity`。

新增关键任务 `TaskId::FreshBootOrchestrator`：

1. 从 PG 加载或创建 `FreshBootRun`（pooled 一条；crypto/weather 各自一条，未到 runway 则不创建）。
2. 订阅 history 接受事件与 research job 终态（已有 event bus / job engine）。
3. 在 `ActivationFrontierReady` 用系统身份入队 DatasetBuild，params 封印 `pooled_binary_1h_bootstrap_trade` 与 activation 窗口。
4. 每个成功边入队下一个 kind，params 只引用上一边的内容寻址身份。
5. Parity 成功后调用现有 `ModelRouteGovernanceService::bootstrap`。不复制 bootstrap 事务。
6. `ReportEligible` 后不自动降低 TopN、不自动改 schedule。默认 20 / 300s / poll 1s 保持。
7. 是否自动 enqueue 第一份 ad-hoc：要。编排到达 `ReportEligible` 后 durable enqueue 一份 ad-hoc，RBAC 记系统身份；人工仍可再点。失败不得回滚 bootstrap。

### 6.6 系统身份（不是绕过治理）

自动 bootstrap 仍是 `BootstrapModelRoute` 事务：空 route、质量门、parity、scenario binding、ReportOnly、ad_hoc 开启、至少一条 enabled schedule。

新增封印系统 actor，例如：

- `requested_by = "system:fresh_boot_orchestrator"`
- `acting_role` = 专用 RoleCode（不是 SuperAdmin 借用）
- `reason_code = "fresh_boot_auto_bootstrap"`
- `idempotency_key` = run 上已有的键

WORM 审计必须能回答“谁激活了首个 champion”。质量门失败时编排 blocked，等人修数据或证据，**没有**管理员“强制下一阶段”按钮。

人工 first-champion 抽屉保留，作为同一幂等键的重放入口：若编排已提交，UI 只展示 receipt；若编排 blocked，UI 展示 `blocked_reason`，禁止用另一套证据手工绕过。

### 6.7 Dataset / 训练窗口

Pooled run 的 dataset 窗口 = activation frontier 的有效时间闭区间，再减去 profile 的 lookback / horizon / embargo。不得为了凑样本数把窗口扩到 retention 未接受的区块。

样本不足：停在 `DatasetReady` 之前，`blocked_reason = InsufficientMatureLabels`（或同等枚举）。禁止降 `minimum_mature_labels` / CPCV 门。

Crypto/Weather run 的窗口用该 profile 的 `required_days`，且只使用 retention 已接受且双源一致的事实。

### 6.8 Readiness 拆分

删除对 bootstrap 的全局 200 天硬门。

| 消费者 | 规则 |
|---|---|
| Full-L2 profile / 长期 raw | 继续 `minimum_raw_retention_days()` = 200 |
| Pooled bootstrap run | `profile.required_days()`（约 33）且 activation frontier 覆盖 |
| Crypto bootstrap run | 该 profile `required_days`（约 93） |
| Weather bootstrap run | 100 |
| Dashboard | 按**当前可服务 route 的 authority** 显示，而不是一张 200 天表 |
| `RESEARCH_HISTORY_GATE_DAYS` | 删除常量，或降为 full-L2 专用 |

`ResearchReadinessEvidenceProducer::capture` 必须能按 profile 取证，不能只 capture 一次 200 天。

### 6.9 ETA / SLO

`blocks_processed` 按 chunk 闭区间 `[from, to]` 累加，禁止 `unwrap_or(block)` 把首块吃掉。

SLO 只评估 **ActivationFrontierReady 的数据面** 与 **ReportEligible 的编排面** 两段。48h warning / 72h violation 是告警，不是转移条件。预测超过 72h 仍必须跑完 attestation。

---

## 7. 预测合同 vs live L2 sizing（第 3 点的规范结论）

### 7.1 结论

**高质量最佳实践不是“一条 `build_features` 里算完所有东西再按模型名字切片”。**

那是 Feast/Michelangelo 文献里点名的偏斜形态：服务端用了训练时不存在的变换与拒绝规则，再假装模型只看自己的列。

正确结构是**两个平面、两次物化、一个报告事务**：

1. **PredictiveFeatureCapture** — 每个 ready route 一次，schema = 该 route 封印的 `ResearchFeatureContract`。
2. **RecommendationExecutionCapture** — 全报告一次（或每个候选一次），输入是 live L2 + 账户 + 参考 scenario；输出是能否 sizing、入场限价、深度、滑点、经济 tier。

`DefaultReportBuilder::build_report` 可以继续作为唯一编排函数。它内部必须调用 `build_predictive_features(route)`，而不是 `build_features(..., FullL2)`。

`ConfiguredFeatureBuilder::new`（默认 FullL2）删除。只留 `new_for_contract`。所有“忘记传合同”的调用变为编译失败。

### 7.2 为什么“FullL2 物化 + model_requirements 过滤”不合格

已在代码里否证：

| 机制 | 为何挡不住偏斜 |
|---|---|
| `model_requirements.union_all()` | 只影响“缺 required 是否拒绝”。`RejectMarket` 对非 required 的 PriceBook 细胞仍然开火 |
| 模型按名字取列 | 向量里多出来的 L2 细胞仍进入 persistence / evidence / DQ / 选市 |
| 训练-服务都叫 `trade.last_fill_return` | 名字相同不能拯救旁边那组改变宇宙的 RejectMarket |
| “反正 sizing 也要 live L2” | sizing 失败应发生在执行平面，并留下 `TierAdmissionRejection`；不应在特征平面把市场从模型宇宙抹掉 |

Fresh boot 上这一点更致命：历史 L2 几乎为空，当前 book 只对已订阅 token 存在。用 FullL2 合同服务 bootstrap 模型，等于用一张训练从未见过的拒绝规则重写候选集。

### 7.3 报告路径重写

当前：

```text
select(empty req) → resolve routes → merge requirements
  → select(merged) → build_features(FullL2, merged)
  → run_route_models(共享向量)
  → sizing 再按 authority 分叉
```

目标：

```text
select(ServingEligibility: live book / 流动性 / 点差 / 账户)
  → resolve routes
  → for route in ready_routes:
        markets = selection ∩ route
        vectors = feature_pipeline.run(FeaturePipelineRequest {
            feature_contract: route.contract.feature_contract,
            model_requirements: route.active.model_requirements,
            ...
        })
        candidates += model_runner.run(route, vectors)
  → sizing / MILP 使用 RecommendationExecutionCapture（live L2）
```

规则：

- 两个 route 若合同不同，必须跑两次 pipeline。禁止 merge contract。
- 两个 route 若合同相同（两个 FullL2，或未来两个同合同 bootstrap），可以按合同去重物化一次，但仍按 route 分别推理。
- `FeatureEvidenceCommitment` 按合同（或按 route-run）分别封印。不得把 FullL2 evidence 挂到 bootstrap model run 上。
- `MarketDecisionCapture` 可以继续带 book 快照，供 sizing 与漏斗审计。book 快照不是 `FeatureVector` 的细胞。
- 缺 live L2 / 账户 / scenario digest：整份报告 fail closed（已有 readiness / empty-report 语义），**不要**回头去改预测合同。
- bootstrap 路径继续不产 TradePolicy，不把 execution gate 标 passed。

### 7.4 选市与“宇宙偏移”

Serving 用 live 流动性过滤宇宙，训练用历史可交易窗口，这是交易系统不可避免的时间轴差异（文献中的 controlled inconsistency）。允许，但必须诚实：

- 过滤规则属于 `ServingEligibility`，进入 selection snapshot / funnel，不进入特征 schema。
- 空 `enabled_categories` = 全类别（已拍板）。pooled route 因此可以选中 Crypto/Weather 市场；这些市场仍必须通过 **TradeBootstrap** 合同，而不是 FullL2 合同。
- 不得把 “没有 5 档深度” 写成 bootstrap 特征缺失。

### 7.5 回放与 parity

| 路径 | 必须使用的合同 |
|---|---|
| `training_dataset` | `profile.spec.feature_contract`（已是） |
| `historical_replay` | 同上（已是） |
| `feature_pipeline` | 请求里的合同（已是） |
| **报告** | **`route.contract.feature_contract`（现在不是）** |
| **durable_feature_parity** | **serving profile 合同 + `new_for_contract`（现在 `new()`）** |
| `bias_table_fit` | 禁止再写死 FullL2；跟被拟合的 profile |

Parity 比较的细胞集合 = 合同 schema ∩ 模型 required。多出来的 live book 字段若出现在任一侧，parity 必须失败，而不是忽略。

### 7.6 删除默认 FullL2 构造

`FeatureSchema::build` / `ConfiguredFeatureBuilder::new` 这类“默认 FullL2”是偏斜入口。删除它们。测试里显式传 `ResearchFeatureContract::FullL2`。

`FeatureFamily::Trade` 留在默认 `enabled_feature_families` 里仍然合理：FullL2 合同可以包含成交族。Bootstrap 合同**忽略**这份 enabled 列表中的 L2 族——`new_for_contract` 已经这样做了，必须保持。

### 7.7 与现有报告类型的关系

`PlannedRecommendationContract` 已经分了 `FullL2` 与 `Bootstrap`。composer 对 FullL2 才读 TradePolicy cohort。这是对的。

不要为了“少改 builder”把 bootstrap 推荐再塞回 `FullL2 { provenance, cohort }`。预测平面的分叉必须延伸到物化，而不是只停在 sizing。

### 7.8 混合 route 报告

编排允许：pooled bootstrap 已激活，crypto bootstrap 稍后激活，full-L2 仍未就绪。

一份报告可以同时包含：

- `BuyModelRoute::Pooled` + `TradeBootstrap`
- `BuyModelRoute::Crypto` + `TradeBootstrapCrypto`

此时必须两次预测物化。MILP 仍然看到统一的经济 tier（都用 live L2 sizing）。参考 scenario 必须覆盖**当时**的 represented route set；缺一边就 fail closed，不做跨 route 特征 fallback。

禁止：pooled bootstrap 与 execution-eligible FullL2 同场。现有 bootstrap scenario 校验已禁止这种混合，保留。

---

## 8. 数据面收尾（服务编排的前提）

这些不是 S1 的主故事，但自动编排会把它们变成生产故障。

1. **Extractor 也禁 Envio 同域**（含 `hyperrpc.xyz`）。启动探针失败则进程不起来。
2. **V1 `last_valid_block`** 设为官方 V2 迁移边界。V2 合约从各自 bootstrap 块开始。区间重叠为零。
3. **时间见证**：每个被引用的 block number（log 所在块与 N+12）用 attestor `eth_getBlockByNumber` 取 timestamp/hash；与 HyperSync header 比 hash 与 timestamp。
4. **`quarantine_reason`** 从 `ExecutionProjectionError` / `ExchangeHistoryError` 直接 `From`，删除字符串 contains。
5. **ETA** 按闭区间计数；首 5% 用真实 `blocks_processed` 重算。
6. **UnknownToken**：仍然停 frontier；进度必须带上 token id 集合与 Gamma 查找结果，便于修身份目录。不跳过、不零填。
7. **Hermetic 测试**（进入 `cargo test`，不是只靠 live smoke）：
   - `chunks_agree` 正反例
   - parent discontinuity rewind + acceptance 撤销
   - V1/V2 × CTF/NegRisk 投影
   - aggregate 排除与双计
   - UnknownToken / DecodeFailure / RemovedLog 阻塞
   - HyperRPC attestor **与** extractor 配置拒绝
   - 部分 CH 写入但无 acceptance → 读者不可见
8. **AvailabilityPolicy**：读者按事实行上的 `availability_policy_hash` + `model_available_at` 过滤。profile 枚举必须参与该 hash 的封印，不能只在 `validate()` 里出现。`IngestionObserved` 不得读 `BlockConfirmation` 行。

`quant_exchange_log_raw/event/match` 的只读审计 API 可以放在首份报告之后，不挡 `ReportEligible`。但接受后不得再“方便起见”删这些表。

---

## 9. UI / UX 闭环

操作员只应回答四问。每一问必须有唯一 API 字段，禁止从 200 天 readiness 脑补。

| 问题 | 投影字段 | UI |
|---|---|---|
| 回填到哪了？还要多久？ | frontier `accepted_through` / `target` / `block_rate` / `projected_completion_at` / `slo_status` | 冷启动页主图 |
| 为什么卡住？ | `status=blocked` + `blocked_reason` + chunk/quarantine/job id | 可点进证据，不能“重试并跳过” |
| 现在能不能训 / 出报告？ | `FreshBootStage` + `manual_report_ready_at` + `next_scheduled_report_at` | 阶段清单勾的是已发生的边 |
| 会不会误下单？ | `ServingAuthority` + 执行入口 disabled | 已有后端阻断；UI 必须显示 ReportOnlyWithLiveL2 |

具体改动：

- Admin SPA 消费新的 bootstrap-activation 投影（或拆成 `/system/exchange-history` + `/system/fresh-boot`）。
- Dashboard 删除 `RESEARCH_HISTORY_GATE_DAYS = 200`。bootstrap 未 `ReportEligible` 时的 blocker 来自编排 stage，不是 200 天。
- 配置编辑器：`max_trade_tape_age_secs` 文案与 generated types 改为 `max_execution_age_secs`。禁止残留 trade-tape 作为可写字段。
- 模型抽屉的 first-champion：编排成功则只读 receipt；编排 blocked 则展示同一 `blocked_reason`。
- 进度页必须能区分 “数据面还在抽 33 天” 与 “数据面好了、CPCV 在跑”。今天的单一 `stage` 做不到。

48h/72h 是横幅，不是绿勾。绿勾只来自状态机到达。

---

## 10. 工作包（破坏式，按依赖）

### WP0 — 合同诚实

- pooled bootstrap `cohort_contract`: `NonVerticalPooled` → `AllEligible`（承认空=全类别）。
- 删除 `ConfiguredFeatureBuilder::new` / `FeatureSchema::build` 默认 FullL2。
- profile content hash 更新；全量迁移调用方；禁止旧 hash 兼容。

### WP1 — 预测平面按 route 物化

- `DefaultReportBuilder` 按 route/合同物化；删除 `ResearchFeatureContract::FullL2` 字面量。
- `durable_feature_parity` / `bias_table_fit` 走 `new_for_contract`。
- 单测：bootstrap 报告向量不含 PriceBook 细胞；缺 live L2 时失败在 sizing/readiness，不在 `trade.*` RejectMarket。
- 单测：同一报告里两个不同合同 route 产生两份 schema hash。

### WP2 — FreshBoot 状态机 + 编排器

- PG 表 + ActiveEnum + 转移函数 + statement-count。
- `FreshBootOrchestrator` 关键任务。
- 系统 actor + WORM。
- history worker **不再**写 `FeaturesAndLabels`。它只发 `ChunkAccepted` / `ActivationWindowComplete`。
- 自动入队 DatasetBuild → … → bootstrap → ad-hoc enqueue。

### WP3 — Readiness / Dashboard / 配置

- 按 profile 取证。
- 删除 bootstrap 200 天硬门。
- UI 接真投影；清掉 trade-tape 字段。

### WP4 — 数据面硬化

- §8 的 HyperRPC、V1 边界、时间见证、quarantine From、ETA、hermetic 测试。

### WP5 — Crypto/Weather 独立 run

- 在 pooled `ReportEligible` 之后，用 retention 覆盖与 `required_days` 启动，不回流 pooled 报告。

每个 WP 结束后跑：

```bash
cargo fmt --all --
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask architecture audit-functions
cargo xtask architecture check
cargo test --workspace
```

另加：fresh-boot schema verify、pinned-block 网络测试、两次空库重建 hash 一致、ReportOnly canary。

---

## 11. 验收

### 11.1 预测 / sizing

- Bootstrap 在线向量的 schema hash = 训练 `TradeBootstrap*` hash。
- 向量细胞名集合 ⊆ 合同 schema；无 `book.*` / 旧 TimeSeries mid 名。
- 人为摘掉 BookStore 订阅：报告 fail closed，原因是 execution capture / readiness，不是 `trade.last_fill_return` 缺失。
- FullL2 champion（若存在）仍按 FullL2 合同物化；两条路径无共享 `build_features` 函数。
- Bootstrap 模型在 SemiAuto / AutoExecution 下仍不能建 OrderIntent。

### 11.2 编排

- 空库 + 合法 HyperSync + 独立 archive RPC，启用 history。
- 无人点击研究页的情况下，状态机自行走到 `ReportEligible`。
- 每个 stage 在 PG 里有进入/离开时间与前驱 job id。
- 注入 provider mismatch：停在数据面 blocked，不入队 DatasetBuild。
- 注入 CPCV 失败：停在 `CpcvQueued/Running` blocked，不 bootstrap。
- 第二次空库重建：source-slice / dataset / model / scenario hash 与第一次相同。
- 人工抽屉对已提交 run 只展示同一 receipt。

### 11.3 UX

- 冷启动页能区分“抽块”与“训练”。
- Dashboard 在 activation=20 天时**不会**显示“还差 180 天才能研究”。
- 配置页没有可提交的 `max_trade_tape_age_secs`。

### 11.4 时间

- 门禁满足时，fresh boot → 手动/自动第一份 Published 报告目标 12–36h，保守 72h。
- 任一门禁失败：SLO 标 Violation，状态机不前进。

---

## 12. 明确不做什么

- 不把 `/prices-history` 接进事实层。
- 不恢复 trade-tape 表、view、re-export、dual-write。
- 不引入 HyperRPC。
- 不放宽 CPCV / DSR / PBO / 最小样本。
- 不把 93/100 做成第三条 frontier 配置。
- 不改空 `enabled_categories` 的“全类别”语义。
- 不让 bootstrap 模型在任何 runtime mode 下下单。
- 不把 24h shadow 套到空 route 首次 bootstrap；也不把首次 bootstrap 的“免 shadow”套到后续 promotion。
- 不做“进度条够了就出报告”的降级通道。

---

## 13. 残余风险（设计接受）

| 风险 | 处理 |
|---|---|
| 空 `enabled_categories` 让 pooled 吃到 Crypto/Weather | 已拍板接受。合同改为 `AllEligible`。vertical 专属域特征仍只在对应 bootstrap 合同里 |
| 33 天 resolved 样本可能不够过统计门 | fail-closed 等待，不降阈值。编排停在 Dataset/CPCV |
| attestor `eth_getLogs` 仍是时间主瓶颈 | SLO 告警；不改权威层级 |
| 自动 bootstrap 的系统身份被滥用 | 专用 role、固定 reason_code、WORM、无“跳过门禁”权限 |
| raw/event/match 暂无产品读路径 | 不挡首份报告；列为后续审计能力，不是兼容债 |

---

## 14. 总评

S1 的数据面重构是对的，PLAN 对 `/prices-history` 与双源见证的否定/加强也是对的。不能验收的原因不是“还差几个 UI 文案”，而是三条结构断裂：

1. 预测合同在报告与 parity 上被 FullL2 覆盖；
2. 回填完成之后没有可恢复的编排状态机；
3. 控制面仍用 200 天全局门禁讲述一个已经被否定的故事。

按本文件实施后，S1 的闭环定义才变成可证伪的句子：

> 空库启动，双源接受 33 天 activation 窗口，编排器在不降低门禁的前提下自动产出一个 `ReportOnlyWithLiveL2` champion；该 champion 的在线预测向量与训练合同逐细胞一致；live L2 只决定能不能 sizing；操作员从同一状态机看到为什么还不能出报告，或为什么已经可以。
