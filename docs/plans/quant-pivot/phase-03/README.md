# Phase 03 — Research Plane 子phase索引

<!-- quant-pivot-lifecycle-contract:v1 -->
> **Lifecycle contract**
> - `lifecycle_assumption`: 项目尚未正式生产上线，当前状态为 `pre_production_resettable`，系统自有基线统一为 `boot` / schema version `1`。
> - `schema_data_version_impact`: 本文中的历史版本号与递增路径不再具有实施效力；当前实现不迁移测试数据、旧结构或旧版本。
> - `pre_production_behavior`: 允许 clean-break、migration squash 与全新基础设施 bootstrap，但任何数据销毁仍需操作者单独授权。
> - `production_frozen_behavior`: 一旦完成不可逆 production seal，后续变更必须提供前向 migration、兼容性评估、回滚方案与数据验证。
> - `rollback_and_data_verification`: 封存前通过清空后的 fresh-install 验证；封存后不得回退到 boot reset。

> 状态：生产级破坏式实施拆分
>
> 父文档（概念规格）：[`../03-data-factor-model-pipeline.md`](../03-data-factor-model-pipeline.md)
> 与 [`../08-third-party-crates-and-ml-stack.md`](../08-third-party-crates-and-ml-stack.md)
>
> 本目录把 Phase 03 拆成 8 个可独立推进、带验收契约的子phase。父文档保持
> "概念真理"，本目录是"可执行实施契约"。任一子phase未满足其 Blocker / 验收，
> 不允许进入下一子phase。

## 0. 为什么拆分

Phase 03 是整个 quant-pivot 的研究平面：市场选择、特征、因子、模型运行时、
训练、回测、质量门禁、治理，外加 ML 技术栈与历史 point-in-time 解析层。它的
体量远超单一可验证增量，因此拆成 3.0–3.7。

**当前代码现实（拆分基线）**

- Phase 1 完成：14 张 `quant_*` Postgres 表 / entity / iden、持久化 DTO、
  [`enums/quant.rs`](../../../crates/quant-pivot-models/src/enums/quant.rs) 生命周期枚举、
  typed IDs、ClickHouse quant fact 表 + row 类型、`QuantFactRepository` /
  `ChQuantFactRepository`。
- Phase 2 完成：`BookFactWriter`、`AsyncWriter` / `ChWriteManager`、
  `IngestPipelineLagTracker`、`BookDataQualityService`，以及 **live-only** 的
  [`LiveBookDataSource`](../../../crates/quant-pivot-core/src/pipeline/point_in_time.rs)。
  runtime-config v3 九段（`selection` / `data_quality` / `features` / `factors` /
  `model` / `reports` / `portfolio` / `execution` / `notification`）齐全。
- Phase 3 基本空白：[`quant-pivot-research`](../../../crates/quant-pivot-research)
  仅 `crate_version()`；无 ML 依赖、无计算 trait、无历史 PIT；Postgres repo impl
  仅 model registry / recommendation report / order intent；quant ClickHouse fact
  无生产者；`FeatureVectorInfo.payload` 为不透明 JSON；`SignalCandidate` 是
  `i8`/`Decimal`/无表的 stub。

**Phase 1 遗留缺口（Phase 03 必补，逐篇标注）**

- 无 `quant_training_dataset` 表（`ModelVersionInfo.training_dataset_id` 是裸 `Uuid`）。
- 无 backtest-report、shadow-comparison 持久化。
- 无 ClickHouse 读层（仅写）。

## 1. 子phase索引

| 子phase | 标题 | 闭环定位 | 文档 |
|---|---|---|---|
| 3.0 | Research Foundation & Contracts | 契约/脚手架 | [`03.0-research-foundation-and-contracts.md`](03.0-research-foundation-and-contracts.md) |
| 3.1 | Market Selection | 在线输入 | [`03.1-market-selection.md`](03.1-market-selection.md) |
| 3.2 | Feature Plane | 在线/离线特征 | [`03.2-feature-plane.md`](03.2-feature-plane.md) |
| 3.3 | Factor Plane | 在线/离线因子 | [`03.3-factor-plane.md`](03.3-factor-plane.md) |
| 3.4 | Model Runtime & Weighted Scorer | **在线推理闭环** | [`03.4-model-runtime-weighted-scorer.md`](03.4-model-runtime-weighted-scorer.md) |
| 3.5 | Historical PIT & Training Dataset | 离线数据 | [`03.5-historical-pit-and-training-dataset.md`](03.5-historical-pit-and-training-dataset.md) |
| 3.5.1 | Training Dataset Admin API | UI/API 契约 | [`03.5.1-training-dataset-admin-api.md`](03.5.1-training-dataset-admin-api.md) |
| 3.6 | Trainer, Classical ML & Backtest | **离线训练/回测闭环** | [`03.6-trainer-classical-ml-backtest.md`](03.6-trainer-classical-ml-backtest.md) |
| 3.7 | Quality Gates & Governance | **离线治理闭环** | [`03.7-quality-gates-and-governance.md`](03.7-quality-gates-and-governance.md) |
| 3.8 | Vertical Domain Closed-Loop（Crypto 参考垂直） | **垂直完整闭环** | [`03.8-vertical-domain-closed-loop.md`](03.8-vertical-domain-closed-loop.md) |

> 3.8 合并了垂直领域的**设计真理（D1–D7）+ 工作流（W1–W7）+ 外部数据源选型**为单一权威文档
> （原 `03.x-vertical-domain-design.md`、原 `03.6 §11`、原 `docs/operations/domain-data-sources.md`）；
> 3.5/3.6 已交付，垂直在其上加性扩展，**不回推** 3.2/3.3/3.5/3.6。

## 2. 依赖图

```mermaid
flowchart TD
    P30["3.0 Foundation & Contracts"] --> P31["3.1 Market Selection"]
    P30 --> P32["3.2 Feature Plane"]
    P31 --> P32
    P32 --> P33["3.3 Factor Plane"]
    P33 --> P34["3.4 Model Runtime & Weighted Scorer"]
    P30 --> P35["3.5 Historical PIT & Training Dataset"]
    P32 --> P35
    P33 --> P35
    P35 --> P36["3.6 Trainer, Classical ML & Backtest"]
    P34 --> P36
    P36 --> P37["3.7 Quality Gates & Governance"]
    P34 --> P37
    P34 --> P38["3.8 Vertical Domain Closed-Loop"]
    P35 --> P38
    P36 --> P38
```

两条闭环：

- **在线闭环**（3.1 → 3.2 → 3.3 → 3.4）：`MarketSelection → FeatureVector →
  FactorValue → ModelRun → SignalCandidate`，持久化 ModelRun + ClickHouse 事实。
- **离线闭环**（3.5 → 3.6 → 3.7）：`Historical PIT → TrainingDataset → Trainer →
  Backtest → QualityGate → Shadow → Publish/Rollback`。
- **垂直闭环**（子phase 3.8，Crypto 参考垂直）：`MarketLinkage → DomainDataSource →
  quant_domain_observation → domain slice 特征/因子 → ModelRouting`。**统一落地于 3.8**
  （3.5/3.6 已交付，垂直在其上加性扩展，不回推 3.2/3.3/3.5/3.6）。

## 3. 已拍板的设计基线（贯穿全部子phase）

1. **Trait 归属**：所有计算 trait（`MarketSelector` / `FeatureBuilder` /
   `FactorComputer` / `QuantModelRuntime` / `ModelTrainer` / `Backtester` /
   `Labeler` / `ModelQualityGate` / `ArtifactStore` / 历史 `PointInTimeDataSource` /
   `PointInTimeSnapshotSource`）与其计算域值类型（强类型 `FeatureVector` / `FactorValue` /
   `SignalCandidate` / `ModelArtifact` / runtime I/O）全部归属
   `quant-pivot-research`。`quant-pivot-models` 只保留持久化 DTO（`*Info` /
   `New*`）、`enums/quant.rs`、typed IDs；research 依赖 models 做持久化映射。
2. **Artifact 后端**：本地 artifact 目录 + `ArtifactStore` trait + `file://` URI
   （deploy 配置 root path），S3/MinIO 后续无缝替换；Postgres 只存 metadata /
   hash / URI。
3. **ML 范围**：weighted-factor 主路径 + smartcore classical ML（shadow 候选），
   统一 `QuantModelRuntime` / `ModelRuntimeFactory` 隔离 concrete crate type。
4. **Backtest 组合层**：Phase 03 用最小 deterministic greedy allocator 产出
   portfolio 级指标；完整受治理 `PortfolioPlanner` 留 Phase 04 复用同一 trait。
5. **零兼容、零 re-export**；`f64` 仅允许出现在训练矩阵边界，禁止泄漏到 money domain。
6. **PIT 正确性是硬不变量**：任何特征 / 训练样本不得读取 `as_of` 之后的事实；
   回测禁止访问 live `BookStore`。

## 4. 跨子phase不变量

- 每个 feature / factor / dataset / model artifact / backtest 都有
  `blake3:` canonical hash（复用
  [`models::hashing::CanonicalDigest`](../../../crates/quant-pivot-models/src/hashing.rs)）。
- 所有计算域 trait 接受**冻结快照**（config version、selection snapshot、PIT
  source），禁止读取 mutable runtime state。
- research crate 禁止：直接下单、读 web state、在循环里查数据库、把第三方 ML
  concrete type 暴露到业务层。
- 货币 / 价格 / shares / probability 一律使用项目 newtype（`Usd` / `Price` /
  `Shares` / `Probability`），绝不用 `f64`。
- 失败语义统一走 `QuantResult` / 结构化错误，`src/` 内禁止 `unwrap()`。

## 5. ML 依赖引入顺序（feature-gate）

落在 `quant-pivot-research` 的 `[features]`：

```toml
# ndarray / ndarray-stats / statrs / rayon 为 base deps（在线 feature plane 必需）
[features]
default = []
dataframe = ["dep:polars", "dep:arrow", "dep:parquet"]
optimize = ["dep:argmin", "dep:argmin-math"]
ml-classical = ["dep:smartcore"]
```

引入子phase对应：

- 3.0：声明 workspace 依赖与 feature gate（numeric stack 为 base dep，`default = []`）。
- 3.2 / 3.5：base numeric stack + `dataframe`（polars/arrow/parquet 仅离线 materialization）。
- 3.6：`optimize`（argmin）+ `ml-classical`（smartcore）。
- 禁止本期引入：`good_lp` / `ort` / `burn` / `candle`（见父文档 §30）。

`quant-pivot-core` 链接 `quant-pivot-research/dataframe`（3.5 数据集 Parquet）；
`quant-pivot-research` 自身 `default = []`。report_only 二进制因此含 polars，但
在线 hot path 不调用 Polars。CI 分 job 测 `ml-classical` / `optimize` heavy features。

## 6. 延后项总表（缺口必须在对应子phase文档显式标注）

| 延后能力 | 本期替代 | 落地 Phase | 标注于 |
|---|---|---|---|
| 完整受治理 `PortfolioPlanner` | 最小 greedy allocator（backtest 用） | Phase 04 | 3.6 / 3.4 §10 |
| TopN 报告生成 / report scheduler / 定时 `live_report_inference` | 按需 `ModelRunner` + SignalCandidate | Phase 04 | 3.4 §10 |
| `required_features` → 03.1 selection 全链路编排 | `QuantModelRuntime::required_features()` trait | Phase 04 | 3.4 §10 / 3.8 §6.5 |
| report-level shadow 完整比较（capital/would-execute/risk envelope delta） | signal/rank 层 `shadow_diff` + metrics | Phase 04 | 3.7 §10 |
| shadow `exceeds_threshold` operator alert + `quant_shadow_comparison` 表 | shadow run `metrics_json` | Phase 3.7 | 3.7 §4 / §10 |
| `ModelConfig.min_quality_gate_age_secs` load-time deny | schema 字段 + validation | Phase 3.7 | 3.4 §4.2 / 3.7 §4 |
| `FactorsConfig.factor_weights` 在线 overlay（非 Published） | artifact 内冻结权重 | Phase 3.7 | 3.4 §4.2 / 3.7 §3.6 |
| `ReturnModelSpec::Calibrated` 拟合 + `objective_report` | `Heuristic` + Calibrated 插值应用 | **Phase 3.6 ✅ 已交付** | 3.4 §10 / 3.6 §1.1 |
| `classical` runtime（smartcore） | factory `RuntimeUnavailable` | **Phase 3.6 ✅ 已交付**（`ml-classical`） | 3.4 §10 |
| `argmin` 权重优化（grid 主干 + `optimize` 精修） | grid coordinate search | **Phase 3.6 ✅ 已交付**（`optimize`） | 3.6 §1 |
| `ModelConfig.prediction_horizon_secs` 在线读取 | artifact `prediction_horizon_secs`（trainer 写入） | **Phase 3.6 ✅ 已交付**（写入 artifact） | 3.4 §4.2 |
| 垂直领域完整闭环（Crypto：linkage + domain PIT + 两层向量 + 真实特征/因子 + ModelRouting + domain dataset + 垂直训练/回测） | skeleton + `DomainDataMissing` + 3.5 generic dataset | **Phase 3.8** | [`03.8`](03.8-vertical-domain-closed-loop.md) |
| 其余四垂直（Sports/Politics/Weather/Geopolitics）真实外部数据 | Crypto 范式加性扩展 | Post–3.8 Crypto | [`03.8`](03.8-vertical-domain-closed-loop.md) §12 |
| `good_lp` 组合优化 | greedy allocator | Phase 05 | 3.6 |
| `ort` ONNX 推理 | `QuantModelRuntime` 预留 arm | Phase 06 | 3.4 §10 |
| auto-execution 门禁生效 | config 口径记录 | Phase 05/06 | 3.7 §10 |
| `burn` / `candle` 深度学习 | — | Phase 08 | 3.4 §10 |
| 对象存储（S3/MinIO）artifact | 本地目录 + content-addressed key | 后续 | 3.0 / 3.4 §1.1 |
| PG `SignalCandidate` 表 | CH `quant_signal_candidate_event` only | —（by design） | 3.4 §10 |
| `SellYes` / `SellNo` 退出候选（机会性 Sell scorer） | Buy 侧 scorer only；05.6 执行侧 Sell 已覆盖 | **Phase 06** | [`phase-06/06.1`](../phase-06/06.1-opportunistic-sell-exit-signal.md) |
| `input_hash` 内容级 audit digest | id + schema + version 绑定 | Phase 04 编排 + 可选 3.7 | 3.4 §10.6 |

## 7. 文档契约模板

每篇子phase文档必须包含以下小节（顺序固定）：

1. **目标与闭环定位** —— 这一子phase交付什么、在两条闭环中的位置。
2. **删除清单** —— 加替代代码前必须删除哪些 crate / 模块 / 类型 / 配置；
   若无可删，显式写"无（本子phase为净新增）"。
3. **新领域类型 / 表 / ClickHouse fact** —— research 计算类型、Postgres 表、CH fact。
4. **deploy-config key 与 runtime-config v3 path** —— 消费哪些既有 config 段、
   是否新增 deploy key。
5. **必建模块与 trait** —— 模块树 + trait 签名（verbatim Rust）。
6. **生产不变量与失败语义** —— PIT、降级、hash、错误处理硬规则。
7. **第三方 crate 引入** —— 本子phase允许 / 禁止的 crate 与 feature gate。
8. **验收测试** —— 必须新增的测试用例（含父文档 §23 对应项）。
9. **Blocker** —— 触发即判定本子phase失败的条件。
10. **延后 / 缺口** —— 本子phase明确不做、留给后续 Phase 的点。

## 8. 质量门禁（每个子phase收尾必跑）

```bash
cargo fmt --all --
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p quant-pivot-research --features ml-classical,dataframe,optimize -- -D warnings
bash scripts/lint-architecture.sh
bash scripts/lint-quant-pivot-boundary.sh
cargo test --workspace
```
