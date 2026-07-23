# 08 — 第三方 Crate 与模型训练技术选型

<!-- quant-pivot-deployment-contract:v1 -->
> **Deployment contract**
> - `fresh_boot_assumption`: 项目尚未正式生产上线，将从全新 `boot` / schema version `1` 部署；仓库和数据库不保存 lifecycle seal 状态。
> - `schema_data_version_impact`: 本文中的历史版本号与递增路径不再具有实施效力；当前实现不迁移测试数据、旧结构或旧版本。
> - `pre_deployment_behavior`: 允许 clean-break、migration squash 与全新基础设施 bootstrap，但任何数据销毁仍需操作者单独授权。
> - `post_deployment_behavior`: 首次部署后使用正常前向 migration、回滚与数据验证；不使用不可逆 production seal 或兼容桥。
> - `rollback_and_data_verification`: 首次部署前通过清空后的 fresh-install 验证；部署后使用备份、前向 migration 与显式回滚。

> 状态：生产级技术选型设计
>
> 目标：明确 quant-pivot 哪些能力应该直接使用成熟 Rust crate，哪些能力先自研薄层，哪些能力暂缓引入。

## 0. 选型原则

- 优先使用稳定、活跃、文档完善、生态常用的 crate。
- 钱、价格、shares、probability 的业务语义仍使用项目 newtype；第三方 crate 只在数值计算边界使用 `f32`/`f64`。
- 训练和推理分层：训练可以离线重，线上推理必须轻、可限时、可降级。
- 第一版模型必须可解释、可审计、可回放，不追求复杂模型。
- 每个重依赖必须有明确引入 Phase，禁止一次性塞入完整 ML 生态。
- 任何 crate 引入前必须检查 MSRV、native 依赖、二进制体积、许可证、维护活跃度。

## 1. 推荐依赖矩阵

| 能力 | 首选 crate | 备选 | 引入 Phase | 说明 |
|---|---|---|---|---|
| DataFrame / 特征聚合 | `polars` | `datafusion` | Phase 3 | rolling/window/groupby 强，适合离线特征 |
| Arrow/Parquet 格式 | `arrow`, `parquet` | Polars re-export | Phase 2/3 | ClickHouse/feature dataset 边界 |
| 数值矩阵 | `ndarray` | `nalgebra` | Phase 3 | ML 矩阵、训练样本 |
| 统计扩展 | `ndarray-stats` | `statrs` | Phase 3 | quantile、correlation、summary stats |
| 分布/检验 | `statrs` | 自研薄层 | Phase 3 | Normal、Beta、StudentT、CDF/PDF |
| Classical ML | `smartcore` | `linfa` | Phase 3/4 | tree/ensemble 覆盖更完整 |
| 线性/逻辑模型 | `linfa` | `smartcore` | Phase 3 | scikit-learn-like API |
| 权重优化 | `argmin` | grid search 自研 | Phase 3 | factor weight optimization |
| 组合优化 | `good_lp` + solver | greedy 自研 | Phase 4/5 | LP/MILP 表达预算和约束 |
| 并行特征 | `rayon` | tokio tasks | Phase 3 | CPU-bound 特征/回测 |
| Report occurrence | `croner` + PostgreSQL durable coordinator | 无 scheduler runtime crate | Phase 11.8 | cadence 计算与 durable run/gap/lease 分离 |
| ONNX 推理 | `ort` | `candle` | Phase 6+ | 外部训练模型线上推理 |
| Rust-native DL 训练 | `burn` | `candle` | Phase 8+ | 深度学习后续选项 |
| 轻量推理/transformers | `candle` | `ort` | Phase 8+ | Hugging Face/safetensors |
| 模型 artifact | `serde_json`, `bitcode` | `bincode` | Phase 3 | weighted/classical model artifact |
| 配置 schema | `schemars` | existing | Phase 1 | 已使用，继续 |
| Polygon 合约绑定 / RPC | `alloy` | Polymarket SDK CTF facade | Phase 5.10 | 仅 `quant-pivot-api::ctf` 使用；core 不直接依赖 alloy；标准二元 `redeemPositions` 首版 |

## 2. 不建议第一版引入

| crate/方案 | 原因 |
|---|---|
| `burn` 主路径训练 | 功能强但重；第一版没有必要上 DL |
| `candle` 主路径训练 | 更偏轻量推理，训练生态不是第一选择 |
| `tch-rs` | libtorch native 依赖重，CI/Docker 复杂 |
| `feature-factory` | alpha，下载和维护活跃度不足，暂不进生产主路径 |
| `oxits-rs` | 太新，适合后续 time-series research，不进第一版 |
| Python/PyO3 训练主路径 | 增加部署面和运行时复杂度；可作为离线实验，不进入生产闭环 |

## 3. 数据处理栈

### 3.1 `polars`

用途：

- 训练数据集构建。
- rolling window 特征。
- groupby category/event。
- lazy query pipeline。
- parquet/arrow dataset 读写。

适用边界：

- 离线 materialization。
- backtest。
- ad-hoc analysis。

不适合：

- 每个 live tick 的热路径。
- money newtype 业务运算。

建议：

```text
research crate 使用 polars
core hot path 不直接依赖 polars
```

### 3.2 `datafusion`

用途：

- SQL-like PIT 查询。
- 大规模 Arrow/Parquet 查询。
- 将 feature materialization 表达为 query plan。

建议：

- Phase 3 可先不用，等数据量增大后引入。
- 如果引入，封装在 `PointInTimeSnapshotSource` trait 后面。

### 3.3 `arrow` / `parquet`

用途：

- dataset artifact。
- model training input。
- ClickHouse export/import 边界。

规则：

- Postgres 存 metadata 和 hash。
- 大型 dataset artifact 放 Parquet。
- artifact hash 必须覆盖 schema + row group metadata + content digest。

## 4. 数值与统计栈

### 4.1 `ndarray`

用途：

- feature matrix `X`。
- label vector `y`。
- train/validation split。
- model inference input。

建议内部统一：

```rust
pub struct TrainingMatrix {
    pub features: Array2<f64>,
    pub labels: Array1<f64>,
    pub feature_names: Vec<FeatureName>,
}
```

业务边界：

- `Decimal` -> `f64` 只在训练矩阵构建时发生。
- 转换必须记录 scale、unit、missing policy。
- 训练输出回到 `Decimal` / newtype 后才进入报告。

### 4.2 `ndarray-stats`

用途：

- quantiles。
- median。
- correlation。
- covariance。
- entropy/KL。
- histogram。

用于：

- feature normalization。
- quality gate。
- drift detection。
- shadow/live diff。

### 4.3 `statrs`

用途：

- 分布 CDF/PDF。
- confidence interval。
- hypothesis tests。
- Bayesian prior 辅助。

注意：

- `statrs` 是统计工具，不是业务概率模型权威。
- 输出进入业务层前必须 clamp 到合法范围并转成 `Probability`。

## 5. Classical ML 栈

### 5.1 第一版：自研 Weighted Factor Scorer

第一版不依赖复杂 ML crate 也能生产：

```text
score = Σ normalized_factor_i * weight_i * confidence_i
```

优势：

- 可解释。
- 易审计。
- 易回放。
- 与 TopN 报告直接对齐。
- 可用 `argmin` 或 grid search 优化权重。

### 5.2 `smartcore`

用途：

- Random Forest。
- Extra Trees。
- XGBoost-style regressor。
- Logistic/Ridge/Lasso/ElasticNet。
- KNN/SVM/Naive Bayes。
- model selection。

推荐用法：

- Phase 3 先作为离线 candidate model。
- Phase 4 shadow report 对比 weighted scorer。
- 通过 model registry 管理 artifact。
- 生产主路径 publish 细化为 [`phase-06/06.4-classical-model-publish-path.md`](phase-06/06.4-classical-model-publish-path.md)。

适合：

- 非线性 factor interaction。
- category-specific models。
- feature importance 分析。

风险：

- model artifact serde 能力要验证。
- 需要封装统一 `ModelArtifact`，避免业务代码依赖 smartcore concrete type。

### 5.3 `linfa`

用途：

- Linear/logistic regression。
- PCA/reduction。
- preprocessing。
- SVM。
- nearest neighbors。

推荐用法：

- 用于 baseline models。
- 用于特征降维或 sanity check。
- 不作为唯一 ML 栈。

### 5.4 选择规则

| 场景 | 选择 |
|---|---|
| 需要最强解释性 | Weighted scorer |
| 需要线性 baseline | `linfa-linear` / `linfa-logistic` |
| 需要 tree ensemble | `smartcore` |
| 需要 NN/transformer | 后续 `burn`/`candle` |
| 需要外部训练模型线上推理 | `ort` |

## 6. 优化栈

### 6.1 `argmin`

用途：

- factor weight optimization。
- threshold calibration。
- stop-loss/take-profit 参数搜索。
- nonlinear objective。

优点：

- pure Rust。
- 支持多种优化算法。
- 可 checkpoint / observer。
- backend agnostic。

建议：

- Phase 3 用于 `WeightedFactorTrainer`。
- 保留 grid search 作为 deterministic fallback。

### 6.2 `good_lp`

用途：

- portfolio allocation。
- budget constraints。
- per-category exposure。
- per-event exposure。
- integer TopN inclusion。

规则：

- `good_lp` 是建模层，不是 solver。
- 第一版可以先用 deterministic greedy planner。
- 若引入 solver，必须明确 solver backend：
  - pure Rust / minimal native：优先小规模问题。
  - HiGHS：性能强，但 native 依赖更复杂。

组合优化问题示例：

```text
maximize Σ score_i * x_i
subject to:
  Σ usd_i * x_i <= total_budget
  usd_i * x_i <= per_market_cap_i
  Σ category_usd_c <= category_cap_c
  x_i ∈ {0, 1}
```

## 7. 推理栈

### 7.1 `ort`

用途：

- 部署外部训练的 ONNX 模型。
- 统一未来 PyTorch/scikit-learn/Burn export 模型推理。

调研结论：

- `ort` 是 Rust ONNX Runtime 主流 wrapper。
- 支持 ONNX Runtime 多硬件后端。
- 文档建议新项目使用 2.x rc。

重要约束：

- 最新 `ort 2.0.0-rc.12` MSRV 约为 1.88。
- 当前 workspace MSRV 是 1.91。
- 引入 latest `ort` 前必须决定是否升级 MSRV。
- 如果不升级，只能评估较旧 rc 版本，且要接受 API/安全/维护权衡。

建议：

- Phase 6 以后引入。
- 不作为第一版 weighted/classical model 的依赖。
- 封装为 `OnnxInferenceEngine` trait。
- 生产集成计划见 [`phase-06/06.3-onnx-runtime-integration.md`](phase-06/06.3-onnx-runtime-integration.md)。

### 7.2 `candle`

用途：

- Rust-native lightweight inference。
- Hugging Face safetensors。
- serverless/small binary 场景。
- transformer/domain text features 后续可能需要。

建议：

- 不作为第一版训练框架。
- 后续做 news/text/domain signal 时评估。

### 7.3 `burn`

用途：

- Rust-native deep learning training。
- autodiff。
- 多 backend：CPU/WGPU/CUDA/Metal 等。

建议：

- Phase 8+ 才考虑。
- 如果需要 Rust 内训练神经网络，优先 `burn` 而不是 `tch-rs`。
- 必须单独做 benchmark 和 deployment spike。

## 8. 调度与后台任务

### 8.1 现有 `PeriodicTask`

保留用于简单 interval worker：

- fact flush。
- health check。
- report expiration。

### 8.2 `croner` + PostgreSQL durable coordinator（Phase 11.8）

`croner` 只负责从 frozen schedule spec 计算 occurrence；它不拥有 job、lease、重试或 misfire
状态。Report scheduling 的权威数据是 `quant_report_schedule_state`、`quant_report_run` 与
`quant_report_schedule_gap`。

并发 claim、latest-only coalescing、全局 build slot、heartbeat 和 crash recovery 均由 PostgreSQL
transaction/CAS 实现。项目不再依赖 scheduler runtime crate，也不保留 facade 或兼容模块。
`PeriodicTask` 只用于 report plane 之外的简单 interval worker。
## 9. 依赖引入顺序

### Phase 1

只新增：

- schema/serde 相关无新增或沿用。

### Phase 2

可新增：

- `arrow`
- `parquet`

如果 Polars 已足够，则通过 Polars 间接管理 Arrow，不直接暴露 Arrow 类型到业务层。

### Phase 3

新增：

- `polars`
- `ndarray`
- `ndarray-stats`
- `statrs`
- `rayon`
- `argmin`
- `smartcore` 或 `linfa`，先二选一，不要同时铺开全部算法。

推荐第一批：

```text
polars + ndarray + ndarray-stats + statrs + rayon + argmin
```

第二批：

```text
smartcore
```

`linfa` 可作为 baseline 或替代，不必和 `smartcore` 同时进入主路径。

### Phase 4

可新增：

- `croner`（只解析 report cadence；durable ownership 由 PostgreSQL 提供）。

### Phase 5

可新增：

- `good_lp`，若 greedy portfolio planner 不够。

### Phase 6+

可新增：

- `ort`，前提是解决 MSRV；详见 [`phase-06/06.3`](phase-06/06.3-onnx-runtime-integration.md)。
- classical model 主路径 publish；详见 [`phase-06/06.4`](phase-06/06.4-classical-model-publish-path.md)。
- attribution feedback / auto retraining；详见 [`phase-06/06.5`](phase-06/06.5-attribution-feedback-and-auto-retraining.md)。
- counterfactual factor attribution；详见 [`phase-06/06.6`](phase-06/06.6-counterfactual-factor-attribution.md)。

### Phase 8+

可评估：

- `burn`
- `candle`

## 10. Cargo Feature 策略

重依赖必须 feature-gated：

```toml
[features]
default = []
ml-classical = ["dep:smartcore", "dep:linfa"]
ml-onnx = ["dep:ort"]
ml-deep = ["dep:burn", "dep:candle-core", "dep:candle-nn"]
dataframe = ["dep:polars"]
lp-solver = ["dep:good_lp"]
```

原则：

- `quant-pivot-core` 启用 `quant-pivot-research/dataframe`（3.5 `DatasetParquetCodec`）——
  **report_only 二进制因此链接 polars**；research crate `default = []` 不变。
- `quant-pivot-bin` production profile 只启用需要的 features。
- CI 分 job 测试 heavy features。
- report_only 基础服务不应强制链接 ONNX/DL native runtime；Polars 仅 dataset/offline 路径。

## 11. 训练流程中的 crate 使用图

```text
ClickHouse/Postgres
 -> polars/datafusion query
 -> parquet/arrow dataset
 -> ndarray matrix
 -> ndarray-stats/statrs feature stats
 -> argmin/grid search optimize weighted factors
 -> smartcore/linfa optional baseline model
 -> model artifact serde/bitcode
 -> backtest
 -> quality gates
 -> report inference
```

ONNX 后续路径：

```text
external trainer or Rust trainer
 -> export ONNX
 -> ort session
 -> ModelRunner::infer
 -> SignalCandidate
```

DL 后续路径：

```text
burn train
 -> burn artifact or ONNX export
 -> burn/candle/ort inference
```

## 12. 必做 Spike

引入前必须做以下 spike：

### 12.1 Polars Spike

- 从 ClickHouse 导出样本。
- rolling feature build。
- Parquet write/read。
- 10k markets x 100 windows benchmark。

### 12.2 SmartCore Spike

- `TrainingMatrix` -> random forest / xgboost regressor。
- artifact serialization。
- feature importance。
- inference latency。

### 12.3 Argmin Spike

- weighted factor objective。
- checkpoint。
- deterministic seed。
- convergence report。

### 12.4 GoodLP Spike

- 100 candidates TopN allocation。
- category/event constraints。
- pure Rust solver vs native solver 对比。

### 12.5 Ort Spike

- MSRV check。
- ONNX model load。
- inference latency。
- Docker/native dependency check。

## 13. 禁止项

- 禁止在 hot path 中直接使用 Polars DataFrame。
- 禁止让 web handler 直接调用 ML crate。
- 禁止训练代码直接写 runtime config。
- 禁止没有 artifact hash 的模型发布。
- 禁止没有 feature schema hash 的训练数据集。
- 禁止未做 MSRV 检查引入 `ort` latest。
- 禁止把 `f64` 从训练层泄漏到 money domain。
- 禁止未 feature-gate 的深度学习依赖进入默认 build。

## 14. 资料来源

调研参考：

- Burn 官方文档：https://burn.dev/docs/burn/
- Candle 官方仓库与文档：https://github.com/huggingface/candle
- Linfa 官方文档：https://rust-ml.github.io/linfa/
- SmartCore docs.rs：https://docs.rs/smartcore/
- Polars Rust 文档：https://docs.pola.rs/
- `ort` crate 与指南：https://ort.pyke.io/
- `argmin` 文档：https://argmin-rs.org/
- `good_lp` 仓库：https://github.com/rust-or/good_lp
- `ndarray` / `ndarray-stats` / `statrs` docs.rs。

## 15. Classical ML 生产设计

Classical ML 是 quant-pivot 第一阶段最适合进入生产闭环的模型族。原因不是它最强，而是它在数据量有限、需要解释、需要快速迭代、需要稳定部署时风险最低。

### 15.1 模型族分层

| 模型族 | 首选 crate | 用途 | 生产状态 |
|---|---|---|---|
| weighted factor scorer | 自研 + `argmin` | 第一版主模型 | 必须实现 |
| linear / logistic | `linfa` 或 `smartcore` | baseline、概率校准 | Phase 3/4 |
| tree / random forest | `smartcore` | 非线性特征交互 | Phase 4 shadow |
| xgboost-style regressor | `smartcore` | 排序/return 预测候选 | Phase 4 shadow |
| SVM/KNN/Naive Bayes | `linfa`/`smartcore` | 研究和 sanity check | 非主路径 |

### 15.2 Classical Model Trait

所有 classical model 必须封装在统一 adapter 后面：

```rust
pub trait ClassicalTrainer {
    fn family(&self) -> ClassicalModelFamily;

    fn train(
        &self,
        dataset: &TrainingMatrix,
        params: ClassicalTrainingParams,
    ) -> QuantResult<ClassicalModelArtifact>;
}

pub trait ClassicalPredictor {
    fn predict_one(
        &self,
        artifact: &ClassicalModelArtifact,
        features: &FeatureVector,
    ) -> QuantResult<ModelPrediction>;

    fn predict_batch(
        &self,
        artifact: &ClassicalModelArtifact,
        matrix: &InferenceMatrix,
    ) -> QuantResult<Vec<ModelPrediction>>;
}
```

业务层禁止出现：

- `smartcore::ensemble::*`
- `linfa::*`
- concrete model type in API / DB / core。

### 15.3 Artifact 设计

Classical artifact 必须可复现、可 hash、可迁移：

```rust
pub struct ClassicalModelArtifact {
    pub artifact_id: ModelArtifactId,
    pub family: ClassicalModelFamily,
    pub crate_name: String,
    pub crate_version: String,
    pub feature_schema_hash: Hash,
    pub label_schema_hash: Hash,
    pub training_dataset_hash: Hash,
    pub serialized_model_uri: ArtifactUri,
    pub serialization_format: ModelSerializationFormat,
    pub preprocessing: PreprocessingArtifact,
    pub metrics: ClassicalModelMetrics,
}
```

序列化策略：

- weighted scorer：直接 `serde_json` + canonical hash。
- `smartcore` / `linfa`：必须先 spike 验证 serde 能力；不能序列化 concrete model 时，使用自有 artifact format 或暂不进入生产。
- 所有 artifact 存 object storage 或 artifact directory，Postgres 只存 metadata/hash/URI。

### 15.4 训练闭环

```text
TrainingDatasetArtifact
 -> TrainingMatrix
 -> train candidate models
 -> validation split metrics
 -> PIT backtest
 -> feature importance/explainability
 -> quality gate
 -> shadow report
 -> publish
```

### 15.5 解释性要求

每个 classical model 必须能输出：

- global feature importance。
- per recommendation factor contribution 或 surrogate explanation。
- confidence estimate。
- missing feature handling。
- training coverage。

如果模型无法解释到 recommendation level，只能作为 shadow model，不能进入 auto execution。

### 15.6 Production Guardrails

- model inference 必须有 timeout。
- inference failure 必须降级到 report empty 或 fallback model。
- model artifact hash mismatch 时禁止加载。
- feature schema mismatch 时禁止推理。
- crate version mismatch 必须告警，并默认拒绝加载。

## 16. `good_lp` 与组合优化设计

`good_lp` 不是模型训练框架，它是 portfolio planner 的优化建模层。它解决“选哪些 recommendation、每个分配多少资本、如何满足约束”。

> **As-built（Phase 05.8）**：greedy allocator 已**彻底删除**，组合分配统一为单一 `good_lp` LP/MILP
> 代码路径（`LinearProgrammingPortfolioAllocator`，生产与回测共用）。关键修正：`good_lp` 的纯 Rust
> 后端 `microlp` **原生支持 integer/binary（完整 MILP）**——并非旧文本所述“microlp 仅连续 LP、MILP
> 必须 HiGHS”。因此 `lp-solver`(microlp) 是 research 的**默认 feature**，零 native 依赖、可进
> report_only build。失败阶梯（报告永不崩）：MILP（microlp 默认 / 可选 native HiGHS）→ 连续 LP
> relaxation + 确定性取整（纯 microlp）→ 空 plan。目标函数定稿为
> `maximize Σ wᵢ·uᵢ，wᵢ = scoreᵢ·(1+λ·ret_normᵢ)`，`uᵢ ≤ desired_usdᵢ·xᵢ`（Kelly 风险锚），
> `Σ xᵢ ≤ top_n`（binary 基数，预算只被发布名消耗）。求解 provenance 落
> `quant_portfolio_plan.optimizer_meta_json`（`PortfolioOptimizerMeta`）。详见
> [`phase-05/05.8-portfolio-optimization-good-lp.md`](phase-05/05.8-portfolio-optimization-good-lp.md)。

### 16.1 何时引入

历史背景（已被上方 As-built 取代）：第一版曾使用 deterministic greedy planner：

```text
sort by risk_adjusted_score
 -> iterate
 -> apply budget/exposure/liquidity caps
 -> accept/reject
```

`good_lp` 在 Phase 05.8 落地，因为：

- TopN inclusion 需要全局最优。
- category/event constraints 互相冲突。
- 需要 binary decision。
- 需要同时优化 capital 和 rank utility。

### 16.2 Optimizer Trait

```rust
pub trait PortfolioOptimizer {
    fn optimize(
        &self,
        input: PortfolioOptimizationInput,
    ) -> QuantResult<PortfolioOptimizationOutput>;
}

pub struct PortfolioOptimizationInput {
    pub candidates: Vec<SignalCandidate>,
    pub budget: PortfolioBudget,
    pub constraints: PortfolioConstraints,
    pub objective: PortfolioObjective,
}
```

实现：

- `GreedyPortfolioOptimizer`
- `LinearProgrammingPortfolioOptimizer`

### 16.3 LP/MILP 建模

变量：

```text
x_i ∈ {0, 1}   是否选择 candidate i
u_i >= 0       分配 USD
```

目标：

```text
maximize Σ (risk_adjusted_score_i * x_i) + λ * expected_return_i * u_i
```

约束：

```text
Σ u_i <= total_budget
u_i <= max_usd_i * x_i
u_i >= min_usd_i * x_i
Σ u_i where category=c <= category_cap_c
Σ u_i where event=e <= event_cap_e
u_i <= liquidity_cap_i
Σ x_i <= top_n
```

### 16.4 Solver 选择

| Solver | 优点 | 风险 | 建议 |
|---|---|---|---|
| greedy self-built | 无依赖、确定性 | 非全局最优 | 第一版默认 |
| `good_lp` + pure Rust solver | 部署简单 | 能力/性能有限 | Phase 5 spike |
| `good_lp` + HiGHS | 性能强 | native 依赖 | 大规模后再引入 |
| CBC | MILP 成熟 | native/许可证/部署复杂 | 谨慎 |

### 16.5 生产要求

- 优化必须有 time limit。
- 超时 fallback 到 greedy。
- solver status 进入 report summary。
- infeasible 必须输出 constraints conflict。
- native solver 不得成为 report_only 启动硬依赖。

## 17. `ort` / ONNX 推理生产设计

`ort` 是未来部署外部训练模型的主选项，不是第一版训练依赖。

### 17.1 使用场景

- Python / Rust / AutoML 外部训练后导出 ONNX。
- 需要跨语言统一模型 artifact。
- 需要高性能线上推理。
- 需要后续支持 tree/NN/深度模型统一推理入口。

### 17.2 ONNX Inference Trait

```rust
pub trait OnnxInferenceEngine {
    fn load(&self, artifact: &OnnxArtifactRef) -> QuantResult<LoadedOnnxModel>;

    fn infer(
        &self,
        model: &LoadedOnnxModel,
        input: OnnxInferenceInput,
    ) -> QuantResult<ModelPredictionBatch>;
}

pub struct OnnxArtifactRef {
    pub model_version_id: ModelVersionId,
    pub onnx_uri: ArtifactUri,
    pub input_schema_hash: Hash,
    pub output_schema_hash: Hash,
    pub opset_version: u32,
    pub ort_version: String,
}
```

### 17.3 Artifact Contract

ONNX artifact 必须包含：

- `model.onnx`
- `input_schema.json`
- `output_schema.json`
- `preprocessing.json`
- `postprocessing.json`
- `training_report.json`
- `sha256sum`

### 17.4 Load 流程

```rust
fn load_onnx_model(ref: &OnnxArtifactRef) -> QuantResult<LoadedOnnxModel> {
    verify_artifact_hash(ref)?;
    verify_feature_schema(ref.input_schema_hash)?;
    verify_ort_version(ref.ort_version)?;
    let session = build_ort_session(ref.onnx_uri, ref.execution_provider_policy)?;
    warmup_session(&session)?;
    Ok(LoadedOnnxModel { session, metadata: ref.metadata() })
}
```

### 17.5 推理流程

```text
FeatureVector
 -> preprocessing
 -> ndarray tensor
 -> ONNX input tensors
 -> ort session.run
 -> raw outputs
 -> postprocessing
 -> ModelPrediction
```

### 17.6 风险与约束

- 最新 `ort` 可能要求 MSRV 1.88；当前 workspace 是 1.91（已覆盖）。
- ONNX Runtime 可能引入 binary/native runtime。
- CUDA/TensorRT 等 execution provider 不能默认启用。
- report_only 不应强制加载 ONNX runtime。
- session 初始化必须可失败降级。

### 17.7 引入门槛

必须先完成：

- MSRV decision。
- Docker image spike。
- CPU inference benchmark。
- session warmup benchmark。
- input/output schema mismatch test。
- corrupted artifact test。

## 18. `burn` 生产设计

`burn` 是 Rust-native 深度学习训练的候选，不进入第一版主路径。

### 18.1 使用场景

- 需要训练神经网络。
- 需要 autodiff。
- 希望保留 Rust-native training pipeline。
- 需要多 backend：CPU/WGPU/CUDA/Metal。

### 18.2 Burn Trainer Trait

```rust
pub trait DeepLearningTrainer {
    fn framework(&self) -> DeepLearningFramework;

    async fn train(
        &self,
        request: DeepTrainingRequest,
    ) -> QuantResult<DeepModelArtifact>;
}

pub enum DeepLearningFramework {
    Burn,
}
```

### 18.3 Burn 训练生命周期

```text
TrainingDatasetArtifact
 -> TensorDataset
 -> Burn model init
 -> train loop with Autodiff backend
 -> validation
 -> checkpoint
 -> export artifact
 -> backtest via ModelRunner
 -> quality gates
 -> shadow
```

### 18.4 Backend 策略

| Backend | 用途 | 生产建议 |
|---|---|---|
| Flex/CPU | 小模型、CI、baseline | 可优先 |
| WGPU | cross-platform GPU | spike 后使用 |
| CUDA | 大训练 | 需要独立 Docker |
| Candle backend | 推理兼容 | 后续评估 |

### 18.5 Burn Artifact

必须包含：

- model architecture spec。
- backend-independent weights。
- training config。
- optimizer config。
- checkpoint metadata。
- feature schema hash。
- label schema hash。
- export format。

### 18.6 进入生产条件

- Classical ML 已无法满足目标。
- 数据量足够。
- 训练/验证收益显著。
- 部署和 rollback 已验证。
- artifact 可稳定加载。
- inference latency 可控。

## 19. `candle` 生产设计

`candle` 是 lightweight inference / Hugging Face ecosystem 的候选，尤其适合后续文本、新闻、事件语义类特征。

### 19.1 使用场景

- 需要加载 safetensors。
- 需要 Hugging Face model。
- 需要小 binary / serverless inference。
- 需要文本 embedding / classification / transformers。

### 19.2 Candle Trait

```rust
pub trait CandleInferenceEngine {
    fn load(&self, artifact: &CandleArtifactRef) -> QuantResult<LoadedCandleModel>;

    fn infer_text(
        &self,
        model: &LoadedCandleModel,
        input: TextInferenceInput,
    ) -> QuantResult<TextFeatureOutput>;
}
```

### 19.3 在 quant-pivot 中的定位

`candle` 不直接输出 recommendation。它更适合生成 domain/text features：

```text
news text / market question / external event
 -> Candle embedding/classifier
 -> domain feature
 -> factor engine
 -> model runner
```

### 19.4 Artifact Contract

- safetensors files。
- config.json。
- tokenizer.json if text。
- preprocessing spec。
- output feature schema。
- model hash。

### 19.5 风险

- 模型算子覆盖需要按模型验证。
- GPU backend 需要额外部署验证。
- 大模型不适合直接放进 report generation 同步路径。
- 文本/新闻源本身有数据授权和延迟问题。

## 20. 多模型编排最佳实践

最终系统应该支持多个模型族，但线上只通过统一 interface：

```rust
pub enum ModelRuntime {
    Weighted(WeightedFactorRuntime),
    Classical(Box<dyn ClassicalPredictor>),
    Onnx(Box<dyn OnnxInferenceEngine>),
    Candle(Box<dyn CandleInferenceEngine>),
    Burn(Box<dyn DeepLearningRuntime>),
}

pub trait UnifiedModelRunner {
    async fn infer(
        &self,
        request: UnifiedInferenceRequest,
    ) -> QuantResult<ModelInferenceOutput>;
}
```

规则：

- report builder 只依赖 `UnifiedModelRunner`。
- model registry 决定 artifact 类型。
- runtime config 决定 active model version。
- feature schema mismatch 一律拒绝。
- 推理失败可以 fallback，但 fallback 必须写入 report summary。

### 20.1 退出侧模型编排（Opportunistic Sell — Phase 6）

Report 路径的 `UnifiedModelRunner`（上文）服务 **Buy 侧** TopN 候选。退出侧的 thesis-invalidation
与机会性 Sell 走 **独立 seam**（05.6 已落地）：

| 路径 | Trait | 05.6 seam | 06.0 impl | 后续 impl |
|---|---|---|---|---|
| Thesis 破 / 分数退化 | `ExitSignalEvaluator` → `ThesisInvalidated` | `ReinferenceSignalEvaluator` + `ExitSignalReinferer` | **06.0** `ModelBackedExitSignalReinferer` | — |
| 机会性平仓（thesis 仍成立） | `ExitSignalEvaluator` → `OpportunisticSell` | seam + metric 占位 | — | **Phase 6.1** |

**权威实施契约**：thesis invalidation → [`phase-06/06.0-exit-signal-reinference.md`](phase-06/06.0-exit-signal-reinference.md)；
opportunistic Sell → [`phase-06/06.1-opportunistic-sell-exit-signal.md`](phase-06/06.1-opportunistic-sell-exit-signal.md)
（`CompositeExitSignalEvaluator`、`SellScorerArtifact`、shadow 期、与 05.6 优先级阶梯第 9 档集成）。

规则（与 §20 一致）：

- Exit monitor **不**直接依赖具体模型族；只依赖 `ExitSignalEvaluator` trait。
- Sell scorer artifact 经 model registry 发布；feature schema mismatch → `Indeterminate`（fail-safe hold）。
- Opportunistic 为 **advisory**；ONNX/classical 选型遵循 §17/§15 引入门槛。

## 21. 生产推荐路线

### Stage A：可解释生产最小闭环

使用：

- `polars`
- `ndarray`
- `ndarray-stats`
- `statrs`
- `argmin`
- self-built weighted scorer
- greedy portfolio planner

不使用：

- `smartcore`
- `linfa`
- `good_lp`
- `ort`
- `burn`
- `candle`

### Stage B：Classical ML shadow

增加：

- `smartcore` 或 `linfa`

目标：

- 和 weighted scorer shadow 对比。
- 评估 feature importance。
- 只在质量明显更好时 publish。

### Stage C：组合优化升级

增加：

- `good_lp`

目标：

- 替代 greedy portfolio planner。
- 提升预算/约束冲突下的全局选择质量。

### Stage D：ONNX 推理

增加：

- `ort`

目标：

- 支持外部训练模型推理。
- 统一复杂模型线上部署。

### Stage E：Rust-native DL / 文本特征

评估：

- `burn`
- `candle`

目标：

- deep learning training。
- text/domain feature extraction。

## 22. 最终验收

第三方 ML 栈进入生产前必须满足：

- 每个 crate 都有明确 feature gate。
- 每个模型族都有 artifact contract。
- 每个模型族都能进入 model registry。
- 每个推理 runtime 都能 timeout / fail / fallback。
- 每个训练 runtime 都写 dataset hash、feature schema hash、label schema hash。
- 每个发布模型都有 quality gate report。
- MSRV/native 依赖风险已在 CI 和 Docker 中验证。

## 23. Phase 3.0 依赖引入登记

> 状态：Phase 3.0 落地时登记；2026-07 升级至 1.91（`alloy` 等传递依赖硬要求）。workspace MSRV = 1.91（`resolver = "2"`）。
> `ndarray` / `ndarray-stats` / `statrs` / `rayon` 为 **base deps**（03.2 在线
> feature plane 必需）；`polars` / `smartcore` / `argmin` 仍按 feature gate optional
> 引用；默认 build（`default = []`）链接 base numeric stack，绝不链接 polars /
> smartcore / argmin。

| crate | 版本 | 引入方式 | native 依赖 | 许可证 | 结论 |
|---|---|---|---|---|---|
| `ndarray` | 0.17 | base dep | 无（纯 Rust） | MIT/Apache-2.0 | 引入 |
| `ndarray-stats` | 0.7 | base dep | 无 | MIT/Apache-2.0 | 引入 |
| `statrs` | 0.18 | base dep | 无 | MIT | 引入 |
| `rayon` | 1 | base dep | 无 | MIT/Apache-2.0 | 引入 |
| `polars` | 0.54.4（`lazy` + `parquet`，`default-features = false`） | `dataframe`（默认关） | 无 native runtime（纯 Rust + 编译期 SIMD） | MIT | 引入，仅离线 |
| `arrow` | 59 | `dataframe`（默认关） | 无 | Apache-2.0 | 引入，仅离线（53 与 chrono 0.4.44 的 `quarter()` 冲突，升 59 |
| `parquet` | 59 | `dataframe`（默认关） | 无 | Apache-2.0 | 引入，仅离线 |
| `argmin` | 0.11 | `optimize`（默认关） | 无 | MIT/Apache-2.0 | 引入 |
| `argmin-math` | 0.5 | `optimize`（默认关） | 无 | MIT/Apache-2.0 | 引入 |
| `smartcore` | 0.5 | `ml-classical`（默认关） | 无 | Apache-2.0 | 引入；artifact serde 能力在 3.6 spike 验证 |

禁止本期引入（见 §2 / §30 父文档）：`good_lp`、`ort`、`burn`、`candle`、`tch-rs`。
`smartcore` 的 model artifact 序列化能力（§15.2 风险）留 3.6 spike；本期仅声明依赖与
`ModelArtifact::Classical` 外壳，不落实现。
