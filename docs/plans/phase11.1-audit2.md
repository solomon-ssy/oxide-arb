# Phase 11.1 深度审计报告

**结论先行**：Phase 11.1 的**后端算法内核**（两阶段 FactorEngine、config 驱动截面归一化、Indeterminate 语义、动量族拆分、持久化/report 透传）已基本落地，且与 Coqueret & Guida / skelf 截面预处理主流做法**方向一致**。但**还不能称为「完整闭环的最佳实践落地」**——存在若干 **11.1 范围内应闭合** 的缺口：exit/sell 单市场路径、共线分析数据源、验收测试/doc 漂移、前端未可编译/未接线，以及回测与在线在小截面上的行为不对称。

以下按「已闭合 → 风险/遗漏 → 可优化 → 明确延后」组织。延后项（11.3–11.6 的 publish-gate、PIT、FittedMonotone 等）**不计为未闭环**。

---

## 1. 审计点 #1/#2/#3 闭合度

| 审计点 | 目标 | 实现状态 | 评价 |
|--------|------|----------|------|
| **#1** 默认 3/9 因子 | 启用全部通用族 | `FactorsConfig::default()` → `FactorFamily::ALL_GENERIC`（8 族 **12 因子**） | ✅ 闭合 |
| **#2** `ts.momentum ≡ return` | 独立动量估计量 | 4 因子 + 4 类 `ts.*` 特征；`momentum_roc_not_equal_simple_return` 单测 | ✅ 特征/因子层闭合；**默认集共线实证测试缺失** |
| **#3** 手调 logistic + 伪截面 | config 截面 fit/apply + 无静默 0.5 | `normalize/{cross_section,stats,outcome}`；`SmallCrossSectionPolicy::Indeterminate` 默认 | ✅ 核心闭合；**exit/backtest 单市场路径未统一** |

---

## 2. 算法与方法论：做得对的地方

### 2.1 截面归一化流水线（符合文献主流）

对照 [mlfactor Ch.4](http://www.mlfactor.com/chap_4.html)、[skelf cross-sectional](https://docs.skelfresearch.com/sigc/operators/cross-sectional/)、New Found「winsorize → z-score」：

```64:96:crates/quant-pivot-research/src/factors/normalize/cross_section.rs
    fn fit(&self, column: &RawFactorColumn) -> NormalizationStats {
        // ...
        let lower = quantile_value(&present, self.winsor_p);
        let upper = quantile_value(&present, Decimal::ONE - self.winsor_p);
        // winsorize → mean/std → z-score → ±σ clamp → map [0,1]
```

- **逐 as_of 截面**：batch 混 as_of 直接 fail（`validate_batch_invariants`）——PIT 截面边界正确。
- **winsor_p / clamp_sigma 全来自 config**，generic.rs 无 logistic 常量——零死语义 ✅。
- **零方差 → `Indeterminate::ZeroVariance`**，不是 0.5——与 #3 一致 ✅。
- **Rank 用 average rank / (n−1)**，tie-corrected Spearman 实现合理 ✅。
- **MinMax 走 `PerMarket`**，小截面策略不挡 data_quality——语义正确 ✅。

与文献的差异（**设计取舍，非 bug**）：z-score 被线性映射到 `[0,1]` Probability 供加权 scorer 使用，而非保留 signed z 或 uniformize（Kelly-Pruitt-Su 那套留 11.4）。对当前 `direction × normalized × confidence` 线性模型是自洽的。

### 2.2 动量族语义（#2 根因修复）

```138:180:crates/quant-pivot-research/src/factors/generic.rs
/// The independent momentum family: four distinct estimators (never a return clone)
fn momentum_factors(...) -> ... {
    // momentum_roc, momentum_ema_slope, momentum_vol_adjusted, momentum_macd
}
```

- ROC 带 lag（12-1 思想）、EMA slope、vol-adjusted、MACD norm——与 11.1 §3.1 及 LTR 基线文献一致。
- `ts.return_{W}` 保留为纯特征，不再当动量输入 ✅。

### 2.3 Indeterminate 全链路

```121:133:crates/quant-pivot-research/src/model/weighted/mod.rs
            let contribution = factor.normalized_score().map_or(Decimal::ZERO, |score| {
                // ...
            });
            if factor.is_scored() && confidence > Decimal::ZERO {
                confidence_mass += weight;
```

- Scorer：indeterminate → contribution=0，不进 confidence mass ✅。
- Postgres：`normalized_score` 可空 + `indeterminate_reason` ✅。
- Report：`FactorBreakdownEntry` + composer 透传 ✅。
- CH：scored-only 投影，indeterminate 只在 PG——分层合理 ✅。

### 2.4 在线 pipeline 架构

```126:137:crates/quant-pivot-core/src/service/factor_pipeline.rs
        let history = self.build_history(&engine, &request).await?;
        // ...
            engine.compute_all_batch_with_history(&vectors, &config, &history);
```

在线 batch 走完整两阶段 + 可选 `HistoricalQuantile` history prefetch——生产路径设计正确。

---

## 3. 11.1 范围内应视为「未闭环」的问题

### 🔴 P0 — Exit / Sell 单市场因子重算几乎必然 Indeterminate

计划要求删除单市场伪截面后 **「确认 exit/sell 侧调用后统一」**。当前 **未统一**：

```232:238:crates/quant-pivot-core/src/service/model_backed_reinferer.rs
    let outcomes =
        factor_engine.compute_all_batch(std::slice::from_ref(vector), &config.factors)?;
```

`opportunistic_sell.rs` 同样调用 `factor_outcome`。batch size=1 + 默认 `min_size=5` + 默认 `Indeterminate` 策略 → **几乎所有截面因子（含 required 的 liquidity_depth / spread_efficiency）CrossSectionTooSmall**。

默认 `missing_factor_policy = ZeroWeight`，市场仍 `Eligible`，但 scorer 侧几乎无有效因子贡献 → **退出重推理与入场 report 因子语义严重不对称**。这是 11.1 架构重构的直接后果，应在 11.1 收尾或紧接 hotfix：

- 方案 A：exit 路径 prefetch `FactorHistory` + `HistoricalQuantile`（与在线一致）
- 方案 B：exit 复用 entry 时 frozen `factor_breakdown`，仅重算 exit 相关特征
- 方案 C：exit 专用 `min_size=1` override（需文档化，且与训练不一致——不推荐）

### 🔴 P0 — 回测/训练 materialize 无 history，小截面与在线不一致

```147:148:crates/quant-pivot-core/src/service/historical_replay.rs
    let outcomes = engine.compute_all_batch(&kept_vectors, &config.factors)?;
```

离线 closure（training dataset、backtest replay）**从不**调用 `compute_all_batch_with_history`。Polymarket 某 as_of 可选市场 <5 时，训练/回测会看到大量 indeterminate，在线 report 同 config 同 as_of 却是正常截面分数——**train-serve skew 的种子**（完整 PIT 证明在 11.6，但 11.1 声称 online/backtest 同 normalizer，当前只有 serial≡parallel 测试，不够）。

### 🟠 P1 — 共线分析用 normalized_score，方法论偏了

```173:187:crates/quant-pivot-core/src/app/research_catalog.rs
        // Pivot ... each column is the factor's normalized score (only scored values participate).
        // ...
            row[*column] = Some(score.inner());
```

行业惯例（[stockalpha custom factor](https://stockalpha.ai/alpha-learning/custom-factor-investing-building-your-own-alpha-factors)）是在 **归一化前的 raw factor** 上查 Spearman。用 normalized：

- 不同方法（Rank vs WinsorizedZScore）的因子在同一矩阵里，相关结构被变换扭曲
- 无法检测「raw 已共线、归一化后看起来还行」的假阴性

11.1 交付的 analyzer **应默认 pivot `raw_value`**，normalized 可作为可选视图。

### 🟠 P1 — 文档/注释与测试漂移

1. `collinearity.rs:6` 写「acceptance 断言默认因子集不共线」——**该测试不存在**（只有 synthetic alpha/beta/gamma）。
2. 11.1 §8 缺 **`cross_sectional_zscore_mean_zero_std_one_per_as_of`**（winsorize 单测有，截面统计性质无）。
3. **`online_and_batch_use_same_normalizer`** 只测 serial vs rayon，**不测 historical_replay vs factor_pipeline**。
4. 无 **`HistoricalQuantile`** 路径 acceptance 测试。
5. 死错误变体 **`FactorRequiresBatch`** 仍在 error crate，从未 throw——零死语义债务。

### 🟠 P1 — 前端「同做」未可交付

| 项 | 状态 |
|----|------|
| `views/research/factors/index.vue` | 引用 `./modules/schemas` — **文件不存在，无法编译** |
| 路由/菜单注册 | 未找到 `/research/factors` |
| `recommendation-factors.vue` | 组件存在，**无任何父页面 import** |
| Collinearity API | 硬编码 7d / 0.9，**未读** `factors.orthogonalize.max_correlation` |
| Web 测试 | 无 `/research/factors/collinearity` 集成测试 |

后端 API + 类型 + drawer 骨架有了，**UI 闭环未闭合**。

### 🟡 P2 — 配置与因子绑定细节

1. **多窗口 config 但因子只吃第一个 window**：

```242:251:crates/quant-pivot-research/src/factors/generic.rs
fn momentum_roc_feature(features: &FeaturesConfig) -> FeatureName {
    FeatureName::ts_momentum_roc(
        features.momentum.roc_windows_secs.first().copied().unwrap_or(0),
    )
}
```

默认 `roc_windows_secs = [900, 3600]`，但 `momentum_roc` 因子永远绑 900s；3600s 特征算了却**无对应因子**——浪费算力，也违背「config 驱动因子族」的闭环语义。应对每个 window 注册独立因子，或 config 显式声明「primary window」。

2. **`unwrap_or(0)`**：空 config 会生成 `ts.momentum_roc_0s` 这类无效特征名，fail-open 而非 fail-closed。

3. **`neutralize_by`**：config 有枚举，**无运行时 OLS 残差实现**——若 UI 暴露该字段，属于半死语义（计划里 analyzer-only 可接受，但 config 已 emit 需标注 readonly 或 11.5 前隐藏）。

4. **`normalization_method` 列**：计划提过 PG/CH 增 method 列；实现用 `definition_json` + `normalization_source`，**运行时 per-run method 快照不在 fact 表**——可接受若 `runtime_config_version` 已绑定，但审计链略弱。

---

## 4. 潜在 Bug / 运行时风险

### 4.1 `present_count` vs batch size（行为正确但易误解）

`min_size` 比较的是 **有 raw 的市场数**，不是 batch 总数。100 个市场里只有 4 个有 momentum → CrossSectionTooSmall。这是 fail-closed 合理设计，但运营/文档需写清，否则会被当成 bug。

### 4.2 HistoricalQuantile 分布构造

`build_history` 把 lookback 内**所有市场、所有 as_of** 的 raw 混成一个 per-factor 向量再 fit。这是 **pooled empirical CDF**，不是「同一 as_of 的历史截面」——小 universe 时有实用价值，但：

- 跨市场混合在 Polymarket（异质事件）上语义模糊
- PIT / 离线一致性 → 11.6 明确延后，但 11.1 若启用 HQ 策略应至少在 doc 里 warn

### 4.3 Required 因子 + 小截面 + ZeroWeight

默认下 required 因子 indeterminate **不会** RejectCandidate，市场仍进 model——与「liquidity/spread 是 hard gate」的直觉可能冲突。若生产期望 hard gate，应默认 `RejectCandidate` 或把 required 语义改到 data-quality 层。

### 4.4 12 因子默认集的实际共线风险

4 个动量子因子 + `mean_reversion` + 多个 return 衍生特征，**未经默认 config 下的 Spearman 面板验收**。`momentum_vol_adjusted` 与 `momentum_roc` 在趋势+低 vol 环境下 ρ 可能仍 >0.9。11.5 硬 gate 延后可理解，但 11.1 自己的 acceptance 应补 **`default_generic_factors_not_collinear_on_synthetic_panel`** 或 CI lint。

### 4.5 Rank 历史路径外推

`interpolated_rank` 对「今日 raw 不在历史 sorted 集合」做 below-count 插值——合理，但极端 outlier 会得 0 或 1，无 clamp audit（WinsorizedZScore 历史路径有 winsor 边界，行为不完全对称）。

---

## 5. 性能 / 设计 / 优雅性

| 点 | 现状 | 建议 |
|----|------|------|
| 并行阈值 `PARALLEL_MIN_MARKETS=16` | 合理 | 可 benchmark 调参，非阻塞 |
| Phase B factor-major + rayon | 正确 | — |
| Collinearity O(n²) Spearman | 12 因子规模足够 | 因子数上百时需稀疏或采样 |
| CH 不写 indeterminate | 减 CH 体积 | PG 为权威；研究 UI 查 PG |
| `FactorEngine::new` 每次 reinfer 重建 registry | exit 热路径小开销 | 可 cache engine per config hash |
| feature schema 多窗口冗余计算 | 3600s ROC 算了无因子 | 删冗余或补因子 |

---

## 6. 验收测试对照（11.1 §8）

| 计划测试 | 状态 |
|----------|------|
| `momentum_roc_not_equal_simple_return` | ✅ `features/stats.rs` |
| `cross_sectional_zscore_mean_zero_std_one_per_as_of` | ❌ 缺失 |
| `winsorize_caps_at_configured_percentile` | ✅ `cross_section.rs` unit |
| `small_cross_section_yields_indeterminate_not_half` | ✅ acceptance |
| `zero_variance_yields_indeterminate` | ✅ acceptance |
| `default_factor_config_enables_all_generic_families` | ✅ acceptance |
| `collinearity_gate_blocks_publish` | ⏭ 11.5（预期） |
| `normalizer_has_no_hardcoded_k_x0` | ✅ grep acceptance |
| `online_and_backtest_use_same_normalizer` | ⚠️ 名不副实（仅 serial≡parallel） |
| 默认集不共线 | ❌ 注释声称有，实际无 |
| HistoricalQuantile | ❌ 无 |
| Weighted scorer indeterminate | ❌ 无显式测试 |

19 个 acceptance 测试全绿，但 **§8 清单未完全覆盖**。

---

## 7. 与最佳实践的差距（非延后、值得 11.1 补丁）

1. **共线分析应用 raw panel**（上面 P1）
2. **Exit/sell + historical_replay 与在线同一 history 策略**（P0）
3. **补 3–4 个统计/parity acceptance 测试**
4. **前端 schemas + 路由 + recommendation 接线**
5. **Collinearity API 读 runtime config threshold/lookback**
6. **删除 `FactorRequiresBatch` 或改为真正 emit**
7. **多窗口 → 多因子或显式 primary window**（避免 config 死字段）

---

## 8. 明确延后（不计未闭环）

- `FittedMonotone` + `CalibrationArtifactId` → 11.3  
- `RankCentered` / `Uniformize` → 11.4  
- 共线 **hard publish-gate** → 11.5  
- `HistoricalQuantile` PIT / 离线一致性证明 → 11.6  
- 领域因子 → 11.2  
- 学习 winsor_p / 权重 → 11.4  
- Category `neutralize` 运行时 → 可跟 11.5 gate 一起做  

---

## 9. 总体判定

```mermaid
flowchart LR
    subgraph done [11.1 已闭合]
        A[两阶段 FactorEngine]
        B[Config 截面归一化]
        C[Indeterminate 无静默0.5]
        D[动量族正交化设计]
        E[PG/Report 审计链]
    end
    subgraph gap [11.1 仍缺口]
        F[Exit/Sell 单市场]
        G[Replay 无 History]
        H[共线用 raw 非 normalized]
        I[验收测试/doc 漂移]
        J[前端不可编译/未接线]
    end
    done --> gap
```

**后端算法重构：约 85% 闭合**——审计 #1/#2/#3 的核心语义在 **在线 report batch 路径** 上成立。  
**生产级完整闭环：约 65%**——exit/sell、离线 replay、共线方法论、前端、验收完整性拖后腿。

若只再收 11.1，建议优先级：

1. **统一 exit/sell/replay 的 small-cross-section 策略**（history 或 frozen breakdown）  
2. **共线 API 改 raw_value + 补默认集共线 CI 测试**  
3. **补 parity + zscore 统计 + HQ 路径测试**  
4. **前端 schemas/路由/recommendation 接线**  
5. **清理 doc/死语义（FactorRequiresBatch、collinearity 注释）**

需要的话我可以按上述 P0/P1 直接开 patch（从 exit `factor_outcome` + collinearity raw pivot 开始）。