# Domain Data Sources（垂直领域数据源调研清单）

> 相关：[`03.x-vertical-domain-design.md`](../plans/quant-pivot/phase-03/03.x-vertical-domain-design.md) ·
> Phase 03：[`README.md`](../plans/quant-pivot/phase-03/README.md) ·
> 技术栈：[`08-third-party-crates-and-ml-stack.md`](../plans/quant-pivot/08-third-party-crates-and-ml-stack.md)
>
> 定位：**垂直领域外部数据源的选型真理**。为每个 `DomainFamily` 列出候选数据源，
> 并按"和钱相关"的硬标准评估：PIT/历史可得性、限速、license、**是否对齐 Polymarket
> 结算源**。本文档是 [`03.x`](../plans/quant-pivot/phase-03/03.x-vertical-domain-design.md)
> `DomainDataSource` 实现的选源依据；不引入代码，只锁定选型。

## 1. 选源硬标准（按重要性排序）

和钱直接相关，每条都是 fail-closed 判据：

| # | 标准 | 说明 | 不满足的后果 |
|---|------|------|-------------|
| 1 | **对齐结算源** | 特征/标签源必须与 Polymarket 该市场 `resolution_source` 同源同口径（同交易所、同交易对、同 K 线粒度、同时区） | basis risk：回测好、实盘亏；训练 label 与 live 特征语义分叉 |
| 2 | **PIT 可回放** | 能按历史 `as_of` 取"不晚于该时刻"的观测，且每条带可信 `publish_time` | look-ahead 泄漏，回测虚高 |
| 3 | **历史深度** | 覆盖训练窗口（≥ 1–2 年） | 样本不足，无法训练 category-specific |
| 4 | **License/ToS** | 商用许可、数据再分发条款清晰 | 法务风险，不可生产 |
| 5 | **限速/成本** | 免费层或可接受成本下满足摄取吞吐 | 摄取断流，domain 特征退化为 `DomainDataMissing` |
| 6 | **稳定性** | API 活跃、schema 稳定、有官方文档 | 维护成本高，易腐 |

> **首选原则**：优先选**与结算源同源**的数据源。若结算源本身不开放历史 API，则选与之
> 高度一致的镜像源，并在 `DomainDataSource` 内做**一致性交叉核验**（见 [`03.x`](../plans/quant-pivot/phase-03/03.x-vertical-domain-design.md) D5）。

---

## 2. Crypto（参考垂直，优先落地）

### 2.1 Polymarket crypto 市场的结算结构（地面真理）

Polymarket crypto 市场是二元合约，标题/描述高度结构化，**结算源明示在 description 内**。典型：

> "Will the price of Bitcoin be above $66,000 on June 4?" —
> *This market will resolve to "Yes" if the **Binance 1 minute candle for BTC/USDT** at
> **12:00 ET (noon)** has a final **"Close"** price higher than the strike. Resolution
> source is Binance BTC/USDT.*

⇒ 结算键 = `{asset=BTC, quote=USDT, venue=Binance, candle=1m, field=close, observation_at=12:00 ET, comparator=Above, strike=$66,000}`。
这正是 [`03.x`](../plans/quant-pivot/phase-03/03.x-vertical-domain-design.md) `CryptoSubject` + `ResolutionSourceBinding` 的来源。

### 2.2 候选源对比

| 源 | 对齐结算 | PIT/历史 | 限速（免费） | API key | License | 结论 |
|----|---------|----------|-------------|---------|---------|------|
| **Binance klines** (`/api/v3/klines`) | ✅ **就是结算源**（BTC/USDT 1m close） | ✅ 多年历史，1000 根/请求，含 open_time/close_time | weight 制，市场数据 ~1200/min，公开端点无需 key | 公开市场数据**无需 key** | 市场数据可商用（遵守 ToS） | **首选 concrete `BinanceKlineSource`** |
| Coinbase candles (`/products/{id}/candles`) | ⚠️ 不同交易对/价格 | ✅ 但 300 根/请求需分页 | 公开 ~10 req/s | 无需 key | — | 备选/交叉核验 |
| Pyth Benchmarks (`/v1/updates/price/{ts}`) | ⚠️ 聚合预言机价，非 Binance | ✅ 历史可查、签名可验证 | 同 Hermes 限速 | **2026-07-31 起需 `PYTH_API_KEY`** | 需查 ToS | 备选；适合做跨源校验 |

### 2.3 决策

- **特征源 = 结算源 = Binance BTC/USDT klines**（粒度对齐市场 description 的 candle）。消除 basis risk。
- `time_to_observation` 用市场的 `observation_at`；`distance_to_strike` / `underlying_momentum` /
  `underlying_realized_vol` 全部基于 Binance close 序列。
- **交叉核验**：`basis_vs_resolution_source` 特征 + 结算时刻用 Coinbase/Pyth 比对 Binance close，
  背离超阈值 ⇒ 告警并标记 linkage 复核（不改变 label 真相，仅风控信号）。

---

## 3. Sports

| 源 | 对齐结算 | PIT/历史 | 限速 | License/成本 | 结论 |
|----|---------|----------|------|-------------|------|
| **API-Sports** (api-sports.io) | ⚠️ 赛果通常一致；赔率源不一定同庄 | ✅ 15+ 年历史、2000+ 赛事；赛果 + pre-match/live odds | 免费 100 req/day/API | 免费层永久，付费 $10/mo 起 | **首选**（赛果 + 赔率，性价比高） |
| The-Odds-API (the-odds-api.com) | ⚠️ 多庄聚合赔率 | ✅ 历史快照 2020-06 起（2022-09 后 5min 间隔）；**历史仅付费** | 免费 500 credits/mo（历史×10 倍消耗） | 历史付费 $30/mo 起 | 备选（赔率漂移信号强，但历史要钱） |

**决策**：Sports 结算 = 赛果（Polymarket 用官方比分）。特征侧用 **API-Sports 赛果 + 赔率**（pre-match
momentum / 赔率漂移）。赔率源与 Polymarket 庄口径不同属可接受 noise（赔率是特征不是 label）。
PIT：以 odds snapshot 的 `publish_time` 截断。Sports 实体解析最复杂（队名/赛事/日期消歧），强依赖
[`03.x`](../plans/quant-pivot/phase-03/03.x-vertical-domain-design.md) D1 的 LLM 离线链接 + curated 兜底。

---

## 4. Politics

| 源 | 对齐结算 | PIT/历史 | 限速/成本 | License | 结论 |
|----|---------|----------|----------|---------|------|
| FiveThirtyEight poll 数据（历史 CSV/归档） | ⚠️ 民调≠结算（结算是选举官方结果） | ✅ 历史归档可得；时效性弱 | 静态文件 | 各源不一 | poll momentum 特征源 |
| 官方选举结果（AP/官方公报） | ✅ 结算源 | 结算后才有 | — | — | settlement label 校验 |
| 聚合民调 API（第三方） | ⚠️ | 视源 | 视源 | 视源 | 备选 |

**决策**：Politics 结算 = 官方选举结果（Polymarket 据此）。特征侧用 **poll 聚合的 momentum / shift +
time-to-resolution**。poll 是特征不是 label。PIT：以民调发布日截断（民调发布有天级滞后，`source_delay`
须足够大）。历史民调可得性好但口径杂，需 curated normalization。

---

## 5. Weather

| 源 | 对齐结算 | PIT/历史 | 限速/成本 | License | 结论 |
|----|---------|----------|----------|---------|------|
| **NOAA / NWS API** | ✅（美国气象市场多以 NOAA 站点为结算源） | ✅ 观测 + 预报历史 | 免费、无需 key（合理使用） | 美国政府公共数据 | **首选**（若结算源是 NOAA） |
| **Open-Meteo** | ⚠️ 模型聚合，非官方站点 | ✅ 历史预报 + 历史观测 archive | 免费（非商用宽松，商用需查） | 视用途 | 备选/预报修正特征 |

**决策**：先确认每个 weather 市场 description 的结算站点/源。结算源是 NOAA ⇒ 用 NOAA 观测做 label 校验、
NOAA/Open-Meteo 预报做 `forecast_revision` 特征。PIT：预报有明确 issue time，按 issue time 截断（预报修正
本身就是按 issue time 序列）。

---

## 6. Geopolitics

| 源 | 对齐结算 | PIT/历史 | 限速/成本 | License | 结论 |
|----|---------|----------|----------|---------|------|
| **GDELT** (gdeltproject.org) | ⚠️ 新闻事件流，非结算 | ✅ 15min 更新、长历史归档 | 免费 | 开放（注明出处） | **首选** news shock 特征源 |
| 官方公告/裁决（事件特定） | ✅ 结算源 | 事件后才有 | — | — | settlement label |

**决策**：Geopolitics 结算高度事件特定（官方公告/UMA 裁决）。特征侧用 **GDELT 新闻强度/语调的
shock-decay**。GDELT 是特征不是 label。news shock 需 NLP/embedding（[`03.x`](../plans/quant-pivot/phase-03/03.x-vertical-domain-design.md)
§ ONNX 路径，Phase 06+）；本期可先用 GDELT 数值指标（事件计数/语调）做非文本特征。PIT：按文章
`publish_time` 截断（GDELT 自带）。

---

## 7. 跨垂直汇总

| 垂直 | 特征源（首选） | 结算/label 源 | 特征源是否=结算源 | 历史/PIT | 免费 | 实体解析难度 | 落地优先级 |
|------|---------------|--------------|------------------|----------|------|-------------|-----------|
| **Crypto** | Binance klines | Binance close（同） | ✅ **是** | ✅ 多年 | ✅ | 低（结构化 desc） | **P0 参考垂直** |
| Sports | API-Sports（赛果+赔率） | 官方比分 | ❌（赔率源异） | ✅ 15y | 免费层有限 | 高（队名/赛事消歧） | P2 |
| Politics | poll 聚合 | 官方选举结果 | ❌ | ✅ | 视源 | 中 | P3 |
| Weather | NOAA/Open-Meteo | NOAA 站点 | ⚠️ 视市场 | ✅ | ✅ | 中 | P3 |
| Geopolitics | GDELT | 官方/UMA 裁决 | ❌ | ✅ | ✅ | 高（需 NLP） | P4（Phase 06+ NLP） |

**关键结论**：

1. **只有 Crypto 做到特征源 = 结算源**（Binance），因此 basis risk 最低、PIT 最干净、最适合作参考垂直。
2. 其余垂直的"特征"与"label"必然异源——这是设计常态，**特征是预测信号、label 必须来自结算真相**
   （复用 035 `market_resolution_event`，见 [`03.x`](../plans/quant-pivot/phase-03/03.x-vertical-domain-design.md) D5）。
3. **统一 `DomainDataSource` 抽象**（[`03.x`](../plans/quant-pivot/phase-03/03.x-vertical-domain-design.md) §3.2）让上述各源以
   provider 无关方式接入；首个 concrete 实现为 `BinanceKlineSource`。
4. 凡需 license/付费/凭证的源（The-Odds-API 历史、Pyth 2026-07-31 后、部分商用 weather），凭证经
   deploy `domain_sources` 段下发，**不入代码库**。

---

## 8. 变更记录

| 日期 | 变更 |
|------|------|
| 2026-06 | 初版：五垂直数据源调研；Crypto=Binance 同源结算锁定为 P0 参考垂直；选源硬标准（对齐结算源优先） |
| 2026-06 | 自 Phase 03 计划目录迁至 `docs/operations/`（运维/选型参考，非推进计划） |
