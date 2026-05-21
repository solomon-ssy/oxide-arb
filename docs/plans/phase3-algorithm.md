# Phase 3 — 套利算法

> **产出**: `oxide-arb-algorithm` crate
>
> **前置条件**: Phase 0 (models) + Phase 1 (API) + Phase 2 (storage/repository) 完成
>
> **验收标准**: Endgame 检测在历史 orderbook 数据上复现已知机会；Calibration 后验概率与手动 Beta 计算一致；FillProbabilityEstimator 输出在 [0,1] 区间且单调递减于 staleness

---

## 0. 工作范围

`oxide-arb-algorithm` 是纯计算 crate，不持有连接或运行时状态。所有 I/O 由上层 `oxide-arb-core` 注入。

1. **EndgameStrategy** — 检测价格收敛到 0/1 附近的市场，生成 directional bet 机会
2. **ResolutionCalibrator** — Empirical Bayes 校准系统，追踪 (category, price_zone, duration_bucket) 维度的历史结算准确率
3. **CalibrationUpdater** — 后台增量更新校准桶（Gamma 回填 + CTF 交叉校验）
4. **FillProbabilityEstimator** — 基于深度/staleness/resolution_adjust 的成交概率估计
5. **Scorer** — 综合评分，将 resolution_adjust 融入最终机会排序

---

## 1. 目录结构

```
crates/oxide-arb-algorithm/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── endgame/
    │   ├── mod.rs              # pub use 汇总
    │   ├── strategy.rs         # EndgameStrategy: detect() → Vec<Opportunity<EndgameMeta>>
    │   ├── convergence.rs      # ConvergenceTracker: 跟踪价格持续收敛时长
    │   └── confidence.rs       # ConfidenceFusion: 融合 calibrator + real-time
    ├── calibration/
    │   ├── mod.rs
    │   ├── types.rs            # BucketKey, PriceZone, DurationBucket, CalibrationEntry
    │   ├── calibrator.rs       # ResolutionCalibrator: lookup + posterior + fallback
    │   ├── updater.rs          # CalibrationUpdater: background reconciliation task
    │   ├── prior.rs            # MoM prior estimation (Method of Moments)
    │   └── fallback.rs         # 4-tier fallback chain implementation
    ├── fill_probability/
    │   ├── mod.rs
    │   ├── estimator.rs        # FillProbabilityEstimator
    │   └── factors.rs          # DepthFactor, StalenessFactor, ResolutionFactor
    ├── scorer/
    │   ├── mod.rs
    │   └── endgame_scorer.rs   # EndgameScorer: E[PnL] + category weight + resolution_adjust
    └── opportunity/
        ├── mod.rs
        └── pipeline.rs         # OpportunityPipeline: detect → calibrate → score → filter → emit
```

---

## 2. EndgameStrategy 检测算法

### 2.1 核心逻辑

Endgame 不是传统套利 — 它是 **方向性投注**：在市场即将结算（价格收敛至 0/1）时，以低于面值的价格买入预测获胜侧的 token，等待结算获利。

```rust
use oxide_arb_models::{
    config::detection::EndgameConfig,
    domain::opportunity::Opportunity,
    domain::strategy_meta::EndgameMeta,
    enums::common::Side,
    types::*,
};
use rust_decimal::Decimal;

pub struct EndgameStrategy {
    config: EndgameConfig,
    convergence: ConvergenceTracker,
    calibrator: Arc<ResolutionCalibrator>,
    fusion: ConfidenceFusion,
}

impl EndgameStrategy {
    /// Detect endgame opportunities on a single market.
    ///
    /// Steps:
    /// 1. Check if market is within the settlement window
    /// 2. Walk the YES-side orderbook to check if best ask >= high_threshold (YES likely)
    ///    OR walk the NO-side orderbook to check if best ask >= high_threshold (NO likely)
    /// 3. Verify convergence duration meets minimum
    /// 4. Compute entry cost by walking depth up to max_investment_usd
    /// 5. Apply calibration to get resolution probability
    /// 6. Fuse calibrator p_correct with real-time confidence
    /// 7. Compute E[PnL] = p × (shares × 1.0 - cost - fees) - (1-p) × cost
    /// 8. Filter by min_profit_per_share
    pub fn detect(
        &self,
        market: &MarketSnapshot,
        book: &OrderBook,
        now: DateTime<Utc>,
    ) -> Option<Opportunity<EndgameMeta>> {
        // Step 1: Settlement window check
        let deadline = market.end_date?;
        let hours_to_settlement = (deadline - now).num_hours();
        if hours_to_settlement < 0
            || hours_to_settlement > self.config.settlement_window_hours as i64
        {
            return None;
        }

        // Step 2: Detect convergence direction
        let direction = self.detect_convergence_direction(book)?;

        // Step 3: Convergence duration
        let convergence_secs = self.convergence.update_and_get(
            &market.market_id,
            direction,
            now,
        );
        if convergence_secs < self.config.min_convergence_duration_secs {
            return None;
        }

        // Step 4: Walk the orderbook
        let walk_result = self.walk_orderbook(book, direction)?;

        // Step 5: Calibration lookup
        let bucket_key = BucketKey {
            category: market.category.to_string(),
            price_zone: PriceZone::from_price(walk_result.avg_entry_price),
            duration_bucket: DurationBucket::from_secs(convergence_secs),
        };
        let cal_entry = self.calibrator.lookup(&bucket_key);

        // Step 6: Confidence fusion
        let realtime_confidence = self.compute_realtime_confidence(
            walk_result.avg_entry_price,
            convergence_secs,
        );
        let fused_p = self.fusion.fuse(
            cal_entry.posterior_mean(),
            realtime_confidence,
            cal_entry.sample_count(),
        );

        // Step 7: E[PnL]
        let gain_if_correct = walk_result.shares.inner() - walk_result.total_cost.inner()
            - walk_result.total_fees.inner();
        let loss_if_wrong = walk_result.total_cost.inner() + walk_result.total_fees.inner();
        let expected_pnl = fused_p * gain_if_correct - (Decimal::ONE - fused_p) * loss_if_wrong;

        // Step 8: Filter
        let profit_per_share = expected_pnl / walk_result.shares.inner();
        if profit_per_share < self.config.min_profit_per_share {
            return None;
        }

        Some(self.build_opportunity(market, walk_result, direction, fused_p, convergence_secs, cal_entry, deadline))
    }
}
```

### 2.2 收敛方向检测

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConvergenceDirection {
    YesLikely,  // YES ask >= threshold → buy YES token
    NoLikely,   // NO ask >= threshold (equivalently YES bid <= 1-threshold) → buy NO token
}

impl EndgameStrategy {
    fn detect_convergence_direction(&self, book: &OrderBook) -> Option<ConvergenceDirection> {
        let yes_ask = book.best_ask()?;
        let no_ask = Price::new(Decimal::ONE - book.best_bid()?.inner());

        if yes_ask.inner() >= self.config.high_threshold {
            Some(ConvergenceDirection::YesLikely)
        } else if no_ask.inner() >= self.config.high_threshold {
            Some(ConvergenceDirection::NoLikely)
        } else {
            None
        }
    }
}
```

### 2.3 Orderbook Walk

```rust
pub struct WalkResult {
    pub side: Side,
    pub token_id: TokenId,
    pub shares: Shares,
    pub avg_entry_price: Price,
    pub total_cost: Usd,
    pub total_fees: Usd,
    pub depth_used_pct: Decimal,
    pub levels_consumed: usize,
}

impl EndgameStrategy {
    /// Walk the ask side to buy the predicted winner token.
    ///
    /// For YesLikely: buy YES token by walking YES asks.
    /// For NoLikely: buy NO token by walking NO asks (YES bids, inverted).
    ///
    /// Stops when:
    /// - max_investment_usd is reached
    /// - no more liquidity
    /// - price crosses below the convergence threshold
    fn walk_orderbook(
        &self,
        book: &OrderBook,
        direction: ConvergenceDirection,
    ) -> Option<WalkResult> {
        let max_cost = self.config.max_investment_usd;
        let threshold = self.config.high_threshold;

        let mut total_shares = Decimal::ZERO;
        let mut total_cost = Decimal::ZERO;
        let mut levels_consumed = 0_usize;

        let asks = match direction {
            ConvergenceDirection::YesLikely => book.asks(),
            ConvergenceDirection::NoLikely => book.inverted_asks(),
        };

        let total_depth = asks.iter().map(|l| l.size * l.price).sum::<Decimal>();

        for level in asks {
            if level.price < threshold {
                break; // Price dropped below convergence zone
            }

            let remaining_budget = max_cost - total_cost;
            if remaining_budget <= Decimal::ZERO {
                break;
            }

            let affordable_shares = remaining_budget / level.price;
            let fill_shares = affordable_shares.min(level.size);

            total_shares += fill_shares;
            total_cost += fill_shares * level.price;
            levels_consumed += 1;
        }

        if total_shares.is_zero() {
            return None;
        }

        let avg_price = total_cost / total_shares;
        let depth_used_pct = if total_depth.is_zero() {
            Decimal::ZERO
        } else {
            total_cost / total_depth * Decimal::from(100)
        };

        // Fee calculation (delegated to FeeService)
        let fees = self.calculate_fees(total_shares, avg_price);

        Some(WalkResult {
            side: match direction {
                ConvergenceDirection::YesLikely => Side::Buy,
                ConvergenceDirection::NoLikely => Side::Buy,
            },
            token_id: match direction {
                ConvergenceDirection::YesLikely => book.yes_token_id().clone(),
                ConvergenceDirection::NoLikely => book.no_token_id().clone(),
            },
            shares: Shares::new(total_shares),
            avg_entry_price: Price::new(avg_price),
            total_cost: Usd::new(total_cost),
            total_fees: Usd::new(fees),
            depth_used_pct,
            levels_consumed,
        })
    }
}
```

---

## 3. ConvergenceTracker

```rust
use moka::sync::Cache;
use std::time::Duration;

pub struct ConvergenceTracker {
    entries: Cache<MarketId, ConvergenceEntry>,
}

struct ConvergenceEntry {
    direction: ConvergenceDirection,
    first_seen: DateTime<Utc>,
    last_updated: DateTime<Utc>,
}

impl ConvergenceTracker {
    pub fn new(max_age_secs: u64) -> Self {
        let entries = Cache::builder()
            .max_capacity(10_000)
            .time_to_idle(Duration::from_secs(max_age_secs))
            .build();
        Self { entries }
    }

    /// Update convergence state and return duration in seconds.
    ///
    /// If the direction changes or the entry expired, the timer resets to 0.
    pub fn update_and_get(
        &self,
        market_id: &MarketId,
        direction: ConvergenceDirection,
        now: DateTime<Utc>,
    ) -> u64 {
        let entry = self.entries.get(market_id);

        match entry {
            Some(existing) if existing.direction == direction => {
                let duration = (now - existing.first_seen).num_seconds().max(0) as u64;
                self.entries.insert(
                    market_id.clone(),
                    ConvergenceEntry {
                        direction,
                        first_seen: existing.first_seen,
                        last_updated: now,
                    },
                );
                duration
            }
            _ => {
                self.entries.insert(
                    market_id.clone(),
                    ConvergenceEntry {
                        direction,
                        first_seen: now,
                        last_updated: now,
                    },
                );
                0
            }
        }
    }

    /// Remove tracking for a market (called on resolution).
    pub fn remove(&self, market_id: &MarketId) {
        self.entries.remove(market_id);
    }
}
```

---

## 4. Calibration 系统

### 4.1 核心类型

```rust
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};

/// Composite key for a calibration bucket.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BucketKey {
    pub category: String,     // MarketCategory as string
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
}

/// Price zone classification for calibration granularity.
///
/// Finer zones near 1.0 because small price differences at the extreme
/// have outsized impact on expected return.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
pub enum PriceZone {
    /// [0.95, 0.96) — weakest convergence signal
    Z95,
    /// [0.96, 0.97)
    Z96,
    /// [0.97, 0.98)
    Z97,
    /// [0.98, 0.99)
    Z98,
    /// [0.99, 1.00] — strongest convergence signal
    Z99,
}

impl PriceZone {
    pub fn from_price(price: Price) -> Self {
        let p = price.inner();
        if p >= dec!(0.99) {
            Self::Z99
        } else if p >= dec!(0.98) {
            Self::Z98
        } else if p >= dec!(0.97) {
            Self::Z97
        } else if p >= dec!(0.96) {
            Self::Z96
        } else {
            Self::Z95
        }
    }

    /// Midpoint of the zone, used for prior estimation.
    pub fn midpoint(&self) -> Decimal {
        match self {
            Self::Z95 => dec!(0.955),
            Self::Z96 => dec!(0.965),
            Self::Z97 => dec!(0.975),
            Self::Z98 => dec!(0.985),
            Self::Z99 => dec!(0.995),
        }
    }
}

/// Duration bucket for how long a market has been converged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display, EnumString)]
pub enum DurationBucket {
    /// 5 min – 1 hour
    Short,
    /// 1 hour – 6 hours
    Medium,
    /// 6 hours – 24 hours
    Long,
    /// > 24 hours
    VeryLong,
}

impl DurationBucket {
    pub fn from_secs(secs: u64) -> Self {
        match secs {
            0..=3599 => Self::Short,
            3600..=21599 => Self::Medium,
            21600..=86399 => Self::Long,
            _ => Self::VeryLong,
        }
    }
}

/// A single calibration entry (corresponds to one DB row).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalibrationEntry {
    pub bucket_key: BucketKey,
    pub total_count: u32,
    pub correct_count: u32,
    pub alpha_prior: Decimal,
    pub beta_prior: Decimal,
}

impl CalibrationEntry {
    /// Empirical Bayes posterior mean: (α + correct) / (α + β + total).
    pub fn posterior_mean(&self) -> Decimal {
        let alpha = self.alpha_prior + Decimal::from(self.correct_count);
        let beta = self.beta_prior + Decimal::from(self.total_count - self.correct_count);
        alpha / (alpha + beta)
    }

    /// Number of observations in this bucket.
    pub fn sample_count(&self) -> u32 {
        self.total_count
    }

    /// Whether the bucket has enough data for reliable estimation.
    pub fn is_credible(&self, min_samples: u32) -> bool {
        self.total_count >= min_samples
    }
}
```

### 4.2 ResolutionCalibrator

```rust
use dashmap::DashMap;

pub struct ResolutionCalibrator {
    buckets: DashMap<BucketKey, CalibrationEntry>,
    config: CalibrationConfig,
}

/// Configuration for the calibration system.
#[derive(Debug, Clone, Deserialize)]
pub struct CalibrationConfig {
    /// Minimum sample count per bucket before the estimate is considered
    /// reliable. Below this, the fallback chain is activated.
    /// Default: 10.
    pub min_samples_per_bucket: u32,

    /// Prior strength parameter n₀ for the dynamic fusion weight.
    /// Higher values give more weight to the calibrator (slower adaptation).
    /// Default: 20.
    pub fusion_prior_strength: u32,

    /// Floor for fused probability. Prevents Kelly sizer from over-betting
    /// on edge cases where calibrator+realtime both return near 1.0.
    /// Default: 0.80.
    pub fused_p_floor: Decimal,

    /// Ceiling for fused probability. Default: 0.995.
    pub fused_p_ceiling: Decimal,

    /// How often (seconds) the calibration updater runs. Default: 3600.
    pub update_interval_secs: u64,

    /// Global prior α for bootstrap (before MoM estimation). Default: 2.0.
    pub bootstrap_alpha: Decimal,

    /// Global prior β for bootstrap. Default: 0.2.
    pub bootstrap_beta: Decimal,
}

impl ResolutionCalibrator {
    /// Load all calibration buckets from the repository into memory.
    pub fn from_entries(entries: Vec<CalibrationEntry>, config: CalibrationConfig) -> Self {
        let buckets = DashMap::new();
        for entry in entries {
            buckets.insert(entry.bucket_key.clone(), entry);
        }
        Self { buckets, config }
    }

    /// Lookup with 4-tier fallback chain.
    ///
    /// Tier 1: Exact match (category, price_zone, duration_bucket)
    /// Tier 2: Same category + price_zone, any duration → aggregate
    /// Tier 3: Same price_zone, any category → aggregate (cross-category)
    /// Tier 4: Global prior (bootstrap α/β)
    pub fn lookup(&self, key: &BucketKey) -> CalibrationEntry {
        // Tier 1: exact match
        if let Some(entry) = self.buckets.get(key) {
            if entry.is_credible(self.config.min_samples_per_bucket) {
                return entry.clone();
            }
        }

        // Tier 2: same category + zone, any duration
        let tier2 = self.aggregate_by_category_zone(&key.category, key.price_zone);
        if tier2.is_credible(self.config.min_samples_per_bucket) {
            return tier2;
        }

        // Tier 3: same zone, any category
        let tier3 = self.aggregate_by_zone(key.price_zone);
        if tier3.is_credible(self.config.min_samples_per_bucket) {
            return tier3;
        }

        // Tier 4: global prior
        CalibrationEntry {
            bucket_key: key.clone(),
            total_count: 0,
            correct_count: 0,
            alpha_prior: self.config.bootstrap_alpha,
            beta_prior: self.config.bootstrap_beta,
        }
    }

    fn aggregate_by_category_zone(
        &self,
        category: &str,
        zone: PriceZone,
    ) -> CalibrationEntry {
        let mut total = 0u32;
        let mut correct = 0u32;
        let mut alpha_sum = Decimal::ZERO;
        let mut beta_sum = Decimal::ZERO;
        let mut count = 0u32;

        for entry in self.buckets.iter() {
            if entry.key().category == category && entry.key().price_zone == zone {
                total += entry.total_count;
                correct += entry.correct_count;
                alpha_sum += entry.alpha_prior;
                beta_sum += entry.beta_prior;
                count += 1;
            }
        }

        let (alpha, beta) = if count > 0 {
            (alpha_sum / Decimal::from(count), beta_sum / Decimal::from(count))
        } else {
            (self.config.bootstrap_alpha, self.config.bootstrap_beta)
        };

        CalibrationEntry {
            bucket_key: BucketKey {
                category: category.to_string(),
                price_zone: zone,
                duration_bucket: DurationBucket::Short, // placeholder
            },
            total_count: total,
            correct_count: correct,
            alpha_prior: alpha,
            beta_prior: beta,
        }
    }

    fn aggregate_by_zone(&self, zone: PriceZone) -> CalibrationEntry {
        let mut total = 0u32;
        let mut correct = 0u32;
        let mut alpha_sum = Decimal::ZERO;
        let mut beta_sum = Decimal::ZERO;
        let mut count = 0u32;

        for entry in self.buckets.iter() {
            if entry.key().price_zone == zone {
                total += entry.total_count;
                correct += entry.correct_count;
                alpha_sum += entry.alpha_prior;
                beta_sum += entry.beta_prior;
                count += 1;
            }
        }

        let (alpha, beta) = if count > 0 {
            (alpha_sum / Decimal::from(count), beta_sum / Decimal::from(count))
        } else {
            (self.config.bootstrap_alpha, self.config.bootstrap_beta)
        };

        CalibrationEntry {
            bucket_key: BucketKey {
                category: "global".into(),
                price_zone: zone,
                duration_bucket: DurationBucket::Short,
            },
            total_count: total,
            correct_count: correct,
            alpha_prior: alpha,
            beta_prior: beta,
        }
    }

    /// Update a bucket with a new observation.
    pub fn record_outcome(&self, key: &BucketKey, was_correct: bool) {
        self.buckets
            .entry(key.clone())
            .and_modify(|e| {
                e.total_count += 1;
                if was_correct {
                    e.correct_count += 1;
                }
            })
            .or_insert_with(|| CalibrationEntry {
                bucket_key: key.clone(),
                total_count: 1,
                correct_count: if was_correct { 1 } else { 0 },
                alpha_prior: self.config.bootstrap_alpha,
                beta_prior: self.config.bootstrap_beta,
            });
    }

    /// Replace all in-memory buckets (called after full DB reload).
    pub fn reload(&self, entries: Vec<CalibrationEntry>) {
        self.buckets.clear();
        for entry in entries {
            self.buckets.insert(entry.bucket_key.clone(), entry);
        }
    }
}
```

### 4.3 MoM Prior Estimation (Method of Moments)

```rust
/// Estimate Beta distribution priors (α₀, β₀) from observed bucket statistics
/// using Method of Moments.
///
/// Given a collection of bucket-level empirical rates p̂ᵢ = correct_i / total_i,
/// estimates the population-level Beta(α₀, β₀) that generated those rates.
///
/// MoM equations:
///   μ = mean(p̂ᵢ)
///   v = var(p̂ᵢ)
///   α₀ = μ × (μ(1-μ)/v - 1)
///   β₀ = (1-μ) × (μ(1-μ)/v - 1)
///
/// Falls back to config.bootstrap_alpha/beta if fewer than 3 buckets
/// have data or variance is zero/negative.
pub fn estimate_mom_prior(
    entries: &[CalibrationEntry],
    min_samples: u32,
    fallback_alpha: Decimal,
    fallback_beta: Decimal,
) -> (Decimal, Decimal) {
    let rates: Vec<Decimal> = entries
        .iter()
        .filter(|e| e.total_count >= min_samples)
        .map(|e| Decimal::from(e.correct_count) / Decimal::from(e.total_count))
        .collect();

    if rates.len() < 3 {
        return (fallback_alpha, fallback_beta);
    }

    let n = Decimal::from(rates.len() as u32);
    let mu = rates.iter().sum::<Decimal>() / n;

    let variance = rates.iter()
        .map(|p| (*p - mu) * (*p - mu))
        .sum::<Decimal>() / (n - Decimal::ONE);

    if variance.is_zero() || variance.is_sign_negative() {
        return (fallback_alpha, fallback_beta);
    }

    let mu_complement = Decimal::ONE - mu;
    let common = mu * mu_complement / variance - Decimal::ONE;

    if common <= Decimal::ZERO {
        return (fallback_alpha, fallback_beta);
    }

    let alpha = mu * common;
    let beta = mu_complement * common;

    // Sanity: both must be positive
    if alpha > Decimal::ZERO && beta > Decimal::ZERO {
        (alpha, beta)
    } else {
        (fallback_alpha, fallback_beta)
    }
}
```

---

## 5. CalibrationUpdater 后台任务

```rust
use oxide_arb_models::types::MarketId;

/// Traits for external dependencies injected by oxide-arb-core.
#[async_trait]
pub trait CalibrationDataSource: Send + Sync + 'static {
    /// Get all unresolved calibration outcomes from the DB.
    async fn get_unresolved_outcomes(&self) -> Result<Vec<UnresolvedOutcome>, AlgoError>;

    /// Check market resolution via Gamma API.
    async fn check_gamma_resolution(&self, market_id: &MarketId) -> Result<Option<bool>, AlgoError>;

    /// Check market resolution via CTF on-chain oracle.
    async fn check_ctf_resolution(&self, market_id: &MarketId) -> Result<Option<bool>, AlgoError>;

    /// Persist updated calibration buckets.
    async fn save_buckets(&self, entries: &[CalibrationEntry]) -> Result<(), AlgoError>;

    /// Mark an outcome as resolved.
    async fn resolve_outcome(
        &self,
        outcome_id: &uuid::Uuid,
        actual_yes: bool,
    ) -> Result<(), AlgoError>;
}

pub struct CalibrationUpdater {
    calibrator: Arc<ResolutionCalibrator>,
    data_source: Arc<dyn CalibrationDataSource>,
    config: CalibrationConfig,
}

impl CalibrationUpdater {
    /// Single tick of the calibration reconciliation loop.
    ///
    /// 1. Fetch all unresolved outcomes from the DB
    /// 2. For each, query Gamma API for resolution
    /// 3. Cross-check with CTF on-chain oracle (2-of-2 for finality)
    /// 4. Update calibration buckets with confirmed outcomes
    /// 5. Re-estimate MoM priors if enough new data
    /// 6. Persist updated buckets to DB
    pub async fn tick(&self) -> Result<UpdateStats, AlgoError> {
        let unresolved = self.data_source.get_unresolved_outcomes().await?;
        let mut resolved_count = 0u32;
        let mut gamma_miss = 0u32;

        for outcome in &unresolved {
            // Gamma check
            let gamma_result = self.data_source
                .check_gamma_resolution(&outcome.market_id)
                .await;

            let gamma_yes = match gamma_result {
                Ok(Some(yes)) => yes,
                Ok(None) => { gamma_miss += 1; continue; }
                Err(_) => { gamma_miss += 1; continue; }
            };

            // CTF cross-check (optional, best-effort)
            let ctf_result = self.data_source
                .check_ctf_resolution(&outcome.market_id)
                .await;

            let confirmed = match ctf_result {
                Ok(Some(ctf_yes)) => {
                    if ctf_yes != gamma_yes {
                        tracing::warn!(
                            market_id = %outcome.market_id,
                            gamma = gamma_yes,
                            ctf = ctf_yes,
                            "Gamma/CTF disagree — skipping"
                        );
                        continue;
                    }
                    gamma_yes
                }
                _ => gamma_yes, // CTF unavailable, trust Gamma alone
            };

            // Update in-memory calibrator
            let was_correct = confirmed == outcome.predicted_yes;
            self.calibrator.record_outcome(&outcome.bucket_key, was_correct);

            // Persist resolution
            self.data_source.resolve_outcome(&outcome.outcome_id, confirmed).await?;
            resolved_count += 1;
        }

        // Re-estimate MoM priors periodically
        if resolved_count > 0 {
            self.update_priors().await?;
        }

        Ok(UpdateStats {
            total_unresolved: unresolved.len() as u32,
            resolved: resolved_count,
            gamma_miss,
        })
    }

    async fn update_priors(&self) -> Result<(), AlgoError> {
        let all_entries: Vec<CalibrationEntry> = self.calibrator.buckets
            .iter()
            .map(|e| e.value().clone())
            .collect();

        let (alpha, beta) = estimate_mom_prior(
            &all_entries,
            self.config.min_samples_per_bucket,
            self.config.bootstrap_alpha,
            self.config.bootstrap_beta,
        );

        // Update priors on buckets with fewer than min_samples
        for mut entry in self.calibrator.buckets.iter_mut() {
            if entry.total_count < self.config.min_samples_per_bucket {
                entry.alpha_prior = alpha;
                entry.beta_prior = beta;
            }
        }

        // Persist all buckets
        self.data_source.save_buckets(&all_entries).await?;
        Ok(())
    }
}

pub struct UpdateStats {
    pub total_unresolved: u32,
    pub resolved: u32,
    pub gamma_miss: u32,
}

pub struct UnresolvedOutcome {
    pub outcome_id: uuid::Uuid,
    pub market_id: MarketId,
    pub bucket_key: BucketKey,
    pub predicted_yes: bool,
}
```

---

## 6. ConfidenceFusion

```rust
/// Fuses calibrator posterior probability with real-time convergence confidence.
///
/// Uses dynamic weight w(n) = n / (n + n₀) where:
/// - n = number of observations in the calibration bucket
/// - n₀ = prior strength parameter (config.fusion_prior_strength)
///
/// fused_p = w × p_calibrator + (1-w) × p_realtime
///
/// When n is small (few observations), real-time confidence dominates.
/// As n grows, the calibrator's posterior takes over.
pub struct ConfidenceFusion {
    prior_strength: Decimal,  // n₀
    p_floor: Decimal,
    p_ceiling: Decimal,
}

impl ConfidenceFusion {
    pub fn new(config: &CalibrationConfig) -> Self {
        Self {
            prior_strength: Decimal::from(config.fusion_prior_strength),
            p_floor: config.fused_p_floor,
            p_ceiling: config.fused_p_ceiling,
        }
    }

    /// Fuse calibrator posterior with real-time confidence.
    pub fn fuse(
        &self,
        p_calibrator: Decimal,
        p_realtime: Decimal,
        sample_count: u32,
    ) -> Decimal {
        let n = Decimal::from(sample_count);
        let w = n / (n + self.prior_strength);

        let raw = w * p_calibrator + (Decimal::ONE - w) * p_realtime;

        // Clamp to [floor, ceiling]
        raw.max(self.p_floor).min(self.p_ceiling)
    }
}
```

### 6.1 实时置信度计算

```rust
impl EndgameStrategy {
    /// Compute real-time confidence from orderbook state.
    ///
    /// Factors:
    /// 1. Price proximity to 1.0 (or 0.0): closer → higher confidence
    /// 2. Convergence duration: longer → higher confidence (log scale)
    /// 3. Depth asymmetry: deeper ask in convergence direction → higher
    fn compute_realtime_confidence(
        &self,
        entry_price: Price,
        convergence_secs: u64,
    ) -> Decimal {
        let p = entry_price.inner();

        // Factor 1: price proximity — linear map [0.95, 1.0] → [0.80, 0.99]
        let price_conf = dec!(0.80) + (p - dec!(0.95)) / dec!(0.05) * dec!(0.19);

        // Factor 2: duration — log saturation
        // log2(secs/300 + 1) / log2(max/300 + 1), clamped to [0, 1]
        let norm_duration = (convergence_secs as f64 / 300.0 + 1.0).log2()
            / (86400.0 / 300.0 + 1.0).log2();
        let duration_conf = Decimal::try_from(norm_duration.clamp(0.0, 1.0))
            .unwrap_or(dec!(0.5));

        // Weighted combination
        let raw = dec!(0.7) * price_conf + dec!(0.3) * duration_conf;
        raw.max(dec!(0.50)).min(dec!(0.995))
    }
}
```

---

## 7. FillProbabilityEstimator

```rust
/// Estimates the probability that a FOK order will fill at the desired price.
///
/// For endgame strategy, this is simpler than multi-leg arbitrage because
/// we always place a single FOK order. Key factors:
///
/// 1. Depth factor: how much of the available depth we're consuming
/// 2. Staleness factor: how old the orderbook data is
/// 3. Resolution adjustment: markets near resolution have more stable books
pub struct FillProbabilityEstimator {
    config: FillProbabilityEstimatorConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FillProbabilityEstimatorConfig {
    /// Base fill probability for a single FOK order with fresh data. Default: 0.90.
    pub base_fill_prob: Decimal,
    /// Depth usage percentage above which fill probability drops sharply. Default: 20%.
    pub depth_penalty_threshold_pct: Decimal,
    /// Per-percentage-point penalty above the threshold. Default: 0.02.
    pub depth_penalty_per_pct: Decimal,
    /// Staleness penalty per StalenessLevel step. Default: 0.05.
    pub staleness_penalty_per_level: Decimal,
    /// Resolution proximity bonus (0-1). Closer to settlement → more stable book. Default: 0.05.
    pub resolution_proximity_bonus: Decimal,
}

impl FillProbabilityEstimator {
    pub fn estimate(
        &self,
        depth_used_pct: Decimal,
        staleness: StalenessLevel,
        hours_to_settlement: i64,
    ) -> Decimal {
        let mut p = self.config.base_fill_prob;

        // Depth penalty
        let excess_depth = (depth_used_pct - self.config.depth_penalty_threshold_pct)
            .max(Decimal::ZERO);
        p -= excess_depth * self.config.depth_penalty_per_pct;

        // Staleness penalty
        let staleness_steps = match staleness {
            StalenessLevel::Fresh => Decimal::ZERO,
            StalenessLevel::Acceptable => Decimal::ONE,
            StalenessLevel::Stale => Decimal::from(2),
            StalenessLevel::Expired => Decimal::from(3),
        };
        p -= staleness_steps * self.config.staleness_penalty_per_level;

        // Resolution proximity bonus (within 6 hours)
        if hours_to_settlement <= 6 && hours_to_settlement >= 0 {
            let bonus_fraction = Decimal::ONE
                - Decimal::from(hours_to_settlement) / Decimal::from(6);
            p += self.config.resolution_proximity_bonus * bonus_fraction;
        }

        p.max(dec!(0.10)).min(dec!(0.99))
    }
}
```

---

## 8. Scorer 集成

```rust
/// Endgame-specific scorer that ranks opportunities by risk-adjusted expected PnL.
///
/// Score = E[PnL] × fill_probability × category_weight × resolution_adjust
pub struct EndgameScorer {
    category_weights: CategoryWeights,
    fill_estimator: FillProbabilityEstimator,
}

impl EndgameScorer {
    /// Score an endgame opportunity.
    ///
    /// resolution_adjust is the fused calibration probability — it modulates
    /// the raw E[PnL] to account for historical resolution accuracy.
    pub fn score(&self, opp: &Opportunity<EndgameMeta>) -> ScoredOpportunity {
        let category_weight = self.category_weights.get(&opp.category);

        let fill_prob = self.fill_estimator.estimate(
            opp.depth_used_pct,
            opp.staleness,
            self.hours_to_settlement(opp),
        );

        // resolution_adjust is already baked into expected_net_profit during detection.
        // The scorer applies fill_probability and category_weight as additional discounts.
        let adjusted_pnl = opp.expected_net_profit.inner() * fill_prob * category_weight;

        ScoredOpportunity {
            opportunity: opp.clone(),
            score: adjusted_pnl,
            fill_probability: fill_prob,
            category_weight,
        }
    }

    fn hours_to_settlement(&self, opp: &Opportunity<EndgameMeta>) -> i64 {
        let now = chrono::Utc::now();
        (opp.meta.settlement_deadline - now).num_hours()
    }
}

pub struct ScoredOpportunity {
    pub opportunity: Opportunity<EndgameMeta>,
    pub score: Decimal,
    pub fill_probability: Decimal,
    pub category_weight: Decimal,
}
```

---

## 9. Opportunity 发射管线

```rust
/// End-to-end pipeline: detect → calibrate → score → filter → emit.
///
/// Called by `oxide-arb-core` on each market data update or periodic scan.
pub struct OpportunityPipeline {
    strategy: EndgameStrategy,
    scorer: EndgameScorer,
    min_score: Decimal,
    max_depth_usage_pct: Decimal,
}

impl OpportunityPipeline {
    /// Process a single market update and optionally emit an opportunity.
    pub fn process(
        &self,
        market: &MarketSnapshot,
        book: &OrderBook,
        now: DateTime<Utc>,
    ) -> Option<ScoredOpportunity> {
        // 1. Detect
        let opp = self.strategy.detect(market, book, now)?;

        // 2. Filter: depth usage
        if opp.depth_used_pct > self.max_depth_usage_pct {
            tracing::debug!(
                market_id = %opp.market_id,
                depth_pct = %opp.depth_used_pct,
                "Depth usage exceeds limit"
            );
            return None;
        }

        // 3. Filter: staleness
        if opp.staleness == StalenessLevel::Expired {
            return None;
        }

        // 4. Score
        let scored = self.scorer.score(&opp);

        // 5. Filter: minimum score
        if scored.score < self.min_score {
            tracing::debug!(
                market_id = %opp.market_id,
                score = %scored.score,
                "Score below threshold"
            );
            return None;
        }

        Some(scored)
    }

    /// Batch process all active endgame-eligible markets.
    pub fn scan_all(
        &self,
        markets: &[MarketSnapshot],
        books: &BookStore,
        now: DateTime<Utc>,
    ) -> Vec<ScoredOpportunity> {
        let mut results: Vec<ScoredOpportunity> = markets
            .iter()
            .filter_map(|m| {
                let book = books.get(&m.yes_token_id)?;
                self.process(m, &book, now)
            })
            .collect();

        // Sort by score descending
        results.sort_by(|a, b| b.score.cmp(&a.score));
        results
    }
}
```

---

## 10. 配置 (TOML)

```toml
[detection.endgame]
enabled = true
settlement_window_hours = 24
high_threshold = "0.95"
low_threshold = "0.05"
min_convergence_duration_secs = 300
min_profit_per_share = "0.005"
max_investment_usd = "500"
max_convergence_age_secs = 7200

[detection.endgame.calibration]
min_samples_per_bucket = 10
fusion_prior_strength = 20
fused_p_floor = "0.80"
fused_p_ceiling = "0.995"
update_interval_secs = 3600
bootstrap_alpha = "2.0"
bootstrap_beta = "0.2"

[detection.endgame.fill_probability]
base_fill_prob = "0.90"
depth_penalty_threshold_pct = "20"
depth_penalty_per_pct = "0.02"
staleness_penalty_per_level = "0.05"
resolution_proximity_bonus = "0.05"

[detection.endgame.scorer]
min_score = "0.10"
max_depth_usage_pct = "50"
```

---

## 11. Cargo.toml

```toml
[package]
name = "oxide-arb-algorithm"
description = "Endgame strategy detection, resolution calibration, and opportunity scoring"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[dependencies]
oxide-arb-error = { workspace = true }
oxide-arb-models = { workspace = true }

rust_decimal = { workspace = true }
rust_decimal_macros = { workspace = true }
chrono = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
tracing = { workspace = true }
async-trait = { workspace = true }
uuid = { workspace = true }
strum = { workspace = true }
dashmap = { workspace = true }
moka = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
proptest = "1"
approx = "0.5"

[lints]
workspace = true
```

---

## 12. 验收检查清单

- [ ] `EndgameStrategy::detect()` 在价格 0.96 + 10 min 收敛的市场上生成机会
- [ ] `EndgameStrategy::detect()` 在价格 0.93 的市场上返回 `None`（未达阈值）
- [ ] `EndgameStrategy::detect()` 在收敛不足 5 min 的市场上返回 `None`
- [ ] `ConvergenceTracker` 方向切换时重置计时器
- [ ] `ConvergenceTracker` idle 超时自动驱逐（Moka TTI）
- [ ] `ResolutionCalibrator::lookup()` 精确匹配时返回 Tier 1 结果
- [ ] `ResolutionCalibrator::lookup()` Tier 1 样本不足时 fallback 到 Tier 2
- [ ] `ResolutionCalibrator::lookup()` 逐级 fallback 到 Tier 4（全局先验）
- [ ] `CalibrationEntry::posterior_mean()` 与手动 Beta 分布计算一致（精确到 8 位小数）
- [ ] `estimate_mom_prior()` 在 <3 个有效桶时回退到 bootstrap 先验
- [ ] `ConfidenceFusion::fuse()` 在 n=0 时完全使用 realtime 置信度
- [ ] `ConfidenceFusion::fuse()` 在 n→∞ 时收敛到 calibrator 后验
- [ ] `ConfidenceFusion::fuse()` 输出被 clamp 到 [floor, ceiling]
- [ ] `FillProbabilityEstimator::estimate()` 输出在 [0.10, 0.99]
- [ ] `FillProbabilityEstimator` staleness=Expired 时显著低于 Fresh
- [ ] `CalibrationUpdater::tick()` Gamma/CTF 不一致时跳过该市场
- [ ] `OpportunityPipeline::scan_all()` 输出按 score 降序排列
- [ ] E[PnL] 计算公式: `p × gain - (1-p) × loss` 与手动计算一致
- [ ] proptest: 对任意 Price ∈ [0.95, 0.99]，PriceZone 分类正确
- [ ] proptest: 对任意 secs ∈ [0, 200_000]，DurationBucket 分类正确

---

## 13. 测试计划

### 13.1 单元测试

| 模块 | 测试点 | 方法 |
|---|---|---|
| `PriceZone::from_price` | 边界值 0.95, 0.96, ..., 0.99, 1.0 | 参数化 + proptest |
| `DurationBucket::from_secs` | 边界值 0, 3599, 3600, 21600, 86400 | 参数化 |
| `CalibrationEntry::posterior_mean` | α=2,β=0.2,correct=8,total=10 → 验证 | 手动 Beta 计算 |
| `estimate_mom_prior` | 3+ 桶正常、<3 桶回退、方差=0 回退 | 构造数据 |
| `ConfidenceFusion::fuse` | n=0, n=n₀, n=100, 边界 clamp | 断言 |
| `FillProbabilityEstimator` | 各 staleness level、各深度使用率 | 单调性断言 |
| `compute_realtime_confidence` | price=0.95→0.80, price=0.99→0.95+ | 范围断言 |

### 13.2 集成测试

| 场景 | 描述 |
|---|---|
| Happy path | 构造 0.97 price、15 min 收敛的 orderbook → 产生有效机会 |
| No convergence | 价格在 0.93 → 不产生机会 |
| Short duration | 价格 0.97 但仅收敛 2 min → 不产生机会 |
| Empty book | 无 liquidity → 不产生机会 |
| Calibration roundtrip | 写入 10 个 outcomes → tick() → 验证 posterior 更新 |
| Fallback chain | 删除精确桶 → 验证 Tier 2/3/4 逐级回退 |

### 13.3 Property-based 测试

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn fused_p_always_in_bounds(
        p_cal in 0.5f64..1.0,
        p_rt in 0.5f64..1.0,
        n in 0u32..1000,
    ) {
        let fusion = ConfidenceFusion::new(&default_config());
        let result = fusion.fuse(
            Decimal::try_from(p_cal).unwrap(),
            Decimal::try_from(p_rt).unwrap(),
            n,
        );
        assert!(result >= dec!(0.80));  // floor
        assert!(result <= dec!(0.995)); // ceiling
    }

    #[test]
    fn fill_prob_decreases_with_staleness(
        depth_pct in 0.0f64..50.0,
        hours in 0i64..48,
    ) {
        let estimator = FillProbabilityEstimator::new(&default_config());
        let d = Decimal::try_from(depth_pct).unwrap();

        let fresh = estimator.estimate(d, StalenessLevel::Fresh, hours);
        let stale = estimator.estimate(d, StalenessLevel::Stale, hours);
        let expired = estimator.estimate(d, StalenessLevel::Expired, hours);

        assert!(fresh >= stale);
        assert!(stale >= expired);
    }
}
```

---

## 14. 预估工作量

| 组件 | 源码 LoC | 测试 LoC |
|---|---|---|
| `endgame/strategy.rs` | ~350 | ~400 |
| `endgame/convergence.rs` | ~120 | ~150 |
| `endgame/confidence.rs` | ~80 | ~100 |
| `calibration/types.rs` | ~150 | ~100 |
| `calibration/calibrator.rs` | ~300 | ~350 |
| `calibration/updater.rs` | ~200 | ~200 |
| `calibration/prior.rs` | ~80 | ~100 |
| `calibration/fallback.rs` | ~100 | ~80 |
| `fill_probability/` | ~150 | ~200 |
| `scorer/` | ~120 | ~150 |
| `opportunity/pipeline.rs` | ~150 | ~200 |
| **合计** | **~1,800** | **~2,030** |

---

## 15. 补充设计：老版 oxide-arb-math 关键算法迁移

> 以下组件在老版 `oxide-arb-math` 中存在或隐含需要，在 Endgame 单策略场景下仍然必需。
> 它们不需要约束求解器，但需要独立设计和实现。

### 15.1 EmissionCooldown（机会发射冷却）

**问题**: 同一市场在短时间内价格持续满足检测阈值，会产生重复机会。

**设计**:

```rust
pub struct EmissionCooldown {
    /// market_id -> last_emission_at
    last_emitted: HashMap<MarketId, Instant>,
    /// 基础冷却时间
    base_cooldown: Duration,
    /// 连续命中时的指数退避乘数上限
    max_multiplier: f64,
    /// market_id -> consecutive_hits (未命中时重置)
    consecutive_hits: HashMap<MarketId, u32>,
}

impl EmissionCooldown {
    /// 返回 true 表示该市场当前可以发射新机会
    pub fn may_emit(&self, market_id: &MarketId) -> bool;

    /// 记录一次发射
    pub fn record_emission(&mut self, market_id: &MarketId);

    /// 记录一次检测但被冷却抑制
    pub fn record_suppressed(&mut self, market_id: &MarketId);

    /// 重置该市场冷却（如：价格显著偏离后回归）
    pub fn reset(&mut self, market_id: &MarketId);
}
```

**冷却策略**:
- `base_cooldown` = 30s (configurable via `RuntimeConfig`)
- 每次连续命中，实际冷却 = `base_cooldown * min(2^consecutive_hits, max_multiplier)`
- 市场结算或价格离开 endgame 区间时 reset

### 15.2 StalenessPolicy（数据新鲜度策略）

**问题**: 订单簿数据有延迟或 WS 断连恢复后数据可能过时，需要分级衰减置信度。

**设计**:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StalenessLevel {
    /// < 2s since last update
    Fresh,
    /// 2-10s
    Aging,
    /// 10-30s
    Stale,
    /// > 30s or disconnected
    Expired,
}

pub struct StalenessPolicy {
    pub fresh_threshold_ms: u64,    // default 2000
    pub aging_threshold_ms: u64,    // default 10000
    pub stale_threshold_ms: u64,    // default 30000
}

impl StalenessPolicy {
    pub fn classify(&self, age_ms: u64) -> StalenessLevel;

    /// 返回 [0.0, 1.0] 的置信度折扣因子
    pub fn confidence_discount(&self, level: StalenessLevel) -> f64 {
        match level {
            StalenessLevel::Fresh => 1.0,
            StalenessLevel::Aging => 0.7,
            StalenessLevel::Stale => 0.3,
            StalenessLevel::Expired => 0.0, // 不可交易
        }
    }
}
```

### 15.3 UrgencyFactor（紧迫度因子）

**问题**: 距离市场结束时间越近，错过机会的机会成本越高，应提升优先级。

**设计**:

```rust
pub struct UrgencyFactor;

impl UrgencyFactor {
    /// 计算紧迫度乘数 [1.0, max_multiplier]
    /// hours_remaining: 距市场关闭的小时数
    /// endgame_window_hours: endgame 检测窗口（如 72h）
    pub fn compute(hours_remaining: f64, endgame_window_hours: f64) -> f64 {
        // 非线性衰减：越接近截止时间，紧迫度指数上升
        let progress = 1.0 - (hours_remaining / endgame_window_hours).clamp(0.0, 1.0);
        // 使用 smoothstep 曲线避免突变
        let t = progress * progress * (3.0 - 2.0 * progress);
        1.0 + t * 2.0  // [1.0, 3.0]
    }
}
```

融入 `EndgameScorer`: `final_score = e_pnl * fill_prob * urgency * confidence_discount`

### 15.4 FeeEstimator trait（手续费估计）

**问题**: 不同价格区间、不同订单大小的手续费率不同（maker/taker/promo），需要精确计算净利润。

**设计**:

```rust
/// 手续费估计 trait，允许不同平台/模式实现
pub trait FeeEstimator: Send + Sync {
    /// 估计特定交易的手续费（USD）
    fn estimate_fee(
        &self,
        side: Side,
        shares: Shares,
        price: Price,
        is_maker: bool,
    ) -> Usd;

    /// 获取当前费率 (bps)
    fn current_rate_bps(&self, is_maker: bool) -> Bps;
}

/// Polymarket 手续费实现
pub struct PolymarketFeeEstimator {
    maker_rate_bps: Bps,
    taker_rate_bps: Bps,
    /// 促销期间费率可能为 0
    promo_active: bool,
}
```

### 15.5 OrderbookWalker + estimate_slippage

**问题**: 大额订单会吃掉多层深度，实际成交均价偏离 BBO。需要模拟 walk book 估算滑点。

**设计**:

```rust
/// 订单簿层级（从 L2 快照）
pub struct BookLevel {
    pub price: Price,
    pub size: Shares,
}

pub struct OrderbookWalker;

impl OrderbookWalker {
    /// 模拟买入 `target_shares` 的成交过程
    /// 返回: (avg_fill_price, total_cost, levels_consumed)
    pub fn walk_buy(
        asks: &[BookLevel],
        target_shares: Shares,
    ) -> Option<WalkResult>;

    /// 模拟卖出
    pub fn walk_sell(
        bids: &[BookLevel],
        target_shares: Shares,
    ) -> Option<WalkResult>;
}

pub struct WalkResult {
    pub avg_price: Price,
    pub total_cost: Usd,
    pub levels_consumed: usize,
    pub fully_filled: bool,
}

/// 高层 API: 估计滑点 (bps)
pub fn estimate_slippage(
    book_side: &[BookLevel],
    target_shares: Shares,
    reference_price: Price,
) -> Option<Bps>;
```

**集成点**:
- `EndgameScorer` 在计算 E[PnL] 时用 slippage-adjusted price
- `FillProbabilityEstimator` 用 `levels_consumed / total_levels` 衡量市场冲击

### 15.6 更新后工作量预估

| 新增组件 | 源码 LoC | 测试 LoC |
|---|---|---|
| `emission_cooldown.rs` | ~100 | ~120 |
| `staleness.rs` | ~60 | ~80 |
| `urgency.rs` | ~40 | ~50 |
| `fee_estimator/` | ~120 | ~150 |
| `orderbook_walker.rs` | ~150 | ~200 |
| **新增合计** | **~470** | **~600** |
| **Phase 3 总计（含 §14）** | **~2,270** | **~2,630** |
