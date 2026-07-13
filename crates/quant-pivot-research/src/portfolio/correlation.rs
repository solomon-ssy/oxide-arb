//! Correlation-cluster estimation for the correlated-exposure cap.
//!
//! Pure compute (no I/O, no clock): the core report builder pre-fetches the
//! historical mid-price series per market and hands them in, so the planner
//! stays deterministically replayable. The estimator groups markets into
//! correlated clusters; the optimizer then caps each cluster's projected
//! exposure (`held + Σ allocated`) at `max_correlated_exposure_usd`.
//!
//! Two strategies sit behind one [`CorrelationEstimator`] trait:
//!
//! - [`HistoricalCorrelationEstimator`] — Pearson correlation of mid-price log
//!   returns over a lookback window; markets with `|corr| ≥ threshold` are
//!   unioned into a cluster. When the prefetched history is too thin to estimate
//!   (`< min_observations` paired returns for every pair) it delegates to the
//!   proxy below, so the result is always defined.
//! - [`ProxyCorrelationEstimator`] — deterministic structural proxy: markets in
//!   the same event or the same category are treated as correlated. Used as the
//!   cold-start / insufficient-history fallback.
//!
//! `f64` is confined to the statistical estimate here (a correlation coefficient
//! is not a money value); cluster membership is the only output and money never
//! flows through this module.

use std::collections::BTreeMap;

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::{common::MarketCategory, quant::CorrelationSource},
    types::{EventId, MarketId, Usd},
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// One market's metadata plus its prefetched historical mid-price series.
#[derive(Debug, Clone)]
pub struct CorrelationMarket {
    /// Market id.
    pub market_id: MarketId,
    /// Owning event, when known (structural proxy clustering).
    pub event_id: Option<EventId>,
    /// Market category (structural proxy clustering).
    pub category: MarketCategory,
    /// Time-ordered historical mid prices over the lookback window. Aligned by
    /// the core prefetch so index `t` is the same bucket across markets.
    pub mid_series: Vec<Decimal>,
}

/// Inputs to one correlation estimation pass.
pub struct CorrelationInput<'a> {
    /// The cross-section's markets with their prefetched mid series.
    pub markets: &'a [CorrelationMarket],
    /// Minimum paired return observations before historical estimation is trusted.
    pub min_observations: u32,
    /// Absolute Pearson correlation at or above which two markets are clustered.
    pub cluster_threshold: Decimal,
}

/// The estimated correlation clusters and their provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelationGroups {
    /// Multi-market clusters (singletons are omitted — they add no cap beyond the
    /// per-market cap). Each inner vec is sorted; the outer vec is sorted.
    pub clusters: Vec<Vec<MarketId>>,
    /// Mean pairwise |ρ| per cluster index (Phase 11.3 §6.2 Kelly shrink).
    pub cluster_mean_rho: BTreeMap<usize, Decimal>,
    /// How the clusters were derived.
    pub source: CorrelationSource,
}

impl CorrelationGroups {
    /// The disabled (no clustering) result, equivalent to Phase 4 behavior.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            clusters: Vec::new(),
            cluster_mean_rho: BTreeMap::new(),
            source: CorrelationSource::Disabled,
        }
    }

    /// Pair the clusters with the governed cap into an optimizer constraint.
    #[must_use]
    pub fn into_constraint(self, cap_usd: Usd) -> CorrelationConstraint {
        CorrelationConstraint {
            clusters: self.clusters,
            cluster_mean_rho: self.cluster_mean_rho,
            cap_usd,
            source: self.source,
        }
    }
}

/// A correlated-cluster exposure constraint consumed by the optimizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrelationConstraint {
    /// Correlated market clusters (≥ 2 members each).
    pub clusters: Vec<Vec<MarketId>>,
    /// Mean pairwise |ρ| per cluster index (Kelly correlation shrink).
    pub cluster_mean_rho: BTreeMap<usize, Decimal>,
    /// Maximum projected exposure (`held + Σ allocated`) per cluster.
    pub cap_usd: Usd,
    /// Provenance of the clusters (carried into the plan's optimizer metadata).
    pub source: CorrelationSource,
}

/// Estimates correlated clusters for a cross-section (pure, deterministic).
pub trait CorrelationEstimator: Send + Sync {
    /// Group the input markets into correlated clusters.
    fn estimate(&self, input: &CorrelationInput<'_>) -> QuantResult<CorrelationGroups>;
}

/// Structural proxy: markets sharing an event or a category are correlated.
///
/// Deterministic and history-free — the cold-start / thin-history fallback.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProxyCorrelationEstimator;

impl ProxyCorrelationEstimator {
    /// Construct the proxy estimator.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Cluster purely on event/category structure (no history needed).
    fn cluster(markets: &[CorrelationMarket]) -> Vec<Vec<MarketId>> {
        let mut uf = UnionFind::new(markets.len());
        // Union markets that share an event.
        let mut by_event: BTreeMap<&str, usize> = BTreeMap::new();
        let mut by_category: BTreeMap<MarketCategory, usize> = BTreeMap::new();
        for (idx, market) in markets.iter().enumerate() {
            if let Some(event_id) = &market.event_id {
                match by_event.get(event_id.as_str()) {
                    Some(&anchor) => uf.union(anchor, idx),
                    None => {
                        by_event.insert(event_id.as_str(), idx);
                    }
                }
            }
            match by_category.get(&market.category) {
                Some(&anchor) => uf.union(anchor, idx),
                None => {
                    by_category.insert(market.category, idx);
                }
            }
        }
        uf.into_clusters(markets)
    }
}

impl CorrelationEstimator for ProxyCorrelationEstimator {
    fn estimate(&self, input: &CorrelationInput<'_>) -> QuantResult<CorrelationGroups> {
        let clusters = Self::cluster(input.markets);
        Ok(CorrelationGroups {
            clusters: clusters.clone(),
            cluster_mean_rho: proxy_cluster_rho(&clusters, input.cluster_threshold),
            source: CorrelationSource::Proxy,
        })
    }
}

/// Historical mid-price co-movement estimator with a structural proxy fallback.
#[derive(Debug, Clone, Copy, Default)]
pub struct HistoricalCorrelationEstimator {
    proxy: ProxyCorrelationEstimator,
}

impl HistoricalCorrelationEstimator {
    /// Construct the historical estimator (delegates to the proxy when history
    /// is insufficient).
    #[must_use]
    pub const fn new() -> Self {
        Self {
            proxy: ProxyCorrelationEstimator::new(),
        }
    }
}

impl CorrelationEstimator for HistoricalCorrelationEstimator {
    fn estimate(&self, input: &CorrelationInput<'_>) -> QuantResult<CorrelationGroups> {
        let min_obs = usize::try_from(input.min_observations).map_err(|error| {
            ResearchError::PortfolioOptimization {
                detail: format!("correlation min_observations exceeds usize: {error}"),
            }
        })?;
        let returns: Vec<Vec<f64>> = input
            .markets
            .iter()
            .map(|market| log_returns(&market.mid_series))
            .collect::<QuantResult<Vec<_>>>()?;

        // Historical estimation is only trustworthy when at least two markets
        // carry enough paired observations; otherwise fall back to the proxy.
        let usable = returns.iter().filter(|r| r.len() >= min_obs).count();
        if usable < 2 {
            return self.proxy.estimate(input);
        }

        let threshold =
            decimal_to_f64(input.cluster_threshold, "correlation cluster threshold")?.abs();
        let mut uf = UnionFind::new(input.markets.len());
        // Every evaluated pair's |ρ| (not just the above-threshold ones that
        // drove clustering) — a cluster formed via transitive closure (A-B and
        // B-C above threshold, A-C below) must still average in the A-C pair
        // when computing the cluster's mean correlation (Phase 11.3 §4.2:
        // "ρ̄ = 平均因子/事件相关" is the full within-cluster pairwise mean,
        // not only the pairs that happened to clear the clustering threshold —
        // restricting to threshold-clearing pairs only would systematically
        // bias ρ̄ upward for any cluster joined by transitivity).
        let mut pair_corrs: Vec<(usize, usize, f64)> = Vec::new();
        for i in 0..returns.len() {
            for j in (i + 1)..returns.len() {
                let overlap = returns[i].len().min(returns[j].len());
                if overlap < min_obs {
                    continue;
                }
                let Some(corr) = pearson(&returns[i][..overlap], &returns[j][..overlap])? else {
                    continue;
                };
                pair_corrs.push((i, j, corr.abs()));
                if corr.abs() >= threshold {
                    uf.union(i, j);
                }
            }
        }
        let clusters = uf.into_clusters(input.markets);
        Ok(CorrelationGroups {
            clusters: clusters.clone(),
            cluster_mean_rho: historical_cluster_rho(
                input.markets,
                &clusters,
                &pair_corrs,
                input.cluster_threshold,
            )?,
            source: CorrelationSource::Historical,
        })
    }
}

/// Natural-log returns of a price series (`ln(p[t+1] / p[t])`), skipping any
/// non-positive price (degenerate for a probability mid, but guarded).
fn log_returns(series: &[Decimal]) -> QuantResult<Vec<f64>> {
    let mut out = Vec::with_capacity(series.len().saturating_sub(1));
    for window in series.windows(2) {
        let prev = decimal_to_f64(window[0], "correlation previous mid price")?;
        let next = decimal_to_f64(window[1], "correlation next mid price")?;
        if prev > 0.0 && next > 0.0 {
            let log_return = (next / prev).ln();
            if !log_return.is_finite() {
                return Err(ResearchError::PortfolioOptimization {
                    detail: format!(
                        "correlation log return is non-finite for previous={prev}, next={next}"
                    ),
                }
                .into());
            }
            out.push(log_return);
        }
    }
    Ok(out)
}

/// Pearson correlation of two equal-length samples, or `None` when a sample has
/// zero variance (correlation undefined).
fn pearson(first: &[f64], second: &[f64]) -> QuantResult<Option<f64>> {
    let len = first.len();
    if len == 0 || len != second.len() {
        return Ok(None);
    }
    let count = count_to_f64(len, "correlation paired observation count")?;
    let mean_first = first.iter().sum::<f64>() / count;
    let mean_second = second.iter().sum::<f64>() / count;
    let mut covariance = 0.0;
    let mut variance_first = 0.0;
    let mut variance_second = 0.0;
    for (left, right) in first.iter().zip(second) {
        let delta_left = left - mean_first;
        let delta_right = right - mean_second;
        covariance = delta_left.mul_add(delta_right, covariance);
        variance_first = delta_left.mul_add(delta_left, variance_first);
        variance_second = delta_right.mul_add(delta_right, variance_second);
    }
    if variance_first <= 0.0 || variance_second <= 0.0 {
        return Ok(None);
    }
    let correlation = covariance / (variance_first.sqrt() * variance_second.sqrt());
    if !correlation.is_finite() {
        return Err(ResearchError::PortfolioOptimization {
            detail: "Pearson correlation produced a non-finite coefficient".to_owned(),
        }
        .into());
    }
    Ok(Some(correlation))
}

/// Convert a (small) observation count to `f64` without a lossy `as` cast.
fn count_to_f64(count: usize, field: &'static str) -> QuantResult<f64> {
    count
        .to_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| {
            ResearchError::PortfolioOptimization {
                detail: format!("{field} {count} is not representable as finite f64"),
            }
            .into()
        })
}

fn proxy_cluster_rho(clusters: &[Vec<MarketId>], threshold: Decimal) -> BTreeMap<usize, Decimal> {
    let default_rho = threshold.abs().min(Decimal::ONE);
    clusters
        .iter()
        .enumerate()
        .map(|(idx, _)| (idx, default_rho))
        .collect()
}

fn historical_cluster_rho(
    markets: &[CorrelationMarket],
    clusters: &[Vec<MarketId>],
    pair_corrs: &[(usize, usize, f64)],
    fallback: Decimal,
) -> QuantResult<BTreeMap<usize, Decimal>> {
    let market_index: BTreeMap<&str, usize> = markets
        .iter()
        .enumerate()
        .map(|(idx, market)| (market.market_id.as_str(), idx))
        .collect();
    let mut out = BTreeMap::new();
    for (cluster_idx, members) in clusters.iter().enumerate() {
        let member_indices: Vec<usize> = members
            .iter()
            .filter_map(|market| market_index.get(market.as_str()).copied())
            .collect();
        let mut rhos = Vec::new();
        for &(left, right, corr) in pair_corrs {
            if member_indices.contains(&left) && member_indices.contains(&right) {
                rhos.push(corr);
            }
        }
        let mean = if rhos.is_empty() {
            fallback.abs().min(Decimal::ONE)
        } else {
            let sum: f64 = rhos.iter().sum();
            let count = count_to_f64(rhos.len(), "within-cluster correlation count")?;
            Decimal::from_f64_retain(sum / count)
                .ok_or_else(|| ResearchError::PortfolioOptimization {
                    detail: format!(
                        "within-cluster mean correlation is not representable as Decimal: sum={sum}, count={count}"
                    ),
                })?
                .abs()
                .min(Decimal::ONE)
        };
        out.insert(cluster_idx, mean);
    }
    Ok(out)
}

/// Lossy `Decimal → f64` for statistical estimation only (never money).
fn decimal_to_f64(value: Decimal, field: &'static str) -> QuantResult<f64> {
    value
        .to_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| {
            ResearchError::PortfolioOptimization {
                detail: format!("{field} Decimal {value} is not representable as finite f64"),
            }
            .into()
        })
}

/// Disjoint-set union over candidate indices for deterministic clustering.
struct UnionFind {
    parent: Vec<usize>,
}

impl UnionFind {
    fn new(len: usize) -> Self {
        Self {
            parent: (0..len).collect(),
        }
    }

    fn find(&mut self, mut node: usize) -> usize {
        while self.parent[node] != node {
            self.parent[node] = self.parent[self.parent[node]];
            node = self.parent[node];
        }
        node
    }

    fn union(&mut self, a: usize, b: usize) {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra != rb {
            // Deterministic: always attach the larger root under the smaller.
            let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
            self.parent[hi] = lo;
        }
    }

    /// Materialize the multi-member clusters, sorted for determinism (singletons
    /// dropped — they add no cap beyond the per-market cap).
    fn into_clusters(mut self, markets: &[CorrelationMarket]) -> Vec<Vec<MarketId>> {
        let mut groups: BTreeMap<usize, Vec<MarketId>> = BTreeMap::new();
        for (idx, market) in markets.iter().enumerate() {
            let root = self.find(idx);
            groups
                .entry(root)
                .or_default()
                .push(market.market_id.clone());
        }
        let mut clusters: Vec<Vec<MarketId>> = groups
            .into_values()
            .filter(|members| members.len() >= 2)
            .map(|mut members| {
                members.sort_by(|a, b| a.as_str().cmp(b.as_str()));
                members
            })
            .collect();
        clusters.sort_by(|a, b| a[0].as_str().cmp(b[0].as_str()));
        clusters
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CorrelationEstimator, CorrelationInput, CorrelationMarket, HistoricalCorrelationEstimator,
        ProxyCorrelationEstimator,
    };
    use quant_pivot_models::{
        enums::{common::MarketCategory, quant::CorrelationSource},
        types::{EventId, MarketId},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn market(
        id: &str,
        event: Option<&str>,
        category: MarketCategory,
        mids: &[f64],
    ) -> CorrelationMarket {
        CorrelationMarket {
            market_id: MarketId::new(id),
            event_id: event.map(EventId::new),
            category,
            mid_series: mids
                .iter()
                .map(|v| Decimal::try_from(*v).expect("decimal"))
                .collect(),
        }
    }

    #[test]
    fn historical_clusters_comoving_markets() {
        // a and b move in lockstep (|corr| = 1); c oscillates near-flat and is
        // (almost) uncorrelated with the monotone pair, so it stays out.
        let a = market(
            "0xa",
            None,
            MarketCategory::Crypto,
            &[0.50, 0.52, 0.54, 0.56, 0.58, 0.60],
        );
        let b = market(
            "0xb",
            None,
            MarketCategory::Sports,
            &[0.40, 0.42, 0.44, 0.46, 0.48, 0.50],
        );
        let c = market(
            "0xc",
            None,
            MarketCategory::Politics,
            &[0.50, 0.49, 0.51, 0.50, 0.49, 0.51],
        );
        let markets = vec![a, b, c];
        let groups = HistoricalCorrelationEstimator::new()
            .estimate(&CorrelationInput {
                markets: &markets,
                min_observations: 3,
                cluster_threshold: dec!(0.9),
            })
            .expect("estimate");
        assert_eq!(groups.source, CorrelationSource::Historical);
        // The co-moving pair lands in one cluster together.
        let pair = groups
            .clusters
            .iter()
            .find(|cluster| cluster.contains(&MarketId::new("0xa")))
            .expect("0xa is clustered");
        assert!(pair.contains(&MarketId::new("0xb")), "0xa and 0xb co-move");
        assert!(
            !pair.contains(&MarketId::new("0xc")),
            "the near-flat market does not join the trend cluster"
        );
    }

    #[test]
    fn thin_history_falls_back_to_proxy() {
        let a = market("0xa", Some("evt1"), MarketCategory::Crypto, &[0.5]);
        let b = market("0xb", Some("evt1"), MarketCategory::Crypto, &[0.4]);
        let markets = vec![a, b];
        let groups = HistoricalCorrelationEstimator::new()
            .estimate(&CorrelationInput {
                markets: &markets,
                min_observations: 5,
                cluster_threshold: dec!(0.7),
            })
            .expect("estimate");
        assert_eq!(groups.source, CorrelationSource::Proxy);
        assert_eq!(groups.clusters.len(), 1, "same event clusters via proxy");
    }

    #[test]
    fn cluster_mean_rho_averages_every_pair_not_only_threshold_clearing_ones() {
        // a-b and b-c both clear the 0.7 threshold and union transitively into
        // one 3-market cluster; a-c does NOT clear it directly (~0.26) but
        // must still be averaged into the cluster's mean ρ̄ once all three
        // share a cluster — the regression this guards against averaged only
        // the threshold-clearing pairs (a-b, b-c ≈ 0.79), which is
        // systematically biased high relative to the true within-cluster mean
        // (≈ 0.62 including the weak a-c pair).
        let a = market("0xa", None, MarketCategory::Crypto, A_MIDS);
        let b = market("0xb", None, MarketCategory::Sports, B_MIDS);
        let c = market("0xc", None, MarketCategory::Politics, C_MIDS);
        let markets = vec![a, b, c];
        let groups = HistoricalCorrelationEstimator::new()
            .estimate(&CorrelationInput {
                markets: &markets,
                min_observations: 10,
                cluster_threshold: dec!(0.7),
            })
            .expect("estimate");
        assert_eq!(groups.source, CorrelationSource::Historical);
        assert_eq!(
            groups.clusters.len(),
            1,
            "all three markets share a cluster"
        );
        assert_eq!(groups.clusters[0].len(), 3);
        let mean_rho = *groups.cluster_mean_rho.get(&0).expect("cluster 0 mean rho");
        // Naive (buggy) mean of only the two threshold-clearing pairs would
        // land close to 0.79; the correct full-cluster mean (including the
        // weak a-c pair) must be materially lower.
        assert!(
            mean_rho < dec!(0.75),
            "mean_rho={mean_rho} must include the weak a-c pair, not just the \
             threshold-clearing a-b/b-c pairs"
        );
        assert!(
            mean_rho > dec!(0.5),
            "mean_rho={mean_rho} should still reflect two strong pairs out of three"
        );
    }

    const A_MIDS: &[f64] = &[
        0.500_000, 0.500_171, 0.497_625, 0.500_468, 0.491_308, 0.494_662, 0.496_958, 0.496_199,
        0.488_742, 0.479_446, 0.473_759, 0.478_029, 0.479_873, 0.482_494, 0.478_584, 0.483_872,
        0.479_833, 0.482_373, 0.486_354, 0.491_024, 0.493_993, 0.492_757, 0.492_667, 0.495_625,
        0.493_046, 0.492_169, 0.501_064, 0.503_399, 0.509_768, 0.516_448, 0.509_731, 0.520_150,
        0.523_823, 0.519_929, 0.518_871, 0.520_471, 0.513_826, 0.512_848, 0.515_574, 0.513_201,
        0.526_559, 0.529_707, 0.527_857, 0.527_974, 0.529_707, 0.533_940, 0.536_844, 0.542_331,
        0.538_378, 0.535_439, 0.533_628, 0.534_831, 0.531_401, 0.535_437, 0.531_074, 0.534_054,
        0.534_985, 0.531_000, 0.534_295, 0.535_987, 0.535_106,
    ];
    const B_MIDS: &[f64] = &[
        0.500_000, 0.503_741, 0.500_774, 0.503_056, 0.499_322, 0.501_857, 0.506_030, 0.507_202,
        0.501_758, 0.491_522, 0.482_751, 0.485_850, 0.489_288, 0.494_266, 0.491_760, 0.493_027,
        0.485_076, 0.485_364, 0.490_385, 0.490_928, 0.488_876, 0.489_849, 0.490_658, 0.492_889,
        0.493_523, 0.494_420, 0.497_861, 0.499_586, 0.501_608, 0.502_934, 0.498_008, 0.501_924,
        0.509_153, 0.506_727, 0.503_897, 0.504_970, 0.498_207, 0.493_643, 0.501_410, 0.495_653,
        0.504_412, 0.504_762, 0.503_772, 0.506_311, 0.502_037, 0.503_876, 0.509_743, 0.513_866,
        0.509_369, 0.506_757, 0.500_205, 0.504_035, 0.499_378, 0.507_473, 0.504_980, 0.508_838,
        0.505_574, 0.502_705, 0.507_541, 0.509_156, 0.507_118,
    ];
    const C_MIDS: &[f64] = &[
        0.500_000, 0.506_766, 0.504_003, 0.505_421, 0.508_206, 0.508_954, 0.513_658, 0.515_991,
        0.513_943, 0.505_618, 0.496_311, 0.496_921, 0.500_812, 0.506_604, 0.506_438, 0.502_848,
        0.493_367, 0.491_986, 0.496_765, 0.493_345, 0.487_153, 0.489_777, 0.490_801, 0.491_907,
        0.495_806, 0.498_820, 0.496_320, 0.496_448, 0.493_983, 0.489_734, 0.488_649, 0.485_121,
        0.493_735, 0.493_372, 0.489_906, 0.490_072, 0.485_732, 0.479_791, 0.490_050, 0.483_428,
        0.485_188, 0.482_789, 0.483_378, 0.487_181, 0.478_038, 0.477_523, 0.484_732, 0.486_135,
        0.481_901, 0.480_441, 0.471_555, 0.476_872, 0.473_395, 0.483_818, 0.483_912, 0.487_569,
        0.481_409, 0.480_105, 0.485_105, 0.485_980, 0.483_676,
    ];

    #[test]
    fn proxy_clusters_same_event() {
        let a = market("0xa", Some("evt1"), MarketCategory::Crypto, &[]);
        let b = market("0xb", Some("evt1"), MarketCategory::Sports, &[]);
        let c = market("0xc", Some("evt2"), MarketCategory::Politics, &[]);
        let markets = vec![a, b, c];
        let groups = ProxyCorrelationEstimator::new()
            .estimate(&CorrelationInput {
                markets: &markets,
                min_observations: 5,
                cluster_threshold: dec!(0.7),
            })
            .expect("estimate");
        assert_eq!(groups.source, CorrelationSource::Proxy);
        // evt1 markets cluster; evt2 singleton dropped.
        assert!(groups.clusters.iter().any(|c| c.len() == 2));
    }
}
