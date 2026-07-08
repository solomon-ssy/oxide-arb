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

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    enums::{common::MarketCategory, quant::CorrelationSource},
    types::{EventId, MarketId, Usd},
};
use rust_decimal::Decimal;

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
        let min_obs = input.min_observations as usize;
        let returns: Vec<Vec<f64>> = input
            .markets
            .iter()
            .map(|market| log_returns(&market.mid_series))
            .collect();

        // Historical estimation is only trustworthy when at least two markets
        // carry enough paired observations; otherwise fall back to the proxy.
        let usable = returns.iter().filter(|r| r.len() >= min_obs).count();
        if usable < 2 {
            return self.proxy.estimate(input);
        }

        let threshold = decimal_to_f64(input.cluster_threshold).abs();
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
                let Some(corr) = pearson(&returns[i][..overlap], &returns[j][..overlap]) else {
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
            ),
            source: CorrelationSource::Historical,
        })
    }
}

/// Natural-log returns of a price series (`ln(p[t+1] / p[t])`), skipping any
/// non-positive price (degenerate for a probability mid, but guarded).
fn log_returns(series: &[Decimal]) -> Vec<f64> {
    let mut out = Vec::with_capacity(series.len().saturating_sub(1));
    for window in series.windows(2) {
        let prev = decimal_to_f64(window[0]);
        let next = decimal_to_f64(window[1]);
        if prev > 0.0 && next > 0.0 {
            out.push((next / prev).ln());
        }
    }
    out
}

/// Pearson correlation of two equal-length samples, or `None` when a sample has
/// zero variance (correlation undefined).
fn pearson(first: &[f64], second: &[f64]) -> Option<f64> {
    let len = first.len();
    if len == 0 || len != second.len() {
        return None;
    }
    let count = count_to_f64(len);
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
        return None;
    }
    Some(covariance / (variance_first.sqrt() * variance_second.sqrt()))
}

/// Convert a (small) observation count to `f64` without a lossy `as` cast.
fn count_to_f64(count: usize) -> f64 {
    u32::try_from(count).map_or(f64::MAX, f64::from)
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
) -> BTreeMap<usize, Decimal> {
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
            Decimal::from_f64_retain(sum / count_to_f64(rhos.len()))
                .unwrap_or(fallback)
                .abs()
                .min(Decimal::ONE)
        };
        out.insert(cluster_idx, mean);
    }
    out
}

/// Lossy `Decimal → f64` for statistical estimation only (never money).
fn decimal_to_f64(value: Decimal) -> f64 {
    use rust_decimal::prelude::ToPrimitive;
    value.to_f64().unwrap_or(0.0)
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
#[allow(clippy::unreadable_literal)]
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
        0.500000, 0.500171, 0.497625, 0.500468, 0.491308, 0.494662, 0.496958, 0.496199, 0.488742,
        0.479446, 0.473759, 0.478029, 0.479873, 0.482494, 0.478584, 0.483872, 0.479833, 0.482373,
        0.486354, 0.491024, 0.493993, 0.492757, 0.492667, 0.495625, 0.493046, 0.492169, 0.501064,
        0.503399, 0.509768, 0.516448, 0.509731, 0.520150, 0.523823, 0.519929, 0.518871, 0.520471,
        0.513826, 0.512848, 0.515574, 0.513201, 0.526559, 0.529707, 0.527857, 0.527974, 0.529707,
        0.533940, 0.536844, 0.542331, 0.538378, 0.535439, 0.533628, 0.534831, 0.531401, 0.535437,
        0.531074, 0.534054, 0.534985, 0.531000, 0.534295, 0.535987, 0.535106,
    ];
    const B_MIDS: &[f64] = &[
        0.500000, 0.503741, 0.500774, 0.503056, 0.499322, 0.501857, 0.506030, 0.507202, 0.501758,
        0.491522, 0.482751, 0.485850, 0.489288, 0.494266, 0.491760, 0.493027, 0.485076, 0.485364,
        0.490385, 0.490928, 0.488876, 0.489849, 0.490658, 0.492889, 0.493523, 0.494420, 0.497861,
        0.499586, 0.501608, 0.502934, 0.498008, 0.501924, 0.509153, 0.506727, 0.503897, 0.504970,
        0.498207, 0.493643, 0.501410, 0.495653, 0.504412, 0.504762, 0.503772, 0.506311, 0.502037,
        0.503876, 0.509743, 0.513866, 0.509369, 0.506757, 0.500205, 0.504035, 0.499378, 0.507473,
        0.504980, 0.508838, 0.505574, 0.502705, 0.507541, 0.509156, 0.507118,
    ];
    const C_MIDS: &[f64] = &[
        0.500000, 0.506766, 0.504003, 0.505421, 0.508206, 0.508954, 0.513658, 0.515991, 0.513943,
        0.505618, 0.496311, 0.496921, 0.500812, 0.506604, 0.506438, 0.502848, 0.493367, 0.491986,
        0.496765, 0.493345, 0.487153, 0.489777, 0.490801, 0.491907, 0.495806, 0.498820, 0.496320,
        0.496448, 0.493983, 0.489734, 0.488649, 0.485121, 0.493735, 0.493372, 0.489906, 0.490072,
        0.485732, 0.479791, 0.490050, 0.483428, 0.485188, 0.482789, 0.483378, 0.487181, 0.478038,
        0.477523, 0.484732, 0.486135, 0.481901, 0.480441, 0.471555, 0.476872, 0.473395, 0.483818,
        0.483912, 0.487569, 0.481409, 0.480105, 0.485105, 0.485980, 0.483676,
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
