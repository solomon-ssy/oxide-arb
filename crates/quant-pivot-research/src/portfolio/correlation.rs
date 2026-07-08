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
        let mut pair_corrs: Vec<(usize, usize, f64)> = Vec::new();
        for i in 0..returns.len() {
            for j in (i + 1)..returns.len() {
                let overlap = returns[i].len().min(returns[j].len());
                if overlap < min_obs {
                    continue;
                }
                if let Some(corr) = pearson(&returns[i][..overlap], &returns[j][..overlap])
                    && corr.abs() >= threshold
                {
                    uf.union(i, j);
                    pair_corrs.push((i, j, corr.abs()));
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
