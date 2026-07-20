//! Canonical strongly typed backtest persistence documents.

use std::{
    ops::{Deref, DerefMut},
    vec::IntoIter,
};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

use crate::{enums::common::MarketCategory, types::Probability};

/// Expected-versus-realized agreement summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct ExpectedVsRealized {
    pub mean_expected_bps: Decimal,
    pub mean_realized_bps: Decimal,
    pub correlation: Decimal,
    pub bias_bps: Decimal,
}

/// One category's backtest metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CategoryMetric {
    pub category: MarketCategory,
    pub sample_count: u64,
    pub rank_ic: Decimal,
    pub hit_rate: Probability,
    pub mean_realized_bps: Decimal,
}

/// Fixed-schema category metrics persisted as one JSONB value object.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct CategoryMetrics(Vec<CategoryMetric>);

impl From<Vec<CategoryMetric>> for CategoryMetrics {
    fn from(values: Vec<CategoryMetric>) -> Self {
        Self(values)
    }
}

impl Deref for CategoryMetrics {
    type Target = [CategoryMetric];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CategoryMetrics {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl IntoIterator for CategoryMetrics {
    type IntoIter = IntoIter<CategoryMetric>;
    type Item = CategoryMetric;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// One cumulative realized-PnL point.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PnlCurvePoint {
    pub decision_at: DateTime<Utc>,
    pub cumulative_realized_pnl_usd: Decimal,
}

/// Portfolio-level `PnL` simulation summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct PnlSimulation {
    pub total_allocated_usd: Decimal,
    pub realized_pnl_usd: Decimal,
    pub gross_return: Decimal,
    pub pnl_curve: Vec<PnlCurvePoint>,
}

/// Sharpe distribution across reconstructed CPCV paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct SharpeDistribution {
    pub min: Decimal,
    pub p25: Decimal,
    pub median: Decimal,
    pub p75: Decimal,
    pub max: Decimal,
    pub median_max_drawdown: Option<Decimal>,
    pub median_tail_loss: Option<Decimal>,
    pub baseline_uplift: Option<Decimal>,
}

/// One complete full-timeline reconstructed CPCV path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestPath {
    pub path_index: u32,
    pub group_returns: Vec<Decimal>,
    pub sharpe: Decimal,
    pub rank_ic: Decimal,
    pub max_drawdown: Decimal,
    pub tail_loss: Decimal,
}

/// Complete reconstructed CPCV paths persisted atomically.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct BacktestPaths(Vec<BacktestPath>);

impl From<Vec<BacktestPath>> for BacktestPaths {
    fn from(paths: Vec<BacktestPath>) -> Self {
        Self(paths)
    }
}

impl Deref for BacktestPaths {
    type Target = [BacktestPath];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for BacktestPaths {
    type IntoIter = IntoIter<BacktestPath>;
    type Item = BacktestPath;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// One category's candidate-versus-baseline rank-IC delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CategoryRankIcDelta {
    pub category: MarketCategory,
    pub baseline_rank_ic: Decimal,
    pub candidate_rank_ic: Decimal,
    pub rank_ic_delta: Decimal,
}

/// Typed category comparison collection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct CategoryRankIcDeltas(Vec<CategoryRankIcDelta>);

impl From<Vec<CategoryRankIcDelta>> for CategoryRankIcDeltas {
    fn from(values: Vec<CategoryRankIcDelta>) -> Self {
        Self(values)
    }
}

impl Deref for CategoryRankIcDeltas {
    type Target = [CategoryRankIcDelta];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl IntoIterator for CategoryRankIcDeltas {
    type IntoIter = IntoIter<CategoryRankIcDelta>;
    type Item = CategoryRankIcDelta;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::ExpectedVsRealized;

    #[test]
    fn fixed_document_rejects_unknown_and_missing_fields() {
        let valid = json!({
            "mean_expected_bps": "1",
            "mean_realized_bps": "2",
            "correlation": "0.5",
            "bias_bps": "-1"
        });
        let decoded: ExpectedVsRealized =
            serde_json::from_value(valid.clone()).expect("fixed document");
        assert_eq!(decoded.correlation, dec!(0.5));

        let mut unknown = valid.clone();
        unknown["extra"] = json!(true);
        assert!(serde_json::from_value::<ExpectedVsRealized>(unknown).is_err());

        let mut missing = valid;
        missing.as_object_mut().expect("object").remove("bias_bps");
        assert!(serde_json::from_value::<ExpectedVsRealized>(missing).is_err());
    }
}
