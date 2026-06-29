//! `Decimal → f64` training-matrix construction — the single numeric boundary.
//!
//! This is the **only** place `Decimal` becomes `f64`. Every conversion is
//! explicit (column scale), every non-finite result rejects the sample, and a
//! missing critical feature or label rejects the sample. Categorical features are
//! never fed as ordinals — they must be one-hot encoded upstream.

use ndarray::{Array1, Array2};
use quant_pivot_error::{QuantResult, research::ResearchError};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use super::{LabelName, TrainingExample};
use crate::features::{FeatureName, FeatureSchema, FeatureUnit, FeatureValue, FeatureValueKind};
use serde::{Deserialize, Serialize};

/// How a column's decimal value is scaled before the `f64` cast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatrixScale {
    /// Use the decimal value directly.
    Identity,
    /// Divide by `10_000` (basis points → fraction).
    BasisPoints,
}

impl MatrixScale {
    fn apply(self, value: Decimal) -> Decimal {
        match self {
            Self::Identity => value,
            Self::BasisPoints => value / Decimal::from(10_000),
        }
    }
}

/// Spec for one feature column of the matrix.
#[derive(Debug, Clone)]
pub struct FeatureColumnSpec {
    /// The feature mapped into this column.
    pub feature: FeatureName,
    /// Scale applied before the `f64` cast.
    pub scale: MatrixScale,
    /// When true, a missing/unconvertible value rejects the whole sample row;
    /// otherwise `fill_missing` is substituted.
    pub critical: bool,
    /// Substituted value for a missing non-critical feature.
    pub fill_missing: f64,
}

/// The full matrix spec: ordered feature columns + the supervised target.
#[derive(Debug, Clone)]
pub struct FeatureMatrixSpec {
    /// Feature columns, in matrix column order.
    pub columns: Vec<FeatureColumnSpec>,
    /// The label used as the regression/classification target.
    pub label_name: LabelName,
    /// The horizon of the target label (`0` for horizon-independent labels).
    pub label_horizon_secs: u64,
}

/// A dense training matrix: `features` is `rows × columns`, `labels` is `rows`.
#[derive(Debug, Clone)]
pub struct TrainingMatrix {
    /// Feature matrix `X` (`f64`, row-major).
    pub features: Array2<f64>,
    /// Label vector `y`.
    pub labels: Array1<f64>,
    /// Column names, aligned with `features` columns.
    pub feature_names: Vec<FeatureName>,
    /// Number of sample rows rejected (NaN/inf, critical-missing, or no label).
    pub rejected_rows: usize,
}

/// Project a [`FeatureValue`] to a scalar decimal, or `None` when it is missing
/// or not a numeric/boolean scalar (categoricals are intentionally rejected).
fn scalar(value: &FeatureValue) -> Option<Decimal> {
    match value {
        FeatureValue::Decimal(d) | FeatureValue::Bps(d) => Some(*d),
        FeatureValue::Probability(p) => Some(p.inner()),
        FeatureValue::Usd(u) => Some(u.inner()),
        FeatureValue::Count(c) => Some(Decimal::from(*c)),
        FeatureValue::Bool(b) => Some(if *b { Decimal::ONE } else { Decimal::ZERO }),
        FeatureValue::Category(_) | FeatureValue::Missing(_) => None,
    }
}

/// Cast a decimal to a finite `f64`, rejecting non-finite and unrepresentable values.
fn finite_f64(decimal: Decimal) -> Option<f64> {
    decimal
        .to_f64()
        .and_then(|value| value.is_finite().then_some(value))
}

/// Resolve one column cell to a finite `f64`, or `None` to reject the row.
fn cell(example: &TrainingExample, column: &FeatureColumnSpec) -> Option<f64> {
    let resolved = example
        .feature_vector
        .values
        .get(&column.feature)
        .and_then(scalar);
    let decimal = match resolved {
        Some(value) => column.scale.apply(value),
        None if column.critical => return None,
        None => return Some(column.fill_missing),
    };
    finite_f64(decimal)
}

/// Resolve the target label cell for one example, or `None` to reject the row.
fn label_cell(example: &TrainingExample, spec: &FeatureMatrixSpec) -> Option<f64> {
    let value = example
        .labels
        .iter()
        .find(|label| {
            let name_matches = label.label_name == spec.label_name;
            let horizon_matches = label.horizon_secs == spec.label_horizon_secs;
            name_matches && horizon_matches
        })?
        .value;
    finite_f64(value)
}

/// Build the dense `f64` training matrix from materialized examples.
///
/// Rows whose target label is absent, whose critical feature is missing, or whose
/// any cell is non-finite are rejected (counted in [`TrainingMatrix::rejected_rows`]).
///
/// # Errors
///
/// Returns [`ResearchError::MatrixBuild`] when the spec has no columns or when
/// the assembled buffer cannot form the declared shape.
pub fn build_training_matrix(
    examples: &[TrainingExample],
    spec: &FeatureMatrixSpec,
) -> QuantResult<TrainingMatrix> {
    if spec.columns.is_empty() {
        return Err(ResearchError::MatrixBuild {
            detail: "feature matrix spec has no columns".to_owned(),
        }
        .into());
    }
    let cols = spec.columns.len();
    let mut data: Vec<f64> = Vec::with_capacity(examples.len() * cols);
    let mut labels: Vec<f64> = Vec::with_capacity(examples.len());
    let mut rejected = 0usize;

    'rows: for example in examples {
        let Some(label) = label_cell(example, spec) else {
            rejected += 1;
            continue;
        };
        let mut row = Vec::with_capacity(cols);
        for column in &spec.columns {
            if let Some(value) = cell(example, column) {
                row.push(value);
            } else {
                rejected += 1;
                continue 'rows;
            }
        }
        data.extend_from_slice(&row);
        labels.push(label);
    }

    let rows = labels.len();
    let features =
        Array2::from_shape_vec((rows, cols), data).map_err(|error| ResearchError::MatrixBuild {
            detail: format!("matrix shape {rows}x{cols} invalid: {error}"),
        })?;
    Ok(TrainingMatrix {
        features,
        labels: Array1::from_vec(labels),
        feature_names: spec.columns.iter().map(|c| c.feature.clone()).collect(),
        rejected_rows: rejected,
    })
}

/// Row counts from an optional [`build_training_matrix`] probe at build time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixCoverageProbe {
    /// Rows that would enter the dense matrix.
    pub accepted_rows: u64,
    /// Rows rejected (missing label, critical feature, or non-finite cell).
    pub rejected_rows: u64,
    /// Supervised label used for the probe.
    pub label_name: LabelName,
    /// Horizon of the probed label column.
    pub label_horizon_secs: u64,
    /// Number of numeric feature columns in the probe spec.
    pub feature_columns: u64,
}

/// Build a diagnostic matrix spec from the governed feature schema (numeric
/// columns only; categoricals excluded).
#[must_use]
pub fn matrix_spec_from_schema(
    schema: &FeatureSchema,
    label_name: LabelName,
    label_horizon_secs: u64,
) -> FeatureMatrixSpec {
    let columns = schema
        .specs()
        .iter()
        .filter(|spec| spec.value_kind != FeatureValueKind::Category)
        .map(|spec| FeatureColumnSpec {
            feature: spec.name.clone(),
            scale: match spec.unit {
                FeatureUnit::Bps => MatrixScale::BasisPoints,
                _ => MatrixScale::Identity,
            },
            critical: spec.critical,
            fill_missing: 0.0,
        })
        .collect();
    FeatureMatrixSpec {
        columns,
        label_name,
        label_horizon_secs,
    }
}

/// Probe how many examples would survive [`build_training_matrix`] (diagnostic).
///
/// Does not gate dataset build; counts are stored in [`DatasetCoverage::matrix_probe`].
pub fn probe_matrix_coverage(
    examples: &[TrainingExample],
    schema: &FeatureSchema,
    label_name: LabelName,
    label_horizon_secs: u64,
) -> QuantResult<MatrixCoverageProbe> {
    let spec = matrix_spec_from_schema(schema, label_name.clone(), label_horizon_secs);
    let feature_columns = u64::try_from(spec.columns.len()).unwrap_or(u64::MAX);
    if spec.columns.is_empty() || examples.is_empty() {
        return Ok(MatrixCoverageProbe {
            accepted_rows: 0,
            rejected_rows: examples.len() as u64,
            label_name,
            label_horizon_secs,
            feature_columns,
        });
    }
    let matrix = build_training_matrix(examples, &spec)?;
    Ok(MatrixCoverageProbe {
        accepted_rows: matrix.features.nrows() as u64,
        rejected_rows: matrix.rejected_rows as u64,
        label_name,
        label_horizon_secs,
        feature_columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        features::FeatureVector,
        training::{LabelName, TrainingExample, TrainingLabel},
    };
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        enums::quant::DataQualityStatus,
        types::{MarketId, SchemaVersion, TokenId, TrainingExampleId, TrainingSampleSource},
    };
    use rust_decimal::prelude::FromPrimitive;
    use std::collections::BTreeMap;

    fn example(spread: Option<Decimal>, label: Option<Decimal>) -> TrainingExample {
        let as_of = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
        let mut values = BTreeMap::new();
        if let Some(spread) = spread {
            values.insert(
                FeatureName::from_static("spread"),
                FeatureValue::Decimal(spread),
            );
        }
        let labels = label
            .map(|value| {
                vec![TrainingLabel {
                    label_name: LabelName::from_static("return_to_horizon"),
                    horizon_secs: 60,
                    value,
                    is_resolved: true,
                }]
            })
            .unwrap_or_default();
        TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: MarketId::new("m"),
            token_id: TokenId::new("t"),
            as_of,
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector: FeatureVector {
                market_id: MarketId::new("m"),
                token_id: Some(TokenId::new("t")),
                as_of,
                schema_version: SchemaVersion::new(1),
                values,
                substitutions: Vec::new(),
                data_quality: DataQualityStatus::Fresh,
                staleness_ms: 0,
                source_refs: Vec::new(),
            },
            factor_values: Vec::new(),
            labels,
            source_refs: Vec::new(),
        }
    }

    fn spec(critical: bool) -> FeatureMatrixSpec {
        FeatureMatrixSpec {
            columns: vec![FeatureColumnSpec {
                feature: FeatureName::from_static("spread"),
                scale: MatrixScale::Identity,
                critical,
                fill_missing: 0.0,
            }],
            label_name: LabelName::from_static("return_to_horizon"),
            label_horizon_secs: 60,
        }
    }

    #[test]
    fn training_matrix_decimal_to_f64_only_at_boundary() {
        let examples = vec![example(
            Some(Decimal::new(25, 2)),
            Some(Decimal::from(1000)),
        )];
        let matrix = build_training_matrix(&examples, &spec(true)).expect("matrix");
        assert_eq!(matrix.features.shape(), [1, 1]);
        assert!((matrix.features[[0, 0]] - 0.25).abs() < 1e-9);
        assert!((matrix.labels[0] - 1000.0).abs() < 1e-9);
        assert_eq!(matrix.rejected_rows, 0);
    }

    #[test]
    fn matrix_rejects_critical_missing_and_missing_label() {
        // Missing critical feature → row rejected.
        let missing_feature = vec![example(None, Some(Decimal::from(1000)))];
        let matrix = build_training_matrix(&missing_feature, &spec(true)).expect("matrix");
        assert_eq!(matrix.features.shape(), [0, 1]);
        assert_eq!(matrix.rejected_rows, 1);

        // Missing label → row rejected.
        let missing_label = vec![example(Some(Decimal::new(25, 2)), None)];
        let matrix = build_training_matrix(&missing_label, &spec(true)).expect("matrix");
        assert_eq!(matrix.rejected_rows, 1);

        // Non-critical missing → filled, row kept.
        let filled = build_training_matrix(&missing_feature, &spec(false)).expect("matrix");
        assert_eq!(filled.features.shape(), [1, 1]);
        assert!((filled.features[[0, 0]] - 0.0).abs() < 1e-9);
    }

    #[test]
    fn matrix_rejects_nan_and_inf() {
        assert!(finite_f64(Decimal::ONE).is_some());
        assert!(finite_f64(Decimal::ZERO).is_some());
        assert!(!f64::NAN.is_finite());
        assert!(!f64::INFINITY.is_finite());

        let overflow_label = vec![example(
            Some(Decimal::new(25, 2)),
            Decimal::from_f64(f64::INFINITY),
        )];
        let matrix = build_training_matrix(&overflow_label, &spec(true)).expect("matrix");
        assert_eq!(matrix.features.shape(), [0, 1]);
        assert_eq!(matrix.rejected_rows, 1);
    }
}
