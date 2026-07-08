//! `Decimal → f64` training-matrix construction — the single numeric boundary.
//!
//! This is the **only** place `Decimal` becomes `f64`. Every conversion is
//! explicit (column scale), every non-finite result rejects the sample, and a
//! missing critical feature or label rejects the sample. Categorical features are
//! never fed as ordinals — they must be one-hot encoded upstream.
//!
//! # Non-critical missingness never collapses to a silent zero
//!
//! A non-critical column's value cell is filled with [`FeatureColumnSpec::fill_missing`]
//! when absent, but that placeholder is **never** presented to the model alone: a
//! companion `{feature}.__available` column is emitted immediately after it,
//! carrying which of three structurally distinct states produced the row —
//! [`CellOutcome::Present`] (`1.0`), [`CellOutcome::NotApplicable`] (`0.0`, the
//! feature structurally never applies to this row — e.g. `domain: None` or an
//! explicit `NullReason::NotApplicable`), or [`CellOutcome::MissingApplicable`]
//! (`-1.0`, the feature applies in principle but this instance has a data/quality
//! gap). A learner can therefore never confuse a fabricated placeholder with a
//! real observation, and never conflates "this market has no such vertical" with
//! "this market's vertical had a data outage."

use ndarray::{Array1, Array2};
use quant_pivot_error::{QuantResult, research::ResearchError};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use super::{LabelName, TrainingExample};
use crate::features::{
    FeatureName, FeatureSchema, FeatureUnit, FeatureValue, FeatureValueKind, NullReason,
};
use quant_pivot_models::types::MatrixCoverageProbe;

/// Availability-column value: the cell was a genuine observation.
const AVAILABILITY_PRESENT: f64 = 1.0;
/// Availability-column value: the feature structurally does not apply to this row.
const AVAILABILITY_NOT_APPLICABLE: f64 = 0.0;
/// Availability-column value: the feature applies but is missing this instance.
const AVAILABILITY_MISSING: f64 = -1.0;
/// Suffix appended to a non-critical column's feature name for its
/// availability companion column.
const AVAILABILITY_SUFFIX: &str = ".__available";

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

/// The three structurally distinct states a training-matrix cell can be in.
///
/// See the module doc for why [`Self::NotApplicable`] and
/// [`Self::MissingApplicable`] must never collapse into the same fabricated
/// placeholder value.
#[derive(Debug, Clone, Copy, PartialEq)]
enum CellOutcome {
    /// A genuine observed, finite value.
    Present(f64),
    /// The feature structurally never applies to this row: the vector's
    /// domain slice is entirely absent (a `domain.*` column on a market whose
    /// category maps to no vertical), or the value is explicitly
    /// `FeatureValue::Missing(NullReason::NotApplicable)`.
    NotApplicable,
    /// The feature applies in principle but this instance has a data or
    /// quality gap (any other [`NullReason`], or an out-of-kind value).
    MissingApplicable,
}

/// Resolve one column's raw state for `example`, before scale/fill.
fn resolve_cell(example: &TrainingExample, column: &FeatureColumnSpec) -> CellOutcome {
    match example.feature_vector.value(&column.feature) {
        // The key being entirely absent (`None`) is folded into the same
        // `NotApplicable` arm as an explicit `NullReason::NotApplicable`: for
        // a `domain.*` column this is the `domain: None` case (structurally
        // distinct from a present-but-missing domain value, never a data
        // gap); for a generic column this should not occur under the
        // fixed-width schema, and failing soft to "not applicable" is the
        // conservative choice (never a fabricated data gap).
        Some(FeatureValue::Missing(NullReason::NotApplicable)) | None => CellOutcome::NotApplicable,
        Some(FeatureValue::Missing(_)) => CellOutcome::MissingApplicable,
        Some(other) => scalar(other)
            .map(|value| column.scale.apply(value))
            .and_then(finite_f64)
            .map_or(CellOutcome::MissingApplicable, CellOutcome::Present),
    }
}

/// Resolve one column cell to `(value, availability)`, or `None` to reject the row.
///
/// Non-critical columns always emit both: the value cell (a genuine
/// observation, or [`FeatureColumnSpec::fill_missing`] when absent) and its
/// availability companion (see the module doc). Critical columns emit only
/// the value cell — any non-[`CellOutcome::Present`] state rejects the row,
/// so there is no missingness left to signal.
fn cell(example: &TrainingExample, column: &FeatureColumnSpec) -> Option<(f64, Option<f64>)> {
    match resolve_cell(example, column) {
        CellOutcome::Present(value) => {
            Some((value, (!column.critical).then_some(AVAILABILITY_PRESENT)))
        }
        _ if column.critical => None,
        CellOutcome::NotApplicable => {
            Some((column.fill_missing, Some(AVAILABILITY_NOT_APPLICABLE)))
        }
        CellOutcome::MissingApplicable => Some((column.fill_missing, Some(AVAILABILITY_MISSING))),
    }
}

/// The companion availability [`FeatureName`] for a non-critical column.
fn availability_name(feature: &FeatureName) -> FeatureName {
    FeatureName::new(format!("{}{AVAILABILITY_SUFFIX}", feature.as_str()))
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
    let feature_names = expanded_feature_names(spec);
    let cols = feature_names.len();
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
            if let Some((value, availability)) = cell(example, column) {
                row.push(value);
                if let Some(availability) = availability {
                    row.push(availability);
                }
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
        feature_names,
        rejected_rows: rejected,
    })
}

/// The matrix's column names in emission order: each column's own name,
/// followed by its `{feature}.__available` companion when non-critical.
fn expanded_feature_names(spec: &FeatureMatrixSpec) -> Vec<FeatureName> {
    let mut names = Vec::with_capacity(spec.columns.len() * 2);
    for column in &spec.columns {
        names.push(column.feature.clone());
        if !column.critical {
            names.push(availability_name(&column.feature));
        }
    }
    names
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
    label_name: &LabelName,
    label_horizon_secs: u64,
) -> QuantResult<MatrixCoverageProbe> {
    let spec = matrix_spec_from_schema(schema, label_name.clone(), label_horizon_secs);
    if spec.columns.is_empty() || examples.is_empty() {
        return Ok(MatrixCoverageProbe {
            accepted_rows: 0,
            rejected_rows: examples.len() as u64,
            label_name: label_name.as_str().to_owned(),
            label_horizon_secs,
            feature_columns: 0,
        });
    }
    let matrix = build_training_matrix(examples, &spec)?;
    // The matrix's true width, including non-critical columns' availability
    // companions (§ module doc) — the honest reported width, not the
    // pre-expansion governed-schema column count.
    let feature_columns = u64::try_from(matrix.feature_names.len()).unwrap_or(u64::MAX);
    Ok(MatrixCoverageProbe {
        accepted_rows: matrix.features.nrows() as u64,
        rejected_rows: matrix.rejected_rows as u64,
        label_name: label_name.as_str().to_owned(),
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

    /// `spread` as an explicit [`FeatureValue`] (present, or a specific
    /// missing reason) — distinct from omitting the key entirely (simulated
    /// by [`example`] passing `None`), so tests can exercise all three
    /// [`CellOutcome`] states precisely.
    fn example_with_value(value: Option<FeatureValue>, label: Option<Decimal>) -> TrainingExample {
        let mut values = BTreeMap::new();
        if let Some(value) = value {
            values.insert(FeatureName::from_static("spread"), value);
        }
        example_from_values(values, label)
    }

    fn example(spread: Option<Decimal>, label: Option<Decimal>) -> TrainingExample {
        example_with_value(spread.map(FeatureValue::Decimal), label)
    }

    fn example_from_values(
        values: BTreeMap<FeatureName, FeatureValue>,
        label: Option<Decimal>,
    ) -> TrainingExample {
        let as_of = Utc.timestamp_opt(1_000_000, 0).single().expect("ts");
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
                generic_schema_version: SchemaVersion::FIRST,
                generic: values,
                domain: None,
                substitutions: Vec::new(),
                data_quality: DataQualityStatus::Fresh,
                staleness_ms: 0,
                source_refs: Vec::new(),
            },
            factor_values: Vec::new(),
            labels,
            source_refs: Vec::new(),
            lot_context: None,
            position_state: None,
            book_fidelity: None,
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

        // Non-critical missing (key entirely absent) → filled, row kept, and
        // the availability companion column reports "not applicable" (never
        // silently indistinguishable from a real observation).
        let filled = build_training_matrix(&missing_feature, &spec(false)).expect("matrix");
        assert_eq!(filled.features.shape(), [1, 2]);
        assert!(
            (filled.features[[0, 0]] - 0.0).abs() < 1e-9,
            "value cell filled"
        );
        assert!(
            (filled.features[[0, 1]] - AVAILABILITY_NOT_APPLICABLE).abs() < 1e-9,
            "availability cell reports not-applicable"
        );
        assert_eq!(
            filled.feature_names,
            vec![
                FeatureName::from_static("spread"),
                FeatureName::from_static("spread.__available"),
            ]
        );
    }

    #[test]
    fn matrix_distinguishes_not_applicable_from_missing_applicable() {
        // Explicit `NullReason::NotApplicable` → availability = 0.0.
        let not_applicable = vec![example_with_value(
            Some(FeatureValue::Missing(NullReason::NotApplicable)),
            Some(Decimal::from(1000)),
        )];
        let matrix = build_training_matrix(&not_applicable, &spec(false)).expect("matrix");
        assert_eq!(matrix.rejected_rows, 0);
        assert!((matrix.features[[0, 0]] - 0.0).abs() < 1e-9);
        assert!((matrix.features[[0, 1]] - AVAILABILITY_NOT_APPLICABLE).abs() < 1e-9);

        // Any other reason (e.g. a domain source outage) → availability = -1.0,
        // a DIFFERENT number from the not-applicable case above — the model
        // can tell "never has this signal" apart from "data gap right now".
        let missing_applicable = vec![example_with_value(
            Some(FeatureValue::Missing(NullReason::DomainSourceUnavailable)),
            Some(Decimal::from(1000)),
        )];
        let matrix = build_training_matrix(&missing_applicable, &spec(false)).expect("matrix");
        assert_eq!(matrix.rejected_rows, 0);
        assert!((matrix.features[[0, 0]] - 0.0).abs() < 1e-9);
        assert!((matrix.features[[0, 1]] - AVAILABILITY_MISSING).abs() < 1e-9);

        // A genuine present value → availability = 1.0.
        let present = vec![example_with_value(
            Some(FeatureValue::Decimal(Decimal::new(25, 2))),
            Some(Decimal::from(1000)),
        )];
        let matrix = build_training_matrix(&present, &spec(false)).expect("matrix");
        assert!((matrix.features[[0, 0]] - 0.25).abs() < 1e-9);
        assert!((matrix.features[[0, 1]] - AVAILABILITY_PRESENT).abs() < 1e-9);
    }

    #[test]
    fn matrix_critical_column_never_gains_an_availability_companion() {
        // Critical columns reject the row on any non-present state, so there
        // is no missingness left to signal — no companion column is emitted.
        let present = vec![example(
            Some(Decimal::new(25, 2)),
            Some(Decimal::from(1000)),
        )];
        let matrix = build_training_matrix(&present, &spec(true)).expect("matrix");
        assert_eq!(
            matrix.feature_names,
            vec![FeatureName::from_static("spread")]
        );
        assert_eq!(matrix.features.shape(), [1, 1]);
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
