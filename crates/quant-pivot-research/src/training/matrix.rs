//! Shared classical-model input transformation.
//!
//! Dataset rows stay typed as raw [`ModelInputCell`] values until a concrete
//! training partition is known. [`FittedInputTransform::fit`] learns optional
//! medians and numeric standardization from that partition only; the same
//! [`FittedInputTransform::apply_cells`] implementation is then used by fold
//! validation, final training, backtests, and online serving.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use ndarray::Array1;
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::common::MarketCategory,
    types::{ContentHash, MatrixCoverageProbe, ModelInputContract, ModelInputRequiredness},
};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};
use serde::{Deserialize, Serialize};

use super::{LabelName, TrainingExample, TrainingLabel};
use crate::{
    features::{
        FeatureCell, FeatureCellState, FeatureName, FeatureSchema, FeatureUnit, FeatureValue,
        FeatureValueKind, feature_scalar,
    },
    hashing::ResearchHasher,
    model::artifact::{
        EncodedColumn, EncodedColumnKind, EncodedColumnName, FittedInputColumn,
        FittedInputTransform, InputStateRates,
    },
};

/// Decimal precision persisted for fitted numeric statistics.
const TRANSFORM_DECIMAL_SCALE: u32 = 15;

/// Model-level contract for one governed numeric input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureColumnSpec {
    /// Governed source feature.
    pub feature: FeatureName,
    /// Unit conversion performed before fitting or applying statistics.
    pub unit: FeatureUnit,
    /// Governed raw value kind. Categories use a frozen one-hot vocabulary;
    /// every other kind follows the numeric transform.
    pub value_kind: FeatureValueKind,
    /// Required inputs reject any non-observed state.
    pub required: bool,
}

/// Ordered raw inputs plus the supervised target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeatureMatrixSpec {
    /// Raw feature inputs in contract order.
    pub columns: Vec<FeatureColumnSpec>,
    /// Supervised label.
    pub label_name: LabelName,
    /// Label horizon (`0` for horizon-independent labels).
    pub label_horizon_secs: u64,
}

/// State of one raw model input before imputation or encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelInputCell {
    /// Genuine observed numeric value.
    Observed(Decimal),
    /// Value supplied by the audited feature null-policy substitution.
    Substituted(Decimal),
    /// Genuine observed categorical value.
    ObservedCategory(MarketCategory),
    /// Category supplied by an audited substitution policy.
    SubstitutedCategory(MarketCategory),
    /// Feature applies but its value is unavailable.
    Missing,
    /// Feature is structurally inapplicable to this market.
    NotApplicable,
}

/// Raw, labelled rows for classical training.
///
/// No placeholder floats or synthesized feature names are stored here. A fitted
/// transform is the only route to an estimator-ready dense matrix.
#[derive(Debug, Clone)]
pub struct TrainingMatrix {
    /// Ordered raw cells for every accepted example.
    pub cells: Vec<Vec<ModelInputCell>>,
    /// Label vector aligned with [`Self::cells`].
    pub labels: Array1<f64>,
    /// Governed raw input contract.
    pub columns: Vec<FeatureColumnSpec>,
    /// Rows rejected for absent label, invalid label, or non-observed required input.
    pub rejected_rows: usize,
    /// Decision time per row, used for label-horizon purge.
    pub row_decision_at: Vec<DateTime<Utc>>,
    /// Label maturity time per row, used for overlap purge.
    pub row_label_horizon_end: Vec<DateTime<Utc>>,
}

impl TrainingMatrix {
    /// Number of accepted raw rows.
    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.cells.len()
    }

    /// Number of governed raw input features.
    #[must_use]
    pub const fn input_count(&self) -> usize {
        self.columns.len()
    }
}

/// Canonical commitment to exact estimator-ready rows and their aligned
/// labels.
///
/// Final classical training and frozen-artifact parity both call this function;
/// changing row order, any encoded byte, or label fails the binding.
pub fn training_input_hash(
    standardized_rows: &[Vec<f64>],
    labels: &Array1<f64>,
) -> QuantResult<ContentHash> {
    #[derive(Serialize)]
    struct TrainingInput<'a> {
        rows: &'a [Vec<f64>],
        labels: &'a [f64],
    }

    let labels = labels
        .as_slice()
        .ok_or_else(|| ResearchError::MatrixBuild {
            detail: "training labels are not stored contiguously".to_owned(),
        })?;
    ResearchHasher::canonical(&TrainingInput {
        rows: standardized_rows,
        labels,
    })
}

/// Classify one schema-bound feature cell with its explicit state.
///
/// Absence from the vector and a value/state shape mismatch are contract
/// violations, not missing-data states. Only an explicit `FeatureCell` may
/// produce `Missing` or `NotApplicable`.
pub fn model_input_cell(
    cell: Option<&FeatureCell>,
    feature: &FeatureName,
    expected_kind: FeatureValueKind,
) -> QuantResult<ModelInputCell> {
    let cell = cell.ok_or_else(|| ResearchError::MatrixBuild {
        detail: format!("model input `{feature}` is absent from the feature vector"),
    })?;
    Ok(match cell.state {
        FeatureCellState::Missing => ModelInputCell::Missing,
        FeatureCellState::NotApplicable => ModelInputCell::NotApplicable,
        FeatureCellState::Observed | FeatureCellState::Substituted => match cell.value() {
            Some(FeatureValue::Category(category)) => {
                if expected_kind != FeatureValueKind::Category {
                    return Err(ResearchError::MatrixBuild {
                        detail: format!(
                            "model input `{feature}` has category value, expected {expected_kind:?}"
                        ),
                    }
                    .into());
                }
                if cell.state == FeatureCellState::Substituted {
                    ModelInputCell::SubstitutedCategory(*category)
                } else {
                    ModelInputCell::ObservedCategory(*category)
                }
            }
            Some(value) => {
                if value.kind() != expected_kind {
                    return Err(ResearchError::MatrixBuild {
                        detail: format!(
                            "model input `{feature}` has value kind {:?}, expected {expected_kind:?}",
                            value.kind()
                        ),
                    }
                    .into());
                }
                let value = feature_scalar(value).ok_or_else(|| ResearchError::MatrixBuild {
                    detail: format!(
                        "model input `{feature}` has value kind {:?} that cannot be projected as numeric",
                        value.kind()
                    ),
                })?;
                if cell.state == FeatureCellState::Substituted {
                    ModelInputCell::Substituted(value)
                } else {
                    ModelInputCell::Observed(value)
                }
            }
            None => {
                return Err(ResearchError::MatrixBuild {
                    detail: format!(
                        "model input `{feature}` is {:?} but carries no typed value",
                        cell.state
                    ),
                }
                .into());
            }
        },
    })
}

fn cell_from_example(
    example: &TrainingExample,
    column: &FeatureColumnSpec,
) -> QuantResult<ModelInputCell> {
    model_input_cell(
        example.feature_vector.cell(&column.feature),
        &column.feature,
        column.value_kind,
    )
}

fn matching_label<'a>(
    example: &'a TrainingExample,
    spec: &FeatureMatrixSpec,
) -> Option<&'a TrainingLabel> {
    example.labels.iter().find(|label| {
        let matches_name = label.label_name == spec.label_name;
        let matches_horizon = label.horizon_secs == spec.label_horizon_secs;
        matches_name && matches_horizon
    })
}

fn label_f64(value: Decimal) -> Option<f64> {
    value.to_f64().filter(|value| value.is_finite())
}

/// Build typed raw rows from frozen training examples.
///
/// Required inputs accept only [`ModelInputCell::Observed`]. Optional inputs
/// preserve their full state for partition-local transform fitting.
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
    let mut names = BTreeSet::new();
    for column in &spec.columns {
        if !names.insert(column.feature.clone()) {
            return Err(ResearchError::MatrixBuild {
                detail: format!("duplicate raw feature input `{}`", column.feature),
            }
            .into());
        }
    }

    let mut cells = Vec::with_capacity(examples.len());
    let mut labels = Vec::with_capacity(examples.len());
    let mut row_decision_at = Vec::with_capacity(examples.len());
    let mut row_label_horizon_end = Vec::with_capacity(examples.len());
    let mut rejected_rows = 0usize;

    for example in examples {
        let Some(label_row) = matching_label(example, spec) else {
            rejected_rows += 1;
            continue;
        };
        let Some(label) = label_f64(label_row.value) else {
            rejected_rows += 1;
            continue;
        };
        let row: Vec<ModelInputCell> = spec
            .columns
            .iter()
            .map(|column| cell_from_example(example, column))
            .collect::<QuantResult<_>>()?;
        if spec.columns.iter().zip(&row).any(|(column, cell)| {
            column.required
                && !matches!(
                    cell,
                    ModelInputCell::Observed(_) | ModelInputCell::ObservedCategory(_)
                )
        }) {
            rejected_rows += 1;
            continue;
        }
        cells.push(row);
        labels.push(label);
        row_decision_at.push(example.decision_at());
        row_label_horizon_end.push(label_row.matured_at);
    }

    Ok(TrainingMatrix {
        cells,
        labels: Array1::from_vec(labels),
        columns: spec.columns.clone(),
        rejected_rows,
        row_decision_at,
        row_label_horizon_end,
    })
}

fn unit_scale(unit: FeatureUnit, value: Decimal) -> Decimal {
    match unit {
        FeatureUnit::Bps => value / Decimal::from(10_000),
        _ => value,
    }
}

fn strict_f64(value: Decimal, field: &str) -> QuantResult<f64> {
    value
        .to_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| {
            ResearchError::MatrixBuild {
                detail: format!("{field} is not representable as a finite f64"),
            }
            .into()
        })
}

fn strict_decimal(value: f64, field: &str) -> QuantResult<Decimal> {
    if !value.is_finite() {
        return Err(ResearchError::MatrixBuild {
            detail: format!("{field} is not finite"),
        }
        .into());
    }
    Decimal::from_f64(value)
        .map(|value| value.round_dp(TRANSFORM_DECIMAL_SCALE))
        .ok_or_else(|| {
            ResearchError::MatrixBuild {
                detail: format!("{field} cannot be persisted as Decimal"),
            }
            .into()
        })
}

fn median(mut values: Vec<Decimal>, feature: &FeatureName) -> QuantResult<Decimal> {
    if values.is_empty() {
        return Err(ResearchError::MatrixBuild {
            detail: format!(
                "optional input `{feature}` has no observed training-partition value for median imputation"
            ),
        }
        .into());
    }
    values.sort();
    let middle = values.len() / 2;
    Ok(if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / Decimal::from(2)
    } else {
        values[middle]
    })
}

fn encoded_name(feature: &FeatureName, suffix: &str) -> EncodedColumnName {
    EncodedColumnName::new(format!("{}.__{suffix}", feature.as_str()))
}

fn encoded_contract(columns: &[FittedInputColumn]) -> Vec<EncodedColumn> {
    let mut encoded = Vec::new();
    for column in columns {
        if column.value_kind == FeatureValueKind::Category {
            for category in &column.category_vocabulary {
                encoded.push(EncodedColumn {
                    name: encoded_name(&column.feature, &format!("category_{}", category.as_str())),
                    source_feature: column.feature.clone(),
                    kind: EncodedColumnKind::CategoryValue,
                });
            }
            encoded.push(EncodedColumn {
                name: encoded_name(&column.feature, "category_unknown"),
                source_feature: column.feature.clone(),
                kind: EncodedColumnKind::CategoryUnknown,
            });
        } else {
            encoded.push(EncodedColumn {
                name: encoded_name(&column.feature, "value"),
                source_feature: column.feature.clone(),
                kind: EncodedColumnKind::NumericValue,
            });
        }
        if !column.required {
            for (kind, suffix) in [
                (EncodedColumnKind::MissingIndicator, "missing"),
                (EncodedColumnKind::NotApplicableIndicator, "not_applicable"),
                (EncodedColumnKind::SubstitutedIndicator, "substituted"),
            ] {
                encoded.push(EncodedColumn {
                    name: encoded_name(&column.feature, suffix),
                    source_feature: column.feature.clone(),
                    kind,
                });
            }
        }
    }
    encoded
}

fn state_rates(rows: &[Vec<ModelInputCell>], index: usize) -> QuantResult<InputStateRates> {
    let count = Decimal::from(rows.len());
    if count.is_zero() {
        return Err(ResearchError::MatrixBuild {
            detail: "cannot fit input state rates from an empty training partition".to_owned(),
        }
        .into());
    }
    let mut observed = 0u64;
    let mut missing = 0u64;
    let mut not_applicable = 0u64;
    let mut substituted = 0u64;
    for row in rows {
        match row.get(index) {
            Some(ModelInputCell::Observed(_) | ModelInputCell::ObservedCategory(_)) => {
                observed += 1;
            }
            Some(ModelInputCell::Missing) => missing += 1,
            Some(ModelInputCell::NotApplicable) => not_applicable += 1,
            Some(ModelInputCell::Substituted(_) | ModelInputCell::SubstitutedCategory(_)) => {
                substituted += 1;
            }
            None => {
                return Err(ResearchError::MatrixBuild {
                    detail: format!("row is missing raw input at index {index}"),
                }
                .into());
            }
        }
    }
    Ok(InputStateRates {
        observed: Decimal::from(observed) / count,
        missing: Decimal::from(missing) / count,
        not_applicable: Decimal::from(not_applicable) / count,
        substituted: Decimal::from(substituted) / count,
    })
}

fn validate_training_matrix(matrix: &TrainingMatrix) -> QuantResult<()> {
    if matrix.cells.len() != matrix.labels.len()
        || matrix.cells.len() != matrix.row_decision_at.len()
        || matrix.cells.len() != matrix.row_label_horizon_end.len()
    {
        return Err(ResearchError::MatrixBuild {
            detail: "training matrix row metadata lengths are inconsistent".to_owned(),
        }
        .into());
    }
    if matrix
        .cells
        .iter()
        .any(|row| row.len() != matrix.columns.len())
    {
        return Err(ResearchError::MatrixBuild {
            detail: "training matrix raw row width does not match input contract".to_owned(),
        }
        .into());
    }
    if matrix.cells.len() < 2 || matrix.columns.is_empty() {
        return Err(ResearchError::MatrixBuild {
            detail: format!(
                "classical transform needs >= 2 rows and >= 1 input, got {}x{}",
                matrix.cells.len(),
                matrix.columns.len()
            ),
        }
        .into());
    }
    Ok(())
}

fn fit_input_column(
    matrix: &TrainingMatrix,
    index: usize,
    spec: &FeatureColumnSpec,
) -> QuantResult<FittedInputColumn> {
    let rates = state_rates(&matrix.cells, index)?;
    if spec.value_kind == FeatureValueKind::Category {
        fit_category_column(matrix, index, spec, rates)
    } else {
        fit_numeric_column(matrix, index, spec, rates)
    }
}

fn fit_category_column(
    matrix: &TrainingMatrix,
    index: usize,
    spec: &FeatureColumnSpec,
    state_rates: InputStateRates,
) -> QuantResult<FittedInputColumn> {
    let mut vocabulary = BTreeSet::new();
    for row in &matrix.cells {
        match row.get(index) {
            Some(ModelInputCell::ObservedCategory(category)) => {
                vocabulary.insert(*category);
            }
            Some(ModelInputCell::SubstitutedCategory(_)) if spec.required => {
                return Err(ResearchError::MatrixBuild {
                    detail: format!(
                        "required categorical input `{}` was substituted",
                        spec.feature
                    ),
                }
                .into());
            }
            Some(
                ModelInputCell::SubstitutedCategory(_)
                | ModelInputCell::Missing
                | ModelInputCell::NotApplicable,
            ) => {}
            Some(ModelInputCell::Observed(_) | ModelInputCell::Substituted(_)) => {
                return Err(ResearchError::MatrixBuild {
                    detail: format!(
                        "categorical input `{}` received a numeric value",
                        spec.feature
                    ),
                }
                .into());
            }
            None => {
                return Err(ResearchError::MatrixBuild {
                    detail: format!("row missing input `{}`", spec.feature),
                }
                .into());
            }
        }
    }
    if vocabulary.is_empty() {
        return Err(ResearchError::MatrixBuild {
            detail: format!(
                "categorical input `{}` has no observed training-partition value for vocabulary fitting",
                spec.feature
            ),
        }
        .into());
    }
    Ok(FittedInputColumn {
        feature: spec.feature.clone(),
        unit: spec.unit,
        value_kind: spec.value_kind,
        required: spec.required,
        median: None,
        mean: None,
        std: None,
        category_vocabulary: vocabulary.into_iter().collect(),
        state_rates,
    })
}

fn observed_numeric_values(
    matrix: &TrainingMatrix,
    index: usize,
    spec: &FeatureColumnSpec,
) -> QuantResult<Vec<Decimal>> {
    matrix
        .cells
        .iter()
        .map(|row| match row.get(index) {
            Some(ModelInputCell::Observed(value)) => Ok(Some(unit_scale(spec.unit, *value))),
            Some(
                ModelInputCell::Substituted(_)
                | ModelInputCell::Missing
                | ModelInputCell::NotApplicable,
            ) => Ok(None),
            Some(ModelInputCell::ObservedCategory(_) | ModelInputCell::SubstitutedCategory(_)) => {
                Err(ResearchError::MatrixBuild {
                    detail: format!("numeric input `{}` received a category", spec.feature),
                }
                .into())
            }
            None => Err(ResearchError::MatrixBuild {
                detail: format!("row missing input `{}`", spec.feature),
            }
            .into()),
        })
        .collect::<QuantResult<Vec<_>>>()
        .map(|values| values.into_iter().flatten().collect())
}

fn numeric_training_value(
    cell: &ModelInputCell,
    spec: &FeatureColumnSpec,
    imputation: Option<Decimal>,
) -> QuantResult<f64> {
    let value = match cell {
        ModelInputCell::Observed(value) | ModelInputCell::Substituted(value) => {
            if spec.required && matches!(cell, ModelInputCell::Substituted(_)) {
                return Err(ResearchError::MatrixBuild {
                    detail: format!("required input `{}` was substituted", spec.feature),
                }
                .into());
            }
            unit_scale(spec.unit, *value)
        }
        ModelInputCell::Missing | ModelInputCell::NotApplicable => {
            imputation.ok_or_else(|| ResearchError::MatrixBuild {
                detail: format!("required input `{}` is absent", spec.feature),
            })?
        }
        ModelInputCell::ObservedCategory(_) | ModelInputCell::SubstitutedCategory(_) => {
            return Err(ResearchError::MatrixBuild {
                detail: format!("numeric input `{}` received a category", spec.feature),
            }
            .into());
        }
    };
    strict_f64(value, spec.feature.as_str())
}

fn numeric_mean_std(values: &[f64], feature: &FeatureName) -> QuantResult<(Decimal, Decimal)> {
    let count = values
        .len()
        .to_f64()
        .ok_or_else(|| ResearchError::MatrixBuild {
            detail: format!(
                "numeric input `{feature}` training count cannot be represented as f64"
            ),
        })?;
    let mean = values.iter().sum::<f64>() / count;
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / count;
    let std = variance.sqrt();
    if !std.is_finite() || std <= f64::EPSILON {
        return Err(ResearchError::MatrixBuild {
            detail: format!(
                "numeric input `{feature}` has zero or invalid training-partition variance"
            ),
        }
        .into());
    }
    Ok((
        strict_decimal(mean, "input mean")?,
        strict_decimal(std, "input standard deviation")?,
    ))
}

fn fit_numeric_column(
    matrix: &TrainingMatrix,
    index: usize,
    spec: &FeatureColumnSpec,
    state_rates: InputStateRates,
) -> QuantResult<FittedInputColumn> {
    let observed = observed_numeric_values(matrix, index, spec)?;
    let imputation = if spec.required {
        None
    } else {
        Some(median(observed, &spec.feature)?)
    };
    let values = matrix
        .cells
        .iter()
        .map(|row| {
            row.get(index)
                .ok_or_else(|| ResearchError::MatrixBuild {
                    detail: format!("row missing input `{}`", spec.feature),
                })
                .map_err(Into::into)
                .and_then(|cell| numeric_training_value(cell, spec, imputation))
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let (mean, std) = numeric_mean_std(&values, &spec.feature)?;
    Ok(FittedInputColumn {
        feature: spec.feature.clone(),
        unit: spec.unit,
        value_kind: spec.value_kind,
        required: spec.required,
        median: imputation,
        mean: Some(mean),
        std: Some(std),
        category_vocabulary: Vec::new(),
        state_rates,
    })
}

fn apply_category_input(input: &FittedInputColumn, cell: &ModelInputCell) -> QuantResult<Vec<f64>> {
    let category = match cell {
        ModelInputCell::ObservedCategory(category) => Some(*category),
        ModelInputCell::SubstitutedCategory(category) if !input.required => Some(*category),
        ModelInputCell::SubstitutedCategory(_) => {
            return Err(ResearchError::Inference {
                detail: format!(
                    "required categorical input `{}` was substituted",
                    input.feature
                ),
            }
            .into());
        }
        ModelInputCell::Missing | ModelInputCell::NotApplicable if !input.required => None,
        ModelInputCell::Missing | ModelInputCell::NotApplicable => {
            return Err(ResearchError::Inference {
                detail: format!("required categorical input `{}` is absent", input.feature),
            }
            .into());
        }
        ModelInputCell::Observed(_) | ModelInputCell::Substituted(_) => {
            return Err(ResearchError::Inference {
                detail: format!(
                    "categorical input `{}` received a numeric value",
                    input.feature
                ),
            }
            .into());
        }
    };
    let mut output = input
        .category_vocabulary
        .iter()
        .map(|value| f64::from(category == Some(*value)))
        .collect::<Vec<_>>();
    output.push(f64::from(
        category.is_some_and(|value| !input.category_vocabulary.contains(&value)),
    ));
    if !input.required {
        output.extend([
            f64::from(matches!(cell, ModelInputCell::Missing)),
            f64::from(matches!(cell, ModelInputCell::NotApplicable)),
            f64::from(matches!(cell, ModelInputCell::SubstitutedCategory(_))),
        ]);
    }
    Ok(output)
}

fn apply_numeric_input(input: &FittedInputColumn, cell: &ModelInputCell) -> QuantResult<Vec<f64>> {
    let value = match cell {
        ModelInputCell::Observed(value) => unit_scale(input.unit, *value),
        ModelInputCell::Substituted(value) if !input.required => unit_scale(input.unit, *value),
        ModelInputCell::Substituted(_) => {
            return Err(ResearchError::Inference {
                detail: format!("required input `{}` was substituted", input.feature),
            }
            .into());
        }
        ModelInputCell::Missing | ModelInputCell::NotApplicable if !input.required => input
            .median
            .ok_or_else(|| ResearchError::InvalidModelArtifact {
                detail: format!("optional input `{}` has no fitted median", input.feature),
            })?,
        ModelInputCell::Missing | ModelInputCell::NotApplicable => {
            return Err(ResearchError::Inference {
                detail: format!("required input `{}` is absent", input.feature),
            }
            .into());
        }
        ModelInputCell::ObservedCategory(_) | ModelInputCell::SubstitutedCategory(_) => {
            return Err(ResearchError::Inference {
                detail: format!("numeric input `{}` received a category", input.feature),
            }
            .into());
        }
    };
    let raw = strict_f64(value, input.feature.as_str())?;
    let mean = strict_f64(
        input
            .mean
            .ok_or_else(|| ResearchError::InvalidModelArtifact {
                detail: format!("numeric input `{}` has no fitted mean", input.feature),
            })?,
        "input mean",
    )?;
    let std = strict_f64(
        input
            .std
            .ok_or_else(|| ResearchError::InvalidModelArtifact {
                detail: format!(
                    "numeric input `{}` has no fitted standard deviation",
                    input.feature
                ),
            })?,
        "input standard deviation",
    )?;
    if std <= f64::EPSILON {
        return Err(ResearchError::InvalidModelArtifact {
            detail: format!(
                "input `{}` has non-positive standard deviation",
                input.feature
            ),
        }
        .into());
    }
    let standardized = (raw - mean) / std;
    if !standardized.is_finite() {
        return Err(ResearchError::Inference {
            detail: format!(
                "input `{}` standardized to a non-finite value",
                input.feature
            ),
        }
        .into());
    }
    let mut output = vec![standardized];
    if !input.required {
        output.extend([
            f64::from(matches!(cell, ModelInputCell::Missing)),
            f64::from(matches!(cell, ModelInputCell::NotApplicable)),
            f64::from(matches!(cell, ModelInputCell::Substituted(_))),
        ]);
    }
    Ok(output)
}

impl FittedInputTransform {
    /// Fit medians and standardization on exactly `matrix`'s rows, then apply
    /// the fitted transform to those rows.
    pub fn fit(matrix: &TrainingMatrix) -> QuantResult<(Self, Vec<Vec<f64>>)> {
        validate_training_matrix(matrix)?;
        let fitted = matrix
            .columns
            .iter()
            .enumerate()
            .map(|(index, spec)| fit_input_column(matrix, index, spec))
            .collect::<QuantResult<Vec<_>>>()?;
        let transform = Self {
            encoded_columns: encoded_contract(&fitted),
            inputs: fitted,
        };
        let rows = transform.apply_rows(&matrix.cells)?;
        Ok((transform, rows))
    }

    /// Apply the fitted transform to one raw row.
    pub fn apply_cells(&self, cells: &[ModelInputCell]) -> QuantResult<Vec<f64>> {
        if cells.len() != self.inputs.len() {
            return Err(ResearchError::Inference {
                detail: format!(
                    "classical input width mismatch: expected {}, got {}",
                    self.inputs.len(),
                    cells.len()
                ),
            }
            .into());
        }
        let mut output = Vec::with_capacity(self.encoded_columns.len());
        for (input, cell) in self.inputs.iter().zip(cells) {
            let encoded = if input.value_kind == FeatureValueKind::Category {
                apply_category_input(input, cell)?
            } else {
                apply_numeric_input(input, cell)?
            };
            output.extend(encoded);
        }
        if output.len() != self.encoded_columns.len() {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!(
                    "classical transform emitted {} columns but declares {}",
                    output.len(),
                    self.encoded_columns.len()
                ),
            }
            .into());
        }
        Ok(output)
    }

    /// Apply the fitted transform to multiple rows without refitting.
    pub fn apply_rows(&self, rows: &[Vec<ModelInputCell>]) -> QuantResult<Vec<Vec<f64>>> {
        rows.iter().map(|row| self.apply_cells(row)).collect()
    }
}

/// Build the governed numeric input spec from the active feature schema.
#[must_use]
pub fn matrix_spec_from_schema(
    schema: &FeatureSchema,
    label_name: LabelName,
    label_horizon_secs: u64,
) -> FeatureMatrixSpec {
    FeatureMatrixSpec {
        columns: schema
            .specs()
            .iter()
            .map(|spec| FeatureColumnSpec {
                feature: spec.name.clone(),
                unit: spec.unit,
                value_kind: spec.value_kind,
                required: false,
            })
            .collect(),
        label_name,
        label_horizon_secs,
    }
}

/// Resolve a model-owned ordered raw-input contract against the governed
/// feature catalog. Unknown or synthetic names fail before any row is read.
pub fn matrix_spec_from_contract(
    schema: &FeatureSchema,
    contract: &ModelInputContract,
    label_name: LabelName,
    label_horizon_secs: u64,
) -> QuantResult<FeatureMatrixSpec> {
    contract
        .validate()
        .map_err(|detail| ResearchError::MatrixBuild {
            detail: format!("invalid model input contract: {detail}"),
        })?;
    if contract.inputs.is_empty() {
        return Err(ResearchError::MatrixBuild {
            detail: "model input contract has no raw inputs".to_owned(),
        }
        .into());
    }
    let columns = contract
        .inputs
        .iter()
        .map(|input| {
            let name = FeatureName::new(input.feature_name.clone());
            let feature = schema
                .by_name(&name)
                .ok_or_else(|| ResearchError::MatrixBuild {
                    detail: format!(
                        "model input `{}` is absent from the governed feature catalog",
                        input.feature_name
                    ),
                })?;
            Ok(FeatureColumnSpec {
                feature: feature.name.clone(),
                unit: feature.unit,
                value_kind: feature.value_kind,
                required: input.requiredness == ModelInputRequiredness::Required,
            })
        })
        .collect::<QuantResult<Vec<_>>>()?;
    Ok(FeatureMatrixSpec {
        columns,
        label_name,
        label_horizon_secs,
    })
}

/// Probe row admission and estimator width without fitting dataset-global
/// statistics. The real trainer still fails closed on zero variance or an
/// optional input with no observed training-partition value.
pub fn probe_matrix_coverage(
    examples: &[TrainingExample],
    schema: &FeatureSchema,
    contract: &ModelInputContract,
    label_name: &LabelName,
    label_horizon_secs: u64,
) -> QuantResult<MatrixCoverageProbe> {
    let spec = matrix_spec_from_contract(schema, contract, label_name.clone(), label_horizon_secs)?;
    let label_rows = u64::try_from(
        examples
            .iter()
            .filter_map(|example| matching_label(example, &spec))
            .filter(|label| label_f64(label.value).is_some())
            .count(),
    )
    .map_err(|error| ResearchError::MatrixBuild {
        detail: format!("matrix probe label-row count does not fit u64: {error}"),
    })?;
    let feature_columns = spec.columns.iter().try_fold(0_u64, |total, column| {
        total
            .checked_add(1 + if column.required { 0 } else { 3 })
            .ok_or_else(|| ResearchError::MatrixBuild {
                detail: "matrix probe encoded-column count overflowed u64".to_owned(),
            })
    })?;
    if examples.is_empty() {
        return Ok(MatrixCoverageProbe {
            accepted_rows: 0,
            rejected_rows: 0,
            label_rows,
            label_name: label_name.as_str().to_owned(),
            label_horizon_secs,
            feature_columns,
        });
    }
    let matrix = build_training_matrix(examples, &spec)?;
    Ok(MatrixCoverageProbe {
        accepted_rows: u64::try_from(matrix.row_count()).map_err(|error| {
            ResearchError::MatrixBuild {
                detail: format!("matrix probe accepted-row count does not fit u64: {error}"),
            }
        })?,
        rejected_rows: u64::try_from(matrix.rejected_rows).map_err(|error| {
            ResearchError::MatrixBuild {
                detail: format!("matrix probe rejected-row count does not fit u64: {error}"),
            }
        })?,
        label_rows,
        label_name: label_name.as_str().to_owned(),
        label_horizon_secs,
        feature_columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        features::{FeatureCell, FeatureStaleness, FeatureValue, FeatureVector, NullReason},
        training::fixtures,
    };
    use chrono::{Duration, TimeZone};
    use quant_pivot_models::{
        domain::DecisionClock,
        enums::quant::DataQualityStatus,
        types::{MarketId, SchemaVersion, TokenId, TrainingExampleId, TrainingSampleSource},
    };
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;

    fn example(value: FeatureValue, substitution: bool, offset: i64) -> TrainingExample {
        let cell = if substitution {
            FeatureCell::substituted(
                value,
                NullReason::SourceUnavailable,
                None,
                FeatureStaleness::Unknown,
            )
        } else {
            FeatureCell::observed(value, None, FeatureStaleness::Unknown)
        };
        example_cell(cell, offset)
    }

    fn missing_example(offset: i64) -> TrainingExample {
        example_cell(
            FeatureCell::missing(
                NullReason::SourceUnavailable,
                None,
                FeatureStaleness::Unknown,
            ),
            offset,
        )
    }

    fn not_applicable_example(offset: i64) -> TrainingExample {
        example_cell(
            FeatureCell::not_applicable(NullReason::NotApplicable),
            offset,
        )
    }

    fn example_cell(cell: FeatureCell, offset: i64) -> TrainingExample {
        let feature = FeatureName::from_static("spread_bps");
        let as_of = Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("timestamp")
            + Duration::seconds(offset);
        TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: MarketId::new(format!("m{offset}")),
            token_id: TokenId::new(format!("t{offset}")),
            selected_market: fixtures::selected_market(
                &MarketId::new(format!("m{offset}")),
                &TokenId::new(format!("t{offset}")),
                MarketCategory::Sports,
            ),
            decision_boundary: DecisionClock::new(0).boundary(as_of).expect("boundary"),
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector: FeatureVector {
                market_id: MarketId::new(format!("m{offset}")),
                token_id: Some(TokenId::new(format!("t{offset}"))),
                decision_at: as_of,
                generic_schema_version: SchemaVersion::FIRST,
                generic: BTreeMap::from([(feature, cell)]),
                domain: None,
                data_quality: DataQualityStatus::Fresh,
            },
            factor_values: Vec::new(),
            labels: vec![TrainingLabel {
                label_name: LabelName::from_static("return"),
                horizon_secs: 60,
                value: dec!(1),
                is_resolved: true,
                matured_at: as_of + Duration::seconds(60),
            }],
            source_refs: Vec::new(),
            decision_capture: None,
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        }
    }

    fn spec(required: bool) -> FeatureMatrixSpec {
        FeatureMatrixSpec {
            columns: vec![FeatureColumnSpec {
                feature: FeatureName::from_static("spread_bps"),
                unit: FeatureUnit::Bps,
                value_kind: FeatureValueKind::Bps,
                required,
            }],
            label_name: LabelName::from_static("return"),
            label_horizon_secs: 60,
        }
    }

    fn category_spec(required: bool) -> FeatureMatrixSpec {
        FeatureMatrixSpec {
            columns: vec![FeatureColumnSpec {
                feature: FeatureName::from_static("spread_bps"),
                unit: FeatureUnit::None,
                value_kind: FeatureValueKind::Category,
                required,
            }],
            label_name: LabelName::from_static("return"),
            label_horizon_secs: 60,
        }
    }

    #[test]
    fn bps_scale_and_median_are_shared_by_fit_and_apply() {
        let examples = vec![
            example(FeatureValue::Bps(dec!(100)), false, 0),
            example(FeatureValue::Bps(dec!(300)), false, 1),
            missing_example(2),
            not_applicable_example(3),
        ];
        let matrix = build_training_matrix(&examples, &spec(false)).expect("raw matrix");
        let (transform, fitted_rows) = FittedInputTransform::fit(&matrix).expect("fit");
        assert_eq!(transform.inputs[0].median, Some(dec!(0.02)));
        let replayed = transform.apply_rows(&matrix.cells).expect("apply");
        assert_eq!(
            fitted_rows, replayed,
            "fit and apply must be byte-identical"
        );
        assert_eq!(
            fitted_rows[2][1].to_bits(),
            1.0_f64.to_bits(),
            "missing indicator"
        );
        assert_eq!(
            fitted_rows[2][2].to_bits(),
            0.0_f64.to_bits(),
            "not-applicable indicator"
        );
        assert_eq!(
            fitted_rows[2][3].to_bits(),
            0.0_f64.to_bits(),
            "substituted indicator"
        );
        assert_eq!(
            fitted_rows[3][1].to_bits(),
            0.0_f64.to_bits(),
            "not-applicable is not missing"
        );
        assert_eq!(
            fitted_rows[3][2].to_bits(),
            1.0_f64.to_bits(),
            "not-applicable indicator"
        );
    }

    #[test]
    fn substitution_is_a_distinct_state() {
        let examples = vec![
            example(FeatureValue::Bps(dec!(100)), false, 0),
            example(FeatureValue::Bps(dec!(300)), false, 1),
            example(FeatureValue::Bps(dec!(200)), true, 2),
        ];
        let matrix = build_training_matrix(&examples, &spec(false)).expect("raw matrix");
        let (transform, rows) = FittedInputTransform::fit(&matrix).expect("fit");
        assert_eq!(rows[2][3].to_bits(), 1.0_f64.to_bits());
        assert_eq!(transform.inputs.len(), 1);
        assert!(!transform.inputs[0].required);
    }

    #[test]
    fn required_missing_or_substituted_row_is_rejected() {
        let examples = vec![
            example(FeatureValue::Bps(dec!(100)), false, 0),
            missing_example(1),
            example(FeatureValue::Bps(dec!(200)), true, 2),
        ];
        let matrix = build_training_matrix(&examples, &spec(true)).expect("raw matrix");
        assert_eq!(matrix.row_count(), 1);
        assert_eq!(matrix.rejected_rows, 2);
    }

    #[test]
    fn absent_contract_column_and_value_kind_mismatch_fail_closed() {
        let mut absent = example(FeatureValue::Bps(dec!(100)), false, 0);
        absent.feature_vector.generic.clear();
        assert!(build_training_matrix(&[absent], &spec(false)).is_err());

        let wrong_kind = example(
            FeatureValue::Probability(quant_pivot_models::types::Probability::new(dec!(0.5))),
            false,
            1,
        );
        assert!(build_training_matrix(&[wrong_kind], &spec(false)).is_err());
    }

    #[test]
    fn zero_variance_fails_closed() {
        let examples = vec![
            example(FeatureValue::Bps(dec!(100)), false, 0),
            example(FeatureValue::Bps(dec!(100)), false, 1),
        ];
        let matrix = build_training_matrix(&examples, &spec(true)).expect("raw matrix");
        assert!(FittedInputTransform::fit(&matrix).is_err());
    }

    #[test]
    fn category_vocabulary_is_frozen_and_unknown_is_explicit() {
        use quant_pivot_models::enums::common::MarketCategory;

        let examples = vec![
            example(FeatureValue::Category(MarketCategory::Sports), false, 0),
            example(FeatureValue::Category(MarketCategory::Politics), false, 1),
        ];
        let matrix =
            build_training_matrix(&examples, &category_spec(false)).expect("category matrix");
        let (transform, fitted_rows) = FittedInputTransform::fit(&matrix).expect("category fit");
        assert_eq!(
            transform.inputs[0].category_vocabulary,
            vec![MarketCategory::Sports, MarketCategory::Politics]
        );
        assert_eq!(
            fitted_rows,
            transform
                .apply_rows(&matrix.cells)
                .expect("category replay")
        );

        let unknown = transform
            .apply_cells(&[ModelInputCell::ObservedCategory(MarketCategory::Crypto)])
            .expect("unknown category");
        let unknown_index = transform
            .encoded_columns
            .iter()
            .position(|column| column.kind == EncodedColumnKind::CategoryUnknown)
            .expect("unknown column");
        assert_eq!(unknown[unknown_index].to_bits(), 1.0_f64.to_bits());
        assert_eq!(unknown.iter().sum::<f64>().to_bits(), 1.0_f64.to_bits());

        let missing = transform
            .apply_cells(&[ModelInputCell::Missing])
            .expect("missing category");
        let missing_index = transform
            .encoded_columns
            .iter()
            .position(|column| column.kind == EncodedColumnKind::MissingIndicator)
            .expect("missing indicator");
        assert_eq!(missing[missing_index].to_bits(), 1.0_f64.to_bits());
        assert_eq!(missing[unknown_index].to_bits(), 0.0_f64.to_bits());
    }
}
