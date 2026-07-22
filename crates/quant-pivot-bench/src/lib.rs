//! Performance benchmark crate for quant-pivot hot paths.

use std::{error::Error, num::TryFromIntError};

use chrono::{DateTime, Utc};
use ndarray::Array1;
use quant_pivot_allocator as _;
use quant_pivot_models::{enums::feature::FeatureValueKind, types::stable_name::FeatureName};
use quant_pivot_research::{
    features::FeatureUnit,
    training::{FeatureColumnSpec, ModelInputCell, RawInputMatrix, TrainingMatrix},
};
use rust_decimal::Decimal;

/// Governed hard-gate training width.
pub const TRAINING_MATRIX_COLUMNS: usize = 128;

/// Build the deterministic numeric matrix shared by Criterion and the
/// single-shot RSS/time gate.
pub fn training_matrix_fixture(rows: usize) -> Result<TrainingMatrix, Box<dyn Error>> {
    let cell_count = rows
        .checked_mul(TRAINING_MATRIX_COLUMNS)
        .ok_or("fixture shape overflowed usize")?;
    let values = (0..cell_count)
        .map(|index| {
            let row = index / TRAINING_MATRIX_COLUMNS;
            let column = index % TRAINING_MATRIX_COLUMNS;
            let value = i64::try_from((row + column) % 1_009)?;
            Ok(ModelInputCell::Observed(Decimal::new(value + 1, 3)))
        })
        .collect::<Result<Vec<_>, TryFromIntError>>()?;
    let decision_at = DateTime::<Utc>::UNIX_EPOCH;
    Ok(TrainingMatrix {
        cells: RawInputMatrix::from_flat(values, rows, TRAINING_MATRIX_COLUMNS)?,
        labels: Array1::from_iter(
            (0..rows).map(|row| if row.is_multiple_of(2) { 0.0 } else { 1.0 }),
        ),
        columns: (0..TRAINING_MATRIX_COLUMNS)
            .map(|column| FeatureColumnSpec {
                feature: FeatureName::new(format!("f{column}")),
                unit: FeatureUnit::Ratio,
                value_kind: FeatureValueKind::Decimal,
                required: true,
            })
            .collect(),
        rejected_rows: 0,
        row_decision_at: vec![decision_at; rows],
        row_label_horizon_end: vec![decision_at; rows],
    })
}
