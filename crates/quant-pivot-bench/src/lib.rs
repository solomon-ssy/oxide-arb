//! Performance benchmark crate for quant-pivot hot paths.

use std::{
    fs,
    io::{Error as IoError, ErrorKind, Result as IoResult},
};

use chrono::{DateTime, Utc};
use ndarray::Array1;
use quant_pivot_allocator as _;
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{enums::feature::FeatureValueKind, types::stable_name::FeatureName};
use quant_pivot_research::{
    features::FeatureUnit,
    training::{FeatureColumnSpec, ModelInputCell, RawInputMatrix, TrainingMatrix},
};
use rust_decimal::Decimal;

/// Governed hard-gate training width.
pub const TRAINING_MATRIX_COLUMNS: usize = 128;

/// Build the deterministic mixed-state Decimal matrix used by performance gates.
///
/// Required columns remain fully observed; optional columns exercise observed,
/// substituted, missing, and structurally inapplicable cells so the gate
/// measures the production transform shape.
pub fn training_matrix_fixture(rows: usize) -> QuantResult<TrainingMatrix> {
    let cell_count =
        rows.checked_mul(TRAINING_MATRIX_COLUMNS)
            .ok_or_else(|| ResearchError::MatrixBuild {
                detail: "training gate fixture shape overflowed usize".to_owned(),
            })?;
    let values = (0..cell_count)
        .map(|index| {
            let row = index / TRAINING_MATRIX_COLUMNS;
            let column = index % TRAINING_MATRIX_COLUMNS;
            let value = i64::try_from((row + column) % 1_009).map_err(|error| {
                ResearchError::MatrixBuild {
                    detail: format!("training gate fixture value does not fit i64: {error}"),
                }
            })?;
            let decimal = Decimal::new(value + 1, 3);
            let cell = if column < TRAINING_MATRIX_COLUMNS / 2 {
                ModelInputCell::Observed(decimal)
            } else if (row + column).is_multiple_of(13) {
                ModelInputCell::NotApplicable
            } else if (row + column).is_multiple_of(11) {
                ModelInputCell::Missing
            } else if (row + column).is_multiple_of(7) {
                ModelInputCell::Substituted(decimal)
            } else {
                ModelInputCell::Observed(decimal)
            };
            Ok(cell)
        })
        .collect::<QuantResult<Vec<_>>>()?;
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
                required: column < TRAINING_MATRIX_COLUMNS / 2,
            })
            .collect(),
        rejected_rows: 0,
        row_decision_at: vec![decision_at; rows],
        row_label_horizon_end: vec![decision_at; rows],
    })
}

/// Return the process high-water RSS reported by Linux, in bytes.
///
/// Non-Linux development hosts return `None`; fixed Linux performance runners
/// treat a missing value as a failed qualification contract.
pub fn peak_rss_bytes() -> IoResult<Option<u64>> {
    let status = match fs::read_to_string("/proc/self/status") {
        Ok(status) => status,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let Some(line) = status.lines().find(|line| line.starts_with("VmHWM:")) else {
        return Ok(None);
    };
    let kib = line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "VmHWM is missing its value"))?
        .parse::<u64>()
        .map_err(|error| IoError::new(ErrorKind::InvalidData, error))?;
    Ok(Some(kib.saturating_mul(1_024)))
}

/// Enforce a kernel RSS ceiling on fixed Linux performance runners.
pub fn enforce_linux_peak_rss(peak_rss: Option<u64>, limit: u64, gate: &str) -> IoResult<()> {
    if cfg!(target_os = "linux") && peak_rss.is_none() {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            format!("{gate} gate could not read Linux VmHWM"),
        ));
    }
    if peak_rss.is_some_and(|rss| rss > limit) {
        return Err(IoError::other(format!(
            "{gate} gate exceeded {limit} bytes RSS: {peak_rss:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use quant_pivot_error::QuantResult;
    use quant_pivot_research::model::artifact::FittedInputTransform;

    use super::{TRAINING_MATRIX_COLUMNS, training_matrix_fixture};

    #[test]
    fn mixed_satisfies_required_contract() -> QuantResult<()> {
        let rows = 34;
        let matrix = training_matrix_fixture(rows)?;
        let (transform, dense) = FittedInputTransform::fit(&matrix)?;

        assert_eq!(dense.row_count(), rows);
        assert!(transform.encoded_columns.len() > TRAINING_MATRIX_COLUMNS);
        Ok(())
    }
}
