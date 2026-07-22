//! Fail-closed serving-only surface for offline dataset Parquet operations.

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::types::DatasetManifest;

use super::TrainingExample;

/// Decoded dataset envelope shape retained at the compile boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedDatasetParquet {
    pub manifest: DatasetManifest,
    pub examples: Vec<TrainingExample>,
}

/// Dataset codec unavailable in serving-only binaries.
pub struct DatasetParquetCodec;

impl DatasetParquetCodec {
    pub fn encode(
        _examples: &[TrainingExample],
        _manifest: &DatasetManifest,
    ) -> QuantResult<Vec<u8>> {
        Err(research_jobs_disabled())
    }

    pub fn decode(_bytes: &[u8]) -> QuantResult<Vec<TrainingExample>> {
        Err(research_jobs_disabled())
    }

    pub fn decode_with_manifest(_bytes: &[u8]) -> QuantResult<DecodedDatasetParquet> {
        Err(research_jobs_disabled())
    }
}

fn research_jobs_disabled() -> QuantError {
    ResearchError::NotEligible {
        code: "research_jobs_feature_disabled",
        detail: "dataset Parquet requires the compile-time `research-jobs` feature".to_owned(),
    }
    .into()
}
