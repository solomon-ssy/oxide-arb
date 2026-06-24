//! Parquet (de)serialization of a training dataset's examples.
//!
//! The artifact is lossless and queryable: queryable meta columns
//! (`example_id` / `market_id` / `token_id` / `as_of_ms`) sit alongside a
//! canonical-JSON `payload` column that fully reconstructs each
//! [`TrainingExample`]. Offline only (`dataframe` feature); never on the hot path.

use std::io::Cursor;

use polars::{
    error::PolarsError,
    prelude::{
        Column, DataFrame, IntoLazy, ParquetReader, ParquetWriter, SerReader, SortMultipleOptions,
    },
};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};

use super::TrainingExample;

/// A Polars failure surfaced during dataset Parquet (de)serialization.
///
/// Local newtype so this crate can implement `From` despite the orphan rule
/// (`PolarsError` and [`ResearchError`] are foreign types).
pub struct ParquetPolarsError(PolarsError);

impl From<PolarsError> for ParquetPolarsError {
    fn from(error: PolarsError) -> Self {
        Self(error)
    }
}

impl From<ParquetPolarsError> for ResearchError {
    fn from(error: ParquetPolarsError) -> Self {
        Self::ParquetCodec {
            detail: error.0.to_string(),
        }
    }
}

impl From<ParquetPolarsError> for QuantError {
    fn from(error: ParquetPolarsError) -> Self {
        ResearchError::from(error).into()
    }
}

/// Lossless Parquet codec for a dataset's [`TrainingExample`] rows.
pub struct DatasetParquetCodec;

impl DatasetParquetCodec {
    /// Encode examples into Parquet bytes (sorted by `(market_id, token_id,
    /// as_of_ms)` so the byte stream is order-stable).
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::ParquetCodec`] on serialization or Parquet
    /// write failures.
    pub fn encode(examples: &[TrainingExample]) -> QuantResult<Vec<u8>> {
        let mut example_ids = Vec::with_capacity(examples.len());
        let mut market_ids = Vec::with_capacity(examples.len());
        let mut token_ids = Vec::with_capacity(examples.len());
        let mut as_of_ms = Vec::with_capacity(examples.len());
        let mut payloads = Vec::with_capacity(examples.len());
        for example in examples {
            example_ids.push(example.example_id.as_uuid().to_string());
            market_ids.push(example.market_id.as_str().to_owned());
            token_ids.push(example.token_id.as_str().to_owned());
            as_of_ms.push(example.as_of.timestamp_millis());
            let payload =
                serde_json::to_string(example).map_err(|error| ResearchError::ParquetCodec {
                    detail: format!("example serialization failed: {error}"),
                })?;
            payloads.push(payload);
        }

        let columns = vec![
            Column::new("example_id".into(), example_ids),
            Column::new("market_id".into(), market_ids),
            Column::new("token_id".into(), token_ids),
            Column::new("as_of_ms".into(), as_of_ms),
            Column::new("payload".into(), payloads),
        ];
        let frame = DataFrame::new(examples.len(), columns).map_err(ParquetPolarsError::from)?;
        let mut sorted = frame
            .lazy()
            .sort(
                ["market_id", "token_id", "as_of_ms"],
                SortMultipleOptions::default(),
            )
            .collect()
            .map_err(ParquetPolarsError::from)?;

        let mut buffer = Vec::new();
        ParquetWriter::new(&mut buffer)
            .finish(&mut sorted)
            .map_err(ParquetPolarsError::from)?;
        Ok(buffer)
    }

    /// Decode Parquet bytes back into examples (read from the `payload` column).
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::ParquetCodec`] on Parquet read or deserialization
    /// failures, including a missing/`null` payload.
    pub fn decode(bytes: &[u8]) -> QuantResult<Vec<TrainingExample>> {
        let frame = ParquetReader::new(Cursor::new(bytes))
            .finish()
            .map_err(ParquetPolarsError::from)?;
        let payload = frame
            .column("payload")
            .map_err(ParquetPolarsError::from)?
            .str()
            .map_err(ParquetPolarsError::from)?;
        let mut examples = Vec::with_capacity(payload.len());
        for value in payload.iter() {
            let raw = value.ok_or_else(|| ResearchError::ParquetCodec {
                detail: "null payload cell in dataset parquet".to_owned(),
            })?;
            let example =
                serde_json::from_str(raw).map_err(|error| ResearchError::ParquetCodec {
                    detail: format!("example deserialization failed: {error}"),
                })?;
            examples.push(example);
        }
        Ok(examples)
    }
}

#[cfg(test)]
mod tests {
    use super::DatasetParquetCodec;
    use crate::{
        artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
        training::fixtures::example,
    };

    #[test]
    fn parquet_dataset_roundtrip() {
        let examples = vec![example("bbb", 200), example("aaa", 100)];
        let bytes = DatasetParquetCodec::encode(&examples).expect("encode");
        let mut decoded = DatasetParquetCodec::decode(&bytes).expect("decode");
        decoded.sort_by(|a, b| a.market_id.as_str().cmp(b.market_id.as_str()));
        let mut expected = examples;
        expected.sort_by(|a, b| a.market_id.as_str().cmp(b.market_id.as_str()));
        assert_eq!(
            decoded, expected,
            "roundtrip must preserve examples exactly"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn parquet_roundtrip_via_local_artifact_store() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let root = std::env::temp_dir().join(format!("quant-pivot-parquet-it-{nanos}"));
        let store = LocalArtifactStore::new(&root);
        let examples = vec![example("store-roundtrip", 123)];
        let bytes = DatasetParquetCodec::encode(&examples).expect("encode");
        let key =
            ArtifactKey::new(ArtifactNamespace::Dataset, "dataset-it", "parquet").expect("key");
        let uri = store.put(key, &bytes).await.expect("put");
        let loaded = store.get(&uri).await.expect("get");
        let decoded = DatasetParquetCodec::decode(&loaded).expect("decode");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].market_id.as_str(), "store-roundtrip");
    }
}
