//! Parquet (de)serialization of a training dataset's examples.
//!
//! The artifact is lossless and queryable: queryable meta columns
//! (`example_id` / `market_id` / `token_id` / `decision_at_ms`) sit alongside a
//! canonical-JSON `payload` column that fully reconstructs each
//! [`TrainingExample`]. Offline only (`dataframe` feature); never on the hot path.

use std::io::Cursor;

use polars::{
    error::PolarsError,
    prelude::{
        Column, DataFrame, Int64Chunked, IntoLazy, ParquetReader, ParquetWriter, SerReader,
        SortMultipleOptions, StringChunked, UInt32Chunked,
    },
};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::types::{DATASET_ARTIFACT_FORMAT_VERSION, DatasetManifest};

use super::{TrainingExample, verify_dataset_manifest};
const MANIFEST_ROW: &str = "manifest";
const EXAMPLE_ROW: &str = "example";

/// Fully decoded v4 Parquet envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedDatasetParquet {
    pub manifest: DatasetManifest,
    pub examples: Vec<TrainingExample>,
}

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

struct DatasetColumns<'a> {
    format_versions: &'a UInt32Chunked,
    row_kinds: &'a StringChunked,
    example_ids: &'a StringChunked,
    market_ids: &'a StringChunked,
    token_ids: &'a StringChunked,
    decision_at_ms: &'a Int64Chunked,
    payloads: &'a StringChunked,
    manifests: &'a StringChunked,
}

impl<'a> DatasetColumns<'a> {
    fn read(frame: &'a DataFrame) -> QuantResult<Self> {
        let format_versions = frame
            .column("format_version")
            .map_err(|error| ResearchError::ParquetCodec {
                detail: format!(
                    "dataset is not the required v{DATASET_ARTIFACT_FORMAT_VERSION} artifact: {error}"
                ),
            })?
            .u32()
            .map_err(ParquetPolarsError::from)?;
        Ok(Self {
            format_versions,
            row_kinds: string_column(frame, "row_kind")?,
            example_ids: string_column(frame, "example_id")?,
            market_ids: string_column(frame, "market_id")?,
            token_ids: string_column(frame, "token_id")?,
            decision_at_ms: frame
                .column("decision_at_ms")
                .map_err(ParquetPolarsError::from)?
                .i64()
                .map_err(ParquetPolarsError::from)?,
            payloads: string_column(frame, "payload")?,
            manifests: string_column(frame, "manifest")?,
        })
    }

    fn validate_lengths(&self) -> QuantResult<usize> {
        let row_count = self.format_versions.len();
        if [
            self.row_kinds.len(),
            self.example_ids.len(),
            self.market_ids.len(),
            self.token_ids.len(),
            self.decision_at_ms.len(),
            self.payloads.len(),
            self.manifests.len(),
        ]
        .into_iter()
        .any(|len| len != row_count)
        {
            return Err(ResearchError::ParquetCodec {
                detail: "dataset parquet columns have different lengths".to_owned(),
            }
            .into());
        }
        Ok(row_count)
    }
}

fn string_column<'a>(frame: &'a DataFrame, name: &str) -> QuantResult<&'a StringChunked> {
    frame
        .column(name)
        .map_err(ParquetPolarsError::from)?
        .str()
        .map_err(ParquetPolarsError::from)
        .map_err(Into::into)
}

fn validate_format_version(columns: &DatasetColumns<'_>, index: usize) -> QuantResult<()> {
    let version =
        columns
            .format_versions
            .get(index)
            .ok_or_else(|| ResearchError::ParquetCodec {
                detail: "null format_version cell in dataset parquet".to_owned(),
            })?;
    if version != DATASET_ARTIFACT_FORMAT_VERSION {
        return Err(ResearchError::ParquetCodec {
            detail: format!(
                "unsupported dataset artifact format {version}, expected {DATASET_ARTIFACT_FORMAT_VERSION}"
            ),
        }
        .into());
    }
    Ok(())
}

fn decode_manifest_row(
    columns: &DatasetColumns<'_>,
    index: usize,
    manifest: &mut Option<DatasetManifest>,
) -> QuantResult<()> {
    if manifest.is_some() || columns.payloads.get(index).is_some() {
        return Err(ResearchError::ParquetCodec {
            detail: "dataset parquet must contain exactly one clean manifest row".to_owned(),
        }
        .into());
    }
    let raw = columns
        .manifests
        .get(index)
        .ok_or_else(|| ResearchError::ParquetCodec {
            detail: "manifest row has no manifest payload".to_owned(),
        })?;
    *manifest = Some(
        serde_json::from_str(raw).map_err(|error| ResearchError::ParquetCodec {
            detail: format!("dataset manifest deserialization failed: {error}"),
        })?,
    );
    Ok(())
}

fn decode_example_row(columns: &DatasetColumns<'_>, index: usize) -> QuantResult<TrainingExample> {
    if columns.manifests.get(index).is_some() {
        return Err(ResearchError::ParquetCodec {
            detail: "example row contains an unexpected manifest payload".to_owned(),
        }
        .into());
    }
    let raw = columns
        .payloads
        .get(index)
        .ok_or_else(|| ResearchError::ParquetCodec {
            detail: "example row has no payload".to_owned(),
        })?;
    let example: TrainingExample =
        serde_json::from_str(raw).map_err(|error| ResearchError::ParquetCodec {
            detail: format!("example deserialization failed: {error}"),
        })?;
    let query_matches = columns.example_ids.get(index)
        == Some(example.example_id.as_uuid().to_string().as_str())
        && columns.market_ids.get(index) == Some(example.market_id.as_str())
        && columns.token_ids.get(index) == Some(example.token_id.as_str())
        && columns.decision_at_ms.get(index) == Some(example.decision_at().timestamp_millis());
    if !query_matches {
        return Err(ResearchError::ParquetCodec {
            detail: format!("query columns do not match example payload at parquet row {index}"),
        }
        .into());
    }
    Ok(example)
}

fn decode_rows(columns: &DatasetColumns<'_>) -> QuantResult<DecodedDatasetParquet> {
    let row_count = columns.validate_lengths()?;
    let mut manifest = None;
    let example_capacity = row_count
        .checked_sub(1)
        .ok_or_else(|| ResearchError::ParquetCodec {
            detail: "dataset parquet contains no manifest row".to_owned(),
        })?;
    let mut examples = Vec::with_capacity(example_capacity);
    for index in 0..row_count {
        validate_format_version(columns, index)?;
        let row_kind = columns
            .row_kinds
            .get(index)
            .ok_or_else(|| ResearchError::ParquetCodec {
                detail: "null row_kind cell in dataset parquet".to_owned(),
            })?;
        match row_kind {
            MANIFEST_ROW => decode_manifest_row(columns, index, &mut manifest)?,
            EXAMPLE_ROW => examples.push(decode_example_row(columns, index)?),
            other => {
                return Err(ResearchError::ParquetCodec {
                    detail: format!("unknown dataset parquet row_kind {other}"),
                }
                .into());
            }
        }
    }
    let manifest = manifest.ok_or_else(|| ResearchError::ParquetCodec {
        detail: "dataset parquet has no manifest row".to_owned(),
    })?;
    verify_dataset_manifest(&manifest, &examples)?;
    Ok(DecodedDatasetParquet { manifest, examples })
}

impl DatasetParquetCodec {
    /// Encode examples into Parquet bytes (sorted by `(market_id, token_id,
    /// decision_at_ms)` so the byte stream is order-stable).
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::ParquetCodec`] on serialization or Parquet
    /// write failures.
    pub fn encode(
        examples: &[TrainingExample],
        manifest: &DatasetManifest,
    ) -> QuantResult<Vec<u8>> {
        verify_dataset_manifest(manifest, examples)?;
        let row_count =
            examples
                .len()
                .checked_add(1)
                .ok_or_else(|| ResearchError::ParquetCodec {
                    detail: "dataset parquet row count overflow".to_owned(),
                })?;
        let format_versions = vec![DATASET_ARTIFACT_FORMAT_VERSION; row_count];
        let mut row_kinds = Vec::with_capacity(row_count);
        let mut example_ids = Vec::with_capacity(row_count);
        let mut market_ids = Vec::with_capacity(row_count);
        let mut token_ids = Vec::with_capacity(row_count);
        let mut decision_at_ms = Vec::with_capacity(row_count);
        let mut payloads = Vec::with_capacity(row_count);
        let mut manifests = Vec::with_capacity(row_count);

        row_kinds.push(MANIFEST_ROW.to_owned());
        example_ids.push(None::<String>);
        market_ids.push(None::<String>);
        token_ids.push(None::<String>);
        decision_at_ms.push(None::<i64>);
        payloads.push(None::<String>);
        manifests.push(Some(serde_json::to_string(manifest).map_err(|error| {
            ResearchError::ParquetCodec {
                detail: format!("dataset manifest serialization failed: {error}"),
            }
        })?));
        for example in examples {
            row_kinds.push(EXAMPLE_ROW.to_owned());
            example_ids.push(Some(example.example_id.as_uuid().to_string()));
            market_ids.push(Some(example.market_id.as_str().to_owned()));
            token_ids.push(Some(example.token_id.as_str().to_owned()));
            decision_at_ms.push(Some(example.decision_at().timestamp_millis()));
            let payload =
                serde_json::to_string(example).map_err(|error| ResearchError::ParquetCodec {
                    detail: format!("example serialization failed: {error}"),
                })?;
            payloads.push(Some(payload));
            manifests.push(None);
        }

        let columns = vec![
            Column::new("format_version".into(), format_versions),
            Column::new("row_kind".into(), row_kinds),
            Column::new("example_id".into(), example_ids),
            Column::new("market_id".into(), market_ids),
            Column::new("token_id".into(), token_ids),
            Column::new("decision_at_ms".into(), decision_at_ms),
            Column::new("payload".into(), payloads),
            Column::new("manifest".into(), manifests),
        ];
        let frame = DataFrame::new(row_count, columns).map_err(ParquetPolarsError::from)?;
        let mut sorted = frame
            .lazy()
            .sort(
                ["row_kind", "market_id", "token_id", "decision_at_ms"],
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
        Ok(Self::decode_with_manifest(bytes)?.examples)
    }

    /// Decode and validate the complete v2 envelope, including its manifest.
    pub fn decode_with_manifest(bytes: &[u8]) -> QuantResult<DecodedDatasetParquet> {
        let frame = ParquetReader::new(Cursor::new(bytes))
            .finish()
            .map_err(ParquetPolarsError::from)?;
        decode_rows(&DatasetColumns::read(&frame)?)
    }
}

#[cfg(test)]
mod tests {
    use super::DatasetParquetCodec;
    use crate::{
        artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore, LocalArtifactStore},
        training::{
            DatasetHashContract, TrainingDatasetArtifact, TrainingExample,
            dataset_source_fingerprint,
            fixtures::{bind_capture_to_boundary, example},
        },
    };
    use chrono::{Duration, Utc};
    use polars::prelude::{Column, DataFrame, ParquetWriter};
    use quant_pivot_models::{
        domain::{DecisionClock, DecisionSource},
        enums::quant::DatasetPurpose,
        types::{
            ArtifactUri, ContentHash, DATASET_ARTIFACT_FORMAT_VERSION, DatasetManifest,
            ModelSpecId, RuntimeConfigVersionId, SourceSliceManifestRef, TrainingDatasetId,
            builtin_research_profiles,
        },
    };
    use std::{
        env,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn manifest(examples: &[TrainingExample]) -> DatasetManifest {
        let hash = |seed: char| {
            ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
        };
        let model_spec_id = ModelSpecId::from_v7();
        let window_start = Utc::now() - Duration::days(1);
        let window_end = Utc::now();
        let feature_hash = hash('1');
        let factor_hash = hash('2');
        let label_hash = hash('3');
        let semantic_dataset_hash = TrainingDatasetArtifact::compute_dataset_hash(
            DatasetHashContract {
                model_spec_id: &model_spec_id,
                window_start,
                window_end,
                purpose: DatasetPurpose::Training,
                feature_schema_hash: &feature_hash,
                factor_schema_hash: &factor_hash,
                label_schema_hash: &label_hash,
            },
            examples,
        )
        .expect("semantic hash");
        DatasetManifest {
            format_version: DATASET_ARTIFACT_FORMAT_VERSION,
            training_dataset_id: TrainingDatasetId::from_v7(),
            profile_ref: builtin_research_profiles()
                .expect("built-in profiles")
                .remove(0)
                .profile_ref,
            research_program_hash: hash('4'),
            source_slice: SourceSliceManifestRef {
                manifest_uri: ArtifactUri::parse("s3://fixture/source-slice.json").expect("URI"),
                manifest_hash: hash('5'),
            },
            model_spec_id,
            trade_policy_artifact_id: None,
            trade_policy_hash: None,
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            window_start,
            window_end,
            purpose: DatasetPurpose::Training,
            knowledge_lag_secs: examples
                .first()
                .map_or(0, |example| example.decision_boundary.knowledge_lag_secs()),
            sample_interval_secs: 60,
            horizons_secs: vec![60],
            feature_schema_hash: hash('1'),
            factor_schema_hash: hash('2'),
            label_schema_hash: hash('3'),
            semantic_dataset_hash,
            source_fingerprint: dataset_source_fingerprint(examples).expect("source fingerprint"),
            sample_count: examples.len() as u64,
        }
    }

    #[test]
    fn parquet_dataset_roundtrip() {
        let examples = vec![example("bbb", 200), example("aaa", 100)];
        let bytes = DatasetParquetCodec::encode(&examples, &manifest(&examples)).expect("encode");
        let mut decoded = DatasetParquetCodec::decode(&bytes).expect("decode");
        decoded.sort_by(|a, b| a.market_id.as_str().cmp(b.market_id.as_str()));
        let mut expected = examples;
        expected.sort_by(|a, b| a.market_id.as_str().cmp(b.market_id.as_str()));
        assert_eq!(
            decoded, expected,
            "roundtrip must preserve examples exactly"
        );
    }

    #[test]
    fn parquet_roundtrip_preserves_complete_nonzero_decision_boundary() {
        let mut row = example("boundary", 100);
        let decision_at = row.feature_vector.decision_at;
        row.decision_boundary = DecisionClock::new(10)
            .boundary(decision_at)
            .expect("global boundary")
            .with_source_cutoff(DecisionSource::Book, 30)
            .expect("book cutoff")
            .with_source_cutoff(DecisionSource::DomainCrypto, 60)
            .expect("domain cutoff");
        bind_capture_to_boundary(&mut row);
        let examples = vec![row];

        let bytes = DatasetParquetCodec::encode(&examples, &manifest(&examples)).expect("encode");
        let decoded = DatasetParquetCodec::decode(&bytes).expect("decode");

        assert_eq!(decoded, examples);
        assert_eq!(
            decoded[0]
                .decision_boundary
                .cutoff_for(DecisionSource::Book),
            decision_at - Duration::seconds(30)
        );
        assert_eq!(
            decoded[0]
                .decision_boundary
                .cutoff_for(DecisionSource::DomainCrypto),
            decision_at - Duration::seconds(60)
        );
    }

    #[test]
    fn legacy_parquet_without_format_version_is_rejected() {
        let example = example("legacy", 100);
        let payload = serde_json::to_string(&example).expect("payload");
        let mut frame =
            DataFrame::new(1, vec![Column::new("payload".into(), vec![payload])]).expect("frame");
        let mut bytes = Vec::new();
        ParquetWriter::new(&mut bytes)
            .finish(&mut frame)
            .expect("legacy parquet");
        assert!(DatasetParquetCodec::decode(&bytes).is_err());
    }

    #[test]
    fn dataset_artifact_version_is_four() {
        assert_eq!(DATASET_ARTIFACT_FORMAT_VERSION, 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn parquet_roundtrip_via_local_artifact_store() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let root = env::temp_dir().join(format!("quant-pivot-parquet-it-{nanos}"));
        let store = LocalArtifactStore::new(&root);
        let examples = vec![example("store-roundtrip", 123)];
        let bytes = DatasetParquetCodec::encode(&examples, &manifest(&examples)).expect("encode");
        let key =
            ArtifactKey::new(ArtifactNamespace::Dataset, "dataset-it", "parquet").expect("key");
        let uri = store.put(key, &bytes).await.expect("put");
        let loaded = store.get(&uri).await.expect("get");
        let decoded = DatasetParquetCodec::decode(&loaded).expect("decode");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].market_id.as_str(), "store-roundtrip");
    }
}
