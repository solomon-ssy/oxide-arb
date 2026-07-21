//! Deterministic Parquet envelope for sealed trade-policy evidence rows.

use std::io::Cursor;

use chrono::{DateTime, Utc};
use polars::{
    error::PolarsError,
    prelude::{
        Column, DataFrame, Int64Chunked, IntoLazy, ParquetReader, ParquetWriter, SerReader,
        SortMultipleOptions, StringChunked, UInt32Chunked,
    },
};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    hashing::CanonicalDigest,
    types::{ContentHash, POLICY_EVIDENCE_OBJECT_FORMAT_VERSION},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

/// One canonical payload row with an independently verified semantic digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyEvidenceRecord {
    pub record_key: String,
    pub event_at: Option<DateTime<Utc>>,
    pub payload: Value,
    pub row_hash: ContentHash,
}

impl PolicyEvidenceRecord {
    pub fn from_typed<T: Serialize>(
        record_key: impl Into<String>,
        event_at: Option<DateTime<Utc>>,
        payload: &T,
    ) -> QuantResult<Self> {
        let record_key = record_key.into();
        let payload =
            serde_json::to_value(payload).map_err(|error| ResearchError::ParquetCodec {
                detail: format!("policy evidence payload serialization failed: {error}"),
            })?;
        let row_hash = row_hash(&record_key, event_at, &payload)?;
        Ok(Self {
            record_key,
            event_at,
            payload,
            row_hash,
        })
    }

    pub fn decode_typed<T: DeserializeOwned>(&self) -> QuantResult<T> {
        serde_json::from_value(self.payload.clone()).map_err(|error| {
            ResearchError::ParquetCodec {
                detail: format!(
                    "policy evidence record {} has an invalid typed payload: {error}",
                    self.record_key
                ),
            }
            .into()
        })
    }
}

pub struct PolicyEvidenceParquetCodec;

pub struct PolicyEvidencePolarsError(PolarsError);

impl From<PolarsError> for PolicyEvidencePolarsError {
    fn from(error: PolarsError) -> Self {
        Self(error)
    }
}

impl From<PolicyEvidencePolarsError> for ResearchError {
    fn from(error: PolicyEvidencePolarsError) -> Self {
        Self::ParquetCodec {
            detail: error.0.to_string(),
        }
    }
}

impl From<PolicyEvidencePolarsError> for QuantError {
    fn from(error: PolicyEvidencePolarsError) -> Self {
        ResearchError::from(error).into()
    }
}

impl PolicyEvidenceParquetCodec {
    pub fn encode(records: &[PolicyEvidenceRecord]) -> QuantResult<Vec<u8>> {
        let mut ordered = records.to_vec();
        ordered.sort_by(|left, right| left.record_key.cmp(&right.record_key));
        validate_records(&ordered)?;
        let format_versions = vec![POLICY_EVIDENCE_OBJECT_FORMAT_VERSION; ordered.len()];
        let record_keys = ordered
            .iter()
            .map(|record| record.record_key.clone())
            .collect::<Vec<_>>();
        let event_at_ms = ordered
            .iter()
            .map(|record| record.event_at.map(|value| value.timestamp_millis()))
            .collect::<Vec<_>>();
        let payloads = ordered
            .iter()
            .map(|record| {
                serde_json::to_string(&record.payload).map_err(|error| {
                    ResearchError::ParquetCodec {
                        detail: format!("policy evidence payload serialization failed: {error}"),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let row_hashes = ordered
            .iter()
            .map(|record| record.row_hash.as_str().to_owned())
            .collect::<Vec<_>>();
        let frame = DataFrame::new(
            ordered.len(),
            vec![
                Column::new("format_version".into(), format_versions),
                Column::new("record_key".into(), record_keys),
                Column::new("event_at_ms".into(), event_at_ms),
                Column::new("payload".into(), payloads),
                Column::new("row_hash".into(), row_hashes),
            ],
        )
        .map_err(PolicyEvidencePolarsError::from)?;
        let mut sorted = frame
            .lazy()
            .sort(["record_key"], SortMultipleOptions::default())
            .collect()
            .map_err(PolicyEvidencePolarsError::from)?;
        let mut bytes = Vec::new();
        ParquetWriter::new(&mut bytes)
            .finish(&mut sorted)
            .map_err(PolicyEvidencePolarsError::from)?;
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> QuantResult<Vec<PolicyEvidenceRecord>> {
        let frame = ParquetReader::new(Cursor::new(bytes))
            .finish()
            .map_err(PolicyEvidencePolarsError::from)?;
        let format_versions = u32_column(&frame, "format_version")?;
        let record_keys = string_column(&frame, "record_key")?;
        let event_at_ms = i64_column(&frame, "event_at_ms")?;
        let payloads = string_column(&frame, "payload")?;
        let row_hashes = string_column(&frame, "row_hash")?;
        let mut records = Vec::with_capacity(frame.height());
        for index in 0..frame.height() {
            let version =
                format_versions
                    .get(index)
                    .ok_or_else(|| ResearchError::ParquetCodec {
                        detail: format!("policy evidence row {index} has no format version"),
                    })?;
            if version != POLICY_EVIDENCE_OBJECT_FORMAT_VERSION {
                return Err(ResearchError::ParquetCodec {
                    detail: format!("unsupported policy evidence object version {version}"),
                }
                .into());
            }
            let record_key = required_string(record_keys, index, "record_key")?.to_owned();
            let event_at = timestamp(event_at_ms.get(index), index)?;
            let payload = serde_json::from_str(required_string(payloads, index, "payload")?)
                .map_err(|error| ResearchError::ParquetCodec {
                    detail: format!("policy evidence row {index} payload is invalid: {error}"),
                })?;
            let row_hash = ContentHash::parse(required_string(row_hashes, index, "row_hash")?)
                .map_err(|error| ResearchError::ParquetCodec {
                    detail: format!("policy evidence row {index} hash is invalid: {error}"),
                })?;
            records.push(PolicyEvidenceRecord {
                record_key,
                event_at,
                payload,
                row_hash,
            });
        }
        validate_records(&records)?;
        Ok(records)
    }

    pub fn row_chain_hash(records: &[PolicyEvidenceRecord]) -> QuantResult<ContentHash> {
        let mut chain = records
            .iter()
            .map(|record| (&record.record_key, &record.row_hash))
            .collect::<Vec<_>>();
        chain.sort_by(|left, right| left.0.cmp(right.0));
        CanonicalDigest::content_hash_json(&chain).map_err(Into::into)
    }
}

fn validate_records(records: &[PolicyEvidenceRecord]) -> QuantResult<()> {
    let mut prior = None;
    for record in records {
        if record.record_key.trim().is_empty()
            || prior.is_some_and(|prior: &str| prior >= record.record_key.as_str())
        {
            return Err(ResearchError::ParquetCodec {
                detail: "policy evidence record keys must be non-empty, unique, and sorted"
                    .to_owned(),
            }
            .into());
        }
        let expected = row_hash(&record.record_key, record.event_at, &record.payload)?;
        if expected != record.row_hash {
            return Err(ResearchError::ParquetCodec {
                detail: format!("policy evidence row {} hash mismatch", record.record_key),
            }
            .into());
        }
        prior = Some(record.record_key.as_str());
    }
    Ok(())
}

fn row_hash(
    record_key: &str,
    event_at: Option<DateTime<Utc>>,
    payload: &Value,
) -> QuantResult<ContentHash> {
    CanonicalDigest::content_hash_json(&(
        POLICY_EVIDENCE_OBJECT_FORMAT_VERSION,
        record_key,
        event_at,
        payload,
    ))
    .map_err(Into::into)
}

fn string_column<'a>(frame: &'a DataFrame, name: &str) -> QuantResult<&'a StringChunked> {
    frame
        .column(name)
        .map_err(PolicyEvidencePolarsError::from)?
        .str()
        .map_err(PolicyEvidencePolarsError::from)
        .map_err(Into::into)
}

fn i64_column<'a>(frame: &'a DataFrame, name: &str) -> QuantResult<&'a Int64Chunked> {
    frame
        .column(name)
        .map_err(PolicyEvidencePolarsError::from)?
        .i64()
        .map_err(PolicyEvidencePolarsError::from)
        .map_err(Into::into)
}

fn u32_column<'a>(frame: &'a DataFrame, name: &str) -> QuantResult<&'a UInt32Chunked> {
    frame
        .column(name)
        .map_err(PolicyEvidencePolarsError::from)?
        .u32()
        .map_err(PolicyEvidencePolarsError::from)
        .map_err(Into::into)
}

fn required_string<'a>(
    column: &'a StringChunked,
    index: usize,
    name: &str,
) -> QuantResult<&'a str> {
    column.get(index).ok_or_else(|| {
        ResearchError::ParquetCodec {
            detail: format!("policy evidence row {index} has null {name}"),
        }
        .into()
    })
}

fn timestamp(millis: Option<i64>, index: usize) -> QuantResult<Option<DateTime<Utc>>> {
    millis
        .map(|value| {
            DateTime::from_timestamp_millis(value).ok_or_else(|| {
                ResearchError::ParquetCodec {
                    detail: format!("policy evidence row {index} has invalid event_at_ms"),
                }
                .into()
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::types::ContentHash;
    use serde::{Deserialize, Serialize};

    use super::{PolicyEvidenceParquetCodec, PolicyEvidenceRecord};

    #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct Payload {
        value: u32,
    }

    #[test]
    fn round_trip_preserves_typed_rows_and_semantic_chain() {
        let at = Utc
            .with_ymd_and_hms(2026, 7, 15, 0, 0, 0)
            .single()
            .expect("time");
        let records = vec![
            PolicyEvidenceRecord::from_typed("b", Some(at), &Payload { value: 2 }).expect("record"),
            PolicyEvidenceRecord::from_typed("a", Some(at), &Payload { value: 1 }).expect("record"),
        ];
        let bytes = PolicyEvidenceParquetCodec::encode(&records).expect("encode");
        let decoded = PolicyEvidenceParquetCodec::decode(&bytes).expect("decode");

        assert_eq!(decoded[0].record_key, "a");
        assert_eq!(
            decoded[0].decode_typed::<Payload>().expect("payload"),
            Payload { value: 1 }
        );
        assert_eq!(
            PolicyEvidenceParquetCodec::row_chain_hash(&records).expect("source chain"),
            PolicyEvidenceParquetCodec::row_chain_hash(&decoded).expect("decoded chain")
        );
    }

    #[test]
    fn tampered_semantic_row_is_rejected() {
        let mut record =
            PolicyEvidenceRecord::from_typed("a", None, &Payload { value: 1 }).expect("record");
        record.payload = serde_json::json!({"value": 2});
        assert!(PolicyEvidenceParquetCodec::encode(&[record]).is_err());
    }

    #[test]
    fn duplicate_keys_are_rejected() {
        let record =
            PolicyEvidenceRecord::from_typed("a", None, &Payload { value: 1 }).expect("record");
        assert!(PolicyEvidenceParquetCodec::encode(&[record.clone(), record]).is_err());
    }

    #[test]
    fn invalid_hash_text_never_enters_a_record() {
        assert!(ContentHash::parse("not-a-hash").is_err());
    }
}
