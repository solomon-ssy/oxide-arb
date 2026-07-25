//! Deterministic Parquet envelope for immutable Source Slice objects.

#[cfg(feature = "research-jobs")]
use std::io::Cursor;

use chrono::{DateTime, Utc};
#[cfg(feature = "research-jobs")]
use polars::{
    error::PolarsError,
    prelude::{
        Column, DataFrame, Int64Chunked, IntoLazy, ParquetReader, ParquetWriter, SerReader,
        SortMultipleOptions, StringChunked, UInt32Chunked,
    },
};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
#[cfg(feature = "research-jobs")]
use quant_pivot_models::types::SOURCE_SLICE_MANIFEST_FORMAT_VERSION;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One lossless typed source fact stored as canonical JSON in a queryable
/// Parquet envelope. `record_key` must be the source-native immutable identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSliceRecord {
    pub record_key: String,
    pub event_at: Option<DateTime<Utc>>,
    pub available_at: Option<DateTime<Utc>>,
    pub payload: Value,
}

pub struct SourceSliceParquetCodec;

#[cfg(feature = "research-jobs")]
pub struct SourceSlicePolarsError(PolarsError);

#[cfg(feature = "research-jobs")]
impl From<PolarsError> for SourceSlicePolarsError {
    fn from(error: PolarsError) -> Self {
        Self(error)
    }
}

#[cfg(feature = "research-jobs")]
impl From<SourceSlicePolarsError> for ResearchError {
    fn from(error: SourceSlicePolarsError) -> Self {
        Self::ParquetCodec {
            detail: error.0.to_string(),
        }
    }
}

#[cfg(feature = "research-jobs")]
impl From<SourceSlicePolarsError> for QuantError {
    fn from(error: SourceSlicePolarsError) -> Self {
        ResearchError::from(error).into()
    }
}

impl SourceSliceParquetCodec {
    #[cfg(feature = "research-jobs")]
    fn validate_timestamp(
        record: &SourceSliceRecord,
        name: &str,
        value: Option<DateTime<Utc>>,
    ) -> QuantResult<()> {
        if value.is_some_and(|timestamp| timestamp.timestamp_subsec_nanos() % 1_000_000 != 0) {
            return Err(ResearchError::ParquetCodec {
                detail: format!(
                    "source-slice record {} {name} must be millisecond-aligned",
                    record.record_key
                ),
            }
            .into());
        }
        Ok(())
    }

    #[cfg(feature = "research-jobs")]
    pub fn encode(records: &[SourceSliceRecord]) -> QuantResult<Vec<u8>> {
        let mut ordered = records.to_vec();
        ordered.sort_by(|left, right| {
            (&left.record_key, left.event_at, left.available_at).cmp(&(
                &right.record_key,
                right.event_at,
                right.available_at,
            ))
        });
        for pair in ordered.windows(2) {
            if pair[0].record_key == pair[1].record_key {
                return Err(ResearchError::ParquetCodec {
                    detail: format!(
                        "duplicate source-slice record identity {}",
                        pair[0].record_key
                    ),
                }
                .into());
            }
        }
        for record in &ordered {
            Self::validate_timestamp(record, "event_at", record.event_at)?;
            Self::validate_timestamp(record, "available_at", record.available_at)?;
        }

        let format_versions = vec![SOURCE_SLICE_MANIFEST_FORMAT_VERSION; ordered.len()];
        let record_keys = ordered
            .iter()
            .map(|record| record.record_key.clone())
            .collect::<Vec<_>>();
        let event_at_ms = ordered
            .iter()
            .map(|record| record.event_at.map(|value| value.timestamp_millis()))
            .collect::<Vec<_>>();
        let available_at_ms = ordered
            .iter()
            .map(|record| record.available_at.map(|value| value.timestamp_millis()))
            .collect::<Vec<_>>();
        let payloads = ordered
            .iter()
            .map(|record| {
                serde_json::to_string(&record.payload).map_err(|error| {
                    ResearchError::ParquetCodec {
                        detail: format!("source-slice payload serialization failed: {error}"),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let frame = DataFrame::new(
            ordered.len(),
            vec![
                Column::new("format_version".into(), format_versions),
                Column::new("record_key".into(), record_keys),
                Column::new("event_at_ms".into(), event_at_ms),
                Column::new("available_at_ms".into(), available_at_ms),
                Column::new("payload".into(), payloads),
            ],
        )
        .map_err(SourceSlicePolarsError::from)?;
        let mut sorted = frame
            .lazy()
            .sort(
                ["record_key", "event_at_ms", "available_at_ms"],
                SortMultipleOptions::default(),
            )
            .collect()
            .map_err(SourceSlicePolarsError::from)?;
        let mut bytes = Vec::new();
        ParquetWriter::new(&mut bytes)
            .finish(&mut sorted)
            .map_err(SourceSlicePolarsError::from)?;
        Ok(bytes)
    }

    #[cfg(feature = "research-jobs")]
    pub fn decode(bytes: &[u8]) -> QuantResult<Vec<SourceSliceRecord>> {
        let frame = ParquetReader::new(Cursor::new(bytes))
            .finish()
            .map_err(SourceSlicePolarsError::from)?;
        let format_versions = u32_column(&frame, "format_version")?;
        let record_keys = string_column(&frame, "record_key")?;
        let event_at_ms = i64_column(&frame, "event_at_ms")?;
        let available_at_ms = i64_column(&frame, "available_at_ms")?;
        let payloads = string_column(&frame, "payload")?;
        let row_count = frame.height();
        let mut records = Vec::with_capacity(row_count);
        for index in 0..row_count {
            let version =
                format_versions
                    .get(index)
                    .ok_or_else(|| ResearchError::ParquetCodec {
                        detail: format!("source-slice row {index} has no format version"),
                    })?;
            if version != SOURCE_SLICE_MANIFEST_FORMAT_VERSION {
                return Err(ResearchError::ParquetCodec {
                    detail: format!("unsupported source-slice object version {version}"),
                }
                .into());
            }
            let record_key = required_string(record_keys, index, "record_key")?.to_owned();
            let payload = serde_json::from_str(required_string(payloads, index, "payload")?)
                .map_err(|error| ResearchError::ParquetCodec {
                    detail: format!("source-slice row {index} payload is invalid: {error}"),
                })?;
            records.push(SourceSliceRecord {
                record_key,
                event_at: timestamp(event_at_ms.get(index), index, "event_at_ms")?,
                available_at: timestamp(available_at_ms.get(index), index, "available_at_ms")?,
                payload,
            });
        }
        for pair in records.windows(2) {
            if pair[0].record_key >= pair[1].record_key {
                return Err(ResearchError::ParquetCodec {
                    detail: "source-slice records are not strictly identity-sorted".to_owned(),
                }
                .into());
            }
        }
        Ok(records)
    }

    #[cfg(not(feature = "research-jobs"))]
    pub fn encode(_records: &[SourceSliceRecord]) -> QuantResult<Vec<u8>> {
        Err(research_jobs_disabled())
    }

    #[cfg(not(feature = "research-jobs"))]
    pub fn decode(_bytes: &[u8]) -> QuantResult<Vec<SourceSliceRecord>> {
        Err(research_jobs_disabled())
    }
}

#[cfg(not(feature = "research-jobs"))]
fn research_jobs_disabled() -> QuantError {
    ResearchError::NotEligible {
        code: "research_jobs_feature_disabled",
        detail: "source-slice Parquet requires the compile-time `research-jobs` feature".to_owned(),
    }
    .into()
}

#[cfg(feature = "research-jobs")]
fn string_column<'a>(frame: &'a DataFrame, name: &str) -> QuantResult<&'a StringChunked> {
    frame
        .column(name)
        .map_err(SourceSlicePolarsError::from)?
        .str()
        .map_err(SourceSlicePolarsError::from)
        .map_err(Into::into)
}

#[cfg(feature = "research-jobs")]
fn i64_column<'a>(frame: &'a DataFrame, name: &str) -> QuantResult<&'a Int64Chunked> {
    frame
        .column(name)
        .map_err(SourceSlicePolarsError::from)?
        .i64()
        .map_err(SourceSlicePolarsError::from)
        .map_err(Into::into)
}

#[cfg(feature = "research-jobs")]
fn u32_column<'a>(frame: &'a DataFrame, name: &str) -> QuantResult<&'a UInt32Chunked> {
    frame
        .column(name)
        .map_err(SourceSlicePolarsError::from)?
        .u32()
        .map_err(SourceSlicePolarsError::from)
        .map_err(Into::into)
}

#[cfg(feature = "research-jobs")]
fn required_string<'a>(
    column: &'a StringChunked,
    index: usize,
    name: &str,
) -> QuantResult<&'a str> {
    column.get(index).ok_or_else(|| {
        ResearchError::ParquetCodec {
            detail: format!("source-slice row {index} has null {name}"),
        }
        .into()
    })
}

#[cfg(feature = "research-jobs")]
fn timestamp(millis: Option<i64>, index: usize, name: &str) -> QuantResult<Option<DateTime<Utc>>> {
    millis
        .map(|value| {
            DateTime::from_timestamp_millis(value).ok_or_else(|| {
                ResearchError::ParquetCodec {
                    detail: format!("source-slice row {index} has invalid {name}"),
                }
                .into()
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_error::{QuantError, research::ResearchError};

    use super::{SourceSliceParquetCodec, SourceSliceRecord};

    fn record(key: &str, second: u32) -> SourceSliceRecord {
        SourceSliceRecord {
            record_key: key.to_owned(),
            event_at: Some(
                Utc.with_ymd_and_hms(2026, 7, 14, 0, 0, second)
                    .single()
                    .expect("timestamp"),
            ),
            available_at: Some(
                Utc.with_ymd_and_hms(2026, 7, 14, 0, 1, second)
                    .single()
                    .expect("timestamp"),
            ),
            payload: serde_json::json!({ "key": key }),
        }
    }

    #[cfg(feature = "research-jobs")]
    #[test]
    fn source_slice_parquet_lossless() {
        let expected = vec![record("a", 1), record("b", 2)];
        let reversed = vec![expected[1].clone(), expected[0].clone()];
        let first = SourceSliceParquetCodec::encode(&expected).expect("encode");
        let second = SourceSliceParquetCodec::encode(&reversed).expect("encode reversed");
        assert_eq!(first, second);
        assert_eq!(
            SourceSliceParquetCodec::decode(&first).expect("decode"),
            expected
        );
    }

    #[cfg(feature = "research-jobs")]
    #[test]
    fn duplicate_source_identity_rejected() {
        let row = record("same", 1);
        assert!(SourceSliceParquetCodec::encode(&[row.clone(), row]).is_err());
    }

    #[cfg(feature = "research-jobs")]
    #[test]
    fn sub_millisecond_timestamp_rejected() {
        let mut row = record("sub-millisecond", 1);
        row.event_at = row.event_at.map(|value| value + Duration::microseconds(1));

        let error = SourceSliceParquetCodec::encode(&[row])
            .expect_err("sub-millisecond timestamp must fail closed");
        assert!(matches!(
            error,
            QuantError::Research(ResearchError::ParquetCodec { detail })
                if detail
                    == "source-slice record sub-millisecond event_at must be millisecond-aligned"
        ));

        let mut row = record("sub-millisecond", 1);
        row.available_at = row
            .available_at
            .map(|value| value + Duration::microseconds(1));
        let error = SourceSliceParquetCodec::encode(&[row])
            .expect_err("sub-millisecond timestamp must fail closed");
        assert!(matches!(
            error,
            QuantError::Research(ResearchError::ParquetCodec { detail })
                if detail
                    == "source-slice record sub-millisecond available_at must be millisecond-aligned"
        ));
    }
}
