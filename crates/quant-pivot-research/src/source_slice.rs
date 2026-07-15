//! Deterministic Parquet envelope for immutable Source Slice objects.

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
use quant_pivot_models::types::SOURCE_SLICE_MANIFEST_FORMAT_VERSION;
use serde::{Deserialize, Serialize};

/// One lossless typed source fact stored as canonical JSON in a queryable
/// Parquet envelope. `record_key` must be the source-native immutable identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSliceRecord {
    pub record_key: String,
    pub event_at: Option<DateTime<Utc>>,
    pub available_at: Option<DateTime<Utc>>,
    pub payload: serde_json::Value,
}

pub struct SourceSliceParquetCodec;

pub struct SourceSlicePolarsError(PolarsError);

impl From<PolarsError> for SourceSlicePolarsError {
    fn from(error: PolarsError) -> Self {
        Self(error)
    }
}

impl From<SourceSlicePolarsError> for ResearchError {
    fn from(error: SourceSlicePolarsError) -> Self {
        Self::ParquetCodec {
            detail: error.0.to_string(),
        }
    }
}

impl From<SourceSlicePolarsError> for QuantError {
    fn from(error: SourceSlicePolarsError) -> Self {
        ResearchError::from(error).into()
    }
}

impl SourceSliceParquetCodec {
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
}

fn string_column<'a>(frame: &'a DataFrame, name: &str) -> QuantResult<&'a StringChunked> {
    frame
        .column(name)
        .map_err(SourceSlicePolarsError::from)?
        .str()
        .map_err(SourceSlicePolarsError::from)
        .map_err(Into::into)
}

fn i64_column<'a>(frame: &'a DataFrame, name: &str) -> QuantResult<&'a Int64Chunked> {
    frame
        .column(name)
        .map_err(SourceSlicePolarsError::from)?
        .i64()
        .map_err(SourceSlicePolarsError::from)
        .map_err(Into::into)
}

fn u32_column<'a>(frame: &'a DataFrame, name: &str) -> QuantResult<&'a UInt32Chunked> {
    frame
        .column(name)
        .map_err(SourceSlicePolarsError::from)?
        .u32()
        .map_err(SourceSlicePolarsError::from)
        .map_err(Into::into)
}

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
    use chrono::{TimeZone, Utc};

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

    #[test]
    fn source_slice_parquet_is_deterministic_and_lossless() {
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

    #[test]
    fn duplicate_source_identity_is_rejected() {
        let row = record("same", 1);
        assert!(SourceSliceParquetCodec::encode(&[row.clone(), row]).is_err());
    }
}
