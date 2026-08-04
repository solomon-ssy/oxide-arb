//! `ClickHouse` feature-parity evidence reader.

use std::{collections::BTreeMap, str::FromStr, sync::Arc};

use chrono::{DateTime, Utc};
use clickhouse::query::Query;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        QuantFeatureEventRow, QuantFeatureParityEventRow, QuantModelInputEventRow,
        QuantServingEvidenceCompletionRow,
    },
    domain::{
        api::{
            FeatureIntegrityCounts, FeatureParityEventListQuery, FeatureParityEventView,
            FeatureParityEvidenceView,
        },
        pagination::{PageWindow, Paginated},
    },
    enums::quant::{FeatureCellState, FeatureParityEventStatus, FeatureParityStage},
    types::{FeatureParityDetail, FeatureVectorId, ModelRunId},
};
use quant_pivot_storage::clickhouse::ClickHousePool;

use crate::{
    clickhouse::{
        query_batch::{UUID_INLINE_BYTES, canonical_by, extend_rows, query_chunks},
        query_limits::{
            FEATURE_CELLS_FOR_VECTORS, FEATURE_PARITY_PAGE, FEATURE_PARITY_SUMMARY,
            MODEL_INPUTS_FOR_RUNS, SERVING_COMPLETIONS_FOR_RUNS,
        },
    },
    traits::{FeatureParityEventRepository, ServingEvidenceRepository},
};

/// ClickHouse-backed row-level parity evidence repository.
pub struct ChFeatureParityEventRepository {
    pool: Arc<ClickHousePool>,
}

impl ChFeatureParityEventRepository {
    #[must_use]
    pub const fn new(pool: Arc<ClickHousePool>) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl FeatureParityEventRepository for ChFeatureParityEventRepository {
    async fn page_events(
        &self,
        query: FeatureParityEventListQuery,
    ) -> Result<Paginated<FeatureParityEventView>, StorageError> {
        let window = PageWindow::from_query(&query);
        let filters = EventFilters::from_query(&query);
        let count = bind_filters(
            FEATURE_PARITY_PAGE.query(
                self.pool.client(),
                "SELECT count() FROM quant_feature_parity_event FINAL \
                 WHERE (? = '' OR parity_run_id = ?) \
                 AND (? = '' OR status = ?) \
                 AND (? = '' OR stage = ?) \
                 AND (? = '' OR feature_name = ?) \
                 AND (? = '' OR reason = ?) \
                 AND (? = '' OR report_id = ?) \
                 AND (? = '' OR model_version_id = ?) \
                 AND (? = '' OR training_dataset_id = ?) \
                 AND event_time >= fromUnixTimestamp64Milli(?) \
                 AND event_time < fromUnixTimestamp64Milli(?)",
            ),
            &filters,
        )
        .fetch_one::<u64>()
        .await?;

        let rows = bind_filters(
            FEATURE_PARITY_PAGE.query(
                self.pool.client(),
                "SELECT ?fields FROM quant_feature_parity_event FINAL \
                 WHERE (? = '' OR parity_run_id = ?) \
                 AND (? = '' OR status = ?) \
                 AND (? = '' OR stage = ?) \
                 AND (? = '' OR feature_name = ?) \
                 AND (? = '' OR reason = ?) \
                 AND (? = '' OR report_id = ?) \
                 AND (? = '' OR model_version_id = ?) \
                 AND (? = '' OR training_dataset_id = ?) \
                 AND event_time >= fromUnixTimestamp64Milli(?) \
                 AND event_time < fromUnixTimestamp64Milli(?) \
                 ORDER BY event_time DESC, parity_event_id DESC \
                 LIMIT ? OFFSET ?",
            ),
            &filters,
        )
        .bind(window.size())
        .bind(window.offset())
        .fetch_all::<QuantFeatureParityEventRow>()
        .await?;
        let items = rows
            .into_iter()
            .map(Self::decode_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Paginated::from_window(items, count, window))
    }

    async fn summary_counts(&self) -> Result<FeatureIntegrityCounts, StorageError> {
        #[derive(Debug, clickhouse::Row, serde::Deserialize)]
        struct CountRow {
            key: String,
            count: u64,
        }

        let states = FEATURE_PARITY_SUMMARY
            .query(
                self.pool.client(),
                "SELECT toString(cell_state) AS key, count() AS count \
                 FROM quant_feature_event \
                 WHERE event_time >= now64(3) - INTERVAL 24 HOUR \
                 GROUP BY key ORDER BY key",
            )
            .fetch_all::<CountRow>()
            .await?;
        let reasons = FEATURE_PARITY_SUMMARY
            .query(
                self.pool.client(),
                "SELECT assumeNotNull(reason) AS key, count() AS count \
                 FROM quant_feature_event \
                 WHERE event_time >= now64(3) - INTERVAL 24 HOUR \
                 AND data_quality = 'insufficient' \
                 AND reason IS NOT NULL AND reason != '' \
                 GROUP BY key ORDER BY key",
            )
            .fetch_all::<CountRow>()
            .await?;
        let feature_state_counts = states
            .into_iter()
            .map(|row| {
                FeatureCellState::from_str(&row.key)
                    .map(|state| (state, row.count))
                    .map_err(|error| {
                        StorageError::Codec(format!(
                            "invalid feature state `{}` in feature facts: {error}",
                            row.key
                        ))
                    })
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let rejection_reason_counts = reasons
            .into_iter()
            .map(|row| (row.key, row.count))
            .collect();
        Ok(FeatureIntegrityCounts {
            feature_state_counts,
            rejection_reason_counts,
        })
    }
}

#[async_trait::async_trait]
impl ServingEvidenceRepository for ChFeatureParityEventRepository {
    async fn completions_for_runs(
        &self,
        model_run_ids: &[ModelRunId],
    ) -> Result<Vec<QuantServingEvidenceCompletionRow>, StorageError> {
        let model_run_ids = canonical_by(model_run_ids.to_vec(), |id| id.as_uuid());
        if model_run_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for run_ids in query_chunks(
            &model_run_ids,
            |_| UUID_INLINE_BYTES,
            "quant_serving_evidence_completion",
        )? {
            let page = SERVING_COMPLETIONS_FOR_RUNS
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM quant_serving_evidence_completion \
                     WHERE model_run_id IN ? \
                     ORDER BY model_run_id, ingestion_time",
                )
                .bind(run_ids.to_vec())
                .fetch_all::<QuantServingEvidenceCompletionRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                SERVING_COMPLETIONS_FOR_RUNS,
                "quant_serving_evidence_completion",
            )?;
        }
        Ok(rows)
    }

    async fn model_inputs_for_runs(
        &self,
        model_run_ids: &[ModelRunId],
    ) -> Result<Vec<QuantModelInputEventRow>, StorageError> {
        let model_run_ids = canonical_by(model_run_ids.to_vec(), |id| id.as_uuid());
        if model_run_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for run_ids in query_chunks(
            &model_run_ids,
            |_| UUID_INLINE_BYTES,
            "quant_model_input_event",
        )? {
            let page = MODEL_INPUTS_FOR_RUNS
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM quant_model_input_event \
                     WHERE model_run_id IN ? \
                     ORDER BY model_run_id, market_id, encoded_column, raw_input_name, ingestion_time",
                )
                .bind(run_ids.to_vec())
                .fetch_all::<QuantModelInputEventRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                MODEL_INPUTS_FOR_RUNS,
                "quant_model_input_event",
            )?;
        }
        Ok(rows)
    }

    async fn feature_cells_for_vectors(
        &self,
        feature_vector_ids: &[FeatureVectorId],
    ) -> Result<Vec<QuantFeatureEventRow>, StorageError> {
        let feature_vector_ids = canonical_by(feature_vector_ids.to_vec(), |id| id.as_uuid());
        if feature_vector_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for vector_ids in query_chunks(
            &feature_vector_ids,
            |_| UUID_INLINE_BYTES,
            "quant_feature_event",
        )? {
            let page = FEATURE_CELLS_FOR_VECTORS
                .query(
                    self.pool.client(),
                    "SELECT ?fields FROM quant_feature_event \
                     WHERE feature_vector_id IN ? \
                     ORDER BY feature_vector_id, feature_name, ingestion_time",
                )
                .bind(vector_ids.to_vec())
                .fetch_all::<QuantFeatureEventRow>()
                .await?;
            extend_rows(
                &mut rows,
                page,
                FEATURE_CELLS_FOR_VECTORS,
                "quant_feature_event",
            )?;
        }
        Ok(rows)
    }
}

struct EventFilters {
    parity_run_id: String,
    status: String,
    stage: String,
    feature_name: String,
    reason: String,
    report_id: String,
    model_version_id: String,
    training_dataset_id: String,
    from_ms: i64,
    to_ms: i64,
}

impl EventFilters {
    fn from_query(query: &FeatureParityEventListQuery) -> Self {
        Self {
            parity_run_id: query
                .parity_run_id
                .as_ref()
                .map_or_else(String::new, ToString::to_string),
            status: query
                .status
                .map_or_else(String::new, |status| status.to_string()),
            stage: query
                .stage
                .map_or_else(String::new, |stage| stage.to_string()),
            feature_name: query.feature_name.clone().unwrap_or_default(),
            reason: query.reason.clone().unwrap_or_default(),
            report_id: query
                .report_id
                .as_ref()
                .map_or_else(String::new, ToString::to_string),
            model_version_id: query
                .model_version_id
                .as_ref()
                .map_or_else(String::new, ToString::to_string),
            training_dataset_id: query
                .training_dataset_id
                .as_ref()
                .map_or_else(String::new, ToString::to_string),
            from_ms: query.from.map_or(0, |value| value.timestamp_millis()),
            to_ms: query
                .to
                .unwrap_or_else(Utc::now)
                .timestamp_millis()
                .saturating_add(1),
        }
    }
}

fn bind_filters(query: Query, filters: &EventFilters) -> Query {
    query
        .bind(&filters.parity_run_id)
        .bind(&filters.parity_run_id)
        .bind(&filters.status)
        .bind(&filters.status)
        .bind(&filters.stage)
        .bind(&filters.stage)
        .bind(&filters.feature_name)
        .bind(&filters.feature_name)
        .bind(&filters.reason)
        .bind(&filters.reason)
        .bind(&filters.report_id)
        .bind(&filters.report_id)
        .bind(&filters.model_version_id)
        .bind(&filters.model_version_id)
        .bind(&filters.training_dataset_id)
        .bind(&filters.training_dataset_id)
        .bind(filters.from_ms)
        .bind(filters.to_ms)
}

impl ChFeatureParityEventRepository {
    fn decode_row(row: QuantFeatureParityEventRow) -> Result<FeatureParityEventView, StorageError> {
        let status = FeatureParityEventStatus::from_str(&row.status).map_err(|error| {
            StorageError::Codec(format!(
                "invalid parity event status `{}`: {error}",
                row.status
            ))
        })?;
        if status != FeatureParityEventStatus::PendingMaterialization
            && (row.online_fingerprint.is_empty() || row.replay_fingerprint.is_empty())
        {
            return Err(StorageError::Codec(format!(
                "parity event {} has an empty audit fingerprint",
                row.parity_event_id
            )));
        }
        let feature_contract_hash = row.feature_contract_hash.parse().map_err(|error| {
            StorageError::Codec(format!(
                "invalid feature contract hash `{}` in parity event {}: {error}",
                row.feature_contract_hash, row.parity_event_id
            ))
        })?;
        let transform_hash = (!row.transform_hash.is_empty())
            .then(|| row.transform_hash.parse())
            .transpose()
            .map_err(|error| {
                StorageError::Codec(format!(
                    "invalid transform hash `{}` in parity event {}: {error}",
                    row.transform_hash, row.parity_event_id
                ))
            })?;
        let detail =
            serde_json::from_str::<FeatureParityDetail>(&row.detail_json).map_err(|error| {
                StorageError::Codec(format!(
                    "invalid detail_json in parity event {}: {error}",
                    row.parity_event_id
                ))
            })?;
        let stage = FeatureParityStage::from_str(&row.stage).map_err(|error| {
            StorageError::Codec(format!("invalid parity stage `{}`: {error}", row.stage))
        })?;
        detail.validate_for(stage, status).map_err(|detail| {
            StorageError::Codec(format!(
                "invalid typed detail in parity event {}: {detail}",
                row.parity_event_id
            ))
        })?;
        Ok(FeatureParityEventView {
            parity_event_id: row.parity_event_id,
            parity_run_id: row.parity_run_id,
            status,
            stage,
            decision_at: required_time(row.decision_at, "decision_at")?,
            report_id: row.report_id,
            model_run_id: row.model_run_id,
            model_version_id: row.model_version_id,
            training_dataset_id: row.training_dataset_id,
            market_id: row.market_id,
            feature_name: row.feature_name,
            reason: row.reason,
            feature_contract_hash,
            transform_hash,
            online: FeatureParityEvidenceView {
                state: parse_state(row.online_state.as_deref())?,
                value: row.online_value,
                effective_at: optional_time(row.online_effective_at, "online_effective_at")?,
                available_at: optional_time(row.online_available_at, "online_available_at")?,
                cutoff: optional_time(row.online_cutoff, "online_cutoff")?,
                fingerprint: row.online_fingerprint,
            },
            replay: FeatureParityEvidenceView {
                state: parse_state(row.replay_state.as_deref())?,
                value: row.replay_value,
                effective_at: optional_time(row.replay_effective_at, "replay_effective_at")?,
                available_at: optional_time(row.replay_available_at, "replay_available_at")?,
                cutoff: optional_time(row.replay_cutoff, "replay_cutoff")?,
                fingerprint: row.replay_fingerprint,
            },
            detail,
            created_at: required_time(row.ingestion_time, "ingestion_time")?,
        })
    }
}

fn parse_state(raw: Option<&str>) -> Result<Option<FeatureCellState>, StorageError> {
    raw.map(FeatureCellState::from_str)
        .transpose()
        .map_err(|error| StorageError::Codec(format!("invalid feature state: {error}")))
}

fn optional_time(
    millis: Option<i64>,
    field: &'static str,
) -> Result<Option<DateTime<Utc>>, StorageError> {
    millis.map(|value| required_time(value, field)).transpose()
}

fn required_time(millis: i64, field: &'static str) -> Result<DateTime<Utc>, StorageError> {
    DateTime::from_timestamp_millis(millis)
        .ok_or_else(|| StorageError::Codec(format!("invalid {field} epoch milliseconds: {millis}")))
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        clickhouse::QuantFeatureParityEventRow,
        enums::quant::FeatureParityStage,
        types::{
            FeatureParityDetail, FeatureParityDetailSource, FeatureParityEventId,
            FeatureParityRunId,
        },
    };

    use super::ChFeatureParityEventRepository;

    const HASH: &str = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn row() -> QuantFeatureParityEventRow {
        QuantFeatureParityEventRow {
            event_time: 1_750_000_000_000,
            parity_event_id: FeatureParityEventId::from_v7(),
            parity_run_id: FeatureParityRunId::from_v7(),
            decision_at: 1_750_000_000_000,
            stage: "model_input".to_owned(),
            status: "matched".to_owned(),
            report_id: None,
            model_run_id: None,
            model_version_id: None,
            training_dataset_id: None,
            market_id: None,
            feature_name: Some("spread_bps".to_owned()),
            reason: None,
            online_state: Some("observed".to_owned()),
            replay_state: Some("observed".to_owned()),
            online_value: Some("12.5".to_owned()),
            replay_value: Some("12.5".to_owned()),
            online_effective_at: Some(1_750_000_000_000),
            online_available_at: Some(1_750_000_000_000),
            online_cutoff: Some(1_750_000_000_000),
            replay_effective_at: Some(1_750_000_000_000),
            replay_available_at: Some(1_750_000_000_000),
            replay_cutoff: Some(1_750_000_000_000),
            feature_contract_hash: HASH.to_owned(),
            transform_hash: HASH.to_owned(),
            online_fingerprint: HASH.to_owned(),
            replay_fingerprint: HASH.to_owned(),
            detail_json: r#"{"kind":"compared","sampling_key":"report:market","source":{"kind":"model_input","raw_input_name":"spread_bps","feature_vector_id":"01900000-0000-7000-8000-000000000001"}}"#.to_owned(),
            ingestion_time: 1_750_000_000_001,
        }
    }

    #[test]
    fn parity_event_preserves_detail() {
        let view =
            ChFeatureParityEventRepository::decode_row(row()).expect("valid parity event view");
        assert_eq!(view.feature_contract_hash.to_string(), HASH);
        assert_eq!(
            view.transform_hash.map(|hash| hash.to_string()).as_deref(),
            Some(HASH)
        );
        let FeatureParityDetail::Compared {
            sampling_key,
            source,
        } = view.detail
        else {
            panic!("expected typed model-input detail");
        };
        let FeatureParityDetailSource::ModelInput { raw_input_name, .. } = source.as_ref() else {
            panic!("expected typed model-input source");
        };
        assert_eq!(sampling_key, "report:market");
        assert_eq!(raw_input_name.as_str(), "spread_bps");
    }

    #[test]
    fn parity_rejects_malformed_payloads() {
        let mut malformed_hash = row();
        malformed_hash.feature_contract_hash = "not-a-hash".to_owned();
        assert!(ChFeatureParityEventRepository::decode_row(malformed_hash).is_err());

        let mut malformed_detail = row();
        malformed_detail.detail_json = "{".to_owned();
        assert!(ChFeatureParityEventRepository::decode_row(malformed_detail).is_err());
    }

    #[test]
    fn parity_event_decodes_stages() {
        for (wire, expected, detail_json) in [
            (
                "capture",
                FeatureParityStage::Capture,
                format!(
                    r#"{{"kind":"compared","sampling_key":"report:market","source":{{"kind":"capture","feature_vector_id":"01900000-0000-7000-8000-000000000001","online_capture_hash":"{HASH}","replay_capture_hash":"{HASH}"}}}}"#,
                ),
            ),
            (
                "data_quality",
                FeatureParityStage::DataQuality,
                r#"{"kind":"compared","sampling_key":"report:market","source":{"kind":"data_quality","online_count":1,"replay_count":1,"online_admitted_count":1,"replay_admitted_count":1}}"#.to_owned(),
            ),
        ] {
            let mut event = row();
            event.stage = wire.to_owned();
            event.detail_json = detail_json;
            assert_eq!(
                ChFeatureParityEventRepository::decode_row(event)
                    .expect("valid parity event")
                    .stage,
                expected
            );
        }
    }
}
