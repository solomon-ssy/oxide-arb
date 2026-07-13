//! Canonical commitments for durable training-serving evidence.
//!
//! Commitments deliberately exclude transport-only `ingestion_time`. Retrying
//! an identical `ClickHouse` insert therefore produces the same content hash,
//! while any conflicting retry is detectable by replay.

use std::collections::{BTreeSet, HashMap, HashSet};

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    clickhouse::{
        QuantFeatureEventRow, QuantModelInputEventRow, QuantServingEvidenceCompletionRow,
    },
    domain::DecisionBoundary,
    types::{ContentHash, FeatureVectorId, MarketId, ModelRunId},
};
use quant_pivot_research::hashing::ResearchHasher;
use serde::Serialize;

pub const SERVING_EVIDENCE_FORMAT_VERSION: u32 = 2;

/// Producer-side commitment for the complete feature evidence batch used by a
/// later model run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureEvidenceCommitment {
    decision_at: i64,
    knowledge_cutoff: i64,
    feature_vector_ids: Vec<FeatureVectorId>,
    vector_markets: HashMap<FeatureVectorId, MarketId>,
    model_vector_markets: HashMap<FeatureVectorId, MarketId>,
    expected_row_count: u64,
    rows_hash: ContentHash,
}

impl FeatureEvidenceCommitment {
    #[must_use]
    pub const fn decision_at(&self) -> i64 {
        self.decision_at
    }

    #[must_use]
    pub fn feature_vector_ids(&self) -> &[FeatureVectorId] {
        &self.feature_vector_ids
    }

    #[must_use]
    pub const fn expected_row_count(&self) -> u64 {
        self.expected_row_count
    }

    #[must_use]
    pub const fn rows_hash(&self) -> &ContentHash {
        &self.rows_hash
    }

    /// Bind the all-vector feature commitment to the subset that was admitted
    /// into model input. Rejected vectors remain committed as serving evidence
    /// but are not required to appear in the encoded-input rows.
    pub fn bind_model_vectors(mut self, admitted: &[FeatureVectorId]) -> QuantResult<Self> {
        let mut seen = HashSet::with_capacity(admitted.len());
        let mut model_vector_markets = HashMap::with_capacity(admitted.len());
        for vector_id in admitted {
            if !seen.insert(vector_id.clone()) {
                return Err(determinism(format!(
                    "model-admitted feature vector list contains duplicate {vector_id}"
                )));
            }
            let market_id = self.vector_markets.get(vector_id).cloned().ok_or_else(|| {
                determinism(format!(
                    "model-admitted feature vector {vector_id} is absent from the serving feature commitment"
                ))
            })?;
            model_vector_markets.insert(vector_id.clone(), market_id);
        }
        self.model_vector_markets = model_vector_markets;
        Ok(self)
    }
}

#[derive(Serialize)]
struct EvidenceEntry<'a> {
    key: String,
    audit_fingerprint: &'a str,
}

#[derive(Serialize)]
struct EvidenceBatch<'a> {
    entries: &'a [EvidenceEntry<'a>],
}

#[derive(Serialize)]
struct CompletionDigest<'a> {
    format_version: u32,
    model_run_id: &'a ModelRunId,
    decision_at: i64,
    knowledge_cutoff: i64,
    feature_vector_ids: &'a [FeatureVectorId],
    expected_feature_row_count: u64,
    feature_rows_hash: &'a ContentHash,
    expected_model_input_row_count: u64,
    model_input_rows_hash: &'a ContentHash,
}

/// Validate and hash one complete feature-cell batch before persistence.
pub fn feature_commitment(rows: &[QuantFeatureEventRow]) -> QuantResult<FeatureEvidenceCommitment> {
    let first = rows
        .first()
        .ok_or_else(|| determinism("feature evidence batch is empty"))?;
    let mut keys = BTreeSet::new();
    let mut vector_ids = HashSet::new();
    let mut vector_markets = HashMap::new();
    let mut vector_capture_hashes = HashMap::new();
    let mut entries = Vec::with_capacity(rows.len());
    for row in rows {
        if row.decision_at != first.decision_at
            || row.knowledge_cutoff != first.knowledge_cutoff
            || row.event_time != first.event_time
            || row.runtime_config_version_id != first.runtime_config_version_id
            || row.per_source_cutoffs_json != first.per_source_cutoffs_json
            || row.feature_schema_version != first.feature_schema_version
            || row.feature_schema_hash != first.feature_schema_hash
        {
            return Err(determinism(
                "feature evidence batch spans multiple decision boundaries",
            ));
        }
        if row.decision_capture_hash.is_empty() {
            return Err(determinism(format!(
                "feature vector {} has no durable decision-capture commitment",
                row.feature_vector_id
            )));
        }
        if row.audit_fingerprint.is_empty() {
            return Err(determinism(format!(
                "feature evidence {}/{} has an empty audit fingerprint",
                row.feature_vector_id, row.feature_name
            )));
        }
        let key = format!("{}/{}", row.feature_vector_id, row.feature_name);
        if !keys.insert(key.clone()) {
            return Err(determinism(format!(
                "feature evidence batch contains duplicate key {key}"
            )));
        }
        vector_ids.insert(row.feature_vector_id.clone());
        match vector_markets.insert(row.feature_vector_id.clone(), row.market_id.clone()) {
            Some(previous) if previous != row.market_id => {
                return Err(determinism(format!(
                    "feature vector {} is bound to multiple markets",
                    row.feature_vector_id
                )));
            }
            _ => {}
        }
        match vector_capture_hashes.insert(
            row.feature_vector_id.clone(),
            row.decision_capture_hash.as_str(),
        ) {
            Some(previous) if previous != row.decision_capture_hash => {
                return Err(determinism(format!(
                    "feature vector {} is bound to multiple decision captures",
                    row.feature_vector_id
                )));
            }
            _ => {}
        }
        entries.push(EvidenceEntry {
            key,
            audit_fingerprint: &row.audit_fingerprint,
        });
    }
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    let expected_row_count = u64::try_from(rows.len()).map_err(|error| {
        determinism(format!(
            "feature evidence row count does not fit u64: {error}"
        ))
    })?;
    let mut feature_vector_ids = vector_ids.into_iter().collect::<Vec<_>>();
    feature_vector_ids.sort_by_key(ToString::to_string);
    Ok(FeatureEvidenceCommitment {
        decision_at: first.decision_at,
        knowledge_cutoff: first.knowledge_cutoff,
        feature_vector_ids,
        model_vector_markets: vector_markets.clone(),
        vector_markets,
        expected_row_count,
        rows_hash: ResearchHasher::canonical(&EvidenceBatch { entries: &entries })?,
    })
}

/// Validate model-input evidence against the acknowledged feature commitment
/// and build the run-scoped completion marker written after the input rows.
pub fn completion_marker(
    model_run_id: &ModelRunId,
    boundary: &DecisionBoundary,
    features: &FeatureEvidenceCommitment,
    model_inputs: &[QuantModelInputEventRow],
    ingestion_time: i64,
) -> QuantResult<QuantServingEvidenceCompletionRow> {
    boundary.validate()?;
    let decision_at = boundary.decision_at().timestamp_millis();
    let knowledge_cutoff = boundary.knowledge_cutoff().timestamp_millis();
    if features.decision_at != decision_at || features.knowledge_cutoff != knowledge_cutoff {
        return Err(determinism(format!(
            "feature evidence boundary ({}, {}) does not match model run boundary ({decision_at}, {knowledge_cutoff})",
            features.decision_at, features.knowledge_cutoff
        )));
    }
    let (vector_markets, expected_model_input_row_count, model_input_rows_hash) =
        model_input_commitment(model_run_id, decision_at, knowledge_cutoff, model_inputs)?;
    if vector_markets != features.model_vector_markets {
        return Err(determinism(format!(
            "model-input vector binding for run {model_run_id} does not match the admitted serving-feature subset"
        )));
    }
    let completion_hash = ResearchHasher::canonical(&CompletionDigest {
        format_version: SERVING_EVIDENCE_FORMAT_VERSION,
        model_run_id,
        decision_at,
        knowledge_cutoff,
        feature_vector_ids: &features.feature_vector_ids,
        expected_feature_row_count: features.expected_row_count,
        feature_rows_hash: &features.rows_hash,
        expected_model_input_row_count,
        model_input_rows_hash: &model_input_rows_hash,
    })?;
    let feature_vector_ids_json =
        serde_json::to_string(&features.feature_vector_ids).map_err(|error| {
            ResearchError::Serialization {
                detail: format!("serialize serving evidence vector ids: {error}"),
            }
        })?;

    Ok(QuantServingEvidenceCompletionRow {
        event_time: decision_at,
        format_version: SERVING_EVIDENCE_FORMAT_VERSION,
        model_run_id: model_run_id.clone(),
        decision_at,
        knowledge_cutoff,
        feature_vector_ids_json,
        expected_feature_row_count: features.expected_row_count,
        feature_rows_hash: features.rows_hash.as_str().to_owned(),
        expected_model_input_row_count,
        model_input_rows_hash: model_input_rows_hash.as_str().to_owned(),
        completion_hash: completion_hash.as_str().to_owned(),
        ingestion_time,
    })
}

/// Verify that a run-scoped completion marker describes exactly the durable
/// feature and model-input rows returned by `ClickHouse`.
pub fn verify_completion(
    marker: &QuantServingEvidenceCompletionRow,
    feature_rows: &[QuantFeatureEventRow],
    model_input_rows: &[QuantModelInputEventRow],
) -> QuantResult<Vec<FeatureVectorId>> {
    if marker.format_version != SERVING_EVIDENCE_FORMAT_VERSION {
        return Err(determinism(format!(
            "serving evidence completion for run {} uses unsupported format {}, expected {}",
            marker.model_run_id, marker.format_version, SERVING_EVIDENCE_FORMAT_VERSION
        )));
    }
    let features = feature_commitment(feature_rows)?;
    let marker_vector_ids = serde_json::from_str::<Vec<FeatureVectorId>>(
        &marker.feature_vector_ids_json,
    )
    .map_err(|error| ResearchError::Serialization {
        detail: format!(
            "deserialize serving evidence vector ids for run {}: {error}",
            marker.model_run_id
        ),
    })?;
    let marker_vector_set = marker_vector_ids.iter().cloned().collect::<HashSet<_>>();
    let mut canonical_vector_ids = marker_vector_set.iter().cloned().collect::<Vec<_>>();
    canonical_vector_ids.sort_by_key(ToString::to_string);
    if marker_vector_ids.len() != marker_vector_set.len()
        || marker_vector_ids != canonical_vector_ids
    {
        return Err(determinism(format!(
            "serving evidence completion {} has non-canonical feature vector ids",
            marker.model_run_id
        )));
    }
    if features.decision_at != marker.decision_at
        || features.knowledge_cutoff != marker.knowledge_cutoff
        || features.feature_vector_ids != marker_vector_ids
        || features.expected_row_count != marker.expected_feature_row_count
        || features.rows_hash.as_str() != marker.feature_rows_hash
    {
        return Err(determinism(format!(
            "durable feature evidence does not match completion marker for run {}",
            marker.model_run_id
        )));
    }
    let (input_vectors, input_count, input_hash) = model_input_commitment(
        &marker.model_run_id,
        marker.decision_at,
        marker.knowledge_cutoff,
        model_input_rows,
    )?;
    if input_vectors
        .iter()
        .any(|(vector_id, market_id)| features.vector_markets.get(vector_id) != Some(market_id))
        || input_count != marker.expected_model_input_row_count
        || input_hash.as_str() != marker.model_input_rows_hash
    {
        return Err(determinism(format!(
            "durable model-input evidence does not match completion marker for run {}",
            marker.model_run_id
        )));
    }
    let feature_hash = marker
        .feature_rows_hash
        .parse::<ContentHash>()
        .map_err(|error| {
            determinism(format!(
                "invalid feature evidence hash in completion {}: {error}",
                marker.model_run_id
            ))
        })?;
    let input_hash = marker
        .model_input_rows_hash
        .parse::<ContentHash>()
        .map_err(|error| {
            determinism(format!(
                "invalid model-input evidence hash in completion {}: {error}",
                marker.model_run_id
            ))
        })?;
    let expected_completion = ResearchHasher::canonical(&CompletionDigest {
        format_version: marker.format_version,
        model_run_id: &marker.model_run_id,
        decision_at: marker.decision_at,
        knowledge_cutoff: marker.knowledge_cutoff,
        feature_vector_ids: &marker_vector_ids,
        expected_feature_row_count: marker.expected_feature_row_count,
        feature_rows_hash: &feature_hash,
        expected_model_input_row_count: marker.expected_model_input_row_count,
        model_input_rows_hash: &input_hash,
    })?;
    if expected_completion.as_str() != marker.completion_hash {
        return Err(determinism(format!(
            "completion hash mismatch for serving run {}",
            marker.model_run_id
        )));
    }
    Ok(marker_vector_ids)
}

fn model_input_commitment(
    model_run_id: &ModelRunId,
    decision_at: i64,
    knowledge_cutoff: i64,
    rows: &[QuantModelInputEventRow],
) -> QuantResult<(HashMap<FeatureVectorId, MarketId>, u64, ContentHash)> {
    rows.first().ok_or_else(|| {
        determinism(format!(
            "model run {model_run_id} emitted no input evidence"
        ))
    })?;
    let mut keys = BTreeSet::new();
    let mut vector_markets = HashMap::new();
    let mut market_vectors = HashMap::new();
    let mut entries = Vec::with_capacity(rows.len());
    for row in rows {
        if &row.model_run_id != model_run_id {
            return Err(determinism(format!(
                "model-input evidence for run {} was supplied to run {model_run_id}",
                row.model_run_id
            )));
        }
        if row.decision_at != decision_at || row.knowledge_cutoff != knowledge_cutoff {
            return Err(determinism(format!(
                "model-input evidence for run {model_run_id} has an inconsistent decision boundary"
            )));
        }
        if row.audit_fingerprint.is_empty() {
            return Err(determinism(format!(
                "model-input evidence for run {model_run_id} has an empty audit fingerprint"
            )));
        }
        let key = model_input_key(row);
        if !keys.insert(key.clone()) {
            return Err(determinism(format!(
                "model-input evidence for run {model_run_id} contains duplicate key {key}"
            )));
        }
        match vector_markets.insert(row.feature_vector_id.clone(), row.market_id.clone()) {
            Some(previous) if previous != row.market_id => {
                return Err(determinism(format!(
                    "model-input vector {} is bound to multiple markets in run {model_run_id}",
                    row.feature_vector_id
                )));
            }
            _ => {}
        }
        match market_vectors.insert(row.market_id.clone(), row.feature_vector_id.clone()) {
            Some(previous) if previous != row.feature_vector_id => {
                return Err(determinism(format!(
                    "model-input market {} is bound to multiple vectors in run {model_run_id}",
                    row.market_id
                )));
            }
            _ => {}
        }
        entries.push(EvidenceEntry {
            key,
            audit_fingerprint: &row.audit_fingerprint,
        });
    }
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    let count = u64::try_from(rows.len()).map_err(|error| {
        determinism(format!(
            "model-input evidence row count does not fit u64: {error}"
        ))
    })?;
    let hash = ResearchHasher::canonical(&EvidenceBatch { entries: &entries })?;
    Ok((vector_markets, count, hash))
}

fn model_input_key(row: &QuantModelInputEventRow) -> String {
    format!(
        "{}/{}/{}/{}/{}",
        row.model_version_id,
        row.market_id,
        row.feature_vector_id,
        row.raw_input_name,
        row.encoded_column
    )
}

fn determinism(detail: impl Into<String>) -> QuantError {
    ResearchError::Determinism {
        detail: detail.into(),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use super::{
        SERVING_EVIDENCE_FORMAT_VERSION, completion_marker, feature_commitment, verify_completion,
    };
    use crate::observability::model_input_fact_writer::ModelInputEventWriter;
    use async_trait::async_trait;
    use chrono::Utc;
    use quant_pivot_error::storage::StorageError;
    use quant_pivot_models::{
        clickhouse::{QuantFeatureEventRow, QuantModelInputEventRow},
        domain::DecisionClock,
        enums::clickhouse::{ChFeatureCellState, ChFeatureSourceKind, ChFeatureValueKind},
        types::{FeatureVectorId, MarketId, ModelRunId, ModelVersionId, RuntimeConfigVersionId},
    };
    use quant_pivot_repository::traits::FactWriter;
    use std::{marker::PhantomData, sync::Arc};
    use uuid::Uuid;

    struct OrderedSink<T> {
        label: &'static str,
        calls: Arc<std::sync::Mutex<Vec<&'static str>>>,
        fail: bool,
        _row: PhantomData<fn(T)>,
    }

    impl<T> OrderedSink<T> {
        fn new(
            label: &'static str,
            calls: Arc<std::sync::Mutex<Vec<&'static str>>>,
            fail: bool,
        ) -> Self {
            Self {
                label,
                calls,
                fail,
                _row: PhantomData,
            }
        }
    }

    #[async_trait]
    impl<T: Send + Sync + 'static> FactWriter<T> for OrderedSink<T> {
        async fn write_batch(&self, _rows: Vec<T>) -> Result<(), StorageError> {
            self.calls
                .lock()
                .expect("ordered sink mutex")
                .push(self.label);
            if self.fail {
                return Err(StorageError::Connection(format!(
                    "{} sink failed",
                    self.label
                )));
            }
            Ok(())
        }
    }

    fn feature_row(
        vector_id: &FeatureVectorId,
        market_id: &MarketId,
        decision_at: i64,
    ) -> QuantFeatureEventRow {
        QuantFeatureEventRow {
            event_time: decision_at,
            feature_vector_id: vector_id.clone(),
            runtime_config_version_id: RuntimeConfigVersionId::new(Uuid::from_u128(1)),
            decision_at,
            knowledge_cutoff: decision_at,
            per_source_cutoffs_json: "{}".to_owned(),
            market_id: market_id.clone(),
            token_id: None,
            feature_schema_version: 6,
            feature_schema_hash: "schema".to_owned(),
            feature_hash: "feature".to_owned(),
            decision_capture_hash: "capture".to_owned(),
            feature_name: "book.best_bid".to_owned(),
            cell_state: ChFeatureCellState::Observed,
            raw_value: Some("0.5".to_owned()),
            value_kind: ChFeatureValueKind::Probability,
            source_kind: ChFeatureSourceKind::Book,
            evidence_source_kind: Some(ChFeatureSourceKind::Book),
            evidence_reference: Some("book:token".to_owned()),
            evidence_effective_at: Some(decision_at),
            evidence_available_at: None,
            reason: None,
            staleness_ms: Some(0),
            data_quality: "fresh".to_owned(),
            audit_fingerprint: "feature-fingerprint".to_owned(),
            ingestion_time: decision_at + 1,
        }
    }

    fn input_row(
        run_id: &ModelRunId,
        vector_id: &FeatureVectorId,
        market_id: &MarketId,
        decision_at: i64,
    ) -> QuantModelInputEventRow {
        QuantModelInputEventRow {
            event_time: decision_at,
            decision_at,
            knowledge_cutoff: decision_at,
            model_run_id: run_id.clone(),
            model_version_id: ModelVersionId::new(Uuid::from_u128(2)),
            recommendation_report_id: None,
            market_id: market_id.clone(),
            feature_vector_id: vector_id.clone(),
            model_family: "classical_logistic".to_owned(),
            raw_input_name: "book.best_bid".to_owned(),
            raw_state: "observed".to_owned(),
            raw_value: Some("0.5".to_owned()),
            encoded_column: "book.best_bid.value".to_owned(),
            encoded_value_bits: Some(0.5_f64.to_bits()),
            input_contract_hash: "contract".to_owned(),
            transform_hash: "transform".to_owned(),
            training_input_hash: "training".to_owned(),
            audit_fingerprint: "input-fingerprint".to_owned(),
            ingestion_time: decision_at + 2,
        }
    }

    #[test]
    fn completion_binds_exact_counts_hashes_and_vector_identity() {
        let decision_at = Utc::now();
        let boundary = DecisionClock::new(0)
            .boundary(decision_at)
            .expect("boundary");
        let vector_id = FeatureVectorId::from_v7();
        let market_id = MarketId::new("0xevidence");
        let run_id = ModelRunId::from_v7();
        let feature_rows = vec![feature_row(
            &vector_id,
            &market_id,
            decision_at.timestamp_millis(),
        )];
        let input_rows = vec![input_row(
            &run_id,
            &vector_id,
            &market_id,
            decision_at.timestamp_millis(),
        )];
        let features = feature_commitment(&feature_rows).expect("feature commitment");
        let marker = completion_marker(&run_id, &boundary, &features, &input_rows, 100)
            .expect("completion marker");

        assert_eq!(marker.expected_feature_row_count, 1);
        assert_eq!(marker.expected_model_input_row_count, 1);
        assert_eq!(
            verify_completion(&marker, &feature_rows, &input_rows).expect("verify"),
            vec![vector_id]
        );

        let retry = completion_marker(&run_id, &boundary, &features, &input_rows, 200)
            .expect("retry marker");
        assert_eq!(marker.completion_hash, retry.completion_hash);
    }

    #[test]
    fn completion_rejects_missing_or_tampered_rows() {
        let decision_at = Utc::now();
        let boundary = DecisionClock::new(0)
            .boundary(decision_at)
            .expect("boundary");
        let vector_id = FeatureVectorId::from_v7();
        let market_id = MarketId::new("0xtampered");
        let run_id = ModelRunId::from_v7();
        let feature_rows = vec![feature_row(
            &vector_id,
            &market_id,
            decision_at.timestamp_millis(),
        )];
        let input_rows = vec![input_row(
            &run_id,
            &vector_id,
            &market_id,
            decision_at.timestamp_millis(),
        )];
        let features = feature_commitment(&feature_rows).expect("feature commitment");
        let mut marker = completion_marker(&run_id, &boundary, &features, &input_rows, 100)
            .expect("completion marker");
        marker.expected_feature_row_count = 2;

        assert!(verify_completion(&marker, &feature_rows, &input_rows).is_err());
        assert!(verify_completion(&marker, &feature_rows, &[]).is_err());
    }

    #[test]
    fn completion_rejects_legacy_format_and_inconsistent_capture_binding() {
        let decision_at = Utc::now();
        let boundary = DecisionClock::new(0)
            .boundary(decision_at)
            .expect("boundary");
        let vector_id = FeatureVectorId::from_v7();
        let market_id = MarketId::new("0xcapture");
        let run_id = ModelRunId::from_v7();
        let mut first = feature_row(&vector_id, &market_id, decision_at.timestamp_millis());
        first.feature_name = "book.best_bid".to_owned();
        let mut second = first.clone();
        second.feature_name = "book.best_ask".to_owned();
        second.audit_fingerprint = "second-feature-fingerprint".to_owned();
        second.decision_capture_hash = "different-capture".to_owned();
        assert!(feature_commitment(&[first.clone(), second]).is_err());

        let input_rows = vec![input_row(
            &run_id,
            &vector_id,
            &market_id,
            decision_at.timestamp_millis(),
        )];
        let features =
            feature_commitment(std::slice::from_ref(&first)).expect("feature commitment");
        let mut marker = completion_marker(&run_id, &boundary, &features, &input_rows, 100)
            .expect("completion marker");
        assert_eq!(marker.format_version, SERVING_EVIDENCE_FORMAT_VERSION);
        marker.format_version = SERVING_EVIDENCE_FORMAT_VERSION - 1;
        assert!(verify_completion(&marker, &[first], &input_rows).is_err());
    }

    #[test]
    fn completion_commits_rejected_vectors_without_requiring_model_inputs() {
        let decision_at = Utc::now();
        let boundary = DecisionClock::new(0)
            .boundary(decision_at)
            .expect("boundary");
        let admitted_id = FeatureVectorId::from_v7();
        let rejected_id = FeatureVectorId::from_v7();
        let admitted_market = MarketId::new("0xadmitted");
        let rejected_market = MarketId::new("0xrejected");
        let run_id = ModelRunId::from_v7();
        let feature_rows = vec![
            feature_row(
                &admitted_id,
                &admitted_market,
                decision_at.timestamp_millis(),
            ),
            feature_row(
                &rejected_id,
                &rejected_market,
                decision_at.timestamp_millis(),
            ),
        ];
        let input_rows = vec![input_row(
            &run_id,
            &admitted_id,
            &admitted_market,
            decision_at.timestamp_millis(),
        )];
        let features = feature_commitment(&feature_rows)
            .expect("feature commitment")
            .bind_model_vectors(std::slice::from_ref(&admitted_id))
            .expect("admission binding");
        let marker = completion_marker(&run_id, &boundary, &features, &input_rows, 100)
            .expect("completion marker");

        assert_eq!(marker.expected_feature_row_count, 2);
        assert_eq!(marker.expected_model_input_row_count, 1);
        let vector_ids =
            verify_completion(&marker, &feature_rows, &input_rows).expect("verify completion");
        assert_eq!(vector_ids.len(), 2);
        assert!(vector_ids.contains(&admitted_id));
        assert!(vector_ids.contains(&rejected_id));
    }

    #[tokio::test]
    async fn completion_marker_is_acknowledged_only_after_model_input_rows() {
        let decision_at = Utc::now();
        let boundary = DecisionClock::new(0)
            .boundary(decision_at)
            .expect("boundary");
        let vector_id = FeatureVectorId::from_v7();
        let market_id = MarketId::new("0xordered");
        let run_id = ModelRunId::from_v7();
        let features = feature_commitment(&[feature_row(
            &vector_id,
            &market_id,
            decision_at.timestamp_millis(),
        )])
        .expect("feature commitment");
        let rows = vec![input_row(
            &run_id,
            &vector_id,
            &market_id,
            decision_at.timestamp_millis(),
        )];
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = ModelInputEventWriter::new(
            Arc::new(OrderedSink::new("inputs", Arc::clone(&calls), false)),
            Arc::new(OrderedSink::new("completion", Arc::clone(&calls), false)),
        );

        writer
            .commit_run(&run_id, &boundary, &features, rows)
            .await
            .expect("durable barrier");
        assert_eq!(
            calls.lock().expect("calls").as_slice(),
            ["inputs", "completion"]
        );
    }

    #[tokio::test]
    async fn input_sink_failure_never_writes_completion_marker() {
        let decision_at = Utc::now();
        let boundary = DecisionClock::new(0)
            .boundary(decision_at)
            .expect("boundary");
        let vector_id = FeatureVectorId::from_v7();
        let market_id = MarketId::new("0xfailure");
        let run_id = ModelRunId::from_v7();
        let features = feature_commitment(&[feature_row(
            &vector_id,
            &market_id,
            decision_at.timestamp_millis(),
        )])
        .expect("feature commitment");
        let rows = vec![input_row(
            &run_id,
            &vector_id,
            &market_id,
            decision_at.timestamp_millis(),
        )];
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = ModelInputEventWriter::new(
            Arc::new(OrderedSink::new("inputs", Arc::clone(&calls), true)),
            Arc::new(OrderedSink::new("completion", Arc::clone(&calls), false)),
        );

        assert!(
            writer
                .commit_run(&run_id, &boundary, &features, rows)
                .await
                .is_err()
        );
        assert_eq!(calls.lock().expect("calls").as_slice(), ["inputs"]);
    }
}
