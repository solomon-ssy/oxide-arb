//! Postgres append-only operational-readiness evidence index.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{NewResearchReadinessEvidence, ResearchReadinessEvidenceInfo},
    entities::quant_research_readiness_evidence,
    enums::quant::ResearchReadinessEvidenceKind,
    hashing::CanonicalDigest,
    types::{ContentHash, ResearchReadinessEvidencePayload},
};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, Statement, TryGetable, sea_query::OnConflict,
};

use crate::traits::{ResearchReadinessEvidenceRepository, ShadowLatencyObservation};

pub struct PgResearchReadinessEvidenceRepository {
    db: DatabaseConnection,
}

impl PgResearchReadinessEvidenceRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ResearchReadinessEvidenceRepository for PgResearchReadinessEvidenceRepository {
    async fn append(
        &self,
        evidence: NewResearchReadinessEvidence,
    ) -> Result<ResearchReadinessEvidenceInfo, StorageError> {
        validate_new(&evidence)?;
        let kind = evidence.kind;
        let scope_hash = evidence.scope_hash.clone();
        let payload_hash = evidence.payload_hash.clone();
        quant_research_readiness_evidence::Entity::insert(evidence.into_active_model())
            .on_conflict(
                OnConflict::columns([
                    quant_research_readiness_evidence::Column::Kind,
                    quant_research_readiness_evidence::Column::ScopeHash,
                    quant_research_readiness_evidence::Column::PayloadHash,
                ])
                .do_nothing()
                .to_owned(),
            )
            .exec_without_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        quant_research_readiness_evidence::Entity::find()
            .filter(quant_research_readiness_evidence::Column::Kind.eq(kind))
            .filter(quant_research_readiness_evidence::Column::ScopeHash.eq(scope_hash))
            .filter(quant_research_readiness_evidence::Column::PayloadHash.eq(payload_hash))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Into::into)
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
                    "readiness evidence was not observable after append",
                )
            })
    }

    async fn latest_valid(
        &self,
        kind: ResearchReadinessEvidenceKind,
        scope_hash: &ContentHash,
        as_of: DateTime<Utc>,
    ) -> Result<Option<ResearchReadinessEvidenceInfo>, StorageError> {
        quant_research_readiness_evidence::Entity::find()
            .filter(quant_research_readiness_evidence::Column::Kind.eq(kind))
            .filter(quant_research_readiness_evidence::Column::ScopeHash.eq(scope_hash.clone()))
            .filter(quant_research_readiness_evidence::Column::ObservedAt.lte(as_of))
            .filter(quant_research_readiness_evidence::Column::ExpiresAt.gt(as_of))
            .order_by_desc(quant_research_readiness_evidence::Column::ObservedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_id(
        &self,
        evidence_id: uuid::Uuid,
    ) -> Result<Option<ResearchReadinessEvidenceInfo>, StorageError> {
        quant_research_readiness_evidence::Entity::find_by_id(evidence_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn observe_shadow_latency(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Result<ShadowLatencyObservation, StorageError> {
        if window_start >= window_end {
            return Err(StorageError::invariant_violation(
                Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
                "shadow latency observation window must be half-open and non-empty",
            ));
        }
        let row = self
            .db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                r"
WITH decision_prepared AS (
    SELECT
        COUNT(*)::bigint AS sample_count,
        percentile_cont(0.95) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (created_at - decision_at)) * 1000
        )::bigint AS p95_ms
    FROM quant_recommendation_report
    WHERE runtime_mode = 'report_only'
      AND created_at >= $1 AND created_at < $2
      AND decision_at <= created_at
), endpoint_rtt AS (
    SELECT
        COUNT(*)::bigint AS sample_count,
        percentile_cont(0.95) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (fetched_at - started_at)) * 1000
        )::bigint AS p95_ms
    FROM catalog_sync_batch
    WHERE status = 'committed'
      AND fetched_at >= $1 AND fetched_at < $2
      AND started_at <= fetched_at
), market_delay AS (
    SELECT
        COUNT(*)::bigint AS sample_count,
        percentile_cont(0.95) WITHIN GROUP (
            ORDER BY COALESCE(minimum_order_age_secs, 0)::double precision * 1000
        )::bigint AS p95_ms
    FROM clob_market_info_version
    WHERE available_at >= $1 AND available_at < $2
)
SELECT
    decision_prepared.sample_count AS decision_prepared_count,
    decision_prepared.p95_ms AS decision_prepared_p95_ms,
    endpoint_rtt.sample_count AS endpoint_rtt_count,
    endpoint_rtt.p95_ms AS endpoint_rtt_p95_ms,
    market_delay.sample_count AS market_delay_count,
    market_delay.p95_ms AS market_delay_p95_ms
FROM decision_prepared, endpoint_rtt, market_delay
",
                [window_start.into(), window_end.into()],
            ))
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
                    "shadow latency aggregate returned no row",
                )
            })?;
        Ok(ShadowLatencyObservation {
            decision_prepared_count: read_count(&row, "decision_prepared_count")?,
            decision_prepared_p95_ms: read_percentile(&row, "decision_prepared_p95_ms")?,
            endpoint_rtt_count: read_count(&row, "endpoint_rtt_count")?,
            endpoint_rtt_p95_ms: read_percentile(&row, "endpoint_rtt_p95_ms")?,
            market_delay_count: read_count(&row, "market_delay_count")?,
            market_delay_p95_ms: read_percentile(&row, "market_delay_p95_ms")?,
        })
    }
}

fn read_count(row: &sea_orm::QueryResult, column: &str) -> Result<u64, StorageError> {
    let value = i64::try_get(row, "", column).map_err(|error| {
        StorageError::invariant_violation(
            Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
            format!("shadow latency aggregate has no valid {column}: {error:?}"),
        )
    })?;
    u64::try_from(value).map_err(|error| {
        StorageError::invariant_violation(
            Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
            format!("shadow latency {column} is invalid: {error}"),
        )
    })
}

fn read_percentile(row: &sea_orm::QueryResult, column: &str) -> Result<Option<u64>, StorageError> {
    Option::<i64>::try_get(row, "", column)
        .map_err(|error| {
            StorageError::invariant_violation(
                Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
                format!("shadow latency aggregate has no valid {column}: {error:?}"),
            )
        })?
        .map(|value| {
            u64::try_from(value).map_err(|error| {
                StorageError::invariant_violation(
                    Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
                    format!("shadow latency {column} is invalid: {error}"),
                )
            })
        })
        .transpose()
}

fn validate_new(evidence: &NewResearchReadinessEvidence) -> Result<(), StorageError> {
    if evidence.window_start >= evidence.window_end
        || evidence.window_end > evidence.observed_at
        || evidence.observed_at >= evidence.expires_at
        || evidence.artifact_version.trim().is_empty()
        || evidence.attestation_key_id.trim().is_empty()
    {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
            "readiness evidence time, artifact, or attestation contract is invalid",
        ));
    }
    let payload_bytes = serde_json::to_vec(&evidence.payload_json).map_err(|error| {
        StorageError::invariant_violation(
            Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
            format!("readiness evidence payload cannot be serialized: {error}"),
        )
    })?;
    let actual_payload_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(&payload_bytes))
        .map_err(|error| {
        StorageError::invariant_violation(
            Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
            format!("readiness evidence payload hash is invalid: {error}"),
        )
    })?;
    if actual_payload_hash != evidence.payload_hash
        || !payload_matches_kind(evidence.kind, &evidence.payload_json)
    {
        return Err(StorageError::invariant_violation(
            Some(entity::QUANT_RESEARCH_READINESS_EVIDENCE),
            "readiness evidence payload hash or kind binding is invalid",
        ));
    }
    Ok(())
}

const fn payload_matches_kind(
    kind: ResearchReadinessEvidenceKind,
    payload: &ResearchReadinessEvidencePayload,
) -> bool {
    matches!(
        (kind, payload),
        (
            ResearchReadinessEvidenceKind::RetentionRunway,
            ResearchReadinessEvidencePayload::RetentionRunway(_)
        ) | (
            ResearchReadinessEvidenceKind::ShadowLatencyProfile,
            ResearchReadinessEvidencePayload::ShadowLatencyProfile(_)
        )
    )
}
