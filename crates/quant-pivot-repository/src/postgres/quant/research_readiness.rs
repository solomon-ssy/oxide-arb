//! Postgres append-only operational-readiness evidence index.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_RESEARCH_READINESS_EVIDENCE};
use quant_pivot_models::{
    domain::quant::{NewResearchReadinessEvidence, ResearchReadinessEvidenceInfo},
    entities::quant_research_readiness_evidence::{Column, Entity},
    enums::quant::ResearchReadinessEvidenceKind,
    hashing::CanonicalDigest,
    types::{ContentHash, ResearchReadinessEvidenceId, ResearchReadinessEvidencePayload},
};
use sea_orm::{
    ColumnTrait, DatabaseBackend, DatabaseConnection, EntityTrait, FromQueryResult,
    IntoActiveModel, QueryFilter, QueryOrder, Statement, sea_query::OnConflict,
};

use crate::traits::{ResearchReadinessEvidenceRepository, ShadowLatencyObservation};

#[derive(Debug, FromQueryResult)]
struct ShadowLatencyAggregate {
    decision_prepared_count: i64,
    decision_prepared_p95_ms: Option<i64>,
    endpoint_rtt_count: i64,
    endpoint_rtt_p95_ms: Option<i64>,
    market_delay_count: i64,
    market_delay_p95_ms: Option<i64>,
}

pub struct PgResearchReadinessEvidenceRepository {
    db: DatabaseConnection,
}

impl PgResearchReadinessEvidenceRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    /// Execute the `PostgreSQL` ordered-set aggregate owned by the readiness
    /// repository. `SeaQuery` has no typed representation for
    /// `percentile_cont ... WITHIN GROUP`.
    async fn shadow_latency(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Result<ShadowLatencyAggregate, StorageError> {
        ShadowLatencyAggregate::find_by_statement(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            r"
WITH decision_prepared AS (
    SELECT
        COUNT(*)::bigint AS sample_count,
        percentile_cont(0.95) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (created_at - decision_at)) * 1000
        )::bigint AS p95_ms
    FROM quant_recommendation_report
    WHERE created_at >= $1 AND created_at < $2
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
        .one(&self.db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| {
            StorageError::invariant_violation(None, "shadow latency query returned no row")
        })
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
        let scope_hash = evidence.scope_hash;
        let payload_hash = evidence.payload_hash;
        Entity::insert(evidence.into_active_model())
            .on_conflict(
                OnConflict::columns([Column::Kind, Column::ScopeHash, Column::PayloadHash])
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        Entity::find()
            .filter(Column::Kind.eq(kind))
            .filter(Column::ScopeHash.eq(scope_hash))
            .filter(Column::PayloadHash.eq(payload_hash))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Into::into)
            .ok_or_else(|| {
                StorageError::invariant_violation(
                    Some(QUANT_RESEARCH_READINESS_EVIDENCE),
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
        Entity::find()
            .filter(Column::Kind.eq(kind))
            .filter(Column::ScopeHash.eq(*scope_hash))
            .filter(Column::ObservedAt.lte(as_of))
            .filter(Column::ExpiresAt.gt(as_of))
            .order_by_desc(Column::ObservedAt)
            .order_by_desc(Column::EvidenceId)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_id(
        &self,
        evidence_id: &ResearchReadinessEvidenceId,
    ) -> Result<Option<ResearchReadinessEvidenceInfo>, StorageError> {
        Entity::find_by_id(*evidence_id)
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
                Some(QUANT_RESEARCH_READINESS_EVIDENCE),
                "shadow latency observation window must be half-open and non-empty",
            ));
        }
        let row = self.shadow_latency(window_start, window_end).await?;
        Ok(ShadowLatencyObservation {
            decision_prepared_count: checked_count(
                "decision_prepared_count",
                row.decision_prepared_count,
            )?,
            decision_prepared_p95_ms: checked_percentile(
                "decision_prepared_p95_ms",
                row.decision_prepared_p95_ms,
            )?,
            endpoint_rtt_count: checked_count("endpoint_rtt_count", row.endpoint_rtt_count)?,
            endpoint_rtt_p95_ms: checked_percentile(
                "endpoint_rtt_p95_ms",
                row.endpoint_rtt_p95_ms,
            )?,
            market_delay_count: checked_count("market_delay_count", row.market_delay_count)?,
            market_delay_p95_ms: checked_percentile(
                "market_delay_p95_ms",
                row.market_delay_p95_ms,
            )?,
        })
    }
}

fn checked_count(column: &str, value: i64) -> Result<u64, StorageError> {
    u64::try_from(value).map_err(|error| {
        StorageError::invariant_violation(
            Some(QUANT_RESEARCH_READINESS_EVIDENCE),
            format!("shadow latency {column} is invalid: {error}"),
        )
    })
}

fn checked_percentile(column: &str, value: Option<i64>) -> Result<Option<u64>, StorageError> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|error| {
                StorageError::invariant_violation(
                    Some(QUANT_RESEARCH_READINESS_EVIDENCE),
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
    {
        return Err(StorageError::invariant_violation(
            Some(QUANT_RESEARCH_READINESS_EVIDENCE),
            "readiness evidence time, artifact, or attestation contract is invalid",
        ));
    }
    let payload_bytes =
        CanonicalDigest::canonical_json_bytes(&evidence.payload_json).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_RESEARCH_READINESS_EVIDENCE),
                format!("readiness evidence payload cannot be serialized: {error}"),
            )
        })?;
    let actual_payload_hash = CanonicalDigest::content_hash_bytes(&payload_bytes);
    if actual_payload_hash != evidence.payload_hash
        || !payload_matches_kind(evidence.kind, &evidence.payload_json)
    {
        return Err(StorageError::invariant_violation(
            Some(QUANT_RESEARCH_READINESS_EVIDENCE),
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
