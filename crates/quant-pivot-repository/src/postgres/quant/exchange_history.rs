//! `PostgreSQL` control-plane repository for accepted history chunks and quarantine evidence.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::data_plane::{
        ExchangeHistoryChunkInfo, ExchangeHistoryChunkStatus, ExchangeHistoryFrontier,
        ExchangeHistoryPlanInfo, ExchangeHistoryQuarantineDisposition,
        ExchangeHistoryQuarantineInfo, ExchangeHistoryQuarantineResolutionInfo,
        NewExchangeHistoryChunk, NewExchangeHistoryPlan, NewExchangeHistoryQuarantine,
        NewExchangeHistoryQuarantineResolution, ResolveAcceptedHistoryRange,
    },
    entities::{
        quant_exchange_history_chunk::{Column as ChunkColumn, Entity as ChunkEntity},
        quant_exchange_history_plan::{Column as PlanColumn, Entity as PlanEntity},
        quant_exchange_history_quarantine::{
            Column as QuarantineColumn, Entity as QuarantineEntity,
        },
        quant_exchange_history_quarantine_resolution::{
            Column as ResolutionColumn, Entity as ResolutionEntity,
        },
    },
};
use sea_orm::{
    ActiveModelTrait,
    ActiveValue::Set,
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    QuerySelect, TransactionTrait,
    sea_query::{OnConflict, Query},
};
use uuid::Uuid;

use crate::traits::ExchangeHistoryRepository;

const QUARANTINE_RESOLUTION_NAMESPACE: Uuid =
    Uuid::from_u128(0x6bc1_1f75_8ca8_4f31_9be5_17ad_1170_e401);

pub struct PgExchangeHistoryRepository {
    db: DatabaseConnection,
}

impl PgExchangeHistoryRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ExchangeHistoryRepository for PgExchangeHistoryRepository {
    async fn create_or_load_plan(
        &self,
        plan: NewExchangeHistoryPlan,
    ) -> Result<ExchangeHistoryPlanInfo, StorageError> {
        let requested = plan.clone();
        PlanEntity::insert(plan.into_active_model())
            .on_conflict(
                OnConflict::column(PlanColumn::ChainId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        let existing = self.load_plan(requested.chain_id).await?.ok_or_else(|| {
            StorageError::not_found("quant_exchange_history_plan", requested.plan_id)
        })?;
        if existing.plan_id != requested.plan_id
            || existing.policy_hash != requested.policy_hash
            || existing.bootstrap_profile_set_hash != requested.bootstrap_profile_set_hash
            || existing.finalized_anchor_block != requested.finalized_anchor_block
            || existing.finalized_anchor_hash != requested.finalized_anchor_hash
            || existing.finalized_anchor_timestamp != requested.finalized_anchor_timestamp
            || existing.activation_from_block != requested.activation_from_block
            || existing.activation_through_block != requested.activation_through_block
            || existing.crypto_required_from_block != requested.crypto_required_from_block
            || existing.weather_required_from_block != requested.weather_required_from_block
            || existing.retention_from_block != requested.retention_from_block
            || existing.retention_through_block != requested.retention_through_block
        {
            return Err(StorageError::state_conflict(
                "quant_exchange_history_plan",
                Some(existing.plan_id),
                "fresh-boot history plan was replayed with a different immutable preimage",
            ));
        }
        Ok(existing)
    }

    async fn load_plan(
        &self,
        chain_id: i64,
    ) -> Result<Option<ExchangeHistoryPlanInfo>, StorageError> {
        PlanEntity::find()
            .filter(PlanColumn::ChainId.eq(chain_id))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_range(
        &self,
        frontier: ExchangeHistoryFrontier,
        from_block: i64,
        to_block: i64,
    ) -> Result<Option<ExchangeHistoryChunkInfo>, StorageError> {
        ChunkEntity::find()
            .filter(ChunkColumn::Frontier.eq(frontier))
            .filter(ChunkColumn::FromBlock.eq(from_block))
            .filter(ChunkColumn::ToBlock.eq(to_block))
            .order_by_desc(ChunkColumn::UpdatedAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn save_chunk(
        &self,
        chunk: NewExchangeHistoryChunk,
    ) -> Result<ExchangeHistoryChunkInfo, StorageError> {
        ChunkEntity::insert(chunk.into_active_model())
            .on_conflict(
                OnConflict::column(ChunkColumn::ChunkId)
                    .update_columns([
                        ChunkColumn::Status,
                        ChunkColumn::AttemptCount,
                        ChunkColumn::HypersyncCount,
                        ChunkColumn::AttestorCount,
                        ChunkColumn::HypersyncDigest,
                        ChunkColumn::AttestorDigest,
                        ChunkColumn::FirstBlockHash,
                        ChunkColumn::LastBlockHash,
                        ChunkColumn::ArchiveHeight,
                        ChunkColumn::ContinuityBasis,
                        ChunkColumn::ContinuityBlock,
                        ChunkColumn::ContinuityHash,
                        ChunkColumn::EffectiveThroughAt,
                        ChunkColumn::AcceptedAt,
                        ChunkColumn::UpdatedAt,
                    ])
                    .to_owned(),
            )
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn latest_accepted(
        &self,
        frontier: ExchangeHistoryFrontier,
    ) -> Result<Option<ExchangeHistoryChunkInfo>, StorageError> {
        ChunkEntity::find()
            .filter(ChunkColumn::Frontier.eq(frontier))
            .filter(ChunkColumn::Status.eq(ExchangeHistoryChunkStatus::Accepted))
            .order_by_desc(ChunkColumn::ToBlock)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn earliest_accepted(
        &self,
        frontier: ExchangeHistoryFrontier,
    ) -> Result<Option<ExchangeHistoryChunkInfo>, StorageError> {
        ChunkEntity::find()
            .filter(ChunkColumn::Frontier.eq(frontier))
            .filter(ChunkColumn::Status.eq(ExchangeHistoryChunkStatus::Accepted))
            .order_by_asc(ChunkColumn::FromBlock)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn accepted_from(
        &self,
        frontier: ExchangeHistoryFrontier,
        from_block: i64,
    ) -> Result<Vec<ExchangeHistoryChunkInfo>, StorageError> {
        ChunkEntity::find()
            .filter(ChunkColumn::Frontier.eq(frontier))
            .filter(ChunkColumn::Status.eq(ExchangeHistoryChunkStatus::Accepted))
            .filter(ChunkColumn::ToBlock.gte(from_block))
            .order_by_asc(ChunkColumn::FromBlock)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn rewind_from(
        &self,
        frontier: ExchangeHistoryFrontier,
        from_block: i64,
        updated_at: DateTime<Utc>,
    ) -> Result<Vec<ExchangeHistoryChunkInfo>, StorageError> {
        let rows = ChunkEntity::find()
            .filter(ChunkColumn::Frontier.eq(frontier))
            .filter(ChunkColumn::Status.eq(ExchangeHistoryChunkStatus::Accepted))
            .filter(ChunkColumn::ToBlock.gte(from_block))
            .order_by_asc(ChunkColumn::FromBlock)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        for row in &rows {
            let mut active = row.clone().into_active_model();
            active.status = Set(ExchangeHistoryChunkStatus::Rewound);
            active.updated_at = Set(updated_at);
            active
                .update(&transaction)
                .await
                .map_err(StorageError::from)?;
        }
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn quarantine_chunk(
        &self,
        chunk: NewExchangeHistoryChunk,
        quarantine: NewExchangeHistoryQuarantine,
    ) -> Result<ExchangeHistoryQuarantineInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        ChunkEntity::insert(chunk.into_active_model())
            .on_conflict(
                OnConflict::column(ChunkColumn::ChunkId)
                    .update_columns([
                        ChunkColumn::Status,
                        ChunkColumn::AttemptCount,
                        ChunkColumn::UpdatedAt,
                    ])
                    .to_owned(),
            )
            .exec_without_returning(&transaction)
            .await
            .map_err(StorageError::from)?;
        let row = QuarantineEntity::insert(quarantine.into_active_model())
            .exec_with_returning(&transaction)
            .await
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(row.into())
    }

    async fn list_quarantine(
        &self,
        frontier: ExchangeHistoryFrontier,
        limit: u64,
    ) -> Result<Vec<ExchangeHistoryQuarantineInfo>, StorageError> {
        QuarantineEntity::find()
            .inner_join(ChunkEntity)
            .filter(ChunkColumn::Frontier.eq(frontier))
            .order_by_desc(QuarantineColumn::QuarantinedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn active_quarantine(
        &self,
        frontier: ExchangeHistoryFrontier,
        from_block: i64,
        to_block: i64,
        limit: u64,
    ) -> Result<Vec<ExchangeHistoryQuarantineInfo>, StorageError> {
        if from_block < 0 || to_block < from_block || limit == 0 {
            return Err(StorageError::invariant_violation(
                Some("quant_exchange_history_quarantine"),
                "active-quarantine range and limit are invalid",
            ));
        }
        let resolved = Query::select()
            .column(ResolutionColumn::QuarantineId)
            .from(ResolutionEntity)
            .to_owned();
        QuarantineEntity::find()
            .inner_join(ChunkEntity)
            .filter(ChunkColumn::Frontier.eq(frontier))
            .filter(ChunkColumn::FromBlock.lte(to_block))
            .filter(ChunkColumn::ToBlock.gte(from_block))
            .filter(QuarantineColumn::QuarantineId.not_in_subquery(resolved))
            .order_by_asc(ChunkColumn::FromBlock)
            .order_by_asc(QuarantineColumn::QuarantinedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn resolve_quarantine(
        &self,
        resolution: NewExchangeHistoryQuarantineResolution,
    ) -> Result<ExchangeHistoryQuarantineResolutionInfo, StorageError> {
        let actor = resolution.actor.trim();
        let detail = resolution.detail.trim();
        if actor.is_empty()
            || actor.len() > 128
            || actor != resolution.actor
            || detail.is_empty()
            || detail.len() > 2_048
            || detail != resolution.detail
        {
            return Err(StorageError::invariant_violation(
                Some("quant_exchange_history_quarantine_resolution"),
                "resolution actor and detail must be bounded canonical text",
            ));
        }
        let requested = resolution.clone();
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let quarantine = QuarantineEntity::find_by_id(resolution.quarantine_id)
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    "quant_exchange_history_quarantine",
                    resolution.quarantine_id,
                )
            })?;
        let original = ChunkEntity::find_by_id(quarantine.chunk_id)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("quant_exchange_history_chunk", quarantine.chunk_id)
            })?;
        let replacement = ChunkEntity::find_by_id(resolution.replacement_chunk_id)
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    "quant_exchange_history_chunk",
                    resolution.replacement_chunk_id,
                )
            })?;
        let scope_valid = match resolution.disposition {
            ExchangeHistoryQuarantineDisposition::AcceptedReplacement => {
                replacement.frontier == original.frontier
                    && replacement.from_block == original.from_block
                    && replacement.to_block == original.to_block
            }
            ExchangeHistoryQuarantineDisposition::CanonicalSupersession => {
                replacement.frontier == original.frontier
                    && replacement.from_block <= original.from_block
                    && replacement.to_block >= original.to_block
            }
        };
        if replacement.status != ExchangeHistoryChunkStatus::Accepted || !scope_valid {
            return Err(StorageError::state_conflict(
                "quant_exchange_history_quarantine_resolution",
                Some(resolution.quarantine_id),
                "resolution requires an accepted replacement covering the exact blocked scope",
            ));
        }
        ResolutionEntity::insert(resolution.into_active_model())
            .on_conflict(
                OnConflict::column(ResolutionColumn::QuarantineId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&transaction)
            .await
            .map_err(StorageError::from)?;
        let stored = ResolutionEntity::find()
            .filter(ResolutionColumn::QuarantineId.eq(requested.quarantine_id))
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found(
                    "quant_exchange_history_quarantine_resolution",
                    requested.resolution_id,
                )
            })?;
        if stored.resolution_id != requested.resolution_id
            || stored.disposition != requested.disposition
            || stored.replacement_chunk_id != requested.replacement_chunk_id
            || stored.evidence_hash != requested.evidence_hash
        {
            return Err(StorageError::state_conflict(
                "quant_exchange_history_quarantine_resolution",
                Some(requested.quarantine_id),
                "quarantine resolution was replayed with a different proof",
            ));
        }
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(stored.into())
    }

    async fn resolve_accepted_range(
        &self,
        resolution: ResolveAcceptedHistoryRange,
    ) -> Result<Vec<ExchangeHistoryQuarantineResolutionInfo>, StorageError> {
        let ResolveAcceptedHistoryRange {
            frontier,
            from_block,
            to_block,
            replacement_chunk_id,
            evidence_hash,
            actor,
            resolved_at,
        } = resolution;
        let resolved = Query::select()
            .column(ResolutionColumn::QuarantineId)
            .from(ResolutionEntity)
            .to_owned();
        let quarantines = QuarantineEntity::find()
            .inner_join(ChunkEntity)
            .filter(ChunkColumn::Frontier.eq(frontier))
            .filter(ChunkColumn::FromBlock.eq(from_block))
            .filter(ChunkColumn::ToBlock.eq(to_block))
            .filter(QuarantineColumn::QuarantineId.not_in_subquery(resolved))
            .order_by_asc(QuarantineColumn::QuarantinedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let mut outcomes = Vec::with_capacity(quarantines.len());
        for quarantine in quarantines {
            let preimage = format!(
                "{}:{}:{}",
                quarantine.quarantine_id, replacement_chunk_id, evidence_hash
            );
            let resolution_id = Uuid::new_v5(&QUARANTINE_RESOLUTION_NAMESPACE, preimage.as_bytes());
            outcomes.push(
                self.resolve_quarantine(NewExchangeHistoryQuarantineResolution {
                    resolution_id,
                    quarantine_id: quarantine.quarantine_id,
                    disposition: ExchangeHistoryQuarantineDisposition::AcceptedReplacement,
                    replacement_chunk_id,
                    evidence_hash,
                    actor: actor.clone(),
                    detail: "accepted dual-provider replacement covers the exact quarantined range"
                        .to_owned(),
                    resolved_at,
                })
                .await?,
            );
        }
        Ok(outcomes)
    }
}
