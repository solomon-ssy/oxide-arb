//! Postgres-backed condition artifact and recommendation-instance state machine.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{ChSchemaVersion, EntryConditionEvaluationEventRow},
    domain::{
        ApplyEntryConditionEvaluation, ApplyEntryConditionEvaluationOutcome,
        CryptoPriceProjectionInfo, EntryConditionArtifactInfo, EntryConditionAuditInfo,
        EntryConditionClaim, EntryConditionInstanceInfo, NewEntryConditionArtifact,
        NewEntryConditionAudit, NewEntryConditionInstance, WeatherDailyTemperatureProjectionInfo,
    },
    entities::{
        quant_crypto_price_projection, quant_entry_condition_artifact, quant_entry_condition_audit,
        quant_entry_condition_evaluation_outbox, quant_entry_condition_instance,
        quant_weather_daily_temperature_projection,
    },
    enums::quant::{EntryConditionAuditAction, EntryConditionState},
    hashing::CanonicalDigest,
    types::{
        ConditionTruth, ContentHash, CryptoEnteredFoldState, DomainInstrumentKey, DomainSourceId,
        EntryConditionArtifactId, EntryConditionAuditId, EntryConditionFoldState,
        EntryConditionInstanceId, OrderIntentId, RecommendationId, TemperatureCelsius,
        WeatherTemperatureStatistic,
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
    sea_query::{Expr, LockBehavior, LockType},
};
use uuid::Uuid;

use crate::traits::EntryConditionRepository;

const ENTITY_ARTIFACT: &str = "quant_entry_condition_artifact";
const ENTITY_INSTANCE: &str = "quant_entry_condition_instance";

pub struct PgEntryConditionRepository {
    db: DatabaseConnection,
}

impl PgEntryConditionRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl EntryConditionRepository for PgEntryConditionRepository {
    async fn insert_artifact(
        &self,
        mut artifact: NewEntryConditionArtifact,
    ) -> Result<EntryConditionArtifactInfo, StorageError> {
        let canonical = artifact
            .payload_json
            .clone()
            .canonicalize()
            .map_err(|error| invariant(ENTITY_ARTIFACT, error.to_string()))?;
        let content_hash = canonical
            .canonical_content_hash()
            .map_err(|error| invariant(ENTITY_ARTIFACT, error.to_string()))?;
        if artifact.content_hash != content_hash
            || artifact.artifact_id != EntryConditionArtifactId::from_content_hash(&content_hash)
        {
            return Err(invariant(
                ENTITY_ARTIFACT,
                "artifact id/hash do not match canonical payload",
            ));
        }
        artifact.payload_json = canonical;
        quant_entry_condition_artifact::Entity::insert(artifact.into_active_model())
            .on_conflict_do_nothing_on([quant_entry_condition_artifact::Column::ContentHash])
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        quant_entry_condition_artifact::Entity::find_by_id(
            EntryConditionArtifactId::from_content_hash(&content_hash),
        )
        .one(&self.db)
        .await
        .map_err(StorageError::from)?
        .map(Into::into)
        .ok_or(StorageError::NotFound {
            entity: ENTITY_ARTIFACT,
            id: content_hash.to_string(),
        })
    }

    async fn create_instance(
        &self,
        instance: NewEntryConditionInstance,
        now: DateTime<Utc>,
    ) -> Result<EntryConditionInstanceInfo, StorageError> {
        validate_new_instance(&instance)?;
        let audit = NewEntryConditionAudit {
            audit_id: EntryConditionAuditId::from_v7(),
            condition_instance_id: instance.condition_instance_id.clone(),
            revision: instance.revision,
            action: EntryConditionAuditAction::Created,
            from_state: None,
            to_state: instance.state,
            truth_json: instance.truth_json.clone(),
            evaluation_hash: instance.evaluation_hash.clone(),
            input_fingerprint: instance.input_fingerprint.clone(),
            continuity_hash: instance.continuity_hash.clone(),
            lease_epoch: instance.lease_epoch,
            detail: None,
            occurred_at: now,
        };
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = quant_entry_condition_instance::Entity::insert(instance.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;
        quant_entry_condition_audit::Entity::insert(audit.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(row.into())
    }

    async fn find_artifact(
        &self,
        artifact_id: &EntryConditionArtifactId,
    ) -> Result<Option<EntryConditionArtifactInfo>, StorageError> {
        quant_entry_condition_artifact::Entity::find_by_id(artifact_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_instance(
        &self,
        instance_id: &EntryConditionInstanceId,
    ) -> Result<Option<EntryConditionInstanceInfo>, StorageError> {
        quant_entry_condition_instance::Entity::find_by_id(instance_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<EntryConditionInstanceInfo>, StorageError> {
        quant_entry_condition_instance::Entity::find()
            .filter(
                quant_entry_condition_instance::Column::RecommendationId
                    .eq(recommendation_id.clone()),
            )
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn audits(
        &self,
        instance_id: &EntryConditionInstanceId,
    ) -> Result<Vec<EntryConditionAuditInfo>, StorageError> {
        quant_entry_condition_audit::Entity::find()
            .filter(
                quant_entry_condition_audit::Column::ConditionInstanceId.eq(instance_id.clone()),
            )
            .order_by_asc(quant_entry_condition_audit::Column::Revision)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_crypto_projection(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
    ) -> Result<Option<CryptoPriceProjectionInfo>, StorageError> {
        quant_crypto_price_projection::Entity::find_by_id((
            source_id.clone(),
            instrument_key.clone(),
        ))
        .one(&self.db)
        .await
        .map_err(StorageError::from)
        .and_then(|row| {
            row.map(|row| {
                let source_sequence = u64::try_from(row.source_sequence).map_err(|error| {
                    invariant(
                        "quant_crypto_price_projection",
                        format!("source_sequence: {error}"),
                    )
                })?;
                Ok(CryptoPriceProjectionInfo {
                    source_id: row.source_id,
                    instrument_key: row.instrument_key,
                    previous_price: row.previous_price,
                    current_price: row.current_price,
                    source_sequence,
                    event_time: row.event_time,
                    available_at: row.available_at,
                    report_hash: row.report_hash,
                    gap_generation: row.gap_generation,
                    source_healthy: row.source_healthy,
                })
            })
            .transpose()
        })
    }

    async fn find_weather_projection(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
        station: &str,
        local_date: chrono::NaiveDate,
        temperature_statistic: WeatherTemperatureStatistic,
    ) -> Result<Option<WeatherDailyTemperatureProjectionInfo>, StorageError> {
        quant_weather_daily_temperature_projection::Entity::find_by_id((
            source_id.clone(),
            instrument_key.clone(),
            local_date,
            temperature_statistic,
        ))
        .filter(quant_weather_daily_temperature_projection::Column::Station.eq(station))
        .one(&self.db)
        .await
        .map_err(StorageError::from)
        .map(|row| {
            row.map(|row| WeatherDailyTemperatureProjectionInfo {
                source_id: row.source_id,
                instrument_key: row.instrument_key,
                station: row.station,
                local_date: row.local_date,
                timezone: row.timezone,
                temperature_statistic,
                current_extreme: TemperatureCelsius::new(row.current_extreme_celsius),
                last_observation_time: row.last_observation_time,
                last_report_hash: row.last_report_hash,
                revision: row.revision,
                day_closed: row.day_closed,
                gap_generation: row.gap_generation,
                source_healthy: row.source_healthy,
                available_at: row.available_at,
            })
        })
    }

    async fn expire_due(
        &self,
        now: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<EntryConditionInstanceInfo>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let rows = quant_entry_condition_instance::Entity::find()
            .filter(quant_entry_condition_instance::Column::State.is_in([
                EntryConditionState::Waiting,
                EntryConditionState::Unavailable,
                EntryConditionState::Confirming,
                EntryConditionState::Qualified,
            ]))
            .filter(quant_entry_condition_instance::Column::ExpiresAt.lte(now))
            .order_by_asc(quant_entry_condition_instance::Column::ExpiresAt)
            .limit(limit)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .all(&txn)
            .await
            .map_err(StorageError::from)?;
        let mut expired = Vec::with_capacity(rows.len());
        for row in rows {
            let from_state = row.state;
            let revision = checked_revision(row.revision)?;
            let mut active = row.into_active_model();
            active.state = ActiveValue::Set(EntryConditionState::Expired);
            active.revision = ActiveValue::Set(revision);
            active.confirmation_started_at = ActiveValue::Set(None);
            active.next_evaluation_at = ActiveValue::Set(None);
            active.lease_owner = ActiveValue::Set(None);
            active.lease_expires_at = ActiveValue::Set(None);
            let updated = active.update(&txn).await.map_err(StorageError::from)?;
            insert_audit(
                &txn,
                audit_from_model(
                    &updated,
                    revision,
                    EntryConditionAuditAction::Expired,
                    Some(from_state),
                    Some("recommendation entry window elapsed".to_owned()),
                    now,
                ),
            )
            .await?;
            expired.push(updated.into());
        }
        txn.commit().await.map_err(StorageError::from)?;
        Ok(expired)
    }

    async fn next_wakeup_at(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, StorageError> {
        let active_states = [
            EntryConditionState::Waiting,
            EntryConditionState::Unavailable,
            EntryConditionState::Confirming,
            EntryConditionState::Qualified,
        ];
        let evaluation = quant_entry_condition_instance::Entity::find()
            .filter(quant_entry_condition_instance::Column::State.is_in(active_states))
            .filter(quant_entry_condition_instance::Column::ExpiresAt.gt(now))
            .filter(quant_entry_condition_instance::Column::NextEvaluationAt.is_not_null())
            .order_by_asc(quant_entry_condition_instance::Column::NextEvaluationAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .and_then(|row| row.next_evaluation_at);
        let expiry = quant_entry_condition_instance::Entity::find()
            .filter(quant_entry_condition_instance::Column::State.is_in(active_states))
            .filter(quant_entry_condition_instance::Column::ExpiresAt.gt(now))
            .order_by_asc(quant_entry_condition_instance::Column::ExpiresAt)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(|row| row.expires_at);
        Ok(match (evaluation, expiry) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        })
    }

    async fn lease_next(
        &self,
        worker_id: Uuid,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Option<EntryConditionInstanceInfo>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let active_states = [
            EntryConditionState::Waiting,
            EntryConditionState::Unavailable,
            EntryConditionState::Confirming,
            EntryConditionState::Qualified,
        ];
        let row = quant_entry_condition_instance::Entity::find()
            .filter(quant_entry_condition_instance::Column::State.is_in(active_states))
            .filter(quant_entry_condition_instance::Column::ExpiresAt.gt(now))
            .filter(
                Condition::any()
                    .add(quant_entry_condition_instance::Column::NextEvaluationAt.is_null())
                    .add(quant_entry_condition_instance::Column::NextEvaluationAt.lte(now)),
            )
            .filter(
                Condition::any()
                    .add(quant_entry_condition_instance::Column::LeaseExpiresAt.is_null())
                    .add(quant_entry_condition_instance::Column::LeaseExpiresAt.lte(now)),
            )
            .order_by_asc(quant_entry_condition_instance::Column::NextEvaluationAt)
            .order_by_asc(quant_entry_condition_instance::Column::CreatedAt)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .one(&txn)
            .await
            .map_err(StorageError::from)?;
        let Some(row) = row else {
            txn.commit().await.map_err(StorageError::from)?;
            return Ok(None);
        };
        let from_state = row.state;
        let takeover = row.lease_owner.is_some();
        let next_epoch = if takeover || row.lease_epoch == 0 {
            checked_revision(row.lease_epoch)?
        } else {
            row.lease_epoch
        };
        let reset = takeover
            && matches!(
                row.state,
                EntryConditionState::Confirming | EntryConditionState::Qualified
            );
        let next_revision = if reset {
            checked_revision(row.revision)?
        } else {
            row.revision
        };
        let mut active = row.into_active_model();
        active.lease_owner = ActiveValue::Set(Some(worker_id));
        active.lease_expires_at = ActiveValue::Set(Some(lease_expires_at));
        active.lease_epoch = ActiveValue::Set(next_epoch);
        if reset {
            active.state = ActiveValue::Set(EntryConditionState::Waiting);
            active.truth_json = ActiveValue::Set(Some(ConditionTruth::Unsatisfied));
            active.confirmation_started_at = ActiveValue::Set(None);
            active.continuity_hash = ActiveValue::Set(None);
            active.revision = ActiveValue::Set(next_revision);
        }
        let leased = active.update(&txn).await.map_err(StorageError::from)?;
        if takeover {
            insert_audit(
                &txn,
                NewEntryConditionAudit {
                    audit_id: EntryConditionAuditId::from_v7(),
                    condition_instance_id: leased.condition_instance_id.clone(),
                    revision: next_revision,
                    action: EntryConditionAuditAction::LeaseTakenOver,
                    from_state: Some(from_state),
                    to_state: leased.state,
                    truth_json: leased.truth_json.clone(),
                    evaluation_hash: leased.evaluation_hash.clone(),
                    input_fingerprint: leased.input_fingerprint.clone(),
                    continuity_hash: leased.continuity_hash.clone(),
                    lease_epoch: next_epoch,
                    detail: Some("lease epoch changed; continuity reset".to_owned()),
                    occurred_at: now,
                },
            )
            .await?;
        }
        txn.commit().await.map_err(StorageError::from)?;
        Ok(Some(leased.into()))
    }

    async fn renew_lease(
        &self,
        instance_id: &EntryConditionInstanceId,
        worker_id: Uuid,
        lease_epoch: i64,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let result = quant_entry_condition_instance::Entity::update_many()
            .col_expr(
                quant_entry_condition_instance::Column::LeaseExpiresAt,
                sea_orm::sea_query::Expr::value(lease_expires_at),
            )
            .filter(
                quant_entry_condition_instance::Column::ConditionInstanceId.eq(instance_id.clone()),
            )
            .filter(quant_entry_condition_instance::Column::LeaseOwner.eq(worker_id))
            .filter(quant_entry_condition_instance::Column::LeaseEpoch.eq(lease_epoch))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(result.rows_affected == 1)
    }

    async fn apply_evaluation(
        &self,
        instance_id: &EntryConditionInstanceId,
        worker_id: Uuid,
        evaluation: ApplyEntryConditionEvaluation,
    ) -> Result<ApplyEntryConditionEvaluationOutcome, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = load_for_update(&txn, instance_id).await?;
        if row.lease_owner != Some(worker_id)
            || row.lease_epoch != evaluation.expected_lease_epoch
            || row.revision != evaluation.expected_revision
            || row
                .lease_expires_at
                .is_none_or(|expires_at| expires_at <= evaluation.evaluated_at)
        {
            return Err(state_conflict(
                instance_id,
                "evaluation lease/revision no longer matches",
            ));
        }
        if matches!(
            row.state,
            EntryConditionState::Consumed
                | EntryConditionState::Expired
                | EntryConditionState::Invalidated
                | EntryConditionState::NotRequired
        ) {
            return Err(state_conflict(instance_id, "instance is not evaluable"));
        }
        let transitioned = row.state != evaluation.state
            || row.truth_json.as_ref() != Some(&evaluation.truth)
            || row.continuity_hash.as_ref() != Some(&evaluation.continuity_hash)
            || row.confirmation_started_at != evaluation.confirmation_started_at
            || fold_semantics_changed(&row.fold_state_json, &evaluation.fold_state);
        let from_state = row.state;
        let revision = if transitioned {
            checked_revision(row.revision)?
        } else {
            row.revision
        };
        let mut active = row.into_active_model();
        if transitioned {
            active.state = ActiveValue::Set(evaluation.state);
            active.truth_json = ActiveValue::Set(Some(evaluation.truth.clone()));
            active.revision = ActiveValue::Set(revision);
            active.evaluation_hash = ActiveValue::Set(Some(evaluation.evaluation_hash.clone()));
            active.input_fingerprint = ActiveValue::Set(Some(evaluation.input_fingerprint.clone()));
            active.continuity_hash = ActiveValue::Set(Some(evaluation.continuity_hash.clone()));
            active.confirmation_started_at = ActiveValue::Set(evaluation.confirmation_started_at);
        }
        active.fold_state_json = ActiveValue::Set(evaluation.fold_state.clone());
        active.last_evaluated_at = ActiveValue::Set(Some(evaluation.evaluated_at));
        active.next_evaluation_at = ActiveValue::Set(evaluation.next_evaluation_at);
        active.lease_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        if transitioned {
            insert_audit(
                &txn,
                NewEntryConditionAudit {
                    audit_id: EntryConditionAuditId::from_v7(),
                    condition_instance_id: instance_id.clone(),
                    revision,
                    action: EntryConditionAuditAction::Evaluated,
                    from_state: Some(from_state),
                    to_state: evaluation.state,
                    truth_json: Some(evaluation.truth.clone()),
                    evaluation_hash: Some(evaluation.evaluation_hash.clone()),
                    input_fingerprint: Some(evaluation.input_fingerprint.clone()),
                    continuity_hash: Some(evaluation.continuity_hash.clone()),
                    lease_epoch: evaluation.expected_lease_epoch,
                    detail: None,
                    occurred_at: evaluation.evaluated_at,
                },
            )
            .await?;
        }
        insert_evaluation_outbox(&txn, instance_id, &evaluation, transitioned, revision).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(ApplyEntryConditionEvaluationOutcome {
            instance: updated.into(),
            transitioned,
        })
    }

    async fn claim_pending_evaluations(
        &self,
        worker_id: Uuid,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<EntryConditionEvaluationEventRow>, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let rows = quant_entry_condition_evaluation_outbox::Entity::find()
            .filter(quant_entry_condition_evaluation_outbox::Column::PublishedAt.is_null())
            .filter(
                Condition::any()
                    .add(quant_entry_condition_evaluation_outbox::Column::LeaseExpiresAt.is_null())
                    .add(quant_entry_condition_evaluation_outbox::Column::LeaseExpiresAt.lte(now)),
            )
            .order_by_asc(quant_entry_condition_evaluation_outbox::Column::CreatedAt)
            .limit(limit)
            .lock_with_behavior(LockType::Update, LockBehavior::SkipLocked)
            .all(&txn)
            .await
            .map_err(StorageError::from)?;
        let mut evaluations = Vec::with_capacity(rows.len());
        for row in rows {
            let attempts = row
                .publish_attempts
                .checked_add(1)
                .ok_or_else(|| invariant(ENTITY_INSTANCE, "evaluation publish attempt overflow"))?;
            let mut active = row.into_active_model();
            active.claim_owner = ActiveValue::Set(Some(worker_id));
            active.lease_expires_at = ActiveValue::Set(Some(lease_expires_at));
            active.publish_attempts = ActiveValue::Set(attempts);
            let claimed = active.update(&txn).await.map_err(StorageError::from)?;
            evaluations.push(claimed.event_json);
        }
        txn.commit().await.map_err(StorageError::from)?;
        Ok(evaluations)
    }

    async fn mark_evaluation_published(
        &self,
        evaluation_id: &ContentHash,
        worker_id: Uuid,
        published_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let result = quant_entry_condition_evaluation_outbox::Entity::update_many()
            .col_expr(
                quant_entry_condition_evaluation_outbox::Column::PublishedAt,
                Expr::value(published_at),
            )
            .col_expr(
                quant_entry_condition_evaluation_outbox::Column::LastError,
                Expr::value(Option::<String>::None),
            )
            .col_expr(
                quant_entry_condition_evaluation_outbox::Column::ClaimOwner,
                Expr::value(Option::<Uuid>::None),
            )
            .col_expr(
                quant_entry_condition_evaluation_outbox::Column::LeaseExpiresAt,
                Expr::value(Option::<DateTime<Utc>>::None),
            )
            .filter(
                quant_entry_condition_evaluation_outbox::Column::EvaluationId
                    .eq(evaluation_id.clone()),
            )
            .filter(quant_entry_condition_evaluation_outbox::Column::ClaimOwner.eq(worker_id))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        ensure_outbox_owner(result.rows_affected, evaluation_id)
    }

    async fn mark_evaluation_failed(
        &self,
        evaluation_id: &ContentHash,
        worker_id: Uuid,
        detail: String,
    ) -> Result<(), StorageError> {
        let result = quant_entry_condition_evaluation_outbox::Entity::update_many()
            .col_expr(
                quant_entry_condition_evaluation_outbox::Column::LastError,
                Expr::value(Some(detail)),
            )
            .col_expr(
                quant_entry_condition_evaluation_outbox::Column::ClaimOwner,
                Expr::value(Option::<Uuid>::None),
            )
            .col_expr(
                quant_entry_condition_evaluation_outbox::Column::LeaseExpiresAt,
                Expr::value(Option::<DateTime<Utc>>::None),
            )
            .filter(
                quant_entry_condition_evaluation_outbox::Column::EvaluationId
                    .eq(evaluation_id.clone()),
            )
            .filter(quant_entry_condition_evaluation_outbox::Column::ClaimOwner.eq(worker_id))
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        ensure_outbox_owner(result.rows_affected, evaluation_id)
    }

    async fn invalidate(
        &self,
        instance_id: &EntryConditionInstanceId,
        worker_id: Uuid,
        expected_revision: i64,
        expected_lease_epoch: i64,
        detail: String,
        now: DateTime<Utc>,
    ) -> Result<EntryConditionInstanceInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = load_for_update(&txn, instance_id).await?;
        if row.lease_owner != Some(worker_id)
            || row.lease_epoch != expected_lease_epoch
            || row.revision != expected_revision
            || row
                .lease_expires_at
                .is_none_or(|expires_at| expires_at <= now)
        {
            return Err(state_conflict(
                instance_id,
                "invalidation lease/revision no longer matches",
            ));
        }
        let from_state = row.state;
        let revision = checked_revision(row.revision)?;
        let mut active = row.into_active_model();
        active.state = ActiveValue::Set(EntryConditionState::Invalidated);
        active.revision = ActiveValue::Set(revision);
        active.confirmation_started_at = ActiveValue::Set(None);
        active.next_evaluation_at = ActiveValue::Set(None);
        active.lease_owner = ActiveValue::Set(None);
        active.lease_expires_at = ActiveValue::Set(None);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        insert_audit(
            &txn,
            audit_from_model(
                &updated,
                revision,
                EntryConditionAuditAction::Invalidated,
                Some(from_state),
                Some(detail),
                now,
            ),
        )
        .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }
}

fn fold_semantics_changed(
    previous: &EntryConditionFoldState,
    next: &EntryConditionFoldState,
) -> bool {
    previous.crypto.len() != next.crypto.len()
        || previous.crypto.iter().any(|previous_leaf| {
            next.crypto
                .iter()
                .find(|next_leaf| next_leaf.node_id == previous_leaf.node_id)
                .is_none_or(|next_leaf| crypto_semantics_changed(previous_leaf, next_leaf))
        })
}

fn crypto_semantics_changed(
    previous: &CryptoEnteredFoldState,
    next: &CryptoEnteredFoldState,
) -> bool {
    previous.node_id != next.node_id
        || previous.source != next.source
        || previous.last_outcome != next.last_outcome
        || previous.latched != next.latched
        || previous.gap_generation != next.gap_generation
        || previous.discontinuity_epoch != next.discontinuity_epoch
        || previous.triggering_report_hash != next.triggering_report_hash
        || previous.triggering_at != next.triggering_at
}

async fn insert_evaluation_outbox<C: sea_orm::ConnectionTrait>(
    db: &C,
    instance_id: &EntryConditionInstanceId,
    evaluation: &ApplyEntryConditionEvaluation,
    transitioned: bool,
    revision: i64,
) -> Result<(), StorageError> {
    let trace_kind = if transitioned { "applied" } else { "observed" };
    let evaluation_id = CanonicalDigest::content_hash_json(&(
        trace_kind,
        instance_id,
        evaluation.expected_revision,
        evaluation.evaluated_at,
        &evaluation.evaluation_hash,
    ))
    .map_err(|error| invariant(ENTITY_INSTANCE, error.to_string()))?;
    let event = EntryConditionEvaluationEventRow {
        evaluation_id: evaluation_id.clone(),
        condition_instance_id: instance_id.clone(),
        base_revision: evaluation.expected_revision,
        applied_revision: transitioned.then_some(revision),
        trace_kind: trace_kind.to_owned(),
        evaluator_version: evaluation.evaluator_version,
        evaluated_at: evaluation.evaluated_at.timestamp_millis(),
        state: evaluation.state.as_str().to_owned(),
        truth: truth_label(&evaluation.truth).to_owned(),
        evaluation_hash: evaluation.evaluation_hash.clone(),
        input_fingerprint: evaluation.input_fingerprint.clone(),
        continuity_hash: evaluation.continuity_hash.clone(),
        tree_json: evaluation.tree_json.clone(),
        schema_version: ChSchemaVersion::FIRST,
    };
    quant_entry_condition_evaluation_outbox::ActiveModel {
        outbox_id: ActiveValue::Set(Uuid::now_v7()),
        evaluation_id: ActiveValue::Set(evaluation_id),
        event_json: ActiveValue::Set(event),
        published_at: ActiveValue::Set(None),
        publish_attempts: ActiveValue::Set(0),
        claim_owner: ActiveValue::Set(None),
        lease_expires_at: ActiveValue::Set(None),
        last_error: ActiveValue::Set(None),
        ..Default::default()
    }
    .insert(db)
    .await
    .map_err(StorageError::from)?;
    Ok(())
}

pub async fn claim_for_submission<C: sea_orm::ConnectionTrait>(
    db: &C,
    claim: &EntryConditionClaim,
) -> Result<quant_entry_condition_instance::Model, StorageError> {
    let row = load_for_update(db, &claim.condition_instance_id).await?;
    let artifact_matches =
        row.artifact_id == claim.artifact_id && row.artifact_hash == claim.artifact_hash;
    let revision_matches = row.revision == claim.expected_revision;
    let evaluation_matches = row.evaluation_hash == claim.evaluation_hash;
    let input_matches = row.input_fingerprint == claim.input_fingerprint;
    let continuity_matches = row.continuity_hash == claim.continuity_hash;
    let evidence_matches = artifact_matches
        && revision_matches
        && evaluation_matches
        && input_matches
        && continuity_matches;
    if !evidence_matches
        || !matches!(
            row.state,
            EntryConditionState::NotRequired | EntryConditionState::Qualified
        )
        || row.expires_at <= claim.claimed_at
        || row.claimed_by_intent_id.is_some()
    {
        return Err(state_conflict(
            &claim.condition_instance_id,
            "condition evidence is not claimable at the expected revision",
        ));
    }
    let from_state = row.state;
    let revision = checked_revision(row.revision)?;
    let mut active = row.into_active_model();
    active.state = ActiveValue::Set(EntryConditionState::Consumed);
    active.revision = ActiveValue::Set(revision);
    active.claimed_by_intent_id = ActiveValue::Set(Some(claim.order_intent_id.clone()));
    active.claim_admission_state_version =
        ActiveValue::Set(Some(claim.admission_state_version.clone()));
    active.consumed_at = ActiveValue::Set(Some(claim.claimed_at));
    active.next_evaluation_at = ActiveValue::Set(None);
    active.lease_owner = ActiveValue::Set(None);
    active.lease_expires_at = ActiveValue::Set(None);
    let updated = active.update(db).await.map_err(StorageError::from)?;
    insert_audit(
        db,
        audit_from_model(
            &updated,
            revision,
            EntryConditionAuditAction::Claimed,
            Some(from_state),
            Some(format!(
                "claimed by intent {}; admission_state_version={}",
                claim.order_intent_id, claim.admission_state_version
            )),
            claim.claimed_at,
        ),
    )
    .await?;
    Ok(updated)
}

pub async fn revert_consumed_for_intent<C: sea_orm::ConnectionTrait>(
    db: &C,
    instance_id: &EntryConditionInstanceId,
    intent_id: &OrderIntentId,
    now: DateTime<Utc>,
) -> Result<quant_entry_condition_instance::Model, StorageError> {
    let row = load_for_update(db, instance_id).await?;
    if row.state != EntryConditionState::Consumed
        || row.claimed_by_intent_id.as_ref() != Some(intent_id)
    {
        return Ok(row);
    }
    let revision = checked_revision(row.revision)?;
    let conditional = row.artifact_id.is_some();
    let next_state = if conditional {
        EntryConditionState::Waiting
    } else {
        EntryConditionState::NotRequired
    };
    let mut active = row.into_active_model();
    active.state = ActiveValue::Set(next_state);
    active.truth_json = ActiveValue::Set(Some(if conditional {
        ConditionTruth::Unsatisfied
    } else {
        ConditionTruth::Satisfied
    }));
    active.revision = ActiveValue::Set(revision);
    active.confirmation_started_at = ActiveValue::Set(None);
    active.continuity_hash = ActiveValue::Set(None);
    active.claimed_by_intent_id = ActiveValue::Set(None);
    active.claim_admission_state_version = ActiveValue::Set(None);
    active.consumed_at = ActiveValue::Set(None);
    active.next_evaluation_at = ActiveValue::Set(conditional.then_some(now));
    let updated = active.update(db).await.map_err(StorageError::from)?;
    insert_audit(
        db,
        audit_from_model(
            &updated,
            revision,
            EntryConditionAuditAction::Reverted,
            Some(EntryConditionState::Consumed),
            Some(format!(
                "pre-submission claim reverted for intent {intent_id}"
            )),
            now,
        ),
    )
    .await?;
    Ok(updated)
}

/// Complete the condition side of a pre-submission intent terminal command.
/// The caller owns the surrounding transaction and follows the global lock
/// order: recommendation, intent, condition, capital reservation.
pub async fn invalidate_for_intent_terminal<C: sea_orm::ConnectionTrait>(
    db: &C,
    instance_id: &EntryConditionInstanceId,
    intent_id: &OrderIntentId,
    detail: String,
    now: DateTime<Utc>,
) -> Result<quant_entry_condition_instance::Model, StorageError> {
    let row = load_for_update(db, instance_id).await?;
    if matches!(
        row.state,
        EntryConditionState::Invalidated | EntryConditionState::Expired
    ) {
        return Ok(row);
    }
    if row.state == EntryConditionState::Consumed
        && row.claimed_by_intent_id.as_ref() != Some(intent_id)
    {
        return Err(state_conflict(
            instance_id,
            "consumed condition belongs to a different terminal intent command",
        ));
    }
    let from_state = row.state;
    let revision = checked_revision(row.revision)?;
    let mut fold_state = row.fold_state_json.clone();
    for state in &mut fold_state.crypto {
        if state.latched {
            state.latched = false;
            state.triggering_report_hash = None;
            state.triggering_at = None;
            state.discontinuity_epoch = state.discontinuity_epoch.saturating_add(1);
        }
    }
    let mut active = row.into_active_model();
    active.state = ActiveValue::Set(EntryConditionState::Invalidated);
    active.revision = ActiveValue::Set(revision);
    active.fold_state_json = ActiveValue::Set(fold_state);
    active.confirmation_started_at = ActiveValue::Set(None);
    active.next_evaluation_at = ActiveValue::Set(None);
    active.lease_owner = ActiveValue::Set(None);
    active.lease_expires_at = ActiveValue::Set(None);
    let updated = active.update(db).await.map_err(StorageError::from)?;
    insert_audit(
        db,
        audit_from_model(
            &updated,
            revision,
            EntryConditionAuditAction::Invalidated,
            Some(from_state),
            Some(detail),
            now,
        ),
    )
    .await?;
    Ok(updated)
}

pub async fn require_consumed_for_intent<C: sea_orm::ConnectionTrait>(
    db: &C,
    instance_id: &EntryConditionInstanceId,
    intent_id: &OrderIntentId,
) -> Result<(), StorageError> {
    let row = load_for_update(db, instance_id).await?;
    if row.state != EntryConditionState::Consumed
        || row.claimed_by_intent_id.as_ref() != Some(intent_id)
    {
        return Err(state_conflict(
            instance_id,
            "condition must be consumed by the submitting intent",
        ));
    }
    Ok(())
}

fn validate_new_instance(instance: &NewEntryConditionInstance) -> Result<(), StorageError> {
    let immediate = instance.artifact_id.is_none() && instance.artifact_hash.is_none();
    let conditional = instance.artifact_id.is_some() && instance.artifact_hash.is_some();
    if !immediate && !conditional {
        return Err(invariant(
            ENTITY_INSTANCE,
            "artifact id and hash must both be present or both absent",
        ));
    }
    let valid_state = if immediate {
        instance.state == EntryConditionState::NotRequired
    } else {
        instance.state == EntryConditionState::Waiting
    };
    if !valid_state || instance.revision != 0 || instance.lease_epoch != 0 {
        return Err(invariant(
            ENTITY_INSTANCE,
            "new condition instance has invalid initial state/revision/lease epoch",
        ));
    }
    Ok(())
}

async fn load_for_update<C: sea_orm::ConnectionTrait>(
    db: &C,
    instance_id: &EntryConditionInstanceId,
) -> Result<quant_entry_condition_instance::Model, StorageError> {
    quant_entry_condition_instance::Entity::find_by_id(instance_id.clone())
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::NotFound {
            entity: ENTITY_INSTANCE,
            id: instance_id.to_string(),
        })
}

async fn insert_audit<C: sea_orm::ConnectionTrait>(
    db: &C,
    audit: NewEntryConditionAudit,
) -> Result<(), StorageError> {
    quant_entry_condition_audit::Entity::insert(audit.into_active_model())
        .exec(db)
        .await
        .map_err(StorageError::from)?;
    Ok(())
}

fn audit_from_model(
    model: &quant_entry_condition_instance::Model,
    revision: i64,
    action: EntryConditionAuditAction,
    from_state: Option<EntryConditionState>,
    detail: Option<String>,
    occurred_at: DateTime<Utc>,
) -> NewEntryConditionAudit {
    NewEntryConditionAudit {
        audit_id: EntryConditionAuditId::from_v7(),
        condition_instance_id: model.condition_instance_id.clone(),
        revision,
        action,
        from_state,
        to_state: model.state,
        truth_json: model.truth_json.clone(),
        evaluation_hash: model.evaluation_hash.clone(),
        input_fingerprint: model.input_fingerprint.clone(),
        continuity_hash: model.continuity_hash.clone(),
        lease_epoch: model.lease_epoch,
        detail,
        occurred_at,
    }
}

fn checked_revision(value: i64) -> Result<i64, StorageError> {
    value
        .checked_add(1)
        .ok_or_else(|| invariant(ENTITY_INSTANCE, "revision/lease epoch overflow"))
}

const fn truth_label(truth: &ConditionTruth) -> &'static str {
    match truth {
        ConditionTruth::Satisfied => "satisfied",
        ConditionTruth::Unsatisfied => "unsatisfied",
        ConditionTruth::Unavailable(_) => "unavailable",
    }
}

fn ensure_outbox_owner(
    rows_affected: u64,
    evaluation_id: &ContentHash,
) -> Result<(), StorageError> {
    if rows_affected == 1 {
        return Ok(());
    }
    Err(StorageError::StateConflict {
        entity: "quant_entry_condition_evaluation_outbox",
        id: Some(evaluation_id.to_string()),
        detail: "evaluation outbox claim owner no longer matches".to_owned(),
    })
}

fn invariant(entity: &'static str, detail: impl Into<String>) -> StorageError {
    StorageError::InvariantViolation {
        entity: Some(entity),
        detail: detail.into(),
    }
}

fn state_conflict(
    instance_id: &EntryConditionInstanceId,
    detail: impl Into<String>,
) -> StorageError {
    StorageError::StateConflict {
        entity: ENTITY_INSTANCE,
        id: Some(instance_id.to_string()),
        detail: detail.into(),
    }
}
