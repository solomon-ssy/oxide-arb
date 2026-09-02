//! Postgres-backed unified calibration-artifact ledger repository. Artifact
//! identity is append-only; `active` is the sole mutable column.

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_CALIBRATION_ARTIFACT, QUANT_MODEL_RUN},
};
use quant_pivot_models::{
    domain::{
        api::CalibrationArtifactListQuery,
        pagination::{PageWindow, Paginated},
        quant::{
            CalibrationArtifactInfo, CalibrationArtifactPayload, ModelRunInfo,
            ModelScoreCalibrationCommitOutcome, NewCalibrationArtifact,
            VerifiedModelScoreCalibrationCommit,
        },
    },
    entities::{
        quant_calibration_artifact::{Column, Entity},
        quant_calibration_artifact_publication::{
            ActiveModel, Column as QuantCalibrationArtifactPublicationColumn,
            Entity as QuantCalibrationArtifactPublicationEntity,
        },
        quant_model_run::{
            ActiveModel as ModelRunActiveModel, Entity as ModelRunEntity, Model as ModelRunModel,
        },
    },
    enums::quant::{CalibrationKind, DatasetPurpose, ModelRunKind, ModelRunStatus},
    hashing::CanonicalDigest,
    types::{
        CalibrationArtifactId, CalibrationArtifactPublicationId, ContentHash,
        calibration::{ModelScoreCalibrationPayload, PublishedWeatherStationLeadBias},
    },
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    EntityTrait, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
    TryInsertResult,
    sea_query::{Expr, OnConflict},
};

use crate::{
    postgres::{
        error, primitives,
        quant::integrity::{load_dataset, load_model_lineage, verify_replay_dataset},
        query::paginate_mapped,
    },
    traits::CalibrationArtifactRepository,
};

/// Postgres-backed unified calibration-artifact ledger repository.
pub struct PgCalibrationArtifactRepository {
    db: DatabaseConnection,
}

struct ModelScoreCommitIdentity {
    kind: CalibrationKind,
    payload_kind: CalibrationKind,
    content_hash: ContentHash,
    fit_window_start: DateTime<Utc>,
    fit_window_end: DateTime<Utc>,
    calibration_split_hash: ContentHash,
    sample_count: i64,
}

impl From<&NewCalibrationArtifact> for ModelScoreCommitIdentity {
    fn from(artifact: &NewCalibrationArtifact) -> Self {
        Self {
            kind: artifact.kind,
            payload_kind: artifact.payload.kind(),
            content_hash: artifact.content_hash,
            fit_window_start: artifact.fit_window_start,
            fit_window_end: artifact.fit_window_end,
            calibration_split_hash: artifact.calibration_split_hash,
            sample_count: artifact.sample_count,
        }
    }
}

impl PgCalibrationArtifactRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    async fn validate_model_score_lineage<C>(
        &self,
        db: &C,
        payload: &ModelScoreCalibrationPayload,
        fit_window_start: DateTime<Utc>,
        fit_window_end: DateTime<Utc>,
        sample_count: i64,
    ) -> Result<ContentHash, StorageError>
    where
        C: ConnectionTrait,
    {
        let fit = &payload.fit_contract;
        let model = load_model_lineage(db, fit.model.model_version_id).await?;
        let bindings = model.version.serving_contract.bindings();
        let training = model.training_materialization()?;
        if fit.model.artifact_hash != model.version.artifact_hash
            || fit.model.serving_contract_hash != model.version.serving_contract.contract_hash()
            || fit.model.model_spec_id != model.spec.model_spec_id
            || fit.model.model_spec_definition_hash != model.spec.definition_hash
            || fit.model.model_family != model.spec.model_family
            || fit.model.profile_ref != bindings.model.profile_ref
            || fit.model.category_scope != bindings.model.category_scope
            || fit.model.prediction_horizon_secs != bindings.model.prediction_horizon_secs
            || fit.model.training_dataset_id != model.training_dataset.training_dataset_id
            || fit.model.training_dataset_hash != *training.dataset_hash
        {
            return Err(invariant(
                "model-score fit contract differs from the exact source model",
            ));
        }
        if fit.policy_snapshot.decision_policy_snapshot_id
            != model.policy.decision_policy_snapshot_id
            || fit.policy_snapshot.snapshot_hash != model.policy.snapshot_hash
            || fit.policy_snapshot.decision_policy_snapshot_id
                != bindings.policy_snapshot.decision_policy_snapshot_id
            || fit.policy_snapshot.snapshot_hash != bindings.policy_snapshot.snapshot_hash
        {
            return Err(invariant(
                "model-score fit contract differs from the exact policy snapshot",
            ));
        }

        let dataset = load_dataset(db, fit.calibration_dataset.calibration_dataset_id).await?;
        let calibration =
            verify_replay_dataset(db, &dataset, DatasetPurpose::Calibration, &model).await?;
        let dataset_binding = &fit.calibration_dataset;
        if dataset_binding.dataset_hash != *calibration.dataset_hash
            || dataset_binding.manifest_hash != *calibration.manifest_hash
            || dataset_binding.artifact_bytes_hash != *calibration.artifact_bytes_hash
            || dataset_binding.source_slice_manifest_hash
                != calibration
                    .manifest
                    .source_lineage
                    .source_slice
                    .manifest_hash
            || dataset_binding.feature_schema_hash != *calibration.feature_schema_hash
            || dataset_binding.factor_schema_hash != calibration.factor_schema_hash()
            || dataset_binding.label_schema_hash != *calibration.label_schema_hash
            || fit_window_start != dataset.window_start
            || fit_window_end != dataset.window_end
            || calibration.manifest.source_lineage.research_program_hash
                != training.manifest.source_lineage.research_program_hash
        {
            return Err(invariant(
                "model-score fit contract differs from the exact Calibration Dataset",
            ));
        }
        let artifact_samples = u64::try_from(sample_count)
            .map_err(|error| invariant(format!("invalid artifact sample_count: {error}")))?;
        let dataset_samples = u64::try_from(calibration.sample_count)
            .map_err(|error| invariant(format!("invalid Dataset sample_count: {error}")))?;
        if artifact_samples > dataset_samples {
            return Err(invariant(
                "model-score artifact uses more samples than its Calibration Dataset",
            ));
        }
        let embargo_secs = i64::try_from(
            model
                .policy
                .snapshot
                .model_routing
                .model
                .calibration
                .embargo_secs,
        )
        .map_err(|error| invariant(format!("invalid calibration embargo: {error}")))?;
        let required_start = model
            .training_dataset
            .window_end
            .checked_add_signed(Duration::seconds(embargo_secs))
            .ok_or_else(|| invariant("calibration embargo timestamp overflow"))?;
        if dataset.training_dataset_id == model.training_dataset.training_dataset_id
            || dataset.window_start < required_start
        {
            return Err(invariant(
                "Calibration Dataset is not independent and embargoed after Training",
            ));
        }
        Ok(*calibration.dataset_hash)
    }

    fn verify_existing_identity(
        stored: &CalibrationArtifactInfo,
        requested: &ModelScoreCommitIdentity,
    ) -> Result<(), StorageError> {
        if stored.kind != requested.kind
            || stored.payload.kind() != requested.payload_kind
            || stored.content_hash != requested.content_hash
            || stored.fit_window_start != requested.fit_window_start
            || stored.fit_window_end != requested.fit_window_end
            || stored.calibration_split_hash != requested.calibration_split_hash
            || stored.sample_count != requested.sample_count
        {
            return Err(StorageError::state_conflict(
                QUANT_CALIBRATION_ARTIFACT,
                Some(&requested.content_hash),
                "content-addressed calibration collision is not an exact immutable replay",
            ));
        }
        Ok(())
    }

    fn validate_run(
        run: &ModelRunModel,
        commit: &VerifiedModelScoreCalibrationCommit,
        payload: &ModelScoreCalibrationPayload,
        dataset_hash: ContentHash,
    ) -> Result<bool, StorageError> {
        let artifact = commit.artifact();
        let exact_subject = run.run_kind == ModelRunKind::Calibration
            && run.model_version_id == Some(payload.fit_contract.model.model_version_id)
            && run.decision_policy_snapshot_id
                == payload
                    .fit_contract
                    .policy_snapshot
                    .decision_policy_snapshot_id
            && run.market_selection_id.is_none()
            && run.window_start == artifact.fit_window_start
            && run.window_end == artifact.fit_window_end
            && run.input_hash == dataset_hash;
        if !exact_subject {
            return Err(StorageError::state_conflict(
                QUANT_MODEL_RUN,
                Some(&commit.model_run_id()),
                "Calibration run differs from the canonical artifact subject",
            ));
        }
        match run.status {
            ModelRunStatus::Running
                if run.output_hash.is_none()
                    && run.error_code.is_none()
                    && run.error_message.is_none()
                    && run.finished_at.is_none() =>
            {
                Ok(false)
            }
            ModelRunStatus::Succeeded
                if run.output_hash == Some(artifact.content_hash)
                    && run.error_code.is_none()
                    && run.error_message.is_none()
                    && run.finished_at.is_some()
                    && run
                        .finished_at
                        .is_some_and(|finished_at| finished_at >= run.started_at) =>
            {
                Ok(true)
            }
            _ => Err(StorageError::state_conflict(
                QUANT_MODEL_RUN,
                Some(&commit.model_run_id()),
                "Calibration run is neither Running nor an exact Succeeded replay",
            )),
        }
    }

    async fn load_by_content_hash<C>(
        db: &C,
        content_hash: ContentHash,
    ) -> Result<CalibrationArtifactInfo, StorageError>
    where
        C: ConnectionTrait,
    {
        Entity::find()
            .filter(Column::ContentHash.eq(content_hash))
            .lock_shared()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .map(Into::into)
            .ok_or_else(|| {
                StorageError::state_conflict(
                    QUANT_CALIBRATION_ARTIFACT,
                    Some(&content_hash),
                    "calibration insert conflicted without an observable canonical artifact",
                )
            })
    }
}

#[async_trait::async_trait]
impl CalibrationArtifactRepository for PgCalibrationArtifactRepository {
    async fn create(
        &self,
        artifact: NewCalibrationArtifact,
    ) -> Result<CalibrationArtifactInfo, StorageError> {
        if artifact.payload.kind() != artifact.kind {
            return Err(invariant(
                "calibration relational kind and payload discriminator differ",
            ));
        }
        if artifact.kind == CalibrationKind::ModelScore {
            return Err(invariant(
                "model_score artifacts must use commit_model_score so artifact append and run success are atomic",
            ));
        }
        if artifact.kind == CalibrationKind::WeatherStationLeadBias {
            validate_weather_artifact(&artifact)?;
        }
        let content_hash = artifact.content_hash;
        Entity::insert(artifact.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(|err| {
                error::map_unique(err, QUANT_CALIBRATION_ARTIFACT, &content_hash.to_string())
            })
            .map(Into::into)
    }

    async fn commit_model_score(
        &self,
        commit: VerifiedModelScoreCalibrationCommit,
    ) -> Result<ModelScoreCalibrationCommitOutcome, StorageError> {
        let payload = commit.payload().ok_or_else(|| {
            invariant("verified model-score commit lost its payload discriminator")
        })?;
        let artifact = commit.artifact();
        let identity = ModelScoreCommitIdentity::from(artifact);
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let run = ModelRunEntity::find_by_id(commit.model_run_id())
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_MODEL_RUN, commit.model_run_id()))?;
        let dataset_hash = Box::pin(self.validate_model_score_lineage(
            &transaction,
            payload,
            artifact.fit_window_start,
            artifact.fit_window_end,
            artifact.sample_count,
        ))
        .await?;
        let already_succeeded = Self::validate_run(&run, &commit, payload, dataset_hash)?;
        if already_succeeded {
            let stored = Self::load_by_content_hash(&transaction, artifact.content_hash).await?;
            Self::verify_existing_identity(&stored, &identity)?;
            let model_run = ModelRunInfo::from(run);
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(ModelScoreCalibrationCommitOutcome::ExistingExact {
                artifact: stored,
                model_run,
            });
        }

        let (_, artifact) = commit.into_parts();
        let insert = Entity::insert(artifact.into_active_model())
            .on_conflict(OnConflict::new().do_nothing().to_owned())
            .try_insert()
            .exec_without_returning(&transaction)
            .await
            .map_err(StorageError::from)?;
        let inserted = match insert {
            TryInsertResult::Inserted(1) => true,
            TryInsertResult::Inserted(0) | TryInsertResult::Conflicted => false,
            TryInsertResult::Inserted(rows) => {
                return Err(invariant(format!(
                    "single model-score commit affected {rows} rows; expected zero or one"
                )));
            }
            TryInsertResult::Empty => {
                return Err(invariant(
                    "model-score commit unexpectedly produced an empty insert",
                ));
            }
        };
        let stored = Self::load_by_content_hash(&transaction, identity.content_hash).await?;
        Self::verify_existing_identity(&stored, &identity)?;

        let finished_at = primitives::statement_timestamp(&transaction).await?;
        let mut terminal: ModelRunActiveModel = run.into_active_model();
        terminal.status = ActiveValue::Set(ModelRunStatus::Succeeded);
        terminal.output_hash = ActiveValue::Set(Some(stored.content_hash));
        terminal.error_code = ActiveValue::Set(None);
        terminal.error_message = ActiveValue::Set(None);
        terminal.finished_at = ActiveValue::Set(Some(finished_at));
        let model_run = ModelRunInfo::from(
            terminal
                .update(&transaction)
                .await
                .map_err(StorageError::from)?,
        );
        transaction.commit().await.map_err(StorageError::from)?;
        if inserted {
            Ok(ModelScoreCalibrationCommitOutcome::Inserted {
                artifact: stored,
                model_run,
            })
        } else {
            Ok(ModelScoreCalibrationCommitOutcome::ExistingExact {
                artifact: stored,
                model_run,
            })
        }
    }

    async fn find_by_id(
        &self,
        artifact_id: &CalibrationArtifactId,
    ) -> Result<Option<CalibrationArtifactInfo>, StorageError> {
        Entity::find_by_id(*artifact_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_content_hash(
        &self,
        content_hash: &ContentHash,
    ) -> Result<Option<CalibrationArtifactInfo>, StorageError> {
        Entity::find()
            .filter(Column::ContentHash.eq(*content_hash))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: CalibrationArtifactListQuery,
    ) -> Result<Paginated<CalibrationArtifactInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(query.kind.map(|kind| Column::Kind.eq(kind)))
            .add_option(query.from.map(|from| Column::CreatedAt.gte(from)))
            .add_option(query.to.map(|to| Column::CreatedAt.lt(to)));
        paginate_mapped(
            Entity::find()
                .filter(condition)
                .order_by_desc(Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn published_weather_through(
        &self,
        at: DateTime<Utc>,
    ) -> Result<Vec<PublishedWeatherStationLeadBias>, StorageError> {
        let rows = QuantCalibrationArtifactPublicationEntity::find()
            .filter(
                QuantCalibrationArtifactPublicationColumn::Kind
                    .eq(CalibrationKind::WeatherStationLeadBias),
            )
            .filter(QuantCalibrationArtifactPublicationColumn::PublishedAt.lte(at))
            .order_by_asc(QuantCalibrationArtifactPublicationColumn::PublishedAt)
            .order_by_asc(QuantCalibrationArtifactPublicationColumn::PublicationId)
            .find_also_related(Entity)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        rows.into_iter()
            .map(|(publication, artifact)| {
                let artifact = artifact.ok_or_else(|| StorageError::InvariantViolation {
                    entity: Some(QUANT_CALIBRATION_ARTIFACT),
                    detail: "calibration publication has no artifact".to_owned(),
                })?;
                let CalibrationArtifactPayload::WeatherStationLeadBias(payload) = artifact.payload
                else {
                    return Err(StorageError::InvariantViolation {
                        entity: Some(QUANT_CALIBRATION_ARTIFACT),
                        detail: "published Weather calibration has mismatched payload kind"
                            .to_owned(),
                    });
                };
                Ok(PublishedWeatherStationLeadBias {
                    artifact_id: artifact.artifact_id,
                    content_hash: artifact.content_hash,
                    fit_window_start: artifact.fit_window_start,
                    fit_window_end: artifact.fit_window_end,
                    sample_count: artifact.sample_count,
                    published_at: publication.published_at,
                    payload,
                })
            })
            .collect()
    }

    async fn mark_active(
        &self,
        artifact_id: &CalibrationArtifactId,
    ) -> Result<CalibrationArtifactInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let Some(row) = Entity::find_by_id(*artifact_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(
                QUANT_CALIBRATION_ARTIFACT,
                artifact_id,
            ));
        };
        if row.kind == CalibrationKind::ModelScore {
            let info = CalibrationArtifactInfo::from(row.clone());
            let payload = info.verify_model_score().map_err(invariant)?;
            Box::pin(self.validate_model_score_lineage(
                &txn,
                payload,
                info.fit_window_start,
                info.fit_window_end,
                info.sample_count,
            ))
            .await?;
        }
        // `market_price_bias` has exactly one global governance pointer
        // (runtime-config `bias_table_ref`), so activating one deactivates
        // every other bias table in the same transaction — the ledger must
        // never have two concurrently active bias tables. `model_score` has
        // no such exclusivity: each model version binds its own calibrator
        // independently (a published version's candidate and its successor
        // candidate can legitimately reference different active calibrators
        // at once), so activating one never touches another.
        if matches!(
            row.kind,
            CalibrationKind::MarketPriceBias | CalibrationKind::WeatherStationLeadBias
        ) {
            Entity::update_many()
                .col_expr(Column::Active, Expr::value(false))
                .filter(Column::Kind.eq(row.kind))
                .filter(Column::Active.eq(true))
                .exec(&txn)
                .await
                .map_err(StorageError::from)?;
        }
        let mut active = row.into_active_model();
        let was_active = active.active.as_ref() == &true;
        active.active = ActiveValue::Set(true);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        if updated.kind == CalibrationKind::WeatherStationLeadBias && !was_active {
            ActiveModel {
                publication_id: ActiveValue::Set(CalibrationArtifactPublicationId::from_v7()),
                artifact_id: ActiveValue::Set(updated.artifact_id),
                kind: ActiveValue::Set(updated.kind),
                published_at: ActiveValue::Set(Utc::now()),
                ..Default::default()
            }
            .insert(&txn)
            .await
            .map_err(StorageError::from)?;
        }
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }
}

fn validate_weather_artifact(artifact: &NewCalibrationArtifact) -> Result<(), StorageError> {
    if artifact.active {
        return Err(invariant(
            "Weather calibration must be published through mark_active",
        ));
    }
    let CalibrationArtifactPayload::WeatherStationLeadBias(payload) = &artifact.payload else {
        return Err(invariant("invalid Weather calibration payload kind"));
    };
    if payload.schema_version != 1
        || payload.methodology.trim().is_empty()
        || payload.grid_hashes.is_empty()
        || payload.source_hashes.is_empty()
        || payload.stations.is_empty()
    {
        return Err(invariant("Weather calibration payload is incomplete"));
    }
    if payload.methodology_hash
        != CanonicalDigest::content_hash_json(&payload.methodology)
            .map_err(|error| invariant(error.to_string()))?
    {
        return Err(invariant("Weather calibration methodology hash mismatch"));
    }
    ensure_strictly_sorted(&payload.grid_hashes, "grid hashes")?;
    ensure_strictly_sorted(&payload.source_hashes, "source hashes")?;
    let mut previous_station = None;
    let mut total_samples = 0_i64;
    for station in &payload.stations {
        if previous_station.is_some_and(|previous| previous >= station.station.as_str()) {
            return Err(invariant(
                "Weather calibration stations must be strictly sorted",
            ));
        }
        previous_station = Some(station.station.as_str());
        let mut previous_lead = None;
        for lead in &station.leads {
            if lead.lead_hours == 0
                || lead.sample_count == 0
                || previous_lead.is_some_and(|previous| previous >= lead.lead_hours)
            {
                return Err(invariant(
                    "Weather calibration leads must be positive and strictly sorted",
                ));
            }
            previous_lead = Some(lead.lead_hours);
            total_samples = total_samples
                .checked_add(i64::from(lead.sample_count))
                .ok_or_else(|| invariant("Weather calibration sample count overflow"))?;
        }
    }
    if total_samples != artifact.sample_count {
        return Err(invariant("Weather calibration sample count mismatch"));
    }
    let expected_hash = CanonicalDigest::content_hash_json(&(
        artifact.kind,
        artifact.fit_window_start,
        artifact.fit_window_end,
        &artifact.calibration_split_hash,
        artifact.sample_count,
        &payload,
    ))
    .map_err(|error| invariant(error.to_string()))?;
    if expected_hash != artifact.content_hash {
        return Err(invariant("Weather calibration content hash mismatch"));
    }
    Ok(())
}

fn ensure_strictly_sorted(values: &[ContentHash], field: &str) -> Result<(), StorageError> {
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invariant(format!(
            "Weather calibration {field} must be strictly sorted"
        )));
    }
    Ok(())
}

fn invariant(detail: impl Into<String>) -> StorageError {
    StorageError::InvariantViolation {
        entity: Some(QUANT_CALIBRATION_ARTIFACT),
        detail: detail.into(),
    }
}
