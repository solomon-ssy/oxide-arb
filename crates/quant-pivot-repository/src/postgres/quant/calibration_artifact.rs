//! Postgres-backed unified calibration-artifact ledger repository (append-only
//! identity; `active` is the sole mutable column — Phase 11.3 §3.4).

use crate::{
    postgres::{error, query::paginate_mapped},
    traits::CalibrationArtifactRepository,
};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        CalibrationArtifactInfo, CalibrationArtifactListQuery, NewCalibrationArtifact, PageWindow,
        Paginated, PublishedWeatherStationLeadBias, WeatherStationLeadBiasArtifactV1,
    },
    entities::{quant_calibration_artifact, quant_calibration_artifact_publication},
    enums::quant::CalibrationKind,
    hashing::CanonicalDigest,
    types::{CalibrationArtifactId, ContentHash},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, TransactionTrait, sea_query::Expr,
};

/// Postgres-backed unified calibration-artifact ledger repository.
pub struct PgCalibrationArtifactRepository {
    db: DatabaseConnection,
}

impl PgCalibrationArtifactRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl CalibrationArtifactRepository for PgCalibrationArtifactRepository {
    async fn create(
        &self,
        artifact: NewCalibrationArtifact,
    ) -> Result<CalibrationArtifactInfo, StorageError> {
        if artifact.kind == CalibrationKind::WeatherStationLeadBias {
            validate_weather_artifact(&artifact)?;
        }
        let content_hash = artifact.content_hash.clone();
        quant_calibration_artifact::Entity::insert(artifact.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(|err| {
                error::map_unique(
                    err,
                    entity::QUANT_CALIBRATION_ARTIFACT,
                    content_hash.as_str(),
                )
            })
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        artifact_id: &CalibrationArtifactId,
    ) -> Result<Option<CalibrationArtifactInfo>, StorageError> {
        quant_calibration_artifact::Entity::find_by_id(artifact_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_content_hash(
        &self,
        content_hash: &ContentHash,
    ) -> Result<Option<CalibrationArtifactInfo>, StorageError> {
        quant_calibration_artifact::Entity::find()
            .filter(quant_calibration_artifact::Column::ContentHash.eq(content_hash.clone()))
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
            .add_option(
                query
                    .kind
                    .map(|kind| quant_calibration_artifact::Column::Kind.eq(kind)),
            )
            .add_option(
                query
                    .from
                    .map(|from| quant_calibration_artifact::Column::CreatedAt.gte(from)),
            )
            .add_option(
                query
                    .to
                    .map(|to| quant_calibration_artifact::Column::CreatedAt.lt(to)),
            );
        paginate_mapped(
            quant_calibration_artifact::Entity::find()
                .filter(condition)
                .order_by_desc(quant_calibration_artifact::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn published_weather_through(
        &self,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<PublishedWeatherStationLeadBias>, StorageError> {
        let rows = quant_calibration_artifact_publication::Entity::find()
            .filter(
                quant_calibration_artifact_publication::Column::Kind
                    .eq(CalibrationKind::WeatherStationLeadBias),
            )
            .filter(quant_calibration_artifact_publication::Column::PublishedAt.lte(at))
            .order_by_asc(quant_calibration_artifact_publication::Column::PublishedAt)
            .order_by_asc(quant_calibration_artifact_publication::Column::PublicationId)
            .find_also_related(quant_calibration_artifact::Entity)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        rows.into_iter()
            .map(|(publication, artifact)| {
                let artifact = artifact.ok_or_else(|| StorageError::InvariantViolation {
                    entity: Some(entity::QUANT_CALIBRATION_ARTIFACT),
                    detail: "calibration publication has no artifact".to_owned(),
                })?;
                let payload = serde_json::from_value::<WeatherStationLeadBiasArtifactV1>(
                    artifact.payload_json,
                )
                .map_err(|error| StorageError::InvariantViolation {
                    entity: Some(entity::QUANT_CALIBRATION_ARTIFACT),
                    detail: format!("invalid published Weather calibration payload: {error}"),
                })?;
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
        let Some(row) = quant_calibration_artifact::Entity::find_by_id(artifact_id.clone())
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(
                entity::QUANT_CALIBRATION_ARTIFACT,
                artifact_id,
            ));
        };
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
            quant_calibration_artifact::Entity::update_many()
                .col_expr(
                    quant_calibration_artifact::Column::Active,
                    Expr::value(false),
                )
                .filter(quant_calibration_artifact::Column::Kind.eq(row.kind))
                .filter(quant_calibration_artifact::Column::Active.eq(true))
                .exec(&txn)
                .await
                .map_err(StorageError::from)?;
        }
        let mut active = row.into_active_model();
        let was_active = active.active.as_ref() == &true;
        active.active = ActiveValue::Set(true);
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        if updated.kind == CalibrationKind::WeatherStationLeadBias && !was_active {
            quant_calibration_artifact_publication::ActiveModel {
                publication_id: ActiveValue::Set(uuid::Uuid::now_v7()),
                artifact_id: ActiveValue::Set(updated.artifact_id.clone()),
                kind: ActiveValue::Set(updated.kind),
                published_at: ActiveValue::Set(chrono::Utc::now()),
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
    let payload =
        serde_json::from_value::<WeatherStationLeadBiasArtifactV1>(artifact.payload_json.clone())
            .map_err(|error| invariant(format!("invalid Weather calibration payload: {error}")))?;
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
        entity: Some(entity::QUANT_CALIBRATION_ARTIFACT),
        detail: detail.into(),
    }
}
