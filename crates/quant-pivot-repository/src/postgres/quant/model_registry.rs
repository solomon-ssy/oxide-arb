//! Postgres-backed model registry repository.

use crate::{
    postgres::{
        error,
        query::{paginate_into_model, paginate_mapped},
    },
    traits::ModelRegistryRepository,
};
use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        ModelSpecInfo, ModelSpecListQuery, ModelVersionInfo, ModelVersionListQuery, NewModelSpec,
        NewModelVersion, PageWindow, Paginated,
    },
    entities::{quant_model_spec, quant_model_version},
    enums::quant::PublicationStatus,
    types::{BacktestPathSetId, ModelSpecId, ModelVersionId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    EntityTrait, IntoActiveModel, JoinType, QueryFilter, QueryOrder, QuerySelect, RelationTrait,
    Select, TransactionTrait,
};

/// Postgres-backed model registry repository.
pub struct PgModelRegistryRepository {
    db: DatabaseConnection,
}

impl PgModelRegistryRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

/// Version row + owning-spec `model_family` (N:1 INNER JOIN).
fn select_version_with_family() -> Select<quant_model_version::Entity> {
    quant_model_version::Entity::find()
        .join(
            JoinType::InnerJoin,
            quant_model_version::Relation::ModelSpec.def(),
        )
        .column_as(quant_model_spec::Column::ModelFamily, "model_family")
}

async fn find_version_info(
    db: &impl ConnectionTrait,
    model_version_id: &ModelVersionId,
) -> Result<Option<ModelVersionInfo>, StorageError> {
    select_version_with_family()
        .filter(quant_model_version::Column::ModelVersionId.eq(model_version_id.clone()))
        .into_model::<ModelVersionInfo>()
        .one(db)
        .await
        .map_err(StorageError::from)
}

async fn require_version_info(
    db: &impl ConnectionTrait,
    model_version_id: &ModelVersionId,
) -> Result<ModelVersionInfo, StorageError> {
    find_version_info(db, model_version_id)
        .await?
        .ok_or_else(|| error::not_found(entity::QUANT_MODEL_VERSION, model_version_id))
}

#[async_trait::async_trait]
impl ModelRegistryRepository for PgModelRegistryRepository {
    async fn create_model_spec(&self, spec: NewModelSpec) -> Result<ModelSpecInfo, StorageError> {
        let name = spec.name.clone();
        quant_model_spec::Entity::insert(spec.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(|err| error::map_unique(err, entity::QUANT_MODEL_SPEC, &name))
            .map(Into::into)
    }

    async fn find_model_spec_by_id(
        &self,
        model_spec_id: &ModelSpecId,
    ) -> Result<Option<ModelSpecInfo>, StorageError> {
        quant_model_spec::Entity::find_by_id(model_spec_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn create_model_version(
        &self,
        mut version: NewModelVersion,
    ) -> Result<ModelVersionInfo, StorageError> {
        // Allocate `version` under an exclusive lock on the owning spec so
        // concurrent trainers cannot mint the same `(model_spec_id, version)`.
        // Callers may pass a preview from `next_version_for_spec`; the locked
        // MAX+1 here is authoritative.
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let Some(_spec) = quant_model_spec::Entity::find_by_id(version.model_spec_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(
                entity::QUANT_MODEL_SPEC,
                &version.model_spec_id,
            ));
        };
        let max_version = quant_model_version::Entity::find()
            .filter(quant_model_version::Column::ModelSpecId.eq(version.model_spec_id.clone()))
            .select_only()
            .column_as(quant_model_version::Column::Version.max(), "max_version")
            .into_tuple::<Option<i32>>()
            .one(&txn)
            .await
            .map_err(StorageError::from)?;
        version.version = max_version.flatten().unwrap_or(0).saturating_add(1);
        let duplicate_key = format!("{}:v{}", version.model_spec_id, version.version);
        let model_version_id = version.model_version_id.clone();
        quant_model_version::Entity::insert(version.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(|err| error::map_unique(err, entity::QUANT_MODEL_VERSION, &duplicate_key))?;
        let info = require_version_info(&txn, &model_version_id).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn next_version_for_spec(
        &self,
        model_spec_id: &ModelSpecId,
    ) -> Result<i32, StorageError> {
        // Preview only — `create_model_version` re-allocates under lock.
        let max_version = quant_model_version::Entity::find()
            .filter(quant_model_version::Column::ModelSpecId.eq(model_spec_id.clone()))
            .select_only()
            .column_as(quant_model_version::Column::Version.max(), "max_version")
            .into_tuple::<Option<i32>>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(max_version.flatten().unwrap_or(0).saturating_add(1))
    }

    async fn find_model_version_by_id(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Option<ModelVersionInfo>, StorageError> {
        find_version_info(&self.db, model_version_id).await
    }

    async fn page_specs(
        &self,
        query: ModelSpecListQuery,
    ) -> Result<Paginated<ModelSpecInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .model_family
                    .map(|family| quant_model_spec::Column::ModelFamily.eq(family)),
            )
            .add_option(
                query
                    .status
                    .map(|status| quant_model_spec::Column::Status.eq(status)),
            );
        paginate_mapped(
            quant_model_spec::Entity::find()
                .filter(condition)
                .order_by_desc(quant_model_spec::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn page_versions(
        &self,
        query: ModelVersionListQuery,
    ) -> Result<Paginated<ModelVersionInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .model_spec_id
                    .clone()
                    .map(|id| quant_model_version::Column::ModelSpecId.eq(id)),
            )
            .add_option(
                query
                    .publication_status
                    .map(|status| quant_model_version::Column::PublicationStatus.eq(status)),
            )
            .add_option(
                query
                    .from
                    .map(|from| quant_model_version::Column::CreatedAt.gte(from)),
            )
            .add_option(
                query
                    .to
                    .map(|to| quant_model_version::Column::CreatedAt.lt(to)),
            );
        paginate_into_model(
            select_version_with_family()
                .filter(condition)
                .order_by_desc(quant_model_version::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
        )
        .await
    }

    async fn list_published_for_spec(
        &self,
        model_spec_id: &ModelSpecId,
    ) -> Result<Vec<ModelVersionInfo>, StorageError> {
        select_version_with_family()
            .filter(quant_model_version::Column::ModelSpecId.eq(model_spec_id.clone()))
            .filter(quant_model_version::Column::PublicationStatus.eq(PublicationStatus::Published))
            .order_by_desc(quant_model_version::Column::PublishedAt)
            .into_model::<ModelVersionInfo>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)
    }

    async fn publish_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let info =
            update_model_version_status(&txn, model_version_id, PublicationStatus::Published)
                .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn retire_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let info =
            update_model_version_status(&txn, model_version_id, PublicationStatus::Retired).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn publish_replacing_predecessors(
        &self,
        model_spec_id: &ModelSpecId,
        model_version_id: &ModelVersionId,
    ) -> Result<
        (
            ModelVersionInfo,
            Vec<ModelVersionId>,
            Option<ModelVersionInfo>,
        ),
        StorageError,
    > {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        // Serialize concurrent publishes for the same spec.
        let Some(_spec) = quant_model_spec::Entity::find_by_id(model_spec_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(entity::QUANT_MODEL_SPEC, model_spec_id));
        };

        let predecessors = select_version_with_family()
            .filter(quant_model_version::Column::ModelSpecId.eq(model_spec_id.clone()))
            .filter(quant_model_version::Column::PublicationStatus.eq(PublicationStatus::Published))
            .order_by_desc(quant_model_version::Column::PublishedAt)
            .into_model::<ModelVersionInfo>()
            .all(&txn)
            .await
            .map_err(StorageError::from)?;
        let rollback_target = predecessors.first().cloned();

        let mut retired = Vec::new();
        for predecessor in &predecessors {
            if predecessor.model_version_id == *model_version_id {
                continue;
            }
            update_model_version_status(
                &txn,
                &predecessor.model_version_id,
                PublicationStatus::Retired,
            )
            .await?;
            retired.push(predecessor.model_version_id.clone());
        }

        let published =
            update_model_version_status(&txn, model_version_id, PublicationStatus::Published)
                .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok((published, retired, rollback_target))
    }

    async fn promote_model_to_shadow(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let info =
            update_model_version_status(&txn, model_version_id, PublicationStatus::Shadow).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn restore_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let info =
            update_model_version_status(&txn, model_version_id, PublicationStatus::Published)
                .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn set_quality_gate_report(
        &self,
        model_version_id: &ModelVersionId,
        quality_gate_report: serde_json::Value,
    ) -> Result<ModelVersionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let Some(row) = quant_model_version::Entity::find_by_id(model_version_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(
                entity::QUANT_MODEL_VERSION,
                model_version_id,
            ));
        };
        let mut active = row.into_active_model();
        active.quality_gate_report = ActiveValue::Set(quality_gate_report);
        active.update(&txn).await.map_err(StorageError::from)?;
        let info = require_version_info(&txn, model_version_id).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn set_publish_path_set_id(
        &self,
        model_version_id: &ModelVersionId,
        publish_path_set_id: Option<BacktestPathSetId>,
    ) -> Result<ModelVersionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let Some(row) = quant_model_version::Entity::find_by_id(model_version_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(
                entity::QUANT_MODEL_VERSION,
                model_version_id,
            ));
        };
        let mut active = row.into_active_model();
        active.publish_path_set_id = ActiveValue::Set(publish_path_set_id);
        active.update(&txn).await.map_err(StorageError::from)?;
        let info = require_version_info(&txn, model_version_id).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }
}

/// Transition a version's publication status under an existing connection/txn.
/// Always `SELECT … FOR UPDATE`s the version row before mutating.
async fn update_model_version_status(
    db: &impl ConnectionTrait,
    model_version_id: &ModelVersionId,
    next: PublicationStatus,
) -> Result<ModelVersionInfo, StorageError> {
    let Some(row) = quant_model_version::Entity::find_by_id(model_version_id.clone())
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(error::not_found(
            entity::QUANT_MODEL_VERSION,
            model_version_id,
        ));
    };
    let from = row.publication_status;
    if !from.allows_transition_to(next) {
        return Err(error::illegal_transition(
            entity::QUANT_MODEL_VERSION,
            Some(model_version_id),
            from,
            next,
        ));
    }
    if from == next {
        return require_version_info(db, model_version_id).await;
    }
    let mut active = row.into_active_model();
    active.publication_status = ActiveValue::Set(next);
    match next {
        PublicationStatus::Published => {
            active.published_at = ActiveValue::Set(Some(Utc::now()));
            active.retired_at = ActiveValue::Set(None);
        }
        PublicationStatus::Retired => {
            active.retired_at = ActiveValue::Set(Some(Utc::now()));
        }
        _ => {}
    }
    active.update(db).await.map_err(StorageError::from)?;
    require_version_info(db, model_version_id).await
}
