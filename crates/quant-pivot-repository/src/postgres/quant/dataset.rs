//! Postgres-backed training-dataset ledger repository.

use crate::{
    postgres::{error, query::paginate_mapped},
    traits::TrainingDatasetRepository,
};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        NewTrainingDataset, PageWindow, Paginated, TrainingDatasetInfo, TrainingDatasetListQuery,
    },
    entities::quant_training_dataset,
    enums::quant::TrainingDatasetStatus,
    types::TrainingDatasetId,
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder,
};

/// Postgres-backed training-dataset ledger repository.
pub struct PgTrainingDatasetRepository {
    db: DatabaseConnection,
}

impl PgTrainingDatasetRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl TrainingDatasetRepository for PgTrainingDatasetRepository {
    async fn create(
        &self,
        dataset: NewTrainingDataset,
    ) -> Result<TrainingDatasetInfo, StorageError> {
        quant_training_dataset::Entity::insert(dataset.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> Result<Option<TrainingDatasetInfo>, StorageError> {
        quant_training_dataset::Entity::find_by_id(training_dataset_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: TrainingDatasetListQuery,
    ) -> Result<Paginated<TrainingDatasetInfo>, StorageError> {
        paginate_mapped(
            quant_training_dataset::Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(quant_training_dataset::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn mark_status(
        &self,
        training_dataset_id: &TrainingDatasetId,
        next: TrainingDatasetStatus,
    ) -> Result<TrainingDatasetInfo, StorageError> {
        let Some(row) = quant_training_dataset::Entity::find_by_id(training_dataset_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(
                entity::QUANT_TRAINING_DATASET,
                training_dataset_id,
            ));
        };
        if !is_valid_transition(row.status, next) {
            return Err(error::illegal_transition(
                entity::QUANT_TRAINING_DATASET,
                Some(training_dataset_id),
                row.status,
                next,
            ));
        }
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(next);
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }
}

fn page_condition(query: &TrainingDatasetListQuery) -> Condition {
    Condition::all()
        .add_option(
            query
                .model_spec_id
                .clone()
                .map(|id| quant_training_dataset::Column::ModelSpecId.eq(id)),
        )
        .add_option(
            query
                .status
                .map(|status| quant_training_dataset::Column::Status.eq(status)),
        )
        .add_option(
            query
                .from
                .map(|from| quant_training_dataset::Column::CreatedAt.gte(from)),
        )
        .add_option(
            query
                .to
                .map(|to| quant_training_dataset::Column::CreatedAt.lt(to)),
        )
}

/// The training-dataset lifecycle state machine.
///
/// `Planned → Building → {Built | InsufficientLabels | Failed}`;
/// `Built → {Ready | Expired | Failed}`; `Ready → Expired`. `InsufficientLabels`
/// and `Failed` are terminal.
const fn is_valid_transition(current: TrainingDatasetStatus, next: TrainingDatasetStatus) -> bool {
    use TrainingDatasetStatus::{
        Building, Built, Expired, Failed, InsufficientLabels, Planned, Ready,
    };
    matches!(
        (current, next),
        (Planned, Building)
            | (Building, Built | InsufficientLabels | Failed)
            | (Built, Ready | Expired | Failed)
            | (Ready, Expired)
    )
}
