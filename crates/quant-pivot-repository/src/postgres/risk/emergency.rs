use crate::traits::EmergencyRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{domain::NewEmergencySnapshot, entities::emergency_snapshot::Entity};
use sea_orm::{DatabaseConnection, EntityTrait, IntoActiveModel};

pub struct PgEmergencyRepository {
    db: DatabaseConnection,
}

impl PgEmergencyRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl EmergencyRepository for PgEmergencyRepository {
    async fn create(&self, snapshot: NewEmergencySnapshot) -> Result<(), StorageError> {
        Entity::insert(snapshot.into_active_model())
            .exec(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }
}
